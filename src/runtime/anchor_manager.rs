// SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Anchor Manager - Internal state management for response anchors
//!
//! This module is never exposed in the public API. It handles:
//! - Creating anchors and returning (handle, stream) pairs
//! - Managing exclusive attachment semantics
//! - Routing StreamFrame<T> to the correct anchor
//! - Processing sentinel frames (Detached, Finalized)
//! - Cleanup on cancel/finalize

use anyhow::Result;
use dashmap::DashMap;
use serde::{Serialize, de::DeserializeOwned};
use std::any::{Any, type_name};
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;
use tracing::{debug, error, warn};
use uuid::Uuid;

use crate::api::client::{ActiveMessageClient, WorkerAddress};
use crate::api::handler::InstanceId;
use crate::protocols::response_anchor::*;
use crate::runtime::network_client::NetworkClient;
use crate::transport::streaming::{StreamReceiver, StreamingTransport};
use crate::zmq::streaming_transport::ZmqStreamingTransport;

/// Internal anchor manager (not exposed in public API)
///
/// This manager tracks all active response anchors for a given instance
/// and routes incoming stream frames to the appropriate anchor.
pub struct AnchorManager {
    /// Registry of active anchors
    anchors: Arc<DashMap<Uuid, AnchorEntry>>,

    /// Streaming transport for data frames
    streaming_transport: Arc<ZmqStreamingTransport>,

    /// Network client for sending cancellation signals
    network_client: Arc<NetworkClient>,

    /// Instance ID of this manager
    instance_id: InstanceId,

    /// Streaming endpoint (bound once, reused for all anchors)
    stream_endpoint: String,
}

/// Entry for an active anchor
struct AnchorEntry {
    /// Channel to send frames to user's stream
    stream_sender: mpsc::Sender<AnchorStreamEvent>,

    /// Current attachment session (if any)
    attachment: Option<AttachmentSession>,

    /// Prologue metadata from the attached source (if received)
    prologue: Option<PrologueMetadata>,

    /// Metadata
    created_at: Instant,
    type_name: &'static str,
}

/// Metadata from the prologue frame
struct PrologueMetadata {
    source_instance_id: InstanceId,
    cancellation_id: Uuid,
    source_endpoint: String,
}

/// Active attachment session
struct AttachmentSession {
    session_id: Uuid,
    source_instance: InstanceId,
    attached_at: Instant,
}

const STREAM_CHANNEL_CAPACITY: usize = 128;

enum AnchorStreamEvent {
    Item(Result<Box<dyn Any + Send>, String>),
    TransportError(anyhow::Error),
}

impl AnchorManager {
    /// Create a new anchor manager
    ///
    /// This binds the streaming transport endpoint once and reuses it
    /// for all anchors created by this manager.
    pub async fn new(
        streaming_transport: Arc<ZmqStreamingTransport>,
        network_client: Arc<NetworkClient>,
        instance_id: InstanceId,
    ) -> Result<Self> {
        let stream_endpoint = (*streaming_transport).bind_stream_endpoint().await?;

        debug!(
            instance_id = %instance_id,
            stream_endpoint = %stream_endpoint,
            "AnchorManager created"
        );

        Ok(Self {
            anchors: Arc::new(DashMap::new()),
            streaming_transport,
            network_client,
            instance_id,
            stream_endpoint,
        })
    }

    /// Create a new response anchor
    ///
    /// Returns:
    /// - Handle: Can be serialized and sent across the network
    /// - Stream: User receives Result<T, E> items
    pub async fn create_anchor<T>(
        &self,
        worker_address: WorkerAddress,
    ) -> Result<(ResponseAnchorHandle, ResponseAnchorStream<T>)>
    where
        T: Serialize + DeserializeOwned + Send + 'static,
    {
        let anchor_id = Uuid::new_v4();

        // Create channel for user stream
        let (tx, rx) = mpsc::channel(STREAM_CHANNEL_CAPACITY);

        // Create anchor entry
        let entry = AnchorEntry {
            stream_sender: tx,
            attachment: None,
            prologue: None,
            created_at: Instant::now(),
            type_name: type_name::<T>(),
        };

        // Register anchor
        self.anchors.insert(anchor_id, entry);

        // Create receiver for streaming frames
        let receiver = (*self.streaming_transport)
            .create_stream_receiver::<T>(anchor_id)
            .await?;

        // Spawn task to route frames to user stream
        self.spawn_frame_router(anchor_id, receiver);

        // Create handle
        let handle = ResponseAnchorHandle::new(anchor_id, self.instance_id, worker_address);

        // Create user stream
        let stream = ResponseAnchorStream::new(rx, type_name::<T>());

        debug!(
            anchor_id = %anchor_id,
            type_name = type_name::<T>(),
            "Created response anchor"
        );

        Ok((handle, stream))
    }

    /// Spawn a task to route frames from the receiver to the user stream
    fn spawn_frame_router<T>(
        &self,
        anchor_id: Uuid,
        mut receiver: Box<dyn StreamReceiver<T> + Send + 'static>,
    ) where
        T: Serialize + DeserializeOwned + Send + 'static,
    {
        let anchors = self.anchors.clone();

        tokio::spawn(async move {
            while let Some(frame_result) = receiver.recv().await {
                match frame_result {
                    Ok(frame) => {
                        if let Err(e) = Self::process_frame(&anchors, anchor_id, frame).await {
                            error!(anchor_id = %anchor_id, error = %e, "Error processing frame");
                            break;
                        }
                    }
                    Err(e) => {
                        error!(anchor_id = %anchor_id, error = %e, "Error receiving frame");
                        // Send transport error to user stream
                        if let Some(entry) = anchors.get(&anchor_id) {
                            let sender = entry.stream_sender.clone();
                            drop(entry);

                            if let Err(error) = sender
                                .send(AnchorStreamEvent::TransportError(anyhow::anyhow!(
                                    "Transport error: {}",
                                    e
                                )))
                                .await
                            {
                                warn!(
                                    anchor_id = %anchor_id,
                                    error = %error,
                                    "Failed to propagate transport error to user stream",
                                );
                            }
                        }
                        break;
                    }
                }
            }

            debug!(anchor_id = %anchor_id, "Frame router task finished");
        });
    }

    /// Process a received frame
    async fn process_frame<T>(
        anchors: &DashMap<Uuid, AnchorEntry>,
        anchor_id: Uuid,
        frame: StreamFrame<T>,
    ) -> Result<()>
    where
        T: Serialize + Send + 'static,
    {
        let entry = match anchors.get(&anchor_id) {
            Some(e) => e,
            None => {
                warn!(anchor_id = %anchor_id, "Received frame for unknown anchor");
                return Ok(());
            }
        };

        match frame {
            StreamFrame::Prologue {
                source_instance_id,
                cancellation_id,
                source_endpoint,
            } => {
                debug!(
                    anchor_id = %anchor_id,
                    source_instance_id = %source_instance_id,
                    cancellation_id = %cancellation_id,
                    "Processing Prologue frame"
                );

                // Store prologue metadata
                drop(entry); // Release the read lock
                if let Some(mut entry) = anchors.get_mut(&anchor_id) {
                    entry.prologue = Some(PrologueMetadata {
                        source_instance_id,
                        cancellation_id,
                        source_endpoint,
                    });
                }

                debug!(anchor_id = %anchor_id, "Prologue metadata stored");
            }

            StreamFrame::Item(result) => {
                let event = match result {
                    Ok(data) => AnchorStreamEvent::Item(Ok(Box::new(data) as Box<dyn Any + Send>)),
                    Err(msg) => AnchorStreamEvent::Item(Err(msg)),
                };

                let sender = entry.stream_sender.clone();
                drop(entry);

                if let Err(error) = sender.send(event).await {
                    warn!(
                        anchor_id = %anchor_id,
                        error = %error,
                        "Failed to deliver item to user stream; removing anchor",
                    );
                    anchors.remove(&anchor_id);
                }
            }

            StreamFrame::Detached => {
                debug!(anchor_id = %anchor_id, "Processing Detached frame");

                // Mark attachment as None (ready for reattachment)
                drop(entry); // Release the read lock
                if let Some(mut entry) = anchors.get_mut(&anchor_id) {
                    entry.attachment = None;
                }

                debug!(anchor_id = %anchor_id, "Anchor detached, ready for reattachment");
            }

            StreamFrame::Finalized => {
                debug!(anchor_id = %anchor_id, "Processing Finalized frame");

                // Close user stream (drop the sender)
                drop(entry); // Release the read lock

                // Remove from registry
                anchors.remove(&anchor_id);

                debug!(anchor_id = %anchor_id, "Anchor finalized and removed");
            }

            StreamFrame::TransportError(msg) => {
                error!(anchor_id = %anchor_id, "Transport error: {}", msg);

                let sender = entry.stream_sender.clone();
                drop(entry);

                if let Err(error) = sender
                    .send(AnchorStreamEvent::TransportError(anyhow::anyhow!(
                        "Transport error: {}",
                        msg
                    )))
                    .await
                {
                    warn!(
                        anchor_id = %anchor_id,
                        error = %error,
                        "Failed to propagate transport error to user stream",
                    );
                }

                anchors.remove(&anchor_id);

                debug!(anchor_id = %anchor_id, "Anchor removed due to transport error");
            }
        }

        Ok(())
    }

    // ========================================================================
    // Control message handlers (called by ActiveMessage system handlers)
    // ========================================================================

    /// Handle an attach request
    pub async fn handle_attach(
        &self,
        request: AnchorAttachRequest,
    ) -> Result<AnchorAttachResponse, String> {
        let anchor_id = request.anchor_id;

        let mut entry = match self.anchors.get_mut(&anchor_id) {
            Some(e) => e,
            None => {
                return Err(AttachError::AnchorNotFound.to_string());
            }
        };

        // Check if already attached
        if entry.attachment.is_some() {
            return Err(AttachError::AlreadyAttached.to_string());
        }

        // Create attachment session
        let session = AttachmentSession {
            session_id: request.session_id,
            source_instance: request.source_instance_id,
            attached_at: Instant::now(),
        };

        entry.attachment = Some(session);

        debug!(
            anchor_id = %anchor_id,
            session_id = %request.session_id,
            "Anchor attached"
        );

        Ok(AnchorAttachResponse {
            stream_endpoint: self.stream_endpoint.clone(),
        })
    }

    /// Handle a detach request
    pub async fn handle_detach(
        &self,
        request: AnchorDetachRequest,
    ) -> Result<AnchorDetachResponse, String> {
        let anchor_id = request.anchor_id;

        let mut entry = match self.anchors.get_mut(&anchor_id) {
            Some(e) => e,
            None => {
                return Err(DetachError::AnchorNotFound.to_string());
            }
        };

        // Check if attached
        let session = match &entry.attachment {
            Some(s) => s,
            None => {
                return Err(DetachError::NotAttached.to_string());
            }
        };

        // Validate session ID
        if session.session_id != request.session_id {
            return Err(DetachError::SessionMismatch.to_string());
        }

        // Clear attachment
        entry.attachment = None;

        debug!(
            anchor_id = %anchor_id,
            session_id = %request.session_id,
            "Anchor detached via control message"
        );

        Ok(AnchorDetachResponse { success: true })
    }

    /// Handle a finalize request
    pub async fn handle_finalize(
        &self,
        request: AnchorFinalizeRequest,
    ) -> Result<AnchorFinalizeResponse, String> {
        let anchor_id = request.anchor_id;

        let entry = match self.anchors.get(&anchor_id) {
            Some(e) => e,
            None => {
                return Err(FinalizeError::AnchorNotFound.to_string());
            }
        };

        // Check if attached
        let session = match &entry.attachment {
            Some(s) => s,
            None => {
                return Err(FinalizeError::NotAttached.to_string());
            }
        };

        // Validate session ID
        if session.session_id != request.session_id {
            return Err(FinalizeError::SessionMismatch.to_string());
        }

        // Note: Actual cleanup happens when Finalized frame is processed
        // This control message just validates the request

        debug!(
            anchor_id = %anchor_id,
            session_id = %request.session_id,
            "Anchor finalize request validated"
        );

        Ok(AnchorFinalizeResponse { success: true })
    }

    /// Handle a cancel request (from creator)
    pub async fn handle_cancel(
        &self,
        request: AnchorCancelRequest,
    ) -> Result<AnchorCancelResponse, String> {
        let anchor_id = request.anchor_id;

        match self.anchors.remove(&anchor_id) {
            Some((_, entry)) => {
                debug!(anchor_id = %anchor_id, "Anchor cancelled by creator");

                // If there's an active attachment with prologue metadata, send cancellation signal
                if let Some(prologue) = entry.prologue {
                    debug!(
                        anchor_id = %anchor_id,
                        source_instance_id = %prologue.source_instance_id,
                        cancellation_id = %prologue.cancellation_id,
                        "Sending cancellation signal to source"
                    );

                    // Send cancellation signal to the source
                    let signal = SourceCancellationSignal::new(prologue.cancellation_id);

                    if let Err(e) = self
                        .network_client
                        .as_ref()
                        .system_active_message("_source_cancel")
                        .payload(signal)
                        .unwrap()
                        .target_instance(prologue.source_instance_id)
                        .execute()
                        .await
                    {
                        warn!(
                            anchor_id = %anchor_id,
                            error = %e,
                            "Failed to send cancellation signal to source"
                        );
                    }
                }

                Ok(AnchorCancelResponse { success: true })
            }
            None => Err(CancelError::AnchorNotFound.to_string()),
        }
    }

    /// Get statistics about active anchors
    pub fn stats(&self) -> AnchorStats {
        let total = self.anchors.len();
        let mut attached = 0;

        for entry in self.anchors.iter() {
            if entry.attachment.is_some() {
                attached += 1;
            }
        }

        AnchorStats {
            total_anchors: total,
            attached_anchors: attached,
            available_anchors: total - attached,
        }
    }
}

/// Statistics about active anchors
#[derive(Debug, Clone)]
pub struct AnchorStats {
    pub total_anchors: usize,
    pub attached_anchors: usize,
    pub available_anchors: usize,
}

// ============================================================================
// ResponseAnchorStream - User-facing stream type
// ============================================================================

/// Stream of results from a response anchor
///
/// This stream yields Result<T, E> items until:
/// - The stream is finalized (returns None)
/// - A transport error occurs (yields error, then None)
pub struct ResponseAnchorStream<T> {
    rx: mpsc::Receiver<AnchorStreamEvent>,
    expected_type: &'static str,
    _phantom: PhantomData<T>,
}

impl<T> ResponseAnchorStream<T> {
    fn new(rx: mpsc::Receiver<AnchorStreamEvent>, expected_type: &'static str) -> Self {
        Self {
            rx,
            expected_type,
            _phantom: PhantomData,
        }
    }

    /// Receive the next item from the stream
    ///
    /// Returns:
    /// - Some(Ok(item)) - successful item
    /// - Some(Err(e)) - application or transport error
    /// - None - stream closed (finalized or error)
    pub async fn recv(&mut self) -> Option<Result<T, anyhow::Error>>
    where
        T: Send + 'static,
    {
        let event = self.rx.recv().await?;
        Some(Self::convert_event(event, self.expected_type))
    }

    fn convert_event(
        event: AnchorStreamEvent,
        expected_type: &'static str,
    ) -> Result<T, anyhow::Error>
    where
        T: Send + 'static,
    {
        match event {
            AnchorStreamEvent::Item(Ok(value)) => match value.downcast::<T>() {
                Ok(v) => Ok(*v),
                Err(_) => Err(anyhow::anyhow!(
                    "Anchor stream received mismatched type; expected {}",
                    expected_type
                )),
            },
            AnchorStreamEvent::Item(Err(msg)) => Err(anyhow::anyhow!("Application error: {}", msg)),
            AnchorStreamEvent::TransportError(err) => Err(err),
        }
    }
}

// Implement Stream trait
impl<T: Send + Unpin + 'static> futures::Stream for ResponseAnchorStream<T> {
    type Item = Result<T, anyhow::Error>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use std::task::Poll;

        match self.rx.poll_recv(cx) {
            Poll::Ready(Some(event)) => {
                let result = Self::convert_event(event, self.expected_type);
                Poll::Ready(Some(result))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::zmq::streaming_transport::ZmqStreamingTransport;

    #[tokio::test]
    async fn test_anchor_manager_creation() {
        let instance_id = Uuid::new_v4();
        let transport = Arc::new(ZmqStreamingTransport::new(instance_id));

        // For tests, we'll need to create a proper NetworkClient
        // This will fail compilation until we have proper test setup
        // let network_client = Arc::new(NetworkClient::new(...));
        // let manager = AnchorManager::new(transport, network_client, instance_id).await.unwrap();

        // let stats = manager.stats();
        // assert_eq!(stats.total_anchors, 0);
    }

    // Tests commented out - need proper NetworkClient setup
    // TODO: Restore tests after integration is complete

    // #[tokio::test]
    // async fn test_create_anchor() { ... }

    // #[tokio::test]
    // async fn test_handle_attach() { ... }

    // #[tokio::test]
    // async fn test_attach_already_attached() { ... }

    // #[tokio::test]
    // async fn test_handle_cancel() { ... }
}
