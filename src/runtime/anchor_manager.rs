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

use anyhow::{Result, anyhow};
use dashmap::DashMap;
use serde::{Serialize, de::DeserializeOwned};
use std::any::{Any, type_name};
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::runtime::{Handle, Runtime};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, error, warn};
use uuid::Uuid;

use crate::api::client::{ActiveMessageClient, WorkerAddress};
use crate::api::handler::InstanceId;
use crate::api::response_anchor::ArmedAnchorHandle;
use crate::protocols::response_anchor::*;
use crate::runtime::network_client::NetworkClient;
use crate::transport::streaming::{StreamReceiver, StreamingTransport};
use crate::zmq::streaming_transport::ZmqStreamingTransport;

/// Internal anchor manager (not exposed in public API)
///
/// This manager tracks all active response anchors for a given instance
/// and routes incoming stream frames to the appropriate anchor.
pub struct AnchorManager {
    inner: Arc<AnchorManagerInner>,
}

struct AnchorManagerInner {
    anchors: DashMap<Uuid, AnchorEntry>,
    streaming_transport: Arc<ZmqStreamingTransport>,
    network_client: Arc<NetworkClient>,
    instance_id: InstanceId,
    stream_endpoint: String,
    timeout_worker: Mutex<Option<JoinHandle<()>>>,
}

/// Entry for an active anchor
#[allow(dead_code)]
struct AnchorEntry {
    /// Channel to send frames to user's stream
    stream_sender: mpsc::Sender<AnchorStreamEvent>,

    /// Current attachment session (if any)
    attachment: Option<AttachmentSession>,

    /// Last known session identifier for idempotent control-plane operations
    last_session_id: Option<Uuid>,

    /// Prologue metadata from the attached source (if received)
    prologue: Option<PrologueMetadata>,

    /// Metadata
    created_at: Instant,
    type_name: &'static str,
    timeout: Option<Duration>,
    deadline: Option<Instant>,
    attached_deadline: Option<Instant>,
}

/// Metadata from the prologue frame
#[allow(dead_code)]
#[derive(Clone)]
struct PrologueMetadata {
    source_instance_id: InstanceId,
    cancellation_id: Uuid,
    source_endpoint: String,
}

/// Active attachment session
#[allow(dead_code)]
struct AttachmentSession {
    session_id: Uuid,
    source_instance: InstanceId,
    attached_at: Instant,
}

const STREAM_CHANNEL_CAPACITY: usize = 128;
const ATTACHED_HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(15);

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
            inner: Arc::new(AnchorManagerInner {
                anchors: DashMap::new(),
                streaming_transport,
                network_client,
                instance_id,
                stream_endpoint,
                timeout_worker: Mutex::new(None),
            }),
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
    ) -> Result<(ArmedAnchorHandle<T>, ResponseAnchorStream<T>)>
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
            last_session_id: None,
            prologue: None,
            created_at: Instant::now(),
            type_name: type_name::<T>(),
            timeout: None,
            deadline: None,
            attached_deadline: None,
        };

        // Register anchor
        self.inner.anchors.insert(anchor_id, entry);

        // Create receiver for streaming frames
        let receiver = (*self.inner.streaming_transport)
            .create_stream_receiver::<T>(anchor_id)
            .await?;

        // Spawn task to route frames to user stream
        self.spawn_frame_router(anchor_id, receiver);

        // Create handle payload
        let payload = AnchorHandlePayload::new(anchor_id, self.inner.instance_id, worker_address);

        // Create armed handle bound to this manager's client
        let handle =
            ArmedAnchorHandle::from_payload(payload, Arc::clone(&self.inner.network_client));

        // Create user stream
        let stream =
            ResponseAnchorStream::new(anchor_id, rx, type_name::<T>(), Arc::clone(&self.inner));

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
        let inner = Arc::clone(&self.inner);

        tokio::spawn(async move {
            while let Some(frame_result) = receiver.recv().await {
                match frame_result {
                    Ok(frame) => {
                        if let Err(e) = Self::process_frame(&inner, anchor_id, frame).await {
                            error!(anchor_id = %anchor_id, error = %e, "Error processing frame");
                            break;
                        }
                    }
                    Err(e) => {
                        error!(anchor_id = %anchor_id, error = %e, "Error receiving frame");
                        // Send transport error to user stream
                        if let Some(entry) = inner.anchors.get(&anchor_id) {
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
        inner: &Arc<AnchorManagerInner>,
        anchor_id: Uuid,
        frame: StreamFrame<T>,
    ) -> Result<()>
    where
        T: Serialize + Send + 'static,
    {
        let entry = match inner.anchors.get(&anchor_id) {
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
                if let Some(mut entry) = inner.anchors.get_mut(&anchor_id) {
                    entry.prologue = Some(PrologueMetadata {
                        source_instance_id,
                        cancellation_id,
                        source_endpoint,
                    });
                    if let Some(timeout) = entry.timeout {
                        entry.deadline = Some(Instant::now() + timeout);
                    }
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
                    inner.anchors.remove(&anchor_id);
                } else if let Some(mut entry) = inner.anchors.get_mut(&anchor_id) {
                    entry.attached_deadline = Some(Instant::now() + ATTACHED_HEARTBEAT_TIMEOUT);
                    entry.deadline = None;
                }

                AnchorManagerInner::ensure_timeout_worker(inner);
            }

            StreamFrame::Detached => {
                debug!(anchor_id = %anchor_id, "Processing Detached frame");

                let previous_session = entry.attachment.as_ref().map(|s| s.session_id);
                // Mark attachment as None (ready for reattachment)
                drop(entry); // Release the read lock
                if let Some(mut entry) = inner.anchors.get_mut(&anchor_id) {
                    entry.attachment = None;
                    entry.last_session_id = previous_session;
                    entry.attached_deadline = None;
                    if let Some(timeout) = entry.timeout {
                        entry.deadline = Some(Instant::now() + timeout);
                    }
                }

                debug!(anchor_id = %anchor_id, "Anchor detached, ready for reattachment");
                AnchorManagerInner::ensure_timeout_worker(inner);
            }

            StreamFrame::Heartbeat => {
                drop(entry);
                if let Some(mut entry) = inner.anchors.get_mut(&anchor_id) {
                    entry.attached_deadline = Some(Instant::now() + ATTACHED_HEARTBEAT_TIMEOUT);
                    entry.deadline = None;
                }
                AnchorManagerInner::ensure_timeout_worker(inner);
            }

            StreamFrame::Finalized => {
                debug!(anchor_id = %anchor_id, "Processing Finalized frame");

                // Close user stream (drop the sender)
                drop(entry); // Release the read lock

                // Remove from registry
                inner.anchors.remove(&anchor_id);

                debug!(anchor_id = %anchor_id, "Anchor finalized and removed");
            }

            StreamFrame::Dropped => {
                debug!(anchor_id = %anchor_id, "Processing Dropped frame");

                let sender = entry.stream_sender.clone();
                drop(entry);

                if let Err(error) = sender
                    .send(AnchorStreamEvent::TransportError(anyhow!(
                        "source dropped without finalize",
                    )))
                    .await
                {
                    warn!(
                        anchor_id = %anchor_id,
                        error = %error,
                        "Failed to propagate drop error to user stream",
                    );
                }
                inner.anchors.remove(&anchor_id);

                debug!(anchor_id = %anchor_id, "Anchor removed after handle drop");
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

                inner.anchors.remove(&anchor_id);

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

        let mut entry = match self.inner.anchors.get_mut(&anchor_id) {
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
        entry.last_session_id = Some(request.session_id);
        entry.deadline = None;
        entry.attached_deadline = Some(Instant::now() + ATTACHED_HEARTBEAT_TIMEOUT);

        debug!(
            anchor_id = %anchor_id,
            session_id = %request.session_id,
            "Anchor attached"
        );

        AnchorManagerInner::ensure_timeout_worker(&self.inner);

        Ok(AnchorAttachResponse {
            stream_endpoint: self.inner.stream_endpoint.clone(),
        })
    }

    /// Handle a detach request
    pub async fn handle_detach(
        &self,
        request: AnchorDetachRequest,
    ) -> Result<AnchorDetachResponse, String> {
        let anchor_id = request.anchor_id;

        let mut entry = match self.inner.anchors.get_mut(&anchor_id) {
            Some(e) => e,
            None => {
                return Err(DetachError::AnchorNotFound.to_string());
            }
        };

        if let Some(session) = entry.attachment.as_ref() {
            if session.session_id != request.session_id {
                return Err(DetachError::SessionMismatch.to_string());
            }

            entry.last_session_id = Some(session.session_id);
            entry.attachment = None;
            entry.attached_deadline = None;
            if let Some(timeout) = entry.timeout {
                entry.deadline = Some(Instant::now() + timeout);
            }

            debug!(
                anchor_id = %anchor_id,
                session_id = %request.session_id,
                "Anchor detached via control message"
            );

            AnchorManagerInner::ensure_timeout_worker(&self.inner);

            Ok(AnchorDetachResponse { success: true })
        } else if entry.last_session_id == Some(request.session_id) {
            debug!(
                anchor_id = %anchor_id,
                session_id = %request.session_id,
                "Detach request acknowledged after stream update"
            );
            Ok(AnchorDetachResponse { success: true })
        } else {
            Err(DetachError::NotAttached.to_string())
        }
    }

    /// Handle a finalize request
    pub async fn handle_finalize(
        &self,
        request: AnchorFinalizeRequest,
    ) -> Result<AnchorFinalizeResponse, String> {
        let anchor_id = request.anchor_id;

        let entry = match self.inner.anchors.get(&anchor_id) {
            Some(e) => e,
            None => {
                return Err(FinalizeError::AnchorNotFound.to_string());
            }
        };

        if matches!(request.reason, FinalizeReason::Dropped) {
            let prologue = entry.prologue.clone();
            drop(entry);
            if let Some((_, entry)) = self.inner.anchors.remove(&anchor_id) {
                if let Err(error) = entry
                    .stream_sender
                    .try_send(AnchorStreamEvent::TransportError(anyhow!(
                        "anchor handle dropped without explicit finalize"
                    )))
                {
                    warn!(
                        anchor_id = %anchor_id,
                        %error,
                        "Failed to propagate drop finalization to stream",
                    );
                }
                if let Some(prologue) = prologue {
                    AnchorManagerInner::send_source_cancellation(&self.inner, prologue).await;
                }
            }
            debug!(anchor_id = %anchor_id, "Anchor finalized due to dropped handle");
            return Ok(AnchorFinalizeResponse { success: true });
        }

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

        match self.inner.anchors.remove(&anchor_id) {
            Some((_, entry)) => {
                debug!(anchor_id = %anchor_id, "Anchor cancelled by creator");

                // If there's an active attachment with prologue metadata, send cancellation signal
                if let Some(prologue) = entry.prologue {
                    AnchorManagerInner::send_source_cancellation(&self.inner, prologue).await;
                }

                Ok(AnchorCancelResponse { success: true })
            }
            None => Err(CancelError::AnchorNotFound.to_string()),
        }
    }

    /// Get statistics about active anchors
    pub fn stats(&self) -> AnchorStats {
        let total = self.inner.anchors.len();
        let mut attached = 0;

        for entry in self.inner.anchors.iter() {
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

impl AnchorManagerInner {
    fn set_timeout(inner: &Arc<Self>, anchor_id: Uuid, duration: Duration) {
        if let Some(mut entry) = inner.anchors.get_mut(&anchor_id) {
            entry.timeout = Some(duration);
            if entry.attachment.is_none() {
                entry.deadline = Some(Instant::now() + duration);
            }
        }
        Self::ensure_timeout_worker(inner);
    }

    fn ensure_timeout_worker(inner: &Arc<Self>) {
        if let Ok(mut guard) = inner.timeout_worker.lock() {
            if guard.is_none() {
                let inner_clone = Arc::clone(inner);
                let handle = tokio::spawn(async move {
                    Self::timeout_loop(inner_clone).await;
                });
                *guard = Some(handle);
            }
        }
    }

    async fn timeout_loop(inner: Arc<Self>) {
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            inner.expire_due_anchors();

            if !inner.has_pending_deadlines() {
                if let Ok(mut guard) = inner.timeout_worker.lock() {
                    *guard = None;
                }
                break;
            }
        }
    }

    fn expire_due_anchors(&self) {
        let now = Instant::now();
        let mut expired_detached = Vec::new();
        let mut expired_attached = Vec::new();

        for entry in self.anchors.iter() {
            if let Some(deadline) = entry.deadline {
                if deadline <= now {
                    expired_detached.push(*entry.key());
                    continue;
                }
            }

            if let Some(deadline) = entry.attached_deadline {
                if deadline <= now {
                    expired_attached.push(*entry.key());
                }
            }
        }

        for anchor_id in expired_detached {
            if let Some((_, entry)) = self.anchors.remove(&anchor_id) {
                let _ = entry
                    .stream_sender
                    .try_send(AnchorStreamEvent::TransportError(anyhow::anyhow!(
                        "anchor timed out"
                    )));
                debug!(anchor_id = %anchor_id, "Anchor timed out due to inactivity");
            }
        }

        for anchor_id in expired_attached {
            if let Some((_, entry)) = self.anchors.remove(&anchor_id) {
                let _ = entry
                    .stream_sender
                    .try_send(AnchorStreamEvent::TransportError(anyhow::anyhow!(
                        "anchor heartbeat timed out"
                    )));
                debug!(anchor_id = %anchor_id, "Anchor removed due to missed heartbeats");
            }
        }
    }

    fn has_pending_deadlines(&self) -> bool {
        self.anchors
            .iter()
            .any(|entry| entry.deadline.is_some() || entry.attached_deadline.is_some())
    }

    async fn handle_stream_drop(inner: Arc<Self>, anchor_id: Uuid) {
        let entry = inner.anchors.remove(&anchor_id);
        if let Some((_, entry)) = entry {
            if let Some(prologue) = entry.prologue.clone() {
                Self::send_source_cancellation(&inner, prologue).await;
            }
            debug!(anchor_id = %anchor_id, "Anchor stream dropped; anchor removed");
        }
    }

    async fn send_source_cancellation(inner: &Arc<Self>, prologue: PrologueMetadata) {
        let builder = match inner
            .network_client
            .system_active_message("_source_cancel")
            .payload(SourceCancellationSignal::new(prologue.cancellation_id))
        {
            Ok(builder) => builder,
            Err(error) => {
                warn!(
                    source_instance = %prologue.source_instance_id,
                    cancellation_id = %prologue.cancellation_id,
                    %error,
                    "Failed to prepare cancellation signal for source",
                );
                return;
            }
        };

        if let Err(error) = builder
            .target_instance(prologue.source_instance_id)
            .execute()
            .await
        {
            warn!(
                source_instance = %prologue.source_instance_id,
                cancellation_id = %prologue.cancellation_id,
                %error,
                "Failed to deliver cancellation signal to source",
            );
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
    anchor_id: Uuid,
    inner: Arc<AnchorManagerInner>,
    rx: mpsc::Receiver<AnchorStreamEvent>,
    expected_type: &'static str,
    _phantom: PhantomData<T>,
}

impl<T> ResponseAnchorStream<T> {
    fn new(
        anchor_id: Uuid,
        rx: mpsc::Receiver<AnchorStreamEvent>,
        expected_type: &'static str,
        inner: Arc<AnchorManagerInner>,
    ) -> Self {
        Self {
            anchor_id,
            inner,
            rx,
            expected_type,
            _phantom: PhantomData,
        }
    }

    /// Configure an inactivity timeout for this anchor when it is not attached.
    pub fn set_timeout(&self, duration: Duration) {
        AnchorManagerInner::set_timeout(&self.inner, self.anchor_id, duration);
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

impl<T> Drop for ResponseAnchorStream<T> {
    fn drop(&mut self) {
        let inner = Arc::clone(&self.inner);
        let anchor_id = self.anchor_id;

        if let Ok(handle) = Handle::try_current() {
            handle.spawn(async move {
                AnchorManagerInner::handle_stream_drop(inner, anchor_id).await;
            });
        } else {
            std::thread::spawn(move || match Runtime::new() {
                Ok(rt) => {
                    let _ = rt.block_on(async {
                        AnchorManagerInner::handle_stream_drop(inner, anchor_id).await;
                    });
                }
                Err(error) => {
                    warn!(
                        anchor_id = %anchor_id,
                        %error,
                        "Failed to create runtime for stream drop cleanup",
                    );
                }
            });
        }
    }
}
