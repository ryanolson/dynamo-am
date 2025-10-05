// SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Public API for Response Anchors
//!
//! This module provides the user-facing types for working with response anchors:
//! - `ResponseAnchorHandle`: Serializable handle for identifying anchors
//! - `ResponseAnchorSource`: Entry point for attaching to remote anchors
//! - `ResponseSink`: Active connection for streaming data to an anchor

use anyhow::Result;
use serde::Serialize;
use std::marker::PhantomData;
use std::sync::Arc;
use uuid::Uuid;

use crate::api::client::ActiveMessageClient;
use crate::protocols::response_anchor::*;
use crate::runtime::network_client::NetworkClient;
use crate::transport::streaming::{StreamSink, StreamingTransport};
use tokio::runtime::Handle;
use tracing::warn;

// Re-export the handle type from protocols
pub use crate::protocols::response_anchor::ResponseAnchorHandle;

/// Entry point for attaching to a remote response anchor
///
/// This type provides the static method for initiating an attachment
/// to a response anchor identified by a handle.
pub struct ResponseAnchorSource<T> {
    _phantom: PhantomData<T>,
}

impl<T: Serialize + Send + 'static> ResponseAnchorSource<T> {
    /// Attach to a remote response anchor
    ///
    /// This sends an attach request via ActiveMessage and, if successful,
    /// establishes a streaming connection for sending data frames.
    ///
    /// # Arguments
    /// * `handle` - Handle to the remote anchor (contains anchor_id, instance_id, address)
    /// * `client` - NetworkClient for sending control messages
    ///
    /// # Returns
    /// A `ResponseSink<T>` that can send data items to the anchor.
    ///
    /// # Errors
    /// Returns an error if:
    /// - The anchor doesn't exist
    /// - The anchor is already attached
    /// - The anchor was cancelled
    /// - Network communication fails
    pub async fn attach(
        handle: ResponseAnchorHandle,
        client: Arc<NetworkClient>,
    ) -> Result<ResponseSink<T>> {
        let session_id = Uuid::new_v4();
        let source_instance_id = client.as_ref().instance_id();
        let source_endpoint = client.as_ref().endpoint().to_string();

        // Send attach request via ActiveMessage
        let request = AnchorAttachRequest::new(
            handle.anchor_id,
            session_id,
            source_instance_id,
            source_endpoint.clone(),
        );

        // Use system_active_message for system handler
        let status = client
            .as_ref()
            .system_active_message("_anchor_attach")
            .expect_response::<AnchorAttachResponse>()
            .payload(request)?
            .send(handle.instance_id)
            .await?;

        // Wait for response
        let response: AnchorAttachResponse = status.await_response().await?;

        let stream_endpoint = response.stream_endpoint;

        // Create the streaming sink
        let mut stream_sink = match client
            .as_ref()
            .streaming_transport()
            .create_stream_source::<T>(&stream_endpoint, handle.anchor_id, session_id)
            .await
        {
            Ok(sink) => sink,
            Err(error) => {
                Self::detach_after_failed_attach(client.as_ref(), &handle, session_id).await;
                return Err(error);
            }
        };

        // Generate cancellation ID and send prologue frame
        let cancellation_id = Uuid::new_v4();

        // TODO: Register cancellation_id with cancellation handler
        // This will be done when we integrate with the cancellation system

        let prologue_frame = StreamFrame::Prologue {
            source_instance_id,
            cancellation_id,
            source_endpoint: source_endpoint.clone(),
        };

        if let Err(error) = stream_sink.send(prologue_frame).await {
            if let Err(close_err) = stream_sink.close().await {
                warn!(
                    anchor_id = %handle.anchor_id,
                    session_id = %session_id,
                    error = %close_err,
                    "Failed to close stream sink after prologue error",
                );
            }
            Self::detach_after_failed_attach(client.as_ref(), &handle, session_id).await;
            return Err(error);
        }

        // Create the sink
        let sink = ResponseSink {
            handle,
            session_id,
            source_instance_id,
            source_endpoint,
            stream_endpoint,
            client,
            stream_sink: Some(stream_sink),
            _phantom: PhantomData,
        };

        Ok(sink)
    }
}

impl<T> ResponseAnchorSource<T> {
    async fn detach_after_failed_attach(
        client: &NetworkClient,
        handle: &ResponseAnchorHandle,
        session_id: Uuid,
    ) {
        let request = AnchorDetachRequest::new(handle.anchor_id, session_id);

        let detach_result = async move {
            let status = client
                .system_active_message("_anchor_detach")
                .expect_response::<AnchorDetachResponse>()
                .payload(request)?
                .send(handle.instance_id)
                .await?;

            let _: AnchorDetachResponse = status.await_response().await?;
            anyhow::Result::<()>::Ok(())
        }
        .await;

        if let Err(error) = detach_result {
            warn!(
                anchor_id = %handle.anchor_id,
                session_id = %session_id,
                error = %error,
                "Failed to detach after aborted attach",
            );
        }
    }
}

impl<T: Serialize + Send + 'static> Drop for ResponseSink<T> {
    fn drop(&mut self) {
        let Some(mut sink) = self.stream_sink.take() else {
            return;
        };

        let Ok(handle) = Handle::try_current() else {
            warn!(
                anchor_id = %self.handle.anchor_id,
                session_id = %self.session_id,
                "ResponseSink dropped outside Tokio runtime; leaving anchor attached",
            );
            return;
        };

        let anchor_id = self.handle.anchor_id;
        let session_id = self.session_id;
        let instance_id = self.handle.instance_id;
        let client = self.client.clone();

        handle.spawn(async move {
            if let Err(error) = sink
                .send(StreamFrame::transport_error(
                    "response sink dropped without detach/finalize",
                ))
                .await
            {
                warn!(
                    anchor_id = %anchor_id,
                    session_id = %session_id,
                    error = %error,
                    "Failed to send transport-error sentinel during ResponseSink drop",
                );
            }

            if let Err(error) = sink.close().await {
                warn!(
                    anchor_id = %anchor_id,
                    session_id = %session_id,
                    error = %error,
                    "Failed to close stream sink during ResponseSink drop",
                );
            }

            let detach_result = async {
                let request = AnchorDetachRequest::new(anchor_id, session_id);
                let status = client
                    .system_active_message("_anchor_detach")
                    .expect_response::<AnchorDetachResponse>()
                    .payload(request)?
                    .send(instance_id)
                    .await?;

                let _: AnchorDetachResponse = status.await_response().await?;
                anyhow::Result::<()>::Ok(())
            }
            .await;

            if let Err(error) = detach_result {
                warn!(
                    anchor_id = %anchor_id,
                    session_id = %session_id,
                    error = %error,
                    "Failed to issue detach after ResponseSink drop",
                );
            }
        });
    }
}

/// Active streaming connection to a response anchor
///
/// This type allows sending data items to an attached anchor.
/// The sink can be detached (releasing the lock but keeping the anchor alive)
/// or finalized (closing the anchor's stream permanently).
pub struct ResponseSink<T>
where
    T: Serialize + Send + 'static,
{
    handle: ResponseAnchorHandle,
    session_id: Uuid,
    source_instance_id: uuid::Uuid,
    source_endpoint: String,
    stream_endpoint: String,
    client: Arc<NetworkClient>,
    stream_sink: Option<Box<dyn StreamSink<T> + Send + 'static>>,
    _phantom: PhantomData<T>,
}

impl<T: Serialize + Send + 'static> ResponseSink<T> {
    async fn send_control_frame(&mut self, frame: StreamFrame<T>) -> Result<()> {
        if let Some(mut sink) = self.stream_sink.take() {
            if let Err(error) = sink.send(frame).await {
                // Put sink back so drop can try a best-effort cleanup
                self.stream_sink = Some(sink);
                return Err(error);
            }

            if let Err(error) = sink.close().await {
                // We already sent the sentinel; log and move on
                warn!(
                    anchor_id = %self.handle.anchor_id,
                    session_id = %self.session_id,
                    error = %error,
                    "Failed to close stream sink after sending control frame",
                );
            }
        }

        Ok(())
    }

    async fn send_detach_control(&self) -> Result<()> {
        let request = AnchorDetachRequest::new(self.handle.anchor_id, self.session_id);

        let status = self
            .client
            .as_ref()
            .system_active_message("_anchor_detach")
            .expect_response::<AnchorDetachResponse>()
            .payload(request)?
            .send(self.handle.instance_id)
            .await?;

        let _: AnchorDetachResponse = status.await_response().await?;

        Ok(())
    }

    async fn send_finalize_control(&self) -> Result<()> {
        let request = AnchorFinalizeRequest::new(self.handle.anchor_id, self.session_id);

        let status = self
            .client
            .as_ref()
            .system_active_message("_anchor_finalize")
            .expect_response::<AnchorFinalizeResponse>()
            .payload(request)?
            .send(self.handle.instance_id)
            .await?;

        let _: AnchorFinalizeResponse = status.await_response().await?;

        Ok(())
    }

    /// Send a successful item to the anchor
    ///
    /// # Errors
    /// Returns an error if serialization or the underlying transport fails.
    pub async fn send_ok(&mut self, item: T) -> Result<()> {
        self.send_frame(StreamFrame::ok(item)).await
    }

    /// Send an application-level error to the anchor
    ///
    /// The error string is surfaced to the consumer of the anchor stream.
    pub async fn send_err(&mut self, error: impl Into<String>) -> Result<()> {
        let message = error.into();
        self.send_frame(StreamFrame::err(message)).await
    }

    #[inline]
    async fn send_frame(&mut self, frame: StreamFrame<T>) -> Result<()> {
        if let Some(ref mut sink) = self.stream_sink {
            sink.send(frame).await
        } else {
            Err(anyhow::anyhow!("Stream sink not initialized"))
        }
    }

    /// Detach from the anchor (release lock, keep stream open)
    ///
    /// This sends a Detached sentinel frame to the anchor, which:
    /// 1. Ensures all previous data items are processed
    /// 2. Releases the exclusive attachment lock
    /// 3. Allows another source to attach
    ///
    /// The anchor's user stream remains open and can continue receiving
    /// data from a new attachment.
    ///
    /// # Errors
    /// Returns an error if the detach control message fails.
    pub async fn detach(mut self) -> Result<()> {
        self.send_control_frame(StreamFrame::Detached).await?;
        self.send_detach_control().await
    }

    /// Finalize the anchor (close stream permanently)
    ///
    /// This sends a Finalized sentinel frame to the anchor, which:
    /// 1. Ensures all previous data items are processed
    /// 2. Closes the user's stream (returns None after last item)
    /// 3. Removes the anchor from the registry
    ///
    /// After finalization, the anchor cannot be reattached.
    ///
    /// # Errors
    /// Returns an error if the finalize control message fails.
    pub async fn finalize(mut self) -> Result<()> {
        self.send_control_frame(StreamFrame::Finalized).await?;
        self.send_finalize_control().await
    }

    /// Get the anchor ID
    pub fn anchor_id(&self) -> Uuid {
        self.handle.anchor_id
    }

    /// Get the session ID
    pub fn session_id(&self) -> Uuid {
        self.session_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_handle_serialization() {
        use crate::api::client::WorkerAddress;

        let handle = ResponseAnchorHandle::new(
            Uuid::new_v4(),
            Uuid::new_v4(),
            WorkerAddress::tcp("tcp://localhost:5555".to_string()),
        );

        let json = serde_json::to_string(&handle).unwrap();
        let deserialized: ResponseAnchorHandle = serde_json::from_str(&json).unwrap();

        assert_eq!(handle.anchor_id, deserialized.anchor_id);
        assert_eq!(handle.instance_id, deserialized.instance_id);
    }
}
