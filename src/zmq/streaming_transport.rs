// SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! ZMQ-based streaming transport for response anchors
//!
//! This module implements the StreamingTransport trait using ZeroMQ's
//! ROUTER-DEALER pattern with a proxy for local and remote connections.
//!
//! Architecture:
//! ```text
//! Anchor DEALER <--inproc--> Local ROUTER <--inproc--> Remote ROUTER (TCP) <--tcp--> Source DEALER
//! ```
//!
//! Frame format:
//! ```text
//! [anchor_id (16 bytes)] [session_id (16 bytes)] [serialized StreamFrame<T>]
//! ```

use anyhow::Result;
use async_trait::async_trait;
use dashmap::DashMap;
use futures::SinkExt;
use futures::stream::StreamExt;
use serde::{Serialize, de::DeserializeOwned};
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::sync::Arc;
use tmq::{
    AsZmqSocket, Context, Message, Multipart,
    dealer::{Dealer, dealer},
    router::router,
};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, error, warn};
use uuid::Uuid;

use crate::protocols::response_anchor::StreamFrame;
use crate::transport::streaming::{StreamReceiver, StreamSink, StreamingTransport};

/// ZMQ-based streaming transport
///
/// This transport uses a ROUTER-ROUTER proxy pattern to support both
/// local (inproc) and remote (TCP) connections.
#[derive(Clone)]
pub struct ZmqStreamingTransport {
    context: Arc<Context>,
    local_router_endpoint: String,

    /// The actual bound remote endpoint (stored after proxy starts)
    remote_router_endpoint: Arc<tokio::sync::Mutex<Option<String>>>,

    /// Map of anchor_id -> receiver channel
    /// When a receiver is created for an anchor, we store the sender here
    receivers: Arc<DashMap<Uuid, mpsc::UnboundedSender<Multipart>>>,

    /// Proxy task handle (spawned when first needed)
    proxy_handle: Arc<tokio::sync::Mutex<Option<JoinHandle<()>>>>,
}

impl std::fmt::Debug for ZmqStreamingTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ZmqStreamingTransport")
            .field("local_router_endpoint", &self.local_router_endpoint)
            .field("remote_router_endpoint", &self.remote_router_endpoint)
            .field("active_receivers", &self.receivers.len())
            .finish()
    }
}

impl ZmqStreamingTransport {
    /// Create a new ZMQ streaming transport
    ///
    /// # Arguments
    /// * `instance_id` - Unique identifier for this instance (used for inproc endpoint)
    pub fn new(instance_id: Uuid) -> Self {
        let local_router_endpoint = format!("inproc://anchors-{}", instance_id);

        Self {
            context: Arc::new(Context::new()),
            local_router_endpoint,
            remote_router_endpoint: Arc::new(tokio::sync::Mutex::new(None)),
            receivers: Arc::new(DashMap::new()),
            proxy_handle: Arc::new(tokio::sync::Mutex::new(None)),
        }
    }

    /// Start the proxy if not already running
    /// Returns the bound remote endpoint
    async fn ensure_proxy_started(&self, remote_endpoint: &str) -> Result<String> {
        // Check if we already have the endpoint cached
        {
            let endpoint_guard = self.remote_router_endpoint.lock().await;
            if let Some(ref endpoint) = *endpoint_guard {
                return Ok(endpoint.clone());
            }
        }

        // Need to start proxy
        let mut handle_guard = self.proxy_handle.lock().await;

        if handle_guard.is_none() {
            debug!("Starting ZMQ streaming proxy");
            let (handle, bound_endpoint) = self.start_proxy(remote_endpoint).await?;
            *handle_guard = Some(handle);

            // Store the bound endpoint
            let mut endpoint_guard = self.remote_router_endpoint.lock().await;
            *endpoint_guard = Some(bound_endpoint.clone());

            Ok(bound_endpoint)
        } else {
            // Proxy already running (race condition), get the endpoint
            let endpoint_guard = self.remote_router_endpoint.lock().await;
            endpoint_guard
                .clone()
                .ok_or_else(|| anyhow::anyhow!("Proxy running but endpoint not stored"))
        }
    }

    /// Start the ROUTER-ROUTER proxy
    ///
    /// This spawns a task that forwards messages between the local ROUTER
    /// (for inproc connections) and the remote ROUTER (for TCP connections).
    /// Returns the task handle and the actual bound endpoint.
    async fn start_proxy(&self, remote_endpoint: &str) -> Result<(JoinHandle<()>, String)> {
        let context = self.context.clone();
        let local_endpoint = self.local_router_endpoint.clone();
        let receivers = self.receivers.clone();

        // Bind the routers first to get the actual endpoints
        debug!("Binding local router to {}", local_endpoint);
        let local_router = router(&context).bind(&local_endpoint)?;

        debug!("Binding remote router to {}", remote_endpoint);
        let remote_router = router(&context).bind(remote_endpoint)?;

        // Get the actual bound endpoint
        let bound_endpoint = remote_router
            .get_socket()
            .get_last_endpoint()
            .expect("Failed to retrieve bound endpoint")
            .expect("Socket did not report bound endpoint");

        debug!("Remote router bound to {}", bound_endpoint);

        let handle = tokio::spawn(async move {
            if let Err(e) = Self::proxy_task(local_router, remote_router, receivers).await {
                error!("Streaming proxy task failed: {}", e);
            }
        });

        Ok((handle, bound_endpoint))
    }

    /// Proxy task that forwards messages between routers
    async fn proxy_task(
        mut local_router: tmq::router::Router,
        mut remote_router: tmq::router::Router,
        receivers: Arc<DashMap<Uuid, mpsc::UnboundedSender<Multipart>>>,
    ) -> Result<()> {
        debug!("Streaming proxy started");

        loop {
            tokio::select! {
                // Messages from local DEALER sockets (anchors)
                Some(msg) = local_router.next() => {
                    if let Ok(multipart) = msg {
                        Self::route_message(&receivers, multipart).await;
                    }
                }

                // Messages from remote DEALER sockets (sources)
                Some(msg) = remote_router.next() => {
                    if let Ok(multipart) = msg {
                        Self::route_message(&receivers, multipart).await;
                    }
                }

                else => {
                    debug!("Streaming proxy shutting down");
                    break;
                }
            }
        }

        Ok(())
    }

    /// Route a message to the appropriate receiver based on anchor_id
    async fn route_message(
        receivers: &DashMap<Uuid, mpsc::UnboundedSender<Multipart>>,
        mut multipart: Multipart,
    ) {
        // Expected format: [identity][anchor_id][session_id][frame_data]
        // The identity frame is added by ROUTER, so we need to strip it

        if multipart.len() < 4 {
            warn!("Received malformed multipart message (too few frames)");
            return;
        }

        // Skip identity frame (first frame from ROUTER)
        let _ = multipart.pop_front();

        // Extract anchor_id
        let anchor_id_bytes = match multipart.pop_front() {
            Some(msg) => msg,
            None => {
                warn!("Missing anchor_id frame");
                return;
            }
        };

        let anchor_id = match Uuid::from_slice(&anchor_id_bytes) {
            Ok(id) => id,
            Err(e) => {
                warn!("Invalid anchor_id: {}", e);
                return;
            }
        };

        // Forward to the receiver for this anchor
        if let Some(sender) = receivers.get(&anchor_id) {
            if let Err(e) = sender.send(multipart) {
                warn!("Failed to forward message to anchor {}: {}", anchor_id, e);
            }
        } else {
            debug!("No receiver registered for anchor {}", anchor_id);
        }
    }
}

#[async_trait]
impl StreamingTransport for ZmqStreamingTransport {
    async fn bind_stream_endpoint(&self) -> Result<String> {
        // Bind to a TCP port for remote connections
        // Use port 0 to get an OS-assigned port
        let endpoint = "tcp://127.0.0.1:0".to_string();

        // Start the proxy (this will bind the remote router and return the actual endpoint)
        let bound_endpoint = self.ensure_proxy_started(&endpoint).await?;

        Ok(bound_endpoint)
    }

    async fn create_stream_source<T>(
        &self,
        endpoint: &str,
        anchor_id: Uuid,
        session_id: Uuid,
    ) -> Result<Box<dyn StreamSink<T> + Send + 'static>>
    where
        T: Serialize + Send + 'static,
    {
        let context = self.context.clone();
        let endpoint = endpoint.to_string();

        let sink = ZmqStreamSink::<T>::new(context, endpoint, anchor_id, session_id).await?;

        Ok(Box::new(sink))
    }

    async fn create_stream_receiver<T>(
        &self,
        anchor_id: Uuid,
    ) -> Result<Box<dyn StreamReceiver<T> + Send + 'static>>
    where
        T: DeserializeOwned + Send + 'static,
    {
        // Create a channel for receiving messages
        let (tx, rx) = mpsc::unbounded_channel();

        // Register this receiver
        self.receivers.insert(anchor_id, tx);

        let receiver = ZmqStreamReceiver::<T>::new(rx, anchor_id, self.receivers.clone());

        Ok(Box::new(receiver))
    }
}

// ============================================================================
// ZmqStreamSink - Sends frames to a remote anchor
// ============================================================================

struct ZmqStreamSink<T> {
    socket: Dealer,
    anchor_id: Uuid,
    session_id: Uuid,
    _phantom: PhantomData<T>,
}

impl<T> ZmqStreamSink<T> {
    async fn new(
        context: Arc<Context>,
        endpoint: String,
        anchor_id: Uuid,
        session_id: Uuid,
    ) -> Result<Self> {
        let socket = dealer(&context).connect(&endpoint)?;

        debug!(
            "Created stream sink to {} for anchor {}",
            endpoint, anchor_id
        );

        Ok(Self {
            socket,
            anchor_id,
            session_id,
            _phantom: PhantomData,
        })
    }

    fn create_multipart(&self, frame_data: Vec<u8>) -> Multipart {
        let mut parts = VecDeque::new();

        // Frame format: [anchor_id][session_id][frame_data]
        parts.push_back(Message::from(self.anchor_id.as_bytes().to_vec()));
        parts.push_back(Message::from(self.session_id.as_bytes().to_vec()));
        parts.push_back(Message::from(frame_data));

        Multipart(parts)
    }
}

#[async_trait]
impl<T: Serialize + Send> StreamSink<T> for ZmqStreamSink<T> {
    async fn send(&mut self, frame: StreamFrame<T>) -> Result<()> {
        // Serialize the frame
        let frame_data = serde_json::to_vec(&frame)
            .map_err(|e| anyhow::anyhow!("Failed to serialize frame: {}", e))?;

        // Create multipart message
        let multipart = self.create_multipart(frame_data);

        // Send to socket
        self.socket
            .send(multipart)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to send frame: {}", e))?;

        Ok(())
    }

    async fn close(mut self: Box<Self>) -> Result<()> {
        debug!("Closing stream sink for anchor {}", self.anchor_id);
        // Socket will be closed when dropped
        Ok(())
    }
}

// ============================================================================
// ZmqStreamReceiver - Receives frames for an anchor
// ============================================================================

struct ZmqStreamReceiver<T> {
    rx: mpsc::UnboundedReceiver<Multipart>,
    anchor_id: Uuid,
    receivers: Arc<DashMap<Uuid, mpsc::UnboundedSender<Multipart>>>,
    _phantom: PhantomData<T>,
}

impl<T> ZmqStreamReceiver<T> {
    fn new(
        rx: mpsc::UnboundedReceiver<Multipart>,
        anchor_id: Uuid,
        receivers: Arc<DashMap<Uuid, mpsc::UnboundedSender<Multipart>>>,
    ) -> Self {
        debug!("Created stream receiver for anchor {}", anchor_id);

        Self {
            rx,
            anchor_id,
            receivers,
            _phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<T: DeserializeOwned + Send> StreamReceiver<T> for ZmqStreamReceiver<T> {
    async fn recv(&mut self) -> Option<Result<StreamFrame<T>>> {
        // Wait for next multipart message
        let mut multipart = self.rx.recv().await?;

        // Expected format: [session_id][frame_data]
        // (anchor_id was already stripped by route_message)

        if multipart.len() < 2 {
            return Some(Err(anyhow::anyhow!("Malformed multipart message")));
        }

        // Skip session_id for now (could validate it later)
        let _ = multipart.pop_front();

        // Get frame data
        let frame_data = match multipart.pop_front() {
            Some(msg) => msg,
            None => return Some(Err(anyhow::anyhow!("Missing frame data"))),
        };

        // Deserialize the frame
        match serde_json::from_slice::<StreamFrame<T>>(&frame_data) {
            Ok(frame) => Some(Ok(frame)),
            Err(e) => Some(Err(anyhow::anyhow!("Failed to deserialize frame: {}", e))),
        }
    }
}

impl<T> Drop for ZmqStreamReceiver<T> {
    fn drop(&mut self) {
        // Unregister this receiver
        self.receivers.remove(&self.anchor_id);
        debug!("Unregistered stream receiver for anchor {}", self.anchor_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_zmq_streaming_transport_creation() {
        let instance_id = Uuid::new_v4();
        let transport = ZmqStreamingTransport::new(instance_id);

        assert_eq!(
            transport.local_router_endpoint,
            format!("inproc://anchors-{}", instance_id)
        );
    }

    #[tokio::test]
    async fn test_bind_stream_endpoint() {
        let instance_id = Uuid::new_v4();
        let transport = ZmqStreamingTransport::new(instance_id);

        let endpoint = transport.bind_stream_endpoint().await.unwrap();
        assert!(endpoint.starts_with("tcp://"));
    }
}
