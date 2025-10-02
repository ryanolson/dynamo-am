// SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Thin ZMQ transport implementation optimized for fast send path.
//!
//! This module provides a clean implementation of the ThinTransport trait for ZMQ.
//! Key design principles:
//! - Connection returns an mpsc::Sender for the endpoint
//! - Serialization happens on caller's thread
//! - Transport workers just write pre-serialized bytes to sockets
//! - No routing logic, no peer management

use anyhow::Result;
use async_trait::async_trait;
use dashmap::DashMap;
use futures::SinkExt;
use std::collections::VecDeque;
use std::sync::Arc;
use tmq::{Context, Message, Multipart, push};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, error};

use crate::{
    handler::ActiveMessage,
    transport::{ThinTransport, TransportType},
};

/// ZMQ-specific wire format (multipart message)
pub type ZmqWireFormat = Multipart;

/// Connection entry for ZMQ endpoint
type ZmqConnection = (mpsc::Sender<ZmqWireFormat>, Arc<JoinHandle<()>>);

/// Thin ZMQ transport that implements the ThinTransport trait.
///
/// This transport:
/// - Spawns a dedicated worker per endpoint
/// - Serializes on caller's thread via serialize_to_wire()
/// - Workers just write pre-serialized Multipart messages to sockets
#[derive(Clone)]
pub struct ZmqThinTransport {
    context: Arc<Context>,
    local_endpoint: Option<String>,
    transport_type: TransportType,

    /// Map of endpoint -> (sender channel, worker handle)
    /// Using DashMap for lock-free concurrent access
    connections: Arc<DashMap<String, ZmqConnection>>,
}

impl std::fmt::Debug for ZmqThinTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ZmqThinTransport")
            .field("local_endpoint", &self.local_endpoint)
            .field("transport_type", &self.transport_type)
            .field("active_connections", &self.connections.len())
            .finish()
    }
}

impl ZmqThinTransport {
    /// Create a new ZMQ thin transport
    pub fn new() -> Self {
        Self {
            context: Arc::new(Context::new()),
            local_endpoint: None,
            transport_type: TransportType::Tcp,
            connections: Arc::new(DashMap::new()),
        }
    }

    /// Create with a specific transport type
    pub fn with_type(transport_type: TransportType) -> Self {
        Self {
            context: Arc::new(Context::new()),
            local_endpoint: None,
            transport_type,
            connections: Arc::new(DashMap::new()),
        }
    }

    /// Bind to a local address for receiving
    pub async fn bind(&mut self, address: &str) -> Result<String> {
        // This would create a PULL socket for receiving
        // For now, just track the endpoint
        self.local_endpoint = Some(address.to_string());
        Ok(address.to_string())
    }

    /// Dedicated transport worker that writes to a ZMQ PUSH socket
    async fn transport_worker(
        endpoint: String,
        context: Arc<Context>,
        mut rx: mpsc::Receiver<ZmqWireFormat>,
    ) {
        debug!("Starting ZMQ transport worker for endpoint: {}", endpoint);

        // Create and connect the PUSH socket
        let mut socket = match push(&context).connect(&endpoint) {
            Ok(s) => s,
            Err(e) => {
                error!("Failed to connect PUSH socket to {}: {}", endpoint, e);
                return;
            }
        };

        debug!("ZMQ worker connected to {}", endpoint);

        // Dumb worker loop: just write pre-serialized messages to socket
        while let Some(multipart) = rx.recv().await {
            if let Err(e) = socket.send(multipart).await {
                error!("Failed to send to {}: {}", endpoint, e);
                // Continue processing - don't fail the whole worker
            }
        }

        debug!("ZMQ transport worker for {} shutting down", endpoint);
    }
}

impl Default for ZmqThinTransport {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ThinTransport for ZmqThinTransport {
    type WireFormat = ZmqWireFormat;

    async fn connect(&self, endpoint: &str) -> Result<mpsc::Sender<Self::WireFormat>> {
        // Check if we already have a connection
        if let Some(entry) = self.connections.get(endpoint) {
            debug!("Reusing existing connection to {}", endpoint);
            return Ok(entry.value().0.clone());
        }

        // Create new channel for this endpoint
        let (tx, rx) = mpsc::channel(100); // Buffer size: 100 messages

        // Spawn dedicated transport worker
        let context = self.context.clone();
        let endpoint_owned = endpoint.to_string();
        let handle = tokio::spawn(Self::transport_worker(endpoint_owned.clone(), context, rx));

        // Store in connections map
        self.connections
            .insert(endpoint.to_string(), (tx.clone(), Arc::new(handle)));

        debug!("Created new ZMQ connection to {}", endpoint);

        Ok(tx)
    }

    fn serialize_to_wire(&self, message: &ActiveMessage) -> Result<Self::WireFormat> {
        // Serialize on caller's thread - this is the hot path optimization
        let mut parts = VecDeque::new();

        // Part 1: Metadata (everything except payload)
        let metadata = serde_json::json!({
            "message_id": message.message_id,
            "handler_name": message.handler_name,
            "sender_instance": message.sender_instance,
            "metadata": message.metadata,
        });
        parts.push_back(Message::from(serde_json::to_vec(&metadata)?));

        // Part 2: Raw payload bytes (zero-copy)
        parts.push_back(Message::from(message.payload.as_ref()));

        Ok(Multipart(parts))
    }

    async fn disconnect(&self, endpoint: &str) -> Result<()> {
        if let Some((_, (tx, handle))) = self.connections.remove(endpoint) {
            // Drop the sender to signal worker to shut down
            drop(tx);

            // Wait for worker to finish
            // Need to get inner handle from Arc
            if let Ok(handle_inner) = Arc::try_unwrap(handle) {
                handle_inner.abort();
                let _ = handle_inner.await;
            }

            debug!("Disconnected from {}", endpoint);
        }
        Ok(())
    }

    fn transport_type(&self) -> TransportType {
        self.transport_type
    }

    fn local_endpoint(&self) -> Option<&str> {
        self.local_endpoint.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_zmq_thin_transport_creation() {
        let transport = ZmqThinTransport::new();
        assert!(transport.local_endpoint().is_none());
        assert_eq!(transport.transport_type(), TransportType::Tcp);
    }

    #[tokio::test]
    async fn test_serialize_to_wire() {
        let transport = ZmqThinTransport::new();

        let message = ActiveMessage::new(
            Uuid::new_v4(),
            "test_handler".to_string(),
            Uuid::new_v4(),
            Bytes::from("test payload"),
            serde_json::Value::Null,
        );

        let wire_format = transport.serialize_to_wire(&message).unwrap();

        // Should have 2 parts: metadata + payload
        assert_eq!(wire_format.len(), 2);
    }
}
