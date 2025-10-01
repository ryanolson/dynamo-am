// SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Transport abstraction for active message system.
//!
//! This module provides both high-level and low-level transport abstractions:
//! - `Transport`: High-level transport that works with ActiveMessage structs (legacy)
//! - `RawTransport`: Low-level transport that only handles raw bytes (preferred)
//!
//! The RawTransport trait represents a pure transport abstraction that:
//! - Only deals with raw bytes, not business logic
//! - Handles connection lifecycle and message delivery
//! - Can be implemented for any underlying transport (ZMQ, TCP, HTTP, etc.)

use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashMap;
use tokio::sync::mpsc;

use super::handler::{ActiveMessage, InstanceId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportType {
    Tcp,
    Ipc,
}

/// Thin transport trait optimized for fast send path.
///
/// This trait represents the ideal transport abstraction where:
/// - Connection establishment returns an mpsc::Sender handle
/// - Serialization happens on caller's thread via serialize_and_send()
/// - Transport workers are dumb - they just write pre-serialized bytes
/// - No routing logic, no peer management in the transport layer
#[async_trait]
pub trait ThinTransport: Send + Sync + std::fmt::Debug {
    /// Opaque wire format type specific to this transport.
    /// For ZMQ: Multipart message
    /// For TCP: Framed bytes
    /// For HTTP: Serialized request body
    type WireFormat: Send + 'static;

    /// Establish a connection to an endpoint and return a sender channel.
    /// The sender is used to push pre-serialized messages.
    /// Returns a channel that feeds a dedicated transport worker.
    async fn connect(&self, endpoint: &str) -> Result<tokio::sync::mpsc::Sender<Self::WireFormat>>;

    /// Serialize an ActiveMessage to transport-specific wire format on the caller's thread.
    /// This allows the caller to do the CPU work of serialization before sending.
    fn serialize_to_wire(&self, message: &ActiveMessage) -> Result<Self::WireFormat>;

    /// Convenience method: serialize on caller's thread and send via the channel.
    /// This is the hot path - just a serialize + try_send.
    fn serialize_and_send(
        &self,
        message: &ActiveMessage,
        sender: &tokio::sync::mpsc::Sender<Self::WireFormat>,
    ) -> Result<()> {
        // Serialize on caller's thread
        let wire_format = self.serialize_to_wire(message)?;

        // Just push to channel - transport worker handles the rest
        sender
            .try_send(wire_format)
            .map_err(|e| anyhow::anyhow!("Failed to send to transport channel: {}", e))?;

        Ok(())
    }

    /// Disconnect from an endpoint and clean up associated resources.
    async fn disconnect(&self, endpoint: &str) -> Result<()>;

    /// Get the transport type.
    fn transport_type(&self) -> TransportType;

    /// Get the local endpoint this transport is bound to.
    fn local_endpoint(&self) -> Option<&str>;
}

/// Handle to an established connection with a peer.
///
/// This struct holds the mpsc::Sender channels for communicating with
/// a specific peer. All messages to the same InstanceId go through the
/// same sender to maintain ordering guarantees.
#[derive(Clone, Debug)]
pub struct ConnectionHandle<WireFormat: Send + 'static> {
    /// Instance ID of the connected peer
    pub instance_id: InstanceId,

    /// Primary sender for the best/preferred endpoint (e.g., IPC for same-host)
    pub primary_sender: mpsc::Sender<WireFormat>,

    /// Alternative senders for failover (e.g., TCP fallback)
    /// Priority determines order: lower number = higher priority
    pub alt_senders: HashMap<u8, mpsc::Sender<WireFormat>>,

    /// The endpoint currently being used
    pub active_endpoint: String,
}

impl<WireFormat: Send + 'static> ConnectionHandle<WireFormat> {
    /// Create a new connection handle with just a primary sender
    pub fn new(
        instance_id: InstanceId,
        primary_sender: mpsc::Sender<WireFormat>,
        endpoint: String,
    ) -> Self {
        Self {
            instance_id,
            primary_sender,
            alt_senders: HashMap::new(),
            active_endpoint: endpoint,
        }
    }

    /// Create a connection handle with both primary and alternative senders
    pub fn with_alternatives(
        instance_id: InstanceId,
        primary_sender: mpsc::Sender<WireFormat>,
        endpoint: String,
        alt_senders: HashMap<u8, mpsc::Sender<WireFormat>>,
    ) -> Self {
        Self {
            instance_id,
            primary_sender,
            alt_senders,
            active_endpoint: endpoint,
        }
    }

    /// Get the primary sender for fast-path sending
    pub fn sender(&self) -> &mpsc::Sender<WireFormat> {
        &self.primary_sender
    }

    /// Try to send with failover to alternative senders
    pub async fn send_with_failover(&self, msg: WireFormat) -> Result<()>
    where
        WireFormat: Clone,
    {
        // Try primary first
        if self.primary_sender.send(msg.clone()).await.is_ok() {
            return Ok(());
        }

        // Try alternatives in priority order
        let mut priorities: Vec<_> = self.alt_senders.keys().copied().collect();
        priorities.sort();

        for priority in priorities {
            if let Some(sender) = self.alt_senders.get(&priority)
                && sender.send(msg.clone()).await.is_ok()
            {
                return Ok(());
            }
        }

        anyhow::bail!("All senders failed for instance {}", self.instance_id)
    }
}
