// SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Type-erased transport wrappers for concrete client types.
//!
//! This module provides type-erased versions of ThinTransport and ConnectionHandle
//! that allow the ActiveMessageClient to be a concrete type without generic parameters.

use anyhow::Result;
use async_trait::async_trait;
use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use tokio::sync::mpsc;

use super::handler::{ActiveMessage, InstanceId};
use super::transport::{ThinTransport, TransportType};

/// Type-erased message that can be sent through any transport
type BoxedMessage = Box<dyn Any + Send>;

/// Type-erased sender channel that works with any transport
pub struct BoxedSender {
    inner: Box<dyn ErasedSender>,
}

impl BoxedSender {
    pub fn new<T: Send + 'static>(sender: mpsc::Sender<T>) -> Self {
        Self {
            inner: Box::new(TypedSender { sender }),
        }
    }

    pub fn try_send(&self, msg: BoxedMessage) -> Result<()> {
        self.inner.try_send_boxed(msg)
    }
}

impl Clone for BoxedSender {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone_box(),
        }
    }
}

impl fmt::Debug for BoxedSender {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoxedSender").finish_non_exhaustive()
    }
}

/// Internal trait for type-erased senders
trait ErasedSender: Send + Sync {
    fn try_send_boxed(&self, msg: BoxedMessage) -> Result<()>;
    fn clone_box(&self) -> Box<dyn ErasedSender>;
}

/// Concrete implementation of ErasedSender for a specific type
struct TypedSender<T: Send + 'static> {
    sender: mpsc::Sender<T>,
}

impl<T: Send + 'static> ErasedSender for TypedSender<T> {
    fn try_send_boxed(&self, msg: BoxedMessage) -> Result<()> {
        let typed_msg = msg
            .downcast::<T>()
            .map_err(|_| anyhow::anyhow!("Failed to downcast message to expected type"))?;

        self.sender
            .try_send(*typed_msg)
            .map_err(|e| anyhow::anyhow!("Failed to send to transport channel: {}", e))
    }

    fn clone_box(&self) -> Box<dyn ErasedSender> {
        Box::new(TypedSender {
            sender: self.sender.clone(),
        })
    }
}

/// Type-erased connection handle
#[derive(Clone, Debug)]
pub struct BoxedConnectionHandle {
    /// Instance ID of the connected peer
    pub instance_id: InstanceId,

    /// Primary sender for the best/preferred endpoint
    pub primary_sender: BoxedSender,

    /// Alternative senders for failover
    pub alt_senders: HashMap<u8, BoxedSender>,

    /// The endpoint currently being used
    pub active_endpoint: String,
}

impl BoxedConnectionHandle {
    /// Create a new connection handle with just a primary sender
    pub fn new(instance_id: InstanceId, primary_sender: BoxedSender, endpoint: String) -> Self {
        Self {
            instance_id,
            primary_sender,
            alt_senders: HashMap::new(),
            active_endpoint: endpoint,
        }
    }

    /// Get the primary sender for fast-path sending
    pub fn sender(&self) -> &BoxedSender {
        &self.primary_sender
    }

    /// Try to send with failover to alternative senders
    pub async fn send_with_failover(&self, msg: BoxedMessage) -> Result<()> {
        // Try primary first - need to clone for fallback attempts
        // This is slightly less efficient than the typed version but maintains API
        if self.primary_sender.try_send(msg).is_ok() {
            return Ok(());
        }

        // For now, no alt sender failover in type-erased version
        // Could be added if needed by cloning message multiple times
        anyhow::bail!("Primary sender failed for instance {}", self.instance_id)
    }
}

/// Type-erased transport wrapper
pub struct BoxedTransport {
    inner: Arc<dyn ErasedTransport>,
}

impl BoxedTransport {
    /// Wrap a concrete transport implementation
    pub fn new<W: Send + 'static>(transport: Arc<dyn ThinTransport<WireFormat = W>>) -> Self {
        Self {
            inner: Arc::new(TypedTransport { transport }),
        }
    }

    /// Establish a connection and return type-erased sender
    pub async fn connect(&self, endpoint: &str) -> Result<BoxedSender> {
        self.inner.connect_erased(endpoint).await
    }

    /// Serialize and send a message
    pub fn serialize_and_send(&self, message: &ActiveMessage, sender: &BoxedSender) -> Result<()> {
        let boxed_msg = self.inner.serialize_to_boxed(message)?;
        sender.try_send(boxed_msg)
    }

    /// Disconnect from an endpoint
    pub async fn disconnect(&self, endpoint: &str) -> Result<()> {
        self.inner.disconnect_erased(endpoint).await
    }

    /// Get transport type
    pub fn transport_type(&self) -> TransportType {
        self.inner.transport_type_erased()
    }

    /// Get local endpoint
    pub fn local_endpoint(&self) -> Option<String> {
        self.inner.local_endpoint_erased()
    }
}

impl fmt::Debug for BoxedTransport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoxedTransport")
            .field("transport_type", &self.transport_type())
            .finish()
    }
}

/// Internal trait for type-erased transports
#[async_trait]
trait ErasedTransport: Send + Sync {
    async fn connect_erased(&self, endpoint: &str) -> Result<BoxedSender>;
    fn serialize_to_boxed(&self, message: &ActiveMessage) -> Result<BoxedMessage>;
    async fn disconnect_erased(&self, endpoint: &str) -> Result<()>;
    fn transport_type_erased(&self) -> TransportType;
    fn local_endpoint_erased(&self) -> Option<String>;
}

/// Concrete implementation that wraps a typed transport
struct TypedTransport<W: Send + 'static> {
    transport: Arc<dyn ThinTransport<WireFormat = W>>,
}

#[async_trait]
impl<W: Send + 'static> ErasedTransport for TypedTransport<W> {
    async fn connect_erased(&self, endpoint: &str) -> Result<BoxedSender> {
        let typed_sender = self.transport.connect(endpoint).await?;
        Ok(BoxedSender::new(typed_sender))
    }

    fn serialize_to_boxed(&self, message: &ActiveMessage) -> Result<BoxedMessage> {
        let wire_format = self.transport.serialize_to_wire(message)?;
        Ok(Box::new(wire_format))
    }

    async fn disconnect_erased(&self, endpoint: &str) -> Result<()> {
        self.transport.disconnect(endpoint).await
    }

    fn transport_type_erased(&self) -> TransportType {
        self.transport.transport_type()
    }

    fn local_endpoint_erased(&self) -> Option<String> {
        self.transport.local_endpoint().map(|s| s.to_string())
    }
}
