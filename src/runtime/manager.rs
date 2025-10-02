// SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;

use crate::api::client::ActiveMessageClient;
use crate::api::handler::HandlerEvent;
use crate::protocol::message::HandlerId;

use super::dispatcher::{ActiveMessageDispatcher, ControlMessage};

/// Internal trait for transport-specific manager implementations.
/// Users should not interact with this trait directly - use the concrete
/// `ActiveMessageManager` struct instead.
#[async_trait]
pub(crate) trait ManagerTransport: Send + Sync {
    fn client(&self) -> Arc<dyn ActiveMessageClient>;

    async fn peer_info(&self) -> crate::api::client::PeerInfo;

    fn control_tx(&self) -> &mpsc::Sender<ControlMessage>;

    async fn register_handler(
        &self,
        name: String,
        handler: Arc<dyn ActiveMessageDispatcher>,
    ) -> Result<()>;

    async fn deregister_handler(&self, name: &str) -> Result<()>;

    async fn list_handlers(&self) -> Vec<HandlerId>;

    fn handler_events(&self) -> broadcast::Receiver<HandlerEvent>;

    async fn shutdown(&self) -> Result<()>;

    fn cancel_token(&self) -> CancellationToken;
}

/// Concrete Active Message Manager that manages distributed messaging.
///
/// This is the main entry point for the Active Message system. It wraps
/// transport-specific implementations (like ZMQ) and provides a clean,
/// transport-agnostic API.
///
/// Use `ActiveMessageManagerBuilder` to create an instance.
pub struct ActiveMessageManager {
    transport: Box<dyn ManagerTransport>,
}

impl ActiveMessageManager {
    /// Create a new manager with the given transport implementation.
    /// This is typically called by the builder, not directly by users.
    pub(crate) fn new(transport: Box<dyn ManagerTransport>) -> Self {
        Self { transport }
    }

    /// Get the client for sending messages.
    pub fn client(&self) -> Arc<dyn ActiveMessageClient> {
        self.transport.client()
    }

    /// Get PeerInfo representing this manager (instance ID and endpoints).
    pub async fn peer_info(&self) -> crate::api::client::PeerInfo {
        self.transport.peer_info().await
    }

    /// Get the control channel sender for dispatcher control messages.
    /// This is needed for cohort.register_handlers() and other advanced use cases.
    pub fn control_tx(&self) -> &mpsc::Sender<ControlMessage> {
        self.transport.control_tx()
    }

    /// Register a handler with the message dispatcher.
    pub async fn register_handler(
        &self,
        name: String,
        handler: Arc<dyn ActiveMessageDispatcher>,
    ) -> Result<()> {
        self.transport.register_handler(name, handler).await
    }

    /// Deregister a handler by name.
    pub async fn deregister_handler(&self, name: &str) -> Result<()> {
        self.transport.deregister_handler(name).await
    }

    /// List all registered handlers.
    pub async fn list_handlers(&self) -> Vec<HandlerId> {
        self.transport.list_handlers().await
    }

    /// Subscribe to handler lifecycle events.
    pub fn handler_events(&self) -> broadcast::Receiver<HandlerEvent> {
        self.transport.handler_events()
    }

    /// Shutdown the manager and all its background tasks.
    pub async fn shutdown(&self) -> Result<()> {
        self.transport.shutdown().await
    }

    /// Get the root cancellation token driving background tasks.
    pub fn cancel_token(&self) -> CancellationToken {
        self.transport.cancel_token()
    }
}
