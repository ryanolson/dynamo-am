// SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Active Message handler trait and implementations

use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::api::client::ActiveMessageClient;
use crate::protocol::message::InstanceId;

/// Message metadata and execution context passed to handlers
#[derive(Debug)]
pub struct ActiveMessageContext {
    /// Message ID
    pub message_id: Uuid,
    /// Instance ID of the sender
    pub sender_instance: Uuid,
    /// Name of the target handler
    pub handler_name: String,
    /// Message metadata
    pub metadata: serde_json::Value,
    /// Client for sending messages
    client: Arc<dyn ActiveMessageClient>,
    /// Optional cancellation token for cancellable handlers
    cancel_token: Option<CancellationToken>,
}

impl ActiveMessageContext {
    /// Create a new message context
    pub fn new(
        message_id: Uuid,
        sender_instance: Uuid,
        handler_name: String,
        metadata: serde_json::Value,
        client: Arc<dyn ActiveMessageClient>,
        cancel_token: Option<CancellationToken>,
    ) -> Self {
        Self {
            message_id,
            sender_instance,
            handler_name,
            metadata,
            client,
            cancel_token,
        }
    }

    /// Get the message ID
    pub fn message_id(&self) -> Uuid {
        self.message_id
    }

    /// Get the sender instance ID
    pub fn sender_instance(&self) -> Uuid {
        self.sender_instance
    }

    /// Check if this message has been cancelled
    pub fn is_cancelled(&self) -> bool {
        self.cancel_token.as_ref().is_some_and(|t| t.is_cancelled())
    }

    /// Get cancellation token for spawning cancellable tasks
    pub fn cancel_token(&self) -> Option<&CancellationToken> {
        self.cancel_token.as_ref()
    }

    /// Get the handler name
    pub fn handler_name(&self) -> &str {
        &self.handler_name
    }

    /// Get message metadata
    pub fn metadata(&self) -> &serde_json::Value {
        &self.metadata
    }

    /// Get the client for sending messages
    pub fn client(&self) -> &Arc<dyn ActiveMessageClient> {
        &self.client
    }
}

/// Events emitted during handler operations
#[derive(Debug, Clone)]
pub enum HandlerEvent {
    /// Handler was registered
    Registered { name: String, instance: InstanceId },
    /// Handler was deregistered
    Deregistered { name: String, instance: InstanceId },
}
