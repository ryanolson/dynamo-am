// SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Active Message handler trait and implementations

use anyhow::Result;
use bytes::Bytes;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::client::ActiveMessageClient;

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

/// The original raw active message structure for transport
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActiveMessage {
    /// Unique identifier for this message
    pub message_id: Uuid,
    /// Name of the target handler
    pub handler_name: String,
    /// Instance ID of the sender
    pub sender_instance: Uuid,
    /// Message payload
    pub payload: Bytes,
    /// Optional metadata
    pub metadata: serde_json::Value,
}

impl ActiveMessage {
    /// Create a new active message
    pub fn new(
        message_id: Uuid,
        handler_name: String,
        sender_instance: Uuid,
        payload: Bytes,
        metadata: serde_json::Value,
    ) -> Self {
        Self {
            message_id,
            handler_name,
            sender_instance,
            payload,
            metadata,
        }
    }

    /// Create an ActiveMessage with default values for testing
    pub fn test(handler_name: impl Into<String>, payload: Bytes) -> Self {
        Self::new(
            Uuid::new_v4(),
            handler_name.into(),
            Uuid::new_v4(),
            payload,
            serde_json::Value::Null,
        )
    }

    /// Deserialize the payload as JSON
    pub fn deserialize<T: DeserializeOwned>(&self) -> Result<T> {
        serde_json::from_slice(&self.payload).map_err(Into::into)
    }
}

pub type InstanceId = Uuid;
pub type HandlerId = String;

/// Events emitted during handler operations
#[derive(Debug, Clone)]
pub enum HandlerEvent {
    /// Handler was registered
    Registered { name: String, instance: InstanceId },
    /// Handler was deregistered
    Deregistered { name: String, instance: InstanceId },
}
