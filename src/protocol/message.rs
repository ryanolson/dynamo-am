// SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Serde-compatible message structures used across transports.

use anyhow::Result;
use bytes::Bytes;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Unique identifier for a manager or handler instance.
pub type InstanceId = Uuid;

/// Identifier for a registered handler.
pub type HandlerId = String;

/// The raw Active Message payload exchanged over the wire.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActiveMessage {
    /// Unique identifier for this message.
    pub message_id: Uuid,
    /// Name of the target handler.
    pub handler_name: String,
    /// Instance ID of the sender.
    pub sender_instance: Uuid,
    /// Message payload.
    pub payload: Bytes,
    /// Optional metadata.
    pub metadata: serde_json::Value,
}

impl ActiveMessage {
    /// Create a new active message.
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

    /// Helper for tests that generates identifiers automatically.
    pub fn test(handler_name: impl Into<String>, payload: Bytes) -> Self {
        Self::new(
            Uuid::new_v4(),
            handler_name.into(),
            Uuid::new_v4(),
            payload,
            serde_json::Value::Null,
        )
    }

    /// Deserialize the payload as JSON into the requested type.
    pub fn deserialize<T: DeserializeOwned>(&self) -> Result<T> {
        serde_json::from_slice(&self.payload).map_err(Into::into)
    }
}
