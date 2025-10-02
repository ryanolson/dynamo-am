// SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! In-memory Active Message envelope used throughout the runtime.

use anyhow::Result;
use bytes::Bytes;
use serde::de::DeserializeOwned;
use uuid::Uuid;

use super::control::ControlMetadata;

/// Unique identifier for a runtime instance.
pub type InstanceId = Uuid;

/// Identifier for a registered handler.
pub type HandlerId = String;

/// Active Message envelope with immutable user payload bytes.
#[derive(Debug, Clone)]
pub struct ActiveMessage {
    pub message_id: Uuid,
    pub handler_name: String,
    pub sender_instance: InstanceId,
    pub control: ControlMetadata,
    pub payload: Bytes,
}

impl ActiveMessage {
    pub fn new(
        message_id: Uuid,
        handler_name: impl Into<String>,
        sender_instance: InstanceId,
        payload: Bytes,
        control: ControlMetadata,
    ) -> Self {
        Self {
            message_id,
            handler_name: handler_name.into(),
            sender_instance,
            control,
            payload,
        }
    }

    pub fn message_id(&self) -> Uuid {
        self.message_id
    }

    pub fn handler_name(&self) -> &str {
        &self.handler_name
    }

    pub fn sender_instance(&self) -> InstanceId {
        self.sender_instance
    }

    pub fn control(&self) -> &ControlMetadata {
        &self.control
    }

    pub fn control_mut(&mut self) -> &mut ControlMetadata {
        &mut self.control
    }

    pub fn deserialize_payload<T: DeserializeOwned>(&self) -> Result<T> {
        Ok(serde_json::from_slice(&self.payload)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_message() {
        let control = ControlMetadata::fire_and_forget();
        let message = ActiveMessage::new(
            Uuid::new_v4(),
            "echo",
            Uuid::new_v4(),
            Bytes::from_static(b"hello"),
            control.clone(),
        );

        assert_eq!(message.handler_name(), "echo");
        assert_eq!(message.control(), &control);
    }
}
