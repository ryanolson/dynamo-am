// SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! In-memory Active Message envelope for the v2 pipeline.

use anyhow::Result;
use bytes::Bytes;
use serde::de::DeserializeOwned;
use uuid::Uuid;

use super::control::ControlMetadata;
use crate::handler as legacy;

/// Unique identifier for a runtime instance.
pub type InstanceId = Uuid;

/// Identifier for a registered handler.
pub type HandlerId = String;

/// Header information carried by every Active Message.
#[derive(Debug, Clone)]
pub struct MessageHeader {
    pub message_id: Uuid,
    pub handler_name: String,
    pub sender_instance: InstanceId,
    pub control: ControlMetadata,
}

impl MessageHeader {
    pub fn new(
        message_id: Uuid,
        handler_name: impl Into<String>,
        sender_instance: InstanceId,
        control: ControlMetadata,
    ) -> Self {
        Self {
            message_id,
            handler_name: handler_name.into(),
            sender_instance,
            control,
        }
    }
}

/// Active Message envelope with immutable user payload bytes.
#[derive(Debug, Clone)]
pub struct ActiveMessage {
    pub header: MessageHeader,
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
            header: MessageHeader::new(message_id, handler_name, sender_instance, control),
            payload,
        }
    }

    pub fn message_id(&self) -> Uuid {
        self.header.message_id
    }

    pub fn handler_name(&self) -> &str {
        &self.header.handler_name
    }

    pub fn sender_instance(&self) -> InstanceId {
        self.header.sender_instance
    }

    pub fn control(&self) -> &ControlMetadata {
        &self.header.control
    }

    pub fn control_mut(&mut self) -> &mut ControlMetadata {
        &mut self.header.control
    }

    pub fn into_parts(self) -> (MessageHeader, Bytes) {
        (self.header, self.payload)
    }

    pub fn from_parts(header: MessageHeader, payload: Bytes) -> Self {
        Self { header, payload }
    }

    pub fn deserialize_payload<T: DeserializeOwned>(&self) -> Result<T> {
        Ok(serde_json::from_slice(&self.payload)?)
    }
}

/// Conversion helpers between the legacy JSON-based envelope and the new
/// structured metadata representation. These are temporary and will be removed
/// once the runtime fully migrates to the v2 pipeline.
impl From<legacy::ActiveMessage> for ActiveMessage {
    fn from(v1: legacy::ActiveMessage) -> Self {
        let control = ControlMetadata::from_json(v1.metadata);
        ActiveMessage::new(
            v1.message_id,
            v1.handler_name,
            v1.sender_instance,
            v1.payload,
            control,
        )
    }
}

impl From<ActiveMessage> for legacy::ActiveMessage {
    fn from(message: ActiveMessage) -> Self {
        let (header, payload) = message.into_parts();
        legacy::ActiveMessage::new(
            header.message_id,
            header.handler_name,
            header.sender_instance,
            payload,
            header.control.to_json(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::receipt_ack::ClientExpectation;

    #[test]
    fn json_round_trip() {
        let control =
            ControlMetadata::with_receipt(Uuid::new_v4(), ClientExpectation::active_message());
        let message = ActiveMessage::new(
            Uuid::new_v4(),
            "echo",
            Uuid::new_v4(),
            Bytes::from_static(b"hello"),
            control,
        );

        let legacy: legacy::ActiveMessage = message.clone().into();
        let round_trip: ActiveMessage = legacy.into();
        assert_eq!(round_trip.message_id(), message.message_id());
        assert_eq!(round_trip.payload, message.payload);
    }
}
