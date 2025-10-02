// SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Structured control metadata describing delivery semantics and response expectations.
//!
//! The goal is to keep user payload bytes pristine while allowing the runtime
//! to reason about delivery semantics, receipts, acknowledgements, and
//! transport-specific hints using strongly-typed fields.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use uuid::Uuid;

use crate::receipt_ack::ClientExpectation;

/// Delivery semantics requested by the sender.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryMode {
    FireAndForget,
    Confirmed,
    WithResponse,
    WithReceiptAck,
    WithReceiptAndResponse,
}

impl DeliveryMode {
    pub fn as_str(self) -> &'static str {
        match self {
            DeliveryMode::FireAndForget => "fire_and_forget",
            DeliveryMode::Confirmed => "confirmed",
            DeliveryMode::WithResponse => "with_response",
            DeliveryMode::WithReceiptAck => "with_receipt_ack",
            DeliveryMode::WithReceiptAndResponse => "with_receipt_and_response",
        }
    }

    pub fn from_str(value: &str) -> Option<Self> {
        match value {
            "fire_and_forget" => Some(DeliveryMode::FireAndForget),
            "confirmed" => Some(DeliveryMode::Confirmed),
            "with_response" => Some(DeliveryMode::WithResponse),
            "with_receipt_ack" => Some(DeliveryMode::WithReceiptAck),
            "with_receipt_and_response" => Some(DeliveryMode::WithReceiptAndResponse),
            _ => None,
        }
    }
}

impl Default for DeliveryMode {
    fn default() -> Self {
        DeliveryMode::FireAndForget
    }
}

/// Metadata recorded when the sender expects a receipt acknowledgement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReceiptMetadata {
    pub receipt_id: Uuid,
    pub expectation: ClientExpectation,
}

/// Metadata recorded when the sender expects a response payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResponseMetadata {
    pub response_id: Uuid,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub accept_id: Option<Uuid>,
}

/// Metadata recorded when the sender expects an explicit acknowledgement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AckMetadata {
    pub ack_id: Uuid,
}

/// Metadata used for internal acceptance notifications.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AcceptanceMetadata {
    pub accept_for: Uuid,
}

/// Metadata carried on response messages to correlate them with requests.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResponseContextMetadata {
    pub response_to: Uuid,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub original_handler: Option<String>,
    #[serde(default)]
    pub envelope_format: bool,
}

/// Hints supplied to transports that need additional routing information.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransportHints {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sender_endpoint: Option<String>,
}

impl TransportHints {
    fn is_empty(&self) -> bool {
        self.sender_endpoint.is_none()
    }
}

fn extras_is_empty(map: &BTreeMap<String, Value>) -> bool {
    map.is_empty()
}

/// Structured control metadata accompanying every Active Message.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ControlMetadata {
    #[serde(default)]
    pub mode: DeliveryMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub accept_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response: Option<ResponseMetadata>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub receipt: Option<ReceiptMetadata>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ack: Option<AckMetadata>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acceptance: Option<AcceptanceMetadata>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_ctx: Option<ResponseContextMetadata>,
    #[serde(default, skip_serializing_if = "TransportHints::is_empty")]
    pub transport_hints: TransportHints,
    #[serde(default, skip_serializing_if = "extras_is_empty", flatten)]
    pub extras: BTreeMap<String, Value>,
}

impl ControlMetadata {
    pub fn fire_and_forget() -> Self {
        Self {
            mode: DeliveryMode::FireAndForget,
            accept_id: None,
            response: None,
            receipt: None,
            ack: None,
            acceptance: None,
            response_ctx: None,
            transport_hints: TransportHints::default(),
            extras: BTreeMap::new(),
        }
    }

    pub fn confirmed(accept_id: Uuid) -> Self {
        Self {
            mode: DeliveryMode::Confirmed,
            accept_id: Some(accept_id),
            response: None,
            receipt: None,
            ack: None,
            acceptance: None,
            response_ctx: None,
            transport_hints: TransportHints::default(),
            extras: BTreeMap::new(),
        }
    }

    pub fn with_response(accept_id: Uuid, response_id: Uuid) -> Self {
        Self {
            mode: DeliveryMode::WithResponse,
            accept_id: Some(accept_id),
            response: Some(ResponseMetadata {
                response_id,
                accept_id: Some(accept_id),
            }),
            receipt: None,
            ack: None,
            acceptance: None,
            response_ctx: None,
            transport_hints: TransportHints::default(),
            extras: BTreeMap::new(),
        }
    }

    pub fn with_receipt(receipt_id: Uuid, expectation: ClientExpectation) -> Self {
        Self {
            mode: DeliveryMode::WithReceiptAck,
            accept_id: None,
            response: None,
            receipt: Some(ReceiptMetadata {
                receipt_id,
                expectation,
            }),
            ack: None,
            acceptance: None,
            response_ctx: None,
            transport_hints: TransportHints::default(),
            extras: BTreeMap::new(),
        }
    }

    pub fn with_receipt_and_response(
        receipt_id: Uuid,
        response_id: Uuid,
        expectation: ClientExpectation,
    ) -> Self {
        Self {
            mode: DeliveryMode::WithReceiptAndResponse,
            accept_id: None,
            response: Some(ResponseMetadata {
                response_id,
                accept_id: None,
            }),
            receipt: Some(ReceiptMetadata {
                receipt_id,
                expectation,
            }),
            ack: None,
            acceptance: None,
            response_ctx: None,
            transport_hints: TransportHints::default(),
            extras: BTreeMap::new(),
        }
    }

    pub fn set_sender_endpoint(&mut self, endpoint: impl Into<String>) {
        self.transport_hints.sender_endpoint = Some(endpoint.into());
    }

    pub fn sender_endpoint(&self) -> Option<&str> {
        self.transport_hints.sender_endpoint.as_deref()
    }

    pub fn set_ack(&mut self, ack_id: Uuid) {
        self.ack = Some(AckMetadata { ack_id });
    }

    pub fn ack(&self) -> Option<Uuid> {
        self.ack.as_ref().map(|meta| meta.ack_id)
    }

    pub fn set_acceptance(&mut self, accept_for: Uuid) {
        self.acceptance = Some(AcceptanceMetadata { accept_for });
    }

    pub fn acceptance(&self) -> Option<Uuid> {
        self.acceptance.as_ref().map(|meta| meta.accept_for)
    }

    pub fn set_response_context(
        &mut self,
        response_to: Uuid,
        original_handler: Option<String>,
        envelope_format: bool,
    ) {
        self.response_ctx = Some(ResponseContextMetadata {
            response_to,
            original_handler,
            envelope_format,
        });
    }

    pub fn response_context(&self) -> Option<&ResponseContextMetadata> {
        self.response_ctx.as_ref()
    }

    pub fn insert_extra(&mut self, key: impl Into<String>, value: Value) {
        self.extras.insert(key.into(), value);
    }

    pub fn extra(&self, key: &str) -> Option<&Value> {
        self.extras.get(key)
    }

    pub fn from_value(value: Value) -> serde_json::Result<Self> {
        serde_json::from_value(value)
    }

    pub fn to_value(&self) -> serde_json::Result<Value> {
        serde_json::to_value(self)
    }
}

impl Default for ControlMetadata {
    fn default() -> Self {
        Self::fire_and_forget()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fire_and_forget_json_round_trip() {
        let control = ControlMetadata::fire_and_forget();
        let value = control.to_value().unwrap();
        assert_eq!(value["mode"], Value::String("fire_and_forget".into()));
        let decoded = ControlMetadata::from_value(value).unwrap();
        assert_eq!(decoded.mode, DeliveryMode::FireAndForget);
    }

    #[test]
    fn confirmed_json_round_trip() {
        let accept_id = Uuid::new_v4();
        let control = ControlMetadata::confirmed(accept_id);
        let value = control.to_value().unwrap();
        assert_eq!(value["mode"], Value::String("confirmed".into()));
        assert_eq!(value["accept_id"], Value::String(accept_id.to_string()));
        let decoded = ControlMetadata::from_value(value).unwrap();
        assert_eq!(decoded.accept_id, Some(accept_id));
        assert_eq!(decoded.mode, DeliveryMode::Confirmed);
    }

    #[test]
    fn with_response_json_round_trip() {
        let accept_id = Uuid::new_v4();
        let response_id = Uuid::new_v4();
        let control = ControlMetadata::with_response(accept_id, response_id);
        let value = control.to_value().unwrap();
        assert_eq!(value["mode"], Value::String("with_response".into()));
        assert_eq!(value["accept_id"], Value::String(accept_id.to_string()));
        assert_eq!(
            value["response"]["response_id"],
            Value::String(response_id.to_string())
        );
        let decoded = ControlMetadata::from_value(value).unwrap();
        assert_eq!(
            decoded.response.as_ref().map(|m| m.response_id),
            Some(response_id)
        );
        assert_eq!(decoded.accept_id, Some(accept_id));
    }

    #[test]
    fn with_receipt_json_round_trip() {
        let receipt_id = Uuid::new_v4();
        let control =
            ControlMetadata::with_receipt(receipt_id, ClientExpectation::active_message());
        let value = control.to_value().unwrap();
        assert_eq!(value["mode"], Value::String("with_receipt_ack".into()));
        assert_eq!(
            value["receipt"]["receipt_id"],
            Value::String(receipt_id.to_string())
        );
        let decoded = ControlMetadata::from_value(value).unwrap();
        assert_eq!(
            decoded.receipt.as_ref().map(|m| m.receipt_id),
            Some(receipt_id)
        );
    }

    #[test]
    fn sender_endpoint_round_trip() {
        let mut control = ControlMetadata::fire_and_forget();
        control.set_sender_endpoint("tcp://127.0.0.1:5555");
        let value = control.to_value().unwrap();
        assert_eq!(
            value["transport_hints"]["sender_endpoint"],
            Value::String("tcp://127.0.0.1:5555".into())
        );
        let decoded = ControlMetadata::from_value(value).unwrap();
        assert_eq!(decoded.sender_endpoint(), Some("tcp://127.0.0.1:5555"));
    }
}
