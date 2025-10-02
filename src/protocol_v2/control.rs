// SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Structured control metadata for the upcoming v2 Active Message pipeline.
//!
//! The goal is to keep user payload bytes pristine while allowing the runtime
//! to reason about delivery semantics, receipts, acknowledgements, and
//! transport-specific hints using strongly-typed fields.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::BTreeMap;
use uuid::Uuid;

use crate::receipt_ack::{ClientExpectation, HandlerType};

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

/// Metadata recorded when the sender expects a receipt acknowledgement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceiptMetadata {
    pub receipt_id: Uuid,
    pub expectation: ClientExpectation,
}

/// Metadata recorded when the sender expects a response payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResponseMetadata {
    pub response_id: Uuid,
    pub accept_id: Option<Uuid>,
}

/// Metadata recorded when the sender expects an explicit acknowledgement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AckMetadata {
    pub ack_id: Uuid,
}

/// Metadata used for internal acceptance notifications.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcceptanceMetadata {
    pub accept_for: Uuid,
}

/// Metadata carried on response messages to correlate them with requests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResponseContextMetadata {
    pub response_to: Uuid,
    pub original_handler: Option<String>,
    pub envelope_format: bool,
}

/// Hints supplied to transports that need additional routing information.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransportHints {
    pub sender_endpoint: Option<String>,
}

/// Structured control metadata accompanying every Active Message.
#[derive(Debug, Clone, PartialEq)]
pub struct ControlMetadata {
    pub mode: DeliveryMode,
    pub accept_id: Option<Uuid>,
    pub response: Option<ResponseMetadata>,
    pub receipt: Option<ReceiptMetadata>,
    pub ack: Option<AckMetadata>,
    pub acceptance: Option<AcceptanceMetadata>,
    pub response_ctx: Option<ResponseContextMetadata>,
    pub transport_hints: TransportHints,
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

    /// Convert the metadata to a JSON object so that legacy transports can ship it
    /// alongside the user payload.
    pub fn to_json(&self) -> Value {
        let mut map = Map::new();
        map.insert(
            "_mode".to_string(),
            Value::String(self.mode.as_str().to_string()),
        );

        if let Some(accept_id) = self.accept_id {
            map.insert(
                "_accept_id".to_string(),
                Value::String(accept_id.to_string()),
            );
        }

        if let Some(response) = &self.response {
            map.insert(
                "_response_id".to_string(),
                Value::String(response.response_id.to_string()),
            );
            if let Some(accept) = response.accept_id {
                map.insert(
                    "_response_accept_id".to_string(),
                    Value::String(accept.to_string()),
                );
            }
        }

        if let Some(receipt) = &self.receipt {
            map.insert(
                "_receipt_id".to_string(),
                Value::String(receipt.receipt_id.to_string()),
            );
            if let Ok(value) = serde_json::to_value(&receipt.expectation) {
                map.insert("_client_expectation".to_string(), value);
            }
        }

        if let Some(ack) = &self.ack {
            map.insert("_ack_id".to_string(), Value::String(ack.ack_id.to_string()));
        }

        if let Some(acceptance) = &self.acceptance {
            map.insert(
                "_accept_for".to_string(),
                Value::String(acceptance.accept_for.to_string()),
            );
        }

        if let Some(response_ctx) = &self.response_ctx {
            map.insert(
                "_response_to".to_string(),
                Value::String(response_ctx.response_to.to_string()),
            );
            if let Some(original) = &response_ctx.original_handler {
                map.insert(
                    "_original_handler".to_string(),
                    Value::String(original.clone()),
                );
            }
            map.insert(
                "_envelope_format".to_string(),
                Value::Bool(response_ctx.envelope_format),
            );
        }

        if let Some(endpoint) = &self.transport_hints.sender_endpoint {
            map.insert(
                "_sender_endpoint".to_string(),
                Value::String(endpoint.clone()),
            );
        }

        for (k, v) in &self.extras {
            map.insert(k.clone(), v.clone());
        }

        Value::Object(map)
    }

    pub fn to_bytes(&self) -> serde_json::Result<Vec<u8>> {
        serde_json::to_vec(&self.to_json())
    }

    pub fn from_json(value: Value) -> Self {
        let mut extras = BTreeMap::new();
        let mut map = match value {
            Value::Object(map) => map,
            Value::Null => Map::new(),
            other => {
                let mut object = Map::new();
                object.insert("_raw".to_string(), other);
                object
            }
        };

        let mode = map
            .remove("_mode")
            .and_then(|v| v.as_str().map(ToOwned::to_owned))
            .and_then(|s| DeliveryMode::from_str(&s))
            .unwrap_or(DeliveryMode::FireAndForget);

        let accept_id = map
            .remove("_accept_id")
            .and_then(|v| v.as_str().and_then(|s| Uuid::parse_str(s).ok()));

        let response = map
            .remove("_response_id")
            .and_then(|v| v.as_str().and_then(|s| Uuid::parse_str(s).ok()))
            .map(|response_id| {
                let accept_id = map
                    .remove("_response_accept_id")
                    .and_then(|v| v.as_str().and_then(|s| Uuid::parse_str(s).ok()));
                ResponseMetadata {
                    response_id,
                    accept_id,
                }
            });

        let receipt = map
            .remove("_receipt_id")
            .and_then(|v| v.as_str().and_then(|s| Uuid::parse_str(s).ok()))
            .map(|receipt_id| {
                let expectation = map
                    .remove("_client_expectation")
                    .and_then(|v| serde_json::from_value::<ClientExpectation>(v).ok())
                    .unwrap_or_else(|| ClientExpectation {
                        expected_handler_type: HandlerType::ActiveMessage,
                        expects_response: false,
                        expected_response_type: None,
                    });
                ReceiptMetadata {
                    receipt_id,
                    expectation,
                }
            });

        let ack = map
            .remove("_ack_id")
            .and_then(|v| v.as_str().and_then(|s| Uuid::parse_str(s).ok()))
            .map(|ack_id| AckMetadata { ack_id });

        let acceptance = map
            .remove("_accept_for")
            .and_then(|v| v.as_str().and_then(|s| Uuid::parse_str(s).ok()))
            .map(|accept_for| AcceptanceMetadata { accept_for });

        let response_ctx = map
            .remove("_response_to")
            .and_then(|v| v.as_str().and_then(|s| Uuid::parse_str(s).ok()))
            .map(|response_to| {
                let original_handler = map
                    .remove("_original_handler")
                    .and_then(|v| v.as_str().map(ToOwned::to_owned));
                let envelope_format = map
                    .remove("_envelope_format")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                ResponseContextMetadata {
                    response_to,
                    original_handler,
                    envelope_format,
                }
            });

        let transport_hints = TransportHints {
            sender_endpoint: map
                .remove("_sender_endpoint")
                .and_then(|v| v.as_str().map(ToOwned::to_owned)),
        };

        for (k, v) in map {
            extras.insert(k, v);
        }

        ControlMetadata {
            mode,
            accept_id,
            response,
            receipt,
            ack,
            acceptance,
            response_ctx,
            transport_hints,
            extras,
        }
    }

    pub fn from_bytes(bytes: &[u8]) -> serde_json::Result<Self> {
        let value: Value = serde_json::from_slice(bytes)?;
        Ok(Self::from_json(value))
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
        let value = control.to_json();
        assert_eq!(value["_mode"], Value::String("fire_and_forget".into()));
        let decoded = ControlMetadata::from_json(value);
        assert_eq!(decoded.mode, DeliveryMode::FireAndForget);
    }

    #[test]
    fn confirmed_json_round_trip() {
        let accept_id = Uuid::new_v4();
        let control = ControlMetadata::confirmed(accept_id);
        let value = control.to_json();
        assert_eq!(value["_mode"], Value::String("confirmed".into()));
        assert_eq!(value["_accept_id"], Value::String(accept_id.to_string()));
        let decoded = ControlMetadata::from_json(value);
        assert_eq!(decoded.accept_id, Some(accept_id));
        assert_eq!(decoded.mode, DeliveryMode::Confirmed);
    }

    #[test]
    fn with_response_json_round_trip() {
        let accept_id = Uuid::new_v4();
        let response_id = Uuid::new_v4();
        let control = ControlMetadata::with_response(accept_id, response_id);
        let value = control.to_json();
        assert_eq!(value["_mode"], Value::String("with_response".into()));
        assert_eq!(value["_accept_id"], Value::String(accept_id.to_string()));
        assert_eq!(
            value["_response_id"],
            Value::String(response_id.to_string())
        );
        let decoded = ControlMetadata::from_json(value);
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
        let value = control.to_json();
        assert_eq!(value["_mode"], Value::String("with_receipt_ack".into()));
        assert_eq!(value["_receipt_id"], Value::String(receipt_id.to_string()));
        let decoded = ControlMetadata::from_json(value);
        assert_eq!(
            decoded.receipt.as_ref().map(|m| m.receipt_id),
            Some(receipt_id)
        );
    }

    #[test]
    fn sender_endpoint_round_trip() {
        let mut control = ControlMetadata::fire_and_forget();
        control.set_sender_endpoint("tcp://127.0.0.1:5555");
        let value = control.to_json();
        assert_eq!(
            value["_sender_endpoint"],
            Value::String("tcp://127.0.0.1:5555".into())
        );
        let decoded = ControlMetadata::from_json(value);
        assert_eq!(decoded.sender_endpoint(), Some("tcp://127.0.0.1:5555"));
    }
}
