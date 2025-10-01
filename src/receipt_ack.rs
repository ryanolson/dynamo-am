// SPDX-FileCopyrightText: Copyright (c) 2024-2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Receipt acknowledgment types and protocol for the active message system.
//!
//! This module implements the receipt ACK protocol that ensures message delivery
//! confirmation before execution, with contract validation between client expectations
//! and server capabilities.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Receipt acknowledgment message sent immediately upon message delivery.
///
/// This is sent by the dispatcher to confirm that a message has been:
/// 1. Successfully received
/// 2. Validated for handler existence
/// 3. Validated for contract compatibility
/// 4. Queued for execution
///
/// The receipt ACK is sent BEFORE handler execution begins.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReceiptAck {
    /// ID of the message being acknowledged
    pub message_id: Uuid,

    /// Status of the receipt
    pub status: ReceiptStatus,

    /// Contract information (only included on successful delivery)
    pub contract_info: Option<ContractInfo>,
}

/// Status of message receipt
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ReceiptStatus {
    /// Message delivered successfully, will be executed
    Delivered,

    /// Handler type doesn't match client expectations
    ContractMismatch(String),

    /// Handler doesn't exist on this instance
    HandlerNotFound,

    /// Message was malformed or couldn't be parsed
    InvalidMessage(String),
}

/// Contract information describing handler capabilities
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ContractInfo {
    /// Type of handler
    pub handler_type: HandlerType,

    /// Expected response type for unary handlers (None for active message handlers)
    pub response_type: Option<String>,

    /// Whether the handler supports the requested operation mode
    pub supports_operation: bool,
}

/// Type of message handler
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum HandlerType {
    /// Active message handler
    /// No response expected, handler may send 0 or more new messages
    ActiveMessage,

    /// Unary handler returning raw bytes
    /// Always sends exactly one response with arbitrary bytes
    UnaryBytes,

    /// Typed unary handler with automatic serialization
    /// Always sends exactly one response with typed data
    UnaryTyped {
        /// Rust type name for the response (for debugging/validation)
        response_type: String,
    },
}

impl HandlerType {
    /// Check if this handler type expects a response
    pub fn expects_response(&self) -> bool {
        match self {
            HandlerType::ActiveMessage => false,
            HandlerType::UnaryBytes | HandlerType::UnaryTyped { .. } => true,
        }
    }

    /// Get a human-readable description of the handler type
    pub fn description(&self) -> &'static str {
        match self {
            HandlerType::ActiveMessage => "active message (no response expected)",
            HandlerType::UnaryBytes => "unary handler (raw bytes response)",
            HandlerType::UnaryTyped { .. } => "typed unary handler (structured response)",
        }
    }
}

/// Client expectations for a message
///
/// This is derived from the message metadata and builder configuration
/// to validate against server handler capabilities.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientExpectation {
    /// What type of handler the client thinks it's calling
    pub expected_handler_type: HandlerType,

    /// Whether the client is expecting a response
    pub expects_response: bool,

    /// Expected response type (if any)
    pub expected_response_type: Option<String>,
}

impl ClientExpectation {
    /// Create expectation for an active message (no response expected)
    pub fn active_message() -> Self {
        Self {
            expected_handler_type: HandlerType::ActiveMessage,
            expects_response: false,
            expected_response_type: None,
        }
    }

    /// Create expectation for a unary handler with raw bytes response
    pub fn unary_bytes() -> Self {
        Self {
            expected_handler_type: HandlerType::UnaryBytes,
            expects_response: true,
            expected_response_type: None,
        }
    }

    /// Create expectation for a typed unary handler
    pub fn unary_typed(response_type: String) -> Self {
        Self {
            expected_handler_type: HandlerType::UnaryTyped {
                response_type: response_type.clone(),
            },
            expects_response: true,
            expected_response_type: Some(response_type),
        }
    }

    /// Validate this expectation against actual handler contract
    pub fn validate_against(&self, contract: &ContractInfo) -> Result<(), String> {
        // Check basic compatibility
        if self.expects_response != contract.handler_type.expects_response() {
            return Err(format!(
                "Response expectation mismatch: client expects response={}, handler provides response={}",
                self.expects_response,
                contract.handler_type.expects_response()
            ));
        }

        // Check handler type compatibility
        match (&self.expected_handler_type, &contract.handler_type) {
            (HandlerType::ActiveMessage, HandlerType::ActiveMessage) => Ok(()),
            (HandlerType::UnaryBytes, HandlerType::UnaryBytes) => Ok(()),
            (HandlerType::UnaryBytes, HandlerType::UnaryTyped { .. }) => {
                // Raw bytes can accept typed responses (client will get serialized bytes)
                Ok(())
            }
            (
                HandlerType::UnaryTyped {
                    response_type: expected,
                },
                HandlerType::UnaryTyped {
                    response_type: actual,
                },
            ) => {
                if expected == actual {
                    Ok(())
                } else {
                    Err(format!(
                        "Response type mismatch: client expects '{}', handler provides '{}'",
                        expected, actual
                    ))
                }
            }
            _ => Err(format!(
                "Handler type mismatch: client expects {}, handler is {}",
                self.expected_handler_type.description(),
                contract.handler_type.description()
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_handler_type_expects_response() {
        assert!(!HandlerType::ActiveMessage.expects_response());
        assert!(HandlerType::UnaryBytes.expects_response());
        assert!(
            HandlerType::UnaryTyped {
                response_type: "String".to_string()
            }
            .expects_response()
        );
    }

    #[test]
    fn test_client_expectation_validation() {
        // Active message should match active message handler
        let am_expectation = ClientExpectation::active_message();
        let am_contract = ContractInfo {
            handler_type: HandlerType::ActiveMessage,
            response_type: None,
            supports_operation: true,
        };
        assert!(am_expectation.validate_against(&am_contract).is_ok());

        // Unary bytes should match unary bytes handler
        let bytes_expectation = ClientExpectation::unary_bytes();
        let bytes_contract = ContractInfo {
            handler_type: HandlerType::UnaryBytes,
            response_type: None,
            supports_operation: true,
        };
        assert!(bytes_expectation.validate_against(&bytes_contract).is_ok());

        // Typed unary should match same typed handler
        let typed_expectation = ClientExpectation::unary_typed("String".to_string());
        let typed_contract = ContractInfo {
            handler_type: HandlerType::UnaryTyped {
                response_type: "String".to_string(),
            },
            response_type: Some("String".to_string()),
            supports_operation: true,
        };
        assert!(typed_expectation.validate_against(&typed_contract).is_ok());

        // Type mismatch should fail
        let mismatch_contract = ContractInfo {
            handler_type: HandlerType::UnaryTyped {
                response_type: "i32".to_string(),
            },
            response_type: Some("i32".to_string()),
            supports_operation: true,
        };
        assert!(
            typed_expectation
                .validate_against(&mismatch_contract)
                .is_err()
        );
    }

    #[test]
    fn test_receipt_ack_serialization() {
        let ack = ReceiptAck {
            message_id: Uuid::new_v4(),
            status: ReceiptStatus::Delivered,
            contract_info: Some(ContractInfo {
                handler_type: HandlerType::UnaryBytes,
                response_type: None,
                supports_operation: true,
            }),
        };

        let serialized = serde_json::to_string(&ack).expect("Should serialize");
        let deserialized: ReceiptAck =
            serde_json::from_str(&serialized).expect("Should deserialize");
        assert_eq!(ack, deserialized);
    }
}
