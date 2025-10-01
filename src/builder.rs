// SPDX-FileCopyrightText: Copyright (c) 2024-2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Message builder with typestate pattern for ergonomic and safe message construction.

use anyhow::Result;
use bytes::Bytes;
use serde::{Serialize, de::DeserializeOwned};
use std::marker::PhantomData;
use std::time::Duration;
use tokio::sync::oneshot;
use uuid::Uuid;

use super::client::{ActiveMessageClient, validate_handler_name};
use super::handler::{ActiveMessage, InstanceId};
use super::receipt_ack::{ClientExpectation, ReceiptAck, ReceiptStatus};
use super::status::{DetachedConfirm, MessageStatus, SendAndConfirm, WithResponse};

/// TypeState marker: Builder needs delivery mode selection
pub struct NeedsDeliveryMode;

/// TypeState marker: Builder configured with response expectation
pub struct WithResponseExpected;

// ============================================================================
// Receipt ACK Types - Two-Phase Await Pattern
// ============================================================================

/// Phase marker for delivery acknowledgment
pub struct DeliveryPhase;

/// Phase marker for response awaiting
pub struct ResponsePhase;

/// Trait for phase transitions in receipt handling
pub trait PhaseTransition {
    type Output;
}

impl PhaseTransition for DeliveryPhase {
    type Output = (); // Active messages complete with unit after delivery
}

impl PhaseTransition for ResponsePhase {
    type Output = Bytes; // Unary handlers complete with response bytes
}

/// Handle for receipt acknowledgment with two-phase await pattern
pub struct ReceiptHandle<Phase> {
    message_id: Uuid,
    receipt_rx: oneshot::Receiver<Result<ReceiptAck, String>>,
    response_rx: Option<oneshot::Receiver<Bytes>>,
    timeout: Duration,
    _phase: PhantomData<Phase>,
}

impl<Phase> ReceiptHandle<Phase> {
    pub fn new(
        message_id: Uuid,
        receipt_rx: oneshot::Receiver<Result<ReceiptAck, String>>,
        response_rx: Option<oneshot::Receiver<Bytes>>,
        timeout: Duration,
    ) -> Self {
        Self {
            message_id,
            receipt_rx,
            response_rx,
            timeout,
            _phase: PhantomData,
        }
    }

    /// Get the message ID
    pub fn message_id(&self) -> Uuid {
        self.message_id
    }
}

impl ReceiptHandle<DeliveryPhase> {
    /// Wait for receipt ACK and complete (for active messages)
    pub async fn await_delivery(self) -> Result<(), anyhow::Error> {
        // Wait for receipt ACK
        let receipt_ack = tokio::time::timeout(self.timeout, self.receipt_rx)
            .await
            .map_err(|_| anyhow::anyhow!("Receipt ACK timeout after {:?}", self.timeout))?
            .map_err(|_| anyhow::anyhow!("Receipt ACK channel dropped"))?
            .map_err(|e| anyhow::anyhow!("Receipt ACK failed: {}", e))?;

        match receipt_ack.status {
            ReceiptStatus::Delivered => Ok(()),
            ReceiptStatus::ContractMismatch(msg) => {
                Err(anyhow::anyhow!("Contract mismatch: {}", msg))
            }
            ReceiptStatus::HandlerNotFound => Err(anyhow::anyhow!("Handler not found")),
            ReceiptStatus::InvalidMessage(msg) => Err(anyhow::anyhow!("Invalid message: {}", msg)),
        }
    }

    /// Wait for receipt ACK and transition to response phase (for unary messages)
    pub async fn await_receipt_then_response(
        self,
    ) -> Result<ReceiptHandle<ResponsePhase>, anyhow::Error> {
        // Wait for receipt ACK
        let receipt_ack = tokio::time::timeout(self.timeout, self.receipt_rx)
            .await
            .map_err(|_| anyhow::anyhow!("Receipt ACK timeout after {:?}", self.timeout))?
            .map_err(|_| anyhow::anyhow!("Receipt ACK channel dropped"))?
            .map_err(|e| anyhow::anyhow!("Receipt ACK failed: {}", e))?;

        match receipt_ack.status {
            ReceiptStatus::Delivered => {
                // Receipt successful, transition to response phase
                Ok(ReceiptHandle::<ResponsePhase> {
                    message_id: self.message_id,
                    receipt_rx: oneshot::channel().1, // Dummy, already used
                    response_rx: self.response_rx,
                    timeout: self.timeout,
                    _phase: PhantomData,
                })
            }
            ReceiptStatus::ContractMismatch(msg) => {
                Err(anyhow::anyhow!("Contract mismatch: {}", msg))
            }
            ReceiptStatus::HandlerNotFound => Err(anyhow::anyhow!("Handler not found")),
            ReceiptStatus::InvalidMessage(msg) => Err(anyhow::anyhow!("Invalid message: {}", msg)),
        }
    }
}

impl ReceiptHandle<ResponsePhase> {
    /// Wait for the response
    pub async fn await_response(self) -> Result<Bytes, anyhow::Error> {
        let response_rx = self
            .response_rx
            .ok_or_else(|| anyhow::anyhow!("No response channel available"))?;

        tokio::time::timeout(self.timeout, response_rx)
            .await
            .map_err(|_| anyhow::anyhow!("Response timeout after {:?}", self.timeout))?
            .map_err(|_| anyhow::anyhow!("Response channel dropped"))
    }
}

/// Builder for detached receipt handling
pub struct DetachedReceiptBuilder<'a> {
    client: &'a dyn ActiveMessageClient,
    handler_name: String,
    payload: Option<Bytes>,
    timeout: Duration,
    target_instance: Option<InstanceId>,
    client_expectation: ClientExpectation,
}

impl<'a> DetachedReceiptBuilder<'a> {
    /// Set payload from serializable type
    pub fn payload<T: Serialize>(mut self, data: T) -> Result<Self> {
        let serialized = serde_json::to_vec(&data)
            .map_err(|e| anyhow::anyhow!("Failed to serialize payload: {}", e))?;
        self.payload = Some(Bytes::from(serialized));
        Ok(self)
    }

    /// Set raw payload bytes
    pub fn raw_payload(mut self, data: Bytes) -> Self {
        self.payload = Some(data);
        self
    }

    /// Set timeout for response waiting
    pub fn timeout(mut self, duration: Duration) -> Self {
        self.timeout = duration;
        self
    }

    /// Set target instance for the message
    pub fn target_instance(mut self, instance_id: InstanceId) -> Self {
        self.target_instance = Some(instance_id);
        self
    }

    /// Configure expectation for unary bytes response
    pub fn expect_bytes_response(mut self) -> Self {
        self.client_expectation = ClientExpectation::unary_bytes();
        self
    }

    /// Configure expectation for typed response
    pub fn expect_typed_response<T>(mut self) -> Self {
        self.client_expectation =
            ClientExpectation::unary_typed(std::any::type_name::<T>().to_string());
        self
    }

    /// Send the message and return a receipt handle for delivery phase
    pub async fn send(self, target: InstanceId) -> Result<ReceiptHandle<DeliveryPhase>> {
        let message_id = Uuid::new_v4();

        // Register for receipt ACK notification with the client
        let receipt_rx = self
            .client
            .register_receipt(message_id, self.timeout)
            .await?;

        // Create message with receipt ACK expectation
        let mut metadata = serde_json::json!({
            "_mode": "with_receipt_ack",
            "_receipt_id": message_id.to_string(),
            "_client_expectation": self.client_expectation
        });

        // Include endpoint if target doesn't have return connection to us
        if !self.client.has_incoming_connection_from(target).await {
            metadata["_sender_endpoint"] =
                serde_json::Value::String(self.client.endpoint().to_string());
        }

        let message = ActiveMessage {
            message_id,
            handler_name: self.handler_name,
            sender_instance: self.client.instance_id(),
            payload: self.payload.unwrap_or_default(),
            metadata,
        };

        // Send the message
        self.client.send_raw_message(target, message).await?;

        Ok(ReceiptHandle::new(
            message_id,
            receipt_rx,
            None, // No response channel for active messages
            self.timeout,
        ))
    }

    /// Send the message and return a receipt handle that can transition to response phase
    pub async fn send_with_response(
        self,
        target: InstanceId,
    ) -> Result<ReceiptHandle<DeliveryPhase>> {
        let message_id = Uuid::new_v4();

        // Register for both receipt ACK and response
        let receipt_rx = self
            .client
            .register_receipt(message_id, self.timeout)
            .await?;
        let (response_tx, response_rx) = oneshot::channel();
        self.client
            .register_response(message_id, response_tx)
            .await?;

        let mut metadata = serde_json::json!({
            "_mode": "with_receipt_and_response",
            "_receipt_id": message_id.to_string(),
            "_response_id": message_id.to_string(),
            "_client_expectation": self.client_expectation
        });

        // Include endpoint if target doesn't have return connection to us
        if !self.client.has_incoming_connection_from(target).await {
            metadata["_sender_endpoint"] =
                serde_json::Value::String(self.client.endpoint().to_string());
        }

        let message = ActiveMessage {
            message_id,
            handler_name: self.handler_name,
            sender_instance: self.client.instance_id(),
            payload: self.payload.unwrap_or_default(),
            metadata,
        };

        // Send the message
        self.client.send_raw_message(target, message).await?;

        Ok(ReceiptHandle::new(
            message_id,
            receipt_rx,
            Some(response_rx),
            self.timeout,
        ))
    }
}

/// Message builder with typestate for compile-time safety
pub struct MessageBuilder<'a, Mode = NeedsDeliveryMode> {
    client: &'a dyn ActiveMessageClient,
    handler_name: String,
    payload: Option<Bytes>,
    timeout: Duration,
    target_instance: Option<InstanceId>,
    _mode: PhantomData<Mode>,
}

/// Methods available for initial builder state
impl<'a> MessageBuilder<'a, NeedsDeliveryMode> {
    /// Create a new message builder
    pub fn new(client: &'a dyn ActiveMessageClient, handler: &str) -> Result<Self> {
        validate_handler_name(handler)?;
        Ok(Self::new_unchecked(client, handler))
    }

    /// Create a new message builder without handler name validation (for internal system use)
    pub(crate) fn new_unchecked(client: &'a dyn ActiveMessageClient, handler: &str) -> Self {
        Self {
            client,
            handler_name: handler.to_string(),
            payload: None,
            timeout: Duration::from_secs(30),
            target_instance: None,
            _mode: PhantomData,
        }
    }

    /// Set payload from serializable type
    pub fn payload<T: Serialize>(mut self, data: T) -> Result<Self> {
        let serialized = serde_json::to_vec(&data)
            .map_err(|e| anyhow::anyhow!("Failed to serialize payload: {}", e))?;
        self.payload = Some(Bytes::from(serialized));
        Ok(self)
    }

    /// Set raw payload bytes
    pub fn raw_payload(mut self, data: Bytes) -> Self {
        self.payload = Some(data);
        self
    }

    /// Set timeout for response waiting
    pub fn timeout(mut self, duration: Duration) -> Self {
        self.timeout = duration;
        self
    }

    /// Set target instance for the message
    pub fn target_instance(mut self, instance_id: InstanceId) -> Self {
        self.target_instance = Some(instance_id);
        self
    }

    /// Clone this builder with a specific target instance (for cohort broadcasting)
    pub fn clone_with_target(&self, target: InstanceId) -> Self {
        Self {
            client: self.client,
            handler_name: self.handler_name.clone(),
            payload: self.payload.clone(),
            timeout: self.timeout,
            target_instance: Some(target),
            _mode: PhantomData,
        }
    }

    /// Execute the message as an active message - no confirmation or responses
    pub async fn execute(self) -> Result<()> {
        let target = self
            .target_instance
            .ok_or_else(|| anyhow::anyhow!("target_instance must be set before execute()"))?;

        let message = ActiveMessage {
            message_id: Uuid::new_v4(),
            handler_name: self.handler_name,
            sender_instance: self.client.instance_id(),
            payload: self.payload.unwrap_or_default(),
            metadata: serde_json::json!({
                "_mode": "fire_and_forget"
            }),
        };

        self.client.send_raw_message(target, message).await
    }

    /// Send and wait for ACK confirmation
    pub async fn send(self, target: InstanceId) -> Result<MessageStatus<SendAndConfirm>> {
        let message_id = Uuid::new_v4();

        // Register for ACK notification
        let ack_rx = self.client.register_ack(message_id, self.timeout).await?;

        // Check if we need to include endpoint for auto-registration
        let mut metadata = serde_json::json!({
            "_mode": "confirmed",
            "_accept_id": message_id.to_string()
        });

        // Include endpoint if target doesn't have return connection to us
        if !self.client.has_incoming_connection_from(target).await {
            metadata["_sender_endpoint"] =
                serde_json::Value::String(self.client.endpoint().to_string());
        }

        let message = ActiveMessage {
            message_id,
            handler_name: self.handler_name,
            sender_instance: self.client.instance_id(),
            payload: self.payload.unwrap_or_default(),
            metadata,
        };

        self.client.send_raw_message(target, message).await?;

        // Wait for ACK
        tokio::time::timeout(self.timeout, ack_rx)
            .await
            .map_err(|_| anyhow::anyhow!("Handler ACK timeout after {:?}", self.timeout))?
            .map_err(|_| anyhow::anyhow!("Handler ACK channel dropped"))?
            .map_err(|nack_error| anyhow::anyhow!("Handler sent NACK: {}", nack_error))?;

        Ok(MessageStatus::new(message_id, None, None, self.timeout))
    }

    /// Send without waiting, get status object for later confirmation
    pub async fn send_detached(self, target: InstanceId) -> Result<MessageStatus<DetachedConfirm>> {
        let message_id = Uuid::new_v4();

        // Register for ACK notification
        let ack_rx = self.client.register_ack(message_id, self.timeout).await?;

        // Check if we need to include endpoint for auto-registration
        let mut metadata = serde_json::json!({
            "_mode": "confirmed",
            "_accept_id": message_id.to_string()
        });

        // Include endpoint if target doesn't have return connection to us
        if !self.client.has_incoming_connection_from(target).await {
            metadata["_sender_endpoint"] =
                serde_json::Value::String(self.client.endpoint().to_string());
        }

        let message = ActiveMessage {
            message_id,
            handler_name: self.handler_name,
            sender_instance: self.client.instance_id(),
            payload: self.payload.unwrap_or_default(),
            metadata,
        };

        self.client.send_raw_message(target, message).await?;

        // Convert ACK receiver to acceptance receiver for compatibility
        let (accept_tx, accept_rx) = oneshot::channel();
        tokio::spawn(async move {
            match ack_rx.await {
                Ok(Ok(())) => {
                    let _ = accept_tx.send(()); // ACK received, signal acceptance
                }
                Ok(Err(_)) => {
                    // NACK received, don't signal acceptance (will timeout)
                }
                Err(_) => {
                    // Channel closed, don't signal acceptance
                }
            }
        });

        Ok(MessageStatus::new(
            message_id,
            Some(accept_rx),
            None,
            self.timeout,
        ))
    }

    /// Detach receipt handling - returns a handle for two-phase await pattern
    /// This enables receipt ACK validation before handler execution
    pub fn detach_receipt(self) -> DetachedReceiptBuilder<'a> {
        DetachedReceiptBuilder {
            client: self.client,
            handler_name: self.handler_name,
            payload: self.payload,
            timeout: self.timeout,
            target_instance: self.target_instance,
            client_expectation: ClientExpectation::active_message(), // Default to AM
        }
    }

    /// Configure to expect additional response beyond acceptance
    pub fn expect_response<R: DeserializeOwned + 'static>(
        self,
    ) -> MessageBuilder<'a, WithResponseExpected> {
        MessageBuilder {
            client: self.client,
            handler_name: self.handler_name,
            payload: self.payload,
            timeout: self.timeout,
            target_instance: self.target_instance,
            _mode: PhantomData,
        }
    }
}

/// Methods available when response is expected
impl<'a> MessageBuilder<'a, WithResponseExpected> {
    /// Set payload from serializable type
    pub fn payload<T: Serialize>(mut self, data: T) -> Result<Self> {
        let serialized = serde_json::to_vec(&data)
            .map_err(|e| anyhow::anyhow!("Failed to serialize payload: {}", e))?;
        self.payload = Some(Bytes::from(serialized));
        Ok(self)
    }

    /// Set raw payload bytes
    pub fn raw_payload(mut self, data: Bytes) -> Self {
        self.payload = Some(data);
        self
    }

    /// Set timeout for response waiting
    pub fn timeout(mut self, duration: Duration) -> Self {
        self.timeout = duration;
        self
    }

    /// Set target instance for the message
    pub fn target_instance(mut self, instance_id: InstanceId) -> Self {
        self.target_instance = Some(instance_id);
        self
    }

    /// Execute the message and await acceptance, return status with response awaiter
    pub async fn execute(self) -> Result<MessageStatus<WithResponse>> {
        let target = self
            .target_instance
            .ok_or_else(|| anyhow::anyhow!("target_instance must be set before execute()"))?;
        self.send(target).await
    }

    /// Send and await acceptance, return status with response awaiter
    pub async fn send(self, target: InstanceId) -> Result<MessageStatus<WithResponse>> {
        let message_id = Uuid::new_v4();
        let (accept_tx, accept_rx) = oneshot::channel();
        let (response_tx, response_rx) = oneshot::channel();

        // Register for both acceptance and response
        self.client
            .register_acceptance(message_id, accept_tx)
            .await?;
        self.client
            .register_response(message_id, response_tx)
            .await?;

        // Check if we need to include endpoint for auto-registration
        let mut metadata = serde_json::json!({
            "_mode": "with_response",
            "_accept_id": message_id.to_string(),
            "_response_id": message_id.to_string()
        });

        // Include endpoint if target doesn't have return connection to us
        if !self.client.has_incoming_connection_from(target).await {
            metadata["_sender_endpoint"] =
                serde_json::Value::String(self.client.endpoint().to_string());
        }

        let message = ActiveMessage {
            message_id,
            handler_name: self.handler_name,
            sender_instance: self.client.instance_id(),
            payload: self.payload.unwrap_or_default(),
            metadata,
        };

        self.client.send_raw_message(target, message).await?;

        // Wait for acceptance
        tokio::time::timeout(self.timeout, accept_rx)
            .await
            .map_err(|_| anyhow::anyhow!("Handler acceptance timeout after {:?}", self.timeout))?
            .map_err(|_| anyhow::anyhow!("Handler acceptance channel dropped"))?;

        Ok(MessageStatus::new(
            message_id,
            None,
            Some(response_rx),
            self.timeout,
        ))
    }

    /// Send detached - don't wait for acceptance, return status with both awaiters
    pub async fn send_detached(self, target: InstanceId) -> Result<MessageStatus<DetachedConfirm>> {
        // For simplicity, with_response mode always waits for acceptance first
        // We can add a DetachedWithResponse mode later if needed
        self.send(target)
            .await
            .map(|status| MessageStatus::new(status.message_id, None, None, status.timeout))
    }
}
