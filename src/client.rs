// SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use serde::Serialize;
use std::time::Duration;
use tokio::sync::oneshot;
use tracing::debug;
use uuid::Uuid;

use super::builder::{MessageBuilder, NeedsDeliveryMode};
use super::handler::{ActiveMessage, HandlerId, InstanceId};
use super::receipt_ack::ReceiptAck;
use super::responses::{
    HealthCheckResponse, JoinCohortResponse, RegisterServiceResponse,
};
use super::utils::extract_host;

/// Trait for types that can be converted to message payload bytes
pub trait IntoPayload: Send {
    fn into_payload(self) -> Result<Bytes>;
}

// Implement for Bytes (pass-through)
impl IntoPayload for Bytes {
    fn into_payload(self) -> Result<Bytes> {
        Ok(self)
    }
}

// Implement for references to Serialize types
impl<T: Serialize + Sync> IntoPayload for &T {
    fn into_payload(self) -> Result<Bytes> {
        let serialized = serde_json::to_vec(self)
            .map_err(|e| anyhow::anyhow!("Failed to serialize payload: {}", e))?;
        Ok(Bytes::from(serialized))
    }
}

pub type Endpoint = String;

/// Represents possible connection endpoints for a worker/peer.
/// This allows connecting to a peer without needing to know their instance_id upfront.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WorkerAddress {
    /// TCP endpoint for cross-host communication
    pub tcp_endpoint: Option<String>,
    /// IPC endpoint for same-host optimization
    pub ipc_endpoint: Option<String>,
}

impl WorkerAddress {
    /// Create a WorkerAddress with just a TCP endpoint
    pub fn tcp(endpoint: impl Into<String>) -> Self {
        Self {
            tcp_endpoint: Some(endpoint.into()),
            ipc_endpoint: None,
        }
    }

    /// Create a WorkerAddress with just an IPC endpoint
    pub fn ipc(endpoint: impl Into<String>) -> Self {
        Self {
            tcp_endpoint: None,
            ipc_endpoint: Some(endpoint.into()),
        }
    }

    /// Create a WorkerAddress with both TCP and IPC endpoints
    pub fn both(tcp: impl Into<String>, ipc: impl Into<String>) -> Self {
        Self {
            tcp_endpoint: Some(tcp.into()),
            ipc_endpoint: Some(ipc.into()),
        }
    }

    /// Get the best endpoint to try first (prefers IPC for same-host)
    pub fn primary_endpoint(&self) -> Option<&str> {
        self.ipc_endpoint
            .as_deref()
            .or(self.tcp_endpoint.as_deref())
    }

    /// Get all available endpoints
    pub fn all_endpoints(&self) -> Vec<&str> {
        let mut endpoints = Vec::new();
        if let Some(ref ipc) = self.ipc_endpoint {
            endpoints.push(ipc.as_str());
        }
        if let Some(ref tcp) = self.tcp_endpoint {
            endpoints.push(tcp.as_str());
        }
        endpoints
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PeerInfo {
    pub instance_id: InstanceId,
    /// Primary endpoint (TCP) - maintained for backward compatibility
    pub endpoint: Endpoint,
    /// All available connection endpoints for this peer
    pub address: WorkerAddress,
}

impl PeerInfo {
    /// Create PeerInfo with single endpoint (backward compatibility)
    pub fn new(instance_id: InstanceId, endpoint: impl Into<String>) -> Self {
        let endpoint = endpoint.into();
        let address = WorkerAddress::tcp(endpoint.clone());
        Self {
            instance_id,
            endpoint,
            address,
        }
    }

    /// Create PeerInfo with WorkerAddress
    pub fn with_address(instance_id: InstanceId, address: WorkerAddress) -> Self {
        let primary_endpoint = address.primary_endpoint().unwrap_or("unknown").to_string();
        Self {
            instance_id,
            endpoint: primary_endpoint,
            address,
        }
    }

    /// Create PeerInfo with both TCP and IPC endpoints (backward compatibility)
    pub fn new_dual(
        instance_id: InstanceId,
        tcp_endpoint: Option<Endpoint>,
        ipc_endpoint: Option<Endpoint>,
    ) -> Self {
        // Use TCP endpoint as primary for backward compatibility
        let primary_endpoint = tcp_endpoint
            .as_ref()
            .cloned()
            .unwrap_or_else(|| "tcp://unknown:0".to_string());

        let address = WorkerAddress {
            tcp_endpoint,
            ipc_endpoint,
        };

        Self {
            instance_id,
            endpoint: primary_endpoint,
            address,
        }
    }

    /// Get the best endpoint for connection based on local preferences
    pub fn select_endpoint(&self, my_endpoint: &str) -> Option<&Endpoint> {
        // Prefer IPC for same-host communication if available
        if let Some(ref ipc_ep) = self.address.ipc_endpoint
            && self.is_local(my_endpoint)
        {
            return Some(ipc_ep);
        }

        // Fall back to TCP endpoint
        self.address.tcp_endpoint.as_ref().or(Some(&self.endpoint))
    }

    pub fn is_local(&self, my_endpoint: &str) -> bool {
        // Extract our host from our endpoint
        let my_host = match extract_host(my_endpoint) {
            Some(host) => host,
            None => return false,
        };

        // If peer has IPC endpoint, check if we're on the same host as their TCP endpoint
        if self.address.ipc_endpoint.is_some() {
            let peer_tcp_endpoint = self.address.tcp_endpoint.as_ref().unwrap_or(&self.endpoint);
            if let Some(peer_host) = extract_host(peer_tcp_endpoint) {
                return my_host == peer_host;
            }
        }

        // Fall back to host comparison using available TCP endpoint
        let peer_endpoint = self.address.tcp_endpoint.as_ref().unwrap_or(&self.endpoint);

        // IPC endpoints are always local
        if peer_endpoint.starts_with("ipc://") {
            return true;
        }

        // Compare hosts for TCP endpoints
        if let Some(peer_host) = extract_host(peer_endpoint) {
            my_host == peer_host
        } else {
            false
        }
    }

    /// Get the preferred endpoint for this peer (IPC if available, otherwise TCP)
    pub fn preferred_endpoint(&self) -> &str {
        self.address.primary_endpoint().unwrap_or(&self.endpoint)
    }

    /// Get all available endpoints for this peer
    pub fn all_endpoints(&self) -> Vec<&str> {
        self.address.all_endpoints()
    }

    /// Get the TCP endpoint if available
    pub fn tcp_endpoint(&self) -> Option<&str> {
        self.address.tcp_endpoint.as_deref()
    }

    /// Get the IPC endpoint if available
    pub fn ipc_endpoint(&self) -> Option<&str> {
        self.address.ipc_endpoint.as_deref()
    }
}

/// Validate that handler name doesn't use reserved system handler prefix
pub(crate) fn validate_handler_name(handler: &str) -> Result<()> {
    if handler.starts_with('_') {
        anyhow::bail!(
            "Cannot directly call system handler '{}'. Use client convenience methods instead: health_check(), ensure_bidirectional_connection(), list_handlers(), await_handler()",
            handler
        );
    }
    Ok(())
}

#[async_trait]
pub trait ActiveMessageClient: Send + Sync + std::fmt::Debug {
    fn instance_id(&self) -> InstanceId;

    fn endpoint(&self) -> &str;

    fn peer_info(&self) -> PeerInfo;

    async fn send_message(&self, target: InstanceId, handler: &str, payload: Bytes) -> Result<()>;

    async fn list_peers(&self) -> Result<Vec<PeerInfo>>;

    async fn connect_to_peer(&self, peer: PeerInfo) -> Result<()>;

    /// Connect to a worker address and discover the peer's instance_id
    async fn connect_to_address(&self, address: &WorkerAddress) -> Result<PeerInfo> {
        // Default implementation tries endpoints in order
        for endpoint in address.all_endpoints() {
            match self.discover_peer(endpoint).await {
                Ok(peer_info) => {
                    self.connect_to_peer(peer_info.clone()).await?;
                    return Ok(peer_info);
                }
                Err(e) => {
                    debug!("Failed to discover peer at {}: {}", endpoint, e);
                    continue;
                }
            }
        }
        anyhow::bail!("Failed to discover peer at any of the provided endpoints")
    }

    /// Discover peer information by sending a discovery message to an endpoint
    async fn discover_peer(&self, _endpoint: &str) -> Result<PeerInfo> {
        // Default implementation - subclasses should override this
        anyhow::bail!("Discovery not implemented for this client type")
    }

    async fn disconnect_from_peer(&self, instance_id: InstanceId) -> Result<()>;

    async fn await_handler(
        &self,
        instance_id: InstanceId,
        handler: &str,
        timeout: Option<Duration>,
    ) -> Result<bool>;

    async fn list_handlers(&self, instance_id: InstanceId) -> Result<Vec<HandlerId>>;

    /// Send a health check request and get the response
    ///
    /// Note: This method requires `Self: Sized`. When working with trait objects (`&dyn ActiveMessageClient`),
    /// use the free function `active_message::client::health_check(client, instance_id)` instead.
    async fn health_check(&self, instance_id: InstanceId) -> Result<HealthCheckResponse>
    where
        Self: Sized,
    {
        health_check(self, instance_id).await
    }

    /// Ensure bidirectional connection by registering our info with a remote instance
    ///
    /// This is used when we have connected to a remote instance, but that instance
    /// doesn't know how to connect back to us. This method sends our PeerInfo to
    /// the remote instance so it can establish a bidirectional connection.
    ///
    /// Note: This method requires `Self: Sized`. When working with trait objects (`&dyn ActiveMessageClient`),
    /// use the free function `active_message::client::ensure_bidirectional_connection(client, instance_id)` instead.
    async fn ensure_bidirectional_connection(&self, instance_id: InstanceId) -> Result<bool>
    where
        Self: Sized,
    {
        ensure_bidirectional_connection(self, instance_id).await
    }

    /// Join a leader-worker cohort
    ///
    /// Note: This method requires `Self: Sized`. When working with trait objects (`&dyn ActiveMessageClient`),
    /// use the free function `active_message::client::join_cohort(client, leader_id, rank)` instead.
    async fn join_cohort(
        &self,
        leader_instance_id: InstanceId,
        rank: Option<usize>,
    ) -> Result<JoinCohortResponse>
    where
        Self: Sized,
    {
        join_cohort(self, leader_instance_id, rank).await
    }

    // New builder pattern API
    /// Create an active message builder
    fn active_message(&self, handler: &str) -> Result<MessageBuilder<'_>>
    where
        Self: Sized,
    {
        MessageBuilder::new(self, handler)
    }

    /// Internal method to create active message builder for system handlers (bypasses validation)
    fn system_active_message(&self, handler: &str) -> MessageBuilder<'_, NeedsDeliveryMode>
    where
        Self: Sized,
    {
        // Use the unchecked constructor for system handlers
        MessageBuilder::new_unchecked(self, handler)
    }

    // Internal methods for builder pattern support

    /// Send raw ActiveMessage (internal use by builder)
    async fn send_raw_message(&self, target: InstanceId, message: ActiveMessage) -> Result<()>;

    /// Check if a peer has an incoming connection to us (internal use by builder)
    async fn has_incoming_connection_from(&self, instance_id: InstanceId) -> bool;

    /// Clone this client as an Arc<dyn ActiveMessageClient> for use in contexts
    fn clone_as_arc(&self) -> std::sync::Arc<dyn ActiveMessageClient>;

    // Response correlation methods (delegated to ResponseManager)
    //
    // These methods provide transport-agnostic response correlation. Implementations
    // should delegate to a ResponseManager instance for centralized correlation handling.

    /// Register for acceptance notification (internal use by builder)
    async fn register_acceptance(
        &self,
        message_id: Uuid,
        sender: oneshot::Sender<()>,
    ) -> Result<()>;

    /// Register for response notification (internal use by builder)
    async fn register_response(
        &self,
        message_id: Uuid,
        sender: oneshot::Sender<Bytes>,
    ) -> Result<()>;

    /// Register for ACK/NACK notification (internal use by builder)
    async fn register_ack(
        &self,
        ack_id: Uuid,
        timeout: Duration,
    ) -> Result<oneshot::Receiver<Result<(), String>>>;

    /// Register for receipt ACK notification (internal use by builder)
    async fn register_receipt(
        &self,
        receipt_id: Uuid,
        timeout: Duration,
    ) -> Result<oneshot::Receiver<Result<ReceiptAck, String>>>;

    // Helper methods for response handling (crate-visible only)

    /// Send an ACK response for a message
    #[doc(hidden)]
    async fn send_ack(&self, response_id: Uuid, sender_id: InstanceId) -> Result<()> {
        let ack_payload = serde_json::json!({
            "response_id": response_id.to_string(),
            "status": "ok"
        });
        let ack_bytes = Bytes::from(serde_json::to_vec(&ack_payload)?);
        // V2 pattern: use any handler name (not "_response") with "_response_to" metadata
        let message = super::handler::ActiveMessage {
            message_id: Uuid::new_v4(),
            handler_name: "ack_response".to_string(), // Use a regular handler name, not "_response"
            sender_instance: self.instance_id(),
            payload: ack_bytes,
            metadata: serde_json::json!({
                "_response_to": response_id.to_string()
            }),
        };
        self.send_raw_message(sender_id, message).await
    }

    /// Send a response with payload for a message
    #[doc(hidden)]
    async fn send_response(
        &self,
        response_id: Uuid,
        sender_id: InstanceId,
        payload: Bytes,
    ) -> Result<()> {
        debug!(
            "Sending response for {} to {} with {} bytes",
            response_id,
            sender_id,
            payload.len()
        );
        let metadata = serde_json::json!({
            "_response_to": response_id.to_string()
        });
        // V2 pattern: use any handler name (not "_response") with "_response_to" metadata
        // This allows the response to be routed correctly
        let message = super::handler::ActiveMessage {
            message_id: Uuid::new_v4(),
            handler_name: "response_message".to_string(), // Use a regular handler name, not "_response"
            sender_instance: self.instance_id(),
            payload,
            metadata,
        };
        debug!(
            "Response message created with handler_name='{}' and metadata={:?}",
            message.handler_name, message.metadata
        );
        self.send_raw_message(sender_id, message).await
    }

    /// Send an error/NACK response for a message
    #[doc(hidden)]
    async fn send_error(
        &self,
        response_id: Uuid,
        sender_id: InstanceId,
        error_msg: String,
    ) -> Result<()> {
        let error_payload = serde_json::json!({
            "response_id": response_id.to_string(),
            "status": "error",
            "message": error_msg
        });
        let error_bytes = Bytes::from(serde_json::to_vec(&error_payload)?);
        // V2 pattern: use any handler name (not "_response") with "_response_to" metadata
        let message = super::handler::ActiveMessage {
            message_id: Uuid::new_v4(),
            handler_name: "error_response".to_string(), // Use a regular handler name, not "_response"
            sender_instance: self.instance_id(),
            payload: error_bytes,
            metadata: serde_json::json!({
                "_response_to": response_id.to_string()
            }),
        };
        self.send_raw_message(sender_id, message).await
    }
}

// Helper functions that work with trait objects (&dyn ActiveMessageClient)

/// Send a health check request and get the response (works with trait objects)
pub async fn health_check(
    client: &dyn ActiveMessageClient,
    instance_id: InstanceId,
) -> Result<HealthCheckResponse> {
    let status = MessageBuilder::new_unchecked(client, "_health_check")
        .expect_response::<HealthCheckResponse>()
        .send(instance_id)
        .await?;

    status.await_response().await
}

/// Ensure bidirectional connection (works with trait objects)
pub async fn ensure_bidirectional_connection(
    client: &dyn ActiveMessageClient,
    instance_id: InstanceId,
) -> Result<bool> {
    let my_info = client.peer_info();

    let mut payload = serde_json::json!({
        "instance_id": my_info.instance_id.to_string(),
    });

    if my_info.tcp_endpoint().is_some() || my_info.ipc_endpoint().is_some() {
        if let Some(tcp_ep) = my_info.tcp_endpoint() {
            payload["tcp_endpoint"] = serde_json::Value::String(tcp_ep.to_string());
        }
        if let Some(ipc_ep) = my_info.ipc_endpoint() {
            payload["ipc_endpoint"] = serde_json::Value::String(ipc_ep.to_string());
        }
    } else {
        payload["endpoint"] = serde_json::Value::String(my_info.endpoint);
    }

    let status = MessageBuilder::new_unchecked(client, "_register_service")
        .payload(payload)?
        .expect_response::<RegisterServiceResponse>()
        .send(instance_id)
        .await?;

    let response: RegisterServiceResponse = status.await_response().await?;
    Ok(response.registered)
}

/// Join a leader-worker cohort (works with trait objects)
pub async fn join_cohort(
    client: &dyn ActiveMessageClient,
    leader_instance_id: InstanceId,
    rank: Option<usize>,
) -> Result<JoinCohortResponse> {
    let payload = serde_json::json!({
        "instance_id": client.instance_id().to_string(),
        "rank": rank,
    });

    let status = MessageBuilder::new_unchecked(client, "_join_cohort")
        .payload(payload)?
        .expect_response::<JoinCohortResponse>()
        .send(leader_instance_id)
        .await?;

    status.await_response().await
}
