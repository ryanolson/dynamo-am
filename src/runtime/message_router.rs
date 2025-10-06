// SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Transport-agnostic message router that handles business logic for active message processing.
//!
//! This module contains all the message routing, response correlation, and peer management
//! logic that was previously embedded in the ZMQ transport layer. By separating this
//! business logic from transport concerns, we achieve:
//!
//! - Clean separation between transport and application logic
//! - Easier testing of routing logic without transport dependencies
//! - Support for multiple transport implementations
//! - Better maintainability and modularity

use anyhow::Result;
use bytes::Bytes;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::{RwLock, mpsc};
use tracing::{debug, error, warn};
use uuid::Uuid;

use crate::api::{client::ActiveMessageClient, handler::ActiveMessage};
use crate::protocols::response::ResponseEnvelope;
use crate::protocols::{ControlMetadata, ResponseContextMetadata};
use crate::runtime::{
    dispatcher::{DispatchMessage, SenderIdentity},
    response_manager::SharedResponseManager,
};

/// Transport-agnostic message router that processes active messages.
///
/// The MessageRouter is responsible for:
/// - Routing messages based on handler names and metadata
/// - Handling response correlation (ACK/NACK/responses)
/// - Managing peer auto-registration
/// - Converting raw messages to dispatch format
/// - Coordinating with the SharedResponseManager for response completion
#[derive(Clone)]
pub struct MessageRouter {
    /// Shared response manager for completing responses, ACKs, and receipts
    response_manager: SharedResponseManager,
    /// Client for auto-registration and peer management
    client: Arc<dyn ActiveMessageClient>,
    /// Channel to send processed messages to the dispatcher
    dispatch_tx: mpsc::Sender<DispatchMessage>,
    /// Cache of known peer IDs to avoid list_peers() on every message
    known_peers: Arc<RwLock<HashSet<Uuid>>>,
}

impl MessageRouter {
    /// Create a new MessageRouter with the given dependencies
    pub fn new(
        response_manager: SharedResponseManager,
        client: Arc<dyn ActiveMessageClient>,
        dispatch_tx: mpsc::Sender<DispatchMessage>,
    ) -> Self {
        Self {
            response_manager,
            client,
            dispatch_tx,
            known_peers: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    /// Update the peer cache with current peers from the client.
    /// This should be called after creating the router to initialize the cache.
    pub async fn initialize_peer_cache(&self) {
        if let Ok(peers) = self.client.list_peers().await {
            let mut cache = self.known_peers.write().await;
            cache.clear();
            for peer in peers {
                cache.insert(peer.instance_id);
            }
            debug!("Initialized peer cache with {} peers", cache.len());
        }
    }

    /// Check if a peer is known (cached lookup)
    async fn is_peer_known(&self, peer_id: Uuid) -> bool {
        self.known_peers.read().await.contains(&peer_id)
    }

    /// Add a peer to the known peers cache
    async fn add_known_peer(&self, peer_id: Uuid) {
        self.known_peers.write().await.insert(peer_id);
    }

    /// Process an incoming active message.
    ///
    /// This method handles all message routing logic that was previously
    /// embedded in the ZMQ manager's receive loop. It determines message
    /// type and routes accordingly:
    ///
    /// - Internal messages (_accept, _response) are handled directly
    /// - Response messages are correlated with pending requests
    /// - Regular messages are auto-registered and forwarded to dispatcher
    pub async fn route_message(&self, message: ActiveMessage) -> Result<()> {
        let control = message.control.clone();

        // Handle special internal messages that bypass the dispatcher
        if message.handler_name == "_accept" {
            return self.handle_acceptance(message, control).await;
        }

        // Check if this is a response message based on control metadata
        if let Some(response_ctx) = control.response_context().cloned() {
            debug!(
                "Received response message for request {}",
                response_ctx.response_to
            );
            return self.handle_response_unified(response_ctx, message).await;
        }

        // Handle auto-registration for unknown senders
        // This is transport-agnostic business logic that belongs in the router
        self.handle_auto_registration(&message, &control).await;

        // Convert ActiveMessage to DispatchMessage
        let dispatch_message = self.convert_to_dispatch_message(message, control).await;

        // Forward to MessageDispatcher
        if let Err(e) = self.dispatch_tx.send(dispatch_message).await {
            error!("Failed to send message to dispatcher: {}", e);
        }

        Ok(())
    }

    /// Handle acceptance messages for pending requests
    async fn handle_acceptance(
        &self,
        message: ActiveMessage,
        control: ControlMetadata,
    ) -> Result<()> {
        if let Some(accept_id) = control.acceptance().or(control.accept_id) {
            self.response_manager.complete_acceptance(accept_id);
            return Ok(());
        }
        error!("Invalid acceptance message: {:?}", message);
        Ok(())
    }

    /// Handle unified response messages (with metadata-based routing)
    ///
    /// This method implements two routing strategies:
    /// 1. **FAST PATH**: Uses `response_type` metadata field (zero JSON parsing for ACK/Response)
    /// 2. **LEGACY PATH**: Falls back to JSON payload parsing for backward compatibility
    ///
    /// The fast path is taken when `control.response_type` is present (new protocol).
    /// The legacy path is taken when `response_type` is None (old clients/servers).
    async fn handle_response_unified(
        &self,
        response_ctx: ResponseContextMetadata,
        message: ActiveMessage,
    ) -> Result<()> {
        let response_id = response_ctx.response_to;
        let control = &message.control;

        debug!(
            "Handling v2 response to {} from {}",
            response_id, message.sender_instance
        );
        debug!("Response payload size: {} bytes", message.payload.len());

        // ============================================================================
        // FAST PATH: Metadata-based response type discrimination (NEW PROTOCOL)
        // ============================================================================
        // This path is taken when the sender includes `response_type` in metadata.
        // It eliminates JSON parsing for ACK (0 parses) and Response (0 parses in router).
        // For NACK, error message is also in metadata, avoiding JSON parse entirely.
        if let Some(response_type) = control.response_type() {
            use crate::api::control::ResponseType;
            debug!("Using metadata response_type: {:?}", response_type);

            match response_type {
                ResponseType::Ack => {
                    debug!("Completing as ACK (from metadata)");
                    self.response_manager
                        .complete_ack_if_present(response_id, Ok(()));
                    return Ok(());
                }
                ResponseType::Nack => {
                    // NEW: Read error message from metadata (zero parse!)
                    let error_msg = response_ctx.error_message.clone().unwrap_or_else(|| {
                        // Fallback: parse JSON for legacy messages without error_message in metadata
                        debug!(
                            "NACK missing error_message in metadata, falling back to JSON parse"
                        );
                        if let Ok(json_value) =
                            serde_json::from_slice::<serde_json::Value>(&message.payload)
                        {
                            json_value
                                .get("message")
                                .and_then(|m| m.as_str())
                                .unwrap_or("Unknown error")
                                .to_string()
                        } else {
                            "Unknown error".to_string()
                        }
                    });
                    debug!("Completing as NACK (from metadata): {}", error_msg);
                    let error_for_response = error_msg.clone();
                    self.response_manager
                        .complete_ack_if_present(response_id, Err(error_msg));
                    match serde_json::to_vec(&ResponseEnvelope::Err(error_for_response)) {
                        Ok(bytes) => {
                            if !self
                                .response_manager
                                .complete_response(response_id, Bytes::from(bytes))
                            {
                                debug!(
                                    "No pending response receiver for NACK response {}",
                                    response_id
                                );
                            }
                        }
                        Err(error) => {
                            warn!(
                                response_id = %response_id,
                                %error,
                                "Failed to serialize ResponseEnvelope for NACK"
                            );
                        }
                    }
                    return Ok(());
                }
                ResponseType::Response => {
                    debug!(
                        "Completing as full response (from metadata) with {} bytes",
                        message.payload.len()
                    );
                    self.response_manager
                        .complete_ack_if_present(response_id, Ok(()));
                    self.response_manager
                        .complete_response(response_id, message.payload);
                    return Ok(());
                }
            }
        }

        // ============================================================================
        // BREAKING CHANGE: response_type is now REQUIRED
        // ============================================================================
        // If we reach here, the sender didn't include response_type metadata.
        // This is a protocol violation in the optimized version.
        error!(
            "Response message {} missing required response_type metadata from {}",
            response_id, message.sender_instance
        );
        anyhow::bail!("Response message missing response_type metadata (protocol version mismatch)")
    }

    /// Convert ActiveMessage to DispatchMessage for the v2 dispatcher
    async fn convert_to_dispatch_message(
        &self,
        message: ActiveMessage,
        control: ControlMetadata,
    ) -> DispatchMessage {
        use std::time::Instant;

        // Determine sender identity based on whether sender is registered (cached lookup)
        let is_known = self.is_peer_known(message.sender_instance).await;
        let sender_identity = if is_known {
            SenderIdentity::Known(message.sender_instance)
        } else if let Some(endpoint) = control.sender_endpoint() {
            let peer_info = crate::client::PeerInfo::new(message.sender_instance, endpoint);
            SenderIdentity::Unknown(peer_info)
        } else {
            // No endpoint available, treat as anonymous
            SenderIdentity::Anonymous
        };

        DispatchMessage {
            message_id: message.message_id,
            handler_name: message.handler_name,
            payload: message.payload,
            sender_identity,
            control,
            received_at: Instant::now(),
        }
    }

    /// Handle auto-registration of unknown senders
    async fn handle_auto_registration(&self, message: &ActiveMessage, control: &ControlMetadata) {
        // Check if sender is already known (cached lookup)
        let is_known = self.is_peer_known(message.sender_instance).await;

        if !is_known {
            // Try to auto-register the sender using endpoint from metadata
            if let Some(endpoint) = control.sender_endpoint() {
                let peer_info = crate::client::PeerInfo::new(message.sender_instance, endpoint);

                if let Err(e) = self.client.connect_to_peer(peer_info.clone()).await {
                    warn!(
                        "Failed to auto-register peer {} at {}: {}",
                        message.sender_instance, endpoint, e
                    );
                } else {
                    // Update cache after successful registration
                    self.add_known_peer(message.sender_instance).await;
                    debug!(
                        "Auto-registered peer {} at {} for response delivery",
                        message.sender_instance, endpoint
                    );
                }
            } else {
                debug!(
                    "Unknown sender {} has no endpoint metadata for auto-registration",
                    message.sender_instance
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::sync::Arc;
    use tokio::sync::mpsc;
    use uuid::Uuid;

    // Mock client for testing
    #[derive(Debug, Clone)]
    struct MockClient {
        instance_id: Uuid,
        endpoint: String,
    }

    #[async_trait::async_trait]
    impl ActiveMessageClient for MockClient {
        fn instance_id(&self) -> Uuid {
            self.instance_id
        }

        fn endpoint(&self) -> &str {
            &self.endpoint
        }

        fn peer_info(&self) -> crate::client::PeerInfo {
            crate::client::PeerInfo::new(self.instance_id, &self.endpoint)
        }

        async fn send_message(
            &self,
            _target: Uuid,
            _handler: &str,
            _payload: Bytes,
        ) -> anyhow::Result<()> {
            Ok(())
        }

        // async fn broadcast_message(&self, _handler: &str, _payload: Bytes) -> anyhow::Result<()> {
        //     Ok(())
        // }

        async fn list_peers(&self) -> anyhow::Result<Vec<crate::client::PeerInfo>> {
            Ok(vec![])
        }

        async fn connect_to_peer(&self, _peer: crate::client::PeerInfo) -> anyhow::Result<()> {
            Ok(())
        }

        async fn disconnect_from_peer(&self, _instance_id: Uuid) -> anyhow::Result<()> {
            Ok(())
        }

        async fn await_handler(
            &self,
            _instance_id: Uuid,
            _handler: &str,
            _timeout: Option<std::time::Duration>,
        ) -> anyhow::Result<bool> {
            Ok(true)
        }

        async fn list_handlers(&self, _instance_id: Uuid) -> anyhow::Result<Vec<String>> {
            Ok(vec![])
        }

        async fn send_raw_message(
            &self,
            _target: Uuid,
            _message: crate::handler::ActiveMessage,
        ) -> anyhow::Result<()> {
            Ok(())
        }

        async fn register_acceptance(
            &self,
            _message_id: Uuid,
            _sender: tokio::sync::oneshot::Sender<()>,
        ) -> anyhow::Result<()> {
            Ok(())
        }

        async fn register_response(
            &self,
            _message_id: Uuid,
            _sender: tokio::sync::oneshot::Sender<Bytes>,
        ) -> anyhow::Result<()> {
            Ok(())
        }

        async fn register_ack(
            &self,
            _ack_id: Uuid,
            _timeout: std::time::Duration,
        ) -> anyhow::Result<tokio::sync::oneshot::Receiver<Result<(), String>>> {
            let (_tx, rx) = tokio::sync::oneshot::channel();
            Ok(rx)
        }

        async fn register_receipt(
            &self,
            _receipt_id: Uuid,
            _timeout: std::time::Duration,
        ) -> anyhow::Result<
            tokio::sync::oneshot::Receiver<Result<crate::receipt_ack::ReceiptAck, String>>,
        > {
            let (_tx, rx) = tokio::sync::oneshot::channel();
            Ok(rx)
        }

        async fn has_incoming_connection_from(&self, _instance_id: Uuid) -> bool {
            false
        }

        fn clone_as_arc(&self) -> Arc<dyn ActiveMessageClient> {
            Arc::new(MockClient {
                instance_id: self.instance_id,
                endpoint: self.endpoint.clone(),
            })
        }
    }

    #[tokio::test]
    async fn test_message_router_creation() {
        use crate::response_manager::ResponseManager;

        let response_manager = Arc::new(ResponseManager::new());
        let client = Arc::new(MockClient {
            instance_id: Uuid::new_v4(),
            endpoint: "test://localhost".to_string(),
        });
        let (dispatch_tx, _dispatch_rx) = mpsc::channel(100);

        let router = MessageRouter::new(response_manager, client, dispatch_tx);

        // Just verify we can create a router - full testing would require more complex setup
        assert!(std::ptr::addr_of!(router) as usize != 0);
    }
}
