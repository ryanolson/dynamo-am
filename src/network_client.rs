// SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Transport-agnostic network client that implements the ActiveMessageClient trait.
//!
//! This module provides the high-level client abstraction that handles peer management,
//! service discovery, and message routing. It uses a ThinTransport implementation for
//! actual network communication, keeping business logic separate from transport concerns.

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use dashmap::DashMap;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{RwLock, oneshot};
use tracing::{debug, warn};
use uuid::Uuid;

use super::boxed_transport::{BoxedConnectionHandle, BoxedTransport};
use super::client::{ActiveMessageClient, Endpoint, PeerInfo, WorkerAddress};
use super::handler::{ActiveMessage, HandlerId, InstanceId};
use super::receipt_ack::ReceiptAck;
use super::response_manager::SharedResponseManager;

/// Internal state for the network client
struct NetworkClientState {
    /// Registry of known peers
    peers: HashMap<InstanceId, PeerInfo>,
    /// Track peers that have connected TO us (for bidirectional awareness)
    incoming_peers: HashSet<InstanceId>,
}

/// Transport-agnostic network client that manages peers and connections.
///
/// The NetworkClient implements the ActiveMessageClient trait and provides
/// high-level operations like peer management, service discovery, and message
/// sending. It uses a type-erased BoxedTransport internally to avoid generic parameters.
pub struct NetworkClient {
    instance_id: InstanceId,
    endpoint: Endpoint,
    state: Arc<RwLock<NetworkClientState>>,

    /// Connection handles for each peer (InstanceId -> ConnectionHandle)
    connections: Arc<DashMap<InstanceId, BoxedConnectionHandle>>,

    /// The underlying transport implementation (type-erased)
    transport: Arc<BoxedTransport>,

    /// Response manager for correlating responses, ACKs, and receipts
    response_manager: SharedResponseManager,
}

impl std::fmt::Debug for NetworkClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NetworkClient")
            .field("instance_id", &self.instance_id)
            .field("endpoint", &self.endpoint)
            .field("connections_count", &self.connections.len())
            .finish()
    }
}

impl NetworkClient {
    /// Create a new NetworkClient with a type-erased transport implementation
    pub fn new(
        instance_id: InstanceId,
        endpoint: Endpoint,
        transport: BoxedTransport,
        response_manager: SharedResponseManager,
    ) -> Self {
        let state = NetworkClientState {
            peers: HashMap::new(),
            incoming_peers: HashSet::new(),
        };

        Self {
            instance_id,
            endpoint,
            state: Arc::new(RwLock::new(state)),
            connections: Arc::new(DashMap::new()),
            transport: Arc::new(transport),
            response_manager,
        }
    }

    /// Fast send path using pre-established connection handle
    async fn send_via_connection(&self, target: InstanceId, message: &ActiveMessage) -> Result<()> {
        // Fast path: lookup connection handle
        let conn_handle = self
            .connections
            .get(&target)
            .ok_or_else(|| anyhow::anyhow!("No connection to peer: {}", target))?;

        // Get the sender - we don't need to clone the whole handle
        let sender = conn_handle.sender().clone();
        drop(conn_handle);

        // Serialize on caller's thread and send
        self.transport.serialize_and_send(message, &sender)?;

        Ok(())
    }

    /// Track an incoming connection from a peer
    pub async fn track_incoming_connection(&self, sender_instance: InstanceId) {
        let mut state = self.state.write().await;
        state.incoming_peers.insert(sender_instance);
    }

    /// Check if we have an incoming connection from a peer
    pub async fn has_incoming_connection_from(&self, instance_id: InstanceId) -> bool {
        let state = self.state.read().await;
        state.incoming_peers.contains(&instance_id)
    }

    /// Shutdown all connections
    pub async fn shutdown_connections(&self) {
        // Disconnect all endpoints via the thin transport
        for entry in self.connections.iter() {
            let conn = entry.value();
            if let Err(e) = self.transport.disconnect(&conn.active_endpoint).await {
                warn!("Failed to disconnect from {}: {}", conn.active_endpoint, e);
            }
        }

        self.connections.clear();
        debug!("All connections shut down");
    }
}

#[async_trait]
impl ActiveMessageClient for NetworkClient {
    fn instance_id(&self) -> InstanceId {
        self.instance_id
    }

    fn endpoint(&self) -> &str {
        &self.endpoint
    }

    fn peer_info(&self) -> PeerInfo {
        PeerInfo::new(self.instance_id, &self.endpoint)
    }

    async fn send_message(&self, target: InstanceId, handler: &str, payload: Bytes) -> Result<()> {
        let message = ActiveMessage::new(
            Uuid::new_v4(),
            handler.to_string(),
            self.instance_id,
            payload,
            serde_json::Value::Null,
        );

        self.send_via_connection(target, &message).await?;

        debug!("Sent message to handler '{}' on peer {}", handler, target);

        Ok(())
    }

    async fn list_peers(&self) -> Result<Vec<PeerInfo>> {
        let state = self.state.read().await;
        Ok(state.peers.values().cloned().collect())
    }

    async fn connect_to_peer(&self, peer: PeerInfo) -> Result<()> {
        // Check if already connected
        if self.connections.contains_key(&peer.instance_id) {
            debug!("Peer {} already connected", peer.instance_id);
            return Ok(());
        }

        // Smart endpoint selection happens HERE at connection time
        // Try IPC first for same-host, fall back to TCP
        let endpoint = peer.select_endpoint(&self.endpoint).ok_or_else(|| {
            anyhow::anyhow!("No available endpoint for peer: {}", peer.instance_id)
        })?;

        debug!(
            "Connecting to peer {} at {} ({})",
            peer.instance_id,
            endpoint,
            if endpoint.starts_with("ipc://") {
                "IPC"
            } else {
                "TCP"
            }
        );

        // Establish connection via thin transport - returns boxed sender
        let sender = self.transport.connect(endpoint).await?;

        // Create BoxedConnectionHandle with the sender
        let conn_handle =
            BoxedConnectionHandle::new(peer.instance_id, sender, endpoint.to_string());

        // Store in connections map
        self.connections.insert(peer.instance_id, conn_handle);

        // Also track in peers registry
        let mut state = self.state.write().await;
        state.peers.insert(peer.instance_id, peer.clone());

        debug!(
            "Established connection to peer {} at {}",
            peer.instance_id, endpoint
        );

        Ok(())
    }

    async fn discover_peer(&self, endpoint: &str) -> Result<PeerInfo> {
        use super::responses::DiscoverResponse;

        debug!("Discovering peer at endpoint: {}", endpoint);

        // Create a temporary UUID for the discovery request
        let temp_target = Uuid::new_v4();

        // Create a temporary PeerInfo to send the discovery message
        let temp_peer = PeerInfo::new(temp_target, endpoint);

        // Connect temporarily for discovery
        self.connect_to_peer(temp_peer).await?;

        // Send discovery request using builder pattern
        let status = self
            .system_active_message("_discover")
            .expect_response::<DiscoverResponse>()
            .send(temp_target)
            .await?;

        // Get the response
        let discover_response: DiscoverResponse = status.await_response().await?;

        // Create WorkerAddress from response
        let address = WorkerAddress {
            tcp_endpoint: discover_response.tcp_endpoint,
            ipc_endpoint: discover_response.ipc_endpoint,
        };

        // Parse instance ID
        let instance_id = Uuid::parse_str(&discover_response.instance_id)
            .map_err(|e| anyhow::anyhow!("Invalid instance ID in discovery response: {}", e))?;

        // Create proper PeerInfo with discovered instance ID
        let peer_info = PeerInfo::with_address(instance_id, address);

        // Clean up temporary connection
        self.disconnect_from_peer(temp_target).await?;

        debug!("Discovered peer {} at {}", instance_id, endpoint);
        Ok(peer_info)
    }

    async fn disconnect_from_peer(&self, instance_id: InstanceId) -> Result<()> {
        // Remove connection handle and disconnect via transport
        if let Some((_, conn)) = self.connections.remove(&instance_id) {
            let endpoint = conn.active_endpoint.clone();
            self.transport.disconnect(&endpoint).await?;
            debug!("Disconnected from peer {} at {}", instance_id, endpoint);
        }

        // Also remove from peers map
        let mut state = self.state.write().await;
        state.peers.remove(&instance_id);

        Ok(())
    }

    async fn await_handler(
        &self,
        instance_id: InstanceId,
        handler: &str,
        timeout: Option<Duration>,
    ) -> Result<bool> {
        let payload = serde_json::json!({
            "handler_name": handler,
            "timeout_ms": timeout.map(|d| d.as_millis()),
        });

        let mut builder = self
            .system_active_message("_wait_for_handler")
            .payload(payload)?
            .expect_response::<super::responses::WaitForHandlerResponse>();

        if let Some(t) = timeout {
            builder = builder.timeout(t);
        }

        let status = builder.send(instance_id).await?;
        let response: super::responses::WaitForHandlerResponse = status.await_response().await?;
        Ok(response.available)
    }

    async fn list_handlers(&self, instance_id: InstanceId) -> Result<Vec<HandlerId>> {
        debug!("list_handlers: Sending request to instance {}", instance_id);
        let status = self
            .system_active_message("_list_handlers")
            .expect_response::<super::responses::ListHandlersResponse>()
            .send(instance_id)
            .await?;

        debug!("list_handlers: Message sent, waiting for response");
        let response: super::responses::ListHandlersResponse = status.await_response().await?;
        debug!(
            "list_handlers: Received response with {} handlers",
            response.handlers.len()
        );
        Ok(response.handlers)
    }

    async fn send_raw_message(&self, target: InstanceId, message: ActiveMessage) -> Result<()> {
        self.send_via_connection(target, &message).await?;

        debug!(
            "Sent raw message to handler '{}' on peer {}",
            message.handler_name, target
        );

        Ok(())
    }

    async fn register_acceptance(
        &self,
        message_id: Uuid,
        sender: oneshot::Sender<()>,
    ) -> Result<()> {
        self.response_manager
            .register_acceptance(message_id, sender, Duration::from_secs(30));

        debug!("Registered acceptance for message {}", message_id);
        Ok(())
    }

    async fn register_response(
        &self,
        message_id: Uuid,
        sender: oneshot::Sender<Bytes>,
    ) -> Result<()> {
        self.response_manager
            .register_response(message_id, sender, Duration::from_secs(30));

        debug!("Registered response for message {}", message_id);
        Ok(())
    }

    async fn register_ack(
        &self,
        ack_id: Uuid,
        timeout: Duration,
    ) -> Result<oneshot::Receiver<Result<(), String>>> {
        let (tx, rx) = oneshot::channel();
        self.response_manager.register_ack(ack_id, tx, timeout);
        debug!("Registered ACK expectation: {}", ack_id);
        Ok(rx)
    }

    async fn register_receipt(
        &self,
        receipt_id: Uuid,
        timeout: Duration,
    ) -> Result<oneshot::Receiver<Result<ReceiptAck, String>>> {
        let (tx, rx) = oneshot::channel();
        self.response_manager
            .register_receipt(receipt_id, tx, timeout);
        debug!("Registered receipt expectation: {}", receipt_id);
        Ok(rx)
    }

    async fn has_incoming_connection_from(&self, instance_id: InstanceId) -> bool {
        self.has_incoming_connection_from(instance_id).await
    }

    fn clone_as_arc(&self) -> Arc<dyn ActiveMessageClient> {
        // This is a bit tricky - we need to return Arc<dyn ActiveMessageClient>
        // but we can't clone self directly. For now, we'll need to refactor this
        // or use a different pattern.
        // TODO: Consider using Arc<Self> pattern throughout
        unimplemented!("clone_as_arc requires refactoring to use Arc<Self> pattern")
    }
}
