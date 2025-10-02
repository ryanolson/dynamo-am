// SPDX-FileCopyrightText: Copyright (c) 2024-2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! System handlers using the active message architecture.
//!
//! These handlers provide core system functionality with clean separation
//! between active message handlers and unary handlers (request-response).

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{debug, info, warn};

use super::client::{ActiveMessageClient, PeerInfo};
use super::handler::InstanceId;
use super::handler_impls::TypedUnaryHandler;
use super::receipt_ack::{ReceiptAck, ReceiptStatus};
use super::responses::{
    DiscoverResponse, HealthCheckResponse, ListHandlersResponse, RegisterServiceResponse,
    RemoveServiceResponse, RequestShutdownResponse, WaitForHandlerResponse,
};

// Note: System handlers now use the transport-agnostic cohort module
// use crate::cohort::LeaderWorkerCohort; // Available if needed

/// Health check handler - returns system status
#[derive(Debug)]
pub struct HealthCheckHandler;

#[async_trait]
impl TypedUnaryHandler<(), HealthCheckResponse> for HealthCheckHandler {
    async fn process(
        &self,
        _input: (),
        _sender_id: InstanceId,
        _client: Arc<dyn ActiveMessageClient>,
    ) -> Result<HealthCheckResponse, String> {
        debug!("Health check received");

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        Ok(HealthCheckResponse {
            status: "ok".to_string(),
            timestamp,
        })
    }

    fn name(&self) -> &str {
        "_health_check"
    }
}

/// Discovery handler - returns instance and endpoint information for peer discovery
#[derive(Debug)]
pub struct DiscoverHandler {
    client: Arc<dyn ActiveMessageClient>,
}

impl DiscoverHandler {
    pub fn new(client: Arc<dyn ActiveMessageClient>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl TypedUnaryHandler<(), DiscoverResponse> for DiscoverHandler {
    async fn process(
        &self,
        _input: (),
        _sender_id: InstanceId,
        _client: Arc<dyn ActiveMessageClient>,
    ) -> Result<DiscoverResponse, String> {
        debug!("Discovery request received");

        // Get peer info from our client
        let peer_info = self.client.peer_info();

        Ok(DiscoverResponse {
            instance_id: peer_info.instance_id.to_string(),
            tcp_endpoint: peer_info.tcp_endpoint().map(|s| s.to_string()),
            ipc_endpoint: peer_info.ipc_endpoint().map(|s| s.to_string()),
        })
    }

    fn name(&self) -> &str {
        "_discover"
    }
}

/// Receipt ACK handler - processes receipt acknowledgments from remote instances
/// This is an active message handler since it doesn't need to send responses
#[derive(Debug)]
pub struct ReceiptAckHandler {
    // Will hold receipt tracking state when implemented
    _phantom: std::marker::PhantomData<()>,
}

impl Default for ReceiptAckHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl ReceiptAckHandler {
    pub fn new() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }

    /// Process a receipt ACK message
    /// This is called when we receive a receipt ACK from a remote instance
    /// indicating that our message was delivered (or failed to be delivered)
    pub fn process_receipt_ack(&self, ack: ReceiptAck) -> Result<(), String> {
        match ack.status {
            ReceiptStatus::Delivered => {
                debug!("Message {} delivered successfully", ack.message_id);
                // TODO: Complete the pending send future for this message
                Ok(())
            }
            ReceiptStatus::ContractMismatch(ref error) => {
                warn!(
                    "Contract mismatch for message {}: {}",
                    ack.message_id, error
                );
                // TODO: Complete the pending send future with error
                Err(format!("Contract mismatch: {}", error))
            }
            ReceiptStatus::HandlerNotFound => {
                warn!("Handler not found for message {}", ack.message_id);
                // TODO: Complete the pending send future with error
                Err("Handler not found".to_string())
            }
            ReceiptStatus::InvalidMessage(ref error) => {
                warn!("Invalid message {}: {}", ack.message_id, error);
                // TODO: Complete the pending send future with error
                Err(format!("Invalid message: {}", error))
            }
        }
    }
}

/// Service registration handler - handles bidirectional connection establishment
#[derive(Debug)]
pub struct RegisterServiceHandler {
    client: Arc<dyn ActiveMessageClient>,
}

impl RegisterServiceHandler {
    pub fn new(client: Arc<dyn ActiveMessageClient>) -> Self {
        Self { client }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct RegisterServicePayload {
    instance_id: String,
    /// Legacy single endpoint field for backward compatibility
    #[serde(skip_serializing_if = "Option::is_none")]
    endpoint: Option<String>,
    /// TCP endpoint for cross-host communication
    #[serde(skip_serializing_if = "Option::is_none")]
    tcp_endpoint: Option<String>,
    /// IPC endpoint for same-host optimization
    #[serde(skip_serializing_if = "Option::is_none")]
    ipc_endpoint: Option<String>,
}

#[async_trait]
impl TypedUnaryHandler<RegisterServicePayload, RegisterServiceResponse> for RegisterServiceHandler {
    async fn process(
        &self,
        input: RegisterServicePayload,
        _sender_id: InstanceId,
        _client: Arc<dyn ActiveMessageClient>,
    ) -> Result<RegisterServiceResponse, String> {
        let instance_id = uuid::Uuid::parse_str(&input.instance_id)
            .map_err(|e| format!("Invalid instance ID: {}", e))?;

        // Create PeerInfo with dual endpoint support, handling backward compatibility
        let peer = if input.tcp_endpoint.is_some() || input.ipc_endpoint.is_some() {
            // New dual endpoint format
            PeerInfo::new_dual(
                instance_id,
                input.tcp_endpoint.clone(),
                input.ipc_endpoint.clone(),
            )
        } else if let Some(ref endpoint) = input.endpoint {
            // Legacy single endpoint format
            PeerInfo::new(instance_id, endpoint)
        } else {
            return Err("No endpoint provided in registration payload".to_string());
        };

        // Clone values needed for both async task and response
        let instance_id_for_task = input.instance_id.clone();
        let tcp_endpoint_for_task = input.tcp_endpoint.clone();
        let ipc_endpoint_for_task = input.ipc_endpoint.clone();
        let endpoint_for_task = input.endpoint.clone();

        // This will be handled asynchronously by spawning a task
        let client = self.client.clone();
        tokio::spawn(async move {
            if let Ok(()) = client.connect_to_peer(peer).await {
                let endpoint_desc = match (&tcp_endpoint_for_task, &ipc_endpoint_for_task) {
                    (Some(tcp), Some(ipc)) => format!("TCP: {}, IPC: {}", tcp, ipc),
                    (Some(tcp), None) => format!("TCP: {}", tcp),
                    (None, Some(ipc)) => format!("IPC: {}", ipc),
                    (None, None) => endpoint_for_task
                        .as_ref()
                        .unwrap_or(&"unknown".to_string())
                        .clone(),
                };
                info!(
                    "Registered service {} at {}",
                    instance_id_for_task, endpoint_desc
                );
            } else {
                debug!("Failed to register service {}", instance_id_for_task);
            }
        });

        // For response, use the primary endpoint (TCP if available, otherwise legacy endpoint)
        let response_endpoint = input
            .tcp_endpoint
            .or(input.endpoint)
            .unwrap_or_else(|| "unknown".to_string());

        // Always return successful registration for now
        // The actual connection happens asynchronously
        Ok(RegisterServiceResponse {
            registered: true,
            instance_id: input.instance_id,
            endpoint: response_endpoint,
        })
    }

    fn name(&self) -> &str {
        "_register_service"
    }
}

// Note: Old trait-based handler implementations have been removed in favor of
// the v2 closure-based pattern implemented in create_core_system_handlers() below

// ============================================================================
// Integration Helper Functions for v2 System Handlers
// ============================================================================

/// Create the core system handlers that don't depend on ZMQ manager state
/// Returns a list of (name, dispatcher) pairs ready for registration
pub fn create_core_system_handlers(
    client: Arc<dyn ActiveMessageClient>,
    task_tracker: tokio_util::task::TaskTracker,
) -> Vec<(String, Arc<dyn super::dispatcher::ActiveMessageDispatcher>)> {
    use super::handler_impls::{am_handler_with_tracker, typed_unary_handler_with_tracker};

    let mut handlers = Vec::new();

    // Health check handler (unary - returns health status)
    // typed_unary_handler_with_tracker returns Arc<dyn ActiveMessageDispatcher>
    let health_dispatcher = typed_unary_handler_with_tracker(
        "_health_check".to_string(),
        move |_ctx: super::handler_impls::TypedContext<()>| {
            // Create the health check response directly
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0);

            Ok(HealthCheckResponse {
                status: "ok".to_string(),
                timestamp,
            })
        },
        task_tracker.clone(),
    );

    handlers.push(("_health_check".to_string(), health_dispatcher));

    // Discovery handler (unary - returns instance and endpoint information)
    let client_for_discover = client.clone();
    let discover_dispatcher = typed_unary_handler_with_tracker(
        "_discover".to_string(),
        move |_ctx: super::handler_impls::TypedContext<()>| {
            // Get peer info from our client
            let peer_info = client_for_discover.peer_info();

            Ok(DiscoverResponse {
                instance_id: peer_info.instance_id.to_string(),
                tcp_endpoint: peer_info.tcp_endpoint().map(|s| s.to_string()),
                ipc_endpoint: peer_info.ipc_endpoint().map(|s| s.to_string()),
            })
        },
        task_tracker.clone(),
    );
    handlers.push(("_discover".to_string(), discover_dispatcher));

    // Receipt ACK handler (active message - no response)
    // am_handler_with_tracker returns Arc<dyn ActiveMessageDispatcher>
    let receipt_dispatcher = am_handler_with_tracker(
        "_receipt_ack".to_string(),
        move |ctx: super::handler_impls::AmContext| async move {
            // Deserialize the receipt ACK
            let ack: ReceiptAck = match serde_json::from_slice(&ctx.payload) {
                Ok(ack) => ack,
                Err(e) => {
                    tracing::error!("Failed to deserialize receipt ACK: {}", e);
                    return Ok(()); // Don't propagate error for receipt ACKs
                }
            };

            // Complete the receipt ACK
            // For now, we just log - proper integration requires response manager access
            tracing::info!(
                "Received receipt ACK for message {}: {:?}",
                ack.message_id,
                ack.status
            );

            Ok(())
        },
        task_tracker.clone(),
    );

    handlers.push(("_receipt_ack".to_string(), receipt_dispatcher));

    // Register service handler (unary - returns registration status)
    let register_dispatcher = typed_unary_handler_with_tracker(
        "_register_service".to_string(),
        move |ctx: super::handler_impls::TypedContext<serde_json::Value>| {
            tracing::info!("Received service registration request: {:?}", ctx.input);
            // For now, always accept registrations
            // In a real implementation, this would connect to a service registry
            Ok(RegisterServiceResponse {
                registered: true,
                instance_id: ctx.sender_id.to_string(),
                endpoint: "tcp://127.0.0.1:0".to_string(), // Placeholder
            })
        },
        task_tracker.clone(),
    );
    handlers.push(("_register_service".to_string(), register_dispatcher));

    // NOTE: _join_cohort handler is NOT a core system handler
    // It should be registered by LeaderWorkerCohort instances via cohort.register_handlers()
    // This ensures cohort logic is isolated to the leader that owns the cohort

    // Remove service handler (unary - returns removal status)
    let remove_service_dispatcher = typed_unary_handler_with_tracker(
        "_remove_service".to_string(),
        move |ctx: super::handler_impls::TypedContext<serde_json::Value>| {
            tracing::info!("Received service removal request: {:?}", ctx.input);
            // For now, always acknowledge removals
            // In a real implementation, this would update the service registry
            Ok(RemoveServiceResponse {
                removed: true,
                instance_id: ctx.sender_id.to_string(),
                rank: None, // TODO: get rank from the request or registry
            })
        },
        task_tracker.clone(),
    );
    handlers.push(("_remove_service".to_string(), remove_service_dispatcher));

    // Request shutdown handler (unary - returns acknowledgment)
    let shutdown_dispatcher = typed_unary_handler_with_tracker(
        "_request_shutdown".to_string(),
        move |ctx: super::handler_impls::TypedContext<serde_json::Value>| {
            tracing::info!("Received shutdown request: {:?}", ctx.input);
            // For now, always acknowledge shutdown requests
            // In a real implementation, this would trigger graceful shutdown
            Ok(RequestShutdownResponse { acknowledged: true })
        },
        task_tracker.clone(),
    );
    handlers.push(("_request_shutdown".to_string(), shutdown_dispatcher));

    handlers
}

/// Register system handlers with a control message sender.
///
/// This is the recommended way to add system handlers to a running dispatcher.
/// Handlers support graceful cancellation via the provided cancel token.
///
/// # Arguments
/// * `control_tx` - Channel to send control messages to the dispatcher
/// * `client` - Active message client for handler context
/// * `task_tracker` - Tracker for managing handler task lifecycle
/// * `cancel_token` - Token for graceful shutdown of long-running handlers
pub async fn register_system_handlers(
    control_tx: &tokio::sync::mpsc::Sender<super::dispatcher::ControlMessage>,
    client: Arc<dyn ActiveMessageClient>,
    task_tracker: tokio_util::task::TaskTracker,
    cancel_token: tokio_util::sync::CancellationToken,
) -> anyhow::Result<()> {
    let mut handlers = create_core_system_handlers(client, task_tracker.clone());

    // Add handler discovery system handlers that need access to control_tx
    let control_tx_for_list = control_tx.clone();
    let list_handlers_dispatcher = super::handler_impls::typed_unary_handler_async_with_tracker(
        "_list_handlers".to_string(),
        move |_ctx: super::handler_impls::TypedContext<()>| {
            let control_tx = control_tx_for_list.clone();
            async move {
                tracing::debug!("_list_handlers: Handler invoked");
                let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
                let control_msg = super::dispatcher::ControlMessage::ListHandlers { reply_tx };

                tracing::debug!("_list_handlers: Sending control message to dispatcher");
                control_tx
                    .send(control_msg)
                    .await
                    .map_err(|e| format!("Failed to send ListHandlers control message: {}", e))?;

                tracing::debug!("_list_handlers: Waiting for reply from dispatcher");
                let handlers = reply_rx
                    .await
                    .map_err(|e| format!("Failed to receive handler list: {}", e))?;

                tracing::debug!(
                    "_list_handlers: Got {} handlers from dispatcher",
                    handlers.len()
                );
                Ok(ListHandlersResponse { handlers })
            }
        },
        task_tracker.clone(),
    );
    handlers.push(("_list_handlers".to_string(), list_handlers_dispatcher));

    // Add wait_for_handler system handler
    let control_tx_for_wait = control_tx.clone();
    let cancel_token_for_wait = cancel_token.clone();
    let wait_for_handler_dispatcher = super::handler_impls::typed_unary_handler_async_with_tracker(
        "_wait_for_handler".to_string(),
        move |ctx: super::handler_impls::TypedContext<serde_json::Value>| {
            let control_tx = control_tx_for_wait.clone();
            let cancel_token = cancel_token_for_wait.clone();
            async move {
                // Parse handler name and timeout from input
                let handler_name = ctx
                    .input
                    .get("handler_name")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| "Missing handler_name in request".to_string())?
                    .to_string();

                let timeout_ms = ctx
                    .input
                    .get("timeout_ms")
                    .and_then(|v| v.as_u64())
                    .map(Duration::from_millis);

                // Poll for handler with timeout
                let start = std::time::Instant::now();
                let max_wait = timeout_ms.unwrap_or(Duration::from_secs(30));

                loop {
                    // Check for cancellation first
                    tokio::select! {
                        _ = cancel_token.cancelled() => {
                            tracing::debug!("_wait_for_handler cancelled during shutdown");
                            return Ok(WaitForHandlerResponse {
                                handler_name,
                                available: false,
                            });
                        }
                        _ = tokio::time::sleep(Duration::from_millis(0)) => {
                            // Continue with handler query
                        }
                    }

                    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
                    let control_msg = super::dispatcher::ControlMessage::QueryHandler {
                        name: handler_name.clone(),
                        reply_tx,
                    };

                    control_tx.send(control_msg).await.map_err(|e| {
                        format!("Failed to send QueryHandler control message: {}", e)
                    })?;

                    let exists = reply_rx
                        .await
                        .map_err(|e| format!("Failed to receive handler query result: {}", e))?;

                    if exists {
                        return Ok(WaitForHandlerResponse {
                            handler_name: handler_name.clone(),
                            available: true,
                        });
                    }

                    if start.elapsed() >= max_wait {
                        return Ok(WaitForHandlerResponse {
                            handler_name,
                            available: false,
                        });
                    }

                    // Wait a bit before polling again, also checking for cancellation
                    tokio::select! {
                        _ = cancel_token.cancelled() => {
                            tracing::debug!("_wait_for_handler cancelled during sleep");
                            return Ok(WaitForHandlerResponse {
                                handler_name,
                                available: false,
                            });
                        }
                        _ = tokio::time::sleep(Duration::from_millis(100)) => {
                            // Continue polling
                        }
                    }
                }
            }
        },
        task_tracker.clone(),
    );
    handlers.push(("_wait_for_handler".to_string(), wait_for_handler_dispatcher));

    // Register all handlers
    for (name, dispatcher) in handlers {
        let control_msg = super::dispatcher::ControlMessage::Register { name, dispatcher };
        control_tx
            .send(control_msg)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to send registration control message: {}", e))?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::sync::Arc;
    use uuid::Uuid;

    #[derive(Debug, Clone)]
    struct MockClient {
        instance_id: Uuid,
        endpoint: String,
    }

    #[async_trait]
    impl ActiveMessageClient for MockClient {
        fn instance_id(&self) -> Uuid {
            self.instance_id
        }

        fn endpoint(&self) -> &str {
            &self.endpoint
        }

        fn peer_info(&self) -> PeerInfo {
            PeerInfo::new(self.instance_id, &self.endpoint)
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

        async fn list_peers(&self) -> anyhow::Result<Vec<PeerInfo>> {
            Ok(vec![])
        }

        async fn connect_to_peer(&self, _peer: PeerInfo) -> anyhow::Result<()> {
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
    async fn test_health_check_handler() {
        let handler = HealthCheckHandler;

        // Create mock client
        let mock_client = Arc::new(MockClient {
            instance_id: Uuid::new_v4(),
            endpoint: "test://localhost".to_string(),
        });

        let result = handler.process((), Uuid::new_v4(), mock_client).await;
        assert!(result.is_ok());
        let response = result.unwrap();
        assert_eq!(response.status, "ok");
        assert!(response.timestamp > 0);
    }
}
