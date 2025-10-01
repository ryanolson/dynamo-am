// SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use async_trait::async_trait;
use std::{sync::Arc, time::Duration};
use tokio::sync::{Mutex, RwLock, broadcast, mpsc};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tracing::{debug, error, info, warn};

use crate::{
    client::ActiveMessageClient,
    dispatcher::{ControlMessage, MessageDispatcher},
    handler::{ActiveMessage, HandlerEvent, HandlerId, InstanceId},
    manager::ActiveMessageManager,
    message_router::MessageRouter,
    network_client::NetworkClient,
    response_manager::{ResponseManager, SharedResponseManager},
};

use super::{
    discovery, thin_transport::ZmqThinTransport,
    transport::ZmqTransport,
};

/// Builder for ZmqActiveMessageManager with configurable options
#[derive(Debug, Clone)]
pub struct ZmqActiveMessageManagerBuilder {}

impl Default for ZmqActiveMessageManagerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ZmqActiveMessageManagerBuilder {
    /// Create a new builder with default settings
    pub fn new() -> Self {
        Self {}
    }

    /// Build the ZmqActiveMessageManager with the configured options
    pub async fn build(
        self,
        endpoint: String,
        cancel_token: CancellationToken,
    ) -> Result<ZmqActiveMessageManager> {
        ZmqActiveMessageManager::new_with_builder(endpoint, cancel_token, self).await
    }
}

pub struct ManagerState {
    pub instance_id: InstanceId,
    pub tcp_endpoint: Option<String>,
    pub ipc_endpoint: Option<String>,
}

impl std::fmt::Debug for ManagerState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ManagerState")
            .field("instance_id", &self.instance_id)
            .field("tcp_endpoint", &self.tcp_endpoint)
            .field("ipc_endpoint", &self.ipc_endpoint)
            .finish()
    }
}

pub struct ZmqActiveMessageManager {
    state: Arc<RwLock<ManagerState>>,
    client: Arc<NetworkClient>,
    handler_events_tx: broadcast::Sender<HandlerEvent>,
    cancel_token: CancellationToken,
    receiver_task: Arc<Mutex<Option<tokio::task::JoinHandle<Result<()>>>>>,
    ack_cleanup_task: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    message_task_tracker: TaskTracker,
    message_router: MessageRouter,

    // v2 Dispatcher integration
    control_tx: mpsc::Sender<ControlMessage>,
    dispatcher_task: Arc<Mutex<Option<tokio::task::JoinHandle<Result<()>>>>>,
}

impl ZmqActiveMessageManager {
    /// Get the client (now returns concrete NetworkClient)
    pub fn client(&self) -> Arc<NetworkClient> {
        self.client.clone()
    }

    pub fn manager_state(&self) -> Arc<RwLock<ManagerState>> {
        self.state.clone()
    }

    /// Get the control channel sender for dispatcher control messages
    /// This is needed for cohort.register_handlers() and other advanced use cases
    pub fn control_tx(&self) -> &mpsc::Sender<ControlMessage> {
        &self.control_tx
    }

    /// Register a handler with the message dispatcher
    pub async fn register_handler(
        &self,
        name: String,
        handler: Arc<dyn crate::dispatcher::ActiveMessageDispatcher>,
    ) -> Result<()> {
        use crate::dispatcher::ControlMessage;

        self.control_tx
            .send(ControlMessage::Register {
                name,
                dispatcher: handler,
            })
            .await
            .map_err(|e| anyhow::anyhow!("Failed to register handler: {}", e))
    }

    /// Get PeerInfo representing this manager with dual endpoints
    pub async fn peer_info(&self) -> crate::client::PeerInfo {
        let state = self.state.read().await;
        crate::client::PeerInfo::new_dual(
            state.instance_id,
            state.tcp_endpoint.clone(),
            state.ipc_endpoint.clone(),
        )
    }

    /// Create a builder for configuring ZmqActiveMessageManager options
    pub fn builder() -> ZmqActiveMessageManagerBuilder {
        ZmqActiveMessageManagerBuilder::new()
    }

    pub async fn new(endpoint: String, cancel_token: CancellationToken) -> Result<Self> {
        // Use the builder with default settings for backward compatibility
        Self::new_with_builder(
            endpoint,
            cancel_token,
            ZmqActiveMessageManagerBuilder::new(),
        )
        .await
    }

    pub(crate) async fn new_with_builder(
        endpoint: String,
        cancel_token: CancellationToken,
        _builder: ZmqActiveMessageManagerBuilder,
    ) -> Result<Self> {
        let instance_id = InstanceId::new_v4();

        let context = tmq::Context::new();

        // Create TCP transport from provided endpoint
        let tcp_transport = ZmqTransport::new_puller_bound(&context, &endpoint)?;
        let tcp_endpoint = tcp_transport
            .local_endpoint()
            .ok_or_else(|| anyhow::anyhow!("Failed to get bound TCP endpoint"))?
            .clone();

        // Create IPC transport for same-host optimization
        let ipc_endpoint_addr = discovery::create_ipc_endpoint(instance_id);
        let ipc_transport = ZmqTransport::new_puller_bound(&context, &ipc_endpoint_addr)?;
        let ipc_endpoint = ipc_transport
            .local_endpoint()
            .ok_or_else(|| anyhow::anyhow!("Failed to get bound IPC endpoint"))?
            .clone();

        info!(
            "ZmqActiveMessageManager bound to TCP: {} and IPC: {} (instance: {})",
            tcp_endpoint, ipc_endpoint, instance_id
        );

        // Create MPSC channel for merging messages from both transports
        let (message_tx, message_rx) = mpsc::unbounded_channel();

        // Create shared response manager for concurrent access from both client and manager
        let response_manager = Arc::new(ResponseManager::new());

        // Create thin ZMQ transport and wrap in BoxedTransport for type erasure
        let zmq_transport = Arc::new(ZmqThinTransport::new());
        let boxed_transport =
            crate::boxed_transport::BoxedTransport::new(zmq_transport);

        // Create NetworkClient (now concrete, no generics!)
        let client = Arc::new(NetworkClient::new(
            instance_id,
            tcp_endpoint.clone(),
            boxed_transport,
            response_manager.clone(),
        ));

        let (handler_events_tx, _) = broadcast::channel(1024);

        let state = Arc::new(RwLock::new(ManagerState {
            instance_id,
            tcp_endpoint: Some(tcp_endpoint),
            ipc_endpoint: Some(ipc_endpoint),
        }));

        // Initialize concurrency controls
        let message_task_tracker = TaskTracker::new();

        // Set up MessageDispatcher channels
        let (dispatch_tx, dispatch_rx) = mpsc::channel(1000); // Buffered channel for dispatch
        let (control_tx, control_rx) = mpsc::channel(100); // Control channel for handler registration

        // Create MessageDispatcher
        let dispatcher = MessageDispatcher::new(
            client.clone() as Arc<dyn ActiveMessageClient>,
            dispatch_rx,
            control_rx,
            message_task_tracker.clone(),
        );

        // Spawn dispatcher task
        let dispatcher_task = tokio::spawn(async move { dispatcher.run().await });

        // Create MessageRouter for transport-agnostic message processing
        let message_router = MessageRouter::new(
            response_manager.clone(),
            client.clone() as Arc<dyn ActiveMessageClient>,
            dispatch_tx.clone(),
        );

        let manager = Self {
            state: state.clone(),
            client: client.clone(),
            handler_events_tx: handler_events_tx.clone(),
            cancel_token: cancel_token.clone(),
            receiver_task: Arc::new(Mutex::new(None)),
            ack_cleanup_task: Arc::new(Mutex::new(None)),
            message_task_tracker,
            message_router,
            control_tx,
            dispatcher_task: Arc::new(Mutex::new(Some(dispatcher_task))),
        };

        manager.register_builtin_handlers().await?;

        // Spawn transport reader tasks for both TCP and IPC
        let _tcp_reader_task = tokio::spawn(Self::transport_reader_task(
            tcp_transport,
            message_tx.clone(),
            cancel_token.clone(),
            "TCP".to_string(),
        ));

        let _ipc_reader_task = tokio::spawn(Self::transport_reader_task(
            ipc_transport,
            message_tx,
            cancel_token.clone(),
            "IPC".to_string(),
        ));

        // Spawn main receive loop that reads from merged channel
        let receiver_task = tokio::spawn(Self::receive_loop(
            message_rx,
            cancel_token.clone(),
            manager.message_router.clone(),
        ));

        let ack_cleanup_task = tokio::spawn(Self::ack_cleanup_loop(
            response_manager.clone(),
            cancel_token.clone(),
        ));

        // Store the task handles
        *manager.receiver_task.lock().await = Some(receiver_task);
        *manager.ack_cleanup_task.lock().await = Some(ack_cleanup_task);

        // Note: We're not storing tcp_reader_task and ipc_reader_task separately
        // They will be cancelled when cancel_token is triggered

        Ok(manager)
    }

    async fn transport_reader_task(
        mut transport: ZmqTransport,
        message_tx: mpsc::UnboundedSender<ActiveMessage>,
        cancel_token: CancellationToken,
        transport_name: String,
    ) {
        debug!("Starting {} transport reader task", transport_name);

        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    debug!("{} transport reader task cancelled", transport_name);
                    break;
                }

                result = transport.receive() => {
                    match result {
                        Ok(message) => {
                            debug!("Received message on {} transport", transport_name);
                            if message_tx.send(message).is_err() {
                                debug!("{} transport reader: message channel closed", transport_name);
                                break;
                            }
                        }
                        Err(e) => {
                            error!("Failed to receive message on {} transport: {}", transport_name, e);
                            break;
                        }
                    }
                }
            }
        }

        debug!("{} transport reader task finished", transport_name);
    }

    async fn ack_cleanup_loop(
        response_manager: SharedResponseManager,
        cancel_token: CancellationToken,
    ) {
        let mut interval = tokio::time::interval(Duration::from_secs(5));

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let cleaned = response_manager.cleanup_expired();
                    if cleaned > 0 {
                        debug!("Cleaned up {} expired entries", cleaned);
                    }
                }
                _ = cancel_token.cancelled() => {
                    debug!("ACK cleanup loop cancelled");
                    break;
                }
            }
        }
    }

    async fn register_builtin_handlers(&self) -> Result<()> {
        // Register system handlers using the MessageDispatcher
        use crate::system_handlers::register_system_handlers;

        // Register system handlers with the dispatcher
        register_system_handlers(
            &self.control_tx,
            self.client.clone(),
            self.message_task_tracker.clone(),
            self.cancel_token.clone(),
        )
        .await?;

        Ok(())
    }

    async fn receive_loop(
        mut message_rx: mpsc::UnboundedReceiver<ActiveMessage>,
        cancel_token: CancellationToken,
        message_router: MessageRouter,
    ) -> Result<()> {
        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    info!("Receive loop cancelled");
                    break;
                }

                message = message_rx.recv() => {
                    match message {
                        Some(message) => {
                            // Forward all messages to the transport-agnostic MessageRouter
                            if let Err(e) = message_router.route_message(message).await {
                                error!("Failed to route message: {}", e);
                                // Continue processing other messages even if one fails
                            }
                        }
                        None => {
                            debug!("Message channel closed");
                            break;
                        }
                    }
                }
            }
        }

        info!("Receive loop finished");

        Ok(())
    }
}

#[async_trait]
impl ActiveMessageManager for ZmqActiveMessageManager {
    fn client(&self) -> Arc<dyn ActiveMessageClient> {
        self.client.clone()
    }

    async fn deregister_handler(&self, name: &str) -> Result<()> {
        // v2 handlers are managed by MessageDispatcher
        // Send deregistration through control channel
        let control_msg = ControlMessage::Unregister {
            name: name.to_string(),
        };

        if let Err(e) = self.control_tx.send(control_msg).await {
            anyhow::bail!(
                "Failed to deregister handler '{}' with dispatcher: {}",
                name,
                e
            );
        }

        let state = self.state.read().await;
        let event = HandlerEvent::Deregistered {
            name: name.to_string(),
            instance: state.instance_id,
        };
        drop(state);
        let _ = self.handler_events_tx.send(event);

        debug!("Deregistered handler: {}", name);
        Ok(())
    }

    async fn list_handlers(&self) -> Vec<HandlerId> {
        // Query the dispatcher for registered handlers
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        let control_msg = ControlMessage::ListHandlers { reply_tx };

        if let Err(e) = self.control_tx.send(control_msg).await {
            warn!("Failed to send ListHandlers control message: {}", e);
            return Vec::new();
        }

        match reply_rx.await {
            Ok(handler_names) => handler_names, // HandlerId is just String
            Err(e) => {
                warn!("Failed to receive handler list: {}", e);
                Vec::new()
            }
        }
    }

    fn handler_events(&self) -> broadcast::Receiver<HandlerEvent> {
        self.handler_events_tx.subscribe()
    }

    async fn shutdown(&self) -> Result<()> {
        info!("Shutting down ZmqActiveMessageManager");
        self.cancel_token.cancel();

        // Send shutdown signal to the dispatcher
        debug!("Sending shutdown to dispatcher");
        if let Err(e) = self.control_tx.send(ControlMessage::Shutdown).await {
            warn!("Failed to send shutdown to dispatcher: {}", e);
        }

        // Join background tasks first
        if let Some(receiver_task) = self.receiver_task.lock().await.take() {
            debug!("Joining receiver task");
            if let Err(e) = receiver_task.await {
                warn!("Receiver task join error: {:?}", e);
            }
        }

        if let Some(ack_cleanup_task) = self.ack_cleanup_task.lock().await.take() {
            debug!("Joining ACK cleanup task");
            if let Err(e) = ack_cleanup_task.await {
                warn!("ACK cleanup task join error: {:?}", e);
            }
        }

        // Wait for dispatcher to shut down
        if let Some(dispatcher_task) = self.dispatcher_task.lock().await.take() {
            debug!("Joining dispatcher task");
            if let Err(e) = dispatcher_task.await {
                warn!("Dispatcher task join error: {:?}", e);
            }
        }

        // v2 handlers are managed by MessageDispatcher
        // The message_task_tracker will handle cleanup of v2 handler tasks
        debug!("Waiting for all message handlers to complete");
        self.message_task_tracker.close();
        self.message_task_tracker.wait().await;

        info!("ZmqActiveMessageManager shutdown complete");
        Ok(())
    }
}
