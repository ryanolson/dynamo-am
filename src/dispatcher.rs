// SPDX-FileCopyrightText: Copyright (c) 2024-2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Message dispatcher for transport-agnostic active message routing.
//!
//! The dispatcher receives messages from transport layers and routes them to
//! appropriate handlers. It supports multiple dispatch modes (inline, spawned, batched)
//! and provides comprehensive measurement infrastructure.

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{RwLock, mpsc};
use tokio_util::task::TaskTracker;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use super::client::{ActiveMessageClient, PeerInfo};
use super::handler::InstanceId;
use super::receipt_ack::{ClientExpectation, ContractInfo, HandlerType, ReceiptAck, ReceiptStatus};

/// A transport-agnostic active message for the dispatcher.
#[derive(Debug, Clone)]
pub struct DispatchMessage {
    /// Original message ID (needed for response correlation)
    pub message_id: uuid::Uuid,

    /// Handler name to route to
    pub handler_name: String,

    /// Message payload
    pub payload: Bytes,

    /// Sender identity information
    pub sender_identity: SenderIdentity,

    /// Optional metadata (None for fastest path)
    pub metadata: Option<Bytes>,

    /// When the transport layer received this message
    pub received_at: Instant,
}

/// Identifies the sender of a message.
#[derive(Debug, Clone)]
pub enum SenderIdentity {
    /// Sender is already registered and bidirectionally connected
    Known(InstanceId),

    /// Sender needs registration (includes endpoint for auto-registration)
    Unknown(PeerInfo),

    /// Anonymous sender (no return path needed)
    Anonymous,
}

/// Address of a message sender (non-anonymous only).
#[derive(Debug, Clone)]
pub enum SenderAddress {
    /// Sender with established bidirectional connection
    Connected(InstanceId),

    /// Sender that needs registration (includes endpoint info)
    Unconnected(PeerInfo),
}

impl SenderAddress {
    /// Get the instance ID
    pub fn instance_id(&self) -> InstanceId {
        match self {
            SenderAddress::Connected(id) => *id,
            SenderAddress::Unconnected(info) => info.instance_id,
        }
    }

    /// Check if sender is connected
    pub fn is_connected(&self) -> bool {
        matches!(self, SenderAddress::Connected(_))
    }

    /// Get PeerInfo for connection if not connected
    pub fn peer_info(&self) -> Option<&PeerInfo> {
        match self {
            SenderAddress::Connected(_) => None,
            SenderAddress::Unconnected(info) => Some(info),
        }
    }
}

/// How a handler should be dispatched.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DispatchMode {
    /// Execute inline in dispatcher thread (ultra-low latency)
    Inline,

    /// Spawn on task tracker (normal async execution)
    Spawn,

    /// Queue for later batch processing
    Batched,
}

/// Parsed metadata for response handling
#[derive(Debug, Clone)]
pub struct ParsedMetadata {
    pub response_id: Option<Uuid>,
    pub receipt_id: Option<Uuid>,
    pub client_expectation: Option<ClientExpectation>,
}

/// Context passed to handlers during dispatch.
#[derive(Clone)]
pub struct DispatchContext {
    /// The message being dispatched
    pub message: DispatchMessage,

    /// Sender address (never anonymous)
    pub sender_address: SenderAddress,

    /// Client for sending responses
    pub client: Arc<dyn ActiveMessageClient>,

    /// Parsed metadata for response handling
    pub parsed_metadata: ParsedMetadata,

    /// Measurement tracking (only when sampling)
    pub trace: Option<Arc<RwLock<MessageTrace>>>,
}

/// Metrics collected for a single message.
#[derive(Debug, Clone)]
pub struct MessageTrace {
    pub message_id: Uuid,
    pub handler_name: String,
    pub received_at: Instant,
    pub queued_at: Option<Instant>,
    pub dispatch_started: Option<Instant>,
    pub handler_started: Option<Instant>,
    pub handler_completed: Option<Instant>,
    pub payload_size: usize,
    pub metadata_size: usize,
    pub dispatch_mode: DispatchMode,
    pub result: Result<(), String>,
}

impl MessageTrace {
    pub fn new(handler_name: String, received_at: Instant) -> Self {
        Self {
            message_id: Uuid::new_v4(),
            handler_name,
            received_at,
            queued_at: None,
            dispatch_started: None,
            handler_started: None,
            handler_completed: None,
            payload_size: 0,
            metadata_size: 0,
            dispatch_mode: DispatchMode::Spawn,
            result: Ok(()),
        }
    }

    /// Calculate total end-to-end latency
    pub fn total_latency(&self) -> Option<Duration> {
        self.handler_completed.map(|end| end - self.received_at)
    }

    /// Calculate queue wait time
    pub fn queue_latency(&self) -> Option<Duration> {
        match (self.queued_at, self.dispatch_started) {
            (Some(queued), Some(started)) => Some(started - queued),
            _ => None,
        }
    }

    /// Calculate handler execution time
    pub fn handler_latency(&self) -> Option<Duration> {
        match (self.handler_started, self.handler_completed) {
            (Some(started), Some(completed)) => Some(completed - started),
            _ => None,
        }
    }
}

/// Base trait for active message handlers.
///
/// This is the lowest level handler interface for active message processing.
/// Handlers process messages but don't return responses - they handle their own
/// response logic if needed. For request-response patterns, use UnaryHandler.
#[async_trait]
pub trait ActiveMessageHandler: Send + Sync {
    /// How this handler should be dispatched
    fn dispatch_mode(&self) -> DispatchMode {
        DispatchMode::Spawn
    }

    /// Handle a message asynchronously - active message semantics
    async fn handle(
        &self,
        message_id: uuid::Uuid,
        payload: Bytes,
        sender: SenderAddress,
        client: Arc<dyn ActiveMessageClient>,
    );

    /// Handle a message inline (for ultra-low latency)
    ///
    /// Default implementation panics - handlers must opt-in to inline execution
    fn handle_inline(&self, _ctx: DispatchContext) {
        panic!("Handler does not support inline execution");
    }

    /// Get the handler name
    fn name(&self) -> &str;

    /// Get contract information for this handler
    fn contract_info(&self) -> ContractInfo {
        // Default implementation for pure active message handlers
        ContractInfo {
            handler_type: HandlerType::ActiveMessage,
            response_type: None,
            supports_operation: true,
        }
    }
}

/// Trait for dispatching messages to handlers.
///
/// Implementations wrap handlers with dispatch logic (spawn, inline, etc.)
#[async_trait]
pub trait ActiveMessageDispatcher: Send + Sync {
    /// Get the handler name
    fn name(&self) -> &str;

    /// Dispatch a message to the handler
    async fn dispatch(&self, ctx: DispatchContext);

    /// Get contract information for this handler
    fn contract_info(&self) -> ContractInfo;
}

/// Dispatcher implementation that spawns handlers on a task tracker.
pub struct SpawnedDispatcher<H: ActiveMessageHandler> {
    handler: Arc<H>,
    task_tracker: TaskTracker,
}

impl<H: ActiveMessageHandler> SpawnedDispatcher<H> {
    pub fn new(handler: H, task_tracker: TaskTracker) -> Self {
        Self {
            handler: Arc::new(handler),
            task_tracker,
        }
    }
}

#[async_trait]
impl<H: ActiveMessageHandler + 'static> ActiveMessageDispatcher for SpawnedDispatcher<H> {
    fn name(&self) -> &str {
        self.handler.name()
    }

    async fn dispatch(&self, ctx: DispatchContext) {
        let handler = self.handler.clone();
        let trace = ctx.trace.clone();

        self.task_tracker.spawn(async move {
            // Mark handler start (only if tracing)
            if let Some(ref trace) = trace {
                let mut t = trace.write().await;
                t.handler_started = Some(Instant::now());
            }

            // Check if this is a with_response mode message that needs acceptance
            if let Some(ref metadata_bytes) = ctx.message.metadata {
                // Parse metadata as JSON
                if let Ok(metadata) = serde_json::from_slice::<serde_json::Value>(metadata_bytes)
                    && let Some(mode) = metadata.get("_mode").and_then(|v| v.as_str())
                    && mode == "with_response"
                {
                    // Send acceptance message before processing
                    if let Some(accept_id_str) = metadata.get("_accept_id").and_then(|v| v.as_str())
                        && let Ok(accept_id) = uuid::Uuid::parse_str(accept_id_str)
                    {
                        debug!("Sending acceptance for with_response message {}", accept_id);

                        let accept_message = crate::handler::ActiveMessage {
                            message_id: uuid::Uuid::new_v4(),
                            handler_name: "_accept".to_string(),
                            sender_instance: ctx.client.instance_id(),
                            payload: bytes::Bytes::new(),
                            metadata: serde_json::json!({
                                "_accept_for": accept_id.to_string()
                            }),
                        };

                        if let Err(e) = ctx
                            .client
                            .send_raw_message(ctx.sender_address.instance_id(), accept_message)
                            .await
                        {
                            error!("Failed to send acceptance for {}: {}", accept_id, e);
                        }
                    }
                }
            }

            // Execute handler (active message semantics)
            handler
                .handle(
                    ctx.message.message_id,
                    ctx.message.payload,
                    ctx.sender_address,
                    ctx.client,
                )
                .await;

            // Mark handler completion (only if tracing)
            if let Some(ref trace) = trace {
                let mut t = trace.write().await;
                t.handler_completed = Some(Instant::now());
                t.result = Ok(()); // Always success for active messages
            }
        });
    }

    fn contract_info(&self) -> ContractInfo {
        self.handler.contract_info()
    }
}

/// Control messages for dispatcher management.
pub enum ControlMessage {
    /// Register a new handler
    Register {
        name: String,
        dispatcher: Arc<dyn ActiveMessageDispatcher>,
    },

    /// Unregister a handler
    Unregister { name: String },

    /// Query registered handlers
    ListHandlers {
        reply_tx: tokio::sync::oneshot::Sender<Vec<String>>,
    },

    /// Check if specific handler exists
    QueryHandler {
        name: String,
        reply_tx: tokio::sync::oneshot::Sender<bool>,
    },

    /// Shutdown the dispatcher
    Shutdown,
}

/// Main message dispatcher that routes messages to handlers.
pub struct MessageDispatcher {
    /// Handler registry (no RwLock needed - single thread access)
    handlers: HashMap<String, Arc<dyn ActiveMessageDispatcher>>,

    /// Client for auto-registration and responses
    client: Arc<dyn ActiveMessageClient>,

    /// Task tracker for spawned handlers
    task_tracker: TaskTracker,

    /// Channel for receiving messages
    message_rx: mpsc::Receiver<DispatchMessage>,

    /// Channel for control messages (registration, etc.)
    control_rx: mpsc::Receiver<ControlMessage>,

    /// Metrics collection (sampled)
    metrics_tx: Option<mpsc::Sender<MessageTrace>>,

    /// Sampling rate for metrics (0.0 - 1.0)
    sampling_rate: f64,
}

impl MessageDispatcher {
    /// Create a new message dispatcher
    pub fn new(
        client: Arc<dyn ActiveMessageClient>,
        message_rx: mpsc::Receiver<DispatchMessage>,
        control_rx: mpsc::Receiver<ControlMessage>,
        task_tracker: TaskTracker,
    ) -> Self {
        Self {
            handlers: HashMap::new(),
            client,
            task_tracker,
            message_rx,
            control_rx,
            metrics_tx: None,
            sampling_rate: 0.01, // Sample 1% by default
        }
    }

    /// Enable metrics collection
    pub fn with_metrics(mut self, tx: mpsc::Sender<MessageTrace>, sampling_rate: f64) -> Self {
        self.metrics_tx = Some(tx);
        self.sampling_rate = sampling_rate;
        self
    }

    /// Run the dispatcher loop
    pub async fn run(mut self) -> Result<()> {
        info!("Message dispatcher starting");

        loop {
            tokio::select! {
                // Prioritize message processing
                biased;

                // Process messages
                Some(message) = self.message_rx.recv() => {
                    self.dispatch_message(message).await;
                }

                // Process control messages
                Some(control) = self.control_rx.recv() => {
                    match control {
                        ControlMessage::Register { name, dispatcher } => {
                            self.register_handler(name, dispatcher);
                        }
                        ControlMessage::Unregister { name } => {
                            self.unregister_handler(name);
                        }
                        ControlMessage::ListHandlers { reply_tx } => {
                            let handlers: Vec<String> = self.handlers.keys().cloned().collect();
                            let _ = reply_tx.send(handlers);
                        }
                        ControlMessage::QueryHandler { name, reply_tx } => {
                            let exists = self.handlers.contains_key(&name);
                            let _ = reply_tx.send(exists);
                        }
                        ControlMessage::Shutdown => {
                            info!("Dispatcher shutting down");
                            break;
                        }
                    }
                }

                // Both channels closed
                else => {
                    warn!("All dispatcher channels closed");
                    break;
                }
            }
        }

        // Wait for all spawned tasks to complete
        self.task_tracker.close();
        self.task_tracker.wait().await;

        info!("Message dispatcher stopped");
        Ok(())
    }

    /// Dispatch a message to the appropriate handler
    async fn dispatch_message(&self, message: DispatchMessage) {
        let handler_name = message.handler_name.clone();

        // Create trace if sampling
        let should_trace = self.should_sample();
        let trace = if should_trace {
            let mut trace = MessageTrace::new(handler_name.clone(), message.received_at);
            trace.queued_at = Some(Instant::now());
            trace.payload_size = message.payload.len();
            trace.metadata_size = message.metadata.as_ref().map(|m| m.len()).unwrap_or(0);
            Some(Arc::new(RwLock::new(trace)))
        } else {
            None
        };

        // Mark dispatch start
        if let Some(ref t) = trace {
            t.write().await.dispatch_started = Some(Instant::now());
        }

        // Look up handler
        let dispatcher = match self.handlers.get(&handler_name) {
            Some(d) => d.clone(),
            None => {
                error!("No handler registered for '{}'", handler_name);
                if let Some(ref t) = trace {
                    t.write().await.result = Err(format!("Handler not found: {}", handler_name));
                }
                return;
            }
        };

        // Convert sender identity to SenderAddress
        let sender_address = match &message.sender_identity {
            SenderIdentity::Known(id) => SenderAddress::Connected(*id),
            SenderIdentity::Unknown(peer_info) => SenderAddress::Unconnected(peer_info.clone()),
            SenderIdentity::Anonymous => {
                error!(
                    "Received message with anonymous sender for handler '{}' - this should not happen",
                    handler_name
                );
                if let Some(ref t) = trace {
                    t.write().await.result = Err("Anonymous sender not allowed".to_string());
                }
                return;
            }
        };

        // Parse metadata for response handling and receipt ACKs
        let parsed_metadata = if let Some(ref meta_bytes) = message.metadata {
            // Try to parse as JSON metadata
            if let Ok(metadata_json) = serde_json::from_slice::<serde_json::Value>(meta_bytes) {
                ParsedMetadata {
                    response_id: metadata_json
                        .get("response_id")
                        .and_then(|v| v.as_str())
                        .and_then(|s| Uuid::parse_str(s).ok()),
                    receipt_id: metadata_json
                        .get("_receipt_id")
                        .and_then(|v| v.as_str())
                        .and_then(|s| Uuid::parse_str(s).ok()),
                    client_expectation: metadata_json
                        .get("_client_expectation")
                        .and_then(|v| serde_json::from_value(v.clone()).ok()),
                }
            } else {
                // Fall back to legacy ResponseMetadata parsing
                serde_json::from_slice::<super::handler_impls::ResponseMetadata>(meta_bytes)
                    .map(|meta| ParsedMetadata {
                        response_id: meta.response_id,
                        receipt_id: None,
                        client_expectation: None,
                    })
                    .unwrap_or(ParsedMetadata {
                        response_id: None,
                        receipt_id: None,
                        client_expectation: None,
                    })
            }
        } else {
            ParsedMetadata {
                response_id: None,
                receipt_id: None,
                client_expectation: None,
            }
        };

        // Contract validation for receipt ACK
        if let Some(ref client_expectation) = parsed_metadata.client_expectation {
            let handler_contract = dispatcher.contract_info();

            // Validate client expectation against handler contract
            let validation_result = client_expectation.validate_against(&handler_contract);

            // Send receipt ACK if receipt_id is present
            if let Some(receipt_id) = parsed_metadata.receipt_id {
                let receipt_status = match validation_result {
                    Ok(()) => ReceiptStatus::Delivered,
                    Err(error_msg) => {
                        warn!(
                            "Contract validation failed for handler '{}': {}",
                            handler_name, error_msg
                        );
                        ReceiptStatus::ContractMismatch(error_msg)
                    }
                };

                let receipt_ack = ReceiptAck {
                    message_id: receipt_id,
                    status: receipt_status.clone(),
                    contract_info: if matches!(receipt_status, ReceiptStatus::Delivered) {
                        Some(handler_contract)
                    } else {
                        None
                    },
                };

                // Send receipt ACK back to the sender
                if let SenderAddress::Connected(sender_id) = sender_address {
                    let receipt_payload = match serde_json::to_vec(&receipt_ack) {
                        Ok(bytes) => Bytes::from(bytes),
                        Err(e) => {
                            error!("Failed to serialize receipt ACK: {}", e);
                            return;
                        }
                    };

                    if let Err(e) = self
                        .client
                        .send_message(sender_id, "_receipt_ack", receipt_payload)
                        .await
                    {
                        error!("Failed to send receipt ACK: {}", e);
                    }
                } else {
                    warn!("Cannot send receipt ACK to unconnected sender");
                }

                // Only proceed with dispatch if validation passed
                if !matches!(receipt_status, ReceiptStatus::Delivered) {
                    return;
                }
            } else {
                // Validation failed but no receipt expected, just log and continue
                if let Err(error_msg) = validation_result {
                    warn!(
                        "Contract validation failed for handler '{}': {}",
                        handler_name, error_msg
                    );
                }
            }
        }

        // Create dispatch context
        let ctx = DispatchContext {
            message,
            sender_address,
            client: self.client.clone(),
            parsed_metadata,
            trace: trace.clone(),
        };

        // Dispatch to handler
        dispatcher.dispatch(ctx).await;

        // Send trace to metrics collector if enabled
        if let Some(ref trace) = trace
            && let Some(ref tx) = self.metrics_tx
        {
            let trace_data = trace.read().await.clone();
            let _ = tx.send(trace_data).await;
        }
    }

    /// Determine if we should sample this message for metrics
    fn should_sample(&self) -> bool {
        use rand::Rng;
        self.metrics_tx.is_some() && rand::rng().random_bool(self.sampling_rate)
    }

    /// Register a handler
    fn register_handler(&mut self, name: String, dispatcher: Arc<dyn ActiveMessageDispatcher>) {
        info!("Registering handler: {}", name);
        self.handlers.insert(name, dispatcher);
    }

    /// Unregister a handler
    fn unregister_handler(&mut self, name: String) {
        info!("Unregistering handler: {}", name);
        self.handlers.remove(&name);
    }
}

/// Builder for creating a dispatcher with channels
pub struct DispatcherBuilder {
    client: Arc<dyn ActiveMessageClient>,
    message_buffer: usize,
    control_buffer: usize,
    task_tracker: Option<TaskTracker>,
}

impl DispatcherBuilder {
    pub fn new(client: Arc<dyn ActiveMessageClient>) -> Self {
        Self {
            client,
            message_buffer: 10000,
            control_buffer: 100,
            task_tracker: None,
        }
    }

    pub fn message_buffer(mut self, size: usize) -> Self {
        self.message_buffer = size;
        self
    }

    pub fn control_buffer(mut self, size: usize) -> Self {
        self.control_buffer = size;
        self
    }

    pub fn task_tracker(mut self, tracker: TaskTracker) -> Self {
        self.task_tracker = Some(tracker);
        self
    }

    pub fn build(
        self,
    ) -> (
        MessageDispatcher,
        mpsc::Sender<DispatchMessage>,
        mpsc::Sender<ControlMessage>,
    ) {
        let (message_tx, message_rx) = mpsc::channel(self.message_buffer);
        let (control_tx, control_rx) = mpsc::channel(self.control_buffer);
        let task_tracker = self.task_tracker.unwrap_or_default();

        let dispatcher = MessageDispatcher::new(self.client, message_rx, control_rx, task_tracker);

        (dispatcher, message_tx, control_tx)
    }
}
