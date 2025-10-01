// SPDX-FileCopyrightText: Copyright (c) 2024-2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Clean, context-based handler implementations for active message patterns.
//!
//! This module provides a unified API with consistent context objects:
//!
//! ## Handler Types
//!
//! ### Active Message Handlers
//! - **`am_handler()`** - Receives `AmContext` with sender info, payload, and client
//!   - Return value: `Result<(), String>` for **internal error handling only**
//!   - **NOT sent to sender** - purely for logging
//!
//! ### Request-Response Handlers
//! - **`unary_handler()`** - Receives `UnaryContext` with payload, sender_id, client
//!   - Return value: `UnifiedResponse` **IS sent back to sender**
//!   - `Ok(None)` = ACK, `Ok(Some(bytes))` = Response, `Err(string)` = Error
//!
//! - **`typed_unary_handler()`** - Receives `TypedContext<I>` with deserialized input
//!   - Return value: `Result<O, String>` **IS sent back to sender** (auto-serialized)
//!   - `Ok(output)` = Response, `Err(string)` = Error
//!   - **No SerializableWrapper needed!** 🎉
//!
//! - **`bytes_unary_handler()`** - Receives `TypedContext<I>` but returns raw bytes
//!   - Return value: `Result<Bytes, String>` **IS sent back to sender**
//!   - `Ok(bytes)` = Response, `Err(string)` = Error
//!
//! ## Context Objects
//!
//! All handlers now use consistent context objects instead of scattered parameters:
//! - `AmContext` - For active message handlers
//! - `UnaryContext` - For request-response handlers
//! - `TypedContext<I>` - For typed handlers with automatic deserialization

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;
use tracing::{debug, error};
use uuid::Uuid;

use super::client::ActiveMessageClient;
use super::dispatcher::{ActiveMessageHandler, DispatchMode, SenderAddress};
use super::handler::InstanceId;
use super::receipt_ack::{ContractInfo, HandlerType};

// ============================================================================
// Unified Response Types
// ============================================================================

/// Unified response that consolidates ACK and Response concepts.
///
/// - `Ok(None)` = ACK (success, no payload)
/// - `Ok(Some(bytes))` = Response (success with payload)
/// - `Err(string)` = NACK/Error
pub type UnifiedResponse = std::result::Result<Option<Bytes>, String>;

// ============================================================================
// Request-Response Handler Traits
// ============================================================================

/// Handler trait for request-response patterns.
///
/// UnaryHandler processes a message and returns a response. It guarantees that
/// a response will be sent back to the sender (ACK, Response, or Error).
#[async_trait]
pub trait UnaryHandler: Send + Sync {
    /// Process a request and return a response
    async fn process(
        &self,
        payload: Bytes,
        sender_id: InstanceId,
        client: Arc<dyn ActiveMessageClient>,
    ) -> UnifiedResponse;

    /// Get the handler name
    fn name(&self) -> &str;
}

/// Typed handler trait for request-response patterns with automatic serialization.
///
/// TypedUnaryHandler handles strongly-typed input/output with automatic JSON
/// serialization. Output types just need to implement Serialize - no wrapper needed!
#[async_trait]
pub trait TypedUnaryHandler<I, O>: Send + Sync
where
    I: serde::de::DeserializeOwned + Send,
    O: serde::Serialize + Send,
{
    /// Process typed input and return typed output (automatically serialized)
    async fn process(
        &self,
        input: I,
        sender_id: InstanceId,
        client: Arc<dyn ActiveMessageClient>,
    ) -> Result<O, String>;

    /// Get the handler name
    fn name(&self) -> &str;
}

/// Bytes handler trait for request-response patterns with typed input but raw bytes output.
///
/// BytesUnaryHandler deserializes the input but returns raw bytes as output.
/// Useful when you need structured input but want to return binary data or
/// already-serialized content.
#[async_trait]
pub trait BytesUnaryHandler<I>: Send + Sync
where
    I: serde::de::DeserializeOwned + Send,
{
    /// Process typed input and return raw bytes
    async fn process(
        &self,
        input: I,
        sender_id: InstanceId,
        client: Arc<dyn ActiveMessageClient>,
    ) -> Result<Bytes, String>;

    /// Get the handler name
    fn name(&self) -> &str;
}

// ============================================================================
// Adapter Wrappers
// ============================================================================

/// Wraps a UnaryHandler to implement ActiveMessageHandler
pub struct UnaryHandlerAdapter<H: UnaryHandler> {
    handler: Arc<H>,
    dispatch_mode: DispatchMode,
}

impl<H: UnaryHandler> UnaryHandlerAdapter<H> {
    pub fn new(handler: H, dispatch_mode: DispatchMode) -> Self {
        Self {
            handler: Arc::new(handler),
            dispatch_mode,
        }
    }
}

#[async_trait]
impl<H: UnaryHandler> ActiveMessageHandler for UnaryHandlerAdapter<H> {
    fn dispatch_mode(&self) -> DispatchMode {
        self.dispatch_mode
    }

    async fn handle(
        &self,
        message_id: uuid::Uuid,
        payload: Bytes,
        sender: SenderAddress,
        client: Arc<dyn ActiveMessageClient>,
    ) {
        // UnaryHandler requires connected sender for response capability
        assert!(
            sender.is_connected(),
            "UnaryHandler requires connected sender (dispatcher should auto-register)"
        );
        let sender_id = sender.instance_id();

        // Process request and get response
        let result = self
            .handler
            .process(payload, sender_id, client.clone())
            .await;

        // Use the original message ID for response correlation
        let response_id = message_id;

        // Send appropriate response using helper methods
        debug!(
            "UnaryHandlerAdapter sending response for message {} to {}",
            response_id, sender_id
        );
        let response_result = match result {
            Ok(None) => {
                debug!("Sending ACK for message {}", response_id);
                client.send_ack(response_id, sender_id).await
            }
            Ok(Some(bytes)) => {
                debug!(
                    "Sending response with {} bytes for message {}",
                    bytes.len(),
                    response_id
                );
                client.send_response(response_id, sender_id, bytes).await
            }
            Err(msg) => {
                debug!(
                    "Sending error response for message {}: {}",
                    response_id, msg
                );
                client.send_error(response_id, sender_id, msg).await
            }
        };

        if let Err(e) = response_result {
            error!("Failed to send response: {}", e);
        }
    }

    fn name(&self) -> &str {
        self.handler.name()
    }

    fn contract_info(&self) -> ContractInfo {
        // For now, assume all unary handlers are bytes-based
        // TODO: Detect typed vs bytes handlers properly
        ContractInfo {
            handler_type: HandlerType::UnaryBytes,
            response_type: None,
            supports_operation: true,
        }
    }
}

/// Wraps a TypedUnaryHandler to implement UnaryHandler
pub struct TypedUnaryAdapter<I, O, H> {
    handler: Arc<H>,
    _phantom: PhantomData<(I, O)>,
}

impl<I, O, H> TypedUnaryAdapter<I, O, H> {
    pub fn new(handler: H) -> Self {
        Self {
            handler: Arc::new(handler),
            _phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<I, O, H> UnaryHandler for TypedUnaryAdapter<I, O, H>
where
    I: serde::de::DeserializeOwned + Send + Sync + 'static,
    O: serde::Serialize + Send + Sync + 'static,
    H: TypedUnaryHandler<I, O> + 'static,
{
    async fn process(
        &self,
        payload: Bytes,
        sender_id: InstanceId,
        client: Arc<dyn ActiveMessageClient>,
    ) -> UnifiedResponse {
        // Deserialize input - handle empty payload as null for unit types
        let input: I = if payload.is_empty() {
            serde_json::from_slice(b"null")
                .map_err(|e| format!("Failed to deserialize empty input: {}", e))?
        } else {
            serde_json::from_slice(&payload)
                .map_err(|e| format!("Failed to deserialize input: {}", e))?
        };

        // Process with typed handler
        let output: O = self.handler.process(input, sender_id, client).await?;

        // Automatically serialize output - no wrapper needed!
        let response_bytes = serde_json::to_vec(&output)
            .map_err(|e| format!("Failed to serialize output: {}", e))?;

        Ok(Some(Bytes::from(response_bytes)))
    }

    fn name(&self) -> &str {
        self.handler.name()
    }
}

/// Wraps a BytesUnaryHandler to implement UnaryHandler
pub struct BytesUnaryAdapter<I, H> {
    handler: Arc<H>,
    _phantom: PhantomData<I>,
}

impl<I, H> BytesUnaryAdapter<I, H> {
    pub fn new(handler: H) -> Self {
        Self {
            handler: Arc::new(handler),
            _phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<I, H> UnaryHandler for BytesUnaryAdapter<I, H>
where
    I: serde::de::DeserializeOwned + Send + Sync + 'static,
    H: BytesUnaryHandler<I> + 'static,
{
    async fn process(
        &self,
        payload: Bytes,
        sender_id: InstanceId,
        client: Arc<dyn ActiveMessageClient>,
    ) -> UnifiedResponse {
        // Deserialize input
        let input: I = serde_json::from_slice(&payload)
            .map_err(|e| format!("Failed to deserialize input: {}", e))?;

        // Process with bytes handler
        let output_bytes: Bytes = self.handler.process(input, sender_id, client).await?;

        Ok(Some(output_bytes))
    }

    fn name(&self) -> &str {
        self.handler.name()
    }
}

// ============================================================================
// Level 0: Raw Handlers (Fastest Path)
// ============================================================================

// ============================================================================
// Level 1: Response Handlers (Unified ACK/Response/Error Support)
// ============================================================================

/// Metadata structure for unified response protocol
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponseMetadata {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response_id: Option<Uuid>,
}

// ============================================================================
// Level 1.5: Active Message Handlers
// ============================================================================

/// Context passed to active message handlers
#[derive(Clone)]
pub struct AmContext {
    /// The sender address
    pub sender: SenderAddress,
    /// Client for sending new messages
    pub client: Arc<dyn ActiveMessageClient>,
    /// The original message payload
    pub payload: Bytes,
}

/// Context passed to unary handlers (request-response)
#[derive(Clone)]
pub struct UnaryContext {
    /// The message payload
    pub payload: Bytes,
    /// The sender instance ID (guaranteed connected)
    pub sender_id: InstanceId,
    /// Client for sending additional messages if needed
    pub client: Arc<dyn ActiveMessageClient>,
}

/// Context passed to typed handlers (already deserialized input)
#[derive(Clone)]
pub struct TypedContext<I> {
    /// The already deserialized input
    pub input: I,
    /// The sender instance ID (guaranteed connected)
    pub sender_id: InstanceId,
    /// Client for sending additional messages if needed
    pub client: Arc<dyn ActiveMessageClient>,
}

/// Handler for active message tasks.
///
/// These handlers:
/// - Are always spawned (async execution)
/// - Get SenderAddress (may be Connected or Unconnected)
/// - Do not send automatic responses (return value is for internal error handling only)
/// - Can send 0 or N new active messages as they choose
/// - Have access to sender information
pub struct AmHandler<F, Fut> {
    handler_fn: F,
    name: String,
    _phantom: PhantomData<Fut>,
}

impl<F, Fut> AmHandler<F, Fut> {
    pub fn new(handler_fn: F, name: String) -> Self {
        Self {
            handler_fn,
            name,
            _phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<F, Fut> ActiveMessageHandler for AmHandler<F, Fut>
where
    F: Fn(AmContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), String>> + Send + Sync + 'static,
{
    fn dispatch_mode(&self) -> DispatchMode {
        DispatchMode::Spawn // Always spawned for basic handlers
    }

    async fn handle(
        &self,
        _message_id: uuid::Uuid,
        payload: Bytes,
        sender: SenderAddress,
        client: Arc<dyn ActiveMessageClient>,
    ) {
        // Create context for the active message handler
        let ctx = AmContext {
            sender,
            client,
            payload,
        };

        // Execute the AM handler (return value is for internal error handling only)
        if let Err(e) = (self.handler_fn)(ctx).await {
            error!("AM handler '{}' failed: {}", self.name, e);
        }
    }

    fn name(&self) -> &str {
        &self.name
    }
}

// ============================================================================
// Helper Functions - Clean API for New Handler Architecture
// ============================================================================

/// Create an active message handler with full context access
///
/// **Important**: The returned Result<(), String) is for internal error handling only
/// and is NOT sent back to the message sender. Active message handlers do not send responses.
///
/// Returns an Arc<dyn ActiveMessageDispatcher> ready for registration.
/// Creates its own TaskTracker for simplicity - use `am_handler_with_tracker` for production.
pub fn am_handler<F, Fut>(
    name: String,
    f: F,
) -> Arc<dyn crate::dispatcher::ActiveMessageDispatcher>
where
    F: Fn(AmContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), String>> + Send + Sync + 'static,
{
    let task_tracker = tokio_util::task::TaskTracker::new();
    am_handler_with_tracker(name, f, task_tracker)
}

/// Create an active message handler with a specific task tracker for graceful shutdown
///
/// **Important**: The returned Result<(), String> is for internal error handling only
/// and is NOT sent back to the message sender. Active message handlers do not send responses.
///
/// Use this version in production code where you need graceful shutdown tracking.
pub fn am_handler_with_tracker<F, Fut>(
    name: String,
    f: F,
    task_tracker: tokio_util::task::TaskTracker,
) -> Arc<dyn crate::dispatcher::ActiveMessageDispatcher>
where
    F: Fn(AmContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), String>> + Send + Sync + 'static,
{
    let handler = AmHandler::new(f, name);
    Arc::new(crate::dispatcher::SpawnedDispatcher::new(
        handler,
        task_tracker,
    ))
}

/// Create a unary handler from a function (guarantees response)
///
/// **Important**: The returned UnifiedResponse IS sent back to the message sender.
/// - Ok(None) = ACK (success, no payload)
/// - Ok(Some(bytes)) = Response (success with payload)
/// - Err(string) = Error/NACK sent to sender
///
/// Returns an Arc<dyn ActiveMessageDispatcher> ready for registration.
/// Creates its own TaskTracker for simplicity - use `unary_handler_with_tracker` for production.
pub fn unary_handler<F>(
    name: String,
    f: F,
) -> Arc<dyn crate::dispatcher::ActiveMessageDispatcher>
where
    F: Fn(UnaryContext) -> UnifiedResponse + Send + Sync + 'static,
{
    let task_tracker = tokio_util::task::TaskTracker::new();
    unary_handler_with_tracker(name, f, task_tracker)
}

/// Create a unary handler with a specific task tracker for graceful shutdown
///
/// **Important**: The returned UnifiedResponse IS sent back to the message sender.
/// - Ok(None) = ACK (success, no payload)
/// - Ok(Some(bytes)) = Response (success with payload)
/// - Err(string) = Error/NACK sent to sender
///
/// Use this version in production code where you need graceful shutdown tracking.
pub fn unary_handler_with_tracker<F>(
    name: String,
    f: F,
    task_tracker: tokio_util::task::TaskTracker,
) -> Arc<dyn crate::dispatcher::ActiveMessageDispatcher>
where
    F: Fn(UnaryContext) -> UnifiedResponse + Send + Sync + 'static,
{
    struct ClosureUnaryHandler<F> {
        f: F,
        name: String,
    }

    #[async_trait]
    impl<F> UnaryHandler for ClosureUnaryHandler<F>
    where
        F: Fn(UnaryContext) -> UnifiedResponse + Send + Sync,
    {
        async fn process(
            &self,
            payload: Bytes,
            sender_id: InstanceId,
            client: Arc<dyn ActiveMessageClient>,
        ) -> UnifiedResponse {
            let ctx = UnaryContext {
                payload,
                sender_id,
                client,
            };
            (self.f)(ctx)
        }

        fn name(&self) -> &str {
            &self.name
        }
    }

    let handler = ClosureUnaryHandler { f, name };
    let adapter = UnaryHandlerAdapter::new(handler, DispatchMode::Spawn);

    Arc::new(crate::dispatcher::SpawnedDispatcher::new(
        adapter,
        task_tracker,
    ))
}

/// Create a typed unary handler with automatic serialization (no wrapper needed!)
///
/// **Important**: The returned Result<O, String> IS sent back to the message sender.
/// - Ok(output) = Response with automatically serialized output
/// - Err(string) = Error/NACK sent to sender
///
/// Returns an Arc<dyn ActiveMessageDispatcher> ready for registration.
/// Creates its own TaskTracker for simplicity - use `typed_unary_handler_with_tracker` for production.
pub fn typed_unary_handler<I, O, F>(
    name: String,
    f: F,
) -> Arc<dyn crate::dispatcher::ActiveMessageDispatcher>
where
    I: serde::de::DeserializeOwned + Send + Sync + 'static,
    O: serde::Serialize + Send + Sync + 'static,
    F: Fn(TypedContext<I>) -> Result<O, String> + Send + Sync + 'static,
{
    let task_tracker = tokio_util::task::TaskTracker::new();
    typed_unary_handler_with_tracker(name, f, task_tracker)
}

/// Create a typed unary handler with a specific task tracker for graceful shutdown
///
/// **Important**: The returned Result<O, String> IS sent back to the message sender.
/// - Ok(output) = Response with automatically serialized output
/// - Err(string) = Error/NACK sent to sender
///
/// Use this version in production code where you need graceful shutdown tracking.
pub fn typed_unary_handler_with_tracker<I, O, F>(
    name: String,
    f: F,
    task_tracker: tokio_util::task::TaskTracker,
) -> Arc<dyn crate::dispatcher::ActiveMessageDispatcher>
where
    I: serde::de::DeserializeOwned + Send + Sync + 'static,
    O: serde::Serialize + Send + Sync + 'static,
    F: Fn(TypedContext<I>) -> Result<O, String> + Send + Sync + 'static,
{
    struct ClosureTypedUnaryHandler<I, O, F> {
        f: F,
        name: String,
        _phantom: PhantomData<(I, O)>,
    }

    #[async_trait]
    impl<I, O, F> TypedUnaryHandler<I, O> for ClosureTypedUnaryHandler<I, O, F>
    where
        I: serde::de::DeserializeOwned + Send + Sync,
        O: serde::Serialize + Send + Sync,
        F: Fn(TypedContext<I>) -> Result<O, String> + Send + Sync,
    {
        async fn process(
            &self,
            input: I,
            sender_id: InstanceId,
            client: Arc<dyn ActiveMessageClient>,
        ) -> Result<O, String> {
            let ctx = TypedContext {
                input,
                sender_id,
                client,
            };
            (self.f)(ctx)
        }

        fn name(&self) -> &str {
            &self.name
        }
    }

    let typed_handler = ClosureTypedUnaryHandler {
        f,
        name,
        _phantom: PhantomData,
    };
    let typed_adapter = TypedUnaryAdapter::new(typed_handler);
    let unary_adapter = UnaryHandlerAdapter::new(typed_adapter, DispatchMode::Spawn);

    Arc::new(crate::dispatcher::SpawnedDispatcher::new(
        unary_adapter,
        task_tracker,
    ))
}

/// Create a typed unary handler with async closures (no wrapper needed!)
///
/// **Important**: The returned Result<O, String> IS sent back to the message sender.
/// - Ok(output) = Response with automatically serialized output
/// - Err(string) = Error/NACK sent to sender
///
/// This variant accepts async closures, allowing you to use `.await` naturally:
/// ```ignore
/// typed_unary_handler_async("my_handler".to_string(), |ctx| async move {
///     let result = some_async_operation().await?;
///     Ok(MyResponse { value: result })
/// })
/// ```
///
/// Returns an Arc<dyn ActiveMessageDispatcher> ready for registration.
/// Creates its own TaskTracker for simplicity - use `typed_unary_handler_async_with_tracker` for production.
pub fn typed_unary_handler_async<I, O, F, Fut>(
    name: String,
    f: F,
) -> Arc<dyn crate::dispatcher::ActiveMessageDispatcher>
where
    I: serde::de::DeserializeOwned + Send + Sync + 'static,
    O: serde::Serialize + Send + Sync + 'static,
    F: Fn(TypedContext<I>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<O, String>> + Send + Sync + 'static,
{
    let task_tracker = tokio_util::task::TaskTracker::new();
    typed_unary_handler_async_with_tracker(name, f, task_tracker)
}

/// Create a typed unary handler with async closures and a specific task tracker
///
/// **Important**: The returned Result<O, String> IS sent back to the message sender.
/// - Ok(output) = Response with automatically serialized output
/// - Err(string) = Error/NACK sent to sender
///
/// Use this version in production code where you need graceful shutdown tracking.
pub fn typed_unary_handler_async_with_tracker<I, O, F, Fut>(
    name: String,
    f: F,
    task_tracker: tokio_util::task::TaskTracker,
) -> Arc<dyn crate::dispatcher::ActiveMessageDispatcher>
where
    I: serde::de::DeserializeOwned + Send + Sync + 'static,
    O: serde::Serialize + Send + Sync + 'static,
    F: Fn(TypedContext<I>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<O, String>> + Send + Sync + 'static,
{
    struct AsyncTypedUnaryHandler<I, O, F, Fut> {
        f: F,
        name: String,
        _phantom: PhantomData<(I, O, Fut)>,
    }

    #[async_trait]
    impl<I, O, F, Fut> TypedUnaryHandler<I, O> for AsyncTypedUnaryHandler<I, O, F, Fut>
    where
        I: serde::de::DeserializeOwned + Send + Sync,
        O: serde::Serialize + Send + Sync,
        F: Fn(TypedContext<I>) -> Fut + Send + Sync,
        Fut: Future<Output = Result<O, String>> + Send + Sync + 'static,
    {
        async fn process(
            &self,
            input: I,
            sender_id: InstanceId,
            client: Arc<dyn ActiveMessageClient>,
        ) -> Result<O, String> {
            let ctx = TypedContext {
                input,
                sender_id,
                client,
            };
            (self.f)(ctx).await
        }

        fn name(&self) -> &str {
            &self.name
        }
    }

    let typed_handler = AsyncTypedUnaryHandler {
        f,
        name,
        _phantom: PhantomData,
    };
    let typed_adapter = TypedUnaryAdapter::new(typed_handler);
    let unary_adapter = UnaryHandlerAdapter::new(typed_adapter, DispatchMode::Spawn);

    Arc::new(crate::dispatcher::SpawnedDispatcher::new(
        unary_adapter,
        task_tracker,
    ))
}

/// Create a bytes unary handler with typed input but raw bytes output
///
/// **Important**: The returned Result<Bytes, String> IS sent back to the message sender.
/// - Ok(bytes) = Response with raw bytes payload
/// - Err(string) = Error/NACK sent to sender
pub fn bytes_unary_handler<I, F>(
    name: String,
    f: F,
) -> UnaryHandlerAdapter<BytesUnaryAdapter<I, impl BytesUnaryHandler<I>>>
where
    I: serde::de::DeserializeOwned + Send + Sync + 'static,
    F: Fn(TypedContext<I>) -> Result<Bytes, String> + Send + Sync + 'static,
{
    struct ClosureBytesUnaryHandler<I, F> {
        f: F,
        name: String,
        _phantom: PhantomData<I>,
    }

    #[async_trait]
    impl<I, F> BytesUnaryHandler<I> for ClosureBytesUnaryHandler<I, F>
    where
        I: serde::de::DeserializeOwned + Send + Sync,
        F: Fn(TypedContext<I>) -> Result<Bytes, String> + Send + Sync,
    {
        async fn process(
            &self,
            input: I,
            sender_id: InstanceId,
            client: Arc<dyn ActiveMessageClient>,
        ) -> Result<Bytes, String> {
            let ctx = TypedContext {
                input,
                sender_id,
                client,
            };
            (self.f)(ctx)
        }

        fn name(&self) -> &str {
            &self.name
        }
    }

    let bytes_handler = ClosureBytesUnaryHandler {
        f,
        name,
        _phantom: PhantomData,
    };
    let bytes_adapter = BytesUnaryAdapter::new(bytes_handler);
    UnaryHandlerAdapter::new(bytes_adapter, DispatchMode::Spawn)
}

#[cfg(test)]
mod tests {
    use crate::dispatcher::DispatchContext;

    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use tokio::sync::Mutex;

    // ============================================================================
    // Example Handlers - Struct-based Implementations
    // ============================================================================

    /// Example struct-based inline handler for ultra-fast ping counting
    pub struct FastPingHandler {
        counter: Arc<AtomicU64>,
    }

    impl Default for FastPingHandler {
        fn default() -> Self {
            Self::new()
        }
    }

    impl FastPingHandler {
        pub fn new() -> Self {
            Self {
                counter: Arc::new(AtomicU64::new(0)),
            }
        }

        pub fn count(&self) -> u64 {
            self.counter.load(Ordering::Relaxed)
        }
    }

    #[async_trait]
    impl ActiveMessageHandler for FastPingHandler {
        fn dispatch_mode(&self) -> DispatchMode {
            DispatchMode::Inline // Execute inline for minimum latency
        }

        async fn handle(
            &self,
            _message_id: uuid::Uuid,
            _payload: Bytes,
            _sender: SenderAddress,
            _client: Arc<dyn ActiveMessageClient>,
        ) {
            // Not used when dispatch_mode is Inline
            unreachable!("FastPingHandler only supports inline execution");
        }

        fn handle_inline(&self, _ctx: DispatchContext) {
            // Ultra-fast inline execution - just increment counter
            self.counter.fetch_add(1, Ordering::Relaxed);
        }

        fn name(&self) -> &str {
            "fast_ping"
        }
    }

    /// Example struct-based response handler that handles echo requests
    pub struct EchoResponseHandler {
        prefix: String,
    }

    impl EchoResponseHandler {
        pub fn new(prefix: String) -> Self {
            Self { prefix }
        }
    }

    #[async_trait]
    impl ActiveMessageHandler for EchoResponseHandler {
        fn dispatch_mode(&self) -> DispatchMode {
            DispatchMode::Spawn
        }

        async fn handle(
            &self,
            _message_id: uuid::Uuid,
            payload: Bytes,
            sender: SenderAddress,
            _client: Arc<dyn ActiveMessageClient>,
        ) {
            let sender_id = match sender {
                SenderAddress::Connected(id) => id,
                SenderAddress::Unconnected(peer_info) => peer_info.instance_id,
            };

            // Echo the payload with prefix (active message)
            let input = String::from_utf8_lossy(&payload);
            let response = format!("{}: {}", self.prefix, input);

            // This is an active message handler - no automatic responses
            println!("Echo processed for {}: {}", sender_id, response);
        }

        fn name(&self) -> &str {
            "echo_handler"
        }
    }

    /// Types for calculator operations (used in typed_unary_handler tests)
    #[derive(Deserialize)]
    struct CalculatorRequest {
        operation: String,
        a: f64,
        b: f64,
    }

    #[derive(Serialize, Debug, Clone)]
    struct CalculatorResponse {
        result: f64,
    }

    /// Example struct-based active message handler for logging
    pub struct LoggerHandler {
        log_count: Arc<Mutex<u64>>,
    }

    impl LoggerHandler {
        pub fn new() -> Self {
            Self {
                log_count: Arc::new(Mutex::new(0)),
            }
        }

        pub async fn get_log_count(&self) -> u64 {
            *self.log_count.lock().await
        }
    }

    #[async_trait]
    impl ActiveMessageHandler for LoggerHandler {
        fn dispatch_mode(&self) -> DispatchMode {
            DispatchMode::Spawn
        }

        async fn handle(
            &self,
            _message_id: uuid::Uuid,
            payload: Bytes,
            sender: SenderAddress,
            _client: Arc<dyn ActiveMessageClient>,
        ) {
            let message = String::from_utf8_lossy(&payload);
            let sender_id = sender.instance_id();

            // Simulate logging
            println!("LOG [{}]: {}", sender_id, message);

            // Update counter
            let mut count = self.log_count.lock().await;
            *count += 1;
        }

        fn name(&self) -> &str {
            "logger"
        }
    }

    // ============================================================================
    // Test Data Structures
    // ============================================================================

    #[derive(Serialize, Deserialize)]
    struct TestRequest {
        value: i32,
        message: String,
    }

    #[derive(Serialize, Deserialize, Clone)]
    struct TestResponse {
        processed_value: i32,
        echo: String,
    }

    // ============================================================================
    // Tests - Showing Both Struct and Closure Approaches
    // ============================================================================

    #[test]
    fn test_fast_ping_handler() {
        let handler = FastPingHandler::new();
        assert_eq!(handler.count(), 0);
        // Would need a full DispatchContext to test inline execution
    }

    #[test]
    fn test_struct_handler_approaches() {
        // Struct approach - showing different dispatch modes
        let inline_handler = FastPingHandler::new();
        assert_eq!(inline_handler.name(), "fast_ping");
        assert_eq!(inline_handler.dispatch_mode(), DispatchMode::Inline);

        let echo_handler = EchoResponseHandler::new("test".to_string());
        assert_eq!(echo_handler.name(), "echo_handler");
        assert_eq!(echo_handler.dispatch_mode(), DispatchMode::Spawn);
    }

    #[test]
    fn test_unary_handler_approaches() {
        // =================================================================
        // New Clean API - Demonstrating all three response patterns
        // =================================================================

        // 1. ACK Pattern: Ok(None) - Success with no payload
        let increment_handler = unary_handler("increment".to_string(), |_ctx| {
            // Simulate incrementing a counter
            println!("Counter incremented");
            Ok(None) // ACK - success, no response payload needed
        });
        assert_eq!(increment_handler.name(), "increment");

        // 2. Response Pattern: Ok(Some(bytes)) - Success with payload
        let echo_handler = unary_handler("echo".to_string(), |ctx| {
            let input = String::from_utf8_lossy(&ctx.payload);
            let response = format!("Echo: {}", input);
            Ok(Some(Bytes::from(response.into_bytes()))) // Response with payload
        });
        assert_eq!(echo_handler.name(), "echo");

        // 3. Error Pattern: Err(string) - Failure/NACK
        let validator_handler = unary_handler("validate".to_string(), |ctx| {
            let input = String::from_utf8_lossy(&ctx.payload);
            if input.trim().is_empty() {
                Err("Input cannot be empty".to_string()) // NACK with error message
            } else if input.contains("forbidden") {
                Err("Forbidden content detected".to_string()) // NACK with specific error
            } else {
                let response = format!("Valid: {}", input);
                Ok(Some(Bytes::from(response.into_bytes()))) // Success with response
            }
        });
        assert_eq!(validator_handler.name(), "validate");
    }

    #[test]
    fn test_typed_unary_handler_approaches() {
        // =================================================================
        // New Clean API - No SerializableWrapper needed! 🎉
        // =================================================================

        // Simple typed handler that always succeeds - automatic serialization!
        let echo_handler = typed_unary_handler(
            "typed_echo".to_string(),
            |ctx: TypedContext<TestRequest>| -> Result<TestResponse, String> {
                Ok(TestResponse {
                    processed_value: ctx.input.value * 2,
                    echo: ctx.input.message,
                })
            },
        );
        assert_eq!(echo_handler.name(), "typed_echo");

        // Calculator handler that can fail (divide by zero) - clean API!
        let calc_handler = typed_unary_handler(
            "calculator".to_string(),
            |ctx: TypedContext<CalculatorRequest>| -> Result<CalculatorResponse, String> {
                let req = ctx.input;
                match req.operation.as_str() {
                    "divide" if req.b == 0.0 => {
                        Err("Division by zero not allowed".to_string()) // Error/NACK
                    }
                    "divide" => Ok(CalculatorResponse {
                        result: req.a / req.b,
                    }),
                    "add" => Ok(CalculatorResponse {
                        result: req.a + req.b,
                    }),
                    "subtract" => Ok(CalculatorResponse {
                        result: req.a - req.b,
                    }),
                    "multiply" => Ok(CalculatorResponse {
                        result: req.a * req.b,
                    }),
                    _ => Err(format!("Unknown operation: {}", req.operation)), // Error/NACK
                }
            },
        );
        assert_eq!(calc_handler.name(), "calculator");
    }

    #[test]
    fn test_bytes_unary_handler() {
        // Bytes handler with typed input but raw bytes output
        let image_processor = bytes_unary_handler(
            "process_image".to_string(),
            |ctx: TypedContext<TestRequest>| -> Result<Bytes, String> {
                if ctx.input.value < 0 {
                    Err("Invalid image size".to_string())
                } else {
                    // Simulate returning binary image data
                    let fake_image_data = vec![0xFF, 0xD8, 0xFF, 0xE0]; // JPEG header
                    Ok(Bytes::from(fake_image_data))
                }
            },
        );
        assert_eq!(image_processor.name(), "process_image");
    }

    #[tokio::test]
    async fn test_am_handler_approaches() {
        // Struct approach
        let struct_handler = LoggerHandler::new();
        assert_eq!(struct_handler.name(), "logger");
        assert_eq!(struct_handler.get_log_count().await, 0);

        // Closure approach
        let counter = Arc::new(Mutex::new(0u64));
        let counter_clone = counter.clone();

        let closure_handler = am_handler("async_counter".to_string(), move |ctx| {
            let counter = counter_clone.clone();
            async move {
                let message = String::from_utf8_lossy(&ctx.payload);
                println!("Async processing: {}", message);

                let mut count = counter.lock().await;
                *count += 1;

                Ok(())
            }
        });
        assert_eq!(closure_handler.name(), "async_counter");
    }

    #[test]
    fn test_dispatch_modes() {
        // Inline mode (fastest)
        let inline_handler = FastPingHandler::new();
        assert_eq!(inline_handler.dispatch_mode(), DispatchMode::Inline);

        // Spawn mode (normal async)
        let echo_handler = EchoResponseHandler::new("test".to_string());
        assert_eq!(echo_handler.dispatch_mode(), DispatchMode::Spawn);

        // AM handler dispatcher (no dispatch_mode method on dispatcher)
    }

    #[test]
    fn test_unified_response_patterns() {
        // ACK pattern (success, no payload)
        let ack_response: UnifiedResponse = Ok(None);
        assert!(ack_response.is_ok());
        assert!(matches!(ack_response, Ok(None)));

        // Response pattern (success with payload)
        let response_data = Bytes::from("response data");
        let response: UnifiedResponse = Ok(Some(response_data.clone()));
        assert!(response.is_ok());
        if let Ok(Some(data)) = response {
            assert_eq!(data, response_data);
        } else {
            panic!("Expected Ok(Some(...))");
        }

        // Error pattern (NACK)
        let error_response: UnifiedResponse = Err("Something went wrong".to_string());
        assert!(error_response.is_err());
        if let Err(msg) = error_response {
            assert_eq!(msg, "Something went wrong");
        } else {
            panic!("Expected Err(...)");
        }
    }

    #[test]
    fn test_sender_address_methods() {
        use crate::client::PeerInfo;

        let instance_id = InstanceId::from(uuid::Uuid::new_v4());

        // Connected variant
        let connected = SenderAddress::Connected(instance_id);
        assert_eq!(connected.instance_id(), instance_id);
        assert!(connected.is_connected());
        assert!(connected.peer_info().is_none());

        // Unconnected variant
        let peer_info = PeerInfo::new(instance_id, "tcp://localhost:5555");
        let unconnected = SenderAddress::Unconnected(peer_info.clone());
        assert_eq!(unconnected.instance_id(), instance_id);
        assert!(!unconnected.is_connected());
        assert!(unconnected.peer_info().is_some());
        assert_eq!(
            unconnected.peer_info().unwrap().endpoint,
            "tcp://localhost:5555"
        );
    }
}
