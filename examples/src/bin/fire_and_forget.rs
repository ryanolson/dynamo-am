// SPDX-FileCopyrightText: Copyright (c) 2024-2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Fire-and-forget active message performance example.
//!
//! This example demonstrates the fastest active message path using fire-and-forget
//! delivery with inline handler execution for maximum throughput and minimum latency.
//!
//! **Architecture:**
//! - Two managers in the same process (server and client)
//! - Server registers three handlers that share state:
//!   - `start`: Records start timestamp (inline dispatch)
//!   - `touch`: Increments atomic counter (inline dispatch)
//!   - `finish`: Records finish timestamp and prints results (spawned dispatch for I/O)
//! - Client sends 1002 messages via fire-and-forget (`.execute()`)
//!
//! **Demonstrates:**
//! - Fire-and-forget delivery mode (fastest path, zero response overhead)
//! - Inline handler execution for maximum performance
//! - Shared state between handlers using Arc and atomics
//! - High-throughput message patterns (1000+ messages)

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use dynamo_am::{
    client::ActiveMessageClient,
    dispatcher::{
        ActiveMessageDispatcher, ActiveMessageHandler, DispatchContext, DispatchMode,
        SenderAddress, SpawnedDispatcher,
    },
    manager::ActiveMessageManager,
    zmq::ZmqActiveMessageManager,
    MessageBuilder,
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::NamedTempFile;
use tokio_util::sync::CancellationToken;

/// Generate a unique IPC socket path for testing
fn unique_ipc_socket_path() -> Result<String> {
    let temp_file = NamedTempFile::new()?;
    let path = temp_file.path().to_string_lossy().to_string();
    // Close the file but keep the path - ZMQ will create the socket
    drop(temp_file);
    Ok(format!("ipc://{}", path))
}

/// Shared state for performance measurement across handlers
struct PerfState {
    /// Start timestamp in nanoseconds (0 = not set)
    start_time: AtomicU64,
    /// Number of touch messages received
    touch_count: AtomicU64,
    /// Finish timestamp in nanoseconds (0 = not set)
    finish_time: AtomicU64,
}

impl PerfState {
    fn new() -> Self {
        Self {
            start_time: AtomicU64::new(0),
            touch_count: AtomicU64::new(0),
            finish_time: AtomicU64::new(0),
        }
    }

    fn set_start_time(&self, time_nanos: u64) {
        self.start_time.store(time_nanos, Ordering::Release);
    }

    fn increment_touch(&self) {
        self.touch_count.fetch_add(1, Ordering::Relaxed);
    }

    fn set_finish_time(&self, time_nanos: u64) {
        self.finish_time.store(time_nanos, Ordering::Release);
    }

    fn get_start_time(&self) -> u64 {
        self.start_time.load(Ordering::Acquire)
    }

    fn get_touch_count(&self) -> u64 {
        self.touch_count.load(Ordering::Acquire)
    }

    fn get_finish_time(&self) -> u64 {
        self.finish_time.load(Ordering::Acquire)
    }
}

/// Inline dispatcher that wraps a handler for inline execution
struct InlineDispatcher<H: ActiveMessageHandler> {
    handler: Arc<H>,
}

impl<H: ActiveMessageHandler> InlineDispatcher<H> {
    fn new(handler: H) -> Self {
        Self {
            handler: Arc::new(handler),
        }
    }
}

#[async_trait]
impl<H: ActiveMessageHandler + 'static> ActiveMessageDispatcher for InlineDispatcher<H> {
    fn name(&self) -> &str {
        self.handler.name()
    }

    async fn dispatch(&self, ctx: DispatchContext) {
        // Execute inline immediately
        self.handler.handle_inline(ctx);
    }

    fn contract_info(&self) -> dynamo_am::protocols::receipt::ContractInfo {
        self.handler.contract_info()
    }
}

/// Handler for 'start' messages - records start timestamp
struct StartHandler {
    state: Arc<PerfState>,
}

impl StartHandler {
    fn new(state: Arc<PerfState>) -> Self {
        Self { state }
    }
}

#[async_trait]
impl ActiveMessageHandler for StartHandler {
    fn dispatch_mode(&self) -> DispatchMode {
        DispatchMode::Inline // Inline execution for minimum latency
    }

    async fn handle(
        &self,
        _message_id: uuid::Uuid,
        _payload: Bytes,
        _sender: SenderAddress,
        _client: Arc<dyn ActiveMessageClient>,
    ) {
        unreachable!("StartHandler only supports inline execution");
    }

    fn handle_inline(&self, _ctx: DispatchContext) {
        // Record start time with nanosecond precision
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        self.state.set_start_time(now);
    }

    fn name(&self) -> &str {
        "start"
    }
}

/// Handler for 'touch' messages - increments counter
struct TouchHandler {
    state: Arc<PerfState>,
}

impl TouchHandler {
    fn new(state: Arc<PerfState>) -> Self {
        Self { state }
    }
}

#[async_trait]
impl ActiveMessageHandler for TouchHandler {
    fn dispatch_mode(&self) -> DispatchMode {
        DispatchMode::Inline // Inline execution for maximum throughput
    }

    async fn handle(
        &self,
        _message_id: uuid::Uuid,
        _payload: Bytes,
        _sender: SenderAddress,
        _client: Arc<dyn ActiveMessageClient>,
    ) {
        unreachable!("TouchHandler only supports inline execution");
    }

    fn handle_inline(&self, _ctx: DispatchContext) {
        // Just increment the counter - ultra-fast
        self.state.increment_touch();
    }

    fn name(&self) -> &str {
        "touch"
    }
}

/// Handler for 'finish' messages - records finish time and prints results
struct FinishHandler {
    state: Arc<PerfState>,
}

impl FinishHandler {
    fn new(state: Arc<PerfState>) -> Self {
        Self { state }
    }
}

#[async_trait]
impl ActiveMessageHandler for FinishHandler {
    fn dispatch_mode(&self) -> DispatchMode {
        DispatchMode::Spawn // Spawned execution for I/O operations
    }

    async fn handle(
        &self,
        _message_id: uuid::Uuid,
        _payload: Bytes,
        _sender: SenderAddress,
        _client: Arc<dyn ActiveMessageClient>,
    ) {
        // Record finish time
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        self.state.set_finish_time(now);

        // Wait a brief moment to ensure all touch messages are processed
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Print results
        let start = self.state.get_start_time();
        let finish = self.state.get_finish_time();
        let touch_count = self.state.get_touch_count();

        if start > 0 && finish > 0 {
            let elapsed_nanos = finish - start;
            let elapsed_ms = elapsed_nanos as f64 / 1_000_000.0;
            let elapsed_secs = elapsed_nanos as f64 / 1_000_000_000.0;

            println!("=== Fire-and-Forget Performance Results ===");
            println!("Touch count: {}", touch_count);
            println!(
                "Elapsed time: {:.3} ms ({:.6} seconds)",
                elapsed_ms, elapsed_secs
            );

            if touch_count > 0 {
                let msg_per_sec = touch_count as f64 / elapsed_secs;
                let ns_per_msg = elapsed_nanos as f64 / touch_count as f64;
                println!("Throughput: {:.0} messages/second", msg_per_sec);
                println!("Latency: {:.0} ns/message", ns_per_msg);
            }
        } else {
            println!("Error: Start or finish time not recorded properly");
        }
    }

    fn name(&self) -> &str {
        "finish"
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().init();

    println!("Starting fire-and-forget performance example");

    let cancel_token = CancellationToken::new();

    // Create shared state
    let state = Arc::new(PerfState::new());

    // Create server manager
    let server_manager =
        ZmqActiveMessageManager::new(unique_ipc_socket_path()?, cancel_token.clone()).await?;

    // Create task tracker for handlers
    let task_tracker = tokio_util::task::TaskTracker::new();

    // Register handlers on server
    let start_handler = StartHandler::new(state.clone());
    let touch_handler = TouchHandler::new(state.clone());
    let finish_handler = FinishHandler::new(state.clone());

    // Wrap handlers in dispatchers
    // For inline handlers, wrap in InlineDispatcher
    server_manager
        .register_handler(
            "start".to_string(),
            Arc::new(InlineDispatcher::new(start_handler)),
        )
        .await?;

    server_manager
        .register_handler(
            "touch".to_string(),
            Arc::new(InlineDispatcher::new(touch_handler)),
        )
        .await?;

    // For spawned handler, wrap in SpawnedDispatcher
    server_manager
        .register_handler(
            "finish".to_string(),
            Arc::new(SpawnedDispatcher::new(finish_handler, task_tracker)),
        )
        .await?;

    // Create client manager
    let client_manager =
        ZmqActiveMessageManager::new(unique_ipc_socket_path()?, cancel_token.clone()).await?;

    let server_client = server_manager.client();
    let client_client = client_manager.client();

    println!("Server listening on: {}", server_client.endpoint());
    println!("Client endpoint: {}", client_client.endpoint());

    // Connect client to server
    let server_peer = server_client.peer_info();
    client_client.connect_to_peer(server_peer).await?;

    // Wait for connection to establish
    tokio::time::sleep(Duration::from_millis(100)).await;

    println!("Connection established, starting message burst...");

    // Start wall-clock timing
    let wall_start = Instant::now();

    // Send start message (fire-and-forget)
    MessageBuilder::new(client_client.as_ref(), "start")?
        .target_instance(server_client.instance_id())
        .execute()
        .await?;

    // Send 1000 touch messages (fire-and-forget)
    let touch_count = 1000;
    for _ in 0..touch_count {
        MessageBuilder::new(client_client.as_ref(), "touch")?
            .target_instance(server_client.instance_id())
            .execute()
            .await?;
    }

    // Send finish message (fire-and-forget)
    MessageBuilder::new(client_client.as_ref(), "finish")?
        .target_instance(server_client.instance_id())
        .execute()
        .await?;

    let wall_elapsed = wall_start.elapsed();
    println!("All messages sent in {:?}", wall_elapsed);

    // Wait for finish handler to print results
    tokio::time::sleep(Duration::from_millis(500)).await;

    println!("\nShutting down...");
    server_manager.shutdown().await?;
    client_manager.shutdown().await?;
    cancel_token.cancel();

    Ok(())
}
