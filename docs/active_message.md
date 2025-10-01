# ActiveMessage System

The ActiveMessage system provides a high-performance distributed messaging framework designed for leader-worker architectures with dynamic handler registration, response tracking, and service discovery.

## Architecture Overview

The system is built on clean architectural layers with clear separation of concerns:

### Transport Layer (Internal)
- **ThinTransport trait**: Pure transport abstraction that only handles raw bytes
- **BoxedTransport**: Type-erased wrapper for ThinTransport (hides generic parameters)
- **BoxedConnectionHandle**: Type-erased per-peer mpsc::Sender channels
- **Smart endpoint selection**: Automatic IPC (same-host) vs TCP (cross-host) optimization

### Business Logic Layer (Public API)
- **NetworkClient**: **Concrete type** (no generics!) for managing connections and sending messages
- **MessageRouter**: Routes incoming messages to responses or handlers
- **MessageDispatcher**: Manages handler registry and dispatches to user handlers
- **ResponseManager**: Tracks pending responses with timeout and cleanup

### Design Principles
1. **Concrete types at API boundary**: Users get `Arc<NetworkClient>`, not trait objects
2. **Type erasure at transport layer**: `BoxedTransport` hides generic parameters internally
3. **Serialization on caller's thread**: CPU work happens before sending to transport
4. **Dumb transport workers**: Just write pre-serialized bytes, no business logic
5. **Lock-free hot paths**: DashMap for concurrent peer registry access
6. **Connection establishment returns channels**: Clean separation between connection lifecycle and message flow

## Core Concepts

### Message Patterns

#### Active Message (Fire-and-Forget)
Messages sent without expecting a response. Ideal for notifications, broadcasts, and one-way updates.

```rust
client.send_am("notification", payload).await?;
```

#### Unary Transaction (Request-Response)
Two-phase await pattern for request-response:
1. First await: Confirms message delivery and receipt acknowledgment
2. Second await: Waits for the handler's actual response

```rust
let status = client.send_unary("compute", payload).await?;
let response = status.wait_response(timeout).await?;
```

### Transport Abstraction

The `ThinTransport` trait provides a minimal interface for transport implementations:

```rust
pub trait ThinTransport: Send + Sync + std::fmt::Debug {
    type WireFormat: Send + 'static;

    async fn connect(&self, endpoint: &str) -> Result<mpsc::Sender<Self::WireFormat>>;
    fn serialize_to_wire(&self, message: &ActiveMessage) -> Result<Self::WireFormat>;
    fn serialize_and_send(&self, message: &ActiveMessage, sender: &mpsc::Sender<Self::WireFormat>) -> Result<()>;
    async fn disconnect(&self, endpoint: &str) -> Result<()>;
}
```

**Key Design Elements:**
- `connect()` returns an `mpsc::Sender` channel, not a connection object
- `serialize_to_wire()` happens on the caller's thread (not in transport worker)
- `serialize_and_send()` is the hot path: serialize + try_send
- Transport workers just read from channel and write bytes

### Connection Management

ConnectionHandle provides per-peer communication:

```rust
pub struct ConnectionHandle<WireFormat> {
    pub instance_id: InstanceId,
    pub primary_sender: mpsc::Sender<WireFormat>,
    pub alt_senders: HashMap<u8, mpsc::Sender<WireFormat>>,
    pub active_endpoint: String,
}
```

**Benefits:**
- Primary sender for fast-path (e.g., IPC for same-host)
- Alternative senders for failover (e.g., TCP fallback)
- Per-peer channel maintains message ordering
- Lock-free sending via mpsc channels

## Core Components

### ActiveMessage

The message structure that flows through the system:

```rust
pub struct ActiveMessage {
    pub message_id: Uuid,
    pub handler_name: String,
    pub sender_instance: InstanceId,
    pub payload: Bytes,
    pub metadata: HashMap<String, String>,
}
```

Use `MessageBuilder` to construct messages with optional features like response tracking.

### ActiveMessageClient

The primary interface for sending messages:

```rust
pub trait ActiveMessageClient: Send + Sync {
    fn endpoint(&self) -> &str;
    fn instance_id(&self) -> InstanceId;

    async fn connect_to_address(&self, address: &WorkerAddress) -> Result<PeerInfo>;
    async fn send_am(&self, handler_name: &str, payload: Bytes) -> Result<()>;
    async fn send_unary(&self, handler_name: &str, payload: Bytes) -> Result<MessageStatus>;
    async fn broadcast(&self, handler_name: &str, payload: Bytes) -> Result<()>;
}
```

**Implementation: NetworkClient (Concrete Type)**
- Returns concrete `NetworkClient` struct, not a trait object
- Uses type-erased `BoxedTransport` internally for flexibility
- Manages peer registry using DashMap for lock-free concurrent access
- Handles smart endpoint selection (IPC vs TCP)
- Integrates with ResponseManager for tracking pending responses
- All methods work directly without `Self: Sized` limitations

### Message Handlers

Create handlers using the v2 API with typed closures:

```rust
// Unary handler (returns responses)
let handler = typed_unary_handler_with_tracker(
    "compute".to_string(),
    |ctx: TypedContext<ComputeRequest>| {
        let result = process_request(ctx.input);
        Ok(ComputeResponse { result })
    },
    task_tracker,
);

// Active message handler (no response)
let handler = am_handler_with_tracker(
    "notify".to_string(),
    |ctx: AmContext| async move {
        let msg: Notification = serde_json::from_slice(&ctx.payload)?;
        process_notification(msg).await;
        Ok(())
    },
    task_tracker,
);
```

**Key Features:**
- Automatic serialization/deserialization
- TaskTracker integration for graceful shutdown
- Error handling with proper Result types
- Type-safe closure-based API

### ActiveMessageManager

Manages service lifecycle and handler registration:

```rust
pub trait ActiveMessageManager: Send + Sync {
    fn client(&self) -> Arc<dyn ActiveMessageClient>;
    async fn deregister_handler(&self, name: &str) -> Result<()>;
    fn handler_events(&self) -> broadcast::Receiver<HandlerEvent>;
    async fn shutdown(&self) -> Result<()>;
}
```

**Implementation: ZmqActiveMessageManager**
- Dual transport binding (TCP + IPC)
- MessageDispatcher for handler management
- Automatic system handler registration
- Background tasks for message receiving and cleanup

## ZMQ Implementation

### Transport Architecture

The ZMQ implementation demonstrates the transport abstraction:

1. **ZmqThinTransport**: Implements ThinTransport trait
   - Creates per-endpoint transport workers
   - Serializes ActiveMessage to ZMQ multipart format
   - Workers just write multipart messages to ZMQ sockets

2. **ZmqTransport**: Low-level ZMQ socket wrapper
   - Handles socket creation and binding
   - Provides async receive() interface
   - Used by manager for receiving incoming messages

3. **ZmqWireFormat**: Type alias for ZMQ multipart messages
   ```rust
   pub type ZmqWireFormat = tmq::Multipart;
   ```

### Manager Setup

The ZmqActiveMessageManager binds to both TCP and IPC endpoints:

```rust
let manager = ZmqActiveMessageManager::new(
    "tcp://0.0.0.0:5555".to_string(),
    cancel_token,
).await?;

// Automatically creates:
// - TCP endpoint: tcp://0.0.0.0:5555
// - IPC endpoint: ipc:///tmp/dynamo-am-{instance-id}.ipc
```

**Internal Architecture:**
1. TCP and IPC transports read from ZMQ sockets
2. Messages merged into single mpsc::UnboundedChannel
3. Main receive loop routes messages via MessageRouter
4. MessageRouter sends to ResponseManager or MessageDispatcher
5. MessageDispatcher executes user handlers in TaskTracker

### LeaderWorkerCohort

Helper for managing groups of workers in leader-worker patterns:

```rust
let cohort = LeaderWorkerCohort::new(
    leader_client,
    CohortType::FixedSize(3),
);

// Add workers with ranks
cohort.add_worker(worker_id, Some(rank)).await?;

// Parallel operations with rank-ordered results
let results: Vec<Response> = cohort.par_map(
    "work",
    |rank, worker_id| async move {
        Ok(create_work_for_rank(rank))
    },
    timeout,
).await?;
```

**Cohort Operations:**
- `par_broadcast_acks()`: Send to all workers, collect ACKs
- `par_broadcast_responses()`: Send same message, collect typed responses
- `par_map()`: Send different messages per rank, collect responses
- `par_map_indexed()`: Like par_map but returns (rank, response) pairs

## Usage Patterns

### Basic Service Setup

```rust
use dynamo_runtime::active_message::{
    manager::ActiveMessageManager,
    zmq::ZmqActiveMessageManager,
    handler_impls::typed_unary_handler_with_tracker,
};

#[tokio::main]
async fn main() -> Result<()> {
    let cancel_token = CancellationToken::new();

    // 1. Create manager
    let manager = ZmqActiveMessageManager::new(
        "tcp://0.0.0.0:5555".to_string(),
        cancel_token.clone(),
    ).await?;

    // 2. Register handlers
    let task_tracker = TaskTracker::new();
    let handler = typed_unary_handler_with_tracker(
        "echo".to_string(),
        |ctx: TypedContext<String>| Ok(ctx.input),
        task_tracker,
    );
    manager.register_handler("echo".to_string(), handler).await?;

    // 3. Get client (concrete NetworkClient type)
    let client = manager.client();  // Returns Arc<NetworkClient>
    let peer = client.connect_to_address(&worker_address).await?;

    // 4. Send messages using the ActiveMessageClient trait methods
    let response = client.send_unary("echo", payload).await?
        .wait_response(Duration::from_secs(5)).await?;

    // All methods work directly on the concrete client type
    client.join_cohort(leader_id, rank).await?;

    // 5. Shutdown
    manager.shutdown().await?;
    cancel_token.cancel();

    Ok(())
}
```

### Leader-Worker Pattern

**Leader:**
```rust
let manager = ZmqActiveMessageManager::new(
    "tcp://0.0.0.0:5555".to_string(),
    cancel_token,
).await?;

let client = manager.client();
let cohort = Arc::new(LeaderWorkerCohort::new(
    client.clone(),
    CohortType::FixedSize(3),
));

// Workers connect and leader auto-registers them
// Wait for cohort to be full
while !cohort.is_full().await {
    tokio::time::sleep(Duration::from_millis(100)).await;
}

// Distribute work
let results = cohort.par_map("compute", |rank, _| {
    Ok(WorkRequest { rank, data: vec![rank; 100] })
}, Duration::from_secs(10)).await?;
```

**Worker:**
```rust
let manager = ZmqActiveMessageManager::new(
    "tcp://0.0.0.0:0".to_string(),  // Random port
    cancel_token,
).await?;

// Register compute handler
let handler = typed_unary_handler_with_tracker(
    "compute".to_string(),
    |ctx: TypedContext<WorkRequest>| {
        let result = process_work(ctx.input);
        Ok(result)
    },
    task_tracker,
);
manager.register_handler("compute".to_string(), handler).await?;

// Connect to leader
let client = manager.client();
let leader_addr = WorkerAddress::tcp("tcp://leader:5555".to_string());
client.connect_to_address(&leader_addr).await?;

// Worker now receives and processes work
```

### Response Tracking

The system provides three patterns for tracking responses:

**1. Fire-and-Forget (Active Message):**
```rust
client.send_am("notification", payload).await?;
```

**2. Unary Transaction (Request-Response):**
```rust
let status = client.send_unary("compute", payload).await?;
let response = status.wait_response(Duration::from_secs(5)).await?;
```

**3. Cohort Parallel Operations:**
```rust
// Collect ACKs from all workers
let ack_results = cohort.par_broadcast_acks(
    "ping",
    "Hello!",
    Duration::from_secs(5),
).await?;

// Collect typed responses from all workers
let responses: Vec<ComputeResult> = cohort.par_broadcast_responses(
    "compute",
    request,
    timeout,
).await?;
```

## Built-in System Handlers

The system provides several built-in handlers (all prefixed with `_`):

- **`_ack`**: Handles acknowledgment responses for tracked messages
- **`_register_service`**: Service registration for leader-worker patterns
- **`_list_handlers`**: Handler discovery for debugging
- **`_wait_for_handler`**: Blocks until handler becomes available
- **`_health_check`**: Simple health check endpoint
- **`_discover`**: Endpoint and capability discovery
- **`_request_shutdown`**: Graceful shutdown coordination

System handlers are automatically registered by `ZmqActiveMessageManager`.

## Error Handling

The system uses comprehensive error handling:

- All operations return `anyhow::Result<T>`
- Transport errors are propagated with context
- Handler panics are caught by TaskTracker
- Response timeouts are handled gracefully
- Deserialization errors send NACK responses with error details

## Performance Characteristics

**Hot Path Optimizations:**
1. Serialization happens on caller's thread (not transport worker)
2. Lock-free peer registry using DashMap
3. Per-peer mpsc channels for ordered, concurrent sends
4. Smart endpoint selection (IPC faster than TCP for same-host)
5. Connection handle caching eliminates repeated lookups

**Concurrency:**
- Multiple concurrent sends to different peers (lock-free via DashMap)
- Multiple concurrent sends to same peer (ordered via mpsc channel)
- Handlers execute concurrently via TaskTracker
- Response tracking is thread-safe with minimal contention

## Thread Safety

All components are designed for concurrent access:
- `ThinTransport`: Send + Sync trait
- `NetworkClient`: Uses Arc for shared ownership, DashMap for concurrent access
- `ResponseManager`: Thread-safe response tracking
- `MessageDispatcher`: Async handler execution with TaskTracker
- Handler registration: Async with proper synchronization

## Lifecycle Management

Proper shutdown sequence:

```rust
// 1. Shutdown manager (joins all handlers)
manager.shutdown().await?;

// 2. Cancel background tasks
cancel_token.cancel();

// 3. Wait for task tracker (if external handlers)
task_tracker.close();
task_tracker.wait().await;
```

The manager's `shutdown()` method:
1. Sends shutdown signal to MessageDispatcher
2. Joins receiver task
3. Joins ACK cleanup task
4. Joins dispatcher task
5. Waits for all handler tasks to complete

## Transport Implementation Guide

To implement a new transport (e.g., HTTP, gRPC):

1. **Define your WireFormat type:**
   ```rust
   pub struct HttpWireFormat {
       body: Vec<u8>,
       headers: HashMap<String, String>,
   }
   ```

2. **Implement ThinTransport trait:**
   ```rust
   impl ThinTransport for HttpTransport {
       type WireFormat = HttpWireFormat;

       async fn connect(&self, endpoint: &str) -> Result<mpsc::Sender<Self::WireFormat>> {
           // Create HTTP client for endpoint
           // Spawn worker that reads from channel and POSTs
           // Return sender channel
       }

       fn serialize_to_wire(&self, message: &ActiveMessage) -> Result<Self::WireFormat> {
           // Serialize ActiveMessage to HTTP format
       }
   }
   ```

3. **Create NetworkClient with your transport:**
   ```rust
   let transport = Arc::new(HttpTransport::new());
   let client = NetworkClient::<HttpWireFormat>::new(
       instance_id,
       endpoint,
       transport,
       response_manager,
   );
   ```

This architecture allows the same business logic (NetworkClient, MessageRouter, MessageDispatcher) to work with any transport implementation.