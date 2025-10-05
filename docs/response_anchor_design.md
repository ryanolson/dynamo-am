# Response Anchor Design Specification

## Overview

Response Anchors provide a streaming abstraction for distributed systems where:
- A service creates a **response anchor** that produces a stream of type `T`
- The anchor can be **attached to** from remote machines via active messages
- **Exclusive attachment** semantics ensure only one source sends data at a time
- **Ordered sentinel frames** guarantee all data is processed before state changes
- **Transport/protocol errors** terminate the stream gracefully

## Use Cases

1. **Streaming RPC Results**: Long-running computations that yield results incrementally
2. **Event Sourcing**: Remote services pushing events to a central collector
3. **Progress Reporting**: Workers reporting progress updates to a coordinator
4. **Data Pipelines**: Multi-stage processing where each stage streams to the next

## Architecture

### High-Level Flow

```
┌─────────────────────────────────────────────────────────────────┐
│ Machine A (Anchor Holder)                                       │
│                                                                  │
│  ┌──────────────┐  create_anchor()  ┌────────────────────────┐ │
│  │ User Code    │ ───────────────>  │ NetworkClient          │ │
│  │              │                    │ (has AnchorManager)    │ │
│  │              │ <─────────────────│                        │ │
│  │              │  (handle, stream)  │                        │ │
│  └──────────────┘                    └────────────────────────┘ │
│         │                                      │                 │
│         │ while let Some(item) = stream.next() │                │
│         │                                      │                 │
│         │                            ┌─────────▼──────────────┐ │
│         │                            │ AnchorManager          │ │
│         │                            │ - Tracks anchors       │ │
│         │                            │ - Routes data frames   │ │
│         │                            │ - Enforces exclusivity │ │
│         │                            └────────────────────────┘ │
│         │                                      ▲                 │
│         │                                      │ StreamFrame<T> │
└─────────┼──────────────────────────────────────┼─────────────────┘
          │                                      │
          │ send handle                          │ data frames
          │ via ActiveMessage                    │ via streaming transport
          │                                      │
┌─────────▼──────────────────────────────────────┼─────────────────┐
│ Machine B (Data Source)                        │                 │
│                                                 │                 │
│  ┌──────────────┐  attach(handle)  ┌───────────┴──────────────┐ │
│  │ User Code    │ ──────────────>  │ ResponseAnchorSource     │ │
│  │              │                   │                          │ │
│  │              │ <────────────────│                          │ │
│  │              │   ResponseSink    │                          │ │
│  └──────────────┘                   └──────────────────────────┘ │
│         │                                                        │
│         │ sink.send_ok(data)                                    │
│         │ sink.detach()                                         │
│         │ sink.finalize()                                       │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

### Communication Paths

1. **Control Path (ActiveMessage)**:
   - `_anchor_attach`: Request exclusive attachment
   - `_anchor_detach`: Release attachment (triggers Detached frame on data stream)
   - `_anchor_finalize`: Close stream (triggers Finalized frame on data stream)
   - `_anchor_cancel`: Creator removes anchor from registry

2. **Data Path (Streaming Transport - ZMQ Router-Router)**:
   ```
   Anchor DEALER <--inproc--> Local ROUTER <--inproc--> Remote ROUTER (TCP) <--tcp--> Source DEALER
   ```

## Core Types

### Response Anchor Handles

Handles are expressed as two cooperating types that encode their lifecycle:

```rust
/// Raw, cloneable payload used for serialization/transport
pub struct SerializedAnchorHandle<T> { /* anchor_id, instance_id, worker_address */ }

/// Armed handle that owns attachment rights (non-cloneable)
pub struct ArmedAnchorHandle<T> { /* payload + lifecycle ref */ }
```

- `SerializedAnchorHandle<T>` is inert data. Dropping it has no effect. Use this when
  passing anchors over the wire or storing them.
- `ArmedAnchorHandle<T>` owns exclusive rights to attach. Dropping an armed handle
  sends a `StreamFrame::Dropped` sentinel and finalizes the anchor so resources are
  reclaimed even if no attachment ever happens.
- `SerializedAnchorHandle::arm(client)` produces an `ArmedAnchorHandle<T>` by
  binding it to a local `NetworkClient`. Conversely, `ArmedAnchorHandle::disarm()`
  consumes the armed handle and returns a `SerializedAnchorHandle<T>` for transport.
- `ArmedAnchorHandle::attach(client)` consumes the armed handle and returns a
  `ResponseSink<T>`; `ResponseSink::detach()` yields a fresh `ArmedAnchorHandle<T>`
  while `ResponseSink::finalize()` consumes the lifecycle entirely.

### StreamFrame<T>

Wire protocol for data streaming:

```rust
pub enum StreamFrame<T> {
    /// Data item or application-level error
    Item(Result<T, String>),

    /// Sentinel: Armed handle was dropped without explicit finalize
    Dropped,

    /// Sentinel: Source detached, anchor can accept new attachment
    /// Only sent after all Item frames from this session
    Detached,blluufincgnuruugrftf
    
    /// Sentinel: Stream finalized, no more attachments allowed
    /// Anchor will close the stream after processing this
    Finalized,

    /// Fatal error (transport/serialization), stream terminates
    /// Sent by either side when unrecoverable error occurs
    TransportError(String),
}
```

**Key Design Points:**
- `Item` carries either data or application errors (part of the stream)
- `Detached` and `Finalized` are **sentinel frames** that guarantee ordering
- User stream never sees control frames, only `Result<T, E>` items
- Stream returns `None` after `Finalized` or `TransportError`

### Control Messages

```rust
pub struct AnchorAttachRequest {
    pub anchor_id: Uuid,
    pub session_id: Uuid,
    pub source_endpoint: String,
}

pub enum AnchorAttachResponse {
    Ok { stream_endpoint: String },
    Err { reason: AttachError },
}

pub enum AttachError {
    AlreadyAttached,
    AnchorNotFound,
    AnchorCancelled,
}

pub struct AnchorDetachRequest {
    pub anchor_id: Uuid,
    pub session_id: Uuid,
}

pub struct AnchorFinalizeRequest {
    pub anchor_id: Uuid,
    pub session_id: Uuid,
}

pub struct AnchorCancelRequest {
    pub anchor_id: Uuid,
}
```

## Component Design

### AnchorManager

Internal component (never exposed in public API):

```rust
pub struct AnchorManager {
    /// Registry of active anchors
    anchors: DashMap<Uuid, AnchorEntry>,

    /// Streaming transport for data frames
    streaming_transport: Arc<dyn StreamingTransport>,

    /// Instance ID of this manager
    instance_id: InstanceId,
}

struct AnchorEntry {
    /// Channel to send frames to user's stream
    stream_sender: mpsc::Sender<AnchorStreamEvent>,

    /// Current attachment session (if any)
    attachment: Option<AttachmentSession>,

    /// Metadata
    created_at: Instant,
    type_name: &'static str,
}

struct AttachmentSession {
    session_id: Uuid,
    source_instance: InstanceId,
    attached_at: Instant,
}
```

**Responsibilities:**
1. Create anchors and return (handle, stream) pairs
2. Register system handlers for control messages
3. Route incoming `StreamFrame<T>` to correct anchor
4. Enforce exclusive attachment (one source at a time)
5. Process sentinel frames for state transitions
6. Cleanup on cancel/finalize

**Key Methods:**

```rust
impl AnchorManager {
    /// Create a new response anchor
    pub async fn create_anchor<T>(&self, instance_id: InstanceId, worker_address: WorkerAddress)
        -> Result<(ResponseAnchorHandle, impl Stream<Item = Result<T, anyhow::Error>>)>
    where
        T: DeserializeOwned + Send + 'static;

    /// Handle attachment request (from _anchor_attach handler)
    pub async fn handle_attach(&self, request: AnchorAttachRequest)
        -> Result<AnchorAttachResponse>;

    /// Handle detach request (from _anchor_detach handler)
    pub async fn handle_detach(&self, request: AnchorDetachRequest)
        -> Result<()>;

    /// Handle finalize request (from _anchor_finalize handler)
    pub async fn handle_finalize(&self, request: AnchorFinalizeRequest)
        -> Result<()>;

    /// Handle cancel request (from _anchor_cancel handler)
    pub async fn handle_cancel(&self, request: AnchorCancelRequest)
        -> Result<()>;

    /// Route incoming stream frame to appropriate anchor
    async fn route_stream_frame<T>(&self, anchor_id: Uuid, frame: StreamFrame<T>)
        -> Result<()>;
}
```

### StreamingTransport Trait

Transport abstraction for streaming data frames:

```rust
pub trait StreamingTransport: Send + Sync + Debug {
    /// Bind a streaming endpoint and return its address
    async fn bind_stream_endpoint(&self) -> Result<String>;

    /// Create a sink for sending frames to a remote anchor
    async fn create_stream_source<T>(
        &self,
        endpoint: &str,
        anchor_id: Uuid,
        session_id: Uuid,
    ) -> Result<Box<dyn StreamSink<T>>>;

    /// Create a receiver for frames addressed to a specific anchor
    async fn create_stream_receiver<T>(
        &self,
        anchor_id: Uuid,
    ) -> Result<Box<dyn StreamReceiver<T>>>;
}

pub trait StreamSink<T>: Send {
    async fn send(&mut self, frame: StreamFrame<T>) -> Result<()>;
    async fn close(self) -> Result<()>;
}

pub trait StreamReceiver<T>: Send {
    async fn recv(&mut self) -> Option<StreamFrame<T>>;
}
```

### Public API

User-facing types in `src/api/response_anchor.rs`:

```rust
/// Handle to a response anchor (serializable)
pub struct ResponseAnchorHandle {
    pub anchor_id: Uuid,
    pub instance_id: InstanceId,
    pub worker_address: WorkerAddress,
}

/// Stream type returned to anchor creator
pub type ResponseAnchorStream<T> = impl Stream<Item = Result<T, anyhow::Error>>;

/// Entry point for attaching to a remote anchor
pub struct ResponseAnchorSource<T> {
    // Internal fields hidden
}

impl<T: Serialize + Send + 'static> ResponseAnchorSource<T> {
    /// Attach to a remote anchor
    pub async fn attach(
        handle: ResponseAnchorHandle,
        client: Arc<dyn ActiveMessageClient>,
    ) -> Result<ResponseSink<T>>;
}

/// Active streaming connection
pub struct ResponseSink<T> {
    // Internal fields hidden
}

impl<T: Serialize> ResponseSink<T> {
    /// Send a successful data item
    pub async fn send_ok(&mut self, item: T) -> Result<()>;

    /// Send an application-level error message
    pub async fn send_err(&mut self, error: impl Into<String>) -> Result<()>;

    /// Detach (release exclusive lock, keep stream open)
    pub async fn detach(self) -> Result<()>;

    /// Finalize (close stream permanently)
    pub async fn finalize(self) -> Result<()>;
}
```

**NetworkClient Extension:**

```rust
impl NetworkClient {
    /// Create a new response anchor
    pub async fn create_response_anchor<T>(&self)
        -> Result<(ResponseAnchorHandle, ResponseAnchorStream<T>)>
    where
        T: DeserializeOwned + Send + 'static
    {
        self.anchor_manager.create_anchor::<T>(
            self.instance_id,
            self.worker_address.clone(),
        ).await
    }
}
```

## State Machine

### Anchor Lifecycle

```
┌─────────────┐
│   Created   │ (anchor exists, no attachment)
└──────┬──────┘
       │
       │ attach request
       ▼
┌─────────────┐
│  Attached   │ (exclusive lock held by source)
└──────┬──────┘
       │
       ├─ Detached frame received ──> back to Created (can reattach)
       │
       ├─ Finalized frame received ──> Closed (removed from registry)
       │
       └─ Cancel request ──> Cancelled (removed from registry)
```

### Attachment Flow

```
Source (Machine B)                          Anchor (Machine A)
      │                                            │
      │ 1. send(_anchor_attach)                   │
      ├──────────────────────────────────────────>│
      │                                            │ check: not attached?
      │                                            │ yes: create session
      │                                            │
      │ 2. AnchorAttachResponse::Ok(endpoint)     │
      │<──────────────────────────────────────────┤
      │                                            │
      │ 3. connect to stream endpoint             │
      │ 4. send StreamFrame::Item(data)           │
      ├──────────────────────────────────────────>│
      │                                            │ route to user stream
      │                                            │
      │ 5. sink.detach() or sink.finalize()       │
      │ 6. send StreamFrame::Detached/Finalized   │
      ├──────────────────────────────────────────>│
      │                                            │ process sentinel
      │                                            │ update state
```

## ZMQ Implementation

### Router-Router Proxy Pattern

**Anchor Holder Setup:**

```
┌──────────────────────────────────────────────────────────┐
│ Anchor DEALER Socket                                     │
│ - Identity: anchor_id                                    │
│ - Connect: inproc://anchors-local                        │
└────────────────┬─────────────────────────────────────────┘
                 │
                 ▼
┌──────────────────────────────────────────────────────────┐
│ Local ROUTER Socket                                      │
│ - Bind: inproc://anchors-local                           │
│ - Routes to DEALERs by anchor_id                         │
└────────────────┬─────────────────────────────────────────┘
                 │
                 │ proxy or manual forwarding
                 ▼
┌──────────────────────────────────────────────────────────┐
│ Remote ROUTER Socket                                     │
│ - Bind: tcp://*:PORT                                     │
│ - Routes to local ROUTER via inproc                      │
└────────────────┬─────────────────────────────────────────┘
                 │ TCP
```

**Source Setup:**

```
┌──────────────────────────────────────────────────────────┐
│ Source DEALER Socket                                     │
│ - Connect: tcp://anchor-host:PORT                        │
│ - Send: [anchor_id][session_id][frame]                   │
└──────────────────────────────────────────────────────────┘
```

**Frame Format:**

```
[Frame 0: anchor_id (16 bytes)]
[Frame 1: session_id (16 bytes)]
[Frame 2: serialized StreamFrame<T>]
```

## Error Handling

### Error Categories

1. **Application Errors**: Part of the data stream
   - Sent as `StreamFrame::Item(Err(app_error))`
   - User receives as `Result::Err` from stream
   - Stream continues

2. **Transport Errors**: Terminate the stream
   - Connection failures, serialization errors
   - Sent as `StreamFrame::TransportError(msg)`
   - User receives error, then `None` (stream closed)
   - Anchor removed from registry

3. **Control Errors**: Returned from control messages
   - Attach already attached: `AnchorAttachResponse::Err(AlreadyAttached)`
   - Anchor not found: Return error from control handler
   - Session mismatch: Reject detach/finalize

### Error Examples

```rust
// Application error (part of stream)
sink.send_err("computation failed").await?;
// → User sees: Some(Err("computation failed"))
// → Stream continues

// Transport error (terminates stream)
// Happens automatically on serialization failure
// → User sees: Some(Err("serialization failed"))
//            : None (stream closed)

// Control error (attach fails)
let sink = ResponseAnchorSource::attach(handle, client).await;
// → Returns Err("anchor already attached")
```

## Concurrency & Thread Safety

- `AnchorManager` uses `DashMap` for lock-free concurrent access
- `StreamFrame` routing is async and non-blocking
- User streams use bounded `mpsc::channel` to enforce backpressure
- Exclusive attachment enforced by atomic compare-and-swap on attachment field
- Armed handle and sink lifecycles use RAII: dropping an armed handle without
  attaching emits `StreamFrame::Dropped`, ensuring anchors do not leak.

## Timeouts & Cancellation

- `ResponseAnchorStream::set_timeout(Duration)` registers an inactivity timeout.
  If no attachment occurs before the deadline the `AnchorManager` finalizes the
  anchor with a `Dropped` sentinel.
- Successful attachments or calls to `ResponseSink::detach()` reset the timeout.
- Attempting to arm or attach a handle after the timeout fires yields an
  `AttachError::AnchorNotFound`, allowing callers to fail fast and abort their work.

## Performance Considerations

1. **Separate Control and Data Paths**: Control messages don't block data streaming
2. **Direct Streaming**: Data frames bypass ActiveMessage overhead
3. **Ordered Processing**: Sentinel frames ensure state consistency without locks
4. **Bounded Channels**: Backpressure via bounded mpsc channels
5. **ZMQ Flow Control**: TCP flow control handles backpressure at transport layer

## Testing Strategy

1. **Unit Tests**:
   - `AnchorManager` attachment logic
   - Sentinel frame processing
   - Error propagation

2. **Integration Tests**:
   - Local anchor creation and streaming
   - Remote attachment and data flow
   - Exclusive attachment enforcement
   - Detach/reattach scenarios
   - Finalize closes stream
   - Cancel prevents attachments
   - Error handling paths

3. **Examples**:
   - Basic local demo
   - Remote two-machine demo
   - Error handling demo

## Open Questions / Future Work

1. **Timeout Handling**: Should attachments have timeouts?
2. **Metrics**: Track anchor count, attachment duration, data throughput?
3. **Backpressure Signals**: Should we expose backpressure to user?
4. **Multiple Consumers**: Future extension to allow fan-out?
5. **Persistence**: Should anchors survive instance restarts?

## Summary

Response Anchors provide a clean abstraction for distributed streaming with:
- ✅ Exclusive attachment semantics
- ✅ Ordered sentinel frames for state transitions
- ✅ Separate control and data paths for performance
- ✅ Graceful error handling and cleanup
- ✅ Type-safe streaming with `Result<T, E>`
- ✅ Integration with existing ActiveMessage infrastructure
