# Active Message Performance Optimizations

This document tracks performance analysis and optimization opportunities for the active message system.

---

## Client Request Lifecycle

### Phase 1: Message Construction (Client Side)
**Location:** `src/api/builder.rs` → `src/api/client.rs`

**Serialization Point 1: Payload Serialization** 📝
- `builder.rs:158` - User payload → JSON via `serde_json::to_vec`
- `client.rs:37` - Generic `IntoPayload` trait serializes `&T` → JSON → Bytes

**Issue:** Possible double serialization if user passes pre-serialized data
- User serializes their data to JSON
- `IntoPayload` serializes it again
- **Impact:** CPU overhead, increased memory allocation

### Phase 2: Control Metadata Preparation
**Location:** `src/api/control.rs`

**Metadata Construction** (in-memory, not serialized yet)
- `control.rs:269-275` - ControlMetadata → JSON Value via `to_value()`
- Creates delivery mode, response IDs, receipt metadata, transport hints
- Metadata stays in memory as a struct until wire serialization

### Phase 3: Wire Serialization (Send Path)
**Location:** `src/zmq/thin_transport.rs:154-170`

**Serialization Point 2: ZMQ Multipart Wire Format** 📝📝
- `thin_transport.rs:156-165` - ControlMetadata → JSON Value
- `thin_transport.rs:160` - Metadata struct with message_id, handler_name, sender, control
- `thin_transport.rs:168` - Metadata JSON → Vec<u8> via `serde_json::to_vec`
- `thin_transport.rs:169` - Payload bytes copied to ZMQ Message

**Key Issue:**
- Serialization happens on **caller's thread** (synchronous blocking)
- While this keeps transport workers simple, it blocks the async task
- **Wire format:** `[JSON metadata bytes][payload bytes]`

**Task Spawn:**
- `thin_transport.rs:143` - Dedicated transport worker per endpoint
- Worker just writes pre-serialized bytes to ZMQ socket

### Phase 4: Network Transport
**Location:** `src/zmq/thin_transport.rs:89-117`

**Async Send Flow:**
1. Pre-serialized Multipart pushed to mpsc channel (100 buffer)
2. Dedicated worker thread sends to ZMQ socket
3. Serialization stays off the hot send path for the worker thread

### Phase 5: Response Flow
**Location:** `src/api/client.rs:386-436`

**Serialization Point 3: Response Packing**
- Handler result → JSON via serde when returning structured data
- `client.rs:388-392` - ACK response: metadata marks `response_type = Ack`
- `client.rs:446-450` - Error response: metadata marks `response_type = Nack` and stores error text
- `client.rs:415-429` - Full response: payload bytes forwarded with metadata set to `response_type = Response`

**Current Handling:**
- `src/runtime/message_router.rs:136-210` consumes the metadata fast-path so ACK/NACK/Response routing avoids payload parsing.
- Legacy senders without `response_type` still trigger the JSON fallback; this remains a compatibility cost until wire versioning lands.

### Phase 6: Response Deserialization
**Location:** `src/api/status.rs:91-103`

**Deserialization Point 4: Response Parsing** 📖📖
- `status.rs:91-95` - Try to deserialize as ResponseEnvelope (new format)
- `status.rs:103` - Fall back to raw deserialization
- Issue: Try/catch pattern parses potentially twice on failure

---

## Server Message Handling Lifecycle

### Architecture Overview
```
ZMQ Socket → Transport Reader → Merged Channel → Receive Loop → MessageRouter → Dispatcher → Handler
   (wire)         (task)         (unbounded)       (task)        (routing)     (spawn)    (execute)
```

### Phase 1: Wire Deserialization (Arrival)
**Location:** `src/zmq/transport.rs:146-191`

**Deserialization Point 1: ZMQ Multipart → ActiveMessage** 📖📖📖
- `transport.rs:200-204` - `receive()` called by transport reader task
- `transport.rs:155` - Part 0: metadata bytes → JSON Value via `serde_json::from_slice`
- `transport.rs:158-173` - Extract and parse fields from JSON:
  - `message_id` string → UUID (parsing)
  - `handler_name` string extraction
  - `sender_instance` string → UUID (parsing)
- `transport.rs:176-179` - Control JSON Value → ControlMetadata::from_value() (full deserialization)
- `transport.rs:182` - Part 1: Copy payload bytes → Bytes::from()

**Performance Issues:**
1. Full JSON parse of metadata object
2. String → UUID parsing (2x per message)
3. Nested JSON deserialization for control metadata
4. Bytes copy instead of zero-copy

**Task Spawn:**
- `manager.rs:237` - TCP reader task spawned
- `manager.rs:244` - IPC reader task spawned
- Both call `transport.receive()` in loop

### Phase 2: Message Merging
**Location:** `src/zmq/manager.rs:273-307`

**Channel Flow:**
- `manager.rs:171` - Unbounded mpsc channel for message merging
- `manager.rs:292` - Transport readers send ActiveMessage to channel
- Unbounded buffering prevents backpressure from blocking transport but risks unchecked memory growth if the consumer stalls

### Phase 3: Main Receive Loop
**Location:** `src/zmq/manager.rs:347-380`

**Task Spawn:**
- `manager.rs:252` - Main receive loop spawned
- Loop simply forwards to `MessageRouter` without additional processing

### Phase 4: Message Routing & Response Correlation
**Location:** `src/runtime/message_router.rs:68-246`

**Routing Logic:**
- `message_router.rs:72-74` - Check for `_accept` handler (internal)
- `message_router.rs:77-82` - Check for response context metadata

**Response Routing:**
- Metadata fast-path (`message_router.rs:136-210`) uses `response_type` and avoids payload parsing for modern clients.
- JSON fallback remains for backward compatibility; consider version negotiation to retire it.

**Auto-Registration:**
- `message_router.rs:212-245` - Check if sender is known
- If unknown and has endpoint, auto-register peer
- Overhead: list peers, iterate to check, potentially establish connection

### Phase 5: Dispatch Preparation
**Location:** `src/runtime/dispatcher.rs:481-620`

**Dispatch Instrumentation:**
- `dispatcher.rs:485-491` - Optional sampling records payload size and timing without reserializing control metadata.

**Receipt ACK Protocol:**
- `dispatcher.rs:536-598` - Contract validation and receipt ACK generation
- Creates ReceiptAck and sends back to sender
- **Serialization:** ReceiptAck → JSON → Bytes
- This is additional network round-trip before handler execution

### Phase 6: Handler Dispatch & Execution
**Location:** `src/runtime/dispatcher.rs:282-348`

**Task Spawn:**
- `dispatcher.rs:292` - Handler spawned on TaskTracker
  ```rust
  self.task_tracker.spawn(async move {
  ```
- Each message handler runs in a separate task, which preserves parallelism for blocking handlers
- Overhead: task spawn cost for simple handlers

**Acceptance Notification (Confirmed/WithResponse modes):**
- `dispatcher.rs:300-324` - Send acceptance message for WithResponse mode
- Creates ActiveMessage, serializes, sends back
- **Additional Network Round-Trip**

**Deserialization Point 6: Payload → Handler Input** 📖
- `handler_impls.rs:256` - Payload bytes → T via `serde_json::from_slice`
- TypedContext deserializes for typed handlers; this is the final application-facing parse

### Phase 7: Response Generation (If Applicable)
**Location:** `src/api/client.rs:386-465`

**Serialization Point 7: Response Serialization** 📝📝
- Handler produces result
- `client.rs:388-392` - ACK messages carry minimal payload, metadata marks response type
- `client.rs:446-450` - NACK metadata includes the error message; payload remains optional
- `client.rs:421-422` - Response payload bytes reuse the original buffer when possible
- Responses go through the same send path (Phases 3-4)

---

## Task Spawn Summary

### Server-Side Tasks (Per Manager Instance)
1. **TCP Transport Reader** (`manager.rs:237`) - 1 task
2. **IPC Transport Reader** (`manager.rs:244`) - 1 task
3. **Main Receive Loop** (`manager.rs:252`) - 1 task
4. **ACK Cleanup Loop** (`manager.rs:258`) - 1 task every 5s
5. **Message Dispatcher** (`manager.rs:212`) - 1 task
6. **Per-Handler Execution** (`dispatcher.rs:292`) - N tasks (one per message)

**Total:** 5 persistent + N concurrent handler tasks

### Client-Side Tasks
1. **Per-Endpoint Transport Worker** (`thin_transport.rs:143`) - N tasks (one per connection)
2. **Response Timeout Monitoring** (`builder.rs:420`) - Optional per request

---

## Serialization/Deserialization Summary

### Per One-Way Message (Fire-and-Forget)
**Client Side:**
- 📝 Payload → JSON
- 📝 Control metadata → JSON
- 📝 Metadata + control → JSON wire format
- **Total: 3 serializations**

**Server Side:**
- 📖 Wire format → JSON metadata
- 📖 JSON → ControlMetadata struct
- 📖 Payload bytes → Handler input type
- **Total: 3 deserializations**

### Per Request-Response Cycle (WithResponse)
**Client Send:**
- 3 serializations (as above)

**Server Receive + Process:**
- 3 deserializations (as above)

**Server Response:**
- 📝 Response → JSON (if structured)
- 📝 Response wrapper → JSON wire format
- **Total: 2 more serializations**

**Client Receive:**
- 📖 Wire format → JSON
- 📖 Metadata fast-path resolves ACK/NACK without payload parse (legacy fallback parses JSON payload)
- 📖 Payload → Response type (for full responses)
- **Total: 2-3 deserializations depending on sender version**

**Grand Total (modern clients): ~7 serializations + 5 deserializations per request-response cycle**

---

## Critical Performance Issues

### Priority 1: Protocol Design Issues (Affect Both Client & Server)

#### Issue 1.1: JSON Wire Format Overhead
**Current:** Metadata serialized as JSON with string UUIDs
```json
{
  "message_id": "550e8400-e29b-41d4-a716-446655440000",
  "handler_name": "my_handler",
  "sender_instance": "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
  "control": { ... }
}
```

**Problems:**
- UUIDs as strings (36 bytes each vs 16 bytes binary)
- Full JSON parsing overhead
- Repeated string allocations

**Solutions:**
1. **Binary metadata format:** Use bincode/protobuf/cap'n proto
2. **Partial optimization:** Keep JSON but use binary UUIDs in custom serde

**Impact:** Breaks wire compatibility, needs versioning

### Priority 2: Wasteful Operations

#### Issue 2.1: Double Payload Serialization
**Location:** `builder.rs:158`, `client.rs:37`

**Solution:**
- Accept `Bytes` directly to skip double serialization
- Or check if `T` is already serialized

**Impact:** Client-side API change

### Priority 3: Async/Await Inefficiencies

#### Issue 3.1: Serialization on Caller Thread
**Location:** `thin_transport.rs:154-170`

**Current:** `serialize_to_wire()` blocks async task
- JSON serialization is CPU-intensive
- Blocks task scheduler

**Solutions:**
1. **Spawn blocking:** Move to `tokio::task::spawn_blocking`
2. **Pre-serialize pool:** Dedicated serialization thread pool
3. **Rayon parallel:** Use rayon for CPU-bound serialization

**Impact:** Client send path optimization

#### Issue 3.2: Handler Spawn Overhead
**Location:** `dispatcher.rs:292`

**Current:** Every message spawns a new task
- Task creation overhead
- Context switching

**Solutions:**
1. **Inline execution:** For simple handlers, execute inline
2. **Worker pool:** Pre-spawned worker tasks
3. **Batch processing:** Group messages before spawn

**Impact:** Server-side optimization

### Priority 4: Memory Optimizations

#### Issue 4.1: Bytes Copying
**Locations:** Multiple

**Current:** `Bytes::from(vec)` copies data
- `thin_transport.rs:169`
- `transport.rs:182`

**Current Constraint:** `tmq::Message::from` copies into ZMQ-managed storage, so `Bytes::clone()` on the caller side cannot avoid the copy without reworking the transport boundary.

**Impact:** Addressing this requires either a different transport API or custom allocators that can hand ZMQ its final buffer.

#### Issue 4.2: Unbounded Channel Growth
**Location:** `manager.rs:171`

**Current:** Unbounded mpsc channel for message merging
- No backpressure
- Memory can grow unbounded

**Solution:** Bounded channel with sensible limit + monitoring

**Impact:** Server stability

---

## Optimization Opportunities

### Wire Format Evolution
- Implement binary metadata (bincode/protobuf) to eliminate JSON parsing and shrink UUID representation.
- Negotiate protocol versions so upgraded nodes can prefer the binary path without breaking compatibility.
- Evaluate lightweight compression for large payloads once binary framing is available.

### Parallelism Optimization
- Offload serialization to blocking tasks or a dedicated pool so async execution is not stalled.
- Explore worker pools or batching to reduce per-message task spawn overhead on the dispatcher side.

### Advanced Research Tracks
- Revisit zero-copy deserialization (Cap'n Proto/FlatBuffers) if transport constraints change.
- Investigate kernel-bypass networking (io_uring, DPDK) for high-throughput deployments.
- Consider NUMA-aware scheduling strategies for multi-socket hosts.

---

## Benchmarking Plan

### Metrics to Track
1. **Latency:**
   - End-to-end message latency (p50, p99, p99.9)
   - Serialization time
   - Deserialization time
   - Network transit time

2. **Throughput:**
   - Messages per second
   - Bytes per second
   - Concurrent handlers

3. **Resource Usage:**
   - CPU utilization
   - Memory allocation rate
   - Task spawn rate

### Test Scenarios
1. Fire-and-forget (one-way)
2. Request-response (round-trip)
3. Burst traffic
4. Sustained load
5. Large payloads (1MB+)
6. Small payloads (<100 bytes)

---

## Appendix A: Completed Optimizations
- Metadata response-type discrimination removes the double JSON parse path for ACK/NACK routing.
- Control-metrics serialization was eliminated; dispatcher tracing now relies on sampling without re-encoding control metadata.
- Auto-registration now uses a cached peer set, avoiding repeated peer-list queries on the hot path.
- Zero-copy across the ZMQ boundary was investigated; buffer ownership requirements prevent adopting `Bytes::clone()` in that section without deeper transport changes.
