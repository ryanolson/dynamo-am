# Active Message Performance Optimizations

This document tracks performance analysis and optimization opportunities for the active message system.

---

## Client Request Lifecycle

### Phase 1: Message Construction (Client Side)
**Location:** `lib/am/src/api/builder.rs` → `lib/am/src/api/client.rs`

**Serialization Point 1: Payload Serialization** 📝
- `builder.rs:158` - User payload → JSON via `serde_json::to_vec`
- `client.rs:37` - Generic `IntoPayload` trait serializes `&T` → JSON → Bytes

**🔴 Issue:** Possible double serialization if user passes pre-serialized data
- User serializes their data to JSON
- `IntoPayload` serializes it again
- **Impact:** CPU overhead, increased memory allocation

### Phase 2: Control Metadata Preparation
**Location:** `lib/am/src/api/control.rs`

**Metadata Construction** (in-memory, not serialized yet)
- `control.rs:269-275` - ControlMetadata → JSON Value via `to_value()`
- Creates delivery mode, response IDs, receipt metadata, transport hints
- **✅ Optimized:** Metadata stays in memory as struct until wire serialization

### Phase 3: Wire Serialization (Send Path)
**Location:** `lib/am/src/zmq/thin_transport.rs:154-170`

**Serialization Point 2: ZMQ Multipart Wire Format** 📝📝
- `thin_transport.rs:156-165` - ControlMetadata → JSON Value
- `thin_transport.rs:160` - Metadata struct with message_id, handler_name, sender, control
- `thin_transport.rs:168` - Metadata JSON → Vec<u8> via `serde_json::to_vec`
- `thin_transport.rs:169` - Payload bytes copied to ZMQ Message

**🔴 Critical Path Issue:**
- Serialization happens on **caller's thread** (synchronous blocking)
- While this keeps transport workers simple, it blocks the async task
- **Wire format:** `[JSON metadata bytes][payload bytes]`

**Task Spawn:**
- `thin_transport.rs:143` - Dedicated transport worker per endpoint
- Worker just writes pre-serialized bytes to ZMQ socket

### Phase 4: Network Transport
**Location:** `lib/am/src/zmq/thin_transport.rs:89-117`

**Async Send Flow:**
1. Pre-serialized Multipart pushed to mpsc channel (100 buffer)
2. Dedicated worker thread sends to ZMQ socket
3. **✅ Good:** Serialization off the hot send path for the worker

### Phase 5: Response Flow
**Location:** `lib/am/src/api/client.rs:386-436`

**Serialization Point 3: Response Packing** 📝📝
- Handler result → JSON via serde
- `client.rs:388-392` - ACK response: JSON object `{"response_id": "...", "status": "ok"}`
- `client.rs:446-450` - Error response: JSON object `{"response_id": "...", "status": "error", "message": "..."}`
- `client.rs:415-429` - Full response: Just payload bytes with metadata

**🔴 PROTOCOL ISSUE - Double Parsing Root Cause:**
The response messages are sent as generic "response_message" or "ack_response" handlers with status embedded in JSON payload. The receiver must parse JSON to determine message type.

**Alternative Design:**
- Use dedicated handler names (`_ack`, `_nack`, `_response`)
- Or use a discriminator byte before payload
- Or structured metadata field for message subtype

### Phase 6: Response Deserialization
**Location:** `lib/am/src/api/status.rs:91-103`

**Deserialization Point 4: Response Parsing** 📖📖
- `status.rs:91-95` - Try to deserialize as ResponseEnvelope (new format)
- `status.rs:103` - Fall back to raw deserialization
- **🔴 Issue:** Try/catch pattern parses potentially twice on failure

---

## Server Message Handling Lifecycle

### Architecture Overview
```
ZMQ Socket → Transport Reader → Merged Channel → Receive Loop → MessageRouter → Dispatcher → Handler
   (wire)         (task)         (unbounded)       (task)        (routing)     (spawn)    (execute)
```

### Phase 1: Wire Deserialization (Arrival)
**Location:** `lib/am/src/zmq/transport.rs:146-191`

**Deserialization Point 1: ZMQ Multipart → ActiveMessage** 📖📖📖
- `transport.rs:200-204` - `receive()` called by transport reader task
- `transport.rs:155` - Part 0: metadata bytes → JSON Value via `serde_json::from_slice`
- `transport.rs:158-173` - Extract and parse fields from JSON:
  - `message_id` string → UUID (parsing)
  - `handler_name` string extraction
  - `sender_instance` string → UUID (parsing)
- `transport.rs:176-179` - Control JSON Value → ControlMetadata::from_value() (full deserialization)
- `transport.rs:182` - Part 1: Copy payload bytes → Bytes::from()

**🔴 Performance Issues:**
1. Full JSON parse of metadata object
2. String → UUID parsing (2x per message)
3. Nested JSON deserialization for control metadata
4. Bytes copy instead of zero-copy

**Task Spawn:**
- `manager.rs:237` - TCP reader task spawned
- `manager.rs:244` - IPC reader task spawned
- Both call `transport.receive()` in loop

### Phase 2: Message Merging
**Location:** `lib/am/src/zmq/manager.rs:273-307`

**Channel Flow:**
- `manager.rs:171` - Unbounded mpsc channel for message merging
- `manager.rs:292` - Transport readers send ActiveMessage to channel
- **✅ Good:** Unbounded channel prevents backpressure from blocking transport
- **⚠️ Risk:** Memory growth if receiver can't keep up

### Phase 3: Main Receive Loop
**Location:** `lib/am/src/zmq/manager.rs:347-380`

**Task Spawn:**
- `manager.rs:252` - Main receive loop spawned
- Simple forwarding to MessageRouter
- **✅ Minimal:** No processing, just routing

### Phase 4: Message Routing & Response Correlation
**Location:** `lib/am/src/runtime/message_router.rs:68-246`

**Routing Logic:**
- `message_router.rs:72-74` - Check for `_accept` handler (internal)
- `message_router.rs:77-82` - Check for response context metadata

**🔴 CRITICAL: Double JSON Parsing for Responses** 📖📖
- `message_router.rs:129` - Parse payload as JSON to check for status field
  ```rust
  if let Ok(json_value) = serde_json::from_slice(&message.payload) {
      if let Some(status) = json_value.get("status").and_then(|s| s.as_str()) {
  ```
- Checks for `"status": "ok"` (ACK) or `"status": "error"` (NACK)
- This is the SECOND parse - first was in wire deserialization
- **Later:** Handler will parse AGAIN for actual response type

**Auto-Registration:**
- `message_router.rs:212-245` - Check if sender is known
- If unknown and has endpoint, auto-register peer
- **🟡 Overhead:** List peers, iterate to check, potentially connect

### Phase 5: Dispatch Preparation
**Location:** `lib/am/src/runtime/dispatcher.rs:481-620`

**Serialization Point 5: Metrics Overhead** 📝
- `dispatcher.rs:490-492` - Serialize ControlMetadata to measure size:
  ```rust
  let control_size = serde_json::to_vec(&message.control)
      .map(|b| b.len())
      .unwrap_or(0);
  ```
- **🔴 Wasteful:** Serializing only to get byte length
- Already have the struct, could estimate or cache size

**Receipt ACK Protocol:**
- `dispatcher.rs:536-598` - Contract validation and receipt ACK generation
- Creates ReceiptAck and sends back to sender
- **Serialization:** ReceiptAck → JSON → Bytes
- This is additional network round-trip before handler execution

### Phase 6: Handler Dispatch & Execution
**Location:** `lib/am/src/runtime/dispatcher.rs:282-348`

**Task Spawn:**
- `dispatcher.rs:292` - Handler spawned on TaskTracker
  ```rust
  self.task_tracker.spawn(async move {
  ```
- Each message handler runs in separate task
- **✅ Good:** Parallelism for blocking handlers
- **⚠️ Overhead:** Task spawn cost for simple handlers

**Acceptance Notification (Confirmed/WithResponse modes):**
- `dispatcher.rs:300-324` - Send acceptance message for WithResponse mode
- Creates ActiveMessage, serializes, sends back
- **Additional Network Round-Trip**

**Deserialization Point 6: Payload → Handler Input** 📖
- `handler_impls.rs:256` - Payload bytes → T via `serde_json::from_slice`
- TypedContext deserializes for typed handlers
- **✅ Necessary:** Final deserialization to application types

### Phase 7: Response Generation (If Applicable)
**Location:** `lib/am/src/api/client.rs:386-465`

**Serialization Point 7: Response Serialization** 📝📝
- Handler produces result
- `client.rs:388-392` - ACK: Serialize `{"response_id": "...", "status": "ok"}` to JSON
- `client.rs:446-450` - NACK: Serialize `{"response_id": "...", "status": "error", "message": "..."}` to JSON
- `client.rs:421-422` - Response: Set metadata, payload already bytes
- Goes through full send path (Phases 3-4)

**🔴 This creates the double-parse problem on the client receive side!**

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
- 📝 Control metadata → JSON (for metrics only!)
- **Total: 3 deserializations + 1 wasteful serialization**

### Per Request-Response Cycle (WithResponse)
**Client Send:**
- 3 serializations (as above)

**Server Receive + Process:**
- 3 deserializations + 1 wasteful serialization (as above)

**Server Response:**
- 📝 Response → JSON (if structured)
- 📝 Response wrapper → JSON wire format
- **Total: 2 more serializations**

**Client Receive:**
- 📖 Wire format → JSON
- 📖 JSON → check for status (double parse issue!)
- 📖 Payload → Response type
- **Total: 3 deserializations (one wasteful)**

**Grand Total: 8 serializations + 6 deserializations = 14 ser/de operations per request-response cycle**

---

## Critical Performance Issues

### 🔴 Priority 1: Protocol Design Issues (Affect Both Client & Server)

#### Issue 1.1: Response Message Type Discrimination
**Current:** Response type embedded in JSON payload
```rust
// Server sends:
{"response_id": "...", "status": "ok"}  // ACK
{"response_id": "...", "status": "error", "message": "..."}  // NACK
// OR raw response payload
```

**Problem:** Receiver must parse JSON to determine message type
- `message_router.rs:129` - Parse to check status
- Handler parses again for actual data
- **Cost:** 2x JSON parsing per response

**Solutions:**
1. **Handler-name discrimination:** Use dedicated handlers `_ack`, `_nack`, `_response`
2. **Metadata flag:** Add `response_type` to ControlMetadata
3. **Wire format prefix:** Single discriminator byte before payload

**Impact:** Both client and server need protocol changes

#### Issue 1.2: JSON Wire Format Overhead
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

### 🔴 Priority 2: Wasteful Operations

#### Issue 2.1: Metrics Serialization
**Location:** `dispatcher.rs:490-492`
```rust
let control_size = serde_json::to_vec(&message.control).unwrap_or(0);
```

**Solution:**
- Estimate from struct field sizes
- Cache serialized size on ControlMetadata
- Skip size tracking entirely

**Impact:** Server-side only, safe to change

#### Issue 2.2: Double Payload Serialization
**Location:** `builder.rs:158`, `client.rs:37`

**Solution:**
- Accept `Bytes` directly to skip double serialization
- Or check if `T` is already serialized

**Impact:** Client-side API change

### 🔴 Priority 3: Async/Await Inefficiencies

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

### 🟡 Priority 4: Memory Optimizations

#### Issue 4.1: Bytes Copying
**Locations:** Multiple

**Current:** `Bytes::from(vec)` copies data
- `thin_transport.rs:169`
- `transport.rs:182`

**Solution:** Use `Bytes::clone()` (reference counted)

**Impact:** Zero-copy optimization

#### Issue 4.2: Unbounded Channel Growth
**Location:** `manager.rs:171`

**Current:** Unbounded mpsc channel for message merging
- No backpressure
- Memory can grow unbounded

**Solution:** Bounded channel with sensible limit + monitoring

**Impact:** Server stability

---

## Optimization Roadmap

### Phase 1: Quick Wins (No Protocol Changes)
1. ✅ Remove metrics serialization overhead
2. ✅ Use Bytes::clone() instead of copies
3. ✅ Optimize auto-registration checks (cache peer list)
4. ✅ Inline handler execution for simple cases

**Estimated Impact:** 10-15% latency reduction

### Phase 2: Wire Format Evolution (Breaking Changes)
1. 🔄 Add response type discrimination to ControlMetadata
2. 🔄 Implement binary metadata format (bincode)
3. 🔄 Version negotiation for backwards compatibility

**Estimated Impact:** 30-40% latency reduction, 50% bandwidth reduction

### Phase 3: Parallelism Optimization
1. 🔄 Spawn blocking for serialization
2. 🔄 Worker pool for handler execution
3. 🔄 Batch message processing

**Estimated Impact:** 20-30% throughput increase

### Phase 4: Advanced (Requires Significant Refactoring)
1. 🔄 Zero-copy deserialization (Cap'n Proto / FlatBuffers)
2. 🔄 Kernel bypass networking (io_uring)
3. 🔄 NUMA-aware task scheduling

**Estimated Impact:** 50-100% performance improvement

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

## Next Steps

1. **Immediate:** Fix double JSON parsing in response routing
2. **Short-term:** Remove wasteful metrics serialization
3. **Medium-term:** Design wire format v2 with binary metadata
4. **Long-term:** Evaluate zero-copy serialization frameworks
