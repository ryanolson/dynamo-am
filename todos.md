# Active Message Performance Optimization TODOs

## Phase 1: Quick Wins (No Breaking Changes)

### 1. Fix Response Type Discrimination
**Status:** TODO
**Branch:** `ryan/perf-response-type-discrimination`
**Priority:** P0 - Critical (fixes double JSON parsing)

**Problem:**
- Response messages embed type info in JSON payload (`status: "ok"/"error"`)
- Receiver must parse JSON to determine if it's ACK/NACK/Response
- This causes double parsing: once to check type, again to get data

**Current Flow:**
```rust
// Server sends (client.rs:388-450):
{"response_id": "...", "status": "ok"}  // ACK
{"response_id": "...", "status": "error", "message": "..."}  // NACK

// Client receives (message_router.rs:129):
if let Ok(json_value) = serde_json::from_slice(&message.payload) {
    if let Some(status) = json_value.get("status") { ... }  // Parse #1
}
// Handler parses again for actual data  // Parse #2
```

**Solution:**
Add `response_type` field to `ControlMetadata` to discriminate at metadata level, not payload level.

**Files to Change:**
- [ ] `lib/am/src/api/control.rs` - Add ResponseType enum to ControlMetadata
- [ ] `lib/am/src/api/client.rs` - Set response_type when sending ACK/NACK/Response
- [ ] `lib/am/src/runtime/message_router.rs` - Check metadata instead of parsing payload
- [ ] Tests to verify no regression

**Impact:** Both client and server (protocol change, but backwards compatible if optional)

---

### 2. Remove Metrics Serialization Overhead
**Status:** TODO
**Branch:** `ryan/perf-metrics-overhead`
**Priority:** P1 - High (wasteful CPU usage)

**Problem:**
```rust
// dispatcher.rs:490-492
let control_size = serde_json::to_vec(&message.control)
    .map(|b| b.len())
    .unwrap_or(0);
```
Serializes ControlMetadata just to measure byte size for metrics.

**Solution:**
- Option A: Estimate size from struct fields
- Option B: Cache serialized size on ControlMetadata
- Option C: Skip size tracking entirely (simplest)

**Files to Change:**
- [ ] `lib/am/src/runtime/dispatcher.rs` - Remove or optimize size calculation
- [ ] Consider adding size estimation method to ControlMetadata if needed

**Impact:** Server-side only, safe to change

---

### 3. Optimize Bytes Usage (Zero-Copy)
**Status:** TODO
**Branch:** `ryan/perf-zero-copy-bytes`
**Priority:** P2 - Medium (memory efficiency)

**Problem:**
Multiple places copy bytes instead of using reference-counted clones:
- `thin_transport.rs:169` - `Message::from(message.payload.as_ref())`
- `transport.rs:182` - `Bytes::from(multipart[1].to_vec())`

**Solution:**
Use `Bytes::clone()` which is cheap (just increments refcount) instead of copying data.

**Files to Change:**
- [ ] `lib/am/src/zmq/thin_transport.rs` - Use Bytes::clone()
- [ ] `lib/am/src/zmq/transport.rs` - Avoid vec copy where possible
- [ ] Audit other locations for unnecessary Bytes copies

**Impact:** Client and server (reduces memory allocations)

---

### 4. Optimize Auto-Registration Checks
**Status:** TODO
**Branch:** `ryan/perf-auto-registration`
**Priority:** P3 - Low (nice to have)

**Problem:**
```rust
// message_router.rs:214-217
if let Ok(peers) = self.client.list_peers().await {
    let is_known = peers.iter().any(|peer| peer.instance_id == message.sender_instance);
```
Every message checks full peer list to determine if sender is known.

**Solution:**
- Cache known peer IDs in a HashSet
- Update cache on connect/disconnect
- Avoid list_peers() call per message

**Files to Change:**
- [ ] `lib/am/src/runtime/message_router.rs` - Add peer cache
- [ ] `lib/am/src/runtime/network_client.rs` - Notify router on peer changes

**Impact:** Server-side only (reduces lock contention and iterations)

---

## Workflow

For each optimization:
1. Create feature branch from `ryan/active-message`
2. Implement changes
3. Run tests: `cargo test -p dynamo-am`
4. Run clippy: `cargo clippy -p dynamo-am -- -D warnings`
5. Local review and validation
6. Commit with signed-off-by
7. Push for review (WAIT for approval before merging)

## Current Status

- [ ] Phase 1.1: Response Type Discrimination
- [ ] Phase 1.2: Metrics Overhead
- [ ] Phase 1.3: Zero-Copy Bytes
- [ ] Phase 1.4: Auto-Registration Cache

**Next:** Start with Phase 1.1 (highest priority, biggest impact)
