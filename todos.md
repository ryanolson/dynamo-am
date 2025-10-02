# Active Message Performance Optimization TODOs

## Phase 1: Quick Wins (No Breaking Changes)

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

- [x] Phase 1.1: Response Type Discrimination (COMPLETED - [PR #2](https://github.com/ryanolson/dynamo-am/pull/2))
- [x] Phase 1.2: Metrics Overhead (COMPLETED - branch `ryan/perf-metrics-overhead`)
- [x] Phase 1.3: Zero-Copy Bytes (SKIPPED - Not feasible due to ZMQ boundary constraints, see docs/perf_optimizations.md)
- [ ] Phase 1.4: Auto-Registration Cache

**Next:** Phase 1.4 - Optimize Auto-Registration Checks
