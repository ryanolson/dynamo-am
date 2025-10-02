# dynamo-am

`dynamo-am` is the Active Message (AM) subsystem that powers Dynamo's high-performance, leader-worker messaging. It delivers low-latency request/response and broadcast patterns on top of asynchronous transports while keeping the public API concrete and ergonomic.

## Highlights
- Concrete `NetworkClient` API: callers work with `Arc<NetworkClient>` instead of hand-rolled trait objects or generics
- Hot-path optimizations: caller-side serialization, per-peer channels, lock-free peer registry, and smart TCP/IPC endpoint selection
- Rich response management through the `MessageBuilder` typestate API for fire-and-forget, confirmed delivery, and typed responses
- Built-in service lifecycle: dynamic handler registration, discovery, cohort join/leave, health checks, and graceful shutdown coordination
- Transport abstraction via `ThinTransport`, with a production ZeroMQ backend and hooks for alternate transports
- Graceful shutdown: the manager owns a root `CancellationToken` (clone via `manager.cancel_token()`) and cancels it automatically during `shutdown()`

## Quickstart
Add the crate to your workspace. Inside this repository the dependency is available via a path reference:

```toml
[dependencies]
dynamo-am = { path = "lib/am" }
```

Spin up a manager, register a typed handler, and issue a request with the concrete `NetworkClient`:

```rust
use dynamo_am::{
    ActiveMessageManagerBuilder,
    handler_impls::{typed_unary_handler, TypedContext},
};
use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    // 1. Create the manager (binds TCP + IPC endpoints automatically)
    let manager = ActiveMessageManagerBuilder::new()
        .endpoint("tcp://0.0.0.0:5555".to_string())
        .build()
        .await?;

    // 2. Register a typed unary handler
    manager
        .register_handler(
            "echo".to_string(),
            typed_unary_handler("echo".to_string(), |ctx: TypedContext<String>| Ok(ctx.input)),
        )
        .await?;

    // 3. Use the concrete NetworkClient behind the Arc
    let client = manager.client();
    let peer = manager.peer_info().await;
    client.connect_to_peer(peer.clone()).await?; // connect to ourselves for the demo

    let status = client
        .active_message("echo")?
        .payload("hello world")?
        .target_instance(peer.instance_id)
        .expect_response::<String>()
        .execute()
        .await?;

    let echoed: String = status.await_response().await?;
    println!("{}", echoed);

    manager.shutdown().await?;
    Ok(())
}
```

Need to tie other tasks into the lifecycle? Clone the root cancellation token with `let cancel = manager.cancel_token();`. `shutdown()` cancels it for you.

> See `lib/am/docs/active_message.md` for an in-depth walkthrough of every API surface.

## Architecture at a Glance
- **Business layer** – `ActiveMessageManager`, `MessageDispatcher`, `MessageRouter`, and `NetworkClient` expose a transport-agnostic API for registering handlers and sending messages.
- **Transport layer** – The `ThinTransport` trait and `BoxedTransport` erase transport details while enabling optimizations like per-peer channel caching and IPC/TCP selection. The ZeroMQ backend (`lib/am/src/zmq`) is the reference implementation.
- **Response management** – `ResponseManager` correlates acceptances, ACK/NACKs, and payloads, backing both the `MessageBuilder` typestate API and cohort operations.
- **System services** – Built-in handlers (prefixed with `_`) cover service discovery, handler wait, cohort registration, health checks, and coordinated shutdown.

## Message Patterns
- **Active Message (fire-and-forget)**: `client.active_message("notify")?.payload(event)?.target_instance(peer).execute().await?;`
- **Confirmed delivery**: `.send(peer)` returns a `MessageStatus<SendAndConfirm>` that verifies handler acceptance with retries handled by the caller.
- **Unary response**: `.expect_response::<T>().execute().await?` yields a `MessageStatus<WithResponse>` whose `.await_response::<T>()` deserializes typed payloads.
- **Detached receipt handling**: `.detach_receipt()` creates a two-phase await allowing receipt validation before waiting on handler execution.
- **Cohort fan-out**: `LeaderWorkerCohort` wraps these primitives to broadcast, gather typed responses, or run rank-specific work in parallel.

All patterns share one optimized send path: serialization happens on the caller's thread, per-peer `mpsc::Sender`s preserve ordering, and DashMap keeps connection lookups lock-free.

## Cohorts & Leader/Worker Orchestration
`LeaderWorkerCohort` helps leaders manage ranked groups of workers:
- Register workers with automatic dual-endpoint addressing (TCP + IPC)
- Broadcast acknowledgements, gather typed responses, or send rank-specific payloads in parallel
- Choose failure semantics via `CohortFailurePolicy` and track membership changes through handler events

## Built-in System Handlers
The manager registers several internal handlers automatically:
- `_ack`, `_response`: acceptance and response tracking
- `_register_service`, `_discover`: service and endpoint discovery
- `_list_handlers`, `_wait_for_handler`: runtime introspection and readiness
- `_health_check`, `_request_shutdown`: basic liveness checks and graceful teardown

These endpoints power helper methods like `health_check`, `await_handler`, and cohort orchestration.

## Extending the Transport
Implement the `ThinTransport` trait to plug in alternate fabrics (HTTP, gRPC, custom IPC). Follow the ZeroMQ backend for guidance:
1. Define a wire format type and serialize `ActiveMessage` values into it.
2. Implement `connect`, returning per-peer `mpsc::Sender`s that workers read from.
3. Spawn lightweight workers that forward serialized messages to your transport.

Because the business layer operates entirely on `ThinTransport`, new transports automatically reuse the dispatcher, response manager, and cohort features.

## Examples & Docs
- **Docs**: `lib/am/docs/active_message.md` (architecture deep dive and design rationale)
- **Examples**: `lib/am/examples/src/bin/` (e.g., `ping_pong.rs`, `leader.rs`, `cohort_parallel.rs`)
- **Tests**: `lib/am/tests/` cover handler registration, cohort flows, and system handler behavior

Run the examples from the crate root:

```bash
cargo run --example ping_pong --manifest-path lib/am/examples/Cargo.toml
```

## License
Distributed under the Apache 2.0 license. Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES.
