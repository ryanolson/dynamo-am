## Active Message API Quickstart (Rust)

The `dynamo-am` crate provides a transport-agnostic RPC framework for distributed control planes. Active messages support both fire-and-forget dispatch and request/response patterns over pluggable transports like ZMQ.

### Core workflow

1. Start an `ActiveMessageManager` (e.g., `ZmqActiveMessageManager`) to manage transports and dispatch
2. Register handlers:
   - `am_handler` for fire-and-forget messages
   - `typed_unary_handler` for request/response with automatic JSON serialization
3. Send messages via `ActiveMessageClient` using the `.active_message()` builder

### Example

This single-node example registers both handler types and demonstrates the builder API:

```rust
use dynamo_am::{
    api::{client::ActiveMessageClient, handler::InstanceId},
    runtime::handler_impls::{am_handler, typed_unary_handler},
    zmq::manager::ZmqActiveMessageManager,
};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

#[derive(Deserialize, Serialize)]
struct SumRequest {
    a: i32,
    b: i32,
}

#[derive(Deserialize, Serialize)]
struct SumResponse {
    total: i32,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let shutdown = CancellationToken::new();

    // Create manager bound to localhost (each instance gets a unique InstanceId)
    let manager = ZmqActiveMessageManager::new("tcp://127.0.0.1:5000".into(), shutdown.clone()).await?;
    let client = manager.client();

    // Fire-and-forget: no response expected
    manager
        .register_handler(
            "log_event".to_string(),
            am_handler("log_event".into(), |ctx| async move {
                println!("log entry: {}", String::from_utf8_lossy(&ctx.payload));
                Ok(())
            }),
        )
        .await?;

    // Request/response: automatic JSON (de)serialization
    manager
        .register_handler(
            "sum".to_string(),
            typed_unary_handler("sum".into(), |ctx| {
                let req = ctx.input;
                Ok(SumResponse { total: req.a + req.b })
            }),
        )
        .await?;

    // Send fire-and-forget message
    client
        .active_message("log_event")?
        .payload(serde_json::json!({ "message": "pipeline started" }))?
        .send(client.instance_id())
        .await?;

    // Send request and await response
    let status = client
        .active_message("sum")?
        .expect_response::<SumResponse>()
        .payload(SumRequest { a: 2, b: 2 })?
        .send(client.instance_id())
        .await?;

    let reply = status.await_response::<SumResponse>().await?;
    assert_eq!(reply.total, 4);

    manager.shutdown().await?;
    shutdown.cancel();
    Ok(())
}
```

### Key points

- **Fire-and-forget**: `.active_message("handler").payload(...).send(target)` returns immediately
- **Request/response**: Add `.expect_response::<T>()` before `.send()`, then call `.await_response::<T>()` on the returned status
- **Instance targeting**: Each manager instance has a unique `InstanceId`. Use `client.instance_id()` for local delivery or remote IDs for cross-node RPC
- **Error handling**: Typed handlers automatically serialize errors as negative acknowledgements

For distributed setups, point each manager to different bind addresses and use the remote instance's ID when calling `.send()`.

See `lib/am/src/runtime/handler_impls.rs` for additional examples.
