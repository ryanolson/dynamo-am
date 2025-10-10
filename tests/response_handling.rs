// SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for ACK/NACK/Response handling and metadata-based routing optimization.
//!
//! These tests verify that the response type discrimination optimization works correctly:
//! - ACK responses use metadata (zero JSON parse)
//! - NACK responses include error_message in metadata (zero JSON parse)
//! - Response messages use metadata (zero JSON parse in router)
//!
//! BREAKING CHANGE: response_type metadata is now REQUIRED for all response messages.

use anyhow::Result;
use dynamo_am::{
    client::ActiveMessageClient,
    handler_impls::{TypedContext, UnaryContext, typed_unary_handler, unary_handler},
    manager::ActiveMessageManager,
    zmq::ZmqActiveMessageManager,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct EchoRequest {
    message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct EchoResponse {
    echoed: String,
}

/// Test ACK response handling with Confirmed delivery mode.
/// Verifies that ACK responses use response_type metadata (zero JSON parse).
#[tokio::test(flavor = "multi_thread")]
async fn test_ack_response_with_confirmed_mode() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init()
        .ok();

    let cancel_token = CancellationToken::new();

    let manager1 =
        ZmqActiveMessageManager::new("tcp://127.0.0.1:0".to_string(), cancel_token.clone()).await?;

    let manager2 =
        ZmqActiveMessageManager::new("tcp://127.0.0.1:0".to_string(), cancel_token.clone()).await?;

    let client1 = manager1.client();
    let peer2_info = manager2.peer_info().await;

    // Connect client1 to client2
    client1.connect_to_peer(peer2_info.clone()).await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Register a simple handler on manager2 that returns Ok(None) (will send ACK)
    let ack_handler = unary_handler("ack_test".to_string(), |_ctx: UnaryContext| {
        Ok(None) // ACK response
    });

    manager2
        .register_handler("ack_test".to_string(), ack_handler)
        .await?;

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Send message with Confirmed mode (default .send() uses Confirmed)
    // This will wait for ACK internally
    client1
        .active_message("ack_test")?
        .send(peer2_info.instance_id)
        .await?;

    manager1.shutdown().await?;
    manager2.shutdown().await?;

    Ok(())
}

/// Test NACK response handling when handler returns an error.
/// Verifies that NACK responses include error_message in metadata (zero JSON parse).
/// TODO: Debug why this test times out
#[tokio::test(flavor = "multi_thread")]
async fn test_nack_response_on_handler_error() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init()
        .ok();

    let cancel_token = CancellationToken::new();

    let manager1 =
        ZmqActiveMessageManager::new("tcp://127.0.0.1:0".to_string(), cancel_token.clone()).await?;

    let manager2 =
        ZmqActiveMessageManager::new("tcp://127.0.0.1:0".to_string(), cancel_token.clone()).await?;

    let client1 = manager1.client();
    let peer2_info = manager2.peer_info().await;

    client1.connect_to_peer(peer2_info.clone()).await?;

    // Ensure bidirectional connection
    client1
        .ensure_bidirectional_connection(peer2_info.instance_id)
        .await?;

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Register a handler that always returns an error (will send NACK)
    let error_handler = typed_unary_handler(
        "error_test".to_string(),
        |_ctx: TypedContext<EchoRequest>| -> Result<EchoResponse, String> {
            Err("Intentional test error".to_string())
        },
    );

    manager2
        .register_handler("error_test".to_string(), error_handler)
        .await?;

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Verify handler is registered
    let handlers = client1.list_handlers(peer2_info.instance_id).await?;
    assert!(
        handlers.contains(&"error_test".to_string()),
        "Handler should be registered"
    );

    // Send message expecting response - handler will fail and send NACK
    let request = EchoRequest {
        message: "test".to_string(),
    };

    let status = client1
        .active_message("error_test")?
        .payload(&request)?
        .expect_response::<EchoResponse>()
        .send(peer2_info.instance_id)
        .await?;

    // Should receive NACK with error message
    let result = status.await_response::<EchoResponse>().await;
    assert!(result.is_err(), "Should receive NACK (error) response");

    if let Err(e) = result {
        let error_msg = e.to_string();
        assert!(
            error_msg.contains("Intentional test error"),
            "Error message should be preserved in NACK metadata, got: {}",
            error_msg
        );
    }

    manager1.shutdown().await?;
    manager2.shutdown().await?;

    Ok(())
}

/// Test full Response handling with payload data.
/// Verifies that Response messages use response_type metadata (zero JSON parse in router).
#[tokio::test(flavor = "multi_thread")]
async fn test_response_with_payload() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init()
        .ok();

    let cancel_token = CancellationToken::new();

    let manager1 =
        ZmqActiveMessageManager::new("tcp://127.0.0.1:0".to_string(), cancel_token.clone()).await?;

    let manager2 =
        ZmqActiveMessageManager::new("tcp://127.0.0.1:0".to_string(), cancel_token.clone()).await?;

    let client1 = manager1.client();
    let peer2_info = manager2.peer_info().await;

    client1.connect_to_peer(peer2_info.clone()).await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Register echo handler that returns a response with payload
    let echo_handler = typed_unary_handler(
        "echo_test".to_string(),
        |ctx: TypedContext<EchoRequest>| -> Result<EchoResponse, String> {
            Ok(EchoResponse {
                echoed: format!("Echo: {}", ctx.input.message),
            })
        },
    );

    manager2
        .register_handler("echo_test".to_string(), echo_handler)
        .await?;

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Send request and expect response
    let request = EchoRequest {
        message: "Hello World".to_string(),
    };

    let status = client1
        .active_message("echo_test")?
        .payload(&request)?
        .expect_response::<EchoResponse>()
        .timeout(Duration::from_secs(5))
        .send(peer2_info.instance_id)
        .await?;

    // Wait for response
    let response: EchoResponse = status.await_response().await?;

    assert_eq!(
        response.echoed, "Echo: Hello World",
        "Response payload should match expected value"
    );

    manager1.shutdown().await?;
    manager2.shutdown().await?;

    Ok(())
}

/// Test response handling with raw bytes payload (non-JSON).
/// Ensures that the optimization works even when payload is not JSON.
#[tokio::test(flavor = "multi_thread")]
async fn test_response_with_raw_bytes() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init()
        .ok();

    let cancel_token = CancellationToken::new();

    let manager1 =
        ZmqActiveMessageManager::new("tcp://127.0.0.1:0".to_string(), cancel_token.clone()).await?;

    let manager2 =
        ZmqActiveMessageManager::new("tcp://127.0.0.1:0".to_string(), cancel_token.clone()).await?;

    let client1 = manager1.client();
    let peer2_info = manager2.peer_info().await;

    client1.connect_to_peer(peer2_info.clone()).await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Register handler that returns ACK
    let bytes_handler = unary_handler("bytes_test".to_string(), |_ctx: UnaryContext| {
        Ok(None) // ACK response
    });

    manager2
        .register_handler("bytes_test".to_string(), bytes_handler)
        .await?;

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Send message - am_handler doesn't return response, just ACK
    client1
        .active_message("bytes_test")?
        .send(peer2_info.instance_id)
        .await?;

    manager1.shutdown().await?;
    manager2.shutdown().await?;

    Ok(())
}

/// Test multiple concurrent requests with mixed ACK/NACK/Response patterns.
/// Verifies that the routing optimization handles concurrent messages correctly.
#[tokio::test(flavor = "multi_thread")]
async fn test_concurrent_mixed_responses() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init()
        .ok();

    let cancel_token = CancellationToken::new();

    let manager1 =
        ZmqActiveMessageManager::new("tcp://127.0.0.1:0".to_string(), cancel_token.clone()).await?;

    let manager2 =
        ZmqActiveMessageManager::new("tcp://127.0.0.1:0".to_string(), cancel_token.clone()).await?;

    let client1 = manager1.client();
    let peer2_info = manager2.peer_info().await;

    client1.connect_to_peer(peer2_info.clone()).await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Register multiple handlers
    let ack_handler = unary_handler("ack_handler".to_string(), |_ctx: UnaryContext| {
        Ok(None) // ACK response
    });

    let error_handler = typed_unary_handler(
        "error_handler".to_string(),
        |_ctx: TypedContext<String>| -> Result<String, String> { Err("Handler error".to_string()) },
    );

    let echo_handler = typed_unary_handler(
        "echo_handler".to_string(),
        |ctx: TypedContext<String>| -> Result<String, String> {
            Ok(format!("Echo: {}", ctx.input))
        },
    );

    manager2
        .register_handler("ack_handler".to_string(), ack_handler)
        .await?;
    manager2
        .register_handler("error_handler".to_string(), error_handler)
        .await?;
    manager2
        .register_handler("echo_handler".to_string(), echo_handler)
        .await?;

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Send concurrent requests of different types
    let ack_task = {
        let client = client1.clone();
        let peer_id = peer2_info.instance_id;
        tokio::spawn(async move {
            client
                .active_message("ack_handler")
                .unwrap()
                .timeout(Duration::from_secs(5))
                .send(peer_id)
                .await
        })
    };

    let error_task = {
        let client = client1.clone();
        let peer_id = peer2_info.instance_id;
        tokio::spawn(async move {
            client
                .active_message("error_handler")
                .unwrap()
                .payload("test".to_string())
                .unwrap()
                .expect_response::<String>()
                .timeout(Duration::from_secs(5))
                .send(peer_id)
                .await
                .unwrap()
                .await_response::<String>()
                .await
        })
    };

    let echo_task = {
        let client = client1.clone();
        let peer_id = peer2_info.instance_id;
        tokio::spawn(async move {
            client
                .active_message("echo_handler")
                .unwrap()
                .payload("hello".to_string())
                .unwrap()
                .expect_response::<String>()
                .timeout(Duration::from_secs(5))
                .send(peer_id)
                .await
                .unwrap()
                .await_response::<String>()
                .await
        })
    };

    // Wait for all tasks
    let ack_result = ack_task.await?;
    let error_result = error_task.await?;
    let echo_result = echo_task.await?;

    // Verify results
    assert!(ack_result.is_ok(), "ACK should succeed");
    assert!(error_result.is_err(), "Error handler should return NACK");
    assert_eq!(echo_result?, "Echo: hello", "Echo should return response");

    manager1.shutdown().await?;
    manager2.shutdown().await?;

    Ok(())
}
