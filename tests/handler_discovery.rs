// SPDX-FileCopyrightText: Copyright (c) 2024-2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use dynamo_am::{
    client::ActiveMessageClient,
    handler_impls::{AmContext, am_handler},
    manager::ActiveMessageManager,
    zmq::ZmqActiveMessageManager,
};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// Test that _list_handlers returns registered handlers
#[tokio::test]
async fn test_list_handlers_returns_registered_handlers() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init()
        .ok();

    let cancel_token = CancellationToken::new();

    // Create manager which registers built-in system handlers
    let manager =
        ZmqActiveMessageManager::new("tcp://127.0.0.1:0".to_string(), cancel_token.clone()).await?;

    // Register a custom handler
    let test_handler = am_handler(
        "custom_handler".to_string(),
        move |_ctx: AmContext| async move { Ok(()) },
    );

    manager
        .register_handler("custom_handler".to_string(), test_handler)
        .await?;

    // Give the handler time to register
    tokio::time::sleep(Duration::from_millis(100)).await;

    // List handlers using the manager API
    let handlers = manager.list_handlers().await;

    // Should include both system handlers and our custom handler
    assert!(
        handlers.contains(&"_list_handlers".to_string()),
        "Should have _list_handlers system handler"
    );
    assert!(
        handlers.contains(&"_wait_for_handler".to_string()),
        "Should have _wait_for_handler system handler"
    );
    assert!(
        handlers.contains(&"custom_handler".to_string()),
        "Should have custom_handler"
    );

    manager.shutdown().await?;
    cancel_token.cancel();

    Ok(())
}

/// Test that await_handler successfully waits for a handler to be registered
#[tokio::test(flavor = "multi_thread")]
async fn test_await_handler_succeeds_when_handler_exists() -> Result<()> {
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

    // Register a handler on manager2
    let test_handler = am_handler(
        "delayed_handler".to_string(),
        move |_ctx: AmContext| async move { Ok(()) },
    );

    manager2
        .register_handler("delayed_handler".to_string(), test_handler)
        .await?;

    // Give the handler time to register
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Wait for the handler from client1
    let result = client1
        .await_handler(
            peer2_info.instance_id,
            "delayed_handler",
            Some(Duration::from_secs(5)),
        )
        .await;

    assert!(
        result.is_ok(),
        "Should successfully wait for handler that exists"
    );

    manager1.shutdown().await?;
    manager2.shutdown().await?;
    cancel_token.cancel();

    Ok(())
}

/// Test that await_handler times out when handler doesn't exist
#[tokio::test(flavor = "multi_thread")]
async fn test_await_handler_times_out_when_handler_missing() -> Result<()> {
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

    // Wait for a handler that doesn't exist (short timeout)
    let result = client1
        .await_handler(
            peer2_info.instance_id,
            "nonexistent_handler",
            Some(Duration::from_millis(500)),
        )
        .await;

    match result {
        Ok(available) => {
            assert!(
                !available,
                "Handler should not be available after timeout"
            );
        }
        Err(e) => {
            // It's also acceptable if the call returns an error due to timeout
            eprintln!("await_handler returned error (acceptable): {}", e);
        }
    }

    // Shutdown in correct order: cancel first, then managers
    cancel_token.cancel();

    // Add small delay to allow cleanup
    tokio::time::sleep(Duration::from_millis(100)).await;

    manager1.shutdown().await?;
    manager2.shutdown().await?;

    Ok(())
}

/// Test that await_handler eventually succeeds when handler is registered later
#[tokio::test(flavor = "multi_thread")]
async fn test_await_handler_succeeds_after_registration() -> Result<()> {
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

    // Spawn a task that waits for the handler
    let wait_task = {
        let client1 = client1.clone();
        let instance_id = peer2_info.instance_id;
        tokio::spawn(async move {
            client1
                .await_handler(instance_id, "late_handler", Some(Duration::from_secs(10)))
                .await
        })
    };

    // Give the wait task time to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Register the handler after a delay
    tokio::time::sleep(Duration::from_millis(500)).await;

    let test_handler = am_handler(
        "late_handler".to_string(),
        move |_ctx: AmContext| async move { Ok(()) },
    );

    manager2
        .register_handler("late_handler".to_string(), test_handler)
        .await?;

    // The wait task should eventually succeed
    let result = wait_task.await??;

    assert!(
        result,
        "Should successfully wait for handler registered later"
    );

    manager1.shutdown().await?;
    manager2.shutdown().await?;
    cancel_token.cancel();

    Ok(())
}

/// Test that list_handlers can be called via active message
#[tokio::test(flavor = "multi_thread")]
async fn test_list_handlers_via_active_message() -> Result<()> {
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

    // Wait for connection to establish
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Register a custom handler on manager2
    let test_handler = am_handler(
        "test_handler".to_string(),
        move |_ctx: AmContext| async move { Ok(()) },
    );

    manager2
        .register_handler("test_handler".to_string(), test_handler)
        .await?;

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Call list_handlers via active message from client1
    let handlers = client1.list_handlers(peer2_info.instance_id).await?;

    assert!(
        handlers.contains(&"_list_handlers".to_string()),
        "Should have _list_handlers"
    );
    assert!(
        handlers.contains(&"test_handler".to_string()),
        "Should have test_handler"
    );

    manager1.shutdown().await?;
    manager2.shutdown().await?;
    cancel_token.cancel();

    Ok(())
}
