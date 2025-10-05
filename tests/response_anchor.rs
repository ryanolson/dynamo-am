// SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for Response Anchors
//!
//! These tests verify the full response anchor lifecycle:
//! - Creating anchors and obtaining handles
//! - Passing handles via active messages
//! - Attaching from remote workers
//! - Sending data through the anchor
//! - Detaching and reattaching from different workers
//! - Finalizing anchors

use anyhow::Result;
use dynamo_am::{
    ResponseAnchorHandle, ResponseAnchorSource,
    client::ActiveMessageClient,
    handler_impls::{TypedContext, typed_unary_handler_with_tracker},
    manager::ActiveMessageManager,
    zmq::ZmqActiveMessageManager,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio_util::{sync::CancellationToken, task::TaskTracker};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct TestData {
    value: i32,
    message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AnchorHandleMessage {
    handle: ResponseAnchorHandle,
}

/// Test basic anchor creation and local streaming
#[tokio::test(flavor = "multi_thread")]
async fn test_anchor_creation_and_local_stream() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init()
        .ok();

    let cancel_token = CancellationToken::new();

    let manager =
        ZmqActiveMessageManager::new("tcp://127.0.0.1:0".to_string(), cancel_token.clone()).await?;

    let worker_address = manager.peer_info().await.address;

    // Create a response anchor
    let (handle, _stream) = manager
        .create_response_anchor::<TestData>(worker_address)
        .await?;

    tracing::info!("Created anchor with ID: {}", handle.anchor_id);

    // Verify handle has correct fields
    assert_eq!(handle.instance_id, manager.client().instance_id());

    manager.shutdown().await?;

    Ok(())
}

/// Test attach, send data, and finalize flow
#[tokio::test(flavor = "multi_thread")]
#[ignore] // Temporarily disabled
async fn test_attach_send_finalize_old() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init()
        .ok();

    let cancel_token = CancellationToken::new();

    // Create two workers
    let manager_a =
        ZmqActiveMessageManager::new("tcp://127.0.0.1:0".to_string(), cancel_token.clone()).await?;
    let manager_b =
        ZmqActiveMessageManager::new("tcp://127.0.0.1:0".to_string(), cancel_token.clone()).await?;

    let client_a = manager_a.client();
    let client_b = manager_b.client();
    let peer_a_info = manager_a.peer_info().await;
    let peer_b_info = manager_b.peer_info().await;

    // Connect workers
    client_a.connect_to_peer(peer_b_info.clone()).await?;
    client_b.connect_to_peer(peer_a_info.clone()).await?;
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Create anchor on A
    let (handle, mut stream) = manager_a
        .create_response_anchor::<TestData>(peer_a_info.address.clone())
        .await?;

    tracing::info!("Worker A created anchor: {}", handle.anchor_id);

    // Wait for handlers to be ready
    tokio::time::sleep(Duration::from_millis(200)).await;

    // B attaches to the anchor
    let mut sink =
        ResponseAnchorSource::<TestData>::attach(handle.clone(), client_b.clone()).await?;

    tracing::info!("Worker B attached to anchor");

    // B sends some data
    sink.send_ok(TestData {
        value: 42,
        message: "Hello from B".to_string(),
    })
    .await?;

    sink.send_ok(TestData {
        value: 100,
        message: "Second message".to_string(),
    })
    .await?;

    // A receives the data
    let item1 = stream.recv().await.expect("Should receive item 1")?;
    assert_eq!(item1.value, 42);
    assert_eq!(item1.message, "Hello from B");

    let item2 = stream.recv().await.expect("Should receive item 2")?;
    assert_eq!(item2.value, 100);
    assert_eq!(item2.message, "Second message");

    // B finalizes
    sink.finalize().await?;

    tracing::info!("Worker B finalized anchor");

    // A should see stream close
    let closed = stream.recv().await;
    assert!(closed.is_none(), "Stream should be closed after finalize");

    manager_a.shutdown().await?;
    manager_b.shutdown().await?;

    Ok(())
}

/// Test detach and reattach flow: A creates, B attaches/detaches, C attaches/finalizes
#[tokio::test(flavor = "multi_thread")]
async fn test_detach_reattach_flow() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init()
        .ok();

    let cancel_token = CancellationToken::new();

    // Create three workers
    let manager_a =
        ZmqActiveMessageManager::new("tcp://127.0.0.1:0".to_string(), cancel_token.clone()).await?;
    let manager_b =
        ZmqActiveMessageManager::new("tcp://127.0.0.1:0".to_string(), cancel_token.clone()).await?;
    let manager_c =
        ZmqActiveMessageManager::new("tcp://127.0.0.1:0".to_string(), cancel_token.clone()).await?;

    let client_a = manager_a.client();
    let client_b = manager_b.client();
    let client_c = manager_c.client();

    let peer_a_info = manager_a.peer_info().await;
    let peer_b_info = manager_b.peer_info().await;
    let peer_c_info = manager_c.peer_info().await;

    // Connect all workers
    client_a.connect_to_peer(peer_b_info.clone()).await?;
    client_a.connect_to_peer(peer_c_info.clone()).await?;
    client_b.connect_to_peer(peer_a_info.clone()).await?;
    client_c.connect_to_peer(peer_a_info.clone()).await?;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // A creates anchor
    let (handle, mut stream) = manager_a
        .create_response_anchor::<TestData>(peer_a_info.address.clone())
        .await?;

    tracing::info!("Worker A created anchor: {}", handle.anchor_id);

    // B attaches and sends data
    let mut sink_b =
        ResponseAnchorSource::<TestData>::attach(handle.clone(), client_b.clone()).await?;

    tracing::info!("Worker B attached to anchor");

    sink_b
        .send_ok(TestData {
            value: 1,
            message: "From B - message 1".to_string(),
        })
        .await?;

    // A receives B's message
    let msg1 = stream.recv().await.expect("Should receive B's message")?;
    assert_eq!(msg1.value, 1);
    assert_eq!(msg1.message, "From B - message 1");

    // B detaches
    sink_b.detach().await?;

    tracing::info!("Worker B detached from anchor");

    // Small delay to ensure detach is processed
    tokio::time::sleep(Duration::from_millis(100)).await;

    // C attaches to the same anchor
    let mut sink_c =
        ResponseAnchorSource::<TestData>::attach(handle.clone(), client_c.clone()).await?;

    tracing::info!("Worker C attached to anchor");

    sink_c
        .send_ok(TestData {
            value: 2,
            message: "From C - message 1".to_string(),
        })
        .await?;

    sink_c
        .send_ok(TestData {
            value: 3,
            message: "From C - message 2".to_string(),
        })
        .await?;

    // A receives C's messages
    let msg2 = stream
        .recv()
        .await
        .expect("Should receive C's first message")?;
    assert_eq!(msg2.value, 2);
    assert_eq!(msg2.message, "From C - message 1");

    let msg3 = stream
        .recv()
        .await
        .expect("Should receive C's second message")?;
    assert_eq!(msg3.value, 3);
    assert_eq!(msg3.message, "From C - message 2");

    // C finalizes
    sink_c.finalize().await?;

    tracing::info!("Worker C finalized anchor");

    // A should see stream close
    let closed = stream.recv().await;
    assert!(closed.is_none(), "Stream should be closed after finalize");

    manager_a.shutdown().await?;
    manager_b.shutdown().await?;
    manager_c.shutdown().await?;

    Ok(())
}

/// Test passing handle via active message
#[tokio::test(flavor = "multi_thread")]
async fn test_pass_handle_via_active_message() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init()
        .ok();

    let cancel_token = CancellationToken::new();

    let manager_a =
        ZmqActiveMessageManager::new("tcp://127.0.0.1:0".to_string(), cancel_token.clone()).await?;
    let manager_b =
        ZmqActiveMessageManager::new("tcp://127.0.0.1:0".to_string(), cancel_token.clone()).await?;

    let client_a = manager_a.client();
    let client_b = manager_b.client();
    let peer_a_info = manager_a.peer_info().await;
    let peer_b_info = manager_b.peer_info().await;

    client_a.connect_to_peer(peer_b_info.clone()).await?;
    client_b.connect_to_peer(peer_a_info.clone()).await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Create anchor on A
    let (handle, mut stream) = manager_a
        .create_response_anchor::<TestData>(peer_a_info.address.clone())
        .await?;

    tracing::info!("Worker A created anchor: {}", handle.anchor_id);

    // Store received handle on B
    let received_handle = Arc::new(Mutex::new(None));
    let received_handle_clone = received_handle.clone();

    // Register handler on B to receive the handle
    let task_tracker = TaskTracker::new();
    let handle_receiver = typed_unary_handler_with_tracker(
        "receive_handle".to_string(),
        move |ctx: TypedContext<AnchorHandleMessage>| {
            let handle = ctx.input.handle;
            let received = received_handle_clone.clone();

            // Store the handle
            tokio::spawn(async move {
                *received.lock().await = Some(handle);
            });

            Ok(())
        },
        task_tracker.clone(),
    );

    manager_b
        .register_handler("receive_handle".to_string(), handle_receiver)
        .await?;

    tokio::time::sleep(Duration::from_millis(100)).await;

    // A sends handle to B via active message
    client_a
        .active_message("receive_handle")?
        .payload(AnchorHandleMessage {
            handle: handle.clone(),
        })?
        .send(peer_b_info.instance_id)
        .await?;

    // Wait for B to receive the handle
    tokio::time::sleep(Duration::from_millis(200)).await;

    // B should have the handle now
    let stored_handle = received_handle.lock().await.clone();
    assert!(stored_handle.is_some(), "B should have received the handle");
    let received = stored_handle.unwrap();

    tracing::info!("Worker B received handle: {}", received.anchor_id);

    // B attaches using the received handle
    let mut sink = ResponseAnchorSource::<TestData>::attach(received, client_b.clone()).await?;

    tracing::info!("Worker B attached using received handle");

    // B sends data
    sink.send_ok(TestData {
        value: 999,
        message: "Sent via handle from active message".to_string(),
    })
    .await?;

    // A receives the data
    let data = stream.recv().await.expect("Should receive data")?;
    assert_eq!(data.value, 999);
    assert_eq!(data.message, "Sent via handle from active message");

    // Cleanup
    sink.finalize().await?;

    manager_a.shutdown().await?;
    manager_b.shutdown().await?;

    Ok(())
}

/// Test error handling in the stream
#[tokio::test(flavor = "multi_thread")]
async fn test_application_errors() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init()
        .ok();

    let cancel_token = CancellationToken::new();

    let manager_a =
        ZmqActiveMessageManager::new("tcp://127.0.0.1:0".to_string(), cancel_token.clone()).await?;
    let manager_b =
        ZmqActiveMessageManager::new("tcp://127.0.0.1:0".to_string(), cancel_token.clone()).await?;

    let client_a = manager_a.client();
    let client_b = manager_b.client();
    let peer_a_info = manager_a.peer_info().await;
    let peer_b_info = manager_b.peer_info().await;

    client_a.connect_to_peer(peer_b_info.clone()).await?;
    client_b.connect_to_peer(peer_a_info.clone()).await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let (handle, mut stream) = manager_a
        .create_response_anchor::<TestData>(peer_a_info.address.clone())
        .await?;

    let mut sink = ResponseAnchorSource::<TestData>::attach(handle, client_b.clone()).await?;

    // Send successful data
    sink.send_ok(TestData {
        value: 1,
        message: "Success".to_string(),
    })
    .await?;

    // Send application error
    sink.send_err("Something went wrong").await?;

    // Send more successful data
    sink.send_ok(TestData {
        value: 2,
        message: "Success again".to_string(),
    })
    .await?;

    // Receive and verify
    let msg1 = stream.recv().await.expect("Should receive first message")?;
    assert_eq!(msg1.value, 1);

    let err_result = stream.recv().await.expect("Should receive error");
    assert!(err_result.is_err(), "Should be an error");
    let err_msg = err_result.unwrap_err().to_string();
    assert!(
        err_msg.contains("Something went wrong"),
        "Error message should be preserved"
    );

    let msg2 = stream
        .recv()
        .await
        .expect("Should receive second message")?;
    assert_eq!(msg2.value, 2);

    sink.finalize().await?;

    manager_a.shutdown().await?;
    manager_b.shutdown().await?;

    Ok(())
}

/// Simplified test matching debug_anchor
#[tokio::test(flavor = "multi_thread")]
async fn test_simple_attach_send() -> Result<()> {
    let cancel_token = CancellationToken::new();

    let manager_a =
        ZmqActiveMessageManager::new("tcp://127.0.0.1:0".to_string(), cancel_token.clone()).await?;
    let manager_b =
        ZmqActiveMessageManager::new("tcp://127.0.0.1:0".to_string(), cancel_token.clone()).await?;

    let client_b = manager_b.client();
    let peer_a_info = manager_a.peer_info().await;
    let peer_b_info = manager_b.peer_info().await;

    manager_a.client().connect_to_peer(peer_b_info).await?;
    client_b.connect_to_peer(peer_a_info.clone()).await?;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let (handle, mut stream) = manager_a
        .create_response_anchor::<TestData>(peer_a_info.address.clone())
        .await?;

    tokio::time::sleep(Duration::from_millis(300)).await;

    let mut sink = ResponseAnchorSource::<TestData>::attach(handle, client_b).await?;

    sink.send_ok(TestData {
        value: 1,
        message: "test".to_string(),
    })
    .await?;

    let msg = stream.recv().await.expect("Should receive message")?;
    assert_eq!(msg.value, 1);

    sink.finalize().await?;

    manager_a.shutdown().await?;
    manager_b.shutdown().await?;

    Ok(())
}
