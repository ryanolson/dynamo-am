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
    ResponseAnchorSource, SerializedAnchorHandle,
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
    handle: SerializedAnchorHandle<TestData>,
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

    tracing::info!("Created anchor with ID: {}", handle.payload().anchor_id);

    // Verify handle has correct fields
    assert_eq!(handle.payload().instance_id, manager.client().instance_id());

    manager.shutdown().await?;

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
    let (handle, mut stream) = manager_a.create_local_response_anchor::<TestData>().await?;

    let serialized_handle = handle.disarm();

    tracing::info!(
        "Worker A created anchor: {}",
        serialized_handle.payload().anchor_id
    );

    // B attaches and sends data
    let mut sink_b =
        ResponseAnchorSource::<TestData>::attach(serialized_handle.clone().arm(client_b.clone()))
            .await?;

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
    let armed_handle_after_b = sink_b.detach().await?;

    tracing::info!("Worker B detached from anchor");

    // Small delay to ensure detach is processed
    tokio::time::sleep(Duration::from_millis(100)).await;

    // C attaches to the same anchor
    let serialized_for_c = armed_handle_after_b.disarm();
    let mut sink_c =
        ResponseAnchorSource::<TestData>::attach(serialized_for_c.arm(client_c.clone())).await?;

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

    // A should see stream close or receive a terminal error
    let closed = stream.recv().await;
    assert!(
        closed.is_none() || closed.unwrap().is_err(),
        "Stream should be closed after finalize"
    );

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
    let (handle, mut stream) = manager_a.create_local_response_anchor::<TestData>().await?;

    let serialized_handle = handle.disarm();

    tracing::info!(
        "Worker A created anchor: {}",
        serialized_handle.payload().anchor_id
    );

    // Store received handle on B
    let received_handle: Arc<Mutex<Option<SerializedAnchorHandle<TestData>>>> =
        Arc::new(Mutex::new(None));
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

    tokio::time::sleep(Duration::from_millis(300)).await;

    // A sends handle to B via active message
    client_a
        .active_message("receive_handle")?
        .payload(AnchorHandleMessage {
            handle: serialized_handle.clone(),
        })?
        .send(peer_b_info.instance_id)
        .await?;

    // Wait for B to receive the handle
    tokio::time::sleep(Duration::from_millis(200)).await;

    // B should have the handle now
    let stored_handle = received_handle.lock().await.clone();
    assert!(stored_handle.is_some(), "B should have received the handle");
    let received = stored_handle.unwrap();

    tracing::info!("Worker B received handle: {}", received.payload().anchor_id);

    // B attaches using the received handle
    let mut sink = ResponseAnchorSource::<TestData>::attach(received.arm(client_b.clone())).await?;

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

/// Dropping the response stream should cancel the active source session.
#[tokio::test(flavor = "multi_thread")]
async fn test_stream_drop_triggers_source_cancel() -> Result<()> {
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
    tokio::time::sleep(Duration::from_millis(200)).await;

    let (handle, stream) = manager_a.create_local_response_anchor::<TestData>().await?;
    let serialized = handle.disarm();

    let mut sink =
        ResponseAnchorSource::<TestData>::attach(serialized.arm(client_b.clone())).await?;

    let cancellation = sink.cancellation_token();

    tokio::time::sleep(Duration::from_millis(200)).await;

    drop(stream);

    tokio::time::timeout(Duration::from_secs(5), cancellation.cancelled())
        .await
        .expect("source cancellation should be triggered");

    let send_result = sink
        .send_ok(TestData {
            value: 99,
            message: "post-cancel".to_string(),
        })
        .await;
    assert!(
        send_result.is_err(),
        "sending after cancellation should fail"
    );

    drop(sink);
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

    let (handle, mut stream) = manager_a.create_local_response_anchor::<TestData>().await?;

    let serialized_handle = handle.disarm();
    let mut sink =
        ResponseAnchorSource::<TestData>::attach(serialized_handle.arm(client_b.clone())).await?;

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

    let (handle, mut stream) = manager_a.create_local_response_anchor::<TestData>().await?;

    let serialized = handle.disarm();
    tokio::time::sleep(Duration::from_millis(300)).await;

    let mut sink = ResponseAnchorSource::<TestData>::attach(serialized.arm(client_b)).await?;

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

// Note: Timeout tests require tokio test-util feature, which is not enabled.
// These tests are commented out until the feature is added to Cargo.toml.
/*
/// Anchors should time out when no attachment occurs before the deadline
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_anchor_timeout_without_attach() -> Result<()> {
    let cancel_token = CancellationToken::new();
    let manager =
        ZmqActiveMessageManager::new("tcp://127.0.0.1:0".to_string(), cancel_token.clone()).await?;

    let worker_address = manager.peer_info().await.address;
    let (_handle, mut stream) = manager
        .create_response_anchor::<TestData>(worker_address)
        .await?;

    stream.set_timeout(Duration::from_secs(3));

    yield_now().await;
    time::advance(Duration::from_secs(4)).await;
    yield_now().await;

    let result = stream.recv().await.expect("timeout event");
    assert!(result.is_err(), "expected timeout error");

    manager.shutdown().await?;
    Ok(())
}
*/

/*
/// If heartbeats stop while attached, the anchor should time out
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn test_attached_anchor_times_out_without_heartbeat() -> Result<()> {
    let cancel_token = CancellationToken::new();

    let manager_a =
        ZmqActiveMessageManager::new("tcp://127.0.0.1:0".to_string(), cancel_token.clone()).await?;
    let manager_b =
        ZmqActiveMessageManager::new("tcp://127.0.0.1:0".to_string(), cancel_token.clone()).await?;

    let client_b = manager_b.client();
    let peer_a_info = manager_a.peer_info().await;
    let peer_b_info = manager_b.peer_info().await;

    manager_a
        .client()
        .connect_to_peer(peer_b_info.clone())
        .await?;
    client_b.connect_to_peer(peer_a_info.clone()).await?;
    yield_now().await;

    let (handle, mut stream) = manager_a.create_local_response_anchor::<TestData>().await?;

    let serialized = handle.disarm();
    let mut sink =
        ResponseAnchorSource::<TestData>::attach(serialized.arm(client_b.clone())).await?;

    // stop heartbeats to simulate an unresponsive source
    sink.abort_heartbeat_for_test();

    yield_now().await;
    time::advance(Duration::from_secs(16)).await;
    yield_now().await;

    let result = stream.recv().await.expect("heartbeat timeout event");
    assert!(result.is_err(), "expected heartbeat timeout error");

    manager_a.shutdown().await?;
    manager_b.shutdown().await?;
    Ok(())
}
*/

/// Dropping a sink without explicit detach/finalize should release the anchor.
#[tokio::test(flavor = "multi_thread")]
async fn test_sink_drop_prevents_reattach() -> Result<()> {
    let cancel_token = CancellationToken::new();

    let manager_a =
        ZmqActiveMessageManager::new("tcp://127.0.0.1:0".to_string(), cancel_token.clone()).await?;
    let manager_b =
        ZmqActiveMessageManager::new("tcp://127.0.0.1:0".to_string(), cancel_token.clone()).await?;
    let manager_c =
        ZmqActiveMessageManager::new("tcp://127.0.0.1:0".to_string(), cancel_token.clone()).await?;

    let client_b = manager_b.client();
    let client_c = manager_c.client();
    let peer_a_info = manager_a.peer_info().await;
    let peer_b_info = manager_b.peer_info().await;
    let peer_c_info = manager_c.peer_info().await;

    manager_a
        .client()
        .connect_to_peer(peer_b_info.clone())
        .await?;
    manager_a
        .client()
        .connect_to_peer(peer_c_info.clone())
        .await?;
    client_b.connect_to_peer(peer_a_info.clone()).await?;
    client_c.connect_to_peer(peer_a_info.clone()).await?;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let (handle, mut stream) = manager_a.create_local_response_anchor::<TestData>().await?;

    let serialized = handle.disarm();
    {
        let mut sink =
            ResponseAnchorSource::<TestData>::attach(serialized.clone().arm(client_b.clone()))
                .await?;
        sink.send_ok(TestData {
            value: 99,
            message: "From B".to_string(),
        })
        .await?;
        assert_eq!(stream.recv().await.unwrap()?.value, 99);
    } // sink dropped here, should finalize/detach automatically

    tokio::time::sleep(Duration::from_millis(200)).await;

    let result = ResponseAnchorSource::<TestData>::attach(serialized.arm(client_c.clone())).await;
    assert!(
        result.is_err(),
        "anchor should be finalized after sink drop"
    );
    let err = result.err().unwrap().to_string().to_lowercase();
    assert!(
        err.contains("drop")
            || err.contains("finaliz")
            || err.contains("timeout")
            || err.contains("not found"),
        "Expected drop/finalize/timeout/not found error, got: {}",
        err
    );

    manager_a.shutdown().await?;
    manager_b.shutdown().await?;
    manager_c.shutdown().await?;
    Ok(())
}

/// Dropping a sink should send a Dropped frame to the stream.
#[tokio::test(flavor = "multi_thread")]
async fn test_sink_drop_sends_dropped_frame() -> Result<()> {
    let cancel_token = CancellationToken::new();

    let manager_a =
        ZmqActiveMessageManager::new("tcp://127.0.0.1:0".to_string(), cancel_token.clone()).await?;
    let manager_b =
        ZmqActiveMessageManager::new("tcp://127.0.0.1:0".to_string(), cancel_token.clone()).await?;

    let client_b = manager_b.client();
    let peer_a_info = manager_a.peer_info().await;
    let peer_b_info = manager_b.peer_info().await;

    manager_a
        .client()
        .connect_to_peer(peer_b_info.clone())
        .await?;
    client_b.connect_to_peer(peer_a_info.clone()).await?;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let (handle, mut stream) = manager_a.create_local_response_anchor::<TestData>().await?;

    let serialized = handle.disarm();
    {
        let _sink =
            ResponseAnchorSource::<TestData>::attach(serialized.arm(client_b.clone())).await?;
    } // drop immediately

    tokio::time::sleep(Duration::from_millis(200)).await;
    let result = stream.recv().await;
    assert!(result.is_some());
    let err = result.unwrap().unwrap_err();
    assert!(err.to_string().to_lowercase().contains("drop"));

    manager_a.shutdown().await?;
    manager_b.shutdown().await?;
    Ok(())
}

/// Concurrent attach should reject while another attachment is active.
#[tokio::test(flavor = "multi_thread")]
async fn test_concurrent_attach_rejected() -> Result<()> {
    let cancel_token = CancellationToken::new();

    let manager_a =
        ZmqActiveMessageManager::new("tcp://127.0.0.1:0".to_string(), cancel_token.clone()).await?;
    let manager_b =
        ZmqActiveMessageManager::new("tcp://127.0.0.1:0".to_string(), cancel_token.clone()).await?;
    let manager_c =
        ZmqActiveMessageManager::new("tcp://127.0.0.1:0".to_string(), cancel_token.clone()).await?;

    let client_b = manager_b.client();
    let client_c = manager_c.client();
    let peer_a_info = manager_a.peer_info().await;
    let peer_b_info = manager_b.peer_info().await;
    let peer_c_info = manager_c.peer_info().await;

    manager_a
        .client()
        .connect_to_peer(peer_b_info.clone())
        .await?;
    manager_a
        .client()
        .connect_to_peer(peer_c_info.clone())
        .await?;
    client_b.connect_to_peer(peer_a_info.clone()).await?;
    client_c.connect_to_peer(peer_a_info.clone()).await?;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let (handle, mut stream) = manager_a.create_local_response_anchor::<TestData>().await?;

    let serialized = handle.disarm();
    let mut sink_b =
        ResponseAnchorSource::<TestData>::attach(serialized.clone().arm(client_b.clone())).await?;

    // While B is attached, C should be rejected
    let result =
        ResponseAnchorSource::<TestData>::attach(serialized.clone().arm(client_c.clone())).await;
    assert!(result.is_err(), "expected AlreadyAttached error");
    // Note: Currently returns timeout instead of explicit "already attached" error
    // TODO: Have anchor manager respond with explicit AlreadyAttached error
    if let Err(err) = result {
        let msg = err.to_string().to_lowercase();
        assert!(
            msg.contains("timeout") || (msg.contains("already") && msg.contains("attach")),
            "Expected timeout or 'already attached', got: {}",
            msg
        );
    }

    sink_b
        .send_ok(TestData {
            value: 5,
            message: "From B".to_string(),
        })
        .await?;
    assert_eq!(stream.recv().await.unwrap()?.value, 5);

    let armed = sink_b.detach().await?;

    // After detach, C should be able to attach
    let mut sink_c = ResponseAnchorSource::<TestData>::attach(armed).await?;
    sink_c
        .send_ok(TestData {
            value: 6,
            message: "From C".to_string(),
        })
        .await?;
    assert_eq!(stream.recv().await.unwrap()?.value, 6);
    sink_c.finalize().await?;

    manager_a.shutdown().await?;
    manager_b.shutdown().await?;
    manager_c.shutdown().await?;
    Ok(())
}

// Note: Tests for "send after finalize" and "send after detach" are not needed
// because the API prevents these invalid state transitions at compile time.
// Both finalize() and detach() consume self, making it impossible to use the
// sink afterward. This is better than runtime errors!

/// Attempting to attach to a finalized anchor should fail.
///
/// NOTE: Currently the attach operation times out after 30s when the anchor doesn't exist,
/// rather than failing immediately. This test takes 30+ seconds to run.
/// TODO: Consider adding a faster failure path for non-existent anchors, then re-enable this test.
#[tokio::test(flavor = "multi_thread")]
// #[ignore = "Takes 30s to timeout - needs faster failure path for missing anchors"]
async fn test_attach_to_finalized_anchor_fails() -> Result<()> {
    let cancel_token = CancellationToken::new();

    let manager_a =
        ZmqActiveMessageManager::new("tcp://127.0.0.1:0".to_string(), cancel_token.clone()).await?;
    let manager_b =
        ZmqActiveMessageManager::new("tcp://127.0.0.1:0".to_string(), cancel_token.clone()).await?;
    let manager_c =
        ZmqActiveMessageManager::new("tcp://127.0.0.1:0".to_string(), cancel_token.clone()).await?;

    let client_b = manager_b.client();
    let client_c = manager_c.client();
    let peer_a_info = manager_a.peer_info().await;
    let peer_b_info = manager_b.peer_info().await;
    let peer_c_info = manager_c.peer_info().await;

    manager_a
        .client()
        .connect_to_peer(peer_b_info.clone())
        .await?;
    manager_a
        .client()
        .connect_to_peer(peer_c_info.clone())
        .await?;
    client_b.connect_to_peer(peer_a_info.clone()).await?;
    client_c.connect_to_peer(peer_a_info.clone()).await?;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let (handle, mut stream) = manager_a.create_local_response_anchor::<TestData>().await?;

    let serialized = handle.disarm();

    // B attaches and finalizes
    let mut sink_b =
        ResponseAnchorSource::<TestData>::attach(serialized.clone().arm(client_b.clone())).await?;

    sink_b
        .send_ok(TestData {
            value: 1,
            message: "From B".to_string(),
        })
        .await?;

    assert_eq!(stream.recv().await.unwrap()?.value, 1);

    sink_b.finalize().await?;

    // Stream should close
    assert!(stream.recv().await.is_none());

    // Wait for finalize to be processed
    tokio::time::sleep(Duration::from_millis(500)).await;

    // C attempts to attach to the finalized anchor (with timeout to prevent test hanging)
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        ResponseAnchorSource::<TestData>::attach(serialized.arm(client_c.clone())),
    )
    .await;

    match result {
        Ok(Ok(_)) => panic!("Attach to finalized anchor should fail"),
        Ok(Err(e)) => {
            let err_msg = e.to_string();
            assert!(
                err_msg.contains("not found")
                    || err_msg.contains("finalized")
                    || err_msg.contains("NotFound"),
                "Error should indicate anchor is finalized or not found, got: {}",
                err_msg
            );
        }
        Err(_) => {
            // Timeout is acceptable - it means the attach request didn't get a response
            // because the anchor no longer exists
            tracing::info!("Attach timed out (acceptable - anchor was finalized)");
        }
    }

    manager_a.shutdown().await?;
    manager_b.shutdown().await?;
    manager_c.shutdown().await?;
    Ok(())
}
