// SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Golden path test demonstrating the primary response anchor use case.
//!
//! This test shows the intended workflow:
//! - Worker A creates an anchor and passes the handle to worker B
//! - Worker B attaches, sends data, detaches, and passes handle to worker C
//! - Worker C attaches, sends more data, and finalizes
//! - Worker A receives all messages through the stream

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

/// Golden path: A creates anchor → B attaches/sends/detaches → C attaches/sends/finalizes → A receives all
#[tokio::test(flavor = "multi_thread")]
async fn test_three_way_handoff_workflow() -> Result<()> {
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

    // Connect all workers in mesh topology
    client_a.connect_to_peer(peer_b_info.clone()).await?;
    client_a.connect_to_peer(peer_c_info.clone()).await?;
    client_b.connect_to_peer(peer_a_info.clone()).await?;
    client_b.connect_to_peer(peer_c_info.clone()).await?;
    client_c.connect_to_peer(peer_a_info.clone()).await?;
    client_c.connect_to_peer(peer_b_info.clone()).await?;

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Worker A creates response anchor
    let (handle, mut stream) = manager_a.create_local_response_anchor::<TestData>().await?;

    let serialized_handle = handle.disarm();
    tracing::info!(
        "Worker A created anchor: {}",
        serialized_handle.payload().anchor_id
    );

    // Set up handle storage for B and C
    let received_handle_b: Arc<Mutex<Option<SerializedAnchorHandle<TestData>>>> =
        Arc::new(Mutex::new(None));
    let received_handle_c: Arc<Mutex<Option<SerializedAnchorHandle<TestData>>>> =
        Arc::new(Mutex::new(None));

    let received_handle_b_clone = received_handle_b.clone();
    let received_handle_c_clone = received_handle_c.clone();

    // Register "receive_handle" handler on B
    let task_tracker_b = TaskTracker::new();
    let handle_receiver_b = typed_unary_handler_with_tracker(
        "receive_handle".to_string(),
        move |ctx: TypedContext<AnchorHandleMessage>| {
            let handle = ctx.input.handle;
            let received = received_handle_b_clone.clone();

            // Store the handle
            tokio::spawn(async move {
                *received.lock().await = Some(handle);
            });

            Ok(())
        },
        task_tracker_b.clone(),
    );

    manager_b
        .register_handler("receive_handle".to_string(), handle_receiver_b)
        .await?;

    // Register "receive_handle" handler on C
    let task_tracker_c = TaskTracker::new();
    let handle_receiver_c = typed_unary_handler_with_tracker(
        "receive_handle".to_string(),
        move |ctx: TypedContext<AnchorHandleMessage>| {
            let handle = ctx.input.handle;
            let received = received_handle_c_clone.clone();

            tokio::spawn(async move {
                *received.lock().await = Some(handle);
            });

            Ok(())
        },
        task_tracker_c.clone(),
    );

    manager_c
        .register_handler("receive_handle".to_string(), handle_receiver_c)
        .await?;

    // Wait longer for handlers to be fully registered
    tokio::time::sleep(Duration::from_millis(500)).await;

    // ===== Step 1: A sends handle to B =====
    tracing::info!("A sending handle to B");
    client_a
        .active_message("receive_handle")?
        .payload(AnchorHandleMessage {
            handle: serialized_handle.clone(),
        })?
        .expect_response::<()>()
        .send(peer_b_info.instance_id)
        .await?
        .await_response::<()>()
        .await?;

    // Wait longer to ensure the spawn task stores the handle
    tokio::time::sleep(Duration::from_millis(500)).await;

    // ===== Step 2: B attaches and sends one message =====
    let handle_for_b = received_handle_b
        .lock()
        .await
        .clone()
        .expect("B should have received handle");

    tracing::info!("Worker B attaching to anchor");
    let mut sink_b =
        ResponseAnchorSource::<TestData>::attach(handle_for_b.arm(client_b.clone())).await?;

    tracing::info!("Worker B sending message");
    sink_b
        .send_ok(TestData {
            value: 1,
            message: "From B".to_string(),
        })
        .await?;

    // ===== Step 3: B detaches and sends handle to C =====
    tracing::info!("Worker B detaching");
    let armed_handle_from_b = sink_b.detach().await?;
    tracing::info!(
        "B got armed handle back from detach: anchor_id={}",
        armed_handle_from_b.payload().anchor_id
    );

    let serialized_for_c = armed_handle_from_b.disarm();
    tracing::info!(
        "B disarmed handle for C: anchor_id={}",
        serialized_for_c.payload().anchor_id
    );

    tracing::info!("B sending handle to C");
    client_b
        .active_message("receive_handle")?
        .payload(AnchorHandleMessage {
            handle: serialized_for_c,
        })?
        .expect_response::<()>()
        .send(peer_c_info.instance_id)
        .await?
        .await_response::<()>()
        .await?;

    // Wait longer to ensure the spawn task stores the handle
    tokio::time::sleep(Duration::from_millis(500)).await;

    // ===== Step 4: C attaches and sends two messages =====
    let handle_for_c = received_handle_c
        .lock()
        .await
        .clone()
        .expect("C should have received handle");

    tracing::info!(
        "Worker C received handle: anchor_id={}",
        handle_for_c.payload().anchor_id
    );
    tracing::info!("Worker C arming handle with client");
    let armed_for_c = handle_for_c.arm(client_c.clone());
    tracing::info!("Worker C armed handle, now attaching");
    let mut sink_c = ResponseAnchorSource::<TestData>::attach(armed_for_c).await?;
    tracing::info!("Worker C attached successfully");

    tracing::info!("Worker C sending first message");
    sink_c
        .send_ok(TestData {
            value: 2,
            message: "From C - first".to_string(),
        })
        .await?;

    tracing::info!("Worker C sending second message");
    sink_c
        .send_ok(TestData {
            value: 3,
            message: "From C - second".to_string(),
        })
        .await?;

    // ===== Step 5: C finalizes =====
    tracing::info!("Worker C finalizing");
    sink_c.finalize().await?;

    // ===== Step 6: A receives all three messages =====
    tracing::info!("Worker A receiving messages");

    let msg1 = stream.recv().await.expect("Should receive message 1")?;
    assert_eq!(msg1.value, 1);
    assert_eq!(msg1.message, "From B");
    tracing::info!("✓ Received message 1 from B");

    let msg2 = stream.recv().await.expect("Should receive message 2")?;
    assert_eq!(msg2.value, 2);
    assert_eq!(msg2.message, "From C - first");
    tracing::info!("✓ Received message 2 from C");

    let msg3 = stream.recv().await.expect("Should receive message 3")?;
    assert_eq!(msg3.value, 3);
    assert_eq!(msg3.message, "From C - second");
    tracing::info!("✓ Received message 3 from C");

    // ===== Step 7: Stream should close =====
    let closed = stream.recv().await;
    assert!(closed.is_none(), "Stream should be closed after finalize");
    tracing::info!("✓ Stream closed cleanly");

    tracing::info!("=== Golden path test completed successfully ===");

    manager_a.shutdown().await?;
    manager_b.shutdown().await?;
    manager_c.shutdown().await?;

    Ok(())
}
