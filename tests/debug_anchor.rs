// SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Minimal debug test for response anchors

use anyhow::Result;
use dynamo_am::{
    ResponseAnchorSource, client::ActiveMessageClient, manager::ActiveMessageManager,
    zmq::ZmqActiveMessageManager,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct TestData {
    value: i32,
}

#[tokio::test(flavor = "multi_thread")]
async fn test_minimal_attach() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init()
        .ok();

    let cancel_token = CancellationToken::new();

    // Create two workers
    println!("Creating manager A...");
    let manager_a =
        ZmqActiveMessageManager::new("tcp://127.0.0.1:0".to_string(), cancel_token.clone()).await?;
    println!("Manager A created");

    println!("Creating manager B...");
    let manager_b =
        ZmqActiveMessageManager::new("tcp://127.0.0.1:0".to_string(), cancel_token.clone()).await?;
    println!("Manager B created");

    let client_a = manager_a.client();
    let client_b = manager_b.client();
    let peer_a_info = manager_a.peer_info().await;
    let peer_b_info = manager_b.peer_info().await;

    // Connect workers
    println!("Connecting workers...");
    client_a.connect_to_peer(peer_b_info.clone()).await?;
    client_b.connect_to_peer(peer_a_info.clone()).await?;
    tokio::time::sleep(Duration::from_millis(200)).await;
    println!("Workers connected");

    // Create anchor on A
    println!("Creating anchor on A...");
    let (handle, mut stream) = manager_a.create_local_response_anchor::<TestData>().await?;
    let serialized = handle.disarm();
    println!("Anchor created: {}", serialized.payload().anchor_id);

    // Wait a bit to ensure handler is registered
    tokio::time::sleep(Duration::from_millis(500)).await;

    // B tries to attach
    println!("B attempting to attach...");
    let mut sink = tokio::time::timeout(
        Duration::from_secs(5),
        ResponseAnchorSource::<TestData>::attach(serialized.arm(client_b.clone())),
    )
    .await
    .expect("Attach timed out")
    .expect("Attach failed");
    println!("SUCCESS: B attached successfully");

    // Try to send data
    println!("B sending data...");
    let send_result =
        tokio::time::timeout(Duration::from_secs(5), sink.send_ok(TestData { value: 42 })).await;

    match send_result {
        Ok(Ok(())) => println!("SUCCESS: Data sent"),
        Ok(Err(e)) => println!("ERROR: Send failed: {}", e),
        Err(_) => println!("TIMEOUT: Send timed out after 5s"),
    }

    // Try to receive data on A
    println!("A attempting to receive...");
    let recv_result = tokio::time::timeout(Duration::from_secs(5), stream.recv()).await;

    match recv_result {
        Ok(Some(Ok(data))) => println!("SUCCESS: Received data: {:?}", data),
        Ok(Some(Err(e))) => println!("ERROR: Received error: {}", e),
        Ok(None) => println!("Stream closed"),
        Err(_) => println!("TIMEOUT: Receive timed out after 5s"),
    }

    // Finalize
    println!("B finalizing...");
    sink.finalize().await?;
    println!("Finalized");

    manager_a.shutdown().await?;
    manager_b.shutdown().await?;

    Ok(())
}
