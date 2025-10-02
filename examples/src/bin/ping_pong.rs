// SPDX-FileCopyrightText: Copyright (c) 2024-2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Latency measurement example using unary handlers with ACK-only responses.
//!
//! This example measures round-trip time (RTT) by sending ping messages that
//! receive acknowledgement responses without payload data. The client tracks
//! latency statistics across multiple iterations.
//!
//! **Demonstrates:**
//! - Unary handlers returning ACK without response payload
//! - Latency measurement and statistics
//! - High-frequency message patterns
//! - Performance characteristics of the messaging system

use anyhow::Result;
use dynamo_am::{
    client::{ActiveMessageClient, PeerInfo},
    handler_impls::{unary_handler, UnaryContext},
    manager::ActiveMessageManager,
    zmq::ZmqActiveMessageManager,
    MessageBuilder,
};
use std::time::{Duration, Instant};
use tempfile::NamedTempFile;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

/// Generate a unique IPC socket path for testing
fn unique_ipc_socket_path() -> Result<String> {
    let temp_file = NamedTempFile::new()?;
    let path = temp_file.path().to_string_lossy().to_string();
    // Close the file but keep the path - ZMQ will create the socket
    drop(temp_file);
    Ok(format!("ipc://{}", path))
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    println!("Starting fast-path ping-pong RTT measurement example");

    let cancel_token = CancellationToken::new();

    // Create two managers for ping-pong
    let server_manager =
        ZmqActiveMessageManager::new(unique_ipc_socket_path()?, cancel_token.clone()).await?;

    let client_manager =
        ZmqActiveMessageManager::new(unique_ipc_socket_path()?, cancel_token.clone()).await?;

    // Register ping handler on server using unary handler (sends ACK)
    let ping_handler = unary_handler("ping".to_string(), |ctx: UnaryContext| {
        let message: String = serde_json::from_slice(&ctx.payload)
            .map_err(|e| format!("Failed to deserialize ping payload: {}", e))?;
        info!("Received ping: {}", message);
        // Return Ok(None) to send ACK without payload
        Ok(None)
    });

    server_manager
        .register_handler("ping".to_string(), ping_handler)
        .await?;

    let server_client = server_manager.client();
    let client_client = client_manager.client();

    println!("Server listening on: {}", server_client.endpoint());
    println!("Client endpoint: {}", client_client.endpoint());

    // Connect client to server (one-way connection)
    // Auto-registration will handle the reverse connection when ACK is needed
    let server_peer = PeerInfo::new(
        server_client.instance_id(),
        server_client.endpoint().to_string(),
    );

    client_client.connect_to_peer(server_peer).await?;

    // Wait for connection to establish
    tokio::time::sleep(Duration::from_millis(100)).await;

    println!("Performing warmup ping to establish publisher tasks...");

    // Warmup ping to establish the publisher task (not measured)
    let warmup_result = MessageBuilder::new(client_client.as_ref(), "ping")?
        .payload("warmup")?
        .send(server_client.instance_id())
        .await;

    match warmup_result {
        Ok(_) => println!("Warmup complete - publisher tasks established"),
        Err(e) => {
            println!("Warmup failed: {}", e);
            return Ok(());
        }
    }

    // Small delay after warmup
    tokio::time::sleep(Duration::from_millis(50)).await;

    println!("Starting fast-path ping-pong measurements...");

    // Perform multiple ping-pong rounds for RTT measurement (fast path only)
    let mut rtts = Vec::new();
    let rounds = 1000;

    for i in 1..=rounds {
        let start = Instant::now();

        let message = format!("ping_{}", i);

        // Send ping and wait for ACK
        let result = MessageBuilder::new(client_client.as_ref(), "ping")?
            .payload(&message)?
            .send(server_client.instance_id())
            .await;

        match result {
            Ok(_) => {
                let rtt = start.elapsed();
                rtts.push(rtt);
                if i % 100 == 0 {
                    println!("Fast-path round {}: RTT = {:?}", i, rtt);
                }
            }
            Err(e) => {
                warn!("Fast-path round {} failed: {}", i, e);
            }
        }

        // Small delay between rounds
        // tokio::time::sleep(Duration::from_millis(1)).await;
    }

    // Calculate statistics
    if !rtts.is_empty() {
        let total: Duration = rtts.iter().sum();
        let avg = total / rtts.len() as u32;
        let min = rtts.iter().min().unwrap();
        let max = rtts.iter().max().unwrap();

        println!("=== Fast-Path RTT Statistics ===");
        println!("Fast-path rounds completed: {}/{}", rtts.len(), rounds);
        println!("Average fast-path RTT: {:?}", avg);
        println!("Min fast-path RTT: {:?}", min);
        println!("Max fast-path RTT: {:?}", max);

        // Calculate standard deviation
        let variance: f64 = rtts
            .iter()
            .map(|rtt| {
                let diff = rtt.as_nanos() as f64 - avg.as_nanos() as f64;
                diff * diff
            })
            .sum::<f64>()
            / rtts.len() as f64;
        let std_dev = Duration::from_nanos(variance.sqrt() as u64);
        println!("Fast-path std deviation: {:?}", std_dev);
    } else {
        println!("No successful fast-path ping-pong rounds completed");
    }

    println!("Shutting down...");
    server_manager.shutdown().await?;
    client_manager.shutdown().await?;
    cancel_token.cancel();

    Ok(())
}
