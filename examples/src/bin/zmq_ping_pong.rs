// SPDX-FileCopyrightText: Copyright (c) 2024-2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Lightweight ZMQ pub-sub benchmark to measure raw ZMQ performance.
//!
//! This benchmark creates a bidirectional pub-sub setup between two endpoints
//! and measures the round-trip time for ping-pong messages, providing a baseline
//! for comparison against the full active message system.

use anyhow::Result;
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::time::{Duration, Instant};
use tempfile::NamedTempFile;
use tmq::{publish, subscribe, Context, Message, Multipart};
use tokio::sync::mpsc;
use tracing::info;

/// Message metadata for ping-pong
#[derive(Debug, Clone, Serialize, Deserialize)]
struct MessageMeta {
    msg_type: String, // "ping" or "pong"
    sequence: u64,
}

/// Generate a unique IPC socket path for testing
fn unique_ipc_socket_path() -> Result<String> {
    let temp_file = NamedTempFile::new()?;
    let path = temp_file.path().to_string_lossy().to_string();
    drop(temp_file);
    Ok(format!("ipc://{}", path))
}

/// Create a multipart message (mimicking active message format)
fn create_message(msg_type: &str, sequence: u64, payload: &[u8]) -> Result<Multipart> {
    let mut parts = VecDeque::new();

    // Part 1: Metadata
    let meta = MessageMeta {
        msg_type: msg_type.to_string(),
        sequence,
    };
    parts.push_back(Message::from(serde_json::to_vec(&meta)?));

    // Part 2: Payload
    parts.push_back(Message::from(payload));

    Ok(Multipart(parts))
}

/// Parse a multipart message
fn parse_message(multipart: Multipart) -> Result<(MessageMeta, Bytes)> {
    if multipart.len() < 2 {
        anyhow::bail!(
            "Invalid multipart message: expected 2 parts, got {}",
            multipart.len()
        );
    }

    // Part 1: Metadata
    let meta: MessageMeta = serde_json::from_slice(&*multipart[0])?;

    // Part 2: Payload
    let payload = Bytes::copy_from_slice(&*multipart[1]);

    Ok((meta, payload))
}

/// Run endpoint A (initiates ping)
async fn run_endpoint_a(
    pub_socket: String,
    sub_socket: String,
    rounds: usize,
    ready_tx: mpsc::Sender<()>,
    mut ready_rx: mpsc::Receiver<()>,
) -> Result<Vec<Duration>> {
    let ctx = Context::new();

    // Create publisher
    let mut publisher = publish(&ctx).bind(&pub_socket)?;

    // Create subscriber
    let mut subscriber = subscribe(&ctx).connect(&sub_socket)?.subscribe(b"")?;

    info!("Endpoint A: Sockets created");

    // Signal we're ready
    ready_tx.send(()).await?;

    // Wait for endpoint B to be ready
    ready_rx
        .recv()
        .await
        .ok_or_else(|| anyhow::anyhow!("Failed to get ready signal from B"))?;

    // Additional delay for ZMQ subscription propagation
    tokio::time::sleep(Duration::from_millis(500)).await;

    let mut rtts = Vec::new();

    // Warmup round (not measured)
    info!("Endpoint A: Sending warmup ping");
    let warmup_msg = create_message("ping", 0, b"warmup")?;
    publisher.send(warmup_msg).await?;

    // Wait for warmup pong
    if let Some(Ok(multipart)) = subscriber.next().await {
        let (meta, _) = parse_message(multipart)?;
        if meta.msg_type != "pong" {
            anyhow::bail!("Expected pong, got {}", meta.msg_type);
        }
        info!("Endpoint A: Received warmup pong");
    }

    // Small delay after warmup
    tokio::time::sleep(Duration::from_millis(50)).await;

    info!("Endpoint A: Starting {} ping-pong rounds", rounds);

    // Measurement rounds
    for i in 1..=rounds {
        let start = Instant::now();

        // Send ping
        let ping_msg = create_message("ping", i as u64, format!("ping_{}", i).as_bytes())?;
        publisher.send(ping_msg).await?;

        // Wait for pong
        match subscriber.next().await {
            Some(Ok(multipart)) => {
                let (meta, _payload) = parse_message(multipart)?;
                if meta.msg_type != "pong" || meta.sequence != i as u64 {
                    anyhow::bail!("Unexpected response: {:?}", meta);
                }

                let rtt = start.elapsed();
                rtts.push(rtt);

                if i % 10 == 0 {
                    info!("Round {}: RTT = {:?}", i, rtt);
                }
            }
            Some(Err(e)) => {
                anyhow::bail!("Failed to receive pong {}: {}", i, e);
            }
            None => {
                anyhow::bail!("Subscriber stream ended unexpectedly at round {}", i);
            }
        }

        // Small delay between rounds to avoid overwhelming
        if i % 10 == 0 {
            tokio::time::sleep(Duration::from_micros(10)).await;
        }
    }

    Ok(rtts)
}

/// Run endpoint B (responds with pong)
async fn run_endpoint_b(
    pub_socket: String,
    sub_socket: String,
    rounds: usize,
    ready_tx: mpsc::Sender<()>,
    mut ready_rx: mpsc::Receiver<()>,
) -> Result<()> {
    let ctx = Context::new();

    // Create publisher
    let mut publisher = publish(&ctx).bind(&pub_socket)?;

    // Create subscriber
    let mut subscriber = subscribe(&ctx).connect(&sub_socket)?.subscribe(b"")?;

    info!("Endpoint B: Sockets created");

    // Signal we're ready
    ready_tx.send(()).await?;

    // Wait for endpoint A to be ready
    ready_rx
        .recv()
        .await
        .ok_or_else(|| anyhow::anyhow!("Failed to get ready signal from A"))?;

    // Additional delay for ZMQ subscription propagation
    tokio::time::sleep(Duration::from_millis(500)).await;

    info!("Endpoint B: Ready to respond to pings");

    let mut received_count = 0;

    loop {
        match subscriber.next().await {
            Some(Ok(multipart)) => {
                let (meta, payload) = parse_message(multipart)?;

                if meta.msg_type == "ping" {
                    // Immediately send pong response
                    let pong_msg = create_message("pong", meta.sequence, &payload)?;
                    publisher.send(pong_msg).await?;

                    if meta.sequence > 0 {
                        // Don't count warmup
                        received_count += 1;
                        if received_count >= rounds {
                            break;
                        }
                    }
                }
            }
            Some(Err(e)) => {
                anyhow::bail!("Endpoint B receive error: {}", e);
            }
            None => {
                anyhow::bail!("Endpoint B subscriber stream ended unexpectedly");
            }
        }
    }

    info!("Endpoint B: Completed {} rounds", received_count);
    Ok(())
}

/// Calculate and print statistics
fn print_statistics(rtts: &mut Vec<Duration>) {
    if rtts.is_empty() {
        println!("No measurements collected");
        return;
    }

    rtts.sort();

    let min = rtts.first().unwrap();
    let max = rtts.last().unwrap();
    let sum: Duration = rtts.iter().sum();
    let mean = sum / rtts.len() as u32;
    let median = rtts[rtts.len() / 2];
    let p95 = rtts[(rtts.len() as f64 * 0.95) as usize];
    let p99 = rtts[(rtts.len() as f64 * 0.99) as usize];

    println!("\n=== ZMQ Pub-Sub Ping-Pong Statistics ===");
    println!("Rounds completed: {}", rtts.len());
    println!("Min RTT:    {:?}", min);
    println!("Mean RTT:   {:?}", mean);
    println!("Median RTT: {:?}", median);
    println!("P95 RTT:    {:?}", p95);
    println!("P99 RTT:    {:?}", p99);
    println!("Max RTT:    {:?}", max);

    // Calculate standard deviation
    let variance: f64 = rtts
        .iter()
        .map(|rtt| {
            let diff = rtt.as_nanos() as f64 - mean.as_nanos() as f64;
            diff * diff
        })
        .sum::<f64>()
        / rtts.len() as f64;
    let std_dev = Duration::from_nanos(variance.sqrt() as u64);
    println!("Std Dev:    {:?}", std_dev);

    // Throughput
    let total_time = rtts.iter().sum::<Duration>();
    let throughput = rtts.len() as f64 / total_time.as_secs_f64();
    println!("Throughput: {:.2} round-trips/sec", throughput);
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    println!("Starting ZMQ Pub-Sub Ping-Pong Benchmark");
    println!("=========================================");
    println!("This benchmark measures raw ZMQ pub-sub round-trip times");
    println!("for comparison with the full active message system.\n");

    // Create unique IPC paths
    let socket_a = unique_ipc_socket_path()?;
    let socket_b = unique_ipc_socket_path()?;

    println!("Socket A: {}", socket_a);
    println!("Socket B: {}", socket_b);

    let rounds = 100;

    // Create channels for synchronization
    let (ready_a_tx, ready_a_rx) = mpsc::channel::<()>(1);
    let (ready_b_tx, ready_b_rx) = mpsc::channel::<()>(1);

    // Create channel to signal completion
    let (result_tx, mut result_rx) = mpsc::channel::<Vec<Duration>>(1);

    // Spawn endpoint B (responder)
    let socket_a_clone = socket_a.clone();
    let socket_b_clone = socket_b.clone();
    let endpoint_b = tokio::spawn(async move {
        if let Err(e) = run_endpoint_b(
            socket_b_clone,
            socket_a_clone,
            rounds,
            ready_b_tx,
            ready_a_rx,
        )
        .await
        {
            eprintln!("Endpoint B error: {}", e);
        }
    });

    // Spawn endpoint A (initiator)
    let socket_a_clone = socket_a.clone();
    let socket_b_clone = socket_b.clone();
    let endpoint_a = tokio::spawn(async move {
        match run_endpoint_a(
            socket_a_clone,
            socket_b_clone,
            rounds,
            ready_a_tx,
            ready_b_rx,
        )
        .await
        {
            Ok(rtts) => {
                let _ = result_tx.send(rtts).await;
            }
            Err(e) => {
                eprintln!("Endpoint A error: {}", e);
            }
        }
    });

    // Wait for results
    if let Some(mut rtts) = result_rx.recv().await {
        print_statistics(&mut rtts);

        // Compare with active message system if available
        println!("\n=== Comparison Notes ===");
        println!("This measures raw ZMQ pub-sub performance without:");
        println!("  - Handler dispatch overhead");
        println!("  - ACK/NACK protocol");
        println!("  - Message validation");
        println!("  - Auto-registration");
        println!("  - Serialization of complex types");
        println!("\nCompare these results with ping_pong.rs to measure");
        println!("the overhead of the active message system.");
    }

    // Wait for both endpoints to complete
    let _ = tokio::join!(endpoint_a, endpoint_b);

    println!("\nBenchmark complete!");

    Ok(())
}
