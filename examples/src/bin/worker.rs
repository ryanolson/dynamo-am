// SPDX-FileCopyrightText: Copyright (c) 2024-2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Worker process that joins a leader's cohort.
//!
//! This worker connects to a leader endpoint, registers as a cohort member,
//! and handles computation requests. Multiple workers can run concurrently.
//!
//! **Usage:** `LEADER_ENDPOINT=tcp://127.0.0.1:5555 cargo run --bin worker`
//!
//! **Environment:**
//! - `LEADER_ENDPOINT`: Leader address to connect to (default: tcp://127.0.0.1:5555)
//! - `RANK`: Optional worker rank identifier
//!
//! **Demonstrates:**
//! - Worker registration with cohort leader
//! - Handler registration for distributed processing
//! - Long-running worker processes

use anyhow::Result;
use dynamo_am::{
    client::{ActiveMessageClient, PeerInfo},
    manager::ActiveMessageManager,
    zmq::ZmqActiveMessageManager,
};
use std::env;
use tokio_util::sync::CancellationToken;

use active_message_example::create_compute_handler;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    println!("Starting Worker");

    let leader_endpoint =
        env::var("LEADER_ENDPOINT").unwrap_or_else(|_| "tcp://127.0.0.1:5555".to_string());

    // Get worker rank from environment (optional for MPI/torch.distributed compatibility)
    let worker_rank = env::var("RANK").ok().and_then(|r| r.parse::<usize>().ok());

    if let Some(rank) = worker_rank {
        println!("Worker rank: {}", rank);
    }

    let cancel_token = CancellationToken::new();

    let manager =
        ZmqActiveMessageManager::new("tcp://0.0.0.0:0".to_string(), cancel_token.clone()).await?;

    let client = manager.client();

    println!("Worker listening on: {}", client.endpoint());
    println!("Worker instance ID: {}", client.instance_id());

    let compute_handler = create_compute_handler();
    manager
        .register_handler("compute".to_string(), compute_handler)
        .await?;

    println!("Registered compute handler");

    // Discover leader instance ID (in real usage, this might be configured differently)
    let leader_instance_id = uuid::Uuid::new_v4();
    let leader_peer = PeerInfo::new(leader_instance_id, leader_endpoint.clone());

    client.connect_to_peer(leader_peer).await?;

    println!("Connected to leader at {}", leader_endpoint);

    // Join cohort with optional rank - now works directly on concrete client!
    match client.join_cohort(leader_instance_id, worker_rank).await {
        Ok(response) => {
            if response.accepted {
                println!(
                    "Successfully joined cohort at position: {:?}",
                    response.position
                );
                if let Some(rank) = response.expected_rank {
                    println!("Assigned rank: {}", rank);
                }
            } else {
                println!("Failed to join cohort: {:?}", response.reason);
                return Ok(());
            }
        }
        Err(e) => {
            println!("Error joining cohort: {}", e);
            return Ok(());
        }
    }

    cancel_token.cancelled().await;

    println!("Worker shutting down");
    manager.shutdown().await?;

    Ok(())
}
