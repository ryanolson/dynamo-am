// SPDX-FileCopyrightText: Copyright (c) 2024-2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Leader process for distributed cohort coordination.
//!
//! This leader waits for workers to register, then broadcasts computation requests
//! across the cohort. Run multiple `worker` processes to join this leader's cohort.
//!
//! **Usage:** `cargo run --bin leader`
//!
//! **Demonstrates:**
//! - Cohort leader coordination with dynamic worker registration
//! - Broadcasting messages to all cohort members
//! - Managing distributed worker lifecycle

use anyhow::Result;
use bytes::Bytes;
use dynamo_am::{
    client::ActiveMessageClient,
    cohort::{CohortType, LeaderWorkerCohort},
    ActiveMessageManagerBuilder,
};
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

use active_message_example::ComputeRequest;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    println!("Starting Leader");

    let cancel_token = CancellationToken::new();

    let manager = ActiveMessageManagerBuilder::new()
        .endpoint("tcp://0.0.0.0:5555".to_string())
        .cancel_token(cancel_token.clone())
        .build()
        .await?;

    let client = manager.client();

    println!("Leader listening on: {}", client.endpoint());
    println!("Leader instance ID: {}", client.instance_id());

    // Create cohort with 2 expected workers
    let cohort = Arc::new(LeaderWorkerCohort::new(
        client.clone(),
        CohortType::FixedSize(2),
    ));

    println!("Created cohort - leader-driven model");
    println!("NOTE: In leader-driven mode, leader must manually add worker instance IDs");
    println!("Example: Configure worker endpoints, connect to them, then call:");
    println!("  cohort.add_worker(worker_instance_id, rank).await");
    println!();
    println!("For this example, we'll simulate by adding known worker IDs");
    println!("In production, you would:");
    println!("  1. Discover workers via service discovery or configuration");
    println!("  2. Connect to each worker: client.connect_to_peer(worker_peer)");
    println!("  3. Add to cohort: cohort.add_worker(worker.instance_id(), Some(rank))");
    println!();

    // In a real implementation, you would:
    // 1. Get worker endpoints from config or service discovery
    // 2. For each worker:
    //    let worker_peer = PeerInfo::new(worker_id, worker_endpoint);
    //    client.connect_to_peer(worker_peer).await?;
    //    cohort.add_worker(worker_id, Some(rank)).await?;
    // 3. Once cohort is full, proceed with work distribution

    println!();
    println!("Leader will now wait for shutdown signal...");
    cancel_token.cancelled().await;

    println!("Leader shutting down");
    manager.shutdown().await?;
    cancel_token.cancel();

    Ok(())
}
