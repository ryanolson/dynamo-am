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
    manager::ActiveMessageManager,
    zmq::ZmqActiveMessageManager,
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

    let manager =
        ZmqActiveMessageManager::new("tcp://0.0.0.0:5555".to_string(), cancel_token.clone())
            .await?;

    let client = manager.client();

    println!("Leader listening on: {}", client.endpoint());
    println!("Leader instance ID: {}", client.instance_id());

    // Create cohort with 2 expected workers
    let cohort = Arc::new(LeaderWorkerCohort::new(
        client.clone(),
        CohortType::FixedSize(2),
    ));

    // Register cohort-specific handlers (_join_cohort) with THIS cohort instance
    // This allows workers to join this specific cohort
    let task_tracker = tokio_util::task::TaskTracker::new();
    cohort
        .register_handlers(manager.control_tx(), task_tracker)
        .await?;

    println!("Registered cohort handlers");

    println!("Waiting for 2 workers to join cohort...");

    // Wait for workers to join the cohort
    let mut attempts = 0;
    let max_attempts = 30; // 30 seconds with 1-second intervals

    loop {
        if cohort.is_full().await {
            break;
        }

        attempts += 1;
        if attempts >= max_attempts {
            println!("Cohort not ready - timeout waiting for workers");
            return Ok(());
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    println!("Cohort ready! Waiting for workers to register compute handler...");

    // Wait for compute handler on all workers
    let workers_ready = cohort
        .await_handler_on_all_workers("compute", Some(Duration::from_secs(30)))
        .await
        .is_ok();

    if !workers_ready {
        println!("Workers do not have compute handler - skipping broadcast");
    } else {
        let request = ComputeRequest {
            x: 10,
            y: 20,
            operation: "add".to_string(),
        };

        let payload = Bytes::from(serde_json::to_vec(&request)?);

        println!("Broadcasting compute request to workers by rank");
        cohort.broadcast_by_rank("compute", payload).await?;
    }

    tokio::time::sleep(Duration::from_secs(2)).await;

    println!("Leader initiating graceful shutdown...");
    cohort.initiate_graceful_shutdown().await?;

    println!("Leader shutting down");
    manager.shutdown().await?;
    cancel_token.cancel();

    Ok(())
}
