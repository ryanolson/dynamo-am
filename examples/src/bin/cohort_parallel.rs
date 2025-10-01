// SPDX-FileCopyrightText: Copyright (c) 2024-2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Cohort-based parallel processing example with leader-worker coordination.
//!
//! This example demonstrates parallel work distribution across a cohort of workers.
//! A leader broadcasts work requests to multiple workers and collects their responses.
//!
//! **Demonstrates:**
//! - Cohort creation and worker registration
//! - Parallel broadcast to multiple workers
//! - Leader-worker coordination patterns
//! - Response collection from distributed workers

use anyhow::Result;
use dynamo_am::{
    client::{ActiveMessageClient, WorkerAddress},
    cohort::{CohortType, LeaderWorkerCohort},
    handler_impls::{
        am_handler_with_tracker, typed_unary_handler_with_tracker, AmContext, TypedContext,
    },
    manager::ActiveMessageManager,
    zmq::ZmqActiveMessageManager,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tempfile::NamedTempFile;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

/// Generate a unique IPC socket path for testing
fn unique_ipc_socket_path() -> Result<String> {
    let temp_file = NamedTempFile::new()?;
    let path = temp_file.path().to_string_lossy().to_string();
    // Close the file but keep the path - ZMQ will create the socket
    drop(temp_file);
    Ok(format!("ipc://{}", path))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WorkRequest {
    rank: usize,
    workload: String,
    multiplier: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WorkResponse {
    rank: usize,
    result: String,
    processed_length: usize,
}

// Handler implementations are now inline in main() using the new v2 pattern

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    println!("Starting cohort parallel operations example");

    let cancel_token = CancellationToken::new();

    // Create leader manager
    let leader_manager =
        ZmqActiveMessageManager::new(unique_ipc_socket_path()?, cancel_token.clone()).await?;

    let leader_client = leader_manager.client();
    println!("Leader listening on: {}", leader_client.endpoint());

    // Create worker managers (3 workers with ranks 0, 1, 2)
    let mut worker_managers = Vec::new();
    let mut worker_clients = Vec::new();

    for rank in 0..3 {
        let worker_manager =
            ZmqActiveMessageManager::new(unique_ipc_socket_path()?, cancel_token.clone()).await?;

        // Register handlers on each worker using the new v2 pattern
        let task_tracker = tokio_util::task::TaskTracker::new();

        // Capture rank by value for use in closures
        let worker_rank = rank;

        // Work handler (unary - returns responses)
        let work_handler = typed_unary_handler_with_tracker(
            "work".to_string(),
            move |ctx: TypedContext<WorkRequest>| {
                let request = ctx.input;
                info!(
                    "Worker {} processing work: {} (multiplier: {})",
                    worker_rank, request.workload, request.multiplier
                );

                // Simulate some work by repeating the string
                let result = request.workload.repeat(request.multiplier as usize);
                let processed_length = result.len();

                info!(
                    "Worker {} completed work, result length: {}",
                    worker_rank, processed_length
                );

                let response = WorkResponse {
                    rank: worker_rank,
                    result,
                    processed_length,
                };
                Ok(response)
            },
            task_tracker.clone(),
        );
        worker_manager
            .register_handler("work".to_string(), work_handler)
            .await?;

        // Ping handler (active message - no response)
        let ping_handler = am_handler_with_tracker(
            "cohort_ping".to_string(),
            move |ctx: AmContext| async move {
                let ping_msg: String = serde_json::from_slice(&ctx.payload)
                    .map_err(|e| format!("Failed to deserialize ping message: {}", e))?;
                info!("Worker {} received ping: {}", worker_rank, ping_msg);
                Ok(())
            },
            task_tracker,
        );
        worker_manager
            .register_handler("cohort_ping".to_string(), ping_handler)
            .await?;

        let worker_client = worker_manager.client();
        println!("Worker {} listening on: {}", rank, worker_client.endpoint());

        worker_managers.push(worker_manager);
        worker_clients.push(worker_client);
    }

    // Create cohort with 3 workers
    let cohort = Arc::new(LeaderWorkerCohort::new(
        leader_client.clone(),
        CohortType::FixedSize(3),
    ));

    // Connect workers to leader using address-based discovery
    let leader_address = WorkerAddress::tcp(leader_client.endpoint().to_string());

    for worker_client in &worker_clients {
        let leader_peer = worker_client.connect_to_address(&leader_address).await?;
        debug!("Worker connected to leader: {}", leader_peer.instance_id);
    }

    // Add workers to cohort (auto-registration will handle leader-to-worker connections)
    for (rank, worker_client) in worker_clients.iter().enumerate() {
        // Add worker to cohort with its rank
        // Leader will auto-register worker endpoints when workers send messages requiring responses
        cohort
            .add_worker(worker_client.instance_id(), Some(rank))
            .await?;
    }

    // Wait for connections to establish
    tokio::time::sleep(Duration::from_millis(500)).await;

    println!("=== Cohort Parallel Ping-Pong Test ===");

    // Test 1: Parallel ping-pong with ACKs
    let ping_results = cohort
        .par_broadcast_acks("cohort_ping", "Hello from leader!", Duration::from_secs(5))
        .await?;

    println!("Ping-pong results:");
    for (worker_id, result) in ping_results {
        match result {
            Ok(()) => println!("Worker {} responded successfully", worker_id),
            Err(e) => println!("Worker {} failed: {}", worker_id, e),
        }
    }

    println!("=== Cohort Parallel Work Distribution Test ===");

    // Test 2: Parallel map with different work for each worker
    let work_results: Vec<WorkResponse> = cohort
        .par_map(
            "work",
            |rank, _worker_id| async move {
                // Create different work for each rank
                Ok(WorkRequest {
                    rank,
                    workload: format!("task-{}", rank),
                    multiplier: (rank + 1) as i32, // rank 0 gets 1x, rank 1 gets 2x, etc.
                })
            },
            Duration::from_secs(10),
        )
        .await?;

    println!("Work distribution results (in rank order):");
    for (i, response) in work_results.iter().enumerate() {
        println!(
            "Rank {}: processed '{}' -> {} chars",
            i, response.result, response.processed_length
        );
    }

    println!("=== Cohort Parallel Broadcast Test ===");

    // Test 3: Broadcast same work to all workers
    let broadcast_results: Vec<WorkResponse> = cohort
        .par_broadcast_responses(
            "work",
            WorkRequest {
                rank: 999, // Will be overridden by each worker
                workload: "broadcast-task".to_string(),
                multiplier: 3,
            },
            Duration::from_secs(10),
        )
        .await?;

    println!("Broadcast results (in rank order):");
    for (i, response) in broadcast_results.iter().enumerate() {
        println!(
            "Rank {}: worker {} processed broadcast task -> {} chars",
            i, response.rank, response.processed_length
        );
    }

    println!("=== Cohort Indexed Parallel Map Test ===");

    // Test 4: Indexed parallel map with rank and worker_id information
    let indexed_results: Vec<(usize, WorkResponse)> = cohort
        .par_map_indexed(
            "work",
            |rank, worker_id| async move {
                // Use both rank and worker_id in the work
                Ok(WorkRequest {
                    rank,
                    workload: format!("indexed-rank-{}-worker-{}", rank, worker_id),
                    multiplier: 2,
                })
            },
            Duration::from_secs(10),
        )
        .await?;

    println!("Indexed map results:");
    for (rank, response) in indexed_results {
        println!(
            "Rank {}: worker processed '{}' -> {} chars",
            rank, response.result, response.processed_length
        );
    }

    println!("=== Performance Summary ===");
    println!("Successfully demonstrated:");
    println!("  - Parallel ping-pong with ACK collection");
    println!("  - Parallel work distribution with custom payloads per worker");
    println!("  - Parallel broadcast with rank-ordered response collection");
    println!("  - Indexed parallel map with rank and worker_id access");
    println!("  - All responses returned in proper rank order");

    println!("Shutting down cohort and workers...");

    // Cleanup
    for manager in worker_managers {
        manager.shutdown().await?;
    }
    leader_manager.shutdown().await?;
    cancel_token.cancel();

    Ok(())
}
