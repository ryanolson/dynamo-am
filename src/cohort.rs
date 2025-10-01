// SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use bytes::Bytes;
use derive_builder::Builder;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::{
    builder::MessageBuilder,
    client::ActiveMessageClient,
    handler::ActiveMessage,
    handler::InstanceId,
};

/// Cohort-specific message builder that extends MessageBuilder with rank targeting
pub struct CohortMessageBuilder<'a> {
    inner: MessageBuilder<'a>,
    cohort: &'a LeaderWorkerCohort,
}

impl<'a> CohortMessageBuilder<'a> {
    /// Set payload from serializable type
    pub fn payload<T: serde::Serialize>(mut self, data: T) -> Result<Self> {
        self.inner = self.inner.payload(data)?;
        Ok(self)
    }

    /// Set raw payload bytes
    pub fn raw_payload(mut self, data: Bytes) -> Self {
        self.inner = self.inner.raw_payload(data);
        self
    }

    /// Set timeout for response waiting
    pub fn timeout(mut self, duration: Duration) -> Self {
        self.inner = self.inner.timeout(duration);
        self
    }

    /// Target a specific worker rank
    pub async fn target_rank(self, rank: usize) -> Result<()> {
        let workers = self.cohort.state.read().await;
        let worker = workers
            .workers
            .values()
            .find(|w| w.rank == Some(rank))
            .ok_or_else(|| anyhow::anyhow!("No worker found with rank {}", rank))?;

        self.inner
            .target_instance(worker.instance_id)
            .execute()
            .await
    }

    /// Target a range of worker ranks
    pub async fn target_ranks(self, range: std::ops::Range<usize>) -> Result<()> {
        let workers = self.cohort.state.read().await;
        let mut target_workers = Vec::new();

        // Collect worker IDs first
        for rank in range {
            if let Some(worker) = workers.workers.values().find(|w| w.rank == Some(rank)) {
                target_workers.push(worker.instance_id);
            }
        }
        drop(workers);

        // Execute to each worker
        for worker_id in target_workers {
            let inner_clone = self.inner.clone_with_target(worker_id);
            inner_clone.execute().await?;
        }

        Ok(())
    }

    /// Broadcast to all workers
    pub async fn bcast(self) -> Result<()> {
        let workers = self.cohort.state.read().await;
        let worker_ids: Vec<InstanceId> = workers.workers.values().map(|w| w.instance_id).collect();
        drop(workers);

        // Execute to each worker
        for worker_id in worker_ids {
            let inner_clone = self.inner.clone_with_target(worker_id);
            inner_clone.execute().await?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum CohortType {
    FixedSize(usize), // Maximum number of workers
                      // Unbounded variant excluded per requirements
}

#[derive(Debug, Clone, PartialEq)]
pub enum CohortFailurePolicy {
    TerminateAll, // If any member fails, terminate entire cohort
                  // Other policies can be added later
}

#[derive(Debug, Clone)]
pub struct WorkerInfo {
    instance_id: InstanceId,
    rank: Option<usize>, // Worker-reported rank (e.g., from torch.distributed)
    last_heartbeat: Arc<RwLock<Instant>>,
}

impl WorkerInfo {
    pub fn new(instance_id: InstanceId, rank: Option<usize>) -> Self {
        let now = Instant::now();
        Self {
            instance_id,
            rank,
            last_heartbeat: Arc::new(RwLock::new(now)),
        }
    }
}

#[derive(Debug)]
struct CohortState {
    leader_instance: InstanceId,
    workers: HashMap<InstanceId, WorkerInfo>,
    workers_by_rank: BTreeMap<usize, InstanceId>, // For rank-ordered operations
    cohort_type: CohortType,
    failure_policy: CohortFailurePolicy,
    requires_ranks: Option<bool>, // None=unknown, Some(true)=all must have ranks, Some(false)=none have ranks
    client: Arc<dyn ActiveMessageClient>,
    cancel_token: CancellationToken,
    heartbeat_interval: Duration,
    heartbeat_timeout: Duration,
    shutdown_timeout: Duration,
}

#[derive(Builder, Debug)]
#[builder(setter(into))]
pub struct LeaderWorkerCohortConfig {
    leader_instance: InstanceId,
    client: Arc<dyn ActiveMessageClient>,
    cohort_type: CohortType,

    #[builder(default = "CohortFailurePolicy::TerminateAll")]
    failure_policy: CohortFailurePolicy,

    #[builder(default = "Duration::from_secs(30)")] // 30 second heartbeat
    heartbeat_interval: Duration,

    #[builder(default = "Duration::from_secs(120)")] // 2 minute timeout (4 missed heartbeats)
    heartbeat_timeout: Duration,

    #[builder(default = "CancellationToken::new()")]
    cancel_token: CancellationToken,

    #[builder(default = "Duration::from_secs(60)")] // 60 second timeout for graceful shutdown
    shutdown_timeout: Duration,
}

#[derive(Debug, Clone)]
pub struct LeaderWorkerCohort {
    state: Arc<RwLock<CohortState>>,
}

impl LeaderWorkerCohort {
    /// Create a new cohort with simplified API
    pub fn new(client: Arc<dyn ActiveMessageClient>, cohort_type: CohortType) -> Self {
        Self::new_with_policy(client, cohort_type, CohortFailurePolicy::TerminateAll)
    }

    /// Create a new cohort with custom failure policy
    pub fn new_with_policy(
        client: Arc<dyn ActiveMessageClient>,
        cohort_type: CohortType,
        failure_policy: CohortFailurePolicy,
    ) -> Self {
        let leader_instance = client.instance_id();
        let state = CohortState {
            leader_instance,
            workers: HashMap::new(),
            workers_by_rank: BTreeMap::new(),
            cohort_type,
            failure_policy,
            requires_ranks: None,
            client,
            cancel_token: CancellationToken::new(),
            heartbeat_interval: Duration::from_secs(30),
            heartbeat_timeout: Duration::from_secs(120),
            shutdown_timeout: Duration::from_secs(60), // Default timeout
        };

        Self {
            state: Arc::new(RwLock::new(state)),
        }
    }

    // Builder pattern constructor
    pub fn builder() -> LeaderWorkerCohortConfigBuilder {
        LeaderWorkerCohortConfigBuilder::default()
    }

    // Create from config
    pub fn from_config(config: LeaderWorkerCohortConfig) -> Self {
        let state = CohortState {
            leader_instance: config.leader_instance,
            workers: HashMap::new(),
            workers_by_rank: BTreeMap::new(),
            cohort_type: config.cohort_type,
            failure_policy: config.failure_policy,
            requires_ranks: None,
            client: config.client,
            cancel_token: config.cancel_token,
            heartbeat_interval: config.heartbeat_interval,
            heartbeat_timeout: config.heartbeat_timeout,
            shutdown_timeout: config.shutdown_timeout,
        };

        Self {
            state: Arc::new(RwLock::new(state)),
        }
    }

    // Worker management methods
    pub async fn validate_and_add_worker(
        &self,
        worker_id: InstanceId,
        rank: Option<usize>,
    ) -> Result<usize> {
        let mut state = self.state.write().await;

        // Check cohort size
        match state.cohort_type {
            CohortType::FixedSize(max) if state.workers.len() >= max => {
                return Err(anyhow::anyhow!(
                    "Cohort is full ({}/{} workers)",
                    state.workers.len(),
                    max
                ));
            }
            _ => {}
        }

        // Validate rank consistency
        match (state.requires_ranks, rank) {
            (None, Some(_)) => {
                // First worker has rank, all must have ranks
                state.requires_ranks = Some(true);
            }
            (None, None) => {
                // First worker has no rank, none should have ranks
                state.requires_ranks = Some(false);
            }
            (Some(true), None) => {
                return Err(anyhow::anyhow!(
                    "All workers must report ranks (torch.distributed or MPI)"
                ));
            }
            (Some(false), Some(_)) => {
                return Err(anyhow::anyhow!(
                    "No workers should report ranks (first worker didn't)"
                ));
            }
            (Some(true), Some(r)) => {
                // Check for rank conflicts
                if state.workers_by_rank.contains_key(&r) {
                    return Err(anyhow::anyhow!(
                        "Rank {} already taken by another worker",
                        r
                    ));
                }
                // Check rank is within expected range
                let CohortType::FixedSize(max) = state.cohort_type;
                if r >= max {
                    return Err(anyhow::anyhow!(
                        "Rank {} exceeds max cohort size {}",
                        r,
                        max
                    ));
                }
            }
            _ => {} // Valid combinations
        }

        // Add worker
        let position = state.workers.len();
        let info = WorkerInfo::new(worker_id, rank);

        if let Some(r) = rank {
            state.workers_by_rank.insert(r, worker_id);
        }
        state.workers.insert(worker_id, info);

        // Check if cohort is complete and validate rank contiguity when needed
        let CohortType::FixedSize(max_size) = state.cohort_type;
        let is_cohort_full = state.workers.len() == max_size;

        if is_cohort_full && state.requires_ranks == Some(true) {
            // Validate contiguous rank assignment now that cohort is full
            if state.workers_by_rank.len() != max_size {
                // Remove the just-added worker before returning error
                state.workers.remove(&worker_id);
                if let Some(r) = rank {
                    state.workers_by_rank.remove(&r);
                }
                return Err(anyhow::anyhow!(
                    "Incomplete rank assignment: have {} ranks, need {} for full cohort",
                    state.workers_by_rank.len(),
                    max_size
                ));
            }

            // Check all ranks [0, max_size) are present
            for expected_rank in 0..max_size {
                if !state.workers_by_rank.contains_key(&expected_rank) {
                    // Remove the just-added worker before returning error
                    state.workers.remove(&worker_id);
                    if let Some(r) = rank {
                        state.workers_by_rank.remove(&r);
                    }
                    return Err(anyhow::anyhow!(
                        "Missing rank {} in contiguous sequence [0, {}). Cannot complete cohort.",
                        expected_rank,
                        max_size
                    ));
                }
            }

            info!("Cohort is complete with contiguous ranks [0, {})", max_size);
        }

        info!(
            "Added worker {} to cohort (rank: {:?}, position: {})",
            worker_id, rank, position
        );

        Ok(position)
    }

    // Add worker (wrapper for validate_and_add_worker)
    pub async fn add_worker(&self, worker_id: InstanceId, rank: Option<usize>) -> Result<usize> {
        self.validate_and_add_worker(worker_id, rank).await
    }

    /// Register cohort-specific handlers (_join_cohort) with the dispatcher
    /// This should only be called by leaders that manage cohorts
    pub async fn register_handlers(
        &self,
        control_tx: &tokio::sync::mpsc::Sender<crate::dispatcher::ControlMessage>,
        task_tracker: tokio_util::task::TaskTracker,
    ) -> Result<()> {
        use crate::handler_impls::{
            TypedContext, typed_unary_handler_with_tracker,
        };
        use crate::responses::JoinCohortResponse;

        let cohort = self.clone();

        // Create handler that captures THIS cohort instance
        let handler = typed_unary_handler_with_tracker(
            "_join_cohort".to_string(),
            move |ctx: TypedContext<serde_json::Value>| {
                let cohort = cohort.clone();
                // Use block_in_place for async work in sync handler
                tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current().block_on(async move {
                        // Parse rank from request
                        let rank = ctx
                            .input
                            .get("rank")
                            .and_then(|v| v.as_u64())
                            .map(|r| r as usize);

                        // Actually add worker to THIS specific cohort
                        match cohort.add_worker(ctx.sender_id, rank).await {
                            Ok(position) => {
                                info!(
                                    "Worker {} joined cohort at position {} with rank {:?}",
                                    ctx.sender_id, position, rank
                                );
                                Ok(JoinCohortResponse {
                                    accepted: true,
                                    position: Some(position),
                                    expected_rank: rank,
                                    reason: None,
                                })
                            }
                            Err(e) => {
                                warn!("Worker {} failed to join cohort: {}", ctx.sender_id, e);
                                Ok(JoinCohortResponse {
                                    accepted: false,
                                    position: None,
                                    expected_rank: None,
                                    reason: Some(e.to_string()),
                                })
                            }
                        }
                    })
                })
            },
            task_tracker,
        );

        // Register with dispatcher
        control_tx
            .send(
                crate::dispatcher::ControlMessage::Register {
                    name: "_join_cohort".to_string(),
                    dispatcher: handler,
                },
            )
            .await
            .map_err(|e| anyhow::anyhow!("Failed to register _join_cohort handler: {}", e))?;

        Ok(())
    }

    /// Connect to a worker and add it to the cohort
    /// This is a convenience method for leader-initiated cohort building
    pub async fn connect_and_add_worker(
        &self,
        address: &crate::client::WorkerAddress,
        rank: Option<usize>,
    ) -> Result<usize> {
        // Connect to worker
        let state = self.state.read().await;
        let client = state.client.clone();
        drop(state);

        let peer_info = client.connect_to_address(address).await?;

        // Add to cohort
        self.add_worker(peer_info.instance_id, rank).await
    }

    // Remove worker (called by RemoveServiceHandler)
    pub async fn remove_worker(&self, worker_id: InstanceId) -> Result<bool> {
        let mut state = self.state.write().await;

        if let Some(info) = state.workers.remove(&worker_id) {
            if let Some(rank) = info.rank {
                state.workers_by_rank.remove(&rank);
            }
            info!(
                "Removed worker {} from cohort (rank: {:?})",
                worker_id, info.rank
            );
            Ok(true)
        } else {
            Ok(false)
        }
    }

    // Check if cohort is full
    pub async fn is_full(&self) -> bool {
        let state = self.state.read().await;
        match state.cohort_type {
            CohortType::FixedSize(max) => state.workers.len() >= max,
        }
    }

    // Get worker count
    pub async fn worker_count(&self) -> usize {
        let state = self.state.read().await;
        state.workers.len()
    }

    // Check if cohort uses ranks
    pub async fn has_ranks(&self) -> bool {
        let state = self.state.read().await;
        state.requires_ranks == Some(true)
    }

    // Get workers in rank order (if ranks are used)
    pub async fn get_workers_by_rank(&self) -> Vec<(usize, InstanceId)> {
        let state = self.state.read().await;
        state
            .workers_by_rank
            .iter()
            .map(|(rank, id)| (*rank, *id))
            .collect()
    }

    // Get all workers as list of IDs
    pub async fn get_worker_ids(&self) -> Vec<InstanceId> {
        let state = self.state.read().await;
        state.workers.keys().cloned().collect()
    }

    // Update worker heartbeat
    pub async fn update_worker_heartbeat(&self, worker_id: InstanceId) -> Result<()> {
        let state = self.state.read().await;
        if let Some(info) = state.workers.get(&worker_id) {
            *info.last_heartbeat.write().await = Instant::now();
            Ok(())
        } else {
            Err(anyhow::anyhow!("Worker {} not found in cohort", worker_id))
        }
    }

    // Check if all ranks form contiguous sequence [0, N) when ranks are required
    pub async fn validate_ranks_complete(&self) -> Result<()> {
        let state = self.state.read().await;

        // Only validate if ranks are required
        if state.requires_ranks != Some(true) {
            return Ok(());
        }

        let CohortType::FixedSize(max_size) = state.cohort_type;

        // Check that we have exactly the right number of ranks
        if state.workers_by_rank.len() != max_size {
            return Err(anyhow::anyhow!(
                "Incomplete rank assignment: have {} ranks, need {}",
                state.workers_by_rank.len(),
                max_size
            ));
        }

        // Check that all ranks [0, max_size) are present
        for expected_rank in 0..max_size {
            if !state.workers_by_rank.contains_key(&expected_rank) {
                return Err(anyhow::anyhow!(
                    "Missing rank {} in contiguous sequence [0, {})",
                    expected_rank,
                    max_size
                ));
            }
        }

        Ok(())
    }

    // Get missing ranks in the sequence [0, N)
    pub async fn get_missing_ranks(&self) -> Vec<usize> {
        let state = self.state.read().await;

        // Only applies when ranks are required
        if state.requires_ranks != Some(true) {
            return vec![];
        }

        let CohortType::FixedSize(max_size) = state.cohort_type;
        let mut missing = Vec::new();

        for expected_rank in 0..max_size {
            if !state.workers_by_rank.contains_key(&expected_rank) {
                missing.push(expected_rank);
            }
        }

        missing
    }

    // Check if cohort is complete (size + ranks if required)
    pub async fn is_cohort_complete(&self) -> bool {
        let state = self.state.read().await;
        let CohortType::FixedSize(max_size) = state.cohort_type;

        // Must have correct number of workers
        if state.workers.len() != max_size {
            return false;
        }

        // If ranks are required, must have contiguous sequence
        if state.requires_ranks == Some(true) {
            if state.workers_by_rank.len() != max_size {
                return false;
            }

            // Check all ranks [0, max_size) are present
            for expected_rank in 0..max_size {
                if !state.workers_by_rank.contains_key(&expected_rank) {
                    return false;
                }
            }
        }

        true
    }

    pub async fn await_handler_on_all_workers(
        &self,
        handler: &str,
        timeout: Option<Duration>,
    ) -> Result<()> {
        let state = self.state.read().await;
        let workers: Vec<InstanceId> = state.workers.keys().cloned().collect();
        let client = state.client.clone();
        drop(state);

        let mut tasks = Vec::new();

        for worker_id in workers {
            let client = client.clone();
            let handler = handler.to_string();

            tasks.push(tokio::spawn(async move {
                client.await_handler(worker_id, &handler, timeout).await
            }));
        }

        for (idx, task) in tasks.into_iter().enumerate() {
            match task.await {
                Ok(Ok(true)) => {
                    debug!("Worker {} has handler '{}'", idx, handler);
                }
                Ok(Ok(false)) => {
                    warn!("Worker {} does not have handler '{}'", idx, handler);
                }
                Ok(Err(e)) => {
                    warn!(
                        "Worker {} failed to confirm handler '{}': {}",
                        idx, handler, e
                    );
                    return Err(e);
                }
                Err(e) => {
                    warn!("Worker {} task panicked: {}", idx, e);
                    return Err(anyhow::anyhow!("Worker task panicked: {}", e));
                }
            }
        }

        Ok(())
    }

    pub async fn broadcast_to_workers(&self, handler: &str, payload: Bytes) -> Result<()> {
        let state = self.state.read().await;
        let workers: Vec<InstanceId> = state.workers.keys().cloned().collect();
        let client = state.client.clone();
        drop(state);

        for worker_id in workers {
            if let Err(e) = client
                .send_message(worker_id, handler, payload.clone())
                .await
            {
                warn!("Failed to send to worker {}: {}", worker_id, e);
            }
        }

        Ok(())
    }

    // Broadcast to workers in rank order
    pub async fn broadcast_by_rank(&self, handler: &str, payload: Bytes) -> Result<()> {
        let workers = self.get_workers_by_rank().await;
        let state = self.state.read().await;
        let client = state.client.clone();
        drop(state);

        for (rank, worker_id) in workers {
            debug!("Sending to worker rank {}: {}", rank, worker_id);
            if let Err(e) = client
                .send_message(worker_id, handler, payload.clone())
                .await
            {
                warn!(
                    "Failed to send to worker {} (rank {}): {}",
                    worker_id, rank, e
                );
            }
        }

        Ok(())
    }

    pub async fn broadcast_to_workers_with_acks(
        &self,
        handler: &str,
        payload: Bytes,
        timeout: Duration,
    ) -> Result<()> {
        let state = self.state.read().await;
        let workers: Vec<InstanceId> = state.workers.keys().cloned().collect();
        let client = state.client.clone();
        drop(state);

        let mut ack_receivers = Vec::new();

        for worker_id in workers {
            let ack_id = Uuid::new_v4();
            let ack_rx = client.register_ack(ack_id, timeout).await?;
            ack_receivers.push((worker_id, ack_rx));

            let mut payload_with_ack = serde_json::from_slice::<serde_json::Value>(&payload)?;
            if let serde_json::Value::Object(ref mut map) = payload_with_ack {
                map.insert(
                    "_ack_id".to_string(),
                    serde_json::Value::String(ack_id.to_string()),
                );
            } else {
                anyhow::bail!(
                    "broadcast_acks requires JSON object payload, got: {}",
                    payload_with_ack.to_string()
                );
            }

            let modified_payload = Bytes::from(serde_json::to_vec(&payload_with_ack)?);
            client
                .send_message(worker_id, handler, modified_payload)
                .await?;
        }

        for (worker_id, ack_rx) in ack_receivers {
            match tokio::time::timeout(timeout, ack_rx).await {
                Ok(Ok(Ok(()))) => {
                    debug!("Received ACK from worker {}", worker_id);
                }
                Ok(Ok(Err(error))) => {
                    warn!("Received NACK from worker {}: {}", worker_id, error);
                }
                Ok(Err(_)) => {
                    warn!("ACK channel closed for worker {}", worker_id);
                }
                Err(_) => {
                    warn!("Timeout waiting for ACK from worker {}", worker_id);
                }
            }
        }

        Ok(())
    }

    /// Rayon-style parallel map over workers with rank-ordered results
    /// Sends a message to all workers and collects their responses in rank order
    ///
    /// TODO: This method requires object-safe message building, currently disabled
    #[allow(dead_code)]
    pub async fn par_map<T, F, Fut, R>(
        &self,
        _handler: &str,
        _mapper: F,
        _timeout: Duration,
    ) -> Result<Vec<R>>
    where
        T: Serialize + Clone + Send + 'static,
        R: for<'a> Deserialize<'a> + Send + 'static,
        F: Fn(usize, InstanceId) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<T>> + Send,
    {
        Err(anyhow::anyhow!(
            "par_map is temporarily disabled due to trait object limitations"
        ))
        /*
         */
    }

    /// Rayon-style parallel broadcast with ACK collection in rank order
    /// Returns a Map of worker_id -> Result for all workers
    ///
    /// Broadcast a message to all workers and collect acknowledgments
    pub async fn par_broadcast_acks<T>(
        &self,
        handler: &str,
        payload: T,
        timeout: Duration,
    ) -> Result<HashMap<InstanceId, Result<(), String>>>
    where
        T: Serialize + Clone + Send + 'static,
    {
        let workers = self.get_workers_by_rank().await;
        let state = self.state.read().await;
        let client = state.client.clone();
        drop(state);

        let mut tasks = Vec::new();

        // Serialize payload once
        let payload_bytes = Bytes::from(serde_json::to_vec(&payload)?);

        // Spawn tasks for each worker in parallel
        for (_rank, worker_id) in workers {
            let client = client.clone();
            let handler = handler.to_string();
            let payload_bytes = payload_bytes.clone();

            let task = tokio::spawn(async move {
                // Create message directly instead of using builder
                let message = ActiveMessage {
                    message_id: Uuid::new_v4(),
                    handler_name: handler,
                    sender_instance: client.instance_id(),
                    payload: payload_bytes,
                    metadata: serde_json::Value::Object(serde_json::Map::new()),
                };

                let result = client.send_raw_message(worker_id, message).await;

                let worker_result = match result {
                    Ok(_) => Ok(()),
                    Err(e) => Err(e.to_string()),
                };

                Ok::<(InstanceId, Result<(), String>), anyhow::Error>((worker_id, worker_result))
            });

            tasks.push(task);
        }

        // Collect results
        let mut results = HashMap::new();
        for task in tasks {
            match tokio::time::timeout(timeout, task).await {
                Ok(Ok(Ok((worker_id, worker_result)))) => {
                    results.insert(worker_id, worker_result);
                }
                Ok(Ok(Err(e))) => {
                    warn!("Worker communication failed: {}", e);
                }
                Ok(Err(e)) => {
                    warn!("Worker task panicked: {}", e);
                }
                Err(_) => {
                    warn!("Timeout waiting for worker");
                }
            }
        }

        Ok(results)
    }

    /// Rayon-style parallel broadcast with response collection in rank order
    /// Returns responses in rank order as Vec<T>
    pub async fn par_broadcast_responses<P, R>(
        &self,
        handler: &str,
        payload: P,
        timeout: Duration,
    ) -> Result<Vec<R>>
    where
        P: Serialize + Clone + Send + 'static,
        R: for<'a> Deserialize<'a> + Send + 'static,
    {
        let workers = self.get_workers_by_rank().await;
        let state = self.state.read().await;
        let client = state.client.clone();
        drop(state);

        let mut tasks = Vec::new();

        // Serialize payload once
        let payload_bytes = Bytes::from(serde_json::to_vec(&payload)?);

        // Spawn tasks for each worker in parallel
        for (rank, worker_id) in workers {
            let client = client.clone();
            let handler = handler.to_string();
            let payload_bytes = payload_bytes.clone();

            let task = tokio::spawn(async move {
                // Create a response channel
                let (tx, rx) = tokio::sync::oneshot::channel();
                let message_id = Uuid::new_v4();

                // Register for response
                client.register_response(message_id, tx).await?;

                // Create message with response expectation
                let mut metadata = serde_json::Map::new();
                metadata.insert(
                    "_expect_response".to_string(),
                    serde_json::Value::Bool(true),
                );

                let message = ActiveMessage {
                    message_id,
                    handler_name: handler,
                    sender_instance: client.instance_id(),
                    payload: payload_bytes,
                    metadata: serde_json::Value::Object(metadata),
                };

                // Send message
                client.send_raw_message(worker_id, message).await?;

                // Await response
                let response_bytes = rx
                    .await
                    .map_err(|_| anyhow::anyhow!("Response channel closed"))?;
                let response: R = serde_json::from_slice(&response_bytes)?;

                Ok::<(usize, R), anyhow::Error>((rank, response))
            });

            tasks.push(task);
        }

        // Collect results and sort by rank
        let mut results = Vec::new();
        for task in tasks {
            match tokio::time::timeout(timeout, task).await {
                Ok(Ok(Ok((rank, response)))) => {
                    results.push((rank, response));
                }
                Ok(Ok(Err(e))) => {
                    warn!("Worker task failed: {}", e);
                    // Continue collecting other results rather than failing entirely
                }
                Ok(Err(e)) => {
                    warn!("Worker task panicked: {}", e);
                }
                Err(_) => {
                    warn!("Timeout waiting for worker task");
                }
            }
        }

        // Sort by rank and extract responses
        results.sort_by_key(|(rank, _)| *rank);
        Ok(results.into_iter().map(|(_, response)| response).collect())
    }

    /// Indexed version of par_map that provides both rank and worker_id to the mapper
    pub async fn par_map_indexed<T, F, Fut, R>(
        &self,
        handler: &str,
        mapper: F,
        timeout: Duration,
    ) -> Result<Vec<(usize, R)>>
    where
        T: Serialize + Clone + Send + 'static,
        R: for<'a> Deserialize<'a> + Send + 'static,
        F: Fn(usize, InstanceId) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<T>> + Send,
    {
        let workers = self.get_workers_by_rank().await;
        let state = self.state.read().await;
        let client = state.client.clone();
        drop(state);

        let mut tasks = Vec::new();
        let mapper = Arc::new(mapper);

        // Spawn tasks for each worker in parallel
        for (rank, worker_id) in workers {
            let client = client.clone();
            let handler = handler.to_string();
            let mapper = mapper.clone();

            let task = tokio::spawn(async move {
                // Call mapper to get the payload for this worker
                let payload = mapper(rank, worker_id).await?;

                // Serialize payload
                let payload_bytes = Bytes::from(serde_json::to_vec(&payload)?);

                // Create a response channel
                let (tx, rx) = tokio::sync::oneshot::channel();
                let message_id = Uuid::new_v4();

                // Register for response
                client.register_response(message_id, tx).await?;

                // Create message with response expectation
                let mut metadata = serde_json::Map::new();
                metadata.insert(
                    "_expect_response".to_string(),
                    serde_json::Value::Bool(true),
                );

                let message = ActiveMessage {
                    message_id,
                    handler_name: handler,
                    sender_instance: client.instance_id(),
                    payload: payload_bytes,
                    metadata: serde_json::Value::Object(metadata),
                };

                // Send message
                client.send_raw_message(worker_id, message).await?;

                // Await response
                let response_bytes = rx
                    .await
                    .map_err(|_| anyhow::anyhow!("Response channel closed"))?;
                let response: R = serde_json::from_slice(&response_bytes)?;

                Ok::<(usize, R), anyhow::Error>((rank, response))
            });

            tasks.push(task);
        }

        // Collect results and sort by rank
        let mut results = Vec::new();
        for task in tasks {
            match tokio::time::timeout(timeout, task).await {
                Ok(Ok(Ok((rank, response)))) => {
                    results.push((rank, response));
                }
                Ok(Ok(Err(e))) => {
                    return Err(anyhow::anyhow!("Worker task failed: {}", e));
                }
                Ok(Err(e)) => {
                    return Err(anyhow::anyhow!("Worker task panicked: {}", e));
                }
                Err(_) => {
                    return Err(anyhow::anyhow!("Timeout waiting for worker response"));
                }
            }
        }

        // Sort by rank
        results.sort_by_key(|(rank, _)| *rank);
        Ok(results)
    }

    pub async fn list_workers(&self) -> Vec<InstanceId> {
        let state = self.state.read().await;
        state.workers.keys().cloned().collect()
    }

    pub fn leader_instance(&self) -> InstanceId {
        let state = self.state.blocking_read();
        state.leader_instance
    }

    // Start monitoring (called after creation)
    pub async fn start_monitoring(self: Arc<Self>) {
        tokio::spawn(self.clone().heartbeat_monitor_loop());
    }

    // Heartbeat monitoring loop for leader
    async fn heartbeat_monitor_loop(self: Arc<Self>) {
        let state = self.state.read().await;
        let heartbeat_interval = state.heartbeat_interval;
        let heartbeat_timeout = state.heartbeat_timeout;
        let cancel_token = state.cancel_token.clone();
        drop(state);

        let mut interval = tokio::time::interval(heartbeat_interval); // 30 seconds

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let now = Instant::now();
                    let mut failed_workers = Vec::new();

                    // Check all worker heartbeats
                    let state = self.state.read().await;
                    for (worker_id, info) in &state.workers {
                        let last_hb = *info.last_heartbeat.read().await;

                        if now.duration_since(last_hb) > heartbeat_timeout { // 2 minutes
                            warn!("Worker {} (rank: {:?}) missed 4 heartbeats, marking as failed",
                                  worker_id, info.rank);
                            failed_workers.push(*worker_id);
                        } else {
                            // Send health check
                            tokio::spawn({
                                let client = state.client.clone();
                                let worker_id = *worker_id;
                                let last_heartbeat = info.last_heartbeat.clone();
                                async move {
                                    // Send health check message directly
                                    let message = ActiveMessage {
                                        message_id: Uuid::new_v4(),
                                        handler_name: "_health_check".to_string(),
                                        sender_instance: client.instance_id(),
                                        payload: Bytes::new(),
                                        metadata: serde_json::json!({"_expect_response": true}),
                                    };

                                    if client.send_raw_message(worker_id, message).await.is_ok() {
                                        *last_heartbeat.write().await = Instant::now();
                                    }
                                }
                            });
                        }
                    }
                    drop(state);

                    // Handle failures
                    if let Some(failed_worker) = failed_workers.into_iter().next() {
                        self.handle_worker_failure(failed_worker).await;
                        return; // Exit loop after handling failure
                    }
                }
                _ = cancel_token.cancelled() => {
                    info!("Heartbeat monitor cancelled");
                    break;
                }
            }
        }
    }

    // Handle worker failure
    async fn handle_worker_failure(&self, failed_worker: InstanceId) {
        let state = self.state.read().await;
        let failure_policy = state.failure_policy.clone();
        drop(state);

        match failure_policy {
            CohortFailurePolicy::TerminateAll => {
                error!(
                    "Worker {} failed, initiating cohort termination",
                    failed_worker
                );
                // Initiate graceful shutdown of all workers
                if let Err(e) = self.initiate_graceful_shutdown().await {
                    error!("Failed to initiate graceful shutdown: {}", e);
                }
                // Cancel own token
                let state = self.state.read().await;
                state.cancel_token.cancel();
            }
        }
    }

    // Graceful shutdown flow
    pub async fn initiate_graceful_shutdown(&self) -> Result<()> {
        info!("Initiating graceful cohort shutdown");

        let state = self.state.read().await;
        let client = state.client.clone();
        let shutdown_timeout = state.shutdown_timeout;

        // Send shutdown requests in rank order (if ranks exist)
        let workers = if state.requires_ranks == Some(true) {
            drop(state);
            self.get_workers_by_rank()
                .await
                .into_iter()
                .map(|(_, id)| id)
                .collect()
        } else {
            let workers: Vec<InstanceId> = state.workers.keys().cloned().collect();
            drop(state);
            workers
        };

        // 1. Send _request_shutdown to all workers
        for worker_id in &workers {
            debug!("Sending shutdown request to worker {}", worker_id);
            // Send shutdown message directly
            let message = ActiveMessage {
                message_id: Uuid::new_v4(),
                handler_name: "_request_shutdown".to_string(),
                sender_instance: client.instance_id(),
                payload: Bytes::new(),
                metadata: serde_json::Value::Object(serde_json::Map::new()),
            };

            if let Err(e) = client.send_raw_message(*worker_id, message).await {
                warn!(
                    "Failed to send shutdown request to worker {}: {}",
                    worker_id, e
                );
            }
        }

        // 2. Wait for all workers to remove their service
        let deadline = Instant::now() + shutdown_timeout;

        while !self.state.read().await.workers.is_empty() && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        let remaining = self.state.read().await.workers.len();
        if remaining > 0 {
            warn!(
                "{} workers did not shutdown gracefully within timeout",
                remaining
            );
        } else {
            info!("All workers shutdown gracefully");
        }

        Ok(())
    }
}

// Worker-side heartbeat monitoring function (to be called by worker)
pub async fn monitor_leader_heartbeat(
    leader_id: InstanceId,
    client: Arc<dyn ActiveMessageClient>,
    cancel_token: CancellationToken,
    worker_rank: Option<usize>,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(30));
    let mut consecutive_failures = 0;

    loop {
        tokio::select! {
            _ = interval.tick() => {
                match client.send_message(leader_id, "_health_check", Bytes::new()).await {
                    Ok(_) => {
                        debug!("Leader heartbeat successful (rank: {:?})", worker_rank);
                        consecutive_failures = 0;
                    }
                    Err(e) => {
                        consecutive_failures += 1;
                        warn!("Leader heartbeat failed ({}/4): {}", consecutive_failures, e);

                        if consecutive_failures >= 4 {
                            error!("Leader failed after 4 heartbeat attempts (2 minutes), shutting down");
                            cancel_token.cancel();
                            return;
                        }
                    }
                }
            }
            _ = cancel_token.cancelled() => {
                info!("Leader monitor cancelled");
                break;
            }
        }
    }
}
