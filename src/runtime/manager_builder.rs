// SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Builder pattern for creating ActiveMessageManager instances.

use anyhow::Result;
use tokio_util::sync::CancellationToken;

use crate::runtime::manager::ActiveMessageManager;
use crate::transport::zmq::ZmqActiveMessageManager;

/// Builder for creating ActiveMessageManager instances.
///
/// # Example
///
/// ```no_run
/// use dynamo_am::ActiveMessageManagerBuilder;
///
/// # async fn example() -> anyhow::Result<()> {
/// let manager = ActiveMessageManagerBuilder::new()
///     .endpoint("tcp://0.0.0.0:5555".to_string())
///     .build()
///     .await?;
/// # Ok(())
/// # }
/// ```
pub struct ActiveMessageManagerBuilder {
    endpoint: Option<String>,
    cancel_token: Option<CancellationToken>,
}

impl ActiveMessageManagerBuilder {
    /// Create a new builder with default settings.
    pub fn new() -> Self {
        Self {
            endpoint: None,
            cancel_token: None,
        }
    }

    /// Set the endpoint to bind to.
    ///
    /// Examples:
    /// - `"tcp://0.0.0.0:5555"` - Bind to all interfaces on port 5555
    /// - `"tcp://127.0.0.1:0"` - Bind to localhost with random port
    /// - `"ipc:///tmp/my-socket.ipc"` - Bind to IPC socket
    pub fn endpoint(mut self, endpoint: String) -> Self {
        self.endpoint = Some(endpoint);
        self
    }

    /// Set the cancellation token for graceful shutdown.
    ///
    /// If not provided, a fresh `CancellationToken` is created internally
    /// and can be retrieved later via `ActiveMessageManager::cancel_token()`.
    pub fn cancel_token(mut self, cancel_token: CancellationToken) -> Self {
        self.cancel_token = Some(cancel_token);
        self
    }

    /// Build the ActiveMessageManager.
    ///
    /// This currently creates a ZMQ-based transport. In the future, this
    /// will support multiple transport types.
    pub async fn build(self) -> Result<ActiveMessageManager> {
        let endpoint = self
            .endpoint
            .ok_or_else(|| anyhow::anyhow!("endpoint is required"))?;
        let cancel_token = self.cancel_token.unwrap_or_else(CancellationToken::new);

        // For now, we default to ZMQ transport
        // In the future, this could be configurable
        let zmq_manager = ZmqActiveMessageManager::new(endpoint, cancel_token).await?;

        Ok(ActiveMessageManager::new(Box::new(zmq_manager)))
    }
}

impl Default for ActiveMessageManagerBuilder {
    fn default() -> Self {
        Self::new()
    }
}
