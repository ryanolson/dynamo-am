// SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::broadcast;

use super::client::ActiveMessageClient;
use super::handler::{HandlerEvent, HandlerId};

#[async_trait]
pub trait ActiveMessageManager: Send + Sync {
    fn client(&self) -> Arc<dyn ActiveMessageClient>;

    async fn deregister_handler(&self, name: &str) -> Result<()>;

    async fn list_handlers(&self) -> Vec<HandlerId>;

    fn handler_events(&self) -> broadcast::Receiver<HandlerEvent>;

    async fn shutdown(&self) -> Result<()>;
}
