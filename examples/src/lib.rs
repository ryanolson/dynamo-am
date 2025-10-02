// SPDX-FileCopyrightText: Copyright (c) 2024-2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use dynamo_runtime::active_message::{
    dispatcher::ActiveMessageDispatcher,
    handler_impls::{typed_unary_handler, TypedContext},
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::info;

#[derive(Debug, Serialize, Deserialize)]
pub struct ComputeRequest {
    pub x: i32,
    pub y: i32,
    pub operation: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ComputeResponse {
    pub result: i32,
}

/// Create a compute handler using the new v2 pattern
pub fn create_compute_handler() -> Arc<dyn ActiveMessageDispatcher> {
    typed_unary_handler(
        "compute".to_string(),
        |ctx: TypedContext<ComputeRequest>| {
            let request = ctx.input;

            let result = match request.operation.as_str() {
                "add" => request.x + request.y,
                "multiply" => request.x * request.y,
                _ => {
                    info!("Unknown operation: {}", request.operation);
                    return Err(format!("Unknown operation: {}", request.operation));
                }
            };

            info!(
                "Computed {} {} {} = {}",
                request.x, request.operation, request.y, result
            );

            let response = ComputeResponse { result };
            Ok(response)
        },
    )
}
