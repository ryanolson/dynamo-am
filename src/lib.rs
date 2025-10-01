// SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

#![doc = include_str!("../docs/active_message.md")]

pub mod boxed_transport;
pub mod builder;
pub mod client;
pub mod cohort;
pub mod dispatcher;
pub mod handler;
pub mod handler_impls;
pub mod manager;
pub mod message_router;
pub mod network_client;
pub mod receipt_ack;
pub mod response;
pub mod response_manager;
pub mod responses;
pub mod status;
pub mod system_handlers;
pub mod transport;
pub(crate) mod utils;

pub use builder::MessageBuilder;
pub use client::ActiveMessageClient;
pub use cohort::{
    CohortFailurePolicy, CohortType, LeaderWorkerCohort, LeaderWorkerCohortConfig,
    LeaderWorkerCohortConfigBuilder, WorkerInfo,
};
pub use handler::ActiveMessage;
pub use handler_impls::{
    am_handler_with_tracker, typed_unary_handler, typed_unary_handler_with_tracker,
    unary_handler_with_tracker,
};
pub use manager::ActiveMessageManager;
pub use network_client::NetworkClient;
pub use receipt_ack::HandlerType; // Re-export for backward compatibility
pub use response::SingleResponseSender;
pub use response_manager::{ResponseManager, SharedResponseManager};
pub use responses::{
    DiscoverResponse, HealthCheckResponse, JoinCohortResponse, ListHandlersResponse,
    RegisterServiceResponse, RemoveServiceResponse, RequestShutdownResponse,
    WaitForHandlerResponse,
};
pub use status::{DetachedConfirm, MessageStatus, SendAndConfirm, WithResponse};
pub use system_handlers::create_core_system_handlers;
pub use transport::{ConnectionHandle, ThinTransport};

pub mod zmq;
