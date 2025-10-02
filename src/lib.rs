// SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

#![doc = include_str!("../docs/active_message.md")]

pub mod api;
pub mod protocol;
pub mod runtime;
pub mod transport;

pub use api::{
    builder::{MessageBuilder, NeedsDeliveryMode},
    client::{ActiveMessageClient, IntoPayload, PeerInfo, WorkerAddress},
    handler::{ActiveMessageContext, HandlerEvent},
    status::{DetachedConfirm, MessageStatus, SendAndConfirm, WithResponse},
};
pub use protocol::{
    message::{ActiveMessage, HandlerId, InstanceId},
    receipt::{ClientExpectation, HandlerType, ReceiptAck, ReceiptStatus},
    response::{ResponseEnvelope, SingleResponseSender},
    responses::{
        DiscoverResponse, HealthCheckResponse, ListHandlersResponse, RegisterServiceResponse,
        RemoveServiceResponse, RequestShutdownResponse, WaitForHandlerResponse,
    },
};
pub use runtime::{
    cohort::{
        CohortFailurePolicy, CohortType, LeaderWorkerCohort, LeaderWorkerCohortConfig,
        LeaderWorkerCohortConfigBuilder, WorkerInfo,
    },
    handler_impls::{
        am_handler_with_tracker, typed_unary_handler, typed_unary_handler_with_tracker,
        unary_handler_with_tracker,
    },
    manager::ActiveMessageManager,
    manager_builder::ActiveMessageManagerBuilder,
    network_client::NetworkClient,
    response_manager::{ResponseManager, SharedResponseManager},
    system_handlers::create_core_system_handlers,
};
pub use transport::{ConnectionHandle, ThinTransport, TransportType};

// Backward-compatible module aliases to ease migration.
pub use api::builder;
pub use api::client;
pub use api::handler;
pub use api::status;
pub use protocol::receipt as receipt_ack;
pub use protocol::response;
pub use protocol::responses;
pub use runtime::cohort;
pub use runtime::dispatcher;
pub use runtime::handler_impls;
pub use runtime::manager;
pub use runtime::manager_builder;
pub use runtime::message_router;
pub use runtime::network_client;
pub use runtime::response_manager;
pub use runtime::system_handlers;
pub use transport::boxed as boxed_transport;

// ZMQ transport implementation - public but hidden from documentation.
#[doc(hidden)]
pub mod zmq {
    pub use crate::transport::zmq::*;
}
