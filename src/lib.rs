// SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

#![cfg_attr(not(doctest), doc = include_str!("../docs/active_message.md"))]

pub mod api;
pub mod protocols;
pub mod runtime;
pub mod transport;
pub mod zmq;

// Public API exports
pub use api::{
    builder::MessageBuilder,
    client::{ActiveMessageClient, PeerInfo, WorkerAddress},
    control::{
        AcceptanceMetadata, AckMetadata, ControlMetadata, DeliveryMode, ReceiptMetadata,
        ResponseContextMetadata, ResponseMetadata, ResponseType, TransportHints,
    },
    handler::{ActiveMessageContext, HandlerEvent, InstanceId},
    message::ActiveMessage,
    response_anchor::{ResponseAnchorHandle, ResponseAnchorSource, ResponseSink},
    status::{DetachedConfirm, MessageStatus, SendAndConfirm, WithResponse},
};

pub use protocols::{
    receipt::{ClientExpectation, ContractInfo, HandlerType, ReceiptAck, ReceiptStatus},
    response::SingleResponseSender,
    responses::{
        DiscoverResponse, HealthCheckResponse, JoinCohortResponse, ListHandlersResponse,
        RegisterServiceResponse, RemoveServiceResponse, RequestShutdownResponse,
        WaitForHandlerResponse,
    },
};

pub use runtime::{
    anchor_manager::ResponseAnchorStream,
    cohort::{
        CohortFailurePolicy, CohortType, LeaderWorkerCohort, LeaderWorkerCohortConfig,
        LeaderWorkerCohortConfigBuilder, WorkerInfo,
    },
    handler_impls::{
        am_handler_with_tracker, typed_unary_handler, typed_unary_handler_with_tracker,
        unary_handler_with_tracker,
    },
    manager::ActiveMessageManager,
    network_client::NetworkClient,
    response_manager::{ResponseManager, SharedResponseManager},
    system_handlers::create_core_system_handlers,
};

pub use transport::{ConnectionHandle, ThinTransport, TransportType};

// Compatibility re-exports (legacy module paths)
pub use api::builder;
pub use api::client;
pub use api::handler;
pub use api::status;
pub use api::utils;
pub use protocols::receipt as receipt_ack;
pub use protocols::response;
pub use protocols::responses;
pub use runtime::cohort;
pub use runtime::dispatcher;
pub use runtime::handler_impls;
pub use runtime::manager;
pub use runtime::message_router;
pub use runtime::network_client;
pub use runtime::response_manager;
pub use runtime::system_handlers;
