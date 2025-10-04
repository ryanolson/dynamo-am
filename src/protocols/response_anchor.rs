// SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Response Anchor Protocol Types
//!
//! This module defines the wire protocol for response anchors, including:
//! - Control messages (attach, detach, finalize, cancel) sent via ActiveMessage
//! - Data frames (items, sentinels) sent via streaming transport
//! - Handles for anchor identification and routing

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::api::client::WorkerAddress;
use crate::api::handler::InstanceId;

/// Serializable handle to a response anchor
///
/// This handle can be sent across the network to allow remote attachment.
/// It contains all information needed to:
/// - Identify the anchor (anchor_id)
/// - Locate the holder (instance_id, worker_address)
/// - Establish control and data connections
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResponseAnchorHandle {
    /// Unique identifier for this anchor
    pub anchor_id: Uuid,

    /// Instance ID of the anchor holder
    pub instance_id: InstanceId,

    /// Address to reach the anchor holder
    pub worker_address: WorkerAddress,
}

impl ResponseAnchorHandle {
    /// Create a new response anchor handle
    pub fn new(anchor_id: Uuid, instance_id: InstanceId, worker_address: WorkerAddress) -> Self {
        Self {
            anchor_id,
            instance_id,
            worker_address,
        }
    }
}

/// Frame sent over the streaming transport
///
/// Sentinel frames (Detached, Finalized) ensure ordering: all data items
/// from an attachment session are processed before the state transition occurs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StreamFrame<T> {
    /// Prologue frame sent immediately after successful attachment
    ///
    /// This frame contains metadata about the attached source and enables
    /// the anchor to send cancellation signals back to the source.
    Prologue {
        /// Instance ID of the source that attached
        source_instance_id: InstanceId,
        /// Cancellation ID registered by the source
        cancellation_id: Uuid,
        /// Endpoint where source can be reached for cancellation
        source_endpoint: String,
    },

    /// Data item or application-level error
    ///
    /// Application errors are part of the stream semantics - they represent
    /// failures in processing individual items, not transport failures.
    Item(Result<T, String>),

    /// Sentinel: Source detached, anchor can accept new attachment
    ///
    /// This frame is sent after all Item frames from the current session.
    /// The anchor processes this in-order, ensuring all data is consumed
    /// before allowing reattachment.
    Detached,

    /// Sentinel: Stream finalized, no more attachments allowed
    ///
    /// After processing this frame, the anchor closes the user stream
    /// and removes itself from the registry.
    Finalized,

    /// Fatal error (transport/serialization), stream terminates
    ///
    /// This is sent when an unrecoverable error occurs (connection failure,
    /// serialization error, protocol violation). The stream will emit this
    /// error to the user and then close (return None).
    TransportError(String),
}

impl<T> StreamFrame<T> {
    /// Create an item frame with a successful result
    pub fn ok(value: T) -> Self {
        Self::Item(Ok(value))
    }

    /// Create an item frame with an error
    pub fn err(error: impl ToString) -> Self {
        Self::Item(Err(error.to_string()))
    }

    /// Create a transport error frame
    pub fn transport_error(error: impl ToString) -> Self {
        Self::TransportError(error.to_string())
    }

    /// Check if this is a terminal frame (Finalized or TransportError)
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Finalized | Self::TransportError(_))
    }

    /// Check if this is a control frame (not user data)
    pub fn is_control(&self) -> bool {
        matches!(
            self,
            Self::Prologue { .. } | Self::Detached | Self::Finalized | Self::TransportError(_)
        )
    }
}

// ============================================================================
// Control Messages (sent via ActiveMessage)
// ============================================================================

/// Request to attach to a response anchor
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnchorAttachRequest {
    /// ID of the anchor to attach to
    pub anchor_id: Uuid,

    /// Unique session ID for this attachment
    pub session_id: Uuid,

    /// Endpoint where the source can be reached for data streaming
    pub source_endpoint: String,
}

impl AnchorAttachRequest {
    pub fn new(anchor_id: Uuid, session_id: Uuid, source_endpoint: String) -> Self {
        Self {
            anchor_id,
            session_id,
            source_endpoint,
        }
    }
}

/// Response to an attach request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnchorAttachResponse {
    /// Endpoint where the anchor can receive streaming data
    pub stream_endpoint: String,
}

/// Reasons why an attach request might fail
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum AttachError {
    /// Another source is currently attached
    AlreadyAttached,

    /// The anchor does not exist (may have been cancelled)
    AnchorNotFound,

    /// The anchor was cancelled by its creator
    AnchorCancelled,

    /// Internal error occurred
    InternalError(String),
}

impl std::fmt::Display for AttachError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AlreadyAttached => write!(f, "anchor is already attached"),
            Self::AnchorNotFound => write!(f, "anchor not found"),
            Self::AnchorCancelled => write!(f, "anchor was cancelled"),
            Self::InternalError(msg) => write!(f, "internal error: {}", msg),
        }
    }
}

impl std::error::Error for AttachError {}

/// Request to detach from a response anchor
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnchorDetachRequest {
    /// ID of the anchor to detach from
    pub anchor_id: Uuid,

    /// Session ID that was provided during attach
    pub session_id: Uuid,
}

impl AnchorDetachRequest {
    pub fn new(anchor_id: Uuid, session_id: Uuid) -> Self {
        Self {
            anchor_id,
            session_id,
        }
    }
}

/// Response to a detach request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnchorDetachResponse {
    pub success: bool,
}

/// Reasons why a detach request might fail
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum DetachError {
    /// The anchor does not exist
    AnchorNotFound,

    /// Session ID doesn't match current attachment
    SessionMismatch,

    /// No active attachment to detach
    NotAttached,

    /// Internal error occurred
    InternalError(String),
}

impl std::fmt::Display for DetachError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AnchorNotFound => write!(f, "anchor not found"),
            Self::SessionMismatch => write!(f, "session ID mismatch"),
            Self::NotAttached => write!(f, "no active attachment"),
            Self::InternalError(msg) => write!(f, "internal error: {}", msg),
        }
    }
}

impl std::error::Error for DetachError {}

/// Request to finalize a response anchor
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnchorFinalizeRequest {
    /// ID of the anchor to finalize
    pub anchor_id: Uuid,

    /// Session ID that was provided during attach
    pub session_id: Uuid,
}

impl AnchorFinalizeRequest {
    pub fn new(anchor_id: Uuid, session_id: Uuid) -> Self {
        Self {
            anchor_id,
            session_id,
        }
    }
}

/// Response to a finalize request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnchorFinalizeResponse {
    pub success: bool,
}

/// Reasons why a finalize request might fail
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum FinalizeError {
    /// The anchor does not exist
    AnchorNotFound,

    /// Session ID doesn't match current attachment
    SessionMismatch,

    /// No active attachment to finalize
    NotAttached,

    /// Internal error occurred
    InternalError(String),
}

impl std::fmt::Display for FinalizeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AnchorNotFound => write!(f, "anchor not found"),
            Self::SessionMismatch => write!(f, "session ID mismatch"),
            Self::NotAttached => write!(f, "no active attachment"),
            Self::InternalError(msg) => write!(f, "internal error: {}", msg),
        }
    }
}

impl std::error::Error for FinalizeError {}

/// Request to cancel a response anchor (sent by creator)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnchorCancelRequest {
    /// ID of the anchor to cancel
    pub anchor_id: Uuid,
}

impl AnchorCancelRequest {
    pub fn new(anchor_id: Uuid) -> Self {
        Self { anchor_id }
    }
}

/// Response to a cancel request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnchorCancelResponse {
    pub success: bool,
}

/// Reasons why a cancel request might fail
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum CancelError {
    /// The anchor does not exist (already cancelled or finalized)
    AnchorNotFound,

    /// Internal error occurred
    InternalError(String),
}

impl std::fmt::Display for CancelError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AnchorNotFound => write!(f, "anchor not found"),
            Self::InternalError(msg) => write!(f, "internal error: {}", msg),
        }
    }
}

impl std::error::Error for CancelError {}

/// Signal sent from anchor to source to trigger cancellation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceCancellationSignal {
    /// The cancellation ID that the source registered
    pub cancellation_id: Uuid,
}

impl SourceCancellationSignal {
    pub fn new(cancellation_id: Uuid) -> Self {
        Self { cancellation_id }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stream_frame_helpers() {
        let frame: StreamFrame<i32> = StreamFrame::ok(42);
        assert!(!frame.is_terminal());
        assert!(!frame.is_control());

        let frame: StreamFrame<i32> = StreamFrame::err("failed");
        assert!(!frame.is_terminal());
        assert!(!frame.is_control());

        let frame: StreamFrame<i32> = StreamFrame::Detached;
        assert!(!frame.is_terminal());
        assert!(frame.is_control());

        let frame: StreamFrame<i32> = StreamFrame::Finalized;
        assert!(frame.is_terminal());
        assert!(frame.is_control());

        let frame: StreamFrame<i32> = StreamFrame::transport_error("connection lost");
        assert!(frame.is_terminal());
        assert!(frame.is_control());
    }

    #[test]
    fn test_handle_serialization() {
        let handle = ResponseAnchorHandle::new(
            Uuid::new_v4(),
            Uuid::new_v4(),
            WorkerAddress::tcp("tcp://localhost:5555".to_string()),
        );

        let json = serde_json::to_string(&handle).unwrap();
        let deserialized: ResponseAnchorHandle = serde_json::from_str(&json).unwrap();

        assert_eq!(handle, deserialized);
    }

    #[test]
    fn test_attach_error_display() {
        assert_eq!(
            AttachError::AlreadyAttached.to_string(),
            "anchor is already attached"
        );
        assert_eq!(AttachError::AnchorNotFound.to_string(), "anchor not found");
        assert_eq!(
            AttachError::AnchorCancelled.to_string(),
            "anchor was cancelled"
        );
    }
}
