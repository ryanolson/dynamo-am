// SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Streaming transport abstraction for response anchors
//!
//! This module provides traits for streaming data frames between response
//! anchors and their sources. Unlike the main ThinTransport which handles
//! ActiveMessage routing, this transport is specialized for streaming:
//!
//! - Ordered delivery of StreamFrame<T> items
//! - Direct point-to-point connections
//! - Backpressure via bounded channels
//! - Type-safe serialization/deserialization

use anyhow::Result;
use async_trait::async_trait;
use serde::{Serialize, de::DeserializeOwned};
use uuid::Uuid;

use crate::protocols::response_anchor::StreamFrame;

/// Transport abstraction for streaming data frames
///
/// This trait defines the interface for creating streaming connections
/// between response anchors (receivers) and sources (senders).
///
/// Implementations should handle:
/// - Connection establishment and lifecycle
/// - Frame serialization/deserialization
/// - Error handling and recovery
/// - Backpressure management
#[async_trait]
pub trait StreamingTransport: Send + Sync + std::fmt::Debug {
    /// Bind a streaming endpoint and return its address
    ///
    /// This creates a receiver endpoint that can accept incoming connections
    /// from sources. The returned address should be usable by `create_stream_source`.
    ///
    /// For ZMQ: Binds a ROUTER socket and returns the endpoint (e.g., "tcp://*:5556")
    /// For HTTP: Starts a server and returns the URL
    async fn bind_stream_endpoint(&self) -> Result<String>;

    /// Create a sink for sending frames to a remote anchor
    ///
    /// This establishes a connection to the anchor's streaming endpoint
    /// and returns a sink that can send StreamFrame<T> items.
    ///
    /// # Arguments
    /// * `endpoint` - The streaming endpoint address (from bind_stream_endpoint)
    /// * `anchor_id` - ID of the anchor to send frames to
    /// * `session_id` - Session ID for this attachment
    async fn create_stream_source<T>(
        &self,
        endpoint: &str,
        anchor_id: Uuid,
        session_id: Uuid,
    ) -> Result<Box<dyn StreamSink<T> + Send + 'static>>
    where
        T: Serialize + Send + 'static;

    /// Create a receiver for frames addressed to a specific anchor
    ///
    /// This sets up a receiver that listens for incoming frames
    /// on the bound streaming endpoint and filters for frames
    /// addressed to the specified anchor_id.
    ///
    /// # Arguments
    /// * `anchor_id` - ID of the anchor that should receive frames
    async fn create_stream_receiver<T>(
        &self,
        anchor_id: Uuid,
    ) -> Result<Box<dyn StreamReceiver<T> + Send + 'static>>
    where
        T: DeserializeOwned + Send + 'static;
}

/// Sink for sending stream frames
///
/// This trait represents an active streaming connection that can
/// send frames to a remote anchor.
#[async_trait]
pub trait StreamSink<T>: Send {
    /// Send a stream frame
    ///
    /// This sends a frame to the remote anchor. The frame may be:
    /// - Item(Ok(data)) - successful data item
    /// - Item(Err(msg)) - application-level error
    /// - Detached - sentinel frame indicating detachment
    /// - Finalized - sentinel frame indicating stream closure
    /// - TransportError(msg) - fatal transport error
    ///
    /// # Errors
    /// Returns an error if:
    /// - The connection is closed
    /// - Serialization fails
    /// - The underlying transport fails
    async fn send(&mut self, frame: StreamFrame<T>) -> Result<()>
    where
        T: Serialize;

    /// Close the sink
    ///
    /// This gracefully closes the connection. After calling close,
    /// no more frames can be sent.
    async fn close(self: Box<Self>) -> Result<()>;
}

/// Receiver for stream frames
///
/// This trait represents a receiver that can consume frames
/// from a remote source.
#[async_trait]
pub trait StreamReceiver<T>: Send {
    /// Receive the next stream frame
    ///
    /// This blocks until a frame is received or the connection is closed.
    /// Returns None when the connection is closed.
    ///
    /// # Errors
    /// Returns an error if:
    /// - Deserialization fails
    /// - The underlying transport fails
    async fn recv(&mut self) -> Option<Result<StreamFrame<T>>>
    where
        T: DeserializeOwned;
}

#[cfg(test)]
mod tests {
    use super::*;

    // StreamingTransport is not dyn-compatible due to generic methods
    // This is intentional - we use Arc<ConcreteType> instead of Box<dyn Trait>
}
