// SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use bytes::Bytes;
use dashmap::DashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;
use tracing::{debug, warn};
use uuid::Uuid;

use crate::receipt_ack::ReceiptAck;

#[derive(Debug)]
pub struct AckEntry {
    pub sender: oneshot::Sender<Result<(), String>>,
    pub deadline: tokio::time::Instant,
}

#[derive(Debug)]
pub struct AcceptanceEntry {
    pub sender: oneshot::Sender<()>,
    pub deadline: tokio::time::Instant,
}

#[derive(Debug)]
pub struct ResponseEntry {
    pub sender: oneshot::Sender<Bytes>,
    pub deadline: tokio::time::Instant,
}

#[derive(Debug)]
pub struct ReceiptEntry {
    pub sender: oneshot::Sender<Result<ReceiptAck, String>>,
    pub deadline: tokio::time::Instant,
}

/// Transport-agnostic response correlation manager
///
/// This manager handles all response correlation state that was previously
/// tied to specific transport implementations (like ZMQ). It provides
/// concurrent access using DashMap and centralizes timeout handling.
#[derive(Debug)]
pub struct ResponseManager {
    pending_acks: DashMap<Uuid, AckEntry>,
    pending_acceptances: DashMap<Uuid, AcceptanceEntry>,
    pending_responses: DashMap<Uuid, ResponseEntry>,
    pending_receipts: DashMap<Uuid, ReceiptEntry>,
}

impl Default for ResponseManager {
    fn default() -> Self {
        Self::new()
    }
}

impl ResponseManager {
    /// Create a new ResponseManager
    pub fn new() -> Self {
        Self {
            pending_acks: DashMap::new(),
            pending_acceptances: DashMap::new(),
            pending_responses: DashMap::new(),
            pending_receipts: DashMap::new(),
        }
    }

    // === ACK MANAGEMENT ===

    /// Register a pending acknowledgment
    pub fn register_ack(
        &self,
        message_id: Uuid,
        sender: oneshot::Sender<Result<(), String>>,
        timeout: Duration,
    ) {
        let entry = AckEntry {
            sender,
            deadline: tokio::time::Instant::now() + timeout,
        };
        self.pending_acks.insert(message_id, entry);
        debug!(message_id = %message_id, "Registered pending ACK");
    }

    /// Complete a pending acknowledgment
    pub fn complete_ack(&self, message_id: Uuid, result: Result<(), String>) -> bool {
        if let Some((_, entry)) = self.pending_acks.remove(&message_id) {
            let _ = entry.sender.send(result);
            debug!(message_id = %message_id, "Completed pending ACK");
            true
        } else {
            warn!(message_id = %message_id, "No pending ACK found for message");
            false
        }
    }

    // === ACCEPTANCE MANAGEMENT ===

    /// Register a pending acceptance
    pub fn register_acceptance(
        &self,
        message_id: Uuid,
        sender: oneshot::Sender<()>,
        timeout: Duration,
    ) {
        let entry = AcceptanceEntry {
            sender,
            deadline: tokio::time::Instant::now() + timeout,
        };
        self.pending_acceptances.insert(message_id, entry);
        debug!(message_id = %message_id, "Registered pending acceptance");
    }

    /// Complete a pending acceptance
    pub fn complete_acceptance(&self, message_id: Uuid) -> bool {
        if let Some((_, entry)) = self.pending_acceptances.remove(&message_id) {
            let _ = entry.sender.send(());
            debug!(message_id = %message_id, "Completed pending acceptance");
            true
        } else {
            warn!(message_id = %message_id, "No pending acceptance found for message");
            false
        }
    }

    // === RESPONSE MANAGEMENT ===

    /// Register a pending response
    pub fn register_response(
        &self,
        message_id: Uuid,
        sender: oneshot::Sender<Bytes>,
        timeout: Duration,
    ) {
        let entry = ResponseEntry {
            sender,
            deadline: tokio::time::Instant::now() + timeout,
        };
        self.pending_responses.insert(message_id, entry);
        debug!(message_id = %message_id, "Registered pending response");
    }

    /// Complete a pending response
    pub fn complete_response(&self, message_id: Uuid, response: Bytes) -> bool {
        if let Some((_, entry)) = self.pending_responses.remove(&message_id) {
            let _ = entry.sender.send(response);
            debug!(message_id = %message_id, "Completed pending response");
            true
        } else {
            warn!(message_id = %message_id, "No pending response found for message");
            false
        }
    }

    // === RECEIPT MANAGEMENT ===

    /// Register a pending receipt
    pub fn register_receipt(
        &self,
        message_id: Uuid,
        sender: oneshot::Sender<Result<ReceiptAck, String>>,
        timeout: Duration,
    ) {
        let entry = ReceiptEntry {
            sender,
            deadline: tokio::time::Instant::now() + timeout,
        };
        self.pending_receipts.insert(message_id, entry);
        debug!(message_id = %message_id, "Registered pending receipt");
    }

    /// Complete a pending receipt
    pub fn complete_receipt(&self, message_id: Uuid, receipt: Result<ReceiptAck, String>) -> bool {
        if let Some((_, entry)) = self.pending_receipts.remove(&message_id) {
            let _ = entry.sender.send(receipt);
            debug!(message_id = %message_id, "Completed pending receipt");
            true
        } else {
            warn!(message_id = %message_id, "No pending receipt found for message");
            false
        }
    }

    // === TIMEOUT MANAGEMENT ===

    /// Clean up expired entries
    /// Returns the number of entries that were cleaned up
    pub fn cleanup_expired(&self) -> usize {
        let now = tokio::time::Instant::now();
        let mut cleaned = 0;

        // Clean expired ACKs
        self.pending_acks.retain(|message_id, entry| {
            if now >= entry.deadline {
                warn!(message_id = %message_id, "ACK timed out");
                cleaned += 1;
                false
            } else {
                true
            }
        });

        // Clean expired acceptances
        self.pending_acceptances.retain(|message_id, entry| {
            if now >= entry.deadline {
                warn!(message_id = %message_id, "Acceptance timed out");
                cleaned += 1;
                false
            } else {
                true
            }
        });

        // Clean expired responses
        self.pending_responses.retain(|message_id, entry| {
            if now >= entry.deadline {
                warn!(message_id = %message_id, "Response timed out");
                cleaned += 1;
                false
            } else {
                true
            }
        });

        // Clean expired receipts
        self.pending_receipts.retain(|message_id, entry| {
            if now >= entry.deadline {
                warn!(message_id = %message_id, "Receipt timed out");
                cleaned += 1;
                false
            } else {
                true
            }
        });

        if cleaned > 0 {
            debug!(count = cleaned, "Cleaned up expired entries");
        }

        cleaned
    }

    /// Get counts of pending entries for debugging/monitoring
    pub fn pending_counts(&self) -> (usize, usize, usize, usize) {
        (
            self.pending_acks.len(),
            self.pending_acceptances.len(),
            self.pending_responses.len(),
            self.pending_receipts.len(),
        )
    }

    /// Cancel all pending operations (useful for shutdown)
    pub fn cancel_all(&self) {
        debug!("Cancelling all pending operations");
        self.pending_acks.clear();
        self.pending_acceptances.clear();
        self.pending_responses.clear();
        self.pending_receipts.clear();
    }
}

/// Shared instance of ResponseManager for use across the application
pub type SharedResponseManager = Arc<ResponseManager>;

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::Duration;

    #[tokio::test]
    async fn test_ack_registration_and_completion() {
        let manager = ResponseManager::new();
        let message_id = Uuid::new_v4();

        let (sender, receiver) = oneshot::channel();
        manager.register_ack(message_id, sender, Duration::from_secs(1));

        // Complete the ACK
        assert!(manager.complete_ack(message_id, Ok(())));

        // Verify the receiver got the result
        assert!(receiver.await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn test_response_registration_and_completion() {
        let manager = ResponseManager::new();
        let message_id = Uuid::new_v4();
        let response_data = Bytes::from("test response");

        let (sender, receiver) = oneshot::channel();
        manager.register_response(message_id, sender, Duration::from_secs(1));

        // Complete the response
        assert!(manager.complete_response(message_id, response_data.clone()));

        // Verify the receiver got the response
        let received = receiver.await.unwrap();
        assert_eq!(received, response_data);
    }

    #[tokio::test]
    async fn test_cleanup_expired() {
        let manager = ResponseManager::new();
        let message_id = Uuid::new_v4();

        // Register with a very short timeout
        let (sender, _receiver) = oneshot::channel();
        manager.register_ack(message_id, sender, Duration::from_millis(1));

        // Wait for expiration
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Cleanup should remove the expired entry
        let cleaned = manager.cleanup_expired();
        assert_eq!(cleaned, 1);

        // Trying to complete should fail now
        assert!(!manager.complete_ack(message_id, Ok(())));
    }

    #[tokio::test]
    async fn test_pending_counts() {
        let manager = ResponseManager::new();

        let (sender1, _) = oneshot::channel();
        let (sender2, _) = oneshot::channel();
        let (sender3, _) = oneshot::channel();

        manager.register_ack(Uuid::new_v4(), sender1, Duration::from_secs(1));
        manager.register_response(Uuid::new_v4(), sender2, Duration::from_secs(1));
        manager.register_acceptance(Uuid::new_v4(), sender3, Duration::from_secs(1));

        let (acks, acceptances, responses, receipts) = manager.pending_counts();
        assert_eq!(acks, 1);
        assert_eq!(acceptances, 1);
        assert_eq!(responses, 1);
        assert_eq!(receipts, 0);
    }
}
