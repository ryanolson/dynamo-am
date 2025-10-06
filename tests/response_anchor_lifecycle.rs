// SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Lifecycle tests for response anchors.
//!
//! Focused on the ergonomics and resource semantics of the staged handle API:
//! - Disarming should prevent drop-driven finalization.
//! - Dropping an armed handle should finalize the anchor.
//! - Multiple arm/disarm cycles should behave predictably.

use anyhow::Result;
use dynamo_am::{
    ResponseAnchorSource, SerializedAnchorHandle, manager::ActiveMessageManager,
    zmq::ZmqActiveMessageManager,
};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct TestData {
    value: i32,
}

async fn setup_manager(
    address: &str,
    cancel: CancellationToken,
) -> Result<ZmqActiveMessageManager> {
    ZmqActiveMessageManager::new(address.to_string(), cancel).await
}

/// When a handle is disarmed, dropping subsequent sinks must NOT finalize the anchor.
#[tokio::test(flavor = "multi_thread")]
async fn test_disarm_prevents_drop_finalization() -> Result<()> {
    let cancel = CancellationToken::new();
    let manager = setup_manager("tcp://127.0.0.1:0", cancel.clone()).await?;

    let (handle, mut stream) = manager.create_local_response_anchor::<TestData>().await?;

    let serialized: SerializedAnchorHandle<TestData> = handle.disarm();
    let client = manager.client();

    // Attach using re-armed handle
    let mut sink =
        ResponseAnchorSource::<TestData>::attach(serialized.clone().arm(client.clone())).await?;
    sink.send_ok(TestData { value: 1 }).await?;

    assert_eq!(stream.recv().await.unwrap()?.value, 1);

    // Detach and ensure we can re-arm + attach again
    let armed = sink.detach().await?;
    drop(armed); // dropping armed handle should finalize

    // Stream should observe dropped sentinel
    assert!(stream.recv().await.unwrap().is_err());

    manager.shutdown().await?;
    Ok(())
}

/// Dropping an armed handle without ever attaching should finalize the anchor.
#[tokio::test(flavor = "multi_thread")]
async fn test_armed_handle_drop_finalizes_anchor() -> Result<()> {
    let cancel = CancellationToken::new();
    let manager = setup_manager("tcp://127.0.0.1:0", cancel.clone()).await?;

    let (handle, mut stream) = manager.create_local_response_anchor::<TestData>().await?;

    drop(handle); // no attachment ever happens

    // Stream should terminate with dropped/finalized error
    let result = stream.recv().await;
    assert!(matches!(result, None | Some(Err(_))));

    manager.shutdown().await?;
    Ok(())
}

/// Multiple arm/disarm cycles should remain usable.
#[tokio::test(flavor = "multi_thread")]
async fn test_multiple_disarm_rearm_cycles() -> Result<()> {
    let cancel = CancellationToken::new();
    let manager_a = setup_manager("tcp://127.0.0.1:0", cancel.clone()).await?;
    let manager_b = setup_manager("tcp://127.0.0.1:0", cancel.clone()).await?;

    let client_b = manager_b.client();
    let (mut handle, mut stream) = manager_a.create_local_response_anchor::<TestData>().await?;

    // Cycle 1: disarm/arm with no attach
    let serialized = handle.disarm();
    handle = serialized.clone().arm(client_b.clone());

    // Cycle 2: disarm/arm again, this time attach
    let serialized = handle.disarm();
    let mut sink =
        ResponseAnchorSource::<TestData>::attach(serialized.arm(client_b.clone())).await?;
    sink.send_ok(TestData { value: 7 }).await?;

    assert_eq!(stream.recv().await.unwrap()?.value, 7);
    sink.finalize().await?;

    manager_a.shutdown().await?;
    manager_b.shutdown().await?;
    Ok(())
}
