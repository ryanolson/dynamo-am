// SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Public API for Response Anchors
//!
//! This module provides the user-facing types for working with response anchors:
//! - `SerializedAnchorHandle<T>`: Serializable payload that can be sent across the wire
//! - `ArmedAnchorHandle<T>`: RAII guard that owns attachment rights
//! - `ResponseAnchorSource`: Entry point for attaching to remote anchors
//! - `ResponseSink`: Active connection that streams data back to an anchor

use anyhow::{Result, anyhow};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::thread;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex as AsyncMutex, Mutex as TokioMutex};
use tokio::task::JoinHandle;
use tokio::time;
use tokio::{
    runtime::{Handle, Runtime},
    time::timeout,
};
use tokio_util::sync::CancellationToken;
use tracing::warn;
use uuid::Uuid;

use crate::api::client::{ActiveMessageClient, PeerInfo};

use crate::protocols::response_anchor::{
    AnchorAttachRequest, AnchorAttachResponse, AnchorDetachRequest, AnchorDetachResponse,
    AnchorFinalizeRequest, AnchorFinalizeResponse, AnchorHandlePayload, FinalizeReason,
    StreamFrame,
};
use crate::runtime::network_client::NetworkClient;
use crate::transport::streaming::{StreamSink, StreamingTransport};

// Re-export legacy alias for incremental migration
pub use crate::protocols::response_anchor::ResponseAnchorHandle;

const STATE_ARMED: u8 = 0;
const STATE_ATTACHED: u8 = 1;
const STATE_FINALIZED: u8 = 2;

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
const CONTROL_PLANE_TIMEOUT: Duration = Duration::from_secs(10);

/// Serializable handle that can be sent over the wire.
#[derive(Debug)]
pub struct SerializedAnchorHandle<T> {
    payload: AnchorHandlePayload,
    _phantom: PhantomData<T>,
}

impl<T> SerializedAnchorHandle<T> {
    pub(crate) fn new(payload: AnchorHandlePayload) -> Self {
        Self {
            payload,
            _phantom: PhantomData,
        }
    }

    /// Access the underlying payload (mostly for diagnostics/tests).
    pub fn payload(&self) -> &AnchorHandlePayload {
        &self.payload
    }

    /// Arm this handle for use in the current process using the supplied client.
    pub fn arm(self, client: Arc<NetworkClient>) -> ArmedAnchorHandle<T> {
        ArmedAnchorHandle::from_payload(self.payload, client)
    }
}

impl<T> Clone for SerializedAnchorHandle<T> {
    fn clone(&self) -> Self {
        Self::new(self.payload.clone())
    }
}

impl<T> Serialize for SerializedAnchorHandle<T> {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.payload.serialize(serializer)
    }
}

impl<'de, T> Deserialize<'de> for SerializedAnchorHandle<T> {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let payload = AnchorHandlePayload::deserialize(deserializer)?;
        Ok(Self::new(payload))
    }
}

/// Armed handle that owns exclusive rights to attach to an anchor.
pub struct ArmedAnchorHandle<T> {
    lifecycle: Arc<HandleLifecycle>,
    client: Arc<NetworkClient>,
    _phantom: PhantomData<T>,
}

impl<T> ArmedAnchorHandle<T> {
    fn new(lifecycle: Arc<HandleLifecycle>, client: Arc<NetworkClient>) -> Self {
        lifecycle.set_client(&client);
        Self {
            lifecycle,
            client,
            _phantom: PhantomData,
        }
    }

    pub(crate) fn from_payload(payload: AnchorHandlePayload, client: Arc<NetworkClient>) -> Self {
        let lifecycle = HandleLifecycle::new(payload, &client);
        Self::new(lifecycle, client)
    }

    /// Consume this handle and return a serialized payload for transport.
    pub fn disarm(self) -> SerializedAnchorHandle<T> {
        let payload = self.lifecycle.payload().clone();
        std::mem::forget(self);
        SerializedAnchorHandle::new(payload)
    }

    /// Access the underlying payload.
    pub fn payload(&self) -> &AnchorHandlePayload {
        self.lifecycle.payload()
    }

    pub(crate) fn into_parts(self) -> (Arc<HandleLifecycle>, Arc<NetworkClient>) {
        let lifecycle = Arc::clone(&self.lifecycle);
        let client = Arc::clone(&self.client);
        std::mem::forget(self);
        (lifecycle, client)
    }
}

impl<T> Drop for ArmedAnchorHandle<T> {
    fn drop(&mut self) {
        self.lifecycle
            .schedule_finalize(None, FinalizeReason::Dropped);
    }
}

/// Attachment entry point for response anchors.
pub struct ResponseAnchorSource<T> {
    _phantom: PhantomData<T>,
}

impl<T: Serialize + Send + 'static> ResponseAnchorSource<T> {
    /// Attach to a remote anchor using an armed handle.
    pub async fn attach(handle: ArmedAnchorHandle<T>) -> Result<ResponseSink<T>> {
        attach_internal(handle).await
    }
}

async fn attach_internal<T: Serialize + Send + 'static>(
    handle: ArmedAnchorHandle<T>,
) -> Result<ResponseSink<T>> {
    let (lifecycle, client) = handle.into_parts();
    lifecycle.begin_attach()?;
    let payload = lifecycle.payload().clone();

    // Ensure we have a control-plane connection to the anchor holder before sending requests.
    let peer_info = PeerInfo::with_address(payload.instance_id, payload.worker_address.clone());
    client.connect_to_peer(peer_info).await?;

    let session_id = Uuid::new_v4();
    let source_instance_id = client.instance_id();
    let source_endpoint = client.endpoint().to_string();

    let request = AnchorAttachRequest::new(
        payload.anchor_id,
        session_id,
        source_instance_id,
        source_endpoint.clone(),
    );

    let attach_status = client
        .system_active_message("_anchor_attach")
        .expect_response::<AnchorAttachResponse>()
        .payload(request)?
        .send(payload.instance_id)
        .await;

    let attach_status = match attach_status {
        Ok(status) => status,
        Err(error) => {
            lifecycle.abort_attach();
            return Err(error);
        }
    };

    let response: AnchorAttachResponse = match timeout(CONTROL_PLANE_TIMEOUT, async {
        attach_status.await_response::<AnchorAttachResponse>().await
    })
    .await
    {
        Ok(result) => match result {
            Ok(resp) => resp,
            Err(error) => {
                lifecycle.abort_attach();
                return Err(error);
            }
        },
        Err(_) => {
            lifecycle.abort_attach();
            return Err(anyhow!("anchor attach timed out waiting for response"));
        }
    };

    let stream_endpoint = response.stream_endpoint;

    let mut stream_sink = match client
        .streaming_transport()
        .create_stream_source::<T>(&stream_endpoint, payload.anchor_id, session_id)
        .await
    {
        Ok(sink) => sink,
        Err(error) => {
            lifecycle.abort_attach();
            detach_after_failed_attach(&client, &payload, session_id).await;
            return Err(error);
        }
    };

    let cancellation_id = Uuid::new_v4();
    let prologue = StreamFrame::Prologue {
        source_instance_id,
        cancellation_id,
        source_endpoint: source_endpoint.clone(),
    };

    if let Err(error) = stream_sink.send(prologue).await {
        lifecycle.abort_attach();
        if let Err(close_err) = stream_sink.close().await {
            warn!(
                anchor_id = %payload.anchor_id,
                session_id = %session_id,
                error = %close_err,
                "Failed to close stream sink after prologue error",
            );
        }
        detach_after_failed_attach(&client, &payload, session_id).await;
        return Err(error);
    }

    let stream_sink = Arc::new(AsyncMutex::new(Some(stream_sink)));
    let last_activity = Arc::new(AsyncMutex::new(Instant::now()));
    let heartbeat_handle = Arc::new(TokioMutex::new(Some(
        ResponseSink::<T>::spawn_heartbeat_task(
            payload.anchor_id,
            Arc::clone(&stream_sink),
            Arc::clone(&last_activity),
        ),
    )));

    let cancel_token = Arc::new(CancellationToken::new());
    client.register_source_cancellation(cancellation_id, Arc::clone(&cancel_token));

    let cancellation_task = ResponseSink::<T>::spawn_cancellation_task(
        cancellation_id,
        Arc::clone(&cancel_token),
        Arc::clone(&stream_sink),
        Arc::clone(&lifecycle),
        Arc::clone(&client),
        session_id,
        Arc::clone(&heartbeat_handle),
    );

    Ok(ResponseSink {
        lifecycle,
        session_id,
        client,
        stream_sink,
        last_activity,
        heartbeat_handle,
        cancellation_id,
        cancel_token,
        cancellation_task: Some(cancellation_task),
        _phantom: PhantomData,
    })
}

async fn detach_after_failed_attach(
    client: &Arc<NetworkClient>,
    payload: &AnchorHandlePayload,
    session_id: Uuid,
) {
    let request = AnchorDetachRequest::new(payload.anchor_id, session_id);
    let result = async {
        let status = client
            .system_active_message("_anchor_detach")
            .expect_response::<AnchorDetachResponse>()
            .payload(request)?
            .send(payload.instance_id)
            .await?;
        let _: AnchorDetachResponse = status.await_response().await?;
        Result::<(), anyhow::Error>::Ok(())
    }
    .await;

    if let Err(error) = result {
        warn!(
            anchor_id = %payload.anchor_id,
            session_id = %session_id,
            error = %error,
            "Failed to detach after aborted attach",
        );
    }
}

/// Active streaming connection to a response anchor.
pub struct ResponseSink<T>
where
    T: Serialize + Send + 'static,
{
    lifecycle: Arc<HandleLifecycle>,
    session_id: Uuid,
    client: Arc<NetworkClient>,
    stream_sink: Arc<AsyncMutex<Option<Box<dyn StreamSink<T> + Send + 'static>>>>,
    last_activity: Arc<AsyncMutex<Instant>>,
    heartbeat_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
    cancellation_id: Uuid,
    cancel_token: Arc<CancellationToken>,
    cancellation_task: Option<JoinHandle<()>>,
    _phantom: PhantomData<T>,
}

impl<T: Serialize + Send + 'static> ResponseSink<T> {
    /// Send a successful payload to the anchor.
    pub async fn send_ok(&mut self, item: T) -> Result<()> {
        self.send_frame(StreamFrame::ok(item)).await
    }

    /// Send an application-level error to the anchor.
    pub async fn send_err(&mut self, error: impl Into<String>) -> Result<()> {
        self.send_frame(StreamFrame::err(error.into())).await
    }

    #[inline]
    async fn send_frame(&mut self, frame: StreamFrame<T>) -> Result<()> {
        if self.cancel_token.is_cancelled() {
            return Err(anyhow!("response sink cancelled"));
        }
        let mut guard = self.stream_sink.lock().await;
        if let Some(ref mut sink) = guard.as_mut() {
            sink.send(frame).await?;
            drop(guard);
            self.touch().await;
            Ok(())
        } else {
            Err(anyhow!("Stream sink not initialized"))
        }
    }

    async fn touch(&self) {
        let mut last = self.last_activity.lock().await;
        *last = Instant::now();
    }

    fn stop_heartbeat(&self) {
        if let Ok(mut guard) = self.heartbeat_handle.try_lock() {
            if let Some(handle) = guard.take() {
                handle.abort();
            }
        }
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn abort_heartbeat_for_test(&self) {
        self.stop_heartbeat();
    }

    fn teardown_cancellation_registration(&mut self) {
        self.client
            .deregister_source_cancellation(self.cancellation_id);
        self.cancel_token.cancel();
        if let Some(handle) = self.cancellation_task.take() {
            handle.abort();
        }
    }

    /// Detach from the anchor and regain the armed handle.
    pub async fn detach(mut self) -> Result<ArmedAnchorHandle<T>> {
        self.stop_heartbeat();
        let payload = self.lifecycle.payload().clone();

        ResponseSink::<T>::send_terminal_frame(
            Arc::clone(&self.stream_sink),
            StreamFrame::Detached,
        )
        .await?;

        self.send_detach_control(&payload).await?;
        self.lifecycle.mark_detached();

        self.teardown_cancellation_registration();

        Ok(ArmedAnchorHandle::new(
            Arc::clone(&self.lifecycle),
            Arc::clone(&self.client),
        ))
    }

    /// Finalize the anchor, preventing further attachments.
    pub async fn finalize(mut self) -> Result<()> {
        self.stop_heartbeat();

        self.lifecycle
            .finalize_explicit(Arc::clone(&self.client), self.session_id)
            .await?;

        ResponseSink::<T>::send_terminal_frame(
            Arc::clone(&self.stream_sink),
            StreamFrame::Finalized,
        )
        .await?;

        self.teardown_cancellation_registration();
        Ok(())
    }

    pub fn anchor_id(&self) -> Uuid {
        self.lifecycle.payload().anchor_id
    }

    pub fn session_id(&self) -> Uuid {
        self.session_id
    }

    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancel_token.as_ref().clone()
    }

    async fn send_detach_control(&self, payload: &AnchorHandlePayload) -> Result<()> {
        let request = AnchorDetachRequest::new(payload.anchor_id, self.session_id);
        let status = self
            .client
            .system_active_message("_anchor_detach")
            .expect_response::<AnchorDetachResponse>()
            .payload(request)?
            .send(payload.instance_id)
            .await?;
        timeout(CONTROL_PLANE_TIMEOUT, async {
            status.await_response::<AnchorDetachResponse>().await
        })
        .await
        .map_err(|_| anyhow!("anchor detach timed out"))??;
        Ok(())
    }

    fn spawn_heartbeat_task(
        anchor_id: Uuid,
        stream_sink: Arc<AsyncMutex<Option<Box<dyn StreamSink<T> + Send + 'static>>>>,
        last_activity: Arc<AsyncMutex<Instant>>,
    ) -> JoinHandle<()>
    where
        T: Serialize + Send + 'static,
    {
        tokio::spawn(async move {
            let mut ticker = time::interval(HEARTBEAT_INTERVAL);
            ticker.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
            loop {
                ticker.tick().await;

                let since_last = {
                    let last = last_activity.lock().await;
                    Instant::now().saturating_duration_since(*last)
                };

                if since_last < HEARTBEAT_INTERVAL {
                    continue;
                }

                let mut guard = stream_sink.lock().await;
                if let Some(ref mut sink) = guard.as_mut() {
                    if let Err(error) = sink.send(StreamFrame::Heartbeat).await {
                        warn!(
                            anchor_id = %anchor_id,
                            error = %error,
                            "Failed to send heartbeat; stopping heartbeat task",
                        );
                        break;
                    }
                    drop(guard);
                    let mut last = last_activity.lock().await;
                    *last = Instant::now();
                } else {
                    break;
                }
            }
        })
    }

    fn spawn_cancellation_task(
        cancellation_id: Uuid,
        cancel_token: Arc<CancellationToken>,
        stream_sink: Arc<AsyncMutex<Option<Box<dyn StreamSink<T> + Send + 'static>>>>,
        lifecycle: Arc<HandleLifecycle>,
        client: Arc<NetworkClient>,
        session_id: Uuid,
        heartbeat_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            cancel_token.cancelled().await;

            {
                let mut guard = heartbeat_handle.lock().await;
                if let Some(handle) = guard.take() {
                    handle.abort();
                }
            }

            client.deregister_source_cancellation(cancellation_id);

            ResponseSink::<T>::finalize_after_cancellation(lifecycle, stream_sink, session_id)
                .await;
        })
    }

    async fn finalize_after_cancellation(
        lifecycle: Arc<HandleLifecycle>,
        stream_sink: Arc<AsyncMutex<Option<Box<dyn StreamSink<T> + Send + 'static>>>>,
        session_id: Uuid,
    ) {
        let mut guard = stream_sink.lock().await;
        if let Some(mut sink) = guard.take() {
            if let Err(error) = sink.send(StreamFrame::Dropped).await {
                warn!(
                    session_id = %session_id,
                    %error,
                    "Failed to send dropped sentinel during cancellation",
                );
            }

            if let Err(error) = sink.close().await {
                warn!(
                    session_id = %session_id,
                    %error,
                    "Failed to close stream sink during cancellation",
                );
            }
        }

        lifecycle.schedule_finalize(Some(session_id), FinalizeReason::Dropped);
    }

    async fn send_terminal_frame(
        stream_sink: Arc<AsyncMutex<Option<Box<dyn StreamSink<T> + Send + 'static>>>>,
        frame: StreamFrame<T>,
    ) -> Result<()>
    where
        T: Serialize + Send + 'static,
    {
        let mut guard = stream_sink.lock().await;
        if let Some(mut sink) = guard.take() {
            sink.send(frame).await?;
            sink.close().await?;
        }
        Ok(())
    }
}

impl<T: Serialize + Send + 'static> Drop for ResponseSink<T> {
    fn drop(&mut self) {
        self.stop_heartbeat();
        self.teardown_cancellation_registration();

        let session_id = self.session_id;
        let anchor_id = self.lifecycle.payload().anchor_id;

        if self.lifecycle.is_armed() {
            return;
        }

        let mut finalize_fallback = true;

        if let Ok(mut guard) = self.stream_sink.try_lock() {
            if let Some(sink) = guard.take() {
                finalize_fallback = false;
                drop(guard);

                let lifecycle = Arc::clone(&self.lifecycle);
                let client = Arc::clone(&self.client);

                if let Ok(handle) = Handle::try_current() {
                    handle.spawn(async move {
                        let mut sink = sink;
                        let mut finalize_required = false;

                        if let Err(error) = sink.send(StreamFrame::Dropped).await {
                            warn!(
                                anchor_id = %anchor_id,
                                session_id = %session_id,
                                error = %error,
                                "Failed to send dropped sentinel during sink drop",
                            );
                            finalize_required = true;
                        }
                        if let Err(error) = sink.close().await {
                            warn!(
                                anchor_id = %anchor_id,
                                session_id = %session_id,
                                error = %error,
                                "Failed to close stream sink during sink drop",
                            );
                            finalize_required = true;
                        }

                        if finalize_required {
                            if let Err(error) = lifecycle
                                .finalize_async(
                                    Arc::clone(&client),
                                    Some(session_id),
                                    FinalizeReason::Dropped,
                                )
                                .await
                            {
                                warn!(
                                    anchor_id = %anchor_id,
                                    session_id = %session_id,
                                    error = %error,
                                    "Failed to finalize anchor during sink drop",
                                );
                            }
                        } else {
                            lifecycle.mark_finalized_local();
                        }
                    });
                } else {
                    thread::spawn(move || {
                        let mut sink = sink;
                        match Runtime::new() {
                            Ok(rt) => {
                                rt.block_on(async {
                                    let mut finalize_required = false;

                                    if let Err(error) = sink.send(StreamFrame::Dropped).await {
                                        warn!(
                                            anchor_id = %anchor_id,
                                            session_id = %session_id,
                                            error = %error,
                                            "Failed to send dropped sentinel during sink drop",
                                        );
                                        finalize_required = true;
                                    }
                                    if let Err(error) = sink.close().await {
                                        warn!(
                                            anchor_id = %anchor_id,
                                            session_id = %session_id,
                                            error = %error,
                                            "Failed to close stream sink during sink drop",
                                        );
                                        finalize_required = true;
                                    }

                                    if finalize_required {
                                        if let Err(error) = lifecycle
                                            .finalize_async(
                                                Arc::clone(&client),
                                                Some(session_id),
                                                FinalizeReason::Dropped,
                                            )
                                            .await
                                        {
                                            warn!(
                                                anchor_id = %anchor_id,
                                                session_id = %session_id,
                                                error = %error,
                                                "Failed to finalize anchor during sink drop",
                                            );
                                        }
                                    } else {
                                        lifecycle.mark_finalized_local();
                                    }
                                });
                            }
                            Err(error) => {
                                warn!(
                                    anchor_id = %anchor_id,
                                    session_id = %session_id,
                                    error = %error,
                                    "Failed to create runtime for sink drop finalize",
                                );
                                lifecycle
                                    .schedule_finalize(Some(session_id), FinalizeReason::Dropped);
                            }
                        }
                    });
                }
            }
        }

        if finalize_fallback {
            self.lifecycle
                .schedule_finalize(Some(self.session_id), FinalizeReason::Dropped);
        }
    }
}

pub(crate) struct HandleLifecycle {
    payload: AnchorHandlePayload,
    state: AtomicU8,
    client: Mutex<Option<Weak<NetworkClient>>>,
}

impl HandleLifecycle {
    fn new(payload: AnchorHandlePayload, client: &Arc<NetworkClient>) -> Arc<Self> {
        Arc::new(Self {
            payload,
            state: AtomicU8::new(STATE_ARMED),
            client: Mutex::new(Some(Arc::downgrade(client))),
        })
    }

    fn payload(&self) -> &AnchorHandlePayload {
        &self.payload
    }

    fn set_client(&self, client: &Arc<NetworkClient>) {
        if let Ok(mut guard) = self.client.lock() {
            *guard = Some(Arc::downgrade(client));
        }
    }

    fn begin_attach(&self) -> Result<()> {
        self.state
            .compare_exchange(
                STATE_ARMED,
                STATE_ATTACHED,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .map(|_| ())
            .map_err(|_| anyhow!("anchor handle is not attachable"))
    }

    fn abort_attach(&self) {
        self.state.store(STATE_ARMED, Ordering::SeqCst);
    }

    fn mark_detached(&self) {
        self.state.store(STATE_ARMED, Ordering::SeqCst);
    }

    fn is_armed(&self) -> bool {
        self.state.load(Ordering::SeqCst) == STATE_ARMED
    }

    fn mark_finalized_local(&self) {
        self.state.store(STATE_FINALIZED, Ordering::SeqCst);
    }

    async fn finalize_explicit(&self, client: Arc<NetworkClient>, session_id: Uuid) -> Result<()> {
        if self.state.swap(STATE_FINALIZED, Ordering::SeqCst) == STATE_FINALIZED {
            return Ok(());
        }
        Self::send_finalize(
            client,
            self.payload.clone(),
            Some(session_id),
            FinalizeReason::Explicit,
        )
        .await
    }

    fn schedule_finalize(&self, session_id: Option<Uuid>, reason: FinalizeReason) {
        if self.state.swap(STATE_FINALIZED, Ordering::SeqCst) == STATE_FINALIZED {
            return;
        }

        if let Some(client_arc) = self.upgrade_client() {
            let payload = self.payload.clone();
            if let Ok(handle) = Handle::try_current() {
                let client = Arc::clone(&client_arc);
                handle.spawn(async move {
                    let log_payload = payload.clone();
                    if let Err(error) =
                        Self::send_finalize(client, payload, session_id, reason).await
                    {
                        warn!(
                            anchor_id = %log_payload.anchor_id,
                            session = ?session_id,
                            error = %error,
                            "Failed to finalize anchor on schedule",
                        );
                    }
                });
            } else {
                let client = Arc::clone(&client_arc);
                let log_payload = payload.clone();
                thread::spawn(move || match Runtime::new() {
                    Ok(rt) => {
                        if let Err(error) =
                            rt.block_on(Self::send_finalize(client, payload, session_id, reason))
                        {
                            warn!(
                                anchor_id = %log_payload.anchor_id,
                                session = ?session_id,
                                error = %error,
                                "Failed to finalize anchor on schedule",
                            );
                        }
                    }
                    Err(error) => {
                        warn!(
                            anchor_id = %log_payload.anchor_id,
                            session = ?session_id,
                            error = %error,
                            "Failed to create runtime for scheduled finalize",
                        );
                    }
                });
            }
        }
    }

    async fn finalize_async(
        &self,
        client: Arc<NetworkClient>,
        session_id: Option<Uuid>,
        reason: FinalizeReason,
    ) -> Result<()> {
        Self::send_finalize(client, self.payload.clone(), session_id, reason).await
    }

    fn upgrade_client(&self) -> Option<Arc<NetworkClient>> {
        self.client
            .lock()
            .ok()
            .and_then(|guard| guard.as_ref().and_then(|weak| weak.upgrade()))
    }

    async fn send_finalize(
        client: Arc<NetworkClient>,
        payload: AnchorHandlePayload,
        session_id: Option<Uuid>,
        reason: FinalizeReason,
    ) -> Result<()> {
        let session = session_id.unwrap_or_else(Uuid::nil);
        let request = AnchorFinalizeRequest::new(payload.anchor_id, session, reason);
        let status = client
            .system_active_message("_anchor_finalize")
            .expect_response::<AnchorFinalizeResponse>()
            .payload(request)?
            .send(payload.instance_id)
            .await?;
        timeout(CONTROL_PLANE_TIMEOUT, async {
            status.await_response::<AnchorFinalizeResponse>().await
        })
        .await
        .map_err(|_| anyhow!("anchor finalize timed out"))??;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::client::WorkerAddress;

    #[test]
    fn test_handle_serialization() {
        let payload = AnchorHandlePayload::new(
            Uuid::new_v4(),
            Uuid::new_v4(),
            WorkerAddress::tcp("tcp://localhost:5555".to_string()),
        );
        let handle: SerializedAnchorHandle<()> = SerializedAnchorHandle::new(payload.clone());

        let json = serde_json::to_string(&handle).unwrap();
        let deserialized: SerializedAnchorHandle<()> = serde_json::from_str(&json).unwrap();

        assert_eq!(handle.payload().anchor_id, deserialized.payload().anchor_id);
        assert_eq!(
            handle.payload().instance_id,
            deserialized.payload().instance_id
        );
    }
}
