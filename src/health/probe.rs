//! Shared health probe cache, background poller, HTTP body fetcher, and JSON object guard.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http::Request;
use http_body_util::BodyExt;
use tokio::sync::watch;
use tracing::{debug, info, warn};

use crate::metrics::{ClSlotSample, HealthSample};
use crate::state::AppState;
use crate::transport::{HttpTransport, ProxyClient, ResBody};

use super::cl::{ClStatus, cl_peer_count, cl_syncing_status};
use super::el::{ElBlockResult, ElSyncingResult, el_batch_detect};
use super::{ReadinessEval, Transition, evaluate_readiness, readiness_transition};

pub(crate) type ProbeError = Arc<str>;

/// Source of readiness and health data for `/healthz`, `/readyz`, and `node_health`.
pub(crate) enum HealthSource {
    /// `--trust-upstream`: no background poller or `watch` channel is allocated.
    Trusted,
    /// Active background polling feeding snapshots via `watch`.
    Polled(watch::Receiver<Arc<Probe>>),
}

/// The five upstream probe results captured by one poll, shared with the request
/// handlers via a `watch` channel. `/healthz` and `/readyz` read the latest one
/// instead of querying upstream per request, so upstream load is decoupled from
/// probe rate. Stored as raw results so block / slot ages can be recomputed live.
#[derive(Clone)]
pub(crate) struct Probe {
    pub(super) el_syncing: Result<ElSyncingResult, ProbeError>,
    pub(super) el_peers: Result<u64, ProbeError>,
    pub(super) el_block: Result<ElBlockResult, ProbeError>,
    pub(super) cl_status: Result<ClStatus, ProbeError>,
    pub(super) cl_peers: Result<u64, ProbeError>,
}

impl Probe {
    pub(crate) fn pending() -> Self {
        let pending: ProbeError = Arc::from("initial poll pending");
        Self {
            el_syncing: Err(pending.clone()),
            el_peers: Err(pending.clone()),
            el_block: Err(pending.clone()),
            cl_status: Err(pending.clone()),
            cl_peers: Err(pending),
        }
    }

    pub(crate) const fn has_errors(&self) -> bool {
        self.el_syncing.is_err()
            || self.el_peers.is_err()
            || self.el_block.is_err()
            || self.cl_status.is_err()
            || self.cl_peers.is_err()
    }
}

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub(super) enum HealthError {
    Timeout,
    Transport(ProbeError),
    HttpStatus(http::StatusCode),
    Decode(ProbeError),
    Rpc(ProbeError),
}

impl HealthError {
    pub(super) fn into_probe_error(self) -> ProbeError {
        match self {
            Self::Timeout => Arc::from("timeout"),
            other => Arc::from(other.to_string()),
        }
    }
}

impl fmt::Display for HealthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Timeout => f.write_str("timeout"),
            Self::Transport(s) => write!(f, "transport: {s}"),
            Self::HttpStatus(status) => write!(f, "http {status}"),
            Self::Decode(s) => write!(f, "decode: {s}"),
            Self::Rpc(s) => write!(f, "rpc error: {s}"),
        }
    }
}

/// A hard transport/connection failure or a timeout (vs. an HTTP status or decode
/// error) — the only signal that the chosen HTTP version might be wrong for this
/// upstream. A timeout is included so that if the upstream hangs on the HTTP/2
/// connection preface, we can fall back to HTTP/1.1 if the alternate client succeeds.
/// If both timeout, we do not switch.
pub(super) const fn is_transport(e: &HealthError) -> bool {
    matches!(e, HealthError::Transport(_) | HealthError::Timeout)
}

/// Query all five upstream signals once. Each call is bounded by
/// `--health-timeout`; failures land in the result rather than aborting.
pub(crate) async fn probe_once(state: &AppState) -> Probe {
    // Independent upstreams share one poll; a slow EL does not delay CL calls.
    // EL still uses one batch request and owns transport detection.
    let (batch, cl_status, cl_peers) = tokio::join!(
        el_batch_detect(state),
        cl_syncing_status(state),
        cl_peer_count(state),
    );
    let (el_syncing, el_peers, el_block) = match batch {
        Ok(res) => (
            res.syncing.map_err(HealthError::into_probe_error),
            res.peers.map_err(HealthError::into_probe_error),
            res.block.map_err(HealthError::into_probe_error),
        ),
        Err(err) => {
            let shared = err.into_probe_error();
            (Err(shared.clone()), Err(shared.clone()), Err(shared))
        }
    };
    Probe {
        el_syncing,
        el_peers,
        el_block,
        cl_status,
        cl_peers,
    }
}

pub(crate) fn update_metrics(state: &AppState, probe: &Probe, eval: ReadinessEval<'_>) {
    let el_peers = probe.el_peers.as_ref().map_or(0.0, |&p| p as f64);
    let cl_peers = probe.cl_peers.as_ref().map_or(0.0, |&p| p as f64);
    let el_distance = match &probe.el_syncing {
        Ok(ElSyncingResult::Syncing {
            current_block,
            highest_block,
        }) => highest_block.saturating_sub(*current_block) as f64,
        _ => 0.0,
    };
    let cl_status = probe.cl_status.as_ref().ok().map(|s| ClSlotSample {
        sync_distance: s.sync_distance,
        head_slot: s.head_slot,
    });
    let el_block = probe.el_block.as_ref().ok().map(|b| b.number);

    state.health_metrics.record(HealthSample {
        el_ok: eval.el_sync_ok(),
        cl_ok: eval.cl_sync_ok(),
        el_peers,
        cl_peers,
        el_distance,
        cl_status,
        el_block,
    });
}

struct OptDisplay<T>(Option<T>);

impl<T: fmt::Display> fmt::Display for OptDisplay<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.0 {
            Some(v) => v.fmt(f),
            None => f.write_str("-"),
        }
    }
}

/// Refresh the shared [`Probe`] until shutdown: poll, then sleep `interval`,
/// repeat. The cache is warmed by one poll before this loop starts, so the first
/// background refresh lands `interval` later. Sleeping *after* each poll gives a
/// slow upstream a full `interval` breather instead of being polled back-to-back.
/// An in-flight poll is cancelled promptly on shutdown.
///
/// This loop is also the single place readiness is logged: a transition is logged
/// once (bounded to the poll rate), independent of how often `/readyz` is probed,
/// and visible even if nothing probes it.
pub(crate) async fn poll_loop(
    state: Arc<AppState>,
    tx: watch::Sender<Arc<Probe>>,
    shutdown: crate::lifecycle::Shutdown,
    interval: Duration,
) {
    let mut prev_ready: Option<bool> = None;
    loop {
        tokio::select! {
            biased;
            _ = crate::lifecycle::deadline(&shutdown) => return,
            _ = tokio::time::sleep(interval) => {}
        }
        let probe = tokio::select! {
            biased;
            _ = crate::lifecycle::deadline(&shutdown) => return,
            probe = probe_once(&state) => probe,
        };

        let eval = evaluate_readiness(&state, &probe);
        let ready = eval.is_ready();
        let status = if ready { "ready" } else { "not_ready" };
        debug!(status, "health poll");

        update_metrics(&state, &probe, eval);

        match readiness_transition(prev_ready, ready) {
            Transition::Silent => {}
            Transition::Recovered => info!("readiness recovered"),
            Transition::BecameNotReady => {
                let (el_block, cl_slot) = match eval.strict {
                    Some((el_f, cl_f)) => (Some(el_f), Some(cl_f)),
                    None => (None, None),
                };
                warn!(
                    el_syncing = %eval.el_sync,
                    cl_syncing = %eval.cl_sync,
                    el_block = %OptDisplay(el_block),
                    cl_slot = %OptDisplay(cl_slot),
                    "not ready"
                );
            }
        }
        prev_ready = Some(ready);

        if tx.send(Arc::new(probe)).is_err() {
            return; // all receivers dropped
        }
    }
}

/// Send a prepared probe request with the health-poll timeout, record any TLS
/// negotiated protocol version as soon as response headers arrive, and return
/// the body of a successful response.
pub(super) async fn fetch_bytes(
    state: &AppState,
    req: Request<ResBody>,
    client: &ProxyClient,
    transport: &HttpTransport,
) -> Result<Bytes, HealthError> {
    tokio::time::timeout(state.cfg.health_timeout, async {
        let resp = client
            .request(req)
            .await
            .map_err(|e| HealthError::Transport(Arc::from(e.to_string())))?;
        transport.record_version(resp.version());
        if !resp.status().is_success() {
            return Err(HealthError::HttpStatus(resp.status()));
        }
        http_body_util::Limited::new(resp.into_body(), 10 * 1024 * 1024)
            .collect()
            .await
            .map(|c| c.to_bytes())
            .map_err(|e| HealthError::Decode(Arc::from(e.to_string())))
    })
    .await
    .map_err(|_| HealthError::Timeout)?
}

/// Serde's derived struct visitors also accept positional sequences. Enforce
/// object shape at each upstream boundary while retaining borrowed `RawValue` data.
pub(super) struct JsonObject<T>(pub(super) T);

impl<'de, T: serde::Deserialize<'de>> serde::Deserialize<'de> for JsonObject<T> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Object<T>(std::marker::PhantomData<T>);
        impl<'de, T: serde::Deserialize<'de>> serde::de::Visitor<'de> for Object<T> {
            type Value = JsonObject<T>;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a JSON object")
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                map: A,
            ) -> Result<Self::Value, A::Error> {
                T::deserialize(serde::de::value::MapAccessDeserializer::new(map)).map(JsonObject)
            }
        }
        deserializer.deserialize_map(Object(std::marker::PhantomData))
    }
}
