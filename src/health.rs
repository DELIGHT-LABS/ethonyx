use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Duration;

use bytes::Bytes;
use http::{Method, Request, Response, StatusCode, Uri, Version};
use http_body_util::{BodyExt, Full};
use serde::Serialize;
use tokio::sync::watch;
use tracing::{debug, info, warn};

use crate::proxy::{ProxyClient, ResBody, box_full};
use crate::state::AppState;

/// Whether `/healthz` reflects active background probes or `--trust-upstream`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HealthMode {
    Probed,
    Trusted,
}

/// Tracks the negotiated HTTP protocol version observed on an `https://` upstream.
#[derive(Default)]
pub(crate) struct ObservedHttpVersion(AtomicU8);

impl ObservedHttpVersion {
    const HTTP_11: u8 = 0;
    const HTTP_2: u8 = 1;

    pub(crate) const fn new() -> Self {
        Self(AtomicU8::new(Self::HTTP_11))
    }

    pub(crate) fn record(&self, version: Version) {
        let encoded = if version == Version::HTTP_2 {
            Self::HTTP_2
        } else {
            Self::HTTP_11
        };
        self.0.store(encoded, Ordering::Relaxed);
    }

    pub(crate) fn label(&self) -> &'static str {
        if self.0.load(Ordering::Relaxed) == Self::HTTP_2 {
            "h2"
        } else {
            "http/1.1"
        }
    }
}

/// Transport state machine for the Execution Layer JSON-RPC HTTP hop.
pub(crate) enum ElTransport {
    /// `--trust-upstream` over cleartext `http://`: never probes `h2c`, uses HTTP/1.1.
    TrustedH1,
    /// `https://` upstream: negotiates `h2` vs `http/1.1` via TLS ALPN on `state.client`.
    TlsAuto(ObservedHttpVersion),
    /// Probed cleartext `http://` upstream with one-shot `h2c` recovery verification.
    AdaptiveH2c(AtomicU8),
}

impl ElTransport {
    const H2C_INITIAL: u8 = 0;
    const H2C_CONFIRMED: u8 = 1;
    const H1_VERIFY_ONCE: u8 = 2;
    const H1_CONFIRMED: u8 = 3;

    pub(crate) const fn new(trust_upstream: bool, is_tls: bool) -> Self {
        if is_tls {
            Self::TlsAuto(ObservedHttpVersion::new())
        } else if trust_upstream {
            Self::TrustedH1
        } else {
            Self::AdaptiveH2c(AtomicU8::new(Self::H2C_INITIAL))
        }
    }

    #[cfg(test)]
    const fn confirmed_cleartext(prefer_h2: bool) -> Self {
        Self::AdaptiveH2c(AtomicU8::new(if prefer_h2 {
            Self::H2C_CONFIRMED
        } else {
            Self::H1_CONFIRMED
        }))
    }

    /// Whether data-plane EL requests should use the cleartext `h2c` prior-knowledge client.
    pub(crate) fn uses_h2c_client(&self) -> bool {
        match self {
            Self::TrustedH1 | Self::TlsAuto(_) => false,
            Self::AdaptiveH2c(mode) => matches!(
                mode.load(Ordering::Relaxed),
                Self::H2C_INITIAL | Self::H2C_CONFIRMED
            ),
        }
    }

    /// Machine-readable transport label for `/healthz` (`"h2c"`, `"h2"`, or `"http/1.1"`).
    pub(crate) fn label(&self) -> &'static str {
        match self {
            Self::TrustedH1 => "http/1.1",
            Self::TlsAuto(obs) => obs.label(),
            Self::AdaptiveH2c(_) => {
                if self.uses_h2c_client() {
                    "h2c"
                } else {
                    "http/1.1"
                }
            }
        }
    }

    /// Human-readable protocol summary for startup lifecycle logging.
    pub(crate) fn startup_proto_label(&self) -> &'static str {
        match self {
            Self::TrustedH1 => "HTTP/1.1",
            Self::TlsAuto(obs) => {
                if obs.label() == "h2" {
                    "HTTP/2 (TLS)"
                } else {
                    "HTTP/1.1"
                }
            }
            Self::AdaptiveH2c(_) => {
                if self.uses_h2c_client() {
                    "HTTP/2 (h2c)"
                } else {
                    "HTTP/1.1"
                }
            }
        }
    }

    pub(crate) const fn tls_observer(&self) -> Option<&ObservedHttpVersion> {
        match self {
            Self::TlsAuto(obs) => Some(obs),
            Self::TrustedH1 | Self::AdaptiveH2c(_) => None,
        }
    }
}

/// One readiness signal: a pass/fail verdict plus a human-readable detail.
/// Used only by `/readyz`, which must render a verdict to gate traffic.
#[derive(Serialize)]
pub struct Check {
    pub ok: bool,
    pub detail: String,
}

/// `/healthz` view: a verdict-free, machine-readable snapshot of current EL + CL
/// state. Numeric fields carry the live values (peer counts, block / slot age,
/// sync status) for the consumer to threshold; any upstream failure is recorded
/// in the layer's `errors`, and unavailable or unpolled fields are omitted. This
/// endpoint **always returns 200** — it reports state, it does not judge it.
#[derive(Serialize)]
pub struct HealthSnapshot {
    pub mode: HealthMode,
    pub el: ElHealth,
    pub cl: ClHealth,
}

#[derive(Serialize, Default)]
pub struct ElHealth {
    /// HTTP version ethonyx uses for this upstream hop: `h2c` / `h2` / `http/1.1`.
    /// Reflects the poller's auto-detected EL transport verdict.
    pub transport: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub syncing: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sync_distance: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub peers: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block_number: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block_age_secs: Option<u64>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub errors: Vec<String>,
}

#[derive(Serialize, Default)]
pub struct ClHealth {
    /// HTTP version ethonyx uses for the Beacon hop. The CL hop never uses h2c
    /// prior-knowledge, so a cleartext upstream is always `http/1.1`; an `https`
    /// upstream may negotiate `h2` via ALPN, which is not separately tracked and
    /// is reported conservatively as `http/1.1`.
    pub transport: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub syncing: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sync_distance: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub peers: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub head_slot: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub slot_age_secs: Option<u64>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub errors: Vec<String>,
}

/// `/readyz` view: the load-balancer readiness verdict. Defaults to sync status
/// only; the freshness fields are populated (and gated on) only under
/// `--readyz-strict`. Peer count is intentionally never part of readiness — it
/// is a soft, fleet-correlated signal that belongs in `/healthz`.
#[derive(Serialize)]
pub struct ReadyReport {
    pub status: &'static str,
    pub el_syncing: Check,
    pub cl_syncing: Check,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub el_block_fresh: Option<Check>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cl_slot_fresh: Option<Check>,
}

#[derive(Clone, serde::Deserialize)]
struct ClStatus {
    #[serde(deserialize_with = "decimal_string")]
    head_slot: u64,
    #[serde(deserialize_with = "decimal_string")]
    sync_distance: u64,
    is_syncing: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ElSyncingResult {
    Synced(bool),
    Syncing {
        current_block: u64,
        highest_block: u64,
    },
}

impl<'de> serde::Deserialize<'de> for ElSyncingResult {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(serde::Deserialize)]
        struct SyncingFields {
            #[serde(rename = "currentBlock", deserialize_with = "hex_string")]
            current_block: u64,
            #[serde(rename = "highestBlock", deserialize_with = "hex_string")]
            highest_block: u64,
        }
        struct SyncingVisitor;
        impl<'de> serde::de::Visitor<'de> for SyncingVisitor {
            type Value = ElSyncingResult;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a boolean or an eth_syncing progress object")
            }
            fn visit_bool<E: serde::de::Error>(self, v: bool) -> Result<Self::Value, E> {
                Ok(ElSyncingResult::Synced(v))
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                map: A,
            ) -> Result<Self::Value, A::Error> {
                let fields = <SyncingFields as serde::Deserialize>::deserialize(
                    serde::de::value::MapAccessDeserializer::new(map),
                )?;
                Ok(ElSyncingResult::Syncing {
                    current_block: fields.current_block,
                    highest_block: fields.highest_block,
                })
            }
        }
        deserializer.deserialize_any(SyncingVisitor)
    }
}

#[derive(serde::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ElBlockResult {
    #[serde(deserialize_with = "hex_string")]
    pub number: u64,
    #[serde(deserialize_with = "hex_string")]
    pub timestamp: u64,
}

/// The five upstream probe results captured by one poll, shared with the request
/// handlers via a `watch` channel. `/healthz` and `/readyz` read the latest one
/// instead of querying upstream per request, so upstream load is decoupled from
/// probe rate. Stored as raw results so block / slot ages can be recomputed live.
#[derive(Clone)]
pub(crate) struct Probe {
    el_syncing: Result<ElSyncingResult, String>,
    el_peers: Result<u64, String>,
    el_block: Result<ElBlockResult, String>,
    cl_status: Result<ClStatus, String>,
    cl_peers: Result<u64, String>,
}

impl Probe {
    /// Placeholder used before the first poll lands. The cache is warmed with a
    /// real poll before any listener accepts, so this is never actually served.
    pub(crate) fn pending() -> Self {
        Probe {
            el_syncing: Err("warming up".into()),
            el_peers: Err("warming up".into()),
            el_block: Err("warming up".into()),
            cl_status: Err("warming up".into()),
            cl_peers: Err("warming up".into()),
        }
    }
}

const EL_BATCH_REQ: &[u8] = br#"[{"jsonrpc":"2.0","method":"eth_syncing","params":[],"id":1},{"jsonrpc":"2.0","method":"net_peerCount","params":[],"id":2},{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["latest",false],"id":3}]"#;

/// `/healthz` — verdict-free EL + CL snapshot, **always 200**. Serves the latest
/// background poll (see [`poll_loop`]); block / slot ages are recomputed live per
/// request. Under `--trust-upstream`, reports `mode: "trusted"` and omits
/// unpolled fields instead of fabricating placeholder metrics.
pub fn report(state: &AppState, head: bool) -> Response<ResBody> {
    if head {
        return empty_json_response(StatusCode::OK);
    }
    let snapshot = if state.cfg.trust_upstream {
        trusted_snapshot(state.el_transport.label(), state.cl_transport_label())
    } else {
        let probe = state.probe.borrow();
        let mut snap = build_snapshot(
            &probe.el_syncing,
            &probe.el_peers,
            &probe.el_block,
            &probe.cl_status,
            &probe.cl_peers,
            state.cl_genesis_time,
            state.cl_seconds_per_slot,
            now_unix(),
        );
        snap.el.transport = state.el_transport.label();
        snap.cl.transport = state.cl_transport_label();
        snap
    };
    json_response(StatusCode::OK, &snapshot)
}

fn trusted_snapshot(el_transport: &'static str, cl_transport: &'static str) -> HealthSnapshot {
    HealthSnapshot {
        mode: HealthMode::Trusted,
        el: ElHealth {
            transport: el_transport,
            ..Default::default()
        },
        cl: ClHealth {
            transport: cl_transport,
            ..Default::default()
        },
    }
}

/// `/readyz` — load-balancer readiness gate. 200 when ready, else 503.
///
/// By default this gates **only on EL + CL sync status**. A node that is caught
/// up reports `eth_syncing == false` even when the chain itself stalls
/// network-wide, so freshness/peer dips that hit the whole fleet at once do not
/// pull every backend out of rotation (which would turn a chain incident into a
/// total RPC outage). Sync status is the node-local signal that genuinely
/// distinguishes "this node can serve" from "this node is behind its peers".
///
/// With `--readyz-strict`, EL block age and CL slot age must also be within
/// their thresholds — choose this when serving strictly-at-head data matters
/// more than fleet availability during a network-wide stall.
pub fn ready(state: &AppState, head: bool) -> Response<ResBody> {
    let probe = state.probe.borrow();
    let report = evaluate_ready(state, &probe);
    let code = if report.status == "ready" {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    if head {
        empty_json_response(code)
    } else {
        json_response(code, &report)
    }
}

/// `/eth/v1/node/health` — Consensus Layer Beacon API standard node health endpoint.
///
/// Returns standard status codes according to the Ethereum Beacon API specification:
/// - 200 OK: Node is healthy and fully synced.
/// - 206 Partial Content: Both layers are reachable and at least one is actively syncing.
/// - 503 Service Unavailable: Node is unhealthy, stale (under `--readyz-strict`), or experiencing upstream errors.
///
/// Serves an empty body with `content-type: application/json` in compliance with the spec.
pub fn node_health(state: &AppState) -> Response<ResBody> {
    if state.cfg.trust_upstream {
        return empty_json_response(StatusCode::OK);
    }
    let probe = state.probe.borrow();
    let report = evaluate_ready(state, &probe);
    let code = classify_node_health(report.status == "ready", state.cfg.readyz_strict, &probe);
    empty_json_response(code)
}

fn classify_node_health(ready: bool, readyz_strict: bool, probe: &Probe) -> StatusCode {
    if ready {
        return StatusCode::OK;
    }
    let el_is_syncing = matches!(
        probe.el_syncing,
        Ok(ElSyncingResult::Synced(true) | ElSyncingResult::Syncing { .. })
    );
    let cl_is_syncing = matches!(&probe.cl_status, Ok(s) if s.is_syncing);
    let both_reachable = probe.el_syncing.is_ok()
        && probe.cl_status.is_ok()
        && (!readyz_strict || probe.el_block.is_ok());
    if both_reachable && (el_is_syncing || cl_is_syncing) {
        StatusCode::PARTIAL_CONTENT
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

/// Build the `/readyz` verdict from a probe snapshot. Default gates on EL + CL
/// sync only; `--readyz-strict` also gates on block / slot freshness. Shared by
/// the endpoint and the poller's transition logging.
pub(crate) fn evaluate_ready(state: &AppState, probe: &Probe) -> ReadyReport {
    if state.cfg.trust_upstream {
        return ReadyReport {
            status: "ready",
            el_syncing: Check {
                ok: true,
                detail: "trusted upstream (unpolled)".into(),
            },
            cl_syncing: Check {
                ok: true,
                detail: "trusted upstream (unpolled)".into(),
            },
            el_block_fresh: None,
            cl_slot_fresh: None,
        };
    }

    let el_syncing = check_el_syncing(&probe.el_syncing);
    let cl_syncing = check_cl_syncing(&probe.cl_status);
    // `now` is only needed for the strict freshness checks; the default
    // sync-only path doesn't read the clock.
    let (el_block_fresh, cl_slot_fresh) = if state.cfg.readyz_strict {
        let now = now_unix();
        (
            Some(check_el_block_fresh(
                &probe.el_block,
                state.cfg.el_max_block_age_secs,
                now,
            )),
            Some(check_cl_slot_fresh(
                &probe.cl_status,
                state.cl_genesis_time,
                state.cl_seconds_per_slot,
                state.cfg.cl_max_slot_age_secs,
                now,
            )),
        )
    } else {
        (None, None)
    };
    let all_ok = el_syncing.ok
        && cl_syncing.ok
        && el_block_fresh.as_ref().is_none_or(|c| c.ok)
        && cl_slot_fresh.as_ref().is_none_or(|c| c.ok);
    ReadyReport {
        status: if all_ok { "ready" } else { "not_ready" },
        el_syncing,
        cl_syncing,
        el_block_fresh,
        cl_slot_fresh,
    }
}

/// What [`poll_loop`] should log given the previous and current readiness, so a
/// sustained state logs once (on transition) rather than once per poll.
enum Transition {
    Silent,
    BecameNotReady,
    Recovered,
}

fn readiness_transition(prev_ready: Option<bool>, ready: bool) -> Transition {
    match prev_ready {
        Some(p) if p == ready => Transition::Silent,
        None if ready => Transition::Silent, // first poll already healthy: stay quiet
        _ if ready => Transition::Recovered,
        _ => Transition::BecameNotReady, // first-poll-degraded or ready -> not-ready
    }
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
    Probe {
        el_syncing: batch.syncing.map_err(|e| e.to_string()),
        el_peers: batch.peers.map_err(|e| e.to_string()),
        el_block: batch.block.map_err(|e| e.to_string()),
        cl_status,
        cl_peers,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum HealthError {
    Timeout,
    Transport(String),
    HttpStatus(http::StatusCode),
    Decode(String),
    Rpc(String),
}

impl std::fmt::Display for HealthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Timeout => write!(f, "timeout"),
            Self::Transport(s) => write!(f, "transport: {s}"),
            Self::HttpStatus(status) => write!(f, "http {status}"),
            Self::Decode(s) => write!(f, "decode: {s}"),
            Self::Rpc(s) => write!(f, "rpc error: {s}"),
        }
    }
}

struct ElBatchResult {
    syncing: Result<ElSyncingResult, HealthError>,
    peers: Result<u64, HealthError>,
    block: Result<ElBlockResult, HealthError>,
}

impl ElBatchResult {
    fn from_error(error: HealthError) -> Self {
        Self {
            syncing: Err(error.clone()),
            peers: Err(error.clone()),
            block: Err(error),
        }
    }
}

/// Query all three EL JSON-RPC methods in a batch request, maintaining the transport verdict.
async fn el_batch_detect(state: &AppState) -> ElBatchResult {
    let payload = Bytes::from_static(EL_BATCH_REQ);
    let result = match &state.el_transport {
        ElTransport::TrustedH1 | ElTransport::TlsAuto(_) => {
            el_batch_rpc(state, payload, &state.client).await
        }
        ElTransport::AdaptiveH2c(mode) => match mode.load(Ordering::Relaxed) {
            cur @ (ElTransport::H2C_INITIAL | ElTransport::H2C_CONFIRMED) => {
                match el_batch_rpc(state, payload.clone(), &state.el_h2_client).await {
                    Err(primary_err) if is_transport(&primary_err) => {
                        let alt = el_batch_rpc(state, payload, &state.client).await;
                        if matches!(&alt, Err(e) if is_transport(e)) {
                            Err(primary_err)
                        } else {
                            mode.store(ElTransport::H1_VERIFY_ONCE, Ordering::Relaxed);
                            if cur == ElTransport::H2C_CONFIRMED {
                                warn!("EL JSON-RPC upstream stopped speaking h2c — using HTTP/1.1");
                            }
                            alt
                        }
                    }
                    reached => {
                        if cur == ElTransport::H2C_INITIAL {
                            mode.store(ElTransport::H2C_CONFIRMED, Ordering::Relaxed);
                        }
                        reached
                    }
                }
            }
            ElTransport::H1_VERIFY_ONCE => {
                // Verify h2c one more time to self-heal if an h2c upstream was
                // probed mid-bind during startup or restart.
                let h2_try = el_batch_rpc(state, payload.clone(), &state.el_h2_client).await;
                if !matches!(&h2_try, Err(e) if is_transport(e)) {
                    mode.store(ElTransport::H2C_CONFIRMED, Ordering::Relaxed);
                    info!("EL JSON-RPC upstream speaks h2c — using HTTP/2");
                    h2_try
                } else {
                    let h1_try = el_batch_rpc(state, payload, &state.client).await;
                    if !matches!(&h1_try, Err(e) if is_transport(e)) {
                        mode.store(ElTransport::H1_CONFIRMED, Ordering::Relaxed);
                    }
                    h1_try
                }
            }
            _ => match el_batch_rpc(state, payload.clone(), &state.client).await {
                Err(primary_err) if is_transport(&primary_err) => {
                    let alt = el_batch_rpc(state, payload, &state.el_h2_client).await;
                    if matches!(&alt, Err(e) if is_transport(e)) {
                        Err(primary_err)
                    } else {
                        mode.store(ElTransport::H2C_CONFIRMED, Ordering::Relaxed);
                        info!("EL JSON-RPC upstream speaks h2c — using HTTP/2");
                        alt
                    }
                }
                reached => reached,
            },
        },
    };
    result.unwrap_or_else(ElBatchResult::from_error)
}

/// A hard transport/connection failure or a timeout (vs. an HTTP status or decode
/// error) — the only signal that the chosen HTTP version might be wrong for this
/// upstream. A timeout is included so that if the upstream hangs on the HTTP/2
/// connection preface, we can fall back to HTTP/1.1 if the alternate client succeeds.
/// If both timeout, we do not switch.
const fn is_transport(e: &HealthError) -> bool {
    matches!(e, HealthError::Transport(_) | HealthError::Timeout)
}

pub(crate) fn update_metrics(probe: &Probe, report: &ReadyReport) {
    ::metrics::gauge!("ethonyx_upstream_health_status", "layer" => "el")
        .set(if report.el_syncing.ok { 1.0 } else { 0.0 });
    ::metrics::gauge!("ethonyx_upstream_health_status", "layer" => "cl")
        .set(if report.cl_syncing.ok { 1.0 } else { 0.0 });

    ::metrics::gauge!("ethonyx_upstream_peers", "layer" => "el")
        .set(probe.el_peers.as_ref().map_or(0.0, |&p| p as f64));
    ::metrics::gauge!("ethonyx_upstream_peers", "layer" => "cl")
        .set(probe.cl_peers.as_ref().map_or(0.0, |&p| p as f64));

    let el_distance = match &probe.el_syncing {
        Ok(ElSyncingResult::Syncing {
            current_block,
            highest_block,
        }) => highest_block.saturating_sub(*current_block) as f64,
        _ => 0.0,
    };
    ::metrics::gauge!("ethonyx_upstream_sync_distance", "layer" => "el").set(el_distance);

    if let Ok(status) = &probe.cl_status {
        ::metrics::gauge!("ethonyx_upstream_sync_distance", "layer" => "cl")
            .set(status.sync_distance as f64);
        ::metrics::gauge!("ethonyx_upstream_slot_number").set(status.head_slot as f64);
    } else {
        ::metrics::gauge!("ethonyx_upstream_sync_distance", "layer" => "cl").set(0.0);
    }

    if let Ok(block) = &probe.el_block {
        ::metrics::gauge!("ethonyx_upstream_block_number").set(block.number as f64);
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
    mut shutdown: crate::lifecycle::Shutdown,
    interval: Duration,
) {
    let mut prev_ready: Option<bool> = None;
    loop {
        tokio::select! {
            biased;
            _ = crate::lifecycle::deadline(&mut shutdown) => return,
            _ = tokio::time::sleep(interval) => {}
        }
        let probe = tokio::select! {
            biased;
            _ = crate::lifecycle::deadline(&mut shutdown) => return,
            probe = probe_once(&state) => probe,
        };

        let report = evaluate_ready(&state, &probe);
        let ready = report.status == "ready";
        debug!(status = report.status, "health poll");

        update_metrics(&probe, &report);

        match readiness_transition(prev_ready, ready) {
            Transition::Silent => {}
            Transition::Recovered => info!("readiness recovered"),
            Transition::BecameNotReady => {
                let el_block = report
                    .el_block_fresh
                    .as_ref()
                    .map_or("-", |c| c.detail.as_str());
                let cl_slot = report
                    .cl_slot_fresh
                    .as_ref()
                    .map_or("-", |c| c.detail.as_str());
                warn!(
                    el_syncing = %report.el_syncing.detail,
                    cl_syncing = %report.cl_syncing.detail,
                    el_block,
                    cl_slot,
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

fn empty_json_response(code: StatusCode) -> Response<ResBody> {
    Response::builder()
        .status(code)
        .header("content-type", "application/json")
        .body(box_full(Full::new(Bytes::new())))
        .expect("response builder")
}

fn json_response<T: Serialize>(code: StatusCode, body: &T) -> Response<ResBody> {
    let body_bytes = serde_json::to_vec(body).unwrap_or_else(|_| b"{}".to_vec());
    Response::builder()
        .status(code)
        .header("content-type", "application/json")
        .body(box_full(Full::new(Bytes::from(body_bytes))))
        .expect("response builder")
}

// ---- readiness verdicts (Check): sync is a fact; freshness uses thresholds ----

fn check_el_syncing(r: &Result<ElSyncingResult, String>) -> Check {
    match r {
        Ok(ElSyncingResult::Synced(false)) => Check {
            ok: true,
            detail: "synced".into(),
        },
        Ok(ElSyncingResult::Synced(true)) => Check {
            ok: false,
            detail: "syncing (true)".into(),
        },
        Ok(ElSyncingResult::Syncing {
            current_block,
            highest_block,
        }) => {
            let distance = highest_block.saturating_sub(*current_block);
            Check {
                ok: false,
                detail: format!("syncing (block {current_block}, distance {distance})"),
            }
        }
        Err(e) => Check {
            ok: false,
            detail: format!("eth_syncing: {e}"),
        },
    }
}

fn check_cl_syncing(r: &Result<ClStatus, String>) -> Check {
    match r {
        Ok(s) if !s.is_syncing => Check {
            ok: true,
            detail: format!(
                "synced (slot {}, distance {})",
                s.head_slot, s.sync_distance
            ),
        },
        Ok(s) => Check {
            ok: false,
            detail: format!(
                "syncing (slot {}, distance {})",
                s.head_slot, s.sync_distance
            ),
        },
        Err(e) => Check {
            ok: false,
            detail: format!("node/syncing: {e}"),
        },
    }
}

fn check_el_block_fresh(r: &Result<ElBlockResult, String>, max_age: u64, now: u64) -> Check {
    match r {
        Ok(block) => {
            let age = now.saturating_sub(block.timestamp);
            if age <= max_age {
                Check {
                    ok: true,
                    detail: format!("block {}, age {age}s", block.number),
                }
            } else {
                Check {
                    ok: false,
                    detail: format!("block {} stale: {age}s (max {max_age})", block.number),
                }
            }
        }
        Err(e) => Check {
            ok: false,
            detail: format!("eth_getBlockByNumber: {e}"),
        },
    }
}

fn check_cl_slot_fresh(
    r: &Result<ClStatus, String>,
    genesis: u64,
    seconds_per_slot: u64,
    max_age: u64,
    now: u64,
) -> Check {
    match r {
        Ok(s) if genesis == 0 => Check {
            ok: true,
            detail: format!("slot {} (age check disabled)", s.head_slot),
        },
        Ok(s) => {
            let age = slot_age(s.head_slot, genesis, seconds_per_slot, now);
            if age <= max_age {
                Check {
                    ok: true,
                    detail: format!("slot {}, age {age}s", s.head_slot),
                }
            } else {
                Check {
                    ok: false,
                    detail: format!("slot {} stale: {age}s (max {max_age})", s.head_slot),
                }
            }
        }
        Err(e) => Check {
            ok: false,
            detail: format!("node/syncing: {e}"),
        },
    }
}

// ---- /healthz: verdict-free numeric snapshot ----

/// Fold the five upstream probe results into a numeric snapshot. Successful
/// signals populate their fields; failed ones are pushed onto the layer's
/// `errors` and leave the field unset.
#[allow(clippy::too_many_arguments)]
fn build_snapshot(
    sync_r: &Result<ElSyncingResult, String>,
    peers_r: &Result<u64, String>,
    block_r: &Result<ElBlockResult, String>,
    cl_status_r: &Result<ClStatus, String>,
    cl_peers_r: &Result<u64, String>,
    genesis: u64,
    seconds_per_slot: u64,
    now: u64,
) -> HealthSnapshot {
    let mut el = ElHealth::default();
    match sync_r {
        Ok(ElSyncingResult::Synced(false)) => el.syncing = Some(false),
        Ok(ElSyncingResult::Synced(true)) => el.syncing = Some(true),
        Ok(ElSyncingResult::Syncing {
            current_block,
            highest_block,
        }) => {
            el.syncing = Some(true);
            el.sync_distance = Some(highest_block.saturating_sub(*current_block));
        }
        Err(e) => el.errors.push(format!("eth_syncing: {e}")),
    }
    match peers_r {
        Ok(n) => el.peers = Some(*n),
        Err(e) => el.errors.push(format!("net_peerCount: {e}")),
    }
    match block_r {
        Ok(block) => {
            el.block_number = Some(block.number);
            el.block_age_secs = Some(now.saturating_sub(block.timestamp));
        }
        Err(e) => el.errors.push(format!("eth_getBlockByNumber: {e}")),
    }

    let mut cl = ClHealth::default();
    match cl_status_r {
        Ok(s) => {
            cl.syncing = Some(s.is_syncing);
            cl.sync_distance = Some(s.sync_distance);
            cl.head_slot = Some(s.head_slot);
            if genesis != 0 {
                cl.slot_age_secs = Some(slot_age(s.head_slot, genesis, seconds_per_slot, now));
            }
        }
        Err(e) => cl.errors.push(format!("node/syncing: {e}")),
    }
    match cl_peers_r {
        Ok(n) => cl.peers = Some(*n),
        Err(e) => cl.errors.push(format!("node/peer_count: {e}")),
    }

    HealthSnapshot {
        mode: HealthMode::Probed,
        el,
        cl,
    }
}

fn slot_age(head_slot: u64, genesis: u64, seconds_per_slot: u64, now: u64) -> u64 {
    // Saturate: a garbage `head_slot` from upstream must not overflow-panic
    // (debug) or wrap (release).
    let expected = genesis.saturating_add(head_slot.saturating_mul(seconds_per_slot));
    now.saturating_sub(expected)
}

/// Send a prepared probe request with the health-poll timeout, record any TLS
/// negotiated protocol version as soon as response headers arrive, and return
/// the body of a successful response.
async fn fetch_bytes(
    state: &AppState,
    req: Request<ResBody>,
    client: &ProxyClient,
    observed_tls: Option<&ObservedHttpVersion>,
) -> Result<Bytes, HealthError> {
    tokio::time::timeout(state.cfg.health_timeout, async {
        let resp = client
            .request(req)
            .await
            .map_err(|e| HealthError::Transport(e.to_string()))?;
        if let Some(obs) = observed_tls {
            obs.record(resp.version());
        }
        if !resp.status().is_success() {
            return Err(HealthError::HttpStatus(resp.status()));
        }
        http_body_util::Limited::new(resp.into_body(), 10 * 1024 * 1024)
            .collect()
            .await
            .map(|c| c.to_bytes())
            .map_err(|e| HealthError::Decode(e.to_string()))
    })
    .await
    .map_err(|_| HealthError::Timeout)?
}

async fn el_batch_rpc(
    state: &AppState,
    payload: Bytes,
    client: &ProxyClient,
) -> Result<ElBatchResult, HealthError> {
    let req = Request::builder()
        .method(Method::POST)
        .uri(state.el_http.uri().clone())
        .header("content-type", "application/json")
        .body(box_full(Full::new(payload)))
        .map_err(|e| HealthError::Transport(format!("build: {e}")))?;
    let body = fetch_bytes(state, req, client, state.el_transport.tls_observer()).await?;
    parse_el_batch(&body)
}

fn parse_el_batch(body: &[u8]) -> Result<ElBatchResult, HealthError> {
    #[derive(serde::Deserialize)]
    struct RpcResponse<'a> {
        id: i64,
        #[serde(borrow)]
        result: Option<&'a serde_json::value::RawValue>,
        #[serde(borrow)]
        error: Option<&'a serde_json::value::RawValue>,
    }

    #[derive(serde::Deserialize)]
    #[serde(transparent)]
    struct HexQuantity(#[serde(deserialize_with = "hex_string")] u64);

    let arr: [JsonObject<RpcResponse<'_>>; 3] =
        serde_json::from_slice(body).map_err(|e| HealthError::Decode(e.to_string()))?;
    let mut slots: [Option<RpcResponse<'_>>; 3] = [None, None, None];
    for JsonObject(resp) in arr {
        let Some(idx) = usize::try_from(resp.id)
            .ok()
            .and_then(|id| id.checked_sub(1))
            .filter(|&idx| idx < 3)
        else {
            return Err(HealthError::Decode(
                "duplicate or unexpected response id".into(),
            ));
        };
        if slots[idx].replace(resp).is_some() {
            return Err(HealthError::Decode(
                "duplicate or unexpected response id".into(),
            ));
        }
    }

    let decode_res = |idx: usize| -> Result<&serde_json::value::RawValue, HealthError> {
        let resp = slots[idx]
            .as_ref()
            .ok_or_else(|| HealthError::Decode(format!("missing response for id {}", idx + 1)))?;
        if let Some(err) = resp.error {
            return Err(HealthError::Rpc(err.get().to_owned()));
        }
        resp.result
            .ok_or_else(|| HealthError::Decode("missing result".into()))
    };

    let syncing = decode_res(0).and_then(|raw| {
        serde_json::from_str(raw.get()).map_err(|e| HealthError::Decode(e.to_string()))
    });
    let peers = decode_res(1).and_then(|raw| {
        serde_json::from_str::<HexQuantity>(raw.get())
            .map(|HexQuantity(n)| n)
            .map_err(|e| HealthError::Decode(e.to_string()))
    });
    let block = decode_res(2).and_then(|raw| {
        serde_json::from_str::<JsonObject<ElBlockResult>>(raw.get())
            .map(|JsonObject(block)| block)
            .map_err(|e| HealthError::Decode(e.to_string()))
    });

    Ok(ElBatchResult {
        syncing,
        peers,
        block,
    })
}

// Serde's derived struct visitors also accept positional sequences. Enforce
// object shape at each upstream boundary while retaining borrowed RawValue data.
struct JsonObject<T>(T);

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

#[derive(serde::Deserialize)]
struct ClEnvelope<T> {
    data: JsonObject<T>,
}

fn parse_cl_response<T: serde::de::DeserializeOwned>(body: &[u8]) -> Result<T, String> {
    serde_json::from_slice::<JsonObject<ClEnvelope<T>>>(body)
        .map(|JsonObject(envelope)| envelope.data.0)
        .map_err(|e| format!("decode: {e}"))
}

fn decimal_string<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<u64, D::Error> {
    struct Decimal;
    impl serde::de::Visitor<'_> for Decimal {
        type Value = u64;
        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("an unsigned decimal string")
        }
        fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<u64, E> {
            value.parse().map_err(E::custom)
        }
    }
    deserializer.deserialize_str(Decimal)
}

fn hex_string<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<u64, D::Error> {
    struct Hex;
    impl serde::de::Visitor<'_> for Hex {
        type Value = u64;
        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("a 0x-prefixed hex quantity string")
        }
        fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<u64, E> {
            hex_to_u64(value).ok_or_else(|| E::custom(format!("invalid hex quantity: {value}")))
        }
    }
    deserializer.deserialize_str(Hex)
}

async fn cl_syncing_status(state: &AppState) -> Result<ClStatus, String> {
    cl_get_json(state, &state.cl_syncing_uri).await
}

async fn cl_peer_count(state: &AppState) -> Result<u64, String> {
    #[derive(serde::Deserialize)]
    struct Peers {
        #[serde(deserialize_with = "decimal_string")]
        connected: u64,
    }
    let peers: Peers = cl_get_json(state, &state.cl_peer_count_uri).await?;
    Ok(peers.connected)
}

async fn cl_get_json<T: serde::de::DeserializeOwned>(
    state: &AppState,
    uri: &Uri,
) -> Result<T, String> {
    let req = Request::builder()
        .method(Method::GET)
        .uri(uri.clone())
        .body(box_full(Full::new(Bytes::new())))
        .map_err(|e| e.to_string())?;
    let body = fetch_bytes(state, req, &state.client, state.cl_tls_version.as_ref())
        .await
        .map_err(|e| e.to_string())?;
    parse_cl_response(&body)
}

fn hex_to_u64(s: &str) -> Option<u64> {
    let digits = s.strip_prefix("0x")?;
    if digits.is_empty()
        || (digits.len() > 1 && digits.starts_with('0'))
        || !digits.bytes().all(|b| b.is_ascii_hexdigit())
    {
        return None;
    }
    u64::from_str_radix(digits, 16).ok()
}

fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn el_transport_fallback_preserves_verdict_and_error_policy() {
        use clap::Parser;
        use hyper::body::Incoming;
        use hyper::service::service_fn;
        use hyper_util::rt::{TokioExecutor, TokioIo};
        use hyper_util::server::conn::auto;
        use std::sync::atomic::AtomicUsize;

        let valid = br#"[{"id":1,"result":false},{"id":2,"result":"0x40"},{"id":3,"result":{"number":"0x1","timestamp":"0x2"}}]"#;
        // Preferred transport, accepted [h1, h2], response, expected transport.
        for (prefer_h2, accepted, status, body, expected_h2) in [
            (true, [true, true], 200, valid.as_slice(), true),
            (true, [true, true], 503, b"unavailable".as_slice(), true),
            (true, [true, true], 200, b"invalid JSON".as_slice(), true),
            (false, [true, true], 503, b"unavailable".as_slice(), false),
            (true, [true, false], 200, valid.as_slice(), false),
            (true, [true, false], 503, b"unavailable".as_slice(), false),
            (true, [true, false], 200, b"invalid JSON".as_slice(), false),
            (false, [false, true], 200, valid.as_slice(), true),
            (false, [false, true], 503, b"unavailable".as_slice(), true),
            (false, [false, true], 200, b"invalid JSON".as_slice(), true),
            (true, [false, false], 200, valid.as_slice(), true),
            (false, [false, false], 200, valid.as_slice(), false),
        ] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            let attempts = Arc::new([AtomicUsize::new(0), AtomicUsize::new(0)]);
            let counts = attempts.clone();
            let server = tokio::spawn(async move {
                loop {
                    let (stream, _) = listener.accept().await.unwrap();
                    let counts = counts.clone();
                    tokio::spawn(async move {
                        let mut preface = [0; 3];
                        let h2 =
                            stream.peek(&mut preface).await.unwrap() == 3 && &preface == b"PRI";
                        counts[usize::from(h2)].fetch_add(1, Ordering::Relaxed);
                        if !accepted[usize::from(h2)] {
                            // On total failure distinguish the original h2 timeout
                            // from the alternate h1 connection error.
                            if h2 && !accepted.contains(&true) {
                                tokio::time::sleep(Duration::from_secs(2)).await;
                            }
                            return;
                        }
                        let svc = service_fn(move |req: Request<Incoming>| async move {
                            req.into_body().collect().await.unwrap();
                            Ok::<_, std::convert::Infallible>(
                                Response::builder()
                                    .status(status)
                                    .body(Full::new(Bytes::from_static(body)))
                                    .unwrap(),
                            )
                        });
                        let _ = auto::Builder::new(TokioExecutor::new())
                            .serve_connection(TokioIo::new(stream), svc)
                            .await;
                    });
                }
            });
            let mut cfg = crate::Config::parse_from(["ethonyx", "--el-http-url", &url]);
            cfg.health_timeout = Duration::from_secs(1);
            let (_, probe) = watch::channel(Arc::new(Probe::pending()));
            let state = AppState {
                el_http: crate::upstream::HttpUpstream::parse("--el-http-url", &url).unwrap(),
                el_ws: crate::upstream::WsUpstream::parse("--el-ws-url", "ws://127.0.0.1:8546")
                    .unwrap(),
                cl_beacon: crate::upstream::HttpUpstream::parse("--cl-beacon-url", &url).unwrap(),
                cl_mev_relay: None,
                cl_syncing_uri: url.parse().unwrap(),
                cl_peer_count_uri: url.parse().unwrap(),
                cfg,
                client: crate::proxy::build_client(false),
                el_h2_client: crate::proxy::build_client(true),
                el_transport: ElTransport::confirmed_cleartext(prefer_h2),
                cl_tls_version: None,
                otel_enabled: false,
                cl_genesis_time: 0,
                cl_seconds_per_slot: 12,
                probe,
            };
            let result = el_batch_detect(&state).await;
            assert_eq!(state.el_transport.uses_h2c_client(), expected_h2);
            assert_eq!(attempts[usize::from(prefer_h2)].load(Ordering::Relaxed), 1);
            assert_eq!(
                attempts[usize::from(!prefer_h2)].load(Ordering::Relaxed),
                usize::from(!accepted[usize::from(prefer_h2)])
            );
            if !accepted.contains(&true) {
                let error = result.syncing.unwrap_err();
                if prefer_h2 {
                    assert_eq!(error, HealthError::Timeout);
                } else {
                    assert!(matches!(error, HealthError::Transport(_)));
                }
                assert_eq!(result.peers.unwrap_err(), error);
                assert_eq!(result.block.unwrap_err(), error);
            } else if status == 503 {
                assert_eq!(
                    result.syncing.unwrap_err(),
                    HealthError::HttpStatus(StatusCode::SERVICE_UNAVAILABLE)
                );
            } else if body == valid {
                assert_eq!(result.syncing.unwrap(), ElSyncingResult::Synced(false));
                assert_eq!(result.peers.unwrap(), 0x40);
            } else {
                assert!(matches!(result.syncing, Err(HealthError::Decode(_))));
            }
            server.abort();
        }
    }

    #[test]
    fn beacon_envelope_and_data_require_objects() {
        for input in [
            r#"[["123","0",false]]"#,
            r#"{"data":["123","0",false]}"#,
            r#"[{"head_slot":"123","sync_distance":"0","is_syncing":false}]"#,
            r#"{"data":null}"#,
            r#"{"data":{"head_slot":"123","sync_distance":"0","is_syncing":false},"data":{}}"#,
            r#"{"data":{"head_slot":"123","head_slot":"123","sync_distance":"0","is_syncing":false}}"#,
            r#"{"data":{"head_slot":"123","sync_distance":"0","is_syncing":false}}x"#,
        ] {
            assert!(
                parse_cl_response::<ClStatus>(input.as_bytes()).is_err(),
                "{input}"
            );
        }
        assert!(parse_cl_response::<ClStatus>(br#"{"extra":[],"data":{"head_slot":"123","sync_distance":"0","is_syncing":false,"ignored":true}}"#).is_ok());
    }

    #[test]
    fn el_batch_requires_three_object_responses_and_object_blocks() {
        let replies = [
            r#"{"id":1,"result":false}"#,
            r#"{"id":2,"result":"0x40"}"#,
            r#"{"id":3,"result":{"number":"0x1","timestamp":"0x2"}}"#,
        ];
        for order in [
            [0, 1, 2],
            [0, 2, 1],
            [1, 0, 2],
            [1, 2, 0],
            [2, 0, 1],
            [2, 1, 0],
        ] {
            let input = format!("[{}]", order.map(|i| replies[i]).join(","));
            let parsed = parse_el_batch(input.as_bytes()).unwrap();
            assert_eq!(parsed.syncing.unwrap(), ElSyncingResult::Synced(false));
            assert_eq!(parsed.peers.unwrap(), 0x40);
            assert_eq!(parsed.block.unwrap().timestamp, 2);
        }
        for input in [
            format!("[{},{}]", replies[0], replies[1]),
            format!(
                "[{},{},{},{}]",
                replies[0], replies[1], replies[2], replies[0]
            ),
            r#"[[1,false,null],{"id":2,"result":"0x40"},{"id":3,"result":{}}]"#.into(),
            r#"[[1,false,null],[2,"0x40",null],[3,{"number":"0x1","timestamp":"0x2"},null]]"#
                .into(),
        ] {
            assert!(parse_el_batch(input.as_bytes()).is_err(), "{input}");
        }
        let parsed = parse_el_batch(br#"[{"id":1,"result":false},{"id":2,"result":"0x40"},{"id":3,"result":["0x1","0x2"]}]"#).unwrap();
        assert!(parsed.syncing.is_ok());
        assert!(parsed.peers.is_ok());
        assert!(parsed.block.is_err());
        assert!(serde_json::from_str::<ElSyncingResult>(r#"["0x1","0x2"]"#).is_err());
    }

    #[test]
    fn el_batch_matches_ids_in_any_order_and_keeps_partial_results() {
        let parsed = parse_el_batch(br#"[{"id":3,"result":{"number":"0x42","timestamp":"0x43"}},{"id":1,"result":false},{"id":2,"error":{"code":-32000,"message":"unavailable"}}]"#).unwrap();
        assert_eq!(parsed.syncing.unwrap(), ElSyncingResult::Synced(false));
        assert_eq!(parsed.block.unwrap().number, 0x42);
        assert!(matches!(parsed.peers, Err(HealthError::Rpc(_))));
    }

    #[test]
    fn el_batch_rejects_ambiguous_ids_and_invalid_results() {
        for input in [
            r#"[]"#,
            r#"[{"id":1,"result":false},{"id":1,"result":false},{"id":3,"result":null}]"#,
            r#"[{"id":1,"result":false},{"id":2,"result":"0x1"},{"id":4,"result":null}]"#,
            r#"[{"id":"1","result":false},{"id":2,"result":"0x1"},{"id":3,"result":null}]"#,
        ] {
            assert!(
                matches!(
                    parse_el_batch(input.as_bytes()),
                    Err(HealthError::Decode(_))
                ),
                "{input}"
            );
        }
        let parsed =
            parse_el_batch(br#"[{"id":1,"result":null},{"id":2},{"id":3,"result":{}}]"#).unwrap();
        assert!(parsed.syncing.is_err());
        assert!(parsed.peers.is_err());
        assert!(parsed.block.is_err());
    }

    #[test]
    fn hex_with_0x_prefix() {
        assert_eq!(hex_to_u64("0x10"), Some(16));
        assert_eq!(hex_to_u64("0xff"), Some(255));
        assert_eq!(hex_to_u64("0x0"), Some(0));
    }

    #[test]
    fn hex_rejects_non_canonical_or_malformed_quantities() {
        for invalid in [
            "",
            "0x",
            "0X10",
            "ff",
            "a",
            "0x00",
            "0x01",
            "0x+10",
            "0x-10",
            "0x0x10",
            "0xZZ",
            "not hex",
            "0x10000000000000000",
        ] {
            assert_eq!(hex_to_u64(invalid), None, "{invalid}");
        }
    }

    #[test]
    fn hex_handles_block_timestamp_width() {
        assert_eq!(hex_to_u64("0x671E0000"), Some(0x671E_0000));
    }

    #[test]
    fn typed_beacon_response_preserves_decimal_semantics() {
        let response: ClEnvelope<ClStatus> = serde_json::from_str(r#"{"data":{"head_slot":"9412341","sync_distance":"0","is_syncing":false,"extra":{"ignored":true}}}"#).unwrap();
        assert_eq!(response.data.0.head_slot, 9_412_341);
        assert_eq!(response.data.0.sync_distance, 0);
        for value in [
            "42",
            "null",
            r#""invalid""#,
            r#""-1""#,
            r#""18446744073709551616""#,
        ] {
            let text = format!(
                r#"{{"data":{{"head_slot":{value},"sync_distance":"0","is_syncing":false}}}}"#
            );
            assert!(
                serde_json::from_str::<ClEnvelope<ClStatus>>(&text).is_err(),
                "{text}"
            );
        }
        assert!(
            serde_json::from_str::<ClEnvelope<ClStatus>>(
                r#"{"data":{"sync_distance":"0","is_syncing":false}}"#
            )
            .is_err()
        );
    }

    #[test]
    fn el_syncing_false_is_synced() {
        let c = check_el_syncing(&Ok(ElSyncingResult::Synced(false)));
        assert!(c.ok);
        assert_eq!(c.detail, "synced");
    }

    #[test]
    fn el_syncing_object_reports_block_progress() {
        let c = check_el_syncing(&Ok(ElSyncingResult::Syncing {
            current_block: 0x10,
            highest_block: 0x20,
        }));
        assert!(!c.ok);
        assert_eq!(c.detail, "syncing (block 16, distance 16)");
    }

    #[test]
    fn el_syncing_error_is_prefixed() {
        let c = check_el_syncing(&Err("boom".into()));
        assert!(!c.ok);
        assert_eq!(c.detail, "eth_syncing: boom");
    }

    #[test]
    fn cl_syncing_synced_is_ok() {
        let s = ClStatus {
            head_slot: 42,
            sync_distance: 0,
            is_syncing: false,
        };
        let c = check_cl_syncing(&Ok(s));
        assert!(c.ok);
        assert_eq!(c.detail, "synced (slot 42, distance 0)");
    }

    #[test]
    fn cl_syncing_in_progress_fails() {
        let s = ClStatus {
            head_slot: 42,
            sync_distance: 5,
            is_syncing: true,
        };
        let c = check_cl_syncing(&Ok(s));
        assert!(!c.ok);
        assert_eq!(c.detail, "syncing (slot 42, distance 5)");
    }

    #[test]
    fn el_block_fresh_within_threshold_is_ok() {
        let block = ElBlockResult {
            number: 5,
            timestamp: 0x3e2,
        };
        let c = check_el_block_fresh(&Ok(block), 60, 1000);
        assert!(c.ok);
        assert_eq!(c.detail, "block 5, age 6s");
    }

    #[test]
    fn el_block_fresh_past_threshold_is_stale() {
        let block = ElBlockResult {
            number: 5,
            timestamp: 0x384,
        };
        let c = check_el_block_fresh(&Ok(block), 60, 1000);
        assert!(!c.ok);
        assert_eq!(c.detail, "block 5 stale: 100s (max 60)");
    }

    #[test]
    fn cl_slot_fresh_disabled_when_genesis_zero() {
        let s = ClStatus {
            head_slot: 7,
            sync_distance: 0,
            is_syncing: false,
        };
        let c = check_cl_slot_fresh(&Ok(s), 0, 12, 60, 9_999_999);
        assert!(c.ok);
        assert_eq!(c.detail, "slot 7 (age check disabled)");
    }

    #[test]
    fn cl_slot_fresh_past_threshold_is_stale() {
        let s = ClStatus {
            head_slot: 10,
            sync_distance: 0,
            is_syncing: false,
        };
        let c = check_cl_slot_fresh(&Ok(s), 1000, 12, 60, 1300);
        assert!(!c.ok);
        assert_eq!(c.detail, "slot 10 stale: 180s (max 60)");
    }

    #[test]
    fn slot_age_computes_wall_clock_age() {
        assert_eq!(slot_age(10, 1000, 12, 1300), 180);
    }

    #[test]
    fn slot_age_saturates_on_garbage_head_slot() {
        assert_eq!(slot_age(u64::MAX, 1_606_824_023, 12, 2_000_000_000), 0);
    }

    #[test]
    fn readiness_transitions_log_only_on_change() {
        use Transition::*;
        assert!(matches!(readiness_transition(None, false), BecameNotReady));
        assert!(matches!(readiness_transition(None, true), Silent));
        assert!(matches!(readiness_transition(Some(true), true), Silent));
        assert!(matches!(readiness_transition(Some(false), false), Silent));
        assert!(matches!(
            readiness_transition(Some(true), false),
            BecameNotReady
        ));
        assert!(matches!(readiness_transition(Some(false), true), Recovered));
    }

    #[test]
    fn snapshot_reports_numeric_values_when_healthy() {
        let snap = build_snapshot(
            &Ok(ElSyncingResult::Synced(false)),
            &Ok(0x10),
            &Ok(ElBlockResult {
                number: 5,
                timestamp: 0x3e2,
            }),
            &Ok(ClStatus {
                head_slot: 10,
                sync_distance: 0,
                is_syncing: false,
            }),
            &Ok(64u64),
            1000,
            12,
            1300,
        );
        assert_eq!(snap.el.syncing, Some(false));
        assert_eq!(snap.el.peers, Some(16));
        assert_eq!(snap.el.block_number, Some(5));
        assert_eq!(snap.el.block_age_secs, Some(306));
        assert!(snap.el.errors.is_empty());
        assert_eq!(snap.cl.syncing, Some(false));
        assert_eq!(snap.cl.peers, Some(64));
        assert_eq!(snap.cl.head_slot, Some(10));
        assert_eq!(snap.cl.slot_age_secs, Some(180));
        assert!(snap.el.errors.is_empty());
    }

    #[test]
    fn snapshot_records_errors_and_omits_values_on_failure() {
        let snap = build_snapshot(
            &Err("timeout".to_string()),
            &Err("http 500".to_string()),
            &Err("timeout".to_string()),
            &Err("http 404".to_string()),
            &Err("http 404".to_string()),
            1000,
            12,
            1300,
        );
        assert_eq!(snap.el.syncing, None);
        assert_eq!(snap.el.peers, None);
        assert_eq!(snap.el.block_number, None);
        assert_eq!(snap.el.errors.len(), 3);
        assert_eq!(snap.cl.head_slot, None);
        assert_eq!(snap.cl.errors.len(), 2);
    }

    #[test]
    fn snapshot_reports_el_sync_distance_while_syncing() {
        let snap = build_snapshot(
            &Ok(ElSyncingResult::Syncing {
                current_block: 0x10,
                highest_block: 0x20,
            }),
            &Err("skip".to_string()),
            &Err("skip".to_string()),
            &Err("skip".to_string()),
            &Err("skip".to_string()),
            0,
            12,
            1300,
        );
        assert_eq!(snap.el.syncing, Some(true));
        assert_eq!(snap.el.sync_distance, Some(16));
    }

    #[test]
    fn snapshot_omits_cl_slot_age_when_genesis_disabled() {
        let snap = build_snapshot(
            &Ok(ElSyncingResult::Synced(false)),
            &Ok(0x10),
            &Ok(ElBlockResult {
                number: 5,
                timestamp: 0x3e2,
            }),
            &Ok(ClStatus {
                head_slot: 10,
                sync_distance: 0,
                is_syncing: false,
            }),
            &Ok(64u64),
            0,
            12,
            1300,
        );
        assert_eq!(snap.cl.head_slot, Some(10));
        assert_eq!(snap.cl.slot_age_secs, None);
    }

    #[test]
    fn trusted_snapshot_reports_mode_and_omits_unpolled_fields() {
        let snap = trusted_snapshot("http/1.1", "h2");
        let v = serde_json::to_value(&snap).unwrap();
        assert_eq!(
            v,
            serde_json::json!({
                "mode": "trusted",
                "el": { "transport": "http/1.1" },
                "cl": { "transport": "h2" },
            })
        );
    }

    #[test]
    fn node_health_returns_206_only_when_both_layers_reachable_and_syncing() {
        let synced_el = Ok(ElSyncingResult::Synced(false));
        let syncing_el = Ok(ElSyncingResult::Syncing {
            current_block: 10,
            highest_block: 20,
        });
        let synced_cl = Ok(ClStatus {
            head_slot: 100,
            sync_distance: 0,
            is_syncing: false,
        });
        let syncing_cl = Ok(ClStatus {
            head_slot: 50,
            sync_distance: 50,
            is_syncing: true,
        });
        let ok_block = Ok(ElBlockResult {
            number: 10,
            timestamp: 1000,
        });

        // 1. Ready -> 200 OK
        let ready_probe = Probe {
            el_syncing: synced_el.clone(),
            el_peers: Ok(10),
            el_block: ok_block.clone(),
            cl_status: synced_cl.clone(),
            cl_peers: Ok(10),
        };
        assert_eq!(
            classify_node_health(true, false, &ready_probe),
            StatusCode::OK
        );

        // 2. EL syncing + CL reachable -> 206 Partial Content
        let el_sync_probe = Probe {
            el_syncing: syncing_el.clone(),
            el_peers: Ok(10),
            el_block: ok_block.clone(),
            cl_status: synced_cl.clone(),
            cl_peers: Ok(10),
        };
        assert_eq!(
            classify_node_health(false, false, &el_sync_probe),
            StatusCode::PARTIAL_CONTENT
        );
        assert_eq!(
            classify_node_health(false, true, &el_sync_probe),
            StatusCode::PARTIAL_CONTENT
        );

        // 3. EL syncing + CL down -> 503 Service Unavailable (must NOT mask CL crash as 206)
        let cl_down_probe = Probe {
            el_syncing: syncing_el,
            el_peers: Ok(10),
            el_block: ok_block.clone(),
            cl_status: Err("connection refused".into()),
            cl_peers: Err("connection refused".into()),
        };
        assert_eq!(
            classify_node_health(false, false, &cl_down_probe),
            StatusCode::SERVICE_UNAVAILABLE
        );

        // 4. CL syncing + EL down -> 503 Service Unavailable (must NOT mask EL crash as 206)
        let el_down_probe = Probe {
            el_syncing: Err("timeout".into()),
            el_peers: Err("timeout".into()),
            el_block: Err("timeout".into()),
            cl_status: syncing_cl.clone(),
            cl_peers: Ok(10),
        };
        assert_eq!(
            classify_node_health(false, false, &el_down_probe),
            StatusCode::SERVICE_UNAVAILABLE
        );

        // 5. CL syncing + EL reachable under --readyz-strict, unless EL block RPC itself failed
        let cl_sync_probe = Probe {
            el_syncing: synced_el.clone(),
            el_peers: Ok(10),
            el_block: ok_block.clone(),
            cl_status: syncing_cl.clone(),
            cl_peers: Ok(10),
        };
        assert_eq!(
            classify_node_health(false, true, &cl_sync_probe),
            StatusCode::PARTIAL_CONTENT
        );
        let strict_block_err_probe = Probe {
            el_block: Err("rpc error".into()),
            ..cl_sync_probe
        };
        assert_eq!(
            classify_node_health(false, true, &strict_block_err_probe),
            StatusCode::SERVICE_UNAVAILABLE
        );

        // 6. Both layers report not-syncing, but --readyz-strict failed freshness (stale/halted) -> 503
        let strict_stale_probe = Probe {
            el_syncing: synced_el,
            el_peers: Ok(10),
            el_block: ok_block,
            cl_status: synced_cl,
            cl_peers: Ok(10),
        };
        assert_eq!(
            classify_node_health(false, true, &strict_stale_probe),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[tokio::test]
    async fn el_transport_verify_once_heals_restart_race_and_settles_on_h1_only() {
        use clap::Parser;
        use hyper::body::Incoming;
        use hyper::service::service_fn;
        use hyper_util::rt::{TokioExecutor, TokioIo};
        use hyper_util::server::conn::auto;
        use std::sync::atomic::AtomicBool;

        let valid = br#"[{"id":1,"result":false},{"id":2,"result":"0x40"},{"id":3,"result":{"number":"0x1","timestamp":"0x2"}}]"#;
        let accept_h2 = Arc::new(AtomicBool::new(false));
        let accept_h2_srv = accept_h2.clone();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                let accept_h2_srv = accept_h2_srv.clone();
                tokio::spawn(async move {
                    let mut preface = [0; 3];
                    let is_h2 =
                        stream.peek(&mut preface).await.unwrap_or(0) == 3 && &preface == b"PRI";
                    if is_h2 && !accept_h2_srv.load(Ordering::SeqCst) {
                        return;
                    }
                    let svc = service_fn(move |req: Request<Incoming>| async move {
                        req.into_body().collect().await.unwrap();
                        Ok::<_, std::convert::Infallible>(
                            Response::builder()
                                .status(StatusCode::OK)
                                .body(Full::new(Bytes::from_static(valid)))
                                .unwrap(),
                        )
                    });
                    let _ = auto::Builder::new(TokioExecutor::new())
                        .serve_connection(TokioIo::new(stream), svc)
                        .await;
                });
            }
        });

        let mut cfg = crate::Config::parse_from(["ethonyx", "--el-http-url", &url]);
        cfg.health_timeout = Duration::from_secs(1);
        let (_, probe) = watch::channel(Arc::new(Probe::pending()));
        let state = AppState {
            el_http: crate::upstream::HttpUpstream::parse("--el-http-url", &url).unwrap(),
            el_ws: crate::upstream::WsUpstream::parse("--el-ws-url", "ws://127.0.0.1:8546")
                .unwrap(),
            cl_beacon: crate::upstream::HttpUpstream::parse("--cl-beacon-url", &url).unwrap(),
            cl_mev_relay: None,
            cl_syncing_uri: url.parse().unwrap(),
            cl_peer_count_uri: url.parse().unwrap(),
            cfg,
            client: crate::proxy::build_client(false),
            el_h2_client: crate::proxy::build_client(true),
            el_transport: ElTransport::new(false, false),
            cl_tls_version: None,
            otel_enabled: false,
            cl_genesis_time: 0,
            cl_seconds_per_slot: 12,
            probe,
        };

        // Case A: h1-only upstream -> H2C_INITIAL -> H1_VERIFY_ONCE -> H1_CONFIRMED.
        assert_eq!(state.el_transport.label(), "h2c");
        assert!(el_batch_detect(&state).await.syncing.is_ok());
        assert_eq!(state.el_transport.label(), "http/1.1");
        assert!(el_batch_detect(&state).await.syncing.is_ok());
        assert_eq!(state.el_transport.label(), "http/1.1");

        // Case B: simulate a confirmed h2c upstream hitting a transient h2c rejection
        // during restart, then recovering h2c on the verify-once poll.
        if let ElTransport::AdaptiveH2c(ref m) = state.el_transport {
            m.store(ElTransport::H2C_CONFIRMED, Ordering::SeqCst);
        }
        assert!(el_batch_detect(&state).await.syncing.is_ok());
        assert_eq!(state.el_transport.label(), "http/1.1"); // fell back to H1_VERIFY_ONCE

        accept_h2.store(true, Ordering::SeqCst);
        assert!(el_batch_detect(&state).await.syncing.is_ok());
        assert_eq!(state.el_transport.label(), "h2c"); // self-healed back to H2C_CONFIRMED!

        server.abort();
    }
}
