use std::borrow::Cow;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Duration;

use bytes::Bytes;
use http::{Method, Request, Response, StatusCode, Uri, Version};
use http_body_util::{BodyExt, Full};
use serde::Serialize;
use tokio::sync::watch;
use tracing::{debug, info, warn};

use crate::config::SlotClock;
use crate::proxy::{Https, ProxyClient, ResBody, box_full, build_h2c_client};
use crate::state::AppState;

const TRUSTED_READY_JSON: &[u8] = br#"{"status":"ready","el_syncing":{"ok":true,"detail":"trusted upstream (unpolled)"},"cl_syncing":{"ok":true,"detail":"trusted upstream (unpolled)"}}"#;

/// Whether `/healthz` reflects active background probes or `--trust-upstream`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HealthMode {
    Probed,
    Trusted,
}

/// Source of readiness and health data for `/healthz`, `/readyz`, and `node_health`.
pub(crate) enum HealthSource {
    /// `--trust-upstream`: no background poller or `watch` channel is allocated.
    Trusted,
    /// Active background polling feeding snapshots via `watch`.
    Polled(watch::Receiver<Arc<Probe>>),
}

/// Tracks the negotiated HTTP protocol version observed on an `https://` upstream.
#[derive(Default)]
pub(crate) struct ObservedHttpVersion(AtomicU8);

impl ObservedHttpVersion {
    const UNOBSERVED: u8 = 0;
    const HTTP_11: u8 = 1;
    const HTTP_2: u8 = 2;

    pub(crate) const fn new() -> Self {
        Self(AtomicU8::new(Self::UNOBSERVED))
    }

    pub(crate) fn record(&self, version: Version) {
        let encoded = if version == Version::HTTP_2 {
            Self::HTTP_2
        } else {
            Self::HTTP_11
        };
        if self.0.load(Ordering::Relaxed) != encoded {
            self.0.store(encoded, Ordering::Relaxed);
        }
    }

    /// Machine-readable `/healthz` transport (`"h2"` once observed, conservative `"http/1.1"` otherwise).
    pub(crate) fn label(&self) -> &'static str {
        if self.0.load(Ordering::Relaxed) == Self::HTTP_2 {
            "h2"
        } else {
            "http/1.1"
        }
    }

    /// Human-readable startup protocol summary (`"HTTPS (ALPN)"` before first TLS response).
    pub(crate) fn startup_label(&self) -> &'static str {
        match self.0.load(Ordering::Relaxed) {
            Self::HTTP_2 => "HTTP/2 (TLS)",
            Self::HTTP_11 => "HTTP/1.1 (TLS)",
            _ => "HTTPS (ALPN)",
        }
    }
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum H2cMode {
    Initial = 0,
    Confirmed = 1,
    H1VerifyOnce = 2,
    H1Confirmed = 3,
}

impl H2cMode {
    fn load(atom: &AtomicU8) -> Self {
        match atom.load(Ordering::Relaxed) {
            0 => Self::Initial,
            1 => Self::Confirmed,
            2 => Self::H1VerifyOnce,
            _ => Self::H1Confirmed,
        }
    }
}

/// Unified transport tracker and client selector for EL and CL HTTP hops.
pub(crate) enum HttpTransport {
    /// Cleartext HTTP/1.1 only (CL cleartext, or EL `--trust-upstream` cleartext).
    CleartextH1,
    /// `https://` upstream: negotiates `h2` vs `http/1.1` via TLS ALPN on `state.client`.
    TlsAuto(ObservedHttpVersion),
    /// Probed cleartext `http://` EL upstream with a dedicated prior-knowledge `h2c` client
    /// and one-shot `h2c` recovery verification.
    AdaptiveH2c {
        mode: AtomicU8,
        client: Box<ProxyClient>,
    },
}

impl HttpTransport {
    pub(crate) const fn new_cl(is_tls: bool) -> Self {
        if is_tls {
            Self::TlsAuto(ObservedHttpVersion::new())
        } else {
            Self::CleartextH1
        }
    }

    pub(crate) fn new_el(trust_upstream: bool, is_tls: bool, https: Https) -> Self {
        if is_tls {
            Self::TlsAuto(ObservedHttpVersion::new())
        } else if trust_upstream {
            Self::CleartextH1
        } else {
            Self::AdaptiveH2c {
                mode: AtomicU8::new(H2cMode::Initial as u8),
                client: Box::new(build_h2c_client(https)),
            }
        }
    }

    #[cfg(test)]
    fn set_confirmed_cleartext(&mut self, prefer_h2: bool) {
        if let Self::AdaptiveH2c { mode, .. } = self {
            mode.store(
                if prefer_h2 {
                    H2cMode::Confirmed as u8
                } else {
                    H2cMode::H1Confirmed as u8
                },
                Ordering::Relaxed,
            );
        }
    }

    /// Whether data-plane requests currently prefer the cleartext `h2c` prior-knowledge client.
    #[cfg(test)]
    pub(crate) fn uses_h2c_client(&self) -> bool {
        match self {
            Self::CleartextH1 | Self::TlsAuto(_) => false,
            Self::AdaptiveH2c { mode, .. } => {
                matches!(H2cMode::load(mode), H2cMode::Initial | H2cMode::Confirmed)
            }
        }
    }

    pub(crate) fn select_client<'a>(&'a self, default: &'a ProxyClient) -> &'a ProxyClient {
        match self {
            Self::AdaptiveH2c { mode, client }
                if matches!(H2cMode::load(mode), H2cMode::Initial | H2cMode::Confirmed) =>
            {
                client
            }
            Self::CleartextH1 | Self::TlsAuto(_) | Self::AdaptiveH2c { .. } => default,
        }
    }

    pub(crate) fn record_version(&self, version: Version) {
        if let Self::TlsAuto(obs) = self {
            obs.record(version);
        }
    }

    /// Machine-readable transport label for `/healthz` (`"h2c"`, `"h2"`, or `"http/1.1"`).
    pub(crate) fn label(&self) -> &'static str {
        match self {
            Self::CleartextH1 => "http/1.1",
            Self::TlsAuto(obs) => obs.label(),
            Self::AdaptiveH2c { mode, .. } => match H2cMode::load(mode) {
                H2cMode::Initial | H2cMode::Confirmed => "h2c",
                H2cMode::H1VerifyOnce | H2cMode::H1Confirmed => "http/1.1",
            },
        }
    }

    /// Human-readable protocol summary for startup lifecycle logging.
    pub(crate) fn startup_proto_label(&self) -> &'static str {
        match self {
            Self::CleartextH1 => "HTTP/1.1",
            Self::TlsAuto(obs) => obs.startup_label(),
            Self::AdaptiveH2c { mode, .. } => match H2cMode::load(mode) {
                H2cMode::Initial => "HTTP/2 (h2c, unverified)",
                H2cMode::Confirmed => "HTTP/2 (h2c)",
                H2cMode::H1VerifyOnce | H2cMode::H1Confirmed => "HTTP/1.1",
            },
        }
    }
}

/// One readiness signal: a pass/fail verdict plus a human-readable detail.
/// Used only by `/readyz`, which must render a verdict to gate traffic.
#[derive(Serialize)]
pub struct Check {
    pub ok: bool,
    pub detail: Cow<'static, str>,
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
    /// HTTP version ethonyx uses for the Beacon hop (`http/1.1` for cleartext;
    /// `h2` or `http/1.1` for `https` once ALPN negotiation is observed).
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

#[derive(Clone, Copy, serde::Deserialize)]
struct ClStatus {
    #[serde(deserialize_with = "decimal_string")]
    head_slot: u64,
    #[serde(deserialize_with = "decimal_string")]
    sync_distance: u64,
    is_syncing: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

#[derive(serde::Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub struct ElBlockResult {
    #[serde(deserialize_with = "hex_string")]
    pub number: u64,
    #[serde(deserialize_with = "hex_string")]
    pub timestamp: u64,
}

pub(crate) type ProbeError = Arc<str>;

/// The five upstream probe results captured by one poll, shared with the request
/// handlers via a `watch` channel. `/healthz` and `/readyz` read the latest one
/// instead of querying upstream per request, so upstream load is decoupled from
/// probe rate. Stored as raw results so block / slot ages can be recomputed live.
#[derive(Clone)]
pub(crate) struct Probe {
    el_syncing: Result<ElSyncingResult, ProbeError>,
    el_peers: Result<u64, ProbeError>,
    el_block: Result<ElBlockResult, ProbeError>,
    cl_status: Result<ClStatus, ProbeError>,
    cl_peers: Result<u64, ProbeError>,
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

const EL_BATCH_REQ: &[u8] = br#"[{"jsonrpc":"2.0","method":"eth_syncing","params":[],"id":1},{"jsonrpc":"2.0","method":"net_peerCount","params":[],"id":2},{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["latest",false],"id":3}]"#;

/// `/healthz` — verdict-free EL + CL snapshot, **always 200**. Serves the latest
/// background poll (see [`poll_loop`]); block / slot ages are recomputed live per
/// request. Under `--trust-upstream`, reports `mode: "trusted"` and omits
/// unpolled fields instead of fabricating placeholder metrics.
pub fn report(state: &AppState, head: bool) -> Response<ResBody> {
    if head {
        return empty_json_response(StatusCode::OK);
    }
    let snapshot = match &state.health {
        HealthSource::Trusted => {
            trusted_snapshot(state.el_transport.label(), state.cl_transport.label())
        }
        HealthSource::Polled(rx) => {
            let probe = rx.borrow();
            let mut snap = build_snapshot(
                &probe.el_syncing,
                &probe.el_peers,
                &probe.el_block,
                &probe.cl_status,
                &probe.cl_peers,
                state.slot_clock,
                now_unix(),
            );
            snap.el.transport = state.el_transport.label();
            snap.cl.transport = state.cl_transport.label();
            snap
        }
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
    match &state.health {
        HealthSource::Trusted => {
            if head {
                empty_json_response(StatusCode::OK)
            } else {
                raw_json_response(StatusCode::OK, Bytes::from_static(TRUSTED_READY_JSON))
            }
        }
        HealthSource::Polled(rx) => {
            let probe = rx.borrow();
            let eval = evaluate_readiness(state, &probe);
            let code = if eval.is_ready() {
                StatusCode::OK
            } else {
                StatusCode::SERVICE_UNAVAILABLE
            };
            if head {
                empty_json_response(code)
            } else {
                json_response(code, &eval.into_report())
            }
        }
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
    let code = match &state.health {
        HealthSource::Trusted => StatusCode::OK,
        HealthSource::Polled(rx) => {
            let probe = rx.borrow();
            let eval = evaluate_readiness(state, &probe);
            classify_node_health(eval.is_ready(), state.cfg.readyz_strict, &probe)
        }
    };
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

#[derive(Clone, Copy)]
enum ElSyncEval<'a> {
    Synced,
    SyncingBool,
    SyncingProgress { current_block: u64, distance: u64 },
    Err(&'a str),
}

impl ElSyncEval<'_> {
    const fn is_ok(self) -> bool {
        matches!(self, Self::Synced)
    }

    fn into_check(self) -> Check {
        let ok = self.is_ok();
        let detail = match self {
            Self::Synced => Cow::Borrowed("synced"),
            Self::SyncingBool => Cow::Borrowed("syncing (true)"),
            Self::SyncingProgress {
                current_block,
                distance,
            } => Cow::Owned(format!(
                "syncing (block {current_block}, distance {distance})"
            )),
            Self::Err(e) => Cow::Owned(format!("eth_syncing: {e}")),
        };
        Check { ok, detail }
    }
}

#[derive(Clone, Copy)]
enum ClSyncEval<'a> {
    Synced { head_slot: u64, sync_distance: u64 },
    Syncing { head_slot: u64, sync_distance: u64 },
    Err(&'a str),
}

impl ClSyncEval<'_> {
    const fn is_ok(self) -> bool {
        matches!(self, Self::Synced { .. })
    }

    fn into_check(self) -> Check {
        let ok = self.is_ok();
        let detail = match self {
            Self::Synced {
                head_slot,
                sync_distance,
            } => Cow::Owned(format!(
                "synced (slot {head_slot}, distance {sync_distance})"
            )),
            Self::Syncing {
                head_slot,
                sync_distance,
            } => Cow::Owned(format!(
                "syncing (slot {head_slot}, distance {sync_distance})"
            )),
            Self::Err(e) => Cow::Owned(format!("node/syncing: {e}")),
        };
        Check { ok, detail }
    }
}

#[derive(Clone, Copy)]
enum FreshEval<'a> {
    Fresh {
        kind: &'static str,
        num: u64,
        age: u64,
    },
    Stale {
        kind: &'static str,
        num: u64,
        age: u64,
        max_age: u64,
    },
    Disabled {
        head_slot: u64,
    },
    Err {
        prefix: &'static str,
        err: &'a str,
    },
}

impl FreshEval<'_> {
    const fn is_ok(self) -> bool {
        matches!(self, Self::Fresh { .. } | Self::Disabled { .. })
    }

    fn into_check(self) -> Check {
        let ok = self.is_ok();
        let detail = match self {
            Self::Fresh { kind, num, age } => Cow::Owned(format!("{kind} {num}, age {age}s")),
            Self::Stale {
                kind,
                num,
                age,
                max_age,
            } => Cow::Owned(format!("{kind} {num} stale: {age}s (max {max_age})")),
            Self::Disabled { head_slot } => {
                Cow::Owned(format!("slot {head_slot} (age check disabled)"))
            }
            Self::Err { prefix, err } => Cow::Owned(format!("{prefix}: {err}")),
        };
        Check { ok, detail }
    }
}

/// Zero-allocation readiness evaluation shared across `HEAD /readyz`, `GET /readyz`,
/// `node_health`, `poll_loop` metrics, and readiness transition logs.
#[derive(Clone, Copy)]
pub(crate) struct ReadinessEval<'a> {
    el_sync: ElSyncEval<'a>,
    cl_sync: ClSyncEval<'a>,
    strict: Option<(FreshEval<'a>, FreshEval<'a>)>,
}

impl ReadinessEval<'_> {
    pub(crate) const fn el_sync_ok(&self) -> bool {
        self.el_sync.is_ok()
    }

    pub(crate) const fn cl_sync_ok(&self) -> bool {
        self.cl_sync.is_ok()
    }

    pub(crate) const fn is_ready(&self) -> bool {
        self.el_sync_ok()
            && self.cl_sync_ok()
            && match self.strict {
                None => true,
                Some((el_f, cl_f)) => el_f.is_ok() && cl_f.is_ok(),
            }
    }

    fn into_report(self) -> ReadyReport {
        let ready = self.is_ready();
        let (el_block_fresh, cl_slot_fresh) = match self.strict {
            Some((el_f, cl_f)) => (Some(el_f.into_check()), Some(cl_f.into_check())),
            None => (None, None),
        };
        ReadyReport {
            status: if ready { "ready" } else { "not_ready" },
            el_syncing: self.el_sync.into_check(),
            cl_syncing: self.cl_sync.into_check(),
            el_block_fresh,
            cl_slot_fresh,
        }
    }
}

pub(crate) fn evaluate_readiness<'a>(state: &AppState, probe: &'a Probe) -> ReadinessEval<'a> {
    let el_sync = eval_el_syncing(&probe.el_syncing);
    let cl_sync = eval_cl_syncing(&probe.cl_status);
    // `now` is only needed for strict freshness checks; the default sync-only path skips reading the clock.
    let strict = state.cfg.readyz_strict.then(|| {
        let now = now_unix();
        (
            eval_el_block_fresh(&probe.el_block, state.cfg.el_max_block_age_secs, now),
            eval_cl_slot_fresh(
                &probe.cl_status,
                state.slot_clock,
                state.cfg.cl_max_slot_age_secs,
                now,
            ),
        )
    });
    ReadinessEval {
        el_sync,
        cl_sync,
        strict,
    }
}

/// What [`poll_loop`] should log given the previous and current readiness, so a
/// sustained state logs once (on transition) rather than once per poll.
#[derive(Clone, Copy)]
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
    let (el_syncing, el_peers, el_block) = batch.into_probe_results();
    Probe {
        el_syncing,
        el_peers,
        el_block,
        cl_status,
        cl_peers,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum HealthError {
    Timeout,
    Transport(ProbeError),
    HttpStatus(http::StatusCode),
    Decode(ProbeError),
    Rpc(ProbeError),
}

impl HealthError {
    fn into_probe_error(self) -> ProbeError {
        match self {
            Self::Timeout => Arc::from("timeout"),
            other => Arc::from(other.to_string()),
        }
    }
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

    fn into_probe_results(
        self,
    ) -> (
        Result<ElSyncingResult, ProbeError>,
        Result<u64, ProbeError>,
        Result<ElBlockResult, ProbeError>,
    ) {
        if let (Err(e1), Err(e2), Err(e3)) = (&self.syncing, &self.peers, &self.block)
            && e1 == e2
            && e2 == e3
        {
            let shared = self.syncing.unwrap_err().into_probe_error();
            return (Err(shared.clone()), Err(shared.clone()), Err(shared));
        }
        (
            self.syncing.map_err(HealthError::into_probe_error),
            self.peers.map_err(HealthError::into_probe_error),
            self.block.map_err(HealthError::into_probe_error),
        )
    }
}

/// Query all three EL JSON-RPC methods in a batch request, maintaining the transport verdict.
async fn el_batch_detect(state: &AppState) -> ElBatchResult {
    let default_client = &state.client;
    let result = match &state.el_transport {
        HttpTransport::CleartextH1 | HttpTransport::TlsAuto(_) => {
            el_batch_rpc(state, default_client).await
        }
        HttpTransport::AdaptiveH2c { mode, client } => {
            let h2c_client: &ProxyClient = client;
            match H2cMode::load(mode) {
                cur @ (H2cMode::Initial | H2cMode::Confirmed) => {
                    match el_batch_rpc(state, h2c_client).await {
                        Err(primary_err) if is_transport(&primary_err) => {
                            let alt = el_batch_rpc(state, default_client).await;
                            if matches!(&alt, Err(e) if is_transport(e)) {
                                Err(primary_err)
                            } else {
                                mode.store(H2cMode::H1VerifyOnce as u8, Ordering::Relaxed);
                                if cur == H2cMode::Confirmed {
                                    warn!(
                                        "EL JSON-RPC upstream stopped speaking h2c — using HTTP/1.1"
                                    );
                                }
                                alt
                            }
                        }
                        reached => {
                            if cur == H2cMode::Initial {
                                mode.store(H2cMode::Confirmed as u8, Ordering::Relaxed);
                            }
                            reached
                        }
                    }
                }
                H2cMode::H1VerifyOnce => {
                    // Verify h2c one more time to self-heal if an h2c upstream was
                    // probed mid-bind during startup or restart.
                    let h2_try = el_batch_rpc(state, h2c_client).await;
                    if !matches!(&h2_try, Err(e) if is_transport(e)) {
                        mode.store(H2cMode::Confirmed as u8, Ordering::Relaxed);
                        info!("EL JSON-RPC upstream speaks h2c — using HTTP/2");
                        h2_try
                    } else {
                        let h1_try = el_batch_rpc(state, default_client).await;
                        if !matches!(&h1_try, Err(e) if is_transport(e)) {
                            mode.store(H2cMode::H1Confirmed as u8, Ordering::Relaxed);
                        }
                        h1_try
                    }
                }
                H2cMode::H1Confirmed => match el_batch_rpc(state, default_client).await {
                    Err(primary_err) if is_transport(&primary_err) => {
                        let alt = el_batch_rpc(state, h2c_client).await;
                        if matches!(&alt, Err(e) if is_transport(e)) {
                            Err(primary_err)
                        } else {
                            mode.store(H2cMode::Confirmed as u8, Ordering::Relaxed);
                            info!("EL JSON-RPC upstream speaks h2c — using HTTP/2");
                            alt
                        }
                    }
                    reached => reached,
                },
            }
        }
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

pub(crate) fn update_metrics(probe: &Probe, eval: ReadinessEval<'_>) {
    ::metrics::gauge!("ethonyx_upstream_health_status", "layer" => "el")
        .set(if eval.el_sync_ok() { 1.0 } else { 0.0 });
    ::metrics::gauge!("ethonyx_upstream_health_status", "layer" => "cl")
        .set(if eval.cl_sync_ok() { 1.0 } else { 0.0 });

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

        let eval = evaluate_readiness(&state, &probe);
        let ready = eval.is_ready();
        let status = if ready { "ready" } else { "not_ready" };
        debug!(status, "health poll");

        update_metrics(&probe, eval);

        match readiness_transition(prev_ready, ready) {
            Transition::Silent => {}
            Transition::Recovered => info!("readiness recovered"),
            Transition::BecameNotReady => {
                let report = eval.into_report();
                let el_block = report.el_block_fresh.as_ref().map_or("-", |c| &*c.detail);
                let cl_slot = report.cl_slot_fresh.as_ref().map_or("-", |c| &*c.detail);
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
    raw_json_response(code, Bytes::new())
}

fn raw_json_response(code: StatusCode, body: Bytes) -> Response<ResBody> {
    Response::builder()
        .status(code)
        .header("content-type", "application/json")
        .body(box_full(Full::new(body)))
        .expect("response builder")
}

fn json_response<T: Serialize>(code: StatusCode, body: &T) -> Response<ResBody> {
    let body_bytes =
        serde_json::to_vec(body).map_or_else(|_| Bytes::from_static(b"{}"), Bytes::from);
    raw_json_response(code, body_bytes)
}

// ---- readiness verdicts (ReadinessEval -> Check): sync is a fact; freshness uses thresholds ----

fn eval_el_syncing(r: &Result<ElSyncingResult, ProbeError>) -> ElSyncEval<'_> {
    match r {
        Ok(ElSyncingResult::Synced(false)) => ElSyncEval::Synced,
        Ok(ElSyncingResult::Synced(true)) => ElSyncEval::SyncingBool,
        Ok(ElSyncingResult::Syncing {
            current_block,
            highest_block,
        }) => ElSyncEval::SyncingProgress {
            current_block: *current_block,
            distance: highest_block.saturating_sub(*current_block),
        },
        Err(e) => ElSyncEval::Err(e),
    }
}

fn eval_cl_syncing(r: &Result<ClStatus, ProbeError>) -> ClSyncEval<'_> {
    match r {
        Ok(s) if !s.is_syncing => ClSyncEval::Synced {
            head_slot: s.head_slot,
            sync_distance: s.sync_distance,
        },
        Ok(s) => ClSyncEval::Syncing {
            head_slot: s.head_slot,
            sync_distance: s.sync_distance,
        },
        Err(e) => ClSyncEval::Err(e),
    }
}

fn eval_el_block_fresh(
    r: &Result<ElBlockResult, ProbeError>,
    max_age: u64,
    now: u64,
) -> FreshEval<'_> {
    match r {
        Ok(block) => {
            let age = now.saturating_sub(block.timestamp);
            if age <= max_age {
                FreshEval::Fresh {
                    kind: "block",
                    num: block.number,
                    age,
                }
            } else {
                FreshEval::Stale {
                    kind: "block",
                    num: block.number,
                    age,
                    max_age,
                }
            }
        }
        Err(e) => FreshEval::Err {
            prefix: "eth_getBlockByNumber",
            err: e,
        },
    }
}

fn eval_cl_slot_fresh(
    r: &Result<ClStatus, ProbeError>,
    slot_clock: Option<SlotClock>,
    max_age: u64,
    now: u64,
) -> FreshEval<'_> {
    match (r, slot_clock) {
        (Ok(s), None) => FreshEval::Disabled {
            head_slot: s.head_slot,
        },
        (Ok(s), Some(clock)) => {
            let age = clock.slot_age(s.head_slot, now);
            if age <= max_age {
                FreshEval::Fresh {
                    kind: "slot",
                    num: s.head_slot,
                    age,
                }
            } else {
                FreshEval::Stale {
                    kind: "slot",
                    num: s.head_slot,
                    age,
                    max_age,
                }
            }
        }
        (Err(e), _) => FreshEval::Err {
            prefix: "node/syncing",
            err: e,
        },
    }
}

// ---- /healthz: verdict-free numeric snapshot ----

/// Fold the five upstream probe results into a numeric snapshot. Successful
/// signals populate their fields; failed ones are pushed onto the layer's
/// `errors` and leave the field unset.
fn build_snapshot(
    sync_r: &Result<ElSyncingResult, ProbeError>,
    peers_r: &Result<u64, ProbeError>,
    block_r: &Result<ElBlockResult, ProbeError>,
    cl_status_r: &Result<ClStatus, ProbeError>,
    cl_peers_r: &Result<u64, ProbeError>,
    slot_clock: Option<SlotClock>,
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
            cl.slot_age_secs = slot_clock.map(|clock| clock.slot_age(s.head_slot, now));
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

/// Send a prepared probe request with the health-poll timeout, record any TLS
/// negotiated protocol version as soon as response headers arrive, and return
/// the body of a successful response.
async fn fetch_bytes(
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

async fn el_batch_rpc(
    state: &AppState,
    client: &ProxyClient,
) -> Result<ElBatchResult, HealthError> {
    let req = Request::builder()
        .method(Method::POST)
        .uri(state.el_http.uri().clone())
        .header("content-type", "application/json")
        .body(box_full(Full::new(Bytes::from_static(EL_BATCH_REQ))))
        .map_err(|e| HealthError::Transport(Arc::from(format!("build: {e}"))))?;
    let body = fetch_bytes(state, req, client, &state.el_transport).await?;
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
        serde_json::from_slice(body).map_err(|e| HealthError::Decode(Arc::from(e.to_string())))?;
    let mut slots: [Option<RpcResponse<'_>>; 3] = [None, None, None];
    for JsonObject(resp) in arr {
        let Some(idx) = usize::try_from(resp.id)
            .ok()
            .and_then(|id| id.checked_sub(1))
            .filter(|&idx| idx < 3)
        else {
            return Err(HealthError::Decode(Arc::from(
                "duplicate or unexpected response id",
            )));
        };
        if slots[idx].replace(resp).is_some() {
            return Err(HealthError::Decode(Arc::from(
                "duplicate or unexpected response id",
            )));
        }
    }

    let decode_res = |idx: usize| -> Result<&serde_json::value::RawValue, HealthError> {
        let resp = slots[idx].as_ref().ok_or_else(|| {
            HealthError::Decode(Arc::from(format!("missing response for id {}", idx + 1)))
        })?;
        if let Some(err) = resp.error {
            return Err(HealthError::Rpc(Arc::from(err.get())));
        }
        resp.result
            .ok_or_else(|| HealthError::Decode(Arc::from("missing result")))
    };

    let syncing = decode_res(0).and_then(|raw| {
        serde_json::from_str(raw.get()).map_err(|e| HealthError::Decode(Arc::from(e.to_string())))
    });
    let peers = decode_res(1).and_then(|raw| {
        serde_json::from_str::<HexQuantity>(raw.get())
            .map(|HexQuantity(n)| n)
            .map_err(|e| HealthError::Decode(Arc::from(e.to_string())))
    });
    let block = decode_res(2).and_then(|raw| {
        serde_json::from_str::<JsonObject<ElBlockResult>>(raw.get())
            .map(|JsonObject(block)| block)
            .map_err(|e| HealthError::Decode(Arc::from(e.to_string())))
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

fn parse_cl_response<T: serde::de::DeserializeOwned>(body: &[u8]) -> Result<T, ProbeError> {
    serde_json::from_slice::<JsonObject<ClEnvelope<T>>>(body)
        .map(|JsonObject(envelope)| envelope.data.0)
        .map_err(|e| Arc::from(format!("decode: {e}")))
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

async fn cl_syncing_status(state: &AppState) -> Result<ClStatus, ProbeError> {
    cl_get_json(state, &state.cl_syncing_uri).await
}

async fn cl_peer_count(state: &AppState) -> Result<u64, ProbeError> {
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
) -> Result<T, ProbeError> {
    let req = Request::builder()
        .method(Method::GET)
        .uri(uri.clone())
        .body(box_full(Full::new(Bytes::new())))
        .map_err(|e| Arc::from(e.to_string()))?;
    let body = fetch_bytes(state, req, &state.client, &state.cl_transport)
        .await
        .map_err(HealthError::into_probe_error)?;
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
            let mut cfg = crate::Config::parse_from([
                "ethonyx",
                "--el-http-url",
                &url,
                "--cl-beacon-url",
                &url,
            ]);
            cfg.health_timeout = Duration::from_secs(1);
            let validated = cfg.parse_validated().unwrap();
            let (_, probe) = watch::channel(Arc::new(Probe::pending()));
            let mut state =
                AppState::new(cfg, validated, false, HealthSource::Polled(probe)).unwrap();
            state.el_transport.set_confirmed_cleartext(prefer_h2);
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
        let c = eval_el_syncing(&Ok(ElSyncingResult::Synced(false))).into_check();
        assert!(c.ok);
        assert_eq!(c.detail, "synced");
    }

    #[test]
    fn el_syncing_object_reports_block_progress() {
        let c = eval_el_syncing(&Ok(ElSyncingResult::Syncing {
            current_block: 0x10,
            highest_block: 0x20,
        }))
        .into_check();
        assert!(!c.ok);
        assert_eq!(c.detail, "syncing (block 16, distance 16)");
    }

    #[test]
    fn el_syncing_error_is_prefixed() {
        let c = eval_el_syncing(&Err("boom".into())).into_check();
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
        let c = eval_cl_syncing(&Ok(s)).into_check();
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
        let c = eval_cl_syncing(&Ok(s)).into_check();
        assert!(!c.ok);
        assert_eq!(c.detail, "syncing (slot 42, distance 5)");
    }

    #[test]
    fn el_block_fresh_within_threshold_is_ok() {
        let block = ElBlockResult {
            number: 5,
            timestamp: 0x3e2,
        };
        let c = eval_el_block_fresh(&Ok(block), 60, 1000).into_check();
        assert!(c.ok);
        assert_eq!(c.detail, "block 5, age 6s");
    }

    #[test]
    fn el_block_fresh_past_threshold_is_stale() {
        let block = ElBlockResult {
            number: 5,
            timestamp: 0x384,
        };
        let c = eval_el_block_fresh(&Ok(block), 60, 1000).into_check();
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
        let c = eval_cl_slot_fresh(&Ok(s), SlotClock::new(0, 12), 60, 9_999_999).into_check();
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
        let c = eval_cl_slot_fresh(&Ok(s), SlotClock::new(1000, 12), 60, 1300).into_check();
        assert!(!c.ok);
        assert_eq!(c.detail, "slot 10 stale: 180s (max 60)");
    }

    #[test]
    fn slot_age_computes_wall_clock_age() {
        let clock = SlotClock::new(1000, 12).unwrap();
        assert_eq!(clock.slot_age(10, 1300), 180);
    }

    #[test]
    fn slot_age_saturates_on_garbage_head_slot() {
        let clock = SlotClock::new(1_606_824_023, 12).unwrap();
        assert_eq!(clock.slot_age(u64::MAX, 2_000_000_000), 0);
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
            SlotClock::new(1000, 12),
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
            &Err("timeout".into()),
            &Err("http 500".into()),
            &Err("timeout".into()),
            &Err("http 404".into()),
            &Err("http 404".into()),
            SlotClock::new(1000, 12),
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
            &Err("skip".into()),
            &Err("skip".into()),
            &Err("skip".into()),
            &Err("skip".into()),
            SlotClock::new(0, 12),
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
            SlotClock::new(0, 12),
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
        let synced_el = ElSyncingResult::Synced(false);
        let syncing_el = ElSyncingResult::Syncing {
            current_block: 10,
            highest_block: 20,
        };
        let synced_cl = ClStatus {
            head_slot: 100,
            sync_distance: 0,
            is_syncing: false,
        };
        let syncing_cl = ClStatus {
            head_slot: 50,
            sync_distance: 50,
            is_syncing: true,
        };
        let ok_block = ElBlockResult {
            number: 10,
            timestamp: 1000,
        };

        // 1. Ready -> 200 OK
        let ready_probe = Probe {
            el_syncing: Ok(synced_el),
            el_peers: Ok(10),
            el_block: Ok(ok_block),
            cl_status: Ok(synced_cl),
            cl_peers: Ok(10),
        };
        assert_eq!(
            classify_node_health(true, false, &ready_probe),
            StatusCode::OK
        );

        // 2. EL syncing + CL reachable -> 206 Partial Content
        let el_sync_probe = Probe {
            el_syncing: Ok(syncing_el),
            el_peers: Ok(10),
            el_block: Ok(ok_block),
            cl_status: Ok(synced_cl),
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
            el_syncing: Ok(syncing_el),
            el_peers: Ok(10),
            el_block: Ok(ok_block),
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
            cl_status: Ok(syncing_cl),
            cl_peers: Ok(10),
        };
        assert_eq!(
            classify_node_health(false, false, &el_down_probe),
            StatusCode::SERVICE_UNAVAILABLE
        );

        // 5. CL syncing + EL reachable under --readyz-strict, unless EL block RPC itself failed
        let cl_sync_probe = Probe {
            el_syncing: Ok(synced_el),
            el_peers: Ok(10),
            el_block: Ok(ok_block),
            cl_status: Ok(syncing_cl),
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
            el_syncing: Ok(synced_el),
            el_peers: Ok(10),
            el_block: Ok(ok_block),
            cl_status: Ok(synced_cl),
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

        let mut cfg =
            crate::Config::parse_from(["ethonyx", "--el-http-url", &url, "--cl-beacon-url", &url]);
        cfg.health_timeout = Duration::from_secs(1);
        let validated = cfg.parse_validated().unwrap();
        let (_, probe) = watch::channel(Arc::new(Probe::pending()));
        let state = AppState::new(cfg, validated, false, HealthSource::Polled(probe)).unwrap();

        // Case A: h1-only upstream -> Initial -> H1VerifyOnce -> H1Confirmed.
        assert_eq!(state.el_transport.label(), "h2c");
        assert_eq!(
            state.el_transport.startup_proto_label(),
            "HTTP/2 (h2c, unverified)"
        );
        assert!(el_batch_detect(&state).await.syncing.is_ok());
        assert_eq!(state.el_transport.label(), "http/1.1");
        assert!(el_batch_detect(&state).await.syncing.is_ok());
        assert_eq!(state.el_transport.label(), "http/1.1");

        // Case B: simulate a confirmed h2c upstream hitting a transient h2c rejection
        // during restart, then recovering h2c on the verify-once poll.
        if let HttpTransport::AdaptiveH2c { ref mode, .. } = state.el_transport {
            mode.store(H2cMode::Confirmed as u8, Ordering::SeqCst);
        }
        assert_eq!(state.el_transport.startup_proto_label(), "HTTP/2 (h2c)");
        assert!(el_batch_detect(&state).await.syncing.is_ok());
        assert_eq!(state.el_transport.label(), "http/1.1"); // fell back to H1VerifyOnce

        accept_h2.store(true, Ordering::SeqCst);
        assert!(el_batch_detect(&state).await.syncing.is_ok());
        assert_eq!(state.el_transport.label(), "h2c"); // self-healed back to Confirmed!

        server.abort();
    }

    #[test]
    fn observed_http_version_distinguishes_unobserved_startup_label() {
        let obs = ObservedHttpVersion::new();
        assert_eq!(obs.startup_label(), "HTTPS (ALPN)");
        assert_eq!(obs.label(), "http/1.1");

        obs.record(Version::HTTP_11);
        assert_eq!(obs.startup_label(), "HTTP/1.1 (TLS)");
        assert_eq!(obs.label(), "http/1.1");

        obs.record(Version::HTTP_2);
        assert_eq!(obs.startup_label(), "HTTP/2 (TLS)");
        assert_eq!(obs.label(), "h2");
    }
}
