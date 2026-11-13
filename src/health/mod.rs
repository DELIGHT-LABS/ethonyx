//! `/healthz`, `/readyz`, and `/eth/v1/node/health` endpoints and readiness evaluation.

mod cl;
mod el;
mod probe;

pub(crate) use probe::{HealthSource, Probe, poll_loop, probe_once, update_metrics};

use std::fmt;
use std::sync::Arc;

use bytes::Bytes;
use http::{Response, StatusCode};
use http_body_util::Full;
use serde::Serialize;

use crate::config::SlotClock;
use crate::state::AppState;
use crate::transport::{ResBody, box_full};

use cl::ClStatus;
use el::{ElBlockResult, ElSyncingResult};
use probe::ProbeError;

const TRUSTED_READY_JSON: &[u8] = br#"{"status":"ready","el_syncing":{"ok":true,"detail":"trusted upstream (unpolled)"},"cl_syncing":{"ok":true,"detail":"trusted upstream (unpolled)"}}"#;
const TRUSTED_HEALTH_H1_H1: &[u8] =
    br#"{"mode":"trusted","el":{"transport":"http/1.1"},"cl":{"transport":"http/1.1"}}"#;
const TRUSTED_HEALTH_H1_H2: &[u8] =
    br#"{"mode":"trusted","el":{"transport":"http/1.1"},"cl":{"transport":"h2"}}"#;
const TRUSTED_HEALTH_H2_H1: &[u8] =
    br#"{"mode":"trusted","el":{"transport":"h2"},"cl":{"transport":"http/1.1"}}"#;
const TRUSTED_HEALTH_H2_H2: &[u8] =
    br#"{"mode":"trusted","el":{"transport":"h2"},"cl":{"transport":"h2"}}"#;
const TRUSTED_HEALTH_JSON: [[&[u8]; 2]; 2] = [
    [TRUSTED_HEALTH_H1_H1, TRUSTED_HEALTH_H1_H2],
    [TRUSTED_HEALTH_H2_H1, TRUSTED_HEALTH_H2_H2],
];

/// Serializes a [`fmt::Display`] value directly into the JSON output stream via
/// [`serde::Serializer::collect_str`] without allocating an intermediate `String`.
struct DisplayJson<D>(D);

impl<D: fmt::Display> Serialize for DisplayJson<D> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(&self.0)
    }
}

#[derive(Clone, Copy)]
struct PrefixedError<'a> {
    prefix: &'static str,
    err: &'a str,
}

impl Serialize for PrefixedError<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(&format_args!("{}: {}", self.prefix, self.err))
    }
}

/// Fixed-capacity stack list of borrowed probe errors so `/healthz` serializes
/// outage reports without per-request `Vec` or `format!` `String` allocations.
struct ErrorList<'a, const N: usize> {
    items: [PrefixedError<'a>; N],
    len: usize,
}

impl<const N: usize> Default for ErrorList<'_, N> {
    fn default() -> Self {
        Self {
            items: [PrefixedError {
                prefix: "",
                err: "",
            }; N],
            len: 0,
        }
    }
}

impl<'a, const N: usize> ErrorList<'a, N> {
    const fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn push(&mut self, prefix: &'static str, err: &'a str) {
        if let Some(slot) = self.items.get_mut(self.len) {
            *slot = PrefixedError { prefix, err };
            self.len += 1;
        }
    }
}

impl<const N: usize> Serialize for ErrorList<'_, N> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(&self.items[..self.len])
    }
}

/// One readiness signal: a pass/fail verdict plus a human-readable detail.
/// Used only by `/readyz`, which must render a verdict to gate traffic.
#[derive(Serialize)]
#[serde(bound = "")]
struct Check<D: fmt::Display> {
    ok: bool,
    detail: DisplayJson<D>,
}

/// `/healthz` view: a verdict-free, machine-readable snapshot of current EL + CL
/// state. Numeric fields carry the live values (peer counts, block / slot age,
/// sync status) for the consumer to threshold; any upstream failure is recorded
/// in the layer's `errors`, and unavailable or unpolled fields are omitted. This
/// endpoint **always returns 200** — it reports state, it does not judge it.
#[derive(Serialize)]
struct HealthSnapshot<'a> {
    mode: &'static str,
    el: ElHealth<'a>,
    cl: ClHealth<'a>,
}

#[derive(Serialize, Default)]
struct ElHealth<'a> {
    /// HTTP version ethonyx uses for this upstream hop: `h2c` / `h2` / `http/1.1`.
    /// Reflects the poller's auto-detected EL transport verdict.
    transport: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    syncing: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sync_distance: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    peers: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    block_number: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    block_age_secs: Option<u64>,
    #[serde(skip_serializing_if = "ErrorList::is_empty")]
    errors: ErrorList<'a, 3>,
}

#[derive(Serialize, Default)]
struct ClHealth<'a> {
    /// HTTP version ethonyx uses for the Beacon hop (`http/1.1` for cleartext;
    /// `h2` or `http/1.1` for `https` once ALPN negotiation is observed).
    transport: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    syncing: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sync_distance: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    peers: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    head_slot: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    slot_age_secs: Option<u64>,
    #[serde(skip_serializing_if = "ErrorList::is_empty")]
    errors: ErrorList<'a, 2>,
}

/// `/readyz` view: the load-balancer readiness verdict. Defaults to sync status
/// only; the freshness fields are populated (and gated on) only under
/// `--readyz-strict`. Peer count is intentionally never part of readiness — it
/// is a soft, fleet-correlated signal that belongs in `/healthz`.
#[derive(Serialize)]
struct ReadyReport<'a> {
    status: &'static str,
    el_syncing: Check<ElSyncEval<'a>>,
    cl_syncing: Check<ClSyncEval<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    el_block_fresh: Option<Check<FreshEval<'a>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cl_slot_fresh: Option<Check<FreshEval<'a>>>,
}

/// `/healthz` — verdict-free EL + CL snapshot, **always 200**. Serves the latest
/// background poll (see [`poll_loop`]); block / slot ages are recomputed live per
/// request. Under `--trust-upstream`, reports `mode: "trusted"` and omits
/// unpolled fields instead of fabricating placeholder metrics.
pub(crate) fn report(state: &AppState, head: bool) -> Response<ResBody> {
    if head {
        return empty_json_response(StatusCode::OK);
    }
    match &state.health {
        HealthSource::Trusted => {
            let raw = TRUSTED_HEALTH_JSON[usize::from(state.el.transport.is_observed_h2())]
                [usize::from(state.cl.transport.is_observed_h2())];
            raw_json_response(StatusCode::OK, Bytes::from_static(raw))
        }
        HealthSource::Polled(rx) => {
            let probe = Arc::clone(&rx.borrow());
            let mut snap = build_snapshot(
                &probe.el_syncing,
                &probe.el_peers,
                &probe.el_block,
                &probe.cl_status,
                &probe.cl_peers,
                state.slot_clock,
                now_unix(),
            );
            snap.el.transport = state.el.transport.label();
            snap.cl.transport = state.cl.transport.label();
            json_response(StatusCode::OK, &snap)
        }
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
pub(crate) fn ready(state: &AppState, head: bool) -> Response<ResBody> {
    match &state.health {
        HealthSource::Trusted => {
            if head {
                empty_json_response(StatusCode::OK)
            } else {
                raw_json_response(StatusCode::OK, Bytes::from_static(TRUSTED_READY_JSON))
            }
        }
        HealthSource::Polled(rx) => {
            let probe = Arc::clone(&rx.borrow());
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
pub(crate) fn node_health(state: &AppState) -> Response<ResBody> {
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
pub(super) enum ElSyncEval<'a> {
    Synced,
    SyncingBool,
    SyncingProgress { current_block: u64, distance: u64 },
    Err(&'a str),
}

impl ElSyncEval<'_> {
    const fn is_ok(self) -> bool {
        matches!(self, Self::Synced)
    }

    const fn into_check(self) -> Check<Self> {
        Check {
            ok: self.is_ok(),
            detail: DisplayJson(self),
        }
    }
}

impl fmt::Display for ElSyncEval<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Synced => f.write_str("synced"),
            Self::SyncingBool => f.write_str("syncing (true)"),
            Self::SyncingProgress {
                current_block,
                distance,
            } => write!(f, "syncing (block {current_block}, distance {distance})"),
            Self::Err(e) => write!(f, "eth_syncing: {e}"),
        }
    }
}

#[derive(Clone, Copy)]
pub(super) enum ClSyncEval<'a> {
    Synced { head_slot: u64, sync_distance: u64 },
    Syncing { head_slot: u64, sync_distance: u64 },
    Err(&'a str),
}

impl ClSyncEval<'_> {
    const fn is_ok(self) -> bool {
        matches!(self, Self::Synced { .. })
    }

    const fn into_check(self) -> Check<Self> {
        Check {
            ok: self.is_ok(),
            detail: DisplayJson(self),
        }
    }
}

impl fmt::Display for ClSyncEval<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Synced {
                head_slot,
                sync_distance,
            } => write!(f, "synced (slot {head_slot}, distance {sync_distance})"),
            Self::Syncing {
                head_slot,
                sync_distance,
            } => write!(f, "syncing (slot {head_slot}, distance {sync_distance})"),
            Self::Err(e) => write!(f, "node/syncing: {e}"),
        }
    }
}

#[derive(Clone, Copy)]
pub(super) enum FreshEval<'a> {
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

    const fn into_check(self) -> Check<Self> {
        Check {
            ok: self.is_ok(),
            detail: DisplayJson(self),
        }
    }
}

impl fmt::Display for FreshEval<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Fresh { kind, num, age } => write!(f, "{kind} {num}, age {age}s"),
            Self::Stale {
                kind,
                num,
                age,
                max_age,
            } => write!(f, "{kind} {num} stale: {age}s (max {max_age})"),
            Self::Disabled { head_slot } => write!(f, "slot {head_slot} (age check disabled)"),
            Self::Err { prefix, err } => write!(f, "{prefix}: {err}"),
        }
    }
}

/// Zero-allocation readiness evaluation shared across `HEAD /readyz`, `GET /readyz`,
/// `node_health`, `poll_loop` metrics, and readiness transition logs.
#[derive(Clone, Copy)]
pub(crate) struct ReadinessEval<'a> {
    pub(super) el_sync: ElSyncEval<'a>,
    pub(super) cl_sync: ClSyncEval<'a>,
    pub(super) strict: Option<(FreshEval<'a>, FreshEval<'a>)>,
}

impl<'a> ReadinessEval<'a> {
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

    const fn into_report(self) -> ReadyReport<'a> {
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
pub(super) enum Transition {
    Silent,
    BecameNotReady,
    Recovered,
}

pub(super) fn readiness_transition(prev_ready: Option<bool>, ready: bool) -> Transition {
    match prev_ready {
        Some(p) if p == ready => Transition::Silent,
        None if ready => Transition::Silent, // first poll already healthy: stay quiet
        _ if ready => Transition::Recovered,
        _ => Transition::BecameNotReady, // first-poll-degraded or ready -> not-ready
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
    let mut buf = Vec::with_capacity(512);
    let body_bytes = if serde_json::to_writer(&mut buf, body).is_ok() {
        Bytes::from(buf)
    } else {
        Bytes::from_static(b"{}")
    };
    raw_json_response(code, body_bytes)
}

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

/// Fold the five upstream probe results into a numeric snapshot. Successful
/// signals populate their fields; failed ones are pushed onto the layer's
/// `errors` and leave the field unset.
fn build_snapshot<'a>(
    sync_r: &'a Result<ElSyncingResult, ProbeError>,
    peers_r: &'a Result<u64, ProbeError>,
    block_r: &'a Result<ElBlockResult, ProbeError>,
    cl_status_r: &'a Result<ClStatus, ProbeError>,
    cl_peers_r: &'a Result<u64, ProbeError>,
    slot_clock: Option<SlotClock>,
    now: u64,
) -> HealthSnapshot<'a> {
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
        Err(e) => el.errors.push("eth_syncing", e),
    }
    match peers_r {
        Ok(n) => el.peers = Some(*n),
        Err(e) => el.errors.push("net_peerCount", e),
    }
    match block_r {
        Ok(block) => {
            el.block_number = Some(block.number);
            el.block_age_secs = Some(now.saturating_sub(block.timestamp));
        }
        Err(e) => el.errors.push("eth_getBlockByNumber", e),
    }

    let mut cl = ClHealth::default();
    match cl_status_r {
        Ok(s) => {
            cl.syncing = Some(s.is_syncing);
            cl.sync_distance = Some(s.sync_distance);
            cl.head_slot = Some(s.head_slot);
            cl.slot_age_secs = slot_clock.map(|clock| clock.slot_age(s.head_slot, now));
        }
        Err(e) => cl.errors.push("node/syncing", e),
    }
    match cl_peers_r {
        Ok(n) => cl.peers = Some(*n),
        Err(e) => cl.errors.push("node/peer_count", e),
    }

    HealthSnapshot {
        mode: "probed",
        el,
        cl,
    }
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

    fn assert_check<D: fmt::Display>(c: Check<D>, expected_ok: bool, expected_detail: &str) {
        assert_eq!(c.ok, expected_ok);
        assert_eq!(c.detail.0.to_string(), expected_detail);
    }

    #[test]
    fn el_syncing_false_is_synced() {
        let c = eval_el_syncing(&Ok(ElSyncingResult::Synced(false))).into_check();
        assert_check(c, true, "synced");
    }

    #[test]
    fn el_syncing_object_reports_block_progress() {
        let c = eval_el_syncing(&Ok(ElSyncingResult::Syncing {
            current_block: 0x10,
            highest_block: 0x20,
        }))
        .into_check();
        assert_check(c, false, "syncing (block 16, distance 16)");
    }

    #[test]
    fn el_syncing_error_is_prefixed() {
        let r = Err("boom".into());
        let c = eval_el_syncing(&r).into_check();
        assert_check(c, false, "eth_syncing: boom");
    }

    #[test]
    fn cl_syncing_synced_is_ok() {
        let r = Ok(ClStatus {
            head_slot: 42,
            sync_distance: 0,
            is_syncing: false,
        });
        let c = eval_cl_syncing(&r).into_check();
        assert_check(c, true, "synced (slot 42, distance 0)");
    }

    #[test]
    fn cl_syncing_in_progress_fails() {
        let r = Ok(ClStatus {
            head_slot: 42,
            sync_distance: 5,
            is_syncing: true,
        });
        let c = eval_cl_syncing(&r).into_check();
        assert_check(c, false, "syncing (slot 42, distance 5)");
    }

    #[test]
    fn el_block_fresh_within_threshold_is_ok() {
        let r = Ok(ElBlockResult {
            number: 5,
            timestamp: 0x3e2,
        });
        let c = eval_el_block_fresh(&r, 60, 1000).into_check();
        assert_check(c, true, "block 5, age 6s");
    }

    #[test]
    fn el_block_fresh_past_threshold_is_stale() {
        let r = Ok(ElBlockResult {
            number: 5,
            timestamp: 0x384,
        });
        let c = eval_el_block_fresh(&r, 60, 1000).into_check();
        assert_check(c, false, "block 5 stale: 100s (max 60)");
    }

    #[test]
    fn cl_slot_fresh_disabled_when_genesis_zero() {
        let r = Ok(ClStatus {
            head_slot: 7,
            sync_distance: 0,
            is_syncing: false,
        });
        let c = eval_cl_slot_fresh(&r, SlotClock::new(0, 12), 60, 9_999_999).into_check();
        assert_check(c, true, "slot 7 (age check disabled)");
    }

    #[test]
    fn cl_slot_fresh_past_threshold_is_stale() {
        let r = Ok(ClStatus {
            head_slot: 10,
            sync_distance: 0,
            is_syncing: false,
        });
        let c = eval_cl_slot_fresh(&r, SlotClock::new(1000, 12), 60, 1300).into_check();
        assert_check(c, false, "slot 10 stale: 180s (max 60)");
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
        assert!(snap.cl.errors.is_empty());
    }

    #[test]
    fn snapshot_records_errors_and_omits_values_on_failure() {
        let r1 = Err("timeout".into());
        let r2 = Err("http 500".into());
        let r3 = Err("timeout".into());
        let r4 = Err("http 404".into());
        let r5 = Err("http 404".into());
        let snap = build_snapshot(&r1, &r2, &r3, &r4, &r5, SlotClock::new(1000, 12), 1300);
        assert_eq!(snap.el.syncing, None);
        assert_eq!(snap.el.peers, None);
        assert_eq!(snap.el.block_number, None);
        assert_eq!(snap.cl.head_slot, None);
        let json = serde_json::to_value(&snap).unwrap();
        assert_eq!(
            json["el"]["errors"],
            serde_json::json!([
                "eth_syncing: timeout",
                "net_peerCount: http 500",
                "eth_getBlockByNumber: timeout"
            ])
        );
        assert_eq!(
            json["cl"]["errors"],
            serde_json::json!(["node/syncing: http 404", "node/peer_count: http 404"])
        );

        // Capacity overflow saturates without panicking under `panic = "abort"`.
        let mut small = ErrorList::<2>::default();
        small.push("a", "1");
        small.push("b", "2");
        small.push("c", "3");
        assert_eq!(
            serde_json::to_value(&small).unwrap(),
            serde_json::json!(["a: 1", "b: 2"])
        );
    }

    #[test]
    fn snapshot_reports_el_sync_distance_while_syncing() {
        let skip_u64 = Err("skip".into());
        let skip_blk = Err("skip".into());
        let skip_cl = Err("skip".into());
        let snap = build_snapshot(
            &Ok(ElSyncingResult::Syncing {
                current_block: 0x10,
                highest_block: 0x20,
            }),
            &skip_u64,
            &skip_blk,
            &skip_cl,
            &skip_u64,
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

    fn trusted_snapshot(
        el_transport: &'static str,
        cl_transport: &'static str,
    ) -> HealthSnapshot<'static> {
        HealthSnapshot {
            mode: "trusted",
            el: ElHealth {
                transport: el_transport,
                ..ElHealth::default()
            },
            cl: ClHealth {
                transport: cl_transport,
                ..ClHealth::default()
            },
        }
    }

    #[test]
    fn trusted_snapshot_reports_mode_and_omits_unpolled_fields() {
        for (el_h2, el) in [(false, "http/1.1"), (true, "h2")] {
            for (cl_h2, cl) in [(false, "http/1.1"), (true, "h2")] {
                let snap = trusted_snapshot(el, cl);
                let v = serde_json::to_value(&snap).unwrap();
                assert_eq!(
                    v,
                    serde_json::json!({
                        "mode": "trusted",
                        "el": { "transport": el },
                        "cl": { "transport": cl },
                    })
                );
                let raw = TRUSTED_HEALTH_JSON[usize::from(el_h2)][usize::from(cl_h2)];
                assert_eq!(serde_json::from_slice::<serde_json::Value>(raw).unwrap(), v);
            }
        }
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
}
