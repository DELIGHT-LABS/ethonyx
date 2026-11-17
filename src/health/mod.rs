//! `/healthz`, `/readyz`, and `/eth/v1/node/health` HTTP endpoints and JSON views.

mod cl;
mod el;
mod eval;
mod fetch;
mod probe;

pub(crate) use eval::{HealthSource, Probe, evaluate_readiness};
pub(crate) use probe::{poll_loop, probe_once, update_metrics};

use std::fmt;
use std::sync::Arc;

use bytes::Bytes;
use http::{Response, StatusCode};
use serde::Serialize;

use crate::config::SlotClock;
use crate::response::{ResBody, empty_json_response, raw_json_response};
use crate::state::AppState;

use cl::ClStatus;
use el::{ElBlockResult, ElSyncingResult};
use eval::{ClSyncEval, ElSyncEval, FreshEval, ReadinessEval, now_unix};
use fetch::ProbeError;

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

impl<D: fmt::Display> Check<D> {
    const fn new(ok: bool, detail: D) -> Self {
        Self {
            ok,
            detail: DisplayJson(detail),
        }
    }
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

impl<'a> ReadyReport<'a> {
    const fn from_eval(eval: ReadinessEval<'a>) -> Self {
        let ready = eval.is_ready();
        let (el_block_fresh, cl_slot_fresh) = match eval.strict {
            Some((el_f, cl_f)) => (
                Some(Check::new(el_f.is_ok(), el_f)),
                Some(Check::new(cl_f.is_ok(), cl_f)),
            ),
            None => (None, None),
        };
        Self {
            status: if ready { "ready" } else { "not_ready" },
            el_syncing: Check::new(eval.el_sync.is_ok(), eval.el_sync),
            cl_syncing: Check::new(eval.cl_sync.is_ok(), eval.cl_sync),
            el_block_fresh,
            cl_slot_fresh,
        }
    }
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

const fn ready_status(ready: bool) -> StatusCode {
    if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
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
    match (&state.health, head) {
        (HealthSource::Trusted, true) => empty_json_response(StatusCode::OK),
        (HealthSource::Trusted, false) => {
            raw_json_response(StatusCode::OK, Bytes::from_static(TRUSTED_READY_JSON))
        }
        (HealthSource::Polled(rx), true) => {
            let probe = rx.borrow();
            let ready = evaluate_readiness(&state.cfg, state.slot_clock, &probe).is_ready();
            empty_json_response(ready_status(ready))
        }
        (HealthSource::Polled(rx), false) => {
            let probe = Arc::clone(&rx.borrow());
            let eval = evaluate_readiness(&state.cfg, state.slot_clock, &probe);
            json_response(ready_status(eval.is_ready()), &ReadyReport::from_eval(eval))
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
            let eval = evaluate_readiness(&state.cfg, state.slot_clock, &probe);
            classify_node_health(eval.is_ready(), state.cfg.readyz_strict, &probe)
        }
    };
    empty_json_response(code)
}

const fn classify_node_health(ready: bool, readyz_strict: bool, probe: &Probe) -> StatusCode {
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

fn json_response<T: Serialize>(code: StatusCode, body: &T) -> Response<ResBody> {
    let mut buf = Vec::with_capacity(512);
    let body_bytes = if serde_json::to_writer(&mut buf, body).is_ok() {
        Bytes::from(buf)
    } else {
        Bytes::from_static(b"{}")
    };
    raw_json_response(code, body_bytes)
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

#[cfg(test)]
mod tests {
    use super::*;

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
