//! `/healthz`, `/readyz`, and `/eth/v1/node/health` HTTP endpoints and JSON views.

mod cl;
mod el;
mod eval;
mod fetch;
mod probe;
mod reference;

pub(crate) use probe::{HealthPoller, HealthSource, warm};

use std::fmt;

use bytes::Bytes;
use http::{Response, StatusCode};
use serde::Serialize;

use crate::config::SlotClock;
use crate::response::{ResBody, empty_json_response, raw_json_response};
use crate::state::CoreState;

use el::ElSyncingResult;
use eval::{
    ClSyncEval, ElSyncEval, FreshEval, Probe, ReadinessEval, ReferenceLagEval, evaluate_readiness,
    now_unix,
};
use fetch::MethodError;

/// Serializes a [`fmt::Display`] value directly into the JSON output stream via
/// [`serde::Serializer::collect_str`] without allocating an intermediate `String`.
struct DisplayJson<D>(D);

impl<D: fmt::Display> Serialize for DisplayJson<D> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(&self.0)
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

/// `/healthz` view: a verdict-free, machine-readable snapshot of current EL,
/// optional CL, and optional Reference state. Numeric fields carry the live
/// values for the consumer to threshold; any upstream failure is recorded in
/// the layer's `errors`, and unavailable or unpolled fields are omitted. This
/// endpoint **always returns 200** — it reports state, it does not judge it.
#[derive(Serialize)]
struct HealthSnapshot<'a> {
    mode: &'static str,
    el: ElHealth<'a>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cl: Option<ClHealth<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reference: Option<ReferenceHealth<'a>>,
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
    #[serde(skip_serializing_if = "Vec::is_empty")]
    errors: Vec<DisplayJson<MethodError<'a>>>,
}

#[derive(Serialize, Default)]
struct ClHealth<'a> {
    /// HTTP version ethonyx uses for the Beacon hop (`http/1.1` for cleartext;
    /// `h2` or `http/1.1` for `https` once ALPN negotiation is observed).
    transport: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    syncing: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    is_optimistic: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    el_offline: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sync_distance: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    peers: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    head_slot: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    slot_age_secs: Option<u64>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    errors: Vec<DisplayJson<MethodError<'a>>>,
}

#[derive(Serialize)]
struct ReferenceHealth<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    block_number: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    block_lag: Option<u64>,
    #[serde(skip_serializing_if = "<[String]>::is_empty")]
    errors: &'a [String],
}

/// `/readyz` view: the load-balancer readiness verdict. Defaults to sync status
/// (plus Engine API flags when CL is configured, and reference block lag when
/// `--el-reference-url` is configured); freshness fields appear only under
/// `--readyz-strict`. Peer count is intentionally never part of readiness.
#[derive(Serialize)]
#[serde(bound = "")]
struct ReadyReport<'a, E: fmt::Display = ElSyncEval<'a>, C: fmt::Display = ClSyncEval<'a>> {
    status: &'static str,
    el_syncing: Check<E>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cl_syncing: Option<Check<C>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    el_reference_lag: Option<Check<ReferenceLagEval>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    el_block_fresh: Option<Check<FreshEval<'a>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cl_slot_fresh: Option<Check<FreshEval<'a>>>,
}

impl<'a> ReadyReport<'a> {
    fn from_eval(eval: ReadinessEval<'a>, ready: bool) -> Self {
        Self {
            status: if ready { "ready" } else { "not_ready" },
            el_syncing: Check::new(eval.el_sync.is_ok(), eval.el_sync),
            cl_syncing: eval.cl_sync.map(|c| Check::new(c.is_ok(), c)),
            el_reference_lag: eval.el_reference_lag.map(|l| Check::new(l.is_ok(), l)),
            el_block_fresh: eval.el_block_fresh.map(|f| Check::new(f.is_ok(), f)),
            cl_slot_fresh: eval.cl_slot_fresh.map(|f| Check::new(f.is_ok(), f)),
        }
    }
}

fn trusted_ready(has_cl: bool) -> ReadyReport<'static, &'static str, &'static str> {
    const DETAIL: &str = "trusted upstream (unpolled)";
    ReadyReport {
        status: "ready",
        el_syncing: Check::new(true, DETAIL),
        cl_syncing: has_cl.then_some(Check::new(true, DETAIL)),
        el_reference_lag: None,
        el_block_fresh: None,
        cl_slot_fresh: None,
    }
}

/// `/healthz` — verdict-free EL (+ optional CL / Reference) snapshot, **always 200**.
/// Serves the latest background poll; block / slot ages are recomputed live per
/// request. Under `--trust-upstream`, reports `mode: "trusted"` and omits unpolled
/// fields instead of fabricating placeholder metrics.
pub(crate) fn report(state: &CoreState, head: bool) -> Response<ResBody> {
    if head {
        return empty_json_response(StatusCode::OK);
    }
    let el_transport = state.el.transport.label();
    let cl_transport = state.cl.as_ref().map(|cl| cl.beacon.transport.label());
    match &state.health {
        HealthSource::Trusted => {
            let snap = trusted_snapshot(el_transport, cl_transport);
            json_response(StatusCode::OK, &snap)
        }
        HealthSource::Polled { rx, policy } => {
            // Clone the `Arc`-backed `Probe` handle so the `watch::Ref` read lock is released before serialization.
            let probe = rx.borrow().clone();
            let snap = build_snapshot(
                &probe,
                el_transport,
                cl_transport,
                policy.slot_clock,
                now_unix(),
            );
            json_response(StatusCode::OK, &snap)
        }
    }
}

fn trusted_snapshot(
    el_transport: &'static str,
    cl_transport: Option<&'static str>,
) -> HealthSnapshot<'static> {
    HealthSnapshot {
        mode: "trusted",
        el: ElHealth {
            transport: el_transport,
            ..ElHealth::default()
        },
        cl: cl_transport.map(|transport| ClHealth {
            transport,
            ..ClHealth::default()
        }),
        reference: None,
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
pub(crate) fn ready(state: &CoreState, head: bool) -> Response<ResBody> {
    match (&state.health, head) {
        (HealthSource::Trusted, true) => empty_json_response(StatusCode::OK),
        (HealthSource::Trusted, false) => {
            json_response(StatusCode::OK, &trusted_ready(state.cl.is_some()))
        }
        (HealthSource::Polled { rx, policy }, true) => {
            let probe = rx.borrow();
            let ready = evaluate_readiness(*policy, &probe).is_ready();
            empty_json_response(ready_status(ready))
        }
        (HealthSource::Polled { rx, policy }, false) => {
            // Clone the `Arc`-backed `Probe` handle so the `watch::Ref` read lock is released before serialization.
            let probe = rx.borrow().clone();
            let eval = evaluate_readiness(*policy, &probe);
            let ready = eval.is_ready();
            json_response(ready_status(ready), &ReadyReport::from_eval(eval, ready))
        }
    }
}

/// `/eth/v1/node/health` — Consensus Layer Beacon API standard node health endpoint.
///
/// Returns standard status codes according to the Ethereum Beacon API specification:
/// - 200 OK: Node is healthy and all enabled readiness gates pass.
/// - 206 Partial Content: Both layers are reachable (`!el_offline`) and at least one is actively syncing.
/// - 503 Service Unavailable: Node is unreachable, `el_offline`, stalled (`is_optimistic` while not syncing),
///   lagging behind reference RPCs, or stale under `--readyz-strict`.
///
/// Serves an empty body with `content-type: application/json` in compliance with the spec.
pub(crate) fn node_health(state: &CoreState, query: Option<&str>) -> Response<ResBody> {
    let Ok(syncing_status) = parse_syncing_status(query) else {
        return crate::response::text_response(
            StatusCode::BAD_REQUEST,
            "invalid syncing_status query parameter",
        );
    };
    let code = match &state.health {
        HealthSource::Trusted => StatusCode::OK,
        HealthSource::Polled { rx, policy } => {
            let probe = rx.borrow();
            classify_node_health(evaluate_readiness(*policy, &probe), syncing_status)
        }
    };
    empty_json_response(code)
}

fn parse_syncing_status(query: Option<&str>) -> Result<StatusCode, ()> {
    let Some(query) = query else {
        return Ok(StatusCode::PARTIAL_CONTENT);
    };
    let mut custom = None;
    for pair in query.split('&') {
        let Some((key, value)) = pair.split_once('=') else {
            if pair == "syncing_status" {
                return Err(());
            }
            continue;
        };
        if key == "syncing_status" {
            let code = StatusCode::from_bytes(value.as_bytes()).map_err(|_| ())?;
            if !(200..=599).contains(&code.as_u16()) {
                return Err(());
            }
            custom = Some(code);
        }
    }
    Ok(custom.unwrap_or(StatusCode::PARTIAL_CONTENT))
}

fn classify_node_health(eval: ReadinessEval<'_>, syncing_status: StatusCode) -> StatusCode {
    if eval.is_ready() {
        return StatusCode::OK;
    }
    let Some(cl_sync) = eval.cl_sync else {
        return StatusCode::SERVICE_UNAVAILABLE;
    };
    let both_reachable = eval.el_sync.is_reachable() && cl_sync.is_reachable();
    if both_reachable && (eval.el_sync.is_syncing() || cl_sync.is_syncing()) {
        syncing_status
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

/// Fold the upstream probe results into a numeric snapshot. Successful signals
/// populate their fields; failed ones are pushed onto the layer's `errors` and
/// leave the field unset.
fn build_snapshot<'a>(
    probe: &'a Probe,
    el_transport: &'static str,
    cl_transport: Option<&'static str>,
    slot_clock: Option<SlotClock>,
    now: Option<u64>,
) -> HealthSnapshot<'a> {
    let local = &probe.local;
    let mut el = ElHealth {
        transport: el_transport,
        ..ElHealth::default()
    };
    match &local.el {
        Ok(batch) => {
            match &batch.syncing {
                Ok(ElSyncingResult::Synced) => el.syncing = Some(false),
                Ok(ElSyncingResult::SyncingBool) => el.syncing = Some(true),
                Ok(ElSyncingResult::SyncingProgress { distance, .. }) => {
                    el.syncing = Some(true);
                    el.sync_distance = Some(*distance);
                }
                Err(e) => el.errors.push(DisplayJson(MethodError("eth_syncing", e))),
            }
            match &batch.peers {
                Ok(peers) => el.peers = *peers,
                Err(e) => el.errors.push(DisplayJson(MethodError("net_peerCount", e))),
            }
            match &batch.block {
                Ok(block) => {
                    el.block_number = Some(block.number);
                    el.block_age_secs = now.map(|now| block.age(now));
                }
                Err(e) => el
                    .errors
                    .push(DisplayJson(MethodError("eth_getBlockByNumber", e))),
            }
        }
        Err(e) => {
            el.errors.extend([
                DisplayJson(MethodError("eth_syncing", e)),
                DisplayJson(MethodError("net_peerCount", e)),
                DisplayJson(MethodError("eth_getBlockByNumber", e)),
            ]);
        }
    }

    let cl = cl_transport
        .zip(local.cl.as_ref())
        .map(|(transport, cl_probe)| {
            let mut cl = ClHealth {
                transport,
                ..ClHealth::default()
            };
            match &cl_probe.status {
                Ok(s) => {
                    cl.syncing = Some(s.is_syncing);
                    cl.is_optimistic = Some(s.is_optimistic);
                    cl.el_offline = Some(s.el_offline);
                    cl.sync_distance = Some(s.sync_distance);
                    cl.head_slot = Some(s.head_slot);
                    cl.slot_age_secs = slot_clock
                        .zip(now)
                        .map(|(clock, now)| clock.slot_age(s.head_slot, now));
                }
                Err(e) => cl.errors.push(DisplayJson(MethodError("node/syncing", e))),
            }
            match &cl_probe.peers {
                Ok(n) => cl.peers = Some(*n),
                Err(e) => cl
                    .errors
                    .push(DisplayJson(MethodError("node/peer_count", e))),
            }
            cl
        });

    let reference = probe.reference.as_deref().map(|ref_probe| {
        let sample = ref_probe.state.sample();
        ReferenceHealth {
            block_number: sample.map(|s| s.block_number),
            block_lag: sample.map(|s| s.block_lag),
            errors: &ref_probe.errors,
        }
    });

    HealthSnapshot {
        mode: "probed",
        el,
        cl,
        reference,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::cl::ClStatus;
    use super::el::{ElBatchResult, ElBlockResult};
    use super::eval::{ClProbe, LocalProbe};
    use super::fetch::HealthError;
    use super::reference::{ReferenceProbe, ReferenceSample, ReferenceState};
    use super::*;

    fn mk_probe(local: LocalProbe, reference: Option<ReferenceProbe>) -> Probe {
        Probe {
            local: Arc::new(local),
            reference: reference.map(Arc::new),
        }
    }

    const fn synced_cl() -> ClStatus {
        ClStatus {
            head_slot: 100,
            sync_distance: 0,
            is_syncing: false,
            is_optimistic: false,
            el_offline: false,
        }
    }

    const fn syncing_cl() -> ClStatus {
        ClStatus {
            head_slot: 50,
            sync_distance: 50,
            is_syncing: true,
            is_optimistic: true,
            el_offline: false,
        }
    }

    fn mk_dual_probe(
        el_syncing: Result<ElSyncingResult, HealthError>,
        el_block: Result<ElBlockResult, HealthError>,
        cl_status: Result<ClStatus, HealthError>,
        reference: Option<ReferenceProbe>,
    ) -> Probe {
        let el_peers = match &el_syncing {
            Ok(_) => Ok(Some(10)),
            Err(e) => Err(e.clone()),
        };
        let cl_peers = match &cl_status {
            Ok(_) => Ok(10),
            Err(e) => Err(e.clone()),
        };
        mk_probe(
            LocalProbe {
                el: Ok(ElBatchResult {
                    syncing: el_syncing,
                    peers: el_peers,
                    block: el_block,
                }),
                cl: Some(ClProbe {
                    status: cl_status,
                    peers: cl_peers,
                }),
            },
            reference,
        )
    }

    #[test]
    fn snapshot_reports_numeric_values_when_healthy_and_omits_disabled_layers() {
        let probe = mk_probe(
            LocalProbe {
                el: Ok(ElBatchResult {
                    syncing: Ok(ElSyncingResult::Synced),
                    peers: Ok(Some(0x10)),
                    block: Ok(ElBlockResult {
                        number: 5,
                        timestamp: 0x3e2,
                    }),
                }),
                cl: Some(ClProbe {
                    status: Ok(ClStatus {
                        head_slot: 10,
                        sync_distance: 0,
                        is_syncing: false,
                        is_optimistic: false,
                        el_offline: false,
                    }),
                    peers: Ok(64),
                }),
            },
            Some(ReferenceProbe {
                state: ReferenceState::Active(ReferenceSample {
                    block_number: 6,
                    block_lag: 1,
                }),
                errors: Vec::new(),
            }),
        );
        let snap = build_snapshot(
            &probe,
            "h2c",
            Some("http/1.1"),
            SlotClock::new(1000, 12),
            Some(1300),
        );
        assert_eq!(snap.el.transport, "h2c");
        assert_eq!(snap.el.syncing, Some(false));
        assert_eq!(snap.el.peers, Some(16));
        assert_eq!(snap.el.block_number, Some(5));
        assert_eq!(snap.el.block_age_secs, Some(306));
        assert!(snap.el.errors.is_empty());
        let cl = snap.cl.as_ref().unwrap();
        assert_eq!(cl.transport, "http/1.1");
        assert_eq!(cl.syncing, Some(false));
        assert_eq!(cl.is_optimistic, Some(false));
        assert_eq!(cl.el_offline, Some(false));
        assert_eq!(cl.peers, Some(64));
        assert_eq!(cl.head_slot, Some(10));
        assert_eq!(cl.slot_age_secs, Some(180));
        assert!(cl.errors.is_empty());
        let reference = snap.reference.as_ref().unwrap();
        assert_eq!(reference.block_number, Some(6));
        assert_eq!(reference.block_lag, Some(1));
        assert!(reference.errors.is_empty());

        // EL-only probe with unsupported `net_peerCount` omits `cl`, `reference`, and `el.peers`.
        let el_only = mk_probe(
            LocalProbe {
                el: Ok(ElBatchResult {
                    syncing: Ok(ElSyncingResult::Synced),
                    peers: Ok(None),
                    block: Ok(ElBlockResult {
                        number: 5,
                        timestamp: 0x3e2,
                    }),
                }),
                cl: None,
            },
            None,
        );
        let el_only_json =
            serde_json::to_value(build_snapshot(&el_only, "http/1.1", None, None, Some(1300)))
                .unwrap();
        assert!(el_only_json.get("cl").is_none());
        assert!(el_only_json.get("reference").is_none());
        assert!(el_only_json["el"].get("peers").is_none());
        assert!(el_only_json["el"].get("errors").is_none());

        // A pre-epoch system clock (`now == None`) omits age fields rather than fabricating 0.
        let no_clock_snap = build_snapshot(
            &probe,
            "h2c",
            Some("http/1.1"),
            SlotClock::new(1000, 12),
            None,
        );
        assert_eq!(no_clock_snap.el.block_age_secs, None);
        assert_eq!(no_clock_snap.cl.as_ref().unwrap().slot_age_secs, None);
    }

    #[test]
    fn snapshot_records_errors_and_omits_values_on_failure() {
        let probe = mk_probe(
            LocalProbe {
                el: Ok(ElBatchResult {
                    syncing: Err(HealthError::Timeout),
                    peers: Err(HealthError::HttpStatus(StatusCode::INTERNAL_SERVER_ERROR)),
                    block: Err(HealthError::Timeout),
                }),
                cl: Some(ClProbe {
                    status: Err(HealthError::HttpStatus(StatusCode::NOT_FOUND)),
                    peers: Err(HealthError::HttpStatus(StatusCode::NOT_FOUND)),
                }),
            },
            Some(ReferenceProbe {
                state: ReferenceState::Unavailable,
                errors: vec!["https://ref.example.com/<redacted>: timeout".into()],
            }),
        );
        let snap = build_snapshot(
            &probe,
            "http/1.1",
            Some("http/1.1"),
            SlotClock::new(1000, 12),
            Some(1300),
        );
        assert_eq!(snap.el.syncing, None);
        assert_eq!(snap.el.peers, None);
        assert_eq!(snap.el.block_number, None);
        assert_eq!(snap.cl.as_ref().unwrap().head_slot, None);
        let json = serde_json::to_value(&snap).unwrap();
        assert_eq!(
            json["el"]["errors"],
            serde_json::json!([
                "eth_syncing: timeout",
                "net_peerCount: http 500 Internal Server Error",
                "eth_getBlockByNumber: timeout"
            ])
        );
        assert_eq!(
            json["cl"]["errors"],
            serde_json::json!([
                "node/syncing: http 404 Not Found",
                "node/peer_count: http 404 Not Found"
            ])
        );
        assert_eq!(
            json["reference"],
            serde_json::json!({
                "errors": ["https://ref.example.com/<redacted>: timeout"]
            })
        );

        let batch_err = mk_probe(
            LocalProbe {
                el: Err(HealthError::Timeout),
                cl: None,
            },
            None,
        );
        let batch_json = serde_json::to_value(build_snapshot(
            &batch_err,
            "http/1.1",
            None,
            None,
            Some(1300),
        ))
        .unwrap();
        assert_eq!(
            batch_json["el"]["errors"],
            serde_json::json!([
                "eth_syncing: timeout",
                "net_peerCount: timeout",
                "eth_getBlockByNumber: timeout"
            ])
        );
    }

    #[test]
    fn snapshot_reports_el_sync_distance_while_syncing() {
        let probe = mk_probe(
            LocalProbe {
                el: Ok(ElBatchResult {
                    syncing: Ok(ElSyncingResult::SyncingProgress {
                        current_block: 0x10,
                        distance: 0x10,
                    }),
                    peers: Err(HealthError::Timeout),
                    block: Err(HealthError::Timeout),
                }),
                cl: None,
            },
            None,
        );
        let snap = build_snapshot(&probe, "http/1.1", None, None, Some(1300));
        assert_eq!(snap.el.syncing, Some(true));
        assert_eq!(snap.el.sync_distance, Some(16));
    }

    #[test]
    fn snapshot_omits_cl_slot_age_when_genesis_disabled() {
        let probe = mk_probe(
            LocalProbe {
                el: Ok(ElBatchResult {
                    syncing: Ok(ElSyncingResult::Synced),
                    peers: Ok(Some(0x10)),
                    block: Ok(ElBlockResult {
                        number: 5,
                        timestamp: 0x3e2,
                    }),
                }),
                cl: Some(ClProbe {
                    status: Ok(ClStatus {
                        head_slot: 10,
                        sync_distance: 0,
                        is_syncing: false,
                        is_optimistic: false,
                        el_offline: false,
                    }),
                    peers: Ok(64),
                }),
            },
            None,
        );
        let snap = build_snapshot(
            &probe,
            "http/1.1",
            Some("http/1.1"),
            SlotClock::new(0, 12),
            Some(1300),
        );
        assert_eq!(snap.cl.as_ref().unwrap().head_slot, Some(10));
        assert_eq!(snap.cl.as_ref().unwrap().slot_age_secs, None);
    }

    #[test]
    fn trusted_snapshot_and_ready_json_match_expected_schema() {
        for el in ["http/1.1", "h2"] {
            let el_only = serde_json::to_value(trusted_snapshot(el, None)).unwrap();
            assert_eq!(
                el_only,
                serde_json::json!({
                    "mode": "trusted",
                    "el": { "transport": el },
                })
            );
            for cl in ["http/1.1", "h2"] {
                let both = serde_json::to_value(trusted_snapshot(el, Some(cl))).unwrap();
                assert_eq!(
                    both,
                    serde_json::json!({
                        "mode": "trusted",
                        "el": { "transport": el },
                        "cl": { "transport": cl },
                    })
                );
            }
        }
        assert_eq!(
            serde_json::to_value(trusted_ready(true)).unwrap(),
            serde_json::json!({
                "status": "ready",
                "el_syncing": { "ok": true, "detail": "trusted upstream (unpolled)" },
                "cl_syncing": { "ok": true, "detail": "trusted upstream (unpolled)" },
            })
        );
        assert_eq!(
            serde_json::to_value(trusted_ready(false)).unwrap(),
            serde_json::json!({
                "status": "ready",
                "el_syncing": { "ok": true, "detail": "trusted upstream (unpolled)" },
            })
        );
    }

    #[test]
    fn node_health_returns_206_only_when_both_layers_reachable_and_syncing() {
        use super::eval::evaluate_readiness_at;

        const NOW: u64 = 2_200;
        let no_clock =
            || -> Option<u64> { panic!("non-strict evaluation must not read the clock") };
        let eval_default = |policy, probe| evaluate_readiness_at(policy, probe, no_clock);
        let eval_at = |policy, probe| evaluate_readiness_at(policy, probe, || Some(NOW));

        let default_policy = crate::config::HealthPolicy {
            el_max_reference_lag: 1,
            strict: None,
            slot_clock: None,
        };
        let strict_policy = crate::config::HealthPolicy {
            strict: Some(crate::config::StrictFreshness {
                el_max_block_age_secs: 60,
                cl_max_slot_age_secs: 60,
            }),
            ..default_policy
        };
        let strict_clock_policy = crate::config::HealthPolicy {
            slot_clock: SlotClock::new(1000, 12),
            ..strict_policy
        };

        let synced_el = ElSyncingResult::Synced;
        let syncing_el = ElSyncingResult::SyncingProgress {
            current_block: 10,
            distance: 10,
        };
        // Exact strict freshness boundaries at `NOW = 2200` (`max_age = 60`):
        // `age == 60` stays fresh, `age == 61` is stale.
        let fresh_block = ElBlockResult {
            number: 10,
            timestamp: NOW - 60,
        };
        let stale_block = ElBlockResult {
            number: 10,
            timestamp: NOW - 61,
        };

        // 1. Ready -> 200 OK (default and strict at exact freshness boundary); stale synced -> 503.
        let ready_probe = mk_dual_probe(Ok(synced_el), Ok(fresh_block), Ok(synced_cl()), None);
        assert_eq!(
            classify_node_health(
                eval_default(default_policy, &ready_probe),
                StatusCode::PARTIAL_CONTENT
            ),
            StatusCode::OK
        );
        assert_eq!(
            classify_node_health(
                eval_at(strict_clock_policy, &ready_probe),
                StatusCode::PARTIAL_CONTENT
            ),
            StatusCode::OK
        );
        let stale_synced_probe =
            mk_dual_probe(Ok(synced_el), Ok(stale_block), Ok(synced_cl()), None);
        assert_eq!(
            classify_node_health(
                eval_at(strict_clock_policy, &stale_synced_probe),
                StatusCode::PARTIAL_CONTENT
            ),
            StatusCode::SERVICE_UNAVAILABLE
        );
        // Pre-epoch system clock (`None`) must fail closed under `--readyz-strict`.
        let clock_err_eval = evaluate_readiness_at(strict_clock_policy, &ready_probe, || None);
        assert!(!clock_err_eval.is_ready());
        assert_eq!(
            clock_err_eval.el_block_fresh.unwrap().to_string(),
            "system clock before unix epoch"
        );
        assert_eq!(
            classify_node_health(clock_err_eval, StatusCode::PARTIAL_CONTENT),
            StatusCode::SERVICE_UNAVAILABLE
        );

        // 2. EL syncing + CL reachable -> 206 Partial Content (or custom `syncing_status`
        // when provided, even when block is stale/missing under --readyz-strict, CL reports
        // `is_optimistic: true`, or node lags behind reference!)
        let el_sync_probe = mk_dual_probe(
            Ok(syncing_el),
            Err(HealthError::decode_msg("latest block not yet populated")),
            Ok(ClStatus {
                is_optimistic: true,
                ..synced_cl()
            }),
            Some(ReferenceProbe {
                state: ReferenceState::Active(ReferenceSample {
                    block_number: 500,
                    block_lag: 490,
                }),
                errors: Vec::new(),
            }),
        );
        assert_eq!(
            classify_node_health(
                eval_default(default_policy, &el_sync_probe),
                StatusCode::PARTIAL_CONTENT
            ),
            StatusCode::PARTIAL_CONTENT
        );
        assert_eq!(
            classify_node_health(eval_default(default_policy, &el_sync_probe), StatusCode::OK),
            StatusCode::OK
        );
        assert_eq!(
            classify_node_health(
                eval_at(strict_policy, &el_sync_probe),
                StatusCode::PARTIAL_CONTENT
            ),
            StatusCode::PARTIAL_CONTENT
        );

        let el_sync_bool_probe = mk_dual_probe(
            Ok(ElSyncingResult::SyncingBool),
            Ok(stale_block),
            Ok(synced_cl()),
            None,
        );
        assert_eq!(
            classify_node_health(
                eval_default(default_policy, &el_sync_bool_probe),
                StatusCode::PARTIAL_CONTENT
            ),
            StatusCode::PARTIAL_CONTENT
        );

        // 3. EL syncing + CL down -> 503 Service Unavailable (must NOT mask CL crash as 206)
        let cl_down_probe = mk_dual_probe(
            Ok(syncing_el),
            Ok(stale_block),
            Err(HealthError::Transport("connection refused".into())),
            None,
        );
        assert_eq!(
            classify_node_health(
                eval_default(default_policy, &cl_down_probe),
                StatusCode::PARTIAL_CONTENT
            ),
            StatusCode::SERVICE_UNAVAILABLE
        );

        // 4. CL syncing + EL down -> 503 Service Unavailable (must NOT mask EL crash as 206)
        let el_down_probe = mk_dual_probe(
            Err(HealthError::Timeout),
            Err(HealthError::Timeout),
            Ok(syncing_cl()),
            None,
        );
        assert_eq!(
            classify_node_health(
                eval_default(default_policy, &el_down_probe),
                StatusCode::PARTIAL_CONTENT
            ),
            StatusCode::SERVICE_UNAVAILABLE
        );

        // 5. CL syncing + EL reachable under --readyz-strict -> 206, unless EL claims synced with unreadable block -> 503
        let cl_sync_probe = mk_dual_probe(Ok(synced_el), Ok(stale_block), Ok(syncing_cl()), None);
        assert_eq!(
            classify_node_health(
                eval_at(strict_clock_policy, &cl_sync_probe),
                StatusCode::PARTIAL_CONTENT
            ),
            StatusCode::PARTIAL_CONTENT
        );
        let strict_block_err_probe = mk_dual_probe(
            Ok(synced_el),
            Err(HealthError::Rpc("rpc error".into())),
            Ok(syncing_cl()),
            None,
        );
        assert_eq!(
            classify_node_health(
                eval_at(strict_policy, &strict_block_err_probe),
                StatusCode::PARTIAL_CONTENT
            ),
            StatusCode::SERVICE_UNAVAILABLE
        );

        // 6. Silent stall (`is_optimistic: true` while neither layer is syncing), `el_offline: true`,
        // or reference lag while synced -> 503 Service Unavailable.
        let silent_optimistic_probe = mk_dual_probe(
            Ok(synced_el),
            Ok(fresh_block),
            Ok(ClStatus {
                is_optimistic: true,
                ..synced_cl()
            }),
            None,
        );
        assert_eq!(
            classify_node_health(
                eval_default(default_policy, &silent_optimistic_probe),
                StatusCode::PARTIAL_CONTENT
            ),
            StatusCode::SERVICE_UNAVAILABLE
        );

        let el_offline_probe = mk_dual_probe(
            Ok(syncing_el),
            Ok(fresh_block),
            Ok(ClStatus {
                el_offline: true,
                ..syncing_cl()
            }),
            None,
        );
        assert_eq!(
            classify_node_health(
                eval_default(default_policy, &el_offline_probe),
                StatusCode::PARTIAL_CONTENT
            ),
            StatusCode::SERVICE_UNAVAILABLE
        );

        let lagging_synced_probe = mk_dual_probe(
            Ok(synced_el),
            Ok(fresh_block),
            Ok(synced_cl()),
            Some(ReferenceProbe {
                state: ReferenceState::Active(ReferenceSample {
                    block_number: 12,
                    block_lag: 2,
                }),
                errors: Vec::new(),
            }),
        );
        assert_eq!(
            classify_node_health(
                eval_default(default_policy, &lagging_synced_probe),
                StatusCode::PARTIAL_CONTENT
            ),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[test]
    fn parse_syncing_status_accepts_canonical_2xx_to_5xx_and_rejects_non_canonical_values() {
        assert_eq!(parse_syncing_status(None), Ok(StatusCode::PARTIAL_CONTENT));
        assert_eq!(
            parse_syncing_status(Some("syncing_status=200")),
            Ok(StatusCode::OK)
        );
        assert_eq!(
            parse_syncing_status(Some("foo=1&syncing_status=503")),
            Ok(StatusCode::SERVICE_UNAVAILABLE)
        );
        assert_eq!(
            parse_syncing_status(Some("syncing_status=204&syncing_status=200")),
            Ok(StatusCode::OK)
        );
        for bad in [
            "syncing_status",
            "syncing_status=",
            "syncing_status=+200",
            "syncing_status=00200",
            "syncing_status=0200",
            "syncing_status=100",
            "syncing_status=199",
            "syncing_status=600",
            "syncing_status=200 ",
            "syncing_status=abc",
        ] {
            assert_eq!(parse_syncing_status(Some(bad)), Err(()), "{bad}");
        }
    }
}
