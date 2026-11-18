//! `/healthz`, `/readyz`, and `/eth/v1/node/health` HTTP endpoints and JSON views.

mod cl;
mod el;
mod eval;
mod fetch;
mod probe;
mod reference;

pub(crate) use eval::HealthSource;
pub(crate) use probe::{HealthPoller, channel};

use std::fmt;

use bytes::Bytes;
use http::{Response, StatusCode};
use serde::Serialize;

use crate::config::SlotClock;
use crate::response::{ResBody, empty_json_response, raw_json_response};
use crate::state::AppState;

use el::ElSyncingResult;
use eval::{
    ClSyncEval, ElSyncEval, FreshEval, Probe, ReadinessEval, ReferenceLagEval, evaluate_readiness,
    now_unix,
};

const TRUSTED_READY_JSON: &[u8] = br#"{"status":"ready","el_syncing":{"ok":true,"detail":"trusted upstream (unpolled)"},"cl_syncing":{"ok":true,"detail":"trusted upstream (unpolled)"}}"#;
const TRUSTED_READY_EL_ONLY_JSON: &[u8] =
    br#"{"status":"ready","el_syncing":{"ok":true,"detail":"trusted upstream (unpolled)"}}"#;

/// Serializes a [`fmt::Display`] value directly into the JSON output stream via
/// [`serde::Serializer::collect_str`] without allocating an intermediate `String`.
struct DisplayJson<D>(D);

impl<D: fmt::Display> Serialize for DisplayJson<D> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(&self.0)
    }
}

/// Fixed-capacity stack list of borrowed probe errors so `/healthz` serializes
/// outage reports without per-request `Vec` or `format!` `String` allocations.
struct ErrorList<'a, const N: usize> {
    items: [&'a str; N],
    len: usize,
}

impl<const N: usize> Default for ErrorList<'_, N> {
    fn default() -> Self {
        Self {
            items: [""; N],
            len: 0,
        }
    }
}

impl<'a, const N: usize> ErrorList<'a, N> {
    const fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn push(&mut self, err: &'a str) {
        if let Some(slot) = self.items.get_mut(self.len) {
            *slot = err;
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
    #[serde(skip_serializing_if = "ErrorList::is_empty")]
    errors: ErrorList<'a, 2>,
}

#[derive(Serialize)]
struct ReferenceHealth<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    block_number: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    block_lag: Option<u64>,
    #[serde(skip_serializing_if = "<[Box<str>]>::is_empty")]
    errors: &'a [Box<str>],
}

/// `/readyz` view: the load-balancer readiness verdict. Defaults to sync status
/// (plus Engine API flags when CL is configured, and reference block lag when
/// `--el-reference-url` is configured); freshness fields appear only under
/// `--readyz-strict`. Peer count is intentionally never part of readiness.
#[derive(Serialize)]
struct ReadyReport<'a> {
    status: &'static str,
    el_syncing: Check<ElSyncEval<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cl_syncing: Option<Check<ClSyncEval<'a>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    el_reference_lag: Option<Check<ReferenceLagEval>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    el_block_fresh: Option<Check<FreshEval<'a>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cl_slot_fresh: Option<Check<FreshEval<'a>>>,
}

impl<'a> ReadyReport<'a> {
    fn from_eval(eval: ReadinessEval<'a>) -> Self {
        let ready = eval.is_ready();
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

/// `/healthz` — verdict-free EL (+ optional CL / Reference) snapshot, **always 200**.
/// Serves the latest background poll; block / slot ages are recomputed live per
/// request. Under `--trust-upstream`, reports `mode: "trusted"` and omits unpolled
/// fields instead of fabricating placeholder metrics.
pub(crate) fn report(state: &AppState, head: bool) -> Response<ResBody> {
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
        HealthSource::Polled(rx) => {
            // Clone the `Arc`-backed `Probe` handle so the `watch::Ref` read lock is released before serialization.
            let probe = rx.borrow().clone();
            let snap = build_snapshot(
                &probe,
                el_transport,
                cl_transport,
                state.opts.slot_clock,
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
pub(crate) fn ready(state: &AppState, head: bool) -> Response<ResBody> {
    match (&state.health, head) {
        (HealthSource::Trusted, true) => empty_json_response(StatusCode::OK),
        (HealthSource::Trusted, false) => {
            let raw = if state.cl.is_some() {
                TRUSTED_READY_JSON
            } else {
                TRUSTED_READY_EL_ONLY_JSON
            };
            raw_json_response(StatusCode::OK, Bytes::from_static(raw))
        }
        (HealthSource::Polled(rx), true) => {
            let probe = rx.borrow();
            let ready = evaluate_readiness(&state.opts, &probe).is_ready();
            empty_json_response(ready_status(ready))
        }
        (HealthSource::Polled(rx), false) => {
            // Clone the `Arc`-backed `Probe` handle so the `watch::Ref` read lock is released before serialization.
            let probe = rx.borrow().clone();
            let eval = evaluate_readiness(&state.opts, &probe);
            json_response(ready_status(eval.is_ready()), &ReadyReport::from_eval(eval))
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
pub(crate) fn node_health(state: &AppState) -> Response<ResBody> {
    let code = match &state.health {
        HealthSource::Trusted => StatusCode::OK,
        HealthSource::Polled(rx) => {
            let probe = rx.borrow();
            classify_node_health(evaluate_readiness(&state.opts, &probe))
        }
    };
    empty_json_response(code)
}

fn classify_node_health(eval: ReadinessEval<'_>) -> StatusCode {
    if eval.is_ready() {
        return StatusCode::OK;
    }
    let Some(cl_sync) = eval.cl_sync else {
        return StatusCode::SERVICE_UNAVAILABLE;
    };
    let both_reachable = eval.el_sync.is_reachable() && cl_sync.is_reachable();
    if both_reachable && (eval.el_sync.is_syncing() || cl_sync.is_syncing()) {
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

/// Fold the upstream probe results into a numeric snapshot. Successful signals
/// populate their fields; failed ones are pushed onto the layer's `errors` and
/// leave the field unset.
fn build_snapshot<'a>(
    probe: &'a Probe,
    el_transport: &'static str,
    cl_transport: Option<&'static str>,
    slot_clock: Option<SlotClock>,
    now: u64,
) -> HealthSnapshot<'a> {
    let local = &probe.local;
    let mut el = ElHealth {
        transport: el_transport,
        ..ElHealth::default()
    };
    match &local.el_syncing {
        Ok(ElSyncingResult::Synced(false)) => el.syncing = Some(false),
        Ok(ElSyncingResult::Synced(true)) => el.syncing = Some(true),
        Ok(ElSyncingResult::Syncing {
            current_block,
            highest_block,
        }) => {
            el.syncing = Some(true);
            el.sync_distance = Some(highest_block.saturating_sub(*current_block));
        }
        Err(e) => el.errors.push(e),
    }
    match &local.el_peers {
        Ok(peers) => el.peers = *peers,
        Err(e) => el.errors.push(e),
    }
    match &local.el_block {
        Ok(block) => {
            el.block_number = Some(block.number);
            el.block_age_secs = Some(now.saturating_sub(block.timestamp));
        }
        Err(e) => el.errors.push(e),
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
                    cl.slot_age_secs = slot_clock.map(|clock| clock.slot_age(s.head_slot, now));
                }
                Err(e) => cl.errors.push(e),
            }
            match &cl_probe.peers {
                Ok(n) => cl.peers = Some(*n),
                Err(e) => cl.errors.push(e),
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
    use super::el::ElBlockResult;
    use super::eval::{ClProbe, LocalProbe};
    use super::reference::{ReferenceProbe, ReferenceSample, ReferenceState};
    use super::*;

    fn mk_probe(local: LocalProbe, reference: Option<ReferenceProbe>) -> Probe {
        Probe {
            local: Arc::new(local),
            reference: reference.map(Arc::new),
            published_ready: None,
        }
    }

    #[test]
    fn snapshot_reports_numeric_values_when_healthy_and_omits_disabled_layers() {
        let probe = mk_probe(
            LocalProbe {
                el_syncing: Ok(ElSyncingResult::Synced(false)),
                el_peers: Ok(Some(0x10)),
                el_block: Ok(ElBlockResult {
                    number: 5,
                    timestamp: 0x3e2,
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
                errors: Box::new([]),
            }),
        );
        let snap = build_snapshot(
            &probe,
            "h2c",
            Some("http/1.1"),
            SlotClock::new(1000, 12),
            1300,
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
                el_syncing: Ok(ElSyncingResult::Synced(false)),
                el_peers: Ok(None),
                el_block: Ok(ElBlockResult {
                    number: 5,
                    timestamp: 0x3e2,
                }),
                cl: None,
            },
            None,
        );
        let el_only_json =
            serde_json::to_value(build_snapshot(&el_only, "http/1.1", None, None, 1300)).unwrap();
        assert!(el_only_json.get("cl").is_none());
        assert!(el_only_json.get("reference").is_none());
        assert!(el_only_json["el"].get("peers").is_none());
        assert!(el_only_json["el"].get("errors").is_none());
    }

    #[test]
    fn snapshot_records_errors_and_omits_values_on_failure() {
        let probe = mk_probe(
            LocalProbe {
                el_syncing: Err("eth_syncing: timeout".into()),
                el_peers: Err("net_peerCount: http 500".into()),
                el_block: Err("eth_getBlockByNumber: timeout".into()),
                cl: Some(ClProbe {
                    status: Err("node/syncing: http 404".into()),
                    peers: Err("node/peer_count: http 404".into()),
                }),
            },
            Some(ReferenceProbe {
                state: ReferenceState::Unavailable,
                errors: Box::new([Box::from("https://ref.example.com/<redacted>: timeout")]),
            }),
        );
        let snap = build_snapshot(
            &probe,
            "http/1.1",
            Some("http/1.1"),
            SlotClock::new(1000, 12),
            1300,
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
                "net_peerCount: http 500",
                "eth_getBlockByNumber: timeout"
            ])
        );
        assert_eq!(
            json["cl"]["errors"],
            serde_json::json!(["node/syncing: http 404", "node/peer_count: http 404"])
        );
        assert_eq!(
            json["reference"],
            serde_json::json!({
                "errors": ["https://ref.example.com/<redacted>: timeout"]
            })
        );

        // Capacity overflow saturates without panicking under `panic = "abort"`.
        let mut small = ErrorList::<2>::default();
        small.push("a: 1");
        small.push("b: 2");
        small.push("c: 3");
        assert_eq!(
            serde_json::to_value(&small).unwrap(),
            serde_json::json!(["a: 1", "b: 2"])
        );
    }

    #[test]
    fn snapshot_reports_el_sync_distance_while_syncing() {
        let probe = mk_probe(
            LocalProbe {
                el_syncing: Ok(ElSyncingResult::Syncing {
                    current_block: 0x10,
                    highest_block: 0x20,
                }),
                el_peers: Err("skip".into()),
                el_block: Err("skip".into()),
                cl: None,
            },
            None,
        );
        let snap = build_snapshot(&probe, "http/1.1", None, None, 1300);
        assert_eq!(snap.el.syncing, Some(true));
        assert_eq!(snap.el.sync_distance, Some(16));
    }

    #[test]
    fn snapshot_omits_cl_slot_age_when_genesis_disabled() {
        let probe = mk_probe(
            LocalProbe {
                el_syncing: Ok(ElSyncingResult::Synced(false)),
                el_peers: Ok(Some(0x10)),
                el_block: Ok(ElBlockResult {
                    number: 5,
                    timestamp: 0x3e2,
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
            1300,
        );
        assert_eq!(snap.cl.as_ref().unwrap().head_slot, Some(10));
        assert_eq!(snap.cl.as_ref().unwrap().slot_age_secs, None);
    }

    #[test]
    fn trusted_snapshot_reports_mode_and_omits_unpolled_or_disabled_fields() {
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
    }

    #[test]
    fn node_health_returns_206_only_when_both_layers_reachable_and_syncing() {
        use clap::Parser;

        let default_opts =
            crate::Config::parse_from(["ethonyx", "--el-http-url", "http://127.0.0.1:8545"])
                .parse_validated()
                .unwrap()
                .opts;
        let strict_opts = crate::config::RuntimeOpts {
            strict: Some(crate::config::StrictFreshness {
                el_max_block_age_secs: 60,
                cl_max_slot_age_secs: 60,
            }),
            ..default_opts
        };
        let strict_clock_opts = crate::config::RuntimeOpts {
            slot_clock: SlotClock::new(1000, 12),
            ..strict_opts
        };

        let synced_el = ElSyncingResult::Synced(false);
        let syncing_el = ElSyncingResult::Syncing {
            current_block: 10,
            highest_block: 20,
        };
        let synced_cl = ClStatus {
            head_slot: 100,
            sync_distance: 0,
            is_syncing: false,
            is_optimistic: false,
            el_offline: false,
        };
        let syncing_cl = ClStatus {
            head_slot: 50,
            sync_distance: 50,
            is_syncing: true,
            is_optimistic: true,
            el_offline: false,
        };
        let fresh_block = ElBlockResult {
            number: 10,
            timestamp: u64::MAX,
        };
        let stale_block = ElBlockResult {
            number: 10,
            timestamp: 1000,
        };

        // 1. Ready -> 200 OK (default and strict with fresh block)
        let ready_probe = mk_probe(
            LocalProbe {
                el_syncing: Ok(synced_el),
                el_peers: Ok(Some(10)),
                el_block: Ok(fresh_block),
                cl: Some(ClProbe {
                    status: Ok(synced_cl),
                    peers: Ok(10),
                }),
            },
            None,
        );
        assert_eq!(
            classify_node_health(evaluate_readiness(&default_opts, &ready_probe)),
            StatusCode::OK
        );
        assert_eq!(
            classify_node_health(evaluate_readiness(&strict_opts, &ready_probe)),
            StatusCode::OK
        );

        // 2. EL syncing + CL reachable -> 206 Partial Content (even when block is stale/missing
        // under --readyz-strict, CL reports `is_optimistic: true`, or node lags behind reference!)
        let el_sync_probe = mk_probe(
            LocalProbe {
                el_syncing: Ok(syncing_el),
                el_peers: Ok(Some(10)),
                el_block: Err("latest block not yet populated".into()),
                cl: Some(ClProbe {
                    status: Ok(ClStatus {
                        is_optimistic: true,
                        ..synced_cl
                    }),
                    peers: Ok(10),
                }),
            },
            Some(ReferenceProbe {
                state: ReferenceState::Active(ReferenceSample {
                    block_number: 500,
                    block_lag: 490,
                }),
                errors: Box::new([]),
            }),
        );
        assert_eq!(
            classify_node_health(evaluate_readiness(&default_opts, &el_sync_probe)),
            StatusCode::PARTIAL_CONTENT
        );
        assert_eq!(
            classify_node_health(evaluate_readiness(&strict_opts, &el_sync_probe)),
            StatusCode::PARTIAL_CONTENT
        );

        let el_sync_bool_probe = mk_probe(
            LocalProbe {
                el_syncing: Ok(ElSyncingResult::Synced(true)),
                el_peers: Ok(Some(10)),
                el_block: Ok(stale_block),
                cl: Some(ClProbe {
                    status: Ok(synced_cl),
                    peers: Ok(10),
                }),
            },
            None,
        );
        assert_eq!(
            classify_node_health(evaluate_readiness(&default_opts, &el_sync_bool_probe)),
            StatusCode::PARTIAL_CONTENT
        );

        // 3. EL syncing + CL down -> 503 Service Unavailable (must NOT mask CL crash as 206)
        let cl_down_probe = mk_probe(
            LocalProbe {
                el_syncing: Ok(syncing_el),
                el_peers: Ok(Some(10)),
                el_block: Ok(stale_block),
                cl: Some(ClProbe {
                    status: Err("connection refused".into()),
                    peers: Err("connection refused".into()),
                }),
            },
            None,
        );
        assert_eq!(
            classify_node_health(evaluate_readiness(&default_opts, &cl_down_probe)),
            StatusCode::SERVICE_UNAVAILABLE
        );

        // 4. CL syncing + EL down -> 503 Service Unavailable (must NOT mask EL crash as 206)
        let el_down_probe = mk_probe(
            LocalProbe {
                el_syncing: Err("timeout".into()),
                el_peers: Err("timeout".into()),
                el_block: Err("timeout".into()),
                cl: Some(ClProbe {
                    status: Ok(syncing_cl),
                    peers: Ok(10),
                }),
            },
            None,
        );
        assert_eq!(
            classify_node_health(evaluate_readiness(&default_opts, &el_down_probe)),
            StatusCode::SERVICE_UNAVAILABLE
        );

        // 5. CL syncing + EL reachable under --readyz-strict -> 206, unless EL claims synced with unreadable block -> 503
        let cl_sync_probe = mk_probe(
            LocalProbe {
                el_syncing: Ok(synced_el),
                el_peers: Ok(Some(10)),
                el_block: Ok(stale_block),
                cl: Some(ClProbe {
                    status: Ok(syncing_cl),
                    peers: Ok(10),
                }),
            },
            None,
        );
        assert_eq!(
            classify_node_health(evaluate_readiness(&strict_clock_opts, &cl_sync_probe)),
            StatusCode::PARTIAL_CONTENT
        );
        let strict_block_err_probe = mk_probe(
            LocalProbe {
                el_syncing: Ok(synced_el),
                el_peers: Ok(Some(10)),
                el_block: Err("rpc error".into()),
                cl: Some(ClProbe {
                    status: Ok(syncing_cl),
                    peers: Ok(10),
                }),
            },
            None,
        );
        assert_eq!(
            classify_node_health(evaluate_readiness(&strict_opts, &strict_block_err_probe)),
            StatusCode::SERVICE_UNAVAILABLE
        );

        // 6. Silent stall (`is_optimistic: true` while neither layer is syncing), `el_offline: true`,
        // or reference lag while synced -> 503 Service Unavailable.
        let silent_optimistic_probe = mk_probe(
            LocalProbe {
                el_syncing: Ok(synced_el),
                el_peers: Ok(Some(10)),
                el_block: Ok(fresh_block),
                cl: Some(ClProbe {
                    status: Ok(ClStatus {
                        is_optimistic: true,
                        ..synced_cl
                    }),
                    peers: Ok(10),
                }),
            },
            None,
        );
        assert_eq!(
            classify_node_health(evaluate_readiness(&default_opts, &silent_optimistic_probe)),
            StatusCode::SERVICE_UNAVAILABLE
        );

        let el_offline_probe = mk_probe(
            LocalProbe {
                el_syncing: Ok(syncing_el),
                el_peers: Ok(Some(10)),
                el_block: Ok(fresh_block),
                cl: Some(ClProbe {
                    status: Ok(ClStatus {
                        el_offline: true,
                        ..syncing_cl
                    }),
                    peers: Ok(10),
                }),
            },
            None,
        );
        assert_eq!(
            classify_node_health(evaluate_readiness(&default_opts, &el_offline_probe)),
            StatusCode::SERVICE_UNAVAILABLE
        );

        let lagging_synced_probe = mk_probe(
            LocalProbe {
                el_syncing: Ok(synced_el),
                el_peers: Ok(Some(10)),
                el_block: Ok(fresh_block),
                cl: Some(ClProbe {
                    status: Ok(synced_cl),
                    peers: Ok(10),
                }),
            },
            Some(ReferenceProbe {
                state: ReferenceState::Active(ReferenceSample {
                    block_number: 12,
                    block_lag: 2,
                }),
                errors: Box::new([]),
            }),
        );
        assert_eq!(
            classify_node_health(evaluate_readiness(&default_opts, &lagging_synced_probe)),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }
}
