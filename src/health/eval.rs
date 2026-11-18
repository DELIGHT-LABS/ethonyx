//! Pure synchronous upstream probe state and readiness/freshness evaluation.

use std::fmt;
use std::sync::Arc;

use crate::config::{HealthPolicy, SlotClock};

use super::cl::ClStatus;
use super::el::{ElBatchResult, ElSyncingResult};
use super::fetch::{HealthError, MethodError, ProbeMethod};
use super::reference::{ReferenceProbe, ReferenceSample, ReferenceState};

pub(super) struct ClProbe {
    pub(super) status: Result<ClStatus, HealthError>,
    pub(super) peers: Result<u64, HealthError>,
}

pub(super) struct LocalProbe {
    pub(super) el: Result<ElBatchResult, HealthError>,
    pub(super) cl: Option<ClProbe>,
}

/// Upstream probe results captured by background polling and shared with request
/// handlers via a `watch` channel. `local` and `reference` live in separate `Arc`s
/// so independent poll cadences swap their half with zero field-by-field cloning.
#[derive(Clone)]
pub(crate) struct Probe {
    pub(super) local: Arc<LocalProbe>,
    pub(super) reference: Option<Arc<ReferenceProbe>>,
}

impl Probe {
    pub(super) fn pending(has_cl: bool, has_reference: bool) -> Self {
        let cl = has_cl.then_some(ClProbe {
            status: Err(HealthError::Pending),
            peers: Err(HealthError::Pending),
        });
        let local = Arc::new(LocalProbe {
            el: Err(HealthError::Pending),
            cl,
        });
        let reference = has_reference.then(|| Arc::new(ReferenceProbe::pending()));
        Self { local, reference }
    }

    pub(super) fn has_errors(&self) -> bool {
        !self
            .local
            .el
            .as_ref()
            .is_ok_and(|el| el.syncing.is_ok() && el.peers.is_ok() && el.block.is_ok())
            || self
                .local
                .cl
                .as_ref()
                .is_some_and(|cl| cl.status.is_err() || cl.peers.is_err())
            || self
                .reference
                .as_deref()
                .is_some_and(|reference| !reference.errors.is_empty())
    }
}

#[derive(Clone, Copy)]
pub(super) enum ElSyncEval<'a> {
    Synced { block: u64 },
    SyncingBool,
    SyncingProgress { current_block: u64, distance: u64 },
    Err(MethodError<'a>),
}

impl ElSyncEval<'_> {
    pub(super) const fn is_ok(self) -> bool {
        matches!(self, Self::Synced { .. })
    }

    pub(super) const fn is_syncing(self) -> bool {
        matches!(self, Self::SyncingBool | Self::SyncingProgress { .. })
    }

    pub(super) const fn is_reachable(self) -> bool {
        !matches!(self, Self::Err(_))
    }
}

impl fmt::Display for ElSyncEval<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Synced { block } => write!(f, "synced (block {block})"),
            Self::SyncingBool => f.write_str("syncing (true)"),
            Self::SyncingProgress {
                current_block,
                distance,
            } => write!(f, "syncing (block {current_block}, distance {distance})"),
            Self::Err(err) => err.fmt(f),
        }
    }
}

#[derive(Clone, Copy)]
pub(super) enum ClSyncEval<'a> {
    Synced { head_slot: u64, sync_distance: u64 },
    Syncing { head_slot: u64, sync_distance: u64 },
    Optimistic { head_slot: u64, sync_distance: u64 },
    ElOffline { head_slot: u64 },
    Err(MethodError<'a>),
}

impl ClSyncEval<'_> {
    pub(super) const fn is_ok(self) -> bool {
        matches!(self, Self::Synced { .. })
    }

    pub(super) const fn is_syncing(self) -> bool {
        matches!(self, Self::Syncing { .. })
    }

    pub(super) const fn is_reachable(self) -> bool {
        !matches!(self, Self::Err(_) | Self::ElOffline { .. })
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
            Self::Optimistic {
                head_slot,
                sync_distance,
            } => write!(f, "optimistic (slot {head_slot}, distance {sync_distance})"),
            Self::ElOffline { head_slot } => write!(f, "el_offline (slot {head_slot})"),
            Self::Err(err) => err.fmt(f),
        }
    }
}

#[derive(Clone, Copy)]
pub(super) enum ReferenceLagEval {
    WithinLag {
        sample: ReferenceSample,
        max_lag: u64,
    },
    Lagging {
        sample: ReferenceSample,
        max_lag: u64,
    },
    FailOpenUnavailable,
}

impl ReferenceLagEval {
    pub(super) const fn is_ok(self) -> bool {
        matches!(self, Self::WithinLag { .. } | Self::FailOpenUnavailable)
    }
}

impl fmt::Display for ReferenceLagEval {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WithinLag { sample, max_lag } => write!(
                f,
                "lag {} (reference block {}, max {max_lag})",
                sample.block_lag, sample.block_number
            ),
            Self::Lagging { sample, max_lag } => write!(
                f,
                "lag {} exceeds max {max_lag} (reference block {})",
                sample.block_lag, sample.block_number
            ),
            Self::FailOpenUnavailable => f.write_str("reference unavailable (fail-open)"),
        }
    }
}

#[derive(Clone, Copy)]
pub(super) enum FreshKind {
    Block,
    Slot,
}

impl FreshKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Block => "block",
            Self::Slot => "slot",
        }
    }
}

#[derive(Clone, Copy)]
pub(super) enum FreshEval<'a> {
    Fresh {
        kind: FreshKind,
        num: u64,
        age: u64,
    },
    Stale {
        kind: FreshKind,
        num: u64,
        age: u64,
        max_age: u64,
    },
    Disabled {
        head_slot: u64,
    },
    Err(MethodError<'a>),
}

impl FreshEval<'_> {
    pub(super) const fn is_ok(self) -> bool {
        matches!(self, Self::Fresh { .. } | Self::Disabled { .. })
    }
}

impl fmt::Display for FreshEval<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Fresh { kind, num, age } => write!(f, "{} {num}, age {age}s", kind.as_str()),
            Self::Stale {
                kind,
                num,
                age,
                max_age,
            } => write!(f, "{} {num} stale: {age}s (max {max_age})", kind.as_str()),
            Self::Disabled { head_slot } => write!(f, "slot {head_slot} (age check disabled)"),
            Self::Err(err) => err.fmt(f),
        }
    }
}

/// Zero-allocation readiness evaluation shared across `HEAD /readyz`, `GET /readyz`,
/// `node_health`, health probe metrics, and readiness transition logs.
#[derive(Clone, Copy)]
pub(super) struct ReadinessEval<'a> {
    pub(super) el_sync: ElSyncEval<'a>,
    pub(super) cl_sync: Option<ClSyncEval<'a>>,
    pub(super) el_reference_lag: Option<ReferenceLagEval>,
    pub(super) el_block_fresh: Option<FreshEval<'a>>,
    pub(super) cl_slot_fresh: Option<FreshEval<'a>>,
}

impl ReadinessEval<'_> {
    pub(super) fn is_ready(&self) -> bool {
        self.el_sync.is_ok()
            && self.cl_sync.is_none_or(ClSyncEval::is_ok)
            && self.el_reference_lag.is_none_or(ReferenceLagEval::is_ok)
            && self.el_block_fresh.is_none_or(FreshEval::is_ok)
            && self.cl_slot_fresh.is_none_or(FreshEval::is_ok)
    }
}

pub(super) fn evaluate_readiness<'a>(policy: HealthPolicy, probe: &'a Probe) -> ReadinessEval<'a> {
    let local = &probe.local;
    let el_sync = eval_el_syncing(&local.el);
    let cl_sync = local.cl.as_ref().map(|cl| eval_cl_syncing(&cl.status));
    let el_reference_lag = probe
        .reference
        .as_deref()
        .map(|ref_probe| eval_reference_lag(ref_probe.state, policy.el_max_reference_lag));

    // `now` is only needed for strict freshness checks; the default path skips reading the clock.
    let (el_block_fresh, cl_slot_fresh) = match policy.strict {
        Some(strict) => {
            let now = now_unix();
            let el_f = Some(eval_el_block_fresh(
                &local.el,
                strict.el_max_block_age_secs,
                now,
            ));
            let cl_f = local.cl.as_ref().map(|cl| {
                eval_cl_slot_fresh(
                    &cl.status,
                    policy.slot_clock,
                    strict.cl_max_slot_age_secs,
                    now,
                )
            });
            (el_f, cl_f)
        }
        None => (None, None),
    };

    ReadinessEval {
        el_sync,
        cl_sync,
        el_reference_lag,
        el_block_fresh,
        cl_slot_fresh,
    }
}

pub(super) fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

fn eval_el_syncing(el: &Result<ElBatchResult, HealthError>) -> ElSyncEval<'_> {
    let batch = match el {
        Ok(batch) => batch,
        Err(err) => return ElSyncEval::Err(MethodError::new(ProbeMethod::EthSyncing, err)),
    };
    match &batch.syncing {
        Err(err) => ElSyncEval::Err(MethodError::new(ProbeMethod::EthSyncing, err)),
        Ok(ElSyncingResult::SyncingBool) => ElSyncEval::SyncingBool,
        Ok(ElSyncingResult::SyncingProgress {
            current_block,
            highest_block,
        }) => ElSyncEval::SyncingProgress {
            current_block: *current_block,
            distance: highest_block.saturating_sub(*current_block),
        },
        Ok(ElSyncingResult::Synced) => match &batch.block {
            Ok(b) => ElSyncEval::Synced { block: b.number },
            Err(err) => ElSyncEval::Err(MethodError::new(ProbeMethod::EthGetBlockByNumber, err)),
        },
    }
}

fn eval_cl_syncing(r: &Result<ClStatus, HealthError>) -> ClSyncEval<'_> {
    match r {
        Err(e) => ClSyncEval::Err(MethodError::new(ProbeMethod::ClSyncing, e)),
        Ok(s) if s.el_offline => ClSyncEval::ElOffline {
            head_slot: s.head_slot,
        },
        Ok(s) if s.is_syncing => ClSyncEval::Syncing {
            head_slot: s.head_slot,
            sync_distance: s.sync_distance,
        },
        Ok(s) if s.is_optimistic => ClSyncEval::Optimistic {
            head_slot: s.head_slot,
            sync_distance: s.sync_distance,
        },
        Ok(s) => ClSyncEval::Synced {
            head_slot: s.head_slot,
            sync_distance: s.sync_distance,
        },
    }
}

const fn eval_reference_lag(state: ReferenceState, max_lag: u64) -> ReferenceLagEval {
    match state.sample() {
        Some(sample) if sample.block_lag <= max_lag => {
            ReferenceLagEval::WithinLag { sample, max_lag }
        }
        Some(sample) => ReferenceLagEval::Lagging { sample, max_lag },
        None => ReferenceLagEval::FailOpenUnavailable,
    }
}

const fn fresh_or_stale(kind: FreshKind, num: u64, age: u64, max_age: u64) -> FreshEval<'static> {
    if age <= max_age {
        FreshEval::Fresh { kind, num, age }
    } else {
        FreshEval::Stale {
            kind,
            num,
            age,
            max_age,
        }
    }
}

fn eval_el_block_fresh(
    el: &Result<ElBatchResult, HealthError>,
    max_age: u64,
    now: u64,
) -> FreshEval<'_> {
    match el.as_ref().and_then(|batch| batch.block.as_ref()) {
        Ok(block) => fresh_or_stale(
            FreshKind::Block,
            block.number,
            now.saturating_sub(block.timestamp),
            max_age,
        ),
        Err(err) => FreshEval::Err(MethodError::new(ProbeMethod::EthGetBlockByNumber, err)),
    }
}

fn eval_cl_slot_fresh(
    r: &Result<ClStatus, HealthError>,
    slot_clock: Option<SlotClock>,
    max_age: u64,
    now: u64,
) -> FreshEval<'_> {
    match (r, slot_clock) {
        (Ok(s), None) => FreshEval::Disabled {
            head_slot: s.head_slot,
        },
        (Ok(s), Some(clock)) => fresh_or_stale(
            FreshKind::Slot,
            s.head_slot,
            clock.slot_age(s.head_slot, now),
            max_age,
        ),
        (Err(err), _) => FreshEval::Err(MethodError::new(ProbeMethod::ClSyncing, err)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::health::el::ElBlockResult;
    use std::borrow::Cow;

    fn assert_eval<E: fmt::Display>(ok: bool, eval: E, expected_ok: bool, expected_detail: &str) {
        assert_eq!(ok, expected_ok);
        assert_eq!(eval.to_string(), expected_detail);
    }

    fn batch(
        syncing: Result<ElSyncingResult, HealthError>,
        block: Result<ElBlockResult, HealthError>,
    ) -> Result<ElBatchResult, HealthError> {
        Ok(ElBatchResult {
            syncing,
            peers: Ok(Some(1)),
            block,
        })
    }

    #[test]
    fn el_syncing_false_requires_readable_latest_block() {
        let ok = batch(
            Ok(ElSyncingResult::Synced),
            Ok(ElBlockResult {
                number: 42,
                timestamp: 1000,
            }),
        );
        let e = eval_el_syncing(&ok);
        assert_eval(e.is_ok(), e, true, "synced (block 42)");

        let broken_block = batch(
            Ok(ElSyncingResult::Synced),
            Err(HealthError::Rpc(Box::from("db error"))),
        );
        let e_err = eval_el_syncing(&broken_block);
        assert_eval(
            e_err.is_ok(),
            e_err,
            false,
            "eth_getBlockByNumber: rpc error: db error",
        );
    }

    #[test]
    fn el_syncing_object_reports_progress_even_when_latest_block_is_null() {
        let null_block = batch(
            Ok(ElSyncingResult::SyncingProgress {
                current_block: 0x10,
                highest_block: 0x20,
            }),
            Err(HealthError::Decode(Cow::Borrowed("null block"))),
        );
        let e = eval_el_syncing(&null_block);
        assert_eval(e.is_ok(), e, false, "syncing (block 16, distance 16)");
        assert!(e.is_syncing());
        assert!(e.is_reachable());
    }

    #[test]
    fn el_syncing_error_is_prefixed() {
        let r = batch(
            Err(HealthError::Rpc("boom".into())),
            Ok(ElBlockResult {
                number: 1,
                timestamp: 1,
            }),
        );
        let e = eval_el_syncing(&r);
        assert_eval(e.is_ok(), e, false, "eth_syncing: rpc error: boom");
        assert!(!e.is_reachable());

        // `Pending` omits the method prefix to match `"initial poll pending"`.
        let pending = eval_el_syncing(&Err(HealthError::Pending));
        assert_eval(pending.is_ok(), pending, false, "initial poll pending");
    }

    #[test]
    fn cl_syncing_distinguishes_synced_syncing_optimistic_and_el_offline() {
        let r_ok = Ok(ClStatus {
            head_slot: 42,
            sync_distance: 0,
            is_syncing: false,
            is_optimistic: false,
            el_offline: false,
        });
        let e_ok = eval_cl_syncing(&r_ok);
        assert_eval(e_ok.is_ok(), e_ok, true, "synced (slot 42, distance 0)");

        // Active syncing with `is_optimistic: true` stays `Syncing` (206-eligible).
        let r_sync = Ok(ClStatus {
            head_slot: 42,
            sync_distance: 5,
            is_syncing: true,
            is_optimistic: true,
            el_offline: false,
        });
        let e_sync = eval_cl_syncing(&r_sync);
        assert_eval(
            e_sync.is_ok(),
            e_sync,
            false,
            "syncing (slot 42, distance 5)",
        );
        assert!(e_sync.is_syncing());
        assert!(e_sync.is_reachable());

        // Non-syncing optimistic (silent EL stall / ENOSPC) fails readiness and is not syncing.
        let r_opt = Ok(ClStatus {
            head_slot: 42,
            sync_distance: 0,
            is_syncing: false,
            is_optimistic: true,
            el_offline: false,
        });
        let e_opt = eval_cl_syncing(&r_opt);
        assert_eval(
            e_opt.is_ok(),
            e_opt,
            false,
            "optimistic (slot 42, distance 0)",
        );
        assert!(!e_opt.is_syncing());
        assert!(e_opt.is_reachable());

        // `el_offline: true` takes precedence even over `is_syncing: true` (unreachable Engine API).
        let r_off = Ok(ClStatus {
            head_slot: 42,
            sync_distance: 5,
            is_syncing: true,
            is_optimistic: true,
            el_offline: true,
        });
        let e_off = eval_cl_syncing(&r_off);
        assert_eval(e_off.is_ok(), e_off, false, "el_offline (slot 42)");
        assert!(!e_off.is_reachable());
    }

    #[test]
    fn reference_lag_eval_covers_within_lagging_retained_and_unavailable() {
        let within = eval_reference_lag(
            ReferenceState::Active(ReferenceSample {
                block_number: 101,
                block_lag: 1,
            }),
            1,
        );
        assert_eval(
            within.is_ok(),
            within,
            true,
            "lag 1 (reference block 101, max 1)",
        );

        let lagging = eval_reference_lag(
            ReferenceState::Retained(ReferenceSample {
                block_number: 102,
                block_lag: 2,
            }),
            1,
        );
        assert_eval(
            lagging.is_ok(),
            lagging,
            false,
            "lag 2 exceeds max 1 (reference block 102)",
        );

        let unavail = eval_reference_lag(ReferenceState::Unavailable, 1);
        assert_eval(
            unavail.is_ok(),
            unavail,
            true,
            "reference unavailable (fail-open)",
        );
    }

    #[test]
    fn el_block_fresh_within_threshold_is_ok() {
        let r = batch(
            Ok(ElSyncingResult::Synced),
            Ok(ElBlockResult {
                number: 5,
                timestamp: 0x3e2,
            }),
        );
        let e = eval_el_block_fresh(&r, 60, 1000);
        assert_eval(e.is_ok(), e, true, "block 5, age 6s");
    }

    #[test]
    fn el_block_fresh_past_threshold_is_stale() {
        let r = batch(
            Ok(ElSyncingResult::Synced),
            Ok(ElBlockResult {
                number: 5,
                timestamp: 0x384,
            }),
        );
        let e = eval_el_block_fresh(&r, 60, 1000);
        assert_eval(e.is_ok(), e, false, "block 5 stale: 100s (max 60)");
    }

    #[test]
    fn cl_slot_fresh_disabled_when_genesis_zero() {
        let r = Ok(ClStatus {
            head_slot: 7,
            sync_distance: 0,
            is_syncing: false,
            is_optimistic: false,
            el_offline: false,
        });
        let e = eval_cl_slot_fresh(&r, SlotClock::new(0, 12), 60, 9_999_999);
        assert_eval(e.is_ok(), e, true, "slot 7 (age check disabled)");
    }

    #[test]
    fn cl_slot_fresh_past_threshold_is_stale() {
        let r = Ok(ClStatus {
            head_slot: 10,
            sync_distance: 0,
            is_syncing: false,
            is_optimistic: false,
            el_offline: false,
        });
        let e = eval_cl_slot_fresh(&r, SlotClock::new(1000, 12), 60, 1300);
        assert_eval(e.is_ok(), e, false, "slot 10 stale: 180s (max 60)");
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
}
