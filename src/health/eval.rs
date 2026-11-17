//! Pure upstream probe state, readiness/freshness evaluation, and transition rules.

use std::fmt;
use std::sync::Arc;

use tokio::sync::watch;

use crate::config::{RuntimeOpts, SlotClock};

use super::cl::ClStatus;
use super::el::{ElBlockResult, ElSyncingResult};
use super::fetch::ProbeError;

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
    pub(super) fn pending() -> Self {
        let pending: ProbeError = Arc::from("initial poll pending");
        Self {
            el_syncing: Err(pending.clone()),
            el_peers: Err(pending.clone()),
            el_block: Err(pending.clone()),
            cl_status: Err(pending.clone()),
            cl_peers: Err(pending),
        }
    }

    pub(super) const fn has_errors(&self) -> bool {
        self.el_syncing.is_err()
            || self.el_peers.is_err()
            || self.el_block.is_err()
            || self.cl_status.is_err()
            || self.cl_peers.is_err()
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
    pub(super) const fn is_ok(self) -> bool {
        matches!(self, Self::Synced)
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
    pub(super) const fn is_ok(self) -> bool {
        matches!(self, Self::Synced { .. })
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
    pub(super) const fn is_ok(self) -> bool {
        matches!(self, Self::Fresh { .. } | Self::Disabled { .. })
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
/// `node_health`, health probe metrics, and readiness transition logs.
#[derive(Clone, Copy)]
pub(super) struct ReadinessEval<'a> {
    pub(super) el_sync: ElSyncEval<'a>,
    pub(super) cl_sync: ClSyncEval<'a>,
    pub(super) strict: Option<(FreshEval<'a>, FreshEval<'a>)>,
}

impl ReadinessEval<'_> {
    pub(super) const fn el_sync_ok(&self) -> bool {
        self.el_sync.is_ok()
    }

    pub(super) const fn cl_sync_ok(&self) -> bool {
        self.cl_sync.is_ok()
    }

    pub(super) const fn is_ready(&self) -> bool {
        self.el_sync_ok()
            && self.cl_sync_ok()
            && match self.strict {
                None => true,
                Some((el_f, cl_f)) => el_f.is_ok() && cl_f.is_ok(),
            }
    }
}

pub(super) fn evaluate_readiness<'a>(
    cfg: &RuntimeOpts,
    slot_clock: Option<SlotClock>,
    probe: &'a Probe,
) -> ReadinessEval<'a> {
    let el_sync = eval_el_syncing(&probe.el_syncing);
    let cl_sync = eval_cl_syncing(&probe.cl_status);
    // `now` is only needed for strict freshness checks; the default sync-only path skips reading the clock.
    let strict = cfg.readyz_strict.then(|| {
        let now = now_unix();
        (
            eval_el_block_fresh(&probe.el_block, cfg.el_max_block_age_secs, now),
            eval_cl_slot_fresh(&probe.cl_status, slot_clock, cfg.cl_max_slot_age_secs, now),
        )
    });
    ReadinessEval {
        el_sync,
        cl_sync,
        strict,
    }
}

/// What `poll_loop` should log given the previous and current readiness, so a
/// sustained state logs once (on transition) rather than once per poll.
#[derive(Clone, Copy)]
pub(super) enum Transition {
    Silent,
    BecameNotReady,
    Recovered,
}

pub(super) const fn readiness_transition(prev_ready: Option<bool>, ready: bool) -> Transition {
    match prev_ready {
        Some(p) if p == ready => Transition::Silent,
        None if ready => Transition::Silent, // first poll already healthy: stay quiet
        _ if ready => Transition::Recovered,
        _ => Transition::BecameNotReady, // first-poll-degraded or ready -> not-ready
    }
}

pub(super) fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
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

const fn fresh_or_stale(
    kind: &'static str,
    num: u64,
    age: u64,
    max_age: u64,
) -> FreshEval<'static> {
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
    r: &Result<ElBlockResult, ProbeError>,
    max_age: u64,
    now: u64,
) -> FreshEval<'_> {
    match r {
        Ok(block) => fresh_or_stale(
            "block",
            block.number,
            now.saturating_sub(block.timestamp),
            max_age,
        ),
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
        (Ok(s), Some(clock)) => fresh_or_stale(
            "slot",
            s.head_slot,
            clock.slot_age(s.head_slot, now),
            max_age,
        ),
        (Err(e), _) => FreshEval::Err {
            prefix: "node/syncing",
            err: e,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_eval<E: fmt::Display>(ok: bool, eval: E, expected_ok: bool, expected_detail: &str) {
        assert_eq!(ok, expected_ok);
        assert_eq!(eval.to_string(), expected_detail);
    }

    #[test]
    fn el_syncing_false_is_synced() {
        let e = eval_el_syncing(&Ok(ElSyncingResult::Synced(false)));
        assert_eval(e.is_ok(), e, true, "synced");
    }

    #[test]
    fn el_syncing_object_reports_block_progress() {
        let e = eval_el_syncing(&Ok(ElSyncingResult::Syncing {
            current_block: 0x10,
            highest_block: 0x20,
        }));
        assert_eval(e.is_ok(), e, false, "syncing (block 16, distance 16)");
    }

    #[test]
    fn el_syncing_error_is_prefixed() {
        let r = Err("boom".into());
        let e = eval_el_syncing(&r);
        assert_eval(e.is_ok(), e, false, "eth_syncing: boom");
    }

    #[test]
    fn cl_syncing_synced_is_ok() {
        let r = Ok(ClStatus {
            head_slot: 42,
            sync_distance: 0,
            is_syncing: false,
        });
        let e = eval_cl_syncing(&r);
        assert_eval(e.is_ok(), e, true, "synced (slot 42, distance 0)");
    }

    #[test]
    fn cl_syncing_in_progress_fails() {
        let r = Ok(ClStatus {
            head_slot: 42,
            sync_distance: 5,
            is_syncing: true,
        });
        let e = eval_cl_syncing(&r);
        assert_eval(e.is_ok(), e, false, "syncing (slot 42, distance 5)");
    }

    #[test]
    fn el_block_fresh_within_threshold_is_ok() {
        let r = Ok(ElBlockResult {
            number: 5,
            timestamp: 0x3e2,
        });
        let e = eval_el_block_fresh(&r, 60, 1000);
        assert_eval(e.is_ok(), e, true, "block 5, age 6s");
    }

    #[test]
    fn el_block_fresh_past_threshold_is_stale() {
        let r = Ok(ElBlockResult {
            number: 5,
            timestamp: 0x384,
        });
        let e = eval_el_block_fresh(&r, 60, 1000);
        assert_eval(e.is_ok(), e, false, "block 5 stale: 100s (max 60)");
    }

    #[test]
    fn cl_slot_fresh_disabled_when_genesis_zero() {
        let r = Ok(ClStatus {
            head_slot: 7,
            sync_distance: 0,
            is_syncing: false,
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
}
