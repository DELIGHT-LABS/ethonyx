//! Background upstream health probing, Prometheus/OTel gauge recording, and readiness transition logging.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tracing::{debug, info, warn};

use crate::metrics::{ClSlotSample, HealthSample};
use crate::state::AppState;

use super::cl::{cl_peer_count, cl_syncing_status};
use super::el::el_batch_detect;
use super::eval::{
    ElSyncEval, Probe, ReadinessEval, Transition, evaluate_readiness, readiness_transition,
};
use super::fetch::{HealthError, ProbeTarget};

/// Query all five upstream signals once. Each call is bounded by
/// `--health-timeout`; failures land in the result rather than aborting.
pub(crate) async fn probe_once(state: &AppState) -> Probe {
    let timeout = state.cfg.health_timeout;
    let el = ProbeTarget {
        client: &state.client,
        transport: &state.el.transport,
        timeout,
    };
    let cl = ProbeTarget {
        client: &state.client,
        transport: &state.cl.transport,
        timeout,
    };
    // Independent upstreams share one poll; a slow EL does not delay CL calls.
    // EL still uses one batch request and owns transport detection.
    let (batch, cl_status, cl_peers) = tokio::join!(
        el_batch_detect(el, state.el.upstream.uri()),
        cl_syncing_status(cl, &state.cl_syncing_uri),
        cl_peer_count(cl, &state.cl_peer_count_uri),
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
    let el_distance = match eval.el_sync {
        ElSyncEval::SyncingProgress { distance, .. } => distance as f64,
        _ => 0.0,
    };
    let cl_status = probe.cl_status.as_ref().ok().map(|s| ClSlotSample {
        sync_distance: s.sync_distance,
        head_slot: s.head_slot,
    });
    let el_block = probe.el_block.as_ref().ok().map(|b| b.number);

    state.metrics.record_health(HealthSample {
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
    let shutdown = crate::lifecycle::deadline(&shutdown);
    tokio::pin!(shutdown);
    let mut prev_ready: Option<bool> = None;
    loop {
        tokio::select! {
            biased;
            _ = &mut shutdown => return,
            _ = tokio::time::sleep(interval) => {}
        }
        let probe = tokio::select! {
            biased;
            _ = &mut shutdown => return,
            probe = probe_once(&state) => probe,
        };

        let eval = evaluate_readiness(&state.cfg, state.slot_clock, &probe);
        let ready = eval.is_ready();
        let status = if ready { "ready" } else { "not_ready" };
        debug!(status, "health poll");

        update_metrics(&state, &probe, eval);

        match readiness_transition(prev_ready, ready) {
            Transition::Silent => {}
            Transition::Recovered => info!("readiness recovered"),
            Transition::BecameNotReady => {
                let (el_block, cl_slot) = eval.strict.unzip();
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
