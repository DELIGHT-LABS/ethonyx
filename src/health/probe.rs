//! Background upstream health probing, Prometheus/OTel gauge recording, and readiness transition logging.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tracing::{debug, info, warn};

use crate::config::HealthMode;
use crate::metrics::{HealthGauge, ServerMetrics};
use crate::state::AppState;

use super::cl::{ClStatus, cl_peer_count, cl_syncing_status};
use super::el::el_batch_detect;
use super::eval::{
    ClProbe, ClSyncEval, ElSyncEval, HealthSource, LocalProbe, Probe, ReadinessEval, Transition,
    evaluate_readiness, readiness_transition,
};
use super::fetch::ProbeTarget;
use super::reference::ReferencePoller;

pub(crate) struct HealthPoller {
    tx: watch::Sender<Probe>,
    interval: Duration,
    reference: Option<ReferencePoller>,
}

/// Merge one cadence's result into the shared [`Probe`], record Prometheus/OTel
/// gauges, log readiness transitions, and broadcast the snapshot last so probe
/// readers always observe metrics at least as fresh as the snapshot.
///
/// Both cadences run on one task and this function never awaits, so its
/// read-modify-write of the `watch` value cannot interleave with another publish.
fn publish(state: &AppState, tx: &watch::Sender<Probe>, merge: impl FnOnce(&mut Probe)) {
    let mut probe = tx.borrow().clone();
    let prev_ready = probe.published_ready;
    merge(&mut probe);

    let eval = evaluate_readiness(&state.opts, &probe);
    let ready = eval.is_ready();
    let status = if ready { "ready" } else { "not_ready" };
    debug!(status, "health poll");

    record_probe_metrics(&state.metrics, &probe, eval);

    match readiness_transition(prev_ready, ready) {
        Transition::Silent => {}
        Transition::Recovered => info!("readiness recovered"),
        Transition::BecameNotReady => {
            warn!(
                el_syncing = %eval.el_sync,
                cl_syncing = eval.cl_sync.map(display),
                el_reference_lag = eval.el_reference_lag.map(display),
                el_block = eval.el_block_fresh.map(display),
                cl_slot = eval.cl_slot_fresh.map(display),
                "not ready"
            );
        }
    }

    probe.published_ready = Some(ready);
    // `AppState` holds a receiver and the poller task holds `Arc<AppState>`.
    tx.send_replace(probe);
}

pub(crate) fn channel(mode: HealthMode, has_cl: bool) -> (HealthSource, Option<HealthPoller>) {
    match mode {
        HealthMode::Trusted => (HealthSource::Trusted, None),
        HealthMode::Polled {
            interval,
            reference,
        } => {
            let (tx, rx) = watch::channel(Probe::pending(has_cl, reference.is_some()));
            (
                HealthSource::Polled(rx),
                Some(HealthPoller {
                    tx,
                    interval,
                    reference: reference.map(ReferencePoller::new),
                }),
            )
        }
    }
}

impl HealthPoller {
    /// Warm the health cache with one poll before serving so `/healthz` and
    /// `/readyz` read a snapshot instead of hitting upstream per probe. Returns
    /// `true` when any configured upstream or reference signal failed during the initial poll.
    pub(crate) async fn warm(&mut self, state: &AppState) -> bool {
        let (local, reference) = match self.reference.as_mut() {
            Some(ref_poller) => {
                let (local, reference) = tokio::join!(
                    probe_local(state),
                    ref_poller.poll_once(&state.client, &state.el, state.opts.health_timeout),
                );
                (local, Some(reference))
            }
            None => (probe_local(state).await, None),
        };
        publish(state, &self.tx, |probe| {
            probe.local = Arc::new(local);
            probe.reference = reference;
        });
        self.tx.borrow().has_errors()
    }

    pub(crate) fn spawn(
        self,
        state: Arc<AppState>,
        shutdown: crate::lifecycle::Shutdown,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            tokio::select! {
                biased;
                _ = shutdown.deadline() => {}
                () = self.run(state) => {}
            }
        })
    }

    /// Refresh the shared [`Probe`] on two independent cadences. Each sleeps its own
    /// interval after a completed poll, so a slow upstream gets a full breather and a
    /// shorter reference interval never delays local polling.
    async fn run(self, state: Arc<AppState>) {
        let Self {
            tx,
            interval,
            reference,
        } = self;
        let local = async {
            loop {
                tokio::time::sleep(interval).await;
                let local = Arc::new(probe_local(&state).await);
                publish(&state, &tx, |probe| probe.local = local);
            }
        };
        match reference {
            None => local.await,
            Some(mut poller) => {
                let ref_interval = poller.interval();
                let references = async {
                    loop {
                        tokio::time::sleep(ref_interval).await;
                        let reference = poller
                            .poll_once(&state.client, &state.el, state.opts.health_timeout)
                            .await;
                        publish(&state, &tx, |probe| probe.reference = Some(reference));
                    }
                };
                tokio::join!(local, references);
            }
        }
    }
}

/// Query local EL (and CL when configured) signals once. Each call is bounded by
/// `--health-timeout`; failures land in the result rather than aborting.
async fn probe_local(state: &AppState) -> LocalProbe {
    let timeout = state.opts.health_timeout;
    let el = ProbeTarget::from_target(&state.client, &state.el, timeout);
    let (batch, cl) = match state.cl.as_ref() {
        Some(cl_state) => {
            let cl = ProbeTarget::from_target(&state.client, &cl_state.beacon, timeout);
            // Independent upstreams share one poll; a slow EL does not delay CL calls.
            // EL still uses one batch request and owns transport detection.
            let (batch, status, peers): (_, Result<ClStatus, _>, Result<u64, _>) = tokio::join!(
                el_batch_detect(el, state.el.upstream.uri()),
                cl_syncing_status(cl, &cl_state.syncing_uri),
                cl_peer_count(cl, &cl_state.peer_count_uri),
            );
            (batch, Some(ClProbe { status, peers }))
        }
        None => (el_batch_detect(el, state.el.upstream.uri()).await, None),
    };
    let (el_syncing, el_peers, el_block) = match batch {
        Ok(res) => (
            res.syncing.map_err(|e| e.during("eth_syncing")),
            res.peers.map_err(|e| e.during("net_peerCount")),
            res.block.map_err(|e| e.during("eth_getBlockByNumber")),
        ),
        Err(err) => (
            Err(err.during("eth_syncing")),
            Err(err.during("net_peerCount")),
            Err(err.during("eth_getBlockByNumber")),
        ),
    };
    LocalProbe {
        el_syncing,
        el_peers,
        el_block,
        cl,
    }
}

fn record_probe_metrics(metrics: &ServerMetrics, probe: &Probe, eval: ReadinessEval<'_>) {
    use HealthGauge as G;
    let flag = |ok: bool| if ok { 1.0 } else { 0.0 };
    let local = &probe.local;

    metrics.set_health(G::ElHealth, flag(eval.el_sync.is_ok()));
    // An unsupported `net_peerCount` (`-32601`) leaves the gauge unset; a failed call reads 0.
    match &local.el_peers {
        Ok(Some(peers)) => metrics.set_health(G::ElPeers, *peers as f64),
        Ok(None) => {}
        Err(_) => metrics.set_health(G::ElPeers, 0.0),
    }
    let el_distance = match eval.el_sync {
        ElSyncEval::SyncingProgress { distance, .. } => distance as f64,
        _ => 0.0,
    };
    metrics.set_health(G::ElSyncDistance, el_distance);
    if let Ok(block) = &local.el_block {
        metrics.set_health(G::ElBlockNumber, block.number as f64);
    }

    if let Some(cl) = &local.cl {
        metrics.set_health(
            G::ClHealth,
            flag(eval.cl_sync.is_some_and(ClSyncEval::is_ok)),
        );
        metrics.set_health(G::ClPeers, cl.peers.as_ref().map_or(0.0, |&p| p as f64));
        let status = cl.status.as_ref().ok();
        metrics.set_health(
            G::ClSyncDistance,
            status.map_or(0.0, |s| s.sync_distance as f64),
        );
        if let Some(status) = status {
            metrics.set_health(G::ClSlotNumber, status.head_slot as f64);
        }
    }

    if let Some(reference) = probe.reference.as_deref() {
        metrics.set_health(G::ReferenceHealth, flag(reference.state.is_healthy()));
        if let Some(sample) = reference.state.sample() {
            metrics.set_health(G::ReferenceBlockNumber, sample.block_number as f64);
            metrics.set_health(G::ReferenceBlockLag, sample.block_lag as f64);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::health::el::{ElBlockResult, ElSyncingResult};
    use clap::Parser;

    #[test]
    fn el_only_probe_metrics_omit_cl_reference_and_unsupported_peer_gauges() {
        let opts = crate::Config::parse_from(["ethonyx", "--el-http-url", "http://127.0.0.1:8545"])
            .parse_validated()
            .unwrap()
            .opts;
        let probe = Probe {
            local: Arc::new(LocalProbe {
                el_syncing: Ok(ElSyncingResult::Synced(false)),
                el_peers: Ok(None),
                el_block: Ok(ElBlockResult {
                    number: 50,
                    timestamp: 1_700_000_000,
                }),
                cl: None,
            }),
            reference: None,
            published_ready: None,
        };
        let metrics = ServerMetrics::new(None);
        record_probe_metrics(&metrics, &probe, evaluate_readiness(&opts, &probe));

        let rendered = metrics.render_prometheus();
        assert!(rendered.contains("ethonyx_upstream_block_number 50"));
        assert!(!rendered.contains("layer=\"cl\""));
        assert!(!rendered.contains("ethonyx_upstream_slot_number"));
        assert!(!rendered.contains("ethonyx_upstream_peers"));
        assert!(!rendered.contains("ethonyx_reference_"));
    }
}
