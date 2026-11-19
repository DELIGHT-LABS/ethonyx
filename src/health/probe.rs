//! Background upstream health probing, Prometheus/OTel gauge recording, and readiness transition logging.

use std::sync::Arc;
use std::time::Duration;

use futures_util::future::{Either, OptionFuture};
use futures_util::stream::{self, StreamExt};
use tokio::sync::watch;
use tracing::{debug, info, warn};

use crate::config::{ClProbeUris, HealthMode, HealthPolicy, PolledHealth};
use crate::metrics::{HealthGauge, ServerMetrics};
use crate::state::AppState;

use super::cl::{cl_peer_count, cl_syncing_status};
use super::el::el_batch_detect;
use super::eval::{
    ClProbe, ClSyncEval, ElSyncEval, LocalProbe, Probe, ReadinessEval, evaluate_readiness,
};
use super::fetch::ProbeTarget;
use super::reference::ReferencePoller;

/// Source of readiness and health data for `/healthz`, `/readyz`, and `node_health`.
pub(crate) enum HealthSource {
    /// `--trust-upstream`: no background poller, `watch` channel, or policy is allocated.
    Trusted,
    /// Active background polling feeding snapshots via `watch`.
    Polled {
        rx: watch::Receiver<Probe>,
        policy: HealthPolicy,
    },
}

pub(crate) struct HealthPoller {
    tx: watch::Sender<Probe>,
    /// Previous readiness; starts `true` so a healthy first poll stays quiet and an
    /// unhealthy first poll logs `"not ready"` once (mathematically equivalent to `None`).
    was_ready: bool,
    interval: Duration,
    timeout: Duration,
    policy: HealthPolicy,
    cl_uris: Option<ClProbeUris>,
    reference: Option<ReferencePoller>,
}

pub(crate) fn channel(mode: HealthMode) -> (HealthSource, Option<HealthPoller>) {
    match mode {
        HealthMode::Trusted => (HealthSource::Trusted, None),
        HealthMode::Polled(polled) => {
            let PolledHealth {
                interval,
                timeout,
                policy,
                cl_uris,
                reference,
            } = *polled;
            let (tx, rx) = watch::channel(Probe::pending(cl_uris.is_some(), reference.is_some()));
            (
                HealthSource::Polled { rx, policy },
                Some(HealthPoller {
                    tx,
                    was_ready: true,
                    interval,
                    timeout,
                    policy,
                    cl_uris,
                    reference: reference.map(ReferencePoller::new),
                }),
            )
        }
    }
}

impl HealthPoller {
    /// Record Prometheus/OTel gauges, log a readiness transition once, and broadcast
    /// `probe` last so readers always observe metrics at least as fresh as the snapshot.
    ///
    /// `&mut self` guarantees this poller is the sole writer, so read-modify-write of the
    /// `watch` value cannot interleave with another publish.
    fn publish(&mut self, metrics: &ServerMetrics, probe: Probe) {
        let eval = evaluate_readiness(self.policy, &probe);
        let ready = eval.is_ready();
        let status = if ready { "ready" } else { "not_ready" };
        debug!(status, "health poll");

        record_probe_metrics(metrics, &probe, eval);

        match (std::mem::replace(&mut self.was_ready, ready), ready) {
            (false, true) => info!("readiness recovered"),
            (true, false) => {
                warn!(
                    el_syncing = %eval.el_sync,
                    cl_syncing = eval.cl_sync.map(display),
                    el_reference_lag = eval.el_reference_lag.map(display),
                    el_block = eval.el_block_fresh.map(display),
                    cl_slot = eval.cl_slot_fresh.map(display),
                    "not ready"
                );
            }
            _ => {}
        }

        // `AppState` holds a receiver and the poller task holds `Arc<AppState>`.
        self.tx.send_replace(probe);
    }

    /// Warm the health cache with one poll before serving so `/healthz` and
    /// `/readyz` read a snapshot instead of hitting upstream per probe. Returns
    /// `true` when any configured upstream or reference signal failed during the initial poll.
    pub(crate) async fn warm(&mut self, state: &AppState) -> bool {
        let timeout = self.timeout;
        let cl_uris = self.cl_uris.as_ref();
        let reference = self
            .reference
            .as_mut()
            .map(|poller| poller.poll_once(el_target(state, timeout)));
        let (local, reference) = tokio::join!(
            probe_local(state, cl_uris, timeout),
            OptionFuture::from(reference)
        );
        let probe = Probe { local, reference };
        let degraded = probe.has_errors();
        self.publish(&state.metrics, probe);
        degraded
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
    /// shorter reference interval never delays local polling. Both cadences run
    /// concurrently as streams and publish through `&mut self` one at a time.
    async fn run(mut self, state: Arc<AppState>) {
        let state = &*state;
        let interval = self.interval;
        let timeout = self.timeout;
        let cl_uris = self.cl_uris.take();
        let local = stream::unfold(cl_uris, move |cl_uris| async move {
            tokio::time::sleep(interval).await;
            let probe = probe_local(state, cl_uris.as_ref(), timeout).await;
            Some((Either::Left(probe), cl_uris))
        });
        let references = match self.reference.take() {
            None => Either::Left(stream::empty()),
            Some(poller) => Either::Right(stream::unfold(poller, move |mut poller| async move {
                tokio::time::sleep(poller.interval()).await;
                let probe = poller.poll_once(el_target(state, timeout)).await;
                Some((Either::Right(probe), poller))
            })),
        };
        let mut updates = std::pin::pin!(stream::select(local, references));
        while let Some(update) = updates.next().await {
            let probe = match update {
                Either::Left(local) => Probe {
                    local,
                    reference: self.tx.borrow().reference.clone(),
                },
                Either::Right(reference) => Probe {
                    local: Arc::clone(&self.tx.borrow().local),
                    reference: Some(reference),
                },
            };
            self.publish(&state.metrics, probe);
        }
    }
}

/// The local EL's JSON-RPC endpoint, bounded by `--health-timeout`.
const fn el_target(state: &AppState, timeout: Duration) -> ProbeTarget<'_> {
    ProbeTarget::from_target(&state.client, &state.el, timeout)
}

/// Query local EL (and CL when configured) signals once. Each call is bounded by
/// `--health-timeout`; failures land in the result rather than aborting.
async fn probe_local(
    state: &AppState,
    cl_uris: Option<&ClProbeUris>,
    timeout: Duration,
) -> Arc<LocalProbe> {
    debug_assert_eq!(state.cl.is_some(), cl_uris.is_some());
    let el_target = el_target(state, timeout);
    let (el, cl) = match state.cl.as_ref().zip(cl_uris) {
        Some((cl_state, uris)) => {
            let cl = |uri| ProbeTarget::new(&state.client, &cl_state.beacon, uri, timeout);
            // Independent upstreams share one poll; a slow EL does not delay CL calls.
            // EL still uses one batch request and owns transport detection.
            let (el, status, peers) = tokio::join!(
                el_batch_detect(el_target),
                cl_syncing_status(cl(&uris.syncing)),
                cl_peer_count(cl(&uris.peer_count)),
            );
            (el, Some(ClProbe { status, peers }))
        }
        None => (el_batch_detect(el_target).await, None),
    };
    Arc::new(LocalProbe { el, cl })
}

fn record_probe_metrics(metrics: &ServerMetrics, probe: &Probe, eval: ReadinessEval<'_>) {
    use HealthGauge as G;
    let local = &probe.local;

    metrics.set_health(G::ElHealth, f64::from(eval.el_sync.is_ok()));
    // An unsupported `net_peerCount` (`-32601`) leaves the gauge unset; a failed call
    // drops it to 0 only if a previous reading proved the EL supports it.
    match local.el.as_ref().and_then(|batch| batch.peers.as_ref()) {
        Ok(Some(peers)) => metrics.set_health(G::ElPeers, *peers as f64),
        Ok(None) => {}
        Err(_) => metrics.update_registered_health(G::ElPeers, 0.0),
    }
    let el_distance = match eval.el_sync {
        ElSyncEval::SyncingProgress { distance, .. } => distance as f64,
        _ => 0.0,
    };
    metrics.set_health(G::ElSyncDistance, el_distance);
    if let Ok(block) = local.el.as_ref().and_then(|batch| batch.block.as_ref()) {
        metrics.set_health(G::ElBlockNumber, block.number as f64);
    }

    if let Some(cl) = &local.cl {
        metrics.set_health(
            G::ClHealth,
            f64::from(eval.cl_sync.is_some_and(ClSyncEval::is_ok)),
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
        metrics.set_health(G::ReferenceHealth, f64::from(reference.state.is_healthy()));
        if let Some(sample) = reference.state.sample() {
            metrics.set_health(G::ReferenceBlockNumber, sample.block_number as f64);
            metrics.set_health(G::ReferenceBlockLag, sample.block_lag as f64);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::health::el::{ElBatchResult, ElBlockResult, ElSyncingResult};
    use clap::Parser;

    #[test]
    fn el_only_probe_metrics_omit_cl_reference_and_unsupported_peer_gauges() {
        let policy =
            crate::Config::parse_from(["ethonyx", "--el-http-url", "http://127.0.0.1:8545"])
                .parse_validated()
                .unwrap()
                .health
                .policy()
                .unwrap();
        let probe = Probe {
            local: Arc::new(LocalProbe {
                el: Ok(ElBatchResult {
                    syncing: Ok(ElSyncingResult::Synced),
                    peers: Ok(None),
                    block: Ok(ElBlockResult {
                        number: 50,
                        timestamp: 1_700_000_000,
                    }),
                }),
                cl: None,
            }),
            reference: None,
        };
        let metrics = ServerMetrics::default();
        // A transient outage before/around `-32601` must never register or pin `ElPeers` at 0.
        let outage = Probe {
            local: Arc::new(LocalProbe {
                el: Err(crate::health::fetch::HealthError::Timeout),
                cl: None,
            }),
            reference: None,
        };
        record_probe_metrics(&metrics, &outage, evaluate_readiness(policy, &outage));
        record_probe_metrics(&metrics, &probe, evaluate_readiness(policy, &probe));

        let rendered = metrics.render_prometheus();
        assert!(rendered.contains("ethonyx_upstream_block_number 50"));
        assert!(!rendered.contains("layer=\"cl\""));
        assert!(!rendered.contains("ethonyx_upstream_slot_number"));
        assert!(!rendered.contains("ethonyx_upstream_peers"));
        assert!(!rendered.contains("ethonyx_reference_"));

        // Once an EL proves `net_peerCount` support, a subsequent outage still drops it to 0.
        let with_peers = Probe {
            local: Arc::new(LocalProbe {
                el: Ok(ElBatchResult {
                    syncing: Ok(ElSyncingResult::Synced),
                    peers: Ok(Some(12)),
                    block: Ok(ElBlockResult {
                        number: 51,
                        timestamp: 1_700_000_001,
                    }),
                }),
                cl: None,
            }),
            reference: None,
        };
        record_probe_metrics(
            &metrics,
            &with_peers,
            evaluate_readiness(policy, &with_peers),
        );
        assert!(
            metrics
                .render_prometheus()
                .contains("ethonyx_upstream_peers{layer=\"el\"} 12")
        );
        record_probe_metrics(&metrics, &outage, evaluate_readiness(policy, &outage));
        assert!(
            metrics
                .render_prometheus()
                .contains("ethonyx_upstream_peers{layer=\"el\"} 0")
        );
    }

    #[test]
    fn readiness_is_logged_once_per_transition_and_a_healthy_start_is_quiet() {
        let log = Arc::new(std::sync::Mutex::new(String::new()));
        struct Sink(Arc<std::sync::Mutex<String>>);
        impl std::io::Write for Sink {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0
                    .lock()
                    .unwrap()
                    .push_str(&String::from_utf8_lossy(buf));
                Ok(buf.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let sink = Arc::clone(&log);
        let subscriber = tracing_subscriber::fmt()
            .with_writer(move || Sink(Arc::clone(&sink)))
            .finish();
        let logged = || {
            let text = log.lock().unwrap();
            (
                text.matches("not ready").count(),
                text.matches("readiness recovered").count(),
            )
        };
        let poller = || {
            let health =
                crate::Config::parse_from(["ethonyx", "--el-http-url", "http://127.0.0.1:8545"])
                    .parse_validated()
                    .unwrap()
                    .health;
            channel(health).1.unwrap()
        };
        let probe = |syncing| Probe {
            local: Arc::new(LocalProbe {
                el: Ok(ElBatchResult {
                    syncing: Ok(syncing),
                    peers: Ok(None),
                    block: Ok(ElBlockResult {
                        number: 50,
                        timestamp: 1_700_000_000,
                    }),
                }),
                cl: None,
            }),
            reference: None,
        };
        let metrics = ServerMetrics::default();

        tracing::subscriber::with_default(subscriber, || {
            let mut healthy_start = poller();
            healthy_start.publish(&metrics, probe(ElSyncingResult::Synced));
            assert_eq!(logged(), (0, 0), "a healthy first poll stays quiet");
            healthy_start.publish(&metrics, probe(ElSyncingResult::SyncingBool));
            healthy_start.publish(&metrics, probe(ElSyncingResult::SyncingBool));
            assert_eq!(logged(), (1, 0), "one warning per degradation");
            healthy_start.publish(&metrics, probe(ElSyncingResult::Synced));
            healthy_start.publish(&metrics, probe(ElSyncingResult::Synced));
            assert_eq!(logged(), (1, 1), "one notice per recovery");

            let mut degraded_start = poller();
            degraded_start.publish(&metrics, probe(ElSyncingResult::SyncingBool));
            assert_eq!(
                logged(),
                (2, 1),
                "a degraded first poll is reported at once"
            );
        });
    }
}
