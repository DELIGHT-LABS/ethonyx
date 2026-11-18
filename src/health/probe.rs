//! Background upstream health probing, Prometheus/OTel gauge recording, and readiness transition logging.

use std::sync::Arc;
use std::time::Duration;

use futures_util::future::{Either, OptionFuture};
use futures_util::stream::{self, StreamExt};
use tokio::sync::watch;
use tracing::{debug, info, warn};

use crate::config::{HealthMode, HealthPolicy, PolledHealth};
use crate::metrics::{HealthGauge, ServerMetrics};
use crate::state::{AppState, ClState};
use crate::transport::{HttpTarget, ProxyClient};

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

impl HealthSource {
    pub(crate) fn startup_message(&self) -> &'static str {
        match self {
            Self::Trusted => "upstreams configured (trusted)",
            Self::Polled { rx, .. } if rx.borrow().has_errors() => {
                "upstreams configured (initial probe degraded)"
            }
            Self::Polled { .. } => "upstreams connected",
        }
    }
}

struct PollObserver {
    policy: HealthPolicy,
    /// Previous readiness; starts `true` so a healthy first poll stays quiet and an
    /// unhealthy first poll logs `"not ready"` once (mathematically equivalent to `None`).
    was_ready: bool,
    /// True once `net_peerCount` has succeeded at least once; keeps unsupported `-32601`
    /// EL peer gauges unregistered across outages without coupling to Prometheus storage.
    el_peers_supported: bool,
}

impl PollObserver {
    const fn new(policy: HealthPolicy) -> Self {
        Self {
            policy,
            was_ready: true,
            el_peers_supported: false,
        }
    }

    fn observe(&mut self, metrics: &ServerMetrics, probe: &Probe) {
        let eval = evaluate_readiness(self.policy, probe);
        let ready = eval.is_ready();
        let status = if ready { "ready" } else { "not_ready" };
        debug!(status, "health poll");

        self.record_metrics(metrics, probe, eval);

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
    }

    fn record_metrics(&mut self, metrics: &ServerMetrics, probe: &Probe, eval: ReadinessEval<'_>) {
        use HealthGauge as G;
        let local = &probe.local;

        metrics.set_health(G::ElHealth, f64::from(eval.el_sync.is_ok()));
        // An unsupported `net_peerCount` (`-32601`) leaves the gauge unset; a failed call
        // drops it to 0 only if a previous reading proved the EL supports it.
        match local.el.as_ref().and_then(|batch| batch.peers.as_ref()) {
            Ok(Some(peers)) => {
                self.el_peers_supported = true;
                metrics.set_health(G::ElPeers, *peers as f64);
            }
            Err(_) if self.el_peers_supported => metrics.set_health(G::ElPeers, 0.0),
            Ok(None) | Err(_) => {}
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
}

struct Publisher {
    tx: watch::Sender<Probe>,
    observer: PollObserver,
}

impl Publisher {
    fn new(
        policy: HealthPolicy,
        initial: Probe,
        metrics: &ServerMetrics,
    ) -> (Self, watch::Receiver<Probe>) {
        let mut observer = PollObserver::new(policy);
        observer.observe(metrics, &initial);
        let (tx, rx) = watch::channel(initial);
        (Self { tx, observer }, rx)
    }

    fn publish(&mut self, metrics: &ServerMetrics, probe: Probe) {
        self.observer.observe(metrics, &probe);
        // `AppState` holds a receiver and the poller task holds `Arc<AppState>`.
        self.tx.send_replace(probe);
    }
}

pub(crate) struct HealthPoller {
    publisher: Publisher,
    interval: Duration,
    timeout: Duration,
    reference: Option<ReferencePoller>,
}

/// Warm the initial health snapshot (when `mode` is `HealthMode::Polled`) before
/// constructing `watch::channel`, eliminating any unreachable `Pending` state.
pub(crate) async fn warm(
    mode: HealthMode,
    client: &ProxyClient,
    el: &HttpTarget,
    cl: Option<&ClState>,
    metrics: &ServerMetrics,
) -> (HealthSource, Option<HealthPoller>) {
    let HealthMode::Polled(polled) = mode else {
        return (HealthSource::Trusted, None);
    };
    let PolledHealth {
        interval,
        timeout,
        policy,
        reference,
    } = *polled;
    let mut reference = reference.map(ReferencePoller::new);
    let el_probe = ProbeTarget::from_target(client, el, timeout);
    let ref_fut = reference.as_mut().map(|poller| poller.poll_once(el_probe));
    let (local, ref_probe) = tokio::join!(
        probe_local(client, el, cl, timeout),
        OptionFuture::from(ref_fut),
    );
    let initial = Probe {
        local,
        reference: ref_probe,
    };
    let (publisher, rx) = Publisher::new(policy, initial, metrics);
    (
        HealthSource::Polled { rx, policy },
        Some(HealthPoller {
            publisher,
            interval,
            timeout,
            reference,
        }),
    )
}

impl HealthPoller {
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
    /// concurrently as streams and publish through `&mut Publisher` one at a time.
    async fn run(self, state: Arc<AppState>) {
        let Self {
            mut publisher,
            interval,
            timeout,
            reference,
        } = self;
        let state = &*state;
        let local = stream::unfold((), move |()| async move {
            tokio::time::sleep(interval).await;
            let probe = probe_local(&state.client, &state.el, state.cl.as_ref(), timeout).await;
            Some((Either::Left(probe), ()))
        });
        let references = match reference {
            None => Either::Left(stream::empty()),
            Some(poller) => Either::Right(stream::unfold(poller, move |mut poller| async move {
                tokio::time::sleep(poller.interval()).await;
                let probe = poller
                    .poll_once(ProbeTarget::from_target(&state.client, &state.el, timeout))
                    .await;
                Some((Either::Right(probe), poller))
            })),
        };
        let mut updates = std::pin::pin!(stream::select(local, references));
        while let Some(update) = updates.next().await {
            let probe = match update {
                Either::Left(local) => Probe {
                    local,
                    reference: publisher.tx.borrow().reference.clone(),
                },
                Either::Right(reference) => Probe {
                    local: Arc::clone(&publisher.tx.borrow().local),
                    reference: Some(reference),
                },
            };
            publisher.publish(&state.metrics, probe);
        }
    }
}

/// Query local EL (and CL when configured) signals once. Each call is bounded by
/// `--health-timeout`; failures land in the result rather than aborting.
async fn probe_local(
    client: &ProxyClient,
    el: &HttpTarget,
    cl: Option<&ClState>,
    timeout: Duration,
) -> Arc<LocalProbe> {
    let el_target = ProbeTarget::from_target(client, el, timeout);
    let (el, cl) = match cl {
        Some(cl) => {
            let target = |uri| ProbeTarget::new(client, &cl.beacon, uri, timeout);
            // Independent upstreams share one poll; a slow EL does not delay CL calls.
            // EL still uses one batch request and owns transport detection.
            let (el, status, peers) = tokio::join!(
                el_batch_detect(el_target),
                cl_syncing_status(target(&cl.probe_uris.syncing)),
                cl_peer_count(target(&cl.probe_uris.peer_count)),
            );
            (el, Some(ClProbe { status, peers }))
        }
        None => (el_batch_detect(el_target).await, None),
    };
    Arc::new(LocalProbe { el, cl })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::health::el::{ElBatchResult, ElBlockResult, ElSyncingResult};

    const DEFAULT_POLICY: HealthPolicy = HealthPolicy {
        el_max_reference_lag: 1,
        strict: None,
        slot_clock: None,
    };

    #[test]
    fn el_only_probe_metrics_omit_cl_reference_and_unsupported_peer_gauges() {
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
        let metrics = ServerMetrics::new(None);
        let mut observer = PollObserver::new(DEFAULT_POLICY);
        let mut record =
            |p: &Probe| observer.record_metrics(&metrics, p, evaluate_readiness(DEFAULT_POLICY, p));
        // A transient outage before/around `-32601` must never register or pin `ElPeers` at 0.
        let outage = Probe {
            local: Arc::new(LocalProbe {
                el: Err(crate::health::fetch::HealthError::Timeout),
                cl: None,
            }),
            reference: None,
        };
        record(&outage);
        record(&probe);

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
        record(&with_peers);
        assert!(
            metrics
                .render_prometheus()
                .contains("ethonyx_upstream_peers{layer=\"el\"} 12")
        );
        record(&outage);
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
        let metrics = ServerMetrics::new(None);

        tracing::subscriber::with_default(subscriber, || {
            tracing::callsite::rebuild_interest_cache();
            let (mut healthy_start, _rx) =
                Publisher::new(DEFAULT_POLICY, probe(ElSyncingResult::Synced), &metrics);
            assert_eq!(logged(), (0, 0), "a healthy first poll stays quiet");
            healthy_start.publish(&metrics, probe(ElSyncingResult::SyncingBool));
            healthy_start.publish(&metrics, probe(ElSyncingResult::SyncingBool));
            assert_eq!(logged(), (1, 0), "one warning per degradation");
            healthy_start.publish(&metrics, probe(ElSyncingResult::Synced));
            healthy_start.publish(&metrics, probe(ElSyncingResult::Synced));
            assert_eq!(logged(), (1, 1), "one notice per recovery");

            let (_degraded_start, _rx2) = Publisher::new(
                DEFAULT_POLICY,
                probe(ElSyncingResult::SyncingBool),
                &metrics,
            );
            assert_eq!(
                logged(),
                (2, 1),
                "a degraded first poll is reported at once"
            );
        });
    }
}
