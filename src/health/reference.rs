//! Reference Execution-Layer block-height comparison and `eth_chainId` verification.

use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use futures_util::future::join_all;
use http::Uri;
use tracing::{info, warn};

use crate::config::ValidatedReference;
use crate::transport::{HttpTarget, ProxyClient};
use crate::upstream::HttpUpstream;

use super::el::{el_block_number, el_chain_id};
use super::fetch::{HealthError, ProbeTarget};

/// Verified reference head height and causal local block lag from a single poll round.
#[derive(Clone, Copy)]
#[cfg_attr(test, derive(Debug, PartialEq, Eq))]
pub(super) struct ReferenceSample {
    pub(super) block_number: u64,
    pub(super) block_lag: u64,
}

/// Outcome of one `--el-reference-url` poll round, shared via `Arc` inside [`super::eval::Probe`].
#[cfg_attr(test, derive(Debug, PartialEq, Eq))]
pub(super) struct ReferenceProbe {
    pub(super) state: ReferenceState,
    pub(super) errors: Box<[Box<str>]>,
}

/// Two-strike availability of the lag sample. A round yields a sample only when at
/// least one reference on the local `chain_id` answered and the local head was read
/// immediately after it; any round without a sample counts as one missed round.
#[derive(Clone, Copy)]
#[cfg_attr(test, derive(Debug, PartialEq, Eq))]
pub(super) enum ReferenceState {
    /// This round produced a verified sample.
    Active(ReferenceSample),
    /// First round without a sample right after [`Self::Active`]: retains the last
    /// sample for one round while reporting `healthy = false`.
    Retained(ReferenceSample),
    /// Initial pending state, or two or more consecutive rounds without a sample (fail-open).
    Unavailable,
}

impl ReferenceState {
    pub(super) const fn is_healthy(self) -> bool {
        matches!(self, Self::Active(_))
    }

    /// Current `(block_number, block_lag)` sample judged by `/readyz` and `/healthz`.
    pub(super) const fn sample(self) -> Option<ReferenceSample> {
        match self {
            Self::Active(sample) | Self::Retained(sample) => Some(sample),
            Self::Unavailable => None,
        }
    }

    const fn on_missed_round(self) -> Self {
        match self {
            Self::Active(sample) => Self::Retained(sample),
            Self::Retained(_) | Self::Unavailable => Self::Unavailable,
        }
    }
}

impl ReferenceProbe {
    pub(super) fn pending() -> Self {
        Self {
            state: ReferenceState::Unavailable,
            errors: Box::new([Box::from("initial poll pending")]),
        }
    }
}

/// One reference's contribution to a round: its `(chain_id, head)` and, when it was
/// the first to raise the round's highest verified head, the local head read right
/// after it.
type Answer = Result<(u64, u64, Option<Result<u64, HealthError>>), HealthError>;

struct ReferenceEndpoint {
    target: HttpTarget,
    /// `None` while matching (or never mismatched); `Some(got)` while mismatching.
    mismatched_chain_id: Option<u64>,
}

impl ReferenceEndpoint {
    fn record_chain_id(&mut self, expected: u64, got: u64) -> bool {
        if got == expected {
            if self.mismatched_chain_id.take().is_some() {
                info!(
                    reference = %self.target.upstream.redacted(),
                    chain_id = expected,
                    "reference chain_id recovered"
                );
            }
            true
        } else {
            if self.mismatched_chain_id.replace(got) != Some(got) {
                warn!(
                    reference = %self.target.upstream.redacted(),
                    expected,
                    got,
                    "reference chain_id mismatch"
                );
            }
            false
        }
    }
}

pub(super) struct ReferencePoller {
    interval: Duration,
    endpoints: Box<[ReferenceEndpoint]>,
    state: ReferenceState,
}

impl ReferencePoller {
    pub(super) fn new(validated: ValidatedReference) -> Self {
        let endpoints = validated
            .upstreams
            .into_iter()
            .map(|upstream| ReferenceEndpoint {
                target: HttpTarget::new_standard(upstream),
                mismatched_chain_id: None,
            })
            .collect::<Box<[_]>>();
        Self {
            interval: validated.poll_interval,
            endpoints,
            state: ReferenceState::Unavailable,
        }
    }

    pub(super) const fn interval(&self) -> Duration {
        self.interval
    }

    /// One round: learn the local `chain_id`, gather every reference's answer, and
    /// judge the round into a lag sample or a missed round.
    pub(super) async fn poll_once(
        &mut self,
        client: &ProxyClient,
        local_el: &HttpTarget,
        timeout: Duration,
    ) -> Arc<ReferenceProbe> {
        let local = ProbeTarget::from_target(client, local_el, timeout);
        let local_uri = local_el.upstream.uri();
        let (sample, errors) = match el_chain_id(local, local_uri).await {
            // No reference answer can be verified without the local chain id, so none is requested.
            Err(err) => (
                None,
                vec![format!("local eth_chainId: {err}").into_boxed_str()],
            ),
            Ok(expected) => {
                let answers = self
                    .gather(client, local, local_uri, timeout, expected)
                    .await;
                self.judge(expected, answers)
            }
        };

        let state = match sample {
            Some(sample) => ReferenceState::Active(sample),
            None => self.state.on_missed_round(),
        };
        self.state = state;
        Arc::new(ReferenceProbe {
            state,
            errors: errors.into_boxed_slice(),
        })
    }

    /// Ask every reference for `(chain_id, head)` concurrently. Only the first reference
    /// to raise the round's highest verified head reads the local head, right after its
    /// own answer (`t_local >= t_ref`, so local progress can only shrink lag). Equal or
    /// lower heads arriving later could only pair with a later local read and never win.
    async fn gather(
        &self,
        client: &ProxyClient,
        local: ProbeTarget<'_>,
        local_uri: &Uri,
        timeout: Duration,
        expected: u64,
    ) -> Vec<Answer> {
        // The round's futures run cooperatively on one task; the inline `Mutex` is never
        // held across `.await` and covers the full `0..=u64::MAX` range.
        let highest = Mutex::new(None::<u64>);
        join_all(self.endpoints.iter().map(|endpoint| {
            let reference = ProbeTarget::from_target(client, &endpoint.target, timeout);
            let uri = endpoint.target.upstream.uri();
            let highest = &highest;
            async move {
                let (chain_id, head) = tokio::try_join!(
                    el_chain_id(reference, uri),
                    el_block_number(reference, uri),
                )?;
                let raised = chain_id == expected && {
                    let mut max_seen = highest.lock().unwrap_or_else(PoisonError::into_inner);
                    let is_higher = max_seen.is_none_or(|prev| head > prev);
                    if is_higher {
                        *max_seen = Some(head);
                    }
                    is_higher
                };
                let local_head = if raised {
                    Some(el_block_number(local, local_uri).await)
                } else {
                    None
                };
                Ok((chain_id, head, local_head))
            }
        }))
        .await
    }

    /// Fold a finished round, in flag order, into the lag sample and the error list.
    /// Paired heads are distinct (only the reference that raised the highest head reads
    /// the local head), so the highest completed pair wins.
    fn judge(
        &mut self,
        expected: u64,
        answers: Vec<Answer>,
    ) -> (Option<ReferenceSample>, Vec<Box<str>>) {
        let total = self.endpoints.len();
        let mut errors = Vec::new();
        let mut best: Option<(u64, u64)> = None;
        let mut local_head_err = None;
        for (idx, (endpoint, answer)) in self.endpoints.iter_mut().zip(answers).enumerate() {
            let (chain_id, head, local_head) = match answer {
                Ok(answer) => answer,
                Err(err) => {
                    errors.push(format_ref_error(
                        idx,
                        total,
                        &endpoint.target.upstream,
                        &err,
                    ));
                    continue;
                }
            };
            if !endpoint.record_chain_id(expected, chain_id) {
                errors.push(format_ref_error(
                    idx,
                    total,
                    &endpoint.target.upstream,
                    &format_args!(
                        "chain_id mismatch (expected 0x{expected:x}, got 0x{chain_id:x})"
                    ),
                ));
                continue;
            }
            match local_head {
                // Paired heads are distinct; highest completed head wins.
                Some(Ok(local_head)) => {
                    best = best.max(Some((head, head.saturating_sub(local_head))));
                }
                Some(Err(err)) => {
                    local_head_err.get_or_insert(err);
                }
                None => {}
            }
        }
        if let Some(err) = local_head_err {
            errors.push(format!("local eth_blockNumber: {err}").into_boxed_str());
        }
        let sample = best.map(|(block_number, block_lag)| ReferenceSample {
            block_number,
            block_lag,
        });
        (sample, errors)
    }
}

fn format_ref_error(
    idx: usize,
    total: usize,
    upstream: &HttpUpstream,
    detail: &dyn fmt::Display,
) -> Box<str> {
    if total > 1 {
        format!("ref[{idx}] {}: {detail}", upstream.redacted()).into_boxed_str()
    } else {
        format!("{}: {detail}", upstream.redacted()).into_boxed_str()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn two_strike_state_machine_retains_once_then_becomes_unavailable() {
        let zero = ReferenceSample {
            block_number: 0,
            block_lag: 0,
        };
        let active = ReferenceState::Active(zero);
        assert!(active.is_healthy());
        assert_eq!(active.sample(), Some(zero));

        let retained = active.on_missed_round();
        assert_eq!(retained, ReferenceState::Retained(zero));
        assert!(!retained.is_healthy());
        assert_eq!(retained.sample(), Some(zero));

        let unavailable = retained.on_missed_round();
        assert_eq!(unavailable, ReferenceState::Unavailable);
        assert!(!unavailable.is_healthy());
        assert_eq!(unavailable.sample(), None);
    }

    #[test]
    fn chain_id_mismatch_tracks_both_remote_and_local_chain_switches() {
        let mut ep = ReferenceEndpoint {
            target: HttpTarget::new_standard(
                HttpUpstream::parse("--el-reference-url", "http://127.0.0.1:8545").unwrap(),
            ),
            mismatched_chain_id: None,
        };

        // Initially matches chain 1.
        assert!(ep.record_chain_id(1, 1));
        assert_eq!(ep.mismatched_chain_id, None);

        // Local restarts onto chain 2 while remote still serves chain 1: must enter mismatch!
        assert!(!ep.record_chain_id(2, 1));
        assert_eq!(ep.mismatched_chain_id, Some(1));

        // Remote switches to chain 2: recovers cleanly back to None.
        assert!(ep.record_chain_id(2, 2));
        assert_eq!(ep.mismatched_chain_id, None);
    }

    #[test]
    fn judge_picks_highest_verified_head_and_reports_errors_in_flag_order() {
        let upstream = |host: &str| {
            HttpUpstream::parse("--el-reference-url", &format!("http://{host}/key")).unwrap()
        };
        let mut poller = ReferencePoller::new(ValidatedReference {
            upstreams: Box::new([upstream("a"), upstream("b"), upstream("c"), upstream("d")]),
            poll_interval: Duration::from_secs(3),
        });

        let (sample, errors) = poller.judge(
            1,
            vec![
                Ok((1, 1000, Some(Ok(997)))),
                Ok((5, 2000, None)),
                Err(HealthError::Timeout),
                Ok((1, 1001, Some(Ok(1000)))),
            ],
        );
        assert_eq!(
            sample,
            Some(ReferenceSample {
                block_number: 1001,
                block_lag: 1,
            })
        );
        assert_eq!(
            errors,
            [
                Box::from("ref[1] http://b/<redacted>: chain_id mismatch (expected 0x1, got 0x5)"),
                Box::<str>::from("ref[2] http://c/<redacted>: timeout"),
            ]
        );
        assert_eq!(poller.endpoints[1].mismatched_chain_id, Some(5));

        // A failed local head read leaves no sample and is reported once, after the references.
        let (sample, errors) = poller.judge(
            1,
            vec![
                Ok((1, 1000, Some(Err(HealthError::Timeout)))),
                Ok((1, 1000, None)),
                Ok((1, 999, None)),
                Ok((1, 1000, None)),
            ],
        );
        assert_eq!(sample, None);
        assert_eq!(errors, [Box::<str>::from("local eth_blockNumber: timeout")]);
        assert_eq!(poller.endpoints[1].mismatched_chain_id, None);

        // When the highest head could not be paired, the highest completed pair stands,
        // exactly as if that reference had not answered; the failed read is still reported.
        let (sample, errors) = poller.judge(
            1,
            vec![
                Ok((1, 1000, Some(Ok(999)))),
                Ok((1, 1005, Some(Err(HealthError::Timeout)))),
                Ok((1, 1005, None)),
                Err(HealthError::Timeout),
            ],
        );
        assert_eq!(
            sample,
            Some(ReferenceSample {
                block_number: 1000,
                block_lag: 1,
            })
        );
        assert_eq!(
            errors,
            [
                Box::<str>::from("ref[3] http://d/<redacted>: timeout"),
                Box::<str>::from("local eth_blockNumber: timeout"),
            ]
        );
    }

    #[tokio::test]
    async fn slow_peer_with_equal_head_never_masks_fast_peer_lag() {
        use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

        use bytes::Bytes;
        use http::Response;
        use http_body_util::BodyExt;
        use hyper::service::service_fn;
        use hyper_util::rt::{TokioExecutor, TokioIo};
        use hyper_util::server::conn::auto;
        use tokio::net::TcpListener;

        use crate::response::full_body;
        use crate::transport::{build_client, build_connectors};

        // Local node starts at 997 (`lag = 3` behind head 1000) and would advance to 999
        // if queried a second time when the slow peer arrives.
        let local_head = Arc::new(AtomicU64::new(997));
        let local_block_calls = Arc::new(AtomicUsize::new(0));
        let fail_local_cid = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let lh = Arc::clone(&local_head);
        let lbc = Arc::clone(&local_block_calls);
        let fcid = Arc::clone(&fail_local_cid);
        let local_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let local_addr = local_listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = local_listener.accept().await else {
                    break;
                };
                let lh = Arc::clone(&lh);
                let lbc = Arc::clone(&lbc);
                let fcid = Arc::clone(&fcid);
                tokio::spawn(async move {
                    let svc = service_fn(move |req: http::Request<hyper::body::Incoming>| {
                        let lh = Arc::clone(&lh);
                        let lbc = Arc::clone(&lbc);
                        let fcid = Arc::clone(&fcid);
                        async move {
                            let body = req.into_body().collect().await.unwrap().to_bytes();
                            let reply = if body.windows(11).any(|w| w == b"eth_chainId") {
                                if fcid.load(Ordering::SeqCst) {
                                    r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"down"}}"#.to_string()
                                } else {
                                    r#"{"jsonrpc":"2.0","id":1,"result":"0x1"}"#.to_string()
                                }
                            } else {
                                lbc.fetch_add(1, Ordering::SeqCst);
                                let cur = lh.fetch_add(2, Ordering::SeqCst);
                                format!(r#"{{"jsonrpc":"2.0","id":1,"result":"0x{cur:x}"}}"#)
                            };
                            Ok::<_, std::convert::Infallible>(Response::new(full_body(
                                Bytes::from(reply),
                            )))
                        }
                    });
                    let _ = auto::Builder::new(TokioExecutor::new())
                        .serve_connection(TokioIo::new(stream), svc)
                        .await;
                });
            }
        });

        let ref_head = Arc::new(AtomicU64::new(1000));
        let ref_calls = Arc::new(AtomicUsize::new(0));
        let spawn_ref = |delay: Duration| {
            let rh = Arc::clone(&ref_head);
            let rc = Arc::clone(&ref_calls);
            async move {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let addr = listener.local_addr().unwrap();
                tokio::spawn(async move {
                    loop {
                        let Ok((stream, _)) = listener.accept().await else {
                            break;
                        };
                        let rh = Arc::clone(&rh);
                        let rc = Arc::clone(&rc);
                        tokio::spawn(async move {
                            let svc =
                                service_fn(move |req: http::Request<hyper::body::Incoming>| {
                                    let rh = Arc::clone(&rh);
                                    let rc = Arc::clone(&rc);
                                    async move {
                                        rc.fetch_add(1, Ordering::SeqCst);
                                        let body =
                                            req.into_body().collect().await.unwrap().to_bytes();
                                        if !delay.is_zero() {
                                            tokio::time::sleep(delay).await;
                                        }
                                        let res = if body.windows(11).any(|w| w == b"eth_chainId") {
                                            r#"{"jsonrpc":"2.0","id":1,"result":"0x1"}"#.to_string()
                                        } else {
                                            format!(
                                                r#"{{"jsonrpc":"2.0","id":1,"result":"0x{:x}"}}"#,
                                                rh.load(Ordering::SeqCst)
                                            )
                                        };
                                        Ok::<_, std::convert::Infallible>(Response::new(full_body(
                                            Bytes::from(res),
                                        )))
                                    }
                                });
                            let _ = auto::Builder::new(TokioExecutor::new())
                                .serve_connection(TokioIo::new(stream), svc)
                                .await;
                        });
                    }
                });
                HttpUpstream::parse("--el-reference-url", &format!("http://{addr}")).unwrap()
            }
        };

        let ep_fast = spawn_ref(Duration::ZERO).await;
        let ep_slow = spawn_ref(Duration::from_millis(80)).await;
        let mut poller = ReferencePoller::new(ValidatedReference {
            upstreams: Box::new([ep_fast, ep_slow]),
            poll_interval: Duration::from_secs(3),
        });
        let (https, _) = build_connectors(false);
        let client = build_client(https);
        let local_target = HttpTarget::new_standard(
            HttpUpstream::parse("--el-http-url", &format!("http://{local_addr}")).unwrap(),
        );

        let probe = poller
            .poll_once(&client, &local_target, Duration::from_secs(2))
            .await;
        assert_eq!(
            probe.state,
            ReferenceState::Active(ReferenceSample {
                block_number: 1000,
                block_lag: 3,
            }),
            "slow peer returning stale 1000 after local advanced to 999 must not mask fast peer lag=3"
        );
        assert_eq!(
            local_block_calls.load(Ordering::SeqCst),
            1,
            "equal-head slow peer must coalesce without a redundant local eth_blockNumber call"
        );

        // `u64::MAX` from both peers still coalesces to a single local `eth_blockNumber` read.
        ref_head.store(u64::MAX, Ordering::SeqCst);
        let max_probe = poller
            .poll_once(&client, &local_target, Duration::from_secs(2))
            .await;
        assert!(max_probe.state.is_healthy());
        assert_eq!(local_block_calls.load(Ordering::SeqCst), 2);

        // Failing local `eth_chainId` skips querying remote references altogether.
        let calls_before = ref_calls.load(Ordering::SeqCst);
        fail_local_cid.store(true, Ordering::SeqCst);
        let err_probe = poller
            .poll_once(&client, &local_target, Duration::from_secs(2))
            .await;
        assert!(!err_probe.state.is_healthy());
        assert_eq!(ref_calls.load(Ordering::SeqCst), calls_before);
    }
}
