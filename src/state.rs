use std::num::NonZeroUsize;
use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::config::{ClProbeUris, HealthMode, ProxyOpts, ValidatedUpstreams};
use crate::health::{self, HealthPoller, HealthSource};
use crate::metrics::ServerMetrics;
use crate::transport::{HttpTarget, ProxyClient, WsTarget, build_client, build_connectors};
use crate::upstream::WsUpstream;

struct BoundedLimit {
    sem: Arc<Semaphore>,
    max: NonZeroUsize,
}

/// Optional connection concurrency cap (`None` when `--max-connections 0`).
/// Avoids semaphore atomic traffic completely when unlimited and eliminates
/// impossible `TryAcquireError::Closed` handling across callers.
pub(crate) struct ConnLimit(Option<BoundedLimit>);

/// RAII permit for one active connection or dedicated HTTP/2 WebSocket stream.
/// Carries zero runtime overhead when `--max-connections 0`.
#[must_use = "connection permit is released immediately if dropped"]
pub(crate) struct ConnPermitGuard {
    permit: Option<OwnedSemaphorePermit>,
}

impl ConnPermitGuard {
    pub(crate) const fn unlimited() -> Self {
        Self { permit: None }
    }

    pub(crate) fn into_inner(self) -> Option<OwnedSemaphorePermit> {
        self.permit
    }

    #[cfg(test)]
    pub(crate) const fn is_bounded(&self) -> bool {
        self.permit.is_some()
    }
}

impl ConnLimit {
    fn new(max: Option<NonZeroUsize>) -> Self {
        Self(max.map(|max| BoundedLimit {
            sem: Arc::new(Semaphore::new(max.get())),
            max,
        }))
    }

    pub(crate) const fn max(&self) -> Option<NonZeroUsize> {
        match &self.0 {
            Some(bounded) => Some(bounded.max),
            None => None,
        }
    }

    /// Try to claim a permit without blocking.
    fn try_acquire(&self) -> Option<ConnPermitGuard> {
        let Some(bounded) = &self.0 else {
            return Some(ConnPermitGuard::unlimited());
        };
        Arc::clone(&bounded.sem)
            .try_acquire_owned()
            .ok()
            .map(|permit| ConnPermitGuard {
                permit: Some(permit),
            })
    }

    /// Wait until a permit becomes available (resolves immediately when unlimited).
    pub(crate) async fn acquire(&self) -> Option<ConnPermitGuard> {
        let Some(bounded) = &self.0 else {
            return Some(ConnPermitGuard::unlimited());
        };
        let permit = Arc::clone(&bounded.sem).acquire_owned().await.ok()?;
        Some(ConnPermitGuard {
            permit: Some(permit),
        })
    }

    #[cfg(test)]
    pub(crate) fn available_permits(&self) -> Option<usize> {
        self.0.as_ref().map(|b| b.sem.available_permits())
    }
}

pub(crate) struct ClState {
    pub(crate) beacon: HttpTarget,
    pub(crate) probe_uris: ClProbeUris,
    pub(crate) mev_relay: Option<HttpTarget>,
    pub(crate) mock_subscriptions: bool,
    pub(crate) mock_node_health: bool,
}

pub(crate) struct AppState {
    pub(crate) opts: ProxyOpts,
    /// Default client: HTTP/1.1 for cleartext, h2 via ALPN for `https`. Serves
    /// the CL Beacon/MEV/Reference hops, and the EL hop unless cleartext `h2c` is active.
    pub(crate) client: ProxyClient,
    pub(crate) el: HttpTarget,
    pub(crate) el_ws: Option<WsTarget>,
    pub(crate) cl: Option<ClState>,
    pub(crate) conn_limit: ConnLimit,
    pub(crate) health: HealthSource,
    pub(crate) metrics: ServerMetrics,
}

impl AppState {
    /// Build runtime state and warm the initial health snapshot (unless `--trust-upstream`
    /// is set).
    pub(crate) async fn start(
        opts: ProxyOpts,
        max_connections: Option<NonZeroUsize>,
        upstreams: ValidatedUpstreams,
        health: HealthMode,
        metrics: ServerMetrics,
    ) -> (Arc<Self>, Option<HealthPoller>) {
        let ValidatedUpstreams { el_http, el_ws, cl } = upstreams;
        let trust_upstream = matches!(health, HealthMode::Trusted);

        let (https, ws_tls) = build_connectors(el_ws.as_ref().is_some_and(WsUpstream::is_tls));
        let el = HttpTarget::new_el(el_http, trust_upstream, &https);
        let client = build_client(https);
        let el_ws = el_ws.map(|upstream| WsTarget {
            upstream,
            tls: ws_tls,
        });
        let cl = cl.map(|cl| ClState {
            beacon: HttpTarget::new_standard(cl.beacon),
            probe_uris: cl.probe_uris,
            mev_relay: cl.mev_relay.map(HttpTarget::new_untracked),
            mock_subscriptions: cl.mock_subscriptions,
            mock_node_health: cl.mock_node_health,
        });
        let (health, poller) = health::warm(health, &client, &el, cl.as_ref(), &metrics).await;
        let conn_limit = ConnLimit::new(max_connections);

        (
            Arc::new(Self {
                opts,
                client,
                el,
                el_ws,
                cl,
                conn_limit,
                health,
                metrics,
            }),
            poller,
        )
    }

    /// Try to claim a `--max-connections` permit without blocking, incrementing
    /// `ethonyx_connection_limit_saturated_total` on contention.
    pub(crate) fn try_acquire_permit(&self) -> Option<ConnPermitGuard> {
        let permit = self.conn_limit.try_acquire();
        if permit.is_none() {
            self.metrics.inc_saturated();
        }
        permit
    }
}
