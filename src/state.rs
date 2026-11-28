use std::num::NonZeroUsize;
use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::config::{ClProbeUris, CoreConfig, HealthMode, ProxyConfig, ProxyOpts};
use crate::health::{self, HealthPoller, HealthSource};
use crate::metrics::ServerMetrics;
use crate::transport::{
    HttpTarget, Https, ProxyClient, WsTarget, build_client, build_connectors, build_https_connector,
};
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
    pub(crate) fn new(max: Option<NonZeroUsize>) -> Self {
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
    pub(crate) fn try_acquire(&self) -> Option<ConnPermitGuard> {
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

pub(crate) struct ClProbeTarget {
    pub(crate) beacon: HttpTarget,
    pub(crate) probe_uris: ClProbeUris,
}

pub(crate) struct ClProxyOpts {
    pub(crate) mev_relay: Option<HttpTarget>,
    pub(crate) mock_subscriptions: bool,
    pub(crate) mock_node_health: bool,
}

/// Shared upstream-client, health, and metrics core used by background polling,
/// `--admin-listen` probes, and (via [`ProxyState`]) the data plane.
pub(crate) struct CoreState {
    /// Default client: HTTP/1.1 for cleartext, h2 via ALPN for `https`. Serves
    /// the CL Beacon/MEV/Reference hops, and the EL hop unless cleartext `h2c` is active.
    pub(crate) client: ProxyClient,
    pub(crate) el: HttpTarget,
    pub(crate) cl: Option<ClProbeTarget>,
    pub(crate) health: HealthSource,
    pub(crate) metrics: Arc<ServerMetrics>,
}

impl CoreState {
    /// Build core runtime state and warm the initial health snapshot (unless
    /// `--trust-upstream` is set).
    pub(crate) async fn start(
        core: CoreConfig,
        metrics: Arc<ServerMetrics>,
    ) -> (Arc<Self>, Option<HealthPoller>) {
        Self::start_with_https(build_https_connector(), core, metrics).await
    }

    async fn start_with_https(
        https: Https,
        core: CoreConfig,
        metrics: Arc<ServerMetrics>,
    ) -> (Arc<Self>, Option<HealthPoller>) {
        let CoreConfig {
            el_http,
            cl,
            health,
        } = core;
        let trust_upstream = matches!(health, HealthMode::Trusted);

        let el = HttpTarget::new_el(el_http, trust_upstream, &https);
        let client = build_client(https);
        let cl = cl.map(|cl| ClProbeTarget {
            beacon: HttpTarget::new_standard(cl.beacon),
            probe_uris: cl.probe_uris,
        });
        let (health, poller) = health::warm(health, &client, &el, cl.as_ref(), &metrics).await;

        (
            Arc::new(Self {
                client,
                el,
                cl,
                health,
                metrics,
            }),
            poller,
        )
    }
}

/// Data-plane proxy state layered over [`CoreState`]; instantiated only when at least
/// one `--listen` listener is configured.
pub(crate) struct ProxyState {
    pub(crate) core: Arc<CoreState>,
    pub(crate) opts: ProxyOpts,
    pub(crate) el_ws: Option<WsTarget>,
    pub(crate) cl_proxy: ClProxyOpts,
    pub(crate) conn_limit: ConnLimit,
}

impl ProxyState {
    /// Build both [`CoreState`] (sharing one WebPKI TLS configuration with `wss://` when
    /// configured) and the data-plane [`ProxyState`], warming the initial health snapshot.
    pub(crate) async fn start(
        core: CoreConfig,
        proxy: ProxyConfig,
        metrics: Arc<ServerMetrics>,
    ) -> (Arc<Self>, Option<HealthPoller>) {
        let ProxyConfig {
            opts,
            max_connections,
            el_ws,
            cl_mev_relay,
            mock_cl_subscriptions,
            mock_cl_node_health,
        } = proxy;

        let (https, ws_tls) = build_connectors(el_ws.as_ref().is_some_and(WsUpstream::is_tls));
        let (core, poller) = CoreState::start_with_https(https, core, metrics).await;
        let el_ws = el_ws.map(|upstream| WsTarget {
            upstream,
            tls: ws_tls,
        });
        let cl_proxy = ClProxyOpts {
            mev_relay: cl_mev_relay.map(HttpTarget::new_untracked),
            mock_subscriptions: mock_cl_subscriptions,
            mock_node_health: mock_cl_node_health,
        };
        let conn_limit = ConnLimit::new(max_connections);

        (
            Arc::new(Self {
                core,
                opts,
                el_ws,
                cl_proxy,
                conn_limit,
            }),
            poller,
        )
    }

    /// Try to claim a `--max-connections` permit without blocking, incrementing
    /// `ethonyx_connection_limit_saturated_total` on contention.
    pub(crate) fn try_acquire_permit(&self) -> Option<ConnPermitGuard> {
        let permit = self.conn_limit.try_acquire();
        if permit.is_none() {
            self.core.metrics.inc_saturated();
        }
        permit
    }
}
