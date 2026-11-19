use std::num::NonZeroUsize;
use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};

use crate::config::{ProxyOpts, ValidatedUpstreams};
use crate::health::HealthSource;
use crate::metrics::ServerMetrics;
use crate::transport::{HttpTarget, ProxyClient, WsTarget, build_client, build_connectors};
use crate::upstream::WsUpstream;

pub(crate) struct ClState {
    pub(crate) beacon: HttpTarget,
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
    pub(crate) conn_limit: Arc<Semaphore>,
    pub(crate) health: HealthSource,
    pub(crate) metrics: ServerMetrics,
}

impl AppState {
    pub(crate) fn new(
        opts: ProxyOpts,
        upstreams: ValidatedUpstreams,
        health: HealthSource,
        metrics: ServerMetrics,
    ) -> Self {
        let ValidatedUpstreams { el_http, el_ws, cl } = upstreams;
        let trust_upstream = matches!(health, HealthSource::Trusted);

        let (https, ws_tls) = build_connectors(el_ws.as_ref().is_some_and(WsUpstream::is_tls));
        let el = HttpTarget::new_el(el_http, trust_upstream, &https);
        let client = build_client(https);
        let el_ws = el_ws.map(|upstream| WsTarget {
            upstream,
            tls: ws_tls,
        });
        let cl = cl.map(|cl| ClState {
            beacon: HttpTarget::new_standard(cl.beacon),
            mev_relay: cl.mev_relay.map(HttpTarget::new_standard),
            mock_subscriptions: cl.mock_subscriptions,
            mock_node_health: cl.mock_node_health,
        });
        let permits = opts
            .max_connections
            .map_or(Semaphore::MAX_PERMITS, NonZeroUsize::get);
        let conn_limit = Arc::new(Semaphore::new(permits));

        Self {
            opts,
            client,
            el,
            el_ws,
            cl,
            conn_limit,
            health,
            metrics,
        }
    }

    /// Try to claim a `--max-connections` permit without blocking, incrementing
    /// `ethonyx_connection_limit_saturated_total` on contention.
    pub(crate) fn try_acquire_permit(&self) -> Result<OwnedSemaphorePermit, TryAcquireError> {
        let res = Arc::clone(&self.conn_limit).try_acquire_owned();
        if matches!(res, Err(TryAcquireError::NoPermits)) {
            self.metrics.inc_saturated();
        }
        res
    }
}
