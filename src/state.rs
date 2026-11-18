use std::sync::Arc;

use http::Uri;
use tokio::sync::Semaphore;

use crate::config::{RuntimeOpts, ValidatedUpstreams};
use crate::health::HealthSource;
use crate::metrics::ServerMetrics;
use crate::transport::{
    HttpTarget, HttpTransport, ProxyClient, WsTarget, build_client, build_connectors,
};
use crate::upstream::WsUpstream;

pub(crate) struct ClState {
    pub(crate) beacon: HttpTarget,
    pub(crate) mev_relay: Option<HttpTarget>,
    pub(crate) syncing_uri: Uri,
    pub(crate) peer_count_uri: Uri,
    pub(crate) mock_subscriptions: bool,
    pub(crate) mock_node_health: bool,
}

pub(crate) struct AppState {
    pub(crate) opts: RuntimeOpts,
    /// Default client: HTTP/1.1 for cleartext, h2 via ALPN for `https`. Serves
    /// the CL Beacon/MEV/Reference hops, and the EL hop unless cleartext `h2c` is active.
    pub(crate) client: ProxyClient,
    pub(crate) el: HttpTarget,
    pub(crate) el_ws: Option<WsTarget>,
    pub(crate) cl: Option<ClState>,
    pub(crate) otel_enabled: bool,
    pub(crate) conn_limit: Arc<Semaphore>,
    pub(crate) health: HealthSource,
    pub(crate) metrics: ServerMetrics,
}

impl AppState {
    pub(crate) fn new(
        opts: RuntimeOpts,
        upstreams: ValidatedUpstreams,
        otel_enabled: bool,
        health: HealthSource,
        metrics: ServerMetrics,
    ) -> Self {
        let ValidatedUpstreams { el_http, el_ws, cl } = upstreams;
        let trust_upstream = matches!(health, HealthSource::Trusted);

        let (https, ws_tls) = build_connectors(el_ws.as_ref().is_some_and(WsUpstream::is_tls));
        let client = build_client(https.clone());
        let el = HttpTarget {
            transport: HttpTransport::new_el(trust_upstream, el_http.is_tls(), https),
            upstream: el_http,
        };
        let el_ws = el_ws.map(|upstream| WsTarget {
            upstream,
            tls: ws_tls,
        });
        let cl = cl.map(|cl| ClState {
            beacon: HttpTarget::new_standard(cl.beacon),
            mev_relay: cl.mev_relay.map(HttpTarget::new_standard),
            syncing_uri: cl.syncing_uri,
            peer_count_uri: cl.peer_count_uri,
            mock_subscriptions: cl.mock_subscriptions,
            mock_node_health: cl.mock_node_health,
        });
        let permits = if opts.max_connections == 0 {
            Semaphore::MAX_PERMITS
        } else {
            opts.max_connections
        };
        let conn_limit = Arc::new(Semaphore::new(permits));

        Self {
            opts,
            client,
            el,
            el_ws,
            cl,
            otel_enabled,
            conn_limit,
            health,
            metrics,
        }
    }
}
