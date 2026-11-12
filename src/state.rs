use std::sync::Arc;

use http::Uri;
use tokio::sync::Semaphore;

use crate::BoxError;
use crate::config::{Config, SlotClock, ValidatedConfig};
use crate::health::{HealthSource, HttpTransport};
use crate::metrics::ProxyMetrics;
use crate::proxy::ProxyClient;
use crate::upstream::{HttpUpstream, WsUpstream};

pub struct AppState {
    pub cfg: Config,
    /// Default client: HTTP/1.1 for cleartext, h2 via ALPN for `https`. Serves
    /// the CL Beacon/MEV hops, and the EL hop unless cleartext `h2c` is active.
    pub client: ProxyClient,
    pub(crate) ws_tls: tokio_tungstenite::Connector,
    pub(crate) el_transport: HttpTransport,
    pub(crate) cl_transport: HttpTransport,
    pub(crate) el_http: HttpUpstream,
    pub(crate) el_ws: WsUpstream,
    pub(crate) cl_beacon: HttpUpstream,
    pub(crate) cl_mev_relay: Option<HttpUpstream>,
    pub cl_syncing_uri: Uri,
    pub cl_peer_count_uri: Uri,
    pub(crate) slot_clock: Option<SlotClock>,
    pub otel_enabled: bool,
    pub(crate) conn_limit: Arc<Semaphore>,
    pub(crate) health: HealthSource,
    pub(crate) metrics: ProxyMetrics,
}

impl AppState {
    pub(crate) fn new(
        cfg: Config,
        validated: ValidatedConfig,
        otel_enabled: bool,
        health: HealthSource,
    ) -> Result<Self, BoxError> {
        let ValidatedConfig {
            listen: _,
            el_http,
            el_ws,
            cl_beacon,
            cl_mev_relay,
            slot_clock,
        } = validated;

        let (https, ws_tls) = crate::proxy::build_connectors();
        let client = crate::proxy::build_client(https.clone());
        let el_transport = HttpTransport::new_el(cfg.trust_upstream, el_http.is_tls(), https);
        let cl_transport = HttpTransport::new_cl(cl_beacon.is_tls());
        let cl_syncing_uri = cl_beacon.rewrite_uri(Uri::from_static("/eth/v1/node/syncing"))?;
        let cl_peer_count_uri =
            cl_beacon.rewrite_uri(Uri::from_static("/eth/v1/node/peer_count"))?;
        let permits = if cfg.max_connections == 0 {
            Semaphore::MAX_PERMITS
        } else {
            cfg.max_connections
        };
        let conn_limit = Arc::new(Semaphore::new(permits));

        Ok(Self {
            cfg,
            client,
            ws_tls,
            el_transport,
            cl_transport,
            el_http,
            el_ws,
            cl_beacon,
            cl_mev_relay,
            cl_syncing_uri,
            cl_peer_count_uri,
            slot_clock,
            otel_enabled,
            conn_limit,
            health,
            metrics: ProxyMetrics::default(),
        })
    }
}
