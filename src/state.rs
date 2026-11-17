use std::sync::Arc;

use http::Uri;
use tokio::sync::Semaphore;

use crate::BoxError;
use crate::config::{Config, RuntimeOpts, SlotClock, ValidatedConfig};
use crate::health::HealthSource;
use crate::metrics::{HealthMetrics, ProxyMetrics};
use crate::transport::{HttpTransport, ProxyClient, build_client, build_connectors};
use crate::upstream::{HttpUpstream, WsUpstream};

pub(crate) struct HttpTarget {
    pub(crate) upstream: HttpUpstream,
    pub(crate) transport: HttpTransport,
}

pub(crate) struct AppState {
    pub(crate) cfg: RuntimeOpts,
    /// Default client: HTTP/1.1 for cleartext, h2 via ALPN for `https`. Serves
    /// the CL Beacon/MEV hops, and the EL hop unless cleartext `h2c` is active.
    pub(crate) client: ProxyClient,
    pub(crate) ws_tls: tokio_tungstenite::Connector,
    pub(crate) el: HttpTarget,
    pub(crate) el_ws: WsUpstream,
    pub(crate) cl: HttpTarget,
    pub(crate) cl_mev_relay: Option<HttpTarget>,
    pub(crate) cl_syncing_uri: Uri,
    pub(crate) cl_peer_count_uri: Uri,
    pub(crate) slot_clock: Option<SlotClock>,
    pub(crate) otel_enabled: bool,
    pub(crate) conn_limit: Arc<Semaphore>,
    pub(crate) health: HealthSource,
    pub(crate) metrics: ProxyMetrics,
    pub(crate) health_metrics: HealthMetrics,
}

impl AppState {
    pub(crate) fn new(
        cfg: &Config,
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
            #[cfg(feature = "otel")]
                otel_endpoint: _,
        } = validated;
        let opts = RuntimeOpts::from_config(cfg);

        let (https, ws_tls) = build_connectors(el_ws.is_tls());
        let client = build_client(https.clone());
        let el = HttpTarget {
            transport: HttpTransport::new_el(cfg.trust_upstream, el_http.is_tls(), https),
            upstream: el_http,
        };
        let cl_syncing_uri = cl_beacon.rewrite_uri(Uri::from_static("/eth/v1/node/syncing"))?;
        let cl_peer_count_uri =
            cl_beacon.rewrite_uri(Uri::from_static("/eth/v1/node/peer_count"))?;
        let cl = HttpTarget {
            transport: HttpTransport::new_cl(cl_beacon.is_tls()),
            upstream: cl_beacon,
        };
        let cl_mev_relay = cl_mev_relay.map(|upstream| HttpTarget {
            transport: HttpTransport::new_cl(upstream.is_tls()),
            upstream,
        });
        let permits = if opts.max_connections == 0 {
            Semaphore::MAX_PERMITS
        } else {
            opts.max_connections
        };
        let conn_limit = Arc::new(Semaphore::new(permits));

        Ok(Self {
            cfg: opts,
            client,
            ws_tls,
            el,
            el_ws,
            cl,
            cl_mev_relay,
            cl_syncing_uri,
            cl_peer_count_uri,
            slot_clock,
            otel_enabled,
            conn_limit,
            health,
            metrics: ProxyMetrics::default(),
            health_metrics: HealthMetrics::default(),
        })
    }
}
