use std::sync::Arc;

use http::Uri;
use tokio::sync::Semaphore;

use crate::config::Config;
use crate::health::{ClTransport, ElTransport, HealthSource};
use crate::proxy::{ProxyClient, UpstreamClient};
use crate::upstream::{HttpUpstream, WsUpstream};

pub struct AppState {
    pub cfg: Config,
    /// Default client: HTTP/1.1 for cleartext, h2 via ALPN for `https`. Serves
    /// the CL Beacon/MEV hops, and the EL hop unless cleartext `h2c` is active.
    pub client: ProxyClient,
    pub(crate) el_transport: ElTransport,
    pub(crate) cl_transport: ClTransport,
    pub(crate) el_http: HttpUpstream,
    pub(crate) el_ws: WsUpstream,
    pub(crate) cl_beacon: HttpUpstream,
    pub(crate) cl_mev_relay: Option<HttpUpstream>,
    pub cl_syncing_uri: Uri,
    pub cl_peer_count_uri: Uri,
    pub cl_genesis_time: u64,
    pub cl_seconds_per_slot: u64,
    pub otel_enabled: bool,
    pub(crate) conn_limit: Option<Arc<Semaphore>>,
    pub(crate) health: HealthSource,
}

impl AppState {
    /// Resolve the active EL HTTP client from the current transport state.
    pub(crate) fn el_client(&self) -> UpstreamClient<'_> {
        self.el_transport.select_client(&self.client)
    }
}
