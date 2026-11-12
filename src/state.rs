use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use http::Uri;
use tokio::sync::watch;

use crate::config::Config;
use crate::health::Probe;
use crate::proxy::ProxyClient;
use crate::upstream::{HttpUpstream, WsUpstream};

pub struct AppState {
    pub cfg: Config,
    /// Default client: HTTP/1.1 for cleartext, h2 via ALPN for `https`. Serves
    /// the CL Beacon hop, and the EL hop while `el_use_h2` is false.
    pub client: ProxyClient,
    /// HTTP/2-only client (cleartext h2c prior-knowledge, or h2 over TLS). Serves
    /// the EL JSON-RPC hop while `el_use_h2` is true.
    pub el_h2_client: ProxyClient,
    /// Whether the EL JSON-RPC hop currently uses HTTP/2. The health poller owns
    /// this: it prefers h2c and falls back to HTTP/1.1 when the upstream does not
    /// speak it, so an h2c↔h1 change self-heals within one poll.
    pub el_use_h2: AtomicBool,
    pub(crate) el_http: HttpUpstream,
    pub(crate) el_ws: WsUpstream,
    pub(crate) cl_beacon: HttpUpstream,
    pub(crate) cl_mev_relay: Option<HttpUpstream>,
    pub cl_syncing_uri: Uri,
    pub cl_peer_count_uri: Uri,
    pub cl_genesis_time: u64,
    pub cl_seconds_per_slot: u64,
    pub otel_enabled: bool,
    /// Latest background health poll, read by `/healthz` and `/readyz`.
    pub probe: watch::Receiver<Arc<Probe>>,
}

impl AppState {
    /// Resolve one transport snapshot, shared by probing and forwarding.
    pub const fn el_client(&self, use_h2: bool) -> &ProxyClient {
        if use_h2 {
            &self.el_h2_client
        } else {
            &self.client
        }
    }
}
