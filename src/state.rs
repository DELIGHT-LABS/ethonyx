use std::sync::Arc;

use http::Uri;
use tokio::sync::watch;

use crate::config::Config;
use crate::health::{ElTransport, ObservedHttpVersion, Probe};
use crate::proxy::ProxyClient;
use crate::upstream::{HttpUpstream, WsUpstream};

pub struct AppState {
    pub cfg: Config,
    /// Default client: HTTP/1.1 for cleartext, h2 via ALPN for `https`. Serves
    /// the CL Beacon hop, and the EL hop unless cleartext `h2c` is active.
    pub client: ProxyClient,
    /// HTTP/2-only client (cleartext h2c prior-knowledge). Serves the EL
    /// JSON-RPC hop while `el_transport.uses_h2c_client()` is true.
    pub el_h2_client: ProxyClient,
    pub(crate) el_transport: ElTransport,
    pub(crate) cl_tls_version: Option<ObservedHttpVersion>,
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
    /// Resolve the active EL HTTP client from the current transport state.
    pub fn el_client(&self) -> &ProxyClient {
        if self.el_transport.uses_h2c_client() {
            &self.el_h2_client
        } else {
            &self.client
        }
    }

    pub(crate) fn cl_transport_label(&self) -> &'static str {
        self.cl_tls_version
            .as_ref()
            .map_or("http/1.1", ObservedHttpVersion::label)
    }

    pub(crate) fn cl_startup_proto_label(&self) -> &'static str {
        if self.cl_transport_label() == "h2" {
            "HTTP/2 (TLS)"
        } else {
            "HTTP/1.1"
        }
    }
}
