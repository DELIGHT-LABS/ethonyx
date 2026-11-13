//! Upstream HTTP/TLS client builders and negotiated protocol tracking.

use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Duration;

use bytes::Bytes;
use http::Version;
use http_body_util::{BodyExt, Full, combinators::UnsyncBoxBody};
use hyper_util::client::legacy::{Client, connect::HttpConnector};
use hyper_util::rt::{TokioExecutor, TokioTimer};
use tracing::{info, warn};

use crate::BoxError;

pub(crate) type ResBody = UnsyncBoxBody<Bytes, BoxError>;
pub(crate) type Https = hyper_rustls::HttpsConnector<HttpConnector>;
pub(crate) type ProxyClient = Client<Https, ResBody>;

pub(crate) fn box_full(body: Full<Bytes>) -> ResBody {
    body.map_err(|e: Infallible| match e {}).boxed_unsync()
}

fn build_http_connector() -> HttpConnector {
    let mut http = HttpConnector::new();
    http.set_nodelay(true);
    http.enforce_http(false);
    http.set_keepalive(Some(Duration::from_secs(60)));
    http
}

fn configure_client_builder() -> hyper_util::client::legacy::Builder {
    let mut builder = Client::builder(TokioExecutor::new());
    builder
        .pool_timer(TokioTimer::new())
        .pool_idle_timeout(Duration::from_secs(60))
        .timer(TokioTimer::new())
        .http2_keep_alive_interval(Duration::from_secs(30))
        .http2_keep_alive_timeout(Duration::from_secs(10))
        .http2_keep_alive_while_idle(false);
    builder
}

/// Build the shared WebPKI TLS configuration once so `https://` and `wss://`
/// upstreams share root certificates while keeping `wss://` ALPN empty (strictly
/// HTTP/1.1 `Upgrade`) and using [`tokio_tungstenite::Connector::Plain`] on `ws://`.
pub(crate) fn build_connectors(ws_is_tls: bool) -> (Https, tokio_tungstenite::Connector) {
    use hyper_rustls::ConfigBuilderExt;

    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let base_tls = rustls::ClientConfig::builder()
        .with_webpki_roots()
        .with_no_client_auth();
    let ws_tls = if ws_is_tls {
        tokio_tungstenite::Connector::Rustls(Arc::new(base_tls.clone()))
    } else {
        tokio_tungstenite::Connector::Plain
    };
    let https = hyper_rustls::HttpsConnectorBuilder::new()
        .with_tls_config(base_tls)
        .https_or_http()
        // ALPN advertises ["h2","http/1.1"] for https upstreams -> auto-negotiate
        // h2 when the upstream offers it; cleartext upstreams stay h1.
        .enable_all_versions()
        .wrap_connector(build_http_connector());
    (https, ws_tls)
}

pub(crate) fn build_client(https: Https) -> ProxyClient {
    configure_client_builder().build(https)
}

/// Cleartext-only HTTP/2 prior-knowledge (`h2c`) client for probed `http://` EL upstreams,
/// sharing the same `Https` connector type so both clients unify as [`ProxyClient`].
pub(crate) fn build_h2c_client(https: Https) -> ProxyClient {
    let mut builder = configure_client_builder();
    builder.http2_only(true);
    builder.build(https)
}

/// Tracks the negotiated HTTP protocol version observed on an `https://` upstream.
#[derive(Default)]
pub(crate) struct ObservedHttpVersion(AtomicU8);

impl ObservedHttpVersion {
    const UNOBSERVED: u8 = 0;
    const HTTP_11: u8 = 1;
    const HTTP_2: u8 = 2;

    pub(crate) const fn new() -> Self {
        Self(AtomicU8::new(Self::UNOBSERVED))
    }

    pub(crate) fn record(&self, version: Version) {
        let encoded = if version == Version::HTTP_2 {
            Self::HTTP_2
        } else {
            Self::HTTP_11
        };
        if self.0.load(Ordering::Relaxed) != encoded {
            self.0.store(encoded, Ordering::Relaxed);
        }
    }

    pub(crate) fn is_h2(&self) -> bool {
        self.0.load(Ordering::Relaxed) == Self::HTTP_2
    }

    /// Machine-readable `/healthz` transport (`"h2"` once observed, conservative `"http/1.1"` otherwise).
    pub(crate) fn label(&self) -> &'static str {
        if self.is_h2() { "h2" } else { "http/1.1" }
    }

    /// Human-readable startup protocol summary (`"HTTPS (ALPN)"` before first TLS response).
    pub(crate) fn startup_label(&self) -> &'static str {
        match self.0.load(Ordering::Relaxed) {
            Self::HTTP_2 => "HTTP/2 (TLS)",
            Self::HTTP_11 => "HTTP/1.1 (TLS)",
            _ => "HTTPS (ALPN)",
        }
    }
}

/// Adaptive cleartext `h2c` state machine for probed `http://` EL upstreams.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum H2cMode {
    Initial = 0,
    Confirmed = 1,
    H1VerifyOnce = 2,
    H1Confirmed = 3,
}

impl H2cMode {
    pub(crate) fn load(atom: &AtomicU8) -> Self {
        match atom.load(Ordering::Relaxed) {
            0 => Self::Initial,
            1 => Self::Confirmed,
            2 => Self::H1VerifyOnce,
            _ => Self::H1Confirmed,
        }
    }

    pub(crate) fn transition(atom: &AtomicU8, cur: Self, next: Self) {
        if cur == next {
            return;
        }
        atom.store(next as u8, Ordering::Relaxed);
        match (cur, next) {
            (Self::Confirmed, Self::H1VerifyOnce) => {
                warn!("EL JSON-RPC upstream stopped speaking h2c — using HTTP/1.1");
            }
            (Self::H1VerifyOnce | Self::H1Confirmed, Self::Confirmed) => {
                info!("EL JSON-RPC upstream speaks h2c — using HTTP/2");
            }
            _ => {}
        }
    }
}

/// Unified transport tracker and client selector for EL and CL HTTP hops.
pub(crate) enum HttpTransport {
    /// Cleartext HTTP/1.1 only (CL cleartext, or EL `--trust-upstream` cleartext).
    CleartextH1,
    /// `https://` upstream: negotiates `h2` vs `http/1.1` via TLS ALPN on `state.client`.
    TlsAuto(ObservedHttpVersion),
    /// Probed cleartext `http://` EL upstream with a dedicated prior-knowledge `h2c` client
    /// and one-shot `h2c` recovery verification.
    AdaptiveH2c {
        mode: AtomicU8,
        client: Box<ProxyClient>,
    },
}

impl HttpTransport {
    pub(crate) const fn new_cl(is_tls: bool) -> Self {
        if is_tls {
            Self::TlsAuto(ObservedHttpVersion::new())
        } else {
            Self::CleartextH1
        }
    }

    pub(crate) fn new_el(trust_upstream: bool, is_tls: bool, https: Https) -> Self {
        if is_tls {
            Self::TlsAuto(ObservedHttpVersion::new())
        } else if trust_upstream {
            Self::CleartextH1
        } else {
            Self::AdaptiveH2c {
                mode: AtomicU8::new(H2cMode::Initial as u8),
                client: Box::new(build_h2c_client(https)),
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn set_confirmed_cleartext(&mut self, prefer_h2: bool) {
        if let Self::AdaptiveH2c { mode, .. } = self {
            mode.store(
                if prefer_h2 {
                    H2cMode::Confirmed as u8
                } else {
                    H2cMode::H1Confirmed as u8
                },
                Ordering::Relaxed,
            );
        }
    }

    /// Whether data-plane requests currently prefer the cleartext `h2c` prior-knowledge client.
    #[cfg(test)]
    pub(crate) fn uses_h2c_client(&self) -> bool {
        match self {
            Self::CleartextH1 | Self::TlsAuto(_) => false,
            Self::AdaptiveH2c { mode, .. } => {
                matches!(H2cMode::load(mode), H2cMode::Initial | H2cMode::Confirmed)
            }
        }
    }

    pub(crate) fn select_client<'a>(&'a self, default: &'a ProxyClient) -> &'a ProxyClient {
        match self {
            Self::AdaptiveH2c { mode, client }
                if matches!(H2cMode::load(mode), H2cMode::Initial | H2cMode::Confirmed) =>
            {
                client
            }
            Self::CleartextH1 | Self::TlsAuto(_) | Self::AdaptiveH2c { .. } => default,
        }
    }

    pub(crate) fn record_version(&self, version: Version) {
        if let Self::TlsAuto(obs) = self {
            obs.record(version);
        }
    }

    pub(crate) fn is_observed_h2(&self) -> bool {
        matches!(self, Self::TlsAuto(obs) if obs.is_h2())
    }

    /// Machine-readable transport label for `/healthz` (`"h2c"`, `"h2"`, or `"http/1.1"`).
    pub(crate) fn label(&self) -> &'static str {
        match self {
            Self::CleartextH1 => "http/1.1",
            Self::TlsAuto(obs) => obs.label(),
            Self::AdaptiveH2c { mode, .. } => match H2cMode::load(mode) {
                H2cMode::Initial | H2cMode::Confirmed => "h2c",
                H2cMode::H1VerifyOnce | H2cMode::H1Confirmed => "http/1.1",
            },
        }
    }

    /// Human-readable protocol summary for startup lifecycle logging.
    pub(crate) fn startup_proto_label(&self) -> &'static str {
        match self {
            Self::CleartextH1 => "HTTP/1.1",
            Self::TlsAuto(obs) => obs.startup_label(),
            Self::AdaptiveH2c { mode, .. } => match H2cMode::load(mode) {
                H2cMode::Initial => "HTTP/2 (h2c, unverified)",
                H2cMode::Confirmed => "HTTP/2 (h2c)",
                H2cMode::H1VerifyOnce | H2cMode::H1Confirmed => "HTTP/1.1",
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn observed_http_version_distinguishes_unobserved_startup_label() {
        let obs = ObservedHttpVersion::new();
        assert_eq!(obs.startup_label(), "HTTPS (ALPN)");
        assert_eq!(obs.label(), "http/1.1");

        obs.record(Version::HTTP_11);
        assert_eq!(obs.startup_label(), "HTTP/1.1 (TLS)");
        assert_eq!(obs.label(), "http/1.1");

        obs.record(Version::HTTP_2);
        assert_eq!(obs.startup_label(), "HTTP/2 (TLS)");
        assert_eq!(obs.label(), "h2");
    }
}
