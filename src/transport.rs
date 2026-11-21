//! Upstream HTTP/TLS client builders and negotiated protocol tracking.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Duration;

use http::{Request, Response, Version};
use hyper::body::Incoming;
use hyper_util::client::legacy::{Client, connect::HttpConnector};
use hyper_util::rt::{TokioExecutor, TokioTimer};
use tracing::{debug, info, warn};

use crate::response::ResBody;
use crate::upstream::{HttpUpstream, WsUpstream};

const TCP_KEEPALIVE_TIME: Duration = Duration::from_secs(60);
const TCP_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);

pub(crate) type Https = hyper_rustls::HttpsConnector<HttpConnector>;
pub(crate) type ProxyClient = Client<Https, ResBody>;

pub(crate) fn configure_tcp_stream(stream: &tokio::net::TcpStream) {
    if let Err(e) = stream.set_nodelay(true) {
        debug!(error = %e, "failed to set TCP_NODELAY");
    }
    let sock_ref = socket2::SockRef::from(stream);
    let keepalive = socket2::TcpKeepalive::new()
        .with_time(TCP_KEEPALIVE_TIME)
        .with_interval(TCP_KEEPALIVE_INTERVAL);
    if let Err(e) = sock_ref.set_tcp_keepalive(&keepalive) {
        debug!(error = %e, "failed to set TCP keepalive");
    }
}

fn build_http_connector() -> HttpConnector {
    let mut http = HttpConnector::new();
    http.set_nodelay(true);
    http.enforce_http(false);
    http.set_keepalive(Some(TCP_KEEPALIVE_TIME));
    http.set_keepalive_interval(Some(TCP_KEEPALIVE_INTERVAL));
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

/// Build one WebPKI TLS configuration per server instance so its `https://` and
/// `wss://` upstreams share root certificates and one session-resumption store, while
/// `wss://` keeps ALPN empty (strictly HTTP/1.1 `Upgrade`) and `ws://` uses
/// [`tokio_tungstenite::Connector::Plain`].
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
struct ObservedHttpVersion(AtomicU8);

impl ObservedHttpVersion {
    const UNOBSERVED: u8 = 0;
    const HTTP_11: u8 = 1;
    const HTTP_2: u8 = 2;

    const fn new() -> Self {
        Self(AtomicU8::new(Self::UNOBSERVED))
    }

    fn record(&self, version: Version) {
        let encoded = if version == Version::HTTP_2 {
            Self::HTTP_2
        } else {
            Self::HTTP_11
        };
        if self.0.load(Ordering::Relaxed) != encoded {
            self.0.store(encoded, Ordering::Relaxed);
        }
    }

    fn is_h2(&self) -> bool {
        self.0.load(Ordering::Relaxed) == Self::HTTP_2
    }

    /// Machine-readable `/healthz` transport (`"h2"` once observed, conservative `"http/1.1"` otherwise).
    fn label(&self) -> &'static str {
        if self.is_h2() { "h2" } else { "http/1.1" }
    }

    /// Human-readable startup protocol summary (`"HTTPS (ALPN)"` before first TLS response).
    fn startup_label(&self) -> &'static str {
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
enum H2cMode {
    Initial = 0,
    Confirmed = 1,
    H1VerifyOnce = 2,
    H1Confirmed = 3,
}

impl H2cMode {
    const fn prefers_h2c(self) -> bool {
        matches!(self, Self::Initial | Self::Confirmed)
    }

    fn load(atom: &AtomicU8) -> Self {
        match atom.load(Ordering::Relaxed) {
            0 => Self::Initial,
            1 => Self::Confirmed,
            2 => Self::H1VerifyOnce,
            _ => Self::H1Confirmed,
        }
    }

    fn transition(atom: &AtomicU8, cur: Self, next: Self) {
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

enum Reached {
    First,
    Second,
    Neither,
}

async fn probe_pair<'a, T, E, F, Fut>(
    first: &'a ProxyClient,
    second: &'a ProxyClient,
    prefer_second_err: bool,
    is_transport_err: impl Fn(&E) -> bool,
    mut send: F,
) -> (Result<T, E>, Reached)
where
    F: FnMut(&'a ProxyClient) -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    match send(first).await {
        Err(first_err) if is_transport_err(&first_err) => match send(second).await {
            Err(second_err) if is_transport_err(&second_err) => (
                Err(if prefer_second_err {
                    second_err
                } else {
                    first_err
                }),
                Reached::Neither,
            ),
            reached => (reached, Reached::Second),
        },
        reached => (reached, Reached::First),
    }
}

enum TransportKind {
    /// Cleartext HTTP/1.1 only (CL cleartext, or EL `--trust-upstream` cleartext).
    /// Also reused by [`HttpTransport::untracked`] for endpoints (including `https://`
    /// reference RPCs) whose negotiated version is never surfaced on `/healthz`.
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

/// Opaque transport tracker, adaptive `h2c` state machine, and client selector for EL and CL hops.
pub(crate) struct HttpTransport(TransportKind);

impl HttpTransport {
    /// Shared untracked transport for endpoints (such as reference RPCs) whose
    /// negotiated HTTP version is not surfaced on `/healthz`. Uses `default`
    /// directly without adaptive `h2c` probing or TLS ALPN observation; never
    /// expose `.label()` on this instance.
    pub(crate) const fn untracked() -> &'static Self {
        const { &Self(TransportKind::CleartextH1) }
    }

    const fn new_standard(is_tls: bool) -> Self {
        Self(if is_tls {
            TransportKind::TlsAuto(ObservedHttpVersion::new())
        } else {
            TransportKind::CleartextH1
        })
    }

    fn new_el(trust_upstream: bool, is_tls: bool, https: &Https) -> Self {
        if is_tls || trust_upstream {
            Self::new_standard(is_tls)
        } else {
            Self(TransportKind::AdaptiveH2c {
                mode: AtomicU8::new(H2cMode::Initial as u8),
                client: Box::new(build_h2c_client(https.clone())),
            })
        }
    }

    #[cfg(test)]
    pub(crate) fn set_confirmed_cleartext(&self, prefer_h2: bool) {
        if let TransportKind::AdaptiveH2c { mode, .. } = &self.0 {
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
        match &self.0 {
            TransportKind::CleartextH1 | TransportKind::TlsAuto(_) => false,
            TransportKind::AdaptiveH2c { mode, .. } => H2cMode::load(mode).prefers_h2c(),
        }
    }

    fn primary_client<'a>(&'a self, default: &'a ProxyClient) -> &'a ProxyClient {
        match &self.0 {
            TransportKind::AdaptiveH2c { mode, client } if H2cMode::load(mode).prefers_h2c() => {
                client
            }
            TransportKind::CleartextH1
            | TransportKind::TlsAuto(_)
            | TransportKind::AdaptiveH2c { .. } => default,
        }
    }

    /// Send `req` over an explicit `client`, recording the protocol the hop negotiated.
    ///
    /// Calls `client.request(req)` eagerly so `Request<ResBody>` is moved into the client
    /// before `.await` instead of bloating the returned future, and captures only `&'a self`.
    pub(crate) fn send<'a>(
        &'a self,
        client: &ProxyClient,
        req: Request<ResBody>,
    ) -> impl Future<Output = Result<Response<Incoming>, hyper_util::client::legacy::Error>> + use<'a>
    {
        let fut = client.request(req);
        async move {
            let resp = fut.await?;
            if let TransportKind::TlsAuto(obs) = &self.0 {
                obs.record(resp.version());
            }
            Ok(resp)
        }
    }

    /// Send `req` over this transport's currently selected client, recording any TLS ALPN version.
    pub(crate) fn request<'a>(
        &'a self,
        default: &ProxyClient,
        req: Request<ResBody>,
    ) -> impl Future<Output = Result<Response<Incoming>, hyper_util::client::legacy::Error>> + use<'a>
    {
        self.send(self.primary_client(default), req)
    }

    /// Execute one read-only probe over the preferred client, falling back once to
    /// `default` (HTTP/1.1) on transport errors when adaptive `h2c` is active without
    /// mutating `H2cMode`, preserving the primary transport error if both fail.
    pub(crate) async fn probe_with_fallback<'a, T, E, F, Fut>(
        &'a self,
        default: &'a ProxyClient,
        is_transport_err: impl Fn(&E) -> bool,
        mut send: F,
    ) -> Result<T, E>
    where
        F: FnMut(&'a ProxyClient) -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        match &self.0 {
            TransportKind::AdaptiveH2c { mode, client } if H2cMode::load(mode).prefers_h2c() => {
                probe_pair(client, default, false, is_transport_err, send)
                    .await
                    .0
            }
            TransportKind::CleartextH1
            | TransportKind::TlsAuto(_)
            | TransportKind::AdaptiveH2c { .. } => send(default).await,
        }
    }

    /// Execute one health probe, driving the adaptive `h2c` <-> HTTP/1.1 state machine
    /// when applicable while keeping `H2cMode` encapsulated inside `transport`.
    pub(crate) async fn probe_adaptive<'a, T, E, F, Fut>(
        &'a self,
        default: &'a ProxyClient,
        is_transport_err: impl Fn(&E) -> bool,
        mut send: F,
    ) -> Result<T, E>
    where
        F: FnMut(&'a ProxyClient) -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        let TransportKind::AdaptiveH2c { mode, client } = &self.0 else {
            return send(default).await;
        };
        let cur = H2cMode::load(mode);
        let (first, second, on_first, on_second) = match cur {
            H2cMode::Initial | H2cMode::Confirmed => (
                &**client,
                default,
                H2cMode::Confirmed,
                H2cMode::H1VerifyOnce,
            ),
            // Verify h2c one more time to self-heal if an h2c upstream was
            // probed mid-bind during startup or restart.
            H2cMode::H1VerifyOnce => (&**client, default, H2cMode::Confirmed, H2cMode::H1Confirmed),
            H2cMode::H1Confirmed => (default, &**client, H2cMode::H1Confirmed, H2cMode::Confirmed),
        };
        let (res, reached) = probe_pair(
            first,
            second,
            cur == H2cMode::H1VerifyOnce,
            is_transport_err,
            send,
        )
        .await;
        match reached {
            Reached::First => H2cMode::transition(mode, cur, on_first),
            Reached::Second => H2cMode::transition(mode, cur, on_second),
            Reached::Neither => {}
        }
        res
    }

    /// Machine-readable transport label for `/healthz` (`"h2c"`, `"h2"`, or `"http/1.1"`).
    pub(crate) fn label(&self) -> &'static str {
        match &self.0 {
            TransportKind::CleartextH1 => "http/1.1",
            TransportKind::TlsAuto(obs) => obs.label(),
            TransportKind::AdaptiveH2c { mode, .. } => {
                if H2cMode::load(mode).prefers_h2c() {
                    "h2c"
                } else {
                    "http/1.1"
                }
            }
        }
    }

    /// Human-readable protocol summary for startup lifecycle logging.
    pub(crate) fn startup_proto_label(&self) -> &'static str {
        match &self.0 {
            TransportKind::CleartextH1 => "HTTP/1.1",
            TransportKind::TlsAuto(obs) => obs.startup_label(),
            TransportKind::AdaptiveH2c { mode, .. } => match H2cMode::load(mode) {
                H2cMode::Initial => "HTTP/2 (h2c, unverified)",
                H2cMode::Confirmed => "HTTP/2 (h2c)",
                H2cMode::H1VerifyOnce | H2cMode::H1Confirmed => "HTTP/1.1",
            },
        }
    }
}

pub(crate) struct HttpTarget {
    pub(crate) upstream: HttpUpstream,
    pub(crate) transport: HttpTransport,
}

impl HttpTarget {
    pub(crate) const fn new_standard(upstream: HttpUpstream) -> Self {
        let transport = HttpTransport::new_standard(upstream.is_tls());
        Self {
            upstream,
            transport,
        }
    }

    pub(crate) fn new_el(upstream: HttpUpstream, trust_upstream: bool, https: &Https) -> Self {
        let transport = HttpTransport::new_el(trust_upstream, upstream.is_tls(), https);
        Self {
            upstream,
            transport,
        }
    }
}

pub(crate) struct WsTarget {
    pub(crate) upstream: WsUpstream,
    pub(crate) tls: tokio_tungstenite::Connector,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn send_records_the_negotiated_version_for_tls_targets_only() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        // `TlsAuto` inspects only the response version, so a cleartext HTTP/1.1 peer suffices.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let uri: http::Uri = format!("http://{}/", listener.local_addr().unwrap())
            .parse()
            .unwrap();
        let server = tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let mut head = Vec::new();
                let mut buf = [0u8; 256];
                while !head.windows(4).any(|w| w == b"\r\n\r\n") {
                    let Ok(n) = stream.read(&mut buf).await else {
                        break;
                    };
                    if n == 0 {
                        break;
                    }
                    head.extend_from_slice(&buf[..n]);
                }
                let _ = stream
                    .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\nconnection: close\r\n\r\n")
                    .await;
                let _ = stream.shutdown().await;
            }
        });
        let client = build_client(build_connectors(false).0);
        let request = || {
            let mut req = Request::new(crate::response::empty_body());
            *req.uri_mut() = uri.clone();
            req
        };

        let tls = HttpTransport::new_standard(true);
        assert_eq!(tls.startup_proto_label(), "HTTPS (ALPN)");
        tls.request(&client, request()).await.unwrap();
        assert_eq!(tls.startup_proto_label(), "HTTP/1.1 (TLS)");

        let cleartext = HttpTransport::new_standard(false);
        cleartext.request(&client, request()).await.unwrap();
        assert_eq!(cleartext.startup_proto_label(), "HTTP/1.1");
        server.abort();
    }

    #[test]
    fn wss_connector_advertises_no_alpn_and_ws_stays_plain() {
        let (_, wss) = build_connectors(true);
        let tokio_tungstenite::Connector::Rustls(config) = wss else {
            panic!("wss:// needs a rustls connector");
        };
        assert!(config.alpn_protocols.is_empty());
        assert!(matches!(
            build_connectors(false).1,
            tokio_tungstenite::Connector::Plain
        ));
    }

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
