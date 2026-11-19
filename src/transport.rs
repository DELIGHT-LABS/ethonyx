//! Upstream HTTP/TLS client builders and negotiated protocol tracking.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Duration;

use http::{Request, Response, Version};
use hyper::body::Incoming;
use hyper_util::client::legacy::{Client, connect::HttpConnector};
use hyper_util::rt::{TokioExecutor, TokioTimer};
use tokio::time::Instant;
use tracing::{debug, warn};

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

/// Executor for OTLP export clients that detaches every spawned HTTP/1.1 or HTTP/2
/// connection driver from the active `tracing` subscriber so `h2`/`hyper` spans
/// cannot recursively re-enter the OpenTelemetry trace pipeline.
#[cfg(feature = "otel")]
#[derive(Clone, Copy)]
pub(crate) struct NoSubscriberExecutor;

#[cfg(feature = "otel")]
impl<Fut> hyper::rt::Executor<Fut> for NoSubscriberExecutor
where
    Fut: Future + Send + 'static,
    Fut::Output: Send + 'static,
{
    fn execute(&self, fut: Fut) {
        use tracing::instrument::WithSubscriber as _;
        tokio::spawn(fut.with_subscriber(tracing::dispatcher::Dispatch::none()));
    }
}

fn configure_client_builder(
    mut builder: hyper_util::client::legacy::Builder,
) -> hyper_util::client::legacy::Builder {
    builder
        .pool_timer(TokioTimer::new())
        .pool_idle_timeout(Duration::from_secs(60))
        .timer(TokioTimer::new())
        .http2_keep_alive_interval(Duration::from_secs(30))
        .http2_keep_alive_timeout(Duration::from_secs(10))
        .http2_keep_alive_while_idle(false);
    builder
}

fn build_tls_config() -> rustls::ClientConfig {
    use hyper_rustls::ConfigBuilderExt;

    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    rustls::ClientConfig::builder()
        .with_webpki_roots()
        .with_no_client_auth()
}

fn wrap_https_connector(tls: rustls::ClientConfig) -> Https {
    hyper_rustls::HttpsConnectorBuilder::new()
        .with_tls_config(tls)
        .https_or_http()
        // ALPN advertises ["h2","http/1.1"] for https upstreams -> auto-negotiate
        // h2 when the upstream offers it; cleartext upstreams stay h1.
        .enable_all_versions()
        .wrap_connector(build_http_connector())
}

/// Build an HTTPS-capable HTTP/1.1+HTTP/2 connector using WebPKI roots.
#[cfg(test)]
pub(crate) fn build_https_connector() -> Https {
    wrap_https_connector(build_tls_config())
}

/// Build one WebPKI TLS configuration per server instance so its `https://` and
/// `wss://` upstreams share root certificates and one session-resumption store, while
/// `wss://` keeps ALPN empty (strictly HTTP/1.1 `Upgrade`) and `ws://` uses
/// [`tokio_tungstenite::Connector::Plain`].
pub(crate) fn build_connectors(ws_is_tls: bool) -> (Https, tokio_tungstenite::Connector) {
    let base_tls = build_tls_config();
    let ws_tls = if ws_is_tls {
        tokio_tungstenite::Connector::Rustls(Arc::new(base_tls.clone()))
    } else {
        tokio_tungstenite::Connector::Plain
    };
    (wrap_https_connector(base_tls), ws_tls)
}

pub(crate) fn build_client(https: Https) -> ProxyClient {
    configure_client_builder(Client::builder(TokioExecutor::new())).build(https)
}

#[cfg(test)]
pub(crate) fn build_https_client() -> ProxyClient {
    build_client(wrap_https_connector(build_tls_config()))
}

/// Build a WebPKI HTTPS client for OTLP export whose connection-driver tasks run
/// with [`tracing::dispatcher::Dispatch::none`].
#[cfg(feature = "otel")]
pub(crate) fn build_otlp_client() -> ProxyClient {
    configure_client_builder(Client::builder(NoSubscriberExecutor))
        .build(wrap_https_connector(build_tls_config()))
}

/// Cleartext-only HTTP/2 prior-knowledge (`h2c`) client for probed `http://` EL upstreams,
/// sharing the same `Https` connector type so both clients unify as [`ProxyClient`].
fn build_h2c_client(https: Https) -> ProxyClient {
    let mut builder = configure_client_builder(Client::builder(TokioExecutor::new()));
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

/// Adaptive cleartext `h2c` state machine for probed `http://` EL upstreams,
/// symmetric with [`ObservedHttpVersion`]. Data-plane requests and `/healthz`
/// conservatively stay on HTTP/1.1 until `h2c` is confirmed.
struct H2cState(AtomicU8);

impl H2cState {
    const UNPROBED: u8 = 0;
    const H2C: u8 = 1;
    const H1: u8 = 2;

    const fn new() -> Self {
        Self(AtomicU8::new(Self::UNPROBED))
    }

    fn load(&self) -> u8 {
        self.0.load(Ordering::Relaxed)
    }

    fn prefers_h2c(&self) -> bool {
        self.load() == Self::H2C
    }

    fn transition(&self, cur: u8, next: u8) {
        if cur == next {
            return;
        }
        self.0.store(next, Ordering::Relaxed);
        match (cur, next) {
            (Self::H2C, Self::H1) => {
                warn!("EL JSON-RPC upstream stopped speaking h2c — using HTTP/1.1");
            }
            (Self::UNPROBED, Self::H2C) => {
                debug!("EL JSON-RPC upstream speaks h2c — using HTTP/2");
            }
            _ => {}
        }
    }

    fn label(&self) -> &'static str {
        if self.prefers_h2c() {
            "h2c"
        } else {
            "http/1.1"
        }
    }

    fn startup_label(&self) -> &'static str {
        match self.load() {
            Self::UNPROBED => "HTTP/1.1 (unverified)",
            Self::H2C => "HTTP/2 (h2c)",
            _ => "HTTP/1.1",
        }
    }

    #[cfg(test)]
    fn set_confirmed(&self, prefer_h2: bool) {
        self.0.store(
            if prefer_h2 { Self::H2C } else { Self::H1 },
            Ordering::Relaxed,
        );
    }
}

/// Whether a health probe drives adaptive cleartext `h2c` state transitions.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProbeMode {
    Fixed,
    Adaptive,
}

/// Header-stage failure returned by [`HttpTransport::send_probe`].
/// Only header/connect errors are eligible for protocol fallback; once headers arrive,
/// status codes and body collection run outside `transport`.
pub(crate) enum ProbeTransportError {
    Timeout,
    Transport(hyper_util::client::legacy::Error),
}

enum TransportKind {
    /// Direct `state.client` dispatch with no version tracking (cleartext HTTP/1.1
    /// upstreams, `--trust-upstream` cleartext EL, untracked MEV relay, and reference probes).
    Direct,
    /// `https://` upstream: negotiates `h2` vs `http/1.1` via TLS ALPN on `state.client`.
    TlsAuto(ObservedHttpVersion),
    /// Probed cleartext `http://` EL upstream with a dedicated prior-knowledge `h2c` client.
    AdaptiveH2c {
        state: H2cState,
        client: Box<ProxyClient>,
    },
}

/// Opaque transport tracker, adaptive `h2c` state machine, and client selector for EL and CL hops.
pub(crate) struct HttpTransport(TransportKind);

/// Shared untracked transport for reference health probes whose protocol is not surfaced on `/healthz`.
pub(crate) static UNTRACKED_TRANSPORT: HttpTransport = HttpTransport::new_untracked();

impl HttpTransport {
    const fn new_untracked() -> Self {
        Self(TransportKind::Direct)
    }

    const fn new_standard(is_tls: bool) -> Self {
        Self(if is_tls {
            TransportKind::TlsAuto(ObservedHttpVersion::new())
        } else {
            TransportKind::Direct
        })
    }

    fn new_el(trust_upstream: bool, is_tls: bool, https: &Https) -> Self {
        if is_tls || trust_upstream {
            Self::new_standard(is_tls)
        } else {
            Self(TransportKind::AdaptiveH2c {
                state: H2cState::new(),
                client: Box::new(build_h2c_client(https.clone())),
            })
        }
    }

    #[cfg(test)]
    pub(crate) fn set_confirmed_cleartext(&self, prefer_h2: bool) {
        if let TransportKind::AdaptiveH2c { state, .. } = &self.0 {
            state.set_confirmed(prefer_h2);
        }
    }

    /// Whether data-plane requests currently prefer the cleartext `h2c` prior-knowledge client.
    #[cfg(test)]
    pub(crate) fn uses_h2c_client(&self) -> bool {
        match &self.0 {
            TransportKind::Direct | TransportKind::TlsAuto(_) => false,
            TransportKind::AdaptiveH2c { state, .. } => state.prefers_h2c(),
        }
    }

    fn primary_client<'a>(&'a self, default: &'a ProxyClient) -> &'a ProxyClient {
        match &self.0 {
            TransportKind::AdaptiveH2c { state, client } if state.prefers_h2c() => client,
            TransportKind::Direct
            | TransportKind::TlsAuto(_)
            | TransportKind::AdaptiveH2c { .. } => default,
        }
    }

    /// Send `req` over an explicit `client`, recording the protocol the hop negotiated.
    ///
    /// Calls `client.request(req)` eagerly so `Request<ResBody>` is moved into the client
    /// before `.await` instead of bloating the returned future, and captures only `&'a self`.
    fn send<'a>(
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

    async fn send_once(
        &self,
        client: &ProxyClient,
        req: Request<ResBody>,
        deadline: Instant,
    ) -> Result<(Response<Incoming>, Instant), ProbeTransportError> {
        let resp = tokio::time::timeout_at(deadline, self.send(client, req))
            .await
            .map_err(|_| ProbeTransportError::Timeout)?
            .map_err(ProbeTransportError::Transport)?;
        Ok((resp, deadline))
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

    /// Retry a confirmed `h2c` probe once on a fast non-timeout failure (e.g., a stale
    /// pooled socket evicted by Hyper) within the same header deadline; timeouts return
    /// immediately in 1× timeout without doubling outage detection latency.
    async fn send_h2c_with_stale_retry(
        &self,
        h2c: &ProxyClient,
        deadline: Instant,
        make_req: &impl Fn() -> Request<ResBody>,
    ) -> Result<(Response<Incoming>, Instant), ProbeTransportError> {
        match self.send_once(h2c, make_req(), deadline).await {
            Ok(ok) => Ok(ok),
            Err(ProbeTransportError::Timeout) => Err(ProbeTransportError::Timeout),
            Err(_) => self.send_once(h2c, make_req(), deadline).await,
        }
    }

    /// Execute one health probe header round-trip within `timeout` (when an `h2c` probe
    /// falls back to HTTP/1.1, the fallback attempt gets its own `timeout`), driving
    /// adaptive `h2c` detection under [`ProbeMode::Adaptive`] while confining fallback
    /// strictly to header/transport failures (never HTTP status codes or body collection).
    pub(crate) async fn send_probe(
        &self,
        default: &ProxyClient,
        timeout: Duration,
        mode: ProbeMode,
        make_req: impl Fn() -> Request<ResBody>,
    ) -> Result<(Response<Incoming>, Instant), ProbeTransportError> {
        let TransportKind::AdaptiveH2c { state, client } = &self.0 else {
            return self
                .send_once(default, make_req(), Instant::now() + timeout)
                .await;
        };
        let cur = state.load();
        let first_deadline = Instant::now() + timeout;
        let primary_err = match (cur, mode) {
            (H2cState::UNPROBED, ProbeMode::Adaptive) => {
                match self.send_once(client, make_req(), first_deadline).await {
                    Ok(ok) => {
                        state.transition(cur, H2cState::H2C);
                        return Ok(ok);
                    }
                    // A HangH2c server may burn the full `timeout` on the preface, so fall
                    // through to give the one-time HTTP/1.1 fallback its own full header budget.
                    Err(err) => err,
                }
            }
            (H2cState::H2C, _) => {
                match self
                    .send_h2c_with_stale_retry(client, first_deadline, &make_req)
                    .await
                {
                    Ok(ok) => return Ok(ok),
                    Err(ProbeTransportError::Timeout) => return Err(ProbeTransportError::Timeout),
                    Err(err) => err,
                }
            }
            // Settled HTTP/1.1 never re-probes h2c (keeps outage detection at 1x timeout);
            // fixed callers (reference local reads) also stay on HTTP/1.1 while Unprobed
            // to avoid racing `el_batch_detect` with duplicate h2c probes during warm-up.
            _ => return self.send_once(default, make_req(), first_deadline).await,
        };
        match self
            .send_once(default, make_req(), Instant::now() + timeout)
            .await
        {
            Ok(ok) => {
                if mode == ProbeMode::Adaptive {
                    state.transition(cur, H2cState::H1);
                }
                Ok(ok)
            }
            Err(_) => Err(primary_err),
        }
    }

    /// Machine-readable transport label for `/healthz` (`"h2c"`, `"h2"`, or `"http/1.1"`).
    pub(crate) fn label(&self) -> &'static str {
        match &self.0 {
            TransportKind::Direct => "http/1.1",
            TransportKind::TlsAuto(obs) => obs.label(),
            TransportKind::AdaptiveH2c { state, .. } => state.label(),
        }
    }

    /// Human-readable protocol summary for startup lifecycle logging.
    pub(crate) fn startup_proto_label(&self) -> &'static str {
        match &self.0 {
            TransportKind::Direct => "HTTP/1.1",
            TransportKind::TlsAuto(obs) => obs.startup_label(),
            TransportKind::AdaptiveH2c { state, .. } => state.startup_label(),
        }
    }
}

pub(crate) struct HttpTarget {
    pub(crate) upstream: HttpUpstream,
    pub(crate) transport: HttpTransport,
}

impl HttpTarget {
    pub(crate) const fn new_untracked(upstream: HttpUpstream) -> Self {
        Self {
            upstream,
            transport: HttpTransport::new_untracked(),
        }
    }

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
        // `TlsAuto` inspects only the response version, so a cleartext HTTP/1.1 peer suffices.
        let (addr, _server) = crate::test_util::spawn_http_mock(|_| async {
            Response::new(crate::response::empty_body())
        })
        .await;
        let uri: http::Uri = format!("http://{addr}/").parse().unwrap();
        let client = build_https_client();
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
    }

    #[test]
    fn h2c_mode_is_conservative_until_confirmed() {
        let https = build_https_connector();
        let el = HttpTransport::new_el(false, false, &https);
        assert!(!el.uses_h2c_client());
        assert_eq!(el.label(), "http/1.1");
        assert_eq!(el.startup_proto_label(), "HTTP/1.1 (unverified)");

        el.set_confirmed_cleartext(true);
        assert!(el.uses_h2c_client());
        assert_eq!(el.label(), "h2c");
        assert_eq!(el.startup_proto_label(), "HTTP/2 (h2c)");

        el.set_confirmed_cleartext(false);
        assert!(!el.uses_h2c_client());
        assert_eq!(el.label(), "http/1.1");
        assert_eq!(el.startup_proto_label(), "HTTP/1.1");
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
