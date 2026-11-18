//! Bounded upstream probe request construction, fetch, and structured error types.

use std::borrow::Cow;
use std::fmt;
use std::time::Duration;

use bytes::Bytes;
use http::{HeaderValue, Method, Request, Uri, header::CONTENT_TYPE};
use http_body_util::BodyExt;
use hyper::body::Body as _;

const MAX_HEALTH_BODY_BYTES: usize = 10 * 1024 * 1024;

use crate::response::{ResBody, empty_body, full_body};
use crate::transport::{HttpTarget, HttpTransport, ProxyClient};
use crate::upstream::HttpUpstream;

#[derive(Debug)]
#[cfg_attr(test, derive(Clone, PartialEq, Eq))]
pub(super) enum HealthError {
    Pending,
    Timeout,
    BodyTimeout,
    Transport(Box<str>),
    HttpStatus(http::StatusCode),
    Decode(Cow<'static, str>),
    Rpc(Box<str>),
}

impl HealthError {
    pub(super) fn decode(err: impl fmt::Display) -> Self {
        Self::Decode(Cow::Owned(err.to_string()))
    }
}

impl fmt::Display for HealthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pending => f.write_str("initial poll pending"),
            Self::Timeout => f.write_str("timeout"),
            Self::BodyTimeout => f.write_str("response body timeout"),
            Self::Transport(s) => write!(f, "transport: {s}"),
            Self::HttpStatus(status) => write!(f, "http {status}"),
            Self::Decode(s) => write!(f, "decode: {s}"),
            Self::Rpc(s) => write!(f, "rpc error: {s}"),
        }
    }
}

/// Single source of truth for local EL/CL probe method labels surfaced on `/healthz` and `/readyz`.
#[derive(Clone, Copy)]
pub(super) enum ProbeMethod {
    EthSyncing,
    NetPeerCount,
    EthGetBlockByNumber,
    ClSyncing,
    ClPeerCount,
}

impl ProbeMethod {
    const fn as_str(self) -> &'static str {
        match self {
            Self::EthSyncing => "eth_syncing",
            Self::NetPeerCount => "net_peerCount",
            Self::EthGetBlockByNumber => "eth_getBlockByNumber",
            Self::ClSyncing => "node/syncing",
            Self::ClPeerCount => "node/peer_count",
        }
    }
}

/// Lazily formatted `"<method>: <detail>"` error (or bare `"initial poll pending"`
/// before the first poll completes) so failed polls allocate zero method-prefix strings.
#[derive(Clone, Copy)]
pub(super) struct MethodError<'a> {
    method: ProbeMethod,
    err: &'a HealthError,
}

impl<'a> MethodError<'a> {
    pub(super) const fn new(method: ProbeMethod, err: &'a HealthError) -> Self {
        Self { method, err }
    }
}

impl fmt::Display for MethodError<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.err {
            HealthError::Pending => f.write_str("initial poll pending"),
            other => write!(f, "{}: {other}", self.method.as_str()),
        }
    }
}

/// A hard transport/connection failure or a timeout (vs. an HTTP status or decode
/// error) — the only signal that the chosen HTTP version might be wrong for this
/// upstream. A timeout is included so that if the upstream hangs on the HTTP/2
/// connection preface, we can fall back to HTTP/1.1 if the alternate client succeeds.
/// If both timeout, we do not switch.
const fn is_transport(e: &HealthError) -> bool {
    matches!(e, HealthError::Transport(_) | HealthError::Timeout)
}

/// Opaque borrowed transport and target-URI context shared by EL, CL, and reference health
/// probes, constructing and sending bounded HTTP requests so leaf probe modules stay
/// independent of [`crate::state::AppState`], [`HttpTransport`], and `http` request builders.
#[derive(Clone, Copy)]
pub(super) struct ProbeTarget<'a> {
    client: &'a ProxyClient,
    transport: &'a HttpTransport,
    uri: &'a Uri,
    timeout: Duration,
}

impl<'a> ProbeTarget<'a> {
    /// Target an explicit `uri` on `target` (e.g. Beacon endpoints whose path differs
    /// from the configured base URI).
    pub(super) const fn new(
        client: &'a ProxyClient,
        target: &'a HttpTarget,
        uri: &'a Uri,
        timeout: Duration,
    ) -> Self {
        Self {
            client,
            transport: &target.transport,
            uri,
            timeout,
        }
    }

    /// Target `target`'s own configured base URI (JSON-RPC EL probes).
    pub(super) const fn from_target(
        client: &'a ProxyClient,
        target: &'a HttpTarget,
        timeout: Duration,
    ) -> Self {
        Self::new(client, target, target.upstream.uri(), timeout)
    }

    /// Retarget the same client and timeout onto an untracked upstream (reference endpoints
    /// whose transport protocol is not surfaced on `/healthz`).
    pub(super) const fn with_upstream(self, upstream: &'a HttpUpstream) -> Self {
        Self {
            client: self.client,
            transport: HttpTransport::untracked(),
            uri: upstream.uri(),
            timeout: self.timeout,
        }
    }

    pub(super) async fn get_bytes(self) -> Result<Bytes, HealthError> {
        let req = self.request(Method::GET, empty_body());
        self.fetch_bytes(self.client, req).await
    }

    /// POST a JSON-RPC payload. An adaptive cleartext target uses its currently selected
    /// `h2c`/`h1` client and retries once over `self.client` (HTTP/1.1) on transport errors
    /// without mutating `H2cMode`, preserving the primary transport error if both fail;
    /// every other target is queried once.
    pub(super) async fn post_json(self, payload: &'static [u8]) -> Result<Bytes, HealthError> {
        self.transport
            .probe_with_fallback(self.client, is_transport, |client| {
                self.post_json_on(client, payload)
            })
            .await
    }

    /// POST a JSON-RPC batch payload while driving this target's adaptive `h2c` state machine.
    pub(super) async fn post_json_adaptive(
        self,
        payload: &'static [u8],
    ) -> Result<Bytes, HealthError> {
        self.transport
            .probe_adaptive(self.client, is_transport, |client| {
                self.post_json_on(client, payload)
            })
            .await
    }

    async fn post_json_on(
        self,
        client: &ProxyClient,
        payload: &'static [u8],
    ) -> Result<Bytes, HealthError> {
        let mut req = self.request(Method::POST, full_body(Bytes::from_static(payload)));
        req.headers_mut()
            .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        self.fetch_bytes(client, req).await
    }

    fn request(self, method: Method, body: ResBody) -> Request<ResBody> {
        let mut req = Request::new(body);
        *req.method_mut() = method;
        *req.uri_mut() = self.uri.clone();
        req
    }

    /// Send a prepared probe request within `self.timeout`, record any TLS
    /// negotiated protocol version as soon as response headers arrive, and return
    /// the body of a successful (`2xx`) response (capped at 10 MiB).
    ///
    /// Only header/connect timeouts map to [`HealthError::Timeout`] (eligible for
    /// adaptive `h2c` fallback); once response headers arrive, the HTTP protocol is
    /// proven valid so body collection timeouts map to [`HealthError::BodyTimeout`].
    async fn fetch_bytes(
        self,
        client: &ProxyClient,
        req: Request<ResBody>,
    ) -> Result<Bytes, HealthError> {
        let deadline = tokio::time::Instant::now() + self.timeout;
        let resp = tokio::time::timeout_at(deadline, self.transport.send(client, req))
            .await
            .map_err(|_| HealthError::Timeout)?
            .map_err(|e| HealthError::Transport(e.to_string().into_boxed_str()))?;
        if !resp.status().is_success() {
            return Err(HealthError::HttpStatus(resp.status()));
        }
        let body = resp.into_body();
        if body.size_hint().lower() > MAX_HEALTH_BODY_BYTES as u64 {
            return Err(HealthError::Decode(Cow::Borrowed("length limit exceeded")));
        }
        if body.is_end_stream() {
            return Ok(Bytes::new());
        }
        tokio::time::timeout_at(
            deadline,
            http_body_util::Limited::new(body, MAX_HEALTH_BODY_BYTES).collect(),
        )
        .await
        .map_err(|_| HealthError::BodyTimeout)?
        .map(|c| c.to_bytes())
        .map_err(HealthError::decode)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::{build_client, build_connectors};
    use http::{Response, StatusCode};
    use http_body_util::Full;
    use hyper::body::Incoming;
    use hyper::service::service_fn;
    use hyper_util::rt::TokioIo;
    use std::convert::Infallible;

    struct AbortOnDrop(tokio::task::JoinHandle<()>);
    impl Drop for AbortOnDrop {
        fn drop(&mut self) {
            self.0.abort();
        }
    }

    async fn serve_h1_once(
        stream: tokio::net::TcpStream,
        status: StatusCode,
        reply: &'static [u8],
    ) {
        let svc = service_fn(move |req: Request<Incoming>| async move {
            let _ = req.into_body().collect().await;
            Ok::<_, Infallible>(
                Response::builder()
                    .status(status)
                    .body(Full::new(Bytes::from_static(reply)))
                    .unwrap(),
            )
        });
        let _ = hyper::server::conn::http1::Builder::new()
            .keep_alive(false)
            .serve_connection(TokioIo::new(stream), svc)
            .await;
    }

    #[tokio::test]
    async fn reference_probes_leave_the_local_transport_untouched() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let reference = HttpUpstream::parse(
            "--el-reference-url",
            &format!("http://{}", listener.local_addr().unwrap()),
        )
        .unwrap();
        let _server = AbortOnDrop(tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                tokio::spawn(serve_h1_once(stream, StatusCode::OK, b"{}"));
            }
        }));
        let client = build_client(build_connectors(false).0);
        // A TLS local target that has not answered yet: any response recorded against it
        // would move its label off "HTTPS (ALPN)".
        let local = HttpTarget::new_standard(
            HttpUpstream::parse("--el-http-url", "https://127.0.0.1:1").unwrap(),
        );
        let el = ProbeTarget::from_target(&client, &local, Duration::from_secs(1));

        let body = el.with_upstream(&reference).post_json(b"{}").await;
        assert_eq!(body.unwrap().as_ref(), b"{}");
        assert_eq!(local.transport.startup_proto_label(), "HTTPS (ALPN)");
    }

    #[tokio::test]
    async fn adaptive_post_json_falls_back_without_mutating_mode_and_preserves_primary_error() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicU8, Ordering};

        // 0 = H1 200 OK, 1 = H1 500 error, 2 = H1 silent timeout; h2c always closes on preface.
        let h1_mode = Arc::new(AtomicU8::new(0));
        let mode_srv = h1_mode.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let _server = AbortOnDrop(tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((stream, _)) = listener.accept().await {
                let mut preface = [0; 3];
                if stream.peek(&mut preface).await.unwrap_or(0) == 3 && &preface == b"PRI" {
                    drop(stream);
                    continue;
                }
                match mode_srv.load(Ordering::Relaxed) {
                    0 => {
                        tokio::spawn(serve_h1_once(stream, StatusCode::OK, b"{}"));
                    }
                    1 => {
                        tokio::spawn(serve_h1_once(
                            stream,
                            StatusCode::INTERNAL_SERVER_ERROR,
                            b"",
                        ));
                    }
                    _ => held.push(stream),
                }
            }
        }));

        let (https, _) = build_connectors(false);
        let target = HttpTarget::new_el(
            HttpUpstream::parse("--el-http-url", &format!("http://{addr}")).unwrap(),
            false,
            &https,
        );
        let client = build_client(https);
        let el = ProbeTarget::from_target(&client, &target, Duration::from_millis(120));

        assert_eq!(el.post_json(b"{}").await.unwrap().as_ref(), b"{}");
        assert!(target.transport.uses_h2c_client());

        h1_mode.store(1, Ordering::Relaxed);
        assert_eq!(
            el.post_json(b"{}").await.unwrap_err(),
            HealthError::HttpStatus(http::StatusCode::INTERNAL_SERVER_ERROR)
        );
        assert!(target.transport.uses_h2c_client());

        // Primary h2c fails immediately with Transport while H1 fallback times out:
        // preserve primary's Transport error (aligned with probe_adaptive) without mutating mode.
        h1_mode.store(2, Ordering::Relaxed);
        assert!(matches!(
            el.post_json(b"{}").await.unwrap_err(),
            HealthError::Transport(_)
        ));
        assert!(target.transport.uses_h2c_client());
    }
}
