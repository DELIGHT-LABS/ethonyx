//! Bounded upstream probe request construction, fetch, and structured error types.

use std::borrow::Cow;
use std::fmt;
use std::time::Duration;

use bytes::Bytes;
use http::{HeaderValue, Method, Request, Uri, header::CONTENT_TYPE};
use http_body_util::BodyExt;
use hyper::body::Body as _;

use crate::response::{ResBody, empty_body, full_body};
use crate::transport::{
    HttpTarget, HttpTransport, ProbeMode, ProbeTransportError, ProxyClient, UNTRACKED_TRANSPORT,
};
use crate::upstream::HttpUpstream;

/// Local EL health batch includes `eth_getBlockByNumber("latest", false)` (tx hashes).
const MAX_EL_BATCH_BODY_BYTES: usize = 10 * 1024 * 1024;
/// CL Beacon (`/eth/v1/node/{syncing,peer_count}`) and Reference EL (`chainId`/`blockNumber`)
/// payloads are ~100–250 bytes; keep untrusted/external endpoint memory tightly bounded.
const MAX_SMALL_PROBE_BODY_BYTES: usize = 64 * 1024;
const MAX_NON_2XX_DRAIN_BYTES: usize = 8 * 1024;
const MAX_NON_2XX_DRAIN_TIMEOUT: Duration = Duration::from_millis(250);
const MAX_RPC_ERROR_BYTES: usize = 512;

/// Format `val` into a bounded `String` (at most `MAX_RPC_ERROR_BYTES + 3` bytes),
/// stopping formatting early on a UTF-8 boundary with `"..."` so untrusted upstream
/// payloads or verbose errors never allocate unbounded intermediate strings.
fn format_truncated(val: impl fmt::Display) -> String {
    struct Bounded {
        buf: String,
        truncated: bool,
    }

    impl fmt::Write for Bounded {
        fn write_str(&mut self, s: &str) -> fmt::Result {
            if self.truncated {
                return Err(fmt::Error);
            }
            let rem = MAX_RPC_ERROR_BYTES.saturating_sub(self.buf.len());
            if s.len() <= rem {
                self.buf.push_str(s);
                Ok(())
            } else {
                let cut = s.floor_char_boundary(rem);
                self.buf.reserve(cut + 3);
                self.buf.push_str(&s[..cut]);
                self.buf.push_str("...");
                self.truncated = true;
                Err(fmt::Error)
            }
        }
    }

    let mut out = Bounded {
        buf: String::new(),
        truncated: false,
    };
    let _ = fmt::write(&mut out, format_args!("{val}"));
    out.buf
}

#[derive(Debug)]
#[cfg_attr(test, derive(Clone, PartialEq, Eq))]
pub(super) enum HealthError {
    Timeout,
    BodyTimeout,
    Transport(String),
    HttpStatus(http::StatusCode),
    Decode(Cow<'static, str>),
    Rpc(String),
}

impl HealthError {
    pub(super) fn transport(err: impl fmt::Display) -> Self {
        Self::Transport(format_truncated(err))
    }

    /// Cap formatted decode errors so untrusted upstream strings echoed by `serde`
    /// cannot bloat `Probe` or repeat verbatim on every `/healthz` and `/readyz`.
    pub(super) fn decode(err: impl fmt::Display) -> Self {
        Self::Decode(Cow::Owned(format_truncated(err)))
    }

    pub(super) const fn decode_msg(msg: &'static str) -> Self {
        Self::Decode(Cow::Borrowed(msg))
    }

    /// Cap copied upstream JSON-RPC error payloads so a multi-MiB error response
    /// cannot bloat `Probe` or repeat verbatim on every `/healthz` and `/readyz`.
    pub(super) fn rpc_truncated(raw: &str) -> Self {
        Self::Rpc(format_truncated(raw))
    }
}

impl From<ProbeTransportError> for HealthError {
    fn from(err: ProbeTransportError) -> Self {
        match err {
            ProbeTransportError::Timeout => Self::Timeout,
            ProbeTransportError::Transport(e) => Self::transport(e),
        }
    }
}

impl fmt::Display for HealthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Timeout => f.write_str("timeout"),
            Self::BodyTimeout => f.write_str("response body timeout"),
            Self::Transport(s) => write!(f, "transport: {s}"),
            Self::HttpStatus(status) => write!(f, "http {status}"),
            Self::Decode(s) => write!(f, "decode: {s}"),
            Self::Rpc(s) => write!(f, "rpc error: {s}"),
        }
    }
}

/// Lazily formatted `"<method>: <detail>"` error so `/readyz`, `HEAD /readyz`,
/// and healthy 1 Hz polls allocate zero detail strings.
#[derive(Clone, Copy)]
pub(super) struct MethodError<'a>(pub(super) &'static str, pub(super) &'a HealthError);

impl fmt::Display for MethodError<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.0, self.1)
    }
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
            transport: &UNTRACKED_TRANSPORT,
            uri: upstream.uri(),
            timeout: self.timeout,
        }
    }

    pub(super) async fn get_bytes(self) -> Result<Bytes, HealthError> {
        self.fetch_bytes(ProbeMode::Fixed, MAX_SMALL_PROBE_BODY_BYTES, || {
            self.request(Method::GET, empty_body())
        })
        .await
    }

    /// POST a small JSON-RPC payload (`eth_chainId` / `eth_blockNumber`) without mutating `H2cState`.
    pub(super) async fn post_json(self, payload: &'static [u8]) -> Result<Bytes, HealthError> {
        self.fetch_bytes(ProbeMode::Fixed, MAX_SMALL_PROBE_BODY_BYTES, || {
            self.json_request(payload)
        })
        .await
    }

    /// POST the local EL JSON-RPC batch payload while driving adaptive `h2c` detection.
    pub(super) async fn post_json_adaptive(
        self,
        payload: &'static [u8],
    ) -> Result<Bytes, HealthError> {
        self.fetch_bytes(ProbeMode::Adaptive, MAX_EL_BATCH_BODY_BYTES, || {
            self.json_request(payload)
        })
        .await
    }

    fn json_request(self, payload: &'static [u8]) -> Request<ResBody> {
        let mut req = self.request(Method::POST, full_body(Bytes::from_static(payload)));
        req.headers_mut()
            .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        req
    }

    fn request(self, method: Method, body: ResBody) -> Request<ResBody> {
        let mut req = Request::new(body);
        *req.method_mut() = method;
        *req.uri_mut() = self.uri.clone();
        req
    }

    /// Send a probe request within `self.timeout`, record any TLS negotiated protocol
    /// version as soon as response headers arrive, drain small non-2xx error bodies so
    /// HTTP/1.1 connections remain pooled, and return the `2xx` body (capped at `max_body`).
    async fn fetch_bytes(
        self,
        mode: ProbeMode,
        max_body: usize,
        make_req: impl Fn() -> Request<ResBody>,
    ) -> Result<Bytes, HealthError> {
        let (resp, deadline) = self
            .transport
            .send_probe(self.client, self.timeout, mode, make_req)
            .await?;
        let status = resp.status();
        let is_http1 = resp.version() == http::Version::HTTP_11;
        let body = resp.into_body();
        if !status.is_success() {
            // Opportunistically drain small HTTP/1.1 error bodies (e.g. 429/503 JSON)
            // so Hyper keeps the TCP+TLS connection pooled, capped tightly so a stalled
            // error body never burns the full `--health-timeout`.
            if is_http1
                && !body.is_end_stream()
                && body.size_hint().lower() <= MAX_NON_2XX_DRAIN_BYTES as u64
            {
                let drain_deadline =
                    deadline.min(tokio::time::Instant::now() + MAX_NON_2XX_DRAIN_TIMEOUT);
                let _ = tokio::time::timeout_at(
                    drain_deadline,
                    http_body_util::Limited::new(body, MAX_NON_2XX_DRAIN_BYTES).collect(),
                )
                .await;
            }
            return Err(HealthError::HttpStatus(status));
        }
        if body.size_hint().lower() > max_body as u64 {
            return Err(HealthError::decode_msg("length limit exceeded"));
        }
        if body.is_end_stream() {
            return Ok(Bytes::new());
        }
        tokio::time::timeout_at(
            deadline,
            http_body_util::Limited::new(body, max_body).collect(),
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
    use crate::health::el::el_batch_detect;
    use crate::transport::{build_client, build_https_client, build_https_connector};
    use http::{Response, StatusCode};
    use http_body_util::Full;
    use hyper::body::Incoming;
    use hyper::service::service_fn;
    use hyper_util::rt::{TokioExecutor, TokioIo};
    use hyper_util::server::conn::auto;
    use std::convert::Infallible;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use crate::test_util::{AbortOnDrop, peek_is_h2_preface, spawn_http_mock};

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

    #[test]
    fn rpc_and_decode_errors_truncate_on_utf8_boundary() {
        let short = "short rpc error";
        assert_eq!(
            HealthError::rpc_truncated(short),
            HealthError::Rpc(short.into())
        );

        let multi = "é".repeat(300); // 600 bytes > 512
        let HealthError::Rpc(truncated) = HealthError::rpc_truncated(&multi) else {
            panic!("expected Rpc");
        };
        assert!(truncated.ends_with("..."));
        assert!(truncated.len() <= MAX_RPC_ERROR_BYTES + 3);

        let HealthError::Decode(decoded) = HealthError::decode(&multi) else {
            panic!("expected Decode");
        };
        assert!(decoded.ends_with("..."));
        assert!(decoded.len() <= MAX_RPC_ERROR_BYTES + 3);

        // Exact-limit string must not get truncated; multi-chunk Display stops at the cap.
        let exact = "a".repeat(MAX_RPC_ERROR_BYTES);
        assert_eq!(format_truncated(&exact), exact);
        // The cap falls inside a two-byte scalar here: the cut backs up one byte.
        let straddling = format!("a{}", "é".repeat(MAX_RPC_ERROR_BYTES));
        let cut = format_truncated(&straddling);
        assert_eq!(cut.len(), MAX_RPC_ERROR_BYTES - 1 + 3);
        assert!(cut.ends_with("é..."));
        struct MultiChunk;
        impl fmt::Display for MultiChunk {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                for _ in 0..100 {
                    f.write_str("0123456789")?;
                }
                Ok(())
            }
        }
        let chunked = format_truncated(MultiChunk);
        assert_eq!(chunked.len(), MAX_RPC_ERROR_BYTES + 3);
        assert!(chunked.ends_with("..."));
    }

    #[tokio::test]
    async fn reference_probes_leave_the_local_transport_untouched() {
        let oversized = Arc::new(AtomicBool::new(false));
        let oversized_srv = Arc::clone(&oversized);
        let (addr, _server) = spawn_http_mock(move |req| {
            let huge = oversized_srv.load(Ordering::SeqCst);
            async move {
                let _ = req.into_body().collect().await;
                let body = if huge {
                    Bytes::from(vec![b'x'; MAX_SMALL_PROBE_BODY_BYTES + 1])
                } else {
                    Bytes::from_static(b"{}")
                };
                Response::new(full_body(body))
            }
        })
        .await;
        let reference =
            HttpUpstream::parse("--el-reference-url", &format!("http://{addr}")).unwrap();
        let client = build_https_client();
        // A TLS local target that has not answered yet: any response recorded against it
        // would move its label off "HTTPS (ALPN)".
        let local = HttpTarget::new_standard(
            HttpUpstream::parse("--el-http-url", "https://127.0.0.1:1").unwrap(),
        );
        let el = ProbeTarget::from_target(&client, &local, Duration::from_secs(1));

        let body = el.with_upstream(&reference).post_json(b"{}").await;
        assert_eq!(body.unwrap().as_ref(), b"{}");
        assert_eq!(local.transport.startup_proto_label(), "HTTPS (ALPN)");

        // Small probes (CL Beacon / Reference EL) cap bodies at 64 KiB (not 10 MiB).
        oversized.store(true, Ordering::SeqCst);
        let err = el
            .with_upstream(&reference)
            .post_json(b"{}")
            .await
            .unwrap_err();
        assert_eq!(err, HealthError::decode_msg("length limit exceeded"));
    }

    #[tokio::test]
    async fn confirmed_h2c_post_json_falls_back_without_mutating_mode_and_preserves_primary_error()
    {
        use std::sync::atomic::AtomicU8;

        // 0 = H1 200 OK, 1 = H1 500 error, 2 = H1 silent timeout; h2c always closes on preface.
        let h1_mode = Arc::new(AtomicU8::new(0));
        let mode_srv = h1_mode.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let _server = AbortOnDrop(tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((stream, _)) = listener.accept().await {
                if peek_is_h2_preface(&stream).await {
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

        let https = build_https_connector();
        let target = HttpTarget::new_el(
            HttpUpstream::parse("--el-http-url", &format!("http://{addr}")).unwrap(),
            false,
            &https,
        );
        target.transport.set_confirmed_cleartext(true);
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
        // preserve primary's Transport error without mutating mode.
        h1_mode.store(2, Ordering::Relaxed);
        assert!(matches!(
            el.post_json(b"{}").await.unwrap_err(),
            HealthError::Transport(_)
        ));
        assert!(target.transport.uses_h2c_client());
    }

    #[tokio::test]
    async fn adaptive_h2c_settles_recovers_stale_socket_and_avoids_double_timeouts() {
        const VALID_BATCH: &[u8] = br#"[{"id":1,"result":false},{"id":2,"result":"0x40"},{"id":3,"result":{"number":"0x1","timestamp":"0x2"}}]"#;

        // Controls: how many upcoming h2 connections to drop on preface, whether to hang h1/h2.
        let drop_h2_budget = Arc::new(AtomicUsize::new(0));
        let reject_all_h2 = Arc::new(AtomicBool::new(false));
        let hang_all = Arc::new(AtomicBool::new(false));
        let attempts = Arc::new([AtomicUsize::new(0), AtomicUsize::new(0)]);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let db = Arc::clone(&drop_h2_budget);
        let rh2 = Arc::clone(&reject_all_h2);
        let hang = Arc::clone(&hang_all);
        let counts = Arc::clone(&attempts);
        let _server = AbortOnDrop(tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let db = Arc::clone(&db);
                let rh2 = Arc::clone(&rh2);
                let hang = Arc::clone(&hang);
                let counts = Arc::clone(&counts);
                tokio::spawn(async move {
                    let is_h2 = peek_is_h2_preface(&stream).await;
                    counts[usize::from(is_h2)].fetch_add(1, Ordering::SeqCst);
                    if is_h2
                        && db
                            .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                            .is_ok()
                    {
                        return;
                    }
                    if hang.load(Ordering::SeqCst) {
                        tokio::time::sleep(Duration::from_millis(500)).await;
                        return;
                    }
                    if is_h2 && rh2.load(Ordering::SeqCst) {
                        return;
                    }
                    let svc = service_fn(|req: Request<Incoming>| async move {
                        let _ = req.into_body().collect().await;
                        Ok::<_, Infallible>(
                            Response::builder()
                                .status(StatusCode::OK)
                                .body(Full::new(Bytes::from_static(VALID_BATCH)))
                                .unwrap(),
                        )
                    });
                    let _ = auto::Builder::new(TokioExecutor::new())
                        .serve_connection(TokioIo::new(stream), svc)
                        .await;
                });
            }
        }));

        let https = build_https_connector();
        let make_target = || {
            HttpTarget::new_el(
                HttpUpstream::parse("--el-http-url", &format!("http://{addr}")).unwrap(),
                false,
                &https,
            )
        };
        let default_client = build_client(https.clone());

        // 1. Unprobed -> H1 settles in a single poll against an H1-only server;
        //    subsequent H1 timeouts return immediately in 1x timeout without probing h2c.
        reject_all_h2.store(true, Ordering::SeqCst);
        let h1_target = make_target();
        let h1_el = ProbeTarget::from_target(&default_client, &h1_target, Duration::from_secs(2));
        assert!(!h1_target.transport.uses_h2c_client());
        assert!(el_batch_detect(h1_el).await.unwrap().syncing.is_ok());
        assert_eq!(h1_target.transport.label(), "http/1.1");

        let h2_before = attempts[1].load(Ordering::SeqCst);
        reject_all_h2.store(false, Ordering::SeqCst);
        assert!(el_batch_detect(h1_el).await.unwrap().syncing.is_ok());
        assert_eq!(
            attempts[1].load(Ordering::SeqCst),
            h2_before,
            "settled H1 must not re-probe h2c"
        );

        hang_all.store(true, Ordering::SeqCst);
        // Fresh client so pooled H1 conn isn't reused; verify timeout does not touch h2c.
        let fresh_client = build_client(https.clone());
        let h1_el_hang =
            ProbeTarget::from_target(&fresh_client, &h1_target, Duration::from_millis(100));
        assert_eq!(
            el_batch_detect(h1_el_hang).await.unwrap_err(),
            HealthError::Timeout
        );
        assert_eq!(
            attempts[1].load(Ordering::SeqCst),
            h2_before,
            "settled H1 timeout must not fall back to h2c"
        );
        hang_all.store(false, Ordering::SeqCst);

        // 2. Unprobed -> H2c; single stale-connection h2c reset self-heals on retry without
        //    downgrading to H1; confirmed H2c timeout returns Timeout in 1x without H1 fallback;
        //    persistent h2c rejection cleanly downgrades to H1.
        let h2_target = make_target();
        let h2_el = ProbeTarget::from_target(&default_client, &h2_target, Duration::from_secs(2));
        assert!(el_batch_detect(h2_el).await.unwrap().syncing.is_ok());
        assert_eq!(h2_target.transport.label(), "h2c");

        let h2_stale = make_target();
        h2_stale.transport.set_confirmed_cleartext(true);
        let h1_before_stale = attempts[0].load(Ordering::SeqCst);
        drop_h2_budget.store(1, Ordering::SeqCst);
        let h2_stale_el =
            ProbeTarget::from_target(&default_client, &h2_stale, Duration::from_secs(2));
        assert!(el_batch_detect(h2_stale_el).await.unwrap().syncing.is_ok());
        assert_eq!(h2_stale.transport.label(), "h2c");
        assert_eq!(
            attempts[0].load(Ordering::SeqCst),
            h1_before_stale,
            "single stale h2c failure must recover via fresh h2c retry without H1 fallback"
        );

        let h2_hang = make_target();
        h2_hang.transport.set_confirmed_cleartext(true);
        let h1_before_hang = attempts[0].load(Ordering::SeqCst);
        hang_all.store(true, Ordering::SeqCst);
        let h2_hang_el =
            ProbeTarget::from_target(&default_client, &h2_hang, Duration::from_millis(100));
        assert_eq!(
            el_batch_detect(h2_hang_el).await.unwrap_err(),
            HealthError::Timeout
        );
        assert!(h2_hang.transport.uses_h2c_client());
        assert_eq!(attempts[0].load(Ordering::SeqCst), h1_before_hang);

        // Stale pooled h2c reset followed by a hung fresh h2c retry must still return Timeout
        // in 1x timeout without falling back to HTTP/1.1 or downgrading H2c -> H1.
        let h2_stale_hang = make_target();
        h2_stale_hang.transport.set_confirmed_cleartext(true);
        let h1_before_stale_hang = attempts[0].load(Ordering::SeqCst);
        drop_h2_budget.store(1, Ordering::SeqCst);
        let h2_stale_hang_el =
            ProbeTarget::from_target(&default_client, &h2_stale_hang, Duration::from_millis(100));
        assert_eq!(
            el_batch_detect(h2_stale_hang_el).await.unwrap_err(),
            HealthError::Timeout
        );
        assert!(h2_stale_hang.transport.uses_h2c_client());
        assert_eq!(
            attempts[0].load(Ordering::SeqCst),
            h1_before_stale_hang,
            "hung h2c retry after stale socket must not fall back to HTTP/1.1"
        );
        hang_all.store(false, Ordering::SeqCst);

        reject_all_h2.store(true, Ordering::SeqCst);
        let h2_down = make_target();
        h2_down.transport.set_confirmed_cleartext(true);
        let h2_down_el =
            ProbeTarget::from_target(&default_client, &h2_down, Duration::from_secs(2));
        assert!(el_batch_detect(h2_down_el).await.unwrap().syncing.is_ok());
        assert_eq!(h2_down.transport.label(), "http/1.1");
    }

    #[tokio::test]
    async fn body_timeout_after_headers_does_not_fallback_or_downgrade_h2c() {
        struct StalledBody;
        impl hyper::body::Body for StalledBody {
            type Data = Bytes;
            type Error = Infallible;
            fn poll_frame(
                self: std::pin::Pin<&mut Self>,
                _cx: &mut std::task::Context<'_>,
            ) -> std::task::Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>>
            {
                std::task::Poll::Pending
            }
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let attempts = Arc::new([AtomicUsize::new(0), AtomicUsize::new(0)]);
        let counts = attempts.clone();
        let _server = AbortOnDrop(tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let counts = counts.clone();
                tokio::spawn(async move {
                    let h2 = peek_is_h2_preface(&stream).await;
                    counts[usize::from(h2)].fetch_add(1, Ordering::Relaxed);
                    let svc = service_fn(|req: Request<Incoming>| async move {
                        let _ = req.into_body().collect().await;
                        Ok::<_, Infallible>(
                            Response::builder()
                                .status(StatusCode::OK)
                                .body(StalledBody)
                                .unwrap(),
                        )
                    });
                    let _ = auto::Builder::new(TokioExecutor::new())
                        .serve_connection(TokioIo::new(stream), svc)
                        .await;
                });
            }
        }));

        let https = build_https_connector();
        let target = HttpTarget::new_el(
            HttpUpstream::parse("--el-http-url", &format!("http://{addr}")).unwrap(),
            false,
            &https,
        );
        let default_client = build_client(https);
        target.transport.set_confirmed_cleartext(true);
        let el = ProbeTarget::from_target(&default_client, &target, Duration::from_millis(200));
        let err = el_batch_detect(el).await.unwrap_err();
        assert_eq!(err, HealthError::BodyTimeout);
        assert!(target.transport.uses_h2c_client());
        assert_eq!(attempts[1].load(Ordering::Relaxed), 1, "h2c tried once");
        assert_eq!(
            attempts[0].load(Ordering::Relaxed),
            0,
            "h1 fallback must not run after 200 headers arrived"
        );
    }
}
