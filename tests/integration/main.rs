//! Integration tests: spawn ethonyx in-process against a hyper-based mock
//! upstream, driving the full proxy / readiness / health flow with the same
//! hyper-util Client used in production code.

mod health;
#[cfg(feature = "otel")]
mod otel;
mod proxy;
mod shutdown;
mod websocket;

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use http::{Method, Request, Response, StatusCode, Version};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::client::legacy::{Client, connect::HttpConnector};
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto;
use serde_json::Value;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_tungstenite::accept_async;

use ethonyx::{BoxError, Config, Prepared};

type TestBody = Full<Bytes>;
type TestClient = Client<HttpConnector, TestBody>;

// ---------- helpers ----------

/// Reserves an ephemeral loopback TCP port (`bind` + `listen`) for the lifetime of
/// this guard, either immediately resetting accepted connections (`SO_LINGER = 0`
/// `TCP RST`, so both Linux and macOS fail fast without multi-second SYN retries or
/// parallel port reuse) or holding them silently open to trigger `--proxy-timeout`.
struct StubPort {
    port: u16,
    accepted: Arc<tokio::sync::Notify>,
    _task: AbortOnDrop,
}

impl StubPort {
    async fn spawn(reset_immediately: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let accepted = Arc::new(tokio::sync::Notify::new());
        let accepted_srv = Arc::clone(&accepted);
        let task = tokio::spawn(async move {
            let mut held = Vec::new();
            loop {
                match listener.accept().await {
                    Ok((stream, _)) => {
                        if reset_immediately {
                            let _ =
                                socket2::SockRef::from(&stream).set_linger(Some(Duration::ZERO));
                        } else {
                            held.push(stream);
                        }
                        accepted_srv.notify_one();
                    }
                    Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
                }
            }
        });
        Self {
            port,
            accepted,
            _task: AbortOnDrop(task),
        }
    }

    async fn dead() -> Self {
        Self::spawn(true).await
    }

    async fn silent() -> Self {
        Self::spawn(false).await
    }

    async fn wait_accepted(&self) {
        self.accepted.notified().await;
    }

    fn http_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    fn ws_url(&self) -> String {
        format!("ws://127.0.0.1:{}", self.port)
    }
}

#[track_caller]
fn wait_until<T, F, Fut>(timeout: Duration, mut step: F) -> impl std::future::Future<Output = T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, String>>,
{
    const POLL_STEP: Duration = Duration::from_millis(15);
    let caller = std::panic::Location::caller();
    async move {
        let deadline = tokio::time::Instant::now() + timeout;
        let mut last = String::from("<step timed out>");
        loop {
            match tokio::time::timeout_at(deadline, step()).await {
                Ok(Ok(val)) => return val,
                Ok(Err(err)) => {
                    last = err;
                    assert!(
                        tokio::time::Instant::now() < deadline,
                        "timed out after {timeout:?} at {caller}; last observed: {last}"
                    );
                    tokio::time::sleep(POLL_STEP).await;
                }
                Err(_) => panic!("timed out after {timeout:?} at {caller}; last observed: {last}"),
            }
        }
    }
}

#[track_caller]
fn wait_for<T, F, Fut>(step: F) -> impl std::future::Future<Output = T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, String>>,
{
    wait_until(Duration::from_secs(2), step)
}

/// Sum Prometheus exposition samples whose metric name matches `metric` and whose
/// line contains every required label fragment in `labels`.
fn prom_metric_sum(text: &str, metric: &str, labels: &[&str]) -> f64 {
    text.lines()
        .filter(|line| {
            !line.starts_with('#')
                && line
                    .strip_prefix(metric)
                    .is_some_and(|rest| rest.starts_with(' ') || rest.starts_with('{'))
                && labels.iter().all(|label| line.contains(label))
        })
        .filter_map(|line| line.split_whitespace().last()?.parse::<f64>().ok())
        .sum()
}

type H2WebSocket = tokio_tungstenite::WebSocketStream<TokioIo<hyper::upgrade::Upgraded>>;

/// RFC 8441 Extended CONNECT request that opens a WebSocket (version 13) on `uri`.
fn ext_connect(uri: impl AsRef<str>) -> Request<TestBody> {
    let mut req = Request::builder()
        .method(Method::CONNECT)
        .uri(uri.as_ref())
        .header("sec-websocket-version", "13")
        .body(Full::new(Bytes::new()))
        .unwrap();
    req.extensions_mut()
        .insert(hyper::ext::Protocol::from_static("websocket"));
    req
}

/// Turn a `200` Extended CONNECT response into a client WebSocket.
async fn upgrade_ws(resp: Response<Incoming>) -> H2WebSocket {
    let upgraded = hyper::upgrade::on(resp).await.expect("upgrade");
    tokio_tungstenite::WebSocketStream::from_raw_socket(
        TokioIo::new(upgraded),
        tokio_tungstenite::tungstenite::protocol::Role::Client,
        None,
    )
    .await
}

struct AbortOnDrop(JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// RAII HTTP/2 client connection that aborts its background driver task on drop.
struct H2Client {
    sender: hyper::client::conn::http2::SendRequest<TestBody>,
    _driver: AbortOnDrop,
}

impl std::ops::Deref for H2Client {
    type Target = hyper::client::conn::http2::SendRequest<TestBody>;

    fn deref(&self) -> &Self::Target {
        &self.sender
    }
}

impl std::ops::DerefMut for H2Client {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.sender
    }
}

fn client() -> TestClient {
    let mut http = HttpConnector::new();
    http.set_nodelay(true);
    Client::builder(TokioExecutor::new()).build::<_, TestBody>(http)
}

/// Cleartext HTTP/2 (h2c) prior-knowledge client — sends the h2 preface directly,
/// simulating a TLS-terminating LB / mesh that forwards h2c to the backend.
fn h2c_client() -> TestClient {
    let mut http = HttpConnector::new();
    http.set_nodelay(true);
    Client::builder(TokioExecutor::new())
        .http2_only(true)
        .build::<_, TestBody>(http)
}

async fn get(c: &TestClient, url: &str) -> (StatusCode, Bytes) {
    let req = Request::builder()
        .method(Method::GET)
        .uri(url)
        .body(Full::new(Bytes::new()))
        .unwrap();
    let resp = c.request(req).await.unwrap();
    let status = resp.status();
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    (status, body)
}

async fn post_json(c: &TestClient, url: &str, body: Value) -> (StatusCode, Bytes) {
    let req = Request::builder()
        .method(Method::POST)
        .uri(url)
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(serde_json::to_vec(&body).unwrap())))
        .unwrap();
    let resp = c.request(req).await.unwrap();
    let status = resp.status();
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    (status, body)
}

// ---------- mock upstream ----------

#[derive(Clone, Debug)]
struct RecordedRequest {
    method: Method,
    path: String,
    uri: http::Uri,
    headers: http::HeaderMap,
    version: Version,
    body: Bytes,
}

type MockHandler = Arc<dyn Fn(&RecordedRequest) -> (StatusCode, Bytes) + Send + Sync>;

#[derive(Clone, Copy)]
enum MockProto {
    Http1,
    H2c,
    HangH2c,
}

struct MockServer {
    url: String,
    recorded: Arc<Mutex<Vec<RecordedRequest>>>,
    _accept_task: AbortOnDrop,
}

impl MockServer {
    /// Serve HTTP/1.1 only (rejects an h2c prior-knowledge preface).
    async fn start(handler: MockHandler) -> Self {
        Self::serve(handler, MockProto::Http1).await
    }

    /// Serve both HTTP/1.1 and cleartext HTTP/2 (h2c), like geth >=v1.17 / erigon.
    async fn start_h2c(handler: MockHandler) -> Self {
        Self::serve(handler, MockProto::H2c).await
    }

    /// Serve HTTP/1.1 only, but hang if an h2c prior-knowledge preface (PRI) is received.
    async fn start_hanging_h2c(handler: MockHandler) -> Self {
        Self::serve(handler, MockProto::HangH2c).await
    }

    async fn serve(handler: MockHandler, proto: MockProto) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr: SocketAddr = listener.local_addr().unwrap();
        let url = format!("http://{addr}");
        let recorded = Arc::new(Mutex::new(Vec::new()));

        let h = handler;
        let rec = recorded.clone();
        let accept_task = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                let h = h.clone();
                let rec = rec.clone();
                tokio::spawn(async move {
                    if matches!(proto, MockProto::HangH2c) && peek_is_h2_preface(&stream).await {
                        // Hang past the caller's `--health-timeout` to trigger fallback.
                        tokio::time::sleep(Duration::from_millis(1500)).await;
                        return;
                    }

                    let io = TokioIo::new(stream);
                    let svc = service_fn(move |req: Request<Incoming>| {
                        handle_mock_request(req, h.clone(), rec.clone())
                    });
                    if matches!(proto, MockProto::H2c) {
                        let _ = auto::Builder::new(TokioExecutor::new())
                            .serve_connection(io, svc)
                            .await;
                    } else {
                        let _ = hyper::server::conn::http1::Builder::new()
                            .serve_connection(io, svc)
                            .await;
                    }
                });
            }
        });

        MockServer {
            url,
            recorded,
            _accept_task: AbortOnDrop(accept_task),
        }
    }

    fn received(&self) -> Vec<RecordedRequest> {
        self.recorded.lock().unwrap().clone()
    }

    fn last_received(&self) -> Option<RecordedRequest> {
        self.recorded.lock().unwrap().last().cloned()
    }
}

/// Peek up to 3 bytes (looping on rare 1–2 byte partial reads) to detect an
/// HTTP/2 prior-knowledge preface (`"PRI"`).
async fn peek_is_h2_preface(stream: &tokio::net::TcpStream) -> bool {
    let mut buf = [0u8; 3];
    tokio::time::timeout(Duration::from_millis(500), async {
        loop {
            match stream.peek(&mut buf).await {
                Ok(0) | Err(_) => return false,
                Ok(n) if n >= 3 => return &buf == b"PRI",
                Ok(n) if !b"PRI".starts_with(&buf[..n]) => return false,
                Ok(_) => tokio::time::sleep(Duration::from_millis(1)).await,
            }
        }
    })
    .await
    .unwrap_or(false)
}

async fn handle_mock_request(
    req: Request<Incoming>,
    handler: MockHandler,
    recorded: Arc<Mutex<Vec<RecordedRequest>>>,
) -> Result<Response<TestBody>, Infallible> {
    let (parts, body) = req.into_parts();
    let path = parts.uri.path().to_owned();
    let body_bytes = body.collect().await.unwrap().to_bytes();

    let (status, resp_bytes) = if body_bytes.first() == Some(&b'[')
        && let Ok(Value::Array(req_arr)) = serde_json::from_slice::<Value>(&body_bytes)
        && !req_arr.is_empty()
        && req_arr.iter().all(|item| {
            item.as_object()
                .is_some_and(|obj| obj.contains_key("jsonrpc"))
        }) {
        // JSON-RPC batch request
        let mut resp_arr = Vec::with_capacity(req_arr.len());
        let mut overall_status = StatusCode::OK;
        for mut req_item in req_arr {
            let item_bytes = serde_json::to_vec(&req_item).unwrap();
            let req_id = req_item.as_object_mut().and_then(|obj| obj.remove("id"));
            let rr = RecordedRequest {
                method: parts.method.clone(),
                path: path.clone(),
                uri: parts.uri.clone(),
                headers: parts.headers.clone(),
                version: parts.version,
                body: Bytes::from(item_bytes),
            };
            let (status, item_resp_bytes) = handler(&rr);
            recorded.lock().unwrap().push(rr);
            if !status.is_success() {
                overall_status = status;
            }
            if let Some(id_val) = req_id {
                let mut item_resp_val: Value =
                    serde_json::from_slice(&item_resp_bytes).unwrap_or(Value::Null);
                if let Some(obj) = item_resp_val.as_object_mut() {
                    obj.insert("id".to_string(), id_val);
                }
                resp_arr.push(item_resp_val);
            }
        }
        if resp_arr.is_empty() {
            let status = if overall_status.is_success() {
                StatusCode::NO_CONTENT
            } else {
                overall_status
            };
            (status, Bytes::new())
        } else {
            let final_resp = serde_json::to_vec(&Value::Array(resp_arr)).unwrap();
            (overall_status, Bytes::from(final_resp))
        }
    } else {
        // Single request (including non-JSON-RPC JSON arrays like `[]`)
        let rr = RecordedRequest {
            method: parts.method,
            path,
            uri: parts.uri,
            headers: parts.headers,
            version: parts.version,
            body: body_bytes,
        };
        let out = handler(&rr);
        recorded.lock().unwrap().push(rr);
        out
    };

    let mut builder = Response::builder().status(status);
    if status != StatusCode::NO_CONTENT {
        builder = builder.header("content-type", "application/json");
    }
    Ok(builder.body(Full::new(resp_bytes)).unwrap())
}

// ---------- ethonyx handle ----------

struct EthonyxHandle {
    port: u16,
    ports: Vec<u16>,
    shutdown: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<Result<(), BoxError>>>,
    shutting_down: bool,
}

impl EthonyxHandle {
    async fn start(args: &[&str]) -> Self {
        Self::start_tuned(args, |_| {}).await
    }

    /// `--trust-upstream` proxy: the placeholder EL URL is never dialed by a probe.
    async fn start_trusted(args: &[&str]) -> Self {
        Self::start_trusted_tuned(args, |_| {}).await
    }

    async fn start_trusted_tuned(args: &[&str], tune: impl FnOnce(&mut Config)) -> Self {
        let mut argv = vec!["--el-http-url", "http://127.0.0.1:8545", "--trust-upstream"];
        argv.extend_from_slice(args);
        Self::start_tuned(&argv, tune).await
    }

    async fn start_trusted_ws(ws_url: &str) -> Self {
        Self::start_trusted(&["--el-ws-url", ws_url]).await
    }

    async fn start_trusted_ws_with_grace(ws_url: &str, grace: Duration) -> Self {
        Self::start_trusted_tuned(&["--el-ws-url", ws_url], |cfg| cfg.shutdown_grace = grace).await
    }

    async fn start_tuned(args: &[&str], tune: impl FnOnce(&mut Config)) -> Self {
        let mut argv: Vec<&str> = vec![
            "ethonyx",
            "--listen",
            "127.0.0.1:0",
            "--shutdown-grace",
            "0",
            "--health-timeout",
            "1",
            "--proxy-timeout",
            "2",
        ];
        argv.extend_from_slice(args);
        let mut cfg = Config::try_parse_from(argv).expect("clap parse");
        tune(&mut cfg);
        let bound = Prepared::new(cfg)
            .await
            .expect("prepare")
            .bind()
            .await
            .expect("bind");
        let ports: Vec<u16> = bound.local_addrs().iter().map(SocketAddr::port).collect();
        let port = ports[0];

        let (tx, rx) = oneshot::channel::<()>();
        let task = tokio::spawn(bound.serve(async move {
            let _ = rx.await;
        }));
        EthonyxHandle {
            port,
            ports,
            shutdown: Some(tx),
            task: Some(task),
            shutting_down: false,
        }
    }

    fn url(&self, path: &str) -> String {
        format!("http://127.0.0.1:{}{}", self.port, path)
    }

    fn trigger_shutdown(&mut self) {
        self.shutdown
            .take()
            .expect("shutdown not yet triggered")
            .send(())
            .expect("serve task still listening for shutdown");
    }

    async fn shutdown(mut self) {
        self.shutting_down = true;
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(task) = self.task.as_mut() {
            let res = task.await;
            self.task = None;
            res.expect("ethonyx serve task panicked")
                .expect("ethonyx serve returned error");
        }
    }

    async fn open_h2(&self) -> H2Client {
        let tcp = tokio::net::TcpStream::connect(("127.0.0.1", self.port))
            .await
            .unwrap();
        let (sender, conn) = hyper::client::conn::http2::Builder::new(TokioExecutor::new())
            .handshake::<_, TestBody>(TokioIo::new(tcp))
            .await
            .unwrap();
        let driver = tokio::spawn(async move {
            let _ = conn.await;
        });
        H2Client {
            sender,
            _driver: AbortOnDrop(driver),
        }
    }

    async fn connect_h2_ws(&self, path: &str) -> (H2WebSocket, H2Client) {
        let mut h2 = self.open_h2().await;
        let resp = h2.send_request(ext_connect(self.url(path))).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let ws = upgrade_ws(resp).await;
        (ws, h2)
    }
}

impl Drop for EthonyxHandle {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(task) = self.task.take() {
            task.abort();
            // Cleanup is for a test that is already failing or timed out while awaiting
            // `shutdown()`. Normal exit must await `shutdown()` so the serve task's
            // Result is checked.
            assert!(
                self.shutting_down || std::thread::panicking(),
                "EthonyxHandle dropped without `shutdown().await`"
            );
        }
    }
}

enum RpcMockReply {
    Result(Value),
    Error(i64, &'static str),
    Http(StatusCode, &'static [u8]),
}

fn jsonrpc_mock(handler: impl Fn(&str) -> RpcMockReply + Send + Sync + 'static) -> MockHandler {
    Arc::new(move |req| {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or(Value::Null);
        let method = body.get("method").and_then(Value::as_str).unwrap_or("");
        let default_id = serde_json::json!(1);
        let id = body.get("id").unwrap_or(&default_id);
        match handler(method) {
            RpcMockReply::Result(result) => (
                StatusCode::OK,
                Bytes::from(
                    serde_json::to_vec(&serde_json::json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "result": result,
                    }))
                    .unwrap(),
                ),
            ),
            RpcMockReply::Error(code, message) => (
                StatusCode::OK,
                Bytes::from(
                    serde_json::to_vec(&serde_json::json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "error": { "code": code, "message": message },
                    }))
                    .unwrap(),
                ),
            ),
            RpcMockReply::Http(status, bytes) => (status, Bytes::from_static(bytes)),
        }
    })
}

fn cl_sync_mock(syncing_data: Value, peers: Option<u64>) -> MockHandler {
    let syncing_bytes =
        Bytes::from(serde_json::to_vec(&serde_json::json!({ "data": syncing_data })).unwrap());
    let peers_bytes = peers.map(|n| {
        // Emit the full Beacon API shape so probes stay tolerant of unused sibling fields.
        Bytes::from(
            serde_json::to_vec(&serde_json::json!({
                "data": {
                    "disconnected": "0",
                    "connecting": "0",
                    "connected": n.to_string(),
                    "disconnecting": "0",
                }
            }))
            .unwrap(),
        )
    });
    Arc::new(move |req| match req.path.as_str() {
        "/eth/v1/node/syncing" => (StatusCode::OK, syncing_bytes.clone()),
        "/eth/v1/node/peer_count" => match &peers_bytes {
            Some(bytes) => (StatusCode::OK, bytes.clone()),
            None => (StatusCode::NOT_FOUND, Bytes::from_static(b"{}")),
        },
        _ => (StatusCode::NOT_FOUND, Bytes::from_static(b"{}")),
    })
}

fn ok_handler() -> MockHandler {
    Arc::new(|_| (StatusCode::OK, Bytes::from_static(b"{}")))
}

fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Minimal RAII WebSocket echo upstream that counts accepted TCP dials and aborts on drop.
struct WsMockServer {
    url: String,
    dials: Arc<std::sync::atomic::AtomicUsize>,
    _accept_task: AbortOnDrop,
}

impl WsMockServer {
    fn dials(&self) -> usize {
        self.dials.load(std::sync::atomic::Ordering::SeqCst)
    }
}

async fn ws_echo_upstream() -> WsMockServer {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let dials = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let dials_srv = Arc::clone(&dials);
    let accept_task = tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            dials_srv.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            tokio::spawn(async move {
                let Ok(mut ws) = accept_async(stream).await else {
                    return;
                };
                while let Some(Ok(msg)) = ws.next().await {
                    if msg.is_close() {
                        let _ = ws.close(None).await;
                        break;
                    }
                    if (msg.is_text() || msg.is_binary()) && ws.send(msg).await.is_err() {
                        break;
                    }
                }
            });
        }
    });
    WsMockServer {
        url: format!("ws://{addr}"),
        dials,
        _accept_task: AbortOnDrop(accept_task),
    }
}
