//! Integration tests: spawn ethonyx in-process against a hyper-based mock
//! upstream, driving the full proxy / readiness / health flow with the same
//! hyper-util Client used in production code. Zero extra dev-dependencies.

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
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::{Error as WsError, Message};
use tokio_tungstenite::{accept_async, connect_async};

use ethonyx::{Config, run};

type TestBody = Full<Bytes>;
type TestClient = Client<HttpConnector, TestBody>;
type BoxError = Box<dyn std::error::Error + Send + Sync>;

// ---------- helpers ----------

/// Bind an ephemeral port and immediately release it. Returns the port number.
/// Race window between drop and re-bind is tiny on localhost.
async fn pick_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

/// POST a JSON-RPC and retry briefly while the response is `502`. The very first
/// data-plane request to an h2c upstream may establish a fresh HTTP/2 connection,
/// which can momentarily exceed the test's short `--proxy-timeout` on a heavily
/// loaded (cold-start) CI runner; a steady-state sidecar keeps the pool warm. This
/// mirrors ethonyx's documented behaviour that a transport in flux can briefly
/// surface a `502` until it settles, so it doesn't mask a real failure (a
/// persistent `502` still fails the test).
async fn post_json_settled(c: &TestClient, url: &str, body: Value) -> (StatusCode, Bytes) {
    let mut last = (StatusCode::BAD_GATEWAY, Bytes::new());
    for _ in 0..50 {
        last = post_json(c, url, body.clone()).await;
        if last.0 != StatusCode::BAD_GATEWAY {
            return last;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    last
}

/// Poll until the given port accepts connections.
async fn wait_for_port(port: u16) {
    for _ in 0..100 {
        if tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .is_ok()
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("port {port} did not become available");
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
    let payload = serde_json::to_vec(&body).unwrap();
    let req = Request::builder()
        .method(Method::POST)
        .uri(url)
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(payload)))
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

type MockHandler = Arc<dyn Fn(&RecordedRequest) -> (StatusCode, Vec<u8>) + Send + Sync>;

struct MockServer {
    url: String,
    recorded: Arc<Mutex<Vec<RecordedRequest>>>,
}

impl MockServer {
    /// Serve HTTP/1.1 only (rejects an h2c prior-knowledge preface).
    async fn start(handler: MockHandler) -> Self {
        Self::serve(handler, false, false).await
    }

    /// Serve both HTTP/1.1 and cleartext HTTP/2 (h2c), like geth >=v1.17 / erigon.
    async fn start_h2c(handler: MockHandler) -> Self {
        Self::serve(handler, true, false).await
    }

    /// Serve HTTP/1.1 only, but hang if an h2c prior-knowledge preface (PRI) is received.
    async fn start_hanging_h2c(handler: MockHandler) -> Self {
        Self::serve(handler, false, true).await
    }

    async fn serve(handler: MockHandler, h2: bool, hang_h2c: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr: SocketAddr = listener.local_addr().unwrap();
        let url = format!("http://{addr}");
        let recorded = Arc::new(Mutex::new(Vec::new()));

        let h = handler;
        let rec = recorded.clone();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                let h = h.clone();
                let rec = rec.clone();
                tokio::spawn(async move {
                    if hang_h2c {
                        // Peek first 3 bytes to see if it starts with HTTP/2 preface "PRI"
                        let mut buf = [0u8; 3];
                        if stream
                            .peek(&mut buf)
                            .await
                            .is_ok_and(|n| n >= 3 && &buf == b"PRI")
                        {
                            // It's the HTTP/2 preface! Hang for 1.5 seconds to cause a timeout.
                            tokio::time::sleep(Duration::from_millis(1500)).await;
                            return;
                        }
                    }

                    let io = TokioIo::new(stream);
                    let svc = service_fn(move |req: Request<Incoming>| {
                        let h = h.clone();
                        let rec = rec.clone();
                        async move {
                            let method = req.method().clone();
                            let path = req.uri().path().to_owned();
                            let uri = req.uri().clone();
                            let headers = req.headers().clone();
                            let version = req.version();
                            let body_bytes = req.into_body().collect().await.unwrap().to_bytes();

                            let (status, resp_bytes) = if body_bytes.first() == Some(&b'[') {
                                // JSON-RPC batch request
                                if let Ok(Value::Array(req_arr)) =
                                    serde_json::from_slice::<Value>(&body_bytes)
                                {
                                    let mut resp_arr = Vec::new();
                                    let mut overall_status = StatusCode::OK;
                                    for req_item in req_arr {
                                        let item_bytes = serde_json::to_vec(&req_item).unwrap();
                                        let rr = RecordedRequest {
                                            method: method.clone(),
                                            path: path.clone(),
                                            uri: uri.clone(),
                                            headers: headers.clone(),
                                            version,
                                            body: Bytes::from(item_bytes),
                                        };
                                        rec.lock().unwrap().push(rr.clone());
                                        let (status, item_resp_bytes) = h(&rr);
                                        if !status.is_success() {
                                            overall_status = status;
                                        }
                                        let req_id = req_item.get("id").cloned();
                                        let mut item_resp_val: Value =
                                            serde_json::from_slice(&item_resp_bytes)
                                                .unwrap_or(Value::Null);
                                        if let (Some(id_val), Some(obj)) =
                                            (req_id, item_resp_val.as_object_mut())
                                        {
                                            obj.insert("id".to_string(), id_val);
                                        }
                                        resp_arr.push(item_resp_val);
                                    }
                                    let final_resp =
                                        serde_json::to_vec(&Value::Array(resp_arr)).unwrap();
                                    (overall_status, final_resp)
                                } else {
                                    (StatusCode::BAD_REQUEST, b"[]".to_vec())
                                }
                            } else {
                                // Single request
                                let rr = RecordedRequest {
                                    method: method.clone(),
                                    path: path.clone(),
                                    uri: uri.clone(),
                                    headers: headers.clone(),
                                    version,
                                    body: body_bytes.clone(),
                                };
                                rec.lock().unwrap().push(rr.clone());
                                let (status, resp_bytes) = h(&rr);
                                (status, resp_bytes)
                            };

                            Ok::<_, Infallible>(
                                Response::builder()
                                    .status(status)
                                    .header("content-type", "application/json")
                                    .body(Full::new(Bytes::from(resp_bytes)))
                                    .unwrap(),
                            )
                        }
                    });
                    if h2 {
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

        MockServer { url, recorded }
    }

    fn received(&self) -> Vec<RecordedRequest> {
        self.recorded.lock().unwrap().clone()
    }
}

// ---------- ethonyx handle ----------

struct EthonyxHandle {
    port: u16,
    shutdown: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<Result<(), BoxError>>>,
}

impl EthonyxHandle {
    async fn start(args: &[&str]) -> Self {
        Self::start_with_grace(args, Duration::ZERO).await
    }

    async fn start_with_grace(args: &[&str], grace: Duration) -> Self {
        let port = pick_port().await;
        let addr_str = format!("127.0.0.1:{port}");
        let grace_str = grace.as_secs().to_string();
        let mut argv: Vec<&str> = vec![
            "ethonyx",
            "--listen",
            &addr_str,
            "--shutdown-grace",
            &grace_str,
            "--health-timeout",
            "1",
            "--proxy-timeout",
            "2",
        ];
        argv.extend_from_slice(args);
        let cfg = Config::try_parse_from(argv).expect("clap parse");

        let (tx, rx) = oneshot::channel::<()>();
        let task = tokio::spawn(run(cfg, async move {
            let _ = rx.await;
        }));
        wait_for_port(port).await;
        EthonyxHandle {
            port,
            shutdown: Some(tx),
            task: Some(task),
        }
    }

    fn url(&self, path: &str) -> String {
        format!("http://127.0.0.1:{}{}", self.port, path)
    }

    async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

fn ok_handler() -> MockHandler {
    Arc::new(|_| (StatusCode::OK, b"{}".to_vec()))
}

fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

// ---------- tests ----------

#[tokio::test]
async fn livez_returns_200_ok() {
    let el = MockServer::start(ok_handler()).await;
    let cl = MockServer::start(ok_handler()).await;
    let ethonyx =
        EthonyxHandle::start(&["--el-http-url", &el.url, "--cl-beacon-url", &cl.url]).await;

    let c = client();
    let (status, body) = get(&c, &ethonyx.url("/livez")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(&body[..], b"ok");

    ethonyx.shutdown().await;
}

#[tokio::test]
async fn jsonrpc_post_is_forwarded_to_el_http_upstream() {
    let el = MockServer::start(Arc::new(|req| {
        assert_eq!(req.method, Method::POST);
        assert_eq!(req.path, "/");
        (
            StatusCode::OK,
            br#"{"jsonrpc":"2.0","id":7,"result":"0xdeadbeef"}"#.to_vec(),
        )
    }))
    .await;
    let cl = MockServer::start(ok_handler()).await;
    let ethonyx =
        EthonyxHandle::start(&["--el-http-url", &el.url, "--cl-beacon-url", &cl.url]).await;

    let c = client();
    let (status, body) = post_json(
        &c,
        &ethonyx.url("/"),
        json!({"jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id": 7}),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["result"], "0xdeadbeef");

    // EL also receives background health-poll RPCs; filter for the forwarded one.
    let methods: Vec<String> = el
        .received()
        .iter()
        .filter_map(|r| serde_json::from_slice::<Value>(&r.body).ok())
        .filter_map(|v| v.get("method").and_then(Value::as_str).map(String::from))
        .collect();
    assert_eq!(
        methods.iter().filter(|m| *m == "eth_blockNumber").count(),
        1,
        "expected one forwarded eth_blockNumber, saw {methods:?}"
    );

    // The EL mock is HTTP/1.1-only, so ethonyx (which prefers h2c) detects that the
    // h2c probe fails and falls back to HTTP/1.1 for the upstream hop.
    let fwd = el
        .received()
        .into_iter()
        .find(|r| String::from_utf8_lossy(&r.body).contains("eth_blockNumber"))
        .expect("forwarded request recorded");
    assert_eq!(fwd.version, Version::HTTP_11);

    ethonyx.shutdown().await;
}

#[tokio::test]
async fn el_upstream_h2c_is_auto_detected() {
    // No flag: when the EL serves h2c, the health poller detects it (it prefers
    // h2c) and the data-plane forwards over HTTP/2. The mock records the version
    // it received. (An h1-only EL falls back to HTTP/1.1 — covered by
    // `jsonrpc_post_is_forwarded_to_el_http_upstream`.)
    let el = MockServer::start_h2c(Arc::new(|_| {
        (
            StatusCode::OK,
            br#"{"jsonrpc":"2.0","id":7,"result":"0xcafe"}"#.to_vec(),
        )
    }))
    .await;
    let cl = MockServer::start(ok_handler()).await;
    let ethonyx =
        EthonyxHandle::start(&["--el-http-url", &el.url, "--cl-beacon-url", &cl.url]).await;

    // Downstream client version is irrelevant — it's the upstream hop we assert.
    let c = client();
    let (status, body) = post_json_settled(
        &c,
        &ethonyx.url("/"),
        json!({"jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id": 7}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["result"], "0xcafe");

    // The EL upstream received the forwarded JSON-RPC over HTTP/2 (auto-detected).
    let fwd = el
        .received()
        .into_iter()
        .find(|r| String::from_utf8_lossy(&r.body).contains("eth_blockNumber"))
        .expect("EL received the forwarded JSON-RPC");
    assert_eq!(fwd.version, Version::HTTP_2);

    // CL is unaffected — its hop stays HTTP/1.1.
    let cl_req = cl
        .received()
        .into_iter()
        .next()
        .expect("CL received a health probe");
    assert_eq!(cl_req.version, Version::HTTP_11);

    // /healthz surfaces the detected upstream transport per layer.
    let (_, hz) = get(&c, &ethonyx.url("/healthz")).await;
    let hv: Value = serde_json::from_slice(&hz).unwrap();
    assert_eq!(hv["el"]["transport"], "h2c");
    assert_eq!(hv["cl"]["transport"], "http/1.1");

    ethonyx.shutdown().await;
}

// HTTP/2 downstream coverage: h1→h1 is `jsonrpc_post_is_forwarded` above; h2c→h1
// (the LB→backend h2c shape) is below. Upstream h2 (h1→h2, h2→h2) can't be tested
// hermetically — the production client trusts only webpki public roots, so a
// self-signed h2 mock is rejected. Verified manually against a public h2 server:
//   ethonyx --el-http-url https://www.cloudflare.com --listen 127.0.0.1:18548 &
//   curl -s http://127.0.0.1:18548/cdn-cgi/trace | grep http=        # h1→h2 ⇒ http/2
//   curl -s --http2-prior-knowledge .../cdn-cgi/trace | grep http=   # h2→h2 ⇒ http/2
#[tokio::test]
async fn http2_h2c_jsonrpc_is_served_and_forwarded() {
    // A prior-knowledge h2c client (the LB→backend h2c shape) is served over
    // HTTP/2 by the auto server, and its JSON-RPC is forwarded to the EL upstream
    // (which stays h1).
    let el = MockServer::start(Arc::new(|req| {
        assert_eq!(req.method, Method::POST);
        (
            StatusCode::OK,
            br#"{"jsonrpc":"2.0","id":7,"result":"0xcafe"}"#.to_vec(),
        )
    }))
    .await;
    let cl = MockServer::start(ok_handler()).await;
    let ethonyx =
        EthonyxHandle::start(&["--el-http-url", &el.url, "--cl-beacon-url", &cl.url]).await;

    let c = h2c_client();
    let (status, body) = post_json(
        &c,
        &ethonyx.url("/"),
        json!({"jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id": 7}),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["result"], "0xcafe");

    ethonyx.shutdown().await;
}

#[tokio::test]
async fn http2_h2c_negotiates_to_http2() {
    // The response comes back over HTTP/2 → confirms the auto server actually
    // spoke h2c (not a silent downgrade to h1).
    let el = MockServer::start(ok_handler()).await;
    let cl = MockServer::start(ok_handler()).await;
    let ethonyx =
        EthonyxHandle::start(&["--el-http-url", &el.url, "--cl-beacon-url", &cl.url]).await;

    let c = h2c_client();
    let req = Request::builder()
        .method(Method::GET)
        .uri(ethonyx.url("/livez"))
        .body(Full::new(Bytes::new()))
        .unwrap();
    let resp = c.request(req).await.unwrap();
    assert_eq!(resp.version(), Version::HTTP_2, "expected h2, got {resp:?}");
    assert_eq!(resp.status(), StatusCode::OK);

    ethonyx.shutdown().await;
}

#[tokio::test]
async fn beacon_path_is_routed_to_cl_upstream() {
    let el = MockServer::start(Arc::new(|req| {
        // Background health polls legitimately POST to "/"; only /eth/ beacon
        // paths must never be routed to the EL upstream.
        assert!(
            !req.path.starts_with("/eth/"),
            "EL must not see beacon /eth/ traffic, got {}",
            req.path
        );
        (StatusCode::OK, b"{}".to_vec())
    }))
    .await;
    let cl = MockServer::start(Arc::new(|req| {
        assert_eq!(req.method, Method::GET);
        assert_eq!(req.path, "/eth/v1/beacon/genesis");
        (
            StatusCode::OK,
            br#"{"data":{"genesis_time":"1606824023"}}"#.to_vec(),
        )
    }))
    .await;
    let ethonyx =
        EthonyxHandle::start(&["--el-http-url", &el.url, "--cl-beacon-url", &cl.url]).await;

    let c = client();
    let (status, body) = get(&c, &ethonyx.url("/eth/v1/beacon/genesis")).await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["data"]["genesis_time"], "1606824023");

    ethonyx.shutdown().await;
}

#[tokio::test]
async fn client_specific_beacon_paths_are_routed_to_cl_upstream() {
    let el = MockServer::start(Arc::new(|req| {
        // Assert that none of the client-specific paths reach EL.
        let path = &req.path;
        assert!(
            !(path.starts_with("/lighthouse")
                || path.starts_with("/prysm")
                || path.starts_with("/teku")
                || path.starts_with("/lodestar")
                || path.starts_with("/nimbus")),
            "EL must not see client-specific beacon traffic, got {}",
            path
        );
        (StatusCode::OK, b"{}".to_vec())
    }))
    .await;
    let cl = MockServer::start(Arc::new(|req| {
        assert_eq!(req.method, Method::GET);
        let resp = format!(r#"{{"path":"{}"}}"#, req.path);
        (StatusCode::OK, resp.into_bytes())
    }))
    .await;
    let ethonyx =
        EthonyxHandle::start(&["--el-http-url", &el.url, "--cl-beacon-url", &cl.url]).await;

    let c = client();
    let paths = vec![
        "/lighthouse/version",
        "/prysm/v1/node/syncing",
        "/teku/v1/node/syncing",
        "/lodestar/v1/node/syncing",
        "/nimbus/v1/node/syncing",
        "/lighthouse",
        "/prysm",
        "/teku",
        "/lodestar",
        "/nimbus",
    ];

    for path in paths {
        let (status, body) = get(&c, &ethonyx.url(path)).await;
        assert_eq!(status, StatusCode::OK, "Failed for path: {path}");
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["path"], path, "Failed for path: {path}");
    }

    ethonyx.shutdown().await;
}

#[tokio::test]
async fn hop_by_hop_headers_are_stripped_on_forward() {
    for upstream_h2 in [false, true] {
        let el = MockServer::serve(ok_handler(), upstream_h2, false).await;
        let cl = MockServer::start(ok_handler()).await;
        let ethonyx =
            EthonyxHandle::start(&["--el-http-url", &el.url, "--cl-beacon-url", &cl.url]).await;
        let req = Request::builder()
            .method(Method::POST)
            .uri(ethonyx.url("/"))
            .header("content-type", "application/json")
            .header("connection", "keep-alive, X-One")
            .header("connection", "x-two")
            .header("x-one", "private")
            .header("x-two", "private")
            .header("proxy-connection", "keep-alive")
            .header("authorization", "Bearer retained")
            .body(Full::new(Bytes::from_static(b"{}")))
            .unwrap();
        let resp = client().request(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        resp.into_body().collect().await.unwrap();
        let received = el.received();
        let forwarded = received.last().unwrap();
        assert_eq!(
            forwarded.version,
            if upstream_h2 {
                Version::HTTP_2
            } else {
                Version::HTTP_11
            }
        );
        for name in ["connection", "x-one", "x-two", "proxy-connection"] {
            assert!(!forwarded.headers.contains_key(name), "leaked {name}");
        }
        assert_eq!(forwarded.headers["authorization"], "Bearer retained");
        assert_eq!(forwarded.headers["content-type"], "application/json");
        assert_eq!(forwarded.body, "{}");
        ethonyx.shutdown().await;
    }
}

#[tokio::test]
async fn response_connection_options_are_stripped_for_h1_and_h2_clients() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            tokio::spawn(async move {
                let svc = service_fn(|req: Request<Incoming>| async move {
                    req.into_body().collect().await.unwrap();
                    Ok::<_, Infallible>(
                        Response::builder()
                            .header("connection", "keep-alive, X-One")
                            .header("connection", "x-two")
                            .header("x-one", "private")
                            .header("x-two", "private")
                            .header("proxy-connection", "keep-alive")
                            .header("x-end-to-end", "retained")
                            .body(Full::new(Bytes::from_static(b"response body")))
                            .unwrap(),
                    )
                });
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), svc)
                    .await;
            });
        }
    });
    let ethonyx = EthonyxHandle::start(&["--trust-upstream", "--cl-beacon-url", &upstream]).await;
    for c in [client(), h2c_client()] {
        let req = Request::builder()
            .uri(ethonyx.url("/eth/header-test"))
            .body(Full::new(Bytes::new()))
            .unwrap();
        let resp = c.request(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        for name in ["connection", "x-one", "x-two", "proxy-connection"] {
            assert!(!resp.headers().contains_key(name), "leaked {name}");
        }
        assert_eq!(resp.headers()["x-end-to-end"], "retained");
        assert_eq!(
            resp.into_body().collect().await.unwrap().to_bytes(),
            "response body"
        );
    }
    ethonyx.shutdown().await;
    server.abort();
}

#[tokio::test]
async fn health_probes_run_together_and_preserve_partial_failures() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream = format!("http://{}", listener.local_addr().unwrap());
    let barrier = Arc::new(tokio::sync::Barrier::new(3));
    let server = tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let barrier = barrier.clone();
            tokio::spawn(async move {
                let svc = service_fn(move |req: Request<Incoming>| {
                    let barrier = barrier.clone();
                    async move {
                        let path = req.uri().path().to_owned();
                        req.into_body().collect().await.unwrap();
                        // No response is released until all three probes arrive.
                        barrier.wait().await;
                        let (status, body) = match path.as_str() {
                            "/" => (
                                StatusCode::OK,
                                r#"[{"id":3,"error":{"code":-32000,"message":"block unavailable"}},{"id":1,"result":false},{"id":2,"result":"0x40"}]"#,
                            ),
                            "/eth/v1/node/syncing" => {
                                (StatusCode::SERVICE_UNAVAILABLE, "unavailable")
                            }
                            "/eth/v1/node/peer_count" => {
                                (StatusCode::OK, r#"{"data":{"connected":"42"}}"#)
                            }
                            _ => panic!("unexpected probe {path}"),
                        };
                        Ok::<_, Infallible>(
                            Response::builder()
                                .status(status)
                                .body(Full::new(Bytes::from_static(body.as_bytes())))
                                .unwrap(),
                        )
                    }
                });
                let _ = auto::Builder::new(TokioExecutor::new())
                    .serve_connection(TokioIo::new(stream), svc)
                    .await;
            });
        }
    });
    let ethonyx = EthonyxHandle::start(&[
        "--el-http-url",
        &upstream,
        "--cl-beacon-url",
        &upstream,
        "--health-poll-interval",
        "60",
    ])
    .await;
    let (_, body) = get(&client(), &ethonyx.url("/healthz")).await;
    let health: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(health["el"]["syncing"], false, "{health}");
    assert_eq!(health["el"]["peers"], 64, "{health}");
    assert_eq!(health["cl"]["peers"], 42, "{health}");
    assert!(
        health["el"]["errors"][0]
            .as_str()
            .unwrap()
            .contains("block unavailable"),
        "{health}"
    );
    assert!(
        health["cl"]["errors"][0].as_str().unwrap().contains("503"),
        "{health}"
    );
    let (status, _) = get(&client(), &ethonyx.url("/readyz")).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    ethonyx.shutdown().await;
    server.abort();
}

#[tokio::test]
async fn healthz_is_200_even_when_upstreams_fail() {
    // Both mocks return 404 for everything: every probe errors. /healthz reports
    // state, it does not judge it, so it still answers 200 with the errors inline.
    let bad: MockHandler = Arc::new(|_| (StatusCode::NOT_FOUND, b"{}".to_vec()));
    let el = MockServer::start(bad.clone()).await;
    let cl = MockServer::start(bad).await;
    let ethonyx =
        EthonyxHandle::start(&["--el-http-url", &el.url, "--cl-beacon-url", &cl.url]).await;

    let c = client();
    let (status, body) = get(&c, &ethonyx.url("/healthz")).await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    // No verdict field; each failed upstream is recorded under its layer's errors.
    assert!(
        v.get("status").is_none(),
        "healthz must not render a verdict"
    );
    let el_errors = v["el"]["errors"].as_array().expect("el.errors array");
    assert!(
        el_errors
            .iter()
            .any(|e| e.as_str().unwrap().contains("404")),
        "got {v}"
    );
    let cl_errors = v["cl"]["errors"].as_array().expect("cl.errors array");
    assert!(
        cl_errors
            .iter()
            .any(|e| e.as_str().unwrap().contains("404")),
        "got {v}"
    );
    // Transport is reported even when the health probes themselves error: the
    // h1-only mock rejects the h2c probe, so the EL hop falls back to http/1.1.
    assert_eq!(v["el"]["transport"], "http/1.1");
    assert_eq!(v["cl"]["transport"], "http/1.1");

    ethonyx.shutdown().await;
}

#[tokio::test]
async fn el_down_keeps_default_h2c_verdict_and_502s() {
    // EL upstream down: both the h2c probe and the h1 fallback fail at the
    // transport layer, so the poller keeps the default (h2c) verdict instead of
    // flapping to h1. /healthz still answers 200 with the EL error inline and
    // reports the unconfirmed default transport; the data-plane returns 502.
    let dead = pick_port().await; // bound then released -> nothing listening
    let cl = MockServer::start(ok_handler()).await;
    let ethonyx = EthonyxHandle::start(&[
        "--el-http-url",
        &format!("http://127.0.0.1:{dead}"),
        "--cl-beacon-url",
        &cl.url,
    ])
    .await;

    let c = client();
    let (status, body) = get(&c, &ethonyx.url("/healthz")).await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    let el_errors = v["el"]["errors"].as_array().expect("el.errors array");
    assert!(!el_errors.is_empty(), "expected an EL error, got {v}");
    assert_eq!(
        v["el"]["transport"], "h2c",
        "a down EL keeps the default verdict"
    );

    // The data-plane forwards to the (dead) EL and surfaces a 502.
    let (status, _) = post_json(
        &c,
        &ethonyx.url("/"),
        json!({"jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id": 1}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);

    ethonyx.shutdown().await;
}

#[tokio::test]
async fn healthz_is_served_from_cache_not_per_request() {
    // With a long poll interval, only the startup poll hits upstream. Repeated
    // /healthz requests must read the cached snapshot and add no upstream calls.
    let el = MockServer::start(ok_handler()).await;
    let cl = MockServer::start(ok_handler()).await;
    let ethonyx = EthonyxHandle::start(&[
        "--el-http-url",
        &el.url,
        "--cl-beacon-url",
        &cl.url,
        "--health-poll-interval",
        "60",
    ])
    .await;

    let c = client();
    for _ in 0..5 {
        let (status, _) = get(&c, &ethonyx.url("/healthz")).await;
        assert_eq!(status, StatusCode::OK);
    }
    // One startup poll = 3 EL calls (eth_syncing, net_peerCount,
    // eth_getBlockByNumber) + 2 CL calls, regardless of how many probes arrive.
    assert_eq!(el.received().len(), 3, "EL calls: {:?}", el.received());
    assert_eq!(cl.received().len(), 2, "CL calls: {:?}", cl.received());

    ethonyx.shutdown().await;
}

#[tokio::test]
async fn healthz_cache_refreshes_in_background() {
    // The poller must keep refreshing. net_peerCount returns 16 on the first
    // (warm-seed) call and 32 after; with a 1s interval, a read taken past one
    // interval must reflect the background poll, not the seed.
    use std::sync::atomic::{AtomicU64, Ordering};
    let pc = Arc::new(AtomicU64::new(0));
    let el = MockServer::start(Arc::new(move |req| {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or(Value::Null);
        let result = match body.get("method").and_then(Value::as_str).unwrap_or("") {
            "eth_syncing" => json!(false),
            "net_peerCount" => {
                if pc.fetch_add(1, Ordering::SeqCst) == 0 {
                    json!("0x10") // 16, warm seed
                } else {
                    json!("0x20") // 32, later background polls
                }
            }
            "eth_getBlockByNumber" => json!({"number": "0x1", "timestamp": "0x1"}),
            _ => json!(null),
        };
        let resp = json!({"jsonrpc": "2.0", "id": 1, "result": result});
        (StatusCode::OK, serde_json::to_vec(&resp).unwrap())
    }))
    .await;
    let cl = MockServer::start(ok_handler()).await;
    let ethonyx = EthonyxHandle::start(&[
        "--el-http-url",
        &el.url,
        "--cl-beacon-url",
        &cl.url,
        "--health-poll-interval",
        "1",
    ])
    .await;

    let c = client();
    let (_, b1) = get(&c, &ethonyx.url("/healthz")).await;
    let v1: Value = serde_json::from_slice(&b1).unwrap();
    assert_eq!(
        v1["el"]["peers"], 16,
        "first read is the warm-seed poll: {v1}"
    );

    tokio::time::sleep(Duration::from_millis(1500)).await;

    let (_, b2) = get(&c, &ethonyx.url("/healthz")).await;
    let v2: Value = serde_json::from_slice(&b2).unwrap();
    assert_eq!(
        v2["el"]["peers"], 32,
        "background poll should have refreshed the cache: {v2}"
    );

    ethonyx.shutdown().await;
}

#[tokio::test]
async fn run_rejects_zero_poll_interval() {
    let cfg = Config::try_parse_from([
        "ethonyx",
        "--listen",
        "127.0.0.1:0",
        "--health-poll-interval",
        "0",
    ])
    .expect("clap parse");
    let err = run(cfg, async {})
        .await
        .expect_err("zero poll interval must be rejected");
    assert!(
        err.to_string().contains("health-poll-interval"),
        "got: {err}"
    );
}

#[tokio::test]
async fn healthz_reports_numeric_state() {
    let now = now_unix();
    let block_ts_hex = format!("0x{:x}", now - 5);
    // Choose a head_slot that puts the wall-clock age within the default 60s threshold.
    let genesis = 1_606_824_023u64;
    let head_slot = (now.saturating_sub(genesis)) / 12;

    let el = MockServer::start(Arc::new(move |req| {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or(Value::Null);
        let method = body.get("method").and_then(Value::as_str).unwrap_or("");
        let result = match method {
            "eth_syncing" => json!(false),
            "net_peerCount" => json!("0x10"), // 16 peers
            "eth_getBlockByNumber" => json!({
                "number": "0x1234",
                "timestamp": block_ts_hex,
            }),
            _ => json!(null),
        };
        let resp = json!({"jsonrpc": "2.0", "id": 1, "result": result});
        (StatusCode::OK, serde_json::to_vec(&resp).unwrap())
    }))
    .await;

    let cl = MockServer::start(Arc::new(move |req| match req.path.as_str() {
        "/eth/v1/node/syncing" => (
            StatusCode::OK,
            serde_json::to_vec(&json!({
                "data": {
                    "head_slot": head_slot.to_string(),
                    "sync_distance": "0",
                    "is_syncing": false,
                }
            }))
            .unwrap(),
        ),
        "/eth/v1/node/peer_count" => (
            StatusCode::OK,
            serde_json::to_vec(&json!({
                "data": {
                    "disconnected": "0",
                    "connecting": "0",
                    "connected": "100",
                    "disconnecting": "0",
                }
            }))
            .unwrap(),
        ),
        _ => (StatusCode::NOT_FOUND, b"{}".to_vec()),
    }))
    .await;

    let ethonyx = EthonyxHandle::start(&[
        "--el-http-url",
        &el.url,
        "--cl-beacon-url",
        &cl.url,
        "--network",
        "mainnet",
    ])
    .await;

    let c = client();
    let (status, body) = get(&c, &ethonyx.url("/healthz")).await;
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        status,
        StatusCode::OK,
        "expected /healthz 200, got {status}: {v}"
    );
    // Verdict-free: machine-readable numeric fields, no `ok` / `status`.
    assert!(
        v.get("status").is_none(),
        "healthz must not render a verdict"
    );
    assert_eq!(v["el"]["syncing"], false);
    assert_eq!(v["el"]["peers"], 16);
    assert!(v["el"]["block_number"].is_number(), "got {v}");
    assert!(v["el"]["block_age_secs"].is_number(), "got {v}");
    assert_eq!(v["cl"]["syncing"], false);
    assert_eq!(v["cl"]["peers"], 100);
    assert!(v["cl"]["head_slot"].is_number(), "got {v}");
    assert!(v["cl"]["slot_age_secs"].is_number(), "got {v}");

    ethonyx.shutdown().await;
}

#[tokio::test]
async fn readyz_ready_when_synced_even_if_degraded() {
    // Synced EL + CL, but zero peers and a stale block. Default /readyz gates on
    // sync only, so the node stays ready; /healthz still surfaces the degraded
    // numbers. This is the whole point of the split.
    let el = MockServer::start(Arc::new(|req| {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or(Value::Null);
        let method = body.get("method").and_then(Value::as_str).unwrap_or("");
        let result = match method {
            "eth_syncing" => json!(false),
            "net_peerCount" => json!("0x0"),
            "eth_getBlockByNumber" => json!({"number": "0x1", "timestamp": "0x1"}),
            _ => json!(null),
        };
        let resp = json!({"jsonrpc": "2.0", "id": 1, "result": result});
        (StatusCode::OK, serde_json::to_vec(&resp).unwrap())
    }))
    .await;
    let cl = MockServer::start(Arc::new(|req| match req.path.as_str() {
        "/eth/v1/node/syncing" => (
            StatusCode::OK,
            serde_json::to_vec(&json!({
                "data": {"head_slot": "1", "sync_distance": "0", "is_syncing": false}
            }))
            .unwrap(),
        ),
        "/eth/v1/node/peer_count" => (
            StatusCode::OK,
            serde_json::to_vec(&json!({"data": {"connected": "0"}})).unwrap(),
        ),
        _ => (StatusCode::NOT_FOUND, b"{}".to_vec()),
    }))
    .await;
    let ethonyx =
        EthonyxHandle::start(&["--el-http-url", &el.url, "--cl-beacon-url", &cl.url]).await;

    let c = client();
    let (status, body) = get(&c, &ethonyx.url("/readyz")).await;
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(status, StatusCode::OK, "expected ready, got {status}: {v}");
    assert_eq!(v["status"], "ready");
    assert_eq!(v["el_syncing"]["ok"], true);
    assert_eq!(v["cl_syncing"]["ok"], true);
    // Default mode omits freshness/peers entirely.
    assert!(v.get("el_block_fresh").is_none(), "got {v}");
    assert!(v.get("cl_slot_fresh").is_none(), "got {v}");
    assert!(v.get("el_peers").is_none(), "got {v}");

    // /healthz reflects the degraded state without affecting readiness.
    let (hstatus, hbody) = get(&c, &ethonyx.url("/healthz")).await;
    let hv: Value = serde_json::from_slice(&hbody).unwrap();
    assert_eq!(hstatus, StatusCode::OK);
    assert_eq!(hv["el"]["peers"], 0);
    assert_eq!(hv["cl"]["peers"], 0);

    ethonyx.shutdown().await;
}

#[tokio::test]
async fn readyz_not_ready_when_el_syncing() {
    let el = MockServer::start(Arc::new(|req| {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or(Value::Null);
        let method = body.get("method").and_then(Value::as_str).unwrap_or("");
        let result = match method {
            "eth_syncing" => json!({"currentBlock": "0x10", "highestBlock": "0x20"}),
            _ => json!(null),
        };
        let resp = json!({"jsonrpc": "2.0", "id": 1, "result": result});
        (StatusCode::OK, serde_json::to_vec(&resp).unwrap())
    }))
    .await;
    let cl = MockServer::start(Arc::new(|req| match req.path.as_str() {
        "/eth/v1/node/syncing" => (
            StatusCode::OK,
            serde_json::to_vec(&json!({
                "data": {"head_slot": "100", "sync_distance": "0", "is_syncing": false}
            }))
            .unwrap(),
        ),
        _ => (StatusCode::NOT_FOUND, b"{}".to_vec()),
    }))
    .await;
    let ethonyx =
        EthonyxHandle::start(&["--el-http-url", &el.url, "--cl-beacon-url", &cl.url]).await;

    let c = client();
    let (status, body) = get(&c, &ethonyx.url("/readyz")).await;
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "got {v}");
    assert_eq!(v["status"], "not_ready");
    assert_eq!(v["el_syncing"]["ok"], false);
    assert_eq!(v["el_syncing"]["detail"], "syncing (block 16, distance 16)");

    ethonyx.shutdown().await;
}

#[tokio::test]
async fn readyz_strict_gates_on_freshness() {
    // Synced, but the latest block is ancient. Default /readyz would be ready;
    // --readyz-strict pulls it because freshness fails.
    let el = MockServer::start(Arc::new(|req| {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or(Value::Null);
        let method = body.get("method").and_then(Value::as_str).unwrap_or("");
        let result = match method {
            "eth_syncing" => json!(false),
            "eth_getBlockByNumber" => json!({"number": "0x5", "timestamp": "0x1"}),
            _ => json!(null),
        };
        let resp = json!({"jsonrpc": "2.0", "id": 1, "result": result});
        (StatusCode::OK, serde_json::to_vec(&resp).unwrap())
    }))
    .await;
    let cl = MockServer::start(Arc::new(|req| match req.path.as_str() {
        "/eth/v1/node/syncing" => (
            StatusCode::OK,
            serde_json::to_vec(&json!({
                "data": {"head_slot": "1", "sync_distance": "0", "is_syncing": false}
            }))
            .unwrap(),
        ),
        _ => (StatusCode::NOT_FOUND, b"{}".to_vec()),
    }))
    .await;
    let ethonyx = EthonyxHandle::start(&[
        "--el-http-url",
        &el.url,
        "--cl-beacon-url",
        &cl.url,
        "--readyz-strict",
    ])
    .await;

    let c = client();
    let (status, body) = get(&c, &ethonyx.url("/readyz")).await;
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "got {v}");
    assert_eq!(v["status"], "not_ready");
    // Sync is fine; freshness is what fails, and it is now present in the report.
    assert_eq!(v["el_syncing"]["ok"], true);
    assert_eq!(v["el_block_fresh"]["ok"], false);

    ethonyx.shutdown().await;
}

#[tokio::test]
async fn multi_port_listen_serves_same_routes() {
    let el = MockServer::start(ok_handler()).await;
    let cl = MockServer::start(ok_handler()).await;

    let p1 = pick_port().await;
    let p2 = pick_port().await;
    let cfg = Config::try_parse_from([
        "ethonyx",
        "--listen",
        &format!("127.0.0.1:{p1}"),
        "--listen",
        &format!("127.0.0.1:{p2}"),
        "--el-http-url",
        &el.url,
        "--cl-beacon-url",
        &cl.url,
        "--shutdown-grace",
        "0",
    ])
    .unwrap();

    let (tx, rx) = oneshot::channel::<()>();
    let task = tokio::spawn(run(cfg, async move {
        let _ = rx.await;
    }));
    wait_for_port(p1).await;
    wait_for_port(p2).await;

    let c = client();
    for port in [p1, p2] {
        let (status, body) = get(&c, &format!("http://127.0.0.1:{port}/livez")).await;
        assert_eq!(status, StatusCode::OK, "port {port}");
        assert_eq!(&body[..], b"ok", "port {port}");
    }

    let _ = tx.send(());
    let _ = task.await;
}

// ---------- websocket ----------

/// Minimal WebSocket echo upstream. Returns its `ws://addr` URL; echoes every
/// text/binary frame straight back.
async fn ws_echo_upstream() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                let Ok(ws) = accept_async(stream).await else {
                    return;
                };
                let (mut tx, mut rx) = ws.split();
                while let Some(Ok(msg)) = rx.next().await {
                    if (msg.is_text() || msg.is_binary()) && tx.send(msg).await.is_err() {
                        break;
                    }
                }
            });
        }
    });
    format!("ws://{addr}")
}

#[tokio::test]
async fn ws_handshake_fails_with_502_when_upstream_down() {
    // Nothing listens on the WS upstream. ethonyx dials upstream *before*
    // completing the client handshake, so the client gets a 502 on the handshake
    // rather than a 101 followed by an immediate abnormal close.
    let el = MockServer::start(ok_handler()).await;
    let cl = MockServer::start(ok_handler()).await;
    let dead = pick_port().await; // bound then released -> nothing listening
    let ethonyx = EthonyxHandle::start(&[
        "--el-http-url",
        &el.url,
        "--cl-beacon-url",
        &cl.url,
        "--el-ws-url",
        &format!("ws://127.0.0.1:{dead}"),
    ])
    .await;

    let url = format!("ws://127.0.0.1:{}/", ethonyx.port);
    match connect_async(&url).await {
        Err(WsError::Http(resp)) => assert_eq!(resp.status(), StatusCode::BAD_GATEWAY),
        other => panic!("expected a 502 handshake error, got {other:?}"),
    }

    ethonyx.shutdown().await;
}

#[tokio::test]
async fn ws_bridge_echoes_through_upstream() {
    // With the upstream up, the reordered handshake still establishes and the
    // bidirectional bridge forwards frames.
    let el = MockServer::start(ok_handler()).await;
    let cl = MockServer::start(ok_handler()).await;
    let ws_upstream = ws_echo_upstream().await;
    let ethonyx = EthonyxHandle::start(&[
        "--el-http-url",
        &el.url,
        "--cl-beacon-url",
        &cl.url,
        "--el-ws-url",
        &ws_upstream,
    ])
    .await;

    let url = format!("ws://127.0.0.1:{}/", ethonyx.port);
    let (mut ws, _resp) = connect_async(&url).await.expect("handshake should succeed");
    ws.send(Message::Text("ping".into())).await.unwrap();
    let echoed = loop {
        match ws.next().await.expect("a reply").expect("ok frame") {
            Message::Text(t) => break t,
            _ => continue,
        }
    };
    assert_eq!(echoed.as_str(), "ping");

    ethonyx.shutdown().await;
}

#[tokio::test]
async fn http2_extended_connect_ws_502_when_upstream_down() {
    // The h2 path dials the upstream first too: a dead upstream surfaces as a 502
    // on the CONNECT response, not an accepted-then-dropped tunnel.
    let el = MockServer::start(ok_handler()).await;
    let cl = MockServer::start(ok_handler()).await;
    let dead = pick_port().await;
    let ethonyx = EthonyxHandle::start(&[
        "--el-http-url",
        &el.url,
        "--cl-beacon-url",
        &cl.url,
        "--el-ws-url",
        &format!("ws://127.0.0.1:{dead}"),
    ])
    .await;

    let tcp = tokio::net::TcpStream::connect(("127.0.0.1", ethonyx.port))
        .await
        .unwrap();
    let (mut sender, conn) = hyper::client::conn::http2::Builder::new(TokioExecutor::new())
        .handshake::<_, TestBody>(TokioIo::new(tcp))
        .await
        .expect("h2 handshake");
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let mut req = Request::builder()
        .method(Method::CONNECT)
        .header("sec-websocket-version", "13")
        .uri(format!("http://127.0.0.1:{}/", ethonyx.port))
        .body(Full::<Bytes>::new(Bytes::new()))
        .unwrap();
    req.extensions_mut()
        .insert(hyper::ext::Protocol::from_static("websocket"));

    let resp = sender.send_request(req).await.expect("extended CONNECT");
    assert_eq!(
        resp.status(),
        StatusCode::BAD_GATEWAY,
        "dead upstream → 502"
    );

    ethonyx.shutdown().await;
}

#[tokio::test]
async fn http2_extended_connect_ws_bridges_to_upstream() {
    // RFC 8441 Extended CONNECT: an h2 client opens a WebSocket via
    // `:method=CONNECT, :protocol=websocket`. ethonyx terminates the h2 stream and
    // bridges the RFC 6455 frames to the upstream h1 WebSocket (h2 ws → h1 ws).
    // Driven by a hand-rolled h2 client because tokio/hyper-tungstenite are h1-only.
    let el = MockServer::start(ok_handler()).await;
    let cl = MockServer::start(ok_handler()).await;
    let ws_upstream = ws_echo_upstream().await;
    let ethonyx = EthonyxHandle::start(&[
        "--el-http-url",
        &el.url,
        "--cl-beacon-url",
        &cl.url,
        "--el-ws-url",
        &ws_upstream,
    ])
    .await;

    // Cleartext h2 (prior-knowledge) connection to ethonyx.
    let tcp = tokio::net::TcpStream::connect(("127.0.0.1", ethonyx.port))
        .await
        .unwrap();
    let (mut sender, conn) = hyper::client::conn::http2::Builder::new(TokioExecutor::new())
        .handshake::<_, TestBody>(TokioIo::new(tcp))
        .await
        .expect("h2 handshake");
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let mut sockets = Vec::new();
    for path in ["/", "/healthz", "/readyz", "/livez", "/metrics"] {
        let mut req = Request::builder()
            .method(Method::CONNECT)
            .header("sec-websocket-version", "13")
            .uri(format!("http://127.0.0.1:{}{path}", ethonyx.port))
            .body(Full::<Bytes>::new(Bytes::new()))
            .unwrap();
        req.extensions_mut()
            .insert(hyper::ext::Protocol::from_static("websocket"));

        let resp = sender.send_request(req).await.expect("extended CONNECT");
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "extended CONNECT should be 200"
        );
        assert_eq!(resp.version(), Version::HTTP_2);

        let upgraded = hyper::upgrade::on(resp).await.expect("upgrade");
        let mut ws = tokio_tungstenite::WebSocketStream::from_raw_socket(
            TokioIo::new(upgraded),
            tokio_tungstenite::tungstenite::protocol::Role::Client,
            None,
        )
        .await;
        ws.send(Message::Text("ping".into())).await.unwrap();
        let echoed = loop {
            match ws.next().await.expect("a reply").expect("ok frame") {
                Message::Text(t) => break t,
                _ => continue,
            }
        };
        assert_eq!(echoed.as_str(), "ping");

        sockets.push(ws);
    }

    ethonyx.shutdown().await;
    for mut ws in sockets {
        let closed = tokio::time::timeout(Duration::from_secs(1), ws.next())
            .await
            .expect("shutdown must release an HTTP/2 upgraded stream");
        assert!(matches!(
            closed,
            None | Some(Err(_)) | Some(Ok(Message::Close(_)))
        ));
    }
}

#[tokio::test]
async fn el_upstream_h2c_timeout_falls_back_to_h1() {
    // If the EL upstream hangs on h2c connection preface (causing a timeout)
    // rather than closing it, ethonyx should fallback to HTTP/1.1 if HTTP/1.1 works.
    let el = MockServer::start_hanging_h2c(Arc::new(|req| {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or(Value::Null);
        let method = body.get("method").and_then(Value::as_str).unwrap_or("");
        let result = match method {
            "eth_syncing" => json!(false),
            "net_peerCount" => json!("0x0"),
            "eth_getBlockByNumber" => json!({"number": "0x1", "timestamp": "0x1"}),
            _ => json!(null),
        };
        let resp = json!({"jsonrpc": "2.0", "id": 1, "result": result});
        (StatusCode::OK, serde_json::to_vec(&resp).unwrap())
    }))
    .await;

    let cl = MockServer::start(Arc::new(|req| match req.path.as_str() {
        "/eth/v1/node/syncing" => (
            StatusCode::OK,
            serde_json::to_vec(&json!({
                "data": {"head_slot": "1", "sync_distance": "0", "is_syncing": false}
            }))
            .unwrap(),
        ),
        "/eth/v1/node/peer_count" => (
            StatusCode::OK,
            serde_json::to_vec(&json!({"data": {"connected": "0"}})).unwrap(),
        ),
        _ => (StatusCode::NOT_FOUND, b"{}".to_vec()),
    }))
    .await;

    // Start ethonyx. By default, it warms up cache on start.
    // If fallback works, starting will succeed because the warm-up succeeds.
    let ethonyx =
        EthonyxHandle::start(&["--el-http-url", &el.url, "--cl-beacon-url", &cl.url]).await;

    let c = client();
    let (status, body) = get(&c, &ethonyx.url("/readyz")).await;
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(status, StatusCode::OK, "expected ready, got {status}: {v}");
    assert_eq!(v["status"], "ready");

    // Let's also check /healthz report
    let (_, hz) = get(&c, &ethonyx.url("/healthz")).await;
    let hv: Value = serde_json::from_slice(&hz).unwrap();
    assert_eq!(hv["el"]["transport"], "http/1.1"); // verified fallback to HTTP/1.1!

    ethonyx.shutdown().await;
}

/// WebSocket upstream that verifies specific request properties (path, query, headers)
/// during handshake and then echoes.
async fn ws_verifying_upstream(
    expected_path: &'static str,
    expected_headers: Vec<(String, String)>,
    tx_verified: oneshot::Sender<bool>,
) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        // We only expect one connection in this test.
        if let Ok((stream, _)) = listener.accept().await {
            let expected_headers = expected_headers.clone();
            let verified = Arc::new(std::sync::atomic::AtomicBool::new(true));
            let verified_cb = Arc::clone(&verified);

            let ws_result = tokio_tungstenite::accept_hdr_async(
                stream,
                #[allow(clippy::result_large_err)]
                move |req: &tokio_tungstenite::tungstenite::handshake::server::Request, resp: tokio_tungstenite::tungstenite::handshake::server::Response| {
                    let actual_path = req.uri().path_and_query().map(|pq| pq.as_str()).unwrap_or("/");
                    if actual_path != expected_path {
                        verified_cb.store(false, std::sync::atomic::Ordering::SeqCst);
                    }
                    for (k, v) in &expected_headers {
                        if req.headers().get(k).and_then(|val| val.to_str().ok()) != Some(v) {
                            verified_cb.store(false, std::sync::atomic::Ordering::SeqCst);
                        }
                    }
                    Ok(resp)
                },
            )
            .await;

            let _ = tx_verified.send(verified.load(std::sync::atomic::Ordering::SeqCst));

            if let Ok(ws) = ws_result {
                let (mut tx, mut rx) = ws.split();
                while let Some(Ok(msg)) = rx.next().await {
                    if (msg.is_text() || msg.is_binary()) && tx.send(msg).await.is_err() {
                        break;
                    }
                }
            }
        }
    });
    format!("ws://{addr}")
}

#[tokio::test]
async fn ws_forwarding_preserves_path_query_and_headers() {
    // Tests that WebSocket proxy relays custom headers (like authorization)
    // and path/queries from client request to the upstream WebSocket correctly.
    let el = MockServer::start(ok_handler()).await;
    let cl = MockServer::start(ok_handler()).await;

    let (tx_verified, rx_verified) = oneshot::channel::<bool>();

    let ws_upstream = ws_verifying_upstream(
        "/foo/bar?baz=qux",
        vec![
            (
                "authorization".to_string(),
                "Bearer secret-token".to_string(),
            ),
            ("x-custom-header".to_string(), "hello-world".to_string()),
        ],
        tx_verified,
    )
    .await;

    let ethonyx = EthonyxHandle::start(&[
        "--el-http-url",
        &el.url,
        "--cl-beacon-url",
        &cl.url,
        "--el-ws-url",
        &ws_upstream,
    ])
    .await;

    // Connect using tungstenite Request with custom path/query and headers.
    let target_url = format!("ws://127.0.0.1:{}/foo/bar?baz=qux", ethonyx.port);
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    let mut req = target_url.into_client_request().unwrap();
    req.headers_mut().insert(
        "authorization",
        http::HeaderValue::from_static("Bearer secret-token"),
    );
    req.headers_mut().insert(
        "x-custom-header",
        http::HeaderValue::from_static("hello-world"),
    );

    let (mut ws, _resp) = connect_async(req).await.expect("handshake should succeed");

    // Send a message and wait for it to be echoed to ensure connection works
    ws.send(Message::Text("ping".into())).await.unwrap();
    let echoed = loop {
        match ws.next().await.expect("a reply").expect("ok frame") {
            Message::Text(t) => break t,
            _ => continue,
        }
    };
    assert_eq!(echoed.as_str(), "ping");

    // Check verification result
    let is_verified = rx_verified.await.unwrap_or(false);
    assert!(
        is_verified,
        "Upstream did not receive the expected path, query, or headers"
    );

    ethonyx.shutdown().await;
}

#[tokio::test]
async fn metrics_endpoint_returns_formatted_prometheus_metrics() {
    let el = MockServer::start(ok_handler()).await;
    let cl = MockServer::start(ok_handler()).await;
    let ethonyx =
        EthonyxHandle::start(&["--el-http-url", &el.url, "--cl-beacon-url", &cl.url]).await;

    let c = client();

    // Fire a quick request to record some request count metrics
    let (status, _) = post_json(
        &c,
        &ethonyx.url("/"),
        json!({"jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id": 1}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // Fetch metrics
    let (status, body) = get(&c, &ethonyx.url("/metrics")).await;
    assert_eq!(status, StatusCode::OK);

    let metrics_str = String::from_utf8_lossy(&body);
    // Assert some of the metrics exist in the response
    assert!(metrics_str.contains("ethonyx_proxy_requests_total"));
    assert!(metrics_str.contains("ethonyx_proxy_request_duration_seconds"));
    assert!(metrics_str.contains(r#"ethonyx_active_connections{protocol="tcp"}"#));
    assert!(metrics_str.contains("ethonyx_upstream_health_status"));
    assert!(metrics_str.contains("ethonyx_upstream_peers"));

    let request_count = |text: &str| -> f64 {
        text.lines()
            .filter(|line| line.starts_with("ethonyx_proxy_requests_total{"))
            .filter(|line| line.contains("method=\"POST\""))
            .map(|line| line.rsplit_once(' ').unwrap().1.parse::<f64>().unwrap())
            .sum()
    };
    let before = request_count(&metrics_str);
    ethonyx.shutdown().await;

    // A second run in the same runtime reuses the process-global recorder.
    let second = EthonyxHandle::start(&["--trust-upstream", "--mock-eth-syncing"]).await;
    let (status, _) = post_json(
        &c,
        &second.url("/"),
        json!({"jsonrpc":"2.0","method":"eth_syncing","id":1}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (_, body) = get(&c, &second.url("/metrics")).await;
    assert!(request_count(&String::from_utf8_lossy(&body)) >= before + 1.0);
    second.shutdown().await;
}

#[tokio::test]
async fn test_cl_node_health_intercept_and_passthrough() {
    let el = MockServer::start(Arc::new(|req| {
        if req.body.windows(11).any(|w| w == b"eth_syncing") {
            (
                StatusCode::OK,
                br#"{"jsonrpc":"2.0","id":1,"result":false}"#.to_vec(),
            )
        } else {
            (StatusCode::OK, b"{}".to_vec())
        }
    }))
    .await;

    let cl_mock_synced = MockServer::start(Arc::new(|req| {
        if req.path == "/eth/v1/node/syncing" {
            (
                StatusCode::OK,
                br#"{"data":{"is_syncing":false,"sync_distance":"0","head_slot":"100"}}"#.to_vec(),
            )
        } else {
            (
                StatusCode::BAD_REQUEST,
                b"{\"error\": \"Unsupported method\"}".to_vec(),
            )
        }
    }))
    .await;

    // 1. Default (mock_cl_node_health = false): GET /eth/v1/node/health is proxied (bypassed) to CL upstream
    let ethonyx_default = EthonyxHandle::start(&[
        "--el-http-url",
        &el.url,
        "--cl-beacon-url",
        &cl_mock_synced.url,
    ])
    .await;

    let c = client();
    let (status_def, body_def) = get(&c, &ethonyx_default.url("/eth/v1/node/health")).await;
    assert_eq!(status_def, StatusCode::BAD_REQUEST);
    assert_eq!(body_def.as_ref(), b"{\"error\": \"Unsupported method\"}");

    ethonyx_default.shutdown().await;

    // 2. Intercept enabled (--mock-cl-node-health): GET /eth/v1/node/health is intercepted locally
    let ethonyx_mock = EthonyxHandle::start(&[
        "--el-http-url",
        &el.url,
        "--cl-beacon-url",
        &cl_mock_synced.url,
        "--mock-cl-node-health",
    ])
    .await;

    let (status_mock, body_mock) = get(&c, &ethonyx_mock.url("/eth/v1/node/health")).await;
    assert_eq!(status_mock, StatusCode::OK);
    assert!(body_mock.is_empty());

    ethonyx_mock.shutdown().await;

    // 3. Trusted upstream (--trust-upstream without --mock-cl-node-health): /readyz is 200, node/health is bypassed
    let ethonyx_trust = EthonyxHandle::start(&[
        "--el-http-url",
        &el.url,
        "--cl-beacon-url",
        &cl_mock_synced.url,
        "--trust-upstream",
    ])
    .await;

    let (status_tr, body_tr) = get(&c, &ethonyx_trust.url("/readyz")).await;
    assert_eq!(status_tr, StatusCode::OK);
    let ready_val: Value = serde_json::from_slice(&body_tr).unwrap();
    assert_eq!(ready_val["status"], "ready");

    let (status_tr_nh, body_tr_nh) = get(&c, &ethonyx_trust.url("/eth/v1/node/health")).await;
    assert_eq!(status_tr_nh, StatusCode::BAD_REQUEST);
    assert_eq!(body_tr_nh.as_ref(), b"{\"error\": \"Unsupported method\"}");

    ethonyx_trust.shutdown().await;
}

#[cfg(feature = "otel")]
#[tokio::test]
async fn trace_context_is_propagated_to_upstream() {
    let el = MockServer::start(ok_handler()).await;
    let cl = MockServer::start(ok_handler()).await;
    let ethonyx = EthonyxHandle::start(&[
        "--el-http-url",
        &el.url,
        "--cl-beacon-url",
        &cl.url,
        "--otel-endpoint",
        "http://127.0.0.1:4318",
    ])
    .await;

    let c = client();

    let traceparent_val = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    let req = Request::builder()
        .method(Method::POST)
        .uri(ethonyx.url("/"))
        .header("traceparent", traceparent_val)
        .body(Full::new(Bytes::from(
            json!({"jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id": 1})
                .to_string(),
        )))
        .unwrap();

    let resp = c.request(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let received_traceparent_str = {
        let recorded_reqs = el.recorded.lock().unwrap();
        let received_traceparent = recorded_reqs
            .iter()
            .filter_map(|r| r.headers.get("traceparent"))
            .next()
            .expect("traceparent header in any upstream request");
        received_traceparent.to_str().unwrap().to_owned()
    };

    assert!(
        received_traceparent_str.contains("4bf92f3577b34da6a3ce929d0e0e4736"),
        "Expected propagated traceparent to contain trace ID, got: {}",
        received_traceparent_str
    );

    ethonyx.shutdown().await;
}

#[tokio::test]
async fn trust_upstream_decoupled_from_mock_eth_syncing() {
    let el_ws_port = pick_port().await;
    let ws_listener = TcpListener::bind(("127.0.0.1", el_ws_port)).await.unwrap();
    let ws_upstream = format!("ws://127.0.0.1:{el_ws_port}");

    tokio::spawn(async move {
        while let Ok((stream, _)) = ws_listener.accept().await {
            if let Ok(ws) = accept_async(stream).await {
                let (mut tx, mut rx) = ws.split();
                while let Some(Ok(msg)) = rx.next().await {
                    if tx.send(msg).await.is_err() {
                        break;
                    }
                }
            }
        }
    });

    let el = MockServer::start_h2c(Arc::new(|req| {
        if req.body.windows(11).any(|w| w == b"eth_syncing") {
            (
                StatusCode::OK,
                br#"{"jsonrpc":"2.0","id":55,"result":"upstream_result"}"#.to_vec(),
            )
        } else {
            (StatusCode::OK, b"{}".to_vec())
        }
    }))
    .await;
    let cl = MockServer::start(ok_handler()).await;

    // 1. --trust-upstream alone (without --mock-eth-syncing): eth_syncing is proxied (bypassed) to EL upstream
    let ethonyx_trust = EthonyxHandle::start(&[
        "--el-http-url",
        &el.url,
        "--cl-beacon-url",
        &cl.url,
        "--el-ws-url",
        &ws_upstream,
        "--trust-upstream",
    ])
    .await;

    let c = client();
    let (status, body) = post_json_settled(
        &c,
        &ethonyx_trust.url("/"),
        json!({"jsonrpc": "2.0", "method": "eth_syncing", "params": [], "id": 55}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let val: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(val["result"], "upstream_result");

    ethonyx_trust.shutdown().await;

    // 2. --mock-eth-syncing enabled: eth_syncing is intercepted locally
    let ethonyx_mock = EthonyxHandle::start(&[
        "--el-http-url",
        &el.url,
        "--cl-beacon-url",
        &cl.url,
        "--el-ws-url",
        &ws_upstream,
        "--mock-eth-syncing",
    ])
    .await;

    let (status_m, body_m) = post_json_settled(
        &c,
        &ethonyx_mock.url("/"),
        json!({"jsonrpc": "2.0", "method": "eth_syncing", "params": [], "id": 55}),
    )
    .await;
    assert_eq!(status_m, StatusCode::OK);
    let val_m: Value = serde_json::from_slice(&body_m).unwrap();
    assert_eq!(val_m["result"], false);

    ethonyx_mock.shutdown().await;
}

#[tokio::test]
async fn test_cl_mev_relay_routing() {
    let el = MockServer::start(Arc::new(|_| (StatusCode::OK, b"{}".to_vec()))).await;
    let cl = MockServer::start(Arc::new(|_| {
        (
            StatusCode::BAD_REQUEST,
            b"{\"error\": \"Unsupported method\"}".to_vec(),
        )
    }))
    .await;
    let mev_relay = MockServer::start(Arc::new(|req| {
        if req.path == "/eth/v1/builder/validators" && req.method == "POST" {
            (StatusCode::OK, b"[]".to_vec())
        } else {
            (StatusCode::NOT_FOUND, b"not found".to_vec())
        }
    }))
    .await;

    let ethonyx = EthonyxHandle::start(&[
        "--el-http-url",
        &el.url,
        "--cl-beacon-url",
        &cl.url,
        "--cl-mev-relay-url",
        &mev_relay.url,
    ])
    .await;

    let c = client();
    let (status, body) = post_json(
        &c,
        &ethonyx.url("/eth/v1/validator/register_validator"),
        json!([]),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.as_ref(), b"[]");

    ethonyx.shutdown().await;
}

#[tokio::test]
async fn test_mock_cl_subscriptions() {
    let el = MockServer::start(Arc::new(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            b"should not be called".to_vec(),
        )
    }))
    .await;
    let cl = MockServer::start(Arc::new(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            b"should not be called".to_vec(),
        )
    }))
    .await;

    let ethonyx = EthonyxHandle::start(&[
        "--el-http-url",
        &el.url,
        "--cl-beacon-url",
        &cl.url,
        "--mock-cl-subscriptions",
    ])
    .await;

    let c = client();

    for path in [
        "/eth/v1/validator/prepare_beacon_proposer",
        "/eth/v1/validator/beacon_committee_subscriptions",
        "/eth/v1/validator/sync_committee_subscriptions",
    ] {
        let (status, body) = post_json(&c, &ethonyx.url(path), json!([])).await;
        assert_eq!(status, StatusCode::OK, "Failed for path: {path}");
        assert_eq!(
            body.as_ref(),
            b"{\"code\":200,\"message\":\"success\"}",
            "Failed for path: {path}"
        );
    }

    ethonyx.shutdown().await;
}

#[tokio::test]
async fn test_mock_eth_syncing_flag() {
    let el = MockServer::start(Arc::new(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            b"should not be called".to_vec(),
        )
    }))
    .await;

    let ethonyx = EthonyxHandle::start(&["--el-http-url", &el.url, "--mock-eth-syncing"]).await;

    let c = client();
    let (status, body) = post_json(
        &c,
        &ethonyx.url("/"),
        json!({"jsonrpc": "2.0", "id": 1, "method": "eth_syncing", "params": []}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["result"], false);

    ethonyx.shutdown().await;
}

#[tokio::test]
#[allow(clippy::result_large_err)]
async fn test_ws_handshake_preserves_upstream_path_without_trailing_slash() {
    let captured_uri = Arc::new(std::sync::Mutex::new(String::new()));
    let captured_uri_clone = captured_uri.clone();

    let server = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = server.local_addr().unwrap().port();

    tokio::spawn(async move {
        if let Ok((stream, _)) = server.accept().await {
            let captured_uri_clone2 = captured_uri_clone.clone();
            let ws_stream = tokio_tungstenite::accept_hdr_async(
                stream,
                move |req: &tokio_tungstenite::tungstenite::handshake::server::Request, res| {
                    if let Ok(mut guard) = captured_uri_clone2.lock() {
                        *guard = req.uri().to_string();
                    }
                    Ok(res)
                },
            )
            .await;

            if let Ok(mut ws) = ws_stream {
                while let Some(Ok(msg)) = ws.next().await {
                    if msg.is_text() || msg.is_binary() {
                        let _ = ws.send(msg).await;
                    }
                }
            }
        }
    });

    let upstream_url = format!("ws://127.0.0.1:{port}/v2/testkey");
    let ethonyx = EthonyxHandle::start(&["--el-ws-url", &upstream_url, "--trust-upstream"]).await;

    let ws_url = ethonyx.url("/").replace("http://", "ws://");
    let (ws, _) = tokio_tungstenite::connect_async(ws_url).await.unwrap();

    let _ = ws;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let final_uri = captured_uri.lock().unwrap().clone();
    assert_eq!(final_uri, "/v2/testkey");

    ethonyx.shutdown().await;
}

#[tokio::test]
async fn trusted_h1_upstream_and_http_notifications() {
    let el = MockServer::start(ok_handler()).await;
    let proxy = EthonyxHandle::start(&[
        "--trust-upstream",
        "--mock-eth-syncing",
        "--el-http-url",
        &el.url,
    ])
    .await;
    let c = client();
    let (status, _) = post_json(
        &c,
        &proxy.url("/"),
        json!({"jsonrpc":"2.0","method":"eth_call","id":1}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, body) = post_json(
        &c,
        &proxy.url("/"),
        json!({"jsonrpc":"2.0","method":"eth_syncing"}),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert!(body.is_empty());
    let (status, body) = post_json(
        &c,
        &proxy.url("/"),
        json!([
            {"jsonrpc":"2.0","method":"eth_syncing"},
            {"jsonrpc":"2.0","method":"eth_syncing","id":null}
        ]),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap(),
        json!([{"jsonrpc":"2.0","id":null,"result":false}])
    );
    proxy.shutdown().await;
}

#[tokio::test]
async fn mock_body_stall_has_a_deadline() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let proxy = EthonyxHandle::start(&["--trust-upstream", "--mock-eth-syncing"]).await;
    let mut socket = tokio::net::TcpStream::connect(("127.0.0.1", proxy.port))
        .await
        .unwrap();
    socket
        .write_all(b"POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: 100\r\n\r\n{")
        .await
        .unwrap();
    let mut buf = [0; 128];
    let count = tokio::time::timeout(Duration::from_secs(4), socket.read(&mut buf))
        .await
        .unwrap()
        .unwrap();
    assert!(String::from_utf8_lossy(&buf[..count]).starts_with("HTTP/1.1 408"));
    proxy.shutdown().await;
}

#[tokio::test]
#[allow(clippy::result_large_err)] // Tungstenite fixes the callback error type.
async fn websocket_subprotocol_is_negotiated_end_to_end() {
    use tokio_tungstenite::tungstenite::{
        client::IntoClientRequest,
        handshake::server::{Request as WsRequest, Response as WsResponse},
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut ws =
            tokio_tungstenite::accept_hdr_async(socket, |req: &WsRequest, mut res: WsResponse| {
                assert_eq!(req.headers()["sec-websocket-protocol"], "jsonrpc");
                res.headers_mut()
                    .insert("sec-websocket-protocol", "jsonrpc".parse().unwrap());
                Ok(res)
            })
            .await
            .unwrap();
        if let Some(Ok(message)) = ws.next().await {
            ws.send(message).await.unwrap();
        }
    });
    let proxy =
        EthonyxHandle::start(&["--trust-upstream", "--el-ws-url", &format!("ws://{addr}")]).await;
    let mut req = format!("ws://127.0.0.1:{}/", proxy.port)
        .into_client_request()
        .unwrap();
    req.headers_mut()
        .insert("sec-websocket-protocol", "jsonrpc".parse().unwrap());
    let (mut ws, response) = connect_async(req).await.unwrap();
    assert_eq!(response.headers()["sec-websocket-protocol"], "jsonrpc");
    ws.send(Message::Text("hello".into())).await.unwrap();
    assert_eq!(
        ws.next().await.unwrap().unwrap(),
        Message::Text("hello".into())
    );
    server.await.unwrap();
    proxy.shutdown().await;
}

#[tokio::test]
async fn websocket_fragmented_utf8_ping_close_and_mock_batch() {
    use tokio_tungstenite::tungstenite::protocol::frame::{
        Frame,
        coding::{Data, OpCode},
    };
    let upstream = ws_echo_upstream().await;
    for mocking in [false, true] {
        let mock_arg = if mocking { "true" } else { "false" };
        let proxy = EthonyxHandle::start(&[
            "--trust-upstream",
            "--mock-eth-syncing",
            mock_arg,
            "--el-ws-url",
            &upstream,
        ])
        .await;
        let (mut ws, _) = connect_async(format!("ws://127.0.0.1:{}/", proxy.port))
            .await
            .unwrap();
        let text = "split 🌍".as_bytes();
        let split = text.len() - 2;
        ws.send(Message::Frame(Frame::message(
            text[..split].to_vec(),
            OpCode::Data(Data::Text),
            false,
        )))
        .await
        .unwrap();
        ws.send(Message::Frame(Frame::message(
            text[split..].to_vec(),
            OpCode::Data(Data::Continue),
            true,
        )))
        .await
        .unwrap();
        let reply = tokio::time::timeout(Duration::from_secs(2), ws.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(reply, Message::Text("split 🌍".into()));
        if mocking {
            ws.send(Message::Text(
                r#"{"jsonrpc":"2.0","method":"eth_syncing"}"#.into(),
            ))
            .await
            .unwrap();
            ws.send(Message::Text(
                r#"[{"jsonrpc":"2.0","method":"eth_\u0073yncing","id":7}]"#.into(),
            ))
            .await
            .unwrap();
            let response = ws.next().await.unwrap().unwrap().into_text().unwrap();
            assert_eq!(
                serde_json::from_str::<Value>(&response).unwrap(),
                json!([{"jsonrpc":"2.0","id":7,"result":false}])
            );
        }
        ws.send(Message::Ping(b"ping".to_vec().into()))
            .await
            .unwrap();
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(2), ws.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
            Message::Pong(_)
        ));
        ws.close(None).await.unwrap();
        let closed = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                match ws.next().await {
                    Some(Ok(Message::Pong(_))) => continue,
                    other => break other,
                }
            }
        })
        .await
        .unwrap();
        assert!(
            matches!(closed, Some(Ok(Message::Close(_))) | None),
            "{closed:?}"
        );
        proxy.shutdown().await;
    }
}

#[tokio::test]
async fn websocket_rejects_invalid_and_oversized_frames() {
    use tokio::io::AsyncWriteExt;
    let upstream = ws_echo_upstream().await;
    let proxy = EthonyxHandle::start(&["--trust-upstream", "--el-ws-url", &upstream]).await;
    let mut oversized_pong = vec![0x8a, 0xfe, 0, 126, 0, 0, 0, 0];
    oversized_pong.extend_from_slice(&[0; 126]);
    let mut oversized_frame = vec![0x82, 0xff];
    oversized_frame.extend_from_slice(&(16 * 1024 * 1024_u64 + 1).to_be_bytes());
    oversized_frame.extend_from_slice(&[0; 4]);
    for wire in [
        vec![0x81, 1, b'x'],
        oversized_pong,
        oversized_frame,
        vec![0xc1, 0x81, 0, 0, 0, 0, b'x'],
        vec![0x81, 0x81, 0, 0, 0, 0, 0xff],   // invalid UTF-8 text
        vec![0x80, 0x81, 0, 0, 0, 0, b'x'],   // continuation without a start
        vec![0x09, 0x80, 0, 0, 0, 0],         // fragmented control frame
        vec![0x88, 0x82, 0, 0, 0, 0, 3, 237], // forbidden close code 1005
    ] {
        let (mut ws, _) = connect_async(format!("ws://127.0.0.1:{}/", proxy.port))
            .await
            .unwrap();
        ws.get_mut().write_all(&wire).await.unwrap();
        let reply = tokio::time::timeout(Duration::from_secs(3), ws.next())
            .await
            .unwrap();
        assert!(
            matches!(reply, None | Some(Err(_)) | Some(Ok(Message::Close(_)))),
            "{reply:?}"
        );
    }
    proxy.shutdown().await;
}

#[tokio::test]
async fn health_timeout_includes_a_stalled_response_body() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let stalled_url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut buf = [0; 4096];
                let _ = socket.read(&mut buf).await;
                let _ = socket
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 1000\r\n\r\n{")
                    .await;
                // Keep the body incomplete until the client's timeout closes it.
                let _ = socket.read(&mut buf).await;
            });
        }
    });
    let el = MockServer::start_h2c(ok_handler()).await;
    let proxy = tokio::time::timeout(
        Duration::from_secs(4),
        EthonyxHandle::start(&["--el-http-url", &el.url, "--cl-beacon-url", &stalled_url]),
    )
    .await
    .expect("body timeout must let startup finish");
    let (status, body) = get(&client(), &proxy.url("/healthz")).await;
    assert_eq!(status, StatusCode::OK);
    let value: Value = serde_json::from_slice(&body).unwrap();
    assert!(
        value["cl"]["errors"]
            .as_array()
            .unwrap()
            .iter()
            .all(|v| v.as_str().unwrap().contains("timeout")),
        "{value}"
    );
    assert_eq!(
        get(&client(), &proxy.url("/readyz")).await.0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    proxy.shutdown().await;
    server.abort();
}

#[tokio::test]
async fn websocket_rejects_invalid_handshakes_on_both_hops() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let dead = format!("ws://127.0.0.1:{}", pick_port().await);
    let proxy = EthonyxHandle::start(&["--trust-upstream", "--el-ws-url", &dead]).await;
    for headers in [
        "Sec-WebSocket-Version: 12\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n",
        "Sec-WebSocket-Version: 13\r\n", // missing key
    ] {
        let mut socket = tokio::net::TcpStream::connect(("127.0.0.1", proxy.port))
            .await
            .unwrap();
        socket.write_all(format!("GET / HTTP/1.1\r\nHost: localhost\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n{headers}\r\n").as_bytes()).await.unwrap();
        let mut buf = [0; 1024];
        let n = tokio::time::timeout(Duration::from_secs(2), socket.read(&mut buf))
            .await
            .unwrap()
            .unwrap();
        assert!(
            buf[..n].starts_with(b"HTTP/1.1 400"),
            "{}",
            String::from_utf8_lossy(&buf[..n])
        );
    }
    proxy.shutdown().await;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        while !request.ends_with(b"\r\n\r\n") {
            assert!(request.len() < 16 * 1024);
            request.push(socket.read_u8().await.unwrap());
        }
        socket.write_all(b"HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: invalid-signature\r\n\r\n").await.unwrap();
    });
    let proxy = EthonyxHandle::start(&["--trust-upstream", "--el-ws-url", &url]).await;
    let err = connect_async(format!("ws://127.0.0.1:{}/", proxy.port))
        .await
        .unwrap_err();
    assert!(matches!(err, WsError::Http(response) if response.status() == StatusCode::BAD_GATEWAY));
    server.await.unwrap();
    proxy.shutdown().await;
}

#[cfg(all(feature = "otel", unix))]
#[tokio::test]
async fn binary_exports_traces_and_metrics_and_flushes_on_sigterm() {
    use std::process::{Child, Command, Stdio};
    struct KillOnDrop(Child);
    impl Drop for KillOnDrop {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let collector = MockServer::start(ok_handler()).await;
    let upstream = MockServer::start(ok_handler()).await;
    let port = pick_port().await;
    let mut child = KillOnDrop(
        Command::new(env!("CARGO_BIN_EXE_ethonyx"))
            .args([
                "--listen",
                &format!("127.0.0.1:{port}"),
                "--trust-upstream",
                "--el-http-url",
                &upstream.url,
                "--cl-beacon-url",
                &upstream.url,
                "--otel-endpoint",
                &format!("{}/prefix", collector.url),
                "--log-level",
                "warn",
                "--shutdown-grace",
                "1",
            ])
            .env("OTEL_METRIC_EXPORT_INTERVAL", "100")
            .env("OTEL_BSP_SCHEDULE_DELAY", "100")
            .env_remove("RUST_LOG")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    wait_for_port(port).await;
    let c = client();
    let mut exported = false;
    for _ in 0..50 {
        get(&c, &format!("http://127.0.0.1:{port}/eth/test")).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        let requests = collector.recorded.lock().unwrap();
        exported = ["/prefix/v1/traces", "/prefix/v1/metrics"]
            .iter()
            .all(|path| {
                requests
                    .iter()
                    .any(|r| r.path == *path && !r.body.is_empty())
            });
        if exported {
            break;
        }
    }
    assert!(
        exported,
        "both OTLP signals must be exported by the actual binary"
    );
    assert!(
        Command::new("kill")
            .args(["-TERM", &child.0.id().to_string()])
            .status()
            .unwrap()
            .success()
    );
    let status = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(status) = child.0.try_wait().unwrap() {
                break status;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert!(status.success(), "{status}");
}

#[tokio::test]
async fn shutdown_releases_upgraded_websocket_connections() {
    let upstream = ws_echo_upstream().await;
    for mocking in ["false", "true"] {
        let proxy = EthonyxHandle::start(&[
            "--trust-upstream",
            "--mock-eth-syncing",
            mocking,
            "--el-ws-url",
            &upstream,
        ])
        .await;
        let (mut ws, _) = connect_async(format!("ws://127.0.0.1:{}/", proxy.port))
            .await
            .unwrap();
        ws.send(Message::Text("established".into())).await.unwrap();
        assert_eq!(
            ws.next().await.unwrap().unwrap(),
            Message::Text("established".into())
        );
        proxy.shutdown().await;
        let closed = tokio::time::timeout(Duration::from_secs(1), ws.next())
            .await
            .expect("an upgraded socket must not outlive zero-grace shutdown");
        assert!(matches!(
            closed,
            None | Some(Err(_)) | Some(Ok(Message::Close(_)))
        ));
    }
}

#[tokio::test]
async fn shutdown_waits_for_websocket_peer_close() {
    let upstream = ws_echo_upstream().await;
    let mut proxy = EthonyxHandle::start_with_grace(
        &["--trust-upstream", "--el-ws-url", &upstream],
        Duration::from_secs(3),
    )
    .await;
    let (mut ws, _) = connect_async(format!("ws://127.0.0.1:{}/", proxy.port))
        .await
        .unwrap();
    ws.send(Message::Text("established".into())).await.unwrap();
    let _ = ws.next().await.unwrap().unwrap();
    proxy.shutdown.take().unwrap().send(()).unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !proxy.task.as_ref().unwrap().is_finished(),
        "the active relay must remain tracked while draining"
    );
    ws.close(None).await.unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(1), ws.next())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), proxy.shutdown())
        .await
        .expect("peer close should release the drain guard before its deadline");
}

#[tokio::test]
async fn shutdown_bounds_an_unresponsive_websocket_peer() {
    let upstream = ws_echo_upstream().await;
    let proxy = EthonyxHandle::start_with_grace(
        &["--trust-upstream", "--el-ws-url", &upstream],
        Duration::from_secs(1),
    )
    .await;
    let (mut ws, _) = connect_async(format!("ws://127.0.0.1:{}/", proxy.port))
        .await
        .unwrap();
    ws.send(Message::Text("established".into())).await.unwrap();
    let _ = ws.next().await.unwrap().unwrap();
    let start = tokio::time::Instant::now();
    tokio::time::timeout(Duration::from_secs(3), proxy.shutdown())
        .await
        .unwrap();
    assert!(
        start.elapsed() >= Duration::from_millis(800),
        "the relay should receive its configured grace period"
    );
    let closed = tokio::time::timeout(Duration::from_secs(1), ws.next())
        .await
        .unwrap();
    assert!(matches!(
        closed,
        None | Some(Err(_)) | Some(Ok(Message::Close(_)))
    ));
}

#[tokio::test]
async fn shutdown_releases_a_stalled_http_response_body() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream = format!("http://{}", listener.local_addr().unwrap());
    let (closed_tx, closed_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut head = Vec::new();
        let mut buffer = [0; 1024];
        while !head.windows(4).any(|part| part == b"\r\n\r\n") {
            let count = socket.read(&mut buffer).await.unwrap();
            assert_ne!(count, 0);
            head.extend_from_slice(&buffer[..count]);
        }
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 1000\r\n\r\nprefix")
            .await
            .unwrap();
        let end = socket.read(&mut buffer).await;
        assert!(matches!(end, Ok(0) | Err(_)), "{end:?}");
        let _ = closed_tx.send(());
    });
    let proxy = EthonyxHandle::start(&["--trust-upstream", "--cl-beacon-url", &upstream]).await;
    let mut socket = tokio::net::TcpStream::connect(("127.0.0.1", proxy.port))
        .await
        .unwrap();
    socket
        .write_all(b"GET /eth/stream HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();
    let mut response = Vec::new();
    let mut buffer = [0; 1024];
    while !response.ends_with(b"prefix") {
        let count = tokio::time::timeout(Duration::from_secs(2), socket.read(&mut buffer))
            .await
            .unwrap()
            .unwrap();
        assert_ne!(count, 0);
        response.extend_from_slice(&buffer[..count]);
    }
    proxy.shutdown().await;
    let end = tokio::time::timeout(Duration::from_secs(1), socket.read(&mut buffer))
        .await
        .expect("zero-grace shutdown must release the downstream body");
    assert!(matches!(end, Ok(0) | Err(_)), "{end:?}");
    tokio::time::timeout(Duration::from_secs(1), closed_rx)
        .await
        .unwrap()
        .unwrap();
    server.await.unwrap();
}

#[tokio::test]
#[ignore = "sustained connection churn; set ETHONYX_STRESS_SECONDS and run explicitly"]
async fn websocket_sustained_churn_preserves_replies_and_releases_upstreams() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Active(Arc<AtomicUsize>);
    impl Drop for Active {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::Relaxed);
        }
    }
    let seconds = std::env::var("ETHONYX_STRESS_SECONDS")
        .map(|value| value.parse::<u64>().expect("integer stress duration"))
        .unwrap_or(30);
    assert!((1..=600).contains(&seconds));
    for mocking in [false, true] {
        let active = Arc::new(AtomicUsize::new(0));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream = format!("ws://{}", listener.local_addr().unwrap());
        let connections = active.clone();
        let server = tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                let connections = connections.clone();
                tokio::spawn(async move {
                    connections.fetch_add(1, Ordering::Relaxed);
                    let _active = Active(connections);
                    let Ok(mut ws) = accept_async(socket).await else {
                        return;
                    };
                    while let Some(Ok(message)) = ws.next().await {
                        if (message.is_text() || message.is_binary())
                            && ws.send(message).await.is_err()
                        {
                            break;
                        }
                    }
                });
            }
        });
        let proxy = EthonyxHandle::start(&[
            "--trust-upstream",
            "--mock-eth-syncing",
            if mocking { "true" } else { "false" },
            "--el-ws-url",
            &upstream,
        ])
        .await;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(seconds);
        let mut workers = tokio::task::JoinSet::new();
        for worker in 0..16 {
            let url = format!("ws://127.0.0.1:{}/", proxy.port);
            workers.spawn(async move {
                let payload = Message::Binary(vec![worker as u8; 4096].into());
                let mut operations = 0;
                let mut round = 0;
                while tokio::time::Instant::now() < deadline {
                    let (mut ws, _) = connect_async(&url).await.unwrap();
                    for request in 0..32 {
                        if mocking && request % 3 == 2 {
                            ws.send(Message::Text(r#"{"jsonrpc":"2.0","method":"eth_syncing"}"#.into())).await.unwrap();
                        }
                        let message = match request % 3 {
                            0 => payload.clone(),
                            1 => Message::Text(r#"{"jsonrpc":"2.0","method":"eth_syncing","id":7}"#.into()),
                            _ => Message::Text(r#"{"jsonrpc":"2.0","method":"eth_call","params":{"tag":"eth_syncing"},"id":2}"#.into()),
                        };
                        ws.send(message.clone()).await.unwrap();
                        let reply = tokio::time::timeout(Duration::from_secs(5), ws.next()).await.unwrap().unwrap().unwrap();
                        if mocking && request % 3 == 1 {
                            assert_eq!(serde_json::from_str::<Value>(reply.to_text().unwrap()).unwrap(), json!({"jsonrpc":"2.0","id":7,"result":false}));
                        } else {
                            assert_eq!(reply, message);
                        }
                        operations += 1;
                    }
                    // Alternate orderly close and abrupt disconnect, including
                    // peers that leave the server to finish its bounded drain.
                    if round % 2 == 0 {
                        ws.close(None).await.unwrap();
                        let _ = tokio::time::timeout(Duration::from_secs(3), ws.next()).await.unwrap();
                    }
                    drop(ws);
                    round += 1;
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                operations
            });
        }
        let operations = tokio::time::timeout(Duration::from_secs(seconds + 15), async {
            let mut operations = 0;
            while let Some(result) = workers.join_next().await {
                operations += result.unwrap();
            }
            operations
        })
        .await
        .expect("churn workers must finish within the overall deadline");
        assert!(operations >= 16 * 32);
        tokio::time::timeout(Duration::from_secs(5), async {
            while active.load(Ordering::Relaxed) != 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("all upstream relay tasks must be released after client churn");
        assert_eq!(get(&client(), &proxy.url("/livez")).await.0, StatusCode::OK);
        proxy.shutdown().await;
        server.abort();
        println!(
            "soak mock={mocking} operations={operations} active_upstreams=0 duration_secs={seconds}"
        );
    }
}

#[tokio::test]
async fn http_forwarding_preserves_configured_uri_and_unmodified_body() {
    let payloads: &[&[u8]] = &[
        br#"{"jsonrpc":"2.0","method":"eth_blockNumber","id":7}"#,
        br#"{"jsonrpc":"2.0","method":"eth_call","params":{"method":"eth_syncing"},"id":null}"#,
        b"malformed JSON \xff",
    ];
    let el = MockServer::start(Arc::new(|req| (StatusCode::OK, req.body.to_vec()))).await;
    let cl = MockServer::start(ok_handler()).await;
    let paths = ["/", "/?tag=a%2Fb", "/rpc/extra?tag=a%2Fb"];
    for (base_path, expected_pqs) in [
        ("", ["/", "/?tag=a%2Fb", "/rpc/extra?tag=a%2Fb"]),
        ("/", ["/", "/?tag=a%2Fb", "/rpc/extra?tag=a%2Fb"]),
        (
            "/rpc/",
            ["/rpc/", "/rpc/?tag=a%2Fb", "/rpc/rpc/extra?tag=a%2Fb"],
        ),
        (
            "/rpc?key=a%2Fb",
            [
                "/rpc?key=a%2Fb",
                "/rpc?key=a%2Fb&tag=a%2Fb",
                "/rpc/rpc/extra?key=a%2Fb&tag=a%2Fb",
            ],
        ),
    ] {
        for mock_enabled in [false, true] {
            let upstream = format!("{}{base_path}", el.url);
            let mut args = vec![
                "--el-http-url",
                &upstream,
                "--cl-beacon-url",
                &cl.url,
                "--trust-upstream",
            ];
            if mock_enabled {
                args.push("--mock-eth-syncing");
            }
            let proxy = EthonyxHandle::start(&args).await;
            for c in [client(), h2c_client()] {
                for (path, expected_pq) in paths.into_iter().zip(expected_pqs) {
                    for &payload in payloads {
                        let request = Request::builder()
                            .method(Method::POST)
                            .uri(proxy.url(path))
                            .body(Full::new(Bytes::copy_from_slice(payload)))
                            .unwrap();
                        let response = c.request(request).await.unwrap();
                        assert_eq!(response.status(), StatusCode::OK);
                        assert_eq!(
                            response.into_body().collect().await.unwrap().to_bytes(),
                            payload
                        );
                        let received = el.received().pop().unwrap();
                        assert_eq!(
                            received.uri.path_and_query().map(|pq| pq.as_str()),
                            Some(expected_pq)
                        );
                        assert_eq!(received.body, payload);
                    }
                }
            }
            proxy.shutdown().await;
        }
    }
}

#[tokio::test]
async fn websocket_header_lists_and_http1_connect_are_validated_before_dialing() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream = format!("ws://{}", listener.local_addr().unwrap());
    let dials = Arc::new(AtomicUsize::new(0));
    let observed = dials.clone();
    let server = tokio::spawn(async move {
        while let Ok((socket, _)) = listener.accept().await {
            observed.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(async move {
                let mut ws = accept_async(socket).await.unwrap();
                while let Some(Ok(message)) = ws.next().await {
                    if message.is_close() {
                        break;
                    }
                    ws.send(message).await.unwrap();
                }
            });
        }
    });
    let key = "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n";
    let canonical = "Connection: Upgrade\r\nUpgrade: websocket\r\n";
    for mocking in [false, true] {
        let proxy = EthonyxHandle::start(&[
            "--trust-upstream",
            "--el-ws-url",
            &upstream,
            "--mock-eth-syncing",
            if mocking { "true" } else { "false" },
        ])
        .await;
        let cases = [
            ("GET /", format!("{canonical}{key}"), true),
            (
                "GET /",
                format!(
                    "Connection: keep-alive\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n{key}"
                ),
                true,
            ),
            (
                "GET /",
                format!(
                    "Connection: Upgrade\r\nConnection: keep-alive\r\nUpgrade: websocket\r\n{key}"
                ),
                true,
            ),
            (
                "GET /",
                format!("Connection: keep-alive, Upgrade\r\nUpgrade: h2c, websocket\r\n{key}"),
                true,
            ),
            (
                "GET /",
                format!(
                    "Connection: keep-alive, uPgRaDe\r\nUpgrade: h2c\r\nUpgrade: WebSocket\r\n{key}"
                ),
                true,
            ),
            ("CONNECT localhost:1234", canonical.into(), false),
            ("CONNECT localhost:1234", format!("{canonical}{key}"), false),
            ("POST /", format!("{canonical}{key}"), false),
            (
                "GET /",
                format!("{canonical}Sec-WebSocket-Version: 13\r\n"),
                false,
            ),
            (
                "GET /",
                format!("{canonical}{key}Sec-WebSocket-Version: 13\r\n"),
                false,
            ),
            (
                "GET /",
                format!("{canonical}{key}Sec-WebSocket-Key: invalid\r\n"),
                false,
            ),
            (
                "GET /",
                format!("{canonical}{}", key.replace("13", "12")),
                false,
            ),
            (
                "GET /",
                format!(
                    "{canonical}{}",
                    key.replace("dGhlIHNhbXBsZSBub25jZQ==", "invalid")
                ),
                false,
            ),
        ];
        for (request_line, headers, valid) in cases {
            let before = dials.load(Ordering::SeqCst);
            let mut socket = tokio::net::TcpStream::connect(("127.0.0.1", proxy.port))
                .await
                .unwrap();
            socket
                .write_all(
                    format!("{request_line} HTTP/1.1\r\nHost: localhost\r\n{headers}\r\n")
                        .as_bytes(),
                )
                .await
                .unwrap();
            let response = tokio::time::timeout(Duration::from_secs(3), async {
                let mut head = Vec::new();
                while !head.ends_with(b"\r\n\r\n") {
                    assert!(head.len() < 16 * 1024);
                    head.push(socket.read_u8().await.unwrap());
                }
                head
            })
            .await
            .unwrap();
            let status = if valid {
                "HTTP/1.1 101"
            } else {
                "HTTP/1.1 400"
            };
            assert!(
                response.starts_with(status.as_bytes()),
                "{request_line} {headers}: {}",
                String::from_utf8_lossy(&response)
            );
            assert_eq!(dials.load(Ordering::SeqCst) - before, usize::from(valid));
            if valid {
                let mut ws = tokio_tungstenite::WebSocketStream::from_raw_socket(
                    socket,
                    tokio_tungstenite::tungstenite::protocol::Role::Client,
                    None,
                )
                .await;
                ws.send(Message::Text("round trip".into())).await.unwrap();
                assert_eq!(
                    tokio::time::timeout(Duration::from_secs(3), ws.next())
                        .await
                        .unwrap()
                        .unwrap()
                        .unwrap(),
                    Message::Text("round trip".into())
                );
                ws.close(None).await.unwrap();
            }
        }
        proxy.shutdown().await;
    }
    server.abort();
}

#[tokio::test]
async fn http2_websocket_rejects_missing_invalid_and_duplicate_versions_without_dialing() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream = format!("ws://{}", listener.local_addr().unwrap());
    let proxy = EthonyxHandle::start(&["--trust-upstream", "--el-ws-url", &upstream]).await;
    let tcp = tokio::net::TcpStream::connect(("127.0.0.1", proxy.port))
        .await
        .unwrap();
    let (mut sender, conn) = hyper::client::conn::http2::Builder::new(TokioExecutor::new())
        .handshake::<_, TestBody>(TokioIo::new(tcp))
        .await
        .unwrap();
    let connection = tokio::spawn(async move {
        let _ = conn.await;
    });
    for versions in [
        vec![],
        vec!["12"],
        vec!["13, 13"],
        vec!["13", "13"],
        vec!["13", "12"],
    ] {
        let mut req = Request::builder()
            .method(Method::CONNECT)
            .uri(proxy.url("/"))
            .body(Full::new(Bytes::new()))
            .unwrap();
        req.extensions_mut()
            .insert(hyper::ext::Protocol::from_static("websocket"));
        for version in versions {
            req.headers_mut()
                .append("sec-websocket-version", version.parse().unwrap());
        }
        let response = tokio::time::timeout(Duration::from_secs(2), sender.send_request(req))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(100), listener.accept())
            .await
            .is_err()
    );
    proxy.shutdown().await;
    connection.abort();
}

#[tokio::test]
async fn invalid_jsonrpc_batches_are_forwarded_intact_over_http_and_websocket() {
    // This upstream echoes the complete body instead of interpreting the batch.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        while let Ok((socket, _)) = listener.accept().await {
            tokio::spawn(async move {
                let service = service_fn(|request: Request<Incoming>| async move {
                    let body = request.into_body().collect().await.unwrap().to_bytes();
                    Ok::<_, Infallible>(Response::new(Full::new(body)))
                });
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(socket), service)
                    .await;
            });
        }
    });
    let ws_upstream = ws_echo_upstream().await;
    let proxy = EthonyxHandle::start(&[
        "--trust-upstream",
        "--mock-eth-syncing",
        "--el-http-url",
        &upstream,
        "--el-ws-url",
        &ws_upstream,
    ])
    .await;
    let (mut ws, _) = connect_async(format!("ws://127.0.0.1:{}/", proxy.port))
        .await
        .unwrap();
    let local = r#"{"jsonrpc":"2.0","method":"eth_syncing","id":2}"#;
    let bodies = [
        r#"[["2.0","eth_syncing",[],1]]"#.to_owned(),
        r#"[["2.0","eth_syncing"]]"#.to_owned(),
        format!(r#"[{local},["2.0","eth_syncing"]]"#),
        format!(r#"[["2.0","eth_syncing",[],1],{local}]"#),
        format!("[{local}] trailing"),
        format!("[{local},]"),
        format!("[{local}"),
        format!(
            r#"[{{"jsonrpc":"2.0","method":"eth_blockNumber","id":1}},{}]"#,
            vec![local; 1000].join(",")
        ),
    ];
    for body in bodies {
        for client in [client(), h2c_client()] {
            let req = Request::builder()
                .method(Method::POST)
                .uri(proxy.url("/"))
                .body(Full::new(Bytes::copy_from_slice(body.as_bytes())))
                .unwrap();
            let response = client.request(req).await.unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(
                response.into_body().collect().await.unwrap().to_bytes(),
                body
            );
        }
        ws.send(Message::Text(body.clone().into())).await.unwrap();
        let reply = tokio::time::timeout(Duration::from_secs(3), ws.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(reply.into_text().unwrap(), body);
    }
    ws.close(None).await.unwrap();
    proxy.shutdown().await;
    server.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn shutdown_already_ready_does_not_wait_for_a_new_watch_event() {
    let cfg = Config::parse_from([
        "ethonyx",
        "--listen",
        "127.0.0.1:0",
        "--trust-upstream",
        "--shutdown-grace",
        "0",
    ]);
    tokio::time::timeout(Duration::from_secs(1), run(cfg, async {}))
        .await
        .expect("an already completed shutdown must stop the accept loop")
        .unwrap();
}

async fn shutdown_during_h2_ws_handshake(complete_handshake: bool, grace: Duration) {
    use tokio::io::AsyncReadExt;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let (dial_tx, dial_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let (closed_tx, closed_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        dial_tx.send(()).unwrap();
        if complete_handshake {
            release_rx.await.unwrap();
            let mut ws = accept_async(socket).await.unwrap();
            while let Some(Ok(message)) = ws.next().await {
                if ws.send(message).await.is_err() {
                    break;
                }
            }
        } else {
            // Read the pending handshake, then observe cancellation of its dial.
            let mut buffer = [0; 4096];
            while let Ok(n) = socket.read(&mut buffer).await {
                if n == 0 {
                    break;
                }
            }
        }
        let _ = closed_tx.send(());
    });
    let mut proxy =
        EthonyxHandle::start_with_grace(&["--trust-upstream", "--el-ws-url", &url], grace).await;
    let tcp = tokio::net::TcpStream::connect(("127.0.0.1", proxy.port))
        .await
        .unwrap();
    let (mut sender, conn) = hyper::client::conn::http2::Builder::new(TokioExecutor::new())
        .handshake::<_, TestBody>(TokioIo::new(tcp))
        .await
        .unwrap();
    let driver = tokio::spawn(async move {
        let _ = conn.await;
    });
    let mut req = Request::builder()
        .method(Method::CONNECT)
        .uri(proxy.url("/"))
        .header("sec-websocket-version", "13")
        .body(Full::new(Bytes::new()))
        .unwrap();
    req.extensions_mut()
        .insert(hyper::ext::Protocol::from_static("websocket"));
    let request = tokio::spawn(async move { sender.send_request(req).await });
    tokio::time::timeout(Duration::from_secs(2), dial_rx)
        .await
        .unwrap()
        .unwrap();
    let started = tokio::time::Instant::now();
    proxy.shutdown.take().unwrap().send(()).unwrap();
    let mut downstream = None;
    if complete_handshake {
        tokio::time::sleep(Duration::from_millis(600)).await;
        release_tx.send(()).unwrap();
        let response = tokio::time::timeout(Duration::from_secs(1), request)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        downstream = Some(hyper::upgrade::on(response).await.unwrap());
    } else {
        drop(release_tx);
        // The shutdown barrier must not depend on the client dropping this request.
        drop(request);
    }
    tokio::time::timeout(grace + Duration::from_secs(1), proxy.shutdown())
        .await
        .expect("the shared deadline must also cancel pending HTTP/2 request tasks");
    assert!(started.elapsed() < grace + Duration::from_millis(500));
    tokio::time::timeout(Duration::from_millis(200), closed_rx)
        .await
        .expect("upstream must be released when run returns, not after a new grace period")
        .unwrap();
    server.await.unwrap();
    drop(downstream);
    driver.abort();
    let _ = driver.await;
}

#[tokio::test]
async fn shutdown_late_websocket_relay_keeps_the_original_deadline() {
    shutdown_during_h2_ws_handshake(true, Duration::from_secs(1)).await;
}

#[tokio::test]
async fn shutdown_cancels_pending_http2_websocket_handshakes() {
    for grace in [Duration::ZERO, Duration::from_secs(1)] {
        shutdown_during_h2_ws_handshake(false, grace).await;
    }
}

#[tokio::test]
async fn health_rejects_array_envelopes_and_data_without_losing_partial_results() {
    let synced = r#"{"data":{"head_slot":"123","sync_distance":"0","is_syncing":false}}"#;
    let peers = r#"{"data":{"connected":"64"}}"#;
    let el = MockServer::start(Arc::new(|req| {
        let body: Value = serde_json::from_slice(&req.body).unwrap();
        let result = match body["method"].as_str().unwrap() {
            "eth_syncing" => json!(false),
            "net_peerCount" => json!("0x40"),
            "eth_getBlockByNumber" => json!({"number":"0x1","timestamp":"0x1"}),
            method => panic!("unexpected probe {method}"),
        };
        (
            StatusCode::OK,
            serde_json::to_vec(&json!({"result":result})).unwrap(),
        )
    }))
    .await;
    for (syncing, peer_count, valid_syncing, valid_peers) in [
        (synced, peers, true, true),
        (r#"[["123","0",false]]"#, peers, false, true),
        (r#"{"data":["123","0",false]}"#, peers, false, true),
        (
            r#"[{"head_slot":"123","sync_distance":"0","is_syncing":false}]"#,
            peers,
            false,
            true,
        ),
        (synced, r#"[["64"]]"#, true, false),
        (synced, r#"{"data":["64"]}"#, true, false),
        (synced, r#"[{"connected":"64"}]"#, true, false),
    ] {
        let cl = MockServer::start(Arc::new(move |req| {
            (
                StatusCode::OK,
                if req.path == "/eth/v1/node/syncing" {
                    syncing
                } else {
                    peer_count
                }
                .as_bytes()
                .to_vec(),
            )
        }))
        .await;
        let proxy =
            EthonyxHandle::start(&["--el-http-url", &el.url, "--cl-beacon-url", &cl.url]).await;
        let c = client();
        let (status, _) = get(&c, &proxy.url("/readyz")).await;
        assert_eq!(
            status,
            if valid_syncing {
                StatusCode::OK
            } else {
                StatusCode::SERVICE_UNAVAILABLE
            },
            "{syncing}"
        );
        let (status, body) = get(&c, &proxy.url("/healthz")).await;
        assert_eq!(status, StatusCode::OK);
        let report: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(report["el"]["syncing"], false);
        assert_eq!(
            report["cl"]["syncing"].as_bool(),
            valid_syncing.then_some(false)
        );
        assert_eq!(report["cl"]["peers"].as_u64(), valid_peers.then_some(64));
        if !valid_syncing || !valid_peers {
            assert!(!report["cl"]["errors"].as_array().unwrap().is_empty());
        }
        proxy.shutdown().await;
    }
}

async fn ws_handshake_response(
    proxy: &EthonyxHandle,
    h2: bool,
    headers: &[(&str, &str)],
) -> (Response<Incoming>, JoinHandle<()>) {
    let socket = tokio::net::TcpStream::connect(("127.0.0.1", proxy.port))
        .await
        .unwrap();
    let mut req = Request::builder()
        .method(if h2 { Method::CONNECT } else { Method::GET })
        .uri(proxy.url("/"))
        .header("sec-websocket-version", "13")
        .body(Full::new(Bytes::new()))
        .unwrap();
    for (name, value) in headers {
        req.headers_mut().append(
            http::HeaderName::from_bytes(name.as_bytes()).unwrap(),
            value.parse().unwrap(),
        );
    }
    if h2 {
        req.extensions_mut()
            .insert(hyper::ext::Protocol::from_static("websocket"));
        let (mut sender, conn) = hyper::client::conn::http2::Builder::new(TokioExecutor::new())
            .handshake::<_, TestBody>(TokioIo::new(socket))
            .await
            .unwrap();
        let driver = tokio::spawn(async move {
            let _ = conn.await;
        });
        (sender.send_request(req).await.unwrap(), driver)
    } else {
        req.headers_mut()
            .insert("host", "localhost".parse().unwrap());
        req.headers_mut()
            .insert("connection", "Upgrade".parse().unwrap());
        req.headers_mut()
            .insert("upgrade", "websocket".parse().unwrap());
        req.headers_mut().insert(
            "sec-websocket-key",
            "dGhlIHNhbXBsZSBub25jZQ==".parse().unwrap(),
        );
        let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(socket))
            .await
            .unwrap();
        let driver = tokio::spawn(async move {
            let _ = conn.with_upgrades().await;
        });
        (sender.send_request(req).await.unwrap(), driver)
    }
}

#[tokio::test]
async fn websocket_negotiates_split_protocols_without_forwarding_extensions() {
    use tokio_tungstenite::tungstenite::handshake::server::{
        Request as WsRequest, Response as WsResponse,
    };
    for (h2, large) in [(false, false), (true, false), (false, true), (true, true)] {
        // Keep the wire case within HTTP/2's header-list limit. Unit tests also
        // exercise 32K tokens near the larger HTTP/1 header buffer limit.
        let first_line = if large {
            (0..1024)
                .rev()
                .map(|i| format!("p{i:04}"))
                .collect::<Vec<_>>()
                .join(", ")
        } else {
            "first".into()
        };
        let expected = format!("{first_line}, Second, second, Third");
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_hdr_async(
                socket,
                #[allow(clippy::result_large_err)] // Tungstenite fixes the callback error type.
                move |request: &WsRequest, mut response: WsResponse| {
                    assert!(!request.headers().contains_key("sec-websocket-extensions"));
                    assert_eq!(
                        request
                            .headers()
                            .get_all("sec-websocket-protocol")
                            .iter()
                            .count(),
                        1
                    );
                    assert_eq!(request.headers()["sec-websocket-protocol"], expected);
                    response
                        .headers_mut()
                        .insert("sec-websocket-protocol", "second".parse().unwrap());
                    Ok(response)
                },
            )
            .await
            .unwrap();
            let message = ws.next().await.unwrap().unwrap();
            ws.send(message).await.unwrap();
            let _ = ws.next().await;
        });
        let proxy = EthonyxHandle::start(&["--trust-upstream", "--el-ws-url", &url]).await;
        let (response, driver) = ws_handshake_response(
            &proxy,
            h2,
            &[
                ("sec-websocket-protocol", &first_line),
                ("sec-websocket-protocol", " Second\t, second, Third "),
                (
                    "sec-websocket-extensions",
                    "permessage-deflate; client_max_window_bits",
                ),
                ("sec-websocket-extensions", "unknown-extension"),
            ],
        )
        .await;
        assert_eq!(
            response.status(),
            if h2 {
                StatusCode::OK
            } else {
                StatusCode::SWITCHING_PROTOCOLS
            }
        );
        assert_eq!(response.headers()["sec-websocket-protocol"], "second");
        assert!(!response.headers().contains_key("sec-websocket-extensions"));
        let upgraded = hyper::upgrade::on(response).await.unwrap();
        let mut ws = tokio_tungstenite::WebSocketStream::from_raw_socket(
            TokioIo::new(upgraded),
            tokio_tungstenite::tungstenite::protocol::Role::Client,
            None,
        )
        .await;
        ws.send(Message::Text("uncompressed round trip".into()))
            .await
            .unwrap();
        assert_eq!(
            ws.next().await.unwrap().unwrap(),
            Message::Text("uncompressed round trip".into())
        );
        ws.close(None).await.unwrap();
        proxy.shutdown().await;
        server.await.unwrap();
        driver.abort();
        let _ = driver.await;
    }
}

#[tokio::test]
async fn websocket_rejects_split_protocol_duplicates_before_dialing() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let proxy = EthonyxHandle::start(&["--trust-upstream", "--el-ws-url", &url]).await;
    for h2 in [false, true] {
        let count = if h2 { 1024 } else { 32_768 };
        let large = (0..count)
            .map(|i| format!("p{i:05}"))
            .collect::<Vec<_>>()
            .join(",");
        for values in [
            vec!["first, second", "Second, third", " \tsecond\t "],
            vec!["first", "", "second"],
            vec![large.as_str(), " p00000 "],
        ] {
            let headers: Vec<_> = values
                .iter()
                .map(|&value| ("sec-websocket-protocol", value))
                .collect();
            let (response, driver) = tokio::time::timeout(
                Duration::from_secs(3),
                ws_handshake_response(&proxy, h2, &headers),
            )
            .await
            .expect("invalid subprotocol offers must be rejected promptly");
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "h2={h2}");
            drop(response);
            driver.abort();
            let _ = driver.await;
        }
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(50), listener.accept())
            .await
            .is_err(),
        "invalid offers must not dial upstream"
    );
    proxy.shutdown().await;
}

#[tokio::test]
async fn websocket_rejects_unsolicited_extensions_and_invalid_protocol_selection() {
    use tokio_tungstenite::tungstenite::handshake::server::{
        Request as WsRequest, Response as WsResponse,
    };
    for h2 in [false, true] {
        for selection in ["extension", "duplicate", "unoffered", "wrong-case", "list"] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("ws://{}", listener.local_addr().unwrap());
            let server = tokio::spawn(async move {
                let (socket, _) = listener.accept().await.unwrap();
                let mut ws = tokio_tungstenite::accept_hdr_async(
                    socket,
                    #[allow(clippy::result_large_err)]
                    // Tungstenite fixes the callback error type.
                    move |request: &WsRequest, mut response: WsResponse| {
                        assert!(!request.headers().contains_key("sec-websocket-extensions"));
                        let protocol = match selection {
                            "unoffered" => "other",
                            "wrong-case" => "Second",
                            "list" => "first, second",
                            _ => "second",
                        };
                        response
                            .headers_mut()
                            .insert("sec-websocket-protocol", protocol.parse().unwrap());
                        if selection == "extension" {
                            response.headers_mut().insert(
                                "sec-websocket-extensions",
                                "permessage-deflate".parse().unwrap(),
                            );
                        }
                        if selection == "duplicate" {
                            response
                                .headers_mut()
                                .append("sec-websocket-protocol", "second".parse().unwrap());
                        }
                        Ok(response)
                    },
                )
                .await
                .unwrap();
                let _ = ws.next().await;
            });
            let proxy = EthonyxHandle::start(&["--trust-upstream", "--el-ws-url", &url]).await;
            let (response, driver) = ws_handshake_response(
                &proxy,
                h2,
                &[
                    ("sec-websocket-protocol", "first"),
                    ("sec-websocket-protocol", "second"),
                ],
            )
            .await;
            assert_eq!(
                response.status(),
                StatusCode::BAD_GATEWAY,
                "{selection}, h2={h2}"
            );
            proxy.shutdown().await;
            tokio::time::timeout(Duration::from_secs(1), server)
                .await
                .unwrap()
                .unwrap();
            driver.abort();
            let _ = driver.await;
        }
    }
}

#[tokio::test]
async fn websocket_strips_hop_by_hop_and_connection_nominated_headers_on_upstream_handshake() {
    use tokio_tungstenite::tungstenite::handshake::server::{
        Request as WsRequest, Response as WsResponse,
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_hdr_async(
            socket,
            #[allow(clippy::result_large_err)]
            move |request: &WsRequest, mut response: WsResponse| {
                let h = request.headers();
                for stripped in [
                    "x-internal",
                    "proxy-authorization",
                    "proxy-connection",
                    "te",
                    "keep-alive",
                ] {
                    assert!(!h.contains_key(stripped), "leaked {stripped}");
                }
                assert_eq!(h["connection"], "Upgrade");
                assert_eq!(h["upgrade"], "websocket");
                assert_eq!(h["sec-websocket-version"], "13");
                assert_eq!(h["sec-websocket-protocol"], "jsonrpc");
                assert_eq!(h["x-end-to-end"], "retained");
                response
                    .headers_mut()
                    .insert("sec-websocket-protocol", "jsonrpc".parse().unwrap());
                Ok(response)
            },
        )
        .await
        .unwrap();
        let _ = ws.next().await;
    });
    let proxy = EthonyxHandle::start(&["--trust-upstream", "--el-ws-url", &url]).await;
    let socket = tokio::net::TcpStream::connect(("127.0.0.1", proxy.port))
        .await
        .unwrap();
    let req = Request::builder()
        .method(Method::GET)
        .uri(proxy.url("/"))
        .header("host", "localhost")
        .header(
            "connection",
            "Upgrade, keep-alive, X-Internal, Sec-WebSocket-Key, Sec-WebSocket-Version, Sec-WebSocket-Protocol",
        )
        .header("upgrade", "websocket")
        .header("sec-websocket-version", "13")
        .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
        .header("sec-websocket-protocol", "jsonrpc")
        .header("x-internal", "secret")
        .header("proxy-authorization", "Basic secret")
        .header("proxy-connection", "keep-alive")
        .header("te", "trailers")
        .header("x-end-to-end", "retained")
        .body(Full::new(Bytes::new()))
        .unwrap();
    let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(socket))
        .await
        .unwrap();
    let driver = tokio::spawn(async move {
        let _ = conn.with_upgrades().await;
    });
    let response = sender.send_request(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
    assert_eq!(response.headers()["sec-websocket-protocol"], "jsonrpc");
    proxy.shutdown().await;
    server.await.unwrap();
    driver.abort();
    let _ = driver.await;
}

#[tokio::test]
async fn websocket_mock_mode_bounds_post_close_drain_when_upstream_keeps_tcp_open() {
    use tokio::io::AsyncReadExt;
    for mocking in ["false", "true"] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let (released_tx, released_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(socket).await.unwrap();
            ws.send(Message::Close(None)).await.unwrap();
            let mut raw = ws.into_inner();
            let mut buf = [0u8; 64];
            while let Ok(n) = raw.read(&mut buf).await {
                if n == 0 {
                    break;
                }
            }
            let _ = released_tx.send(());
        });
        let proxy = EthonyxHandle::start(&[
            "--trust-upstream",
            "--mock-eth-syncing",
            mocking,
            "--el-ws-url",
            &url,
        ])
        .await;
        let (ws, _) = connect_async(format!("ws://127.0.0.1:{}/", proxy.port))
            .await
            .unwrap();
        let _raw_client = ws.into_inner();
        tokio::time::timeout(Duration::from_millis(2600), released_rx)
            .await
            .unwrap_or_else(|_| panic!("mock={mocking}: upstream must be released after Close"))
            .unwrap();
        server.await.unwrap();
        proxy.shutdown().await;
    }
}

#[tokio::test]
async fn readyz_strict_rejects_malformed_hex_quantities_in_el_probes() {
    let now_hex = format!("0x{:x}", now_unix());
    for bad_hex in ["0x+10", "0x0x10", "0x00", "ff"] {
        let ts = now_hex.clone();
        let el = MockServer::start(Arc::new(move |req| {
            let body: Value = serde_json::from_slice(&req.body).unwrap_or(Value::Null);
            let method = body.get("method").and_then(Value::as_str).unwrap_or("");
            let result = match method {
                "eth_syncing" => json!(false),
                "net_peerCount" => json!("0x10"),
                "eth_getBlockByNumber" => json!({"number": bad_hex, "timestamp": ts}),
                _ => json!(null),
            };
            let resp = json!({"jsonrpc": "2.0", "id": 1, "result": result});
            (StatusCode::OK, serde_json::to_vec(&resp).unwrap())
        }))
        .await;
        let cl = MockServer::start(Arc::new(|req| match req.path.as_str() {
            "/eth/v1/node/syncing" => (
                StatusCode::OK,
                serde_json::to_vec(&json!({
                    "data": {"head_slot": "1", "sync_distance": "0", "is_syncing": false}
                }))
                .unwrap(),
            ),
            "/eth/v1/node/peer_count" => (
                StatusCode::OK,
                serde_json::to_vec(&json!({"data": {"connected": "8"}})).unwrap(),
            ),
            _ => (StatusCode::NOT_FOUND, b"{}".to_vec()),
        }))
        .await;
        let proxy = EthonyxHandle::start(&[
            "--el-http-url",
            &el.url,
            "--cl-beacon-url",
            &cl.url,
            "--readyz-strict",
        ])
        .await;
        let c = client();
        let (status, body) = get(&c, &proxy.url("/readyz")).await;
        assert_eq!(
            status,
            StatusCode::SERVICE_UNAVAILABLE,
            "{bad_hex}: {body:?}"
        );
        let (_, hz) = get(&c, &proxy.url("/healthz")).await;
        let hv: Value = serde_json::from_slice(&hz).unwrap();
        assert!(hv["el"]["block_number"].is_null(), "{bad_hex}: {hv}");
        assert!(
            !hv["el"]["errors"].as_array().unwrap().is_empty(),
            "{bad_hex}: {hv}"
        );
        proxy.shutdown().await;
    }
}

#[tokio::test]
async fn mock_eth_syncing_forwards_non_empty_params_over_http_and_websocket() {
    let el = MockServer::start(Arc::new(|req| (StatusCode::OK, req.body.to_vec()))).await;
    let ws_upstream = ws_echo_upstream().await;
    let proxy = EthonyxHandle::start(&[
        "--trust-upstream",
        "--mock-eth-syncing",
        "--el-http-url",
        &el.url,
        "--el-ws-url",
        &ws_upstream,
    ])
    .await;
    let c = client();
    let (mut ws, _) = connect_async(format!("ws://127.0.0.1:{}/", proxy.port))
        .await
        .unwrap();
    for payload in [
        r#"{"jsonrpc":"2.0","method":"eth_syncing","params":[1,2,3],"id":1}"#,
        r#"{"jsonrpc":"2.0","method":"eth_syncing","params":{"unexpected":1},"id":1}"#,
    ] {
        let req = Request::builder()
            .method(Method::POST)
            .uri(proxy.url("/"))
            .header("content-type", "application/json")
            .body(Full::new(Bytes::from(payload)))
            .unwrap();
        let resp = c.request(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(body, payload);

        ws.send(Message::Text(payload.into())).await.unwrap();
        let echoed = ws.next().await.unwrap().unwrap().into_text().unwrap();
        assert_eq!(echoed, payload);
    }
    ws.close(None).await.unwrap();
    proxy.shutdown().await;
}

#[tokio::test]
async fn bad_gateway_redacts_upstream_details_and_http1_preserves_content_length() {
    let dead_port = pick_port().await;
    let dead_url = format!("http://127.0.0.1:{dead_port}");
    let live_el = MockServer::start(Arc::new(|_| {
        (
            StatusCode::OK,
            br#"{"jsonrpc":"2.0","id":1,"result":"0x2a"}"#.to_vec(),
        )
    }))
    .await;

    let dead_proxy = EthonyxHandle::start(&["--trust-upstream", "--el-http-url", &dead_url]).await;
    let c = client();
    let (status, body) = post_json(
        &c,
        &dead_proxy.url("/"),
        json!({"jsonrpc":"2.0","method":"eth_blockNumber","id":1}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert_eq!(body.as_ref(), b"bad gateway");
    dead_proxy.shutdown().await;

    let live_proxy =
        EthonyxHandle::start(&["--trust-upstream", "--el-http-url", &live_el.url]).await;
    let req = Request::builder()
        .method(Method::POST)
        .uri(live_proxy.url("/"))
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from_static(
            br#"{"jsonrpc":"2.0","method":"eth_blockNumber","id":1}"#,
        )))
        .unwrap();
    let resp = c.request(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers()
            .get("content-length")
            .and_then(|v| v.to_str().ok()),
        Some("40"),
        "HTTP/1.1 response must preserve Content-Length instead of chunked encoding"
    );
    assert!(!resp.headers().contains_key("transfer-encoding"));
    live_proxy.shutdown().await;
}

#[tokio::test]
async fn mock_cl_subscriptions_drains_body_over_keep_alive_and_times_out_stalled_body() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let proxy = EthonyxHandle::start(&["--trust-upstream", "--mock-cl-subscriptions"]).await;

    // 1. Two sequential POSTs with split chunked bodies over one HTTP/1.1 keep-alive socket.
    let mut keepalive = tokio::net::TcpStream::connect(("127.0.0.1", proxy.port))
        .await
        .unwrap();
    for i in 0..2 {
        keepalive
            .write_all(
                b"POST /eth/v1/validator/beacon_committee_subscriptions HTTP/1.1\r\n\
                  Host: localhost\r\n\
                  Content-Type: application/json\r\n\
                  Transfer-Encoding: chunked\r\n\r\n\
                  2\r\n[{\r\n",
            )
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        keepalive
            .write_all(b"e\r\n\"slot\":\"100\"}]\r\n0\r\n\r\n")
            .await
            .unwrap();

        let mut buf = vec![0u8; 512];
        let n = keepalive.read(&mut buf).await.unwrap();
        let text = String::from_utf8_lossy(&buf[..n]);
        assert!(
            text.starts_with("HTTP/1.1 200 OK"),
            "round {i}: expected 200 OK on keep-alive socket, got: {text}"
        );
        assert!(text.contains("{\"code\":200,\"message\":\"success\"}"));
    }
    drop(keepalive);

    // 2. Stalled Content-Length body on mock_cl_subscriptions returns 408 Request Timeout.
    let mut stalled = tokio::net::TcpStream::connect(("127.0.0.1", proxy.port))
        .await
        .unwrap();
    stalled
        .write_all(
            b"POST /eth/v1/validator/prepare_beacon_proposer HTTP/1.1\r\n\
              Host: localhost\r\n\
              Content-Length: 64\r\n\r\n\
              partial",
        )
        .await
        .unwrap();
    let mut buf = [0u8; 256];
    let n = tokio::time::timeout(Duration::from_secs(4), stalled.read(&mut buf))
        .await
        .unwrap()
        .unwrap();
    assert!(
        String::from_utf8_lossy(&buf[..n]).starts_with("HTTP/1.1 408"),
        "expected 408 on stalled mock subscription body"
    );

    proxy.shutdown().await;
}

#[tokio::test]
async fn node_health_returns_503_on_partial_outage_or_strict_staleness_and_206_when_syncing() {
    let now_hex = format!("0x{:x}", now_unix());
    let stale_hex = format!("0x{:x}", now_unix().saturating_sub(600));
    let dead_port = pick_port().await;
    let dead_cl = format!("http://127.0.0.1:{dead_port}");

    // 1. EL syncing + CL unreachable => 503 (never 206).
    let ts1 = now_hex.clone();
    let el_syncing = MockServer::start(Arc::new(move |req| {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or(Value::Null);
        let method = body.get("method").and_then(Value::as_str).unwrap_or("");
        let result = match method {
            "eth_syncing" => json!({"currentBlock": "0x10", "highestBlock": "0x20"}),
            "net_peerCount" => json!("0x5"),
            "eth_getBlockByNumber" => json!({"number": "0x10", "timestamp": ts1}),
            _ => json!(null),
        };
        (
            StatusCode::OK,
            serde_json::to_vec(&json!({"jsonrpc":"2.0","id":1,"result":result})).unwrap(),
        )
    }))
    .await;
    let p1 = EthonyxHandle::start(&[
        "--mock-cl-node-health",
        "--el-http-url",
        &el_syncing.url,
        "--cl-beacon-url",
        &dead_cl,
    ])
    .await;
    let c = client();
    assert_eq!(
        get(&c, &p1.url("/eth/v1/node/health")).await.0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    p1.shutdown().await;

    // 2. EL syncing + CL synced (both reachable) => 206 Partial Content.
    let cl_synced = MockServer::start(Arc::new(|req| match req.path.as_str() {
        "/eth/v1/node/syncing" => (
            StatusCode::OK,
            br#"{"data":{"is_syncing":false,"sync_distance":"0","head_slot":"100"}}"#.to_vec(),
        ),
        "/eth/v1/node/peer_count" => (StatusCode::OK, br#"{"data":{"connected":"12"}}"#.to_vec()),
        _ => (StatusCode::NOT_FOUND, Vec::new()),
    }))
    .await;
    let p2 = EthonyxHandle::start(&[
        "--mock-cl-node-health",
        "--el-http-url",
        &el_syncing.url,
        "--cl-beacon-url",
        &cl_synced.url,
    ])
    .await;
    assert_eq!(
        get(&c, &p2.url("/eth/v1/node/health")).await.0,
        StatusCode::PARTIAL_CONTENT
    );
    p2.shutdown().await;

    // 3. Both synced, --readyz-strict enabled, stale EL block => 503 Service Unavailable.
    let el_stale = MockServer::start(Arc::new(move |req| {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or(Value::Null);
        let method = body.get("method").and_then(Value::as_str).unwrap_or("");
        let result = match method {
            "eth_syncing" => json!(false),
            "net_peerCount" => json!("0x5"),
            "eth_getBlockByNumber" => json!({"number": "0x20", "timestamp": stale_hex}),
            _ => json!(null),
        };
        (
            StatusCode::OK,
            serde_json::to_vec(&json!({"jsonrpc":"2.0","id":1,"result":result})).unwrap(),
        )
    }))
    .await;
    let p3 = EthonyxHandle::start(&[
        "--mock-cl-node-health",
        "--readyz-strict",
        "--cl-genesis-time",
        "0",
        "--el-http-url",
        &el_stale.url,
        "--cl-beacon-url",
        &cl_synced.url,
    ])
    .await;
    assert_eq!(
        get(&c, &p3.url("/eth/v1/node/health")).await.0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    p3.shutdown().await;
}

#[tokio::test]
async fn trust_upstream_reports_honest_healthz_mode_without_fabricated_fields() {
    let trusted = EthonyxHandle::start(&["--trust-upstream"]).await;
    let c = client();
    let (status, body) = get(&c, &trusted.url("/healthz")).await;
    assert_eq!(status, StatusCode::OK);
    let snap: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(snap["mode"], "trusted");
    assert_eq!(snap["el"], json!({"transport": "http/1.1"}));
    assert_eq!(snap["cl"], json!({"transport": "http/1.1"}));
    trusted.shutdown().await;
}

#[tokio::test]
async fn sse_event_stream_survives_past_proxy_timeout_while_stalled_json_aborts() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // Custom HTTP/1.1 upstream: `/eth/v1/events` streams 3 SSE frames spaced by 900ms
    // (2.7s total > 2s proxy_timeout); `/stalled` sends 200 JSON headers and stalls 4s.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let upstream_url = format!("http://{addr}");
    let upstream = tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut req_buf = [0u8; 1024];
                let Ok(n) = sock.read(&mut req_buf).await else {
                    return;
                };
                let head = String::from_utf8_lossy(&req_buf[..n]);
                if head.starts_with("GET /eth/v1/events") {
                    let _ = sock
                        .write_all(
                            b"HTTP/1.1 200 OK\r\n\
                              Content-Type: text/event-stream; charset=utf-8\r\n\
                              Transfer-Encoding: chunked\r\n\r\n",
                        )
                        .await;
                    for idx in 1..=3 {
                        tokio::time::sleep(Duration::from_millis(900)).await;
                        let ev = format!("data: slot-{idx}\n\n");
                        let chunk = format!("{:x}\r\n{ev}\r\n", ev.len());
                        if sock.write_all(chunk.as_bytes()).await.is_err() {
                            return;
                        }
                    }
                    let _ = sock.write_all(b"0\r\n\r\n").await;
                } else {
                    let _ = sock
                        .write_all(
                            b"HTTP/1.1 200 OK\r\n\
                              Content-Type: application/json\r\n\
                              Content-Length: 32\r\n\r\n\
                              {\"partial\":",
                        )
                        .await;
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            });
        }
    });

    // EthonyxHandle defaults to --proxy-timeout 2; set --sse-idle-timeout 5.
    let proxy = EthonyxHandle::start(&[
        "--trust-upstream",
        "--sse-idle-timeout",
        "5",
        "--cl-beacon-url",
        &upstream_url,
        "--el-http-url",
        &upstream_url,
    ])
    .await;
    let c = client();

    // 1. SSE stream stays alive across 2.7s (> 2s proxy_timeout) and delivers all 3 events.
    let sse_req = Request::builder()
        .method(Method::GET)
        .uri(proxy.url("/eth/v1/events?topics=head"))
        .body(Full::new(Bytes::new()))
        .unwrap();
    let sse_resp = c.request(sse_req).await.unwrap();
    assert_eq!(sse_resp.status(), StatusCode::OK);
    let sse_bytes = sse_resp.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(
        sse_bytes.as_ref(),
        b"data: slot-1\n\ndata: slot-2\n\ndata: slot-3\n\n"
    );

    // 2. Ordinary JSON response that stalls mid-body > 2s aborts cleanly instead of hanging.
    let json_req = Request::builder()
        .method(Method::GET)
        .uri(proxy.url("/stalled"))
        .body(Full::new(Bytes::new()))
        .unwrap();
    let json_resp = c.request(json_req).await.unwrap();
    assert_eq!(json_resp.status(), StatusCode::OK);
    let collected = tokio::time::timeout(Duration::from_secs(4), json_resp.into_body().collect())
        .await
        .expect("idle timeout must abort stalled body before 4s");
    assert!(
        collected.is_err(),
        "truncated stalled body must fail collection"
    );

    proxy.shutdown().await;
    upstream.abort();
}

#[tokio::test]
async fn max_connections_caps_concurrent_http_and_upgraded_websocket_clients() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let ws_upstream = ws_echo_upstream().await;
    let proxy = EthonyxHandle::start(&[
        "--trust-upstream",
        "--max-connections",
        "2",
        "--el-ws-url",
        &ws_upstream,
    ])
    .await;

    // Permit #1: persistent HTTP/1.1 keep-alive connection.
    let mut http_conn = tokio::net::TcpStream::connect(("127.0.0.1", proxy.port))
        .await
        .unwrap();
    http_conn
        .write_all(b"GET /livez HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();
    let mut buf = [0u8; 256];
    let n = http_conn.read(&mut buf).await.unwrap();
    assert!(String::from_utf8_lossy(&buf[..n]).starts_with("HTTP/1.1 200 OK"));

    // Permit #2: HTTP/1.1 WebSocket upgrade (its HTTP connection task returns right
    // after 101 Switching Protocols, so the spawned WS relay must retain the permit!).
    let (mut ws_conn, _) = connect_async(format!("ws://127.0.0.1:{}/", proxy.port))
        .await
        .unwrap();
    ws_conn
        .send(Message::Ping(Bytes::from_static(b"check")))
        .await
        .unwrap();
    assert_eq!(
        ws_conn.next().await.unwrap().unwrap(),
        Message::Pong(Bytes::from_static(b"check"))
    );

    // 3rd client must wait behind the semaphore while both permits are held.
    let c = client();
    let third_url = proxy.url("/livez");
    let third = tokio::spawn(async move { get(&c, &third_url).await });
    tokio::time::sleep(Duration::from_millis(120)).await;
    assert!(
        !third.is_finished(),
        "3rd connection must not be served while 2 permits (1 HTTP + 1 WS) are active"
    );

    // Closing the WebSocket releases permit #2 and immediately unblocks the 3rd client.
    ws_conn.close(None).await.unwrap();
    let _ = ws_conn.next().await;
    drop(ws_conn);

    let (status, _) = tokio::time::timeout(Duration::from_secs(2), third)
        .await
        .expect("3rd client must proceed once WS permit drops")
        .unwrap();
    assert_eq!(status, StatusCode::OK);

    drop(http_conn);
    proxy.shutdown().await;
}

#[tokio::test]
async fn adaptive_el_h2c_recovers_after_transient_upstream_restart() {
    use std::sync::atomic::{AtomicBool, Ordering};

    let el_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let el_addr = el_listener.local_addr().unwrap();
    let el_url = format!("http://{el_addr}");
    let allow_h2c = Arc::new(AtomicBool::new(true));
    let conn_handles: Arc<Mutex<Vec<JoinHandle<()>>>> = Arc::new(Mutex::new(Vec::new()));

    let flag = allow_h2c.clone();
    let handles = conn_handles.clone();
    let el_server = tokio::spawn(async move {
        while let Ok((stream, _)) = el_listener.accept().await {
            let h2 = flag.load(Ordering::Acquire);
            let h = tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let svc = service_fn(|req: Request<Incoming>| async move {
                    let bytes = req.into_body().collect().await.unwrap().to_bytes();
                    let v: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
                    let resp = if v.is_array() {
                        json!([
                            {"jsonrpc":"2.0","id":1,"result":false},
                            {"jsonrpc":"2.0","id":2,"result":"0x4"},
                            {"jsonrpc":"2.0","id":3,"result":{"number":"0x1","timestamp":"0x1"}}
                        ])
                    } else {
                        json!({"jsonrpc":"2.0","id":1,"result":"0x1"})
                    };
                    Ok::<_, Infallible>(
                        Response::builder()
                            .status(StatusCode::OK)
                            .header("content-type", "application/json")
                            .body(Full::new(Bytes::from(serde_json::to_vec(&resp).unwrap())))
                            .unwrap(),
                    )
                });
                if h2 {
                    let _ = auto::Builder::new(TokioExecutor::new())
                        .serve_connection(io, svc)
                        .await;
                } else {
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(io, svc)
                        .await;
                }
            });
            handles.lock().unwrap().push(h);
        }
    });

    let cl = MockServer::start(Arc::new(|req| match req.path.as_str() {
        "/eth/v1/node/syncing" => (
            StatusCode::OK,
            br#"{"data":{"is_syncing":false,"sync_distance":"0","head_slot":"1"}}"#.to_vec(),
        ),
        "/eth/v1/node/peer_count" => (StatusCode::OK, br#"{"data":{"connected":"4"}}"#.to_vec()),
        _ => (StatusCode::NOT_FOUND, Vec::new()),
    }))
    .await;

    let proxy = EthonyxHandle::start(&[
        "--health-poll-interval",
        "1",
        "--el-http-url",
        &el_url,
        "--cl-beacon-url",
        &cl.url,
    ])
    .await;
    let c = client();

    // 1. Initial poll confirms h2c (`H2C_CONFIRMED`).
    let (_, hz1) = get(&c, &proxy.url("/healthz")).await;
    let v1: Value = serde_json::from_slice(&hz1).unwrap();
    assert_eq!(v1["el"]["transport"], "h2c");

    // 2. Simulate transient h2c rejection (h1-only fallback -> `H1_VERIFY_ONCE`):
    // flip `allow_h2c = false` and drop pooled server connections.
    allow_h2c.store(false, Ordering::Release);
    for h in conn_handles.lock().unwrap().drain(..) {
        h.abort();
    }

    let mut entered_fallback = false;
    for _ in 0..30 {
        let (_, hz) = get(&c, &proxy.url("/healthz")).await;
        let v: Value = serde_json::from_slice(&hz).unwrap();
        if v["el"]["transport"] == "http/1.1" {
            // Restore h2c immediately so the `H1_VERIFY_ONCE` poll sees h2c back up!
            allow_h2c.store(true, Ordering::Release);
            entered_fallback = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(80)).await;
    }
    assert!(
        entered_fallback,
        "expected fallback to http/1.1 when h2c dropped"
    );

    // 3. `H1_VERIFY_ONCE` probes h2c once more and self-heals back to `H2C_CONFIRMED`.
    let mut recovered_h2c = false;
    for _ in 0..30 {
        let (_, hz) = get(&c, &proxy.url("/healthz")).await;
        let v: Value = serde_json::from_slice(&hz).unwrap();
        if v["el"]["transport"] == "h2c" {
            recovered_h2c = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(80)).await;
    }
    assert!(
        recovered_h2c,
        "confirmed h2c EL must self-heal back to h2c on the H1_VERIFY_ONCE poll"
    );

    proxy.shutdown().await;
    el_server.abort();
}

#[cfg(feature = "otel")]
#[tokio::test]
async fn traceparent_survives_connection_nomination_and_propagates_to_websocket() {
    use std::process::{Child, Command, Stdio};
    struct KillOnDrop(Child);
    impl Drop for KillOnDrop {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    let collector = MockServer::start(ok_handler()).await;
    let el = MockServer::start(ok_handler()).await;
    let cl = MockServer::start(ok_handler()).await;
    let ws_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws_url = format!("ws://{}", ws_listener.local_addr().unwrap());
    let (ws_tp_tx, ws_tp_rx) = oneshot::channel::<Option<String>>();
    let ws_server = tokio::spawn(async move {
        let (sock, _) = ws_listener.accept().await.unwrap();
        let mut captured = None;
        let ws = tokio_tungstenite::accept_hdr_async(
            sock,
            #[allow(clippy::result_large_err)]
            |req: &tokio_tungstenite::tungstenite::handshake::server::Request, resp| {
                captured = req
                    .headers()
                    .get("traceparent")
                    .and_then(|v| v.to_str().ok())
                    .map(str::to_owned);
                Ok(resp)
            },
        )
        .await
        .unwrap();
        let _ = ws_tp_tx.send(captured);
        drop(ws);
    });

    let port = pick_port().await;
    let _child = KillOnDrop(
        Command::new(env!("CARGO_BIN_EXE_ethonyx"))
            .args([
                "--listen",
                &format!("127.0.0.1:{port}"),
                "--trust-upstream",
                "--el-http-url",
                &el.url,
                "--el-ws-url",
                &ws_url,
                "--cl-beacon-url",
                &cl.url,
                "--otel-endpoint",
                &collector.url,
            ])
            .env("RUST_LOG", "info")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    wait_for_port(port).await;

    // 1. HTTP request nominating `Connection: keep-alive, traceparent` still injects
    // ethonyx's outgoing `traceparent` header upstream.
    let c = client();
    let req = Request::builder()
        .method(Method::POST)
        .uri(format!("http://127.0.0.1:{port}/"))
        .header("connection", "keep-alive, traceparent")
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from_static(
            br#"{"jsonrpc":"2.0","method":"eth_blockNumber","id":1}"#,
        )))
        .unwrap();
    let resp = c.request(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let http_tp = el
        .received()
        .into_iter()
        .find_map(|r| r.headers.get("traceparent").cloned());
    assert!(
        http_tp.is_some(),
        "Connection: traceparent must not strip ethonyx's outgoing traceparent"
    );

    // 2. WebSocket handshake propagates `traceparent` to the upstream WS server.
    let mut ws_req =
        tokio_tungstenite::tungstenite::client::IntoClientRequest::into_client_request(format!(
            "ws://127.0.0.1:{port}/"
        ))
        .unwrap();
    ws_req.headers_mut().insert(
        "traceparent",
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
            .parse()
            .unwrap(),
    );
    let (mut ws, _) = connect_async(ws_req).await.unwrap();
    let ws_tp = ws_tp_rx
        .await
        .unwrap()
        .expect("upstream WS handshake must receive traceparent");
    assert!(
        ws_tp.contains("4bf92f3577b34da6a3ce929d0e0e4736"),
        "expected propagated WS trace ID, got: {ws_tp}"
    );
    let _ = ws.close(None).await;

    ws_server.await.unwrap();
}

#[tokio::test]
async fn multi_listener_max_connections_fairness_and_h2_extended_connect_cap() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let ws_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws_url = format!("ws://{}", ws_listener.local_addr().unwrap());
    let ws_dials = Arc::new(AtomicUsize::new(0));
    let ws_dials_srv = ws_dials.clone();
    let ws_server = tokio::spawn(async move {
        while let Ok((sock, _)) = ws_listener.accept().await {
            ws_dials_srv.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(async move {
                if let Ok(mut ws) = accept_async(sock).await {
                    while let Some(Ok(msg)) = ws.next().await {
                        if msg.is_close() {
                            let _ = ws.close(None).await;
                            break;
                        }
                        if ws.send(msg).await.is_err() {
                            break;
                        }
                    }
                }
            });
        }
    });

    let second_port = pick_port().await;
    let second_listen = format!("127.0.0.1:{second_port}");
    let proxy = EthonyxHandle::start(&[
        "--listen",
        &second_listen,
        "--trust-upstream",
        "--max-connections",
        "1",
        "--el-ws-url",
        &ws_url,
    ])
    .await;

    // 1. Even with --max-connections 1 and an idle second listener, repeated
    // connections on either port succeed without permit starvation.
    for port in [proxy.port, proxy.port, second_port, proxy.port] {
        let mut sock = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .unwrap();
        sock.write_all(b"GET /livez HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut buf = Vec::new();
        sock.read_to_end(&mut buf).await.unwrap();
        assert!(buf.starts_with(b"HTTP/1.1 200 OK"));
    }

    // 2. Over a single HTTP/2 connection under --max-connections 1:
    //    - 1st Extended CONNECT WS stream reuses the H2 connection's permit (200 OK).
    //    - 2nd concurrent Extended CONNECT WS stream is rejected with 503 before dialing.
    //    - Closing the 1st stream frees the slot for reuse.
    let tcp = tokio::net::TcpStream::connect(("127.0.0.1", proxy.port))
        .await
        .unwrap();
    let (mut sender, conn) = hyper::client::conn::http2::Builder::new(TokioExecutor::new())
        .handshake::<_, TestBody>(TokioIo::new(tcp))
        .await
        .unwrap();
    let h2_conn = tokio::spawn(async move {
        let _ = conn.await;
    });

    let make_ext_connect = |port: u16| {
        let mut req = Request::builder()
            .method(Method::CONNECT)
            .uri(format!("http://127.0.0.1:{port}/"))
            .header("sec-websocket-version", "13")
            .body(Full::new(Bytes::new()))
            .unwrap();
        req.extensions_mut()
            .insert(hyper::ext::Protocol::from_static("websocket"));
        req
    };

    let resp1 = sender
        .send_request(make_ext_connect(proxy.port))
        .await
        .unwrap();
    assert_eq!(resp1.status(), StatusCode::OK);
    let upgraded1 = hyper::upgrade::on(resp1).await.unwrap();
    let mut ws1 = tokio_tungstenite::WebSocketStream::from_raw_socket(
        TokioIo::new(upgraded1),
        tokio_tungstenite::tungstenite::protocol::Role::Client,
        None,
    )
    .await;
    ws1.send(Message::Text("hello".into())).await.unwrap();
    assert_eq!(
        ws1.next().await.unwrap().unwrap(),
        Message::Text("hello".into())
    );
    assert_eq!(ws_dials.load(Ordering::SeqCst), 1);

    let resp2 = sender
        .send_request(make_ext_connect(proxy.port))
        .await
        .unwrap();
    assert_eq!(resp2.status(), StatusCode::SERVICE_UNAVAILABLE);
    drop(resp2);
    assert_eq!(
        ws_dials.load(Ordering::SeqCst),
        1,
        "saturated stream must not dial upstream"
    );

    ws1.close(None).await.unwrap();
    while let Some(Ok(msg)) = ws1.next().await {
        if msg.is_close() {
            break;
        }
    }
    drop(ws1);

    // Wait briefly for the spawned bridge task to drop its IncludedH2 permit.
    let mut reused_ok = false;
    for _ in 0..20 {
        let resp3 = sender
            .send_request(make_ext_connect(proxy.port))
            .await
            .unwrap();
        if resp3.status() == StatusCode::OK {
            let upgraded3 = hyper::upgrade::on(resp3).await.unwrap();
            drop(upgraded3);
            reused_ok = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(reused_ok, "closed H2 WS stream must release slot for reuse");

    drop(sender);
    h2_conn.abort();
    let _ = h2_conn.await;

    // Saturation metric is exposed on /metrics once the H2 TCP connection closes.
    let c = client();
    let metrics_resp = tokio::time::timeout(
        Duration::from_secs(3),
        c.get(proxy.url("/metrics").parse().unwrap()),
    )
    .await
    .expect("closing H2 connection must release --max-connections 1 permit")
    .unwrap();
    let metrics_text = String::from_utf8(
        metrics_resp
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    assert!(
        metrics_text.contains("ethonyx_connection_limit_saturated_total"),
        "expected saturation counter in /metrics, got:\n{metrics_text}"
    );

    proxy.shutdown().await;
    ws_server.abort();
}

#[tokio::test]
async fn uppercase_ws_scheme_and_non_origin_targets_work_safely() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let el = MockServer::start(ok_handler()).await;
    let ws_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let uppercase_ws_url = format!("WS://{}/ws", ws_listener.local_addr().unwrap());
    let ws_server = tokio::spawn(async move {
        let (sock, _) = ws_listener.accept().await.unwrap();
        let mut ws = accept_async(sock).await.unwrap();
        if let Some(Ok(msg)) = ws.next().await {
            let _ = ws.send(msg).await;
        }
        let _ = ws.close(None).await;
    });

    let scoped_el = format!("{}/v3/SECRET", el.url);
    let proxy = EthonyxHandle::start(&[
        "--trust-upstream",
        "--el-http-url",
        &scoped_el,
        "--el-ws-url",
        &uppercase_ws_url,
    ])
    .await;

    // 1. Uppercase WS:// scheme connects and echoes cleanly.
    let (mut ws, resp) = connect_async(format!("ws://127.0.0.1:{}/", proxy.port))
        .await
        .expect("uppercase WS:// upstream must succeed");
    assert_eq!(resp.status(), StatusCode::SWITCHING_PROTOCOLS);
    ws.send(Message::Text("upper-ok".into())).await.unwrap();
    assert_eq!(
        ws.next().await.unwrap().unwrap(),
        Message::Text("upper-ok".into())
    );
    let _ = ws.close(None).await;

    // 2. Asterisk-form `OPTIONS *` and authority-form `CONNECT host:port` return 400
    // locally without forwarding or corrupting `/v3/SECRET`.
    for raw_req in [
        "OPTIONS * HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        "CONNECT example.com:443 HTTP/1.1\r\nHost: example.com\r\nConnection: close\r\n\r\n",
    ] {
        let mut sock = tokio::net::TcpStream::connect(("127.0.0.1", proxy.port))
            .await
            .unwrap();
        sock.write_all(raw_req.as_bytes()).await.unwrap();
        let mut buf = Vec::new();
        sock.read_to_end(&mut buf).await.unwrap();
        assert!(
            buf.starts_with(b"HTTP/1.1 400 Bad Request"),
            "expected 400 for {raw_req:?}, got: {}",
            String::from_utf8_lossy(&buf)
        );
    }
    assert!(
        el.received().is_empty(),
        "non-origin targets must never reach upstream"
    );

    proxy.shutdown().await;
    ws_server.await.unwrap();
}

#[tokio::test]
async fn post_accept_permits_allow_two_h2_ws_streams_and_reject_invalid_or_oversized_targets() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn sum_metric_lines(metrics_text: &str, needle: &str) -> u64 {
        metrics_text
            .lines()
            .filter(|l| !l.starts_with('#') && l.contains(needle))
            .filter_map(|l| l.split_whitespace().last()?.parse::<u64>().ok())
            .sum()
    }

    let el = MockServer::start(ok_handler()).await;
    let ws_upstream = ws_echo_upstream().await;
    let long_prefix = "k".repeat(62_000);
    let scoped_el = format!("{}/v3/{long_prefix}", el.url);
    let scoped_ws = format!("{ws_upstream}/v3/{long_prefix}");

    let proxy = EthonyxHandle::start(&[
        "--trust-upstream",
        "--max-connections",
        "2",
        "--el-http-url",
        &scoped_el,
        "--el-ws-url",
        &scoped_ws,
    ])
    .await;

    let tcp = tokio::net::TcpStream::connect(("127.0.0.1", proxy.port))
        .await
        .unwrap();
    let (mut sender, conn) = hyper::client::conn::http2::Builder::new(TokioExecutor::new())
        .handshake::<_, TestBody>(TokioIo::new(tcp))
        .await
        .unwrap();
    let h2_conn = tokio::spawn(async move {
        let _ = conn.await;
    });

    let fetch_metrics = |sender: &mut hyper::client::conn::http2::SendRequest<TestBody>| {
        let req = Request::builder()
            .method(Method::GET)
            .uri(format!("http://127.0.0.1:{}/metrics", proxy.port))
            .body(Full::new(Bytes::new()))
            .unwrap();
        let fut = sender.send_request(req);
        async move {
            let resp = fut.await.unwrap();
            assert_eq!(resp.status(), StatusCode::OK);
            String::from_utf8(
                resp.into_body()
                    .collect()
                    .await
                    .unwrap()
                    .to_bytes()
                    .to_vec(),
            )
            .unwrap()
        }
    };

    let m0 = fetch_metrics(&mut sender).await;
    let sat0 = sum_metric_lines(&m0, "ethonyx_connection_limit_saturated_total");
    let opts0 = sum_metric_lines(&m0, "method=\"OPTIONS\"");
    let conn0 = sum_metric_lines(&m0, "method=\"CONNECT\"");

    // 1. Rejected targets (`OPTIONS *`, authority `CONNECT`, and CL `CONNECT`) return 400
    // BEFORE `ethonyx_proxy_requests_total` is recorded!
    let mut raw_sock = tokio::net::TcpStream::connect(("127.0.0.1", proxy.port))
        .await
        .unwrap();
    raw_sock
        .write_all(b"OPTIONS * HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut raw_resp = Vec::new();
    raw_sock.read_to_end(&mut raw_resp).await.unwrap();
    assert!(raw_resp.starts_with(b"HTTP/1.1 400 Bad Request"));
    drop(raw_sock);

    let mut cl_connect = Request::builder()
        .method(Method::CONNECT)
        .uri(format!(
            "http://127.0.0.1:{}/eth/v1/node/health",
            proxy.port
        ))
        .body(Full::new(Bytes::new()))
        .unwrap();
    cl_connect
        .extensions_mut()
        .insert(hyper::ext::Protocol::from_static("websocket"));
    let cl_connect_resp = sender.send_request(cl_connect).await.unwrap();
    assert_eq!(cl_connect_resp.status(), StatusCode::BAD_REQUEST);
    drop(cl_connect_resp);

    let m1 = fetch_metrics(&mut sender).await;
    assert_eq!(
        sum_metric_lines(&m1, "method=\"OPTIONS\""),
        opts0,
        "OPTIONS * must not increment ethonyx_proxy_requests_total"
    );
    assert_eq!(
        sum_metric_lines(&m1, "method=\"CONNECT\""),
        conn0,
        "rejected CONNECT must not increment ethonyx_proxy_requests_total"
    );

    // 2. Oversized rewritten URIs (> u16::MAX after joining scoped upstream prefix)
    // return 414 URI Too Long instead of 502 Bad Gateway on both HTTP and WS paths.
    let near_limit_path = format!("/{}", "a".repeat(4_096));
    let long_http = Request::builder()
        .method(Method::POST)
        .uri(&near_limit_path)
        .body(Full::new(Bytes::from_static(b"{}")))
        .unwrap();
    let long_http_resp = sender.send_request(long_http).await.unwrap();
    assert_eq!(long_http_resp.status(), StatusCode::URI_TOO_LONG);
    drop(long_http_resp);

    let mut long_ws = Request::builder()
        .method(Method::CONNECT)
        .uri(&near_limit_path)
        .header("sec-websocket-version", "13")
        .body(Full::new(Bytes::new()))
        .unwrap();
    long_ws
        .extensions_mut()
        .insert(hyper::ext::Protocol::from_static("websocket"));
    let long_ws_resp = sender.send_request(long_ws).await.unwrap();
    assert_eq!(long_ws_resp.status(), StatusCode::URI_TOO_LONG);
    drop(long_ws_resp);

    // 3. Under --max-connections 2, post-accept permit acquisition leaves the 2nd
    // permit free so 1 H2 connection can open TWO concurrent Extended CONNECT WS
    // streams (`IncludedH2` + `DedicatedH2`) with zero spurious saturation increments,
    // and rejects the 3rd concurrent stream with 503 (+1 saturation).
    let make_ext_ws = || {
        let mut req = Request::builder()
            .method(Method::CONNECT)
            .uri(format!("http://127.0.0.1:{}/", proxy.port))
            .header("sec-websocket-version", "13")
            .body(Full::new(Bytes::new()))
            .unwrap();
        req.extensions_mut()
            .insert(hyper::ext::Protocol::from_static("websocket"));
        req
    };

    let ws_resp1 = sender.send_request(make_ext_ws()).await.unwrap();
    assert_eq!(ws_resp1.status(), StatusCode::OK);
    let up1 = hyper::upgrade::on(ws_resp1).await.unwrap();
    let mut ws1 = tokio_tungstenite::WebSocketStream::from_raw_socket(
        TokioIo::new(up1),
        tokio_tungstenite::tungstenite::protocol::Role::Client,
        None,
    )
    .await;

    let ws_resp2 = sender.send_request(make_ext_ws()).await.unwrap();
    assert_eq!(
        ws_resp2.status(),
        StatusCode::OK,
        "2nd H2 Extended CONNECT stream must acquire the 2nd permit under --max-connections 2"
    );
    let up2 = hyper::upgrade::on(ws_resp2).await.unwrap();
    let mut ws2 = tokio_tungstenite::WebSocketStream::from_raw_socket(
        TokioIo::new(up2),
        tokio_tungstenite::tungstenite::protocol::Role::Client,
        None,
    )
    .await;

    let m2 = fetch_metrics(&mut sender).await;
    assert_eq!(
        sum_metric_lines(&m2, "ethonyx_connection_limit_saturated_total"),
        sat0,
        "saturation counter must stay unchanged before actual over-limit contention"
    );

    let ws_resp3 = sender.send_request(make_ext_ws()).await.unwrap();
    assert_eq!(ws_resp3.status(), StatusCode::SERVICE_UNAVAILABLE);
    drop(ws_resp3);

    let m3 = fetch_metrics(&mut sender).await;
    assert_eq!(
        sum_metric_lines(&m3, "ethonyx_connection_limit_saturated_total"),
        sat0 + 1,
        "3rd concurrent H2 WS stream under --max-connections 2 must record +1 saturation"
    );

    let _ = ws1.close(None).await;
    let _ = ws2.close(None).await;
    drop((ws1, ws2, sender));
    h2_conn.abort();
    let _ = h2_conn.await;
    proxy.shutdown().await;
}

#[tokio::test]
async fn http2_extended_connect_large_frame_slow_reader_completes_past_proxy_timeout() {
    use std::pin::Pin;
    use std::task::{Context, Poll};

    struct ThrottledRead<I> {
        inner: I,
        chunk_limit: usize,
        bytes_since_sleep: usize,
        delay: Duration,
        sleep: Option<Pin<Box<tokio::time::Sleep>>>,
    }

    impl<I: tokio::io::AsyncRead + Unpin> tokio::io::AsyncRead for ThrottledRead<I> {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            let this = &mut *self;
            if let Some(sleep) = this.sleep.as_mut() {
                if sleep.as_mut().poll(cx).is_pending() {
                    return Poll::Pending;
                }
                this.sleep = None;
            }
            let budget = this.chunk_limit.saturating_sub(this.bytes_since_sleep);
            let cap = buf.remaining().min(budget.max(1));
            let mut sub = buf.take(cap);
            let poll = Pin::new(&mut this.inner).poll_read(cx, &mut sub);
            if let Poll::Ready(Ok(())) = poll {
                let n = sub.filled().len();
                if n > 0 {
                    buf.advance(n);
                    this.bytes_since_sleep += n;
                    if this.bytes_since_sleep >= this.chunk_limit {
                        this.bytes_since_sleep = 0;
                        this.sleep = Some(Box::pin(tokio::time::sleep(this.delay)));
                    }
                }
            }
            poll
        }
    }

    impl<I: tokio::io::AsyncWrite + Unpin> tokio::io::AsyncWrite for ThrottledRead<I> {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            Pin::new(&mut self.inner).poll_write(cx, buf)
        }

        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.inner).poll_flush(cx)
        }

        fn poll_shutdown(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.inner).poll_shutdown(cx)
        }
    }

    let payload_len = 1536 * 1024; // 1.5 MiB (24x H2 default 65_535-byte window & 24x 64 KiB cap)
    let ws_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws_url = format!("ws://{}", ws_listener.local_addr().unwrap());
    let ws_server = tokio::spawn(async move {
        let (sock, _) = ws_listener.accept().await.unwrap();
        let mut ws = accept_async(sock).await.unwrap();
        ws.send(Message::Binary(Bytes::from(vec![0x5A; payload_len])))
            .await
            .unwrap();
        ws.send(Message::Text("tail-after-large-h2-frame".into()))
            .await
            .unwrap();
        while let Some(Ok(msg)) = ws.next().await {
            if msg.is_close() {
                break;
            }
        }
    });

    let el = MockServer::start(ok_handler()).await;
    let proxy = EthonyxHandle::start(&[
        "--trust-upstream",
        "--el-http-url",
        &el.url,
        "--el-ws-url",
        &ws_url,
    ])
    .await;

    let tcp = tokio::net::TcpStream::connect(("127.0.0.1", proxy.port))
        .await
        .unwrap();
    let (mut sender, conn) = hyper::client::conn::http2::Builder::new(TokioExecutor::new())
        .initial_stream_window_size(65_535)
        .initial_connection_window_size(65_535)
        .handshake::<_, TestBody>(TokioIo::new(tcp))
        .await
        .unwrap();
    let h2_conn = tokio::spawn(async move {
        let _ = conn.await;
    });

    let mut req = Request::builder()
        .method(Method::CONNECT)
        .uri(format!("http://127.0.0.1:{}/", proxy.port))
        .header("sec-websocket-version", "13")
        .body(Full::new(Bytes::new()))
        .unwrap();
    req.extensions_mut()
        .insert(hyper::ext::Protocol::from_static("websocket"));

    let resp = sender.send_request(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let upgraded = hyper::upgrade::on(resp).await.unwrap();

    // Throttle reads to 32 KiB every 80ms (~400 KiB/s): 1.5 MiB takes ~3.8s (> 2s --proxy-timeout).
    let throttled = ThrottledRead {
        inner: TokioIo::new(upgraded),
        chunk_limit: 32 * 1024,
        bytes_since_sleep: 0,
        delay: Duration::from_millis(80),
        sleep: None,
    };
    let mut ws = tokio_tungstenite::WebSocketStream::from_raw_socket(
        throttled,
        tokio_tungstenite::tungstenite::protocol::Role::Client,
        None,
    )
    .await;

    let started = std::time::Instant::now();
    let first = ws
        .next()
        .await
        .expect("first frame")
        .expect("large frame ok");
    assert!(
        matches!(&first, Message::Binary(b) if b.len() == payload_len && b.iter().all(|&x| x == 0x5A)),
        "expected intact {payload_len}-byte binary frame"
    );
    let second = ws
        .next()
        .await
        .expect("second frame")
        .expect("tail frame ok");
    assert_eq!(second, Message::Text("tail-after-large-h2-frame".into()));
    assert!(
        started.elapsed() > Duration::from_millis(2500),
        "transfer must span past --proxy-timeout 2s (took {:?})",
        started.elapsed()
    );

    ws.close(None).await.unwrap();
    while let Some(Ok(msg)) = ws.next().await {
        if msg.is_close() {
            break;
        }
    }
    ws_server.await.unwrap();
    drop((ws, sender));
    h2_conn.abort();
    let _ = h2_conn.await;
    proxy.shutdown().await;
}

#[tokio::test]
async fn shutdown_during_initial_health_probe_exits_promptly_without_binding_listener() {
    use clap::Parser;

    // Hanging EL + CL TCP listeners that accept and never reply for 30s.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let hang_url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((sock, _)) = listener.accept().await {
            held.push(sock);
        }
    });

    let cfg = ethonyx::Config::parse_from([
        "ethonyx",
        "--listen",
        "127.0.0.1:0",
        "--health-timeout",
        "30",
        "--el-http-url",
        &hang_url,
        "--cl-beacon-url",
        &hang_url,
    ]);

    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let run_task = tokio::spawn(async move {
        ethonyx::run(cfg, async move {
            let _ = stop_rx.await;
        })
        .await
    });

    tokio::time::sleep(Duration::from_millis(80)).await;
    let _ = stop_tx.send(());

    let res = tokio::time::timeout(Duration::from_secs(1), run_task)
        .await
        .expect("shutdown during initial health probe must return within 1s")
        .unwrap();
    assert!(res.is_ok());
    server.abort();
}

#[tokio::test]
async fn shutdown_sends_going_away_close_frame_to_active_h1_and_h2_websockets() {
    use tokio_tungstenite::tungstenite::protocol::{Role, frame::coding::CloseCode};

    let upstream = ws_echo_upstream().await;
    let mut proxy = EthonyxHandle::start_with_grace(
        &["--trust-upstream", "--el-ws-url", &upstream],
        Duration::from_secs(1),
    )
    .await;

    // 1. HTTP/1.1 Upgrade WebSocket subscriber.
    let (mut ws_h1, _) = connect_async(format!("ws://127.0.0.1:{}/", proxy.port))
        .await
        .unwrap();

    // 2. HTTP/2 Extended CONNECT WebSocket subscriber.
    let tcp = tokio::net::TcpStream::connect(("127.0.0.1", proxy.port))
        .await
        .unwrap();
    let (mut sender, h2_conn) = hyper::client::conn::http2::Builder::new(TokioExecutor::new())
        .handshake::<_, TestBody>(TokioIo::new(tcp))
        .await
        .unwrap();
    let h2_driver = tokio::spawn(async move {
        let _ = h2_conn.await;
    });
    let mut req = Request::builder()
        .method(Method::CONNECT)
        .uri(proxy.url("/"))
        .header("sec-websocket-version", "13")
        .body(Full::new(Bytes::new()))
        .unwrap();
    req.extensions_mut()
        .insert(hyper::ext::Protocol::from_static("websocket"));
    let resp = sender.send_request(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let upgraded = hyper::upgrade::on(resp).await.unwrap();
    let mut ws_h2 = tokio_tungstenite::WebSocketStream::from_raw_socket(
        TokioIo::new(upgraded),
        Role::Client,
        None,
    )
    .await;

    let started = tokio::time::Instant::now();
    proxy.shutdown.take().unwrap().send(()).unwrap();

    // Both H1 and H2 relays remain fully operational during the drain window.
    tokio::time::sleep(Duration::from_millis(100)).await;
    ws_h1
        .send(Message::Text("mid-drain-h1".into()))
        .await
        .unwrap();
    assert_eq!(
        ws_h1.next().await.unwrap().unwrap(),
        Message::Text("mid-drain-h1".into())
    );
    ws_h2
        .send(Message::Text("mid-drain-h2".into()))
        .await
        .unwrap();
    assert_eq!(
        ws_h2.next().await.unwrap().unwrap(),
        Message::Text("mid-drain-h2".into())
    );

    async fn read_going_away<S>(mut ws: tokio_tungstenite::WebSocketStream<S>)
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        let msg = tokio::time::timeout(Duration::from_secs(2), ws.next())
            .await
            .expect("Close frame before timeout")
            .expect("stream item")
            .expect("clean Close frame without reset");
        let Message::Close(Some(frame)) = msg else {
            panic!("expected Close(Some(..)), got {msg:?}");
        };
        assert_eq!(frame.code, CloseCode::Away);
        assert_eq!(frame.reason.as_str(), "server shutting down");
        assert!(
            tokio::time::timeout(Duration::from_secs(1), ws.next())
                .await
                .expect("EOF after Close handshake")
                .is_none(),
            "peer must observe clean EOF after Close handshake"
        );
    }

    let ((), (), ()) = tokio::join!(
        read_going_away(ws_h1),
        read_going_away(ws_h2),
        proxy.shutdown(),
    );
    assert!(
        started.elapsed() >= Duration::from_millis(800)
            && started.elapsed() < Duration::from_millis(1500),
        "active relays should close just before grace expiry (elapsed = {:?})",
        started.elapsed()
    );

    drop(sender);
    h2_driver.abort();
    let _ = h2_driver.await;
}

#[tokio::test]
async fn query_only_upstream_url_reaches_h2c_and_h1_as_root_path() {
    for h2 in [true, false] {
        let handler: MockHandler = Arc::new(|_| {
            (
                StatusCode::OK,
                br#"{"jsonrpc":"2.0","id":7,"result":"0xcafe"}"#.to_vec(),
            )
        });
        let el = if h2 {
            MockServer::start_h2c(handler).await
        } else {
            MockServer::start(handler).await
        };
        let cl = MockServer::start(ok_handler()).await;
        let el_url = format!("{}?key=abc", el.url);
        let ethonyx =
            EthonyxHandle::start(&["--el-http-url", &el_url, "--cl-beacon-url", &cl.url]).await;

        let c = client();
        let (status, _) = post_json_settled(
            &c,
            &ethonyx.url("/"),
            json!({"jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id": 7}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let (status, _) = post_json(
            &c,
            &ethonyx.url("/?tag=x"),
            json!({"jsonrpc": "2.0", "method": "eth_chainId", "params": [], "id": 7}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);

        let received = el.received();
        let find_req = |needle: &[u8]| {
            received
                .iter()
                .find(|r| r.body.windows(needle.len()).any(|w| w == needle))
                .expect("EL received forwarded request")
        };
        let plain = find_req(b"eth_blockNumber");
        let with_query = find_req(b"eth_chainId");
        let expected_version = if h2 {
            http::Version::HTTP_2
        } else {
            http::Version::HTTP_11
        };
        assert_eq!(plain.version, expected_version);
        assert_eq!(plain.uri.path_and_query().unwrap().as_str(), "/?key=abc");
        assert_eq!(
            with_query.uri.path_and_query().unwrap().as_str(),
            "/?key=abc&tag=x"
        );

        ethonyx.shutdown().await;
    }
}
