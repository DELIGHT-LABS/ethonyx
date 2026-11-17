//! Integration tests: spawn ethonyx in-process against a hyper-based mock
//! upstream, driving the full proxy / readiness / health flow with the same
//! hyper-util Client used in production code. Zero extra dev-dependencies.

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

use ethonyx::{Config, Prepared};

type TestBody = Full<Bytes>;
type TestClient = Client<HttpConnector, TestBody>;
type BoxError = Box<dyn std::error::Error + Send + Sync>;

// ---------- helpers ----------

/// Reserves an ephemeral loopback TCP port without calling `listen()`, preventing
/// any concurrent test from binding the same port until this guard drops.
/// Connection attempts fail immediately with `ConnectionRefused` on Linux, while
/// BSD/macOS drops incoming `SYN`s on bound non-listening sockets and exercises
/// the `--health-timeout` / `--proxy-timeout` path instead.
struct DeadPort {
    _socket: tokio::net::TcpSocket,
    port: u16,
}

impl DeadPort {
    fn new() -> Self {
        let socket = tokio::net::TcpSocket::new_v4().unwrap();
        socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let port = socket.local_addr().unwrap().port();
        Self {
            _socket: socket,
            port,
        }
    }

    fn http_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    fn ws_url(&self) -> String {
        format!("ws://127.0.0.1:{}", self.port)
    }
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
    ports: Vec<u16>,
    shutdown: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<Result<(), BoxError>>>,
}

impl EthonyxHandle {
    async fn start(args: &[&str]) -> Self {
        Self::start_with_grace(args, Duration::ZERO).await
    }

    async fn start_with_grace(args: &[&str], grace: Duration) -> Self {
        let grace_str = grace.as_secs().to_string();
        let mut argv: Vec<&str> = vec![
            "ethonyx",
            "--listen",
            "127.0.0.1:0",
            "--shutdown-grace",
            &grace_str,
            "--health-timeout",
            "1",
            "--proxy-timeout",
            "2",
        ];
        argv.extend_from_slice(args);
        let cfg = Config::try_parse_from(argv).expect("clap parse");
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
            task.await
                .expect("ethonyx serve task panicked")
                .expect("ethonyx serve returned error");
        }
    }

    async fn connect_h2_ws(
        &self,
        path: &str,
    ) -> (
        tokio_tungstenite::WebSocketStream<TokioIo<hyper::upgrade::Upgraded>>,
        hyper::client::conn::http2::SendRequest<TestBody>,
        JoinHandle<()>,
    ) {
        let tcp = tokio::net::TcpStream::connect(("127.0.0.1", self.port))
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
            .uri(self.url(path))
            .header("sec-websocket-version", "13")
            .body(Full::new(Bytes::new()))
            .unwrap();
        req.extensions_mut()
            .insert(hyper::ext::Protocol::from_static("websocket"));
        let resp = sender.send_request(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let upgraded = hyper::upgrade::on(resp).await.unwrap();
        let ws = tokio_tungstenite::WebSocketStream::from_raw_socket(
            TokioIo::new(upgraded),
            tokio_tungstenite::tungstenite::protocol::Role::Client,
            None,
        )
        .await;
        (ws, sender, h2_driver)
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
