//! OpenTelemetry W3C context propagation, OTLP export flush on `SIGTERM`, and startup failure cleanup tests.

use super::*;
use serde_json::json;
use tokio_tungstenite::connect_async;

struct KillOnDrop {
    child: std::process::Child,
    stderr_buf: Arc<Mutex<String>>,
    stderr_reader: Option<std::thread::JoinHandle<()>>,
}

impl KillOnDrop {
    fn spawn(mut cmd: std::process::Command) -> Self {
        let mut child = cmd.spawn().unwrap();
        let stderr_buf = Arc::new(Mutex::new(String::new()));
        let reader = child.stderr.take().map(|mut pipe| {
            let buf = Arc::clone(&stderr_buf);
            std::thread::spawn(move || {
                let mut captured = String::new();
                let _ = std::io::Read::read_to_string(&mut pipe, &mut captured);
                *buf.lock().unwrap() = captured;
            })
        });
        Self {
            child,
            stderr_buf,
            stderr_reader: reader,
        }
    }

    fn stderr_snapshot(&self) -> String {
        self.stderr_buf.lock().unwrap().clone()
    }
}

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if let Some(handle) = self.stderr_reader.take() {
            let _ = handle.join();
        }
    }
}

async fn spawn_binary_on_ephemeral_port(
    mut configure: impl FnMut(&mut std::process::Command),
) -> (KillOnDrop, u16) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    for attempt in 0..5 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        let mut cmd = std::process::Command::new(env!("CARGO_BIN_EXE_ethonyx"));
        cmd.args(["--listen", &format!("127.0.0.1:{port}")])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped());
        configure(&mut cmd);
        let mut child = KillOnDrop::spawn(cmd);

        for _ in 0..250 {
            if let Some(status) = child.child.try_wait().unwrap() {
                // Allow the stderr reader thread to finish draining EOF.
                if let Some(handle) = child.stderr_reader.take() {
                    let _ = handle.join();
                }
                let stderr = child.stderr_snapshot();
                if attempt < 4 && stderr.contains("failed to bind --listen address") {
                    break;
                }
                panic!("ethonyx binary exited prematurely with {status} on port {port}: {stderr}");
            }
            // Probe `/livez` directly over a fresh `Connection: close` socket so startup
            // readiness never pollutes upstream mock logs or emits proxy spans/metrics.
            let probed = tokio::time::timeout(Duration::from_millis(250), async {
                let mut sock = tokio::net::TcpStream::connect(("127.0.0.1", port))
                    .await
                    .ok()?;
                sock.write_all(
                    b"GET /livez HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
                )
                .await
                .ok()?;
                let mut buf = Vec::new();
                let _ = sock.read_to_end(&mut buf).await;
                Some(buf)
            })
            .await;
            if let Ok(Some(resp)) = probed
                && resp.starts_with(b"HTTP/1.1 200 OK")
                && resp.ends_with(b"\r\n\r\nok")
            {
                return (child, port);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    panic!("ethonyx subprocess failed to become ready after retries");
}

/// With a real `tracing-opentelemetry` subscriber installed, ethonyx extracts incoming
/// `traceparent` + multi-line `tracestate`, generates a child `span-id`, forwards the
/// updated W3C headers to upstream, and flushes the exported trace batch to the collector
/// on shutdown.
#[tokio::test]
async fn trace_context_is_propagated_to_upstream() {
    use tracing_subscriber::prelude::*;

    let collector = MockServer::start(ok_handler()).await;
    let el = MockServer::start(ok_handler()).await;
    let cl = MockServer::start(ok_handler()).await;

    let cfg = Config::parse_from([
        "ethonyx",
        "--listen",
        "127.0.0.1:0",
        "--trust-upstream",
        "--el-http-url",
        &el.url,
        "--cl-beacon-url",
        &cl.url,
        "--otel-endpoint",
        &collector.url,
        "--shutdown-grace",
        "0",
    ]);
    let prepared = Prepared::new(cfg).await.unwrap();
    let tracer = prepared.tracer().expect("otel tracer must be initialized");
    let subscriber = tracing_subscriber::registry().with(
        tracing_opentelemetry::layer()
            .with_tracer(tracer)
            .with_filter(tracing_subscriber::EnvFilter::new(
                "ethonyx=info,access_log=off",
            )),
    );
    // `#[tokio::test]` runs on a `current_thread` runtime, so `set_default` binds all
    // tasks spawned by `bound.serve(...)` to this test's isolated OTel subscriber.
    let _sub_guard = tracing::subscriber::set_default(subscriber);
    tracing::callsite::rebuild_interest_cache();

    let bound = prepared.bind().await.unwrap();
    let addr = bound.local_addrs()[0];
    let (stop_tx, stop_rx) = oneshot::channel::<()>();
    let server = tokio::spawn(bound.serve(async move {
        let _ = stop_rx.await;
    }));

    let c = client();
    let traceparent_val = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    let req = Request::builder()
        .method(Method::POST)
        .uri(format!("http://{addr}/"))
        .header("traceparent", traceparent_val)
        .header("tracestate", "vendor=ethonyx")
        .header("tracestate", "tier=edge")
        .body(Full::new(Bytes::from(
            json!({"jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id": 1})
                .to_string(),
        )))
        .unwrap();

    let resp = c.request(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let _ = resp.into_body().collect().await.unwrap();
    drop(c);

    {
        let recorded_reqs = el.recorded.lock().unwrap();
        let forwarded = recorded_reqs
            .iter()
            .find(|r| r.method == Method::POST && r.path == "/")
            .expect("POST request forwarded to EL");
        let received_tp = forwarded
            .headers
            .get("traceparent")
            .expect("traceparent header in upstream request")
            .to_str()
            .unwrap();
        assert!(
            received_tp.starts_with("00-4bf92f3577b34da6a3ce929d0e0e4736-"),
            "must preserve parent trace-id, got: {received_tp}"
        );
        assert!(
            !received_tp.contains("-00f067aa0ba902b7-"),
            "must rewrite span-id to the child span, got: {received_tp}"
        );
        assert_eq!(
            forwarded
                .headers
                .get("tracestate")
                .and_then(|v| v.to_str().ok()),
            Some("vendor=ethonyx,tier=edge")
        );
    }

    let _ = stop_tx.send(());
    server.await.unwrap().unwrap();

    let exported = collector.recorded.lock().unwrap();
    assert!(
        exported
            .iter()
            .any(|r| r.path == "/v1/traces" && !r.body.is_empty()),
        "shutdown must flush trace batch to collector, got paths: {:?}",
        exported.iter().map(|r| &r.path).collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn pooled_client_exports_first_request_span_while_keepalive_socket_stays_open() {
    use opentelemetry::Value;
    use opentelemetry::trace::{Status, TracerProvider as _};
    use opentelemetry_sdk::error::OTelSdkResult;
    use tracing_subscriber::prelude::*;

    #[derive(Clone, Debug, Default)]
    struct CapturingSpanExporter(Arc<Mutex<Vec<opentelemetry_sdk::trace::SpanData>>>);

    impl opentelemetry_sdk::trace::SpanExporter for CapturingSpanExporter {
        async fn export(&self, batch: Vec<opentelemetry_sdk::trace::SpanData>) -> OTelSdkResult {
            self.0.lock().unwrap().extend(batch);
            Ok(())
        }
    }

    let h1_el = MockServer::start(ok_handler()).await;
    let h2_el = MockServer::start_h2c(ok_handler()).await;
    let err_el = MockServer::start(Arc::new(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Bytes::from_static(b"boom"),
        )
    }))
    .await;

    let span_exporter = CapturingSpanExporter::default();
    let tracer_provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_simple_exporter(span_exporter.clone())
        .build();
    let subscriber = tracing_subscriber::registry().with(
        tracing_opentelemetry::layer()
            .with_tracer(tracer_provider.tracer("ethonyx"))
            .with_filter(tracing_subscriber::EnvFilter::new(
                "ethonyx=debug,access_log=off",
            )),
    );
    let _sub_guard = tracing::subscriber::set_default(subscriber);
    tracing::callsite::rebuild_interest_cache();

    // 1. Pooled HTTP/1.1 keep-alive (`--trust-upstream`) and 2. Pooled h2c keep-alive
    // (probed), plus a 500 response checking `http.response.status_code` & Error status.
    let h1_proxy = EthonyxHandle::start(&["--trust-upstream", "--el-http-url", &h1_el.url]).await;
    let h2_proxy = EthonyxHandle::start(&["--el-http-url", &h2_el.url]).await;
    let err_proxy = EthonyxHandle::start(&["--trust-upstream", "--el-http-url", &err_el.url]).await;
    let c = client();

    for proxy in [&h1_proxy, &h2_proxy] {
        let (status, _) = post_json(
            &c,
            &proxy.url("/"),
            json!({"jsonrpc":"2.0","method":"eth_blockNumber","id":1}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
    }
    let (err_status, _) = post_json(
        &c,
        &err_proxy.url("/"),
        json!({"jsonrpc":"2.0","method":"eth_blockNumber","id":1}),
    )
    .await;
    assert_eq!(err_status, StatusCode::INTERNAL_SERVER_ERROR);

    // All three pooled upstream keep-alive connections remain open inside the proxies:
    // without `rt-tracing-exec-force`, background pool driver tasks must NOT retain a
    // clone of Request #1's span, so all three spans are exported immediately!
    wait_for(|| {
        let count = span_exporter.0.lock().unwrap().len();
        async move {
            (count >= 3)
                .then_some(())
                .ok_or_else(|| format!("exported spans={count}, expected 3"))
        }
    })
    .await;

    {
        let spans = span_exporter.0.lock().unwrap();
        assert_eq!(spans.len(), 3);
        let statuses: Vec<i64> = spans
            .iter()
            .filter_map(|s| {
                s.attributes.iter().find_map(|kv| {
                    (kv.key.as_str() == "http.response.status_code")
                        .then_some(&kv.value)
                        .and_then(|v| match v {
                            Value::I64(n) => Some(*n),
                            _ => None,
                        })
                })
            })
            .collect();
        assert_eq!(statuses, [200, 200, 500]);
        assert!(matches!(spans[2].status, Status::Error { .. }));
    }

    drop(c);
    h1_proxy.shutdown().await;
    h2_proxy.shutdown().await;
    err_proxy.shutdown().await;
    let _ = tracer_provider.shutdown();
}

#[cfg(unix)]
#[tokio::test]
async fn binary_exports_traces_and_metrics_and_flushes_on_sigterm() {
    use std::process::Command;
    let collector = MockServer::start(ok_handler()).await;
    let upstream = MockServer::start(ok_handler()).await;
    // Set periodic export intervals to 60s so neither BatchSpanProcessor nor
    // PeriodicReader exports on a timer during the test — only SIGTERM's
    // `telemetry.shutdown().await` can deliver both signals to `collector`.
    let (mut child, port) = spawn_binary_on_ephemeral_port(|cmd| {
        cmd.args([
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
        .env("OTEL_METRIC_EXPORT_INTERVAL", "60000")
        .env("OTEL_BSP_SCHEDULE_DELAY", "60000")
        .env_remove("RUST_LOG");
    })
    .await;

    let c = client();
    let (status, _) = get(&c, &format!("http://127.0.0.1:{port}/eth/test")).await;
    assert_eq!(status, StatusCode::OK);
    drop(c);

    assert!(
        collector.recorded.lock().unwrap().is_empty(),
        "signals must still be buffered before SIGTERM"
    );

    assert!(
        Command::new("kill")
            .args(["-TERM", &child.child.id().to_string()])
            .status()
            .unwrap()
            .success()
    );
    let status = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(status) = child.child.try_wait().unwrap() {
                break status;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert!(status.success(), "{status}");

    let requests = collector.recorded.lock().unwrap();
    for path in ["/prefix/v1/traces", "/prefix/v1/metrics"] {
        assert!(
            requests
                .iter()
                .any(|r| r.path == path && !r.body.is_empty()),
            "SIGTERM shutdown must flush {path} (recorded paths: {:?})",
            requests.iter().map(|r| &r.path).collect::<Vec<_>>()
        );
    }
}

#[tokio::test]
async fn bind_failure_is_not_swallowed_by_shutdown_with_unresponsive_otel_collector() {
    let occupied = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let occupied_addr = occupied.local_addr().unwrap();

    let blackhole = StubPort::silent().await;
    let collector_url = blackhole.http_url();

    let cfg = Config::parse_from([
        "ethonyx",
        "--listen",
        &occupied_addr.to_string(),
        "--el-http-url",
        "http://127.0.0.1:8545",
        "--trust-upstream",
        "--otel-endpoint",
        &collector_url,
    ]);
    let prepared = Prepared::new(cfg).await.unwrap();
    {
        use opentelemetry::trace::{Span as _, Tracer as _};
        let mut span = prepared
            .tracer()
            .expect("otel tracer")
            .start("startup_check");
        span.end();
    }

    // Under `--trust-upstream` with a numeric IP literal (`127.0.0.1`), `TcpListener::bind`
    // resolves in a single poll without async DNS or probe I/O. Pending on poll #1 and
    // ready on poll #2+: if `Prepared::bind` ever yields at a post-error `.await`,
    // `biased` `select!` in `Prepared::run` immediately resolves `shutdown` (`Ok(())`).
    let mut polls = 0u32;
    let shutdown = std::future::poll_fn(move |_| {
        polls += 1;
        if polls > 1 {
            std::task::Poll::Ready(())
        } else {
            std::task::Poll::Pending
        }
    });
    let err = tokio::time::timeout(Duration::from_secs(1), prepared.run(shutdown))
        .await
        .expect(
            "bind failure must return promptly without stalling on an unresponsive OTel collector",
        )
        .expect_err("bind failure must not be overtaken and swallowed by a shutdown signal");
    assert!(
        err.to_string().contains("failed to bind --listen address"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn traceparent_survives_connection_nomination_and_propagates_to_websocket() {
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

    let (_child, port) = spawn_binary_on_ephemeral_port(|cmd| {
        cmd.args([
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
        .env("RUST_LOG", "info");
    })
    .await;

    // 1. HTTP request nominating `Connection: keep-alive, traceparent` drops the
    // hop-by-hop incoming `traceparent` on extraction and injects a fresh outgoing
    // root `traceparent` header upstream.
    let c = client();
    let req = Request::builder()
        .method(Method::POST)
        .uri(format!("http://127.0.0.1:{port}/"))
        .header("connection", "keep-alive, traceparent")
        .header(
            "traceparent",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        )
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from_static(
            br#"{"jsonrpc":"2.0","method":"eth_blockNumber","id":1}"#,
        )))
        .unwrap();
    let resp = c.request(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let http_tp = el
        .recorded
        .lock()
        .unwrap()
        .iter()
        .find(|r| r.method == Method::POST && r.path == "/")
        .and_then(|r| r.headers.get("traceparent"))
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
        .expect("Connection: traceparent must not strip ethonyx's outgoing traceparent");
    assert!(
        http_tp.starts_with("00-") && !http_tp.starts_with("00-4bf92f3577b34da6a3ce929d0e0e4736-"),
        "Connection-nominated incoming traceparent must not link parent trace-id, got: {http_tp}"
    );

    // 2. WebSocket handshake extracts the non-nominated parent `trace-id` and injects
    // a newly generated child `span-id` to the upstream WS server.
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
        ws_tp.starts_with("00-4bf92f3577b34da6a3ce929d0e0e4736-")
            && !ws_tp.contains("-00f067aa0ba902b7-"),
        "expected propagated WS traceparent to preserve trace-id and rewrite child span-id, got: {ws_tp}"
    );
    let _ = ws.close(None).await;

    ws_server.await.unwrap();
}
