//! OpenTelemetry W3C context propagation, OTLP export flush on `SIGTERM`, and startup failure cleanup tests.

use super::*;
use serde_json::json;
use tokio_tungstenite::connect_async;

struct KillOnDrop(std::process::Child);

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

async fn spawn_binary_on_ephemeral_port(
    upstream: &MockServer,
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
        let mut child = KillOnDrop(cmd.spawn().unwrap());
        let probe_path = format!("/__ethonyx_ready_{attempt}");
        let probe_req =
            format!("GET {probe_path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");

        for _ in 0..250 {
            if let Some(status) = child.0.try_wait().unwrap() {
                let mut stderr = String::new();
                if let Some(mut err_pipe) = child.0.stderr.take() {
                    let _ = std::io::Read::read_to_string(&mut err_pipe, &mut stderr);
                }
                if attempt < 4 && stderr.contains("failed to bind --listen address") {
                    break;
                }
                panic!("ethonyx binary exited prematurely with {status} on port {port}: {stderr}");
            }
            // Open a fresh `Connection: close` socket each tick (never reusing a pooled
            // keep-alive socket to a previous port holder), bound the probe with a short
            // timeout in case a blackhole listener stole `port`, and verify that the
            // request reached THIS test's dedicated `upstream` MockServer.
            let probed = tokio::time::timeout(Duration::from_millis(250), async {
                let mut sock = tokio::net::TcpStream::connect(("127.0.0.1", port))
                    .await
                    .ok()?;
                sock.write_all(probe_req.as_bytes()).await.ok()?;
                let mut discard = [0u8; 512];
                let _ = sock.read(&mut discard).await;
                Some(())
            })
            .await;
            if matches!(probed, Ok(Some(()))) {
                let mut recorded = upstream.recorded.lock().unwrap();
                if recorded.iter().any(|r| r.path == probe_path) {
                    recorded.retain(|r| !r.path.starts_with("/__ethonyx_ready_"));
                    return (child, port);
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    panic!("ethonyx subprocess failed to become ready after retries");
}

/// Without an active `tracing-opentelemetry` subscriber layer (`EthonyxHandle`
/// runs in-process without installing a global subscriber), non-`Connection`-nominated
/// `traceparent` headers pass through `strip_hop_by_hop` intact to the upstream.
#[tokio::test]
async fn trace_context_is_propagated_to_upstream() {
    let collector = MockServer::start(ok_handler()).await;
    let el = MockServer::start(ok_handler()).await;
    let cl = MockServer::start(ok_handler()).await;
    let ethonyx = EthonyxHandle::start(&[
        "--el-http-url",
        &el.url,
        "--cl-beacon-url",
        &cl.url,
        "--otel-endpoint",
        &collector.url,
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

#[cfg(unix)]
#[tokio::test]
async fn binary_exports_traces_and_metrics_and_flushes_on_sigterm() {
    use std::process::Command;
    let collector = MockServer::start(ok_handler()).await;
    let upstream = MockServer::start(ok_handler()).await;
    // Set periodic export intervals to 60s so neither BatchSpanProcessor nor
    // PeriodicReader exports on a timer during the test — only SIGTERM's
    // `telemetry.shutdown().await` can deliver both signals to `collector`.
    let (mut child, port) = spawn_binary_on_ephemeral_port(&upstream, |cmd| {
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

    let blackhole = SilentPort::new().await;
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

    let (_child, port) = spawn_binary_on_ephemeral_port(&el, |cmd| {
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
