//! Liveness, `/healthz`, `/readyz`, `/metrics`, `/eth/v1/node/health`, and `h2c` probe tests.

use super::*;
use ethonyx::run;
use serde_json::json;

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
    let dead = DeadPort::new();
    let cl = MockServer::start(ok_handler()).await;
    let ethonyx = EthonyxHandle::start(&[
        "--el-http-url",
        &dead.http_url(),
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
        "--el-http-url",
        "http://127.0.0.1:8545",
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
    assert_eq!(request_count(&metrics_str), 1.0);
    ethonyx.shutdown().await;

    // A second server instance owns an isolated recorder (starts at 0 and omits unpolled gauges).
    let second = EthonyxHandle::start(&[
        "--el-http-url",
        "http://127.0.0.1:8545",
        "--trust-upstream",
        "--mock-eth-syncing",
    ])
    .await;
    let (status, _) = post_json(
        &c,
        &second.url("/"),
        json!({"jsonrpc":"2.0","method":"eth_syncing","id":1}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (_, body) = get(&c, &second.url("/metrics")).await;
    let second_metrics = String::from_utf8_lossy(&body);
    assert_eq!(request_count(&second_metrics), 1.0);
    assert!(!second_metrics.contains("ethonyx_upstream_health_status"));
    second.shutdown().await;
}

#[tokio::test]
async fn test_cl_node_health_intercept_and_passthrough() {
    let el = MockServer::start(Arc::new(|req| {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or(Value::Null);
        let method = body.get("method").and_then(Value::as_str).unwrap_or("");
        let result = match method {
            "eth_syncing" => json!(false),
            "eth_getBlockByNumber" => json!({"number": "0x1", "timestamp": "0x1"}),
            _ => json!("0x1"),
        };
        (
            StatusCode::OK,
            serde_json::to_vec(&json!({"jsonrpc":"2.0","id":1,"result":result})).unwrap(),
        )
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
async fn node_health_returns_503_on_partial_outage_or_strict_staleness_and_206_when_syncing() {
    let now_hex = format!("0x{:x}", now_unix());
    let stale_hex = format!("0x{:x}", now_unix().saturating_sub(600));
    let dead = DeadPort::new();
    let dead_cl = dead.http_url();

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
    let c = client();

    // 1. EL-only `--trust-upstream`: omits `cl` completely on both `/healthz` and `/readyz`.
    let trusted_el =
        EthonyxHandle::start(&["--el-http-url", "http://127.0.0.1:8545", "--trust-upstream"]).await;
    let (status_el, body_el) = get(&c, &trusted_el.url("/healthz")).await;
    assert_eq!(status_el, StatusCode::OK);
    let snap_el: Value = serde_json::from_slice(&body_el).unwrap();
    assert_eq!(
        snap_el,
        json!({"mode": "trusted", "el": {"transport": "http/1.1"}})
    );
    let (r_status_el, r_body_el) = get(&c, &trusted_el.url("/readyz")).await;
    assert_eq!(r_status_el, StatusCode::OK);
    let ready_el: Value = serde_json::from_slice(&r_body_el).unwrap();
    assert!(ready_el.get("cl_syncing").is_none(), "{ready_el}");
    trusted_el.shutdown().await;

    // 2. EL + CL `--trust-upstream`: includes both `el` and `cl` transports.
    let trusted = EthonyxHandle::start(&[
        "--el-http-url",
        "http://127.0.0.1:8545",
        "--trust-upstream",
        "--cl-beacon-url",
        "http://127.0.0.1:5052",
    ])
    .await;
    let (status, body) = get(&c, &trusted.url("/healthz")).await;
    assert_eq!(status, StatusCode::OK);
    let snap: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(snap["mode"], "trusted");
    assert_eq!(snap["el"], json!({"transport": "http/1.1"}));
    assert_eq!(snap["cl"], json!({"transport": "http/1.1"}));
    trusted.shutdown().await;
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
