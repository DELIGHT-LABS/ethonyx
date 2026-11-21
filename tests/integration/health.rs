//! Liveness, `/healthz`, `/readyz`, `/metrics`, `/eth/v1/node/health`, and `h2c` probe tests.

use super::*;
use ethonyx::run;
use serde_json::json;

#[tokio::test]
async fn el_upstream_h2c_is_auto_detected() {
    // No flag: when the EL serves h2c, the health poller detects it (it prefers
    // h2c) and the data-plane forwards over HTTP/2. The mock records the version
    // it received. (An h1-only EL falls back to HTTP/1.1 — covered by
    // `jsonrpc_post_is_forwarded_to_el_http_upstream`.)
    let el = MockServer::start_h2c(Arc::new(|_| {
        (
            StatusCode::OK,
            Bytes::from_static(br#"{"jsonrpc":"2.0","id":7,"result":"0xcafe"}"#),
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
    let el_reqs = el.received();
    let fwd = el_reqs
        .iter()
        .find(|r| String::from_utf8_lossy(&r.body).contains("eth_blockNumber"))
        .expect("EL received the forwarded JSON-RPC");
    assert_eq!(fwd.version, Version::HTTP_2);
    // Health probes reach the EL as JSON POSTs, like the forwarded call.
    for req in &el_reqs {
        assert_eq!(req.headers["content-type"], "application/json", "{req:?}");
    }

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
    assert_eq!(
        health["el"]["errors"],
        json!([
            r#"eth_getBlockByNumber: rpc error: {"code":-32000,"message":"block unavailable"}"#
        ]),
        "{health}"
    );
    assert_eq!(
        health["cl"]["errors"],
        json!(["node/syncing: http 503 Service Unavailable"]),
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
    let bad: MockHandler = Arc::new(|_| (StatusCode::NOT_FOUND, Bytes::from_static(b"{}")));
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
    // The poller must keep refreshing. net_peerCount stays at 16 until we have observed
    // the warm-seed value (avoiding a slow-runner race before the first read), then flips
    // to 32 so the background poll must surface within POLL * 4.
    use std::sync::atomic::{AtomicBool, Ordering};
    let allow_refresh = Arc::new(AtomicBool::new(false));
    let gate = allow_refresh.clone();
    let el = MockServer::start(jsonrpc_mock(move |method| {
        RpcMockReply::Result(match method {
            "eth_syncing" => json!(false),
            "net_peerCount" => {
                if gate.load(Ordering::Acquire) {
                    json!("0x20") // 32, later background polls
                } else {
                    json!("0x10") // 16, warm seed
                }
            }
            "eth_getBlockByNumber" => json!({"number": "0x1", "timestamp": "0x1"}),
            _ => json!(null),
        })
    }))
    .await;
    let cl = MockServer::start(ok_handler()).await;
    const POLL: Duration = Duration::from_millis(200);
    let ethonyx = EthonyxHandle::start_tuned(
        &["--el-http-url", &el.url, "--cl-beacon-url", &cl.url],
        |cfg| cfg.health_poll_interval = POLL,
    )
    .await;

    let c = client();
    let (_, b1) = get(&c, &ethonyx.url("/healthz")).await;
    let v1: Value = serde_json::from_slice(&b1).unwrap();
    assert_eq!(
        v1["el"]["peers"], 16,
        "first read is the warm-seed poll: {v1}"
    );
    allow_refresh.store(true, Ordering::Release);

    // Bounded by the configured poll interval so a sluggish poller fails this test.
    wait_until(POLL * 4, || async {
        let (_, b2) = get(&c, &ethonyx.url("/healthz")).await;
        let v2: Value = serde_json::from_slice(&b2).unwrap();
        if v2["el"]["peers"] == 32 {
            Ok(())
        } else {
            Err(v2.to_string())
        }
    })
    .await;

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

    let el = MockServer::start(jsonrpc_mock(move |method| {
        RpcMockReply::Result(match method {
            "eth_syncing" => json!(false),
            "net_peerCount" => json!("0x10"), // 16 peers
            "eth_getBlockByNumber" => json!({
                "number": "0x1234",
                "timestamp": block_ts_hex,
            }),
            _ => json!(null),
        })
    }))
    .await;

    let cl = MockServer::start(cl_sync_mock(
        json!({
            "head_slot": head_slot.to_string(),
            "sync_distance": "0",
            "is_syncing": false,
        }),
        Some(100),
    ))
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
    let el = MockServer::start(jsonrpc_mock(|method| {
        RpcMockReply::Result(match method {
            "eth_syncing" => json!(false),
            "net_peerCount" => json!("0x0"),
            "eth_getBlockByNumber" => json!({"number": "0x1", "timestamp": "0x1"}),
            _ => json!(null),
        })
    }))
    .await;
    let cl = MockServer::start(cl_sync_mock(
        json!({"head_slot": "1", "sync_distance": "0", "is_syncing": false}),
        Some(0),
    ))
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
    let el = MockServer::start(jsonrpc_mock(|method| {
        RpcMockReply::Result(match method {
            "eth_syncing" => json!({"currentBlock": "0x10", "highestBlock": "0x20"}),
            _ => json!(null),
        })
    }))
    .await;
    let cl = MockServer::start(cl_sync_mock(
        json!({"head_slot": "100", "sync_distance": "0", "is_syncing": false}),
        None,
    ))
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
    let el = MockServer::start(jsonrpc_mock(|method| {
        RpcMockReply::Result(match method {
            "eth_syncing" => json!(false),
            "eth_getBlockByNumber" => json!({"number": "0x5", "timestamp": "0x1"}),
            _ => json!(null),
        })
    }))
    .await;
    let cl = MockServer::start(cl_sync_mock(
        json!({"head_slot": "1", "sync_distance": "0", "is_syncing": false}),
        None,
    ))
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
    let el = MockServer::start_hanging_h2c(jsonrpc_mock(|method| {
        RpcMockReply::Result(match method {
            "eth_syncing" => json!(false),
            "net_peerCount" => json!("0x0"),
            "eth_getBlockByNumber" => json!({"number": "0x1", "timestamp": "0x1"}),
            _ => json!(null),
        })
    }))
    .await;

    let cl = MockServer::start(cl_sync_mock(
        json!({"head_slot": "1", "sync_distance": "0", "is_syncing": false}),
        Some(0),
    ))
    .await;

    // Start ethonyx. By default, it warms up cache on start.
    // If fallback works, starting will succeed because the warm-up succeeds.
    let ethonyx = EthonyxHandle::start_tuned(
        &["--el-http-url", &el.url, "--cl-beacon-url", &cl.url],
        // The h2c attempt burns one full timeout; the HTTP/1.1 retry then has to finish
        // inside the next one, so leave it room on a loaded runner.
        |cfg| cfg.health_timeout = Duration::from_millis(500),
    )
    .await;

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
    let second = EthonyxHandle::start_trusted(&["--mock-eth-syncing"]).await;
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
    let el = MockServer::start(jsonrpc_mock(|method| {
        RpcMockReply::Result(match method {
            "eth_syncing" => json!(false),
            "eth_getBlockByNumber" => json!({"number": "0x1", "timestamp": "0x1"}),
            _ => json!("0x1"),
        })
    }))
    .await;

    let cl_mock_synced = MockServer::start(Arc::new(|req| {
        if req.path == "/eth/v1/node/syncing" {
            (
                StatusCode::OK,
                Bytes::from_static(
                    br#"{"data":{"is_syncing":false,"sync_distance":"0","head_slot":"100"}}"#,
                ),
            )
        } else {
            (
                StatusCode::BAD_REQUEST,
                Bytes::from_static(b"{\"error\": \"Unsupported method\"}"),
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
        Duration::from_secs(1),
        EthonyxHandle::start_tuned(
            &["--el-http-url", &el.url, "--cl-beacon-url", &stalled_url],
            |cfg| cfg.health_timeout = Duration::from_millis(150),
        ),
    )
    .await
    .expect("body timeout must let startup finish");
    let (status, body) = get(&client(), &proxy.url("/healthz")).await;
    assert_eq!(status, StatusCode::OK);
    let value: Value = serde_json::from_slice(&body).unwrap();
    let cl_errors = value["cl"]["errors"].as_array().unwrap();
    assert_eq!(
        cl_errors.len(),
        2,
        "expected syncing + peer_count errors: {value}"
    );
    assert!(
        cl_errors
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
            Bytes::from(serde_json::to_vec(&json!({"result":result})).unwrap()),
        )
    }))
    .await;
    // Individual malformed CL envelope shapes are unit-tested in `health::cl`; here
    // verify end-to-end that an array/non-object response on either endpoint fails only
    // that endpoint while preserving the valid sibling result (including raw ID-less EL replies).
    for (syncing, peer_count, valid_syncing, valid_peers) in [
        (r#"{"data":["123","0",false]}"#, peers, false, true),
        (synced, r#"{"data":["64"]}"#, true, false),
    ] {
        let cl = MockServer::start(Arc::new(move |req| {
            (
                StatusCode::OK,
                Bytes::from_static(
                    if req.path == "/eth/v1/node/syncing" {
                        syncing
                    } else {
                        peer_count
                    }
                    .as_bytes(),
                ),
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
        assert_eq!(status, StatusCode::OK, "{syncing} / {peer_count}");
        let report: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(report["el"]["syncing"], false, "{syncing} / {peer_count}");
        assert_eq!(
            report["cl"]["syncing"].as_bool(),
            valid_syncing.then_some(false),
            "{syncing} / {peer_count}"
        );
        assert_eq!(
            report["cl"]["peers"].as_u64(),
            valid_peers.then_some(64),
            "{syncing} / {peer_count}"
        );
        assert!(
            !report["cl"]["errors"].as_array().unwrap().is_empty(),
            "{syncing} / {peer_count}: {report}"
        );
        proxy.shutdown().await;
    }
}

#[tokio::test]
async fn readyz_strict_rejects_malformed_hex_quantities_in_el_probes() {
    // Exhaustive malformed hex inputs are unit-tested in `health::el`; verify end-to-end
    // that a non-canonical quantity ("0x00") gates `/readyz` under `--readyz-strict`.
    let now_hex = format!("0x{:x}", now_unix());
    let el = MockServer::start(jsonrpc_mock(move |method| {
        RpcMockReply::Result(match method {
            "eth_syncing" => json!(false),
            "net_peerCount" => json!("0x10"),
            "eth_getBlockByNumber" => json!({"number": "0x00", "timestamp": now_hex}),
            _ => json!(null),
        })
    }))
    .await;
    let cl = MockServer::start(cl_sync_mock(
        json!({"head_slot": "1", "sync_distance": "0", "is_syncing": false}),
        Some(8),
    ))
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
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body:?}");
    let (_, hz) = get(&c, &proxy.url("/healthz")).await;
    let hv: Value = serde_json::from_slice(&hz).unwrap();
    assert!(hv["el"]["block_number"].is_null(), "{hv}");
    assert!(!hv["el"]["errors"].as_array().unwrap().is_empty(), "{hv}");
    proxy.shutdown().await;
}

#[tokio::test]
async fn node_health_returns_503_on_partial_outage_or_strict_staleness_and_206_when_syncing() {
    let now_hex = format!("0x{:x}", now_unix());
    let stale_hex = format!("0x{:x}", now_unix().saturating_sub(600));
    let dead = DeadPort::new();
    let dead_cl = dead.http_url();

    // 1. EL syncing + CL unreachable => 503 (never 206).
    let el_syncing = MockServer::start(jsonrpc_mock(move |method| {
        RpcMockReply::Result(match method {
            "eth_syncing" => json!({"currentBlock": "0x10", "highestBlock": "0x20"}),
            "net_peerCount" => json!("0x5"),
            "eth_getBlockByNumber" => json!({"number": "0x10", "timestamp": now_hex}),
            _ => json!(null),
        })
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
    let cl_synced = MockServer::start(cl_sync_mock(
        json!({"is_syncing": false, "sync_distance": "0", "head_slot": "100"}),
        Some(12),
    ))
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
    let el_stale = MockServer::start(jsonrpc_mock(move |method| {
        RpcMockReply::Result(match method {
            "eth_syncing" => json!(false),
            "net_peerCount" => json!("0x5"),
            "eth_getBlockByNumber" => json!({"number": "0x20", "timestamp": stale_hex}),
            _ => json!(null),
        })
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
async fn trust_upstream_serves_livez_and_reports_honest_healthz_mode() {
    let c = client();

    // 1. EL-only `--trust-upstream`: omits `cl` completely on both `/healthz` and `/readyz`.
    let trusted_el = EthonyxHandle::start_trusted(&[]).await;
    let (live_st, live_body) = get(&c, &trusted_el.url("/livez")).await;
    assert_eq!(live_st, StatusCode::OK);
    assert_eq!(&live_body[..], b"ok");
    let head_live = c
        .request(
            Request::builder()
                .method(Method::HEAD)
                .uri(trusted_el.url("/livez"))
                .body(Full::new(Bytes::new()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(head_live.status(), StatusCode::OK);
    assert!(
        head_live
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .is_empty()
    );
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
    let trusted = EthonyxHandle::start_trusted(&["--cl-beacon-url", "http://127.0.0.1:5052"]).await;
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
    use std::sync::atomic::{AtomicUsize, Ordering};

    let el_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let el_addr = el_listener.local_addr().unwrap();
    let el_url = format!("http://{el_addr}");
    // Number of upcoming connections forced to HTTP/1.1-only; the mock bounds the
    // outage deterministically instead of racing the background poller's timer.
    let h1_only = Arc::new(AtomicUsize::new(0));
    let batch_versions: Arc<Mutex<Vec<Version>>> = Arc::new(Mutex::new(Vec::new()));
    let conn_handles: Arc<Mutex<Vec<JoinHandle<()>>>> = Arc::new(Mutex::new(Vec::new()));

    let budget = h1_only.clone();
    let versions = batch_versions.clone();
    let handles = conn_handles.clone();
    let el_server = tokio::spawn(async move {
        while let Ok((stream, _)) = el_listener.accept().await {
            let h2 = budget
                .try_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_sub(1))
                .is_err();
            let versions = versions.clone();
            let h = tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let svc = service_fn(move |req: Request<Incoming>| {
                    let versions = versions.clone();
                    async move {
                        let version = req.version();
                        let bytes = req.into_body().collect().await.unwrap().to_bytes();
                        let v: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
                        let resp = if v.is_array() {
                            versions.lock().unwrap().push(version);
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
            handles.lock().unwrap().push(h);
        }
    });

    let cl = MockServer::start(cl_sync_mock(
        json!({"is_syncing": false, "sync_distance": "0", "head_slot": "1"}),
        Some(4),
    ))
    .await;

    let proxy = EthonyxHandle::start_tuned(
        &["--el-http-url", &el_url, "--cl-beacon-url", &cl.url],
        |cfg| cfg.health_poll_interval = Duration::from_millis(60),
    )
    .await;
    let c = client();

    // 1. Initial poll confirms h2c (`Confirmed`).
    let (_, hz1) = get(&c, &proxy.url("/healthz")).await;
    let v1: Value = serde_json::from_slice(&hz1).unwrap();
    assert_eq!(v1["el"]["transport"], "h2c");

    // 2. Reject h2c on the next connection and drop pooled server connections so one poll
    //    falls back to HTTP/1.1 (`H1VerifyOnce`), then the following poll self-heals to h2c.
    h1_only.store(1, Ordering::Release);
    for h in conn_handles.lock().unwrap().drain(..) {
        h.abort();
    }

    wait_for(|| async {
        let seen = batch_versions.lock().unwrap();
        match seen.iter().position(|&v| v == Version::HTTP_11) {
            Some(at) if seen[at..].contains(&Version::HTTP_2) => Ok(()),
            _ => Err(format!("{:?}", *seen)),
        }
    })
    .await;

    // 3. `/healthz` settles back to `"h2c"`.
    wait_for(|| async {
        let (_, hz) = get(&c, &proxy.url("/healthz")).await;
        let v: Value = serde_json::from_slice(&hz).unwrap();
        if v["el"]["transport"] == "h2c" {
            Ok(())
        } else {
            Err(v["el"]["transport"].to_string())
        }
    })
    .await;

    proxy.shutdown().await;
    el_server.abort();
}

#[tokio::test]
async fn el_only_mode_omits_cl_metrics_returns_404_on_cl_routes_and_tolerates_missing_net_peer_count()
 {
    let el = MockServer::start(jsonrpc_mock(|method| match method {
        "net_peerCount" => RpcMockReply::Error(-32601, "Method not found"),
        "eth_syncing" => RpcMockReply::Result(json!(false)),
        "eth_getBlockByNumber" => {
            RpcMockReply::Result(json!({"number": "0x2a", "timestamp": "0x67000000"}))
        }
        _ => RpcMockReply::Result(json!("0x2a")),
    }))
    .await;

    let proxy = EthonyxHandle::start(&["--el-http-url", &el.url]).await;
    let c = client();

    // 1. `/readyz` is 200 without `--cl-beacon-url`, reporting block height in `el_syncing`.
    let (r_status, r_body) = get(&c, &proxy.url("/readyz")).await;
    assert_eq!(r_status, StatusCode::OK);
    let r_json: Value = serde_json::from_slice(&r_body).unwrap();
    assert_eq!(r_json["el_syncing"]["detail"], "synced (block 42)");
    assert!(r_json.get("cl_syncing").is_none());

    // 2. `/healthz` omits `cl`, `el.peers`, and `el.errors` when `net_peerCount` returns `-32601`.
    let (_, h_body) = get(&c, &proxy.url("/healthz")).await;
    let h_json: Value = serde_json::from_slice(&h_body).unwrap();
    assert!(h_json.get("cl").is_none(), "{h_json}");
    assert!(h_json["el"].get("peers").is_none(), "{h_json}");
    assert!(h_json["el"].get("errors").is_none(), "{h_json}");
    assert_eq!(h_json["el"]["block_number"], 42);

    // 3. Unconfigured CL and WS routes (both HTTP/1.1 Upgrade and H2 Extended CONNECT)
    //    return 404 locally BEFORE recording proxy metrics, while EL HTTP JSON-RPC passes.
    for cl_path in [
        "/eth/v1/node/syncing",
        "/lighthouse/ui/health",
        "/teku/v1/admin",
    ] {
        let (st, _) = get(&c, &proxy.url(cl_path)).await;
        assert_eq!(st, StatusCode::NOT_FOUND, "path {cl_path}");
    }
    let ws_h1_req = Request::builder()
        .method(Method::GET)
        .uri(proxy.url("/"))
        .header("connection", "Upgrade")
        .header("upgrade", "websocket")
        .header("sec-websocket-version", "13")
        .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
        .body(Full::new(Bytes::new()))
        .unwrap();
    let ws_h1_resp = c.request(ws_h1_req).await.unwrap();
    assert_eq!(ws_h1_resp.status(), StatusCode::NOT_FOUND);

    let mut h2 = proxy.open_h2().await;
    let ws_h2_req = ext_connect(proxy.url("/"));
    let ws_h2_resp = h2.send_request(ws_h2_req).await.unwrap();
    assert_eq!(ws_h2_resp.status(), StatusCode::NOT_FOUND);
    drop(h2);

    let (el_st, _) = post_json(
        &c,
        &proxy.url("/"),
        json!({"jsonrpc":"2.0","method":"eth_chainId","params":[],"id":1}),
    )
    .await;
    assert_eq!(el_st, StatusCode::OK);

    let (_, m_body) = get(&c, &proxy.url("/metrics")).await;
    let metrics_text = std::str::from_utf8(&m_body).unwrap();
    assert!(!metrics_text.contains("layer=\"cl\""), "{metrics_text}");
    assert!(!metrics_text.contains("upstream=\"CL\""), "{metrics_text}");
    // Only the EL JSON-RPC POST was proxied; every 404 above was answered before metrics.
    assert!(!metrics_text.contains("status=\"404\""), "{metrics_text}");
    assert!(
        !metrics_text.contains("ethonyx_upstream_slot_number"),
        "{metrics_text}"
    );
    assert!(
        !metrics_text.contains("ethonyx_upstream_peers"),
        "{metrics_text}"
    );

    proxy.shutdown().await;
}

#[tokio::test]
async fn cl_optimistic_and_el_offline_gate_readyz_and_node_health_while_queries_pass() {
    use std::sync::atomic::{AtomicU8, Ordering};

    // 0 = synced, 1 = silent stall (`is_optimistic: true`, `is_syncing: false`), 2 = `el_offline: true`
    let mode = Arc::new(AtomicU8::new(1));
    let mode_cl = mode.clone();

    let el = MockServer::start(jsonrpc_mock(|method| {
        RpcMockReply::Result(match method {
            "eth_syncing" => json!(false),
            "net_peerCount" => json!("0x8"),
            "eth_getBlockByNumber" => json!({"number": "0x64", "timestamp": "0x67000000"}),
            _ => json!("0x1234"),
        })
    }))
    .await;

    let cl = MockServer::start(Arc::new(move |req| match req.path.as_str() {
        "/eth/v1/node/syncing" => {
            let payload = match mode_cl.load(Ordering::Acquire) {
                0 => json!({"data":{"head_slot":"100","sync_distance":"0","is_syncing":false,"is_optimistic":false,"el_offline":false}}),
                1 => json!({"data":{"head_slot":"100","sync_distance":"0","is_syncing":false,"is_optimistic":true,"el_offline":false}}),
                _ => json!({"data":{"head_slot":"100","sync_distance":"5","is_syncing":true,"is_optimistic":true,"el_offline":true}}),
            };
            (StatusCode::OK, Bytes::from(serde_json::to_vec(&payload).unwrap()))
        }
        "/eth/v1/node/peer_count" => (
            StatusCode::OK,
            Bytes::from_static(br#"{"data":{"connected":"16"}}"#),
        ),
        _ => (StatusCode::OK, Bytes::from_static(br#"{"data":"ok"}"#)),
    }))
    .await;

    let proxy = EthonyxHandle::start_tuned(
        &[
            "--el-http-url",
            &el.url,
            "--cl-beacon-url",
            &cl.url,
            "--mock-cl-node-health",
        ],
        |cfg| cfg.health_poll_interval = Duration::from_millis(50),
    )
    .await;
    let c = client();

    // Silent EL stall (`is_optimistic: true`, `!is_syncing`): `/readyz` and `/eth/v1/node/health` are 503,
    // while data-plane EL JSON-RPC and CL REST queries still pass 100%.
    let (r1, rb1) = get(&c, &proxy.url("/readyz")).await;
    assert_eq!(r1, StatusCode::SERVICE_UNAVAILABLE);
    let rv1: Value = serde_json::from_slice(&rb1).unwrap();
    assert_eq!(rv1["cl_syncing"]["ok"], false);
    assert!(
        rv1["cl_syncing"]["detail"]
            .as_str()
            .unwrap()
            .contains("optimistic")
    );
    assert_eq!(
        get(&c, &proxy.url("/eth/v1/node/health")).await.0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        post_json(
            &c,
            &proxy.url("/"),
            json!({"jsonrpc":"2.0","method":"eth_call","params":[],"id":1})
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        get(&c, &proxy.url("/eth/v1/beacon/headers")).await.0,
        StatusCode::OK
    );

    // `el_offline: true` also returns 503 even when `is_syncing: true`.
    mode.store(2, Ordering::Release);
    wait_for(|| async {
        let (_, hb) = get(&c, &proxy.url("/healthz")).await;
        let hv: Value = serde_json::from_slice(&hb).unwrap();
        if hv["cl"]["syncing"] == true {
            Ok(())
        } else {
            Err(hv.to_string())
        }
    })
    .await;
    assert_eq!(
        get(&c, &proxy.url("/eth/v1/node/health")).await.0,
        StatusCode::SERVICE_UNAVAILABLE
    );

    // Recovery (`is_optimistic: false`, `el_offline: false`) restores 200 on both endpoints.
    mode.store(0, Ordering::Release);
    wait_for(|| async {
        let (st, rb) = get(&c, &proxy.url("/readyz")).await;
        if st == StatusCode::OK {
            Ok(())
        } else {
            Err(format!("{st}: {}", String::from_utf8_lossy(&rb)))
        }
    })
    .await;
    assert_eq!(
        get(&c, &proxy.url("/eth/v1/node/health")).await.0,
        StatusCode::OK
    );

    proxy.shutdown().await;
}

#[tokio::test]
async fn multi_reference_rpc_gates_block_lag_passes_queries_and_handles_two_strike_fail_open() {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    let local_height = Arc::new(AtomicU64::new(98));
    let ref_height = Arc::new(AtomicU64::new(100));
    let ref_chain_id = Arc::new(AtomicU64::new(1));
    let ref_alive = Arc::new(AtomicBool::new(true));

    let lh = local_height.clone();
    let local_el = MockServer::start(jsonrpc_mock(move |method| {
        let h = format!("0x{:x}", lh.load(Ordering::Acquire));
        RpcMockReply::Result(match method {
            "eth_syncing" => json!(false),
            "net_peerCount" => json!("0x4"),
            "eth_getBlockByNumber" => json!({"number": h, "timestamp": "0x67000000"}),
            "eth_chainId" => json!("0x1"),
            "eth_blockNumber" => json!(h),
            _ => json!("0xdeadbeef"),
        })
    }))
    .await;

    let rh = ref_height.clone();
    let rcid = ref_chain_id.clone();
    let ralive = ref_alive.clone();
    let peer_ref = MockServer::start(jsonrpc_mock(move |method| {
        if !ralive.load(Ordering::Acquire) {
            return RpcMockReply::Http(StatusCode::INTERNAL_SERVER_ERROR, b"down");
        }
        RpcMockReply::Result(match method {
            "eth_chainId" => json!(format!("0x{:x}", rcid.load(Ordering::Acquire))),
            "eth_blockNumber" => json!(format!("0x{:x}", rh.load(Ordering::Acquire))),
            _ => json!(null),
        })
    }))
    .await;

    let dead = DeadPort::new();
    let ref_arg = format!(
        "{}/v2/SECRET_KEY_1,{}/v2/SECRET_KEY_2",
        peer_ref.url,
        dead.http_url()
    );

    let proxy = EthonyxHandle::start_tuned(
        &[
            "--el-http-url",
            &local_el.url,
            "--el-reference-url",
            &ref_arg,
            "--el-max-reference-lag",
            "1",
        ],
        |cfg| {
            cfg.health_poll_interval = Duration::from_millis(60);
            cfg.el_reference_poll_interval = Some(Duration::from_millis(60));
        },
    )
    .await;
    let c = client();

    // 1. Initial state: local=98, peer_ref=100 (`lag = 2 > 1`) alongside a dead sibling reference!
    // `/readyz` MUST be 503 (dead peer did NOT hide lag), API keys are redacted on `/healthz`,
    // and ordinary `POST /` JSON-RPC queries still pass 100%!
    let (st1, b1) = get(&c, &proxy.url("/readyz")).await;
    assert_eq!(st1, StatusCode::SERVICE_UNAVAILABLE);
    let r1: Value = serde_json::from_slice(&b1).unwrap();
    assert_eq!(r1["el_reference_lag"]["ok"], false);
    assert!(
        r1["el_reference_lag"]["detail"]
            .as_str()
            .unwrap()
            .contains("lag 2 exceeds max 1")
    );

    let (q_st, q_body) = post_json(
        &c,
        &proxy.url("/"),
        json!({"jsonrpc":"2.0","method":"eth_call","params":[],"id":9}),
    )
    .await;
    assert_eq!(q_st, StatusCode::OK);
    let q_val: Value = serde_json::from_slice(&q_body).unwrap();
    assert_eq!(q_val["result"], "0xdeadbeef");

    let (_, hb1) = get(&c, &proxy.url("/healthz")).await;
    let hb1_str = std::str::from_utf8(&hb1).unwrap();
    assert!(!hb1_str.contains("SECRET_KEY_1"), "{hb1_str}");
    assert!(!hb1_str.contains("SECRET_KEY_2"), "{hb1_str}");
    let hv1: Value = serde_json::from_slice(&hb1).unwrap();
    assert_eq!(hv1["reference"]["block_number"], 100);
    assert_eq!(hv1["reference"]["block_lag"], 2);
    assert_eq!(hv1["reference"]["errors"].as_array().unwrap().len(), 1);

    let wait_readyz = |expected: StatusCode| {
        let c = c.clone();
        let url = proxy.url("/readyz");
        wait_for(move || {
            let c = c.clone();
            let url = url.clone();
            async move {
                let (st, body) = get(&c, &url).await;
                if st == expected {
                    Ok(body)
                } else {
                    Err(format!(
                        "expected {expected}, got {st}: {}",
                        String::from_utf8_lossy(&body)
                    ))
                }
            }
        })
    };

    // 2. Partial-propagation Chain Halt (`local = 99`, `peer_ref = 100`, `lag = 1 <= 1`) -> `/readyz` recovers to 200!
    local_height.store(99, Ordering::Release);
    let _ = wait_readyz(StatusCode::OK).await;

    // 3. Lag again (`peer_ref = 105`, `lag = 6 > 1` -> 503), then test 2-strike failure (`Retained` -> `Unavailable` fail-open)!
    ref_height.store(105, Ordering::Release);
    let _ = wait_readyz(StatusCode::SERVICE_UNAVAILABLE).await;

    // Kill `peer_ref`: 1st failed round enters `Retained` (still 503), 2nd consecutive failed round enters `Unavailable` (200 fail-open)!
    ref_alive.store(false, Ordering::Release);
    let b_fo = wait_readyz(StatusCode::OK).await;
    let r_fo: Value = serde_json::from_slice(&b_fo).unwrap();
    assert_eq!(
        r_fo["el_reference_lag"]["detail"],
        "reference unavailable (fail-open)"
    );

    // 4. `chain_id` mismatch is rejected as an error, then self-heals when matching `chain_id` returns!
    ref_chain_id.store(11155111, Ordering::Release);
    ref_alive.store(true, Ordering::Release);
    wait_for(|| async {
        let (_, hb_mis) = get(&c, &proxy.url("/healthz")).await;
        let text = std::str::from_utf8(&hb_mis).unwrap();
        if text.contains("chain_id mismatch") {
            Ok(())
        } else {
            Err(text.to_owned())
        }
    })
    .await;

    ref_chain_id.store(1, Ordering::Release);
    local_height.store(105, Ordering::Release);
    wait_for(|| async {
        let (_, m_body) = get(&c, &proxy.url("/metrics")).await;
        let m_text = std::str::from_utf8(&m_body).unwrap();
        if m_text.contains("ethonyx_reference_health_status 1")
            && m_text.contains("ethonyx_reference_block_number 105")
            && m_text.contains("ethonyx_reference_block_lag 0")
        {
            Ok(())
        } else {
            Err(m_text.to_owned())
        }
    })
    .await;
    assert_eq!(get(&c, &proxy.url("/readyz")).await.0, StatusCode::OK);

    proxy.shutdown().await;
}

#[tokio::test]
async fn reference_publish_keeps_the_newest_local_half() {
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

    // The local head grows with every health batch so each local poll is distinguishable.
    let local_batches = Arc::new(AtomicU64::new(0));
    let lb = local_batches.clone();
    let local_el = MockServer::start(jsonrpc_mock(move |method| {
        RpcMockReply::Result(match method {
            "eth_syncing" => {
                lb.fetch_add(1, Ordering::Relaxed);
                json!(false)
            }
            "net_peerCount" => json!("0x2"),
            "eth_getBlockByNumber" => json!({
                "number": format!("0x{:x}", lb.load(Ordering::Relaxed)),
                "timestamp": "0x67000000",
            }),
            _ => json!("0x1"),
        })
    }))
    .await;
    let ref_rounds = Arc::new(AtomicUsize::new(0));
    let rr = ref_rounds.clone();
    let peer_ref = MockServer::start(jsonrpc_mock(move |method| {
        if method == "eth_blockNumber" {
            rr.fetch_add(1, Ordering::Relaxed);
        }
        RpcMockReply::Result(json!("0x1"))
    }))
    .await;

    // Local polls are slow (350ms) and reference rounds frequent (35ms): two reference
    // rounds complete well before the next local poll.
    let proxy = EthonyxHandle::start_tuned(
        &[
            "--el-http-url",
            &local_el.url,
            "--el-reference-url",
            &peer_ref.url,
        ],
        |cfg| {
            cfg.health_poll_interval = Duration::from_millis(350);
            cfg.el_reference_poll_interval = Some(Duration::from_millis(35));
        },
    )
    .await;
    let c = client();
    let local_head = || async {
        let (_, body) = get(&c, &proxy.url("/healthz")).await;
        let snapshot: Value = serde_json::from_slice(&body).unwrap();
        snapshot["el"]["block_number"].as_u64().unwrap()
    };

    // Head 1 is the warm-up poll; anything above was published by a background local poll.
    let published = wait_for(|| async {
        match local_head().await {
            head if head > 1 => Ok(head),
            head => Err(head.to_string()),
        }
    })
    .await;
    let rounds = ref_rounds.load(Ordering::Relaxed);
    wait_for(|| {
        let now = ref_rounds.load(Ordering::Relaxed);
        async move {
            if now >= rounds + 2 {
                Ok(())
            } else {
                Err(format!(
                    "reference rounds {now}, waiting for {}",
                    rounds + 2
                ))
            }
        }
    })
    .await;
    let head = local_head().await;
    assert!(
        head >= published,
        "a reference publish replaced the local half with an older one: {published} -> {head}"
    );

    proxy.shutdown().await;
}

#[tokio::test]
async fn shorter_reference_poll_interval_never_starves_longer_local_health_poll() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let local_batches = Arc::new(AtomicUsize::new(0));
    let ref_polls = Arc::new(AtomicUsize::new(0));
    // Reference polls already seen when the first background local poll arrives.
    let ref_polls_at_local_poll = Arc::new(AtomicUsize::new(0));
    let (lb, rp, seen) = (
        local_batches.clone(),
        ref_polls.clone(),
        ref_polls_at_local_poll.clone(),
    );
    let local_el = MockServer::start(jsonrpc_mock(move |method| {
        // Batch 1 is the warm-up poll; batch 2 is the first background local poll.
        // Record `seen` BEFORE publishing `lb == 2` with Release so the waiter's Acquire
        // load always observes the stored snapshot.
        if method == "eth_syncing" {
            if lb.load(Ordering::Relaxed) == 1 {
                seen.store(rp.load(Ordering::Relaxed), Ordering::Relaxed);
            }
            lb.fetch_add(1, Ordering::Release);
        }
        RpcMockReply::Result(match method {
            "eth_syncing" => json!(false),
            "net_peerCount" => json!("0x2"),
            "eth_getBlockByNumber" => json!({"number": "0x10", "timestamp": "0x67000000"}),
            _ => json!("0x10"),
        })
    }))
    .await;

    let rp = ref_polls.clone();
    let peer_ref = MockServer::start(jsonrpc_mock(move |method| {
        if method == "eth_blockNumber" {
            rp.fetch_add(1, Ordering::Relaxed);
        }
        RpcMockReply::Result(json!("0x10"))
    }))
    .await;

    // `health_poll_interval` (1200ms) > `el_reference_poll_interval` (40ms): faster reference
    // updates must NOT reset or starve the longer local poll timer, and the reference
    // cadence must follow its own interval instead of the local one.
    let proxy = EthonyxHandle::start_tuned(
        &[
            "--el-http-url",
            &local_el.url,
            "--el-reference-url",
            &peer_ref.url,
        ],
        |cfg| {
            cfg.health_poll_interval = Duration::from_millis(1200);
            cfg.el_reference_poll_interval = Some(Duration::from_millis(40));
        },
    )
    .await;

    wait_until(Duration::from_secs(4), || {
        let lb_val = local_batches.load(Ordering::Acquire);
        let rp_val = ref_polls.load(Ordering::Relaxed);
        async move {
            if lb_val >= 2 && rp_val >= 3 {
                Ok(())
            } else {
                Err(format!("local_batches={lb_val}, ref_polls={rp_val}"))
            }
        }
    })
    .await;
    // Thirty reference intervals fit into one local interval, so even a heavily loaded
    // runner completes two background rounds; a reference cadence that followed the
    // local interval would have polled once or twice by then, warm-up included.
    let seen = ref_polls_at_local_poll.load(Ordering::Relaxed);
    assert!(
        seen >= 3,
        "only {seen} reference polls before the first background local poll"
    );

    proxy.shutdown().await;
}

#[tokio::test]
async fn reference_gate_ignores_stale_local_block_on_local_block_number_glitch() {
    use std::sync::atomic::{AtomicBool, Ordering};

    // Sub-second chain (40ms blocks) with `health_poll_interval` (240ms) > `el_reference_poll_interval` (90ms):
    // when local `eth_blockNumber` fails, the reference gate must NOT subtract a stale `local.el_block`
    // and fabricate false 503s; instead it retains `200` on strike 1 and fails open `200` on strike 2.
    let t0 = std::time::Instant::now();
    let head = move || 1_000 + (t0.elapsed().as_millis() / 40) as u64;
    let fail_local_block_num = Arc::new(AtomicBool::new(false));
    let fail_bn = fail_local_block_num.clone();

    let fast_el = MockServer::start(jsonrpc_mock(move |method| {
        if method == "eth_blockNumber" && fail_bn.load(Ordering::Acquire) {
            return RpcMockReply::Error(-32000, "busy");
        }
        let h = format!("0x{:x}", head());
        RpcMockReply::Result(match method {
            "eth_syncing" => json!(false),
            "net_peerCount" => json!("0x4"),
            "eth_getBlockByNumber" => json!({"number": h, "timestamp": "0x67000000"}),
            "eth_chainId" => json!("0x1"),
            _ => json!(h),
        })
    }))
    .await;

    let fast_ref = MockServer::start(jsonrpc_mock(move |method| {
        RpcMockReply::Result(match method {
            "eth_chainId" => json!("0x1"),
            _ => json!(format!("0x{:x}", head())),
        })
    }))
    .await;

    let fast_proxy = EthonyxHandle::start_tuned(
        &[
            "--el-http-url",
            &fast_el.url,
            "--el-reference-url",
            &fast_ref.url,
        ],
        |cfg| {
            cfg.health_poll_interval = Duration::from_millis(240);
            cfg.el_reference_poll_interval = Some(Duration::from_millis(90));
        },
    )
    .await;
    let c = client();

    fail_local_block_num.store(true, Ordering::Release);
    // Continuous invariant-hold loop: `/readyz` must remain 200 OK on EVERY sample
    // across both strike 1 (`Retained`) and strike 2 (`Unavailable` fail-open).
    let mut saw_local_bn_err = false;
    let mut saw_fail_open = false;
    let mut post_fail_open_checks = 0u32;
    let hold_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while post_fail_open_checks < 5 {
        assert!(
            tokio::time::Instant::now() < hold_deadline,
            "timed out waiting for glitch hold checks (bn_err={saw_local_bn_err}, fail_open={saw_fail_open})"
        );
        let (st, r_body) = get(&c, &fast_proxy.url("/readyz")).await;
        assert_eq!(
            st,
            StatusCode::OK,
            "false-positive 503 during local eth_blockNumber glitch: {}",
            String::from_utf8_lossy(&r_body)
        );
        let r_val: Value = serde_json::from_slice(&r_body).unwrap();
        if r_val["el_reference_lag"]["detail"] == "reference unavailable (fail-open)" {
            saw_fail_open = true;
        }
        let (_, h_body) = get(&c, &fast_proxy.url("/healthz")).await;
        if std::str::from_utf8(&h_body)
            .unwrap()
            .contains("local eth_blockNumber:")
        {
            saw_local_bn_err = true;
        }
        if saw_local_bn_err && saw_fail_open {
            post_fail_open_checks += 1;
        }
        tokio::time::sleep(Duration::from_millis(12)).await;
    }
    fast_proxy.shutdown().await;
}

#[tokio::test]
async fn reference_gate_retains_503_on_first_local_chain_id_glitch_then_fails_open() {
    use std::sync::atomic::{AtomicBool, Ordering};

    // Local `eth_chainId` failure while lagging (50 vs 100): must hold 503 on strike 1
    // (`Retained`) with `reference_health_status == 0` and recorded error, then fail open
    // (`200`) on strike 2 (`Unavailable`).
    let fail_local_chain = Arc::new(AtomicBool::new(false));
    let c = client();
    let fail_cid = fail_local_chain.clone();
    let lag_el = MockServer::start(jsonrpc_mock(move |method| {
        if method == "eth_chainId" && fail_cid.load(Ordering::Acquire) {
            return RpcMockReply::Error(-32000, "chainId blocked");
        }
        RpcMockReply::Result(match method {
            "eth_syncing" => json!(false),
            "net_peerCount" => json!("0x4"),
            "eth_getBlockByNumber" => json!({"number": "0x32", "timestamp": "0x67000000"}),
            "eth_chainId" => json!("0x1"),
            _ => json!("0x32"),
        })
    }))
    .await;

    let ahead_ref = MockServer::start(jsonrpc_mock(|method| {
        RpcMockReply::Result(match method {
            "eth_chainId" => json!("0x1"),
            _ => json!("0x64"),
        })
    }))
    .await;

    // Use a 500ms reference poll interval (~33x the 15ms check interval) so Strike 1
    // (`Retained`, which still reports `block_lag == 50` alongside `local eth_chainId:`)
    // is observed cleanly before Strike 2 (`Unavailable`, where `block_lag` clears).
    let lag_proxy = EthonyxHandle::start_tuned(
        &[
            "--el-http-url",
            &lag_el.url,
            "--el-reference-url",
            &ahead_ref.url,
        ],
        |cfg| {
            cfg.health_poll_interval = Duration::from_millis(500);
            cfg.el_reference_poll_interval = Some(Duration::from_millis(500));
        },
    )
    .await;
    assert_eq!(
        get(&c, &lag_proxy.url("/readyz")).await.0,
        StatusCode::SERVICE_UNAVAILABLE
    );

    fail_local_chain.store(true, Ordering::Release);
    // Catch strike 1 (`Retained`) as soon as `local eth_chainId:` appears in `/healthz`.
    wait_for(|| async {
        let (_, hb) = get(&c, &lag_proxy.url("/healthz")).await;
        let val: Value = serde_json::from_slice(&hb).unwrap();
        let has_err = val["reference"]["errors"].as_array().is_some_and(|errs| {
            errs.iter()
                .any(|e| e.as_str().is_some_and(|s| s.contains("local eth_chainId:")))
        });
        if has_err && val["reference"]["block_lag"] == 50 {
            Ok(())
        } else {
            Err(val.to_string())
        }
    })
    .await;
    // On strike 1, `Retained` MUST still hold 503 while reporting `reference_health_status 0`!
    assert_eq!(
        get(&c, &lag_proxy.url("/readyz")).await.0,
        StatusCode::SERVICE_UNAVAILABLE,
        "local eth_chainId failure must not bypass 1st-strike Retained hold"
    );
    let (_, m1) = get(&c, &lag_proxy.url("/metrics")).await;
    assert!(
        std::str::from_utf8(&m1)
            .unwrap()
            .contains("ethonyx_reference_health_status 0")
    );

    // On strike 2 (`Unavailable`), `/readyz` fails open (`200`) while keeping the error visible.
    wait_for(|| async {
        let (st, body) = get(&c, &lag_proxy.url("/readyz")).await;
        if st == StatusCode::OK {
            let r_val: Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(
                r_val["el_reference_lag"]["detail"],
                "reference unavailable (fail-open)"
            );
            Ok(())
        } else {
            Err(format!("{st}: {}", String::from_utf8_lossy(&body)))
        }
    })
    .await;
    let (_, hb2) = get(&c, &lag_proxy.url("/healthz")).await;
    assert!(
        std::str::from_utf8(&hb2)
            .unwrap()
            .contains("local eth_chainId:")
    );

    lag_proxy.shutdown().await;
}
