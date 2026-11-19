//! HTTP/1.1 & HTTP/2 JSON-RPC and Beacon routing, header sanitization, mocks, SSE, and connection-limit tests.

use super::*;
use serde_json::json;
use tokio_tungstenite::{connect_async, tungstenite::Message};

#[tokio::test]
async fn jsonrpc_post_is_forwarded_to_el_http_upstream() {
    let el = MockServer::start(Arc::new(|req| {
        assert_eq!(req.method, Method::POST);
        assert_eq!(req.path, "/");
        (
            StatusCode::OK,
            Bytes::from_static(br#"{"jsonrpc":"2.0","id":7,"result":"0xdeadbeef"}"#),
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
            Bytes::from_static(br#"{"jsonrpc":"2.0","id":7,"result":"0xcafe"}"#),
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
    let el = MockServer::start(ok_handler()).await;
    let cl = MockServer::start(Arc::new(|_| {
        (
            StatusCode::OK,
            Bytes::from_static(br#"{"data":{"genesis_time":"1606824023"}}"#),
        )
    }))
    .await;
    let ethonyx = EthonyxHandle::start(&[
        "--el-http-url",
        &el.url,
        "--cl-beacon-url",
        &cl.url,
        "--trust-upstream",
    ])
    .await;

    let c = client();
    let (status, body) = get(&c, &ethonyx.url("/eth/v1/beacon/genesis")).await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["data"]["genesis_time"], "1606824023");

    assert!(
        el.received().is_empty(),
        "EL must not see beacon /eth/ traffic: {:?}",
        el.received()
    );
    let cl_reqs = cl.received();
    assert_eq!(cl_reqs.len(), 1);
    assert_eq!(cl_reqs[0].method, Method::GET);
    assert_eq!(cl_reqs[0].path, "/eth/v1/beacon/genesis");

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
        (StatusCode::OK, Bytes::from_static(b"{}"))
    }))
    .await;
    let cl = MockServer::start(Arc::new(|req| {
        assert_eq!(req.method, Method::GET);
        let resp = format!(r#"{{"path":"{}"}}"#, req.path);
        (StatusCode::OK, Bytes::from(resp))
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
    for (proto, expected_version) in [
        (MockProto::Http1, Version::HTTP_11),
        (MockProto::H2c, Version::HTTP_2),
    ] {
        let el = MockServer::serve(ok_handler(), proto).await;
        let ethonyx = EthonyxHandle::start(&["--el-http-url", &el.url]).await;
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
        let forwarded = received
            .iter()
            .find(|r| r.body.as_ref() == b"{}")
            .expect("forwarded request");
        assert_eq!(forwarded.version, expected_version);
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
    let _server = AbortOnDrop(tokio::spawn(async move {
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
    }));
    let ethonyx = EthonyxHandle::start_trusted(&["--cl-beacon-url", &upstream]).await;
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
}

#[tokio::test]
async fn multi_port_listen_serves_same_routes() {
    let el = MockServer::start(ok_handler()).await;
    let cl = MockServer::start(ok_handler()).await;

    let cfg = Config::try_parse_from([
        "ethonyx",
        "--listen",
        "127.0.0.1:0",
        "--listen",
        "127.0.0.1:0",
        "--el-http-url",
        &el.url,
        "--cl-beacon-url",
        &cl.url,
        "--shutdown-grace",
        "0",
    ])
    .unwrap();

    let bound = Prepared::new(cfg).await.unwrap().bind().await.unwrap();
    let [a1, a2] = bound.local_addrs() else {
        panic!("expected two bound addresses");
    };
    let ports = [a1.port(), a2.port()];

    let (tx, rx) = oneshot::channel::<()>();
    let task = tokio::spawn(bound.serve(async move {
        let _ = rx.await;
    }));

    let c = client();
    for port in ports {
        let (status, body) = get(&c, &format!("http://127.0.0.1:{port}/livez")).await;
        assert_eq!(status, StatusCode::OK, "port {port}");
        assert_eq!(&body[..], b"ok", "port {port}");
    }

    let _ = tx.send(());
    let _ = task.await;
}

#[tokio::test]
async fn trust_upstream_decoupled_from_mock_eth_syncing() {
    let ws_upstream = ws_echo_upstream().await;

    let el = MockServer::start_h2c(Arc::new(|req| {
        if req.body.windows(11).any(|w| w == b"eth_syncing") {
            (
                StatusCode::OK,
                Bytes::from_static(br#"{"jsonrpc":"2.0","id":55,"result":"upstream_result"}"#),
            )
        } else {
            (StatusCode::OK, Bytes::from_static(b"{}"))
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
        &ws_upstream.url,
        "--trust-upstream",
    ])
    .await;

    let c = client();
    let (status, body) = post_json(
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
        &ws_upstream.url,
        "--mock-eth-syncing",
    ])
    .await;

    let (status_m, body_m) = post_json(
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
    let el = MockServer::start(ok_handler()).await;
    let cl = MockServer::start(Arc::new(|_| {
        (
            StatusCode::BAD_REQUEST,
            Bytes::from_static(b"{\"error\": \"Unsupported method\"}"),
        )
    }))
    .await;
    let mev_relay = MockServer::start(Arc::new(|req| {
        if req.path == "/eth/v1/builder/validators" && req.method == "POST" {
            (StatusCode::OK, Bytes::from_static(b"[]"))
        } else {
            (StatusCode::NOT_FOUND, Bytes::from_static(b"not found"))
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
        "--trust-upstream",
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
    let relay_reqs = mev_relay.received();
    assert_eq!(relay_reqs.len(), 1);
    assert_eq!(relay_reqs[0].method, Method::POST);
    assert_eq!(relay_reqs[0].path, "/eth/v1/builder/validators");
    assert_eq!(relay_reqs[0].body.as_ref(), b"[]");
    assert!(cl.received().is_empty());

    ethonyx.shutdown().await;
}

#[tokio::test]
async fn test_mock_cl_subscriptions() {
    let el = MockServer::start(Arc::new(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Bytes::from_static(b"should not be called"),
        )
    }))
    .await;
    let cl = MockServer::start(Arc::new(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Bytes::from_static(b"should not be called"),
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
            Bytes::from_static(b"should not be called"),
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
    let proxy = EthonyxHandle::start_trusted_tuned(&["--mock-eth-syncing"], |cfg| {
        cfg.proxy_timeout = Duration::from_millis(200);
    })
    .await;
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
async fn http_forwarding_preserves_configured_uri_and_unmodified_body() {
    let payloads: &[&[u8]] = &[
        br#"{"jsonrpc":"2.0","method":"eth_blockNumber","id":7}"#,
        br#"{"jsonrpc":"2.0","method":"eth_call","params":{"method":"eth_syncing"},"id":null}"#,
        b"malformed JSON \xff",
    ];
    let el = MockServer::start(Arc::new(|req| (StatusCode::OK, req.body.clone()))).await;
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
                        let received = el.last_received().unwrap();
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
async fn invalid_jsonrpc_batches_are_forwarded_intact_over_http_and_websocket() {
    // This upstream echoes the complete body instead of interpreting the batch.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream = format!("http://{}", listener.local_addr().unwrap());
    let _server = AbortOnDrop(tokio::spawn(async move {
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
    }));
    let ws_upstream = ws_echo_upstream().await;
    let proxy = EthonyxHandle::start(&[
        "--trust-upstream",
        "--mock-eth-syncing",
        "--el-http-url",
        &upstream,
        "--el-ws-url",
        &ws_upstream.url,
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
}

#[tokio::test]
async fn mock_eth_syncing_forwards_non_empty_params_over_http_and_websocket() {
    let el = MockServer::start(Arc::new(|req| (StatusCode::OK, req.body.clone()))).await;
    let ws_upstream = ws_echo_upstream().await;
    let proxy = EthonyxHandle::start(&[
        "--trust-upstream",
        "--mock-eth-syncing",
        "--el-http-url",
        &el.url,
        "--el-ws-url",
        &ws_upstream.url,
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
    let dead = StubPort::dead().await;
    let dead_url = dead.http_url();
    let live_el = MockServer::start(Arc::new(|_| {
        (
            StatusCode::OK,
            Bytes::from_static(br#"{"jsonrpc":"2.0","id":1,"result":"0x2a"}"#),
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

    let proxy = EthonyxHandle::start_trusted_tuned(
        &[
            "--cl-beacon-url",
            "http://127.0.0.1:5052",
            "--mock-cl-subscriptions",
        ],
        |cfg| cfg.proxy_timeout = Duration::from_millis(250),
    )
    .await;

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

        let expected_body = b"{\"code\":200,\"message\":\"success\"}";
        let mut resp = Vec::new();
        let mut chunk = [0u8; 512];
        tokio::time::timeout(Duration::from_secs(2), async {
            while !resp.ends_with(expected_body) {
                let n = keepalive.read(&mut chunk).await.unwrap();
                assert_ne!(
                    n,
                    0,
                    "round {i}: unexpected EOF on keep-alive socket: {}",
                    String::from_utf8_lossy(&resp)
                );
                resp.extend_from_slice(&chunk[..n]);
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "round {i}: timed out waiting for response on keep-alive socket: {}",
                String::from_utf8_lossy(&resp)
            )
        });
        let text = String::from_utf8_lossy(&resp);
        assert!(
            text.starts_with("HTTP/1.1 200 OK"),
            "round {i}: expected 200 OK on keep-alive socket, got: {text}"
        );
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
async fn sse_event_stream_survives_past_proxy_timeout_while_stalled_json_aborts() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // Custom HTTP/1.1 upstream: `/eth/v1/events` emits an SSE stream with a single 300ms
    // inter-frame idle gap (> 220ms proxy_timeout, proving `sse_idle_timeout` is active);
    // `/stalled` sends 200 JSON headers and stalls past `proxy_timeout`.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let upstream_url = format!("http://{addr}");
    let _upstream = AbortOnDrop(tokio::spawn(async move {
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
                    for (idx, delay_ms) in [(1, 0u64), (2, 300), (3, 40)] {
                        if delay_ms > 0 {
                            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                        }
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
    }));

    let proxy = EthonyxHandle::start_tuned(
        &[
            "--trust-upstream",
            "--sse-idle-timeout",
            "5",
            "--cl-beacon-url",
            &upstream_url,
            "--el-http-url",
            &upstream_url,
        ],
        |cfg| cfg.proxy_timeout = Duration::from_millis(220),
    )
    .await;
    let c = client();

    // Run both checks concurrently:
    // 1. SSE stream stays alive across 300ms (> 220ms proxy_timeout) and delivers all 3 events.
    // 2. Ordinary JSON response that stalls mid-body > 220ms aborts cleanly instead of hanging.
    let sse_check = async {
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
    };
    let stalled_json_check = async {
        let json_req = Request::builder()
            .method(Method::GET)
            .uri(proxy.url("/stalled"))
            .body(Full::new(Bytes::new()))
            .unwrap();
        let json_resp = c.request(json_req).await.unwrap();
        assert_eq!(json_resp.status(), StatusCode::OK);
        let collected =
            tokio::time::timeout(Duration::from_secs(2), json_resp.into_body().collect())
                .await
                .expect("idle timeout must abort stalled body before 2s");
        assert!(
            collected.is_err(),
            "truncated stalled body must fail collection"
        );
    };
    tokio::join!(sse_check, stalled_json_check);

    proxy.shutdown().await;
}

#[tokio::test]
async fn max_connections_caps_concurrent_http_and_upgraded_websocket_clients() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let ws_upstream = ws_echo_upstream().await;
    let proxy =
        EthonyxHandle::start_trusted(&["--max-connections", "2", "--el-ws-url", &ws_upstream.url])
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
async fn multi_listener_max_connections_fairness_and_h2_extended_connect_cap() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let ws_upstream = ws_echo_upstream().await;

    let proxy = EthonyxHandle::start_trusted(&[
        "--listen",
        "127.0.0.1:0",
        "--max-connections",
        "1",
        "--el-ws-url",
        &ws_upstream.url,
    ])
    .await;
    let second_port = proxy.ports[1];

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
    let mut sender = proxy.open_h2().await;

    let before_resp = sender
        .send_request(
            Request::builder()
                .method(Method::GET)
                .uri(format!("http://127.0.0.1:{}/metrics", proxy.port))
                .body(Full::new(Bytes::new()))
                .unwrap(),
        )
        .await
        .unwrap();
    let before_bytes = before_resp.into_body().collect().await.unwrap().to_bytes();
    let before = prom_metric_sum(
        std::str::from_utf8(&before_bytes).unwrap(),
        "ethonyx_connection_limit_saturated_total",
        &[],
    );

    let make_ext_connect = |port: u16| ext_connect(format!("http://127.0.0.1:{port}/"));

    let resp1 = sender
        .send_request(make_ext_connect(proxy.port))
        .await
        .unwrap();
    assert_eq!(resp1.status(), StatusCode::OK);
    let mut ws1 = upgrade_ws(resp1).await;
    ws1.send(Message::Text("hello".into())).await.unwrap();
    assert_eq!(
        ws1.next().await.unwrap().unwrap(),
        Message::Text("hello".into())
    );
    assert_eq!(ws_upstream.dials(), 1);

    let resp2 = sender
        .send_request(make_ext_connect(proxy.port))
        .await
        .unwrap();
    assert_eq!(resp2.status(), StatusCode::SERVICE_UNAVAILABLE);
    drop(resp2);
    assert_eq!(
        ws_upstream.dials(),
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

    // Wait for the spawned bridge task to drop its shared H2 permit.
    wait_for(|| {
        let fut = sender.send_request(make_ext_connect(proxy.port));
        async move {
            let resp3 = fut.await.unwrap();
            if resp3.status() == StatusCode::OK {
                let upgraded3 = hyper::upgrade::on(resp3).await.unwrap();
                drop(upgraded3);
                Ok(())
            } else {
                Err(format!("status {}", resp3.status()))
            }
        }
    })
    .await;

    drop(sender);

    // Saturation metric is exposed on /metrics once the H2 TCP connection closes.
    let c = client();
    let metrics_resp = tokio::time::timeout(
        Duration::from_secs(3),
        c.get(proxy.url("/metrics").parse().unwrap()),
    )
    .await
    .expect("closing H2 connection must release --max-connections 1 permit")
    .unwrap();
    let metrics_bytes = metrics_resp.into_body().collect().await.unwrap().to_bytes();
    let metrics_text = std::str::from_utf8(&metrics_bytes).unwrap();
    let after = prom_metric_sum(
        metrics_text,
        "ethonyx_connection_limit_saturated_total",
        &[],
    );
    assert!(
        after >= before + 1.0,
        "expected saturation counter to increment (before={before}, after={after}), got:\n{metrics_text}"
    );

    proxy.shutdown().await;
}

#[tokio::test]
async fn query_only_upstream_url_reaches_h2c_and_h1_as_root_path() {
    for h2 in [true, false] {
        let handler: MockHandler = Arc::new(|_| {
            (
                StatusCode::OK,
                Bytes::from_static(br#"{"jsonrpc":"2.0","id":7,"result":"0xcafe"}"#),
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
        let (status, _) = post_json(
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

#[tokio::test]
async fn post_accept_permits_allow_two_h2_ws_streams_and_reject_invalid_or_oversized_targets() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let ws_upstream = ws_echo_upstream().await;
    let http_upstream = ws_upstream.url.replacen("ws://", "http://", 1);
    let long_prefix = "k".repeat(62_000);
    let scoped_el = format!("{http_upstream}/v3/{long_prefix}");
    let scoped_ws = format!("{}/v3/{long_prefix}", ws_upstream.url);

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
    let port = proxy.port;
    let mut sender = proxy.open_h2().await;

    let fetch_metrics = |sender: &mut hyper::client::conn::http2::SendRequest<TestBody>| {
        let req = Request::builder()
            .method(Method::GET)
            .uri(format!("http://127.0.0.1:{port}/metrics"))
            .body(Full::new(Bytes::new()))
            .unwrap();
        let fut = sender.send_request(req);
        async move {
            let resp = fut.await.unwrap();
            assert_eq!(resp.status(), StatusCode::OK);
            resp.into_body().collect().await.unwrap().to_bytes()
        }
    };

    let m0_bytes = fetch_metrics(&mut sender).await;
    let m0 = std::str::from_utf8(&m0_bytes).unwrap();
    assert_eq!(
        prom_metric_sum(m0, "ethonyx_connection_limit_saturated_total", &[]),
        0.0
    );
    assert_eq!(
        prom_metric_sum(m0, "ethonyx_proxy_requests_total", &[r#"method="OPTIONS""#]),
        0.0
    );
    assert_eq!(
        prom_metric_sum(m0, "ethonyx_proxy_requests_total", &[r#"method="CONNECT""#]),
        0.0
    );

    // 1. Rejected targets (`OPTIONS *` and CL `CONNECT`) return 400
    // BEFORE `ethonyx_proxy_requests_total` is recorded!
    let mut raw_sock = tokio::net::TcpStream::connect(("127.0.0.1", port))
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
    // Ensure `serve_connection` finishes dropping the 2nd TCP permit before section 3
    // (`_guard` decrements `active_connections{protocol="tcp"}` strictly after `permit` drops).
    wait_for(|| {
        let fut = fetch_metrics(&mut sender);
        async move {
            let body = fut.await;
            let text = std::str::from_utf8(&body).unwrap();
            let active =
                prom_metric_sum(text, "ethonyx_active_connections", &[r#"protocol="tcp""#]);
            (active == 1.0)
                .then_some(())
                .ok_or_else(|| format!("active_tcp={active}"))
        }
    })
    .await;

    let mut cl_connect = Request::builder()
        .method(Method::CONNECT)
        .uri(format!("http://127.0.0.1:{port}/eth/v1/node/health"))
        .body(Full::new(Bytes::new()))
        .unwrap();
    cl_connect
        .extensions_mut()
        .insert(hyper::ext::Protocol::from_static("websocket"));
    let cl_connect_resp = sender.send_request(cl_connect).await.unwrap();
    assert_eq!(cl_connect_resp.status(), StatusCode::BAD_REQUEST);
    drop(cl_connect_resp);

    let m1_bytes = fetch_metrics(&mut sender).await;
    let m1 = std::str::from_utf8(&m1_bytes).unwrap();
    assert_eq!(
        prom_metric_sum(m1, "ethonyx_proxy_requests_total", &[r#"method="OPTIONS""#]),
        0.0,
        "OPTIONS * must not increment ethonyx_proxy_requests_total"
    );
    assert_eq!(
        prom_metric_sum(m1, "ethonyx_proxy_requests_total", &[r#"method="CONNECT""#]),
        0.0,
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

    let long_ws = ext_connect(&near_limit_path);
    let long_ws_resp = sender.send_request(long_ws).await.unwrap();
    assert_eq!(long_ws_resp.status(), StatusCode::URI_TOO_LONG);
    drop(long_ws_resp);

    // 3. Under --max-connections 2, post-accept permit acquisition leaves the 2nd
    // permit free so 1 H2 connection can open TWO concurrent Extended CONNECT WS
    // streams (`IncludedH2` + `DedicatedH2`) with zero spurious saturation increments,
    // and rejects the 3rd concurrent stream with 503 (+1 saturation).
    let make_ext_ws = || ext_connect(format!("http://127.0.0.1:{port}/"));

    let ws_resp1 = sender.send_request(make_ext_ws()).await.unwrap();
    assert_eq!(ws_resp1.status(), StatusCode::OK);
    let mut ws1 = upgrade_ws(ws_resp1).await;

    let ws_resp2 = sender.send_request(make_ext_ws()).await.unwrap();
    assert_eq!(
        ws_resp2.status(),
        StatusCode::OK,
        "2nd H2 Extended CONNECT stream must acquire the 2nd permit under --max-connections 2"
    );
    let mut ws2 = upgrade_ws(ws_resp2).await;

    let m2_bytes = fetch_metrics(&mut sender).await;
    let m2 = std::str::from_utf8(&m2_bytes).unwrap();
    assert_eq!(
        prom_metric_sum(m2, "ethonyx_connection_limit_saturated_total", &[]),
        0.0,
        "saturation counter must stay unchanged before actual over-limit contention"
    );

    let ws_resp3 = sender.send_request(make_ext_ws()).await.unwrap();
    assert_eq!(ws_resp3.status(), StatusCode::SERVICE_UNAVAILABLE);
    drop(ws_resp3);

    let m3_bytes = fetch_metrics(&mut sender).await;
    let m3 = std::str::from_utf8(&m3_bytes).unwrap();
    assert_eq!(
        prom_metric_sum(m3, "ethonyx_connection_limit_saturated_total", &[]),
        1.0,
        "3rd concurrent H2 WS stream under --max-connections 2 must record +1 saturation"
    );

    let _ = ws1.close(None).await;
    let _ = ws2.close(None).await;
    drop((ws1, ws2, sender));

    proxy.shutdown().await;
}

#[tokio::test]
async fn upstream_that_never_answers_is_a_502_after_proxy_timeout() {
    let silent = StubPort::silent().await;
    let url = silent.http_url();
    let proxy = EthonyxHandle::start_tuned(&["--trust-upstream", "--el-http-url", &url], |cfg| {
        cfg.proxy_timeout = Duration::from_millis(200);
    })
    .await;
    let started = tokio::time::Instant::now();
    let (http, proxy_url) = (client(), proxy.url("/"));
    let body = json!({"jsonrpc":"2.0","method":"eth_blockNumber","id":1});
    let (status, body) =
        tokio::time::timeout(Duration::from_secs(5), post_json(&http, &proxy_url, body))
            .await
            .expect("--proxy-timeout must end the request");
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert_eq!(body.as_ref(), b"bad gateway");
    assert!(
        started.elapsed() >= Duration::from_millis(200),
        "the 502 must come from --proxy-timeout (took {:?})",
        started.elapsed()
    );
    proxy.shutdown().await;
}
