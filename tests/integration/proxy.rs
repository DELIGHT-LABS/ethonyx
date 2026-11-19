//! HTTP/1.1 & HTTP/2 JSON-RPC and Beacon routing, header sanitization, mocks, SSE, and connection-limit tests.

use super::*;
use serde_json::json;
use std::borrow::Cow;
use tokio_tungstenite::{connect_async, tungstenite::Message};

/// Read an HTTP/1.x request head (up to `\r\n\r\n`) from a raw TCP socket.
async fn read_http1_head(sock: &mut tokio::net::TcpStream) -> Option<String> {
    use tokio::io::AsyncReadExt;

    let mut buf = vec![0u8; 8192];
    let mut filled = 0usize;
    loop {
        let n = sock.read(&mut buf[filled..]).await.ok()?;
        if n == 0 {
            return None;
        }
        filled += n;
        if let Some(pos) = buf[..filled].windows(4).position(|w| w == b"\r\n\r\n") {
            return Some(String::from_utf8_lossy(&buf[..pos]).into_owned());
        }
        if filled == buf.len() {
            return None;
        }
    }
}

/// Spawn a raw HTTP/1.x mock upstream replying to each connection's request head and closing.
async fn spawn_raw_http1_upstream<F>(handler: F) -> (String, AbortOnDrop)
where
    F: Fn(&str) -> Cow<'static, [u8]> + Send + Sync + 'static,
{
    use tokio::io::AsyncWriteExt;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let handler = Arc::new(handler);
    let server = AbortOnDrop(tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            let handler = Arc::clone(&handler);
            tokio::spawn(async move {
                let Some(head) = read_http1_head(&mut sock).await else {
                    return;
                };
                let reply = handler(&head);
                let _ = sock.write_all(&reply).await;
                let _ = sock.shutdown().await;
            });
        }
    }));
    (url, server)
}

/// Send a raw HTTP/1.1 request over loopback TCP and read the full response until EOF.
async fn raw_http1_exchange(port: u16, req: &[u8]) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut sock = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    sock.write_all(req).await.unwrap();
    let mut resp = Vec::new();
    tokio::time::timeout(Duration::from_secs(2), sock.read_to_end(&mut resp))
        .await
        .expect("raw HTTP/1.1 exchange must complete before timeout")
        .unwrap();
    String::from_utf8_lossy(&resp).into_owned()
}

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
    // HTTP/2 by the server, and its JSON-RPC is forwarded to the EL upstream
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
    let req = Request::builder()
        .method(Method::POST)
        .uri(ethonyx.url("/"))
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(
            serde_json::to_vec(
                &json!({"jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id": 7}),
            )
            .unwrap(),
        )))
        .unwrap();
    let resp = c.request(req).await.unwrap();
    assert_eq!(resp.version(), Version::HTTP_2);
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let v: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["result"], "0xcafe");

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
    // Exhaustive `rewrite_uri` combinations are unit-tested in `upstream.rs`; verify
    // end-to-end over both H1 and H2c across a root unmocked upstream and a scoped
    // percent-encoded query upstream with `--mock-eth-syncing` enabled.
    let payloads: &[&[u8]] = &[
        br#"{"jsonrpc":"2.0","method":"eth_blockNumber","id":7}"#,
        br#"{"jsonrpc":"2.0","method":"eth_call","params":{"method":"eth_syncing"},"id":null}"#,
        b"malformed JSON \xff",
    ];
    let el = MockServer::start(Arc::new(|req| (StatusCode::OK, req.body.clone()))).await;
    let paths = ["/", "/?tag=a%2Fb", "/rpc/extra?tag=a%2Fb"];
    for (base_path, mock_enabled, expected_pqs) in [
        ("", false, ["/", "/?tag=a%2Fb", "/rpc/extra?tag=a%2Fb"]),
        (
            "/rpc?key=a%2Fb",
            true,
            [
                "/rpc?key=a%2Fb",
                "/rpc?key=a%2Fb&tag=a%2Fb",
                "/rpc/rpc/extra?key=a%2Fb&tag=a%2Fb",
            ],
        ),
    ] {
        let upstream = format!("{}{base_path}", el.url);
        let mut args = vec!["--el-http-url", &upstream, "--trust-upstream"];
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
        let resp = raw_http1_exchange(
            port,
            b"GET /livez HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        )
        .await;
        assert!(resp.starts_with("HTTP/1.1 200 OK"), "got: {resp}");
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
    let raw_resp = raw_http1_exchange(
        port,
        b"OPTIONS * HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    )
    .await;
    assert!(
        raw_resp.starts_with("HTTP/1.1 400 Bad Request"),
        "got: {raw_resp}"
    );
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
    // streams (`WsPermit::Shared` + `WsPermit::Dedicated`) with zero spurious saturation increments,
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

#[tokio::test]
async fn upstream_http10_normalizes_to_http11_keep_alive_and_unsolicited_101_is_502() {
    let (upstream, _server) = spawn_raw_http1_upstream(|head| {
        if head.starts_with("GET /101 ") {
            Cow::Borrowed(
                b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: h2c\r\nConnection: Upgrade\r\n\r\n",
            )
        } else {
            Cow::Borrowed(b"HTTP/1.0 200 OK\r\n\r\nok-from-h10")
        }
    })
    .await;

    let proxy = EthonyxHandle::start(&["--trust-upstream", "--el-http-url", &upstream]).await;

    // 1. Close-delimited HTTP/1.0 upstream responses must be normalized to HTTP/1.1 so
    // a downstream HTTP/1.1 client keeps its connection alive across sequential requests.
    let tcp = tokio::net::TcpStream::connect(("127.0.0.1", proxy.port))
        .await
        .unwrap();
    let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(tcp))
        .await
        .unwrap();
    let _conn = AbortOnDrop(tokio::spawn(async move {
        let _ = conn.await;
    }));
    for _ in 0..2 {
        let req = Request::builder()
            .method(Method::GET)
            .uri(proxy.url("/http10"))
            .body(Full::new(Bytes::new()))
            .unwrap();
        let resp = sender.send_request(req).await.unwrap();
        assert_eq!(resp.version(), Version::HTTP_11);
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.into_body().collect().await.unwrap().to_bytes(),
            &b"ok-from-h10"[..]
        );
    }

    // 2. Unsolicited 101 Switching Protocols on a plain HTTP request must return 502 on
    // both HTTP/1.1 and HTTP/2 clients instead of closing H1 or resetting the H2 stream.
    for (label, c) in [("h1", client()), ("h2c", h2c_client())] {
        let (status, body) = get(&c, &proxy.url("/101")).await;
        assert_eq!(status, StatusCode::BAD_GATEWAY, "{label} unsolicited 101");
        assert_eq!(body.as_ref(), b"bad gateway", "{label} unsolicited 101");
    }

    proxy.shutdown().await;
}

#[tokio::test]
async fn oversized_content_length_and_body_carrying_ws_upgrade_fail_fast_without_reading() {
    let ws_upstream = ws_echo_upstream().await;

    let proxy = EthonyxHandle::start_trusted(&[
        "--mock-eth-syncing",
        "--cl-beacon-url",
        "http://127.0.0.1:5052",
        "--mock-cl-subscriptions",
        "--el-ws-url",
        &ws_upstream.url,
    ])
    .await;

    // 1. Oversized Content-Length (> 64 MiB) + Expect: 100-continue on the local-only
    // mocked CL subscription endpoint must return 413 immediately without sending `100 Continue`.
    let text = raw_http1_exchange(
        proxy.port,
        b"POST /eth/v1/validator/prepare_beacon_proposer HTTP/1.1\r\nHost: localhost\r\nContent-Length: 100000000\r\nExpect: 100-continue\r\n\r\n",
    )
    .await;
    assert!(
        text.starts_with("HTTP/1.1 413 Payload Too Large"),
        "expected immediate 413 without 100 Continue, got: {text}"
    );
    assert!(!text.contains("100 Continue"), "{text}");

    // 2. Unsupported Transfer-Encoding on inbound requests must return 501 Not Implemented.
    let text = raw_http1_exchange(
        proxy.port,
        b"POST / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nTransfer-Encoding: gzip, chunked\r\n\r\n0\r\n\r\n",
    )
    .await;
    assert!(
        text.starts_with("HTTP/1.1 501 Not Implemented"),
        "expected 501 for unsupported Transfer-Encoding, got: {text}"
    );

    // 3. HTTP/1.1 WebSocket Upgrade carrying a non-empty request body must be rejected
    // with 400 Bad Request before dialing the upstream WebSocket server.
    for framing in ["Content-Length: 5", "Transfer-Encoding: chunked"] {
        let req = format!(
            "GET / HTTP/1.1\r\nHost: localhost\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n{framing}\r\n\r\n"
        );
        let text = raw_http1_exchange(proxy.port, req.as_bytes()).await;
        assert!(
            text.starts_with("HTTP/1.1 400 Bad Request"),
            "framing={framing}: expected 400, got: {text}"
        );
    }
    assert_eq!(ws_upstream.dials(), 0);

    proxy.shutdown().await;
}

#[tokio::test]
async fn mock_eth_syncing_streams_large_bodies_over_64k_without_buffering_or_413() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let el = MockServer::start(Arc::new(|req| (StatusCode::OK, req.body.clone()))).await;
    let proxy = EthonyxHandle::start(&[
        "--trust-upstream",
        "--mock-eth-syncing",
        "--el-http-url",
        &el.url,
    ])
    .await;

    let c = client();

    // 0. Exact 64 KiB boundary: a 64 KiB `eth_syncing` body is inspected and mocked locally
    // (0 upstream requests), while 64 KiB + 1 byte bypasses inspection and streams upstream.
    let syncing_req = r#"{"jsonrpc":"2.0","method":"eth_syncing","params":[],"id":1}"#;
    let exact_64k = format!("{}{syncing_req}", " ".repeat(64 * 1024 - syncing_req.len()));
    assert_eq!(exact_64k.len(), 64 * 1024);
    let resp_64k = c
        .request(
            Request::builder()
                .method(Method::POST)
                .uri(proxy.url("/"))
                .header("content-type", "application/json")
                .body(Full::new(Bytes::from(exact_64k)))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp_64k.status(), StatusCode::OK);
    let body_64k = resp_64k.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(
        body_64k.as_ref(),
        br#"{"jsonrpc":"2.0","id":1,"result":false}"#
    );
    assert!(
        el.recorded.lock().unwrap().is_empty(),
        "64 KiB exact must mock locally"
    );

    let over_64k = Bytes::from(format!(
        "{}{syncing_req}",
        " ".repeat(64 * 1024 + 1 - syncing_req.len())
    ));
    assert_eq!(over_64k.len(), 64 * 1024 + 1);
    let resp_over = c
        .request(
            Request::builder()
                .method(Method::POST)
                .uri(proxy.url("/"))
                .header("content-type", "application/json")
                .body(Full::new(over_64k.clone()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp_over.status(), StatusCode::OK);
    let body_over = resp_over.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(
        body_over, over_64k,
        "64 KiB + 1 must stream straight to upstream"
    );

    // 1. Known Content-Length > 64 KiB skips inspection completely and streams straight through.
    let large_payload = Bytes::from(format!(
        "{{\"jsonrpc\":\"2.0\",\"method\":\"eth_call\",\"params\":[\"{}\"],\"id\":1}}",
        "a".repeat(96 * 1024)
    ));
    let req = Request::builder()
        .method(Method::POST)
        .uri(proxy.url("/"))
        .header("content-type", "application/json")
        .body(Full::new(large_payload.clone()))
        .unwrap();
    let resp = c.request(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let echoed = resp.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(echoed, large_payload);

    // 2. Chunked request (no Content-Length) crossing the 64 KiB inspection boundary replays
    // its buffered prefix + remaining live chunks intact via `ResBody::prefixed`.
    let mut sock = tokio::net::TcpStream::connect(("127.0.0.1", proxy.port))
        .await
        .unwrap();
    sock.write_all(
        b"POST / HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n",
    )
    .await
    .unwrap();
    for chunk in large_payload.chunks(24 * 1024) {
        sock.write_all(format!("{:x}\r\n", chunk.len()).as_bytes())
            .await
            .unwrap();
        sock.write_all(chunk).await.unwrap();
        sock.write_all(b"\r\n").await.unwrap();
    }
    sock.write_all(b"0\r\n\r\n").await.unwrap();
    let mut raw_resp = Vec::new();
    tokio::time::timeout(Duration::from_secs(3), sock.read_to_end(&mut raw_resp))
        .await
        .unwrap()
        .unwrap();
    let split = raw_resp
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("HTTP response separator");
    let head = std::str::from_utf8(&raw_resp[..split]).unwrap();
    assert!(head.starts_with("HTTP/1.1 200 OK"), "got: {head}");
    assert_eq!(&raw_resp[split + 4..], large_payload.as_ref());

    proxy.shutdown().await;
}

#[tokio::test]
async fn http2_head_and_bodyless_upstream_responses_never_emit_forbidden_data_or_cl() {
    struct OpenBody {
        chunk: Option<Bytes>,
        exact: Option<u64>,
    }

    impl hyper::body::Body for OpenBody {
        type Data = Bytes;
        type Error = Infallible;

        fn poll_frame(
            mut self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Result<hyper::body::Frame<Bytes>, Infallible>>> {
            std::task::Poll::Ready(self.chunk.take().map(|b| Ok(hyper::body::Frame::data(b))))
        }

        fn is_end_stream(&self) -> bool {
            self.chunk.is_none()
        }

        fn size_hint(&self) -> hyper::body::SizeHint {
            self.exact.map_or_else(
                hyper::body::SizeHint::default,
                hyper::body::SizeHint::with_exact,
            )
        }
    }

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_url = format!("http://{}", listener.local_addr().unwrap());
    let _server = AbortOnDrop(tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                let svc = service_fn(|req: Request<Incoming>| async move {
                    let path = req.uri().path().to_owned();
                    let _ = req.into_body().collect().await;
                    let resp = match path.as_str() {
                        "/204-open" => Response::builder()
                            .status(StatusCode::NO_CONTENT)
                            .header("content-length", "5")
                            .body(OpenBody {
                                chunk: Some(Bytes::from_static(b"hello")),
                                exact: Some(5),
                            })
                            .unwrap(),
                        "/304-open" => Response::builder()
                            .status(StatusCode::NOT_MODIFIED)
                            .header("content-length", "5")
                            .body(OpenBody {
                                chunk: Some(Bytes::from_static(b"hello")),
                                exact: Some(5),
                            })
                            .unwrap(),
                        "/205-nonzero" => Response::builder()
                            .status(StatusCode::RESET_CONTENT)
                            .header("content-length", "5")
                            .body(OpenBody {
                                chunk: None,
                                exact: None,
                            })
                            .unwrap(),
                        "/head-open" => Response::builder()
                            .status(StatusCode::OK)
                            .header("content-length", "42")
                            .body(OpenBody {
                                chunk: Some(Bytes::new()),
                                exact: None,
                            })
                            .unwrap(),
                        _ => {
                            let batch = Bytes::from_static(
                                br#"[{"jsonrpc":"2.0","id":1,"result":false},{"jsonrpc":"2.0","id":2,"result":"0x5"},{"jsonrpc":"2.0","id":3,"result":{"number":"0x1","timestamp":"0x1"}}]"#,
                            );
                            let len = batch.len() as u64;
                            Response::builder()
                                .status(StatusCode::OK)
                                .header("content-type", "application/json")
                                .body(OpenBody {
                                    chunk: Some(batch),
                                    exact: Some(len),
                                })
                                .unwrap()
                        }
                    };
                    Ok::<_, Infallible>(resp)
                });
                let _ = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                    .serve_connection(TokioIo::new(stream), svc)
                    .await;
            });
        }
    }));

    let proxy = EthonyxHandle::start(&["--el-http-url", &upstream_url]).await;
    let h2 = h2c_client();

    // Confirm the proxy negotiated h2c upstream during its startup warm-up poll.
    let (hz_status, hz_body) = get(&h2, &proxy.url("/healthz")).await;
    assert_eq!(hz_status, StatusCode::OK);
    let hz: Value = serde_json::from_slice(&hz_body).unwrap();
    assert_eq!(hz["el"]["transport"], "h2c");

    // 1. Upstream H2 204/304 with END_STREAM=false and content-length must not re-inject
    // `content-length` or stream forbidden DATA frames downstream over HTTP/2.
    for (path, expected) in [
        ("/204-open", StatusCode::NO_CONTENT),
        ("/304-open", StatusCode::NOT_MODIFIED),
    ] {
        let req = Request::builder()
            .method(Method::GET)
            .uri(proxy.url(path))
            .body(Full::new(Bytes::new()))
            .unwrap();
        let resp = h2.request(req).await.unwrap();
        assert_eq!(resp.status(), expected, "{path}");
        assert!(
            resp.headers().get("content-length").is_none(),
            "{path} must not re-inject content-length: {:?}",
            resp.headers()
        );
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        assert!(body.is_empty(), "{path} must emit no body bytes");
    }

    // 2. HTTP/2 HEAD responses (local 404, 502 from HEAD 205, and proxied H2 HEAD with
    // END_STREAM=false) must emit no DATA frames (no RST_STREAM PROTOCOL_ERROR) while
    // keeping proxied response headers such as Content-Length intact.
    for (path, expected, expected_cl) in [
        ("/eth/v1/node/version", StatusCode::NOT_FOUND, None),
        ("/205-nonzero", StatusCode::BAD_GATEWAY, None),
        ("/head-open", StatusCode::OK, Some("42")),
    ] {
        let req = Request::builder()
            .method(Method::HEAD)
            .uri(proxy.url(path))
            .body(Full::new(Bytes::new()))
            .unwrap();
        let resp = h2.request(req).await.unwrap();
        assert_eq!(resp.status(), expected, "{path}");
        assert_eq!(
            resp.headers()
                .get("content-length")
                .and_then(|v| v.to_str().ok()),
            expected_cl,
            "{path}"
        );
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        assert!(body.is_empty(), "{path} must emit no body bytes");
    }

    proxy.shutdown().await;
}

#[tokio::test]
async fn http1_protocol_edges_transfer_encoding_205_expect_and_empty_post_cl() {
    let (upstream_url, _server) = spawn_raw_http1_upstream(|head| {
        let mut lines = head.lines();
        let req_line = lines.next().unwrap_or_default();
        let path = req_line.split_whitespace().nth(1).unwrap_or_default();
        let mut content_length = None;
        let mut has_custom_expect = false;
        for line in lines {
            if let Some((name, value)) = line.split_once(':') {
                if name.eq_ignore_ascii_case("content-length") {
                    content_length = Some(value.trim());
                } else if name.eq_ignore_ascii_case("expect")
                    && value.trim().eq_ignore_ascii_case("custom-expectation")
                {
                    has_custom_expect = true;
                }
            }
        }
        if has_custom_expect {
            return Cow::Borrowed(
                b"HTTP/1.1 417 Expectation Failed\r\nContent-Length: 10\r\nConnection: close\r\n\r\nfrom-upstr",
            );
        }
        match path {
            "/te-empty" => Cow::Borrowed(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: \r\n\r\nbody"),
            "/te-gzip" => Cow::Borrowed(
                b"HTTP/1.1 200 OK\r\nTransfer-Encoding: gzip, chunked\r\nConnection: close\r\n\r\n0\r\n\r\n",
            ),
            "/te-gzip-204" => Cow::Borrowed(
                b"HTTP/1.1 204 No Content\r\nTransfer-Encoding: gzip, chunked\r\nConnection: close\r\n\r\n",
            ),
            "/te-gzip-304" => Cow::Borrowed(
                b"HTTP/1.1 304 Not Modified\r\nTransfer-Encoding: gzip, chunked\r\nConnection: close\r\n\r\n",
            ),
            "/te-gzip-head" => Cow::Borrowed(
                b"HTTP/1.1 200 OK\r\nTransfer-Encoding: gzip, chunked\r\nConnection: close\r\n\r\n",
            ),
            "/te-double-chunked" => Cow::Borrowed(
                b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked, chunked\r\nConnection: close\r\n\r\n0\r\n\r\n",
            ),
            "/te-trailing-empty" => Cow::Borrowed(
                b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked,\r\nConnection: close\r\n\r\n5\r\nhello\r\n0\r\n\r\n",
            ),
            "/205-nonzero" => {
                Cow::Borrowed(b"HTTP/1.1 205 Reset Content\r\nContent-Length: 42\r\n\r\n")
            }
            "/205-zero" => Cow::Borrowed(b"HTTP/1.1 205 Reset Content\r\nContent-Length: 0\r\n\r\n"),
            "/205-chunked-empty" => Cow::Borrowed(
                b"HTTP/1.1 205 Reset Content\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n",
            ),
            "/205-chunked-nonempty" => Cow::Borrowed(
                b"HTTP/1.1 205 Reset Content\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n\r\n",
            ),
            "/get-205-nonempty" => Cow::Borrowed(
                b"HTTP/1.1 205 Reset Content\r\nContent-Length: 5\r\nConnection: close\r\n\r\nhello",
            ),
            "/echo-cl" => {
                let body = content_length.unwrap_or("none");
                Cow::Owned(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    )
                    .into_bytes(),
                )
            }
            _ => Cow::Borrowed(
                b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok",
            ),
        }
    })
    .await;

    let proxy = EthonyxHandle::start(&[
        "--trust-upstream",
        "--el-http-url",
        &upstream_url,
        "--cl-beacon-url",
        &upstream_url,
        "--mock-eth-syncing",
        "--mock-cl-subscriptions",
        "--mock-cl-node-health",
    ])
    .await;
    let c = client();

    // 1. Unsupported, empty, duplicate-chunked, or trailing-empty upstream Transfer-Encoding
    // and non-empty GET 205 responses (Content-Length or chunked) must all fail with 502.
    for path in [
        "/te-empty",
        "/te-gzip",
        "/te-gzip-204",
        "/te-double-chunked",
        "/te-trailing-empty",
        "/205-chunked-nonempty",
        "/get-205-nonempty",
    ] {
        let (status, body) = get(&c, &proxy.url(path)).await;
        assert_eq!(status, StatusCode::BAD_GATEWAY, "{path}");
        assert_eq!(body.as_ref(), b"bad gateway", "{path}");
    }
    // RFC 9110 §15.3.6: `HEAD 205 Reset Content` with non-zero `Content-Length` is
    // rejected as 502 (preserving HTTP/1.1 `HEAD` `Content-Length: 11` for "bad gateway")
    // while `Content-Length: 0` passes through; empty chunked `205` is drained cleanly.
    for (method, path, expected, expected_cl) in [
        (Method::HEAD, "/205-nonzero", StatusCode::BAD_GATEWAY, "11"),
        (Method::HEAD, "/205-zero", StatusCode::RESET_CONTENT, "0"),
        (
            Method::GET,
            "/205-chunked-empty",
            StatusCode::RESET_CONTENT,
            "0",
        ),
    ] {
        let req = Request::builder()
            .method(method.clone())
            .uri(proxy.url(path))
            .body(Full::new(Bytes::new()))
            .unwrap();
        let resp = c.request(req).await.unwrap();
        assert_eq!(resp.status(), expected, "{method} {path}");
        assert_eq!(
            resp.headers()
                .get("content-length")
                .and_then(|v| v.to_str().ok()),
            Some(expected_cl),
            "{method} {path}"
        );
        assert!(
            resp.into_body()
                .collect()
                .await
                .unwrap()
                .to_bytes()
                .is_empty(),
            "{method} {path} must carry zero body bytes"
        );
    }
    // RFC 9112 §6.1 / §6.3: bodyless 304 and non-204/205 HEAD responses may carry
    // representation Transfer-Encoding metadata (stripped on hop boundary without
    // synthesizing Content-Length: 0), while HEAD 204 + unsupported TE stays 502.
    for (method, path, expected_status) in [
        (Method::GET, "/te-gzip-304", StatusCode::NOT_MODIFIED),
        (Method::HEAD, "/te-gzip-head", StatusCode::OK),
        (Method::HEAD, "/te-gzip-204", StatusCode::BAD_GATEWAY),
    ] {
        let req = Request::builder()
            .method(method.clone())
            .uri(proxy.url(path))
            .body(Full::new(Bytes::new()))
            .unwrap();
        let resp = c.request(req).await.unwrap();
        assert_eq!(resp.status(), expected_status, "{method} {path}");
        assert!(
            !resp.headers().contains_key("transfer-encoding"),
            "{method} {path} must strip hop-by-hop transfer-encoding"
        );
        if expected_status != StatusCode::BAD_GATEWAY {
            assert!(
                !resp.headers().contains_key("content-length"),
                "{method} {path} must not fabricate Content-Length"
            );
        }
        assert!(
            resp.into_body()
                .collect()
                .await
                .unwrap()
                .to_bytes()
                .is_empty(),
            "{method} {path} must carry zero body bytes"
        );
    }

    // 2. Inbound CL request with unsupported Transfer-Encoding returns 501 Not Implemented.
    let text = raw_http1_exchange(
        proxy.port,
        b"POST /eth/v1/beacon/blocks HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nTransfer-Encoding: gzip, chunked\r\n\r\n0\r\n\r\n",
    )
    .await;
    assert!(
        text.starts_with("HTTP/1.1 501 Not Implemented"),
        "expected 501 on CL unsupported Transfer-Encoding, got: {text}"
    );

    // 3. Empty POST/PUT/PATCH restore Content-Length: 0 over HTTP/1.1 while GET omits it.
    for (method, expected_cl) in [
        (Method::POST, "0"),
        (Method::PUT, "0"),
        (Method::PATCH, "0"),
        (Method::GET, "none"),
    ] {
        let req = Request::builder()
            .method(method.clone())
            .uri(proxy.url("/echo-cl"))
            .body(Full::new(Bytes::new()))
            .unwrap();
        let resp = c.request(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "{method}");
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(body.as_ref(), expected_cl.as_bytes(), "{method}");
    }

    // 4. Non-`100-continue` Expect headers bypass all three local mocks and reach upstream.
    for (method, path, payload) in [
        (
            Method::POST,
            "/",
            &br#"{"jsonrpc":"2.0","method":"eth_syncing","params":[],"id":1}"#[..],
        ),
        (
            Method::POST,
            "/eth/v1/validator/prepare_beacon_proposer",
            &b"[]"[..],
        ),
        (Method::GET, "/eth/v1/node/health", &b""[..]),
    ] {
        let req = Request::builder()
            .method(method)
            .uri(proxy.url(path))
            .header("expect", "custom-expectation")
            .body(Full::new(Bytes::from_static(payload)))
            .unwrap();
        let resp = c.request(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::EXPECTATION_FAILED, "{path}");
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(body.as_ref(), b"from-upstr", "{path}");
    }

    proxy.shutdown().await;
}
