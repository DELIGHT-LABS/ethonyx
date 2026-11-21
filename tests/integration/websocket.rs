//! WebSocket HTTP/1.1 `Upgrade` and HTTP/2 Extended `CONNECT` handshake, framing, backpressure, and churn tests.

use super::*;
use serde_json::json;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Error as WsError, Message},
};

#[tokio::test]
async fn ws_handshake_fails_with_502_when_upstream_down() {
    // DeadPort accepts TCP and immediately resets with SO_LINGER(0). ethonyx dials
    // upstream *before* completing the client handshake, so the client gets a 502 on
    // the handshake rather than a 101 followed by an immediate abnormal close.
    let dead = DeadPort::new();
    let ethonyx = EthonyxHandle::start_trusted_ws(&dead.ws_url()).await;

    let url = format!("ws://127.0.0.1:{}/", ethonyx.port);
    match connect_async(&url).await {
        Err(WsError::Http(resp)) => assert_eq!(resp.status(), StatusCode::BAD_GATEWAY),
        other => panic!("expected a 502 handshake error, got {other:?}"),
    }

    ethonyx.shutdown().await;
}

#[tokio::test]
async fn http2_extended_connect_ws_502_when_upstream_down() {
    // The h2 path dials the upstream first too: a dead upstream surfaces as a 502
    // on the CONNECT response, not an accepted-then-dropped tunnel.
    let dead = DeadPort::new();
    let ethonyx = EthonyxHandle::start_trusted_ws(&dead.ws_url()).await;
    let mut h2 = ethonyx.open_h2().await;

    let req = ext_connect(format!("http://127.0.0.1:{}/", ethonyx.port));

    let resp = h2.send_request(req).await.expect("extended CONNECT");
    assert_eq!(
        resp.status(),
        StatusCode::BAD_GATEWAY,
        "dead upstream → 502"
    );

    drop(h2);
    ethonyx.shutdown().await;
}

#[tokio::test]
async fn ws_upstream_that_never_answers_is_a_502_after_proxy_timeout() {
    // Accepts TCP and stays silent so only `--proxy-timeout` can end the upstream handshake;
    // `DeadPort` resets immediately and never reaches the dial-timeout branch.
    let silent = SilentPort::new().await;
    let ws_url = silent.ws_url();
    let proxy = EthonyxHandle::start_trusted_tuned(&["--el-ws-url", &ws_url], |cfg| {
        cfg.proxy_timeout = Duration::from_millis(200);
    })
    .await;
    let started = tokio::time::Instant::now();
    let handshake = connect_async(format!("ws://127.0.0.1:{}/", proxy.port));
    match tokio::time::timeout(Duration::from_secs(5), handshake)
        .await
        .expect("--proxy-timeout must end the upstream dial")
    {
        Err(WsError::Http(resp)) => {
            assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);
            assert_eq!(
                resp.body().as_deref(),
                Some(&b"upstream ws connect timed out"[..])
            );
        }
        other => panic!("expected a 502 handshake error, got {other:?}"),
    }
    assert!(
        started.elapsed() >= Duration::from_millis(200),
        "the 502 must come from --proxy-timeout (took {:?})",
        started.elapsed()
    );
    proxy.shutdown().await;
}

#[tokio::test]
async fn http2_extended_connect_ws_bridges_to_upstream() {
    // RFC 8441 Extended CONNECT: an h2 client opens a WebSocket via
    // `:method=CONNECT, :protocol=websocket`. ethonyx terminates the h2 stream and
    // bridges the RFC 6455 frames to the upstream h1 WebSocket (h2 ws → h1 ws).
    // Driven by a hand-rolled h2 client because tokio/hyper-tungstenite are h1-only.
    let ws_upstream = ws_echo_upstream().await;
    let ethonyx = EthonyxHandle::start_trusted_ws(&ws_upstream.url).await;
    let mut h2 = ethonyx.open_h2().await;

    let mut sockets = Vec::new();
    for path in ["/", "/healthz", "/readyz", "/livez", "/metrics"] {
        let req = ext_connect(format!("http://127.0.0.1:{}{path}", ethonyx.port));

        let resp = h2.send_request(req).await.expect("extended CONNECT");
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "extended CONNECT should be 200"
        );
        assert_eq!(resp.version(), Version::HTTP_2);

        let mut ws = upgrade_ws(resp).await;
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

    let ethonyx = EthonyxHandle::start_trusted_ws(&ws_upstream).await;

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

    // 2. Scoped upstream path without trailing slash (`/v2/testkey` + inbound `/`)
    //    must preserve `/v2/testkey` without appending `/` and bridge frames cleanly.
    let (uri_tx, uri_rx) = oneshot::channel::<String>();
    let scoped_server = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let scoped_port = scoped_server.local_addr().unwrap().port();
    tokio::spawn(async move {
        if let Ok((stream, _)) = scoped_server.accept().await {
            let mut tx = Some(uri_tx);
            #[allow(clippy::result_large_err)]
            let ws_stream = tokio_tungstenite::accept_hdr_async(
                stream,
                move |req: &tokio_tungstenite::tungstenite::handshake::server::Request, res| {
                    if let Some(tx) = tx.take() {
                        let _ = tx.send(req.uri().to_string());
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

    let scoped_proxy =
        EthonyxHandle::start_trusted_ws(&format!("ws://127.0.0.1:{scoped_port}/v2/testkey")).await;
    let (mut scoped_ws, _) = connect_async(scoped_proxy.url("/").replace("http://", "ws://"))
        .await
        .unwrap();
    assert_eq!(uri_rx.await.unwrap(), "/v2/testkey");
    scoped_ws.send(Message::Text("echo".into())).await.unwrap();
    assert_eq!(
        scoped_ws.next().await.unwrap().unwrap(),
        Message::Text("echo".into())
    );
    scoped_proxy.shutdown().await;
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
        let proxy = EthonyxHandle::start_trusted(&[
            "--mock-eth-syncing",
            mock_arg,
            "--el-ws-url",
            &upstream.url,
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
        ws.send(Message::Ping(Bytes::from_static(b"ping")))
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
    let proxy = EthonyxHandle::start_trusted_ws(&upstream.url).await;
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
async fn websocket_rejects_invalid_handshakes_on_both_hops() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let dead = DeadPort::new();
    let dead_url = dead.ws_url();
    let proxy = EthonyxHandle::start_trusted_ws(&dead_url).await;
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
    let proxy = EthonyxHandle::start_trusted_ws(&url).await;
    let err = connect_async(format!("ws://127.0.0.1:{}/", proxy.port))
        .await
        .unwrap_err();
    assert!(matches!(err, WsError::Http(response) if response.status() == StatusCode::BAD_GATEWAY));
    server.await.unwrap();
    proxy.shutdown().await;
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
        let proxy = EthonyxHandle::start_trusted(&[
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
        let proxy = EthonyxHandle::start_trusted(&[
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
    let proxy = EthonyxHandle::start_trusted_ws(&upstream).await;
    let mut h2 = proxy.open_h2().await;
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
        let response = tokio::time::timeout(Duration::from_secs(2), h2.send_request(req))
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
}

async fn ws_handshake_response(
    proxy: &EthonyxHandle,
    h2: bool,
    headers: &[(&str, &str)],
) -> (Response<Incoming>, AbortOnDrop) {
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
        let H2Client {
            mut sender,
            _driver,
        } = proxy.open_h2().await;
        (sender.send_request(req).await.unwrap(), _driver)
    } else {
        let socket = tokio::net::TcpStream::connect(("127.0.0.1", proxy.port))
            .await
            .unwrap();
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
        (sender.send_request(req).await.unwrap(), AbortOnDrop(driver))
    }
}

#[tokio::test]
async fn websocket_negotiates_split_protocols_without_forwarding_extensions() {
    use tokio_tungstenite::tungstenite::handshake::server::{
        Request as WsRequest, Response as WsResponse,
    };
    let (plan_tx, mut plan_rx) = tokio::sync::mpsc::unbounded_channel::<(String, &'static str)>();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        while let Some((expected, selected)) = plan_rx.recv().await {
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
                        .insert("sec-websocket-protocol", selected.parse().unwrap());
                    Ok(response)
                },
            )
            .await
            .unwrap();
            let message = ws.next().await.unwrap().unwrap();
            ws.send(message).await.unwrap();
            let _ = ws.next().await;
        }
    });
    let proxy = EthonyxHandle::start_trusted_ws(&url).await;

    let large_line = (0..1024)
        .rev()
        .map(|i| format!("p{i:04}"))
        .collect::<Vec<_>>()
        .join(", ");
    // Covers both the single-token zero-allocation fast path (`" jsonrpc\t "` -> `"jsonrpc"`)
    // and multi-line split / 1024-token slow paths across HTTP/1.1 and HTTP/2 Extended CONNECT.
    for h2 in [false, true] {
        for (offer_lines, expected, selected) in [
            (vec![" jsonrpc\t "], "jsonrpc".to_owned(), "jsonrpc"),
            (
                vec!["first", " Second\t, second, Third "],
                "first, Second, second, Third".to_owned(),
                "second",
            ),
            (
                vec![large_line.as_str(), " Second\t, second, Third "],
                format!("{large_line}, Second, second, Third"),
                "second",
            ),
        ] {
            plan_tx.send((expected, selected)).unwrap();
            let mut headers: Vec<(&str, &str)> = offer_lines
                .into_iter()
                .map(|v| ("sec-websocket-protocol", v))
                .collect();
            headers.push((
                "sec-websocket-extensions",
                "permessage-deflate; client_max_window_bits",
            ));
            headers.push(("sec-websocket-extensions", "unknown-extension"));

            let (response, _driver) = ws_handshake_response(&proxy, h2, &headers).await;
            assert_eq!(
                response.status(),
                if h2 {
                    StatusCode::OK
                } else {
                    StatusCode::SWITCHING_PROTOCOLS
                },
                "h2={h2}, selected={selected}"
            );
            assert_eq!(
                response.headers()["sec-websocket-protocol"],
                selected,
                "h2={h2}, selected={selected}"
            );
            assert!(
                !response.headers().contains_key("sec-websocket-extensions"),
                "h2={h2}, selected={selected}"
            );
            let mut ws = upgrade_ws(response).await;
            ws.send(Message::Text("uncompressed round trip".into()))
                .await
                .unwrap();
            assert_eq!(
                ws.next().await.unwrap().unwrap(),
                Message::Text("uncompressed round trip".into()),
                "h2={h2}, selected={selected}"
            );
            ws.close(None).await.unwrap();
        }
    }
    drop(plan_tx);
    proxy.shutdown().await;
    server.await.unwrap();
}

#[tokio::test]
async fn websocket_rejects_split_protocol_duplicates_before_dialing() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let proxy = EthonyxHandle::start_trusted_ws(&url).await;
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
            let (response, _driver) = tokio::time::timeout(
                Duration::from_secs(10),
                ws_handshake_response(&proxy, h2, &headers),
            )
            .await
            .expect("invalid subprotocol offers must be rejected promptly");
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "h2={h2}");
            drop(response);
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
    let (sel_tx, mut sel_rx) = tokio::sync::mpsc::unbounded_channel::<&'static str>();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        while let Some(selection) = sel_rx.recv().await {
            let (socket, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_hdr_async(
                socket,
                #[allow(clippy::result_large_err)] // Tungstenite fixes the callback error type.
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
        }
    });
    let proxy = EthonyxHandle::start_trusted_ws(&url).await;

    for h2 in [false, true] {
        for (selection, expected_body) in [
            ("extension", &b"invalid upstream websocket negotiation"[..]),
            ("duplicate", b"invalid upstream websocket negotiation"),
            ("unoffered", b"upstream ws connect failed"),
            ("wrong-case", b"upstream ws connect failed"),
            ("list", b"upstream ws connect failed"),
        ] {
            sel_tx.send(selection).unwrap();
            let (response, _driver) = ws_handshake_response(
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
            let body = response.into_body().collect().await.unwrap().to_bytes();
            assert_eq!(&body[..], expected_body, "{selection}, h2={h2}");
        }
    }
    drop(sel_tx);
    proxy.shutdown().await;
    tokio::time::timeout(Duration::from_secs(1), server)
        .await
        .unwrap()
        .unwrap();
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
    let proxy = EthonyxHandle::start_trusted_ws(&url).await;
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
    let _driver = AbortOnDrop(tokio::spawn(async move {
        let _ = conn.with_upgrades().await;
    }));
    let response = sender.send_request(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
    assert_eq!(response.headers()["sec-websocket-protocol"], "jsonrpc");
    proxy.shutdown().await;
    server.await.unwrap();
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
        let proxy =
            EthonyxHandle::start_trusted(&["--mock-eth-syncing", mocking, "--el-ws-url", &url])
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

    let payload_len = 768 * 1024; // 768 KiB (12x H2 default 65_535-byte window & 12x 64 KiB cap)
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

    let proxy = EthonyxHandle::start_trusted_tuned(&["--el-ws-url", &ws_url], |cfg| {
        cfg.proxy_timeout = Duration::from_secs(1);
    })
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
    let h2_conn = AbortOnDrop(tokio::spawn(async move {
        let _ = conn.await;
    }));

    let req = ext_connect(format!("http://127.0.0.1:{}/", proxy.port));

    let resp = sender.send_request(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let upgraded = hyper::upgrade::on(resp).await.unwrap();

    // Throttle reads to 32 KiB every 80ms (~400 KiB/s): 768 KiB takes ~1.8s. The proxy's
    // writes back up from the first 64 KiB window, so it stays write-blocked for ~1.7s, well
    // past the 1s proxy_timeout: a stall timer that did not restart on progress would cut the
    // frame. The relay only gives up on a reader that makes no progress for the full second,
    // so a loaded runner that pauses this task for a few hundred milliseconds does not fail it.
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
        started.elapsed() > Duration::from_millis(1600),
        "transfer must span well past proxy_timeout 1s (took {:?})",
        started.elapsed()
    );

    ws.close(None).await.unwrap();
    while let Some(Ok(msg)) = ws.next().await {
        if msg.is_close() {
            break;
        }
    }
    ws_server.await.unwrap();
    drop((ws, sender, h2_conn));
    proxy.shutdown().await;
}
