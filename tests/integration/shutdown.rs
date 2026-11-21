//! Graceful shutdown, drain-deadline propagation, pre-bind cancellation, and WebSocket `GoingAway` tests.

use super::*;
use ethonyx::run;
use tokio_tungstenite::{connect_async, tungstenite::Message};

#[tokio::test]
async fn shutdown_releases_upgraded_websocket_connections() {
    let upstream = ws_echo_upstream().await;
    for mocking in ["false", "true"] {
        let proxy = EthonyxHandle::start_trusted(&[
            "--mock-eth-syncing",
            mocking,
            "--el-ws-url",
            &upstream.url,
        ])
        .await;
        let (mut ws_h1, _) = connect_async(format!("ws://127.0.0.1:{}/", proxy.port))
            .await
            .unwrap();
        let (mut ws_h2, _h2) = proxy.connect_h2_ws("/").await;
        ws_h1
            .send(Message::Text("established".into()))
            .await
            .unwrap();
        assert_eq!(
            ws_h1.next().await.unwrap().unwrap(),
            Message::Text("established".into())
        );
        ws_h2
            .send(Message::Text("established-h2".into()))
            .await
            .unwrap();
        assert_eq!(
            ws_h2.next().await.unwrap().unwrap(),
            Message::Text("established-h2".into())
        );
        // Zero-grace shutdown must exit promptly without blocking on WS_CLOSE_GRACE (1s).
        tokio::time::timeout(Duration::from_millis(500), proxy.shutdown())
            .await
            .expect("zero-grace shutdown must not block on WS_CLOSE_GRACE");
        for closed in [
            tokio::time::timeout(Duration::from_secs(1), ws_h1.next())
                .await
                .expect("an upgraded H1 socket must not outlive zero-grace shutdown"),
            tokio::time::timeout(Duration::from_secs(1), ws_h2.next())
                .await
                .expect("an upgraded H2 stream must not outlive zero-grace shutdown"),
        ] {
            assert!(matches!(
                closed,
                None | Some(Err(_)) | Some(Ok(Message::Close(_)))
            ));
        }
    }
}

#[tokio::test]
async fn shutdown_waits_for_websocket_peer_close() {
    let upstream = ws_echo_upstream().await;
    let mut proxy =
        EthonyxHandle::start_trusted_ws_with_grace(&upstream.url, Duration::from_secs(3)).await;
    let (mut ws, _) = connect_async(format!("ws://127.0.0.1:{}/", proxy.port))
        .await
        .unwrap();
    ws.send(Message::Text("established".into())).await.unwrap();
    let _ = ws.next().await.unwrap().unwrap();
    proxy.trigger_shutdown();
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
    let proxy =
        EthonyxHandle::start_trusted_ws_with_grace(&upstream.url, Duration::from_millis(250)).await;
    let (mut ws, _) = connect_async(format!("ws://127.0.0.1:{}/", proxy.port))
        .await
        .unwrap();
    ws.send(Message::Text("established".into())).await.unwrap();
    let _ = ws.next().await.unwrap().unwrap();
    let start = tokio::time::Instant::now();
    tokio::time::timeout(Duration::from_secs(1), proxy.shutdown())
        .await
        .expect("unresponsive peer must be bounded by shutdown_grace");
    assert!(
        start.elapsed() >= Duration::from_millis(200),
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
    let proxy = EthonyxHandle::start_trusted(&["--cl-beacon-url", &upstream]).await;
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

#[tokio::test(flavor = "current_thread")]
async fn shutdown_already_ready_does_not_wait_for_a_new_watch_event() {
    let make_cfg = || {
        Config::parse_from([
            "ethonyx",
            "--listen",
            "127.0.0.1:0",
            "--el-http-url",
            "http://127.0.0.1:8545",
            "--trust-upstream",
            "--shutdown-grace",
            "0",
        ])
    };
    let bound = Prepared::new(make_cfg())
        .await
        .unwrap()
        .bind()
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), bound.serve(async {}))
        .await
        .expect("an already completed shutdown must stop Bound::serve and the accept loop")
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), run(make_cfg(), async {}))
        .await
        .expect("an already completed shutdown must cancel Prepared::run before binding")
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
    let mut proxy = EthonyxHandle::start_trusted_ws_with_grace(&url, grace).await;
    let H2Client {
        mut sender,
        _driver,
    } = proxy.open_h2().await;
    let req = ext_connect(proxy.url("/"));
    let request = tokio::spawn(async move { sender.send_request(req).await });
    tokio::time::timeout(Duration::from_secs(2), dial_rx)
        .await
        .unwrap()
        .unwrap();
    let started = tokio::time::Instant::now();
    proxy.trigger_shutdown();
    let mut downstream = None;
    if complete_handshake {
        // Wait 400ms past shutdown before releasing the handshake: a regression that grants
        // a fresh 800ms grace period finishes around 1200ms and fails the `grace + 250ms` bound.
        tokio::time::sleep(Duration::from_millis(400)).await;
        release_tx.send(()).unwrap();
        let response = tokio::time::timeout(Duration::from_secs(1), request)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        downstream = Some(
            tokio::time::timeout(Duration::from_secs(1), hyper::upgrade::on(response))
                .await
                .expect("client upgrade must resolve promptly")
                .unwrap(),
        );
    } else {
        drop(release_tx);
        // The shutdown barrier must not depend on the client dropping this request.
        drop(request);
    }
    tokio::time::timeout(grace + Duration::from_secs(1), proxy.shutdown())
        .await
        .expect("the shared deadline must also cancel pending HTTP/2 request tasks");
    let slack = if complete_handshake {
        Duration::from_millis(250)
    } else {
        Duration::from_millis(500)
    };
    assert!(
        started.elapsed() < grace + slack,
        "shutdown took {:?} (expected < {:?})",
        started.elapsed(),
        grace + slack
    );
    tokio::time::timeout(Duration::from_millis(200), closed_rx)
        .await
        .expect("upstream must be released when run returns, not after a new grace period")
        .unwrap();
    server.await.unwrap();
    drop(downstream);
}

#[tokio::test]
async fn shutdown_late_websocket_relay_keeps_the_original_deadline() {
    shutdown_during_h2_ws_handshake(true, Duration::from_millis(800)).await;
}

#[tokio::test]
async fn shutdown_cancels_pending_http2_websocket_handshakes() {
    for grace in [Duration::ZERO, Duration::from_millis(350)] {
        shutdown_during_h2_ws_handshake(false, grace).await;
    }
}

#[tokio::test]
async fn shutdown_during_initial_health_probe_exits_promptly_without_binding_listener() {
    // Hanging EL + CL TCP listener that accepts and never replies for 30s, plus an
    // already-occupied `--listen` port so `Ok(())` proves `TcpListener::bind` never ran.
    let occupied = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let occupied_addr = occupied.local_addr().unwrap().to_string();
    let silent = SilentPort::new().await;
    let hang_url = silent.http_url();

    let cfg = ethonyx::Config::parse_from([
        "ethonyx",
        "--listen",
        &occupied_addr,
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

    tokio::time::timeout(Duration::from_secs(2), silent.wait_accepted())
        .await
        .expect("initial health probe must dial the silent upstream");
    let _ = stop_tx.send(());

    let res = tokio::time::timeout(Duration::from_secs(1), run_task)
        .await
        .expect("shutdown during initial health probe must return within 1s")
        .unwrap();
    assert!(res.is_ok());
    drop(occupied);
}

#[tokio::test]
async fn shutdown_sends_going_away_close_frame_to_active_h1_and_h2_websockets() {
    use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

    let upstream = ws_echo_upstream().await;
    // `shutdown_close_signal` reserves `min(remaining / 8, WS_CLOSE_GRACE)` for the Close
    // handshake, so 800ms grace leaves a 700ms live drain window followed by a full 100ms
    // Close window (resilient to 15.6ms Windows timer ticks and loaded CI runners).
    let mut proxy =
        EthonyxHandle::start_trusted_ws_with_grace(&upstream.url, Duration::from_millis(800)).await;

    // 1. HTTP/1.1 Upgrade WebSocket subscriber.
    let (mut ws_h1, _) = connect_async(format!("ws://127.0.0.1:{}/", proxy.port))
        .await
        .unwrap();

    // 2. HTTP/2 Extended CONNECT WebSocket subscriber.
    let (mut ws_h2, _h2) = proxy.connect_h2_ws("/").await;

    let started = tokio::time::Instant::now();
    proxy.trigger_shutdown();

    // Both H1 and H2 relays remain fully operational during the drain window.
    tokio::time::sleep(Duration::from_millis(50)).await;
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
        started.elapsed() >= Duration::from_millis(560)
            && started.elapsed() < Duration::from_millis(2000),
        "active relays should close after the ~700ms live drain window (elapsed = {:?})",
        started.elapsed()
    );
}

#[tokio::test]
async fn shutdown_waits_for_an_in_flight_http_request_without_websocket() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    // No `--el-ws-url`: the connection holds no drain token of its own, only the
    // server-wide `ServerBuilders` token, so plain HTTP alone delays `serve()`.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream = format!("http://{}", listener.local_addr().unwrap());
    let (seen_tx, seen_rx) = oneshot::channel::<()>();
    let (release_tx, release_rx) = oneshot::channel::<()>();
    let mut server = AbortOnDrop(tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut head = Vec::new();
        let mut buffer = [0; 1024];
        while !head.windows(4).any(|part| part == b"\r\n\r\n") {
            let count = socket.read(&mut buffer).await.unwrap();
            assert_ne!(count, 0);
            head.extend_from_slice(&buffer[..count]);
        }
        seen_tx.send(()).unwrap();
        release_rx.await.unwrap();
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
            .await
            .unwrap();
        let _ = socket.shutdown().await;
    }));
    let mut proxy =
        EthonyxHandle::start_tuned(&["--trust-upstream", "--el-http-url", &upstream], |cfg| {
            cfg.shutdown_grace = Duration::from_secs(3);
        })
        .await;
    let url = proxy.url("/");
    let request = tokio::spawn(async move { get(&client(), &url).await });
    tokio::time::timeout(Duration::from_secs(5), seen_rx)
        .await
        .expect("proxied request must reach the upstream")
        .unwrap();
    proxy.trigger_shutdown();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !proxy.task.as_ref().unwrap().is_finished(),
        "an in-flight request must remain tracked while draining"
    );
    release_tx.send(()).unwrap();
    let (status, body) = tokio::time::timeout(Duration::from_secs(5), request)
        .await
        .expect("released request must finish")
        .unwrap();
    assert_eq!((status, body.as_ref()), (StatusCode::OK, &b"ok"[..]));
    tokio::time::timeout(Duration::from_secs(2), proxy.shutdown())
        .await
        .expect("the finished request should release the drain barrier before its deadline");
    tokio::time::timeout(Duration::from_secs(2), &mut server.0)
        .await
        .expect("upstream task should exit after replying")
        .unwrap();
}
