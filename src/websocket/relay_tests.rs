use super::super::ws_config;
use super::*;
use tokio_tungstenite::tungstenite::protocol::Role;

fn spawn_bridge<C, U>(
    proxy_client: WebSocketStream<C>,
    proxy_upstream: WebSocketStream<U>,
    mock_enabled: bool,
    io_timeout: Duration,
) -> tokio::task::JoinHandle<()>
where
    C: RelayIo + Send + 'static,
    U: RelayIo + Send + 'static,
{
    tokio::spawn(async move {
        let (_tx, lifecycle, _drain) = crate::lifecycle::channel();
        let shutdown = lifecycle.shutdown();
        bridge_ws(
            proxy_client,
            proxy_upstream,
            mock_enabled,
            io_timeout,
            &shutdown,
        )
        .await;
    })
}

async fn duplex_ws_endpoints(
    c2p_cap: usize,
    p2u_cap: usize,
    peer_cfg: Option<tokio_tungstenite::tungstenite::protocol::WebSocketConfig>,
) -> (
    WebSocketStream<tokio::io::DuplexStream>,
    WebSocketStream<WriteProgressIo<tokio::io::DuplexStream>>,
    WebSocketStream<WriteProgressIo<tokio::io::DuplexStream>>,
    WebSocketStream<tokio::io::DuplexStream>,
) {
    let (client_io, proxy_client_io) = tokio::io::duplex(c2p_cap);
    let (proxy_upstream_io, upstream_io) = tokio::io::duplex(p2u_cap);
    let client = WebSocketStream::from_raw_socket(client_io, Role::Client, peer_cfg).await;
    let proxy_client = WebSocketStream::from_raw_socket(
        WriteProgressIo::new(proxy_client_io),
        Role::Server,
        Some(ws_config()),
    )
    .await;
    let proxy_upstream = WebSocketStream::from_raw_socket(
        WriteProgressIo::new(proxy_upstream_io),
        Role::Client,
        Some(ws_config()),
    )
    .await;
    let upstream = WebSocketStream::from_raw_socket(upstream_io, Role::Server, peer_cfg).await;
    (client, proxy_client, proxy_upstream, upstream)
}

async fn duplex_bridge_with_shutdown(
    c2p_cap: usize,
    p2u_cap: usize,
    mock_enabled: bool,
    io_timeout: Duration,
) -> (
    WebSocketStream<tokio::io::DuplexStream>,
    WebSocketStream<tokio::io::DuplexStream>,
    crate::lifecycle::ShutdownTx,
    tokio::task::JoinHandle<()>,
) {
    let (client, proxy_client, proxy_upstream, upstream) =
        duplex_ws_endpoints(c2p_cap, p2u_cap, None).await;
    let (tx, lifecycle, _drain) = crate::lifecycle::channel();
    let rx = lifecycle.shutdown();
    let relay = tokio::spawn(async move {
        bridge_ws(proxy_client, proxy_upstream, mock_enabled, io_timeout, &rx).await;
    });
    (client, upstream, tx, relay)
}

async fn duplex_bridge(
    c2p_cap: usize,
    p2u_cap: usize,
    mock_enabled: bool,
    io_timeout: Duration,
) -> (
    WebSocketStream<tokio::io::DuplexStream>,
    WebSocketStream<tokio::io::DuplexStream>,
    tokio::task::JoinHandle<()>,
) {
    let (client, proxy_client, proxy_upstream, upstream) =
        duplex_ws_endpoints(c2p_cap, p2u_cap, None).await;
    let relay = spawn_bridge(proxy_client, proxy_upstream, mock_enabled, io_timeout);
    (client, upstream, relay)
}

/// Bridge with raw client byte-stream access (for Slowloris trickle reads or malformed wire frames).
async fn raw_client_bridge(
    c2p_cap: usize,
    p2u_cap: usize,
    io_timeout: Duration,
) -> (
    tokio::io::DuplexStream,
    WebSocketStream<tokio::io::DuplexStream>,
    tokio::task::JoinHandle<()>,
) {
    let (raw_client, proxy_client_io) = tokio::io::duplex(c2p_cap);
    let (proxy_upstream_io, upstream_io) = tokio::io::duplex(p2u_cap);
    let proxy_client = WebSocketStream::from_raw_socket(
        WriteProgressIo::new(proxy_client_io),
        Role::Server,
        Some(ws_config()),
    )
    .await;
    let proxy_upstream = WebSocketStream::from_raw_socket(
        WriteProgressIo::new(proxy_upstream_io),
        Role::Client,
        Some(ws_config()),
    )
    .await;
    let upstream = WebSocketStream::from_raw_socket(upstream_io, Role::Server, None).await;
    let relay = spawn_bridge(proxy_client, proxy_upstream, false, io_timeout);
    (raw_client, upstream, relay)
}

/// Bridge whose client can write freely into the proxy while its read half is stalled
/// (simulating a zero-window peer that never drains outbound frames from the proxy).
type SplitClientWs = WebSocketStream<
    tokio::io::Join<
        tokio::io::ReadHalf<tokio::io::DuplexStream>,
        tokio::io::WriteHalf<tokio::io::DuplexStream>,
    >,
>;

struct StalledClientGuard {
    _stalled_client: tokio::io::DuplexStream,
    _keep_open_tx: tokio::io::DuplexStream,
}

async fn stalled_client_bridge(
    p2u_cap: usize,
    io_timeout: Duration,
) -> (
    SplitClientWs,
    WebSocketStream<tokio::io::DuplexStream>,
    StalledClientGuard,
    tokio::task::JoinHandle<()>,
) {
    let (client_to_proxy, proxy_from_client) = tokio::io::duplex(64 * 1024);
    let (_, client_tx) = tokio::io::split(client_to_proxy);
    let (proxy_rx, _) = tokio::io::split(proxy_from_client);

    let (proxy_to_client, _stalled_client) = tokio::io::duplex(64);
    let (_, proxy_tx) = tokio::io::split(proxy_to_client);

    let (_keep_open_tx, client_idle) = tokio::io::duplex(64);
    let (client_rx, _) = tokio::io::split(client_idle);

    let client_ws =
        WebSocketStream::from_raw_socket(tokio::io::join(client_rx, client_tx), Role::Client, None)
            .await;
    let proxy_client = WebSocketStream::from_raw_socket(
        WriteProgressIo::new(tokio::io::join(proxy_rx, proxy_tx)),
        Role::Server,
        Some(ws_config()),
    )
    .await;
    let (proxy_upstream_io, upstream_io) = tokio::io::duplex(p2u_cap);
    let proxy_upstream = WebSocketStream::from_raw_socket(
        WriteProgressIo::new(proxy_upstream_io),
        Role::Client,
        Some(ws_config()),
    )
    .await;
    let upstream_ws = WebSocketStream::from_raw_socket(upstream_io, Role::Server, None).await;
    let relay = spawn_bridge(proxy_client, proxy_upstream, false, io_timeout);
    (
        client_ws,
        upstream_ws,
        StalledClientGuard {
            _stalled_client,
            _keep_open_tx,
        },
        relay,
    )
}

#[tokio::test]
async fn single_loop_bridge_flushes_ping_locally_without_forwarding_or_duplicating_pong() {
    let (mut client, mut upstream, relay) =
        duplex_bridge(16 * 1024, 16 * 1024, true, Duration::from_secs(5)).await;

    // 1. Client Ping while upstream is completely idle (not even reading!) must
    // immediately yield 1 Pong to the client and never forward Ping to upstream.
    client
        .send(Message::Ping(Bytes::from_static(b"hb-1")))
        .await
        .unwrap();
    let pong = tokio::time::timeout(Duration::from_secs(1), client.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(pong, Message::Pong(Bytes::from_static(b"hb-1")));

    // 2. Unsolicited Pong from client is absorbed locally.
    client
        .send(Message::Pong(Bytes::from_static(b"unsolicited")))
        .await
        .unwrap();

    // 3. Mocked eth_syncing replies locally without touching upstream.
    client
        .send(Message::Text(
            r#"{"jsonrpc":"2.0","method":"eth_syncing","id":9}"#.into(),
        ))
        .await
        .unwrap();
    let mock_reply = client.next().await.unwrap().unwrap();
    assert_eq!(
        mock_reply,
        Message::Text(r#"{"jsonrpc":"2.0","id":9,"result":false}"#.into())
    );

    // 4. Non-eth_syncing message is the VERY FIRST frame upstream sees!
    client
        .send(Message::Text(
            r#"{"jsonrpc":"2.0","method":"eth_blockNumber","id":1}"#.into(),
        ))
        .await
        .unwrap();
    let seen_by_upstream = upstream.next().await.unwrap().unwrap();
    assert_eq!(
        seen_by_upstream,
        Message::Text(r#"{"jsonrpc":"2.0","method":"eth_blockNumber","id":1}"#.into())
    );
    upstream
        .send(Message::Text(
            r#"{"jsonrpc":"2.0","id":1,"result":"0x2a"}"#.into(),
        ))
        .await
        .unwrap();
    let forwarded_reply = client.next().await.unwrap().unwrap();
    assert_eq!(
        forwarded_reply,
        Message::Text(r#"{"jsonrpc":"2.0","id":1,"result":"0x2a"}"#.into())
    );

    // 5. Orderly close completes with zero extra Pong frames.
    client.close(None).await.unwrap();
    let close_reply = client.next().await.unwrap().unwrap();
    assert!(matches!(close_reply, Message::Close(_)), "{close_reply:?}");
    assert!(matches!(
        upstream.next().await,
        Some(Ok(Message::Close(_))) | None
    ));
    // Poll once more (as a server read loop does) so Tungstenite flushes its Close-ack.
    assert!(upstream.next().await.is_none());
    relay.await.unwrap();
}

#[tokio::test]
async fn pipelined_and_bidirectional_bursts_complete_without_deadlock() {
    for mock_enabled in [false, true] {
        // Scenario A: Client pipelines 64 x 16 KiB (1 MiB) requests before reading a
        // single reply over 64 KiB duplexes while upstream replies 2 KiB per request
        // (128 KiB total > 64 KiB c2p duplex, saturating the outbound client sink).
        let (mut client, mut upstream, relay) =
            duplex_bridge(64 * 1024, 64 * 1024, mock_enabled, Duration::from_secs(5)).await;
        let upstream_task = tokio::spawn(async move {
            let reply_pad = Utf8Bytes::from("r".repeat(2048));
            while let Some(Ok(msg)) = upstream.next().await {
                if msg.is_close() {
                    break;
                }
                upstream
                    .send(Message::Text(reply_pad.clone()))
                    .await
                    .unwrap();
            }
        });

        let req_payload = Utf8Bytes::from(format!(
            r#"{{"jsonrpc":"2.0","method":"eth_call","params":["{}"],"id":1}}"#,
            "a".repeat(16 * 1024)
        ));
        tokio::time::timeout(Duration::from_secs(5), async {
            for i in 0..64 {
                if mock_enabled && i == 40 {
                    // Interleave a local eth_syncing reply while client sink is already full!
                    client
                        .send(Message::Text(
                            r#"{"jsonrpc":"2.0","method":"eth_syncing","id":999}"#.into(),
                        ))
                        .await
                        .unwrap();
                }
                client
                    .send(Message::Text(req_payload.clone()))
                    .await
                    .unwrap();
            }
            let expected_replies = 64 + usize::from(mock_enabled);
            for _ in 0..expected_replies {
                let msg = client.next().await.unwrap().unwrap();
                assert!(matches!(msg, Message::Text(_)));
            }
            client.close(None).await.unwrap();
            while let Some(Ok(msg)) = client.next().await {
                if msg.is_close() {
                    break;
                }
            }
        })
        .await
        .expect("pipelined burst must not deadlock");

        upstream_task.await.unwrap();
        relay.await.unwrap();

        // Scenario B: Simultaneous 96 KiB bursts (> 64 KiB duplex) from BOTH client
        // and upstream before either side begins reading.
        let (mut client, mut upstream, relay) =
            duplex_bridge(64 * 1024, 64 * 1024, mock_enabled, Duration::from_secs(5)).await;
        let chunk = Bytes::from(vec![b'x'; 16 * 1024]);
        let up_chunk = chunk.clone();
        let upstream_burst = tokio::spawn(async move {
            for _ in 0..6 {
                upstream
                    .send(Message::Binary(up_chunk.clone()))
                    .await
                    .unwrap();
            }
            for _ in 0..6 {
                let msg = upstream.next().await.unwrap().unwrap();
                assert!(matches!(msg, Message::Binary(b) if b.len() == 16 * 1024));
            }
        });

        tokio::time::timeout(Duration::from_secs(5), async {
            for _ in 0..6 {
                client.send(Message::Binary(chunk.clone())).await.unwrap();
            }
            for _ in 0..6 {
                let msg = client.next().await.unwrap().unwrap();
                assert!(matches!(msg, Message::Binary(b) if b.len() == 16 * 1024));
            }
            upstream_burst.await.unwrap();
        })
        .await
        .expect("simultaneous bidirectional burst must not deadlock");
        drop(client);
        relay.await.unwrap();
    }
}

#[tokio::test(start_paused = true)]
async fn stalled_sink_deadline_is_not_extended_by_trickle_frames() {
    // Tiny 256-byte duplex so a single 1 KiB frame blocks inside `poll_flush`.
    let (mut client, _stalled_upstream, relay) =
        duplex_bridge(16 * 1024, 256, false, Duration::from_secs(10)).await;

    // Frame 1 (1 KiB) fits in Tungstenite's 16 KiB write_buffer, blocks in poll_flush,
    // and arms the 10s deadline at t = 0.
    client
        .send(Message::Binary(Bytes::from(vec![1u8; 1024])))
        .await
        .unwrap();
    tokio::task::yield_now().await;

    // Trickle tiny frames at t = 4s and t = 8s; deadline must stay at t = 10s!
    for _ in 0..2 {
        tokio::time::advance(Duration::from_secs(4)).await;
        client
            .send(Message::Binary(Bytes::from_static(b"trickle")))
            .await
            .unwrap();
        tokio::task::yield_now().await;
        assert!(!relay.is_finished());
    }

    // Advancing 3s more (t = 11s > 10s, even though only 3s since last trickle) aborts!
    tokio::time::advance(Duration::from_secs(3)).await;
    relay.await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn slow_progressing_reader_survives_past_io_timeout_and_concurrent_ping() {
    // 4 KiB client duplex with io_timeout = 10s (> 5s WS_CONTROL_FLUSH_TIMEOUT).
    let (mut client, mut upstream, relay) =
        duplex_bridge(4096, 64 * 1024, false, Duration::from_secs(10)).await;

    let sender = tokio::spawn(async move {
        for i in 0u8..8 {
            upstream
                .send(Message::Binary(Bytes::from(vec![i; 8 * 1024])))
                .await
                .unwrap();
        }
        upstream
    });
    tokio::task::yield_now().await;

    // At t = 1s, send a client Ping while `proxy_client` is already blocked in
    // `DataQueued`/`DataFlush` (writing 64 KiB through a 4 KiB socket). The 5s control-flush
    // timeout must NOT overwrite the active 10s data deadline!
    tokio::time::advance(Duration::from_secs(1)).await;
    client
        .send(Message::Ping(Bytes::from_static(b"hb-under-load")))
        .await
        .unwrap();
    tokio::task::yield_now().await;

    // Wait 7s after Ping (t = 8s: > 5s control timeout, < 10s data timeout).
    tokio::time::advance(Duration::from_secs(7)).await;
    tokio::task::yield_now().await;
    assert!(
        !relay.is_finished(),
        "Ping must not shrink active data io_timeout to 5s"
    );

    // Drain slowly: 1 data frame every 6s starting at t = 8s -> t = 50s (5x io_timeout!).
    // Because each 8 KiB read frees >= MIN_WRITE_PROGRESS_BYTES (4 KiB) in the socket,
    // `WriteProgressIo` observes real write progress and resets the stall deadline.
    let mut data_received = 0usize;
    let mut saw_pong = false;
    while data_received < 8 || !saw_pong {
        match client.next().await.unwrap().unwrap() {
            Message::Binary(b) => {
                assert_eq!(b, vec![data_received as u8; 8 * 1024]);
                data_received += 1;
                if data_received < 8 {
                    tokio::time::advance(Duration::from_secs(6)).await;
                }
            }
            Message::Pong(p) => {
                assert_eq!(p, Bytes::from_static(b"hb-under-load"));
                saw_pong = true;
            }
            other => panic!("unexpected frame: {other:?}"),
        }
    }
    let mut upstream = sender.await.unwrap();
    client.close(None).await.unwrap();
    while let Some(Ok(msg)) = client.next().await {
        if msg.is_close() {
            break;
        }
    }
    let _ = upstream.next().await;
    relay.await.unwrap();
}

#[tokio::test]
async fn upstream_ping_forwards_to_idle_client_without_duplicate_pong_and_skips_busy_client() {
    // 1. Idle client: upstream Ping is answered locally to upstream AND forwarded
    // to the idle client; the client's Pong is absorbed without duplicating to upstream.
    let (mut client, mut upstream, relay) =
        duplex_bridge(16 * 1024, 16 * 1024, false, Duration::from_secs(5)).await;

    upstream
        .send(Message::Ping(Bytes::from_static(b"el-idle-hb")))
        .await
        .unwrap();
    let up_pong = tokio::time::timeout(Duration::from_secs(1), upstream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(up_pong, Message::Pong(Bytes::from_static(b"el-idle-hb")));

    let forwarded_ping = tokio::time::timeout(Duration::from_secs(1), client.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        forwarded_ping,
        Message::Ping(Bytes::from_static(b"el-idle-hb"))
    );

    // Client's tungstenite auto-queued Pong during `client.next()`; flush it and
    // follow immediately with a data frame to prove upstream never sees a 2nd Pong.
    client.flush().await.unwrap();
    client
        .send(Message::Text("after-pong".into()))
        .await
        .unwrap();
    let next_at_upstream = upstream.next().await.unwrap().unwrap();
    assert_eq!(next_at_upstream, Message::Text("after-pong".into()));

    client.close(None).await.unwrap();
    while let Some(Ok(msg)) = client.next().await {
        if msg.is_close() {
            break;
        }
    }
    let _ = upstream.next().await;
    relay.await.unwrap();

    // 2. Busy client (`DataFlush`/`DataQueued`): upstream Ping is still answered to upstream,
    // but NOT injected onto the already-active client stream.
    let (mut client, mut upstream, relay) =
        duplex_bridge(512, 64 * 1024, false, Duration::from_secs(5)).await;
    for i in 0u8..2 {
        upstream
            .send(Message::Binary(Bytes::from(vec![i; 4096])))
            .await
            .unwrap();
    }
    tokio::task::yield_now().await;

    upstream
        .send(Message::Ping(Bytes::from_static(b"busy-hb")))
        .await
        .unwrap();
    let up_pong = tokio::time::timeout(Duration::from_secs(1), upstream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(up_pong, Message::Pong(Bytes::from_static(b"busy-hb")));

    for i in 0u8..2 {
        let msg = client.next().await.unwrap().unwrap();
        assert_eq!(msg, Message::Binary(Bytes::from(vec![i; 4096])));
    }
    client.close(None).await.unwrap();
    let close_msg = client.next().await.expect("expected Close frame").unwrap();
    assert!(
        close_msg.is_close(),
        "busy client must not receive injected upstream Ping before Close, got: {close_msg:?}"
    );
    let _ = upstream.next().await;
    relay.await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn sub_threshold_slowloris_drain_and_stalled_ping_flood_still_abort() {
    use tokio::io::AsyncReadExt;

    // 1. Sub-4 KiB Slowloris drain (512 B every 4s on 10s io_timeout = 1024 B < 4 KiB)
    // must NOT disarm the stall deadline and must abort at t = 10s.
    let (mut raw_client, mut upstream, relay) =
        raw_client_bridge(4096, 64 * 1024, Duration::from_secs(10)).await;

    upstream
        .send(Message::Binary(Bytes::from(vec![0xAB; 32 * 1024])))
        .await
        .unwrap();
    tokio::task::yield_now().await;

    let mut trickle_buf = [0u8; 512];
    for _ in 0..2 {
        tokio::time::advance(Duration::from_secs(4)).await;
        let n = raw_client.read(&mut trickle_buf).await.unwrap();
        assert_eq!(n, 512);
        tokio::task::yield_now().await;
        assert!(!relay.is_finished());
    }

    // At t = 11s (> 10s, only 3s after last 512 B read), relay aborts!
    tokio::time::advance(Duration::from_secs(3)).await;
    relay.await.unwrap();

    // 2. Zero-window client flooding Ping frames: bounded by max_write_buffer_size
    // and evicted at `min(io_timeout, WS_CONTROL_FLUSH_TIMEOUT)`.
    for (io_secs, expected_secs) in [(60, 5), (2, 2)] {
        let (mut attacker_ws, _upstream, _guard, relay) =
            stalled_client_bridge(16 * 1024, Duration::from_secs(io_secs)).await;

        for _ in 0..32 {
            attacker_ws
                .feed(Message::Ping(Bytes::from(vec![b'p'; 125])))
                .await
                .unwrap();
        }
        attacker_ws.flush().await.unwrap();
        tokio::task::yield_now().await;

        tokio::time::advance(Duration::from_secs(expected_secs - 1)).await;
        tokio::task::yield_now().await;
        assert!(!relay.is_finished(), "io_secs={io_secs}");

        tokio::time::advance(Duration::from_secs(2)).await;
        relay.await.unwrap();
    }

    // 3. ControlFlush -> DataQueued preserves elapsed stall time when upgrading
    // (`io_timeout > WS_CONTROL_FLUSH_TIMEOUT`) as well as when `io_timeout <= 5s`,
    // so a stalled socket never gets an extra `WS_CONTROL_FLUSH_TIMEOUT` window.
    for (io_secs, advance_before_data, advance_after_data, expect_finished) in [
        (3u64, 2u64, 2u64, true), // t=4s > 3s io_timeout -> must abort (not reset to 5s)
        (10u64, 4u64, 3u64, false), // t=7s > 5s control timeout, < 10s upgraded data timeout
        (10u64, 4u64, 7u64, true), // t=11s > 10s io_timeout -> elapsed 4s preserved (not 14s)
    ] {
        let (mut client_ws, mut upstream_ws, _guard, relay) =
            stalled_client_bridge(64 * 1024, Duration::from_secs(io_secs)).await;

        // Fill proxy->client's 64 B socket with a Ping reply so proxy enters `ControlFlush`.
        client_ws
            .send(Message::Ping(Bytes::from(vec![b'p'; 125])))
            .await
            .unwrap();
        tokio::task::yield_now().await;

        tokio::time::advance(Duration::from_secs(advance_before_data)).await;
        tokio::task::yield_now().await;
        assert!(!relay.is_finished(), "io_secs={io_secs} before data");

        // Upstream data transitions `ControlFlush -> DataQueued`.
        upstream_ws
            .send(Message::Binary(Bytes::from(vec![b'd'; 32 * 1024])))
            .await
            .unwrap();
        tokio::task::yield_now().await;

        tokio::time::advance(Duration::from_secs(advance_after_data)).await;
        tokio::task::yield_now().await;
        assert_eq!(
            relay.is_finished(),
            expect_finished,
            "io_secs={io_secs} after ControlFlush -> DataQueued"
        );
        if !expect_finished {
            tokio::time::advance(Duration::from_secs(io_secs)).await;
        }
        relay.await.unwrap();
    }
}

#[tokio::test]
async fn peer_close_while_sink_is_backpressured_still_flushes_close_reply_and_drains_other() {
    // Tiny 512-byte client duplex so upstream frames immediately backpressure `client`.
    let (mut client, mut upstream, relay) =
        duplex_bridge(512, 64 * 1024, false, Duration::from_secs(5)).await;

    // Fill `proxy_client`'s 16 KiB write buffer + 1-slot `DataQueued`.
    for _ in 0..3 {
        upstream
            .send(Message::Binary(Bytes::from(vec![b'p'; 12 * 1024])))
            .await
            .unwrap();
    }
    tokio::task::yield_now().await;

    // Also pipeline a client->upstream frame right before Close to verify `other` drains cleanly.
    client
        .send(Message::Text("final-before-close".into()))
        .await
        .unwrap();
    client
        .send(Message::Close(Some(CloseFrame {
            code: CloseCode::Normal,
            reason: "done".into(),
        })))
        .await
        .unwrap();

    // Drain inbound frames on client until the RFC 6455 Close echo arrives cleanly
    // (must not fail with `ResetWithoutClosingHandshake` from `SendAfterClosing`).
    let mut saw_close_echo = false;
    while let Some(res) = client.next().await {
        if res.expect("clean close handshake").is_close() {
            saw_close_echo = true;
            break;
        }
    }
    assert!(saw_close_echo, "client must receive Close echo");

    let forwarded = upstream.next().await.unwrap().unwrap();
    assert_eq!(forwarded, Message::Text("final-before-close".into()));
    let close_to_upstream = upstream.next().await.unwrap().unwrap();
    assert!(close_to_upstream.is_close());
    let _ = upstream.next().await;
    relay.await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn shutdown_signal_sends_going_away_and_completes_clean_close_handshake() {
    async fn expect_going_away(peer: &mut WebSocketStream<tokio::io::DuplexStream>) {
        let close_msg = peer
            .next()
            .await
            .expect("Close frame")
            .expect("clean frame");
        let Message::Close(Some(frame)) = close_msg else {
            panic!("expected Close(Some(..)), got {close_msg:?}");
        };
        assert_eq!(frame.code, CloseCode::Away);
        assert_eq!(frame.reason.as_str(), "server shutting down");
        assert!(
            peer.next().await.is_none(),
            "peer must observe clean EOF without reset after Close handshake"
        );
    }

    // 1. Both peers active (`10s` grace -> `9s` live relay + `1s` WS_CLOSE_GRACE margin):
    // frames relay across the drain window, then Close(1001) completes cleanly on both sides.
    let (mut client, mut upstream, tx, relay) =
        duplex_bridge_with_shutdown(16 * 1024, 16 * 1024, false, Duration::from_secs(60)).await;
    tokio::task::yield_now().await;

    let started = tokio::time::Instant::now();
    tx.send(started + Duration::from_secs(10));
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_millis(8500)).await;
    client
        .send(Message::Text("mid-drain".into()))
        .await
        .unwrap();
    assert_eq!(
        upstream.next().await.unwrap().unwrap(),
        Message::Text("mid-drain".into())
    );
    assert!(!relay.is_finished());

    tokio::time::advance(Duration::from_millis(500)).await;
    tokio::join!(
        expect_going_away(&mut client),
        expect_going_away(&mut upstream)
    );
    relay.await.unwrap();
    assert_eq!(started.elapsed(), Duration::from_secs(9));

    // 2. Stalled upstream never echoes Close: active client still observes immediate
    // Close(1001) + clean EOF at `end - WS_CLOSE_GRACE` (`9s`) without waiting for
    // upstream's `1s` timeout, and `relay` terminates strictly at `end` (`10s`).
    let (mut client, _stalled_upstream, tx, relay) =
        duplex_bridge_with_shutdown(16 * 1024, 16 * 1024, false, Duration::from_secs(60)).await;
    tokio::task::yield_now().await;

    let started = tokio::time::Instant::now();
    tx.send(started + Duration::from_secs(10));
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(9)).await;
    tokio::time::timeout(Duration::from_millis(50), expect_going_away(&mut client))
        .await
        .expect("client Close handshake must not block on stalled upstream");
    assert!(!relay.is_finished());
    tokio::time::advance(Duration::from_secs(1)).await;
    relay.await.unwrap();
    assert_eq!(started.elapsed(), Duration::from_secs(10));

    // 3. Zero-grace (`end == now`) with stalled peers exits immediately without
    // blocking on `WS_CLOSE_GRACE`.
    let (_stalled_client, _stalled_upstream, tx, relay) =
        duplex_bridge_with_shutdown(16 * 1024, 16 * 1024, false, Duration::from_secs(60)).await;
    let started = tokio::time::Instant::now();
    tx.send(started);
    relay.await.unwrap();
    assert_eq!(started.elapsed(), Duration::ZERO);
}

#[tokio::test(start_paused = true)]
async fn shutdown_deadline_bounds_an_in_progress_peer_close() {
    let (mut client, _stalled_upstream, tx, relay) =
        duplex_bridge_with_shutdown(16 * 1024, 16 * 1024, false, Duration::from_secs(60)).await;
    tokio::task::yield_now().await;

    // Client initiates a clean Close while upstream never echoes Close, parking
    // `bridge_ws` inside Phase 2 (`finish` -> `WS_CLOSE_GRACE` = 1s).
    client.close(None).await.unwrap();
    let echo = client.next().await.expect("Close echo").expect("clean");
    assert!(echo.is_close(), "expected Close echo, got {echo:?}");

    tokio::time::advance(Duration::from_millis(100)).await;
    tokio::task::yield_now().await;
    assert!(
        !relay.is_finished(),
        "peer close must still be waiting on stalled upstream before shutdown"
    );

    // Zero-grace shutdown arriving mid-`finish` must preempt the remaining 900ms immediately.
    let shutdown_at = tokio::time::Instant::now();
    tx.send(shutdown_at);
    relay.await.unwrap();
    assert_eq!(
        shutdown_at.elapsed(),
        Duration::ZERO,
        "shutdown deadline must preempt an in-progress peer close immediately"
    );
}

#[tokio::test]
async fn oversized_text_and_binary_messages_fragment_within_peer_frame_limit_and_pipelined_ping_resumes()
 {
    // Both endpoints enforce a 16 MiB frame cap and 64 MiB message cap (standard RFC 6455
    // peer defaults, without the proxy's tight 2 MiB write buffer so peers can send 8.5 MiB frames).
    let peer_cfg = tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default()
        .max_frame_size(Some(16 * 1024 * 1024))
        .max_message_size(Some(64 * 1024 * 1024));
    let (mut client, proxy_client, proxy_upstream, mut upstream) =
        duplex_ws_endpoints(256 * 1024, 256 * 1024, Some(peer_cfg)).await;
    let relay = spawn_bridge(proxy_client, proxy_upstream, false, Duration::from_secs(10));

    // 1. Pipelined Ping + Text in a single flush must resume reading immediately after
    // ControlFlush clears instead of stalling the buffered Text frame.
    client
        .feed(Message::Ping(Bytes::from_static(b"pipe")))
        .await
        .unwrap();
    client
        .feed(Message::Text("after-pipelined-ping".into()))
        .await
        .unwrap();
    client.flush().await.unwrap();
    let (pong, forwarded) = tokio::time::timeout(Duration::from_secs(2), async {
        tokio::join!(client.next(), upstream.next())
    })
    .await
    .expect("pipelined Ping + Text must not deadlock");
    assert_eq!(
        pong.unwrap().unwrap(),
        Message::Pong(Bytes::from_static(b"pipe"))
    );
    assert_eq!(
        forwarded.unwrap().unwrap(),
        Message::Text("after-pipelined-ping".into())
    );

    // 2. 17 MiB multi-byte UTF-8 Text message (> 16 MiB max_frame_size) sent as fragmented
    // frames from upstream -> proxy -> client, with 3-byte UTF-8 glyphs straddling 1 MiB cuts.
    let mut text_payload = String::with_capacity(17 * 1024 * 1024 + 32);
    let chunk = "한".repeat(349_525); // 1_048_575 B (straddles 1 MiB boundary on every fragment!)
    for _ in 0..17 {
        text_payload.push_str(&chunk);
    }
    let text_bytes = Bytes::from(text_payload);
    let text_expected = text_bytes.clone();
    let send_text = tokio::spawn(async move {
        let mut split = text_bytes.len() / 2;
        while (text_bytes[split] & 0xC0) == 0x80 {
            split -= 1;
        }
        upstream
            .send(Message::Frame(Frame::message(
                text_bytes.slice(..split),
                OpCode::Data(OpData::Text),
                false,
            )))
            .await
            .unwrap();
        upstream
            .send(Message::Frame(Frame::message(
                text_bytes.slice(split..),
                OpCode::Data(OpData::Continue),
                true,
            )))
            .await
            .unwrap();
        upstream
    });
    let received_text = tokio::time::timeout(Duration::from_secs(10), client.next())
        .await
        .expect("17 MiB Text must complete")
        .unwrap()
        .expect("client must accept 1 MiB outbound fragments");
    let Message::Text(received_utf8) = received_text else {
        panic!("expected Text, got {received_text:?}");
    };
    assert_eq!(received_utf8.as_bytes(), text_expected.as_ref());
    let mut upstream = send_text.await.unwrap();

    // 3. 17 MiB Binary message sent from client -> proxy -> upstream.
    let bin_payload = Bytes::from(vec![0x5Au8; 17 * 1024 * 1024]);
    let bin_expected = bin_payload.clone();
    let send_bin = tokio::spawn(async move {
        let mid = bin_payload.len() / 2;
        client
            .send(Message::Frame(Frame::message(
                bin_payload.slice(..mid),
                OpCode::Data(OpData::Binary),
                false,
            )))
            .await
            .unwrap();
        client
            .send(Message::Frame(Frame::message(
                bin_payload.slice(mid..),
                OpCode::Data(OpData::Continue),
                true,
            )))
            .await
            .unwrap();
        client
    });
    let received_bin = tokio::time::timeout(Duration::from_secs(10), upstream.next())
        .await
        .expect("17 MiB Binary must complete")
        .unwrap()
        .expect("upstream must accept 1 MiB masked outbound fragments");
    let Message::Binary(received_bytes) = received_bin else {
        panic!("expected Binary, got {received_bin:?}");
    };
    assert_eq!(received_bytes, bin_expected);

    let mut client = send_bin.await.unwrap();
    client.close(None).await.unwrap();
    let _ = client.next().await;
    let _ = upstream.next().await;
    let _ = upstream.next().await;
    relay.await.unwrap();
}

#[tokio::test]
async fn peer_eof_and_protocol_error_flush_pending_and_shut_down_cleanly() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // 1. Abrupt client drop without WS Close (`PeerStop::Failed` via
    // `ResetWithoutClosingHandshake`) flushes in-flight upstream frames and shuts
    // down the upstream write half cleanly.
    {
        let (mut client, mut upstream, relay) =
            duplex_bridge(64 * 1024, 64 * 1024, false, Duration::from_secs(5)).await;
        client
            .send(Message::Text("before-eof".into()))
            .await
            .unwrap();
        drop(client);

        let msg = tokio::time::timeout(Duration::from_secs(2), upstream.next())
            .await
            .expect("upstream must receive frame sent before EOF")
            .unwrap()
            .unwrap();
        assert_eq!(msg, Message::Text("before-eof".into()));

        let end = tokio::time::timeout(Duration::from_secs(2), upstream.next())
            .await
            .expect("upstream must observe clean transport EOF");
        assert!(
            matches!(
                end,
                None | Some(Err(
                    tokio_tungstenite::tungstenite::Error::Protocol(
                        tokio_tungstenite::tungstenite::error::ProtocolError::ResetWithoutClosingHandshake
                    )
                ))
            ),
            "unexpected post-EOF item: {end:?}"
        );
        tokio::time::timeout(Duration::from_secs(2), relay)
            .await
            .expect("relay must exit promptly on peer EOF")
            .unwrap();
    }

    // 2. `PeerStop::Failed`: malformed client frame flushes prior upstream data
    // (`other.flush_and_shutdown`), flushes any buffered control reply on the
    // failed peer (`src.flush_close_reply`), and shuts down both write halves.
    {
        let (mut client_io, mut upstream, relay) =
            raw_client_bridge(64 * 1024, 64 * 1024, Duration::from_secs(5)).await;

        // Valid masked Text("ok") + masked Ping("p") (mask key 0x00000000) followed
        // immediately by an illegal unmasked Text("x") triggering `PeerStop::Failed`.
        client_io
            .write_all(&[
                0x81, 0x82, 0x00, 0x00, 0x00, 0x00, b'o', b'k', // masked Text("ok")
                0x89, 0x81, 0x00, 0x00, 0x00, 0x00, b'p', // masked Ping("p")
                0x81, 0x01, b'x', // illegal unmasked Text("x")
            ])
            .await
            .unwrap();
        client_io.flush().await.unwrap();

        let msg = tokio::time::timeout(Duration::from_secs(2), upstream.next())
            .await
            .expect("upstream must receive valid frame preceding protocol error")
            .unwrap()
            .unwrap();
        assert_eq!(msg, Message::Text("ok".into()));
        let end = tokio::time::timeout(Duration::from_secs(2), upstream.next())
            .await
            .expect("upstream must observe transport shutdown after peer protocol error");
        assert!(
            matches!(
                end,
                None | Some(Err(
                    tokio_tungstenite::tungstenite::Error::Protocol(
                        tokio_tungstenite::tungstenite::error::ProtocolError::ResetWithoutClosingHandshake
                    )
                ))
            ),
            "unexpected post-error item: {end:?}"
        );

        let mut reply = Vec::new();
        tokio::time::timeout(Duration::from_secs(2), client_io.read_to_end(&mut reply))
            .await
            .expect("client must observe flushed control reply and clean EOF")
            .unwrap();
        assert_eq!(
            reply,
            vec![0x8A, 0x01, b'p'],
            "expected flushed server Pong(\"p\") before shutdown, got: {reply:02x?}"
        );
        tokio::time::timeout(Duration::from_secs(2), relay)
            .await
            .expect("relay must exit promptly on peer protocol error")
            .unwrap();
    }
}

#[test]
fn fragments_keep_order_opcodes_fin_and_utf8_scalar_boundaries() {
    fn collect_frames(mut msg: QueuedMsg) -> Vec<Frame> {
        let mut frames = Vec::new();
        loop {
            let (frame_msg, is_final) = msg.pop_frame();
            let Message::Frame(frame) = frame_msg else {
                panic!("expected Frame, got {frame_msg:?}");
            };
            frames.push(frame);
            if is_final {
                break;
            }
        }
        frames
    }

    // Empty and small messages emit a single final frame with the expected opcode.
    for (msg, expected_op, expected_payload) in [
        (
            QueuedMsg::text(Utf8Bytes::from_static("")),
            OpCode::Data(OpData::Text),
            b"".as_slice(),
        ),
        (
            QueuedMsg::text(Utf8Bytes::from_static("small")),
            OpCode::Data(OpData::Text),
            b"small",
        ),
        (
            QueuedMsg::binary(Bytes::new()),
            OpCode::Data(OpData::Binary),
            b"",
        ),
        (
            QueuedMsg::binary(Bytes::from_static(b"bin")),
            OpCode::Data(OpData::Binary),
            b"bin",
        ),
    ] {
        let frames = collect_frames(msg);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].header().opcode, expected_op);
        assert!(frames[0].header().is_final);
        assert_eq!(frames[0].payload(), expected_payload);
    }

    // Text straddling a 4-byte UTF-8 scalar right at `MAX_OUTBOUND_FRAME_BYTES`.
    let mut text = "a".repeat(MAX_OUTBOUND_FRAME_BYTES - 1);
    text.push('🦀');
    text.push_str(&"b".repeat(MAX_OUTBOUND_FRAME_BYTES));
    text.push('🦀');
    let text_frames = collect_frames(QueuedMsg::text(Utf8Bytes::from(text.clone())));
    assert_eq!(text_frames.len(), 3);
    let mut reassembled_text = Vec::new();
    for (i, frame) in text_frames.iter().enumerate() {
        let expected_op = if i == 0 {
            OpCode::Data(OpData::Text)
        } else {
            OpCode::Data(OpData::Continue)
        };
        assert_eq!(frame.header().opcode, expected_op, "frame {i}");
        assert_eq!(
            frame.header().is_final,
            i + 1 == text_frames.len(),
            "frame {i}"
        );
        let payload = frame.payload();
        assert!(!payload.is_empty() && payload.len() <= MAX_OUTBOUND_FRAME_BYTES);
        assert!(
            std::str::from_utf8(payload).is_ok(),
            "frame {i} split inside a UTF-8 scalar"
        );
        reassembled_text.extend_from_slice(payload);
    }
    assert_eq!(text_frames[0].payload().len(), MAX_OUTBOUND_FRAME_BYTES - 1);
    assert!(text_frames[1].payload().starts_with("🦀".as_bytes()));
    assert_eq!(reassembled_text, text.as_bytes());

    // Binary payload > 2 MiB fragments cleanly on exact `MAX_OUTBOUND_FRAME_BYTES` boundaries.
    let bin: Vec<u8> = (0..(2 * MAX_OUTBOUND_FRAME_BYTES + 17))
        .map(|i| (i % 251) as u8)
        .collect();
    let bin_frames = collect_frames(QueuedMsg::binary(Bytes::from(bin.clone())));
    assert_eq!(bin_frames.len(), 3);
    let mut reassembled_bin = Vec::new();
    for (i, frame) in bin_frames.iter().enumerate() {
        let expected_op = if i == 0 {
            OpCode::Data(OpData::Binary)
        } else {
            OpCode::Data(OpData::Continue)
        };
        assert_eq!(frame.header().opcode, expected_op, "frame {i}");
        assert_eq!(
            frame.header().is_final,
            i + 1 == bin_frames.len(),
            "frame {i}"
        );
        assert!(frame.payload().len() <= MAX_OUTBOUND_FRAME_BYTES);
        reassembled_bin.extend_from_slice(frame.payload());
    }
    assert_eq!(reassembled_bin, bin);
}

#[test]
fn deferred_local_reply_waits_for_the_final_fragment_of_the_queued_message() {
    let mut outbound = OutboundState::DataQueued {
        current: QueuedMsg::text(Utf8Bytes::from(
            "x".repeat(2 * MAX_OUTBOUND_FRAME_BYTES + 7),
        )),
        deferred_reply: None,
    };

    let mut frames = Vec::new();
    let mut push_frame = |msg: Message| {
        let Message::Frame(frame) = msg else {
            panic!("expected Frame, got {msg:?}");
        };
        let header = frame.header();
        frames.push((header.opcode, header.is_final, frame.payload().len()));
        frame
    };

    // Pop the opening fragment first so `current.first` is already false when a
    // local `eth_syncing` mock reply is deferred mid-message.
    push_frame(outbound.pop_frame().expect("first fragment"));
    match &mut outbound {
        OutboundState::DataQueued { deferred_reply, .. } => {
            *deferred_reply = Some("reply".to_owned());
        }
        _ => panic!("expected DataQueued after non-final fragment"),
    }

    let mut last_frame = None;
    while let Some(msg) = outbound.pop_frame() {
        last_frame = Some(push_frame(msg));
    }
    assert_eq!(
        frames,
        [
            (OpCode::Data(OpData::Text), false, MAX_OUTBOUND_FRAME_BYTES),
            (
                OpCode::Data(OpData::Continue),
                false,
                MAX_OUTBOUND_FRAME_BYTES
            ),
            (OpCode::Data(OpData::Continue), true, 7),
            (OpCode::Data(OpData::Text), true, "reply".len()),
        ]
    );
    assert_eq!(last_frame.expect("reply frame").payload(), b"reply");
    assert!(matches!(outbound, OutboundState::DataFlush));
}
