//! Execution Layer JSON-RPC batch probe (`eth_syncing`, `net_peerCount`, `eth_getBlockByNumber`)
//! and adaptive cleartext `h2c` transport detection.

use std::sync::Arc;

use bytes::Bytes;
use http::{Method, Request};
use http_body_util::Full;

use crate::state::AppState;
use crate::transport::{H2cMode, HttpTransport, ProxyClient, box_full};

use super::probe::{HealthError, JsonObject, fetch_bytes, is_transport};

const EL_BATCH_REQ: &[u8] = br#"[{"jsonrpc":"2.0","method":"eth_syncing","params":[],"id":1},{"jsonrpc":"2.0","method":"net_peerCount","params":[],"id":2},{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["latest",false],"id":3}]"#;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ElSyncingResult {
    Synced(bool),
    Syncing {
        current_block: u64,
        highest_block: u64,
    },
}

impl<'de> serde::Deserialize<'de> for ElSyncingResult {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(serde::Deserialize)]
        struct SyncingFields {
            #[serde(rename = "currentBlock", deserialize_with = "hex_string")]
            current_block: u64,
            #[serde(rename = "highestBlock", deserialize_with = "hex_string")]
            highest_block: u64,
        }
        struct SyncingVisitor;
        impl<'de> serde::de::Visitor<'de> for SyncingVisitor {
            type Value = ElSyncingResult;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a boolean or an eth_syncing progress object")
            }
            fn visit_bool<E: serde::de::Error>(self, v: bool) -> Result<Self::Value, E> {
                Ok(ElSyncingResult::Synced(v))
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                map: A,
            ) -> Result<Self::Value, A::Error> {
                let fields = <SyncingFields as serde::Deserialize>::deserialize(
                    serde::de::value::MapAccessDeserializer::new(map),
                )?;
                Ok(ElSyncingResult::Syncing {
                    current_block: fields.current_block,
                    highest_block: fields.highest_block,
                })
            }
        }
        deserializer.deserialize_any(SyncingVisitor)
    }
}

#[derive(serde::Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ElBlockResult {
    #[serde(deserialize_with = "hex_string")]
    pub(super) number: u64,
    #[serde(deserialize_with = "hex_string")]
    pub(super) timestamp: u64,
}

#[cfg_attr(test, derive(Debug))]
pub(super) struct ElBatchResult {
    pub(super) syncing: Result<ElSyncingResult, HealthError>,
    pub(super) peers: Result<u64, HealthError>,
    pub(super) block: Result<ElBlockResult, HealthError>,
}

/// Query all three EL JSON-RPC methods in a batch request, maintaining the transport verdict.
pub(super) async fn el_batch_detect(state: &AppState) -> Result<ElBatchResult, HealthError> {
    let default_client = &state.client;
    match &state.el.transport {
        HttpTransport::CleartextH1 | HttpTransport::TlsAuto(_) => {
            el_batch_rpc(state, default_client).await
        }
        HttpTransport::AdaptiveH2c { mode, client } => {
            let h2c_client: &ProxyClient = client;
            let cur = H2cMode::load(mode);
            let (first, second, on_first, on_second) = match cur {
                H2cMode::Initial | H2cMode::Confirmed => (
                    h2c_client,
                    default_client,
                    H2cMode::Confirmed,
                    H2cMode::H1VerifyOnce,
                ),
                // Verify h2c one more time to self-heal if an h2c upstream was
                // probed mid-bind during startup or restart.
                H2cMode::H1VerifyOnce => (
                    h2c_client,
                    default_client,
                    H2cMode::Confirmed,
                    H2cMode::H1Confirmed,
                ),
                H2cMode::H1Confirmed => (
                    default_client,
                    h2c_client,
                    H2cMode::H1Confirmed,
                    H2cMode::Confirmed,
                ),
            };
            match el_batch_rpc(state, first).await {
                Err(first_err) if is_transport(&first_err) => {
                    match el_batch_rpc(state, second).await {
                        Err(second_err) if is_transport(&second_err) => {
                            Err(if cur == H2cMode::H1VerifyOnce {
                                second_err
                            } else {
                                first_err
                            })
                        }
                        reached => {
                            H2cMode::transition(mode, cur, on_second);
                            reached
                        }
                    }
                }
                reached => {
                    H2cMode::transition(mode, cur, on_first);
                    reached
                }
            }
        }
    }
}

async fn el_batch_rpc(
    state: &AppState,
    client: &ProxyClient,
) -> Result<ElBatchResult, HealthError> {
    let req = Request::builder()
        .method(Method::POST)
        .uri(state.el.upstream.uri().clone())
        .header("content-type", "application/json")
        .body(box_full(Full::new(Bytes::from_static(EL_BATCH_REQ))))
        .map_err(|e| HealthError::Transport(Arc::from(format!("build: {e}"))))?;
    let body = fetch_bytes(state, req, client, &state.el.transport).await?;
    parse_el_batch(&body)
}

fn parse_el_batch(body: &[u8]) -> Result<ElBatchResult, HealthError> {
    #[derive(serde::Deserialize)]
    struct RpcResponse<'a> {
        id: i64,
        #[serde(borrow)]
        result: Option<&'a serde_json::value::RawValue>,
        #[serde(borrow)]
        error: Option<&'a serde_json::value::RawValue>,
    }

    #[derive(serde::Deserialize)]
    #[serde(transparent)]
    struct HexQuantity(#[serde(deserialize_with = "hex_string")] u64);

    let arr: [JsonObject<RpcResponse<'_>>; 3] =
        serde_json::from_slice(body).map_err(|e| HealthError::Decode(Arc::from(e.to_string())))?;
    let mut slots: [Option<RpcResponse<'_>>; 3] = [None, None, None];
    for JsonObject(resp) in arr {
        let Some(idx) = usize::try_from(resp.id)
            .ok()
            .and_then(|id| id.checked_sub(1))
            .filter(|&idx| idx < 3)
        else {
            return Err(HealthError::Decode(Arc::from(
                "duplicate or unexpected response id",
            )));
        };
        if slots[idx].replace(resp).is_some() {
            return Err(HealthError::Decode(Arc::from(
                "duplicate or unexpected response id",
            )));
        }
    }

    let decode_res = |idx: usize| -> Result<&serde_json::value::RawValue, HealthError> {
        let resp = slots[idx].as_ref().ok_or_else(|| {
            HealthError::Decode(Arc::from(format!("missing response for id {}", idx + 1)))
        })?;
        if let Some(err) = resp.error {
            return Err(HealthError::Rpc(Arc::from(err.get())));
        }
        resp.result
            .ok_or_else(|| HealthError::Decode(Arc::from("missing result")))
    };

    let syncing = decode_res(0).and_then(|raw| {
        serde_json::from_str(raw.get()).map_err(|e| HealthError::Decode(Arc::from(e.to_string())))
    });
    let peers = decode_res(1).and_then(|raw| {
        serde_json::from_str::<HexQuantity>(raw.get())
            .map(|HexQuantity(n)| n)
            .map_err(|e| HealthError::Decode(Arc::from(e.to_string())))
    });
    let block = decode_res(2).and_then(|raw| {
        serde_json::from_str::<JsonObject<ElBlockResult>>(raw.get())
            .map(|JsonObject(block)| block)
            .map_err(|e| HealthError::Decode(Arc::from(e.to_string())))
    });

    Ok(ElBatchResult {
        syncing,
        peers,
        block,
    })
}

fn hex_string<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<u64, D::Error> {
    struct Hex;
    impl serde::de::Visitor<'_> for Hex {
        type Value = u64;
        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("a 0x-prefixed hex quantity string")
        }
        fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<u64, E> {
            hex_to_u64(value).ok_or_else(|| E::custom(format!("invalid hex quantity: {value}")))
        }
    }
    deserializer.deserialize_str(Hex)
}

fn hex_to_u64(s: &str) -> Option<u64> {
    let digits = s.strip_prefix("0x")?;
    if digits.is_empty()
        || (digits.len() > 1 && digits.starts_with('0'))
        || !digits.bytes().all(|b| b.is_ascii_hexdigit())
    {
        return None;
    }
    u64::from_str_radix(digits, 16).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::health::{HealthSource, Probe};
    use http::{Response, StatusCode};
    use http_body_util::BodyExt;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::sync::watch;

    #[tokio::test]
    async fn el_transport_fallback_preserves_verdict_and_error_policy() {
        use clap::Parser;
        use hyper::body::Incoming;
        use hyper::service::service_fn;
        use hyper_util::rt::{TokioExecutor, TokioIo};
        use hyper_util::server::conn::auto;

        let valid = br#"[{"id":1,"result":false},{"id":2,"result":"0x40"},{"id":3,"result":{"number":"0x1","timestamp":"0x2"}}]"#;
        // Preferred transport, accepted [h1, h2], response, expected transport.
        for (prefer_h2, accepted, status, body, expected_h2) in [
            (true, [true, true], 200, valid.as_slice(), true),
            (true, [true, true], 503, b"unavailable".as_slice(), true),
            (true, [true, true], 200, b"invalid JSON".as_slice(), true),
            (false, [true, true], 503, b"unavailable".as_slice(), false),
            (true, [true, false], 200, valid.as_slice(), false),
            (true, [true, false], 503, b"unavailable".as_slice(), false),
            (true, [true, false], 200, b"invalid JSON".as_slice(), false),
            (false, [false, true], 200, valid.as_slice(), true),
            (false, [false, true], 503, b"unavailable".as_slice(), true),
            (false, [false, true], 200, b"invalid JSON".as_slice(), true),
            (true, [false, false], 200, valid.as_slice(), true),
            (false, [false, false], 200, valid.as_slice(), false),
        ] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            let attempts = Arc::new([AtomicUsize::new(0), AtomicUsize::new(0)]);
            let counts = attempts.clone();
            let server = tokio::spawn(async move {
                loop {
                    let (stream, _) = listener.accept().await.unwrap();
                    let counts = counts.clone();
                    tokio::spawn(async move {
                        let mut preface = [0; 3];
                        let h2 =
                            stream.peek(&mut preface).await.unwrap() == 3 && &preface == b"PRI";
                        counts[usize::from(h2)].fetch_add(1, Ordering::Relaxed);
                        if !accepted[usize::from(h2)] {
                            // On total failure distinguish the original h2 timeout
                            // from the alternate h1 connection error.
                            if h2 && !accepted.contains(&true) {
                                tokio::time::sleep(Duration::from_secs(2)).await;
                            }
                            return;
                        }
                        let svc = service_fn(move |req: Request<Incoming>| async move {
                            req.into_body().collect().await.unwrap();
                            Ok::<_, std::convert::Infallible>(
                                Response::builder()
                                    .status(status)
                                    .body(Full::new(Bytes::from_static(body)))
                                    .unwrap(),
                            )
                        });
                        let _ = auto::Builder::new(TokioExecutor::new())
                            .serve_connection(TokioIo::new(stream), svc)
                            .await;
                    });
                }
            });
            let mut cfg = crate::Config::parse_from([
                "ethonyx",
                "--el-http-url",
                &url,
                "--cl-beacon-url",
                &url,
            ]);
            cfg.health_timeout = Duration::from_secs(1);
            let validated = cfg.parse_validated().unwrap();
            let (_, probe) = watch::channel(Arc::new(Probe::pending()));
            let mut state =
                AppState::new(&cfg, validated, false, HealthSource::Polled(probe)).unwrap();
            state.el.transport.set_confirmed_cleartext(prefer_h2);
            let result = el_batch_detect(&state).await;
            assert_eq!(state.el.transport.uses_h2c_client(), expected_h2);
            assert_eq!(attempts[usize::from(prefer_h2)].load(Ordering::Relaxed), 1);
            assert_eq!(
                attempts[usize::from(!prefer_h2)].load(Ordering::Relaxed),
                usize::from(!accepted[usize::from(prefer_h2)])
            );
            if !accepted.contains(&true) {
                let error = result.unwrap_err();
                if prefer_h2 {
                    assert_eq!(error, HealthError::Timeout);
                } else {
                    assert!(matches!(error, HealthError::Transport(_)));
                }
            } else if status == 503 {
                assert_eq!(
                    result.unwrap_err(),
                    HealthError::HttpStatus(StatusCode::SERVICE_UNAVAILABLE)
                );
            } else if body == valid {
                let batch = result.unwrap();
                assert_eq!(batch.syncing.unwrap(), ElSyncingResult::Synced(false));
                assert_eq!(batch.peers.unwrap(), 0x40);
            } else {
                assert!(matches!(result.unwrap_err(), HealthError::Decode(_)));
            }
            server.abort();
        }
    }

    #[tokio::test]
    async fn el_transport_verify_once_heals_restart_race_and_settles_on_h1_only() {
        use clap::Parser;
        use hyper::body::Incoming;
        use hyper::service::service_fn;
        use hyper_util::rt::{TokioExecutor, TokioIo};
        use hyper_util::server::conn::auto;

        let valid = br#"[{"id":1,"result":false},{"id":2,"result":"0x40"},{"id":3,"result":{"number":"0x1","timestamp":"0x2"}}]"#;
        let accept_h2 = Arc::new(AtomicBool::new(false));
        let accept_h2_srv = accept_h2.clone();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                let accept_h2_srv = accept_h2_srv.clone();
                tokio::spawn(async move {
                    let mut preface = [0; 3];
                    let is_h2 =
                        stream.peek(&mut preface).await.unwrap_or(0) == 3 && &preface == b"PRI";
                    if is_h2 && !accept_h2_srv.load(Ordering::SeqCst) {
                        return;
                    }
                    let svc = service_fn(move |req: Request<Incoming>| async move {
                        req.into_body().collect().await.unwrap();
                        Ok::<_, std::convert::Infallible>(
                            Response::builder()
                                .status(StatusCode::OK)
                                .body(Full::new(Bytes::from_static(valid)))
                                .unwrap(),
                        )
                    });
                    let _ = auto::Builder::new(TokioExecutor::new())
                        .serve_connection(TokioIo::new(stream), svc)
                        .await;
                });
            }
        });

        let mut cfg =
            crate::Config::parse_from(["ethonyx", "--el-http-url", &url, "--cl-beacon-url", &url]);
        cfg.health_timeout = Duration::from_secs(1);
        let validated = cfg.parse_validated().unwrap();
        let (_, probe) = watch::channel(Arc::new(Probe::pending()));
        let state = AppState::new(&cfg, validated, false, HealthSource::Polled(probe)).unwrap();

        // Case A: h1-only upstream -> Initial -> H1VerifyOnce -> H1Confirmed.
        assert_eq!(state.el.transport.label(), "h2c");
        assert_eq!(
            state.el.transport.startup_proto_label(),
            "HTTP/2 (h2c, unverified)"
        );
        assert!(el_batch_detect(&state).await.unwrap().syncing.is_ok());
        assert_eq!(state.el.transport.label(), "http/1.1");
        assert!(el_batch_detect(&state).await.unwrap().syncing.is_ok());
        assert_eq!(state.el.transport.label(), "http/1.1");

        // Case B: simulate a confirmed h2c upstream hitting a transient h2c rejection
        // during restart, then recovering h2c on the verify-once poll.
        if let HttpTransport::AdaptiveH2c { ref mode, .. } = state.el.transport {
            mode.store(H2cMode::Confirmed as u8, Ordering::SeqCst);
        }
        assert_eq!(state.el.transport.startup_proto_label(), "HTTP/2 (h2c)");
        assert!(el_batch_detect(&state).await.unwrap().syncing.is_ok());
        assert_eq!(state.el.transport.label(), "http/1.1"); // fell back to H1VerifyOnce

        accept_h2.store(true, Ordering::SeqCst);
        assert!(el_batch_detect(&state).await.unwrap().syncing.is_ok());
        assert_eq!(state.el.transport.label(), "h2c"); // self-healed back to Confirmed!

        server.abort();
    }

    #[test]
    fn el_batch_requires_three_object_responses_and_object_blocks() {
        let replies = [
            r#"{"id":1,"result":false}"#,
            r#"{"id":2,"result":"0x40"}"#,
            r#"{"id":3,"result":{"number":"0x1","timestamp":"0x2"}}"#,
        ];
        for order in [
            [0, 1, 2],
            [0, 2, 1],
            [1, 0, 2],
            [1, 2, 0],
            [2, 0, 1],
            [2, 1, 0],
        ] {
            let input = format!("[{}]", order.map(|i| replies[i]).join(","));
            let parsed = parse_el_batch(input.as_bytes()).unwrap();
            assert_eq!(parsed.syncing.unwrap(), ElSyncingResult::Synced(false));
            assert_eq!(parsed.peers.unwrap(), 0x40);
            assert_eq!(parsed.block.unwrap().timestamp, 2);
        }
        for input in [
            format!("[{},{}]", replies[0], replies[1]),
            format!(
                "[{},{},{},{}]",
                replies[0], replies[1], replies[2], replies[0]
            ),
            r#"[[1,false,null],{"id":2,"result":"0x40"},{"id":3,"result":{}}]"#.into(),
            r#"[[1,false,null],[2,"0x40",null],[3,{"number":"0x1","timestamp":"0x2"},null]]"#
                .into(),
        ] {
            assert!(parse_el_batch(input.as_bytes()).is_err(), "{input}");
        }
        let parsed = parse_el_batch(br#"[{"id":1,"result":false},{"id":2,"result":"0x40"},{"id":3,"result":["0x1","0x2"]}]"#).unwrap();
        assert!(parsed.syncing.is_ok());
        assert!(parsed.peers.is_ok());
        assert!(parsed.block.is_err());
        assert!(serde_json::from_str::<ElSyncingResult>(r#"["0x1","0x2"]"#).is_err());
    }

    #[test]
    fn el_batch_matches_ids_in_any_order_and_keeps_partial_results() {
        let parsed = parse_el_batch(br#"[{"id":3,"result":{"number":"0x42","timestamp":"0x43"}},{"id":1,"result":false},{"id":2,"error":{"code":-32000,"message":"unavailable"}}]"#).unwrap();
        assert_eq!(parsed.syncing.unwrap(), ElSyncingResult::Synced(false));
        assert_eq!(parsed.block.unwrap().number, 0x42);
        assert!(matches!(parsed.peers, Err(HealthError::Rpc(_))));
    }

    #[test]
    fn el_batch_rejects_ambiguous_ids_and_invalid_results() {
        for input in [
            r#"[]"#,
            r#"[{"id":1,"result":false},{"id":1,"result":false},{"id":3,"result":null}]"#,
            r#"[{"id":1,"result":false},{"id":2,"result":"0x1"},{"id":4,"result":null}]"#,
            r#"[{"id":"1","result":false},{"id":2,"result":"0x1"},{"id":3,"result":null}]"#,
        ] {
            assert!(
                matches!(
                    parse_el_batch(input.as_bytes()),
                    Err(HealthError::Decode(_))
                ),
                "{input}"
            );
        }
        let parsed =
            parse_el_batch(br#"[{"id":1,"result":null},{"id":2},{"id":3,"result":{}}]"#).unwrap();
        assert!(parsed.syncing.is_err());
        assert!(parsed.peers.is_err());
        assert!(parsed.block.is_err());
    }

    #[test]
    fn hex_with_0x_prefix() {
        assert_eq!(hex_to_u64("0x10"), Some(16));
        assert_eq!(hex_to_u64("0xff"), Some(255));
        assert_eq!(hex_to_u64("0x0"), Some(0));
    }

    #[test]
    fn hex_rejects_non_canonical_or_malformed_quantities() {
        for invalid in [
            "",
            "0x",
            "0X10",
            "ff",
            "a",
            "0x00",
            "0x01",
            "0x+10",
            "0x-10",
            "0x0x10",
            "0xZZ",
            "not hex",
            "0x10000000000000000",
        ] {
            assert_eq!(hex_to_u64(invalid), None, "{invalid}");
        }
    }

    #[test]
    fn hex_handles_block_timestamp_width() {
        assert_eq!(hex_to_u64("0x671E0000"), Some(0x671E_0000));
    }
}
