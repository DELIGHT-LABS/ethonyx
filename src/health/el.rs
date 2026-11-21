//! Execution Layer JSON-RPC batch (`eth_syncing`, `net_peerCount`, `eth_getBlockByNumber`)
//! and hex-quantity (`eth_blockNumber`, `eth_chainId`) probe codecs.

use std::borrow::Cow;

use crate::rpc::JsonObject;

use super::fetch::{HealthError, ProbeTarget};

const EL_BATCH_REQ: &[u8] = br#"[{"jsonrpc":"2.0","method":"eth_syncing","params":[],"id":1},{"jsonrpc":"2.0","method":"net_peerCount","params":[],"id":2},{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["latest",false],"id":3}]"#;
const EL_BLOCK_NUMBER_REQ: &[u8] =
    br#"{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}"#;
const EL_CHAIN_ID_REQ: &[u8] = br#"{"jsonrpc":"2.0","method":"eth_chainId","params":[],"id":1}"#;
const JSONRPC_METHOD_NOT_FOUND: i64 = -32601;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ElSyncingResult {
    /// `eth_syncing` returned `false`.
    Synced,
    /// `eth_syncing` returned `true` (syncing without block progress details).
    SyncingBool,
    /// `eth_syncing` returned a progress object.
    SyncingProgress {
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
            fn visit_bool<E: serde::de::Error>(self, syncing: bool) -> Result<Self::Value, E> {
                Ok(if syncing {
                    ElSyncingResult::SyncingBool
                } else {
                    ElSyncingResult::Synced
                })
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                map: A,
            ) -> Result<Self::Value, A::Error> {
                let fields = <SyncingFields as serde::Deserialize>::deserialize(
                    serde::de::value::MapAccessDeserializer::new(map),
                )?;
                Ok(ElSyncingResult::SyncingProgress {
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
    pub(super) peers: Result<Option<u64>, HealthError>,
    pub(super) block: Result<ElBlockResult, HealthError>,
}

#[derive(serde::Deserialize)]
struct RpcResponse<'a> {
    id: i64,
    #[serde(borrow)]
    result: Option<&'a serde_json::value::RawValue>,
    #[serde(borrow)]
    error: Option<&'a serde_json::value::RawValue>,
}

impl RpcResponse<'_> {
    fn decode<T: serde::de::DeserializeOwned>(self) -> Result<T, HealthError> {
        if let Some(err) = self.error {
            return Err(HealthError::Rpc(Box::from(err.get())));
        }
        let raw = self
            .result
            .ok_or(HealthError::Decode(Cow::Borrowed("missing result")))?;
        serde_json::from_str(raw.get()).map_err(HealthError::decode)
    }
}

#[derive(serde::Deserialize)]
#[serde(transparent)]
struct HexQuantity(#[serde(deserialize_with = "hex_string")] u64);

/// Query all three EL JSON-RPC methods in a batch request, maintaining the transport verdict.
pub(super) async fn el_batch_detect(el: ProbeTarget<'_>) -> Result<ElBatchResult, HealthError> {
    let body = el.post_json_adaptive(EL_BATCH_REQ).await?;
    parse_el_batch(&body)
}

pub(super) async fn el_block_number(el: ProbeTarget<'_>) -> Result<u64, HealthError> {
    el_quantity_rpc(el, EL_BLOCK_NUMBER_REQ).await
}

pub(super) async fn el_chain_id(el: ProbeTarget<'_>) -> Result<u64, HealthError> {
    el_quantity_rpc(el, EL_CHAIN_ID_REQ).await
}

async fn el_quantity_rpc(el: ProbeTarget<'_>, payload: &'static [u8]) -> Result<u64, HealthError> {
    let body = el.post_json(payload).await?;
    parse_el_quantity(&body)
}

fn parse_el_quantity(body: &[u8]) -> Result<u64, HealthError> {
    let JsonObject(resp): JsonObject<RpcResponse<'_>> =
        serde_json::from_slice(body).map_err(HealthError::decode)?;
    if resp.id != 1 {
        return Err(HealthError::Decode(Cow::Borrowed("unexpected response id")));
    }
    resp.decode::<HexQuantity>().map(|HexQuantity(n)| n)
}

fn is_method_not_found(err_raw: &str) -> bool {
    #[derive(serde::Deserialize)]
    struct RpcErrorCode {
        code: i64,
    }
    serde_json::from_str::<JsonObject<RpcErrorCode>>(err_raw)
        .is_ok_and(|JsonObject(e)| e.code == JSONRPC_METHOD_NOT_FOUND)
}

fn parse_el_batch(body: &[u8]) -> Result<ElBatchResult, HealthError> {
    let arr: [JsonObject<RpcResponse<'_>>; 3] =
        serde_json::from_slice(body).map_err(HealthError::decode)?;
    let mut slots: [Option<RpcResponse<'_>>; 3] = [None, None, None];
    for JsonObject(resp) in arr {
        // An id outside `1..=3` has no slot, and a repeated id overwrites one: with
        // exactly three responses either case leaves a slot empty below.
        if let Some(slot) = usize::try_from(resp.id)
            .ok()
            .and_then(|id| id.checked_sub(1))
            .and_then(|idx| slots.get_mut(idx))
        {
            *slot = Some(resp);
        }
    }
    let [Some(r_syncing), Some(r_peers), Some(r_block)] = slots else {
        return Err(HealthError::Decode(Cow::Borrowed(
            "duplicate or unexpected response id",
        )));
    };

    let syncing = r_syncing.decode();
    let peers = match r_peers.error {
        Some(err) if is_method_not_found(err.get()) => Ok(None),
        _ => r_peers
            .decode::<HexQuantity>()
            .map(|HexQuantity(n)| Some(n)),
    };
    let block = r_block
        .decode::<JsonObject<ElBlockResult>>()
        .map(|JsonObject(block)| block);

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
            hex_to_u64(value)
                .ok_or_else(|| E::custom(format_args!("invalid hex quantity: {value}")))
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
    use crate::transport::{HttpTarget, build_client, build_connectors};
    use crate::upstream::HttpUpstream;
    use bytes::Bytes;
    use http::{Request, Response, StatusCode};
    use http_body_util::{BodyExt, Full};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::Duration;

    #[tokio::test]
    async fn el_transport_fallback_preserves_verdict_and_error_policy() {
        use hyper::body::Incoming;
        use hyper::service::service_fn;
        use hyper_util::rt::{TokioExecutor, TokioIo};
        use hyper_util::server::conn::auto;

        let (https, _) = build_connectors(false);
        let default_client = build_client(https.clone());
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
            let addr = listener.local_addr().unwrap();
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
                                tokio::time::sleep(Duration::from_millis(500)).await;
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
            let target = HttpTarget::new_el(
                HttpUpstream::parse("--el-http-url", &format!("http://{addr}")).unwrap(),
                false,
                &https,
            );
            target.transport.set_confirmed_cleartext(prefer_h2);
            let el = ProbeTarget::from_target(&default_client, &target, Duration::from_millis(150));
            let result = el_batch_detect(el).await;
            assert_eq!(target.transport.uses_h2c_client(), expected_h2);
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
                assert_eq!(batch.syncing.unwrap(), ElSyncingResult::Synced);
                assert_eq!(batch.peers.unwrap(), Some(0x40));
            } else {
                assert!(matches!(result.unwrap_err(), HealthError::Decode(_)));
            }
            server.abort();
        }
    }

    #[tokio::test]
    async fn el_transport_verify_once_heals_restart_race_and_settles_on_h1_only() {
        use hyper::body::Incoming;
        use hyper::service::service_fn;
        use hyper_util::rt::{TokioExecutor, TokioIo};
        use hyper_util::server::conn::auto;

        let valid = br#"[{"id":1,"result":false},{"id":2,"result":"0x40"},{"id":3,"result":{"number":"0x1","timestamp":"0x2"}}]"#;
        let accept_h2 = Arc::new(AtomicBool::new(false));
        let hang_h1 = Arc::new(AtomicBool::new(false));
        let attempts = Arc::new([AtomicUsize::new(0), AtomicUsize::new(0)]);
        let accept_h2_srv = accept_h2.clone();
        let hang_h1_srv = hang_h1.clone();
        let counts = attempts.clone();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                let accept_h2_srv = accept_h2_srv.clone();
                let hang_h1_srv = hang_h1_srv.clone();
                let counts = counts.clone();
                tokio::spawn(async move {
                    let mut preface = [0; 3];
                    let is_h2 =
                        stream.peek(&mut preface).await.unwrap_or(0) == 3 && &preface == b"PRI";
                    counts[usize::from(is_h2)].fetch_add(1, Ordering::SeqCst);
                    if is_h2 && !accept_h2_srv.load(Ordering::SeqCst) {
                        return;
                    }
                    let svc = service_fn(move |req: Request<Incoming>| {
                        let hang_h1_srv = hang_h1_srv.clone();
                        async move {
                            req.into_body().collect().await.unwrap();
                            if !is_h2 && hang_h1_srv.load(Ordering::SeqCst) {
                                tokio::time::sleep(Duration::from_millis(500)).await;
                            }
                            Ok::<_, std::convert::Infallible>(
                                Response::builder()
                                    .status(StatusCode::OK)
                                    .body(Full::new(Bytes::from_static(valid)))
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

        let (https, _) = build_connectors(false);
        let target = HttpTarget::new_el(
            HttpUpstream::parse("--el-http-url", &format!("http://{addr}")).unwrap(),
            false,
            &https,
        );
        let default_client = build_client(https);
        let el = ProbeTarget::from_target(&default_client, &target, Duration::from_millis(150));

        // Case A: h1-only upstream -> Initial -> H1VerifyOnce -> H1Confirmed.
        assert_eq!(target.transport.label(), "h2c");
        assert_eq!(
            target.transport.startup_proto_label(),
            "HTTP/2 (h2c, unverified)"
        );
        assert!(el_batch_detect(el).await.unwrap().syncing.is_ok());
        assert_eq!(target.transport.label(), "http/1.1");
        assert!(el_batch_detect(el).await.unwrap().syncing.is_ok());
        assert_eq!(target.transport.label(), "http/1.1");

        // Once settled in `H1Confirmed`, subsequent healthy polls stay on HTTP/1.1
        // without re-probing `h2c` even if the server later accepts `h2c`.
        let h2_before_settled = attempts[1].load(Ordering::SeqCst);
        accept_h2.store(true, Ordering::SeqCst);
        assert!(el_batch_detect(el).await.unwrap().syncing.is_ok());
        assert_eq!(target.transport.label(), "http/1.1");
        assert_eq!(
            attempts[1].load(Ordering::SeqCst),
            h2_before_settled,
            "H1Confirmed must not probe h2c while HTTP/1.1 succeeds"
        );

        // Case B: simulate a confirmed h2c upstream hitting a transient h2c rejection
        // during restart (falling back to H1VerifyOnce), then surviving a dual transport
        // failure (preserving active H1's Timeout rather than speculative h2c's Transport
        // without leaving H1VerifyOnce), and self-healing back to Confirmed once h2c returns.
        accept_h2.store(false, Ordering::SeqCst);
        target.transport.set_confirmed_cleartext(true);
        assert_eq!(target.transport.startup_proto_label(), "HTTP/2 (h2c)");
        assert!(el_batch_detect(el).await.unwrap().syncing.is_ok());
        assert_eq!(target.transport.label(), "http/1.1"); // fell back to H1VerifyOnce

        let h2_before_dual_fail = attempts[1].load(Ordering::SeqCst);
        hang_h1.store(true, Ordering::SeqCst);
        assert_eq!(el_batch_detect(el).await.unwrap_err(), HealthError::Timeout);
        assert_eq!(target.transport.label(), "http/1.1");
        assert_eq!(attempts[1].load(Ordering::SeqCst), h2_before_dual_fail + 1);

        hang_h1.store(false, Ordering::SeqCst);
        accept_h2.store(true, Ordering::SeqCst);
        assert!(el_batch_detect(el).await.unwrap().syncing.is_ok());
        assert_eq!(target.transport.label(), "h2c"); // self-healed back to Confirmed!

        server.abort();
    }

    #[tokio::test]
    async fn body_timeout_after_headers_does_not_fallback_or_downgrade_h2c() {
        use hyper::body::Incoming;
        use hyper::service::service_fn;
        use hyper_util::rt::{TokioExecutor, TokioIo};
        use hyper_util::server::conn::auto;

        struct StalledBody;
        impl hyper::body::Body for StalledBody {
            type Data = Bytes;
            type Error = std::convert::Infallible;
            fn poll_frame(
                self: std::pin::Pin<&mut Self>,
                _cx: &mut std::task::Context<'_>,
            ) -> std::task::Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>>
            {
                std::task::Poll::Pending
            }
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let attempts = Arc::new([AtomicUsize::new(0), AtomicUsize::new(0)]);
        let counts = attempts.clone();
        let server = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                let counts = counts.clone();
                tokio::spawn(async move {
                    let mut preface = [0; 3];
                    let h2 =
                        stream.peek(&mut preface).await.unwrap_or(0) == 3 && &preface == b"PRI";
                    counts[usize::from(h2)].fetch_add(1, Ordering::Relaxed);
                    let svc = service_fn(|req: Request<Incoming>| async move {
                        let _ = req.into_body().collect().await;
                        Ok::<_, std::convert::Infallible>(
                            Response::builder()
                                .status(StatusCode::OK)
                                .body(StalledBody)
                                .unwrap(),
                        )
                    });
                    let _ = auto::Builder::new(TokioExecutor::new())
                        .serve_connection(TokioIo::new(stream), svc)
                        .await;
                });
            }
        });

        let (https, _) = build_connectors(false);
        let target = HttpTarget::new_el(
            HttpUpstream::parse("--el-http-url", &format!("http://{addr}")).unwrap(),
            false,
            &https,
        );
        let default_client = build_client(https);
        target.transport.set_confirmed_cleartext(true);
        let el = ProbeTarget::from_target(&default_client, &target, Duration::from_millis(200));
        let err = el_batch_detect(el).await.unwrap_err();
        assert_eq!(err, HealthError::BodyTimeout);
        assert!(target.transport.uses_h2c_client());
        assert_eq!(attempts[1].load(Ordering::Relaxed), 1, "h2c tried once");
        assert_eq!(
            attempts[0].load(Ordering::Relaxed),
            0,
            "h1 fallback must not run after 200 headers arrived"
        );
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
            assert_eq!(parsed.syncing.unwrap(), ElSyncingResult::Synced);
            assert_eq!(parsed.peers.unwrap(), Some(0x40));
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
        assert_eq!(parsed.syncing.unwrap(), ElSyncingResult::Synced);
        assert_eq!(parsed.block.unwrap().number, 0x42);
        assert!(matches!(parsed.peers, Err(HealthError::Rpc(_))));

        // JSON-RPC `-32601` (Method not found) on `net_peerCount` is treated as unsupported (`Ok(None)`), not an error.
        let method_not_found = parse_el_batch(br#"[{"id":1,"result":false},{"id":2,"error":{"code":-32601,"message":"Method not found"}},{"id":3,"result":{"number":"0x0","timestamp":"0x0"}}]"#).unwrap();
        assert_eq!(method_not_found.peers.unwrap(), None);
        assert_eq!(method_not_found.block.unwrap().number, 0);
    }

    #[test]
    fn single_quantity_parser_enforces_object_id_and_hex_rules() {
        assert_eq!(
            parse_el_quantity(br#"{"jsonrpc":"2.0","id":1,"result":"0x0"}"#).unwrap(),
            0
        );
        assert_eq!(
            parse_el_quantity(br#"{"jsonrpc":"2.0","id":1,"result":"0xa4b1"}"#).unwrap(),
            42_161
        );
        for bad in [
            r#"{"jsonrpc":"2.0","id":2,"result":"0x1"}"#,
            r#"{"jsonrpc":"2.0","id":1,"result":null}"#,
            r#"{"jsonrpc":"2.0","id":1,"result":"0x00"}"#,
            r#"[1,"0x1"]"#,
        ] {
            assert!(
                matches!(
                    parse_el_quantity(bad.as_bytes()),
                    Err(HealthError::Decode(_))
                ),
                "{bad}"
            );
        }
        assert!(matches!(
            parse_el_quantity(
                br#"{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"boom"}}"#
            ),
            Err(HealthError::Rpc(_))
        ));
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
