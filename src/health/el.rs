//! Execution Layer JSON-RPC batch (`eth_syncing`, `net_peerCount`, `eth_getBlockByNumber`)
//! and hex-quantity (`eth_blockNumber`, `eth_chainId`) probe codecs.

use crate::rpc::JsonObject;

use super::fetch::{HealthError, ProbeTarget};

const EL_BATCH_REQ: &[u8] = br#"[{"jsonrpc":"2.0","method":"eth_syncing","params":[],"id":1},{"jsonrpc":"2.0","method":"net_peerCount","params":[],"id":2},{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["latest",false],"id":3}]"#;
const EL_REF_BATCH_REQ: &[u8] = br#"[{"jsonrpc":"2.0","method":"eth_chainId","params":[],"id":1},{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":2}]"#;
const EL_BLOCK_NUMBER_REQ: &[u8] =
    br#"{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}"#;
const EL_CHAIN_ID_REQ: &[u8] = br#"{"jsonrpc":"2.0","method":"eth_chainId","params":[],"id":1}"#;
const JSONRPC_METHOD_NOT_FOUND: i64 = -32601;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ElSyncingResult {
    /// `eth_syncing` returned `false`.
    Synced,
    /// `eth_syncing` returned `true` or an L2/client-specific object without both progress fields.
    SyncingBool,
    /// `eth_syncing` returned a progress object with both `currentBlock` and `highestBlock`.
    SyncingProgress { current_block: u64, distance: u64 },
}

impl<'de> serde::Deserialize<'de> for ElSyncingResult {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(serde::Deserialize)]
        struct SyncingFields {
            #[serde(rename = "currentBlock", default)]
            current_block: Option<HexQuantity>,
            #[serde(rename = "highestBlock", default)]
            highest_block: Option<HexQuantity>,
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
            fn visit_str<E: serde::de::Error>(self, _v: &str) -> Result<Self::Value, E> {
                // Reject strings without echoing them into `serde_json::Error` so a multi-MiB
                // malformed payload cannot allocate an unbounded error buffer.
                Err(E::invalid_type(
                    serde::de::Unexpected::Other("string"),
                    &self,
                ))
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                map: A,
            ) -> Result<Self::Value, A::Error> {
                let fields = <SyncingFields as serde::Deserialize>::deserialize(
                    serde::de::value::MapAccessDeserializer::new(map),
                )?;
                Ok(match (fields.current_block, fields.highest_block) {
                    (Some(HexQuantity(current_block)), Some(HexQuantity(highest_block))) => {
                        ElSyncingResult::SyncingProgress {
                            current_block,
                            distance: highest_block.saturating_sub(current_block),
                        }
                    }
                    // Any JSON object returned by `eth_syncing` indicates syncing even when
                    // L2 clients (e.g. Arbitrum Nitro) omit `currentBlock`/`highestBlock`.
                    _ => ElSyncingResult::SyncingBool,
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

impl ElBlockResult {
    pub(super) const fn age(self, now: u64) -> u64 {
        now.saturating_sub(self.timestamp)
    }
}

#[cfg_attr(test, derive(Debug))]
pub(super) struct ElBatchResult {
    pub(super) syncing: Result<ElSyncingResult, HealthError>,
    pub(super) peers: Result<Option<u64>, HealthError>,
    pub(super) block: Result<ElBlockResult, HealthError>,
}

/// `i64` JSON decoder that rejects string values without echoing them into
/// `serde_json::Error`, bounding error allocations against multi-MiB EL batch payloads.
#[derive(Clone, Copy, PartialEq, Eq)]
struct BoundedI64(i64);

impl<'de> serde::Deserialize<'de> for BoundedI64 {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl serde::de::Visitor<'_> for Visitor {
            type Value = BoundedI64;

            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a 64-bit signed integer")
            }

            fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<Self::Value, E> {
                Ok(BoundedI64(v))
            }

            fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<Self::Value, E> {
                i64::try_from(v)
                    .map(BoundedI64)
                    .map_err(|_| E::invalid_value(serde::de::Unexpected::Unsigned(v), &self))
            }

            fn visit_str<E: serde::de::Error>(self, _v: &str) -> Result<Self::Value, E> {
                Err(E::invalid_type(
                    serde::de::Unexpected::Other("string"),
                    &self,
                ))
            }
        }
        deserializer.deserialize_any(Visitor)
    }
}

#[derive(serde::Deserialize)]
struct RpcResponse<'a> {
    #[serde(default)]
    id: Option<BoundedI64>,
    #[serde(borrow)]
    result: Option<&'a serde_json::value::RawValue>,
    #[serde(borrow)]
    error: Option<&'a serde_json::value::RawValue>,
}

impl RpcResponse<'_> {
    const EMPTY: Self = Self {
        id: None,
        result: None,
        error: None,
    };

    fn decode<T: serde::de::DeserializeOwned>(self) -> Result<T, HealthError> {
        if let Some(err) = self.error {
            return Err(HealthError::rpc_truncated(err.get()));
        }
        let raw = self
            .result
            .ok_or_else(|| HealthError::decode_msg("missing result"))?;
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

/// Query `(eth_chainId, eth_blockNumber)` in a single 2-element JSON-RPC batch so
/// reference endpoints use one HTTP round-trip and stay atomic behind load balancers.
pub(super) async fn el_chain_and_block(el: ProbeTarget<'_>) -> Result<(u64, u64), HealthError> {
    let body = el.post_json(EL_REF_BATCH_REQ).await?;
    parse_el_ref_batch(&body)
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
    if resp.id.is_none()
        && let Some(err) = resp.error
    {
        return Err(HealthError::rpc_truncated(err.get()));
    }
    if resp.id != Some(BoundedI64(1)) {
        return Err(HealthError::decode_msg("unexpected response id"));
    }
    resp.decode::<HexQuantity>().map(|HexQuantity(n)| n)
}

#[derive(serde::Deserialize)]
struct RpcErrorEnvelope<'a> {
    #[serde(borrow)]
    error: Option<&'a serde_json::value::RawValue>,
}

/// Parse a fixed-size JSON-RPC batch with response IDs `1..=N` in any order,
/// surfacing top-level `{ "error": ... }` replies (e.g. rate limits or
/// unsupported batching) as [`HealthError::Rpc`] instead of an opaque decode error.
fn parse_rpc_batch<'a, const N: usize>(body: &'a [u8]) -> Result<[RpcResponse<'a>; N], HealthError>
where
    [JsonObject<RpcResponse<'a>>; N]: serde::Deserialize<'a>,
{
    let start = body
        .iter()
        .position(|&b| !matches!(b, b' ' | b'\t' | b'\r' | b'\n'))
        .unwrap_or(body.len());
    let trimmed = &body[start..];
    if trimmed.first() == Some(&b'{') {
        return match serde_json::from_slice::<JsonObject<RpcErrorEnvelope<'_>>>(trimmed) {
            Ok(JsonObject(RpcErrorEnvelope { error: Some(err) })) => {
                Err(HealthError::rpc_truncated(err.get()))
            }
            Ok(JsonObject(RpcErrorEnvelope { error: None })) => {
                Err(HealthError::decode_msg("expected JSON-RPC batch array"))
            }
            Err(err) => Err(HealthError::decode(err)),
        };
    }
    if trimmed.first() != Some(&b'[') {
        return Err(HealthError::decode_msg("expected JSON-RPC batch array"));
    }

    let arr: [JsonObject<RpcResponse<'a>>; N] =
        serde_json::from_slice(trimmed).map_err(HealthError::decode)?;
    let mut slots = [const { RpcResponse::EMPTY }; N];
    let mut seen = [false; N];
    for JsonObject(resp) in arr {
        let Some(idx) = resp
            .id
            .and_then(|BoundedI64(id)| id.checked_sub(1))
            .and_then(|i| usize::try_from(i).ok())
            .filter(|&i| i < N)
        else {
            return Err(HealthError::decode_msg(
                "duplicate or unexpected response id",
            ));
        };
        if std::mem::replace(&mut seen[idx], true) {
            return Err(HealthError::decode_msg(
                "duplicate or unexpected response id",
            ));
        }
        slots[idx] = resp;
    }
    Ok(slots)
}

fn parse_el_ref_batch(body: &[u8]) -> Result<(u64, u64), HealthError> {
    let [r_chain_id, r_block] = parse_rpc_batch(body)?;
    let HexQuantity(chain_id) = r_chain_id.decode()?;
    let HexQuantity(block) = r_block.decode()?;
    Ok((chain_id, block))
}

#[derive(serde::Deserialize)]
struct RpcErrorCode {
    code: BoundedI64,
}

fn is_method_not_found(err_raw: &str) -> bool {
    serde_json::from_str::<JsonObject<RpcErrorCode>>(err_raw)
        .is_ok_and(|JsonObject(e)| e.code.0 == JSONRPC_METHOD_NOT_FOUND)
}

fn parse_el_batch(body: &[u8]) -> Result<ElBatchResult, HealthError> {
    let [r_syncing, r_peers, r_block] = parse_rpc_batch(body)?;

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
            hex_to_u64(value).ok_or_else(|| E::custom("invalid hex quantity"))
        }
    }
    deserializer.deserialize_str(Hex)
}

fn hex_to_u64(s: &str) -> Option<u64> {
    let digits = s.strip_prefix("0x")?;
    if digits.is_empty()
        || digits.len() > 16
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

    #[test]
    fn el_syncing_supports_l2_objects_without_block_progress_and_rejects_malformed_hex() {
        assert_eq!(
            serde_json::from_str::<ElSyncingResult>("false").unwrap(),
            ElSyncingResult::Synced
        );
        assert_eq!(
            serde_json::from_str::<ElSyncingResult>("true").unwrap(),
            ElSyncingResult::SyncingBool
        );
        assert_eq!(
            serde_json::from_str::<ElSyncingResult>(
                r#"{"currentBlock":"0x10","highestBlock":"0x1a"}"#
            )
            .unwrap(),
            ElSyncingResult::SyncingProgress {
                current_block: 16,
                distance: 10,
            }
        );
        // Arbitrum Nitro / early-stage sync objects omitting standard progress keys still classify as syncing.
        for l2_obj in [
            r#"{"batchSeen":100,"blockNum":200}"#,
            r#"{"currentBlock":"0x10"}"#,
            r#"{}"#,
        ] {
            assert_eq!(
                serde_json::from_str::<ElSyncingResult>(l2_obj).unwrap(),
                ElSyncingResult::SyncingBool,
                "{l2_obj}"
            );
        }
        for bad in [
            r#"["0x1","0x2"]"#,
            r#"{"currentBlock":"invalid","highestBlock":"0x2"}"#,
            r#"{"currentBlock":42,"highestBlock":"0x2"}"#,
        ] {
            assert!(
                serde_json::from_str::<ElSyncingResult>(bad).is_err(),
                "{bad}"
            );
        }
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
    fn el_ref_batch_decodes_chain_id_and_block_in_either_order() {
        assert_eq!(
            parse_el_ref_batch(br#"[{"id":2,"result":"0x64"},{"id":1,"result":"0x1"}]"#).unwrap(),
            (1, 100)
        );
        for bad in [
            r#"[{"id":1,"result":"0x1"},{"id":1,"result":"0x64"}]"#,
            r#"[{"id":1,"result":"0x1"},{"id":3,"result":"0x64"}]"#,
            r#"[{"id":1,"result":"0x1"}]"#,
        ] {
            assert!(
                matches!(
                    parse_el_ref_batch(bad.as_bytes()),
                    Err(HealthError::Decode(_))
                ),
                "{bad}"
            );
        }
        assert!(matches!(
            parse_el_ref_batch(
                br#"[{"id":1,"result":"0x1"},{"id":2,"error":{"code":-32000,"message":"down"}}]"#
            ),
            Err(HealthError::Rpc(_))
        ));
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
        assert!(serde_json::from_str::<BoundedI64>("9223372036854775808").is_err());
        assert!(serde_json::from_str::<BoundedI64>("18446744073709519015").is_err());
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

    #[test]
    fn batch_parser_surfaces_top_level_rpc_error_and_truncates_oversized_hex_decode() {
        let rate_limited =
            br#"  {"jsonrpc":"2.0","id":null,"error":{"code":-32005,"message":"rate limited"}}"#;
        for err in [
            parse_el_batch(rate_limited).unwrap_err(),
            parse_el_ref_batch(rate_limited).unwrap_err(),
            parse_el_quantity(rate_limited).unwrap_err(),
        ] {
            let HealthError::Rpc(detail) = err else {
                panic!("expected Rpc error, got {err:?}");
            };
            assert!(detail.contains("-32005"), "{detail}");
        }
        assert!(matches!(
            parse_el_quantity(br#"{"jsonrpc":"2.0","id":null,"result":"0x1"}"#),
            Err(HealthError::Decode(_))
        ));
        assert!(matches!(
            parse_el_quantity(br#"{"jsonrpc":"2.0","id":2,"error":{"code":-32005}}"#),
            Err(HealthError::Decode(_))
        ));

        for non_error_obj in [
            br#"{"jsonrpc":"2.0","id":null,"error":null}"#.as_slice(),
            br#"{"jsonrpc":"2.0","id":1,"result":"0x1"}"#.as_slice(),
            br#"{"truncated":"#.as_slice(),
        ] {
            assert!(
                matches!(parse_el_batch(non_error_obj), Err(HealthError::Decode(_))),
                "{}",
                String::from_utf8_lossy(non_error_obj)
            );
        }

        // Oversized invalid hex string (2 KiB) must not echo the payload into serde_json::Error.
        let huge = "é".repeat(1024);
        let payload = format!(r#"{{"jsonrpc":"2.0","id":1,"result":"{huge}"}}"#);
        let HealthError::Decode(msg) = parse_el_quantity(payload.as_bytes()).unwrap_err() else {
            panic!("expected Decode error");
        };
        assert!(msg.len() <= 128, "decode error bloated: len={}", msg.len());
        assert!(!msg.contains("ééé"), "decode error echoed payload: {msg}");

        // Oversized string in `eth_syncing` must not echo the payload into serde_json::Error.
        let syncing_str = format!(r#""{huge}""#);
        let err = serde_json::from_str::<ElSyncingResult>(&syncing_str)
            .unwrap_err()
            .to_string();
        assert!(err.len() <= 128, "syncing decode error bloated: {err}");
        assert!(!err.contains("ééé"), "syncing error echoed payload: {err}");

        // Oversized top-level string for a batch response must fail upfront without echoing
        // into transient `serde_json::Error` allocations.
        let HealthError::Decode(top_msg) = parse_el_batch(syncing_str.as_bytes()).unwrap_err()
        else {
            panic!("expected Decode error for oversized top-level batch string");
        };
        assert!(top_msg.len() <= 128, "top-level decode bloated: {top_msg}");
        assert!(
            !top_msg.contains("ééé"),
            "top-level batch error echoed payload: {top_msg}"
        );

        // Oversized strings in `RpcResponse.id` and `RpcErrorCode.code` must not echo
        // into transient `serde_json::Error` allocations.
        let huge_id_batch = format!(
            r#"[{{"jsonrpc":"2.0","id":"{huge}","result":false}},{{"jsonrpc":"2.0","id":2,"result":"0x1"}},{{"jsonrpc":"2.0","id":3,"result":{{"number":"0x1","timestamp":"0x1"}}}}]"#
        );
        let HealthError::Decode(id_msg) = parse_el_batch(huge_id_batch.as_bytes()).unwrap_err()
        else {
            panic!("expected Decode error for oversized string id");
        };
        assert!(id_msg.len() <= 128, "id decode error bloated: {id_msg}");
        assert!(
            !id_msg.contains("ééé"),
            "id decode error echoed payload: {id_msg}"
        );

        let huge_code = format!(r#"{{"code":"{huge}"}}"#);
        let code_err = serde_json::from_str::<JsonObject<RpcErrorCode>>(&huge_code)
            .err()
            .expect("string error code must fail")
            .to_string();
        assert!(
            code_err.len() <= 128,
            "error code decode bloated: {code_err}"
        );
        assert!(
            !code_err.contains("ééé"),
            "error code echoed payload: {code_err}"
        );
        assert!(!is_method_not_found(&huge_code));
    }
}
