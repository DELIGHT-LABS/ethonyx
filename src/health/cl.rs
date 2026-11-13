//! Consensus Layer Beacon API health probes (`/eth/v1/node/syncing`, `/eth/v1/node/peer_count`).

use std::sync::Arc;

use bytes::Bytes;
use http::{Method, Request, Uri};
use http_body_util::Full;

use crate::state::AppState;
use crate::transport::box_full;

use super::probe::{HealthError, JsonObject, ProbeError, fetch_bytes};

#[derive(Clone, Copy, serde::Deserialize)]
pub(super) struct ClStatus {
    #[serde(deserialize_with = "decimal_string")]
    pub(super) head_slot: u64,
    #[serde(deserialize_with = "decimal_string")]
    pub(super) sync_distance: u64,
    pub(super) is_syncing: bool,
}

#[derive(serde::Deserialize)]
struct ClEnvelope<T> {
    data: JsonObject<T>,
}

pub(super) async fn cl_syncing_status(state: &AppState) -> Result<ClStatus, ProbeError> {
    cl_get_json(state, &state.cl_syncing_uri).await
}

pub(super) async fn cl_peer_count(state: &AppState) -> Result<u64, ProbeError> {
    #[derive(serde::Deserialize)]
    struct Peers {
        #[serde(deserialize_with = "decimal_string")]
        connected: u64,
    }
    let peers: Peers = cl_get_json(state, &state.cl_peer_count_uri).await?;
    Ok(peers.connected)
}

async fn cl_get_json<T: serde::de::DeserializeOwned>(
    state: &AppState,
    uri: &Uri,
) -> Result<T, ProbeError> {
    let req = Request::builder()
        .method(Method::GET)
        .uri(uri.clone())
        .body(box_full(Full::new(Bytes::new())))
        .map_err(|e| Arc::from(e.to_string()))?;
    let body = fetch_bytes(state, req, &state.client, &state.cl.transport)
        .await
        .map_err(HealthError::into_probe_error)?;
    parse_cl_response(&body)
}

fn parse_cl_response<T: serde::de::DeserializeOwned>(body: &[u8]) -> Result<T, ProbeError> {
    serde_json::from_slice::<JsonObject<ClEnvelope<T>>>(body)
        .map(|JsonObject(envelope)| envelope.data.0)
        .map_err(|e| Arc::from(format!("decode: {e}")))
}

fn decimal_string<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<u64, D::Error> {
    struct Decimal;
    impl serde::de::Visitor<'_> for Decimal {
        type Value = u64;
        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("an unsigned decimal string")
        }
        fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<u64, E> {
            value.parse().map_err(E::custom)
        }
    }
    deserializer.deserialize_str(Decimal)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn beacon_envelope_and_data_require_objects() {
        for input in [
            r#"[["123","0",false]]"#,
            r#"{"data":["123","0",false]}"#,
            r#"[{"head_slot":"123","sync_distance":"0","is_syncing":false}]"#,
            r#"{"data":null}"#,
            r#"{"data":{"head_slot":"123","sync_distance":"0","is_syncing":false},"data":{}}"#,
            r#"{"data":{"head_slot":"123","head_slot":"123","sync_distance":"0","is_syncing":false}}"#,
            r#"{"data":{"head_slot":"123","sync_distance":"0","is_syncing":false}}x"#,
        ] {
            assert!(
                parse_cl_response::<ClStatus>(input.as_bytes()).is_err(),
                "{input}"
            );
        }
        assert!(parse_cl_response::<ClStatus>(br#"{"extra":[],"data":{"head_slot":"123","sync_distance":"0","is_syncing":false,"ignored":true}}"#).is_ok());
    }

    #[test]
    fn typed_beacon_response_preserves_decimal_semantics() {
        let response: ClEnvelope<ClStatus> = serde_json::from_str(r#"{"data":{"head_slot":"9412341","sync_distance":"0","is_syncing":false,"extra":{"ignored":true}}}"#).unwrap();
        assert_eq!(response.data.0.head_slot, 9_412_341);
        assert_eq!(response.data.0.sync_distance, 0);
        for value in [
            "42",
            "null",
            r#""invalid""#,
            r#""-1""#,
            r#""18446744073709551616""#,
        ] {
            let text = format!(
                r#"{{"data":{{"head_slot":{value},"sync_distance":"0","is_syncing":false}}}}"#
            );
            assert!(
                serde_json::from_str::<ClEnvelope<ClStatus>>(&text).is_err(),
                "{text}"
            );
        }
        assert!(
            serde_json::from_str::<ClEnvelope<ClStatus>>(
                r#"{"data":{"sync_distance":"0","is_syncing":false}}"#
            )
            .is_err()
        );
    }
}
