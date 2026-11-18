//! Consensus Layer Beacon API health probes (`/eth/v1/node/syncing`, `/eth/v1/node/peer_count`).

use http::{Method, Request, Uri};

use crate::response::empty_body;

use super::fetch::{HealthError, JsonObject, ProbeError, ProbeTarget};

#[derive(Clone, Copy, serde::Deserialize)]
pub(super) struct ClStatus {
    #[serde(deserialize_with = "decimal_string")]
    pub(super) head_slot: u64,
    #[serde(deserialize_with = "decimal_string")]
    pub(super) sync_distance: u64,
    pub(super) is_syncing: bool,
    #[serde(default)]
    pub(super) is_optimistic: bool,
    #[serde(default)]
    pub(super) el_offline: bool,
}

#[derive(serde::Deserialize)]
struct ClEnvelope<T> {
    data: JsonObject<T>,
}

pub(super) async fn cl_syncing_status(
    cl: ProbeTarget<'_>,
    uri: &Uri,
) -> Result<ClStatus, ProbeError> {
    cl_get_json(cl, uri, "node/syncing").await
}

pub(super) async fn cl_peer_count(cl: ProbeTarget<'_>, uri: &Uri) -> Result<u64, ProbeError> {
    #[derive(serde::Deserialize)]
    struct Peers {
        #[serde(deserialize_with = "decimal_string")]
        connected: u64,
    }
    let peers: Peers = cl_get_json(cl, uri, "node/peer_count").await?;
    Ok(peers.connected)
}

async fn cl_get_json<T: serde::de::DeserializeOwned>(
    cl: ProbeTarget<'_>,
    uri: &Uri,
    method: &str,
) -> Result<T, ProbeError> {
    let req = Request::builder()
        .method(Method::GET)
        .uri(uri.clone())
        .body(empty_body())
        .map_err(|e| format!("{method}: {e}").into_boxed_str())?;
    let body = cl.fetch_bytes(req).await.map_err(|e| e.during(method))?;
    parse_cl_response(&body).map_err(|e| e.during(method))
}

fn parse_cl_response<T: serde::de::DeserializeOwned>(body: &[u8]) -> Result<T, HealthError> {
    serde_json::from_slice::<JsonObject<ClEnvelope<T>>>(body)
        .map(|JsonObject(envelope)| envelope.data.0)
        .map_err(|e| HealthError::Decode(e.to_string().into_boxed_str()))
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
        assert!(!response.data.0.is_optimistic);
        assert!(!response.data.0.el_offline);

        let flags: ClEnvelope<ClStatus> = serde_json::from_str(
            r#"{"data":{"head_slot":"10","sync_distance":"0","is_syncing":false,"is_optimistic":true,"el_offline":true}}"#,
        )
        .unwrap();
        assert!(flags.data.0.is_optimistic);
        assert!(flags.data.0.el_offline);
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
