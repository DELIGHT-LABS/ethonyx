//! Consensus Layer Beacon API health probes (`/eth/v1/node/syncing`, `/eth/v1/node/peer_count`).

use crate::rpc::JsonObject;

use super::fetch::{HealthError, ProbeTarget};

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

pub(super) async fn cl_syncing_status(cl: ProbeTarget<'_>) -> Result<ClStatus, HealthError> {
    cl_get_json(cl).await
}

pub(super) async fn cl_peer_count(cl: ProbeTarget<'_>) -> Result<u64, HealthError> {
    #[derive(serde::Deserialize)]
    struct Peers {
        #[serde(deserialize_with = "decimal_string")]
        connected: u64,
    }
    let peers: Peers = cl_get_json(cl).await?;
    Ok(peers.connected)
}

async fn cl_get_json<T: serde::de::DeserializeOwned>(
    cl: ProbeTarget<'_>,
) -> Result<T, HealthError> {
    let body = cl.get_bytes().await?;
    parse_cl_response(&body)
}

fn parse_cl_response<T: serde::de::DeserializeOwned>(body: &[u8]) -> Result<T, HealthError> {
    serde_json::from_slice::<JsonObject<ClEnvelope<T>>>(body)
        .map(|JsonObject(envelope)| envelope.data.0)
        .map_err(HealthError::decode)
}

fn decimal_string<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<u64, D::Error> {
    struct Decimal;
    impl serde::de::Visitor<'_> for Decimal {
        type Value = u64;
        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("an unsigned decimal string")
        }
        fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<u64, E> {
            if !value.bytes().all(|b| b.is_ascii_digit()) {
                return Err(E::custom("invalid unsigned decimal string"));
            }
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
            r#""""#,
            r#""invalid""#,
            r#""+1""#,
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
