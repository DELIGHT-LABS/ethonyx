//! Shared JSON-RPC inspection for HTTP and WebSocket mock requests.
//! Mixed batches are forwarded intact; only entirely local batches are handled.

use serde::de::{MapAccess, SeqAccess, Visitor, value::MapAccessDeserializer};
use serde::{Deserialize, Deserializer};
use serde_json::value::RawValue;

/// Serde's derived struct visitors also accept positional sequences. Enforce
/// object shape at each JSON-RPC / Beacon boundary while retaining borrowed data.
pub(crate) struct JsonObject<T>(pub(crate) T);

impl<'de, T: Deserialize<'de>> Deserialize<'de> for JsonObject<T> {
    #[inline]
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Object<T>(std::marker::PhantomData<T>);
        impl<'de, T: Deserialize<'de>> Visitor<'de> for Object<T> {
            type Value = JsonObject<T>;

            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a JSON object")
            }

            fn visit_str<E: serde::de::Error>(self, _v: &str) -> Result<Self::Value, E> {
                // Reject strings without echoing them into `serde_json::Error` so a
                // multi-MiB malformed payload cannot allocate an unbounded error buffer.
                Err(E::invalid_type(
                    serde::de::Unexpected::Other("string"),
                    &self,
                ))
            }

            #[inline]
            fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<Self::Value, A::Error> {
                T::deserialize(MapAccessDeserializer::new(map)).map(JsonObject)
            }
        }
        // Use `deserialize_any` rather than `deserialize_map` so non-object strings route
        // through `visit_str` above instead of `serde_json::peek_invalid_type`.
        deserializer.deserialize_any(Object(std::marker::PhantomData))
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum MockResult {
    Forward,
    Silent,
    Reply(String),
}

struct Request<'a> {
    id: Option<&'a RawValue>,
}

impl<'de> Deserialize<'de> for Request<'de> {
    #[inline]
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Fields<'a> {
            #[serde(rename = "jsonrpc", deserialize_with = "version_2")]
            _version: (),
            #[serde(rename = "method", deserialize_with = "eth_syncing_method")]
            _method: (),
            #[serde(rename = "params", default, deserialize_with = "empty_params")]
            _params: (),
            // Missing is a notification; explicit null still gets a reply.
            #[serde(default, borrow, deserialize_with = "present_id")]
            id: Option<&'a RawValue>,
        }
        // Derive also accepts positional sequences; JSON-RPC requests do not.
        JsonObject::<Fields<'de>>::deserialize(d)
            .map(|JsonObject(fields)| Request { id: fields.id })
    }
}

/// Accept exactly `expected`. Every caller maps a mismatch to "forward upstream", so the
/// error carries no wording of its own.
fn expect_str<'de, D: Deserializer<'de>>(d: D, expected: &'static str) -> Result<(), D::Error> {
    struct Exact(&'static str);
    impl Visitor<'_> for Exact {
        type Value = ();

        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "the string {:?}", self.0)
        }

        fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<(), E> {
            if value == self.0 {
                Ok(())
            } else {
                Err(E::custom("unexpected string"))
            }
        }
    }
    d.deserialize_str(Exact(expected))
}

fn version_2<'de, D: Deserializer<'de>>(d: D) -> Result<(), D::Error> {
    expect_str(d, "2.0")
}

fn eth_syncing_method<'de, D: Deserializer<'de>>(d: D) -> Result<(), D::Error> {
    expect_str(d, "eth_syncing")
}

/// Accept only an empty JSON array (`[]`, optional RFC 8259 whitespace inside) via
/// borrowed `&RawValue` so oversized string/object/array `params` never copy into
/// `serde_json`'s `scratch` or `Error` buffers.
fn empty_params<'de, D: Deserializer<'de>>(d: D) -> Result<(), D::Error> {
    let raw = <&RawValue>::deserialize(d)?;
    let inner = raw
        .get()
        .as_bytes()
        .strip_prefix(b"[")
        .and_then(|s| s.strip_suffix(b"]"))
        .ok_or_else(|| serde::de::Error::custom("expected empty params array"))?;
    if inner
        .iter()
        .all(|b| matches!(b, b' ' | b'\t' | b'\r' | b'\n'))
    {
        Ok(())
    } else {
        Err(serde::de::Error::custom("non-empty params"))
    }
}

fn present_id<'de, D: Deserializer<'de>>(d: D) -> Result<Option<&'de RawValue>, D::Error> {
    let id = <&RawValue>::deserialize(d)?;
    if matches!(id.get().as_bytes().first(), Some(b'"' | b'-' | b'0'..=b'9')) || id.get() == "null"
    {
        Ok(Some(id))
    } else {
        Err(serde::de::Error::custom("invalid JSON-RPC id"))
    }
}

const REPLY_PREFIX: &str = r#"{"jsonrpc":"2.0","id":"#;
const REPLY_SUFFIX: &str = r#","result":false}"#;

fn reply_len(id: &RawValue) -> usize {
    REPLY_PREFIX.len() + id.get().len() + REPLY_SUFFIX.len()
}

fn append_reply(out: &mut String, id: &RawValue) {
    out.push_str(REPLY_PREFIX);
    out.push_str(id.get());
    out.push_str(REPLY_SUFFIX);
}

struct Batch(MockResult);

impl<'de> Deserialize<'de> for Batch {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct Requests;
        impl<'de> Visitor<'de> for Requests {
            type Value = Batch;

            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a JSON-RPC batch")
            }

            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Batch, A::Error> {
                let mut seen = false;
                let mut reply = String::new();
                // Any non-local element fails inside Request::deserialize,
                // stopping at the first upstream request without parsing the tail.
                while let Some(req) = seq.next_element::<Request<'_>>()? {
                    seen = true;
                    if let Some(id) = req.id {
                        // +2 reserves the leading `'['`/',' and the trailing `']'`.
                        reply.reserve(reply_len(id) + 2);
                        reply.push(if reply.is_empty() { '[' } else { ',' });
                        append_reply(&mut reply, id);
                    }
                }
                Ok(Batch(if !seen {
                    MockResult::Forward
                } else if reply.is_empty() {
                    MockResult::Silent
                } else {
                    reply.push(']');
                    MockResult::Reply(reply)
                }))
            }
        }
        d.deserialize_seq(Requests)
    }
}

pub(crate) fn mock_syncing(bytes: &[u8]) -> MockResult {
    let Some(start) = bytes
        .iter()
        .position(|&b| !matches!(b, b' ' | b'\t' | b'\r' | b'\n'))
    else {
        return MockResult::Forward;
    };
    let trimmed = &bytes[start..];
    match trimmed[0] {
        b'[' => {
            // `from_slice` also validates the closing bracket and trailing input
            // before a fully local batch can become `Reply` or `Silent`.
            serde_json::from_slice::<Batch>(trimmed).map_or(MockResult::Forward, |batch| batch.0)
        }
        b'{' => {
            let Ok(req) = serde_json::from_slice::<Request<'_>>(trimmed) else {
                return MockResult::Forward;
            };
            let Some(id) = req.id else {
                return MockResult::Silent;
            };
            let mut reply = String::with_capacity(reply_len(id));
            append_reply(&mut reply, id);
            MockResult::Reply(reply)
        }
        _ => MockResult::Forward,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn method_classification_requires_an_exact_json_string() {
        for (method, local) in [
            (r#""eth_syncing""#, true),
            (r#""eth_\u0073yncing""#, true),
            (r#""\u0065th_syncing""#, true),
            (r#""eth_syncing ""#, false),
            (r#""ETH_SYNCING""#, false),
            (r#""eth_synсing""#, false), // Cyrillic c is not ASCII c.
            (r#""eth_syncing\u0000""#, false),
            (r#""""#, false),
            ("null", false),
            ("true", false),
            ("false", false),
            ("1", false),
            ("[]", false),
            (r#"["eth_syncing"]"#, false),
            (r#"{"eth_syncing":null}"#, false),
        ] {
            let request = format!(r#"{{"jsonrpc":"2.0","method":{method},"id":7}}"#);
            for batch in [false, true] {
                let input = if batch {
                    format!("[{request}]")
                } else {
                    request.clone()
                };
                for padding in [0, 512] {
                    let input = format!("{input}{}", " ".repeat(padding));
                    let expected = if local {
                        let reply = r#"{"jsonrpc":"2.0","id":7,"result":false}"#;
                        MockResult::Reply(if batch {
                            format!("[{reply}]")
                        } else {
                            reply.into()
                        })
                    } else {
                        MockResult::Forward
                    };
                    assert_eq!(mock_syncing(input.as_bytes()), expected, "{input}");
                }
            }
        }
        for input in [
            r#"{"jsonrpc":"2.0","is_syncing":true,"id":7}"#,
            r#"{"jsonrpc":"2.0","method":"eth_syncing","method":"eth_syncing","id":7}"#,
            r#"{"jsonrpc":"2.0","method":"eth_syncing","metho\u0064":"eth_syncing","id":7}"#,
            r#"{"jsonrpc":"2.0","params":["0xdeadbeef","latest"],"method":"eth_syncing","id":7}"#,
            r#"{"jsonrpc":"2.0","params":{},"method":"eth_syncing","id":7}"#,
            r#"{"jsonrpc":"2.0","params":[INVALID"#,
        ] {
            assert_eq!(mock_syncing(input.as_bytes()), MockResult::Forward);
        }
        assert_eq!(
            mock_syncing(
                b"{\"jsonrpc\":\"2.0\",\"params\":[ \t\r\n ],\"method\":\"eth_syncing\",\"id\":7}"
            ),
            MockResult::Reply(r#"{"jsonrpc":"2.0","id":7,"result":false}"#.into())
        );
    }

    #[test]
    fn batch_reply_growth_preserves_raw_ids_and_late_forwarding() {
        let large_id = format!("\"{}\"", "x".repeat(4096));
        for id in [
            "1",
            "null",
            "900719925474099312345",
            r#""é😀""#,
            r#""x\\y\"z""#,
            &large_id,
        ] {
            let request = format!(r#"{{"jsonrpc":"2.0","method":"eth_syncing","id":{id}}}"#);
            let response = format!(r#"{{"jsonrpc":"2.0","id":{id},"result":false}}"#);
            // Cover single, pair and multi-element batch growth for short and long IDs.
            for count in [1, 2, 64] {
                let prefix = vec![request.as_str(); count].join(",");
                let input = format!("[{prefix}]");
                let expected = format!("[{}]", vec![response.as_str(); count].join(","));
                assert_eq!(mock_syncing(input.as_bytes()), MockResult::Reply(expected));
                let mixed =
                    format!(r#"[{prefix},{{"jsonrpc":"2.0","method":"eth_blockNumber","id":2}}]"#);
                assert_eq!(mock_syncing(mixed.as_bytes()), MockResult::Forward);
                assert_eq!(
                    mock_syncing(format!("{input}x").as_bytes()),
                    MockResult::Forward
                );
            }
        }
    }

    #[test]
    fn batch_requests_must_be_objects() {
        let local = r#"{"jsonrpc":"2.0","method":"eth_syncing","id":2}"#;
        for invalid in [
            r#"["2.0","eth_syncing",[],1]"#,
            r#"["2.0","eth_syncing"]"#,
            r#"["2.0","eth_syncing",{},null]"#,
            "[]",
            "null",
            "true",
            "1",
            r#""eth_syncing""#,
        ] {
            for batch in [
                format!("[{invalid}]"),
                format!("[{local},{invalid}]"),
                format!("[{invalid},{local}]"),
                format!("[{local},{invalid},{local}]"),
            ] {
                for padding in [0, 512] {
                    let input = format!("{batch}{}", " ".repeat(padding));
                    assert_eq!(
                        mock_syncing(input.as_bytes()),
                        MockResult::Forward,
                        "{input}"
                    );
                }
            }
        }
    }

    #[test]
    fn local_batches_require_complete_validation() {
        for local in [
            r#"{"jsonrpc":"2.0","method":"eth_syncing","id":null}"#,
            r#"{"jsonrpc":"2.0","method":"eth_syncing"}"#,
        ] {
            for input in [
                format!("[{local}"),
                format!("[{local},]"),
                format!("[{local}]x"),
                format!("[{local}] []"),
                format!("[{local},{{\"jsonrpc\":\"2.0\",\"method\":\"eth_syncing\",\"id\":true}}]"),
            ] {
                assert_eq!(
                    mock_syncing(input.as_bytes()),
                    MockResult::Forward,
                    "{input}"
                );
            }
            let mut invalid_utf8 = format!("[{local}]").into_bytes();
            invalid_utf8.push(0xff);
            assert_eq!(mock_syncing(&invalid_utf8), MockResult::Forward);
        }
    }

    #[test]
    fn batches_forward_at_any_nonlocal_position_and_preserve_reply_ids() {
        let local = r#"{"jsonrpc":"2.0","method":"eth_syncing","id":1}"#;
        let remote = r#"{"jsonrpc":"2.0","method":"eth_blockNumber","id":2}"#;
        for position in [0, 1, 99] {
            let mut requests = vec![local; 100];
            requests[position] = remote;
            let input = format!("[{}]", requests.join(","));
            assert_eq!(mock_syncing(input.as_bytes()), MockResult::Forward);
        }
        let input = br#"[{"jsonrpc":"2.0","method":"eth_syncing"},{"id":900719925474099312345,"method":"eth_\u0073yncing","jsonrpc":"2.0"},{"jsonrpc":"2.0","method":"eth_syncing","id":null},{"jsonrpc":"2.0","method":"eth_syncing"}]"#;
        assert_eq!(mock_syncing(input), MockResult::Reply(
            r#"[{"jsonrpc":"2.0","id":900719925474099312345,"result":false},{"jsonrpc":"2.0","id":null,"result":false}]"#.into()
        ));
    }

    #[test]
    fn preserves_ids_and_decodes_escaped_methods() {
        for id in ["null", "900719925474099312345", r#""a\\b\"c""#, "-1"] {
            let input = format!(r#"{{"jsonrpc":"2.0","method":"eth_\u0073yncing","id":{id}}}"#);
            let MockResult::Reply(reply) = mock_syncing(input.as_bytes()) else {
                panic!("{input}")
            };
            assert!(reply.contains(&format!("\"id\":{id}")));
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(&reply).unwrap()["result"],
                false
            );
        }
        assert!(matches!(
            mock_syncing(br#"{"jsonrpc":"2.0","meth\u006fd":"eth_\u0073yncing","id":1}"#),
            MockResult::Reply(_)
        ));
        assert!(matches!(
            mock_syncing(
                b"{\"jsonrpc\":\"2.\\u0030\",\"method\":\"eth_syncing\",\"params\":[ \t\r\n ],\"id\":0}"
            ),
            MockResult::Reply(_)
        ));
    }

    #[test]
    fn notifications_are_silent_and_batches_omit_them() {
        let notification = r#"{"jsonrpc":"2.0","method":"eth_syncing"}"#;
        assert_eq!(mock_syncing(notification.as_bytes()), MockResult::Silent);
        assert_eq!(
            mock_syncing(format!("[{notification}]").as_bytes()),
            MockResult::Silent
        );
        let batch =
            format!(r#"[{notification},{{"jsonrpc":"2.0","method":"eth_syncing","id":null}}]"#);
        let MockResult::Reply(reply) = mock_syncing(batch.as_bytes()) else {
            panic!()
        };
        let parsed: Vec<serde_json::Value> = serde_json::from_str(&reply).unwrap();
        assert_eq!(parsed.len(), 1);
        assert!(parsed[0]["id"].is_null());
    }

    #[test]
    fn forwards_mixed_invalid_and_unknown_requests_without_rewriting() {
        for input in [
            "[]",
            "null",
            "{}",
            "[",
            "[1]",
            r#"{"jsonrpc":"1.0","method":"eth_syncing","id":1}"#,
            r#"{"jsonrpc":"2.0","method":"eth_syncing","id":true}"#,
            r#"{"jsonrpc":"2.0","method":"eth_syncing","params":null,"id":1}"#,
            r#"{"jsonrpc":"2.0","method":"eth_syncing","params":1,"id":1}"#,
            r#"{"jsonrpc":"2.0","method":"eth_syncing","params":{},"id":1}"#,
            r#"{"jsonrpc":"2.0","method":"eth_syncing","params":{"unexpected":1},"id":1}"#,
            r#"{"jsonrpc":"2.0","method":"eth_syncing","params":[1,2,3],"id":1}"#,
            r#"{"jsonrpc":"2.0","method":"eth_syncing","params":[false],"id":1}"#,
            r#"{"method":"eth_syncing","id":1}"#,
            r#"{"jsonrpc":"2.0","method":"eth_syncing","method":"eth_call","id":1}"#,
            r#"[{"jsonrpc":"2.0","method":"eth_syncing","id":1},{"jsonrpc":"2.0","method":"eth_call","id":2}]"#,
            r#"[{"jsonrpc":"2.0","method":"eth_syncing","id":1},{"jsonrpc":"2.0","method":"eth_syncing","params":[1],"id":2}]"#,
        ] {
            assert_eq!(
                mock_syncing(input.as_bytes()),
                MockResult::Forward,
                "{input}"
            );
        }
    }

    #[test]
    fn oversized_strings_fail_without_echoing_into_serde_errors() {
        let huge = "a".repeat(4096);
        let json_str = format!(r#""{huge}""#);
        let obj_err = serde_json::from_str::<JsonObject<serde::de::IgnoredAny>>(&json_str)
            .err()
            .expect("string must not deserialize as JsonObject")
            .to_string();
        assert!(
            obj_err.len() < 128 && !obj_err.contains("aaaaaaaa"),
            "JsonObject error must not echo string payload, got: {obj_err}"
        );

        let params_json =
            format!(r#"{{"jsonrpc":"2.0","params":"{huge}","method":"eth_syncing","id":1}}"#);
        let params_err = serde_json::from_str::<Request<'_>>(&params_json)
            .err()
            .expect("string params must fail")
            .to_string();
        assert!(
            params_err.len() < 128 && !params_err.contains("aaaaaaaa"),
            "Request params error must not echo string payload, got: {params_err}"
        );
        assert_eq!(mock_syncing(params_json.as_bytes()), MockResult::Forward);
        assert_eq!(
            mock_syncing(format!("[{json_str}]").as_bytes()),
            MockResult::Forward
        );
    }
}
