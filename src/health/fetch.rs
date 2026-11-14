//! Bounded upstream probe fetch, probe error types, and the JSON object guard.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http::Request;
use http_body_util::BodyExt;

use crate::transport::{HttpTransport, ProxyClient, ResBody};

pub(crate) type ProbeError = Arc<str>;

#[derive(Debug)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub(super) enum HealthError {
    Timeout,
    Transport(ProbeError),
    HttpStatus(http::StatusCode),
    Decode(ProbeError),
    Rpc(ProbeError),
}

impl HealthError {
    pub(super) fn into_probe_error(self) -> ProbeError {
        match self {
            Self::Timeout => Arc::from("timeout"),
            other => Arc::from(other.to_string()),
        }
    }
}

impl fmt::Display for HealthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Timeout => f.write_str("timeout"),
            Self::Transport(s) => write!(f, "transport: {s}"),
            Self::HttpStatus(status) => write!(f, "http {status}"),
            Self::Decode(s) => write!(f, "decode: {s}"),
            Self::Rpc(s) => write!(f, "rpc error: {s}"),
        }
    }
}

/// A hard transport/connection failure or a timeout (vs. an HTTP status or decode
/// error) — the only signal that the chosen HTTP version might be wrong for this
/// upstream. A timeout is included so that if the upstream hangs on the HTTP/2
/// connection preface, we can fall back to HTTP/1.1 if the alternate client succeeds.
/// If both timeout, we do not switch.
pub(super) const fn is_transport(e: &HealthError) -> bool {
    matches!(e, HealthError::Transport(_) | HealthError::Timeout)
}

/// Borrowed transport context shared by EL and CL health probe requests so leaf
/// probe modules stay independent of [`crate::state::AppState`].
#[derive(Clone, Copy)]
pub(super) struct ProbeTarget<'a> {
    pub(super) client: &'a ProxyClient,
    pub(super) transport: &'a HttpTransport,
    pub(super) timeout: Duration,
}

impl ProbeTarget<'_> {
    /// Send a prepared probe request within `self.timeout`, record any TLS
    /// negotiated protocol version as soon as response headers arrive, and return
    /// the body of a successful response.
    pub(super) async fn fetch_bytes(self, req: Request<ResBody>) -> Result<Bytes, HealthError> {
        tokio::time::timeout(self.timeout, async {
            let resp = self
                .client
                .request(req)
                .await
                .map_err(|e| HealthError::Transport(Arc::from(e.to_string())))?;
            self.transport.record_version(resp.version());
            if !resp.status().is_success() {
                return Err(HealthError::HttpStatus(resp.status()));
            }
            http_body_util::Limited::new(resp.into_body(), 10 * 1024 * 1024)
                .collect()
                .await
                .map(|c| c.to_bytes())
                .map_err(|e| HealthError::Decode(Arc::from(e.to_string())))
        })
        .await
        .map_err(|_| HealthError::Timeout)?
    }
}

/// Serde's derived struct visitors also accept positional sequences. Enforce
/// object shape at each upstream boundary while retaining borrowed `RawValue` data.
pub(super) struct JsonObject<T>(pub(super) T);

impl<'de, T: serde::Deserialize<'de>> serde::Deserialize<'de> for JsonObject<T> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Object<T>(std::marker::PhantomData<T>);
        impl<'de, T: serde::Deserialize<'de>> serde::de::Visitor<'de> for Object<T> {
            type Value = JsonObject<T>;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a JSON object")
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                map: A,
            ) -> Result<Self::Value, A::Error> {
                T::deserialize(serde::de::value::MapAccessDeserializer::new(map)).map(JsonObject)
            }
        }
        deserializer.deserialize_map(Object(std::marker::PhantomData))
    }
}
