use http::header::{
    CONNECTION, CONTENT_LENGTH, HOST, PROXY_AUTHENTICATE, PROXY_AUTHORIZATION, TE, TRAILER,
    TRANSFER_ENCODING, UPGRADE,
};
use http::{HeaderMap, HeaderName};

static HOP_BY_HOP: [HeaderName; 12] = [
    CONNECTION,
    HeaderName::from_static("keep-alive"),
    HeaderName::from_static("proxy-connection"),
    PROXY_AUTHENTICATE,
    PROXY_AUTHORIZATION,
    TE,
    TRAILER,
    HeaderName::from_static("trailers"),
    TRANSFER_ENCODING,
    UPGRADE,
    HOST,
    CONTENT_LENGTH,
];

pub fn strip_hop_by_hop(headers: &mut HeaderMap) {
    // Read every Connection field before removing it: its options nominate
    // additional headers that must not cross this hop (RFC 9110, section 7.6.1).
    let nominated: Vec<HeaderName> = headers
        .get_all(CONNECTION)
        .iter()
        .flat_map(|value| value.as_bytes().split(|&b| b == b','))
        .filter_map(|name| HeaderName::from_bytes(name.trim_ascii()).ok())
        .filter(|name| !HOP_BY_HOP.contains(name) && headers.contains_key(name))
        .collect();
    for name in nominated {
        headers.remove(name);
    }
    for name in &HOP_BY_HOP {
        headers.remove(name);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderValue;

    fn h(pairs: &[(&'static str, &'static str)]) -> HeaderMap<HeaderValue> {
        let mut m = HeaderMap::new();
        for (k, v) in pairs {
            m.append(*k, HeaderValue::from_static(v));
        }
        m
    }

    #[test]
    fn removes_known_hop_by_hop() {
        let mut m = h(&[
            ("host", "example.com"),
            ("connection", "keep-alive"),
            ("content-length", "42"),
            ("transfer-encoding", "chunked"),
            ("upgrade", "websocket"),
            ("content-type", "application/json"),
        ]);
        strip_hop_by_hop(&mut m);
        assert!(!m.contains_key("host"));
        assert!(!m.contains_key("connection"));
        assert!(!m.contains_key("content-length"));
        assert!(!m.contains_key("transfer-encoding"));
        assert!(!m.contains_key("upgrade"));
        assert!(m.contains_key("content-type"));
    }

    #[test]
    fn is_case_insensitive_via_canonical_lookup() {
        let mut m = h(&[("Host", "x"), ("CONNECTION", "close"), ("Close", "1")]);
        strip_hop_by_hop(&mut m);
        assert!(m.is_empty());
    }

    #[test]
    fn empty_map_is_noop() {
        let mut m: HeaderMap<HeaderValue> = HeaderMap::new();
        strip_hop_by_hop(&mut m);
        assert!(m.is_empty());
    }

    #[test]
    fn preserves_unrelated_headers() {
        let mut m = h(&[
            ("authorization", "Bearer x"),
            ("x-custom", "1"),
            ("user-agent", "ethonyx"),
        ]);
        strip_hop_by_hop(&mut m);
        assert_eq!(m.len(), 3);
    }

    #[test]
    fn removes_options_from_every_connection_field() {
        let mut m = h(&[
            ("connection", "keep-alive, X-One, , connection"),
            ("connection", "\tX-TWO\t, X-One"),
            ("x-one", "first"),
            ("x-one", "second"),
            ("x-two", "third"),
            ("proxy-connection", "keep-alive"),
            ("authorization", "Bearer token"),
        ]);
        strip_hop_by_hop(&mut m);
        assert_eq!(m.len(), 1);
        assert_eq!(m["authorization"], "Bearer token");
    }

    #[test]
    fn invalid_options_do_not_hide_valid_options() {
        let mut m = h(&[
            ("connection", "bad option, x-remove, :invalid, ,"),
            ("x-remove", "removed"),
            ("x-keep", "kept"),
        ]);
        m.append(
            CONNECTION,
            HeaderValue::from_bytes(b"\xff, x-last").unwrap(),
        );
        m.insert("x-last", HeaderValue::from_static("removed"));
        strip_hop_by_hop(&mut m);
        assert_eq!(m.len(), 1);
        assert_eq!(m["x-keep"], "kept");
    }
}
