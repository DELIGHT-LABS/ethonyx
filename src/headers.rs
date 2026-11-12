use http::header::{
    CONNECTION, CONTENT_LENGTH, HOST, PROXY_AUTHENTICATE, PROXY_AUTHORIZATION, TE, TRAILER,
    TRANSFER_ENCODING, UPGRADE,
};
use http::{HeaderMap, HeaderName, HeaderValue, StatusCode};

static HOP_BY_HOP: [HeaderName; 10] = [
    CONNECTION,
    HeaderName::from_static("keep-alive"),
    HeaderName::from_static("proxy-connection"),
    PROXY_AUTHENTICATE,
    PROXY_AUTHORIZATION,
    TE,
    TRAILER,
    HeaderName::from_static("trailers"),
    UPGRADE,
    HOST,
];

/// Strip hop-by-hop headers, `Connection`-nominated headers, `Host`, and
/// `Content-Length` from an inbound request before forwarding upstream (where
/// Hyper recalculates body framing and `Host`/`:authority` from the target URI).
pub fn strip_hop_by_hop(headers: &mut HeaderMap) {
    if headers.is_empty() {
        return;
    }
    remove_connection_nominated(headers);
    for name in &HOP_BY_HOP {
        headers.remove(name);
    }
    headers.remove(TRANSFER_ENCODING);
    headers.remove(CONTENT_LENGTH);
}

/// Sanitize upstream response headers before returning to the downstream client:
/// strips hop-by-hop and `Connection`-nominated headers while preserving a valid
/// `Content-Length` whenever framing is unambiguous (RFC 9110 §8.6, RFC 9112 §6.1–6.2)
/// so HTTP/1.1 responses do not unnecessarily degrade to `Transfer-Encoding: chunked`.
pub fn sanitize_response_headers(status: StatusCode, headers: &mut HeaderMap) {
    if headers.is_empty() {
        return;
    }
    let had_transfer_encoding = headers.remove(TRANSFER_ENCODING).is_some();
    remove_connection_nominated(headers);
    for name in &HOP_BY_HOP {
        headers.remove(name);
    }

    if had_transfer_encoding
        || status.is_informational()
        || matches!(
            status,
            StatusCode::NO_CONTENT | StatusCode::RESET_CONTENT | StatusCode::NOT_MODIFIED
        )
    {
        headers.remove(CONTENT_LENGTH);
        return;
    }

    let mut values = headers.get_all(CONTENT_LENGTH).iter();
    let Some(first) = values.next() else {
        return;
    };
    let raw = first.as_bytes();
    if values.next().is_none()
        && (raw == b"0" || !raw.starts_with(b"0"))
        && parse_strict_u64(raw).is_some()
    {
        return;
    }

    // Multiple, leading-zero, or whitespace-padded Content-Length headers:
    // accept only if all comma-split tokens across all lines parse to the exact
    // same u64, and collapse into one canonical header value; otherwise remove.
    let mut agreed: Option<u64> = None;
    let mut valid = true;
    for line in headers.get_all(CONTENT_LENGTH) {
        for part in line.as_bytes().split(|&b| b == b',') {
            match parse_strict_u64(part.trim_ascii()) {
                Some(n) if agreed.is_none_or(|prev| prev == n) => agreed = Some(n),
                _ => {
                    valid = false;
                    break;
                }
            }
        }
        if !valid {
            break;
        }
    }
    if valid && let Some(len) = agreed {
        headers.insert(CONTENT_LENGTH, HeaderValue::from(len));
    } else {
        headers.remove(CONTENT_LENGTH);
    }
}

#[cfg(feature = "otel")]
pub(crate) fn is_connection_nominated(headers: &HeaderMap, target: &str) -> bool {
    let target = target.as_bytes();
    headers
        .get_all(CONNECTION)
        .iter()
        .flat_map(|value| value.as_bytes().split(|&b| b == b','))
        .any(|token| token.trim_ascii().eq_ignore_ascii_case(target))
}

fn remove_connection_nominated(headers: &mut HeaderMap) {
    // Read every Connection field before removing it: its options nominate
    // additional headers that must not cross this hop (RFC 9110, section 7.6.1).
    let mut nominated = Vec::new();
    for value in headers.get_all(CONNECTION) {
        for raw in value.as_bytes().split(|&b| b == b',') {
            let Ok(token) = std::str::from_utf8(raw.trim_ascii()) else {
                continue;
            };
            if token.is_empty()
                || token.eq_ignore_ascii_case("keep-alive")
                || token.eq_ignore_ascii_case("upgrade")
                || token.eq_ignore_ascii_case("connection")
            {
                continue;
            }
            if headers.contains_key(token)
                && let Ok(name) = HeaderName::from_bytes(token.as_bytes())
            {
                nominated.push(name);
            }
        }
    }
    for name in nominated {
        headers.remove(name);
    }
}

fn parse_strict_u64(bytes: &[u8]) -> Option<u64> {
    if bytes.is_empty() {
        return None;
    }
    bytes.iter().try_fold(0u64, |acc, &b| {
        let digit = u64::from(b.wrapping_sub(b'0'));
        (digit < 10)
            .then_some(digit)
            .and_then(|d| acc.checked_mul(10)?.checked_add(d))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn sanitize_response_preserves_valid_content_length_and_strips_ambiguous_framing() {
        // 1. Valid Content-Length on 200 and 404 is preserved intact, and "0" is kept.
        for status in [StatusCode::OK, StatusCode::NOT_FOUND] {
            let mut m = h(&[
                ("connection", "keep-alive, x-hop"),
                ("x-hop", "drop"),
                ("content-length", "128"),
                ("content-type", "application/json"),
            ]);
            sanitize_response_headers(status, &mut m);
            assert_eq!(m["content-length"], "128");
            assert_eq!(m["content-type"], "application/json");
            assert!(!m.contains_key("connection"));
            assert!(!m.contains_key("x-hop"));
        }

        // 2. Transfer-Encoding + Content-Length strips both to prevent smuggling.
        let mut chunked = h(&[("transfer-encoding", "chunked"), ("content-length", "128")]);
        sanitize_response_headers(StatusCode::OK, &mut chunked);
        assert!(!chunked.contains_key("transfer-encoding"));
        assert!(!chunked.contains_key("content-length"));

        // 3. 1xx, 204, 205, and 304 strip Content-Length to protect HTTP/1.1 framing
        // and avoid empty-body Content-Length warnings.
        for status in [
            StatusCode::CONTINUE,
            StatusCode::SWITCHING_PROTOCOLS,
            StatusCode::NO_CONTENT,
            StatusCode::RESET_CONTENT,
            StatusCode::NOT_MODIFIED,
        ] {
            let mut m = h(&[("content-length", "42")]);
            sanitize_response_headers(status, &mut m);
            assert!(!m.contains_key("content-length"), "status={status}");
        }

        // 4. Connection-nominated Content-Length is stripped.
        let mut nominated = h(&[("connection", "content-length"), ("content-length", "42")]);
        sanitize_response_headers(StatusCode::OK, &mut nominated);
        assert!(!nominated.contains_key("content-length"));

        // 5. Leading zeros and identical duplicate Content-Length values canonicalize;
        // conflicting or malformed values are stripped.
        let mut leading_zero = h(&[("content-length", "0042")]);
        sanitize_response_headers(StatusCode::OK, &mut leading_zero);
        assert_eq!(leading_zero["content-length"], "42");

        let mut exact_zero = h(&[("content-length", "0")]);
        sanitize_response_headers(StatusCode::OK, &mut exact_zero);
        assert_eq!(exact_zero["content-length"], "0");

        let mut dup_same = h(&[("content-length", "42"), ("content-length", "42, 42")]);
        sanitize_response_headers(StatusCode::OK, &mut dup_same);
        assert_eq!(dup_same.get_all("content-length").iter().count(), 1);
        assert_eq!(dup_same["content-length"], "42");

        for bad in [
            vec![("content-length", "42"), ("content-length", "99")],
            vec![("content-length", "+42")],
            vec![("content-length", "abc")],
            vec![("content-length", "")],
            vec![("content-length", "999999999999999999999")],
        ] {
            let mut m = h(&bad);
            sanitize_response_headers(StatusCode::OK, &mut m);
            assert!(!m.contains_key("content-length"), "bad={bad:?}");
        }
    }
}
