use http::header::{
    CONNECTION, CONTENT_LENGTH, EXPECT, HOST, PROXY_AUTHENTICATE, PROXY_AUTHORIZATION, TE, TRAILER,
    TRANSFER_ENCODING, UPGRADE,
};
use http::{HeaderMap, HeaderName, HeaderValue, StatusCode};

static HOP_BY_HOP_FIXED: [HeaderName; 9] = [
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

fn strip_common_hop_by_hop(headers: &mut HeaderMap) {
    for name in &HOP_BY_HOP_FIXED {
        headers.remove(name);
    }
    remove_connection_and_nominated(headers);
}

/// Strip hop-by-hop headers, `Connection`-nominated headers, `Host`,
/// `Content-Length`, and `Expect: 100-continue` expectations (RFC 9110 §10.1.1)
/// from an inbound request before forwarding upstream (where Hyper recalculates
/// body framing and `Host`/`:authority` from the target URI).
pub(crate) fn strip_hop_by_hop(headers: &mut HeaderMap) {
    if headers.is_empty() {
        return;
    }
    // Remove framing headers before `remove_connection_nominated` so `Connection:
    // content-length` / `transfer-encoding` do not allocate a nominated `Vec`.
    headers.remove(TRANSFER_ENCODING);
    headers.remove(CONTENT_LENGTH);
    strip_common_hop_by_hop(headers);
    // Hyper's HTTP/1.1 server handles `100-continue` on first body poll while its
    // client sends headers and body without waiting for an upstream 100 Continue;
    // preserve unknown custom `Expect` values so upstream can reject them with `417`.
    // Use immutable `get_all` (never `entry`, which reserves capacity even when absent).
    let mut expect = headers.get_all(EXPECT).iter();
    let is_100 = |v: &HeaderValue| {
        v.as_bytes()
            .trim_ascii()
            .eq_ignore_ascii_case(b"100-continue")
    };
    if expect.next().is_some_and(is_100) && expect.all(is_100) {
        headers.remove(EXPECT);
    }
}

/// Sanitize upstream response headers before returning to the downstream client:
/// strips hop-by-hop and `Connection`-nominated headers while preserving a valid
/// `Content-Length` whenever framing is unambiguous (RFC 9110 §8.6, RFC 9112 §6.1–6.2)
/// so HTTP/1.1 responses do not unnecessarily degrade to `Transfer-Encoding: chunked`.
pub(crate) fn sanitize_response_headers(status: StatusCode, headers: &mut HeaderMap) {
    if headers.is_empty() {
        return;
    }
    let drop_content_length = headers.remove(TRANSFER_ENCODING).is_some()
        || status.is_informational()
        || matches!(
            status,
            StatusCode::NO_CONTENT | StatusCode::RESET_CONTENT | StatusCode::NOT_MODIFIED
        );
    if drop_content_length {
        headers.remove(CONTENT_LENGTH);
    }
    strip_common_hop_by_hop(headers);
    if drop_content_length {
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
    let agreed = header_tokens(headers, CONTENT_LENGTH)
        .try_fold(None, |agreed: Option<u64>, part| {
            let n = parse_strict_u64(part)?;
            agreed.is_none_or(|prev| prev == n).then_some(Some(n))
        })
        .flatten();
    if let Some(len) = agreed {
        headers.insert(CONTENT_LENGTH, HeaderValue::from(len));
    } else {
        headers.remove(CONTENT_LENGTH);
    }
}

fn header_tokens(headers: &HeaderMap, name: HeaderName) -> impl Iterator<Item = &[u8]> {
    headers
        .get_all(name)
        .iter()
        .flat_map(|v| v.as_bytes().split(|&b| b == b','))
        .map(<[u8]>::trim_ascii)
}

/// Return `true` if any field line for `name` contains the comma-separated
/// ASCII case-insensitive `token` (ignoring surrounding ASCII whitespace).
pub(crate) fn header_has_token(headers: &HeaderMap, name: HeaderName, token: &str) -> bool {
    let token = token.as_bytes();
    header_tokens(headers, name).any(|part| part.eq_ignore_ascii_case(token))
}

fn remove_connection_and_nominated(headers: &mut HeaderMap) {
    // Read every Connection field before removing `CONNECTION`: its options nominate
    // additional headers that must not cross this hop (RFC 9110, section 7.6.1).
    // Fixed hop-by-hop headers were already removed above, so standard tokens like
    // `keep-alive`, `te`, and `upgrade` miss `contains_key` without allocating `Vec`.
    // Track whether any `Connection` field existed (`split(',')` yields once even for
    // empty values) so HTTP/2 headers skip a redundant `remove(CONNECTION)` lookup.
    let mut has_connection = false;
    let mut nominated = Vec::new();
    for raw in header_tokens(headers, CONNECTION) {
        has_connection = true;
        // Skip empty tokens and `"connection"` explicitly because `CONNECTION` is removed
        // right after this loop, so `Connection: connection` would otherwise hit
        // `contains_key` and allocate `nominated` unnecessarily.
        if raw.is_empty() || raw.eq_ignore_ascii_case(b"connection") {
            continue;
        }
        if let Ok(token) = std::str::from_utf8(raw)
            && headers.contains_key(token)
            && let Ok(name) = HeaderName::from_bytes(token.as_bytes())
        {
            nominated.push(name);
        }
    }
    for name in nominated {
        headers.remove(name);
    }
    if has_connection {
        headers.remove(CONNECTION);
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
    fn strip_hop_by_hop_removes_standard_and_nominated_headers_preserving_others() {
        let mut empty = HeaderMap::new();
        strip_hop_by_hop(&mut empty);
        assert!(empty.is_empty());

        let mut known = h(&[
            ("host", "example.com"),
            ("connection", "keep-alive, TE"),
            ("te", "trailers"),
            ("trailer", "expires"),
            ("trailers", "legacy"),
            ("content-length", "42"),
            ("transfer-encoding", "chunked"),
            ("upgrade", "websocket"),
            ("content-type", "application/json"),
            ("authorization", "Bearer x"),
        ]);
        strip_hop_by_hop(&mut known);
        assert_eq!(known.len(), 2);
        assert_eq!(known["content-type"], "application/json");
        assert_eq!(known["authorization"], "Bearer x");

        let mut upper = h(&[("Host", "x"), ("CONNECTION", "close"), ("Close", "1")]);
        strip_hop_by_hop(&mut upper);
        assert!(upper.is_empty());

        let mut multi = h(&[
            ("connection", "keep-alive, X-One, , connection"),
            ("connection", "\tX-TWO\t, X-One"),
            ("x-one", "first"),
            ("x-one", "second"),
            ("x-two", "third"),
            ("proxy-connection", "keep-alive"),
            ("authorization", "Bearer token"),
        ]);
        strip_hop_by_hop(&mut multi);
        assert_eq!(multi.len(), 1);
        assert_eq!(multi["authorization"], "Bearer token");

        let mut malformed = h(&[
            ("connection", "bad option, x-remove, :invalid, ,"),
            ("x-remove", "removed"),
            ("x-keep", "kept"),
        ]);
        malformed.append(
            CONNECTION,
            HeaderValue::from_bytes(b"\xff, x-last").unwrap(),
        );
        malformed.insert("x-last", HeaderValue::from_static("removed"));
        strip_hop_by_hop(&mut malformed);
        assert_eq!(malformed.len(), 1);
        assert_eq!(malformed["x-keep"], "kept");

        let mut empty_conn = h(&[("connection", ""), ("x-keep", "1")]);
        strip_hop_by_hop(&mut empty_conn);
        assert!(!empty_conn.contains_key("connection"));
        assert_eq!(empty_conn["x-keep"], "1");

        // `Expect: 100-continue` is satisfied locally by Hyper and stripped; unknown
        // custom `Expect` values remain forwarded so upstream can answer `417`.
        let mut expect_100 = h(&[
            ("expect", "100-CONTINUE"),
            ("content-type", "application/json"),
        ]);
        strip_hop_by_hop(&mut expect_100);
        assert!(!expect_100.contains_key(EXPECT));
        assert_eq!(expect_100["content-type"], "application/json");

        // One unknown member, on the same line or another, keeps every `Expect` field.
        for kept in [
            &[("expect", "custom-expectation")][..],
            &[("expect", "100-continue, custom")],
            &[("expect", "100-continue"), ("expect", "custom")],
        ] {
            let mut headers = h(kept);
            strip_hop_by_hop(&mut headers);
            assert_eq!(headers.get_all(EXPECT).iter().count(), kept.len());
        }
        let mut repeated = h(&[("expect", "100-continue"), ("expect", " 100-Continue ")]);
        strip_hop_by_hop(&mut repeated);
        assert!(repeated.is_empty());

        // Probing for `Expect` must not grow a map that is already at capacity.
        let mut full = HeaderMap::new();
        for i in 0..6 {
            full.append(
                HeaderName::from_bytes(format!("x-{i}").as_bytes()).unwrap(),
                HeaderValue::from_static("v"),
            );
        }
        let capacity = full.capacity();
        strip_hop_by_hop(&mut full);
        assert_eq!((full.len(), full.capacity()), (6, capacity));
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

    #[test]
    fn matches_comma_separated_header_tokens_case_insensitively() {
        let mut m = h(&[
            ("connection", "keep-alive, , \tUpGrAdE "),
            ("upgrade", "h2c"),
        ]);
        m.append(
            UPGRADE,
            HeaderValue::from_bytes(b"\xff, WebSocket").unwrap(),
        );
        assert!(header_has_token(&m, CONNECTION, "upgrade"));
        assert!(header_has_token(&m, CONNECTION, "KEEP-ALIVE"));
        assert!(header_has_token(&m, UPGRADE, "websocket"));
        assert!(!header_has_token(&m, CONNECTION, "up"));
        assert!(!header_has_token(&m, CONNECTION, "traceparent"));
    }
}
