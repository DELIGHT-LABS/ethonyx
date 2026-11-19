use std::fmt::{self, Write as _};

use bytes::BytesMut;
use http::uri::{Authority, InvalidUri, Parts, PathAndQuery, Scheme};
use http::{HeaderValue, Uri};

use crate::BoxError;

/// Formats a URL for logs or diagnostics with credentials, non-root paths,
/// queries, and fragments redacted so API keys and tokens never leak.
#[derive(Clone, Copy)]
pub(crate) struct RedactedUrl<'a>(pub(crate) &'a str);

fn fmt_redacted_parts(
    f: &mut fmt::Formatter<'_>,
    scheme: &str,
    authority: &Authority,
    path: &str,
    has_query_or_frag: bool,
) -> fmt::Result {
    for b in scheme.bytes() {
        f.write_char(char::from(b.to_ascii_lowercase()))?;
    }
    f.write_str("://")?;
    match authority.as_str().rsplit_once('@') {
        Some((_, host_port)) => {
            f.write_str("<redacted>@")?;
            f.write_str(host_port)?;
        }
        None => f.write_str(authority.as_str())?,
    }
    if !matches!(path, "" | "/") || has_query_or_frag {
        f.write_str("/<redacted>")?;
    }
    Ok(())
}

impl fmt::Display for RedactedUrl<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let raw = self.0.trim();
        let (without_frag, has_frag) = match raw.split_once('#') {
            Some((base, _)) => (base, true),
            None => (raw, false),
        };
        let Ok(uri) = without_frag.parse::<Uri>() else {
            return f.write_str("<invalid-url>");
        };
        let (Some(scheme), Some(authority)) = (uri.scheme_str(), uri.authority()) else {
            return f.write_str("<invalid-url>");
        };
        fmt_redacted_parts(
            f,
            scheme,
            authority,
            uri.path(),
            uri.query().is_some() || has_frag,
        )
    }
}

impl fmt::Debug for RedactedUrl<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "\"{self}\"")
    }
}

/// Formats a pre-validated [`Uri`] (lowercase scheme, validated authority, no fragment)
/// with non-root paths and queries redacted.
#[derive(Clone, Copy)]
pub(crate) struct RedactedUri<'a>(&'a Uri);

impl fmt::Display for RedactedUri<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let scheme = self.0.scheme_str().expect("validated scheme");
        let authority = self.0.authority().expect("validated authority");
        fmt_redacted_parts(
            f,
            scheme,
            authority,
            self.0.path(),
            self.0.query().is_some(),
        )
    }
}

/// Formats an optional [`RedactedUri`] when `Some`, or `"none"` when `None`.
#[derive(Clone, Copy)]
pub(crate) struct RedactedOptUri<'a>(pub(crate) Option<RedactedUri<'a>>);

impl fmt::Display for RedactedOptUri<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            Some(uri) => uri.fmt(f),
            None => f.write_str("none"),
        }
    }
}

/// Parse a canonical decimal TCP port without `+` or leading zeros (`"0"` allowed iff `allow_zero`).
pub(crate) fn parse_canonical_port(s: &str, allow_zero: bool) -> Option<u16> {
    if s.is_empty() {
        return None;
    }
    if s == "0" {
        return allow_zero.then_some(0);
    }
    if s.starts_with('0') || !s.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    s.parse::<u16>().ok()
}

fn invalid_url(flag: &'static str, raw: &str, detail: impl fmt::Display) -> BoxError {
    format!("invalid {flag} ({}): {detail}", RedactedUrl(raw)).into()
}

fn is_valid_ipv6_literal(host: &str) -> bool {
    match host.split_once("%25") {
        Some((addr, zone)) => !zone.is_empty() && addr.parse::<std::net::Ipv6Addr>().is_ok(),
        None => host.parse::<std::net::Ipv6Addr>().is_ok(),
    }
}

/// Shared authority validation for upstream and OTLP URLs: rejects deprecated
/// `user:pass@` userinfo (RFC 9110 §4.2.4 / RFC 6455 §3), empty or malformed
/// bracketed IPv6 hosts, trailing garbage after `]`, and empty, zero, non-canonical,
/// or out-of-range `:port` suffixes without leaking URL secrets.
fn validate_authority(
    flag: &'static str,
    raw: &str,
    authority: &Authority,
) -> Result<(), BoxError> {
    let auth = authority.as_str();
    if auth.contains('@') {
        return Err(invalid_url(flag, raw, "URL userinfo is not supported"));
    }
    let port_str = if let Some(rest) = auth.strip_prefix('[') {
        let Some((_, suffix)) = rest
            .split_once(']')
            .filter(|(host, _)| is_valid_ipv6_literal(host))
        else {
            return Err(invalid_url(flag, raw, "invalid IPv6 host"));
        };
        match suffix.strip_prefix(':') {
            Some(port) => Some(port),
            None if suffix.is_empty() => None,
            None => return Err(invalid_url(flag, raw, "invalid authority")),
        }
    } else {
        if auth.contains(['[', ']']) {
            return Err(invalid_url(flag, raw, "invalid host"));
        }
        let (host, port) = match auth.split_once(':') {
            Some((h, p)) => (h, Some(p)),
            None => (auth, None),
        };
        if host.is_empty() {
            return Err(invalid_url(flag, raw, "empty host"));
        }
        port
    };
    if port_str.is_some_and(|p| parse_canonical_port(p, false).is_none()) {
        return Err(invalid_url(flag, raw, "invalid port"));
    }
    Ok(())
}

/// Pre-classified base path/query shape so `rewrite_uri` avoids repeating string
/// trimming or multi-stage `format!` allocations on every proxied request.
#[derive(Clone, Copy)]
enum BaseTarget {
    /// Root upstream (`/` or empty path, no query): moves inbound `PathAndQuery` zero-copy.
    Root,
    /// Path-prefixed or query-bearing commercial RPC URL.
    Scoped { join_prefix_len: usize },
}

struct Endpoint {
    uri: Uri,
    target: BaseTarget,
    is_tls: bool,
}

impl Endpoint {
    fn parse(
        flag: &'static str,
        raw: &str,
        allowed_schemes: (&'static str, &'static str),
    ) -> Result<Self, BoxError> {
        if raw.contains('#') {
            return Err(invalid_url(flag, raw, "URL must not contain a #fragment"));
        }
        let parsed: Uri = raw
            .parse()
            .map_err(|_| invalid_url(flag, raw, "malformed URI"))?;

        let (plain_scheme, tls_scheme) = allowed_schemes;
        let (scheme, is_tls) = match parsed.scheme_str() {
            Some(s) if s.eq_ignore_ascii_case(plain_scheme) => (
                Scheme::try_from(plain_scheme).expect("valid static scheme"),
                false,
            ),
            Some(s) if s.eq_ignore_ascii_case(tls_scheme) => (
                Scheme::try_from(tls_scheme).expect("valid static scheme"),
                true,
            ),
            _ => {
                return Err(invalid_url(
                    flag,
                    raw,
                    format_args!("scheme must be {plain_scheme} or {tls_scheme}"),
                ));
            }
        };

        let mut parts = parsed.into_parts();
        let Some(authority) = parts.authority.as_ref() else {
            return Err(invalid_url(flag, raw, "missing host"));
        };
        validate_authority(flag, raw, authority)?;

        // Normalize `https://host?query` (`path_and_query == "?query"`) to `"/?query"` once
        // at startup so `uri.path_and_query()` always begins with `'/'` (RFC 9112 origin-form).
        if let Some(pq) = parts.path_and_query.as_ref()
            && !pq.as_str().starts_with('/')
        {
            parts.path_and_query = Some(
                compose_pq("/", "", pq.query(), None)
                    .map_err(|_| invalid_url(flag, raw, "malformed URI"))?,
            );
        }

        parts.scheme = Some(scheme);
        let uri = Uri::from_parts(parts).map_err(|_| invalid_url(flag, raw, "malformed URI"))?;

        let base_path = uri.path();
        let target = if base_path == "/" && uri.query().is_none() {
            BaseTarget::Root
        } else {
            BaseTarget::Scoped {
                join_prefix_len: base_path.trim_end_matches('/').len(),
            }
        };

        Ok(Self {
            uri,
            target,
            is_tls,
        })
    }

    /// Compose the upstream URI in a single pass. Moves `inbound`'s [`PathAndQuery`]
    /// directly into [`Uri::from_parts`] on root-based upstreams (zero heap allocation
    /// when `path_override` is absent or query-less), and assembles one pre-sized
    /// [`BytesMut`] buffer when joining scoped prefixes, queries, or path overrides.
    fn rewrite_uri_with_path(
        &self,
        inbound: Uri,
        path_override: Option<&'static str>,
    ) -> Result<Uri, http::Error> {
        let inbound_pq = inbound.into_parts().path_and_query;
        let inbound_path =
            path_override.unwrap_or_else(|| inbound_pq.as_ref().map_or("", PathAndQuery::path));
        let inbound_query = inbound_pq.as_ref().and_then(PathAndQuery::query);

        if matches!(inbound_path, "" | "/") && inbound_query.is_none() {
            return Ok(self.uri.clone());
        }

        let pq = match self.target {
            BaseTarget::Root => match path_override {
                Some(static_path) if inbound_query.is_none() => {
                    PathAndQuery::from_static(static_path)
                }
                Some(static_path) => compose_pq(static_path, "", None, inbound_query)?,
                None => match inbound_pq {
                    Some(pq) if pq.as_str().starts_with('/') => pq,
                    _ => compose_pq("/", "", None, inbound_query)?,
                },
            },
            BaseTarget::Scoped { join_prefix_len } => {
                let base_path = self.uri.path();
                let (head, tail) = if matches!(inbound_path, "" | "/") {
                    (base_path, "")
                } else {
                    (&base_path[..join_prefix_len], inbound_path)
                };
                compose_pq(head, tail, self.uri.query(), inbound_query)?
            }
        };
        let mut parts = Parts::default();
        parts.scheme = self.uri.scheme().cloned();
        parts.authority = self.uri.authority().cloned();
        parts.path_and_query = Some(pq);
        Uri::from_parts(parts).map_err(http::Error::from)
    }
}

fn compose_pq(
    path_head: &str,
    path_tail: &str,
    base_query: Option<&str>,
    inbound_query: Option<&str>,
) -> Result<PathAndQuery, InvalidUri> {
    let query_cap = match (base_query, inbound_query) {
        (Some(bq), Some(iq)) => {
            1 + bq.len() + usize::from(!bq.is_empty() && !iq.is_empty()) + iq.len()
        }
        (Some(q), None) | (None, Some(q)) => 1 + q.len(),
        (None, None) => 0,
    };
    let mut buf = BytesMut::with_capacity(path_head.len() + path_tail.len() + query_cap);
    buf.extend_from_slice(path_head.as_bytes());
    buf.extend_from_slice(path_tail.as_bytes());
    match (base_query, inbound_query) {
        (Some(bq), Some(iq)) => {
            buf.extend_from_slice(b"?");
            buf.extend_from_slice(bq.as_bytes());
            if !bq.is_empty() && !iq.is_empty() {
                buf.extend_from_slice(b"&");
            }
            buf.extend_from_slice(iq.as_bytes());
        }
        (Some(q), None) | (None, Some(q)) => {
            buf.extend_from_slice(b"?");
            buf.extend_from_slice(q.as_bytes());
        }
        (None, None) => {}
    }
    PathAndQuery::from_maybe_shared(buf.freeze())
}

/// Validated `http://` or `https://` upstream endpoint.
pub(crate) struct HttpUpstream(Endpoint);

impl HttpUpstream {
    pub(crate) fn parse(flag: &'static str, raw: &str) -> Result<Self, BoxError> {
        Endpoint::parse(flag, raw, ("http", "https")).map(Self)
    }

    pub(crate) const fn is_tls(&self) -> bool {
        self.0.is_tls
    }

    pub(crate) const fn uri(&self) -> &Uri {
        &self.0.uri
    }

    pub(crate) const fn redacted(&self) -> RedactedUri<'_> {
        RedactedUri(&self.0.uri)
    }

    /// Compare two validated endpoints after normalizing host case and default
    /// `:80`/`:443` ports while preserving path and query bytes (`/v1` vs `/v1/`).
    pub(crate) fn same_endpoint(&self, other: &Self) -> bool {
        if self.0.is_tls != other.0.is_tls {
            return false;
        }
        let a = self.0.uri.authority().expect("validated authority");
        let b = other.0.uri.authority().expect("validated authority");
        let default_port = if self.0.is_tls { 443 } else { 80 };
        a.host().eq_ignore_ascii_case(b.host())
            && a.port_u16().unwrap_or(default_port) == b.port_u16().unwrap_or(default_port)
            && self.0.uri.path() == other.0.uri.path()
            && self.0.uri.query() == other.0.uri.query()
    }

    pub(crate) fn rewrite_uri(&self, inbound: Uri) -> Result<Uri, http::Error> {
        self.0.rewrite_uri_with_path(inbound, None)
    }

    pub(crate) fn rewrite_uri_with_path(
        &self,
        inbound: Uri,
        path_override: Option<&'static str>,
    ) -> Result<Uri, http::Error> {
        self.0.rewrite_uri_with_path(inbound, path_override)
    }
}

/// Validated `ws://` or `wss://` upstream endpoint with a pre-built `Host` header.
pub(crate) struct WsUpstream {
    endpoint: Endpoint,
    host_header: HeaderValue,
}

impl WsUpstream {
    pub(crate) fn parse(flag: &'static str, raw: &str) -> Result<Self, BoxError> {
        let endpoint = Endpoint::parse(flag, raw, ("ws", "wss"))?;
        let authority = endpoint.uri.authority().expect("validated authority");
        let host_header = HeaderValue::from_str(authority.as_str())
            .map_err(|_| invalid_url(flag, raw, "invalid Host header"))?;
        Ok(Self {
            endpoint,
            host_header,
        })
    }

    pub(crate) const fn is_tls(&self) -> bool {
        self.endpoint.is_tls
    }

    pub(crate) const fn redacted(&self) -> RedactedUri<'_> {
        RedactedUri(&self.endpoint.uri)
    }

    pub(crate) const fn host_header(&self) -> &HeaderValue {
        &self.host_header
    }

    /// `(host, port)` suitable for `TcpStream::connect` (IPv6 square brackets
    /// stripped and default `80` / `443` port resolved from the validated authority).
    pub(crate) fn dial_target(&self) -> (&str, u16) {
        let authority = self.endpoint.uri.authority().expect("validated authority");
        let raw_host = authority.host();
        let host = raw_host
            .strip_prefix('[')
            .and_then(|s| s.strip_suffix(']'))
            .unwrap_or(raw_host);
        let port = authority
            .port_u16()
            .unwrap_or(if self.endpoint.is_tls { 443 } else { 80 });
        (host, port)
    }

    pub(crate) fn rewrite_uri(&self, inbound: Uri) -> Result<Uri, http::Error> {
        self.endpoint.rewrite_uri_with_path(inbound, None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacted_url_masks_userinfo_paths_queries_fragments_and_garbage() {
        for (raw, expected) in [
            ("http://127.0.0.1:8545", "http://127.0.0.1:8545"),
            ("http://127.0.0.1:8545/", "http://127.0.0.1:8545"),
            (
                "https://mainnet.infura.io/v3/super-secret-key",
                "https://mainnet.infura.io/<redacted>",
            ),
            (
                "https://rpc.example.com?api_key=secret",
                "https://rpc.example.com/<redacted>",
            ),
            (
                "https://rpc.example.com/?api_key=secret",
                "https://rpc.example.com/<redacted>",
            ),
            (
                "https://user:p@ssword@rpc.example.com:8545",
                "https://<redacted>@rpc.example.com:8545",
            ),
            (
                "wss://user:secret@ws.example.com/v2/token?foo=bar",
                "wss://<redacted>@ws.example.com/<redacted>",
            ),
            (
                "http://[::1]:8545/secret-path",
                "http://[::1]:8545/<redacted>",
            ),
            (
                "https://rpc.example.com#frag-secret",
                "https://rpc.example.com/<redacted>",
            ),
            (
                " https://rpc.example.com/secret?k=1 ",
                "https://rpc.example.com/<redacted>",
            ),
            ("not-a-url-with-secret", "<invalid-url>"),
            ("/relative/secret", "<invalid-url>"),
        ] {
            assert_eq!(RedactedUrl(raw).to_string(), expected, "raw={raw}");
            assert_eq!(
                format!("{:?}", RedactedUrl(raw)),
                format!("\"{expected}\""),
                "debug raw={raw}"
            );
        }
    }

    #[test]
    fn upstream_parse_normalizes_schemes_and_rejects_userinfo_bad_ports_and_fragments_redacted() {
        let upper_http = HttpUpstream::parse("--el-http-url", "HTTP://127.0.0.1:8545").unwrap();
        assert_eq!(upper_http.uri().scheme_str(), Some("http"));
        assert!(!upper_http.is_tls());
        assert!(
            HttpUpstream::parse("--el-http-url", "HTTPS://rpc.example.com/v3/key")
                .unwrap()
                .is_tls()
        );

        let upper_ws = WsUpstream::parse("--el-ws-url", "WS://127.0.0.1:8546/v2/key").unwrap();
        assert_eq!(upper_ws.host_header(), "127.0.0.1:8546");
        assert_eq!(
            upper_ws.rewrite_uri(Uri::from_static("/")).unwrap(),
            "ws://127.0.0.1:8546/v2/key"
        );
        let upper_wss = WsUpstream::parse("--el-ws-url", "Wss://[::1]:8546").unwrap();
        assert_eq!(upper_wss.host_header(), "[::1]:8546");
        assert_eq!(upper_wss.dial_target(), ("::1", 8546));
        assert_eq!(
            upper_wss.rewrite_uri(Uri::from_static("/")).unwrap(),
            "wss://[::1]:8546"
        );
        let default_wss = WsUpstream::parse("--el-ws-url", "wss://ws.example.com").unwrap();
        assert_eq!(default_wss.dial_target(), ("ws.example.com", 443));
        let default_ws = WsUpstream::parse("--el-ws-url", "ws://ws.example.com").unwrap();
        assert_eq!(default_ws.dial_target(), ("ws.example.com", 80));
        let ipv6_zone =
            HttpUpstream::parse("--el-http-url", "http://[fe80::1%25en0]:8545").unwrap();
        assert_eq!(ipv6_zone.uri(), "http://[fe80::1%25en0]:8545/");

        for bad_http in [
            "ws://user:TOPSECRET@127.0.0.1:8546/APIKEY",
            "http://alice:TOPSECRET@127.0.0.1:8545/APIKEY",
            "https://alice:TOPSECRET@rpc.example.com/v3/APIKEY",
            "http://127.0.0.1:99999/APIKEY",
            "http://127.0.0.1:0/APIKEY",
            "http://127.0.0.1:+8545/APIKEY",
            "http://127.0.0.1:008545/APIKEY",
            "http://127.0.0.1:/APIKEY",
            "http://[::1]:/APIKEY",
            "http://[::1]:70000/APIKEY",
            "http://[]/APIKEY",
            "http://[]:8545/APIKEY",
            "http://[::1]garbage/APIKEY",
            "http://[::1]garbage:99999/APIKEY",
            "http://foo[::1]:8545/APIKEY",
            "http://[fe80::1%25]:8545/APIKEY",
            "http://[not-ipv6]:8545/APIKEY",
            "ftp://127.0.0.1:8545/APIKEY",
            "http:///APIKEY",
            "/v3/APIKEY",
            "https://rpc.example.com/v3/APIKEY#FRAGSECRET",
        ] {
            let err = HttpUpstream::parse("--el-http-url", bad_http)
                .err()
                .unwrap_or_else(|| panic!("must reject invalid HTTP upstream: {bad_http}"))
                .to_string();
            assert!(!err.contains("TOPSECRET"), "leaked userinfo in {err}");
            assert!(!err.contains("APIKEY"), "leaked path in {err}");
            assert!(!err.contains("FRAGSECRET"), "leaked fragment in {err}");
        }

        for bad_ws in [
            "http://127.0.0.1:8545/APIKEY",
            "https://127.0.0.1:8545/APIKEY",
            "ws://user:TOPSECRET@127.0.0.1:8546/APIKEY",
            "wss://user:TOPSECRET@ws.example.com:8546/APIKEY",
            "ws://127.0.0.1:/APIKEY",
            "ws://127.0.0.1:0/APIKEY",
            "ws://127.0.0.1:+8546/APIKEY",
            "ws://127.0.0.1:008546/APIKEY",
            "ws://127.0.0.1:99999/APIKEY",
            "ws://[]:8546/APIKEY",
            "ws://[::1]garbage:8546/APIKEY",
            "ws://foo[::1]:8546/APIKEY",
            "ws:///APIKEY",
            "wss://ws.example.com/APIKEY#FRAGSECRET",
        ] {
            let err = WsUpstream::parse("--el-ws-url", bad_ws)
                .err()
                .unwrap_or_else(|| panic!("must reject invalid WS upstream: {bad_ws}"))
                .to_string();
            assert!(!err.contains("TOPSECRET"), "leaked userinfo in {err}");
            assert!(!err.contains("APIKEY"), "leaked path in {err}");
            assert!(!err.contains("FRAGSECRET"), "leaked fragment in {err}");
        }
    }

    #[test]
    fn rewrite_uri_preserves_paths_queries_and_overrides_for_http_and_ws() {
        let root_ws = WsUpstream::parse("--el-ws-url", "ws://127.0.0.1:8546").unwrap();
        assert_eq!(
            root_ws.rewrite_uri(Uri::from_static("/")).unwrap(),
            "ws://127.0.0.1:8546"
        );
        assert_eq!(
            root_ws
                .rewrite_uri(Uri::from_static("/foo/bar?baz=qux"))
                .unwrap(),
            "ws://127.0.0.1:8546/foo/bar?baz=qux"
        );
        assert_eq!(
            root_ws.rewrite_uri(Uri::from_static("/?q=1")).unwrap(),
            "ws://127.0.0.1:8546/?q=1"
        );
        // Absolute-form query-only inbound URI (`http://host?foo=1`) must still yield
        // a leading `'/'` for valid HTTP/2 `:path` pseudo-headers.
        let root_http = HttpUpstream::parse("--el-http-url", "http://127.0.0.1:8545").unwrap();
        let abs_query: Uri = "http://example.com?foo=1".parse().unwrap();
        let rewritten = root_http.rewrite_uri(abs_query).unwrap();
        assert_eq!(
            rewritten.path_and_query().map(PathAndQuery::as_str),
            Some("/?foo=1")
        );

        let keyed_ws = WsUpstream::parse("--el-ws-url", "ws://127.0.0.1:8546/v2/testkey").unwrap();
        assert_eq!(
            keyed_ws.rewrite_uri(Uri::from_static("/")).unwrap(),
            "ws://127.0.0.1:8546/v2/testkey"
        );

        for (base, inbound, override_path, expected) in [
            ("http://127.0.0.1:8545", "/", None, "/"),
            ("http://127.0.0.1:8545/", "/", None, "/"),
            (
                "http://127.0.0.1:5052",
                "/eth/v1/node/syncing",
                None,
                "/eth/v1/node/syncing",
            ),
            (
                "http://127.0.0.1:5052/",
                "/eth/v1/node/syncing?tag=1",
                None,
                "/eth/v1/node/syncing?tag=1",
            ),
            (
                "http://127.0.0.1:5052",
                "/eth/v1/validator/register_validator?tag=1",
                Some("/eth/v1/builder/validators"),
                "/eth/v1/builder/validators?tag=1",
            ),
            ("http://127.0.0.1:8545/rpc/", "/", None, "/rpc/"),
            ("http://127.0.0.1:8545/rpc/", "/?tag=1", None, "/rpc/?tag=1"),
            (
                "https://rpc.example.com/rpc?key=a%2Fb",
                "/",
                None,
                "/rpc?key=a%2Fb",
            ),
            (
                "https://rpc.example.com/rpc?key=a%2Fb",
                "/?tag=a%2Fb",
                None,
                "/rpc?key=a%2Fb&tag=a%2Fb",
            ),
            (
                "https://rpc.example.com/rpc?key=a%2Fb",
                "/rpc/extra?tag=a%2Fb",
                None,
                "/rpc/rpc/extra?key=a%2Fb&tag=a%2Fb",
            ),
            (
                "http://127.0.0.1:8545/rpc?api_key=secret",
                "/",
                None,
                "/rpc?api_key=secret",
            ),
            (
                "http://127.0.0.1:8545/rpc?api_key=secret",
                "/?tag=1",
                None,
                "/rpc?api_key=secret&tag=1",
            ),
            (
                "http://127.0.0.1:8545/rpc?api_key=secret",
                "/extra",
                None,
                "/rpc/extra?api_key=secret",
            ),
            (
                "http://127.0.0.1:8545/rpc?api_key=secret",
                "/extra?tag=1",
                None,
                "/rpc/extra?api_key=secret&tag=1",
            ),
            (
                "https://rpc.example.com?api_key=secret",
                "/",
                None,
                "/?api_key=secret",
            ),
            (
                "https://rpc.example.com?api_key=secret",
                "/?tag=1",
                None,
                "/?api_key=secret&tag=1",
            ),
            (
                "https://rpc.example.com?api_key=secret",
                "/eth/v1/node/syncing?tag=1",
                None,
                "/eth/v1/node/syncing?api_key=secret&tag=1",
            ),
            (
                "https://relay.example/prefix?api_key=secret",
                "/eth/v1/validator/register_validator?tag=1",
                Some("/eth/v1/builder/validators"),
                "/prefix/eth/v1/builder/validators?api_key=secret&tag=1",
            ),
            (
                "https://relay.example",
                "/eth/v1/validator/register_validator",
                Some("/eth/v1/builder/validators"),
                "/eth/v1/builder/validators",
            ),
        ] {
            let upstream = HttpUpstream::parse("--el-http-url", base).unwrap();
            let inbound_uri: Uri = inbound.parse().unwrap();
            let uri = upstream
                .rewrite_uri_with_path(inbound_uri, override_path)
                .unwrap();
            assert_eq!(
                uri.path_and_query().unwrap().as_str(),
                expected,
                "base={base}, inbound={inbound}, override={override_path:?}"
            );
            let base_uri: Uri = base.parse().unwrap();
            assert_eq!(uri.scheme(), base_uri.scheme(), "base={base}");
            assert_eq!(uri.authority(), base_uri.authority(), "base={base}");
        }
    }
}
