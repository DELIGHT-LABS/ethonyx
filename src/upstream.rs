use std::fmt::{self, Write as _};

use bytes::BytesMut;
use http::uri::{Authority, InvalidUri, Parts, PathAndQuery, Scheme};
use http::{HeaderValue, Uri};

use crate::BoxError;

/// Formats a URL for logs or diagnostics with credentials, non-root paths,
/// queries, and fragments redacted so API keys and tokens never leak.
#[derive(Clone, Copy)]
pub(crate) struct RedactedUrl<'a>(pub &'a str);

impl fmt::Display for RedactedUrl<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (without_frag, has_frag) = match self.0.split_once('#') {
            Some((base, _)) => (base, true),
            None => (self.0, false),
        };
        let Ok(uri) = without_frag.parse::<Uri>() else {
            return f.write_str("<invalid-url>");
        };
        let (Some(scheme), Some(authority)) = (uri.scheme_str(), uri.authority()) else {
            return f.write_str("<invalid-url>");
        };

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

        let has_secret_suffix =
            !matches!(uri.path(), "" | "/") || uri.query().is_some() || has_frag;
        if has_secret_suffix {
            f.write_str("/<redacted>")?;
        }
        Ok(())
    }
}

impl fmt::Debug for RedactedUrl<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "\"{self}\"")
    }
}

/// Formats an optional URL as [`RedactedUrl`] when `Some`, or `"none"` when `None`.
#[derive(Clone, Copy)]
pub(crate) struct RedactedOptUrl<'a>(pub Option<&'a str>);

impl fmt::Display for RedactedOptUrl<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            Some(url) => RedactedUrl(url).fmt(f),
            None => f.write_str("none"),
        }
    }
}

/// Shared authority validation for upstream and OTLP URLs: rejects deprecated
/// `user:pass@` userinfo (RFC 9110 §4.2.4 / RFC 6455 §3), empty hosts, and
/// empty, zero, or out-of-range `:port` suffixes without leaking URL secrets.
pub(crate) fn validate_authority(
    flag: &'static str,
    raw: &str,
    authority: &Authority,
) -> Result<(), BoxError> {
    let auth = authority.as_str();
    if auth.contains('@') {
        return Err(format!(
            "invalid {flag} ({}): URL userinfo is not supported",
            RedactedUrl(raw)
        )
        .into());
    }
    if authority.host().is_empty() {
        return Err(format!("invalid {flag} ({}): empty host", RedactedUrl(raw)).into());
    }
    let port_str = match auth.rsplit_once(']') {
        Some((_, rest)) => rest.strip_prefix(':'),
        None => auth.split_once(':').map(|(_, p)| p),
    };
    if port_str.is_some_and(|p| p.is_empty() || authority.port_u16().is_none_or(|n| n == 0)) {
        return Err(format!("invalid {flag} ({}): invalid port", RedactedUrl(raw)).into());
    }
    Ok(())
}

/// Pre-classified base path/query shape so `rewrite_uri` avoids repeating string
/// trimming or multi-stage `format!` allocations on every proxied request.
#[derive(Clone)]
enum BaseTarget {
    /// Root upstream (`/` or empty path, no query): moves inbound `PathAndQuery` zero-copy.
    Root,
    /// Path-prefixed or query-bearing commercial RPC URL.
    Scoped {
        root_path: Box<str>,
        join_prefix: Box<str>,
        query: Option<Box<str>>,
    },
}

#[derive(Clone)]
struct Endpoint {
    uri: Uri,
    scheme: Scheme,
    authority: Authority,
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
            return Err(format!(
                "invalid {flag} ({}): URL must not contain a #fragment",
                RedactedUrl(raw)
            )
            .into());
        }
        let parsed: Uri = raw
            .parse()
            .map_err(|_| format!("invalid {flag} ({}): malformed URI", RedactedUrl(raw)))?;

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
                return Err(format!(
                    "invalid {flag} ({}): scheme must be {plain_scheme} or {tls_scheme}",
                    RedactedUrl(raw)
                )
                .into());
            }
        };

        let mut parts = parsed.into_parts();
        let Some(authority) = parts.authority.take() else {
            return Err(format!("invalid {flag} ({}): missing host", RedactedUrl(raw)).into());
        };
        validate_authority(flag, raw, &authority)?;

        parts.scheme = Some(scheme.clone());
        parts.authority = Some(authority.clone());
        let uri = Uri::from_parts(parts)
            .map_err(|_| format!("invalid {flag} ({}): malformed URI", RedactedUrl(raw)))?;

        let base_path = uri.path();
        let base_query = uri.query();
        let target = if matches!(base_path, "" | "/") && base_query.is_none() {
            BaseTarget::Root
        } else {
            let root_path: Box<str> = if base_path.is_empty() { "/" } else { base_path }.into();
            let join_prefix: Box<str> = root_path.trim_end_matches('/').into();
            BaseTarget::Scoped {
                root_path,
                join_prefix,
                query: base_query.map(Box::from),
            }
        };

        Ok(Self {
            uri,
            scheme,
            authority,
            target,
            is_tls,
        })
    }

    /// Compose the upstream URI by moving `inbound`'s [`PathAndQuery`] directly
    /// into [`Uri::from_parts`] on root-based upstreams (zero heap allocation),
    /// and assembling a single pre-sized [`BytesMut`] buffer when joining scoped
    /// prefixes or queries.
    fn rewrite_uri(
        &self,
        inbound: Uri,
        override_path: Option<&'static str>,
    ) -> Result<Uri, RewriteUriError> {
        let inbound_pq = inbound.into_parts().path_and_query;
        let inbound_path =
            override_path.unwrap_or_else(|| inbound_pq.as_ref().map_or("", PathAndQuery::path));
        if !inbound_path.is_empty() && !inbound_path.starts_with('/') {
            return Err(RewriteUriError::NonOriginTarget);
        }
        let inbound_query = inbound_pq.as_ref().and_then(PathAndQuery::query);

        if override_path.is_none() && matches!(inbound_path, "" | "/") && inbound_query.is_none() {
            return Ok(self.uri.clone());
        }

        let pq = match &self.target {
            BaseTarget::Root => match (override_path, inbound_query) {
                (Some(static_path), None) => PathAndQuery::from_static(static_path),
                (None, _) => inbound_pq.unwrap_or_else(|| PathAndQuery::from_static("/")),
                (Some(static_path), Some(iq)) => compose_pq("", static_path, None, Some(iq))
                    .map_err(|e| RewriteUriError::Http(e.into()))?,
            },
            BaseTarget::Scoped {
                root_path,
                join_prefix,
                query,
            } => {
                let (head, tail) = if override_path.is_none() && matches!(inbound_path, "" | "/") {
                    (root_path.as_ref(), "")
                } else {
                    (join_prefix.as_ref(), inbound_path)
                };
                compose_pq(head, tail, query.as_deref(), inbound_query)
                    .map_err(|e| RewriteUriError::Http(e.into()))?
            }
        };
        self.with_path_and_query(pq)
    }

    fn with_path_and_query(&self, pq: PathAndQuery) -> Result<Uri, RewriteUriError> {
        let mut parts = Parts::default();
        parts.scheme = Some(self.scheme.clone());
        parts.authority = Some(self.authority.clone());
        parts.path_and_query = Some(pq);
        Uri::from_parts(parts).map_err(|e| RewriteUriError::Http(e.into()))
    }
}

#[derive(Debug)]
pub(crate) enum RewriteUriError {
    NonOriginTarget,
    Http(http::Error),
}

impl fmt::Display for RewriteUriError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NonOriginTarget => f.write_str("inbound request target must be origin-form"),
            Self::Http(e) => e.fmt(f),
        }
    }
}

impl std::error::Error for RewriteUriError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::NonOriginTarget => None,
            Self::Http(e) => Some(e),
        }
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
#[derive(Clone)]
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

    pub(crate) fn rewrite_uri(
        &self,
        inbound: Uri,
        override_path: Option<&'static str>,
    ) -> Result<Uri, RewriteUriError> {
        self.0.rewrite_uri(inbound, override_path)
    }
}

/// Validated `ws://` or `wss://` upstream endpoint with a pre-built `Host` header.
#[derive(Clone)]
pub(crate) struct WsUpstream {
    endpoint: Endpoint,
    host_header: HeaderValue,
}

impl WsUpstream {
    pub(crate) fn parse(flag: &'static str, raw: &str) -> Result<Self, BoxError> {
        let endpoint = Endpoint::parse(flag, raw, ("ws", "wss"))?;
        let host_header = HeaderValue::from_str(endpoint.authority.as_str())
            .map_err(|_| format!("invalid {flag} ({}): invalid Host header", RedactedUrl(raw)))?;
        Ok(Self {
            endpoint,
            host_header,
        })
    }

    pub(crate) const fn host_header(&self) -> &HeaderValue {
        &self.host_header
    }

    pub(crate) fn rewrite_uri(&self, inbound: Uri) -> Result<Uri, RewriteUriError> {
        self.endpoint.rewrite_uri(inbound, None)
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
        assert_eq!(
            upper_wss.rewrite_uri(Uri::from_static("/")).unwrap(),
            "wss://[::1]:8546"
        );

        for bad_http in [
            "ws://user:TOPSECRET@127.0.0.1:8546/APIKEY",
            "http://alice:TOPSECRET@127.0.0.1:8545/APIKEY",
            "https://alice:TOPSECRET@rpc.example.com/v3/APIKEY",
            "http://127.0.0.1:99999/APIKEY",
            "http://127.0.0.1:0/APIKEY",
            "http://127.0.0.1:/APIKEY",
            "http://[::1]:/APIKEY",
            "http://[::1]:70000/APIKEY",
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
            "ws://127.0.0.1:99999/APIKEY",
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
    fn rewrite_uri_rejects_asterisk_form_without_corrupting_scoped_paths() {
        let root_http = HttpUpstream::parse("--el-http-url", "http://127.0.0.1:8545").unwrap();
        let scoped_http =
            HttpUpstream::parse("--el-http-url", "https://mainnet.infura.io/v3/SECRET").unwrap();
        assert!(root_http.rewrite_uri(Uri::from_static("*"), None).is_err());
        assert!(
            scoped_http
                .rewrite_uri(Uri::from_static("*"), None)
                .is_err()
        );
    }

    #[test]
    fn rewrite_uri_preserves_paths_queries_and_overrides_for_http_and_ws() {
        let root_http = HttpUpstream::parse("--cl-beacon-url", "http://127.0.0.1:5052").unwrap();
        assert_eq!(
            root_http.rewrite_uri(Uri::from_static("/"), None).unwrap(),
            "http://127.0.0.1:5052/"
        );
        assert_eq!(
            root_http
                .rewrite_uri(Uri::from_static("/eth/v1/node/syncing?foo=1"), None)
                .unwrap(),
            "http://127.0.0.1:5052/eth/v1/node/syncing?foo=1"
        );
        assert_eq!(
            root_http
                .rewrite_uri(
                    Uri::from_static("/eth/v1/validator/register_validator"),
                    Some("/eth/v1/builder/validators")
                )
                .unwrap(),
            "http://127.0.0.1:5052/eth/v1/builder/validators"
        );

        let prefixed_http =
            HttpUpstream::parse("--el-http-url", "https://rpc.example.com/rpc?key=a%2Fb").unwrap();
        assert_eq!(
            prefixed_http
                .rewrite_uri(Uri::from_static("/"), None)
                .unwrap(),
            "https://rpc.example.com/rpc?key=a%2Fb"
        );
        assert_eq!(
            prefixed_http
                .rewrite_uri(Uri::from_static("/?tag=a%2Fb"), None)
                .unwrap(),
            "https://rpc.example.com/rpc?key=a%2Fb&tag=a%2Fb"
        );
        assert_eq!(
            prefixed_http
                .rewrite_uri(Uri::from_static("/rpc/extra?tag=a%2Fb"), None)
                .unwrap(),
            "https://rpc.example.com/rpc/rpc/extra?key=a%2Fb&tag=a%2Fb"
        );

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
                "https://relay.example/prefix?api_key=secret",
                "/eth/v1/validator/register_validator?tag=1",
                Some("/eth/v1/builder/validators"),
                "/prefix/eth/v1/builder/validators?api_key=secret&tag=1",
            ),
        ] {
            let upstream = HttpUpstream::parse("--el-http-url", base).unwrap();
            let inbound_uri: Uri = inbound.parse().unwrap();
            let uri = upstream.rewrite_uri(inbound_uri, override_path).unwrap();
            assert_eq!(
                uri.path_and_query().unwrap().as_str(),
                expected,
                "base={base}, inbound={inbound}, override={override_path:?}"
            );
        }
    }
}
