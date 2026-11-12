use std::fmt;

use http::uri::{Authority, Parts, PathAndQuery, Scheme};
use http::{HeaderValue, Uri};

use crate::BoxError;

/// Formats a URL for logs or diagnostics with credentials, non-root paths,
/// queries, and fragments redacted so API keys and tokens never leak.
#[derive(Clone, Copy)]
pub struct RedactedUrl<'a>(pub &'a str);

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

        f.write_str(scheme)?;
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

#[derive(Clone)]
struct Endpoint {
    uri: Uri,
    scheme: Scheme,
    authority: Authority,
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
        let uri: Uri = raw
            .parse()
            .map_err(|_| format!("invalid {flag} ({}): malformed URI", RedactedUrl(raw)))?;

        let (plain_scheme, tls_scheme) = allowed_schemes;
        let is_tls = match uri.scheme_str() {
            Some(s) if s.eq_ignore_ascii_case(plain_scheme) => false,
            Some(s) if s.eq_ignore_ascii_case(tls_scheme) => true,
            _ => {
                return Err(format!(
                    "invalid {flag} ({}): scheme must be {plain_scheme} or {tls_scheme}",
                    RedactedUrl(raw)
                )
                .into());
            }
        };

        let parts = uri.clone().into_parts();
        let Some(scheme) = parts.scheme else {
            return Err(format!("invalid {flag} ({}): missing scheme", RedactedUrl(raw)).into());
        };
        let Some(authority) = parts.authority else {
            return Err(format!("invalid {flag} ({}): missing host", RedactedUrl(raw)).into());
        };
        if authority.host().is_empty() {
            return Err(format!("invalid {flag} ({}): empty host", RedactedUrl(raw)).into());
        }

        Ok(Self {
            uri,
            scheme,
            authority,
            is_tls,
        })
    }

    /// Compose the upstream URI by moving `inbound`'s [`PathAndQuery`] directly
    /// into [`Uri::from_parts`] on root-based upstreams (zero heap allocation),
    /// and preserving prefix/query composition for path- or query-scoped URLs.
    fn rewrite_uri(
        &self,
        inbound: Uri,
        override_path: Option<&'static str>,
    ) -> Result<Uri, http::Error> {
        let base_path = self.uri.path();
        let base_query = self.uri.query();
        let inbound_pq = inbound.into_parts().path_and_query;
        let inbound_path =
            override_path.unwrap_or_else(|| inbound_pq.as_ref().map_or("", PathAndQuery::path));
        let inbound_query = inbound_pq.as_ref().and_then(PathAndQuery::query);

        if override_path.is_none() && matches!(inbound_path, "" | "/") && inbound_query.is_none() {
            return Ok(self.uri.clone());
        }

        if matches!(base_path, "" | "/") && base_query.is_none() {
            if let Some(static_path) = override_path
                && inbound_query.is_none()
            {
                return self.with_path_and_query(PathAndQuery::from_static(static_path));
            }
            if override_path.is_none()
                && inbound_path.starts_with('/')
                && let Some(pq) = inbound_pq
            {
                return self.with_path_and_query(pq);
            }
        }

        let effective_base_path = if base_path.is_empty() { "/" } else { base_path };
        let joined_path;
        let path = if override_path.is_none() && matches!(inbound_path, "" | "/") {
            effective_base_path
        } else if effective_base_path == "/" {
            inbound_path
        } else {
            joined_path = format!(
                "{}{inbound_path}",
                effective_base_path.trim_end_matches('/')
            );
            &joined_path
        };

        let pq: PathAndQuery = match (base_query, inbound_query) {
            (Some(bq), Some(iq)) if !bq.is_empty() && !iq.is_empty() => {
                format!("{path}?{bq}&{iq}").parse()?
            }
            (Some(bq), Some(iq)) => format!("{path}?{bq}{iq}").parse()?,
            (Some(bq), None) => format!("{path}?{bq}").parse()?,
            (None, Some(iq)) => format!("{path}?{iq}").parse()?,
            (None, None) => path.parse()?,
        };
        self.with_path_and_query(pq)
    }

    fn with_path_and_query(&self, pq: PathAndQuery) -> Result<Uri, http::Error> {
        let mut parts = Parts::default();
        parts.scheme = Some(self.scheme.clone());
        parts.authority = Some(self.authority.clone());
        parts.path_and_query = Some(pq);
        Ok(Uri::from_parts(parts)?)
    }
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
    ) -> Result<Uri, http::Error> {
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
        let host_port = endpoint
            .authority
            .as_str()
            .rsplit_once('@')
            .map_or(endpoint.authority.as_str(), |(_, hp)| hp);
        let host_header = HeaderValue::from_str(host_port)
            .map_err(|_| format!("invalid {flag} ({}): invalid Host header", RedactedUrl(raw)))?;
        Ok(Self {
            endpoint,
            host_header,
        })
    }

    pub(crate) const fn host_header(&self) -> &HeaderValue {
        &self.host_header
    }

    pub(crate) fn rewrite_uri(&self, inbound: Uri) -> Result<Uri, http::Error> {
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
    fn upstream_parse_enforces_scheme_authority_and_no_fragment_without_leaking_secrets() {
        assert!(HttpUpstream::parse("--el-http-url", "http://127.0.0.1:8545").is_ok());
        assert!(
            HttpUpstream::parse("--el-http-url", "https://rpc.example.com/v3/key")
                .unwrap()
                .is_tls()
        );
        assert!(WsUpstream::parse("--el-ws-url", "ws://127.0.0.1:8546").is_ok());
        assert!(WsUpstream::parse("--el-ws-url", "wss://ws.example.com/v2/key").is_ok());

        for bad_http in [
            "ws://user:TOPSECRET@127.0.0.1:8546/APIKEY",
            "ftp://127.0.0.1:8545/APIKEY",
            "http:///APIKEY",
            "/v3/APIKEY",
            "https://rpc.example.com/v3/APIKEY#FRAGSECRET",
        ] {
            let err = HttpUpstream::parse("--el-http-url", bad_http)
                .err()
                .expect("must reject invalid HTTP upstream")
                .to_string();
            assert!(!err.contains("TOPSECRET"), "leaked userinfo in {err}");
            assert!(!err.contains("APIKEY"), "leaked path in {err}");
            assert!(!err.contains("FRAGSECRET"), "leaked fragment in {err}");
        }

        for bad_ws in [
            "http://127.0.0.1:8545/APIKEY",
            "https://127.0.0.1:8545/APIKEY",
            "ws:///APIKEY",
            "wss://ws.example.com/APIKEY#FRAGSECRET",
        ] {
            let err = WsUpstream::parse("--el-ws-url", bad_ws)
                .err()
                .expect("must reject invalid WS upstream")
                .to_string();
            assert!(!err.contains("APIKEY"), "leaked path in {err}");
            assert!(!err.contains("FRAGSECRET"), "leaked fragment in {err}");
        }
    }

    #[test]
    fn ws_host_header_strips_userinfo() {
        let ws = WsUpstream::parse("--el-ws-url", "wss://user:secret@ws.example.com:8546/v2/k")
            .expect("valid wss upstream");
        assert_eq!(ws.host_header(), "ws.example.com:8546");
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
