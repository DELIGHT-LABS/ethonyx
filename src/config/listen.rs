//! Sans-IO listen-address parsing, IPv4/IPv6/dual-stack conflict algebra, and
//! pre-bind/post-bind socket overlap validation for `--listen` and `--admin-listen`.

use std::fmt;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};

use crate::BoxError;
use crate::upstream::{HostPort, HostPortError, parse_canonical_port, split_host_port};

/// Parsed host representation preserving literal IPv4 and non-zoned IPv6 addresses
/// without heap allocation or async DNS lookups, while retaining unbracketed
/// link-local `%zone` and hostname strings for OS `getaddrinfo` resolution.
#[derive(Debug, PartialEq, Eq)]
enum ListenHost {
    Ipv4(Ipv4Addr),
    Ipv6(Ipv6Addr),
    Ipv6Zoned(Box<str>),
    Name(Box<str>),
}

/// Resolution target returned by [`ListenAddr::resolve_target`].
pub(crate) enum ResolveTarget<'a> {
    Literal(SocketAddr),
    Lookup(&'a str, u16),
}

/// Validated `--listen` / `--admin-listen` endpoint.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ListenAddr {
    host: ListenHost,
    port: u16,
}

impl ListenAddr {
    pub(crate) fn resolve_target(&self) -> ResolveTarget<'_> {
        match &self.host {
            ListenHost::Ipv4(ip) => {
                ResolveTarget::Literal(SocketAddr::V4(SocketAddrV4::new(*ip, self.port)))
            }
            ListenHost::Ipv6(ip) => {
                ResolveTarget::Literal(SocketAddr::V6(SocketAddrV6::new(*ip, self.port, 0, 0)))
            }
            ListenHost::Ipv6Zoned(raw) | ListenHost::Name(raw) => {
                ResolveTarget::Lookup(raw, self.port)
            }
        }
    }
}

impl fmt::Display for ListenAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.host {
            ListenHost::Ipv4(ip) => write!(f, "{ip}:{}", self.port),
            ListenHost::Ipv6(ip) => write!(f, "[{ip}]:{}", self.port),
            ListenHost::Ipv6Zoned(raw) => write!(f, "[{raw}]:{}", self.port),
            ListenHost::Name(name) => write!(f, "{name}:{}", self.port),
        }
    }
}

/// Validated data-plane and/or admin-plane listen addresses.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ListenPlan {
    /// Data-plane listeners only (`--listen`); probes are served alongside proxy routes.
    DataOnly(Vec<ListenAddr>),
    /// Data-plane listeners (`--listen`) paired with an isolated `--admin-listen` listener.
    DataAndAdmin {
        data: Vec<ListenAddr>,
        admin: ListenAddr,
    },
    /// Control/probe plane only (`--admin-listen` without `--listen`); no data-plane sockets.
    ProbeOnly(ListenAddr),
}

impl ListenPlan {
    pub(crate) fn parse(listen: &[String], admin_listen: Option<&str>) -> Result<Self, BoxError> {
        let mut parsed = Vec::with_capacity(listen.len());
        for raw in listen {
            parsed.push(ParsedListen::parse("--listen", raw)?);
        }
        let admin = admin_listen
            .map(|raw| ParsedListen::parse("--admin-listen", raw))
            .transpose()?;
        check_conflicts(&parsed, admin.as_ref(), |a, b, scope| {
            a.conflict_with(*b, scope)
        })?;
        let data: Vec<_> = parsed
            .into_iter()
            .map(ParsedListen::into_listen_addr)
            .collect();
        let admin = admin.map(ParsedListen::into_listen_addr);
        match (data.is_empty(), admin) {
            (true, None) => Err("at least one of --listen or --admin-listen is required".into()),
            (true, Some(admin)) => Ok(Self::ProbeOnly(admin)),
            (false, None) => Ok(Self::DataOnly(data)),
            (false, Some(admin)) => Ok(Self::DataAndAdmin { data, admin }),
        }
    }
}

enum ResolvedAddrs {
    Single(SocketAddr),
    Many(Vec<SocketAddr>),
}

/// A configured [`ListenAddr`] paired with its resolved OS candidate socket addresses.
pub(crate) struct ResolvedListen {
    configured: ListenAddr,
    addrs: ResolvedAddrs,
}

impl ResolvedListen {
    pub(crate) const fn single(configured: ListenAddr, addr: SocketAddr) -> Self {
        Self {
            configured,
            addrs: ResolvedAddrs::Single(addr),
        }
    }

    pub(crate) fn many(configured: ListenAddr, addrs: Vec<SocketAddr>) -> Self {
        Self {
            configured,
            addrs: ResolvedAddrs::Many(addrs),
        }
    }

    pub(crate) const fn configured(&self) -> &ListenAddr {
        &self.configured
    }

    pub(crate) fn addrs(&self) -> &[SocketAddr] {
        match &self.addrs {
            ResolvedAddrs::Single(addr) => std::slice::from_ref(addr),
            ResolvedAddrs::Many(addrs) => addrs,
        }
    }

    /// Materialize the actual kernel-assigned socket address and port across every
    /// resolved candidate IP so post-bind validation catches ephemeral `:0` collisions
    /// across dual-stack sibling addresses (e.g. `0.0.0.0:0` vs `localhost:0` picking
    /// the same port `P` on `[::1]:P`).
    pub(crate) fn set_bound_addr(&mut self, bound: SocketAddr) {
        match &mut self.addrs {
            ResolvedAddrs::Single(addr) => *addr = bound,
            ResolvedAddrs::Many(addrs) => {
                let port = bound.port();
                for addr in addrs.iter_mut() {
                    addr.set_port(port);
                }
                if !addrs.contains(&bound) {
                    addrs.push(bound);
                }
            }
        }
    }
}

impl fmt::Display for ResolvedListen {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.configured.port == 0
            && let Some(bound_port) = self.addrs().first().map(SocketAddr::port)
            && bound_port != 0
        {
            write!(f, "{} (port {bound_port})", self.configured)
        } else {
            self.configured.fmt(f)
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ListenConflict {
    Duplicate,
    WildcardOverlap,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ConflictScope {
    /// Inside `--listen`: permits `[::]:port` + `0.0.0.0:port` for `IPV6_V6ONLY=1` dual-stack hosts.
    SamePlane,
    /// Across `--listen` and `--admin-listen`: blocks `[::]:port` vs any IPv4 or hostname on the same port.
    CrossPlane,
}

#[derive(Clone, Copy, Debug)]
enum ParsedHost<'a> {
    Ipv4(Ipv4Addr),
    Ipv6(Ipv6Addr, Option<&'a str>),
    Name(&'a str),
}

#[derive(Clone, Copy, Debug)]
struct ParsedListen<'a> {
    raw_host: &'a str,
    host: ParsedHost<'a>,
    port: u16,
}

fn ipv4_conflict(a: Ipv4Addr, b: Ipv4Addr) -> Option<ListenConflict> {
    if a == b {
        Some(ListenConflict::Duplicate)
    } else if a.is_unspecified() || b.is_unspecified() {
        Some(ListenConflict::WildcardOverlap)
    } else {
        None
    }
}

fn ipv6_conflict<Z: Eq>(a: Ipv6Addr, za: Z, b: Ipv6Addr, zb: Z) -> Option<ListenConflict> {
    if a == b && za == zb {
        Some(ListenConflict::Duplicate)
    } else if a.is_unspecified() || b.is_unspecified() {
        Some(ListenConflict::WildcardOverlap)
    } else if let (Some(va), Some(vb)) = (a.to_ipv4_mapped(), b.to_ipv4_mapped()) {
        ipv4_conflict(va, vb)
    } else {
        None
    }
}

fn v4_v6_conflict(v4: Ipv4Addr, v6: Ipv6Addr, scope: ConflictScope) -> Option<ListenConflict> {
    if let Some(mapped) = v6.to_ipv4_mapped() {
        ipv4_conflict(v4, mapped)
    } else if scope == ConflictScope::CrossPlane && v6.is_unspecified() {
        Some(ListenConflict::WildcardOverlap)
    } else {
        None
    }
}

fn socket_addr_conflict(
    a: SocketAddr,
    b: SocketAddr,
    scope: ConflictScope,
) -> Option<ListenConflict> {
    if a.port() == 0 || a.port() != b.port() {
        return None;
    }
    match (a, b) {
        (SocketAddr::V4(a), SocketAddr::V4(b)) => ipv4_conflict(*a.ip(), *b.ip()),
        (SocketAddr::V6(a), SocketAddr::V6(b)) => {
            ipv6_conflict(*a.ip(), a.scope_id(), *b.ip(), b.scope_id())
        }
        (SocketAddr::V4(v4), SocketAddr::V6(v6)) | (SocketAddr::V6(v6), SocketAddr::V4(v4)) => {
            v4_v6_conflict(*v4.ip(), *v6.ip(), scope)
        }
    }
}

fn check_conflicts<T: fmt::Display>(
    data: &[T],
    admin: Option<&T>,
    conflict: impl Fn(&T, &T, ConflictScope) -> Option<ListenConflict>,
) -> Result<(), BoxError> {
    for (idx, addr) in data.iter().enumerate() {
        for prev in &data[..idx] {
            match conflict(addr, prev, ConflictScope::SamePlane) {
                Some(ListenConflict::Duplicate) => {
                    return Err(format!("duplicate --listen address '{addr}'").into());
                }
                Some(ListenConflict::WildcardOverlap) => {
                    return Err(format!("--listen address '{addr}' conflicts with '{prev}'").into());
                }
                None => {}
            }
        }
    }
    if let Some(admin) = admin
        && let Some(prev) = data
            .iter()
            .find(|prev| conflict(prev, admin, ConflictScope::CrossPlane).is_some())
    {
        return Err(format!(
            "--admin-listen address '{admin}' conflicts with --listen address '{prev}'"
        )
        .into());
    }
    Ok(())
}

/// Reject same-plane or cross-plane overlaps across all resolved OS candidate addresses
/// (both before opening sockets for fixed ports and after [`ResolvedListen::set_bound_addr`]
/// materializes kernel-assigned ephemeral `:0` ports), preventing dual-stack split-brain
/// (e.g. `--listen 0.0.0.0:8547` alongside `--admin-listen localhost:8547` silently binding
/// only `[::1]:8547`).
pub(crate) fn validate_resolved_addrs(
    data: &[ResolvedListen],
    admin: Option<&ResolvedListen>,
) -> Result<(), BoxError> {
    check_conflicts(data, admin, |a, b, scope| {
        for &sa in a.addrs() {
            for &sb in b.addrs() {
                if let Some(conflict) = socket_addr_conflict(sa, sb, scope) {
                    return Some(conflict);
                }
            }
        }
        None
    })
}

impl<'a> ParsedListen<'a> {
    fn parse(flag: &'static str, raw: &'a str) -> Result<Self, BoxError> {
        let addr = raw.trim();
        if addr.is_empty() {
            return Err(format!("invalid {flag} address: entry must not be empty").into());
        }
        let err = |reason: &str| -> BoxError {
            format!("invalid {flag} address '{addr}': {reason}").into()
        };
        let (raw_host, host, port_str) = match split_host_port(addr) {
            Ok(HostPort::Bracketed { host, port }) => {
                let Some(port_str) = port else {
                    return Err(err("expected [ipv6]:port"));
                };
                let parsed = match host.split_once('%') {
                    Some((addr_part, zone)) => {
                        if zone.is_empty()
                            || zone.bytes().any(|b| {
                                b.is_ascii_whitespace() || b.is_ascii_control() || b == b'%'
                            })
                        {
                            return Err(err("invalid IPv6 host"));
                        }
                        let Ok(ip) = addr_part.parse::<Ipv6Addr>() else {
                            return Err(err("invalid IPv6 host"));
                        };
                        if !ip.is_unicast_link_local() {
                            return Err(err("IPv6 zone ID is only valid for link-local addresses"));
                        }
                        ParsedHost::Ipv6(ip, Some(zone))
                    }
                    None => {
                        let Ok(ip) = host.parse::<Ipv6Addr>() else {
                            return Err(err("invalid IPv6 host"));
                        };
                        ParsedHost::Ipv6(ip, None)
                    }
                };
                (host, parsed, port_str)
            }
            Ok(HostPort::Plain { host, port }) => {
                let Some(port_str) = port else {
                    return Err(err("expected host:port"));
                };
                if host.is_empty()
                    || host.bytes().any(|b| {
                        b.is_ascii_whitespace()
                            || b.is_ascii_control()
                            || matches!(b, b'/' | b'@' | b'[' | b']')
                    })
                {
                    return Err(err("invalid host"));
                }
                // `HostPort::Plain` never contains `:`, so a literal IP can only be IPv4.
                let parsed = host
                    .parse::<Ipv4Addr>()
                    .map_or(ParsedHost::Name(host), ParsedHost::Ipv4);
                (host, parsed, port_str)
            }
            Err(HostPortError::UnclosedBracket) => {
                return Err(err("unclosed IPv6 bracket"));
            }
            Err(HostPortError::TrailingAfterBracket) => {
                return Err(err("expected [ipv6]:port"));
            }
        };
        let Some(port) = parse_canonical_port(port_str, true) else {
            return Err(err("invalid port"));
        };
        Ok(Self {
            raw_host,
            host,
            port,
        })
    }

    fn conflict_with(self, other: Self, scope: ConflictScope) -> Option<ListenConflict> {
        if self.port == 0 || self.port != other.port {
            return None;
        }
        match (self.host, other.host) {
            (ParsedHost::Ipv4(a), ParsedHost::Ipv4(b)) => ipv4_conflict(a, b),
            // POSIX `if_nametoindex` / `getaddrinfo` interface names are case-sensitive,
            // so canonicalize only the IPv6 address literal and compare `%zone` verbatim.
            // `ParsedListen::parse` already rejects `%zone` on non-link-local addresses,
            // so `is_unspecified()` implies `zone.is_none()`.
            (ParsedHost::Ipv6(a, za), ParsedHost::Ipv6(b, zb)) => ipv6_conflict(a, za, b, zb),
            (ParsedHost::Ipv4(v4), ParsedHost::Ipv6(v6, _))
            | (ParsedHost::Ipv6(v6, _), ParsedHost::Ipv4(v4)) => v4_v6_conflict(v4, v6, scope),
            // Across planes, `[::]` overlaps both IPv6 and dual-stack IPv4 destinations,
            // so any hostname on the same port conflicts regardless of A/AAAA resolution.
            (ParsedHost::Ipv6(v6, _), ParsedHost::Name(_))
            | (ParsedHost::Name(_), ParsedHost::Ipv6(v6, _))
                if scope == ConflictScope::CrossPlane && v6.is_unspecified() =>
            {
                Some(ListenConflict::WildcardOverlap)
            }
            (ParsedHost::Name(a), ParsedHost::Name(b)) if a.eq_ignore_ascii_case(b) => {
                Some(ListenConflict::Duplicate)
            }
            _ => None,
        }
    }

    fn into_listen_addr(self) -> ListenAddr {
        let host = match self.host {
            ParsedHost::Ipv4(ip) => ListenHost::Ipv4(ip),
            ParsedHost::Ipv6(ip, None) => ListenHost::Ipv6(ip),
            ParsedHost::Ipv6(_, Some(_)) => ListenHost::Ipv6Zoned(Box::from(self.raw_host)),
            ParsedHost::Name(name) => ListenHost::Name(Box::from(name)),
        };
        ListenAddr {
            host,
            port: self.port,
        }
    }
}

impl fmt::Display for ParsedListen<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.raw_host.contains(':') {
            write!(f, "[{}]:{}", self.raw_host, self.port)
        } else {
            write!(f, "{}:{}", self.raw_host, self.port)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sa(s: &str) -> SocketAddr {
        s.parse().unwrap()
    }

    fn ll(scope: u32, port: u16) -> SocketAddr {
        SocketAddr::V6(SocketAddrV6::new(
            "fe80::1".parse::<Ipv6Addr>().unwrap(),
            port,
            0,
            scope,
        ))
    }

    fn parsed_addr(raw: &str) -> ListenAddr {
        ParsedListen::parse("--listen", raw)
            .unwrap()
            .into_listen_addr()
    }

    #[test]
    fn literal_ips_resolve_synchronously_while_zoned_ipv6_and_names_use_lookup() {
        assert!(matches!(
            parsed_addr("0.0.0.0:8547").resolve_target(),
            ResolveTarget::Literal(a) if a == sa("0.0.0.0:8547")
        ));
        assert!(matches!(
            parsed_addr("[0:0:0:0:0:0:0:1]:8547").resolve_target(),
            ResolveTarget::Literal(a) if a == sa("[::1]:8547")
        ));
        assert!(matches!(
            parsed_addr("[fe80::1%lo0]:8547").resolve_target(),
            ResolveTarget::Lookup("fe80::1%lo0", 8547)
        ));
        assert!(matches!(
            parsed_addr("localhost:8547").resolve_target(),
            ResolveTarget::Lookup("localhost", 8547)
        ));
        assert_eq!(
            parsed_addr("[0:0:0:0:0:0:0:1]:8547").to_string(),
            "[::1]:8547"
        );
        assert_eq!(
            parsed_addr("[fe80::1%lo0]:8547").to_string(),
            "[fe80::1%lo0]:8547"
        );
    }

    #[test]
    fn resolved_candidate_validation_prevents_dual_stack_split_brain_and_allows_ephemeral_zero() {
        let one = |raw: &str, addr: SocketAddr| ResolvedListen::single(parsed_addr(raw), addr);
        let many = |raw: &str, addrs: &[SocketAddr]| {
            ResolvedListen::many(parsed_addr(raw), addrs.to_vec())
        };

        // Allowed: explicit IPv4 wildcard + explicit IPv6 loopback, same-plane dual-stack,
        // IPv4-only hostname alongside `[::]` on same plane, distinct NIC vs localhost,
        // distinct link-local scope IDs, and ephemeral `:0` ports across both planes.
        for (data, admin) in [
            (
                vec![one("0.0.0.0:8547", sa("0.0.0.0:8547"))],
                Some(one("[::1]:8547", sa("[::1]:8547"))),
            ),
            (
                vec![
                    one("[::]:8547", sa("[::]:8547")),
                    one("0.0.0.0:8547", sa("0.0.0.0:8547")),
                ],
                None,
            ),
            (
                vec![
                    one("[::]:8547", sa("[::]:8547")),
                    many("localhost:8547", &[sa("127.0.0.1:8547")]),
                ],
                None,
            ),
            (
                vec![one("10.0.0.5:8547", sa("10.0.0.5:8547"))],
                Some(many(
                    "localhost:8547",
                    &[sa("[::1]:8547"), sa("127.0.0.1:8547")],
                )),
            ),
            (
                vec![one("[fe80::1%lo0]:8547", ll(1, 8547))],
                Some(one("[fe80::1%en0]:8547", ll(2, 8547))),
            ),
            (
                vec![
                    many("localhost:0", &[sa("[::1]:0"), sa("127.0.0.1:0")]),
                    one("127.0.0.1:0", sa("127.0.0.1:0")),
                ],
                Some(many("localhost:0", &[sa("[::1]:0"), sa("127.0.0.1:0")])),
            ),
        ] {
            validate_resolved_addrs(&data, admin.as_ref())
                .unwrap_or_else(|e| panic!("expected ok: {e}"));
        }

        // Rejected cross-plane dual-stack `localhost` conflicts regardless of A/AAAA ordering.
        for (listen_raw, listen_sa, admin_candidates) in [
            (
                "0.0.0.0:8547",
                sa("0.0.0.0:8547"),
                &[sa("[::1]:8547"), sa("127.0.0.1:8547")][..],
            ),
            (
                "0.0.0.0:8547",
                sa("0.0.0.0:8547"),
                &[sa("127.0.0.1:8547"), sa("[::1]:8547")],
            ),
            (
                "127.0.0.1:8547",
                sa("127.0.0.1:8547"),
                &[sa("[::1]:8547"), sa("127.0.0.1:8547")],
            ),
            (
                "[::1]:8547",
                sa("[::1]:8547"),
                &[sa("127.0.0.1:8547"), sa("[::1]:8547")],
            ),
            ("[fe80::1%lo0]:8547", ll(4, 8547), &[ll(4, 8547)]),
        ] {
            let data = [one(listen_raw, listen_sa)];
            let admin = many("localhost:8547", admin_candidates);
            let err = validate_resolved_addrs(&data, Some(&admin))
                .unwrap_err()
                .to_string();
            assert!(
                err.contains("--admin-listen") && err.contains("conflicts with --listen"),
                "unexpected error for {listen_raw}: {err}"
            );
        }

        // Rejected same-plane resolved conflicts (`0.0.0.0` + `localhost`, `[::]` + dual-stack `localhost`).
        for (a, b, expected) in [
            (
                one("0.0.0.0:8547", sa("0.0.0.0:8547")),
                many("localhost:8547", &[sa("[::1]:8547"), sa("127.0.0.1:8547")]),
                "conflicts with",
            ),
            (
                one("[::]:8547", sa("[::]:8547")),
                many("localhost:8547", &[sa("[::1]:8547"), sa("127.0.0.1:8547")]),
                "conflicts with",
            ),
            (
                one("127.0.0.1:8547", sa("127.0.0.1:8547")),
                many("localhost:8547", &[sa("[::1]:8547"), sa("127.0.0.1:8547")]),
                "duplicate --listen",
            ),
        ] {
            let err = validate_resolved_addrs(&[a, b], None)
                .unwrap_err()
                .to_string();
            assert!(err.contains(expected), "unexpected error: {err}");
        }
    }

    #[test]
    fn bound_socket_addr_validation_catches_same_plane_cross_plane_and_ephemeral_sibling_overlaps()
    {
        let bound_single = |addr: SocketAddr| {
            let mut r = ResolvedListen::single(parsed_addr("0.0.0.0:0"), sa("0.0.0.0:0"));
            r.set_bound_addr(addr);
            r
        };

        // Allowed: distinct ports, distinct NICs, dual-stack same-plane, IPv4 wildcard vs IPv6 loopback,
        // and link-local addresses with distinct interface scope IDs.
        for (listen, admin) in [
            (&[sa("0.0.0.0:8547")][..], Some(sa("127.0.0.1:9090"))),
            (&[sa("0.0.0.0:8547")], Some(sa("[::1]:8547"))),
            (&[sa("[::]:8547"), sa("0.0.0.0:8547")], None),
            (&[sa("127.0.0.1:40001"), sa("127.0.0.1:40002")], None),
            (&[ll(1, 8547)], Some(ll(2, 8547))),
            (&[ll(1, 8547), ll(2, 8547)], None),
        ] {
            let data: Vec<_> = listen.iter().copied().map(bound_single).collect();
            let admin_bound = admin.map(bound_single);
            validate_resolved_addrs(&data, admin_bound.as_ref())
                .unwrap_or_else(|e| panic!("expected ok for {listen:?} / {admin:?}: {e}"));
        }

        // Rejected same-plane duplicates and wildcard overlaps.
        for (listen, expected) in [
            (
                &[sa("127.0.0.1:8547"), sa("127.0.0.1:8547")][..],
                "duplicate --listen",
            ),
            (
                &[sa("0.0.0.0:8547"), sa("127.0.0.1:8547")],
                "conflicts with",
            ),
            (&[sa("[::]:8547"), sa("[::1]:8547")], "conflicts with"),
            // Every earlier entry is checked, not only the immediately preceding one.
            (
                &[
                    sa("0.0.0.0:8547"),
                    sa("127.0.0.1:9000"),
                    sa("127.0.0.1:8547"),
                ],
                "conflicts with",
            ),
            (&[ll(4, 8547), ll(4, 8547)], "duplicate --listen"),
            (&[sa("[::]:8547"), ll(4, 8547)], "conflicts with"),
            (
                &[sa("[::ffff:0.0.0.0]:8547"), sa("127.0.0.1:8547")],
                "conflicts with",
            ),
        ] {
            let data: Vec<_> = listen.iter().copied().map(bound_single).collect();
            let err = validate_resolved_addrs(&data, None)
                .unwrap_err()
                .to_string();
            assert!(
                err.contains(expected),
                "unexpected error for {listen:?}: {err}"
            );
        }

        // Rejected cross-plane duplicates and wildcard overlaps.
        for (listen, admin) in [
            (&[sa("0.0.0.0:8547")][..], sa("127.0.0.1:8547")),
            // Admin address is checked against every `--listen` entry, not only the first.
            (
                &[sa("127.0.0.1:9000"), sa("0.0.0.0:8547")],
                sa("127.0.0.1:8547"),
            ),
            (&[sa("127.0.0.1:8547")], sa("0.0.0.0:8547")),
            (&[sa("127.0.0.1:8547")], sa("127.0.0.1:8547")),
            (&[sa("[::]:8547")], sa("127.0.0.1:8547")),
            (&[sa("[::]:8547")], sa("[::1]:8547")),
            (&[sa("0.0.0.0:8547")], sa("[::ffff:127.0.0.1]:8547")),
            (&[ll(4, 8547)], ll(4, 8547)),
            (&[sa("[::]:8547")], ll(4, 8547)),
        ] {
            let data: Vec<_> = listen.iter().copied().map(bound_single).collect();
            let admin_bound = bound_single(admin);
            let err = validate_resolved_addrs(&data, Some(&admin_bound))
                .unwrap_err()
                .to_string();
            assert!(
                err.contains("--admin-listen") && err.contains("conflicts with --listen"),
                "unexpected error for {listen:?} / {admin}: {err}"
            );
        }

        // Ephemeral `:0` dual-stack sibling collision: `--listen 0.0.0.0:0` bound to
        // `0.0.0.0:54321` and `--admin-listen localhost:0` (`[::1], 127.0.0.1`) bound to
        // `[::1]:54321` must conflict post-bind across the sibling `127.0.0.1:54321` IP.
        let mut data = [ResolvedListen::single(
            parsed_addr("0.0.0.0:0"),
            sa("0.0.0.0:0"),
        )];
        let mut admin = ResolvedListen::many(
            parsed_addr("localhost:0"),
            vec![sa("[::1]:0"), sa("127.0.0.1:0")],
        );
        validate_resolved_addrs(&data, Some(&admin)).expect("pre-bind :0 must be allowed");
        data[0].set_bound_addr(sa("0.0.0.0:54321"));
        admin.set_bound_addr(sa("[::1]:54321"));
        let err = validate_resolved_addrs(&data, Some(&admin))
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("localhost:0 (port 54321)") && err.contains("0.0.0.0:0 (port 54321)"),
            "unexpected ephemeral sibling collision error: {err}"
        );
    }

    #[tokio::test]
    async fn listen_addr_resolves_numeric_and_named_ipv6_zone_ids() {
        #[cfg(target_os = "macos")]
        let named = "[fe80::1%lo0]:0";
        #[cfg(target_os = "linux")]
        let named = "[fe80::1%lo]:0";

        let addr = parsed_addr("[fe80::1%1]:0");
        let ResolveTarget::Lookup(host, port) = addr.resolve_target() else {
            panic!("zoned IPv6 must use OS lookup");
        };
        let resolved: Vec<_> = tokio::net::lookup_host((host, port))
            .await
            .expect("numeric IPv6 zone ID must resolve")
            .collect();
        assert!(matches!(resolved.as_slice(), [SocketAddr::V6(v6), ..] if v6.scope_id() == 1));

        #[cfg(any(target_os = "macos", target_os = "linux"))]
        {
            let addr = parsed_addr(named);
            let ResolveTarget::Lookup(host, port) = addr.resolve_target() else {
                panic!("named zoned IPv6 must use OS lookup");
            };
            let resolved: Vec<_> = tokio::net::lookup_host((host, port))
                .await
                .expect("named IPv6 zone ID must resolve via OS getaddrinfo")
                .collect();
            assert!(matches!(resolved.as_slice(), [SocketAddr::V6(v6), ..] if v6.scope_id() != 0));

            #[cfg(target_os = "macos")]
            {
                let listener = tokio::net::TcpListener::bind((host, port))
                    .await
                    .expect("macOS lo0 must bind [fe80::1%lo0]:0");
                let local = listener.local_addr().unwrap();
                assert!(matches!(local, SocketAddr::V6(v6) if v6.scope_id() != 0));
            }
        }
    }

    #[test]
    fn listen_plan_parse_covers_all_listener_combinations() {
        let err = ListenPlan::parse(&[], None).unwrap_err().to_string();
        assert!(
            err.contains("at least one of --listen or --admin-listen is required"),
            "unexpected error: {err}"
        );

        assert_eq!(
            ListenPlan::parse(&[], Some("127.0.0.1:9099")).unwrap(),
            ListenPlan::ProbeOnly(parsed_addr("127.0.0.1:9099"))
        );
        assert_eq!(
            ListenPlan::parse(&["0.0.0.0:8545".to_owned()], None).unwrap(),
            ListenPlan::DataOnly(vec![parsed_addr("0.0.0.0:8545")])
        );
        assert_eq!(
            ListenPlan::parse(&["0.0.0.0:8545".to_owned()], Some("0.0.0.0:9099")).unwrap(),
            ListenPlan::DataAndAdmin {
                data: vec![parsed_addr("0.0.0.0:8545")],
                admin: parsed_addr("0.0.0.0:9099"),
            }
        );
    }
}
