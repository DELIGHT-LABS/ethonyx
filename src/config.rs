use std::fmt;
use std::num::NonZeroU64;
use std::time::Duration;

use clap::{Parser, ValueEnum};

use crate::BoxError;
use crate::upstream::{HttpUpstream, RedactedUrl, WsUpstream, parse_canonical_port};

const DEFAULT_CL_MAX_SLOT_AGE_SECS: u64 = 60;
const DEFAULT_EL_MAX_REFERENCE_LAG: u64 = 1;
const DEFAULT_EL_REFERENCE_POLL_INTERVAL: Duration = Duration::from_secs(3);

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum Network {
    Mainnet,
    Hoodi,
    Sepolia,
    Holesky,
    /// Private / custom beacon chain. Requires explicit --cl-genesis-time and --cl-seconds-per-slot.
    Custom,
}

impl Network {
    pub const fn preset_genesis_time(self) -> Option<u64> {
        match self {
            Network::Mainnet => Some(1_606_824_023),
            Network::Hoodi => Some(1_742_213_400),
            Network::Sepolia => Some(1_655_733_600),
            Network::Holesky => Some(1_695_902_400),
            Network::Custom => None,
        }
    }

    pub const fn preset_seconds_per_slot(self) -> Option<u64> {
        match self {
            Network::Custom => None,
            _ => Some(12),
        }
    }
}

/// An upstream URL exactly as given on the command line or in the environment.
/// Its [`fmt::Debug`] output redacts credentials, non-root paths, queries, and
/// fragments so [`Config`] (or individual URL fields) can be logged safely.
#[derive(Clone)]
pub struct UrlArg(String);

impl From<String> for UrlArg {
    fn from(raw: String) -> Self {
        Self(raw)
    }
}

impl From<&str> for UrlArg {
    fn from(raw: &str) -> Self {
        Self(String::from(raw))
    }
}

impl std::ops::Deref for UrlArg {
    type Target = str;

    fn deref(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for UrlArg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&RedactedUrl(&self.0), f)
    }
}

#[derive(Clone, Debug, Parser)]
#[command(
    version,
    about = "Ethonyx — Ethereum EL/CL reverse proxy & health-check sidecar"
)]
pub struct Config {
    /// Listen address (host:port). Repeat the flag or comma-separate for multi-port.
    /// All ports serve the same routes; intended for direct IP:port access plus LB.
    #[arg(
        long,
        env = "ETHONYX_LISTEN",
        value_delimiter = ',',
        default_value = "0.0.0.0:8547"
    )]
    pub listen: Vec<String>,

    /// Execution-layer JSON-RPC over HTTP upstream URL (required).
    #[arg(long, env = "ETHONYX_EL_HTTP_URL")]
    pub el_http_url: UrlArg,

    /// Optional Execution-layer JSON-RPC over WebSocket upstream URL.
    /// Omit to disable WebSocket forwarding (HTTP-only EL mode).
    #[arg(long, env = "ETHONYX_EL_WS_URL")]
    pub el_ws_url: Option<UrlArg>,

    /// Optional Consensus-layer Beacon API upstream URL (REST, /eth/v1/...).
    /// Omit to run in EL-only mode (L2, Cosmos EVM, non-Beacon chains).
    #[arg(long, env = "ETHONYX_CL_BEACON_URL")]
    pub cl_beacon_url: Option<UrlArg>,

    /// Optional Consensus-layer MEV Relay / MEV-Boost upstream URL (REST, /eth/v1/builder/validators).
    /// When specified, POST /eth/v1/validator/register_validator requests are rewritten to
    /// /eth/v1/builder/validators and routed to this URL. Requires `--cl-beacon-url`.
    #[arg(long, env = "ETHONYX_CL_MEV_RELAY_URL")]
    pub cl_mev_relay_url: Option<UrlArg>,

    /// Beacon network (defaults to `mainnet` when `--cl-beacon-url` is set).
    /// Selects defaults for cl-genesis-time / cl-seconds-per-slot.
    /// Use `custom` for private chains and pass both values explicitly.
    #[arg(long, env = "ETHONYX_NETWORK", value_enum)]
    pub network: Option<Network>,

    /// Reference Execution-layer JSON-RPC HTTP upstream URL(s) used to gate `/readyz`
    /// on block-height lag. Repeat the flag or comma-separate for multiple peers/RPCs.
    #[arg(long, env = "ETHONYX_EL_REFERENCE_URL", value_delimiter = ',')]
    pub el_reference_url: Vec<UrlArg>,

    /// Maximum allowed EL block lag behind the highest valid `--el-reference-url`
    /// before `/readyz` returns 503 (defaults to 1 block when `--el-reference-url` is set;
    /// increase on sub-second block chains to account for P2P propagation spread).
    #[arg(long, env = "ETHONYX_EL_MAX_REFERENCE_LAG")]
    pub el_max_reference_lag: Option<u64>,

    /// Interval between background `--el-reference-url` polls (seconds; defaults to 3
    /// when `--el-reference-url` is set; must be at least 1 on the CLI).
    #[arg(long, env = "ETHONYX_EL_REFERENCE_POLL_INTERVAL", value_parser = parse_secs)]
    pub el_reference_poll_interval: Option<Duration>,

    /// Maximum EL block age (seconds) used to gate `/readyz` under
    /// `--readyz-strict`. `/healthz` always reports the raw age regardless.
    #[arg(long, env = "ETHONYX_EL_MAX_BLOCK_AGE_SECS", default_value_t = 60)]
    pub el_max_block_age_secs: u64,

    /// Maximum CL head_slot age (seconds, wall-clock; defaults to 60 when `--cl-beacon-url`
    /// is set) used to gate `/readyz` under `--readyz-strict`. `/healthz` always reports
    /// the raw age regardless.
    #[arg(long, env = "ETHONYX_CL_MAX_SLOT_AGE_SECS")]
    pub cl_max_slot_age_secs: Option<u64>,

    /// Also gate `/readyz` on EL block / CL slot freshness, not just sync status.
    /// Off by default so a network-wide stall (or a peer dip) does not drain the
    /// whole fleet from the load balancer at once.
    #[arg(
        long,
        env = "ETHONYX_READYZ_STRICT",
        default_value_t = false,
        action = clap::ArgAction::Set,
        num_args = 0..=1,
        default_missing_value = "true"
    )]
    pub readyz_strict: bool,

    /// Beacon-chain genesis Unix timestamp. Overrides --network preset. Set 0 to disable slot-age check.
    #[arg(long, env = "ETHONYX_CL_GENESIS_TIME")]
    pub cl_genesis_time: Option<u64>,

    /// Seconds per slot. Overrides --network preset.
    #[arg(long, env = "ETHONYX_CL_SECONDS_PER_SLOT")]
    pub cl_seconds_per_slot: Option<u64>,

    /// Upstream timeout for each background health-poll RPC (seconds).
    #[arg(long, env = "ETHONYX_HEALTH_TIMEOUT", default_value = "2", value_parser = parse_secs)]
    pub health_timeout: Duration,

    /// Interval between background health polls (seconds). `/healthz` and
    /// `/readyz` serve the latest poll, so upstream load stays constant
    /// regardless of probe rate. Must be at least 1 second on the CLI.
    #[arg(long, env = "ETHONYX_HEALTH_POLL_INTERVAL", default_value = "1", value_parser = parse_secs)]
    pub health_poll_interval: Duration,

    /// Log level used when `RUST_LOG` is unset. `RUST_LOG` overrides this and
    /// additionally allows per-target directives (e.g. `ethonyx=debug,hyper=warn`).
    #[arg(
        long,
        env = "ETHONYX_LOG_LEVEL",
        default_value = "info",
        value_parser = ["trace", "debug", "info", "warn", "error"]
    )]
    pub log_level: String,

    /// Emit a per-request access log (peer, HTTP version, method, path,
    /// upstream, protocol, status, and latency_ms) on the `access_log` target.
    /// Off by default — the main log stays quiet at `info` (lifecycle and state
    /// changes only); health-probe paths are excluded even when this is on.
    /// `RUST_LOG` can target it directly, e.g. `RUST_LOG=access_log=info`.
    #[arg(
        long,
        env = "ETHONYX_ACCESS_LOG",
        default_value_t = false,
        action = clap::ArgAction::Set,
        num_args = 0..=1,
        default_missing_value = "true"
    )]
    pub access_log: bool,

    /// Trust the upstream node: disable background health polling (0 RPC cost for paid endpoints) and always report healthy/ready.
    #[arg(
        long,
        env = "ETHONYX_TRUST_UPSTREAM",
        default_value_t = false,
        action = clap::ArgAction::Set,
        num_args = 0..=1,
        default_missing_value = "true"
    )]
    pub trust_upstream: bool,

    /// Statically mock eth_syncing JSON-RPC requests (HTTP & WebSocket) locally with result: false.
    #[arg(
        long,
        env = "ETHONYX_MOCK_ETH_SYNCING",
        default_value_t = false,
        action = clap::ArgAction::Set,
        num_args = 0..=1,
        default_missing_value = "true"
    )]
    pub mock_eth_syncing: bool,

    /// Statically mock CL validator subscription & prepare proposer requests locally with 200 OK.
    #[arg(
        long,
        env = "ETHONYX_MOCK_CL_SUBSCRIPTIONS",
        default_value_t = false,
        action = clap::ArgAction::Set,
        num_args = 0..=1,
        default_missing_value = "true"
    )]
    pub mock_cl_subscriptions: bool,

    /// Serve GET/HEAD /eth/v1/node/health locally from ethonyx health state (200/206/503; 200 under --trust-upstream).
    #[arg(
        long,
        env = "ETHONYX_MOCK_CL_NODE_HEALTH",
        default_value_t = false,
        action = clap::ArgAction::Set,
        num_args = 0..=1,
        default_missing_value = "true"
    )]
    pub mock_cl_node_health: bool,

    /// Upstream timeout for proxied requests (seconds; must be at least 1 on the CLI).
    #[arg(long, env = "ETHONYX_PROXY_TIMEOUT", default_value = "60", value_parser = parse_secs)]
    pub proxy_timeout: Duration,

    /// Idle timeout between frames for `text/event-stream` responses such as CL
    /// `/eth/v1/events` (seconds; 0 disables idle timeout).
    #[arg(long, env = "ETHONYX_SSE_IDLE_TIMEOUT", default_value = "900", value_parser = parse_secs)]
    pub sse_idle_timeout: Duration,

    /// Maximum concurrent inbound TCP connections across all listeners (0 = unlimited).
    #[arg(long, env = "ETHONYX_MAX_CONNECTIONS", default_value_t = 4096)]
    pub max_connections: usize,

    /// Shutdown grace period (seconds) for draining in-flight connections.
    #[arg(long, env = "ETHONYX_SHUTDOWN_GRACE", default_value = "10", value_parser = parse_secs)]
    pub shutdown_grace: Duration,

    /// OpenTelemetry collector endpoint (HTTP) for exporting traces/metrics
    /// (requires building with the `otel` feature).
    #[arg(long, env = "ETHONYX_OTEL_ENDPOINT")]
    pub otel_endpoint: Option<UrlArg>,
}

/// Validated beacon-chain slot timing (`None` in EL-only mode or when `--cl-genesis-time 0` disables slot-age checks).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SlotClock {
    genesis_time: NonZeroU64,
    seconds_per_slot: NonZeroU64,
}

impl SlotClock {
    pub(crate) const fn new(genesis_time: u64, seconds_per_slot: u64) -> Option<Self> {
        match (
            NonZeroU64::new(genesis_time),
            NonZeroU64::new(seconds_per_slot),
        ) {
            (Some(genesis_time), Some(seconds_per_slot)) => Some(Self {
                genesis_time,
                seconds_per_slot,
            }),
            _ => None,
        }
    }

    pub(crate) const fn slot_age(self, head_slot: u64, now: u64) -> u64 {
        // Saturate: a garbage `head_slot` from upstream must not overflow-panic
        // (debug) or wrap (release).
        let expected = self
            .genesis_time
            .get()
            .saturating_add(head_slot.saturating_mul(self.seconds_per_slot.get()));
        now.saturating_sub(expected)
    }
}

/// Parsed `--listen` endpoint preserving unbracketed host/zone strings so
/// `(host, port)` binds both numeric (`%1`) and named (`%lo0`, `%eth0`) IPv6
/// link-local scope IDs via OS `getaddrinfo`.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ListenAddr {
    pub(crate) host: Box<str>,
    pub(crate) port: u16,
}

impl ListenAddr {
    fn parse(raw: &str) -> Result<Self, BoxError> {
        let addr = raw.trim();
        if addr.is_empty() {
            return Err("invalid --listen address: entry must not be empty".into());
        }
        let (host, port_str) = match crate::upstream::split_host_port(addr) {
            Ok(crate::upstream::HostPort::Bracketed { host, port }) => {
                let Some(port_str) = port else {
                    return Err(
                        format!("invalid --listen address '{addr}': expected [ipv6]:port").into(),
                    );
                };
                let valid_ipv6 = match host.split_once('%') {
                    Some((addr_part, zone)) => {
                        !zone.is_empty()
                            && !zone
                                .bytes()
                                .any(|b| b.is_ascii_whitespace() || b.is_ascii_control())
                            && addr_part.parse::<std::net::Ipv6Addr>().is_ok()
                    }
                    None => host.parse::<std::net::Ipv6Addr>().is_ok(),
                };
                if !valid_ipv6 {
                    return Err(
                        format!("invalid --listen address '{addr}': invalid IPv6 host").into(),
                    );
                }
                (host, port_str)
            }
            Ok(crate::upstream::HostPort::Plain { host, port }) => {
                let Some(port_str) = port else {
                    return Err(
                        format!("invalid --listen address '{addr}': expected host:port").into(),
                    );
                };
                if host.is_empty()
                    || host.bytes().any(|b| {
                        b.is_ascii_whitespace()
                            || b.is_ascii_control()
                            || matches!(b, b'/' | b'@' | b'[' | b']')
                    })
                {
                    return Err(format!("invalid --listen address '{addr}': invalid host").into());
                }
                (host, port_str)
            }
            Err(crate::upstream::HostPortError::UnclosedBracket) => {
                return Err(
                    format!("invalid --listen address '{addr}': unclosed IPv6 bracket").into(),
                );
            }
            Err(crate::upstream::HostPortError::TrailingAfterBracket) => {
                return Err(
                    format!("invalid --listen address '{addr}': expected [ipv6]:port").into(),
                );
            }
        };
        let Some(port) = parse_canonical_port(port_str, true) else {
            return Err(format!("invalid --listen address '{addr}': invalid port").into());
        };
        Ok(Self {
            host: Box::from(host),
            port,
        })
    }
}

impl fmt::Display for ListenAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.host.contains(':') {
            write!(f, "[{}]:{}", self.host, self.port)
        } else {
            write!(f, "{}:{}", self.host, self.port)
        }
    }
}

/// Strict wall-clock freshness thresholds enabled by `--readyz-strict`.
#[derive(Clone, Copy)]
pub(crate) struct StrictFreshness {
    pub(crate) el_max_block_age_secs: u64,
    pub(crate) cl_max_slot_age_secs: u64,
}

/// Scalar data-plane proxy options retained by [`crate::state::AppState`] after
/// startup so heap-allocated CLI URL/listen strings in [`Config`] can be freed early.
#[derive(Clone, Copy)]
pub(crate) struct ProxyOpts {
    pub(crate) proxy_timeout: Duration,
    pub(crate) sse_idle_timeout: Duration,
    pub(crate) max_connections: usize,
    pub(crate) mock_eth_syncing: bool,
}

/// Health readiness and freshness evaluation thresholds owned by polled health state
/// (absent when `--trust-upstream` disables background polling).
#[derive(Clone, Copy)]
pub(crate) struct HealthPolicy {
    pub(crate) el_max_reference_lag: u64,
    pub(crate) strict: Option<StrictFreshness>,
    pub(crate) slot_clock: Option<SlotClock>,
}

pub(crate) struct ValidatedCl {
    pub(crate) beacon: HttpUpstream,
    pub(crate) probe_uris: ClProbeUris,
    pub(crate) mev_relay: Option<HttpUpstream>,
    pub(crate) mock_subscriptions: bool,
    pub(crate) mock_node_health: bool,
}

pub(crate) struct ValidatedReference {
    pub(crate) upstreams: Vec<HttpUpstream>,
    pub(crate) poll_interval: Duration,
}

pub(crate) struct ClProbeUris {
    pub(crate) syncing: http::Uri,
    pub(crate) peer_count: http::Uri,
}

impl ClProbeUris {
    fn from_beacon(beacon: &HttpUpstream) -> Result<Self, BoxError> {
        Ok(Self {
            syncing: beacon.join_static_path("--cl-beacon-url", "/eth/v1/node/syncing")?,
            peer_count: beacon.join_static_path("--cl-beacon-url", "/eth/v1/node/peer_count")?,
        })
    }
}

pub(crate) struct PolledHealth {
    pub(crate) interval: Duration,
    pub(crate) timeout: Duration,
    pub(crate) policy: HealthPolicy,
    pub(crate) reference: Option<ValidatedReference>,
}

pub(crate) enum HealthMode {
    Trusted,
    Polled(Box<PolledHealth>),
}

impl HealthMode {
    pub(crate) fn reference_count(&self) -> usize {
        match self {
            Self::Polled(p) => p.reference.as_ref().map_or(0, |r| r.upstreams.len()),
            Self::Trusted => 0,
        }
    }

    #[cfg(test)]
    fn policy(&self) -> Option<HealthPolicy> {
        match self {
            Self::Trusted => None,
            Self::Polled(p) => Some(p.policy),
        }
    }
}

pub(crate) struct ValidatedUpstreams {
    pub(crate) el_http: HttpUpstream,
    pub(crate) el_ws: Option<WsUpstream>,
    pub(crate) cl: Option<ValidatedCl>,
}

pub(crate) struct ParsedConfig {
    pub(crate) listen: Vec<ListenAddr>,
    pub(crate) opts: ProxyOpts,
    pub(crate) upstreams: ValidatedUpstreams,
    pub(crate) health: HealthMode,
    pub(crate) shutdown_grace: Duration,
}

fn reject_flags_when(cond: bool, flags: &[(&str, bool)], reason: &str) -> Result<(), BoxError> {
    if cond {
        for &(flag, given) in flags {
            if given {
                return Err(format!("{flag} {reason}").into());
            }
        }
    }
    Ok(())
}

impl Config {
    fn resolve_slot_clock(&self) -> Result<Option<SlotClock>, &'static str> {
        let net = self.network.unwrap_or(Network::Mainnet);
        let genesis_time = self
            .cl_genesis_time
            .or_else(|| net.preset_genesis_time())
            .ok_or(
                "--network custom requires --cl-genesis-time (use 0 to disable slot-age check)",
            )?;
        if genesis_time == 0 {
            return Ok(None);
        }
        let seconds_per_slot = self
            .cl_seconds_per_slot
            .or_else(|| net.preset_seconds_per_slot())
            .ok_or("--network custom requires --cl-seconds-per-slot")?;
        Ok(SlotClock::new(genesis_time, seconds_per_slot))
    }

    /// Validate all CLI/library configuration invariants and upstream URLs.
    pub fn validate(&self) -> Result<(), BoxError> {
        self.parse_with_otel().map(|_| ())
    }

    fn validate_scalars_and_durations(&self) -> Result<(), BoxError> {
        if !matches!(
            self.log_level.as_str(),
            "trace" | "debug" | "info" | "warn" | "error"
        ) {
            return Err(format!(
                "invalid --log-level '{}': expected one of trace, debug, info, warn, error",
                self.log_level
            )
            .into());
        }
        if self.max_connections > tokio::sync::Semaphore::MAX_PERMITS {
            return Err(format!(
                "max-connections must be at most {}",
                tokio::sync::Semaphore::MAX_PERMITS
            )
            .into());
        }
        for (name, dur, allow_zero) in [
            (
                "health-poll-interval",
                Some(self.health_poll_interval),
                false,
            ),
            ("health-timeout", Some(self.health_timeout), false),
            ("proxy-timeout", Some(self.proxy_timeout), false),
            (
                "el-reference-poll-interval",
                self.el_reference_poll_interval,
                false,
            ),
            ("sse-idle-timeout", Some(self.sse_idle_timeout), true),
            ("shutdown-grace", Some(self.shutdown_grace), true),
        ] {
            if let Some(dur) = dur {
                // CLI flags parse whole seconds (`parse_secs`), so `!is_zero()` enforces `>= 1s`
                // on the CLI while deliberately allowing sub-second `Duration`s via library `Config`.
                if !allow_zero && dur.is_zero() {
                    return Err(format!("{name} must be greater than zero").into());
                }
                if dur > MAX_TIMEOUT {
                    return Err(format!("{name} must be at most {MAX_TIMEOUT_SECS} seconds").into());
                }
            }
        }
        Ok(())
    }

    fn validate_flag_combinations(&self) -> Result<(), BoxError> {
        reject_flags_when(
            self.cl_beacon_url.is_none(),
            &[
                ("--network", self.network.is_some()),
                ("--cl-mev-relay-url", self.cl_mev_relay_url.is_some()),
                ("--cl-genesis-time", self.cl_genesis_time.is_some()),
                ("--cl-seconds-per-slot", self.cl_seconds_per_slot.is_some()),
                (
                    "--cl-max-slot-age-secs",
                    self.cl_max_slot_age_secs.is_some(),
                ),
                ("--mock-cl-subscriptions", self.mock_cl_subscriptions),
                ("--mock-cl-node-health", self.mock_cl_node_health),
            ],
            "requires --cl-beacon-url",
        )?;
        reject_flags_when(
            self.el_reference_url.is_empty(),
            &[
                (
                    "--el-max-reference-lag",
                    self.el_max_reference_lag.is_some(),
                ),
                (
                    "--el-reference-poll-interval",
                    self.el_reference_poll_interval.is_some(),
                ),
            ],
            "requires --el-reference-url",
        )?;
        reject_flags_when(
            self.trust_upstream,
            &[
                ("--readyz-strict", self.readyz_strict),
                ("--el-reference-url", !self.el_reference_url.is_empty()),
            ],
            "cannot be combined with --trust-upstream",
        )
    }

    fn parse_listen_addrs(&self) -> Result<Vec<ListenAddr>, BoxError> {
        if self.listen.is_empty() {
            return Err("no listen addresses configured".into());
        }
        let mut addrs = Vec::with_capacity(self.listen.len());
        for raw in &self.listen {
            let addr = ListenAddr::parse(raw)?;
            if addr.port != 0
                && addrs.iter().any(|prev: &ListenAddr| {
                    prev.port == addr.port && prev.host.eq_ignore_ascii_case(&addr.host)
                })
            {
                return Err(format!("duplicate --listen address '{addr}'").into());
            }
            addrs.push(addr);
        }
        Ok(addrs)
    }

    fn parse_cl(&self) -> Result<Option<ValidatedCl>, BoxError> {
        let Some(raw_beacon) = self.cl_beacon_url.as_deref() else {
            return Ok(None);
        };
        if self.cl_seconds_per_slot == Some(0) {
            return Err("cl-seconds-per-slot must be at least 1".into());
        }
        let beacon = HttpUpstream::parse("--cl-beacon-url", raw_beacon)?;
        let probe_uris = ClProbeUris::from_beacon(&beacon)?;
        let mev_relay = self
            .cl_mev_relay_url
            .as_deref()
            .map(|u| HttpUpstream::parse("--cl-mev-relay-url", u))
            .transpose()?;
        Ok(Some(ValidatedCl {
            beacon,
            probe_uris,
            mev_relay,
            mock_subscriptions: self.mock_cl_subscriptions,
            mock_node_health: self.mock_cl_node_health,
        }))
    }

    fn parse_reference_upstreams(
        &self,
        el_http: &HttpUpstream,
    ) -> Result<Option<ValidatedReference>, BoxError> {
        if self.el_reference_url.is_empty() {
            return Ok(None);
        }
        let mut upstreams = Vec::with_capacity(self.el_reference_url.len());
        for raw in &self.el_reference_url {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                return Err("invalid --el-reference-url: entry must not be empty".into());
            }
            let parsed = HttpUpstream::parse("--el-reference-url", trimmed)?;
            if !parsed.same_endpoint(el_http)
                && !upstreams
                    .iter()
                    .any(|existing: &HttpUpstream| existing.same_endpoint(&parsed))
            {
                upstreams.push(parsed);
            }
        }
        if upstreams.is_empty() {
            return Err(
                "--el-reference-url must include at least one endpoint distinct from --el-http-url"
                    .into(),
            );
        }
        Ok(Some(ValidatedReference {
            upstreams,
            poll_interval: self
                .el_reference_poll_interval
                .unwrap_or(DEFAULT_EL_REFERENCE_POLL_INTERVAL),
        }))
    }

    fn parse_health_mode(
        &self,
        el_http: &HttpUpstream,
        has_cl: bool,
    ) -> Result<HealthMode, BoxError> {
        if self.trust_upstream {
            return Ok(HealthMode::Trusted);
        }
        let slot_clock = if has_cl {
            self.resolve_slot_clock()?
        } else {
            None
        };
        let reference = self.parse_reference_upstreams(el_http)?;
        let policy = HealthPolicy {
            el_max_reference_lag: self
                .el_max_reference_lag
                .unwrap_or(DEFAULT_EL_MAX_REFERENCE_LAG),
            strict: self.readyz_strict.then_some(StrictFreshness {
                el_max_block_age_secs: self.el_max_block_age_secs,
                cl_max_slot_age_secs: self
                    .cl_max_slot_age_secs
                    .unwrap_or(DEFAULT_CL_MAX_SLOT_AGE_SECS),
            }),
            slot_clock,
        };
        Ok(HealthMode::Polled(Box::new(PolledHealth {
            interval: self.health_poll_interval,
            timeout: self.health_timeout,
            policy,
            reference,
        })))
    }

    pub(crate) fn parse_with_otel(&self) -> Result<(ParsedConfig, Option<HttpUpstream>), BoxError> {
        self.validate_scalars_and_durations()?;
        self.validate_flag_combinations()?;
        let listen = self.parse_listen_addrs()?;
        let el_http = HttpUpstream::parse("--el-http-url", &self.el_http_url)?;
        let el_ws = self
            .el_ws_url
            .as_deref()
            .map(|u| WsUpstream::parse("--el-ws-url", u))
            .transpose()?;
        let cl = self.parse_cl()?;
        let health = self.parse_health_mode(&el_http, cl.is_some())?;
        let otel_endpoint = crate::otel::parse_config_endpoint(self)?;
        let opts = ProxyOpts {
            proxy_timeout: self.proxy_timeout,
            sse_idle_timeout: self.sse_idle_timeout,
            max_connections: self.max_connections,
            mock_eth_syncing: self.mock_eth_syncing,
        };
        Ok((
            ParsedConfig {
                listen,
                opts,
                upstreams: ValidatedUpstreams { el_http, el_ws, cl },
                health,
                shutdown_grace: self.shutdown_grace,
            },
            otel_endpoint,
        ))
    }

    #[cfg(test)]
    pub(crate) fn parse_validated(&self) -> Result<ParsedConfig, BoxError> {
        self.parse_with_otel().map(|(parsed, _)| parsed)
    }
}

/// Upper bound (100 years) for timeout/interval flags so `Instant::now() + value`
/// cannot overflow on any platform (macOS `u64` ns or Linux `i64` secs).
const MAX_TIMEOUT_SECS: u64 = 100 * 365 * 24 * 60 * 60;
const MAX_TIMEOUT: Duration = Duration::from_secs(MAX_TIMEOUT_SECS);

fn parse_secs(s: &str) -> Result<Duration, String> {
    let secs = s
        .parse::<u64>()
        .map_err(|e| format!("invalid seconds value: {e}"))?;
    if secs > MAX_TIMEOUT_SECS {
        return Err(format!("seconds value must be at most {MAX_TIMEOUT_SECS}"));
    }
    Ok(Duration::from_secs(secs))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(extra: &[&str]) -> Config {
        let mut argv = vec!["ethonyx"];
        if !extra.contains(&"--el-http-url") {
            argv.extend_from_slice(&["--el-http-url", "http://127.0.0.1:8545"]);
        }
        argv.extend_from_slice(extra);
        Config::try_parse_from(argv).expect("clap parse")
    }

    #[test]
    fn network_presets_and_slot_clock_resolution() {
        for (net, genesis) in [
            (Network::Mainnet, 1_606_824_023),
            (Network::Hoodi, 1_742_213_400),
            (Network::Sepolia, 1_655_733_600),
            (Network::Holesky, 1_695_902_400),
        ] {
            assert_eq!(net.preset_genesis_time(), Some(genesis));
            assert_eq!(net.preset_seconds_per_slot(), Some(12));
        }
        assert_eq!(Network::Custom.preset_genesis_time(), None);
        assert_eq!(Network::Custom.preset_seconds_per_slot(), None);

        let override_gen = parse(&[
            "--cl-beacon-url",
            "http://127.0.0.1:5052",
            "--network",
            "hoodi",
            "--cl-genesis-time",
            "999",
        ])
        .parse_validated()
        .unwrap();
        assert_eq!(
            override_gen.health.policy().unwrap().slot_clock,
            SlotClock::new(999, 12)
        );

        let sepolia = parse(&[
            "--cl-beacon-url",
            "http://127.0.0.1:5052",
            "--network",
            "sepolia",
        ])
        .parse_validated()
        .unwrap();
        assert_eq!(
            sepolia.health.policy().unwrap().slot_clock,
            SlotClock::new(1_655_733_600, 12)
        );

        for missing in [
            &[
                "--cl-beacon-url",
                "http://127.0.0.1:5052",
                "--network",
                "custom",
            ][..],
            &[
                "--cl-beacon-url",
                "http://127.0.0.1:5052",
                "--network",
                "custom",
                "--cl-genesis-time",
                "1700000000",
            ],
            // Slot length given, genesis missing: only the missing-genesis check rejects this.
            &[
                "--cl-beacon-url",
                "http://127.0.0.1:5052",
                "--network",
                "custom",
                "--cl-seconds-per-slot",
                "12",
            ],
        ] {
            assert!(parse(missing).validate().is_err(), "{missing:?}");
        }

        let custom = parse(&[
            "--cl-beacon-url",
            "http://127.0.0.1:5052",
            "--network",
            "custom",
            "--cl-genesis-time",
            "1700000000",
            "--cl-seconds-per-slot",
            "6",
        ])
        .parse_validated()
        .unwrap();
        assert_eq!(
            custom.health.policy().unwrap().slot_clock,
            SlotClock::new(1_700_000_000, 6)
        );

        let disabled = parse(&[
            "--cl-beacon-url",
            "http://127.0.0.1:5052",
            "--cl-genesis-time",
            "0",
        ])
        .parse_validated()
        .unwrap();
        assert!(disabled.upstreams.cl.is_some());
        assert_eq!(disabled.health.policy().unwrap().slot_clock, None);
    }

    #[test]
    fn cli_defaults_and_flag_forms() {
        let def = parse(&[]);
        assert!(!def.readyz_strict);
        assert!(!def.access_log);
        assert!(!def.trust_upstream);
        assert_eq!(def.health_poll_interval, Duration::from_secs(1));
        assert_eq!(def.log_level, "info");

        assert!(parse(&["--readyz-strict"]).readyz_strict);
        assert!(parse(&["--readyz-strict", "true"]).readyz_strict);
        assert!(!parse(&["--readyz-strict", "false"]).readyz_strict);
        assert!(parse(&["--access-log"]).access_log);
        assert!(parse(&["--trust-upstream"]).trust_upstream);

        assert_eq!(
            parse(&["--listen", "127.0.0.1:1", "--listen", "127.0.0.1:2"]).listen,
            vec!["127.0.0.1:1", "127.0.0.1:2"]
        );
        assert_eq!(
            parse(&["--listen", "127.0.0.1:1,127.0.0.1:2,127.0.0.1:3"]).listen,
            vec!["127.0.0.1:1", "127.0.0.1:2", "127.0.0.1:3"]
        );

        let argv = [
            "ethonyx",
            "--el-http-url",
            "http://127.0.0.1:8545",
            "--log-level",
        ];
        assert!(Config::try_parse_from(argv.into_iter().chain(["info"])).is_ok());
        assert!(Config::try_parse_from(argv.into_iter().chain(["bogus"])).is_err());

        assert_eq!(parse_secs("30"), Ok(Duration::from_secs(30)));
        assert_eq!(parse_secs("0"), Ok(Duration::from_secs(0)));
        for bad in ["abc", "3.14", "-1"] {
            assert!(parse_secs(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn debug_redacts_secrets_across_all_url_fields() {
        let args = [
            "--el-http-url",
            "https://user:EL_PASS@el.example.com:8545/v3/EL_API_KEY?tok=EL_TOK",
            "--el-ws-url",
            "wss://user:WS_PASS@ws.example.com/v2/WS_API_KEY",
            "--cl-beacon-url",
            "https://cl.example.com?api_key=CL_API_KEY",
            "--cl-mev-relay-url",
            "https://relay:MEV_PASS@mev.example.com/MEV_TOKEN",
            "--el-reference-url",
            "https://ref1:REF1_PASS@ref1.example.com/v2/REF1_KEY,https://ref2.example.com?apikey=REF2_KEY",
            "--otel-endpoint",
            "https://otel:OTEL_PASS@collector.example.com/OTEL_PATH",
        ];
        let cfg = parse(&args);
        let dbg = format!("{cfg:?}");
        for secret in [
            "EL_PASS",
            "EL_API_KEY",
            "EL_TOK",
            "WS_PASS",
            "WS_API_KEY",
            "CL_API_KEY",
            "MEV_PASS",
            "MEV_TOKEN",
            "REF1_PASS",
            "REF1_KEY",
            "REF2_KEY",
            "OTEL_PASS",
            "OTEL_PATH",
        ] {
            assert!(!dbg.contains(secret), "leaked {secret} in {dbg}");
        }
        assert!(dbg.contains("https://<redacted>@el.example.com:8545/<redacted>"));
        assert!(dbg.contains("wss://<redacted>@ws.example.com/<redacted>"));
        assert!(dbg.contains("https://cl.example.com/<redacted>"));
        assert!(dbg.contains("https://<redacted>@mev.example.com/<redacted>"));
        assert!(dbg.contains("https://<redacted>@ref1.example.com/<redacted>"));
        assert!(dbg.contains("https://ref2.example.com/<redacted>"));

        let from_str = UrlArg::from("https://rpc.example.com/SECRET");
        assert_eq!(
            format!("{from_str:?}"),
            "\"https://rpc.example.com/<redacted>\""
        );
    }

    #[test]
    fn mode_prerequisite_and_reference_dedup_guards() {
        // `--el-http-url` has no implicit default, while `--listen` defaults to `0.0.0.0:8547`
        // and `--el-ws-url` / `--cl-beacon-url` / `--el-reference-url` default to disabled (`None`).
        assert!(Config::try_parse_from(["ethonyx"]).is_err());
        assert!(Config::try_parse_from(["ethonyx", "--el-ws-url", "ws://127.0.0.1:8546"]).is_err());

        let bare_cfg = parse(&[]);
        assert_eq!(bare_cfg.listen, vec!["0.0.0.0:8547"]);
        let bare = bare_cfg.parse_validated().unwrap();
        assert!(bare.upstreams.el_ws.is_none());
        assert!(bare.upstreams.cl.is_none());
        assert_eq!(bare.health.reference_count(), 0);
        let bare_policy = bare.health.policy().unwrap();
        assert!(bare_policy.strict.is_none());
        assert!(bare_policy.slot_clock.is_none());

        // CL-specific flags require `--cl-beacon-url`.
        for args in [
            &["--network", "hoodi"][..],
            &["--cl-mev-relay-url", "http://127.0.0.1:18550"],
            &["--cl-genesis-time", "0"],
            &["--cl-seconds-per-slot", "12"],
            &["--cl-max-slot-age-secs", "30"],
            &["--mock-cl-subscriptions"],
            &["--mock-cl-node-health"],
        ] {
            let err = parse(args).validate().unwrap_err().to_string();
            assert!(
                err.contains("requires --cl-beacon-url"),
                "{args:?} -> {err}"
            );
        }

        // Reference-specific flags require `--el-reference-url`.
        for args in [
            &["--el-max-reference-lag", "2"][..],
            &["--el-reference-poll-interval", "5"],
        ] {
            let err = parse(args).validate().unwrap_err().to_string();
            assert!(
                err.contains("requires --el-reference-url"),
                "{args:?} -> {err}"
            );
        }

        // `--trust-upstream` rejects polling-only flags.
        for args in [
            &["--trust-upstream", "--readyz-strict"][..],
            &[
                "--trust-upstream",
                "--el-reference-url",
                "http://127.0.0.1:9545",
            ],
        ] {
            let err = parse(args).validate().unwrap_err().to_string();
            assert!(err.contains("--trust-upstream"), "{args:?} -> {err}");
        }

        // Empty reference entries and all-self copy-paste mistakes are rejected.
        for val in [
            "",
            "   ",
            "http://127.0.0.1:9545,",
            "http://127.0.0.1:8545",
            "HTTP://127.0.0.1:8545/",
        ] {
            assert!(
                parse(&["--el-reference-url", val]).validate().is_err(),
                "must reject {val:?}"
            );
        }

        // Shared ConfigMap self-URLs matching `--el-http-url` are stripped, case/default-port
        // duplicates collapse, and `/v1` vs `/v1/` remain distinct.
        let deduped = parse(&[
            "--el-http-url",
            "http://node-1.local:80",
            "--el-reference-url",
            "http://NODE-1.local, http://node-2.local:80/v1, HTTP://Node-2.local/v1, http://node-2.local/v1/",
        ])
        .parse_validated()
        .unwrap();
        let HealthMode::Polled(polled) = deduped.health else {
            panic!("expected polled reference mode");
        };
        let refs = polled.reference.expect("expected reference upstreams");
        assert_eq!(refs.upstreams.len(), 2);
        assert_eq!(refs.upstreams[0].uri(), "http://node-2.local:80/v1");
        assert_eq!(refs.upstreams[1].uri(), "http://node-2.local/v1/");
        assert_eq!(refs.poll_interval, Duration::from_secs(3));

        // With `--cl-beacon-url` present, slot duration `0` and `wss://` MEV schemes still fail validation.
        assert!(
            parse(&[
                "--cl-beacon-url",
                "http://127.0.0.1:5052",
                "--cl-seconds-per-slot",
                "0"
            ])
            .validate()
            .unwrap_err()
            .to_string()
            .contains("must be at least 1")
        );
        assert!(
            parse(&[
                "--cl-beacon-url",
                "http://127.0.0.1:5052",
                "--cl-mev-relay-url",
                "wss://127.0.0.1:18550"
            ])
            .validate()
            .unwrap_err()
            .to_string()
            .contains("scheme must be http or https")
        );
    }

    #[test]
    fn validate_checks_timeouts_schemes_and_allows_zero_grace_and_sse_disable() {
        let default_cfg = parse(&[]);
        assert_eq!(default_cfg.sse_idle_timeout, Duration::from_secs(900));
        assert_eq!(default_cfg.max_connections, 4096);
        assert!(default_cfg.validate().is_ok());

        let zero_optional = parse(&[
            "--shutdown-grace",
            "0",
            "--sse-idle-timeout",
            "0",
            "--max-connections",
            "0",
        ]);
        assert!(zero_optional.validate().is_ok());

        let trimmed_listen = parse(&[
            "--listen",
            " 127.0.0.1:8547 , [::1]:8548 , [fe80::1%lo0]:8549 , [fe80::1%1]:18999 , localhost:0 ",
        ]);
        let validated = trimmed_listen.parse_validated().unwrap();
        assert_eq!(
            validated
                .listen
                .iter()
                .map(|addr| (&*addr.host, addr.port, addr.to_string()))
                .collect::<Vec<_>>(),
            vec![
                ("127.0.0.1", 8547, "127.0.0.1:8547".to_owned()),
                ("::1", 8548, "[::1]:8548".to_owned()),
                ("fe80::1%lo0", 8549, "[fe80::1%lo0]:8549".to_owned()),
                ("fe80::1%1", 18999, "[fe80::1%1]:18999".to_owned()),
                ("localhost", 0, "localhost:0".to_owned()),
            ]
        );

        let max_permits_overflow = (tokio::sync::Semaphore::MAX_PERMITS + 1).to_string();
        for (flag, val, expected_substr) in [
            ("--listen", "", "entry must not be empty"),
            ("--listen", "   ", "entry must not be empty"),
            ("--listen", "127.0.0.1:8547,", "entry must not be empty"),
            (
                "--listen",
                "127.0.0.1:8547,127.0.0.1:8547",
                "duplicate --listen",
            ),
            (
                "--listen",
                "LocalHost:8547,localhost:8547",
                "duplicate --listen",
            ),
            ("--listen", "127.0.0.1", "--listen"),
            ("--listen", "18823", "--listen"),
            ("--listen", ":8547", "--listen"),
            ("--listen", "[::1:8547", "--listen"),
            ("--listen", "foo]bar:8547", "--listen"),
            ("--listen", "foo\0bar:8547", "--listen"),
            ("--listen", "[fe80::1%]:8547", "--listen"),
            ("--listen", "[fe80::1%lo\0]:8547", "--listen"),
            ("--listen", "127.0.0.1:99999", "--listen"),
            ("--listen", "127.0.0.1:+8547", "--listen"),
            ("--listen", "127.0.0.1:0080", "--listen"),
            ("--listen", "http://127.0.0.1:8547", "--listen"),
            (
                "--max-connections",
                max_permits_overflow.as_str(),
                "max-connections",
            ),
            ("--health-poll-interval", "0", "health-poll-interval"),
            ("--health-timeout", "0", "health-timeout"),
            ("--proxy-timeout", "0", "proxy-timeout"),
            ("--el-http-url", "ws://127.0.0.1:8545", "--el-http-url"),
            ("--el-ws-url", "http://127.0.0.1:8546", "--el-ws-url"),
            ("--cl-beacon-url", "ws://127.0.0.1:5052", "--cl-beacon-url"),
            ("--otel-endpoint", "http://:4318", "--otel-endpoint"),
        ] {
            let err = parse(&[flag, val])
                .validate()
                .expect_err("must reject invalid config")
                .to_string();
            assert!(
                err.contains(expected_substr),
                "expected {expected_substr} in {err}"
            );
        }

        // Duplicate ephemeral `:0` bindings stay allowed so callers can request N dynamic ports.
        assert!(
            parse(&["--listen", "127.0.0.1:0,127.0.0.1:0"])
                .validate()
                .is_ok()
        );

        // Custom network with `--cl-genesis-time 0` (slot age disabled) or `--trust-upstream`
        // does not demand unused slot timing parameters, while explicit `0` slot seconds still fails.
        let custom_disabled = parse(&[
            "--cl-beacon-url",
            "http://127.0.0.1:5052",
            "--network",
            "custom",
            "--cl-genesis-time",
            "0",
        ])
        .parse_validated()
        .unwrap();
        assert_eq!(custom_disabled.health.policy().unwrap().slot_clock, None);
        assert!(
            parse(&[
                "--cl-beacon-url",
                "http://127.0.0.1:5052",
                "--network",
                "custom",
                "--trust-upstream",
            ])
            .validate()
            .is_ok()
        );

        // Dependent zero-duration checks that require their prerequisite URL flag.
        let ref_zero = parse(&[
            "--el-reference-url",
            "http://127.0.0.1:8546",
            "--el-reference-poll-interval",
            "0",
        ])
        .validate()
        .unwrap_err()
        .to_string();
        assert!(
            ref_zero.contains("el-reference-poll-interval")
                && ref_zero.contains("greater than zero"),
            "{ref_zero}"
        );
        assert!(
            parse(&[
                "--trust-upstream",
                "--cl-beacon-url",
                "http://127.0.0.1:5052",
                "--cl-seconds-per-slot",
                "0",
            ])
            .validate()
            .unwrap_err()
            .to_string()
            .contains("must be at least 1")
        );

        // Programmatic `Config` construction with an unknown `log_level` is rejected.
        let mut bad_log = parse(&[]);
        bad_log.log_level = "verbose".to_owned();
        assert!(
            bad_log
                .validate()
                .unwrap_err()
                .to_string()
                .contains("--log-level")
        );

        // Without the `otel` feature, setting `--otel-endpoint` fails loudly instead of silently ignoring.
        #[cfg(not(feature = "otel"))]
        {
            let err = parse(&["--otel-endpoint", "http://127.0.0.1:4318"])
                .validate()
                .unwrap_err()
                .to_string();
            assert!(err.contains("otel"), "unexpected error: {err}");
        }
        // Upper-bound checks prevent `Instant::now() + duration` panics via both CLI and library Config.
        assert_eq!(parse_secs(&MAX_TIMEOUT_SECS.to_string()), Ok(MAX_TIMEOUT));
        assert!(parse_secs(&(MAX_TIMEOUT_SECS + 1).to_string()).is_err());
        assert!(parse_secs(&u64::MAX.to_string()).is_err());
        let mut over = parse(&[]);
        over.shutdown_grace = Duration::MAX;
        assert!(
            over.validate()
                .unwrap_err()
                .to_string()
                .contains("shutdown-grace")
        );

        // Library `Config` deliberately accepts non-zero sub-second `Duration`s even though
        // CLI flags parse integer seconds.
        let mut sub_sec = parse(&["--el-reference-url", "http://127.0.0.1:8546"]);
        sub_sec.health_poll_interval = Duration::from_millis(50);
        sub_sec.health_timeout = Duration::from_millis(150);
        sub_sec.proxy_timeout = Duration::from_millis(250);
        sub_sec.el_reference_poll_interval = Some(Duration::from_millis(60));
        assert!(sub_sec.validate().is_ok());
    }

    #[tokio::test]
    async fn listen_addr_resolves_numeric_and_named_ipv6_zone_ids() {
        use std::net::SocketAddr;

        #[cfg(target_os = "macos")]
        let named = "[fe80::1%lo0]:0";
        #[cfg(target_os = "linux")]
        let named = "[fe80::1%lo]:0";

        let cfg = Config::parse_from([
            "ethonyx",
            "--listen",
            "[fe80::1%1]:0",
            "--el-http-url",
            "http://127.0.0.1:8545",
        ]);
        let validated = cfg.parse_validated().unwrap();
        let addr = &validated.listen[0];
        let resolved: Vec<_> = tokio::net::lookup_host((&*addr.host, addr.port))
            .await
            .expect("numeric IPv6 zone ID must resolve")
            .collect();
        assert!(matches!(resolved.as_slice(), [SocketAddr::V6(v6), ..] if v6.scope_id() == 1));

        #[cfg(any(target_os = "macos", target_os = "linux"))]
        {
            let cfg = Config::parse_from([
                "ethonyx",
                "--listen",
                named,
                "--el-http-url",
                "http://127.0.0.1:8545",
            ]);
            let validated = cfg.parse_validated().unwrap();
            let addr = &validated.listen[0];
            let resolved: Vec<_> = tokio::net::lookup_host((&*addr.host, addr.port))
                .await
                .expect("named IPv6 zone ID must resolve via OS getaddrinfo")
                .collect();
            assert!(matches!(resolved.as_slice(), [SocketAddr::V6(v6), ..] if v6.scope_id() != 0));

            #[cfg(target_os = "macos")]
            {
                let listener = tokio::net::TcpListener::bind((&*addr.host, addr.port))
                    .await
                    .expect("macOS lo0 must bind [fe80::1%lo0]:0");
                let local = listener.local_addr().unwrap();
                assert!(matches!(local, SocketAddr::V6(v6) if v6.scope_id() != 0));
            }
        }
    }
}
