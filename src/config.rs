use std::fmt;
use std::num::NonZeroU64;
use std::time::Duration;

use clap::{Parser, ValueEnum};

use crate::BoxError;
use crate::upstream::{HttpUpstream, RedactedUrl, WsUpstream, parse_canonical_port};

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

#[derive(Clone, Parser)]
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

    /// Execution-layer JSON-RPC over HTTP upstream URL.
    #[arg(
        long,
        env = "ETHONYX_EL_HTTP_URL",
        default_value = "http://127.0.0.1:8545"
    )]
    pub el_http_url: String,

    /// Execution-layer JSON-RPC over WebSocket upstream URL.
    #[arg(long, env = "ETHONYX_EL_WS_URL", default_value = "ws://127.0.0.1:8546")]
    pub el_ws_url: String,

    /// Consensus-layer Beacon API upstream URL (REST, /eth/v1/...).
    #[arg(
        long,
        env = "ETHONYX_CL_BEACON_URL",
        default_value = "http://127.0.0.1:5052"
    )]
    pub cl_beacon_url: String,

    /// Optional Consensus-layer MEV Relay / MEV-Boost upstream URL (REST, /eth/v1/builder/validators).
    /// When specified, POST /eth/v1/validator/register_validator requests are rewritten to
    /// /eth/v1/builder/validators and routed to this URL.
    #[arg(long, env = "ETHONYX_CL_MEV_RELAY_URL")]
    pub cl_mev_relay_url: Option<String>,

    /// Beacon network. Selects defaults for cl-genesis-time / cl-seconds-per-slot.
    /// Use `custom` for private chains and pass both values explicitly.
    #[arg(long, env = "ETHONYX_NETWORK", value_enum, default_value_t = Network::Mainnet)]
    pub network: Network,

    /// Maximum EL block age (seconds) used to gate `/readyz` under
    /// `--readyz-strict`. `/healthz` always reports the raw age regardless.
    #[arg(long, env = "ETHONYX_EL_MAX_BLOCK_AGE_SECS", default_value_t = 60)]
    pub el_max_block_age_secs: u64,

    /// Maximum CL head_slot age (seconds, wall-clock) used to gate `/readyz`
    /// under `--readyz-strict`. `/healthz` always reports the raw age regardless.
    #[arg(long, env = "ETHONYX_CL_MAX_SLOT_AGE_SECS", default_value_t = 60)]
    pub cl_max_slot_age_secs: u64,

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
    /// regardless of probe rate. Must be at least 1 second.
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

    /// Statically mock GET /eth/v1/node/health requests locally with 200 OK.
    #[arg(
        long,
        env = "ETHONYX_MOCK_CL_NODE_HEALTH",
        default_value_t = false,
        action = clap::ArgAction::Set,
        num_args = 0..=1,
        default_missing_value = "true"
    )]
    pub mock_cl_node_health: bool,

    /// Upstream timeout for proxied requests (seconds; must be at least 1).
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

    /// OpenTelemetry collector endpoint (HTTP) for exporting traces/metrics.
    #[cfg(feature = "otel")]
    #[arg(long, env = "ETHONYX_OTEL_ENDPOINT")]
    pub otel_endpoint: Option<String>,
}

/// Validated beacon-chain slot timing (`None` when `--cl-genesis-time 0` disables slot-age checks).
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
    ipv6: bool,
}

impl ListenAddr {
    fn parse(raw: &str) -> Result<Self, BoxError> {
        let addr = raw.trim();
        if addr.is_empty() {
            return Err("no listen addresses configured".into());
        }
        let (host, port_str, ipv6) = if let Some(rest) = addr.strip_prefix('[') {
            let Some((ipv6_host, port_part)) = rest.split_once(']') else {
                return Err(
                    format!("invalid --listen address '{addr}': unclosed IPv6 bracket").into(),
                );
            };
            let Some(port_str) = port_part.strip_prefix(':') else {
                return Err(
                    format!("invalid --listen address '{addr}': expected [ipv6]:port").into(),
                );
            };
            let valid_ipv6 = match ipv6_host.split_once('%') {
                Some((addr_part, zone)) => {
                    !zone.is_empty()
                        && !zone.bytes().any(|b| b.is_ascii_whitespace())
                        && addr_part.parse::<std::net::Ipv6Addr>().is_ok()
                }
                None => ipv6_host.parse::<std::net::Ipv6Addr>().is_ok(),
            };
            if !valid_ipv6 {
                return Err(format!("invalid --listen address '{addr}': invalid IPv6 host").into());
            }
            (ipv6_host, port_str, true)
        } else {
            let Some((host, port_str)) = addr.rsplit_once(':') else {
                return Err(
                    format!("invalid --listen address '{addr}': expected host:port").into(),
                );
            };
            if host.is_empty()
                || host.contains(':')
                || host
                    .bytes()
                    .any(|b| b.is_ascii_whitespace() || b == b'/' || b == b'@')
            {
                return Err(format!("invalid --listen address '{addr}': invalid host").into());
            }
            (host, port_str, false)
        };
        let Some(port) = parse_canonical_port(port_str, true) else {
            return Err(format!("invalid --listen address '{addr}': invalid port").into());
        };
        Ok(Self {
            host: Box::from(host),
            port,
            ipv6,
        })
    }
}

impl fmt::Display for ListenAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.ipv6 {
            write!(f, "[{}]:{}", self.host, self.port)
        } else {
            write!(f, "{}:{}", self.host, self.port)
        }
    }
}

/// Scalar runtime options retained by [`crate::state::AppState`] after startup
/// so heap-allocated CLI URL/listen strings in [`Config`] can be freed early.
#[derive(Clone, Copy)]
pub(crate) struct RuntimeOpts {
    pub(crate) el_max_block_age_secs: u64,
    pub(crate) cl_max_slot_age_secs: u64,
    pub(crate) health_timeout: Duration,
    pub(crate) proxy_timeout: Duration,
    pub(crate) sse_idle_timeout: Duration,
    pub(crate) max_connections: usize,
    pub(crate) readyz_strict: bool,
    pub(crate) mock_eth_syncing: bool,
    pub(crate) mock_cl_subscriptions: bool,
    pub(crate) mock_cl_node_health: bool,
}

impl RuntimeOpts {
    pub(crate) const fn from_config(cfg: &Config) -> Self {
        Self {
            el_max_block_age_secs: cfg.el_max_block_age_secs,
            cl_max_slot_age_secs: cfg.cl_max_slot_age_secs,
            health_timeout: cfg.health_timeout,
            proxy_timeout: cfg.proxy_timeout,
            sse_idle_timeout: cfg.sse_idle_timeout,
            max_connections: cfg.max_connections,
            readyz_strict: cfg.readyz_strict,
            mock_eth_syncing: cfg.mock_eth_syncing,
            mock_cl_subscriptions: cfg.mock_cl_subscriptions,
            mock_cl_node_health: cfg.mock_cl_node_health,
        }
    }
}

pub(crate) struct ValidatedConfig {
    pub(crate) listen: Vec<ListenAddr>,
    pub(crate) el_http: HttpUpstream,
    pub(crate) el_ws: WsUpstream,
    pub(crate) cl_beacon: HttpUpstream,
    pub(crate) cl_mev_relay: Option<HttpUpstream>,
    pub(crate) slot_clock: Option<SlotClock>,
    #[cfg(feature = "otel")]
    pub(crate) otel_endpoint: Option<HttpUpstream>,
}

impl Config {
    pub const fn resolve_cl_genesis_time(&self) -> Result<u64, &'static str> {
        match self.cl_genesis_time {
            Some(t) => Ok(t),
            None => match self.network.preset_genesis_time() {
                Some(t) => Ok(t),
                None => Err(
                    "--network custom requires --cl-genesis-time (use 0 to disable slot-age check)",
                ),
            },
        }
    }

    pub const fn resolve_cl_seconds_per_slot(&self) -> Result<u64, &'static str> {
        let secs = match self.cl_seconds_per_slot {
            Some(s) => s,
            None => match self.network.preset_seconds_per_slot() {
                Some(s) => s,
                None => return Err("--network custom requires --cl-seconds-per-slot"),
            },
        };
        if secs == 0 {
            return Err("cl-seconds-per-slot must be at least 1");
        }
        Ok(secs)
    }

    /// Validate all CLI/library configuration invariants and upstream URLs.
    pub fn validate(&self) -> Result<(), BoxError> {
        self.parse_validated().map(|_| ())
    }

    pub(crate) fn parse_validated(&self) -> Result<ValidatedConfig, BoxError> {
        if self.listen.is_empty() {
            return Err("no listen addresses configured".into());
        }
        let listen = self
            .listen
            .iter()
            .map(|raw| ListenAddr::parse(raw))
            .collect::<Result<Vec<_>, _>>()?;
        if self.max_connections > tokio::sync::Semaphore::MAX_PERMITS {
            return Err(format!(
                "max-connections must be at most {}",
                tokio::sync::Semaphore::MAX_PERMITS
            )
            .into());
        }
        if self.health_poll_interval.is_zero() {
            return Err("health-poll-interval must be at least 1 second".into());
        }
        if self.health_timeout.is_zero() {
            return Err("health-timeout must be at least 1 second".into());
        }
        if self.proxy_timeout.is_zero() {
            return Err("proxy-timeout must be at least 1 second".into());
        }
        let cl_genesis_time = self.resolve_cl_genesis_time()?;
        let cl_seconds_per_slot = self.resolve_cl_seconds_per_slot()?;
        let el_http = HttpUpstream::parse("--el-http-url", &self.el_http_url)?;
        let el_ws = WsUpstream::parse("--el-ws-url", &self.el_ws_url)?;
        let cl_beacon = HttpUpstream::parse("--cl-beacon-url", &self.cl_beacon_url)?;
        let cl_mev_relay = self
            .cl_mev_relay_url
            .as_deref()
            .map(|u| HttpUpstream::parse("--cl-mev-relay-url", u))
            .transpose()?;
        #[cfg(feature = "otel")]
        let otel_endpoint = self
            .otel_endpoint
            .as_deref()
            .map(crate::otel::parse_endpoint)
            .transpose()?;
        Ok(ValidatedConfig {
            listen,
            el_http,
            el_ws,
            cl_beacon,
            cl_mev_relay,
            slot_clock: SlotClock::new(cl_genesis_time, cl_seconds_per_slot),
            #[cfg(feature = "otel")]
            otel_endpoint,
        })
    }
}

impl fmt::Debug for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            listen,
            el_http_url,
            el_ws_url,
            cl_beacon_url,
            cl_mev_relay_url,
            network,
            el_max_block_age_secs,
            cl_max_slot_age_secs,
            readyz_strict,
            cl_genesis_time,
            cl_seconds_per_slot,
            health_timeout,
            health_poll_interval,
            log_level,
            access_log,
            trust_upstream,
            mock_eth_syncing,
            mock_cl_subscriptions,
            mock_cl_node_health,
            proxy_timeout,
            sse_idle_timeout,
            max_connections,
            shutdown_grace,
            #[cfg(feature = "otel")]
            otel_endpoint,
        } = self;

        let mut ds = f.debug_struct("Config");
        ds.field("listen", listen)
            .field("el_http_url", &RedactedUrl(el_http_url))
            .field("el_ws_url", &RedactedUrl(el_ws_url))
            .field("cl_beacon_url", &RedactedUrl(cl_beacon_url))
            .field(
                "cl_mev_relay_url",
                &cl_mev_relay_url.as_deref().map(RedactedUrl),
            )
            .field("network", network)
            .field("el_max_block_age_secs", el_max_block_age_secs)
            .field("cl_max_slot_age_secs", cl_max_slot_age_secs)
            .field("readyz_strict", readyz_strict)
            .field("cl_genesis_time", cl_genesis_time)
            .field("cl_seconds_per_slot", cl_seconds_per_slot)
            .field("health_timeout", health_timeout)
            .field("health_poll_interval", health_poll_interval)
            .field("log_level", log_level)
            .field("access_log", access_log)
            .field("trust_upstream", trust_upstream)
            .field("mock_eth_syncing", mock_eth_syncing)
            .field("mock_cl_subscriptions", mock_cl_subscriptions)
            .field("mock_cl_node_health", mock_cl_node_health)
            .field("proxy_timeout", proxy_timeout)
            .field("sse_idle_timeout", sse_idle_timeout)
            .field("max_connections", max_connections)
            .field("shutdown_grace", shutdown_grace);
        #[cfg(feature = "otel")]
        ds.field("otel_endpoint", &otel_endpoint.as_deref().map(RedactedUrl));
        ds.finish()
    }
}

fn parse_secs(s: &str) -> Result<Duration, String> {
    s.parse::<u64>()
        .map(Duration::from_secs)
        .map_err(|e| format!("invalid seconds value: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn presets_match_known_genesis_times() {
        assert_eq!(Network::Mainnet.preset_genesis_time(), Some(1_606_824_023));
        assert_eq!(Network::Hoodi.preset_genesis_time(), Some(1_742_213_400));
        assert_eq!(Network::Sepolia.preset_genesis_time(), Some(1_655_733_600));
        assert_eq!(Network::Holesky.preset_genesis_time(), Some(1_695_902_400));
    }

    #[test]
    fn presets_use_ethereum_slot_duration() {
        for n in [
            Network::Mainnet,
            Network::Hoodi,
            Network::Sepolia,
            Network::Holesky,
        ] {
            assert_eq!(n.preset_seconds_per_slot(), Some(12));
        }
    }

    #[test]
    fn custom_has_no_presets() {
        assert_eq!(Network::Custom.preset_genesis_time(), None);
        assert_eq!(Network::Custom.preset_seconds_per_slot(), None);
    }

    fn parse(extra: &[&str]) -> Config {
        let mut argv = vec!["ethonyx"];
        argv.extend_from_slice(extra);
        Config::try_parse_from(argv).expect("clap parse")
    }

    #[test]
    fn explicit_genesis_overrides_network_preset() {
        let cfg = parse(&["--network", "hoodi", "--cl-genesis-time", "999"]);
        assert_eq!(cfg.resolve_cl_genesis_time().unwrap(), 999);
    }

    #[test]
    fn network_preset_used_when_genesis_not_provided() {
        let cfg = parse(&["--network", "sepolia"]);
        assert_eq!(cfg.resolve_cl_genesis_time().unwrap(), 1_655_733_600);
        assert_eq!(cfg.resolve_cl_seconds_per_slot().unwrap(), 12);
    }

    #[test]
    fn custom_without_genesis_fails() {
        let cfg = parse(&["--network", "custom"]);
        assert!(cfg.resolve_cl_genesis_time().is_err());
        assert!(cfg.resolve_cl_seconds_per_slot().is_err());
    }

    #[test]
    fn custom_with_explicit_values_resolves() {
        let cfg = parse(&[
            "--network",
            "custom",
            "--cl-genesis-time",
            "1700000000",
            "--cl-seconds-per-slot",
            "6",
        ]);
        assert_eq!(cfg.resolve_cl_genesis_time().unwrap(), 1_700_000_000);
        assert_eq!(cfg.resolve_cl_seconds_per_slot().unwrap(), 6);
    }

    #[test]
    fn cl_genesis_zero_disables_check_but_still_resolves() {
        let cfg = parse(&["--cl-genesis-time", "0"]);
        assert_eq!(cfg.resolve_cl_genesis_time().unwrap(), 0);
    }

    #[test]
    fn listen_accepts_repeated_flag() {
        let cfg = parse(&["--listen", "127.0.0.1:1", "--listen", "127.0.0.1:2"]);
        assert_eq!(cfg.listen, vec!["127.0.0.1:1", "127.0.0.1:2"]);
    }

    #[test]
    fn listen_accepts_comma_separated() {
        let cfg = parse(&["--listen", "127.0.0.1:1,127.0.0.1:2,127.0.0.1:3"]);
        assert_eq!(cfg.listen.len(), 3);
        assert_eq!(cfg.listen[2], "127.0.0.1:3");
    }

    #[test]
    fn readyz_strict_defaults_off() {
        assert!(!parse(&[]).readyz_strict);
    }

    #[test]
    fn readyz_strict_bare_flag_enables() {
        assert!(parse(&["--readyz-strict"]).readyz_strict);
    }

    #[test]
    fn readyz_strict_accepts_explicit_bool() {
        assert!(parse(&["--readyz-strict", "true"]).readyz_strict);
        assert!(!parse(&["--readyz-strict", "false"]).readyz_strict);
    }

    #[test]
    fn health_poll_interval_defaults_to_1s() {
        assert_eq!(parse(&[]).health_poll_interval, Duration::from_secs(1));
    }

    #[test]
    fn log_level_defaults_to_info() {
        assert_eq!(parse(&[]).log_level, "info");
    }

    #[test]
    fn access_log_defaults_off() {
        assert!(!parse(&[]).access_log);
    }

    #[test]
    fn access_log_bare_flag_enables() {
        assert!(parse(&["--access-log"]).access_log);
    }

    #[test]
    fn trust_upstream_defaults_off() {
        assert!(!parse(&[]).trust_upstream);
    }

    #[test]
    fn trust_upstream_bare_flag_enables() {
        assert!(parse(&["--trust-upstream"]).trust_upstream);
    }

    #[test]
    fn log_level_rejects_unknown_value() {
        assert!(Config::try_parse_from(["ethonyx", "--log-level", "bogus"]).is_err());
    }

    #[test]
    fn parse_secs_accepts_integer() {
        assert_eq!(parse_secs("30").unwrap(), Duration::from_secs(30));
        assert_eq!(parse_secs("0").unwrap(), Duration::from_secs(0));
    }

    #[test]
    fn parse_secs_rejects_non_integer() {
        assert!(parse_secs("abc").is_err());
        assert!(parse_secs("3.14").is_err());
        assert!(parse_secs("-1").is_err());
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
            #[cfg(feature = "otel")]
            "--otel-endpoint",
            #[cfg(feature = "otel")]
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
            "OTEL_PASS",
            "OTEL_PATH",
        ] {
            assert!(!dbg.contains(secret), "leaked {secret} in {dbg}");
        }
        assert!(dbg.contains("https://<redacted>@el.example.com:8545/<redacted>"));
        assert!(dbg.contains("wss://<redacted>@ws.example.com/<redacted>"));
        assert!(dbg.contains("https://cl.example.com/<redacted>"));
        assert!(dbg.contains("https://<redacted>@mev.example.com/<redacted>"));
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
            ("--listen", "", "listen"),
            ("--listen", "   ", "listen"),
            ("--listen", "127.0.0.1", "--listen"),
            ("--listen", "18823", "--listen"),
            ("--listen", ":8547", "--listen"),
            ("--listen", "[::1:8547", "--listen"),
            ("--listen", "[fe80::1%]:8547", "--listen"),
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
            ("--cl-seconds-per-slot", "0", "cl-seconds-per-slot"),
            ("--el-http-url", "ws://127.0.0.1:8545", "--el-http-url"),
            ("--el-ws-url", "http://127.0.0.1:8546", "--el-ws-url"),
            ("--cl-beacon-url", "ws://127.0.0.1:5052", "--cl-beacon-url"),
            (
                "--cl-mev-relay-url",
                "wss://127.0.0.1:18550",
                "--cl-mev-relay-url",
            ),
            #[cfg(feature = "otel")]
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
    }
}
