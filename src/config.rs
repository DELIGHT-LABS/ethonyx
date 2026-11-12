use std::time::Duration;

use clap::{Parser, ValueEnum};

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
    pub fn preset_genesis_time(self) -> Option<u64> {
        match self {
            Network::Mainnet => Some(1_606_824_023),
            Network::Hoodi => Some(1_742_213_400),
            Network::Sepolia => Some(1_655_733_600),
            Network::Holesky => Some(1_695_902_400),
            Network::Custom => None,
        }
    }

    pub fn preset_seconds_per_slot(self) -> Option<u64> {
        match self {
            Network::Custom => None,
            _ => Some(12),
        }
    }
}

#[derive(Debug, Clone, Parser)]
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

    /// Upstream timeout for proxied requests (seconds).
    #[arg(long, env = "ETHONYX_PROXY_TIMEOUT", default_value = "60", value_parser = parse_secs)]
    pub proxy_timeout: Duration,

    /// Shutdown grace period (seconds) for draining in-flight connections.
    #[arg(long, env = "ETHONYX_SHUTDOWN_GRACE", default_value = "10", value_parser = parse_secs)]
    pub shutdown_grace: Duration,

    /// OpenTelemetry collector endpoint (HTTP) for exporting traces/metrics.
    #[cfg(feature = "otel")]
    #[arg(long, env = "ETHONYX_OTEL_ENDPOINT")]
    pub otel_endpoint: Option<String>,
}

impl Config {
    pub fn resolve_cl_genesis_time(&self) -> Result<u64, &'static str> {
        match self.cl_genesis_time {
            Some(v) => Ok(v),
            None => self.network.preset_genesis_time().ok_or(
                "--network custom requires --cl-genesis-time (use 0 to disable slot-age check)",
            ),
        }
    }

    pub fn resolve_cl_seconds_per_slot(&self) -> Result<u64, &'static str> {
        match self.cl_seconds_per_slot {
            Some(v) => Ok(v),
            None => self
                .network
                .preset_seconds_per_slot()
                .ok_or("--network custom requires --cl-seconds-per-slot"),
        }
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
}
