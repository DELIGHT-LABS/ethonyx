use std::sync::OnceLock;
use std::time::Duration;

use ::metrics::{Counter, Gauge, Histogram};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle, PrometheusRecorder};

use crate::otel::{OtelActiveGuard, OtelMetrics};

pub(crate) const ACTIVE_CONNECTIONS: &str = "ethonyx_active_connections";
pub(crate) const CONNECTION_LIMIT_SATURATED: &str = "ethonyx_connection_limit_saturated_total";
pub(crate) const PROXY_REQUEST_DURATION: &str = "ethonyx_proxy_request_duration_seconds";
pub(crate) const PROXY_REQUESTS_TOTAL: &str = "ethonyx_proxy_requests_total";

/// Unarmed Prometheus + OpenTelemetry active-connection handle.
///
/// Captured before `tokio::spawn` for WebSocket relays so sessions activate the `ws`
/// gauge only after `on_upgrade` succeeds without retaining `Arc<AppState>`.
#[must_use]
pub(crate) struct ActiveGauge {
    prom: Gauge,
    otel: OtelActiveGuard,
}

impl ActiveGauge {
    pub(crate) fn activate(self) -> ActiveConnectionGuard {
        self.prom.increment(1.0);
        self.otel.add(1);
        ActiveConnectionGuard(self)
    }
}

/// Armed active-connection guard that decrements both gauges on drop.
#[must_use]
pub(crate) struct ActiveConnectionGuard(ActiveGauge);

impl Drop for ActiveConnectionGuard {
    fn drop(&mut self) {
        self.0.prom.decrement(1.0);
        self.0.otel.add(-1);
    }
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum UpstreamLayer {
    El = 0,
    // Must remain last; `Self::COUNT` is derived from this variant.
    Cl = 1,
}

impl UpstreamLayer {
    const COUNT: usize = Self::Cl as usize + 1;

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::El => "EL",
            Self::Cl => "CL",
        }
    }
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MethodBucket {
    Post = 0,
    Get,
    Connect,
    Head,
    Put,
    Delete,
    Options,
    Patch,
    Trace,
    // Must remain last; `Self::COUNT` is derived from this variant.
    Other,
}

impl MethodBucket {
    const COUNT: usize = Self::Other as usize + 1;

    pub(crate) fn classify(m: &http::Method) -> Self {
        match *m {
            http::Method::POST => Self::Post,
            http::Method::GET => Self::Get,
            http::Method::CONNECT => Self::Connect,
            http::Method::HEAD => Self::Head,
            http::Method::PUT => Self::Put,
            http::Method::DELETE => Self::Delete,
            http::Method::OPTIONS => Self::Options,
            http::Method::PATCH => Self::Patch,
            http::Method::TRACE => Self::Trace,
            _ => Self::Other,
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Post => "POST",
            Self::Get => "GET",
            Self::Connect => "CONNECT",
            Self::Head => "HEAD",
            Self::Put => "PUT",
            Self::Delete => "DELETE",
            Self::Options => "OPTIONS",
            Self::Patch => "PATCH",
            Self::Trace => "TRACE",
            Self::Other => "OTHER",
        }
    }
}

macro_rules! define_tracked_statuses {
    ($($code:literal),+ $(,)?) => {
        /// Status codes that get their own `ethonyx_proxy_requests_total` series (`50` bytes,
        /// strictly sorted for `binary_search`). Every other code shares the trailing
        /// `status="OTHER"` slot, which bounds the label set.
        const TRACKED_STATUS: &[u16] = &[$($code),+];
        const STATUS_SLOTS: usize = TRACKED_STATUS.len() + 1;
        const STATUS_LABELS: [&str; STATUS_SLOTS] = [$(stringify!($code)),+, "OTHER"];
    };
}

define_tracked_statuses![
    100, 101, 200, 201, 202, 204, 206, 301, 302, 304, 400, 401, 403, 404, 405, 408, 409, 413, 414,
    415, 429, 500, 502, 503, 504,
];

const _: () = {
    let mut i = 1;
    while i < TRACKED_STATUS.len() {
        assert!(TRACKED_STATUS[i - 1] < TRACKED_STATUS[i]);
        i += 1;
    }
};

fn status_slot(status: u16) -> usize {
    TRACKED_STATUS
        .binary_search(&status)
        .unwrap_or(TRACKED_STATUS.len())
}

/// Per-server metric registry and lazily cached hot-path handles.
///
/// Each [`crate::Bound`] server owns its own concrete [`PrometheusRecorder`] plus a
/// typed [`OtelMetrics`] fanout (zero-sized when `otel` is disabled), isolating `/metrics`
/// output, upkeep, and OTel export per instance with zero `Recorder` virtual dispatch or
/// per-series wrapper allocations. Every finite Prometheus series sits behind a dedicated [`OnceLock`]
/// so steady-state traffic costs a single atomic load.
pub(crate) struct ServerMetrics {
    prometheus: PrometheusRecorder,
    prometheus_handle: PrometheusHandle,
    otel: OtelMetrics,
    active_tcp: OnceLock<Gauge>,
    active_ws: OnceLock<Gauge>,
    saturated: OnceLock<Counter>,
    duration: [OnceLock<Histogram>; UpstreamLayer::COUNT],
    /// `[UpstreamLayer][MethodBucket][status slot]` (`2 × 10 × 26 = 520` slots, ~12.2 KiB);
    /// uncalled slots remain uninitialized and are never registered or exported.
    requests: Box<[[[OnceLock<Counter>; STATUS_SLOTS]; MethodBucket::COUNT]; UpstreamLayer::COUNT]>,
    health: [OnceLock<Gauge>; HealthGauge::COUNT],
}

impl Default for ServerMetrics {
    fn default() -> Self {
        Self::new(OtelMetrics::disabled())
    }
}

impl ServerMetrics {
    pub(crate) fn new(otel: OtelMetrics) -> Self {
        let prometheus = PrometheusBuilder::new().build_recorder();
        let prometheus_handle = prometheus.handle();
        Self {
            prometheus,
            prometheus_handle,
            otel,
            active_tcp: OnceLock::new(),
            active_ws: OnceLock::new(),
            saturated: OnceLock::new(),
            duration: Default::default(),
            requests: Box::default(),
            health: Default::default(),
        }
    }

    pub(crate) const fn otel_enabled(&self) -> bool {
        self.otel.is_enabled()
    }

    pub(crate) fn prometheus_handle(&self) -> PrometheusHandle {
        self.prometheus_handle.clone()
    }

    pub(crate) fn render_prometheus(&self) -> String {
        self.prometheus_handle.render()
    }

    /// The one place a Prometheus series is registered: through this server's concrete
    /// recorder (never the process-global one), on first use, once.
    fn handle<'a, H>(&'a self, slot: &'a OnceLock<H>, register: impl FnOnce() -> H) -> &'a H {
        slot.get_or_init(|| ::metrics::with_local_recorder(&self.prometheus, register))
    }

    fn active(&self, slot: &OnceLock<Gauge>, protocol: &'static str) -> ActiveGauge {
        let prom = self
            .handle(
                slot,
                || ::metrics::gauge!(ACTIVE_CONNECTIONS, "protocol" => protocol),
            )
            .clone();
        ActiveGauge {
            prom,
            otel: self.otel.active_guard(protocol),
        }
    }

    pub(crate) fn active_tcp(&self) -> ActiveConnectionGuard {
        self.active(&self.active_tcp, "tcp").activate()
    }

    pub(crate) fn ws_slot(&self) -> ActiveGauge {
        self.active(&self.active_ws, "ws")
    }

    pub(crate) fn inc_saturated(&self) {
        self.handle(&self.saturated, || {
            ::metrics::counter!(CONNECTION_LIMIT_SATURATED)
        })
        .increment(1);
        self.otel.inc_saturated();
    }

    pub(crate) fn record_request(
        &self,
        layer: UpstreamLayer,
        method: MethodBucket,
        status: u16,
        elapsed: Duration,
    ) {
        let l_idx = layer as usize;
        let secs = elapsed.as_secs_f64();
        let upstream = layer.as_str();
        self.handle(
            &self.duration[l_idx],
            || ::metrics::histogram!(PROXY_REQUEST_DURATION, "upstream" => upstream),
        )
        .record(secs);

        let s_idx = status_slot(status);
        let method_str = method.as_str();
        let status_str = STATUS_LABELS[s_idx];
        self.handle(&self.requests[l_idx][method as usize][s_idx], || {
            ::metrics::counter!(
                PROXY_REQUESTS_TOTAL,
                "upstream" => upstream,
                "method" => method_str,
                "status" => status_str,
            )
        })
        .increment(1);

        self.otel
            .record_request(upstream, method_str, status_str, secs);
    }

    /// Set one health gauge. Its handle is registered on first use, so a gauge that is
    /// never set (a disabled layer, or everything under `--trust-upstream`) never appears.
    pub(crate) fn set_health(&self, gauge: HealthGauge, value: f64) {
        let (instrument, layer) = gauge.spec();
        let name = instrument.name();
        self.handle(&self.health[gauge as usize], || match layer {
            Some(layer) => ::metrics::gauge!(name, "layer" => layer),
            None => ::metrics::gauge!(name),
        })
        .set(value);
        self.otel.set_health(instrument, layer, value);
    }

    /// Update a health gauge only if it was already registered by a previous reading,
    /// avoiding spuriously pinning unsupported gauges (like `-32601` `net_peerCount`) at `0`.
    pub(crate) fn update_registered_health(&self, gauge: HealthGauge, value: f64) {
        if let Some(handle) = self.health[gauge as usize].get() {
            handle.set(value);
            let (instrument, layer) = gauge.spec();
            self.otel.set_health(instrument, layer, value);
        }
    }
}

/// Distinct OTel health gauge instruments shared between Prometheus and OpenTelemetry;
/// the only place their series names appear.
#[repr(u8)]
#[derive(Clone, Copy)]
pub(crate) enum HealthInstrument {
    UpstreamHealth = 0,
    UpstreamPeers,
    UpstreamSyncDistance,
    UpstreamBlockNumber,
    UpstreamSlotNumber,
    ReferenceHealth,
    ReferenceBlockNumber,
    // Must remain last; `Self::COUNT` is derived from this variant.
    ReferenceBlockLag,
}

impl HealthInstrument {
    #[cfg(feature = "otel")]
    pub(crate) const COUNT: usize = Self::ReferenceBlockLag as usize + 1;

    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::UpstreamHealth => "ethonyx_upstream_health_status",
            Self::UpstreamPeers => "ethonyx_upstream_peers",
            Self::UpstreamSyncDistance => "ethonyx_upstream_sync_distance",
            Self::UpstreamBlockNumber => "ethonyx_upstream_block_number",
            Self::UpstreamSlotNumber => "ethonyx_upstream_slot_number",
            Self::ReferenceHealth => "ethonyx_reference_health_status",
            Self::ReferenceBlockNumber => "ethonyx_reference_block_number",
            Self::ReferenceBlockLag => "ethonyx_reference_block_lag",
        }
    }
}

/// Every gauge series the health poller sets (EL/CL share 3 layered instruments).
#[repr(u8)]
#[derive(Clone, Copy)]
pub(crate) enum HealthGauge {
    ElHealth = 0,
    ElPeers,
    ElSyncDistance,
    ElBlockNumber,
    ClHealth,
    ClPeers,
    ClSyncDistance,
    ClSlotNumber,
    ReferenceHealth,
    ReferenceBlockNumber,
    // Must remain last; `Self::COUNT` is derived from this variant.
    ReferenceBlockLag,
}

impl HealthGauge {
    const COUNT: usize = Self::ReferenceBlockLag as usize + 1;

    const fn spec(self) -> (HealthInstrument, Option<&'static str>) {
        match self {
            Self::ElHealth => (HealthInstrument::UpstreamHealth, Some("el")),
            Self::ElPeers => (HealthInstrument::UpstreamPeers, Some("el")),
            Self::ElSyncDistance => (HealthInstrument::UpstreamSyncDistance, Some("el")),
            Self::ElBlockNumber => (HealthInstrument::UpstreamBlockNumber, None),
            Self::ClHealth => (HealthInstrument::UpstreamHealth, Some("cl")),
            Self::ClPeers => (HealthInstrument::UpstreamPeers, Some("cl")),
            Self::ClSyncDistance => (HealthInstrument::UpstreamSyncDistance, Some("cl")),
            Self::ClSlotNumber => (HealthInstrument::UpstreamSlotNumber, None),
            Self::ReferenceHealth => (HealthInstrument::ReferenceHealth, None),
            Self::ReferenceBlockNumber => (HealthInstrument::ReferenceBlockNumber, None),
            Self::ReferenceBlockLag => (HealthInstrument::ReferenceBlockLag, None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::Method;

    #[test]
    fn method_buckets_cover_every_label() {
        let methods = [
            (Method::POST, MethodBucket::Post, "POST"),
            (Method::GET, MethodBucket::Get, "GET"),
            (Method::CONNECT, MethodBucket::Connect, "CONNECT"),
            (Method::HEAD, MethodBucket::Head, "HEAD"),
            (Method::PUT, MethodBucket::Put, "PUT"),
            (Method::DELETE, MethodBucket::Delete, "DELETE"),
            (Method::OPTIONS, MethodBucket::Options, "OPTIONS"),
            (Method::PATCH, MethodBucket::Patch, "PATCH"),
            (Method::TRACE, MethodBucket::Trace, "TRACE"),
            (
                Method::from_bytes(b"PURGE").unwrap(),
                MethodBucket::Other,
                "OTHER",
            ),
        ];
        assert_eq!(methods.len(), MethodBucket::COUNT);
        for (method, expected, label) in &methods {
            let bucket = MethodBucket::classify(method);
            assert_eq!(bucket, *expected);
            assert_eq!(bucket.as_str(), *label);
        }
    }

    #[test]
    fn tracked_statuses_get_their_own_series_and_the_rest_share_other() {
        let tracked = [
            100, 101, 200, 201, 202, 204, 206, 301, 302, 304, 400, 401, 403, 404, 405, 408, 409,
            413, 414, 415, 429, 500, 502, 503, 504,
        ];
        assert_eq!(tracked.len(), TRACKED_STATUS.len());
        let m = ServerMetrics::default();
        for status in tracked.into_iter().chain([307, 418, 599]) {
            m.record_request(
                UpstreamLayer::Cl,
                MethodBucket::Other,
                status,
                Duration::ZERO,
            );
        }

        let rendered = m.render_prometheus();
        let prefix = r#"ethonyx_proxy_requests_total{upstream="CL",method="OTHER",status=""#;
        let mut series: Vec<&str> = rendered
            .lines()
            .filter_map(|line| line.strip_prefix(prefix))
            .collect();
        series.sort_unstable();
        let mut expected: Vec<String> = tracked
            .iter()
            .map(|status| format!("{status}\"}} 1"))
            .collect();
        expected.push("OTHER\"} 3".to_owned());
        assert_eq!(series, expected);
    }

    #[test]
    fn request_series_that_share_two_coordinates_stay_separate() {
        let m = ServerMetrics::default();
        for (layer, method, status) in [
            (UpstreamLayer::El, MethodBucket::Post, 200),
            (UpstreamLayer::Cl, MethodBucket::Post, 200),
            (UpstreamLayer::El, MethodBucket::Get, 200),
            (UpstreamLayer::El, MethodBucket::Post, 404),
            (UpstreamLayer::El, MethodBucket::Post, 418),
        ] {
            m.record_request(layer, method, status, Duration::ZERO);
        }

        let rendered = m.render_prometheus();
        let mut series: Vec<&str> = rendered
            .lines()
            .filter_map(|line| line.strip_prefix("ethonyx_proxy_requests_total"))
            .collect();
        series.sort_unstable();
        assert_eq!(
            series,
            [
                r#"{upstream="CL",method="POST",status="200"} 1"#,
                r#"{upstream="EL",method="GET",status="200"} 1"#,
                r#"{upstream="EL",method="POST",status="200"} 1"#,
                r#"{upstream="EL",method="POST",status="404"} 1"#,
                r#"{upstream="EL",method="POST",status="OTHER"} 1"#,
            ]
        );
    }

    #[test]
    fn server_metrics_instances_are_isolated_across_all_request_buckets() {
        let m1 = ServerMetrics::default();
        let m2 = ServerMetrics::default();

        let _tcp = m1.active_tcp();
        drop(m1.ws_slot().activate());
        m1.inc_saturated();
        m1.record_request(
            UpstreamLayer::El,
            MethodBucket::Post,
            200,
            Duration::from_millis(5),
        );
        m1.record_request(
            UpstreamLayer::Cl,
            MethodBucket::Options,
            414,
            Duration::from_millis(1),
        );
        m1.record_request(
            UpstreamLayer::Cl,
            MethodBucket::Get,
            206,
            Duration::from_millis(2),
        );
        m1.record_request(
            UpstreamLayer::El,
            MethodBucket::Delete,
            429,
            Duration::from_millis(3),
        );
        for (gauge, value) in [
            (HealthGauge::ElBlockNumber, 1234.0),
            (HealthGauge::ReferenceHealth, 1.0),
            (HealthGauge::ReferenceBlockNumber, 1235.0),
            (HealthGauge::ReferenceBlockLag, 1.0),
        ] {
            m1.set_health(gauge, value);
        }

        let r1 = m1.render_prometheus();
        assert!(r1.contains("ethonyx_connection_limit_saturated_total 1"));
        assert!(r1.contains("status=\"200\""));
        assert!(r1.contains("status=\"414\""));
        assert!(r1.contains("status=\"206\""));
        assert!(r1.contains("status=\"429\""));
        assert!(!r1.contains("status=\"500\""));
        assert!(r1.contains("ethonyx_upstream_block_number 1234"));
        assert!(r1.contains("ethonyx_reference_health_status 1"));
        assert!(r1.contains("ethonyx_reference_block_number 1235"));
        assert!(r1.contains("ethonyx_reference_block_lag 1"));

        let r2 = m2.render_prometheus();
        assert!(
            r2.is_empty(),
            "second ServerMetrics instance must not share series from m1, got:\n{r2}"
        );

        // A gauge that is never set is never registered.
        m2.set_health(HealthGauge::ElBlockNumber, 50.0);
        let r2_el_only = m2.render_prometheus();
        assert!(r2_el_only.contains("ethonyx_upstream_block_number 50"));
        assert!(!r2_el_only.contains("layer=\"cl\""));
        assert!(!r2_el_only.contains("ethonyx_upstream_slot_number"));
        assert!(!r2_el_only.contains("ethonyx_upstream_peers"));
        assert!(!r2_el_only.contains("ethonyx_reference_"));
    }

    #[test]
    fn active_connection_gauges_follow_their_guards() {
        let metrics = ServerMetrics::default();
        let active = |protocol: &str| {
            let prefix = format!("{ACTIVE_CONNECTIONS}{{protocol=\"{protocol}\"}} ");
            metrics
                .render_prometheus()
                .lines()
                .find_map(|line| line.strip_prefix(prefix.as_str()).map(str::to_owned))
        };
        let tcp = metrics.active_tcp();
        // Dropping an unarmed WS slot registers the series without counting or underflowing.
        let unarmed_ws = metrics.ws_slot();
        assert_eq!(active("tcp").as_deref(), Some("1"));
        assert_eq!(active("ws").as_deref(), Some("0"));
        drop(unarmed_ws);
        assert_eq!(active("ws").as_deref(), Some("0"));

        let ws = metrics.ws_slot().activate();
        let second_tcp = metrics.active_tcp();
        assert_eq!(active("tcp").as_deref(), Some("2"));
        assert_eq!(active("ws").as_deref(), Some("1"));
        drop((tcp, ws, second_tcp));
        assert_eq!(active("tcp").as_deref(), Some("0"));
        assert_eq!(active("ws").as_deref(), Some("0"));
    }
}
