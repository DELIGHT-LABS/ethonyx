use std::sync::OnceLock;
use std::time::Duration;

use ::metrics::{Counter, Gauge, Histogram};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle, PrometheusRecorder};

pub(crate) const ACTIVE_CONNECTIONS: &str = "ethonyx_active_connections";

/// A recorder that can live in `Arc<AppState>` and be shared across tasks.
pub(crate) type DynRecorder = Box<dyn ::metrics::Recorder + Send + Sync>;

/// This server's recorder. Handles are registered through it and never through
/// the process-global `metrics` recorder.
struct LocalRecorder(DynRecorder);

impl LocalRecorder {
    fn with<T>(&self, f: impl FnOnce() -> T) -> T {
        ::metrics::with_local_recorder(&*self.0, f)
    }
}

struct PairRecorder {
    primary: PrometheusRecorder,
    secondary: DynRecorder,
}

struct PairCounter(Counter, Counter);

impl ::metrics::CounterFn for PairCounter {
    fn increment(&self, value: u64) {
        self.0.increment(value);
        self.1.increment(value);
    }

    fn absolute(&self, value: u64) {
        self.0.absolute(value);
        self.1.absolute(value);
    }
}

struct PairGauge(Gauge, Gauge);

impl ::metrics::GaugeFn for PairGauge {
    fn increment(&self, value: f64) {
        self.0.increment(value);
        self.1.increment(value);
    }

    fn decrement(&self, value: f64) {
        self.0.decrement(value);
        self.1.decrement(value);
    }

    fn set(&self, value: f64) {
        self.0.set(value);
        self.1.set(value);
    }
}

struct PairHistogram(Histogram, Histogram);

impl ::metrics::HistogramFn for PairHistogram {
    fn record(&self, value: f64) {
        self.0.record(value);
        self.1.record(value);
    }
}

impl ::metrics::Recorder for PairRecorder {
    fn describe_counter(
        &self,
        key: ::metrics::KeyName,
        unit: Option<::metrics::Unit>,
        description: ::metrics::SharedString,
    ) {
        self.primary
            .describe_counter(key.clone(), unit, description.clone());
        self.secondary.describe_counter(key, unit, description);
    }

    fn describe_gauge(
        &self,
        key: ::metrics::KeyName,
        unit: Option<::metrics::Unit>,
        description: ::metrics::SharedString,
    ) {
        self.primary
            .describe_gauge(key.clone(), unit, description.clone());
        self.secondary.describe_gauge(key, unit, description);
    }

    fn describe_histogram(
        &self,
        key: ::metrics::KeyName,
        unit: Option<::metrics::Unit>,
        description: ::metrics::SharedString,
    ) {
        self.primary
            .describe_histogram(key.clone(), unit, description.clone());
        self.secondary.describe_histogram(key, unit, description);
    }

    fn register_counter(
        &self,
        key: &::metrics::Key,
        metadata: &::metrics::Metadata<'_>,
    ) -> Counter {
        Counter::from_arc(std::sync::Arc::new(PairCounter(
            self.primary.register_counter(key, metadata),
            self.secondary.register_counter(key, metadata),
        )))
    }

    fn register_gauge(&self, key: &::metrics::Key, metadata: &::metrics::Metadata<'_>) -> Gauge {
        Gauge::from_arc(std::sync::Arc::new(PairGauge(
            self.primary.register_gauge(key, metadata),
            self.secondary.register_gauge(key, metadata),
        )))
    }

    fn register_histogram(
        &self,
        key: &::metrics::Key,
        metadata: &::metrics::Metadata<'_>,
    ) -> Histogram {
        Histogram::from_arc(std::sync::Arc::new(PairHistogram(
            self.primary.register_histogram(key, metadata),
            self.secondary.register_histogram(key, metadata),
        )))
    }
}

pub(crate) struct ActiveConnectionGuard(Gauge);

impl ActiveConnectionGuard {
    fn new(gauge: Gauge) -> Self {
        gauge.increment(1.0);
        Self(gauge)
    }
}

impl Drop for ActiveConnectionGuard {
    fn drop(&mut self) {
        self.0.decrement(1.0);
    }
}

/// Deferred WebSocket gauge handle captured before `tokio::spawn` so upgraded
/// sessions activate the `ws` gauge only after `on_upgrade` succeeds without
/// retaining `Arc<AppState>` across the relay's lifetime.
pub(crate) struct WsConnectionSlot(Gauge);

impl WsConnectionSlot {
    pub(crate) fn activate(self) -> ActiveConnectionGuard {
        ActiveConnectionGuard::new(self.0)
    }
}

#[repr(usize)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum UpstreamLayer {
    El = 0,
    Cl = 1,
}

impl UpstreamLayer {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::El => "EL",
            Self::Cl => "CL",
        }
    }
}

/// Per-server metric registry and lazily cached hot-path handles.
///
/// Each [`crate::Bound`] server owns its own [`PrometheusRecorder`] (plus an
/// optional OTel fanout layer), isolating `/metrics` output, upkeep, and OTel
/// metric export per instance while keeping steady-state HTTP and WebSocket
/// traffic on single-atomic-load [`OnceLock`] handles without global state.
pub(crate) struct ServerMetrics {
    recorder: LocalRecorder,
    prometheus: PrometheusHandle,
    active_tcp: OnceLock<Gauge>,
    active_ws: OnceLock<Gauge>,
    saturated: OnceLock<Counter>,
    duration: [OnceLock<Histogram>; 2],
    /// `[upstream: EL=0 | CL=1][method: POST=0 | GET=1 | CONNECT=2 | HEAD=3][status: 8 hot codes]`
    requests: [[[OnceLock<Counter>; 8]; 4]; 2],
    health: [OnceLock<Gauge>; HealthGauge::COUNT],
}

impl ServerMetrics {
    /// `secondary`, when present, mirrors every registered series into OTel.
    pub(crate) fn new(secondary: Option<DynRecorder>) -> Self {
        let primary = PrometheusBuilder::new().build_recorder();
        let prometheus = primary.handle();
        let recorder: DynRecorder = match secondary {
            Some(secondary) => Box::new(PairRecorder { primary, secondary }),
            None => Box::new(primary),
        };
        Self {
            recorder: LocalRecorder(recorder),
            prometheus,
            active_tcp: OnceLock::new(),
            active_ws: OnceLock::new(),
            saturated: OnceLock::new(),
            duration: Default::default(),
            requests: Default::default(),
            health: Default::default(),
        }
    }

    pub(crate) fn prometheus_handle(&self) -> PrometheusHandle {
        self.prometheus.clone()
    }

    pub(crate) fn render_prometheus(&self) -> String {
        self.prometheus.render()
    }

    pub(crate) fn active_tcp(&self) -> ActiveConnectionGuard {
        let gauge = self
            .active_tcp
            .get_or_init(|| {
                self.recorder
                    .with(|| ::metrics::gauge!(ACTIVE_CONNECTIONS, "protocol" => "tcp"))
            })
            .clone();
        ActiveConnectionGuard::new(gauge)
    }

    pub(crate) fn ws_slot(&self) -> WsConnectionSlot {
        let gauge = self
            .active_ws
            .get_or_init(|| {
                self.recorder
                    .with(|| ::metrics::gauge!(ACTIVE_CONNECTIONS, "protocol" => "ws"))
            })
            .clone();
        WsConnectionSlot(gauge)
    }

    pub(crate) fn inc_saturated(&self) {
        self.saturated
            .get_or_init(|| {
                self.recorder
                    .with(|| ::metrics::counter!("ethonyx_connection_limit_saturated_total"))
            })
            .increment(1);
    }

    pub(crate) fn record_request(
        &self,
        layer: UpstreamLayer,
        method_idx: Option<usize>,
        method_label: &'static str,
        status: u16,
        elapsed: Duration,
    ) {
        let l_idx = layer as usize;
        let upstream_label = layer.as_str();
        self.duration[l_idx]
            .get_or_init(|| {
                self.recorder.with(|| {
                    ::metrics::histogram!(
                        "ethonyx_proxy_request_duration_seconds",
                        "upstream" => upstream_label,
                    )
                })
            })
            .record(elapsed.as_secs_f64());

        let (status_idx, status_label) = classify_status(status);
        if let (Some(m_idx), Some(s_idx)) = (method_idx, status_idx) {
            self.requests[l_idx][m_idx][s_idx]
                .get_or_init(|| {
                    self.recorder.with(|| {
                        ::metrics::counter!(
                            "ethonyx_proxy_requests_total",
                            "upstream" => upstream_label,
                            "method" => method_label,
                            "status" => status_label,
                        )
                    })
                })
                .increment(1);
        } else {
            self.recorder.with(|| {
                ::metrics::counter!(
                    "ethonyx_proxy_requests_total",
                    "upstream" => upstream_label,
                    "method" => method_label,
                    "status" => status_label,
                )
                .increment(1);
            });
        }
    }

    /// Set one health gauge. Its handle is registered on first use, so a gauge that is
    /// never set (a disabled layer, or everything under `--trust-upstream`) never appears.
    pub(crate) fn set_health(&self, gauge: HealthGauge, value: f64) {
        self.health[gauge as usize]
            .get_or_init(|| self.recorder.with(|| gauge.register()))
            .set(value);
    }
}

pub(crate) fn classify_method(m: &http::Method) -> (Option<usize>, &'static str) {
    match *m {
        http::Method::POST => (Some(0), "POST"),
        http::Method::GET => (Some(1), "GET"),
        http::Method::CONNECT => (Some(2), "CONNECT"),
        http::Method::HEAD => (Some(3), "HEAD"),
        http::Method::PUT => (None, "PUT"),
        http::Method::DELETE => (None, "DELETE"),
        http::Method::OPTIONS => (None, "OPTIONS"),
        http::Method::PATCH => (None, "PATCH"),
        http::Method::TRACE => (None, "TRACE"),
        _ => (None, "OTHER"),
    }
}

const fn classify_status(status: u16) -> (Option<usize>, &'static str) {
    match status {
        100 => (None, "100"),
        101 => (Some(0), "101"),
        200 => (Some(1), "200"),
        201 => (None, "201"),
        202 => (None, "202"),
        204 => (Some(2), "204"),
        206 => (None, "206"),
        301 => (None, "301"),
        302 => (None, "302"),
        304 => (None, "304"),
        400 => (Some(3), "400"),
        401 => (None, "401"),
        403 => (None, "403"),
        404 => (Some(4), "404"),
        405 => (None, "405"),
        408 => (None, "408"),
        409 => (None, "409"),
        413 => (Some(5), "413"),
        414 => (None, "414"),
        415 => (None, "415"),
        429 => (None, "429"),
        500 => (None, "500"),
        502 => (Some(6), "502"),
        503 => (Some(7), "503"),
        504 => (None, "504"),
        _ => (None, "OTHER"),
    }
}

/// Every gauge the health poller sets; the only place their series names appear.
#[repr(usize)]
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

    fn register(self) -> Gauge {
        use ::metrics::gauge;
        match self {
            Self::ElHealth => gauge!("ethonyx_upstream_health_status", "layer" => "el"),
            Self::ElPeers => gauge!("ethonyx_upstream_peers", "layer" => "el"),
            Self::ElSyncDistance => gauge!("ethonyx_upstream_sync_distance", "layer" => "el"),
            Self::ElBlockNumber => gauge!("ethonyx_upstream_block_number"),
            Self::ClHealth => gauge!("ethonyx_upstream_health_status", "layer" => "cl"),
            Self::ClPeers => gauge!("ethonyx_upstream_peers", "layer" => "cl"),
            Self::ClSyncDistance => gauge!("ethonyx_upstream_sync_distance", "layer" => "cl"),
            Self::ClSlotNumber => gauge!("ethonyx_upstream_slot_number"),
            Self::ReferenceHealth => gauge!("ethonyx_reference_health_status"),
            Self::ReferenceBlockNumber => gauge!("ethonyx_reference_block_number"),
            Self::ReferenceBlockLag => gauge!("ethonyx_reference_block_lag"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::Method;

    #[test]
    fn test_classify_method() {
        assert_eq!(classify_method(&Method::POST), (Some(0), "POST"));
        assert_eq!(classify_method(&Method::GET), (Some(1), "GET"));
        assert_eq!(classify_method(&Method::CONNECT), (Some(2), "CONNECT"));
        assert_eq!(classify_method(&Method::HEAD), (Some(3), "HEAD"));
        assert_eq!(
            classify_method(&Method::from_bytes(b"OPTIONS").unwrap()),
            (None, "OPTIONS")
        );
        assert_eq!(
            classify_method(&Method::from_bytes(b"PURGE").unwrap()),
            (None, "OTHER")
        );
    }

    #[test]
    fn test_classify_status() {
        assert_eq!(classify_status(101), (Some(0), "101"));
        assert_eq!(classify_status(200), (Some(1), "200"));
        assert_eq!(classify_status(204), (Some(2), "204"));
        assert_eq!(classify_status(400), (Some(3), "400"));
        assert_eq!(classify_status(404), (Some(4), "404"));
        assert_eq!(classify_status(408), (None, "408"));
        assert_eq!(classify_status(413), (Some(5), "413"));
        assert_eq!(classify_status(414), (None, "414"));
        assert_eq!(classify_status(502), (Some(6), "502"));
        assert_eq!(classify_status(503), (Some(7), "503"));
        assert_eq!(classify_status(418), (None, "OTHER"));
        assert_eq!(classify_status(307), (None, "OTHER"));
    }

    #[test]
    fn server_metrics_instances_are_isolated_across_hot_and_cold_series() {
        let m1 = ServerMetrics::new(None);
        let m2 = ServerMetrics::new(None);

        let _tcp = m1.active_tcp();
        drop(m1.ws_slot().activate());
        m1.inc_saturated();
        m1.record_request(
            UpstreamLayer::El,
            Some(0),
            "POST",
            200,
            Duration::from_millis(5),
        );
        m1.record_request(
            UpstreamLayer::Cl,
            None,
            "OPTIONS",
            414,
            Duration::from_millis(1),
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
}
