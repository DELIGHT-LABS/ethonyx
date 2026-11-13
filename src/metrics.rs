use std::sync::OnceLock;
use std::time::Duration;

use ::metrics::{Counter, Gauge, Histogram};

#[cfg(feature = "otel")]
pub(crate) const ACTIVE_CONNECTIONS: &str = "ethonyx_active_connections";

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

/// Lazily cached metric handles for hot data-plane paths so steady-state HTTP
/// and WebSocket traffic performs zero `Vec<Label>` or `Fanout` heap allocations
/// and zero `RwLock` lookups while keeping unused series unregistered on `/metrics`.
#[derive(Default)]
pub(crate) struct ProxyMetrics {
    active_tcp: OnceLock<Gauge>,
    active_ws: OnceLock<Gauge>,
    saturated: OnceLock<Counter>,
    duration: [OnceLock<Histogram>; 2],
    /// `[upstream: EL=0 | CL=1][method: POST=0 | GET=1 | CONNECT=2 | HEAD=3][status: 8 hot codes]`
    requests: [[[OnceLock<Counter>; 8]; 4]; 2],
}

impl ProxyMetrics {
    pub(crate) fn active_tcp(&self) -> ActiveConnectionGuard {
        let gauge = self
            .active_tcp
            .get_or_init(|| ::metrics::gauge!("ethonyx_active_connections", "protocol" => "tcp"))
            .clone();
        ActiveConnectionGuard::new(gauge)
    }

    pub(crate) fn active_ws(&self) -> ActiveConnectionGuard {
        let gauge = self
            .active_ws
            .get_or_init(|| ::metrics::gauge!("ethonyx_active_connections", "protocol" => "ws"))
            .clone();
        ActiveConnectionGuard::new(gauge)
    }

    pub(crate) fn inc_saturated(&self) {
        self.saturated
            .get_or_init(|| ::metrics::counter!("ethonyx_connection_limit_saturated_total"))
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
                ::metrics::histogram!(
                    "ethonyx_proxy_request_duration_seconds",
                    "upstream" => upstream_label,
                )
            })
            .record(elapsed.as_secs_f64());

        let (status_idx, status_label) = classify_status(status);
        if let (Some(m_idx), Some(s_idx)) = (method_idx, status_idx) {
            self.requests[l_idx][m_idx][s_idx]
                .get_or_init(|| {
                    ::metrics::counter!(
                        "ethonyx_proxy_requests_total",
                        "upstream" => upstream_label,
                        "method" => method_label,
                        "status" => status_label,
                    )
                })
                .increment(1);
        } else {
            ::metrics::counter!(
                "ethonyx_proxy_requests_total",
                "upstream" => upstream_label,
                "method" => method_label,
                "status" => status_label,
            )
            .increment(1);
        }
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

pub(crate) const fn classify_status(status: u16) -> (Option<usize>, &'static str) {
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

#[derive(Clone, Copy)]
pub(crate) struct ClSlotSample {
    pub(crate) sync_distance: u64,
    pub(crate) head_slot: u64,
}

#[derive(Clone, Copy)]
pub(crate) struct HealthSample {
    pub(crate) el_ok: bool,
    pub(crate) cl_ok: bool,
    pub(crate) el_peers: f64,
    pub(crate) cl_peers: f64,
    pub(crate) el_distance: f64,
    pub(crate) cl_status: Option<ClSlotSample>,
    pub(crate) el_block: Option<u64>,
}

/// Lazily cached gauge handles for background health polling so OTel `Fanout`
/// never allocates `Vec<Gauge>` / `Arc<FanoutGauge>` or locks registries per poll,
/// while keeping gauges unregistered under `--trust-upstream`.
#[derive(Default)]
pub(crate) struct HealthMetrics {
    el_health: OnceLock<Gauge>,
    cl_health: OnceLock<Gauge>,
    el_peers: OnceLock<Gauge>,
    cl_peers: OnceLock<Gauge>,
    el_distance: OnceLock<Gauge>,
    cl_distance: OnceLock<Gauge>,
    slot_number: OnceLock<Gauge>,
    block_number: OnceLock<Gauge>,
}

impl HealthMetrics {
    pub(crate) fn record(&self, sample: HealthSample) {
        self.el_health
            .get_or_init(|| ::metrics::gauge!("ethonyx_upstream_health_status", "layer" => "el"))
            .set(if sample.el_ok { 1.0 } else { 0.0 });
        self.cl_health
            .get_or_init(|| ::metrics::gauge!("ethonyx_upstream_health_status", "layer" => "cl"))
            .set(if sample.cl_ok { 1.0 } else { 0.0 });

        self.el_peers
            .get_or_init(|| ::metrics::gauge!("ethonyx_upstream_peers", "layer" => "el"))
            .set(sample.el_peers);
        self.cl_peers
            .get_or_init(|| ::metrics::gauge!("ethonyx_upstream_peers", "layer" => "cl"))
            .set(sample.cl_peers);

        self.el_distance
            .get_or_init(|| ::metrics::gauge!("ethonyx_upstream_sync_distance", "layer" => "el"))
            .set(sample.el_distance);

        let cl_dist_gauge = self
            .cl_distance
            .get_or_init(|| ::metrics::gauge!("ethonyx_upstream_sync_distance", "layer" => "cl"));
        if let Some(cl) = sample.cl_status {
            cl_dist_gauge.set(cl.sync_distance as f64);
            self.slot_number
                .get_or_init(|| ::metrics::gauge!("ethonyx_upstream_slot_number"))
                .set(cl.head_slot as f64);
        } else {
            cl_dist_gauge.set(0.0);
        }

        if let Some(block_num) = sample.el_block {
            self.block_number
                .get_or_init(|| ::metrics::gauge!("ethonyx_upstream_block_number"))
                .set(block_num as f64);
        }
    }
}
