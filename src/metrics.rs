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

/// Lazily cached metric handles for hot data-plane paths so steady-state HTTP
/// and WebSocket traffic performs zero `Vec<Label>` or `Fanout` heap allocations
/// and zero `RwLock` lookups while keeping unused series unregistered on `/metrics`.
#[derive(Default)]
pub(crate) struct ProxyMetrics {
    active_tcp: OnceLock<Gauge>,
    active_ws: OnceLock<Gauge>,
    saturated: OnceLock<Counter>,
    el_duration: OnceLock<Histogram>,
    cl_duration: OnceLock<Histogram>,
    /// `[upstream: EL=0 | CL=1][method: POST=0 | GET=1][status: 8 hot codes]`
    requests: [[[OnceLock<Counter>; 8]; 2]; 2],
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
        is_cl: bool,
        method_label: &'static str,
        status: u16,
        elapsed: Duration,
    ) {
        let hist = if is_cl {
            self.cl_duration.get_or_init(|| {
                ::metrics::histogram!("ethonyx_proxy_request_duration_seconds", "upstream" => "CL")
            })
        } else {
            self.el_duration.get_or_init(|| {
                ::metrics::histogram!("ethonyx_proxy_request_duration_seconds", "upstream" => "EL")
            })
        };
        hist.record(elapsed.as_secs_f64());

        let upstream_label = if is_cl { "CL" } else { "EL" };
        let method_idx = match method_label {
            "POST" => Some(0),
            "GET" => Some(1),
            _ => None,
        };
        let (status_idx, status_label) = classify_status(status);
        if let (Some(m_idx), Some(s_idx)) = (method_idx, status_idx) {
            self.requests[usize::from(is_cl)][m_idx][s_idx]
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
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn record(
        &self,
        el_ok: bool,
        cl_ok: bool,
        el_peers: f64,
        cl_peers: f64,
        el_distance: f64,
        cl_status: Option<(u64, u64)>,
        el_block: Option<u64>,
    ) {
        self.el_health
            .get_or_init(|| ::metrics::gauge!("ethonyx_upstream_health_status", "layer" => "el"))
            .set(if el_ok { 1.0 } else { 0.0 });
        self.cl_health
            .get_or_init(|| ::metrics::gauge!("ethonyx_upstream_health_status", "layer" => "cl"))
            .set(if cl_ok { 1.0 } else { 0.0 });

        self.el_peers
            .get_or_init(|| ::metrics::gauge!("ethonyx_upstream_peers", "layer" => "el"))
            .set(el_peers);
        self.cl_peers
            .get_or_init(|| ::metrics::gauge!("ethonyx_upstream_peers", "layer" => "cl"))
            .set(cl_peers);

        self.el_distance
            .get_or_init(|| ::metrics::gauge!("ethonyx_upstream_sync_distance", "layer" => "el"))
            .set(el_distance);

        let cl_dist_gauge = self
            .cl_distance
            .get_or_init(|| ::metrics::gauge!("ethonyx_upstream_sync_distance", "layer" => "cl"));
        if let Some((sync_distance, head_slot)) = cl_status {
            cl_dist_gauge.set(sync_distance as f64);
            self.slot_number
                .get_or_init(|| ::metrics::gauge!("ethonyx_upstream_slot_number"))
                .set(head_slot as f64);
        } else {
            cl_dist_gauge.set(0.0);
        }

        if let Some(block_num) = el_block {
            self.block_number
                .get_or_init(|| ::metrics::gauge!("ethonyx_upstream_block_number"))
                .set(block_num as f64);
        }
    }
}
