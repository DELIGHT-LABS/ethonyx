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
        let status_idx = match status {
            101 => Some(0),
            200 => Some(1),
            204 => Some(2),
            400 => Some(3),
            404 => Some(4),
            413 => Some(5),
            502 => Some(6),
            503 => Some(7),
            _ => None,
        };
        if let (Some(m_idx), Some(s_idx)) = (method_idx, status_idx) {
            let status_label = crate::status_to_static_str(status);
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
                "status" => crate::status_to_static_str(status),
            )
            .increment(1);
        }
    }
}
