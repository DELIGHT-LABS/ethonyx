//! Zero-sized stand-ins for `otel.rs` when the `otel` feature is disabled,
//! keeping lifecycle, config, metrics, and request paths free of scattered `#[cfg]` gates.

pub(crate) struct OtelActiveGuard;

impl OtelActiveGuard {
    pub(crate) const fn add(&self, _delta: i64) {}
}

pub(crate) struct OtelMetrics;

impl OtelMetrics {
    pub(crate) const fn disabled() -> Self {
        Self
    }

    pub(crate) const fn is_enabled(&self) -> bool {
        false
    }

    pub(crate) const fn active_guard(&self, _protocol: &'static str) -> OtelActiveGuard {
        OtelActiveGuard
    }

    pub(crate) const fn inc_saturated(&self) {}

    pub(crate) const fn record_request(
        &self,
        _upstream: &'static str,
        _method: &'static str,
        _status: &'static str,
        _secs: f64,
    ) {
    }

    pub(crate) const fn set_health(
        &self,
        _instrument: crate::metrics::HealthInstrument,
        _layer: Option<&'static str>,
        _value: f64,
    ) {
    }
}

pub(crate) struct Telemetry;

impl Telemetry {
    pub(crate) const fn metrics(&self) -> OtelMetrics {
        OtelMetrics
    }

    pub(crate) async fn shutdown(self) {}
}

pub(crate) struct OtelEndpoints;

pub(crate) fn parse_config_endpoint(
    _cfg: &crate::Config,
) -> Result<Option<OtelEndpoints>, crate::BoxError> {
    Ok(None)
}

pub(crate) async fn start(_endpoints: Option<OtelEndpoints>) -> Result<Telemetry, crate::BoxError> {
    Ok(Telemetry)
}

pub(crate) fn attach_parent_context(_span: &tracing::Span, _headers: &http::HeaderMap) {}

pub(crate) fn propagate_context(_headers: &mut http::HeaderMap) {}
