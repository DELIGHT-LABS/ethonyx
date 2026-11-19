//! Zero-sized stand-ins for `otel/` when the `otel` feature is disabled,
//! keeping lifecycle, config, metrics, and request paths free of scattered `#[cfg]` gates.

use crate::metrics::{ConnProtocol, HealthInstrument, UpstreamLayer};

pub(crate) struct OtelActiveGuard;

impl OtelActiveGuard {
    pub(crate) const fn add(&self, _delta: i64) {}
}

pub(crate) struct OtelMetrics;

impl OtelMetrics {
    pub(crate) const fn disabled() -> Self {
        Self
    }

    pub(crate) const fn active_guard(&self, _protocol: ConnProtocol) -> OtelActiveGuard {
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
        _instrument: HealthInstrument,
        _layer: Option<UpstreamLayer>,
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
    endpoint: Option<&str>,
) -> Result<Option<OtelEndpoints>, crate::BoxError> {
    if endpoint.is_some() {
        return Err("--otel-endpoint requires building ethonyx with the `otel` feature".into());
    }
    Ok(None)
}

pub(crate) fn start(_endpoints: Option<OtelEndpoints>) -> Result<Telemetry, crate::BoxError> {
    Ok(Telemetry)
}

pub(crate) const fn attach_parent_context(_span: &tracing::Span, _headers: &http::HeaderMap) {}

pub(crate) const fn propagate_context(_headers: &mut http::HeaderMap) {}

pub(crate) const fn mark_span_error(_span: &tracing::Span) {}
