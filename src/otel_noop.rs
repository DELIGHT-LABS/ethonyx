//! Zero-sized stand-ins for `otel/` when the `otel` feature is disabled,
//! keeping lifecycle, config, metrics, and request paths free of scattered `#[cfg]` gates.

use std::time::Duration;

use crate::metrics::{
    BodyErrorReason, ConnProtocol, HealthInstrument, MethodBucket, StatusSlot, UpstreamLayer,
};

pub(crate) struct OtelMetrics;

const _: () = assert!(std::mem::size_of::<OtelMetrics>() == 0);

impl OtelMetrics {
    pub(crate) const fn disabled() -> Self {
        Self
    }

    pub(crate) const fn add_active(&self, _protocol: ConnProtocol, _delta: i64) {}

    pub(crate) const fn inc_saturated(&self) {}

    pub(crate) const fn record_request(
        &self,
        _layer: UpstreamLayer,
        _method: MethodBucket,
        _status: StatusSlot,
        _elapsed: Duration,
    ) {
    }

    pub(crate) const fn inc_body_error(&self, _layer: UpstreamLayer, _reason: BodyErrorReason) {}

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
        OtelMetrics::disabled()
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

pub(crate) fn start(
    _endpoints: Option<OtelEndpoints>,
    _enable_traces: bool,
) -> Result<Telemetry, crate::BoxError> {
    Ok(Telemetry)
}

pub(crate) const fn attach_parent_context(_span: &tracing::Span, _headers: &http::HeaderMap) {}

pub(crate) const fn propagate_context(_headers: &mut http::HeaderMap) {}

pub(crate) const fn mark_span_error(_span: &tracing::Span) {}
