//! Zero-sized stand-ins for `otel.rs` when the `otel` feature is disabled,
//! keeping lifecycle, config, and request paths free of scattered `#[cfg]` gates.

pub(crate) struct Telemetry;

impl Telemetry {
    pub(crate) const fn is_enabled(&self) -> bool {
        false
    }

    pub(crate) const fn metric_sink(&self) -> Option<crate::metrics::DynRecorder> {
        None
    }

    pub(crate) async fn shutdown(self) {}
}

pub(crate) fn parse_config_endpoint(
    _cfg: &crate::Config,
) -> Result<Option<crate::upstream::HttpUpstream>, crate::BoxError> {
    Ok(None)
}

pub(crate) async fn start(
    _base: Option<crate::upstream::HttpUpstream>,
) -> Result<Telemetry, crate::BoxError> {
    Ok(Telemetry)
}

pub(crate) fn attach_parent_context(_span: &tracing::Span, _headers: &http::HeaderMap) {}

pub(crate) fn propagate_context(_headers: &mut http::HeaderMap) {}
