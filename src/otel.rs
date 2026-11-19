use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::Resource;

const SERVICE_NAME: &str = env!("CARGO_PKG_NAME");
const TRACEPARENT: http::HeaderName = http::HeaderName::from_static("traceparent");
const TRACESTATE: http::HeaderName = http::HeaderName::from_static("tracestate");

struct HeaderMapCarrier<'a>(&'a mut http::HeaderMap);

impl<'a> opentelemetry::propagation::Injector for HeaderMapCarrier<'a> {
    fn set(&mut self, key: &str, value: String) {
        let name = if key.eq_ignore_ascii_case("traceparent") {
            TRACEPARENT
        } else if key.eq_ignore_ascii_case("tracestate") {
            TRACESTATE
        } else if let Ok(custom) = http::HeaderName::from_bytes(key.as_bytes()) {
            custom
        } else {
            return;
        };
        if value.is_empty() {
            // W3C Trace Context §3.3.1.4: omit empty `tracestate` headers (and strip any
            // orphaned incoming `tracestate` when a fresh root trace starts).
            self.0.remove(name);
            return;
        }
        if let Ok(val) = http::HeaderValue::try_from(value) {
            self.0.insert(name, val);
        }
    }
}

struct HeaderMapCarrierRef<'a>(&'a http::HeaderMap);

impl<'a> opentelemetry::propagation::Extractor for HeaderMapCarrierRef<'a> {
    fn get(&self, key: &str) -> Option<&str> {
        let val = self.0.get(key)?.to_str().ok()?;
        (!crate::headers::header_has_token(self.0, http::header::CONNECTION, key)).then_some(val)
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(|k| k.as_str()).collect()
    }
}

/// Link `span` (constructed with `parent: None` so ambient thread-local tracing/OTel
/// contexts are never inherited) to any non-`Connection`-nominated parent trace
/// context in `headers`.
///
/// Must be called BEFORE `span` is first entered so `tracing-opentelemetry` builds
/// the underlying OTel `Span` with the extracted parent `TraceId`.
pub(crate) fn attach_parent_context(span: &tracing::Span, headers: &http::HeaderMap) {
    if span.is_disabled() || !headers.contains_key(TRACEPARENT) {
        return;
    }
    use opentelemetry::propagation::TextMapPropagator as _;
    let parent_cx = opentelemetry_sdk::propagation::TraceContextPropagator::new()
        .extract_with_context(
            &opentelemetry::Context::new(),
            &HeaderMapCarrierRef(headers),
        );
    if opentelemetry::trace::TraceContextExt::has_active_span(&parent_cx) {
        let _ = tracing_opentelemetry::OpenTelemetrySpanExt::set_parent(span, parent_cx);
    }
}

pub(crate) fn propagate_context(headers: &mut http::HeaderMap) {
    let span = tracing::Span::current();
    if span.is_disabled() {
        // When no tracing subscriber/span is active, any non-Connection-nominated
        // incoming `traceparent` was already preserved intact by `strip_hop_by_hop`.
        return;
    }
    use opentelemetry::propagation::TextMapPropagator as _;
    let cx = tracing_opentelemetry::OpenTelemetrySpanExt::context(&span);
    opentelemetry_sdk::propagation::TraceContextPropagator::new()
        .inject_context(&cx, &mut HeaderMapCarrier(headers));
}

struct TelemetryProviders {
    traces: opentelemetry_sdk::trace::SdkTracerProvider,
    metrics: opentelemetry_sdk::metrics::SdkMeterProvider,
}

impl TelemetryProviders {
    fn shutdown_blocking(self) {
        let _ = self.metrics.shutdown();
        let _ = self.traces.shutdown();
    }
}

pub(crate) struct Telemetry(Option<TelemetryProviders>);

impl Telemetry {
    pub(crate) fn tracer(&self) -> Option<opentelemetry_sdk::trace::SdkTracer> {
        use opentelemetry::trace::TracerProvider as _;
        self.0.as_ref().map(|p| p.traces.tracer(SERVICE_NAME))
    }

    /// Typed OpenTelemetry metric instruments mirroring this server's metrics.
    pub(crate) fn metrics(&self) -> OtelMetrics {
        use opentelemetry::metrics::MeterProvider as _;
        match &self.0 {
            Some(providers) => OtelMetrics::from_meter(providers.metrics.meter(SERVICE_NAME)),
            None => OtelMetrics::disabled(),
        }
    }

    pub(crate) async fn shutdown(mut self) {
        if let Some(providers) = self.0.take() {
            // Blocking readers/exporters must be joined outside the async worker.
            let _ = tokio::task::spawn_blocking(move || providers.shutdown_blocking()).await;
        }
    }
}

impl Drop for Telemetry {
    fn drop(&mut self) {
        if let Some(providers) = self.0.take() {
            // Installed subscriber layers (`SdkTracer`) and `OtelMetrics` (`Meter`)
            // retain strong clones of the underlying pipelines, so abnormal drops
            // without `shutdown().await` offload explicit shutdown to a detached
            // thread without blocking async workers or coupling to a dying runtime.
            let _ = std::thread::Builder::new().spawn(move || providers.shutdown_blocking());
        }
    }
}

/// Pre-validated OTLP HTTP/S signal URLs constructed during `Config::parse_validated()`.
pub(crate) struct OtelEndpoints {
    traces: String,
    metrics: String,
}

impl OtelEndpoints {
    fn parse(endpoint: &str) -> Result<Self, crate::BoxError> {
        let base = crate::upstream::HttpUpstream::parse("--otel-endpoint", endpoint)?;
        if base.uri().query().is_some() {
            return Err(format!(
                "invalid --otel-endpoint ({}): OTLP endpoint must be an HTTP(S) base URL without a query or fragment",
                base.redacted()
            )
            .into());
        }
        let signal = |path: &'static str| -> Result<String, crate::BoxError> {
            base.rewrite_uri(http::Uri::from_static(path))
                .map(|uri| uri.to_string())
                .map_err(|e| format!("invalid --otel-endpoint ({}): {e}", base.redacted()).into())
        };
        Ok(Self {
            traces: signal("/v1/traces")?,
            metrics: signal("/v1/metrics")?,
        })
    }
}

/// Start the OTLP trace and metric providers for validated endpoints.
pub(crate) async fn start(endpoints: Option<OtelEndpoints>) -> Result<Telemetry, crate::BoxError> {
    let Some(endpoints) = endpoints else {
        return Ok(Telemetry(None));
    };
    tokio::task::spawn_blocking(move || init_otel(endpoints)).await?
}

pub(crate) fn parse_config_endpoint(
    cfg: &crate::Config,
) -> Result<Option<OtelEndpoints>, crate::BoxError> {
    cfg.otel_endpoint
        .as_deref()
        .map(OtelEndpoints::parse)
        .transpose()
}

fn build_resource() -> Resource {
    build_resource_with(
        &[
            Box::new(opentelemetry_sdk::resource::TelemetryResourceDetector),
            Box::new(opentelemetry_sdk::resource::EnvResourceDetector::new()),
        ],
        std::env::var("OTEL_SERVICE_NAME").ok(),
    )
}

fn build_resource_with(
    detectors: &[Box<dyn opentelemetry_sdk::resource::ResourceDetector>],
    otel_service_name: Option<String>,
) -> Resource {
    // Seed `SERVICE_NAME` as the fallback default first so `OTEL_RESOURCE_ATTRIBUTES`
    // (`service.name=...`) and non-empty `OTEL_SERVICE_NAME` override it per OTel spec.
    let mut builder = Resource::builder_empty()
        .with_service_name(SERVICE_NAME)
        .with_detectors(detectors);
    if let Some(name) = otel_service_name.filter(|s| !s.is_empty()) {
        builder = builder.with_service_name(name);
    }
    builder.build()
}

fn init_otel(endpoints: OtelEndpoints) -> Result<Telemetry, crate::BoxError> {
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_endpoint(endpoints.traces)
        .build()?;
    let meter_exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_http()
        .with_endpoint(endpoints.metrics)
        .build()?;

    let resource = build_resource();
    let tracer_provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(resource.clone())
        .build();

    let reader = opentelemetry_sdk::metrics::PeriodicReader::builder(meter_exporter).build();
    let meter_provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
        .with_resource(resource)
        .with_reader(reader)
        .build();

    Ok(Telemetry(Some(TelemetryProviders {
        traces: tracer_provider,
        metrics: meter_provider,
    })))
}

// ---- Typed OpenTelemetry metric instruments ----

use crate::metrics::{
    ACTIVE_CONNECTIONS, CONNECTION_LIMIT_SATURATED, HealthInstrument, PROXY_REQUEST_DURATION,
    PROXY_REQUESTS_TOTAL,
};
use opentelemetry::KeyValue;
use std::sync::OnceLock;

/// Detached active-connection up-down counter handle carried inside
/// `ActiveGauge` / `ActiveConnectionGuard` without retaining `Arc<AppState>`.
pub(crate) struct OtelActiveGuard(
    Option<(opentelemetry::metrics::UpDownCounter<i64>, &'static str)>,
);

impl OtelActiveGuard {
    pub(crate) fn add(&self, delta: i64) {
        if let Some((counter, protocol)) = &self.0 {
            counter.add(delta, &[KeyValue::new("protocol", *protocol)]);
        }
    }
}

struct OtelMetricsInner {
    meter: opentelemetry::metrics::Meter,
    active_connections: opentelemetry::metrics::UpDownCounter<i64>,
    saturated: OnceLock<opentelemetry::metrics::Counter<u64>>,
    duration: opentelemetry::metrics::Histogram<f64>,
    requests: opentelemetry::metrics::Counter<u64>,
    health: [OnceLock<opentelemetry::metrics::Gauge<f64>>; HealthInstrument::COUNT],
}

/// Typed OpenTelemetry metric fanout owned directly by [`crate::metrics::ServerMetrics`].
///
/// Builds one OTel instrument per metric name (zero per-series wrapper allocations or
/// `Recorder` virtual dispatch), lazily creating saturation and health instruments on first use.
pub(crate) struct OtelMetrics(Option<Box<OtelMetricsInner>>);

impl OtelMetrics {
    pub(crate) const fn disabled() -> Self {
        Self(None)
    }

    fn from_meter(meter: opentelemetry::metrics::Meter) -> Self {
        let active_connections = meter.i64_up_down_counter(ACTIVE_CONNECTIONS).build();
        let duration = meter.f64_histogram(PROXY_REQUEST_DURATION).build();
        let requests = meter.u64_counter(PROXY_REQUESTS_TOTAL).build();
        Self(Some(Box::new(OtelMetricsInner {
            meter,
            active_connections,
            saturated: OnceLock::new(),
            duration,
            requests,
            health: Default::default(),
        })))
    }

    pub(crate) const fn is_enabled(&self) -> bool {
        self.0.is_some()
    }

    pub(crate) fn active_guard(&self, protocol: &'static str) -> OtelActiveGuard {
        OtelActiveGuard(
            self.0
                .as_ref()
                .map(|m| (m.active_connections.clone(), protocol)),
        )
    }

    pub(crate) fn inc_saturated(&self) {
        if let Some(m) = &self.0 {
            m.saturated
                .get_or_init(|| m.meter.u64_counter(CONNECTION_LIMIT_SATURATED).build())
                .add(1, &[]);
        }
    }

    pub(crate) fn record_request(
        &self,
        upstream: &'static str,
        method: &'static str,
        status: &'static str,
        secs: f64,
    ) {
        if let Some(m) = &self.0 {
            m.duration
                .record(secs, &[KeyValue::new("upstream", upstream)]);
            // Keep keys sorted ascending so `opentelemetry_sdk`'s `ValueMap` retains one
            // `Vec<KeyValue>` entry per series instead of both unsorted and sorted copies.
            m.requests.add(
                1,
                &[
                    KeyValue::new("method", method),
                    KeyValue::new("status", status),
                    KeyValue::new("upstream", upstream),
                ],
            );
        }
    }

    pub(crate) fn set_health(
        &self,
        instrument: HealthInstrument,
        layer: Option<&'static str>,
        value: f64,
    ) {
        if let Some(m) = &self.0 {
            let gauge = m.health[instrument as usize]
                .get_or_init(|| m.meter.f64_gauge(instrument.name()).build());
            match layer {
                Some(layer) => gauge.record(value, &[KeyValue::new("layer", layer)]),
                None => gauge.record(value, &[]),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        OtelEndpoints, OtelMetrics, Resource, SERVICE_NAME, Telemetry, TelemetryProviders,
        attach_parent_context, build_resource_with, propagate_context,
    };
    use opentelemetry::metrics::MeterProvider;
    use opentelemetry::{Key, KeyValue, Value};
    use opentelemetry_sdk::error::OTelSdkResult;
    use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData, ResourceMetrics};
    use opentelemetry_sdk::metrics::exporter::PushMetricExporter;
    use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider, Temporality};
    use opentelemetry_sdk::resource::ResourceDetector;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    struct StaticAttrDetector(&'static str);

    impl ResourceDetector for StaticAttrDetector {
        fn detect(&self) -> Resource {
            Resource::builder_empty()
                .with_attribute(KeyValue::new("service.name", self.0))
                .build()
        }
    }

    type ExportedMetric = (String, &'static str, Vec<(Vec<KeyValue>, f64)>);

    #[derive(Clone, Default)]
    struct SnapshotExporter(Arc<Mutex<Vec<ExportedMetric>>>);

    impl PushMetricExporter for SnapshotExporter {
        async fn export(&self, metrics: &ResourceMetrics) -> OTelSdkResult {
            let mut out = self.0.lock().unwrap();
            for scope_metrics in metrics.scope_metrics() {
                assert_eq!(scope_metrics.scope().name(), "ethonyx");
                let mut seen_names = std::collections::HashSet::new();
                for metric in scope_metrics.metrics() {
                    assert!(
                        seen_names.insert(metric.name().to_owned()),
                        "duplicate exported metric stream: {}",
                        metric.name()
                    );
                    let (kind, points) = match metric.data() {
                        AggregatedMetrics::U64(MetricData::Sum(sum)) => {
                            let kind = if sum.is_monotonic() {
                                "u64_monotonic_sum"
                            } else {
                                "u64_up_down_sum"
                            };
                            let pts = sum
                                .data_points()
                                .map(|p| (p.attributes().cloned().collect(), p.value() as f64))
                                .collect();
                            (kind, pts)
                        }
                        AggregatedMetrics::I64(MetricData::Sum(sum)) => {
                            let kind = if sum.is_monotonic() {
                                "i64_monotonic_sum"
                            } else {
                                "i64_up_down_sum"
                            };
                            let pts = sum
                                .data_points()
                                .map(|p| (p.attributes().cloned().collect(), p.value() as f64))
                                .collect();
                            (kind, pts)
                        }
                        AggregatedMetrics::F64(MetricData::Gauge(gauge)) => {
                            let pts = gauge
                                .data_points()
                                .map(|p| (p.attributes().cloned().collect(), p.value()))
                                .collect();
                            ("gauge", pts)
                        }
                        AggregatedMetrics::F64(MetricData::Histogram(hist)) => {
                            let pts = hist
                                .data_points()
                                .map(|p| (p.attributes().cloned().collect(), p.count() as f64))
                                .collect();
                            ("histogram", pts)
                        }
                        other => panic!("unexpected metric aggregation: {other:?}"),
                    };
                    out.push((metric.name().to_owned(), kind, points));
                }
            }
            Ok(())
        }

        fn force_flush(&self) -> OTelSdkResult {
            Ok(())
        }

        fn shutdown_with_timeout(&self, _timeout: Duration) -> OTelSdkResult {
            Ok(())
        }

        fn temporality(&self) -> Temporality {
            Temporality::Cumulative
        }
    }

    #[test]
    fn otel_metrics_exports_active_connections_as_up_down_sum_and_other_gauges_as_gauge() {
        assert_eq!(SERVICE_NAME, "ethonyx");
        let exporter = SnapshotExporter::default();
        let provider = SdkMeterProvider::builder()
            .with_reader(PeriodicReader::builder(exporter.clone()).build())
            .build();
        let otel_metrics = OtelMetrics::from_meter(provider.meter(SERVICE_NAME));
        let server_metrics = crate::metrics::ServerMetrics::new(otel_metrics);

        let tcp_guard = server_metrics.active_tcp();
        // Open and immediately drop a WS connection to verify symmetrical +1 / -1.
        drop(server_metrics.ws_slot().activate());
        server_metrics.inc_saturated();
        // Exercise both common and edge method/status buckets twice each so OnceLock
        // caching and OTel SDK stream deduplication are verified end-to-end.
        for _ in 0..2 {
            server_metrics.record_request(
                crate::metrics::UpstreamLayer::El,
                crate::metrics::MethodBucket::Post,
                200,
                Duration::from_millis(5),
            );
            server_metrics.record_request(
                crate::metrics::UpstreamLayer::Cl,
                crate::metrics::MethodBucket::Options,
                206,
                Duration::from_millis(3),
            );
            server_metrics.record_request(
                crate::metrics::UpstreamLayer::El,
                crate::metrics::MethodBucket::Other,
                429,
                Duration::from_millis(2),
            );
        }
        {
            use crate::metrics::HealthGauge as G;
            // Register CL peers first and call `update_registered_health` on unregistered
            // EL peers: OTel must NOT emit a synthetic `layer="el"` zero datapoint.
            server_metrics.set_health(G::ClPeers, 12.0);
            server_metrics.update_registered_health(G::ElPeers, 0.0);
            provider.force_flush().unwrap();

            for (gauge, value) in [
                (G::ElHealth, 1.0),
                (G::ElPeers, 8.0),
                (G::ElSyncDistance, 0.0),
                (G::ElBlockNumber, 100.0),
                (G::ClHealth, 1.0),
                (G::ClSyncDistance, 0.0),
                (G::ClSlotNumber, 42.0),
                (G::ReferenceHealth, 1.0),
                (G::ReferenceBlockNumber, 100.0),
                (G::ReferenceBlockLag, 0.0),
            ] {
                server_metrics.set_health(gauge, value);
            }
            provider.force_flush().unwrap();

            // Once EL peers is registered, `update_registered_health` fans out `0.0` to OTel.
            server_metrics.update_registered_health(G::ElPeers, 0.0);
        }

        // Dropping `tcp_guard` exercises the retained `OtelActiveGuard` handle.
        drop(tcp_guard);
        provider.force_flush().unwrap();
        provider.shutdown().unwrap();

        let snapshots = exporter.0.lock().unwrap();
        let first_active = snapshots
            .iter()
            .find(|(name, _, _)| name == "ethonyx_active_connections")
            .expect("active_connections exported on first flush");
        assert_eq!(first_active.1, "i64_up_down_sum");
        assert!(
            first_active
                .2
                .contains(&(vec![KeyValue::new("protocol", "tcp")], 1.0))
        );
        assert!(
            first_active
                .2
                .contains(&(vec![KeyValue::new("protocol", "ws")], 0.0))
        );

        let final_active = snapshots
            .iter()
            .rfind(|(name, _, _)| name == "ethonyx_active_connections")
            .expect("active_connections exported after tcp_guard drop");
        assert!(
            final_active
                .2
                .contains(&(vec![KeyValue::new("protocol", "tcp")], 0.0))
        );

        let peer_flushes: Vec<_> = snapshots
            .iter()
            .filter(|(name, _, _)| name == "ethonyx_upstream_peers")
            .collect();
        assert!(
            peer_flushes.len() >= 3,
            "expected at least 3 peer flush snapshots, got {}",
            peer_flushes.len()
        );
        assert_eq!(peer_flushes[0].1, "gauge");
        assert!(
            peer_flushes[0]
                .2
                .contains(&(vec![KeyValue::new("layer", "cl")], 12.0))
        );
        assert!(
            !peer_flushes[0]
                .2
                .iter()
                .any(|(attrs, _)| attrs.contains(&KeyValue::new("layer", "el"))),
            "unregistered EL peers must stay omitted on OTel: {:?}",
            peer_flushes[0].2
        );
        assert!(
            peer_flushes[1]
                .2
                .contains(&(vec![KeyValue::new("layer", "el")], 8.0))
        );
        assert!(
            peer_flushes
                .last()
                .unwrap()
                .2
                .contains(&(vec![KeyValue::new("layer", "el")], 0.0))
        );

        for (metric_name, expected_val) in [
            ("ethonyx_reference_health_status", 1.0),
            ("ethonyx_reference_block_number", 100.0),
            ("ethonyx_reference_block_lag", 0.0),
        ] {
            let exported = snapshots
                .iter()
                .find(|(name, _, _)| name == metric_name)
                .unwrap_or_else(|| panic!("{metric_name} exported"));
            assert_eq!(exported.1, "gauge");
            assert!(exported.2.contains(&(Vec::new(), expected_val)));
        }

        let requests = snapshots
            .iter()
            .find(|(name, _, _)| name == "ethonyx_proxy_requests_total")
            .expect("proxy_requests_total exported");
        assert_eq!(requests.1, "u64_monotonic_sum");
        for (upstream, method, status) in [
            ("EL", "POST", "200"),
            ("CL", "OPTIONS", "206"),
            ("EL", "OTHER", "429"),
        ] {
            let expected_attrs = vec![
                KeyValue::new("method", method),
                KeyValue::new("status", status),
                KeyValue::new("upstream", upstream),
            ];
            assert!(
                requests.2.contains(&(expected_attrs, 2.0)),
                "expected sorted ({method}, {status}, {upstream}) == 2 in {:?}",
                requests.2
            );
        }

        let duration = snapshots
            .iter()
            .find(|(name, _, _)| name == "ethonyx_proxy_request_duration_seconds")
            .expect("proxy_request_duration_seconds exported");
        assert_eq!(duration.1, "histogram");
        assert!(
            duration
                .2
                .contains(&(vec![KeyValue::new("upstream", "EL")], 4.0))
        );
        assert!(
            duration
                .2
                .contains(&(vec![KeyValue::new("upstream", "CL")], 2.0))
        );

        let saturated = snapshots
            .iter()
            .find(|(name, _, _)| name == "ethonyx_connection_limit_saturated_total")
            .expect("connection_limit_saturated_total exported");
        assert_eq!(saturated.1, "u64_monotonic_sum");
        assert_eq!(saturated.2, [(Vec::new(), 1.0)]);

        for (metric_name, el, cl) in [
            ("ethonyx_upstream_health_status", 1.0, 1.0),
            ("ethonyx_upstream_sync_distance", 0.0, 0.0),
        ] {
            let exported = snapshots
                .iter()
                .find(|(name, _, _)| name == metric_name)
                .unwrap_or_else(|| panic!("{metric_name} exported"));
            assert_eq!(exported.1, "gauge");
            assert!(
                exported
                    .2
                    .contains(&(vec![KeyValue::new("layer", "el")], el))
            );
            assert!(
                exported
                    .2
                    .contains(&(vec![KeyValue::new("layer", "cl")], cl))
            );
        }
        for (metric_name, expected_val) in [
            ("ethonyx_upstream_block_number", 100.0),
            ("ethonyx_upstream_slot_number", 42.0),
        ] {
            let exported = snapshots
                .iter()
                .find(|(name, _, _)| name == metric_name)
                .unwrap_or_else(|| panic!("{metric_name} exported"));
            assert_eq!(exported.1, "gauge");
            assert!(exported.2.contains(&(Vec::new(), expected_val)));
        }

        // `ServerMetrics` mirrors the same series to the primary Prometheus handle.
        let prom = server_metrics.render_prometheus();
        assert!(prom.contains("ethonyx_connection_limit_saturated_total 1"));
        assert!(prom.contains(
            r#"ethonyx_proxy_requests_total{upstream="CL",method="OPTIONS",status="206"} 2"#
        ));
    }

    #[test]
    fn resource_honors_otel_service_name_and_resource_attributes_precedence() {
        let key = Key::from_static_str("service.name");

        // 1. Default falls back to SERVICE_NAME ("ethonyx") when unset or empty.
        let default_res = build_resource_with(&[], None);
        assert_eq!(default_res.get(&key), Some(Value::from("ethonyx")));
        let empty_env_res = build_resource_with(&[], Some(String::new()));
        assert_eq!(empty_env_res.get(&key), Some(Value::from("ethonyx")));

        // 2. OTEL_RESOURCE_ATTRIBUTES (`service.name=...`) overrides the default.
        let detectors: [Box<dyn ResourceDetector>; 1] =
            [Box::new(StaticAttrDetector("ethonyx-from-resource-attrs"))];
        let attr_res = build_resource_with(&detectors, None);
        assert_eq!(
            attr_res.get(&key),
            Some(Value::from("ethonyx-from-resource-attrs"))
        );

        // 3. Explicit OTEL_SERVICE_NAME takes highest precedence over both.
        let env_res = build_resource_with(&detectors, Some("ethonyx-mainnet-01".to_owned()));
        assert_eq!(env_res.get(&key), Some(Value::from("ethonyx-mainnet-01")));
    }

    #[test]
    fn otlp_signal_paths_preserve_the_base_path() {
        for base in [
            "https://collector:4318/prefix",
            "https://collector:4318/prefix/",
            "HTTP://collector:4318",
        ] {
            let expected_scheme = if base.starts_with("HTTP:") {
                "http://collector:4318"
            } else {
                "https://collector:4318/prefix"
            };
            let endpoints = OtelEndpoints::parse(base).unwrap();
            assert_eq!(endpoints.traces, format!("{expected_scheme}/v1/traces"));
            assert_eq!(endpoints.metrics, format!("{expected_scheme}/v1/metrics"));
        }
        let oversized = format!("http://collector:4318/SECRET/{}", "a".repeat(65_530));
        // Fits `http::Uri` (65,534 bytes) by itself; appending `/v1/traces` overflows it.
        let overflows_when_joined = format!("http://c/SECRET/{}", "a".repeat(65_518));
        for invalid in [
            "collector:4318",
            "ftp://collector",
            "http://user:SECRET@collector:4318",
            "http://:4318",
            "http://collector:0",
            "http://collector:+4318",
            "http://collector:004318",
            "http://collector:99999",
            "http://collector:",
            "http://collector?x=SECRET",
            "http://collector/#SECRET",
            oversized.as_str(),
            overflows_when_joined.as_str(),
        ] {
            let err = OtelEndpoints::parse(invalid)
                .err()
                .expect(invalid)
                .to_string();
            assert!(err.contains("--otel-endpoint"), "missing flag in {err}");
            assert!(!err.contains("SECRET"), "leaked secret in {err}");
        }
    }

    #[derive(Clone, Debug, Default)]
    struct CapturingSpanExporter(Arc<Mutex<Vec<String>>>);

    impl opentelemetry_sdk::trace::SpanExporter for CapturingSpanExporter {
        async fn export(&self, batch: Vec<opentelemetry_sdk::trace::SpanData>) -> OTelSdkResult {
            let mut out = self.0.lock().unwrap();
            out.extend(batch.into_iter().map(|s| s.name.into_owned()));
            Ok(())
        }
    }

    #[tokio::test]
    async fn pooled_client_exports_first_request_span_while_keepalive_socket_stays_open() {
        use bytes::Bytes;
        use http::{Method, Request, Response, StatusCode};
        use http_body_util::{BodyExt, Full};
        use hyper::body::Incoming;
        use hyper::service::service_fn;
        use hyper_util::rt::{TokioExecutor, TokioIo};
        use hyper_util::server::conn::auto;
        use opentelemetry::trace::TracerProvider as _;
        use tracing::Instrument as _;
        use tracing::instrument::WithSubscriber as _;
        use tracing_subscriber::prelude::*;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                tokio::spawn(async move {
                    let svc = service_fn(|req: Request<Incoming>| async move {
                        let _ = req.into_body().collect().await;
                        Ok::<_, std::convert::Infallible>(
                            Response::builder()
                                .status(StatusCode::OK)
                                .body(Full::new(Bytes::from_static(b"ok")))
                                .unwrap(),
                        )
                    });
                    let _ = auto::Builder::new(TokioExecutor::new())
                        .serve_connection(TokioIo::new(stream), svc)
                        .await;
                });
            }
        });

        let span_exporter = CapturingSpanExporter::default();
        let tracer_provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
            .with_simple_exporter(span_exporter.clone())
            .build();
        let subscriber = tracing_subscriber::registry().with(
            tracing_opentelemetry::layer()
                .with_tracer(tracer_provider.tracer(SERVICE_NAME))
                .with_filter(tracing_subscriber::EnvFilter::new(
                    "ethonyx=info,access_log=off",
                )),
        );

        let (https, _) = crate::transport::build_connectors(false);
        let h1_client = crate::transport::build_client(https.clone());
        let h2c_client = crate::transport::build_h2c_client(https);
        let uri: http::Uri = format!("http://{addr}/").parse().unwrap();

        async {
            for (name, client) in [("request_h1", &h1_client), ("request_h2c", &h2c_client)] {
                let req = Request::builder()
                    .method(Method::POST)
                    .uri(uri.clone())
                    .body(crate::response::full_body(Bytes::from_static(b"{}")))
                    .unwrap();
                async {
                    let resp = client.request(req).await.unwrap();
                    assert_eq!(resp.status(), StatusCode::OK);
                    let _ = resp.into_body().collect().await.unwrap();
                }
                .instrument(tracing::info_span!("request", kind = name))
                .await;
            }
        }
        .with_subscriber(subscriber)
        .await;

        // Both pooled keep-alive connections (HTTP/1.1 and HTTP/2) are STILL open inside
        // `h1_client` and `h2c_client`. Without `rt-tracing-exec-force`, background pool
        // driver tasks must NOT retain a clone of Request #1's span!
        {
            let exported = span_exporter.0.lock().unwrap();
            assert_eq!(
                exported.as_slice(),
                &["request", "request"],
                "first request spans must export immediately while pooled connections remain open"
            );
        }

        drop((h1_client, h2c_client));
        server.abort();
        let _ = tracer_provider.shutdown();
    }

    #[tokio::test]
    async fn dropping_telemetry_without_explicit_shutdown_flushes_providers() {
        use opentelemetry::trace::{Span as _, Tracer as _};

        let span_exporter = CapturingSpanExporter::default();
        let traces = opentelemetry_sdk::trace::SdkTracerProvider::builder()
            .with_batch_exporter(span_exporter.clone())
            .build();
        let metric_exporter = SnapshotExporter::default();
        let metrics = SdkMeterProvider::builder()
            .with_reader(PeriodicReader::builder(metric_exporter.clone()).build())
            .build();

        // Retain clones just like an installed `tracing-opentelemetry` layer and
        // `OtelMetrics` do in production so refcount-based Drop alone would leak
        // background processor/reader threads.
        let _retained_traces_clone = traces.clone();
        let _retained_metrics_clone = metrics.clone();

        let telemetry = Telemetry(Some(TelemetryProviders { traces, metrics }));
        telemetry.metrics().inc_saturated();
        let mut span = telemetry.tracer().unwrap().start("dropped_span");
        span.end();
        drop(telemetry);

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let spans_done = span_exporter.0.lock().unwrap().as_slice() == ["dropped_span"];
                let metrics_done = metric_exporter
                    .0
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|(name, _, _)| name == crate::metrics::CONNECTION_LIMIT_SATURATED);
                if spans_done && metrics_done {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("Telemetry::drop must flush span and metric providers before schedule delay");
    }

    #[test]
    fn attach_and_propagate_context_links_parent_trace_and_rewrites_child_span_id() {
        use opentelemetry::trace::TracerProvider as _;
        use tracing_subscriber::prelude::*;

        let tracer_provider = opentelemetry_sdk::trace::SdkTracerProvider::builder().build();
        let subscriber = tracing_subscriber::registry()
            .with(tracing_opentelemetry::layer().with_tracer(tracer_provider.tracer(SERVICE_NAME)));

        tracing::subscriber::with_default(subscriber, || {
            // 1. Valid incoming traceparent + tracestate: child span preserves trace-id
            // and tracestate while emitting a newly generated child span-id.
            let mut inbound = http::HeaderMap::new();
            inbound.insert(
                "traceparent",
                "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
                    .parse()
                    .unwrap(),
            );
            inbound.insert("tracestate", "vendor=ethonyx".parse().unwrap());

            let mut outbound = http::HeaderMap::new();
            let span = tracing::info_span!("request");
            attach_parent_context(&span, &inbound);
            {
                let _enter = span.enter();
                propagate_context(&mut outbound);
            }
            let tp = outbound["traceparent"].to_str().unwrap();
            assert!(
                tp.starts_with("00-4bf92f3577b34da6a3ce929d0e0e4736-"),
                "must preserve parent trace-id, got: {tp}"
            );
            assert!(
                !tp.contains("-00f067aa0ba902b7-"),
                "must rewrite span-id to the new child span, got: {tp}"
            );
            assert_eq!(outbound["tracestate"], "vendor=ethonyx");

            // 2. Connection-nominated incoming traceparent (`Connection: traceparent`):
            // rejected on extraction so outgoing traceparent starts a fresh root trace-id,
            // and any orphaned incoming `tracestate` is stripped rather than emitted empty.
            let mut nominated = inbound;
            nominated.insert("connection", "keep-alive, traceparent".parse().unwrap());
            let mut outbound_root = http::HeaderMap::new();
            outbound_root.insert("tracestate", "stale=orphan".parse().unwrap());
            let root_span = tracing::info_span!("request");
            attach_parent_context(&root_span, &nominated);
            {
                let _enter = root_span.enter();
                propagate_context(&mut outbound_root);
            }
            let root_tp = outbound_root["traceparent"].to_str().unwrap();
            assert!(
                root_tp.starts_with("00-")
                    && !root_tp.starts_with("00-4bf92f3577b34da6a3ce929d0e0e4736-"),
                "Connection-nominated traceparent must not link parent trace-id, got: {root_tp}"
            );
            assert!(
                !outbound_root.contains_key("tracestate"),
                "fresh root span must omit empty tracestate and strip orphaned tracestate"
            );

            // 3. Ambient thread-local OTel context must not bleed into a request whose
            // `traceparent` was stripped by `Connection: traceparent` or absent altogether.
            use opentelemetry::propagation::TextMapPropagator as _;
            use opentelemetry::trace::TraceContextExt as _;
            let ambient_cx = opentelemetry_sdk::propagation::TraceContextPropagator::new().extract(
                &super::HeaderMapCarrierRef(&{
                    let mut h = http::HeaderMap::new();
                    h.insert(
                        "traceparent",
                        "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01"
                            .parse()
                            .unwrap(),
                    );
                    h
                }),
            );
            assert!(ambient_cx.has_active_span());
            let _ambient_guard = ambient_cx.attach();
            let empty_headers = http::HeaderMap::new();
            for headers in [&nominated, &empty_headers] {
                let mut outbound_isolated = http::HeaderMap::new();
                let isolated_span = tracing::info_span!(parent: None, "request");
                attach_parent_context(&isolated_span, headers);
                {
                    let _enter = isolated_span.enter();
                    propagate_context(&mut outbound_isolated);
                }
                let isolated_tp = outbound_isolated["traceparent"].to_str().unwrap();
                assert!(
                    !isolated_tp.starts_with("00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-"),
                    "ambient OTel context must not leak into isolated request, got: {isolated_tp}"
                );
                assert!(
                    !outbound_isolated.contains_key("tracestate"),
                    "fresh isolated span must not inject an empty tracestate header"
                );
            }
        });

        let _ = tracer_provider.shutdown();
    }
}
