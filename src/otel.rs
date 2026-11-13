use opentelemetry::global;
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
        if let Ok(val) = http::HeaderValue::try_from(value) {
            self.0.insert(name, val);
        }
    }
}

struct HeaderMapCarrierRef<'a>(&'a http::HeaderMap);

impl<'a> opentelemetry::propagation::Extractor for HeaderMapCarrierRef<'a> {
    fn get(&self, key: &str) -> Option<&str> {
        let val = self.0.get(key)?.to_str().ok()?;
        (!crate::headers::is_connection_nominated(self.0, key)).then_some(val)
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(|k| k.as_str()).collect()
    }
}

/// Link `span` to any non-`Connection`-nominated parent trace context in `headers`.
/// Must be called BEFORE `span` is first entered so `tracing-opentelemetry` builds
/// the underlying OTel `Span` with the extracted parent `TraceId`.
pub(crate) fn attach_parent_context(span: &tracing::Span, headers: &http::HeaderMap) {
    if span.is_disabled() {
        return;
    }
    let parent_cx = global::get_text_map_propagator(|propagator| {
        propagator.extract(&HeaderMapCarrierRef(headers))
    });
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
    let cx = tracing_opentelemetry::OpenTelemetrySpanExt::context(&span);
    global::get_text_map_propagator(|propagator| {
        propagator.inject_context(&cx, &mut HeaderMapCarrier(headers))
    });
}

pub(crate) struct Telemetry {
    traces: opentelemetry_sdk::trace::SdkTracerProvider,
    metrics: opentelemetry_sdk::metrics::SdkMeterProvider,
}

impl Telemetry {
    pub(crate) fn tracer(&self) -> opentelemetry_sdk::trace::SdkTracer {
        use opentelemetry::trace::TracerProvider;
        self.traces.tracer(SERVICE_NAME)
    }

    pub(crate) async fn shutdown(self) {
        // Blocking readers/exporters must be joined outside the async worker.
        let _ = tokio::task::spawn_blocking(move || {
            let _ = self.metrics.shutdown();
            let _ = self.traces.shutdown();
        })
        .await;
    }
}

/// Start the OTLP trace and metric providers for a validated endpoint.
pub(crate) async fn start(
    base: Option<crate::upstream::HttpUpstream>,
) -> Result<Option<Telemetry>, crate::BoxError> {
    let Some(base) = base else {
        return Ok(None);
    };
    tokio::task::spawn_blocking(move || init_otel(&base))
        .await?
        .map(Some)
}

pub(crate) fn parse_endpoint(
    endpoint: &str,
) -> Result<crate::upstream::HttpUpstream, crate::BoxError> {
    let upstream = crate::upstream::HttpUpstream::parse("--otel-endpoint", endpoint)?;
    if upstream.uri().query().is_some() {
        return Err(format!(
            "invalid --otel-endpoint ({}): OTLP endpoint must be an HTTP(S) base URL without a query or fragment",
            crate::upstream::RedactedUrl(endpoint)
        )
        .into());
    }
    Ok(upstream)
}

fn signal_url(base: &crate::upstream::HttpUpstream, signal: &str) -> String {
    let uri = base.uri();
    let scheme = uri.scheme_str().expect("validated scheme");
    let authority = uri.authority().expect("validated authority");
    let prefix = uri.path().trim_end_matches('/');
    format!("{scheme}://{authority}{prefix}/v1/{signal}")
}

#[cfg(test)]
fn signal_endpoint(endpoint: &str, signal: &str) -> Result<String, crate::BoxError> {
    parse_endpoint(endpoint).map(|base| signal_url(&base, signal))
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

fn init_otel(base: &crate::upstream::HttpUpstream) -> Result<Telemetry, crate::BoxError> {
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_endpoint(signal_url(base, "traces"))
        .build()?;
    let meter_exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_http()
        .with_endpoint(signal_url(base, "metrics"))
        .build()?;

    let resource = build_resource();
    global::set_text_map_propagator(opentelemetry_sdk::propagation::TraceContextPropagator::new());

    let tracer_provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(resource.clone())
        .build();
    global::set_tracer_provider(tracer_provider.clone());

    let reader = opentelemetry_sdk::metrics::PeriodicReader::builder(meter_exporter).build();
    let meter_provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
        .with_resource(resource)
        .with_reader(reader)
        .build();
    global::set_meter_provider(meter_provider.clone());

    Ok(Telemetry {
        traces: tracer_provider,
        metrics: meter_provider,
    })
}

// ---- Custom metrics::Recorder bridge for OpenTelemetry ----

use metrics::{Counter, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit};
use opentelemetry::KeyValue;
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub(crate) struct OtelRecorder {
    meter: opentelemetry::metrics::Meter,
    counters: RwLock<HashMap<Key, Counter>>,
    gauges: RwLock<HashMap<Key, Gauge>>,
    histograms: RwLock<HashMap<Key, Histogram>>,
}

impl Default for OtelRecorder {
    fn default() -> Self {
        Self::new()
    }
}

impl OtelRecorder {
    pub(crate) fn new() -> Self {
        Self::with_meter(global::meter(SERVICE_NAME))
    }

    fn with_meter(meter: opentelemetry::metrics::Meter) -> Self {
        Self {
            meter,
            counters: RwLock::new(HashMap::new()),
            gauges: RwLock::new(HashMap::new()),
            histograms: RwLock::new(HashMap::new()),
        }
    }
}

fn convert_labels(key: &Key) -> Box<[KeyValue]> {
    key.labels()
        .cloned()
        .map(|label| {
            let (k, v) = label.into_parts();
            KeyValue::new(Cow::from(k), Cow::from(v))
        })
        .collect()
}

fn get_or_register<H: Clone>(
    cache: &RwLock<HashMap<Key, H>>,
    key: &Key,
    build: impl FnOnce(&Key) -> H,
) -> H {
    if let Some(existing) = cache.read().unwrap().get(key) {
        return existing.clone();
    }
    let mut write = cache.write().unwrap();
    if let Some(existing) = write.get(key) {
        return existing.clone();
    }
    let handle = build(key);
    write.insert(key.clone(), handle.clone());
    handle
}

impl Recorder for OtelRecorder {
    fn describe_counter(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}
    fn describe_gauge(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}
    fn describe_histogram(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    fn register_counter(&self, key: &Key, _metadata: &Metadata<'_>) -> Counter {
        get_or_register(&self.counters, key, |k| {
            let counter = self.meter.u64_counter(k.name_shared()).build();
            Counter::from_arc(Arc::new(OtelCounter {
                counter,
                labels: convert_labels(k),
            }))
        })
    }

    fn register_gauge(&self, key: &Key, _metadata: &Metadata<'_>) -> Gauge {
        get_or_register(&self.gauges, key, |k| {
            let labels = convert_labels(k);
            if k.name() == crate::metrics::ACTIVE_CONNECTIONS {
                let counter = self.meter.f64_up_down_counter(k.name_shared()).build();
                Gauge::from_arc(Arc::new(OtelUpDownGauge { counter, labels }))
            } else {
                let gauge = self.meter.f64_gauge(k.name_shared()).build();
                Gauge::from_arc(Arc::new(OtelValueGauge { gauge, labels }))
            }
        })
    }

    fn register_histogram(&self, key: &Key, _metadata: &Metadata<'_>) -> Histogram {
        get_or_register(&self.histograms, key, |k| {
            let histogram = self.meter.f64_histogram(k.name_shared()).build();
            Histogram::from_arc(Arc::new(OtelHistogram {
                histogram,
                labels: convert_labels(k),
            }))
        })
    }
}

struct OtelCounter {
    counter: opentelemetry::metrics::Counter<u64>,
    labels: Box<[KeyValue]>,
}

impl metrics::CounterFn for OtelCounter {
    fn increment(&self, value: u64) {
        self.counter.add(value, &self.labels);
    }

    fn absolute(&self, _value: u64) {}
}

struct OtelValueGauge {
    gauge: opentelemetry::metrics::Gauge<f64>,
    labels: Box<[KeyValue]>,
}

impl metrics::GaugeFn for OtelValueGauge {
    fn increment(&self, _value: f64) {}

    fn decrement(&self, _value: f64) {}

    fn set(&self, value: f64) {
        self.gauge.record(value, &self.labels);
    }
}

struct OtelUpDownGauge {
    counter: opentelemetry::metrics::UpDownCounter<f64>,
    labels: Box<[KeyValue]>,
}

impl metrics::GaugeFn for OtelUpDownGauge {
    fn increment(&self, value: f64) {
        self.counter.add(value, &self.labels);
    }

    fn decrement(&self, value: f64) {
        self.counter.add(-value, &self.labels);
    }

    fn set(&self, _value: f64) {}
}

struct OtelHistogram {
    histogram: opentelemetry::metrics::Histogram<f64>,
    labels: Box<[KeyValue]>,
}

impl metrics::HistogramFn for OtelHistogram {
    fn record(&self, value: f64) {
        self.histogram.record(value, &self.labels);
    }
}

#[cfg(test)]
mod tests {
    use super::{OtelRecorder, Resource, SERVICE_NAME, build_resource_with, signal_endpoint};
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
                for metric in scope_metrics.metrics() {
                    let (kind, points) = match metric.data() {
                        AggregatedMetrics::F64(MetricData::Sum(sum)) => {
                            let kind = if sum.is_monotonic() {
                                "monotonic_sum"
                            } else {
                                "up_down_sum"
                            };
                            let pts = sum
                                .data_points()
                                .map(|p| (p.attributes().cloned().collect(), p.value()))
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
    fn otel_recorder_exports_active_connections_as_up_down_sum_and_other_gauges_as_gauge() {
        assert_eq!(SERVICE_NAME, "ethonyx");
        let exporter = SnapshotExporter::default();
        let provider = SdkMeterProvider::builder()
            .with_reader(PeriodicReader::builder(exporter.clone()).build())
            .build();
        let recorder = OtelRecorder::with_meter(provider.meter(SERVICE_NAME));

        let proxy_metrics = crate::metrics::ProxyMetrics::default();
        let health_metrics = crate::metrics::HealthMetrics::default();
        let tcp_guard = ::metrics::with_local_recorder(&recorder, || {
            let tcp = proxy_metrics.active_tcp();
            // Open and immediately drop a WS connection to verify symmetrical +1 / -1.
            drop(proxy_metrics.active_ws());
            health_metrics.record(crate::metrics::HealthSample {
                el_ok: true,
                cl_ok: true,
                el_peers: 8.0,
                cl_peers: 12.0,
                el_distance: 0.0,
                cl_status: Some(crate::metrics::ClSlotSample {
                    sync_distance: 0,
                    head_slot: 42,
                }),
                el_block: Some(100),
            });
            tcp
        });

        provider.force_flush().unwrap();
        // Dropping `tcp_guard` outside `with_local_recorder` exercises the retained `Gauge` handle.
        drop(tcp_guard);
        provider.force_flush().unwrap();
        provider.shutdown().unwrap();

        let snapshots = exporter.0.lock().unwrap();
        let first_active = snapshots
            .iter()
            .find(|(name, _, _)| name == crate::metrics::ACTIVE_CONNECTIONS)
            .expect("active_connections exported on first flush");
        assert_eq!(first_active.1, "up_down_sum");
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
            .rfind(|(name, _, _)| name == crate::metrics::ACTIVE_CONNECTIONS)
            .expect("active_connections exported after tcp_guard drop");
        assert!(
            final_active
                .2
                .contains(&(vec![KeyValue::new("protocol", "tcp")], 0.0))
        );

        let peers = snapshots
            .iter()
            .find(|(name, _, _)| name == "ethonyx_upstream_peers")
            .expect("upstream_peers exported");
        assert_eq!(peers.1, "gauge");
        assert!(peers.2.contains(&(vec![KeyValue::new("layer", "el")], 8.0)));
        assert!(
            peers
                .2
                .contains(&(vec![KeyValue::new("layer", "cl")], 12.0))
        );
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
            assert_eq!(
                signal_endpoint(base, "traces").unwrap(),
                format!("{expected_scheme}/v1/traces")
            );
            assert_eq!(
                signal_endpoint(base, "metrics").unwrap(),
                format!("{expected_scheme}/v1/metrics")
            );
        }
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
        ] {
            let err = signal_endpoint(invalid, "traces")
                .expect_err(invalid)
                .to_string();
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
                    .body(crate::transport::box_full(Full::new(Bytes::from_static(
                        b"{}",
                    ))))
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
}
