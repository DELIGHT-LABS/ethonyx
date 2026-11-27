//! OpenTelemetry lifecycle, W3C Trace Context propagation, OTLP HTTP client, and typed metrics.

mod client;
mod metrics;
mod propagation;

use std::time::Duration;

use opentelemetry_otlp::{WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::Resource;

pub(crate) use metrics::OtelMetrics;
pub(crate) use propagation::{attach_parent_context, mark_span_error, propagate_context};

const SERVICE_NAME: &str = env!("CARGO_PKG_NAME");
const OTLP_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

struct TelemetryProviders {
    // Declared first so fallback drops (if `Telemetry::shutdown` or `Telemetry::drop`
    // cannot spawn a helper thread) signal `ShutdownTx::drop` (`Instant::now()`)
    // before `traces`/`metrics` run blocking drops.
    shutdown_tx: crate::lifecycle::ShutdownTx,
    traces: Option<opentelemetry_sdk::trace::SdkTracerProvider>,
    metrics: opentelemetry_sdk::metrics::SdkMeterProvider,
}

impl TelemetryProviders {
    fn shutdown_blocking(self, tokio_deadline: tokio::time::Instant, timeout: Duration) {
        let Self {
            shutdown_tx,
            traces,
            metrics,
        } = self;
        // Publish a shared Tokio shutdown deadline so in-flight OTLP HTTP exports
        // (including `SdkMeterProvider::shutdown_with_timeout`, whose timeout
        // argument is ignored by `opentelemetry_sdk` 0.33.1) abort on time and
        // strip `Retry-After` throttle headers. Track the OS thread's own
        // sequential fallback budget against `std::time::Instant` so it never
        // reads `tokio::time::Instant::now()` outside a Tokio runtime context.
        shutdown_tx.send(tokio_deadline);
        let Some(traces) = traces else {
            if let Err(error) = metrics.shutdown_with_timeout(timeout) {
                tracing::warn!(%error, "OTel meter provider shutdown failed");
            }
            return;
        };
        let wall_now = std::time::Instant::now();
        let wall_deadline = wall_now.checked_add(timeout).unwrap_or(wall_now);
        std::thread::scope(|s| {
            let metrics_ref = &metrics;
            let metrics_handle = std::thread::Builder::new()
                .name("otel-metrics-sd".into())
                .spawn_scoped(s, move || {
                    if let Err(error) = metrics_ref.shutdown_with_timeout(timeout) {
                        tracing::warn!(%error, "OTel meter provider shutdown failed");
                    }
                });
            if let Err(error) = traces.shutdown_with_timeout(timeout) {
                tracing::warn!(%error, "OTel tracer provider shutdown failed");
            }
            match metrics_handle {
                Ok(handle) => {
                    // Explicit join absorbs any panic from the metrics thread so
                    // `Telemetry::drop` cannot double-panic during unwinding.
                    let _ = handle.join();
                }
                Err(error) => {
                    tracing::warn!(
                        %error,
                        "failed to spawn OTel metrics shutdown thread; shutting down sequentially"
                    );
                    let remaining =
                        wall_deadline.saturating_duration_since(std::time::Instant::now());
                    if let Err(error) = metrics.shutdown_with_timeout(remaining) {
                        tracing::warn!(%error, "OTel meter provider shutdown failed");
                    }
                }
            }
        });
    }
}

pub(crate) struct Telemetry(Option<TelemetryProviders>);

impl Telemetry {
    pub(crate) fn tracer(&self) -> Option<opentelemetry_sdk::trace::SdkTracer> {
        use opentelemetry::trace::TracerProvider as _;
        self.0
            .as_ref()
            .and_then(|p| p.traces.as_ref())
            .map(|t| t.tracer(SERVICE_NAME))
    }

    /// Typed OpenTelemetry metric instruments mirroring this server's metrics.
    pub(crate) fn metrics(&self) -> OtelMetrics {
        use opentelemetry::metrics::MeterProvider as _;
        match &self.0 {
            Some(providers) => OtelMetrics::from_meter(providers.metrics.meter(SERVICE_NAME)),
            None => OtelMetrics::disabled(),
        }
    }

    pub(crate) async fn shutdown(self) {
        self.shutdown_with_timeout(OTLP_SHUTDOWN_TIMEOUT).await;
    }

    async fn shutdown_with_timeout(mut self, timeout: Duration) {
        let Some(providers) = self.0.take() else {
            return;
        };
        let now = tokio::time::Instant::now();
        let deadline = now.checked_add(timeout).unwrap_or(now);
        // Run blocking OTel provider shutdown on a dedicated detached OS thread
        // instead of Tokio's blocking pool so:
        // 1. `tokio::time::timeout_at` enforces a strict upper bound even if
        //    `opentelemetry-otlp` sleeps in synchronous retry backoff after the
        //    HTTP export deadline elapses, and
        // 2. If the timeout elapses, `#[tokio::main]`'s `Runtime::drop` does not
        //    stall waiting for a `spawn_blocking` thread to finish.
        let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();
        match std::thread::Builder::new()
            .name("otel-shutdown".into())
            .spawn(move || {
                providers.shutdown_blocking(deadline, timeout);
                let _ = done_tx.send(());
            }) {
            Ok(_handle) => {
                if tokio::time::timeout_at(deadline, done_rx).await.is_err() {
                    tracing::warn!(
                        timeout_secs = timeout.as_secs_f64(),
                        "OTel provider shutdown timed out; continuing exit"
                    );
                }
            }
            Err(error) => {
                tracing::warn!(
                    %error,
                    "failed to spawn OTel shutdown thread; skipping graceful flush"
                );
            }
        }
    }
}

impl Drop for Telemetry {
    fn drop(&mut self) {
        if let Some(providers) = self.0.take() {
            let now = tokio::time::Instant::now();
            let deadline = now.checked_add(OTLP_SHUTDOWN_TIMEOUT).unwrap_or(now);
            // Installed subscriber layers (`SdkTracer`) and `OtelMetrics` (`Meter`)
            // retain strong clones of the underlying pipelines, so abnormal drops
            // without `shutdown().await` offload explicit shutdown to a detached
            // thread without blocking async workers or coupling to a dying runtime.
            let _ = std::thread::Builder::new()
                .name("otel-drop".into())
                .spawn(move || providers.shutdown_blocking(deadline, OTLP_SHUTDOWN_TIMEOUT));
        }
    }
}

/// Pre-validated OTLP HTTP/S signal URLs constructed during `Config::parse_with_otel()`.
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
        Ok(Self {
            traces: base
                .join_static_path("--otel-endpoint", "/v1/traces")?
                .to_string(),
            metrics: base
                .join_static_path("--otel-endpoint", "/v1/metrics")?
                .to_string(),
        })
    }
}

/// Start the OTLP metric (and optionally trace) providers for validated endpoints.
pub(crate) fn start(
    endpoints: Option<OtelEndpoints>,
    enable_traces: bool,
) -> Result<Telemetry, crate::BoxError> {
    let Some(endpoints) = endpoints else {
        return Ok(Telemetry(None));
    };
    init_otel(endpoints, enable_traces, tokio::runtime::Handle::current())
}

pub(crate) fn parse_config_endpoint(
    endpoint: Option<&str>,
) -> Result<Option<OtelEndpoints>, crate::BoxError> {
    endpoint.map(OtelEndpoints::parse).transpose()
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

fn init_otel(
    endpoints: OtelEndpoints,
    enable_traces: bool,
    rt: tokio::runtime::Handle,
) -> Result<Telemetry, crate::BoxError> {
    let (shutdown_tx, shutdown) = crate::lifecycle::shutdown_channel();
    let (trace_client, metric_client) = client::build_signal_clients(rt, shutdown, enable_traces);
    let metric_timeout = metric_client.timeout();
    let meter_exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_http()
        .with_http_client(metric_client)
        .with_endpoint(endpoints.metrics)
        .with_timeout(metric_timeout)
        .build()?;

    let resource = build_resource();
    let traces = match trace_client {
        Some(trace_client) => {
            let trace_timeout = trace_client.timeout();
            let exporter = opentelemetry_otlp::SpanExporter::builder()
                .with_http()
                .with_http_client(trace_client)
                .with_endpoint(endpoints.traces)
                .with_timeout(trace_timeout)
                .build()?;
            Some(
                opentelemetry_sdk::trace::SdkTracerProvider::builder()
                    .with_batch_exporter(exporter)
                    .with_resource(resource.clone())
                    .build(),
            )
        }
        None => None,
    };

    let meter_provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
        .with_resource(resource)
        .with_periodic_exporter(meter_exporter)
        .build();

    Ok(Telemetry(Some(TelemetryProviders {
        traces,
        metrics: meter_provider,
        shutdown_tx,
    })))
}

#[cfg(test)]
mod tests {
    use super::metrics::SnapshotExporter;
    use super::propagation::CapturingSpanExporter;
    use super::{OtelEndpoints, Resource, Telemetry, TelemetryProviders, build_resource_with};
    use opentelemetry::{Key, KeyValue, Value};
    use opentelemetry_sdk::metrics::SdkMeterProvider;
    use opentelemetry_sdk::resource::ResourceDetector;
    use std::time::Duration;

    struct StaticAttrDetector(&'static str);

    impl ResourceDetector for StaticAttrDetector {
        fn detect(&self) -> Resource {
            Resource::builder_empty()
                .with_attribute(KeyValue::new("service.name", self.0))
                .build()
        }
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

    #[tokio::test]
    async fn dropping_telemetry_without_explicit_shutdown_flushes_providers() {
        use opentelemetry::trace::{Span as _, Tracer as _};

        let span_exporter = CapturingSpanExporter::default();
        let traces = opentelemetry_sdk::trace::SdkTracerProvider::builder()
            .with_batch_exporter(span_exporter.clone())
            .build();
        let metric_exporter = SnapshotExporter::default();
        let metrics = SdkMeterProvider::builder()
            .with_periodic_exporter(metric_exporter.clone())
            .build();

        // Retain clones just like an installed `tracing-opentelemetry` layer and
        // `OtelMetrics` do in production so refcount-based Drop alone would leak
        // background processor/reader threads.
        let _retained_traces_clone = traces.clone();
        let _retained_metrics_clone = metrics.clone();

        let (shutdown_tx, _shutdown) = crate::lifecycle::shutdown_channel();
        let telemetry = Telemetry(Some(TelemetryProviders {
            traces: Some(traces),
            metrics,
            shutdown_tx,
        }));
        telemetry.metrics().inc_saturated();
        let mut span = telemetry.tracer().unwrap().start("dropped_span");
        span.end();
        drop(telemetry);

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let spans_done = span_exporter.has_span("dropped_span");
                let metrics_done =
                    metric_exporter.has_metric(crate::metrics::CONNECTION_LIMIT_SATURATED);
                if spans_done && metrics_done {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("Telemetry::drop must flush span and metric providers before schedule delay");
    }

    #[tokio::test]
    async fn telemetry_shutdown_bounds_hanging_metric_provider_without_stalling() {
        use opentelemetry_sdk::metrics::data::ResourceMetrics;
        use opentelemetry_sdk::metrics::exporter::PushMetricExporter;
        use std::sync::{Arc, Mutex, mpsc};

        #[derive(Clone)]
        struct BlockingMetricExporter(Arc<Mutex<mpsc::Receiver<()>>>);

        impl PushMetricExporter for BlockingMetricExporter {
            async fn export(
                &self,
                _metrics: &ResourceMetrics,
            ) -> opentelemetry_sdk::error::OTelSdkResult {
                let _ = self.0.lock().unwrap().recv_timeout(Duration::from_secs(5));
                Ok(())
            }

            fn force_flush(&self) -> opentelemetry_sdk::error::OTelSdkResult {
                Ok(())
            }

            fn shutdown_with_timeout(
                &self,
                _timeout: Duration,
            ) -> opentelemetry_sdk::error::OTelSdkResult {
                Ok(())
            }

            fn temporality(&self) -> opentelemetry_sdk::metrics::Temporality {
                opentelemetry_sdk::metrics::Temporality::Cumulative
            }
        }

        let (release_tx, release_rx) = mpsc::channel::<()>();
        let exporter = BlockingMetricExporter(Arc::new(Mutex::new(release_rx)));
        let metrics = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter)
            .build();
        let traces = opentelemetry_sdk::trace::SdkTracerProvider::builder().build();
        let (shutdown_tx, shutdown) = crate::lifecycle::shutdown_channel();
        let telemetry = Telemetry(Some(TelemetryProviders {
            traces: Some(traces),
            metrics,
            shutdown_tx,
        }));
        telemetry.metrics().inc_saturated();

        let started = tokio::time::Instant::now();
        telemetry
            .shutdown_with_timeout(Duration::from_millis(80))
            .await;
        let elapsed = started.elapsed();

        assert!(
            elapsed < Duration::from_secs(1),
            "Telemetry::shutdown must return promptly at its deadline even when a provider hangs (elapsed={elapsed:?})"
        );
        // Confirm the spawned shutdown thread published the export deadline
        // (waiting briefly so a heavily loaded CI runner scheduling the new OS
        // thread cannot flake before `shutdown_tx.send` runs).
        tokio::time::timeout(Duration::from_secs(1), shutdown.deadline())
            .await
            .expect("shutdown thread must publish the export deadline");
        // Unblock the detached shutdown thread cleanly before the test returns.
        let _ = release_tx.send(());
    }

    #[tokio::test]
    async fn metrics_only_telemetry_skips_tracer_and_flushes_metrics_on_shutdown() {
        let metric_exporter = SnapshotExporter::default();
        let metrics = SdkMeterProvider::builder()
            .with_periodic_exporter(metric_exporter.clone())
            .build();
        let (shutdown_tx, shutdown) = crate::lifecycle::shutdown_channel();
        let telemetry = Telemetry(Some(TelemetryProviders {
            traces: None,
            metrics,
            shutdown_tx,
        }));
        assert!(telemetry.tracer().is_none());
        telemetry.metrics().set_health(
            crate::metrics::HealthInstrument::UpstreamHealth,
            Some(crate::metrics::UpstreamLayer::El),
            1.0,
        );
        telemetry.shutdown().await;

        assert!(shutdown.current_deadline().is_some());
        assert!(metric_exporter.has_metric("ethonyx_upstream_health_status"));
        assert!(!metric_exporter.has_metric(crate::metrics::ACTIVE_CONNECTIONS));
        assert!(!metric_exporter.has_metric(crate::metrics::PROXY_REQUEST_DURATION));
        assert!(!metric_exporter.has_metric(crate::metrics::PROXY_REQUESTS_TOTAL));
    }
}
