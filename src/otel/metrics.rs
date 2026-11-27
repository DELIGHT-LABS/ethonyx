//! Typed OpenTelemetry metric instruments mirroring `crate::metrics::ServerMetrics`.

use std::sync::OnceLock;
use std::time::Duration;

use opentelemetry::KeyValue;

use crate::metrics::{
    ACTIVE_CONNECTIONS, BodyErrorReason, CONNECTION_LIMIT_SATURATED, ConnProtocol,
    HealthInstrument, LATENCY_BUCKET_BOUNDS, MethodBucket, PROXY_REQUEST_DURATION,
    PROXY_REQUESTS_TOTAL, RESPONSE_BODY_ERRORS_TOTAL, StatusSlot, UpstreamLayer,
};

struct OtelMetricsInner {
    meter: opentelemetry::metrics::Meter,
    // Kept lazy: `opentelemetry_sdk` pre-allocates ~135 KiB per built instrument,
    // so unused proxy/saturation/body-error/health instruments (e.g. probe-only,
    // EL-only, or `--trust-upstream`) stay zero-cost.
    active_connections: OnceLock<opentelemetry::metrics::UpDownCounter<i64>>,
    saturated: OnceLock<opentelemetry::metrics::Counter<u64>>,
    body_errors: OnceLock<opentelemetry::metrics::Counter<u64>>,
    duration: OnceLock<opentelemetry::metrics::Histogram<f64>>,
    requests: OnceLock<opentelemetry::metrics::Counter<u64>>,
    health: [OnceLock<opentelemetry::metrics::Gauge<f64>>; HealthInstrument::COUNT],
}

impl OtelMetricsInner {
    fn duration(&self) -> &opentelemetry::metrics::Histogram<f64> {
        self.duration.get_or_init(|| {
            self.meter
                .f64_histogram(PROXY_REQUEST_DURATION)
                .with_unit("s")
                .with_boundaries(
                    LATENCY_BUCKET_BOUNDS
                        .iter()
                        .map(Duration::as_secs_f64)
                        .collect(),
                )
                .build()
        })
    }
}

/// Typed OpenTelemetry metric fanout owned directly by [`crate::metrics::ServerMetrics`].
///
/// Builds one OTel instrument per metric name lazily on first use (zero per-series
/// wrapper allocations or `Recorder` virtual dispatch, and zero unused proxy instruments
/// in probe-only mode).
pub(crate) struct OtelMetrics(Option<Box<OtelMetricsInner>>);

impl OtelMetrics {
    pub(crate) const fn disabled() -> Self {
        Self(None)
    }

    pub(super) fn from_meter(meter: opentelemetry::metrics::Meter) -> Self {
        Self(Some(Box::new(OtelMetricsInner {
            meter,
            active_connections: OnceLock::new(),
            saturated: OnceLock::new(),
            body_errors: OnceLock::new(),
            duration: OnceLock::new(),
            requests: OnceLock::new(),
            health: [const { OnceLock::new() }; HealthInstrument::COUNT],
        })))
    }

    pub(crate) fn add_active(&self, protocol: ConnProtocol, delta: i64) {
        if let Some(m) = &self.0 {
            m.active_connections
                .get_or_init(|| m.meter.i64_up_down_counter(ACTIVE_CONNECTIONS).build())
                .add(delta, &[KeyValue::new("protocol", protocol.as_str())]);
        }
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
        layer: UpstreamLayer,
        method: MethodBucket,
        status: StatusSlot,
        elapsed: Duration,
    ) {
        if let Some(m) = &self.0 {
            let upstream = layer.as_str();
            m.duration().record(
                elapsed.as_secs_f64(),
                &[KeyValue::new("upstream", upstream)],
            );
            // Keep keys sorted ascending so `opentelemetry_sdk`'s `ValueMap` retains one
            // `Vec<KeyValue>` entry per series instead of both unsorted and sorted copies.
            m.requests
                .get_or_init(|| m.meter.u64_counter(PROXY_REQUESTS_TOTAL).build())
                .add(
                    1,
                    &[
                        KeyValue::new("method", method.as_str()),
                        KeyValue::new("status", status.as_str()),
                        KeyValue::new("upstream", upstream),
                    ],
                );
        }
    }

    pub(crate) fn inc_body_error(&self, layer: UpstreamLayer, reason: BodyErrorReason) {
        if let Some(m) = &self.0 {
            m.body_errors
                .get_or_init(|| m.meter.u64_counter(RESPONSE_BODY_ERRORS_TOTAL).build())
                .add(
                    1,
                    &[
                        KeyValue::new("reason", reason.as_str()),
                        KeyValue::new("upstream", layer.as_str()),
                    ],
                );
        }
    }

    pub(crate) fn set_health(
        &self,
        instrument: HealthInstrument,
        layer: Option<UpstreamLayer>,
        value: f64,
    ) {
        if let Some(m) = &self.0 {
            let gauge = m.health[instrument as usize]
                .get_or_init(|| m.meter.f64_gauge(instrument.name()).build());
            match layer {
                Some(layer) => gauge.record(value, &[KeyValue::new("layer", layer.as_lower_str())]),
                None => gauge.record(value, &[]),
            }
        }
    }
}

#[cfg(test)]
type ExportedMetric = (String, &'static str, Vec<(Vec<KeyValue>, f64)>);

#[cfg(test)]
#[derive(Clone, Default)]
pub(super) struct SnapshotExporter(std::sync::Arc<std::sync::Mutex<Vec<ExportedMetric>>>);

#[cfg(test)]
impl SnapshotExporter {
    pub(super) fn has_metric(&self, name: &str) -> bool {
        self.0.lock().unwrap().iter().any(|(n, _, _)| n == name)
    }
}

#[cfg(test)]
impl opentelemetry_sdk::metrics::exporter::PushMetricExporter for SnapshotExporter {
    async fn export(
        &self,
        metrics: &opentelemetry_sdk::metrics::data::ResourceMetrics,
    ) -> opentelemetry_sdk::error::OTelSdkResult {
        use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};

        let mut out = self.0.lock().unwrap();
        for scope_metrics in metrics.scope_metrics() {
            assert_eq!(scope_metrics.scope().name(), "ethonyx");
            let mut seen_names = std::collections::HashSet::<&str>::new();
            for metric in scope_metrics.metrics() {
                let name = metric.name();
                assert!(
                    seen_names.insert(name),
                    "duplicate exported metric stream: {name}"
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
                        assert_eq!(
                            metric.unit(),
                            "s",
                            "duration histogram must declare second unit"
                        );
                        let mut pts = Vec::new();
                        let mut sum_pts = Vec::new();
                        for p in hist.data_points() {
                            let bounds: Vec<f64> = p.bounds().collect();
                            let expected: Vec<f64> = LATENCY_BUCKET_BOUNDS
                                .iter()
                                .map(Duration::as_secs_f64)
                                .collect();
                            assert_eq!(
                                bounds, expected,
                                "OTel histogram boundaries must match Prometheus LATENCY_BUCKET_BOUNDS"
                            );
                            let attrs: Vec<KeyValue> = p.attributes().cloned().collect();
                            sum_pts.push((attrs.clone(), p.sum()));
                            pts.push((attrs, p.count() as f64));
                        }
                        out.push((format!("{name}_sum"), "histogram_sum", sum_pts));
                        ("histogram", pts)
                    }
                    other => panic!("unexpected metric aggregation: {other:?}"),
                };
                out.push((name.to_owned(), kind, points));
            }
        }
        Ok(())
    }

    fn force_flush(&self) -> opentelemetry_sdk::error::OTelSdkResult {
        Ok(())
    }

    fn shutdown_with_timeout(
        &self,
        _timeout: std::time::Duration,
    ) -> opentelemetry_sdk::error::OTelSdkResult {
        Ok(())
    }

    fn temporality(&self) -> opentelemetry_sdk::metrics::Temporality {
        opentelemetry_sdk::metrics::Temporality::Cumulative
    }
}

#[cfg(test)]
mod tests {
    use super::{OtelMetrics, SnapshotExporter};
    use crate::otel::SERVICE_NAME;
    use opentelemetry::KeyValue;
    use opentelemetry::metrics::MeterProvider;
    use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
    use std::time::Duration;

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
        // Exercise both common and edge method/status buckets twice each so direct atomic
        // indexing and OTel SDK stream deduplication are verified end-to-end.
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
        // Untracked status codes collapse into `status="OTHER"` on OTel as well as Prometheus.
        server_metrics.record_request(
            crate::metrics::UpstreamLayer::El,
            crate::metrics::MethodBucket::Get,
            418,
            Duration::from_millis(1),
        );
        {
            use crate::metrics::HealthGauge as G;
            // Register CL peers first while leaving EL peers unset: OTel must NOT emit
            // a synthetic `layer="el"` datapoint until EL peers is explicitly recorded.
            server_metrics.set_health(G::ClPeers, 12.0);
            provider.force_flush().unwrap();
            assert!(
                !exporter.has_metric(crate::metrics::RESPONSE_BODY_ERRORS_TOTAL),
                "body error counter must stay unregistered until a body error occurs"
            );

            server_metrics.inc_body_error(
                crate::metrics::UpstreamLayer::El,
                crate::metrics::BodyErrorReason::IdleTimeout,
            );
            server_metrics.inc_body_error(
                crate::metrics::UpstreamLayer::Cl,
                crate::metrics::BodyErrorReason::UpstreamError,
            );

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

            server_metrics.set_health(G::ElPeers, 0.0);
        }

        // Dropping `tcp_guard` exercises the `-1` OTel active-connection update.
        drop(tcp_guard);
        provider.force_flush().unwrap();
        provider.shutdown().unwrap();

        let snapshots = exporter.0.lock().unwrap();
        let assert_metric =
            |name: &str, kind: &str, expected: &[(&[(&'static str, &'static str)], f64)]| {
                let exported = snapshots
                    .iter()
                    .find(|(n, _, _)| n == name)
                    .unwrap_or_else(|| panic!("{name} exported"));
                assert_eq!(exported.1, kind, "{name} kind");
                for &(attrs, val) in expected {
                    let kvs: Vec<_> = attrs.iter().map(|&(k, v)| KeyValue::new(k, v)).collect();
                    assert!(
                        exported.2.contains(&(kvs, val)),
                        "{name}: expected ({attrs:?}, {val}) in {:?}",
                        exported.2
                    );
                }
                exported
            };

        assert_metric(
            "ethonyx_active_connections",
            "i64_up_down_sum",
            &[(&[("protocol", "tcp")], 1.0), (&[("protocol", "ws")], 0.0)],
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

        assert_metric(
            "ethonyx_proxy_requests_total",
            "u64_monotonic_sum",
            &[
                (
                    &[("method", "POST"), ("status", "200"), ("upstream", "EL")],
                    2.0,
                ),
                (
                    &[("method", "OPTIONS"), ("status", "206"), ("upstream", "CL")],
                    2.0,
                ),
                (
                    &[("method", "OTHER"), ("status", "429"), ("upstream", "EL")],
                    2.0,
                ),
                (
                    &[("method", "GET"), ("status", "OTHER"), ("upstream", "EL")],
                    1.0,
                ),
            ],
        );
        assert_metric(
            "ethonyx_proxy_request_duration_seconds",
            "histogram",
            &[(&[("upstream", "EL")], 5.0), (&[("upstream", "CL")], 2.0)],
        );
        let hist_sum = snapshots
            .iter()
            .find(|(n, kind, _)| {
                n == "ethonyx_proxy_request_duration_seconds_sum" && *kind == "histogram_sum"
            })
            .expect("duration histogram sum exported");
        // EL: 2*5ms + 2*2ms + 1ms = 0.015s; CL: 2*3ms = 0.006s (catches seconds vs ms drift).
        for (upstream, expected_sum) in [("EL", 0.015_f64), ("CL", 0.006_f64)] {
            let kvs = vec![KeyValue::new("upstream", upstream)];
            let (_, actual) = hist_sum
                .2
                .iter()
                .find(|(attrs, _)| *attrs == kvs)
                .unwrap_or_else(|| panic!("missing histogram sum for {upstream}"));
            assert!(
                (*actual - expected_sum).abs() < 1e-9,
                "OTel histogram sum for {upstream} must be in seconds: expected {expected_sum}, got {actual}"
            );
        }
        let saturated = assert_metric(
            "ethonyx_connection_limit_saturated_total",
            "u64_monotonic_sum",
            &[(&[], 1.0)],
        );
        assert_eq!(saturated.2.len(), 1);
        assert_metric(
            "ethonyx_response_body_errors_total",
            "u64_monotonic_sum",
            &[
                (&[("reason", "idle_timeout"), ("upstream", "EL")], 1.0),
                (&[("reason", "upstream_error"), ("upstream", "CL")], 1.0),
            ],
        );

        for (name, el, cl) in [
            ("ethonyx_upstream_health_status", 1.0, 1.0),
            ("ethonyx_upstream_sync_distance", 0.0, 0.0),
        ] {
            assert_metric(
                name,
                "gauge",
                &[(&[("layer", "el")], el), (&[("layer", "cl")], cl)],
            );
        }
        for (name, val) in [
            ("ethonyx_reference_health_status", 1.0),
            ("ethonyx_reference_block_number", 100.0),
            ("ethonyx_reference_block_lag", 0.0),
            ("ethonyx_upstream_block_number", 100.0),
            ("ethonyx_upstream_slot_number", 42.0),
        ] {
            assert_metric(name, "gauge", &[(&[], val)]);
        }

        // `ServerMetrics` renders the same series directly in Prometheus text format.
        let prom = server_metrics.render_prometheus();
        assert!(prom.contains("ethonyx_connection_limit_saturated_total 1"));
        assert!(prom.contains(
            r#"ethonyx_proxy_requests_total{upstream="CL",method="OPTIONS",status="206"} 2"#
        ));
        assert!(prom.contains(
            r#"ethonyx_response_body_errors_total{upstream="EL",reason="idle_timeout"} 1"#
        ));
        assert!(prom.contains(
            r#"ethonyx_response_body_errors_total{upstream="CL",reason="upstream_error"} 1"#
        ));
    }
}
