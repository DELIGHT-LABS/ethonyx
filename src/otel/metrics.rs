//! Typed OpenTelemetry metric instruments mirroring `crate::metrics::ServerMetrics`.

use std::sync::OnceLock;

use opentelemetry::KeyValue;

use crate::metrics::{
    ACTIVE_CONNECTIONS, CONNECTION_LIMIT_SATURATED, ConnProtocol, HealthInstrument,
    LATENCY_BUCKETS, PROXY_REQUEST_DURATION, PROXY_REQUESTS_TOTAL, UpstreamLayer,
};

/// Detached active-connection up-down counter handle carried inside
/// `ActiveGauge` / `ActiveConnectionGuard` without retaining `Arc<AppState>`.
pub(crate) struct OtelActiveGuard(
    Option<(opentelemetry::metrics::UpDownCounter<i64>, ConnProtocol)>,
);

impl OtelActiveGuard {
    pub(crate) fn add(&self, delta: i64) {
        if let Some((counter, protocol)) = &self.0 {
            counter.add(delta, &[KeyValue::new("protocol", protocol.as_str())]);
        }
    }
}

struct OtelMetricsInner {
    meter: opentelemetry::metrics::Meter,
    active_connections: opentelemetry::metrics::UpDownCounter<i64>,
    // Kept lazy: `opentelemetry_sdk` pre-allocates ~135 KiB per built instrument,
    // so unused saturation/health gauges (e.g. EL-only or `--trust-upstream`) stay zero-cost.
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

    pub(super) fn from_meter(meter: opentelemetry::metrics::Meter) -> Self {
        let active_connections = meter.i64_up_down_counter(ACTIVE_CONNECTIONS).build();
        let duration = meter
            .f64_histogram(PROXY_REQUEST_DURATION)
            .with_unit("s")
            .with_boundaries(LATENCY_BUCKETS.to_vec())
            .build();
        let requests = meter.u64_counter(PROXY_REQUESTS_TOTAL).build();
        Self(Some(Box::new(OtelMetricsInner {
            meter,
            active_connections,
            saturated: OnceLock::new(),
            duration,
            requests,
            health: [const { OnceLock::new() }; HealthInstrument::COUNT],
        })))
    }

    pub(crate) fn active_guard(&self, protocol: ConnProtocol) -> OtelActiveGuard {
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
                        let pts = hist
                            .data_points()
                            .map(|p| {
                                let bounds: Vec<f64> = p.bounds().collect();
                                assert_eq!(
                                    bounds, LATENCY_BUCKETS,
                                    "OTel histogram boundaries must match Prometheus LATENCY_BUCKETS"
                                );
                                (p.attributes().cloned().collect(), p.count() as f64)
                            })
                            .collect();
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
            // Register CL peers first while leaving EL peers unset: OTel must NOT emit
            // a synthetic `layer="el"` datapoint until EL peers is explicitly recorded.
            server_metrics.set_health(G::ClPeers, 12.0);
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

            server_metrics.set_health(G::ElPeers, 0.0);
        }

        // Dropping `tcp_guard` exercises the retained `OtelActiveGuard` handle.
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
            ],
        );
        assert_metric(
            "ethonyx_proxy_request_duration_seconds",
            "histogram",
            &[(&[("upstream", "EL")], 4.0), (&[("upstream", "CL")], 2.0)],
        );
        let saturated = assert_metric(
            "ethonyx_connection_limit_saturated_total",
            "u64_monotonic_sum",
            &[(&[], 1.0)],
        );
        assert_eq!(saturated.2.len(), 1);

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

        // `ServerMetrics` mirrors the same series to the primary Prometheus handle.
        let prom = server_metrics.render_prometheus();
        assert!(prom.contains("ethonyx_connection_limit_saturated_total 1"));
        assert!(prom.contains(
            r#"ethonyx_proxy_requests_total{upstream="CL",method="OPTIONS",status="206"} 2"#
        ));
    }
}
