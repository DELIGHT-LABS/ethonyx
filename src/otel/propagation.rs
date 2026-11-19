//! W3C Trace Context extraction, injection, and span error marking.

use std::borrow::Cow;

use opentelemetry::propagation::TextMapPropagator as _;
use opentelemetry::trace::TraceContextExt as _;
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

use crate::headers::{TRACEPARENT, TRACESTATE};

struct HeaderMapCarrier<'a>(&'a mut http::HeaderMap);

impl opentelemetry::propagation::Injector for HeaderMapCarrier<'_> {
    fn set(&mut self, key: &str, value: String) {
        let name = if key.eq_ignore_ascii_case(TRACEPARENT.as_str()) {
            TRACEPARENT
        } else if key.eq_ignore_ascii_case(TRACESTATE.as_str()) {
            TRACESTATE
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

/// Pre-validated W3C Trace Context carrier extracted from inbound HTTP headers.
///
/// Validates `traceparent` before inspecting `tracestate` so invalid, duplicate,
/// or `Connection`-nominated headers return immediately without allocating.
struct TraceCarrier<'a> {
    traceparent: &'a str,
    tracestate: Option<Cow<'a, str>>,
}

impl<'a> TraceCarrier<'a> {
    fn from_headers(headers: &'a http::HeaderMap) -> Option<Self> {
        // W3C Trace Context §3.2: duplicate `traceparent` headers are invalid and
        // must be ignored so a fresh root trace starts.
        let mut tp_iter = headers.get_all(TRACEPARENT).iter();
        let first = tp_iter.next()?;
        if tp_iter.next().is_some() {
            return None;
        }
        let traceparent = first.to_str().ok()?;
        if crate::headers::header_has_token(headers, http::header::CONNECTION, TRACEPARENT.as_str())
        {
            return None;
        }
        let tracestate = extract_tracestate(headers);
        Some(Self {
            traceparent,
            tracestate,
        })
    }
}

fn extract_tracestate(headers: &http::HeaderMap) -> Option<Cow<'_, str>> {
    let mut iter = headers.get_all(TRACESTATE).iter();
    let first = iter.next()?;
    if crate::headers::header_has_token(headers, http::header::CONNECTION, TRACESTATE.as_str()) {
        return None;
    }
    // W3C Trace Context §3.3.1.1: multiple `tracestate` header fields may be
    // combined with `,` in order. Single surviving non-empty values borrow zero-copy.
    let mut non_empty = std::iter::once(first)
        .chain(iter)
        .filter_map(|v| v.to_str().ok().map(str::trim).filter(|s| !s.is_empty()));
    let head = non_empty.next()?;
    let Some(next) = non_empty.next() else {
        return Some(Cow::Borrowed(head));
    };
    let mut joined = String::with_capacity(head.len() + 1 + next.len());
    joined.push_str(head);
    joined.push(',');
    joined.push_str(next);
    for part in non_empty {
        joined.push(',');
        joined.push_str(part);
    }
    Some(Cow::Owned(joined))
}

impl opentelemetry::propagation::Extractor for TraceCarrier<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        if key.eq_ignore_ascii_case(TRACEPARENT.as_str()) {
            Some(self.traceparent)
        } else if key.eq_ignore_ascii_case(TRACESTATE.as_str()) {
            self.tracestate.as_deref()
        } else {
            None
        }
    }

    fn keys(&self) -> Vec<&str> {
        if self.tracestate.is_some() {
            vec!["traceparent", "tracestate"]
        } else {
            vec!["traceparent"]
        }
    }
}

/// Link `span` (constructed with `parent: None` so ambient thread-local tracing/OTel
/// contexts are never inherited) to any non-`Connection`-nominated parent trace
/// context in `headers`.
///
/// Must be called BEFORE `span` is first entered so `tracing-opentelemetry` builds
/// the underlying OTel `Span` with the extracted parent `TraceId`.
pub(crate) fn attach_parent_context(span: &tracing::Span, headers: &http::HeaderMap) {
    if span.is_disabled() {
        return;
    }
    let Some(carrier) = TraceCarrier::from_headers(headers) else {
        return;
    };
    let parent_cx = opentelemetry_sdk::propagation::TraceContextPropagator::new()
        .extract_with_context(&opentelemetry::Context::new(), &carrier);
    if parent_cx.has_active_span() {
        let _ = span.set_parent(parent_cx);
    }
}

pub(crate) fn propagate_context(headers: &mut http::HeaderMap) {
    let span = tracing::Span::current();
    if span.is_disabled() {
        // When no tracing subscriber/span is active, any non-Connection-nominated
        // incoming `traceparent` was already preserved intact by `strip_hop_by_hop`.
        return;
    }
    let cx = span.context();
    if !cx.has_active_span() {
        // No OTel layer attached to the active subscriber (e.g. fmt-only debug logs):
        // leave any non-Connection-nominated incoming trace headers untouched.
        return;
    }
    opentelemetry_sdk::propagation::TraceContextPropagator::new()
        .inject_context(&cx, &mut HeaderMapCarrier(headers));
}

/// Mark an active OTel request span as errored (used for HTTP 5xx responses).
pub(crate) fn mark_span_error(span: &tracing::Span) {
    span.set_status(opentelemetry::trace::Status::error("server error"));
}

#[cfg(test)]
#[derive(Clone, Debug, Default)]
pub(super) struct CapturingSpanExporter(
    pub(super) std::sync::Arc<std::sync::Mutex<Vec<opentelemetry_sdk::trace::SpanData>>>,
);

#[cfg(test)]
impl CapturingSpanExporter {
    pub(super) fn has_span(&self, name: &str) -> bool {
        self.0.lock().unwrap().iter().any(|s| s.name == name)
    }
}

#[cfg(test)]
impl opentelemetry_sdk::trace::SpanExporter for CapturingSpanExporter {
    async fn export(
        &self,
        batch: Vec<opentelemetry_sdk::trace::SpanData>,
    ) -> opentelemetry_sdk::error::OTelSdkResult {
        self.0.lock().unwrap().extend(batch);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CapturingSpanExporter, TraceCarrier, attach_parent_context, mark_span_error,
        propagate_context,
    };
    use crate::otel::SERVICE_NAME;
    use opentelemetry::Value;
    use opentelemetry::propagation::{Extractor as _, TextMapPropagator as _};
    use opentelemetry::trace::{Status, TraceContextExt as _, TracerProvider as _};
    use tracing_subscriber::prelude::*;

    fn with_capturing_tracer<F: FnOnce()>(f: F) -> Vec<opentelemetry_sdk::trace::SpanData> {
        let exporter = CapturingSpanExporter::default();
        let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let subscriber = tracing_subscriber::registry()
            .with(tracing_opentelemetry::layer().with_tracer(provider.tracer(SERVICE_NAME)));
        tracing::subscriber::with_default(subscriber, || {
            tracing::callsite::rebuild_interest_cache();
            f();
        });
        let _ = provider.shutdown();
        std::mem::take(&mut *exporter.0.lock().unwrap())
    }

    const PARENT_TP: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    const PARENT_PREFIX: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-";

    fn headers_from(pairs: &[(&str, &str)]) -> http::HeaderMap {
        let mut map = http::HeaderMap::new();
        for &(k, v) in pairs {
            map.append(
                http::header::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                v.parse().unwrap(),
            );
        }
        map
    }

    fn run_propagation(
        inbound: &http::HeaderMap,
        seed_outbound: &[(&str, &str)],
    ) -> http::HeaderMap {
        let mut outbound = headers_from(seed_outbound);
        let span = tracing::info_span!(parent: None, "request");
        attach_parent_context(&span, inbound);
        {
            let _enter = span.enter();
            propagate_context(&mut outbound);
        }
        outbound
    }

    #[test]
    fn valid_traceparent_and_multiline_tracestate_link_parent_and_mark_5xx_error() {
        let mut outbound = http::HeaderMap::new();
        let spans = with_capturing_tracer(|| {
            let inbound = headers_from(&[
                ("traceparent", PARENT_TP),
                ("tracestate", "vendor=ethonyx"),
                ("tracestate", "other=val"),
            ]);
            let span = tracing::info_span!(
                parent: None,
                "request",
                http.response.status_code = tracing::field::Empty
            );
            attach_parent_context(&span, &inbound);
            {
                let _enter = span.enter();
                propagate_context(&mut outbound);
            }
            span.record("http.response.status_code", 502_i64);
            mark_span_error(&span);
        });

        let tp = outbound["traceparent"].to_str().unwrap();
        assert!(
            tp.starts_with(PARENT_PREFIX),
            "must preserve parent trace-id, got: {tp}"
        );
        assert!(
            !tp.contains("-00f067aa0ba902b7-"),
            "must rewrite span-id to the new child span, got: {tp}"
        );
        assert_eq!(outbound["tracestate"], "vendor=ethonyx,other=val");

        let first = &spans[0];
        assert!(
            matches!(first.status, Status::Error { .. }),
            "5xx response must set OTel Error status, got: {:?}",
            first.status
        );
        assert!(
            first
                .attributes
                .iter()
                .any(|kv| kv.key.as_str() == "http.response.status_code"
                    && kv.value == Value::I64(502)),
            "expected http.response.status_code=502 in {:?}",
            first.attributes
        );
    }

    #[test]
    fn whitespace_only_multiline_tracestate_is_ignored_without_allocating() {
        let mut headers = headers_from(&[
            ("traceparent", PARENT_TP),
            ("tracestate", "   "),
            ("tracestate", "\t "),
        ]);
        let carrier = TraceCarrier::from_headers(&headers).expect("valid traceparent");
        assert_eq!(carrier.get("tracestate"), None);
        assert_eq!(carrier.keys(), ["traceparent"]);

        // A single non-empty line among blank lines borrows without allocating.
        headers.append("tracestate", "  vendor=ethonyx  ".parse().unwrap());
        let carrier = TraceCarrier::from_headers(&headers).expect("valid traceparent");
        assert_eq!(carrier.get("tracestate"), Some("vendor=ethonyx"));
        assert!(matches!(
            carrier.tracestate,
            Some(std::borrow::Cow::Borrowed(_))
        ));
        assert_eq!(carrier.keys(), ["traceparent", "tracestate"]);
    }

    #[test]
    fn duplicate_and_connection_nominated_traceparent_start_fresh_root_and_strip_orphan_tracestate()
    {
        with_capturing_tracer(|| {
            // Duplicate incoming `traceparent` headers are invalid per W3C Trace Context §3.2.
            let dup_tp = headers_from(&[
                ("traceparent", PARENT_TP),
                (
                    "traceparent",
                    "00-11111111111111111111111111111111-2222222222222222-01",
                ),
            ]);
            assert!(TraceCarrier::from_headers(&dup_tp).is_none());
            let dup_out = run_propagation(&dup_tp, &[]);
            let dup_out_tp = dup_out["traceparent"].to_str().unwrap();
            assert!(
                !dup_out_tp.starts_with(PARENT_PREFIX)
                    && !dup_out_tp.starts_with("00-11111111111111111111111111111111-"),
                "duplicate traceparent must start a fresh root trace, got: {dup_out_tp}"
            );

            // Connection-nominated `traceparent` is rejected on extraction and strips
            // any orphaned incoming `tracestate` when emitting a fresh root trace.
            let nominated = headers_from(&[
                ("traceparent", PARENT_TP),
                ("connection", "keep-alive, traceparent"),
            ]);
            assert!(TraceCarrier::from_headers(&nominated).is_none());
            let outbound_root = run_propagation(&nominated, &[("tracestate", "stale=orphan")]);
            let root_tp = outbound_root["traceparent"].to_str().unwrap();
            assert!(
                root_tp.starts_with("00-") && !root_tp.starts_with(PARENT_PREFIX),
                "Connection-nominated traceparent must not link parent trace-id, got: {root_tp}"
            );
            assert!(
                !outbound_root.contains_key("tracestate"),
                "fresh root span must omit empty tracestate and strip orphaned tracestate"
            );

            // Connection-nominated `tracestate` keeps the valid parent `traceparent`
            // linked while suppressing inbound `tracestate` propagation.
            let nom_ts = headers_from(&[
                ("traceparent", PARENT_TP),
                ("tracestate", "vendor=secret"),
                ("connection", "keep-alive, tracestate"),
            ]);
            let carrier = TraceCarrier::from_headers(&nom_ts).expect("valid traceparent");
            assert_eq!(carrier.get("tracestate"), None);
            let outbound_ts = run_propagation(&nom_ts, &[("tracestate", "stale=drop")]);
            let ts_tp = outbound_ts["traceparent"].to_str().unwrap();
            assert!(
                ts_tp.starts_with(PARENT_PREFIX),
                "valid traceparent must remain linked when only tracestate is nominated, got: {ts_tp}"
            );
            assert!(
                !outbound_ts.contains_key("tracestate"),
                "Connection-nominated tracestate must not be re-emitted"
            );
        });
    }

    #[test]
    fn ambient_thread_local_otel_context_does_not_bleed_into_isolated_requests() {
        with_capturing_tracer(|| {
            let ambient_headers = headers_from(&[(
                "traceparent",
                "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01",
            )]);
            let ambient_cx = opentelemetry_sdk::propagation::TraceContextPropagator::new()
                .extract(&TraceCarrier::from_headers(&ambient_headers).unwrap());
            assert!(ambient_cx.has_active_span());
            let _ambient_guard = ambient_cx.attach();

            let nominated =
                headers_from(&[("traceparent", PARENT_TP), ("connection", "traceparent")]);
            let empty_headers = http::HeaderMap::new();

            for headers in [&nominated, &empty_headers] {
                let outbound = run_propagation(headers, &[]);
                let isolated_tp = outbound["traceparent"].to_str().unwrap();
                assert!(
                    !isolated_tp.starts_with("00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-"),
                    "ambient OTel context must not leak into isolated request, got: {isolated_tp}"
                );
                assert!(
                    !outbound.contains_key("tracestate"),
                    "fresh isolated span must not inject an empty tracestate header"
                );
            }
        });
    }
}
