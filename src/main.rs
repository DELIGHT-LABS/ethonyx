#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used))]

use clap::Parser;
use ethonyx::{BoxError, Config};
use tracing_subscriber::EnvFilter;
#[cfg(feature = "otel")]
use tracing_subscriber::filter::FilterExt as _;
use tracing_subscriber::prelude::*;

fn parse_env_filter(spec: &str) -> Result<EnvFilter, BoxError> {
    EnvFilter::try_new(spec).map_err(|e| format!("invalid RUST_LOG: {e}").into())
}

/// Cap third-party `opentelemetry-otlp` exporter logs at `INFO` and drop
/// `*.PartialSuccess` warnings (which repeat raw collector `error_message`
/// payloads verbatim up to 4 MiB) under both `--log-level` and `RUST_LOG` so
/// raw OTLP URLs and collector error bodies are never emitted to stdout logs
/// or OTel span events.
#[cfg(feature = "otel")]
fn allow_otlp_event(meta: &tracing::Metadata<'_>) -> bool {
    let target = meta.target();
    if !(target.starts_with("opentelemetry-otlp") || target.starts_with("opentelemetry_otlp")) {
        return true;
    }
    !matches!(*meta.level(), tracing::Level::DEBUG | tracing::Level::TRACE)
        && !meta.name().ends_with(".PartialSuccess")
}

#[cfg(feature = "otel")]
fn cap_otlp_logs<S>(
    filter: EnvFilter,
) -> impl tracing_subscriber::layer::Filter<S> + Send + Sync + 'static
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    // Preserve `EnvFilter::max_level_hint()` across `And::max_level_hint()` (`cmp::min`):
    // `FilterFn::new` defaults to `None` (`None < Some(_)`), which would otherwise collapse
    // the global `log::max_level()` to `Trace` and force per-frame `tungstenite`/`rustls`
    // `log::trace!` evaluation even at `--log-level info` or `warn`.
    filter.and(
        tracing_subscriber::filter::filter_fn(allow_otlp_event)
            .with_max_level_hint(tracing::level_filters::LevelFilter::TRACE),
    )
}

fn build_fmt_filter<S>(
    rust_log: Option<&str>,
    log_level: &str,
    access_log: bool,
) -> Result<impl tracing_subscriber::layer::Filter<S> + Send + Sync + 'static, BoxError>
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    let filter = match rust_log {
        Some(spec) => parse_env_filter(spec)?,
        None => {
            let access = if access_log { "info" } else { "off" };
            EnvFilter::new(format!("{log_level},access_log={access}"))
        }
    };
    #[cfg(feature = "otel")]
    let filter = cap_otlp_logs(filter);
    Ok(filter)
}

#[cfg(feature = "otel")]
fn build_otel_filter<S>(
    rust_log: Option<&str>,
) -> Result<impl tracing_subscriber::layer::Filter<S> + Send + Sync + 'static, BoxError>
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    // Keep OTel request spans active when `--log-level warn` quiets stdout,
    // while still honoring `RUST_LOG` if explicitly provided.
    let filter = match rust_log {
        Some(spec) => parse_env_filter(spec)?,
        None => EnvFilter::new("ethonyx=info,access_log=off"),
    };
    Ok(cap_otlp_logs(filter))
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let cfg = Config::parse();
    #[cfg(unix)]
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;

    // Validate `RUST_LOG` before starting OTLP providers so a bad directive fails
    // immediately without spawning background telemetry cleanup threads.
    let rust_log = match std::env::var(EnvFilter::DEFAULT_ENV) {
        Ok(val) => Some(val),
        Err(std::env::VarError::NotPresent) => None,
        Err(e) => return Err(format!("invalid RUST_LOG: {e}").into()),
    };
    let fmt_filter = build_fmt_filter(rust_log.as_deref(), &cfg.log_level, cfg.access_log)?;
    let fmt_layer = tracing_subscriber::fmt::layer()
        .json()
        .with_filter(fmt_filter);

    let app = ethonyx::Prepared::new(cfg).await?;

    #[cfg(feature = "otel")]
    let otel_layer = app
        .tracer()
        .map(|tracer| -> Result<_, BoxError> {
            let otel_filter = build_otel_filter(rust_log.as_deref())?;
            Ok(tracing_opentelemetry::layer()
                .with_tracer(tracer)
                .with_filter(otel_filter))
        })
        .transpose()?;

    let registry = tracing_subscriber::registry();
    #[cfg(feature = "otel")]
    let registry = registry.with(otel_layer);
    registry.with(fmt_layer).init();

    let shutdown = async move {
        let ctrl_c = async {
            if let Err(error) = tokio::signal::ctrl_c().await {
                tracing::warn!(%error, "failed to install ctrl-c handler");
                std::future::pending::<()>().await;
            }
        };
        #[cfg(unix)]
        tokio::select! {
            () = ctrl_c => {},
            _ = terminate.recv() => {},
        }
        #[cfg(not(unix))]
        ctrl_c.await;
    };
    app.run(shutdown).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fmt_filter_rejects_invalid_rust_log_and_preserves_max_level_hint() {
        use tracing::level_filters::LevelFilter;

        let err =
            build_fmt_filter::<tracing_subscriber::Registry>(Some("ethonyx=[bad"), "info", false)
                .err()
                .expect("invalid RUST_LOG must fail")
                .to_string();
        assert!(err.contains("invalid RUST_LOG"), "got: {err}");

        for (rust_log, log_level, expected) in [
            (None, "info", Some(LevelFilter::INFO)),
            (None, "warn", Some(LevelFilter::WARN)),
            (Some("warn"), "info", Some(LevelFilter::WARN)),
            (Some("debug"), "info", Some(LevelFilter::DEBUG)),
        ] {
            let filter = build_fmt_filter(rust_log, log_level, false).expect("valid filter");
            let subscriber = tracing_subscriber::registry()
                .with(tracing_subscriber::fmt::layer().with_filter(filter));
            assert_eq!(
                tracing::Subscriber::max_level_hint(&subscriber),
                expected,
                "max_level_hint mismatch for ({rust_log:?}, {log_level})"
            );
        }
    }

    #[cfg(feature = "otel")]
    #[test]
    fn otlp_exporter_debug_and_trace_logs_stay_capped_under_rust_log_and_log_level() {
        use std::sync::{Arc, Mutex};
        use tracing::level_filters::LevelFilter;
        use tracing_subscriber::Layer;

        #[derive(Clone, Copy, Debug)]
        struct CapturedEvent {
            target: &'static str,
            name: &'static str,
            level: tracing::Level,
        }

        #[derive(Clone, Default)]
        struct Capture(Arc<Mutex<Vec<CapturedEvent>>>);

        impl<S: tracing::Subscriber> Layer<S> for Capture {
            fn on_event(
                &self,
                event: &tracing::Event<'_>,
                _ctx: tracing_subscriber::layer::Context<'_, S>,
            ) {
                let meta = event.metadata();
                self.0.lock().unwrap().push(CapturedEvent {
                    target: meta.target(),
                    name: meta.name(),
                    level: *meta.level(),
                });
            }
        }

        let record = |rust_log: Option<&str>,
                      log_level: &str,
                      expected_hint: Option<LevelFilter>| {
            let fmt_capture = Capture::default();
            let otel_capture = Capture::default();
            let fmt_filter = build_fmt_filter(rust_log, log_level, false).expect("valid filter");
            let otel_filter = build_otel_filter(rust_log).expect("valid otel filter");
            let subscriber = tracing_subscriber::registry()
                .with(otel_capture.clone().with_filter(otel_filter))
                .with(fmt_capture.clone().with_filter(fmt_filter));
            assert_eq!(
                tracing::Subscriber::max_level_hint(&subscriber),
                expected_hint,
                "max_level_hint mismatch for ({rust_log:?}, {log_level})"
            );
            tracing::subscriber::with_default(subscriber, || {
                tracing::debug!(target: "ethonyx", "app debug");
                tracing::trace!(target: "opentelemetry-otlp", "raw otlp trace");
                tracing::debug!(target: "opentelemetry-otlp", "raw otlp debug");
                tracing::debug!(target: "opentelemetry_otlp::exporter", "raw otlp mod debug");
                tracing::info!(target: "opentelemetry-otlp", "otlp info");
                tracing::warn!(target: "opentelemetry-otlp", "otlp warn");
                tracing::warn!(
                    name: "HttpMetricsClient.PartialSuccess",
                    target: "opentelemetry-otlp",
                    error_message = "raw collector metrics error"
                );
                tracing::warn!(
                    name: "HttpTraceClient.PartialSuccess",
                    target: "opentelemetry-otlp",
                    error_message = "raw collector trace error"
                );
            });
            (
                fmt_capture.0.lock().unwrap().clone(),
                otel_capture.0.lock().unwrap().clone(),
            )
        };

        for (rust_log, log_level, hint) in [
            (None, "trace", Some(LevelFilter::TRACE)),
            (Some("debug"), "info", Some(LevelFilter::DEBUG)),
            (Some("trace"), "info", Some(LevelFilter::TRACE)),
            (
                Some("ethonyx=debug,opentelemetry-otlp=trace,opentelemetry_otlp=debug"),
                "info",
                Some(LevelFilter::TRACE),
            ),
        ] {
            let (fmt_events, otel_events) = record(rust_log, log_level, hint);
            assert!(
                fmt_events
                    .iter()
                    .any(|e| e.target == "ethonyx" && e.level == tracing::Level::DEBUG),
                "app debug must pass for ({rust_log:?}, {log_level}): {fmt_events:?}"
            );
            assert!(
                fmt_events
                    .iter()
                    .any(|e| e.target == "opentelemetry-otlp" && e.level == tracing::Level::INFO),
                "otlp info must pass for ({rust_log:?}, {log_level}): {fmt_events:?}"
            );
            assert!(
                fmt_events.iter().any(|e| {
                    e.target == "opentelemetry-otlp"
                        && !e.name.ends_with(".PartialSuccess")
                        && e.level == tracing::Level::WARN
                }),
                "non-PartialSuccess otlp warn must pass for ({rust_log:?}, {log_level}): {fmt_events:?}"
            );
            for (label, events) in [("fmt", &fmt_events), ("otel", &otel_events)] {
                assert!(
                    !events.iter().any(|e| {
                        (e.target.starts_with("opentelemetry-otlp")
                            || e.target.starts_with("opentelemetry_otlp"))
                            && (e.name.ends_with(".PartialSuccess")
                                || matches!(e.level, tracing::Level::DEBUG | tracing::Level::TRACE))
                    }),
                    "{label} otlp debug/trace/PartialSuccess must stay blocked for ({rust_log:?}, {log_level}): {events:?}"
                );
            }
        }

        // Quieter levels (`warn`) must never be raised for `opentelemetry-otlp` or global max level,
        // and `*.PartialSuccess` stays suppressed while general `warn` events still pass.
        let (fmt_warn, otel_warn) = record(Some("warn"), "info", Some(LevelFilter::WARN));
        let (fmt_default_warn, _) = record(None, "warn", Some(LevelFilter::INFO));
        for (label, events) in [
            ("fmt_warn", &fmt_warn),
            ("otel_warn", &otel_warn),
            ("fmt_default_warn", &fmt_default_warn),
        ] {
            assert_eq!(
                events.len(),
                1,
                "{label} must retain only the non-PartialSuccess otlp warn: {events:?}"
            );
            assert_eq!(events[0].target, "opentelemetry-otlp");
            assert!(!events[0].name.ends_with(".PartialSuccess"));
            assert_eq!(events[0].level, tracing::Level::WARN);
        }
    }
}
