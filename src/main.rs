use clap::Parser;
use ethonyx::{BoxError, Config};

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let cfg = Config::parse();
    #[cfg(unix)]
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;

    use tracing_subscriber::prelude::*;

    // Validate `RUST_LOG` before starting OTLP providers so a bad directive fails
    // immediately without spawning background telemetry cleanup threads.
    let rust_log = match std::env::var(tracing_subscriber::EnvFilter::DEFAULT_ENV) {
        Ok(val) => Some(val),
        Err(std::env::VarError::NotPresent) => None,
        Err(e) => return Err(format!("invalid RUST_LOG: {e}").into()),
    };
    let fmt_filter = match &rust_log {
        Some(spec) => tracing_subscriber::EnvFilter::try_new(spec)
            .map_err(|e| format!("invalid RUST_LOG: {e}"))?,
        None => {
            let access = if cfg.access_log { "info" } else { "off" };
            tracing_subscriber::EnvFilter::new(format!("{},access_log={access}", cfg.log_level))
        }
    };
    let fmt_layer = tracing_subscriber::fmt::layer()
        .json()
        .with_filter(fmt_filter);

    let app = ethonyx::Prepared::new(cfg).await?;

    #[cfg(feature = "otel")]
    let otel_layer = app.tracer().map(|tracer| {
        // Keep OTel request spans active when `--log-level warn` quiets stdout,
        // while still honoring `RUST_LOG` if explicitly provided.
        let otel_filter = match &rust_log {
            Some(spec) => {
                tracing_subscriber::EnvFilter::try_new(spec).expect("RUST_LOG validated above")
            }
            None => tracing_subscriber::EnvFilter::new("ethonyx=info,access_log=off"),
        };
        tracing_opentelemetry::layer()
            .with_tracer(tracer)
            .with_filter(otel_filter)
    });

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
