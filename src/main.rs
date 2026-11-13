use clap::Parser;
use ethonyx::{BoxError, Config};

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let app = ethonyx::Prepared::new(Config::parse()).await?;
    let cfg = app.config();

    use tracing_subscriber::prelude::*;

    let fmt_filter = tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        let access = if cfg.access_log { "info" } else { "off" };
        tracing_subscriber::EnvFilter::new(format!("{},access_log={access}", cfg.log_level))
    });
    let fmt_layer = tracing_subscriber::fmt::layer()
        .json()
        .with_filter(fmt_filter);

    #[cfg(feature = "otel")]
    {
        let otel_layer = app.tracer().map(|tracer| {
            // Keep OTel request spans active when `--log-level warn` quiets stdout,
            // while still honoring `RUST_LOG` if explicitly provided.
            let otel_filter =
                tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                    tracing_subscriber::EnvFilter::new("ethonyx=info,access_log=off")
                });
            tracing_opentelemetry::layer()
                .with_tracer(tracer)
                .with_filter(otel_filter)
        });
        tracing_subscriber::registry()
            .with(otel_layer)
            .with(fmt_layer)
            .init();
    }
    #[cfg(not(feature = "otel"))]
    {
        tracing_subscriber::registry().with(fmt_layer).init();
    }

    let shutdown = async {
        #[cfg(unix)]
        {
            let mut terminate =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("install SIGTERM handler");
            tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = terminate.recv() => {} }
        }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
        }
    };
    app.run(shutdown).await
}
