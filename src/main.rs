use clap::Parser;
use ethonyx::{BoxError, Config};

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let cfg = Config::parse();
    cfg.validate()?;

    #[cfg(feature = "otel")]
    let telemetry = ethonyx::otel::start(cfg.otel_endpoint.as_deref()).await?;

    use tracing_subscriber::prelude::*;

    let filter = tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        let access = if cfg.access_log { "info" } else { "off" };
        tracing_subscriber::EnvFilter::new(format!("{},access_log={access}", cfg.log_level))
    });

    #[cfg(feature = "otel")]
    {
        if let Some(ref telemetry) = telemetry {
            let tracer = telemetry.tracer();
            let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);
            tracing_subscriber::registry()
                .with(filter)
                .with(otel_layer)
                .with(tracing_subscriber::fmt::layer().json())
                .init();
        } else {
            tracing_subscriber::registry()
                .with(filter)
                .with(tracing_subscriber::fmt::layer().json())
                .init();
        }
    }
    #[cfg(not(feature = "otel"))]
    {
        tracing_subscriber::registry()
            .with(filter)
            .with(tracing_subscriber::fmt::layer().json())
            .init();
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
    #[cfg(feature = "otel")]
    ethonyx::run_with_telemetry(cfg, shutdown, telemetry).await?;
    #[cfg(not(feature = "otel"))]
    ethonyx::run(cfg, shutdown).await?;
    Ok(())
}
