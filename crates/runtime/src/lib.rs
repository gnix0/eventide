use anyhow::Result;
use event_pipeline_config::ServiceRuntimeConfig;
use tracing::info;
use tracing_subscriber::EnvFilter;

pub async fn run_service(config: ServiceRuntimeConfig) -> Result<()> {
    init_tracing(&config.log_filter);

    info!(
        service = config.service_name.as_str(),
        bind_addr = config.bind_addr,
        metrics_addr = config.metrics_addr,
        "service bootstrap complete; runtime endpoints are not wired yet"
    );

    tokio::signal::ctrl_c().await?;

    info!(
        service = config.service_name.as_str(),
        "shutdown signal received"
    );
    Ok(())
}

fn init_tracing(log_filter: &str) {
    let filter = EnvFilter::try_new(log_filter).unwrap_or_else(|_| EnvFilter::new("info"));

    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .try_init();
}
