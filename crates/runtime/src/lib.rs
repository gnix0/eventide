use anyhow::Result;
use eventide_config::ServiceRuntimeConfig;
use tracing::info;
use tracing_subscriber::EnvFilter;

pub async fn run_service(config: ServiceRuntimeConfig) -> Result<()> {
    init_tracing(&config.log_filter);

    info!(
        service = config.service_name.as_str(),
        bind_addr = config.bind_addr,
        database_url = config.database_url,
        metrics_addr = config.metrics_addr,
        "service bootstrap complete; runtime endpoints are not wired yet"
    );

    wait_for_shutdown(config.service_name.as_str()).await
}

pub fn init_tracing(log_filter: &str) {
    let filter = EnvFilter::try_new(log_filter).unwrap_or_else(|_| EnvFilter::new("info"));

    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .try_init();
}

pub async fn wait_for_shutdown(service_name: &str) -> Result<()> {
    tokio::signal::ctrl_c().await?;
    info!(service = service_name, "shutdown signal received");
    Ok(())
}
