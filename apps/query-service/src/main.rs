use anyhow::Result;
use eventide_config::ServiceRuntimeConfig;
use eventide_postgres_store::PostgresMetadataRepository;
use eventide_query_app::{QueryService, QuerySettings};
use eventide_runtime::{init_tracing, wait_for_shutdown};
use eventide_types::ServiceName;
use std::sync::Arc;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    let config = ServiceRuntimeConfig::from_env(ServiceName::QueryService);
    init_tracing(&config.log_filter);

    let repository = Arc::new(PostgresMetadataRepository::connect(&config.database_url).await?);
    repository.migrate().await?;

    let _service = QueryService::new(
        repository,
        QuerySettings {
            worker_stale_after_secs: config.worker_stale_after_secs,
        },
    );

    info!(
        service = ServiceName::QueryService.as_str(),
        bind_addr = config.bind_addr,
        database_url = config.database_url,
        metrics_addr = config.metrics_addr,
        worker_stale_after_secs = config.worker_stale_after_secs,
        "query service initialized"
    );

    wait_for_shutdown(ServiceName::QueryService.as_str()).await
}
