use anyhow::Result;
use eventide_config::ServiceRuntimeConfig;
use eventide_coordinator_app::{CoordinatorService, CoordinatorSettings};
use eventide_postgres_store::PostgresMetadataRepository;
use eventide_runtime::{init_tracing, wait_for_shutdown};
use eventide_types::{ExpireWorkerLeasesRequest, ListWorkersRequest, ServiceName};
use std::sync::Arc;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    let config = ServiceRuntimeConfig::from_env(ServiceName::Coordinator);
    init_tracing(&config.log_filter);

    let repository = Arc::new(PostgresMetadataRepository::connect(&config.database_url).await?);
    repository.migrate().await?;

    let service = CoordinatorService::new(
        repository,
        CoordinatorSettings {
            worker_lease_ttl_secs: config.worker_lease_ttl_secs,
            worker_stale_after_secs: config.worker_stale_after_secs,
        },
    );

    let expiry = service
        .expire_worker_leases(ExpireWorkerLeasesRequest {
            stale_after_secs: config.worker_stale_after_secs,
        })
        .await?;
    let workers = service
        .list_workers(ListWorkersRequest::default())
        .await?
        .workers
        .len();

    info!(
        service = ServiceName::Coordinator.as_str(),
        bind_addr = config.bind_addr,
        database_url = config.database_url,
        metrics_addr = config.metrics_addr,
        worker_lease_ttl_secs = config.worker_lease_ttl_secs,
        worker_stale_after_secs = config.worker_stale_after_secs,
        worker_count = workers,
        expired_worker_count = expiry.expired_worker_ids.len(),
        revoked_assignment_count = expiry.revoked_assignment_count,
        "coordinator lease service initialized"
    );

    wait_for_shutdown(ServiceName::Coordinator.as_str()).await
}
