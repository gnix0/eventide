use anyhow::Result;
use eventide_config::ServiceRuntimeConfig;
use eventide_postgres_store::PostgresMetadataRepository;
use eventide_processor_app::{ProcessorService, ProcessorSettings};
use eventide_runtime::init_tracing;
use eventide_types::ServiceName;
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

#[tokio::main]
async fn main() -> Result<()> {
    let config = ServiceRuntimeConfig::from_env(ServiceName::ProcessorRuntime);
    init_tracing(&config.log_filter);

    let repository = Arc::new(PostgresMetadataRepository::connect(&config.database_url).await?);
    repository.migrate().await?;

    let service = ProcessorService::new(
        repository.clone(),
        ProcessorSettings {
            worker_id: config.processor_worker_id.clone(),
            batch_size: config.processor_batch_size,
        },
    );

    info!(
        service = ServiceName::ProcessorRuntime.as_str(),
        bind_addr = config.bind_addr,
        database_url = config.database_url,
        metrics_addr = config.metrics_addr,
        worker_id = config.processor_worker_id,
        poll_interval_ms = config.processor_poll_interval_ms,
        batch_size = config.processor_batch_size,
        "processor runtime initialized"
    );

    let mut ticker =
        tokio::time::interval(Duration::from_millis(config.processor_poll_interval_ms));
    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let response = service.poll_once().await?;
                if !response.processed_assignments.is_empty() {
                    let processed_events = response
                        .processed_assignments
                        .iter()
                        .map(|assignment| assignment.processed_event_count)
                        .sum::<usize>();
                    let sink_emissions = response
                        .processed_assignments
                        .iter()
                        .map(|assignment| assignment.sink_emissions.len())
                        .sum::<usize>();
                    info!(
                        processed_assignments = response.processed_assignments.len(),
                        processed_events,
                        sink_emissions,
                        "processor poll completed"
                    );
                }
                for failure in response.failed_assignments {
                    warn!(
                        tenant_id = failure.assignment.tenant_id,
                        pipeline_id = failure.assignment.pipeline_id,
                        version = failure.assignment.version,
                        partition_id = failure.assignment.partition_id,
                        worker_id = failure.assignment.worker_id,
                        reason = failure.reason,
                        "processor assignment failed"
                    );
                }
            }
            signal = tokio::signal::ctrl_c() => {
                signal?;
                info!(service = ServiceName::ProcessorRuntime.as_str(), "shutdown signal received");
                break;
            }
        }
    }

    Ok(())
}
