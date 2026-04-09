use anyhow::Result;
use event_pipeline_config::ServiceRuntimeConfig;
use event_pipeline_control_plane_app::MetadataService;
use event_pipeline_postgres_store::PostgresMetadataRepository;
use event_pipeline_runtime::{init_tracing, wait_for_shutdown};
use event_pipeline_types::{ListPipelinesRequest, ListTopicsRequest, ServiceName};
use std::sync::Arc;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    let migrate_only = std::env::args()
        .skip(1)
        .any(|argument| argument == "--migrate-only");
    let config = ServiceRuntimeConfig::from_env(ServiceName::ControlPlane);

    init_tracing(&config.log_filter);

    let repository = PostgresMetadataRepository::connect(&config.database_url).await?;
    repository.migrate().await?;

    if migrate_only {
        info!("control-plane migrations completed");
        return Ok(());
    }

    let service = MetadataService::new(Arc::new(repository));
    let pipelines = service
        .list_pipelines(ListPipelinesRequest::default())
        .await?
        .pipelines
        .len();
    let topics = service
        .list_topics(ListTopicsRequest::default())
        .await?
        .topics
        .len();

    info!(
        service = ServiceName::ControlPlane.as_str(),
        bind_addr = config.bind_addr,
        database_url = config.database_url,
        pipeline_count = pipelines,
        topic_count = topics,
        "control-plane metadata service initialized"
    );

    wait_for_shutdown(ServiceName::ControlPlane.as_str()).await
}
