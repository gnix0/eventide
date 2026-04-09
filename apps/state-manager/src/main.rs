use anyhow::Result;
use event_pipeline_config::ServiceRuntimeConfig;
use event_pipeline_runtime::run_service;
use event_pipeline_types::ServiceName;

#[tokio::main]
async fn main() -> Result<()> {
    let config = ServiceRuntimeConfig::from_env(ServiceName::StateManager);
    run_service(config).await
}
