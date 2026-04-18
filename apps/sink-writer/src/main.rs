use anyhow::Result;
use eventide_config::ServiceRuntimeConfig;
use eventide_runtime::run_service;
use eventide_types::ServiceName;

#[tokio::main]
async fn main() -> Result<()> {
    let config = ServiceRuntimeConfig::from_env(ServiceName::SinkWriter);
    run_service(config).await
}
