use anyhow::Result;
use eventide_auth::{OidcSettings, OidcTokenValidator};
use eventide_config::ServiceRuntimeConfig;
use eventide_control_plane_app::{IdentityService, MetadataService};
use eventide_postgres_store::PostgresMetadataRepository;
use eventide_runtime::{init_tracing, wait_for_shutdown};
use eventide_types::{ListPipelinesRequest, ListTenantsRequest, ListTopicsRequest, ServiceName};
use std::sync::Arc;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    let migrate_only = std::env::args()
        .skip(1)
        .any(|argument| argument == "--migrate-only");
    let config = ServiceRuntimeConfig::from_env(ServiceName::ControlPlane);

    init_tracing(&config.log_filter);

    let repository = Arc::new(PostgresMetadataRepository::connect(&config.database_url).await?);
    repository.migrate().await?;

    if migrate_only {
        info!("control-plane migrations completed");
        return Ok(());
    }

    let service = MetadataService::new(repository.clone());
    let identity_service = IdentityService::new(repository);
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
    let tenants = identity_service
        .list_tenants(ListTenantsRequest)
        .await?
        .tenants
        .len();

    let oidc_validator_enabled = config.oidc_public_key_pem.is_some();
    let _validator = config
        .oidc_public_key_pem
        .as_ref()
        .map(|public_key_pem| {
            OidcTokenValidator::new(OidcSettings {
                issuer: config.oidc_issuer_url.clone(),
                audience: config.oidc_audience.clone(),
                public_key_pem: public_key_pem.clone(),
            })
        })
        .transpose()?;

    info!(
        service = ServiceName::ControlPlane.as_str(),
        bind_addr = config.bind_addr,
        database_url = config.database_url,
        oidc_issuer = config.oidc_issuer_url,
        oidc_audience = config.oidc_audience,
        oidc_validator_enabled,
        pipeline_count = pipelines,
        tenant_count = tenants,
        topic_count = topics,
        "control-plane metadata service initialized"
    );

    wait_for_shutdown(ServiceName::ControlPlane.as_str()).await
}
