use event_pipeline_types::ServiceName;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceRuntimeConfig {
    pub service_name: ServiceName,
    pub bind_addr: String,
    pub database_url: String,
    pub metrics_addr: String,
    pub oidc_audience: String,
    pub oidc_issuer_url: String,
    pub oidc_public_key_pem: Option<String>,
    pub log_filter: String,
}

impl ServiceRuntimeConfig {
    #[must_use]
    pub fn from_env(service_name: ServiceName) -> Self {
        let bind_addr = std::env::var("BIND_ADDR").unwrap_or_else(|_| String::from("0.0.0.0:8080"));
        let database_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
            String::from("postgres://postgres:postgres@127.0.0.1:5432/event_pipeline")
        });
        let metrics_addr =
            std::env::var("METRICS_ADDR").unwrap_or_else(|_| String::from("0.0.0.0:9090"));
        let oidc_audience =
            std::env::var("OIDC_AUDIENCE").unwrap_or_else(|_| String::from("event-pipeline-api"));
        let oidc_issuer_url = std::env::var("OIDC_ISSUER_URL")
            .unwrap_or_else(|_| String::from("http://localhost:8081/realms/event-pipeline"));
        let oidc_public_key_pem = std::env::var("OIDC_PUBLIC_KEY_PEM")
            .ok()
            .filter(|value| !value.trim().is_empty());
        let log_filter =
            std::env::var("RUST_LOG").unwrap_or_else(|_| format!("{},info", service_name.as_str()));

        Self {
            service_name,
            bind_addr,
            database_url,
            metrics_addr,
            oidc_audience,
            oidc_issuer_url,
            oidc_public_key_pem,
            log_filter,
        }
    }
}
