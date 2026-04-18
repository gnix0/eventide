use eventide_types::ServiceName;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceRuntimeConfig {
    pub service_name: ServiceName,
    pub bind_addr: String,
    pub database_url: String,
    pub metrics_addr: String,
    pub processor_worker_id: String,
    pub processor_poll_interval_ms: u64,
    pub processor_batch_size: usize,
    pub worker_lease_ttl_secs: u32,
    pub worker_stale_after_secs: u32,
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
            String::from("postgres://postgres:postgres@127.0.0.1:5432/eventide")
        });
        let metrics_addr =
            std::env::var("METRICS_ADDR").unwrap_or_else(|_| String::from("0.0.0.0:9090"));
        let processor_worker_id = std::env::var("PROCESSOR_WORKER_ID")
            .unwrap_or_else(|_| format!("{}-default", service_name.as_str()));
        let processor_poll_interval_ms = std::env::var("PROCESSOR_POLL_INTERVAL_MS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(5_000);
        let processor_batch_size = std::env::var("PROCESSOR_BATCH_SIZE")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(128);
        let worker_lease_ttl_secs = std::env::var("WORKER_LEASE_TTL_SECS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(30);
        let worker_stale_after_secs = std::env::var("WORKER_STALE_AFTER_SECS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(90);
        let oidc_audience =
            std::env::var("OIDC_AUDIENCE").unwrap_or_else(|_| String::from("eventide-api"));
        let oidc_issuer_url = std::env::var("OIDC_ISSUER_URL")
            .unwrap_or_else(|_| String::from("http://localhost:8081/realms/eventide"));
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
            processor_worker_id,
            processor_poll_interval_ms,
            processor_batch_size,
            worker_lease_ttl_secs,
            worker_stale_after_secs,
            oidc_audience,
            oidc_issuer_url,
            oidc_public_key_pem,
            log_filter,
        }
    }
}
