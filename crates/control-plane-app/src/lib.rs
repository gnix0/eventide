use anyhow::{Result, anyhow, bail};
use async_trait::async_trait;
use event_pipeline_types::{
    AssignRoleBindingRequest, AssignRoleBindingResponse, AuthenticatedPrincipal,
    CreatePipelineRequest, CreatePipelineResponse, CreateServiceAccountRequest,
    CreateServiceAccountResponse, CreateTenantRequest, CreateTenantResponse, DeploymentState,
    GetPipelineRequest, GetPipelineResponse, GetTenantRequest, GetTenantResponse, GetTopicRequest,
    GetTopicResponse, ListPipelinesRequest, ListPipelinesResponse, ListRoleBindingsRequest,
    ListRoleBindingsResponse, ListServiceAccountsRequest, ListServiceAccountsResponse,
    ListTenantsRequest, ListTenantsResponse, ListTopicsRequest, ListTopicsResponse, PipelineSpec,
    PipelineSummary, PlatformRole, RegisterTopicRequest, RegisterTopicResponse, RegisteredTopic,
    RoleBinding, ServiceAccount, SubjectKind, TenantRecord, TenantSummary, TopicSummary,
    UpdatePipelineVersionRequest, UpdatePipelineVersionResponse,
};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

#[async_trait]
pub trait MetadataRepository: Send + Sync {
    async fn create_pipeline(&self, pipeline: &PipelineSpec) -> Result<PipelineSummary>;
    async fn update_pipeline_version(&self, pipeline: &PipelineSpec) -> Result<PipelineSummary>;
    async fn get_pipeline(
        &self,
        tenant_id: &str,
        pipeline_id: &str,
        version: Option<u32>,
    ) -> Result<Option<PipelineSpec>>;
    async fn list_pipelines(
        &self,
        tenant_id: Option<&str>,
        deployment_state: Option<DeploymentState>,
    ) -> Result<Vec<PipelineSummary>>;
    async fn register_topic(&self, topic: &RegisteredTopic) -> Result<RegisteredTopic>;
    async fn get_topic(&self, tenant_id: &str, topic_name: &str)
    -> Result<Option<RegisteredTopic>>;
    async fn list_topics(&self, tenant_id: Option<&str>) -> Result<Vec<TopicSummary>>;
    async fn upsert_tenant(&self, tenant: &TenantRecord) -> Result<TenantSummary>;
    async fn get_tenant(&self, tenant_id: &str) -> Result<Option<TenantRecord>>;
    async fn list_tenants(&self) -> Result<Vec<TenantSummary>>;
    async fn assign_role_binding(&self, binding: &RoleBinding) -> Result<RoleBinding>;
    async fn list_role_bindings(
        &self,
        tenant_id: Option<&str>,
        subject_id: Option<&str>,
    ) -> Result<Vec<RoleBinding>>;
    async fn create_service_account(
        &self,
        service_account: &ServiceAccount,
    ) -> Result<ServiceAccount>;
    async fn list_service_accounts(&self, tenant_id: Option<&str>) -> Result<Vec<ServiceAccount>>;
}

#[derive(Clone)]
pub struct MetadataService {
    repository: Arc<dyn MetadataRepository>,
}

impl MetadataService {
    #[must_use]
    pub fn new(repository: Arc<dyn MetadataRepository>) -> Self {
        Self { repository }
    }

    pub async fn create_pipeline(
        &self,
        request: CreatePipelineRequest,
    ) -> Result<CreatePipelineResponse> {
        request.pipeline.validate()?;
        let pipeline = self.repository.create_pipeline(&request.pipeline).await?;
        Ok(CreatePipelineResponse { pipeline })
    }

    pub async fn update_pipeline_version(
        &self,
        request: UpdatePipelineVersionRequest,
    ) -> Result<UpdatePipelineVersionResponse> {
        request.pipeline.validate()?;
        let pipeline = self
            .repository
            .update_pipeline_version(&request.pipeline)
            .await?;
        Ok(UpdatePipelineVersionResponse { pipeline })
    }

    pub async fn get_pipeline(&self, request: GetPipelineRequest) -> Result<GetPipelineResponse> {
        let pipeline = self
            .repository
            .get_pipeline(&request.tenant_id, &request.pipeline_id, request.version)
            .await?
            .ok_or_else(|| anyhow!("pipeline not found"))?;

        Ok(GetPipelineResponse { pipeline })
    }

    pub async fn list_pipelines(
        &self,
        request: ListPipelinesRequest,
    ) -> Result<ListPipelinesResponse> {
        let pipelines = self
            .repository
            .list_pipelines(request.tenant_id.as_deref(), request.deployment_state)
            .await?;

        Ok(ListPipelinesResponse { pipelines })
    }

    pub async fn register_topic(
        &self,
        request: RegisterTopicRequest,
    ) -> Result<RegisterTopicResponse> {
        validate_topic(&request.topic)?;
        let topic = self.repository.register_topic(&request.topic).await?;
        Ok(RegisterTopicResponse { topic })
    }

    pub async fn get_topic(&self, request: GetTopicRequest) -> Result<GetTopicResponse> {
        let topic = self
            .repository
            .get_topic(&request.tenant_id, &request.topic_name)
            .await?
            .ok_or_else(|| anyhow!("topic not found"))?;

        Ok(GetTopicResponse { topic })
    }

    pub async fn list_topics(&self, request: ListTopicsRequest) -> Result<ListTopicsResponse> {
        let topics = self
            .repository
            .list_topics(request.tenant_id.as_deref())
            .await?;
        Ok(ListTopicsResponse { topics })
    }
}

#[derive(Clone)]
pub struct IdentityService {
    repository: Arc<dyn MetadataRepository>,
}

impl IdentityService {
    #[must_use]
    pub fn new(repository: Arc<dyn MetadataRepository>) -> Self {
        Self { repository }
    }

    pub async fn create_tenant(
        &self,
        request: CreateTenantRequest,
    ) -> Result<CreateTenantResponse> {
        validate_tenant(&request.tenant)?;
        let tenant = self.repository.upsert_tenant(&request.tenant).await?;
        Ok(CreateTenantResponse { tenant })
    }

    pub async fn get_tenant(&self, request: GetTenantRequest) -> Result<GetTenantResponse> {
        let tenant = self
            .repository
            .get_tenant(&request.tenant_id)
            .await?
            .ok_or_else(|| anyhow!("tenant not found"))?;

        Ok(GetTenantResponse { tenant })
    }

    pub async fn list_tenants(&self, _request: ListTenantsRequest) -> Result<ListTenantsResponse> {
        let tenants = self.repository.list_tenants().await?;
        Ok(ListTenantsResponse { tenants })
    }

    pub async fn assign_role_binding(
        &self,
        request: AssignRoleBindingRequest,
    ) -> Result<AssignRoleBindingResponse> {
        validate_role_binding(&request.binding)?;
        let binding = self
            .repository
            .assign_role_binding(&request.binding)
            .await?;
        Ok(AssignRoleBindingResponse { binding })
    }

    pub async fn list_role_bindings(
        &self,
        request: ListRoleBindingsRequest,
    ) -> Result<ListRoleBindingsResponse> {
        let bindings = self
            .repository
            .list_role_bindings(request.tenant_id.as_deref(), request.subject_id.as_deref())
            .await?;

        Ok(ListRoleBindingsResponse { bindings })
    }

    pub async fn create_service_account(
        &self,
        request: CreateServiceAccountRequest,
    ) -> Result<CreateServiceAccountResponse> {
        if request.tenant_id.trim().is_empty() {
            bail!("tenant id must not be empty");
        }

        if request.display_name.trim().is_empty() {
            bail!("service account display name must not be empty");
        }

        if self
            .repository
            .get_tenant(&request.tenant_id)
            .await?
            .is_none()
        {
            bail!("tenant not found");
        }

        let service_account = ServiceAccount {
            service_account_id: Uuid::now_v7().to_string(),
            tenant_id: request.tenant_id.clone(),
            client_id: format!(
                "{}-{}-{}",
                request.tenant_id,
                slugify(&request.display_name),
                &Uuid::now_v7().simple().to_string()[..8]
            ),
            display_name: request.display_name,
            enabled: true,
        };

        let service_account = self
            .repository
            .create_service_account(&service_account)
            .await?;
        Ok(CreateServiceAccountResponse { service_account })
    }

    pub async fn list_service_accounts(
        &self,
        request: ListServiceAccountsRequest,
    ) -> Result<ListServiceAccountsResponse> {
        let service_accounts = self
            .repository
            .list_service_accounts(request.tenant_id.as_deref())
            .await?;

        Ok(ListServiceAccountsResponse { service_accounts })
    }

    pub async fn authorize(
        &self,
        principal: &AuthenticatedPrincipal,
        tenant_id: &str,
        required_role: PlatformRole,
    ) -> Result<()> {
        if tenant_id.trim().is_empty() {
            bail!("tenant id must not be empty");
        }

        let bindings = self
            .repository
            .list_role_bindings(Some(tenant_id), Some(&principal.subject_id))
            .await?;

        if bindings.iter().any(|binding| {
            binding.tenant_id.is_none() && binding.role == PlatformRole::PlatformAdmin
        }) {
            return Ok(());
        }

        if has_permission(&bindings, tenant_id, required_role) {
            return Ok(());
        }

        bail!("principal is not authorized for the requested tenant action");
    }
}

fn validate_topic(topic: &RegisteredTopic) -> Result<()> {
    if topic.tenant_id.trim().is_empty() {
        bail!("tenant id must not be empty");
    }

    if topic.topic_name.trim().is_empty() {
        bail!("topic name must not be empty");
    }

    if topic.partition_count == 0 {
        bail!("partition count must be greater than zero");
    }

    if topic.retention_hours == 0 {
        bail!("retention hours must be greater than zero");
    }

    Ok(())
}

fn validate_tenant(tenant: &TenantRecord) -> Result<()> {
    if tenant.tenant_id.trim().is_empty() {
        bail!("tenant id must not be empty");
    }

    if tenant.display_name.trim().is_empty() {
        bail!("tenant display name must not be empty");
    }

    if tenant.oidc_realm.trim().is_empty() {
        bail!("tenant oidc realm must not be empty");
    }

    Ok(())
}

fn validate_role_binding(binding: &RoleBinding) -> Result<()> {
    if binding.subject_id.trim().is_empty() {
        bail!("role binding subject id must not be empty");
    }

    if binding.role != PlatformRole::PlatformAdmin && binding.tenant_id.is_none() {
        bail!("tenant-scoped roles must provide a tenant id");
    }

    Ok(())
}

fn has_permission(bindings: &[RoleBinding], tenant_id: &str, required_role: PlatformRole) -> bool {
    bindings.iter().any(|binding| {
        let tenant_matches = binding
            .tenant_id
            .as_deref()
            .is_none_or(|value| value == tenant_id);
        tenant_matches && role_satisfies(binding.role, required_role)
    })
}

fn role_satisfies(granted_role: PlatformRole, required_role: PlatformRole) -> bool {
    if granted_role == PlatformRole::PlatformAdmin || granted_role == PlatformRole::TenantAdmin {
        return true;
    }

    if granted_role == required_role {
        return true;
    }

    matches!(
        (granted_role, required_role),
        (PlatformRole::PipelineEditor, PlatformRole::PipelineViewer)
            | (PlatformRole::TopicEditor, PlatformRole::TopicViewer)
    )
}

fn slugify(value: &str) -> String {
    let slug: String = value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect();

    slug.trim_matches('-')
        .split('-')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>()
        .join("-")
}

type RoleBindingKey = (SubjectKind, String, Option<String>, PlatformRole);

#[derive(Default)]
pub struct InMemoryMetadataRepository {
    pipelines: RwLock<BTreeMap<(String, String, u32), PipelineSpec>>,
    current_versions: RwLock<BTreeMap<(String, String), u32>>,
    topics: RwLock<BTreeMap<(String, String), RegisteredTopic>>,
    tenants: RwLock<BTreeMap<String, TenantRecord>>,
    role_bindings: RwLock<BTreeSet<RoleBindingKey>>,
    service_accounts: RwLock<BTreeMap<String, ServiceAccount>>,
}

#[async_trait]
impl MetadataRepository for InMemoryMetadataRepository {
    async fn create_pipeline(&self, pipeline: &PipelineSpec) -> Result<PipelineSummary> {
        let pipeline_key = (pipeline.tenant_id.clone(), pipeline.pipeline_id.clone());
        let version_key = (
            pipeline.tenant_id.clone(),
            pipeline.pipeline_id.clone(),
            pipeline.version,
        );

        let mut current_versions = self.current_versions.write().await;
        if current_versions.contains_key(&pipeline_key) {
            bail!("pipeline already exists");
        }

        current_versions.insert(pipeline_key, pipeline.version);
        self.pipelines
            .write()
            .await
            .insert(version_key, pipeline.clone());

        Ok(PipelineSummary {
            tenant_id: pipeline.tenant_id.clone(),
            pipeline_id: pipeline.pipeline_id.clone(),
            version: pipeline.version,
            deployment_state: pipeline.deployment_state,
        })
    }

    async fn update_pipeline_version(&self, pipeline: &PipelineSpec) -> Result<PipelineSummary> {
        let pipeline_key = (pipeline.tenant_id.clone(), pipeline.pipeline_id.clone());
        let mut current_versions = self.current_versions.write().await;
        let Some(current_version) = current_versions.get_mut(&pipeline_key) else {
            bail!("pipeline not found");
        };

        if pipeline.version <= *current_version {
            bail!("pipeline version must be greater than the current version");
        }

        *current_version = pipeline.version;
        self.pipelines.write().await.insert(
            (
                pipeline.tenant_id.clone(),
                pipeline.pipeline_id.clone(),
                pipeline.version,
            ),
            pipeline.clone(),
        );

        Ok(PipelineSummary {
            tenant_id: pipeline.tenant_id.clone(),
            pipeline_id: pipeline.pipeline_id.clone(),
            version: pipeline.version,
            deployment_state: pipeline.deployment_state,
        })
    }

    async fn get_pipeline(
        &self,
        tenant_id: &str,
        pipeline_id: &str,
        version: Option<u32>,
    ) -> Result<Option<PipelineSpec>> {
        let resolved_version = if let Some(version) = version {
            version
        } else {
            let current_versions = self.current_versions.read().await;
            let Some(version) =
                current_versions.get(&(tenant_id.to_owned(), pipeline_id.to_owned()))
            else {
                return Ok(None);
            };
            *version
        };

        Ok(self
            .pipelines
            .read()
            .await
            .get(&(
                tenant_id.to_owned(),
                pipeline_id.to_owned(),
                resolved_version,
            ))
            .cloned())
    }

    async fn list_pipelines(
        &self,
        tenant_id: Option<&str>,
        deployment_state: Option<DeploymentState>,
    ) -> Result<Vec<PipelineSummary>> {
        let current_versions = self.current_versions.read().await;
        let pipelines = self.pipelines.read().await;
        let mut results = Vec::new();

        for ((stored_tenant_id, stored_pipeline_id), version) in &*current_versions {
            if tenant_id.is_some_and(|tenant_id| tenant_id != stored_tenant_id) {
                continue;
            }

            let Some(pipeline) = pipelines.get(&(
                stored_tenant_id.clone(),
                stored_pipeline_id.clone(),
                *version,
            )) else {
                continue;
            };

            if deployment_state
                .is_some_and(|deployment_state| deployment_state != pipeline.deployment_state)
            {
                continue;
            }

            results.push(PipelineSummary {
                tenant_id: stored_tenant_id.clone(),
                pipeline_id: stored_pipeline_id.clone(),
                version: *version,
                deployment_state: pipeline.deployment_state,
            });
        }

        results.sort_by(|left, right| {
            (&left.tenant_id, &left.pipeline_id, left.version).cmp(&(
                &right.tenant_id,
                &right.pipeline_id,
                right.version,
            ))
        });
        Ok(results)
    }

    async fn register_topic(&self, topic: &RegisteredTopic) -> Result<RegisteredTopic> {
        self.topics.write().await.insert(
            (topic.tenant_id.clone(), topic.topic_name.clone()),
            topic.clone(),
        );
        Ok(topic.clone())
    }

    async fn get_topic(
        &self,
        tenant_id: &str,
        topic_name: &str,
    ) -> Result<Option<RegisteredTopic>> {
        Ok(self
            .topics
            .read()
            .await
            .get(&(tenant_id.to_owned(), topic_name.to_owned()))
            .cloned())
    }

    async fn list_topics(&self, tenant_id: Option<&str>) -> Result<Vec<TopicSummary>> {
        let mut topics = Vec::new();
        for topic in self.topics.read().await.values() {
            if tenant_id.is_some_and(|tenant_id| tenant_id != topic.tenant_id) {
                continue;
            }

            topics.push(TopicSummary {
                tenant_id: topic.tenant_id.clone(),
                topic_name: topic.topic_name.clone(),
                partition_count: topic.partition_count,
            });
        }

        topics.sort_by(|left, right| {
            (&left.tenant_id, &left.topic_name).cmp(&(&right.tenant_id, &right.topic_name))
        });
        Ok(topics)
    }

    async fn upsert_tenant(&self, tenant: &TenantRecord) -> Result<TenantSummary> {
        self.tenants
            .write()
            .await
            .insert(tenant.tenant_id.clone(), tenant.clone());

        Ok(TenantSummary {
            tenant_id: tenant.tenant_id.clone(),
            display_name: tenant.display_name.clone(),
            oidc_realm: tenant.oidc_realm.clone(),
            enabled: tenant.enabled,
        })
    }

    async fn get_tenant(&self, tenant_id: &str) -> Result<Option<TenantRecord>> {
        Ok(self.tenants.read().await.get(tenant_id).cloned())
    }

    async fn list_tenants(&self) -> Result<Vec<TenantSummary>> {
        let mut tenants: Vec<_> = self
            .tenants
            .read()
            .await
            .values()
            .map(|tenant| TenantSummary {
                tenant_id: tenant.tenant_id.clone(),
                display_name: tenant.display_name.clone(),
                oidc_realm: tenant.oidc_realm.clone(),
                enabled: tenant.enabled,
            })
            .collect();

        tenants.sort_by(|left, right| left.tenant_id.cmp(&right.tenant_id));
        Ok(tenants)
    }

    async fn assign_role_binding(&self, binding: &RoleBinding) -> Result<RoleBinding> {
        self.role_bindings.write().await.insert((
            binding.subject_kind,
            binding.subject_id.clone(),
            binding.tenant_id.clone(),
            binding.role,
        ));
        Ok(binding.clone())
    }

    async fn list_role_bindings(
        &self,
        tenant_id: Option<&str>,
        subject_id: Option<&str>,
    ) -> Result<Vec<RoleBinding>> {
        let mut bindings = Vec::new();
        for (subject_kind, stored_subject_id, stored_tenant_id, role) in
            self.role_bindings.read().await.iter()
        {
            if subject_id.is_some_and(|subject_id| subject_id != stored_subject_id) {
                continue;
            }

            if tenant_id.is_some_and(|tenant_id| {
                stored_tenant_id
                    .as_deref()
                    .is_some_and(|value| value != tenant_id)
            }) {
                continue;
            }

            bindings.push(RoleBinding {
                subject_kind: *subject_kind,
                subject_id: stored_subject_id.clone(),
                tenant_id: stored_tenant_id.clone(),
                role: *role,
            });
        }

        Ok(bindings)
    }

    async fn create_service_account(
        &self,
        service_account: &ServiceAccount,
    ) -> Result<ServiceAccount> {
        self.service_accounts.write().await.insert(
            service_account.service_account_id.clone(),
            service_account.clone(),
        );
        Ok(service_account.clone())
    }

    async fn list_service_accounts(&self, tenant_id: Option<&str>) -> Result<Vec<ServiceAccount>> {
        let mut service_accounts: Vec<_> = self
            .service_accounts
            .read()
            .await
            .values()
            .filter(|service_account| {
                tenant_id.is_none_or(|tenant_id| tenant_id == service_account.tenant_id)
            })
            .cloned()
            .collect();

        service_accounts.sort_by(|left, right| {
            (&left.tenant_id, &left.client_id).cmp(&(&right.tenant_id, &right.client_id))
        });
        Ok(service_accounts)
    }
}

#[cfg(test)]
mod tests {
    use super::{IdentityService, InMemoryMetadataRepository, MetadataRepository, MetadataService};
    use event_pipeline_types::{
        AggregateFunction, AssignRoleBindingRequest, AuthenticatedPrincipal, CreatePipelineRequest,
        CreateServiceAccountRequest, CreateTenantRequest, DeploymentState, EventEncoding,
        ListPipelinesRequest, ListRoleBindingsRequest, ListServiceAccountsRequest,
        ListTenantsRequest, ListTopicsRequest, OperatorKind, OperatorNode, PipelineSpec,
        PlatformRole, RegisterTopicRequest, RegisteredTopic, RoleBinding, SinkKind, SinkSpec,
        SourceTopic, SubjectKind, TenantRecord, UpdatePipelineVersionRequest, WindowKind,
        WindowSpec,
    };
    use std::sync::Arc;

    #[tokio::test]
    async fn creates_and_reads_pipeline_metadata() {
        let service = MetadataService::new(Arc::new(InMemoryMetadataRepository::default()));
        let pipeline = valid_pipeline(1);

        service
            .create_pipeline(CreatePipelineRequest {
                pipeline: pipeline.clone(),
            })
            .await
            .expect("pipeline creation should succeed");

        let response = service
            .get_pipeline(event_pipeline_types::GetPipelineRequest {
                tenant_id: pipeline.tenant_id.clone(),
                pipeline_id: pipeline.pipeline_id.clone(),
                version: None,
            })
            .await
            .expect("pipeline lookup should succeed");

        assert_eq!(response.pipeline, pipeline);
    }

    #[tokio::test]
    async fn updates_pipeline_versions_and_lists_latest_version() {
        let service = MetadataService::new(Arc::new(InMemoryMetadataRepository::default()));

        service
            .create_pipeline(CreatePipelineRequest {
                pipeline: valid_pipeline(1),
            })
            .await
            .expect("pipeline creation should succeed");

        let mut version_two = valid_pipeline(2);
        version_two.deployment_state = DeploymentState::Validated;
        service
            .update_pipeline_version(UpdatePipelineVersionRequest {
                pipeline: version_two,
            })
            .await
            .expect("pipeline update should succeed");

        let response = service
            .list_pipelines(ListPipelinesRequest {
                tenant_id: Some(String::from("tenant-acme")),
                deployment_state: Some(DeploymentState::Validated),
            })
            .await
            .expect("pipeline listing should succeed");

        assert_eq!(response.pipelines.len(), 1);
        assert_eq!(response.pipelines[0].version, 2);
    }

    #[tokio::test]
    async fn rejects_invalid_pipeline_specs_before_storage() {
        let service = MetadataService::new(Arc::new(InMemoryMetadataRepository::default()));
        let mut pipeline = valid_pipeline(1);
        pipeline.sources[0].partition_count = 0;

        let error = service
            .create_pipeline(CreatePipelineRequest { pipeline })
            .await
            .expect_err("pipeline should fail validation");

        assert!(error.to_string().contains("partition count"));
    }

    #[tokio::test]
    async fn registers_and_lists_topics() {
        let service = MetadataService::new(Arc::new(InMemoryMetadataRepository::default()));

        service
            .register_topic(RegisterTopicRequest {
                topic: RegisteredTopic {
                    tenant_id: String::from("tenant-acme"),
                    topic_name: String::from("orders"),
                    partition_count: 12,
                    retention_hours: 168,
                },
            })
            .await
            .expect("topic registration should succeed");

        let response = service
            .list_topics(ListTopicsRequest {
                tenant_id: Some(String::from("tenant-acme")),
            })
            .await
            .expect("topic listing should succeed");

        assert_eq!(response.topics.len(), 1);
        assert_eq!(response.topics[0].topic_name, "orders");
    }

    #[tokio::test]
    async fn creates_tenants_role_bindings_and_service_accounts() {
        let repository = Arc::new(InMemoryMetadataRepository::default());
        let service = IdentityService::new(repository);

        service
            .create_tenant(CreateTenantRequest {
                tenant: TenantRecord {
                    tenant_id: String::from("tenant-acme"),
                    display_name: String::from("Acme"),
                    oidc_realm: String::from("event-pipeline"),
                    enabled: true,
                },
            })
            .await
            .expect("tenant creation should succeed");

        service
            .assign_role_binding(AssignRoleBindingRequest {
                binding: RoleBinding {
                    subject_kind: SubjectKind::User,
                    subject_id: String::from("user-123"),
                    tenant_id: Some(String::from("tenant-acme")),
                    role: PlatformRole::TenantAdmin,
                },
            })
            .await
            .expect("role assignment should succeed");

        let service_account = service
            .create_service_account(CreateServiceAccountRequest {
                tenant_id: String::from("tenant-acme"),
                display_name: String::from("Control Plane"),
            })
            .await
            .expect("service account creation should succeed")
            .service_account;

        let tenants = service
            .list_tenants(ListTenantsRequest)
            .await
            .expect("tenant listing should succeed");
        let bindings = service
            .list_role_bindings(ListRoleBindingsRequest {
                tenant_id: Some(String::from("tenant-acme")),
                subject_id: Some(String::from("user-123")),
            })
            .await
            .expect("role binding listing should succeed");
        let service_accounts = service
            .list_service_accounts(ListServiceAccountsRequest {
                tenant_id: Some(String::from("tenant-acme")),
            })
            .await
            .expect("service account listing should succeed");

        assert_eq!(tenants.tenants.len(), 1);
        assert_eq!(bindings.bindings.len(), 1);
        assert_eq!(service_accounts.service_accounts, vec![service_account]);
    }

    #[tokio::test]
    async fn authorizes_tenant_admins_for_tenant_scoped_actions() {
        let repository = Arc::new(InMemoryMetadataRepository::default());
        let service = IdentityService::new(repository.clone());

        service
            .create_tenant(CreateTenantRequest {
                tenant: TenantRecord {
                    tenant_id: String::from("tenant-acme"),
                    display_name: String::from("Acme"),
                    oidc_realm: String::from("event-pipeline"),
                    enabled: true,
                },
            })
            .await
            .expect("tenant creation should succeed");

        repository
            .assign_role_binding(&RoleBinding {
                subject_kind: SubjectKind::User,
                subject_id: String::from("user-123"),
                tenant_id: Some(String::from("tenant-acme")),
                role: PlatformRole::TenantAdmin,
            })
            .await
            .expect("binding should be stored");

        service
            .authorize(
                &AuthenticatedPrincipal {
                    subject_kind: SubjectKind::User,
                    subject_id: String::from("user-123"),
                    preferred_username: Some(String::from("alice")),
                    issuer: String::from("issuer"),
                    audience: vec![String::from("event-pipeline-api")],
                    realm_roles: Vec::new(),
                },
                "tenant-acme",
                PlatformRole::PipelineEditor,
            )
            .await
            .expect("tenant admin should be authorized");
    }

    fn valid_pipeline(version: u32) -> PipelineSpec {
        PipelineSpec {
            tenant_id: String::from("tenant-acme"),
            pipeline_id: String::from("orders-throughput"),
            version,
            sources: vec![SourceTopic {
                source_id: String::from("orders"),
                topic_name: String::from("orders"),
                partition_count: 12,
                partition_key: String::from("account_id"),
                encoding: EventEncoding::Json,
            }],
            operators: vec![
                OperatorNode {
                    operator_id: String::from("filter"),
                    upstream_ids: vec![String::from("orders")],
                    kind: OperatorKind::Filter {
                        expression: String::from("status == 'accepted'"),
                    },
                },
                OperatorNode {
                    operator_id: String::from("window"),
                    upstream_ids: vec![String::from("filter")],
                    kind: OperatorKind::Window(WindowSpec {
                        kind: WindowKind::Tumbling,
                        size_secs: 60,
                        slide_secs: None,
                        grace_secs: 10,
                    }),
                },
                OperatorNode {
                    operator_id: String::from("aggregate"),
                    upstream_ids: vec![String::from("window")],
                    kind: OperatorKind::Aggregate {
                        function: AggregateFunction::Count,
                        state_ttl_secs: 300,
                    },
                },
            ],
            sinks: vec![SinkSpec {
                sink_id: String::from("analytics"),
                upstream_id: String::from("aggregate"),
                kind: SinkKind::MaterializedView {
                    view_name: String::from("orders_throughput"),
                },
            }],
            deployment: event_pipeline_types::DeploymentConfig {
                parallelism: 12,
                checkpoint_interval_secs: 30,
                max_in_flight_messages: 5_000,
            },
            replay_policy: event_pipeline_types::ReplayPolicy {
                allow_manual_replay: true,
                retention_hours: 168,
            },
            deployment_state: DeploymentState::Draft,
        }
    }
}
