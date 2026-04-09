use anyhow::{Result, anyhow, bail};
use async_trait::async_trait;
use event_pipeline_types::{
    CreatePipelineRequest, CreatePipelineResponse, DeploymentState, GetPipelineRequest,
    GetPipelineResponse, GetTopicRequest, GetTopicResponse, ListPipelinesRequest,
    ListPipelinesResponse, ListTopicsRequest, ListTopicsResponse, PipelineSpec, PipelineSummary,
    RegisterTopicRequest, RegisterTopicResponse, RegisteredTopic, TopicSummary,
    UpdatePipelineVersionRequest, UpdatePipelineVersionResponse,
};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::RwLock;

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

#[derive(Default)]
pub struct InMemoryMetadataRepository {
    pipelines: RwLock<BTreeMap<(String, String, u32), PipelineSpec>>,
    current_versions: RwLock<BTreeMap<(String, String), u32>>,
    topics: RwLock<BTreeMap<(String, String), RegisteredTopic>>,
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
}

#[cfg(test)]
mod tests {
    use super::{InMemoryMetadataRepository, MetadataService};
    use event_pipeline_types::{
        AggregateFunction, CreatePipelineRequest, DeploymentConfig, DeploymentState, EventEncoding,
        ListPipelinesRequest, ListTopicsRequest, OperatorKind, OperatorNode, PipelineSpec,
        RegisterTopicRequest, RegisteredTopic, ReplayPolicy, SinkKind, SinkSpec, SourceTopic,
        UpdatePipelineVersionRequest, WindowKind, WindowSpec,
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
            deployment: DeploymentConfig {
                parallelism: 12,
                checkpoint_interval_secs: 30,
                max_in_flight_messages: 5_000,
            },
            replay_policy: ReplayPolicy {
                allow_manual_replay: true,
                retention_hours: 168,
            },
            deployment_state: DeploymentState::Draft,
        }
    }
}
