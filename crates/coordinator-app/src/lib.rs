use anyhow::{Result, anyhow, bail};
use async_trait::async_trait;
use event_pipeline_types::{
    DeploymentState, ExpireWorkerLeasesRequest, ExpireWorkerLeasesResponse, HeartbeatWorkerRequest,
    HeartbeatWorkerResponse, ListAssignmentsRequest, ListAssignmentsResponse, ListWorkersRequest,
    ListWorkersResponse, PartitionAssignment, PipelineSpec, RebalancePipelineRequest,
    RebalancePipelineResponse, RegisterWorkerRequest, RegisterWorkerResponse, WorkerRecord,
    WorkerStatus,
};
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CoordinatorSettings {
    pub worker_lease_ttl_secs: u32,
    pub worker_stale_after_secs: u32,
}

impl Default for CoordinatorSettings {
    fn default() -> Self {
        Self {
            worker_lease_ttl_secs: 30,
            worker_stale_after_secs: 90,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct LeaseExpiryOutcome {
    pub expired_worker_ids: Vec<String>,
    pub revoked_assignment_count: u32,
}

#[async_trait]
pub trait CoordinatorRepository: Send + Sync {
    async fn register_worker(&self, worker: &WorkerRecord) -> Result<WorkerRecord>;
    async fn heartbeat_worker(
        &self,
        worker_id: &str,
        heartbeat_epoch_secs: u64,
    ) -> Result<Option<WorkerRecord>>;
    async fn list_workers(&self, status: Option<WorkerStatus>) -> Result<Vec<WorkerRecord>>;
    async fn get_pipeline(
        &self,
        tenant_id: &str,
        pipeline_id: &str,
    ) -> Result<Option<PipelineSpec>>;
    async fn list_assignments(
        &self,
        tenant_id: &str,
        pipeline_id: &str,
    ) -> Result<Vec<PartitionAssignment>>;
    async fn list_assignments_for_worker(
        &self,
        worker_id: &str,
    ) -> Result<Vec<PartitionAssignment>>;
    async fn replace_assignments(
        &self,
        tenant_id: &str,
        pipeline_id: &str,
        version: u32,
        lease_epoch: u64,
        lease_expires_at_epoch_secs: u64,
        assignments: &[PartitionAssignment],
    ) -> Result<Vec<PartitionAssignment>>;
    async fn expire_workers(&self, stale_before_epoch_secs: u64) -> Result<LeaseExpiryOutcome>;
}

#[derive(Clone)]
pub struct CoordinatorService {
    repository: Arc<dyn CoordinatorRepository>,
    settings: CoordinatorSettings,
    clock: fn() -> u64,
}

impl CoordinatorService {
    #[must_use]
    pub fn new(repository: Arc<dyn CoordinatorRepository>, settings: CoordinatorSettings) -> Self {
        Self {
            repository,
            settings,
            clock: current_epoch_secs,
        }
    }

    #[cfg(test)]
    fn with_clock(
        repository: Arc<dyn CoordinatorRepository>,
        settings: CoordinatorSettings,
        clock: fn() -> u64,
    ) -> Self {
        Self {
            repository,
            settings,
            clock,
        }
    }

    pub async fn register_worker(
        &self,
        request: RegisterWorkerRequest,
    ) -> Result<RegisterWorkerResponse> {
        validate_worker_registration(&request.worker)?;
        let now = (self.clock)();
        let worker = WorkerRecord {
            worker_id: request.worker.worker_id,
            endpoint: request.worker.endpoint,
            availability_zone: request.worker.availability_zone,
            max_assignments: request.worker.max_assignments,
            labels: normalize_labels(request.worker.labels),
            status: WorkerStatus::Ready,
            active_assignments: 0,
            registered_at_epoch_secs: now,
            last_heartbeat_at_epoch_secs: now,
        };

        let worker = self.repository.register_worker(&worker).await?;
        Ok(RegisterWorkerResponse {
            worker,
            lease_ttl_secs: self.settings.worker_lease_ttl_secs,
        })
    }

    pub async fn heartbeat_worker(
        &self,
        request: HeartbeatWorkerRequest,
    ) -> Result<HeartbeatWorkerResponse> {
        if request.worker_id.trim().is_empty() {
            bail!("worker id must not be empty");
        }

        let heartbeat_epoch_secs = (self.clock)();
        let worker = self
            .repository
            .heartbeat_worker(&request.worker_id, heartbeat_epoch_secs)
            .await?
            .ok_or_else(|| anyhow!("worker not found"))?;
        let assignments = self
            .repository
            .list_assignments_for_worker(&request.worker_id)
            .await?;

        Ok(HeartbeatWorkerResponse {
            worker,
            lease_ttl_secs: self.settings.worker_lease_ttl_secs,
            assignments,
        })
    }

    pub async fn list_workers(&self, request: ListWorkersRequest) -> Result<ListWorkersResponse> {
        let workers = self.repository.list_workers(request.status).await?;
        Ok(ListWorkersResponse { workers })
    }

    pub async fn list_assignments(
        &self,
        request: ListAssignmentsRequest,
    ) -> Result<ListAssignmentsResponse> {
        if request.tenant_id.trim().is_empty() {
            bail!("tenant id must not be empty");
        }

        if request.pipeline_id.trim().is_empty() {
            bail!("pipeline id must not be empty");
        }

        let assignments = self
            .repository
            .list_assignments(&request.tenant_id, &request.pipeline_id)
            .await?;

        Ok(ListAssignmentsResponse { assignments })
    }

    pub async fn rebalance_pipeline(
        &self,
        request: RebalancePipelineRequest,
    ) -> Result<RebalancePipelineResponse> {
        if request.tenant_id.trim().is_empty() {
            bail!("tenant id must not be empty");
        }

        if request.pipeline_id.trim().is_empty() {
            bail!("pipeline id must not be empty");
        }

        let pipeline = self
            .repository
            .get_pipeline(&request.tenant_id, &request.pipeline_id)
            .await?
            .ok_or_else(|| anyhow!("pipeline not found"))?;
        validate_rebalance_target(&pipeline)?;

        let now = (self.clock)();
        let mut workers = self
            .repository
            .list_workers(Some(WorkerStatus::Ready))
            .await?
            .into_iter()
            .filter(|worker| {
                worker.last_heartbeat_at_epoch_secs
                    + u64::from(self.settings.worker_stale_after_secs)
                    >= now
            })
            .collect::<Vec<_>>();

        if workers.is_empty() {
            bail!("no ready workers are available for assignment");
        }

        let partition_count = pipeline_partition_count(&pipeline)?;
        let total_capacity: u32 = workers
            .iter()
            .map(|worker| u32::from(worker.max_assignments))
            .sum();

        if total_capacity < partition_count {
            bail!("worker capacity is lower than the required partition count");
        }

        workers.sort_by(compare_worker_load);
        let current_assignments = self
            .repository
            .list_assignments(&request.tenant_id, &request.pipeline_id)
            .await?;
        let lease_epoch = current_assignments
            .iter()
            .map(|assignment| assignment.lease_epoch)
            .max()
            .unwrap_or(0)
            + 1;

        let assignments = build_assignments(partition_count, lease_epoch, &mut workers)?;
        let persisted = self
            .repository
            .replace_assignments(
                &request.tenant_id,
                &request.pipeline_id,
                pipeline.version,
                lease_epoch,
                now + u64::from(self.settings.worker_lease_ttl_secs),
                &assignments,
            )
            .await?;

        Ok(RebalancePipelineResponse {
            version: pipeline.version,
            lease_epoch,
            assignments: persisted,
        })
    }

    pub async fn expire_worker_leases(
        &self,
        request: ExpireWorkerLeasesRequest,
    ) -> Result<ExpireWorkerLeasesResponse> {
        let stale_after_secs = if request.stale_after_secs == 0 {
            self.settings.worker_stale_after_secs
        } else {
            request.stale_after_secs
        };
        let now = (self.clock)();
        let outcome = self
            .repository
            .expire_workers(now.saturating_sub(u64::from(stale_after_secs)))
            .await?;

        Ok(ExpireWorkerLeasesResponse {
            expired_worker_ids: outcome.expired_worker_ids,
            revoked_assignment_count: outcome.revoked_assignment_count,
        })
    }
}

fn validate_worker_registration(worker: &WorkerRecord) -> Result<()> {
    if worker.worker_id.trim().is_empty() {
        bail!("worker id must not be empty");
    }

    if worker.endpoint.trim().is_empty() {
        bail!("worker endpoint must not be empty");
    }

    if worker.availability_zone.trim().is_empty() {
        bail!("worker availability zone must not be empty");
    }

    if worker.max_assignments == 0 {
        bail!("worker max assignments must be greater than zero");
    }

    Ok(())
}

fn validate_rebalance_target(pipeline: &PipelineSpec) -> Result<()> {
    match pipeline.deployment_state {
        DeploymentState::Validated | DeploymentState::Deploying | DeploymentState::Running => {
            Ok(())
        }
        DeploymentState::Draft => bail!("pipeline must be validated before it can be rebalanced"),
        DeploymentState::Paused => {
            bail!("paused pipelines are not eligible for worker assignments")
        }
        DeploymentState::Failed => {
            bail!("failed pipelines are not eligible for worker assignments")
        }
    }
}

fn pipeline_partition_count(pipeline: &PipelineSpec) -> Result<u32> {
    pipeline
        .sources
        .iter()
        .map(|source| u32::from(source.partition_count))
        .max()
        .ok_or_else(|| anyhow!("pipeline has no source partitions"))
}

fn build_assignments(
    partition_count: u32,
    lease_epoch: u64,
    workers: &mut [WorkerRecord],
) -> Result<Vec<PartitionAssignment>> {
    let mut assignments = Vec::with_capacity(usize::try_from(partition_count)?);

    for partition_id in 0..partition_count {
        workers.sort_by(compare_worker_load);
        let Some(worker) = workers
            .iter_mut()
            .find(|worker| worker.active_assignments < worker.max_assignments)
        else {
            bail!("worker capacity is lower than the required partition count");
        };

        assignments.push(PartitionAssignment {
            partition_id,
            worker_id: worker.worker_id.clone(),
            lease_epoch,
        });
        worker.active_assignments += 1;
    }

    Ok(assignments)
}

fn compare_worker_load(left: &WorkerRecord, right: &WorkerRecord) -> Ordering {
    (
        left.active_assignments,
        left.last_heartbeat_at_epoch_secs,
        &left.worker_id,
    )
        .cmp(&(
            right.active_assignments,
            right.last_heartbeat_at_epoch_secs,
            &right.worker_id,
        ))
}

fn normalize_labels(labels: Vec<String>) -> Vec<String> {
    let mut labels = labels
        .into_iter()
        .map(|label| label.trim().to_ascii_lowercase())
        .filter(|label| !label.is_empty())
        .collect::<Vec<_>>();
    labels.sort();
    labels.dedup();
    labels
}

fn current_epoch_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_secs()
}

#[derive(Default)]
pub struct InMemoryCoordinatorRepository {
    workers: RwLock<BTreeMap<String, WorkerRecord>>,
    pipelines: RwLock<BTreeMap<(String, String), PipelineSpec>>,
    assignments: RwLock<BTreeMap<(String, String, u32), Vec<PartitionAssignment>>>,
}

impl InMemoryCoordinatorRepository {
    pub async fn seed_pipeline(&self, pipeline: PipelineSpec) {
        self.pipelines.write().await.insert(
            (pipeline.tenant_id.clone(), pipeline.pipeline_id.clone()),
            pipeline,
        );
    }

    async fn assignment_count_for_worker(&self, worker_id: &str) -> u16 {
        let assignments = self.assignments.read().await;
        let count = assignments
            .values()
            .flatten()
            .filter(|assignment| assignment.worker_id == worker_id)
            .count();

        u16::try_from(count).unwrap_or(u16::MAX)
    }
}

#[async_trait]
impl CoordinatorRepository for InMemoryCoordinatorRepository {
    async fn register_worker(&self, worker: &WorkerRecord) -> Result<WorkerRecord> {
        let active_assignments = self.assignment_count_for_worker(&worker.worker_id).await;
        let mut worker = worker.clone();
        if let Some(existing) = self.workers.read().await.get(&worker.worker_id).cloned() {
            worker.registered_at_epoch_secs = existing.registered_at_epoch_secs;
        }
        worker.active_assignments = active_assignments;
        self.workers
            .write()
            .await
            .insert(worker.worker_id.clone(), worker.clone());
        Ok(worker)
    }

    async fn heartbeat_worker(
        &self,
        worker_id: &str,
        heartbeat_epoch_secs: u64,
    ) -> Result<Option<WorkerRecord>> {
        let active_assignments = self.assignment_count_for_worker(worker_id).await;
        let mut workers = self.workers.write().await;
        let Some(worker) = workers.get_mut(worker_id) else {
            return Ok(None);
        };

        worker.last_heartbeat_at_epoch_secs = heartbeat_epoch_secs;
        if worker.status == WorkerStatus::Expired {
            worker.status = WorkerStatus::Ready;
        }
        worker.active_assignments = active_assignments;
        Ok(Some(worker.clone()))
    }

    async fn list_workers(&self, status: Option<WorkerStatus>) -> Result<Vec<WorkerRecord>> {
        let workers = self.workers.read().await;
        let mut results = Vec::new();

        for worker in workers.values() {
            if status.is_some_and(|status| worker.status != status) {
                continue;
            }

            let mut worker = worker.clone();
            worker.active_assignments = self.assignment_count_for_worker(&worker.worker_id).await;
            results.push(worker);
        }

        results.sort_by(|left, right| left.worker_id.cmp(&right.worker_id));
        Ok(results)
    }

    async fn get_pipeline(
        &self,
        tenant_id: &str,
        pipeline_id: &str,
    ) -> Result<Option<PipelineSpec>> {
        Ok(self
            .pipelines
            .read()
            .await
            .get(&(tenant_id.to_owned(), pipeline_id.to_owned()))
            .cloned())
    }

    async fn list_assignments(
        &self,
        tenant_id: &str,
        pipeline_id: &str,
    ) -> Result<Vec<PartitionAssignment>> {
        let assignments = self.assignments.read().await;
        let mut results = assignments
            .iter()
            .filter(|((stored_tenant_id, stored_pipeline_id, _), _)| {
                stored_tenant_id == tenant_id && stored_pipeline_id == pipeline_id
            })
            .flat_map(|(_, assignments)| assignments.clone())
            .collect::<Vec<_>>();
        results.sort_by(|left, right| left.partition_id.cmp(&right.partition_id));
        Ok(results)
    }

    async fn list_assignments_for_worker(
        &self,
        worker_id: &str,
    ) -> Result<Vec<PartitionAssignment>> {
        let assignments = self.assignments.read().await;
        let mut results = assignments
            .values()
            .flatten()
            .filter(|assignment| assignment.worker_id == worker_id)
            .cloned()
            .collect::<Vec<_>>();
        results.sort_by(|left, right| {
            (&left.worker_id, left.partition_id).cmp(&(&right.worker_id, right.partition_id))
        });
        Ok(results)
    }

    async fn replace_assignments(
        &self,
        tenant_id: &str,
        pipeline_id: &str,
        version: u32,
        _lease_epoch: u64,
        _lease_expires_at_epoch_secs: u64,
        assignments: &[PartitionAssignment],
    ) -> Result<Vec<PartitionAssignment>> {
        let mut stored = self.assignments.write().await;
        stored.retain(|(stored_tenant_id, stored_pipeline_id, _), _| {
            stored_tenant_id != tenant_id || stored_pipeline_id != pipeline_id
        });
        stored.insert(
            (tenant_id.to_owned(), pipeline_id.to_owned(), version),
            assignments.to_vec(),
        );

        Ok(assignments.to_vec())
    }

    async fn expire_workers(&self, stale_before_epoch_secs: u64) -> Result<LeaseExpiryOutcome> {
        let mut workers = self.workers.write().await;
        let expired_worker_ids = workers
            .values_mut()
            .filter(|worker| {
                worker.status != WorkerStatus::Expired
                    && worker.last_heartbeat_at_epoch_secs < stale_before_epoch_secs
            })
            .map(|worker| {
                worker.status = WorkerStatus::Expired;
                worker.active_assignments = 0;
                worker.worker_id.clone()
            })
            .collect::<Vec<_>>();

        if expired_worker_ids.is_empty() {
            return Ok(LeaseExpiryOutcome::default());
        }

        let mut revoked_assignment_count = 0_u32;
        let mut assignments = self.assignments.write().await;
        for stored in assignments.values_mut() {
            let before = stored.len();
            stored.retain(|assignment| !expired_worker_ids.contains(&assignment.worker_id));
            revoked_assignment_count += u32::try_from(before - stored.len()).unwrap_or(u32::MAX);
        }
        assignments.retain(|_, stored| !stored.is_empty());

        Ok(LeaseExpiryOutcome {
            expired_worker_ids,
            revoked_assignment_count,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CoordinatorService, CoordinatorSettings, InMemoryCoordinatorRepository, current_epoch_secs,
    };
    use event_pipeline_types::{
        AggregateFunction, DeploymentConfig, DeploymentState, EventEncoding,
        ExpireWorkerLeasesRequest, HeartbeatWorkerRequest, JoinKind, ListAssignmentsRequest,
        OperatorKind, OperatorNode, PipelineSpec, RebalancePipelineRequest, RegisterWorkerRequest,
        ReplayPolicy, SinkKind, SinkSpec, SourceTopic, WindowKind, WindowSpec, WorkerRecord,
        WorkerStatus,
    };
    use std::sync::Arc;

    fn fixed_clock() -> u64 {
        1_735_000_000
    }

    fn later_clock() -> u64 {
        fixed_clock() + 120
    }

    fn worker(worker_id: &str, max_assignments: u16) -> WorkerRecord {
        WorkerRecord {
            worker_id: worker_id.to_owned(),
            endpoint: format!("grpc://{worker_id}:9090"),
            availability_zone: String::from("sa-east-1a"),
            max_assignments,
            labels: vec![String::from("streaming"), String::from("general")],
            status: WorkerStatus::Ready,
            active_assignments: 99,
            registered_at_epoch_secs: 0,
            last_heartbeat_at_epoch_secs: 0,
        }
    }

    fn pipeline(partitions: u16) -> PipelineSpec {
        PipelineSpec {
            tenant_id: String::from("tenant-a"),
            pipeline_id: String::from("pipeline-a"),
            version: 3,
            sources: vec![SourceTopic {
                source_id: String::from("orders"),
                topic_name: String::from("orders.v1"),
                partition_count: partitions,
                partition_key: String::from("tenant_id"),
                encoding: EventEncoding::Json,
            }],
            operators: vec![
                OperatorNode {
                    operator_id: String::from("key_by_tenant"),
                    upstream_ids: vec![String::from("orders")],
                    kind: OperatorKind::KeyBy {
                        key_field: String::from("tenant_id"),
                        partition_count: partitions,
                    },
                },
                OperatorNode {
                    operator_id: String::from("window"),
                    upstream_ids: vec![String::from("key_by_tenant")],
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
                        state_ttl_secs: 600,
                    },
                },
                OperatorNode {
                    operator_id: String::from("enrich"),
                    upstream_ids: vec![String::from("aggregate")],
                    kind: OperatorKind::Enrich {
                        table_name: String::from("tenant_profiles"),
                        key_field: String::from("tenant_id"),
                    },
                },
                OperatorNode {
                    operator_id: String::from("join"),
                    upstream_ids: vec![String::from("aggregate"), String::from("enrich")],
                    kind: OperatorKind::Join {
                        kind: JoinKind::Left,
                        key_field: String::from("tenant_id"),
                        within_secs: 30,
                    },
                },
            ],
            sinks: vec![SinkSpec {
                sink_id: String::from("sink"),
                upstream_id: String::from("join"),
                kind: SinkKind::Kafka {
                    topic_name: String::from("aggregates.v1"),
                    partition_count: partitions,
                    partition_key: String::from("tenant_id"),
                },
            }],
            deployment: DeploymentConfig {
                parallelism: partitions,
                checkpoint_interval_secs: 30,
                max_in_flight_messages: 1_000,
            },
            replay_policy: ReplayPolicy {
                allow_manual_replay: true,
                retention_hours: 24,
            },
            deployment_state: DeploymentState::Running,
        }
    }

    #[tokio::test]
    async fn register_worker_normalizes_runtime_fields() {
        let repository = Arc::new(InMemoryCoordinatorRepository::default());
        let service =
            CoordinatorService::with_clock(repository, CoordinatorSettings::default(), fixed_clock);

        let response = service
            .register_worker(RegisterWorkerRequest {
                worker: worker("worker-a", 4),
            })
            .await
            .expect("worker should register");

        assert_eq!(response.worker.status, WorkerStatus::Ready);
        assert_eq!(response.worker.active_assignments, 0);
        assert_eq!(response.worker.registered_at_epoch_secs, fixed_clock());
        assert_eq!(response.worker.last_heartbeat_at_epoch_secs, fixed_clock());
        assert_eq!(
            response.worker.labels,
            vec![String::from("general"), String::from("streaming")]
        );
    }

    #[tokio::test]
    async fn rebalance_assigns_partitions_across_ready_workers() {
        let repository = Arc::new(InMemoryCoordinatorRepository::default());
        repository.seed_pipeline(pipeline(4)).await;

        let service = CoordinatorService::with_clock(
            repository.clone(),
            CoordinatorSettings {
                worker_lease_ttl_secs: 45,
                worker_stale_after_secs: 90,
            },
            fixed_clock,
        );

        service
            .register_worker(RegisterWorkerRequest {
                worker: worker("worker-a", 2),
            })
            .await
            .expect("worker-a should register");
        service
            .register_worker(RegisterWorkerRequest {
                worker: worker("worker-b", 2),
            })
            .await
            .expect("worker-b should register");

        let response = service
            .rebalance_pipeline(RebalancePipelineRequest {
                tenant_id: String::from("tenant-a"),
                pipeline_id: String::from("pipeline-a"),
            })
            .await
            .expect("rebalance should succeed");

        assert_eq!(response.version, 3);
        assert_eq!(response.lease_epoch, 1);
        assert_eq!(response.assignments.len(), 4);
        assert_eq!(
            response
                .assignments
                .iter()
                .filter(|assignment| assignment.worker_id == "worker-a")
                .count(),
            2
        );
        assert_eq!(
            response
                .assignments
                .iter()
                .filter(|assignment| assignment.worker_id == "worker-b")
                .count(),
            2
        );
    }

    #[tokio::test]
    async fn heartbeat_returns_current_worker_assignments() {
        let repository = Arc::new(InMemoryCoordinatorRepository::default());
        repository.seed_pipeline(pipeline(2)).await;

        let service = CoordinatorService::with_clock(
            repository.clone(),
            CoordinatorSettings::default(),
            fixed_clock,
        );

        service
            .register_worker(RegisterWorkerRequest {
                worker: worker("worker-a", 2),
            })
            .await
            .expect("worker should register");

        service
            .rebalance_pipeline(RebalancePipelineRequest {
                tenant_id: String::from("tenant-a"),
                pipeline_id: String::from("pipeline-a"),
            })
            .await
            .expect("rebalance should succeed");

        let heartbeat = service
            .heartbeat_worker(HeartbeatWorkerRequest {
                worker_id: String::from("worker-a"),
            })
            .await
            .expect("heartbeat should succeed");

        assert_eq!(heartbeat.worker.active_assignments, 2);
        assert_eq!(heartbeat.assignments.len(), 2);
        assert_eq!(heartbeat.lease_ttl_secs, 30);
    }

    #[tokio::test]
    async fn expire_worker_leases_revokes_assignments_for_stale_workers() {
        let repository = Arc::new(InMemoryCoordinatorRepository::default());
        repository.seed_pipeline(pipeline(2)).await;

        let registration_service = CoordinatorService::with_clock(
            repository.clone(),
            CoordinatorSettings {
                worker_lease_ttl_secs: 30,
                worker_stale_after_secs: 30,
            },
            fixed_clock,
        );

        registration_service
            .register_worker(RegisterWorkerRequest {
                worker: worker("worker-a", 2),
            })
            .await
            .expect("worker should register");
        registration_service
            .rebalance_pipeline(RebalancePipelineRequest {
                tenant_id: String::from("tenant-a"),
                pipeline_id: String::from("pipeline-a"),
            })
            .await
            .expect("rebalance should succeed");

        let expiry_service = CoordinatorService::with_clock(
            repository,
            CoordinatorSettings {
                worker_lease_ttl_secs: 30,
                worker_stale_after_secs: 30,
            },
            later_clock,
        );

        let expiry = expiry_service
            .expire_worker_leases(ExpireWorkerLeasesRequest {
                stale_after_secs: 1,
            })
            .await
            .expect("expiry should succeed");

        assert_eq!(expiry.expired_worker_ids, vec![String::from("worker-a")]);
        assert_eq!(expiry.revoked_assignment_count, 2);

        let assignments = expiry_service
            .list_assignments(ListAssignmentsRequest {
                tenant_id: String::from("tenant-a"),
                pipeline_id: String::from("pipeline-a"),
            })
            .await
            .expect("assignments should list");
        assert!(assignments.assignments.is_empty());
    }

    #[test]
    fn current_epoch_secs_is_non_zero() {
        assert!(current_epoch_secs() > 0);
    }
}
