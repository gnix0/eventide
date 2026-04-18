use anyhow::{Result, anyhow, bail};
use async_trait::async_trait;
use eventide_types::{
    CheckpointSummary, DeadLetterRecord, DeploymentState, GetCheckpointHistoryRequest,
    GetCheckpointHistoryResponse, GetRunStatusRequest, GetRunStatusResponse,
    ListAssignmentsRequest, ListAssignmentsResponse, ListDeadLettersRequest,
    ListDeadLettersResponse, ListReplayJobsRequest, ListReplayJobsResponse, PartitionAssignment,
    PipelineRunStatus, PipelineSpec, ReplayJob, RunStatus, WorkerRecord, WorkerStatus,
};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QuerySettings {
    pub worker_stale_after_secs: u32,
}

impl Default for QuerySettings {
    fn default() -> Self {
        Self {
            worker_stale_after_secs: 90,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PartitionHighWatermark {
    pub partition_id: u32,
    pub max_offset: u64,
}

#[async_trait]
pub trait QueryRepository: Send + Sync {
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
    async fn list_workers_for_pipeline(
        &self,
        tenant_id: &str,
        pipeline_id: &str,
    ) -> Result<Vec<WorkerRecord>>;
    async fn list_replay_jobs(&self, tenant_id: &str, pipeline_id: &str) -> Result<Vec<ReplayJob>>;
    async fn list_checkpoints(
        &self,
        tenant_id: &str,
        pipeline_id: &str,
    ) -> Result<Vec<CheckpointSummary>>;
    async fn list_dead_letters(
        &self,
        tenant_id: &str,
        pipeline_id: &str,
    ) -> Result<Vec<DeadLetterRecord>>;
    async fn list_partition_high_watermarks(
        &self,
        tenant_id: &str,
        pipeline_id: &str,
    ) -> Result<Vec<PartitionHighWatermark>>;
}

#[derive(Clone)]
pub struct QueryService {
    repository: Arc<dyn QueryRepository>,
    settings: QuerySettings,
    clock: fn() -> u64,
}

impl QueryService {
    #[must_use]
    pub fn new(repository: Arc<dyn QueryRepository>, settings: QuerySettings) -> Self {
        Self {
            repository,
            settings,
            clock: current_epoch_secs,
        }
    }

    #[cfg(test)]
    fn with_clock(
        repository: Arc<dyn QueryRepository>,
        settings: QuerySettings,
        clock: fn() -> u64,
    ) -> Self {
        Self {
            repository,
            settings,
            clock,
        }
    }

    pub async fn get_run_status(
        &self,
        request: GetRunStatusRequest,
    ) -> Result<GetRunStatusResponse> {
        validate_pipeline_scope(&request.tenant_id, &request.pipeline_id)?;

        let pipeline = self
            .repository
            .get_pipeline(&request.tenant_id, &request.pipeline_id)
            .await?
            .ok_or_else(|| anyhow!("pipeline not found"))?;
        let assignments = self
            .repository
            .list_assignments(&request.tenant_id, &request.pipeline_id)
            .await?;
        let workers = self
            .repository
            .list_workers_for_pipeline(&request.tenant_id, &request.pipeline_id)
            .await?;
        let replay_jobs = self
            .repository
            .list_replay_jobs(&request.tenant_id, &request.pipeline_id)
            .await?;
        let checkpoints = self
            .repository
            .list_checkpoints(&request.tenant_id, &request.pipeline_id)
            .await?;
        let dead_letters = self
            .repository
            .list_dead_letters(&request.tenant_id, &request.pipeline_id)
            .await?;
        let high_watermarks = self
            .repository
            .list_partition_high_watermarks(&request.tenant_id, &request.pipeline_id)
            .await?;

        let checkpoint_offsets = latest_checkpoint_offsets(&checkpoints);
        let consumer_lag = high_watermarks
            .into_iter()
            .map(|high_watermark| {
                high_watermark.max_offset.saturating_sub(
                    *checkpoint_offsets
                        .get(&high_watermark.partition_id)
                        .unwrap_or(&0),
                )
            })
            .sum();

        let now = (self.clock)();
        let latest_checkpoint_created_at = checkpoints
            .iter()
            .map(|checkpoint| checkpoint.created_at_epoch_secs)
            .max();
        let checkpoint_age_secs = latest_checkpoint_created_at.map_or(u32::MAX, |created_at| {
            u32::try_from(now.saturating_sub(created_at)).unwrap_or(u32::MAX)
        });

        let ready_worker_count = workers
            .iter()
            .filter(|worker| {
                worker.status == WorkerStatus::Ready
                    && worker.last_heartbeat_at_epoch_secs
                        + u64::from(self.settings.worker_stale_after_secs)
                        >= now
            })
            .count();
        let replay_backlog = replay_jobs
            .iter()
            .filter(|replay_job| {
                matches!(
                    replay_job.status,
                    eventide_types::ReplayJobStatus::Pending
                        | eventide_types::ReplayJobStatus::Running
                )
            })
            .count();

        let run_status = PipelineRunStatus {
            tenant_id: request.tenant_id,
            pipeline_id: request.pipeline_id,
            state: derive_run_status(&pipeline, &assignments, replay_backlog),
            consumer_lag,
            checkpoint_age_secs,
            active_assignment_count: u32::try_from(assignments.len()).unwrap_or(u32::MAX),
            ready_worker_count: u32::try_from(ready_worker_count).unwrap_or(u32::MAX),
            replay_backlog: u32::try_from(replay_backlog).unwrap_or(u32::MAX),
            dead_letter_count: u64::try_from(dead_letters.len()).unwrap_or(u64::MAX),
        };

        Ok(GetRunStatusResponse { run_status })
    }

    pub async fn list_assignments(
        &self,
        request: ListAssignmentsRequest,
    ) -> Result<ListAssignmentsResponse> {
        validate_pipeline_scope(&request.tenant_id, &request.pipeline_id)?;
        let assignments = self
            .repository
            .list_assignments(&request.tenant_id, &request.pipeline_id)
            .await?;
        Ok(ListAssignmentsResponse { assignments })
    }

    pub async fn list_replay_jobs(
        &self,
        request: ListReplayJobsRequest,
    ) -> Result<ListReplayJobsResponse> {
        validate_pipeline_scope(&request.tenant_id, &request.pipeline_id)?;
        let replay_jobs = self
            .repository
            .list_replay_jobs(&request.tenant_id, &request.pipeline_id)
            .await?;
        Ok(ListReplayJobsResponse { replay_jobs })
    }

    pub async fn get_checkpoint_history(
        &self,
        request: GetCheckpointHistoryRequest,
    ) -> Result<GetCheckpointHistoryResponse> {
        validate_pipeline_scope(&request.tenant_id, &request.pipeline_id)?;
        let checkpoints = self
            .repository
            .list_checkpoints(&request.tenant_id, &request.pipeline_id)
            .await?;
        Ok(GetCheckpointHistoryResponse { checkpoints })
    }

    pub async fn list_dead_letters(
        &self,
        request: ListDeadLettersRequest,
    ) -> Result<ListDeadLettersResponse> {
        validate_pipeline_scope(&request.tenant_id, &request.pipeline_id)?;
        let dead_letters = self
            .repository
            .list_dead_letters(&request.tenant_id, &request.pipeline_id)
            .await?;
        Ok(ListDeadLettersResponse { dead_letters })
    }
}

fn validate_pipeline_scope(tenant_id: &str, pipeline_id: &str) -> Result<()> {
    if tenant_id.trim().is_empty() {
        bail!("tenant id must not be empty");
    }
    if pipeline_id.trim().is_empty() {
        bail!("pipeline id must not be empty");
    }
    Ok(())
}

fn latest_checkpoint_offsets(checkpoints: &[CheckpointSummary]) -> BTreeMap<u32, u64> {
    let mut latest = BTreeMap::new();
    for checkpoint in checkpoints {
        latest
            .entry(checkpoint.partition_id)
            .and_modify(|offset: &mut u64| *offset = (*offset).max(checkpoint.offset))
            .or_insert(checkpoint.offset);
    }
    latest
}

fn derive_run_status(
    pipeline: &PipelineSpec,
    assignments: &[PartitionAssignment],
    replay_backlog: usize,
) -> RunStatus {
    match pipeline.deployment_state {
        DeploymentState::Draft => RunStatus::Pending,
        DeploymentState::Validated => RunStatus::Pending,
        DeploymentState::Deploying => RunStatus::Deploying,
        DeploymentState::Paused => RunStatus::Paused,
        DeploymentState::Failed => RunStatus::Failed,
        DeploymentState::Running => {
            if replay_backlog > 0 {
                RunStatus::Rebalancing
            } else if assignments.is_empty() {
                RunStatus::Pending
            } else {
                RunStatus::Running
            }
        }
    }
}

fn current_epoch_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs())
}

#[derive(Default)]
pub struct InMemoryQueryRepository {
    pipelines: RwLock<BTreeMap<(String, String), PipelineSpec>>,
    assignments: RwLock<BTreeMap<(String, String), Vec<PartitionAssignment>>>,
    workers: RwLock<BTreeMap<(String, String), Vec<WorkerRecord>>>,
    replay_jobs: RwLock<BTreeMap<(String, String), Vec<ReplayJob>>>,
    checkpoints: RwLock<BTreeMap<(String, String), Vec<CheckpointSummary>>>,
    dead_letters: RwLock<BTreeMap<(String, String), Vec<DeadLetterRecord>>>,
    high_watermarks: RwLock<BTreeMap<(String, String), Vec<PartitionHighWatermark>>>,
}

#[async_trait]
impl QueryRepository for InMemoryQueryRepository {
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
        Ok(self
            .assignments
            .read()
            .await
            .get(&(tenant_id.to_owned(), pipeline_id.to_owned()))
            .cloned()
            .unwrap_or_default())
    }

    async fn list_workers_for_pipeline(
        &self,
        tenant_id: &str,
        pipeline_id: &str,
    ) -> Result<Vec<WorkerRecord>> {
        Ok(self
            .workers
            .read()
            .await
            .get(&(tenant_id.to_owned(), pipeline_id.to_owned()))
            .cloned()
            .unwrap_or_default())
    }

    async fn list_replay_jobs(&self, tenant_id: &str, pipeline_id: &str) -> Result<Vec<ReplayJob>> {
        Ok(self
            .replay_jobs
            .read()
            .await
            .get(&(tenant_id.to_owned(), pipeline_id.to_owned()))
            .cloned()
            .unwrap_or_default())
    }

    async fn list_checkpoints(
        &self,
        tenant_id: &str,
        pipeline_id: &str,
    ) -> Result<Vec<CheckpointSummary>> {
        Ok(self
            .checkpoints
            .read()
            .await
            .get(&(tenant_id.to_owned(), pipeline_id.to_owned()))
            .cloned()
            .unwrap_or_default())
    }

    async fn list_dead_letters(
        &self,
        tenant_id: &str,
        pipeline_id: &str,
    ) -> Result<Vec<DeadLetterRecord>> {
        Ok(self
            .dead_letters
            .read()
            .await
            .get(&(tenant_id.to_owned(), pipeline_id.to_owned()))
            .cloned()
            .unwrap_or_default())
    }

    async fn list_partition_high_watermarks(
        &self,
        tenant_id: &str,
        pipeline_id: &str,
    ) -> Result<Vec<PartitionHighWatermark>> {
        Ok(self
            .high_watermarks
            .read()
            .await
            .get(&(tenant_id.to_owned(), pipeline_id.to_owned()))
            .cloned()
            .unwrap_or_default())
    }
}

#[cfg(test)]
mod tests {
    use super::{InMemoryQueryRepository, PartitionHighWatermark, QueryService, QuerySettings};
    use eventide_types::{
        CheckpointSummary, DeadLetterRecord, DeploymentConfig, DeploymentState, EventEncoding,
        GetCheckpointHistoryRequest, GetRunStatusRequest, ListAssignmentsRequest,
        ListDeadLettersRequest, ListReplayJobsRequest, PartitionAssignment, PipelineSpec,
        ReplayJob, ReplayJobStatus, ReplayPolicy, RunStatus, SinkKind, SinkSpec, SourceTopic,
        WorkerRecord, WorkerStatus,
    };
    use std::sync::Arc;

    fn fixed_clock() -> u64 {
        2_000
    }

    fn pipeline() -> PipelineSpec {
        PipelineSpec {
            tenant_id: String::from("tenant-a"),
            pipeline_id: String::from("pipeline-a"),
            version: 3,
            sources: vec![SourceTopic {
                source_id: String::from("orders"),
                topic_name: String::from("orders.v1"),
                partition_count: 2,
                partition_key: String::from("account_id"),
                encoding: EventEncoding::Json,
            }],
            operators: Vec::new(),
            sinks: vec![SinkSpec {
                sink_id: String::from("sink"),
                upstream_id: String::from("orders"),
                kind: SinkKind::MaterializedView {
                    view_name: String::from("orders_mv"),
                },
            }],
            deployment: DeploymentConfig {
                parallelism: 2,
                checkpoint_interval_secs: 15,
                max_in_flight_messages: 128,
            },
            replay_policy: ReplayPolicy {
                allow_manual_replay: true,
                retention_hours: 24,
            },
            deployment_state: DeploymentState::Running,
        }
    }

    #[tokio::test]
    async fn derives_run_status_from_runtime_state() {
        let repository = Arc::new(InMemoryQueryRepository::default());
        repository.pipelines.write().await.insert(
            (String::from("tenant-a"), String::from("pipeline-a")),
            pipeline(),
        );
        repository.assignments.write().await.insert(
            (String::from("tenant-a"), String::from("pipeline-a")),
            vec![PartitionAssignment {
                tenant_id: String::from("tenant-a"),
                pipeline_id: String::from("pipeline-a"),
                version: 3,
                partition_id: 0,
                worker_id: String::from("worker-a"),
                lease_epoch: 9,
            }],
        );
        repository.workers.write().await.insert(
            (String::from("tenant-a"), String::from("pipeline-a")),
            vec![WorkerRecord {
                worker_id: String::from("worker-a"),
                endpoint: String::from("processor://worker-a"),
                availability_zone: String::from("zone-a"),
                max_assignments: 8,
                labels: vec![String::from("processor")],
                status: WorkerStatus::Ready,
                active_assignments: 1,
                registered_at_epoch_secs: 1_900,
                last_heartbeat_at_epoch_secs: 1_990,
            }],
        );
        repository.replay_jobs.write().await.insert(
            (String::from("tenant-a"), String::from("pipeline-a")),
            vec![ReplayJob {
                replay_job_id: String::from("replay-1"),
                tenant_id: String::from("tenant-a"),
                pipeline_id: String::from("pipeline-a"),
                version: 3,
                from_offset: 10,
                to_offset: Some(20),
                reason: String::from("backfill"),
                status: ReplayJobStatus::Running,
                claimed_by_worker_id: Some(String::from("worker-a")),
                last_processed_offset: Some(15),
                error_message: None,
                created_at_epoch_secs: 1_950,
                updated_at_epoch_secs: 1_999,
            }],
        );
        repository.checkpoints.write().await.insert(
            (String::from("tenant-a"), String::from("pipeline-a")),
            vec![
                CheckpointSummary {
                    partition_id: 0,
                    offset: 100,
                    snapshot_uri: String::from("snapshot://0/100"),
                    snapshot_version: 1,
                    created_at_epoch_secs: 1_990,
                },
                CheckpointSummary {
                    partition_id: 1,
                    offset: 92,
                    snapshot_uri: String::from("snapshot://1/92"),
                    snapshot_version: 1,
                    created_at_epoch_secs: 1_980,
                },
            ],
        );
        repository.dead_letters.write().await.insert(
            (String::from("tenant-a"), String::from("pipeline-a")),
            vec![DeadLetterRecord {
                source_id: String::from("orders"),
                partition_id: 1,
                event_offset: 88,
                record_key: String::from("order-88"),
                failure_reason: String::from("bad payload"),
                retryable: false,
                created_at_epoch_secs: 1_970,
            }],
        );
        repository.high_watermarks.write().await.insert(
            (String::from("tenant-a"), String::from("pipeline-a")),
            vec![
                PartitionHighWatermark {
                    partition_id: 0,
                    max_offset: 108,
                },
                PartitionHighWatermark {
                    partition_id: 1,
                    max_offset: 100,
                },
            ],
        );

        let service = QueryService::with_clock(repository, QuerySettings::default(), fixed_clock);
        let response = service
            .get_run_status(GetRunStatusRequest {
                tenant_id: String::from("tenant-a"),
                pipeline_id: String::from("pipeline-a"),
            })
            .await
            .expect("run status should succeed");

        assert_eq!(response.run_status.state, RunStatus::Rebalancing);
        assert_eq!(response.run_status.consumer_lag, 16);
        assert_eq!(response.run_status.checkpoint_age_secs, 10);
        assert_eq!(response.run_status.active_assignment_count, 1);
        assert_eq!(response.run_status.ready_worker_count, 1);
        assert_eq!(response.run_status.replay_backlog, 1);
        assert_eq!(response.run_status.dead_letter_count, 1);
    }

    #[tokio::test]
    async fn lists_runtime_artifacts() {
        let repository = Arc::new(InMemoryQueryRepository::default());
        repository.assignments.write().await.insert(
            (String::from("tenant-a"), String::from("pipeline-a")),
            vec![PartitionAssignment {
                tenant_id: String::from("tenant-a"),
                pipeline_id: String::from("pipeline-a"),
                version: 3,
                partition_id: 0,
                worker_id: String::from("worker-a"),
                lease_epoch: 9,
            }],
        );
        repository.replay_jobs.write().await.insert(
            (String::from("tenant-a"), String::from("pipeline-a")),
            vec![ReplayJob {
                replay_job_id: String::from("replay-1"),
                tenant_id: String::from("tenant-a"),
                pipeline_id: String::from("pipeline-a"),
                version: 3,
                from_offset: 10,
                to_offset: None,
                reason: String::from("backfill"),
                status: ReplayJobStatus::Pending,
                claimed_by_worker_id: None,
                last_processed_offset: None,
                error_message: None,
                created_at_epoch_secs: 1_950,
                updated_at_epoch_secs: 1_950,
            }],
        );
        repository.checkpoints.write().await.insert(
            (String::from("tenant-a"), String::from("pipeline-a")),
            vec![CheckpointSummary {
                partition_id: 0,
                offset: 100,
                snapshot_uri: String::from("snapshot://0/100"),
                snapshot_version: 1,
                created_at_epoch_secs: 1_990,
            }],
        );
        repository.dead_letters.write().await.insert(
            (String::from("tenant-a"), String::from("pipeline-a")),
            vec![DeadLetterRecord {
                source_id: String::from("orders"),
                partition_id: 1,
                event_offset: 88,
                record_key: String::from("order-88"),
                failure_reason: String::from("bad payload"),
                retryable: false,
                created_at_epoch_secs: 1_970,
            }],
        );

        let service = QueryService::with_clock(repository, QuerySettings::default(), fixed_clock);

        let assignments = service
            .list_assignments(ListAssignmentsRequest {
                tenant_id: String::from("tenant-a"),
                pipeline_id: String::from("pipeline-a"),
            })
            .await
            .expect("assignments should succeed");
        let replay_jobs = service
            .list_replay_jobs(ListReplayJobsRequest {
                tenant_id: String::from("tenant-a"),
                pipeline_id: String::from("pipeline-a"),
            })
            .await
            .expect("replay listing should succeed");
        let checkpoints = service
            .get_checkpoint_history(GetCheckpointHistoryRequest {
                tenant_id: String::from("tenant-a"),
                pipeline_id: String::from("pipeline-a"),
            })
            .await
            .expect("checkpoints should succeed");
        let dead_letters = service
            .list_dead_letters(ListDeadLettersRequest {
                tenant_id: String::from("tenant-a"),
                pipeline_id: String::from("pipeline-a"),
            })
            .await
            .expect("dead letters should succeed");

        assert_eq!(assignments.assignments.len(), 1);
        assert_eq!(replay_jobs.replay_jobs.len(), 1);
        assert_eq!(checkpoints.checkpoints.len(), 1);
        assert_eq!(dead_letters.dead_letters.len(), 1);
    }
}
