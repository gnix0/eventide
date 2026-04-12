use crate::auth::{RoleBinding, ServiceAccount, TenantRecord};
use crate::coordinator::{WorkerRecord, WorkerStatus};
use crate::pipeline::{DeploymentState, PipelineSpec};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreatePipelineRequest {
    pub pipeline: PipelineSpec,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreatePipelineResponse {
    pub pipeline: PipelineSummary,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UpdatePipelineVersionRequest {
    pub pipeline: PipelineSpec,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UpdatePipelineVersionResponse {
    pub pipeline: PipelineSummary,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GetPipelineRequest {
    pub tenant_id: String,
    pub pipeline_id: String,
    pub version: Option<u32>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GetPipelineResponse {
    pub pipeline: PipelineSpec,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ListPipelinesRequest {
    pub tenant_id: Option<String>,
    pub deployment_state: Option<DeploymentState>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ListPipelinesResponse {
    pub pipelines: Vec<PipelineSummary>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeployPipelineRequest {
    pub tenant_id: String,
    pub pipeline_id: String,
    pub version: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeployPipelineResponse {
    pub deployment_state: DeploymentState,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PausePipelineRequest {
    pub tenant_id: String,
    pub pipeline_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PausePipelineResponse {
    pub deployment_state: DeploymentState,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResumePipelineRequest {
    pub tenant_id: String,
    pub pipeline_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResumePipelineResponse {
    pub deployment_state: DeploymentState,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestReplayRequest {
    pub tenant_id: String,
    pub pipeline_id: String,
    pub from_offset: u64,
    pub to_offset: Option<u64>,
    pub reason: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestReplayResponse {
    pub replay_job: ReplayJob,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GetReplayJobRequest {
    pub tenant_id: String,
    pub replay_job_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GetReplayJobResponse {
    pub replay_job: ReplayJob,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegisterTopicRequest {
    pub topic: RegisteredTopic,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegisterTopicResponse {
    pub topic: RegisteredTopic,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateTenantRequest {
    pub tenant: TenantRecord,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateTenantResponse {
    pub tenant: TenantSummary,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GetTenantRequest {
    pub tenant_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GetTenantResponse {
    pub tenant: TenantRecord,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ListTenantsRequest;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ListTenantsResponse {
    pub tenants: Vec<TenantSummary>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AssignRoleBindingRequest {
    pub binding: RoleBinding,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AssignRoleBindingResponse {
    pub binding: RoleBinding,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ListRoleBindingsRequest {
    pub tenant_id: Option<String>,
    pub subject_id: Option<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ListRoleBindingsResponse {
    pub bindings: Vec<RoleBinding>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateServiceAccountRequest {
    pub tenant_id: String,
    pub display_name: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateServiceAccountResponse {
    pub service_account: ServiceAccount,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ListServiceAccountsRequest {
    pub tenant_id: Option<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ListServiceAccountsResponse {
    pub service_accounts: Vec<ServiceAccount>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GetTopicRequest {
    pub tenant_id: String,
    pub topic_name: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GetTopicResponse {
    pub topic: RegisteredTopic,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ListTopicsRequest {
    pub tenant_id: Option<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ListTopicsResponse {
    pub topics: Vec<TopicSummary>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GetRunStatusRequest {
    pub tenant_id: String,
    pub pipeline_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GetRunStatusResponse {
    pub run_status: PipelineRunStatus,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListAssignmentsRequest {
    pub tenant_id: String,
    pub pipeline_id: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ListAssignmentsResponse {
    pub assignments: Vec<PartitionAssignment>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegisterWorkerRequest {
    pub worker: WorkerRecord,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegisterWorkerResponse {
    pub worker: WorkerRecord,
    pub lease_ttl_secs: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HeartbeatWorkerRequest {
    pub worker_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HeartbeatWorkerResponse {
    pub worker: WorkerRecord,
    pub lease_ttl_secs: u32,
    pub assignments: Vec<PartitionAssignment>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ListWorkersRequest {
    pub status: Option<WorkerStatus>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ListWorkersResponse {
    pub workers: Vec<WorkerRecord>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RebalancePipelineRequest {
    pub tenant_id: String,
    pub pipeline_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RebalancePipelineResponse {
    pub version: u32,
    pub lease_epoch: u64,
    pub assignments: Vec<PartitionAssignment>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExpireWorkerLeasesRequest {
    pub stale_after_secs: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExpireWorkerLeasesResponse {
    pub expired_worker_ids: Vec<String>,
    pub revoked_assignment_count: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GetCheckpointHistoryRequest {
    pub tenant_id: String,
    pub pipeline_id: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GetCheckpointHistoryResponse {
    pub checkpoints: Vec<CheckpointSummary>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListDeadLettersRequest {
    pub tenant_id: String,
    pub pipeline_id: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ListDeadLettersResponse {
    pub dead_letters: Vec<DeadLetterRecord>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PipelineSummary {
    pub tenant_id: String,
    pub pipeline_id: String,
    pub version: u32,
    pub deployment_state: DeploymentState,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegisteredTopic {
    pub tenant_id: String,
    pub topic_name: String,
    pub partition_count: u16,
    pub retention_hours: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TopicSummary {
    pub tenant_id: String,
    pub topic_name: String,
    pub partition_count: u16,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TenantSummary {
    pub tenant_id: String,
    pub display_name: String,
    pub oidc_realm: String,
    pub enabled: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PipelineRunStatus {
    pub tenant_id: String,
    pub pipeline_id: String,
    pub state: RunStatus,
    pub consumer_lag: u64,
    pub checkpoint_age_secs: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RunStatus {
    Pending,
    Deploying,
    Running,
    Rebalancing,
    Paused,
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PartitionAssignment {
    pub tenant_id: String,
    pub pipeline_id: String,
    pub version: u32,
    pub partition_id: u32,
    pub worker_id: String,
    pub lease_epoch: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CheckpointSummary {
    pub partition_id: u32,
    pub offset: u64,
    pub snapshot_uri: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeadLetterRecord {
    pub record_key: String,
    pub failure_reason: String,
    pub retryable: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplayJob {
    pub replay_job_id: String,
    pub tenant_id: String,
    pub pipeline_id: String,
    pub status: ReplayJobStatus,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplayJobStatus {
    Pending,
    Running,
    Succeeded,
    Failed,
}
