#![allow(clippy::module_name_repetitions)]

mod auth;
mod contracts;
mod coordinator;
mod pipeline;
mod service;

pub use auth::{
    AuthenticatedPrincipal, PlatformRole, RoleBinding, ServiceAccount, SubjectKind, TenantRecord,
};
pub use contracts::{
    AssignRoleBindingRequest, AssignRoleBindingResponse, CheckpointSummary, CreatePipelineRequest,
    CreatePipelineResponse, CreateServiceAccountRequest, CreateServiceAccountResponse,
    CreateTenantRequest, CreateTenantResponse, DeadLetterRecord, DeployPipelineRequest,
    DeployPipelineResponse, ExpireWorkerLeasesRequest, ExpireWorkerLeasesResponse,
    GetCheckpointHistoryRequest, GetCheckpointHistoryResponse, GetPipelineRequest,
    GetPipelineResponse, GetReplayJobRequest, GetReplayJobResponse, GetRunStatusRequest,
    GetRunStatusResponse, GetTenantRequest, GetTenantResponse, GetTopicRequest, GetTopicResponse,
    HeartbeatWorkerRequest, HeartbeatWorkerResponse, ListAssignmentsRequest,
    ListAssignmentsResponse, ListDeadLettersRequest, ListDeadLettersResponse, ListPipelinesRequest,
    ListPipelinesResponse, ListRoleBindingsRequest, ListRoleBindingsResponse,
    ListServiceAccountsRequest, ListServiceAccountsResponse, ListTenantsRequest,
    ListTenantsResponse, ListTopicsRequest, ListTopicsResponse, ListWorkersRequest,
    ListWorkersResponse, PartitionAssignment, PausePipelineRequest, PausePipelineResponse,
    PipelineRunStatus, PipelineSummary, RebalancePipelineRequest, RebalancePipelineResponse,
    RegisterTopicRequest, RegisterTopicResponse, RegisterWorkerRequest, RegisterWorkerResponse,
    RegisteredTopic, ReplayJob, ReplayJobStatus, RequestReplayRequest, RequestReplayResponse,
    ResumePipelineRequest, ResumePipelineResponse, RunStatus, TenantSummary, TopicSummary,
    UpdatePipelineVersionRequest, UpdatePipelineVersionResponse,
};
pub use coordinator::{WorkerRecord, WorkerStatus};
pub use pipeline::{
    AggregateFunction, DeploymentConfig, DeploymentState, EventEncoding, JoinKind, OperatorKind,
    OperatorNode, PipelineSpec, PipelineValidationError, PipelineValidationIssue, ReplayPolicy,
    SinkKind, SinkSpec, SourceTopic, WindowKind, WindowSpec,
};
pub use service::ServiceName;
