#![allow(clippy::module_name_repetitions)]

mod auth;
mod contracts;
mod pipeline;
mod service;

pub use auth::{
    AuthenticatedPrincipal, PlatformRole, RoleBinding, ServiceAccount, SubjectKind, TenantRecord,
};
pub use contracts::{
    AssignRoleBindingRequest, AssignRoleBindingResponse, CheckpointSummary, CreatePipelineRequest,
    CreatePipelineResponse, CreateServiceAccountRequest, CreateServiceAccountResponse,
    CreateTenantRequest, CreateTenantResponse, DeadLetterRecord, DeployPipelineRequest,
    DeployPipelineResponse, GetCheckpointHistoryRequest, GetCheckpointHistoryResponse,
    GetPipelineRequest, GetPipelineResponse, GetReplayJobRequest, GetReplayJobResponse,
    GetRunStatusRequest, GetRunStatusResponse, GetTenantRequest, GetTenantResponse,
    GetTopicRequest, GetTopicResponse, ListAssignmentsRequest, ListAssignmentsResponse,
    ListDeadLettersRequest, ListDeadLettersResponse, ListPipelinesRequest, ListPipelinesResponse,
    ListRoleBindingsRequest, ListRoleBindingsResponse, ListServiceAccountsRequest,
    ListServiceAccountsResponse, ListTenantsRequest, ListTenantsResponse, ListTopicsRequest,
    ListTopicsResponse, PartitionAssignment, PausePipelineRequest, PausePipelineResponse,
    PipelineRunStatus, PipelineSummary, RegisterTopicRequest, RegisterTopicResponse,
    RegisteredTopic, ReplayJob, ReplayJobStatus, RequestReplayRequest, RequestReplayResponse,
    ResumePipelineRequest, ResumePipelineResponse, RunStatus, TenantSummary, TopicSummary,
    UpdatePipelineVersionRequest, UpdatePipelineVersionResponse,
};
pub use pipeline::{
    AggregateFunction, DeploymentConfig, DeploymentState, EventEncoding, JoinKind, OperatorKind,
    OperatorNode, PipelineSpec, PipelineValidationError, PipelineValidationIssue, ReplayPolicy,
    SinkKind, SinkSpec, SourceTopic, WindowKind, WindowSpec,
};
pub use service::ServiceName;
