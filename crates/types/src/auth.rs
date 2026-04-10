use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TenantRecord {
    pub tenant_id: String,
    pub display_name: String,
    pub oidc_realm: String,
    pub enabled: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ServiceAccount {
    pub service_account_id: String,
    pub tenant_id: String,
    pub client_id: String,
    pub display_name: String,
    pub enabled: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RoleBinding {
    pub subject_kind: SubjectKind,
    pub subject_id: String,
    pub tenant_id: Option<String>,
    pub role: PlatformRole,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AuthenticatedPrincipal {
    pub subject_kind: SubjectKind,
    pub subject_id: String,
    pub preferred_username: Option<String>,
    pub issuer: String,
    pub audience: Vec<String>,
    pub realm_roles: Vec<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub enum SubjectKind {
    User,
    ServiceAccount,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub enum PlatformRole {
    PlatformAdmin,
    TenantAdmin,
    PipelineEditor,
    PipelineViewer,
    TopicEditor,
    TopicViewer,
    ReplayOperator,
}
