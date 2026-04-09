#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServiceName {
    ApiGateway,
    ControlPlane,
    Coordinator,
    ProcessorRuntime,
    QueryService,
    SinkWriter,
    StateManager,
}

impl ServiceName {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ApiGateway => "api-gateway",
            Self::ControlPlane => "control-plane",
            Self::Coordinator => "coordinator",
            Self::ProcessorRuntime => "processor-runtime",
            Self::QueryService => "query-service",
            Self::SinkWriter => "sink-writer",
            Self::StateManager => "state-manager",
        }
    }
}
