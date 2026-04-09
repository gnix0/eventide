use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::error::Error;
use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PipelineSpec {
    pub tenant_id: String,
    pub pipeline_id: String,
    pub version: u32,
    pub sources: Vec<SourceTopic>,
    pub operators: Vec<OperatorNode>,
    pub sinks: Vec<SinkSpec>,
    pub deployment: DeploymentConfig,
    pub replay_policy: ReplayPolicy,
    pub deployment_state: DeploymentState,
}

impl PipelineSpec {
    pub fn validate(&self) -> Result<(), PipelineValidationError> {
        let mut error = PipelineValidationError::default();

        self.validate_required_fields(&mut error);
        let namespaces = self.validate_namespaces(&mut error);
        let topological_order = self.validate_graph(&namespaces, &mut error);
        self.validate_deployment(&mut error);

        if error.is_empty() {
            self.validate_partitions(&namespaces, &topological_order, &mut error);
        }

        if error.is_empty() { Ok(()) } else { Err(error) }
    }

    fn validate_required_fields(&self, error: &mut PipelineValidationError) {
        if self.tenant_id.trim().is_empty() {
            error.push("tenant_id", "tenant id must not be empty");
        }

        if self.pipeline_id.trim().is_empty() {
            error.push("pipeline_id", "pipeline id must not be empty");
        }

        if self.version == 0 {
            error.push("version", "pipeline version must be greater than zero");
        }

        if self.sources.is_empty() {
            error.push("sources", "at least one source topic is required");
        }

        if self.sinks.is_empty() {
            error.push("sinks", "at least one sink is required");
        }
    }

    fn validate_namespaces(&self, error: &mut PipelineValidationError) -> ValidationNamespaces {
        let mut source_ids = BTreeSet::new();
        let mut operator_ids = BTreeSet::new();
        let mut sink_ids = BTreeSet::new();

        for source in &self.sources {
            if source.source_id.trim().is_empty() {
                error.push("sources.source_id", "source id must not be empty");
            }

            if source.topic_name.trim().is_empty() {
                error.push("sources.topic_name", "source topic name must not be empty");
            }

            if source.partition_count == 0 {
                error.push(
                    "sources.partition_count",
                    "source partition count must be greater than zero",
                );
            }

            if source.partition_key.trim().is_empty() {
                error.push(
                    "sources.partition_key",
                    "source partition key must not be empty",
                );
            }

            if !source_ids.insert(source.source_id.clone()) {
                error.push("sources.source_id", "source ids must be unique");
            }
        }

        for operator in &self.operators {
            if operator.operator_id.trim().is_empty() {
                error.push("operators.operator_id", "operator id must not be empty");
            }

            if !operator_ids.insert(operator.operator_id.clone()) {
                error.push("operators.operator_id", "operator ids must be unique");
            }
        }

        for sink in &self.sinks {
            if sink.sink_id.trim().is_empty() {
                error.push("sinks.sink_id", "sink id must not be empty");
            }

            if !sink_ids.insert(sink.sink_id.clone()) {
                error.push("sinks.sink_id", "sink ids must be unique");
            }
        }

        for source_id in &source_ids {
            if operator_ids.contains(source_id) {
                error.push(
                    "sources.source_id",
                    "source ids must not overlap with operator ids",
                );
            }
        }

        ValidationNamespaces {
            source_ids,
            operator_ids,
        }
    }

    fn validate_graph(
        &self,
        namespaces: &ValidationNamespaces,
        error: &mut PipelineValidationError,
    ) -> Vec<String> {
        let mut indegree: BTreeMap<String, usize> = self
            .operators
            .iter()
            .map(|operator| (operator.operator_id.clone(), 0_usize))
            .collect();
        let mut adjacency: BTreeMap<String, Vec<String>> = self
            .operators
            .iter()
            .map(|operator| (operator.operator_id.clone(), Vec::new()))
            .collect();

        for operator in &self.operators {
            if operator.upstream_ids.is_empty() {
                error.push(
                    "operators.upstream_ids",
                    "operators must declare at least one upstream id",
                );
            }

            let mut seen_upstream_ids = BTreeSet::new();
            for upstream_id in &operator.upstream_ids {
                if !seen_upstream_ids.insert(upstream_id.clone()) {
                    error.push(
                        "operators.upstream_ids",
                        "operators must not declare duplicate upstream ids",
                    );
                }

                if namespaces.source_ids.contains(upstream_id) {
                    continue;
                }

                if namespaces.operator_ids.contains(upstream_id) {
                    if let Some(next) = adjacency.get_mut(upstream_id) {
                        next.push(operator.operator_id.clone());
                    }

                    if let Some(entry) = indegree.get_mut(&operator.operator_id) {
                        *entry += 1;
                    }

                    continue;
                }

                error.push(
                    "operators.upstream_ids",
                    "operator upstream ids must reference known sources or operators",
                );
            }
        }

        let mut ready = VecDeque::new();
        for (operator_id, count) in &indegree {
            if *count == 0 {
                ready.push_back(operator_id.clone());
            }
        }

        let mut topological_order = Vec::with_capacity(self.operators.len());
        while let Some(operator_id) = ready.pop_front() {
            topological_order.push(operator_id.clone());

            if let Some(children) = adjacency.get(&operator_id) {
                for child in children {
                    if let Some(count) = indegree.get_mut(child) {
                        *count -= 1;
                        if *count == 0 {
                            ready.push_back(child.clone());
                        }
                    }
                }
            }
        }

        if topological_order.len() != self.operators.len() {
            error.push(
                "operators",
                "operator graph must be acyclic and fully connected from declared upstreams",
            );
        }

        for sink in &self.sinks {
            if !namespaces.source_ids.contains(&sink.upstream_id)
                && !namespaces.operator_ids.contains(&sink.upstream_id)
            {
                error.push(
                    "sinks.upstream_id",
                    "sink upstream ids must reference known sources or operators",
                );
            }
        }

        topological_order
    }

    fn validate_deployment(&self, error: &mut PipelineValidationError) {
        if self.deployment.parallelism == 0 {
            error.push(
                "deployment.parallelism",
                "deployment parallelism must be greater than zero",
            );
        }

        if self.deployment.checkpoint_interval_secs == 0 {
            error.push(
                "deployment.checkpoint_interval_secs",
                "checkpoint interval must be greater than zero",
            );
        }

        if self.deployment.max_in_flight_messages == 0 {
            error.push(
                "deployment.max_in_flight_messages",
                "max in-flight messages must be greater than zero",
            );
        }
    }

    fn validate_partitions(
        &self,
        _namespaces: &ValidationNamespaces,
        topological_order: &[String],
        error: &mut PipelineValidationError,
    ) {
        let mut outputs = BTreeMap::new();
        let operators_by_id: BTreeMap<_, _> = self
            .operators
            .iter()
            .map(|operator| (operator.operator_id.clone(), operator))
            .collect();

        for source in &self.sources {
            outputs.insert(
                source.source_id.clone(),
                EffectivePartitioning {
                    partition_count: source.partition_count,
                    partition_key: source.partition_key.clone(),
                },
            );
        }

        for operator_id in topological_order {
            let Some(operator) = operators_by_id.get(operator_id) else {
                continue;
            };

            let upstream_outputs: Vec<_> = operator
                .upstream_ids
                .iter()
                .filter_map(|id| outputs.get(id).cloned())
                .collect();

            match operator.partitioning_output(&upstream_outputs) {
                Ok(output) => {
                    outputs.insert(operator.operator_id.clone(), output);
                }
                Err(message) => {
                    error.push("operators", &message);
                }
            }
        }

        for sink in &self.sinks {
            let Some(upstream) = outputs.get(&sink.upstream_id) else {
                continue;
            };

            match &sink.kind {
                SinkKind::Kafka {
                    topic_name,
                    partition_count,
                    partition_key,
                } => {
                    if topic_name.trim().is_empty() {
                        error.push(
                            "sinks.kind.kafka.topic_name",
                            "Kafka sink topic name is required",
                        );
                    }

                    if *partition_count != upstream.partition_count {
                        error.push(
                            "sinks.kind.kafka.partition_count",
                            "Kafka sink partition count must match upstream partition count",
                        );
                    }

                    if partition_key != &upstream.partition_key {
                        error.push(
                            "sinks.kind.kafka.partition_key",
                            "Kafka sink partition key must match upstream partition key",
                        );
                    }
                }
                SinkKind::MaterializedView { view_name } => {
                    if view_name.trim().is_empty() {
                        error.push(
                            "sinks.kind.materialized_view.view_name",
                            "materialized view sink name is required",
                        );
                    }
                }
                SinkKind::ObjectStorage { bucket, prefix } => {
                    if bucket.trim().is_empty() {
                        error.push(
                            "sinks.kind.object_storage.bucket",
                            "object storage sink bucket is required",
                        );
                    }

                    if prefix.trim().is_empty() {
                        error.push(
                            "sinks.kind.object_storage.prefix",
                            "object storage sink prefix is required",
                        );
                    }
                }
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceTopic {
    pub source_id: String,
    pub topic_name: String,
    pub partition_count: u16,
    pub partition_key: String,
    pub encoding: EventEncoding,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OperatorNode {
    pub operator_id: String,
    pub upstream_ids: Vec<String>,
    pub kind: OperatorKind,
}

impl OperatorNode {
    fn partitioning_output(
        &self,
        upstream_outputs: &[EffectivePartitioning],
    ) -> Result<EffectivePartitioning, String> {
        match &self.kind {
            OperatorKind::Filter { expression } => {
                if expression.trim().is_empty() {
                    return Err(String::from(
                        "filter operators require a non-empty expression",
                    ));
                }

                single_input_output(upstream_outputs, "filter operators")
            }
            OperatorKind::Map { projection } => {
                if projection.trim().is_empty() {
                    return Err(String::from("map operators require a non-empty projection"));
                }

                single_input_output(upstream_outputs, "map operators")
            }
            OperatorKind::KeyBy {
                key_field,
                partition_count,
            } => {
                if key_field.trim().is_empty() {
                    return Err(String::from(
                        "keyBy operators require a non-empty key field",
                    ));
                }

                if *partition_count == 0 {
                    return Err(String::from(
                        "keyBy operators require a non-zero partition count",
                    ));
                }

                require_upstream_count(upstream_outputs, 1, "keyBy operators")?;
                Ok(EffectivePartitioning {
                    partition_count: *partition_count,
                    partition_key: key_field.clone(),
                })
            }
            OperatorKind::Window(window_spec) => {
                validate_window_spec(window_spec)?;
                single_input_output(upstream_outputs, "window operators")
            }
            OperatorKind::Aggregate {
                function: _,
                state_ttl_secs,
            } => {
                if *state_ttl_secs == 0 {
                    return Err(String::from(
                        "aggregate operators require a positive state ttl",
                    ));
                }

                single_input_output(upstream_outputs, "aggregate operators")
            }
            OperatorKind::Join {
                kind: _,
                key_field,
                within_secs,
            } => {
                if key_field.trim().is_empty() {
                    return Err(String::from("join operators require a non-empty key field"));
                }

                if *within_secs == 0 {
                    return Err(String::from(
                        "join operators require a positive within window",
                    ));
                }

                require_upstream_count(upstream_outputs, 2, "join operators")?;
                let left = &upstream_outputs[0];
                let right = &upstream_outputs[1];

                if left.partition_count != right.partition_count {
                    return Err(String::from(
                        "join operators require upstream partition counts to match",
                    ));
                }

                if left.partition_key != *key_field || right.partition_key != *key_field {
                    return Err(String::from(
                        "join operators require both upstreams to be partitioned by the join key",
                    ));
                }

                Ok(EffectivePartitioning {
                    partition_count: left.partition_count,
                    partition_key: key_field.clone(),
                })
            }
            OperatorKind::Enrich {
                table_name,
                key_field,
            } => {
                if table_name.trim().is_empty() {
                    return Err(String::from("enrich operators require a lookup table name"));
                }

                if key_field.trim().is_empty() {
                    return Err(String::from(
                        "enrich operators require a non-empty key field",
                    ));
                }

                let output = single_input_output(upstream_outputs, "enrich operators")?;
                if output.partition_key != *key_field {
                    return Err(String::from(
                        "enrich operators require the upstream partition key to match the lookup key",
                    ));
                }

                Ok(output)
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OperatorKind {
    Filter {
        expression: String,
    },
    Map {
        projection: String,
    },
    KeyBy {
        key_field: String,
        partition_count: u16,
    },
    Window(WindowSpec),
    Aggregate {
        function: AggregateFunction,
        state_ttl_secs: u32,
    },
    Join {
        kind: JoinKind,
        key_field: String,
        within_secs: u32,
    },
    Enrich {
        table_name: String,
        key_field: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WindowSpec {
    pub kind: WindowKind,
    pub size_secs: u32,
    pub slide_secs: Option<u32>,
    pub grace_secs: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WindowKind {
    Tumbling,
    Sliding,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AggregateFunction {
    Count,
    Sum,
    Min,
    Max,
    Average,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JoinKind {
    Inner,
    Left,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EventEncoding {
    Json,
    Avro,
    Protobuf,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SinkSpec {
    pub sink_id: String,
    pub upstream_id: String,
    pub kind: SinkKind,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SinkKind {
    Kafka {
        topic_name: String,
        partition_count: u16,
        partition_key: String,
    },
    MaterializedView {
        view_name: String,
    },
    ObjectStorage {
        bucket: String,
        prefix: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeploymentConfig {
    pub parallelism: u16,
    pub checkpoint_interval_secs: u32,
    pub max_in_flight_messages: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplayPolicy {
    pub allow_manual_replay: bool,
    pub retention_hours: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeploymentState {
    Draft,
    Validated,
    Deploying,
    Running,
    Paused,
    Failed,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PipelineValidationError {
    pub issues: Vec<PipelineValidationIssue>,
}

impl PipelineValidationError {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.issues.is_empty()
    }

    pub fn push(&mut self, field: &str, message: &str) {
        self.issues.push(PipelineValidationIssue {
            field: field.to_owned(),
            message: message.to_owned(),
        });
    }
}

impl fmt::Display for PipelineValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for issue in &self.issues {
            writeln!(formatter, "{}: {}", issue.field, issue.message)?;
        }

        Ok(())
    }
}

impl Error for PipelineValidationError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PipelineValidationIssue {
    pub field: String,
    pub message: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ValidationNamespaces {
    source_ids: BTreeSet<String>,
    operator_ids: BTreeSet<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct EffectivePartitioning {
    partition_count: u16,
    partition_key: String,
}

fn validate_window_spec(window_spec: &WindowSpec) -> Result<(), String> {
    if window_spec.size_secs == 0 {
        return Err(String::from(
            "window operators require a positive window size",
        ));
    }

    if matches!(window_spec.kind, WindowKind::Sliding) {
        let Some(slide_secs) = window_spec.slide_secs else {
            return Err(String::from(
                "sliding windows require a slide interval to be configured",
            ));
        };

        if slide_secs == 0 || slide_secs > window_spec.size_secs {
            return Err(String::from(
                "sliding windows require a positive slide interval not greater than the window size",
            ));
        }
    }

    Ok(())
}

fn require_upstream_count(
    upstream_outputs: &[EffectivePartitioning],
    expected_count: usize,
    operator_name: &str,
) -> Result<(), String> {
    if upstream_outputs.len() == expected_count {
        Ok(())
    } else {
        Err(format!(
            "{operator_name} require exactly {expected_count} upstream(s)"
        ))
    }
}

fn single_input_output(
    upstream_outputs: &[EffectivePartitioning],
    operator_name: &str,
) -> Result<EffectivePartitioning, String> {
    require_upstream_count(upstream_outputs, 1, operator_name)?;
    Ok(upstream_outputs[0].clone())
}

#[cfg(test)]
mod tests {
    use super::{
        AggregateFunction, DeploymentConfig, DeploymentState, EventEncoding, JoinKind,
        OperatorKind, OperatorNode, PipelineSpec, ReplayPolicy, SinkKind, SinkSpec, SourceTopic,
        WindowKind, WindowSpec,
    };

    #[test]
    fn validates_a_partition_compatible_pipeline() {
        let spec = valid_pipeline_spec();
        assert!(spec.validate().is_ok());
    }

    #[test]
    fn rejects_cycles_in_the_operator_graph() {
        let mut spec = valid_pipeline_spec();
        spec.operators = vec![
            OperatorNode {
                operator_id: String::from("transform"),
                upstream_ids: vec![String::from("aggregate")],
                kind: OperatorKind::Map {
                    projection: String::from("project order fields"),
                },
            },
            OperatorNode {
                operator_id: String::from("aggregate"),
                upstream_ids: vec![String::from("transform")],
                kind: OperatorKind::Aggregate {
                    function: AggregateFunction::Count,
                    state_ttl_secs: 300,
                },
            },
        ];

        let error = spec.validate().expect_err("pipeline should be invalid");
        assert!(
            error
                .issues
                .iter()
                .any(|issue| issue.message.contains("acyclic"))
        );
    }

    #[test]
    fn rejects_duplicate_source_ids() {
        let mut spec = valid_pipeline_spec();
        spec.sources.push(SourceTopic {
            source_id: String::from("orders"),
            topic_name: String::from("orders-retry"),
            partition_count: 12,
            partition_key: String::from("account_id"),
            encoding: EventEncoding::Json,
        });

        let error = spec.validate().expect_err("pipeline should be invalid");
        assert!(
            error
                .issues
                .iter()
                .any(|issue| issue.message.contains("source ids must be unique"))
        );
    }

    #[test]
    fn rejects_invalid_sliding_window_configuration() {
        let mut spec = valid_pipeline_spec();
        spec.operators[1].kind = OperatorKind::Window(WindowSpec {
            kind: WindowKind::Sliding,
            size_secs: 60,
            slide_secs: Some(90),
            grace_secs: 10,
        });

        let error = spec.validate().expect_err("pipeline should be invalid");
        assert!(
            error
                .issues
                .iter()
                .any(|issue| issue.message.contains("slide interval"))
        );
    }

    #[test]
    fn rejects_join_partition_mismatches() {
        let mut spec = valid_join_pipeline_spec();
        spec.sources[1].partition_count = 6;

        let error = spec.validate().expect_err("pipeline should be invalid");
        assert!(
            error
                .issues
                .iter()
                .any(|issue| issue.message.contains("partition counts to match"))
        );
    }

    #[test]
    fn rejects_sink_partition_mismatches() {
        let mut spec = valid_pipeline_spec();
        spec.sinks[0].kind = SinkKind::Kafka {
            topic_name: String::from("orders-materialized"),
            partition_count: 6,
            partition_key: String::from("account_id"),
        };

        let error = spec.validate().expect_err("pipeline should be invalid");
        assert!(
            error
                .issues
                .iter()
                .any(|issue| issue.message.contains("Kafka sink partition count"))
        );
    }

    fn valid_pipeline_spec() -> PipelineSpec {
        PipelineSpec {
            tenant_id: String::from("tenant-acme"),
            pipeline_id: String::from("order-throughput"),
            version: 1,
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
                sink_id: String::from("metrics-out"),
                upstream_id: String::from("aggregate"),
                kind: SinkKind::Kafka {
                    topic_name: String::from("orders-materialized"),
                    partition_count: 12,
                    partition_key: String::from("account_id"),
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

    fn valid_join_pipeline_spec() -> PipelineSpec {
        PipelineSpec {
            tenant_id: String::from("tenant-acme"),
            pipeline_id: String::from("settlement-enrichment"),
            version: 3,
            sources: vec![
                SourceTopic {
                    source_id: String::from("payments"),
                    topic_name: String::from("payments"),
                    partition_count: 12,
                    partition_key: String::from("merchant_id"),
                    encoding: EventEncoding::Json,
                },
                SourceTopic {
                    source_id: String::from("disputes"),
                    topic_name: String::from("disputes"),
                    partition_count: 12,
                    partition_key: String::from("merchant_id"),
                    encoding: EventEncoding::Json,
                },
            ],
            operators: vec![OperatorNode {
                operator_id: String::from("join"),
                upstream_ids: vec![String::from("payments"), String::from("disputes")],
                kind: OperatorKind::Join {
                    kind: JoinKind::Inner,
                    key_field: String::from("merchant_id"),
                    within_secs: 600,
                },
            }],
            sinks: vec![SinkSpec {
                sink_id: String::from("snapshot"),
                upstream_id: String::from("join"),
                kind: SinkKind::ObjectStorage {
                    bucket: String::from("analytics-snapshots"),
                    prefix: String::from("settlements/hourly"),
                },
            }],
            deployment: DeploymentConfig {
                parallelism: 12,
                checkpoint_interval_secs: 60,
                max_in_flight_messages: 10_000,
            },
            replay_policy: ReplayPolicy {
                allow_manual_replay: true,
                retention_hours: 720,
            },
            deployment_state: DeploymentState::Validated,
        }
    }
}
