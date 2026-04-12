use anyhow::{Result, anyhow, bail};
use async_trait::async_trait;
use event_pipeline_types::{
    AggregateFunction, CheckpointSummary, DeploymentState, JoinKind, OperatorKind,
    PartitionAssignment, PipelineSpec, SinkKind, WindowKind,
};
use serde_json::{Map, Number, Value, json};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProcessorSettings {
    pub worker_id: String,
    pub batch_size: usize,
}

impl Default for ProcessorSettings {
    fn default() -> Self {
        Self {
            worker_id: String::from("processor-runtime"),
            batch_size: 128,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct PipelineEvent {
    pub tenant_id: String,
    pub pipeline_id: String,
    pub source_id: String,
    pub partition_id: u32,
    pub offset: u64,
    pub record_key: String,
    pub payload: Value,
    pub event_time_epoch_ms: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SinkEmission {
    pub sink_id: String,
    pub partition_id: u32,
    pub offset: u64,
    pub payload: Value,
    pub destination: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CheckpointWrite {
    pub tenant_id: String,
    pub pipeline_id: String,
    pub partition_id: u32,
    pub offset: u64,
    pub snapshot_uri: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ProcessedAssignment {
    pub assignment: PartitionAssignment,
    pub processed_event_count: usize,
    pub start_offset: u64,
    pub checkpoint: Option<CheckpointSummary>,
    pub sink_emissions: Vec<SinkEmission>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AssignmentFailure {
    pub assignment: PartitionAssignment,
    pub reason: String,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct PollProcessorResponse {
    pub processed_assignments: Vec<ProcessedAssignment>,
    pub failed_assignments: Vec<AssignmentFailure>,
}

#[async_trait]
pub trait ProcessorRepository: Send + Sync {
    async fn list_assignments_for_worker(
        &self,
        worker_id: &str,
    ) -> Result<Vec<PartitionAssignment>>;
    async fn get_pipeline_version(
        &self,
        tenant_id: &str,
        pipeline_id: &str,
        version: u32,
    ) -> Result<Option<PipelineSpec>>;
    async fn latest_checkpoint(
        &self,
        tenant_id: &str,
        pipeline_id: &str,
        partition_id: u32,
    ) -> Result<Option<CheckpointSummary>>;
    async fn fetch_partition_events(
        &self,
        tenant_id: &str,
        pipeline_id: &str,
        partition_id: u32,
        offset_gt: u64,
        limit: usize,
    ) -> Result<Vec<PipelineEvent>>;
    async fn append_checkpoint(&self, checkpoint: &CheckpointWrite) -> Result<CheckpointSummary>;
}

#[derive(Clone)]
pub struct ProcessorService {
    repository: Arc<dyn ProcessorRepository>,
    settings: ProcessorSettings,
    state: Arc<RwLock<BTreeMap<AssignmentKey, AssignmentState>>>,
}

impl ProcessorService {
    #[must_use]
    pub fn new(repository: Arc<dyn ProcessorRepository>, settings: ProcessorSettings) -> Self {
        Self {
            repository,
            settings,
            state: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    pub async fn poll_once(&self) -> Result<PollProcessorResponse> {
        let assignments = self
            .repository
            .list_assignments_for_worker(&self.settings.worker_id)
            .await?;

        let mut response = PollProcessorResponse::default();
        for assignment in assignments {
            match self.process_assignment(assignment.clone()).await {
                Ok(processed) => response.processed_assignments.push(processed),
                Err(error) => response.failed_assignments.push(AssignmentFailure {
                    assignment,
                    reason: error.to_string(),
                }),
            }
        }

        Ok(response)
    }

    async fn process_assignment(
        &self,
        assignment: PartitionAssignment,
    ) -> Result<ProcessedAssignment> {
        let pipeline = self
            .repository
            .get_pipeline_version(
                &assignment.tenant_id,
                &assignment.pipeline_id,
                assignment.version,
            )
            .await?
            .ok_or_else(|| anyhow!("pipeline version not found for assignment"))?;

        validate_runtime_supported(&pipeline)?;

        let latest_checkpoint = self
            .repository
            .latest_checkpoint(
                &assignment.tenant_id,
                &assignment.pipeline_id,
                assignment.partition_id,
            )
            .await?;
        let start_offset = latest_checkpoint
            .as_ref()
            .map_or(0, |checkpoint| checkpoint.offset);

        let events = self
            .repository
            .fetch_partition_events(
                &assignment.tenant_id,
                &assignment.pipeline_id,
                assignment.partition_id,
                start_offset,
                self.settings.batch_size,
            )
            .await?;

        if events.is_empty() {
            return Ok(ProcessedAssignment {
                assignment,
                processed_event_count: 0,
                start_offset,
                checkpoint: latest_checkpoint,
                sink_emissions: Vec::new(),
            });
        }

        let sink_emissions = self.execute_events(&assignment, &pipeline, &events).await?;

        let last_offset = events.last().map_or(start_offset, |event| event.offset);
        let checkpoint = self
            .repository
            .append_checkpoint(&CheckpointWrite {
                tenant_id: assignment.tenant_id.clone(),
                pipeline_id: assignment.pipeline_id.clone(),
                partition_id: assignment.partition_id,
                offset: last_offset,
                snapshot_uri: format!(
                    "processor-runtime://worker/{}/pipeline/{}/partition/{}/offset/{}",
                    self.settings.worker_id,
                    assignment.pipeline_id,
                    assignment.partition_id,
                    last_offset
                ),
            })
            .await?;

        Ok(ProcessedAssignment {
            assignment,
            processed_event_count: events.len(),
            start_offset,
            checkpoint: Some(checkpoint),
            sink_emissions,
        })
    }

    async fn execute_events(
        &self,
        assignment: &PartitionAssignment,
        pipeline: &PipelineSpec,
        events: &[PipelineEvent],
    ) -> Result<Vec<SinkEmission>> {
        let mut emissions = Vec::new();
        for event in events {
            let mut values = BTreeMap::new();
            values.insert(event.source_id.clone(), vec![event.payload.clone()]);

            for operator in &pipeline.operators {
                if operator
                    .upstream_ids
                    .iter()
                    .all(|upstream_id| values.contains_key(upstream_id))
                {
                    let upstream_values = operator
                        .upstream_ids
                        .iter()
                        .filter_map(|upstream_id| values.get(upstream_id))
                        .cloned()
                        .collect::<Vec<_>>();
                    if upstream_values.iter().any(Vec::is_empty) {
                        values.insert(operator.operator_id.clone(), Vec::new());
                        continue;
                    }
                    let output = self
                        .apply_operator(
                            assignment,
                            operator.operator_id.as_str(),
                            &operator.kind,
                            &upstream_values,
                            event,
                        )
                        .await?;
                    values.insert(operator.operator_id.clone(), output);
                }
            }

            for sink in &pipeline.sinks {
                let Some(records) = values.get(&sink.upstream_id) else {
                    continue;
                };

                let destination = sink_destination(&sink.kind);
                for payload in records {
                    emissions.push(SinkEmission {
                        sink_id: sink.sink_id.clone(),
                        partition_id: assignment.partition_id,
                        offset: event.offset,
                        payload: payload.clone(),
                        destination: destination.clone(),
                    });
                }
            }
        }

        Ok(emissions)
    }

    async fn apply_operator(
        &self,
        assignment: &PartitionAssignment,
        operator_id: &str,
        kind: &OperatorKind,
        upstream_values: &[Vec<Value>],
        event: &PipelineEvent,
    ) -> Result<Vec<Value>> {
        match kind {
            OperatorKind::Filter { expression } => {
                let value = single_input_value(upstream_values, "filter")?;
                if evaluate_filter(expression, value)? {
                    Ok(vec![value.clone()])
                } else {
                    Ok(Vec::new())
                }
            }
            OperatorKind::Map { projection } => {
                let value = single_input_value(upstream_values, "map")?;
                Ok(vec![project_value(projection, value)?])
            }
            OperatorKind::KeyBy { .. } => {
                let value = single_input_value(upstream_values, "keyBy")?;
                Ok(vec![value.clone()])
            }
            OperatorKind::Window(window_spec) => {
                let value = single_input_value(upstream_values, "window")?;
                Ok(vec![apply_window(
                    window_spec,
                    value,
                    event.event_time_epoch_ms,
                )?])
            }
            OperatorKind::Aggregate { function, .. } => {
                let value = single_input_value(upstream_values, "aggregate")?;
                Ok(vec![
                    self.update_aggregate_state(assignment, operator_id, *function, value)
                        .await?,
                ])
            }
            OperatorKind::Join {
                kind: JoinKind::Inner | JoinKind::Left,
                ..
            } => bail!("join operators are deferred to the stateful-processing branch"),
            OperatorKind::Enrich {
                table_name,
                key_field,
            } => {
                let value = single_input_value(upstream_values, "enrich")?;
                Ok(vec![apply_enrichment(table_name, key_field, value)?])
            }
        }
    }

    async fn update_aggregate_state(
        &self,
        assignment: &PartitionAssignment,
        operator_id: &str,
        function: AggregateFunction,
        value: &Value,
    ) -> Result<Value> {
        let mut state = self.state.write().await;
        let entry = state
            .entry(AssignmentKey::from_assignment(assignment))
            .or_default();
        let aggregate = entry
            .aggregates
            .entry(operator_id.to_owned())
            .or_insert_with(|| AggregateState::new(function));

        aggregate.update(function, value)
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Ord, PartialOrd)]
struct AssignmentKey {
    tenant_id: String,
    pipeline_id: String,
    version: u32,
    partition_id: u32,
}

impl AssignmentKey {
    fn from_assignment(assignment: &PartitionAssignment) -> Self {
        Self {
            tenant_id: assignment.tenant_id.clone(),
            pipeline_id: assignment.pipeline_id.clone(),
            version: assignment.version,
            partition_id: assignment.partition_id,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
struct AssignmentState {
    aggregates: BTreeMap<String, AggregateState>,
}

#[derive(Clone, Debug, PartialEq)]
enum AggregateState {
    Count(u64),
    Numeric {
        count: u64,
        sum: f64,
        min: f64,
        max: f64,
    },
}

impl AggregateState {
    fn new(function: AggregateFunction) -> Self {
        match function {
            AggregateFunction::Count => Self::Count(0),
            AggregateFunction::Sum
            | AggregateFunction::Min
            | AggregateFunction::Max
            | AggregateFunction::Average => Self::Numeric {
                count: 0,
                sum: 0.0,
                min: f64::INFINITY,
                max: f64::NEG_INFINITY,
            },
        }
    }

    fn update(&mut self, function: AggregateFunction, value: &Value) -> Result<Value> {
        match (function, self) {
            (AggregateFunction::Count, Self::Count(count)) => {
                *count = count.saturating_add(1);
                Ok(json!({ "count": *count }))
            }
            (
                AggregateFunction::Sum
                | AggregateFunction::Min
                | AggregateFunction::Max
                | AggregateFunction::Average,
                Self::Numeric {
                    count,
                    sum,
                    min,
                    max,
                },
            ) => {
                let numeric = numeric_value(value)?;
                *count = count.saturating_add(1);
                *sum += numeric;
                *min = min.min(numeric);
                *max = max.max(numeric);

                let aggregate_value = match function {
                    AggregateFunction::Sum => *sum,
                    AggregateFunction::Min => *min,
                    AggregateFunction::Max => *max,
                    AggregateFunction::Average => *sum / f64::from(*count as u32),
                    AggregateFunction::Count => unreachable!("count handled above"),
                };

                Ok(json!({
                    "count": *count,
                    "value": aggregate_value,
                }))
            }
            _ => bail!("aggregate state does not match aggregate function"),
        }
    }
}

fn validate_runtime_supported(pipeline: &PipelineSpec) -> Result<()> {
    match pipeline.deployment_state {
        DeploymentState::Validated | DeploymentState::Deploying | DeploymentState::Running => {}
        DeploymentState::Draft => bail!("pipeline draft is not eligible for processor execution"),
        DeploymentState::Paused => bail!("pipeline is paused"),
        DeploymentState::Failed => bail!("pipeline is failed"),
    }

    if pipeline
        .operators
        .iter()
        .any(|operator| matches!(operator.kind, OperatorKind::Join { .. }))
    {
        bail!("join operators are deferred to the stateful-processing branch");
    }

    Ok(())
}

fn single_input_value<'a>(
    upstream_values: &'a [Vec<Value>],
    operator_name: &str,
) -> Result<&'a Value> {
    let Some(values) = upstream_values.first() else {
        bail!("{operator_name} operators require one upstream");
    };
    let Some(value) = values.first() else {
        bail!("{operator_name} operators received an empty upstream batch");
    };
    Ok(value)
}

fn evaluate_filter(expression: &str, value: &Value) -> Result<bool> {
    let expression = expression.trim();
    if expression.eq_ignore_ascii_case("true") {
        return Ok(true);
    }
    if expression.eq_ignore_ascii_case("false") {
        return Ok(false);
    }
    if let Some(field) = expression
        .strip_prefix("exists(")
        .and_then(|remaining| remaining.strip_suffix(')'))
    {
        return Ok(lookup_field(value, field.trim()).is_some());
    }

    let parts = expression.split_whitespace().collect::<Vec<_>>();
    if parts.len() != 3 {
        bail!("unsupported filter expression: {expression}");
    }

    let field_value =
        lookup_field(value, parts[0]).ok_or_else(|| anyhow!("missing field {}", parts[0]))?;
    let literal = parse_literal(parts[2]);

    match parts[1] {
        "==" => Ok(field_value == &literal),
        "!=" => Ok(field_value != &literal),
        ">" => compare_numeric(field_value, &literal, |left, right| left > right),
        ">=" => compare_numeric(field_value, &literal, |left, right| left >= right),
        "<" => compare_numeric(field_value, &literal, |left, right| left < right),
        "<=" => compare_numeric(field_value, &literal, |left, right| left <= right),
        _ => bail!("unsupported filter operator: {}", parts[1]),
    }
}

fn compare_numeric<F>(left: &Value, right: &Value, comparison: F) -> Result<bool>
where
    F: FnOnce(f64, f64) -> bool,
{
    Ok(comparison(numeric_value(left)?, numeric_value(right)?))
}

fn parse_literal(input: &str) -> Value {
    if let Ok(boolean) = input.parse::<bool>() {
        return Value::Bool(boolean);
    }
    if let Ok(number) = input.parse::<f64>() {
        return Number::from_f64(number).map_or(Value::Null, Value::Number);
    }
    Value::String(input.trim_matches('"').to_owned())
}

fn project_value(projection: &str, value: &Value) -> Result<Value> {
    let projection = projection.trim();
    if projection == "*" {
        return Ok(value.clone());
    }

    let mut object = Map::new();
    for selector in projection
        .split(',')
        .map(str::trim)
        .filter(|selector| !selector.is_empty())
    {
        let parts = selector.split_whitespace().collect::<Vec<_>>();
        let (field, alias) = match parts.as_slice() {
            [field] => (*field, *field),
            [field, "as", alias] => (*field, *alias),
            _ => bail!("unsupported projection selector: {selector}"),
        };

        if let Some(projected) = lookup_field(value, field) {
            object.insert(alias.to_owned(), projected.clone());
        }
    }

    Ok(Value::Object(object))
}

fn apply_window(
    window_kind: &event_pipeline_types::WindowSpec,
    value: &Value,
    event_time_epoch_ms: u64,
) -> Result<Value> {
    let mut object = object_like(value);
    let size_ms = u64::from(window_kind.size_secs) * 1_000;
    let window_start_epoch_ms = match window_kind.kind {
        WindowKind::Tumbling => (event_time_epoch_ms / size_ms) * size_ms,
        WindowKind::Sliding => {
            let slide_secs = window_kind
                .slide_secs
                .ok_or_else(|| anyhow!("sliding windows require slide_secs"))?;
            let slide_ms = u64::from(slide_secs) * 1_000;
            (event_time_epoch_ms / slide_ms) * slide_ms
        }
    };
    let window_end_epoch_ms = window_start_epoch_ms.saturating_add(size_ms);

    object.insert(
        "_window".to_owned(),
        json!({
            "start_epoch_ms": window_start_epoch_ms,
            "end_epoch_ms": window_end_epoch_ms,
            "grace_secs": window_kind.grace_secs,
        }),
    );
    Ok(Value::Object(object))
}

fn apply_enrichment(table_name: &str, key_field: &str, value: &Value) -> Result<Value> {
    let mut object = object_like(value);
    let lookup_value = lookup_field(value, key_field)
        .ok_or_else(|| anyhow!("missing enrichment key field {key_field}"))?;
    object.insert(
        "_enrichment".to_owned(),
        json!({
            "table": table_name,
            "key_field": key_field,
            "key_value": lookup_value,
        }),
    );
    Ok(Value::Object(object))
}

fn object_like(value: &Value) -> Map<String, Value> {
    match value {
        Value::Object(object) => object.clone(),
        _ => {
            let mut object = Map::new();
            object.insert("value".to_owned(), value.clone());
            object
        }
    }
}

fn lookup_field<'a>(value: &'a Value, field: &str) -> Option<&'a Value> {
    let mut current = value;
    for segment in field.split('.') {
        current = current.get(segment)?;
    }
    Some(current)
}

fn numeric_value(value: &Value) -> Result<f64> {
    if let Some(number) = value.as_f64() {
        return Ok(number);
    }

    if let Some(number) = lookup_field(value, "value").and_then(Value::as_f64) {
        return Ok(number);
    }

    bail!("aggregate operators require a numeric payload or payload.value field")
}

fn sink_destination(kind: &SinkKind) -> String {
    match kind {
        SinkKind::Kafka { topic_name, .. } => format!("kafka:{topic_name}"),
        SinkKind::MaterializedView { view_name } => format!("view:{view_name}"),
        SinkKind::ObjectStorage { bucket, prefix } => format!("object-storage:{bucket}/{prefix}"),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CheckpointWrite, PipelineEvent, PollProcessorResponse, ProcessorRepository,
        ProcessorService, ProcessorSettings,
    };
    use anyhow::Result;
    use async_trait::async_trait;
    use event_pipeline_types::{
        AggregateFunction, CheckpointSummary, DeploymentConfig, DeploymentState, EventEncoding,
        OperatorKind, OperatorNode, PartitionAssignment, PipelineSpec, ReplayPolicy, SinkKind,
        SinkSpec, SourceTopic, WindowKind, WindowSpec,
    };
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    #[derive(Default)]
    struct InMemoryProcessorRepository {
        assignments: RwLock<BTreeMap<String, Vec<PartitionAssignment>>>,
        pipelines: RwLock<BTreeMap<(String, String, u32), PipelineSpec>>,
        checkpoints: RwLock<BTreeMap<(String, String, u32), CheckpointSummary>>,
        events: RwLock<BTreeMap<(String, String, u32), Vec<PipelineEvent>>>,
    }

    #[async_trait]
    impl ProcessorRepository for InMemoryProcessorRepository {
        async fn list_assignments_for_worker(
            &self,
            worker_id: &str,
        ) -> Result<Vec<PartitionAssignment>> {
            Ok(self
                .assignments
                .read()
                .await
                .get(worker_id)
                .cloned()
                .unwrap_or_default())
        }

        async fn get_pipeline_version(
            &self,
            tenant_id: &str,
            pipeline_id: &str,
            version: u32,
        ) -> Result<Option<PipelineSpec>> {
            Ok(self
                .pipelines
                .read()
                .await
                .get(&(tenant_id.to_owned(), pipeline_id.to_owned(), version))
                .cloned())
        }

        async fn latest_checkpoint(
            &self,
            tenant_id: &str,
            pipeline_id: &str,
            partition_id: u32,
        ) -> Result<Option<CheckpointSummary>> {
            Ok(self
                .checkpoints
                .read()
                .await
                .get(&(tenant_id.to_owned(), pipeline_id.to_owned(), partition_id))
                .cloned())
        }

        async fn fetch_partition_events(
            &self,
            tenant_id: &str,
            pipeline_id: &str,
            partition_id: u32,
            offset_gt: u64,
            limit: usize,
        ) -> Result<Vec<PipelineEvent>> {
            Ok(self
                .events
                .read()
                .await
                .get(&(tenant_id.to_owned(), pipeline_id.to_owned(), partition_id))
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .filter(|event| event.offset > offset_gt)
                .take(limit)
                .collect())
        }

        async fn append_checkpoint(
            &self,
            checkpoint: &CheckpointWrite,
        ) -> Result<CheckpointSummary> {
            let summary = CheckpointSummary {
                partition_id: checkpoint.partition_id,
                offset: checkpoint.offset,
                snapshot_uri: checkpoint.snapshot_uri.clone(),
            };
            self.checkpoints.write().await.insert(
                (
                    checkpoint.tenant_id.clone(),
                    checkpoint.pipeline_id.clone(),
                    checkpoint.partition_id,
                ),
                summary.clone(),
            );
            Ok(summary)
        }
    }

    fn assignment() -> PartitionAssignment {
        PartitionAssignment {
            tenant_id: String::from("tenant-a"),
            pipeline_id: String::from("pipeline-a"),
            version: 1,
            partition_id: 0,
            worker_id: String::from("worker-a"),
            lease_epoch: 3,
        }
    }

    fn pipeline() -> PipelineSpec {
        PipelineSpec {
            tenant_id: String::from("tenant-a"),
            pipeline_id: String::from("pipeline-a"),
            version: 1,
            sources: vec![SourceTopic {
                source_id: String::from("orders"),
                topic_name: String::from("orders.v1"),
                partition_count: 4,
                partition_key: String::from("tenant_id"),
                encoding: EventEncoding::Json,
            }],
            operators: vec![
                OperatorNode {
                    operator_id: String::from("filter_paid"),
                    upstream_ids: vec![String::from("orders")],
                    kind: OperatorKind::Filter {
                        expression: String::from("status == paid"),
                    },
                },
                OperatorNode {
                    operator_id: String::from("project_amount"),
                    upstream_ids: vec![String::from("filter_paid")],
                    kind: OperatorKind::Map {
                        projection: String::from("tenant_id, value"),
                    },
                },
                OperatorNode {
                    operator_id: String::from("window_5m"),
                    upstream_ids: vec![String::from("project_amount")],
                    kind: OperatorKind::Window(WindowSpec {
                        kind: WindowKind::Tumbling,
                        size_secs: 300,
                        slide_secs: None,
                        grace_secs: 30,
                    }),
                },
                OperatorNode {
                    operator_id: String::from("count_amount"),
                    upstream_ids: vec![String::from("window_5m")],
                    kind: OperatorKind::Aggregate {
                        function: AggregateFunction::Count,
                        state_ttl_secs: 600,
                    },
                },
            ],
            sinks: vec![SinkSpec {
                sink_id: String::from("orders-counts"),
                upstream_id: String::from("count_amount"),
                kind: SinkKind::MaterializedView {
                    view_name: String::from("orders_count_mv"),
                },
            }],
            deployment: DeploymentConfig {
                parallelism: 4,
                checkpoint_interval_secs: 15,
                max_in_flight_messages: 256,
            },
            replay_policy: ReplayPolicy {
                allow_manual_replay: true,
                retention_hours: 24,
            },
            deployment_state: DeploymentState::Running,
        }
    }

    fn event(offset: u64, status: &str, value: u64) -> PipelineEvent {
        PipelineEvent {
            tenant_id: String::from("tenant-a"),
            pipeline_id: String::from("pipeline-a"),
            source_id: String::from("orders"),
            partition_id: 0,
            offset,
            record_key: format!("order-{offset}"),
            payload: serde_json::json!({
                "tenant_id": "tenant-a",
                "status": status,
                "value": value,
            }),
            event_time_epoch_ms: 1_735_000_000_000 + (offset * 1_000),
        }
    }

    async fn build_service() -> (ProcessorService, Arc<InMemoryProcessorRepository>) {
        let repository = Arc::new(InMemoryProcessorRepository::default());
        repository
            .assignments
            .write()
            .await
            .insert(String::from("worker-a"), vec![assignment()]);
        repository.pipelines.write().await.insert(
            (String::from("tenant-a"), String::from("pipeline-a"), 1),
            pipeline(),
        );

        let service = ProcessorService::new(
            repository.clone(),
            ProcessorSettings {
                worker_id: String::from("worker-a"),
                batch_size: 32,
            },
        );

        (service, repository)
    }

    #[tokio::test]
    async fn poll_once_processes_partition_and_writes_checkpoint() -> Result<()> {
        let (service, repository) = build_service().await;
        repository.events.write().await.insert(
            (String::from("tenant-a"), String::from("pipeline-a"), 0),
            vec![
                event(1, "paid", 10),
                event(2, "paid", 20),
                event(3, "pending", 30),
            ],
        );

        let response = service.poll_once().await?;

        assert!(response.failed_assignments.is_empty());
        assert_eq!(response.processed_assignments.len(), 1);
        let processed = &response.processed_assignments[0];
        assert_eq!(processed.processed_event_count, 3);
        assert_eq!(processed.start_offset, 0);
        assert_eq!(
            processed
                .checkpoint
                .as_ref()
                .map(|checkpoint| checkpoint.offset),
            Some(3)
        );
        assert_eq!(processed.sink_emissions.len(), 2);
        assert_eq!(
            processed.sink_emissions[0].payload["count"],
            serde_json::json!(1)
        );
        assert_eq!(
            processed.sink_emissions[1].payload["count"],
            serde_json::json!(2)
        );

        Ok(())
    }

    #[tokio::test]
    async fn poll_once_resumes_from_latest_checkpoint() -> Result<()> {
        let (service, repository) = build_service().await;
        repository.events.write().await.insert(
            (String::from("tenant-a"), String::from("pipeline-a"), 0),
            vec![
                event(1, "paid", 10),
                event(2, "paid", 20),
                event(3, "paid", 30),
            ],
        );
        repository.checkpoints.write().await.insert(
            (String::from("tenant-a"), String::from("pipeline-a"), 0),
            CheckpointSummary {
                partition_id: 0,
                offset: 1,
                snapshot_uri: String::from("processor-runtime://offset/1"),
            },
        );

        let response = service.poll_once().await?;
        let processed = &response.processed_assignments[0];

        assert_eq!(processed.start_offset, 1);
        assert_eq!(processed.processed_event_count, 2);
        assert_eq!(
            processed
                .checkpoint
                .as_ref()
                .map(|checkpoint| checkpoint.offset),
            Some(3)
        );

        Ok(())
    }

    #[tokio::test]
    async fn aggregate_state_persists_across_polls() -> Result<()> {
        let (service, repository) = build_service().await;
        repository.events.write().await.insert(
            (String::from("tenant-a"), String::from("pipeline-a"), 0),
            vec![event(1, "paid", 10), event(2, "paid", 20)],
        );

        let first = service.poll_once().await?;
        assert_eq!(
            first.processed_assignments[0].sink_emissions[1].payload["count"],
            serde_json::json!(2)
        );

        repository.events.write().await.insert(
            (String::from("tenant-a"), String::from("pipeline-a"), 0),
            vec![
                event(1, "paid", 10),
                event(2, "paid", 20),
                event(3, "paid", 30),
            ],
        );

        let second = service.poll_once().await?;
        assert_eq!(second.processed_assignments[0].sink_emissions.len(), 1);
        assert_eq!(
            second.processed_assignments[0].sink_emissions[0].payload["count"],
            serde_json::json!(3)
        );

        Ok(())
    }

    #[tokio::test]
    async fn join_operator_is_reported_as_failed_assignment() -> Result<()> {
        let (service, repository) = build_service().await;
        let mut join_pipeline = pipeline();
        join_pipeline.operators.push(OperatorNode {
            operator_id: String::from("join_inventory"),
            upstream_ids: vec![String::from("orders"), String::from("project_amount")],
            kind: OperatorKind::Join {
                kind: event_pipeline_types::JoinKind::Inner,
                key_field: String::from("tenant_id"),
                within_secs: 60,
            },
        });
        repository.pipelines.write().await.insert(
            (String::from("tenant-a"), String::from("pipeline-a"), 1),
            join_pipeline,
        );
        repository.events.write().await.insert(
            (String::from("tenant-a"), String::from("pipeline-a"), 0),
            vec![event(1, "paid", 10)],
        );

        let response: PollProcessorResponse = service.poll_once().await?;

        assert!(response.processed_assignments.is_empty());
        assert_eq!(response.failed_assignments.len(), 1);
        assert!(
            response.failed_assignments[0]
                .reason
                .contains("stateful-processing branch")
        );

        Ok(())
    }
}
