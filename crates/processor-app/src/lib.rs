use anyhow::{Result, anyhow, bail};
use async_trait::async_trait;
use event_pipeline_types::{
    AggregateFunction, CheckpointSummary, DeadLetterRecord, DeploymentState, JoinKind,
    OperatorKind, PartitionAssignment, PipelineSpec, ReplayJob, SinkKind, WindowKind,
};
use serde::{Deserialize, Serialize};
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

#[derive(Clone, Debug, PartialEq)]
pub struct LoadedCheckpoint {
    pub summary: CheckpointSummary,
    pub snapshot_state: Value,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CheckpointWrite {
    pub tenant_id: String,
    pub pipeline_id: String,
    pub partition_id: u32,
    pub offset: u64,
    pub snapshot_uri: String,
    pub snapshot_version: u32,
    pub snapshot_state: Value,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DeadLetterWrite {
    pub tenant_id: String,
    pub pipeline_id: String,
    pub source_id: String,
    pub partition_id: u32,
    pub event_offset: u64,
    pub record_key: String,
    pub failure_reason: String,
    pub retryable: bool,
    pub payload: Value,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ProcessedAssignment {
    pub assignment: PartitionAssignment,
    pub processed_event_count: usize,
    pub start_offset: u64,
    pub checkpoint: Option<CheckpointSummary>,
    pub sink_emissions: Vec<SinkEmission>,
    pub dead_letter_count: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ProcessedReplayJob {
    pub replay_job: ReplayJob,
    pub processed_event_count: usize,
    pub sink_emissions: Vec<SinkEmission>,
    pub dead_letter_count: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AssignmentFailure {
    pub assignment: PartitionAssignment,
    pub reason: String,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct PollProcessorResponse {
    pub processed_assignments: Vec<ProcessedAssignment>,
    pub processed_replay_jobs: Vec<ProcessedReplayJob>,
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
    ) -> Result<Option<LoadedCheckpoint>>;
    async fn fetch_partition_events(
        &self,
        tenant_id: &str,
        pipeline_id: &str,
        partition_id: u32,
        offset_gt: u64,
        limit: usize,
    ) -> Result<Vec<PipelineEvent>>;
    async fn append_checkpoint(&self, checkpoint: &CheckpointWrite) -> Result<LoadedCheckpoint>;
    async fn claim_pending_replay_job(&self, worker_id: &str) -> Result<Option<ReplayJob>>;
    async fn fetch_replay_events(
        &self,
        tenant_id: &str,
        pipeline_id: &str,
        from_offset: u64,
        to_offset: Option<u64>,
        last_processed_offset: Option<u64>,
        limit: usize,
    ) -> Result<Vec<PipelineEvent>>;
    async fn update_replay_job_progress(
        &self,
        replay_job_id: &str,
        last_processed_offset: u64,
    ) -> Result<ReplayJob>;
    async fn complete_replay_job(
        &self,
        replay_job_id: &str,
        last_processed_offset: Option<u64>,
    ) -> Result<ReplayJob>;
    async fn fail_replay_job(
        &self,
        replay_job_id: &str,
        error_message: &str,
        last_processed_offset: Option<u64>,
    ) -> Result<ReplayJob>;
    async fn append_dead_letter(&self, dead_letter: &DeadLetterWrite) -> Result<DeadLetterRecord>;
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

        if let Some(replay_job) = self
            .repository
            .claim_pending_replay_job(&self.settings.worker_id)
            .await?
        {
            match self.process_replay_job(replay_job.clone()).await {
                Ok(processed) => response.processed_replay_jobs.push(processed),
                Err(error) => {
                    let _ = self
                        .repository
                        .fail_replay_job(&replay_job.replay_job_id, &error.to_string(), None)
                        .await;
                }
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
            .map_or(0, |checkpoint| checkpoint.summary.offset);

        self.restore_live_state_if_missing(&assignment, latest_checkpoint.as_ref())
            .await?;

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
                checkpoint: latest_checkpoint.map(|checkpoint| checkpoint.summary),
                sink_emissions: Vec::new(),
                dead_letter_count: 0,
            });
        }

        let key = AssignmentKey::from_assignment(&assignment);
        let execution = {
            let mut states = self.state.write().await;
            let state = states.entry(key).or_default();
            self.execute_events(&assignment, &pipeline, &events, state)?
        };

        let snapshot_state = self.snapshot_assignment_state(&assignment).await?;
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
                snapshot_version: 1,
                snapshot_state,
            })
            .await?;

        self.persist_dead_letters(&execution.dead_letters).await?;

        Ok(ProcessedAssignment {
            assignment,
            processed_event_count: events.len(),
            start_offset,
            checkpoint: Some(checkpoint.summary),
            sink_emissions: execution.sink_emissions,
            dead_letter_count: execution.dead_letters.len(),
        })
    }

    async fn process_replay_job(&self, replay_job: ReplayJob) -> Result<ProcessedReplayJob> {
        let pipeline = self
            .repository
            .get_pipeline_version(
                &replay_job.tenant_id,
                &replay_job.pipeline_id,
                replay_job.version,
            )
            .await?
            .ok_or_else(|| anyhow!("pipeline version not found for replay job"))?;

        validate_runtime_supported(&pipeline)?;

        let events = self
            .repository
            .fetch_replay_events(
                &replay_job.tenant_id,
                &replay_job.pipeline_id,
                replay_job.from_offset,
                replay_job.to_offset,
                replay_job.last_processed_offset,
                self.settings.batch_size,
            )
            .await?;

        if events.is_empty() {
            let replay_job = self
                .repository
                .complete_replay_job(&replay_job.replay_job_id, replay_job.last_processed_offset)
                .await?;
            return Ok(ProcessedReplayJob {
                replay_job,
                processed_event_count: 0,
                sink_emissions: Vec::new(),
                dead_letter_count: 0,
            });
        }

        let mut replay_state = BTreeMap::new();
        let mut sink_emissions = Vec::new();
        let mut dead_letters = Vec::new();

        let grouped = group_events_by_partition(&events);
        for (partition_id, partition_events) in grouped {
            let assignment = PartitionAssignment {
                tenant_id: replay_job.tenant_id.clone(),
                pipeline_id: replay_job.pipeline_id.clone(),
                version: replay_job.version,
                partition_id,
                worker_id: self.settings.worker_id.clone(),
                lease_epoch: 0,
            };
            let state = replay_state
                .entry(AssignmentKey::from_assignment(&assignment))
                .or_default();
            let execution =
                self.execute_events(&assignment, &pipeline, &partition_events, state)?;
            sink_emissions.extend(execution.sink_emissions);
            dead_letters.extend(execution.dead_letters);
        }

        self.persist_dead_letters(&dead_letters).await?;

        let last_processed_offset = events.last().map(|event| event.offset);
        let replay_job = self
            .repository
            .update_replay_job_progress(
                &replay_job.replay_job_id,
                last_processed_offset.unwrap_or(replay_job.from_offset),
            )
            .await?;

        let replay_job = if events.len() < self.settings.batch_size {
            self.repository
                .complete_replay_job(&replay_job.replay_job_id, last_processed_offset)
                .await?
        } else {
            replay_job
        };

        Ok(ProcessedReplayJob {
            replay_job,
            processed_event_count: events.len(),
            sink_emissions,
            dead_letter_count: dead_letters.len(),
        })
    }

    async fn restore_live_state_if_missing(
        &self,
        assignment: &PartitionAssignment,
        checkpoint: Option<&LoadedCheckpoint>,
    ) -> Result<()> {
        let key = AssignmentKey::from_assignment(assignment);
        let mut states = self.state.write().await;
        if states.contains_key(&key) {
            return Ok(());
        }

        let restored = checkpoint
            .map(|checkpoint| deserialize_state(&checkpoint.snapshot_state))
            .transpose()?
            .unwrap_or_default();
        states.insert(key, restored);
        Ok(())
    }

    async fn snapshot_assignment_state(&self, assignment: &PartitionAssignment) -> Result<Value> {
        let states = self.state.read().await;
        let state = states
            .get(&AssignmentKey::from_assignment(assignment))
            .cloned()
            .unwrap_or_default();
        serde_json::to_value(state).map_err(Into::into)
    }

    async fn persist_dead_letters(&self, dead_letters: &[DeadLetterWrite]) -> Result<()> {
        for dead_letter in dead_letters {
            self.repository.append_dead_letter(dead_letter).await?;
        }
        Ok(())
    }

    fn execute_events(
        &self,
        assignment: &PartitionAssignment,
        pipeline: &PipelineSpec,
        events: &[PipelineEvent],
        state: &mut AssignmentState,
    ) -> Result<ExecutionOutcome> {
        let mut sink_emissions = Vec::new();
        let mut dead_letters = Vec::new();

        for event in events {
            match self.execute_event(assignment, pipeline, event, state) {
                Ok(mut emissions) => sink_emissions.append(&mut emissions),
                Err(error) => dead_letters.push(DeadLetterWrite {
                    tenant_id: assignment.tenant_id.clone(),
                    pipeline_id: assignment.pipeline_id.clone(),
                    source_id: event.source_id.clone(),
                    partition_id: event.partition_id,
                    event_offset: event.offset,
                    record_key: event.record_key.clone(),
                    failure_reason: error.to_string(),
                    retryable: false,
                    payload: event.payload.clone(),
                }),
            }
        }

        Ok(ExecutionOutcome {
            sink_emissions,
            dead_letters,
        })
    }

    fn execute_event(
        &self,
        assignment: &PartitionAssignment,
        pipeline: &PipelineSpec,
        event: &PipelineEvent,
        state: &mut AssignmentState,
    ) -> Result<Vec<SinkEmission>> {
        let mut values = BTreeMap::new();
        values.insert(event.source_id.clone(), vec![event.payload.clone()]);

        for operator in &pipeline.operators {
            let output = match &operator.kind {
                OperatorKind::Join {
                    kind,
                    key_field,
                    within_secs,
                } => {
                    let Some((active_upstream_id, active_values)) =
                        operator_active_upstream(operator, &values)
                    else {
                        continue;
                    };
                    self.apply_join(
                        state,
                        operator.operator_id.as_str(),
                        operator,
                        *kind,
                        key_field,
                        *within_secs,
                        active_upstream_id,
                        &active_values,
                        event,
                    )?
                }
                kind => {
                    if !operator
                        .upstream_ids
                        .iter()
                        .all(|upstream_id| values.contains_key(upstream_id))
                    {
                        continue;
                    }

                    let upstream_values = operator
                        .upstream_ids
                        .iter()
                        .map(|upstream_id| {
                            values
                                .get(upstream_id)
                                .cloned()
                                .ok_or_else(|| anyhow!("missing upstream value for {upstream_id}"))
                        })
                        .collect::<Result<Vec<_>>>()?;

                    self.apply_operator(
                        state,
                        operator.operator_id.as_str(),
                        kind,
                        &upstream_values,
                        event,
                    )?
                }
            };
            values.insert(operator.operator_id.clone(), output);
        }

        let mut emissions = Vec::new();
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

        Ok(emissions)
    }

    fn apply_operator(
        &self,
        state: &mut AssignmentState,
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
                Ok(vec![update_aggregate_state(
                    state,
                    operator_id,
                    *function,
                    value,
                )?])
            }
            OperatorKind::Join { .. } => unreachable!("join handled separately"),
            OperatorKind::Enrich {
                table_name,
                key_field,
            } => {
                let value = single_input_value(upstream_values, "enrich")?;
                Ok(vec![apply_enrichment(table_name, key_field, value)?])
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_join(
        &self,
        state: &mut AssignmentState,
        operator_id: &str,
        operator: &event_pipeline_types::OperatorNode,
        join_kind: JoinKind,
        key_field: &str,
        within_secs: u32,
        active_upstream_id: &str,
        active_values: &[Value],
        event: &PipelineEvent,
    ) -> Result<Vec<Value>> {
        let [left_upstream_id, right_upstream_id] = operator.upstream_ids.as_slice() else {
            bail!("join operators require exactly two upstreams");
        };

        let join_state = state.joins.entry(operator_id.to_owned()).or_default();
        prune_join_state(join_state, event.event_time_epoch_ms, within_secs);

        let active_side = if active_upstream_id == left_upstream_id {
            JoinSide::Left
        } else if active_upstream_id == right_upstream_id {
            JoinSide::Right
        } else {
            bail!("active upstream does not belong to join");
        };

        let mut output = Vec::new();
        for value in active_values {
            let join_key = lookup_field(value, key_field)
                .ok_or_else(|| anyhow!("join key field {key_field} missing"))?;
            let key = join_key_string(join_key)?;
            let buffered = BufferedRecord {
                payload: value.clone(),
                event_time_epoch_ms: event.event_time_epoch_ms,
                offset: event.offset,
            };

            let opposite_records = join_state
                .other_side(active_side)
                .get(&key)
                .cloned()
                .unwrap_or_default();
            join_state
                .side_mut(active_side)
                .entry(key)
                .or_default()
                .push(buffered.clone());

            let matches: Vec<_> = opposite_records
                .into_iter()
                .filter(|other| {
                    within_window(
                        other.event_time_epoch_ms,
                        event.event_time_epoch_ms,
                        within_secs,
                    )
                })
                .collect();

            match (join_kind, active_side, matches.is_empty()) {
                (JoinKind::Inner, _, true) => {}
                (JoinKind::Inner, _, false) => {
                    for matched in matches {
                        output.push(merge_join_records(
                            active_side,
                            &buffered.payload,
                            &matched.payload,
                            join_key,
                        ));
                    }
                }
                (JoinKind::Left, JoinSide::Left, true) => {
                    output.push(merge_join_records(
                        active_side,
                        &buffered.payload,
                        &Value::Null,
                        join_key,
                    ));
                }
                (JoinKind::Left, JoinSide::Left, false) => {
                    for matched in matches {
                        output.push(merge_join_records(
                            active_side,
                            &buffered.payload,
                            &matched.payload,
                            join_key,
                        ));
                    }
                }
                (JoinKind::Left, JoinSide::Right, false) => {
                    for matched in matches {
                        output.push(merge_join_records(
                            active_side,
                            &buffered.payload,
                            &matched.payload,
                            join_key,
                        ));
                    }
                }
                (JoinKind::Left, JoinSide::Right, true) => {}
            }
        }

        Ok(output)
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

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
struct AssignmentState {
    aggregates: BTreeMap<String, AggregateState>,
    joins: BTreeMap<String, JoinState>,
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
struct JoinState {
    left: BTreeMap<String, Vec<BufferedRecord>>,
    right: BTreeMap<String, Vec<BufferedRecord>>,
}

impl JoinState {
    fn side_mut(&mut self, side: JoinSide) -> &mut BTreeMap<String, Vec<BufferedRecord>> {
        match side {
            JoinSide::Left => &mut self.left,
            JoinSide::Right => &mut self.right,
        }
    }

    fn other_side(&self, side: JoinSide) -> &BTreeMap<String, Vec<BufferedRecord>> {
        match side {
            JoinSide::Left => &self.right,
            JoinSide::Right => &self.left,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
struct BufferedRecord {
    payload: Value,
    event_time_epoch_ms: u64,
    offset: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum JoinSide {
    Left,
    Right,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
enum AggregateState {
    Count(u64),
    Numeric {
        count: u64,
        sum: f64,
        min: f64,
        max: f64,
    },
}

#[derive(Default)]
struct ExecutionOutcome {
    sink_emissions: Vec<SinkEmission>,
    dead_letters: Vec<DeadLetterWrite>,
}

fn deserialize_state(snapshot_state: &Value) -> Result<AssignmentState> {
    if snapshot_state.is_null() {
        return Ok(AssignmentState::default());
    }

    serde_json::from_value(snapshot_state.clone()).map_err(Into::into)
}

fn group_events_by_partition(events: &[PipelineEvent]) -> BTreeMap<u32, Vec<PipelineEvent>> {
    let mut grouped = BTreeMap::new();
    for event in events {
        grouped
            .entry(event.partition_id)
            .or_insert_with(Vec::new)
            .push(event.clone());
    }
    grouped
}

fn update_aggregate_state(
    state: &mut AssignmentState,
    operator_id: &str,
    function: AggregateFunction,
    value: &Value,
) -> Result<Value> {
    let aggregate = state
        .aggregates
        .entry(operator_id.to_owned())
        .or_insert_with(|| AggregateState::new(function));

    aggregate.update(function, value)
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
                    AggregateFunction::Average => *sum / (*count as f64),
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

    Ok(())
}

fn operator_active_upstream<'a>(
    operator: &'a event_pipeline_types::OperatorNode,
    values: &'a BTreeMap<String, Vec<Value>>,
) -> Option<(&'a str, Vec<Value>)> {
    operator.upstream_ids.iter().find_map(|upstream_id| {
        values
            .get(upstream_id)
            .cloned()
            .map(|value| (upstream_id.as_str(), value))
    })
}

fn prune_join_state(join_state: &mut JoinState, event_time_epoch_ms: u64, within_secs: u32) {
    let prune_before = event_time_epoch_ms.saturating_sub(u64::from(within_secs) * 1_000);
    for side in [&mut join_state.left, &mut join_state.right] {
        side.retain(|_, records| {
            records.retain(|record| record.event_time_epoch_ms >= prune_before);
            !records.is_empty()
        });
    }
}

fn within_window(left_ms: u64, right_ms: u64, within_secs: u32) -> bool {
    let delta = left_ms.abs_diff(right_ms);
    delta <= u64::from(within_secs) * 1_000
}

fn join_key_string(value: &Value) -> Result<String> {
    serde_json::to_string(value).map_err(Into::into)
}

fn merge_join_records(
    active_side: JoinSide,
    active_payload: &Value,
    matched_payload: &Value,
    join_key: &Value,
) -> Value {
    match active_side {
        JoinSide::Left => json!({
            "join_key": join_key,
            "left": active_payload,
            "right": matched_payload,
        }),
        JoinSide::Right => json!({
            "join_key": join_key,
            "left": matched_payload,
            "right": active_payload,
        }),
    }
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
    Value::String(input.trim_matches('"').trim_matches('\'').to_owned())
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
        CheckpointWrite, DeadLetterWrite, LoadedCheckpoint, PipelineEvent, ProcessorRepository,
        ProcessorService, ProcessorSettings,
    };
    use anyhow::Result;
    use async_trait::async_trait;
    use event_pipeline_types::{
        AggregateFunction, CheckpointSummary, DeadLetterRecord, DeploymentConfig, DeploymentState,
        EventEncoding, JoinKind, OperatorKind, OperatorNode, PartitionAssignment, PipelineSpec,
        ReplayJob, ReplayJobStatus, ReplayPolicy, SinkKind, SinkSpec, SourceTopic, WindowKind,
        WindowSpec,
    };
    use serde_json::{Value, json};
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    #[derive(Default)]
    struct InMemoryProcessorRepository {
        assignments: RwLock<BTreeMap<String, Vec<PartitionAssignment>>>,
        pipelines: RwLock<BTreeMap<(String, String, u32), PipelineSpec>>,
        checkpoints: RwLock<BTreeMap<(String, String, u32), LoadedCheckpoint>>,
        events: RwLock<BTreeMap<(String, String, u32), Vec<PipelineEvent>>>,
        replay_jobs: RwLock<Vec<ReplayJob>>,
        dead_letters: RwLock<Vec<DeadLetterRecord>>,
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
        ) -> Result<Option<LoadedCheckpoint>> {
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
        ) -> Result<LoadedCheckpoint> {
            let loaded = LoadedCheckpoint {
                summary: CheckpointSummary {
                    partition_id: checkpoint.partition_id,
                    offset: checkpoint.offset,
                    snapshot_uri: checkpoint.snapshot_uri.clone(),
                    snapshot_version: checkpoint.snapshot_version,
                },
                snapshot_state: checkpoint.snapshot_state.clone(),
            };
            self.checkpoints.write().await.insert(
                (
                    checkpoint.tenant_id.clone(),
                    checkpoint.pipeline_id.clone(),
                    checkpoint.partition_id,
                ),
                loaded.clone(),
            );
            Ok(loaded)
        }

        async fn claim_pending_replay_job(&self, worker_id: &str) -> Result<Option<ReplayJob>> {
            let mut replay_jobs = self.replay_jobs.write().await;
            if let Some(replay_job) = replay_jobs
                .iter_mut()
                .find(|replay_job| replay_job.status == ReplayJobStatus::Pending)
            {
                replay_job.status = ReplayJobStatus::Running;
                replay_job.claimed_by_worker_id = Some(worker_id.to_owned());
                return Ok(Some(replay_job.clone()));
            }

            Ok(None)
        }

        async fn fetch_replay_events(
            &self,
            tenant_id: &str,
            pipeline_id: &str,
            from_offset: u64,
            to_offset: Option<u64>,
            last_processed_offset: Option<u64>,
            limit: usize,
        ) -> Result<Vec<PipelineEvent>> {
            let lower_bound = last_processed_offset.unwrap_or(from_offset.saturating_sub(1));
            let upper_bound = to_offset.unwrap_or(u64::MAX);
            let mut events: Vec<_> = self
                .events
                .read()
                .await
                .iter()
                .filter(|((stored_tenant_id, stored_pipeline_id, _), _)| {
                    stored_tenant_id == tenant_id && stored_pipeline_id == pipeline_id
                })
                .flat_map(|(_, events)| events.clone())
                .filter(|event| {
                    event.offset > lower_bound
                        && event.offset >= from_offset
                        && event.offset <= upper_bound
                })
                .collect();
            events.sort_by_key(|event| (event.partition_id, event.offset));
            events.truncate(limit);
            Ok(events)
        }

        async fn update_replay_job_progress(
            &self,
            replay_job_id: &str,
            last_processed_offset: u64,
        ) -> Result<ReplayJob> {
            let mut replay_jobs = self.replay_jobs.write().await;
            let replay_job = replay_jobs
                .iter_mut()
                .find(|replay_job| replay_job.replay_job_id == replay_job_id)
                .ok_or_else(|| anyhow::anyhow!("replay job not found"))?;
            replay_job.last_processed_offset = Some(last_processed_offset);
            Ok(replay_job.clone())
        }

        async fn complete_replay_job(
            &self,
            replay_job_id: &str,
            last_processed_offset: Option<u64>,
        ) -> Result<ReplayJob> {
            let mut replay_jobs = self.replay_jobs.write().await;
            let replay_job = replay_jobs
                .iter_mut()
                .find(|replay_job| replay_job.replay_job_id == replay_job_id)
                .ok_or_else(|| anyhow::anyhow!("replay job not found"))?;
            replay_job.status = ReplayJobStatus::Succeeded;
            replay_job.last_processed_offset = last_processed_offset;
            Ok(replay_job.clone())
        }

        async fn fail_replay_job(
            &self,
            replay_job_id: &str,
            error_message: &str,
            last_processed_offset: Option<u64>,
        ) -> Result<ReplayJob> {
            let mut replay_jobs = self.replay_jobs.write().await;
            let replay_job = replay_jobs
                .iter_mut()
                .find(|replay_job| replay_job.replay_job_id == replay_job_id)
                .ok_or_else(|| anyhow::anyhow!("replay job not found"))?;
            replay_job.status = ReplayJobStatus::Failed;
            replay_job.error_message = Some(error_message.to_owned());
            replay_job.last_processed_offset = last_processed_offset;
            Ok(replay_job.clone())
        }

        async fn append_dead_letter(
            &self,
            dead_letter: &DeadLetterWrite,
        ) -> Result<DeadLetterRecord> {
            let record = DeadLetterRecord {
                source_id: dead_letter.source_id.clone(),
                partition_id: dead_letter.partition_id,
                event_offset: dead_letter.event_offset,
                record_key: dead_letter.record_key.clone(),
                failure_reason: dead_letter.failure_reason.clone(),
                retryable: dead_letter.retryable,
            };
            self.dead_letters.write().await.push(record.clone());
            Ok(record)
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

    fn aggregate_pipeline() -> PipelineSpec {
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

    fn join_pipeline() -> PipelineSpec {
        PipelineSpec {
            tenant_id: String::from("tenant-a"),
            pipeline_id: String::from("pipeline-a"),
            version: 1,
            sources: vec![
                SourceTopic {
                    source_id: String::from("orders"),
                    topic_name: String::from("orders.v1"),
                    partition_count: 4,
                    partition_key: String::from("account_id"),
                    encoding: EventEncoding::Json,
                },
                SourceTopic {
                    source_id: String::from("profiles"),
                    topic_name: String::from("profiles.v1"),
                    partition_count: 4,
                    partition_key: String::from("account_id"),
                    encoding: EventEncoding::Json,
                },
            ],
            operators: vec![OperatorNode {
                operator_id: String::from("join_orders_profiles"),
                upstream_ids: vec![String::from("orders"), String::from("profiles")],
                kind: OperatorKind::Join {
                    kind: JoinKind::Left,
                    key_field: String::from("account_id"),
                    within_secs: 60,
                },
            }],
            sinks: vec![SinkSpec {
                sink_id: String::from("joined"),
                upstream_id: String::from("join_orders_profiles"),
                kind: SinkKind::MaterializedView {
                    view_name: String::from("joined_mv"),
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

    fn event(source_id: &str, partition_id: u32, offset: u64, payload: Value) -> PipelineEvent {
        PipelineEvent {
            tenant_id: String::from("tenant-a"),
            pipeline_id: String::from("pipeline-a"),
            source_id: source_id.to_owned(),
            partition_id,
            offset,
            record_key: format!("{source_id}-{offset}"),
            payload,
            event_time_epoch_ms: 1_735_000_000_000 + (offset * 1_000),
        }
    }

    async fn build_service(
        pipeline: PipelineSpec,
    ) -> (ProcessorService, Arc<InMemoryProcessorRepository>) {
        let repository = Arc::new(InMemoryProcessorRepository::default());
        repository
            .assignments
            .write()
            .await
            .insert(String::from("worker-a"), vec![assignment()]);
        repository.pipelines.write().await.insert(
            (String::from("tenant-a"), String::from("pipeline-a"), 1),
            pipeline,
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
    async fn poll_once_processes_partition_and_persists_snapshot_checkpoint() -> Result<()> {
        let (service, repository) = build_service(aggregate_pipeline()).await;
        repository.events.write().await.insert(
            (String::from("tenant-a"), String::from("pipeline-a"), 0),
            vec![
                event(
                    "orders",
                    0,
                    1,
                    json!({"tenant_id": "tenant-a", "status": "paid", "value": 10}),
                ),
                event(
                    "orders",
                    0,
                    2,
                    json!({"tenant_id": "tenant-a", "status": "paid", "value": 20}),
                ),
            ],
        );

        let response = service.poll_once().await?;

        assert!(response.failed_assignments.is_empty());
        assert_eq!(response.processed_assignments.len(), 1);
        let processed = &response.processed_assignments[0];
        assert_eq!(processed.processed_event_count, 2);
        assert_eq!(processed.sink_emissions.len(), 2);
        assert_eq!(processed.dead_letter_count, 0);
        assert_eq!(
            processed
                .checkpoint
                .as_ref()
                .map(|checkpoint| checkpoint.snapshot_version),
            Some(1)
        );

        let checkpoint = repository
            .latest_checkpoint("tenant-a", "pipeline-a", 0)
            .await?
            .expect("checkpoint should exist");
        assert!(checkpoint.snapshot_state["aggregates"]["count_amount"].is_object());

        Ok(())
    }

    #[tokio::test]
    async fn join_operator_emits_left_join_records() -> Result<()> {
        let (service, repository) = build_service(join_pipeline()).await;
        repository.events.write().await.insert(
            (String::from("tenant-a"), String::from("pipeline-a"), 0),
            vec![
                event(
                    "profiles",
                    0,
                    1,
                    json!({"account_id": "a-1", "plan": "pro"}),
                ),
                event("orders", 0, 2, json!({"account_id": "a-1", "amount": 42})),
            ],
        );

        let response = service.poll_once().await?;
        let processed = &response.processed_assignments[0];

        assert_eq!(processed.sink_emissions.len(), 1);
        assert_eq!(
            processed.sink_emissions[0].payload["left"]["amount"],
            json!(42)
        );
        assert_eq!(
            processed.sink_emissions[0].payload["right"]["plan"],
            json!("pro")
        );

        Ok(())
    }

    #[tokio::test]
    async fn replay_jobs_are_claimed_processed_and_completed() -> Result<()> {
        let (service, repository) = build_service(aggregate_pipeline()).await;
        repository.events.write().await.insert(
            (String::from("tenant-a"), String::from("pipeline-a"), 0),
            vec![
                event(
                    "orders",
                    0,
                    10,
                    json!({"tenant_id": "tenant-a", "status": "paid", "value": 10}),
                ),
                event(
                    "orders",
                    0,
                    11,
                    json!({"tenant_id": "tenant-a", "status": "paid", "value": 20}),
                ),
            ],
        );
        repository.replay_jobs.write().await.push(ReplayJob {
            replay_job_id: String::from("replay-1"),
            tenant_id: String::from("tenant-a"),
            pipeline_id: String::from("pipeline-a"),
            version: 1,
            from_offset: 10,
            to_offset: Some(11),
            reason: String::from("backfill"),
            status: ReplayJobStatus::Pending,
            claimed_by_worker_id: None,
            last_processed_offset: None,
            error_message: None,
        });

        let response = service.poll_once().await?;

        assert_eq!(response.processed_replay_jobs.len(), 1);
        let replay = &response.processed_replay_jobs[0];
        assert_eq!(replay.processed_event_count, 2);
        assert_eq!(replay.replay_job.status, ReplayJobStatus::Succeeded);
        assert_eq!(replay.replay_job.last_processed_offset, Some(11));

        Ok(())
    }

    #[tokio::test]
    async fn malformed_events_are_dead_lettered_without_failing_assignment() -> Result<()> {
        let (service, repository) = build_service(aggregate_pipeline()).await;
        repository.events.write().await.insert(
            (String::from("tenant-a"), String::from("pipeline-a"), 0),
            vec![
                event(
                    "orders",
                    0,
                    1,
                    json!({"tenant_id": "tenant-a", "value": 10}),
                ),
                event(
                    "orders",
                    0,
                    2,
                    json!({"tenant_id": "tenant-a", "status": "paid", "value": 20}),
                ),
            ],
        );

        let response = service.poll_once().await?;

        assert!(response.failed_assignments.is_empty());
        let processed = &response.processed_assignments[0];
        assert_eq!(processed.dead_letter_count, 1);
        assert_eq!(processed.sink_emissions.len(), 1);
        let dead_letters = repository.dead_letters.read().await;
        assert_eq!(dead_letters.len(), 1);
        assert_eq!(dead_letters[0].event_offset, 1);

        Ok(())
    }
}
