use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use event_pipeline_control_plane_app::MetadataRepository;
use event_pipeline_coordinator_app::{CoordinatorRepository, LeaseExpiryOutcome};
use event_pipeline_processor_app::{
    CheckpointWrite, DeadLetterWrite, LoadedCheckpoint, PipelineEvent, ProcessorRepository,
};
use event_pipeline_types::{
    DeadLetterRecord, DeploymentState, PartitionAssignment, PipelineSpec, PipelineSummary, PlatformRole,
    RegisteredTopic, ReplayJob, ReplayJobStatus, RoleBinding, ServiceAccount, SubjectKind,
    TenantRecord, TenantSummary, TopicSummary, WorkerRecord, WorkerStatus,
};
use sqlx::Row;
use sqlx::migrate::Migrator;
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::types::Json;
use uuid::Uuid;

static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

#[derive(Clone)]
pub struct PostgresMetadataRepository {
    pool: PgPool,
}

impl PostgresMetadataRepository {
    pub async fn connect(database_url: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(database_url)
            .await
            .with_context(|| format!("failed to connect to {database_url}"))?;

        Ok(Self { pool })
    }

    pub async fn migrate(&self) -> Result<()> {
        MIGRATOR.run(&self.pool).await?;
        Ok(())
    }

    async fn ensure_tenant<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        tenant: &TenantRecord,
    ) -> Result<()> {
        sqlx::query(
            r#"
            insert into tenants (tenant_id, display_name, oidc_realm, enabled)
            values ($1, $2, $3, $4)
            on conflict (tenant_id)
            do update
              set display_name = excluded.display_name,
                  oidc_realm = excluded.oidc_realm,
                  enabled = excluded.enabled
            "#,
        )
        .bind(&tenant.tenant_id)
        .bind(&tenant.display_name)
        .bind(&tenant.oidc_realm)
        .bind(tenant.enabled)
        .execute(tx.as_mut())
        .await?;

        Ok(())
    }
}

#[async_trait]
impl MetadataRepository for PostgresMetadataRepository {
    async fn create_pipeline(&self, pipeline: &PipelineSpec) -> Result<PipelineSummary> {
        let mut tx = self.pool.begin().await?;
        self.ensure_tenant(
            &mut tx,
            &TenantRecord {
                tenant_id: pipeline.tenant_id.clone(),
                display_name: pipeline.tenant_id.clone(),
                oidc_realm: String::from("event-pipeline"),
                enabled: true,
            },
        )
        .await?;

        let rows = sqlx::query(
            r#"
            insert into pipelines (
              tenant_id,
              pipeline_id,
              current_version,
              deployment_state
            )
            values ($1, $2, $3, $4)
            on conflict (tenant_id, pipeline_id) do nothing
            "#,
        )
        .bind(&pipeline.tenant_id)
        .bind(&pipeline.pipeline_id)
        .bind(i64::from(pipeline.version))
        .bind(deployment_state_to_db(pipeline.deployment_state))
        .execute(tx.as_mut())
        .await?;

        if rows.rows_affected() == 0 {
            bail!("pipeline already exists");
        }

        sqlx::query(
            r#"
            insert into pipeline_versions (
              tenant_id,
              pipeline_id,
              version,
              spec,
              deployment_state
            )
            values ($1, $2, $3, $4, $5)
            "#,
        )
        .bind(&pipeline.tenant_id)
        .bind(&pipeline.pipeline_id)
        .bind(i64::from(pipeline.version))
        .bind(Json(pipeline))
        .bind(deployment_state_to_db(pipeline.deployment_state))
        .execute(tx.as_mut())
        .await?;

        tx.commit().await?;

        Ok(PipelineSummary {
            tenant_id: pipeline.tenant_id.clone(),
            pipeline_id: pipeline.pipeline_id.clone(),
            version: pipeline.version,
            deployment_state: pipeline.deployment_state,
        })
    }

    async fn update_pipeline_version(&self, pipeline: &PipelineSpec) -> Result<PipelineSummary> {
        let mut tx = self.pool.begin().await?;
        self.ensure_tenant(
            &mut tx,
            &TenantRecord {
                tenant_id: pipeline.tenant_id.clone(),
                display_name: pipeline.tenant_id.clone(),
                oidc_realm: String::from("event-pipeline"),
                enabled: true,
            },
        )
        .await?;

        let existing = sqlx::query(
            "select current_version from pipelines where tenant_id = $1 and pipeline_id = $2",
        )
        .bind(&pipeline.tenant_id)
        .bind(&pipeline.pipeline_id)
        .fetch_optional(tx.as_mut())
        .await?;

        let Some(existing) = existing else {
            bail!("pipeline not found");
        };

        let current_version: i64 = existing.try_get("current_version")?;
        if i64::from(pipeline.version) <= current_version {
            bail!("pipeline version must be greater than the current version");
        }

        sqlx::query(
            r#"
            insert into pipeline_versions (
              tenant_id,
              pipeline_id,
              version,
              spec,
              deployment_state
            )
            values ($1, $2, $3, $4, $5)
            "#,
        )
        .bind(&pipeline.tenant_id)
        .bind(&pipeline.pipeline_id)
        .bind(i64::from(pipeline.version))
        .bind(Json(pipeline))
        .bind(deployment_state_to_db(pipeline.deployment_state))
        .execute(tx.as_mut())
        .await?;

        sqlx::query(
            r#"
            update pipelines
            set current_version = $3,
                deployment_state = $4,
                updated_at = now()
            where tenant_id = $1 and pipeline_id = $2
            "#,
        )
        .bind(&pipeline.tenant_id)
        .bind(&pipeline.pipeline_id)
        .bind(i64::from(pipeline.version))
        .bind(deployment_state_to_db(pipeline.deployment_state))
        .execute(tx.as_mut())
        .await?;

        tx.commit().await?;

        Ok(PipelineSummary {
            tenant_id: pipeline.tenant_id.clone(),
            pipeline_id: pipeline.pipeline_id.clone(),
            version: pipeline.version,
            deployment_state: pipeline.deployment_state,
        })
    }

    async fn get_pipeline(
        &self,
        tenant_id: &str,
        pipeline_id: &str,
        version: Option<u32>,
    ) -> Result<Option<PipelineSpec>> {
        let row = if let Some(version) = version {
            sqlx::query(
                r#"
                select spec
                from pipeline_versions
                where tenant_id = $1 and pipeline_id = $2 and version = $3
                "#,
            )
            .bind(tenant_id)
            .bind(pipeline_id)
            .bind(i64::from(version))
            .fetch_optional(&self.pool)
            .await?
        } else {
            sqlx::query(
                r#"
                select pv.spec
                from pipelines p
                join pipeline_versions pv
                  on pv.tenant_id = p.tenant_id
                 and pv.pipeline_id = p.pipeline_id
                 and pv.version = p.current_version
                where p.tenant_id = $1 and p.pipeline_id = $2
                "#,
            )
            .bind(tenant_id)
            .bind(pipeline_id)
            .fetch_optional(&self.pool)
            .await?
        };

        let Some(row) = row else {
            return Ok(None);
        };

        let Json(pipeline) = row.try_get::<Json<PipelineSpec>, _>("spec")?;
        Ok(Some(pipeline))
    }

    async fn list_pipelines(
        &self,
        tenant_id: Option<&str>,
        deployment_state: Option<DeploymentState>,
    ) -> Result<Vec<PipelineSummary>> {
        let rows = sqlx::query(
            r#"
            select tenant_id, pipeline_id, current_version, deployment_state
            from pipelines
            where ($1::text is null or tenant_id = $1)
              and ($2::text is null or deployment_state = $2)
            order by tenant_id, pipeline_id
            "#,
        )
        .bind(tenant_id)
        .bind(deployment_state.map(deployment_state_to_db))
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok(PipelineSummary {
                    tenant_id: row.try_get("tenant_id")?,
                    pipeline_id: row.try_get("pipeline_id")?,
                    version: u32::try_from(row.try_get::<i64, _>("current_version")?)?,
                    deployment_state: deployment_state_from_db(
                        &row.try_get::<String, _>("deployment_state")?,
                    )?,
                })
            })
            .collect()
    }

    async fn request_replay(&self, replay_job: &ReplayJob) -> Result<ReplayJob> {
        sqlx::query(
            r#"
            insert into replay_jobs (
              replay_job_id,
              tenant_id,
              pipeline_id,
              version,
              from_offset,
              to_offset,
              reason,
              status,
              claimed_by_worker_id,
              last_processed_offset,
              error_message
            )
            values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            "#,
        )
        .bind(Uuid::parse_str(&replay_job.replay_job_id)?)
        .bind(&replay_job.tenant_id)
        .bind(&replay_job.pipeline_id)
        .bind(i64::from(replay_job.version))
        .bind(i64::try_from(replay_job.from_offset)?)
        .bind(replay_job.to_offset.map(i64::try_from).transpose()?)
        .bind(&replay_job.reason)
        .bind(replay_job_status_to_db(replay_job.status))
        .bind(&replay_job.claimed_by_worker_id)
        .bind(
            replay_job
                .last_processed_offset
                .map(i64::try_from)
                .transpose()?,
        )
        .bind(&replay_job.error_message)
        .execute(&self.pool)
        .await?;

        Ok(replay_job.clone())
    }

    async fn get_replay_job(
        &self,
        tenant_id: &str,
        replay_job_id: &str,
    ) -> Result<Option<ReplayJob>> {
        let row = sqlx::query(
            r#"
            select
              replay_job_id,
              tenant_id,
              pipeline_id,
              version,
              from_offset,
              to_offset,
              reason,
              status,
              claimed_by_worker_id,
              last_processed_offset,
              error_message
            from replay_jobs
            where tenant_id = $1 and replay_job_id = $2
            "#,
        )
        .bind(tenant_id)
        .bind(Uuid::parse_str(replay_job_id)?)
        .fetch_optional(&self.pool)
        .await?;

        row.map(map_replay_job).transpose()
    }

    async fn register_topic(&self, topic: &RegisteredTopic) -> Result<RegisteredTopic> {
        let mut tx = self.pool.begin().await?;
        self.ensure_tenant(
            &mut tx,
            &TenantRecord {
                tenant_id: topic.tenant_id.clone(),
                display_name: topic.tenant_id.clone(),
                oidc_realm: String::from("event-pipeline"),
                enabled: true,
            },
        )
        .await?;

        sqlx::query(
            r#"
            insert into topics (
              tenant_id,
              topic_name,
              partition_count,
              retention_hours
            )
            values ($1, $2, $3, $4)
            on conflict (tenant_id, topic_name)
            do update
              set partition_count = excluded.partition_count,
                  retention_hours = excluded.retention_hours
            "#,
        )
        .bind(&topic.tenant_id)
        .bind(&topic.topic_name)
        .bind(i32::from(topic.partition_count))
        .bind(i32::try_from(topic.retention_hours)?)
        .execute(tx.as_mut())
        .await?;

        tx.commit().await?;
        Ok(topic.clone())
    }

    async fn get_topic(
        &self,
        tenant_id: &str,
        topic_name: &str,
    ) -> Result<Option<RegisteredTopic>> {
        let row = sqlx::query(
            r#"
            select tenant_id, topic_name, partition_count, retention_hours
            from topics
            where tenant_id = $1 and topic_name = $2
            "#,
        )
        .bind(tenant_id)
        .bind(topic_name)
        .fetch_optional(&self.pool)
        .await?;

        row.map(map_registered_topic).transpose()
    }

    async fn list_topics(&self, tenant_id: Option<&str>) -> Result<Vec<TopicSummary>> {
        let rows = sqlx::query(
            r#"
            select tenant_id, topic_name, partition_count
            from topics
            where ($1::text is null or tenant_id = $1)
            order by tenant_id, topic_name
            "#,
        )
        .bind(tenant_id)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok(TopicSummary {
                    tenant_id: row.try_get("tenant_id")?,
                    topic_name: row.try_get("topic_name")?,
                    partition_count: u16::try_from(row.try_get::<i32, _>("partition_count")?)?,
                })
            })
            .collect()
    }

    async fn upsert_tenant(&self, tenant: &TenantRecord) -> Result<TenantSummary> {
        let mut tx = self.pool.begin().await?;
        self.ensure_tenant(&mut tx, tenant).await?;
        tx.commit().await?;

        Ok(TenantSummary {
            tenant_id: tenant.tenant_id.clone(),
            display_name: tenant.display_name.clone(),
            oidc_realm: tenant.oidc_realm.clone(),
            enabled: tenant.enabled,
        })
    }

    async fn get_tenant(&self, tenant_id: &str) -> Result<Option<TenantRecord>> {
        let row = sqlx::query(
            r#"
            select tenant_id, display_name, oidc_realm, enabled
            from tenants
            where tenant_id = $1
            "#,
        )
        .bind(tenant_id)
        .fetch_optional(&self.pool)
        .await?;

        row.map(map_tenant).transpose()
    }

    async fn list_tenants(&self) -> Result<Vec<TenantSummary>> {
        let rows = sqlx::query(
            r#"
            select tenant_id, display_name, oidc_realm, enabled
            from tenants
            order by tenant_id
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok(TenantSummary {
                    tenant_id: row.try_get("tenant_id")?,
                    display_name: row.try_get("display_name")?,
                    oidc_realm: row.try_get("oidc_realm")?,
                    enabled: row.try_get("enabled")?,
                })
            })
            .collect()
    }

    async fn assign_role_binding(&self, binding: &RoleBinding) -> Result<RoleBinding> {
        sqlx::query(
            r#"
            insert into role_bindings (
              binding_id,
              subject_kind,
              subject_id,
              tenant_id,
              role
            )
            values ($1, $2, $3, $4, $5)
            on conflict (subject_kind, subject_id, tenant_id, role) do nothing
            "#,
        )
        .bind(Uuid::now_v7())
        .bind(subject_kind_to_db(binding.subject_kind))
        .bind(&binding.subject_id)
        .bind(binding.tenant_id.as_deref())
        .bind(role_to_db(binding.role))
        .execute(&self.pool)
        .await?;

        Ok(binding.clone())
    }

    async fn list_role_bindings(
        &self,
        tenant_id: Option<&str>,
        subject_id: Option<&str>,
    ) -> Result<Vec<RoleBinding>> {
        let rows = sqlx::query(
            r#"
            select subject_kind, subject_id, tenant_id, role
            from role_bindings
            where ($1::text is null or tenant_id = $1 or tenant_id is null)
              and ($2::text is null or subject_id = $2)
            order by subject_id, tenant_id, role
            "#,
        )
        .bind(tenant_id)
        .bind(subject_id)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok(RoleBinding {
                    subject_kind: subject_kind_from_db(&row.try_get::<String, _>("subject_kind")?)?,
                    subject_id: row.try_get("subject_id")?,
                    tenant_id: row.try_get("tenant_id")?,
                    role: role_from_db(&row.try_get::<String, _>("role")?)?,
                })
            })
            .collect()
    }

    async fn create_service_account(
        &self,
        service_account: &ServiceAccount,
    ) -> Result<ServiceAccount> {
        sqlx::query(
            r#"
            insert into service_accounts (
              service_account_id,
              tenant_id,
              client_id,
              display_name,
              enabled
            )
            values ($1, $2, $3, $4, $5)
            "#,
        )
        .bind(Uuid::parse_str(&service_account.service_account_id)?)
        .bind(&service_account.tenant_id)
        .bind(&service_account.client_id)
        .bind(&service_account.display_name)
        .bind(service_account.enabled)
        .execute(&self.pool)
        .await?;

        Ok(service_account.clone())
    }

    async fn list_service_accounts(&self, tenant_id: Option<&str>) -> Result<Vec<ServiceAccount>> {
        let rows = sqlx::query(
            r#"
            select service_account_id, tenant_id, client_id, display_name, enabled
            from service_accounts
            where ($1::text is null or tenant_id = $1)
            order by tenant_id, client_id
            "#,
        )
        .bind(tenant_id)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok(ServiceAccount {
                    service_account_id: row.try_get::<Uuid, _>("service_account_id")?.to_string(),
                    tenant_id: row.try_get("tenant_id")?,
                    client_id: row.try_get("client_id")?,
                    display_name: row.try_get("display_name")?,
                    enabled: row.try_get("enabled")?,
                })
            })
            .collect()
    }
}

#[async_trait]
impl CoordinatorRepository for PostgresMetadataRepository {
    async fn register_worker(&self, worker: &WorkerRecord) -> Result<WorkerRecord> {
        let row = sqlx::query(
            r#"
            insert into workers (
              worker_id,
              endpoint,
              availability_zone,
              max_assignments,
              labels,
              status,
              last_heartbeat_at
            )
            values ($1, $2, $3, $4, $5, $6, to_timestamp($7))
            on conflict (worker_id)
            do update
              set endpoint = excluded.endpoint,
                  availability_zone = excluded.availability_zone,
                  max_assignments = excluded.max_assignments,
                  labels = excluded.labels,
                  status = excluded.status,
                  last_heartbeat_at = excluded.last_heartbeat_at
            returning
              worker_id,
              endpoint,
              availability_zone,
              max_assignments,
              labels,
              status,
              extract(epoch from registered_at)::bigint as registered_at_epoch_secs,
              extract(epoch from last_heartbeat_at)::bigint as last_heartbeat_at_epoch_secs,
              (
                select count(*)
                from partition_assignments pa
                where pa.worker_id = workers.worker_id
              )::bigint as active_assignments
            "#,
        )
        .bind(&worker.worker_id)
        .bind(&worker.endpoint)
        .bind(&worker.availability_zone)
        .bind(i32::from(worker.max_assignments))
        .bind(Json(&worker.labels))
        .bind(worker_status_to_db(worker.status))
        .bind(i64::try_from(worker.last_heartbeat_at_epoch_secs)?)
        .fetch_one(&self.pool)
        .await?;

        map_worker_record(row)
    }

    async fn heartbeat_worker(
        &self,
        worker_id: &str,
        heartbeat_epoch_secs: u64,
    ) -> Result<Option<WorkerRecord>> {
        let row = sqlx::query(
            r#"
            update workers
            set last_heartbeat_at = to_timestamp($2),
                status = case
                    when status = 'expired' then 'ready'
                    else status
                  end
            where worker_id = $1
            returning
              worker_id,
              endpoint,
              availability_zone,
              max_assignments,
              labels,
              status,
              extract(epoch from registered_at)::bigint as registered_at_epoch_secs,
              extract(epoch from last_heartbeat_at)::bigint as last_heartbeat_at_epoch_secs,
              (
                select count(*)
                from partition_assignments pa
                where pa.worker_id = workers.worker_id
              )::bigint as active_assignments
            "#,
        )
        .bind(worker_id)
        .bind(i64::try_from(heartbeat_epoch_secs)?)
        .fetch_optional(&self.pool)
        .await?;

        row.map(map_worker_record).transpose()
    }

    async fn list_workers(&self, status: Option<WorkerStatus>) -> Result<Vec<WorkerRecord>> {
        let rows = sqlx::query(
            r#"
            select
              w.worker_id,
              w.endpoint,
              w.availability_zone,
              w.max_assignments,
              w.labels,
              w.status,
              extract(epoch from w.registered_at)::bigint as registered_at_epoch_secs,
              extract(epoch from w.last_heartbeat_at)::bigint as last_heartbeat_at_epoch_secs,
              count(pa.assignment_id)::bigint as active_assignments
            from workers w
            left join partition_assignments pa
              on pa.worker_id = w.worker_id
            where ($1::text is null or w.status = $1)
            group by
              w.worker_id,
              w.endpoint,
              w.availability_zone,
              w.max_assignments,
              w.labels,
              w.status,
              w.registered_at,
              w.last_heartbeat_at
            order by w.worker_id
            "#,
        )
        .bind(status.map(worker_status_to_db))
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(map_worker_record).collect()
    }

    async fn get_pipeline(
        &self,
        tenant_id: &str,
        pipeline_id: &str,
    ) -> Result<Option<PipelineSpec>> {
        let row = sqlx::query(
            r#"
            select pv.spec
            from pipelines p
            join pipeline_versions pv
              on pv.tenant_id = p.tenant_id
             and pv.pipeline_id = p.pipeline_id
             and pv.version = p.current_version
            where p.tenant_id = $1 and p.pipeline_id = $2
            "#,
        )
        .bind(tenant_id)
        .bind(pipeline_id)
        .fetch_optional(&self.pool)
        .await?;

        let Some(row) = row else {
            return Ok(None);
        };

        let Json(pipeline) = row.try_get::<Json<PipelineSpec>, _>("spec")?;
        Ok(Some(pipeline))
    }

    async fn list_assignments(
        &self,
        tenant_id: &str,
        pipeline_id: &str,
    ) -> Result<Vec<PartitionAssignment>> {
        let rows = sqlx::query(
            r#"
            select tenant_id, pipeline_id, version, partition_id, worker_id, lease_epoch
            from partition_assignments
            where tenant_id = $1 and pipeline_id = $2
            order by partition_id
            "#,
        )
        .bind(tenant_id)
        .bind(pipeline_id)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(map_partition_assignment).collect()
    }

    async fn list_assignments_for_worker(
        &self,
        worker_id: &str,
    ) -> Result<Vec<PartitionAssignment>> {
        let rows = sqlx::query(
            r#"
            select tenant_id, pipeline_id, version, partition_id, worker_id, lease_epoch
            from partition_assignments
            where worker_id = $1
            order by tenant_id, pipeline_id, partition_id
            "#,
        )
        .bind(worker_id)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(map_partition_assignment).collect()
    }

    async fn replace_assignments(
        &self,
        tenant_id: &str,
        pipeline_id: &str,
        version: u32,
        lease_epoch: u64,
        lease_expires_at_epoch_secs: u64,
        assignments: &[PartitionAssignment],
    ) -> Result<Vec<PartitionAssignment>> {
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            r#"
            delete from partition_assignments
            where tenant_id = $1 and pipeline_id = $2
            "#,
        )
        .bind(tenant_id)
        .bind(pipeline_id)
        .execute(tx.as_mut())
        .await?;

        for assignment in assignments {
            sqlx::query(
                r#"
                insert into partition_assignments (
                  assignment_id,
                  tenant_id,
                  pipeline_id,
                  version,
                  partition_id,
                  worker_id,
                  lease_epoch,
                  lease_expires_at
                )
                values ($1, $2, $3, $4, $5, $6, $7, to_timestamp($8))
                "#,
            )
            .bind(Uuid::now_v7())
            .bind(tenant_id)
            .bind(pipeline_id)
            .bind(i64::from(version))
            .bind(i32::try_from(assignment.partition_id)?)
            .bind(&assignment.worker_id)
            .bind(i64::try_from(lease_epoch)?)
            .bind(i64::try_from(lease_expires_at_epoch_secs)?)
            .execute(tx.as_mut())
            .await?;
        }

        tx.commit().await?;
        Ok(assignments.to_vec())
    }

    async fn expire_workers(&self, stale_before_epoch_secs: u64) -> Result<LeaseExpiryOutcome> {
        let mut tx = self.pool.begin().await?;
        let rows = sqlx::query(
            r#"
            update workers
            set status = 'expired'
            where status <> 'expired'
              and extract(epoch from last_heartbeat_at)::bigint < $1
            returning worker_id
            "#,
        )
        .bind(i64::try_from(stale_before_epoch_secs)?)
        .fetch_all(tx.as_mut())
        .await?;

        let expired_worker_ids = rows
            .into_iter()
            .map(|row| row.try_get::<String, _>("worker_id"))
            .collect::<std::result::Result<Vec<_>, _>>()?;

        let mut revoked_assignment_count = 0_u32;
        for worker_id in &expired_worker_ids {
            let result = sqlx::query(
                r#"
                delete from partition_assignments
                where worker_id = $1
                "#,
            )
            .bind(worker_id)
            .execute(tx.as_mut())
            .await?;
            revoked_assignment_count += u32::try_from(result.rows_affected()).unwrap_or(u32::MAX);
        }

        tx.commit().await?;

        Ok(LeaseExpiryOutcome {
            expired_worker_ids,
            revoked_assignment_count,
        })
    }
}

#[async_trait]
impl ProcessorRepository for PostgresMetadataRepository {
    async fn list_assignments_for_worker(
        &self,
        worker_id: &str,
    ) -> Result<Vec<PartitionAssignment>> {
        CoordinatorRepository::list_assignments_for_worker(self, worker_id).await
    }

    async fn get_pipeline_version(
        &self,
        tenant_id: &str,
        pipeline_id: &str,
        version: u32,
    ) -> Result<Option<PipelineSpec>> {
        let row = sqlx::query(
            r#"
            select spec
            from pipeline_versions
            where tenant_id = $1 and pipeline_id = $2 and version = $3
            "#,
        )
        .bind(tenant_id)
        .bind(pipeline_id)
        .bind(i64::from(version))
        .fetch_optional(&self.pool)
        .await?;

        let Some(row) = row else {
            return Ok(None);
        };

        let Json(pipeline) = row.try_get::<Json<PipelineSpec>, _>("spec")?;
        Ok(Some(pipeline))
    }

    async fn latest_checkpoint(
        &self,
        tenant_id: &str,
        pipeline_id: &str,
        partition_id: u32,
    ) -> Result<Option<LoadedCheckpoint>> {
        let row = sqlx::query(
            r#"
            select partition_id, kafka_offset, snapshot_uri, snapshot_version, snapshot_state
            from checkpoints
            where tenant_id = $1 and pipeline_id = $2 and partition_id = $3
            order by created_at desc
            limit 1
            "#,
        )
        .bind(tenant_id)
        .bind(pipeline_id)
        .bind(i32::try_from(partition_id)?)
        .fetch_optional(&self.pool)
        .await?;

        row.map(|row| {
            let Json(snapshot_state) =
                row.try_get::<Json<serde_json::Value>, _>("snapshot_state")?;
            Ok(LoadedCheckpoint {
                summary: event_pipeline_types::CheckpointSummary {
                    partition_id: u32::try_from(row.try_get::<i32, _>("partition_id")?)?,
                    offset: u64::try_from(row.try_get::<i64, _>("kafka_offset")?)?,
                    snapshot_uri: row.try_get("snapshot_uri")?,
                    snapshot_version: u32::try_from(row.try_get::<i64, _>("snapshot_version")?)?,
                },
                snapshot_state,
            })
        })
        .transpose()
    }

    async fn fetch_partition_events(
        &self,
        tenant_id: &str,
        pipeline_id: &str,
        partition_id: u32,
        offset_gt: u64,
        limit: usize,
    ) -> Result<Vec<PipelineEvent>> {
        let rows = sqlx::query(
            r#"
            select
              tenant_id,
              pipeline_id,
              source_id,
              partition_id,
              event_offset,
              record_key,
              payload,
              (extract(epoch from event_time) * 1000)::bigint as event_time_epoch_ms
            from source_events
            where tenant_id = $1
              and pipeline_id = $2
              and partition_id = $3
              and event_offset > $4
            order by event_offset
            limit $5
            "#,
        )
        .bind(tenant_id)
        .bind(pipeline_id)
        .bind(i32::try_from(partition_id)?)
        .bind(i64::try_from(offset_gt)?)
        .bind(i64::try_from(limit)?)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                let Json(payload) = row.try_get::<Json<serde_json::Value>, _>("payload")?;
                Ok(PipelineEvent {
                    tenant_id: row.try_get("tenant_id")?,
                    pipeline_id: row.try_get("pipeline_id")?,
                    source_id: row.try_get("source_id")?,
                    partition_id: u32::try_from(row.try_get::<i32, _>("partition_id")?)?,
                    offset: u64::try_from(row.try_get::<i64, _>("event_offset")?)?,
                    record_key: row.try_get("record_key")?,
                    payload,
                    event_time_epoch_ms: u64::try_from(
                        row.try_get::<i64, _>("event_time_epoch_ms")?,
                    )?,
                })
            })
            .collect()
    }

    async fn append_checkpoint(&self, checkpoint: &CheckpointWrite) -> Result<LoadedCheckpoint> {
        sqlx::query(
            r#"
            insert into checkpoints (
              checkpoint_id,
              tenant_id,
              pipeline_id,
              partition_id,
              kafka_offset,
              snapshot_uri,
              snapshot_version,
              snapshot_state
            )
            values ($1, $2, $3, $4, $5, $6, $7, $8)
            "#,
        )
        .bind(Uuid::now_v7())
        .bind(&checkpoint.tenant_id)
        .bind(&checkpoint.pipeline_id)
        .bind(i32::try_from(checkpoint.partition_id)?)
        .bind(i64::try_from(checkpoint.offset)?)
        .bind(&checkpoint.snapshot_uri)
        .bind(i64::from(checkpoint.snapshot_version))
        .bind(Json(&checkpoint.snapshot_state))
        .execute(&self.pool)
        .await?;

        Ok(LoadedCheckpoint {
            summary: event_pipeline_types::CheckpointSummary {
                partition_id: checkpoint.partition_id,
                offset: checkpoint.offset,
                snapshot_uri: checkpoint.snapshot_uri.clone(),
                snapshot_version: checkpoint.snapshot_version,
            },
            snapshot_state: checkpoint.snapshot_state.clone(),
        })
    }

    async fn claim_pending_replay_job(&self, worker_id: &str) -> Result<Option<ReplayJob>> {
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query(
            r#"
            select replay_job_id
            from replay_jobs
            where status = 'pending'
            order by created_at
            limit 1
            for update skip locked
            "#,
        )
        .fetch_optional(tx.as_mut())
        .await?;

        let Some(row) = row else {
            tx.commit().await?;
            return Ok(None);
        };

        let replay_job_id = row.try_get::<Uuid, _>("replay_job_id")?;
        let row = sqlx::query(
            r#"
            update replay_jobs
            set status = 'running',
                claimed_by_worker_id = $2,
                updated_at = now()
            where replay_job_id = $1
            returning replay_job_id, tenant_id, pipeline_id, version, from_offset, to_offset, reason, status,
                      claimed_by_worker_id, last_processed_offset, error_message
            "#,
        )
        .bind(replay_job_id)
        .bind(worker_id)
        .fetch_one(tx.as_mut())
        .await?;

        tx.commit().await?;
        map_replay_job(row).map(Some)
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
        let rows = sqlx::query(
            r#"
            select
              tenant_id,
              pipeline_id,
              source_id,
              partition_id,
              event_offset,
              record_key,
              payload,
              (extract(epoch from event_time) * 1000)::bigint as event_time_epoch_ms
            from source_events
            where tenant_id = $1
              and pipeline_id = $2
              and event_offset > $3
              and event_offset >= $4
              and ($5::bigint is null or event_offset <= $5)
            order by partition_id, event_offset
            limit $6
            "#,
        )
        .bind(tenant_id)
        .bind(pipeline_id)
        .bind(i64::try_from(lower_bound)?)
        .bind(i64::try_from(from_offset)?)
        .bind(to_offset.map(i64::try_from).transpose()?)
        .bind(i64::try_from(limit)?)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(map_pipeline_event).collect()
    }

    async fn update_replay_job_progress(
        &self,
        replay_job_id: &str,
        last_processed_offset: u64,
    ) -> Result<ReplayJob> {
        let row = sqlx::query(
            r#"
            update replay_jobs
            set last_processed_offset = $2,
                updated_at = now()
            where replay_job_id = $1
            returning replay_job_id, tenant_id, pipeline_id, version, from_offset, to_offset, reason, status,
                      claimed_by_worker_id, last_processed_offset, error_message
            "#,
        )
        .bind(Uuid::parse_str(replay_job_id)?)
        .bind(i64::try_from(last_processed_offset)?)
        .fetch_one(&self.pool)
        .await?;

        map_replay_job(row)
    }

    async fn complete_replay_job(
        &self,
        replay_job_id: &str,
        last_processed_offset: Option<u64>,
    ) -> Result<ReplayJob> {
        let row = sqlx::query(
            r#"
            update replay_jobs
            set status = 'succeeded',
                last_processed_offset = coalesce($2, last_processed_offset),
                updated_at = now()
            where replay_job_id = $1
            returning replay_job_id, tenant_id, pipeline_id, version, from_offset, to_offset, reason, status,
                      claimed_by_worker_id, last_processed_offset, error_message
            "#,
        )
        .bind(Uuid::parse_str(replay_job_id)?)
        .bind(last_processed_offset.map(i64::try_from).transpose()?)
        .fetch_one(&self.pool)
        .await?;

        map_replay_job(row)
    }

    async fn fail_replay_job(
        &self,
        replay_job_id: &str,
        error_message: &str,
        last_processed_offset: Option<u64>,
    ) -> Result<ReplayJob> {
        let row = sqlx::query(
            r#"
            update replay_jobs
            set status = 'failed',
                error_message = $2,
                last_processed_offset = coalesce($3, last_processed_offset),
                updated_at = now()
            where replay_job_id = $1
            returning replay_job_id, tenant_id, pipeline_id, version, from_offset, to_offset, reason, status,
                      claimed_by_worker_id, last_processed_offset, error_message
            "#,
        )
        .bind(Uuid::parse_str(replay_job_id)?)
        .bind(error_message)
        .bind(last_processed_offset.map(i64::try_from).transpose()?)
        .fetch_one(&self.pool)
        .await?;

        map_replay_job(row)
    }

    async fn append_dead_letter(&self, dead_letter: &DeadLetterWrite) -> Result<DeadLetterRecord> {
        sqlx::query(
            r#"
            insert into dead_letters (
              dead_letter_id,
              tenant_id,
              pipeline_id,
              source_id,
              partition_id,
              event_offset,
              record_key,
              failure_reason,
              retryable,
              payload
            )
            values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            "#,
        )
        .bind(Uuid::now_v7())
        .bind(&dead_letter.tenant_id)
        .bind(&dead_letter.pipeline_id)
        .bind(&dead_letter.source_id)
        .bind(i32::try_from(dead_letter.partition_id)?)
        .bind(i64::try_from(dead_letter.event_offset)?)
        .bind(&dead_letter.record_key)
        .bind(&dead_letter.failure_reason)
        .bind(dead_letter.retryable)
        .bind(Json(&dead_letter.payload))
        .execute(&self.pool)
        .await?;

        Ok(DeadLetterRecord {
            source_id: dead_letter.source_id.clone(),
            partition_id: dead_letter.partition_id,
            event_offset: dead_letter.event_offset,
            record_key: dead_letter.record_key.clone(),
            failure_reason: dead_letter.failure_reason.clone(),
            retryable: dead_letter.retryable,
        })
    }
}

fn map_registered_topic(row: sqlx::postgres::PgRow) -> Result<RegisteredTopic> {
    Ok(RegisteredTopic {
        tenant_id: row.try_get("tenant_id")?,
        topic_name: row.try_get("topic_name")?,
        partition_count: u16::try_from(row.try_get::<i32, _>("partition_count")?)?,
        retention_hours: u32::try_from(row.try_get::<i32, _>("retention_hours")?)?,
    })
}

fn map_pipeline_event(row: sqlx::postgres::PgRow) -> Result<PipelineEvent> {
    let Json(payload) = row.try_get::<Json<serde_json::Value>, _>("payload")?;
    Ok(PipelineEvent {
        tenant_id: row.try_get("tenant_id")?,
        pipeline_id: row.try_get("pipeline_id")?,
        source_id: row.try_get("source_id")?,
        partition_id: u32::try_from(row.try_get::<i32, _>("partition_id")?)?,
        offset: u64::try_from(row.try_get::<i64, _>("event_offset")?)?,
        record_key: row.try_get("record_key")?,
        payload,
        event_time_epoch_ms: u64::try_from(row.try_get::<i64, _>("event_time_epoch_ms")?)?,
    })
}

fn map_replay_job(row: sqlx::postgres::PgRow) -> Result<ReplayJob> {
    Ok(ReplayJob {
        replay_job_id: row.try_get::<Uuid, _>("replay_job_id")?.to_string(),
        tenant_id: row.try_get("tenant_id")?,
        pipeline_id: row.try_get("pipeline_id")?,
        version: u32::try_from(row.try_get::<i64, _>("version")?)?,
        from_offset: u64::try_from(row.try_get::<i64, _>("from_offset")?)?,
        to_offset: row
            .try_get::<Option<i64>, _>("to_offset")?
            .map(u64::try_from)
            .transpose()?,
        reason: row.try_get("reason")?,
        status: replay_job_status_from_db(&row.try_get::<String, _>("status")?)?,
        claimed_by_worker_id: row.try_get("claimed_by_worker_id")?,
        last_processed_offset: row
            .try_get::<Option<i64>, _>("last_processed_offset")?
            .map(u64::try_from)
            .transpose()?,
        error_message: row.try_get("error_message")?,
    })
}

fn map_partition_assignment(row: sqlx::postgres::PgRow) -> Result<PartitionAssignment> {
    Ok(PartitionAssignment {
        tenant_id: row.try_get("tenant_id")?,
        pipeline_id: row.try_get("pipeline_id")?,
        version: u32::try_from(row.try_get::<i64, _>("version")?)?,
        partition_id: u32::try_from(row.try_get::<i32, _>("partition_id")?)?,
        worker_id: row.try_get("worker_id")?,
        lease_epoch: u64::try_from(row.try_get::<i64, _>("lease_epoch")?)?,
    })
}

fn map_tenant(row: sqlx::postgres::PgRow) -> Result<TenantRecord> {
    Ok(TenantRecord {
        tenant_id: row.try_get("tenant_id")?,
        display_name: row.try_get("display_name")?,
        oidc_realm: row.try_get("oidc_realm")?,
        enabled: row.try_get("enabled")?,
    })
}

fn map_worker_record(row: sqlx::postgres::PgRow) -> Result<WorkerRecord> {
    let Json(labels) = row.try_get::<Json<Vec<String>>, _>("labels")?;

    Ok(WorkerRecord {
        worker_id: row.try_get("worker_id")?,
        endpoint: row.try_get("endpoint")?,
        availability_zone: row.try_get("availability_zone")?,
        max_assignments: u16::try_from(row.try_get::<i32, _>("max_assignments")?)?,
        labels,
        status: worker_status_from_db(&row.try_get::<String, _>("status")?)?,
        active_assignments: u16::try_from(row.try_get::<i64, _>("active_assignments")?)?,
        registered_at_epoch_secs: u64::try_from(
            row.try_get::<i64, _>("registered_at_epoch_secs")?,
        )?,
        last_heartbeat_at_epoch_secs: u64::try_from(
            row.try_get::<i64, _>("last_heartbeat_at_epoch_secs")?,
        )?,
    })
}

fn deployment_state_to_db(state: DeploymentState) -> &'static str {
    match state {
        DeploymentState::Draft => "draft",
        DeploymentState::Validated => "validated",
        DeploymentState::Deploying => "deploying",
        DeploymentState::Running => "running",
        DeploymentState::Paused => "paused",
        DeploymentState::Failed => "failed",
    }
}

fn deployment_state_from_db(value: &str) -> Result<DeploymentState> {
    match value {
        "draft" => Ok(DeploymentState::Draft),
        "validated" => Ok(DeploymentState::Validated),
        "deploying" => Ok(DeploymentState::Deploying),
        "running" => Ok(DeploymentState::Running),
        "paused" => Ok(DeploymentState::Paused),
        "failed" => Ok(DeploymentState::Failed),
        _ => Err(anyhow!("unknown deployment state: {value}")),
    }
}

fn replay_job_status_to_db(status: ReplayJobStatus) -> &'static str {
    match status {
        ReplayJobStatus::Pending => "pending",
        ReplayJobStatus::Running => "running",
        ReplayJobStatus::Succeeded => "succeeded",
        ReplayJobStatus::Failed => "failed",
    }
}

fn replay_job_status_from_db(value: &str) -> Result<ReplayJobStatus> {
    match value {
        "pending" => Ok(ReplayJobStatus::Pending),
        "running" => Ok(ReplayJobStatus::Running),
        "succeeded" => Ok(ReplayJobStatus::Succeeded),
        "failed" => Ok(ReplayJobStatus::Failed),
        other => bail!("unsupported replay job status {other}"),
    }
}

fn subject_kind_to_db(kind: SubjectKind) -> &'static str {
    match kind {
        SubjectKind::User => "user",
        SubjectKind::ServiceAccount => "service_account",
    }
}

fn subject_kind_from_db(value: &str) -> Result<SubjectKind> {
    match value {
        "user" => Ok(SubjectKind::User),
        "service_account" => Ok(SubjectKind::ServiceAccount),
        _ => Err(anyhow!("unknown subject kind: {value}")),
    }
}

fn role_to_db(role: PlatformRole) -> &'static str {
    match role {
        PlatformRole::PlatformAdmin => "platform_admin",
        PlatformRole::TenantAdmin => "tenant_admin",
        PlatformRole::PipelineEditor => "pipeline_editor",
        PlatformRole::PipelineViewer => "pipeline_viewer",
        PlatformRole::TopicEditor => "topic_editor",
        PlatformRole::TopicViewer => "topic_viewer",
        PlatformRole::ReplayOperator => "replay_operator",
    }
}

fn role_from_db(value: &str) -> Result<PlatformRole> {
    match value {
        "platform_admin" => Ok(PlatformRole::PlatformAdmin),
        "tenant_admin" => Ok(PlatformRole::TenantAdmin),
        "pipeline_editor" => Ok(PlatformRole::PipelineEditor),
        "pipeline_viewer" => Ok(PlatformRole::PipelineViewer),
        "topic_editor" => Ok(PlatformRole::TopicEditor),
        "topic_viewer" => Ok(PlatformRole::TopicViewer),
        "replay_operator" => Ok(PlatformRole::ReplayOperator),
        _ => Err(anyhow!("unknown platform role: {value}")),
    }
}

fn worker_status_to_db(status: WorkerStatus) -> &'static str {
    match status {
        WorkerStatus::Ready => "ready",
        WorkerStatus::Draining => "draining",
        WorkerStatus::Expired => "expired",
    }
}

fn worker_status_from_db(value: &str) -> Result<WorkerStatus> {
    match value {
        "ready" => Ok(WorkerStatus::Ready),
        "draining" => Ok(WorkerStatus::Draining),
        "expired" => Ok(WorkerStatus::Expired),
        _ => Err(anyhow!("unknown worker status: {value}")),
    }
}
