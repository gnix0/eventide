use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use event_pipeline_control_plane_app::MetadataRepository;
use event_pipeline_types::{
    DeploymentState, PipelineSpec, PipelineSummary, RegisteredTopic, TopicSummary,
};
use sqlx::Row;
use sqlx::migrate::Migrator;
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::types::Json;

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
        tenant_id: &str,
    ) -> Result<()> {
        sqlx::query(
            "insert into tenants (tenant_id) values ($1) on conflict (tenant_id) do nothing",
        )
        .bind(tenant_id)
        .execute(tx.as_mut())
        .await?;

        Ok(())
    }
}

#[async_trait]
impl MetadataRepository for PostgresMetadataRepository {
    async fn create_pipeline(&self, pipeline: &PipelineSpec) -> Result<PipelineSummary> {
        let mut tx = self.pool.begin().await?;
        self.ensure_tenant(&mut tx, &pipeline.tenant_id).await?;

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
        self.ensure_tenant(&mut tx, &pipeline.tenant_id).await?;

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

    async fn register_topic(&self, topic: &RegisteredTopic) -> Result<RegisteredTopic> {
        let mut tx = self.pool.begin().await?;
        self.ensure_tenant(&mut tx, &topic.tenant_id).await?;

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
}

fn map_registered_topic(row: sqlx::postgres::PgRow) -> Result<RegisteredTopic> {
    Ok(RegisteredTopic {
        tenant_id: row.try_get("tenant_id")?,
        topic_name: row.try_get("topic_name")?,
        partition_count: u16::try_from(row.try_get::<i32, _>("partition_count")?)?,
        retention_hours: u32::try_from(row.try_get::<i32, _>("retention_hours")?)?,
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
