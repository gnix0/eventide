create table if not exists tenants (
  tenant_id text primary key,
  display_name text not null default '',
  oidc_realm text not null default 'eventide',
  enabled boolean not null default true,
  created_at timestamptz not null default now()
);

create table if not exists topics (
  tenant_id text not null references tenants (tenant_id) on delete cascade,
  topic_name text not null,
  partition_count integer not null check (partition_count > 0),
  retention_hours integer not null check (retention_hours > 0),
  created_at timestamptz not null default now(),
  primary key (tenant_id, topic_name)
);

create table if not exists pipelines (
  tenant_id text not null references tenants (tenant_id) on delete cascade,
  pipeline_id text not null,
  current_version bigint not null check (current_version > 0),
  deployment_state text not null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (tenant_id, pipeline_id)
);

create table if not exists pipeline_versions (
  tenant_id text not null,
  pipeline_id text not null,
  version bigint not null check (version > 0),
  spec jsonb not null,
  deployment_state text not null,
  created_at timestamptz not null default now(),
  primary key (tenant_id, pipeline_id, version),
  foreign key (tenant_id, pipeline_id) references pipelines (tenant_id, pipeline_id) on delete cascade
);

create table if not exists deployments (
  deployment_id uuid primary key,
  tenant_id text not null,
  pipeline_id text not null,
  version bigint not null,
  state text not null,
  requested_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  foreign key (tenant_id, pipeline_id, version) references pipeline_versions (tenant_id, pipeline_id, version) on delete cascade
);

create table if not exists replay_jobs (
  replay_job_id uuid primary key,
  tenant_id text not null,
  pipeline_id text not null,
  from_offset bigint not null,
  to_offset bigint,
  reason text not null,
  status text not null,
  created_at timestamptz not null default now(),
  foreign key (tenant_id, pipeline_id) references pipelines (tenant_id, pipeline_id) on delete cascade
);

create table if not exists checkpoints (
  checkpoint_id uuid primary key,
  tenant_id text not null,
  pipeline_id text not null,
  partition_id integer not null,
  kafka_offset bigint not null,
  snapshot_uri text not null,
  created_at timestamptz not null default now(),
  foreign key (tenant_id, pipeline_id) references pipelines (tenant_id, pipeline_id) on delete cascade
);

create table if not exists dead_letters (
  dead_letter_id uuid primary key,
  tenant_id text not null,
  pipeline_id text not null,
  record_key text not null,
  failure_reason text not null,
  retryable boolean not null,
  created_at timestamptz not null default now(),
  foreign key (tenant_id, pipeline_id) references pipelines (tenant_id, pipeline_id) on delete cascade
);

create index if not exists idx_pipeline_versions_latest
  on pipeline_versions (tenant_id, pipeline_id, version desc);

create index if not exists idx_topics_tenant
  on topics (tenant_id);

create index if not exists idx_replay_jobs_pipeline
  on replay_jobs (tenant_id, pipeline_id, created_at desc);

create index if not exists idx_checkpoints_pipeline
  on checkpoints (tenant_id, pipeline_id, created_at desc);

create index if not exists idx_dead_letters_pipeline
  on dead_letters (tenant_id, pipeline_id, created_at desc);
