create table if not exists workers (
  worker_id text primary key,
  endpoint text not null,
  availability_zone text not null,
  max_assignments integer not null check (max_assignments > 0),
  labels jsonb not null default '[]'::jsonb,
  status text not null,
  registered_at timestamptz not null default now(),
  last_heartbeat_at timestamptz not null default now()
);

create table if not exists partition_assignments (
  assignment_id uuid primary key,
  tenant_id text not null,
  pipeline_id text not null,
  version bigint not null check (version > 0),
  partition_id integer not null check (partition_id >= 0),
  worker_id text not null references workers (worker_id) on delete cascade,
  lease_epoch bigint not null check (lease_epoch > 0),
  lease_expires_at timestamptz not null,
  assigned_at timestamptz not null default now(),
  unique (tenant_id, pipeline_id, version, partition_id),
  foreign key (tenant_id, pipeline_id, version) references pipeline_versions (tenant_id, pipeline_id, version) on delete cascade
);

create index if not exists idx_workers_status
  on workers (status, last_heartbeat_at desc);

create index if not exists idx_partition_assignments_pipeline
  on partition_assignments (tenant_id, pipeline_id, version, partition_id);

create index if not exists idx_partition_assignments_worker
  on partition_assignments (worker_id, lease_expires_at desc);
