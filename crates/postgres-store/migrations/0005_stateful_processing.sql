alter table replay_jobs
  add column if not exists version bigint not null default 1,
  add column if not exists claimed_by_worker_id text,
  add column if not exists last_processed_offset bigint,
  add column if not exists error_message text,
  add column if not exists updated_at timestamptz not null default now();

alter table checkpoints
  add column if not exists snapshot_version bigint not null default 1,
  add column if not exists snapshot_state jsonb not null default '{}'::jsonb;

alter table dead_letters
  add column if not exists source_id text not null default '',
  add column if not exists partition_id integer not null default 0,
  add column if not exists event_offset bigint not null default 0,
  add column if not exists payload jsonb not null default 'null'::jsonb;

create index if not exists idx_replay_jobs_status
  on replay_jobs (status, created_at asc);

create index if not exists idx_dead_letters_partition_offset
  on dead_letters (tenant_id, pipeline_id, partition_id, event_offset desc);
