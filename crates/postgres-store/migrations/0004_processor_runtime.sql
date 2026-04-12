create table if not exists source_events (
  tenant_id text not null,
  pipeline_id text not null,
  source_id text not null,
  partition_id integer not null check (partition_id >= 0),
  event_offset bigint not null check (event_offset >= 0),
  record_key text not null,
  payload jsonb not null,
  event_time timestamptz not null default now(),
  primary key (tenant_id, pipeline_id, source_id, partition_id, event_offset),
  foreign key (tenant_id, pipeline_id) references pipelines (tenant_id, pipeline_id) on delete cascade
);

create index if not exists idx_source_events_partition_offset
  on source_events (tenant_id, pipeline_id, partition_id, event_offset);
