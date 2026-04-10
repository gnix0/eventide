create table if not exists role_bindings (
  binding_id uuid primary key,
  subject_kind text not null,
  subject_id text not null,
  tenant_id text references tenants (tenant_id) on delete cascade,
  role text not null,
  created_at timestamptz not null default now(),
  unique (subject_kind, subject_id, tenant_id, role)
);

create table if not exists service_accounts (
  service_account_id uuid primary key,
  tenant_id text not null references tenants (tenant_id) on delete cascade,
  client_id text not null unique,
  display_name text not null,
  enabled boolean not null default true,
  created_at timestamptz not null default now()
);

create index if not exists idx_role_bindings_subject
  on role_bindings (subject_id, tenant_id);

create index if not exists idx_service_accounts_tenant
  on service_accounts (tenant_id, client_id);
