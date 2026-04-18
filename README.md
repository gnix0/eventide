# eventide

`eventide` is a Rust stream-processing control plane and processor runtime focused on pipeline metadata, worker coordination, stateful execution, replay, and runtime introspection.

## Architecture

The workspace is organized as a set of service binaries and reusable domain crates:

- `control-plane`: pipeline, topic, tenant, RBAC, service-account, and replay metadata bootstrap
- `coordinator`: worker registration, heartbeats, partition assignment, rebalance, and stale-worker lease expiry
- `processor-runtime`: assignment polling, source-event processing, operator execution, checkpointing, replay, and dead-letter handling
- `query-service`: runtime read model for run status, assignments, checkpoints, replay jobs, and dead letters
- `api-gateway`, `state-manager`, and `sink-writer`: service entrypoints using the shared runtime bootstrap

Core crates:

- `crates/types`: domain contracts, pipeline validation, auth/RBAC records, worker records, and query result types
- `crates/control-plane-app`: metadata and identity use cases
- `crates/coordinator-app`: worker lease and partition assignment logic
- `crates/processor-app`: processor loop, operator execution, state snapshots, replay, and dead-letter behavior
- `crates/query-app`: runtime introspection and derived status views
- `crates/postgres-store`: Postgres-backed repositories and migrations
- `crates/auth`: OIDC/JWT validation helpers
- `crates/config`: environment-backed service configuration
- `crates/runtime`: tracing and service bootstrap helpers

## Processing Model

A pipeline is represented by a validated `PipelineSpec`:

- one or more source topics
- an acyclic operator graph
- one or more sinks
- deployment settings such as parallelism, checkpoint interval, and max in-flight messages
- replay policy and deployment state

Pipeline validation covers:

- required tenant, pipeline, source, operator, and sink fields
- duplicate source/operator/sink identifiers
- invalid upstream references
- operator graph cycles
- window configuration
- partition compatibility across joins and Kafka sinks

Supported operator shapes include:

- `Filter`
- `Map`
- `KeyBy`
- `Window`
- `Aggregate`
- `Enrich`
- `Join`

The processor runtime executes assigned partitions from persisted source events, emits sink records, advances checkpoints, and keeps partition-local state for stateful operators.

## Stateful Runtime

The processor runtime includes:

- partition assignment loading for a specific worker
- checkpoint restore from the latest persisted checkpoint
- assignment-local state snapshots
- aggregate state handling
- partition-local join state
- source-event batch processing
- replay/backfill job claiming and progress tracking
- per-record dead-letter persistence for unrecoverable events

Replay jobs are durable records with:

- replay job id
- tenant and pipeline identity
- target pipeline version
- bounded offset range
- status
- claimed worker id
- last processed offset
- error message
- created and updated timestamps

Dead-letter records include:

- source id
- partition id
- event offset
- record key
- failure reason
- retryable flag
- creation timestamp

## Coordination

The coordinator owns worker and assignment metadata:

- worker registration
- heartbeat updates
- worker status tracking
- assignment return on heartbeat
- partition rebalance across ready workers
- stale-worker lease expiry
- assignment revocation for expired workers

Assignments include:

- tenant id
- pipeline id
- pipeline version
- partition id
- worker id
- lease epoch

## Query And Introspection

The query layer exposes runtime state as application-level read models rather than raw table access.

It can derive:

- pipeline run status
- consumer lag estimate
- checkpoint age
- active assignment count
- ready worker count
- replay backlog
- dead-letter count

Queryable artifacts include:

- assignments
- replay jobs
- checkpoint history
- dead letters
- workers

## Contracts

The protobuf contract lives under `proto/eventide/v1/eventide.proto`.

It defines:

- `PipelineService`
- `TopicService`
- `AdminService`
- `RunService`
- `CoordinatorService`

The Rust application layer uses domain structs that mirror the protobuf contract boundaries.

## Persistence

Postgres is the system of record. Migrations live under `crates/postgres-store/migrations`.

Migration coverage includes:

- metadata tables for tenants, topics, pipelines, and versions
- RBAC and service-account state
- worker registry, leases, and assignments
- source events and checkpoints
- replay jobs
- checkpoint snapshot payloads
- dead letters

Default local database URL:

```bash
postgres://postgres:postgres@127.0.0.1:5432/eventide
```

## Auth

The auth layer supports OIDC/JWT validation with:

- issuer validation
- audience validation
- realm role extraction
- service-account token detection
- subject kind mapping

Default local OIDC values:

```bash
OIDC_ISSUER_URL=http://localhost:8081/realms/eventide
OIDC_AUDIENCE=eventide-api
```

Keycloak bootstrap assets live under `ops/keycloak`.

## Configuration

Runtime configuration is environment-backed:

| Variable | Default |
| --- | --- |
| `BIND_ADDR` | `0.0.0.0:8080` |
| `DATABASE_URL` | `postgres://postgres:postgres@127.0.0.1:5432/eventide` |
| `METRICS_ADDR` | `0.0.0.0:9090` |
| `PROCESSOR_WORKER_ID` | `<service>-default` |
| `PROCESSOR_POLL_INTERVAL_MS` | `5000` |
| `PROCESSOR_BATCH_SIZE` | `128` |
| `WORKER_LEASE_TTL_SECS` | `30` |
| `WORKER_STALE_AFTER_SECS` | `90` |
| `OIDC_AUDIENCE` | `eventide-api` |
| `OIDC_ISSUER_URL` | `http://localhost:8081/realms/eventide` |
| `OIDC_PUBLIC_KEY_PEM` | unset |
| `RUST_LOG` | `<service>,info` |

## Local Development

Install the pinned Rust toolchain from `rust-toolchain.toml`, then run:

```bash
cargo +stable fmt --all --check
cargo +stable test --workspace
cargo +stable clippy --workspace --all-targets -- -D warnings
cargo +stable build --workspace
```

Equivalent Make targets:

```bash
make fmt-check
make test
make lint
make build
```

Start local Postgres:

```bash
make db-up
```

Run migrations:

```bash
make migrate
```

Run the control-plane bootstrap:

```bash
make control-plane
```

Stop local Postgres:

```bash
make db-down
```

## Repository Layout

```text
apps/                 service binaries
crates/               reusable application, domain, store, auth, config, and runtime crates
ops/keycloak/         local Keycloak realm bootstrap assets
proto/                protobuf service contracts
scripts/              local developer helpers
Cargo.toml            workspace definition
Makefile              common local workflows
```

## Validation

Current automated coverage includes:

- pipeline graph and partition validation tests
- auth token validation tests
- metadata, tenant, topic, RBAC, and replay metadata tests
- coordinator registration, heartbeat, rebalance, and lease-expiry tests
- processor checkpoint, replay, join, and dead-letter tests
- query read-model and run-status tests

Primary validation command:

```bash
cargo +stable test --workspace
```

Full local gate:

```bash
cargo +stable fmt --all --check
cargo +stable test --workspace
cargo +stable clippy --workspace --all-targets -- -D warnings
cargo +stable build --workspace
```
