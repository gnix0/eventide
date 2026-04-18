RUSTFLAGS ?=

DATABASE_URL ?= postgres://postgres:postgres@127.0.0.1:5432/eventide

.PHONY: fmt fmt-check lint test build check db-up db-down migrate control-plane

fmt:
	cargo fmt --all

fmt-check:
	cargo fmt --all --check

lint:
	cargo clippy --workspace --all-targets -- -D warnings

test:
	cargo test --workspace

build:
	cargo build --workspace

check:
	cargo check --workspace

db-up:
	bash scripts/postgres-up.sh

db-down:
	bash scripts/postgres-down.sh

migrate:
	DATABASE_URL=$(DATABASE_URL) cargo +stable run -p control-plane -- --migrate-only

control-plane:
	DATABASE_URL=$(DATABASE_URL) cargo +stable run -p control-plane
