RUSTFLAGS ?=

.PHONY: fmt fmt-check lint test build check

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
