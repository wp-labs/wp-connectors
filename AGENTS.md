# Repository Guidelines

## Project Structure & Modules
- `src/lib.rs` exposes `kafka` module.
- `src/kafka/` contains `source.rs`, `sink.rs`, and `factory.rs` (unified Kafka Source/Sink + registration helpers).
- `tests/` holds Rust tests (see `tests/kafka/integration_tests.rs`).
- `testcase/` has runnable model/config examples used by the wider wp-engine system; useful for manual/system tests.

## Build, Test, and Development
- Build: `cargo build` (or `cargo build --release`).
- Lint/format: `cargo fmt --all` and `cargo clippy --all-targets -- -D warnings`.
- Test: `cargo test`.
  - Tip: set `SKIP_KAFKA_INTEGRATION_TESTS=1` if you add tests that require a running Kafka.

## Coding Style & Naming
- Rust edition 2024; use async Rust (Tokio) and `async-trait` for traits.
- Follow standard Rust style: 4-space indent, snake_case for functions/modules, CamelCase for types/traits.
- Prefer `anyhow::Result<T>` for fallible constructors; map external errors into project error types (`wp_err`, `SourceReason`).
- Keep modules cohesive: `source` for consume logic/topic bootstrap; `sink` for produce/formatting; `factory` for validation/build.

## Testing Guidelines
- Unit tests go near code or under `tests/`; async tests use `#[tokio::test]`.
- Avoid external dependencies in unit tests; for Kafka E2E, gate with an env flag and document prerequisites.
- Naming: `tests/<area>/*_tests.rs`; constants and helpers live in `tests/common.rs`.
- Validate configs in tests via factories:
```rust
let f = KafkaSinkFactory; f.validate_spec(&spec)?;
```

## Commit & Pull Request Guidelines
- Use clear, scoped commits. Conventional style is encouraged: `feat:`, `fix:`, `refactor:`, `docs:`, `test:`, `ci:`.
- Commit messages: short imperative title (<= 72 chars), optional body explaining what/why.
- PRs should include: purpose and scope, summary of changes, verification steps (`cargo test`, relevant scripts), and linked issues.
- Add tests for new behavior and update `testcase/` models if user-visible configs change.

## Security & Configuration Tips
- Do not hardcode credentials; pass Kafka settings via specs (`brokers`, `topic`, optional `config=["key=value"]`).
- Output format for sinks defaults to JSON; override with `fmt` (`json`, `csv`, `kv`, `raw`, `show`, `proto-text`).
- Register factories when integrating upstream:
```rust
use wp_connectors::kafka::register_factories; register_factories();
```
