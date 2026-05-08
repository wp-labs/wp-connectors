# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## [Unreleased]

## [0.14.0] - 2026-05-08

### Changed
- Migrate component testing infrastructure from `anyhow::Result` to OrionError system
- Migrate sink and HTTP source constructors from `anyhow::Result` to `SinkResult`/`SourceResult`
- Adapt connector error handling for `wp-connector-api` `0.10.1` unit `SinkReason`/`SourceReason` variants
- Store connector error messages on `StructError.detail` instead of reason variant payloads
- Preserve raw external errors with `source_raw_err(...)`/`with_source(...)` where supported
- Replace runtime `anyhow::Result` constructors with `SourceResult`/`SinkResult` and structured Postgres `PgReason`/`PgResult` internals
- Move `anyhow` out of runtime dependencies and keep it only for test helpers
- Restore `Tags::from_parse(...)` for source metadata tags to keep existing tag parsing semantics
- Extract test infrastructure to `wp-connector-test-utils` crate
- Extract `dmdb` connector into separate `wp-connectors-labs` crate
- Remove doc examples from `clickhouse`, `elasticsearch`, `http` module docs
- Remove flaky timing assertion from `send_with_retry_exponential_backoff_timing` test

### Fixed
- Update unit tests to assert reason classification separately from `StructError.detail`
- Fix test assertions for OrionError Display format changes
- Fix `ErrorOweBase` and `ToStructError` import paths for orion-error 0.7.2
- Fix `StdError` conversion in Kafka source `create_topics()`
- Fix doctest compilation for `SinkError`/`SourceError` in doc examples

## [0.12.0] - 2026-04-11

### Changed
- Bump project version to `0.12.0`
- Rework VictoriaMetrics exporter metrics collection and labels compared with `0.10.0`
- Add process CPU and memory gauges for the current connector process via `sysinfo`
- Align VictoriaMetrics source/parse/sink labels with pipeline fields such as `wp_source_type`, `wp_package_name`, `wp_rule_name`, and `wp_sink_group`
- Simplify VictoriaMetrics parse counting to export total parsed records through `wparse_parse_all`
- Clean `Cargo.toml` dependencies by removing unused `wp-schema` and `urlencoding`
- Move test-only `rand` and `sqlx` dependencies into `dev-dependencies`
- Drop duplicated `chrono` dev dependency while keeping runtime usage unchanged

## [0.10.0] - 2026-03-12

### Added
- Add HTTP sink connector with configurable `endpoint`, `method`, `headers`, basic auth, batching, retries, and optional gzip compression
- Add HTTP sink examples and a local test server under `examples/http/`
- Add Postgres sink support with dedicated `postgres` connector module, factory, config parser, and sink implementation

### Changed
- Upgrade core WP dependencies to 0.8 series (`wp-connector-api`, `wp-parse-api`, `wp-error`, `wp-specs`, `wp-conf-base`, `wp-log`)
- Upgrade Orion dependencies (`orion-error` to 0.6, `orion_conf` to 0.5)
- Bump project version to `0.10.0`
- Clarify Elasticsearch connector support in 0.10 (`elasticsearch` feature, included in default features)
- Adopt version channel policy: odd `minor` for integration, even `minor` for stable, `patch` for fixes only
- Enable the `http` connector in the default feature set
- Change ClickHouse sink configuration from `host` to `endpoint` and support explicit HTTP(S) endpoints
- Enable the `postgres` connector in the default and `full` feature sets
- Add Postgres connection support in shared database dependencies (`sqlx-postgres` via SeaORM)

### Fixed
- Fix Kafka/MySQL build breaks after dependency upgrades
- Align `RawData` import with new public path (`wp_model_core::raw::RawData`)
- Align error handling with `orion-error` 0.6 (`ErrorOweBase`, `from_validation()`, `UvsReason::data_error()`)
- Refine HTTP sink JSON payload handling and related examples
- Correct a Postgres sink test case
- Harden Doris Stream Load handling for `Publish Timeout`, `Label Already Exists`, filtered-row partial success, and non-retryable `Fail` responses
- Generate deterministic Doris load labels from batch payloads so caller-level retries can preserve Stream Load idempotency
- Make Doris sink shutdown terminal and validate retry configuration values more strictly
- Add Doris unit and `httpmock` coverage for retry, conflict, and partial-failure paths

## [0.7.8] - 2026-02-27

### Changed
- Change license from Elastic License 2.0 to Apache License 2.0
- Update LICENSE file with Apache 2.0 full text
- Update `license` field in Cargo.toml to `Apache-2.0`
- Update reqwest requirement from 0.12 to 0.13 (#45)
- Update env_logger requirement from 0.10 to 0.11 (#46)
- Refactor Doris connector: migrate from MySQL protocol to Stream Load API (#49)

### Added
- Add CONTRIBUTING.md with branch strategy and contribution guidelines (bilingual EN/CN)
- Add comprehensive README.md with connector overview, features, and project structure (bilingual EN/CN)

### Fixed
- Remove absolute reliance on the field `wp_event_id` in MySQL sink (#53)
- Fix Clippy warnings in VictoriaLogs sink

## [0.7.6] - 2026-01-12

### Added
- Add version setting in Cargo.toml workspace configuration

### Fixed
- Fix abnormal rescue data handling in MySQL sink ([#36](https://github.com/wp-labs/wp-connectors/issues/36))

### Changed
- Update dependencies to latest versions

## [0.7.4-alpha] - 2026-01-10

### Changed
- Update dependencies

## [0.7.3-alpha] - Previous Release

### Changed
- Bump version to 0.7.3
- Update dependencies to latest versions

## [0.7.2-alpha] - Previous Release

### Changed
- Update dependencies and version to 0.7.2

## [0.7.2-beta] - Previous Release

### Added
- Add Doris connector support
- Add VictoriaMetrics sink support

### Changed
- Update CI configuration
- Remove Cargo.lock from version control

### Security
- Add security audit configuration (audit.toml)
- Add security decision record for RSA vulnerability
- Downgrade reqwest to address security concerns
- Remove unused dependencies with security issues

## [0.7.1-alpha] - Previous Release

### Changed
- Update CI and Codecov badge URLs to new repository location

## [0.7.0-alpha] - Previous Release

Initial 0.7.x series release.

[Unreleased]: https://github.com/wp-labs/wp-connectors/compare/v0.14.0...HEAD
[0.14.0]: https://github.com/wp-labs/wp-connectors/compare/v0.12.0...v0.14.0
[0.12.0]: https://github.com/wp-labs/wp-connectors/compare/v0.10.0...v0.12.0
[0.10.0]: https://github.com/wp-labs/wp-connectors/compare/v0.7.8...v0.10.0
[0.8.0]: https://github.com/wp-labs/wp-connectors/compare/v0.7.8...v0.8.0
[0.7.8]: https://github.com/wp-labs/wp-connectors/compare/v0.7.7...v0.7.8
[0.7.7]: https://github.com/wp-labs/wp-connectors/compare/v0.7.6...v0.7.7
[0.7.6]: https://github.com/wp-labs/wp-connectors/compare/v0.7.5...v0.7.6
[0.7.5]: https://github.com/wp-labs/wp-connectors/compare/v0.7.4...v0.7.5
[0.7.4]: https://github.com/wp-labs/wp-connectors/compare/v0.7.4-alpha...v0.7.4
[0.7.4-alpha]: https://github.com/wp-labs/wp-connectors/compare/v0.7.3-alpha...v0.7.4-alpha
[0.7.3-alpha]: https://github.com/wp-labs/wp-connectors/compare/v0.7.2-beta...v0.7.3-alpha
[0.7.2-alpha]: https://github.com/wp-labs/wp-connectors/compare/v0.7.2-alpha...v0.7.2-beta
[0.7.2-beta]: https://github.com/wp-labs/wp-connectors/compare/v0.7.1-alpha...v0.7.2-beta
[0.7.1-alpha]: https://github.com/wp-labs/wp-connectors/compare/v0.7.0-alpha...v0.7.1-alpha
[0.7.0-alpha]: https://github.com/wp-labs/wp-connectors/releases/tag/v0.7.0-alpha
