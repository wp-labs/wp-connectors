# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.7.4] - 2026-01-12

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

[Unreleased]: https://github.com/wp-labs/wp-connectors/compare/v0.7.4...HEAD
[0.7.4]: https://github.com/wp-labs/wp-connectors/compare/v0.7.4-alpha...v0.7.4
[0.7.4-alpha]: https://github.com/wp-labs/wp-connectors/compare/v0.7.3-alpha...v0.7.4-alpha
[0.7.3-alpha]: https://github.com/wp-labs/wp-connectors/compare/v0.7.2-beta...v0.7.3-alpha
[0.7.2-alpha]: https://github.com/wp-labs/wp-connectors/compare/v0.7.2-alpha...v0.7.2-beta
[0.7.2-beta]: https://github.com/wp-labs/wp-connectors/compare/v0.7.1-alpha...v0.7.2-beta
[0.7.1-alpha]: https://github.com/wp-labs/wp-connectors/compare/v0.7.0-alpha...v0.7.1-alpha
[0.7.0-alpha]: https://github.com/wp-labs/wp-connectors/releases/tag/v0.7.0-alpha
