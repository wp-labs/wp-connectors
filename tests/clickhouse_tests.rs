#![cfg(all(
    feature = "clickhouse",
    any(feature = "external_integration", feature = "external_performance")
))]

#[cfg(any(feature = "external_integration", feature = "external_performance"))]
#[path = "common/mod.rs"]
mod common;

#[cfg(any(feature = "external_integration", feature = "external_performance"))]
#[path = "clickhouse/common.rs"]
mod clickhouse_common;

#[cfg(feature = "external_integration")]
#[path = "clickhouse/integration_tests.rs"]
mod integration_tests;

#[cfg(feature = "external_performance")]
#[path = "clickhouse/performance_tests.rs"]
mod performance_tests;
