#![cfg(all(
    feature = "postgres",
    any(feature = "external_integration", feature = "external_performance")
))]

#[cfg(any(feature = "external_integration", feature = "external_performance"))]
#[path = "common/mod.rs"]
mod common;

#[cfg(any(feature = "external_integration", feature = "external_performance"))]
#[path = "postgresql/common.rs"]
mod postgresql_common;

#[cfg(feature = "external_integration")]
#[path = "postgresql/sinks/integration_tests.rs"]
mod integration_tests;

#[cfg(feature = "external_performance")]
#[path = "postgresql/sinks/performance_tests.rs"]
mod performance_tests;
