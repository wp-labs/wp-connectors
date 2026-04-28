#![cfg(all(
    feature = "mysql",
    any(feature = "external_integration", feature = "external_performance")
))]

#[cfg(any(feature = "external_integration", feature = "external_performance"))]
#[path = "common/mod.rs"]
mod common;

#[cfg(any(feature = "external_integration", feature = "external_performance"))]
#[path = "mysql/common.rs"]
mod mysql_common;

#[cfg(feature = "external_integration")]
#[path = "mysql/sinks/integration_tests.rs"]
mod integration_tests;

#[cfg(feature = "external_performance")]
#[path = "mysql/sinks/performance_tests.rs"]
mod performance_tests;
