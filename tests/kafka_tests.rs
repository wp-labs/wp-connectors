#![cfg(all(
    feature = "kafka",
    any(feature = "external_integration", feature = "external_performance")
))]

#[cfg(any(feature = "external_integration", feature = "external_performance"))]
#[path = "common/mod.rs"]
mod common;

#[cfg(any(feature = "external_integration", feature = "external_performance"))]
#[path = "kafka/common.rs"]
mod kafka_common;

#[cfg(feature = "external_integration")]
#[path = "kafka/integration_tests.rs"]
mod integration_tests;

#[cfg(feature = "external_performance")]
#[path = "kafka/performance_tests.rs"]
mod performance_tests;
