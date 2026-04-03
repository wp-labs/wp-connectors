#![cfg(all(
    feature = "elasticsearch",
    any(feature = "external_integration", feature = "external_performance")
))]

#[cfg(any(feature = "external_integration", feature = "external_performance"))]
#[path = "common/mod.rs"]
mod common;

#[cfg(any(feature = "external_integration", feature = "external_performance"))]
#[path = "elasticsearch/common.rs"]
mod elasticsearch_common;

#[cfg(feature = "external_integration")]
#[path = "elasticsearch/integration_tests.rs"]
mod integration_tests;

#[cfg(feature = "external_performance")]
#[path = "elasticsearch/performance_tests.rs"]
mod performance_tests;
