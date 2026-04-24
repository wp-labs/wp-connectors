#![cfg(all(
    feature = "http",
    any(feature = "external_integration", feature = "external_performance")
))]

#[cfg(any(feature = "external_integration", feature = "external_performance"))]
#[path = "common/mod.rs"]
mod common;

#[cfg(any(feature = "external_integration", feature = "external_performance"))]
#[path = "http/common.rs"]
mod http_common;

#[cfg(feature = "external_integration")]
#[path = "http/sinks/integration_tests.rs"]
mod integration_tests;

#[cfg(feature = "external_integration")]
#[path = "http/sources/source_integration_tests.rs"]
mod source_integration_tests;

#[cfg(feature = "external_performance")]
#[path = "http/sinks/performance_tests.rs"]
mod performance_tests;
