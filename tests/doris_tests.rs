#![cfg(all(
    feature = "doris",
    any(feature = "external_integration", feature = "external_performance")
))]
#[cfg(any(feature = "external_integration", feature = "external_performance"))]
#[path = "doris/common.rs"]
mod doris_common;

#[cfg(feature = "external_integration")]
#[path = "doris/sinks/integration_tests.rs"]
mod integration_tests;

#[cfg(feature = "external_performance")]
#[path = "doris/sinks/performance_tests.rs"]
mod performance_tests;

// #[path = "doris/test_1.rs"]
// mod t1;
