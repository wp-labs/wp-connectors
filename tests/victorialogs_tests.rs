#![cfg(all(
    feature = "victorialogs",
    any(feature = "external_integration", feature = "external_performance")
))]

#[cfg(any(feature = "external_integration", feature = "external_performance"))]
#[path = "common/mod.rs"]
mod common;

#[cfg(any(feature = "external_integration", feature = "external_performance"))]
#[path = "victorialogs/common.rs"]
mod victorialogs_common;

#[cfg(feature = "external_integration")]
#[path = "victorialogs/integration_tests.rs"]
mod integration_tests;

#[cfg(feature = "external_performance")]
#[path = "victorialogs/performance_tests.rs"]
mod performance_tests;
