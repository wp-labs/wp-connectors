#![cfg(all(
    any(feature = "external_integration", feature = "external_performance"),
    any(
        feature = "full",
        feature = "kafka",
        feature = "doris",
        feature = "http",
        feature = "clickhouse",
        feature = "elasticsearch",
        feature = "mysql",
        feature = "postgres",
        feature = "victorialogs"
    )
))]

pub mod component_tools;
pub mod sink;
