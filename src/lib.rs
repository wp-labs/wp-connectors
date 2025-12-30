// Kafka：默认启用（feature = "kafka" 是默认特性）
#[cfg(feature = "kafka")]
pub mod kafka;

// MySQL：默认启用（feature = "mysql" 是默认特性）
#[cfg(feature = "mysql")]
pub mod mysql;

// Prometheus：可选功能，启用方式 `--features prometheus`
#[cfg(feature = "prometheus")]
pub mod prometheus;

// VictoriaLog：可选功能，启用方式 `--features victorialog`
#[cfg(feature = "victorialogs")]
pub mod victorialogs;

// Elasticsearch：预留可选特性，后续接入独立子 crate 时再暴露实现
#[cfg(feature = "elasticsearch")]
pub mod elasticsearch {
    //! 占位模块：后续将通过 `wp_connectors::elasticsearch::*` 暴露 sink 实现
}

// VictoriaMetrics：可选功能，启用方式 `--features victoriametric`
#[cfg(feature = "victoriametric")]
pub mod victoriametric;
