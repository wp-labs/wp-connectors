/// Tag key for access source identifier
pub const WP_SRC_VAL: &str = "wp_src_val";

// Kafka：默认启用（feature = "kafka" 是默认特性）
#[cfg(feature = "kafka")]
pub mod kafka;

// MySQL：默认启用（feature = "mysql" 是默认特性）
#[cfg(feature = "mysql")]
pub mod mysql;

// Prometheus：可选功能，启用方式 `--features prometheus`
#[cfg(feature = "prometheus")]
pub mod prometheus;

// Doris：可选功能，启用方式 `--features doris`
#[cfg(feature = "doris")]
pub mod doris;

// VictoriaLog：可选功能，启用方式 `--features victorialog`
#[cfg(feature = "victorialog")]
pub mod victorialogs;

// Elasticsearch：预留可选特性，后续接入独立子 crate 时再暴露实现
#[cfg(feature = "elasticsearch")]
pub mod elasticsearch {
    //! 占位模块：后续将通过 `wp_connectors::elasticsearch::*` 暴露 sink 实现
}

// VictoriaMetrics：可选功能，启用方式 `--features victoriametric`
#[cfg(feature = "victoriametric")]
pub mod victoriametric;
