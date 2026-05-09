/// Tag key for access source identifier
pub const WP_SRC_VAL: &str = "wp_src_val";

// 通用工具模块
mod http_utils;
pub mod utils;

// Kafka
#[cfg(feature = "kafka")]
pub mod kafka;

// MySQL
#[cfg(feature = "mysql")]
pub mod mysql;

// Postgres
#[cfg(feature = "postgres")]
pub mod postgres;

#[cfg(feature = "dmdb")]
pub mod dmdb;
// ClickHouse
#[cfg(feature = "clickhouse")]
pub mod clickhouse;

// Prometheus：可选功能，启用方式 `--features prometheus`
#[cfg(feature = "prometheus")]
pub mod prometheus;

// Doris：可选功能，启用方式 `--features doris`
#[cfg(feature = "doris")]
pub mod doris;

// count：可选功能，启用方式 `--features count`
#[cfg(feature = "count")]
pub mod count;

// VictoriaLog：可选功能，启用方式 `--features victorialog`
#[cfg(feature = "victorialogs")]
pub mod victorialogs;

// Elasticsearch：可选功能，启用方式 `--features elasticsearch`
#[cfg(feature = "elasticsearch")]
pub mod elasticsearch;

// VictoriaMetrics：可选功能，启用方式 `--features victoriametric`
#[cfg(feature = "victoriametrics")]
pub mod victoriametrics;

// HTTP：可选功能，启用方式 `--features http`
#[cfg(feature = "http")]
pub mod http;
