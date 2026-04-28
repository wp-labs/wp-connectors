//! wp-connector-postgres: Unified Postgres Source/Sink + Factories
//!
//! 模块划分：
//! - source：PostgresSource & 错误映射/建 Topic
//! - sink：PostgresSink（AsyncRawDataSink/AsyncRecordSink）
//! - factory：Source/Sink 工厂与注册函数

mod adapter;
mod config;
mod factory;
mod sink;
mod source;

// 统一导出：便于上游 `wp_connector_postgres::Sink/Factory` 使用
pub use factory::{PostgresSinkFactory, PostgresSourceFactory};
pub use sink::PostgresSink;
pub use source::PostgresSource;
