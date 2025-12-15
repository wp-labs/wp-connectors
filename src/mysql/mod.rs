//! wp-connector-mysql: Unified MySql Source/Sink + Factories
//!
//! 模块划分：
//! - source：MySqlSource & 错误映射/建 Topic
//! - sink：MySqlSink（AsyncRawDataSink/AsyncRecordSink）
//! - factory：Source/Sink 工厂与注册函数

mod adapter;
mod config;
mod factory;
mod sink;
mod source;

// 统一导出：便于上游 `wp_connector_mysql::Source/Sink/Factory` 使用
pub use factory::{MySQLSinkFactory, MySQLSourceFactory};
pub use sink::MysqlSink;
pub use source::MysqlSource;
