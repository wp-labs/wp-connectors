//! Elasticsearch sink implementation for wp-connectors
//!
//! 提供 Sink 实现，负责连接 Elasticsearch 并使用 Bulk API 批量写入数据。

mod config;
mod factory;
mod sink;

pub use config::ElasticsearchSinkConfig;
pub use factory::ElasticsearchSinkFactory;
pub use sink::ElasticsearchSink;
