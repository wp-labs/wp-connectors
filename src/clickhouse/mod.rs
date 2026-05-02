//! ClickHouse sink implementation for wp-connectors
//!
//! 提供 Sink 实现，负责连接 ClickHouse 并使用 clickhouse 库批量写入数据。

mod config;
mod factory;
mod sink;

pub use config::ClickHouseSinkConfig;
pub use factory::ClickHouseSinkFactory;
pub use sink::ClickHouseSink;
