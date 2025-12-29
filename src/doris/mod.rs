//! Doris sink implementation for wp-connectors
//!
//! 当前仅提供 Sink 实现，负责连接 Doris（MySQL 协议）、建表与批量写入。

mod config;
mod factory;
mod sink;

pub use config::DorisSinkConfig;
pub use factory::DorisSinkFactory;
pub use sink::DorisSink;
