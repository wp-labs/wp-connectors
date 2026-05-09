//! wp-connector-dmdb: 达梦数据库 Sink
//!
//! 模块划分：
//! - config：达梦连接、Source、Sink 配置
//! - common：Source/Sink 共享的建连与 SQL 辅助能力
//! - sink：严格失败语义的达梦 Sink 实现
//! - source：达梦 Source 与 checkpoint/游标能力
//! - factory：Source/Sink 工厂与默认参数声明

mod common;
mod config;
mod factory;
mod sink;
mod source;

pub use config::{DmdbConnConf, DmdbSinkConf, DmdbSourceConf};
pub use factory::{DmdbSinkFactory, DmdbSourceFactory};
pub use sink::DmdbSink;
pub use source::DmdbSource;
