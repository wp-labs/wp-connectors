//! wp-connector-dmdb: 达梦数据库 Sink
//!
//! 模块划分：
//! - config：达梦连接与批量写入配置
//! - sink：严格失败语义的达梦 Sink 实现
//! - factory：Sink 工厂与默认参数声明

mod config;
mod factory;
mod sink;

pub use config::DmdbConf;
pub use factory::DmdbSinkFactory;
pub use sink::DmdbSink;
