//! wp-connectors: Unified Kafka Source/Sink + Factories
//!
//! 模块划分：
//! - source：KafkaSource & 错误映射/建 Topic
//! - sink：KafkaSink（AsyncRawDataSink/AsyncRecordSink）
//! - factory：Source/Sink 工厂与注册函数

//mod adapter;
mod config;
mod factory;
mod sink;
mod source;

// 统一导出：便于上游 `wp_connectors::Source/Sink/Factory` 使用
pub use factory::{KafkaSinkFactory, KafkaSourceFactory};
pub use sink::KafkaSink;
pub use source::KafkaSource;
