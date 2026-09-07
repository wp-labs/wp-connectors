use orion_conf::ToStructError;

use orion_conf::error::{ConfIOReason, OrionConfResult};
use serde::{Deserialize, Serialize};
use wp_conf_base::structure::Validate;

/// 默认值：单个 `SourceBatch` 的最大事件数。
///
/// 默认 `0` = **不限条数**，单批大小由 `batch_max_bytes` 按字节控制；
/// 显式设为 ≥1 时按条数限制（与字节上限谁先到谁触发，防止极小消息把单批
/// 事件数/派发量子撑得过大）。
pub const KAFKA_SOURCE_BATCH_SIZE: usize = 0;
/// 默认值：单个 `SourceBatch` 的最大字节数（`0` = 不限字节）。
///
/// 主参：控制单批物理大小/内存。小事件按字节凑批（默认 256KiB），大事件
/// 天然少装；与 `batch_size` 谁先到谁触发。
pub const KAFKA_SOURCE_BATCH_MAX_BYTES: usize = 256 * 1024;
/// 默认值：凑批时单次读取的最长等待毫秒数。
///
/// 收满上限前每次尝试读取都会等待至多该时长，超时即认为 "暂时读不到"并
/// 返回已收消息；设为 0 表示不额外等待（逐条返回）。
pub const KAFKA_SOURCE_BATCH_TIMEOUT_MS: u64 = 1;

fn default_kafka_source_batch_size() -> usize {
    KAFKA_SOURCE_BATCH_SIZE
}

fn default_kafka_source_batch_timeout_ms() -> u64 {
    KAFKA_SOURCE_BATCH_TIMEOUT_MS
}

fn default_kafka_source_batch_max_bytes() -> usize {
    KAFKA_SOURCE_BATCH_MAX_BYTES
}

/// 批量限制组合是否非法；返回 `Some(原因)` 表示非法。
///
/// `batch_size` 与 `batch_max_bytes` 同时为 0（都不限大小）而
/// `batch_timeout_ms > 0` 时，单批可无限增长（每轮读取窗口滑动不设内存边界），
/// 直接拒绝以避免内存失控。
pub fn batch_limits_error(
    batch_size: usize,
    batch_timeout_ms: u64,
    batch_max_bytes: usize,
) -> Option<&'static str> {
    if batch_size == 0 && batch_max_bytes == 0 && batch_timeout_ms > 0 {
        Some(
            "kafka.batch_size and batch_max_bytes must not both be 0 when batch_timeout_ms > 0 (batch would be unbounded)",
        )
    } else {
        None
    }
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Clone)]
pub struct KafkaSourceConf {
    pub key: String,
    pub brokers: String,
    pub topic: Vec<String>,
    pub config: Option<Vec<String>>,
    pub enable: bool,
    /// 单个 `SourceBatch` 最大事件数（`0` = 不限条数，默认按字节凑批）。
    #[serde(default = "default_kafka_source_batch_size")]
    pub batch_size: usize,
    /// 凑批时单次读取的最长等待毫秒数（`0` = 不等待，逐条返回）。
    #[serde(default = "default_kafka_source_batch_timeout_ms")]
    pub batch_timeout_ms: u64,
    /// 单个 `SourceBatch` 最大字节数（`0` = 不限字节，只按条数凑批）。
    #[serde(default = "default_kafka_source_batch_max_bytes")]
    pub batch_max_bytes: usize,
    //#[serde(default)]
    //pub tags: Vec<String>,
}

impl Validate for KafkaSourceConf {
    fn validate(&self) -> OrionConfResult<()> {
        if self.brokers.trim().is_empty() {
            return Err(ConfIOReason::validation_error()
                .to_err()
                .with_detail("kafka.brokers must not be empty"));
        }
        if self.topic.is_empty() {
            return Err(ConfIOReason::validation_error()
                .to_err()
                .with_detail("kafka.topic must not be empty"));
        }
        if let Some(detail) =
            batch_limits_error(self.batch_size, self.batch_timeout_ms, self.batch_max_bytes)
        {
            return Err(ConfIOReason::validation_error()
                .to_err()
                .with_detail(detail));
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Clone)]
pub struct KafkaSinkConf {
    pub brokers: String,
    pub topic: String,
    pub num_partitions: i32,
    pub replication: i32,
    pub config: Option<Vec<String>>,
}

impl KafkaSinkConf {
    pub fn new(topic: &str) -> Self {
        Self {
            topic: topic.to_string(),
            ..Self::default()
        }
    }
}

impl Default for KafkaSourceConf {
    fn default() -> Self {
        Self {
            key: "kafka_1".to_string(),
            brokers: "localhost:9092".to_string(),
            topic: vec!["test".to_string()],
            config: Some(vec![
                "enable.partition.eof = false".to_string(),
                "enable.auto.commit = true".to_string(),
                "enable.auto.offset.store = true".to_string(),
                "receive.message.max.bytes = 100001000".to_string(),
                "auto.offset.reset = earliest".to_string(),
            ]),
            enable: false,
            batch_size: KAFKA_SOURCE_BATCH_SIZE,
            batch_timeout_ms: KAFKA_SOURCE_BATCH_TIMEOUT_MS,
            batch_max_bytes: KAFKA_SOURCE_BATCH_MAX_BYTES,
        }
    }
}

impl Default for KafkaSinkConf {
    fn default() -> Self {
        Self {
            brokers: "localhost:9092".to_string(),
            topic: "test".to_string(),
            num_partitions: 3,
            replication: 1,
            config: Some(vec![
                "queue.buffering.max.messages = 50000".to_string(),
                "queue.buffering.max.kbytes = 2147483647".to_string(),
                "message.max.bytes = 10485760".to_string(),
            ]),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_conf_default_batch_values() {
        let conf = KafkaSourceConf::default();
        // 默认 batch_size = 0（不限条数），单批大小由字节上限控制。
        assert_eq!(conf.batch_size, 0);
        assert_eq!(conf.batch_max_bytes, KAFKA_SOURCE_BATCH_MAX_BYTES);
        assert_eq!(conf.batch_timeout_ms, KAFKA_SOURCE_BATCH_TIMEOUT_MS);
    }

    #[test]
    fn source_conf_deserializes_batch_defaults_when_missing() {
        let toml = r#"
key = "k1"
brokers = "localhost:9092"
topic = ["topic_a"]
enable = true
"#;
        let conf: KafkaSourceConf = toml::from_str(toml).expect("deserialize source conf");
        assert_eq!(conf.batch_size, KAFKA_SOURCE_BATCH_SIZE);
        assert_eq!(conf.batch_max_bytes, KAFKA_SOURCE_BATCH_MAX_BYTES);
        assert_eq!(conf.batch_timeout_ms, KAFKA_SOURCE_BATCH_TIMEOUT_MS);
    }

    #[test]
    fn source_conf_deserializes_custom_batch_values() {
        let toml = r#"
key = "k1"
brokers = "localhost:9092"
topic = ["topic_a"]
enable = true
batch_size = 512
batch_timeout_ms = 10
batch_max_bytes = 1048576
"#;
        let conf: KafkaSourceConf = toml::from_str(toml).expect("deserialize source conf");
        assert_eq!(conf.batch_size, 512);
        assert_eq!(conf.batch_timeout_ms, 10);
        assert_eq!(conf.batch_max_bytes, 1024 * 1024);
    }

    #[test]
    fn source_conf_accepts_zero_for_all_batch_limits() {
        let conf = KafkaSourceConf {
            batch_size: 0,
            batch_timeout_ms: 0,
            batch_max_bytes: 0,
            ..KafkaSourceConf::default()
        };
        assert!(conf.validate().is_ok());
    }

    #[test]
    fn source_conf_rejects_unbounded_waiting_batch() {
        // batch_size=0 且 batch_max_bytes=0 且 batch_timeout_ms>0：批无大小上限，拒绝。
        let conf = KafkaSourceConf {
            batch_size: 0,
            batch_timeout_ms: 1,
            batch_max_bytes: 0,
            ..KafkaSourceConf::default()
        };
        assert!(conf.validate().is_err());
    }

    #[test]
    fn batch_limits_error_reports_unbounded_combination() {
        assert!(batch_limits_error(0, 1, 0).is_some());
        assert!(batch_limits_error(0, 0, 0).is_none()); // timeout=0：逐条，安全
        assert!(batch_limits_error(512, 1, 256 * 1024).is_none());
        assert!(batch_limits_error(0, 1, 256 * 1024).is_none()); // 默认组合
        assert!(batch_limits_error(512, 1, 0).is_none()); // 仅条数限制
    }

    #[test]
    fn source_conf_validate_ok() {
        let conf = KafkaSourceConf::default();
        assert!(conf.validate().is_ok());
    }
}
