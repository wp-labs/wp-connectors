use orion_conf::error::{ConfIOReason, OrionConfResult};
use orion_error::{ToStructError, UvsValidationFrom};
use serde::{Deserialize, Serialize};
use wp_conf_base::structure::{GetTagStr, Validate};

#[derive(Debug, Deserialize, Serialize, PartialEq, Clone)]
pub struct KafkaSourceConf {
    pub key: String,
    pub brokers: String,
    pub topic: Vec<String>,
    pub config: Option<Vec<String>>,
    pub enable: bool,
    #[serde(default)]
    pub tags: Vec<String>,
}

impl GetTagStr for KafkaSourceConf {
    fn tag_vec_str(&self) -> &Vec<String> {
        &self.tags
    }
}

impl Validate for KafkaSourceConf {
    fn validate(&self) -> OrionConfResult<()> {
        if self.brokers.trim().is_empty() {
            return ConfIOReason::from_validation("kafka.brokers must not be empty").err_result();
        }
        if self.topic.is_empty() {
            return ConfIOReason::from_validation("kafka.topic must not be empty").err_result();
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
            tags: Vec::new(),
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
