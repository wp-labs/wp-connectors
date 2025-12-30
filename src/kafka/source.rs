use async_trait::async_trait;
use orion_error::ErrorOwe;
use rdkafka_wrap::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka_wrap::client::DefaultClientContext;
use rdkafka_wrap::config::RDKafkaLogLevel;
use rdkafka_wrap::error::KafkaError;
use rdkafka_wrap::types::RDKafkaErrorCode;
use rdkafka_wrap::{ClientConfig, KWConsumer, KWConsumerConf, Message};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use wp_parse_api::RawData;

use wp_connector_api::{
    DataSource, SourceBatch, SourceError, SourceEvent, SourceReason, SourceResult, Tags,
};

type AnyResult<T> = anyhow::Result<T>;

pub struct KafkaSource {
    key: String,
    tags: Tags,
    consumer: KWConsumer,
    event_seq: u64,
}

impl KafkaSource {
    pub fn identifier(&self) -> &str {
        &self.key
    }

    pub async fn new(
        key: String,
        tags: Tags,
        group_id: &str,
        config: &KafkaSourceConf,
    ) -> AnyResult<Self> {
        // Create topics if not exists (best-effort)
        create_topics(config).await?;

        wp_log::info_data!("[kafka] topics: {:?}, group_id: {}", config.topic, group_id);
        let mut conf = KWConsumerConf::new(&config.brokers, group_id)
            .set_log_level(RDKafkaLogLevel::Info)
            .set_topics(config.topic.clone());
        if let Some(config) = &config.config {
            let mut map = HashMap::new();
            for c in config {
                let v: Vec<&str> = c.split('=').collect();
                if v.len() >= 2 {
                    map.insert(v[0].trim(), v[1].trim());
                }
            }
            conf = conf.set_config(map);
        }
        let consumer = KWConsumer::new_subscribe(conf)?;
        Ok(Self {
            key,
            consumer,
            tags,
            event_seq: 0,
        })
    }

    pub async fn recv_impl(&mut self) -> SourceResult<SourceBatch> {
        self.consumer
            .recv()
            .await
            .map(|msg| {
                let payload = Bytes::copy_from_slice(msg.payload().unwrap_or(&[]));
                // 转换标签为 Tags
                let mut stags = self.tags.clone();
                stags.set("access_source", msg.topic().to_string());
                self.event_seq = self.event_seq.wrapping_add(1);
                let event_id = self.event_seq;
                vec![SourceEvent::new(
                    event_id,
                    self.key.clone().into(),
                    RawData::Bytes(payload),
                    stags.into(),
                )]
            })
            .map_err(KafkaErrorWrapper)
            .owe(SourceReason::SupplierError("kafka".to_string()))
    }
}

async fn create_topics(config: &KafkaSourceConf) -> AnyResult<()> {
    let admin_client: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", &config.brokers)
        .set_log_level(RDKafkaLogLevel::Info)
        .create()?;
    for topic in &config.topic {
        let new_topic = NewTopic::new(topic, 1, TopicReplication::Fixed(1));
        let results = admin_client
            .create_topics::<Vec<&NewTopic>>(vec![&new_topic], &AdminOptions::new())
            .await?;
        for r in results {
            match r {
                Ok(success) => {
                    wp_log::info_data!("[kafka] topic '{}' creation successful: {}", topic, success)
                }
                Err((name, code)) => {
                    if let RDKafkaErrorCode::TopicAlreadyExists = code {
                        wp_log::warn_data!("[kafka] topic {} already exists, continuing", name);
                        continue;
                    }
                    return Err(SourceError::from(SourceReason::SupplierError(format!(
                        "Failed to create Kafka topic {} with error: {}",
                        name, code
                    )))
                    .into());
                }
            }
        }
    }
    Ok(())
}

#[derive(Clone)]
pub struct KafkaErrorWrapper(pub KafkaError);

impl Display for KafkaErrorWrapper {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl From<KafkaErrorWrapper> for SourceReason {
    fn from(value: KafkaErrorWrapper) -> Self {
        if value.0 == KafkaError::NoMessageReceived {
            return SourceReason::NotData;
        }
        SourceReason::SupplierError(value.0.to_string())
    }
}

#[async_trait]
impl DataSource for KafkaSource {
    async fn receive(&mut self) -> SourceResult<SourceBatch> {
        self.recv_impl().await
    }
    fn try_receive(&mut self) -> Option<SourceBatch> {
        None
    }
    fn identifier(&self) -> String {
        self.identifier().to_string()
    }
}
use bytes::Bytes;

use crate::kafka::config::KafkaSourceConf;
