use async_trait::async_trait;
use orion_error::conversion::SourceRawErr;
use orion_error::conversion::ToStructError;
use rdkafka_wrap::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka_wrap::client::DefaultClientContext;
use rdkafka_wrap::config::RDKafkaLogLevel;
use rdkafka_wrap::error::KafkaError;
use rdkafka_wrap::types::RDKafkaErrorCode;
use rdkafka_wrap::{ClientConfig, KWConsumer, KWConsumerConf, Message};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use wp_model_core::event_id::next_wp_event_id;
use wp_model_core::raw::RawData;

use crate::WP_SRC_VAL;
use wp_connector_api::{DataSource, SourceBatch, SourceEvent, SourceReason, SourceResult, Tags};

pub struct KafkaSource {
    key: String,
    tags: Tags,
    consumer: KWConsumer,
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
    ) -> SourceResult<Self> {
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
        let consumer = KWConsumer::new_subscribe(conf)
            .source_raw_err(SourceReason::SupplierError, "subscribe kafka source failed")?;
        Ok(Self {
            key,
            consumer,
            tags,
        })
    }

    pub async fn recv_impl(&mut self) -> SourceResult<SourceBatch> {
        self.consumer
            .recv()
            .await
            .map(|msg| {
                let payload = Bytes::copy_from_slice(msg.payload().unwrap_or(&[]));
                let mut stags = self.tags.clone();
                stags.set(WP_SRC_VAL, msg.topic().to_string());
                vec![SourceEvent::new(
                    next_wp_event_id(),
                    self.key.clone(),
                    RawData::Bytes(payload),
                    stags.into(),
                )]
            })
            .map_err(KafkaErrorWrapper)
            .source_raw_err(SourceReason::SupplierError, "kafka".to_string())
    }
}

async fn create_topics(config: &KafkaSourceConf) -> SourceResult<()> {
    let admin_client: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", &config.brokers)
        .set_log_level(RDKafkaLogLevel::Info)
        .create()
        .source_raw_err(
            SourceReason::SupplierError,
            "create kafka admin client failed",
        )?;
    for topic in &config.topic {
        let new_topic = NewTopic::new(topic, 1, TopicReplication::Fixed(1));
        let results = admin_client
            .create_topics::<Vec<&NewTopic>>(vec![&new_topic], &AdminOptions::new())
            .await
            .source_raw_err(SourceReason::SupplierError, "create kafka topic failed")?;
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
                    return Err(SourceReason::SupplierError.to_err().with_detail(format!(
                        "failed to create kafka topic {name} with error: {code}"
                    )));
                }
            }
        }
    }
    Ok(())
}

#[derive(Clone, Debug)]
pub struct KafkaErrorWrapper(pub KafkaError);

impl Display for KafkaErrorWrapper {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for KafkaErrorWrapper {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.source()
    }
}

impl From<KafkaErrorWrapper> for SourceReason {
    fn from(value: KafkaErrorWrapper) -> Self {
        if value.0 == KafkaError::NoMessageReceived {
            return SourceReason::NotData;
        }
        SourceReason::SupplierError
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

// ---------------------------------------------------------------------------
// wf-connector-api (warp-fusion) BatchSource impl — gated behind "wf" + "kafka"
// ---------------------------------------------------------------------------

#[cfg(feature = "kafka")]
mod wf_impl {
    use std::sync::{Arc, OnceLock};

    use async_trait::async_trait;
    use rdkafka_wrap::error::KafkaError;
    use rdkafka_wrap::types::RDKafkaErrorCode;
    use rdkafka_wrap::Message;

    use arrow::array::{BinaryBuilder, Int32Builder, Int64Builder, StringBuilder};
    use arrow::array::ArrayBuilder;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use wf_connector_api::{
        BatchSource, SourceReason as WfReason, SourceResult as WfResult,
    };

    use super::KafkaSource;

    /// Max messages to batch into one [`RecordBatch`].
    const BATCH_SIZE: usize = 1024;

    /// Cached Arrow schema for Kafka message batches — initialized once.
    static KAFKA_BATCH_SCHEMA: OnceLock<Arc<Schema>> = OnceLock::new();

    fn kafka_batch_schema() -> Arc<Schema> {
        Arc::clone(KAFKA_BATCH_SCHEMA.get_or_init(|| {
            Arc::new(Schema::new(vec![
                Field::new("topic", DataType::Utf8, false),
                Field::new("partition", DataType::Int32, false),
                Field::new("offset", DataType::Int64, false),
                Field::new("timestamp", DataType::Int64, true),
                Field::new("key", DataType::Binary, true),
                Field::new("payload", DataType::Binary, true),
            ]))
        }))
    }

    /// Returns `true` if the error code indicates a connection/auth failure
    /// where the broker is unreachable (auto-commit cannot fire).
    fn is_connection_loss(code: &RDKafkaErrorCode) -> bool {
        use RDKafkaErrorCode::*;
        matches!(
            code,
            BrokerTransportFailure | AllBrokersDown | Authentication | SaslAuthenticationFailed
        )
    }

    #[async_trait]
    impl BatchSource for KafkaSource {
        async fn start(&mut self) -> WfResult<()> {
            // Consumer is already subscribed in `new()`; start is idempotent.
            Ok(())
        }

        async fn receive_batch(&mut self) -> WfResult<Vec<RecordBatch>> {
            let schema = kafka_batch_schema();
            let mut topic_builder = StringBuilder::new();
            let mut partition_builder = Int32Builder::new();
            let mut offset_builder = Int64Builder::new();
            let mut ts_builder = Int64Builder::new();
            let mut key_builder = BinaryBuilder::new();
            let mut payload_builder = BinaryBuilder::new();

            loop {
                match self.consumer.recv().await {
                    Ok(msg) => {
                        topic_builder.append_value(msg.topic());
                        partition_builder.append_value(msg.partition());
                        offset_builder.append_value(msg.offset());
                        ts_builder.append_option(msg.timestamp().to_millis());
                        key_builder.append_option(msg.key());
                        payload_builder.append_option(msg.payload());

                        if topic_builder.len() >= BATCH_SIZE {
                            break;
                        }
                    }
                    // No data currently available → stop batching
                    Err(KafkaError::NoMessageReceived) => break,
                    // Partition EOF: return partial batch if any, otherwise signal EOF
                    Err(KafkaError::PartitionEOF(partition)) => {
                        if topic_builder.len() > 0 {
                            break; // return partial batch; EOF will surface next call
                        }
                        return Err(WfReason::EOF
                            .err_detail(format!("partition {partition} EOF")));
                    }
                    // Consumption error with specific code
                    Err(KafkaError::MessageConsumption(code)) => {
                        if is_connection_loss(&code) {
                            // Broker unreachable → auto-commit cannot fire,
                            // safe to discard accumulated batch on error.
                            return Err(WfReason::Connect
                                .err_detail(format!("kafka connection error: {code}")));
                        }
                        // Broker reachable → return partial batch to avoid data loss;
                        // the error will surface on the next receive_batch call.
                        if topic_builder.len() > 0 {
                            break;
                        }
                        return Err(WfReason::Io
                            .err_detail(format!("kafka recv error: {code}")));
                    }
                    // All other Kafka errors → Io
                    Err(e) => {
                        if topic_builder.len() > 0 {
                            break; // return partial batch; error will surface next call
                        }
                        return Err(WfReason::Io
                            .err_detail(format!("kafka recv error: {e}")));
                    }
                }
            }

            if topic_builder.len() == 0 {
                return Ok(vec![]);
            }

            let batch = RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(topic_builder.finish()),
                    Arc::new(partition_builder.finish()),
                    Arc::new(offset_builder.finish()),
                    Arc::new(ts_builder.finish()),
                    Arc::new(key_builder.finish()),
                    Arc::new(payload_builder.finish()),
                ],
            )
            .map_err(|e| {
                WfReason::Decode.err_detail(format!("RecordBatch build failed: {e}"))
            })?;

            Ok(vec![batch])
        }

        async fn close(&mut self) -> WfResult<()> {
            // Resource cleanup is handled by KWConsumer::Drop.
            Ok(())
        }

        fn identifier(&self) -> &str {
            &self.key
        }
    }

    // ------------------------------------------------------------------
    // Tests
    // ------------------------------------------------------------------

    #[cfg(test)]
    mod tests {
        use super::*;
        use std::sync::Arc;

        // -- Schema tests --------------------------------------------------

        #[test]
        fn schema_is_cached_same_arc() {
            let s1 = kafka_batch_schema();
            let s2 = kafka_batch_schema();
            // Same cached schema → Arc pointers are equal
            assert!(Arc::ptr_eq(&s1, &s2));
        }

        #[test]
        fn schema_has_six_fields() {
            let schema = kafka_batch_schema();
            assert_eq!(schema.fields().len(), 6);
        }

        #[test]
        fn schema_field_names() {
            let schema = kafka_batch_schema();
            let names: Vec<&str> = schema
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect();
            assert_eq!(
                names,
                vec!["topic", "partition", "offset", "timestamp", "key", "payload"]
            );
        }

        #[test]
        fn schema_non_nullable_fields() {
            let schema = kafka_batch_schema();
            for name in &["topic", "partition", "offset"] {
                let field = schema.field_with_name(name).unwrap();
                assert!(!field.is_nullable(), "{name} should be non-nullable");
            }
        }

        #[test]
        fn schema_nullable_fields() {
            let schema = kafka_batch_schema();
            for name in &["timestamp", "key", "payload"] {
                let field = schema.field_with_name(name).unwrap();
                assert!(field.is_nullable(), "{name} should be nullable");
            }
        }

        #[test]
        fn schema_field_data_types() {
            let schema = kafka_batch_schema();
            use arrow::datatypes::DataType;
            assert_eq!(
                schema.field_with_name("topic").unwrap().data_type(),
                &DataType::Utf8
            );
            assert_eq!(
                schema.field_with_name("partition").unwrap().data_type(),
                &DataType::Int32
            );
            assert_eq!(
                schema.field_with_name("offset").unwrap().data_type(),
                &DataType::Int64
            );
            assert_eq!(
                schema.field_with_name("timestamp").unwrap().data_type(),
                &DataType::Int64
            );
            assert_eq!(
                schema.field_with_name("key").unwrap().data_type(),
                &DataType::Binary
            );
            assert_eq!(
                schema.field_with_name("payload").unwrap().data_type(),
                &DataType::Binary
            );
        }

        // -- is_connection_loss tests --------------------------------------

        #[test]
        fn connection_loss_codes_return_true() {
            use RDKafkaErrorCode::*;
            for code in &[
                BrokerTransportFailure,
                AllBrokersDown,
                Authentication,
                SaslAuthenticationFailed,
            ] {
                assert!(
                    is_connection_loss(code),
                    "{code:?} should be a connection-loss code"
                );
            }
        }

        #[test]
        fn non_connection_codes_return_false() {
            use RDKafkaErrorCode::*;
            // A representative sample of codes that are NOT connection-loss
            for code in &[
                BadMessage,
                Fail,
                QueueFull,
                OperationTimedOut,
                MessageTimedOut,
                UnknownTopic,
                PartitionEOF,
                SSL,
                NoOffset,
            ] {
                assert!(
                    !is_connection_loss(code),
                    "{code:?} should NOT be a connection-loss code"
                );
            }
        }
    }
}
