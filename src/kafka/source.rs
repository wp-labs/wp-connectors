use async_trait::async_trait;
use orion_error::conversion::SourceRawErr;
use orion_error::conversion::ToStructError;
use rdkafka_wrap::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka_wrap::client::{ClientContext, DefaultClientContext};
use rdkafka_wrap::config::RDKafkaLogLevel;
use rdkafka_wrap::consumer::{Consumer, ConsumerContext, Rebalance, StreamConsumer};
use rdkafka_wrap::error::KafkaError;
use rdkafka_wrap::statistics::Statistics;
use rdkafka_wrap::types::RDKafkaErrorCode;
use rdkafka_wrap::{ClientConfig, Message};
use std::fmt::{Display, Formatter};
use std::time::Duration;
use tokio::time::timeout;
use wp_model_core::event_id::next_wp_event_id;
use wp_model_core::raw::RawData;

use crate::WP_SRC_VAL;
use wp_connector_api::{DataSource, SourceBatch, SourceEvent, SourceReason, SourceResult, Tags};

#[cfg(feature = "victoriametrics")]
use prometheus::{GaugeVec, register_gauge_vec};

#[cfg(feature = "victoriametrics")]
lazy_static::lazy_static! {
    static ref KAFKA_CONSUMER_LAG: GaugeVec = register_gauge_vec!(
        "kafka_consumer_lag",
        "Kafka consumer lag per topic-partition",
        &["source_name","topic", "partition"]
    ).expect("register kafka_consumer_lag metric failed");
}

struct KafkaContext {
    source_name: String,
}

impl ClientContext for KafkaContext {
    #[cfg(feature = "victoriametrics")]
    fn stats(&self, stats: Statistics) {
        for (topic_name, topic) in &stats.topics {
            for (part_id, partition) in &topic.partitions {
                if *part_id < 0 {
                    continue; // skip unassigned partitions (RD_KAFKA_PARTITION_UA = -1)
                }
                KAFKA_CONSUMER_LAG
                    .with_label_values(&[&self.source_name, topic_name, &part_id.to_string()])
                    .set(partition.consumer_lag as f64);
            }
        }
    }
}

impl ConsumerContext for KafkaContext {
    fn pre_rebalance(&self, _rebalance: &Rebalance) {}
    fn post_rebalance(&self, _rebalance: &Rebalance) {}
}

pub struct KafkaSource {
    key: String,
    tags: Tags,
    consumer: StreamConsumer<KafkaContext>,
    /// 单个 `SourceBatch` 最大事件数（`0` = 不限条数，默认按字节凑批）。
    batch_size: usize,
    /// 凑批时单次读取的最长等待毫秒数（`0` = 不等待，逐条返回）。
    batch_timeout_ms: u64,
    /// 单个 `SourceBatch` 最大字节数（`0` = 不限字节，只按条数凑批）。
    batch_max_bytes: usize,
}

/// 把一条 Kafka 消息转成 `SourceEvent`，并把 topic 记录到 `WP_SRC_VAL` 标签。
fn event_from_message(key: &str, tags: &Tags, topic: &str, payload: Option<&[u8]>) -> SourceEvent {
    let mut stags = tags.clone();
    stags.set(WP_SRC_VAL, topic);
    SourceEvent::new(
        next_wp_event_id(),
        key.to_string(),
        RawData::Bytes(Bytes::copy_from_slice(payload.unwrap_or(&[]))),
        stags.into(),
    )
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
        if let Err(err) = create_topics(config).await {
            wp_log::warn_data!(
                "[kafka] create topics failed, continuing: {}",
                err.to_string()
            );
        }

        wp_log::info_data!("[kafka] topics: {:?}, group_id: {}", config.topic, group_id);

        let mut client_config = ClientConfig::new();
        client_config
            .set("group.id", group_id)
            .set("bootstrap.servers", &config.brokers)
            .set("enable.partition.eof", "false")
            .set("enable.auto.commit", "true")
            .set("enable.auto.offset.store", "true")
            .set("receive.message.max.bytes", "100001000")
            .set("auto.offset.reset", "earliest")
            .set("statistics.interval.ms", "5000")
            .set_log_level(RDKafkaLogLevel::Info);

        if let Some(custom_configs) = &config.config {
            for c in custom_configs {
                let v: Vec<&str> = c.split('=').collect();
                if v.len() >= 2 {
                    client_config.set(v[0].trim(), v[1].trim());
                }
            }
        }

        let consumer: StreamConsumer<KafkaContext> = client_config
            .create_with_context(KafkaContext {
                source_name: key.clone(),
            })
            .source_raw_err(SourceReason::SupplierError, "create kafka consumer failed")?;

        let topics: Vec<&str> = config.topic.iter().map(|s| s.as_str()).collect();
        consumer
            .subscribe(&topics)
            .source_raw_err(SourceReason::SupplierError, "subscribe kafka topics failed")?;

        Ok(Self {
            key,
            consumer,
            tags,
            batch_size: config.batch_size, // 0 = 不限条数
            batch_timeout_ms: config.batch_timeout_ms,
            batch_max_bytes: config.batch_max_bytes,
        })
    }

    /// 批量接收：
    /// 1. 为空就等待 —— 阻塞等待第一条消息（空闲时不空转、不加额外延迟）；
    /// 2. 读到就继续 —— 达到任一上限（条数/字节/时间）前持续读取；
    /// 3. 读不到就结束 —— 单次读取等待超过 `batch_timeout_ms`
    ///    （或队列暂无数据 / EOF / 其它消费错误）即返回已收消息。
    ///
    /// 上限语义：`batch_size`（`0` = 不限条数）与 `batch_max_bytes`
    /// （`0` = 不限字节）谁先到达谁触发返回；默认 `batch_size = 0`，即按字节
    /// 凑批（小事件自动多装、大事件天然少装），显式设置条数可限制派发量子。
    ///
    /// 只有一条消息都没收到时才会返回错误（保留原有 `NotData` 等语义），
    /// 凑批过程中的错误会顺延到下一次调用再暴露，避免丢弃已 poll 的消息。
    pub async fn recv_impl(&mut self) -> SourceResult<SourceBatch> {
        let mut events: Vec<SourceEvent> =
            Vec::with_capacity(batch_capacity_hint(self.batch_size, self.batch_max_bytes));
        let mut batch_bytes = 0usize;

        let msg = self
            .consumer
            .recv()
            .await
            .map_err(KafkaErrorWrapper)
            .source_raw_err(SourceReason::SupplierError, "kafka".to_string())?;
        batch_bytes += msg.payload().map_or(0, |p| p.len());
        events.push(event_from_message(
            &self.key,
            &self.tags,
            msg.topic(),
            msg.payload(),
        ));

        // batch_timeout_ms == 0：不额外等待，退化为逐条返回（旧行为）。
        if self.batch_timeout_ms > 0 {
            let wait = Duration::from_millis(self.batch_timeout_ms);
            while batch_caps_open(
                self.batch_size,
                self.batch_max_bytes,
                events.len(),
                batch_bytes,
            ) {
                match timeout(wait, self.consumer.recv()).await {
                    // 窗口内读到下一条 → 继续凑批。
                    Ok(Ok(msg)) => {
                        batch_bytes += msg.payload().map_or(0, |p| p.len());
                        events.push(event_from_message(
                            &self.key,
                            &self.tags,
                            msg.topic(),
                            msg.payload(),
                        ));
                    }
                    // 读不到（超时 / 无数据 / EOF / 消费错误）→ 结束本批。
                    Ok(Err(_)) | Err(_) => break,
                }
            }
        }

        Ok(events)
    }
}

/// 是否还能继续凑批：条数上限（`batch_size`=`0` 表示不限）与字节上限
/// （`batch_max_bytes`=`0` 表示不限）都未触发。
#[inline]
fn batch_caps_open(
    batch_size: usize,
    batch_max_bytes: usize,
    event_cnt: usize,
    batch_bytes: usize,
) -> bool {
    (batch_size == 0 || event_cnt < batch_size)
        && (batch_max_bytes == 0 || batch_bytes < batch_max_bytes)
}

/// `Vec` 预分配容量估算（仅影响分配，不影响语义）：取两个上限的保守最小值，
/// 避免 `batch_size` 设得很大且按字节先触发时过度预分配。
#[inline]
fn batch_capacity_hint(batch_size: usize, batch_max_bytes: usize) -> usize {
    let by_bytes = if batch_max_bytes > 0 {
        (batch_max_bytes / 512).clamp(64, 8192)
    } else {
        8192
    };
    let by_count = if batch_size > 0 {
        batch_size
    } else {
        usize::MAX
    };
    by_bytes.min(by_count)
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
    use rdkafka_wrap::Message;
    use rdkafka_wrap::error::KafkaError;
    use rdkafka_wrap::types::RDKafkaErrorCode;

    use arrow::array::ArrayBuilder;
    use arrow::array::{BinaryBuilder, Int32Builder, Int64Builder, StringBuilder};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use wf_connector_api::{BatchSource, SourceReason as WfReason, SourceResult as WfResult};

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
                        return Err(WfReason::EOF.err_detail(format!("partition {partition} EOF")));
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
                        return Err(WfReason::Io.err_detail(format!("kafka recv error: {code}")));
                    }
                    // All other Kafka errors → Io
                    Err(e) => {
                        if topic_builder.len() > 0 {
                            break; // return partial batch; error will surface next call
                        }
                        return Err(WfReason::Io.err_detail(format!("kafka recv error: {e}")));
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
            .map_err(|e| WfReason::Decode.err_detail(format!("RecordBatch build failed: {e}")))?;

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
            let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
            assert_eq!(
                names,
                vec![
                    "topic",
                    "partition",
                    "offset",
                    "timestamp",
                    "key",
                    "payload"
                ]
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_from_message_uses_topic_tag_and_copies_payload() {
        let mut tags = Tags::new();
        tags.set("custom", "v");
        let ev = event_from_message("k1", &tags, "orders", Some(b"hello"));
        assert_eq!(ev.tags.get(WP_SRC_VAL), Some("orders"));
        assert_eq!(ev.tags.get("custom"), Some("v"));
        match &ev.payload {
            RawData::Bytes(bytes) => assert_eq!(bytes.as_ref(), b"hello"),
            _ => panic!("expected RawData::Bytes payload"),
        }
    }

    #[test]
    fn event_from_message_none_payload_produces_empty_bytes() {
        let ev = event_from_message("k1", &Tags::new(), "t", None);
        match &ev.payload {
            RawData::Bytes(bytes) => assert!(bytes.is_empty()),
            _ => panic!("expected RawData::Bytes payload"),
        }
    }

    #[test]
    fn event_from_message_event_ids_are_unique() {
        let tags = Tags::new();
        let a = event_from_message("k1", &tags, "t", Some(b"1"));
        let b = event_from_message("k1", &tags, "t", Some(b"2"));
        assert_ne!(a.event_id, b.event_id);
    }

    // -- batch caps semantics (pure) --------------------------------------

    #[test]
    fn batch_caps_open_default_limits_by_bytes_only() {
        // 默认 batch_size=0（不限条数），batch_max_bytes 决定何时关闭。
        assert!(batch_caps_open(0, 256 * 1024, 0, 0));
        assert!(batch_caps_open(0, 256 * 1024, 1700, 200 * 1024));
        assert!(!batch_caps_open(0, 256 * 1024, 1700, 256 * 1024));
        assert!(!batch_caps_open(0, 256 * 1024, 1, 256 * 1024));
    }

    #[test]
    fn batch_caps_open_count_limit_closes_at_size() {
        assert!(batch_caps_open(512, 0, 0, 0));
        assert!(batch_caps_open(512, 0, 511, 0));
        assert!(!batch_caps_open(512, 0, 512, 0));
    }

    #[test]
    fn batch_caps_open_whichever_limit_first() {
        // 小消息：条数先到（150B × 512 < 256KiB）
        assert!(!batch_caps_open(512, 256 * 1024, 512, 512 * 150));
        assert!(batch_caps_open(512, 256 * 1024, 128, 128 * 150));
        // 大消息：字节先到（128 × 2KiB > 256KiB）
        assert!(!batch_caps_open(512, 256 * 1024, 128, 128 * 2048));
    }

    #[test]
    fn batch_caps_open_no_limits_never_closes() {
        // 全 0 组合由 conf 校验拒绝（timeout>0 时）；纯函数层面保持开放。
        assert!(batch_caps_open(0, 0, 100_000, usize::MAX / 2));
    }

    #[test]
    fn batch_capacity_hint_respects_both_limits() {
        // 默认（0 条 / 256KiB）：按字节估算 ≈512。
        assert_eq!(batch_capacity_hint(0, 256 * 1024), 512);
        // 显式条数更小 → 取条数。
        assert_eq!(batch_capacity_hint(128, 0), 128);
        assert_eq!(batch_capacity_hint(128, 256 * 1024), 128);
        // 只按字节 → 估算上限 8192。
        assert_eq!(batch_capacity_hint(0, 0), 8192);
        assert_eq!(batch_capacity_hint(0, 16 * 1024 * 1024), 8192);
        // 大条数 + 默认字节 → 不按条数整体预分配。
        assert_eq!(batch_capacity_hint(100_000, 256 * 1024), 512);
    }
}
