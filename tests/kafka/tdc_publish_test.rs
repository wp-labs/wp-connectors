//! Integration-style test to ensure AsyncRecordSink path publishes data.
//! Skips automatically when Kafka is not reachable on localhost:9092.

use std::time::Duration;

use wp_connectors::kafka::KafkaSinkFactory;
use wp_model_core::model::{DataField, DataRecord};
use wp_connector_api::{SinkSpec, SinkFactory};
use std::collections::BTreeMap;
use serde_json::Value;

mod common;

/// Minimal spec helper
fn make_spec(name: &str, topic: &str) -> SinkSpec {
    let mut params: BTreeMap<String, Value> = BTreeMap::new();
    params.insert(
        "brokers".to_string(),
        Value::String(common::TEST_KAFKA_BROKERS.to_string()),
    );
    params.insert("topic".to_string(), Value::String(topic.to_string()));
    params.insert("num_partitions".to_string(), Value::from(1));
    params.insert("replication".to_string(), Value::from(1));

    SinkSpec {
        name: name.to_string(),
        kind: "kafka".to_string(),
        connector_id: String::new(),
        group: "tdc_publish_test".to_string(),
        params,
        filter: None,
    }
}

#[tokio::test]
async fn kafka_sink_async_record_publishes_json_line() -> anyhow::Result<()> {
    if !common::is_kafka_available().await {
        eprintln!("skip: Kafka unavailable on {}", common::TEST_KAFKA_BROKERS);
        return Ok(());
    }
    let topic = common::generate_test_topic_name("tdc_publish");
    let spec = make_spec("tdc_publish", &topic);
    let factory = KafkaSinkFactory;
    factory.validate_spec(&spec)?;
    let ctx = wp_connector_api::SinkBuildCtx::new(std::env::current_dir()?);
    let mut handle = tokio::time::timeout(common::TEST_TIMEOUT, factory.build(&spec, &ctx))
        .await
        .map_err(|_| anyhow::anyhow!("build timeout"))??;

    // Build a tiny DataRecord and publish via sink_record
    let mut rec = DataRecord::default();
    rec.append(DataField::from_chars("k", "v"));
    rec.append(DataField::from_chars("n", "42"));

    tokio::time::timeout(common::TEST_TIMEOUT, handle.sink.sink_record(&rec))
        .await
        .map_err(|_| anyhow::anyhow!("publish timeout"))??;

    // Clean shutdown (best-effort)
    let _ = tokio::time::timeout(common::TEST_TIMEOUT, handle.sink.stop()).await;
    Ok(())
}
