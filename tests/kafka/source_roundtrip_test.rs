//! Source integration test: produce one message to a fresh topic,
//! then build KafkaSource via factory and assert we can recv the payload.

use std::time::Duration;

use rdkafka_wrap::{KWProducer, KWProducerConf, OptionExt};
use wp_connectors::kafka::KafkaSourceFactory;
use wp_lang::RawData;
use wp_connector_api::{SourceSpec, SourceFactory};
use std::collections::BTreeMap;
use serde_json::Value;

mod common;

#[tokio::test]
async fn kafka_source_factory_roundtrip_recv() -> anyhow::Result<()> {
    if !common::is_kafka_available().await {
        eprintln!("skip: Kafka unavailable on {}", common::TEST_KAFKA_BROKERS);
        return Ok(());
    }

    // Unique topic + group
    let topic = common::generate_test_topic_name("src_rt");
    let group_id = common::generate_test_group_id("src_rt");
    let expected = "hello-source-roundtrip";

    // 1) Produce a single message to the topic
    let pconf = KWProducerConf::new(common::TEST_KAFKA_BROKERS).set_topic_conf(&topic, 1, 1);
    let producer = KWProducer::new(pconf)?;
    producer.create_topic().await?;
    producer
        .publish(expected.as_bytes(), Default::default())
        .await?;

    // 2) Build source via factory and recv
    let mut params: BTreeMap<String, Value> = BTreeMap::new();
    params.insert(
        "brokers".to_string(),
        Value::String(common::TEST_KAFKA_BROKERS.to_string()),
    );
    params.insert("topic".to_string(), Value::String(topic.clone()));
    params.insert("group_id".to_string(), Value::String(group_id));
    params.insert(
        "config".to_string(),
        Value::Array(vec![Value::String("auto.offset.reset=earliest".to_string())]),
    );

    let spec = SourceSpec {
        name: "src_roundtrip".to_string(),
        kind: "kafka".to_string(),
        connector_id: String::new(),
        params,
        tags: vec![],
    };
    let factory = KafkaSourceFactory;
    factory.validate_spec(&spec)?;
    let ctx = wp_connector_api::SourceBuildCtx::new(std::env::current_dir()?);
    let mut handle = tokio::time::timeout(common::TEST_TIMEOUT, factory.build(&spec, &ctx))
        .await
        .map_err(|_| anyhow::anyhow!("build timeout"))??;

    // 3) Recv and assert payload
    let frame = tokio::time::timeout(common::TEST_TIMEOUT, handle.source.recv())
        .await
        .map_err(|_| anyhow::anyhow!("recv timeout"))??;
    match frame.payload {
        RawData::String(ref s) => assert_eq!(s, expected),
        RawData::Bytes(ref b) => assert_eq!(std::str::from_utf8(b).unwrap(), expected),
    }
    Ok(())
}
#![cfg(feature = "kafka")]
