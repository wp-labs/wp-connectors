//! Sink roundtrip test: build Kafka sink, publish raw payload, then consume via KWConsumer and assert payload.

use std::time::Duration;

use rdkafka_wrap::consumer::{CommitMode, Consumer};
use rdkafka_wrap::message::Message;
use rdkafka_wrap::{KWConsumer, KWConsumerConf};
use serde_json::Value;
use std::collections::BTreeMap;
use tokio::time::timeout;
use wp_connector_api::{ResolvedSinkSpec, SinkFactory};
use wp_connectors::kafka::KafkaSinkFactory;

use crate::common::TEST_KAFKA_BROKERS;

mod common;

#[tokio::test]
async fn kafka_sink_roundtrip_raw() -> anyhow::Result<()> {
    if !common::is_kafka_available().await {
        eprintln!("skip: Kafka unavailable on {}", common::TEST_KAFKA_BROKERS);
        return Ok(());
    }

    let topic = common::generate_test_topic_name("sink_rt");
    let group = common::generate_test_group_id("sink_rt");
    let expected = "hello-kafka-roundtrip";

    // 1) Build sink
    let mut params: BTreeMap<String, Value> = BTreeMap::new();
    params.insert(
        "brokers".to_string(),
        Value::String(common::TEST_KAFKA_BROKERS.to_string()),
    );
    params.insert("topic".to_string(), Value::String(topic.clone()));
    params.insert("num_partitions".to_string(), Value::from(1));
    params.insert("replication".to_string(), Value::from(1));
    let spec = ResolvedSinkSpec {
        name: "sink_roundtrip".to_string(),
        kind: "kafka".to_string(),
        connector_id: String::new(),
        group: "rt".to_string(),
        params,
        filter: None,
    };
    let factory = KafkaSinkFactory;
    factory.validate_spec(&spec)?;
    let ctx = wp_connector_api::SinkBuildCtx::new(std::env::current_dir()?);
    let mut handle = timeout(common::TEST_TIMEOUT, factory.build(&spec, &ctx))
        .await
        .map_err(|_| anyhow::anyhow!("build timeout"))??;

    // 2) Produce raw payload via sink
    timeout(common::TEST_TIMEOUT, handle.sink.sink_str(expected))
        .await
        .map_err(|_| anyhow::anyhow!("publish timeout"))??;

    // 3) Consume and assert
    let conf = KWConsumerConf::new(TEST_KAFKA_BROKERS, &group)
        .set_config(std::collections::HashMap::from([
            ("enable.partition.eof", "false"),
            ("auto.offset.reset", "earliest"),
            ("enable.auto.commit", "true"),
        ]))
        .set_topics(vec![topic.as_str()]);
    let consumer = KWConsumer::new_subscribe(conf)?;
    let msg = timeout(common::TEST_TIMEOUT, async {
        loop {
            if let Ok(m) = consumer.recv().await {
                break m;
            }
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("consume timeout"))?;
    let payload = match msg.payload_view::<str>() {
        Some(Ok(s)) => s,
        _ => "",
    };
    assert_eq!(payload, expected);
    // async commit
    let _ = consumer.consumer.commit_message(&msg, CommitMode::Async);

    // 4) stop sink
    let _ = timeout(common::TEST_TIMEOUT, handle.sink.stop()).await;
    Ok(())
}
