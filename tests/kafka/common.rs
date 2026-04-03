#![cfg(all(
    feature = "kafka",
    any(feature = "external_integration", feature = "external_performance")
))]

use anyhow::{Context, Result};
use rdkafka_wrap::{KWConsumer, KWConsumerConf, KWProducer, KWProducerConf, OptionExt};
use serde_json::json;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::net::TcpStream;
use tokio::time::{Duration, timeout};
use wp_connector_api::ParamMap;

pub const TEST_KAFKA_BROKERS: &str = "127.0.0.1:9092";
#[cfg(feature = "external_integration")]
pub const ALL_KAFKA_FORMATS: [&str; 6] = ["json", "csv", "show", "kv", "raw", "proto-text"];

#[cfg(feature = "external_integration")]
const TEST_KAFKA_TOPIC_PREFIX: &str = "wp_kafka_sink";
const KAFKA_READY_ATTEMPTS: usize = 20;
const KAFKA_READY_INTERVAL_SECS: u64 = 2;
const KAFKA_READY_STABLE_PROBES: usize = 3;
const KAFKA_COUNT_IDLE_TIMEOUT_MS: u64 = 1000;

static NEXT_KAFKA_TOPIC_SUFFIX: AtomicU64 = AtomicU64::new(1);

fn next_suffix() -> u64 {
    NEXT_KAFKA_TOPIC_SUFFIX.fetch_add(1, Ordering::Relaxed)
}

fn unique_topic(prefix: &str) -> String {
    format!("{}_{}", prefix, next_suffix())
}

fn topic_from_params(params: &ParamMap) -> Result<String> {
    params
        .get("topic")
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned)
        .ok_or_else(|| anyhow::anyhow!("Kafka 测试参数缺少 topic"))
}

fn kafka_producer(topic: &str) -> Result<KWProducer> {
    let conf = KWProducerConf::new(TEST_KAFKA_BROKERS).set_topic_conf(topic, 1, 1);
    KWProducer::new(conf).context("创建 Kafka producer 失败")
}

async fn create_topic(topic: &str) -> Result<()> {
    let producer = kafka_producer(topic)?;
    producer
        .create_topic()
        .await
        .with_context(|| format!("创建 Kafka topic 失败: {topic}"))?;
    Ok(())
}

async fn probe_kafka_service_ready() -> Result<()> {
    TcpStream::connect(TEST_KAFKA_BROKERS)
        .await
        .with_context(|| format!("连接 Kafka 失败: {}", TEST_KAFKA_BROKERS))?;
    Ok(())
}

#[cfg(feature = "external_performance")]
pub fn create_kafka_performance_config() -> ParamMap {
    create_kafka_config(unique_topic("wp_kafka_perf"), "json")
}

#[cfg(feature = "external_integration")]
pub fn create_kafka_test_scenarios() -> Vec<(String, ParamMap)> {
    ALL_KAFKA_FORMATS
        .into_iter()
        .map(|fmt| {
            (
                format!("basic_{fmt}"),
                create_kafka_config(
                    unique_topic(&format!("{}_{}", TEST_KAFKA_TOPIC_PREFIX, fmt)),
                    fmt,
                ),
            )
        })
        .collect()
}

fn create_kafka_config(topic: String, fmt: &str) -> ParamMap {
    let mut params = BTreeMap::new();
    params.insert("brokers".into(), json!(TEST_KAFKA_BROKERS));
    params.insert("topic".into(), json!(topic));
    params.insert("num_partitions".into(), json!(1));
    params.insert("replication".into(), json!(1));
    params.insert("fmt".into(), json!(fmt));
    params.insert("config".into(), json!(["acks=all", "linger.ms=5"]));
    params
}

pub async fn wait_for_kafka_ready() -> Result<()> {
    let mut last_error = None;
    let mut stable_successes = 0usize;

    for attempt in 1..=KAFKA_READY_ATTEMPTS {
        match probe_kafka_service_ready().await {
            Ok(()) => {
                stable_successes += 1;
                if stable_successes >= KAFKA_READY_STABLE_PROBES {
                    println!(
                        "✓ Kafka 服务已稳定就绪，连续 {} 次探测成功（第 {} 次完成）",
                        KAFKA_READY_STABLE_PROBES, attempt
                    );
                    return Ok(());
                }

                println!(
                    "Kafka 服务探测成功，继续观察稳定性（{}/{})...",
                    stable_successes, KAFKA_READY_STABLE_PROBES
                );
            }
            Err(err) => {
                stable_successes = 0;
                last_error = Some(err.to_string());
            }
        }

        tokio::time::sleep(Duration::from_secs(KAFKA_READY_INTERVAL_SECS)).await;
    }

    anyhow::bail!(
        "等待 Kafka 服务就绪超时: {}",
        last_error.unwrap_or_else(|| "未知错误".to_string())
    )
}

pub async fn init_kafka_topic_with_params(params: ParamMap) -> Result<()> {
    let topic = topic_from_params(&params)?;
    create_topic(&topic).await?;
    println!("✓ Kafka 测试 topic 初始化完成: {topic}");
    Ok(())
}

pub async fn query_topic_count(params: ParamMap) -> Result<i64> {
    let topic = topic_from_params(&params)?;
    let group_id = unique_topic("wp_kafka_count_group");
    let conf = KWConsumerConf::new(TEST_KAFKA_BROKERS, &group_id)
        .set_config(HashMap::from([
            ("enable.partition.eof", "false"),
            ("auto.offset.reset", "earliest"),
            ("enable.auto.commit", "false"),
            ("session.timeout.ms", "6000"),
        ]))
        .set_topics(vec![topic.as_str()]);
    let consumer = KWConsumer::new_subscribe(conf)
        .with_context(|| format!("创建 Kafka count consumer 失败: {topic}"))?;

    let mut count = 0i64;
    loop {
        match timeout(
            Duration::from_millis(KAFKA_COUNT_IDLE_TIMEOUT_MS),
            consumer.recv(),
        )
        .await
        {
            Ok(Ok(_)) => count += 1,
            Ok(Err(err)) => {
                return Err(anyhow::anyhow!("消费 Kafka topic 失败: {topic}: {err}"));
            }
            Err(_) => break,
        }
    }

    Ok(count)
}
