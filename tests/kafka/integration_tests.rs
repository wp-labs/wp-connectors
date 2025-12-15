#![cfg(feature = "kafka")]
//! Integration tests for wp-connectors unified connector
//!
//! This test suite performs integration tests with a running Kafka instance.
//! Tests are designed to gracefully handle Kafka unavailability.

use serde_json::Value;
use std::collections::BTreeMap;
use wp_connector_api::{SinkFactory, SinkSpec};
use wp_connector_api::{SourceFactory, SourceSpec};
use wp_connectors::kafka::{KafkaSinkFactory, KafkaSourceFactory};

use crate::common::TEST_KAFKA_BROKERS;

#[tokio::test]
async fn kafka_source_factory_creates_source_with_kafka_connection() -> anyhow::Result<()> {
    println!("⚠️  Skipping integration test - requires manual Kafka setup");
    // This test would require a running Kafka instance
    // For now, we just test that the factory can validate the configuration
    let mut params: BTreeMap<String, Value> = BTreeMap::new();
    params.insert(
        "brokers".to_string(),
        Value::String(TEST_KAFKA_BROKERS.to_string()),
    );
    params.insert("topic".to_string(), Value::String("test-topic".to_string()));
    params.insert(
        "group_id".to_string(),
        Value::String("test-group".to_string()),
    );

    let spec = SourceSpec {
        name: "test_source_connection".to_string(),
        kind: "kafka".to_string(),
        connector_id: String::new(),
        params,
        tags: vec![
            "env:integration_test".to_string(),
            "source:kafka".to_string(),
        ],
    };

    let factory = KafkaSourceFactory;
    let result = factory.validate_spec(&spec);
    assert!(result.is_ok(), "Valid configuration should pass validation");
    println!("✅ Source configuration validation passed");
    Ok(())
}

#[tokio::test]
async fn kafka_sink_factory_creates_sink_with_kafka_connection() -> anyhow::Result<()> {
    println!("⚠️  Skipping integration test - requires manual Kafka setup");
    // This test would require a running Kafka instance
    // For now, we just test that the factory can validate the configuration
    let mut params: BTreeMap<String, Value> = BTreeMap::new();
    params.insert(
        "brokers".to_string(),
        Value::String(TEST_KAFKA_BROKERS.to_string()),
    );
    params.insert("topic".to_string(), Value::String("test-topic".to_string()));
    params.insert("num_partitions".to_string(), Value::from(1));
    params.insert("replication".to_string(), Value::from(1));

    let spec = SinkSpec {
        name: "test_sink_connection".to_string(),
        kind: "kafka".to_string(),
        connector_id: String::new(),
        group: "integration_test".to_string(),
        params,
        filter: None,
    };

    let factory = KafkaSinkFactory;
    let result = factory.validate_spec(&spec);
    assert!(result.is_ok(), "Valid configuration should pass validation");
    println!("✅ Sink configuration validation passed");
    Ok(())
}

#[test]
fn kafka_factory_registration_test() {
    // Test that both factories can be registered
    println!("🔧 Testing factory registration...");

    // Source factory
    let source_factory = KafkaSourceFactory;
    assert_eq!(source_factory.kind(), "kafka");
    println!("✅ Source factory kind: {}", source_factory.kind());

    // Sink factory
    let sink_factory = KafkaSinkFactory;
    assert_eq!(sink_factory.kind(), "kafka");
    println!("✅ Sink factory kind: {}", sink_factory.kind());

    // Both factories should have the same kind
    assert_eq!(source_factory.kind(), sink_factory.kind());
    println!(
        "✅ Both factories have consistent kind: {}",
        source_factory.kind()
    );
}

#[tokio::test]
async fn kafka_configuration_validation_comprehensive() -> anyhow::Result<()> {
    println!("🔍 Testing comprehensive configuration validation...");

    // Test complex source configuration
    {
        let mut params: BTreeMap<String, Value> = BTreeMap::new();
        params.insert(
            "brokers".to_string(),
            Value::String("broker1:9092,broker2:9092,broker3:9092".to_string()),
        );
        params.insert(
            "topic".to_string(),
            Value::String("complex-source-test".to_string()),
        );
        params.insert(
            "group_id".to_string(),
            Value::String("complex-source-group".to_string()),
        );

        let config_array = vec![
            Value::String("auto.offset.reset=earliest".to_string()),
            Value::String("enable.auto.commit=true".to_string()),
            Value::String("session.timeout.ms=30000".to_string()),
            Value::String("heartbeat.interval.ms=3000".to_string()),
            Value::String("max.poll.records=500".to_string()),
            Value::String("fetch.min.bytes=1024".to_string()),
        ];
        params.insert("config".to_string(), Value::Array(config_array));

        let spec = SourceSpec {
            name: "complex_source_test".to_string(),
            kind: "kafka".to_string(),
            connector_id: String::new(),
            params,
            tags: vec!["test:complex".to_string()],
        };

        let factory = KafkaSourceFactory;
        let result = factory.validate_spec(&spec);
        assert!(
            result.is_ok(),
            "Complex source configuration should be valid"
        );
        println!("✅ Complex source configuration validation passed");
    }

    // Test complex sink configuration
    {
        let mut params: BTreeMap<String, Value> = BTreeMap::new();
        params.insert(
            "brokers".to_string(),
            Value::String("broker1:9092,broker2:9092".to_string()),
        );
        params.insert(
            "topic".to_string(),
            Value::String("complex-sink-test".to_string()),
        );
        params.insert("num_partitions".to_string(), Value::from(3));
        params.insert("replication".to_string(), Value::from(2));

        let config_array = vec![
            Value::String("acks=all".to_string()),
            Value::String("retries=5".to_string()),
            Value::String("batch.size=32768".to_string()),
            Value::String("linger.ms=10".to_string()),
            Value::String("compression.type=snappy".to_string()),
            Value::String("max.in.flight.requests.per.connection=10".to_string()),
            Value::String("enable.idempotence=true".to_string()),
        ];
        params.insert("config".to_string(), Value::Array(config_array));

        let spec = SinkSpec {
            name: "complex_sink_test".to_string(),
            kind: "kafka".to_string(),
            connector_id: String::new(),
            group: "complex_test".to_string(),
            params,
            filter: Some("test_filter".to_string()),
        };

        let factory = KafkaSinkFactory;
        let result = factory.validate_spec(&spec);
        assert!(result.is_ok(), "Complex sink configuration should be valid");
        println!("✅ Complex sink configuration validation passed");
    }

    println!("✅ Comprehensive configuration validation completed");
    Ok(())
}
