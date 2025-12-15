//! Sink tests for wp-connectors unified connector

use serde_json::Value;
use std::collections::BTreeMap;
use wp_connector_api::{ResolvedSinkSpec, SinkFactory, SinkSpec};
use wp_connectors::kafka::KafkaSinkFactory;

mod common;

/// Create a test sink build context
fn create_build_context() -> wp_connector_api::SinkBuildCtx {
    wp_connector_api::SinkBuildCtx::new(std::path::PathBuf::from("."))
}

/// Create a minimal valid Kafka sink specification
fn create_test_spec(name: &str, topic: &str) -> SinkSpec {
    let mut params: BTreeMap<String, Value> = BTreeMap::new();
    params.insert(
        "brokers".to_string(),
        Value::String(common::TEST_KAFKA_BROKERS.to_string()),
    );
    params.insert("topic".to_string(), Value::String(topic.to_string()));
    params.insert("num_partitions".to_string(), Value::from(1));
    params.insert("replication".to_string(), Value::from(1));

    // Add common producer configuration
    let config_array = vec![
        Value::String("acks=all".to_string()),
        Value::String("retries=3".to_string()),
        Value::String("batch.size=16384".to_string()),
        Value::String("linger.ms=5".to_string()),
        Value::String("compression.type=snappy".to_string()),
    ];
    params.insert("config".to_string(), Value::Array(config_array));

    SinkSpec {
        name: name.to_string(),
        kind: "kafka".to_string(),
        connector_id: String::new(),
        group: "test".to_string(),
        params,
        filter: None,
    }
}

#[test]
fn kafka_sink_factory_kind_returns_kafka() {
    let factory = KafkaSinkFactory;
    assert_eq!(factory.kind(), "kafka");
}

#[test]
fn kafka_sink_factory_validates_minimal_valid_config() {
    let factory = KafkaSinkFactory;

    let mut params: BTreeMap<String, Value> = BTreeMap::new();
    params.insert(
        "brokers".to_string(),
        Value::String("localhost:9092".to_string()),
    );
    params.insert("topic".to_string(), Value::String("test-topic".to_string()));

    let spec = SinkSpec {
        group: "test_group".to_string(),
        name: "test".to_string(),
        kind: "kafka".to_string(),
        connector_id: String::new(),
        params,
        filter: None,
    };

    let result = factory.validate_spec(&spec);
    assert!(
        result.is_ok(),
        "Valid minimal config should pass validation: {:?}",
        result
    );
}

#[test]
fn kafka_sink_factory_validates_config_with_partitions_and_replication() {
    let factory = KafkaSinkFactory;

    let mut params: BTreeMap<String, Value> = BTreeMap::new();
    params.insert(
        "brokers".to_string(),
        Value::String("broker1:9092,broker2:9092".to_string()),
    );
    params.insert("topic".to_string(), Value::String("test-topic".to_string()));
    params.insert("num_partitions".to_string(), Value::from(3));
    params.insert("replication".to_string(), Value::from(2));

    let spec = SinkSpec {
        group: "test_partitions".to_string(),
        name: "test-partitions".to_string(),
        kind: "kafka".to_string(),
        connector_id: String::new(),
        params,
        filter: None,
    };

    let result = factory.validate_spec(&spec);
    assert!(
        result.is_ok(),
        "Config with partitions and replication should pass validation"
    );
}

#[test]
fn kafka_sink_factory_validates_config_with_custom_settings() {
    let factory = KafkaSinkFactory;

    let mut params: BTreeMap<String, Value> = BTreeMap::new();
    params.insert(
        "brokers".to_string(),
        Value::String("localhost:9092".to_string()),
    );
    params.insert("topic".to_string(), Value::String("test-topic".to_string()));

    let config_array = vec![
        Value::String("batch.size=16384".to_string()),
        Value::String("linger.ms=5".to_string()),
        Value::String("compression.type=snappy".to_string()),
        Value::String("acks=all".to_string()),
    ];
    params.insert("config".to_string(), Value::Array(config_array));

    let spec = SinkSpec {
        group: "test_custom".to_string(),
        name: "test-custom-config".to_string(),
        kind: "kafka".to_string(),
        connector_id: String::new(),
        params,
        filter: None,
    };

    let result = factory.validate_spec(&spec);
    assert!(
        result.is_ok(),
        "Config with custom Kafka settings should pass validation"
    );
}

//=============================================================================
// Validation Error Tests
//=============================================================================

#[test]
fn kafka_sink_factory_rejects_missing_brokers() {
    let factory = KafkaSinkFactory;

    let mut params: BTreeMap<String, Value> = BTreeMap::new();
    params.insert("topic".to_string(), Value::String("test-topic".to_string()));

    let spec = SinkSpec {
        group: "test_missing_brokers".to_string(),
        name: "test-missing-brokers".to_string(),
        kind: "kafka".to_string(),
        connector_id: String::new(),
        params,
        filter: None,
    };

    let result = factory.validate_spec(&spec);
    assert!(result.is_err(), "Missing brokers should be rejected");
    let error_msg = result.unwrap_err().to_string();
    assert!(
        error_msg.contains("brokers"),
        "Error should mention brokers: {}",
        error_msg
    );
}

#[test]
fn kafka_sink_factory_rejects_missing_topic() {
    let factory = KafkaSinkFactory;

    let mut params: BTreeMap<String, Value> = BTreeMap::new();
    params.insert(
        "brokers".to_string(),
        Value::String("localhost:9092".to_string()),
    );

    let spec = SinkSpec {
        group: "test_missing_topic".to_string(),
        name: "test-missing-topic".to_string(),
        kind: "kafka".to_string(),
        connector_id: String::new(),
        params,
        filter: None,
    };

    let result = factory.validate_spec(&spec);
    assert!(result.is_err(), "Missing topic should be rejected");
    let error_msg = result.unwrap_err().to_string();
    assert!(
        error_msg.contains("topic"),
        "Error should mention topic: {}",
        error_msg
    );
}

#[test]
fn kafka_sink_factory_rejects_zero_partitions() {
    let factory = KafkaSinkFactory;

    let mut params: BTreeMap<String, Value> = BTreeMap::new();
    params.insert(
        "brokers".to_string(),
        Value::String("localhost:9092".to_string()),
    );
    params.insert("topic".to_string(), Value::String("test-topic".to_string()));
    params.insert("num_partitions".to_string(), Value::from(0));

    let spec = SinkSpec {
        group: "test_zero_partitions".to_string(),
        name: "test-zero-partitions".to_string(),
        kind: "kafka".to_string(),
        connector_id: String::new(),
        params,
        filter: None,
    };

    let result = factory.validate_spec(&spec);
    assert!(result.is_err(), "Zero partitions should be rejected");
    let error_msg = result.unwrap_err().to_string();
    assert!(
        error_msg.contains("num_partitions"),
        "Error should mention num_partitions: {}",
        error_msg
    );
}

#[test]
fn kafka_sink_factory_rejects_negative_partitions() {
    let factory = KafkaSinkFactory;

    let mut params: BTreeMap<String, Value> = BTreeMap::new();
    params.insert(
        "brokers".to_string(),
        Value::String("localhost:9092".to_string()),
    );
    params.insert("topic".to_string(), Value::String("test-topic".to_string()));
    params.insert("num_partitions".to_string(), Value::from(-1));

    let spec = SinkSpec {
        group: "test_negative_partitions".to_string(),
        name: "test-negative-partitions".to_string(),
        kind: "kafka".to_string(),
        connector_id: String::new(),
        params,
        filter: None,
    };

    let result = factory.validate_spec(&spec);
    assert!(result.is_err(), "Negative partitions should be rejected");
    let error_msg = result.unwrap_err().to_string();
    assert!(
        error_msg.contains("num_partitions"),
        "Error should mention num_partitions: {}",
        error_msg
    );
}

#[test]
fn kafka_sink_factory_rejects_zero_replication() {
    let factory = KafkaSinkFactory;

    let mut params: BTreeMap<String, Value> = BTreeMap::new();
    params.insert(
        "brokers".to_string(),
        Value::String("localhost:9092".to_string()),
    );
    params.insert("topic".to_string(), Value::String("test-topic".to_string()));
    params.insert("replication".to_string(), Value::from(0));

    let spec = SinkSpec {
        group: "test_zero_replication".to_string(),
        name: "test-zero-replication".to_string(),
        kind: "kafka".to_string(),
        connector_id: String::new(),
        params,
        filter: None,
    };

    let result = factory.validate_spec(&spec);
    assert!(result.is_err(), "Zero replication should be rejected");
    let error_msg = result.unwrap_err().to_string();
    assert!(
        error_msg.contains("replication"),
        "Error should mention replication: {}",
        error_msg
    );
}

#[test]
fn kafka_sink_factory_rejects_negative_replication() {
    let factory = KafkaSinkFactory;

    let mut params: BTreeMap<String, Value> = BTreeMap::new();
    params.insert(
        "brokers".to_string(),
        Value::String("localhost:9092".to_string()),
    );
    params.insert("topic".to_string(), Value::String("test-topic".to_string()));
    params.insert("replication".to_string(), Value::from(-1));

    let spec = ResolvedSinkSpec {
        group: "test_negative_replication".to_string(),
        name: "test-negative-replication".to_string(),
        kind: "kafka".to_string(),
        connector_id: String::new(),
        params,
        filter: None,
    };

    let result = factory.validate_spec(&spec);
    assert!(result.is_err(), "Negative replication should be rejected");
    let error_msg = result.unwrap_err().to_string();
    assert!(
        error_msg.contains("replication"),
        "Error should mention replication: {}",
        error_msg
    );
}

//=============================================================================
// Edge Case Tests
//=============================================================================

#[test]
fn kafka_sink_factory_handles_complex_broker_strings() {
    let factory = KafkaSinkFactory;

    let test_cases = vec![
        "localhost:9092",
        "broker1:9092,broker2:9092",
        "kafka1.example.com:9092,kafka2.example.com:9092,kafka3.example.com:9092",
        "127.0.0.1:9092,127.0.0.1:9093",
        "[::1]:9092", // IPv6
    ];

    for (i, brokers) in test_cases.iter().enumerate() {
        let mut params: BTreeMap<String, Value> = BTreeMap::new();
        params.insert("brokers".to_string(), Value::String(brokers.to_string()));
        params.insert(
            "topic".to_string(),
            Value::String(format!("test-topic-{}", i)),
        );

        let spec = ResolvedSinkSpec {
            group: format!("test_brokers_{}", i),
            name: format!("test-brokers-{}", i),
            kind: "kafka".to_string(),
            connector_id: String::new(),
            params,
            filter: None,
        };

        let result = factory.validate_spec(&spec);
        assert!(
            result.is_ok(),
            "Broker string '{}' should be valid",
            brokers
        );
    }
}

#[test]
fn kafka_sink_factory_handles_special_characters_in_topics() {
    let factory = KafkaSinkFactory;

    let test_topics = vec![
        "test-topic.normal",
        "test_topic_underscore",
        "test-topic.with-dashes",
        "test_topic.with_underscores_and-numbers123",
        "logs.app.service",
        "metrics.system.performance",
    ];

    for topic in test_topics {
        let mut params: BTreeMap<String, Value> = BTreeMap::new();
        params.insert(
            "brokers".to_string(),
            Value::String("localhost:9092".to_string()),
        );
        params.insert("topic".to_string(), Value::String(topic.to_string()));

        let spec = ResolvedSinkSpec {
            group: format!("test_topic_{}", topic.replace('.', "-")),
            name: format!("test-topic-{}", topic.replace('.', "-")),
            kind: "kafka".to_string(),
            connector_id: String::new(),
            params,
            filter: None,
        };

        let result = factory.validate_spec(&spec);
        assert!(result.is_ok(), "Topic '{}' should be accepted", topic);
    }
}

#[test]
fn kafka_sink_factory_handles_large_partition_and_replication_values() {
    let factory = KafkaSinkFactory;

    let test_cases = vec![
        (1, 1),   // Minimum valid values
        (3, 2),   // Common production values
        (10, 3),  // Larger values
        (100, 5), // Very large values
    ];

    for (partitions, replication) in test_cases {
        let mut params: BTreeMap<String, Value> = BTreeMap::new();
        params.insert(
            "brokers".to_string(),
            Value::String("localhost:9092".to_string()),
        );
        params.insert("topic".to_string(), Value::String("test-topic".to_string()));
        params.insert("num_partitions".to_string(), Value::from(partitions));
        params.insert("replication".to_string(), Value::from(replication));

        let spec = ResolvedSinkSpec {
            group: format!("test_p{}-r{}", partitions, replication),
            name: format!("test-p{}-r{}", partitions, replication),
            kind: "kafka".to_string(),
            connector_id: String::new(),
            params,
            filter: None,
        };

        let result = factory.validate_spec(&spec);
        assert!(
            result.is_ok(),
            "Partitions: {}, Replication: {} should be valid",
            partitions,
            replication
        );
    }
}
