//! Source tests for wp-connectors unified connector

use wp_connectors::kafka::KafkaSourceFactory;
use wp_connector_api::{SourceSpec, SourceFactory, ResolvedSourceSpec};
use std::collections::BTreeMap;
use serde_json::Value;

mod common;

/// Create a test source build context
fn create_build_context() -> wp_connector_api::SourceBuildCtx {
    wp_connector_api::SourceBuildCtx::new(std::path::PathBuf::from("."))
}

/// Create a minimal valid Kafka source specification
fn create_test_spec(name: &str, topic: &str, group_id: &str) -> SourceSpec {
    let mut params: BTreeMap<String, Value> = BTreeMap::new();
    params.insert(
        "brokers".to_string(),
        Value::String(common::TEST_KAFKA_BROKERS.to_string()),
    );
    params.insert("topic".to_string(), Value::String(topic.to_string()));
    params.insert("group_id".to_string(), Value::String(group_id.to_string()));

    // Use earliest offset to consume existing messages
    let config_array = vec![
        Value::String("auto.offset.reset=earliest".to_string()),
        Value::String("enable.auto.commit=true".to_string()),
    ];
    params.insert("config".to_string(), Value::Array(config_array));

    SourceSpec {
        name: name.to_string(),
        kind: "kafka".to_string(),
        connector_id: String::new(),
        params,
        tags: vec!["env:unit_test".to_string(), "source:kafka".to_string()],
    }
}

#[test]
fn kafka_source_factory_kind_returns_kafka() {
    let factory = KafkaSourceFactory;
    assert_eq!(factory.kind(), "kafka");
}

#[test]
fn kafka_source_factory_validates_minimal_valid_config() {
    let factory = KafkaSourceFactory;

    let mut params: BTreeMap<String, Value> = BTreeMap::new();
    params.insert(
        "brokers".to_string(),
        Value::String("localhost:9092".to_string()),
    );
    params.insert(
        "topic".to_string(),
        Value::String("test-topic".to_string()),
    );
    params.insert(
        "group_id".to_string(),
        Value::String("test-group".to_string()),
    );

    let spec = SourceSpec {
        name: "test".to_string(),
        kind: "kafka".to_string(),
        connector_id: String::new(),
        params,
        tags: vec![],
    };

    let result = factory.validate_spec(&spec);
    assert!(
        result.is_ok(),
        "Valid minimal config should pass validation: {:?}",
        result
    );
}

#[test]
fn kafka_source_factory_validates_config_with_custom_settings() {
    let factory = KafkaSourceFactory;

    let mut params: BTreeMap<String, Value> = BTreeMap::new();
    params.insert(
        "brokers".to_string(),
        Value::String("localhost:9092".to_string()),
    );
    params.insert(
        "topic".to_string(),
        Value::String("test-topic".to_string()),
    );
    params.insert(
        "group_id".to_string(),
        Value::String("test-group".to_string()),
    );

    let config_array = vec![
        Value::String("auto.offset.reset=earliest".to_string()),
        Value::String("enable.auto.commit=true".to_string()),
        Value::String("session.timeout.ms=30000".to_string()),
        Value::String("heartbeat.interval.ms=3000".to_string()),
    ];
    params.insert("config".to_string(), Value::Array(config_array));

    let spec = SourceSpec {
        name: "test-custom-config".to_string(),
        kind: "kafka".to_string(),
        connector_id: String::new(),
        params,
        tags: vec![],
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
fn kafka_source_factory_rejects_missing_brokers() {
    let factory = KafkaSourceFactory;

    let mut params: BTreeMap<String, Value> = BTreeMap::new();
    params.insert(
        "topic".to_string(),
        Value::String("test-topic".to_string()),
    );
    params.insert(
        "group_id".to_string(),
        Value::String("test-group".to_string()),
    );

    let spec = SourceSpec {
        name: "test-missing-brokers".to_string(),
        kind: "kafka".to_string(),
        connector_id: String::new(),
        params,
        tags: vec![],
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
fn kafka_source_factory_rejects_missing_topic() {
    let factory = KafkaSourceFactory;

    let mut params: BTreeMap<String, Value> = BTreeMap::new();
    params.insert(
        "brokers".to_string(),
        Value::String("localhost:9092".to_string()),
    );
    params.insert(
        "group_id".to_string(),
        Value::String("test-group".to_string()),
    );

    let spec = SourceSpec {
        name: "test-missing-topic".to_string(),
        kind: "kafka".to_string(),
        connector_id: String::new(),
        params,
        tags: vec![],
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
fn kafka_source_factory_rejects_missing_group_id() {
    let factory = KafkaSourceFactory;

    let mut params: BTreeMap<String, Value> = BTreeMap::new();
    params.insert(
        "brokers".to_string(),
        Value::String("localhost:9092".to_string()),
    );
    params.insert(
        "topic".to_string(),
        Value::String("test-topic".to_string()),
    );

    let spec = SourceSpec {
        name: "test-missing-group-id".to_string(),
        kind: "kafka".to_string(),
        connector_id: String::new(),
        params,
        tags: vec![],
    };

    let result = factory.validate_spec(&spec);
    assert!(result.is_err(), "Missing group_id should be rejected");
    let error_msg = result.unwrap_err().to_string();
    assert!(
        error_msg.contains("group_id"),
        "Error should mention group_id: {}",
        error_msg
    );
}

#[test]
fn kafka_source_factory_rejects_empty_brokers() {
    let factory = KafkaSourceFactory;

    let mut params: BTreeMap<String, Value> = BTreeMap::new();
    params.insert("brokers".to_string(), Value::String("".to_string()));
    params.insert(
        "topic".to_string(),
        Value::String("test-topic".to_string()),
    );
    params.insert(
        "group_id".to_string(),
        Value::String("test-group".to_string()),
    );

    let spec = SourceSpec {
        name: "test-empty-brokers".to_string(),
        kind: "kafka".to_string(),
        connector_id: String::new(),
        params,
        tags: vec![],
    };

    let result = factory.validate_spec(&spec);
    assert!(result.is_err(), "Empty brokers should be rejected");
}

#[test]
fn kafka_source_factory_rejects_empty_topic() {
    let factory = KafkaSourceFactory;

    let mut params: BTreeMap<String, Value> = BTreeMap::new();
    params.insert(
        "brokers".to_string(),
        Value::String("localhost:9092".to_string()),
    );
    params.insert("topic".to_string(), Value::String("".to_string()));
    params.insert(
        "group_id".to_string(),
        Value::String("test-group".to_string()),
    );

    let spec = ResolvedSourceSpec {
        name: "test-empty-topic".to_string(),
        kind: "kafka".to_string(),
        connector_id: String::new(),
        params,
        tags: vec![],
    };

    let result = factory.validate_spec(&spec);
    assert!(result.is_err(), "Empty topic should be rejected");
}

#[test]
fn kafka_source_factory_rejects_empty_group_id() {
    let factory = KafkaSourceFactory;

    let mut params: BTreeMap<String, Value> = BTreeMap::new();
    params.insert(
        "brokers".to_string(),
        Value::String("localhost:9092".to_string()),
    );
    params.insert(
        "topic".to_string(),
        Value::String("test-topic".to_string()),
    );
    params.insert("group_id".to_string(), Value::String("".to_string()));

    let spec = ResolvedSourceSpec {
        name: "test-empty-group-id".to_string(),
        kind: "kafka".to_string(),
        connector_id: String::new(),
        params,
        tags: vec![],
    };

    let result = factory.validate_spec(&spec);
    assert!(result.is_err(), "Empty group_id should be rejected");
}

#[test]
fn kafka_source_factory_handles_whitespace_only_values() {
    let factory = KafkaSourceFactory;

    let test_cases = vec![
        ("brokers", "   \t\n   "),
        ("topic", "   \t\n   "),
        ("group_id", "   \t\n   "),
    ];

    for (field, value) in test_cases {
        let mut params: BTreeMap<String, Value> = BTreeMap::new();
        params.insert(
            "brokers".to_string(),
            Value::String("localhost:9092".to_string()),
        );
        params.insert(
            "topic".to_string(),
            Value::String("test-topic".to_string()),
        );
        params.insert(
            "group_id".to_string(),
            Value::String("test-group".to_string()),
        );

        // Replace one field with whitespace-only value
        params.insert(field.to_string(), Value::String(value.to_string()));

        let spec = ResolvedSourceSpec {
            name: format!("test-whitespace-{}", field),
            kind: "kafka".to_string(),
            connector_id: String::new(),
            params,
            tags: vec![],
        };

        let result = factory.validate_spec(&spec);
        assert!(
            result.is_err(),
            "Whitespace-only {} should be rejected",
            field
        );
    }
}

//=============================================================================
// Edge Case Tests
//=============================================================================

#[test]
fn kafka_source_factory_handles_complex_broker_strings() {
    let factory = KafkaSourceFactory;

    let test_cases = vec![
        "localhost:9092",
        "broker1:9092,broker2:9092",
        "kafka1.example.com:9092,kafka2.example.com:9092,kafka3.example.com:9092",
        "127.0.0.1:9092,127.0.0.1:9093",
        "[::1]:9092", // IPv6
    ];

    for (i, brokers) in test_cases.iter().enumerate() {
        let mut params: BTreeMap<String, Value> = BTreeMap::new();
        params.insert(
            "brokers".to_string(),
            Value::String(brokers.to_string()),
        );
        params.insert(
            "topic".to_string(),
            Value::String(format!("test-topic-{}", i)),
        );
        params.insert(
            "group_id".to_string(),
            Value::String(format!("test-group-{}", i)),
        );

        let spec = ResolvedSourceSpec {
            name: format!("test-brokers-{}", i),
            kind: "kafka".to_string(),
            connector_id: String::new(),
            params,
            tags: vec![],
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
fn kafka_source_factory_handles_special_characters_in_topics_and_groups() {
    let factory = KafkaSourceFactory;

    let test_topics = vec![
        "test-topic.normal",
        "test_topic_underscore",
        "test-topic.with-dashes",
        "test_topic.with_underscores_and-numbers123",
        "logs.app.service",
        "metrics.system.performance",
    ];

    let test_groups = vec![
        "consumer-group.test",
        "test_consumer_group",
        "group-with-dashes",
        "group_with_underscores_and-numbers123",
        "app.service.group",
        "system.metrics.group",
    ];

    for (i, (topic, group)) in test_topics.iter().zip(test_groups.iter()).enumerate() {
        let mut params: BTreeMap<String, Value> = BTreeMap::new();
        params.insert(
            "brokers".to_string(),
            Value::String("localhost:9092".to_string()),
        );
        params.insert("topic".to_string(), Value::String(topic.to_string()));
        params.insert(
            "group_id".to_string(),
            Value::String(group.to_string()),
        );

        let spec = ResolvedSourceSpec {
            name: format!("test-special-chars-{}", i),
            kind: "kafka".to_string(),
            connector_id: String::new(),
            params,
            tags: vec![],
        };

        let result = factory.validate_spec(&spec);
        assert!(
            result.is_ok(),
            "Topic '{}' and group '{}' should be accepted",
            topic,
            group
        );
    }
}
#![cfg(feature = "kafka")]
