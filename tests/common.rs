#![cfg(feature = "kafka")]
#![allow(dead_code)]
//! Common utilities and helpers for Kafka connector tests

use std::time::Duration;

/// Kafka broker address for testing
pub const TEST_KAFKA_BROKERS: &str = "localhost:9092";

/// Test topic name
pub const TEST_TOPIC: &str = "wp_kafka_test_topic";

/// Test timeout duration
pub const TEST_TIMEOUT: Duration = Duration::from_secs(15);

/// Connection test timeout
pub const CONNECTION_TIMEOUT: Duration = Duration::from_secs(5);

/// Environment variable to skip integration tests
pub const SKIP_INTEGRATION_TESTS: &str = "SKIP_KAFKA_INTEGRATION_TESTS";

/// Check if Kafka is available before running integration tests
pub async fn is_kafka_available() -> bool {
    // Skip if environment variable is set
    if std::env::var(SKIP_INTEGRATION_TESTS).is_ok() {
        println!(
            "⚠️  Skipping Kafka integration tests ({} is set)",
            SKIP_INTEGRATION_TESTS
        );
        return false;
    }

    // Simple connectivity check (could be enhanced with actual connection attempt)
    println!("🔍 Checking Kafka availability...");

    // For now, we'll use a simple TCP connection check
    // In a real implementation, you might want to create a minimal Kafka connection
    match tokio::net::TcpStream::connect("localhost:9092").await {
        Ok(_) => {
            println!("✅ Kafka is available for integration tests");
            true
        }
        Err(e) => {
            println!("⚠️  Kafka is not available: {}", e);
            false
        }
    }
}

/// Generate unique test topic names
pub fn generate_test_topic_name(test_name: &str) -> String {
    format!(
        "{}_{}_{}",
        TEST_TOPIC,
        test_name,
        chrono::Utc::now().timestamp()
    )
}

/// Generate unique test group IDs
pub fn generate_test_group_id(test_name: &str) -> String {
    format!(
        "test-group-{}-{}",
        test_name,
        chrono::Utc::now().timestamp()
    )
}
