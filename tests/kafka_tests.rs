#![cfg(feature = "kafka")]
// Wrapper test to include tests under tests/kafka/ and shared helpers in tests/common.rs

#[path = "kafka/integration_tests.rs"]
mod integration_tests;

#[path = "common.rs"]
mod common;
