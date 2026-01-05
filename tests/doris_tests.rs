#![cfg(feature = "doris")]
// Wrapper test to include tests under tests/kafka/ and shared helpers in tests/common.rs

#[path = "doris/integration_tests.rs"]
mod integration_tests;
