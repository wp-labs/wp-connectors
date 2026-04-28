//! HTTP Sink module for wp-connectors
//!
//! This module provides HTTP/HTTPS data transmission capabilities, enabling data records
//! to be sent to arbitrary HTTP endpoints. It supports multiple data formats, compression,
//! authentication, and retry mechanisms.
//!
//! # Features
//!
//! - **Protocol Support**: HTTP and HTTPS with automatic TLS/SSL handling
//! - **HTTP Methods**: GET, POST, PUT, PATCH, DELETE (configurable)
//! - **Authentication**: HTTP Basic Authentication with Base64 encoding
//! - **Data Formats**: JSON, NDJSON, CSV, KV, Raw, Proto-Text
//! - **Compression**: Gzip compression to reduce network bandwidth
//! - **Batch Sending**: Configurable batch size for optimized throughput
//! - **Retry Mechanism**: Exponential backoff retry strategy with configurable limits
//! - **Concurrent Safety**: Thread-safe HTTP client based on reqwest
//! - **Performance Monitoring**: Integrated time statistics for performance tracking
//!
//! # Configuration Parameters
//!
//! ## Required Parameters
//!
//! - **endpoint**: Target HTTP(S) URL (e.g., `https://api.example.com/webhook`)
//!
//! ## Optional Parameters
//!
//! - **method**: HTTP method (default: `POST`)
//!   - Supported: GET, POST, PUT, PATCH, DELETE
//! - **username**: Basic Auth username (default: empty, no auth)
//! - **password**: Basic Auth password (default: empty)
//! - **headers**: Custom HTTP headers as key-value map (default: empty)
//!   - Can include Content-Type to override auto-inference
//! - **fmt**: Output format (default: `json`)
//!   - Supported: json, ndjson, csv, kv, raw, proto-text
//! - **batch_size**: Number of records per request (default: `1`)
//! - **timeout_secs**: Request timeout in seconds (default: `60`)
//! - **max_retries**: Maximum retry attempts (default: `3`, use `-1` for infinite)
//! - **compression**: Compression algorithm (default: `none`)
//!   - Supported: none, gzip
//!
//! # Usage Examples
//!
//! ## Basic Usage with JSON Format
//!
//! ```rust,no_run
//! use wp_connectors::http::{HttpSink, HttpSinkConfig};
//! use wp_connector_api::AsyncRecordSink;
//! use wp_model_core::model::{DataRecord, DataField};
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     // Create configuration with defaults
//!     let config = HttpSinkConfig::new(
//!         "https://api.example.com/webhook".to_string(),
//!         None, // Use default POST method
//!         None, // No authentication
//!         None,
//!         None, // No custom headers
//!         None, // Use default JSON format
//!         None, // Use default batch_size=1
//!         None, // Use default timeout=60s
//!         None, // Use default max_retries=3
//!         None, // No compression
//!     );
//!
//!     let mut sink = HttpSink::new(config).await?;
//!     
//!     // Create and send a record
//!     let mut record = DataRecord::default();
//!     record.append(DataField::from_digit("id", 123));
//!     record.append(DataField::from_chars("message", "Hello World"));
//!     
//!     sink.sink_record(&record).await?;
//!     Ok(())
//! }
//! ```
//!
//! ## Using Basic Authentication
//!
//! ```rust,no_run
//! use wp_connectors::http::{HttpSink, HttpSinkConfig};
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let config = HttpSinkConfig::new(
//!         "https://api.example.com/webhook".to_string(),
//!         Some("POST".to_string()),
//!         Some("api_user".to_string()),    // username
//!         Some("api_password".to_string()), // password
//!         None,
//!         None,
//!         None,
//!         None,
//!         None,
//!         None,
//!     );
//!
//!     let sink = HttpSink::new(config).await?;
//!     // Authorization header will be automatically added
//!     Ok(())
//! }
//! ```
//!
//! ## Using Custom Headers and Compression
//!
//! ```rust,no_run
//! use wp_connectors::http::{HttpSink, HttpSinkConfig};
//! use std::collections::HashMap;
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let mut headers = HashMap::new();
//!     headers.insert("X-API-Key".to_string(), "your-api-key".to_string());
//!     headers.insert("X-Custom-Header".to_string(), "custom-value".to_string());
//!     
//!     let config = HttpSinkConfig::new(
//!         "https://api.example.com/webhook".to_string(),
//!         Some("POST".to_string()),
//!         None,
//!         None,
//!         Some(headers),                    // Custom headers
//!         Some("json".to_string()),
//!         None,
//!         None,
//!         None,
//!         Some("gzip".to_string()),         // Enable gzip compression
//!     );
//!
//!     let sink = HttpSink::new(config).await?;
//!     // Requests will include custom headers and gzip compression
//!     Ok(())
//! }
//! ```
//!
//! ## Batch Sending with NDJSON Format
//!
//! ```rust,no_run
//! use wp_connectors::http::{HttpSink, HttpSinkConfig};
//! use wp_connector_api::AsyncRecordSink;
//! use wp_model_core::model::{DataRecord, DataField};
//! use std::sync::Arc;
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let config = HttpSinkConfig::new(
//!         "https://api.example.com/bulk".to_string(),
//!         Some("POST".to_string()),
//!         None,
//!         None,
//!         None,
//!         Some("ndjson".to_string()),       // NDJSON format for batches
//!         Some(100),                        // Send 100 records per request
//!         None,
//!         None,
//!         None,
//!     );
//!
//!     let mut sink = HttpSink::new(config).await?;
//!     
//!     // Create multiple records
//!     let records: Vec<Arc<DataRecord>> = (0..100)
//!         .map(|i| {
//!             let mut record = DataRecord::default();
//!             record.append(DataField::from_digit("id", i));
//!             Arc::new(record)
//!         })
//!         .collect();
//!     
//!     // Send all records in a single HTTP request
//!     sink.sink_records(records).await?;
//!     Ok(())
//! }
//! ```
//!
//! # Data Formats
//!
//! The HTTP Sink supports multiple output formats:
//!
//! - **json**: Standard JSON format, single object per record
//! - **ndjson**: Newline-Delimited JSON, one JSON object per line (ideal for batches)
//! - **csv**: Comma-Separated Values with header row
//! - **kv**: Key-value pairs (field=value format)
//! - **raw**: Raw field values separated by spaces
//! - **proto-text**: Protocol Buffer text format (field: value)
//!
//! # Error Handling and Retry Behavior
//!
//! The HTTP Sink implements intelligent error handling with automatic retries:
//!
//! ## Retryable Errors
//!
//! The following errors trigger automatic retry with exponential backoff:
//! - HTTP 5xx status codes (server errors)
//! - HTTP 408 (Request Timeout)
//! - HTTP 429 (Too Many Requests)
//! - Network errors (connection refused, DNS failure, timeout)
//!
//! ## Non-Retryable Errors
//!
//! The following errors fail immediately without retry:
//! - HTTP 4xx status codes (client errors, except 408 and 429)
//! - Invalid configuration
//! - Data formatting errors
//!
//! ## Retry Strategy
//!
//! - **Exponential Backoff**: Delay = 1000ms × 2^(attempt), capped at 1024 seconds
//! - **Configurable Limit**: Set `max_retries` to control retry attempts
//! - **Infinite Retry**: Set `max_retries = -1` for unlimited retries
//! - **Non-Blocking**: Uses async sleep, doesn't block other operations
//!
//! # Performance Considerations
//!
//! - **Connection Pooling**: reqwest automatically reuses HTTP connections
//! - **Batch Optimization**: Larger batch sizes reduce network overhead
//! - **Compression**: Gzip compression reduces bandwidth usage
//! - **Async I/O**: Non-blocking operations for high concurrency
//!
//! # Thread Safety
//!
//! All components are thread-safe and can be used in concurrent environments:
//! - HTTP client uses connection pooling with internal synchronization
//! - Multiple sink operations can run concurrently
//! - Safe to share across async tasks with proper Arc wrapping

mod config;
mod factory;
mod sink;
mod source;
mod source_factory;

pub use config::HttpSinkConfig;
pub use factory::HttpSinkFactory;
pub use sink::HttpSink;
pub use source::{HttpSource, HttpSourceConfig};
pub use source_factory::HttpSourceFactory;

/// Register the HTTP sink factory with the connector registry
///
/// This function should be called during application initialization to make
/// the HTTP sink available to the wp-connector-api framework.
///
/// # Example
///
/// ```rust,no_run
/// use wp_connectors::http::register_factory;
///
/// register_factory();
/// // Continue with application initialization...
/// ```
///
/// # Note
///
/// Currently, this function serves as a placeholder for future registry integration.
/// The HttpSinkFactory can be used directly by instantiating it and calling its
/// trait methods (validate_spec, build, etc.).
pub fn register_factory() {
    // Placeholder for future factory registration with a central registry.
    // For now, factories are used directly by instantiating HttpSinkFactory.
    // When a central registry is implemented, this function will register
    // the HttpSinkFactory with that registry.
}
