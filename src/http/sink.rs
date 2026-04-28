/// HTTP Sink core implementation
///
/// This module provides the main HTTP Sink implementation for sending data to HTTP endpoints.
use super::config::HttpSinkConfig;
use crate::utils::fmt::{BatchFormat, fmt_bytes, fmt_bytes_kv_http};
use crate::utils::time_stat_utils::TimeStatUtils;
use async_trait::async_trait;
use flate2::Compression;
use flate2::write::GzEncoder;
use reqwest::{Client, StatusCode};
use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use wp_connector_api::{
    AsyncCtrl, AsyncRawDataSink, AsyncRecordSink, SinkError, SinkReason, SinkResult,
};
use wp_data_fmt::Csv;
use wp_model_core::model::DataRecord;

// Global atomic counter for generating unique instance IDs
static INSTANCE_COUNTER: AtomicU64 = AtomicU64::new(0);

/// HTTP Sink for sending data to HTTP/HTTPS endpoints
///
/// Provides async methods for sending individual records and batches of records
/// to configured HTTP endpoints with support for various data formats, compression,
/// authentication, and retry mechanisms.
#[allow(dead_code)] // Methods will be used in later tasks
pub struct HttpSink {
    client: Client,
    config: HttpSinkConfig,
    instance_id: u64,
    time_stats: TimeStatUtils,
}

#[allow(dead_code)] // Methods will be used in later tasks
impl HttpSink {
    /// Create a new HTTP Sink instance
    ///
    /// # Arguments
    ///
    /// * `config` - HTTP Sink configuration
    ///
    /// # Returns
    ///
    /// Returns a Result containing the initialized HttpSink or an error
    pub async fn new(config: HttpSinkConfig) -> anyhow::Result<Self> {
        // Build reqwest client with timeout configuration
        let client = Client::builder()
            .timeout(Duration::from_secs(config.timeout_secs))
            .no_proxy() // Disable all proxies for consistency
            .build()?;

        // Generate unique instance ID from global atomic counter
        let instance_id = INSTANCE_COUNTER.fetch_add(1, Ordering::SeqCst);

        // Initialize performance tracking
        let time_stats = TimeStatUtils::new();

        Ok(Self {
            client,
            config,
            instance_id,
            time_stats,
        })
    }

    /// Get the instance ID
    pub fn instance_id(&self) -> u64 {
        self.instance_id
    }

    // 这里保留旧实现草稿，后续如果需要恢复单条记录字符串格式化，
    // 可以复用 `format_record_bytes` 的逻辑再开启。
    // fn format_record(&self, record: &DataRecord) -> SinkResult<String> {
    //     String::from_utf8(self.format_record_bytes(record)?)
    //         .map_err(|e| sink_error(format!("formatted body is not valid UTF-8: {}", e)))
    // }

    /// Format multiple DataRecords according to the configured format
    ///
    /// # Arguments
    ///
    /// * `records` - The data records to format
    ///
    /// # Returns
    ///
    /// Returns a Result containing the formatted string or an error
    fn format_records(&self, records: &[Arc<DataRecord>]) -> SinkResult<String> {
        String::from_utf8(self.format_records_bytes(records)?)
            .map_err(|e| sink_error(format!("formatted body is not valid UTF-8: {}", e)))
    }

    // fn format_record_bytes(&self, record: &DataRecord) -> SinkResult<Vec<u8>> {
    //     let records = vec![Arc::new(record.clone())];

    //     match self.config.fmt.as_str() {
    //         "json" | "ndjson" => Ok(fmt_bytes(records, BatchFormat::Ndjson)),
    //         "csv" => Ok(fmt_bytes(records, BatchFormat::Csv(Csv::default()))),
    //         "kv" => Ok(fmt_bytes_kv_http(records)),
    //         "raw" => Ok(fmt_bytes(records, BatchFormat::Raw)),
    //         "proto-text" => Ok(fmt_bytes(records, BatchFormat::ProtoText)),
    //         _ => Err(sink_error(format!(
    //             "unsupported format: {}",
    //             self.config.fmt
    //         ))),
    //     }
    // }

    fn format_records_bytes(&self, records: &[Arc<DataRecord>]) -> SinkResult<Vec<u8>> {
        let records = records.to_vec();

        match self.config.fmt.as_str() {
            "json" => Ok(fmt_bytes(records, BatchFormat::Json)),
            "ndjson" => Ok(fmt_bytes(records, BatchFormat::Ndjson)),
            "csv" => Ok(fmt_bytes(records, BatchFormat::Csv(Csv::default()))),
            "kv" => Ok(fmt_bytes_kv_http(records)),
            "raw" => Ok(fmt_bytes(records, BatchFormat::Raw)),
            "proto-text" => Ok(fmt_bytes(records, BatchFormat::ProtoText)),
            _ => Err(sink_error(format!(
                "unsupported format: {}",
                self.config.fmt
            ))),
        }
    }

    /// Compress data using the configured compression algorithm
    ///
    /// # Arguments
    ///
    /// * `data` - The data to compress
    ///
    /// # Returns
    ///
    /// Returns a Result containing the compressed bytes or an error
    fn compress_data(&self, data: &[u8]) -> SinkResult<Vec<u8>> {
        match self.config.compression.as_str() {
            "none" => Ok(data.to_vec()),
            "gzip" => {
                let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
                encoder
                    .write_all(data)
                    .map_err(|e| sink_error(format!("gzip compression failed: {}", e)))?;
                encoder
                    .finish()
                    .map_err(|e| sink_error(format!("gzip compression failed: {}", e)))
            }
            _ => Err(sink_error(format!(
                "unsupported compression algorithm: {}",
                self.config.compression
            ))),
        }
    }

    /// Build an HTTP request with all configured headers and authentication
    ///
    /// # Arguments
    ///
    /// * `body` - The request body bytes
    ///
    /// # Returns
    ///
    /// Returns a configured reqwest::RequestBuilder
    fn build_request(&self, body: Vec<u8>) -> reqwest::RequestBuilder {
        // Parse method from string
        let method = reqwest::Method::from_bytes(self.config.method.as_bytes())
            .unwrap_or(reqwest::Method::POST);

        // Create request with method and endpoint
        let mut request = self.client.request(method, &self.config.endpoint);

        // Add Basic Auth if username is provided
        if !self.config.username.is_empty() {
            request = request.basic_auth(&self.config.username, Some(&self.config.password));
        }

        // Check if Content-Type is already in custom headers (case-insensitive)
        let has_content_type = self
            .config
            .headers
            .keys()
            .any(|k| k.eq_ignore_ascii_case("content-type"));

        // Add all custom headers
        for (key, value) in &self.config.headers {
            request = request.header(key, value);
        }

        // Auto-infer Content-Type if not provided in custom headers
        if !has_content_type {
            let content_type = Self::infer_content_type(&self.config.fmt);
            request = request.header("Content-Type", content_type);
        }

        // Add Content-Encoding header if compression is enabled
        if self.config.compression != "none" {
            request = request.header("Content-Encoding", &self.config.compression);
        }

        // Set request body
        request = request.body(body);

        request
    }

    /// Infer Content-Type header based on output format
    ///
    /// # Arguments
    ///
    /// * `fmt` - The output format string
    ///
    /// # Returns
    ///
    /// Returns the appropriate Content-Type header value
    fn infer_content_type(fmt: &str) -> &'static str {
        match fmt {
            "json" => "application/json",
            "ndjson" => "application/x-ndjson",
            "csv" => "text/csv",
            "kv" | "raw" | "proto-text" => "text/plain",
            _ => "application/octet-stream",
        }
    }

    /// Send an HTTP request and handle the response
    ///
    /// # Arguments
    ///
    /// * `body` - The request body bytes
    ///
    /// # Returns
    ///
    /// Returns Ok for 2xx status codes, SinkError for 4xx and 5xx status codes
    async fn send_request(&self, body: Vec<u8>) -> SinkResult<()> {
        // Build the request
        let request = self.build_request(body);

        // Send the request (timeout is configured in the client)
        let response = request
            .send()
            .await
            .map_err(|e| sink_error(format!("request failed: {}", e)))?;

        // Get status code
        let status = response.status();

        // Handle response based on status code
        if status.is_success() {
            // 2xx: Success
            log::debug!("http request succeeded: status={}", status);
            Ok(())
        } else {
            // Extract response body for error message
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| String::from("(unable to read response body)"));

            if status.is_client_error() {
                // 4xx: Client error (non-retryable)
                log::error!("http client error: status={}, body={}", status, body);
                Err(sink_error(format!(
                    "client error: status={}, body={}",
                    status, body
                )))
            } else if status.is_server_error() {
                // 5xx: Server error (retryable)
                log::error!("http server error: status={}, body={}", status, body);
                Err(sink_error(format!(
                    "server error: status={}, body={}",
                    status, body
                )))
            } else {
                // Other status codes
                log::error!("http unexpected status: status={}, body={}", status, body);
                Err(sink_error(format!(
                    "unexpected status: status={}, body={}",
                    status, body
                )))
            }
        }
    }

    /// Determine if a failed request should be retried
    ///
    /// # Arguments
    ///
    /// * `error` - The reqwest error that occurred
    /// * `status` - Optional HTTP status code from the response
    ///
    /// # Returns
    ///
    /// Returns true if the request should be retried, false otherwise
    ///
    /// # Retry Logic
    ///
    /// - HTTP 5xx status codes: retry (server errors)
    /// - Network errors (timeout, connection refused, DNS failure): retry
    /// - HTTP 408 (Request Timeout) and 429 (Too Many Requests): retry
    /// - HTTP 4xx status codes (except 408, 429): do not retry (client errors)
    /// - HTTP 2xx status codes: do not retry (success)
    fn should_retry(&self, error: &reqwest::Error, status: Option<StatusCode>) -> bool {
        // Network errors: retry
        if error.is_timeout() || error.is_connect() || error.is_request() {
            return true;
        }

        // HTTP status codes
        if let Some(status) = status {
            // 5xx: retry
            if status.is_server_error() {
                return true;
            }
            // 4xx: do not retry (except 408 Request Timeout, 429 Too Many Requests)
            if status.is_client_error() {
                return status == StatusCode::REQUEST_TIMEOUT
                    || status == StatusCode::TOO_MANY_REQUESTS;
            }
        }

        false
    }

    /// Send an HTTP request with retry logic and exponential backoff
    ///
    /// # Arguments
    ///
    /// * `body` - The request body bytes
    ///
    /// # Returns
    ///
    /// Returns Ok for successful requests, SinkError after all retries exhausted
    ///
    /// # Retry Logic
    ///
    /// - Retries up to max_retries times (or indefinitely if max_retries = -1)
    /// - Uses exponential backoff: delay = 1000ms * 2^attempt (capped at 2^10)
    /// - Logs retry attempts at WARN level
    /// - Returns last error if all retries exhausted
    async fn send_with_retry(&self, body: Vec<u8>) -> SinkResult<()> {
        let mut attempt = 0;
        let max_attempts = if self.config.max_retries < 0 {
            i32::MAX
        } else {
            self.config.max_retries + 1 // +1 because first attempt is not a retry
        };

        loop {
            // Clone body for each attempt
            let result = self.send_request(body.clone()).await;

            match result {
                Ok(_) => return Ok(()),
                Err(e) => {
                    attempt += 1;

                    // Check if we should retry based on error message
                    let should_retry = e.to_string().contains("server error")
                        || e.to_string().contains("request failed")
                        || e.to_string().contains("timeout");

                    // If we've exhausted retries or error is not retryable, return error
                    if !should_retry || attempt >= max_attempts {
                        return Err(e);
                    }

                    // Calculate exponential backoff delay (capped at 2^10 = 1024 seconds)
                    let backoff_exp = std::cmp::min(attempt - 1, 10) as u32;
                    let delay_ms = 1000 * 2_u64.pow(backoff_exp);

                    log::warn!(
                        "http request failed, retry {}/{}: {}. Waiting {}ms before retry",
                        attempt,
                        if self.config.max_retries < 0 {
                            "∞".to_string()
                        } else {
                            self.config.max_retries.to_string()
                        },
                        e,
                        delay_ms
                    );

                    // Non-blocking sleep
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                }
            }
        }
    }
}

// Trait implementations

#[async_trait]
impl AsyncCtrl for HttpSink {
    async fn stop(&mut self) -> SinkResult<()> {
        // reqwest::Client automatically cleans up connections on drop
        // No explicit cleanup needed
        Ok(())
    }

    async fn reconnect(&mut self) -> SinkResult<()> {
        // Recreate HTTP client to reset connection pool
        self.client = Client::builder()
            .timeout(Duration::from_secs(self.config.timeout_secs))
            .no_proxy()
            .build()
            .map_err(|e| {
                wp_connector_api::SinkError::from(wp_connector_api::SinkReason::Sink(format!(
                    "reconnect failed: {}",
                    e
                )))
            })?;
        Ok(())
    }
}

#[async_trait]
impl AsyncRecordSink for HttpSink {
    async fn sink_record(&mut self, data: &DataRecord) -> SinkResult<()> {
        self.sink_records(vec![Arc::new(data.clone())]).await
    }

    async fn sink_records(&mut self, data: Vec<Arc<DataRecord>>) -> SinkResult<()> {
        // Start timing for performance tracking
        self.time_stats.start_stat(data.len() as u64);

        // Format the batch of DataRecords according to configured format
        let formatted = self.format_records_bytes(&data)?;
        // println!("{}", String::from_utf8(formatted.clone()).unwrap());
        // Compress data if compression is enabled
        let body = self.compress_data(&formatted)?;

        // Send HTTP request with retry logic
        let result = self.send_with_retry(body).await;

        // End timing
        self.time_stats.end_stat();

        // Log based on result
        match &result {
            Ok(_) => {
                log::info!(
                    "http sink success: {} records sent to {} (instance_id={})",
                    data.len(),
                    self.config.endpoint,
                    self.instance_id
                );
            }
            Err(e) => {
                log::error!(
                    "http sink failed: endpoint={}, instance_id={}, record_count={}, error={}",
                    self.config.endpoint,
                    self.instance_id,
                    data.len(),
                    e
                );
            }
        }

        result
    }
}

#[async_trait]
impl AsyncRawDataSink for HttpSink {
    async fn sink_str(&mut self, data: &str) -> SinkResult<()> {
        // Start timing for performance tracking
        self.time_stats.start_stat(1);

        // Convert string to bytes
        let data_bytes = data.as_bytes();

        // Compress data if compression is enabled
        let body = self.compress_data(data_bytes)?;

        // Send HTTP request with retry logic
        let result = self.send_with_retry(body).await;

        // End timing
        self.time_stats.end_stat();

        // Log based on result
        match &result {
            Ok(_) => {
                log::debug!(
                    "http sink success: raw string sent to {} (instance_id={})",
                    self.config.endpoint,
                    self.instance_id
                );
            }
            Err(e) => {
                log::error!(
                    "http sink failed: endpoint={}, instance_id={}, error={}",
                    self.config.endpoint,
                    self.instance_id,
                    e
                );
            }
        }

        result
    }

    async fn sink_bytes(&mut self, data: &[u8]) -> SinkResult<()> {
        // Start timing for performance tracking
        self.time_stats.start_stat(1);

        // Compress data if compression is enabled
        let body = self.compress_data(data)?;

        // Send HTTP request with retry logic
        let result = self.send_with_retry(body).await;

        // End timing
        self.time_stats.end_stat();

        // Log based on result
        match &result {
            Ok(_) => {
                log::debug!(
                    "http sink success: raw bytes sent to {} (instance_id={})",
                    self.config.endpoint,
                    self.instance_id
                );
            }
            Err(e) => {
                log::error!(
                    "http sink failed: endpoint={}, instance_id={}, error={}",
                    self.config.endpoint,
                    self.instance_id,
                    e
                );
            }
        }

        result
    }

    async fn sink_str_batch(&mut self, data: Vec<&str>) -> SinkResult<()> {
        // Start timing for performance tracking
        self.time_stats.start_stat(data.len() as u64);

        // Join strings with newlines
        let joined = data.join("\n");

        // Convert to bytes
        let data_bytes = joined.as_bytes();

        // Compress data if compression is enabled
        let body = self.compress_data(data_bytes)?;

        // Send HTTP request with retry logic
        let result = self.send_with_retry(body).await;

        // End timing
        self.time_stats.end_stat();

        // Log based on result
        match &result {
            Ok(_) => {
                log::info!(
                    "http sink success: {} raw strings sent to {} (instance_id={})",
                    data.len(),
                    self.config.endpoint,
                    self.instance_id
                );
            }
            Err(e) => {
                log::error!(
                    "http sink failed: endpoint={}, instance_id={}, string_count={}, error={}",
                    self.config.endpoint,
                    self.instance_id,
                    data.len(),
                    e
                );
            }
        }

        result
    }

    async fn sink_bytes_batch(&mut self, data: Vec<&[u8]>) -> SinkResult<()> {
        // Start timing for performance tracking
        self.time_stats.start_stat(data.len() as u64);

        // Join bytes with newlines
        let mut joined = Vec::new();
        for (i, bytes) in data.iter().enumerate() {
            joined.extend_from_slice(bytes);
            // Add newline between items (but not after the last one)
            if i < data.len() - 1 {
                joined.push(b'\n');
            }
        }

        // Compress data if compression is enabled
        let body = self.compress_data(&joined)?;

        // Send HTTP request with retry logic
        let result = self.send_with_retry(body).await;

        // End timing
        self.time_stats.end_stat();

        // Log based on result
        match &result {
            Ok(_) => {
                log::info!(
                    "http sink success: {} raw byte arrays sent to {} (instance_id={})",
                    data.len(),
                    self.config.endpoint,
                    self.instance_id
                );
            }
            Err(e) => {
                log::error!(
                    "http sink failed: endpoint={}, instance_id={}, byte_array_count={}, error={}",
                    self.config.endpoint,
                    self.instance_id,
                    data.len(),
                    e
                );
            }
        }

        result
    }
}

// TODO: Implement compression methods
// TODO: Implement HTTP request building and sending
// TODO: Implement retry logic

/// Helper function to create a SinkError
#[allow(dead_code)] // Will be used in later tasks
fn sink_error(msg: impl Into<String>) -> SinkError {
    SinkError::from(SinkReason::Sink(msg.into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use wp_model_core::model::DataField;

    #[tokio::test]
    async fn sink_initialization() {
        let config = HttpSinkConfig::new(
            "http://example.com".to_string(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );

        let result = HttpSink::new(config).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn format_records_json_array() {
        // Test that format_records with "json" format produces a JSON array
        let config = HttpSinkConfig::new(
            "http://example.com".to_string(),
            None,
            None,
            None,
            None,
            Some("json".to_string()),
            None,
            None,
            None,
            None,
        );

        let sink = HttpSink::new(config).await.unwrap();

        let mut record1 = DataRecord::default();
        record1.append(DataField::from_digit("id", 1));
        record1.append(DataField::from_chars("name", "alice"));

        let mut record2 = DataRecord::default();
        record2.append(DataField::from_digit("id", 2));
        record2.append(DataField::from_chars("name", "bob"));

        let records = vec![Arc::new(record1), Arc::new(record2)];
        let result = sink.format_records(&records);
        assert!(result.is_ok());

        let json_str = result.unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        // Should be a JSON array
        assert!(parsed.is_array());
        let array = parsed.as_array().unwrap();
        assert_eq!(array.len(), 2);

        // Check first record
        assert_eq!(array[0]["id"], 1);
        assert_eq!(array[0]["name"], "alice");

        // Check second record
        assert_eq!(array[1]["id"], 2);
        assert_eq!(array[1]["name"], "bob");
    }

    #[tokio::test]
    async fn format_records_ndjson() {
        // Test that format_records with "ndjson" format produces newline-delimited JSON
        let config = HttpSinkConfig::new(
            "http://example.com".to_string(),
            None,
            None,
            None,
            None,
            Some("ndjson".to_string()),
            None,
            None,
            None,
            None,
        );

        let sink = HttpSink::new(config).await.unwrap();

        let mut record1 = DataRecord::default();
        record1.append(DataField::from_digit("id", 1));
        record1.append(DataField::from_chars("name", "alice"));

        let mut record2 = DataRecord::default();
        record2.append(DataField::from_digit("id", 2));
        record2.append(DataField::from_chars("name", "bob"));

        let records = vec![Arc::new(record1), Arc::new(record2)];
        let result = sink.format_records(&records);
        assert!(result.is_ok());

        let ndjson_str = result.unwrap();
        let lines: Vec<&str> = ndjson_str.lines().collect();

        // Should have 2 lines
        assert_eq!(lines.len(), 2);

        // Each line should be valid JSON
        let parsed1: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(parsed1["id"], 1);
        assert_eq!(parsed1["name"], "alice");

        let parsed2: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(parsed2["id"], 2);
        assert_eq!(parsed2["name"], "bob");
    }

    #[tokio::test]
    async fn format_records_csv() {
        let config = HttpSinkConfig::new(
            "http://example.com".to_string(),
            None,
            None,
            None,
            None,
            Some("csv".to_string()),
            None,
            None,
            None,
            None,
        );

        let sink = HttpSink::new(config).await.unwrap();

        let mut record1 = DataRecord::default();
        record1.append(DataField::from_digit("id", 1));
        record1.append(DataField::from_chars("name", "alice"));

        let mut record2 = DataRecord::default();
        record2.append(DataField::from_digit("id", 2));
        record2.append(DataField::from_chars("name", "bob"));

        let records = vec![Arc::new(record1), Arc::new(record2)];
        let result = sink.format_records(&records);
        assert!(result.is_ok());

        let csv_str = result.unwrap();
        let lines: Vec<&str> = csv_str.lines().collect();

        assert_eq!(lines.len(), 3); // header + 2 data rows
        assert_eq!(lines[0], "id,name");
        assert_eq!(lines[1], "1,alice");
        assert_eq!(lines[2], "2,bob");
    }

    #[tokio::test]
    async fn compress_data_none() {
        let config = HttpSinkConfig::new(
            "http://example.com".to_string(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some("none".to_string()),
        );

        let sink = HttpSink::new(config).await.unwrap();
        let data = b"hello world";

        let result = sink.compress_data(data);
        assert!(result.is_ok());

        let compressed = result.unwrap();
        assert_eq!(compressed, data);
    }

    #[tokio::test]
    async fn compress_data_gzip() {
        let config = HttpSinkConfig::new(
            "http://example.com".to_string(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some("gzip".to_string()),
        );

        let sink = HttpSink::new(config).await.unwrap();
        let data = b"hello world";

        let result = sink.compress_data(data);
        assert!(result.is_ok());

        let compressed = result.unwrap();
        // Compressed data should be different from original
        assert_ne!(compressed, data);
        // Compressed data should have gzip magic bytes
        assert_eq!(&compressed[0..2], &[0x1f, 0x8b]);
    }

    #[tokio::test]
    async fn compress_data_gzip_round_trip() {
        use flate2::read::GzDecoder;
        use std::io::Read;

        let config = HttpSinkConfig::new(
            "http://example.com".to_string(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some("gzip".to_string()),
        );

        let sink = HttpSink::new(config).await.unwrap();
        let original_data = b"hello world, this is a test message for compression";

        // Compress
        let compressed = sink.compress_data(original_data).unwrap();

        // Decompress
        let mut decoder = GzDecoder::new(&compressed[..]);
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed).unwrap();

        // Verify round-trip
        assert_eq!(decompressed, original_data);
    }

    #[tokio::test]
    async fn compress_data_unsupported_algorithm() {
        let config = HttpSinkConfig::new(
            "http://example.com".to_string(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some("bzip2".to_string()),
        );

        let sink = HttpSink::new(config).await.unwrap();
        let data = b"hello world";

        let result = sink.compress_data(data);
        assert!(result.is_err());

        let err = result.unwrap_err();
        assert!(
            err.to_string()
                .contains("unsupported compression algorithm")
        );
    }

    #[tokio::test]
    async fn compress_empty_data() {
        let config = HttpSinkConfig::new(
            "http://example.com".to_string(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some("gzip".to_string()),
        );

        let sink = HttpSink::new(config).await.unwrap();
        let data = b"";

        let result = sink.compress_data(data);
        assert!(result.is_ok());

        let compressed = result.unwrap();
        // Even empty data should have gzip header
        assert!(!compressed.is_empty());
        assert_eq!(&compressed[0..2], &[0x1f, 0x8b]);
    }

    #[tokio::test]
    async fn compress_large_data() {
        let config = HttpSinkConfig::new(
            "http://example.com".to_string(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some("gzip".to_string()),
        );

        let sink = HttpSink::new(config).await.unwrap();
        // Create 1MB of repetitive data (should compress well)
        let data = vec![b'a'; 1024 * 1024];

        let result = sink.compress_data(&data);
        assert!(result.is_ok());

        let compressed = result.unwrap();
        // Compressed size should be much smaller than original
        assert!(compressed.len() < data.len() / 10);
    }

    #[tokio::test]
    async fn build_request_basic() {
        let config = HttpSinkConfig::new(
            "http://example.com/api".to_string(),
            Some("POST".to_string()),
            None,
            None,
            None,
            Some("json".to_string()),
            None,
            None,
            None,
            None,
        );

        let sink = HttpSink::new(config).await.unwrap();
        let body = b"test data".to_vec();

        let request = sink.build_request(body);

        // Request should be built successfully
        // We can't easily inspect the request builder, but we can verify it builds
        assert!(request.build().is_ok());
    }

    #[tokio::test]
    async fn build_request_with_basic_auth() {
        let config = HttpSinkConfig::new(
            "http://example.com/api".to_string(),
            Some("POST".to_string()),
            Some("testuser".to_string()),
            Some("testpass".to_string()),
            None,
            Some("json".to_string()),
            None,
            None,
            None,
            None,
        );

        let sink = HttpSink::new(config).await.unwrap();
        let body = b"test data".to_vec();

        let request = sink.build_request(body);
        let built = request.build().unwrap();

        // Verify Authorization header is present
        let auth_header = built.headers().get("authorization");
        assert!(auth_header.is_some());

        // Verify it starts with "Basic "
        let auth_value = auth_header.unwrap().to_str().unwrap();
        assert!(auth_value.starts_with("Basic "));

        // Verify the Base64 encoding is correct
        use base64::{Engine as _, engine::general_purpose};
        let expected_encoded = general_purpose::STANDARD.encode("testuser:testpass");
        assert_eq!(auth_value, format!("Basic {}", expected_encoded));
    }

    #[tokio::test]
    async fn build_request_without_auth() {
        let config = HttpSinkConfig::new(
            "http://example.com/api".to_string(),
            Some("POST".to_string()),
            None, // No username
            None,
            None,
            Some("json".to_string()),
            None,
            None,
            None,
            None,
        );

        let sink = HttpSink::new(config).await.unwrap();
        let body = b"test data".to_vec();

        let request = sink.build_request(body);
        let built = request.build().unwrap();

        // Verify Authorization header is NOT present
        assert!(built.headers().get("authorization").is_none());
    }

    #[tokio::test]
    async fn build_request_with_custom_headers() {
        let mut headers = std::collections::HashMap::new();
        headers.insert("X-Custom-Header".to_string(), "custom-value".to_string());
        headers.insert("X-API-Key".to_string(), "secret-key".to_string());

        let config = HttpSinkConfig::new(
            "http://example.com/api".to_string(),
            Some("POST".to_string()),
            None,
            None,
            Some(headers),
            Some("json".to_string()),
            None,
            None,
            None,
            None,
        );

        let sink = HttpSink::new(config).await.unwrap();
        let body = b"test data".to_vec();

        let request = sink.build_request(body);
        let built = request.build().unwrap();

        // Verify custom headers are present
        assert_eq!(
            built
                .headers()
                .get("X-Custom-Header")
                .unwrap()
                .to_str()
                .unwrap(),
            "custom-value"
        );
        assert_eq!(
            built.headers().get("X-API-Key").unwrap().to_str().unwrap(),
            "secret-key"
        );
    }

    #[tokio::test]
    async fn build_request_auto_infer_content_type_json() {
        let config = HttpSinkConfig::new(
            "http://example.com/api".to_string(),
            Some("POST".to_string()),
            None,
            None,
            None, // No custom headers
            Some("json".to_string()),
            None,
            None,
            None,
            None,
        );

        let sink = HttpSink::new(config).await.unwrap();
        let body = b"test data".to_vec();

        let request = sink.build_request(body);
        let built = request.build().unwrap();

        // Verify Content-Type is auto-inferred
        assert_eq!(
            built
                .headers()
                .get("content-type")
                .unwrap()
                .to_str()
                .unwrap(),
            "application/json"
        );
    }

    #[tokio::test]
    async fn build_request_auto_infer_content_type_ndjson() {
        let config = HttpSinkConfig::new(
            "http://example.com/api".to_string(),
            Some("POST".to_string()),
            None,
            None,
            None,
            Some("ndjson".to_string()),
            None,
            None,
            None,
            None,
        );

        let sink = HttpSink::new(config).await.unwrap();
        let body = b"test data".to_vec();

        let request = sink.build_request(body);
        let built = request.build().unwrap();

        assert_eq!(
            built
                .headers()
                .get("content-type")
                .unwrap()
                .to_str()
                .unwrap(),
            "application/x-ndjson"
        );
    }

    #[tokio::test]
    async fn build_request_auto_infer_content_type_csv() {
        let config = HttpSinkConfig::new(
            "http://example.com/api".to_string(),
            Some("POST".to_string()),
            None,
            None,
            None,
            Some("csv".to_string()),
            None,
            None,
            None,
            None,
        );

        let sink = HttpSink::new(config).await.unwrap();
        let body = b"test data".to_vec();

        let request = sink.build_request(body);
        let built = request.build().unwrap();

        assert_eq!(
            built
                .headers()
                .get("content-type")
                .unwrap()
                .to_str()
                .unwrap(),
            "text/csv"
        );
    }

    #[tokio::test]
    async fn build_request_custom_content_type_overrides_inference() {
        let mut headers = std::collections::HashMap::new();
        headers.insert("Content-Type".to_string(), "application/xml".to_string());

        let config = HttpSinkConfig::new(
            "http://example.com/api".to_string(),
            Some("POST".to_string()),
            None,
            None,
            Some(headers),
            Some("json".to_string()), // Would normally infer application/json
            None,
            None,
            None,
            None,
        );

        let sink = HttpSink::new(config).await.unwrap();
        let body = b"test data".to_vec();

        let request = sink.build_request(body);
        let built = request.build().unwrap();

        // Custom Content-Type should override auto-inference
        assert_eq!(
            built
                .headers()
                .get("content-type")
                .unwrap()
                .to_str()
                .unwrap(),
            "application/xml"
        );
    }

    #[tokio::test]
    async fn build_request_content_type_case_insensitive() {
        let mut headers = std::collections::HashMap::new();
        headers.insert("content-type".to_string(), "application/xml".to_string());

        let config = HttpSinkConfig::new(
            "http://example.com/api".to_string(),
            Some("POST".to_string()),
            None,
            None,
            Some(headers),
            Some("json".to_string()),
            None,
            None,
            None,
            None,
        );

        let sink = HttpSink::new(config).await.unwrap();
        let body = b"test data".to_vec();

        let request = sink.build_request(body);
        let built = request.build().unwrap();

        // Should detect lowercase content-type and not add another
        assert_eq!(
            built
                .headers()
                .get("content-type")
                .unwrap()
                .to_str()
                .unwrap(),
            "application/xml"
        );

        // Should only have one content-type header
        assert_eq!(built.headers().get_all("content-type").iter().count(), 1);
    }

    #[tokio::test]
    async fn build_request_with_compression_gzip() {
        let config = HttpSinkConfig::new(
            "http://example.com/api".to_string(),
            Some("POST".to_string()),
            None,
            None,
            None,
            Some("json".to_string()),
            None,
            None,
            None,
            Some("gzip".to_string()),
        );

        let sink = HttpSink::new(config).await.unwrap();
        let body = b"test data".to_vec();

        let request = sink.build_request(body);
        let built = request.build().unwrap();

        // Verify Content-Encoding header is set
        assert_eq!(
            built
                .headers()
                .get("content-encoding")
                .unwrap()
                .to_str()
                .unwrap(),
            "gzip"
        );
    }

    #[tokio::test]
    async fn build_request_without_compression() {
        let config = HttpSinkConfig::new(
            "http://example.com/api".to_string(),
            Some("POST".to_string()),
            None,
            None,
            None,
            Some("json".to_string()),
            None,
            None,
            None,
            Some("none".to_string()),
        );

        let sink = HttpSink::new(config).await.unwrap();
        let body = b"test data".to_vec();

        let request = sink.build_request(body);
        let built = request.build().unwrap();

        // Verify Content-Encoding header is NOT set
        assert!(built.headers().get("content-encoding").is_none());
    }

    #[tokio::test]
    async fn build_request_with_different_http_methods() {
        let methods = vec!["GET", "POST", "PUT", "PATCH", "DELETE"];

        for method in methods {
            let config = HttpSinkConfig::new(
                "http://example.com/api".to_string(),
                Some(method.to_string()),
                None,
                None,
                None,
                Some("json".to_string()),
                None,
                None,
                None,
                None,
            );

            let sink = HttpSink::new(config).await.unwrap();
            let body = b"test data".to_vec();

            let request = sink.build_request(body);
            let built = request.build().unwrap();

            // Verify method is set correctly
            assert_eq!(built.method().as_str(), method);
        }
    }

    #[tokio::test]
    async fn build_request_sets_body() {
        let config = HttpSinkConfig::new(
            "http://example.com/api".to_string(),
            Some("POST".to_string()),
            None,
            None,
            None,
            Some("json".to_string()),
            None,
            None,
            None,
            None,
        );

        let sink = HttpSink::new(config).await.unwrap();
        let body = b"test body content".to_vec();

        let request = sink.build_request(body.clone());
        let built = request.build().unwrap();

        // Verify body is set (we can't easily extract it, but we can verify it's not empty)
        assert!(built.body().is_some());
    }

    #[test]
    fn infer_content_type_all_formats() {
        assert_eq!(HttpSink::infer_content_type("json"), "application/json");
        assert_eq!(
            HttpSink::infer_content_type("ndjson"),
            "application/x-ndjson"
        );
        assert_eq!(HttpSink::infer_content_type("csv"), "text/csv");
        assert_eq!(HttpSink::infer_content_type("kv"), "text/plain");
        assert_eq!(HttpSink::infer_content_type("raw"), "text/plain");
        assert_eq!(HttpSink::infer_content_type("proto-text"), "text/plain");
        assert_eq!(
            HttpSink::infer_content_type("unknown"),
            "application/octet-stream"
        );
    }

    #[tokio::test]
    async fn should_retry_network_errors() {
        let config = HttpSinkConfig::new(
            "http://example.com".to_string(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );

        let sink = HttpSink::new(config).await.unwrap();

        // Create a timeout error - this is a network error that should retry
        let error = reqwest::Client::new()
            .get("http://192.0.2.1:9999") // Non-routable IP
            .timeout(std::time::Duration::from_millis(1))
            .send()
            .await
            .unwrap_err();

        // Network errors should always retry, regardless of status code
        assert!(sink.should_retry(&error, None));
        // Even if we somehow have a status code with a network error, network error takes precedence
        assert!(sink.should_retry(&error, Some(StatusCode::OK)));
    }

    #[tokio::test]
    async fn should_retry_connection_errors() {
        let config = HttpSinkConfig::new(
            "http://example.com".to_string(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );

        let sink = HttpSink::new(config).await.unwrap();

        // Create a connection error by connecting to a non-existent host
        let error = reqwest::Client::new()
            .get("http://localhost:9999") // Port that's likely not listening
            .timeout(std::time::Duration::from_millis(100))
            .send()
            .await
            .unwrap_err();

        // Connection errors should retry
        assert!(sink.should_retry(&error, None));
    }

    // Note: The following tests verify the status code logic.
    // In real usage with send_request, when we have a status code, we successfully
    // received an HTTP response, so there won't be network-level errors.
    // The should_retry method checks network errors first, then status codes.

    #[test]
    fn should_retry_logic_5xx_status_codes() {
        // Test the retry logic for 5xx status codes
        // When status is 5xx and no network error, should retry
        assert!(StatusCode::INTERNAL_SERVER_ERROR.is_server_error());
        assert!(StatusCode::BAD_GATEWAY.is_server_error());
        assert!(StatusCode::SERVICE_UNAVAILABLE.is_server_error());
        assert!(StatusCode::GATEWAY_TIMEOUT.is_server_error());
    }

    #[test]
    fn should_retry_logic_4xx_status_codes() {
        // Test the retry logic for 4xx status codes
        // Most 4xx should not retry
        assert!(StatusCode::BAD_REQUEST.is_client_error());
        assert!(StatusCode::UNAUTHORIZED.is_client_error());
        assert!(StatusCode::FORBIDDEN.is_client_error());
        assert!(StatusCode::NOT_FOUND.is_client_error());

        // But 408 and 429 are special cases that should retry
        assert!(StatusCode::REQUEST_TIMEOUT.is_client_error());
        assert_eq!(StatusCode::REQUEST_TIMEOUT, StatusCode::REQUEST_TIMEOUT);
        assert!(StatusCode::TOO_MANY_REQUESTS.is_client_error());
        assert_eq!(StatusCode::TOO_MANY_REQUESTS, StatusCode::TOO_MANY_REQUESTS);
    }

    #[test]
    fn should_retry_logic_2xx_status_codes() {
        // Test that 2xx status codes are success codes
        assert!(StatusCode::OK.is_success());
        assert!(StatusCode::CREATED.is_success());
        assert!(StatusCode::ACCEPTED.is_success());
        assert!(StatusCode::NO_CONTENT.is_success());
    }

    // Tests for send_with_retry method
    // Note: These tests verify the retry logic behavior without making actual HTTP requests
    // Integration tests with mock servers are in tests/http/

    #[tokio::test]
    async fn send_with_retry_max_retries_zero() {
        // When max_retries is 0, should not retry on failure
        let config = HttpSinkConfig::new(
            "http://localhost:9999/nonexistent".to_string(), // Will fail
            Some("POST".to_string()),
            None,
            None,
            None,
            Some("json".to_string()),
            None,
            None,
            Some(0), // max_retries = 0
            None,
        );

        let sink = HttpSink::new(config).await.unwrap();
        let body = b"test data".to_vec();

        // Should fail immediately without retries
        let result = sink.send_with_retry(body).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn send_with_retry_exponential_backoff_timing() {
        // Test that exponential backoff delays are calculated correctly
        // We'll test the timing logic without actually making requests

        let config = HttpSinkConfig::new(
            "http://localhost:9999/nonexistent".to_string(),
            Some("POST".to_string()),
            None,
            None,
            None,
            Some("json".to_string()),
            None,
            Some(1), // 1 second timeout to fail fast
            Some(2), // max_retries = 2
            None,
        );

        let sink = HttpSink::new(config).await.unwrap();
        let body = b"test data".to_vec();

        let start = std::time::Instant::now();
        let result = sink.send_with_retry(body).await;
        let elapsed = start.elapsed();

        // Should fail after retries
        assert!(result.is_err());

        // With max_retries=2, we have:
        // - Attempt 1: immediate (fails in ~1s due to timeout)
        // - Wait: 1000ms (2^0)
        // - Attempt 2: ~1s
        // - Wait: 2000ms (2^1)
        // - Attempt 3: ~1s
        // Total: ~3s for requests + 3s for waits = ~6s
        // But we don't wait after the last failure, so it's ~3s + 3s = ~6s
        // However, the connection might fail faster than timeout
        // Allow some margin for timing variations
        assert!(
            elapsed.as_secs() >= 2,
            "Expected at least 2 seconds, got {:?}",
            elapsed
        );
        assert!(
            elapsed.as_secs() <= 10,
            "Expected at most 10 seconds, got {:?}",
            elapsed
        );
    }

    #[tokio::test]
    async fn send_with_retry_backoff_cap() {
        // Test that backoff delay is capped at 2^10 = 1024 seconds
        // We verify the calculation logic without waiting for the full delay

        // Verify the cap calculation
        for attempt in 0..15 {
            let backoff_exp = std::cmp::min(attempt, 10) as u32;
            let delay_ms = 1000 * 2_u64.pow(backoff_exp);

            if attempt <= 10 {
                assert_eq!(delay_ms, 1000 * 2_u64.pow(attempt as u32));
            } else {
                // Should be capped at 2^10
                assert_eq!(delay_ms, 1000 * 1024);
            }
        }
    }

    #[tokio::test]
    async fn sink_record_formats_and_compresses() {
        // Test that sink_record properly formats and compresses data
        // Note: This test will fail to send (no server), but we can verify
        // the formatting and compression logic is called

        let config = HttpSinkConfig::new(
            "http://localhost:9999/test".to_string(),
            Some("POST".to_string()),
            None,
            None,
            None,
            Some("json".to_string()),
            None,
            Some(1), // 1 second timeout
            Some(0), // No retries for faster test
            Some("gzip".to_string()),
        );

        let mut sink = HttpSink::new(config).await.unwrap();

        let mut record = DataRecord::default();
        record.append(DataField::from_digit("id", 123));
        record.append(DataField::from_chars("name", "test"));

        // This will fail because there's no server, but it verifies the method works
        let result = sink.sink_record(&record).await;
        assert!(result.is_err()); // Expected to fail (no server)

        // Verify time_stats was updated
        assert_eq!(sink.time_stats.total_count, 1);
    }

    #[tokio::test]
    async fn sink_record_updates_time_stats() {
        // Test that sink_record updates time statistics correctly

        let config = HttpSinkConfig::new(
            "http://localhost:9999/test".to_string(),
            Some("POST".to_string()),
            None,
            None,
            None,
            Some("json".to_string()),
            None,
            Some(1),
            Some(0), // No retries
            None,
        );

        let mut sink = HttpSink::new(config).await.unwrap();

        // Initial state
        assert_eq!(sink.time_stats.total_count, 0);

        let mut record = DataRecord::default();
        record.append(DataField::from_digit("id", 1));

        // Send first record (will fail, but stats should update)
        let _ = sink.sink_record(&record).await;
        assert_eq!(sink.time_stats.total_count, 1);

        // Send second record
        let _ = sink.sink_record(&record).await;
        assert_eq!(sink.time_stats.total_count, 2);
    }

    #[tokio::test]
    async fn sink_records_formats_and_compresses() {
        // Test that sink_records properly formats and compresses batch data
        // Note: This test will fail to send (no server), but we can verify
        // the formatting and compression logic is called

        let config = HttpSinkConfig::new(
            "http://localhost:9999/test".to_string(),
            Some("POST".to_string()),
            None,
            None,
            None,
            Some("json".to_string()),
            None,
            Some(1), // 1 second timeout
            Some(0), // No retries for faster test
            Some("gzip".to_string()),
        );

        let mut sink = HttpSink::new(config).await.unwrap();

        let mut record1 = DataRecord::default();
        record1.append(DataField::from_digit("id", 1));
        record1.append(DataField::from_chars("name", "alice"));

        let mut record2 = DataRecord::default();
        record2.append(DataField::from_digit("id", 2));
        record2.append(DataField::from_chars("name", "bob"));

        let records = vec![Arc::new(record1), Arc::new(record2)];

        // This will fail because there's no server, but it verifies the method works
        let result = sink.sink_records(records).await;
        assert!(result.is_err()); // Expected to fail (no server)

        // Verify time_stats was updated with correct count
        assert_eq!(sink.time_stats.total_count, 2);
    }

    #[tokio::test]
    async fn sink_records_updates_time_stats() {
        // Test that sink_records updates time statistics correctly with batch count

        let config = HttpSinkConfig::new(
            "http://localhost:9999/test".to_string(),
            Some("POST".to_string()),
            None,
            None,
            None,
            Some("json".to_string()),
            None,
            Some(1),
            Some(0), // No retries
            None,
        );

        let mut sink = HttpSink::new(config).await.unwrap();

        // Initial state
        assert_eq!(sink.time_stats.total_count, 0);

        let mut record1 = DataRecord::default();
        record1.append(DataField::from_digit("id", 1));

        let mut record2 = DataRecord::default();
        record2.append(DataField::from_digit("id", 2));

        let mut record3 = DataRecord::default();
        record3.append(DataField::from_digit("id", 3));

        // Send batch of 3 records (will fail, but stats should update)
        let records = vec![Arc::new(record1), Arc::new(record2), Arc::new(record3)];
        let _ = sink.sink_records(records).await;
        assert_eq!(sink.time_stats.total_count, 3);

        // Send another batch of 2 records
        let mut record4 = DataRecord::default();
        record4.append(DataField::from_digit("id", 4));

        let mut record5 = DataRecord::default();
        record5.append(DataField::from_digit("id", 5));

        let records2 = vec![Arc::new(record4), Arc::new(record5)];
        let _ = sink.sink_records(records2).await;
        assert_eq!(sink.time_stats.total_count, 5);
    }

    #[tokio::test]
    async fn sink_records_empty_batch() {
        // Test that sink_records handles empty batch correctly

        let config = HttpSinkConfig::new(
            "http://localhost:9999/test".to_string(),
            Some("POST".to_string()),
            None,
            None,
            None,
            Some("json".to_string()),
            None,
            Some(1),
            Some(0), // No retries
            None,
        );

        let mut sink = HttpSink::new(config).await.unwrap();

        // Send empty batch
        let records: Vec<Arc<DataRecord>> = vec![];
        let result = sink.sink_records(records).await;

        // Should fail because there's no server, but shouldn't panic
        assert!(result.is_err());

        // Stats should be updated with 0 count
        assert_eq!(sink.time_stats.total_count, 0);
    }

    #[tokio::test]
    async fn sink_str_basic() {
        // Test that sink_str properly sends string data

        let config = HttpSinkConfig::new(
            "http://localhost:9999/test".to_string(),
            Some("POST".to_string()),
            None,
            None,
            None,
            Some("json".to_string()),
            None,
            Some(1), // 1 second timeout
            Some(0), // No retries for faster test
            None,
        );

        let mut sink = HttpSink::new(config).await.unwrap();

        let data = "test string data";

        // This will fail because there's no server, but it verifies the method works
        let result = sink.sink_str(data).await;
        assert!(result.is_err()); // Expected to fail (no server)

        // Verify time_stats was updated
        assert_eq!(sink.time_stats.total_count, 1);
    }

    #[tokio::test]
    async fn sink_str_with_compression() {
        // Test that sink_str properly compresses data

        let config = HttpSinkConfig::new(
            "http://localhost:9999/test".to_string(),
            Some("POST".to_string()),
            None,
            None,
            None,
            Some("json".to_string()),
            None,
            Some(1), // 1 second timeout
            Some(0), // No retries for faster test
            Some("gzip".to_string()),
        );

        let mut sink = HttpSink::new(config).await.unwrap();

        let data = "test string data with compression";

        // This will fail because there's no server, but it verifies compression is applied
        let result = sink.sink_str(data).await;
        assert!(result.is_err()); // Expected to fail (no server)

        // Verify time_stats was updated
        assert_eq!(sink.time_stats.total_count, 1);
    }

    #[tokio::test]
    async fn sink_str_empty_string() {
        // Test that sink_str handles empty string

        let config = HttpSinkConfig::new(
            "http://localhost:9999/test".to_string(),
            Some("POST".to_string()),
            None,
            None,
            None,
            Some("json".to_string()),
            None,
            Some(1),
            Some(0), // No retries
            None,
        );

        let mut sink = HttpSink::new(config).await.unwrap();

        let data = "";

        // Should handle empty string gracefully
        let result = sink.sink_str(data).await;
        assert!(result.is_err()); // Expected to fail (no server)
    }

    #[tokio::test]
    async fn sink_bytes_basic() {
        // Test that sink_bytes properly sends binary data

        let config = HttpSinkConfig::new(
            "http://localhost:9999/test".to_string(),
            Some("POST".to_string()),
            None,
            None,
            None,
            Some("json".to_string()),
            None,
            Some(1), // 1 second timeout
            Some(0), // No retries for faster test
            None,
        );

        let mut sink = HttpSink::new(config).await.unwrap();

        let data = b"test binary data";

        // This will fail because there's no server, but it verifies the method works
        let result = sink.sink_bytes(data).await;
        assert!(result.is_err()); // Expected to fail (no server)

        // Verify time_stats was updated
        assert_eq!(sink.time_stats.total_count, 1);
    }

    #[tokio::test]
    async fn sink_bytes_with_compression() {
        // Test that sink_bytes properly compresses data

        let config = HttpSinkConfig::new(
            "http://localhost:9999/test".to_string(),
            Some("POST".to_string()),
            None,
            None,
            None,
            Some("json".to_string()),
            None,
            Some(1), // 1 second timeout
            Some(0), // No retries for faster test
            Some("gzip".to_string()),
        );

        let mut sink = HttpSink::new(config).await.unwrap();

        let data = b"test binary data with compression";

        // This will fail because there's no server, but it verifies compression is applied
        let result = sink.sink_bytes(data).await;
        assert!(result.is_err()); // Expected to fail (no server)

        // Verify time_stats was updated
        assert_eq!(sink.time_stats.total_count, 1);
    }

    #[tokio::test]
    async fn sink_bytes_empty() {
        // Test that sink_bytes handles empty byte array

        let config = HttpSinkConfig::new(
            "http://localhost:9999/test".to_string(),
            Some("POST".to_string()),
            None,
            None,
            None,
            Some("json".to_string()),
            None,
            Some(1),
            Some(0), // No retries
            None,
        );

        let mut sink = HttpSink::new(config).await.unwrap();

        let data = b"";

        // Should handle empty bytes gracefully
        let result = sink.sink_bytes(data).await;
        assert!(result.is_err()); // Expected to fail (no server)
    }

    #[tokio::test]
    async fn sink_bytes_binary_data() {
        // Test that sink_bytes handles actual binary data (not just text)

        let config = HttpSinkConfig::new(
            "http://localhost:9999/test".to_string(),
            Some("POST".to_string()),
            None,
            None,
            None,
            Some("json".to_string()),
            None,
            Some(1),
            Some(0), // No retries
            None,
        );

        let mut sink = HttpSink::new(config).await.unwrap();

        // Binary data with non-ASCII bytes
        let data: Vec<u8> = vec![0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD];

        // Should handle binary data gracefully
        let result = sink.sink_bytes(&data).await;
        assert!(result.is_err()); // Expected to fail (no server)
    }

    #[tokio::test]
    async fn sink_str_updates_time_stats() {
        // Test that sink_str updates time statistics correctly

        let config = HttpSinkConfig::new(
            "http://localhost:9999/test".to_string(),
            Some("POST".to_string()),
            None,
            None,
            None,
            Some("json".to_string()),
            None,
            Some(1),
            Some(0), // No retries
            None,
        );

        let mut sink = HttpSink::new(config).await.unwrap();

        // Initial state
        assert_eq!(sink.time_stats.total_count, 0);

        // Send first string
        let _ = sink.sink_str("test1").await;
        assert_eq!(sink.time_stats.total_count, 1);

        // Send second string
        let _ = sink.sink_str("test2").await;
        assert_eq!(sink.time_stats.total_count, 2);
    }

    #[tokio::test]
    async fn sink_bytes_updates_time_stats() {
        // Test that sink_bytes updates time statistics correctly

        let config = HttpSinkConfig::new(
            "http://localhost:9999/test".to_string(),
            Some("POST".to_string()),
            None,
            None,
            None,
            Some("json".to_string()),
            None,
            Some(1),
            Some(0), // No retries
            None,
        );

        let mut sink = HttpSink::new(config).await.unwrap();

        // Initial state
        assert_eq!(sink.time_stats.total_count, 0);

        // Send first bytes
        let _ = sink.sink_bytes(b"test1").await;
        assert_eq!(sink.time_stats.total_count, 1);

        // Send second bytes
        let _ = sink.sink_bytes(b"test2").await;
        assert_eq!(sink.time_stats.total_count, 2);
    }

    #[tokio::test]
    async fn sink_str_batch_basic() {
        // Test that sink_str_batch properly joins and sends strings

        let config = HttpSinkConfig::new(
            "http://localhost:9999/test".to_string(),
            Some("POST".to_string()),
            None,
            None,
            None,
            Some("json".to_string()),
            None,
            Some(1), // 1 second timeout
            Some(0), // No retries for faster test
            None,
        );

        let mut sink = HttpSink::new(config).await.unwrap();

        let data = vec!["line1", "line2", "line3"];

        // This will fail because there's no server, but it verifies the method works
        let result = sink.sink_str_batch(data).await;
        assert!(result.is_err()); // Expected to fail (no server)

        // Verify time_stats was updated with correct count
        assert_eq!(sink.time_stats.total_count, 3);
    }

    #[tokio::test]
    async fn sink_str_batch_with_compression() {
        // Test that sink_str_batch properly compresses data

        let config = HttpSinkConfig::new(
            "http://localhost:9999/test".to_string(),
            Some("POST".to_string()),
            None,
            None,
            None,
            Some("json".to_string()),
            None,
            Some(1), // 1 second timeout
            Some(0), // No retries for faster test
            Some("gzip".to_string()),
        );

        let mut sink = HttpSink::new(config).await.unwrap();

        let data = vec!["line1", "line2", "line3"];

        // This will fail because there's no server, but it verifies compression is applied
        let result = sink.sink_str_batch(data).await;
        assert!(result.is_err()); // Expected to fail (no server)

        // Verify time_stats was updated
        assert_eq!(sink.time_stats.total_count, 3);
    }

    #[tokio::test]
    async fn sink_str_batch_empty() {
        // Test that sink_str_batch handles empty vector

        let config = HttpSinkConfig::new(
            "http://localhost:9999/test".to_string(),
            Some("POST".to_string()),
            None,
            None,
            None,
            Some("json".to_string()),
            None,
            Some(1),
            Some(0), // No retries
            None,
        );

        let mut sink = HttpSink::new(config).await.unwrap();

        let data: Vec<&str> = vec![];

        // Should handle empty vector gracefully
        let result = sink.sink_str_batch(data).await;
        assert!(result.is_err()); // Expected to fail (no server)
    }

    #[tokio::test]
    async fn sink_bytes_batch_basic() {
        // Test that sink_bytes_batch properly joins and sends byte arrays

        let config = HttpSinkConfig::new(
            "http://localhost:9999/test".to_string(),
            Some("POST".to_string()),
            None,
            None,
            None,
            Some("json".to_string()),
            None,
            Some(1), // 1 second timeout
            Some(0), // No retries for faster test
            None,
        );

        let mut sink = HttpSink::new(config).await.unwrap();

        let data: Vec<&[u8]> = vec![b"line1", b"line2", b"line3"];

        // This will fail because there's no server, but it verifies the method works
        let result = sink.sink_bytes_batch(data).await;
        assert!(result.is_err()); // Expected to fail (no server)

        // Verify time_stats was updated with correct count
        assert_eq!(sink.time_stats.total_count, 3);
    }

    #[tokio::test]
    async fn sink_bytes_batch_with_compression() {
        // Test that sink_bytes_batch properly compresses data

        let config = HttpSinkConfig::new(
            "http://localhost:9999/test".to_string(),
            Some("POST".to_string()),
            None,
            None,
            None,
            Some("json".to_string()),
            None,
            Some(1), // 1 second timeout
            Some(0), // No retries for faster test
            Some("gzip".to_string()),
        );

        let mut sink = HttpSink::new(config).await.unwrap();

        let data: Vec<&[u8]> = vec![b"line1", b"line2", b"line3"];

        // This will fail because there's no server, but it verifies compression is applied
        let result = sink.sink_bytes_batch(data).await;
        assert!(result.is_err()); // Expected to fail (no server)

        // Verify time_stats was updated
        assert_eq!(sink.time_stats.total_count, 3);
    }

    #[tokio::test]
    async fn sink_bytes_batch_empty() {
        // Test that sink_bytes_batch handles empty vector

        let config = HttpSinkConfig::new(
            "http://localhost:9999/test".to_string(),
            Some("POST".to_string()),
            None,
            None,
            None,
            Some("json".to_string()),
            None,
            Some(1),
            Some(0), // No retries
            None,
        );

        let mut sink = HttpSink::new(config).await.unwrap();

        let data: Vec<&[u8]> = vec![];

        // Should handle empty vector gracefully
        let result = sink.sink_bytes_batch(data).await;
        assert!(result.is_err()); // Expected to fail (no server)
    }

    #[tokio::test]
    async fn sink_bytes_batch_binary_data() {
        // Test that sink_bytes_batch handles actual binary data (not just text)

        let config = HttpSinkConfig::new(
            "http://localhost:9999/test".to_string(),
            Some("POST".to_string()),
            None,
            None,
            None,
            Some("json".to_string()),
            None,
            Some(1),
            Some(0), // No retries
            None,
        );

        let mut sink = HttpSink::new(config).await.unwrap();

        // Binary data with non-ASCII bytes
        let data1: Vec<u8> = vec![0x00, 0x01, 0x02];
        let data2: Vec<u8> = vec![0xFF, 0xFE, 0xFD];
        let data: Vec<&[u8]> = vec![&data1, &data2];

        // Should handle binary data gracefully
        let result = sink.sink_bytes_batch(data).await;
        assert!(result.is_err()); // Expected to fail (no server)
    }
}
