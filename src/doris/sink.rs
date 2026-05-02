//! Doris Sink 实现 - 使用 Stream Load API
//!
//! # Stream Load API
//!
//! 本实现使用 Doris 的 HTTP Stream Load API 进行批量数据导入：
//! - 高性能批量导入（比 MySQL 协议快 3-5 倍）
//! - Exactly-Once 语义（通过 label 机制）
//! - 无 SQL 注入风险（使用 JSON 格式）
//! - 支持负载均衡和故障转移
//!
//! # Label 机制
//!
//! 每次批量导入基于批次内容生成确定性 label，保证重试幂等：
//! - 相同 payload 会得到相同 label
//! - Doris 可据此识别重复导入请求

use crate::doris::config::DorisSinkConfig;
use crate::utils::time_stat_utils::TimeStatUtils;
use async_trait::async_trait;
use bytes::Bytes;
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use serde::Serialize;
use serde::ser::{SerializeMap, SerializeSeq};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use wp_connector_api::{
    AsyncCtrl, AsyncRawDataSink, AsyncRecordSink, SinkError, SinkReason, SinkResult,
};
use wp_model_core::model::{DataRecord, DataType, Value};

// 全局原子计数器，用于生成唯一的实例 ID
static INSTANCE_COUNTER: AtomicU64 = AtomicU64::new(0);

pub struct DorisSink {
    client: Client,
    url: String, // 预先构建的完整 URL
    user: String,
    password: String,
    max_retries: i32,
    headers: HashMap<String, String>,
    instance_id: u64, // 实例唯一 ID
    // 时间统计工具
    time_stats: TimeStatUtils,
    stopped: bool,
}

#[derive(Debug, Deserialize)]
struct StreamLoadResponse {
    #[serde(rename = "Status")]
    status: String,
    #[serde(rename = "Message", default)]
    message: String,
    #[serde(rename = "NumberTotalRows", default)]
    #[allow(dead_code)]
    number_total_rows: i64,
    #[serde(rename = "NumberLoadedRows", default)]
    number_loaded_rows: i64,
    #[serde(rename = "NumberFilteredRows", default)]
    number_filtered_rows: i64,
    #[serde(rename = "ExistingJobStatus", default)]
    existing_job_status: Option<String>,
}

#[derive(Debug, PartialEq, Eq)]
enum LoadOutcome {
    Success,
    Retry(String),
    Error(String),
}

impl DorisSink {
    /// 构建 Doris Sink，使用 Stream Load API。
    ///
    /// # Arguments
    /// * `config` - Doris 连接与写入配置
    ///
    /// # Returns
    /// * `anyhow::Result<Self>` - 成功返回初始化后的 sink
    pub async fn new(config: DorisSinkConfig) -> SinkResult<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(config.timeout_secs))
            .no_proxy()
            .build()
            .map_err(|e| SinkError::from(SinkReason::sink(format!("doris client build: {e}"))))?;

        // 预先构建完整的 Stream Load URL
        let url = format!(
            "{}/api/{}/{}/_stream_load",
            config.endpoint, config.database, config.table
        );

        // 从全局原子变量获取递增的实例 ID
        let instance_id = INSTANCE_COUNTER.fetch_add(1, Ordering::SeqCst);

        Ok(Self {
            client,
            url,
            user: config.user,
            password: config.password,
            max_retries: config.max_retries,
            headers: config.headers.unwrap_or_default(),
            instance_id,
            time_stats: TimeStatUtils::new(),
            stopped: false,
        })
    }

    /// 生成唯一的 label 用于 Stream Load。
    ///
    /// 使用批次内容生成稳定标签，确保上游重试同一批数据时仍能命中 Doris 的幂等语义。
    ///
    /// # Returns
    /// * `SinkResult<String>` - label 字符串
    fn generate_label(&self, payload: &Bytes) -> String {
        let (hash_a, hash_b) = fingerprint_bytes(payload.as_ref());
        format!(
            "doris_load_{:016x}_{:016x}_{:x}",
            hash_a,
            hash_b,
            payload.len()
        )
    }

    /// 将 DataRecord 转换为 JSON 对象。
    ///
    /// # Arguments
    /// * `record` - 数据记录
    ///
    /// # Returns
    /// 将批量记录转换为 NDJSON 格式（每行一个 JSON 对象）。
    ///
    /// # Arguments
    /// * `records` - 数据记录列表
    ///
    /// # Returns
    /// * `SinkResult<Bytes>` - NDJSON 字节流
    fn records_to_ndjson(&self, records: &[Arc<DataRecord>]) -> SinkResult<Bytes> {
        let mut buffer = Vec::new();

        for record in records {
            serde_json::to_writer(&mut buffer, &JsonRecord(record.as_ref()))
                .map_err(|e| sink_error(format!("json serialization failed: {}", e)))?;
            buffer.push(b'\n');
        }

        Ok(Bytes::from(buffer))
    }

    /// 执行 Stream Load 请求。
    ///
    /// # Arguments
    ///
    /// * `label` - 唯一标识符（使用第一条记录的 wp_event_id）
    /// * `data` - NDJSON 格式的数据
    ///
    /// # Returns
    ///
    /// * `SinkResult<()>` - 成功或错误
    async fn stream_load(&self, label: &str, data: Bytes) -> SinkResult<()> {
        let mut retries = 0i32;

        loop {
            let mut request = self
                .client
                .put(&self.url)
                .basic_auth(&self.user, Some(&self.password))
                .header("label", label)
                .header("format", "json")
                .header("read_json_by_line", "true")
                .header("Expect", "100-continue")
                .body(data.clone());

            // 添加自定义 headers
            for (key, value) in &self.headers {
                request = request.header(key, value);
            }

            match request.send().await {
                Ok(response) => {
                    let status = response.status();
                    let body = response
                        .text()
                        .await
                        .unwrap_or_else(|_| "failed to read response".to_string());

                    match Self::classify_load_response(status, &body) {
                        LoadOutcome::Success => return Ok(()),
                        LoadOutcome::Error(message) => return Err(sink_error(message)),
                        LoadOutcome::Retry(reason) => {
                            let retry_limit = if self.max_retries < 0 {
                                "unlimited".to_string()
                            } else {
                                self.max_retries.to_string()
                            };

                            if self.max_retries >= 0 && retries >= self.max_retries {
                                return Err(sink_error(format!(
                                    "max retries ({}) exceeded: {}",
                                    self.max_retries, reason
                                )));
                            }

                            log::warn!(
                                "stream load retry needed: label={}, reason={}, retry={}/{}",
                                label,
                                reason,
                                retries + 1,
                                retry_limit
                            );
                        }
                    }
                }
                Err(e) => {
                    if self.max_retries >= 0 && retries >= self.max_retries {
                        return Err(sink_error(format!(
                            "max retries ({}) exceeded: request failed: {}",
                            self.max_retries, e
                        )));
                    }

                    let retry_limit = if self.max_retries < 0 {
                        "unlimited".to_string()
                    } else {
                        self.max_retries.to_string()
                    };
                    log::warn!(
                        "request failed: {}, retry={}/{}",
                        e,
                        retries + 1,
                        retry_limit
                    );
                }
            }

            retries += 1;
            let backoff_ms = 1000 * 2_u64.pow(retries.min(10) as u32);
            tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
        }
    }

    /// 刷新缓冲区，将所有缓存的记录发送到 Doris。
    ///
    /// # Returns
    /// * `SinkResult<()>` - 成功或错误
    async fn flush_buffer(&mut self) -> SinkResult<()> {
        // No-op: buffering is disabled, records are sent immediately
        Ok(())
    }

    fn ensure_running(&self) -> SinkResult<()> {
        if self.stopped {
            return Err(sink_error("doris sink is stopped"));
        }
        Ok(())
    }

    fn classify_load_response(http_status: StatusCode, body: &str) -> LoadOutcome {
        match serde_json::from_str::<StreamLoadResponse>(body) {
            Ok(resp) => Self::classify_stream_load_response(http_status, &resp),
            Err(err) if http_status.is_success() => {
                log::warn!(
                    "failed to parse stream load response: {}, body: {}",
                    err,
                    body
                );
                LoadOutcome::Error(format!("invalid response format: {}", err))
            }
            Err(_) if http_status.is_client_error() => LoadOutcome::Error(format!(
                "client error: status={}, body={}",
                http_status, body
            )),
            Err(_) => LoadOutcome::Retry(format!(
                "server error: status={}, body={}",
                http_status, body
            )),
        }
    }

    fn classify_stream_load_response(
        http_status: StatusCode,
        response: &StreamLoadResponse,
    ) -> LoadOutcome {
        match response.status.as_str() {
            "Success" | "Publish Timeout" if response.number_filtered_rows == 0 => {
                log::info!(
                    "stream load success: status={}, loaded={}, filtered={}",
                    response.status,
                    response.number_loaded_rows,
                    response.number_filtered_rows
                );
                LoadOutcome::Success
            }
            "Success" | "Publish Timeout" => LoadOutcome::Error(format!(
                "stream load partially failed: status={}, loaded={}, filtered={}",
                response.status, response.number_loaded_rows, response.number_filtered_rows
            )),
            "Label Already Exists" => match response.existing_job_status.as_deref() {
                Some("FINISHED") => {
                    log::info!("stream load label already committed, treating as success");
                    LoadOutcome::Success
                }
                Some("RUNNING") => LoadOutcome::Retry(
                    "label already exists and previous load is still running".to_string(),
                ),
                Some(existing_status) => LoadOutcome::Error(format!(
                    "stream load label already exists with unexpected job status={}",
                    existing_status
                )),
                None => LoadOutcome::Error(
                    "stream load label already exists but ExistingJobStatus is missing".to_string(),
                ),
            },
            "Fail" => LoadOutcome::Error(format!(
                "stream load failed: status={}, message={}",
                response.status, response.message
            )),
            _ if http_status.is_client_error() => LoadOutcome::Error(format!(
                "stream load failed: status={}, message={}",
                response.status, response.message
            )),
            _ if http_status.is_server_error() => LoadOutcome::Retry(format!(
                "stream load failed: status={}, message={}",
                response.status, response.message
            )),
            _ => LoadOutcome::Error(format!(
                "stream load failed: status={}, message={}",
                response.status, response.message
            )),
        }
    }
}

#[async_trait]
impl AsyncCtrl for DorisSink {
    async fn stop(&mut self) -> SinkResult<()> {
        if self.stopped {
            return Ok(());
        }

        // 停止前刷新缓冲区，确保没有数据丢失
        self.flush_buffer().await?;
        self.stopped = true;
        Ok(())
    }

    async fn reconnect(&mut self) -> SinkResult<()> {
        if self.stopped {
            return Err(sink_error("doris sink is stopped"));
        }
        // HTTP 客户端无需显式重连
        Ok(())
    }
}

#[async_trait]
impl AsyncRecordSink for DorisSink {
    async fn sink_record(&mut self, data: &DataRecord) -> SinkResult<()> {
        self.sink_records(vec![Arc::new(data.clone())]).await
    }

    async fn sink_records(&mut self, data: Vec<Arc<DataRecord>>) -> SinkResult<()> {
        self.ensure_running()?;

        if data.is_empty() {
            return Ok(());
        }

        // 开始统计
        self.time_stats.start_stat(data.len() as u64);

        // 生成label
        let ndjson = self.records_to_ndjson(&data)?;
        let label = self.generate_label(&ndjson);
        self.stream_load(&label, ndjson).await?;

        // 结束统计
        self.time_stats.end_stat();

        // 打印统计信息
        self.time_stats
            .println(&format!("DorisSink-{}", self.instance_id));

        Ok(())
    }
}

#[async_trait]
impl AsyncRawDataSink for DorisSink {
    async fn sink_str(&mut self, _data: &str) -> SinkResult<()> {
        Err(sink_error("doris sink does not accept raw text input"))
    }

    async fn sink_bytes(&mut self, _data: &[u8]) -> SinkResult<()> {
        Err(sink_error("doris sink does not accept raw byte input"))
    }

    async fn sink_str_batch(&mut self, _data: Vec<&str>) -> SinkResult<()> {
        Err(sink_error("doris sink does not accept raw batch input"))
    }

    async fn sink_bytes_batch(&mut self, _data: Vec<&[u8]>) -> SinkResult<()> {
        Err(sink_error(
            "doris sink does not accept raw batch byte input",
        ))
    }
}

/// 统一封装 sink 层错误。
fn sink_error(msg: impl Into<String>) -> SinkError {
    SinkError::from(SinkReason::Sink(msg.into()))
}

fn fingerprint_bytes(bytes: &[u8]) -> (u64, u64) {
    const OFFSET_A: u64 = 0xcbf29ce484222325;
    const OFFSET_B: u64 = 0x84222325cbf29ce4;
    const PRIME_A: u64 = 0x100000001b3;
    const PRIME_B: u64 = 0x10000000161;

    let mut hash_a = OFFSET_A;
    let mut hash_b = OFFSET_B;

    for (idx, byte) in bytes.iter().copied().enumerate() {
        hash_a ^= u64::from(byte);
        hash_a = hash_a.wrapping_mul(PRIME_A);

        hash_b ^= u64::from(byte).wrapping_add(idx as u64);
        hash_b = hash_b.wrapping_mul(PRIME_B);
    }

    (hash_a, hash_b)
}

struct JsonRecord<'a>(&'a DataRecord);

impl Serialize for JsonRecord<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let field_count = self
            .0
            .items
            .iter()
            .filter(|field| *field.get_meta() != DataType::Ignore)
            .count();
        let mut map = serializer.serialize_map(Some(field_count))?;

        for field in &self.0.items {
            if *field.get_meta() == DataType::Ignore {
                continue;
            }

            map.serialize_entry(field.get_name(), &JsonFieldValue(field.get_value()))?;
        }

        map.end()
    }
}

struct JsonFieldValue<'a>(&'a Value);

impl Serialize for JsonFieldValue<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self.0 {
            Value::Null => serializer.serialize_none(),
            Value::Bool(v) => serializer.serialize_bool(*v),
            Value::Chars(v) => serializer.serialize_str(v),
            Value::Float(v) => serializer.serialize_f64(*v),
            Value::Digit(v) => serializer.serialize_i64(*v),
            Value::Time(v) => serializer.serialize_str(&v.to_string()),
            Value::IpNet(v) => serializer.serialize_str(&v.to_string()),
            Value::IpAddr(v) => serializer.serialize_str(&v.to_string()),
            Value::Domain(v) => serializer.serialize_str(&v.to_string()),
            Value::Url(v) => serializer.serialize_str(&v.to_string()),
            Value::Email(v) => serializer.serialize_str(&v.to_string()),
            Value::IdCard(v) => serializer.serialize_str(&v.to_string()),
            Value::MobilePhone(v) => serializer.serialize_str(&v.to_string()),
            Value::Hex(v) => serializer.serialize_str(&v.to_string()),
            Value::Obj(obj) => {
                let field_count = obj
                    .iter()
                    .filter(|(_, field)| *field.as_field().get_meta() != DataType::Ignore)
                    .count();
                let mut map = serializer.serialize_map(Some(field_count))?;
                for (key, field) in obj.iter() {
                    if *field.as_field().get_meta() == DataType::Ignore {
                        continue;
                    }
                    map.serialize_entry(
                        key.as_str(),
                        &JsonFieldValue(field.as_field().get_value()),
                    )?;
                }
                map.end()
            }
            Value::Array(values) => {
                let item_count = values
                    .iter()
                    .filter(|field| *field.as_field().get_meta() != DataType::Ignore)
                    .count();
                let mut seq = serializer.serialize_seq(Some(item_count))?;
                for field in values {
                    if *field.as_field().get_meta() == DataType::Ignore {
                        continue;
                    }
                    seq.serialize_element(&JsonFieldValue(field.as_field().get_value()))?;
                }
                seq.end()
            }
            Value::Symbol(v) => serializer.serialize_str(v),
            Value::Ignore(_) => serializer.serialize_none(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use httpmock::prelude::*;
    use std::collections::HashMap;
    use wp_model_core::model::DataField;
    use wp_model_core::model::types::value::ObjectValue;

    fn test_config() -> DorisSinkConfig {
        DorisSinkConfig::new(
            "http://localhost:8040".into(),
            "demo".into(),
            "events".into(),
            "root".into(),
            "".into(),
            Some(1),
            Some(0),
            None,
        )
    }

    async fn create_mock_sink(server: &MockServer, max_retries: i32) -> DorisSink {
        let mut headers = HashMap::new();
        headers.insert("strict_mode".into(), "true".into());

        DorisSink::new(DorisSinkConfig::new(
            server.base_url(),
            "demo".into(),
            "events".into(),
            "root".into(),
            "".into(),
            Some(5),
            Some(max_retries),
            Some(headers),
        ))
        .await
        .unwrap()
    }

    fn sample_record() -> DataRecord {
        let mut record = DataRecord::default();
        record.append(DataField::from_digit("id", 1));
        record.append(DataField::from_chars("name", "alice"));
        record
    }

    #[test]
    fn classifies_publish_timeout_as_success() {
        let response = StreamLoadResponse {
            status: "Publish Timeout".into(),
            message: String::new(),
            number_total_rows: 1,
            number_loaded_rows: 1,
            number_filtered_rows: 0,
            existing_job_status: None,
        };

        assert_eq!(
            DorisSink::classify_stream_load_response(StatusCode::OK, &response),
            LoadOutcome::Success
        );
    }

    #[test]
    fn classifies_filtered_rows_as_error() {
        let response = StreamLoadResponse {
            status: "Success".into(),
            message: String::new(),
            number_total_rows: 10,
            number_loaded_rows: 9,
            number_filtered_rows: 1,
            existing_job_status: None,
        };

        assert!(matches!(
            DorisSink::classify_stream_load_response(StatusCode::OK, &response),
            LoadOutcome::Error(_)
        ));
    }

    #[test]
    fn label_already_exists_only_succeeds_for_finished_job() {
        let running = StreamLoadResponse {
            status: "Label Already Exists".into(),
            message: String::new(),
            number_total_rows: 0,
            number_loaded_rows: 0,
            number_filtered_rows: 0,
            existing_job_status: Some("RUNNING".into()),
        };
        let finished = StreamLoadResponse {
            status: "Label Already Exists".into(),
            message: String::new(),
            number_total_rows: 0,
            number_loaded_rows: 0,
            number_filtered_rows: 0,
            existing_job_status: Some("FINISHED".into()),
        };

        assert!(matches!(
            DorisSink::classify_stream_load_response(StatusCode::CONFLICT, &finished),
            LoadOutcome::Success
        ));
        assert!(matches!(
            DorisSink::classify_stream_load_response(StatusCode::CONFLICT, &running),
            LoadOutcome::Retry(_)
        ));
    }

    #[test]
    fn classifies_fail_as_non_retryable() {
        let response = StreamLoadResponse {
            status: "Fail".into(),
            message: "type mismatch".into(),
            number_total_rows: 1,
            number_loaded_rows: 0,
            number_filtered_rows: 0,
            existing_job_status: None,
        };

        assert!(matches!(
            DorisSink::classify_stream_load_response(StatusCode::INTERNAL_SERVER_ERROR, &response),
            LoadOutcome::Error(_)
        ));
    }

    #[tokio::test]
    async fn stop_prevents_future_writes() {
        let mut sink = DorisSink::new(test_config()).await.unwrap();
        sink.stop().await.unwrap();

        let mut record = DataRecord::default();
        record.append(DataField::from_digit("id", 1));

        let err = sink.sink_record(&record).await.unwrap_err();
        assert!(matches!(err.reason(), SinkReason::Sink(m) if m.contains("stopped")));
    }

    #[tokio::test]
    async fn record_to_json_preserves_value_types() {
        let sink = DorisSink::new(test_config()).await.unwrap();
        let mut record = DataRecord::default();
        record.append(DataField::from_chars("zip_code", "00123"));
        record.append(DataField::new(DataType::Bool, "enabled", Value::Bool(true)));

        let mut nested = ObjectValue::new();
        nested.insert("count", DataField::from_digit("count", 7));
        nested.insert(
            "ignored",
            DataField::new(
                DataType::Ignore,
                "ignored",
                Value::Ignore(Default::default()),
            ),
        );
        record.append(DataField::new(DataType::Obj, "meta", Value::Obj(nested)));

        let encoded = sink.records_to_ndjson(&[Arc::new(record)]).unwrap();
        let json: serde_json::Value = serde_json::from_slice(encoded.as_ref()).unwrap();

        assert_eq!(json["zip_code"], serde_json::Value::String("00123".into()));
        assert_eq!(json["enabled"], serde_json::Value::Bool(true));
        assert_eq!(json["meta"]["count"], serde_json::Value::Number(7.into()));
        assert!(json["meta"].get("ignored").is_none());
    }

    #[tokio::test]
    async fn label_is_deterministic_for_same_payload() {
        let sink = DorisSink::new(test_config()).await.unwrap();
        let record = sample_record();

        let payload = sink.records_to_ndjson(&[Arc::new(record.clone())]).unwrap();
        let label_a = sink.generate_label(&payload);
        let payload_again = sink.records_to_ndjson(&[Arc::new(record)]).unwrap();
        let label_b = sink.generate_label(&payload_again);

        assert_eq!(label_a, label_b);
    }

    #[tokio::test]
    async fn stream_load_retries_running_label_until_finished() {
        let server = MockServer::start_async().await;
        let mut sink = create_mock_sink(&server, 1).await;
        let record = sample_record();
        let payload = sink.records_to_ndjson(&[Arc::new(record.clone())]).unwrap();
        let expected_label = sink.generate_label(&payload);

        let running_mock = server
            .mock_async(|when, then| {
                when.method(PUT)
                    .path("/api/demo/events/_stream_load")
                    .header("label", expected_label.as_str())
                    .header("format", "json")
                    .header("read_json_by_line", "true");
                then.status(409).json_body_obj(&serde_json::json!({
                    "Status": "Label Already Exists",
                    "ExistingJobStatus": "RUNNING",
                    "Message": ""
                }));
            })
            .await;

        let task = tokio::spawn(async move { sink.sink_records(vec![Arc::new(record)]).await });

        for _ in 0..50 {
            if running_mock.calls_async().await == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        running_mock.assert_calls_async(1).await;
        running_mock.delete_async().await;

        let finished_mock = server
            .mock_async(|when, then| {
                when.method(PUT)
                    .path("/api/demo/events/_stream_load")
                    .header("label", expected_label.as_str())
                    .header("format", "json")
                    .header("read_json_by_line", "true");
                then.status(409).json_body_obj(&serde_json::json!({
                    "Status": "Label Already Exists",
                    "ExistingJobStatus": "FINISHED",
                    "Message": ""
                }));
            })
            .await;

        let result = task.await.unwrap();
        assert!(result.is_ok(), "RUNNING -> FINISHED should succeed");
        finished_mock.assert_calls_async(1).await;
    }

    #[tokio::test]
    async fn stream_load_does_not_retry_fail_status() {
        let server = MockServer::start_async().await;
        let mut sink = create_mock_sink(&server, 3).await;

        let fail_mock = server
            .mock_async(|when, then| {
                when.method(PUT).path("/api/demo/events/_stream_load");
                then.status(500).json_body_obj(&serde_json::json!({
                    "Status": "Fail",
                    "Message": "type mismatch"
                }));
            })
            .await;

        let err = sink
            .sink_records(vec![Arc::new(sample_record())])
            .await
            .unwrap_err();
        assert!(matches!(err.reason(), SinkReason::Sink(m) if m.contains("type mismatch")));
        fail_mock.assert_calls_async(1).await;
    }

    #[tokio::test]
    async fn stream_load_returns_error_on_filtered_rows() {
        let server = MockServer::start_async().await;
        let mut sink = create_mock_sink(&server, 2).await;

        let filtered_mock = server
            .mock_async(|when, then| {
                when.method(PUT).path("/api/demo/events/_stream_load");
                then.status(200).json_body_obj(&serde_json::json!({
                    "Status": "Success",
                    "Message": "",
                    "NumberTotalRows": 10,
                    "NumberLoadedRows": 9,
                    "NumberFilteredRows": 1
                }));
            })
            .await;

        let err = sink
            .sink_records(vec![Arc::new(sample_record())])
            .await
            .unwrap_err();
        assert!(matches!(err.reason(), SinkReason::Sink(m) if m.contains("partially failed")));
        filtered_mock.assert_calls_async(1).await;
    }
}
