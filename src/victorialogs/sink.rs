use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use orion_error::conversion::ToStructError;
use tokio::time::sleep;
use wp_connector_api::{AsyncCtrl, AsyncRawDataSink, AsyncRecordSink, SinkReason, SinkResult};
use wp_data_fmt::{FormatType, RecordFormatter};
use wp_log::error_data;
use wp_model_core::model::{DataRecord, Value, fmt_def::TextFmt};

pub(crate) struct VictoriaLogSink {
    write_url: String,
    client: reqwest::Client,
    fmt: TextFmt,
    timestamp_field: Option<String>,
    tags: HashMap<String, String>,
}

impl VictoriaLogSink {
    /// 解析时间字段为字符串，逻辑与原实现一致：优先使用 timestamp_field，回退当前时间。
    fn resolve_timestamp_str(&self, data: &DataRecord) -> String {
        let now = chrono::Utc::now()
            .timestamp_nanos_opt()
            .unwrap_or(chrono::Utc::now().timestamp_millis())
            .to_string();

        let Some(field) = &self.timestamp_field else {
            return now;
        };
        let Some(orin_timestamp) = data.get2(field) else {
            return now;
        };

        if let Value::Time(dt) = orin_timestamp.get_value() {
            return dt
                .and_utc()
                .timestamp_nanos_opt()
                .unwrap_or_else(|| dt.and_utc().timestamp_millis())
                .to_string();
        }

        now
    }

    /// 构建单条 JSON line 载荷，供单条或批量发送复用。
    fn build_jsonline(&self, data: &DataRecord) -> SinkResult<String> {
        let mut value_map = data
            .items
            .clone()
            .into_iter()
            .map(|item| (item.get_name().to_string(), item.get_value().to_string()))
            .collect::<HashMap<String, String>>();
        let timestamp = self.resolve_timestamp_str(data);
        let fmt = FormatType::from(&self.fmt);
        let formatted_msg = fmt.fmt_record(data);
        value_map.extend(self.tags.clone());
        value_map.insert("_msg".to_string(), formatted_msg);
        value_map.insert("_time".to_string(), timestamp);
        serde_json::to_string(&value_map).map_err(|e| {
            SinkReason::sink(format!("build jsonline for victorialogs flush fail: {}", e))
        })
    }

    async fn send_payload(&self, payload: String) -> SinkResult<()> {
        let client = &self.client;
        let write_url = &self.write_url;
        // 重试次数
        const MAX_ATTEMPTS: usize = 3;
        // 是重试之间的退避时间
        const BACKOFF_MS: [u64; 2] = [200, 500];

        for attempt in 0..MAX_ATTEMPTS {
            match client.post(write_url).body(payload.clone()).send().await {
                Ok(resp) => {
                    let status = resp.status();
                    if status.is_success() {
                        return Ok(());
                    }
                    error_data!("reqwest send error, text: {:?}", resp.text().await);
                    if !status.is_server_error() {
                        return Err(SinkReason::sink("reqwest send error"));
                    }
                }
                Err(e) => {
                    error_data!("reqwest send error, text: {:?}", e);
                    if !(e.is_timeout() || e.is_connect()) {
                        return Err(SinkReason::Sink
                            .to_err()
                            .with_detail("reqwest send fail")
                            .with_source(e));
                    }
                }
            }

            if attempt + 1 < MAX_ATTEMPTS {
                let delay = BACKOFF_MS
                    .get(attempt)
                    .copied()
                    .unwrap_or_else(|| BACKOFF_MS[BACKOFF_MS.len() - 1]);
                sleep(Duration::from_millis(delay)).await;
            }
        }

        Err(SinkReason::sink("reqwest send fail after retries"))
    }

    pub(crate) fn new(
        write_url: String,
        client: reqwest::Client,
        fmt: TextFmt,
        timestamp_field: Option<String>,
        tags: Vec<String>,
    ) -> Self {
        let tag_map: HashMap<String, String> = tags
            .into_iter()
            .filter_map(|tag| {
                let (key, value) = tag.split_once(':')?;
                Some((key.to_string(), value.to_string()))
            })
            .collect();
        Self {
            write_url,
            client,
            fmt,
            timestamp_field,
            tags: tag_map,
        }
    }
}

#[async_trait]
impl AsyncRecordSink for VictoriaLogSink {
    async fn sink_record(&mut self, data: &DataRecord) -> SinkResult<()> {
        let res = self.build_jsonline(data)?;
        self.send_payload(res).await
    }

    async fn sink_records(&mut self, data: Vec<Arc<DataRecord>>) -> SinkResult<()> {
        if data.is_empty() {
            return Ok(());
        }
        let mut buf = String::new();
        for (idx, record) in data.iter().enumerate() {
            let line = self.build_jsonline(record.as_ref())?;
            if idx > 0 {
                buf.push('\n');
            }
            buf.push_str(&line);
        }
        self.send_payload(buf).await
    }
}

#[async_trait]
impl AsyncCtrl for VictoriaLogSink {
    async fn stop(&mut self) -> SinkResult<()> {
        Ok(())
    }
    async fn reconnect(&mut self) -> SinkResult<()> {
        Ok(())
    }
}

#[async_trait]
impl AsyncRawDataSink for VictoriaLogSink {
    async fn sink_str(&mut self, _data: &str) -> SinkResult<()> {
        Ok(())
    }
    async fn sink_bytes(&mut self, _data: &[u8]) -> SinkResult<()> {
        Ok(())
    }

    async fn sink_str_batch(&mut self, _data: Vec<&str>) -> SinkResult<()> {
        Ok(())
    }

    async fn sink_bytes_batch(&mut self, _data: Vec<&[u8]>) -> SinkResult<()> {
        Ok(())
    }
}

// 仅在测试时编译该测试模块
#[cfg(test)]
mod tests {
    use super::*;
    use httpmock::prelude::*;
    use serde_json::Value as JsonValue;
    use std::time::Duration;
    use wp_connector_api::AsyncRecordSink;
    use wp_model_core::model::{DataField, DataRecord};

    #[tokio::test]
    async fn test_sink_record_json_conversion() {
        // 使用本地 mock server 模拟 VictoriaLogs 接口，避免真实网络依赖
        let server = MockServer::start_async().await;
        let _mock = server.mock(|when, then| {
            when.method(POST).path("/insert");
            then.status(200);
        });

        let record = DataRecord::default();
        let client = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(1))
            .build()
            .expect("Failed to create client");

        let mut sink = VictoriaLogSink::new(
            format!("{}/insert", server.base_url()),
            client,
            TextFmt::Json,
            None,
            Vec::new(),
        );

        let result = sink.sink_record(&record).await;
        assert!(result.is_ok(), "sink_record should return Ok");
    }

    /// 创建用于测试的 VictoriaLogSink 实例（mock server）
    fn create_mock_sink(server: &MockServer) -> VictoriaLogSink {
        let client = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(2))
            .build()
            .expect("Failed to create client");

        VictoriaLogSink::new(
            format!("{}/insert", server.base_url()),
            client,
            TextFmt::Json,
            None,
            Vec::new(),
        )
    }

    #[test]
    fn test_build_jsonline_contains_msg_and_time() {
        let mut record = DataRecord::default();
        record.append(DataField::from_chars("level", "info"));
        let sink = create_test_sink(None);
        let line = sink.build_jsonline(&record).expect("构建 jsonline 失败");
        let parsed: JsonValue = serde_json::from_str(&line).expect("解析 json 失败");

        assert_eq!(parsed["level"], "info");
        assert!(parsed.get("_msg").is_some(), "_msg 字段应存在");
        let time_val = parsed
            .get("_time")
            .and_then(|v| v.as_str())
            .expect("_time 应为字符串");
        assert!(time_val.parse::<i64>().is_ok(), "_time 应能解析为 i64");
    }

    #[tokio::test]
    async fn test_send_payload_success() {
        let server = MockServer::start_async().await;
        let mock_200 = server.mock(|when, then| {
            when.method(POST).path("/insert");
            then.status(200);
        });

        let sink = create_mock_sink(&server);
        let result = sink.send_payload("{}".to_string()).await;

        assert!(result.is_ok(), "send_payload 应返回 Ok");
        assert_eq!(mock_200.calls(), 1, "仅应发送一次请求");
    }

    #[tokio::test]
    async fn test_send_payload_retry_on_server_error() {
        let server = MockServer::start_async().await;
        let mock_500 = server.mock(|when, then| {
            when.method(POST).path("/insert");
            then.status(500);
        });

        let sink = create_mock_sink(&server);
        let result = sink.send_payload("{}".to_string()).await;

        assert!(result.is_err(), "服务端错误应返回 Err");
        assert_eq!(mock_500.calls(), 3, "应重试 3 次");
    }

    #[tokio::test]
    async fn test_send_payload_no_retry_on_client_error() {
        let server = MockServer::start_async().await;
        let mock_400 = server.mock(|when, then| {
            when.method(POST).path("/insert");
            then.status(400);
        });

        let sink = create_mock_sink(&server);
        let result = sink.send_payload("{}".to_string()).await;

        assert!(result.is_err(), "客户端错误应返回 Err");
        assert_eq!(mock_400.calls(), 1, "4xx 不应重试");
    }

    /// 创建用于测试的 VictoriaLogSink 实例
    ///
    /// # 参数
    /// * `timestamp_field` - 可选的自定义时间戳字段名称
    ///
    /// # 返回
    /// 配置好的 VictoriaLogSink 实例，用于测试
    fn create_test_sink(timestamp_field: Option<&str>) -> VictoriaLogSink {
        let client = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(1))
            .build()
            .expect("Failed to create client");

        VictoriaLogSink::new(
            "http://127.0.0.1:8428/insert".into(),
            client,
            TextFmt::Json,
            timestamp_field.map(|s| s.to_string()),
            Vec::new(),
        )
    }

    #[tokio::test]
    async fn test_resolve_timestamp_str() {
        // 测试用例定义
        //
        // 格式: (timestamp_field, field_value, description)
        //
        // * timestamp_field - sink 配置的时间戳字段（None 表示使用默认）
        // * field_value       - DataRecord 中实际存储的字段值（None 表示不添加字段）
        // * description       - 测试用例描述
        let cases = vec![
            (None, None, "no custom field"),
            (Some("timestamp_field"), None, "field not found"),
            (
                Some("timestamp_field"),
                Some("1234567890"),
                "with valid field",
            ),
        ];

        for (timestamp_field, field_value, desc) in cases {
            let mut record = DataRecord::default();
            if let Some(value) = field_value {
                record.append(DataField::from_chars("timestamp_field", value));
            }

            let sink = create_test_sink(timestamp_field);
            let timestamp = sink.resolve_timestamp_str(&record);

            assert!(
                !timestamp.is_empty(),
                "[{}] timestamp should not be empty",
                desc
            );
            assert!(
                timestamp.parse::<i64>().is_ok(),
                "[{}] timestamp should be valid i64",
                desc
            );
        }
    }
}
