use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use wp_connector_api::{
    AsyncCtrl, AsyncRawDataSink, AsyncRecordSink, SinkError, SinkReason, SinkResult,
};
use wp_data_fmt::{DataFormat, FormatType};
use wp_log::error_data;
use wp_model_core::model::{DataRecord, Value, fmt_def::TextFmt};

pub(crate) struct VictoriaLogSink {
    endpoint: String,
    insert_path: String,
    client: reqwest::Client,
    fmt: TextFmt,
    create_time_field: Option<String>,
}

impl VictoriaLogSink {
    /// 解析时间字段为字符串，逻辑与原实现一致：优先使用 create_time_field，回退当前时间。
    fn resolve_timestamp_str(&self, data: &DataRecord) -> String {
        let now = chrono::Utc::now()
            .timestamp_nanos_opt()
            .unwrap_or(chrono::Utc::now().timestamp_millis())
            .to_string();

        let Some(field) = &self.create_time_field else {
            return now;
        };
        let Some(orin_timestamp) = data.get2(field) else {
            return now;
        };

        if let Value::Time(dt) = &orin_timestamp.value {
            return dt
                .and_utc()
                .timestamp_nanos_opt()
                .unwrap_or_else(|| dt.and_utc().timestamp_millis())
                .to_string();
        }

        now
    }

    pub(crate) fn new(
        endpoint: String,
        insert_path: String,
        client: reqwest::Client,
        fmt: TextFmt,
        create_time_field: Option<String>,
    ) -> Self {
        Self {
            endpoint,
            insert_path,
            client,
            fmt,
            create_time_field,
        }
    }
}

#[async_trait]
impl AsyncRecordSink for VictoriaLogSink {
    async fn sink_record(&mut self, data: &DataRecord) -> SinkResult<()> {
        let mut value_map = data
            .items
            .clone()
            .into_iter()
            .map(|item| (item.get_name().to_string(), item.get_value().to_string()))
            .collect::<HashMap<String, String>>();
        let timestamp = self.resolve_timestamp_str(data);
        let fmt = FormatType::from(&self.fmt);
        let formatted_msg = fmt.format_record(data);
        value_map.insert("_msg".to_string(), formatted_msg.clone());
        value_map.insert("_time".to_string(), timestamp);
        let res = serde_json::to_string(&value_map).map_err(|e| {
            SinkError::from(SinkReason::Sink(format!(
                "build jsonline for victorialogs flush fail: {}",
                e
            )))
        })?;

        match self
            .client
            .post(format!("{}{}", self.endpoint, self.insert_path))
            .body(res)
            .send()
            .await
        {
            Ok(resp) => {
                if !resp.status().is_success() {
                    error_data!("reqwest send error, text: {:?}", resp.text().await);
                }
            }
            Err(e) => {
                error_data!("reqwest send error, text: {:?}", e);
            }
        };
        Ok(())
    }

    async fn sink_records(&mut self, data: Vec<Arc<DataRecord>>) -> SinkResult<()> {
        for record in data {
            self.sink_record(record.as_ref()).await?;
        }
        Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use wp_connector_api::AsyncRecordSink;
    use wp_model_core::model::{DataField, DataRecord};

    #[tokio::test]
    async fn test_sink_record_json_conversion() {
        let record = DataRecord::default();
        let client = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(1))
            .build()
            .expect("Failed to create client");

        let mut sink = VictoriaLogSink::new(
            "http://127.0.0.1:8428".into(),
            "/insert".into(),
            client,
            TextFmt::Json,
            None,
        );

        let result = sink.sink_record(&record).await;
        assert!(result.is_ok(), "sink_record should return Ok");
    }

    /// 创建用于测试的 VictoriaLogSink 实例
    ///
    /// # 参数
    /// * `create_time_field` - 可选的自定义时间戳字段名称
    ///
    /// # 返回
    /// 配置好的 VictoriaLogSink 实例，用于测试
    fn create_test_sink(create_time_field: Option<&str>) -> VictoriaLogSink {
        let client = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(1))
            .build()
            .expect("Failed to create client");

        VictoriaLogSink::new(
            "http://127.0.0.1:8428".into(),
            "/insert".into(),
            client,
            TextFmt::Json,
            create_time_field.map(|s| s.to_string()),
        )
    }

    #[tokio::test]
    async fn test_resolve_timestamp_str() {
        // 测试用例定义
        //
        // 格式: (create_time_field, field_value, description)
        //
        // * create_time_field - sink 配置的时间戳字段（None 表示使用默认）
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

        for (create_time_field, field_value, desc) in cases {
            let mut record = DataRecord::default();
            if let Some(value) = field_value {
                record.append(DataField::from_chars("timestamp_field", value));
            }

            let sink = create_test_sink(create_time_field);
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
