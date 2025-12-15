use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use wp_connector_api::{AsyncCtrl, AsyncRawDataSink, AsyncRecordSink, SinkResult};
use wp_data_fmt::{DataFormat, FormatType};
use wp_log::error_data;
use wp_model_core::model::{DataRecord, fmt_def::TextFmt};

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct VictoriaLogData {
    pub streams: Vec<VictoriaLogStream>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct VictoriaLogStream {
    pub stream: HashMap<String, String>,
    pub values: Vec<(String, String)>,
}

pub(crate) struct VictoriaLogSink {
    endpoint: String,
    insert_path: String,
    client: reqwest::Client,
    fmt: TextFmt,
}

impl VictoriaLogSink {
    pub(crate) fn new(
        endpoint: String,
        insert_path: String,
        client: reqwest::Client,
        fmt: TextFmt,
    ) -> Self {
        Self {
            endpoint,
            insert_path,
            client,
            fmt,
        }
    }
}

#[async_trait]
impl AsyncRecordSink for VictoriaLogSink {
    async fn sink_record(&mut self, data: &DataRecord) -> SinkResult<()> {
        let mut value_map = HashMap::new();

        // 处理data.items中的字段，转换为HashMap
        for item in &data.items {
            // 获取字段名和值，并添加到HashMap中
            let name = item.get_name();
            let val = item.get_value().to_string();
            value_map.insert(name.to_string(), val);
        }

        // 获取当前时间的毫秒级时间戳
        let timestamp = chrono::Local::now().timestamp_millis().to_string();
        let fmt = FormatType::from(&self.fmt);
        let stream = VictoriaLogStream {
            stream: value_map,
            values: vec![(timestamp, fmt.format_record(data))],
        };

        let data = VictoriaLogData {
            streams: vec![stream],
        };

        match self
            .client
            .post(format!("{}{}", self.endpoint, self.insert_path))
            .json(&data)
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
    use wp_model_core::model::DataRecord;

    #[tokio::test]
    async fn test_sink_record_json_conversion() {
        // 创建测试用的DataRecord
        let record = DataRecord::default();

        // 创建测试用的VictoriaLogSink实例
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
        );

        // 调用 sink_record 方法处理数据
        let result = sink.sink_record(&record).await;

        // 验证方法执行成功
        assert!(result.is_ok(), "sink_record should return Ok");
    }
}
