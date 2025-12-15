use async_trait::async_trait;
use reqwest::StatusCode;
use std::collections::HashMap;
use std::sync::Arc;
use wp_connector_api::{AsyncCtrl, AsyncRawDataSink, AsyncRecordSink};
use wp_model_core::format::{DataFormat, Json};
use wp_model_core::model::DataRecord;

const DEFAULT_BATCH: usize = 100;

pub struct ClickhouseSink {
    pub(crate) conf: wp_config::structure::io::Clickhouse,
    pub(crate) table: String,
    pub(crate) proc_cnt: usize,
    pub(crate) values: HashMap<String, Vec<String>>,
}

impl ClickhouseSink {
    pub fn new(conf: wp_config::structure::io::Clickhouse, table: String) -> Self {
        Self {
            conf,
            table,
            proc_cnt: 0,
            values: Default::default(),
        }
    }

    pub async fn insert_values(&self, table: &str, values: Vec<u8>) -> SinkResult<()> {
        let mut query = Vec::new();
        query.push(("database", self.conf.database.to_string()));
        query.push(("input_format_import_nested_json", "1".to_string()));
        query.push(("enable_http_compression", "1".to_string()));
        if self.conf.skip_unknown {
            query.push(("input_format_skip_unknown_fields", "1".to_string()));
        }
        if self.conf.date_time_best_effort {
            query.push(("date_time_input_format", "best_effort".to_string()));
        }
        query.push((
            "query",
            format!("INSERT INTO \"{}\" FORMAT JSONEachRow", table),
        ));

        let client = reqwest::Client::builder().build().map_err(|e| {
            SinkError::from(SinkReason::SinkError(format!(
                "ck client build fail: {}",
                e
            )))
        })?;
        let resp = client
            .post(self.conf.get_endpoint())
            .basic_auth(&self.conf.username, Some(&self.conf.password))
            .query(&query)
            .body(values)
            .send()
            .await
            .map_err(|e| SinkError::from(SinkReason::SinkError(format!("ck send fail: {}", e))))?;
        if resp.status().ne(&StatusCode::OK) {
            let text = resp.text().await.unwrap_or_default();
            return Err(SinkError::from(SinkReason::SinkError(format!(
                "CK insert fail: {}",
                text
            ))));
        }
        Ok(())
    }
}

#[async_trait]
impl AsyncCtrl for ClickhouseSink {
    async fn stop(&mut self) -> SinkResult<()> {
        for (table, values) in &self.values {
            let mut buf = Vec::new();
            for v in values {
                buf.extend_from_slice(format!("{}\n", v).as_bytes());
            }
            self.insert_values(table, buf).await?;
        }
        self.values.clear();
        Ok(())
    }

    async fn reconnect(&mut self) -> SinkResult<()> {
        let client = reqwest::Client::builder().build().map_err(|e| {
            SinkError::from(SinkReason::SinkError(format!(
                "ck client build fail: {}",
                e
            )))
        })?;
        let resp = client
            .get(self.conf.get_endpoint())
            .basic_auth(&self.conf.username, Some(&self.conf.password))
            .send()
            .await
            .map_err(|e| {
                SinkError::from(SinkReason::SinkError(format!("ck reconnect fail: {}", e)))
            })?;
        if resp.status() != StatusCode::OK {
            let t = resp.text().await.unwrap_or_default();
            return Err(SinkError::from(SinkReason::SinkError(format!(
                "ck reconnect fail: {}",
                t
            ))));
        }
        Ok(())
    }
}

#[async_trait]
impl AsyncRecordSink for ClickhouseSink {
    async fn sink_record(&mut self, data: &DataRecord) -> SinkResult<()> {
        // build json line
        let json_fmt = Json;
        let v = json_fmt.format_record(data).to_string();
        self.proc_cnt += 1;
        self.values.entry(self.table.clone()).or_default().push(v);
        if self
            .proc_cnt
            .is_multiple_of(self.conf.batch.unwrap_or(DEFAULT_BATCH))
        {
            for (table, values) in &self.values {
                let mut buf = Vec::new();
                for v in values {
                    buf.extend_from_slice(format!("{}\n", v).as_bytes());
                }
                self.insert_values(table, buf).await?;
            }
            self.values.clear();
        }
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
impl AsyncRawDataSink for ClickhouseSink {
    async fn sink_str(&mut self, _data: &str) -> SinkResult<()> {
        Err(SinkReason::SinkError("ClickHouse sink does not support raw input".into()).into())
    }
    async fn sink_bytes(&mut self, _data: &[u8]) -> SinkResult<()> {
        Err(SinkReason::SinkError("ClickHouse sink does not support raw bytes".into()).into())
    }

    async fn sink_str_batch(&mut self, _data: Vec<&str>) -> SinkResult<()> {
        Err(SinkReason::SinkError("ClickHouse sink does not support raw batch input".into()).into())
    }

    async fn sink_bytes_batch(&mut self, _data: Vec<&[u8]>) -> SinkResult<()> {
        Err(SinkReason::SinkError("ClickHouse sink does not support raw batch input".into()).into())
    }
}
