use async_trait::async_trait;
use reqwest::{StatusCode, header::CONTENT_TYPE};
use std::collections::VecDeque;
use std::sync::Arc;
use wp_connector_api::{AsyncCtrl, AsyncRawDataSink, AsyncRecordSink};
use wp_error::dist_error::{SinkError, SinkReason, SinkResult};
use wp_model_core::format::{DataFormat, Json};
use wp_model_core::model::DataRecord;

const DEFAULT_BATCH: usize = 100;

pub struct ElasticsearchSink {
    pub(crate) conf: wp_config::structure::io::Elasticsearch,
    pub(crate) table: String,
    pub(crate) batch: usize,
    pub(crate) proc_cnt: usize,
    pub(crate) values: VecDeque<(String, String)>, // (table, json)
}

impl ElasticsearchSink {
    pub fn new(conf: wp_config::structure::io::Elasticsearch, table: String) -> Self {
        Self {
            batch: conf.batch.unwrap_or(DEFAULT_BATCH),
            conf,
            table,
            proc_cnt: 0,
            values: Default::default(),
        }
    }

    async fn insert_values(
        conf: &wp_config::structure::io::Elasticsearch,
        body: Vec<u8>,
    ) -> SinkResult<()> {
        let uri = format!("{}/_bulk", conf.get_endpoint());
        let client = reqwest::Client::builder().build().map_err(|e| {
            SinkError::from(SinkReason::SinkError(format!(
                "es client build fail: {}",
                e
            )))
        })?;
        let resp = client
            .put(&uri)
            .basic_auth(&conf.username, Some(&conf.password))
            .header(CONTENT_TYPE, "application/x-ndjson")
            .body(body)
            .send()
            .await
            .map_err(|e| {
                SinkError::from(SinkReason::SinkError(format!("es bulk send fail: {}", e)))
            })?;
        if resp.status() != StatusCode::OK {
            let t = resp.text().await.unwrap_or_default();
            return Err(SinkError::from(SinkReason::SinkError(format!(
                "es bulk fail: {}",
                t
            ))));
        }
        Ok(())
    }
}

#[async_trait]
impl AsyncCtrl for ElasticsearchSink {
    async fn stop(&mut self) -> SinkResult<()> {
        if self.values.is_empty() {
            return Ok(());
        }
        let mut buf = Vec::new();
        while let Some((table, json)) = self.values.pop_front() {
            let header = format!(
                "{{\"index\":{{\"_index\":\"{}\",\"_type\":\"_doc\"}}}}\n",
                table
            );
            buf.extend_from_slice(header.as_bytes());
            buf.extend_from_slice(json.as_bytes());
            buf.extend_from_slice(b"\n");
        }
        Self::insert_values(&self.conf, buf).await
    }
    async fn reconnect(&mut self) -> SinkResult<()> {
        Ok(())
    }
}

#[async_trait]
impl AsyncRecordSink for ElasticsearchSink {
    async fn sink_record(&mut self, data: &DataRecord) -> SinkResult<()> {
        let j = Json;
        let val = j.format_record(data).to_string();
        self.proc_cnt += 1;
        self.values.push_back((self.table.clone(), val));
        if self.proc_cnt.is_multiple_of(self.batch) {
            let mut buf = Vec::new();
            while let Some((table, json)) = self.values.pop_front() {
                let header = format!(
                    "{{\"index\":{{\"_index\":\"{}\",\"_type\":\"_doc\"}}}}\n",
                    table
                );
                buf.extend_from_slice(header.as_bytes());
                buf.extend_from_slice(json.as_bytes());
                buf.extend_from_slice(b"\n");
            }
            Self::insert_values(&self.conf, buf).await?;
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
impl AsyncRawDataSink for ElasticsearchSink {
    async fn sink_str(&mut self, _data: &str) -> SinkResult<()> {
        Err(SinkError::from(SinkReason::SinkError(
            "Elasticsearch sink does not support raw input; route TDC records".into(),
        )))
    }
    async fn sink_bytes(&mut self, _data: &[u8]) -> SinkResult<()> {
        Err(SinkError::from(SinkReason::SinkError(
            "Elasticsearch sink does not support raw bytes input; route TDC records".into(),
        )))
    }

    async fn sink_str_batch(&mut self, _data: Vec<&str>) -> SinkResult<()> {
        Err(SinkError::from(SinkReason::SinkError(
            "Elasticsearch sink does not support raw batch input; route TDC records".into(),
        )))
    }

    async fn sink_bytes_batch(&mut self, _data: Vec<&[u8]>) -> SinkResult<()> {
        Err(SinkError::from(SinkReason::SinkError(
            "Elasticsearch sink does not support raw batch input; route TDC records".into(),
        )))
    }
}
