//! Elasticsearch Sink 实现 - 使用 Bulk API
//!
//! # Bulk API
//!
//! 本实现使用 Elasticsearch 的 HTTP Bulk API 进行批量数据导入：
//! - 高性能批量导入
//! - NDJSON 格式
//! - 无 SQL 注入风险（使用 JSON 格式）
//! - 支持负载均衡和故障转移

use crate::elasticsearch::config::ElasticsearchSinkConfig;
use crate::utils::fmt::{BatchFormat, fmt_bytes};
use crate::utils::time_stat_utils::TimeStatUtils;
use async_trait::async_trait;
use reqwest::Client;
use serde::Deserialize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use wp_connector_api::{
    AsyncCtrl, AsyncRawDataSink, AsyncRecordSink, SinkError, SinkReason, SinkResult,
};
use wp_model_core::model::DataRecord;

// 全局原子计数器，用于生成唯一的实例 ID
static INSTANCE_COUNTER: AtomicU64 = AtomicU64::new(0);

pub struct ElasticsearchSink {
    client: Client,
    url: String,   // 预先构建的完整 URL
    index: String, // 索引名称
    username: String,
    password: String,
    max_retries: i32,
    instance_id: u64,          // 实例唯一 ID
    time_stats: TimeStatUtils, // 时间统计工具
}

#[derive(Debug, Deserialize)]
struct BulkResponse {
    #[serde(default)]
    errors: bool,
    #[serde(default)]
    items: Vec<BulkItemResponse>,
}

#[derive(Debug, Deserialize)]
struct BulkItemResponse {
    #[serde(default)]
    index: Option<BulkItemResult>,
}

#[derive(Debug, Deserialize)]
struct BulkItemResult {
    #[serde(default)]
    status: u16,
    #[serde(default)]
    error: Option<serde_json::Value>,
}

impl ElasticsearchSink {
    /// 构建 Elasticsearch Sink，使用 Bulk API
    ///
    /// # Arguments
    /// * `config` - Elasticsearch 连接与写入配置
    ///
    /// # Returns
    /// * `anyhow::Result<Self>` - 成功返回初始化后的 sink
    pub async fn new(config: ElasticsearchSinkConfig) -> anyhow::Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(config.timeout_secs))
            .no_proxy() // 禁用所有代理
            .build()?;

        // 预先构建完整的 Bulk API URL
        let url = format!("{}/_bulk", config.endpoint());

        // 从全局原子变量获取递增的实例 ID
        let instance_id = INSTANCE_COUNTER.fetch_add(1, Ordering::SeqCst);

        Ok(Self {
            client,
            url,
            index: config.index,
            username: config.username,
            password: config.password,
            max_retries: config.max_retries,
            instance_id,
            time_stats: TimeStatUtils::new(),
        })
    }

    /// 将批量记录转换为 NDJSON 格式（Bulk API 格式）
    ///
    /// # Arguments
    /// * `records` - 数据记录列表
    ///
    /// # Returns
    /// * `SinkResult<Vec<u8>>` - NDJSON 字节流
    fn records_to_ndjson(&self, records: &[Arc<DataRecord>]) -> SinkResult<Vec<u8>> {
        let mut ndjson = Vec::new();

        for record in records {
            // 操作行：指定索引操作
            let action = serde_json::json!({
                "index": {
                    "_index": &self.index
                }
            });
            let action_bytes = serde_json::to_vec(&action)
                .map_err(|e| sink_error(format!("json serialization failed for action: {}", e)))?;
            ndjson.extend_from_slice(&action_bytes);
            ndjson.push(b'\n');

            // 文档行：实际数据
            let doc_bytes = fmt_bytes(vec![Arc::clone(record)], BatchFormat::Ndjson);
            ndjson.extend_from_slice(&doc_bytes);
            ndjson.push(b'\n');
        }

        Ok(ndjson)
    }

    /// 执行 Bulk 请求
    ///
    /// # Arguments
    /// * `ndjson` - NDJSON 格式的数据
    ///
    /// # Returns
    /// * `SinkResult<()>` - 成功或错误
    async fn bulk_request(&self, ndjson: Vec<u8>) -> SinkResult<()> {
        let mut retries = 0;
        let max_retries = if self.max_retries < 0 {
            i32::MAX
        } else {
            self.max_retries
        };

        loop {
            let request = self
                .client
                .post(&self.url)
                .basic_auth(&self.username, Some(&self.password))
                .header("Content-Type", "application/x-ndjson")
                .body(ndjson.clone());

            match request.send().await {
                Ok(response) => {
                    let status = response.status();
                    let body = response
                        .text()
                        .await
                        .unwrap_or_else(|_| "failed to read response".to_string());

                    if status.is_success() {
                        // 解析响应
                        return self.parse_bulk_response(&body);
                    } else if status.is_client_error() {
                        // 4xx 错误不重试
                        return Err(sink_error(format!(
                            "client error: status={}, body={}",
                            status, body
                        )));
                    } else {
                        // 5xx 错误重试
                        log::warn!(
                            "server error: status={}, body={}, retry={}/{}",
                            status,
                            body,
                            retries + 1,
                            max_retries
                        );
                    }
                }
                Err(e) => {
                    log::warn!(
                        "request failed: {}, retry={}/{}",
                        e,
                        retries + 1,
                        max_retries
                    );
                }
            }

            // 检查是否需要重试
            retries += 1;
            if retries >= max_retries {
                return Err(sink_error(format!(
                    "max retries ({}) exceeded",
                    max_retries
                )));
            }

            // 指数退避
            let backoff_ms = 1000 * 2_u64.pow(retries.min(10) as u32);
            tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
        }
    }

    /// 解析 Bulk API 响应
    ///
    /// # Arguments
    /// * `body` - 响应体
    ///
    /// # Returns
    /// * `SinkResult<()>` - 成功或错误
    fn parse_bulk_response(&self, body: &str) -> SinkResult<()> {
        match serde_json::from_str::<BulkResponse>(body) {
            Ok(resp) => {
                if !resp.errors {
                    log::info!("bulk request success: {} items", resp.items.len());
                    return Ok(());
                }

                // 有错误，记录详情
                let mut error_count = 0;
                for item in &resp.items {
                    if let Some(index_result) = &item.index
                        && let Some(error) = &index_result.error
                    {
                        log::error!(
                            "bulk operation failed: status={}, error={:?}",
                            index_result.status,
                            error
                        );
                        error_count += 1;
                    }
                }

                Err(sink_error(format!(
                    "bulk operation had {} partial failures",
                    error_count
                )))
            }
            Err(e) => {
                log::warn!("failed to parse response: {}, body: {}", e, body);
                Err(sink_error(format!("invalid response format: {}", e)))
            }
        }
    }
}

#[async_trait]
impl AsyncCtrl for ElasticsearchSink {
    async fn stop(&mut self) -> SinkResult<()> {
        // HTTP 客户端无需显式清理
        Ok(())
    }

    async fn reconnect(&mut self) -> SinkResult<()> {
        // HTTP 客户端无需显式重连
        Ok(())
    }
}

#[async_trait]
impl AsyncRecordSink for ElasticsearchSink {
    async fn sink_record(&mut self, data: &DataRecord) -> SinkResult<()> {
        self.sink_records(vec![Arc::new(data.clone())]).await
    }

    async fn sink_records(&mut self, data: Vec<Arc<DataRecord>>) -> SinkResult<()> {
        if data.is_empty() {
            return Ok(());
        }

        // 开始统计
        self.time_stats.start_stat(data.len() as u64);

        // 转换为 NDJSON
        let ndjson = self.records_to_ndjson(&data)?;

        // 发送批量请求
        self.bulk_request(ndjson).await?;

        // 结束统计
        self.time_stats.end_stat();

        // 打印统计信息
        self.time_stats
            .println(&format!("ElasticsearchSink-{}", self.instance_id));

        Ok(())
    }
}

#[async_trait]
impl AsyncRawDataSink for ElasticsearchSink {
    async fn sink_str(&mut self, _data: &str) -> SinkResult<()> {
        Err(sink_error(
            "elasticsearch sink does not accept raw text input",
        ))
    }

    async fn sink_bytes(&mut self, _data: &[u8]) -> SinkResult<()> {
        Err(sink_error(
            "elasticsearch sink does not accept raw byte input",
        ))
    }

    async fn sink_str_batch(&mut self, _data: Vec<&str>) -> SinkResult<()> {
        Err(sink_error(
            "elasticsearch sink does not accept raw batch input",
        ))
    }

    async fn sink_bytes_batch(&mut self, _data: Vec<&[u8]>) -> SinkResult<()> {
        Err(sink_error(
            "elasticsearch sink does not accept raw batch byte input",
        ))
    }
}

/// 统一封装 sink 层错误
fn sink_error(msg: impl Into<String>) -> SinkError {
    SinkError::from(SinkReason::Sink(msg.into()))
}
