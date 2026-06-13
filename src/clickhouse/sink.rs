use super::config::ClickHouseSinkConfig;
use crate::utils::fmt::{BatchFormat, fmt_strs};
use crate::utils::time_stat_utils::TimeStatUtils;
use crate::utils::{Protocol, arrow_fmt::records_to_arrow_ipc};
use async_trait::async_trait;
use clickhouse::Client;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use wp_connector_api::SinkErrorOwe;
use wp_connector_api::{
    AsyncCtrl, AsyncRawDataSink, AsyncRecordSink, SinkError, SinkReason, SinkResult,
};
use wp_model_core::model::DataRecord;

// 全局原子计数器，用于生成唯一的实例 ID
static INSTANCE_COUNTER: AtomicU64 = AtomicU64::new(0);

static ARROW_HTTP_CLIENT: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();

fn arrow_http_client() -> &'static reqwest::Client {
    ARROW_HTTP_CLIENT.get_or_init(reqwest::Client::new)
}

/// ClickHouse Sink 实现，负责将数据记录批量写入 ClickHouse
pub struct ClickHouseSink {
    client: Client,            // ClickHouse 客户端
    database: String,          // 数据库名称
    table: String,             // 目标表名称
    max_retries: i32,          // 最大重试次数
    instance_id: u64,          // 实例唯一 ID
    time_stats: TimeStatUtils, // 性能统计工具
    endpoint: String,          // Arrow 路径用
    ch_user: String,           // Arrow 路径 Basic Auth
    ch_password: String,       // Arrow 路径 Basic Auth
    protocol: Protocol,
}

impl ClickHouseSink {
    /// 创建新的 ClickHouseSink 实例
    ///
    /// # Arguments
    /// * `config` - ClickHouse 连接与写入配置
    ///
    pub async fn new(config: ClickHouseSinkConfig) -> SinkResult<Self> {
        // 构建 ClickHouse 客户端
        let client = Client::default()
            .with_url(&config.endpoint)
            .with_database(&config.database)
            .with_user(&config.username)
            .with_password(&config.password);

        // 从全局原子变量获取递增的实例 ID
        let instance_id = INSTANCE_COUNTER.fetch_add(1, Ordering::SeqCst);

        Ok(Self {
            client,
            database: config.database,
            table: config.table,
            max_retries: config.max_retries,
            instance_id,
            time_stats: TimeStatUtils::new(),
            endpoint: config.endpoint.clone(),
            ch_user: config.username.clone(),
            ch_password: config.password.clone(),
            protocol: Protocol::default(),
        })
    }

    pub fn set_protocol(&mut self, protocol: Protocol) {
        self.protocol = protocol;
    }

    /// 将批量记录转换为 NDJSON 格式（每行一个 JSON 对象）
    ///
    /// # Arguments
    /// * `records` - 数据记录列表
    ///
    /// # Returns
    /// * `SinkResult<String>` - NDJSON 字符串
    fn records_to_ndjson(&self, records: &[Arc<DataRecord>]) -> SinkResult<String> {
        Ok(fmt_strs(records.to_vec(), BatchFormat::Ndjson))
    }

    /// 执行批量插入请求（使用同步插入确保立即捕获错误）
    ///
    /// # Arguments
    /// * `ndjson` - NDJSON 格式的数据
    /// * `row_count` - 行数
    ///
    /// # Returns
    /// * `SinkResult<()>` - 成功或错误
    async fn insert_batch(&self, ndjson: String, row_count: usize) -> SinkResult<()> {
        let mut retries = 0;
        let max_retries = if self.max_retries < 0 {
            i32::MAX
        } else {
            self.max_retries
        };

        loop {
            // 使用原始 SQL 查询插入 JSONEachRow 格式数据，使用完整的表名（database.table）
            let query = format!(
                "INSERT INTO {}.{} FORMAT JSONEachRow\n{}",
                self.database, self.table, ndjson
            );

            // 使用同步插入设置，确保立即返回错误
            // async_insert=0: 禁用异步插入
            // wait_for_async_insert=0: 不等待异步插入（因为已禁用）
            match self
                .client
                .query(&query)
                .with_setting("async_insert", "0")
                .with_setting("wait_for_async_insert", "0")
                .execute()
                .await
            {
                Ok(_) => {
                    log::info!(
                        "ClickHouseSink-{}: successfully inserted {} rows",
                        self.instance_id,
                        row_count
                    );
                    return Ok(());
                }
                Err(e) => {
                    let error_msg = e.to_string();

                    // 检查是否是客户端错误（不重试）
                    if error_msg.contains("Code: 62") // Syntax error
                        || error_msg.contains("Code: 60") // Table doesn't exist
                        || error_msg.contains("Code: 47") // Unknown identifier
                        || error_msg.contains("Code: 16") // No such column
                        || error_msg.contains("Code: 10") // Cannot parse input
                        || error_msg.contains("Code: 53") // Type mismatch
                        || error_msg.contains("Code: 117") // Unknown type
                        || error_msg.contains("Code: 27")
                    // Cannot parse
                    {
                        return Err(sink_error(format!("schema/data error: {}", error_msg)));
                    }

                    // 服务器错误或网络错误，重试
                    log::warn!(
                        "ClickHouseSink-{}: insert failed: {}, retry={}/{}",
                        self.instance_id,
                        error_msg,
                        retries + 1,
                        max_retries
                    );

                    retries += 1;
                    if retries >= max_retries {
                        return Err(sink_error(format!(
                            "max retries ({}) exceeded: {}",
                            max_retries, error_msg
                        )));
                    }

                    // 指数退避
                    let backoff_ms = 1000 * 2_u64.pow(retries.min(10) as u32);
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                }
            }
        }
    }
}

#[async_trait]
impl AsyncRecordSink for ClickHouseSink {
    async fn sink_record(&mut self, data: &DataRecord) -> SinkResult<()> {
        self.sink_records(vec![Arc::new(data.clone())]).await
    }

    async fn sink_records(&mut self, data: Vec<Arc<DataRecord>>) -> SinkResult<()> {
        if data.is_empty() {
            return Ok(());
        }
        self.time_stats.start_stat(data.len() as u64);

        if self.protocol == Protocol::Arrow {
            let ipc_bytes = records_to_arrow_ipc(&data).owe_sink("arrow ipc")?;
            let query = format!("INSERT INTO {}.{} FORMAT Arrow", self.database, self.table)
                .replace(' ', "+");
            let url = format!("{}/?query={}", self.endpoint, query);
            let resp = arrow_http_client()
                .post(&url)
                .basic_auth(&self.ch_user, Some(&self.ch_password))
                .body(ipc_bytes)
                .send()
                .await
                .map_err(|e| sink_error(format!("arrow http: {e}")))?;
            if !resp.status().is_success() {
                return Err(sink_error(format!(
                    "arrow insert failed: {} {}",
                    resp.status(),
                    resp.text().await.unwrap_or_default()
                )));
            }
            self.time_stats.end_stat();
            self.time_stats
                .println(&format!("ClickHouseSink-{}", self.instance_id));
            return Ok(());
        }

        let ndjson = self.records_to_ndjson(&data)?;
        let row_count = data.len();
        self.insert_batch(ndjson, row_count).await?;
        self.time_stats.end_stat();
        self.time_stats
            .println(&format!("ClickHouseSink-{}", self.instance_id));
        Ok(())
    }
}

#[async_trait]
impl AsyncRawDataSink for ClickHouseSink {
    async fn sink_str(&mut self, _data: &str) -> SinkResult<()> {
        Err(sink_error("clickhouse sink does not accept raw text input"))
    }

    async fn sink_bytes(&mut self, _data: &[u8]) -> SinkResult<()> {
        Err(sink_error("clickhouse sink does not accept raw byte input"))
    }

    async fn sink_str_batch(&mut self, _data: Vec<&str>) -> SinkResult<()> {
        Err(sink_error(
            "clickhouse sink does not accept raw batch input",
        ))
    }

    async fn sink_bytes_batch(&mut self, _data: Vec<&[u8]>) -> SinkResult<()> {
        Err(sink_error(
            "clickhouse sink does not accept raw batch byte input",
        ))
    }
}

#[async_trait]
impl AsyncCtrl for ClickHouseSink {
    async fn stop(&mut self) -> SinkResult<()> {
        // ClickHouse 客户端无需显式清理
        Ok(())
    }

    async fn reconnect(&mut self) -> SinkResult<()> {
        // ClickHouse 客户端会自动重连
        Ok(())
    }
}

/// 统一封装 sink 层错误
fn sink_error(msg: impl Into<String>) -> SinkError {
    SinkReason::sink(msg)
}
