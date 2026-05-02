//! Count Sink 实现 - 统计数据量
//!
//! # 功能
//!
//! 本实现用于统计接收到的数据记录数量：
//! - 统计当前批次的数据量
//! - 累计总数据量
//! - 使用唯一 ID 标识每个 sink 实例

use crate::utils::time_stat_utils::TimeStatUtils;
use async_trait::async_trait;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use wp_connector_api::{
    AsyncCtrl, AsyncRawDataSink, AsyncRecordSink, SinkError, SinkReason, SinkResult,
};
use wp_model_core::model::DataRecord;

// 全局原子计数器，用于生成唯一的实例 ID
static INSTANCE_COUNTER: AtomicU64 = AtomicU64::new(0);

pub struct CountSink {
    id: String,
    time_stats: TimeStatUtils,
}

impl CountSink {
    pub async fn new() -> SinkResult<Self> {
        // 获取当前 tokio 任务 ID
        let task_id = tokio::task::try_id()
            .map(|id| format!("{:?}", id))
            .unwrap_or_else(|| "_".to_string());

        // 从全局原子变量获取递增的实例编号
        let instance_num = INSTANCE_COUNTER.fetch_add(1, Ordering::SeqCst);

        // 拼接 ID: CountSink-{tokio异步id}_{实例编号}
        let id = format!("CountSink-{}_{}", task_id, instance_num);

        Ok(Self {
            id,
            time_stats: TimeStatUtils::new(),
        })
    }
}

#[async_trait]
impl AsyncCtrl for CountSink {
    async fn stop(&mut self) -> SinkResult<()> {
        wp_log::info_ctrl!("[{}] 停止", self.id);
        Ok(())
    }

    async fn reconnect(&mut self) -> SinkResult<()> {
        Ok(())
    }
}

#[async_trait]
impl AsyncRecordSink for CountSink {
    async fn sink_record(&mut self, data: &DataRecord) -> SinkResult<()> {
        self.sink_records(vec![Arc::new(data.clone())]).await
    }

    async fn sink_records(&mut self, data: Vec<Arc<DataRecord>>) -> SinkResult<()> {
        if data.is_empty() {
            return Ok(());
        }

        // 开始统计
        self.time_stats.start_stat(data.len() as u64);

        // 这里可以添加实际的业务逻辑
        // 目前只是统计，不做其他处理

        // 结束统计
        self.time_stats.end_stat();

        // 打印统计信息
        self.time_stats.println(&self.id);

        Ok(())
    }
}

#[async_trait]
impl AsyncRawDataSink for CountSink {
    async fn sink_str(&mut self, _data: &str) -> SinkResult<()> {
        Err(sink_error("count sink does not accept raw text input"))
    }

    async fn sink_bytes(&mut self, _data: &[u8]) -> SinkResult<()> {
        Err(sink_error("count sink does not accept raw byte input"))
    }

    async fn sink_str_batch(&mut self, _data: Vec<&str>) -> SinkResult<()> {
        Err(sink_error("count sink does not accept raw batch input"))
    }

    async fn sink_bytes_batch(&mut self, _data: Vec<&[u8]>) -> SinkResult<()> {
        Err(sink_error(
            "count sink does not accept raw batch byte input",
        ))
    }
}

/// 统一封装 sink 层错误。
fn sink_error(msg: impl Into<String>) -> SinkError {
    SinkError::from(SinkReason::Sink(msg.into()))
}
