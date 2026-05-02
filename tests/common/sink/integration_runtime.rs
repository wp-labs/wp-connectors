#![allow(dead_code)]

use super::sink_info::SinkInfo;
use crate::common::component_tools::{
    ComponentTool, RuntimeReason, RuntimeResult, ToolReason,
};
use orion_error::conversion_ext::ConvStructError;
use orion_error::StructError;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use wp_connector_api::{SinkBuildCtx, SinkFactory, SinkSpec};

static NEXT_TEST_RECORD_ID: AtomicI64 = AtomicI64::new(1);
const TEST_RECORD_COUNT: usize = 3;

fn runtime_err(msg: impl Into<String>) -> StructError<RuntimeReason> {
    StructError::from(RuntimeReason::from(ToolReason::Script(msg.into())))
}

pub struct SinkIntegrationRuntime<T: ComponentTool, F: SinkFactory> {
    component_tool: T,
    sink_infos: Vec<SinkInfo<F>>,
}

impl<T: ComponentTool + Sync, F: SinkFactory> SinkIntegrationRuntime<T, F> {
    pub fn new(component_tool: T, sink_infos: Vec<SinkInfo<F>>) -> Self {
        Self { component_tool, sink_infos }
    }

    pub async fn run(&self, clear: bool) -> RuntimeResult<()> {
        println!("启动组件...");
        self.component_tool.setup_and_up().await.map_err(|e| e.conv())?;

        for (idx, sink_info) in self.sink_infos.iter().enumerate() {
            let kind = sink_info.factory().kind();
            let display_name = format_display_name(kind, sink_info.test_name(), idx);
            println!("\n========== 测试 Sink: {display_name} =========");

            sink_info.wait_ready().await.map_err(|e| runtime_err(format!("{e}")))?;
            println!("执行初始化...");
            sink_info.init().await.map_err(|e| runtime_err(format!("{e}")))?;

            let spec = SinkSpec {
                group: "integration_test".to_string(),
                name: display_name.clone(),
                kind: kind.to_string(),
                connector_id: display_name.clone(),
                params: sink_info.params().clone(),
                filter: None,
            };

            let ctx = SinkBuildCtx::new(PathBuf::from("."));
            let mut sink = sink_info.factory().build(&spec, &ctx).await.map_err(|e| e.conv())?;

            let count_before = sink_info.count().await.map_err(|e| runtime_err(format!("{e}")))?;
            println!("发送前数量: {count_before}");
            let test_records = self.create_test_records(TEST_RECORD_COUNT);
            println!("发送 {TEST_RECORD_COUNT} 条数据...");
            sink.sink
                .sink_records(test_records.iter().cloned().map(Arc::new).collect())
                .await
                .map_err(|e| e.conv())?;

            tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

            let count_after = sink_info.count().await.map_err(|e| runtime_err(format!("{e}")))?;
            let diff = count_after - count_before;
            if diff == TEST_RECORD_COUNT as i64 {
                println!("✓ 数据发送成功，新增 {diff} 条记录");
            } else {
                return Err(runtime_err(format!(
                    "❌ 数据发送失败，预期新增 {TEST_RECORD_COUNT} 条，实际新增 {diff} 条"
                )));
            }

            println!("\n重启外部组件...");
            self.component_tool.restart().await.map_err(|e| e.conv())?;
            self.component_tool.wait_started().await.map_err(|e| e.conv())?;
            sink_info.wait_ready().await.map_err(|e| runtime_err(format!("{e}")))?;

            println!("重启后再次发送数据...");
            let count_before_restart = sink_info.count().await.map_err(|e| runtime_err(format!("{e}")))?;

            let mut sink = sink_info.factory().build(&spec, &ctx).await.map_err(|e| e.conv())?;
            let retry_records = self.create_test_records(TEST_RECORD_COUNT);
            sink.sink
                .sink_records(retry_records.iter().cloned().map(Arc::new).collect())
                .await
                .map_err(|e| e.conv())?;

            tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

            let count_after_restart = sink_info.count().await.map_err(|e| runtime_err(format!("{e}")))?;
            let diff_restart = count_after_restart - count_before_restart;
            if diff_restart >= TEST_RECORD_COUNT as i64 {
                println!("✓ 重启后数据发送成功，新增 {diff_restart} 条记录");
            } else {
                return Err(runtime_err(format!(
                    "❌ 重启后数据发送失败，预期新增 {TEST_RECORD_COUNT} 条，实际新增 {diff_restart} 条"
                )));
            }
        }

        if clear {
            println!("\n清理环境...");
            self.component_tool.down().await.map_err(|e| e.conv())?;
        }
        Ok(())
    }

    fn create_test_records(&self, count: usize) -> Vec<wp_model_core::model::DataRecord> {
        use wp_model_core::model::{DataField, DataRecord};
        let start_id = NEXT_TEST_RECORD_ID.fetch_add(count as i64, Ordering::SeqCst);
        (0..count)
            .map(|i| {
                let id = start_id + i as i64;
                let mut record = DataRecord::default();
                record.append(DataField::from_digit("wp_event_id", id));
                record.append(DataField::from_chars("wp_src_key", format!("integration_test_{id}")));
                record.append(DataField::from_chars("sip", "192.168.1.100"));
                record.append(DataField::from_chars(
                    "timestamp",
                    chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string(),
                ));
                record.append(DataField::from_chars("http/request", format!("GET /api/test/{id} HTTP/1.1")));
                record.append(DataField::from_digit("status", 200));
                record.append(DataField::from_digit("size", 1024 + i as i64));
                record.append(DataField::from_chars("referer", format!("{id:06}")));
                record.append(DataField::from_chars("http/agent", "Mozilla/5.0 (Integration Test)"));
                record
            })
            .collect()
    }
}

fn format_display_name(kind: &str, test_name: Option<&str>, idx: usize) -> String {
    match test_name {
        Some(name) if !name.trim().is_empty() => format!("{kind}_{name}_{}", idx + 1),
        _ => format!("{kind}_{}", idx + 1),
    }
}
