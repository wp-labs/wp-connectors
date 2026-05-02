#![allow(dead_code)]

use super::source_info::{SourceInfo, SourceRunPhase};
use crate::common::component_tools::{
    ComponentTool, RuntimeReason, RuntimeResult, ToolReason,
};
use orion_error::conversion_ext::ConvStructError;
use orion_error::StructError;
use std::path::PathBuf;
use tokio::time::{Instant as TokioInstant, sleep, timeout};
use wp_connector_api::{
    SourceBuildCtx, SourceFactory, SourceHandle, SourceResult, SourceSpec,
};

fn runtime_err(msg: impl Into<String>) -> StructError<RuntimeReason> {
    StructError::from(RuntimeReason::from(ToolReason::Script(msg.into())))
}

pub struct SourceIntegrationRuntime<T: ComponentTool, F: SourceFactory> {
    component_tool: T,
    source_infos: Vec<SourceInfo<F>>,
}

impl<T: ComponentTool + Sync, F: SourceFactory> SourceIntegrationRuntime<T, F> {
    pub fn new(component_tool: T, source_infos: Vec<SourceInfo<F>>) -> Self {
        Self { component_tool, source_infos }
    }

    pub async fn run(&self, clear: bool) -> RuntimeResult<()> {
        println!("启动 Source 集成测试组件...");
        self.component_tool.setup_and_up().await.map_err(|e| e.conv())?;

        for (idx, source_info) in self.source_infos.iter().enumerate() {
            let kind = source_info.factory().kind();
            let display_name = format_display_name(kind, source_info.test_name(), idx);
            println!("\n========== 测试 Source: {} =========", display_name);

            source_info.wait_ready().await.map_err(|e| runtime_err(format!("{e}")))?;
            println!("执行初始化...");
            source_info.init().await.map_err(|e| runtime_err(format!("{e}")))?;

            self.run_phase(&display_name, SourceRunPhase::Initial, source_info).await?;

            if source_info.restart_verification() {
                println!("\n重启外部组件...");
                self.component_tool.restart().await.map_err(|e| e.conv())?;
                self.component_tool.wait_started().await.map_err(|e| e.conv())?;
                source_info.wait_ready().await.map_err(|e| runtime_err(format!("{e}")))?;
                self.run_phase(&display_name, SourceRunPhase::AfterRestart, source_info).await?;
            }
        }

        if clear {
            println!("\n清理 Source 集成测试环境...");
            self.component_tool.down().await.map_err(|e| e.conv())?;
        }
        Ok(())
    }

    async fn run_phase(
        &self,
        display_name: &str,
        phase: SourceRunPhase,
        source_info: &SourceInfo<F>,
    ) -> RuntimeResult<()> {
        let phase_label = match phase {
            SourceRunPhase::Initial => "首次运行",
            SourceRunPhase::AfterRestart => "重启后运行",
        };
        println!("{}: 构建 Source...", phase_label);

        let spec = SourceSpec {
            name: display_name.to_string(),
            kind: source_info.factory().kind().to_string(),
            connector_id: display_name.to_string(),
            params: source_info.params().clone(),
            tags: vec![],
        };

        let ctx = SourceBuildCtx::new(PathBuf::from("."));
        let service: SourceResult<_> = source_info.factory().build(&spec, &ctx).await;
        let mut service = service.map_err(|e| e.conv())?;
        if service.sources.is_empty() {
            return Err(runtime_err(format!("{display_name} 未返回任何 SourceHandle")));
        }

        let mut expected_events = 0usize;
        println!("{}: 发送测试数据（repeat={}）...", phase_label, source_info.input_repeat());
        for _ in 0..source_info.input_repeat() {
            expected_events += source_info.input().await.map_err(|e| runtime_err(format!("{e}")))?;
        }

        let collect_config = source_info.collect_config();
        println!(
            "{}: 收集事件（expected_events={expected_events}, timeout={}ms）...",
            phase_label, collect_config.timeout.as_millis()
        );

        let deadline = TokioInstant::now() + collect_config.timeout;
        let mut received = Vec::new();

        while TokioInstant::now() < deadline {
            let mut progress = false;
            for handle in &mut service.sources {
                let now = TokioInstant::now();
                if now >= deadline { break; }
                let remain = deadline.duration_since(now);
                match timeout(remain, handle.source.receive()).await {
                    Ok(Ok(batch)) => {
                        if batch.is_empty() {  } else { received.extend(batch); progress = true; }
                    }
                    Ok(Err(err)) => {
                        let m = err.to_string().to_ascii_lowercase();
                        if m.contains("notdata") || m.contains("no message received") {  continue; }
                        if m.contains("eof") { continue; }
                        let _ = close_all_sources(&mut service.sources).await;
                        return Err(err.conv());
                    }
                    Err(_) => {  }
                }
            }
            if !progress { sleep(collect_config.poll_interval).await; }
        }

        let _ = close_all_sources(&mut service.sources).await;

        if received.len() != expected_events {
            return Err(runtime_err(format!(
                "{display_name} 数量校验失败，预期 {expected_events} 条事件，实际收到 {} 条", received.len()
            )));
        }
        Ok(())
    }
}

async fn close_all_sources(sources: &mut [SourceHandle]) -> SourceResult<()> {
    for handle in sources {
        handle.source.close().await?;
    }
    Ok(())
}

fn format_display_name(kind: &str, test_name: Option<&str>, idx: usize) -> String {
    match test_name {
        Some(name) if !name.trim().is_empty() => format!("{kind}_{name}_{}", idx + 1),
        _ => format!("{kind}_{}", idx + 1),
    }
}
