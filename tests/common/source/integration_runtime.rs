#![allow(dead_code)]

use super::source_info::{SourceInfo, SourceRunPhase};
use crate::common::component_tools::ComponentTool;
use anyhow::Result;
use std::path::PathBuf;
use std::time::Instant;
use tokio::time::{Instant as TokioInstant, sleep, timeout};
use wp_connector_api::{SourceBuildCtx, SourceFactory, SourceHandle, SourceSpec};

/// Source 集成测试运行时。
pub struct SourceIntegrationRuntime<T: ComponentTool, F: SourceFactory> {
    component_tool: T,
    source_infos: Vec<SourceInfo<F>>,
}

impl<T: ComponentTool + Sync, F: SourceFactory> SourceIntegrationRuntime<T, F> {
    pub fn new(component_tool: T, source_infos: Vec<SourceInfo<F>>) -> Self {
        Self {
            component_tool,
            source_infos,
        }
    }

    pub async fn run(&self, clear_component: bool) -> Result<()> {
        println!("启动 Source 集成测试组件...");
        self.component_tool.setup_and_up().await?;

        for (idx, source_info) in self.source_infos.iter().enumerate() {
            let kind = source_info.factory().kind();
            let display_name = format_display_name(kind, source_info.test_name(), idx);
            println!("\n========== 测试 Source: {} =========", display_name);

            source_info.wait_ready().await?;
            println!("执行初始化...");
            source_info.init().await?;

            self.run_phase(&display_name, SourceRunPhase::Initial, source_info)
                .await?;

            if source_info.restart_verification() {
                println!("\n重启外部组件...");
                self.component_tool.restart().await?;
                self.component_tool.wait_started().await?;
                source_info.wait_ready().await?;
                self.run_phase(&display_name, SourceRunPhase::AfterRestart, source_info)
                    .await?;
            }
        }

        if clear_component {
            println!("\n清理 Source 集成测试环境...");
            self.component_tool.down().await?;
        }

        Ok(())
    }

    async fn run_phase(
        &self,
        display_name: &str,
        phase: SourceRunPhase,
        source_info: &SourceInfo<F>,
    ) -> Result<()> {
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
        let mut service = source_info.factory().build(&spec, &ctx).await?;
        let source_count = service.sources.len();
        if source_count == 0 {
            anyhow::bail!("{} 未返回任何 SourceHandle", display_name);
        }

        let mut expected_events = 0usize;
        println!(
            "{}: 发送测试数据（repeat={}）...",
            phase_label,
            source_info.input_repeat()
        );
        for _ in 0..source_info.input_repeat() {
            expected_events += source_info.input().await?;
        }

        let collect_config = source_info.collect_config();
        println!(
            "{}: 收集事件（expected_events={}, timeout={}ms, poll_interval={}ms）...",
            phase_label,
            expected_events,
            collect_config.timeout.as_millis(),
            collect_config.poll_interval.as_millis()
        );

        let started_at = Instant::now();
        let deadline = TokioInstant::now() + collect_config.timeout;
        let mut received_events = Vec::new();
        let mut receive_attempts = 0usize;
        let mut idle_count = 0usize;
        let mut eof_count = 0usize;

        while TokioInstant::now() < deadline {
            let mut had_progress = false;
            for handle in &mut service.sources {
                let now = TokioInstant::now();
                if now >= deadline {
                    break;
                }
                let remaining = deadline.duration_since(now);
                receive_attempts += 1;
                match timeout(remaining, handle.source.receive()).await {
                    Ok(Ok(batch)) => {
                        if batch.is_empty() {
                            idle_count += 1;
                        } else {
                            received_events.extend(batch);
                            had_progress = true;
                        }
                    }
                    Ok(Err(err)) => {
                        if is_not_data_error(&err.to_string()) {
                            idle_count += 1;
                            continue;
                        }
                        if is_eof_error(&err.to_string()) {
                            eof_count += 1;
                            continue;
                        }
                        close_all_sources(&mut service.sources).await?;
                        return Err(err.into());
                    }
                    Err(_) => {
                        idle_count += 1;
                    }
                }
            }

            if !had_progress {
                sleep(collect_config.poll_interval).await;
            }
        }

        let elapsed = started_at.elapsed();
        close_all_sources(&mut service.sources).await?;

        println!(
            "{}: 收集完成，sources={}, expected_events={}, events={}, attempts={}, idle={}, eof={}, elapsed={:.2}s",
            phase_label,
            source_count,
            expected_events,
            received_events.len(),
            receive_attempts,
            idle_count,
            eof_count,
            elapsed.as_secs_f64()
        );

        anyhow::ensure!(
            received_events.len() == expected_events,
            "{} 数量校验失败，预期 {} 条事件，实际收到 {} 条",
            display_name,
            expected_events,
            received_events.len()
        );

        Ok(())
    }
}

async fn close_all_sources(sources: &mut [SourceHandle]) -> Result<()> {
    for handle in sources {
        handle.source.close().await?;
    }
    Ok(())
}

fn is_not_data_error(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("notdata") || lower.contains("not data") || lower.contains("no message received")
}

fn is_eof_error(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("eof")
}

fn format_display_name(kind: &str, test_name: Option<&str>, idx: usize) -> String {
    match test_name {
        Some(name) if !name.trim().is_empty() => format!("{}_{}_{}", kind, name.trim(), idx + 1),
        _ => format!("{}_{}", kind, idx + 1),
    }
}
