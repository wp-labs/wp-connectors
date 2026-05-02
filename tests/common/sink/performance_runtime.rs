#![allow(dead_code)]

use super::sink_info::SinkInfo;
use crate::common::component_tools::{
    ComponentTool, RuntimeReason, RuntimeResult, ToolReason,
};
use orion_error::conversion_ext::ConvStructError;
use orion_error::StructError;
use rand::RngExt;
use std::path::PathBuf;
use std::sync::{
    Arc,
    atomic::{AtomicI64, AtomicUsize, Ordering},
};
use std::time::Instant;
use sysinfo::{
    MINIMUM_CPU_UPDATE_INTERVAL, ProcessRefreshKind, ProcessesToUpdate, RefreshKind, System,
    get_current_pid,
};
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time::{Duration, sleep};
use wp_connector_api::{SinkBuildCtx, SinkFactory, SinkSpec};
use wp_model_core::model::{DataField, DataRecord};

static NEXT_PERF_RECORD_ID: AtomicI64 = AtomicI64::new(1);

const DEFAULT_TOTAL_RECORDS: usize = 3_000_000;
const DEFAULT_TASK_COUNT: usize = 4;
const DEFAULT_BATCH_SIZE: usize = 10_000;
const DEFAULT_PROGRESS_INTERVAL_SECS: u64 = 5;

thread_local! {
    static RNG: std::cell::RefCell<rand::rngs::ThreadRng> = std::cell::RefCell::new(rand::rng());
}

fn runtime_err(msg: impl Into<String>) -> StructError<RuntimeReason> {
    StructError::from(RuntimeReason::from(ToolReason::Script(msg.into())))
}

#[derive(Clone, Copy, Debug)]
pub struct SinkPerformanceConfig {
    pub total_records: usize,
    pub task_count: usize,
    pub batch_size: usize,
    pub progress_interval: Duration,
}

impl SinkPerformanceConfig {
    pub fn new() -> Self { Self::default() }
    pub fn with_total_records(mut self, v: usize) -> Self { self.total_records = v; self }
    pub fn with_task_count(mut self, v: usize) -> Self { self.task_count = v; self }
    pub fn with_batch_size(mut self, v: usize) -> Self { self.batch_size = v; self }
    pub fn with_progress_interval(mut self, v: Duration) -> Self { self.progress_interval = v; self }
}

impl Default for SinkPerformanceConfig {
    fn default() -> Self {
        Self {
            total_records: DEFAULT_TOTAL_RECORDS,
            task_count: DEFAULT_TASK_COUNT,
            batch_size: DEFAULT_BATCH_SIZE,
            progress_interval: Duration::from_secs(DEFAULT_PROGRESS_INTERVAL_SECS),
        }
    }
}

#[derive(Default)]
struct PerformanceCounters {
    sent_records: AtomicUsize,
    sent_batches: AtomicUsize,
    failed_batches: AtomicUsize,
}

#[derive(Debug, Default)]
struct MonitorSummary {
    peak_cpu: f32,
    peak_memory: u64,
    avg_cpu: f32,
    avg_memory: u64,
}

pub struct SinkPerformanceRuntime<T: ComponentTool, F: SinkFactory> {
    component_tool: T,
    sink_infos: Vec<SinkInfo<F>>,
    config: SinkPerformanceConfig,
}

impl<T: ComponentTool + Sync, F: SinkFactory + Sync> SinkPerformanceRuntime<T, F> {
    pub fn new(component_tool: T, sink_infos: Vec<SinkInfo<F>>, config: SinkPerformanceConfig) -> Self {
        Self { component_tool, sink_infos, config }
    }

    pub async fn run(&self) -> RuntimeResult<()> {
        println!("启动性能测试环境...");
        self.component_tool.setup_and_up().await.map_err(|e| e.conv())?;

        for (idx, sink_info) in self.sink_infos.iter().enumerate() {
            let kind = sink_info.factory().kind();
            let display_name = format_display_name(kind, sink_info.test_name(), idx);
            println!("\n========== 性能测试 Sink: {display_name} =========");
            sink_info.wait_ready().await.map_err(|e| runtime_err(format!("{e}")))?;
            println!("执行初始化...");
            sink_info.init().await.map_err(|e| runtime_err(format!("{e}")))?;
            self.run_single_sink(idx, &display_name, sink_info).await?;
        }

        println!("\n清理性能测试环境...");
        self.component_tool.down().await.map_err(|e| e.conv())?;
        Ok(())
    }

    async fn run_single_sink(
        &self, _idx: usize, display_name: &str, sink_info: &SinkInfo<F>,
    ) -> RuntimeResult<()> {
        let kind = sink_info.factory().kind();
        let count_before = if sink_info.has_count_fn() {
            let c = sink_info.count().await.map_err(|e| runtime_err(format!("{e}")))?;
            println!("发送前数量: {c}");
            Some(c)
        } else {
            println!("未配置 count_fn，跳过发送前数量统计");
            None
        };

        let base_spec = SinkSpec {
            group: "performance_test".to_string(),
            name: format!("perf_{display_name}"),
            kind: kind.to_string(),
            connector_id: format!("perf_{display_name}"),
            params: sink_info.params().clone(),
            filter: None,
        };

        let records_per_task = self.config.total_records / self.config.task_count;
        let remainder = self.config.total_records % self.config.task_count;

        let mut task_specs = Vec::with_capacity(self.config.task_count);
        let ctx = SinkBuildCtx::new(PathBuf::from("."));
        for task_id in 0..self.config.task_count {
            let assigned = records_per_task + usize::from(task_id < remainder);
            let spec = SinkSpec {
                name: format!("{}_task_{task_id}", base_spec.name),
                connector_id: format!("{}_task_{task_id}", base_spec.connector_id),
                ..base_spec.clone()
            };
            let sink = sink_info.factory().build(&spec, &ctx).await.map_err(|e| e.conv())?;
            task_specs.push((task_id, assigned, sink));
        }

        let counters = Arc::new(PerformanceCounters::default());
        let stop_monitor = Arc::new(Notify::new());
        let start = Instant::now();
        let monitor = self.spawn_monitor(counters.clone(), stop_monitor.clone(), start);

        let mut handles = Vec::with_capacity(self.config.task_count);
        for (task_id, assigned, mut sink) in task_specs {
            let counters = counters.clone();
            let batch_size = self.config.batch_size;
            handles.push(tokio::spawn(async move {
                let mut sent = 0usize;
                while sent < assigned {
                    let current_batch = batch_size.min(assigned - sent);
                    let records = create_test_records(current_batch);
                    match sink.sink.sink_records(records).await {
                        Ok(_) => {
                            sent += current_batch;
                            counters.sent_records.fetch_add(current_batch, Ordering::Relaxed);
                            counters.sent_batches.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(err) => {
                            counters.failed_batches.fetch_add(1, Ordering::Relaxed);
                            return Err(runtime_err(format!(
                                "任务 {task_id} 发送失败（sent={sent}, batch_size={current_batch}）: {err}"
                            )));
                        }
                    }
                }
                Ok(())
            }));
        }

        let mut task_failures = 0usize;
        for handle in handles {
            match handle.await {
                Ok(Ok(())) => {}
                Ok(Err(err)) => { task_failures += 1; eprintln!("❌ 性能测试任务失败: {err}"); }
                Err(err) => { task_failures += 1; eprintln!("❌ 性能测试任务 panic/cancelled: {err}"); }
            }
        }

        stop_monitor.notify_one();
        let monitor_summary = match monitor.await {
            Ok(s) => s,
            Err(e) => { eprintln!("⚠️ 性能监控任务结束异常: {e}"); MonitorSummary::default() }
        };

        let elapsed = start.elapsed();
        let sent_records = counters.sent_records.load(Ordering::Relaxed);
        let sent_batches = counters.sent_batches.load(Ordering::Relaxed);
        let failed_batches = counters.failed_batches.load(Ordering::Relaxed);
        let avg_eps = sent_records as f64 / elapsed.as_secs_f64().max(1.0);

        println!("\n========== 性能测试结果 ({kind}) =========");
        println!("总发送记录数: {sent_records}");
        println!("总发送批次数: {sent_batches}");
        println!("失败批次数: {failed_batches}");
        println!("失败任务数: {task_failures}");
        println!("总耗时: {:.2}s", elapsed.as_secs_f64());
        println!("平均 EPS: {avg_eps:.0}");
        println!("峰值 CPU: {:.2}%", monitor_summary.peak_cpu);
        println!("平均 CPU: {:.2}%", monitor_summary.avg_cpu);
        println!("峰值内存: {}", format_memory(monitor_summary.peak_memory));
        println!("平均内存: {}", format_memory(monitor_summary.avg_memory));

        if let Some(before) = count_before {
            let after = sink_info.count().await.map_err(|e| runtime_err(format!("{e}")))?;
            let diff = after - before;
            println!("数量校验新增: {diff}");
            if diff < sent_records as i64 {
                return Err(runtime_err(format!(
                    "性能测试数量校验失败，预期至少新增 {sent_records} 条，实际新增 {diff} 条"
                )));
            }
        }

        if task_failures > 0 {
            return Err(runtime_err(format!("性能测试存在失败任务: {task_failures}")));
        }
        Ok(())
    }

    fn spawn_monitor(
        &self, counters: Arc<PerformanceCounters>, stop: Arc<Notify>, start: Instant,
    ) -> JoinHandle<MonitorSummary> {
        let interval = self.config.progress_interval;
        tokio::spawn(async move {
            let Ok(pid) = get_current_pid() else {
                return MonitorSummary::default();
            };
            let refresh_kind = RefreshKind::nothing()
                .with_processes(ProcessRefreshKind::nothing().with_cpu().with_memory());
            let mut system = System::new_with_specifics(refresh_kind);
            system.refresh_processes_specifics(
                ProcessesToUpdate::Some(&[pid]), false,
                ProcessRefreshKind::nothing().with_cpu().with_memory(),
            );
            sleep(MINIMUM_CPU_UPDATE_INTERVAL).await;

            let mut last_sent = 0usize;
            let mut peak_cpu = 0.0f32;
            let mut peak_memory = 0u64;
            let mut cpu_sum = 0.0f64;
            let mut memory_sum = 0u128;
            let mut samples = 0u64;

            loop {
                tokio::select! {
                    _ = sleep(interval) => {}
                    _ = stop.notified() => { break; }
                }
                system.refresh_processes_specifics(
                    ProcessesToUpdate::Some(&[pid]), false,
                    ProcessRefreshKind::nothing().with_cpu().with_memory(),
                );
                let sent = counters.sent_records.load(Ordering::Relaxed);
                let total_eps = sent as f64 / start.elapsed().as_secs_f64().max(1.0);
                let current_eps = (sent.saturating_sub(last_sent)) as f64 / interval.as_secs_f64();
                last_sent = sent;

                if let Some(process) = system.process(pid) {
                    let cpu = process.cpu_usage();
                    let memory = process.memory();
                    peak_cpu = peak_cpu.max(cpu);
                    peak_memory = peak_memory.max(memory);
                    cpu_sum += cpu as f64;
                    memory_sum += memory as u128;
                    samples += 1;
                    println!(
                        "[性能进度] sent={sent}, batches={}, failed={}, eps={current_eps:.0}/{total_eps:.0}, cpu={cpu:.2}%, mem={}",
                        counters.sent_batches.load(Ordering::Relaxed),
                        counters.failed_batches.load(Ordering::Relaxed),
                        format_memory(memory)
                    );
                } else {
                    println!(
                        "[性能进度] sent={sent}, batches={}, failed={}",
                        counters.sent_batches.load(Ordering::Relaxed),
                        counters.failed_batches.load(Ordering::Relaxed)
                    );
                }
            }
            let avg_cpu = if samples > 0 { (cpu_sum / samples as f64) as f32 } else { 0.0 };
            let avg_memory = if samples > 0 { (memory_sum / samples as u128) as u64 } else { 0 };
            MonitorSummary { peak_cpu, peak_memory, avg_cpu, avg_memory }
        })
    }
}

fn format_display_name(kind: &str, test_name: Option<&str>, idx: usize) -> String {
    match test_name {
        Some(n) if !n.trim().is_empty() => format!("{kind}_{n}_{}", idx + 1),
        _ => format!("{kind}_{}", idx + 1),
    }
}

fn format_memory(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut value = bytes as f64;
    let mut idx = 0usize;
    while value >= 1024.0 && idx < UNITS.len() - 1 { value /= 1024.0; idx += 1; }
    format!("{value:.2} {}", UNITS[idx])
}

fn create_test_records(count: usize) -> Vec<Arc<DataRecord>> {
    let start_id = NEXT_PERF_RECORD_ID.fetch_add(count as i64, Ordering::SeqCst);
    const TS: &str = "2024-03-02 10:00:00";
    let size: i64 = RNG.with(|rng| rng.borrow_mut().random());
    (0..count)
        .map(|i| {
            let id = start_id + i as i64;
            let mut r = DataRecord::default();
            r.append(DataField::from_digit("wp_event_id", id));
            r.append(DataField::from_chars("wp_src_key", format!("performance_test_{id}")));
            r.append(DataField::from_chars("sip", "192.168.1.100"));
            r.append(DataField::from_chars("timestamp", TS));
            r.append(DataField::from_chars("http/request", format!("GET /api/perf/{id} HTTP/1.1")));
            r.append(DataField::from_digit("status", 200));
            r.append(DataField::from_digit("size", size));
            r.append(DataField::from_chars("referer", format!("perf-{id:06}")));
            r.append(DataField::from_chars("http/agent", "Mozilla/5.0 (Performance Test)"));
            Arc::new(r)
        })
        .collect()
}
