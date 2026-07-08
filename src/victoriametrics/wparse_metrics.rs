use lazy_static::lazy_static;
use orion_exp::ValueGet0;
use prometheus::GaugeVec;
use prometheus::register_gauge_vec;
use sysinfo::ProcessRefreshKind;
use sysinfo::ProcessesToUpdate;
use sysinfo::System;
use wp_model_core::model::FieldStorage;

// 本地轻薄封装：将 Option<&FieldStorage> 包成本地类型，
// 以便实现 orion_exp::ValueGet0（避免孤儿规则限制）。
pub(crate) struct OptField<'a>(Option<&'a FieldStorage>);

impl<'a> ValueGet0<Value> for OptField<'a> {
    fn get_value(&self) -> Option<&Value> {
        self.0.map(|f| f.get_value())
    }
}

pub(crate) trait IntoOptField<'a> {
    fn opt(self) -> OptField<'a>;
}

impl<'a> IntoOptField<'a> for Option<&'a FieldStorage> {
    fn opt(self) -> OptField<'a> {
        OptField(self)
    }
}
use prometheus::{IntCounterVec, register_int_counter_vec};
use wp_model_core::model::DataRecord;
use wp_model_core::model::Value;

// ------------- metrics helpers -------------

/// 一次 sysinfo 刷新同时更新 CPU + 内存两个 gauge，避免重复的系统调用开销。
/// 在定时 flush 任务中调用，采样间隔即 flush_secs。
pub(crate) fn system_usage_stat(system: &mut System) {
    if let Some((cpu, mem)) = current_process_usage(system) {
        CPU_USAGE
            .with_label_values(&CpuMetrics::new().values())
            .set(cpu);
        MEMORY_USAGE
            .with_label_values(&MemoryMetrics::new().values())
            .set(mem);
    }
}

fn current_process_usage(system: &mut System) -> Option<(f64, f64)> {
    let pid = sysinfo::get_current_pid().ok()?;
    system.refresh_processes_specifics(
        ProcessesToUpdate::Some(&[pid]),
        true,
        ProcessRefreshKind::nothing().with_cpu().with_memory(),
    );
    let process = system.process(pid)?;
    Some((
        process.cpu_usage() as f64,
        process.memory() as f64 / 1024.0 / 1024.0,
    ))
}

pub(crate) fn source_values(data: &DataRecord) -> (RecvMetrics, i64) {
    let mut recv_metrics = RecvMetrics::new();
    let mut count = 0;
    if let Some(Value::Chars(f)) = data.get2("wp_source_type").map(|x| x.get_value()) {
        recv_metrics.source_type = f.to_string();
    }
    if let Some(Value::Chars(f)) = data.get2("target").map(|x| x.get_value()) {
        recv_metrics.source_name = f.to_string();
    }
    if let Some(Value::Chars(f)) = data.get2("wp_access_ip").map(|x| x.get_value()) {
        recv_metrics.source_name = f.to_string();
    }
    if let Some(Value::Digit(f)) = data.get2("total").map(|x| x.get_value()) {
        count = *f;
    }
    (recv_metrics, count)
}

pub(crate) fn parse_all(data: &DataRecord) -> (ParseAllMetrics, u64) {
    let mut parse_metrics = ParseAllMetrics::new();
    if let Some(Value::Chars(f)) = data.get2("wp_package_name").map(|x| x.get_value()) {
        parse_metrics.package_name = f.to_string();
    }
    if let Some(Value::Chars(f)) = data.get2("wp_rule_name").map(|x| x.get_value()) {
        parse_metrics.rule_name = f.to_string();
    }
    let mut count = 0;
    if let Some(Value::Digit(total)) = data.get2("total").opt().get_value() {
        count = *total
    }
    (parse_metrics, count as u64)
}

pub(crate) fn send_sink(data: &DataRecord) -> (SinkMetrics, u64) {
    let mut sink_metrics = SinkMetrics::new();
    if let Some(Value::Chars(f)) = data.get2("wp_sink_group").opt().get_value() {
        sink_metrics.sink_group = f.to_string();
    }
    // if let Some(Value::Chars(f)) = data.get2("sink_type").opt().get_value() {
    //     sink_metrics.sink_type = f.to_string();
    // }
    if let Some(Value::Chars(f)) = data.get2("wp_sink_name").opt().get_value() {
        sink_metrics.sink_name = f.to_string();
    }
    let mut count = 0;
    // sink_metrics.extend_metrics(data);
    if let Some(Value::Digit(f)) = data.get2("success").opt().get_value() {
        count = *f;
    }
    (sink_metrics, count as u64)
}

pub fn receive_data_stat(data: &DataRecord) {
    let (values, total) = source_values(data);
    if values.is_valid() {
        RECV_FROM_SOURCE
            .with_label_values(&values.values())
            .inc_by(total as u64);
    }
}

pub fn parse_all_stat(data: &DataRecord) {
    let (values, all) = parse_all(data);
    if values.is_valid() {
        PARSE_ALL.with_label_values(&values.values()).inc_by(all);
    }
}
pub fn sink_stat(data: &DataRecord) {
    let (values, count) = send_sink(data);
    if values.is_valid() {
        SEND_TO_SINK
            .with_label_values(&values.values())
            .inc_by(count);
    }
}

macro_rules! generate_metrics {
    ($name:ident; $($field:ident), *) => {
        #[derive(Default, Debug)] pub struct $name { $(pub $field: String,)* }
        impl $name {
            pub fn new() -> $name {
                let mut metrics = $name::default();
                metrics.pid = PID.to_string();
                metrics.instance = PID.to_string();
                metrics.access_type = String::from("service");
                metrics.access_name = String::from("warp-parse");
                metrics
            }
            pub fn labels() -> Vec<&'static str> { vec![ $( stringify!($field), )* ] }
            pub fn values(&self) -> Vec<&str> { vec![ $( self.$field.as_str(), )* ] }
            // 检查所有字段是否非空
            #[allow(dead_code)]
            pub fn is_valid(&self) -> bool {
                self.values().iter().all(|x| !x.is_empty())
            }
        }
    };
}
generate_metrics!(CpuMetrics;  pid, access_type, access_name, instance);
generate_metrics!(MemoryMetrics; pid, access_type, access_name, instance);

generate_metrics!(RecvMetrics; pid, access_type, access_name, instance, source_type, source_name);
generate_metrics!(ParseAllMetrics; pid, access_type, access_name, instance, package_name, rule_name);
// generate_extend_metrics!(ParseMetrics; pid, rule_name, wp_src_ip, log_business, log_type, log_desc, pos_sn);
generate_metrics!(SinkMetrics; pid, access_type, access_name, instance, sink_group, sink_name);

lazy_static! {
    pub static ref PID: String = sysinfo::get_current_pid()
        .expect("获取当前进程 PID 失败")
        .to_string();
    pub static ref RECV_FROM_SOURCE: IntCounterVec = register_int_counter_vec!(
        "wparse_receive_data",
        "Number of logs obtained from the data source.",
        &RecvMetrics::labels()
    )
    .expect("register wparse_receive_data fail");
    pub static ref PARSE_ALL: IntCounterVec = register_int_counter_vec!(
        "wparse_parse_all",
        "Number of logs parse.",
        &ParseAllMetrics::labels()
    )
    .expect("register wparse_parse_all fail");
    pub static ref SEND_TO_SINK: IntCounterVec = register_int_counter_vec!(
        "wparse_send_to_sink",
        "The count of send to sink.",
        &SinkMetrics::labels()
    )
    .expect("register wparse_send_to_sink fail");
    pub static ref CPU_USAGE: GaugeVec =
        register_gauge_vec!("wparse_cpu_usage", "The CPU usage.", &CpuMetrics::labels())
            .expect("register wparse_cpu_usage fail");
    pub static ref MEMORY_USAGE: GaugeVec = register_gauge_vec!(
        "wparse_memory_usage",
        "The memory usage.",
        &MemoryMetrics::labels()
    )
    .expect("register wparse_memory_usage fail");
}
