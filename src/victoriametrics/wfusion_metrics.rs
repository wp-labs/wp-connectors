use orion_exp::ValueGet0;
use wp_model_core::model::{DataRecord, FValueStr, Value};

use crate::victoriametrics::wparse_metrics::IntoOptField;

use prometheus::{GaugeVec, IntCounterVec, register_gauge_vec, register_int_counter_vec};

trait Labels {
    fn values(&self) -> Vec<&str>;
}

macro_rules! generate_metrics {
    ($name:ident; $($field:ident),*) => {
        #[derive(Debug, Clone, Default)]
        pub struct $name {
            $(pub $field: String,)*
        }

        impl Labels for $name {
            fn values(&self) -> Vec<&str> {
                vec![$(self.$field.as_str(),)*]
            }
        }

        impl $name {
            pub fn new() -> $name {
                let mut metrics = $name::default();
                metrics.pid = PID.to_string();
                metrics.instance = PID.to_string();
                metrics.access_type = String::from("service");
                metrics.access_name = String::from("warp-fusion");
                metrics
            }

            pub fn labels() -> Vec<&'static str> { vec![ $( stringify!($field), )* ] }
        }
    };
}

trait FromRecord: Sized + Labels {
    fn from_record(record: &DataRecord) -> Option<(Self, f64)>;
}

fn record_value_to_f64(record: &DataRecord) -> f64 {
    match record.get2("value").opt().get_value() {
        Some(Value::Digit(f)) => *f as f64,
        Some(Value::Chars(s)) => match s.parse::<f64>() {
            Ok(v) => v,
            Err(_) => {
                log::warn!("metrics value parse failed: {s:?}");
                0.0
            }
        },
        other => {
            log::warn!("metrics value unexpected type: {other:?}");
            0.0
        }
    }
}

fn record_counter<M: FromRecord>(data: &DataRecord, counter: &IntCounterVec) {
    if let Some((labels, count)) = M::from_record(data) {
        counter
            .with_label_values(&labels.values())
            .inc_by(count as u64);
    }
}

fn record_gauge<M: FromRecord>(data: &DataRecord, gauge: &GaugeVec) {
    if let Some((labels, value)) = M::from_record(data) {
        gauge.with_label_values(&labels.values()).set(value);
    }
}

generate_metrics!(ReceiveTotalMetrics; pid, access_type, access_name, instance, source_name, source_type, machine_name);
generate_metrics!(RouteErrorsTotal; pid, access_type, access_name, instance, source_name);
generate_metrics!(WindowRowsTotal; pid, access_type, access_name, instance, window_name);
generate_metrics!(WindowMemoryBytes; pid, access_type, access_name, instance, window_name);
generate_metrics!(WindowMemoryCapacityBytes; pid, access_type, access_name, instance, window_name);
generate_metrics!(WindowLateTotal; pid, access_type, access_name, instance, window_name);
generate_metrics!(RuleEventsTotal; pid, access_type, access_name, instance, rule_name);
generate_metrics!(RuleMatchesTotal; pid, access_type, access_name, instance, rule_name);
generate_metrics!(RuleInstances; pid, access_type, access_name, instance, rule_name);
generate_metrics!(EventE2ELatencySecondP99; pid, access_type, access_name, instance);
generate_metrics!(AlertEmittedTotal; pid, access_type, access_name, instance, alert_name, machine_name, scope_key);
generate_metrics!(AlertDispatchFailedTotal; pid, access_type, access_name, instance);

lazy_static::lazy_static! {
    pub static ref PID: String = std::process::id().to_string();

    pub static ref RECEIVE_TOTAL: IntCounterVec = register_int_counter_vec!(
        "wf_receive_total",
        "Total number of records received by warp-fusion",
        &ReceiveTotalMetrics::labels()
    ).expect("register wf_receive_total metric failed");

    pub static ref ROUTE_ERRORS_TOTAL: IntCounterVec = register_int_counter_vec!(
        "wf_route_errors_total",
        "Total number of routing errors in warp-fusion",
        &RouteErrorsTotal::labels()
    ).expect("register wf_route_errors_total metric failed");

    pub static ref WINDOW_ROWS_TOTAL: GaugeVec = register_gauge_vec!(
        "wf_window_rows_total",
        "Total number of rows processed in warp-fusion windows",
        &WindowRowsTotal::labels()
    ).expect("register wf_window_rows_total metric failed");

    pub static ref WINDOW_MEMORY_BYTES: GaugeVec = register_gauge_vec!(
        "wf_window_memory_bytes",
        "Current memory usage of warp-fusion windows",
        &WindowMemoryBytes::labels()
    ).expect("register wf_window_memory_bytes metric failed");

    pub static ref WINDOW_MEMORY_CAPACITY_BYTES: GaugeVec = register_gauge_vec!(
        "wf_window_memory_capacity_bytes",
        "Capacity of warp-fusion window memory",
        &WindowMemoryCapacityBytes::labels()
    ).expect("register wf_window_memory_capacity_bytes metric failed");

    pub static ref WINDOW_LATE_TOTAL: IntCounterVec = register_int_counter_vec!(
        "wf_window_late_total",
        "Total number of late events in warp-fusion windows",
        &WindowLateTotal::labels()
    ).expect("register wf_window_late_total metric failed");

    pub static ref RULE_EVENTS_TOTAL: IntCounterVec = register_int_counter_vec!(
        "wf_rule_events_total",
        "Total number of events processed by each rule in warp-fusion",
        &RuleEventsTotal::labels()
    ).expect("register wf_rule_events_total metric failed");

    pub static ref RULE_MATCHES_TOTAL: IntCounterVec = register_int_counter_vec!(
        "wf_rule_matches_total",
        "Total number of matches for each rule in warp-fusion",
        &RuleMatchesTotal::labels()
    ).expect("register wf_rule_matches_total metric failed");

    pub static ref RULE_INSTANCES_TOTAL: GaugeVec = register_gauge_vec!(
        "wf_rule_instances_total",
        "Total number of instances for each rule in warp-fusion",
        &RuleInstances::labels()
    ).expect("register wf_rule_instances_total metric failed");

    pub static ref EVENT_E2E_LATENCY_SECOND_P99: GaugeVec = register_gauge_vec!(
        "wf_event_e2e_latency_second_p99",
        "99th percentile of end-to-end latency for events in warp-fusion",
        &EventE2ELatencySecondP99::labels()
    ).expect("register wf_event_e2e_latency_second_p99 metric failed");

    pub static ref ALERT_EMITTED_TOTAL: IntCounterVec = register_int_counter_vec!(
        "wf_alert_emitted_total",
        "Total number of alerts emitted by warp-fusion",
        &AlertEmittedTotal::labels()
    ).expect("register wf_alert_emitted_total metric failed");

    pub static ref ALERT_DISPATCH_FAILED_TOTAL: IntCounterVec = register_int_counter_vec!(
        "wf_alert_dispatch_failed_total",
        "Total number of alerts failed to dispatch in warp-fusion",
        &AlertDispatchFailedTotal::labels()
    ).expect("register wf_alert_dispatch_failed_total metric failed");
}

impl FromRecord for ReceiveTotalMetrics {
    fn from_record(record: &DataRecord) -> Option<(Self, f64)> {
        if record.get2("name").opt().get_value()
            != Some(&Value::Chars(FValueStr::from("rows_total")))
        {
            return None;
        }
        let mut m = Self::new();
        m.source_name = record.get2("label").opt().get_value()?.to_string();
        m.source_type = record
            .get2("source_type")
            .opt()
            .get_value()
            .and_then(|v| match v {
                Value::Chars(s) => Some(s.to_string()),
                _ => None,
            })
            .unwrap_or_default();
        m.machine_name = record
            .get2("machine")
            .opt()
            .get_value()
            .and_then(|v| match v {
                Value::Chars(s) => Some(s.to_string()),
                _ => None,
            })
            .unwrap_or_default();
        let count = record_value_to_f64(record);
        Some((m, count))
    }
}

pub fn receive_total_stat(data: &DataRecord) {
    record_counter::<ReceiveTotalMetrics>(data, &RECEIVE_TOTAL);
}

impl FromRecord for RouteErrorsTotal {
    fn from_record(record: &DataRecord) -> Option<(Self, f64)> {
        let mut m = Self::new();
        if record.get2("name").opt().get_value()
            != Some(&Value::Chars(FValueStr::from("route_errors_total")))
        {
            return None;
        }
        m.source_name = record.get2("label").opt().get_value()?.to_string();
        let count = record_value_to_f64(record);
        Some((m, count))
    }
}

pub fn route_errors_stat(data: &DataRecord) {
    record_counter::<RouteErrorsTotal>(data, &ROUTE_ERRORS_TOTAL);
}

impl FromRecord for WindowRowsTotal {
    fn from_record(record: &DataRecord) -> Option<(Self, f64)> {
        if record.get2("name").opt().get_value() != Some(&Value::Chars(FValueStr::from("rows"))) {
            return None;
        }
        let mut m = Self::new();
        m.window_name = record.get2("label").opt().get_value()?.to_string();
        let count = record_value_to_f64(record);
        Some((m, count))
    }
}

pub fn window_rows_stat(data: &DataRecord) {
    record_gauge::<WindowRowsTotal>(data, &WINDOW_ROWS_TOTAL);
}

impl FromRecord for WindowMemoryBytes {
    fn from_record(record: &DataRecord) -> Option<(Self, f64)> {
        if record.get2("name").opt().get_value()
            != Some(&Value::Chars(FValueStr::from("memory_bytes")))
        {
            return None;
        }
        let mut m = Self::new();
        m.window_name = record.get2("label").opt().get_value()?.to_string();
        let bytes = record_value_to_f64(record);
        Some((m, bytes))
    }
}

pub fn window_memory_stat(data: &DataRecord) {
    record_gauge::<WindowMemoryBytes>(data, &WINDOW_MEMORY_BYTES);
}

impl FromRecord for WindowMemoryCapacityBytes {
    fn from_record(record: &DataRecord) -> Option<(Self, f64)> {
        if record.get2("name").opt().get_value()
            != Some(&Value::Chars(FValueStr::from("window_capacity_bytes")))
        {
            return None;
        }
        let mut m = Self::new();
        m.window_name = record.get2("label").opt().get_value()?.to_string();
        let bytes = record_value_to_f64(record);
        Some((m, bytes))
    }
}

pub fn window_memory_capacity_stat(data: &DataRecord) {
    record_gauge::<WindowMemoryCapacityBytes>(data, &WINDOW_MEMORY_CAPACITY_BYTES);
}

impl FromRecord for WindowLateTotal {
    fn from_record(record: &DataRecord) -> Option<(Self, f64)> {
        let mut window_late_total = Self::new();
        if record.get2("name").opt().get_value()
            != Some(&Value::Chars(FValueStr::from("late_total")))
        {
            return None;
        }
        window_late_total.window_name = record.get2("label").opt().get_value()?.to_string();
        let count = record_value_to_f64(record);
        Some((window_late_total, count))
    }
}

pub fn window_late_stat(data: &DataRecord) {
    record_counter::<WindowLateTotal>(data, &WINDOW_LATE_TOTAL);
}
impl FromRecord for RuleEventsTotal {
    fn from_record(record: &DataRecord) -> Option<(Self, f64)> {
        let mut rule_events_total = Self::new();
        if record.get2("name").opt().get_value()
            != Some(&Value::Chars(FValueStr::from("events_total")))
        {
            return None;
        }
        rule_events_total.rule_name = record.get2("label").opt().get_value()?.to_string();
        let count = record_value_to_f64(record);
        Some((rule_events_total, count))
    }
}

pub fn rule_events_total_stat(data: &DataRecord) {
    record_counter::<RuleEventsTotal>(data, &RULE_EVENTS_TOTAL);
}

impl FromRecord for RuleMatchesTotal {
    fn from_record(record: &DataRecord) -> Option<(Self, f64)> {
        let mut rule_matches_total = Self::new();
        if record.get2("name").opt().get_value()
            != Some(&Value::Chars(FValueStr::from("matches_total")))
        {
            return None;
        }
        rule_matches_total.rule_name = record.get2("label").opt().get_value()?.to_string();
        let count = record_value_to_f64(record);
        Some((rule_matches_total, count))
    }
}

pub fn rule_matches_total_stat(data: &DataRecord) {
    record_counter::<RuleMatchesTotal>(data, &RULE_MATCHES_TOTAL);
}

impl FromRecord for RuleInstances {
    fn from_record(record: &DataRecord) -> Option<(Self, f64)> {
        let mut rule_instances = Self::new();
        if record.get2("name").opt().get_value()
            != Some(&Value::Chars(FValueStr::from("instances")))
        {
            return None;
        }
        rule_instances.rule_name = record.get2("label").opt().get_value()?.to_string();
        let count = record_value_to_f64(record);
        Some((rule_instances, count))
    }
}

pub fn rule_instances_stat(data: &DataRecord) {
    record_gauge::<RuleInstances>(data, &RULE_INSTANCES_TOTAL);
}

impl FromRecord for EventE2ELatencySecondP99 {
    fn from_record(record: &DataRecord) -> Option<(Self, f64)> {
        if record.get2("name").opt().get_value()
            != Some(&Value::Chars(FValueStr::from("e2e_latency_seconds_p99")))
        {
            return None;
        }
        let latency = record_value_to_f64(record);
        Some((Self::new(), latency))
    }
}

pub fn event_e2e_latency_second_p99_stat(data: &DataRecord) {
    record_gauge::<EventE2ELatencySecondP99>(data, &EVENT_E2E_LATENCY_SECOND_P99);
}

impl FromRecord for AlertEmittedTotal {
    fn from_record(record: &DataRecord) -> Option<(Self, f64)> {
        if record.get2("name").opt().get_value()
            != Some(&Value::Chars(FValueStr::from("emitted_total")))
        {
            return None;
        }
        let mut m = Self::new();
        m.alert_name = record.get2("label").opt().get_value()?.to_string();
        // machine_name: optional, defaults to "" so PromQL filter machine_name!="" excludes unassociated records
        m.machine_name = record
            .get2("machine")
            .opt()
            .get_value()
            .and_then(|v| match v {
                Value::Chars(s) => Some(s.to_string()),
                _ => None,
            })
            .unwrap_or_default();
        // scope_key: optional, default "-"
        m.scope_key = record
            .get2("scope_key")
            .opt()
            .get_value()
            .and_then(|v| match v {
                Value::Chars(s) => Some(s.to_string()),
                _ => None,
            })
            .unwrap_or_else(|| "-".to_string());
        let count = record_value_to_f64(record);
        Some((m, count))
    }
}

pub fn alert_emitted_total_stat(data: &DataRecord) {
    record_counter::<AlertEmittedTotal>(data, &ALERT_EMITTED_TOTAL);
}

impl FromRecord for AlertDispatchFailedTotal {
    fn from_record(record: &DataRecord) -> Option<(Self, f64)> {
        if record.get2("name").opt().get_value()
            != Some(&Value::Chars(FValueStr::from("sink_dispatch_failed_total")))
        {
            return None;
        }
        let count = record_value_to_f64(record);
        Some((Self::new(), count))
    }
}

pub fn alert_dispatch_failed_stat(data: &DataRecord) {
    record_counter::<AlertDispatchFailedTotal>(data, &ALERT_DISPATCH_FAILED_TOTAL);
}
