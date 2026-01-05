#![allow(dead_code)] // Prometheus 指标辅助函数仅在特定集成开启

use crate::WP_SRC_VAL;
use lazy_static::lazy_static;
use orion_exp::ValueGet0;
use wp_model_core::model::data::Field as ModelField;

// 本地轻薄封装：将 Option<&Field<Value>> 包成本地类型，
// 以便实现 orion_exp::ValueGet0（避免孤儿规则限制）。
pub(crate) struct OptField<'a>(Option<&'a ModelField<Value>>);

impl<'a> ValueGet0<Value> for OptField<'a> {
    fn get_value(&self) -> Option<&Value> {
        self.0.map(|f| f.get_value())
    }
}

pub(crate) trait IntoOptField<'a> {
    fn opt(self) -> OptField<'a>;
}

impl<'a> IntoOptField<'a> for Option<&'a ModelField<Value>> {
    fn opt(self) -> OptField<'a> {
        OptField(self)
    }
}
use prometheus::{IntCounterVec, register_int_counter_vec};
use regex::Regex;
use uuid::Uuid;
use wp_model_core::model::DataRecord;
use wp_model_core::model::Value;

// ------------- metrics helpers -------------

pub(crate) fn source_values(data: &DataRecord, format: &str) -> (RecvMetrics, i64) {
    let mut recv_metrics = RecvMetrics::new();
    let mut count = 0;
    if let Some(Value::Chars(f)) = data.get2("target").opt().get_value() {
        recv_metrics.key = f.to_string();
        if let Ok(re) = Regex::new(format)
            && let Some(caps) = re.captures(f)
        {
            recv_metrics.source_type = caps["source_type"].to_string();
        }
    }
    if let Some(Value::Chars(f)) = data.get2(WP_SRC_VAL).opt().get_value() {
        recv_metrics.access_source = f.to_string();
    }
    if let Some(Value::Digit(f)) = data.get2("total").opt().get_value() {
        count = *f;
    }
    (recv_metrics, count)
}

pub(crate) fn parse_success(data: &DataRecord) -> (ParseMetrics, u64) {
    let mut parse_metrics = ParseMetrics::new();
    let mut count = 0;
    if let Some(Value::Chars(f)) = data.get2("target").opt().get_value() {
        parse_metrics.rule_name = f.to_string();
    }
    parse_metrics.extend_metrics(data);
    if let Some(Value::Digit(f)) = data.get2("success").opt().get_value() {
        count = *f;
    }
    (parse_metrics, count as u64)
}

pub(crate) fn parse_all(data: &DataRecord) -> (ParseAllMetrics, u64) {
    let mut parse_metrics = ParseAllMetrics::new();
    parse_metrics.parse = String::from("parse");
    let mut count = 0;
    if let Some(Value::Digit(total)) = data.get2("total").opt().get_value() {
        count = *total
    }
    (parse_metrics, count as u64)
}

pub(crate) fn send_sink(data: &DataRecord, format: &str) -> (SinkMetrics, u64) {
    let mut sink_metrics = SinkMetrics::new();
    let mut count = 0;
    if let Some(Value::Chars(f)) = data.get2("target").opt().get_value() {
        sink_metrics.name = f.to_string();
        if let Ok(re) = Regex::new(format)
            && let Some(caps) = re.captures(f)
        {
            sink_metrics.sink_type = caps["sink_type"].to_string();
        }
    }
    sink_metrics.extend_metrics(data);
    if let Some(Value::Digit(f)) = data.get2("success").opt().get_value() {
        count = *f;
    }
    (sink_metrics, count as u64)
}

pub fn receive_data_stat(data: &DataRecord, format: &str) {
    let (values, total) = source_values(data, format);
    if values.is_valid() {
        RECV_FROM_SOURCE
            .with_label_values(&values.values())
            .inc_by(total as u64);
    }
}
pub fn parse_success_stat(data: &DataRecord) {
    let (values, success) = parse_success(data);
    if values.is_valid() {
        PARSE_SUCCESS
            .with_label_values(&values.values())
            .inc_by(success);
    }
}
pub fn parse_all_stat(data: &DataRecord) {
    let (values, all) = parse_all(data);
    PARSE_ALL.with_label_values(&values.values()).inc_by(all);
}
pub fn sink_stat(data: &DataRecord, format: &str) {
    let (values, count) = send_sink(data, format);
    if values.is_valid() {
        SEND_TO_SINK
            .with_label_values(&values.values())
            .inc_by(count);
    }
}

macro_rules! generate_metrics {
    ($name:ident; $($field:ident), *) => {
        #[derive(Default, Debug)] pub struct $name { $(pub $field: String,)* }
        impl $name { pub fn new() -> $name { $name{ pid: PID.to_string(), .. Default::default() } }
            pub fn labels() -> Vec<&'static str> { vec![ $( stringify!($field), )* ] }
            pub fn values(&self) -> Vec<&str> { vec![ $( self.$field.as_str(), )* ] }
            pub fn is_valid(&self) -> bool { $( if stringify!($field).ne("pos_sn") && self.$field.is_empty() { return false; } )* true }
        }
    };
}

macro_rules! generate_extend_metrics {
    ($name:ident; $($field:ident), *) => {
        generate_metrics!($name; $($field), *);
        impl $name { pub fn extend_metrics(&mut self, data: &DataRecord) {
            if let Some(Value::Chars(f)) = data.get2("pos_sn").opt().get_value() { self.pos_sn = f.to_string(); }
            if let Some(Value::Chars(f)) = data.get2("access_ip").opt().get_value() { self.access_ip = f.to_string(); }
            if let Some(Value::Chars(f)) = data.get2("log_desc").opt().get_value() { self.log_desc = f.to_string(); }
            if let Some(Value::Chars(f)) = data.get2("log_type").opt().get_value() { self.log_type = f.to_string(); }
        }}
    }
}

generate_metrics!(RecvMetrics; pid, key, source_type, access_source);
generate_metrics!(ParseAllMetrics; pid, parse);
generate_extend_metrics!(ParseMetrics; pid, rule_name, access_ip, log_type, log_desc, pos_sn);
generate_extend_metrics!(SinkMetrics; pid, name, sink_type, access_ip, log_type, log_desc, pos_sn);

lazy_static! {
    static ref PID: String = Uuid::new_v4().to_string();
    pub static ref RECV_FROM_SOURCE: IntCounterVec = register_int_counter_vec!(
        "wparse_receive_data",
        "Number of logs obtained from the data source.",
        &RecvMetrics::labels()
    )
    .expect("register wparse_receive_data fail");
    pub static ref PARSE_SUCCESS: IntCounterVec = register_int_counter_vec!(
        "wparse_parse_success",
        "Number of logs parse.",
        &ParseMetrics::labels()
    )
    .expect("register wparse_parse_success fail");
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
}
