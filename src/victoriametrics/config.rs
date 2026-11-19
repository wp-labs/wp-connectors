//! 通用指标映射配置；默认值与校验由工厂和运行实例共用。
use std::collections::BTreeMap;
use std::time::Duration;

use chrono::FixedOffset;
use chrono::format::{Item, StrftimeItems};
use serde::{Deserialize, Serialize};
use wp_connector_api::{ParamMap, SinkReason, SinkResult};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
/// 选择通用字段映射或显式规则映射。
pub enum Mapping {
    /// 从配置字段中读取指标名和值。
    #[default]
    Fields,
    /// 按规则中配置的 JSON Pointer 条件选择指标。
    Rules,
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
/// Prometheus 指标类型。
pub enum MetricKind {
    /// 表示可增可减的当前值。
    #[default]
    Gauge,
    /// 表示单调增加的累计值。
    Counter,
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
/// Unix 时间戳输入值的单位。
pub enum TimeUnit {
    /// 秒。
    S,
    /// 毫秒，也是默认单位。
    #[default]
    Ms,
    /// 微秒。
    Us,
    /// 纳秒。
    Ns,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
/// 标签值从一个字段读取，或按顺序从多个候选字段回退读取。
pub enum LabelPath {
    /// 单个源字段路径。
    One(String),
    /// 按顺序尝试的源字段路径列表。
    Fallback(Vec<String>),
}
impl LabelPath {
    /// 返回候选字段路径，并保持配置中的优先顺序。
    pub(crate) fn paths(&self) -> Vec<&str> {
        match self {
            Self::One(path) => vec![path],
            Self::Fallback(paths) => paths.iter().map(String::as_str).collect(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
/// 将一种输入指标映射为一个 Prometheus 指标的规则。
pub struct MetricRule {
    /// fields 模式下用于匹配输入指标名；rules 模式使用 match 条件。
    pub input: String,
    /// rules 模式下按 JSON Pointer 路径和 JSON 标量值匹配记录，多个条件同时成立才命中。
    #[serde(rename = "match")]
    pub match_fields: BTreeMap<String, serde_json::Value>,
    /// 输出指标名；fields 模式省略时沿用 input。
    pub name: Option<String>,
    /// 数值字段路径；省略时使用全局 value_field。
    pub value: Option<String>,
    /// 输出指标类型；序列化配置键名为 type。
    #[serde(rename = "type")]
    pub kind: Option<MetricKind>,
    /// Prometheus HELP 文本；省略时不输出 HELP 行。
    pub help: Option<String>,
    /// 输出标签名到输入字段路径的映射。
    pub labels: Option<BTreeMap<String, LabelPath>>,
    /// 注入旧 WParse/WFusion 兼容标签的上下文类型。
    pub context: Option<String>,
    /// 此规则使用的时间字段路径；解析方式沿用全局时间配置。
    pub time: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
/// VictoriaMetrics sink 的传输、映射、标签和时间配置。
pub struct VictoriaMetric {
    /// VictoriaMetrics 服务根地址。
    pub endpoint: String,
    /// Prometheus 文本导入 API 路径，可包含查询参数。
    pub api_path: String,
    /// 定时发送间隔，单位为秒。
    pub flush_secs: f64,
    /// HTTP 请求和停止时最终 flush 的超时，单位为秒。
    pub timeout_secs: f64,
    /// 输入指标映射方式。
    pub mapping: Mapping,
    /// fields 模式下读取输入指标名的字段路径。
    pub name_field: String,
    /// 默认数值字段路径。
    pub value_field: String,
    /// 全局时间字段路径；单条规则可用 time 覆盖字段路径。
    pub time_field: Option<String>,
    /// 全局时间文本格式；未设置时按 time_unit 解析 Unix 时间戳。
    pub time_format: Option<String>,
    /// 全局 Unix 时间戳单位，默认毫秒。
    pub time_unit: TimeUnit,
    /// 解析无时区时间文本时使用的固定时区偏移；未配置时使用机器本地时区。
    pub time_zone: Option<String>,
    /// 标签白名单；省略时允许所有符合条件的标量字段。
    pub keep_labels: Option<Vec<String>>,
    /// 按输入字段路径剔除标签。
    pub drop_labels: Vec<String>,
    /// 输入标签名到 Prometheus 输出标签名的全局别名。
    pub label_aliases: BTreeMap<String, String>,
    /// 输入名或条件规则到指标输出定义的映射表。
    pub metrics: Vec<MetricRule>,
}
impl Default for VictoriaMetric {
    /// 返回本地 VictoriaMetrics 地址和定时推送的默认配置。
    fn default() -> Self {
        Self {
            endpoint: "http://127.0.0.1:8428".into(),
            api_path: "/api/v1/import/prometheus".into(),
            flush_secs: 1.0,
            timeout_secs: 5.0,
            mapping: Mapping::Fields,
            name_field: "__name__".into(),
            value_field: "values".into(),
            time_field: None,
            time_format: None,
            time_unit: TimeUnit::Ms,
            time_zone: None,
            keep_labels: None,
            drop_labels: Vec::new(),
            label_aliases: BTreeMap::new(),
            metrics: Vec::new(),
        }
    }
}

/// 将配置问题包装成 sink 错误。
pub(crate) fn invalid(message: impl Into<String>) -> wp_connector_api::SinkError {
    SinkReason::sink(message.into())
}

/// 将秒数转换为有效的正时长，并检查系统计时器是否可表示。
pub(crate) fn duration(duration_seconds: f64, duration_key: &str) -> SinkResult<Duration> {
    if !duration_seconds.is_finite() || duration_seconds <= 0.0 {
        return Err(invalid(format!("{duration_key} 必须是有限正数")));
    }
    let parsed_duration = Duration::try_from_secs_f64(duration_seconds)
        .map_err(|_| invalid(format!("{duration_key} 超出有效范围")))?;
    if parsed_duration.is_zero()
        || std::time::Instant::now()
            .checked_add(parsed_duration)
            .is_none()
    {
        return Err(invalid(format!("{duration_key} 超出有效范围")));
    }
    Ok(parsed_duration)
}

/// 将 JSON 数字或兼容的数字字符串规范化为有限秒数。
fn normalize_seconds(
    configured_value: &serde_json::Value,
    parameter_name: &str,
) -> SinkResult<serde_json::Value> {
    let duration_seconds = configured_value
        .as_f64()
        .or_else(|| configured_value.as_str()?.parse::<f64>().ok())
        .filter(|duration_seconds| duration_seconds.is_finite())
        .ok_or_else(|| invalid(format!("{parameter_name} 必须是有限数值")))?;
    Ok(serde_json::json!(duration_seconds))
}

/// 校验普通字段名或 JSON Pointer 路径的语法。
pub(crate) fn path_valid(field_path: &str) -> bool {
    if field_path.is_empty() {
        return false;
    }
    // 无前导斜杠时按字面顶层字段名处理；带斜杠时交由 jsonptr 校验 RFC 6901 语法。
    !field_path.starts_with('/') || jsonptr::Pointer::parse(field_path).is_ok()
}

/// 检查 Prometheus 指标名是否合法。
pub(crate) fn metric_name_valid(metric_name: &str) -> bool {
    // Desc::new 要求非空 HELP；校验只创建临时描述符，不注册指标。
    prometheus::core::Desc::new(
        metric_name.into(),
        "validation".into(),
        Vec::new(),
        std::collections::HashMap::new(),
    )
    .is_ok()
}
/// 检查 Prometheus 标签名是否合法，并拒绝保留标签前缀。
pub(crate) fn label_name_valid(label_name: &str) -> bool {
    !label_name.starts_with("__")
        && prometheus::core::Desc::new(
            "validation_metric".into(),
            "validation".into(),
            vec![label_name.into()],
            std::collections::HashMap::new(),
        )
        .is_ok()
}

/// 校验时间字段、Chrono 格式和时区配置。
pub(crate) fn validate_time(
    time_field_path: &str,
    time_format: Option<&str>,
    time_zone: Option<&str>,
) -> SinkResult<()> {
    if !path_valid(time_field_path) {
        return Err(invalid("时间字段路径无效"));
    }
    if let Some(time_format) = time_format {
        // time_zone 只在文本格式解析时参与 NaiveDateTime 的时区解释。
        if let Some(time_zone) = time_zone {
            time_zone
                .parse::<FixedOffset>()
                .map_err(|_| invalid("时区必须是明确的固定偏移，例如 +08:00"))?;
        }
        if StrftimeItems::new(time_format).any(|format_item| matches!(format_item, Item::Error)) {
            return Err(invalid("time_format 不是有效的 Chrono 时间格式"));
        }
    }
    Ok(())
}

impl VictoriaMetric {
    pub(crate) fn from_params(params: &ParamMap) -> SinkResult<Self> {
        let mut config_fields: serde_json::Map<String, serde_json::Value> = params
            .iter()
            .map(|(parameter_name, parameter_value)| {
                (parameter_name.clone(), parameter_value.clone())
            })
            .collect();

        // 兼容旧配置中的秒数字符串；正数和范围由统一校验负责。
        for duration_parameter in ["flush_secs", "timeout_secs"] {
            if let Some(configured_value) = config_fields.get(duration_parameter).cloned() {
                config_fields.insert(
                    duration_parameter.into(),
                    normalize_seconds(&configured_value, duration_parameter)?,
                );
            }
        }

        let mut sink_config: Self =
            serde_json::from_value(serde_json::Value::Object(config_fields))
                .map_err(|error| invalid(format!("VictoriaMetrics 配置错误: {error}")))?;
        sink_config.endpoint = sink_config.endpoint.trim_end_matches('/').into();
        sink_config.api_path = format!("/{}", sink_config.api_path.trim_start_matches('/'));
        // 参数解析仅负责规范化；完整配置校验统一由 Mapper::new 执行。
        Ok(sink_config)
    }

    pub(crate) fn validate(&self) -> SinkResult<()> {
        let endpoint_url = reqwest::Url::parse(&self.endpoint)
            .map_err(|_| invalid("endpoint 必须是有效的 HTTP URL"))?;
        if !matches!(endpoint_url.scheme(), "http" | "https")
            || endpoint_url.host_str().is_none()
            || endpoint_url.query().is_some()
            || endpoint_url.fragment().is_some()
        {
            return Err(invalid(
                "endpoint 必须是无查询参数和片段的 HTTP 地址，查询参数请写入 api_path",
            ));
        }
        if self.api_path.trim_matches('/').trim().is_empty() {
            return Err(invalid("api_path 不能为空"));
        }
        // 分别检查根地址和拼接后的写入地址，避免路径或查询参数组合出无效 URL。
        let write_endpoint = crate::http_utils::join_endpoint_path(&self.endpoint, &self.api_path);
        let write_url = reqwest::Url::parse(&write_endpoint)
            .map_err(|_| invalid("endpoint/api_path 不能组成有效的 HTTP URL"))?;
        if write_url.fragment().is_some() {
            return Err(invalid("api_path 不允许 URL 片段"));
        }

        duration(self.flush_secs, "flush_secs")?;
        duration(self.timeout_secs, "timeout_secs")?;
        // 所有可配置路径统一检查；规则级时间路径稍后由 Mapper 按规则逐项检查。
        for field_path in [&self.name_field, &self.value_field]
            .into_iter()
            .chain(self.keep_labels.iter().flatten())
            .chain(&self.drop_labels)
            .chain(self.label_aliases.keys())
        {
            if !path_valid(field_path) {
                return Err(invalid(format!("字段路径无效: {field_path}")));
            }
        }
        for output_label_name in self.label_aliases.values() {
            if !label_name_valid(output_label_name) {
                return Err(invalid(format!("标签别名无效: {output_label_name}")));
            }
        }
        // 全局时间字段作为默认来源在这里校验；规则级 time 在 Mapper 中逐条校验。
        // 没有实际时间字段时，time_format/time_zone 不参与运行，保持忽略。
        if let Some(time_field) = self.time_field.as_deref() {
            validate_time(
                time_field,
                self.time_format.as_deref(),
                self.time_zone.as_deref(),
            )?;
        }
        // rules 不使用动态 name_field 回退；没有规则时不会生成任何指标，因此显式要求配置规则。
        if self.mapping == Mapping::Rules && self.metrics.is_empty() {
            return Err(invalid("rules 模式必须显式配置 metrics"));
        }
        Ok(())
    }
}
