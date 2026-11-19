//! 显式字段映射；raw JSON 与 DataRecord 共用同一条校验管线。
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use chrono::{DateTime, FixedOffset, Local, NaiveDateTime, TimeZone};
use serde_json::Value;
use wp_connector_api::SinkResult;
use wp_model_core::model::{DataRecord, Value as FieldValue};

use super::config::{
    Mapping, MetricKind, MetricRule, TimeUnit, VictoriaMetric, invalid, label_name_valid,
    metric_name_valid, path_valid, validate_time,
};

#[derive(Debug, Clone, PartialEq, Eq)]
/// 同一 Prometheus 指标族必须保持一致的类型和 HELP。
pub(crate) struct MetricFamilyDefinition {
    pub kind: MetricKind,
    pub help: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
/// 由输出指标名和完整标签集合组成的时间序列键。
pub(crate) struct SeriesKey {
    pub name: String,
    pub labels: BTreeMap<String, String>,
}
#[derive(Debug, Clone)]
/// 单条输入记录映射出的指标样本。
pub(crate) struct MetricUpdate {
    pub name: String,
    pub labels: BTreeMap<String, String>,
    pub value: f64,
    pub timestamp: Option<i64>,
    pub kind: MetricKind,
    pub help: Option<String>,
}

/// 仅借用输入记录，不复制或物化整条记录。
#[derive(Clone, Copy)]
pub(crate) enum InputRecord<'a> {
    Json(&'a Value),
    DataRecord(&'a DataRecord),
}

/// 仅借用输入字段值；字段最终进入 MetricUpdate 时才转换为自有值。
#[derive(Clone, Copy)]
enum InputValue<'a> {
    Json(&'a Value),
    DataRecord(&'a FieldValue),
}

impl<'a> From<&'a Value> for InputRecord<'a> {
    fn from(json_value: &'a Value) -> Self {
        Self::Json(json_value)
    }
}

impl<'a> From<&'a DataRecord> for InputRecord<'a> {
    fn from(data_record: &'a DataRecord) -> Self {
        Self::DataRecord(data_record)
    }
}

impl<'a> InputRecord<'a> {
    /// 输入必须是 JSON object；DataRecord 本身始终表示 object。
    fn is_object(self) -> bool {
        matches!(self, Self::DataRecord(_) | Self::Json(Value::Object(_)))
    }

    /// 检查 DataRecord 中的非有限浮点数，不构造中间 JSON 树。
    fn validate(self) -> SinkResult<()> {
        let Self::DataRecord(data_record) = self else {
            return Ok(());
        };
        for data_field in &data_record.items {
            validate_field_value(data_field.get_value())?;
        }
        Ok(())
    }

    /// 遍历顶层字段，供未配置显式标签列表时筛选标量标签。
    fn for_each_field(
        self,
        mut visit: impl FnMut(&str, InputValue<'a>) -> SinkResult<()>,
    ) -> SinkResult<()> {
        match self {
            Self::Json(Value::Object(object)) => {
                for (field_name, field_value) in object {
                    visit(field_name, InputValue::Json(field_value))?;
                }
            }
            Self::DataRecord(data_record) => {
                for data_field in &data_record.items {
                    visit(
                        data_field.get_name(),
                        InputValue::DataRecord(data_field.get_value()),
                    )?;
                }
            }
            Self::Json(_) => {}
        }
        Ok(())
    }
}

impl<'a> InputValue<'a> {
    /// 按配置 JSON 值的类型精确比较输入标量。
    fn matches(self, expected_scalar: &Value) -> bool {
        match self {
            Self::Json(actual_value) => {
                !actual_value.is_object()
                    && !actual_value.is_array()
                    && actual_value == expected_scalar
            }
            Self::DataRecord(FieldValue::Null) => expected_scalar.is_null(),
            Self::DataRecord(FieldValue::Bool(value)) => expected_scalar.as_bool() == Some(*value),
            Self::DataRecord(FieldValue::Float(value)) => {
                serde_json::Number::from_f64(*value).as_ref() == expected_scalar.as_number()
            }
            Self::DataRecord(FieldValue::Int(value)) => {
                expected_scalar
                    .as_i64()
                    .is_some_and(|expected| i128::from(expected) == i128::from(*value))
                    || expected_scalar
                        .as_u64()
                        .is_some_and(|expected| i128::from(expected) == i128::from(*value))
            }
            Self::DataRecord(FieldValue::Chars(value)) => expected_scalar.as_str() == Some(value),
            Self::DataRecord(FieldValue::Obj(_) | FieldValue::Array(_)) => false,
            Self::DataRecord(field_value) => expected_scalar
                .as_str()
                .is_some_and(|expected| field_value.to_string() == expected),
        }
    }

    /// 返回输入文本；字符串字段借用原值，领域值按兼容规则临时格式化。
    fn as_text(self) -> Option<Cow<'a, str>> {
        match self {
            Self::Json(Value::String(value)) => Some(Cow::Borrowed(value)),
            Self::DataRecord(FieldValue::Chars(value)) => Some(Cow::Borrowed(value.as_str())),
            Self::DataRecord(
                FieldValue::Null
                | FieldValue::Bool(_)
                | FieldValue::Float(_)
                | FieldValue::Int(_)
                | FieldValue::Obj(_)
                | FieldValue::Array(_),
            ) => None,
            Self::DataRecord(value) => Some(Cow::Owned(value.to_string())),
            Self::Json(_) => None,
        }
    }

    /// 沿一个对象键或数组下标读取子值。
    fn child(self, path_segment: &PathSegment) -> Option<Self> {
        // JSON Pointer token 同时保留键和值为数字的数组下标；访问对象时用 key，访问数组时用 index。
        match self {
            Self::Json(Value::Object(object)) => {
                object.get(&path_segment.object_key).map(Self::Json)
            }
            Self::Json(Value::Array(array)) => array.get(path_segment.array_index?).map(Self::Json),
            Self::DataRecord(FieldValue::Obj(object)) => object
                .get(&path_segment.object_key)
                .map(|field| Self::DataRecord(field.get_value())),
            Self::DataRecord(FieldValue::Array(array)) => array
                .get(path_segment.array_index?)
                .map(|field| Self::DataRecord(field.get_value())),
            _ => None,
        }
    }

    /// 沿已编译路径片段逐级读取。
    fn descend(mut self, path_segments: &[PathSegment]) -> Option<Self> {
        for path_segment in path_segments {
            self = self.child(path_segment)?;
        }
        Some(self)
    }

    /// 判断是否为 JSON object 可用作标签的标量。
    fn is_scalar(self) -> bool {
        match self {
            Self::Json(Value::String(_) | Value::Number(_) | Value::Bool(_)) => true,
            Self::DataRecord(FieldValue::Null | FieldValue::Obj(_) | FieldValue::Array(_)) => false,
            Self::DataRecord(_) => true,
            Self::Json(_) => false,
        }
    }

    /// 将标量转换成标签文本。
    fn scalar(self) -> Option<String> {
        match self {
            Self::Json(Value::String(value)) => Some(value.clone()),
            Self::Json(Value::Number(value)) => Some(value.to_string()),
            Self::Json(Value::Bool(value)) => Some(value.to_string()),
            Self::DataRecord(FieldValue::Float(value)) => {
                serde_json::Number::from_f64(*value).map(|value| value.to_string())
            }
            Self::DataRecord(FieldValue::Null | FieldValue::Obj(_) | FieldValue::Array(_)) => None,
            Self::DataRecord(value) => Some(value.to_string()),
            Self::Json(_) => None,
        }
    }

    /// 读取有限数值，DataRecord 的领域标量按旧转换规则解析其文本。
    fn number(self) -> Option<f64> {
        match self {
            Self::Json(Value::Number(value)) => value.as_f64(),
            Self::Json(Value::String(value)) => value.parse().ok(),
            Self::DataRecord(FieldValue::Int(value)) => Some(*value as f64),
            Self::DataRecord(FieldValue::Float(value)) => Some(*value),
            Self::DataRecord(FieldValue::Chars(value)) => value.parse().ok(),
            Self::DataRecord(
                FieldValue::Null | FieldValue::Bool(_) | FieldValue::Obj(_) | FieldValue::Array(_),
            ) => None,
            Self::DataRecord(value) => value.to_string().parse().ok(),
            Self::Json(_) => None,
        }
    }

    /// 读取 Unix 时间戳允许的整数或整数字符串。
    fn timestamp_integer(self) -> Option<i128> {
        match self {
            Self::Json(Value::Number(value)) => value
                .as_i64()
                .map(i128::from)
                .or_else(|| value.as_u64().map(i128::from)),
            Self::Json(Value::String(value)) => value.parse().ok(),
            Self::DataRecord(FieldValue::Int(value)) => Some(i128::from(*value)),
            Self::DataRecord(FieldValue::Chars(value)) => value.parse().ok(),
            Self::DataRecord(
                FieldValue::Null
                | FieldValue::Bool(_)
                | FieldValue::Float(_)
                | FieldValue::Obj(_)
                | FieldValue::Array(_),
            ) => None,
            Self::DataRecord(value) => value.to_string().parse().ok(),
            Self::Json(_) => None,
        }
    }
}

/// 检查 DataRecord 值中不能由 serde_json 表示的非有限浮点数。
fn validate_field_value(field_value: &FieldValue) -> SinkResult<()> {
    match field_value {
        FieldValue::Float(float_value) if !float_value.is_finite() => {
            Err(invalid("DataRecord 包含非有限浮点数"))
        }
        FieldValue::Obj(fields_by_name) => {
            for nested_field in fields_by_name.values() {
                validate_field_value(nested_field.get_value())?;
            }
            Ok(())
        }
        FieldValue::Array(array_items) => {
            for array_item in array_items {
                validate_field_value(array_item.get_value())?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}
impl MetricUpdate {
    /// 生成时间序列键。Prometheus 将空标签值视为标签不存在。
    pub fn series_key(&self) -> SeriesKey {
        SeriesKey {
            name: self.name.clone(),
            labels: self
                .labels
                .iter()
                .filter(|(_, label_value)| !label_value.is_empty())
                .map(|(label_name, label_value)| (label_name.clone(), label_value.clone()))
                .collect(),
        }
    }
    /// 提取同名指标共享的类型和 HELP。
    pub fn family_definition(&self) -> MetricFamilyDefinition {
        MetricFamilyDefinition {
            kind: self.kind,
            help: self.help.clone(),
        }
    }
}

/// JSON Pointer 路径片段，提前解析数组下标以免每条记录重复解析。
#[derive(Debug, Clone)]
struct PathSegment {
    object_key: String,
    array_index: Option<usize>,
}

/// 区分普通字段名和 JSON Pointer 路径。
#[derive(Debug, Clone)]
enum PathAccess {
    Field,
    Pointer(Vec<PathSegment>),
}

/// 已编译的输入字段路径，并缓存用于筛选和别名查询的规范化键。
#[derive(Debug, Clone)]
struct CompiledPath {
    /// 用户配置中的原始写法，用于错误信息和普通顶层字段读取。
    expression: String,
    /// 统一后的比较键，用于保留字段、筛选策略和别名查找。
    canonical_pointer: String,
    /// 预先解析的读取方式，避免每条记录重复解析 JSON Pointer。
    access: PathAccess,
}
impl CompiledPath {
    /// 在 Mapper 初始化时缓存比较键和读取方式，热路径只执行字段查找。
    fn new(configured_path: &str) -> Self {
        Self {
            expression: configured_path.into(),
            canonical_pointer: pointer(configured_path),
            access: if configured_path.starts_with('/') {
                PathAccess::Pointer(pointer_segments(configured_path))
            } else {
                PathAccess::Field
            },
        }
    }

    /// 按字段名或已编译的 JSON Pointer 读取借用值。
    fn read<'a>(&self, input: InputRecord<'a>) -> Option<InputValue<'a>> {
        match (input, &self.access) {
            (InputRecord::Json(object), PathAccess::Field) => {
                object.get(&self.expression).map(InputValue::Json)
            }
            (InputRecord::Json(object), PathAccess::Pointer(segments)) => {
                InputValue::Json(object).descend(segments)
            }
            (InputRecord::DataRecord(record), PathAccess::Field) => record
                .items
                .iter()
                .find(|field| field.get_name() == self.expression)
                .map(|field| InputValue::DataRecord(field.get_value())),
            (InputRecord::DataRecord(record), PathAccess::Pointer(segments)) => {
                // DataRecord 的根对象由 items 表示，先查首段，再由 InputValue 读取嵌套对象/数组。
                let (first_segment, remaining_segments) = segments.split_first()?;
                let first_field_value = record
                    .items
                    .iter()
                    .find(|data_field| data_field.get_name() == first_segment.object_key)?
                    .get_value();
                InputValue::DataRecord(first_field_value).descend(remaining_segments)
            }
        }
    }
}

/// 使用 jsonptr 解析并预编译 JSON Pointer 片段。
fn pointer_segments(json_pointer: &str) -> Vec<PathSegment> {
    jsonptr::Pointer::parse(json_pointer)
        .expect("JSON Pointer 在 Mapper 初始化时已校验")
        .tokens()
        .map(|token| {
            let decoded_token = token.decoded().into_owned();
            // 同一片段既保留对象键，也缓存数组访问时需要的数字下标。
            let array_index = token
                .to_index()
                .ok()
                .and_then(|parsed_index| match parsed_index {
                    jsonptr::index::Index::Num(array_index) => Some(array_index),
                    jsonptr::index::Index::Next => None,
                });
            PathSegment {
                object_key: decoded_token,
                array_index,
            }
        })
        .collect()
}

/// 将白名单、黑名单和别名路径预先转换为查找表。
struct LabelPolicy {
    /// `keep_labels` 的规范化键，用于判断来源路径是否保留为标签。
    keep_label_pointers: Option<HashSet<String>>,
    /// `keep_labels` 的预编译读取路径，支持嵌套 JSON Pointer。
    keep_label_paths: Option<Vec<CompiledPath>>,
    /// 黑名单的规范化键；只需做成员判断，因此不另外保存读取路径。
    excluded_label_pointers: HashSet<String>,
    /// 规范化输入路径到输出标签名的映射。
    label_aliases_by_source_path: HashMap<String, String>,
}
impl LabelPolicy {
    /// 编译全局标签筛选和别名配置。
    fn new(sink_config: &VictoriaMetric) -> Self {
        // 保留路径列表供读取字段使用，同时生成集合供规则校验和别名策略做快速成员判断。
        let keep_label_paths = sink_config.keep_labels.as_ref().map(|configured_paths| {
            configured_paths
                .iter()
                .map(|configured_path| CompiledPath::new(configured_path))
                .collect::<Vec<_>>()
        });
        let keep_label_pointers = keep_label_paths.as_ref().map(|compiled_paths| {
            compiled_paths
                .iter()
                .map(|compiled_path| compiled_path.canonical_pointer.clone())
                .collect::<HashSet<_>>()
        });
        let excluded_label_pointers = sink_config
            .drop_labels
            .iter()
            .map(|configured_path| pointer(configured_path))
            .collect();
        let mut output_names_by_source_path =
            HashMap::with_capacity(sink_config.label_aliases.len());

        // 普通字段名与等价的 JSON Pointer 形式统一成同一个键。
        for (source_path, output_label_name) in &sink_config.label_aliases {
            output_names_by_source_path
                .entry(pointer(source_path))
                .or_insert_with(|| output_label_name.clone());
        }
        Self {
            keep_label_pointers,
            keep_label_paths,
            excluded_label_pointers,
            label_aliases_by_source_path: output_names_by_source_path,
        }
    }

    /// 判断规范化后的输入路径是否通过白名单和黑名单筛选。
    fn eligible(&self, canonical_path: &str) -> bool {
        !self.excluded_label_pointers.contains(canonical_path)
            && self
                .keep_label_pointers
                .as_ref()
                .is_none_or(|keep_label_pointers| keep_label_pointers.contains(canonical_path))
    }
}

/// 一项输出标签及其已筛选、按顺序排列的字段路径。
struct CompiledLabelMapping {
    output_label_name: String,
    source_paths: Vec<CompiledPath>,
}

/// 预编译的规则匹配条件。
struct CompiledMatch {
    match_path: CompiledPath,
    expected_value: Value,
}

struct CompiledRule {
    /// rules 模式的条件列表；全部匹配才命中。
    match_conditions: Vec<CompiledMatch>,
    /// 指标名、值和时间字段，不能再作为自动标签来源。
    reserved_paths: HashSet<String>,
    /// 当前规则覆盖的样本值字段。
    value_path: Option<CompiledPath>,
    /// 规则级时间字段优先，全局时间字段作为默认值。
    timestamp_path: Option<CompiledPath>,
    /// 显式标签定义；为 None 时使用全局策略自动收集。
    explicit_label_mappings: Option<Vec<CompiledLabelMapping>>,
}
impl CompiledRule {
    /// 将已校验规则编译成运行时使用的匹配、数值、时间和标签路径。
    fn new(
        sink_config: &VictoriaMetric,
        metric_rule: &MetricRule,
        label_policy: &LabelPolicy,
        reserved_paths: HashSet<String>,
    ) -> Self {
        let match_conditions = metric_rule
            .match_fields
            .iter()
            .map(|(match_path, expected_value)| CompiledMatch {
                match_path: CompiledPath::new(match_path),
                expected_value: expected_value.clone(),
            })
            .collect();
        let explicit_label_mappings = metric_rule.labels.as_ref().map(|configured_mappings| {
            configured_mappings
                .iter()
                .map(|(output_label_name, source_paths)| CompiledLabelMapping {
                    output_label_name: output_label_name.clone(),
                    source_paths: source_paths
                        .paths()
                        .into_iter()
                        .map(CompiledPath::new)
                        .filter(|compiled_path| {
                            label_policy.eligible(&compiled_path.canonical_pointer)
                        })
                        .collect(),
                })
                .collect()
        });
        Self {
            match_conditions,
            reserved_paths,
            value_path: metric_rule.value.as_deref().map(CompiledPath::new),
            timestamp_path: metric_rule
                .time
                .as_deref()
                .or(sink_config.time_field.as_deref())
                .map(CompiledPath::new),
            explicit_label_mappings,
        }
    }

    /// 所有配置条件都精确匹配时，输入记录命中此规则。
    fn matches(&self, input: InputRecord<'_>) -> bool {
        !self.match_conditions.is_empty()
            && self.match_conditions.iter().all(|condition| {
                condition
                    .match_path
                    .read(input)
                    .is_some_and(|actual_value| actual_value.matches(&condition.expected_value))
            })
    }
}

/// 按输入指标名或通用 JSON Pointer 条件，将记录映射为指标更新。
pub(crate) struct Mapper {
    sink_config: VictoriaMetric,
    rule_index_by_input_name: HashMap<String, usize>,
    name_field: CompiledPath,
    value_field: CompiledPath,
    label_policy: LabelPolicy,
    compiled_rules: Vec<CompiledRule>,
    default_rule: MetricRule,
    compiled_default_rule: CompiledRule,
}

/// 将普通字段名转换为 JSON Pointer，并转义路径中的特殊字符。
fn pointer(field_path: &str) -> String {
    // 不带前导斜杠的是字面顶层字段名；转义斜杠和波浪号以生成稳定的比较键。
    if field_path.starts_with('/') {
        field_path.into()
    } else {
        format!("/{}", field_path.replace('~', "~0").replace('/', "~1"))
    }
}
/// 按已编译路径读取非空字符串。
fn path_text<'a>(input: InputRecord<'a>, field_path: &CompiledPath) -> SinkResult<Cow<'a, str>> {
    field_path
        .read(input)
        .and_then(InputValue::as_text)
        .filter(|text_value| !text_value.is_empty())
        .ok_or_else(|| invalid(format!("字段 {} 必须是非空字符串", field_path.expression)))
}

/// 按已编译路径读取有限数值。
fn number(input: InputRecord<'_>, field_path: &CompiledPath) -> SinkResult<f64> {
    field_path
        .read(input)
        .and_then(InputValue::number)
        .filter(|numeric_value| numeric_value.is_finite())
        .ok_or_else(|| invalid(format!("字段 {} 必须是有限数值", field_path.expression)))
}
/// 为旧 WParse/WFusion 指标生成兼容上下文标签。
fn context_labels(compatibility_context: Option<&str>) -> BTreeMap<String, String> {
    compatibility_context
        .map(|compatibility_context| {
            BTreeMap::from([
                ("pid".into(), std::process::id().to_string()),
                ("instance".into(), std::process::id().to_string()),
                ("access_type".into(), "service".into()),
                ("access_name".into(), compatibility_context.into()),
            ])
        })
        .unwrap_or_default()
}

impl Mapper {
    /// 先校验完整配置，再预编译映射索引、路径和指标族定义，避免在每条记录上重复准备。
    pub(crate) fn new(sink_config: &VictoriaMetric) -> SinkResult<Self> {
        sink_config.validate()?;
        Self::validate_aliases(sink_config)?;

        let label_policy = LabelPolicy::new(sink_config);
        let mut rule_index_by_input_name = HashMap::with_capacity(sink_config.metrics.len());
        let mut metric_families: BTreeMap<String, MetricFamilyDefinition> = BTreeMap::new();
        let mut compiled_rules = Vec::with_capacity(sink_config.metrics.len());

        // 先校验每条规则并登记同名指标定义，再把通过校验的字段路径编译供 map 复用。
        for (rule_index, metric_rule) in sink_config.metrics.iter().enumerate() {
            Self::index_rule(
                sink_config,
                metric_rule,
                rule_index,
                &mut rule_index_by_input_name,
                &sink_config.metrics[..rule_index],
            )?;

            let reserved_paths = Self::reserved(sink_config, metric_rule);
            Self::validate_rule(sink_config, metric_rule, &reserved_paths)?;
            let output_metric_name = metric_rule.name.as_deref().unwrap_or(&metric_rule.input);
            Self::record_family_definition(
                output_metric_name,
                metric_rule.kind.unwrap_or_default(),
                &metric_rule.help,
                &mut metric_families,
            )?;
            compiled_rules.push(CompiledRule::new(
                sink_config,
                metric_rule,
                &label_policy,
                reserved_paths,
            ));
        }

        let default_rule = MetricRule::default();
        let default_reserved_paths = Self::reserved(sink_config, &default_rule);
        let compiled_default_rule = CompiledRule::new(
            sink_config,
            &default_rule,
            &label_policy,
            default_reserved_paths,
        );

        Ok(Self {
            sink_config: sink_config.clone(),
            rule_index_by_input_name,
            name_field: CompiledPath::new(&sink_config.name_field),
            value_field: CompiledPath::new(&sink_config.value_field),
            label_policy,
            compiled_rules,
            default_rule,
            compiled_default_rule,
        })
    }

    /// 检查全局标签别名定义不会映射到相同的输出标签名。
    fn validate_aliases(sink_config: &VictoriaMetric) -> SinkResult<()> {
        let mut output_label_names = BTreeSet::new();
        // keep/drop 决定运行时哪些字段进入标签；重复别名本身仍是配置冲突，不随筛选结果变化。
        for output_label_name in sink_config.label_aliases.values() {
            if !output_label_names.insert(output_label_name) {
                return Err(invalid(format!("标签别名冲突: {output_label_name}")));
            }
        }
        Ok(())
    }

    /// 按映射方式登记字段规则或校验通用规则条件。
    fn index_rule(
        sink_config: &VictoriaMetric,
        metric_rule: &MetricRule,
        rule_index: usize,
        rule_index_by_input_name: &mut HashMap<String, usize>,
        previous_rules: &[MetricRule],
    ) -> SinkResult<()> {
        match sink_config.mapping {
            Mapping::Fields => {
                // fields 用输入指标名建索引；match/context 属于 rules 配置，避免两种模式混用。
                if !metric_rule.match_fields.is_empty() || metric_rule.context.is_some() {
                    return Err(invalid("match/context 仅用于 rules 模式"));
                }
                if metric_rule.input.is_empty() {
                    return Err(invalid("metrics.input 不能为空"));
                }
                if rule_index_by_input_name
                    .insert(metric_rule.input.clone(), rule_index)
                    .is_some()
                {
                    return Err(invalid("metrics 包含重复的输入名"));
                }
            }
            Mapping::Rules => {
                // 一个规则的所有条件同时成立才命中；完全相同的条件组无法区分，启动时拒绝。
                if !metric_rule.input.is_empty() {
                    return Err(invalid("rules 模式使用 match 条件，不配置 input"));
                }
                if metric_rule.match_fields.is_empty() {
                    return Err(invalid("rules 项至少需要一个 match 条件"));
                }
                for (match_path, expected_value) in &metric_rule.match_fields {
                    if !path_valid(match_path) {
                        return Err(invalid(format!("rules.match 路径无效: {match_path}")));
                    }
                    if expected_value.is_object() || expected_value.is_array() {
                        return Err(invalid("rules.match 值必须是 JSON 标量"));
                    }
                }
                if previous_rules
                    .iter()
                    .any(|previous_rule| previous_rule.match_fields == metric_rule.match_fields)
                {
                    return Err(invalid("rules 项包含完全相同的 match 条件"));
                }
            }
        }
        Ok(())
    }

    /// 校验规则的输出指标名、值和时间字段，以及显式标签定义。
    fn validate_rule(
        sink_config: &VictoriaMetric,
        metric_rule: &MetricRule,
        reserved_label_paths: &HashSet<String>,
    ) -> SinkResult<()> {
        // rules 的输出定义必须完整；fields 可以从输入名和全局 value_field 使用默认值。
        if sink_config.mapping == Mapping::Rules
            && (metric_rule.name.is_none()
                || metric_rule.value.is_none()
                || metric_rule.kind.is_none())
        {
            return Err(invalid("rules 项必须给出 name/value/type"));
        }
        let output_metric_name = metric_rule.name.as_deref().unwrap_or(&metric_rule.input);
        if !metric_name_valid(output_metric_name) {
            return Err(invalid(format!("指标名无效: {output_metric_name}")));
        }
        if let Some(compatibility_context) = &metric_rule.context
            && !matches!(compatibility_context.as_str(), "warp-parse" | "warp-fusion")
        {
            return Err(invalid("context 暂时只支持 warp-parse 或 warp-fusion"));
        }

        let compatibility_labels = context_labels(metric_rule.context.as_deref());
        // 按每条规则最终生效的时间字段校验；规则级 time 覆盖全局 time_field。
        let timestamp_field_path = metric_rule
            .time
            .as_deref()
            .or(sink_config.time_field.as_deref());
        if let Some(timestamp_field_path) = timestamp_field_path {
            validate_time(
                timestamp_field_path,
                sink_config.time_format.as_deref(),
                sink_config.time_zone.as_deref(),
            )?;
        }
        if let Some(sample_value_path) = &metric_rule.value
            && !path_valid(sample_value_path)
        {
            return Err(invalid("指标 value 路径无效"));
        }

        if let Some(label_mappings) = &metric_rule.labels {
            for (output_label_name, lab_value) in label_mappings {
                if !label_name_valid(output_label_name)
                    || compatibility_labels.contains_key(output_label_name)
                {
                    return Err(invalid(format!(
                        "标签名无效或与 context 冲突: {output_label_name}"
                    )));
                }
                if lab_value.paths().is_empty() {
                    return Err(invalid("标签 fallback 列表不能为空"));
                }
                for source_path in lab_value.paths() {
                    if !path_valid(source_path)
                        || reserved_label_paths.contains(&pointer(source_path))
                    {
                        return Err(invalid(format!(
                            "标签源路径无效或属于保留字段: {source_path}"
                        )));
                    }
                }
            }
        }
        Ok(())
    }

    /// 校验同名指标族的类型和 HELP。
    fn record_family_definition(
        metric_name: &str,
        kind: MetricKind,
        help: &Option<String>,
        families: &mut BTreeMap<String, MetricFamilyDefinition>,
    ) -> SinkResult<()> {
        if families
            .get(metric_name)
            .is_some_and(|old| old.kind != kind || &old.help != help)
        {
            return Err(invalid(format!("同名指标的类型/HELP 冲突: {metric_name}")));
        }
        families.insert(
            metric_name.to_owned(),
            MetricFamilyDefinition {
                kind,
                help: help.clone(),
            },
        );
        Ok(())
    }

    /// 收集指标名、指标值和时间字段路径；这些字段不能作为标签来源。
    fn reserved(sink_config: &VictoriaMetric, metric_rule: &MetricRule) -> HashSet<String> {
        let mut reserved_paths = HashSet::new();
        // fields 的指标名/全局值字段，加上规则值和时间字段，都属于样本元数据而非标签。
        if sink_config.mapping == Mapping::Fields {
            reserved_paths.insert(pointer(&sink_config.name_field));
            reserved_paths.insert(pointer(&sink_config.value_field));
        }
        reserved_paths.extend(
            metric_rule
                .value
                .as_ref()
                .map(|field_path| pointer(field_path)),
        );
        reserved_paths.extend(
            metric_rule
                .time
                .as_ref()
                .map(|field_path| pointer(field_path)),
        );
        reserved_paths.extend(
            sink_config
                .time_field
                .as_ref()
                .map(|field_path| pointer(field_path)),
        );

        // rules.match 路径不排除，匹配字段也可以作为业务标签。
        reserved_paths
    }
    /// 将一条借用的 JSON 或 DataRecord 映射为零条或多条指标更新。
    /// fields 模式最多映射一条；rules 模式为所有命中规则分别映射一条。
    fn map_into<'a>(
        &self,
        input_record: impl Into<InputRecord<'a>>,
        metric_updates: &mut Vec<MetricUpdate>,
    ) -> SinkResult<()> {
        let input_record = input_record.into();
        input_record.validate()?;
        if !input_record.is_object() {
            return Err(invalid("输入必须是 JSON object 或 object 数组"));
        }

        match self.sink_config.mapping {
            Mapping::Fields => {
                let input_metric_name = path_text(input_record, &self.name_field)?;
                let rule_index = self
                    .rule_index_by_input_name
                    .get(input_metric_name.as_ref())
                    .copied();
                metric_updates.push(self.map_with_rule(
                    input_record,
                    Some(input_metric_name.as_ref()),
                    rule_index,
                )?);
            }
            Mapping::Rules => {
                for (rule_index, compiled_rule) in self.compiled_rules.iter().enumerate() {
                    if compiled_rule.matches(input_record) {
                        metric_updates.push(self.map_with_rule(
                            input_record,
                            None,
                            Some(rule_index),
                        )?);
                    }
                }
            }
        }
        Ok(())
    }

    /// 使用指定规则生成一条指标更新。
    fn map_with_rule(
        &self,
        input_record: InputRecord<'_>,
        input_metric_name: Option<&str>,
        rule_index: Option<usize>,
    ) -> SinkResult<MetricUpdate> {
        let (metric_rule, compiled_metric_rule) = rule_index.map_or(
            (&self.default_rule, &self.compiled_default_rule),
            |rule_index| {
                (
                    &self.sink_config.metrics[rule_index],
                    &self.compiled_rules[rule_index],
                )
            },
        );
        let output_metric_name = metric_rule
            .name
            .as_deref()
            .or(input_metric_name)
            .ok_or_else(|| invalid("指标输出名缺失"))?;
        let output_metric_kind = metric_rule.kind.unwrap_or_default();
        let sample_value = number(
            input_record,
            compiled_metric_rule
                .value_path
                .as_ref()
                .unwrap_or(&self.value_field),
        )?;
        if output_metric_kind == MetricKind::Counter && sample_value < 0.0 {
            return Err(invalid("Counter 值必须非负"));
        }
        let timestamp_ms =
            self.read_timestamp_ms(input_record, compiled_metric_rule.timestamp_path.as_ref())?;
        let output_labels = self.map_labels(input_record, metric_rule, compiled_metric_rule)?;
        Ok(MetricUpdate {
            name: output_metric_name.into(),
            labels: output_labels,
            value: sample_value,
            timestamp: timestamp_ms,
            kind: output_metric_kind,
            help: metric_rule.help.clone(),
        })
    }

    /// 按当前规则解析输入标签和兼容上下文标签。
    fn map_labels(
        &self,
        input_record: InputRecord<'_>,
        metric_rule: &MetricRule,
        compiled_metric_rule: &CompiledRule,
    ) -> SinkResult<BTreeMap<String, String>> {
        let mut output_labels = BTreeMap::new();
        // 静态名称已在配置校验时检查；动态名称由 Prometheus Desc 在创建序列时校验。
        match (
            compiled_metric_rule.explicit_label_mappings.as_deref(),
            self.label_policy.keep_label_paths.as_deref(),
        ) {
            (Some(label_mappings), _) => {
                Self::map_explicit_labels(input_record, label_mappings, &mut output_labels)?
            }
            (None, Some(keep_label_paths)) => self.map_keep_labels(
                input_record,
                compiled_metric_rule,
                keep_label_paths,
                &mut output_labels,
            )?,
            (None, None) => {
                self.map_automatic_labels(input_record, compiled_metric_rule, &mut output_labels)?
            }
        }
        // 兼容 context 标签在输入标签映射完成后注入。
        for (context_label_name, context_label_value) in
            context_labels(metric_rule.context.as_deref())
        {
            if output_labels
                .insert(context_label_name.clone(), context_label_value)
                .is_some()
            {
                return Err(invalid(format!("context 标签冲突: {context_label_name}")));
            }
        }
        Ok(output_labels)
    }

    /// 按配置顺序尝试显式标签的候选字段，命中后停止回退。
    fn map_explicit_labels(
        input_record: InputRecord<'_>,
        label_mappings: &[CompiledLabelMapping],
        output_labels: &mut BTreeMap<String, String>,
    ) -> SinkResult<()> {
        for label_mapping in label_mappings {
            for source_path in &label_mapping.source_paths {
                let Some(input_value) = source_path.read(input_record) else {
                    continue;
                };
                Self::insert_label_value(
                    output_labels,
                    &label_mapping.output_label_name,
                    &source_path.expression,
                    input_value,
                )?;
                break;
            }
        }
        Ok(())
    }

    /// 按编译后的白名单路径读取标签，支持嵌套字段。
    fn map_keep_labels(
        &self,
        input_record: InputRecord<'_>,
        compiled_metric_rule: &CompiledRule,
        keep_label_paths: &[CompiledPath],
        output_labels: &mut BTreeMap<String, String>,
    ) -> SinkResult<()> {
        for source_path in keep_label_paths {
            if compiled_metric_rule
                .reserved_paths
                .contains(&source_path.canonical_pointer)
                || !self.label_policy.eligible(&source_path.canonical_pointer)
            {
                continue;
            }
            let Some(input_value) = source_path.read(input_record) else {
                continue;
            };
            let output_label_name =
                self.output_label_name(&source_path.canonical_pointer, &source_path.expression);
            Self::insert_label_value(
                output_labels,
                output_label_name,
                &source_path.expression,
                input_value,
            )?;
        }
        Ok(())
    }

    /// 未配置白名单时自动收集顶层标量字段，不递归展开嵌套对象。
    fn map_automatic_labels(
        &self,
        input_record: InputRecord<'_>,
        compiled_metric_rule: &CompiledRule,
        output_labels: &mut BTreeMap<String, String>,
    ) -> SinkResult<()> {
        input_record.for_each_field(|field_name, field_value| {
            if !field_value.is_scalar() {
                return Ok(());
            }
            let canonical_path = pointer(field_name);
            if compiled_metric_rule
                .reserved_paths
                .contains(&canonical_path)
                || !self.label_policy.eligible(&canonical_path)
            {
                return Ok(());
            }
            let output_label_name = self.output_label_name(&canonical_path, field_name);
            Self::insert_label_value(output_labels, output_label_name, field_name, field_value)
        })
    }

    /// 取字段配置的别名；未配置别名时沿用原字段名。
    fn output_label_name<'a>(&'a self, canonical_path: &str, fallback: &'a str) -> &'a str {
        self.label_policy
            .label_aliases_by_source_path
            .get(canonical_path)
            .map(String::as_str)
            .unwrap_or(fallback)
    }

    /// 转换标量标签值，并检查两个源字段是否映射到了同一输出标签名。
    fn insert_label_value(
        output_labels: &mut BTreeMap<String, String>,
        output_label_name: &str,
        source_name: &str,
        input_value: InputValue<'_>,
    ) -> SinkResult<()> {
        let label_value = input_value
            .scalar()
            .ok_or_else(|| invalid(format!("标签 {source_name} 必须是标量")))?;
        output_labels.insert(output_label_name.into(), label_value);
        Ok(())
    }

    /// 消费 JSON 对象并映射，逐条释放已处理的输入对象。
    pub(crate) fn map_batch(&self, input_objects: Vec<Value>) -> SinkResult<Vec<MetricUpdate>> {
        let mut metric_updates = Vec::new();
        for input_object in input_objects {
            self.map_into(InputRecord::Json(&input_object), &mut metric_updates)?;
        }
        Ok(metric_updates)
    }

    /// 直接从借用的 DataRecord 映射，不构造对应的 serde_json::Value 树。
    pub(crate) fn map_records<'a>(
        &self,
        input_records: impl IntoIterator<Item = &'a DataRecord>,
    ) -> SinkResult<Vec<MetricUpdate>> {
        let mut metric_updates = Vec::new();
        for data_record in input_records {
            self.map_into(InputRecord::DataRecord(data_record), &mut metric_updates)?;
        }
        Ok(metric_updates)
    }

    /// 按预编译时间字段和全局时间策略解析 Unix 毫秒时间戳。
    fn read_timestamp_ms(
        &self,
        input: InputRecord<'_>,
        timestamp_path: Option<&CompiledPath>,
    ) -> SinkResult<Option<i64>> {
        let Some(timestamp_path) = timestamp_path else {
            return Ok(None);
        };
        let timestamp_value = timestamp_path
            .read(input)
            .ok_or_else(|| invalid(format!("缺少时间字段: {}", timestamp_path.expression)))?;
        let timestamp_ms = match self.sink_config.time_format.as_deref() {
            Some(time_format) => {
                let timestamp_text = timestamp_value
                    .as_text()
                    .ok_or_else(|| invalid("格式化时间必须是字符串"))?;
                self.parse_formatted_time(timestamp_text.as_ref(), time_format)?
            }
            None => {
                let unix_timestamp = timestamp_value
                    .timestamp_integer()
                    .ok_or_else(|| invalid("Unix 时间戳必须是整数或整数字符串"))?;
                let timestamp_ms = match self.sink_config.time_unit {
                    TimeUnit::S => unix_timestamp
                        .checked_mul(1000)
                        .ok_or_else(|| invalid("时间戳溢出"))?,
                    TimeUnit::Ms => unix_timestamp,
                    TimeUnit::Us => unix_timestamp / 1000,
                    TimeUnit::Ns => unix_timestamp / 1_000_000,
                };
                i64::try_from(timestamp_ms).map_err(|_| invalid("时间戳超出毫秒整数范围"))?
            }
        };
        if timestamp_ms == 0 {
            return Err(invalid("epoch 0 无法由当前 TextEncoder 区分于无时间戳"));
        }
        Ok(Some(timestamp_ms))
    }

    /// 按配置格式解析时间；优先使用输入中的偏移，其次使用配置时区或机器本地时区。
    fn parse_formatted_time(&self, timestamp_text: &str, time_format: &str) -> SinkResult<i64> {
        if let Ok(offset_datetime) = DateTime::parse_from_str(timestamp_text, time_format) {
            return Ok(offset_datetime.timestamp_millis());
        }

        let local_datetime = NaiveDateTime::parse_from_str(timestamp_text, time_format)
            .map_err(|error| invalid(format!("时间解析失败: {error}")))?;
        // 夏令时不存在或重复的本地时刻无法唯一转换，直接返回错误。
        if let Some(time_zone) = self.sink_config.time_zone.as_deref() {
            let fixed_offset = time_zone
                .parse::<FixedOffset>()
                .map_err(|_| invalid("时间时区无效"))?;
            return fixed_offset
                .from_local_datetime(&local_datetime)
                .single()
                .map(|datetime| datetime.timestamp_millis())
                .ok_or_else(|| invalid("时间超出有效范围"));
        }

        Local
            .from_local_datetime(&local_datetime)
            .single()
            .map(|datetime| datetime.timestamp_millis())
            .ok_or_else(|| invalid("机器本地时间不存在或有夏令时歧义"))
    }
}
