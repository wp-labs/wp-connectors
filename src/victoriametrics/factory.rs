use super::config::VictoriaMetric;
use super::exporter::VictoriaMetricExporter;
use super::mapper::Mapper;
use async_trait::async_trait;
use wp_connector_api::{
    ConnectorDef, ConnectorScope, ParamMap, SinkBuildCtx, SinkDefProvider, SinkFactory, SinkHandle,
    SinkResult, SinkSpec,
};

/// VictoriaMetrics sink 的配置校验、构建和参数注册工厂。
pub struct VictoriaMetricFactory;
#[async_trait]
impl SinkFactory for VictoriaMetricFactory {
    /// 返回此工厂注册的 connector 类型名。
    fn kind(&self) -> &'static str {
        "victoriametrics"
    }
    /// 解析配置并校验映射规则及 sink 名称。
    fn validate_spec(&self, spec: &SinkSpec) -> SinkResult<()> {
        let sink_config = VictoriaMetric::from_params(&spec.params)?;
        // Mapper 构造会统一校验配置、字段映射及规则；工厂只补充 spec.name 校验。
        Mapper::new(&sink_config)?;
        if spec.name.trim().is_empty() {
            return Err(super::config::invalid("sink.name 不能为空"));
        }
        Ok(())
    }
    /// 根据 SinkSpec 创建 sink 句柄并共享同名指标状态。
    async fn build(&self, spec: &SinkSpec, _ctx: &SinkBuildCtx) -> SinkResult<SinkHandle> {
        let sink_config = VictoriaMetric::from_params(&spec.params)?;
        let exporter = VictoriaMetricExporter::for_name(&spec.name, sink_config)?;
        Ok(SinkHandle::new(Box::new(exporter)))
    }
}
impl SinkDefProvider for VictoriaMetricFactory {
    /// 注册可覆盖参数，并返回 VictoriaMetrics sink 的默认参数定义。
    fn sink_def(&self) -> ConnectorDef {
        ConnectorDef {
            id: "victoriametrics_sink".into(),
            kind: self.kind().into(),
            scope: ConnectorScope::Sink,
            allow_override: [
                "endpoint",
                "api_path",
                "flush_secs",
                "timeout_secs",
                "mapping",
                "name_field",
                "value_field",
                "time_field",
                "time_format",
                "time_unit",
                "time_zone",
                "keep_labels",
                "drop_labels",
                "label_aliases",
                "metrics",
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
            default_params: victoriametric_defaults(),
            origin: Some("wp-connectors:victoriametrics_sink".into()),
        }
    }
}
/// 序列化默认配置，并省略未设置的可选字段。
fn victoriametric_defaults() -> ParamMap {
    // Option::None 对应的键不注入 null，保持可选参数真正省略的语义。
    let serde_json::Value::Object(mut default_params) =
        serde_json::to_value(VictoriaMetric::default()).expect("内置配置可序列化")
    else {
        unreachable!()
    };
    default_params.retain(|_, parameter_value| !parameter_value.is_null());
    default_params.into_iter().collect()
}
