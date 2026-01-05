use std::time::Duration;

use async_trait::async_trait;
use serde_json::json;
use wp_connector_api::{
    ConnectorDef, ConnectorScope, ParamMap, SinkBuildCtx, SinkDefProvider, SinkError, SinkFactory,
    SinkHandle, SinkReason, SinkResult, SinkSpec,
};

use super::config::VictoriaMetric;
use super::exporter::VictoriaMetricExporter;

pub struct VictoriaMetricFactory;

#[async_trait]
impl SinkFactory for VictoriaMetricFactory {
    fn kind(&self) -> &'static str {
        "victoriametrics"
    }
    fn validate_spec(&self, spec: &SinkSpec) -> SinkResult<()> {
        let endpoint = spec
            .params
            .get("endpoint")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if endpoint.trim().is_empty() {
            return Err(SinkReason::sink("victoriametric.endpoint must not be empty").into());
        }
        Ok(())
    }
    async fn build(&self, spec: &SinkSpec, _ctx: &SinkBuildCtx) -> SinkResult<SinkHandle> {
        let mut conf = VictoriaMetric::default();
        if let Some(v) = spec.params.get("flush_interval_secs") {
            if let Some(n) = v.as_f64() {
                if n > 0.0 {
                    conf.flush_interval_secs = n;
                }
            } else if let Some(s) = v.as_str()
                && let Ok(n) = s.parse::<f64>()
            {
                conf.flush_interval_secs = n;
            }
        }
        if let Some(s) = spec.params.get("insert_url").and_then(|v| v.as_str()) {
            conf.insert_url = s.to_string();
        }

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .map_err(|err| {
                SinkError::from(SinkReason::sink(format!(
                    "build victoriametric client failed: {err}"
                )))
            })?;
        let mut sink = VictoriaMetricExporter::new(
            conf.insert_url.clone(),
            client,
            Duration::from_secs_f64(conf.flush_interval_secs),
        );
        sink.start_flush_task();
        Ok(SinkHandle::new(Box::new(sink)))
    }
}

impl SinkDefProvider for VictoriaMetricFactory {
    fn sink_def(&self) -> ConnectorDef {
        ConnectorDef {
            id: "victoriametric_sink".into(),
            kind: self.kind().into(),
            scope: ConnectorScope::Sink,
            allow_override: vec!["endpoint", "flush_interval_secs"]
                .into_iter()
                .map(str::to_string)
                .collect(),
            default_params: victoriametric_defaults(),
            origin: Some("wp-connectors:victoriametric_sink".into()),
        }
    }
}

fn victoriametric_defaults() -> ParamMap {
    let mut params = ParamMap::new();
    params.insert("endpoint".into(), json!("http://localhost:8480"));
    params.insert("flush_interval_secs".into(), json!(5.0));
    params
}
