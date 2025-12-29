use std::time::Duration;

use async_trait::async_trait;
use serde_json::json;
use wp_connector_api::{
    ConnectorDef, ConnectorDefProvider, ConnectorScope, ParamMap, SinkBuildCtx, SinkError,
    SinkFactory, SinkHandle, SinkReason, SinkResult, SinkSpec,
};
use wp_model_core::model::fmt_def::TextFmt;

use super::config::VictoriaLog;
use super::sink::VictoriaLogSink;

pub struct VictoriaLogSinkFactory;

#[async_trait]
impl SinkFactory for VictoriaLogSinkFactory {
    fn kind(&self) -> &'static str {
        "victorialog"
    }
    fn validate_spec(&self, spec: &SinkSpec) -> SinkResult<()> {
        let endpoint = spec
            .params
            .get("endpoint")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if endpoint.trim().is_empty() {
            return Err(SinkReason::sink("victorialog.endpoint must not be empty").into());
        }
        Ok(())
    }
    async fn build(&self, spec: &SinkSpec, _ctx: &SinkBuildCtx) -> SinkResult<SinkHandle> {
        let mut conf = VictoriaLog::default();
        if let Some(s) = spec.params.get("endpoint").and_then(|v| v.as_str()) {
            conf.endpoint = s.to_string();
        }
        if let Some(s) = spec.params.get("insert_path").and_then(|v| v.as_str()) {
            conf.insert_path = s.to_string();
        }
        let fmt = spec
            .params
            .get("fmt")
            .and_then(|v| v.as_str())
            .map(TextFmt::from)
            .unwrap_or(TextFmt::Json);
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .map_err(|err| {
                SinkError::from(SinkReason::sink(format!(
                    "build victorialog client failed: {err}"
                )))
            })?;
        let sink =
            VictoriaLogSink::new(conf.endpoint.clone(), conf.insert_path.clone(), client, fmt);
        Ok(SinkHandle::new(Box::new(sink)))
    }
}

impl ConnectorDefProvider for VictoriaLogSinkFactory {
    fn sink_def(&self) -> ConnectorDef {
        ConnectorDef {
            id: "victorialog_sink".into(),
            kind: self.kind().into(),
            scope: ConnectorScope::Sink,
            allow_override: vec!["endpoint", "insert_path", "fmt"]
                .into_iter()
                .map(str::to_string)
                .collect(),
            default_params: victorialog_defaults(),
            origin: Some("wp-connectors:victorialog_sink".into()),
        }
    }
}

fn victorialog_defaults() -> ParamMap {
    let mut params = ParamMap::new();
    params.insert("endpoint".into(), json!("http://localhost:8481"));
    params.insert("insert_path".into(), json!("/insert/json"));
    params.insert("fmt".into(), json!("json"));
    params
}
