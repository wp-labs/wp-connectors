use std::time::Duration;

use async_trait::async_trait;
use serde_json::json;
use wp_connector_api::{
    ConnectorDef, ConnectorScope, ParamMap, SinkBuildCtx, SinkDefProvider, SinkError, SinkFactory,
    SinkHandle, SinkReason, SinkResult, SinkSpec,
};
use wp_model_core::model::fmt_def::TextFmt;

use super::config::VictoriaLog;
use super::sink::VictoriaLogSink;

pub struct VictoriaLogSinkFactory;

#[async_trait]
impl SinkFactory for VictoriaLogSinkFactory {
    fn kind(&self) -> &'static str {
        "victorialogs"
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
        if let Some(s) = spec
            .params
            .get("create_time_field")
            .and_then(|v| v.as_str())
        {
            conf.create_time_field = Some(s.to_string());
        }
        if let Some(v) = spec.params.get("request_timeout_secs") {
            if let Some(n) = v.as_f64() {
                if n > 0.0 {
                    conf.request_timeout_secs = n;
                }
            } else if let Some(s) = v.as_str()
                && let Ok(n) = s.parse::<f64>()
                && n > 0.0
            {
                conf.request_timeout_secs = n;
            }
        }
        let fmt = spec
            .params
            .get("fmt")
            .and_then(|v| v.as_str())
            .map(TextFmt::from)
            .unwrap_or(TextFmt::Json);
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs_f64(conf.request_timeout_secs))
            .build()
            .map_err(|err| {
                SinkError::from(SinkReason::sink(format!(
                    "build victorialog client failed: {err}"
                )))
            })?;
        let sink = VictoriaLogSink::new(
            conf.endpoint.clone(),
            conf.insert_path.clone(),
            client,
            fmt,
            conf.create_time_field.clone(),
        );
        Ok(SinkHandle::new(Box::new(sink)))
    }
}

impl SinkDefProvider for VictoriaLogSinkFactory {
    fn sink_def(&self) -> ConnectorDef {
        ConnectorDef {
            id: "victorialog_sink".into(),
            kind: self.kind().into(),
            scope: ConnectorScope::Sink,
            allow_override: vec!["endpoint", "insert_path", "fmt", "request_timeout_secs"]
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
    params.insert("request_timeout_secs".into(), json!(60.0));
    params
}
