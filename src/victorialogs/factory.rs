use std::time::Duration;

use async_trait::async_trait;
use orion_error::prelude::SourceRawErr;
use serde_json::json;
use wp_connector_api::{
    ConnectorDef, ConnectorScope, ParamMap, SinkBuildCtx, SinkDefProvider, SinkFactory, SinkHandle,
    SinkReason, SinkResult, SinkSpec,
};
use wp_model_core::model::fmt_def::TextFmt;

use crate::http_utils::join_endpoint_path;

use super::config::VictoriaLog;
use super::sink::VictoriaLogSink;

pub struct VictoriaLogSinkFactory;

#[async_trait]
impl SinkFactory for VictoriaLogSinkFactory {
    fn kind(&self) -> &'static str {
        "victorialogs"
    }
    fn validate_spec(&self, spec: &SinkSpec) -> SinkResult<()> {
        crate::utils::reject_arrow_protocol(spec, self.kind())?;
        let endpoint = spec
            .params
            .get("endpoint")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if endpoint.trim().is_empty() {
            return Err(SinkReason::sink("victorialog.endpoint must not be empty"));
        }
        let api_path = spec
            .params
            .get("api_path")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if api_path.trim().is_empty() {
            return Err(SinkReason::sink("victorialog.api_path must not be empty"));
        }
        Ok(())
    }
    async fn build(&self, spec: &SinkSpec, _ctx: &SinkBuildCtx) -> SinkResult<SinkHandle> {
        let mut conf = VictoriaLog::default();
        if let Some(s) = spec.params.get("endpoint").and_then(|v| v.as_str()) {
            conf.endpoint = s.to_string();
        }
        if let Some(s) = spec.params.get("api_path").and_then(|v| v.as_str()) {
            conf.api_path = s.to_string();
        }
        if let Some(s) = spec.params.get("timestamp_field").and_then(|v| v.as_str()) {
            conf.timestamp_field = Some(s.to_string());
        }
        if let Some(v) = spec.params.get("timeout_secs") {
            if let Some(n) = v.as_f64() {
                if n > 0.0 {
                    conf.timeout_secs = n;
                }
            } else if let Some(s) = v.as_str()
                && let Ok(n) = s.parse::<f64>()
                && n > 0.0
            {
                conf.timeout_secs = n;
            }
        }
        if let Some(v) = spec.params.get("tags")
            && let Some(tags) = v.as_array()
        {
            conf.tags.extend(
                tags.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect::<Vec<_>>(),
            );
        }

        let fmt = spec
            .params
            .get("fmt")
            .and_then(|v| v.as_str())
            .map(TextFmt::from)
            .unwrap_or(TextFmt::Json);
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs_f64(conf.timeout_secs))
            .build()
            .source_raw_err(SinkReason::Sink, "build victorialog client failed")?;
        let write_url = join_endpoint_path(&conf.endpoint, &conf.api_path);
        let sink = VictoriaLogSink::new(
            write_url,
            client,
            fmt,
            conf.timestamp_field.clone(),
            conf.tags.clone(),
        );
        Ok(SinkHandle::new(Box::new(sink)))
    }
}

impl SinkDefProvider for VictoriaLogSinkFactory {
    fn sink_def(&self) -> ConnectorDef {
        ConnectorDef {
            id: "victorialogs_sink".into(),
            kind: self.kind().into(),
            scope: ConnectorScope::Sink,
            allow_override: vec![
                "endpoint",
                "api_path",
                "timestamp_field",
                "timeout_secs",
                "tags",
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
            default_params: victorialog_defaults(),
            origin: Some("wp-connectors:victorialogs_sink".into()),
        }
    }
}

fn victorialog_defaults() -> ParamMap {
    let mut params = ParamMap::new();
    params.insert("endpoint".into(), json!("http://127.0.0.1:9428"));
    params.insert("api_path".into(), json!("/insert/jsonline"));
    params.insert("timeout_secs".into(), json!(60));
    params
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sink_def_matches_official_template() {
        let def = VictoriaLogSinkFactory.sink_def();
        assert_eq!(def.id, "victorialogs_sink");
        assert_eq!(
            def.allow_override,
            vec![
                "endpoint".to_string(),
                "api_path".to_string(),
                "timestamp_field".to_string(),
                "timeout_secs".to_string(),
                "tags".to_string(),
            ]
        );
        assert_eq!(
            def.default_params.get("endpoint").and_then(|v| v.as_str()),
            Some("http://127.0.0.1:9428")
        );
        assert_eq!(
            def.default_params.get("api_path").and_then(|v| v.as_str()),
            Some("/insert/jsonline")
        );
        assert_eq!(
            def.default_params
                .get("timeout_secs")
                .and_then(|v| v.as_i64()),
            Some(60)
        );
        assert!(!def.default_params.contains_key("flush_secs"));
    }
}
