use std::time::Duration;

use async_trait::async_trait;
use orion_error::prelude::SourceRawErr;
use serde_json::json;
use wp_connector_api::{
    ConnectorDef, ConnectorScope, ParamMap, SinkBuildCtx, SinkDefProvider, SinkFactory, SinkHandle,
    SinkReason, SinkResult, SinkSpec,
};

use crate::http_utils::join_endpoint_path;

use super::config::VictoriaMetric;
use super::exporter::VictoriaMetricExporter;

pub struct VictoriaMetricFactory;

#[async_trait]
impl SinkFactory for VictoriaMetricFactory {
    fn kind(&self) -> &'static str {
        "victoriametrics"
    }
    fn validate_spec(&self, spec: &SinkSpec) -> SinkResult<()> {
        crate::utils::reject_arrow_protocol(spec, self.kind())?;
        let endpoint = spec
            .params
            .get("endpoint")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if endpoint.trim().is_empty() {
            return Err(SinkReason::sink(
                "victoriametrics.endpoint must not be empty",
            ));
        }
        let api_path = spec
            .params
            .get("api_path")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if api_path.trim().is_empty() {
            return Err(SinkReason::sink(
                "victoriametrics.api_path must not be empty",
            ));
        }
        Ok(())
    }
    async fn build(&self, spec: &SinkSpec, _ctx: &SinkBuildCtx) -> SinkResult<SinkHandle> {
        let mut conf = VictoriaMetric::default();
        if let Some(v) = spec.params.get("flush_secs") {
            if let Some(n) = v.as_f64() {
                if n > 0.0 {
                    conf.flush_secs = n;
                }
            } else if let Some(s) = v.as_str()
                && let Ok(n) = s.parse::<f64>()
                && n > 0.0
            {
                conf.flush_secs = n;
            }
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
        if let Some(s) = spec.params.get("endpoint").and_then(|v| v.as_str()) {
            conf.endpoint = s.to_string();
        }
        if let Some(s) = spec.params.get("api_path").and_then(|v| v.as_str()) {
            conf.api_path = s.to_string();
        }

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs_f64(conf.timeout_secs))
            .build()
            .source_raw_err(SinkReason::Sink, "build victoriametric client failed")?;
        let write_url = join_endpoint_path(&conf.endpoint, &conf.api_path);
        let mut sink = VictoriaMetricExporter::new(
            write_url,
            client,
            Duration::from_secs_f64(conf.flush_secs),
        );
        // 启动定时 flush 任务：计数器收集与推送解耦，
        sink.start_flush_task();
        Ok(SinkHandle::new(Box::new(sink)))
    }
}

impl SinkDefProvider for VictoriaMetricFactory {
    fn sink_def(&self) -> ConnectorDef {
        ConnectorDef {
            id: "victoriametrics_sink".into(),
            kind: self.kind().into(),
            scope: ConnectorScope::Sink,
            allow_override: vec!["endpoint", "api_path", "flush_secs", "timeout_secs"]
                .into_iter()
                .map(str::to_string)
                .collect(),
            default_params: victoriametric_defaults(),
            origin: Some("wp-connectors:victoriametrics_sink".into()),
        }
    }
}

fn victoriametric_defaults() -> ParamMap {
    let mut params = ParamMap::new();
    params.insert("endpoint".into(), json!("http://127.0.0.1:8428"));
    params.insert("api_path".into(), json!("/api/v1/import/prometheus"));
    // flush_secs 决定推送到 VictoriaMetrics 的时间分辨率，
    // 1s 可获得秒级数据点，适合 rate([20s+]) 的稳定计算。
    params.insert("flush_secs".into(), json!(1));
    params.insert("timeout_secs".into(), json!(5));
    params
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sink_spec(params: &[(&str, serde_json::Value)]) -> SinkSpec {
        let mut map = ParamMap::new();
        for (key, value) in params {
            map.insert((*key).to_string(), value.clone());
        }
        SinkSpec {
            group: "g".into(),
            name: "vm".into(),
            kind: "victoriametrics".into(),
            connector_id: "victoriametrics_sink".into(),
            params: map,
            filter: None,
        }
    }

    #[test]
    fn sink_def_matches_official_template() {
        let def = VictoriaMetricFactory.sink_def();
        assert_eq!(def.id, "victoriametrics_sink");
        assert_eq!(
            def.allow_override,
            vec![
                "endpoint".to_string(),
                "api_path".to_string(),
                "flush_secs".to_string(),
                "timeout_secs".to_string()
            ]
        );
        assert_eq!(
            def.default_params.get("endpoint").and_then(|v| v.as_str()),
            Some("http://127.0.0.1:8428")
        );
        assert_eq!(
            def.default_params.get("api_path").and_then(|v| v.as_str()),
            Some("/api/v1/import/prometheus")
        );
        assert_eq!(
            def.default_params
                .get("flush_secs")
                .and_then(|v| v.as_i64()),
            Some(1)
        );
        assert_eq!(
            def.default_params
                .get("timeout_secs")
                .and_then(|v| v.as_i64()),
            Some(5)
        );
    }

    #[test]
    fn validate_accepts_endpoint_and_api_path() {
        let spec = sink_spec(&[
            ("endpoint", json!("http://localhost:8428")),
            ("api_path", json!("/api/v1/import/prometheus")),
        ]);
        assert!(VictoriaMetricFactory.validate_spec(&spec).is_ok());
    }
}
