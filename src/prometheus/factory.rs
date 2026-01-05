#![allow(dead_code)] // Prometheus sink 目前仅在部分部署启用

use async_trait::async_trait;
use serde_json::json;
use wp_connector_api::{
    ConnectorDef, ConnectorScope, ParamMap, SinkBuildCtx, SinkDefProvider, SinkFactory, SinkHandle,
    SinkReason, SinkResult, SinkSpec,
};

use super::config::Prometheus;
use super::exporter::PrometheusExporter;

struct PrometheusFactory;

#[async_trait]
impl SinkFactory for PrometheusFactory {
    fn kind(&self) -> &'static str {
        "prometheus"
    }
    fn validate_spec(&self, spec: &SinkSpec) -> SinkResult<()> {
        let endpoint = spec
            .params
            .get("endpoint")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if endpoint.trim().is_empty() {
            return Err(SinkReason::sink("prometheus.endpoint must not be empty").into());
        }
        Ok(())
    }
    async fn build(&self, spec: &SinkSpec, _ctx: &SinkBuildCtx) -> SinkResult<SinkHandle> {
        let mut conf = Prometheus::default();
        if let Some(s) = spec.params.get("endpoint").and_then(|v| v.as_str()) {
            conf.endpoint = s.to_string();
        }
        if let Some(s) = spec
            .params
            .get("source_key_format")
            .and_then(|v| v.as_str())
        {
            conf.source_key_format = s.to_string();
        }
        if let Some(s) = spec.params.get("sink_key_format").and_then(|v| v.as_str()) {
            conf.sink_key_format = s.to_string();
        }
        let endpoint = conf.endpoint.clone();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async move {
                let _ = PrometheusExporter::metrics_service(endpoint).await;
            });
        });
        let sink = PrometheusExporter {
            source_key_format: conf.source_key_format.clone(),
            sink_key_format: conf.sink_key_format.clone(),
        };
        Ok(SinkHandle::new(Box::new(sink)))
    }
}

impl SinkDefProvider for PrometheusFactory {
    fn sink_def(&self) -> ConnectorDef {
        ConnectorDef {
            id: "prometheus_sink".into(),
            kind: self.kind().into(),
            scope: ConnectorScope::Sink,
            allow_override: vec!["endpoint", "source_key_format", "sink_key_format"]
                .into_iter()
                .map(str::to_string)
                .collect(),
            default_params: prometheus_defaults(),
            origin: Some("wp-connectors:prometheus_sink".into()),
        }
    }
}

fn prometheus_defaults() -> ParamMap {
    let mut params = ParamMap::new();
    params.insert("endpoint".into(), json!("0.0.0.0:9898"));
    params.insert("source_key_format".into(), json!("source"));
    params.insert("sink_key_format".into(), json!("sink"));
    params
}
