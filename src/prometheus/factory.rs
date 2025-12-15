#![allow(dead_code)] // Prometheus sink 目前仅在部分部署启用

use async_trait::async_trait;
use wp_connector_api::{SinkBuildCtx, SinkFactory, SinkHandle, SinkSpec};

use super::config::Prometheus;
use super::exporter::PrometheusExporter;

struct PrometheusFactory;

#[async_trait]
impl SinkFactory for PrometheusFactory {
    fn kind(&self) -> &'static str {
        "prometheus"
    }
    fn validate_spec(&self, spec: &SinkSpec) -> anyhow::Result<()> {
        let endpoint = spec
            .params
            .get("endpoint")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if endpoint.trim().is_empty() {
            anyhow::bail!("prometheus.endpoint must not be empty");
        }
        Ok(())
    }
    async fn build(&self, spec: &SinkSpec, _ctx: &SinkBuildCtx) -> anyhow::Result<SinkHandle> {
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
