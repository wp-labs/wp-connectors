use std::time::Duration;

use async_trait::async_trait;
use wp_connector_api::{SinkBuildCtx, SinkFactory, SinkHandle, SinkSpec};

use super::config::VictoriaMetric;
use super::exporter::VictoriaMetricExporter;

pub struct VictoriaMetricFactory;

#[async_trait]
impl SinkFactory for VictoriaMetricFactory {
    fn kind(&self) -> &'static str {
        "victoriametric"
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
        let mut conf = VictoriaMetric::default();
        if let Some(s) = spec.params.get("endpoint").and_then(|v| v.as_str()) {
            conf.endpoint = s.to_string();
        }
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
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()?;
        let mut sink = VictoriaMetricExporter::new(
            conf.endpoint.clone(),
            conf.insert_path.clone(),
            client,
            Duration::from_secs_f64(conf.flush_interval_secs),
        );
        sink.start_flush_task();
        Ok(SinkHandle::new(Box::new(sink)))
    }
}
