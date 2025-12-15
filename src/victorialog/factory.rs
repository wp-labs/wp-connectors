use std::time::Duration;

use async_trait::async_trait;
use wp_connector_api::{SinkBuildCtx, SinkFactory, SinkHandle, SinkSpec};
use wp_model_core::model::fmt_def::TextFmt;

use super::config::VictoriaLog;
use super::sink::VictoriaLogSink;

pub struct VictoriaLogSinkFactory;

#[async_trait]
impl SinkFactory for VictoriaLogSinkFactory {
    fn kind(&self) -> &'static str {
        "victorialog"
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
            .build()?;
        let sink =
            VictoriaLogSink::new(conf.endpoint.clone(), conf.insert_path.clone(), client, fmt);
        Ok(SinkHandle::new(Box::new(sink)))
    }
}
