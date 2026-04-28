use async_trait::async_trait;
use serde_json::{Value, json};
use wp_connector_api::{
    ConnectorDef, ConnectorScope, ParamMap, SinkBuildCtx, SinkDefProvider, SinkError, SinkFactory,
    SinkHandle, SinkReason, SinkResult, SinkSpec, SourceBuildCtx, SourceDefProvider, SourceFactory,
    SourceHandle, SourceMeta, SourceReason, SourceResult, SourceSpec, SourceSvcIns,
};

use crate::count::{CountSink, CountSource};

pub struct CountSourceFactory;

#[async_trait]
impl SourceFactory for CountSourceFactory {
    fn kind(&self) -> &'static str {
        "count"
    }

    fn validate_spec(&self, spec: &SourceSpec) -> SourceResult<()> {
        build_count_source_config(spec)?;
        Ok(())
    }

    async fn build(&self, spec: &SourceSpec, _ctx: &SourceBuildCtx) -> SourceResult<SourceSvcIns> {
        let config = build_count_source_config(spec)?;
        let meta = SourceMeta::new(spec.name.clone(), spec.kind.clone());
        let source = CountSource::new(config.batch_size, config.total, config.interval);

        Ok(SourceSvcIns::new().with_sources(vec![SourceHandle::new(Box::new(source), meta)]))
    }
}

impl SourceDefProvider for CountSourceFactory {
    fn source_def(&self) -> ConnectorDef {
        ConnectorDef {
            id: "count_src".into(),
            kind: self.kind().into(),
            scope: ConnectorScope::Source,
            allow_override: vec!["batch", "total", "interval_ms"]
                .into_iter()
                .map(str::to_string)
                .collect(),
            default_params: count_source_defaults(),
            origin: Some("wp-connectors:count_source".into()),
        }
    }
}

pub struct CountSinkFactory;

#[async_trait]
impl SinkFactory for CountSinkFactory {
    fn kind(&self) -> &'static str {
        "count"
    }

    fn validate_spec(&self, _spec: &SinkSpec) -> SinkResult<()> {
        Ok(())
    }

    async fn build(&self, _spec: &SinkSpec, _ctx: &SinkBuildCtx) -> SinkResult<SinkHandle> {
        let sink = CountSink::new().await.map_err(|err| {
            SinkError::from(SinkReason::sink(format!("init count sink failed: {err}")))
        })?;

        Ok(SinkHandle::new(Box::new(sink)))
    }
}

impl SinkDefProvider for CountSinkFactory {
    fn sink_def(&self) -> ConnectorDef {
        ConnectorDef {
            id: "count_sink".into(),
            kind: self.kind().into(),
            scope: ConnectorScope::Sink,
            allow_override: vec![].into_iter().map(str::to_string).collect(),
            default_params: count_defaults(),
            origin: Some("wp-connectors:count_sink".into()),
        }
    }
}

fn count_defaults() -> ParamMap {
    ParamMap::new()
}

struct CountSourceConfig {
    batch_size: usize,
    total: Option<u64>,
    interval: std::time::Duration,
}

fn build_count_source_config(spec: &SourceSpec) -> SourceResult<CountSourceConfig> {
    let batch_size = parse_u64_param(spec, "batch")?.unwrap_or(1) as usize;
    if batch_size == 0 {
        return Err(SourceReason::Other("count.batch must be > 0".into()).into());
    }

    let total = parse_u64_param(spec, "total")?;
    let interval_ms = parse_u64_param(spec, "interval_ms")?.unwrap_or(1000);

    Ok(CountSourceConfig {
        batch_size,
        total,
        interval: std::time::Duration::from_millis(interval_ms),
    })
}

fn parse_u64_param(spec: &SourceSpec, key: &str) -> SourceResult<Option<u64>> {
    match spec.params.get(key) {
        None => Ok(None),
        Some(Value::Number(number)) => number.as_u64().map(Some).ok_or_else(|| {
            SourceReason::Other(format!("count.{key} must be a non-negative integer")).into()
        }),
        Some(_) => Err(SourceReason::Other(format!("count.{key} must be an integer")).into()),
    }
}

fn count_source_defaults() -> ParamMap {
    let mut params = ParamMap::new();
    params.insert("batch".into(), json!(1));
    params.insert("interval_ms".into(), json!(1000));
    params
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn build_source_spec(params: BTreeMap<String, Value>) -> SourceSpec {
        SourceSpec {
            name: "count_source".into(),
            kind: "count".into(),
            connector_id: "connector".into(),
            params,
            tags: vec![],
        }
    }

    #[test]
    fn validate_count_source_defaults() {
        let factory = CountSourceFactory;
        factory
            .validate_spec(&build_source_spec(BTreeMap::new()))
            .expect("validate defaults");
    }

    #[test]
    fn reject_zero_batch() {
        let factory = CountSourceFactory;
        let spec = build_source_spec(BTreeMap::from([("batch".into(), json!(0))]));
        let err = factory
            .validate_spec(&spec)
            .expect_err("zero batch should fail");
        assert!(err.to_string().contains("count.batch must be > 0"));
    }
}
