use async_trait::async_trait;
use serde_json::json;
use wp_connector_api::{
    register_sink_factory, ConnectorDef, ConnectorDefProvider, ConnectorScope, ParamMap,
    SinkBuildCtx, SinkFactory, SinkHandle, SinkReason, SinkResult, SinkSpec,
};

use super::sink::ElasticsearchSink;

pub fn register_builder() {
    register_sink_factory(ElasticsearchFactory);
}

pub fn register_factory_only() {
    register_sink_factory(ElasticsearchFactory);
}

struct ElasticsearchFactory;

#[async_trait]
impl SinkFactory for ElasticsearchFactory {
    fn kind(&self) -> &'static str {
        "elasticsearch"
    }
    fn validate_spec(&self, spec: &SinkSpec) -> SinkResult<()> {
        let endpoint = spec
            .params
            .get("endpoint")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if endpoint.trim().is_empty() {
            return Err(SinkReason::sink("elasticsearch.endpoint must not be empty").into());
        }
        if let Some(i) = spec.params.get("batch").and_then(|v| v.as_i64()) {
            if i <= 0 {
                return Err(SinkReason::sink("elasticsearch.batch must be > 0").into());
            }
        }
        Ok(())
    }
    async fn build(&self, spec: &SinkSpec, _ctx: &SinkBuildCtx) -> SinkResult<SinkHandle> {
        let mut tbl = toml::map::Map::new();
        if let Some(s) = spec.params.get("endpoint").and_then(|v| v.as_str()) {
            tbl.insert("endpoint".to_string(), toml::Value::String(s.to_string()));
        }
        if let Some(s) = spec.params.get("username").and_then(|v| v.as_str()) {
            tbl.insert("username".to_string(), toml::Value::String(s.to_string()));
        }
        if let Some(s) = spec.params.get("password").and_then(|v| v.as_str()) {
            tbl.insert("password".to_string(), toml::Value::String(s.to_string()));
        }
        if let Some(i) = spec.params.get("batch").and_then(|v| v.as_i64()) {
            tbl.insert("batch".to_string(), toml::Value::Integer(i));
        }
        if let Some(s) = spec.params.get("table").and_then(|v| v.as_str()) {
            tbl.insert("table".to_string(), toml::Value::String(s.to_string()));
        }
        let value = toml::Value::Table(tbl);
        let serialized = toml::to_string(&value)
            .map_err(|err| SinkReason::sink(format!("encode elasticsearch conf failed: {err}")).into())?;
        let conf: wp_config::structure::io::Elasticsearch = toml::from_str(&serialized)
            .map_err(|err| SinkReason::sink(format!("parse elasticsearch conf failed: {err}")).into())?;
        let table = conf.table.clone().unwrap_or_else(|| spec.name.clone());
        let sink = ElasticsearchSink::new(conf, table);
        Ok(SinkHandle::new(Box::new(sink)))
    }
}

impl ConnectorDefProvider for ElasticsearchFactory {
    fn sink_def(&self) -> ConnectorDef {
        ConnectorDef {
            id: "elasticsearch_sink".into(),
            kind: self.kind().into(),
            scope: ConnectorScope::Sink,
            allow_override: vec!["endpoint", "username", "password", "table", "batch"]
                .into_iter()
                .map(str::to_string)
                .collect(),
            default_params: elasticsearch_defaults(),
            origin: Some("wp-connectors:elasticsearch_sink".into()),
        }
    }
}

fn elasticsearch_defaults() -> ParamMap {
    let mut params = ParamMap::new();
    params.insert("endpoint".into(), json!("http://localhost:9200"));
    params.insert("username".into(), json!("elastic"));
    params.insert("table".into(), json!("wp_events"));
    params.insert("batch".into(), json!(500));
    params
}
