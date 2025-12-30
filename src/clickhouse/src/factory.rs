use async_trait::async_trait;
use serde_json::json;
use wp_connector_api::{
    register_sink_factory, ConnectorDef, ConnectorDefProvider, ConnectorScope, ParamMap,
    SinkBuildCtx, SinkFactory, SinkHandle, SinkReason, SinkResult, SinkSpec,
};

use super::sink::ClickhouseSink;

pub fn register_builder() {
    register_sink_factory(ClickhouseFactory);
}

pub fn register_factory_only() {
    register_sink_factory(ClickhouseFactory);
}

struct ClickhouseFactory;

#[async_trait]
impl SinkFactory for ClickhouseFactory {
    fn kind(&self) -> &'static str {
        "clickhouse"
    }
    fn validate_spec(&self, spec: &SinkSpec) -> SinkResult<()> {
        let endpoint = spec
            .params
            .get("endpoint")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if endpoint.trim().is_empty() {
            return Err(SinkReason::sink("clickhouse.endpoint must not be empty").into());
        }
        let database = spec
            .params
            .get("database")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if database.trim().is_empty() {
            return Err(SinkReason::sink("clickhouse.database must not be empty").into());
        }
        if let Some(i) = spec.params.get("batch").and_then(|v| v.as_i64()) {
            if i <= 0 {
                return Err(SinkReason::sink("clickhouse.batch must be > 0").into());
            }
        }
        Ok(())
    }
    async fn build(&self, spec: &SinkSpec, _ctx: &SinkBuildCtx) -> SinkResult<SinkHandle> {
        // Build Clickhouse conf via serde (fields有私有的)
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
        if let Some(s) = spec.params.get("database").and_then(|v| v.as_str()) {
            tbl.insert("database".to_string(), toml::Value::String(s.to_string()));
        }
        if let Some(i) = spec.params.get("batch").and_then(|v| v.as_i64()) {
            tbl.insert("batch".to_string(), toml::Value::Integer(i));
        }
        if let Some(s) = spec.params.get("table").and_then(|v| v.as_str()) {
            tbl.insert("table".to_string(), toml::Value::String(s.to_string()));
        }
        let value = toml::Value::Table(tbl);
        let serialized = toml::to_string(&value)
            .map_err(|err| SinkReason::sink(format!("encode clickhouse conf failed: {err}")).into())?;
        let conf: wp_config::structure::io::Clickhouse = toml::from_str(&serialized)
            .map_err(|err| SinkReason::sink(format!("parse clickhouse conf failed: {err}")).into())?;
        let table = conf.table.clone().unwrap_or_else(|| spec.name.clone());
        let sink = ClickhouseSink::new(conf, table);
        Ok(SinkHandle::new(Box::new(sink)))
    }
}

impl ConnectorDefProvider for ClickhouseFactory {
    fn sink_def(&self) -> ConnectorDef {
        ConnectorDef {
            id: "clickhouse_sink".into(),
            kind: self.kind().into(),
            scope: ConnectorScope::Sink,
            allow_override: vec!["endpoint", "database", "table", "username", "batch"]
                .into_iter()
                .map(str::to_string)
                .collect(),
            default_params: clickhouse_defaults(),
            origin: Some("wp-connectors:clickhouse_sink".into()),
        }
    }
}

fn clickhouse_defaults() -> ParamMap {
    let mut params = ParamMap::new();
    params.insert("endpoint".into(), json!("http://localhost:8123"));
    params.insert("database".into(), json!("default"));
    params.insert("table".into(), json!("wp_events"));
    params.insert("username".into(), json!("default"));
    params.insert("batch".into(), json!(1000));
    params
}
