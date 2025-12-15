use async_trait::async_trait;
use wp_connector_api::{register_sink_factory, SinkBuildCtx, SinkFactory, SinkHandle, SinkSpec};

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
    fn validate_spec(&self, spec: &SinkSpec) -> anyhow::Result<()> {
        let endpoint = spec
            .params
            .get("endpoint")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if endpoint.trim().is_empty() {
            anyhow::bail!("clickhouse.endpoint must not be empty");
        }
        let database = spec
            .params
            .get("database")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if database.trim().is_empty() {
            anyhow::bail!("clickhouse.database must not be empty");
        }
        if let Some(i) = spec.params.get("batch").and_then(|v| v.as_i64()) {
            if i <= 0 {
                anyhow::bail!("clickhouse.batch must be > 0");
            }
        }
        Ok(())
    }
    async fn build(&self, spec: &SinkSpec, _ctx: &SinkBuildCtx) -> anyhow::Result<SinkHandle> {
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
        let conf: wp_config::structure::io::Clickhouse =
            toml::from_str(&toml::to_string(&toml::Value::Table(tbl))?)?;
        let table = conf.table.clone().unwrap_or_else(|| spec.name.clone());
        let sink = ClickhouseSink::new(conf, table);
        Ok(SinkHandle::new(Box::new(sink)))
    }
}
