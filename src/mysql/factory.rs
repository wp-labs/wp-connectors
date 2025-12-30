use crate::mysql::config::MysqlConf;

use super::sink::MysqlSink;
use super::source::MysqlSource;
use async_trait::async_trait;
use sea_orm::{ConnectOptions, Database};
use serde_json::json;
use std::time::Duration;
use wp_connector_api::{
    ConnectorDef, ConnectorDefProvider, ConnectorScope, ParamMap, SinkBuildCtx, SinkError,
    SinkFactory, SinkHandle, SinkReason, SinkResult, SinkSpec, SourceFactory, SourceHandle,
    SourceMeta, SourceReason, SourceResult, SourceSvcIns, Tags,
};
use wp_model_core::model::TagSet;

pub struct MySQLSourceFactory;

#[async_trait]
impl wp_connector_api::SourceFactory for MySQLSourceFactory {
    fn kind(&self) -> &'static str {
        "mysql"
    }

    fn validate_spec(&self, spec: &wp_connector_api::SourceSpec) -> SourceResult<()> {
        let endpoint = spec
            .params
            .get("endpoint")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if endpoint.trim().is_empty() {
            return Err(SourceReason::Other("mysql.endpoint must not be empty".into()).into());
        }

        let database = spec
            .params
            .get("database")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if database.trim().is_empty() {
            return Err(SourceReason::Other("mysql.database must not be empty".into()).into());
        }

        Ok(())
    }

    async fn build(
        &self,
        spec: &wp_connector_api::SourceSpec,
        _ctx: &wp_connector_api::SourceBuildCtx,
    ) -> SourceResult<SourceSvcIns> {
        let mut conf = MysqlConf::default();

        if let Some(s) = spec.params.get("endpoint").and_then(|v| v.as_str()) {
            conf.endpoint = s.to_string();
        }
        if let Some(s) = spec.params.get("database").and_then(|v| v.as_str()) {
            conf.database = s.to_string();
        }
        if let Some(username) = spec.params.get("username").and_then(|v| v.as_str()) {
            conf.username = username.to_string();
        }
        if let Some(password) = spec.params.get("password").and_then(|v| v.as_str()) {
            conf.password = password.to_string();
        }
        // Prefer unsigned for JSON numbers here to avoid negative -> usize casts
        if let Some(batch) = spec.params.get("batch").and_then(|v| v.as_u64()) {
            conf.batch = Some(batch as usize);
        }
        if let Some(database) = spec.params.get("database").and_then(|v| v.as_str()) {
            conf.database = database.to_string();
        }
        if let Some(table) = spec.params.get("table").and_then(|v| v.as_str()) {
            conf.table = Some(table.to_string());
        }
        let (mut tag_set, mut meta_tags) = extract_spec_tags(&spec.tags);
        tag_set.append("access_source", "mysql");
        meta_tags.set("access_source", "mysql");
        let source = MysqlSource::new(spec.name.clone(), tag_set, &conf)
            .await
            .map_err(|err| SourceReason::Other(err.to_string()))?;

        let mut meta = SourceMeta::new(spec.name.clone(), spec.kind.clone());
        meta.tags = meta_tags;
        let handle = SourceHandle::new(Box::new(source), meta);
        Ok(SourceSvcIns::new().with_sources(vec![handle]))
    }
}

pub struct MySQLSinkFactory;

#[async_trait]
impl SinkFactory for MySQLSinkFactory {
    fn kind(&self) -> &'static str {
        "mysql"
    }
    fn validate_spec(&self, spec: &SinkSpec) -> SinkResult<()> {
        let endpoint = spec
            .params
            .get("endpoint")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if endpoint.trim().is_empty() {
            return Err(SinkReason::sink("mysql.endpoint must not be empty").into());
        }
        let database = spec
            .params
            .get("database")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if database.trim().is_empty() {
            return Err(SinkReason::sink("mysql.database must not be empty").into());
        }
        if let Some(i) = spec.params.get("batch").and_then(|v| v.as_i64())
            && i <= 0
        {
            return Err(SinkReason::sink("mysql.batch must be > 0").into());
        }
        Ok(())
    }
    async fn build(&self, spec: &SinkSpec, _ctx: &SinkBuildCtx) -> SinkResult<SinkHandle> {
        // Build Mysql conf from flat params
        let mut conf = MysqlConf::default();
        if let Some(s) = spec.params.get("endpoint").and_then(|v| v.as_str()) {
            conf.endpoint = s.to_string();
        }
        if let Some(s) = spec.params.get("username").and_then(|v| v.as_str()) {
            conf.username = s.to_string();
        }
        if let Some(s) = spec.params.get("password").and_then(|v| v.as_str()) {
            conf.password = s.to_string();
        }
        if let Some(s) = spec.params.get("database").and_then(|v| v.as_str()) {
            conf.database = s.to_string();
        }
        if let Some(s) = spec.params.get("table").and_then(|v| v.as_str()) {
            conf.table = Some(s.to_string());
        }
        // Use unsigned extraction to match usize semantics
        if let Some(i) = spec.params.get("batch").and_then(|v| v.as_u64()) {
            conf.batch = Some(i as usize);
        }
        // columns 列表在新版配置中不在 conf 中，作为外部参数传入 sink
        let mut columns: Vec<String> =
            if let Some(arr) = spec.params.get("columns").and_then(|v| v.as_array()) {
                let mut out = Vec::with_capacity(arr.len());
                for item in arr {
                    if let Some(s) = item.as_str() {
                        out.push(s.to_string());
                    } else {
                        return Err(SinkReason::sink("mysql.columns entries must be string").into());
                    }
                }
                out
            } else {
                Vec::new()
            };

        // 内置主键 wp_event_id 必须包含在 columns 中
        if !columns.contains(&"wp_event_id".to_string()) {
            columns.push("wp_event_id".to_string());
        }
        let url = conf.get_database_url();
        let mut opt = ConnectOptions::new(url.clone());
        opt.max_connections(10)
            .min_connections(1)
            .connect_timeout(Duration::from_secs(8))
            .acquire_timeout(Duration::from_secs(8))
            .idle_timeout(Duration::from_secs(8))
            .max_lifetime(Duration::from_secs(8))
            .sqlx_logging(false)
            .sqlx_logging_level(log::LevelFilter::Info);
        let db = Database::connect(opt).await.map_err(|err| {
            SinkError::from(SinkReason::sink(format!("connect mysql fail: {err}")))
        })?;
        let table = conf.table.clone().unwrap_or_else(|| spec.name.clone());
        let sink = MysqlSink::new(db, table, columns, conf.batch, url);
        Ok(SinkHandle::new(Box::new(sink)))
    }
}

fn extract_spec_tags(raw_tags: &[String]) -> (TagSet, Tags) {
    let mut tag_set = TagSet::default();
    let mut src_tags = Tags::new();
    for raw in raw_tags {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            continue;
        }
        let (key, value) = if let Some((k, v)) = trimmed.split_once('=') {
            (k.trim(), v.trim())
        } else {
            (trimmed, "")
        };
        if key.is_empty() {
            continue;
        }
        tag_set.append(key, value);
        src_tags.set(key.to_string(), value.to_string());
    }
    (tag_set, src_tags)
}

impl ConnectorDefProvider for MySQLSourceFactory {
    fn source_def(&self) -> ConnectorDef {
        ConnectorDef {
            id: "mysql_src".into(),
            kind: self.kind().into(),
            scope: ConnectorScope::Source,
            allow_override: vec!["endpoint", "database", "table", "username", "batch"]
                .into_iter()
                .map(str::to_string)
                .collect(),
            default_params: mysql_source_defaults(),
            origin: Some("wp-connectors:mysql_source".into()),
        }
    }
}

impl ConnectorDefProvider for MySQLSinkFactory {
    fn sink_def(&self) -> ConnectorDef {
        ConnectorDef {
            id: "mysql_sink".into(),
            kind: self.kind().into(),
            scope: ConnectorScope::Sink,
            allow_override: vec![
                "endpoint", "database", "table", "username", "batch", "columns",
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
            default_params: mysql_sink_defaults(),
            origin: Some("wp-connectors:mysql_sink".into()),
        }
    }
}

fn mysql_source_defaults() -> ParamMap {
    let mut params = ParamMap::new();
    params.insert("endpoint".into(), json!("mysql://localhost:3306"));
    params.insert("database".into(), json!("wp_data"));
    params.insert("table".into(), json!("wp_events"));
    params.insert("username".into(), json!("root"));
    params.insert("batch".into(), json!(1024));
    params
}

fn mysql_sink_defaults() -> ParamMap {
    let mut params = ParamMap::new();
    params.insert("endpoint".into(), json!("mysql://localhost:3306"));
    params.insert("database".into(), json!("wp_data"));
    params.insert("table".into(), json!("wp_events"));
    params.insert("username".into(), json!("root"));
    params.insert("batch".into(), json!(1000));
    params.insert("columns".into(), json!(["wp_event_id", "payload"]));
    params
}
