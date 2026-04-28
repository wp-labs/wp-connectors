use async_trait::async_trait;
use sea_orm::{ConnectOptions, Database};
use serde_json::{Value, json};
use std::time::Duration;
use wp_conf_base::ConfParser;
use wp_connector_api::{
    ConnectorDef, ConnectorScope, ParamMap, SinkBuildCtx, SinkDefProvider, SinkError, SinkFactory,
    SinkHandle, SinkReason, SinkResult, SinkSpec, SourceBuildCtx, SourceDefProvider, SourceFactory,
    SourceHandle, SourceMeta, SourceReason, SourceResult, SourceSpec, SourceSvcIns, Tags,
};

use crate::WP_SRC_VAL;
use crate::postgres::{
    PostgresSink, PostgresSource, config::PostgresConf,
    source::validate_source_cursor_type_and_start_from,
};

pub struct PostgresSourceFactory;

#[async_trait]
impl SourceFactory for PostgresSourceFactory {
    fn kind(&self) -> &'static str {
        "postgres"
    }

    fn validate_spec(&self, spec: &SourceSpec) -> SourceResult<()> {
        build_postgres_source_conf(spec)?;
        Ok(())
    }

    async fn build(&self, spec: &SourceSpec, _ctx: &SourceBuildCtx) -> SourceResult<SourceSvcIns> {
        let conf = build_postgres_source_conf(spec)?;
        let mut meta_tags = Tags::from_parse(&spec.tags);
        meta_tags.set(WP_SRC_VAL, "postgres");
        let source = PostgresSource::new(spec.name.clone(), meta_tags.clone(), &conf)
            .await
            .map_err(|err| SourceReason::Other(err.to_string()))?;

        let mut meta = SourceMeta::new(spec.name.clone(), spec.kind.clone());
        meta.tags = meta_tags;
        let handle = SourceHandle::new(Box::new(source), meta);
        Ok(SourceSvcIns::new().with_sources(vec![handle]))
    }
}

pub struct PostgresSinkFactory;

#[async_trait]
impl SinkFactory for PostgresSinkFactory {
    fn kind(&self) -> &'static str {
        "postgres"
    }

    fn validate_spec(&self, spec: &SinkSpec) -> SinkResult<()> {
        build_postgres_sink_conf(spec)?;
        Ok(())
    }

    async fn build(&self, spec: &SinkSpec, _ctx: &SinkBuildCtx) -> SinkResult<SinkHandle> {
        let (conf, columns) = build_postgres_sink_conf(spec)?;
        let url = conf.get_database_url();
        let mut opt = ConnectOptions::new(url.clone());

        opt.max_connections(50)
            .min_connections(1)
            .connect_timeout(Duration::from_secs(8))
            .acquire_timeout(Duration::from_secs(8))
            .idle_timeout(Duration::from_secs(8))
            .max_lifetime(Duration::from_secs(8))
            .sqlx_logging(false)
            .map_sqlx_postgres_opts(|opt| opt.statement_cache_capacity(0))
            .sqlx_logging_level(log::LevelFilter::Info);
        let db = Database::connect(opt).await.map_err(|err| {
            SinkError::from(SinkReason::sink(format!("connect postgres fail: {err}")))
        })?;
        let table = conf.table.clone().unwrap_or_else(|| spec.name.clone());
        let sink = PostgresSink::new(db, table, columns);
        Ok(SinkHandle::new(Box::new(sink)))
    }
}

impl SourceDefProvider for PostgresSourceFactory {
    fn source_def(&self) -> ConnectorDef {
        ConnectorDef {
            id: "postgres_src".into(),
            kind: self.kind().into(),
            scope: ConnectorScope::Source,
            allow_override: vec![
                "endpoint",
                "database",
                "schema",
                "table",
                "username",
                "password",
                "batch",
                "cursor_column",
                "cursor_type",
                "start_from",
                "start_from_format",
                "poll_interval_ms",
                "error_backoff_ms",
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
            default_params: postgres_source_defaults(),
            origin: Some("wp-connectors:postgres_source".into()),
        }
    }
}

impl SinkDefProvider for PostgresSinkFactory {
    fn sink_def(&self) -> ConnectorDef {
        ConnectorDef {
            id: "postgres_sink".into(),
            kind: self.kind().into(),
            scope: ConnectorScope::Sink,
            allow_override: vec![
                "endpoint", "database", "table", "username", "batch", "columns",
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
            default_params: postgres_sink_defaults(),
            origin: Some("wp-connectors:postgres_sink".into()),
        }
    }
}

fn postgres_source_defaults() -> ParamMap {
    let mut params = ParamMap::new();
    params.insert("endpoint".into(), json!("localhost:5432"));
    params.insert("database".into(), json!("wp_data"));
    params.insert("schema".into(), json!("public"));
    params.insert("table".into(), json!("wp_events"));
    params.insert("username".into(), json!("root"));
    params.insert("password".into(), json!("dayu"));
    params.insert("batch".into(), json!(5000));
    params.insert("cursor_column".into(), json!("id"));
    params.insert("cursor_type".into(), json!("int"));
    params.insert("poll_interval_ms".into(), json!(1000));
    params.insert("error_backoff_ms".into(), json!(2000));
    params
}

fn postgres_sink_defaults() -> ParamMap {
    let mut params = ParamMap::new();
    params.insert("endpoint".into(), json!("localhost:5432"));
    params.insert("database".into(), json!("wp_data"));
    params.insert("table".into(), json!("wp_events"));
    params.insert("username".into(), json!("root"));
    params.insert("columns".into(), json!(["wp_event_id", "payload"]));
    params.insert("batch_size".into(), json!(1024));
    params
}

fn build_base_postgres_conf_from_params(params: &ParamMap) -> Result<PostgresConf, String> {
    let mut conf = PostgresConf::default();
    if let Some(s) = params.get("endpoint").and_then(Value::as_str) {
        conf.endpoint = s.to_string();
    }
    if let Some(s) = params.get("username").and_then(Value::as_str) {
        conf.username = s.to_string();
    }
    if let Some(s) = params.get("password").and_then(Value::as_str) {
        conf.password = s.to_string();
    }
    if let Some(s) = params.get("database").and_then(Value::as_str) {
        conf.database = s.to_string();
    }
    if let Some(s) = params.get("schema").and_then(Value::as_str) {
        conf.schema = s.to_string();
    }
    if let Some(s) = params.get("table").and_then(Value::as_str) {
        conf.table = Some(s.to_string());
    }
    if let Some(batch) = params.get("batch").and_then(Value::as_u64) {
        conf.batch = Some(batch as usize);
    }
    if let Some(s) = params.get("cursor_column").and_then(Value::as_str) {
        conf.cursor_column = Some(s.to_string());
    }
    if let Some(s) = params.get("cursor_type").and_then(Value::as_str) {
        conf.cursor_type = s.to_string();
    }
    if let Some(s) = params.get("start_from").and_then(Value::as_str) {
        conf.start_from = Some(s.to_string());
    }
    if let Some(s) = params.get("start_from_format").and_then(Value::as_str) {
        conf.start_from_format = Some(s.to_string());
    }
    if let Some(v) = params.get("poll_interval_ms") {
        conf.poll_interval_ms = Some(
            parse_non_negative_u64(v, "postgres.poll_interval_ms")
                .map_err(|err| err.to_string())?,
        );
    }
    if let Some(v) = params.get("error_backoff_ms") {
        conf.error_backoff_ms = Some(
            parse_non_negative_u64(v, "postgres.error_backoff_ms")
                .map_err(|err| err.to_string())?,
        );
    }
    Ok(conf)
}

fn build_postgres_source_conf(spec: &SourceSpec) -> SourceResult<PostgresConf> {
    let conf = build_base_postgres_conf_from_params(&spec.params).map_err(SourceReason::Other)?;
    validate_postgres_source_conf(&conf)?;
    Ok(conf)
}

fn validate_postgres_source_conf(conf: &PostgresConf) -> SourceResult<()> {
    if conf.endpoint.trim().is_empty() {
        return Err(SourceReason::Other("postgres.endpoint must not be empty".into()).into());
    }
    if conf.database.trim().is_empty() {
        return Err(SourceReason::Other("postgres.database must not be empty".into()).into());
    }
    if conf.schema.trim().is_empty() {
        return Err(SourceReason::Other("postgres.schema must not be empty".into()).into());
    }
    let table = conf.table.as_deref().unwrap_or("").trim();
    if table.is_empty() {
        return Err(SourceReason::Other("postgres.table must not be empty".into()).into());
    }
    let cursor_column = conf.cursor_column.as_deref().unwrap_or("").trim();
    if cursor_column.is_empty() {
        return Err(SourceReason::Other("postgres.cursor_column must not be empty".into()).into());
    }

    let batch = conf.batch.unwrap_or(5000);
    if batch == 0 {
        return Err(SourceReason::Other("postgres.batch must be > 0".into()).into());
    }

    let poll_interval_ms = conf.poll_interval_ms.unwrap_or(1000);
    if poll_interval_ms < 100 {
        return Err(SourceReason::Other("postgres.poll_interval_ms must be >= 100".into()).into());
    }
    let error_backoff_ms = conf.error_backoff_ms.unwrap_or(2000);
    if error_backoff_ms < 200 {
        return Err(SourceReason::Other("postgres.error_backoff_ms must be >= 200".into()).into());
    }

    validate_source_cursor_type_and_start_from(
        &conf.cursor_type,
        conf.start_from.as_deref(),
        conf.start_from_format.as_deref(),
    )
    .map_err(|err| SourceReason::Other(err.to_string()))?;

    Ok(())
}

fn build_postgres_sink_conf(spec: &SinkSpec) -> SinkResult<(PostgresConf, Vec<String>)> {
    let mut conf = build_base_postgres_conf_from_params(&spec.params).map_err(SinkReason::sink)?;

    if let Some(i) = spec.params.get("batch_size").and_then(Value::as_u64) {
        conf.batch = Some(i as usize);
    }

    let endpoint = conf.endpoint.trim();
    if endpoint.is_empty() {
        return Err(SinkReason::sink("postgres.endpoint must not be empty").into());
    }
    let database = conf.database.trim();
    if database.is_empty() {
        return Err(SinkReason::sink("postgres.database must not be empty").into());
    }
    if let Some(i) = spec.params.get("batch").and_then(Value::as_i64)
        && i <= 0
    {
        return Err(SinkReason::sink("postgres.batch must be > 0").into());
    }

    let columns = parse_columns(spec.params.get("columns"))?;
    Ok((conf, columns))
}

fn parse_columns(value: Option<&Value>) -> SinkResult<Vec<String>> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    let Some(arr) = value.as_array() else {
        return Err(SinkReason::sink("postgres.columns must be an array").into());
    };
    let mut out = Vec::with_capacity(arr.len());
    for item in arr {
        if let Some(s) = item.as_str() {
            out.push(s.to_string());
        } else {
            return Err(SinkReason::sink("postgres.columns entries must be string").into());
        }
    }
    Ok(out)
}

fn parse_non_negative_u64(value: &Value, field: &str) -> Result<u64, SourceReason> {
    value
        .as_u64()
        .ok_or_else(|| SourceReason::Other(format!("{field} must be a non-negative integer")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn build_source_spec(params: BTreeMap<String, Value>) -> SourceSpec {
        SourceSpec {
            name: "postgres_source".into(),
            kind: "postgres".into(),
            connector_id: "connector".into(),
            params,
            tags: vec![],
        }
    }

    #[test]
    fn validate_source_requires_cursor_column() {
        let factory = PostgresSourceFactory;
        let spec = build_source_spec(BTreeMap::from([
            ("endpoint".into(), json!("localhost:5432")),
            ("database".into(), json!("db")),
            ("table".into(), json!("events")),
        ]));
        let err = factory
            .validate_spec(&spec)
            .expect_err("missing cursor_column should fail");
        assert!(
            err.to_string()
                .contains("postgres.cursor_column must not be empty")
        );
    }

    #[test]
    fn validate_source_accepts_time_start_from() {
        let factory = PostgresSourceFactory;
        let spec = build_source_spec(BTreeMap::from([
            ("endpoint".into(), json!("localhost:5432")),
            ("database".into(), json!("db")),
            ("table".into(), json!("events")),
            ("cursor_column".into(), json!("log_time")),
            ("cursor_type".into(), json!("time")),
            ("start_from".into(), json!("2025-04-15 12:30:45")),
        ]));
        factory.validate_spec(&spec).expect("valid time spec");
    }

    #[test]
    fn validate_source_accepts_time_start_from_format() {
        let factory = PostgresSourceFactory;
        let spec = build_source_spec(BTreeMap::from([
            ("endpoint".into(), json!("localhost:5432")),
            ("database".into(), json!("db")),
            ("table".into(), json!("events")),
            ("cursor_column".into(), json!("log_time")),
            ("cursor_type".into(), json!("time")),
            ("start_from".into(), json!("1776328285")),
            ("start_from_format".into(), json!("unix_s")),
        ]));
        factory
            .validate_spec(&spec)
            .expect("time cursor should accept start_from_format");
    }

    #[test]
    fn validate_source_rejects_start_from_format_without_start_from() {
        let factory = PostgresSourceFactory;
        let spec = build_source_spec(BTreeMap::from([
            ("endpoint".into(), json!("localhost:5432")),
            ("database".into(), json!("db")),
            ("table".into(), json!("events")),
            ("cursor_column".into(), json!("log_time")),
            ("cursor_type".into(), json!("time")),
            ("start_from_format".into(), json!("unix_s")),
        ]));
        let err = factory
            .validate_spec(&spec)
            .expect_err("start_from_format without start_from should fail");
        assert!(
            err.to_string()
                .contains("postgres.start_from_format requires postgres.start_from")
        );
    }

    #[test]
    fn validate_source_rejects_start_from_format_for_int_cursor() {
        let factory = PostgresSourceFactory;
        let spec = build_source_spec(BTreeMap::from([
            ("endpoint".into(), json!("localhost:5432")),
            ("database".into(), json!("db")),
            ("table".into(), json!("events")),
            ("cursor_column".into(), json!("id")),
            ("cursor_type".into(), json!("int")),
            ("start_from".into(), json!("42")),
            ("start_from_format".into(), json!("unix_s")),
        ]));
        let err = factory
            .validate_spec(&spec)
            .expect_err("int cursor should reject start_from_format");
        assert!(
            err.to_string()
                .contains("postgres.start_from_format is only supported for time cursor")
        );
    }
}
