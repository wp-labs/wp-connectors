use super::common::connect_shared;
use super::config::{DmdbConnConf, DmdbSinkConf, DmdbSourceConf};
use super::sink::DmdbSink;
use super::source::{
    DmdbReason, DmdbResult, DmdbSource, dmdb_err, validate_source_cursor_type_and_start_from,
};
use async_trait::async_trait;
use serde_json::{Value, json};
use wp_conf_base::ConfParser;
use wp_connector_api::{
    ConnectorDef, ConnectorScope, ParamMap, SinkBuildCtx, SinkDefProvider, SinkError, SinkFactory,
    SinkHandle, SinkReason, SinkResult, SinkSpec, SourceBuildCtx, SourceDefProvider, SourceFactory,
    SourceHandle, SourceMeta, SourceReason, SourceResult, SourceSpec, SourceSvcIns, Tags,
};

use crate::WP_SRC_VAL;

const DEFAULT_SOURCE_BATCH: usize = 1000;
const DEFAULT_SOURCE_POLL_INTERVAL_MS: u64 = 1000;
const DEFAULT_SOURCE_ERROR_BACKOFF_MS: u64 = 2000;
const DEFAULT_SINK_BATCH_SIZE: usize = 1024;
const DEFAULT_CONNECT_TIMEOUT_SECS: u64 = 8;
const MIN_POLL_INTERVAL_MS: u64 = 100;
const MIN_ERROR_BACKOFF_MS: u64 = 200;

type ParseResult<T> = Result<T, String>;

/// 达梦 Source 工厂：负责配置校验、实例化与 connector 定义暴露。
pub struct DmdbSourceFactory;
/// 达梦 Sink 工厂：负责配置校验、实例化与 connector 定义暴露。
pub struct DmdbSinkFactory;

/// 运行时支持的连接方式，按优先级择一生效。
enum DmdbConnectionMode {
    ConnectionString(String),
    Endpoint {
        endpoint: String,
        driver: String,
        username: String,
        password: String,
    },
    Dsn {
        dsn: String,
        username: String,
        password: String,
    },
}

// ===== 对外工厂实现 =====

#[async_trait]
impl SourceFactory for DmdbSourceFactory {
    fn kind(&self) -> &'static str {
        "dmdb"
    }

    fn validate_spec(&self, spec: &SourceSpec) -> SourceResult<()> {
        build_dmdb_source_conf(spec)?;
        Ok(())
    }

    async fn build(&self, spec: &SourceSpec, _ctx: &SourceBuildCtx) -> SourceResult<SourceSvcIns> {
        let conf = build_dmdb_source_conf(spec)?;
        let mut meta_tags = Tags::from_parse(&spec.tags);
        meta_tags.set(WP_SRC_VAL, "dmdb");
        let source = DmdbSource::new(spec.name.clone(), meta_tags.clone(), &conf).await?;

        let mut meta = SourceMeta::new(spec.name.clone(), spec.kind.clone());
        meta.tags = meta_tags;
        let handle = SourceHandle::new(Box::new(source), meta);
        Ok(SourceSvcIns::new().with_sources(vec![handle]))
    }
}

#[async_trait]
impl SinkFactory for DmdbSinkFactory {
    fn kind(&self) -> &'static str {
        "dmdb"
    }

    fn validate_spec(&self, spec: &SinkSpec) -> SinkResult<()> {
        validate_dmdb_sink_spec(spec)
    }

    async fn build(&self, spec: &SinkSpec, _ctx: &SinkBuildCtx) -> SinkResult<SinkHandle> {
        let conf = build_dmdb_sink_conf(spec)?;
        let columns = sink_columns_from_spec(spec)?;
        let table = conf
            .table
            .clone()
            .ok_or_else(|| SinkError::from(SinkReason::sink("dmdb.table must be provided")))?;
        let batch_size = conf.normalized_batch_size();
        let query_timeout_sec = conf.conn.query_timeout_secs;
        let schema = conf.conn.schema.clone();
        let connection = connect_shared(&conf.conn).await.map_err(|err| {
            SinkError::from(SinkReason::sink(format!("connect dmdb fail: {err}")))
        })?;

        let sink = DmdbSink::new(
            connection,
            conf,
            schema,
            table,
            columns,
            batch_size,
            query_timeout_sec,
        );
        Ok(SinkHandle::new(Box::new(sink)))
    }
}

// ===== Connector 定义 =====

impl SourceDefProvider for DmdbSourceFactory {
    fn source_def(&self) -> ConnectorDef {
        ConnectorDef {
            id: "dmdb_src".into(),
            kind: self.kind().into(),
            scope: ConnectorScope::Source,
            allow_override: dmdb_source_allow_override(),
            default_params: dmdb_source_defaults(),
            origin: Some("wp-connectors:dmdb_source".into()),
        }
    }
}

impl SinkDefProvider for DmdbSinkFactory {
    fn sink_def(&self) -> ConnectorDef {
        ConnectorDef {
            id: "dmdb_sink".into(),
            kind: self.kind().into(),
            scope: ConnectorScope::Sink,
            allow_override: dmdb_sink_allow_override(),
            default_params: dmdb_sink_defaults(),
            origin: Some("wp-connectors:dmdb_sink".into()),
        }
    }
}

/// 声明允许被上游覆盖的 Source 参数白名单。
fn dmdb_source_allow_override() -> Vec<String> {
    vec![
        "endpoint",
        "dsn",
        "connection_string",
        "driver",
        "username",
        "password",
        "schema",
        "table",
        "batch",
        "cursor_column",
        "cursor_type",
        "start_from",
        "start_from_format",
        "poll_interval_ms",
        "error_backoff_ms",
        "connect_timeout_secs",
        "query_timeout_secs",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

/// 声明允许被上游覆盖的 Sink 参数白名单。
fn dmdb_sink_allow_override() -> Vec<String> {
    vec![
        "endpoint",
        "dsn",
        "connection_string",
        "driver",
        "username",
        "password",
        "schema",
        "table",
        "columns",
        "batch_size",
        "connect_timeout_secs",
        "query_timeout_secs",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

fn dmdb_source_defaults() -> ParamMap {
    let mut params = ParamMap::new();
    params.insert("batch".into(), json!(DEFAULT_SOURCE_BATCH));
    params.insert("cursor_type".into(), json!("int"));
    params.insert(
        "poll_interval_ms".into(),
        json!(DEFAULT_SOURCE_POLL_INTERVAL_MS),
    );
    params.insert(
        "error_backoff_ms".into(),
        json!(DEFAULT_SOURCE_ERROR_BACKOFF_MS),
    );
    params.insert(
        "connect_timeout_secs".into(),
        json!(DEFAULT_CONNECT_TIMEOUT_SECS),
    );
    params
}

fn dmdb_sink_defaults() -> ParamMap {
    let mut params = ParamMap::new();
    params.insert("batch_size".into(), json!(DEFAULT_SINK_BATCH_SIZE));
    params.insert(
        "connect_timeout_secs".into(),
        json!(DEFAULT_CONNECT_TIMEOUT_SECS),
    );
    params
}

// ===== Source / Sink 配置构建 =====

/// 将 `SourceSpec` 解析为强类型配置，并在构建前完成基础校验。
fn build_dmdb_source_conf(spec: &SourceSpec) -> SourceResult<DmdbSourceConf> {
    let conf = build_dmdb_source_conf_from_params(&spec.params).map_err(SourceReason::other)?;
    validate_dmdb_source_conf(&conf)?;
    Ok(conf)
}

fn build_dmdb_sink_conf(spec: &SinkSpec) -> SinkResult<DmdbSinkConf> {
    build_dmdb_sink_conf_from_params(&spec.params).map_err(SinkReason::sink)
}

fn build_dmdb_source_conf_from_params(params: &ParamMap) -> ParseResult<DmdbSourceConf> {
    Ok(DmdbSourceConf {
        conn: build_dmdb_conn_conf_from_params(params)?,
        table: optional_string(params, "table")?,
        cursor_column: optional_string(params, "cursor_column")?,
        cursor_type: optional_string(params, "cursor_type")?,
        start_from: optional_string(params, "start_from")?,
        start_from_format: optional_string(params, "start_from_format")?,
        batch: optional_usize(params, "batch")?,
        poll_interval_ms: optional_u64(params, "poll_interval_ms")?,
        error_backoff_ms: optional_u64(params, "error_backoff_ms")?,
    })
}

fn build_dmdb_sink_conf_from_params(params: &ParamMap) -> ParseResult<DmdbSinkConf> {
    Ok(DmdbSinkConf {
        conn: build_dmdb_conn_conf_from_params(params)?,
        table: optional_string(params, "table")?,
        batch_size: optional_usize(params, "batch_size")?,
    })
}

/// 先解析连接模式，再回填为统一的 `DmdbConnConf`，便于运行时代码复用。
fn build_dmdb_conn_conf_from_params(params: &ParamMap) -> ParseResult<DmdbConnConf> {
    let connection_mode = parse_connection_mode(params)?;

    let (connection_string, endpoint, dsn, driver, username, password) = match connection_mode {
        DmdbConnectionMode::ConnectionString(connection_string) => (
            Some(connection_string),
            String::new(),
            None,
            String::new(),
            String::new(),
            String::new(),
        ),
        DmdbConnectionMode::Endpoint {
            endpoint,
            driver,
            username,
            password,
        } => (None, endpoint, None, driver, username, password),
        DmdbConnectionMode::Dsn {
            dsn,
            username,
            password,
        } => (
            None,
            String::new(),
            Some(dsn),
            String::new(),
            username,
            password,
        ),
    };

    Ok(DmdbConnConf {
        endpoint,
        dsn,
        connection_string,
        driver,
        username,
        password,
        schema: optional_string(params, "schema")?,
        connect_timeout_secs: optional_u64(params, "connect_timeout_secs")?,
        query_timeout_secs: optional_usize(params, "query_timeout_secs")?,
    })
}

// ===== Source / Sink 校验 =====

/// Source 校验除了必填字段外，还会确认游标类型与 `start_from` 组合是否合法。
fn validate_dmdb_source_conf(conf: &DmdbSourceConf) -> SourceResult<()> {
    validate_connection_mode_from_conf(&conf.conn)
        .map_err(|err| SourceReason::other(err.to_string()))?;

    require_present_field(
        conf.table.as_deref(),
        "dmdb.table must not be empty",
        SourceReason::other,
    )?;
    require_present_field(
        conf.cursor_column.as_deref(),
        "dmdb.cursor_column must not be empty",
        SourceReason::other,
    )?;

    let cursor_type = conf
        .cursor_type
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("int");

    validate_positive_usize(
        conf.batch,
        DEFAULT_SOURCE_BATCH,
        "dmdb.batch must be > 0",
        SourceReason::other,
    )?;
    validate_min_u64(
        conf.poll_interval_ms,
        DEFAULT_SOURCE_POLL_INTERVAL_MS,
        MIN_POLL_INTERVAL_MS,
        "dmdb.poll_interval_ms must be >= 100",
        SourceReason::other,
    )?;
    validate_min_u64(
        conf.error_backoff_ms,
        DEFAULT_SOURCE_ERROR_BACKOFF_MS,
        MIN_ERROR_BACKOFF_MS,
        "dmdb.error_backoff_ms must be >= 200",
        SourceReason::other,
    )?;
    validate_optional_positive_u64(
        conf.conn.connect_timeout_secs,
        "dmdb.connect_timeout_secs must be > 0",
        SourceReason::other,
    )?;
    validate_optional_positive_usize(
        conf.conn.query_timeout_secs,
        "dmdb.query_timeout_secs must be > 0",
        SourceReason::other,
    )?;

    validate_source_cursor_type_and_start_from(
        cursor_type,
        conf.start_from.as_deref(),
        conf.start_from_format.as_deref(),
    )
    .map_err(|err| SourceReason::other(err.to_string()))?;

    Ok(())
}

fn validate_dmdb_sink_spec(spec: &SinkSpec) -> SinkResult<()> {
    let conf = build_dmdb_sink_conf(spec)?;
    validate_connection_mode_from_conf(&conf.conn)
        .map_err(|err| SinkReason::sink(err.to_string()))?;

    require_present_field(
        conf.table.as_deref(),
        "dmdb.table must not be empty",
        SinkReason::sink,
    )?;

    let columns = sink_columns_from_spec(spec)?;
    if columns.is_empty() {
        return Err(SinkReason::sink("dmdb.columns must not be empty"));
    }

    validate_optional_positive_usize(
        conf.batch_size,
        "dmdb.batch_size must be > 0",
        SinkReason::sink,
    )?;
    validate_optional_positive_u64(
        conf.conn.connect_timeout_secs,
        "dmdb.connect_timeout_secs must be > 0",
        SinkReason::sink,
    )?;
    validate_optional_positive_usize(
        conf.conn.query_timeout_secs,
        "dmdb.query_timeout_secs must be > 0",
        SinkReason::sink,
    )?;

    Ok(())
}

// ===== 连接参数解析 =====

/// 按 `connection_string` > `endpoint` > `dsn` 的优先级解析连接方式。
fn parse_connection_mode(params: &ParamMap) -> ParseResult<DmdbConnectionMode> {
    let connection_string = optional_string(params, "connection_string")?;
    let dsn = optional_string(params, "dsn")?;
    let endpoint = optional_string(params, "endpoint")?;

    if let Some(connection_string) = connection_string {
        return Ok(DmdbConnectionMode::ConnectionString(connection_string));
    }

    if let Some(endpoint) = endpoint {
        let driver = require_non_empty_string(
            params,
            "driver",
            "dmdb.driver must not be empty when using endpoint connection",
        )?;
        let username = require_non_empty_string(
            params,
            "username",
            "dmdb.username must not be empty when using endpoint or dsn connection",
        )?;
        let password = require_non_empty_string(
            params,
            "password",
            "dmdb.password must not be empty when using endpoint or dsn connection",
        )?;

        return Ok(DmdbConnectionMode::Endpoint {
            endpoint,
            driver,
            username,
            password,
        });
    }

    if let Some(dsn) = dsn {
        let username = require_non_empty_string(
            params,
            "username",
            "dmdb.username must not be empty when using endpoint or dsn connection",
        )?;
        let password = require_non_empty_string(
            params,
            "password",
            "dmdb.password must not be empty when using endpoint or dsn connection",
        )?;

        return Ok(DmdbConnectionMode::Dsn {
            dsn,
            username,
            password,
        });
    }

    Err("dmdb.connection_string, dmdb.endpoint or dmdb.dsn must provide at least one".into())
}

/// 对已解析配置做二次校验，确保运行时能无歧义地选择连接方式。
fn validate_connection_mode_from_conf(conf: &DmdbConnConf) -> DmdbResult<()> {
    if has_text(conf.connection_string.as_deref()) {
        return Ok(());
    }

    if has_text(Some(conf.endpoint.as_str())) {
        if conf.driver.trim().is_empty() {
            return Err(dmdb_err(
                DmdbReason::Config,
                "dmdb.driver must not be empty when using endpoint connection",
            ));
        }
        if conf.username.trim().is_empty() {
            return Err(dmdb_err(
                DmdbReason::Config,
                "dmdb.username must not be empty when using endpoint or dsn connection",
            ));
        }
        if conf.password.is_empty() {
            return Err(dmdb_err(
                DmdbReason::Config,
                "dmdb.password must not be empty when using endpoint or dsn connection",
            ));
        }
        return Ok(());
    }

    if has_text(conf.dsn.as_deref()) {
        if conf.username.trim().is_empty() {
            return Err(dmdb_err(
                DmdbReason::Config,
                "dmdb.username must not be empty when using endpoint or dsn connection",
            ));
        }
        if conf.password.is_empty() {
            return Err(dmdb_err(
                DmdbReason::Config,
                "dmdb.password must not be empty when using endpoint or dsn connection",
            ));
        }
        return Ok(());
    }

    Err(dmdb_err(
        DmdbReason::Config,
        "dmdb.connection_string, dmdb.endpoint or dmdb.dsn must provide at least one",
    ))
}

// ===== 参数读取与通用校验 =====

/// Sink 的列顺序来自上游配置，这里只做结构化解析，不尝试反查数据库元数据。
fn sink_columns_from_spec(spec: &SinkSpec) -> SinkResult<Vec<String>> {
    parse_columns(&spec.params).map_err(SinkReason::sink)
}

fn parse_columns(params: &ParamMap) -> ParseResult<Vec<String>> {
    match params.get("columns") {
        None => Ok(Vec::new()),
        Some(Value::Array(arr)) => arr
            .iter()
            .map(|item| {
                let value = item
                    .as_str()
                    .ok_or_else(|| "dmdb.columns entries must be string".to_string())?;
                let trimmed = value.trim();
                if trimmed.is_empty() {
                    return Err("dmdb.columns entries must not be empty".to_string());
                }
                Ok(trimmed.to_string())
            })
            .collect(),
        Some(_) => Err("dmdb.columns must be an array".into()),
    }
}

/// 统一将空字符串视为“未配置”，减少调用端重复 trim/判空。
fn optional_string(params: &ParamMap, key: &str) -> ParseResult<Option<String>> {
    match params.get(key) {
        None => Ok(None),
        Some(Value::String(value)) => {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                Ok(None)
            } else {
                Ok(Some(trimmed.to_string()))
            }
        }
        Some(_) => Err(format!("dmdb.{key} must be a string")),
    }
}

fn require_non_empty_string(params: &ParamMap, key: &str, message: &str) -> ParseResult<String> {
    optional_string(params, key)?.ok_or_else(|| message.to_string())
}

fn optional_usize(params: &ParamMap, key: &str) -> ParseResult<Option<usize>> {
    match params.get(key) {
        None => Ok(None),
        Some(Value::Number(number)) => number
            .as_u64()
            .map(|value| Some(value as usize))
            .ok_or_else(|| format!("dmdb.{key} must be a non-negative integer")),
        Some(_) => Err(format!("dmdb.{key} must be an integer")),
    }
}

fn optional_u64(params: &ParamMap, key: &str) -> ParseResult<Option<u64>> {
    match params.get(key) {
        None => Ok(None),
        Some(Value::Number(number)) => number
            .as_u64()
            .map(Some)
            .ok_or_else(|| format!("dmdb.{key} must be a non-negative integer")),
        Some(_) => Err(format!("dmdb.{key} must be an integer")),
    }
}

fn has_text(value: Option<&str>) -> bool {
    value.map(str::trim).is_some_and(|value| !value.is_empty())
}

/// 复用“字段必须存在且非空”的校验逻辑，兼容 Source/Sink 两种错误类型。
fn require_present_field<E, F>(value: Option<&str>, message: &str, reason: F) -> Result<(), E>
where
    F: FnOnce(String) -> E + Copy,
{
    if has_text(value) {
        Ok(())
    } else {
        Err(reason(message.to_string()))
    }
}

fn validate_positive_usize<E, F>(
    value: Option<usize>,
    default_value: usize,
    message: &str,
    reason: F,
) -> Result<(), E>
where
    F: FnOnce(String) -> E + Copy,
{
    if value.unwrap_or(default_value) == 0 {
        Err(reason(message.to_string()))
    } else {
        Ok(())
    }
}

fn validate_min_u64<E, F>(
    value: Option<u64>,
    default_value: u64,
    min_value: u64,
    message: &str,
    reason: F,
) -> Result<(), E>
where
    F: FnOnce(String) -> E + Copy,
{
    if value.unwrap_or(default_value) < min_value {
        Err(reason(message.to_string()))
    } else {
        Ok(())
    }
}

fn validate_optional_positive_usize<E, F>(
    value: Option<usize>,
    message: &str,
    reason: F,
) -> Result<(), E>
where
    F: FnOnce(String) -> E + Copy,
{
    if value.is_some_and(|value| value == 0) {
        Err(reason(message.to_string()))
    } else {
        Ok(())
    }
}

fn validate_optional_positive_u64<E, F>(
    value: Option<u64>,
    message: &str,
    reason: F,
) -> Result<(), E>
where
    F: FnOnce(String) -> E + Copy,
{
    if value.is_some_and(|value| value == 0) {
        Err(reason(message.to_string()))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::BTreeMap;
    use wp_connector_api::{SinkDefProvider, SinkFactory};

    fn build_source_spec(params: BTreeMap<String, serde_json::Value>) -> SourceSpec {
        SourceSpec {
            name: "dmdb_source".into(),
            kind: "dmdb".into(),
            connector_id: "connector".into(),
            params,
            tags: vec![],
        }
    }

    fn build_sink_spec(params: BTreeMap<String, serde_json::Value>) -> SinkSpec {
        SinkSpec {
            group: "default".into(),
            name: "dmdb_sink".into(),
            kind: "dmdb".into(),
            connector_id: "connector".into(),
            params,
            filter: None,
        }
    }

    #[test]
    fn validate_source_endpoint_connection_spec() {
        let factory = DmdbSourceFactory;
        let spec = build_source_spec(BTreeMap::from([
            ("endpoint".into(), json!("127.0.0.1:5236")),
            ("driver".into(), json!("DM8 ODBC DRIVER")),
            ("username".into(), json!("SYSDBA")),
            ("password".into(), json!("Dameng123")),
            ("table".into(), json!("EVENTS")),
            ("cursor_column".into(), json!("ID")),
            ("cursor_type".into(), json!("int")),
        ]));

        factory.validate_spec(&spec).expect("validate source spec");
    }

    #[test]
    fn validate_source_rejects_missing_cursor_column() {
        let factory = DmdbSourceFactory;
        let spec = build_source_spec(BTreeMap::from([
            ("endpoint".into(), json!("127.0.0.1:5236")),
            ("driver".into(), json!("DM8 ODBC DRIVER")),
            ("username".into(), json!("SYSDBA")),
            ("password".into(), json!("Dameng123")),
            ("table".into(), json!("EVENTS")),
        ]));

        let err = factory
            .validate_spec(&spec)
            .expect_err("missing cursor column should fail");
        assert!(
            err.to_string()
                .contains("dmdb.cursor_column must not be empty")
        );
    }

    #[test]
    fn validate_source_rejects_zero_batch() {
        let factory = DmdbSourceFactory;
        let spec = build_source_spec(BTreeMap::from([
            ("endpoint".into(), json!("127.0.0.1:5236")),
            ("driver".into(), json!("DM8 ODBC DRIVER")),
            ("username".into(), json!("SYSDBA")),
            ("password".into(), json!("Dameng123")),
            ("table".into(), json!("EVENTS")),
            ("cursor_column".into(), json!("ID")),
            ("cursor_type".into(), json!("time")),
            ("batch".into(), json!(0)),
        ]));

        let err = factory
            .validate_spec(&spec)
            .expect_err("zero batch should fail");
        assert!(err.to_string().contains("dmdb.batch must be > 0"));
    }

    #[test]
    fn endpoint_takes_priority_over_dsn() {
        let params = BTreeMap::from([
            ("dsn".into(), json!("DM8_LOCAL")),
            ("endpoint".into(), json!("127.0.0.1:5236")),
            ("driver".into(), json!("DM8 ODBC DRIVER")),
            ("username".into(), json!("SYSDBA")),
            ("password".into(), json!("Dameng123")),
            ("table".into(), json!("EVENTS")),
            ("columns".into(), json!(["id"])),
        ]);

        let mode = parse_connection_mode(&params).expect("endpoint should be selected before dsn");
        assert!(matches!(mode, DmdbConnectionMode::Endpoint { .. }));

        let conf = build_dmdb_conn_conf_from_params(&params).expect("build endpoint-priority conf");
        assert_eq!(conf.endpoint, "127.0.0.1:5236");
        assert_eq!(conf.dsn, None);
        assert_eq!(conf.driver, "DM8 ODBC DRIVER");
    }

    #[test]
    fn validate_connection_string_mode_without_split_credentials() {
        let factory = DmdbSinkFactory;
        let spec = build_sink_spec(BTreeMap::from([
            (
                "connection_string".into(),
                json!(
                    "Driver={DM8 ODBC DRIVER};SERVER=127.0.0.1;TCP_PORT=5236;UID=SYSDBA;PWD=Dameng123;"
                ),
            ),
            ("table".into(), json!("EVENTS")),
            ("columns".into(), json!(["id", "payload"])),
        ]));

        factory
            .validate_spec(&spec)
            .expect("connection string mode should not require split credentials");
    }

    #[test]
    fn reject_empty_columns() {
        let factory = DmdbSinkFactory;
        let spec = build_sink_spec(BTreeMap::from([
            ("endpoint".into(), json!("127.0.0.1:5236")),
            ("driver".into(), json!("DM8 ODBC DRIVER")),
            ("username".into(), json!("SYSDBA")),
            ("password".into(), json!("Dameng123")),
            ("table".into(), json!("EVENTS")),
            ("columns".into(), json!([])),
        ]));

        let err = factory
            .validate_spec(&spec)
            .expect_err("empty columns should fail");
        assert!(err.to_string().contains("dmdb.columns must not be empty"));
    }

    #[test]
    fn reject_missing_driver_for_endpoint_mode() {
        let factory = DmdbSinkFactory;
        let spec = build_sink_spec(BTreeMap::from([
            ("endpoint".into(), json!("127.0.0.1:5236")),
            ("username".into(), json!("SYSDBA")),
            ("password".into(), json!("Dameng123")),
            ("table".into(), json!("EVENTS")),
            ("columns".into(), json!(["id"])),
        ]));

        let err = factory
            .validate_spec(&spec)
            .expect_err("endpoint mode without driver should fail");
        assert!(
            err.to_string()
                .contains("dmdb.driver must not be empty when using endpoint connection")
        );
    }

    #[test]
    fn dmdb_sink_def_does_not_expose_misleading_connection_defaults() {
        let def = DmdbSinkFactory.sink_def();
        assert!(!def.default_params.contains_key("endpoint"));
        assert!(!def.default_params.contains_key("driver"));
        assert!(!def.default_params.contains_key("username"));
        assert!(!def.default_params.contains_key("table"));
        assert!(def.default_params.contains_key("batch_size"));
        assert!(def.default_params.contains_key("connect_timeout_secs"));
    }

    #[test]
    fn dmdb_source_def_exposes_cursor_defaults() {
        let def = DmdbSourceFactory.source_def();
        assert!(def.default_params.contains_key("batch"));
        assert!(def.default_params.contains_key("cursor_type"));
        assert!(def.default_params.contains_key("poll_interval_ms"));
        assert!(def.default_params.contains_key("error_backoff_ms"));
    }
}
