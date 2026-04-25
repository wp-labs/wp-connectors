use super::config::DmdbConf;
use super::sink::DmdbSink;
use async_trait::async_trait;
use serde_json::{Value, json};
use wp_connector_api::{
    ConnectorDef, ConnectorScope, ParamMap, SinkBuildCtx, SinkDefProvider, SinkError, SinkFactory,
    SinkHandle, SinkReason, SinkResult, SinkSpec,
};

pub struct DmdbSinkFactory;

#[async_trait]
impl SinkFactory for DmdbSinkFactory {
    fn kind(&self) -> &'static str {
        "dmdb"
    }

    fn validate_spec(&self, spec: &SinkSpec) -> SinkResult<()> {
        validate_dmdb_spec(spec)
    }

    async fn build(&self, spec: &SinkSpec, _ctx: &SinkBuildCtx) -> SinkResult<SinkHandle> {
        let conf = build_dmdb_conf(spec)?;
        let columns = parse_columns(spec)?;
        let table = conf
            .table
            .clone()
            .ok_or_else(|| SinkError::from(SinkReason::sink("dmdb.table must be provided")))?;
        let batch_size = conf.normalized_batch_size();
        let query_timeout_sec = conf.query_timeout_secs;
        let schema = conf.schema.clone();
        let connection = DmdbSink::connect_shared(&conf).await.map_err(|err| {
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

impl SinkDefProvider for DmdbSinkFactory {
    fn sink_def(&self) -> ConnectorDef {
        ConnectorDef {
            id: "dmdb_sink".into(),
            kind: self.kind().into(),
            scope: ConnectorScope::Sink,
            allow_override: vec![
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
            .collect(),
            default_params: dmdb_sink_defaults(),
            origin: Some("wp-connectors:dmdb_sink".into()),
        }
    }
}

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

fn validate_dmdb_spec(spec: &SinkSpec) -> SinkResult<()> {
    parse_connection_mode(spec)?;

    required_string(spec, "table", "dmdb.table must not be empty")?;

    let columns = parse_columns(spec)?;
    if columns.is_empty() {
        return Err(SinkReason::sink("dmdb.columns must not be empty").into());
    }

    check_positive_usize(spec, "batch_size", "dmdb.batch_size must be > 0")?;
    check_positive_u64(
        spec,
        "connect_timeout_secs",
        "dmdb.connect_timeout_secs must be > 0",
    )?;
    check_positive_usize(
        spec,
        "query_timeout_secs",
        "dmdb.query_timeout_secs must be > 0",
    )?;

    Ok(())
}

fn build_dmdb_conf(spec: &SinkSpec) -> SinkResult<DmdbConf> {
    let connection_mode = parse_connection_mode(spec)?;

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

    Ok(DmdbConf {
        endpoint,
        dsn,
        connection_string,
        driver,
        username,
        password,
        schema: optional_string(spec, "schema")?,
        table: Some(required_string(
            spec,
            "table",
            "dmdb.table must not be empty",
        )?),
        batch_size: optional_usize(spec, "batch_size")?,
        connect_timeout_secs: optional_u64(spec, "connect_timeout_secs")?,
        query_timeout_secs: optional_usize(spec, "query_timeout_secs")?,
    })
}

fn parse_connection_mode(spec: &SinkSpec) -> SinkResult<DmdbConnectionMode> {
    let connection_string = optional_string(spec, "connection_string")?;
    let dsn = optional_string(spec, "dsn")?;
    let endpoint = optional_string(spec, "endpoint")?;

    if let Some(connection_string) = connection_string {
        return Ok(DmdbConnectionMode::ConnectionString(connection_string));
    }

    if let Some(endpoint) = endpoint {
        let driver = required_string(
            spec,
            "driver",
            "dmdb.driver must not be empty when using endpoint connection",
        )?;
        let username = required_string(
            spec,
            "username",
            "dmdb.username must not be empty when using endpoint or dsn connection",
        )?;
        let password = required_string(
            spec,
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
        let username = required_string(
            spec,
            "username",
            "dmdb.username must not be empty when using endpoint or dsn connection",
        )?;
        let password = required_string(
            spec,
            "password",
            "dmdb.password must not be empty when using endpoint or dsn connection",
        )?;

        return Ok(DmdbConnectionMode::Dsn {
            dsn,
            username,
            password,
        });
    }

    Err(SinkReason::sink(
        "dmdb.connection_string, dmdb.endpoint or dmdb.dsn must provide at least one",
    )
    .into())
}

fn parse_columns(spec: &SinkSpec) -> SinkResult<Vec<String>> {
    match spec.params.get("columns") {
        None => Ok(Vec::new()),
        Some(Value::Array(arr)) => {
            let out = arr
                .iter()
                .map(|item| {
                    if let Some(value) = item.as_str() {
                        let trimmed = value.trim();
                        if trimmed.is_empty() {
                            return Err(
                                SinkReason::sink("dmdb.columns entries must not be empty").into()
                            );
                        }
                        Ok(trimmed.to_string())
                    } else {
                        Err(SinkReason::sink("dmdb.columns entries must be string").into())
                    }
                })
                .collect::<SinkResult<Vec<String>>>()?;
            Ok(out)
        }
        Some(_) => Err(SinkReason::sink("dmdb.columns must be an array").into()),
    }
}

fn optional_string(spec: &SinkSpec, key: &str) -> SinkResult<Option<String>> {
    match spec.params.get(key) {
        None => Ok(None),
        Some(Value::String(value)) => {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                Ok(None)
            } else {
                Ok(Some(trimmed.to_string()))
            }
        }
        Some(_) => Err(SinkReason::sink(format!("dmdb.{key} must be a string")).into()),
    }
}

fn required_string(spec: &SinkSpec, key: &str, message: &str) -> SinkResult<String> {
    optional_string(spec, key)?.ok_or_else(|| SinkReason::sink(message).into())
}

fn optional_usize(spec: &SinkSpec, key: &str) -> SinkResult<Option<usize>> {
    match spec.params.get(key) {
        None => Ok(None),
        Some(Value::Number(number)) => number
            .as_u64()
            .map(|value| Some(value as usize))
            .ok_or_else(|| {
                SinkReason::sink(format!("dmdb.{key} must be a non-negative integer")).into()
            }),
        Some(_) => Err(SinkReason::sink(format!("dmdb.{key} must be an integer")).into()),
    }
}

fn optional_u64(spec: &SinkSpec, key: &str) -> SinkResult<Option<u64>> {
    match spec.params.get(key) {
        None => Ok(None),
        Some(Value::Number(number)) => number.as_u64().map(Some).ok_or_else(|| {
            SinkReason::sink(format!("dmdb.{key} must be a non-negative integer")).into()
        }),
        Some(_) => Err(SinkReason::sink(format!("dmdb.{key} must be an integer")).into()),
    }
}

fn check_positive_usize(spec: &SinkSpec, key: &str, message: &str) -> SinkResult<()> {
    if let Some(value) = optional_usize(spec, key)?
        && value == 0
    {
        return Err(SinkReason::sink(message).into());
    }
    Ok(())
}

fn check_positive_u64(spec: &SinkSpec, key: &str, message: &str) -> SinkResult<()> {
    if let Some(value) = optional_u64(spec, key)?
        && value == 0
    {
        return Err(SinkReason::sink(message).into());
    }
    Ok(())
}

fn dmdb_sink_defaults() -> ParamMap {
    let mut params = ParamMap::new();
    params.insert("batch_size".into(), json!(1024));
    params.insert("connect_timeout_secs".into(), json!(8));
    params
}

#[cfg(test)]
mod tests {
    use super::{DmdbConnectionMode, DmdbSinkFactory, build_dmdb_conf, parse_connection_mode};
    use serde_json::json;
    use std::collections::BTreeMap;
    use wp_connector_api::{SinkDefProvider, SinkFactory, SinkSpec};

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
    fn validate_endpoint_connection_spec() {
        let factory = DmdbSinkFactory;
        let spec = build_sink_spec(BTreeMap::from([
            ("endpoint".into(), json!("127.0.0.1:5236")),
            ("driver".into(), json!("DM8 ODBC DRIVER")),
            ("username".into(), json!("SYSDBA")),
            ("password".into(), json!("Dameng123")),
            ("table".into(), json!("EVENTS")),
            ("columns".into(), json!(["id", "payload"])),
        ]));

        factory
            .validate_spec(&spec)
            .expect("validate endpoint mode");
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
    fn endpoint_takes_priority_over_dsn() {
        let spec = build_sink_spec(BTreeMap::from([
            ("dsn".into(), json!("DM8_LOCAL")),
            ("endpoint".into(), json!("127.0.0.1:5236")),
            ("driver".into(), json!("DM8 ODBC DRIVER")),
            ("username".into(), json!("SYSDBA")),
            ("password".into(), json!("Dameng123")),
            ("table".into(), json!("EVENTS")),
            ("columns".into(), json!(["id"])),
        ]));

        let mode = parse_connection_mode(&spec).expect("endpoint should be selected before dsn");
        assert!(matches!(mode, DmdbConnectionMode::Endpoint { .. }));

        let conf = build_dmdb_conf(&spec).expect("build endpoint-priority config");
        assert_eq!(conf.endpoint, "127.0.0.1:5236");
        assert_eq!(conf.dsn, None);
        assert_eq!(conf.driver, "DM8 ODBC DRIVER");
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
    fn reject_missing_password_for_dsn_mode() {
        let factory = DmdbSinkFactory;
        let spec = build_sink_spec(BTreeMap::from([
            ("dsn".into(), json!("DM8_LOCAL")),
            ("username".into(), json!("SYSDBA")),
            ("table".into(), json!("EVENTS")),
            ("columns".into(), json!(["id"])),
        ]));

        let err = factory
            .validate_spec(&spec)
            .expect_err("dsn mode without password should fail");
        assert!(
            err.to_string()
                .contains("dmdb.password must not be empty when using endpoint or dsn connection")
        );
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
    fn reject_missing_endpoint_when_only_split_fields_expected() {
        let factory = DmdbSinkFactory;
        let spec = build_sink_spec(BTreeMap::from([
            ("driver".into(), json!("DM8 ODBC DRIVER")),
            ("username".into(), json!("SYSDBA")),
            ("password".into(), json!("Dameng123")),
            ("table".into(), json!("EVENTS")),
            ("columns".into(), json!(["id"])),
        ]));

        let err = factory
            .validate_spec(&spec)
            .expect_err("missing connection target should fail");
        assert!(err.to_string().contains(
            "dmdb.connection_string, dmdb.endpoint or dmdb.dsn must provide at least one"
        ));
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
    fn reject_zero_batch_size() {
        let factory = DmdbSinkFactory;
        let spec = build_sink_spec(BTreeMap::from([
            ("endpoint".into(), json!("127.0.0.1:5236")),
            ("driver".into(), json!("DM8 ODBC DRIVER")),
            ("username".into(), json!("SYSDBA")),
            ("password".into(), json!("Dameng123")),
            ("table".into(), json!("EVENTS")),
            ("columns".into(), json!(["id"])),
            ("batch_size".into(), json!(0)),
        ]));

        let err = factory
            .validate_spec(&spec)
            .expect_err("zero batch should fail");
        assert!(err.to_string().contains("dmdb.batch_size must be > 0"));
    }
}
