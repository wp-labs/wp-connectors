use crate::elasticsearch::{ElasticsearchSink, ElasticsearchSinkConfig};
use async_trait::async_trait;
use serde_json::{Value, json};
use wp_connector_api::{
    ConnectorDef, ConnectorScope, ParamMap, SinkBuildCtx, SinkDefProvider, SinkFactory, SinkHandle,
    SinkReason, SinkResult, SinkSpec,
};

pub struct ElasticsearchSinkFactory;

#[async_trait]
impl SinkFactory for ElasticsearchSinkFactory {
    fn kind(&self) -> &'static str {
        "elasticsearch"
    }

    fn validate_spec(&self, spec: &SinkSpec) -> SinkResult<()> {
        ensure_not_empty(spec, "host")?;
        ensure_not_empty(spec, "index")?;
        ensure_not_empty(spec, "username")?;

        // 验证 protocol（如果提供）
        if let Some(protocol) = optional_string(spec, "protocol")
            && protocol != "http"
            && protocol != "https"
        {
            return Err(
                SinkReason::sink("elasticsearch.protocol must be 'http' or 'https'").into(),
            );
        }

        // 验证 port（如果提供）
        if let Some(port) = get_u64(spec, "port")
            && (port == 0 || port > 65535)
        {
            return Err(SinkReason::sink("elasticsearch.port must be between 1 and 65535").into());
        }

        // 验证 timeout
        if let Some(timeout) = get_u64(spec, "timeout_secs")
            && timeout == 0
        {
            return Err(SinkReason::sink("elasticsearch.timeout_secs must be > 0").into());
        }

        Ok(())
    }

    async fn build(&self, spec: &SinkSpec, _ctx: &SinkBuildCtx) -> SinkResult<SinkHandle> {
        let protocol = optional_string(spec, "protocol");
        let host = required_param(spec, "host")?;
        let port = get_u64(spec, "port").map(|p| p as u16);
        let index = required_param(spec, "index")?;
        let username = required_param(spec, "username")?;
        let password = optional_string(spec, "password").unwrap_or_default();
        let timeout_secs: Option<u64> = parse_u64_param(spec, &["timeout_secs", "timeout"])?;
        let max_retries = parse_i32_param(spec, &["max_retries", "retries"])?;

        let cfg = ElasticsearchSinkConfig::new(
            protocol,
            host,
            port,
            index,
            username,
            password,
            timeout_secs,
            max_retries,
        );

        let sink = ElasticsearchSink::new(cfg).await?;

        Ok(SinkHandle::new(Box::new(sink)))
    }
}

impl SinkDefProvider for ElasticsearchSinkFactory {
    fn sink_def(&self) -> ConnectorDef {
        ConnectorDef {
            id: "elasticsearch_sink".into(),
            kind: self.kind().into(),
            scope: ConnectorScope::Sink,
            allow_override: vec![
                "protocol",
                "host",
                "port",
                "index",
                "username",
                "password",
                "timeout_secs",
                "timeout",
                "max_retries",
                "retries",
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
            default_params: elasticsearch_defaults(),
            origin: Some("wp-connectors:elasticsearch_sink".into()),
        }
    }
}

/// 保证指定参数存在且非空
fn ensure_not_empty(spec: &SinkSpec, key: &str) -> SinkResult<()> {
    let value = spec
        .params
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim()
        .to_string();
    if value.is_empty() {
        return Err(SinkReason::sink(format!("elasticsearch.{key} must not be empty")).into());
    }
    Ok(())
}

/// 读取必填参数并返回修剪后的字符串
fn required_param(spec: &SinkSpec, key: &str) -> SinkResult<String> {
    spec.params
        .get(key)
        .and_then(Value::as_str)
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| SinkReason::sink(format!("elasticsearch.{key} must not be empty")).into())
}

/// 读取可选字符串参数
fn optional_string(spec: &SinkSpec, key: &str) -> Option<String> {
    spec.params
        .get(key)
        .and_then(Value::as_str)
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// 将参数解析为 `u64`
fn get_u64(spec: &SinkSpec, key: &str) -> Option<u64> {
    spec.params.get(key).and_then(Value::as_u64)
}

/// 从多个键中挑选第一个存在的 u64
fn parse_u64_param(spec: &SinkSpec, keys: &[&str]) -> SinkResult<Option<u64>> {
    for key in keys {
        if let Some(value) = get_u64(spec, key) {
            if value == 0 {
                return Err(SinkReason::sink(format!("elasticsearch.{key} must be > 0")).into());
            }
            return Ok(Some(value));
        }
    }
    Ok(None)
}

/// 从多个键中挑选第一个存在的 i32
fn parse_i32_param(spec: &SinkSpec, keys: &[&str]) -> SinkResult<Option<i32>> {
    for &key in keys {
        if let Some(value) = spec.params.get(key).and_then(Value::as_i64) {
            let converted = i32::try_from(value)
                .map_err(|_| SinkReason::sink(format!("elasticsearch.{key} exceeds i32 range")))?;
            return Ok(Some(converted));
        }
    }
    Ok(None)
}

fn elasticsearch_defaults() -> ParamMap {
    let mut params = ParamMap::new();
    params.insert(
        "protocol".into(),
        json!(ElasticsearchSinkConfig::default_protocol()),
    );
    params.insert("host".into(), json!("localhost"));
    params.insert(
        "port".into(),
        json!(ElasticsearchSinkConfig::default_port()),
    );
    params.insert("index".into(), json!("wp_logs"));
    params.insert("username".into(), json!("elastic"));
    params.insert("password".into(), json!(""));
    params.insert(
        "timeout_secs".into(),
        json!(ElasticsearchSinkConfig::default_timeout_secs()),
    );
    params.insert(
        "max_retries".into(),
        json!(ElasticsearchSinkConfig::default_max_retries()),
    );
    params
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn base_spec() -> SinkSpec {
        let mut params = BTreeMap::new();
        params.insert("host".into(), Value::String("localhost".into()));
        params.insert("index".into(), Value::String("test_index".into()));
        params.insert("username".into(), Value::String("elastic".into()));
        params.insert("password".into(), Value::String("password".into()));
        SinkSpec {
            name: "elasticsearch_sink".into(),
            kind: "elasticsearch".into(),
            connector_id: String::new(),
            group: "test".into(),
            params,
            filter: None,
        }
    }

    #[test]
    fn validate_rejects_empty_host() {
        let mut spec = base_spec();
        spec.params.insert("host".into(), Value::String("".into()));
        let factory = ElasticsearchSinkFactory;
        assert!(factory.validate_spec(&spec).is_err());
    }

    #[test]
    fn validate_rejects_invalid_protocol() {
        let mut spec = base_spec();
        spec.params
            .insert("protocol".into(), Value::String("ftp".into()));
        let factory = ElasticsearchSinkFactory;
        assert!(factory.validate_spec(&spec).is_err());
    }

    #[test]
    fn validate_rejects_empty_index() {
        let mut spec = base_spec();
        spec.params.insert("index".into(), Value::String("".into()));
        let factory = ElasticsearchSinkFactory;
        assert!(factory.validate_spec(&spec).is_err());
    }

    #[test]
    fn validate_rejects_empty_username() {
        let mut spec = base_spec();
        spec.params
            .insert("username".into(), Value::String("".into()));
        let factory = ElasticsearchSinkFactory;
        assert!(factory.validate_spec(&spec).is_err());
    }

    #[test]
    fn validate_rejects_zero_timeout() {
        let mut spec = base_spec();
        spec.params
            .insert("timeout_secs".into(), Value::Number(0.into()));
        let factory = ElasticsearchSinkFactory;
        assert!(factory.validate_spec(&spec).is_err());
    }

    #[test]
    fn validate_accepts_minimal_spec() {
        let spec = base_spec();
        let factory = ElasticsearchSinkFactory;
        assert!(factory.validate_spec(&spec).is_ok());
    }
}
