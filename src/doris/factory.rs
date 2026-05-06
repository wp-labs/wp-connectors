use crate::doris::{DorisSink, config::DorisSinkConfig};
use async_trait::async_trait;
use serde_json::{Value, json};
use std::collections::HashMap;
use wp_connector_api::{
    ConnectorDef, ConnectorScope, ParamMap, SinkBuildCtx, SinkDefProvider, SinkFactory, SinkHandle,
    SinkReason, SinkResult, SinkSpec,
};

pub struct DorisSinkFactory;

#[async_trait]
impl SinkFactory for DorisSinkFactory {
    fn kind(&self) -> &'static str {
        "doris"
    }

    fn validate_spec(&self, spec: &SinkSpec) -> SinkResult<()> {
        ensure_not_empty(spec, "endpoint")?;
        ensure_not_empty(spec, "user")?;
        ensure_not_empty(spec, "table")?;
        ensure_not_empty(spec, "database")?;

        // 验证 endpoint 格式
        let endpoint = required_param(spec, "endpoint")?;
        if !endpoint.starts_with("http://") && !endpoint.starts_with("https://") {
            return Err(
                SinkReason::sink("doris.endpoint must start with http:// or https://").into(),
            );
        }

        // 验证 timeout
        if let Some(timeout) = get_u64(spec, "timeout_secs")
            && timeout == 0
        {
            return Err(SinkReason::sink("doris.timeout_secs must be > 0").into());
        }

        if let Some(max_retries) = get_i64(spec, "max_retries")
            && max_retries < -1
        {
            return Err(SinkReason::sink("doris.max_retries must be >= -1").into());
        }

        if let Some(retries) = get_i64(spec, "retries")
            && retries < -1
        {
            return Err(SinkReason::sink("doris.retries must be >= -1").into());
        }

        Ok(())
    }

    async fn build(&self, spec: &SinkSpec, _ctx: &SinkBuildCtx) -> SinkResult<SinkHandle> {
        let endpoint = required_param(spec, "endpoint")?;
        let database = required_param(spec, "database")?;
        let table = required_param(spec, "table")?;
        let user = required_param(spec, "user")?;
        let password = optional_string(spec, "password").unwrap_or_default();
        let timeout_secs: Option<u64> = parse_u64_param(spec, &["timeout_secs", "timeout"])?;
        let max_retries = parse_i32_param(spec, &["max_retries", "retries"])?;
        let headers = parse_headers(spec)?;

        let cfg = DorisSinkConfig::new(
            endpoint,
            database,
            table,
            user,
            password,
            timeout_secs,
            max_retries,
            headers,
        );

        let sink = DorisSink::new(cfg).await?;

        Ok(SinkHandle::new(Box::new(sink)))
    }
}

impl SinkDefProvider for DorisSinkFactory {
    fn sink_def(&self) -> ConnectorDef {
        ConnectorDef {
            id: "doris_sink".into(),
            kind: self.kind().into(),
            scope: ConnectorScope::Sink,
            allow_override: vec![
                "endpoint",
                "database",
                "table",
                "user",
                "password",
                "timeout_secs",
                "timeout",
                "max_retries",
                "retries",
                "headers",
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
            default_params: doris_defaults(),
            origin: Some("wp-connectors:doris_sink".into()),
        }
    }
}

/// 保证指定参数存在且非空。
fn ensure_not_empty(spec: &SinkSpec, key: &str) -> SinkResult<()> {
    let value = spec
        .params
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim()
        .to_string();
    if value.is_empty() {
        return Err(SinkReason::sink(format!("doris.{key} must not be empty")).into());
    }
    Ok(())
}

/// 读取必填参数并返回修剪后的字符串。
fn required_param(spec: &SinkSpec, key: &str) -> SinkResult<String> {
    spec.params
        .get(key)
        .and_then(Value::as_str)
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| SinkReason::sink(format!("doris.{key} must not be empty")).into())
}

/// 读取可选字符串参数。
fn optional_string(spec: &SinkSpec, key: &str) -> Option<String> {
    spec.params
        .get(key)
        .and_then(Value::as_str)
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// 将参数解析为 `u64`。
fn get_u64(spec: &SinkSpec, key: &str) -> Option<u64> {
    spec.params.get(key).and_then(Value::as_u64)
}

fn get_i64(spec: &SinkSpec, key: &str) -> Option<i64> {
    spec.params.get(key).and_then(Value::as_i64)
}

/// 从多个键中挑选第一个存在的 u64。
fn parse_u64_param(spec: &SinkSpec, keys: &[&str]) -> SinkResult<Option<u64>> {
    for key in keys {
        if let Some(value) = get_u64(spec, key) {
            if value == 0 {
                return Err(SinkReason::sink(format!("doris.{key} must be > 0")).into());
            }
            return Ok(Some(value));
        }
    }
    Ok(None)
}

/// 从多个键中挑选第一个存在的 i32。
fn parse_i32_param(spec: &SinkSpec, keys: &[&str]) -> SinkResult<Option<i32>> {
    for &key in keys {
        if let Some(value) = spec.params.get(key).and_then(Value::as_i64) {
            let converted = i32::try_from(value)
                .map_err(|_| SinkReason::sink(format!("doris.{key} exceeds i32 range")))?;
            return Ok(Some(converted));
        }
    }
    Ok(None)
}

/// 解析 headers 参数（可以是对象或嵌套的 headers 字段）。
fn parse_headers(spec: &SinkSpec) -> SinkResult<Option<HashMap<String, String>>> {
    if let Some(headers_value) = spec.params.get("headers")
        && let Some(obj) = headers_value.as_object()
    {
        let mut headers = HashMap::new();
        for (k, v) in obj {
            if let Some(s) = v.as_str() {
                headers.insert(k.clone(), s.to_string());
            } else {
                return Err(
                    SinkReason::sink(format!("doris.headers.{} must be a string", k)).into(),
                );
            }
        }
        return Ok(Some(headers));
    }
    Ok(None)
}

fn doris_defaults() -> ParamMap {
    let mut params = ParamMap::new();
    params.insert("endpoint".into(), json!("http://localhost:8040"));
    params.insert("database".into(), json!("wp_data"));
    params.insert("user".into(), json!("root"));
    params.insert("password".into(), json!(""));
    params.insert("table".into(), json!("wp_events"));
    params.insert(
        "timeout_secs".into(),
        json!(DorisSinkConfig::default_timeout_secs()),
    );
    params.insert(
        "max_retries".into(),
        json!(DorisSinkConfig::default_max_retries()),
    );
    params
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn base_spec() -> SinkSpec {
        let mut params = BTreeMap::new();
        params.insert(
            "endpoint".into(),
            Value::String("http://localhost:8040".into()),
        );
        params.insert("database".into(), Value::String("demo".into()));
        params.insert("user".into(), Value::String("root".into()));
        params.insert("password".into(), Value::String("pwd".into()));
        params.insert("table".into(), Value::String("events".into()));
        SinkSpec {
            name: "doris_sink".into(),
            kind: "doris".into(),
            connector_id: String::new(),
            group: "test".into(),
            params,
            filter: None,
        }
    }

    #[test]
    fn validate_rejects_empty_endpoint() {
        let mut spec = base_spec();
        spec.params
            .insert("endpoint".into(), Value::String("".into()));
        let factory = DorisSinkFactory;
        assert!(factory.validate_spec(&spec).is_err());
    }

    #[test]
    fn validate_rejects_invalid_endpoint() {
        let mut spec = base_spec();
        spec.params.insert(
            "endpoint".into(),
            Value::String("mysql://localhost:9030".into()),
        );
        let factory = DorisSinkFactory;
        assert!(factory.validate_spec(&spec).is_err());
    }

    #[test]
    fn validate_accepts_minimal_spec() {
        let spec = base_spec();
        let factory = DorisSinkFactory;
        assert!(factory.validate_spec(&spec).is_ok());
    }

    #[test]
    fn validate_rejects_invalid_negative_retry_count() {
        let mut spec = base_spec();
        spec.params.insert("max_retries".into(), Value::from(-2));
        let factory = DorisSinkFactory;
        assert!(factory.validate_spec(&spec).is_err());
    }
}
