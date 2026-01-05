use crate::doris::{DorisSink, config::DorisSinkConfig};
use async_trait::async_trait;
use serde_json::{Value, json};
use wp_connector_api::{
    ConnectorDef, ConnectorScope, ParamMap, SinkBuildCtx, SinkDefProvider, SinkError, SinkFactory,
    SinkHandle, SinkReason, SinkResult, SinkSpec,
};

pub struct DorisSinkFactory;
///wp_connector_api::runtime::sink::SinkFactory
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
        if let Some(pool) = get_u64(spec, "pool").or_else(|| get_u64(spec, "pool_size"))
            && pool == 0
        {
            return Err(SinkReason::sink("doris.pool must be > 0").into());
        }
        if let Some(batch) = get_u64(spec, "batch").or_else(|| get_u64(spec, "batch_size"))
            && batch == 0
        {
            return Err(SinkReason::sink("doris.batch must be > 0").into());
        }
        Ok(())
    }

    async fn build(&self, spec: &SinkSpec, _ctx: &SinkBuildCtx) -> SinkResult<SinkHandle> {
        let endpoint = required_param(spec, "endpoint")?;
        let user = required_param(spec, "user")?;
        let password = optional_string(spec, "password").unwrap_or_default();
        let table = required_param(spec, "table")?;
        let database = required_param(spec, "database")?;
        let create_stmt = optional_string(spec, "create_table");
        let pool_size = parse_u32_param(spec, &["pool", "pool_size"])?;
        let batch_size = parse_usize_param(spec, &["batch", "batch_size"])?;
        let cfg = DorisSinkConfig::new(
            endpoint,
            database,
            user,
            password,
            table,
            create_stmt,
            pool_size,
            batch_size,
        );
        let sink = DorisSink::new(cfg).await.map_err(|err| {
            SinkError::from(SinkReason::sink(format!("init doris sink failed: {err}")))
        })?;
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
                "user",
                "password",
                "table",
                "create_table",
                "pool",
                "pool_size",
                "batch",
                "batch_size",
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
///
/// # 参数
/// * `spec` - Sink 定义。
/// * `key` - 需要检查的键。
///
/// # 返回
/// * `SinkResult<()>` - 成功表示字段有效。
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
///
/// # 参数
/// * `spec` - Sink 定义。
/// * `key` - 参数名称。
///
/// # 返回
/// * `SinkResult<String>` - 对应参数值。
fn required_param(spec: &SinkSpec, key: &str) -> SinkResult<String> {
    spec.params
        .get(key)
        .and_then(Value::as_str)
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| SinkReason::sink(format!("doris.{key} must not be empty")).into())
}

/// 读取可选字符串参数。
///
/// # 参数
/// * `spec` - Sink 定义。
/// * `key` - 参数名称。
///
/// # 返回
/// * `Option<String>` - 存在且非空时返回值。
fn optional_string(spec: &SinkSpec, key: &str) -> Option<String> {
    spec.params
        .get(key)
        .and_then(Value::as_str)
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// 将参数解析为 `u64`。
///
/// # 参数
/// * `spec` - Sink 定义。
/// * `key` - 参数名称。
///
/// # 返回
/// * `Option<u64>` - 字段可解析时返回。
fn get_u64(spec: &SinkSpec, key: &str) -> Option<u64> {
    spec.params.get(key).and_then(Value::as_u64)
}

/// 从多个键中挑选第一个存在的无符号整型（`u32`）。
///
/// # 参数
/// * `spec` - Sink 定义。
/// * `keys` - 优先级有序的键列表。
///
/// # 返回
/// * `SinkResult<Option<u32>>` - 成功则返回转换后的值或 `None`。
fn parse_u32_param(spec: &SinkSpec, keys: &[&str]) -> SinkResult<Option<u32>> {
    for key in keys {
        if let Some(value) = get_u64(spec, key) {
            if value == 0 {
                return Err(SinkReason::sink(format!("doris.{key} must be > 0")).into());
            }
            let converted = u32::try_from(value)
                .map_err(|_| SinkReason::sink(format!("doris.{key} exceeds u32 range")))?;
            return Ok(Some(converted));
        }
    }
    Ok(None)
}

/// 与 [`parse_u32_param`] 类似，但返回 `usize`。
///
/// # 参数
/// * `spec` - Sink 定义。
/// * `keys` - 键列表。
///
/// # 返回
/// * `SinkResult<Option<usize>>` - 解析结果。
fn parse_usize_param(spec: &SinkSpec, keys: &[&str]) -> SinkResult<Option<usize>> {
    for key in keys {
        if let Some(value) = get_u64(spec, key) {
            if value == 0 {
                return Err(SinkReason::sink(format!("doris.{key} must be > 0")).into());
            }
            let converted = usize::try_from(value)
                .map_err(|_| SinkReason::sink(format!("doris.{key} exceeds usize range")))?;
            return Ok(Some(converted));
        }
    }
    Ok(None)
}

fn doris_defaults() -> ParamMap {
    let mut params = ParamMap::new();
    params.insert("endpoint".into(), json!("mysql://localhost:9030"));
    params.insert("database".into(), json!("wp_data"));
    params.insert("user".into(), json!("root"));
    params.insert("password".into(), json!(""));
    params.insert("table".into(), json!("wp_events"));
    params.insert("pool".into(), json!(DorisSinkConfig::default_pool_size()));
    params.insert("batch".into(), json!(DorisSinkConfig::default_batch_size()));
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
            Value::String("mysql://localhost:9030/db".into()),
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
    fn validate_accepts_minimal_spec() {
        let spec = base_spec();
        let factory = DorisSinkFactory;
        assert!(factory.validate_spec(&spec).is_ok());
    }
}
