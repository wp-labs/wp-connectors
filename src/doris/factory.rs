use crate::doris::{DorisSink, config::DorisSinkConfig};
use anyhow::Context;
use async_trait::async_trait;
use serde_json::Value;
use wp_connector_api::{SinkBuildCtx, SinkFactory, SinkHandle, SinkSpec};

pub struct DorisSinkFactory;

#[async_trait]
impl SinkFactory for DorisSinkFactory {
    fn kind(&self) -> &'static str {
        "doris"
    }

    fn validate_spec(&self, spec: &SinkSpec) -> anyhow::Result<()> {
        ensure_not_empty(spec, "endpoint")?;
        ensure_not_empty(spec, "user")?;
        ensure_not_empty(spec, "table")?;
        ensure_not_empty(spec, "database")?;
        if let Some(pool) = get_u64(spec, "pool").or_else(|| get_u64(spec, "pool_size"))
            && pool == 0
        {
            anyhow::bail!("doris.pool must be > 0");
        }
        if let Some(batch) = get_u64(spec, "batch").or_else(|| get_u64(spec, "batch_size"))
            && batch == 0
        {
            anyhow::bail!("doris.batch must be > 0");
        }
        Ok(())
    }

    async fn build(&self, spec: &SinkSpec, _ctx: &SinkBuildCtx) -> anyhow::Result<SinkHandle> {
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
        let sink = DorisSink::new(cfg).await?;
        Ok(SinkHandle::new(Box::new(sink)))
    }
}

fn ensure_not_empty(spec: &SinkSpec, key: &str) -> anyhow::Result<()> {
    let value = spec
        .params
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim()
        .to_string();
    if value.is_empty() {
        anyhow::bail!("doris.{key} must not be empty");
    }
    Ok(())
}

fn required_param(spec: &SinkSpec, key: &str) -> anyhow::Result<String> {
    spec.params
        .get(key)
        .and_then(Value::as_str)
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| anyhow::anyhow!("doris.{key} must not be empty"))
}

fn optional_string(spec: &SinkSpec, key: &str) -> Option<String> {
    spec.params
        .get(key)
        .and_then(Value::as_str)
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

fn get_u64(spec: &SinkSpec, key: &str) -> Option<u64> {
    spec.params.get(key).and_then(Value::as_u64)
}

fn parse_u32_param(spec: &SinkSpec, keys: &[&str]) -> anyhow::Result<Option<u32>> {
    for key in keys {
        if let Some(value) = get_u64(spec, key) {
            if value == 0 {
                anyhow::bail!("doris.{key} must be > 0");
            }
            let converted =
                u32::try_from(value).context(format!("doris.{key} exceeds u32 range"))?;
            return Ok(Some(converted));
        }
    }
    Ok(None)
}

fn parse_usize_param(spec: &SinkSpec, keys: &[&str]) -> anyhow::Result<Option<usize>> {
    for key in keys {
        if let Some(value) = get_u64(spec, key) {
            if value == 0 {
                anyhow::bail!("doris.{key} must be > 0");
            }
            let converted =
                usize::try_from(value).context(format!("doris.{key} exceeds usize range"))?;
            return Ok(Some(converted));
        }
    }
    Ok(None)
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
