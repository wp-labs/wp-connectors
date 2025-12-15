//! Dev-only ClickHouse adapter：从 `clickhouse://...` 连接串提取参数，注册到 wp-connector 适配层。

use std::collections::BTreeMap;
use wp_connector_api::{register_adapter, ConnectorKindAdapter, ParamMap};

use super::config::Clickhouse;

pub struct DevClickhouseAdapter;

impl ConnectorKindAdapter for DevClickhouseAdapter {
    fn kind(&self) -> &'static str {
        "clickhouse"
    }
    fn url_to_params(&self, url: &str) -> anyhow::Result<ParamMap> {
        let mut s = url;
        let cfg = Clickhouse::parse_clickhouse_connect(&mut s)?;
        // Convert to ParamMap (serde_json values)
        let mut m: ParamMap = BTreeMap::new();
        m.insert("endpoint".into(), serde_json::Value::String(cfg.get_endpoint()));
        m.insert("username".into(), serde_json::Value::String(cfg.username));
        m.insert("password".into(), serde_json::Value::String(cfg.password));
        m.insert("database".into(), serde_json::Value::String(cfg.database));
        if let Some(t) = cfg.table {
            m.insert("table".into(), serde_json::Value::String(t));
        }
        if let Some(b) = cfg.batch {
            m.insert("batch".into(), serde_json::Value::from(b as i64));
        }
        Ok(m)
    }
}

static DEV_CLICKHOUSE: DevClickhouseAdapter = DevClickhouseAdapter;

/// 注册 ClickHouse 开发期适配器（仅用于本地开发与示例）。
pub fn register_clickhouse_adapter() {
    register_adapter(&DEV_CLICKHOUSE);
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    #[test]
    fn parse_clickhouse_basic_url() {
        let url = "clickhouse://root:pass@127.0.0.1:8123/wparse";
        let a = DevClickhouseAdapter;
        let m = a.url_to_params(url).expect("parse ok");
        assert_eq!(m.get("endpoint"), Some(&Value::String("http://127.0.0.1:8123".into())));
        assert_eq!(m.get("username"), Some(&Value::String("root".into())));
        assert_eq!(m.get("password"), Some(&Value::String("pass".into())));
        assert_eq!(m.get("database"), Some(&Value::String("wparse".into())));
    }
}

