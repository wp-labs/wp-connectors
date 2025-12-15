//! Dev-only MySQL adapter：从 `mysql://...` 连接串提取参数，注册到 connector 适配层。
//! 迁移自 `sinks/dev_adapters.rs`，便于按类型聚合维护。
#![allow(dead_code)] // dev 适配器仅在 CLI/测试环境使用

use std::collections::BTreeMap;
use wp_connector_api::{ConnectorKindAdapter, ParamMap};

use crate::mysql::config::MysqlConf;

pub struct DevMysqlAdapter;

impl ConnectorKindAdapter for DevMysqlAdapter {
    fn kind(&self) -> &'static str {
        "mysql"
    }
    fn url_to_params(&self, url: &str) -> anyhow::Result<ParamMap> {
        let cfg = MysqlConf::from_url(url)?;
        // Convert to ParamMap (serde_json values)
        let mut m: ParamMap = BTreeMap::new();
        m.insert("endpoint".into(), serde_json::Value::String(cfg.endpoint));
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

static DEV_MYSQL: DevMysqlAdapter = DevMysqlAdapter;

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    #[test]
    fn parse_mysql_basic_url() {
        let url = "mysql://root:pass@127.0.0.1:3306/wparse";
        let a = DevMysqlAdapter;
        let m = a.url_to_params(url).expect("parse ok");
        assert_eq!(
            m.get("endpoint"),
            Some(&Value::String("127.0.0.1:3306".into()))
        );
        assert_eq!(m.get("username"), Some(&Value::String("root".into())));
        assert_eq!(m.get("password"), Some(&Value::String("pass".into())));
        assert_eq!(m.get("database"), Some(&Value::String("wparse".into())));
        assert!(!m.contains_key("table"));
        assert!(!m.contains_key("batch"));
    }
}
