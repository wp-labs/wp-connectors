//! Dev-only Kafka adapter：从 `kafka://...` 连接串提取参数，注册到 wp-connector 桥接层。
//! 迁移自 `sinks/dev_adapters.rs`，便于按类型聚合维护。

use std::collections::BTreeMap;
use winnow::prelude::*;
use wp_connector_api::{ConnectorKindAdapter, ParamMap};
// use explicit fully-qualified calls to avoid version-coherence issues; keep import empty

#[allow(dead_code)]
pub struct DevKafkaAdapter;

impl ConnectorKindAdapter for DevKafkaAdapter {
    fn kind(&self) -> &'static str {
        "kafka"
    }
    fn url_to_params(&self, url: &str) -> anyhow::Result<ParamMap> {
        // 支持：kafka://<brokers>[?k=v&...]
        let mut input = url;
        type CtxE = winnow::error::ContextError;
        // 前缀（可选）
        if winnow::token::literal::<&str, &str, CtxE>("kafka://")
            .parse_next(&mut input)
            .is_err()
        {
            // 若无前缀，视为纯 brokers/qs 拼接
        }

        let mut params: ParamMap = BTreeMap::new();
        // brokers：直到 '?'（或结尾）
        let brokers: &str = winnow::token::take_till::<_, &str, CtxE>(0.., |c: char| c == '?')
            .parse_next(&mut input)
            .unwrap_or("");
        let brokers = brokers.trim();
        if !brokers.is_empty() {
            params.insert(
                "brokers".to_string(),
                serde_json::Value::String(brokers.to_string()),
            );
        }
        // 可选的 '?'
        let _ = winnow::token::literal::<&str, &str, CtxE>("?").parse_next(&mut input);

        // 解析 querystring：k=v(&k=v)*
        while !input.is_empty() {
            // key
            let key: &str = winnow::token::take_until::<&str, &str, CtxE>(0.., "=")
                .parse_next(&mut input)
                .unwrap_or("");
            let key = key.trim();
            if key.is_empty() {
                break;
            }
            // '='
            let _ = winnow::token::literal::<&str, &str, CtxE>("=").parse_next(&mut input);
            // value（直到 '&' 或结尾；允许空）
            let val: &str = winnow::token::take_till::<_, &str, CtxE>(0.., |c: char| c == '&')
                .parse_next(&mut input)
                .unwrap_or("");
            let v = val.trim();

            if matches!(key, "num_partitions" | "replication")
                && let Ok(i) = v.parse::<i64>()
            {
                params.insert(key.to_string(), serde_json::Value::from(i));
            } else if !key.is_empty() {
                params.insert(key.to_string(), serde_json::Value::String(v.to_string()));
            }

            // 可选 &
            if winnow::token::literal::<&str, &str, CtxE>("&")
                .parse_next(&mut input)
                .is_err()
            {
                break;
            }
        }
        Ok(params)
    }
}

//static DEV_KAFKA: DevKafkaAdapter = DevKafkaAdapter;

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    #[test]
    fn parse_with_prefix_and_query() {
        let url = "kafka://k1:9092,k2:9092?topic=t1&num_partitions=3&replication=2&fmt=json&config=acks=all";
        let a = DevKafkaAdapter;
        let m = a.url_to_params(url).expect("parse ok");
        assert_eq!(
            m.get("brokers"),
            Some(&Value::String("k1:9092,k2:9092".into()))
        );
        assert_eq!(m.get("topic"), Some(&Value::String("t1".into())));
        assert_eq!(m.get("fmt"), Some(&Value::String("json".into())));
        assert_eq!(m.get("config"), Some(&Value::String("acks=all".into())));
        assert_eq!(m.get("num_partitions"), Some(&Value::from(3)));
        assert_eq!(m.get("replication"), Some(&Value::from(2)));
    }

    #[test]
    fn parse_without_prefix() {
        let url = "localhost:9092?topic=a";
        let a = DevKafkaAdapter;
        let m = a.url_to_params(url).expect("parse ok");
        assert_eq!(
            m.get("brokers"),
            Some(&Value::String("localhost:9092".into()))
        );
        assert_eq!(m.get("topic"), Some(&Value::String("a".into())));
    }

    #[test]
    fn parse_only_brokers() {
        let url = "kafka://localhost:9092";
        let a = DevKafkaAdapter;
        let m = a.url_to_params(url).expect("parse ok");
        assert_eq!(
            m.get("brokers"),
            Some(&Value::String("localhost:9092".into()))
        );
        assert!(!m.contains_key("topic"));
    }

    #[test]
    fn parse_empty_value_and_invalid_int() {
        let url = "kafka://b:9092?topic=&replication=abc";
        let a = DevKafkaAdapter;
        let m = a.url_to_params(url).expect("parse ok");
        assert_eq!(m.get("topic"), Some(&Value::String(String::new())));
        // 无法解析为整数时，按字符串保留
        assert_eq!(m.get("replication"), Some(&Value::String("abc".into())));
    }
}
