use async_trait::async_trait;
use serde_json::{Value, json};

use wp_conf_base::ConfParser;
use wp_connector_api::{
    ConnectorDef, ConnectorScope, ParamMap, SinkBuildCtx, SinkDefProvider, SinkError, SinkFactory,
    SinkHandle, SinkReason, SinkResult, SinkSpec, SourceDefProvider, SourceFactory, SourceHandle,
    SourceMeta, SourceReason, SourceResult, SourceSvcIns, Tags,
};
use wp_model_core::model::fmt_def::TextFmt;

use crate::WP_SRC_VAL;
use crate::kafka::{
    KafkaSink, KafkaSource,
    config::{KafkaSinkConf, KafkaSourceConf},
};

fn build_kafka_conf_from_spec(
    spec: &wp_connector_api::SourceSpec,
) -> SourceResult<(KafkaSourceConf, String)> {
    let brokers = parse_required_string(spec.params.get("brokers"), "kafka.brokers")?;
    let topics = parse_topics(spec.params.get("topic"))?;
    let group_id = parse_required_string(spec.params.get("group_id"), "kafka.group_id")?;
    let config = parse_config(spec.params.get("config"))?;

    let conf = KafkaSourceConf {
        key: spec.name.clone(),
        brokers,
        topic: topics,
        config,
        //TODO: use spec.enable
        enable: true,
    };
    Ok((conf, group_id))
}

fn build_kafka_sink_conf_from_spec(spec: &SinkSpec) -> SinkResult<(KafkaSinkConf, TextFmt)> {
    let brokers = parse_sink_required_string(spec.params.get("brokers"), "kafka.brokers")?;
    let topic = parse_sink_required_string(spec.params.get("topic"), "kafka.topic")?;
    let num_partitions =
        parse_positive_i32(spec.params.get("num_partitions"), "kafka.num_partitions")?;
    let replication = parse_positive_i32(spec.params.get("replication"), "kafka.replication")?;
    let config = parse_sink_config(spec.params.get("config"))?;
    let fmt = parse_sink_fmt(spec.params.get("fmt"))?;

    let conf = KafkaSinkConf {
        brokers,
        topic,
        num_partitions: num_partitions.unwrap_or_default(),
        replication: replication.unwrap_or_default(),
        config,
    };
    Ok((conf, fmt))
}

fn parse_required_string(value: Option<&Value>, field: &str) -> SourceResult<String> {
    if let Some(Value::String(raw)) = value {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Err(SourceReason::Other(format!("{field} must not be empty")).into());
        }
        return Ok(trimmed.to_string());
    }
    Err(SourceReason::Other(format!("{field} must not be empty")).into())
}

fn parse_topics(value: Option<&Value>) -> SourceResult<Vec<String>> {
    match value {
        Some(Value::String(raw)) => {
            let topics = raw
                .split(',')
                .map(|topic| topic.trim())
                .filter(|topic| !topic.is_empty())
                .map(|topic| topic.to_string())
                .collect::<Vec<_>>();
            if topics.is_empty() {
                return Err(SourceReason::Other("kafka.topic must not be empty".into()).into());
            }
            Ok(topics)
        }
        Some(Value::Array(values)) => {
            let mut topics = Vec::new();
            for value in values {
                let Some(raw) = value.as_str() else {
                    return Err(
                        SourceReason::Other("kafka.topic entries must be strings".into()).into(),
                    );
                };
                let trimmed = raw.trim();
                if trimmed.is_empty() {
                    continue;
                }
                topics.push(trimmed.to_string());
            }
            if topics.is_empty() {
                return Err(SourceReason::Other("kafka.topic must not be empty".into()).into());
            }
            Ok(topics)
        }
        Some(_) => Err(SourceReason::Other("kafka.topic must be a string or array".into()).into()),
        None => Err(SourceReason::Other("kafka.topic must not be empty".into()).into()),
    }
}

fn parse_config(value: Option<&Value>) -> SourceResult<Option<Vec<String>>> {
    match value {
        None => Ok(None),
        Some(Value::Array(values)) => {
            let mut configs = Vec::new();
            for value in values {
                let Some(raw) = value.as_str() else {
                    return Err(
                        SourceReason::Other("kafka.config entries must be strings".into()).into(),
                    );
                };
                let trimmed = raw.trim();
                if trimmed.is_empty() {
                    continue;
                }
                configs.push(trimmed.to_string());
            }
            if configs.is_empty() {
                Ok(None)
            } else {
                Ok(Some(configs))
            }
        }
        Some(Value::String(raw)) => {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                Ok(None)
            } else {
                Ok(Some(vec![trimmed.to_string()]))
            }
        }
        Some(_) => Err(SourceReason::Other("kafka.config must be a string or array".into()).into()),
    }
}

fn parse_sink_required_string(value: Option<&Value>, field: &str) -> SinkResult<String> {
    if let Some(Value::String(raw)) = value {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Err(SinkReason::sink(format!("{field} must not be empty")).into());
        }
        return Ok(trimmed.to_string());
    }
    Err(SinkReason::sink(format!("{field} must not be empty")).into())
}

fn parse_positive_i32(value: Option<&Value>, field: &str) -> SinkResult<Option<i32>> {
    match value {
        None => Ok(None),
        Some(v) => {
            let i = v
                .as_i64()
                .ok_or_else(|| SinkReason::sink(format!("{field} must be an integer")))?;
            if i <= 0 {
                return Err(SinkReason::sink(format!("{field} must be > 0")).into());
            }
            Ok(Some(i as i32))
        }
    }
}

fn parse_sink_config(value: Option<&Value>) -> SinkResult<Option<Vec<String>>> {
    match value {
        None => Ok(None),
        Some(Value::Array(values)) => {
            let mut configs = Vec::new();
            for value in values {
                let Some(raw) = value.as_str() else {
                    return Err(SinkReason::sink("kafka.config entries must be strings").into());
                };
                let trimmed = raw.trim();
                if trimmed.is_empty() {
                    continue;
                }
                configs.push(trimmed.to_string());
            }
            if configs.is_empty() {
                Ok(None)
            } else {
                Ok(Some(configs))
            }
        }
        Some(Value::String(raw)) => {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                Ok(None)
            } else {
                Ok(Some(vec![trimmed.to_string()]))
            }
        }
        Some(_) => Err(SinkReason::sink("kafka.config must be a string or array").into()),
    }
}

fn parse_sink_fmt(value: Option<&Value>) -> SinkResult<TextFmt> {
    match value {
        None => Ok(TextFmt::Json),
        Some(Value::String(raw)) => {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                return Err(SinkReason::sink("kafka.fmt must not be empty").into());
            }
            let ok = matches!(
                trimmed,
                "json" | "csv" | "show" | "kv" | "raw" | "proto" | "proto-text"
            );
            if !ok {
                return Err(SinkReason::sink(format!(
                    "invalid fmt: '{}'; allowed: json,csv,show,kv,raw,proto,proto-text",
                    trimmed
                ))
                .into());
            }
            Ok(TextFmt::from(trimmed))
        }
        Some(_) => Err(SinkReason::sink("kafka.fmt must be a string").into()),
    }
}

pub struct KafkaSourceFactory;

#[async_trait]
impl wp_connector_api::SourceFactory for KafkaSourceFactory {
    fn kind(&self) -> &'static str {
        "kafka"
    }

    fn validate_spec(&self, spec: &wp_connector_api::SourceSpec) -> SourceResult<()> {
        build_kafka_conf_from_spec(spec)?;
        Ok(())
    }

    async fn build(
        &self,
        spec: &wp_connector_api::SourceSpec,
        _ctx: &wp_connector_api::SourceBuildCtx,
    ) -> SourceResult<SourceSvcIns> {
        let (conf, group_id) = build_kafka_conf_from_spec(spec)?;

        let mut meta_tags = Tags::from_parse(&spec.tags);
        let access_source = spec.kind.clone();
        meta_tags.set(WP_SRC_VAL, access_source);
        let source = KafkaSource::new(spec.name.clone(), meta_tags.clone(), &group_id, &conf)
            .await
            .map_err(|err| SourceReason::Other(err.to_string()))?;

        let mut meta = SourceMeta::new(spec.name.clone(), spec.kind.clone());
        meta.tags = meta_tags;
        let handle = SourceHandle::new(Box::new(source), meta);
        Ok(SourceSvcIns::new().with_sources(vec![handle]))
    }
}

pub struct KafkaSinkFactory;

#[async_trait]
impl SinkFactory for KafkaSinkFactory {
    fn kind(&self) -> &'static str {
        "kafka"
    }

    fn validate_spec(&self, spec: &SinkSpec) -> SinkResult<()> {
        build_kafka_sink_conf_from_spec(spec)?;
        Ok(())
    }

    async fn build(&self, spec: &SinkSpec, _ctx: &SinkBuildCtx) -> SinkResult<SinkHandle> {
        let (conf, fmt) = build_kafka_sink_conf_from_spec(spec)?;
        let sink = KafkaSink::from_conf(&conf, fmt).await.map_err(|err| {
            SinkError::from(SinkReason::sink(format!("init kafka sink failed: {err}")))
        })?;
        Ok(SinkHandle::new(Box::new(sink)))
    }
}

impl SourceDefProvider for KafkaSourceFactory {
    fn source_def(&self) -> ConnectorDef {
        ConnectorDef {
            id: "kafka_src".into(),
            kind: self.kind().into(),
            scope: ConnectorScope::Source,
            allow_override: vec!["brokers", "topic", "group_id", "config"]
                .into_iter()
                .map(str::to_string)
                .collect(),
            default_params: kafka_source_defaults(),
            origin: Some("wp-connectors:kafka_source".into()),
        }
    }
}

impl SinkDefProvider for KafkaSinkFactory {
    fn sink_def(&self) -> ConnectorDef {
        ConnectorDef {
            id: "kafka_sink".into(),
            kind: self.kind().into(),
            scope: ConnectorScope::Sink,
            allow_override: vec![
                "brokers",
                "topic",
                "fmt",
                "num_partitions",
                "replication",
                "config",
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
            default_params: kafka_sink_defaults(),
            origin: Some("wp-connectors:kafka_sink".into()),
        }
    }
}

fn kafka_source_defaults() -> ParamMap {
    let mut params = ParamMap::new();
    params.insert("brokers".into(), json!("localhost:9092"));
    params.insert("topic".into(), json!("wp_events"));
    params.insert("group_id".into(), json!("wp_events_group"));
    params.insert(
        "config".into(),
        json!(["auto.offset.reset=latest", "enable.auto.commit=true"]),
    );
    params
}

fn kafka_sink_defaults() -> ParamMap {
    let mut params = ParamMap::new();
    params.insert("brokers".into(), json!("localhost:9092"));
    params.insert("topic".into(), json!("wp_events"));
    params.insert("fmt".into(), json!("json"));
    params.insert("num_partitions".into(), json!(1));
    params.insert("replication".into(), json!(1));
    params
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};
    use std::collections::BTreeMap;

    fn build_source_spec(params: BTreeMap<String, Value>) -> wp_connector_api::SourceSpec {
        wp_connector_api::SourceSpec {
            name: "kafka_source".into(),
            kind: "kafka".into(),
            connector_id: "connector".into(),
            params,
            tags: vec![],
        }
    }

    fn build_sink_spec(params: BTreeMap<String, Value>) -> wp_connector_api::SinkSpec {
        wp_connector_api::SinkSpec {
            name: "kafka_sink".into(),
            kind: "kafka".into(),
            connector_id: "connector".into(),
            group: "group".into(),
            params,
            filter: None,
        }
    }

    #[test]
    fn kafka_conf_from_spec_parses_topics_and_config_array() {
        let mut params = BTreeMap::new();
        params.insert("brokers".into(), json!("localhost:9092"));
        params.insert("topic".into(), json!("topic_a,topic_b"));
        params.insert("group_id".into(), json!("group-a"));
        params.insert(
            "config".into(),
            json!(["auto.offset.reset=earliest", "enable.auto.commit=true"]),
        );
        let spec = build_source_spec(params);

        let (conf, group_id) = build_kafka_conf_from_spec(&spec).expect("valid spec");
        assert_eq!(conf.brokers, "localhost:9092");
        assert_eq!(
            conf.topic,
            vec!["topic_a".to_string(), "topic_b".to_string()]
        );
        assert_eq!(group_id, "group-a");
        assert_eq!(
            conf.config.as_ref().unwrap(),
            &vec![
                "auto.offset.reset=earliest".to_string(),
                "enable.auto.commit=true".to_string()
            ]
        );
    }

    #[test]
    fn kafka_conf_from_spec_rejects_missing_topic() {
        let mut params = BTreeMap::new();
        params.insert("brokers".into(), json!("localhost:9092"));
        params.insert("group_id".into(), json!("group-a"));
        let spec = build_source_spec(params);

        let err = build_kafka_conf_from_spec(&spec).expect_err("topic missing");
        let msg = format!("{err}");
        assert!(msg.contains("kafka.topic"));
    }

    #[test]
    fn kafka_conf_from_spec_supports_array_topics_and_string_config() {
        let mut params = BTreeMap::new();
        params.insert("brokers".into(), json!("localhost:9092"));
        params.insert("group_id".into(), json!("group-a"));
        params.insert("topic".into(), json!(["topic_a", "topic_b"]));
        params.insert("config".into(), json!("auto.offset.reset=latest"));
        let spec = build_source_spec(params);

        let (conf, group_id) = build_kafka_conf_from_spec(&spec).expect("valid spec");
        assert_eq!(
            conf.topic,
            vec!["topic_a".to_string(), "topic_b".to_string()]
        );
        assert_eq!(group_id, "group-a");
        assert_eq!(
            conf.config,
            Some(vec!["auto.offset.reset=latest".to_string()])
        );
    }

    #[test]
    fn kafka_sink_conf_from_spec_parses_fields() {
        let mut params = BTreeMap::new();
        params.insert("brokers".into(), json!("localhost:9092"));
        params.insert("topic".into(), json!("sink-topic"));
        params.insert("num_partitions".into(), json!(6));
        params.insert("replication".into(), json!(2));
        params.insert("fmt".into(), json!("csv"));
        params.insert(
            "config".into(),
            json!(["acks=all", "compression.type=snappy"]),
        );
        let spec = build_sink_spec(params);

        let (conf, fmt) = build_kafka_sink_conf_from_spec(&spec).expect("valid sink spec");
        assert_eq!(conf.brokers, "localhost:9092");
        assert_eq!(conf.topic, "sink-topic");
        assert_eq!(conf.num_partitions, 6);
        assert_eq!(conf.replication, 2);
        assert_eq!(fmt, TextFmt::Csv);
        assert_eq!(
            conf.config,
            Some(vec![
                "acks=all".to_string(),
                "compression.type=snappy".to_string()
            ])
        );
    }

    #[test]
    fn kafka_sink_conf_from_spec_rejects_invalid_fmt() {
        let mut params = BTreeMap::new();
        params.insert("brokers".into(), json!("localhost:9092"));
        params.insert("topic".into(), json!("sink-topic"));
        params.insert("fmt".into(), json!("bad"));
        let spec = build_sink_spec(params);

        let err = build_kafka_sink_conf_from_spec(&spec).expect_err("invalid fmt");
        let msg = format!("{err}");
        assert!(msg.contains("invalid fmt"));
    }

    #[test]
    fn kafka_sink_conf_from_spec_supports_string_config() {
        let mut params = BTreeMap::new();
        params.insert("brokers".into(), json!("localhost:9092"));
        params.insert("topic".into(), json!("sink-topic"));
        params.insert("config".into(), json!("acks=1"));
        let spec = build_sink_spec(params);

        let (conf, _fmt) = build_kafka_sink_conf_from_spec(&spec).expect("valid sink spec");
        assert_eq!(conf.config, Some(vec!["acks=1".to_string()]));
    }
}
