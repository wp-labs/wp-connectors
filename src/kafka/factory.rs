use async_trait::async_trait;
use serde_json::{Value, json};

use wp_connector_api::{
    ConnectorDef, ConnectorScope, ParamMap, SinkBuildCtx, SinkDefProvider, SinkFactory, SinkHandle,
    SinkReason, SinkResult, SinkSpec, SourceDefProvider, SourceFactory, SourceHandle, SourceMeta,
    SourceReason, SourceResult, SourceSvcIns, Tags,
};
use wp_model_core::model::fmt_def::TextFmt;

use crate::WP_SRC_VAL;
use crate::kafka::{
    KafkaSink, KafkaSource,
    config::{KafkaSinkConf, KafkaSourceConf},
};
use crate::utils::Protocol;
use crate::utils::arrow_decode::WireFormat;

fn build_kafka_conf_from_spec(
    spec: &wp_connector_api::SourceSpec,
) -> SourceResult<(KafkaSourceConf, String)> {
    let brokers = parse_required_string(spec.params.get("brokers"), "kafka.brokers")?;
    let topics = parse_topics(spec.params.get("topic"))?;
    let group_id = parse_required_string(spec.params.get("group_id"), "kafka.group_id")?;
    let config = parse_config(spec.params.get("config"))?;

    // Validate data_format up front so a typo is caught at validation time.
    WireFormat::parse_strict(spec.params.get("data_format").and_then(|v| v.as_str()))
        .map_err(SourceReason::other)?;

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
    let protocol = parse_protocol(spec.params.get("protocol"));

    // Validate data_format (only matters for protocol: arrow)
    if protocol == Protocol::Arrow
        && let Some(df) = spec.params.get("data_format").and_then(|v| v.as_str())
        && let Err(e) = WireFormat::parse_strict(Some(df))
    {
        return Err(SinkReason::core_conf().err().with_detail(e));
    }

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
            return Err(SourceReason::other(format!("{field} must not be empty")));
        }
        return Ok(trimmed.to_string());
    }
    Err(SourceReason::other(format!("{field} must not be empty")))
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
                return Err(SourceReason::other("kafka.topic must not be empty"));
            }
            Ok(topics)
        }
        Some(Value::Array(values)) => {
            let mut topics = Vec::new();
            for value in values {
                let Some(raw) = value.as_str() else {
                    return Err(SourceReason::other("kafka.topic entries must be strings"));
                };
                let trimmed = raw.trim();
                if trimmed.is_empty() {
                    continue;
                }
                topics.push(trimmed.to_string());
            }
            if topics.is_empty() {
                return Err(SourceReason::other("kafka.topic must not be empty"));
            }
            Ok(topics)
        }
        Some(_) => Err(SourceReason::other("kafka.topic must be a string or array")),
        None => Err(SourceReason::other("kafka.topic must not be empty")),
    }
}

fn parse_config(value: Option<&Value>) -> SourceResult<Option<Vec<String>>> {
    match value {
        None => Ok(None),
        Some(Value::Array(values)) => {
            let mut configs = Vec::new();
            for value in values {
                let Some(raw) = value.as_str() else {
                    return Err(SourceReason::other("kafka.config entries must be strings"));
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
        Some(_) => Err(SourceReason::other(
            "kafka.config must be a string or array",
        )),
    }
}

fn parse_sink_required_string(value: Option<&Value>, field: &str) -> SinkResult<String> {
    if let Some(Value::String(raw)) = value {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Err(SinkReason::sink(format!("{field} must not be empty")));
        }
        return Ok(trimmed.to_string());
    }
    Err(SinkReason::sink(format!("{field} must not be empty")))
}

fn parse_positive_i32(value: Option<&Value>, field: &str) -> SinkResult<Option<i32>> {
    match value {
        None => Ok(None),
        Some(v) => {
            let i = v
                .as_i64()
                .ok_or_else(|| SinkReason::sink(format!("{field} must be an integer")))?;
            if i <= 0 {
                return Err(SinkReason::sink(format!("{field} must be > 0")));
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
                    return Err(SinkReason::sink("kafka.config entries must be strings"));
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
        Some(_) => Err(SinkReason::sink("kafka.config must be a string or array")),
    }
}

fn parse_sink_fmt(value: Option<&Value>) -> SinkResult<TextFmt> {
    match value {
        None => Ok(TextFmt::Json),
        Some(Value::String(raw)) => {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                return Err(SinkReason::sink("kafka.fmt must not be empty"));
            }
            let ok = matches!(
                trimmed,
                "json" | "csv" | "show" | "kv" | "raw" | "proto" | "proto-text"
            );
            if !ok {
                return Err(SinkReason::sink(format!(
                    "invalid fmt: '{}'; allowed: json,csv,show,kv,raw,proto,proto-text",
                    trimmed
                )));
            }
            Ok(TextFmt::from(trimmed))
        }
        Some(_) => Err(SinkReason::sink("kafka.fmt must be a string")),
    }
}

fn parse_protocol(value: Option<&Value>) -> crate::utils::Protocol {
    match value.and_then(|v| v.as_str()).map(|s| s.trim()) {
        Some("arrow") => crate::utils::Protocol::Arrow,
        _ => crate::utils::Protocol::Text,
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

        let mut meta_tags = {
            let mut tags = Tags::new();
            for item in &spec.tags {
                if let Some((k, v)) = item.split_once('=').or_else(|| item.split_once(':')) {
                    tags.set(k, v);
                }
            }
            tags
        };
        let access_source = spec.kind.clone();
        meta_tags.set(WP_SRC_VAL, access_source);
        let source =
            KafkaSource::new(spec.name.clone(), meta_tags.clone(), &group_id, &conf).await?;

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
        let mut sink = KafkaSink::from_conf(&conf, fmt).await?;
        let protocol = parse_protocol(spec.params.get("protocol"));
        sink.set_protocol(protocol);
        // Parse data_format (only meaningful for protocol: arrow)
        let data_format = spec
            .params
            .get("data_format")
            .and_then(|v| v.as_str())
            .map(|s| WireFormat::from_data_format(Some(s)))
            .unwrap_or(if protocol == Protocol::Arrow {
                WireFormat::ArrowStream
            } else {
                WireFormat::default()
            });
        let tag = spec
            .params
            .get("tag")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        sink.set_data_format(data_format, tag);
        Ok(SinkHandle::new(Box::new(sink)))
    }
}

impl SourceDefProvider for KafkaSourceFactory {
    fn source_def(&self) -> ConnectorDef {
        ConnectorDef {
            id: "kafka_src".into(),
            kind: self.kind().into(),
            scope: ConnectorScope::Source,
            allow_override: vec!["brokers", "topic", "group_id", "config", "data_format"]
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
                "protocol",
                "num_partitions",
                "replication",
                "config",
                "data_format",
                "tag",
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
    params.insert("data_format".into(), json!("ndjson"));
    params
}

fn kafka_sink_defaults() -> ParamMap {
    let mut params = ParamMap::new();
    params.insert("brokers".into(), json!("localhost:9092"));
    params.insert("topic".into(), json!("wp_events"));
    params.insert("fmt".into(), json!("json"));
    params.insert("protocol".into(), json!("text"));
    params.insert("num_partitions".into(), json!(1));
    params.insert("replication".into(), json!(1));
    params.insert("data_format".into(), json!("arrow_ipc"));
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
        assert_eq!(err.reason(), &SourceReason::Other);
        assert!(
            err.detail()
                .as_deref()
                .is_some_and(|m| m.contains("kafka.topic"))
        );
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
        assert_eq!(err.reason(), &SinkReason::Sink);
        assert!(
            err.detail()
                .as_deref()
                .is_some_and(|m| m.contains("invalid fmt"))
        );
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

    #[test]
    fn parse_protocol_arrow() {
        let val = json!("arrow");
        let p = super::parse_protocol(Some(&val));
        assert_eq!(p, crate::utils::Protocol::Arrow);
    }

    #[test]
    fn parse_protocol_text() {
        let val = json!("text");
        let p = super::parse_protocol(Some(&val));
        assert_eq!(p, crate::utils::Protocol::Text);
    }

    #[test]
    fn parse_protocol_default_on_missing() {
        let p = super::parse_protocol(None);
        assert_eq!(p, crate::utils::Protocol::Text);
    }

    #[test]
    fn parse_protocol_default_on_unknown() {
        let val = json!("protobuf");
        let p = super::parse_protocol(Some(&val));
        assert_eq!(p, crate::utils::Protocol::Text);
    }

    // -- data_format validation ------------------------------------------

    fn source_spec_with_data_format(data_format: Option<&str>) -> wp_connector_api::SourceSpec {
        let mut params = BTreeMap::new();
        params.insert("brokers".into(), json!("localhost:9092"));
        params.insert("topic".into(), json!("topic_a"));
        params.insert("group_id".into(), json!("group-a"));
        if let Some(v) = data_format {
            params.insert("data_format".into(), json!(v));
        }
        build_source_spec(params)
    }

    #[test]
    fn kafka_source_accepts_known_data_format() {
        for v in [
            Some("ndjson"),
            Some("arrow_ipc"),
            Some("arrow_framed"),
            None,
        ] {
            let spec = source_spec_with_data_format(v);
            assert!(
                build_kafka_conf_from_spec(&spec).is_ok(),
                "expected OK for data_format = {v:?}"
            );
        }
    }

    #[test]
    fn kafka_source_rejects_unknown_data_format() {
        let spec = source_spec_with_data_format(Some("arrowipcc"));
        let err = build_kafka_conf_from_spec(&spec).expect_err("unknown data_format");
        assert!(err.to_string().contains("data_format must be one of"));
    }

    #[test]
    fn kafka_source_def_advertises_data_format() {
        let def = KafkaSourceFactory.source_def();
        assert!(def.allow_override.contains(&"data_format".to_string()));
        assert_eq!(
            def.default_params.get("data_format"),
            Some(&json!("ndjson"))
        );
    }

    // -- Sink data_format validation ------------------------------------

    fn sink_spec_with_data_format(
        data_format: Option<&str>,
        protocol: Option<&str>,
    ) -> wp_connector_api::SinkSpec {
        let mut params = BTreeMap::new();
        params.insert("brokers".into(), json!("localhost:9092"));
        params.insert("topic".into(), json!("topic_a"));
        if let Some(v) = data_format {
            params.insert("data_format".into(), json!(v));
        }
        if let Some(v) = protocol {
            params.insert("protocol".into(), json!(v));
        }
        SinkSpec {
            name: "test".into(),
            kind: "kafka".into(),
            connector_id: String::new(),
            group: "test".into(),
            params,
            filter: None,
        }
    }

    #[test]
    fn kafka_sink_accepts_known_data_format() {
        let factory = KafkaSinkFactory;
        for v in [
            Some("ndjson"),
            Some("arrow_ipc"),
            Some("arrow_framed"),
            None,
        ] {
            let spec = sink_spec_with_data_format(v, None);
            assert!(
                factory.validate_spec(&spec).is_ok(),
                "expected OK for data_format = {v:?} with default protocol"
            );
        }
    }

    #[test]
    fn kafka_sink_rejects_unknown_data_format() {
        let factory = KafkaSinkFactory;
        // data_format validation only fires when protocol is Arrow
        let spec = sink_spec_with_data_format(Some("arrowipcc"), Some("arrow"));
        let err = factory
            .validate_spec(&spec)
            .expect_err("unknown data_format with protocol=arrow");
        assert!(err.to_string().contains("data_format must be one of"));
    }

    #[test]
    fn kafka_sink_text_protocol_ignores_data_format() {
        let factory = KafkaSinkFactory;
        // When protocol is text (or missing), data_format validation is skipped.
        let spec = sink_spec_with_data_format(Some("arrowipcc"), Some("text"));
        assert!(
            factory.validate_spec(&spec).is_ok(),
            "text protocol should ignore data_format"
        );
        let spec2 = sink_spec_with_data_format(Some("nonsense"), None);
        assert!(
            factory.validate_spec(&spec2).is_ok(),
            "default protocol (text) should ignore data_format"
        );
    }

    #[test]
    fn kafka_sink_def_advertises_data_format() {
        let def = KafkaSinkFactory.sink_def();
        assert!(def.allow_override.contains(&"data_format".to_string()));
        assert!(def.allow_override.contains(&"tag".to_string()));
        assert_eq!(
            def.default_params.get("data_format"),
            Some(&json!("arrow_ipc"))
        );
    }
}
