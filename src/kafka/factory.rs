use async_trait::async_trait;

use wp_connector_api::{
    SinkBuildCtx, SinkFactory, SinkHandle, SinkSpec, SourceHandle, SourceMeta, SourceReason,
    SourceResult, SourceSvcIns, Tags,
};
use wp_model_core::model::{TagSet, fmt_def::TextFmt};

use crate::kafka::{
    KafkaSink, KafkaSource,
    config::{KafkaSourceConf, OutKafka},
};

pub struct KafkaSourceFactory;

#[async_trait]
impl wp_connector_api::SourceFactory for KafkaSourceFactory {
    fn kind(&self) -> &'static str {
        "kafka"
    }

    fn validate_spec(&self, spec: &wp_connector_api::SourceSpec) -> SourceResult<()> {
        let brokers = spec
            .params
            .get("brokers")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if brokers.trim().is_empty() {
            return Err(SourceReason::Other("kafka.brokers must not be empty".into()).into());
        }

        let topic = spec
            .params
            .get("topic")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if topic.trim().is_empty() {
            return Err(SourceReason::Other("kafka.topic must not be empty".into()).into());
        }

        let group_id = spec
            .params
            .get("group_id")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if group_id.trim().is_empty() {
            return Err(SourceReason::Other("kafka.group_id must not be empty".into()).into());
        }

        Ok(())
    }

    async fn build(
        &self,
        spec: &wp_connector_api::SourceSpec,
        _ctx: &wp_connector_api::SourceBuildCtx,
    ) -> SourceResult<SourceSvcIns> {
        // Merge params into KafkaSourceConf
        let mut conf = KafkaSourceConf::default();
        if let Some(s) = spec.params.get("brokers").and_then(|v| v.as_str()) {
            conf.brokers = s.to_string();
        }
        if let Some(s) = spec.params.get("topic").and_then(|v| v.as_str()) {
            conf.topic = vec![s.to_string()];
        }
        if let Some(arr) = spec.params.get("config").and_then(|v| v.as_array()) {
            let vec = arr
                .iter()
                .filter_map(|x| x.as_str().map(|s| s.to_string()))
                .collect();
            conf.config = Some(vec);
        }

        let (mut tag_set, mut meta_tags) = extract_spec_tags(&spec.tags);
        let access_source = spec.kind.clone();
        tag_set.append("access_source", &access_source);
        meta_tags.set("access_source", access_source);
        let source = KafkaSource::new(
            spec.name.clone(),
            tag_set,
            &format!("{}_group", spec.name), // Use spec name as group_id (conf lacks explicit field)
            &conf,
        )
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

    fn validate_spec(&self, spec: &SinkSpec) -> anyhow::Result<()> {
        let brokers = spec
            .params
            .get("brokers")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if brokers.trim().is_empty() {
            anyhow::bail!("kafka.brokers must not be empty");
        }

        let topic = spec
            .params
            .get("topic")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if topic.trim().is_empty() {
            anyhow::bail!("kafka.topic must not be empty");
        }

        if let Some(i) = spec.params.get("num_partitions").and_then(|v| v.as_i64())
            && i <= 0
        {
            anyhow::bail!("kafka.num_partitions must be > 0");
        }
        if let Some(i) = spec.params.get("replication").and_then(|v| v.as_i64())
            && i <= 0
        {
            anyhow::bail!("kafka.replication must be > 0");
        }
        if let Some(s) = spec.params.get("fmt").and_then(|v| v.as_str()) {
            let ok = matches!(
                s,
                "json" | "csv" | "show" | "kv" | "raw" | "proto" | "proto-text"
            );
            if !ok {
                anyhow::bail!(
                    "invalid fmt: '{}'; allowed: json,csv,show,kv,raw,proto,proto-text",
                    s
                );
            }
        }
        Ok(())
    }

    async fn build(&self, spec: &SinkSpec, _ctx: &SinkBuildCtx) -> anyhow::Result<SinkHandle> {
        // Merge params into OutKafka
        let mut conf = OutKafka::default();
        if let Some(s) = spec.params.get("brokers").and_then(|v| v.as_str()) {
            conf.brokers = s.to_string();
        }
        if let Some(s) = spec.params.get("topic").and_then(|v| v.as_str()) {
            conf.topic = s.to_string();
        }
        if let Some(i) = spec.params.get("num_partitions").and_then(|v| v.as_i64()) {
            conf.num_partitions = i as i32;
        }
        if let Some(i) = spec.params.get("replication").and_then(|v| v.as_i64()) {
            conf.replication = i as i32;
        }
        // Output format (optional): default json
        let fmt = spec
            .params
            .get("fmt")
            .and_then(|v| v.as_str())
            .map(TextFmt::from)
            .unwrap_or(TextFmt::Json);
        if let Some(arr) = spec.params.get("config").and_then(|v| v.as_array()) {
            let vec = arr
                .iter()
                .filter_map(|x| x.as_str().map(|s| s.to_string()))
                .collect();
            conf.config = Some(vec);
        }
        let sink = KafkaSink::from_conf(&conf, fmt).await?;
        Ok(SinkHandle::new(Box::new(sink)))
    }
}

fn extract_spec_tags(raw_tags: &[String]) -> (TagSet, Tags) {
    let mut tag_set = TagSet::default();
    let mut src_tags = Tags::new();
    for raw in raw_tags {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            continue;
        }
        let (key, value) = if let Some((k, v)) = trimmed.split_once('=') {
            (k.trim(), v.trim())
        } else {
            (trimmed, "")
        };
        if key.is_empty() {
            continue;
        }
        tag_set.append(key, value);
        src_tags.set(key.to_string(), value.to_string());
    }
    (tag_set, src_tags)
}
