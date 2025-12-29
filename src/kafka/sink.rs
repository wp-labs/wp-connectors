use async_trait::async_trait;
use orion_conf::ErrorOwe;
use rdkafka_wrap::{KWProducer, KWProducerConf, OptionExt};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use wp_connector_api::{AsyncCtrl, AsyncRawDataSink, AsyncRecordSink, SinkReason, SinkResult};
use wp_data_fmt::{DataFormat, FormatType};
use wp_model_core::model::{DataRecord, fmt_def::TextFmt};

use crate::kafka::config::KafkaSinkConf;

type AnyResult<T> = anyhow::Result<T>;

pub struct KafkaSink {
    pub(crate) inner: Arc<KWProducer>,
    pub(crate) fmt: TextFmt,
}

#[async_trait]
impl AsyncCtrl for KafkaSink {
    async fn stop(&mut self) -> SinkResult<()> {
        self.inner
            .flush(rdkafka_wrap::util::Timeout::After(Duration::from_secs(3)))
            .owe(SinkReason::Sink("kafka stop fail".into()))?;
        Ok(())
    }
    async fn reconnect(&mut self) -> SinkResult<()> {
        let conf = self.inner.conf.clone();
        self.inner =
            Arc::new(KWProducer::new(conf).owe(SinkReason::Sink("kafka  reconnect fail".into()))?);
        Ok(())
    }
}

#[async_trait]
impl AsyncRawDataSink for KafkaSink {
    async fn sink_str(&mut self, data: &str) -> SinkResult<()> {
        self.inner
            .publish(data.as_bytes(), Default::default())
            .await
            .owe(SinkReason::Sink("kafka send fail".into()))?;
        Ok(())
    }
    async fn sink_bytes(&mut self, data: &[u8]) -> SinkResult<()> {
        self.inner
            .publish(data, Default::default())
            .await
            .owe(SinkReason::Sink("kafka send fail".into()))?;
        Ok(())
    }

    async fn sink_str_batch(&mut self, data: Vec<&str>) -> SinkResult<()> {
        for item in data {
            self.sink_str(item).await?;
        }
        Ok(())
    }

    async fn sink_bytes_batch(&mut self, data: Vec<&[u8]>) -> SinkResult<()> {
        for item in data {
            self.sink_bytes(item).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl AsyncRecordSink for KafkaSink {
    async fn sink_record(&mut self, data: &DataRecord) -> SinkResult<()> {
        // 非文件类 sink 支持通过参数选择输出格式（默认 json）
        let fmt = FormatType::from(&self.fmt);
        let line = format!("{}\n", fmt.format_record(data));
        self.inner
            .publish(line.as_bytes(), Default::default())
            .await
            .owe(SinkReason::Sink("kafka send fail".into()))?;
        Ok(())
    }
    async fn sink_records(&mut self, data: Vec<Arc<DataRecord>>) -> SinkResult<()> {
        for item in data {
            self.sink_record(item.as_ref()).await?;
        }
        Ok(())
    }
}

impl KafkaSink {
    pub async fn from_conf(conf: &KafkaSinkConf, fmt: TextFmt) -> AnyResult<Self> {
        let mut kc = KWProducerConf::new(&conf.brokers).set_topic_conf(
            &conf.topic,
            conf.num_partitions,
            conf.replication,
        );
        if let Some(items) = &conf.config {
            let mut m = HashMap::new();
            for c in items {
                let v: Vec<&str> = c.split('=').collect();
                if v.len() >= 2 {
                    m.insert(v[0].trim(), v[1].trim());
                }
            }
            kc = kc.set_config(m);
        }
        let producer = KWProducer::new(kc)?;
        producer.create_topic().await?;
        Ok(Self {
            inner: Arc::new(producer),
            fmt,
        })
    }
}
