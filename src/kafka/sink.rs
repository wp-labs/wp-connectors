use async_trait::async_trait;
use orion_error::conversion::SourceRawErr;
use rdkafka_wrap::{KWProducer, KWProducerConf, OptionExt};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use wp_connector_api::SinkErrorOwe;
use wp_connector_api::{AsyncCtrl, AsyncRawDataSink, AsyncRecordSink, SinkReason, SinkResult};
use wp_data_fmt::{FormatType, RecordFormatter};
use wp_model_core::model::{DataRecord, fmt_def::TextFmt};

use crate::kafka::config::KafkaSinkConf;
use crate::utils::Protocol;

pub struct KafkaSink {
    pub(crate) inner: Arc<KWProducer>,
    pub(crate) fmt: TextFmt,
    pub(crate) protocol: Protocol,
}

#[async_trait]
impl AsyncCtrl for KafkaSink {
    async fn stop(&mut self) -> SinkResult<()> {
        self.inner
            .flush(rdkafka_wrap::util::Timeout::After(Duration::from_secs(3)))
            .source_raw_err(SinkReason::Sink, "kafka stop fail")?;
        Ok(())
    }
    async fn reconnect(&mut self) -> SinkResult<()> {
        let conf = self.inner.conf.clone();
        self.inner = Arc::new(
            KWProducer::new(conf).source_raw_err(SinkReason::Sink, "kafka  reconnect fail")?,
        );
        Ok(())
    }
}

#[async_trait]
impl AsyncRawDataSink for KafkaSink {
    async fn sink_str(&mut self, data: &str) -> SinkResult<()> {
        self.inner
            .publish(data.as_bytes(), Default::default())
            .await
            .source_raw_err(SinkReason::Sink, "kafka send fail")?;
        Ok(())
    }
    async fn sink_bytes(&mut self, data: &[u8]) -> SinkResult<()> {
        self.inner
            .publish(data, Default::default())
            .await
            .source_raw_err(SinkReason::Sink, "kafka send fail")?;
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
        let line = format!("{}\n", fmt.fmt_record(data));
        self.inner
            .publish(line.as_bytes(), Default::default())
            .await
            .source_raw_err(SinkReason::Sink, "kafka send fail")?;
        Ok(())
    }
    async fn sink_records(&mut self, data: Vec<Arc<DataRecord>>) -> SinkResult<()> {
        if self.protocol == Protocol::Arrow {
            use crate::utils::arrow_fmt::records_to_arrow_ipc;
            let ipc_bytes = records_to_arrow_ipc(&data).owe_sink("arrow ipc")?;
            self.inner
                .publish(&ipc_bytes, Default::default())
                .await
                .source_raw_err(SinkReason::Sink, "kafka send arrow fail")?;
            return Ok(());
        }
        // Text path
        for item in data {
            self.sink_record(item.as_ref()).await?;
        }
        Ok(())
    }
}

impl KafkaSink {
    pub fn set_protocol(&mut self, protocol: Protocol) {
        self.protocol = protocol;
    }

    pub async fn from_conf(conf: &KafkaSinkConf, fmt: TextFmt) -> SinkResult<Self> {
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
        let producer =
            KWProducer::new(kc).source_raw_err(SinkReason::Sink, "init kafka producer failed")?;
        producer
            .create_topic()
            .await
            .source_raw_err(SinkReason::Sink, "create kafka topic failed")?;
        Ok(Self {
            inner: Arc::new(producer),
            fmt,
            protocol: Protocol::default(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::arrow_fmt::records_to_arrow_ipc;
    use arrow::array::Array;
    use arrow::ipc::reader::StreamReader;
    use std::sync::Arc;
    use wp_model_core::model::DataField;

    fn make_record(fields: Vec<DataField>) -> Arc<DataRecord> {
        let mut rec = DataRecord::default();
        for f in fields {
            rec.append(f);
        }
        Arc::new(rec)
    }

    // -- Arrow IPC roundtrip (sink path) ------------------------------------

    #[test]
    fn arrow_sink_path_produces_valid_ipc() {
        // Simulate the data that would flow through KafkaSink with protocol:arrow
        let records: Vec<Arc<DataRecord>> = vec![
            make_record(vec![
                DataField::from_chars("name", "alice"),
                DataField::from_digit("count", 42),
            ]),
            make_record(vec![
                DataField::from_chars("name", "bob"),
                DataField::from_digit("count", 7),
            ]),
        ];

        // Same conversion that sink_records uses
        let ipc_bytes = records_to_arrow_ipc(&records).expect("ipc conversion");
        assert!(!ipc_bytes.is_empty());

        // Read back
        let cursor = std::io::Cursor::new(&ipc_bytes);
        let mut reader = StreamReader::try_new(cursor, None).expect("read ipc");
        let schema = reader.schema();
        assert_eq!(schema.fields().len(), 2);

        let batch = reader.next().expect("first batch").expect("ok");
        assert_eq!(batch.num_rows(), 2);

        // Verify data
        let names = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("name col");
        assert_eq!(names.value(0), "alice");
        assert_eq!(names.value(1), "bob");

        let counts = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("count col");
        assert_eq!(counts.value(0), 42);
        assert_eq!(counts.value(1), 7);
    }

    #[test]
    fn arrow_sink_path_empty_records() {
        let records: Vec<Arc<DataRecord>> = vec![];
        let ipc_bytes = records_to_arrow_ipc(&records).expect("ipc");
        assert!(!ipc_bytes.is_empty()); // schema-only IPC stream

        let cursor = std::io::Cursor::new(&ipc_bytes);
        let reader = StreamReader::try_new(cursor, None).expect("read ipc");
        assert_eq!(reader.schema().fields().len(), 0);
    }

    #[test]
    fn arrow_sink_path_typed_fields() {
        // Simulate records with typed fields (Digit, Float, Bool)
        let records: Vec<Arc<DataRecord>> = vec![make_record(vec![
            DataField::from_bool("active", true),
            DataField::from_digit("count", 100),
            DataField::from_float("score", 1.5),
        ])];

        let ipc_bytes = records_to_arrow_ipc(&records).expect("ipc");
        let cursor = std::io::Cursor::new(&ipc_bytes);
        let mut reader = StreamReader::try_new(cursor, None).expect("read ipc");

        let batch = reader.next().expect("first batch").expect("ok");
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 3);

        // Verify types are preserved
        batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .expect("col 0 should be Boolean");
        batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("col 1 should be Int64");
        batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .expect("col 2 should be Float64");
    }
}
