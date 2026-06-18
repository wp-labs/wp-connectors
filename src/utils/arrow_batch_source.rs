//! Generic `BatchSource` adapter that decodes a raw-byte `DataSource` into
//! Arrow `RecordBatch`es.
//!
//! This is the `wp-connectors` counterpart to `wp-core-connectors`'
//! `TcpBatchSource` / `FileBatchSource`: sources such as Kafka and HTTP emit
//! payloads as raw bytes ([`RawData::Bytes`]); when those bytes are an Arrow
//! IPC stream (or a wp_arrow frame), this adapter turns them into
//! `RecordBatch`es.
//!
//! This adapter is **Arrow-only**: it does not parse NDJSON. NDJSON → typed
//! `RecordBatch` conversion is handled by `wp-core-connectors`'
//! `FileBatchSource` / `TcpBatchSource`. The schema is taken from each Arrow
//! stream itself, so no external schema is needed.

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use wf_connector_api::{BatchSource, SourceError, SourceReason, SourceResult};
use wp_connector_api::{DataSource, SourceBatch, SourceError as WpError};

use super::arrow_decode::{WireFormat, decode_arrow_framed_batches, decode_arrow_ipc_batches};

/// A `BatchSource` that decodes a wrapped `DataSource`'s raw payloads into
/// Arrow `RecordBatch`es.
///
/// Construct with [`ArrowBatchSource::new`], which only accepts an Arrow
/// [`WireFormat`] (`ArrowStream` or `ArrowFramed`); passing `Ndjson` is a
/// configuration error and is rejected at construction time.
pub struct ArrowBatchSource {
    key: String,
    inner: Box<dyn DataSource>,
    format: WireFormat,
}

impl ArrowBatchSource {
    /// Wrap a raw-byte `DataSource` and decode it as the given Arrow `format`.
    ///
    /// Returns an error if `format` is [`WireFormat::Ndjson`] — this adapter
    /// is Arrow-only (NDJSON parsing belongs to `wp-core-connectors`).
    /// The Arrow schema is derived from each stream, so no schema is required.
    pub fn new(
        key: impl Into<String>,
        source: Box<dyn DataSource>,
        format: WireFormat,
    ) -> SourceResult<Self> {
        if !format.is_arrow() {
            return Err(SourceReason::Decode.err_detail(
                "ArrowBatchSource requires an Arrow WireFormat (arrow_ipc or arrow_framed); \
                 NDJSON is not supported by this adapter",
            ));
        }
        Ok(Self {
            key: key.into(),
            inner: source,
            format,
        })
    }

    fn convert_batch(&self, events: SourceBatch) -> SourceResult<Vec<RecordBatch>> {
        if events.is_empty() {
            return Ok(vec![]);
        }
        match self.format {
            // Ndjson is rejected in new(); keep the arm exhaustive and explicit.
            WireFormat::ArrowStream => decode_arrow_ipc_batches(&events),
            WireFormat::ArrowFramed => decode_arrow_framed_batches(&events),
            WireFormat::Ndjson => {
                unreachable!("ArrowBatchSource::new rejects Ndjson; this branch should never run")
            }
        }
    }

    fn wp_error_to_wf(err: WpError) -> SourceError {
        // Map a wp-connector-api source error into a wf-connector-api error.
        // Mirrors wp-core-connectors' batch/error.rs::wp_error_to_wf.
        match err.reason() {
            wp_connector_api::SourceReason::EOF => SourceError::from(SourceReason::EOF),
            wp_connector_api::SourceReason::SupplierError
            | wp_connector_api::SourceReason::Disconnect => {
                SourceReason::Connect.err_detail(err.to_string())
            }
            _ => SourceReason::Decode.err_detail(err.to_string()),
        }
    }
}

#[async_trait]
impl BatchSource for ArrowBatchSource {
    async fn start(&mut self) -> SourceResult<()> {
        self.inner.close().await.ok();
        Ok(())
    }

    async fn receive_batch(&mut self) -> SourceResult<Vec<RecordBatch>> {
        match self.inner.receive().await {
            Ok(batch) => self.convert_batch(batch),
            Err(e) => Err(Self::wp_error_to_wf(e)),
        }
    }

    async fn close(&mut self) -> SourceResult<()> {
        self.inner.close().await.ok();
        Ok(())
    }

    fn identifier(&self) -> &str {
        &self.key
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::writer::StreamWriter;
    use bytes::Bytes;
    use std::sync::Arc;
    use wp_connector_api::SourceEvent;
    use wp_model_core::raw::RawData;

    /// In-memory `DataSource` that yields one pre-built batch then EOF.
    struct OnceDataSource {
        batch: Option<Vec<SourceEvent>>,
    }

    #[async_trait]
    impl DataSource for OnceDataSource {
        async fn receive(&mut self) -> Result<Vec<SourceEvent>, WpError> {
            match self.batch.take() {
                Some(b) => Ok(b),
                None => Err(WpError::from(wp_connector_api::SourceReason::EOF)),
            }
        }
        fn try_receive(&mut self) -> Option<Vec<SourceEvent>> {
            None
        }
        fn identifier(&self) -> String {
            "once".to_string()
        }
    }

    fn ipc_bytes(values: &[&str]) -> Vec<u8> {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Utf8, false)]));
        let mut buf = Vec::new();
        let mut w = StreamWriter::try_new(&mut buf, &schema).unwrap();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(values.to_vec()))],
        )
        .unwrap();
        w.write(&batch).unwrap();
        w.finish().unwrap();
        buf
    }

    fn make_event(bytes: impl Into<Bytes>) -> SourceEvent {
        SourceEvent::new(
            1,
            "k".to_string(),
            RawData::Bytes(bytes.into()),
            Default::default(),
        )
    }

    #[tokio::test]
    async fn arrow_ipc_roundtrip() {
        let inner = Box::new(OnceDataSource {
            batch: Some(vec![make_event(ipc_bytes(&["a", "b"]))]),
        });
        let mut src = ArrowBatchSource::new("t", inner, WireFormat::ArrowStream).unwrap();
        src.start().await.unwrap();
        let batches = src.receive_batch().await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);
        // drained -> EOF surfaces as error
        assert!(src.receive_batch().await.is_err());
    }

    #[tokio::test]
    async fn arrow_framed_roundtrip() {
        let ipc = ipc_bytes(&["c"]);
        let mut frame = Vec::new();
        frame.extend_from_slice(&1u32.to_be_bytes());
        frame.extend_from_slice(b"t");
        frame.extend_from_slice(&ipc);

        let inner = Box::new(OnceDataSource {
            batch: Some(vec![make_event(frame)]),
        });
        let mut src = ArrowBatchSource::new("t", inner, WireFormat::ArrowFramed).unwrap();
        src.start().await.unwrap();
        let batches = src.receive_batch().await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
    }

    #[tokio::test]
    async fn corrupt_arrow_returns_err_not_panic() {
        let inner = Box::new(OnceDataSource {
            batch: Some(vec![make_event(Bytes::from_static(b"not arrow"))]),
        });
        let mut src = ArrowBatchSource::new("t", inner, WireFormat::ArrowStream).unwrap();
        src.start().await.unwrap();
        assert!(src.receive_batch().await.is_err());
    }

    #[test]
    fn rejects_ndjson_format() {
        // ArrowBatchSource is Arrow-only; Ndjson must be rejected at construction.
        let inner = Box::new(OnceDataSource { batch: None });
        match ArrowBatchSource::new("t", inner, WireFormat::Ndjson) {
            Err(e) => assert!(
                e.to_string().contains("Arrow WireFormat"),
                "unexpected error: {e}"
            ),
            Ok(_) => panic!("expected Err for Ndjson format, got Ok"),
        }
    }
}
