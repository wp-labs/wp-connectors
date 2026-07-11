//! Arrow decode helpers for raw-byte sources.
//!
//! Sources such as Kafka and HTTP emit payloads as raw [`RawData::Bytes`].
//! When those bytes are an Arrow IPC stream (or a wp_arrow frame), this module
//! turns them into `RecordBatch`es.
//!
//! This mirrors the encode-side `arrow_fmt` module: `arrow_fmt` serialises
//! `DataRecord`s → Arrow IPC (sink direction); this module deserialises
//! Arrow IPC → `RecordBatch` (source direction).
//!
//! [`WireFormat`] and the core decode logic are re-exported from
//! `wp_connector_utils::arrow`.

pub use wp_connector_utils::arrow::SUPPORTED_DATA_FORMATS;
pub use wp_connector_utils::arrow::WireFormat;

use arrow::record_batch::RecordBatch;
use wf_connector_api::{SourceReason, SourceResult};
use wp_connector_api::SourceBatch;

/// Decode raw Arrow IPC Stream bytes from `SourceEvent`s into `RecordBatch`es.
///
/// Each event's payload is decoded independently via
/// [`wp_connector_utils::arrow::decode_arrow_ipc_batches`]; results are
/// concatenated.
pub fn decode_arrow_ipc_batches(events: &SourceBatch) -> SourceResult<Vec<RecordBatch>> {
    let mut batches = Vec::new();
    for event in events {
        let decoded = wp_connector_utils::arrow::decode_arrow_ipc_batches(event.payload.as_bytes())
            .map_err(|e| SourceReason::Decode.err_detail(e))?;
        batches.extend(decoded);
    }
    Ok(batches)
}

/// Decode wp_arrow frames into `RecordBatch`es.
///
/// Each event's payload is decoded independently via
/// [`wp_connector_utils::arrow::decode_arrow_framed_batches`]; results are
/// concatenated.
pub fn decode_arrow_framed_batches(events: &SourceBatch) -> SourceResult<Vec<RecordBatch>> {
    let mut batches = Vec::new();
    for event in events {
        let decoded =
            wp_connector_utils::arrow::decode_arrow_framed_batches(event.payload.as_bytes())
                .map_err(|e| SourceReason::Decode.err_detail(e))?;
        batches.extend(decoded);
    }
    Ok(batches)
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

    // -- WireFormat parsing tests (verifying shared re-export) ------------

    #[test]
    fn wire_format_re_export_works() {
        assert_eq!(WireFormat::default(), WireFormat::Ndjson);
        assert_eq!(
            WireFormat::from_data_format(Some("arrow_ipc")),
            WireFormat::ArrowStream
        );
        assert_eq!(
            WireFormat::from_data_format(Some("arrow_framed")),
            WireFormat::ArrowFramed
        );
        assert!(!WireFormat::Ndjson.is_arrow());
        assert!(WireFormat::ArrowStream.is_arrow());
        assert!(WireFormat::parse_strict(Some("arrowipcc")).is_err());
    }

    // -- decode helpers ----------------------------------------------------

    fn make_ipc_bytes(schema: &Arc<Schema>, values: &[&str]) -> Vec<u8> {
        let mut buf = Vec::new();
        let mut w = StreamWriter::try_new(&mut buf, schema).unwrap();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(values.to_vec()))],
        )
        .unwrap();
        w.write(&batch).unwrap();
        w.finish().unwrap();
        buf
    }

    fn event_from_bytes(bytes: impl Into<Bytes>) -> SourceEvent {
        SourceEvent::new(
            1,
            "key".to_string(),
            RawData::Bytes(bytes.into()),
            Default::default(),
        )
    }

    #[test]
    fn decode_ipc_roundtrip() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Utf8, false)]));
        let event = event_from_bytes(make_ipc_bytes(&schema, &["hello"]));
        let batches = decode_arrow_ipc_batches(&vec![event]).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
    }

    #[test]
    fn decode_ipc_invalid_returns_err() {
        let event = event_from_bytes(Bytes::from_static(b"ARROW?? not really"));
        assert!(decode_arrow_ipc_batches(&vec![event]).is_err());
    }

    #[test]
    fn decode_framed_roundtrip() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Utf8, false)]));
        let ipc = make_ipc_bytes(&schema, &["hi"]);
        let tag = b"my_tag";
        let mut frame = Vec::new();
        frame.extend_from_slice(&(tag.len() as u32).to_be_bytes());
        frame.extend_from_slice(tag);
        frame.extend_from_slice(&ipc);

        let batches = decode_arrow_framed_batches(&vec![event_from_bytes(frame)]).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
    }

    #[test]
    fn decode_framed_too_short_payload_is_skipped() {
        let event = event_from_bytes(Bytes::from(vec![0, 0]));
        let batches = decode_arrow_framed_batches(&vec![event]).unwrap();
        assert!(batches.is_empty());
    }

    #[test]
    fn decode_framed_tag_len_exceeds_payload_is_skipped() {
        let event = event_from_bytes(Bytes::from(vec![0xff, 0xff, 0xff, 0xff, 0x00]));
        let batches = decode_arrow_framed_batches(&vec![event]).unwrap();
        assert!(batches.is_empty());
    }

    #[test]
    fn decode_framed_empty_tag() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Utf8, false)]));
        let ipc = make_ipc_bytes(&schema, &["hi"]);
        let mut frame = Vec::new();
        frame.extend_from_slice(&0u32.to_be_bytes());
        frame.extend_from_slice(&ipc);
        let batches = decode_arrow_framed_batches(&vec![event_from_bytes(frame)]).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
    }
}
