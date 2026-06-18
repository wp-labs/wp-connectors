//! Arrow decode helpers for raw-byte sources.
//!
//! Sources such as Kafka and HTTP emit payloads as raw [`RawData::Bytes`].
//! When those bytes are an Arrow IPC stream (or a wp_arrow frame), this module
//! turns them into `RecordBatch`es.
//!
//! This mirrors the encode-side `arrow_fmt` module: `arrow_fmt` serialises
//! `DataRecord`s → Arrow IPC (sink direction); this module deserialises
//! Arrow IPC → `RecordBatch` (source direction).

use arrow::record_batch::RecordBatch;
use wf_connector_api::{SourceReason, SourceResult};
use wp_connector_api::SourceBatch;

/// On-the-wire payload format expected by a raw-byte source.
///
/// Parsed from the `data_format` spec parameter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WireFormat {
    /// Newline-delimited JSON (line-oriented).
    #[default]
    Ndjson,
    /// Arrow IPC Stream format (no length prefix, `StreamReader`-compatible).
    ArrowStream,
    /// wp_arrow frame: `[4B tag_len][tag][Arrow IPC Stream]`.
    /// Produced by wparse `encode_ipc`.
    ArrowFramed,
}

/// Strict parse error for [`WireFormat::parse_strict`].
pub const SUPPORTED_DATA_FORMATS: &[&str] = &["ndjson", "arrow_ipc", "arrow_framed"];

impl WireFormat {
    /// Lenient parse from the `data_format` spec parameter.
    ///
    /// `None` and any unrecognised value fall back to [`WireFormat::Ndjson`].
    /// Use [`WireFormat::parse_strict`] in factories/specs that should reject
    /// unknown values explicitly rather than silently degrading to NDJSON.
    pub fn from_data_format(value: Option<&str>) -> Self {
        match value.unwrap_or("ndjson") {
            "arrow_framed" => WireFormat::ArrowFramed,
            "arrow_ipc" => WireFormat::ArrowStream,
            _ => WireFormat::Ndjson,
        }
    }

    /// Strict parse from the `data_format` spec parameter.
    ///
    /// Returns `Err` for unknown values so a typo (e.g. `arrowipcc`) is caught
    /// at validation time instead of silently degrading to NDJSON.
    pub fn parse_strict(value: Option<&str>) -> Result<Self, String> {
        match value {
            None | Some("ndjson") => Ok(WireFormat::Ndjson),
            Some("arrow_ipc") => Ok(WireFormat::ArrowStream),
            Some("arrow_framed") => Ok(WireFormat::ArrowFramed),
            Some(raw) => Err(format!(
                "data_format must be one of: {} (got '{raw}')",
                SUPPORTED_DATA_FORMATS.join(", ")
            )),
        }
    }

    /// `true` when this is a binary Arrow format (not NDJSON).
    pub fn is_arrow(self) -> bool {
        matches!(self, WireFormat::ArrowStream | WireFormat::ArrowFramed)
    }
}

/// Decode raw Arrow IPC Stream bytes from `SourceEvent`s into `RecordBatch`es.
///
/// Each event's payload is decoded independently; batches from all events are
/// concatenated into the result.
pub fn decode_arrow_ipc_batches(events: &SourceBatch) -> SourceResult<Vec<RecordBatch>> {
    use arrow::ipc::reader::StreamReader;
    let mut batches = Vec::new();
    for event in events {
        // RawData::as_bytes() works for Bytes / ArcBytes / String variants.
        let payload = event.payload.as_bytes().to_vec();
        let cursor = std::io::Cursor::new(payload);
        let reader = StreamReader::try_new(cursor, None)
            .map_err(|e| SourceReason::Decode.err_detail(format!("arrow ipc: {e}")))?;
        for batch in reader {
            let batch = batch
                .map_err(|e| SourceReason::Decode.err_detail(format!("arrow ipc batch: {e}")))?;
            batches.push(batch);
        }
    }
    Ok(batches)
}

/// Decode wp_arrow frames into `RecordBatch`es.
///
/// Frame layout: `[4B tag_len (big-endian u32)][tag][Arrow IPC Stream]`.
/// The `tag` is currently only used to locate the IPC payload and is otherwise
/// discarded. Payloads too short for the header (or whose declared `tag_len`
/// exceeds the payload) are silently skipped rather than aborting the batch.
pub fn decode_arrow_framed_batches(events: &SourceBatch) -> SourceResult<Vec<RecordBatch>> {
    use arrow::ipc::reader::StreamReader;
    let mut batches = Vec::new();
    for event in events {
        let payload = event.payload.as_bytes().to_vec();
        if payload.len() < 4 {
            continue;
        }
        let tag_len = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]) as usize;
        let ipc_start = 4 + tag_len;
        if ipc_start > payload.len() {
            continue;
        }
        let ipc_bytes = &payload[ipc_start..];
        let cursor = std::io::Cursor::new(ipc_bytes);
        let reader = StreamReader::try_new(cursor, None)
            .map_err(|e| SourceReason::Decode.err_detail(format!("arrow framed: {e}")))?;
        for batch in reader {
            let batch = batch
                .map_err(|e| SourceReason::Decode.err_detail(format!("arrow framed batch: {e}")))?;
            batches.push(batch);
        }
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

    // -- WireFormat parsing ------------------------------------------------

    #[test]
    fn from_data_format_defaults_to_ndjson() {
        assert_eq!(WireFormat::from_data_format(None), WireFormat::Ndjson);
        assert_eq!(WireFormat::default(), WireFormat::Ndjson);
    }

    #[test]
    fn from_data_format_parses_arrow() {
        assert_eq!(
            WireFormat::from_data_format(Some("arrow_ipc")),
            WireFormat::ArrowStream
        );
        assert_eq!(
            WireFormat::from_data_format(Some("arrow_framed")),
            WireFormat::ArrowFramed
        );
    }

    #[test]
    fn from_data_format_unknown_falls_back_to_ndjson() {
        assert_eq!(
            WireFormat::from_data_format(Some("nonsense")),
            WireFormat::Ndjson
        );
    }

    #[test]
    fn parse_strict_rejects_unknown() {
        let err = WireFormat::parse_strict(Some("arrowipcc")).unwrap_err();
        assert!(err.contains("data_format must be one of"));
    }

    #[test]
    fn parse_strict_accepts_known() {
        assert_eq!(WireFormat::parse_strict(None).unwrap(), WireFormat::Ndjson);
        assert_eq!(
            WireFormat::parse_strict(Some("ndjson")).unwrap(),
            WireFormat::Ndjson
        );
        assert_eq!(
            WireFormat::parse_strict(Some("arrow_ipc")).unwrap(),
            WireFormat::ArrowStream
        );
        assert_eq!(
            WireFormat::parse_strict(Some("arrow_framed")).unwrap(),
            WireFormat::ArrowFramed
        );
    }

    #[test]
    fn is_arrow_flag() {
        assert!(!WireFormat::Ndjson.is_arrow());
        assert!(WireFormat::ArrowStream.is_arrow());
        assert!(WireFormat::ArrowFramed.is_arrow());
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
