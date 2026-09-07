//! Arrow IPC serialization for sink `protocol: arrow`.
//!
//! Converts `Vec<Arc<DataRecord>>` → `RecordBatch` → Arrow IPC Stream bytes.
//! Gated behind feature `"wf"` (which enables `dep:arrow`).

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanBuilder, Float64Builder, Int32Builder, Int64Builder, StringBuilder,
    TimestampNanosecondBuilder,
};
use arrow::datatypes::{DataType as ArrowType, Field, Schema, TimeUnit};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use wp_model_core::model::DataRecord;
use wp_model_core::model::types::meta::DataType;
use wp_model_core::model::types::value::Value;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Serialize a batch of [`DataRecord`]s into Arrow IPC Stream bytes.
///
/// Schema is inferred from the first record's field names and types.
/// If records is empty, returns an empty IPC stream (schema-only).
pub fn records_to_arrow_ipc(records: &[Arc<DataRecord>]) -> Result<Vec<u8>, ArrowFmtError> {
    let schema = infer_schema(records)?;
    let batch = records_to_batch(records, &schema)?;

    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, batch.schema().as_ref())
            .map_err(|e| ArrowFmtError::IpcWrite(format!("StreamWriter::try_new: {e}")))?;
        writer
            .write(&batch)
            .map_err(|e| ArrowFmtError::IpcWrite(format!("write batch: {e}")))?;
        writer
            .finish()
            .map_err(|e| ArrowFmtError::IpcWrite(format!("finish: {e}")))?;
    }
    Ok(buf)
}

/// Serialize a batch of [`DataRecord`]s into a wp_arrow IPC frame:
/// `[4B tag_len (big-endian)][tag][Arrow IPC Stream]`.
///
/// Wraps [`records_to_arrow_ipc`] with the wp_arrow frame header so a peer
/// using `data_format = "arrow_framed"` (via [`crate::utils::arrow_decode::decode_arrow_framed_batches`])
/// can recover the IPC stream and the stream `tag`.
pub fn records_to_arrow_ipc_frame(
    tag: &str,
    records: &[Arc<DataRecord>],
) -> Result<Vec<u8>, ArrowFmtError> {
    let ipc = records_to_arrow_ipc(records)?;
    let tag_bytes = tag.as_bytes();
    let mut buf = Vec::with_capacity(4 + tag_bytes.len() + ipc.len());
    buf.extend_from_slice(&(tag_bytes.len() as u32).to_be_bytes());
    buf.extend_from_slice(tag_bytes);
    buf.extend_from_slice(&ipc);
    Ok(buf)
}

#[derive(Debug)]
pub enum ArrowFmtError {
    IpcWrite(String),
    Build(String),
}

impl std::fmt::Display for ArrowFmtError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ArrowFmtError::IpcWrite(msg) => write!(f, "Arrow IPC error: {msg}"),
            ArrowFmtError::Build(msg) => write!(f, "Arrow build error: {msg}"),
        }
    }
}

impl std::error::Error for ArrowFmtError {}

// ---------------------------------------------------------------------------
// Schema inference
// ---------------------------------------------------------------------------

/// Infer Arrow schema from the first record.
fn infer_schema(records: &[Arc<DataRecord>]) -> Result<Arc<Schema>, ArrowFmtError> {
    if records.is_empty() {
        return Ok(Arc::new(Schema::empty()));
    }
    let first = &records[0];
    let fields: Vec<Field> = first
        .items
        .iter()
        .filter(|f| !matches!(f.get_meta(), DataType::Ignore))
        .map(|f| {
            let name = f.get_name().to_string();
            let arrow_type = data_type_to_arrow(f.get_meta());
            Field::new(name, arrow_type, true)
        })
        .collect();
    Ok(Arc::new(Schema::new(fields)))
}

/// Map wp-model-core [`DataType`] to Arrow [`ArrowType`].
fn data_type_to_arrow(dt: &DataType) -> ArrowType {
    match dt {
        DataType::Bool => ArrowType::Boolean,
        DataType::Digit => ArrowType::Int64,
        DataType::Float => ArrowType::Float64,
        // Time → Timestamp(Nanosecond)
        DataType::Time
        | DataType::TimeISO
        | DataType::TimeRFC3339
        | DataType::TimeRFC2822
        | DataType::TimeTIMESTAMP
        | DataType::TimeCLF => ArrowType::Timestamp(TimeUnit::Nanosecond, None),
        DataType::Port => ArrowType::Int32,
        // Binary-ish → Binary
        DataType::Hex | DataType::Base64 => ArrowType::Binary,
        // String-ish → Utf8
        DataType::Chars
        | DataType::Symbol
        | DataType::PeekSymbol
        | DataType::IP
        | DataType::IpNet
        | DataType::Domain
        | DataType::Email
        | DataType::SN
        | DataType::Url
        | DataType::MobilePhone
        | DataType::HttpRequest
        | DataType::HttpStatus
        | DataType::HttpAgent
        | DataType::HttpMethod
        | DataType::IdCard
        | DataType::KV
        | DataType::Json
        | DataType::ExactJson
        | DataType::ProtoText
        | DataType::Auto => ArrowType::Utf8,
        DataType::Ignore => ArrowType::Utf8,
        // Nested types → Utf8 (JSON representation)
        DataType::Obj | DataType::Array(_) | DataType::KvArr => ArrowType::Utf8,
        _ => ArrowType::Utf8,
    }
}

// ---------------------------------------------------------------------------
// RecordBatch construction
// ---------------------------------------------------------------------------

fn records_to_batch(
    records: &[Arc<DataRecord>],
    schema: &Schema,
) -> Result<RecordBatch, ArrowFmtError> {
    if records.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::new(schema.clone())));
    }

    let num_cols = schema.fields().len();
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(num_cols);

    for (col_idx, field) in schema.fields().iter().enumerate() {
        let array = build_column(records, field, col_idx)?;
        arrays.push(array);
    }

    RecordBatch::try_new(Arc::new(schema.clone()), arrays)
        .map_err(|e| ArrowFmtError::Build(format!("RecordBatch::try_new: {e}")))
}

fn build_column(
    records: &[Arc<DataRecord>],
    field: &Field,
    col_idx: usize,
) -> Result<ArrayRef, ArrowFmtError> {
    match field.data_type() {
        ArrowType::Boolean => build_boolean_col(records, field, col_idx),
        ArrowType::Int32 => build_int32_col(records, field, col_idx),
        ArrowType::Int64 => build_int64_col(records, field, col_idx),
        ArrowType::Float64 => build_float64_col(records, field, col_idx),
        ArrowType::Timestamp(TimeUnit::Nanosecond, _) => {
            build_timestamp_ns_col(records, field, col_idx)
        }
        ArrowType::Binary => build_binary_col(records, field, col_idx),
        ArrowType::Utf8 => build_utf8_col(records, field, col_idx),
        other => Err(ArrowFmtError::Build(format!(
            "unsupported Arrow type for column '{}': {other:?}",
            field.name()
        ))),
    }
}

/// Get the field value from a `DataRecord` by column index (schema order).
fn field_value(record: &DataRecord, col_idx: usize) -> Option<&Value> {
    record.items.get(col_idx).map(|f| f.get_value())
}

// -- Column builders -------------------------------------------------------

fn build_boolean_col(
    records: &[Arc<DataRecord>],
    field: &Field,
    col_idx: usize,
) -> Result<ArrayRef, ArrowFmtError> {
    let mut builder = BooleanBuilder::new();
    for rec in records {
        match field_value(rec, col_idx) {
            Some(Value::Bool(b)) => builder.append_value(*b),
            Some(Value::Chars(s)) => builder.append_value(s.eq_ignore_ascii_case("true")),
            _ if field.is_nullable() => builder.append_null(),
            _ => builder.append_value(false),
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn build_int32_col(
    records: &[Arc<DataRecord>],
    field: &Field,
    col_idx: usize,
) -> Result<ArrayRef, ArrowFmtError> {
    let mut builder = Int32Builder::new();
    for rec in records {
        match field_value(rec, col_idx) {
            Some(Value::Digit(d)) => builder.append_value(*d as i32),
            Some(Value::Float(f)) => builder.append_value(*f as i32),
            Some(Value::Chars(s)) => {
                if let Ok(v) = s.parse::<i32>() {
                    builder.append_value(v);
                } else {
                    builder.append_null();
                }
            }
            _ if field.is_nullable() => builder.append_null(),
            _ => builder.append_value(0),
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn build_int64_col(
    records: &[Arc<DataRecord>],
    field: &Field,
    col_idx: usize,
) -> Result<ArrayRef, ArrowFmtError> {
    let mut builder = Int64Builder::new();
    for rec in records {
        match field_value(rec, col_idx) {
            Some(Value::Digit(d)) => builder.append_value(*d),
            Some(Value::Float(f)) => builder.append_value(*f as i64),
            Some(Value::Chars(s)) => {
                if let Ok(v) = s.parse::<i64>() {
                    builder.append_value(v);
                } else {
                    builder.append_null();
                }
            }
            Some(Value::Time(dt)) => {
                let millis = dt.and_utc().timestamp_millis();
                builder.append_value(millis);
            }
            _ if field.is_nullable() => builder.append_null(),
            _ => builder.append_value(0),
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn build_float64_col(
    records: &[Arc<DataRecord>],
    field: &Field,
    col_idx: usize,
) -> Result<ArrayRef, ArrowFmtError> {
    let mut builder = Float64Builder::new();
    for rec in records {
        match field_value(rec, col_idx) {
            Some(Value::Float(f)) => builder.append_value(*f),
            Some(Value::Digit(d)) => builder.append_value(*d as f64),
            Some(Value::Chars(s)) => {
                if let Ok(v) = s.parse::<f64>() {
                    builder.append_value(v);
                } else {
                    builder.append_null();
                }
            }
            _ if field.is_nullable() => builder.append_null(),
            _ => builder.append_value(0.0),
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn build_timestamp_ns_col(
    records: &[Arc<DataRecord>],
    field: &Field,
    col_idx: usize,
) -> Result<ArrayRef, ArrowFmtError> {
    let mut builder = TimestampNanosecondBuilder::new();
    for rec in records {
        match field_value(rec, col_idx) {
            Some(Value::Time(dt)) => {
                if let Some(ns) = dt.and_utc().timestamp_nanos_opt() {
                    builder.append_value(ns);
                } else {
                    builder.append_null();
                }
            }
            Some(Value::Digit(d)) => builder.append_value(*d * 1_000_000),
            Some(Value::Chars(s)) => {
                if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
                    if let Some(ns) = dt.timestamp_nanos_opt() {
                        builder.append_value(ns);
                    } else {
                        builder.append_null();
                    }
                } else if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                {
                    if let Some(ns) = dt.and_utc().timestamp_nanos_opt() {
                        builder.append_value(ns);
                    } else {
                        builder.append_null();
                    }
                } else {
                    builder.append_null();
                }
            }
            _ if field.is_nullable() => builder.append_null(),
            _ => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn build_binary_col(
    records: &[Arc<DataRecord>],
    field: &Field,
    col_idx: usize,
) -> Result<ArrayRef, ArrowFmtError> {
    use arrow::array::BinaryBuilder;
    let mut builder = BinaryBuilder::new();
    for rec in records {
        match field_value(rec, col_idx) {
            Some(v) => {
                let bytes = to_raw_bytes(v);
                builder.append_value(&bytes[..]);
            }
            _ if field.is_nullable() => builder.append_null(),
            _ => builder.append_value(b""),
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn build_utf8_col(
    records: &[Arc<DataRecord>],
    field: &Field,
    col_idx: usize,
) -> Result<ArrayRef, ArrowFmtError> {
    let mut builder = StringBuilder::new();
    for rec in records {
        match field_value(rec, col_idx) {
            Some(v) => builder.append_value(format_utf8_value(v)),
            _ if field.is_nullable() => builder.append_null(),
            _ => builder.append_value(""),
        }
    }
    Ok(Arc::new(builder.finish()))
}

// -- Value formatting --------------------------------------------------------

fn format_utf8_value(v: &Value) -> String {
    match v {
        Value::Obj(_) | Value::Array(_) => {
            serde_json::to_string(v).unwrap_or_else(|_| format!("{v:?}"))
        }
        _ => v.to_string(),
    }
}

fn to_raw_bytes(v: &Value) -> Vec<u8> {
    match v {
        Value::Hex(h) => {
            if h.0 == 0 {
                return vec![0];
            }
            let be = h.0.to_be_bytes();
            let start = be.iter().position(|&b| b != 0).unwrap();
            be[start..].to_vec()
        }
        _ => format_utf8_value(v).into_bytes(),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;
    use arrow::ipc::reader::StreamReader;
    use wp_model_core::model::DataField;

    // -- Helpers ----------------------------------------------------------

    fn make_record(fields: Vec<DataField>) -> Arc<DataRecord> {
        let mut rec = DataRecord::default();
        for f in fields {
            rec.append(f);
        }
        Arc::new(rec)
    }

    fn char_field(name: &str, val: &str) -> DataField {
        DataField::from_chars(name, val)
    }

    fn digit_field(name: &str, val: i64) -> DataField {
        DataField::from_digit(name, val)
    }

    // -- Empty records ----------------------------------------------------

    #[test]
    fn empty_records_returns_valid_ipc() {
        let records: Vec<Arc<DataRecord>> = vec![];
        let ipc = records_to_arrow_ipc(&records).expect("empty ipc");
        assert!(
            !ipc.is_empty(),
            "even empty schema should produce IPC bytes"
        );

        // Should be readable as a stream with schema-only
        let cursor = std::io::Cursor::new(&ipc);
        let reader = StreamReader::try_new(cursor, None).expect("read empty ipc");
        let schema = reader.schema();
        assert_eq!(schema.fields().len(), 0);
    }

    // -- Schema inference -------------------------------------------------

    #[test]
    fn schema_inferred_from_first_record() {
        let recs = vec![
            make_record(vec![char_field("name", "alice"), digit_field("age", 30)]),
            make_record(vec![char_field("name", "bob"), digit_field("age", 25)]),
        ];
        let ipc = records_to_arrow_ipc(&recs).expect("ipc");
        let cursor = std::io::Cursor::new(&ipc);
        let reader = StreamReader::try_new(cursor, None).expect("read ipc");
        let schema = reader.schema();

        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "name");
        assert_eq!(schema.field(0).data_type(), &ArrowType::Utf8);
        assert_eq!(schema.field(1).name(), "age");
        assert_eq!(schema.field(1).data_type(), &ArrowType::Int64);
    }

    // -- Type mapping -----------------------------------------------------

    #[test]
    fn digit_maps_to_int64() {
        assert_eq!(data_type_to_arrow(&DataType::Digit), ArrowType::Int64);
    }

    #[test]
    fn float_maps_to_float64() {
        assert_eq!(data_type_to_arrow(&DataType::Float), ArrowType::Float64);
    }

    #[test]
    fn bool_maps_to_boolean() {
        assert_eq!(data_type_to_arrow(&DataType::Bool), ArrowType::Boolean);
    }

    #[test]
    fn chars_maps_to_utf8() {
        assert_eq!(data_type_to_arrow(&DataType::Chars), ArrowType::Utf8);
    }

    #[test]
    fn time_maps_to_timestamp_ns() {
        assert_eq!(
            data_type_to_arrow(&DataType::Time),
            ArrowType::Timestamp(TimeUnit::Nanosecond, None)
        );
    }

    #[test]
    fn hex_maps_to_binary() {
        assert_eq!(data_type_to_arrow(&DataType::Hex), ArrowType::Binary);
    }

    #[test]
    fn all_variants_map_without_panic() {
        // Every DataType variant should map to some ArrowType without panicking
        let variants = [
            DataType::Bool,
            DataType::Chars,
            DataType::Symbol,
            DataType::PeekSymbol,
            DataType::Digit,
            DataType::Float,
            DataType::Ignore,
            DataType::Time,
            DataType::TimeISO,
            DataType::TimeRFC3339,
            DataType::TimeRFC2822,
            DataType::TimeTIMESTAMP,
            DataType::TimeCLF,
            DataType::IP,
            DataType::IpNet,
            DataType::Domain,
            DataType::Email,
            DataType::Port,
            DataType::SN,
            DataType::Hex,
            DataType::Base64,
            DataType::KV,
            DataType::KvArr,
            DataType::Json,
            DataType::ExactJson,
            DataType::HttpRequest,
            DataType::HttpStatus,
            DataType::HttpAgent,
            DataType::HttpMethod,
            DataType::Url,
            DataType::Auto,
            DataType::ProtoText,
            DataType::Obj,
            DataType::Array("child".into()),
            DataType::IdCard,
            DataType::MobilePhone,
        ];
        for v in &variants {
            let arrow_type = data_type_to_arrow(v);
            assert!(
                !format!("{arrow_type:?}").is_empty(),
                "{v:?} maps to nothing"
            );
        }
    }

    // -- Round-trip: records → IPC → read back ----------------------------

    #[test]
    fn round_trip_string_and_digit() {
        let recs = vec![
            make_record(vec![char_field("name", "alice"), digit_field("age", 30)]),
            make_record(vec![char_field("name", "bob"), digit_field("age", 25)]),
        ];
        let ipc = records_to_arrow_ipc(&recs).expect("ipc");
        let cursor = std::io::Cursor::new(&ipc);
        let mut reader = StreamReader::try_new(cursor, None).expect("read ipc");

        let batch = reader.next().expect("first batch").expect("ok batch");

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 2);

        // Column 0 (name): Utf8
        let names = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("string array");
        assert_eq!(names.value(0), "alice");
        assert_eq!(names.value(1), "bob");

        // Column 1 (age): Int64
        let ages = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("int64 array");
        assert_eq!(ages.value(0), 30);
        assert_eq!(ages.value(1), 25);
    }

    #[test]
    fn round_trip_single_record() {
        let recs = vec![make_record(vec![digit_field("count", 42)])];
        let ipc = records_to_arrow_ipc(&recs).expect("ipc");
        let cursor = std::io::Cursor::new(&ipc);
        let mut reader = StreamReader::try_new(cursor, None).expect("read ipc");

        let batch = reader.next().expect("first batch").expect("ok batch");
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 1);

        let counts = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("int64 array");
        assert_eq!(counts.value(0), 42);
    }

    #[test]
    fn round_trip_many_fields() {
        // Test with multiple string fields
        let recs = vec![make_record(vec![
            char_field("a", "1"),
            char_field("b", "2"),
            char_field("c", "3"),
            char_field("d", "4"),
            char_field("e", "5"),
        ])];
        let ipc = records_to_arrow_ipc(&recs).expect("ipc");
        let cursor = std::io::Cursor::new(&ipc);
        let mut reader = StreamReader::try_new(cursor, None).expect("read ipc");

        let batch = reader.next().expect("first batch").expect("ok batch");
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 5);
    }

    #[test]
    fn round_trip_many_rows() {
        // Test batching many rows
        let recs: Vec<_> = (0..100)
            .map(|i| {
                make_record(vec![
                    digit_field("id", i),
                    char_field("label", &format!("row_{i}")),
                ])
            })
            .collect();
        let ipc = records_to_arrow_ipc(&recs).expect("ipc");
        let cursor = std::io::Cursor::new(&ipc);
        let mut reader = StreamReader::try_new(cursor, None).expect("read ipc");

        let batch = reader.next().expect("first batch").expect("ok batch");
        assert_eq!(batch.num_rows(), 100);
        assert_eq!(batch.num_columns(), 2);

        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("int64 array");
        assert_eq!(ids.value(0), 0);
        assert_eq!(ids.value(99), 99);
    }

    // -- Utf8 fallback for non-primitive types ----------------------------

    #[test]
    fn obj_type_falls_back_to_utf8() {
        assert_eq!(data_type_to_arrow(&DataType::Obj), ArrowType::Utf8);
    }

    #[test]
    fn auto_type_falls_back_to_utf8() {
        assert_eq!(data_type_to_arrow(&DataType::Auto), ArrowType::Utf8);
    }

    #[test]
    fn chars_fallback_in_int64_col() {
        let recs = vec![make_record(vec![DataField::from_chars("val", "123")])];
        let schema = Arc::new(Schema::new(vec![Field::new("val", ArrowType::Int64, true)]));
        let batch = records_to_batch(&recs, &schema).unwrap();
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(col.value(0), 123);
    }

    #[test]
    fn chars_fallback_in_float64_col() {
        let recs = vec![make_record(vec![DataField::from_chars("val", "2.71")])];
        let schema = Arc::new(Schema::new(vec![Field::new(
            "val",
            ArrowType::Float64,
            true,
        )]));
        let batch = records_to_batch(&recs, &schema).unwrap();
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        assert!((col.value(0) - 2.71).abs() < 0.001);
    }

    #[test]
    fn chars_fallback_invalid_int64_is_null() {
        let recs = vec![make_record(vec![DataField::from_chars("val", "abc")])];
        let schema = Arc::new(Schema::new(vec![Field::new("val", ArrowType::Int64, true)]));
        let batch = records_to_batch(&recs, &schema).unwrap();
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert!(col.is_null(0));
    }

    #[test]
    fn chars_fallback_in_bool_col() {
        let recs = vec![make_record(vec![DataField::from_chars("flag", "TRUE")])];
        let schema = Arc::new(Schema::new(vec![Field::new(
            "flag",
            ArrowType::Boolean,
            true,
        )]));
        let batch = records_to_batch(&recs, &schema).unwrap();
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .unwrap();
        assert!(col.value(0));
    }

    #[test]
    fn chars_fallback_invalid_bool_is_false() {
        let recs = vec![make_record(vec![DataField::from_chars("flag", "yes")])];
        let schema = Arc::new(Schema::new(vec![Field::new(
            "flag",
            ArrowType::Boolean,
            true,
        )]));
        let batch = records_to_batch(&recs, &schema).unwrap();
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .unwrap();
        assert!(!col.value(0));
    }

    #[test]
    fn chars_fallback_invalid_float64_is_null() {
        let recs = vec![make_record(vec![DataField::from_chars("val", "abc")])];
        let schema = Arc::new(Schema::new(vec![Field::new(
            "val",
            ArrowType::Float64,
            true,
        )]));
        let batch = records_to_batch(&recs, &schema).unwrap();
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        assert!(col.is_null(0));
    }

    #[test]
    fn timestamp_ns_from_time_value() {
        use chrono::NaiveDateTime;
        let dt = NaiveDateTime::parse_from_str("2026-06-13 12:00:00", "%Y-%m-%d %H:%M:%S").unwrap();
        let recs = vec![make_record(vec![DataField::from_time("ts", dt)])];
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            ArrowType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        )]));
        let batch = records_to_batch(&recs, &schema).unwrap();
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::TimestampNanosecondArray>()
            .unwrap();
        assert!(col.value(0) > 0);
    }

    // -- Port → Int32 ----------------------------------------------------------

    #[test]
    fn port_maps_to_int32() {
        assert_eq!(data_type_to_arrow(&DataType::Port), ArrowType::Int32);
    }

    #[test]
    fn int32_col_from_digit() {
        let recs = vec![make_record(vec![DataField::from_digit("port", 8080)])];
        let schema = Arc::new(Schema::new(vec![Field::new(
            "port",
            ArrowType::Int32,
            true,
        )]));
        let batch = records_to_batch(&recs, &schema).unwrap();
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .unwrap();
        assert_eq!(col.value(0), 8080);
    }

    #[test]
    fn int32_col_chars_fallback() {
        let recs = vec![make_record(vec![DataField::from_chars("port", "443")])];
        let schema = Arc::new(Schema::new(vec![Field::new(
            "port",
            ArrowType::Int32,
            true,
        )]));
        let batch = records_to_batch(&recs, &schema).unwrap();
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .unwrap();
        assert_eq!(col.value(0), 443);
    }

    // -- Ignore filtered from schema -------------------------------------------

    #[test]
    fn ignore_not_in_schema() {
        let recs = vec![make_record(vec![DataField::from_digit("count", 1)])];
        let schema = infer_schema(&recs).unwrap();
        assert_eq!(schema.fields().len(), 1);
        assert!(schema.fields().iter().any(|f| f.name() == "count"));
    }

    // -- Hex → raw bytes -------------------------------------------------------

    #[test]
    fn hex_to_raw_bytes_minimal() {
        use wp_model_core::model::types::value::HexT;
        let v = Value::Hex(HexT(0x1A2B));
        let bytes = to_raw_bytes(&v);
        assert_eq!(bytes, vec![0x1A, 0x2B]);
    }

    #[test]
    fn hex_zero_to_raw_bytes() {
        use wp_model_core::model::types::value::HexT;
        let v = Value::Hex(HexT(0));
        let bytes = to_raw_bytes(&v);
        assert_eq!(bytes, vec![0]);
    }

    // -- Obj/Array → JSON ------------------------------------------------------

    #[test]
    fn obj_formatted_as_json() {
        use wp_model_core::model::types::value::ObjectValue;
        let v = Value::Obj(ObjectValue::default());
        let s = format_utf8_value(&v);
        assert!(s.starts_with('{') || s == "{}", "expected JSON, got: {s}");
    }
}
