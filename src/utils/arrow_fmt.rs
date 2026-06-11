//! Arrow IPC serialization for sink `protocol: arrow`.
//!
//! Converts `Vec<Arc<DataRecord>>` → `RecordBatch` → Arrow IPC Stream bytes.
//! Gated behind feature `"wf"` (which enables `dep:arrow`).

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder,
};
use arrow::datatypes::{DataType as ArrowType, Field, Schema};
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
        .map(|f| {
            let name = f.get_name().to_string();
            let arrow_type = data_type_to_arrow(f.get_meta());
            let nullable = matches!(f.get_meta(), DataType::Ignore);
            Field::new(name, arrow_type, nullable)
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
        // Time → Int64 (Unix millis)
        DataType::Time
        | DataType::TimeISO
        | DataType::TimeRFC3339
        | DataType::TimeRFC2822
        | DataType::TimeTIMESTAMP
        | DataType::TimeCLF => ArrowType::Int64,
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
        | DataType::Port
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
        // Ignore → Utf8 (nullable; caller should filter)
        DataType::Ignore => ArrowType::Utf8,
        // Nested types → Utf8 (JSON representation)
        DataType::Obj | DataType::Array(_) | DataType::KvArr => ArrowType::Utf8,
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
        ArrowType::Int64 => build_int64_col(records, field, col_idx),
        ArrowType::Float64 => build_float64_col(records, field, col_idx),
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
            _ if field.is_nullable() => builder.append_null(),
            _ => builder.append_value(false),
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
            _ if field.is_nullable() => builder.append_null(),
            _ => builder.append_value(0.0),
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
                let s = format_value(v);
                builder.append_value(s.as_bytes());
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
            Some(v) => builder.append_value(format_value(v)),
            _ if field.is_nullable() => builder.append_null(),
            _ => builder.append_value(""),
        }
    }
    Ok(Arc::new(builder.finish()))
}

// -- Value formatting (fallback) --------------------------------------------

fn format_value(v: &Value) -> String {
    match v {
        Value::Null => String::new(),
        Value::Bool(b) => b.to_string(),
        Value::Chars(s) => s.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Digit(d) => d.to_string(),
        Value::Time(dt) => dt.and_utc().timestamp_millis().to_string(),
        Value::Symbol(s) => s.to_string(),
        Value::Obj(o) => format!("{o:?}"),
        Value::Array(_) => String::from("[array]"),
        Value::IpAddr(ip) => ip.to_string(),
        Value::IpNet(net) => net.to_string(),
        Value::Domain(d) => d.to_string(),
        Value::Url(u) => u.to_string(),
        Value::Email(e) => e.to_string(),
        Value::IdCard(id) => id.to_string(),
        Value::MobilePhone(m) => m.to_string(),
        Value::Hex(h) => h.to_string(),
        Value::Ignore(_) => String::new(),
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
        assert!(!ipc.is_empty(), "even empty schema should produce IPC bytes");

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
            make_record(vec![
                char_field("name", "alice"),
                digit_field("age", 30),
            ]),
            make_record(vec![
                char_field("name", "bob"),
                digit_field("age", 25),
            ]),
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
    fn time_maps_to_int64() {
        assert_eq!(data_type_to_arrow(&DataType::Time), ArrowType::Int64);
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
            assert!(!format!("{arrow_type:?}").is_empty(), "{v:?} maps to nothing");
        }
    }

    // -- Round-trip: records → IPC → read back ----------------------------

    #[test]
    fn round_trip_string_and_digit() {
        let recs = vec![
            make_record(vec![
                char_field("name", "alice"),
                digit_field("age", 30),
            ]),
            make_record(vec![
                char_field("name", "bob"),
                digit_field("age", 25),
            ]),
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
        let recs = vec![make_record(vec![
            digit_field("count", 42),
        ])];
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
}
