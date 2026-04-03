use std::sync::Arc;

use wp_data_fmt::{
    Csv, FormatType, Json, KeyValue, ProtoTxt, Raw, RecordFormatter, ValueFormatter,
};
use wp_model_core::model::{DataField, DataRecord, DataType};

pub enum BatchFormat {
    Json,
    Ndjson,
    Csv(Csv),
    Kv(KeyValue),
    Raw,
    ProtoText,
}

impl BatchFormat {
    fn into_format_type(self) -> FormatType {
        match self {
            Self::Json | Self::Ndjson => FormatType::Json(Json),
            Self::Csv(fmt) => FormatType::Csv(fmt),
            Self::Kv(fmt) => FormatType::Kv(fmt),
            Self::Raw => FormatType::Raw(Raw),
            Self::ProtoText => FormatType::ProtoText(ProtoTxt),
        }
    }
}

fn build_csv_header_record(record: &DataRecord) -> DataRecord {
    let mut header = DataRecord::default();

    for field in record
        .items
        .iter()
        .filter(|field| *field.get_meta() != DataType::Ignore)
    {
        header.append(DataField::from_chars(field.get_name(), field.get_name()));
    }

    header
}

pub fn fmt_strs(records: Vec<Arc<DataRecord>>, format: BatchFormat) -> String {
    match format {
        BatchFormat::Json => fmt_str_json(records),
        BatchFormat::Ndjson => fmt_str_ndjson(records),
        BatchFormat::Csv(fmt) => fmt_str_csv(records, fmt),
        BatchFormat::Kv(fmt) => fmt_str_kv(records, fmt),
        BatchFormat::Raw => fmt_str_raw(records),
        BatchFormat::ProtoText => fmt_str_proto_text(records),
    }
}

pub fn fmt_bytes(records: Vec<Arc<DataRecord>>, format: BatchFormat) -> Vec<u8> {
    match format {
        BatchFormat::Json => fmt_bytes_json(records),
        BatchFormat::Ndjson => fmt_bytes_ndjson(records),
        BatchFormat::Csv(fmt) => fmt_bytes_csv(records, fmt),
        BatchFormat::Kv(fmt) => fmt_bytes_kv(records, fmt),
        BatchFormat::Raw => fmt_bytes_raw(records),
        BatchFormat::ProtoText => fmt_bytes_proto_text(records),
    }
}

pub fn fmt_str_json(records: Vec<Arc<DataRecord>>) -> String {
    let formatter = BatchFormat::Json.into_format_type();
    let mut buffer = String::from("[");

    for (idx, record) in records.iter().enumerate() {
        if idx > 0 {
            buffer.push(',');
        }
        buffer.push_str(&formatter.fmt_record(record.as_ref()));
    }

    buffer.push(']');
    buffer
}

pub fn fmt_str_ndjson(records: Vec<Arc<DataRecord>>) -> String {
    let formatter = BatchFormat::Ndjson.into_format_type();
    let mut buffer = String::new();

    for (idx, record) in records.iter().enumerate() {
        if idx > 0 {
            buffer.push('\n');
        }
        buffer.push_str(&formatter.fmt_record(record.as_ref()));
    }

    buffer
}

pub fn fmt_str_csv(records: Vec<Arc<DataRecord>>, format: Csv) -> String {
    if records.is_empty() {
        return String::new();
    }

    let formatter = FormatType::Csv(format);
    let mut buffer = String::new();
    let header = build_csv_header_record(records[0].as_ref());
    buffer.push_str(&formatter.fmt_record(&header));

    for record in records.iter() {
        buffer.push('\n');
        buffer.push_str(&formatter.fmt_record(record.as_ref()));
    }

    buffer
}

pub fn fmt_str_kv(records: Vec<Arc<DataRecord>>, format: KeyValue) -> String {
    let formatter = BatchFormat::Kv(format).into_format_type();
    let mut buffer = String::new();

    for (idx, record) in records.iter().enumerate() {
        if idx > 0 {
            buffer.push('\n');
        }
        buffer.push_str(&formatter.fmt_record(record.as_ref()));
    }

    buffer
}

pub fn fmt_str_kv_http(records: Vec<Arc<DataRecord>>) -> String {
    let json = Json;
    let mut buffer = String::new();

    for (record_idx, record) in records.iter().enumerate() {
        if record_idx > 0 {
            buffer.push('\n');
        }

        let mut first = true;
        for field in record
            .items
            .iter()
            .filter(|f| *f.get_meta() != DataType::Ignore)
        {
            if !first {
                buffer.push(' ');
            }
            first = false;
            buffer.push_str(field.get_name());
            buffer.push('=');
            buffer.push_str(&json.format_value(field.get_value()));
        }
    }

    buffer
}

pub fn fmt_str_raw(records: Vec<Arc<DataRecord>>) -> String {
    let formatter = BatchFormat::Raw.into_format_type();
    let mut buffer = String::new();

    for (idx, record) in records.iter().enumerate() {
        if idx > 0 {
            buffer.push('\n');
        }
        buffer.push_str(&formatter.fmt_record(record.as_ref()));
    }

    buffer
}

pub fn fmt_str_proto_text(records: Vec<Arc<DataRecord>>) -> String {
    let formatter = BatchFormat::ProtoText.into_format_type();
    let mut buffer = String::new();

    for (idx, record) in records.iter().enumerate() {
        if idx > 0 {
            buffer.push_str("\n\n");
        }
        buffer.push_str(&formatter.fmt_record(record.as_ref()));
    }

    buffer
}

pub fn fmt_bytes_json(records: Vec<Arc<DataRecord>>) -> Vec<u8> {
    let formatter = BatchFormat::Json.into_format_type();
    let mut buffer = Vec::new();
    buffer.push(b'[');

    for (idx, record) in records.iter().enumerate() {
        if idx > 0 {
            buffer.push(b',');
        }
        buffer.extend_from_slice(formatter.fmt_record(record.as_ref()).as_bytes());
    }

    buffer.push(b']');
    buffer
}

pub fn fmt_bytes_ndjson(records: Vec<Arc<DataRecord>>) -> Vec<u8> {
    let formatter = BatchFormat::Ndjson.into_format_type();
    let mut buffer = Vec::new();

    for (idx, record) in records.iter().enumerate() {
        if idx > 0 {
            buffer.push(b'\n');
        }
        buffer.extend_from_slice(formatter.fmt_record(record.as_ref()).as_bytes());
    }

    buffer
}

pub fn fmt_bytes_csv(records: Vec<Arc<DataRecord>>, format: Csv) -> Vec<u8> {
    fmt_str_csv(records, format).into_bytes()
}

pub fn fmt_bytes_kv(records: Vec<Arc<DataRecord>>, format: KeyValue) -> Vec<u8> {
    let formatter = BatchFormat::Kv(format).into_format_type();
    let mut buffer = Vec::new();

    for (idx, record) in records.iter().enumerate() {
        if idx > 0 {
            buffer.push(b'\n');
        }
        buffer.extend_from_slice(formatter.fmt_record(record.as_ref()).as_bytes());
    }

    buffer
}

pub fn fmt_bytes_kv_http(records: Vec<Arc<DataRecord>>) -> Vec<u8> {
    fmt_str_kv_http(records).into_bytes()
}

pub fn fmt_bytes_raw(records: Vec<Arc<DataRecord>>) -> Vec<u8> {
    let formatter = BatchFormat::Raw.into_format_type();
    let mut buffer = Vec::new();

    for (idx, record) in records.iter().enumerate() {
        if idx > 0 {
            buffer.push(b'\n');
        }
        buffer.extend_from_slice(formatter.fmt_record(record.as_ref()).as_bytes());
    }

    buffer
}

pub fn fmt_bytes_proto_text(records: Vec<Arc<DataRecord>>) -> Vec<u8> {
    let formatter = BatchFormat::ProtoText.into_format_type();
    let mut buffer = Vec::new();

    for (idx, record) in records.iter().enumerate() {
        if idx > 0 {
            buffer.extend_from_slice(b"\n\n");
        }
        buffer.extend_from_slice(formatter.fmt_record(record.as_ref()).as_bytes());
    }

    buffer
}

#[cfg(test)]
mod tests {
    use super::*;
    use wp_model_core::model::{DataField, DataRecord, types::value::ObjectValue};

    fn sample_record(id: i64, name: &str) -> Arc<DataRecord> {
        let mut record = DataRecord::default();
        record.append(DataField::from_digit("id", id));
        record.append(DataField::from_chars("name", name));
        Arc::new(record)
    }

    #[test]
    fn fmt_json_batch_as_array() {
        let rendered = fmt_strs(
            vec![sample_record(1, "alice"), sample_record(2, "bob")],
            BatchFormat::Json,
        );

        assert_eq!(
            rendered,
            r#"[{"id":1,"name":"alice"},{"id":2,"name":"bob"}]"#
        );
    }

    #[test]
    fn fmt_ndjson_batch_as_lines() {
        let rendered = fmt_strs(
            vec![sample_record(1, "alice"), sample_record(2, "bob")],
            BatchFormat::Ndjson,
        );

        assert_eq!(
            rendered,
            "{\"id\":1,\"name\":\"alice\"}\n{\"id\":2,\"name\":\"bob\"}"
        );
    }

    #[test]
    fn fmt_csv_batch_as_lines() {
        let rendered = fmt_strs(
            vec![sample_record(1, "alice"), sample_record(2, "bob")],
            BatchFormat::Csv(Csv::default()),
        );

        assert_eq!(rendered, "id,name\n1,alice\n2,bob");
    }

    #[test]
    fn fmt_bytes_returns_utf8_bytes() {
        let rendered = fmt_bytes(
            vec![sample_record(1, "alice")],
            BatchFormat::Kv(KeyValue::default()),
        );

        assert_eq!(
            String::from_utf8(rendered).unwrap(),
            "id: 1, name: \"alice\""
        );
    }

    #[test]
    fn fmt_proto_text_uses_blank_line_separator() {
        let rendered = fmt_strs(
            vec![sample_record(1, "alice"), sample_record(2, "bob")],
            BatchFormat::ProtoText,
        );

        assert!(rendered.contains("\n\n"));
    }
    #[test]
    fn fmt_json() {
        let rec = create_batch(3);
        let res = fmt_strs(rec, BatchFormat::Ndjson);
        println!("{}", res);
    }

    #[test]
    fn fmt_csv() {
        let rec = create_batch(3);
        let res = fmt_strs(rec, BatchFormat::Kv(KeyValue::default()));
        println!("{}", res);
    }

    fn create_batch(cnt: u8) -> Vec<Arc<DataRecord>> {
        (0..cnt)
            .map(|_i| {
                let mut e = DataRecord::default();
                let child_arr = DataField::from_arr(
                    "child_arr",
                    vec![
                        DataField::from_chars("c_a_k", "c_a_v"),
                        DataField::from_chars("c_b_k", "c_b_v"),
                    ],
                );
                let parent_arr = DataField::from_arr(
                    "parent_arr",
                    vec![
                        DataField::from_chars("p_a_k", "p_a_v"),
                        DataField::from_chars("p_a_k", "p_a_v"),
                        // DataField::from_digit("p_b_k",18),
                    ],
                );
                let mut o = ObjectValue::new();
                o.insert("o", DataField::from_chars("oc_k", "oc_v"));
                o.insert("2_arr", child_arr);

                e.append(DataField::from_chars("p_a_k", "p_a_v"));
                e.append(DataField::from_obj("obj", o));
                e.append(parent_arr);
                Arc::new(e)
            })
            .collect()
    }
}
