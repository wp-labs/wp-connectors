use rand::RngExt;
use wp_model_core::model::{DataField, DataRecord};

thread_local! {
    static RNG: std::cell::RefCell<rand::rngs::ThreadRng> = std::cell::RefCell::new(rand::rng());
}

#[allow(dead_code)]
pub fn create_sample_record(id: i64) -> DataRecord {
    let x: i64 = RNG.with(|rng| rng.borrow_mut().random());

    let mut record = DataRecord::default();
    record.append(DataField::from_int("wp_event_id", id));
    record.append(DataField::from_chars(
        "wp_src_key",
        format!("concurrent_test_{}", id),
    ));
    record.append(DataField::from_chars("sip", "192.168.1.100"));
    record.append(DataField::from_chars("timestamp", "2024-03-02 10:00:00"));
    record.append(DataField::from_chars(
        "http/request",
        format!("GET /api/test/{} HTTP/1.1", id),
    ));
    record.append(DataField::from_int("status", x));
    record.append(DataField::from_int("size", x));
    record.append(DataField::from_chars("referer", "000123"));
    record.append(DataField::from_chars(
        "http/agent",
        "Mozilla/5.0 (Concurrent Test)",
    ));
    record
}

#[allow(dead_code)]
fn main() {
    eprintln!("This is a utility module, not a runnable example");
}
