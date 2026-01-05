use educe::Educe;
use serde::Deserialize;
use serde::Serialize;
#[derive(Educe, Deserialize, Serialize, PartialEq, Clone)]
#[educe(Debug, Default)]
pub struct VictoriaMetric {
    #[educe(Default = "http://0.0.0.0:9090/insert/0/prometheus/api/v1/import/prometheus")]
    pub insert_url: String,
    #[educe(Default = 0.1)]
    pub flush_interval_secs: f64,
}
