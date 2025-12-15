use educe::Educe;
use serde::Deserialize;
use serde::Serialize;
#[derive(Educe, Deserialize, Serialize, PartialEq, Clone)]
#[educe(Debug, Default)]
pub struct VictoriaMetric {
    #[educe(Default = "0.0.0.0:9090")]
    pub endpoint: String,
    #[educe(Default = "/insert/0/prometheus/api/v1/import/prometheus")]
    pub insert_path: String,
    #[educe(Default = 0.1)]
    pub flush_interval_secs: f64,
}
