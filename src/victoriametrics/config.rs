use educe::Educe;
use serde::Deserialize;
use serde::Serialize;
#[derive(Educe, Deserialize, Serialize, PartialEq, Clone)]
#[educe(Debug, Default)]
pub struct VictoriaMetric {
    #[educe(Default = "http://127.0.0.1:8428")]
    pub endpoint: String,
    #[educe(Default = "/api/v1/import/prometheus")]
    pub api_path: String,
    #[educe(Default = 1.0)]
    pub flush_secs: f64,
    #[educe(Default = 5.0)]
    pub timeout_secs: f64,
}
