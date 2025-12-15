use educe::Educe;
use serde::Deserialize;
use serde::Serialize;
#[derive(Educe, Deserialize, Serialize, PartialEq, Clone)]
#[educe(Debug, Default)]
pub struct VictoriaLog {
    #[educe(Default = "127.0.0.1:9428")]
    pub endpoint: String,
    #[educe(Default = "/insert/loki/api/v1/push")]
    pub insert_path: String,
    #[educe(Default = 0.1)]
    pub flush_interval_secs: f64,
}
