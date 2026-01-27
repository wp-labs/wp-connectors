use educe::Educe;
use serde::Deserialize;
use serde::Serialize;
#[derive(Educe, Deserialize, Serialize, PartialEq, Clone)]
#[educe(Debug, Default)]
pub struct VictoriaLog {
    #[educe(Default = "127.0.0.1:9428")]
    pub endpoint: String,
    #[educe(Default = "/insert/jsonline")]
    pub insert_path: String,
    pub create_time_field: Option<String>,
    /// 请求超时（秒），数据量大时可适当调高。
    #[educe(Default = 60.0)]
    pub request_timeout_secs: f64,
}
