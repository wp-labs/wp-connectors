use educe::Educe;
use serde::Deserialize;
use serde::Serialize;

/// Prometheus configuration for metrics
/// Note: The regex capture group names in default values correspond to tag constants
/// (e.g., "access_source" corresponds to crate::ACCESS_SOURCE_TAG)
#[derive(Educe, Deserialize, Serialize, PartialEq, Clone)]
#[educe(Debug, Default)]
pub struct Prometheus {
    #[educe(Default = "0.0.0.0:9090")]
    pub endpoint: String,
    #[educe(Default = "(?P<source_type>.*)_(?P<access_source>.*)")]
    pub source_key_format: String,
    #[educe(Default = "(?P<rule>.*)_(?P<sink_type>.*)_sink")]
    pub sink_key_format: String,
}
