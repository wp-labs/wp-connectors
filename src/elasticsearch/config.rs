use educe::Educe;
use serde::{Deserialize, Serialize};
use winnow::error::ModalResult;
use winnow::prelude::*;
use winnow::token::{literal, take_till, take_until};

#[derive(Educe, Deserialize, Serialize, PartialEq, Clone)]
#[educe(Debug, Default)]
pub struct Elasticsearch {
    #[educe(Default = "http://127.0.0.1:9200")]
    pub(crate) endpoint: String,
    #[educe(Default = "elastic")]
    pub username: String,
    #[educe(Default = "wparse")]
    pub password: String,
    // 批量插入数据到elasticsearch的数据条数
    pub batch: Option<usize>,
    pub table: Option<String>,
}

impl Elasticsearch {
    pub fn get_endpoint(&self) -> String {
        if let Ok(endpoint) = std::env::var("ES_ENDPOINT") {
            endpoint
        } else {
            self.endpoint.clone()
        }
    }
    pub fn get_database_url(&self) -> String {
        let endpoint = self.get_endpoint();
        // Accept both http/https, and avoid panic on unexpected prefix
        let endpoint = endpoint
            .strip_prefix("http://")
            .or_else(|| endpoint.strip_prefix("https://"))
            .unwrap_or(endpoint.as_str());
        format!(
            "elasticsearch://{}:{}@{}",
            self.username, self.password, endpoint
        )
    }

    // elasticsearch://root:dayu@localhost:9200
    pub fn parse_elasticsearch_connect(input: &mut &str) -> ModalResult<Elasticsearch> {
        literal("elasticsearch://").parse_next(input)?;
        let username: &str = take_until(0.., ":").parse_next(input)?;
        literal(":").parse_next(input)?;
        let password: &str = take_until(0.., "@").parse_next(input)?;
        literal("@").parse_next(input)?;
        let address: &str = take_till(0.., |c: char| c == '/').parse_next(input)?;

        Ok(Elasticsearch {
            endpoint: format!("http://{}", address),
            username: username.to_string(),
            password: password.to_string(),
            batch: None,
            table: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orion_error::TestAssert;

    #[test]
    fn test_es_url() {
        let url = "elasticsearch://root:dayu@localhost:9200";
        let mut s = url;
        let ck = Elasticsearch::parse_elasticsearch_connect(&mut s).unwrap();
        assert_eq!(s, "");
        assert_eq!(ck.endpoint, "http://localhost:9200");
    }
}
