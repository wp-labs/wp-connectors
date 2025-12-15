use educe::Educe;
use serde::{Deserialize, Serialize};
use winnow::error::ModalResult;
use winnow::prelude::*;
use winnow::token::{literal, take_till, take_until};

#[derive(Educe, Deserialize, Serialize, PartialEq, Clone)]
#[educe(Debug, Default)]
pub struct Clickhouse {
    #[educe(Default = "http://127.0.0.1:8123")]
    pub(crate) endpoint: String,
    #[educe(Default = "dayu")]
    pub username: String,
    #[educe(Default = "wparse")]
    pub password: String,
    #[educe(Default = "wparse")]
    pub database: String,
    // 批量插入数据到clickhouse的数据条数
    pub batch: Option<usize>,
    pub table: Option<String>,

    #[educe(Default = true)]
    pub skip_unknown: bool,
    #[educe(Default = true)]
    pub date_time_best_effort: bool,
}

impl Clickhouse {
    pub fn get_endpoint(&self) -> String {
        if let Ok(url) = std::env::var("CLICKHOUSE_ENDPOINT") {
            url
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
            "clickhouse://{}:{}@{}/{}",
            self.username, self.password, endpoint, self.database
        )
    }

    // clickhouse://root:dayu@localhost:8123/wparse
    pub fn parse_clickhouse_connect(input: &mut &str) -> ModalResult<Clickhouse> {
        literal("clickhouse://").parse_next(input)?;
        let username: &str = take_until(0.., ":").parse_next(input)?;
        literal(":").parse_next(input)?;
        let password: &str = take_until(0.., "@").parse_next(input)?;
        literal("@").parse_next(input)?;
        let address: &str = take_until(0.., "/").parse_next(input)?;
        literal("/").parse_next(input)?;
        let database: &str = take_till(0.., |c: char| c == '?').parse_next(input)?;

        Ok(Clickhouse {
            endpoint: format!("http://{}", address),
            username: username.to_string(),
            password: password.to_string(),
            database: database.to_string(),
            batch: None,
            table: None,
            skip_unknown: false,
            date_time_best_effort: false,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orion_error::TestAssert;

    #[test]
    fn test_clickhouse_url() {
        let url = "clickhouse://root:dayu@localhost:8123/wparse";
        let mut s = url;
        let ck = Clickhouse::parse_clickhouse_connect(&mut s).unwrap();
        assert_eq!(s, "");
        assert_eq!(ck.endpoint, "http://localhost:8123");
        assert_eq!(ck.database, "wparse");
    }
}
