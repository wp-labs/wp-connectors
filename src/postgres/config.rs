use educe::Educe;
use serde::{Deserialize, Serialize};
use winnow::error::ModalResult;
use winnow::prelude::*;
use winnow::token::{literal, take_till, take_until};

#[derive(Educe, Deserialize, Serialize, PartialEq, Clone)]
#[educe(Debug, Default)]
pub struct PostgresConf {
    /// 形如 `host:port` 的地址
    #[educe(Default = "localhost:5432")]
    pub endpoint: String,
    #[educe(Default = "root")]
    pub username: String,
    #[educe(Default = "dayu")]
    pub password: String,
    #[educe(Default = "wparse")]
    pub database: String,
    #[educe(Default = "public")]
    pub schema: String,
    pub table: Option<String>,
    /// 批量写入的条数（可选）
    pub batch: Option<usize>,
    /// source 使用的增量游标列
    pub cursor_column: Option<String>,
    /// 游标类型，支持 `int` / `time`
    #[educe(Default = "\"int\".to_string()")]
    pub cursor_type: String,
    /// 首次启动且无 checkpoint 时的起点
    pub start_from: Option<String>,
    /// start_from 的输入格式，仅 time 游标使用
    pub start_from_format: Option<String>,
    /// 空轮询间隔（毫秒）
    pub poll_interval_ms: Option<u64>,
    /// 查询失败后的退避间隔（毫秒）
    pub error_backoff_ms: Option<u64>,
}

impl PostgresConf {
    /// 优先读取环境变量 `POSTGRES_URL`，否则拼接配置字段。
    /// postgres://root:root@localhost:3306/database
    pub fn get_database_url(&self) -> String {
        if let Ok(url) = std::env::var("POSTGRES_URL") {
            url
        } else {
            format!(
                "postgres://{}:{}@{}/{}",
                self.username, self.password, self.endpoint, self.database
            )
        }
    }

    /// 从标准连接串解析 Postgres 配置。
    /// 支持：postgres://<user>:<pass>@<host[:port]>/<db>[?params]
    pub fn parse_postgres_connect(input: &mut &str) -> ModalResult<PostgresConf> {
        // 解析形如：postgres://user:pass@host[:port]/database[?params]
        literal("postgres://").parse_next(input)?;
        let username: &str = take_until(0.., ":").parse_next(input)?;
        literal(":").parse_next(input)?;
        let password: &str = take_until(0.., "@").parse_next(input)?;
        literal("@").parse_next(input)?;
        let address: &str = take_until(0.., "/").parse_next(input)?;
        literal("/").parse_next(input)?;
        // 数据库名读到 `?`（若无参数则读到结尾）
        let database: &str = take_till(0.., |c: char| c == '?').parse_next(input)?;

        Ok(PostgresConf {
            endpoint: address.to_string(),
            username: username.to_string(),
            password: password.to_string(),
            database: database.to_string(),
            schema: "public".to_string(),
            table: None,
            batch: None,
            cursor_column: None,
            cursor_type: "int".to_string(),
            start_from: None,
            start_from_format: None,
            poll_interval_ms: None,
            error_backoff_ms: None,
        })
    }

    /// 便捷封装：从 url 解析并返回结构体。
    pub fn from_url(url: &str) -> anyhow::Result<Self> {
        let mut s = url;
        Self::parse_postgres_connect(&mut s)
            .map_err(|e| anyhow::anyhow!("parse postgres url failed: {:?}", e))
    }
}

#[cfg(test)]
mod tests {
    use super::PostgresConf;

    #[test]
    fn test_parse_postgres_url() {
        let url = "postgres://root:dayu@localhost:3306/wparse";
        let mut s = url;
        let cfg = PostgresConf::parse_postgres_connect(&mut s).unwrap();
        assert_eq!(s, "");
        assert_eq!(cfg.endpoint, "localhost:3306");
        assert_eq!(cfg.username, "root");
        assert_eq!(cfg.password, "dayu");
        assert_eq!(cfg.database, "wparse");
    }
}
