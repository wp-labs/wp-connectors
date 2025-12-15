use educe::Educe;
use serde::{Deserialize, Serialize};
use winnow::error::ModalResult;
use winnow::prelude::*;
use winnow::token::{literal, take_till, take_until};

/// 与 `structure::io::Mysql` 等价的配置结构，集中到 mysql/ 目录便于后续抽离。
#[derive(Educe, Deserialize, Serialize, PartialEq, Clone)]
#[educe(Debug, Default)]
pub struct MysqlConf {
    /// 形如 `host:port` 的地址
    #[educe(Default = "localhost:3306")]
    pub endpoint: String,
    #[educe(Default = "root")]
    pub username: String,
    #[educe(Default = "dayu")]
    pub password: String,
    #[educe(Default = "wparse")]
    pub database: String,
    pub table: Option<String>,
    /// 批量写入的条数（可选）
    pub batch: Option<usize>,
}

impl MysqlConf {
    /// 优先读取环境变量 `MYSQL_URL`，否则拼接配置字段。
    /// mysql://root:root@localhost:3306/database
    pub fn get_database_url(&self) -> String {
        if let Ok(url) = std::env::var("MYSQL_URL") {
            url
        } else {
            format!(
                "mysql://{}:{}@{}/{}",
                self.username, self.password, self.endpoint, self.database
            )
        }
    }

    /// 从标准连接串解析 MySQL 配置。
    /// 支持：mysql://<user>:<pass>@<host[:port]>/<db>[?params]
    pub fn parse_mysql_connect(input: &mut &str) -> ModalResult<MysqlConf> {
        // 解析形如：mysql://user:pass@host[:port]/database[?params]
        literal("mysql://").parse_next(input)?;
        let username: &str = take_until(0.., ":").parse_next(input)?;
        literal(":").parse_next(input)?;
        let password: &str = take_until(0.., "@").parse_next(input)?;
        literal("@").parse_next(input)?;
        let address: &str = take_until(0.., "/").parse_next(input)?;
        literal("/").parse_next(input)?;
        // 数据库名读到 `?`（若无参数则读到结尾）
        let database: &str = take_till(0.., |c: char| c == '?').parse_next(input)?;

        Ok(MysqlConf {
            endpoint: address.to_string(),
            username: username.to_string(),
            password: password.to_string(),
            database: database.to_string(),
            table: None,
            batch: None,
        })
    }

    /// 便捷封装：从 url 解析并返回结构体。
    pub fn from_url(url: &str) -> anyhow::Result<Self> {
        let mut s = url;
        Self::parse_mysql_connect(&mut s)
            .map_err(|e| anyhow::anyhow!("parse mysql url failed: {:?}", e))
    }

    /// 将配置转换为扁平的 toml::Table，便于写入 `SinkInstanceConf.core.params`。
    pub fn to_params_table(&self) -> toml::value::Table {
        let mut m = toml::map::Map::new();
        m.insert(
            "endpoint".to_string(),
            toml::Value::String(self.endpoint.clone()),
        );
        m.insert(
            "username".to_string(),
            toml::Value::String(self.username.clone()),
        );
        m.insert(
            "password".to_string(),
            toml::Value::String(self.password.clone()),
        );
        m.insert(
            "database".to_string(),
            toml::Value::String(self.database.clone()),
        );
        if let Some(t) = &self.table {
            m.insert("table".to_string(), toml::Value::String(t.clone()));
        }
        if let Some(b) = self.batch {
            m.insert("batch".to_string(), toml::Value::Integer(b as i64));
        }
        m
    }
}

#[cfg(test)]
mod tests {
    use super::MysqlConf;

    #[test]
    fn test_parse_mysql_url() {
        let url = "mysql://root:dayu@localhost:3306/wparse";
        let mut s = url;
        let cfg = MysqlConf::parse_mysql_connect(&mut s).unwrap();
        assert_eq!(s, "");
        assert_eq!(cfg.endpoint, "localhost:3306");
        assert_eq!(cfg.username, "root");
        assert_eq!(cfg.password, "dayu");
        assert_eq!(cfg.database, "wparse");
    }
}
