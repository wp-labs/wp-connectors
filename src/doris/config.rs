use educe::Educe;
use serde::{Deserialize, Serialize};

/// Configuration for building a [`DorisSink`](crate::doris::DorisSink).
#[derive(Educe, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[educe(Debug, Default)]
pub struct DorisSinkConfig {
    #[educe(Default = "localhost:9030")]
    pub endpoint: String,
    #[educe(Default = "dayu")]
    pub database: String,
    #[educe(Default = "root")]
    pub user: String,
    #[educe(Default = "")]
    pub password: String,
    #[educe(Default = "wparse")]
    pub table: String,
    pub create_table: Option<String>,
    #[educe(Default = 1)]
    pub pool_size: u32,
    #[educe(Default = 32)]
    pub batch_size: usize,
}

impl DorisSinkConfig {
    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = endpoint.into();
        self
    }

    pub fn with_database(mut self, database: impl Into<String>) -> Self {
        self.database = database.into();
        self
    }

    pub fn with_user(mut self, user: impl Into<String>) -> Self {
        self.user = user.into();
        self
    }

    pub fn with_password(mut self, password: impl Into<String>) -> Self {
        self.password = password.into();
        self
    }

    pub fn with_table(mut self, table: impl Into<String>) -> Self {
        self.table = table.into();
        self
    }

    /* ---------- Option field ---------- */

    pub fn with_create_table(mut self, sql: impl Into<String>) -> Self {
        let sql = sql.into();
        let trimmed = sql.trim();

        self.create_table = if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        };

        self
    }

    /* ---------- numeric fields ---------- */
    pub fn with_pool_size(mut self, pool_size: u32) -> Self {
        self.pool_size = pool_size.max(1);
        self
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size.max(1);
        self
    }

    /// 返回带数据库后缀的连接串。
    ///
    /// # 参数
    /// * `self` - 使用当前配置中的 endpoint/database。
    ///
    /// # 返回
    /// * `String` - 形如 `<endpoint>/<database>[?params]` 的 DSN。
    pub fn database_dsn(&self) -> String {
        let (base, query) = split_query(&self.endpoint);
        let mut base = base.trim_end_matches('/').to_string();
        if base.is_empty() {
            base = self.endpoint.clone();
        }
        let mut dsn = format!("{}/{}", base, self.database);
        if let Some(q) = query {
            dsn.push('?');
            dsn.push_str(q);
        }
        dsn
    }
}

/// 将 endpoint 拆分为主体和查询参数。
///
/// # 参数
/// * `input` - 可能包含 `?` 参数的原始 endpoint。
///
/// # 返回
/// `(base, query)`，若不存在参数则第二项为 `None`。
fn split_query(input: &str) -> (&str, Option<&str>) {
    match input.split_once('?') {
        Some((left, right)) => (left, Some(right)),
        None => (input, None),
    }
}

#[cfg(test)]
mod tests {
    use super::DorisSinkConfig;

    // #[test]
    // fn config_defaults() {
    //     let cfg = DorisSinkConfig::new(
    //         "mysql://localhost:9030".into(),
    //         "demo".into(),
    //         "root".into(),
    //         "".into(),
    //         "events".into(),
    //         None,
    //         None,
    //         None,
    //     );
    //     assert_eq!(cfg.pool_size, DorisSinkConfig::default_pool_size());
    //     assert_eq!(cfg.batch_size, DorisSinkConfig::default_batch_size());
    //     assert_eq!(cfg.table, "events");
    //     assert_eq!(cfg.database, "demo");
    //     assert_eq!(
    //         cfg.database_dsn(),
    //         "mysql://localhost:9030/demo".to_string()
    //     );
    //     assert_eq!(cfg.create_table, None);
    // }

    // #[test]
    // fn config_keeps_query_string() {
    //     let cfg = DorisSinkConfig::new(
    //         "mysql://localhost:9030?charset=utf8".into(),
    //         "demo".into(),
    //         "root".into(),
    //         "".into(),
    //         "events".into(),
    //         None,
    //         Some(2),
    //         Some(10),
    //     );
    //     assert_eq!(
    //         cfg.database_dsn(),
    //         "mysql://localhost:9030/demo?charset=utf8"
    //     );
    //     assert_eq!(cfg.pool_size, 2);
    //     assert_eq!(cfg.batch_size, 10);
    // }
}
