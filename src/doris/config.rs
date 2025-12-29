use serde::{Deserialize, Serialize};

const DEFAULT_POOL_SIZE: u32 = 4;
const DEFAULT_BATCH_SIZE: usize = 64;

/// Configuration for building a [`DorisSink`](crate::doris::DorisSink).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DorisSinkConfig {
    pub endpoint: String,
    pub database: String,
    pub user: String,
    pub password: String,
    pub table: String,
    pub create_table: Option<String>,
    pub pool_size: u32,
    pub batch_size: usize,
}

impl DorisSinkConfig {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        endpoint: String,
        database: String,
        user: String,
        password: String,
        table: String,
        create_table: Option<String>,
        pool_size: Option<u32>,
        batch_size: Option<usize>,
    ) -> Self {
        let pool_size = pool_size.unwrap_or(Self::default_pool_size()).max(1);
        let batch_size = batch_size.unwrap_or(Self::default_batch_size()).max(1);
        Self {
            endpoint,
            database,
            user,
            password,
            table,
            create_table: create_table.and_then(|s| {
                let trimmed = s.trim().to_string();
                (!trimmed.is_empty()).then_some(trimmed)
            }),
            pool_size,
            batch_size,
        }
    }

    pub fn default_pool_size() -> u32 {
        DEFAULT_POOL_SIZE
    }

    pub fn default_batch_size() -> usize {
        DEFAULT_BATCH_SIZE
    }

    /// Returns a DSN that points to the configured database.
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

fn split_query(input: &str) -> (&str, Option<&str>) {
    match input.split_once('?') {
        Some((left, right)) => (left, Some(right)),
        None => (input, None),
    }
}

#[cfg(test)]
mod tests {
    use super::DorisSinkConfig;

    #[test]
    fn config_defaults() {
        let cfg = DorisSinkConfig::new(
            "mysql://localhost:9030".into(),
            "demo".into(),
            "root".into(),
            "".into(),
            "events".into(),
            None,
            None,
            None,
        );
        assert_eq!(cfg.pool_size, DorisSinkConfig::default_pool_size());
        assert_eq!(cfg.batch_size, DorisSinkConfig::default_batch_size());
        assert_eq!(cfg.table, "events");
        assert_eq!(cfg.database, "demo");
        assert_eq!(
            cfg.database_dsn(),
            "mysql://localhost:9030/demo".to_string()
        );
        assert_eq!(cfg.create_table, None);
    }

    #[test]
    fn config_keeps_query_string() {
        let cfg = DorisSinkConfig::new(
            "mysql://localhost:9030?charset=utf8".into(),
            "demo".into(),
            "root".into(),
            "".into(),
            "events".into(),
            None,
            Some(2),
            Some(10),
        );
        assert_eq!(
            cfg.database_dsn(),
            "mysql://localhost:9030/demo?charset=utf8"
        );
        assert_eq!(cfg.pool_size, 2);
        assert_eq!(cfg.batch_size, 10);
    }
}
