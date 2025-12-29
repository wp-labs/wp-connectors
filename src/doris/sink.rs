use crate::doris::config::DorisSinkConfig;
use async_trait::async_trait;
use sqlx::{
    Row,
    mysql::{MySqlConnectOptions, MySqlPool, MySqlPoolOptions},
    raw_sql,
};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use wp_connector_api::{
    AsyncCtrl, AsyncRawDataSink, AsyncRecordSink, SinkError, SinkReason, SinkResult,
};
use wp_model_core::model::{DataRecord, DataType};

pub struct DorisSink {
    pub pool: MySqlPool,
    quoted_table: String,
    column_order: Vec<String>,
    quoted_columns: Vec<String>,
    column_set: HashSet<String>,
    batch_size: usize,
    pending_values: Vec<String>,
}

impl DorisSink {
    pub async fn new(config: DorisSinkConfig) -> anyhow::Result<Self> {
        create_database_if_missing(&config).await?;

        let db_opts = sanitize_options(
            config.database_dsn().parse::<MySqlConnectOptions>()?,
            &config.user,
            &config.password,
        );
        let pool = MySqlPoolOptions::new()
            .max_connections(config.pool_size.max(1))
            .connect_with(db_opts)
            .await?;

        ensure_table_exists(
            &pool,
            &config.database,
            &config.table,
            config.create_table.as_deref(),
        )
        .await?;

        let column_order = load_table_columns(&pool, &config.database, &config.table).await?;
        if column_order.is_empty() {
            anyhow::bail!("table `{}` has no columns", config.table);
        }
        let column_set = column_order.iter().cloned().collect::<HashSet<_>>();
        let quoted_columns = column_order
            .iter()
            .map(|name| quote_identifier(name))
            .collect::<Vec<_>>();
        let quoted_table = quote_identifier(&format!("{}.{}", config.database, config.table));

        Ok(Self {
            pool,
            quoted_table,
            column_order,
            quoted_columns,
            column_set,
            batch_size: config.batch_size,
            pending_values: Vec::with_capacity(config.batch_size),
        })
    }

    fn base_insert_prefix(&self) -> String {
        format!(
            "INSERT INTO {} ({}) VALUES ",
            self.quoted_table,
            self.quoted_columns.join(", ")
        )
    }

    fn format_values_tuple(&self, record: &DataRecord) -> Option<String> {
        let mut field_map: HashMap<&str, String> = HashMap::new();
        for field in &record.items {
            if *field.get_meta() == DataType::Ignore {
                continue;
            }
            let name = field.get_name();
            if self.column_set.contains(name) {
                field_map.insert(name, field.get_value().to_string());
            }
        }
        if field_map.is_empty() {
            return None;
        }

        let values: Vec<String> = self
            .column_order
            .iter()
            .map(|column| match field_map.get(column.as_str()) {
                Some(value) => format!("'{}'", escape_single_quotes(value)),
                None => "NULL".to_string(),
            })
            .collect();
        Some(format!("({})", values.join(", ")))
    }

    async fn flush_pending(&mut self) -> SinkResult<()> {
        if self.pending_values.is_empty() {
            return Ok(());
        }
        let sql = format!(
            "{}{}",
            self.base_insert_prefix(),
            self.pending_values.join(", ")
        );
        raw_sql(&sql)
            .execute(&self.pool)
            .await
            .map_err(|e| sink_error(format!("doris insert fail: {}", e)))?;
        self.pending_values.clear();
        Ok(())
    }
}

#[async_trait]
impl AsyncCtrl for DorisSink {
    async fn stop(&mut self) -> SinkResult<()> {
        self.flush_pending().await
    }

    async fn reconnect(&mut self) -> SinkResult<()> {
        sqlx::query("SELECT 1")
            .execute(&self.pool)
            .await
            .map_err(|e| sink_error(format!("doris reconnect fail: {}", e)))?
            .rows_affected();
        Ok(())
    }
}

#[async_trait]
impl AsyncRecordSink for DorisSink {
    async fn sink_record(&mut self, data: &DataRecord) -> SinkResult<()> {
        if let Some(raw) = self.format_values_tuple(data) {
            self.pending_values.push(raw);
            if self.pending_values.len() >= self.batch_size {
                self.flush_pending().await?;
            }
        }
        Ok(())
    }

    async fn sink_records(&mut self, data: Vec<Arc<DataRecord>>) -> SinkResult<()> {
        for record in data {
            self.sink_record(record.as_ref()).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl AsyncRawDataSink for DorisSink {
    async fn sink_str(&mut self, _data: &str) -> SinkResult<()> {
        Err(sink_error("doris sink does not accept raw text input"))
    }

    async fn sink_bytes(&mut self, _data: &[u8]) -> SinkResult<()> {
        Err(sink_error("doris sink does not accept raw byte input"))
    }

    async fn sink_str_batch(&mut self, _data: Vec<&str>) -> SinkResult<()> {
        Err(sink_error("doris sink does not accept raw batch input"))
    }

    async fn sink_bytes_batch(&mut self, _data: Vec<&[u8]>) -> SinkResult<()> {
        Err(sink_error(
            "doris sink does not accept raw batch byte input",
        ))
    }
}

fn quote_identifier(input: &str) -> String {
    input
        .split('.')
        .map(|segment| format!("`{}`", segment.replace('`', "``")))
        .collect::<Vec<_>>()
        .join(".")
}

fn escape_single_quotes(value: &str) -> String {
    value.replace('\'', "''")
}

fn sink_error(msg: impl Into<String>) -> SinkError {
    SinkError::from(SinkReason::Sink(msg.into()))
}

fn sanitize_options(
    mut opts: MySqlConnectOptions,
    user: &str,
    password: &str,
) -> MySqlConnectOptions {
    opts = opts
        .username(user)
        .pipes_as_concat(false)
        .no_engine_substitution(false)
        .set_names(false)
        .timezone(None);
    if !password.is_empty() {
        opts = opts.password(password);
    }
    opts
}

async fn create_database_if_missing(cfg: &DorisSinkConfig) -> anyhow::Result<()> {
    let admin_opts = sanitize_options(
        cfg.endpoint.parse::<MySqlConnectOptions>()?,
        &cfg.user,
        &cfg.password,
    );
    let admin_pool = MySqlPoolOptions::new()
        .max_connections(1)
        .connect_with(admin_opts)
        .await?;
    let create_sql = format!(
        "CREATE DATABASE IF NOT EXISTS {}",
        quote_identifier(&cfg.database)
    );
    raw_sql(&create_sql).execute(&admin_pool).await?;
    Ok(())
}

async fn ensure_table_exists(
    pool: &MySqlPool,
    database: &str,
    table: &str,
    create_stmt: Option<&str>,
) -> anyhow::Result<()> {
    let exists_sql = format!(
        "SELECT COUNT(1) AS cnt FROM information_schema.TABLES WHERE TABLE_SCHEMA='{}' AND TABLE_NAME='{}'",
        escape_single_quotes(database),
        escape_single_quotes(table)
    );
    let row = raw_sql(&exists_sql).fetch_one(pool).await?;
    let exists: i64 = row.try_get("cnt")?;
    if exists > 0 {
        return Ok(());
    }

    let stmt = create_stmt.ok_or_else(|| {
        anyhow::anyhow!("table `{table}` not found and create_table statement not provided")
    })?;
    let statement = stmt.replace("{table}", table);
    raw_sql(&statement).execute(pool).await?;
    Ok(())
}

async fn load_table_columns(
    pool: &MySqlPool,
    database: &str,
    table: &str,
) -> anyhow::Result<Vec<String>> {
    let sql = format!(
        "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='{}' AND TABLE_NAME='{}' ORDER BY ORDINAL_POSITION",
        escape_single_quotes(database),
        escape_single_quotes(table)
    );
    let rows = raw_sql(&sql).fetch_all(pool).await?;

    let mut cols = Vec::with_capacity(rows.len());
    for row in rows {
        let name: String = row.try_get("COLUMN_NAME")?;
        cols.push(name);
    }
    Ok(cols)
}

#[cfg(test)]
mod tests {
    use super::quote_identifier;

    #[test]
    fn quote_identifier_handles_segments() {
        assert_eq!(quote_identifier("events"), "`events`");
        assert_eq!(quote_identifier("demo.events"), "`demo`.`events`");
    }

    // #[test]
    // fn test_new() {
    //     DorisSinkConfig{
    //         endpoint:"",

    //     }
    // }
}
