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
    /// 构建 Doris Sink，负责拉起连接池、建库建表并缓存列信息。
    ///
    /// # args
    /// * `config` - Doris 连接与写入所需的完整配置。
    /// # return: `anyhow::Result<Self>` - 成功则返回初始化后的 sink。
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

    /// 生成固定的 INSERT 语句前缀（含表名和列名）。
    ///
    /// # return
    /// * `String` - 形如 `INSERT INTO db.table (col1,...) VALUES ` 的片段。
    fn base_insert_prefix(&self) -> String {
        format!(
            "INSERT INTO {} ({}) VALUES ",
            self.quoted_table,
            self.quoted_columns.join(", ")
        )
    }

    /// 将一条 [`DataRecord`] 转换成 `(v1, v2, ..)` 形式的 VALUES 片段。
    ///
    /// # args
    /// * `record` - 上层传入的数据记录。
    ///
    /// # return
    /// * `Option<String>` - 若存在可写字段则返回 VALUES 字符串，否则为 `None`。
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

    /// 将缓存的 VALUES 组成批量 INSERT 并写入 Doris。
    ///
    /// # return
    /// * `SinkResult<()>` - 成功表示缓存已清空。
    async fn flush_pending(&mut self) -> SinkResult<()> {
        if self.pending_values.is_empty() {
            return Ok(());
        }
        //"INSERT INTO `wp_test`.`events_parsed` (`occur_time`, `src_ip`, `wp_event_id`, `time`, `digit`, `chars`, `wp_src_key`) VALUES (NULL, NULL, '1766973779209849128', NULL, NULL, NULL, 'file_1'), (NULL, NULL, '1766973779209849129', NULL, NULL, NULL, 'file_1'), (NULL, NULL, '1766973779209849130', NULL, NULL, NULL, 'file_1'), (NULL, NULL, '1766973779209849131', NULL, NULL, NULL, 'file_1'), (NULL, NULL, '1766973779209849132', NULL, NULL, NULL, 'file_1'), (NULL, NULL, '1766973779209849133', NULL, NULL, NULL, 'file_1'), (NULL, NULL, '1766973779209849134', NULL, NULL, NULL, 'file_1'), (NULL, NULL, '1766973779209849135', NULL, NULL, NULL, 'file_1'), (NULL, NULL, '1766973779209849136', NULL, NULL, NULL, 'file_1'), (NULL, NULL, '1766973779209849137', NULL, NULL, NULL, 'file_1'), (NULL, NULL, '1766973779209849138', NULL, NULL, NULL, 'file_1'), (NULL, NULL, '1766973779209849139', NULL, NULL, NULL, 'file_1'), (NULL, NULL, '1766973779209849140', NULL, NULL, NULL, 'file_1'), (NULL, NULL, '1766973779209849141', NULL, NULL, NULL, 'file_1'), (NULL, NULL, '1766973779209849142', NULL, NULL, NULL, 'file_1'), (NULL, NULL, '1766973779209849143', NULL, NULL, NULL, 'file_1'), (NULL, NULL, '1766973779209849144', NULL, NULL, NULL, 'file_1'), (NULL, NULL, '1766973779209849145', NULL, NULL, NULL, 'file_1'), (NULL, NULL, '1766973779209849146', NULL, NULL, NULL, 'file_1'), (NULL, NULL, '1766973779209849147', NULL, NULL, NULL, 'file_1'), (NULL, NULL, '1766973779209849148', NULL, NULL, NULL, 'file_1'), (NULL, NULL, '1766973779209849149', NULL, NULL, NULL, 'file_1'), (NULL, NULL, '1766973779209849150', NULL, NULL, NULL, 'file_1'), (NULL, NULL, '1766973779209849151', NULL, NULL, NULL, 'file_1'), (NULL, NULL, '1766973779209849152', NULL, NULL, NULL, 'file_1'), (NULL, NULL, '1766973779209849153', NULL, NULL, NULL, 'file_1'), (NULL, NULL, '1766973779209849154', NULL, NULL, NULL, 'file_1'), (NULL, NULL, '1766973779209849155', NULL, NULL, NULL, 'file_1'), (NULL, NULL, '1766973779209849156', NULL, NULL, NULL, 'file_1'), (NULL, NULL, '1766973779209849157', NULL, NULL, NULL, 'file_1'), (NULL, NULL, '1766973779209849158', NULL, NULL, NULL, 'file_1'), (NULL, NULL, '1766973779209849159', NULL, NULL, NULL, 'file_1'), (NULL, NULL, '1766973779209849160', NULL, NULL, NULL, 'file_1'), (NULL, NULL, '1766973779209849161', NULL, NULL, NULL, 'file_1'), (NULL, NULL, '1766973779209849162', NULL, NULL, NULL, 'file_1'), (NULL, NULL, '1766973779209849163', NULL, NULL, NULL, 'file_1'), (NULL, NULL, '1766973779209849164', NULL, NULL, NULL, 'file_1'), (NULL, NULL, '1766973779209849165', NULL, NULL, NULL, 'file_1'), (NULL, NULL, '1766973779209849166', NULL, NULL, NULL, 'file_1'), (NULL, NULL, '1766973779209849167', NULL, NULL, NULL, 'file_1'), (NULL, NULL, '1766973779209849168', NULL, NULL, NULL, 'file_1'), (NULL, NULL, '1766973779209849169', NULL, NULL, NULL, 'file_1'), (NULL, NULL, '1766973779209849170', NULL, NULL, NULL, 'file_1'), (NULL, NULL, '1766973779209849171', NULL, NULL, NULL, 'file_1'), (NULL, NULL, '1766973779209849172', NULL, NULL, NULL, 'file_1'), (NULL, NULL, '1766973779209849173', NULL, NULL, NULL, 'file_1'), (NULL, NULL, '1766973779209849174', NULL, NULL, NULL, 'file_1'), (NULL, NULL, '1766973779209849175', NULL, NULL, NULL, 'file_1'), (NULL, NULL, '1766973779209849176', NULL, NULL, NULL, 'file_1'), (NULL, NULL, '1766973779209849177', NULL, NULL, NULL, 'file_1'), (NULL, NULL, '1766973779209849178', NULL, NULL, NULL, 'file_1'), (NULL, NULL, '1766973779209849179', NULL, NULL, NULL, 'file_1'), (NULL, NULL, '1766973779209849180', NULL, NULL, NULL, 'file_1'), (NULL, NULL, '1766973779209849181', NULL, NULL, NULL, 'file_1'), (NULL, NULL, '1766973779209849182', NULL, NULL, NULL, 'file_1'), (NULL, NULL, '1766973779209849183', NULL, NULL, NULL, 'file_1'), (NULL, NULL, '1766973779209849184', NULL, NULL, NULL, 'file_1'), (NULL, NULL, '1766973779209849185', NULL, NULL, NULL, 'file_1'), (NULL, NULL, '1766973779209849186', NULL, NULL, NULL, 'file_1'), (NULL, NULL, '1766973779209849187', NULL, NULL, NULL, 'file_1'), (NULL, NULL, '1766973779209849188', NULL, NULL, NULL, 'file_1'), (NULL, NULL, '1766973779209849189', NULL, NULL, NULL, 'file_1'), (NULL, NULL, '1766973779209849190', NULL, NULL, NULL, 'file_1'), (NULL, NULL, '1766973779209849191', NULL, NULL, NULL, 'file_1')"
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

/// 以 MySQL 语法转义标识符，支持 `db.table`。
///
/// # args
/// * `input` - 需要被引用的名称。
///
/// # return
/// * `String` - 每段都以反引号包裹的标识符。
fn quote_identifier(input: &str) -> String {
    input
        .split('.')
        .map(|segment| format!("`{}`", segment.replace('`', "``")))
        .collect::<Vec<_>>()
        .join(".")
}

/// 将值中的单引号替换成 SQL 可接受的形式。
///
/// # args
/// * `value` - 原始字符串。
///
/// # return
/// * `String` - 转义后的字符串。
fn escape_single_quotes(value: &str) -> String {
    value.replace('\'', "''")
}

/// 统一封装 sink 层错误，便于上层识别。
///
/// # args
/// * `msg` - 错误描述。
///
/// # return
/// * `SinkError` - 以 `SinkReason::Sink` 包装后的错误。
fn sink_error(msg: impl Into<String>) -> SinkError {
    SinkError::from(SinkReason::Sink(msg.into()))
}

/// 规范化 MySQL 连接参数，禁用不兼容选项并设置账号密码。
///
/// # args
/// * `opts` - 初始的连接配置。
/// * `user`/`password` - 凭证。
///
/// # return
/// * `MySqlConnectOptions` - 可直接用于 sqlx 的配置。
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

/// 如目标库不存在则创建。
///
/// # args
/// * `cfg` - 当前 sink 配置。
///
/// # return
/// * `anyhow::Result<()>` - 成功后数据库一定存在。
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

/// 检查并必要时创建目标表。
///
/// # args
/// * `pool` - 已连接至目标库的连接池。
/// * `database`/`table` - 表所在的库和表名。
/// * `create_stmt` - 可选建表语句模板。
///
/// # return
/// * `anyhow::Result<()>` - 表存在或建表成功。
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

/// 从 information_schema 读取列顺序，作为批量写入的列序。
///
/// # args
/// * `pool` - 连接池。
/// * `database`/`table` - 目标表。
///
/// # return
/// * `Vec<String>` - 按 ordinal_position 排序的列名列表。
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
