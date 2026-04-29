use super::common::{DmdbConnectionHandle, connect_shared, escape_sql_literal, quote_identifier};
use super::config::DmdbSinkConf;
use async_trait::async_trait;
use odbc_api::Connection;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::task;
use wp_connector_api::{
    AsyncCtrl, AsyncRawDataSink, AsyncRecordSink, SinkError, SinkReason, SinkResult,
};
use wp_log::{error_data, warn_data};
use wp_model_core::model::{DataRecord, DataType};

/// 达梦批量写入 Sink。
/// 当前采用“多值 INSERT + 单事务提交”模式，保证一个 batch 内要么全部成功，要么全部回滚。
pub struct DmdbSink {
    /// 当前 Sink 自己持有的 ODBC 连接句柄，事务写入时会在阻塞线程中独占使用。
    connection: Option<DmdbConnectionHandle>,
    /// 原始配置，主要用于 reconnect 时重建连接。
    config: DmdbSinkConf,
    /// 可选 schema。
    schema: Option<String>,
    /// 目标表名。
    table: String,
    /// 写入列顺序，决定 INSERT 字段列表与值的映射关系。
    column_names: Vec<String>,
    /// 单条 INSERT 最多拼接的记录数。
    batch_size: usize,
    /// SQL 执行超时。
    query_timeout_secs: Option<usize>,
}

impl DmdbSink {
    pub fn new(
        connection: DmdbConnectionHandle,
        config: DmdbSinkConf,
        schema: Option<String>,
        table: String,
        column_names: Vec<String>,
        batch_size: usize,
        query_timeout_secs: Option<usize>,
    ) -> Self {
        Self {
            connection: Some(connection),
            config,
            schema,
            table,
            column_names,
            batch_size,
            query_timeout_secs,
        }
    }

    fn shared_connection(&self) -> SinkResult<DmdbConnectionHandle> {
        self.connection.clone().ok_or_else(|| {
            SinkError::from(SinkReason::Sink(
                "dmdb connection is not initialized".into(),
            ))
        })
    }

    fn qualified_table_name(&self) -> String {
        match self.schema.as_deref().map(str::trim) {
            Some(schema) if !schema.is_empty() => {
                format!(
                    "{}.{}",
                    quote_identifier(schema),
                    quote_identifier(self.table.as_str())
                )
            }
            _ => quote_identifier(self.table.as_str()),
        }
    }

    /// 生成固定的 INSERT 前缀，后续批次只需要追加 values 元组。
    fn insert_prefix(&self) -> String {
        format!(
            "INSERT INTO {} ({}) VALUES ",
            self.qualified_table_name(),
            self.column_names
                .iter()
                .map(|column| quote_identifier(column))
                .collect::<Vec<_>>()
                .join(", ")
        )
    }

    /// 按配置列顺序提取 DataRecord 字段，缺失字段显式写为 `NULL`。
    /// 这里不做类型推断，统一交给数据库侧做隐式转换或抛错。
    fn format_values_row(&self, record: &DataRecord) -> String {
        let field_map: HashMap<&str, String> = record
            .items
            .iter()
            .filter(|field| *field.get_meta() != DataType::Ignore)
            .map(|field| (field.get_name(), field.get_value().to_string()))
            .collect();

        let values = self
            .column_names
            .iter()
            .map(|column_name| match field_map.get(column_name.as_str()) {
                Some(field) => format!("'{}'", escape_sql_literal(field)),
                None => {
                    warn_data!(
                        "dmdb sink missing field for column '{}', fallback to NULL",
                        column_name
                    );
                    "NULL".to_string()
                }
            })
            .collect::<Vec<_>>();

        format!("({})", values.join(", "))
    }

    fn build_insert_statement(&self, records: &[Arc<DataRecord>]) -> Option<String> {
        if records.is_empty() {
            return None;
        }

        // 一个批次拼成一条多 values INSERT，减少数据库往返次数。
        let tuples = records
            .iter()
            .map(|record| self.format_values_row(record.as_ref()))
            .collect::<Vec<_>>();

        let mut statement = self.insert_prefix();
        statement.push_str(&tuples.join(","));
        Some(statement)
    }
}

#[async_trait]
impl AsyncCtrl for DmdbSink {
    async fn stop(&mut self) -> SinkResult<()> {
        Ok(())
    }

    async fn reconnect(&mut self) -> SinkResult<()> {
        let connection = connect_shared(&self.config.conn).await.map_err(|err| {
            SinkError::from(SinkReason::Sink(format!("reconnect dmdb fail: {err}")))
        })?;
        self.connection = Some(connection);
        Ok(())
    }
}

#[async_trait]
impl AsyncRecordSink for DmdbSink {
    async fn sink_record(&mut self, data: &DataRecord) -> SinkResult<()> {
        self.sink_records(vec![Arc::new(data.clone())]).await
    }

    async fn sink_records(&mut self, data: Vec<Arc<DataRecord>>) -> SinkResult<()> {
        if data.is_empty() {
            return Ok(());
        }

        // 按 batch_size 切分 SQL，但整次 sink_records 仍包在一个事务里执行。
        let mut statements = Vec::new();
        for chunk in data.chunks(self.batch_size) {
            let Some(statement) = self.build_insert_statement(chunk) else {
                continue;
            };
            statements.push(statement);
        }

        if statements.is_empty() {
            return Ok(());
        }

        let connection = self.shared_connection()?;
        let query_timeout_secs = self.query_timeout_secs;
        let columns = self.column_names.clone();
        let statements_for_log = statements.clone();

        let result = task::spawn_blocking(move || {
            execute_statements_in_transaction(connection, statements, query_timeout_secs)
        })
        .await
        .map_err(|err| {
            SinkError::from(SinkReason::Sink(format!(
                "spawn dmdb transaction exec task failed: {err}"
            )))
        })?;

        if let Err(err) = result {
            error_data!(
                "dmdb exec transaction columns:{:?}, fail: {}, sqls: {:?}",
                columns,
                err,
                statements_for_log
            );
            return Err(SinkError::from(SinkReason::Sink(format!(
                "dmdb exec transaction columns:{:?}, fail: {}, sqls: {:?}",
                columns, err, statements_for_log
            ))));
        }

        Ok(())
    }
}

#[async_trait]
impl AsyncRawDataSink for DmdbSink {
    async fn sink_str(&mut self, _data: &str) -> SinkResult<()> {
        Err(SinkError::from(SinkReason::Sink(
            "dmdb sink does not accept raw input".into(),
        )))
    }

    async fn sink_bytes(&mut self, _data: &[u8]) -> SinkResult<()> {
        Err(SinkError::from(SinkReason::Sink(
            "dmdb sink does not accept raw bytes".into(),
        )))
    }

    async fn sink_str_batch(&mut self, _data: Vec<&str>) -> SinkResult<()> {
        Err(SinkError::from(SinkReason::Sink(
            "dmdb sink does not accept raw input".into(),
        )))
    }

    async fn sink_bytes_batch(&mut self, _data: Vec<&[u8]>) -> SinkResult<()> {
        Err(SinkError::from(SinkReason::Sink(
            "dmdb sink does not accept raw bytes".into(),
        )))
    }
}

fn execute_statements_in_transaction(
    connection: DmdbConnectionHandle,
    statements: Vec<String>,
    query_timeout_secs: Option<usize>,
) -> anyhow::Result<()> {
    if statements.is_empty() {
        return Ok(());
    }

    let conn_guard = connection
        .lock()
        .map_err(|_| anyhow::anyhow!("lock dmdb connection fail"))?;

    // 整次 sink_records 共用一个事务，避免前半批成功、后半批失败后留下部分写入。
    conn_guard
        .set_autocommit(false)
        .map_err(|err| anyhow::anyhow!("set dmdb autocommit=false fail: {err}"))?;

    let result = (|| -> anyhow::Result<()> {
        for statement in &statements {
            conn_guard
                .execute(statement.as_str(), (), query_timeout_secs)
                .map(|_| ())
                .map_err(|err| {
                    anyhow::anyhow!("execute dmdb transaction sql fail: {err}, sql: {statement}")
                })?;
        }

        conn_guard
            .commit()
            .map_err(|err| anyhow::anyhow!("commit dmdb transaction fail: {err}"))?;

        Ok(())
    })();

    match result {
        Ok(()) => {
            if let Err(err) = conn_guard.set_autocommit(true) {
                warn_data!("restore dmdb autocommit after commit failed: {err}");
            }
            Ok(())
        }
        Err(err) => rollback_and_restore_autocommit(err, &conn_guard),
    }
}

/// 回滚事务并恢复自动提交模式。
/// 如果 rollback 自身失败，则保留 autocommit=false，避免在未知事务状态下继续提交。
fn rollback_and_restore_autocommit(
    err: anyhow::Error,
    connection: &Connection<'static>,
) -> anyhow::Result<()> {
    connection.rollback().map_err(|rollback_err| {
        anyhow::anyhow!(
            "{err}; rollback dmdb transaction also failed: {rollback_err}; autocommit is not restored to avoid committing an uncertain transaction"
        )
    })?;

    connection.set_autocommit(true).map_err(|autocommit_err| {
        anyhow::anyhow!(
            "{err}; dmdb transaction has been rolled back, but restore autocommit failed: {autocommit_err}"
        )
    })?;

    Err(anyhow::anyhow!(
        "{err}; dmdb transaction has been rolled back"
    ))
}

#[cfg(test)]
mod tests {
    use super::DmdbSink;
    use crate::dmdb::common::{escape_sql_literal, quote_identifier};
    use crate::dmdb::{DmdbConnConf, DmdbSinkConf};
    use wp_model_core::model::{DataField, DataRecord};

    fn build_test_conf() -> DmdbSinkConf {
        DmdbSinkConf {
            conn: DmdbConnConf {
                endpoint: String::new(),
                dsn: None,
                connection_string: None,
                driver: String::new(),
                username: String::new(),
                password: String::new(),
                schema: None,
                connect_timeout_secs: None,
                query_timeout_secs: None,
            },
            table: None,
            batch_size: None,
        }
    }

    #[test]
    fn quote_identifier_escapes_double_quote() {
        assert_eq!(quote_identifier("A\"B"), "\"A\"\"B\"");
    }

    #[test]
    fn escape_sql_literal_escapes_single_quote() {
        assert_eq!(escape_sql_literal("O'Reilly"), "O''Reilly");
    }

    #[test]
    fn dmdb_sink_insert_prefix() {
        let sink = DmdbSink {
            connection: None,
            config: build_test_conf(),
            schema: Some("WP_DATA".into()),
            table: "users".into(),
            column_names: vec!["name".into(), "age".into()],
            batch_size: 1024,
            query_timeout_secs: Some(8),
        };

        let sql = sink.insert_prefix();
        assert_eq!(
            sql,
            "INSERT INTO \"WP_DATA\".\"users\" (\"name\", \"age\") VALUES "
        );
    }

    #[test]
    fn dmdb_sink_format_values_row() {
        let sink = DmdbSink {
            connection: None,
            config: build_test_conf(),
            schema: Some("WP_DATA".into()),
            table: "users".into(),
            column_names: vec!["name".into(), "age".into(), "note".into()],
            batch_size: 1024,
            query_timeout_secs: Some(8),
        };
        let mut record = DataRecord::default();
        record.append(DataField::from_chars("name", "O'Reilly"));
        record.append(DataField::from_digit("age", 42));
        record.append(DataField::from_ignore("unused"));

        let values = sink.format_values_row(&record);
        assert_eq!(values, "('O''Reilly', '42', NULL)");
    }
}
