use async_trait::async_trait;
use sea_orm::{ConnectionTrait, DatabaseBackend, DatabaseConnection, Statement};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::runtime::Builder;
use wp_connector_api::{
    AsyncCtrl, AsyncRawDataSink, AsyncRecordSink, SinkError, SinkReason, SinkResult,
};
use wp_log::error_data;
use wp_model_core::model::{DataRecord, DataType};

// no local Result alias needed

const DEFAULT_BATCH: usize = 100;
pub struct MysqlSink {
    pub db: DatabaseConnection,
    pub table: String,
    pub cloumn_name: Vec<String>,
    pub batch: usize,
    pub proc_cnt: usize,
    pub values: HashMap<String, Vec<String>>,
    pub dsn: String,
}

impl MysqlSink {
    pub fn new(
        db: DatabaseConnection,
        table: String,
        cloumn_name: Vec<String>,
        batch: Option<usize>,
        dsn: String,
    ) -> Self {
        Self {
            db,
            table,
            cloumn_name,
            batch: batch.unwrap_or(DEFAULT_BATCH),
            proc_cnt: 0,
            values: Default::default(),
            dsn,
        }
    }

    fn base_insert_prefix(&self) -> String {
        format!(
            "INSERT INTO {} ({}) VALUES ",
            self.table,
            self.cloumn_name
                .iter()
                .map(|s| format!("`{}`", s))
                .collect::<Vec<_>>()
                .join(", ")
        )
    }

    fn format_values_tuple(&self, record: &DataRecord) -> String {
        let field_map: HashMap<&str, String> = record
            .items
            .iter()
            .filter(|f| *f.get_meta() != DataType::Ignore)
            .map(|f| (f.get_name(), f.get_value().to_string()))
            .collect();
        let values: Vec<String> = self
            .cloumn_name
            .iter()
            .map(|col_name| match field_map.get(col_name.as_str()) {
                Some(field) => format!("'{}'", field.replace("'", "''")),
                None => {
                    error_data!("Warning: Missing field for column '{}'", col_name);
                    "NULL".to_string()
                }
            })
            .collect();
        format!("({})", values.join(", "))
    }

    /// 推送缓存区的数据
    async fn flush_pending_sqls(
        &self,
        pending_sqls: Vec<String>,
        backend: DatabaseBackend,
    ) -> SinkResult<()> {
        if pending_sqls.is_empty() {
            return Ok(());
        }
        let existing = self.db.clone();
        tokio::task::spawn_blocking(move || -> SinkResult<()> {
            let runtime = Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| {
                    SinkError::from(SinkReason::Sink(format!(
                        "build runtime for mysql flush fail: {}",
                        e
                    )))
                })?;

            runtime.block_on(async move {
                let conn = existing.clone();

                for sql in pending_sqls {
                    let state = Statement::from_string(backend, sql.clone());
                    match conn.execute(state).await {
                        Ok(_) => {}
                        Err(e) => {
                            return Err(SinkError::from(SinkReason::Sink(format!(
                                "mysql execute fail: {}",
                                e
                            ))));
                        }
                    }
                }
                Ok(())
            })
        })
        .await
        .map_err(|e| SinkError::from(SinkReason::Sink(format!("mysql flush join error: {}", e))))?
    }
}

#[async_trait]
impl AsyncCtrl for MysqlSink {
    async fn stop(&mut self) -> SinkResult<()> {
        // 将待写入的数据提前组装为 SQL 字符串，避免对 self.values 的借用贯穿异步等待
        // 同时避免在异步上下文中使用阻塞行为（如 std::thread::sleep）
        let backend = self.db.get_database_backend();
        let mut pending_sqls: Vec<String> = Vec::new();
        for vals in self.values.values() {
            if vals.is_empty() {
                continue;
            }
            // 单条 INSERT + 多个 VALUES（不加分号，兼容性更好）
            let mut sql = self.base_insert_prefix();
            sql.push_str(&vals.join(","));
            pending_sqls.push(sql);
        }
        // 清空缓存，避免重复写
        if pending_sqls.is_empty() {
            return Ok(());
        }
        self.flush_pending_sqls(pending_sqls, backend).await
    }
    async fn reconnect(&mut self) -> SinkResult<()> {
        self.db.ping().await.map_err(|e| {
            SinkError::from(SinkReason::Sink(format!("reconnect mysql fail: {}", e)))
        })?;
        Ok(())
    }
}

#[async_trait]
impl AsyncRecordSink for MysqlSink {
    async fn sink_record(&mut self, data: &DataRecord) -> SinkResult<()> {
        let raw = self.format_values_tuple(data);
        // Defer batching by grouping under same table key
        self.proc_cnt += 1;
        self.values.entry(self.table.clone()).or_default().push(raw);

        for vals in self.values.values() {
            if vals.is_empty() {
                continue;
            }
            // 单条 INSERT + 多个 VALUES
            let mut sql = self.base_insert_prefix();
            sql.push_str(&vals.join(","));
            sql.push(';');
            let state = Statement::from_string(self.db.get_database_backend(), sql);
            self.db.execute(state).await.map_err(|e| {
                SinkError::from(SinkReason::Sink(format!(
                    "mysql exec cloumns:{:?}, fail: {}",
                    self.cloumn_name, e
                )))
            })?;
        }
        self.values.clear();
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
impl AsyncRawDataSink for MysqlSink {
    async fn sink_str(&mut self, _data: &str) -> SinkResult<()> {
        Err(SinkError::from(SinkReason::Sink(
            "mysql sink does not accept raw input".into(),
        )))
    }
    async fn sink_bytes(&mut self, _data: &[u8]) -> SinkResult<()> {
        Err(SinkError::from(SinkReason::Sink(
            "mysql sink does not accept raw bytes".into(),
        )))
    }

    async fn sink_str_batch(&mut self, _data: Vec<&str>) -> SinkResult<()> {
        Err(SinkError::from(SinkReason::Sink(
            "mysql sink does not accept raw input".into(),
        )))
    }
    async fn sink_bytes_batch(&mut self, _data: Vec<&[u8]>) -> SinkResult<()> {
        Err(SinkError::from(SinkReason::Sink(
            "mysql sink does not accept raw bytes".into(),
        )))
    }
}
