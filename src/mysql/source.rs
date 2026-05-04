use crate::mysql::config::MysqlConf as MySqlConf;
use async_trait::async_trait;
use orion_error::conversion::SourceErr;
use orion_error::conversion::SourceRawErr;
use sea_orm::ConnectionTrait;
use sea_orm::{ConnectOptions, Database, DatabaseConnection, Statement};
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::time::Duration;
use wp_connector_api::{
    DataSource, SourceBatch, SourceError, SourceEvent, SourceReason, SourceResult, Tags,
};
use wp_log::info_data;
use wp_model_core::raw::RawData;

pub struct MysqlSource {
    key: String,
    db: DatabaseConnection,
    statement: String,
    checkpoint: u64,
    checkpoint_path: PathBuf,
    data_cache: VecDeque<String>,
    tags: Tags,
}

impl MysqlSource {
    pub fn identifier(&self) -> &str {
        &self.key
    }

    pub async fn new(key: String, tags: Tags, config: &MySqlConf) -> SourceResult<Self> {
        // table 在新版配置中为 Option<String>
        let table = config.table.as_deref().unwrap_or("");
        if table.trim().is_empty() {
            return Err(SourceReason::other("mysql.table must not be empty"));
        }

        wp_log::info_data!("[mysql] database: {:?}, table: {}", config.database, table);

        let checkpoint = MysqlSource::get_checkpoints(&key)?;

        let mut opt = ConnectOptions::new(config.get_database_url());
        opt.max_connections(3)
            .min_connections(1)
            .connect_timeout(Duration::from_secs(8))
            .acquire_timeout(Duration::from_secs(8))
            .idle_timeout(Duration::from_secs(8))
            .max_lifetime(Duration::from_secs(8))
            .sqlx_logging(true)
            .sqlx_logging_level(log::LevelFilter::Debug);
        let db = Database::connect(opt)
            .await
            .source_raw_err(SourceReason::SupplierError, "connect mysql source failed")?;

        let cols_sql = "SELECT COLUMN_NAME, DATA_TYPE \
                    FROM INFORMATION_SCHEMA.COLUMNS \
                    WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? \
                    ORDER BY ORDINAL_POSITION";
        let cols_stmt = Statement::from_sql_and_values(
            db.get_database_backend(),
            cols_sql,
            vec![config.database.clone().into(), table.to_string().into()],
        );

        let col_rows = db.query_all(cols_stmt).await.source_raw_err(
            SourceReason::SupplierError,
            "query mysql source columns failed",
        )?;
        let mut parts = Vec::with_capacity(col_rows.len());
        for row in col_rows {
            let name: String = row
                .try_get_by_index(0)
                .source_raw_err(SourceReason::Other, "read mysql column name failed")?;
            let dt: String = row
                .try_get_by_index(1)
                .source_raw_err(SourceReason::Other, "read mysql column data type failed")?;
            let expr = match dt.to_ascii_lowercase().as_str() {
                "binary" | "varbinary" | "blob" | "mediumblob" | "longblob" => {
                    format!("'{}', TO_BASE64(`{}`)", name, name)
                }
                _ => format!("'{}', `{}`", name, name),
            };
            parts.push(expr);
        }

        // 采样批大小：复用配置中的 batch 字段；默认 100
        let step_len: usize = config.batch.unwrap_or(100);
        let statement = format!(
            "SELECT CAST(JSON_OBJECT({}) AS CHAR CHARACTER SET utf8mb4) FROM `{}` LIMIT {} OFFSET ?;",
            parts.join(", "),
            table,
            step_len
        );
        let path_str = format!("./.run/.checkpoints/{}.dat", &key);
        let checkpoint_path = Path::new(&path_str).to_path_buf();
        Ok(Self {
            key,
            db,
            statement,
            checkpoint,
            checkpoint_path,
            data_cache: VecDeque::new(),
            tags,
        })
    }

    fn create_event(&self, event_id: u64, json_str: String) -> SourceEvent {
        SourceEvent::new(
            event_id,
            self.key.clone(),
            RawData::from_string(json_str),
            self.tags.clone().into(),
        )
    }

    /// 从数据库获取数据并填充缓存
    async fn fetch_data_from_db(&mut self) -> SourceResult<()> {
        let rows = self
            .db
            .query_all(Statement::from_sql_and_values(
                self.db.get_database_backend(),
                self.statement.clone(),
                vec![self.checkpoint.into()],
            ))
            .await
            .source_raw_err(
                SourceReason::SupplierError,
                "query mysql source data failed",
            )?;

        if rows.is_empty() {
            return Err(SourceError::from(SourceReason::EOF));
        }

        // 填充缓存
        for row in rows {
            let json_str: String = row
                .try_get_by_index(0)
                .source_raw_err(SourceReason::Other, "read mysql source row failed")?;
            self.data_cache.push_back(json_str);
        }

        Ok(())
    }

    pub async fn recv_impl(&mut self) -> SourceResult<SourceBatch> {
        if self.data_cache.is_empty() {
            // 缓存为空时，从数据库获取新数据
            self.fetch_data_from_db().await?;
        }

        if self.data_cache.is_empty() {
            // fetch_data_from_db 已经保证空时返回 EOF，这里兜底返回空批次
            return Ok(vec![]);
        }

        let mut batch = Vec::with_capacity(self.data_cache.len());
        while let Some(json_str) = self.data_cache.pop_front() {
            self.set_checkpoints()?;
            let event_id = self.checkpoint;
            batch.push(self.create_event(event_id, json_str));
        }
        Ok(batch)
    }

    /// 获取并新增 checkpoint 文件
    pub fn get_checkpoints(file_name: &str) -> SourceResult<u64> {
        let path_str = format!("./.run/.checkpoints/{}.dat", file_name);
        let path = Path::new(&path_str);
        if path.exists() {
            let contents = std::fs::read_to_string(path)
                .source_err(SourceReason::Other, "read mysql checkpoint failed")?;
            if !contents.trim().is_empty() {
                return serde_json::from_str(&contents)
                    .source_raw_err(SourceReason::Other, "parse mysql checkpoint failed");
            }
            return Ok(0);
        }

        std::fs::create_dir_all("./.run/.checkpoints")
            .source_err(SourceReason::Other, "create mysql checkpoint dir failed")?;
        std::fs::File::create(path)
            .source_err(SourceReason::Other, "create mysql checkpoint file failed")?;
        Ok(0)
    }

    /// 更新 checkpoint 文件
    pub fn set_checkpoints(&mut self) -> SourceResult<()> {
        self.checkpoint += 1;
        if let Err(e) = std::fs::write(&self.checkpoint_path, self.checkpoint.to_string()) {
            info_data!("set checkpoint {} failed: {}", self.checkpoint, e);
        }
        Ok(())
    }
}

#[async_trait]
impl DataSource for MysqlSource {
    async fn receive(&mut self) -> SourceResult<SourceBatch> {
        self.recv_impl().await
    }

    fn try_receive(&mut self) -> Option<SourceBatch> {
        None
    }

    fn identifier(&self) -> String {
        self.key.clone()
    }
}
