use crate::mysql::config::MysqlConf as MySqlConf;
use async_trait::async_trait;
use orion_error::UvsReason;
use sea_orm::ConnectionTrait;
use sea_orm::{ConnectOptions, Database, DatabaseConnection, Statement};
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::time::Duration;
use wp_connector_api::{
    DataSource, SourceBatch, SourceError, SourceEvent, SourceReason, SourceResult, Tags,
};
use wp_log::info_data;
use wp_model_core::model::TagSet;
use wp_parse_api::RawData;

type AnyResult<T> = anyhow::Result<T>;

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

    pub async fn new(key: String, tag_set: TagSet, config: &MySqlConf) -> AnyResult<Self> {
        // table 在新版配置中为 Option<String>
        let table = config.table.as_deref().unwrap_or("");
        if table.trim().is_empty() {
            anyhow::bail!("mysql.table must not be empty");
        }

        wp_log::info_data!("[mysql] database: {:?}, table: {}", config.database, table);

        let mut tags = Tags::new();
        for (k, v) in &tag_set.item {
            tags.set(k.clone(), v.clone());
        }

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
        let db = Database::connect(opt).await?;

        let cols_sql = "SELECT COLUMN_NAME, DATA_TYPE \
                    FROM INFORMATION_SCHEMA.COLUMNS \
                    WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? \
                    ORDER BY ORDINAL_POSITION";
        let cols_stmt = Statement::from_sql_and_values(
            db.get_database_backend(),
            cols_sql,
            vec![config.database.clone().into(), table.to_string().into()],
        );

        let col_rows = db.query_all(cols_stmt).await?;
        let mut parts = Vec::with_capacity(col_rows.len());
        for row in col_rows {
            let name: String = row.try_get_by_index(0)?;
            let dt: String = row.try_get_by_index(1)?;
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
            self.key.clone().into(),
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
            .map_err(|e| SourceReason::Uvs(UvsReason::DataError(e.to_string(), Some(200))))?;

        if rows.is_empty() {
            return Err(SourceError::from(SourceReason::EOF));
        }

        // 填充缓存
        for row in rows {
            let json_str: String = row
                .try_get_by_index(0)
                .map_err(|e| SourceReason::Uvs(UvsReason::DataError(e.to_string(), Some(200))))?;
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
    pub fn get_checkpoints(file_name: &str) -> anyhow::Result<u64> {
        let path_str = format!("./.run/.checkpoints/{}.dat", file_name);
        let path = Path::new(&path_str);
        if path.exists() {
            let contents = std::fs::read_to_string(path)?;
            if !contents.trim().is_empty() {
                return Ok(serde_json::from_str(&contents)?);
            }
            return Ok(0);
        }

        std::fs::create_dir_all("./.run/.checkpoints")?;
        std::fs::File::create(path)?;
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
