use async_trait::async_trait;
use orion_error::prelude::SourceRawErr;
use sea_orm::{ConnectionTrait, DatabaseConnection};
use std::collections::HashMap;
use std::sync::Arc;
use wp_connector_api::{AsyncCtrl, AsyncRawDataSink, AsyncRecordSink, SinkReason, SinkResult};
use wp_log::error_data;
use wp_model_core::model::{DataRecord, DataType};

pub struct MysqlSink {
    pub db: DatabaseConnection,
    pub table: String,
    pub cloumn_name: Vec<String>,
}

impl MysqlSink {
    pub fn new(db: DatabaseConnection, table: String, cloumn_name: Vec<String>) -> Self {
        Self {
            db,
            table,
            cloumn_name,
        }
    }

    fn base_insert_prefix(&self) -> String {
        // 使用 INSERT IGNORE：若数据库已写入但客户端因断连未收到响应，重试时避免主键/唯一键冲突
        format!(
            "INSERT IGNORE INTO {} ({}) VALUES ",
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
}

#[async_trait]
impl AsyncCtrl for MysqlSink {
    async fn stop(&mut self) -> SinkResult<()> {
        Ok(())
    }
    async fn reconnect(&mut self) -> SinkResult<()> {
        self.db
            .ping()
            .await
            .source_raw_err(SinkReason::Sink, "reconnect mysql fail")?;
        Ok(())
    }
}

#[async_trait]
impl AsyncRecordSink for MysqlSink {
    async fn sink_record(&mut self, data: &DataRecord) -> SinkResult<()> {
        self.sink_records(vec![Arc::new(data.clone())]).await
    }

    async fn sink_records(&mut self, data: Vec<Arc<DataRecord>>) -> SinkResult<()> {
        let mut raws = Vec::with_capacity(data.len());
        for record in data {
            raws.push(self.format_values_tuple(record.as_ref()));
        }
        if !raws.is_empty() {
            // 单条 INSERT + 多个 VALUES
            let mut sql = self.base_insert_prefix();
            sql.push_str(&raws.join(","));
            if let Err(e) = self.db.execute_unprepared(sql.as_str()).await {
                return Err(SinkReason::sink(format!(
                    "mysql exec cloumns:{:?}, fail: {}, sql: {}",
                    self.cloumn_name, e, sql
                )));
            }
        }
        Ok(())
    }
}

#[async_trait]
impl AsyncRawDataSink for MysqlSink {
    async fn sink_str(&mut self, _data: &str) -> SinkResult<()> {
        Err(SinkReason::sink("mysql sink does not accept raw input"))
    }
    async fn sink_bytes(&mut self, _data: &[u8]) -> SinkResult<()> {
        Err(SinkReason::sink("mysql sink does not accept raw bytes"))
    }

    async fn sink_str_batch(&mut self, _data: Vec<&str>) -> SinkResult<()> {
        Err(SinkReason::sink("mysql sink does not accept raw input"))
    }
    async fn sink_bytes_batch(&mut self, _data: Vec<&[u8]>) -> SinkResult<()> {
        Err(SinkReason::sink("mysql sink does not accept raw bytes"))
    }
}

#[cfg(test)]
mod tests {
    use super::MysqlSink;
    use sea_orm::DatabaseConnection;
    use wp_model_core::model::{DataField, DataRecord};

    fn make_sink(table: &str, columns: Vec<&str>) -> MysqlSink {
        MysqlSink::new(
            DatabaseConnection::default(),
            table.to_string(),
            columns.into_iter().map(|s| s.to_string()).collect(),
        )
    }

    #[test]
    fn mysql_sink_base_insert_prefix() {
        let sink = make_sink("users", vec!["name", "age"]);
        let sql = sink.base_insert_prefix();
        assert_eq!(sql, "INSERT IGNORE INTO users (`name`, `age`) VALUES ");
    }

    #[test]
    fn mysql_sink_format_values_tuple() {
        let sink = make_sink("users", vec!["name", "age", "note"]);
        let mut record = DataRecord::default();
        record.append(DataField::from_chars("name", "O'Reilly"));
        record.append(DataField::from_digit("age", 42));
        record.append(DataField::from_ignore("unused"));

        let values = sink.format_values_tuple(&record);
        assert_eq!(values, "('O''Reilly', '42', NULL)");
    }
}
