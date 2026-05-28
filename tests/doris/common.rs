use anyhow::Result;
use sea_orm::sqlx::{
    self, ConnectOptions, Row,
    mysql::{MySqlConnectOptions, MySqlPoolOptions, MySqlRow},
};
use sea_orm::{ConnectOptions as SeaConnectOptions, ConnectionTrait, Database};
use serde_json::json;
use std::str::FromStr;
use wp_connector_api::ParamMap;
use wp_model_core::model::{DataField, DataRecord};

/// Doris BE HTTP 地址，用于 Stream Load 写入。
pub const TEST_DORIS_ENDPOINT: &str = "http://localhost:8040";
/// Doris MySQL 协议地址，用于建库建表、就绪探测和数量查询。
pub const TEST_DORIS_MYSQL_HOST: &str = "127.0.0.1";
/// Doris MySQL 协议端口。
pub const TEST_DORIS_MYSQL_PORT: u16 = 9030;
/// Doris 测试数据库名。
pub const TEST_DORIS_DB: &str = "test_db";
/// Doris 动态表名前缀，实际表名会拼成 `wp_nginx_<referer>`。
pub const TEST_DORIS_TABLE: &str = "wp_nginx";
/// Doris sink 动态表模板；测试记录中的 `referer` 字段会替换占位符。
pub const TEST_DORIS_DYNAMIC_TABLE_TEMPLATE: &str = "wp_nginx_#{referer}";
/// 集成测试预建的动态表数量，记录会按 `wp_event_id % 表数量` 路由。
pub const INTEGRATION_DYNAMIC_TABLE_COUNT: i64 = 3;
/// 性能测试预建的动态表数量，独立于性能测试总记录数。
pub const PERFORMANCE_DYNAMIC_TABLE_COUNT: i64 = 4;
/// 性能测试总记录数。
pub const PERFORMANCE_RECORD_COUNT: usize = 1000_0000;
/// Doris 测试用户名。
pub const TEST_DORIS_USER: &str = "root";
/// Doris 测试密码；None 表示空密码。
pub const TEST_DORIS_PASSWORD: Option<&str> = None;
/// Doris 就绪探测最大次数。
const DORIS_READY_ATTEMPTS: usize = 30;
/// Doris 就绪探测间隔秒数。
const DORIS_READY_INTERVAL_SECS: u64 = 2;
/// 连续探测成功次数，达到后才认为 Doris 集群稳定就绪。
const DORIS_READY_STABLE_PROBES: usize = 3;

pub fn doris_mysql_options(database: Option<&str>) -> Result<MySqlConnectOptions> {
    let mut options = MySqlConnectOptions::from_str(&format!(
        "mysql://{}@{}:{}",
        TEST_DORIS_USER, TEST_DORIS_MYSQL_HOST, TEST_DORIS_MYSQL_PORT
    ))?
    .disable_statement_logging();

    if let Some(password) = TEST_DORIS_PASSWORD {
        options = options.password(password);
    }

    if let Some(database) = database {
        options = options.database(database);
    }

    Ok(options)
}

pub fn doris_mysql_url(database: Option<&str>) -> String {
    let auth = match TEST_DORIS_PASSWORD {
        Some(password) => format!("{}:{}", TEST_DORIS_USER, password),
        None => TEST_DORIS_USER.to_string(),
    };

    match database {
        Some(database) => format!(
            "mysql://{}@{}:{}/{}",
            auth, TEST_DORIS_MYSQL_HOST, TEST_DORIS_MYSQL_PORT, database
        ),
        None => format!(
            "mysql://{}@{}:{}",
            auth, TEST_DORIS_MYSQL_HOST, TEST_DORIS_MYSQL_PORT
        ),
    }
}

pub async fn create_doris_admin_conn(
    database: Option<&str>,
) -> Result<sea_orm::DatabaseConnection> {
    let mut options = SeaConnectOptions::new(doris_mysql_url(database));
    options
        .max_connections(1)
        .min_connections(1)
        .connect_timeout(std::time::Duration::from_secs(5))
        .acquire_timeout(std::time::Duration::from_secs(5))
        .idle_timeout(std::time::Duration::from_secs(5))
        .max_lifetime(std::time::Duration::from_secs(5))
        .sqlx_logging(false)
        .map_sqlx_mysql_opts(|opt| opt.statement_cache_capacity(0));
    Ok(Database::connect(options).await?)
}

pub async fn create_doris_pool(database: Option<&str>) -> Result<sqlx::MySqlPool> {
    Ok(MySqlPoolOptions::new()
        .max_connections(1)
        .connect_with(doris_mysql_options(database)?)
        .await?)
}

pub fn create_doris_test_config() -> ParamMap {
    let mut params = ParamMap::new();
    params.insert("endpoint".into(), json!(TEST_DORIS_ENDPOINT));
    params.insert("database".into(), json!(TEST_DORIS_DB));
    params.insert("table".into(), json!(TEST_DORIS_DYNAMIC_TABLE_TEMPLATE));
    params.insert("user".into(), json!(TEST_DORIS_USER));
    params.insert("password".into(), json!(TEST_DORIS_PASSWORD.unwrap_or("")));
    params.insert("timeout_secs".into(), json!(30));
    params.insert("max_retries".into(), json!(3));
    params
}

pub fn create_doris_test_record(id: i64, table_count: i64, prefix: &str) -> DataRecord {
    let mut record = DataRecord::default();
    record.append(DataField::from_digit("wp_event_id", id));
    record.append(DataField::from_chars(
        "wp_src_key",
        format!("{prefix}_{id}"),
    ));
    record.append(DataField::from_chars("sip", "192.168.1.100"));
    record.append(DataField::from_chars("timestamp", "2024-03-02 10:00:00"));
    record.append(DataField::from_chars(
        "http/request",
        format!("GET /api/{prefix}/{id} HTTP/1.1"),
    ));
    record.append(DataField::from_digit("status", 200));
    record.append(DataField::from_digit("size", 1024 + id));
    record.append(DataField::from_chars(
        "referer",
        dynamic_table_suffix(id, table_count),
    ));
    record.append(DataField::from_chars(
        "http/agent",
        "Mozilla/5.0 (Doris Test)",
    ));
    record
}

pub fn create_doris_test_records(
    start_id: i64,
    count: usize,
    table_count: i64,
    prefix: &str,
) -> Vec<DataRecord> {
    (0..count)
        .map(|idx| create_doris_test_record(start_id + idx as i64, table_count, prefix))
        .collect()
}

pub async fn query_table_count() -> Result<i64> {
    let pool = create_doris_pool(Some(TEST_DORIS_DB)).await?;
    let count = query_dynamic_table_count(&pool).await?;
    pool.close().await;
    Ok(count)
}

pub async fn query_dynamic_table_count(pool: &sqlx::MySqlPool) -> Result<i64> {
    let table_rows = sqlx::query(&format!(
        "SHOW TABLES FROM {} LIKE '{}_%'",
        quote_identifier(TEST_DORIS_DB),
        TEST_DORIS_TABLE
    ))
    .fetch_all(pool)
    .await?;

    let mut total = 0i64;
    for row in table_rows {
        let table_name: String = row.try_get(0)?;
        let count = sqlx::query(&format!(
            "SELECT COUNT(*) FROM {}.{}",
            quote_identifier(TEST_DORIS_DB),
            quote_identifier(&table_name)
        ))
        .fetch_one(pool)
        .await?
        .try_get::<i64, _>(0)?;
        total += count;
    }

    Ok(total)
}

fn read_bool_column(row: &MySqlRow, column: &str) -> Option<bool> {
    if let Ok(value) = row.try_get::<bool, _>(column) {
        return Some(value);
    }
    if let Ok(value) = row.try_get::<i64, _>(column) {
        return Some(value != 0);
    }
    if let Ok(value) = row.try_get::<u64, _>(column) {
        return Some(value != 0);
    }
    if let Ok(value) = row.try_get::<String, _>(column) {
        match value.trim().to_ascii_lowercase().as_str() {
            "true" | "yes" | "1" => return Some(true),
            "false" | "no" | "0" => return Some(false),
            _ => {}
        }
    }

    None
}

async fn ensure_doris_cluster_ready(pool: &sqlx::MySqlPool) -> Result<()> {
    sqlx::query("SHOW DATABASES").fetch_one(pool).await?;

    let frontends = sqlx::query("SHOW FRONTENDS").fetch_all(pool).await?;
    if frontends.is_empty() {
        anyhow::bail!("Doris frontend 尚未注册");
    }

    let has_alive_master = frontends.iter().any(|row| {
        read_bool_column(row, "Alive").unwrap_or(true)
            && read_bool_column(row, "IsMaster").unwrap_or(false)
    });
    if !has_alive_master {
        anyhow::bail!("Doris FE Master 尚未就绪");
    }

    let backends = sqlx::query("SHOW BACKENDS").fetch_all(pool).await?;
    if backends.is_empty() {
        anyhow::bail!("Doris backend 尚未注册");
    }

    let all_backends_alive = backends
        .iter()
        .all(|row| read_bool_column(row, "Alive").unwrap_or(false));
    if !all_backends_alive {
        anyhow::bail!("Doris backend 尚未全部存活");
    }

    Ok(())
}

async fn probe_doris_table_ddl() -> Result<()> {
    let db = create_doris_admin_conn(None).await?;
    let probe_table = "__wp_ready_probe";

    db.execute_unprepared(&format!("CREATE DATABASE IF NOT EXISTS {}", TEST_DORIS_DB))
        .await?;
    db.execute_unprepared(&format!(
        "DROP TABLE IF EXISTS {}.{}",
        TEST_DORIS_DB, probe_table
    ))
    .await?;
    db.execute_unprepared(&format!(
        r#"CREATE TABLE {}.{} (
            id BIGINT
        )
        ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")"#,
        TEST_DORIS_DB, probe_table
    ))
    .await?;
    db.execute_unprepared(&format!(
        "DROP TABLE IF EXISTS {}.{}",
        TEST_DORIS_DB, probe_table
    ))
    .await?;

    Ok(())
}

pub async fn wait_for_doris_sink_ready() -> Result<()> {
    let mut last_error = None;
    let mut stable_successes = 0usize;

    for attempt in 1..=DORIS_READY_ATTEMPTS {
        match create_doris_pool(None).await {
            Ok(pool) => {
                let ready = async {
                    ensure_doris_cluster_ready(&pool).await?;
                    probe_doris_table_ddl().await?;
                    Ok::<(), anyhow::Error>(())
                }
                .await;

                pool.close().await;
                match ready {
                    Ok(()) => {
                        stable_successes += 1;
                        if stable_successes >= DORIS_READY_STABLE_PROBES {
                            println!(
                                "✓ Doris 集群已稳定就绪，连续 {} 次探测成功（第 {} 次完成）",
                                DORIS_READY_STABLE_PROBES, attempt
                            );
                            return Ok(());
                        }

                        println!(
                            "Doris 集群探测成功，继续观察稳定性（{}/{})...",
                            stable_successes, DORIS_READY_STABLE_PROBES
                        );
                    }
                    Err(err) => {
                        stable_successes = 0;
                        last_error = Some(err.to_string());
                    }
                }
            }
            Err(err) => {
                stable_successes = 0;
                last_error = Some(err.to_string());
            }
        }

        tokio::time::sleep(tokio::time::Duration::from_secs(DORIS_READY_INTERVAL_SECS)).await;
    }

    anyhow::bail!(
        "等待 Doris sink 就绪超时: {}",
        last_error.unwrap_or_else(|| "未知错误".to_string())
    )
}

pub async fn init_doris_database() -> Result<()> {
    init_doris_database_with_suffixes(dynamic_table_suffixes(INTEGRATION_DYNAMIC_TABLE_COUNT)).await
}

pub async fn init_doris_performance_database() -> Result<()> {
    init_doris_database_with_suffixes(dynamic_table_suffixes(PERFORMANCE_DYNAMIC_TABLE_COUNT)).await
}

pub async fn init_doris_database_with_suffixes<I>(suffixes: I) -> Result<()>
where
    I: IntoIterator<Item = String>,
{
    println!("初始化 Doris 数据库和表...");
    let suffixes: Vec<String> = suffixes.into_iter().collect();

    let db = create_doris_admin_conn(None).await?;

    db.execute_unprepared(&format!("CREATE DATABASE IF NOT EXISTS {}", TEST_DORIS_DB))
        .await?;
    println!("✓ 数据库创建成功");

    for table_name in list_existing_dynamic_tables().await? {
        db.execute_unprepared(&format!(
            "DROP TABLE IF EXISTS {}.{}",
            quote_identifier(TEST_DORIS_DB),
            quote_identifier(&table_name)
        ))
        .await?;
    }
    println!("✓ 旧动态表已删除");

    let mut last_error = None;
    for attempt in 1..=10 {
        match create_dynamic_tables(&db, &suffixes).await {
            Ok(()) => {
                println!("✓ 动态表创建成功");
                return Ok(());
            }
            Err(err) => {
                let err_msg = err.to_string();
                if err_msg.contains("already exists") {
                    println!("✓ 表已存在，视为创建成功");
                    return Ok(());
                }
                last_error = Some(err_msg);
                println!("Doris 集群未就绪，第 {} 次重试...", attempt);
                tokio::time::sleep(tokio::time::Duration::from_secs(DORIS_READY_INTERVAL_SECS))
                    .await;
            }
        }
    }

    anyhow::bail!(
        "表创建失败，已重试多次: {}",
        last_error.unwrap_or_else(|| "未知错误".to_string())
    )
}

async fn create_dynamic_tables(
    db: &sea_orm::DatabaseConnection,
    suffixes: &[String],
) -> Result<()> {
    for suffix in suffixes {
        db.execute_unprepared(&create_table_sql(&format!(
            "{}_{}",
            TEST_DORIS_TABLE, suffix
        )))
        .await?;
    }
    Ok(())
}

async fn list_existing_dynamic_tables() -> Result<Vec<String>> {
    let pool = create_doris_pool(Some(TEST_DORIS_DB)).await?;
    let rows = sqlx::query(&format!(
        "SHOW TABLES FROM {} LIKE '{}_%'",
        quote_identifier(TEST_DORIS_DB),
        TEST_DORIS_TABLE
    ))
    .fetch_all(&pool)
    .await?;

    let mut tables = Vec::with_capacity(rows.len());
    for row in rows {
        tables.push(row.try_get::<String, _>(0)?);
    }
    pool.close().await;
    Ok(tables)
}

fn dynamic_table_suffix(id: i64, table_count: i64) -> String {
    let bucket = id.rem_euclid(table_count);
    format!("bucket_{bucket:04}")
}

fn dynamic_table_suffixes(table_count: i64) -> impl Iterator<Item = String> {
    (0..table_count).map(move |id| dynamic_table_suffix(id, table_count))
}

fn create_table_sql(table_name: &str) -> String {
    format!(
        r#"CREATE TABLE {}.{} (
            wp_event_id BIGINT COMMENT '事件唯一ID',
            wp_src_key STRING COMMENT '数据来源表示',
            sip STRING COMMENT '客户端IP',
            `timestamp` STRING COMMENT '原始时间字符串',
            `http/request` STRING COMMENT 'HTTP请求行',
            status SMALLINT COMMENT 'HTTP状态码',
            size INT COMMENT '响应大小(byte)',
            `http/agent` STRING COMMENT 'User-Agent'
        )
        ENGINE=OLAP
        DUPLICATE KEY(wp_event_id)
        DISTRIBUTED BY HASH(wp_event_id) BUCKETS 8
        PROPERTIES ("replication_num" = "1")"#,
        quote_identifier(TEST_DORIS_DB),
        quote_identifier(table_name)
    )
}

fn quote_identifier(identifier: &str) -> String {
    format!("`{}`", identifier.replace('`', "``"))
}
