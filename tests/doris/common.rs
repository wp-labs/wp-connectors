use anyhow::Result;
use sea_orm::sqlx::{
    self, ConnectOptions, Row,
    mysql::{MySqlConnectOptions, MySqlPoolOptions, MySqlRow},
};
use sea_orm::{ConnectOptions as SeaConnectOptions, ConnectionTrait, Database};
use serde_json::json;
use std::str::FromStr;
use wp_connector_api::ParamMap;

pub const TEST_DORIS_ENDPOINT: &str = "http://localhost:8040";
pub const TEST_DORIS_MYSQL_HOST: &str = "127.0.0.1";
pub const TEST_DORIS_MYSQL_PORT: u16 = 9030;
pub const TEST_DORIS_DB: &str = "test_db";
pub const TEST_DORIS_TABLE: &str = "wp_nginx";
pub const TEST_DORIS_USER: &str = "root";
pub const TEST_DORIS_PASSWORD: Option<&str> = None;
const DORIS_READY_ATTEMPTS: usize = 30;
const DORIS_READY_INTERVAL_SECS: u64 = 2;
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
    params.insert("table".into(), json!(TEST_DORIS_TABLE));
    params.insert("user".into(), json!(TEST_DORIS_USER));
    params.insert("password".into(), json!(TEST_DORIS_PASSWORD.unwrap_or("")));
    params.insert("timeout_secs".into(), json!(30));
    params.insert("max_retries".into(), json!(3));
    params
}

pub async fn query_table_count() -> Result<i64> {
    let pool = create_doris_pool(Some(TEST_DORIS_DB)).await?;
    let count = sqlx::query(&format!(
        "SELECT COUNT(*) FROM {}.{}",
        TEST_DORIS_DB, TEST_DORIS_TABLE
    ))
    .fetch_one(&pool)
    .await?
    .try_get::<i64, _>(0)?;
    pool.close().await;
    Ok(count)
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
    println!("初始化 Doris 数据库和表...");

    let db = create_doris_admin_conn(None).await?;

    db.execute_unprepared(&format!("CREATE DATABASE IF NOT EXISTS {}", TEST_DORIS_DB))
        .await?;
    println!("✓ 数据库创建成功");

    db.execute_unprepared(&format!(
        "DROP TABLE IF EXISTS {}.{}",
        TEST_DORIS_DB, TEST_DORIS_TABLE
    ))
    .await?;
    println!("✓ 旧表已删除");

    let create_table_sql = format!(
        r#"CREATE TABLE {}.{} (
            wp_event_id BIGINT COMMENT '事件唯一ID',
            wp_src_key STRING COMMENT '数据来源表示',
            sip STRING COMMENT '客户端IP',
            `timestamp` STRING COMMENT '原始时间字符串',
            `http/request` STRING COMMENT 'HTTP请求行',
            status SMALLINT COMMENT 'HTTP状态码',
            size INT COMMENT '响应大小(byte)',
            referer STRING COMMENT '来源页面',
            `http/agent` STRING COMMENT 'User-Agent'
        )
        ENGINE=OLAP
        DUPLICATE KEY(wp_event_id)
        DISTRIBUTED BY HASH(wp_event_id) BUCKETS 8
        PROPERTIES ("replication_num" = "1")"#,
        TEST_DORIS_DB, TEST_DORIS_TABLE
    );

    let mut last_error = None;
    for attempt in 1..=10 {
        match db.execute_unprepared(&create_table_sql).await {
            Ok(_) => {
                println!("✓ 表创建成功");
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
