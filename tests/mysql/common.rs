#![cfg(all(
    feature = "mysql",
    any(feature = "external_integration", feature = "external_performance")
))]

use anyhow::{Context, Result};
use sea_orm::sqlx::{
    self, ConnectOptions, Row,
    mysql::{MySqlConnectOptions, MySqlPoolOptions},
};
use serde_json::json;
use std::str::FromStr;
use wp_connector_api::ParamMap;

pub const TEST_MYSQL_HOST: &str = "127.0.0.1";
pub const TEST_MYSQL_PORT: u16 = 3306;
pub const TEST_MYSQL_DB: &str = "default";
pub const TEST_MYSQL_TABLE: &str = "wp_nginx";
pub const TEST_MYSQL_USER: &str = "root";
pub const TEST_MYSQL_PASSWORD: &str = "123456";

const MYSQL_READY_ATTEMPTS: usize = 20;
const MYSQL_READY_INTERVAL_SECS: u64 = 2;
const MYSQL_READY_STABLE_PROBES: usize = 3;

pub fn create_mysql_test_config() -> ParamMap {
    let mut params = ParamMap::new();
    params.insert(
        "endpoint".into(),
        json!(format!("{}:{}", TEST_MYSQL_HOST, TEST_MYSQL_PORT)),
    );
    params.insert("database".into(), json!(TEST_MYSQL_DB));
    params.insert("table".into(), json!(TEST_MYSQL_TABLE));
    params.insert("username".into(), json!(TEST_MYSQL_USER));
    params.insert("password".into(), json!(TEST_MYSQL_PASSWORD));
    params.insert(
        "columns".into(),
        json!([
            "wp_event_id",
            "wp_src_key",
            "sip",
            "timestamp",
            "http/request",
            "status",
            "size",
            "referer",
            "http/agent"
        ]),
    );
    params.insert("batch_size".into(), json!(5_000));
    params
}

fn mysql_url(database: Option<&str>) -> String {
    match database {
        Some(database) => format!(
            "mysql://{}:{}@{}:{}/{}",
            TEST_MYSQL_USER, TEST_MYSQL_PASSWORD, TEST_MYSQL_HOST, TEST_MYSQL_PORT, database
        ),
        None => format!(
            "mysql://{}:{}@{}:{}",
            TEST_MYSQL_USER, TEST_MYSQL_PASSWORD, TEST_MYSQL_HOST, TEST_MYSQL_PORT
        ),
    }
}

async fn create_mysql_pool(database: Option<&str>) -> Result<sqlx::MySqlPool> {
    let options = MySqlConnectOptions::from_str(&mysql_url(database))?.disable_statement_logging();
    Ok(MySqlPoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await?)
}

async fn execute_mysql(database: Option<&str>, sql: &str) -> Result<()> {
    let pool = create_mysql_pool(database).await?;
    sqlx::query(sql)
        .execute(&pool)
        .await
        .with_context(|| format!("执行 MySQL SQL 失败: {sql}"))?;
    pool.close().await;
    Ok(())
}

async fn probe_mysql_service_ready() -> Result<()> {
    let pool = create_mysql_pool(None).await?;
    sqlx::query("SELECT 1").fetch_one(&pool).await?;
    pool.close().await;
    Ok(())
}

pub async fn wait_for_mysql_ready() -> Result<()> {
    let mut last_error = None;
    let mut stable_successes = 0usize;

    for attempt in 1..=MYSQL_READY_ATTEMPTS {
        match probe_mysql_service_ready().await {
            Ok(()) => {
                stable_successes += 1;
                if stable_successes >= MYSQL_READY_STABLE_PROBES {
                    println!(
                        "✓ MySQL 服务已稳定就绪，连续 {} 次探测成功（第 {} 次完成）",
                        MYSQL_READY_STABLE_PROBES, attempt
                    );
                    return Ok(());
                }

                println!(
                    "MySQL 服务探测成功，继续观察稳定性（{}/{})...",
                    stable_successes, MYSQL_READY_STABLE_PROBES
                );
            }
            Err(err) => {
                stable_successes = 0;
                last_error = Some(err.to_string());
            }
        }

        tokio::time::sleep(tokio::time::Duration::from_secs(MYSQL_READY_INTERVAL_SECS)).await;
    }

    anyhow::bail!(
        "等待 MySQL 服务就绪超时: {}",
        last_error.unwrap_or_else(|| "未知错误".to_string())
    )
}

pub async fn init_mysql_database() -> Result<()> {
    execute_mysql(
        None,
        &format!("CREATE DATABASE IF NOT EXISTS `{}`", TEST_MYSQL_DB),
    )
    .await?;
    execute_mysql(
        Some(TEST_MYSQL_DB),
        &format!("DROP TABLE IF EXISTS `{}`", TEST_MYSQL_TABLE),
    )
    .await?;

    execute_mysql(
        Some(TEST_MYSQL_DB),
        &format!(
            "CREATE TABLE `{}` (\
                `wp_event_id` BIGINT NOT NULL, \
                `wp_src_key` VARCHAR(255) NOT NULL, \
                `sip` VARCHAR(64) NOT NULL, \
                `timestamp` VARCHAR(64) NOT NULL, \
                `http/request` TEXT NULL, \
                `status` INT NOT NULL, \
                `size` BIGINT NOT NULL, \
                `referer` VARCHAR(255) NULL, \
                `http/agent` TEXT NULL, \
                PRIMARY KEY (`wp_event_id`)\
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
            TEST_MYSQL_TABLE
        ),
    )
    .await?;

    println!("✓ MySQL 测试库表初始化完成");
    Ok(())
}

pub async fn query_table_count() -> Result<i64> {
    let pool = create_mysql_pool(Some(TEST_MYSQL_DB)).await?;
    let count = sqlx::query(&format!("SELECT COUNT(*) FROM `{}`", TEST_MYSQL_TABLE))
        .fetch_one(&pool)
        .await?
        .try_get::<i64, _>(0)?;
    pool.close().await;
    Ok(count)
}
