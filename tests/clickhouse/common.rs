use anyhow::{Context, Result};
use reqwest::Client;
use serde_json::json;
use wp_connector_api::ParamMap;

pub const TEST_CLICKHOUSE_ENDPOINT: &str = "http://127.0.0.1:8123";
pub const TEST_CLICKHOUSE_DB: &str = "test_db";
pub const TEST_CLICKHOUSE_TABLE: &str = "wp_nginx";
pub const TEST_CLICKHOUSE_USER: &str = "default";
pub const TEST_CLICKHOUSE_PASSWORD: &str = "default";
const CLICKHOUSE_READY_ATTEMPTS: usize = 10;
const CLICKHOUSE_READY_INTERVAL_SECS: u64 = 2;
const CLICKHOUSE_READY_STABLE_PROBES: usize = 3;

fn clickhouse_client() -> Client {
    Client::new()
}

pub fn create_clickhouse_test_config() -> ParamMap {
    let mut params = ParamMap::new();
    params.insert("endpoint".into(), json!(TEST_CLICKHOUSE_ENDPOINT));
    params.insert("database".into(), json!(TEST_CLICKHOUSE_DB));
    params.insert("table".into(), json!(TEST_CLICKHOUSE_TABLE));
    params.insert("username".into(), json!(TEST_CLICKHOUSE_USER));
    params.insert("password".into(), json!(TEST_CLICKHOUSE_PASSWORD));
    params.insert("timeout_secs".into(), json!(30));
    params.insert("max_retries".into(), json!(3));
    params
}

async fn execute_sql(sql: &str) -> Result<String> {
    let response = clickhouse_client()
        .post(format!("{}/", TEST_CLICKHOUSE_ENDPOINT))
        .basic_auth(TEST_CLICKHOUSE_USER, Some(TEST_CLICKHOUSE_PASSWORD))
        .body(sql.to_owned())
        .send()
        .await
        .with_context(|| format!("执行 ClickHouse SQL 失败: {sql}"))?;

    let status = response.status();
    let body = response.text().await?;
    if !status.is_success() {
        anyhow::bail!("ClickHouse SQL 执行失败: status={}, body={}", status, body);
    }

    Ok(body)
}

async fn probe_clickhouse_service_ready() -> Result<()> {
    let ping_result = async {
        let resp = clickhouse_client()
            .get(format!("{}/ping", TEST_CLICKHOUSE_ENDPOINT))
            .basic_auth(TEST_CLICKHOUSE_USER, Some(TEST_CLICKHOUSE_PASSWORD))
            .send()
            .await
            .context("请求 ClickHouse /ping 失败")?;

        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        if !status.is_success() || body.trim() != "Ok." {
            anyhow::bail!("status={}, body={}", status, body);
        }

        Ok::<(), anyhow::Error>(())
    }
    .await;

    let sql_result = execute_sql("SELECT 1").await;

    match (ping_result, sql_result) {
        (Ok(()), Ok(_)) => Ok(()),
        (_, Ok(_)) => Ok(()),
        (Ok(()), Err(err)) => Err(err),
        (Err(ping_err), Err(sql_err)) => {
            anyhow::bail!("ping 与 SQL 探测都失败: ping={}, sql={}", ping_err, sql_err)
        }
    }?;

    Ok(())
}

pub async fn wait_for_clickhouse_ready() -> Result<()> {
    let mut last_error = None;
    let mut stable_successes = 0usize;

    for attempt in 1..=CLICKHOUSE_READY_ATTEMPTS {
        match probe_clickhouse_service_ready().await {
            Ok(()) => {
                stable_successes += 1;
                if stable_successes >= CLICKHOUSE_READY_STABLE_PROBES {
                    println!(
                        "✓ ClickHouse 服务已稳定就绪，连续 {} 次探测成功（第 {} 次完成）",
                        CLICKHOUSE_READY_STABLE_PROBES, attempt
                    );
                    return Ok(());
                }

                println!(
                    "ClickHouse 服务探测成功，继续观察稳定性（{}/{})...",
                    stable_successes, CLICKHOUSE_READY_STABLE_PROBES
                );
            }
            Err(err) => {
                stable_successes = 0;
                last_error = Some(err.to_string());
            }
        }

        tokio::time::sleep(tokio::time::Duration::from_secs(
            CLICKHOUSE_READY_INTERVAL_SECS,
        ))
        .await;
    }

    anyhow::bail!(
        "等待 ClickHouse 就绪超时: {}",
        last_error.unwrap_or_else(|| "未知错误".to_string())
    )
}

pub async fn init_clickhouse_database() -> Result<()> {
    execute_sql(&format!(
        "CREATE DATABASE IF NOT EXISTS {}",
        TEST_CLICKHOUSE_DB
    ))
    .await?;
    execute_sql(&format!(
        "DROP TABLE IF EXISTS {}.{}",
        TEST_CLICKHOUSE_DB, TEST_CLICKHOUSE_TABLE
    ))
    .await?;

    execute_sql(&format!(
        "CREATE TABLE {}.{} (\
            wp_event_id Int64, \
            wp_src_key String, \
            sip String, \
            timestamp String, \
            `http/request` String, \
            status Int32, \
            size Int64, \
            referer String, \
            `http/agent` String\
        ) ENGINE = MergeTree ORDER BY wp_event_id",
        TEST_CLICKHOUSE_DB, TEST_CLICKHOUSE_TABLE
    ))
    .await?;

    println!("✓ ClickHouse 测试库表初始化完成");
    Ok(())
}

pub async fn query_table_count() -> Result<i64> {
    let body = execute_sql(&format!(
        "SELECT count() FROM {}.{}",
        TEST_CLICKHOUSE_DB, TEST_CLICKHOUSE_TABLE
    ))
    .await?;

    body.trim()
        .parse::<i64>()
        .with_context(|| format!("解析 ClickHouse count 失败: {}", body.trim()))
}
