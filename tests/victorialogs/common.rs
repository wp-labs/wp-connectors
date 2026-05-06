#![allow(dead_code)]

use anyhow::{Context, Result};
use reqwest::header::CONTENT_TYPE;
use serde::Deserialize;
use serde_json::json;
use wp_connector_api::ParamMap;

pub const TEST_VLOGS_ENDPOINT: &str = "http://127.0.0.1:9428";
pub const TEST_VLOGS_INSERT_PATH: &str = "/insert/jsonline";

const VLOGS_READY_ATTEMPTS: usize = 20;
const VLOGS_READY_INTERVAL_SECS: u64 = 2;
const VLOGS_READY_STABLE_PROBES: usize = 3;

#[derive(Debug, Deserialize)]
struct HitsResponse {
    hits: Vec<HitsItem>,
}

#[derive(Debug, Deserialize)]
struct HitsItem {
    total: i64,
}

fn vlogs_client() -> reqwest::Client {
    reqwest::Client::new()
}

pub fn create_vlogs_test_config() -> ParamMap {
    let mut params = ParamMap::new();
    params.insert("endpoint".into(), json!(TEST_VLOGS_ENDPOINT));
    params.insert("insert_path".into(), json!(TEST_VLOGS_INSERT_PATH));
    params.insert("fmt".into(), json!("json"));
    params.insert("request_timeout_secs".into(), json!(30.0));
    params
}

async fn probe_vlogs_service_ready() -> Result<()> {
    let response = vlogs_client()
        .post(format!("{}/select/logsql/query", TEST_VLOGS_ENDPOINT))
        .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
        .body("query=*&limit=1")
        .send()
        .await
        .context("请求 VictoriaLogs 查询接口失败")?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("status={}, body={}", status, body);
    }

    Ok(())
}

pub async fn wait_for_vlogs_ready() -> Result<()> {
    let mut last_error = None;
    let mut stable_successes = 0usize;

    for attempt in 1..=VLOGS_READY_ATTEMPTS {
        match probe_vlogs_service_ready().await {
            Ok(()) => {
                stable_successes += 1;
                if stable_successes >= VLOGS_READY_STABLE_PROBES {
                    println!(
                        "✓ VictoriaLogs 服务已稳定就绪，连续 {} 次探测成功（第 {} 次完成）",
                        VLOGS_READY_STABLE_PROBES, attempt
                    );
                    return Ok(());
                }

                println!(
                    "VictoriaLogs 服务探测成功，继续观察稳定性（{}/{})...",
                    stable_successes, VLOGS_READY_STABLE_PROBES
                );
            }
            Err(err) => {
                stable_successes = 0;
                last_error = Some(err.to_string());
            }
        }

        tokio::time::sleep(tokio::time::Duration::from_secs(VLOGS_READY_INTERVAL_SECS)).await;
    }

    anyhow::bail!(
        "等待 VictoriaLogs 就绪超时: {}",
        last_error.unwrap_or_else(|| "未知错误".to_string())
    )
}

pub async fn init_vlogs_state() -> Result<()> {
    println!("✓ VictoriaLogs 测试无需额外初始化，沿用 count_before/count_after 差量校验");
    Ok(())
}

pub async fn query_vlogs_count(_params: ParamMap) -> Result<i64> {
    let response = vlogs_client()
        .post(format!("{}/select/logsql/hits", TEST_VLOGS_ENDPOINT))
        .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
        .body("query=wp_event_id:*&start=7d&end=now&step=7d")
        .send()
        .await
        .context("请求 VictoriaLogs hits 接口失败")?;

    let status = response.status();
    let body = response.text().await?;
    if !status.is_success() {
        anyhow::bail!(
            "查询 VictoriaLogs count 失败: status={}, body={}",
            status,
            body
        );
    }

    let hits: HitsResponse = serde_json::from_str(&body)
        .with_context(|| format!("解析 VictoriaLogs hits 响应失败: {}", body))?;

    Ok(hits.hits.iter().map(|item| item.total).sum())
}
