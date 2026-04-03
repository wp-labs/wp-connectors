use anyhow::Result;
use serde_json::json;
use wp_connector_api::ParamMap;

#[cfg(feature = "external_integration")]
pub const TEST_HTTP_SERVER: &str = "http://127.0.0.1:18080";

#[cfg(feature = "external_performance")]
pub const TEST_HTTP_NGINX_SERVER: &str = "http://127.0.0.1:8080";

pub const ALL_HTTP_FORMATS: [&str; 6] = ["json", "ndjson", "csv", "kv", "raw", "proto-text"];
const HTTP_READY_ATTEMPTS: usize = 15;
const HTTP_READY_INTERVAL_SECS: u64 = 1;
const HTTP_READY_STABLE_PROBES: usize = 1;

#[cfg(feature = "external_integration")]
pub fn create_http_test_config(
    path: &str,
    fmt: &str,
    compression: &str,
    username: Option<&str>,
    password: Option<&str>,
    batch_size: usize,
) -> ParamMap {
    let mut params = ParamMap::new();
    params.insert(
        "endpoint".into(),
        json!(format!("{}{}", TEST_HTTP_SERVER, path)),
    );
    params.insert("method".into(), json!("POST"));
    params.insert("fmt".into(), json!(fmt));
    params.insert("batch_size".into(), json!(batch_size));
    params.insert("timeout_secs".into(), json!(30));
    params.insert("max_retries".into(), json!(3));
    params.insert("compression".into(), json!(compression));

    if let Some(username) = username {
        params.insert("username".into(), json!(username));
    }
    if let Some(password) = password {
        params.insert("password".into(), json!(password));
    }

    params
}

#[cfg(feature = "external_performance")]
pub fn create_http_test_config_with_base(
    base_url: &str,
    path: &str,
    fmt: &str,
    compression: &str,
    username: Option<&str>,
    password: Option<&str>,
    batch_size: usize,
) -> ParamMap {
    let mut params = ParamMap::new();
    params.insert("endpoint".into(), json!(format!("{}{}", base_url, path)));
    params.insert("method".into(), json!("POST"));
    params.insert("fmt".into(), json!(fmt));
    params.insert("batch_size".into(), json!(batch_size));
    params.insert("timeout_secs".into(), json!(30));
    params.insert("max_retries".into(), json!(3));
    params.insert("compression".into(), json!(compression));

    if let Some(username) = username {
        params.insert("username".into(), json!(username));
    }
    if let Some(password) = password {
        params.insert("password".into(), json!(password));
    }

    params
}

#[cfg(feature = "external_integration")]
pub fn create_http_integration_scenarios() -> Vec<(String, ParamMap)> {
    let mut scenarios = Vec::new();

    for fmt in ALL_HTTP_FORMATS {
        scenarios.push((
            format!("basic_{fmt}"),
            create_http_test_config(&format!("/ingest/{fmt}"), fmt, "none", None, None, 1),
        ));
        scenarios.push((
            format!("auth_{fmt}"),
            create_http_test_config(
                &format!("/auth/ingest/{fmt}"),
                fmt,
                "none",
                Some("root"),
                Some("root"),
                1,
            ),
        ));
        scenarios.push((
            format!("gzip_{fmt}"),
            create_http_test_config(&format!("/gzip/ingest/{fmt}"), fmt, "gzip", None, None, 1),
        ));
    }

    scenarios
}

#[cfg(feature = "external_performance")]
pub fn create_http_performance_scenarios() -> Vec<(String, ParamMap)> {
    let mut scenarios = Vec::new();

    for fmt in ALL_HTTP_FORMATS {
        scenarios.push((
            format!("basic_{fmt}"),
            create_http_test_config_with_base(
                TEST_HTTP_NGINX_SERVER,
                &format!("/ingest/{fmt}"),
                fmt,
                "none",
                None,
                None,
                1,
            ),
        ));
        scenarios.push((
            format!("auth_{fmt}"),
            create_http_test_config_with_base(
                TEST_HTTP_NGINX_SERVER,
                &format!("/auth/ingest/{fmt}"),
                fmt,
                "none",
                Some("root"),
                Some("root"),
                1,
            ),
        ));
        scenarios.push((
            format!("gzip_{fmt}"),
            create_http_test_config_with_base(
                TEST_HTTP_NGINX_SERVER,
                &format!("/gzip/ingest/{fmt}"),
                fmt,
                "gzip",
                None,
                None,
                1,
            ),
        ));
    }

    scenarios
}

async fn wait_for_http_endpoint_ready(
    base_url: &str,
    path: &str,
    service_name: &str,
) -> Result<()> {
    let client = reqwest::Client::new();
    let mut last_error = None;
    let mut stable_successes = 0usize;

    for attempt in 1..=HTTP_READY_ATTEMPTS {
        match client.get(format!("{}{}", base_url, path)).send().await {
            Ok(resp) if resp.status().is_success() => {
                stable_successes += 1;
                if stable_successes >= HTTP_READY_STABLE_PROBES {
                    println!(
                        "✓ {}已稳定就绪，连续 {} 次探测成功（第 {} 次完成）",
                        service_name, HTTP_READY_STABLE_PROBES, attempt
                    );
                    return Ok(());
                }

                println!(
                    "{}探测成功，继续观察稳定性（{}/{})...",
                    service_name, stable_successes, HTTP_READY_STABLE_PROBES
                );
            }
            Ok(resp) => {
                stable_successes = 0;
                last_error = Some(format!("unexpected status: {}", resp.status()));
            }
            Err(err) => {
                stable_successes = 0;
                last_error = Some(err.to_string());
            }
        }

        tokio::time::sleep(tokio::time::Duration::from_secs(HTTP_READY_INTERVAL_SECS)).await;
    }

    anyhow::bail!(
        "等待{}超时: {}",
        service_name,
        last_error.unwrap_or_else(|| "未知错误".to_string())
    )
}

#[cfg(feature = "external_integration")]
pub async fn wait_for_http_ready() -> Result<()> {
    wait_for_http_endpoint_ready(TEST_HTTP_SERVER, "/health", "HTTP 测试服务").await
}

#[cfg(feature = "external_integration")]
pub async fn query_http_count(params: ParamMap) -> Result<i64> {
    let fmt = params
        .get("fmt")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("http test params missing fmt"))?;

    let value: serde_json::Value = reqwest::get(format!("{}/count", TEST_HTTP_SERVER))
        .await?
        .json()
        .await?;

    value
        .get("counts")
        .and_then(|counts| counts.get(fmt))
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| anyhow::anyhow!("http count response missing counts.{}", fmt))
}

#[cfg(feature = "external_performance")]
pub async fn wait_for_http_nginx_ready() -> Result<()> {
    wait_for_http_endpoint_ready(TEST_HTTP_NGINX_SERVER, "/nginx_status", "HTTP nginx 服务").await
}

#[cfg(feature = "external_performance")]
#[allow(dead_code)]
pub async fn query_http_total_count(_params: ParamMap) -> Result<i64> {
    let body = reqwest::get(format!("{}/nginx_status", TEST_HTTP_NGINX_SERVER))
        .await?
        .text()
        .await?;

    let requests = body
        .lines()
        .find_map(|line| {
            let numbers = line
                .split_whitespace()
                .filter_map(|part| part.parse::<i64>().ok())
                .collect::<Vec<_>>();

            if numbers.len() == 3 {
                Some(numbers[2])
            } else {
                None
            }
        })
        .ok_or_else(|| anyhow::anyhow!("nginx_status 中未找到 requests 统计: {}", body))?;

    Ok(requests)
}
