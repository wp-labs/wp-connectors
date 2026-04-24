#![cfg(all(feature = "http", feature = "external_integration"))]

use std::io::Write;
use std::net::TcpListener;
use std::time::Duration;

use anyhow::Result;
use flate2::{Compression, write::GzEncoder};
use serde_json::{Value, json};
use wp_connector_api::ParamMap;
use wp_connectors::http::HttpSourceFactory;

use crate::common::{
    component_tools::{ShellScriptRestart, ShellScriptTool},
    source::{integration_runtime::SourceIntegrationRuntime, source_info::SourceInfo},
};

fn free_port() -> Result<u16> {
    Ok(TcpListener::bind(("127.0.0.1", 0))?.local_addr()?.port())
}

fn create_http_source_test_config(port: u16, path: &str) -> ParamMap {
    let mut params = ParamMap::new();
    params.insert("port".into(), json!(port));
    params.insert("path".into(), json!(path));
    params
}

fn gzip_bytes(input: &[u8]) -> Result<Vec<u8>> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(input)?;
    Ok(encoder.finish()?)
}

fn build_http_log_record(idx: usize) -> Value {
    json!({
        "event_id": idx + 1,
        "service": "edge-gateway",
        "method": if idx.is_multiple_of(2) { "GET" } else { "POST" },
        "path": format!("/api/v1/orders/{}", 1000 + idx),
        "status": if idx.is_multiple_of(3) { 200 } else { 201 },
        "latency_ms": 12 + idx,
        "client_ip": format!("10.0.0.{}", 10 + idx),
        "user_agent": "integration-test-client/1.0",
        "request_id": format!("req-{:04}", idx + 1),
        "ts": format!("2026-04-23T02:18:{:02}Z", idx % 60),
    })
}

fn build_http_log_records(count: usize) -> Vec<Value> {
    (0..count).map(build_http_log_record).collect()
}

fn encode_json_body(count: usize) -> Result<Vec<u8>> {
    Ok(serde_json::to_vec(&build_http_log_records(count))?)
}

fn encode_ndjson_body(count: usize) -> Result<Vec<u8>> {
    let mut body = String::new();
    for record in build_http_log_records(count) {
        body.push_str(&serde_json::to_string(&record)?);
        body.push('\n');
    }
    Ok(body.into_bytes())
}

fn create_request_body(fmt: &str, count: usize, gzip: bool) -> Result<Vec<u8>> {
    let body = match fmt {
        "json" => encode_json_body(count)?,
        "ndjson" => encode_ndjson_body(count)?,
        other => anyhow::bail!("unsupported http source test fmt: {other}"),
    };

    if gzip { gzip_bytes(&body) } else { Ok(body) }
}

struct HttpSourceScenario {
    test_name: &'static str,
    path: &'static str,
    query: &'static str,
    request_body: Vec<u8>,
    expected_count: usize,
}

#[tokio::test]
#[ignore = "集成测试默认忽略，请按需手动执行"]
async fn test_http_source_basic_integration() -> Result<()> {
    let tool = ShellScriptTool::new_with_options(
        "tests/http/component/source_noop_start.sh",
        "tests/http/component/source_noop_stop.sh",
        None::<&str>,
        None::<&str>,
        ShellScriptRestart::NoRestart,
    )?;

    let scenarios = vec![
        HttpSourceScenario {
            test_name: "json",
            path: "/ingest/source-json",
            query: "fmt=json",
            request_body: create_request_body("json", 4, false)?,
            expected_count: 4,
        },
        HttpSourceScenario {
            test_name: "ndjson",
            path: "/ingest/source-ndjson",
            query: "fmt=ndjson",
            request_body: create_request_body("ndjson", 4, false)?,
            expected_count: 4,
        },
        HttpSourceScenario {
            test_name: "gzip_json",
            path: "/ingest/source-gzip-json",
            query: "fmt=json&compression=gzip",
            request_body: create_request_body("json", 4, true)?,
            expected_count: 4,
        },
        HttpSourceScenario {
            test_name: "gzip_ndjson",
            path: "/ingest/source-gzip-ndjson",
            query: "fmt=ndjson&compression=gzip",
            request_body: create_request_body("ndjson", 4, true)?,
            expected_count: 4,
        },
    ];

    let source_infos = scenarios
        .into_iter()
        .map(|scenario| {
            let port = free_port().expect("allocate free port");
            let endpoint = format!(
                "http://127.0.0.1:{port}{}?{}",
                scenario.path, scenario.query
            );
            let request_body = scenario.request_body;
            let expected_count = scenario.expected_count;
            let params = create_http_source_test_config(port, scenario.path);

            SourceInfo::new(HttpSourceFactory, params)
                .with_test_name(scenario.test_name)
                .with_input_repeat(10)
                .with_async_input(move |_params| {
                    let endpoint = endpoint.clone();
                    let request_body = request_body.clone();
                    async move {
                        let client = reqwest::Client::new();
                        let mut last_error = None;

                        // “最多重试 20 次”。
                        // 一旦 endpoint 返回 200 + OK，就立即退出，所以正常只会成功发送 1 次请求。
                        for _ in 0..20 {
                            match client
                                .post(&endpoint)
                                .body(request_body.clone())
                                .send()
                                .await
                            {
                                Ok(response) => {
                                    let status = response.status();
                                    let body = response.text().await.unwrap_or_default();
                                    if status.is_success() && body == "OK" {
                                        return Ok(expected_count);
                                    }
                                    last_error =
                                        Some(format!("unexpected response: {status}, body={body}"));
                                }
                                Err(err) => {
                                    last_error = Some(err.to_string());
                                }
                            }

                            tokio::time::sleep(Duration::from_millis(50)).await;
                        }

                        anyhow::bail!(
                            "HTTP source endpoint did not become ready in time: {}",
                            last_error.unwrap_or_else(|| "unknown error".to_string())
                        )
                    }
                })
                .with_collect_timeout(Duration::from_secs(3))
                .with_poll_interval(Duration::from_millis(50))
        })
        .collect();

    let runtime = SourceIntegrationRuntime::new(tool, source_infos);
    runtime.run(true).await
}
