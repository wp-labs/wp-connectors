#![cfg(all(feature = "http", feature = "external_integration"))]

use anyhow::Result;
use wp_connectors::http::HttpSinkFactory;

use crate::common::{
    component_tools::{ShellScriptRestart, ShellScriptTool},
    sink::{integration_runtime::SinkIntegrationRuntime, sink_info::SinkInfo},
};
use crate::http_common::{
    create_http_integration_scenarios, query_http_count, wait_for_http_ready,
};

#[tokio::test]
#[ignore = "集成测试默认忽略，请按需手动执行"]
async fn test_http_sink_full_integration() -> Result<()> {
    let tool = ShellScriptTool::new_with_options(
        "tests/http/component/start_server.sh",
        "tests/http/component/stop_server.sh",
        Some("tests/http/component/install_deps.sh"),
        Some("tests/http/component/wait_ready.sh"),
        ShellScriptRestart::NoRestart,
    )?;

    let sink_infos = create_http_integration_scenarios()
        .into_iter()
        .map(|(test_name, params)| {
            SinkInfo::new(HttpSinkFactory, params)
                .with_test_name(test_name)
                .with_async_count_fn(query_http_count)
                .with_async_wait_ready(|_params| async { wait_for_http_ready().await })
        })
        .collect();

    let runtime = SinkIntegrationRuntime::new(tool, sink_infos);
    runtime.run(true).await
}
