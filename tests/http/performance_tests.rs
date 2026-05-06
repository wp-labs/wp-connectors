#![cfg(all(feature = "http", feature = "external_performance"))]

use anyhow::Result;
use std::time::Duration;
use wp_connectors::http::HttpSinkFactory;

use crate::common::{
    component_tools::DockerComposeTool,
    sink::{
        performance_runtime::{SinkPerformanceConfig, SinkPerformanceRuntime},
        sink_info::SinkInfo,
    },
};
use crate::http_common::{create_http_performance_scenarios, wait_for_http_nginx_ready};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "性能测试默认忽略，请按需手动执行"]
// cargo test --release --package wp-connectors --test http_tests --features http,external_performance performance_tests::test_http_sink_performance -- --exact --nocapture
async fn test_http_sink_performance() -> Result<()> {
    let tool = DockerComposeTool::new("tests/http/component/docker-compose.yml")?;

    let sink_infos = create_http_performance_scenarios()
        .into_iter()
        .map(|(test_name, params)| {
            SinkInfo::new(HttpSinkFactory, params)
                .with_test_name(test_name)
                .with_async_wait_ready(|_params| async { wait_for_http_nginx_ready().await })
        })
        .collect();

    let config = SinkPerformanceConfig::new()
        .with_total_records(300_0000)
        //这里的并发数量需要和tokio:test中的并发数一致
        .with_task_count(4)
        .with_batch_size(1_0000)
        .with_progress_interval(Duration::from_secs(5));

    let runtime = SinkPerformanceRuntime::new(tool, sink_infos, config);
    runtime.run().await
}
