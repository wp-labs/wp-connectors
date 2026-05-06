#![cfg(all(feature = "clickhouse", feature = "external_performance"))]

use anyhow::Result;
use wp_connectors::clickhouse::ClickHouseSinkFactory;

use crate::clickhouse_common::{
    create_clickhouse_test_config, init_clickhouse_database, query_table_count,
    wait_for_clickhouse_ready,
};
use crate::common::{
    component_tools::DockerComposeTool,
    sink::{
        performance_runtime::{SinkPerformanceConfig, SinkPerformanceRuntime},
        sink_info::SinkInfo,
    },
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "性能测试默认忽略，请按需手动执行"]
async fn test_clickhouse_sink_performance() -> Result<()> {
    let docker_tool = DockerComposeTool::new("tests/clickhouse/component/performance_tests.yml")?;

    let sink_info = SinkInfo::new(ClickHouseSinkFactory, create_clickhouse_test_config())
        .with_test_name("baseline")
        .with_async_count_fn(|_params| async { query_table_count().await })
        .with_async_init(|| async { init_clickhouse_database().await })
        .with_async_wait_ready(|_params| async { wait_for_clickhouse_ready().await });

    let config = SinkPerformanceConfig::default()
        .with_batch_size(10_0000)
        .with_task_count(4);

    let runtime = SinkPerformanceRuntime::new(docker_tool, vec![sink_info], config);
    runtime.run().await
}
