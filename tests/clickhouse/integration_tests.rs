#![cfg(all(feature = "clickhouse", feature = "external_integration"))]

use anyhow::Result;
use wp_connectors::clickhouse::ClickHouseSinkFactory;

use crate::clickhouse_common::{
    create_clickhouse_test_config, init_clickhouse_database, query_table_count,
    wait_for_clickhouse_ready,
};
use crate::common::{
    component_tools::DockerComposeTool,
    sink::{integration_runtime::SinkIntegrationRuntime, sink_info::SinkInfo},
};

#[tokio::test]
#[ignore = "集成测试默认忽略，请按需手动执行"]
async fn test_clickhouse_sink_full_integration() -> Result<()> {
    let docker_tool = DockerComposeTool::new("tests/clickhouse/component/integration_tests.yml")?;

    let sink_info = SinkInfo::new(ClickHouseSinkFactory, create_clickhouse_test_config())
        .with_test_name("basic")
        .with_async_count_fn(|_params| async { query_table_count().await })
        .with_async_init(|| async { init_clickhouse_database().await })
        .with_async_wait_ready(|_params| async { wait_for_clickhouse_ready().await });

    let runtime = SinkIntegrationRuntime::new(docker_tool, vec![sink_info]);
    runtime.run(true).await
}
