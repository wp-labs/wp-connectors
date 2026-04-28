#![cfg(all(feature = "postgres", feature = "external_integration"))]

use anyhow::Result;
use wp_connectors::postgres::PostgresSinkFactory;

use crate::common::{
    component_tools::DockerComposeTool,
    sink::{integration_runtime::SinkIntegrationRuntime, sink_info::SinkInfo},
};
use crate::postgresql_common::{
    create_postgresql_test_config, init_postgresql_database, query_table_count,
    wait_for_postgresql_ready,
};

#[tokio::test]
#[ignore = "集成测试默认忽略，请按需手动执行"]
async fn test_postgresql_sink_full_integration() -> Result<()> {
    let docker_tool = DockerComposeTool::new("tests/postgresql/component/docker-compose.yml")?;

    let sink_info = SinkInfo::new(PostgresSinkFactory, create_postgresql_test_config())
        .with_test_name("basic")
        .with_async_count_fn(|_params| async { query_table_count().await })
        .with_async_init(|| async { init_postgresql_database().await })
        .with_async_wait_ready(|_params| async { wait_for_postgresql_ready().await });

    let runtime = SinkIntegrationRuntime::new(docker_tool, vec![sink_info]);
    runtime.run(true).await
}
