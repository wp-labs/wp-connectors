#![cfg(all(feature = "postgres", feature = "external_performance"))]

use anyhow::Result;
use wp_connectors::postgres::PostgresSinkFactory;

use crate::common::{
    component_tools::DockerComposeTool,
    sink::{
        performance_runtime::{SinkPerformanceConfig, SinkPerformanceRuntime},
        sink_info::SinkInfo,
    },
};
use crate::postgresql_common::{
    create_postgresql_test_config, init_postgresql_database, query_table_count,
    wait_for_postgresql_ready,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "性能测试默认忽略，请按需手动执行"]
async fn test_postgresql_sink_performance() -> Result<()> {
    let docker_tool = DockerComposeTool::new("tests/postgresql/component/docker-compose.yml")?;

    let sink_info = SinkInfo::new(PostgresSinkFactory, create_postgresql_test_config())
        .with_test_name("baseline")
        .with_async_count_fn(|_params| async { query_table_count().await })
        .with_async_init(|| async { init_postgresql_database().await })
        .with_async_wait_ready(|_params| async { wait_for_postgresql_ready().await });

    let config = SinkPerformanceConfig::default()
        .with_batch_size(5_000)
        .with_task_count(4);

    let runtime = SinkPerformanceRuntime::new(docker_tool, vec![sink_info], config);
    runtime.run().await
}
