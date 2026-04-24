#![cfg(all(feature = "mysql", feature = "external_performance"))]

use anyhow::Result;
use std::time::Duration;
use wp_connectors::mysql::MySQLSinkFactory;

use crate::common::{
    component_tools::DockerComposeTool,
    sink::{
        performance_runtime::{SinkPerformanceConfig, SinkPerformanceRuntime},
        sink_info::SinkInfo,
    },
};
use crate::mysql_common::{
    create_mysql_test_config, init_mysql_database, query_table_count, wait_for_mysql_ready,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "性能测试默认忽略，请按需手动执行"]
async fn test_mysql_sink_performance() -> Result<()> {
    let docker_tool = DockerComposeTool::new("tests/mysql/component/docker-compose.yml")?;

    let sink_info = SinkInfo::new(MySQLSinkFactory, create_mysql_test_config())
        .with_test_name("baseline")
        .with_async_count_fn(|_params| async { query_table_count().await })
        .with_async_init(|| async { init_mysql_database().await })
        .with_async_wait_ready(|_params| async { wait_for_mysql_ready().await });

    let config = SinkPerformanceConfig::new()
        .with_task_count(4)
        .with_batch_size(5_000)
        .with_progress_interval(Duration::from_secs(2));

    let runtime = SinkPerformanceRuntime::new(docker_tool, vec![sink_info], config);
    runtime.run().await
}
