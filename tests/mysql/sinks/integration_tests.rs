#![cfg(all(feature = "mysql", feature = "external_integration"))]


use wp_connectors::mysql::MySQLSinkFactory;

use wp_connector_test_utils::{
    wp_connector_test_utils::{DockerComposeTool, RuntimeResult, ToolResultExt},
    SinkIntegrationRuntime, SinkInfo,
};
use crate::mysql_common::{
    create_mysql_test_config, init_mysql_database, query_table_count, wait_for_mysql_ready,
};

#[tokio::test]
#[ignore = "集成测试默认忽略，请按需手动执行"]
async fn test_mysql_sink_full_integration() -> RuntimeResult<()> {
    let docker_tool = DockerComposeTool::new("tests/mysql/component/docker-compose.yml").into_rt()?;

    let sink_info = SinkInfo::new(MySQLSinkFactory, create_mysql_test_config())
        .with_test_name("basic")
        .with_async_count_fn(|_params| async { query_table_count().await })
        .with_async_init(|| async { init_mysql_database().await })
        .with_async_wait_ready(|_params| async { wait_for_mysql_ready().await });

    let runtime = SinkIntegrationRuntime::new(docker_tool, vec![sink_info]);
    runtime.run(true).await
}
