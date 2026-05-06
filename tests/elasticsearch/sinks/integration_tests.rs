#![cfg(all(feature = "elasticsearch", feature = "external_integration"))]


use wp_connectors::elasticsearch::ElasticsearchSinkFactory;

use wp_connector_test_utils::{
    wp_connector_test_utils::{DockerComposeTool, RuntimeResult, ToolResultExt},
    SinkIntegrationRuntime, SinkInfo,
};
use crate::elasticsearch_common::{
    create_elasticsearch_test_config, init_elasticsearch_index, query_index_count,
    wait_for_elasticsearch_ready,
};

#[tokio::test]
#[ignore = "集成测试默认忽略，请按需手动执行"]
async fn test_elasticsearch_sink_full_integration() -> RuntimeResult<()> {
    let docker_tool = DockerComposeTool::new("tests/elasticsearch/component/docker-compose.yml").into_rt()?;

    let sink_info = SinkInfo::new(ElasticsearchSinkFactory, create_elasticsearch_test_config())
        .with_test_name("basic")
        .with_async_count_fn(|_params| async { query_index_count().await })
        .with_async_init(|| async { init_elasticsearch_index().await })
        .with_async_wait_ready(|_params| async { wait_for_elasticsearch_ready().await });

    let runtime = SinkIntegrationRuntime::new(docker_tool, vec![sink_info]);
    runtime.run(true).await
}
