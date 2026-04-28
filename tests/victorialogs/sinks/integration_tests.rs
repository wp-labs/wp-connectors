#![cfg(all(feature = "victorialogs", feature = "external_integration"))]

use anyhow::Result;
use wp_connectors::victorialogs::VictoriaLogSinkFactory;

use crate::common::{
    component_tools::DockerComposeTool,
    sink::{integration_runtime::SinkIntegrationRuntime, sink_info::SinkInfo},
};
use crate::victorialogs_common::{
    create_vlogs_test_config, init_vlogs_state, query_vlogs_count, wait_for_vlogs_ready,
};

#[tokio::test]
#[ignore = "集成测试默认忽略，请按需手动执行"]
async fn test_victorialogs_sink_full_integration() -> Result<()> {
    let docker_tool = DockerComposeTool::new("tests/victorialogs/component/docker-compose.yml")?;

    let sink_info = SinkInfo::new(VictoriaLogSinkFactory, create_vlogs_test_config())
        .with_test_name("basic")
        .with_async_count_fn(|params| async move { query_vlogs_count(params).await })
        .with_async_init(|| async { init_vlogs_state().await })
        .with_async_wait_ready(|_params| async { wait_for_vlogs_ready().await });

    let runtime = SinkIntegrationRuntime::new(docker_tool, vec![sink_info]);
    runtime.run(true).await
}
