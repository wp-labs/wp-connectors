#![cfg(all(feature = "victorialogs", feature = "external_performance"))]

use anyhow::Result;
use wp_connectors::victorialogs::VictoriaLogSinkFactory;

use crate::common::{
    component_tools::DockerComposeTool,
    sink::{
        performance_runtime::{SinkPerformanceConfig, SinkPerformanceRuntime},
        sink_info::SinkInfo,
    },
};
use crate::victorialogs_common::{
    create_vlogs_test_config, init_vlogs_state, query_vlogs_count, wait_for_vlogs_ready,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "性能测试默认忽略，请按需手动执行"]
async fn test_victorialogs_sink_performance() -> Result<()> {
    let docker_tool = DockerComposeTool::new("tests/victorialogs/component/docker-compose.yml")?;

    let sink_info = SinkInfo::new(VictoriaLogSinkFactory, create_vlogs_test_config())
        .with_test_name("baseline")
        .with_async_count_fn(|params| async move { query_vlogs_count(params).await })
        .with_async_init(|| async { init_vlogs_state().await })
        .with_async_wait_ready(|_params| async { wait_for_vlogs_ready().await });

    let config = SinkPerformanceConfig::default()
        .with_batch_size(10_000)
        .with_task_count(4);

    let runtime = SinkPerformanceRuntime::new(docker_tool, vec![sink_info], config);
    runtime.run().await
}
