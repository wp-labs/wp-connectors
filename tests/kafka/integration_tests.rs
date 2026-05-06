#![cfg(all(feature = "kafka", feature = "external_integration"))]

use anyhow::Result;
use wp_connectors::kafka::KafkaSinkFactory;

use crate::common::{
    component_tools::DockerComposeTool,
    sink::{integration_runtime::SinkIntegrationRuntime, sink_info::SinkInfo},
};
use crate::kafka_common::{
    create_kafka_test_scenarios, init_kafka_topic_with_params, query_topic_count,
    wait_for_kafka_ready,
};

#[tokio::test]
#[ignore = "集成测试默认忽略，请按需手动执行"]
async fn test_kafka_sink_full_integration() -> Result<()> {
    let docker_tool = DockerComposeTool::new("tests/kafka/component/docker-compose.yml")?;

    let sink_infos = create_kafka_test_scenarios()
        .into_iter()
        .map(|(test_name, params)| {
            let init_params = params.clone();
            SinkInfo::new(KafkaSinkFactory, params)
                .with_test_name(test_name)
                .with_async_count_fn(|params| async move { query_topic_count(params).await })
                .with_async_init(move || {
                    let params = init_params.clone();
                    async move { init_kafka_topic_with_params(params).await }
                })
                .with_async_wait_ready(|_params| async move { wait_for_kafka_ready().await })
        })
        .collect();

    let runtime = SinkIntegrationRuntime::new(docker_tool, sink_infos);
    runtime.run(true).await
}
