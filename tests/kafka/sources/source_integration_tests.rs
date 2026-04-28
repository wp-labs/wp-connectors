#![cfg(all(feature = "kafka", feature = "external_integration"))]

use std::time::Duration;

use anyhow::Result;
use wp_connectors::kafka::KafkaSourceFactory;

use crate::common::{
    component_tools::DockerComposeTool,
    source::{integration_runtime::SourceIntegrationRuntime, source_info::SourceInfo},
};
use crate::kafka_common::{
    create_kafka_source_config, init_kafka_topic_with_params, produce_topic_messages,
    unique_kafka_topic, wait_for_kafka_ready,
};

#[tokio::test]
#[ignore = "集成测试默认忽略，请按需手动执行"]
async fn test_kafka_source_basic_integration() -> Result<()> {
    let docker_tool = DockerComposeTool::new("tests/kafka/component/docker-compose.yml")?;

    let params = create_kafka_source_config(unique_kafka_topic("wp_kafka_source_basic"));
    let source_info = SourceInfo::new(KafkaSourceFactory, params.clone())
        .with_test_name("basic")
        .with_async_wait_ready(|_params| async move { wait_for_kafka_ready().await })
        .with_async_init(move || {
            let params = params.clone();
            async move { init_kafka_topic_with_params(params).await }
        })
        .with_async_input(|params| async move {
            produce_topic_messages(
                params,
                vec![
                    br#"{"seq":1,"case":"basic"}"#.to_vec(),
                    br#"{"seq":2,"case":"basic"}"#.to_vec(),
                    br#"{"seq":3,"case":"basic"}"#.to_vec(),
                ],
            )
            .await?;
            Ok(3)
        })
        .with_collect_timeout(Duration::from_secs(5))
        .with_poll_interval(Duration::from_millis(100));

    let runtime = SourceIntegrationRuntime::new(docker_tool, vec![source_info]);
    runtime.run(true).await
}
