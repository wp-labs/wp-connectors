#![cfg(all(feature = "doris", feature = "external_integration"))]
//! Integration tests for Doris sink using the new integration test framework.

use anyhow::Result;
use wp_connectors::doris::DorisSinkFactory;

use crate::common::{
    component_tools::DockerComposeTool,
    sink::{integration_runtime::SinkIntegrationRuntime, sink_info::SinkInfo},
};
use crate::doris_common::{
    create_doris_test_config, init_doris_database, query_table_count, wait_for_doris_sink_ready,
};

/// 完整的 Doris 集成测试
/// 运行测试:
#[tokio::test]
#[ignore = "集成测试默认忽略，请按需手动执行"]
async fn test_doris_sink_full_integration() -> Result<()> {
    // 1. 创建 Docker Compose 工具
    let docker_tool = DockerComposeTool::new("tests/doris/component/integration_tests.yml")?;

    // 2. 创建 Sink 集成测试信息
    let sink_info = SinkInfo::new(DorisSinkFactory, create_doris_test_config())
        .with_test_name("basic")
        .with_async_count_fn(|_params| async { query_table_count().await })
        .with_async_wait_ready(|_params| async { wait_for_doris_sink_ready().await })
        .with_async_init(|| async { init_doris_database().await });

    // 3. 创建运行时并执行测试
    let runtime = SinkIntegrationRuntime::new(docker_tool, vec![sink_info]);
    runtime.run(true).await?;

    println!("\n✅ Doris 集成测试完成！");
    Ok(())
}
