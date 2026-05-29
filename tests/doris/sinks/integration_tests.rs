#![cfg(all(feature = "doris", feature = "external_integration"))]
//! Integration tests for Doris sink using the new integration test framework.

use wp_connectors::doris::DorisSinkFactory;

use crate::doris_common::{
    INTEGRATION_DYNAMIC_TABLE_COUNT, create_doris_test_config, create_doris_test_records,
    init_doris_database, query_table_count, wait_for_doris_sink_ready,
};
use wp_connector_test_utils::{
    DockerComposeTool, RuntimeResult, SinkInfo, SinkIntegrationRuntime, ToolResultExt,
};

/// 完整的 Doris 集成测试
/// 运行测试:
#[tokio::test]
#[ignore = "集成测试默认忽略，请按需手动执行"]
async fn test_doris_sink_full_integration() -> RuntimeResult<()> {
    // 1. 创建 Docker Compose 工具
    let docker_tool =
        DockerComposeTool::new("tests/doris/component/integration_tests.yml").into_rt()?;

    // 使用框架的生命周期和重启验证，只覆盖测试记录生成逻辑。
    let sink_info = SinkInfo::new(DorisSinkFactory, create_doris_test_config())
        .with_test_name("dynamic_table")
        .with_async_count_fn(|_params| async { query_table_count().await })
        .with_async_wait_ready(|_params| async { wait_for_doris_sink_ready().await })
        .with_async_init(|| async { init_doris_database().await })
        .with_record_builder(|start_id, count| {
            create_doris_test_records(
                start_id,
                count,
                INTEGRATION_DYNAMIC_TABLE_COUNT,
                "integration_test",
            )
        });

    let runtime = SinkIntegrationRuntime::new(docker_tool, vec![sink_info]);
    runtime.run(false).await?;

    println!("\n✅ Doris 集成测试完成！");
    Ok(())
}
