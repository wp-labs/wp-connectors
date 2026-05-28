#![cfg(all(feature = "doris", feature = "external_performance"))]

use wp_connectors::doris::DorisSinkFactory;

use crate::doris_common::{
    PERFORMANCE_DYNAMIC_TABLE_COUNT, PERFORMANCE_RECORD_COUNT, create_doris_test_config,
    create_doris_test_records, init_doris_performance_database, query_table_count,
    wait_for_doris_sink_ready,
};
use wp_connector_test_utils::{
    DockerComposeTool, RuntimeResult, SinkInfo, SinkPerformanceConfig, SinkPerformanceRuntime,
    ToolResultExt,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "性能测试默认忽略，请按需手动执行"]
// 执行命令: cargo test --release --package wp-connectors --test doris_tests --features doris,external_performance performance_tests::test_doris_sink_performance -- --exact --nocapture
async fn test_doris_sink_performance() -> RuntimeResult<()> {
    let docker_tool =
        DockerComposeTool::new("tests/doris/component/performance_tests.yml").into_rt()?;

    let sink_info = SinkInfo::new(DorisSinkFactory, create_doris_test_config())
        .with_test_name("dynamic_table")
        .with_async_count_fn(|_params| async { query_table_count().await })
        .with_async_init(|| async { init_doris_performance_database().await })
        .with_async_wait_ready(|_params| async { wait_for_doris_sink_ready().await })
        .with_record_builder(|start_id, count| {
            create_doris_test_records(
                start_id,
                count,
                PERFORMANCE_DYNAMIC_TABLE_COUNT,
                "performance_test",
            )
        });

    let config = SinkPerformanceConfig::default()
        .with_total_records(PERFORMANCE_RECORD_COUNT)
        .with_batch_size(20_000)
        .with_task_count(8);
    let runtime = SinkPerformanceRuntime::new(docker_tool, vec![sink_info], config);
    runtime.run().await?;
    println!("动态表数量{}", PERFORMANCE_DYNAMIC_TABLE_COUNT);
    Ok(())
}
