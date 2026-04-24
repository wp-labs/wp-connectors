#![cfg(all(feature = "doris", feature = "external_performance"))]

use anyhow::Result;
use wp_connectors::doris::DorisSinkFactory;

use crate::common::{
    component_tools::DockerComposeTool,
    sink::{
        performance_runtime::{SinkPerformanceConfig, SinkPerformanceRuntime},
        sink_info::SinkInfo,
    },
};
use crate::doris_common::{
    create_doris_test_config, init_doris_database, query_table_count, wait_for_doris_sink_ready,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "性能测试默认忽略，请按需手动执行"]
// 执行命令: cargo test --release --package wp-connectors --test doris_tests --features doris,external_performance performance_tests::test_doris_sink_performance -- --exact --nocapture
async fn test_doris_sink_performance() -> Result<()> {
    let docker_tool = DockerComposeTool::new("tests/doris/component/performance_tests.yml")?;
    // 添加sink信息、包括sink工厂、测试的初始化方法，基础方法
    let sink_info = SinkInfo::new(DorisSinkFactory, create_doris_test_config())
        .with_test_name("baseline")
        .with_async_count_fn(|_params| async { query_table_count().await })
        .with_async_init(|| async { init_doris_database().await })
        .with_async_wait_ready(|_params| async { wait_for_doris_sink_ready().await });
    //性能测试配置
    let config = SinkPerformanceConfig::default()
        .with_total_records(1000_0000)
        //批量大小
        .with_batch_size(1_0000)
        //并行度
        .with_task_count(4);
    let runtime = SinkPerformanceRuntime::new(docker_tool, vec![sink_info], config);
    runtime.run().await
}
