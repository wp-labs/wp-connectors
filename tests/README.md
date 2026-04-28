# Tests Guide

`tests/` 目录现在提供两套通用框架：

- Sink 集成测试和性能测试框架
- Source 集成测试框架

它们复用相同的外部依赖管理能力，把 connector 测试统一收敛到相同目录结构和运行方式中。

这套框架的目标是：

- 让不同 connector 的测试结构保持一致
- 让集成测试可以稳定接入 CI 或本地手动执行
- 让开发者新增测试时只需要关心参数、初始化、输入动作和结果校验

## 目录结构

```text
tests/
├── common/
│   ├── component_tools.rs      # 外部组件生命周期管理
│   ├── sink/
│   │   ├── integration_runtime.rs
│   │   ├── performance_runtime.rs
│   │   └── sink_info.rs
│   └── source/
│       ├── integration_runtime.rs
│       ├── mod.rs
│       └── source_info.rs
├── kafka/
│   ├── common.rs
│   ├── component/
│   │   └── docker-compose.yml
│   ├── sinks/
│   │   ├── integration_tests.rs
│   │   └── performance_tests.rs
│   └── sources/
│       └── source_integration_tests.rs
├── doris/
│   ├── common.rs
│   ├── sinks/
│   │   ├── integration_tests.rs
│   │   └── performance_tests.rs
│   └── sources/
│       └── .gitkeep
├── clickhouse/
│   ├── common.rs
│   ├── sinks/
│   └── sources/
├── http/
│   ├── common.rs
│   ├── component/
│   ├── sinks/
│   └── sources/
├── *_tests.rs                  # 各 connector 的测试入口
└── README.md
```

说明：

- `tests/<connector>/sinks/` 用于放置 sink 集成测试和性能测试
- `tests/<connector>/sources/` 用于放置 source 集成测试
- 即使某个 connector 暂时没有 source 测试，也建议保留 `sources/` 目录，方便后续扩展
- `tests/<connector>/common.rs` 用于放该 connector 的公共配置、初始化逻辑和辅助函数

## 框架概览

### CI 集成

- 集成测试会被纳入 CI 或单独手动执行
- 性能测试默认使用 `#[ignore]`，不会进入普通 CI
- Source 集成测试默认也使用 `#[ignore]`，按需单独运行

### 通用组件工具

所有外部依赖都通过 `ComponentTool` 管理，负责：

- `pull_dependencies()`
- `up()`
- `down()`
- `wait_started()`
- `restart()`
- `setup_and_up()`

可选实现：

- `DockerComposeTool`
- `ShellScriptTool`

## Sink 测试框架

### 开发者需要准备的对象

新增一个 sink 测试时，通常只需要准备三类内容：

1. `ComponentTool`
2. `SinkInfo`
3. 对应运行时：
   - `SinkIntegrationRuntime`：集成测试
   - `SinkPerformanceRuntime`：性能测试

### Sink 集成测试

文件位置约定：

- `tests/<connector>/sinks/integration_tests.rs`

编写流程：

1. 如有公共逻辑，抽到 `tests/<connector>/common.rs`
2. 创建 `ComponentTool`
3. 构造 `SinkInfo`
4. 按需配置 `init`、`wait_ready`、`count_fn`、`test_name`
5. 创建 `SinkIntegrationRuntime` 并执行 `run(true)`

示例：

```rust
let component_tool = DockerComposeTool::new("tests/doris/component/docker-compose.yml")?;
let sink_info = SinkInfo::new(DorisSinkFactory, create_doris_test_config())
    .with_test_name("basic")
    .with_async_count_fn(|_params| async { query_table_count().await })
    .with_async_init(|| async { init_doris_database().await })
    .with_async_wait_ready(|_params| async { wait_for_doris_ready().await });

let runtime = SinkIntegrationRuntime::new(component_tool, vec![sink_info]);
runtime.run(true).await?;
```

统一执行流程：

- 拉起测试依赖环境
- 初始化测试资源
- 创建 sink
- 发送少量固定测试数据
- 校验发送前后数量变化
- 执行一次重启后的可用性验证
- 清理环境

### Sink 性能测试

文件位置约定：

- `tests/<connector>/sinks/performance_tests.rs`

编写流程：

1. 复用同目录的 `common.rs`
2. 创建 `ComponentTool`
3. 构造 `SinkInfo`
4. 按需配置 `count_fn`、`init`、`wait_ready`、`test_name`
5. 创建 `SinkPerformanceConfig`
6. 创建 `SinkPerformanceRuntime` 并执行 `run()`
7. 给测试打上 `#[ignore]`

示例：

```rust
let component_tool = DockerComposeTool::new("tests/doris/component/docker-compose.yml")?;
let sink_info = SinkInfo::new(DorisSinkFactory, create_doris_test_config())
    .with_test_name("baseline")
    .with_async_count_fn(|_params| async { query_table_count().await })
    .with_async_init(|| async { init_doris_database().await })
    .with_async_wait_ready(|_params| async { wait_for_doris_ready().await });

let config = SinkPerformanceConfig::new()
    .with_total_records(2_000_000)
    .with_task_count(4)
    .with_batch_size(10_000);

let runtime = SinkPerformanceRuntime::new(component_tool, vec![sink_info], config);
runtime.run().await?;
```

统一执行流程：

- 拉起测试依赖环境
- 初始化测试资源
- 并发创建多个 sink 和 Tokio 任务
- 按批次持续发送数据
- 打印吞吐、CPU、内存等运行指标
- 如配置了 `count_fn`，则在结束后做数量校验；未配置时跳过
- 清理环境

## Source 集成测试框架

### 设计原则

Source 集成测试统一采用下面的黑盒流程：

1. 启动外部组件
2. 等待组件和 connector 就绪
3. 执行初始化逻辑
4. 构建 `SourceFactory`
5. 执行一次输入动作 `input`
6. 在固定超时窗口内循环调用 `receive()` 收集事件
7. 超时后统一执行断言 `assert`
8. 如开启重启验证，则重启后重复一次

当前 Source 框架不区分 push / pull 型 connector，也不按 EOF 或条数提前结束，而是统一使用时间窗口收集数据。

### 相关对象

- `SourceInfo`
  - 描述单个 source 测试用例
  - 关键配置包括：`factory`、`params`、`test_name`、`init`、`wait_ready`、`input`、`collect_timeout`、`poll_interval`、`assert`、`restart_verification`
- `SourceIntegrationRuntime`
  - 负责通用执行流程
- `SourceRunContext`
  - 在断言阶段提供完整上下文，包含：`received_events`、`receive_attempts`、`idle_count`、`eof_count`、`elapsed`、`phase` 等

### 文件位置约定

- `tests/<connector>/sources/source_integration_tests.rs`

如后续某个 connector 有多个 source 场景，也可以继续拆成多个 source 测试文件，但建议都放在 `sources/` 下。

### 编写流程

1. 在 `tests/<connector>/common.rs` 中准备输入动作、就绪检查和初始化逻辑
2. 在 `tests/<connector>/sources/source_integration_tests.rs` 中创建 `SourceInfo`
3. 配置：
   - `with_async_wait_ready(...)`
   - `with_async_init(...)`
   - `with_async_input(...)`
   - `with_collect_timeout(...)`
   - `with_poll_interval(...)`
   - `with_async_assert(...)`
   - `with_restart_verification(...)`
4. 创建 `SourceIntegrationRuntime` 并执行 `run(true)`

### 示例

下面是当前 Kafka Source 的简化示例：

```rust
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
            ],
        )
        .await
    })
    .with_collect_timeout(Duration::from_secs(5))
    .with_poll_interval(Duration::from_millis(100))
    .with_async_assert(|ctx| async move {
        anyhow::ensure!(ctx.received_events.len() >= 2, "收集到的事件不足");
        Ok(())
    });

let runtime = SourceIntegrationRuntime::new(docker_tool, vec![source_info]);
runtime.run(true).await?;
```

### Source 运行时行为

`SourceIntegrationRuntime` 会统一处理：

- 启动组件
- build source
- 调用输入动作
- 在超时窗口内轮询 `receive()`
- 将 `NotData` 和 `EOF` 视为可接受的空闲信号
- 将其它错误视为测试失败
- 关闭所有 `SourceHandle`
- 把收集结果交给断言函数

## 当前示例

目前仓库已经提供：

- Doris sink 集成测试和性能测试
- ClickHouse sink 集成测试和性能测试
- Elasticsearch sink 集成测试和性能测试
- PostgreSQL sink 集成测试和性能测试
- VictoriaLogs sink 集成测试和性能测试
- HTTP sink 集成测试和性能测试
- MySQL sink 集成测试和性能测试
- Kafka sink 集成测试和性能测试
- Kafka source 集成测试

说明：

- HTTP 集成测试使用本地 Python 测试服务
- HTTP 性能测试使用 `tests/http/component/docker-compose.yml` 启动的 nginx 服务端
- HTTP 性能测试会覆盖 basic / auth / gzip 三类场景，并使用服务端总请求数作为统一的 count 校验

## 常用命令

```bash
# 全部 sink 集成测试
cargo test --tests --features external_integration integration_tests:: -- --nocapture --ignored --test-threads=1

# 全部 sink 性能测试
cargo test --release --tests --features external_performance performance_tests:: -- --nocapture --ignored

# Kafka source 集成测试
cargo test --package wp-connectors --test kafka_tests --features kafka,external_integration source_integration_tests::test_kafka_source_basic_integration -- --exact --nocapture --ignored

# Kafka sink 集成测试
cargo test --package wp-connectors --test kafka_tests --features kafka,external_integration integration_tests::test_kafka_sink_full_integration -- --exact --nocapture --ignored

# Kafka sink 性能测试
cargo test --package wp-connectors --test kafka_tests --features kafka,external_performance performance_tests::test_kafka_sink_performance -- --exact --nocapture --ignored

# Doris sink 集成测试
cargo test --package wp-connectors --test doris_tests integration_tests::test_doris_sink_full_integration --features doris,external_integration -- --exact --nocapture --ignored

# Doris sink 性能测试
cargo test --package wp-connectors --test doris_tests performance_tests::test_doris_sink_performance --features doris,external_performance -- --exact --nocapture --ignored

# HTTP sink 集成测试
cargo test --package wp-connectors --test http_tests --features http,external_integration integration_tests::test_http_sink_full_integration -- --exact --nocapture --ignored

# HTTP sink 性能测试
cargo test --package wp-connectors --test http_tests --features http,external_performance performance_tests::test_http_sink_performance -- --exact --nocapture --ignored

# ClickHouse sink 集成测试
cargo test --package wp-connectors --test clickhouse_tests --features clickhouse,external_integration integration_tests::test_clickhouse_sink_full_integration -- --exact --nocapture --ignored

# ClickHouse sink 性能测试
cargo test --package wp-connectors --test clickhouse_tests --features clickhouse,external_performance performance_tests::test_clickhouse_sink_performance -- --exact --nocapture --ignored

# MySQL sink 集成测试
cargo test --package wp-connectors --test mysql_tests --features mysql,external_integration integration_tests::test_mysql_sink_full_integration -- --exact --nocapture --ignored

# MySQL sink 性能测试
cargo test --package wp-connectors --test mysql_tests --features mysql,external_performance performance_tests::test_mysql_sink_performance -- --exact --nocapture --ignored

# Elasticsearch sink 集成测试
cargo test --package wp-connectors --test elasticsearch_tests --features elasticsearch,external_integration integration_tests::test_elasticsearch_sink_full_integration -- --exact --nocapture --ignored

# Elasticsearch sink 性能测试
cargo test --package wp-connectors --test elasticsearch_tests --features elasticsearch,external_performance performance_tests::test_elasticsearch_sink_performance -- --exact --nocapture --ignored

# PostgreSQL sink 集成测试
cargo test --package wp-connectors --test postgresql_tests --features postgres,external_integration integration_tests::test_postgresql_sink_full_integration -- --exact --nocapture --ignored

# PostgreSQL sink 性能测试
cargo test --package wp-connectors --test postgresql_tests --features postgres,external_performance performance_tests::test_postgresql_sink_performance -- --exact --nocapture --ignored

# VictoriaLogs sink 集成测试
cargo test --package wp-connectors --test victorialogs_tests --features victorialogs,external_integration integration_tests::test_victorialogs_sink_full_integration -- --exact --nocapture --ignored

# VictoriaLogs sink 性能测试
cargo test --package wp-connectors --test victorialogs_tests --features victorialogs,external_performance performance_tests::test_victorialogs_sink_performance -- --exact --nocapture --ignored
```
