# Tests Guide

`tests/` 目录提供了一套统一的 sink 集成测试和性能测试框架，用来复用外部依赖管理、sink 构建、数据发送、结果校验和性能统计逻辑。

这套框架的目标是：

- 让不同 connector 的测试结构保持一致
- 让集成测试可以稳定接入 CI
- 让开发者新增 sink 测试时只需要关心测试参数、初始化逻辑和就绪检查

## 目录结构

```text
tests/
├── common/    # 通用工具
│   ├── component_tools.rs  # 外部组件相关工具
│   └── sink/
│       ├── integration_runtime.rs  # 集成测试运行时
│       ├── performance_runtime.rs  # 性能测试运行时
│       └── sink_info.rs            # sink描述信息
├── doris/       # Doris 测试相关配置
├── clickhouse/  # ClickHouse 测试相关配置
├── elasticsearch/  # Elasticsearch 测试相关配置
├── http/        # HTTP 测试相关配置
│   └── .run/    # 运行时文件目录，例如 pid 和日志
├── doris_tests.rs
├── clickhouse_tests.rs
├── elasticsearch_tests.rs
└── http_tests.rs
```

## 框架概览

### CI 集成

- 集成测试会被纳入 CI
- CI 会将普通单元测试和集成测试拆开执行，方便分别观察稳定性和失败原因
- 性能测试默认使用 `#[ignore]`，不会进入普通 CI

### 开发者如何编写测试

开发者编写一个新的 sink 测试时，通常只需要准备三类内容：

1. `ComponentTool`
   - 描述测试依赖环境如何启动、停止、等待启动完成和重启
   - 可以选择 `DockerComposeTool`
   - 也可以选择 `ShellScriptTool`
2. `SinkInfo`
   - 描述使用哪个 `SinkFactory`
   - Sink的连接参数 `ParamMap`
   - 可选配置测试名称 `test_name`
   - 配置初始化逻辑 `init`
   - 如有需要，配置数量校验逻辑 `count_fn`
   - 如有需要，可配置 sink 级 `wait_ready`
3. 运行时
   - 集成测试使用 `SinkIntegrationRuntime`
   - 性能测试使用 `SinkPerformanceRuntime`

也就是说，测试文件本身只需要“组装对象并调用 `run()`”，通用执行流程由框架负责。

## 集成测试

### 编写流程

1. 在 `tests/<connector>/` 下创建 `integration_tests.rs`
2. 如果有公共初始化逻辑，抽到 `tests/<connector>/common.rs`
3. 创建 `ComponentTool`
4. 构造 `SinkInfo`
5. 配置 `count_fn`，
6. 并按需补充 `init`、`test_name` 和 `wait_ready`
7. 创建 `SinkIntegrationRuntime` 并执行 `run()`

示例：

```rust
let component_tool = DockerComposeTool::new("tests/doris/integration_tests.yml")?;
let sink_info = SinkInfo::new(
    DorisSinkFactory,
    create_doris_test_config(),
)
    .with_test_name("basic")
    .with_async_count_fn(|_params| async { query_table_count().await })
    .with_async_init(|| async { init_doris_database().await })
    .with_async_wait_ready(|_params| async { wait_for_doris_ready().await });

let runtime = SinkIntegrationRuntime::new(component_tool, vec![sink_info]);
runtime.run().await?;
```

### 执行流程

集成测试框架会统一完成这些事情：

- 拉起测试依赖环境
- 初始化测试资源
- 创建 sink
- 发送少量固定测试数据
- 校验发送前后数量变化
- 执行一次重启后的可用性验证
- 清理环境

## 性能测试

### 编写流程

1. 在 `tests/<connector>/` 下创建 `performance_tests.rs`
2. 复用同目录下已有的 `common.rs`
3. 创建 `ComponentTool`
4. 构造 `SinkInfo`
5. 如有需要配置 `count_fn`，并按需补充 `init`、`test_name` 和 `wait_ready`
6. 创建 `SinkPerformanceConfig`
7. 创建 `SinkPerformanceRuntime` 并执行 `run()`
8. 给测试打上 `#[ignore]`

示例：

```rust
let component_tool = DockerComposeTool::new("tests/doris/performance_tests.yml")?;
let sink_info = SinkInfo::new(
    DorisSinkFactory,
    create_doris_test_config(),
)
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

### 执行流程

性能测试框架会统一完成这些事情：

- 拉起测试依赖环境
- 初始化测试资源
- 并发创建多个 sink 和 Tokio 任务
- 按批次持续发送数据
- 打印吞吐、CPU、内存等运行指标
- 如配置了 `count_fn`，则在结束后做数量校验；未配置时会跳过该检查
- 清理环境

## Common 工具介绍

### `ComponentTool`

`ComponentTool` 是统一的外部组件生命周期抽象，负责：

- `pull_dependencies()`
- `up()`
- `down()`
- `wait_started()`
- `restart()`
- `setup_and_up()`

### `DockerComposeTool`

`DockerComposeTool` 适合依赖 Docker Compose 的测试场景。

```rust
let tool = DockerComposeTool::new("tests/doris/integration_tests.yml")?;
```

### `ShellScriptTool`

`ShellScriptTool` 适合通过 shell 脚本管理外部测试依赖。

```rust
let tool = ShellScriptTool::new_with_options(
    "tests/http/start_server.sh",
    "tests/http/stop_server.sh",
    Some("tests/http/install_deps.sh"),
    Some("tests/http/wait_ready.sh"),
    ShellScriptRestart::Default,
)?;
```

说明：

- `start` / `stop` 脚本是必选项
- `install` / `ready` 是可选项
- `restart` 使用 `ShellScriptRestart::Default`、`ShellScriptRestart::Script(...)`、`ShellScriptRestart::NoRestart`

### `SinkInfo`

`SinkInfo` 用于描述单个 sink 测试用例，包含：

- `factory`
- `params`
- 可选 `test_name`
- 可选 `init`
- 可选 `count_fn`
- 可选 `wait_ready`

如果配置了 `test_name`，运行时会在遍历开始时输出 `kind + test_name + 编号` 形式的标识。

## 当前示例

目前这套框架已经提供了多个示例：

- Doris 的集成测试和性能测试
- ClickHouse 的集成测试和性能测试
- Elasticsearch 的集成测试和性能测试
- PostgreSQL 的集成测试和性能测试
- VictoriaLogs 的集成测试和性能测试
- HTTP sink 的集成测试和性能测试
- MySQL sink 的集成测试和性能测试

说明：

- HTTP 集成测试使用本地 Python 测试服务
- HTTP 性能测试使用 `tests/http/component/docker-compose.yml` 启动的 nginx 服务端
- HTTP 性能测试会覆盖 basic / auth / gzip 三类场景，并使用服务端总请求数作为统一的 count 校验

## 常用命令

```bash
# 全部集成测试
cargo test --tests --features external_integration integration_tests:: -- --nocapture --ignored

# 全部性能测试
cargo test --release --tests --features external_performance performance_tests:: -- --nocapture --ignored

# Doris 集成测试
cargo test --package wp-connectors --test doris_tests integration_tests::test_doris_sink_full_integration --features doris,external_integration -- --exact --nocapture --ignored

# Doris 性能测试
cargo test --package wp-connectors --test doris_tests performance_tests::test_doris_sink_performance --features doris,external_performance -- --exact --nocapture --ignored

# HTTP 集成测试
cargo test --package wp-connectors --test http_tests --features http,external_integration integration_tests::test_http_sink_full_integration -- --exact --nocapture --ignored

# HTTP 性能测试
cargo test --package wp-connectors --test http_tests --features http,external_performance performance_tests::test_http_sink_performance -- --exact --nocapture --ignored

# Kafka 集成测试
cargo test --package wp-connectors --test kafka_tests --features kafka,external_integration integration_tests::test_kafka_sink_full_integration -- --exact --nocapture --ignored

# Kafka 性能测试
cargo test --package wp-connectors --test kafka_tests --features kafka,external_performance performance_tests::test_kafka_sink_performance -- --exact --nocapture --ignored

# ClickHouse 集成测试
cargo test --package wp-connectors --test clickhouse_tests --features clickhouse,external_integration integration_tests::test_clickhouse_sink_full_integration -- --exact --nocapture --ignored

# ClickHouse 性能测试
cargo test --package wp-connectors --test clickhouse_tests --features clickhouse,external_performance performance_tests::test_clickhouse_sink_performance -- --exact --nocapture --ignored

# MySQL 集成测试
cargo test --package wp-connectors --test mysql_tests --features mysql,external_integration integration_tests::test_mysql_sink_full_integration -- --exact --nocapture --ignored

# MySQL 性能测试
cargo test --package wp-connectors --test mysql_tests --features mysql,external_performance performance_tests::test_mysql_sink_performance -- --exact --nocapture --ignored

# Elasticsearch 集成测试
cargo test --package wp-connectors --test elasticsearch_tests --features elasticsearch,external_integration integration_tests::test_elasticsearch_sink_full_integration -- --exact --nocapture --ignored

# Elasticsearch 性能测试
cargo test --package wp-connectors --test elasticsearch_tests --features elasticsearch,external_performance performance_tests::test_elasticsearch_sink_performance -- --exact --nocapture --ignored

# PostgreSQL 集成测试
cargo test --package wp-connectors --test postgresql_tests --features postgres,external_integration integration_tests::test_postgresql_sink_full_integration -- --exact --nocapture --ignored

# PostgreSQL 性能测试
cargo test --package wp-connectors --test postgresql_tests --features postgres,external_performance performance_tests::test_postgresql_sink_performance -- --exact --nocapture --ignored

# VictoriaLogs 集成测试
cargo test --package wp-connectors --test victorialogs_tests --features victorialogs,external_integration integration_tests::test_victorialogs_sink_full_integration -- --exact --nocapture --ignored

# VictoriaLogs 性能测试
cargo test --package wp-connectors --test victorialogs_tests --features victorialogs,external_performance performance_tests::test_victorialogs_sink_performance -- --exact --nocapture --ignored
```
