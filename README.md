# wp-connectors

![License](https://img.shields.io/badge/License-Apache%202.0-blue)
[![CI](https://github.com/wp-labs/wp-connectors/actions/workflows/ci.yml/badge.svg)](https://github.com/wp-labs/wp-connectors/actions/workflows/ci.yml)
[![Codecov](https://codecov.io/gh/wp-labs/wp-connectors/branch/main/graph/badge.svg)](https://codecov.io/gh/wp-labs/wp-connectors)
![Rust](https://img.shields.io/badge/rust-stable%2Bbeta-orange)

A unified connector library for the wp-flow system, providing Source and Sink support for multiple data systems.

## Supported Connectors

| Connector | Source | Sink | Feature Flag |
|-----------|:------:|:----:|-------------|
| Kafka | ✅ | ✅ | `kafka` (default) |
| MySQL | ✅ | ✅ | `mysql` (default) |
| Doris | - | ✅ | `doris` (default) |
| HTTP | - | ✅ | `http` |
| Elasticsearch | - | ✅ | `elasticsearch` (placeholder) |
| ClickHouse | - | ✅ | `clickhouse` (placeholder) |
| Prometheus | - | Exporter | `prometheus` (default) |
| VictoriaMetrics | - | Exporter | `victoriametrics` (default) |
| VictoriaLogs | - | ✅ | `victorialogs` (default) |

## Quick Start

### Build

```bash
# Default features (all enabled connectors)
cargo build

# Kafka only
cargo build --no-default-features --features kafka

# All features
cargo build --features full
```

### Test

```bash
cargo test

# Skip Kafka integration tests
SKIP_KAFKA_INTEGRATION_TESTS=1 cargo test
```

### Lint

```bash
cargo fmt --all
cargo clippy --all-targets -- -D warnings
```

## Features

| Feature | Description | Default |
|---------|-------------|:-------:|
| `kafka` | Kafka Source/Sink | ✅ |
| `mysql` | MySQL Source/Sink | ✅ |
| `doris` | Doris Sink (HTTP Stream Load) | ✅ |
| `http` | HTTP/HTTPS Sink | - |
| `prometheus` | Prometheus Exporter (actix-web) | ✅ |
| `victoriametrics` | VictoriaMetrics Exporter | ✅ |
| `victorialogs` | VictoriaLogs Sink | ✅ |
| `elasticsearch` | Elasticsearch Sink (placeholder) | - |
| `clickhouse` | ClickHouse Sink (placeholder) | - |
| `full` | Enable all features | - |

## Arrow / warp-fusion Integration

Sinks support a `protocol` parameter for output format selection. Set `protocol: arrow` to output [Arrow IPC Stream](https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format) format instead of text.

**Supported sinks:** `kafka`, `clickhouse`, `doris`

```yaml
# Example: Kafka sink with Arrow output
brokers: localhost:9092
topic: wp_events
protocol: arrow    # Output Arrow IPC Stream bytes
# fmt is ignored when protocol is arrow
```

### Kafka BatchSource

The Kafka source implements `wf_connector_api::BatchSource`, producing Arrow `RecordBatch` for warp-fusion CEP engine consumption. Each batch contains up to 1024 messages with the following schema:

| Column | Type | Nullable |
|--------|------|:--------:|
| `topic` | Utf8 | - |
| `partition` | Int32 | - |
| `offset` | Int64 | - |
| `timestamp` | Int64 | ✅ |
| `key` | Binary | ✅ |
| `payload` | Binary | ✅ |

### Validation

Setting `protocol: arrow` on unsupported sinks (MySQL, Postgres, Elasticsearch, Prometheus, VictoriaMetrics, VictoriaLogs, Count, HTTP) produces a clear validation error listing supported sinks.

### Source direction (Arrow decoding)

Raw-byte **sources** (Kafka, HTTP) now accept a `data_format` parameter that
selects how each payload is decoded into Arrow `RecordBatch`es. This is the
source-side counterpart of the sink `protocol: arrow` output.

| `data_format` | Meaning |
|---------------|---------|
| `ndjson` (default) | Newline-delimited JSON (line-oriented) |
| `arrow_ipc` | Arrow IPC Stream (`StreamReader`-compatible) |
| `arrow_framed` | wp_arrow frame: `[4B tag_len][tag][Arrow IPC Stream]` |

`data_format` is validated strictly at spec parse time — a typo (e.g.
`arrowipcc`) is rejected with a clear error rather than silently degrading to
NDJSON.

To turn a raw-byte `DataSource` into a `RecordBatch`-producing `BatchSource`,
wrap it with `ArrowBatchSource`:

```rust
use wp_connectors::utils::arrow_batch_source::ArrowBatchSource;
use wp_connectors::utils::arrow_decode::WireFormat;

// `source` is any DataSource emitting raw bytes (e.g. KafkaSource, HttpSource)
let batch = ArrowBatchSource::new("my_arrow", source, WireFormat::ArrowStream)?;
```

`ArrowBatchSource` is **Arrow-only**: it rejects `WireFormat::Ndjson` at
construction. NDJSON → typed `RecordBatch` conversion (with schema-driven
columns) is handled by `wp-core-connectors`' `FileBatchSource` /
`TcpBatchSource`. The Arrow schema is taken from each stream itself.

## Project Structure

```
src/
├── lib.rs                 # Entry point, exports modules by feature
├── kafka/                 # Kafka Source/Sink
├── mysql/                 # MySQL Source/Sink
├── doris/                 # Doris Sink
├── elasticsearch/         # Elasticsearch Sink (placeholder)
├── clickhouse/            # ClickHouse Sink (placeholder)
├── prometheus/            # Prometheus Exporter
├── victoriametrics/       # VictoriaMetrics Exporter
└── victorialogs/          # VictoriaLogs Sink
tests/                     # Integration tests
```

Each connector module typically contains:
- `config.rs` — Configuration parsing
- `factory.rs` — Factory pattern for creation and validation
- `sink.rs` / `source.rs` — Data write/read logic

## Usage

```rust
use wp_connectors::kafka::register_factories;

// Register Kafka connector factories
register_factories();
```

### HTTP Sink Example

To use the HTTP sink, enable the `http` feature:

```bash
cargo build --features http
cargo test --features http
```

Register and use the HTTP sink:

```rust
#[cfg(feature = "http")]
use wp_connectors::http::register_factory;

#[cfg(feature = "http")]
register_factory();
```

See `examples/http_sink_example.rs` and `.other/connectors/sink.d/95-http.toml` for complete configuration examples.

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md) for development workflow and contribution guidelines.

## License

[Apache License 2.0](./LICENSE)

---

# wp-connectors（中文）

wp-flow 系统的统一连接器库，提供多种数据源（Source）和数据汇（Sink）的接入支持。

## 支持的连接器

| 连接器 | Source | Sink | Feature Flag |
|--------|:------:|:----:|-------------|
| Kafka | ✅ | ✅ | `kafka`（默认） |
| MySQL | ✅ | ✅ | `mysql`（默认） |
| Doris | - | ✅ | `doris`（默认） |
| HTTP | - | ✅ | `http` |
| Elasticsearch | - | ✅ | `elasticsearch`（占位） |
| ClickHouse | - | ✅ | `clickhouse`（占位） |
| Prometheus | - | 导出器 | `prometheus`（默认） |
| VictoriaMetrics | - | 导出器 | `victoriametrics`（默认） |
| VictoriaLogs | - | ✅ | `victorialogs`（默认） |

## 快速开始

### 构建

```bash
# 默认特性（全部已启用的连接器）
cargo build

# 仅 Kafka
cargo build --no-default-features --features kafka

# 全部特性
cargo build --features full
```

### 测试

```bash
cargo test

# 跳过 Kafka 集成测试
SKIP_KAFKA_INTEGRATION_TESTS=1 cargo test
```

### Lint

```bash
cargo fmt --all
cargo clippy --all-targets -- -D warnings
```

## 功能特性（Features）

| Feature | 说明 | 默认 |
|---------|------|:----:|
| `kafka` | Kafka Source/Sink | ✅ |
| `mysql` | MySQL Source/Sink | ✅ |
| `doris` | Doris Sink（HTTP Stream Load） | ✅ |
| `http` | HTTP/HTTPS Sink | - |
| `prometheus` | Prometheus 导出器（actix-web） | ✅ |
| `victoriametrics` | VictoriaMetrics 导出器 | ✅ |
| `victorialogs` | VictoriaLogs Sink | ✅ |
| `elasticsearch` | Elasticsearch Sink（占位） | - |
| `clickhouse` | ClickHouse Sink（占位） | - |
| `full` | 启用全部特性 | - |

## Arrow / warp-fusion 集成

Sink 支持 `protocol` 参数选择输出格式。设置 `protocol: arrow` 可输出 [Arrow IPC Stream](https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format) 二进制格式。

**支持的 sink：**`kafka`、`clickhouse`、`doris`

```yaml
# 示例：Kafka sink 输出 Arrow 格式
brokers: localhost:9092
topic: wp_events
protocol: arrow    # 输出 Arrow IPC Stream 二进制
# fmt 在 protocol 为 arrow 时无效
```

### Kafka BatchSource

Kafka source 实现了 `wf_connector_api::BatchSource`，为 warp-fusion CEP 引擎提供 Arrow `RecordBatch`。每批最多 1024 条消息，schema 如下：

| 列 | 类型 | 可空 |
|----|------|:----:|
| `topic` | Utf8 | - |
| `partition` | Int32 | - |
| `offset` | Int64 | - |
| `timestamp` | Int64 | ✅ |
| `key` | Binary | ✅ |
| `payload` | Binary | ✅ |

### 参数校验

在不支持的 sink（MySQL、Postgres、Elasticsearch、Prometheus、VictoriaMetrics、VictoriaLogs、Count、HTTP）上设置 `protocol: arrow` 会返回明确的校验错误，并列出支持的 sink。

### Source 方向（Arrow 解码）

原始字节的 **source**（Kafka、HTTP）现在支持 `data_format` 参数，选择每个 payload 如何被解码为 Arrow `RecordBatch`。这是 sink 侧 `protocol: arrow` 输出的对应物。

| `data_format` | 含义 |
|---------------|------|
| `ndjson`（默认） | 换行分隔的 JSON（行式） |
| `arrow_ipc` | Arrow IPC Stream（`StreamReader` 可读） |
| `arrow_framed` | wp_arrow 帧：`[4B tag_len][tag][Arrow IPC Stream]` |

`data_format` 在 spec 解析期被严格校验——拼写错误（如 `arrowipcc`）会返回明确错误，而不是静默退化为 NDJSON。

把任意原始字节 `DataSource` 包装成产出 `RecordBatch` 的 `BatchSource`，使用 `ArrowBatchSource`：

```rust
use wp_connectors::utils::arrow_batch_source::ArrowBatchSource;
use wp_connectors::utils::arrow_decode::WireFormat;

// `source` 是任何输出原始字节的 DataSource（如 KafkaSource、HttpSource）
let batch = ArrowBatchSource::new("my_arrow", source, WireFormat::ArrowStream)?;
```

`ArrowBatchSource` 是 **Arrow 专用**的：构造期会拒绝 `WireFormat::Ndjson`。NDJSON → 类型化 `RecordBatch` 转换（按 schema 构建列）由 `wp-core-connectors` 的 `FileBatchSource` / `TcpBatchSource` 负责。Arrow 的 schema 直接取自每个流本身。

## 项目结构

```
src/
├── lib.rs                 # 入口，按 feature 导出各模块
├── kafka/                 # Kafka Source/Sink
├── mysql/                 # MySQL Source/Sink
├── doris/                 # Doris Sink
├── elasticsearch/         # Elasticsearch Sink（占位）
├── clickhouse/            # ClickHouse Sink（占位）
├── prometheus/            # Prometheus 导出器
├── victoriametrics/       # VictoriaMetrics 导出器
└── victorialogs/          # VictoriaLogs Sink
tests/                     # 集成测试
```

每个连接器模块通常包含：
- `config.rs` — 配置解析
- `factory.rs` — 工厂模式创建和验证
- `sink.rs` / `source.rs` — 数据写入/读取逻辑

## 使用示例

```rust
use wp_connectors::kafka::register_factories;

// 注册 Kafka 连接器工厂
register_factories();
```

### HTTP Sink 示例

要使用 HTTP sink，需启用 `http` 特性：

```bash
cargo build --features http
cargo test --features http
```

注册并使用 HTTP sink：

```rust
#[cfg(feature = "http")]
use wp_connectors::http::register_factory;

#[cfg(feature = "http")]
register_factory();
```

完整配置示例请参见 `examples/http_sink_example.rs` 和 `.other/connectors/sink.d/95-http.toml`。

## 贡献

请参阅 [CONTRIBUTING.md](./CONTRIBUTING.md) 了解开发流程和贡献规范。

## 许可证

[Apache License 2.0](./LICENSE)
