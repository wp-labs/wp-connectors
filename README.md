# wp-connectors

![License](https://img.shields.io/badge/License-Elastic%20License%202.0-blue)
[![CI](https://github.com/wp-labs/wp-connectors/actions/workflows/ci.yml/badge.svg)](https://github.com/wp-labs/wp-connectors/actions/workflows/ci.yml)
[![Codecov](https://codecov.io/gh/wp-labs/wp-connectors/branch/main/graph/badge.svg)](https://codecov.io/gh/wp-labs/wp-connectors)
![Rust](https://img.shields.io/badge/rust-stable%2Bbeta-orange)

## 功能特性与编译选项

特性开关（features）
- default = ["kafka"]：默认仅编译 Kafka 模块
- kafka：Kafka Source/Sink 与工厂（默认开启）
- prometheus：Prometheus 导出器（actix-web + prometheus），需显式开启
- elasticsearch：预留特性位（未来接入子 crate 后启用），目前仅占位

构建命令示例
- 仅 Kafka（默认）：
  - cargo build
  - cargo test
- 仅 Prometheus：
  - cargo build --no-default-features --features prometheus
  - cargo test --no-default-features --features prometheus  # 无测试用例将被编译
- Kafka + Prometheus：
  - cargo build --features prometheus
  - cargo test --features prometheus
- 仅占位（Elasticsearch）：
  - cargo build --no-default-features --features elasticsearch  # 仅启用特性位，不引入依赖

模块导出
- 启用 kafka 特性：`wp_connectors::kafka::{KafkaSourceFactory, KafkaSinkFactory, register_factories, ..}`
- 启用 prometheus 特性：`wp_connectors::prometheus::{register_builder, ..}`
- 启用 elasticsearch 特性（占位）：将来会通过 `wp_connectors::elasticsearch::*` 暴露子 crate 内容

测试
- Kafka 相关测试使用 `#[cfg(feature = "kafka")]` 条件编译；在 `--no-default-features --features prometheus` 下不会被编译
- 如需跳过需要 Kafka 的 E2E 测试，可设置环境变量：`SKIP_KAFKA_INTEGRATION_TESTS=1`
