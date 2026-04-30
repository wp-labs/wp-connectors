# 达梦数据库 Sink 产品需求文档

## 前置说明

- 文档版本：v0.2
- 最后更新：2026-04-21
- 适用范围：`wp-connectors` 中新增 `dmdb` Sink 的产品需求、实现边界与验收标准。
- 输入来源：
  - 原始需求：当前文件中的“达梦数据库 sink 配置”简述。
  - 用户确认：第 11 节“已确认结论”。
  - 代码依据：`src/mysql/sink.rs`、`src/mysql/config.rs`、`src/mysql/factory.rs`、`src/postgres/sink.rs`。
  - 外部依据：达梦官方文档《ODBC 接口》，访问日期 2026-04-20，链接：<https://eco.dameng.com/document/dm/zh-cn/app-dev/c_c%2B%2B_odbc.html>
- 工具说明：本文为 Markdown 文档，结构化代码编辑工具不适用；文档编辑采用 Codex `apply_patch` 降级处理，影响范围仅本文档，回滚方式为 `git restore -- src/dmdb/docs/dmdb_sink.md` 或按 Git 历史恢复。
- 已确认说明：本期目标环境为 Linux + DM8；同时支持 DSN 与无 DSN 连接方式；写入失败采用严格失败语义，单次 `sink_records` 整批事务回滚并打印日志；不自动建表；当前不关注 40 万条/秒压测目标；暂无必须支持的达梦特殊类型；无指定 Rust ODBC 库或内部 SDK 约束。

## 1. 背景

达梦数据库是关系型数据库管理系统。`wp-connectors` 需要提供达梦数据库 Sink，将上游 Source 或处理链路输出的 `DataRecord` 批量写入达梦表，支撑国产数据库落地场景。

当前仓库已有 MySQL/Postgres Sink，其核心行为是：

- 接收 `AsyncRecordSink::sink_record` / `sink_records` 输入。
- 按配置的目标表和列名生成 `INSERT ... VALUES ...` 批量写入语句。
- 对 `DataType::Ignore` 字段不写入。
- 缺失字段写入 `NULL` 并记录告警。
- Raw 文本和 Raw 字节输入不被数据库 Sink 接受。

达梦 Sink 应优先复用这些已存在的接口语义，避免为单一数据库新增上游使用方式。

## 2. 产品目标

### 2.1 业务目标

- 为 `wp-connectors` 增加 `dmdb` Sink 能力，使数据可写入达梦数据库指定表。
- 支持通过 ODBC 驱动连接达梦数据库，本期仅要求适配 Linux 部署。
- 提供批量写入、超时控制和失败整批回滚能力，满足常规批量入库场景。
- 与现有 MySQL Sink 的配置与字段映射体验保持一致，降低迁移成本。

### 2.2 成功标准

- 功能可用：可通过 `kind = "dmdb"` 构建 Sink，并将 `DataRecord` 写入达梦目标表。
- 配置清晰：必填参数、可选参数、范围校验和错误提示明确。
- 批量高效：支持按批次写入，避免单条记录单次网络往返。
- 可靠可观测：连接失败、SQL 执行失败、字段缺失、事务回滚等场景有明确错误和日志。
- 可验证：提供单元测试、配置校验测试和可选外部集成测试。

## 3. 用户与使用场景

### 3.1 目标用户

- 使用 `wp-engine` / `wp-connectors` 搭建数据同步链路的工程师。
- 需要将日志、业务事件或结构化数据写入达梦数据库的交付团队。
- 需要在国产化数据库环境中做批量数据入库的运维与数据平台团队。

### 3.2 典型场景

- 将 Kafka、HTTP、文件或数据库 Source 输出的数据导入达梦业务表。
- 将转换后的结构化事件按字段映射写入审计表、指标明细表或归档表。
- 高峰期存在突增流量，需要通过批量写入降低单条写入开销。
- 网络抖动或数据库短暂不可用时，Sink 应返回明确错误，由上游决定是否重试。

## 4. 范围

### 4.1 本期范围

- 新增 `dmdb` Sink，不包含 `dmdb` Source。
- 支持 `AsyncRecordSink` 写入结构化 `DataRecord`。
- 支持 ODBC 连接达梦数据库。
- 支持目标 schema、目标表、列映射、批大小和超时参数。
- 支持批量写入，当前实现使用单条 SQL 多组 `VALUES`。
- 支持严格失败、错误分类和单次 `sink_records` 事务回滚；不支持忽略失败或自动冲突更新。
- 支持本地单元测试，以及需要达梦实例和 ODBC 驱动的外部集成测试。

### 4.2 非本期范围

- 不实现达梦 Source。
- 不负责自动创建数据库、用户、表、索引或分区。
- 不实现跨表路由、动态 DDL 或 Schema 自动迁移。
- 不实现 Exactly Once 端到端语义。
- 不实现 Prometheus/OpenTelemetry 等重型观测体系。
- 不上传敏感连接信息到外部服务。

## 5. 功能需求

### 5.1 Sink 注册与构建

- 应新增 `dmdb` feature，并在启用后导出 `DmdbSink` 与 `DmdbSinkFactory`。
- `DmdbSinkFactory.kind()` 应返回 `dmdb`。
- 应提供 `SinkDefProvider`，用于声明参数元数据和来源标识；除明确列出的可选运行参数外，不得为连接信息、认证信息或目标表隐式填充默认值。
- 构建失败时应返回 `SinkError`，错误信息需包含可定位的参数名或连接阶段。

### 5.2 连接能力

- 必须支持通过 ODBC 驱动连接达梦数据库。
- 应支持 DSN 连接方式，便于复用系统级 ODBC 数据源配置。
- 应支持无 DSN 连接串方式，便于容器化或自动化部署。
- 应提供建连超时配置。
- `reconnect()` 应重建 ODBC 连接。
- `stop()` 不做额外资源编排，连接随 Sink 生命周期释放。

达梦官方文档依据：达梦提供 ODBC 3.0 接口；DM ODBC 3.0 支持 DM 数据库 8.0 及以上版本；文档包含 Linux/Windows 数据源配置和 ODBC 连接池章节。本期仅以其中 Linux ODBC 连接能力作为实现依据，Windows 与连接池内容只作为后续扩展参考。

### 5.3 连接复用

- 当前实现维护单个 ODBC 连接，避免每批数据重新建立连接。
- 本期不提供 `max_connections`、`min_connections`、连接最大生命周期或空闲连接回收等连接池参数。
- 如后续重新提出高并发写入或连接池需求，应单独设计连接池能力与压测验收口径。

### 5.4 字段映射

- 必须支持 `columns` 参数声明目标表写入列顺序。
- `DataRecord` 中字段名与 `columns` 按名称匹配。
- `DataType::Ignore` 字段不得写入数据库。
- 当 `columns` 中的字段在 `DataRecord` 中不存在时，应写入 `NULL`，并记录字段缺失告警。
- 当 `DataRecord` 中存在但 `columns` 未声明的字段时，应忽略，不应报错。
- 字段顺序必须以 `columns` 为准，不能依赖 `DataRecord` 内部顺序。

### 5.5 数据类型转换

- 本期应先支持与 MySQL Sink 一致的基础转换：将字段值转换为字符串或数据库参数值后写入。
- 推荐实现优先使用参数绑定，避免手工拼接 SQL 带来的转义和注入风险。

若因驱动限制临时使用 SQL 拼接，必须至少覆盖单引号转义、`NULL`、标识符引用和错误 SQL 脱敏。

- 当前暂无必须支持的达梦特殊类型；基础类型映射测试应优先覆盖：
  - 字符串：`VARCHAR`、`CHAR`、`TEXT` 类字段。
  - 数值：整数、浮点、定点数。
  - 时间：日期、时间戳。
  - 布尔：按达梦目标字段能力映射为数值或字符。
  - 空值：缺失字段或显式空值写入 `NULL`。

### 5.6 批量写入

- `sink_record` 应复用 `sink_records`，保持单条与批量路径一致。
- `sink_records` 收到空批次时应直接返回成功。
- 应支持 `batch_size` 控制单次写入最大记录数。
- 当上游传入批次超过 `batch_size` 时，应在 Sink 内部分片写入。
- 当前实现使用 `INSERT INTO table (columns...) VALUES (...), (...), ...`。
- 单次 `sink_records` 调用内的所有分片共用一个事务：全部成功后提交；任意分片失败则整批回滚、记录错误并返回失败。
- 当前事务实现集中在 `execute_statements_in_transaction`：关闭 ODBC `autocommit`，顺序执行本次 `sink_records` 生成的全部 SQL，全部成功后 `commit` 并恢复 `autocommit`；执行或提交失败时进入 `rollback_and_restore_autocommit`。
- 提交成功后如果恢复 `autocommit` 失败，仅记录告警并保持本次写入成功返回，避免上游误判失败后重试造成重复写入。
- 执行或提交失败后，如果回滚失败，会返回同时包含原始错误与回滚错误的错误信息，并保持保守策略，不再尝试恢复 `autocommit`，避免不确定事务被误提交。
- 如后续切换为 ODBC 参数数组绑定，必须保持同样的整批事务回滚语义。

### 5.7 高吞吐性能

- 40 万条/秒不作为本期关注目标或验收指标。
- 本期性能目标聚焦功能正确、批量写入稳定、失败整批回滚且错误可定位。
- 如后续重新关注高吞吐，应另行明确记录大小、字段数量、索引情况、数据库资源和并发模型。

### 5.8 幂等与重复数据

- MySQL Sink 当前使用 `INSERT IGNORE` 降低重试导致的主键/唯一键冲突风险。
- 经需求确认，达梦 Sink 本期不支持忽略失败或冲突更新逻辑。
- 达梦 Sink 统一采用严格失败语义：
  - 普通插入成功则提交事务并返回成功。
  - 主键/唯一键冲突、SQL 执行失败、类型转换失败或连接异常时，直接返回错误。
  - 单次 `sink_records` 内任意分片失败时，已执行分片整批回滚，后续分片不再执行。
  - 失败日志保留，用于上游排查和重试决策。
- 当前版本不提供 `write_mode`、`ignore_conflict` 或 `upsert` 参数。
- 文档和实现不得默认承诺 Exactly Once。

### 5.9 错误处理

- 应区分参数错误、连接错误、SQL 构建错误、SQL 执行错误、数据类型转换错误。
- 参数错误和 SQL 语法错误默认不可重试。
- 本期不在 Sink 内部做自动重试；网络抖动、连接失效、数据库短暂不可用等错误直接返回给上游。
- 上游如需重试，应基于返回错误自行决策。
- 不得吞错，不得自动忽略冲突。

### 5.10 原始数据输入

- 与 MySQL/Postgres Sink 保持一致，`dmdb` Sink 不接受 Raw 文本或 Raw 字节输入。
- `sink_str`、`sink_bytes`、`sink_str_batch`、`sink_bytes_batch` 应返回明确错误。

## 6. 配置需求

### 6.1 参数表

| 参数                     | 类型        | 必填                           | 默认值    | 说明                                                  |
| ---------------------- | --------- | ---------------------------- | ------ | --------------------------------------------------- |
| `connection_string`    | string    | 否                            | 无      | 完整 ODBC 连接串；若提供，优先级高于拆分字段。                          |
| `endpoint`             | string    | 仅 endpoint 模式是               | 无      | 达梦地址，建议格式 `host:port`；优先级高于 `dsn`。                  |
| `dsn`                  | string    | 仅 DSN 模式是                    | 无      | ODBC 数据源名称；仅在未提供 `connection_string` 和 `endpoint` 时使用。 |
| `driver`               | string    | 仅 endpoint 模式是               | 无      | 达梦 ODBC 驱动名称。                                       |
| `username`             | string    | connection\_string 模式否，其他模式是 | 无      | 数据库用户名。                                             |
| `password`             | string    | connection\_string 模式否，其他模式是 | 无      | 数据库密码。                                              |
| `database`             | string    | 否                            | 无      | 保留字段；当前不参与连接串拼装，也不参与 SQL 路由。                         |
| `schema`               | string    | 否                            | 无      | 目标 Schema。                                          |
| `table`                | string    | 是                            | 无      | 目标表名。                                               |
| `columns`              | string\[] | 是                            | 无      | 目标表列名，顺序即写入顺序。                                      |
| `batch_size`           | integer   | 否                            | `1024` | 单次写入最大记录数，必须大于 0。                                   |
| `connect_timeout_secs` | integer   | 否                            | `8`    | 建连超时。                                               |
| `query_timeout_secs`   | integer   | 否                            | 无      | SQL 执行超时。                                            |

### 6.2 配置示例

```toml
[[sinks]]
name = "dmdb_events"
kind = "dmdb"

[sinks.params]
endpoint = "127.0.0.1:5236"
driver = "DM8 ODBC DRIVER"
username = "SYSDBA"
password = "${DMDB_PASSWORD}"
schema = "WP_DATA"
table = "EVENTS"
columns = ["event_id", "event_time", "source", "payload"]
batch_size = 5000
connect_timeout_secs = 8
query_timeout_secs = 15
```

DSN 模式示例：

```toml
[[sinks]]
name = "dmdb_events"
kind = "dmdb"

[sinks.params]
dsn = "DM8_LOCAL"
username = "SYSDBA"
password = "${DMDB_PASSWORD}"
schema = "WP_DATA"
table = "EVENTS"
columns = ["event_id", "event_time", "source", "payload"]
batch_size = 5000
query_timeout_secs = 15
```

## 7. 非功能需求

### 7.1 性能

- 空批次处理耗时应接近常数级，不访问数据库。
- 单批 SQL 或参数绑定载荷大小应可控，避免超出驱动或数据库限制。
- 在有效配置下应支持稳定批量写入；后续高吞吐场景需另行压测和调优 `batch_size`。
- 40 万条/秒不作为本期压测目标。

### 7.2 可用性

- 数据库短暂断连后，`reconnect()` 应可恢复连接。
- 重连失败时应返回明确错误。
- 不在 Sink 内部无限阻塞或自动吞错。

### 7.3 可维护性

- 模块结构建议为：
  - `src/dmdb/mod.rs`
  - `src/dmdb/config.rs`
  - `src/dmdb/factory.rs`
  - `src/dmdb/sink.rs`
  - `src/dmdb/docs/dmdb_sink.md`
- 配置解析、SQL 构建、数据类型转换和 ODBC 执行应拆分，便于单元测试。
- 公共逻辑如字段映射可考虑与 MySQL/Postgres Sink 抽象复用，但不得牺牲清晰性。

### 7.4 安全

- 不得将用户输入的表名、列名直接无校验拼接到 SQL。
- 推荐对标识符做白名单校验或安全引用。
- 推荐使用参数绑定写入字段值。

### 7.5 可观测性

- 应记录每批写入条数、耗时、失败原因和事务回滚结果。
- 应记录建连失败、重连失败、字段缺失等关键事件。
- 不引入 Prometheus/OpenTelemetry 作为本期强依赖。

## 8. 验收标准

### 8.1 单元测试

- 配置校验：
  - 缺少连接信息时报错。
  - 缺少用户名/密码时报错。
  - 缺少表名或列名时报错。
  - `batch_size <= 0` 报错。
  - `query_timeout_secs <= 0` 报错。
- SQL 或参数构建：
  - 列顺序按 `columns` 保持。
  - 单引号、空值、缺失字段处理正确。
  - `DataType::Ignore` 字段不写入。
  - 空批次不生成 SQL。
- 事务语义：
  - 单次 `sink_records` 内任意分片失败时触发事务回滚。
  - 提交成功后恢复 `autocommit` 失败不应改变本次写入成功结果。
  - 回滚失败时错误信息应同时保留原始失败原因和回滚失败原因。
- Raw 输入：
  - 文本和字节 Raw Sink 方法返回明确错误。

### 8.2 集成测试

- 需要真实达梦数据库和 ODBC 驱动，建议使用 feature 或环境变量显式开启，避免默认 `cargo test` 依赖外部服务。
- 测试内容：
  - 建连成功。
  - 单条写入成功。
  - 批量写入成功。
  - 缺失字段写入 `NULL`。
  - 连接断开后可重连。
  - 错误密码或错误 DSN 返回可理解错误。

### 8.3 性能测试

- 如后续重新关注性能压测，压测前必须固定数据模型、字段数量、记录大小、批大小、客户端并发、数据库资源规格。
- 至少输出：
  - 吞吐：records/s。
  - 延迟：p50、p95、p99。
  - 错误率。
  - 客户端 CPU/内存。
  - 数据库 CPU、内存、磁盘和网络。
- 当前不验证 40 万条/秒。

## 9. 实现建议

- 第一阶段先完成配置、工厂、字段映射、基础批量写入和单元测试。
- 第二阶段接入真实 ODBC 驱动，完成集成测试。
- 第三阶段评估是否需要从多值 `INSERT` 切换为 ODBC 参数数组绑定。
- 第四阶段如重新提出性能目标，再进行专项压测并调整 `batch_size` 和并发建议。

当前无指定 Rust ODBC 库或公司内部达梦 SDK 约束；选型以稳定性、性能和维护活跃度为准。选型时至少比较：

- 是否支持达梦 ODBC 驱动。
- 是否支持参数绑定和批量执行。
- 是否可在 Tokio 异步运行时中安全使用。
- 若后续重新提出连接池需求，是否提供连接池，或能否与现有连接池生态组合。
- 维护活跃度、许可证和跨平台支持。

## 10. 风险与缓解

| 风险                      | 影响             | 缓解                          |
| ----------------------- | -------------- | --------------------------- |
| Rust ODBC 生态与达梦驱动兼容性不确定 | 影响实现周期和稳定性     | 先做最小连接 POC，再进入正式开发。         |
| 后续高吞吐目标依赖数据库资源和表设计       | 可能无法由单 Sink 达成 | 如重新提出性能目标，先明确压测口径。          |
| 手工 SQL 拼接存在转义与注入风险      | 数据错误或安全问题      | 优先参数绑定；标识符白名单校验。            |
| 上游重试可能导致重复写入            | 数据重复或主键冲突      | Sink 内部保持严格失败和事务回滚，由上游决定重试策略。 |
| ODBC 驱动安装和 DSN 配置差异大    | 集成部署复杂         | 同时支持 DSN 和无 DSN 连接串，提供部署文档。 |

## 11. 已确认结论

以下结论来自 2026-04-21 用户确认，后续实现和验收以本节为准：

1. 目标版本：本期按 DM8 作为主要目标版本。更低版本兼容成本取决于 ODBC 驱动、SQL 语法、事务行为和类型映射差异，需单独验证，不纳入本期验收。
2. 部署系统：本期只需要支持 Linux。
3. 连接方式：DSN 和无 DSN 连接方式都需要支持。当前实现优先级为 `connection_string > endpoint > dsn`。
4. `database`、`schema`、`table` 定义：
   - `database` 当前作为保留字段，不参与连接串拼装或 SQL 路由。
   - `schema` 表示目标表所在模式；配置后写入 `"schema"."table"`。
   - `table` 表示目标表名，必须显式配置。
   - 跨 schema 写入是指写入非当前登录用户默认 schema 下的表，例如登录用户为 `SYSDBA`，目标表在 `WP_DATA.EVENTS`。当前实现通过可选 `schema` 参数支持显式 schema 写入。
5. 40 万条/秒性能目标：本期可以不用关注，不作为当前验收标准。
6. 写入失败语义：严格失败。写入失败时整批回滚，返回错误，并打印失败日志。
7. 事务语义：单次 `sink_records` 调用需要事务包裹；任意分片失败时整批回滚。
8. 自动建表或表结构校验：不需要自动建表；表结构由使用方提前准备。
9. 达梦特殊类型：当前没有必须支持的大字段、地理类型、二进制字段等特殊类型。
10. Rust ODBC 库或内部 SDK：没有指定约束，优先选择性能较好、稳定、维护活跃的方案。

## 12. 证据表

```csv
id,type,source,title,version,publish_date,access_date,link,applies_to
E001,code,local,MySQL Sink 实现,workspace,unknown,2026-04-20,src/mysql/sink.rs,字段映射与批量写入参考
E002,code,local,MySQL Sink 配置与工厂,workspace,unknown,2026-04-20,src/mysql/config.rs;src/mysql/factory.rs,配置参数参考
E003,code,local,Postgres Sink 实现,workspace,unknown,2026-04-20,src/postgres/sink.rs,数据库 Sink 接口语义参考
E004,official-doc,达梦官方文档,ODBC 接口,DM8,unknown,2026-04-20,https://eco.dameng.com/document/dm/zh-cn/app-dev/c_c%2B%2B_odbc.html,ODBC 连接方式依据
```
