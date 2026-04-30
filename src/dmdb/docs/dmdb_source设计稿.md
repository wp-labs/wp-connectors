# DMDB Source 设计稿

## 前置说明

- 文档版本：v0.1
- 最后更新：2026-04-27
- 适用范围：`wp-connectors` 中新增 `dmdb` Source 的设计、实现边界与验收建议。
- 输入来源：
  - 参考实现：[src/postgres/source.rs](/Users/tangxiangyan/Documents/projects/wp/git/wp-connectors/src/postgres/source.rs)
  - 配套工厂：[src/postgres/factory.rs](/Users/tangxiangyan/Documents/projects/wp/git/wp-connectors/src/postgres/factory.rs)
  - 现有达梦实现：[src/dmdb/config.rs](/Users/tangxiangyan/Documents/projects/wp/git/wp-connectors/src/dmdb/config.rs)、[src/dmdb/factory.rs](/Users/tangxiangyan/Documents/projects/wp/git/wp-connectors/src/dmdb/factory.rs)、[src/dmdb/sink.rs](/Users/tangxiangyan/Documents/projects/wp/git/wp-connectors/src/dmdb/sink.rs)
- 工具说明：本次产物为新增 Markdown 文档。由于 Serena 不适合对非符号型 Markdown 新文件执行结构化插入与创建，按仓库治理要求降级使用 `apply_patch` 写入，影响范围仅本文档；回滚方式为删除本文档或按 Git 历史恢复。
- 约束说明：
  - 本稿聚焦 `dmdb source` 首版设计，不包含 `dmdb sink` 重构。
  - 本稿默认延续当前 `dmdb` 连接语义：`connection_string > endpoint > dsn`。
  - 本稿默认首版以“单表增量拉取”作为能力边界，不覆盖 CDC、全量快照并行分片或多表路由。
  - 本稿默认首版必须支持 `time` 时间游标，不接受只交付 `int` 游标的裁剪方案。

## 1. 设计目标

`dmdb source` 需要尽量复用 `postgres source` 的成熟模式，在仓库内形成统一的数据库增量采集体验。首版目标如下：

- 提供 `kind = "dmdb"` 的 Source 能力，支持从达梦单表增量轮询读取数据。
- 保持与 `postgres source` 一致的运行心智模型：启动建连、解析游标、按游标分页拉取、输出 `SourceEvent`、持久化 checkpoint。
- 优先复用现有 `dmdb` 模块中的连接配置与 ODBC 建连逻辑，避免重复维护两套连接语义。
- 在不引入额外复杂基础设施的前提下，为后续 `time` 游标、更多类型兼容和性能优化预留扩展位。

## 2. 对齐 `postgres source` 的核心原则

`dmdb source` 建议完整继承 `postgres source` 的以下设计原则：

- 采用 keyset pagination，而不是 offset pagination。
- `checkpoint` 优先于 `start_from`，防止运行中修改起点导致重复回扫。
- Source 只暴露阻塞式 `receive()`，`try_receive()` 固定返回 `None`。
- 每一批查询结果都以“最后一条游标值”为进度点写 checkpoint。
- 查询无数据时按 `poll_interval_ms` 轮询；查询失败时按 `error_backoff_ms` 退避。
- 输出载荷统一为 JSON 字符串，并补充 `warp_parse_table` 元数据字段。

## 3. 与现有 `dmdb` 模块的差异与约束

当前仓库中的 `dmdb` 模块只有 sink，没有 source，实现基于 `odbc-api` 的同步 ODBC 调用。由此带来以下设计约束：

- 建连、执行 SQL、读取结果集都属于同步阻塞调用。
- 与 `postgres source` 的 async ORM 查询不同，`dmdb source` 默认走同步 ODBC 查询；即使同样采用“SQL 直接返回 `cursor_value` 和 `payload`”这一路径，参数绑定、结果集读取和大字段处理也需要按 ODBC 单独实现。
- 达梦 SQL 方言与 PostgreSQL 不同，因此虽然默认优先数据库侧 JSON 生成 `payload`，也不能直接照搬 PostgreSQL 的 SQL 写法、JSON 函数和时间函数，需要按达梦官方能力与 `JSON_MODE` 重新验证。
- 时间类型、时区解释、日期格式兼容性需要额外验证，首版应避免过度承诺。

## 4. 建议新增和修改的文件

### 4.1 新增文件

- `src/dmdb/source.rs`
- `src/dmdb/docs/dmdb_source设计稿.md`

### 4.2 修改文件

- `src/dmdb/mod.rs`
- `src/dmdb/factory.rs`
- `src/dmdb/config.rs` 或新增 source 专用配置结构

### 4.3 建议导出结构

```rust
pub mod config;
pub mod factory;
pub mod sink;
pub mod source;

pub use factory::{DmdbSinkFactory, DmdbSourceFactory};
pub use sink::DmdbSink;
pub use source::DmdbSource;
```

## 5. 运行时结构设计

建议新增 `DmdbSource` 结构，整体形状尽量向 `PostgresSource` 对齐：

```rust
pub struct DmdbSource {
    key: String,
    connection: Option<DmdbConnectionHandle>,
    table_ref: String,
    cursor_column: String,
    cursor_plan: CursorPlan,
    batch: usize,
    poll_interval: Duration,
    error_backoff: Duration,
    checkpoint_path: PathBuf,
    checkpoint: Option<CheckpointState>,
    start_from: Option<String>,
    query_timeout_secs: Option<usize>,
    tags: Tags,
}
```

字段说明：

- `key`：Source 对外标识，与 `SourceSpec.name` 保持一致。
- `connection`：复用现有 `dmdb sink` 的共享连接类型，避免重造连接封装。
- `table_ref`：运行时可直接用于 SQL 拼接的目标表引用，推荐在启动时一次性构建。
- `cursor_column`：增量游标列名。
- `cursor_plan`：游标类型与下界表达式规划结果。
- `batch`：单次查询最大行数。
- `poll_interval`：无数据时的轮询间隔。
- `error_backoff`：查询失败后的退避时间。
- `checkpoint_path`：checkpoint 文件路径。
- `checkpoint`：当前内存中的 checkpoint 状态。
- `start_from`：首次启动时的起始游标，只有没有 checkpoint 时才生效。
- `query_timeout_secs`：单条 SQL 查询超时秒数。
- `tags`：写入 `SourceEvent` 的标签集合。

## 6. 配置设计

### 6.1 建议不要直接复用 sink 的完整配置语义

现有 `DmdbConf` 已经服务于 sink，包含 `table`、`batch_size`、`query_timeout_secs` 等偏写入语义字段。`source` 虽可共用一部分连接字段，但建议单独拆出 `DmdbSourceConf`，避免以下混淆：

- sink 的 `batch_size` 表示单次写入分片大小。
- source 的 `batch` 表示单次查询上限。
- sink 的 `columns` 是目标表写入列。
- source 不需要 `columns`，而是需要 `cursor_column`、`cursor_type`。

### 6.2 建议配置结构

```rust
pub struct DmdbSourceConf {
    pub dsn: Option<String>,
    pub connection_string: Option<String>,
    pub endpoint: String,
    pub driver: String,
    pub username: String,
    pub password: String,
    pub schema: Option<String>,
    pub table: Option<String>,
    pub cursor_column: Option<String>,
    pub cursor_type: Option<String>,
    pub start_from: Option<String>,
    pub start_from_format: Option<String>,
    pub batch: Option<usize>,
    pub poll_interval_ms: Option<u64>,
    pub error_backoff_ms: Option<u64>,
    pub connect_timeout_secs: Option<u64>,
    pub query_timeout_secs: Option<usize>,
}
```

### 6.3 Source 必填参数建议

- `table`
- `cursor_column`
- `cursor_type`
- 一组合法连接参数：
  - `connection_string`
  - 或 `endpoint + driver + username + password`
  - 或 `dsn + username + password`

### 6.4 Source 可选参数建议

- `schema`
- `start_from`
- `start_from_format`
- `batch`
- `poll_interval_ms`
- `error_backoff_ms`
- `connect_timeout_secs`
- `query_timeout_secs`

## 7. 游标类型与游标规划

### 7.1 首版支持范围

首版必须支持两类游标：

- `int`
- `time`

建议结构：

```rust
enum CursorType {
    Int,
    Time,
}

enum LowerBoundBinding {
    Int,
    Text,
}

struct CursorPlan {
    cursor_type: CursorType,
    lower_bound_binding: LowerBoundBinding,
}
```

### 7.2 现实约束

- `int` 游标仍然是最稳的切入点，但不能替代 `time` 游标交付。
- `time` 游标首版必须交付，建议先支持数据库可稳定映射为 `date`、`timestamp`、带时区时间戳同类语义的列。
- 若达梦实际类型命名与 PostgreSQL 不一致，应保留同样的“时间游标能力”，但改为达梦自己的类型探测和规范化实现，不能直接删除该能力。

### 7.3 `start_from` 语义

延续 `postgres source`：

- `start_from` 只在没有 checkpoint 的首次启动时生效。
- 一旦已有 checkpoint，后续即使修改 `start_from` 也不应生效。
- `start_from_format` 只描述输入如何解析，不改变数据库列本身的比较语义。

## 8. 查询设计

### 8.1 总体原则

建议保留 `postgres source` 的 keyset pagination 思路，并将“数据库侧 JSON 产出”作为首版唯一实现路径。查询负责：

- 读取并返回 `cursor_value`
- 在 SQL 内直接生成 `payload`
- 按游标升序返回

这样 `dmdb source` 在运行时更接近 `postgres source` 的消费模型：Rust 侧只需要按列名读取 `cursor_value` 和 `payload` 两列。

### 8.2 有下界时的建议 SQL

以下 SQL 仅表达查询结构，不表示达梦最终可执行语法；实际实现时需要按达梦真实分页语法、JSON 函数写法和参数绑定方式落地。

```sql
SELECT
    cursor_value,
    payload
FROM (
    SELECT
        <cursor_column> AS cursor_value,
        <达梦 JSON 组装表达式> AS payload
    FROM <table_ref> t
    WHERE <cursor_column> > ?
    ORDER BY <cursor_column> ASC
    LIMIT ?
) base
ORDER BY cursor_value ASC
```

### 8.3 无下界时的建议 SQL

以下 SQL 同样为结构示意，实际实现时需要替换为达梦可执行版本。

```sql
SELECT
    cursor_value,
    payload
FROM (
    SELECT
        <cursor_column> AS cursor_value,
        <达梦 JSON 组装表达式> AS payload
    FROM <table_ref> t
    ORDER BY <cursor_column> ASC
    LIMIT ?
) base
ORDER BY cursor_value ASC
```

### 8.4 为什么首版直接采用数据库侧 JSON

- 达梦官方文档确认提供 `json_object`、`json_build_object`、`to_json`、`to_jsonb` 等 JSON/JSONB 能力，因此数据库侧直接生成 `payload` 在能力上是成立的。
- 对 ODBC 而言，若数据库直接返回 `cursor_value + payload` 两列，Rust 侧只需读取两列字符串，通常比“把整行所有列都取出来再自行组装 JSON”更省 CPU 和内存分配。
- 列数越多、字段类型越杂、批量越大，这种差距通常越明显，因为 ODBC 列扫描和 Rust 侧 JSON 构造成本会持续累积。
- 该方案也更贴近现有 `postgres source` 的实现形态，后续复用查询消费和 checkpoint 逻辑更直接。

## 9. 结果集到事件的转换设计

### 9.1 数据库侧直接产出 payload

首版实现与 `postgres source` 保持一致：

- SQL 直接返回 `cursor_value`
- SQL 直接返回 `payload`
- Rust 侧只负责读取这两列并构造 `SourceEvent`

推荐输出形态如下：

```rust
// 伪代码：这里只表达“读取 cursor_value/payload 两列”的实现意图，
// 实际代码需要按 odbc-api 的结果集读取接口落地。
let cursor_raw: String = row.read("cursor_value")?;
let payload: String = row.read("payload")?;

SourceEvent::new(
    next_wp_event_id(),
    self.key.clone(),
    RawData::from_string(payload),
    self.tags.clone().into(),
)
```

### 9.2 时间与空值序列化建议

首版建议在达梦 SQL 侧明确约定序列化规则：

- 普通字符串按 JSON string 输出
- 数值列按 JSON number 输出
- `NULL` 按 JSON null 输出
- 时间游标相关字段必须输出稳定、可比较、可复用到 `start_from_format` 语义中的字符串格式
- 若数据库存在时区信息，输出格式必须保留时区；若数据库列本身不保存时区，则应在文档中明确按达梦列语义输出

## 10. checkpoint 设计

建议与 `postgres source` 完全同型：

```rust
struct CheckpointState {
    version: u32,
    cursor_type: String,
    cursor_column: String,
    last_cursor_raw: String,
    updated_at: String,
}
```

### 10.1 行为要求

- checkpoint 文件不存在：自动创建父目录并视为“无 checkpoint”。
- checkpoint 文件为空：视为“无 checkpoint”。
- checkpoint 与当前 `cursor_type` 或 `cursor_column` 不兼容：直接报错，并提示删除 checkpoint 后重启。
- 当前批次成功转换完成后，用“最后一条记录的游标值”刷新 checkpoint。

### 10.2 下界解析规则

建议复用 `postgres source` 语义：

```rust
fn resolve_lower_bound<'a>(
    checkpoint: Option<&'a CheckpointState>,
    start_from: Option<&'a str>,
) -> Option<&'a str>
```

规则如下：

- 有 checkpoint 时使用 checkpoint
- 没有 checkpoint 时才使用 start_from
- 两者都没有时，从最小游标开始拉取

## 11. 生命周期与主流程设计

### 11.1 `new`

建议流程：

1. 校验 `table`、`cursor_column`、`cursor_type`
2. 按现有 `dmdb` 规则建连
3. 构建 `table_ref`
4. 构建 `cursor_plan`
5. 规范化 `start_from`
6. 计算并加载 `checkpoint_path`
7. 校验 checkpoint 与当前配置兼容
8. 若存在有效下界，执行一次下界合法性校验
9. 返回 `DmdbSource`

### 11.2 `recv_impl`

建议逻辑与 `PostgresSource::recv_impl` 一致：

1. 循环调用 `query_next_batch`
2. 查询失败时记录告警并 `sleep(error_backoff)`
3. 查询为空时 `sleep(poll_interval)`
4. 查询有数据时：
   - 逐条构造 `SourceEvent`
   - 记录最后一条游标值
   - 写 checkpoint
   - 返回 `SourceBatch`

### 11.3 `query_next_batch`

职责建议保持聚焦：

- 根据是否存在下界构建 SQL
- 绑定参数并执行查询
- 将结果集转成 `Vec<(String, String)>`
  - 第一个字段：`cursor_raw`
  - 第二个字段：`payload`

### 11.4 `DataSource` 实现

直接对齐现有数据库 source 风格：

```rust
#[async_trait]
impl DataSource for DmdbSource {
    async fn receive(&mut self) -> SourceResult<SourceBatch> {
        self.recv_impl().await
    }

    fn try_receive(&mut self) -> Option<SourceBatch> {
        None
    }

    fn identifier(&self) -> String {
        self.key.clone()
    }
}
```

## 12. Factory 设计

### 12.1 建议新增项

在 `src/dmdb/factory.rs` 中新增：

- `DmdbSourceFactory`
- `validate_dmdb_source_spec`
- `build_dmdb_source_conf`
- `dmdb_source_defaults`

### 12.2 `SourceFactory` 形态

建议直接平移 `PostgresSourceFactory` 的结构：

```rust
#[async_trait]
impl SourceFactory for DmdbSourceFactory {
    fn kind(&self) -> &'static str {
        "dmdb"
    }

    fn validate_spec(&self, spec: &SourceSpec) -> SourceResult<()> {
        build_dmdb_source_conf(spec)?;
        Ok(())
    }

    async fn build(&self, spec: &SourceSpec, _ctx: &SourceBuildCtx) -> SourceResult<SourceSvcIns> {
        let conf = build_dmdb_source_conf(spec)?;
        let mut meta_tags = Tags::from_parse(&spec.tags);
        meta_tags.set(WP_SRC_VAL, "dmdb");
        let source = DmdbSource::new(spec.name.clone(), meta_tags.clone(), &conf)
            .await
            .map_err(|err| SourceReason::Other(err.to_string()))?;

        let mut meta = SourceMeta::new(spec.name.clone(), spec.kind.clone());
        meta.tags = meta_tags;
        let handle = SourceHandle::new(Box::new(source), meta);
        Ok(SourceSvcIns::new().with_sources(vec![handle]))
    }
}
```

### 12.3 `source_def` 建议

建议 `allow_override` 至少包括：

- `endpoint`
- `dsn`
- `connection_string`
- `driver`
- `schema`
- `table`
- `username`
- `password`
- `batch`
- `cursor_column`
- `cursor_type`
- `start_from`
- `start_from_format`
- `poll_interval_ms`
- `error_backoff_ms`
- `connect_timeout_secs`
- `query_timeout_secs`

## 13. 错误处理建议

错误风格建议向现有 source 对齐，统一映射到 `SourceReason`：

- 建连失败：`SourceReason::Other`
- SQL 执行失败：`SourceReason::SupplierError`
- 读取游标列失败：`SourceReason::Other`
- JSON 序列化失败：`SourceReason::Other`
- checkpoint 读写失败：`SourceReason::Other`

建议错误文案中包含以下信息：

- 当前模块名 `dmdb source`
- 失败阶段：connect/query/read cursor/build payload/checkpoint
- 必要时包含 `table`、`cursor_column`、checkpoint 路径

## 14. 关于阻塞模型的建议

这是实现阶段必须明确的点。

现有 `dmdb` 能力基于同步 ODBC，而仓库历史里既出现过 `spawn_blocking` 包裹，也出现过“按用户要求直接同步调用”的版本。基于当前代码现状，`dmdb source` 首版建议：

- 默认先沿用当前 `dmdb` 模块行为，在 async 上下文中直接执行同步 ODBC 调用。
- 文档中明确记录：慢查询或慢网络可能阻塞 runtime 线程。
- 如果后续压测或联调证明阻塞不可接受，再单独演进为 `spawn_blocking` 版本。

这样能避免 source 首版在运行模型上和现有 `dmdb sink` 再次分叉。

## 15. 测试设计

### 15.1 单元测试建议

至少覆盖：

- `cursor_type` 配置校验
- `start_from` 与 `start_from_format` 基础校验
- `resolve_lower_bound` 的 checkpoint 优先级
- `build_batch_query` 在有/无下界时的 SQL 生成
- `checkpoint` 文件为空、缺失、版本不兼容时的行为
- 标识符引用和表名拼接

### 15.2 集成测试建议

如果测试环境具备达梦实例和 ODBC 驱动，可补充：

- `int` 游标递增拉取
- `time` 游标递增拉取
- checkpoint 重启续跑
- `start_from` 生效
- `time` 游标下的 `start_from_format` 生效
- 空批次轮询
- 非法 `cursor_column` 报错

### 15.3 首版验收建议

首版建议把验收门槛放在：

- `cargo fmt --all`
- `cargo clippy --all-targets -- -D warnings`
- `cargo test --features dmdb`
- 若本机具备环境，再执行一组手工或脚本化达梦集成验证

## 16. 风险与未决项

### 16.1 风险

- `odbc-api` 结果集读取接口可能比预期更底层，代码量会高于 `postgres source`。
- 达梦时间类型字符串格式可能与 PostgreSQL 不一致，影响 `time` 游标解析。
- 若直接同步查询，在高延迟数据库场景可能阻塞 Tokio worker。

### 16.2 未决项

- 查询结果是否需要做更强的 JSON 类型还原，而不是保守字符串化。
- 是否需要为 Source 单独引入“连接重建”逻辑，而不是仅依赖首次建连。

## 17. 建议实施顺序

建议按以下顺序推进：

1. 先新增 `DmdbSourceFactory` 与基础配置校验。
2. 落地 `DmdbSource` 骨架、`receive/try_receive/identifier` 和 checkpoint 基础能力。
3. 实现 `int` 与 `time` 两类游标查询，并同时打通时间格式游标的 `start_from_format`。
4. 实现数据库层直接生成 `payload` 的 SQL 路径，并校准时间/空值序列化格式。
5. 补齐单元测试、集成测试与基础文档，再评估阻塞模型优化。

## 18. 结论

`dmdb source` 的最稳妥首版方案，不是照抄 `postgres source` 的数据库细节，而是复用它的整体架构：

- 相同的生命周期
- 相同的 checkpoint 语义
- 相同的 keyset pagination 思路
- 相同的 `SourceEvent` 输出方式

同时把达梦的特殊性收敛在以下几个点：

- ODBC 建连与查询
- SQL 方言差异
- 数据库层直接生成 `payload`
- 时间游标兼容边界

这样后续无论是直接实现首版，还是继续演进更完整的 `dmdb source`，都能保持仓库内部的一致性与可维护性。
