use super::common::{
    DmdbConnectionHandle, connect_shared_blocking, escape_sql_literal, quote_identifier,
};
use super::config::DmdbSourceConf;
use async_trait::async_trait;
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, SecondsFormat, TimeZone, Utc};
use odbc_api::sys::SqlDataType;
use odbc_api::{Cursor, DataType};
use orion_error::conversion::{SourceErr, SourceRawErr, ToStructError};
use orion_error::{OrionError, StructError, UnifiedReason};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::{self, Receiver as TokioReceiver, Sender as TokioSender};
use tokio::task;
use wp_connector_api::{
    CtrlRx, DataSource, SourceBatch, SourceEvent, SourceReason, SourceResult, Tags,
};
use wp_log::{debug_ctrl, info_ctrl, info_data, warn_ctrl, warn_data};
use wp_model_core::event_id::next_wp_event_id;
use wp_model_core::raw::RawData;

// ===== 错误模型 =====

pub(crate) type DmdbError = StructError<DmdbReason>;
pub(crate) type DmdbResult<T> = Result<T, DmdbError>;

#[derive(Debug, Clone, PartialEq, Serialize, OrionError)]
pub(crate) enum DmdbReason {
    #[orion_error(identity = "conf.dmdb_config", message = "dmdb config error")]
    Config,
    #[orion_error(identity = "logic.dmdb_cursor", message = "dmdb cursor error")]
    Cursor,
    #[orion_error(identity = "logic.dmdb_checkpoint", message = "dmdb checkpoint error")]
    Checkpoint,
    #[orion_error(identity = "logic.dmdb_time", message = "dmdb time error")]
    Time,
    #[orion_error(identity = "conf.dmdb_parse", message = "dmdb parse error")]
    Parse,
    #[orion_error(identity = "sys.dmdb_database", message = "dmdb database error")]
    Database,
    #[orion_error(identity = "sys.dmdb_io", message = "dmdb io error")]
    Io,
    #[orion_error(transparent)]
    #[allow(dead_code)]
    Uvs(UnifiedReason),
}

pub(crate) fn dmdb_err(reason: DmdbReason, detail: impl Into<String>) -> DmdbError {
    reason.to_err().with_detail(detail)
}

// ===== 运行时默认值 =====

const DEFAULT_BATCH: usize = 2048;
const DEFAULT_POLL_INTERVAL_MS: u64 = 1000;
const DEFAULT_ERROR_BACKOFF_MS: u64 = 2000;
const CHECKPOINT_VERSION: u32 = 1;
/// `CursorRow::get_text` 遇到空缓冲时只会先申请 256B，达梦 ODBC 在分段返回较长文本时会为每次首段读取输出 `01004/String truncate` 告警。
/// 这里预留一个足够覆盖常见游标文本的容量。
const INITIAL_CURSOR_BUF_CAPACITY: usize = 128;
/// payload 是整行 JSON，常见场景明显超过 256B；直接给较大的初始容量，并在逐行读取时复用，
const INITIAL_PAYLOAD_BUF_CAPACITY: usize = 64 * 1024;
/// 预取队列按“批次”限流，避免后台无限预取导致内存膨胀。
const PREFETCH_QUEUE_CAPACITY: usize = 8;

// ===== 游标、checkpoint 与元数据静态模型 =====

/// 用户可配置的游标语义，目前支持整数游标和时间游标两类。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CursorType {
    Int,
    Time,
}

/// lower bound 在 SQL 中的绑定策略，取决于游标列的真实数据库类型。
#[derive(Clone, Debug, Eq, PartialEq)]
enum LowerBoundBinding {
    /// 普通整数列，使用整数比较。
    Integer,
    /// NUMBER/DECIMAL/FLOAT 等数值列，允许小数。
    Numeric,
    /// DATE 列，序列化为 `YYYY-MM-DD`。
    Date,
    /// TIMESTAMP（无时区）列，序列化为 `YYYY-MM-DD HH24:MI:SS.FF6`。
    Timestamp,
    /// TIMESTAMP WITH TIME ZONE 列，序列化为带偏移的 RFC3339 风格文本。
    TimestampTz,
}

/// 启动阶段基于表元数据推导出的游标执行计划。
#[derive(Clone, Debug, Eq, PartialEq)]
struct CursorPlan {
    /// 用户声明的游标语义类型。
    cursor_type: CursorType,
    /// 由真实列类型推导出的 lower bound 绑定方式。
    lower_bound_binding: LowerBoundBinding,
    /// 生成 payload JSON 的 SQL 片段。
    payload_expr: String,
}

/// `start_from_format` 的解析模式，用来区分 unix 时间戳和自定义时间格式。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StartFromFormatKind {
    UnixSeconds,
    UnixMillis,
    Pattern,
}

/// 用户声明的 `start_from` 格式定义。
#[derive(Clone, Debug, Eq, PartialEq)]
struct StartFromFormat {
    raw: String,
    kind: StartFromFormatKind,
}

/// 本地 checkpoint 文件结构。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CheckpointState {
    version: u32,
    cursor_type: String,
    cursor_column: String,
    last_cursor_raw: String,
    updated_at: String,
}

/// 从 ODBC 元数据接口提取出的列信息，用于构建查询和序列化 payload。
#[derive(Debug, Clone, PartialEq, Eq)]
struct DmdbColumnMeta {
    name: String,
    type_name: String,
    data_type: DataType,
    ordinal_position: i32,
}

// ===== Source 与预取 worker 运行时结构 =====

/// 运行时会维护本地 checkpoint，以便重启后从上次成功消费位置继续拉取。
pub struct DmdbSource {
    /// Source 唯一标识，同时用于 checkpoint 文件命名。
    key: String,
    /// 当前 Source 自己持有的 ODBC 连接句柄，所有查询都在阻塞线程中串行执行。
    connection: DmdbConnectionHandle,
    /// 带 schema 的表引用，直接参与 SQL 拼装。
    table_ref: String,
    /// 原始表名，用于回填到 payload 元数据。
    table_name: String,
    /// 增量游标列名。
    cursor_column: String,
    /// 由表元数据推导出的游标执行计划，决定 lower bound 比较与 payload 序列化策略。
    cursor_plan: CursorPlan,
    /// 单批拉取上限。
    batch: usize,
    /// 无数据时的轮询间隔。
    poll_interval: Duration,
    /// 查询失败时的退避间隔。
    error_backoff: Duration,
    /// checkpoint 文件路径。
    checkpoint_path: PathBuf,
    /// 当前已加载的 checkpoint。
    checkpoint: Option<CheckpointState>,
    /// 首次启动且无 checkpoint 时使用的起始游标。
    start_from: Option<String>,
    /// SQL 查询超时。
    query_timeout_secs: Option<usize>,
    /// 透传给下游事件的标签。
    tags: Tags,
    /// 后台预取线程写入的有界批次队列。
    batch_rx: Option<TokioReceiver<PreparedBatch>>,
    /// 当前 worker 的停止标记；仅在 worker 存活期间存在。
    worker_stop: Option<Arc<AtomicBool>>,
    /// worker 线程句柄。close 时仅发送 stop 并释放句柄，不在 async 上下文阻塞 join。
    worker_handle: Option<thread::JoinHandle<()>>,
}

/// 后台 worker 预取完成后投递给 Source 主循环的批次载体。
#[derive(Debug)]
struct PreparedBatch {
    events: SourceBatch,
    last_cursor_raw: String,
    round: u64,
    lower_bound: Option<String>,
}

/// 独立阻塞线程中的预取 worker，负责循环执行 SQL 并把结果批次送入队列。
struct PrefetchWorker {
    key: String,
    connection: DmdbConnectionHandle,
    table_ref: String,
    table_name: String,
    cursor_column: String,
    cursor_plan: CursorPlan,
    tags: Tags,
    batch_size: usize,
    poll_interval: Duration,
    error_backoff: Duration,
    query_timeout_secs: Option<usize>,
    stop_flag: Arc<AtomicBool>,
    batch_tx: TokioSender<PreparedBatch>,
    query_round: u64,
    current_lower_bound: Option<String>,
}

// ===== Source 生命周期与 checkpoint 管理 =====

impl DmdbSource {
    /// 返回 Source 标识，供运行时与日志复用。
    pub fn identifier(&self) -> &str {
        &self.key
    }

    /// 根据配置建立达梦增量 Source，并在启动阶段完成游标计划探测。
    pub async fn new(key: String, tags: Tags, config: &DmdbSourceConf) -> SourceResult<Self> {
        let table = required_opt_field("dmdb.table", &config.table)?;
        let cursor_column = required_opt_field("dmdb.cursor_column", &config.cursor_column)?;
        let cursor_type = CursorType::from_config(
            config
                .cursor_type
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .unwrap_or("int"),
        )
        .source_err(SourceReason::Other, "initialize dmdb source failed")?;

        // 建连走阻塞线程，避免 ODBC 调用卡住 Tokio runtime。
        let connection = {
            let conf = config.conn.clone();
            task::spawn_blocking(move || connect_shared_blocking(&conf))
                .await
                .source_raw_err(
                    SourceReason::Other,
                    "initialize dmdb source failed: spawn connect task failed",
                )?
                .source_err(SourceReason::Other, "initialize dmdb source failed")?
        };

        let schema = normalized_schema(config.conn.schema.as_deref());
        // 启动阶段预先读取表结构，提早发现游标列不存在或类型不兼容的问题。
        let cursor_plan = {
            let connection = connection.clone();
            let schema = schema.clone();
            let table = table.to_string();
            let cursor_column = cursor_column.to_string();
            task::spawn_blocking(move || {
                CursorPlan::build(
                    &connection,
                    schema.as_deref(),
                    &table,
                    &cursor_column,
                    cursor_type,
                )
            })
            .await
            .source_raw_err(
                SourceReason::Other,
                "initialize dmdb source failed: spawn cursor plan task failed",
            )?
            .source_err(SourceReason::Other, "initialize dmdb source failed")?
        };

        let start_from_format =
            parse_start_from_format(config.start_from_format.as_deref(), cursor_type)
                .source_err(SourceReason::Other, "initialize dmdb source failed")?;
        let session_offset = FixedOffset::east_opt(0)
            .ok_or_else(|| SourceReason::other("build UTC fixed offset failed"))?;
        // `start_from` 会先标准化为数据库可稳定比较的文本形式。
        let start_from = normalize_optional_start_from(
            &cursor_plan,
            config.start_from.as_deref(),
            start_from_format.as_ref(),
            session_offset,
        )
        .source_err(SourceReason::Other, "initialize dmdb source failed")?;

        let batch = config.batch.unwrap_or(DEFAULT_BATCH).max(1);
        let poll_interval =
            Duration::from_millis(config.poll_interval_ms.unwrap_or(DEFAULT_POLL_INTERVAL_MS));
        let error_backoff =
            Duration::from_millis(config.error_backoff_ms.unwrap_or(DEFAULT_ERROR_BACKOFF_MS));

        let table_ref = qualified_table_name(schema.as_deref(), table);
        let checkpoint_path = checkpoint_path(&key);
        let checkpoint = Self::load_checkpoint(&checkpoint_path, cursor_column, &cursor_plan)
            .source_err(SourceReason::Other, "initialize dmdb source failed")?;
        // checkpoint 优先于 start_from，避免 Source 重启后重复回到初始位点。
        if let Some(lower_bound) = resolve_lower_bound(checkpoint.as_ref(), start_from.as_deref()) {
            cursor_plan
                .validate_lower_bound(lower_bound, "dmdb active lower bound")
                .source_err(SourceReason::Other, "initialize dmdb source failed")?;
        }

        info_data!(
            "[dmdb-source] table: {}, cursor_column: {}, cursor_type: {:?}",
            table_ref,
            cursor_column,
            cursor_type
        );

        Ok(Self {
            key,
            connection,
            table_ref,
            table_name: table.to_string(),
            cursor_column: cursor_column.to_string(),
            cursor_plan,
            batch,
            poll_interval,
            error_backoff,
            checkpoint_path,
            checkpoint,
            start_from,
            query_timeout_secs: config.conn.query_timeout_secs,
            tags,
            batch_rx: None,
            worker_stop: None,
            worker_handle: None,
        })
    }

    fn ensure_worker_started(&mut self) -> SourceResult<()> {
        if self.batch_rx.is_some() {
            return Ok(());
        }
        let connection = self.connection.clone();
        let (batch_tx, batch_rx) = mpsc::channel(PREFETCH_QUEUE_CAPACITY);
        let key = self.key.clone();
        let table_ref = self.table_ref.clone();
        let table_name = self.table_name.clone();
        let cursor_column = self.cursor_column.clone();
        let cursor_plan = self.cursor_plan.clone();
        let tags = self.tags.clone();
        let batch_size = self.batch;
        let poll_interval = self.poll_interval;
        let error_backoff = self.error_backoff;
        let query_timeout_secs = self.query_timeout_secs;
        let start_lower_bound = self.lower_bound_raw().map(str::to_string);
        let stop_flag = Arc::new(AtomicBool::new(false));
        self.worker_stop = Some(Arc::clone(&stop_flag));
        let handle = thread::Builder::new()
            .name(format!("dmdb-source-worker-{key}"))
            .spawn(move || {
                PrefetchWorker {
                    key,
                    connection,
                    table_ref,
                    table_name,
                    cursor_column,
                    cursor_plan,
                    tags,
                    batch_size,
                    poll_interval,
                    error_backoff,
                    query_timeout_secs,
                    stop_flag,
                    batch_tx,
                    query_round: 0,
                    current_lower_bound: start_lower_bound,
                }
                .run()
            })
            .map_err(|err| {
                SourceReason::other(format!("spawn dmdb source worker failed: {err}"))
            })?;
        self.batch_rx = Some(batch_rx);
        self.worker_handle = Some(handle);
        info_ctrl!(
            "[dmdb-source] {} prefetch worker started (queue_cap={})",
            self.key,
            PREFETCH_QUEUE_CAPACITY
        );
        Ok(())
    }

    /// 主轮询循环：查询一批数据、组装事件，并在成功后推进 checkpoint。
    async fn recv_impl(&mut self) -> SourceResult<SourceBatch> {
        self.ensure_worker_started()?;
        let batch_rx = self
            .batch_rx
            .as_mut()
            .ok_or_else(|| SourceReason::other("dmdb source batch receiver is not initialized"))?;
        let prepared = batch_rx
            .recv()
            .await
            .ok_or_else(|| SourceReason::supplier_error("dmdb prefetch worker channel closed"))?;
        debug_ctrl!(
            "[dmdb-source] round={} checkpoint advance from {:?} to {}",
            prepared.round,
            prepared.lower_bound.as_deref(),
            prepared.last_cursor_raw
        );
        self.persist_checkpoint(prepared.last_cursor_raw)?;
        Ok(prepared.events)
    }

    /// 获取本轮查询应使用的 lower bound，优先使用 checkpoint 中的最新游标。
    fn lower_bound_raw(&self) -> Option<&str> {
        resolve_lower_bound(self.checkpoint.as_ref(), self.start_from.as_deref())
    }

    /// 加载并校验本地 checkpoint；不存在时自动创建目录并视为首次启动。
    fn load_checkpoint(
        path: &Path,
        cursor_column: &str,
        cursor_plan: &CursorPlan,
    ) -> DmdbResult<Option<CheckpointState>> {
        if !path.exists() {
            ensure_checkpoint_dir(path)?;
            return Ok(None);
        }

        let contents = std::fs::read_to_string(path).source_err(
            DmdbReason::Io,
            format!("dmdb read checkpoint {} failed", path.display()),
        )?;
        if contents.trim().is_empty() {
            return Ok(None);
        }

        let state: CheckpointState = serde_json::from_str(&contents).source_raw_err(
            DmdbReason::Checkpoint,
            format!(
                "dmdb checkpoint file {} is invalid; if you changed source cursor config, delete this checkpoint and restart",
                path.display()
            ),
        )?;
        validate_checkpoint_state(&state, cursor_column, cursor_plan).map_err(|err| {
            dmdb_err(
                DmdbReason::Checkpoint,
                format!(
                    "dmdb checkpoint {} is incompatible with current config: {}; if you changed cursor_column/cursor_type or want to restart from a new cursor, delete this checkpoint and restart",
                    path.display(),
                    err
                ),
            )
        })?;
        Ok(Some(state))
    }

    /// 将当前批次最后一条游标持久化到本地 checkpoint 文件。
    fn persist_checkpoint(&mut self, last_cursor_raw: String) -> SourceResult<()> {
        ensure_checkpoint_dir(&self.checkpoint_path).map_err(|err| {
            SourceReason::other(format!("dmdb ensure checkpoint dir failed: {err}"))
        })?;

        // 仅在一批事件成功组装后持久化游标，避免“游标前推但事件未发出”的不一致状态。
        let state = CheckpointState {
            version: CHECKPOINT_VERSION,
            cursor_type: self.cursor_plan.cursor_type.as_str().to_string(),
            cursor_column: self.cursor_column.clone(),
            last_cursor_raw,
            updated_at: chrono::Utc::now().to_rfc3339(),
        };
        let content = serde_json::to_vec(&state).map_err(|err| {
            SourceReason::other(format!("dmdb serialize checkpoint failed: {err}"))
        })?;

        std::fs::write(&self.checkpoint_path, content)
            .map_err(|err| SourceReason::other(format!("dmdb write checkpoint failed: {err}")))?;
        self.checkpoint = Some(state);
        Ok(())
    }
}

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

    async fn start(&mut self, _ctrl_rx: CtrlRx) -> SourceResult<()> {
        self.ensure_worker_started()
    }

    async fn close(&mut self) -> SourceResult<()> {
        self.batch_rx = None;
        if let Some(stop_flag) = self.worker_stop.take() {
            stop_flag.store(true, Ordering::SeqCst);
        }
        if let Some(handle) = self.worker_handle.take() {
            drop(handle);
        }
        warn_ctrl!("[dmdb-source] {} prefetch worker stop requested", self.key);
        Ok(())
    }
}

// ===== 游标计划行为与 Source 配置校验 =====

/// 校验 Source 侧游标类型与 `start_from` 相关配置是否匹配。
pub(crate) fn validate_source_cursor_type_and_start_from(
    raw_cursor_type: &str,
    start_from: Option<&str>,
    start_from_format: Option<&str>,
) -> DmdbResult<()> {
    let cursor_type = CursorType::from_config(raw_cursor_type)?;
    cursor_type.validate_start_from(start_from, start_from_format)?;
    Ok(())
}

impl CursorType {
    /// 解析配置中的游标类型。
    fn from_config(raw: &str) -> DmdbResult<Self> {
        match raw {
            "int" => Ok(Self::Int),
            "time" => Ok(Self::Time),
            other => Err(dmdb_err(
                DmdbReason::Config,
                format!("unsupported dmdb cursor_type: {other}"),
            )),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Int => "int",
            Self::Time => "time",
        }
    }

    /// 校验 `start_from` 与 `start_from_format` 的组合是否符合当前游标语义。
    fn validate_start_from(
        self,
        start_from: Option<&str>,
        start_from_format: Option<&str>,
    ) -> DmdbResult<()> {
        let Some(start_from) = start_from else {
            if start_from_format.is_some() {
                return Err(dmdb_err(
                    DmdbReason::Config,
                    "dmdb.start_from_format requires dmdb.start_from",
                ));
            }
            return Ok(());
        };

        if start_from.trim().is_empty() {
            return Err(dmdb_err(
                DmdbReason::Config,
                format!(
                    "dmdb.start_from must not be empty for {} cursor",
                    self.as_str()
                ),
            ));
        }
        if start_from_format.is_some() && self != CursorType::Time {
            return Err(dmdb_err(
                DmdbReason::Config,
                "dmdb.start_from_format is only supported for time cursor",
            ));
        }
        Ok(())
    }

    /// 基于实际列类型决定 lower bound 的比较和绑定方式。
    fn lower_bound_binding(
        self,
        cursor_column: &str,
        data_type: &DataType,
        type_name: &str,
    ) -> DmdbResult<LowerBoundBinding> {
        match self {
            Self::Int => {
                if is_integer_data_type(data_type) {
                    Ok(LowerBoundBinding::Integer)
                } else if is_numeric_data_type(data_type, type_name) {
                    Ok(LowerBoundBinding::Numeric)
                } else {
                    Err(dmdb_err(
                        DmdbReason::Cursor,
                        format!(
                            "dmdb source cursor_column {} must be numeric-like for cursor_type=int, got {} / {:?}",
                            cursor_column, type_name, data_type
                        ),
                    ))
                }
            }
            Self::Time => {
                let Some(binding) = time_lower_bound_binding(data_type, type_name) else {
                    return Err(dmdb_err(
                        DmdbReason::Cursor,
                        format!(
                            "dmdb source cursor_column {} must be time-like for cursor_type=time, got {} / {:?}",
                            cursor_column, type_name, data_type
                        ),
                    ));
                };
                Ok(binding)
            }
        }
    }
}

impl CursorPlan {
    /// 启动时基于表元数据构建游标执行计划。
    fn build(
        connection: &DmdbConnectionHandle,
        schema: Option<&str>,
        table: &str,
        cursor_column: &str,
        cursor_type: CursorType,
    ) -> DmdbResult<Self> {
        // 元数据探测在启动时完成，避免运行中才发现类型不匹配。
        let columns = query_table_columns(connection, schema, table)?;
        if columns.is_empty() {
            return Err(dmdb_err(
                DmdbReason::Database,
                format!(
                    "dmdb source table metadata not found for {}.{}",
                    schema.unwrap_or(""),
                    table
                ),
            ));
        }

        let cursor_meta = columns
            .iter()
            .find(|column| column.name.eq_ignore_ascii_case(cursor_column))
            .ok_or_else(|| {
                dmdb_err(
                    DmdbReason::Cursor,
                    format!(
                        "dmdb source cursor_column not found: {}.{}.{}",
                        schema.unwrap_or(""),
                        table,
                        cursor_column
                    ),
                )
            })?;

        let lower_bound_binding = cursor_type.lower_bound_binding(
            cursor_column,
            &cursor_meta.data_type,
            &cursor_meta.type_name,
        )?;
        let payload_expr = build_payload_expr(&columns)?;

        Ok(Self {
            cursor_type,
            lower_bound_binding,
            payload_expr,
        })
    }

    /// 将 lower bound 文本转换为适配达梦 SQL 的字面量表达式。
    fn lower_bound_sql_literal(&self, raw: &str) -> DmdbResult<String> {
        self.validate_lower_bound(raw, "dmdb lower bound")?;
        match self.lower_bound_binding {
            LowerBoundBinding::Integer | LowerBoundBinding::Numeric => Ok(raw.to_string()),
            LowerBoundBinding::Date => Ok(format!(
                "TO_DATE('{}', 'YYYY-MM-DD')",
                escape_sql_literal(raw)
            )),
            LowerBoundBinding::Timestamp => Ok(format!(
                "TO_TIMESTAMP('{}', 'YYYY-MM-DD HH24:MI:SS.FF6')",
                escape_sql_literal(raw)
            )),
            LowerBoundBinding::TimestampTz => Ok(format!(
                "TO_TIMESTAMP_TZ('{}', 'YYYY-MM-DD\"T\"HH24:MI:SS.FF6TZH:TZM')",
                escape_sql_literal(raw)
            )),
        }
    }

    /// 校验 lower bound 文本能否被当前游标策略接受。
    fn validate_lower_bound(&self, raw: &str, field_name: &str) -> DmdbResult<()> {
        if raw.trim().is_empty() {
            return Err(dmdb_err(
                DmdbReason::Parse,
                format!("{field_name} must not be empty"),
            ));
        }
        match self.lower_bound_binding {
            LowerBoundBinding::Integer => {
                raw.parse::<i64>().map(|_| ()).map_err(|err| {
                    dmdb_err(
                        DmdbReason::Parse,
                        format!("{field_name} must be an integer: {err}"),
                    )
                })?;
            }
            LowerBoundBinding::Numeric => {
                raw.parse::<f64>().map(|_| ()).map_err(|err| {
                    dmdb_err(
                        DmdbReason::Parse,
                        format!("{field_name} must be numeric-like: {err}"),
                    )
                })?;
            }
            LowerBoundBinding::Date
            | LowerBoundBinding::Timestamp
            | LowerBoundBinding::TimestampTz => {}
        }
        Ok(())
    }

    /// 将用户输入的 `start_from` 归一化为适合当前游标类型比较的文本值。
    fn normalize_start_from(
        &self,
        raw: &str,
        format: Option<&StartFromFormat>,
        session_offset: FixedOffset,
    ) -> DmdbResult<String> {
        self.validate_lower_bound(raw, "dmdb.start_from")?;
        match self.lower_bound_binding {
            LowerBoundBinding::Integer | LowerBoundBinding::Numeric => Ok(raw.to_string()),
            LowerBoundBinding::Date => normalize_date_start_from(raw, format, session_offset),
            LowerBoundBinding::Timestamp => {
                normalize_timestamp_start_from(raw, format, session_offset)
            }
            LowerBoundBinding::TimestampTz => {
                normalize_timestamptz_start_from(raw, format, session_offset)
            }
        }
    }
}

// ===== Worker 执行与批次封装 =====

fn query_next_batch_blocking(
    connection: DmdbConnectionHandle,
    sql: String,
    query_timeout_secs: Option<usize>,
    batch_hint: usize,
) -> DmdbResult<Vec<(String, String)>> {
    let conn_guard = connection
        .lock()
        .map_err(|_| dmdb_err(DmdbReason::Database, "lock dmdb source connection fail"))?;
    let Some(mut cursor) = conn_guard
        .execute(sql.as_str(), (), query_timeout_secs)
        .map_err(|err| {
            dmdb_err(
                DmdbReason::Database,
                format!("execute dmdb source sql failed: {err}, sql: {sql}"),
            )
        })?
    else {
        return Ok(Vec::new());
    };

    // 查询结果固定为两列：游标值 + JSON payload。
    // `get_text` 会沿用传入 Vec 的 capacity 作为首段 SQLGetData 缓冲；
    // 若每行都从空 Vec 开始，达梦驱动会反复输出 `01004/String truncate` 告警。
    let mut out = Vec::with_capacity(batch_hint);
    let mut cursor_buf = Vec::with_capacity(INITIAL_CURSOR_BUF_CAPACITY);
    let mut payload_buf = Vec::with_capacity(INITIAL_PAYLOAD_BUF_CAPACITY);
    while let Some(mut row) = cursor
        .next_row()
        .source_raw_err(DmdbReason::Database, "iterate dmdb source cursor failed")?
    {
        cursor_buf.clear();
        payload_buf.clear();

        let has_cursor = row.get_text(1, &mut cursor_buf).map_err(|err| {
            dmdb_err(
                DmdbReason::Database,
                format!("read dmdb cursor_value failed: {err}"),
            )
        })?;
        if !has_cursor {
            return Err(dmdb_err(
                DmdbReason::Cursor,
                "dmdb source cursor_value must not be NULL",
            ));
        }
        let has_payload = row.get_text(2, &mut payload_buf).map_err(|err| {
            dmdb_err(
                DmdbReason::Database,
                format!("read dmdb payload failed: {err}"),
            )
        })?;
        if !has_payload {
            return Err(dmdb_err(
                DmdbReason::Database,
                "dmdb source payload must not be NULL",
            ));
        }
        let cursor_raw = std::str::from_utf8(&cursor_buf)
            .map_err(|err| {
                dmdb_err(
                    DmdbReason::Parse,
                    format!("dmdb cursor_value is not valid utf-8: {err}"),
                )
            })?
            .to_owned();
        let payload = std::str::from_utf8(&payload_buf)
            .map_err(|err| {
                dmdb_err(
                    DmdbReason::Parse,
                    format!("dmdb payload is not valid utf-8: {err}"),
                )
            })?
            .to_owned();
        out.push((cursor_raw, payload));
    }
    Ok(out)
}

impl PrefetchWorker {
    fn run(mut self) {
        while !self.stop_flag.load(Ordering::SeqCst) {
            let round = self.next_round();
            let lower_bound = self.current_lower_bound.clone();
            let started = Instant::now();
            let rows = match self.fetch_rows(&lower_bound, round) {
                Ok(rows) => rows,
                Err(err) => {
                    warn_data!(
                        "[dmdb-source] round={} query failed, backing off {:?}: {}",
                        round,
                        self.error_backoff,
                        err
                    );
                    thread::sleep(self.error_backoff);
                    continue;
                }
            };
            let elapsed_ms = started.elapsed().as_millis() as u64;
            self.log_round_end(round, &lower_bound, rows.len(), elapsed_ms);

            if rows.is_empty() {
                debug_ctrl!(
                    "[dmdb-source] round={} no new rows, next poll after {:?} (lower_bound={:?})",
                    round,
                    self.poll_interval,
                    lower_bound.as_deref()
                );
                thread::sleep(self.poll_interval);
                continue;
            }

            let prepared = self.prepare_batch(rows, round, lower_bound);
            if self.batch_tx.blocking_send(prepared).is_err() {
                warn_ctrl!(
                    "[dmdb-source] {} prefetch queue closed, worker exit",
                    self.key
                );
                break;
            }
        }
        warn_ctrl!("[dmdb-source] {} prefetch worker exited", self.key);
    }

    fn next_round(&mut self) -> u64 {
        self.query_round += 1;
        self.query_round
    }

    fn fetch_rows(
        &self,
        lower_bound: &Option<String>,
        round: u64,
    ) -> DmdbResult<Vec<(String, String)>> {
        let sql = build_batch_query(
            &self.table_ref,
            &self.table_name,
            &self.cursor_column,
            lower_bound.as_deref(),
            self.batch_size,
            &self.cursor_plan,
        );
        debug_ctrl!(
            "[dmdb-source] round={} query begin lower_bound={:?} batch_size={} timeout_secs={:?}",
            round,
            lower_bound.as_deref(),
            self.batch_size,
            self.query_timeout_secs
        );
        query_next_batch_blocking(
            self.connection.clone(),
            sql,
            self.query_timeout_secs,
            self.batch_size,
        )
    }

    fn log_round_end(
        &self,
        round: u64,
        lower_bound: &Option<String>,
        row_count: usize,
        elapsed_ms: u64,
    ) {
        debug_ctrl!(
            "[dmdb-source] round={} query end rows={} elapsed_ms={} lower_bound={:?}",
            round,
            row_count,
            elapsed_ms,
            lower_bound.as_deref()
        );
    }

    fn prepare_batch(
        &mut self,
        rows: Vec<(String, String)>,
        round: u64,
        lower_bound: Option<String>,
    ) -> PreparedBatch {
        let mut events = Vec::with_capacity(rows.len());
        let mut last_cursor_raw = String::new();
        for (cursor_raw, payload) in rows {
            events.push(SourceEvent::new(
                next_wp_event_id(),
                self.key.clone(),
                RawData::from_string(payload),
                Arc::new(self.tags.clone()),
            ));
            last_cursor_raw = cursor_raw;
        }
        self.current_lower_bound = Some(last_cursor_raw.clone());
        PreparedBatch {
            events,
            last_cursor_raw,
            round,
            lower_bound,
        }
    }
}

// ===== 元数据读取与 SQL 构造 =====

fn query_table_columns(
    connection: &DmdbConnectionHandle,
    schema: Option<&str>,
    table: &str,
) -> DmdbResult<Vec<DmdbColumnMeta>> {
    let conn_guard = connection.lock().map_err(|_| {
        dmdb_err(
            DmdbReason::Database,
            "lock dmdb source connection for metadata fail",
        )
    })?;
    let schema_pattern = schema.unwrap_or("");
    let table_pattern = table;
    // 通过 ODBC 元数据接口读取列顺序与类型，供查询构造和 JSON 序列化复用。
    let mut rows = Vec::new();
    for row_result in conn_guard
        .columns("", schema_pattern, table_pattern, "%")
        .source_raw_err(DmdbReason::Database, "query dmdb metadata columns failed")?
    {
        let row =
            row_result.source_raw_err(DmdbReason::Database, "read dmdb metadata row failed")?;
        let table_name = row.table.as_str().map_err(|err| {
            dmdb_err(
                DmdbReason::Database,
                format!("decode dmdb table metadata table name failed: {err}"),
            )
        })?;
        let Some(table_name) = table_name else {
            continue;
        };
        if !table_name.eq_ignore_ascii_case(table) {
            continue;
        }
        let schema_name = row.schema.as_str().map_err(|err| {
            dmdb_err(
                DmdbReason::Database,
                format!("decode dmdb table metadata schema name failed: {err}"),
            )
        })?;
        if let Some(expected_schema) = schema
            && let Some(actual_schema) = schema_name
            && !actual_schema.eq_ignore_ascii_case(expected_schema)
        {
            continue;
        }
        let column_name = row
            .column_name
            .as_str()
            .map_err(|err| {
                dmdb_err(
                    DmdbReason::Database,
                    format!("decode dmdb table metadata column name failed: {err}"),
                )
            })?
            .ok_or_else(|| {
                dmdb_err(
                    DmdbReason::Database,
                    "dmdb table metadata column name is NULL",
                )
            })?;
        let type_name = row
            .type_name
            .as_str()
            .map_err(|err| {
                dmdb_err(
                    DmdbReason::Database,
                    format!("decode dmdb table metadata type name failed: {err}"),
                )
            })?
            .unwrap_or("")
            .to_string();
        let data_type = DataType::new(
            SqlDataType(row.data_type),
            row.column_size.into_opt().unwrap_or_default().max(0) as usize,
            row.decimal_digits.into_opt().unwrap_or_default(),
        );
        rows.push(DmdbColumnMeta {
            name: column_name.to_string(),
            type_name,
            data_type,
            ordinal_position: row.ordinal_position,
        });
    }
    rows.sort_by_key(|row| row.ordinal_position);
    Ok(rows)
}

fn build_payload_expr(columns: &[DmdbColumnMeta]) -> DmdbResult<String> {
    let mut parts = Vec::with_capacity(columns.len() + 2);
    for column in columns {
        parts.push(format!(
            "'{}' VALUE {}",
            escape_sql_literal(&column.name),
            json_value_expr(column)
        ));
    }
    parts.push("'warp_parse_table' VALUE t.__warp_parse_table".to_string());
    Ok(format!(
        "CAST(JSON_OBJECT({} NULL ON NULL) AS CLOB)",
        parts.join(", ")
    ))
}

/// 将单列引用转换成适合 `JSON_OBJECT` 的 SQL 表达式。
fn json_value_expr(column: &DmdbColumnMeta) -> String {
    let column_ref = format!("t.{}", quote_identifier(&column.name));
    match &column.data_type {
        DataType::Binary { .. } | DataType::Varbinary { .. } | DataType::LongVarbinary { .. } => {
            format!("RAWTOHEX({column_ref})")
        }
        DataType::Date => {
            format!("TO_CHAR({column_ref}, 'YYYY-MM-DD')")
        }
        DataType::Time { .. } => {
            format!("TO_CHAR({column_ref}, 'HH24:MI:SS.FF6')")
        }
        DataType::Timestamp { .. } => {
            let type_name = column.type_name.to_ascii_uppercase();
            if type_name.contains("WITH TIME ZONE") || type_name.contains("TIME ZONE") {
                format!("TO_CHAR({column_ref}, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF6TZH:TZM')")
            } else {
                format!("TO_CHAR({column_ref}, 'YYYY-MM-DD HH24:MI:SS.FF6')")
            }
        }
        _ => column_ref,
    }
}

/// 组装增量拉取 SQL，内层负责过滤与排序，外层负责把游标转成文本返回 Rust。
fn build_batch_query(
    table_ref: &str,
    table_name: &str,
    cursor_column: &str,
    lower_bound: Option<&str>,
    batch: usize,
    cursor_plan: &CursorPlan,
) -> String {
    let cursor_expr = format!("t.{}", quote_identifier(cursor_column));
    let payload_expr = &cursor_plan.payload_expr;
    let base_select = if let Some(lower_bound) = lower_bound {
        let lower_bound_expr = cursor_plan
            .lower_bound_sql_literal(lower_bound)
            .unwrap_or_else(|_| "NULL".to_string());
        format!(
            "SELECT t.*, '{}' AS __warp_parse_table, \
                {cursor_expr} AS \"__warp_cursor_value\" \
             FROM {table_ref} t \
             WHERE {cursor_expr} > {lower_bound_expr} \
             ORDER BY {cursor_expr} ASC LIMIT {batch}",
            escape_sql_literal(table_name)
        )
    } else {
        format!(
            "SELECT t.*, '{}' AS __warp_parse_table, \
                {cursor_expr} AS \"__warp_cursor_value\" \
             FROM {table_ref} t \
             ORDER BY {cursor_expr} ASC LIMIT {batch}",
            escape_sql_literal(table_name)
        )
    };

    // 先通过 base 子查询完成 keyset 分页，再只对截断后的 batch 生成 payload，
    // 避免达梦在大结果集上提前执行 JSON_OBJECT 这类昂贵投影。
    // 外层排序仍必须基于原始游标类型，不能按 cast 后的 cursor_value 文本排序，
    // 否则整数游标会退化成字符串字典序，导致 checkpoint 回退并重复拉取整段数据。
    format!(
        "WITH base AS ({base_select}) \
         SELECT CAST(t.\"__warp_cursor_value\" AS VARCHAR(128)) AS cursor_value, \
            {payload_expr} AS payload \
         FROM base t ORDER BY t.\"__warp_cursor_value\" ASC"
    )
}

/// 根据时间列的真实类型推导 lower bound 绑定方式。
fn time_lower_bound_binding(data_type: &DataType, type_name: &str) -> Option<LowerBoundBinding> {
    match data_type {
        DataType::Date => Some(LowerBoundBinding::Date),
        DataType::Timestamp { .. } => {
            let normalized = type_name.to_ascii_uppercase();
            if normalized.contains("WITH TIME ZONE") || normalized.contains("TIME ZONE") {
                Some(LowerBoundBinding::TimestampTz)
            } else {
                Some(LowerBoundBinding::Timestamp)
            }
        }
        _ => {
            let normalized = type_name.to_ascii_uppercase();
            if normalized == "DATE" {
                Some(LowerBoundBinding::Date)
            } else if normalized.contains("TIMESTAMP") {
                if normalized.contains("WITH TIME ZONE") || normalized.contains("TIME ZONE") {
                    Some(LowerBoundBinding::TimestampTz)
                } else {
                    Some(LowerBoundBinding::Timestamp)
                }
            } else {
                None
            }
        }
    }
}

fn is_integer_data_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::TinyInt | DataType::SmallInt | DataType::Integer | DataType::BigInt
    )
}

fn is_numeric_data_type(data_type: &DataType, type_name: &str) -> bool {
    matches!(
        data_type,
        DataType::Numeric { .. }
            | DataType::Decimal { .. }
            | DataType::Float { .. }
            | DataType::Real
            | DataType::Double
    ) || matches!(
        type_name.to_ascii_uppercase().as_str(),
        "NUMBER" | "NUMERIC" | "DECIMAL" | "DOUBLE" | "FLOAT" | "REAL"
    )
}

// ===== checkpoint 与基础配置辅助 =====

/// 校验 checkpoint 的游标信息是否仍与当前 Source 配置兼容。
fn validate_checkpoint_state(
    state: &CheckpointState,
    cursor_column: &str,
    cursor_plan: &CursorPlan,
) -> DmdbResult<()> {
    if state.version != CHECKPOINT_VERSION {
        return Err(dmdb_err(
            DmdbReason::Checkpoint,
            format!(
                "dmdb checkpoint version mismatch: expect {}, got {}",
                CHECKPOINT_VERSION, state.version
            ),
        ));
    }
    if state.cursor_column != cursor_column {
        return Err(dmdb_err(
            DmdbReason::Checkpoint,
            format!(
                "dmdb checkpoint cursor_column mismatch: expect {}, got {}",
                cursor_column, state.cursor_column
            ),
        ));
    }
    if state.cursor_type != cursor_plan.cursor_type.as_str() {
        return Err(dmdb_err(
            DmdbReason::Checkpoint,
            format!(
                "dmdb checkpoint cursor_type mismatch: expect {}, got {}",
                cursor_plan.cursor_type.as_str(),
                state.cursor_type
            ),
        ));
    }
    cursor_plan.validate_lower_bound(&state.last_cursor_raw, "dmdb checkpoint last_cursor_raw")
}

/// 读取必填字符串配置，并去掉首尾空白。
fn required_opt_field<'a>(name: &str, value: &'a Option<String>) -> SourceResult<&'a str> {
    value
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| SourceReason::other(format!("{name} must not be empty")))
}

/// 生成当前 Source 对应的 checkpoint 文件路径。
fn checkpoint_path(source_key: &str) -> PathBuf {
    Path::new("./.run/.checkpoints").join(format!("{source_key}.json"))
}

/// 确保 checkpoint 目录存在，便于后续写入。
fn ensure_checkpoint_dir(path: &Path) -> DmdbResult<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).source_err(
            DmdbReason::Io,
            format!("dmdb create checkpoint dir {} failed", parent.display()),
        )?;
    }
    Ok(())
}

/// checkpoint 一旦存在就优先使用；只有首次启动且无 checkpoint 时才回退到 start_from。
fn resolve_lower_bound<'a>(
    checkpoint: Option<&'a CheckpointState>,
    start_from: Option<&'a str>,
) -> Option<&'a str> {
    checkpoint
        .map(|state| state.last_cursor_raw.as_str())
        .or(start_from)
}

/// 解析 `dmdb.start_from_format` 配置，只允许时间游标使用该选项。
fn parse_start_from_format(
    raw: Option<&str>,
    cursor_type: CursorType,
) -> DmdbResult<Option<StartFromFormat>> {
    let Some(raw) = raw.map(str::trim).filter(|s| !s.is_empty()) else {
        return Ok(None);
    };
    if cursor_type != CursorType::Time {
        return Err(dmdb_err(
            DmdbReason::Config,
            "dmdb.start_from_format is only supported for time cursor",
        ));
    }
    let kind = match raw {
        "unix" | "unix_s" => StartFromFormatKind::UnixSeconds,
        "unix_ms" => StartFromFormatKind::UnixMillis,
        _ => StartFromFormatKind::Pattern,
    };
    Ok(Some(StartFromFormat {
        raw: raw.to_string(),
        kind,
    }))
}

/// 将用户输入的 `start_from` 归一化为数据库比较时使用的标准文本形式。
fn normalize_optional_start_from(
    cursor_plan: &CursorPlan,
    raw: Option<&str>,
    format: Option<&StartFromFormat>,
    session_offset: FixedOffset,
) -> DmdbResult<Option<String>> {
    raw.map(|raw| cursor_plan.normalize_start_from(raw, format, session_offset))
        .transpose()
}

// ===== start_from 解析与时间归一化 =====

/// 归一化 DATE 游标的 `start_from` 输入。
fn normalize_date_start_from(
    raw: &str,
    format: Option<&StartFromFormat>,
    session_offset: FixedOffset,
) -> DmdbResult<String> {
    let date = if let Some(format) = format {
        parse_date_by_format(raw, format, session_offset)?
    } else {
        parse_date_fallback(raw, session_offset)?
    };
    Ok(date.format("%Y-%m-%d").to_string())
}

/// 归一化 TIMESTAMP（无时区）游标的 `start_from` 输入。
fn normalize_timestamp_start_from(
    raw: &str,
    format: Option<&StartFromFormat>,
    session_offset: FixedOffset,
) -> DmdbResult<String> {
    let datetime = if let Some(format) = format {
        parse_timestamp_by_format(raw, format, session_offset)?
    } else {
        parse_timestamp_fallback(raw, session_offset)?
    };
    Ok(datetime.format("%Y-%m-%d %H:%M:%S%.6f").to_string())
}

/// 归一化 TIMESTAMP WITH TIME ZONE 游标的 `start_from` 输入。
fn normalize_timestamptz_start_from(
    raw: &str,
    format: Option<&StartFromFormat>,
    session_offset: FixedOffset,
) -> DmdbResult<String> {
    let datetime = if let Some(format) = format {
        parse_timestamptz_by_format(raw, format, session_offset)?
    } else {
        parse_timestamptz_fallback(raw, session_offset)?
    };
    Ok(datetime.to_rfc3339_opts(SecondsFormat::Micros, true))
}

/// 按显式格式把输入解析为日期。
fn parse_date_by_format(
    raw: &str,
    format: &StartFromFormat,
    session_offset: FixedOffset,
) -> DmdbResult<NaiveDate> {
    match format.kind {
        StartFromFormatKind::UnixSeconds => Ok(parse_unix_seconds(raw)?
            .with_timezone(&session_offset)
            .date_naive()),
        StartFromFormatKind::UnixMillis => Ok(parse_unix_millis(raw)?
            .with_timezone(&session_offset)
            .date_naive()),
        StartFromFormatKind::Pattern => {
            if pattern_has_offset(&format.raw) {
                Ok(DateTime::parse_from_str(raw, &format.raw)
                    .source_raw_err(DmdbReason::Time, "dmdb.start_from parse failed")?
                    .date_naive())
            } else if pattern_has_time(&format.raw) {
                Ok(NaiveDateTime::parse_from_str(raw, &format.raw)
                    .source_raw_err(DmdbReason::Time, "dmdb.start_from parse failed")?
                    .date())
            } else {
                NaiveDate::parse_from_str(raw, &format.raw)
                    .source_raw_err(DmdbReason::Time, "dmdb.start_from parse failed")
            }
        }
    }
}

/// 按显式格式把输入解析为无时区时间。
fn parse_timestamp_by_format(
    raw: &str,
    format: &StartFromFormat,
    session_offset: FixedOffset,
) -> DmdbResult<NaiveDateTime> {
    match format.kind {
        StartFromFormatKind::UnixSeconds => Ok(parse_unix_seconds(raw)?
            .with_timezone(&session_offset)
            .naive_local()),
        StartFromFormatKind::UnixMillis => Ok(parse_unix_millis(raw)?
            .with_timezone(&session_offset)
            .naive_local()),
        StartFromFormatKind::Pattern => {
            if pattern_has_offset(&format.raw) {
                Ok(DateTime::parse_from_str(raw, &format.raw)
                    .source_raw_err(DmdbReason::Time, "dmdb.start_from parse failed")?
                    .naive_local())
            } else if pattern_has_time(&format.raw) {
                NaiveDateTime::parse_from_str(raw, &format.raw)
                    .source_raw_err(DmdbReason::Time, "dmdb.start_from parse failed")
            } else {
                Ok(NaiveDate::parse_from_str(raw, &format.raw)
                    .source_raw_err(DmdbReason::Time, "dmdb.start_from parse failed")?
                    .and_hms_opt(0, 0, 0)
                    .ok_or_else(|| dmdb_err(DmdbReason::Time, "dmdb.start_from parse failed"))?)
            }
        }
    }
}

/// 按显式格式把输入解析为带固定时区偏移的时间。
fn parse_timestamptz_by_format(
    raw: &str,
    format: &StartFromFormat,
    session_offset: FixedOffset,
) -> DmdbResult<DateTime<FixedOffset>> {
    match format.kind {
        StartFromFormatKind::UnixSeconds => {
            Ok(parse_unix_seconds(raw)?.with_timezone(&session_offset))
        }
        StartFromFormatKind::UnixMillis => {
            Ok(parse_unix_millis(raw)?.with_timezone(&session_offset))
        }
        StartFromFormatKind::Pattern => {
            if pattern_has_offset(&format.raw) {
                DateTime::parse_from_str(raw, &format.raw)
                    .source_raw_err(DmdbReason::Time, "dmdb.start_from parse failed")
            } else if pattern_has_time(&format.raw) {
                let naive = NaiveDateTime::parse_from_str(raw, &format.raw)
                    .source_raw_err(DmdbReason::Time, "dmdb.start_from parse failed")?;
                bind_naive_to_fixed_offset(naive, session_offset, "dmdb.start_from")
            } else {
                let naive = NaiveDate::parse_from_str(raw, &format.raw)
                    .source_raw_err(DmdbReason::Time, "dmdb.start_from parse failed")?
                    .and_hms_opt(0, 0, 0)
                    .ok_or_else(|| dmdb_err(DmdbReason::Time, "dmdb.start_from parse failed"))?;
                bind_naive_to_fixed_offset(naive, session_offset, "dmdb.start_from")
            }
        }
    }
}

/// 在未显式指定格式时，按常见日期表示自动回退解析。
fn parse_date_fallback(raw: &str, session_offset: FixedOffset) -> DmdbResult<NaiveDate> {
    if let Ok(date) = NaiveDate::parse_from_str(raw, "%Y-%m-%d") {
        return Ok(date);
    }
    if let Ok(dt) = DateTime::parse_from_rfc3339(raw) {
        return Ok(dt.date_naive());
    }
    if let Ok(dt) = parse_naive_datetime_fallback(raw) {
        return Ok(dt.date());
    }
    let dt = parse_unix_auto(raw)?.with_timezone(&session_offset);
    Ok(dt.date_naive())
}

/// 在未显式指定格式时，按常见时间表示自动回退解析。
fn parse_timestamp_fallback(raw: &str, session_offset: FixedOffset) -> DmdbResult<NaiveDateTime> {
    if let Ok(dt) = parse_naive_datetime_fallback(raw) {
        return Ok(dt);
    }
    if let Ok(dt) = DateTime::parse_from_rfc3339(raw) {
        return Ok(dt.naive_local());
    }
    if let Ok(date) = NaiveDate::parse_from_str(raw, "%Y-%m-%d") {
        return date
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| dmdb_err(DmdbReason::Time, "dmdb.start_from parse failed"));
    }
    Ok(parse_unix_auto(raw)?
        .with_timezone(&session_offset)
        .naive_local())
}

/// 在未显式指定格式时，按常见带时区时间表示自动回退解析。
fn parse_timestamptz_fallback(
    raw: &str,
    session_offset: FixedOffset,
) -> DmdbResult<DateTime<FixedOffset>> {
    if let Ok(dt) = DateTime::parse_from_rfc3339(raw) {
        return Ok(dt);
    }
    if let Ok(dt) = parse_naive_datetime_fallback(raw) {
        return bind_naive_to_fixed_offset(dt, session_offset, "dmdb.start_from");
    }
    if let Ok(date) = NaiveDate::parse_from_str(raw, "%Y-%m-%d") {
        let naive = date
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| dmdb_err(DmdbReason::Time, "dmdb.start_from parse failed"))?;
        return bind_naive_to_fixed_offset(naive, session_offset, "dmdb.start_from");
    }
    Ok(parse_unix_auto(raw)?.with_timezone(&session_offset))
}

/// 自动识别 10 位秒级或 13 位毫秒级 unix 时间戳。
fn parse_unix_auto(raw: &str) -> DmdbResult<DateTime<Utc>> {
    match raw.len() {
        13 => parse_unix_millis(raw),
        10 => parse_unix_seconds(raw),
        _ => Err(dmdb_err(DmdbReason::Time, "dmdb.start_from parse failed")),
    }
}

/// 解析秒级 unix 时间戳。
fn parse_unix_seconds(raw: &str) -> DmdbResult<DateTime<Utc>> {
    let secs = raw.parse::<i64>().map_err(|err| {
        dmdb_err(
            DmdbReason::Time,
            format!("dmdb.start_from parse unix seconds failed: {err}"),
        )
    })?;
    DateTime::<Utc>::from_timestamp(secs, 0).ok_or_else(|| {
        dmdb_err(
            DmdbReason::Time,
            "dmdb.start_from unix seconds out of range",
        )
    })
}

/// 解析毫秒级 unix 时间戳。
fn parse_unix_millis(raw: &str) -> DmdbResult<DateTime<Utc>> {
    let millis = raw.parse::<i64>().map_err(|err| {
        dmdb_err(
            DmdbReason::Time,
            format!("dmdb.start_from parse unix milliseconds failed: {err}"),
        )
    })?;
    DateTime::<Utc>::from_timestamp_millis(millis).ok_or_else(|| {
        dmdb_err(
            DmdbReason::Time,
            "dmdb.start_from unix milliseconds out of range",
        )
    })
}

/// 按常见无时区时间格式解析输入。
fn parse_naive_datetime_fallback(raw: &str) -> Result<NaiveDateTime, chrono::ParseError> {
    NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(raw, "%Y-%m-%dT%H:%M:%S%.f"))
}

/// 将无时区时间绑定到固定偏移，避免时区丢失。
fn bind_naive_to_fixed_offset(
    naive: NaiveDateTime,
    offset: FixedOffset,
    field_name: &str,
) -> DmdbResult<DateTime<FixedOffset>> {
    offset.from_local_datetime(&naive).single().ok_or_else(|| {
        dmdb_err(
            DmdbReason::Time,
            format!("{field_name} is ambiguous or invalid in fixed timezone"),
        )
    })
}

/// 判断格式串是否显式包含时区偏移。
fn pattern_has_offset(pattern: &str) -> bool {
    pattern.contains("%z") || pattern.contains("%:z")
}

/// 判断格式串是否包含时间部分。
fn pattern_has_time(pattern: &str) -> bool {
    pattern.contains("%H")
        || pattern.contains("%M")
        || pattern.contains("%S")
        || pattern.contains("%T")
        || pattern.contains("%R")
}

// ===== 表引用与 schema 归一化 =====

/// 归一化 schema 配置，空白值视为未设置。
fn normalized_schema(schema: Option<&str>) -> Option<String> {
    schema
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

/// 生成带可选 schema 的达梦表引用。
fn qualified_table_name(schema: Option<&str>, table: &str) -> String {
    match schema {
        Some(schema) => format!("{}.{}", quote_identifier(schema), quote_identifier(table)),
        None => quote_identifier(table),
    }
}

// ===== 测试 =====

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dmdb::common::quote_identifier;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn test_cursor_plan(
        cursor_type: CursorType,
        lower_bound_binding: LowerBoundBinding,
    ) -> CursorPlan {
        CursorPlan {
            cursor_type,
            lower_bound_binding,
            payload_expr: "JSON_OBJECT('name' VALUE t.\"name\")".into(),
        }
    }

    fn temp_checkpoint_path(name: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time before epoch")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "wp-connectors-dmdb-source-{name}-{}-{unique}.json",
            std::process::id()
        ))
    }

    #[test]
    fn quote_identifier_escapes_quotes_via_sink_helper() {
        assert_eq!(quote_identifier("a\"b"), "\"a\"\"b\"");
    }

    #[test]
    fn validate_source_cursor_type_rejects_invalid_type() {
        let err = validate_source_cursor_type_and_start_from("string", None, None)
            .expect_err("invalid cursor type should fail");
        assert!(err.to_string().contains("unsupported dmdb cursor_type"));
    }

    #[test]
    fn validate_source_start_from_format_requires_time_cursor() {
        let err = validate_source_cursor_type_and_start_from("int", Some("1"), Some("unix"))
            .expect_err("int cursor should reject start_from_format");
        assert!(err.to_string().contains("only supported for time cursor"));
    }

    #[test]
    fn resolve_lower_bound_prefers_checkpoint() {
        let checkpoint = CheckpointState {
            version: CHECKPOINT_VERSION,
            cursor_type: "int".into(),
            cursor_column: "id".into(),
            last_cursor_raw: "10".into(),
            updated_at: "2026-04-27T00:00:00Z".into(),
        };
        assert_eq!(
            resolve_lower_bound(Some(&checkpoint), Some("1")),
            Some("10")
        );
    }

    #[test]
    fn build_batch_query_without_lower_bound_omits_where() {
        let sql = build_batch_query(
            "\"T_EVENTS\"",
            "T_EVENTS",
            "id",
            None,
            100,
            &test_cursor_plan(CursorType::Int, LowerBoundBinding::Integer),
        );
        assert!(sql.contains("ORDER BY t.\"id\" ASC LIMIT 100"));
        assert!(!sql.contains("WHERE t.\"id\" >"));
        assert!(sql.contains("WITH base AS (SELECT t.*, 'T_EVENTS' AS __warp_parse_table"));
        assert!(
            sql.contains("SELECT CAST(t.\"__warp_cursor_value\" AS VARCHAR(128)) AS cursor_value")
        );
    }

    #[test]
    fn build_batch_query_with_lower_bound_contains_predicate() {
        let sql = build_batch_query(
            "\"T_EVENTS\"",
            "T_EVENTS",
            "id",
            Some("42"),
            100,
            &test_cursor_plan(CursorType::Int, LowerBoundBinding::Integer),
        );
        assert!(sql.contains("WHERE t.\"id\" > 42"));
        assert!(sql.contains("FROM base t ORDER BY t.\"__warp_cursor_value\" ASC"));
    }

    #[test]
    fn build_batch_query_outer_order_keeps_original_cursor_order() {
        let sql = build_batch_query(
            "\"T_EVENTS\"",
            "T_EVENTS",
            "id",
            Some("42"),
            100,
            &test_cursor_plan(CursorType::Int, LowerBoundBinding::Integer),
        );
        assert!(sql.contains("ORDER BY t.\"__warp_cursor_value\" ASC"));
        assert!(!sql.contains("ORDER BY dmdb_source_batch.cursor_value ASC"));
    }

    #[test]
    fn checkpoint_empty_file_is_treated_as_none() {
        let path = temp_checkpoint_path("empty");
        std::fs::create_dir_all(path.parent().expect("has parent")).expect("create dir");
        std::fs::write(&path, "").expect("write empty checkpoint");
        let state = DmdbSource::load_checkpoint(
            &path,
            "id",
            &test_cursor_plan(CursorType::Int, LowerBoundBinding::Integer),
        )
        .expect("load empty checkpoint");
        assert!(state.is_none());
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn checkpoint_incompatible_cursor_type_fails() {
        let path = temp_checkpoint_path("mismatch");
        std::fs::create_dir_all(path.parent().expect("has parent")).expect("create dir");
        let state = CheckpointState {
            version: CHECKPOINT_VERSION,
            cursor_type: "time".into(),
            cursor_column: "id".into(),
            last_cursor_raw: "2026-04-27".into(),
            updated_at: "2026-04-27T00:00:00Z".into(),
        };
        std::fs::write(
            &path,
            serde_json::to_string(&state).expect("serialize checkpoint"),
        )
        .expect("write checkpoint");
        let err = DmdbSource::load_checkpoint(
            &path,
            "id",
            &test_cursor_plan(CursorType::Int, LowerBoundBinding::Integer),
        )
        .expect_err("mismatch checkpoint should fail");
        assert!(err.to_string().contains("cursor_type mismatch"));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn normalize_time_start_from_supports_unix_millis() {
        let start_from = normalize_optional_start_from(
            &test_cursor_plan(CursorType::Time, LowerBoundBinding::Timestamp),
            Some("1714478400000"),
            Some(&StartFromFormat {
                raw: "unix_ms".into(),
                kind: StartFromFormatKind::UnixMillis,
            }),
            FixedOffset::east_opt(0).expect("utc offset"),
        )
        .expect("normalize unix millis");
        assert_eq!(start_from.as_deref(), Some("2024-04-30 12:00:00.000000"));
    }

    #[test]

    fn build_payload_expr_adds_table_field() {
        let columns = vec![
            DmdbColumnMeta {
                name: "id".into(),
                type_name: "INTEGER".into(),
                data_type: DataType::Integer,
                ordinal_position: 1,
            },
            DmdbColumnMeta {
                name: "name".into(),
                type_name: "VARCHAR".into(),
                data_type: DataType::Varchar { length: None },
                ordinal_position: 2,
            },
        ];
        let expr = build_payload_expr(&columns).expect("build payload expr");
        assert!(expr.contains("'id' VALUE t.\"id\""));
        assert!(expr.contains("'name' VALUE t.\"name\""));
        assert!(expr.contains("'warp_parse_table' VALUE t.__warp_parse_table"));
        assert!(expr.contains("AS CLOB"));
    }
}
