use crate::postgres::config::PostgresConf;
use async_trait::async_trait;
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, SecondsFormat, TimeZone, Utc};
use sea_orm::sea_query::Values;
use sea_orm::{ConnectOptions, ConnectionTrait, Database, DatabaseConnection, Statement, Value};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::time::sleep;
use wp_connector_api::{DataSource, SourceBatch, SourceEvent, SourceReason, SourceResult, Tags};
use wp_log::{info_data, warn_data};
use wp_model_core::event_id::next_wp_event_id;
use wp_model_core::raw::RawData;

type AnyResult<T> = anyhow::Result<T>;

const DEFAULT_BATCH: usize = 1000;
const DEFAULT_POLL_INTERVAL_MS: u64 = 1000;
const DEFAULT_ERROR_BACKOFF_MS: u64 = 2000;
// checkpoint 文件结构版本。只有 checkpoint 字段语义发生不兼容变化时才需要手动升级。
const CHECKPOINT_VERSION: u32 = 1;

// ===== Source 主流程 =====

pub struct PostgresSource {
    key: String,
    db: DatabaseConnection,
    table_ref: String,
    cursor_column: String,
    cursor_plan: CursorPlan,
    batch: usize,
    poll_interval: Duration,
    error_backoff: Duration,
    checkpoint_path: PathBuf,
    checkpoint: Option<CheckpointState>,
    start_from: Option<String>,
    tags: Tags,
}

impl PostgresSource {
    /// 返回当前 source 的内部标识。
    pub fn identifier(&self) -> &str {
        &self.key
    }

    /// 根据配置创建 PostgreSQL Source，并完成连接、游标计划、时区和 checkpoint 初始化。
    pub async fn new(key: String, tags: Tags, config: &PostgresConf) -> AnyResult<Self> {
        let table = required_opt_field("postgres.table", &config.table)?;
        let cursor_column = required_opt_field("postgres.cursor_column", &config.cursor_column)?;
        let cursor_type = CursorType::from_config(&config.cursor_type)?;

        let mut opt = ConnectOptions::new(config.get_database_url());
        opt.max_connections(3)
            .min_connections(1)
            .connect_timeout(Duration::from_secs(8))
            .acquire_timeout(Duration::from_secs(8))
            .idle_timeout(Duration::from_secs(8))
            .max_lifetime(Duration::from_secs(8))
            .sqlx_logging(false)
            .map_sqlx_postgres_opts(|opt| opt.statement_cache_capacity(0))
            .sqlx_logging_level(log::LevelFilter::Info);
        let db = Database::connect(opt).await?;

        let cursor_plan =
            CursorPlan::build(&db, &config.schema, table, cursor_column, cursor_type).await?;
        // 无时区 start_from 和 Unix 时间戳按 PostgreSQL 当前连接的 session TimeZone 解释。
        // 这里在 Source 启动时固定一次，避免运行中数据库配置变化导致首次起点语义漂移。
        let session_tz = query_session_time_zone(&db).await?;
        // start_from_format 只描述“用户输入怎么解析”；真正输出什么格式由数据库列类型决定。
        // 例如同样输入秒级时间，date 列最终只保留 YYYY-MM-DD。
        let start_from_format =
            parse_start_from_format(config.start_from_format.as_deref(), cursor_type)?;
        let start_from = normalize_optional_start_from(
            &cursor_plan,
            config.start_from.as_deref(),
            start_from_format.as_ref(),
            session_tz.offset,
        )?;

        let batch = config.batch.unwrap_or(DEFAULT_BATCH);
        let poll_interval =
            Duration::from_millis(config.poll_interval_ms.unwrap_or(DEFAULT_POLL_INTERVAL_MS));
        let error_backoff =
            Duration::from_millis(config.error_backoff_ms.unwrap_or(DEFAULT_ERROR_BACKOFF_MS));

        let table_ref = format!("{}.{}", quote_ident(&config.schema), quote_ident(table));
        let checkpoint_path = checkpoint_path(&key);
        let checkpoint = Self::load_checkpoint(&checkpoint_path, cursor_column, &cursor_plan)?;
        // checkpoint 优先于 start_from。start_from 只在首次启动且没有 checkpoint 时作为起点。
        if let Some(lower_bound) = resolve_lower_bound(checkpoint.as_ref(), start_from.as_deref()) {
            cursor_plan
                .validate_active_lower_bound(&db, lower_bound)
                .await?;
        }

        info_data!(
            "[postgres-source] schema: {}, table: {}, cursor_column: {}, cursor_type: {:?}, session_timezone: {}",
            config.schema,
            table,
            cursor_column,
            cursor_type,
            session_tz.name
        );

        Ok(Self {
            key,
            db,
            table_ref,
            cursor_column: cursor_column.to_string(),
            cursor_plan,
            batch,
            poll_interval,
            error_backoff,
            checkpoint_path,
            checkpoint,
            start_from,
            tags,
        })
    }

    /// 阻塞式轮询下一批数据；无数据时按 poll_interval_ms 等待，查询失败时按 error_backoff_ms 退避。
    async fn recv_impl(&mut self) -> SourceResult<SourceBatch> {
        loop {
            let rows: Vec<(String, String)> = match self.query_next_batch().await {
                Ok(rows) => rows,
                Err(err) => {
                    warn_data!(
                        "[postgres-source] query failed, backing off {:?}: {}",
                        self.error_backoff,
                        err
                    );
                    sleep(self.error_backoff).await;
                    continue;
                }
            };

            if rows.is_empty() {
                sleep(self.poll_interval).await;
                continue;
            }

            let mut batch = Vec::with_capacity(rows.len());
            let mut last_cursor_raw = None;
            for (cursor_raw, payload) in rows {
                batch.push(SourceEvent::new(
                    next_wp_event_id(),
                    self.key.clone(),
                    RawData::from_string(payload),
                    self.tags.clone().into(),
                ));
                last_cursor_raw = Some(cursor_raw);
            }

            if let Some(last_cursor_raw) = last_cursor_raw {
                self.persist_checkpoint(last_cursor_raw)?;
            }
            return Ok(batch);
        }
    }

    /// 执行下一批增量查询，返回游标原始文本和 JSON payload。
    async fn query_next_batch(&self) -> SourceResult<Vec<(String, String)>> {
        let statement = self.build_query_statement()?;

        let rows = self.db.query_all(statement).await.map_err(|err| {
            SourceReason::SupplierError(format!("postgres query batch failed: {err}"))
        })?;

        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let cursor_raw: String = row.try_get_by("cursor_value").map_err(|err| {
                SourceReason::Other(format!("postgres source read cursor_value failed: {err}"))
            })?;
            let payload: String = row.try_get_by("payload").map_err(|err: sea_orm::DbErr| {
                SourceReason::Other(format!("postgres source read payload failed: {err}"))
            })?;
            out.push((cursor_raw, payload));
        }
        Ok(out)
    }

    /// 解析当前活动下界；checkpoint 存在时优先使用 checkpoint，否则使用 start_from。
    fn lower_bound_raw(&self) -> Option<&str> {
        resolve_lower_bound(self.checkpoint.as_ref(), self.start_from.as_deref())
    }

    /// 按当前下界状态构建 PostgreSQL 查询语句和绑定参数。
    fn build_query_statement(&self) -> SourceResult<Statement> {
        let lower_bound = self.lower_bound_raw();
        let sql = build_batch_query(
            &self.table_ref,
            &self.cursor_column,
            lower_bound.is_some(),
            &self.cursor_plan,
        );
        let table_name = table_name_from_ref(&self.table_ref);

        let values = if let Some(lower_bound) = lower_bound {
            vec![
                self.cursor_plan
                    .lower_bound_into_value(lower_bound)
                    .map_err(|err| {
                        SourceReason::Other(format!("postgres resolve lower bound failed: {err}"))
                    })?,
                Value::BigUnsigned(Some(self.batch as u64)),
                Value::String(Some(Box::new(table_name))),
            ]
        } else {
            vec![
                Value::BigUnsigned(Some(self.batch as u64)),
                Value::String(Some(Box::new(table_name))),
            ]
        };

        Ok(Statement::from_sql_and_values(
            self.db.get_database_backend(),
            sql,
            Values(values),
        ))
    }

    /// 读取并校验 checkpoint；文件不存在或为空时视为没有 checkpoint。
    fn load_checkpoint(
        path: &Path,
        cursor_column: &str,
        cursor_plan: &CursorPlan,
    ) -> AnyResult<Option<CheckpointState>> {
        if !path.exists() {
            ensure_checkpoint_dir(path)?;
            return Ok(None);
        }

        let contents = std::fs::read_to_string(path)?;
        if contents.trim().is_empty() {
            return Ok(None);
        }

        let state: CheckpointState = serde_json::from_str(&contents).map_err(|err| {
            anyhow::anyhow!(
                "postgres checkpoint file {} is invalid: {}; if you changed source cursor config, delete this checkpoint and restart",
                path.display(),
                err
            )
        })?;
        validate_checkpoint_state(&state, cursor_column, cursor_plan).map_err(|err| {
            anyhow::anyhow!(
                "postgres checkpoint {} is incompatible with current config: {}; if you changed cursor_column/cursor_type or want to restart from a new cursor, delete this checkpoint and restart",
                path.display(),
                err
            )
        })?;
        Ok(Some(state))
    }

    /// 将当前批次最后一条游标值写入 checkpoint，并同步更新内存状态。
    fn persist_checkpoint(&mut self, last_cursor_raw: String) -> SourceResult<()> {
        ensure_checkpoint_dir(&self.checkpoint_path).map_err(|err| {
            SourceReason::Other(format!("postgres ensure checkpoint dir failed: {err}"))
        })?;

        let state = CheckpointState {
            version: CHECKPOINT_VERSION,
            cursor_type: self.cursor_plan.cursor_type.as_str().to_string(),
            cursor_column: self.cursor_column.clone(),
            last_cursor_raw,
            updated_at: chrono::Utc::now().to_rfc3339(),
        };
        let content = serde_json::to_string_pretty(&state).map_err(|err| {
            SourceReason::Other(format!("postgres serialize checkpoint failed: {err}"))
        })?;

        std::fs::write(&self.checkpoint_path, content).map_err(|err| {
            SourceReason::Other(format!("postgres write checkpoint failed: {err}"))
        })?;
        self.checkpoint = Some(state);
        Ok(())
    }
}

#[async_trait]
impl DataSource for PostgresSource {
    /// DataSource 入口：阻塞等待并返回下一批 SourceEvent。
    async fn receive(&mut self) -> SourceResult<SourceBatch> {
        self.recv_impl().await
    }

    /// 当前 PostgreSQL Source 只支持阻塞 receive，非阻塞读取固定返回 None。
    fn try_receive(&mut self) -> Option<SourceBatch> {
        None
    }

    /// 返回 DataSource 对外暴露的标识。
    fn identifier(&self) -> String {
        self.key.clone()
    }
}

// ===== 对外校验入口 =====

/// 校验 source 配置中的 cursor_type、start_from 和 start_from_format 基础约束。
pub(crate) fn validate_source_cursor_type_and_start_from(
    raw_cursor_type: &str,
    start_from: Option<&str>,
    start_from_format: Option<&str>,
) -> AnyResult<()> {
    let cursor_type = CursorType::from_config(raw_cursor_type)?;
    cursor_type.validate_start_from(start_from, start_from_format)?;
    Ok(())
}

// ===== 核心类型定义 =====

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CursorType {
    // 数字游标：整数族、numeric、decimal，按数据库数值比较推进。
    Int,
    // 时间游标：仅支持 PostgreSQL 原生 date/timestamp/timestamptz 类型。
    Time,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LowerBoundBinding {
    // 整数族可以直接绑定为 i64，SQL 中直接使用 $1。
    Integer,
    // numeric 和时间类参数以字符串绑定，再在 SQL 参数侧追加 PostgreSQL cast。
    // 这样不会对游标列本身做函数转换，仍然可以利用游标列索引。
    TextParamWithCast(&'static str),
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CursorPlan {
    cursor_type: CursorType,
    lower_bound_binding: LowerBoundBinding,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DbSessionTimeZone {
    name: String,
    offset: FixedOffset,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StartFromFormatKind {
    // start_from 是 Unix 秒。
    UnixSeconds,
    // start_from 是 Unix 毫秒。
    UnixMillis,
    // start_from 是 chrono 自定义格式，例如 %Y-%m-%d %H:%M:%S。
    Pattern,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct StartFromFormat {
    raw: String,
    kind: StartFromFormatKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CheckpointState {
    version: u32,
    cursor_type: String,
    cursor_column: String,
    last_cursor_raw: String,
    updated_at: String,
}

// ===== 游标与配置逻辑 =====

impl CursorType {
    /// 从配置字符串解析游标类型。
    fn from_config(raw: &str) -> AnyResult<Self> {
        match raw {
            "int" => Ok(Self::Int),
            "time" => Ok(Self::Time),
            other => anyhow::bail!("unsupported postgres cursor_type: {other}"),
        }
    }

    /// 返回 checkpoint 中保存的游标类型字符串。
    fn as_str(self) -> &'static str {
        match self {
            Self::Int => "int",
            Self::Time => "time",
        }
    }

    /// 校验 start_from 相关参数是否符合当前游标类型约束。
    fn validate_start_from(
        self,
        start_from: Option<&str>,
        start_from_format: Option<&str>,
    ) -> AnyResult<()> {
        let Some(start_from) = start_from else {
            if start_from_format.is_some() {
                anyhow::bail!("postgres.start_from_format requires postgres.start_from");
            }
            return Ok(());
        };

        if start_from.trim().is_empty() {
            anyhow::bail!(
                "postgres.start_from must not be empty for {} cursor",
                self.as_str()
            );
        }
        if start_from_format.is_some() && self != CursorType::Time {
            anyhow::bail!("postgres.start_from_format is only supported for time cursor");
        }
        Ok(())
    }
    /// 根据配置游标类型和数据库真实 data_type 确定下界绑定策略。
    fn lower_bound_binding(
        self,
        cursor_column: &str,
        data_type: &str,
    ) -> AnyResult<LowerBoundBinding> {
        let ty = data_type.to_ascii_lowercase();
        match self {
            Self::Int => {
                if is_integer_type(&ty) {
                    Ok(LowerBoundBinding::Integer)
                } else if is_decimal_type(&ty) {
                    Ok(LowerBoundBinding::TextParamWithCast("numeric"))
                } else {
                    anyhow::bail!(
                        "postgres source cursor_column in database {} must be numeric-like for cursor_type=int, got {}",
                        cursor_column,
                        data_type
                    );
                }
            }
            Self::Time => {
                let Some(lower_bound_cast) = time_lower_bound_cast(&ty) else {
                    anyhow::bail!(
                        "postgres source cursor_column {} must be timestamp/date-like for cursor_type=time, got {}",
                        cursor_column,
                        data_type
                    );
                };
                Ok(LowerBoundBinding::TextParamWithCast(lower_bound_cast))
            }
        }
    }
}

impl CursorPlan {
    /// 根据数据库中的真实列类型构建游标执行计划。
    async fn build(
        db: &DatabaseConnection,
        schema: &str,
        table: &str,
        cursor_column: &str,
        cursor_type: CursorType,
    ) -> AnyResult<Self> {
        let data_type = query_cursor_data_type(db, schema, table, cursor_column).await?;
        let lower_bound_binding = cursor_type.lower_bound_binding(cursor_column, &data_type)?;
        Ok(Self {
            cursor_type,
            lower_bound_binding,
        })
    }

    /// 生成 SQL 中使用的下界参数表达式，例如 `$1` 或 `$1::timestamptz`。
    fn lower_bound_expr(&self) -> String {
        match self.lower_bound_binding {
            LowerBoundBinding::Integer => "$1".to_string(),
            // 只 cast 参数，不 cast 列，避免 WHERE 中的游标列表达式失去索引友好性。
            LowerBoundBinding::TextParamWithCast(lower_bound_cast) => {
                format!("$1::{lower_bound_cast}")
            }
        }
    }

    /// 将下界原始文本转换为 SeaORM 参数值。
    fn lower_bound_into_value(&self, raw: &str) -> AnyResult<Value> {
        self.validate_lower_bound(raw, "postgres lower bound")?;
        match self.lower_bound_binding {
            LowerBoundBinding::Integer => Ok(Value::BigInt(Some(raw.parse::<i64>()?))),
            // numeric/decimal 要保留原始精度文本；时间类也让 PostgreSQL 按目标类型 cast。
            LowerBoundBinding::TextParamWithCast(_) => {
                Ok(Value::String(Some(Box::new(raw.to_string()))))
            }
        }
    }

    /// 对当前将要使用的下界做启动期校验，提前暴露 checkpoint/start_from 格式错误。
    async fn validate_active_lower_bound(
        &self,
        db: &DatabaseConnection,
        raw: &str,
    ) -> AnyResult<()> {
        self.validate_lower_bound(raw, "postgres active lower bound")?;
        let LowerBoundBinding::TextParamWithCast(lower_bound_cast) = self.lower_bound_binding
        else {
            return Ok(());
        };

        // 构建 Source 时先让 PostgreSQL 校验一次下界值是否能 cast 成目标列类型。
        // 这样 start_from 或 checkpoint 不合法时可以在启动阶段尽早失败。
        let sql = format!("SELECT ($1::{lower_bound_cast})::text AS lower_bound_value");
        let stmt = Statement::from_sql_and_values(
            db.get_database_backend(),
            sql,
            Values(vec![Value::String(Some(Box::new(raw.to_string())))]),
        );
        db.query_one(stmt).await.map_err(|err| {
            anyhow::anyhow!(
                "postgres active lower bound {} cannot be cast to {}: {}",
                raw,
                lower_bound_cast,
                err
            )
        })?;
        Ok(())
    }

    /// 对下界值做 Rust 侧基础校验，整数游标会先校验为 i64。
    fn validate_lower_bound(&self, raw: &str, field_name: &str) -> AnyResult<()> {
        if raw.trim().is_empty() {
            anyhow::bail!("{field_name} must not be empty");
        }
        if self.lower_bound_binding == LowerBoundBinding::Integer {
            raw.parse::<i64>()
                .map(|_| ())
                .map_err(|err| anyhow::anyhow!("{field_name} must be an integer: {err}"))?;
        }
        Ok(())
    }

    /// 只对首次启动用的 start_from 做归一化；checkpoint 值保持数据库返回原文。
    fn normalize_start_from(
        &self,
        raw: &str,
        format: Option<&StartFromFormat>,
        session_offset: FixedOffset,
    ) -> AnyResult<String> {
        self.validate_lower_bound(raw, "postgres.start_from")?;
        match self.lower_bound_binding {
            LowerBoundBinding::Integer => Ok(raw.to_string()),
            LowerBoundBinding::TextParamWithCast("numeric") => Ok(raw.to_string()),
            // 时间游标需要先按用户输入格式解析，再按数据库列类型归一化成下界文本。
            LowerBoundBinding::TextParamWithCast(lower_bound_cast) => {
                normalize_time_start_from(raw, format, lower_bound_cast, session_offset)
            }
        }
    }
}

// ===== SQL 与数据库辅助 =====

/// 查询游标列在 PostgreSQL information_schema 中的 data_type。
async fn query_cursor_data_type(
    db: &DatabaseConnection,
    schema: &str,
    table: &str,
    cursor_column: &str,
) -> AnyResult<String> {
    let sql = "SELECT data_type \
          FROM information_schema.columns \
          WHERE table_schema = $1 AND table_name = $2 AND column_name = $3";
    let stmt = Statement::from_sql_and_values(
        db.get_database_backend(),
        sql,
        Values(vec![
            Value::String(Some(Box::new(schema.to_string()))),
            Value::String(Some(Box::new(table.to_string()))),
            Value::String(Some(Box::new(cursor_column.to_string()))),
        ]),
    );

    let row = db.query_one(stmt).await?;
    let Some(row) = row else {
        anyhow::bail!(
            "postgres source cursor_column not found: {}.{}.{}",
            schema,
            table,
            cursor_column
        );
    };

    Ok(row.try_get_by_index(0)?)
}

/// 查询当前连接的 PostgreSQL session TimeZone，并固定启动时 UTC offset。
async fn query_session_time_zone(db: &DatabaseConnection) -> AnyResult<DbSessionTimeZone> {
    let timezone_stmt = Statement::from_string(db.get_database_backend(), "SHOW TIME ZONE");
    let timezone_row = db
        .query_one(timezone_stmt)
        .await?
        .ok_or_else(|| anyhow::anyhow!("postgres SHOW TIME ZONE returned no row"))?;
    let name: String = timezone_row.try_get_by_index(0)?;

    // PostgreSQL session TimeZone 可能是 Asia/Shanghai 这类 IANA 名称，也可能是 UTC/+08。
    // chrono::FixedOffset 不能解析完整 IANA 名称，所以这里让 PostgreSQL 按当前连接的
    // session TimeZone 计算“启动这一刻”的 UTC 偏移，Rust 后续只持有这个固定偏移。
    let offset_sql = "SELECT EXTRACT(TIMEZONE FROM now())::bigint AS offset_seconds";
    let offset_stmt = Statement::from_string(db.get_database_backend(), offset_sql);
    let offset_row = db
        .query_one(offset_stmt)
        .await?
        .ok_or_else(|| anyhow::anyhow!("postgres session timezone offset query returned no row"))?;
    let offset_seconds: i64 = offset_row.try_get_by_index(0)?;
    let offset = fixed_offset_from_seconds(offset_seconds)?;

    Ok(DbSessionTimeZone { name, offset })
}

/// 构建 keyset pagination 查询；有下界时拼 WHERE，无下界时从最小游标回补。
fn build_batch_query(
    table_ref: &str,
    cursor_column: &str,
    has_lower_bound: bool,
    cursor_plan: &CursorPlan,
) -> String {
    let cursor_expr = quote_ident(cursor_column);
    if has_lower_bound {
        let lower_bound_expr = cursor_plan.lower_bound_expr();
        // 有 checkpoint/start_from 时使用 keyset pagination：cursor > lower_bound。
        format!(
            "WITH base AS (\
                SELECT t.*, {cursor_expr} AS \"__warp_cursor_value\" \
                FROM {table_ref} t \
                WHERE {cursor_expr} > {lower_bound_expr} \
                ORDER BY {cursor_expr} ASC \
                LIMIT $2\
            ) \
            SELECT \
                \"__warp_cursor_value\"::text AS cursor_value, \
                ((to_jsonb(base) - '__warp_cursor_value') \
                    || jsonb_build_object('warp_parse_table', $3::text))::text AS payload \
            FROM base \
            ORDER BY \"__warp_cursor_value\" ASC"
        )
    } else {
        // 首次启动且没有 start_from 时不拼 WHERE > $1，从当前最小游标开始回补。
        format!(
            "WITH base AS (\
                SELECT t.*, {cursor_expr} AS \"__warp_cursor_value\" \
                FROM {table_ref} t \
                ORDER BY {cursor_expr} ASC \
                LIMIT $1\
            ) \
            SELECT \
                \"__warp_cursor_value\"::text AS cursor_value, \
                ((to_jsonb(base) - '__warp_cursor_value') \
                    || jsonb_build_object('warp_parse_table', $2::text))::text AS payload \
            FROM base \
            ORDER BY \"__warp_cursor_value\" ASC"
        )
    }
}

/// 将 PostgreSQL 原生时间列 data_type 映射成参数侧 cast 类型。
fn time_lower_bound_cast(data_type: &str) -> Option<&'static str> {
    match data_type {
        "timestamp with time zone" => Some("timestamptz"),
        "timestamp without time zone" | "timestamp" => Some("timestamp"),
        "date" => Some("date"),
        _ => None,
    }
}

/// 判断 PostgreSQL data_type 是否为支持的整数族。
fn is_integer_type(data_type: &str) -> bool {
    matches!(
        data_type,
        "smallint" | "integer" | "bigint" | "smallserial" | "serial" | "bigserial"
    )
}

/// 判断 PostgreSQL data_type 是否为支持的精确小数类型。
fn is_decimal_type(data_type: &str) -> bool {
    matches!(data_type, "numeric" | "decimal")
}

// ===== checkpoint 与通用辅助 =====

/// 校验 checkpoint 是否和当前 cursor_column/cursor_type/版本兼容。
fn validate_checkpoint_state(
    state: &CheckpointState,
    cursor_column: &str,
    cursor_plan: &CursorPlan,
) -> AnyResult<()> {
    if state.version != CHECKPOINT_VERSION {
        anyhow::bail!(
            "postgres checkpoint version mismatch: expect {}, got {}",
            CHECKPOINT_VERSION,
            state.version
        );
    }
    if state.cursor_column != cursor_column {
        anyhow::bail!(
            "postgres checkpoint cursor_column mismatch: expect {}, got {}",
            cursor_column,
            state.cursor_column
        );
    }
    if state.cursor_type != cursor_plan.cursor_type.as_str() {
        anyhow::bail!(
            "postgres checkpoint cursor_type mismatch: expect {}, got {}",
            cursor_plan.cursor_type.as_str(),
            state.cursor_type
        );
    }
    cursor_plan.validate_lower_bound(
        &state.last_cursor_raw,
        "postgres checkpoint last_cursor_raw",
    )
}

/// 读取必填 Option<String> 配置，并拒绝空字符串。
fn required_opt_field<'a>(name: &str, value: &'a Option<String>) -> AnyResult<&'a str> {
    value
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow::anyhow!("{name} must not be empty"))
}

/// 给 PostgreSQL 标识符加双引号，并转义内部双引号。
fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

/// 根据 source key 生成本地 checkpoint 文件路径。
fn checkpoint_path(source_key: &str) -> PathBuf {
    Path::new("./.run/.checkpoints").join(format!("{source_key}.json"))
}

/// 确保 checkpoint 文件所在目录存在。
fn ensure_checkpoint_dir(path: &Path) -> AnyResult<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    Ok(())
}

/// 从带 schema 的表引用中提取表名，用于写入 payload 的 warp_parse_table。
fn table_name_from_ref(table_ref: &str) -> String {
    table_ref
        .rsplit('.')
        .next()
        .unwrap_or(table_ref)
        .trim_matches('"')
        .to_string()
}

/// 解析实际查询下界；checkpoint 优先，start_from 只在无 checkpoint 时生效。
fn resolve_lower_bound<'a>(
    checkpoint: Option<&'a CheckpointState>,
    start_from: Option<&'a str>,
) -> Option<&'a str> {
    // 有 checkpoint 时必须从 checkpoint 继续，避免运行一段时间后修改 start_from 导致重复回扫。
    checkpoint
        .map(|state| state.last_cursor_raw.as_str())
        .or(start_from)
}

/// 将 PostgreSQL 返回的时区偏移秒数转换成 chrono::FixedOffset。
fn fixed_offset_from_seconds(offset_seconds: i64) -> AnyResult<FixedOffset> {
    let offset_seconds = i32::try_from(offset_seconds)
        .map_err(|err| anyhow::anyhow!("postgres session timezone offset out of range: {err}"))?;
    FixedOffset::east_opt(offset_seconds).ok_or_else(|| {
        anyhow::anyhow!("postgres session timezone offset seconds is invalid: {offset_seconds}")
    })
}

// ===== 时间解析与 start_from 归一化 =====

/// 将用户配置的 start_from_format 归类为 Unix 秒、Unix 毫秒或 chrono pattern。
fn parse_start_from_format(
    raw: Option<&str>,
    cursor_type: CursorType,
) -> AnyResult<Option<StartFromFormat>> {
    let Some(raw) = raw.map(str::trim).filter(|s| !s.is_empty()) else {
        return Ok(None);
    };
    if cursor_type != CursorType::Time {
        anyhow::bail!("postgres.start_from_format is only supported for time cursor");
    }
    // 这里只决定“输入格式类别”。最终下界文本仍由 date/timestamp/timestamptz 分支决定。
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

/// 可选地归一化 start_from；未配置时直接返回 None。
fn normalize_optional_start_from(
    cursor_plan: &CursorPlan,
    raw: Option<&str>,
    format: Option<&StartFromFormat>,
    session_offset: FixedOffset,
) -> AnyResult<Option<String>> {
    raw.map(|raw| cursor_plan.normalize_start_from(raw, format, session_offset))
        .transpose()
}

/// 按数据库真实时间列类型归一化 start_from。
fn normalize_time_start_from(
    raw: &str,
    format: Option<&StartFromFormat>,
    lower_bound_cast: &str,
    session_offset: FixedOffset,
) -> AnyResult<String> {
    // lower_bound_cast 来自数据库真实列类型，不来自用户配置。
    // 这保证 timestamp/date/timestamptz 按各自数据库语义生成下界。
    match lower_bound_cast {
        "date" => normalize_date_start_from(raw, format, session_offset),
        "timestamp" => normalize_timestamp_start_from(raw, format, session_offset),
        "timestamptz" => normalize_timestamptz_start_from(raw, format, session_offset),
        other => anyhow::bail!("unsupported postgres time lower bound cast: {other}"),
    }
}

/// 将 start_from 归一化为 date 可比较的 `YYYY-MM-DD` 文本。
fn normalize_date_start_from(
    raw: &str,
    format: Option<&StartFromFormat>,
    session_offset: FixedOffset,
) -> AnyResult<String> {
    let date = if let Some(format) = format {
        parse_date_by_format(raw, format, session_offset)?
    } else {
        parse_date_fallback(raw, session_offset)?
    };
    // PostgreSQL date 列没有时分秒，即使用户输入秒级时间，最终也只能比较到日期。
    Ok(date.format("%Y-%m-%d").to_string())
}

/// 将 start_from 归一化为 timestamp 可比较的无时区文本。
fn normalize_timestamp_start_from(
    raw: &str,
    format: Option<&StartFromFormat>,
    session_offset: FixedOffset,
) -> AnyResult<String> {
    let datetime = if let Some(format) = format {
        parse_timestamp_by_format(raw, format, session_offset)?
    } else {
        parse_timestamp_fallback(raw, session_offset)?
    };
    // timestamp without time zone 不保存时区，最终输出无时区时间。
    // 如果输入带时区，取用户输入时区下的“墙上时间”；如果输入不带时区或是 Unix 时间戳，
    // 则按 PostgreSQL session TimeZone 对应的启动时 offset 解释。
    Ok(datetime.format("%Y-%m-%d %H:%M:%S%.6f").to_string())
}

/// 将 start_from 归一化为 timestamptz 可比较的 RFC3339 文本。
fn normalize_timestamptz_start_from(
    raw: &str,
    format: Option<&StartFromFormat>,
    session_offset: FixedOffset,
) -> AnyResult<String> {
    let datetime = if let Some(format) = format {
        parse_timestamptz_by_format(raw, format, session_offset)?
    } else {
        parse_timestamptz_fallback(raw, session_offset)?
    };
    // timestamptz 需要保留明确时区，避免交给 PostgreSQL 时受会话时区影响。
    Ok(datetime.to_rfc3339_opts(SecondsFormat::Micros, true))
}

/// 在用户显式提供 start_from_format 时，按该格式解析 date 语义输入。
fn parse_date_by_format(
    raw: &str,
    format: &StartFromFormat,
    session_offset: FixedOffset,
) -> AnyResult<NaiveDate> {
    match format.kind {
        StartFromFormatKind::UnixSeconds => Ok(parse_unix_seconds(raw)?
            .with_timezone(&session_offset)
            .date_naive()),
        StartFromFormatKind::UnixMillis => Ok(parse_unix_millis(raw)?
            .with_timezone(&session_offset)
            .date_naive()),
        StartFromFormatKind::Pattern => {
            if pattern_has_offset(&format.raw) {
                Ok(DateTime::parse_from_str(raw, &format.raw)?.date_naive())
            } else if pattern_has_time(&format.raw) {
                Ok(NaiveDateTime::parse_from_str(raw, &format.raw)?.date())
            } else {
                // 纯日期 pattern 必须用 NaiveDate 解析，不能用 NaiveDateTime。
                Ok(NaiveDate::parse_from_str(raw, &format.raw)?)
            }
        }
    }
}

/// 在用户显式提供 start_from_format 时，按该格式解析 timestamp 语义输入。
fn parse_timestamp_by_format(
    raw: &str,
    format: &StartFromFormat,
    session_offset: FixedOffset,
) -> AnyResult<NaiveDateTime> {
    match format.kind {
        StartFromFormatKind::UnixSeconds => Ok(parse_unix_seconds(raw)?
            .with_timezone(&session_offset)
            .naive_local()),
        StartFromFormatKind::UnixMillis => Ok(parse_unix_millis(raw)?
            .with_timezone(&session_offset)
            .naive_local()),
        StartFromFormatKind::Pattern => {
            if pattern_has_offset(&format.raw) {
                // 用户给了时区但列是 timestamp，则只去掉时区，保留用户给定时区下的墙上时间。
                Ok(DateTime::parse_from_str(raw, &format.raw)?.naive_local())
            } else if pattern_has_time(&format.raw) {
                Ok(NaiveDateTime::parse_from_str(raw, &format.raw)?)
            } else {
                // timestamp 列允许用户只给日期，此时把时间补成当天 00:00:00。
                Ok(NaiveDate::parse_from_str(raw, &format.raw)?
                    .and_hms_opt(0, 0, 0)
                    .ok_or_else(|| anyhow::anyhow!("postgres.start_from parse failed"))?)
            }
        }
    }
}

/// 在用户显式提供 start_from_format 时，按该格式解析 timestamptz 语义输入。
fn parse_timestamptz_by_format(
    raw: &str,
    format: &StartFromFormat,
    session_offset: FixedOffset,
) -> AnyResult<DateTime<FixedOffset>> {
    match format.kind {
        StartFromFormatKind::UnixSeconds => {
            Ok(parse_unix_seconds(raw)?.with_timezone(&session_offset))
        }
        StartFromFormatKind::UnixMillis => {
            Ok(parse_unix_millis(raw)?.with_timezone(&session_offset))
        }
        StartFromFormatKind::Pattern => {
            if pattern_has_offset(&format.raw) {
                Ok(DateTime::parse_from_str(raw, &format.raw)?)
            } else if pattern_has_time(&format.raw) {
                let naive = NaiveDateTime::parse_from_str(raw, &format.raw)?;
                // 用户输入没有时区但目标是 timestamptz，按 PostgreSQL session TimeZone 绑定。
                bind_naive_to_fixed_offset(naive, session_offset, "postgres.start_from")
            } else {
                let naive = NaiveDate::parse_from_str(raw, &format.raw)?
                    .and_hms_opt(0, 0, 0)
                    .ok_or_else(|| anyhow::anyhow!("postgres.start_from parse failed"))?;
                bind_naive_to_fixed_offset(naive, session_offset, "postgres.start_from")
            }
        }
    }
}

/// date 游标的兜底解析：允许日期、日期时间、带时区时间和 Unix 时间，最终只保留日期。
fn parse_date_fallback(raw: &str, session_offset: FixedOffset) -> AnyResult<NaiveDate> {
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

/// timestamp 游标的兜底解析：允许日期、日期时间、带时区时间和 Unix 时间。
fn parse_timestamp_fallback(raw: &str, session_offset: FixedOffset) -> AnyResult<NaiveDateTime> {
    if let Ok(dt) = parse_naive_datetime_fallback(raw) {
        return Ok(dt);
    }
    if let Ok(dt) = DateTime::parse_from_rfc3339(raw) {
        return Ok(dt.naive_local());
    }
    if let Ok(date) = NaiveDate::parse_from_str(raw, "%Y-%m-%d") {
        return date
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| anyhow::anyhow!("postgres.start_from parse failed"));
    }
    Ok(parse_unix_auto(raw)?
        .with_timezone(&session_offset)
        .naive_local())
}

/// timestamptz 游标的兜底解析：无时区输入按 session TimeZone 绑定。
fn parse_timestamptz_fallback(
    raw: &str,
    session_offset: FixedOffset,
) -> AnyResult<DateTime<FixedOffset>> {
    if let Ok(dt) = DateTime::parse_from_rfc3339(raw) {
        return Ok(dt);
    }
    if let Ok(dt) = parse_naive_datetime_fallback(raw) {
        return bind_naive_to_fixed_offset(dt, session_offset, "postgres.start_from");
    }
    if let Ok(date) = NaiveDate::parse_from_str(raw, "%Y-%m-%d") {
        let naive = date
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| anyhow::anyhow!("postgres.start_from parse failed"))?;
        return bind_naive_to_fixed_offset(naive, session_offset, "postgres.start_from");
    }
    Ok(parse_unix_auto(raw)?.with_timezone(&session_offset))
}

/// 未显式声明格式时，按 10 位秒 / 13 位毫秒自动识别 Unix 时间戳。
fn parse_unix_auto(raw: &str) -> AnyResult<DateTime<Utc>> {
    // 未显式指定 start_from_format 时，按位数兜底判断 Unix 秒/毫秒。
    match raw.len() {
        13 => parse_unix_millis(raw),
        10 => parse_unix_seconds(raw),
        _ => anyhow::bail!("postgres.start_from parse failed"),
    }
}

/// 将 Unix 秒解析为 UTC 时间点。
fn parse_unix_seconds(raw: &str) -> AnyResult<DateTime<Utc>> {
    let secs = raw
        .parse::<i64>()
        .map_err(|err| anyhow::anyhow!("postgres.start_from parse unix seconds failed: {err}"))?;
    DateTime::<Utc>::from_timestamp(secs, 0)
        .ok_or_else(|| anyhow::anyhow!("postgres.start_from unix seconds out of range"))
}

/// 将 Unix 毫秒解析为 UTC 时间点。
fn parse_unix_millis(raw: &str) -> AnyResult<DateTime<Utc>> {
    let millis = raw.parse::<i64>().map_err(|err| {
        anyhow::anyhow!("postgres.start_from parse unix milliseconds failed: {err}")
    })?;
    DateTime::<Utc>::from_timestamp_millis(millis)
        .ok_or_else(|| anyhow::anyhow!("postgres.start_from unix milliseconds out of range"))
}

/// 兜底解析常见无时区日期时间文本。
fn parse_naive_datetime_fallback(raw: &str) -> Result<NaiveDateTime, chrono::ParseError> {
    NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(raw, "%Y-%m-%dT%H:%M:%S%.f"))
}

/// 将无时区时间按指定固定偏移绑定成带时区时间。
fn bind_naive_to_fixed_offset(
    naive: NaiveDateTime,
    offset: FixedOffset,
    field_name: &str,
) -> AnyResult<DateTime<FixedOffset>> {
    // FixedOffset 没有夏令时跳变歧义；这里仍保留 single() 校验，防止后续替换时放宽语义。
    offset
        .from_local_datetime(&naive)
        .single()
        .ok_or_else(|| anyhow::anyhow!("{field_name} is ambiguous or invalid in session timezone"))
}

/// 判断 chrono pattern 是否显式包含时区信息。
fn pattern_has_offset(pattern: &str) -> bool {
    pattern.contains("%z") || pattern.contains("%:z")
}

/// 判断 chrono pattern 是否包含时分秒等时间部分。
fn pattern_has_time(pattern: &str) -> bool {
    pattern.contains("%H")
        || pattern.contains("%M")
        || pattern.contains("%S")
        || pattern.contains("%T")
        || pattern.contains("%R")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    /// 便捷构造测试用的游标计划，减少样板代码。
    fn test_cursor_plan(
        cursor_type: CursorType,
        lower_bound_binding: LowerBoundBinding,
    ) -> CursorPlan {
        CursorPlan {
            cursor_type,
            lower_bound_binding,
        }
    }

    /// 为检查点测试创建唯一的临时文件路径，避免并发冲突。
    fn temp_checkpoint_path(name: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time before epoch")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "wp-connectors-postgres-source-{name}-{}-{unique}.json",
            std::process::id()
        ))
    }

    /// 构造整数游标场景使用的游标计划。
    fn integer_cursor_plan() -> CursorPlan {
        test_cursor_plan(CursorType::Int, LowerBoundBinding::Integer)
    }

    /// 构造 decimal / numeric 数值游标场景使用的游标计划。
    fn numeric_cursor_plan() -> CursorPlan {
        test_cursor_plan(
            CursorType::Int,
            LowerBoundBinding::TextParamWithCast("numeric"),
        )
    }

    /// 构造 `timestamptz` 时间游标场景使用的游标计划。
    fn timestamptz_cursor_plan() -> CursorPlan {
        test_cursor_plan(
            CursorType::Time,
            LowerBoundBinding::TextParamWithCast("timestamptz"),
        )
    }

    /// 验证标识符引用时会正确转义双引号。
    #[test]
    fn quote_ident_escapes_quotes() {
        assert_eq!(quote_ident("a\"b"), "\"a\"\"b\"");
    }

    /// 验证表引用拆解后能拿到末尾表名。
    #[test]
    fn table_name_from_ref_extracts_tail() {
        assert_eq!(table_name_from_ref("\"public\".\"events\""), "events");
    }

    /// 验证时间游标的下界参数会按原始字符串透传。
    #[test]
    fn lower_bound_time_stays_string() {
        let value = test_cursor_plan(
            CursorType::Time,
            LowerBoundBinding::TextParamWithCast("timestamptz"),
        )
        .lower_bound_into_value("2025-04-15T12:30:45+08:00")
        .unwrap();
        match value {
            Value::String(Some(raw)) => assert_eq!(&*raw, "2025-04-15T12:30:45+08:00"),
            other => panic!("unexpected value: {other:?}"),
        }
    }

    /// 验证整数游标的下界参数会解析成数值绑定。
    #[test]
    fn lower_bound_int_parses_integer() {
        let value = test_cursor_plan(CursorType::Int, LowerBoundBinding::Integer)
            .lower_bound_into_value("42")
            .unwrap();
        match value {
            Value::BigInt(Some(raw)) => assert_eq!(raw, 42),
            other => panic!("unexpected value: {other:?}"),
        }
    }

    /// 验证高精度 decimal 下界会保留原始文本，避免精度损失。
    #[test]
    fn lower_bound_decimal_stays_string() {
        let value = test_cursor_plan(
            CursorType::Int,
            LowerBoundBinding::TextParamWithCast("numeric"),
        )
        .lower_bound_into_value("12345678901234567890.123400")
        .unwrap();
        match value {
            Value::String(Some(raw)) => assert_eq!(&*raw, "12345678901234567890.123400"),
            other => panic!("unexpected value: {other:?}"),
        }
    }

    /// 验证已有 checkpoint 时，会优先使用 checkpoint 里的游标。
    #[test]
    fn resolve_lower_bound_prefers_checkpoint_over_start_from() {
        let checkpoint = CheckpointState {
            version: CHECKPOINT_VERSION,
            cursor_type: "time".into(),
            cursor_column: "log_time".into(),
            last_cursor_raw: "2025-04-16 08:00:00".into(),
            updated_at: "2026-04-16T00:00:00Z".into(),
        };
        let lower_bound = resolve_lower_bound(Some(&checkpoint), Some("2025-01-01 00:00:00"));
        assert_eq!(lower_bound, Some("2025-04-16 08:00:00"));
    }

    /// 验证首次启动且没有 checkpoint 时，会回退到 `start_from`。
    #[test]
    fn resolve_lower_bound_uses_start_from_without_checkpoint() {
        let lower_bound = resolve_lower_bound(None, Some("2025-01-01 00:00:00"));
        assert_eq!(lower_bound, Some("2025-01-01 00:00:00"));
    }

    /// 验证没有下界时查询语句不会拼接 `WHERE > ...` 条件。
    #[test]
    fn build_batch_query_omits_where_without_lower_bound() {
        let cursor_plan = test_cursor_plan(CursorType::Int, LowerBoundBinding::Integer);
        let sql = build_batch_query("\"public\".\"events\"", "id", false, &cursor_plan);
        assert!(sql.contains("ORDER BY \"id\" ASC"));
        assert!(sql.contains("LIMIT $1"));
        assert!(!sql.contains("WHERE \"id\" > $1"));
    }

    /// 验证有下界时查询语句会拼接增量过滤条件与正确的参数序号。
    #[test]
    fn build_batch_query_includes_where_with_lower_bound() {
        let cursor_plan = test_cursor_plan(CursorType::Int, LowerBoundBinding::Integer);
        let sql = build_batch_query("\"public\".\"events\"", "id", true, &cursor_plan);
        assert!(sql.contains("WHERE \"id\" > $1"));
        assert!(!sql.contains("WHERE \"id\" > $1::"));
        assert!(sql.contains("LIMIT $2"));
        assert!(sql.contains("warp_parse_table', $3::text"));
    }

    /// 验证 decimal / numeric 游标会在 SQL 中追加 `numeric` 转换。
    #[test]
    fn build_batch_query_uses_numeric_cast_for_decimal_cursor() {
        let cursor_plan = numeric_cursor_plan();
        let sql = build_batch_query("\"public\".\"events\"", "amount_id", true, &cursor_plan);
        assert!(sql.contains("WHERE \"amount_id\" > $1::numeric"));
    }

    /// 验证时间游标会在 SQL 中使用对应的原生 PostgreSQL 时间类型转换。
    #[test]
    fn build_batch_query_uses_native_time_cast() {
        let cursor_plan = timestamptz_cursor_plan();
        let sql = build_batch_query("\"public\".\"events\"", "create_time", true, &cursor_plan);
        assert!(sql.contains("WHERE \"create_time\" > $1::timestamptz"));
    }

    /// 验证整数游标配置允许绑定到 decimal / numeric 列类型。
    #[test]
    fn int_cursor_accepts_decimal_column_type() {
        assert_eq!(
            CursorType::Int
                .lower_bound_binding("amount_id", "numeric")
                .unwrap(),
            LowerBoundBinding::TextParamWithCast("numeric")
        );
        assert_eq!(
            CursorType::Int
                .lower_bound_binding("amount_id", "decimal")
                .unwrap(),
            LowerBoundBinding::TextParamWithCast("numeric")
        );
    }

    /// 验证 PostgreSQL 的 `timestamp with time zone` 会映射到 `timestamptz` 转换。
    #[test]
    fn time_lower_bound_cast_supports_timestamp_with_time_zone() {
        assert_eq!(
            time_lower_bound_cast("timestamp with time zone"),
            Some("timestamptz")
        );
    }

    /// 验证 `timestamp without time zone` 也能作为 `time` 游标。
    #[test]
    fn time_cursor_accepts_timestamp_without_time_zone() {
        assert_eq!(
            CursorType::Time
                .lower_bound_binding("create_time", "timestamp without time zone")
                .unwrap(),
            LowerBoundBinding::TextParamWithCast("timestamp")
        );
    }

    /// 验证纯 `time` 列不会被当成可用的增量时间游标。
    #[test]
    fn time_lower_bound_cast_rejects_plain_time_type() {
        assert_eq!(time_lower_bound_cast("time without time zone"), None);
    }

    /// 验证不支持的游标类型会在配置校验阶段被拒绝。
    #[test]
    fn validate_source_rejects_unsupported_cursor_type() {
        let err = validate_source_cursor_type_and_start_from("text", None, None)
            .expect_err("unsupported cursor_type should fail");
        assert!(err.to_string().contains("unsupported postgres cursor_type"));
    }

    /// 验证单独配置 `start_from_format` 会被拒绝。
    #[test]
    fn validate_source_requires_start_from_when_format_is_present() {
        let err = validate_source_cursor_type_and_start_from("time", None, Some("unix_s"))
            .expect_err("missing start_from should fail");
        assert!(
            err.to_string()
                .contains("postgres.start_from_format requires postgres.start_from")
        );
    }

    /// 验证 `int` 游标不允许配置 `start_from_format`。
    #[test]
    fn validate_source_rejects_start_from_format_for_int_cursor() {
        let err = validate_source_cursor_type_and_start_from("int", Some("100"), Some("unix_s"))
            .expect_err("int cursor with start_from_format should fail");
        assert!(
            err.to_string()
                .contains("postgres.start_from_format is only supported for time cursor")
        );
    }

    /// 验证空白 `start_from` 会在配置校验阶段被拒绝。
    #[test]
    fn validate_source_rejects_empty_start_from() {
        let err = validate_source_cursor_type_and_start_from("time", Some("   "), None)
            .expect_err("empty start_from should fail");
        assert!(
            err.to_string()
                .contains("postgres.start_from must not be empty")
        );
    }

    /// 验证整数游标不能绑定到非数值列类型。
    #[test]
    fn int_cursor_rejects_non_numeric_column_type() {
        let err = CursorType::Int
            .lower_bound_binding("event_id", "uuid")
            .expect_err("uuid should not be accepted by int cursor");
        assert!(err.to_string().contains("must be numeric-like"));
    }

    /// 验证时间游标不能绑定到非时间列类型。
    #[test]
    fn time_cursor_rejects_non_temporal_column_type() {
        let err = CursorType::Time
            .lower_bound_binding("create_time", "text")
            .expect_err("text should not be accepted by time cursor");
        assert!(err.to_string().contains("must be timestamp/date-like"));
    }

    /// 验证 `start_from_format` 能识别 Unix 别名和自定义 pattern。
    #[test]
    fn parse_start_from_format_accepts_unix_alias_and_pattern() {
        assert_eq!(
            parse_start_from_format(Some("unix"), CursorType::Time).unwrap(),
            Some(StartFromFormat {
                raw: "unix".into(),
                kind: StartFromFormatKind::UnixSeconds,
            })
        );
        assert_eq!(
            parse_start_from_format(Some("%Y-%m-%d"), CursorType::Time).unwrap(),
            Some(StartFromFormat {
                raw: "%Y-%m-%d".into(),
                kind: StartFromFormatKind::Pattern,
            })
        );
    }

    /// 验证 `timestamptz` 在接收 Unix 秒时，会按 session 时区展开成下界字符串。
    #[test]
    fn normalize_timestamptz_start_from_uses_session_timezone_for_unix_seconds() {
        let offset = FixedOffset::east_opt(8 * 3600).unwrap();
        let normalized = normalize_timestamptz_start_from(
            "1776328285",
            Some(&StartFromFormat {
                raw: "unix_s".into(),
                kind: StartFromFormatKind::UnixSeconds,
            }),
            offset,
        )
        .unwrap();
        assert_eq!(normalized, "2026-04-16T16:31:25.000000+08:00");
    }

    /// 验证 `timestamp` 在接收 Unix 秒时，会按 session 时区还原本地时间。
    #[test]
    fn normalize_timestamp_start_from_uses_session_timezone_for_unix_seconds() {
        let offset = FixedOffset::east_opt(8 * 3600).unwrap();
        let normalized = normalize_timestamp_start_from(
            "1776328285",
            Some(&StartFromFormat {
                raw: "unix_s".into(),
                kind: StartFromFormatKind::UnixSeconds,
            }),
            offset,
        )
        .unwrap();
        assert_eq!(normalized, "2026-04-16 16:31:25.000000");
    }

    /// 验证 `timestamp` 支持按纯日期 pattern 解析，并补齐到当天零点。
    #[test]
    fn normalize_timestamp_start_from_accepts_date_only_pattern() {
        let offset = FixedOffset::east_opt(8 * 3600).unwrap();
        let normalized = normalize_timestamp_start_from(
            "2026-04-16",
            Some(&StartFromFormat {
                raw: "%Y-%m-%d".into(),
                kind: StartFromFormatKind::Pattern,
            }),
            offset,
        )
        .unwrap();
        assert_eq!(normalized, "2026-04-16 00:00:00.000000");
    }

    /// 验证带时区输入写入 `timestamptz` 时会保留用户原始时区语义。
    #[test]
    fn normalize_timestamptz_start_from_keeps_input_timezone() {
        let session_offset = FixedOffset::east_opt(0).unwrap();
        let normalized =
            normalize_timestamptz_start_from("2026-04-16T16:31:25+08:00", None, session_offset)
                .unwrap();
        assert_eq!(normalized, "2026-04-16T16:31:25.000000+08:00");
    }

    /// 验证无时区时间按显式 pattern 解析到 `timestamptz` 时，会绑定当前 session 时区。
    #[test]
    fn normalize_timestamptz_start_from_binds_session_timezone_for_naive_pattern() {
        let offset = FixedOffset::east_opt(8 * 3600).unwrap();
        let normalized = normalize_timestamptz_start_from(
            "2026-04-16 16:31:25",
            Some(&StartFromFormat {
                raw: "%Y-%m-%d %H:%M:%S".into(),
                kind: StartFromFormatKind::Pattern,
            }),
            offset,
        )
        .unwrap();
        assert_eq!(normalized, "2026-04-16T16:31:25.000000+08:00");
    }

    /// 验证带时区输入写入 `timestamp` 时，只保留用户输入的墙上时间部分。
    #[test]
    fn normalize_timestamp_start_from_keeps_input_wall_time_when_timezone_is_present() {
        let session_offset = FixedOffset::east_opt(0).unwrap();
        let normalized =
            normalize_timestamp_start_from("2026-04-16T16:31:25+08:00", None, session_offset)
                .unwrap();
        assert_eq!(normalized, "2026-04-16 16:31:25.000000");
    }

    /// 验证带时区输入写入 `date` 时，会直接取用户输入中的日期部分。
    #[test]
    fn normalize_date_start_from_keeps_input_date_when_timezone_is_present() {
        let session_offset = FixedOffset::west_opt(10 * 3600).unwrap();
        let normalized =
            normalize_date_start_from("2026-04-16T01:30:00+08:00", None, session_offset).unwrap();
        assert_eq!(normalized, "2026-04-16");
    }

    /// 验证 `date` 类型支持按纯日期格式显式解析 `start_from`。
    #[test]
    fn normalize_date_start_from_accepts_plain_date_pattern() {
        let offset = FixedOffset::east_opt(8 * 3600).unwrap();
        let normalized = normalize_date_start_from(
            "2026-04-16",
            Some(&StartFromFormat {
                raw: "%Y-%m-%d".into(),
                kind: StartFromFormatKind::Pattern,
            }),
            offset,
        )
        .unwrap();
        assert_eq!(normalized, "2026-04-16");
    }

    /// 验证 `date` 类型也支持先按日期时间格式解析，再截取日期部分。
    #[test]
    fn normalize_date_start_from_accepts_datetime_pattern() {
        let offset = FixedOffset::east_opt(8 * 3600).unwrap();
        let normalized = normalize_date_start_from(
            "2026-04-16 16:31:25",
            Some(&StartFromFormat {
                raw: "%Y-%m-%d %H:%M:%S".into(),
                kind: StartFromFormatKind::Pattern,
            }),
            offset,
        )
        .unwrap();
        assert_eq!(normalized, "2026-04-16");
    }

    /// 验证 `date` 类型支持将 Unix 秒转换成 session 时区下的日期。
    #[test]
    fn normalize_date_start_from_accepts_unix_seconds() {
        let offset = FixedOffset::east_opt(8 * 3600).unwrap();
        let normalized = normalize_date_start_from(
            "1776328285",
            Some(&StartFromFormat {
                raw: "unix_s".into(),
                kind: StartFromFormatKind::UnixSeconds,
            }),
            offset,
        )
        .unwrap();
        assert_eq!(normalized, "2026-04-16");
    }

    /// 验证未声明格式时，`date` 类型仍能兜底解析常见纯日期字符串。
    #[test]
    fn normalize_date_start_from_fallback_accepts_plain_date() {
        let offset = FixedOffset::east_opt(8 * 3600).unwrap();
        let normalized = normalize_date_start_from("2026-04-16", None, offset).unwrap();
        assert_eq!(normalized, "2026-04-16");
    }

    /// 验证 PostgreSQL 返回的秒级时区偏移能正确转成 `FixedOffset`。
    #[test]
    fn fixed_offset_from_seconds_accepts_postgres_offset() {
        let offset = fixed_offset_from_seconds(8 * 3600).unwrap();
        assert_eq!(offset.to_string(), "+08:00");
    }

    /// 验证整数游标检查点中的非法值会在加载阶段被拒绝。
    #[test]
    fn load_checkpoint_rejects_invalid_int_cursor_value() {
        let path = temp_checkpoint_path("invalid-int");
        let content = r#"{
  "version": 1,
  "cursor_type": "int",
  "cursor_column": "id",
  "last_cursor_raw": "not-an-int",
  "updated_at": "2026-04-16T00:00:00Z"
}"#;
        std::fs::write(&path, content).unwrap();

        let cursor_plan = integer_cursor_plan();
        let err = PostgresSource::load_checkpoint(&path, "id", &cursor_plan)
            .expect_err("invalid int checkpoint should fail");
        assert!(err.to_string().contains("must be an integer"));

        let _ = std::fs::remove_file(&path);
    }

    /// 验证空 checkpoint 文件会被视为“当前没有 checkpoint”。
    #[test]
    fn load_checkpoint_returns_none_for_empty_file() {
        let path = temp_checkpoint_path("empty");
        std::fs::write(&path, "").unwrap();

        let cursor_plan = integer_cursor_plan();
        let checkpoint = PostgresSource::load_checkpoint(&path, "id", &cursor_plan)
            .expect("empty checkpoint should load as none");
        assert_eq!(checkpoint, None);

        let _ = std::fs::remove_file(&path);
    }

    /// 验证检查点中的游标列与当前配置不一致时，会返回带路径与修复提示的错误。
    #[test]
    fn load_checkpoint_reports_incompatible_cursor_column_with_path_and_hint() {
        let path = temp_checkpoint_path("mismatch-column");
        let content = r#"{
  "version": 1,
  "cursor_type": "int",
  "cursor_column": "id",
  "last_cursor_raw": "42",
  "updated_at": "2026-04-16T00:00:00Z"
}"#;
        std::fs::write(&path, content).unwrap();

        let cursor_plan = integer_cursor_plan();
        let err = PostgresSource::load_checkpoint(&path, "wp_event_id", &cursor_plan)
            .expect_err("mismatch cursor_column should fail");
        let err_text = err.to_string();
        assert!(err_text.contains("is incompatible with current config"));
        assert!(err_text.contains("cursor_column mismatch"));
        assert!(err_text.contains(&path.display().to_string()));
        assert!(err_text.contains("delete this checkpoint and restart"));

        let _ = std::fs::remove_file(&path);
    }

    /// 验证 checkpoint 版本不匹配时会被拒绝。
    #[test]
    fn load_checkpoint_rejects_version_mismatch() {
        let path = temp_checkpoint_path("version-mismatch");
        let content = r#"{
  "version": 999,
  "cursor_type": "int",
  "cursor_column": "id",
  "last_cursor_raw": "42",
  "updated_at": "2026-04-16T00:00:00Z"
}"#;
        std::fs::write(&path, content).unwrap();

        let cursor_plan = integer_cursor_plan();
        let err = PostgresSource::load_checkpoint(&path, "id", &cursor_plan)
            .expect_err("mismatched checkpoint version should fail");
        assert!(err.to_string().contains("version mismatch"));

        let _ = std::fs::remove_file(&path);
    }

    /// 验证 decimal / numeric 游标的原始字符串检查点可以被直接接受。
    #[test]
    fn load_checkpoint_accepts_decimal_raw_value() {
        let path = temp_checkpoint_path("valid-decimal");
        let content = r#"{
  "version": 1,
  "cursor_type": "int",
  "cursor_column": "amount_id",
  "last_cursor_raw": "12345678901234567890.123400",
  "updated_at": "2026-04-16T00:00:00Z"
}"#;
        std::fs::write(&path, content).unwrap();

        let cursor_plan = numeric_cursor_plan();
        let checkpoint = PostgresSource::load_checkpoint(&path, "amount_id", &cursor_plan)
            .expect("valid decimal checkpoint should load")
            .expect("checkpoint should exist");
        assert_eq!(checkpoint.last_cursor_raw, "12345678901234567890.123400");

        let _ = std::fs::remove_file(&path);
    }

    /// 验证合法的时间游标检查点可以被正常恢复。
    #[test]
    fn load_checkpoint_accepts_valid_time_checkpoint() {
        let path = temp_checkpoint_path("valid-time");
        let content = r#"{
  "version": 1,
  "cursor_type": "time",
  "cursor_column": "log_time",
  "last_cursor_raw": "2025-04-16T08:00:00+08:00",
  "updated_at": "2026-04-16T00:00:00Z"
}"#;
        std::fs::write(&path, content).unwrap();

        let cursor_plan = timestamptz_cursor_plan();
        let checkpoint = PostgresSource::load_checkpoint(&path, "log_time", &cursor_plan)
            .expect("valid checkpoint should load")
            .expect("checkpoint should exist");
        assert_eq!(checkpoint.last_cursor_raw, "2025-04-16T08:00:00+08:00");

        let _ = std::fs::remove_file(&path);
    }
}
