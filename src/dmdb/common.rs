use super::config::DmdbConnConf;
use super::source::{DmdbReason, DmdbResult, dmdb_err};
use odbc_api::{Connection, ConnectionOptions, environment};
use std::sync::{Arc, Mutex};
use tokio::task;

/// 达梦模块内可克隆的连接句柄。
/// 这里的“可克隆”仅表示句柄可在同一组件内部跨任务传递，并不表示 source 与 sink
/// 需要或会复用同一条数据库连接。
/// `odbc_api::Connection` 不是异步对象，因此统一放入 `Arc<Mutex<_>>`
/// 并在阻塞线程中访问，避免直接阻塞 Tokio worker 线程。
pub(crate) type DmdbConnectionHandle = Arc<Mutex<Connection<'static>>>;

/// 在异步上下文中建立达梦连接句柄。
pub(crate) async fn connect_shared(config: &DmdbConnConf) -> DmdbResult<DmdbConnectionHandle> {
    let config = config.clone();
    task::spawn_blocking(move || connect_shared_blocking(&config))
        .await
        .map_err(|err| {
            dmdb_err(
                DmdbReason::Database,
                format!("spawn dmdb connect task failed: {err}"),
            )
        })?
}

/// 在阻塞上下文中建立达梦连接句柄。
pub(crate) fn connect_shared_blocking(config: &DmdbConnConf) -> DmdbResult<DmdbConnectionHandle> {
    // ODBC Environment 需要和连接生命周期保持一致，这里使用全局 environment。
    let env = environment().map_err(|err| {
        dmdb_err(
            DmdbReason::Database,
            format!("acquire odbc environment fail: {err}"),
        )
    })?;
    let options = config.connect_options();
    let connection = open_connection(env, config, options)?;
    Ok(Arc::new(Mutex::new(connection)))
}

/// 按连接模式打开达梦 ODBC 连接。
pub(crate) fn open_connection(
    env: &'static odbc_api::Environment,
    config: &DmdbConnConf,
    options: ConnectionOptions,
) -> DmdbResult<Connection<'static>> {
    // 连接优先级与工厂侧校验保持一致：完整连接串 > endpoint 拼装连接串 > DSN。
    if let Some(connection_string) = config.connection_string.as_deref().map(str::trim)
        && !connection_string.is_empty()
    {
        return env
            .connect_with_connection_string(connection_string, options)
            .map_err(|err| {
                dmdb_err(
                    DmdbReason::Database,
                    format!("connect dmdb with connection_string fail: {err}"),
                )
            });
    }

    if !config.endpoint.trim().is_empty() {
        let connection_string = config.generated_connection_string()?;
        return env
            .connect_with_connection_string(connection_string.as_str(), options)
            .map_err(|err| {
                dmdb_err(
                    DmdbReason::Database,
                    format!("connect dmdb with generated connection string fail: {err}"),
                )
            });
    }

    if let Some(dsn) = config.dsn.as_deref().map(str::trim)
        && !dsn.is_empty()
    {
        return env
            .connect(
                dsn,
                config.username.trim(),
                config.password.as_str(),
                options,
            )
            .map_err(|err| {
                dmdb_err(
                    DmdbReason::Database,
                    format!("connect dmdb with dsn fail: {err}"),
                )
            });
    }

    Err(dmdb_err(
        DmdbReason::Config,
        "dmdb.connection_string, dmdb.endpoint or dmdb.dsn must provide at least one",
    ))
}

/// 转义 SQL 标识符，避免字段名或表名中的双引号破坏 SQL。
pub(crate) fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

/// 转义 SQL 字面量中的单引号。
pub(crate) fn escape_sql_literal(value: &str) -> String {
    value.replace('\'', "''")
}

#[cfg(test)]
mod tests {
    use super::{escape_sql_literal, quote_identifier};

    #[test]
    fn quote_identifier_escapes_double_quote() {
        assert_eq!(quote_identifier("A\"B"), "\"A\"\"B\"");
    }

    #[test]
    fn escape_sql_literal_escapes_single_quote() {
        assert_eq!(escape_sql_literal("O'Reilly"), "O''Reilly");
    }
}
