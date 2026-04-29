use educe::Educe;
use odbc_api::{ConnectionOptions, escape_attribute_value};
use serde::{Deserialize, Serialize};

/// 达梦连接公共配置。
#[derive(Educe, Deserialize, Serialize, PartialEq, Clone)]
#[educe(Debug)]
pub struct DmdbConnConf {
    /// ODBC 数据源名称。仅在未提供 `connection_string` 和 `endpoint` 时使用。
    pub dsn: Option<String>,
    /// 完整 ODBC 连接串。若提供，优先级最高。
    pub connection_string: Option<String>,
    /// 形如 `host:port` 的地址。未提供 `connection_string` 时优先于 `dsn` 使用。
    pub endpoint: String,
    /// 达梦 ODBC 驱动名称。走 `endpoint` 模式时必须显式配置。
    pub driver: String,
    /// 数据库用户名。走 `dsn` 或 `endpoint` 模式时必须显式配置。
    pub username: String,
    /// 数据库密码。走 `dsn` 或 `endpoint` 模式时必须显式配置。
    pub password: String,
    /// 目标 schema，可选。
    pub schema: Option<String>,
    /// 建连超时秒数。
    pub connect_timeout_secs: Option<u64>,
    /// SQL 查询超时秒数。
    pub query_timeout_secs: Option<usize>,
}

/// 达梦 Source 配置。
#[derive(Educe, Deserialize, Serialize, PartialEq, Clone)]
#[educe(Debug)]
pub struct DmdbSourceConf {
    /// 公共连接配置。
    pub conn: DmdbConnConf,
    /// 目标表，运行时要求显式提供。
    pub table: Option<String>,
    /// source 使用的增量游标列。
    pub cursor_column: Option<String>,
    /// source 游标类型，支持 `int` / `time`。
    pub cursor_type: Option<String>,
    /// source 首次启动且无 checkpoint 时的起点。
    pub start_from: Option<String>,
    /// source 起点时间格式，仅 `time` 游标使用。
    pub start_from_format: Option<String>,
    /// source 单批最大拉取记录数。
    pub batch: Option<usize>,
    /// source 空轮询间隔（毫秒）。
    pub poll_interval_ms: Option<u64>,
    /// source 查询失败后的退避间隔（毫秒）。
    pub error_backoff_ms: Option<u64>,
}

/// 达梦 Sink 配置。
#[derive(Educe, Deserialize, Serialize, PartialEq, Clone)]
#[educe(Debug)]
pub struct DmdbSinkConf {
    /// 公共连接配置。
    pub conn: DmdbConnConf,
    /// 目标表，运行时要求显式提供。
    pub table: Option<String>,
    /// 单批最大记录数。
    pub batch_size: Option<usize>,
}

impl DmdbConnConf {
    /// 将仓库统一配置映射为 ODBC 建连选项。
    pub fn connect_options(&self) -> ConnectionOptions {
        ConnectionOptions {
            login_timeout_sec: self.connect_timeout_secs.map(|secs| secs as u32),
            packet_size: None,
        }
    }

    /// 解析 `host:port` 形式的 endpoint，供运行时动态拼接连接串。
    pub fn endpoint_parts(&self) -> anyhow::Result<(&str, u16)> {
        let endpoint = self.endpoint.trim();
        if endpoint.is_empty() {
            return Err(anyhow::anyhow!(
                "dmdb.endpoint must not be empty when using endpoint connection"
            ));
        }

        let (host, port) = endpoint
            .rsplit_once(':')
            .ok_or_else(|| anyhow::anyhow!("dmdb.endpoint must be in host:port format"))?;

        if host.trim().is_empty() {
            return Err(anyhow::anyhow!("dmdb.endpoint host must not be empty"));
        }

        let port = port
            .parse::<u16>()
            .map_err(|_| anyhow::anyhow!("dmdb.endpoint port must be a valid u16 integer"))?;

        Ok((host, port))
    }

    /// 基于 endpoint 模式生成 DM ODBC 连接串。
    /// 用户名和密码会做 ODBC 属性值转义，避免特殊字符破坏连接串结构。
    pub fn generated_connection_string(&self) -> anyhow::Result<String> {
        let (host, port) = self.endpoint_parts()?;
        let username = escape_attribute_value(self.username.trim());
        let password = escape_attribute_value(self.password.as_str());

        let mut parts = vec![
            format!("Driver={{{}}}", self.driver.trim()),
            format!("SERVER={host}"),
            format!("TCP_PORT={port}"),
            format!("UID={username}"),
            format!("PWD={password}"),
        ];

        if let Some(schema) = self.schema.as_deref().map(str::trim)
            && !schema.is_empty()
        {
            parts.push(format!("SCHEMA={schema}"));
        }

        Ok(format!("{};", parts.join(";")))
    }
}

impl DmdbSinkConf {
    /// 归一化批量写入条数，避免运行时取到 0 或空值。
    pub fn normalized_batch_size(&self) -> usize {
        self.batch_size.unwrap_or(1024).max(1)
    }
}

#[cfg(test)]
mod tests {
    use super::DmdbConnConf;

    #[test]
    fn dmdb_config_builds_direct_connection_string() {
        let conf = DmdbConnConf {
            endpoint: "127.0.0.1:5236".into(),
            dsn: None,
            connection_string: None,
            driver: "DM8 ODBC DRIVER".into(),
            username: "SYSDBA".into(),
            password: "abc;123}".into(),
            schema: Some("WP_DATA".into()),
            connect_timeout_secs: None,
            query_timeout_secs: None,
        };

        let connection_string = conf
            .generated_connection_string()
            .expect("should build connection string");

        assert!(connection_string.contains("Driver={DM8 ODBC DRIVER}"));
        assert!(connection_string.contains("SERVER=127.0.0.1"));
        assert!(connection_string.contains("TCP_PORT=5236"));
        assert!(connection_string.contains("UID=SYSDBA"));
        assert!(connection_string.contains("PWD={abc;123}}};"));
        assert!(connection_string.contains("SCHEMA=WP_DATA"));
    }

    #[test]
    fn dmdb_config_rejects_invalid_endpoint() {
        let conf = DmdbConnConf {
            endpoint: "localhost".into(),
            dsn: None,
            connection_string: None,
            driver: "DM8 ODBC DRIVER".into(),
            username: "SYSDBA".into(),
            password: "Dameng123".into(),
            schema: None,
            connect_timeout_secs: None,
            query_timeout_secs: None,
        };

        let err = conf
            .generated_connection_string()
            .expect_err("endpoint without port should fail");
        assert!(err.to_string().contains("host:port"));
    }
}
