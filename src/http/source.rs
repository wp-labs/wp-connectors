use std::collections::HashMap;
use std::io::Read;
use std::sync::{Arc, Mutex as StdMutex, OnceLock};

use actix_web::dev::ServerHandle;
use actix_web::http::{
    Method, StatusCode,
    header::{CONTENT_ENCODING, CONTENT_TYPE},
};
use actix_web::{App, HttpRequest, HttpResponse, HttpServer, web};
use anyhow::Context;
use async_trait::async_trait;
use bytes::Bytes;
use flate2::read::GzDecoder;
use serde::Deserialize;
use serde_json::Value;
use tokio::sync::{Mutex, RwLock, mpsc};
use wp_conf_base::ConfParser;
use wp_connector_api::{
    CtrlRx, DataSource, SourceBatch, SourceEvent, SourceReason, SourceResult, Tags,
};
use wp_model_core::event_id::next_wp_event_id;
use wp_model_core::raw::RawData;

use crate::WP_SRC_VAL;

const DEFAULT_FMT: &str = "json";
const DEFAULT_COMPRESSION: &str = "none";
const HTTP_SOURCE_QUEUE_CAPACITY: usize = 1024;
const DEFAULT_BODY_LIMIT: usize = 16 * 1024 * 1024;

// 进程级 HTTP Source 运行时。
// 约束：同一端口只启动一个 actix server，不同 path 在该端口下复用，避免多个 source 争抢监听同一端口。
static HTTP_SOURCE_RUNTIME: OnceLock<Arc<HttpSourceRuntime>> = OnceLock::new();

#[derive(Debug, Clone)]
pub struct HttpSourceConfig {
    pub port: u16,
    pub path: String,
}

impl HttpSourceConfig {
    pub fn route_key(&self) -> String {
        format!("{}{}", self.port, self.path)
    }
}

pub struct HttpSource {
    key: Arc<String>,
    tags: Arc<Tags>,
    config: HttpSourceConfig,
    // HTTP handler 负责接收和解析请求，source 侧只消费已就绪的 payload，
    // 这样可以避免把网络接入和解析阻塞在 receive() 调度路径上。
    receiver: mpsc::Receiver<Vec<Bytes>>,
    runtime: Arc<HttpSourceRuntime>,
}

impl HttpSource {
    pub fn new(
        key: String,
        tags: Tags,
        config: HttpSourceConfig,
        receiver: mpsc::Receiver<Vec<Bytes>>,
    ) -> Self {
        Self {
            key: Arc::new(key),
            tags: Arc::new(tags),
            config,
            receiver,
            runtime: http_source_runtime(),
        }
    }

    pub async fn register(
        config: &HttpSourceConfig,
        sender: mpsc::Sender<Vec<Bytes>>,
    ) -> anyhow::Result<()> {
        http_source_runtime()
            .register(config.port, config.path.clone(), sender)
            .await
    }

    fn build_batch(&self, payloads: Vec<Bytes>) -> SourceBatch {
        // 进入队列的数据已经完成协议层解析，这里只负责补 SourceEvent 元信息。
        payloads
            .into_iter()
            .map(|payload| {
                SourceEvent::new(
                    next_wp_event_id(),
                    self.key.as_str(),
                    RawData::Bytes(payload),
                    self.tags.clone(),
                )
            })
            .collect()
    }
}

#[async_trait]
impl DataSource for HttpSource {
    async fn receive(&mut self) -> SourceResult<SourceBatch> {
        match self.receiver.recv().await {
            Some(payloads) => Ok(self.build_batch(payloads)),
            None => Err(SourceReason::Disconnect("http source channel closed".into()).into()),
        }
    }

    fn try_receive(&mut self) -> Option<SourceBatch> {
        self.receiver
            .try_recv()
            .ok()
            .map(|payloads| self.build_batch(payloads))
    }

    fn supports_try_receive(&self) -> bool {
        true
    }

    fn identifier(&self) -> String {
        self.key.to_string()
    }

    async fn start(&mut self, _ctrl_rx: CtrlRx) -> SourceResult<()> {
        Ok(())
    }

    async fn close(&mut self) -> SourceResult<()> {
        self.runtime
            .unregister(self.config.port, &self.config.path)
            .await;
        Ok(())
    }
}

fn http_source_runtime() -> Arc<HttpSourceRuntime> {
    HTTP_SOURCE_RUNTIME
        .get_or_init(|| Arc::new(HttpSourceRuntime::default()))
        .clone()
}

#[derive(Default)]
struct HttpSourceRuntime {
    // 按端口复用 server；path 级别路由在 PortRuntime 内部维护。
    ports: Mutex<HashMap<u16, Arc<PortRuntime>>>,
}

impl HttpSourceRuntime {
    async fn register(
        &self,
        port: u16,
        path: String,
        sender: mpsc::Sender<Vec<Bytes>>,
    ) -> anyhow::Result<()> {
        let port_runtime = self.ensure_port_runtime(port).await?;
        let mut routes = port_runtime.routes.write().await;
        if routes.contains_key(&path) {
            // `port + path` 是 source 的业务唯一键；重复注册直接拒绝，
            // 否则多个 source 会收到同一路径请求，语义不明确。
            anyhow::bail!("http source already exists for {}{}", port, path);
        }
        routes.insert(path, RouteTarget { sender });
        Ok(())
    }

    async fn unregister(&self, port: u16, path: &str) {
        let port_runtime = {
            let ports = self.ports.lock().await;
            ports.get(&port).cloned()
        };

        let Some(port_runtime) = port_runtime else {
            return;
        };

        let should_stop = {
            let mut routes = port_runtime.routes.write().await;
            routes.remove(path);
            routes.is_empty()
        };

        if should_stop {
            // 最后一个路由移除后主动关闭对应端口，避免测试或热重建时长期占用端口。
            {
                let mut ports = self.ports.lock().await;
                ports.remove(&port);
            }
            port_runtime.stop().await;
        }
    }

    async fn ensure_port_runtime(&self, port: u16) -> anyhow::Result<Arc<PortRuntime>> {
        let mut ports = self.ports.lock().await;
        if let Some(runtime) = ports.get(&port) {
            return Ok(runtime.clone());
        }

        let runtime = Arc::new(PortRuntime::new(port));
        runtime.start()?;
        ports.insert(port, runtime.clone());
        Ok(runtime)
    }
}

struct PortRuntime {
    port: u16,
    routes: RwLock<HashMap<String, RouteTarget>>,
    handle: StdMutex<Option<ServerHandle>>,
}

impl PortRuntime {
    fn new(port: u16) -> Self {
        Self {
            port,
            routes: RwLock::new(HashMap::new()),
            handle: StdMutex::new(None),
        }
    }

    fn start(self: &Arc<Self>) -> anyhow::Result<()> {
        let app_state = self.clone();
        let log_state = self.clone();
        let server = HttpServer::new(move || {
            App::new()
                // 这里统一做 body 上限保护；source 是接收入口，若不限制，错误请求可能直接放大内存占用。
                .app_data(web::PayloadConfig::new(DEFAULT_BODY_LIMIT))
                .app_data(web::Data::new(app_state.clone()))
                .default_service(web::to(handle_request))
        })
        .workers(1)
        .bind(("0.0.0.0", self.port))?
        .run();

        let handle = server.handle();
        *self.handle.lock().expect("lock http source server handle") = Some(handle);
        tokio::spawn(async move {
            if let Err(err) = server.await {
                log::error!(
                    "http source server on port {} exited: {}",
                    log_state.port,
                    err
                );
            }
        });

        Ok(())
    }

    async fn stop(&self) {
        let handle = {
            let mut guard = self.handle.lock().expect("lock http source server handle");
            guard.take()
        };
        if let Some(handle) = handle {
            handle.stop(true).await;
        }
    }
}

#[derive(Clone)]
struct RouteTarget {
    sender: mpsc::Sender<Vec<Bytes>>,
}

#[derive(Debug, Default, Deserialize)]
struct RequestQuery {
    fmt: Option<String>,
    compression: Option<String>,
}

async fn handle_request(
    request: HttpRequest,
    query: web::Query<RequestQuery>,
    body: web::Bytes,
    state: web::Data<Arc<PortRuntime>>,
) -> HttpResponse {
    if request.method() != Method::POST {
        return HttpResponse::MethodNotAllowed().body("only POST is supported");
    }

    let target = {
        let routes = state.routes.read().await;
        routes.get(request.path()).cloned()
    };

    let Some(target) = target else {
        return HttpResponse::NotFound().body("http source route not found");
    };

    let fmt = match resolve_fmt(&request, &query) {
        Ok(fmt) => fmt,
        Err(err) => return error_response(StatusCode::UNSUPPORTED_MEDIA_TYPE, err),
    };

    let compression = match resolve_compression(&request, &query) {
        Ok(compression) => compression,
        Err(err) => return error_response(StatusCode::UNSUPPORTED_MEDIA_TYPE, err),
    };

    let decoded = match decode_body(body, compression) {
        Ok(decoded) => decoded,
        Err(err) => {
            return error_response(
                StatusCode::BAD_REQUEST,
                format!("decode body failed: {err}"),
            );
        }
    };
    let payloads = match parse_payloads(&decoded, &fmt) {
        Ok(payloads) => payloads,
        Err(err) => {
            return error_response(StatusCode::BAD_REQUEST, format!("parse body failed: {err}"));
        }
    };

    match target.sender.send(payloads).await {
        Ok(()) => HttpResponse::Ok().body("OK"),
        Err(_) => error_response(StatusCode::GONE, "http source receiver dropped"),
    }
}

fn resolve_fmt(request: &HttpRequest, query: &RequestQuery) -> Result<String, String> {
    // 协议约束：请求参数优先级高于请求头，便于调用方在不改 header 的情况下做临时覆盖。
    let fmt = query
        .fmt
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_ascii_lowercase)
        .or_else(|| content_type_to_fmt(request.headers().get(CONTENT_TYPE)))
        .unwrap_or_else(|| DEFAULT_FMT.to_string());

    if matches!(fmt.as_str(), "json" | "ndjson") {
        Ok(fmt)
    } else {
        Err(format!("unsupported fmt: {fmt}"))
    }
}

fn content_type_to_fmt(value: Option<&actix_web::http::header::HeaderValue>) -> Option<String> {
    let raw = value?.to_str().ok()?.trim();
    let mime = raw
        .split(';')
        .next()
        .map(str::trim)
        .filter(|value| !value.is_empty())?
        .to_ascii_lowercase();

    match mime.as_str() {
        "application/json" => Some("json".to_string()),
        "application/x-ndjson" | "application/ndjson" => Some("ndjson".to_string()),
        _ => None,
    }
}

fn resolve_compression(
    request: &HttpRequest,
    query: &RequestQuery,
) -> Result<CompressionKind, String> {
    // 只认 Content-Encoding，不读取 Accept-Encoding。
    // 原因：这里处理的是“请求体已经采用何种编码”，不是“客户端希望响应怎么编码”。
    let compression = query
        .compression
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_ascii_lowercase)
        .or_else(|| {
            request
                .headers()
                .get(CONTENT_ENCODING)
                .and_then(|value| value.to_str().ok())
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_ascii_lowercase)
        })
        .unwrap_or_else(|| DEFAULT_COMPRESSION.to_string());

    match compression.as_str() {
        "none" | "identity" => Ok(CompressionKind::None),
        "gzip" => Ok(CompressionKind::Gzip),
        _ => Err(format!("unsupported compression: {compression}")),
    }
}

fn decode_body(body: web::Bytes, compression: CompressionKind) -> anyhow::Result<Bytes> {
    match compression {
        CompressionKind::None => Ok(body),
        CompressionKind::Gzip => {
            // Actix may already decode request bodies based on Content-Encoding,
            // so only decompress when the payload still carries the gzip signature.
            // 风险：不同框架/feature 组合对请求解压的时机可能不同，这里做兼容判断，避免二次解压失败。
            if !looks_like_gzip(body.as_ref()) {
                return Ok(body);
            }
            let mut decoder = GzDecoder::new(body.as_ref());
            let mut decoded = Vec::new();
            decoder
                .read_to_end(&mut decoded)
                .context("gzip decompression failed")?;
            Ok(Bytes::from(decoded))
        }
    }
}

fn looks_like_gzip(body: &[u8]) -> bool {
    body.len() >= 2 && body[0] == 0x1f && body[1] == 0x8b
}

fn parse_payloads(body: &[u8], fmt: &str) -> anyhow::Result<Vec<Bytes>> {
    match fmt {
        "json" => parse_json_payloads(body),
        "ndjson" => parse_ndjson_payloads(body),
        _ => anyhow::bail!("unsupported fmt: {fmt}"),
    }
}

fn parse_json_payloads(body: &[u8]) -> anyhow::Result<Vec<Bytes>> {
    // 业务语义：json 模式允许单对象或数组输入，统一展开成“多条记录”的内部表示，
    // 这样 source 下游不需要再区分顶层结构。
    let value: Value = serde_json::from_slice(body).context("invalid json payload")?;
    let values = match value {
        Value::Array(values) => values,
        other => vec![other],
    };

    values
        .into_iter()
        .map(|value| {
            serde_json::to_vec(&value)
                .map(Bytes::from)
                .map_err(anyhow::Error::from)
        })
        .collect()
}

fn parse_ndjson_payloads(body: &[u8]) -> anyhow::Result<Vec<Bytes>> {
    let text = std::str::from_utf8(body).context("ndjson payload is not valid utf-8")?;
    let mut lines = Vec::new();

    for (idx, raw_line) in text.lines().enumerate() {
        let line = raw_line.trim_end_matches('\r').trim();
        if line.is_empty() {
            continue;
        }
        // ndjson 不做整体 JSON 包装，但每一行必须是合法 JSON。
        // 这样能尽早把坏数据挡在入口，避免下游 parser 收到格式污染的行。
        let value: Value = serde_json::from_str(line)
            .with_context(|| format!("invalid ndjson line {}", idx + 1))?;
        lines.push(Bytes::from(serde_json::to_vec(&value)?));
    }

    Ok(lines)
}

fn error_response(status: StatusCode, message: impl Into<String>) -> HttpResponse {
    HttpResponse::build(status).body(message.into())
}

enum CompressionKind {
    None,
    Gzip,
}

pub fn build_source_tags(tags: &[String], config: &HttpSourceConfig) -> Tags {
    let mut meta_tags = Tags::from_parse(tags);
    meta_tags.set(WP_SRC_VAL, config.route_key());
    meta_tags
}

pub fn http_source_queue_capacity() -> usize {
    HTTP_SOURCE_QUEUE_CAPACITY
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::Compression;
    use flate2::write::GzEncoder;
    use serde_json::json;
    use std::collections::BTreeMap;
    use std::io::Write;
    use std::time::Duration;
    use wp_connector_api::{AsyncRawDataSink, SourceBuildCtx, SourceFactory, SourceSpec};

    use crate::http::{HttpSink, HttpSinkConfig, HttpSourceFactory};

    fn gzip_bytes(input: &[u8]) -> Vec<u8> {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(input).expect("write gzip input");
        encoder.finish().expect("finish gzip")
    }

    fn free_port() -> u16 {
        std::net::TcpListener::bind(("127.0.0.1", 0))
            .expect("bind free port")
            .local_addr()
            .expect("read local addr")
            .port()
    }

    fn build_spec(port: u16, path: &str) -> SourceSpec {
        let mut params = BTreeMap::new();
        params.insert("port".into(), json!(port));
        params.insert("path".into(), json!(path));
        SourceSpec {
            name: format!("http_{port}_{path}"),
            kind: "http".into(),
            connector_id: "connector".into(),
            params,
            tags: vec![],
        }
    }

    #[test]
    fn json_payload_becomes_one_line_per_element() {
        let lines = parse_payloads(br#"[{"a":1},{"b":2}]"#, "json").expect("parse json lines");
        let payloads = lines
            .into_iter()
            .map(|line| String::from_utf8(line.to_vec()).expect("utf8"))
            .collect::<Vec<_>>();
        assert_eq!(payloads, vec![r#"{"a":1}"#, r#"{"b":2}"#]);
    }

    #[test]
    fn ndjson_payload_is_split_and_validated() {
        let lines =
            parse_payloads(b"{\"a\":1}\n{\"b\":2}\n", "ndjson").expect("parse ndjson lines");
        let payloads = lines
            .into_iter()
            .map(|line| String::from_utf8(line.to_vec()).expect("utf8"))
            .collect::<Vec<_>>();
        assert_eq!(payloads, vec![r#"{"a":1}"#, r#"{"b":2}"#]);
    }

    #[test]
    fn invalid_ndjson_line_is_rejected() {
        let err = parse_payloads(b"{\"a\":1}\nnot-json\n", "ndjson")
            .expect_err("invalid ndjson should fail");
        assert!(err.to_string().contains("invalid ndjson line 2"));
    }

    #[test]
    fn gzip_body_is_decoded() {
        let body = gzip_bytes(b"{\"a\":1}\n");
        let decoded =
            decode_body(Bytes::from(body), CompressionKind::Gzip).expect("decode gzip body");
        assert_eq!(decoded, Bytes::from_static(b"{\"a\":1}\n"));
    }

    #[test]
    fn gzip_decode_skips_already_decoded_body() {
        let decoded = decode_body(Bytes::from_static(b"{\"a\":1}\n"), CompressionKind::Gzip)
            .expect("accept already decoded body");
        assert_eq!(decoded, Bytes::from_static(b"{\"a\":1}\n"));
    }

    #[test]
    fn query_fmt_has_higher_priority_than_content_type() {
        let request = actix_web::test::TestRequest::default()
            .insert_header((CONTENT_TYPE, "application/json"))
            .to_http_request();
        let query = web::Query(RequestQuery {
            fmt: Some("ndjson".into()),
            compression: None,
        });

        let fmt = resolve_fmt(&request, &query).expect("resolve fmt");
        assert_eq!(fmt, "ndjson");
    }

    #[test]
    fn query_compression_has_higher_priority_than_header() {
        let request = actix_web::test::TestRequest::default()
            .insert_header((CONTENT_ENCODING, "gzip"))
            .to_http_request();
        let query = web::Query(RequestQuery {
            fmt: None,
            compression: Some("none".into()),
        });

        let compression = resolve_compression(&request, &query).expect("resolve compression");
        assert!(matches!(compression, CompressionKind::None));
    }

    #[tokio::test]
    async fn source_receives_request_body_from_runtime() {
        let port = free_port();
        let path = "/ingest/runtime-test";
        let spec = build_spec(port, path);
        let factory = HttpSourceFactory;
        let ctx = SourceBuildCtx::new(std::env::temp_dir());
        let mut service = factory.build(&spec, &ctx).await.expect("build source");
        let mut handle = service.sources.remove(0);

        let client = reqwest::Client::new();
        let endpoint = format!("http://127.0.0.1:{port}{path}?fmt=json");

        for _ in 0..20 {
            let response = client
                .post(&endpoint)
                .body("[{\"a\":1},{\"a\":2}]")
                .send()
                .await;
            if let Ok(response) = response {
                assert_eq!(response.status(), reqwest::StatusCode::OK);
                assert_eq!(response.text().await.expect("read response body"), "OK");
                let batch = handle.source.receive().await.expect("receive batch");
                let payloads = batch
                    .into_iter()
                    .map(|event| {
                        String::from_utf8(event.payload.into_bytes().to_vec()).expect("utf8")
                    })
                    .collect::<Vec<_>>();
                assert_eq!(payloads, vec![r#"{"a":1}"#, r#"{"a":2}"#]);
                handle.source.close().await.expect("close source");
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        panic!("http source server did not become ready in time");
    }

    #[tokio::test]
    async fn invalid_ndjson_returns_reason() {
        let port = free_port();
        let path = "/ingest/invalid-ndjson";
        let spec = build_spec(port, path);
        let factory = HttpSourceFactory;
        let ctx = SourceBuildCtx::new(std::env::temp_dir());
        let mut service = factory.build(&spec, &ctx).await.expect("build source");
        let mut handle = service.sources.remove(0);

        let client = reqwest::Client::new();
        let endpoint = format!("http://127.0.0.1:{port}{path}?fmt=ndjson");

        for _ in 0..20 {
            let response = client
                .post(&endpoint)
                .body("{\"a\":1}\nnot-json\n")
                .send()
                .await;
            if let Ok(response) = response {
                assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);
                let body = response.text().await.expect("read response body");
                assert!(body.contains("invalid ndjson line 2"));
                handle.source.close().await.expect("close source");
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        panic!("http source server did not become ready in time");
    }

    #[tokio::test]
    async fn http_sink_gzip_can_send_to_http_source() {
        let port = free_port();
        let path = "/ingest/gzip-roundtrip";
        let spec = build_spec(port, path);
        let factory = HttpSourceFactory;
        let ctx = SourceBuildCtx::new(std::env::temp_dir());
        let mut service = factory.build(&spec, &ctx).await.expect("build source");
        let mut handle = service.sources.remove(0);

        let endpoint = format!("http://127.0.0.1:{port}{path}");
        let config = HttpSinkConfig::new(
            endpoint,
            None,
            None,
            None,
            None,
            Some("json".to_string()),
            None,
            Some(5),
            Some(0),
            Some("gzip".to_string()),
        );
        let mut sink = HttpSink::new(config).await.expect("build sink");

        for _ in 0..20 {
            match sink.sink_str(r#"[{"a":1},{"a":2}]"#).await {
                Ok(()) => {
                    let batch = handle.source.receive().await.expect("receive batch");
                    let payloads = batch
                        .into_iter()
                        .map(|event| {
                            String::from_utf8(event.payload.into_bytes().to_vec()).expect("utf8")
                        })
                        .collect::<Vec<_>>();
                    assert_eq!(payloads, vec![r#"{"a":1}"#, r#"{"a":2}"#]);
                    handle.source.close().await.expect("close source");
                    return;
                }
                Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
            }
        }

        panic!("http sink gzip could not send to http source in time");
    }
}
