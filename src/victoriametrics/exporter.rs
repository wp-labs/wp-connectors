//! 按名称共享指标状态；各传输配置独立定时推送指标快照。
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, LazyLock, Mutex, Weak};
use std::time::Duration;

use async_trait::async_trait;
use orion_error::prelude::SourceRawErr;
use prometheus::core::Metric;
use prometheus::{
    Counter, Encoder, Gauge, Opts, TextEncoder,
    proto::{MetricFamily, MetricType},
};
use serde_json::Value;
use tokio::task::JoinHandle;
use wp_connector_api::{SinkReason, SinkResult};
use wp_model_core::model::DataRecord;

use super::config::{MetricKind, VictoriaMetric, duration, invalid};
use super::mapper::{Mapper, MetricFamilyDefinition, MetricUpdate};
use crate::http_utils::join_endpoint_path;

/// 按 SinkSpec.name 共享指标状态，并复用相同传输配置的运行任务。
static RUNTIMES: LazyLock<Mutex<HashMap<String, NamedRuntimes>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// 一个逻辑 sink 名称下的共享状态和传输运行实例。
#[derive(Default)]
struct NamedRuntimes {
    /// 弱引用避免索引表延长指标状态的生命周期；活跃 runtime 持有实际状态。
    metric_store: Weak<SharedMetricStore>,
    /// 同一 name 可有多个传输配置；相同配置的 worker 共用对应 runtime。
    runtimes: Vec<Weak<SinkRuntime>>,
}

/// 单个 worker 持有的 sink 句柄；映射器独立，指标状态按名称共享。
pub(crate) struct VictoriaMetricExporter {
    runtime: Option<Arc<SinkRuntime>>,
    mapper: Mapper,
}
/// 一个推送任务使用的传输和推送配置；映射规则不在这里，因此同配置 worker 可共用任务。
#[derive(Clone, PartialEq)]
struct SharedSinkConfig {
    endpoint: String,
    api_path: String,
    flush_secs: f64,
    timeout_secs: f64,
}
impl From<&VictoriaMetric> for SharedSinkConfig {
    fn from(sink_config: &VictoriaMetric) -> Self {
        Self {
            endpoint: sink_config.endpoint.clone(),
            api_path: sink_config.api_path.clone(),
            flush_secs: sink_config.flush_secs,
            timeout_secs: sink_config.timeout_secs,
        }
    }
}
/// 保存同一传输配置的 worker 共用的 HTTP client、指标状态和定时任务。
struct SinkRuntime {
    transport_config: SharedSinkConfig,
    http_client: reqwest::Client,
    metric_store: Arc<SharedMetricStore>,
    flush_task: Option<JoinHandle<()>>,
}
/// 可由输入处理和推送任务并发访问的指标状态。
struct SharedMetricStore {
    store: Mutex<MetricStore>,
}
/// 每条时间序列独立持有的 Prometheus 指标值。
enum SeriesValue {
    Gauge(Gauge),
    Counter(Counter),
}

/// 保存一条时间序列的当前值和样本时间。
struct SeriesState {
    metric: SeriesValue,
    timestamp: Option<i64>,
}
type Labels = BTreeMap<String, String>;

/// 同名序列共用指标类型和 HELP，标签由每条序列独立决定。
struct MetricFamilyState {
    definition: MetricFamilyDefinition,
    series: BTreeMap<Labels, SeriesState>,
}

/// 每个 sink 名称共享指标族及其时间序列。
#[derive(Default)]
struct MetricStore {
    families: BTreeMap<String, MetricFamilyState>,
}
impl SharedMetricStore {
    /// 创建独立的指标状态。
    fn new() -> Self {
        Self {
            store: Mutex::new(MetricStore::default()),
        }
    }
    /// 获取 指标状态锁，并将锁中毒转换为 sink 错误。
    fn lock(&self) -> SinkResult<std::sync::MutexGuard<'_, MetricStore>> {
        self.store.lock().map_err(|_| invalid("指标状态锁不可用"))
    }
}

impl SeriesState {
    /// 创建带有本条序列标签的 Prometheus 指标。
    fn new(metric_update: &MetricUpdate) -> SinkResult<Self> {
        // 客户端构造指标时要求非空 HELP，导出时仍以指标族配置为准。
        let help_text = metric_update
            .help
            .as_deref()
            .filter(|help_text| !help_text.is_empty());
        let metric_options = Opts::new(
            &metric_update.name,
            help_text.unwrap_or(&metric_update.name),
        )
        .const_labels(metric_update.labels.clone().into_iter().collect());
        let metric = match metric_update.kind {
            MetricKind::Gauge => Gauge::with_opts(metric_options)
                .map(SeriesValue::Gauge)
                .map_err(|error| invalid(format!("创建 Gauge 失败: {error}")))?,
            MetricKind::Counter => Counter::with_opts(metric_options)
                .map(SeriesValue::Counter)
                .map_err(|error| invalid(format!("创建 Counter 失败: {error}")))?,
        };
        Ok(Self {
            metric,
            timestamp: None,
        })
    }

    /// 更新序列当前值，Counter 直接采用 Prometheus 客户端的累加行为。
    fn apply(&mut self, metric_update: &MetricUpdate) {
        match &self.metric {
            SeriesValue::Gauge(gauge) => gauge.set(metric_update.value),
            SeriesValue::Counter(counter) => counter.inc_by(metric_update.value),
        }
        self.timestamp = metric_update.timestamp;
    }
}

impl MetricStore {
    /// 先校验整批定义并准备新序列，再更新状态，避免映射或 schema 错误造成部分写入。
    fn apply(&mut self, metric_updates: &[MetricUpdate]) -> SinkResult<()> {
        let mut batch_family_definitions = BTreeMap::new();
        let mut new_series_by_key = BTreeMap::new();
        for metric_update in metric_updates {
            let series_key = metric_update.series_key();
            let family_definition = metric_update.family_definition();
            let existing_family = self.families.get(&metric_update.name);
            if existing_family
                .is_some_and(|family_state| family_state.definition != family_definition)
                || batch_family_definitions
                    .get(&metric_update.name)
                    .is_some_and(|existing_definition| existing_definition != &family_definition)
            {
                return Err(invalid(format!(
                    "指标 {} 类型/HELP 冲突",
                    metric_update.name
                )));
            }
            batch_family_definitions.insert(metric_update.name.clone(), family_definition);
            if !existing_family
                .is_some_and(|family_state| family_state.series.contains_key(&series_key.labels))
                && let std::collections::btree_map::Entry::Vacant(series_entry) =
                    new_series_by_key.entry(series_key)
            {
                series_entry.insert(SeriesState::new(metric_update)?);
            }
        }

        // 所有可能失败的校验和新指标构造都已完成；下面按顺序应用 Gauge/Counter 更新。
        for (metric_name, family_definition) in batch_family_definitions {
            self.families
                .entry(metric_name)
                .or_insert_with(|| MetricFamilyState {
                    definition: family_definition,
                    series: BTreeMap::new(),
                });
        }
        for (series_key, series_state) in new_series_by_key {
            self.families
                .get_mut(&series_key.name)
                .expect("指标族已准备")
                .series
                .insert(series_key.labels, series_state);
        }
        for metric_update in metric_updates {
            let series_key = metric_update.series_key();
            self.families
                .get_mut(&series_key.name)
                .expect("指标族已准备")
                .series
                .get_mut(&series_key.labels)
                .expect("时间序列已准备")
                .apply(metric_update);
        }
        Ok(())
    }

    /// 按指标名收集快照，同一指标族可包含不同标签集合的样本。
    fn gather(&self) -> Vec<MetricFamily> {
        // 手动组装 MetricFamily，允许同一指标名下的序列拥有不同标签集合。
        self.families
            .iter()
            .map(|(metric_name, family_state)| {
                let mut family_snapshot = MetricFamily::default();
                family_snapshot.set_name(metric_name.clone());
                family_snapshot.set_help(family_state.definition.help.clone().unwrap_or_default());
                family_snapshot.set_field_type(match family_state.definition.kind {
                    MetricKind::Gauge => MetricType::GAUGE,
                    MetricKind::Counter => MetricType::COUNTER,
                });
                for series_state in family_state.series.values() {
                    let mut metric_sample = match &series_state.metric {
                        SeriesValue::Gauge(gauge) => gauge.metric(),
                        SeriesValue::Counter(counter) => counter.metric(),
                    };
                    if let Some(timestamp_ms) = series_state.timestamp {
                        metric_sample.set_timestamp_ms(timestamp_ms);
                    }
                    family_snapshot.mut_metric().push(metric_sample);
                }
                family_snapshot
            })
            .collect()
    }
}

impl Drop for SinkRuntime {
    /// 运行实例被异常释放时取消后台推送任务。
    fn drop(&mut self) {
        if let Some(flush_task) = &self.flush_task {
            flush_task.abort();
        }
    }
}
impl VictoriaMetricExporter {
    /// 按名称共享指标状态，复用相同传输配置的推送任务，并创建独立映射器。
    pub(crate) fn for_name(sink_name: &str, sink_config: VictoriaMetric) -> SinkResult<Self> {
        if sink_name.trim().is_empty() {
            return Err(invalid("victoriametrics.name 不能为空"));
        }
        let mapper = Mapper::new(&sink_config)?;
        let transport_config = SharedSinkConfig::from(&sink_config);
        let mut runtime_index = RUNTIMES.lock().map_err(|_| invalid("运行实例锁不可用"))?;
        // 弱引用只用于索引；清理已退出的 runtime，避免 name 表保留过期条目。
        runtime_index.retain(|_, named_runtimes| {
            named_runtimes
                .runtimes
                .retain(|runtime_ref| runtime_ref.strong_count() > 0);
            !named_runtimes.runtimes.is_empty()
        });
        let named_runtimes = runtime_index.entry(sink_name.into()).or_default();
        // 相同传输配置共用 client 和 flush 任务；Mapper 仍由每个 sink 句柄单独持有。
        if let Some(runtime) = named_runtimes
            .runtimes
            .iter()
            .filter_map(Weak::upgrade)
            .find(|runtime| runtime.transport_config == transport_config)
        {
            return Ok(Self {
                runtime: Some(runtime),
                mapper,
            });
        }
        // 不同传输配置建立独立 runtime，但升级同一个 name 下的指标状态弱引用。
        let metric_store = match named_runtimes.metric_store.upgrade() {
            Some(metric_store) => metric_store,
            None => {
                let metric_store = Arc::new(SharedMetricStore::new());
                named_runtimes.metric_store = Arc::downgrade(&metric_store);
                metric_store
            }
        };
        let http_client = reqwest::Client::builder()
            .timeout(duration(sink_config.timeout_secs, "timeout_secs")?)
            .build()
            .source_raw_err(SinkReason::Sink, "创建 VictoriaMetrics HTTP client 失败")?;
        let mut runtime = SinkRuntime {
            transport_config,
            http_client,
            metric_store,
            flush_task: None,
        };
        runtime.start()?;
        let runtime = Arc::new(runtime);
        named_runtimes.runtimes.push(Arc::downgrade(&runtime));
        Ok(Self {
            runtime: Some(runtime),
            mapper,
        })
    }
    /// 获取当前运行实例；已停止的句柄返回 sink 错误。
    fn runtime(&self) -> SinkResult<&SinkRuntime> {
        self.runtime
            .as_deref()
            .ok_or_else(|| invalid("VictoriaMetrics sink 已停止"))
    }

    /// 逐个消费 JSON 对象并映射；整批通过后更新 Prometheus 指标。
    async fn submit(&self, input_objects: Vec<Value>) -> SinkResult<()> {
        let metric_updates = self.mapper.map_batch(input_objects)?;
        self.submit_updates(metric_updates).await
    }

    /// 从借用的 DataRecord 直接映射，避免中间 JSON 树。
    async fn submit_record(&self, data_record: &DataRecord) -> SinkResult<()> {
        let metric_updates = self.mapper.map_records(std::iter::once(data_record))?;
        self.submit_updates(metric_updates).await
    }

    /// 映射借用的 DataRecord 批次；整批映射完成后才提交。
    async fn submit_records(&self, data_records: &[Arc<DataRecord>]) -> SinkResult<()> {
        let metric_updates = self
            .mapper
            .map_records(data_records.iter().map(AsRef::as_ref))?;
        self.submit_updates(metric_updates).await
    }

    /// 将排序后的整批映射结果应用到该 sink 名称共享的指标状态。
    async fn submit_updates(&self, mut metric_updates: Vec<MetricUpdate>) -> SinkResult<()> {
        let runtime = self.runtime()?;
        if metric_updates.is_empty() {
            return Ok(());
        }
        // 只排序本次调用：显式时间戳按升序应用；无时间戳样本由 VM 按接收时间处理，因此排在最后。
        metric_updates.sort_by(|left, right| {
            left.timestamp
                .is_none()
                .cmp(&right.timestamp.is_none())
                .then_with(|| left.timestamp.cmp(&right.timestamp))
        });
        runtime.metric_store.lock()?.apply(&metric_updates)
    }

    /// 解析 raw JSON 对象或对象数组，再交给统一映射和 指标状态更新流程。
    async fn submit_raw(&self, raw_payloads: Vec<&[u8]>) -> SinkResult<()> {
        let parsed_input_objects = (|| {
            let mut input_objects = Vec::new();
            for raw_payload in &raw_payloads {
                // 多个 payload 和 object 数组摊平成一次映射批次；任一输入非法时整批不提交。
                match serde_json::from_slice::<Value>(raw_payload)
                    .map_err(|error| invalid(format!("JSON 解析失败: {error}")))?
                {
                    Value::Object(object) => input_objects.push(Value::Object(object)),
                    Value::Array(array) if array.iter().all(Value::is_object) => {
                        input_objects.extend(array)
                    }
                    _ => return Err(invalid("JSON 输入仅支持 object 或 object 数组")),
                }
            }
            Ok(input_objects)
        })();
        let input_objects = parsed_input_objects.inspect_err(|parse_error| {
            log::debug!("JSON 输入失败: {parse_error}; 输入: {raw_payloads:?}")
        })?;
        self.submit(input_objects)
            .await
            .inspect_err(|mapping_error| {
                log::debug!(
                    "指标批次拒绝: {mapping_error}; 输入: {:?}",
                    raw_payloads
                        .iter()
                        .map(|raw_payload| String::from_utf8_lossy(raw_payload))
                        .collect::<Vec<_>>()
                )
            })
    }
}

impl SinkRuntime {
    /// 启动定时快照推送任务，并在发送失败后按退避间隔重试。
    fn start(&mut self) -> SinkResult<()> {
        let metric_store = self.metric_store.clone();
        let http_client = self.http_client.clone();
        let transport_config = self.transport_config.clone();
        let flush_interval = duration(transport_config.flush_secs, "flush_secs")?;
        // 推送任务持有共享指标状态和传输参数，避免与运行实例形成循环引用。
        self.flush_task = Some(tokio::spawn(async move {
            let mut flush_timer = tokio::time::interval(flush_interval);
            flush_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                flush_timer.tick().await;
                let mut retry_delay_secs = 1;
                // 失败时保留指标状态；每次重试重新 gather 最新快照，退避上限为 30 秒。
                while let Err(flush_error) =
                    flush(&metric_store, &http_client, &transport_config).await
                {
                    log::error!("VictoriaMetrics flush 失败: {flush_error}");
                    tokio::time::sleep(Duration::from_secs(retry_delay_secs)).await;
                    retry_delay_secs = (retry_delay_secs * 2).min(30);
                }
                flush_timer.reset();
            }
        }));
        Ok(())
    }
    /// 取消定时任务，并在请求超时范围内执行最终快照推送。
    async fn stop(&mut self) -> SinkResult<()> {
        if let Some(flush_task) = self.flush_task.take() {
            flush_task.abort();
            let _ = flush_task.await;
        }
        let final_flush_result = tokio::time::timeout(
            duration(self.transport_config.timeout_secs, "timeout_secs")?,
            flush(
                &self.metric_store,
                &self.http_client,
                &self.transport_config,
            ),
        )
        .await;
        match final_flush_result {
            Ok(Ok(())) => Ok(()),
            result => {
                let error_detail = match result {
                    Ok(Err(push_error)) => push_error.to_string(),
                    Err(timeout_error) => timeout_error.to_string(),
                    _ => unreachable!(),
                };
                Err(invalid(format!("最终快照推送失败: {error_detail}")))
            }
        }
    }
}

/// 收集当前指标，编码并推送快照。
async fn flush(
    metric_store: &SharedMetricStore,
    http_client: &reqwest::Client,
    transport_config: &SharedSinkConfig,
) -> SinkResult<()> {
    // gather 得到自有快照后立即释放状态锁，网络等待不会阻塞输入更新。
    let metric_snapshot = metric_store.lock()?.gather();
    if metric_snapshot.is_empty() {
        return Ok(());
    }
    push(http_client, transport_config, &metric_snapshot).await
}

/// 使用 Prometheus TextEncoder 对指标快照进行编码。
fn encode(metric_families: &[MetricFamily]) -> SinkResult<Vec<u8>> {
    let mut encoded_payload = Vec::new();
    TextEncoder::new()
        .encode(metric_families, &mut encoded_payload)
        .source_raw_err(SinkReason::Sink, "Prometheus 编码失败")?;
    Ok(encoded_payload)
}
/// 将编码后的指标快照 POST 到 VictoriaMetrics 导入接口。
async fn push(
    http_client: &reqwest::Client,
    transport_config: &SharedSinkConfig,
    metric_families: &[MetricFamily],
) -> SinkResult<()> {
    let request_body = encode(metric_families)?;
    let http_response = http_client
        .post(join_endpoint_path(
            &transport_config.endpoint,
            &transport_config.api_path,
        ))
        .header(
            reqwest::header::CONTENT_TYPE,
            TextEncoder::new().format_type(),
        )
        .body(request_body)
        .send()
        .await
        .source_raw_err(SinkReason::Sink, "VictoriaMetrics HTTP 请求失败")?;
    if !http_response.status().is_success() {
        let http_status = http_response.status();
        let response_body = http_response.text().await.unwrap_or_default();
        return Err(invalid(format!(
            "VictoriaMetrics HTTP {http_status}: {response_body}"
        )));
    }
    Ok(())
}

#[async_trait]
impl wp_connector_api::AsyncRecordSink for VictoriaMetricExporter {
    /// 直接映射并提交一条 DataRecord。
    async fn sink_record(&mut self, data_record: &DataRecord) -> SinkResult<()> {
        self.submit_record(data_record)
            .await
            .inspect_err(|error| log::debug!("DataRecord 批次拒绝: {error}; 输入: {data_record:?}"))
    }
    /// 逐条直接映射 DataRecord，并在整批通过后一次性提交。
    async fn sink_records(&mut self, data_records: Vec<Arc<DataRecord>>) -> SinkResult<()> {
        self.submit_records(&data_records)
            .await
            .inspect_err(|error| {
                log::debug!("DataRecord 批次拒绝: {error}; 输入: {data_records:?}")
            })
    }
}
#[async_trait]
impl wp_connector_api::AsyncRawDataSink for VictoriaMetricExporter {
    /// 提交 UTF-8 JSON 文本，可为单个对象或对象数组。
    async fn sink_str(&mut self, json_text: &str) -> SinkResult<()> {
        self.submit_raw(vec![json_text.as_bytes()]).await
    }
    /// 提交 JSON 字节，可为单个对象或对象数组。
    async fn sink_bytes(&mut self, json_payload: &[u8]) -> SinkResult<()> {
        self.submit_raw(vec![json_payload]).await
    }
    /// 提交多段 UTF-8 JSON 输入，并将整批作为一次提交处理。
    async fn sink_str_batch(&mut self, json_texts: Vec<&str>) -> SinkResult<()> {
        self.submit_raw(
            json_texts
                .iter()
                .map(|json_text| json_text.as_bytes())
                .collect(),
        )
        .await
    }
    /// 提交多段 JSON 字节输入，并将整批作为一次提交处理。
    async fn sink_bytes_batch(&mut self, json_payloads: Vec<&[u8]>) -> SinkResult<()> {
        self.submit_raw(json_payloads).await
    }
}
#[async_trait]
impl wp_connector_api::AsyncCtrl for VictoriaMetricExporter {
    /// 释放当前 worker；该传输任务的最后一个 worker 会触发最终 flush。
    async fn stop(&mut self) -> SinkResult<()> {
        // 只有最后一个持有 runtime 的句柄能取得 Arc 并停止任务、执行最终推送。
        if let Some(runtime) = self.runtime.take()
            && let Some(mut runtime) = Arc::into_inner(runtime)
        {
            return runtime.stop().await;
        }
        Ok(())
    }
    /// 检查当前 sink 句柄是否仍连接到运行实例。
    async fn reconnect(&mut self) -> SinkResult<()> {
        self.runtime().map(|_| ())
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
