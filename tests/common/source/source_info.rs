#![allow(dead_code)]

use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;
use wp_connector_api::{ParamMap, SourceFactory};

/// 将任何可序列化对象转换为 ParamMap。
#[allow(dead_code)]
pub fn to_param_map<T: Serialize>(obj: &T) -> Result<ParamMap> {
    let value = serde_json::to_value(obj)?;
    match value {
        Value::Object(map) => Ok(map.into_iter().collect()),
        _ => anyhow::bail!("Expected an object, got {:?}", value),
    }
}

/// 将 ParamMap 转换为指定类型的对象。
#[allow(dead_code)]
pub fn from_param_map<T: for<'de> Deserialize<'de>>(params: &ParamMap) -> Result<T> {
    let value = Value::Object(params.clone().into_iter().collect());
    let obj = serde_json::from_value(value)?;
    Ok(obj)
}

pub type AsyncInitFn =
    Box<dyn Fn() -> Pin<Box<dyn Future<Output = Result<()>> + Send>> + Send + Sync>;

pub type AsyncWaitReadyFn =
    Box<dyn Fn(ParamMap) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> + Send + Sync>;

pub type AsyncInputFn =
    Box<dyn Fn(ParamMap) -> Pin<Box<dyn Future<Output = Result<usize>> + Send>> + Send + Sync>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SourceRunPhase {
    Initial,
    AfterRestart,
}

#[derive(Clone, Copy, Debug)]
pub struct SourceCollectConfig {
    /// collect 阶段的总时间窗口；时间到后停止收集并进入断言。
    pub timeout: Duration,
    /// 单次轮询未拿到数据时的退避间隔，避免空转占满 CPU。
    pub poll_interval: Duration,
}

impl Default for SourceCollectConfig {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(5),
            poll_interval: Duration::from_millis(100),
        }
    }
}

/// Source 集成测试信息。
pub struct SourceInfo<F: SourceFactory> {
    /// 被测试的 `SourceFactory` 实例。
    factory: F,
    /// 可选测试名称，用于生成更易读的日志展示名。
    test_name: Option<String>,
    /// 构建 `SourceSpec` 时使用的参数。
    params: ParamMap,
    /// 可选初始化逻辑，通常用于建表、建 topic、清理历史数据等。
    init_fn: Option<AsyncInitFn>,
    /// 可选就绪检查，用于确认 connector 依赖已经可用。
    wait_ready_fn: Option<AsyncWaitReadyFn>,
    /// 输入动作，在 source 构建完成后执行，用于向上游送入测试数据。
    input_fn: Option<AsyncInputFn>,
    /// 输入动作的执行次数；总理论输入量为多次执行返回值之和。
    input_repeat: usize,
    /// collect 窗口与轮询节奏配置。
    collect_config: SourceCollectConfig,
    /// 是否在首次运行结束后再执行一轮重启验证。
    restart_verification: bool,
}

impl<F: SourceFactory> SourceInfo<F> {
    pub fn new(factory: F, params: ParamMap) -> Self {
        Self {
            factory,
            test_name: None,
            params,
            init_fn: None,
            wait_ready_fn: None,
            input_fn: None,
            input_repeat: 1,
            collect_config: SourceCollectConfig::default(),
            restart_verification: false,
        }
    }

    pub fn with_test_name(mut self, test_name: impl Into<String>) -> Self {
        self.test_name = Some(test_name.into());
        self
    }

    pub fn with_async_init<Fut>(mut self, init_fn: impl Fn() -> Fut + Send + Sync + 'static) -> Self
    where
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        self.init_fn = Some(Box::new(move || Box::pin(init_fn())));
        self
    }

    pub fn with_async_wait_ready<Fut>(
        mut self,
        wait_ready_fn: impl Fn(ParamMap) -> Fut + Send + Sync + 'static,
    ) -> Self
    where
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        self.wait_ready_fn = Some(Box::new(move |params| Box::pin(wait_ready_fn(params))));
        self
    }

    pub fn with_async_input<Fut>(
        mut self,
        input_fn: impl Fn(ParamMap) -> Fut + Send + Sync + 'static,
    ) -> Self
    where
        Fut: Future<Output = Result<usize>> + Send + 'static,
    {
        self.input_fn = Some(Box::new(move |params| Box::pin(input_fn(params))));
        self
    }

    pub fn with_input_repeat(mut self, input_repeat: usize) -> Self {
        self.input_repeat = input_repeat.max(1);
        self
    }

    pub fn with_collect_timeout(mut self, timeout: Duration) -> Self {
        self.collect_config.timeout = timeout;
        self
    }

    pub fn with_poll_interval(mut self, poll_interval: Duration) -> Self {
        self.collect_config.poll_interval = poll_interval;
        self
    }

    pub fn with_restart_verification(mut self, enabled: bool) -> Self {
        self.restart_verification = enabled;
        self
    }

    pub fn factory(&self) -> &F {
        &self.factory
    }

    pub fn params(&self) -> &ParamMap {
        &self.params
    }

    pub fn test_name(&self) -> Option<&str> {
        self.test_name.as_deref()
    }

    pub fn collect_config(&self) -> SourceCollectConfig {
        self.collect_config
    }

    pub fn restart_verification(&self) -> bool {
        self.restart_verification
    }

    pub fn input_repeat(&self) -> usize {
        self.input_repeat
    }

    pub async fn init(&self) -> Result<()> {
        if let Some(init_fn) = &self.init_fn {
            init_fn().await?;
        }
        Ok(())
    }

    pub async fn wait_ready(&self) -> Result<()> {
        if let Some(wait_ready_fn) = &self.wait_ready_fn {
            wait_ready_fn(self.params.clone()).await?;
        }
        Ok(())
    }

    pub async fn input(&self) -> Result<usize> {
        if let Some(input_fn) = &self.input_fn {
            return input_fn(self.params.clone()).await;
        }
        Ok(0)
    }
}
