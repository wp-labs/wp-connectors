#![allow(dead_code)]

use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::future::Future;
use std::pin::Pin;
use wp_connector_api::{ParamMap, SinkFactory};

/// 将任何可序列化的对象转换为 ParamMap
#[allow(dead_code)]
pub fn to_param_map<T: Serialize>(obj: &T) -> Result<ParamMap> {
    let value = serde_json::to_value(obj)?;
    match value {
        Value::Object(map) => Ok(map.into_iter().collect()),
        _ => anyhow::bail!("Expected an object, got {:?}", value),
    }
}

/// 将 ParamMap 转换为指定类型的对象
#[allow(dead_code)]
pub fn from_param_map<T: for<'de> Deserialize<'de>>(params: &ParamMap) -> Result<T> {
    let value = Value::Object(params.clone().into_iter().collect());
    let obj = serde_json::from_value(value)?;
    Ok(obj)
}

/// 异步初始化函数类型
pub type AsyncInitFn =
    Box<dyn Fn() -> Pin<Box<dyn Future<Output = Result<()>> + Send>> + Send + Sync>;

/// 异步计数函数类型
pub type AsyncCountFn =
    Box<dyn Fn(ParamMap) -> Pin<Box<dyn Future<Output = Result<i64>> + Send>> + Send + Sync>;

/// 异步就绪检查函数类型
pub type AsyncWaitReadyFn =
    Box<dyn Fn(ParamMap) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> + Send + Sync>;

/// Sink 集成测试信息结构体
pub struct SinkInfo<F: SinkFactory> {
    /// SinkFactory 实例（必须）
    factory: F,
    /// 测试名称（可选）
    test_name: Option<String>,
    /// 测试配置参数（必须）
    params: ParamMap,
    /// 异步初始化方法（可选）
    init_fn: Option<AsyncInitFn>,
    /// 初始化脚本路径（可选）
    init_sh: Option<String>,
    /// 异步计数方法（可选）- 用于获取 sink 当前发送的数量
    count_fn: Option<AsyncCountFn>,
    /// Sink 级别的就绪检查（可选）
    wait_ready_fn: Option<AsyncWaitReadyFn>,
}

impl<F: SinkFactory> SinkInfo<F> {
    /// 创建新的集成测试信息实例（必须提供 factory 和 params）
    pub fn new(factory: F, params: ParamMap) -> Self {
        Self {
            factory,
            test_name: None,
            params,
            init_fn: None,
            init_sh: None,
            count_fn: None,
            wait_ready_fn: None,
        }
    }

    /// 设置测试名称（链式调用）
    pub fn with_test_name(mut self, test_name: impl Into<String>) -> Self {
        self.test_name = Some(test_name.into());
        self
    }

    /// 设置异步初始化方法（链式调用）
    pub fn with_async_init<Fut>(mut self, init_fn: impl Fn() -> Fut + Send + Sync + 'static) -> Self
    where
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        self.init_fn = Some(Box::new(move || Box::pin(init_fn())));
        self
    }

    /// 设置初始化脚本（链式调用）
    #[allow(dead_code)]
    pub fn with_init_sh(mut self, script_path: String) -> Self {
        self.init_sh = Some(script_path);
        self
    }

    /// 设置异步计数方法（链式调用）
    pub fn with_async_count_fn<Fut>(
        mut self,
        count_fn: impl Fn(ParamMap) -> Fut + Send + Sync + 'static,
    ) -> Self
    where
        Fut: Future<Output = Result<i64>> + Send + 'static,
    {
        self.count_fn = Some(Box::new(move |params| Box::pin(count_fn(params))));
        self
    }

    /// 设置 Sink 级别异步就绪检查（链式调用）
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

    /// 获取 factory 引用
    pub fn factory(&self) -> &F {
        &self.factory
    }

    /// 获取 params 引用
    pub fn params(&self) -> &ParamMap {
        &self.params
    }

    /// 获取测试名称
    pub fn test_name(&self) -> Option<&str> {
        self.test_name.as_deref()
    }

    /// 是否配置了数量检查
    pub fn has_count_fn(&self) -> bool {
        self.count_fn.is_some()
    }

    /// 执行初始化
    pub async fn init(&self) -> Result<()> {
        // 1. 执行初始化脚本（如果有）
        if let Some(script) = &self.init_sh {
            self.run_init_script(script)?;
        }

        // 2. 执行异步初始化方法（如果有）
        if let Some(init_fn) = &self.init_fn {
            init_fn().await?;
        }

        Ok(())
    }

    /// 执行 sink 级就绪检查
    pub async fn wait_ready(&self) -> Result<()> {
        if let Some(wait_ready_fn) = &self.wait_ready_fn {
            wait_ready_fn(self.params.clone()).await?;
        }

        Ok(())
    }

    /// 获取当前数量
    pub async fn count(&self) -> Result<i64> {
        let count_fn = self
            .count_fn
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("当前 SinkInfo 未配置 count_fn"))?;
        count_fn(self.params.clone()).await
    }

    /// 运行初始化脚本
    fn run_init_script(&self, script_path: &str) -> Result<()> {
        use std::process::Command;

        let output = Command::new("bash").arg(script_path).output()?;

        if !output.status.success() {
            anyhow::bail!(
                "初始化脚本执行失败: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }

        Ok(())
    }
}
