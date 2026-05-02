use async_trait::async_trait;
use derive_more::From;
use orion_error::{OrionError, StructError};
use serde::Serialize;
use std::path::Path;
use tokio::process::Command;
use tokio::time::{Duration, sleep};
use wp_connector_api::{SinkReason, SourceReason};

// ── ToolReason ──────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Serialize, From, OrionError)]
pub enum ToolReason {
    #[from]
    #[orion_error(identity = "sys.docker", message = "docker compose operation failed")]
    Docker(String),
    #[from(skip)]
    #[orion_error(identity = "sys.script", message = "shell script execution failed")]
    Script(String),
    #[from(skip)]
    #[orion_error(identity = "sys.file_not_found", message = "file not found")]
    FileNotFound(String),
    #[from(skip)]
    #[orion_error(identity = "sys.wait_timeout", message = "wait timeout")]
    WaitTimeout(String),
}

pub type ToolResult<T> = Result<T, StructError<ToolReason>>;

fn tool_err(reason: ToolReason) -> StructError<ToolReason> {
    StructError::from(reason)
}

// ── RuntimeReason ───────────────────────────────────────────

#[derive(Debug, PartialEq, Serialize, From, OrionError)]
pub enum RuntimeReason {
    #[orion_error(transparent)]
    Tool(ToolReason),
    #[orion_error(transparent)]
    Source(SourceReason),
    #[orion_error(transparent)]
    Sink(SinkReason),
}

pub type RuntimeResult<T> = Result<T, StructError<RuntimeReason>>;

/// Bridging helpers（integration test entry point 向 anyhow 边界用）。
pub fn to_anyhow<T>(r: ToolResult<T>) -> anyhow::Result<T> {
    r.map_err(|e| anyhow::anyhow!("{e}"))
}

pub fn runtime_anyhow<T>(r: RuntimeResult<T>) -> anyhow::Result<T> {
    r.map_err(|e| anyhow::anyhow!("{e}"))
}

// ── ComponentTool（返回值改为 ToolResult）─────────────────

#[allow(dead_code)]
#[async_trait]
pub trait ComponentTool {
    async fn pull_dependencies(&self) -> ToolResult<()>;
    async fn up(&self) -> ToolResult<()>;
    async fn down(&self) -> ToolResult<()>;
    async fn wait_started(&self) -> ToolResult<()>;
    async fn restart(&self) -> ToolResult<()>;

    async fn setup_and_up(&self) -> ToolResult<()> {
        self.pull_dependencies().await?;
        self.up().await?;
        self.wait_started().await?;
        Ok(())
    }
}

// ── DockerComposeTool ───────────────────────────────────────

pub struct DockerComposeTool {
    compose_file: String,
}

impl DockerComposeTool {
    pub fn new<P: AsRef<Path>>(compose_file: P) -> ToolResult<Self> {
        let path = compose_file.as_ref();
        if !path.exists() {
            return Err(tool_err(ToolReason::FileNotFound(format!(
                "Docker Compose 文件不存在: {}",
                path.display()
            ))));
        }
        Ok(Self {
            compose_file: path.to_string_lossy().to_string(),
        })
    }

    async fn services(&self, args: &[&str], ctx: &str) -> ToolResult<Vec<String>> {
        let output = docker_cmd(args, &self.compose_file, ctx).await?;
        Ok(String::from_utf8_lossy(&output.stdout)
            .lines()
            .map(str::trim)
            .filter(|l| !l.is_empty())
            .map(str::to_string)
            .collect())
    }

    pub async fn pull(&self) -> ToolResult<()> {
        println!("==> 拉取镜像: {}", self.compose_file);
        docker_cmd(&["pull"], &self.compose_file, "docker compose pull").await?;
        println!("✓ 镜像拉取完成");
        Ok(())
    }

    pub async fn up(&self) -> ToolResult<()> {
        println!("==> 启动服务: {}", self.compose_file);
        docker_cmd(&["up", "-d"], &self.compose_file, "docker compose up").await?;
        println!("✓ 服务已启动");
        Ok(())
    }

    pub async fn down(&self) -> ToolResult<()> {
        println!("==> 停止服务: {}", self.compose_file);
        docker_cmd(&["down", "-v"], &self.compose_file, "docker compose down").await?;
        println!("✓ 服务已停止");
        Ok(())
    }

    pub async fn ps(&self) -> ToolResult<String> {
        let output = docker_cmd(&["ps"], &self.compose_file, "docker compose ps").await?;
        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    }

    pub async fn wait_started(&self) -> ToolResult<()> {
        let expected = self
            .services(&["--services"], "获取 docker compose 服务列表")
            .await?;

        for attempt in 1..=10 {
            let running = self
                .services(
                    &["--services", "--status", "running"],
                    "获取 docker compose 运行状态",
                )
                .await?;

            if expected.iter().all(|service| running.contains(service)) {
                println!("✓ Docker Compose 服务已就绪，第 {} 次检查成功", attempt);
                return Ok(());
            }
            sleep(Duration::from_secs(2)).await;
        }

        let status = self.ps().await?;
        Err(tool_err(ToolReason::WaitTimeout(format!(
            "等待 Docker Compose 服务就绪超时:\n{}",
            status
        ))))
    }

    pub async fn restart(&self) -> ToolResult<()> {
        println!("==> 重启 Docker Compose 服务: {}", self.compose_file);
        docker_cmd(&["restart"], &self.compose_file, "docker compose restart").await?;
        let status = self.ps().await?;
        println!("==> 服务状态:\n{}", status);
        println!("✓ 服务已重启");
        Ok(())
    }
}

async fn docker_cmd(args: &[&str], file: &str, label: &str) -> ToolResult<std::process::Output> {
    let output = Command::new("docker")
        .args(["compose", "-f", file])
        .args(args)
        .output()
        .await
        .map_err(|e| tool_err(ToolReason::Docker(format!("{label}: {e}"))))?;

    if !output.status.success() {
        return Err(tool_err(ToolReason::Docker(format!(
            "{label} 失败: {}",
            String::from_utf8_lossy(&output.stderr)
        ))));
    }
    Ok(output)
}

#[async_trait]
impl ComponentTool for DockerComposeTool {
    async fn pull_dependencies(&self) -> ToolResult<()> { self.pull().await }
    async fn up(&self) -> ToolResult<()> { DockerComposeTool::up(self).await }
    async fn down(&self) -> ToolResult<()> { DockerComposeTool::down(self).await }
    async fn wait_started(&self) -> ToolResult<()> { DockerComposeTool::wait_started(self).await }
    async fn restart(&self) -> ToolResult<()> { DockerComposeTool::restart(self).await }
    async fn setup_and_up(&self) -> ToolResult<()> {
        self.up().await?;
        self.wait_started().await?;
        Ok(())
    }
}

// ── ShellScriptTool ─────────────────────────────────────────

#[allow(dead_code)]
pub enum ShellScriptRestart<P: AsRef<Path>> {
    Default,
    Script(P),
    NoRestart,
}

#[allow(dead_code)]
pub struct ShellScriptTool {
    install_deps_sh: Option<String>,
    start_sh: String,
    stop_sh: String,
    ready_sh: Option<String>,
    restart: ShellScriptRestart<String>,
}

#[allow(dead_code)]
impl ShellScriptTool {
    pub fn new<P: AsRef<Path>>(start_sh: P, stop_sh: P) -> ToolResult<Self> {
        Self::new_with_options(start_sh, stop_sh, None::<P>, None::<P>, ShellScriptRestart::Default)
    }

    pub fn new_with_ready<P: AsRef<Path>>(
        start_sh: P, stop_sh: P, ready_sh: Option<P>,
    ) -> ToolResult<Self> {
        Self::new_with_options(start_sh, stop_sh, None::<P>, ready_sh, ShellScriptRestart::Default)
    }

    pub fn new_with_options<P: AsRef<Path>>(
        start_sh: P, stop_sh: P,
        install_deps_sh: Option<P>, ready_sh: Option<P>,
        restart: ShellScriptRestart<P>,
    ) -> ToolResult<Self> {
        let check = |p: &Path, label: &str| -> ToolResult<()> {
            if !p.exists() {
                Err(StructError::from(ToolReason::FileNotFound(format!(
                    "{label} 不存在: {}",
                    p.display()
                ))))
            } else {
                Ok(())
            }
        };

        let start = start_sh.as_ref();
        let stop = stop_sh.as_ref();
        check(start, "启动脚本")?;
        check(stop, "停止脚本")?;

        let install_deps_sh = install_deps_sh
            .map(|p| {
                let p = p.as_ref();
                check(p, "安装依赖脚本")?;
                Ok::<_, StructError<ToolReason>>(p.to_string_lossy().to_string())
            })
            .transpose()?;

        let ready_sh = ready_sh
            .map(|p| {
                let p = p.as_ref();
                check(p, "就绪检查脚本")?;
                Ok::<_, StructError<ToolReason>>(p.to_string_lossy().to_string())
            })
            .transpose()?;

        let restart = match restart {
            ShellScriptRestart::Default => ShellScriptRestart::Default,
            ShellScriptRestart::Script(p) => {
                let p = p.as_ref();
                check(p, "重启脚本")?;
                ShellScriptRestart::Script(p.to_string_lossy().to_string())
            }
            ShellScriptRestart::NoRestart => ShellScriptRestart::NoRestart,
        };

        Ok(Self {
            install_deps_sh,
            start_sh: start.to_string_lossy().to_string(),
            stop_sh: stop.to_string_lossy().to_string(),
            ready_sh,
            restart,
        })
    }

    async fn run_script(&self, script_path: &str) -> ToolResult<()> {
        let output = Command::new("bash")
            .arg(script_path)
            .output()
            .await
            .map_err(|e| {
                StructError::from(ToolReason::Script(format!(
                    "执行脚本失败 {}: {}",
                    script_path, e
                )))
            })?;

        if !output.status.success() {
            return Err(StructError::from(ToolReason::Script(format!(
                "脚本执行失败: {}\n{}",
                script_path,
                String::from_utf8_lossy(&output.stderr)
            ))));
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        if !stdout.is_empty() {
            println!("{}", stdout);
        }
        Ok(())
    }

    pub async fn install(&self) -> ToolResult<()> {
        if let Some(path) = &self.install_deps_sh {
            println!("==> 安装依赖: {}", path);
            self.run_script(path).await?;
            println!("✓ 依赖安装完成");
        }
        Ok(())
    }

    pub async fn start(&self) -> ToolResult<()> {
        println!("==> 启动服务: {}", self.start_sh);
        self.run_script(&self.start_sh).await?;
        println!("✓ 服务已启动");
        Ok(())
    }

    pub async fn stop(&self) -> ToolResult<()> {
        println!("==> 停止服务: {}", self.stop_sh);
        self.run_script(&self.stop_sh).await?;
        println!("✓ 服务已停止");
        Ok(())
    }

    pub async fn wait_started(&self) -> ToolResult<()> {
        if let Some(path) = &self.ready_sh {
            println!("==> 检查服务就绪: {}", path);
            self.run_script(path).await?;
            println!("✓ 服务已就绪");
        }
        Ok(())
    }

    pub async fn restart(&self) -> ToolResult<()> {
        match &self.restart {
            ShellScriptRestart::Script(path) => {
                println!("==> 重启服务: {}", path);
                self.run_script(path).await?;
            }
            ShellScriptRestart::Default => {
                println!("==> 未提供重启脚本，使用 stop + start 回退重启...");
                self.stop().await?;
                sleep(Duration::from_secs(2)).await;
                self.start().await?;
            }
            ShellScriptRestart::NoRestart => {
                println!("==> 配置为不重启，跳过重启步骤");
            }
        }
        println!("✓ 服务已重启");
        Ok(())
    }
}

#[async_trait]
impl ComponentTool for ShellScriptTool {
    async fn pull_dependencies(&self) -> ToolResult<()> { self.install().await }
    async fn up(&self) -> ToolResult<()> { ShellScriptTool::start(self).await }
    async fn down(&self) -> ToolResult<()> { ShellScriptTool::stop(self).await }
    async fn wait_started(&self) -> ToolResult<()> { ShellScriptTool::wait_started(self).await }
    async fn restart(&self) -> ToolResult<()> { ShellScriptTool::restart(self).await }
}
