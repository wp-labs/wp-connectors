use anyhow::{Context, Result};
use async_trait::async_trait;
use std::path::Path;
use tokio::process::Command;
use tokio::time::{Duration, sleep};

/// 组件管理接口
#[allow(dead_code)]
#[async_trait]
pub trait ComponentTool {
    /// 拉取依赖（镜像或安装脚本）
    async fn pull_dependencies(&self) -> Result<()>;

    /// 启动组件
    async fn up(&self) -> Result<()>;

    /// 停止组件
    async fn down(&self) -> Result<()>;

    /// 等待组件完成启动
    async fn wait_started(&self) -> Result<()>;

    /// 重启组件
    async fn restart(&self) -> Result<()>;

    /// 完整启动流程：拉取依赖 + 启动 + 等待启动完成
    async fn setup_and_up(&self) -> Result<()> {
        self.pull_dependencies().await?;
        self.up().await?;
        self.wait_started().await?;
        Ok(())
    }
}

/// Docker Compose 操作工具
pub struct DockerComposeTool {
    compose_file: String,
}

#[allow(dead_code)]
impl DockerComposeTool {
    async fn services(&self, args: &[&str], error_context: &str) -> Result<Vec<String>> {
        let output = Command::new("docker")
            .args(["compose", "-f", &self.compose_file, "ps"])
            .args(args)
            .output()
            .await
            .context(error_context.to_string())?;

        if !output.status.success() {
            anyhow::bail!(
                "{}: {}",
                error_context,
                String::from_utf8_lossy(&output.stderr)
            );
        }

        Ok(String::from_utf8_lossy(&output.stdout)
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .map(str::to_string)
            .collect())
    }

    /// 创建新的 Docker Compose 工具实例
    pub fn new<P: AsRef<Path>>(compose_file: P) -> Result<Self> {
        let path = compose_file.as_ref();
        if !path.exists() {
            anyhow::bail!("Docker Compose 文件不存在: {}", path.display());
        }
        Ok(Self {
            compose_file: path.to_string_lossy().to_string(),
        })
    }

    /// 拉取镜像
    pub async fn pull(&self) -> Result<()> {
        println!("==> 拉取镜像: {}", self.compose_file);
        let output = Command::new("docker")
            .args(["compose", "-f", &self.compose_file, "pull"])
            .output()
            .await
            .context("执行 docker compose pull 失败")?;

        if !output.status.success() {
            anyhow::bail!("拉取镜像失败: {}", String::from_utf8_lossy(&output.stderr));
        }
        println!("✓ 镜像拉取完成");
        Ok(())
    }

    /// 启动服务
    pub async fn up(&self) -> Result<()> {
        println!("==> 启动服务: {}", self.compose_file);
        let output = Command::new("docker")
            .args(["compose", "-f", &self.compose_file, "up", "-d"])
            .output()
            .await
            .context("执行 docker compose up 失败")?;

        if !output.status.success() {
            anyhow::bail!("启动服务失败: {}", String::from_utf8_lossy(&output.stderr));
        }
        println!("✓ 服务已启动");
        Ok(())
    }

    /// 停止服务
    pub async fn down(&self) -> Result<()> {
        println!("==> 停止服务: {}", self.compose_file);
        let output = Command::new("docker")
            .args(["compose", "-f", &self.compose_file, "down", "-v"])
            .output()
            .await
            .context("执行 docker compose down 失败")?;

        if !output.status.success() {
            anyhow::bail!("停止服务失败: {}", String::from_utf8_lossy(&output.stderr));
        }
        println!("✓ 服务已停止");
        Ok(())
    }

    /// 查看服务状态
    pub async fn ps(&self) -> Result<String> {
        let output = Command::new("docker")
            .args(["compose", "-f", &self.compose_file, "ps"])
            .output()
            .await
            .context("执行 docker compose ps 失败")?;

        if !output.status.success() {
            anyhow::bail!("查看状态失败: {}", String::from_utf8_lossy(&output.stderr));
        }
        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    }

    /// 等待所有服务进入运行状态
    pub async fn wait_started(&self) -> Result<()> {
        let expected = self
            .services(&["--services"], "获取 docker compose 服务列表失败")
            .await?;

        for attempt in 1..=10 {
            let running = self
                .services(
                    &["--services", "--status", "running"],
                    "获取 docker compose 运行状态失败",
                )
                .await?;

            if expected.iter().all(|service| running.contains(service)) {
                println!("✓ Docker Compose 服务已就绪，第 {} 次检查成功", attempt);
                return Ok(());
            }

            sleep(Duration::from_secs(2)).await;
        }

        let status = self.ps().await?;
        anyhow::bail!("等待 Docker Compose 服务就绪超时:\n{}", status)
    }

    /// 重启服务
    pub async fn restart(&self) -> Result<()> {
        println!("==> 重启 Docker Compose 服务: {}", self.compose_file);
        let output = Command::new("docker")
            .args(["compose", "-f", &self.compose_file, "restart"])
            .output()
            .await
            .context("执行 docker compose restart 失败")?;

        if !output.status.success() {
            anyhow::bail!("重启服务失败: {}", String::from_utf8_lossy(&output.stderr));
        }

        let status = self.ps().await?;
        println!("==> 服务状态:\n{}", status);
        println!("✓ 服务已重启");
        Ok(())
    }
}

#[async_trait]
impl ComponentTool for DockerComposeTool {
    async fn pull_dependencies(&self) -> Result<()> {
        self.pull().await
    }

    async fn up(&self) -> Result<()> {
        DockerComposeTool::up(self).await
    }

    async fn down(&self) -> Result<()> {
        DockerComposeTool::down(self).await
    }

    async fn wait_started(&self) -> Result<()> {
        DockerComposeTool::wait_started(self).await
    }

    async fn restart(&self) -> Result<()> {
        DockerComposeTool::restart(self).await
    }
}

/// Shell 脚本操作工具
#[allow(dead_code)]
pub enum ShellScriptRestart<P: AsRef<Path>> {
    Default,
    Script(P),
    NoRestart,
}

/// Shell 脚本操作工具
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
    /// 创建新的 Shell 脚本工具实例
    pub fn new<P: AsRef<Path>>(start_sh: P, stop_sh: P) -> Result<Self> {
        Self::new_with_options(
            start_sh,
            stop_sh,
            None::<P>,
            None::<P>,
            ShellScriptRestart::Default,
        )
    }

    /// 创建带就绪检查脚本的 Shell 脚本工具实例
    pub fn new_with_ready<P: AsRef<Path>>(
        start_sh: P,
        stop_sh: P,
        ready_sh: Option<P>,
    ) -> Result<Self> {
        Self::new_with_options(
            start_sh,
            stop_sh,
            None::<P>,
            ready_sh,
            ShellScriptRestart::Default,
        )
    }

    /// 创建带完整可选脚本配置的 Shell 脚本工具实例
    pub fn new_with_options<P: AsRef<Path>>(
        start_sh: P,
        stop_sh: P,
        install_deps_sh: Option<P>,
        ready_sh: Option<P>,
        restart: ShellScriptRestart<P>,
    ) -> Result<Self> {
        let start = start_sh.as_ref();
        let stop = stop_sh.as_ref();

        if !start.exists() {
            anyhow::bail!("启动脚本不存在: {}", start.display());
        }
        if !stop.exists() {
            anyhow::bail!("停止脚本不存在: {}", stop.display());
        }

        let install_deps_sh = match install_deps_sh {
            Some(install) => {
                let install = install.as_ref();
                if !install.exists() {
                    anyhow::bail!("安装依赖脚本不存在: {}", install.display());
                }
                Some(install.to_string_lossy().to_string())
            }
            None => None,
        };

        let ready_sh = match ready_sh {
            Some(ready) => {
                let ready = ready.as_ref();
                if !ready.exists() {
                    anyhow::bail!("就绪检查脚本不存在: {}", ready.display());
                }
                Some(ready.to_string_lossy().to_string())
            }
            None => None,
        };

        let restart = match restart {
            ShellScriptRestart::Default => ShellScriptRestart::Default,
            ShellScriptRestart::Script(restart) => {
                let restart = restart.as_ref();
                if !restart.exists() {
                    anyhow::bail!("重启脚本不存在: {}", restart.display());
                }
                ShellScriptRestart::Script(restart.to_string_lossy().to_string())
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

    /// 安装依赖
    pub async fn install(&self) -> Result<()> {
        if let Some(install_deps_sh) = &self.install_deps_sh {
            println!("==> 安装依赖: {}", install_deps_sh);
            self.run_script(install_deps_sh).await?;
            println!("✓ 依赖安装完成");
        } else {
            println!("==> 未提供安装依赖脚本，跳过此步骤");
        }
        Ok(())
    }

    /// 启动服务
    pub async fn start(&self) -> Result<()> {
        println!("==> 启动服务: {}", self.start_sh);
        self.run_script(&self.start_sh).await?;
        println!("✓ 服务已启动");
        Ok(())
    }

    /// 停止服务
    pub async fn stop(&self) -> Result<()> {
        println!("==> 停止服务: {}", self.stop_sh);
        self.run_script(&self.stop_sh).await?;
        println!("✓ 服务已停止");
        Ok(())
    }

    /// 重启服务
    pub async fn restart(&self) -> Result<()> {
        match &self.restart {
            ShellScriptRestart::Script(restart_sh) => {
                println!("==> 重启服务: {}", restart_sh);
                self.run_script(restart_sh).await?;
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

    /// 等待服务完成启动
    pub async fn wait_started(&self) -> Result<()> {
        match &self.ready_sh {
            Some(ready_sh) => {
                println!("==> 检查服务就绪: {}", ready_sh);
                self.run_script(ready_sh).await?;
                println!("✓ 服务已就绪");
                Ok(())
            }
            None => {
                println!("==> 未提供就绪检查脚本，默认视为已就绪");
                Ok(())
            }
        }
    }

    /// 执行脚本
    async fn run_script(&self, script_path: &str) -> Result<()> {
        let output = Command::new("bash")
            .arg(script_path)
            .output()
            .await
            .context(format!("执行脚本失败: {}", script_path))?;

        if !output.status.success() {
            anyhow::bail!(
                "脚本执行失败: {}\n{}",
                script_path,
                String::from_utf8_lossy(&output.stderr)
            );
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        if !stdout.is_empty() {
            println!("{}", stdout);
        }

        Ok(())
    }
}

#[async_trait]
impl ComponentTool for ShellScriptTool {
    async fn pull_dependencies(&self) -> Result<()> {
        self.install().await
    }

    async fn up(&self) -> Result<()> {
        ShellScriptTool::start(self).await
    }

    async fn down(&self) -> Result<()> {
        ShellScriptTool::stop(self).await
    }

    async fn wait_started(&self) -> Result<()> {
        ShellScriptTool::wait_started(self).await
    }

    async fn restart(&self) -> Result<()> {
        ShellScriptTool::restart(self).await
    }
}
