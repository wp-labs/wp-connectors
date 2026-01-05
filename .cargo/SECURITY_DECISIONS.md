# 安全决策记录

本文档记录了项目中已知的安全漏洞及其处理决策。

## 当前忽略的漏洞

### RUSTSEC-2023-0071 - RSA Marvin Attack

**状态**: 临时忽略，等待上游修复
**记录日期**: 2026-01-03
**重新评估日期**: 2026-07-03（6个月后）
**风险级别**: 中等

#### 漏洞详情
- **受影响包**: `rsa v0.9.9`
- **问题**: 通过定时侧信道攻击可能恢复私钥（Marvin Attack）
- **CVE**: CVE-2023-49092
- **参考**: https://github.com/RustCrypto/RSA/issues/19#issuecomment-1822995643

#### 依赖路径
```
wp-connectors → sea-orm → sqlx → sqlx-mysql → rsa v0.9.9
```

#### 风险评估
- ✅ 这是**传递依赖**，项目代码未直接使用 RSA 功能
- ✅ 仅在 MySQL `caching_sha2_password`/`sha256_password` 认证时使用
- ✅ 攻击需要观察网络定时信息，内网环境风险较低
- ⚠️ 如果在公网环境使用 MySQL 连接，风险较高

#### 决策理由
1. 当前 `rsa` 稳定版本（0.9.x）暂无补丁
2. 新版本 `rsa 0.10.0-rc` 仍为候选版本，不适合生产环境
3. 上游 `sqlx` 尚未迁移到 rsa 0.10.x
4. 项目 MySQL 连接主要在内网环境，攻击面有限

#### 缓解措施
- [x] 在 `.cargo/audit.toml` 中配置忽略此漏洞
- [x] 文档化决策过程和风险评估
- [ ] 定期检查上游更新（sqlx、sea-orm、rsa）
- [ ] 建议生产环境使用 `mysql_native_password` 认证插件
- [ ] 在 2026-07-03 前重新评估此决策

#### 跟踪上游进度
- RustCrypto/RSA: https://github.com/RustCrypto/RSA/issues/19
- launchbadge/sqlx: https://github.com/launchbadge/sqlx
- SeaQL/sea-orm: https://github.com/SeaQL/sea-orm

#### 重新评估清单（2026-07-03）
在重新评估日期时，检查以下内容：
- [ ] `rsa` crate 是否发布了 0.10.0 正式版？
- [ ] `sqlx` 是否已升级到使用 rsa 0.10.x？
- [ ] `sea-orm` 是否已更新 sqlx 依赖？
- [ ] 项目的 MySQL 使用场景是否有变化？
- [ ] 是否可以安全更新依赖？

如果以上任一项为"是"，应立即更新依赖。如果全部为"否"，需要决定：
1. 延长忽略期限（需重新评估风险）
2. 寻找替代方案（如切换到其他数据库客户端）
3. 自行维护补丁版本（不推荐）

---

## 设置日历提醒

### macOS/iOS
```bash
# 创建一个提醒事项
osascript -e 'tell application "Reminders" to make new reminder with properties {name:"重新评估 RUSTSEC-2023-0071 安全漏洞", due date:date "2026-07-03"}'
```

### Linux (使用 at 命令)
```bash
echo 'notify-send "安全提醒" "需要重新评估 wp-connectors RUSTSEC-2023-0071 漏洞"' | at 09:00 2026-07-03
```

### GitHub Issue
可以创建一个 GitHub Issue 并设置 milestone 为 2026-07-03，标题为：
```
[安全] 重新评估 RUSTSEC-2023-0071 (rsa Marvin Attack)
```
