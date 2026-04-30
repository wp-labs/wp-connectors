# DMDB Sink 使用配置说明

## 1. 功能概览

- 通过 ODBC 连接达梦数据库。
- 按 `columns` 声明的列顺序生成 `INSERT INTO ... VALUES ...` 批量写入 SQL。
- 支持按 `batch_size` 分批执行。
- 一次 `sink_records` 调用会作为一个事务提交；任意分片失败时，已执行分片会回滚，后续分片不再执行。
- 缺失字段写入 `NULL` 并记录告警。
- `DataType::Ignore` 字段不会参与写入。

## 2. 使用前准备

### 2.1 准备 ODBC 环境

- 使用 `dsn` 或 `endpoint` 模式前，宿主机必须已经安装达梦 ODBC 驱动。
- 使用 `dsn` 模式前，系统中必须先配置好对应的数据源名称。

- 以Ubuntu为例，安装ODBC驱动系统依赖：

```bash
apt-get update
apt-get install -y unixodbc unixodbc-dev odbcinst
```

验证：

```bash
which isql
isql --version
odbcinst -j
```

预期结果：

```text
unixODBC 2.x.x
DRIVERS............: /etc/odbcinst.ini
SYSTEM DATA SOURCES: /etc/odbc.ini
...
```

- 达梦 ODBC 驱动安装
  - Windows 版：<https://dn.navicat.com/drivers/dameng_odbc_win.zip>
  - Linux  版：<https://dn.navicat.com/drivers/dameng_odbc_linux.tar.gz>
以下载到root目录为例：

```bash
cd /root
tar -xvf dameng_odbc_linux.tar.gz
cd dameng_odbc_linux
ls

预期：
bin  install_odbc.sh  libcrypto.so  libdmdpi.so  libdmfldr.so  libdodbc.a  libdodbc.so  libssl.so
```

配置动态库路径：

```bash
echo "/root/dameng_odbc_linux" > /etc/ld.so.conf.d/dameng.conf
ldconfig
```

验证：

```bash
ldconfig -p | grep libdodbc

预期结果：
```text
libdodbc.so.2 => /root/dameng_odbc_linux/libdodbc.so
```

### 2.2 配置 ODBC 数据源

### 2.1 编辑 `/etc/odbcinst.ini`

如果系统中已经存在 `/etc/odbcinst.ini`，直接追加下面内容即可：

```ini
[DM8 ODBC DRIVER]
Description = DM8 ODBC DRIVER for DM8
Driver = /root/dameng_odbc_linux/libdodbc.so
FileUsage = 1
```

如果文件不存在，可以直接创建该文件并写入上述内容。

### 2.2 配置完成后验证驱动是否注册成功

执行：

```bash
odbcinst -q -d
odbcinst -q -d -n "DM8 ODBC DRIVER"
```

预期结果：

```text
[DM8 ODBC DRIVER]
```

以及类似：

```ini
[DM8 ODBC DRIVER]
Description=DM8 ODBC DRIVER for DM8
Driver=/root/dameng_odbc_linux/libdodbc.so
FileUsage=1
```

## 3. 数据库连接

在wparse运行的机器运行以下命令，替换为实际的数据库地址、端口、用户名和密码：

```bash
isql -v -k "Driver={DM8 ODBC DRIVER};Server=localhost;Port=5236;UID=SYSDBA;PWD=password"
```

### 2.3 准备目标表

- 目标表需要预先存在。
- `table` 和 `columns` 必须与目标表实际结构匹配, 包括列名、列类型等。

## 3. 连接方式

### 3.1 优先级

当前实现按以下优先级选择连接方式：

```text
connection_string > endpoint > dsn
```

这意味着：

- 只要配置了 `connection_string`，就优先使用它建连，`dsn`、`endpoint`、`driver`、`username`、`password` 即使同时出现也不会参与建连。
- 未配置 `connection_string` 且配置了 `endpoint` 时，使用 `endpoint + driver + username + password` 自动拼接 ODBC 连接串。
- 只有在前两者都未配置时，才会进入 `dsn` 模式，使用 `dsn + username + password` 建连。

### 3.2 `connection_string` 模式

适合已经有完整 ODBC 连接串的场景。

- 必填：`connection_string`、`table`、`columns`
- 可选：`schema`、`batch_size`、`connect_timeout_secs`、`query_timeout_secs`
- 不要求额外提供：`dsn`、`endpoint`、`driver`、`username`、`password`

示例：

```toml
[[sinks]]
name = "dmdb_events"
kind = "dmdb"

[sinks.params]
connection_string = "Driver={DM8 ODBC DRIVER};SERVER=127.0.0.1;TCP_PORT=5236;UID=SYSDBA;PWD=${DMDB_PASSWORD};"
schema = "WP_DATA"
table = "EVENTS"
columns = ["event_id", "event_time", "source", "payload"]
batch_size = 2000
query_timeout_secs = 15
```

### 3.3 `endpoint` 模式

适合不想预先配置 DSN，而是直接通过地址建连的场景。

- 必填：`endpoint`、`driver`、`username`、`password`、`table`、`columns`
- 可选：`schema`、`batch_size`、`connect_timeout_secs`、`query_timeout_secs`
- `endpoint` 必须是 `host:port` 格式，且 `port` 必须能解析为合法 `u16`

示例：

```toml
[[sinks]]
name = "dmdb_events"
kind = "dmdb"

[sinks.params]
endpoint = "127.0.0.1:5236"
driver = "DM8 ODBC DRIVER"
username = "SYSDBA"
password = "${DMDB_PASSWORD}"
schema = "WP_DATA"
table = "EVENTS"
columns = ["event_id", "event_time", "source", "payload"]
batch_size = 2000
connect_timeout_secs = 8
query_timeout_secs = 15
```

### 3.4 `dsn` 模式

适合宿主机已经配置好 ODBC DSN，且没有传入 `connection_string` 或 `endpoint` 的场景。

- 必填：`dsn`、`username`、`password`、`table`、`columns`
- 可选：`schema`、`batch_size`、`connect_timeout_secs`、`query_timeout_secs`
- 不要求额外提供：`endpoint`、`driver`

示例：

```toml
[[sinks]]
name = "dmdb_events"
kind = "dmdb"

[sinks.params]
dsn = "DM8_LOCAL"
username = "SYSDBA"
password = "${DMDB_PASSWORD}"
schema = "WP_DATA"
table = "EVENTS"
columns = ["event_id", "event_time", "source", "payload"]
batch_size = 2000
connect_timeout_secs = 8
query_timeout_secs = 15
```

## 4. 参数说明

| 参数                     | 类型         | 是否必填                     | 适用模式 | 说明                                                                           |
| ---------------------- | ---------- | ------------------------ | ---- | ---------------------------------------------------------------------------- |
| `connection_string`    | `string`   | `connection_string` 模式必填 | 全部   | 完整 ODBC 连接串，优先级最高。                                                           |
| `endpoint`             | `string`   | `endpoint` 模式必填          | 全部   | 达梦地址，格式必须为 `host:port`；优先级高于 `dsn`。                                          |
| `dsn`                  | `string`   | `dsn` 模式必填               | 全部   | ODBC 数据源名称；仅在未配置 `connection_string` 和 `endpoint` 时使用。                       |
| `driver`               | `string`   | `endpoint` 模式必填          | 全部   | 达梦 ODBC 驱动名称，例如 `DM8 ODBC DRIVER`。                                           |
| `username`             | `string`   | `dsn`、`endpoint` 模式必填    | 全部   | 达梦用户名。`connection_string` 模式不要求单独传入。                                         |
| `password`             | `string`   | `dsn`、`endpoint` 模式必填    | 全部   | 达梦密码。`connection_string` 模式不要求单独传入。                                          |
| `database`             | `string`   | 否                        | 全部   | 当前实现接受该字段，但不会参与连接串拼装，也不会参与 SQL 路由，暂时等价于保留字段。                                 |
| `schema`               | `string`   | 否                        | 全部   | 用于拼接目标表的限定名；在 `endpoint` 模式下还会被附加进生成的连接串。                                    |
| `table`                | `string`   | 是                        | 全部   | 目标表名，不能为空。                                                                   |
| `columns`              | `string[]` | 是                        | 全部   | 目标列名列表。必须是字符串数组，且每一项都不能为空字符串。                                                |
| `batch_size`           | `integer`  | 否                        | 全部   | 单次 SQL 最多写入的记录数，必须大于 `0`。未配置时 sink 内部回落为 `1024`。                             |
| `connect_timeout_secs` | `integer`  | 否                        | 全部   | ODBC 登录超时秒数，必须大于 `0`。配置后会传给 `ConnectionOptions.login_timeout_sec`；未配置时不显式设置。 |
| `query_timeout_secs`   | `integer`  | 否                        | 全部   | 每批 SQL 的执行超时秒数，必须大于 `0`。未配置时不显式设置。                                           |
