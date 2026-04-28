# HTTP Source

`HttpSource` 是一个基于 `actix-web` 的接收型 Source。

它通过 HTTP `POST` 请求接收外部数据，请求被解析成功后投递到内部队列，再由 `DataSource::receive()` / `try_receive()` 消费并包装成 `SourceEvent`。

## 功能

- 监听地址固定为 `0.0.0.0`
- 使用 `port + path` 唯一确定一个 source
- 只接收 `POST` 请求
- 支持 `json` 与 `ndjson`
- 支持通过 `Content-Encoding: gzip` 或请求参数 `compression=gzip` 解压请求体
- 解析成功时返回 `200 OK` 和响应体 `OK`
- 解析失败时返回错误状态码和失败原因文本

## 请求协议

### 1. 格式选择

支持以下两种输入格式：

- `json`
- `ndjson`

可以通过请求参数 `fmt` 或者请求头`Content-Type`指定输入格式：
`fmt` 参数优先级高于 Content-Type，且两者都不指定时默认使用 `json`。

`Content-Type` 映射规则：

- `application/json` => `json`
- `application/x-ndjson` => `ndjson`
- `application/ndjson` => `ndjson`

### 2. 压缩选择

支持以下压缩参数：

- `none`:不压缩，直接解析请求体
- `gzip`:使用 gzip 解压请求体后再解析

可以通过请求参数 `compression` 或者请求头`Content-Encoding`指定压缩方式：

## 解析规则

### `json`

- 先整体解析为一个 JSON 值
- 如果顶层是数组，则逐元素处理
- 如果顶层不是数组，则自动包装成单元素数组
- 每个元素最终被重新序列化成一行 JSON 字节串

示例：

请求体：

```json
[{"a":1},{"b":2}]
```

进入 source 后会生成两条 payload：

```json
{"a":1}
{"b":2}
```

### `ndjson`

- 按行切分请求体
- 忽略空行
- 每一行都必须是合法 JSON
- 每一行解析成功后会被规范化成一行 JSON 字节串

示例：

```text
{"a":1}
{"b":2}
```

如果某一行不是合法 JSON，请求会直接失败。

## 运行时设计

`HttpSource` 采用“全局 HTTP 运行时 + 每个 source 一个队列”的结构。

### 全局运行时

- 使用 `OnceLock<Arc<HttpSourceRuntime>>` 保存全局运行时
- 第一个 source 注册某个端口时，启动对应的 `actix-web` server
- 同一端口的多个 path 复用同一个 server

### 端口级运行时

每个端口对应一个 `PortRuntime`：

- 维护当前端口下的 `path -> RouteTarget` 映射
- 所有请求先进入 `default_service`
- 再根据请求 path 找到目标 source 的投递通道

### Source 数据流

处理流程如下：

```text
HTTP Request
  -> actix-web handler
  -> 解析 fmt / compression
  -> 解压 body
  -> 解析成 Vec<Bytes>
  -> 通过 channel 投递给 HttpSource
  -> receive()/try_receive() 读取并组装 SourceEvent
```

## 配置示例

```toml
[[sources]]
name = "http_ingest"
connector_id = "http_src"

params = { port=18080, path="/ingest" }
```

示例请求：

```bash
curl -X POST "http://127.0.0.1:18080/ingest?fmt=ndjson" \
  -H "Content-Type: application/x-ndjson" \
  --data-binary $'{"a":1}\n{"b":2}\n'
```

gzip 示例：

```bash
gzip -c payload.ndjson | curl -X POST "http://127.0.0.1:18080/ingest?fmt=ndjson" \
  -H "Content-Encoding: gzip" \
  --data-binary @-
```

## 测试

当前已经覆盖这些行为：

- `json` 数组拆分
- `ndjson` 逐行切分
- `ndjson` 每行 JSON 合法性校验
- `gzip` 解压
- 请求参数优先级高于请求头
- 端到端请求进入 `HttpSource`
- 非法输入返回失败原因

可用命令：

```bash
cargo test --features http
```
