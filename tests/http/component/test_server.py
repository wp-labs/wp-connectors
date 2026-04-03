#!/usr/bin/env python3
"""
HTTP Sink Test Server for wp-connectors
支持多种数据格式的接收、解析和统计

接口:
- POST /ingest/{format}        - 接收指定格式的数据
- POST /auth/ingest/{format}   - 需要认证的数据接收接口 (用户名/密码: root/root)
- GET  /count                  - 查看所有格式的统计
- GET  /details/{format}       - 查看指定格式的最后3条数据
- GET  /                       - 健康检查
"""

from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import sys
import csv
import io
import re
import shlex
import base64
import gzip
from datetime import datetime
from collections import defaultdict

# 全局数据存储
data_storage = {
    "json": {"count": 0, "records": []},
    "ndjson": {"count": 0, "records": []},
    "csv": {"count": 0, "records": []},
    "kv": {"count": 0, "records": []},
    "raw": {"count": 0, "records": []},
    "proto-text": {"count": 0, "records": []},
}

# 认证配置
AUTH_USERNAME = "root"
AUTH_PASSWORD = "root"


class FormatParser:
    """数据格式解析器"""

    @staticmethod
    def _iter_non_empty_lines(body_str):
        for line in body_str.strip().split("\n"):
            stripped = line.strip()
            if stripped:
                yield stripped

    @staticmethod
    def _split_kv_tokens(line):
        lexer = shlex.shlex(line, posix=True)
        lexer.whitespace_split = True
        lexer.commenters = ""
        return list(lexer)

    @staticmethod
    def _split_proto_records(body_str):
        records = []
        current = []

        for raw_line in body_str.split("\n"):
            line = raw_line.strip()
            if not line:
                if current:
                    records.append(" ".join(current).strip())
                    current = []
                continue
            current.append(line)

        if current:
            records.append(" ".join(current).strip())

        return records

    @staticmethod
    def _parse_proto_record(record_str):
        record_str = record_str.strip()
        if record_str.startswith("{") and record_str.endswith("}"):
            record_str = record_str[1:-1].strip()

        lexer = shlex.shlex(record_str, posix=True)
        lexer.whitespace_split = True
        lexer.commenters = ""
        tokens = list(lexer)

        if not tokens:
            raise ValueError("Proto-Text 记录为空")

        record = {}
        idx = 0
        while idx < len(tokens):
            key_token = tokens[idx]
            if not key_token.endswith(":"):
                raise ValueError(f"Proto-Text 字段格式错误: {key_token}")

            key = key_token[:-1]
            idx += 1 
            if idx >= len(tokens):
                raise ValueError(f"Proto-Text 字段缺少值: {key}")

            value = tokens[idx]
            record[key] = value
            idx += 1

        return record

    @staticmethod
    def parse_json(body_str):
        """解析 JSON 格式"""
        data = json.loads(body_str)
        if isinstance(data, list):
            return data
        return [data]

    @staticmethod
    def parse_ndjson(body_str):
        """解析 NDJSON 格式"""
        records = []
        for i, line in enumerate(FormatParser._iter_non_empty_lines(body_str), 1):
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError as e:
                raise ValueError(f"NDJSON 解析失败 (第 {i} 行): {e}")
        if not records:
            raise ValueError("NDJSON 数据为空")
        return records

    @staticmethod
    def parse_csv(body_str):
        """解析 CSV 格式"""
        records = []
        reader = csv.DictReader(io.StringIO(body_str))
        for i, row in enumerate(reader, 1):
            if row:
                records.append(row)
        if not records:
            raise ValueError("CSV 数据为空或格式错误")
        return records

    @staticmethod
    def parse_kv(body_str):
        """解析 KV 格式 (key=value key2=value2)"""
        records = []
        for line_num, line in enumerate(
            FormatParser._iter_non_empty_lines(body_str), 1
        ):
            record = {}
            tokens = FormatParser._split_kv_tokens(line)
            if not tokens:
                continue

            for token in tokens:
                if "=" not in token:
                    raise ValueError(
                        f"KV 格式解析失败 (第 {line_num} 行): 无效 token {token}"
                    )
                key, value = token.split("=", 1)
                if not key:
                    raise ValueError(f"KV 格式解析失败 (第 {line_num} 行): 空 key")
                record[key] = value

            if not record:
                raise ValueError(
                    f"KV 格式解析失败 (第 {line_num} 行): 未找到有效的 key=value 对"
                )
            records.append(record)
        if not records:
            raise ValueError("KV 数据为空")
        return records

    @staticmethod
    def parse_proto_text(body_str):
        """解析 Proto-Text 格式"""
        records = []

        for idx, record_str in enumerate(
            FormatParser._split_proto_records(body_str), 1
        ):
            if record_str.startswith("#"):
                continue
            try:
                records.append(FormatParser._parse_proto_record(record_str))
            except ValueError as e:
                raise ValueError(f"Proto-Text 解析失败 (第 {idx} 条): {e}")

        if not records:
            raise ValueError("Proto-Text 格式解析失败: 未找到有效数据")
        return records

    @staticmethod
    def parse_raw(body_str):
        """解析 RAW 格式"""
        if not body_str:
            raise ValueError("RAW 数据为空")

        records = []
        for line_num, line in enumerate(body_str.strip().split("\n"), 1):
            if line.strip():
                records.append(
                    {"raw": line, "length": len(line), "line_number": line_num}
                )

        if not records:
            raise ValueError("RAW 数据为空或无有效行")
        return records


class HTTPSinkHandler(BaseHTTPRequestHandler):
    """HTTP Sink 请求处理器"""

    def check_auth(self):
        """检查 HTTP Basic 认证"""
        auth_header = self.headers.get("Authorization")
        if not auth_header:
            return False

        try:
            # 解析 Basic Auth
            auth_type, auth_string = auth_header.split(" ", 1)
            if auth_type.lower() != "basic":
                return False

            # Base64 解码
            decoded = base64.b64decode(auth_string).decode("utf-8")
            username, password = decoded.split(":", 1)

            return username == AUTH_USERNAME and password == AUTH_PASSWORD
        except:
            return False

    def send_auth_required(self):
        """发送 401 认证要求响应"""
        self.send_response(401)
        self.send_header("WWW-Authenticate", 'Basic realm="HTTP Sink Auth"')
        self.send_header("Content-Type", "application/json")
        self.end_headers()

        response = {
            "status": "error",
            "message": "Authentication required",
            "hint": "Use username: root, password: root",
        }
        self.wfile.write(json.dumps(response, indent=2).encode("utf-8"))

    def send_json_response(self, status_code, data):
        """发送 JSON 响应"""
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.end_headers()
        self.wfile.write(json.dumps(data, indent=2, ensure_ascii=False).encode("utf-8"))

    def store_data(self, fmt, records):
        """存储接收到的数据"""
        global data_storage

        data_storage[fmt]["count"] += len(records)
        data_storage[fmt]["records"].extend(records)

        # 只保留最后 100 条记录
        if len(data_storage[fmt]["records"]) > 100:
            data_storage[fmt]["records"] = data_storage[fmt]["records"][-100:]

    def handle_ingest(self, fmt, require_auth=False, is_gzip=False):
        """处理数据接收请求"""
        # 检查认证
        if require_auth and not self.check_auth():
            self.send_auth_required()
            return

        # 检查格式是否支持
        if fmt not in data_storage:
            self.send_json_response(
                400,
                {
                    "status": "error",
                    "message": f"不支持的格式: {fmt}",
                    "supported_formats": list(data_storage.keys()),
                },
            )
            return

        # 读取请求体
        content_length = int(self.headers.get("Content-Length", 0))
        if content_length == 0:
            self.send_json_response(400, {"status": "error", "message": "请求体为空"})
            return

        body = self.rfile.read(content_length)

        # 输出到控制台
        print(f"\n{'=' * 70}")
        print(f"📥 接收数据 - {fmt.upper()} 格式 {'(GZIP 压缩)' if is_gzip else ''}")
        print(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}")
        print(f"路径: {self.path}")
        print(f"方法: {self.command}")
        print(f"客户端: {self.client_address[0]}:{self.client_address[1]}")
        print(f"认证: {'是' if require_auth else '否'}")
        print(f"压缩: {'是' if is_gzip else '否'}")
        print(f"压缩大小: {content_length} bytes")
        print(f"\n📋 请求头:")
        for header, value in self.headers.items():
            print(f"   {header}: {value}")
        print(f"-" * 70)

        try:
            # 如果是 gzip 压缩,先解压
            if is_gzip:
                try:
                    decompressed = gzip.decompress(body)
                    print(f"🗜️  GZIP 解压:")
                    print(f"   压缩前: {content_length} bytes")
                    print(f"   解压后: {len(decompressed)} bytes")
                    print(
                        f"   压缩率: {(1 - content_length / len(decompressed)) * 100:.1f}%"
                    )
                    print(f"-" * 70)
                    body_str = decompressed.decode("utf-8")
                except Exception as e:
                    print(f"❌ GZIP 解压失败: {e}")
                    print(f"{'=' * 70}\n")
                    self.send_json_response(
                        400, {"status": "error", "message": f"GZIP 解压失败: {str(e)}"}
                    )
                    return
            else:
                body_str = body.decode("utf-8")

            print(f"原始内容:")
            # 限制输出长度
            if len(body_str) > 1000:
                print(body_str[:1000] + f"\n... (还有 {len(body_str) - 1000} 字符)")
            else:
                print(body_str)
            print(f"-" * 70)

            # 根据格式解析数据
            parser_map = {
                "json": FormatParser.parse_json,
                "ndjson": FormatParser.parse_ndjson,
                "csv": FormatParser.parse_csv,
                "kv": FormatParser.parse_kv,
                "proto-text": FormatParser.parse_proto_text,
                "raw": FormatParser.parse_raw,
            }

            parser = parser_map[fmt]
            records = parser(body_str)

            print(f"✅ 解析成功!")
            print(f"记录数: {len(records)}")
            print(f"前 3 条记录:")
            for i, record in enumerate(records[:3], 1):
                print(f"  {i}. {json.dumps(record, ensure_ascii=False)}")
            if len(records) > 3:
                print(f"  ... 还有 {len(records) - 3} 条记录")

            # 存储数据
            self.store_data(fmt, records)

            print(f"{'=' * 70}\n")

            # 返回成功响应
            response_data = {
                "status": "success",
                "message": "数据接收成功",
                "format": fmt,
                "compressed": is_gzip,
                "records_received": len(records),
                "total_records": data_storage[fmt]["count"],
            }

            if is_gzip:
                response_data["compression_info"] = {
                    "compressed_size": content_length,
                    "decompressed_size": len(body_str.encode("utf-8")),
                    "compression_ratio": f"{(1 - content_length / len(body_str.encode('utf-8'))) * 100:.1f}%",
                }

            self.send_json_response(200, response_data)

        except UnicodeDecodeError as e:
            print(f"❌ 解码失败: {e}")
            print(f"{'=' * 70}\n")
            self.send_json_response(
                400, {"status": "error", "message": f"数据解码失败: {str(e)}"}
            )
        except (json.JSONDecodeError, ValueError, csv.Error) as e:
            print(f"❌ 解析失败: {e}")
            print(f"{'=' * 70}\n")
            self.send_json_response(
                400,
                {"status": "error", "message": f"{fmt.upper()} 格式解析失败: {str(e)}"},
            )
        except Exception as e:
            print(f"❌ 未知错误: {e}")
            import traceback

            traceback.print_exc()
            print(f"{'=' * 70}\n")
            self.send_json_response(
                500, {"status": "error", "message": f"服务器错误: {str(e)}"}
            )

    def do_POST(self):
        """处理 POST 请求"""
        path_parts = self.path.strip("/").split("/")

        # /ingest/{format}
        if len(path_parts) == 2 and path_parts[0] == "ingest":
            fmt = path_parts[1]
            self.handle_ingest(fmt, require_auth=False, is_gzip=False)
            return

        # /gzip/ingest/{format}
        if (
            len(path_parts) == 3
            and path_parts[0] == "gzip"
            and path_parts[1] == "ingest"
        ):
            fmt = path_parts[2]
            self.handle_ingest(fmt, require_auth=False, is_gzip=True)
            return

        # /auth/ingest/{format}
        if (
            len(path_parts) == 3
            and path_parts[0] == "auth"
            and path_parts[1] == "ingest"
        ):
            fmt = path_parts[2]
            self.handle_ingest(fmt, require_auth=True, is_gzip=False)
            return

        # 未知路径
        self.send_json_response(
            404,
            {
                "status": "error",
                "message": f"路径未找到: {self.path}",
                "hint": "使用 POST /ingest/{{format}}, POST /gzip/ingest/{{format}} 或 POST /auth/ingest/{{format}}",
            },
        )

    def do_GET(self):
        """处理 GET 请求"""
        global data_storage

        path_parts = self.path.strip("/").split("/")

        # 健康检查
        if self.path == "/" or self.path == "/health":
            self.send_json_response(
                200,
                {
                    "status": "ok",
                    "message": "HTTP Sink Test Server is running",
                    "version": "2.0",
                    "supported_formats": list(data_storage.keys()),
                    "endpoints": {
                        "POST /ingest/{format}": "接收指定格式的数据",
                        "POST /gzip/ingest/{format}": "接收 GZIP 压缩的数据",
                        "POST /auth/ingest/{format}": "需要认证的数据接收 (root/root)",
                        "GET /count": "查看所有格式的统计",
                        "GET /details/{format}": "查看指定格式的最后3条数据",
                    },
                },
            )
            return

        # /count
        if path_parts[0] == "count" and len(path_parts) == 1:
            counts = {fmt: data_storage[fmt]["count"] for fmt in data_storage}
            self.send_json_response(
                200,
                {"status": "success", "counts": counts, "total": sum(counts.values())},
            )
            return

        # /details/{format}
        if path_parts[0] == "details" and len(path_parts) == 2:
            fmt = path_parts[1]
            if fmt not in data_storage:
                self.send_json_response(
                    404,
                    {
                        "status": "error",
                        "message": f"格式未找到: {fmt}",
                        "supported_formats": list(data_storage.keys()),
                    },
                )
                return

            last_records = data_storage[fmt]["records"][-3:]
            self.send_json_response(
                200,
                {
                    "status": "success",
                    "format": fmt,
                    "total_count": data_storage[fmt]["count"],
                    "last_3_records": last_records,
                },
            )
            return

        # 未知路径
        self.send_json_response(
            404, {"status": "error", "message": f"路径未找到: {self.path}"}
        )

    def log_message(self, format, *args):
        """禁用默认日志,使用自定义日志"""
        pass


def run_server(port=18080):
    """运行 HTTP 服务器"""
    server_address = ("", port)
    httpd = HTTPServer(server_address, HTTPSinkHandler)

    print(f"🚀 HTTP Sink Test Server v2.0")
    print(f"=" * 70)
    print(f"📡 监听端口: {port}")
    print(f"🔐 认证账号: {AUTH_USERNAME} / {AUTH_PASSWORD}")
    print(f"\n📋 支持的格式:")
    for fmt in data_storage.keys():
        print(f"   - {fmt}")
    print(f"\n🌐 接口列表:")
    print(f"   POST http://localhost:{port}/ingest/{{format}}")
    print(f"   POST http://localhost:{port}/gzip/ingest/{{format}}  (GZIP 压缩)")
    print(f"   POST http://localhost:{port}/auth/ingest/{{format}}")
    print(f"   GET  http://localhost:{port}/count")
    print(f"   GET  http://localhost:{port}/details/{{format}}")
    print(f"\n💡 按 Ctrl+C 停止服务器")
    print(f"=" * 70)
    print()

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n\n🛑 服务器已停止")
        print(f"\n📊 最终统计:")
        for fmt in data_storage:
            count = data_storage[fmt]["count"]
            if count > 0:
                print(f"   {fmt}: {count} 条记录")
        sys.exit(0)


if __name__ == "__main__":
    port = 18080
    if len(sys.argv) > 1:
        port = int(sys.argv[1])

    run_server(port)
