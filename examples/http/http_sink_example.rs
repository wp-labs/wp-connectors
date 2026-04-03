//! HTTP Sink 示例程序
//!
//! 演示如何使用 HTTP Sink 发送数据到 HTTP/HTTPS 端点
//! 配合 test_server.py 进行完整的功能测试
//!
//! 运行方式：
//! 1. 启动测试服务器: python3 examples/http/test_server.py
//! 2. 运行示例: cargo run --example http_sink_example --features http

// 引入共享的工具函数（从上级目录）
#[path = "../common_utils.rs"]
mod common_utils;

use std::sync::Arc;
use wp_connector_api::AsyncRecordSink;
use wp_connectors::http::{HttpSink, HttpSinkConfig};
use wp_model_core::model::DataRecord;

// ========================================
// 配置常量
// ========================================

/// 测试服务器地址
const SERVER_HOST: &str = "http://localhost:18080";

/// HTTP 方法
const HTTP_METHOD: &str = "POST";

/// 请求超时时间（秒）
const TIMEOUT_SECS: u64 = 30;

/// 最大重试次数
const MAX_RETRIES: i32 = 3;

/// 测试记录数
const TEST_RECORD_COUNT: usize = 3;

/// 所有支持的格式
const ALL_FORMATS: &[&str] = &["json", "ndjson", "csv", "kv", "raw", "proto-text"];

/// 创建测试记录
fn create_test_records(count: usize, start_id: i64) -> Vec<Arc<DataRecord>> {
    (0..count)
        .map(|i| Arc::new(common_utils::create_sample_record(start_id + i as i64)))
        .collect()
}

/// 测试类别 1: 所有格式不压缩，向 /ingest/{格式} 发送数据
async fn test_category_1_basic_ingest() {
    println!("\n╔════════════════════════════════════════════════════════════════╗");
    println!("║  测试类别 1: 基础数据接收 (无压缩, 无认证)                    ║");
    println!("╚════════════════════════════════════════════════════════════════╝\n");

    for fmt in ALL_FORMATS {
        println!("📋 测试格式: {}", fmt);
        println!("   端点: {}/ingest/{}", SERVER_HOST, fmt);
        println!("   压缩: 无");
        println!("   认证: 无");

        let endpoint = format!("{}/ingest/{}", SERVER_HOST, fmt);
        let config = HttpSinkConfig::new(
            endpoint,
            Some(HTTP_METHOD.to_string()),
            None, // 无认证
            None,
            None, // 无自定义头
            Some(fmt.to_string()),
            None, // 使用默认批量大小
            Some(TIMEOUT_SECS),
            Some(MAX_RETRIES),
            Some("none".to_string()), // 无压缩
        );

        let mut sink = match HttpSink::new(config).await {
            Ok(s) => s,
            Err(e) => {
                eprintln!("   ❌ 创建 Sink 失败: {}", e);
                continue;
            }
        };

        // 创建测试记录
        let records = create_test_records(TEST_RECORD_COUNT, 1000);

        // 发送数据
        match sink.sink_records(records).await {
            Ok(_) => {
                println!("   ✅ {} 格式发送成功 ({} 条记录)", fmt, TEST_RECORD_COUNT);
            }
            Err(e) => {
                eprintln!("   ❌ {} 格式发送失败: {}", fmt, e);
            }
        }

        println!();
    }
}

/// 测试类别 2: 所有格式不压缩，向 /auth/ingest/{格式} 发送数据（需要认证）
async fn test_category_2_auth_ingest() {
    println!("\n╔════════════════════════════════════════════════════════════════╗");
    println!("║  测试类别 2: 认证数据接收 (无压缩, 需要认证)                  ║");
    println!("╚════════════════════════════════════════════════════════════════╝\n");

    for fmt in ALL_FORMATS {
        println!("📋 测试格式: {}", fmt);
        println!("   端点: {}/auth/ingest/{}", SERVER_HOST, fmt);
        println!("   压缩: 无");
        println!("   认证: root / root");

        let endpoint = format!("{}/auth/ingest/{}", SERVER_HOST, fmt);
        let config = HttpSinkConfig::new(
            endpoint,
            Some(HTTP_METHOD.to_string()),
            Some("root".to_string()), // 用户名
            Some("root".to_string()), // 密码
            None,                     // 无自定义头
            Some(fmt.to_string()),
            None, // 使用默认批量大小
            Some(TIMEOUT_SECS),
            Some(MAX_RETRIES),
            Some("none".to_string()), // 无压缩
        );

        let mut sink = match HttpSink::new(config).await {
            Ok(s) => s,
            Err(e) => {
                eprintln!("   ❌ 创建 Sink 失败: {}", e);
                continue;
            }
        };

        // 创建测试记录
        let records = create_test_records(TEST_RECORD_COUNT, 2000);

        // 发送数据
        match sink.sink_records(records).await {
            Ok(_) => {
                println!("   ✅ {} 格式发送成功 ({} 条记录)", fmt, TEST_RECORD_COUNT);
            }
            Err(e) => {
                eprintln!("   ❌ {} 格式发送失败: {}", fmt, e);
            }
        }

        println!();
    }
}

/// 测试类别 3: 所有格式使用 gzip 压缩，向 /gzip/ingest/{fmt} 发送数据
async fn test_category_3_gzip_ingest() {
    println!("\n╔════════════════════════════════════════════════════════════════╗");
    println!("║  测试类别 3: GZIP 压缩数据接收 (GZIP 压缩, 无认证)            ║");
    println!("╚════════════════════════════════════════════════════════════════╝\n");

    for fmt in ALL_FORMATS {
        println!("📋 测试格式: {}", fmt);
        println!("   端点: {}/gzip/ingest/{}", SERVER_HOST, fmt);
        println!("   压缩: gzip");
        println!("   认证: 无");

        let endpoint = format!("{}/gzip/ingest/{}", SERVER_HOST, fmt);
        let config = HttpSinkConfig::new(
            endpoint,
            Some(HTTP_METHOD.to_string()),
            None, // 无认证
            None,
            None, // 无自定义头
            Some(fmt.to_string()),
            None, // 使用默认批量大小
            Some(TIMEOUT_SECS),
            Some(MAX_RETRIES),
            Some("gzip".to_string()), // GZIP 压缩
        );

        let mut sink = match HttpSink::new(config).await {
            Ok(s) => s,
            Err(e) => {
                eprintln!("   ❌ 创建 Sink 失败: {}", e);
                continue;
            }
        };

        // 创建测试记录
        let records = create_test_records(TEST_RECORD_COUNT, 3000);

        // 发送数据
        match sink.sink_records(records).await {
            Ok(_) => {
                println!("   ✅ {} 格式发送成功 ({} 条记录)", fmt, TEST_RECORD_COUNT);
            }
            Err(e) => {
                eprintln!("   ❌ {} 格式发送失败: {}", fmt, e);
            }
        }

        println!();
    }
}

/// 打印测试统计信息
async fn print_test_statistics() {
    println!("\n╔════════════════════════════════════════════════════════════════╗");
    println!("║  测试统计信息                                                  ║");
    println!("╚════════════════════════════════════════════════════════════════╝\n");

    println!("💡 查看服务器统计:");
    println!("   curl http://localhost:8080/count");
    println!();

    println!("💡 查看特定格式的详细数据:");
    println!("   curl http://localhost:8080/details/json");
    println!("   curl http://localhost:8080/details/csv");
    println!();

    println!("💡 预期结果:");
    println!(
        "   每种格式应该接收到 {} 条记录 (3 个测试类别 × {} 条)",
        TEST_RECORD_COUNT * 3,
        TEST_RECORD_COUNT
    );
    println!();
}

#[tokio::main]
async fn main() {
    // 初始化日志
    let _ = env_logger::Builder::new()
        .filter_level(log::LevelFilter::Info)
        .try_init();

    println!("\n╔════════════════════════════════════════════════════════════════╗");
    println!("║           HTTP Sink 完整功能测试程序                          ║");
    println!("╚════════════════════════════════════════════════════════════════╝");

    println!("\n📋 测试配置:");
    println!("   服务器地址: {}", SERVER_HOST);
    println!("   测试格式: {:?}", ALL_FORMATS);
    println!("   每次发送记录数: {}", TEST_RECORD_COUNT);
    println!("   测试类别数: 3");
    println!();

    println!("⚠️  重要提示:");
    println!("   请先启动测试服务器:");
    println!("   python3 examples/http/test_server.py");
    println!();

    // 执行三类测试
    test_category_1_basic_ingest().await;
    test_category_2_auth_ingest().await;
    test_category_3_gzip_ingest().await;

    // 打印统计信息
    print_test_statistics().await;

    println!("\n╔════════════════════════════════════════════════════════════════╗");
    println!("║  所有测试执行完成                                              ║");
    println!("╚════════════════════════════════════════════════════════════════╝\n");

    println!("📊 测试总结:");
    println!("   测试类别: 3");
    println!("   测试格式: {} 种", ALL_FORMATS.len());
    println!(
        "   总测试数: {} (3 类别 × {} 格式)",
        ALL_FORMATS.len() * 3,
        ALL_FORMATS.len()
    );
    println!(
        "   预期总记录数: {} 条",
        ALL_FORMATS.len() * 3 * TEST_RECORD_COUNT
    );
    println!();

    println!("💡 支持的输出格式:");
    println!("   - json:       JSON 数组格式 [{{...}}, {{...}}]");
    println!("   - ndjson:     换行分隔的 JSON (每行一个对象)");
    println!("   - csv:        CSV 格式（带表头）");
    println!("   - kv:         键值对格式 (key=value key2=value2)");
    println!("   - raw:        原始字段值（空格分隔）");
    println!("   - proto-text: Protocol Buffer 文本格式");
    println!();

    println!("� 测试类别说明:");
    println!("   1. 基础接收:   POST /ingest/{{format}}        (无压缩, 无认证)");
    println!("   2. 认证接收:   POST /auth/ingest/{{format}}   (无压缩, 需认证 root/root)");
    println!("   3. 压缩接收:   POST /gzip/ingest/{{format}}   (GZIP 压缩, 无认证)");
    println!();
}
