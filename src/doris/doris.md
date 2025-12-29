## 背景
这是一款日志解析输出工具，我需要为这个工具实现一个doris的输出插件。

## 相关接口
`@../wp-open-api/docs/zh/connector_guide.md`：插件接口相关文档
`@../wp-open-api/wp-connector-api`：接口对应的代码

## 用例
### 配置用例
```
[connectors.params]
endpoint="mysql://localhost:9030?charset=utf8mb4&connect_timeout=10"
database = "test_db"

user = "root"
password = ""

table = "events_parsed"
create_table = "
    CREATE TABLE IF NOT EXISTS `{table}` (
        `id` INT,
        `name` STRING,
        `score` DOUBLE
    )
    DUPLICATE KEY (`id`)
    DISTRIBUTED BY HASH(`id`) BUCKETS 2
    PROPERTIES (
        "replication_num" = "1"
    )
"
pool = 2 
```
- endpoint: 连接的端点（不包含 database 名称）
- database: 需要写入的数据库名，若不存在将自动创建
- user: 数据库用户
- password: 密码
- pool：数据库连接池大小
- table: 表名
- create_table:建表语句
<!-- - protocol：协议，MYSQL和Arrow Flight SQL协议，目前先支持MYSQL协议 -->
### 执行流程
- 初始化：
    - 构建连接池.
    - 查询表是否存在。
        - 不存在则：先判断create_table是否存在，存在则执行创建语句，不存在则报错。
    - 获取表字段，存到Set集合中。
写入数据：遍历字段的key，根据Set集合中的字段构建语句，然后进行批量插入。

### 代码规范
写完后，需要有注释、有测试用例、单元测试和集成测试。集成测试。你需要执行cargo test --package wp-connectors --test doris_tests -- integration_tests --nocapture 进行集成测试

### 集成测试
在`@tests/doris/integration_tests.rs`中编写，需要完成数据库的连接、字段查询、数据插入的测试。

集成测试的环境：mysql://root:@localhost:9030/wp_test
表名：doris_test,   
```
      CREATE TABLE IF NOT EXISTS doris_test (
            `id` INT,
            `name` STRING,
            `score` DOUBLE
        )
        DUPLICATE KEY (`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        )
```
