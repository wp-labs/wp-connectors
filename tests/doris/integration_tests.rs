#![cfg(feature = "doris")]
//! Integration tests for Doris sink factory. These tests focus on config validation so they
//! don't require a running Doris cluster.

use anyhow::Context;
use serde_json::Value;
use sqlx::{Row, mysql::MySqlPool, raw_sql};
use std::collections::BTreeMap;
use wp_connector_api::{SinkBuildCtx, SinkFactory, SinkSpec};
use wp_connectors::doris::{DorisSink, DorisSinkConfig, DorisSinkFactory};
use wp_model_core::model::{DataField, DataRecord};

const TEST_DORIS_ENDPOINT: &str = "mysql://localhost:9030";
const TEST_DORIS_DB: &str = "wp_test";
const TEST_DORIS_USER: &str = "root";
const TEST_DORIS_PASSWORD: &str = "";
const TEST_DORIS_TABLE: &str = "doris_test";
const CREATE_TABLE_TEMPLATE: &str = r#"
    CREATE TABLE IF NOT EXISTS `{table}` (
        `id` BIGINT NOT NULL AUTO_INCREMENT,
        `name` STRING,
        `score` DOUBLE
    )
    DUPLICATE KEY (`id`)
    DISTRIBUTED BY HASH(`id`) BUCKETS 2
    PROPERTIES (
        "replication_num" = "1"
    )
"#;
const SKIP_ENV: &str = "SKIP_DORIS_INTEGRATION_TESTS";

fn integration_spec() -> SinkSpec {
    let mut params = BTreeMap::new();
    params.insert("endpoint".into(), Value::String(TEST_DORIS_ENDPOINT.into()));
    params.insert("database".into(), Value::String(TEST_DORIS_DB.into()));
    params.insert("user".into(), Value::String(TEST_DORIS_USER.into()));
    params.insert("password".into(), Value::String(TEST_DORIS_PASSWORD.into()));
    params.insert("table".into(), Value::String(TEST_DORIS_TABLE.into()));
    params.insert("pool".into(), Value::from(2));
    params.insert("batch".into(), Value::from(1));
    params.insert(
        "create_table".into(),
        Value::String(CREATE_TABLE_TEMPLATE.into()),
    );
    SinkSpec {
        name: "doris_integration".into(),
        kind: "doris".into(),
        connector_id: String::new(),
        group: "integration".into(),
        params,
        filter: None,
    }
}

fn integration_config() -> DorisSinkConfig {
    DorisSinkConfig::default()
        .with_endpoint(TEST_DORIS_ENDPOINT)
        .with_database(TEST_DORIS_DB)
        .with_user(TEST_DORIS_USER)
        .with_password(TEST_DORIS_PASSWORD)
        .with_table(TEST_DORIS_TABLE)
        .with_create_table(CREATE_TABLE_TEMPLATE)
        .with_pool_size(2)
        .with_batch_size(1)
}

#[ignore = "not ready"]
#[tokio::test]
async fn doris_sink_connects_queries_schema_and_inserts_rows() -> anyhow::Result<()> {
    if should_skip_integration() {
        eprintln!("⚠️  Skipping Doris integration test ({} set)", SKIP_ENV);
        return Ok(());
    }

    let spec = integration_spec();
    let factory = DorisSinkFactory;
    factory.validate_spec(&spec)?;
    let ctx = SinkBuildCtx::new(std::env::current_dir()?);
    let mut handle = factory.build(&spec, &ctx).await?;
    let sink = DorisSink::new(integration_config()).await?;
    let pool = sink.pool;
    cleanup_test_rows(&pool).await?;

    let columns = fetch_table_columns(&pool).await?;
    assert_eq!(columns, vec!["id", "name", "score"]);

    let mut alice = DataRecord::default();
    alice.append(DataField::from_digit("id", 1));
    alice.append(DataField::from_chars("name", "Alice"));
    alice.append(DataField::from_chars("score", "98.5"));

    let mut bob = DataRecord::default();
    bob.append(DataField::from_digit("id", 2));
    bob.append(DataField::from_chars("name", "Bob"));
    bob.append(DataField::from_chars("score", "87.0"));

    handle.sink.sink_record(&alice).await?;
    handle.sink.sink_record(&bob).await?;
    handle.sink.stop().await?;

    let select_sql = format!(
        "SELECT id, name, score FROM `{}` ORDER BY id",
        TEST_DORIS_TABLE
    );
    let rows = raw_sql(&select_sql).fetch_all(&pool).await?;

    let mut actual = Vec::new();
    for row in rows {
        let id: i32 = row.try_get("id")?;
        let name: String = row.try_get("name")?;
        let score: f64 = row.try_get("score")?;
        actual.push((id, name, score));
    }

    assert_eq!(actual.len(), 2);
    assert_eq!(actual[0].0, 1);
    assert_eq!(actual[0].1, "Alice");
    assert!((actual[0].2 - 98.5).abs() < 1e-6);
    assert_eq!(actual[1].0, 2);
    assert_eq!(actual[1].1, "Bob");
    assert!((actual[1].2 - 87.0).abs() < 1e-6);

    cleanup_test_rows(&pool).await?;
    Ok(())
}

#[ignore = "not ready"]
#[tokio::test]
async fn doris_sink_new_initializes_via_direct_config() -> anyhow::Result<()> {
    if should_skip_integration() {
        eprintln!("⚠️  Skipping Doris integration test ({} set)", SKIP_ENV);
        return Ok(());
    }

    let _sink = DorisSink::new(integration_config()).await?;
    Ok(())
}

fn should_skip_integration() -> bool {
    std::env::var(SKIP_ENV).is_ok()
}

async fn cleanup_test_rows(pool: &MySqlPool) -> anyhow::Result<()> {
    let truncate_sql = format!("TRUNCATE TABLE `{}`", TEST_DORIS_TABLE);
    raw_sql(&truncate_sql)
        .execute(pool)
        .await
        .with_context(|| format!("truncate table failed: {}", truncate_sql))?;
    Ok(())
}

async fn fetch_table_columns(pool: &MySqlPool) -> anyhow::Result<Vec<String>> {
    let columns_sql = format!(
        "SELECT COLUMN_NAME FROM information_schema.COLUMNS \
         WHERE TABLE_SCHEMA='{}' AND TABLE_NAME='{}' \
         ORDER BY ORDINAL_POSITION",
        escape_single_quotes(TEST_DORIS_DB),
        escape_single_quotes(TEST_DORIS_TABLE)
    );
    let rows = raw_sql(&columns_sql)
        .fetch_all(pool)
        .await
        .context("fetch table columns")?;

    let mut cols = Vec::with_capacity(rows.len());
    for row in rows {
        cols.push(row.try_get("COLUMN_NAME")?);
    }
    Ok(cols)
}

fn escape_single_quotes(input: &str) -> String {
    input.replace('\'', "''")
}
