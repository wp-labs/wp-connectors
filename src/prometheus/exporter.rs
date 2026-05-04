#![allow(dead_code)] // Prometheus 导出器目前仅在上游服务注册时使用

use actix_web::{App, HttpRequest, HttpResponse, HttpServer, get};
use async_trait::async_trait;
use orion_error::conversion::SourceRawErr;
use prometheus::Encoder;
use std::sync::Arc;
use sysinfo::System;
use wp_connector_api::{SinkReason, SinkResult};
use wp_model_core::model::DataRecord;
use wp_model_core::model::Value;

use super::metrics::IntoOptField; // 使 .opt() 可见
use super::metrics::{
    cpu_usage_stat, memory_usage_stat, parse_all_stat, receive_data_stat, sink_stat,
};
use orion_exp::ValueGet0; // 使 .get_value() 可见

#[get("/metrics")]
async fn metrics(_req: HttpRequest) -> HttpResponse {
    let encoder = prometheus::TextEncoder::new();
    let mut buffer = vec![];
    let mf = prometheus::gather();
    match encoder.encode(&mf, &mut buffer) {
        Ok(_) => HttpResponse::Ok().body(buffer),
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}

pub(crate) struct PrometheusExporter {
    pub(super) system: System,
}

#[async_trait]
impl wp_connector_api::AsyncRecordSink for PrometheusExporter {
    async fn sink_record(&mut self, data: &DataRecord) -> SinkResult<()> {
        if let Some(Value::Chars(field)) = data.get2("stage").opt().get_value() {
            match field.as_str() {
                "Pick" => {
                    receive_data_stat(data);
                }
                "Parse" => {
                    parse_all_stat(data);
                }
                "Sink" => {
                    sink_stat(data);
                }
                _ => {}
            }
        }
        cpu_usage_stat(data, &mut self.system);
        memory_usage_stat(data, &mut self.system);
        Ok(())
    }

    async fn sink_records(&mut self, data: Vec<Arc<DataRecord>>) -> SinkResult<()> {
        for record in data {
            self.sink_record(record.as_ref()).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl wp_connector_api::AsyncCtrl for PrometheusExporter {
    async fn stop(&mut self) -> SinkResult<()> {
        Ok(())
    }
    async fn reconnect(&mut self) -> SinkResult<()> {
        Ok(())
    }
}

#[async_trait]
impl wp_connector_api::AsyncRawDataSink for PrometheusExporter {
    async fn sink_str(&mut self, _data: &str) -> SinkResult<()> {
        Err(SinkReason::sink(
            "Prometheus sink does not support raw input; route TDC metrics only",
        ))
    }

    async fn sink_bytes(&mut self, _data: &[u8]) -> SinkResult<()> {
        Err(SinkReason::sink(
            "Prometheus sink does not support raw bytes input; route TDC metrics only",
        ))
    }

    async fn sink_str_batch(&mut self, _data: Vec<&str>) -> SinkResult<()> {
        Err(SinkReason::sink(
            "Prometheus sink does not support raw input; route TDC metrics only",
        ))
    }

    async fn sink_bytes_batch(&mut self, _data: Vec<&[u8]>) -> SinkResult<()> {
        Err(SinkReason::sink(
            "Prometheus sink does not support raw bytes input; route TDC metrics only",
        ))
    }
}

impl PrometheusExporter {
    pub(super) async fn metrics_service(endpoint: String) -> SinkResult<()> {
        HttpServer::new(|| App::new().service(metrics))
            .bind(endpoint.as_str())
            .source_raw_err(SinkReason::Sink, "bind prometheus metrics endpoint failed")?
            .run()
            .await
            .source_raw_err(SinkReason::Sink, "run prometheus metrics service failed")?;
        Ok(())
    }
}
