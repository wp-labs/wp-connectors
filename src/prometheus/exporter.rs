#![allow(dead_code)] // Prometheus 导出器目前仅在上游服务注册时使用

use actix_web::{App, HttpRequest, HttpResponse, HttpServer, get};
use async_trait::async_trait;
use prometheus::Encoder;
use std::sync::Arc;
use wp_connector_api::{SinkReason, SinkResult};
use wp_model_core::model::DataRecord;
use wp_model_core::model::Value;

use super::metrics::IntoOptField; // 使 .opt() 可见
use super::metrics::{parse_all_stat, parse_success_stat, receive_data_stat, sink_stat};
use orion_exp::ValueGet0; // 使 .get_value() 可见

type AnyResult<T> = anyhow::Result<T>;

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

#[derive(Clone)]
pub(crate) struct PrometheusExporter {
    pub(super) source_key_format: String,
    pub(super) sink_key_format: String,
}

#[async_trait]
impl wp_connector_api::AsyncRecordSink for PrometheusExporter {
    async fn sink_record(&mut self, data: &DataRecord) -> SinkResult<()> {
        if let Some(Value::Chars(field)) = data.get2("stage").opt().get_value() {
            match field.as_str() {
                "Pick" => {
                    receive_data_stat(data, &self.source_key_format);
                }
                "Parse" => {
                    parse_success_stat(data);
                    parse_all_stat(data);
                }
                "Sink" => {
                    sink_stat(data, &self.sink_key_format);
                }
                _ => {}
            }
        }
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
        Err(SinkReason::Sink(
            "Prometheus sink does not support raw input; route TDC metrics only".into(),
        )
        .into())
    }

    async fn sink_bytes(&mut self, _data: &[u8]) -> SinkResult<()> {
        Err(SinkReason::Sink(
            "Prometheus sink does not support raw bytes input; route TDC metrics only".into(),
        )
        .into())
    }

    async fn sink_str_batch(&mut self, _data: Vec<&str>) -> SinkResult<()> {
        Err(SinkReason::Sink(
            "Prometheus sink does not support raw input; route TDC metrics only".into(),
        )
        .into())
    }

    async fn sink_bytes_batch(&mut self, _data: Vec<&[u8]>) -> SinkResult<()> {
        Err(SinkReason::Sink(
            "Prometheus sink does not support raw bytes input; route TDC metrics only".into(),
        )
        .into())
    }
}

impl PrometheusExporter {
    pub(super) async fn metrics_service(endpoint: String) -> AnyResult<()> {
        HttpServer::new(|| App::new().service(metrics))
            .bind(endpoint.as_str())?
            .run()
            .await?;
        Ok(())
    }
}
