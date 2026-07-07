use super::factory::UdpSinkSpec;
use async_trait::async_trait;
use orion_error::conversion::SourceErr;
use wp_connector_api::{AsyncCtrl, AsyncRawDataSink, AsyncRecordSink, SinkReason, SinkResult};
use wp_data_fmt::RecordFormatter;

pub struct UdpSink {
    socket: tokio::net::UdpSocket,
    target: String,
    sent_cnt: u64,
}

impl UdpSink {
    pub async fn connect(spec: &UdpSinkSpec) -> SinkResult<Self> {
        let target = spec.target_addr();
        let socket = tokio::net::UdpSocket::bind("0.0.0.0:0")
            .await
            .source_err(SinkReason::Sink, "udp sink bind")?;
        socket2::SockRef::from(&socket)
            .set_send_buffer_size(spec.send_buffer_size)
            .source_err(SinkReason::Sink, "udp sink set send buffer")?;
        socket
            .connect(&target)
            .await
            .source_err(SinkReason::Sink, "udp sink connect")?;
        log::info!("udp sink connected: target={}", target);
        Ok(Self {
            socket,
            target,
            sent_cnt: 0,
        })
    }
}

#[async_trait]
impl AsyncCtrl for UdpSink {
    async fn stop(&mut self) -> SinkResult<()> {
        Ok(())
    }
    async fn reconnect(&mut self) -> SinkResult<()> {
        Ok(())
    }
}

#[async_trait]
impl AsyncRecordSink for UdpSink {
    async fn sink_record(&mut self, data: &wp_model_core::model::DataRecord) -> SinkResult<()> {
        let raw = wp_data_fmt::Raw::new().fmt_record(data);
        AsyncRawDataSink::sink_str(self, raw.as_str()).await
    }

    async fn sink_records(
        &mut self,
        data: Vec<std::sync::Arc<wp_model_core::model::DataRecord>>,
    ) -> SinkResult<()> {
        for record in data {
            self.sink_record(&record).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl AsyncRawDataSink for UdpSink {
    async fn sink_str(&mut self, data: &str) -> SinkResult<()> {
        let payload = data.as_bytes();
        if self.sent_cnt == 0 {
            log::info!(
                "udp sink first-send: target={} msg_len={} preview='{}'",
                self.target,
                payload.len(),
                data.chars().take(64).collect::<String>()
            );
        }
        self.socket
            .send(payload)
            .await
            .source_err(SinkReason::Sink, "udp sink send")?;
        self.sent_cnt = self.sent_cnt.saturating_add(1);
        Ok(())
    }

    async fn sink_bytes(&mut self, data: &[u8]) -> SinkResult<()> {
        if self.sent_cnt == 0 {
            log::info!(
                "udp sink first-send(bytes): target={} msg_len={}",
                self.target,
                data.len(),
            );
        }
        self.socket
            .send(data)
            .await
            .source_err(SinkReason::Sink, "udp sink send")?;
        self.sent_cnt = self.sent_cnt.saturating_add(1);
        Ok(())
    }

    async fn sink_str_batch(&mut self, data: Vec<&str>) -> SinkResult<()> {
        for s in data {
            self.sink_str(s).await?;
        }
        Ok(())
    }

    async fn sink_bytes_batch(&mut self, data: Vec<&[u8]>) -> SinkResult<()> {
        for bytes in data {
            self.sink_bytes(bytes).await?;
        }
        Ok(())
    }
}
