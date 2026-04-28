use std::time::Duration;

use async_trait::async_trait;
use tokio::time::Instant;
use wp_connector_api::{DataSource, SourceBatch, SourceEvent, SourceReason, SourceResult, Tags};
use wp_model_core::raw::RawData;

pub struct CountSource {
    emitted: u64,
    batch_size: usize,
    remaining: Option<u64>,
    interval: Duration,
    last_emit_at: Option<Instant>,
}

impl CountSource {
    pub fn new(batch_size: usize, total: Option<u64>, interval: Duration) -> Self {
        Self {
            emitted: 0,
            batch_size,
            remaining: total,
            interval,
            last_emit_at: None,
        }
    }

    fn exhausted(&self) -> bool {
        matches!(self.remaining, Some(0))
    }

    fn build_batch(&mut self) -> SourceBatch {
        if self.exhausted() {
            return Vec::new();
        }

        let batch_len = match self.remaining {
            Some(remaining) => remaining.min(self.batch_size as u64) as usize,
            None => self.batch_size,
        };

        let mut batch = Vec::with_capacity(batch_len);
        for _ in 0..batch_len {
            let value = self.emitted + 1;
            let payload = format!(r#"{{"count":{value}}}"#);
            batch.push(SourceEvent::new(
                value,
                "count".to_string(),
                RawData::from_string(payload),
                Tags::new().into(),
            ));
            self.emitted = value;
        }

        if let Some(remaining) = &mut self.remaining {
            *remaining = remaining.saturating_sub(batch_len as u64);
        }

        batch
    }
}

#[async_trait]
impl DataSource for CountSource {
    async fn receive(&mut self) -> SourceResult<SourceBatch> {
        if self.exhausted() {
            return Err(SourceReason::EOF.into());
        }
        // 这里不是每次固定 sleep(interval)，而是按“距离上次产出还差多少”补齐等待。
        // 原因：上层调度可能会中断长时间阻塞，固定睡眠会放大丢轮询风险。
        if !self.interval.is_zero()
            && let Some(last_emit_at) = self.last_emit_at
        {
            let elapsed = last_emit_at.elapsed();
            if elapsed < self.interval {
                tokio::time::sleep(self.interval - elapsed).await;
            }
        }
        println!(
            "⏰ Emitting batch of count events, emitted so far: {}, remaining: {:?}...",
            self.emitted, self.remaining
        );
        let batch = self.build_batch();
        self.last_emit_at = Some(Instant::now());
        Ok(batch)
    }

    fn try_receive(&mut self) -> Option<SourceBatch> {
        None
    }

    fn identifier(&self) -> String {
        "count".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn count_source_respects_total() {
        let mut source = CountSource::new(2, Some(3), Duration::ZERO);

        let batch1 = source.receive().await.expect("first batch");
        assert_eq!(batch1.len(), 2);

        let payloads1: Vec<String> = batch1
            .iter()
            .map(|event| {
                String::from_utf8(event.payload.clone().into_bytes().to_vec()).expect("utf8")
            })
            .collect();
        assert_eq!(
            payloads1,
            vec![r#"{"count":1}"#.to_string(), r#"{"count":2}"#.to_string()]
        );

        let batch2 = source.receive().await.expect("second batch");
        assert_eq!(batch2.len(), 1);
        let payload2 =
            String::from_utf8(batch2[0].payload.clone().into_bytes().to_vec()).expect("utf8");
        assert_eq!(payload2, r#"{"count":3}"#);

        let err = source.receive().await.expect_err("should eof");
        assert!(err.to_string().contains("EOF") || err.to_string().contains("eof"));
    }
}
