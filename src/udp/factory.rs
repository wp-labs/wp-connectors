use super::sink::UdpSink;
use async_trait::async_trait;
use orion_error::conversion::ToStructError;
use serde_json::json;
use wp_connector_api::{
    ConnectorDef, ConnectorScope, ParamMap, SinkBuildCtx, SinkDefProvider, SinkFactory, SinkHandle,
    SinkReason, SinkResult, SinkSpec,
};

#[derive(Clone, Debug)]
pub struct UdpSinkSpec {
    pub addr: String,
    pub port: u16,
    pub send_buffer_size: usize,
}

impl UdpSinkSpec {
    pub fn from_params(params: &ParamMap) -> SinkResult<Self> {
        let addr = params.get("addr").and_then(|v| v.as_str()).ok_or_else(|| {
            SinkReason::core_conf()
                .to_err()
                .with_detail("udp.addr must be a string")
        })?;
        if let Some(i) = params.get("port").and_then(|v| v.as_i64())
            && !(1..=65535).contains(&i)
        {
            return Err(SinkReason::core_conf()
                .to_err()
                .with_detail("udp.port must be in 1..=65535"));
        }
        let port = params.get("port").and_then(|v| v.as_i64()).unwrap_or(514) as u16;
        let send_buffer_size = params
            .get("send_buffer_size")
            .and_then(|v| v.as_i64())
            .unwrap_or(65536) as usize;
        Ok(Self {
            addr: addr.to_string(),
            port,
            send_buffer_size,
        })
    }

    pub fn target_addr(&self) -> String {
        format!("{}:{}", self.addr, self.port)
    }
}

pub struct UdpSinkFactory;

#[async_trait]
impl SinkFactory for UdpSinkFactory {
    fn kind(&self) -> &'static str {
        "udp"
    }

    fn validate_spec(&self, spec: &SinkSpec) -> SinkResult<()> {
        UdpSinkSpec::from_params(&spec.params)?;
        Ok(())
    }

    async fn build(&self, spec: &SinkSpec, _ctx: &SinkBuildCtx) -> SinkResult<SinkHandle> {
        let conf = UdpSinkSpec::from_params(&spec.params)?;
        let target = conf.target_addr();
        log::info!("udp sink build: target={}", target);
        let sink = UdpSink::connect(&conf).await?;
        Ok(SinkHandle::new(Box::new(sink)))
    }
}

impl SinkDefProvider for UdpSinkFactory {
    fn sink_def(&self) -> ConnectorDef {
        let mut params = ParamMap::new();
        params.insert("addr".into(), json!("127.0.0.1"));
        params.insert("port".into(), json!(514));
        params.insert("send_buffer_size".into(), json!(65536));
        ConnectorDef {
            id: "udp_sink".into(),
            kind: self.kind().into(),
            scope: ConnectorScope::Sink,
            allow_override: vec!["addr".into(), "port".into(), "send_buffer_size".into()],
            default_params: params,
            origin: Some("wp-connectors-labs:udp_sink".into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk_params(entries: &[(&str, serde_json::Value)]) -> ParamMap {
        let mut params = ParamMap::new();
        for (k, v) in entries {
            params.insert((*k).to_string(), v.clone());
        }
        params
    }

    #[test]
    fn udp_conf_defaults_to_port_514() {
        let conf = UdpSinkSpec::from_params(&mk_params(&[("addr", json!("192.168.1.1"))])).unwrap();
        assert_eq!(conf.addr, "192.168.1.1");
        assert_eq!(conf.port, 514);
    }

    #[test]
    fn udp_conf_accepts_custom_port() {
        let conf = UdpSinkSpec::from_params(&mk_params(&[
            ("addr", json!("10.0.0.1")),
            ("port", json!(1514)),
        ]))
        .unwrap();
        assert_eq!(conf.addr, "10.0.0.1");
        assert_eq!(conf.port, 1514);
        assert_eq!(conf.target_addr(), "10.0.0.1:1514");
    }

    #[test]
    fn udp_conf_rejects_missing_addr() {
        let err = UdpSinkSpec::from_params(&mk_params(&[])).unwrap_err();
        assert!(err.to_string().contains("udp.addr"));
    }

    #[test]
    fn udp_conf_rejects_invalid_port() {
        let err = UdpSinkSpec::from_params(&mk_params(&[
            ("addr", json!("127.0.0.1")),
            ("port", json!(70000)),
        ]))
        .unwrap_err();
        assert!(err.to_string().contains("udp.port"));
    }
}
