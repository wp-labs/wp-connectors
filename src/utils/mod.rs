//! 通用工具模块
pub mod fmt;
pub mod time_stat_utils;

pub mod arrow_fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Protocol {
    #[default]
    Text,
    Arrow,
}

/// Supported Arrow sinks. Used by `reject_arrow_protocol_on_unsupported_sink`.
const ARROW_SUPPORTED_SINKS: &[&str] = &["kafka", "clickhouse", "doris"];

/// Reject `protocol: arrow` on sinks that don't support Arrow output.
///
/// Always available regardless of the `wf` feature — the check is a simple
/// string comparison. Call this in every unsupported sink's `validate_spec`.
pub fn reject_arrow_protocol(
    spec: &wp_connector_api::SinkSpec,
    sink_kind: &str,
) -> wp_connector_api::SinkResult<()> {
    use wp_connector_api::SinkReason;
    if let Some("arrow") = spec
        .params
        .get("protocol")
        .and_then(|v| v.as_str())
        .map(|s| s.trim())
    {
        return Err(SinkReason::sink(format!(
            "protocol 'arrow' is not supported by the '{sink_kind}' sink. \
             Arrow output is currently supported on: {}",
            ARROW_SUPPORTED_SINKS.join(", ")
        )));
    }
    Ok(())
}

#[cfg(test)]
mod protocol_tests {
    use super::{ARROW_SUPPORTED_SINKS, reject_arrow_protocol};
    use std::collections::BTreeMap;
    use wp_connector_api::SinkSpec;

    fn sink_spec(kind: &str, protocol: Option<&str>) -> SinkSpec {
        let mut params = BTreeMap::new();
        if let Some(p) = protocol {
            params.insert("protocol".into(), serde_json::Value::String(p.into()));
        }
        SinkSpec {
            name: "test".into(),
            kind: kind.into(),
            connector_id: String::new(),
            group: "test".into(),
            params,
            filter: None,
        }
    }

    // -- Protocol enum ----------------------------------------------------

    #[test]
    fn protocol_default_is_text() {
        use super::Protocol;
        assert_eq!(Protocol::default(), Protocol::Text);
    }

    #[test]
    fn protocol_arrow_is_not_text() {
        use super::Protocol;
        assert_ne!(Protocol::Arrow, Protocol::Text);
    }

    // -- reject_arrow_protocol --------------------------------------------

    #[test]
    fn reject_arrow_on_unsupported_sink() {
        let spec = sink_spec("mysql", Some("arrow"));
        let err = reject_arrow_protocol(&spec, "mysql").unwrap_err();
        let detail = err.detail().as_deref().expect("should have detail");
        assert!(detail.contains("not supported"), "got: {detail}");
        assert!(detail.contains("mysql"), "got: {detail}");
        assert!(detail.contains("kafka"), "should list supported sinks");
    }

    #[test]
    fn accept_text_on_unsupported_sink() {
        let spec = sink_spec("mysql", Some("text"));
        assert!(reject_arrow_protocol(&spec, "mysql").is_ok());
    }

    #[test]
    fn accept_missing_protocol() {
        let spec = sink_spec("mysql", None);
        assert!(reject_arrow_protocol(&spec, "mysql").is_ok());
    }

    #[test]
    fn accept_arrow_on_supported_sink() {
        // The helper rejects *any* "arrow" value regardless of sink kind.
        // Factories only call it for unsupported sinks.
        let spec = sink_spec("kafka", Some("arrow"));
        let err = reject_arrow_protocol(&spec, "kafka").unwrap_err();
        assert!(err.detail().as_deref().unwrap().contains("not supported"));
    }

    #[test]
    fn supported_sinks_list_is_non_empty() {
        assert!(!ARROW_SUPPORTED_SINKS.is_empty());
        assert!(ARROW_SUPPORTED_SINKS.contains(&"kafka"));
        assert!(ARROW_SUPPORTED_SINKS.contains(&"clickhouse"));
        assert!(ARROW_SUPPORTED_SINKS.contains(&"doris"));
    }

    #[test]
    fn reject_arrow_with_whitespace() {
        let spec = sink_spec("prometheus", Some("  arrow  "));
        let err = reject_arrow_protocol(&spec, "prometheus").unwrap_err();
        assert!(err.detail().as_deref().unwrap().contains("not supported"));
    }

    #[test]
    fn accept_unknown_protocol_value() {
        let spec = sink_spec("mysql", Some("grpc"));
        assert!(reject_arrow_protocol(&spec, "mysql").is_ok());
    }
}
