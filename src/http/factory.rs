use crate::http::{HttpSink, HttpSinkConfig};
use async_trait::async_trait;
use serde_json::{Value, json};
use std::collections::HashMap;
use wp_connector_api::{
    ConnectorDef, ConnectorScope, ParamMap, SinkBuildCtx, SinkDefProvider, SinkFactory, SinkHandle,
    SinkReason, SinkResult, SinkSpec,
};

/// Factory for creating and validating HTTP Sink instances
///
/// This factory implements the SinkFactory trait to integrate with the wp-connector-api
/// framework. It validates configuration parameters and constructs HttpSink instances.
pub struct HttpSinkFactory;

#[async_trait]
impl SinkFactory for HttpSinkFactory {
    fn kind(&self) -> &'static str {
        "http"
    }

    fn validate_spec(&self, spec: &SinkSpec) -> SinkResult<()> {
        crate::utils::reject_arrow_protocol(spec, self.kind())?;
        // Validate endpoint
        let endpoint = required_string(spec, "endpoint")?;
        validate_url_scheme(&endpoint)?;

        // Validate HTTP method
        if let Some(method) = optional_string(spec, "method") {
            validate_http_method(&method)?;
        }

        // Validate timeout
        if let Some(timeout) = get_u64(spec, "timeout_secs")
            && timeout == 0
        {
            return Err(SinkReason::sink("http.timeout_secs must be > 0"));
        }

        // Validate format
        if let Some(fmt) = optional_string(spec, "fmt") {
            validate_format(&fmt)?;
        }

        // Validate compression
        if let Some(compression) = optional_string(spec, "compression") {
            validate_compression(&compression)?;
        }

        Ok(())
    }

    async fn build(&self, spec: &SinkSpec, _ctx: &SinkBuildCtx) -> SinkResult<SinkHandle> {
        let endpoint = required_string(spec, "endpoint")?;
        let method = optional_string(spec, "method");
        let username = optional_string(spec, "username");
        let password = optional_string(spec, "password");
        let headers = parse_headers(spec)?;
        let fmt = optional_string(spec, "fmt");
        let batch_size = get_usize(spec, "batch_size");
        let timeout_secs = get_u64(spec, "timeout_secs");
        let max_retries = get_i32(spec, "max_retries");
        let compression = optional_string(spec, "compression");

        let config = HttpSinkConfig::new(
            endpoint,
            method,
            username,
            password,
            headers,
            fmt,
            batch_size,
            timeout_secs,
            max_retries,
            compression,
        );

        let sink = HttpSink::new(config).await?;

        Ok(SinkHandle::new(Box::new(sink)))
    }
}

impl SinkDefProvider for HttpSinkFactory {
    fn sink_def(&self) -> ConnectorDef {
        ConnectorDef {
            id: "http_sink".into(),
            kind: self.kind().into(),
            scope: ConnectorScope::Sink,
            allow_override: vec![
                "endpoint",
                "method",
                "username",
                "password",
                "headers",
                "fmt",
                "batch_size",
                "timeout_secs",
                "max_retries",
                "compression",
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
            default_params: http_sink_defaults(),
            origin: Some("wp-connectors:http_sink".into()),
        }
    }
}

/// Validate URL scheme is http or https
fn validate_url_scheme(endpoint: &str) -> SinkResult<()> {
    if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        Ok(())
    } else {
        Err(SinkReason::sink(
            "http.endpoint must use http or https protocol",
        ))
    }
}

/// Validate HTTP method is one of the supported methods
fn validate_http_method(method: &str) -> SinkResult<()> {
    let method_upper = method.to_uppercase();
    if matches!(
        method_upper.as_str(),
        "GET" | "POST" | "PUT" | "PATCH" | "DELETE"
    ) {
        Ok(())
    } else {
        Err(SinkReason::sink(
            "http.method must be one of GET, POST, PUT, PATCH, DELETE",
        ))
    }
}

/// Validate output format is one of the supported formats
fn validate_format(fmt: &str) -> SinkResult<()> {
    if matches!(fmt, "json" | "ndjson" | "csv" | "kv" | "raw" | "proto-text") {
        Ok(())
    } else {
        Err(SinkReason::sink(
            "http.fmt must be one of json, ndjson, csv, kv, raw, proto-text",
        ))
    }
}

/// Validate compression algorithm is one of the supported algorithms
fn validate_compression(compression: &str) -> SinkResult<()> {
    if matches!(compression, "none" | "gzip") {
        Ok(())
    } else {
        Err(SinkReason::sink(
            "http.compression must be one of none, gzip",
        ))
    }
}

/// Read required string parameter
fn required_string(spec: &SinkSpec, key: &str) -> SinkResult<String> {
    spec.params
        .get(key)
        .and_then(Value::as_str)
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| SinkReason::sink(format!("http.{key} must not be empty")))
}

/// Read optional string parameter
fn optional_string(spec: &SinkSpec, key: &str) -> Option<String> {
    spec.params
        .get(key)
        .and_then(Value::as_str)
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// Read u64 parameter
fn get_u64(spec: &SinkSpec, key: &str) -> Option<u64> {
    spec.params.get(key).and_then(Value::as_u64)
}

/// Read usize parameter
fn get_usize(spec: &SinkSpec, key: &str) -> Option<usize> {
    spec.params
        .get(key)
        .and_then(Value::as_u64)
        .map(|v| v as usize)
}

/// Read i32 parameter
fn get_i32(spec: &SinkSpec, key: &str) -> Option<i32> {
    spec.params
        .get(key)
        .and_then(Value::as_i64)
        .map(|v| v as i32)
}

/// Parse headers from spec parameters
fn parse_headers(spec: &SinkSpec) -> SinkResult<Option<HashMap<String, String>>> {
    match spec.params.get("headers") {
        None => Ok(None),
        Some(Value::Object(map)) => {
            let mut headers = HashMap::new();
            for (key, value) in map {
                if let Some(val_str) = value.as_str() {
                    headers.insert(key.clone(), val_str.to_string());
                } else {
                    return Err(SinkReason::sink(format!(
                        "http.headers.{key} must be a string"
                    )));
                }
            }
            Ok(Some(headers))
        }
        Some(_) => Err(SinkReason::sink("http.headers must be an object")),
    }
}

/// Default parameters for HTTP Sink
fn http_sink_defaults() -> ParamMap {
    let mut params = ParamMap::new();
    params.insert("endpoint".into(), json!("http://localhost:8080/webhook"));
    params.insert("method".into(), json!(HttpSinkConfig::default_method()));
    params.insert("username".into(), json!(""));
    params.insert("password".into(), json!(""));
    params.insert("headers".into(), json!({}));
    params.insert("fmt".into(), json!(HttpSinkConfig::default_fmt()));
    params.insert(
        "batch_size".into(),
        json!(HttpSinkConfig::default_batch_size()),
    );
    params.insert(
        "timeout_secs".into(),
        json!(HttpSinkConfig::default_timeout_secs()),
    );
    params.insert(
        "max_retries".into(),
        json!(HttpSinkConfig::default_max_retries()),
    );
    params.insert(
        "compression".into(),
        json!(HttpSinkConfig::default_compression()),
    );
    params
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn base_spec() -> SinkSpec {
        let mut params = BTreeMap::new();
        params.insert(
            "endpoint".into(),
            Value::String("http://localhost:8080/webhook".into()),
        );
        SinkSpec {
            name: "http_sink".into(),
            kind: "http".into(),
            connector_id: String::new(),
            group: "test".into(),
            params,
            filter: None,
        }
    }

    #[test]
    fn validate_rejects_empty_endpoint() {
        let mut spec = base_spec();
        spec.params
            .insert("endpoint".into(), Value::String("".into()));
        let factory = HttpSinkFactory;
        assert!(factory.validate_spec(&spec).is_err());
    }

    #[test]
    fn validate_rejects_invalid_url_scheme() {
        let mut spec = base_spec();
        spec.params
            .insert("endpoint".into(), Value::String("ftp://example.com".into()));
        let factory = HttpSinkFactory;
        let result = factory.validate_spec(&spec);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.reason(), &SinkReason::Sink);
        assert!(
            err.detail()
                .as_deref()
                .is_some_and(|m| m.contains("http or https protocol"))
        );
    }

    #[test]
    fn validate_accepts_http_scheme() {
        let mut spec = base_spec();
        spec.params.insert(
            "endpoint".into(),
            Value::String("http://example.com".into()),
        );
        let factory = HttpSinkFactory;
        assert!(factory.validate_spec(&spec).is_ok());
    }

    #[test]
    fn validate_accepts_https_scheme() {
        let mut spec = base_spec();
        spec.params.insert(
            "endpoint".into(),
            Value::String("https://example.com".into()),
        );
        let factory = HttpSinkFactory;
        assert!(factory.validate_spec(&spec).is_ok());
    }

    #[test]
    fn validate_rejects_invalid_method() {
        let mut spec = base_spec();
        spec.params
            .insert("method".into(), Value::String("INVALID".into()));
        let factory = HttpSinkFactory;
        let result = factory.validate_spec(&spec);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.reason(), &SinkReason::Sink);
        assert!(
            err.detail()
                .as_deref()
                .is_some_and(|m| m.contains("method"))
        );
    }

    #[test]
    fn validate_accepts_all_valid_methods() {
        let methods = vec!["GET", "POST", "PUT", "PATCH", "DELETE"];
        let factory = HttpSinkFactory;

        for method in methods {
            let mut spec = base_spec();
            spec.params
                .insert("method".into(), Value::String(method.into()));
            assert!(
                factory.validate_spec(&spec).is_ok(),
                "Method {} should be valid",
                method
            );
        }
    }

    #[test]
    fn validate_accepts_case_insensitive_methods() {
        let methods = vec!["get", "post", "Put", "pAtCh", "DeLeTe"];
        let factory = HttpSinkFactory;

        for method in methods {
            let mut spec = base_spec();
            spec.params
                .insert("method".into(), Value::String(method.into()));
            assert!(
                factory.validate_spec(&spec).is_ok(),
                "Method {} should be valid",
                method
            );
        }
    }

    #[test]
    fn validate_rejects_zero_timeout() {
        let mut spec = base_spec();
        spec.params
            .insert("timeout_secs".into(), Value::Number(0.into()));
        let factory = HttpSinkFactory;
        let result = factory.validate_spec(&spec);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.reason(), &SinkReason::Sink);
        assert!(
            err.detail()
                .as_deref()
                .is_some_and(|m| m.contains("timeout_secs"))
        );
    }

    #[test]
    fn validate_accepts_positive_timeout() {
        let mut spec = base_spec();
        spec.params
            .insert("timeout_secs".into(), Value::Number(30.into()));
        let factory = HttpSinkFactory;
        assert!(factory.validate_spec(&spec).is_ok());
    }

    #[test]
    fn validate_rejects_unsupported_format() {
        let mut spec = base_spec();
        spec.params
            .insert("fmt".into(), Value::String("xml".into()));
        let factory = HttpSinkFactory;
        let result = factory.validate_spec(&spec);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.reason(), &SinkReason::Sink);
        assert!(err.detail().as_deref().is_some_and(|m| m.contains("fmt")));
    }

    #[test]
    fn validate_accepts_all_supported_formats() {
        let formats = vec!["json", "ndjson", "csv", "kv", "raw", "proto-text"];
        let factory = HttpSinkFactory;

        for fmt in formats {
            let mut spec = base_spec();
            spec.params.insert("fmt".into(), Value::String(fmt.into()));
            assert!(
                factory.validate_spec(&spec).is_ok(),
                "Format {} should be valid",
                fmt
            );
        }
    }

    #[test]
    fn validate_rejects_unsupported_compression() {
        let mut spec = base_spec();
        spec.params
            .insert("compression".into(), Value::String("bzip2".into()));
        let factory = HttpSinkFactory;
        let result = factory.validate_spec(&spec);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.reason(), &SinkReason::Sink);
        assert!(
            err.detail()
                .as_deref()
                .is_some_and(|m| m.contains("compression"))
        );
    }

    #[test]
    fn validate_accepts_supported_compression() {
        let compressions = vec!["none", "gzip"];
        let factory = HttpSinkFactory;

        for compression in compressions {
            let mut spec = base_spec();
            spec.params
                .insert("compression".into(), Value::String(compression.into()));
            assert!(
                factory.validate_spec(&spec).is_ok(),
                "Compression {} should be valid",
                compression
            );
        }
    }

    #[test]
    fn validate_accepts_minimal_spec() {
        let spec = base_spec();
        let factory = HttpSinkFactory;
        assert!(factory.validate_spec(&spec).is_ok());
    }

    #[test]
    fn factory_kind_returns_http() {
        let factory = HttpSinkFactory;
        assert_eq!(factory.kind(), "http");
    }
}
