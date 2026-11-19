use super::super::config::{LabelPath, Mapping, MetricRule, TimeUnit};
use super::super::factory::VictoriaMetricFactory;
use super::*;
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use wp_connector_api::{
    AsyncCtrl, AsyncRawDataSink, AsyncRecordSink, ParamMap, SinkBuildCtx, SinkDefProvider,
    SinkFactory, SinkSpec,
};
use wp_model_core::model::DataField;
use wp_model_core::model::types::value::ObjectValue;

fn config() -> VictoriaMetric {
    VictoriaMetric {
        keep_labels: Some(vec![]),
        ..Default::default()
    }
}
fn rules() -> VictoriaMetric {
    toml::from_str(include_str!(
        "../../examples/victoriametrics/monitoring.toml"
    ))
    .unwrap()
}
fn make_sink(config: VictoriaMetric) -> VictoriaMetricExporter {
    config.validate().unwrap();
    let mapper = Mapper::new(&config).unwrap();
    VictoriaMetricExporter {
        runtime: Some(Arc::new(SinkRuntime {
            transport_config: SharedSinkConfig::from(&config),
            http_client: reqwest::Client::builder()
                .no_proxy()
                .timeout(duration(config.timeout_secs, "timeout_secs").unwrap())
                .build()
                .unwrap(),
            metric_store: Arc::new(SharedMetricStore::new()),
            flush_task: None,
        })),
        mapper,
    }
}
fn map_json_record(mapper: &Mapper, input: &Value) -> SinkResult<Vec<MetricUpdate>> {
    mapper.map_batch(vec![input.clone()])
}
fn map_data_record(mapper: &Mapper, input: &DataRecord) -> SinkResult<Vec<MetricUpdate>> {
    mapper.map_records(std::iter::once(input))
}
fn first_update(mapper: &Mapper, input: &Value) -> MetricUpdate {
    map_json_record(mapper, input)
        .unwrap()
        .into_iter()
        .next()
        .unwrap()
}
fn first_data_record_update(mapper: &Mapper, input: &DataRecord) -> MetricUpdate {
    map_data_record(mapper, input)
        .unwrap()
        .into_iter()
        .next()
        .unwrap()
}
fn named(config: VictoriaMetric) -> VictoriaMetricExporter {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    VictoriaMetricExporter::for_name(
        &format!("vm-test-{}", NEXT.fetch_add(1, Ordering::Relaxed)),
        config,
    )
    .unwrap()
}
fn update(conf: &VictoriaMetric, value: Value) -> MetricUpdate {
    first_update(&Mapper::new(conf).unwrap(), &value)
}
fn counter() -> VictoriaMetric {
    let mut conf = config();
    conf.metrics.push(MetricRule {
        input: "requests".into(),
        name: Some("http_requests_total".into()),
        kind: Some(MetricKind::Counter),
        ..Default::default()
    });
    conf
}
fn timed() -> VictoriaMetric {
    VictoriaMetric {
        time_field: Some("ts".into()),
        ..config()
    }
}
fn snapshots(sink: &VictoriaMetricExporter) -> Vec<MetricUpdate> {
    sink.runtime()
        .unwrap()
        .metric_store
        .lock()
        .unwrap()
        .gather()
        .into_iter()
        .flat_map(|family| {
            let name = family.name().to_owned();
            let kind = match family.get_field_type() {
                prometheus::proto::MetricType::GAUGE => MetricKind::Gauge,
                prometheus::proto::MetricType::COUNTER => MetricKind::Counter,
                _ => return Vec::new(),
            };
            family
                .get_metric()
                .iter()
                .map(|metric| MetricUpdate {
                    name: name.clone(),
                    labels: metric
                        .get_label()
                        .iter()
                        .map(|label| (label.name().to_owned(), label.value().to_owned()))
                        .collect(),
                    value: match kind {
                        MetricKind::Gauge => metric.get_gauge().value(),
                        MetricKind::Counter => metric.get_counter().value(),
                    },
                    timestamp: (metric.timestamp_ms() != 0).then_some(metric.timestamp_ms()),
                    kind,
                    help: (!family.help().is_empty()).then(|| family.help().to_owned()),
                })
                .collect::<Vec<_>>()
        })
        .collect()
}
fn spec(params: Value) -> SinkSpec {
    SinkSpec {
        name: "test".into(),
        group: "g".into(),
        kind: "victoriametrics".into(),
        connector_id: "victoriametrics_sink".into(),
        params: serde_json::from_value(params).unwrap(),
        filter: None,
    }
}

#[test]
fn defaults_and_factory_register_every_parameter() {
    let conf = VictoriaMetric::from_params(&ParamMap::new()).unwrap();
    assert_eq!(conf.mapping, Mapping::Fields);
    assert_eq!(
        (conf.name_field.as_str(), conf.value_field.as_str()),
        ("__name__", "values")
    );
    let def = VictoriaMetricFactory.sink_def();
    for key in [
        "mapping",
        "metrics",
        "time_zone",
        "label_aliases",
        "keep_labels",
    ] {
        assert!(def.allow_override.contains(&key.to_string()));
    }
    VictoriaMetricFactory
        .validate_spec(&spec(json!({})))
        .unwrap();
}

#[tokio::test]
async fn invalid_durations_fail_validation_and_build() {
    let ctx = SinkBuildCtx::new(".".into());
    for key in ["flush_secs", "timeout_secs"] {
        for value in [
            json!(0),
            json!(-1),
            json!("0"),
            json!("-1"),
            json!("NaN"),
            json!("inf"),
            json!(1e100),
            json!(1e19),
            json!(1e-20),
            json!(true),
            Value::Null,
            json!([]),
            json!("bad"),
        ] {
            let mut input = json!({});
            input[key] = value;
            let spec = spec(input);
            assert!(VictoriaMetricFactory.validate_spec(&spec).is_err(), "{key}");
            assert!(
                VictoriaMetricFactory.build(&spec, &ctx).await.is_err(),
                "{key}"
            );
        }
    }
    assert_eq!(
        VictoriaMetric::from_params(&spec(json!({"flush_secs":"0.25"})).params)
            .unwrap()
            .flush_secs,
        0.25
    );
}

#[test]
fn configuration_rejects_invalid_contracts() {
    for input in [
        json!({"mapping":"auto"}),
        json!({"unknown":true}),
        json!({"stage_field":"stage"}),
        json!({"rule_field":"name"}),
        json!({"endpoint":"file:///tmp/metrics"}),
        json!({"api_path":""}),
        json!({"api_path":"/metrics#ignored"}),
        json!({"mapping":"rules"}),
        json!({"time_field":"ts","time_format":"%Q","time_zone":"+08:00"}),
        json!({"time_field":"ts","time_format":"%Y-%m-%d","time_zone":"Asia/Shanghai"}),
        json!({"name_field":"/bad~2path"}),
        json!({"metrics":[{"input":"a","type":"gauge","mode":"delta"}]}),
        json!({"metrics":[{"input":"a","name":"bad-name"}]}),
        json!({"metrics":[{"input":"a","labels":{"x":[]}}]}),
        json!({"metrics":[{"input":"a","labels":{"x":"values"}}]}),
        json!({"metrics":[{"input":"a","format":"%Y"}]}),
        json!({"metrics":[{"input":"a","unit":"s"}]}),
        json!({"metrics":[{"input":"a","zone":"+08:00"}]}),
        json!({"mapping":"rules","metrics":[{"stage":"Pick","input":"a"}]}),
        json!({"metrics":[{"input":"a"},{"input":"a"}]}),
        json!({"metrics":[{"input":"a","name":"x","type":"gauge"},{"input":"b","name":"x","type":"counter"}]}),
        json!({"metrics":[{"input":"a","name":"x","help":"one"},{"input":"b","name":"x","help":"two"}]}),
    ] {
        assert!(
            VictoriaMetricFactory
                .validate_spec(&spec(input.clone()))
                .is_err(),
            "{input}"
        );
    }
    let mut conf = rules();
    let duplicate = conf.metrics[0].clone();
    conf.metrics.push(duplicate);
    assert!(Mapper::new(&conf).is_err());
    let input = json!({"metrics":[{"input":"a","name":"x","labels":{"host":"host"}},{"input":"b","name":"x","labels":{"region":"region"}}]});
    assert!(VictoriaMetricFactory.validate_spec(&spec(input)).is_ok());
}

#[test]
fn fields_mapping_is_explicit_and_one_record_one_sample() {
    let conf = config();
    let value = update(
        &conf,
        json!({"__name__":"temperature","values":"23.5","other":99}),
    );
    assert_eq!(value.kind, MetricKind::Gauge);
    assert_eq!(value.value, 23.5);
    assert_eq!(value.timestamp, None);
    assert!(value.labels.is_empty());
    for object in [
        json!({"stage":"Pick","total":3}),
        json!({"__name__":"" ,"values":1}),
        json!({"__name__":"a","values":[1,2]}),
        json!({"__name__":"a","values":"NaN"}),
        json!({"__name__":"a","values":"inf"}),
        json!({"__name__":"a"}),
    ] {
        assert!(map_json_record(&Mapper::new(&conf).unwrap(), &object).is_err());
    }
    let invalid_name_update = map_json_record(
        &Mapper::new(&conf).unwrap(),
        &json!({"__name__":"bad-name","values":1}),
    )
    .unwrap();
    assert!(
        SharedMetricStore::new()
            .lock()
            .unwrap()
            .apply(&invalid_name_update)
            .is_err()
    );
    let conf = counter();
    assert_eq!(
        update(&conf, json!({"__name__":"requests","values":2})).name,
        "http_requests_total"
    );
    assert_eq!(
        update(&conf, json!({"__name__":"unlisted","values":2})).kind,
        MetricKind::Gauge
    );
}

#[test]
fn json_pointer_labels_filter_drop_and_alias() {
    let mut conf = config();
    conf.name_field = "/metric/name".into();
    conf.value_field = "/sample/value".into();
    conf.keep_labels = Some(vec!["/host/name".into(), "region".into(), "ignored".into()]);
    conf.drop_labels = vec!["region".into()];
    conf.label_aliases
        .insert("/host/name".into(), "instance".into());
    let sample = update(
        &conf,
        json!({"metric":{"name":"load"},"sample":{"value":1.5},"host":{"name":"node"},"region":"west","unused":true}),
    );
    assert_eq!(
        sample.labels,
        BTreeMap::from([("instance".into(), "node".into())])
    );
    let mut conf = config();
    conf.keep_labels = None;
    let sample = update(
        &conf,
        json!({"__name__":"load","values":1,"host":"node","up":true,"nested":{"x":1},"array":[1]}),
    );
    assert_eq!(sample.labels.len(), 2);
    conf.keep_labels = Some(vec!["host".into(), "up".into()]);
    conf.label_aliases =
        BTreeMap::from([("host".into(), "same".into()), ("up".into(), "same".into())]);
    assert!(Mapper::new(&conf).is_err());
}

#[test]
fn explicit_label_fallback_and_context() {
    let mut conf = rules();
    let input = json!({"stage":"Pick","name":"pick_stat","total":3,"wp_source_type":"kafka","wp_access_ip":"preferred","target":"fallback"});
    let sample = update(&conf, input.clone());
    assert_eq!(sample.labels["source_name"], "preferred");
    assert!(!sample.labels.contains_key("tenant_id"));
    assert_eq!(sample.labels["access_name"], "warp-parse");
    let mut input = input;
    input.as_object_mut().unwrap().remove("wp_access_ip");
    assert_eq!(
        update(&conf, input.clone()).labels["source_name"],
        "fallback"
    );
    input["wp_access_ip"] = json!({"bad":"scalar"});
    assert!(map_json_record(&Mapper::new(&conf).unwrap(), &input).is_err());
    input.as_object_mut().unwrap().remove("wp_access_ip");
    input.as_object_mut().unwrap().remove("target");
    let sample_without_source_name = first_update(&Mapper::new(&conf).unwrap(), &input);
    assert!(
        !sample_without_source_name
            .labels
            .contains_key("source_name")
    );
    conf.metrics[0]
        .labels
        .as_mut()
        .unwrap()
        .insert("pid".into(), LabelPath::One("host".into()));
    assert!(Mapper::new(&conf).is_err());
}

#[test]
fn drop_labels_excludes_explicit_label_mapping() {
    let mut conf = config();
    conf.keep_labels = None;
    conf.drop_labels = vec!["host".into()];
    conf.metrics.push(MetricRule {
        input: "a".into(),
        labels: Some(BTreeMap::from([(
            "instance".into(),
            LabelPath::One("host".into()),
        )])),
        ..Default::default()
    });
    assert!(
        update(&conf, json!({"__name__":"a","values":1,"host":"x"}))
            .labels
            .is_empty()
    );
    let mut conf = config();
    conf.keep_labels = Some(vec!["/host".into()]);
    conf.label_aliases.insert("host".into(), "instance".into());
    assert_eq!(
        update(&conf, json!({"__name__":"a","values":1,"host":"x"})).labels["instance"],
        "x"
    );
}

#[test]
fn per_metric_time_field_and_native_record_time() {
    let mut conf = config();
    conf.time_format = Some("%Y-%m-%d %H:%M:%S%.f".into());
    conf.time_zone = Some("+08:00".into());
    conf.metrics.push(MetricRule {
        input: "a".into(),
        name: Some("renamed".into()),
        value: Some("/sample/value".into()),
        time: Some("when".into()),
        help: Some("description".into()),
        ..Default::default()
    });
    let mut nested = ObjectValue::new();
    nested.insert("value", DataField::from_float("value", 2.5));
    let mut record = DataRecord::default();
    record.append(DataField::from_chars("__name__", "a"));
    record.append(DataField::from_obj("sample", nested));
    record.append(DataField::from_time(
        "when",
        chrono::NaiveDate::from_ymd_opt(2020, 1, 1)
            .unwrap()
            .and_hms_opt(8, 0, 0)
            .unwrap(),
    ));
    let sample = first_data_record_update(&Mapper::new(&conf).unwrap(), &record);
    assert_eq!(sample.name, "renamed");
    assert_eq!(sample.value, 2.5);
    assert_eq!(sample.timestamp, Some(1_577_836_800_000));
    assert_eq!(sample.help.as_deref(), Some("description"));
}

#[test]
fn compatibility_rules_cover_all_fifteen_business_metrics() {
    let conf = rules();
    assert_eq!(conf.metrics.len(), 15);
    let mapper = Mapper::new(&conf).unwrap();
    let cases = [
        (
            "Pick",
            "pick_stat",
            "wparse_receive_data",
            MetricKind::Counter,
        ),
        (
            "Parse",
            "parse_stat",
            "wparse_parse_all",
            MetricKind::Counter,
        ),
        (
            "Sink",
            "sink_stat",
            "wparse_send_to_sink",
            MetricKind::Counter,
        ),
        (
            "Receiver",
            "rows_total",
            "wf_receive_total",
            MetricKind::Counter,
        ),
        (
            "Router",
            "route_errors_total",
            "wf_route_errors_total",
            MetricKind::Counter,
        ),
        ("Window", "rows", "wf_window_rows_total", MetricKind::Gauge),
        (
            "Window",
            "memory_bytes",
            "wf_window_memory_bytes",
            MetricKind::Gauge,
        ),
        (
            "Window",
            "window_capacity_bytes",
            "wf_window_memory_capacity_bytes",
            MetricKind::Gauge,
        ),
        (
            "Window",
            "late_total",
            "wf_window_late_total",
            MetricKind::Counter,
        ),
        (
            "Rule",
            "events_total",
            "wf_rule_events_total",
            MetricKind::Counter,
        ),
        (
            "Rule",
            "matches_total",
            "wf_rule_matches_total",
            MetricKind::Counter,
        ),
        (
            "Rule",
            "instances",
            "wf_rule_instances_total",
            MetricKind::Gauge,
        ),
        (
            "Event",
            "e2e_latency_seconds_p99",
            "wf_event_e2e_latency_second_p99",
            MetricKind::Gauge,
        ),
        (
            "Alert",
            "emitted_total",
            "wf_alert_emitted_total",
            MetricKind::Counter,
        ),
        (
            "Alert",
            "sink_dispatch_failed_total",
            "wf_alert_dispatch_failed_total",
            MetricKind::Counter,
        ),
    ];
    for (stage, name, output, kind) in cases {
        let input = json!({"stage":stage,"name":name,"value":7,"total":3,"success":2,"label":"business",
            "wp_source_type":"kafka","wp_access_ip":"node","wp_package_name":"pkg","wp_rule_name":"rule","wp_sink_group":"group","wp_sink_name":"sink",
            "machine":"machine","scope_key":"-"});
        let sample = first_update(&mapper, &input);
        assert_eq!(sample.name, output);
        assert_eq!(sample.kind, kind);
        assert!(sample.help.is_some());
        assert_eq!(
            sample.value,
            if stage == "Pick" || stage == "Parse" {
                3.0
            } else if stage == "Sink" {
                2.0
            } else {
                7.0
            }
        );
        assert!(!sample.labels.contains_key("tenant_id"));
        if stage == "Alert" && name == "emitted_total" {
            assert!(sample.labels.contains_key("machine_name"));
            assert_eq!(sample.labels["scope_key"], "-");
        }
    }
    assert!(
        map_json_record(
            &mapper,
            &json!({"stage":"Pick","name":"parse_stat","total":3})
        )
        .unwrap()
        .is_empty()
    );
    assert!(
        map_json_record(&mapper, &json!({"stage":"Alloc","name":"alloc_stat"}))
            .unwrap()
            .is_empty()
    );
    assert!(
        map_json_record(&mapper, &json!({"stage":"Pick"}))
            .unwrap()
            .is_empty()
    );
    assert!(
        map_json_record(&mapper, &json!({"name":"pick_stat"}))
            .unwrap()
            .is_empty()
    );
    assert!(
        conf.metrics
            .iter()
            .all(|rule| !rule.name.as_deref().unwrap().contains("cpu_usage"))
    );
}

#[test]
fn timestamps_units_formats_offsets_and_errors() {
    for (unit, value) in [
        (TimeUnit::S, 1),
        (TimeUnit::Ms, 1000),
        (TimeUnit::Us, 1_000_000),
        (TimeUnit::Ns, 1_000_000_000),
    ] {
        let conf = VictoriaMetric {
            time_unit: unit,
            ..timed()
        };
        assert_eq!(
            update(&conf, json!({"__name__":"a","values":1,"ts":value})).timestamp,
            Some(1000)
        );
        assert_eq!(
            update(
                &conf,
                json!({"__name__":"a","values":1,"ts":value.to_string()})
            )
            .timestamp,
            Some(1000)
        );
    }
    let conf = VictoriaMetric {
        time_format: Some("%Y-%m-%d %H:%M:%S%.f".into()),
        time_zone: Some("+08:00".into()),
        ..timed()
    };
    assert_eq!(
        update(
            &conf,
            json!({"__name__":"a","values":1,"ts":"2020-01-01 08:00:00.123456789"})
        )
        .timestamp,
        Some(1_577_836_800_123)
    );
    let conf = VictoriaMetric {
        time_format: Some("%Y-%m-%dT%H:%M:%S%:z".into()),
        ..timed()
    };
    assert_eq!(
        update(
            &conf,
            json!({"__name__":"a","values":1,"ts":"2020-01-01T08:00:00+08:00"})
        )
        .timestamp,
        Some(1_577_836_800_000)
    );
    let permissive = VictoriaMetric {
        time_format: Some("%Y-%m-%dT%H:%M:%S%#z".into()),
        ..timed()
    };
    assert_eq!(
        update(
            &permissive,
            json!({"__name__":"a","values":1,"ts":"2020-01-01T08:00:00+08"})
        )
        .timestamp,
        Some(1_577_836_800_000)
    );
    for ts in [
        json!(0),
        json!(1.5),
        json!("bad"),
        json!("999999999999999999999999999999999999999"),
        Value::Null,
    ] {
        assert!(
            map_json_record(
                &Mapper::new(&timed()).unwrap(),
                &json!({"__name__":"a","values":1,"ts":ts})
            )
            .is_err()
        );
    }
    assert!(
        map_json_record(
            &Mapper::new(&timed()).unwrap(),
            &json!({"__name__":"a","values":1})
        )
        .is_err()
    );
    assert!(
        map_json_record(
            &Mapper::new(&conf).unwrap(),
            &json!({"__name__":"a","values":1,"ts":"invalid"})
        )
        .is_err()
    );
}

#[test]
fn data_record_mapping_reads_first_duplicate_field() {
    let mut record = DataRecord::default();
    record.append(DataField::from_chars("__name__", "load"));
    record.append(DataField::from_float("values", 1.25));
    record.append(DataField::from_bool("up", true));
    let mut nested = ObjectValue::new();
    nested.insert("value", DataField::from_int("value", 3));
    record.append(DataField::from_obj("nested", nested));
    let mapper = Mapper::new(&config()).unwrap();
    let sample = first_data_record_update(&mapper, &record);
    assert_eq!(sample.name, "load");
    assert_eq!(sample.value, 1.25);
    record.append(DataField::from_chars("__name__", "duplicate"));
    let sample = first_data_record_update(&mapper, &record);
    assert_eq!(sample.name, "load");
    let mut record = DataRecord::default();
    record.append(DataField::from_float("values", f64::NAN));
    record.append(DataField::from_chars("__name__", "load"));
    assert!(map_data_record(&mapper, &record).is_err());
}

#[tokio::test]
async fn gauge_and_counter_state_semantics() {
    let mut sink = make_sink(config());
    sink.sink_str(r#"[{"__name__":"load","values":12},{"__name__":"load","values":8}]"#)
        .await
        .unwrap();
    assert_eq!(snapshots(&sink)[0].value, 8.0);
    let mut sink = make_sink(counter());
    sink.sink_str(r#"[{"__name__":"requests","values":3},{"__name__":"requests","values":5}]"#)
        .await
        .unwrap();
    assert_eq!(snapshots(&sink)[0].value, 8.0);
    let before = snapshots(&sink)[0].value;
    for value in ["-1", "NaN", "inf", "bad"] {
        let input = json!({"__name__":"requests","values":value}).to_string();
        assert!(sink.sink_str(&input).await.is_err());
        assert_eq!(snapshots(&sink)[0].value, before);
    }
}

#[tokio::test]
async fn batches_fail_atomically_for_every_input_interface() {
    let mut sink = make_sink(config());
    assert!(
        sink.sink_str(r#"[{"__name__":"a","values":1},{"__name__":"b","values":"bad"}]"#)
            .await
            .is_err()
    );
    assert!(snapshots(&sink).is_empty());
    assert!(
        sink.sink_str_batch(vec![r#"{"__name__":"a","values":1}"#, "bad-json"])
            .await
            .is_err()
    );
    assert!(
        sink.sink_bytes_batch(vec![br#"{"__name__":"a","values":1}"#, b"[] trailing"])
            .await
            .is_err()
    );
    assert!(sink.sink_bytes(&[0xff]).await.is_err());
    assert!(
        sink.sink_str(r#"[{"__name__":"a","values":1},2]"#)
            .await
            .is_err()
    );
    assert!(snapshots(&sink).is_empty());
    let mut valid = DataRecord::default();
    valid.append(DataField::from_chars("__name__", "a"));
    valid.append(DataField::from_int("values", 1));
    let mut duplicate = valid.clone();
    duplicate.append(DataField::from_chars("__name__", "duplicate"));
    sink.sink_records(vec![Arc::new(valid.clone()), Arc::new(duplicate)])
        .await
        .unwrap();
    assert_eq!(snapshots(&sink).len(), 1);
    sink.sink_record(&valid).await.unwrap();
    assert_eq!(snapshots(&sink).len(), 1);
}

#[tokio::test]
async fn different_label_sets_coexist_and_snapshots_keep_latest_value() {
    let mut conf = config();
    conf.keep_labels = None;
    let mut sink = make_sink(conf);
    sink.sink_str(r#"{"__name__":"a","values":1,"host":"x"}"#)
        .await
        .unwrap();
    sink.sink_str(
        r#"[{"__name__":"a","values":3,"host":"x","region":"cn"},{"__name__":"a","values":4,"host":"x","region":""}]"#,
    )
    .await
    .unwrap();
    let series = snapshots(&sink);
    assert_eq!(series.len(), 2);
    assert!(
        series
            .iter()
            .any(|sample| sample.value == 3.0 && sample.labels["region"] == "cn")
    );
    assert!(
        series
            .iter()
            .any(|sample| sample.value == 4.0 && !sample.labels.contains_key("region"))
    );
    let mut sink = make_sink(timed());
    sink.sink_str(
        r#"[{"__name__":"a","values":1,"ts":1000},{"__name__":"a","values":2,"ts":1000}]"#,
    )
    .await
    .unwrap();
    assert_eq!(snapshots(&sink)[0].value, 2.0);
    assert_eq!(snapshots(&sink)[0].timestamp, Some(1000));
    let metric_store = &sink.runtime().unwrap().metric_store;
    let mut conflict = update(&timed(), json!({"__name__":"a","values":2,"ts":2000}));
    conflict.kind = MetricKind::Counter;
    let other = update(&timed(), json!({"__name__":"b","values":2,"ts":2000}));
    assert!(
        metric_store
            .lock()
            .unwrap()
            .apply(&[other, conflict])
            .is_err()
    );
    assert_eq!(snapshots(&sink).len(), 1);
}

#[tokio::test]
async fn rules_allow_separate_shapes_and_emit_for_each_matching_rule() {
    let conf = spec(json!({
        "mapping": "rules",
        "metrics": [
            {"match": {"/a": 1, "/b": 2}, "name": "requests_total",
             "value": "/left", "type": "counter", "labels": {"a": "/a", "b": "/b"}},
            {"match": {"/c": 1, "/d": 1}, "name": "requests_total",
             "value": "/right", "type": "counter", "labels": {"c": "/c", "d": "/d"}}
        ]
    }));
    let mut sink = make_sink(VictoriaMetric::from_params(&conf.params).unwrap());
    sink.sink_str(r#"[{"a":1,"b":2,"left":3},{"c":1,"d":1,"right":5}]"#)
        .await
        .unwrap();
    sink.sink_str(r#"[{"a":1,"b":2,"left":7},{"a":1,"b":2,"c":1,"d":1,"left":1,"right":1},{"c":1,"d":1,"right":2}]"#)
        .await
        .unwrap();
    let series = snapshots(&sink);
    assert_eq!(series.len(), 2);
    assert!(
        series
            .iter()
            .any(|sample| sample.labels.contains_key("a") && sample.value == 11.0)
    );
    assert!(
        series
            .iter()
            .any(|sample| sample.labels.contains_key("c") && sample.value == 8.0)
    );
}

#[tokio::test]
async fn counter_overflow_uses_prometheus_behavior() {
    let mut sink = make_sink(counter());
    sink.sink_str(&json!({"__name__":"requests","values":f64::MAX}).to_string())
        .await
        .unwrap();
    sink.sink_str(&json!({"__name__":"requests","values":f64::MAX}).to_string())
        .await
        .unwrap();
    assert_eq!(snapshots(&sink)[0].value, f64::INFINITY);
}

#[test]
fn registry_snapshot_encoding_preserves_schema_labels_and_latest_timestamp() {
    let mut conf = timed();
    conf.keep_labels = Some(vec!["host".into()]);
    let mut first = update(
        &conf,
        json!({"__name__":"sample","values":3,"ts":1000,"host":"line\n\"\\"}),
    );
    first.help = Some("help\ntext".into());
    let mut second = first.clone();
    second.timestamp = Some(2000);
    second.value = 7.0;
    let metric_store = SharedMetricStore::new();
    metric_store
        .lock()
        .unwrap()
        .apply(&[first.clone(), second.clone()])
        .unwrap();
    let families = metric_store.lock().unwrap().gather();
    let text = String::from_utf8(encode(&families).unwrap()).unwrap();
    assert!(text.contains("# HELP sample help\\ntext\n# TYPE sample gauge\n"));
    assert!(text.contains("host=\"line\\n\\\"\\\\\""));
    assert!(text.contains(" 7 2000\n"));
    assert!(!text.contains(" 3 1000\n"));
    let counter_shared = SharedMetricStore::new();
    counter_shared
        .lock()
        .unwrap()
        .apply(&[update(
            &counter(),
            json!({"__name__":"requests","values":1.5}),
        )])
        .unwrap();
    let text =
        String::from_utf8(encode(&counter_shared.lock().unwrap().gather()).unwrap()).unwrap();
    assert!(text.contains("# TYPE http_requests_total counter"));
    assert!(!text.contains("# HELP"));
}

#[tokio::test]
async fn same_name_shares_metrics_and_transport_config_controls_runtime_reuse() {
    let conf = config();
    let mut a = named(conf.clone());
    let name = RUNTIMES
        .lock()
        .unwrap()
        .iter()
        .find(|(_, named)| {
            named.runtimes.iter().any(|weak| {
                weak.upgrade()
                    .is_some_and(|runtime| Arc::ptr_eq(&runtime, a.runtime.as_ref().unwrap()))
            })
        })
        .unwrap()
        .0
        .clone();
    let mut b = VictoriaMetricExporter::for_name(&name, conf.clone()).unwrap();
    let mut other = named(conf.clone());
    assert!(Arc::ptr_eq(
        a.runtime.as_ref().unwrap(),
        b.runtime.as_ref().unwrap()
    ));
    assert_eq!(
        a.runtime().unwrap().flush_task.as_ref().unwrap().id(),
        b.runtime().unwrap().flush_task.as_ref().unwrap().id()
    );
    a.sink_str(r#"{"__name__":"a","values":3}"#).await.unwrap();
    other
        .sink_str(r#"{"__name__":"a","values":7}"#)
        .await
        .unwrap();
    assert_eq!(snapshots(&b)[0].value, 3.0);
    assert_eq!(snapshots(&other)[0].value, 7.0);
    let changed_sinks: Vec<_> = [
        VictoriaMetric {
            flush_secs: 2.0,
            ..conf.clone()
        },
        VictoriaMetric {
            timeout_secs: 2.0,
            ..conf.clone()
        },
        VictoriaMetric {
            endpoint: "http://127.0.0.1:8429".into(),
            ..conf.clone()
        },
    ]
    .into_iter()
    .map(|changed| VictoriaMetricExporter::for_name(&name, changed).unwrap())
    .collect();
    for changed in changed_sinks {
        assert!(!Arc::ptr_eq(
            changed.runtime.as_ref().unwrap(),
            a.runtime.as_ref().unwrap()
        ));
        assert!(Arc::ptr_eq(
            &changed.runtime().unwrap().metric_store,
            &a.runtime().unwrap().metric_store
        ));
    }
    a.stop().await.unwrap();
    b.sink_str(r#"{"__name__":"a","values":5}"#).await.unwrap();
    assert_eq!(snapshots(&b)[0].value, 5.0);
    let weak = Arc::downgrade(b.runtime.as_ref().unwrap());
    let abort = b
        .runtime()
        .unwrap()
        .flush_task
        .as_ref()
        .unwrap()
        .abort_handle();
    drop(b);
    tokio::task::yield_now().await;
    assert!(weak.upgrade().is_none());
    assert!(abort.is_finished());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_builds_share_one_runtime() {
    let name = format!("concurrent-{}", std::process::id());
    let tasks: Vec<_> = (0..8)
        .map(|_| {
            let name = name.clone();
            tokio::spawn(async move { VictoriaMetricExporter::for_name(&name, config()).unwrap() })
        })
        .collect();
    let mut sinks = Vec::new();
    for task in tasks {
        sinks.push(task.await.unwrap());
    }
    for sink in &sinks[1..] {
        assert!(Arc::ptr_eq(
            sinks[0].runtime.as_ref().unwrap(),
            sink.runtime.as_ref().unwrap()
        ));
    }
}

#[tokio::test]
async fn successful_flush_repeats_the_current_snapshot_and_preserves_query() {
    let server = httpmock::MockServer::start_async().await;
    let sent = server
        .mock_async(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/metrics")
                .query_param("extra_label", "source=test")
                .query_param_missing("time_stamp");
            then.status(204);
        })
        .await;
    let conf = VictoriaMetric {
        endpoint: server.base_url(),
        api_path: "/metrics?extra_label=source%3Dtest".into(),
        ..timed()
    };
    let mut sink = make_sink(conf.clone());
    sink.sink_str(r#"[{"__name__":"a","values":1,"ts":1},{"__name__":"a","values":2,"ts":2}]"#)
        .await
        .unwrap();
    let runtime = sink.runtime().unwrap();
    flush(
        &runtime.metric_store,
        &runtime.http_client,
        &SharedSinkConfig::from(&conf),
    )
    .await
    .unwrap();
    assert_eq!(snapshots(&sink).len(), 1);
    flush(
        &runtime.metric_store,
        &runtime.http_client,
        &SharedSinkConfig::from(&conf),
    )
    .await
    .unwrap();
    sent.assert_calls_async(2).await;
    let conf = VictoriaMetric {
        endpoint: server.base_url(),
        api_path: conf.api_path.clone(),
        ..config()
    };
    let mut sink = make_sink(conf.clone());
    sink.sink_str(r#"{"__name__":"a","values":1}"#)
        .await
        .unwrap();
    sink.sink_str(r#"{"__name__":"b","values":2}"#)
        .await
        .unwrap();
    let runtime = sink.runtime().unwrap();
    flush(
        &runtime.metric_store,
        &runtime.http_client,
        &SharedSinkConfig::from(&conf),
    )
    .await
    .unwrap();
    assert_eq!(snapshots(&sink).len(), 2);
    sent.assert_calls_async(3).await;
}

#[tokio::test]
async fn http_failures_keep_registry_state_for_retry() {
    let server = httpmock::MockServer::start_async().await;
    let failed = server
        .mock_async(|when, then| {
            when.path("/metrics");
            then.status(503).body("unavailable");
        })
        .await;
    let conf = VictoriaMetric {
        endpoint: server.base_url(),
        api_path: "/metrics".into(),
        ..timed()
    };
    let mut sink = make_sink(conf.clone());
    sink.sink_str(r#"{"__name__":"a","values":1,"ts":1000}"#)
        .await
        .unwrap();
    let runtime = sink.runtime().unwrap();
    let error = flush(
        &runtime.metric_store,
        &runtime.http_client,
        &SharedSinkConfig::from(&conf),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("503"));
    assert!(error.to_string().contains("unavailable"));
    assert_eq!(snapshots(&sink).len(), 1);
    failed.delete_async().await;
    let sent = server
        .mock_async(|when, then| {
            when.path("/metrics").body_includes("a 1 1000\n");
            then.status(204);
        })
        .await;
    flush(
        &runtime.metric_store,
        &runtime.http_client,
        &SharedSinkConfig::from(&conf),
    )
    .await
    .unwrap();
    assert_eq!(snapshots(&sink).len(), 1);
    sent.assert_calls_async(1).await;
}

#[tokio::test]
async fn in_flight_http_success_does_not_delete_new_writes() {
    let server = httpmock::MockServer::start_async().await;
    let sent = server
        .mock_async(|when, then| {
            when.path("/metrics").body_includes("a 1 1\n");
            then.status(204).delay(Duration::from_millis(100));
        })
        .await;
    let conf = VictoriaMetric {
        endpoint: server.base_url(),
        api_path: "/metrics".into(),
        ..timed()
    };
    let mut sink = make_sink(conf.clone());
    sink.sink_str(r#"{"__name__":"a","values":1,"ts":1}"#)
        .await
        .unwrap();
    let metric_store = sink.runtime().unwrap().metric_store.clone();
    let client = sink.runtime().unwrap().http_client.clone();
    let sending =
        tokio::spawn(
            async move { flush(&metric_store, &client, &SharedSinkConfig::from(&conf)).await },
        );
    tokio::time::timeout(Duration::from_secs(2), async {
        while sent.calls_async().await == 0 {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    sink.sink_str(r#"{"__name__":"a","values":2,"ts":2}"#)
        .await
        .unwrap();
    sending.await.unwrap().unwrap();
    assert_eq!(snapshots(&sink).len(), 1);
    assert_eq!(snapshots(&sink)[0].timestamp, Some(2));
}

#[tokio::test]
async fn final_stop_is_shared_and_reports_http_failure() {
    let server = httpmock::MockServer::start_async().await;
    let sent = server
        .mock_async(|when, then| {
            when.path("/metrics");
            then.status(204);
        })
        .await;
    let conf = VictoriaMetric {
        endpoint: server.base_url(),
        api_path: "/metrics".into(),
        ..config()
    };
    let mut first = named(conf.clone());
    let mut last = VictoriaMetricExporter {
        runtime: first.runtime.clone(),
        mapper: Mapper::new(&conf).unwrap(),
    };
    first
        .sink_str(r#"{"__name__":"a","values":3}"#)
        .await
        .unwrap();
    first.stop().await.unwrap();
    last.stop().await.unwrap();
    sent.assert_calls_async(1).await;
    last.stop().await.unwrap();
    assert!(
        last.sink_str(r#"{"__name__":"a","values":2}"#)
            .await
            .is_err()
    );
    let server = httpmock::MockServer::start_async().await;
    server
        .mock_async(|when, then| {
            when.any_request();
            then.status(500).body("failed");
        })
        .await;
    let mut sink = make_sink(VictoriaMetric {
        endpoint: server.base_url(),
        ..config()
    });
    sink.sink_str(r#"{"__name__":"a","values":1}"#)
        .await
        .unwrap();
    sink.sink_str(r#"{"__name__":"b","values":2}"#)
        .await
        .unwrap();
    let error = sink.stop().await.unwrap_err().to_string();
    assert!(error.contains("最终快照推送失败"), "{error}");
    assert!(error.contains("500"));
}

#[tokio::test]
async fn request_timeout_keeps_samples_and_final_stop_is_bounded() {
    let server = httpmock::MockServer::start_async().await;
    server
        .mock_async(|when, then| {
            when.any_request();
            then.status(204).delay(Duration::from_millis(200));
        })
        .await;
    let conf = VictoriaMetric {
        endpoint: server.base_url(),
        timeout_secs: 0.02,
        ..timed()
    };
    let mut sink = make_sink(conf.clone());
    sink.sink_str(r#"{"__name__":"a","values":1,"ts":1000}"#)
        .await
        .unwrap();
    let runtime = sink.runtime().unwrap();
    assert!(
        flush(
            &runtime.metric_store,
            &runtime.http_client,
            &SharedSinkConfig::from(&conf),
        )
        .await
        .is_err()
    );
    assert_eq!(snapshots(&sink).len(), 1);
    let error = tokio::time::timeout(Duration::from_secs(1), sink.stop())
        .await
        .unwrap()
        .unwrap_err()
        .to_string();
    assert!(error.contains("最终快照推送失败"));
}

#[tokio::test]
async fn periodic_worker_retries_registry_snapshot_after_failure() {
    let server = httpmock::MockServer::start_async().await;
    let failed = server
        .mock_async(|when, then| {
            when.any_request();
            then.status(503);
        })
        .await;
    let mut sink = named(VictoriaMetric {
        endpoint: server.base_url(),
        flush_secs: 0.01,
        ..timed()
    });
    sink.sink_str(r#"{"__name__":"a","values":1,"ts":1000}"#)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while failed.calls_async().await == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(snapshots(&sink).len(), 1);
    failed.delete_async().await;
    let sent = server
        .mock_async(|when, then| {
            when.any_request();
            then.status(204);
        })
        .await;
    tokio::time::timeout(Duration::from_secs(3), async {
        while sent.calls_async().await == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(snapshots(&sink).len(), 1);
    sink.stop().await.unwrap();
}
