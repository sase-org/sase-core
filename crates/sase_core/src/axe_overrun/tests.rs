use serde_json::json;

use super::*;

fn request() -> ChopOverrunRequestWire {
    ChopOverrunRequestWire {
        schema_version: CHOP_OVERRUN_SCHEMA_VERSION,
        now: "2026-08-12T09:15:00-04:00".to_string(),
        interval_seconds: 60,
        runs: Vec::new(),
    }
}

fn run(
    status: &str,
    started_at: &str,
    duration_ms: Option<i64>,
    script_duration_ms: Option<i64>,
) -> ChopOverrunRunEntryWire {
    ChopOverrunRunEntryWire {
        status: status.to_string(),
        started_at: started_at.to_string(),
        duration_ms,
        script_duration_ms,
    }
}

const T: &str = "2026-08-12T09:12:00-04:00";

#[test]
fn newest_sampled_run_over_is_over() {
    let mut input = request();
    input.runs = vec![run("success", T, Some(65_000), None)];
    let verdict = classify_chop_overrun(&input).unwrap();
    assert_eq!(verdict.level, ChopOverrunLevelWire::Over);
    assert_eq!(verdict.sampled_runs, 1);
    assert_eq!(verdict.over_runs, 1);
    assert_eq!(verdict.worst_blocking_ms, Some(65_000));
    assert!((verdict.latest_ratio.unwrap() - 65_000.0 / 60_000.0).abs() < 1e-9);
}

#[test]
fn only_an_older_run_over_is_intermittent() {
    let mut input = request();
    input.runs = vec![
        run("success", T, Some(10_000), None),
        run("success", T, Some(65_000), None),
    ];
    let verdict = classify_chop_overrun(&input).unwrap();
    assert_eq!(verdict.level, ChopOverrunLevelWire::Intermittent);
    assert_eq!(verdict.sampled_runs, 2);
    assert_eq!(verdict.over_runs, 1);
    assert_eq!(verdict.worst_blocking_ms, Some(65_000));
    assert_eq!(verdict.latest_ratio, Some(10_000.0 / 60_000.0));
}

#[test]
fn fast_skipped_newest_run_in_front_of_over_success_is_still_over() {
    let mut input = request();
    input.runs = vec![
        run("skipped", T, Some(1), None),
        run("success", T, Some(65_000), None),
    ];
    let verdict = classify_chop_overrun(&input).unwrap();
    assert_eq!(verdict.level, ChopOverrunLevelWire::Over);
    assert_eq!(verdict.sampled_runs, 1);
    assert_eq!(verdict.over_runs, 1);
}

#[test]
fn action_succeeded_without_script_duration_is_ignored() {
    let mut input = request();
    input.runs = vec![run("action_succeeded", T, Some(9_999_999), None)];
    let verdict = classify_chop_overrun(&input).unwrap();
    assert_eq!(verdict.level, ChopOverrunLevelWire::None);
    assert_eq!(verdict.sampled_runs, 0);
    assert_eq!(verdict.over_runs, 0);
    assert_eq!(verdict.worst_ratio, None);
    assert_eq!(verdict.worst_blocking_ms, None);
    assert_eq!(verdict.latest_ratio, None);
}

#[test]
fn action_succeeded_with_script_duration_is_sampled_on_that_value() {
    let mut input = request();
    input.runs =
        vec![run("action_succeeded", T, Some(9_999_999), Some(65_000))];
    let verdict = classify_chop_overrun(&input).unwrap();
    assert_eq!(verdict.level, ChopOverrunLevelWire::Over);
    assert_eq!(verdict.sampled_runs, 1);
    assert_eq!(verdict.worst_blocking_ms, Some(65_000));
}

#[test]
fn live_running_run_past_interval_is_over_from_elapsed_time() {
    let mut input = request();
    input.runs = vec![run("running", "2026-08-12T09:13:59-04:00", None, None)];
    let verdict = classify_chop_overrun(&input).unwrap();
    assert_eq!(verdict.level, ChopOverrunLevelWire::Over);
    assert_eq!(verdict.sampled_runs, 1);
    assert_eq!(verdict.worst_blocking_ms, Some(61_000));
}

#[test]
fn exact_equality_counts_as_over() {
    let mut input = request();
    input.runs = vec![run("success", T, Some(60_000), None)];
    let verdict = classify_chop_overrun(&input).unwrap();
    assert_eq!(verdict.level, ChopOverrunLevelWire::Over);
    assert_eq!(verdict.over_runs, 1);
}

#[test]
fn non_positive_interval_is_an_error() {
    let mut input = request();
    input.interval_seconds = 0;
    let error = classify_chop_overrun(&input).unwrap_err();
    assert_eq!(error.code, "non_positive_interval");
    assert_eq!(error.path, "$.interval_seconds");
}

#[test]
fn empty_history_is_none_with_null_ratios() {
    let input = request();
    let verdict = classify_chop_overrun(&input).unwrap();
    assert_eq!(verdict.level, ChopOverrunLevelWire::None);
    assert_eq!(verdict.sampled_runs, 0);
    assert_eq!(verdict.over_runs, 0);
    assert_eq!(verdict.worst_ratio, None);
    assert_eq!(verdict.worst_blocking_ms, None);
    assert_eq!(verdict.latest_ratio, None);
}

#[test]
fn unparsable_started_at_is_dropped_not_fatal() {
    let mut input = request();
    input.runs = vec![run("running", "not-a-timestamp", None, None)];
    let verdict = classify_chop_overrun(&input).unwrap();
    assert_eq!(verdict.level, ChopOverrunLevelWire::None);
    assert_eq!(verdict.sampled_runs, 0);
}

#[test]
fn negative_duration_is_dropped_not_fatal() {
    let mut input = request();
    input.runs = vec![run("success", T, Some(-5), None)];
    let verdict = classify_chop_overrun(&input).unwrap();
    assert_eq!(verdict.level, ChopOverrunLevelWire::None);
    assert_eq!(verdict.sampled_runs, 0);
}

#[test]
fn unknown_status_string_is_dropped_not_fatal() {
    let mut input = request();
    input.runs = vec![run("mystery", T, Some(999_999), None)];
    let verdict = classify_chop_overrun(&input).unwrap();
    assert_eq!(verdict.sampled_runs, 0);
}

#[test]
fn only_ten_percent_ratio_is_not_over_and_not_intermittent() {
    let mut input = request();
    input.runs = vec![run("success", T, Some(6_000), None)];
    let verdict = classify_chop_overrun(&input).unwrap();
    assert_eq!(verdict.level, ChopOverrunLevelWire::None);
    assert_eq!(verdict.sampled_runs, 1);
    assert_eq!(verdict.over_runs, 0);
    assert_eq!(verdict.latest_ratio, Some(0.1));
}

#[test]
fn structural_validation_returns_path_specific_errors() {
    let mut input = request();
    input.schema_version += 1;
    let error = classify_chop_overrun(&input).unwrap_err();
    assert_eq!(error.code, "schema_version_mismatch");
    assert_eq!(error.path, "$.schema_version");

    input = request();
    input.now = " ".to_string();
    let error = classify_chop_overrun(&input).unwrap_err();
    assert_eq!(error.code, "blank_value");
    assert_eq!(error.path, "$.now");

    input = request();
    input.now = "not a timestamp".to_string();
    let error = classify_chop_overrun(&input).unwrap_err();
    assert_eq!(error.code, "invalid_timestamp");
    assert_eq!(error.path, "$.now");
}

#[test]
fn serde_rejects_unknown_fields_and_invalid_enum_values() {
    let mut value = serde_json::to_value(request()).unwrap();
    value
        .as_object_mut()
        .unwrap()
        .insert("surprise".to_string(), json!(true));
    let error = serde_json::from_value::<ChopOverrunRequestWire>(value)
        .unwrap_err()
        .to_string();
    assert!(error.contains("unknown field `surprise`"));
}

#[test]
fn serialization_pins_public_field_order_and_round_trips() {
    let mut input = request();
    input.runs = vec![run("success", T, Some(65_000), None)];
    let verdict = classify_chop_overrun(&input).unwrap();
    let value = serde_json::to_value(&verdict).unwrap();
    let keys = value
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect::<Vec<_>>();
    assert_eq!(
        keys,
        vec![
            "schema_version",
            "level",
            "sampled_runs",
            "over_runs",
            "worst_ratio",
            "worst_blocking_ms",
            "latest_ratio",
        ]
    );
    assert_eq!(value["schema_version"], json!(1));
    assert_eq!(value["level"], json!("over"));

    let round_trip: ChopOverrunWire = serde_json::from_value(value).unwrap();
    assert_eq!(round_trip, verdict);
}
