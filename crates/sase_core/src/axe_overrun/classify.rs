use chrono::{DateTime, FixedOffset};

use super::wire::{
    ChopOverrunError, ChopOverrunLevelWire, ChopOverrunRequestWire,
    ChopOverrunRunEntryWire, ChopOverrunWire, CHOP_OVERRUN_SCHEMA_VERSION,
};

/// Outcomes whose near-zero durations reflect the script never running, not
/// how long it blocked the tick.
const EXCLUDED_STATUSES: [&str; 3] =
    ["skipped", "missing_script", "check_error"];

/// Outcomes whose duration is a direct measure of blocking time (subject to
/// the `script_duration_ms` override applied uniformly below).
const DIRECT_STATUSES: [&str; 6] = [
    "success", "failure", "timeout", "no_op", "running", "launched",
];

#[derive(Clone, Copy)]
struct Sample {
    blocking_ms: i64,
    ratio: f64,
    over: bool,
}

/// Classify one chop's cached run history against its lumberjack's interval.
///
/// This operation performs no filesystem, clock, or process I/O.
pub fn classify_chop_overrun(
    request: &ChopOverrunRequestWire,
) -> Result<ChopOverrunWire, ChopOverrunError> {
    validate_request(request)?;

    let now = DateTime::parse_from_rfc3339(request.now.trim())
        .expect("validated as a parsable RFC-3339 timestamp above");
    let interval_ms = request.interval_seconds * 1000;

    let per_run_samples = request
        .runs
        .iter()
        .map(|run| {
            sampled_blocking_ms(run, now).map(|blocking_ms| Sample {
                blocking_ms,
                ratio: blocking_ms as f64 / interval_ms as f64,
                over: blocking_ms >= interval_ms,
            })
        })
        .collect::<Vec<_>>();

    let run_ratios = per_run_samples
        .iter()
        .map(|sample| sample.map(|sample| sample.ratio))
        .collect::<Vec<_>>();

    let samples = per_run_samples.into_iter().flatten().collect::<Vec<_>>();

    let sampled_runs = samples.len() as u32;
    let over_runs = samples.iter().filter(|sample| sample.over).count() as u32;

    let level = if samples.is_empty() {
        ChopOverrunLevelWire::None
    } else if samples[0].over {
        ChopOverrunLevelWire::Over
    } else if samples.iter().any(|sample| sample.over) {
        ChopOverrunLevelWire::Intermittent
    } else {
        ChopOverrunLevelWire::None
    };

    let worst = samples
        .iter()
        .max_by(|left, right| left.blocking_ms.cmp(&right.blocking_ms));

    Ok(ChopOverrunWire {
        schema_version: CHOP_OVERRUN_SCHEMA_VERSION,
        level,
        sampled_runs,
        over_runs,
        worst_ratio: worst.map(|sample| sample.ratio),
        worst_blocking_ms: worst.map(|sample| sample.blocking_ms),
        latest_ratio: samples.first().map(|sample| sample.ratio),
        run_ratios,
    })
}

fn validate_request(
    request: &ChopOverrunRequestWire,
) -> Result<(), ChopOverrunError> {
    if request.schema_version != CHOP_OVERRUN_SCHEMA_VERSION {
        return Err(ChopOverrunError::new(
            "schema_version_mismatch",
            "$.schema_version",
            format!(
                "got {}, expected {CHOP_OVERRUN_SCHEMA_VERSION}",
                request.schema_version
            ),
        ));
    }
    if request.now.trim().is_empty() {
        return Err(ChopOverrunError::new(
            "blank_value",
            "$.now",
            "now must not be blank",
        ));
    }
    if DateTime::parse_from_rfc3339(request.now.trim()).is_err() {
        return Err(ChopOverrunError::new(
            "invalid_timestamp",
            "$.now",
            format!("now must be an RFC-3339 timestamp: {:?}", request.now),
        ));
    }
    if request.interval_seconds <= 0 {
        return Err(ChopOverrunError::new(
            "non_positive_interval",
            "$.interval_seconds",
            "interval_seconds must be positive",
        ));
    }
    Ok(())
}

/// Derive one run's blocking time in milliseconds, or `None` when the run is
/// not sampleable. Per-run defects (an unparsable `started_at`, a negative
/// duration, or an unknown status string) drop the run silently rather than
/// failing the whole request.
fn sampled_blocking_ms(
    run: &ChopOverrunRunEntryWire,
    now: DateTime<FixedOffset>,
) -> Option<i64> {
    if EXCLUDED_STATUSES.contains(&run.status.as_str()) {
        return None;
    }
    if !is_known_sampleable_status(&run.status) {
        return None;
    }

    // Every otherwise-sampleable run must carry a parsable `started_at`,
    // even when its blocking time is ultimately derived from a duration
    // field rather than from `started_at` itself.
    let started = DateTime::parse_from_rfc3339(run.started_at.trim()).ok()?;

    let blocking_ms = if let Some(script_ms) = run.script_duration_ms {
        script_ms
    } else if run.status == "running" {
        now.signed_duration_since(started).num_milliseconds()
    } else if is_action_status(&run.status) {
        // A launched-run finalization overwrites `duration_ms` with the
        // agents' lifetime, which the lumberjack tick never waited for.
        // Without a preserved `script_duration_ms` there is no reliable
        // blocking time to sample.
        return None;
    } else {
        run.duration_ms?
    };

    (blocking_ms >= 0).then_some(blocking_ms)
}

fn is_action_status(status: &str) -> bool {
    status.starts_with("action_")
}

fn is_known_sampleable_status(status: &str) -> bool {
    DIRECT_STATUSES.contains(&status) || is_action_status(status)
}
