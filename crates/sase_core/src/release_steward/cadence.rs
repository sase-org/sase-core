//! Daily merge cadence: after 09:00 UTC, at most one merge per UTC day
//! unless that day's candidate was abandoned.

use chrono::{DateTime, Timelike, Utc};

use super::facts::MergeEventWire;
use super::wire::{CadencePolicyWire, ReleaseStewardError};

pub const DAILY_CADENCE_KIND: &str = "daily";

pub fn parse_observed_at(
    value: &str,
) -> Result<DateTime<Utc>, ReleaseStewardError> {
    DateTime::parse_from_rfc3339(value)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|error| {
            ReleaseStewardError::invalid(format!(
                "observed_at is not RFC 3339 UTC: {error}"
            ))
        })
}

pub fn validate_cadence(
    cadence: &CadencePolicyWire,
) -> Result<(), ReleaseStewardError> {
    if cadence.kind != DAILY_CADENCE_KIND {
        return Err(ReleaseStewardError::invalid(format!(
            "rejected cadence kind {:?}; only {DAILY_CADENCE_KIND} is accepted",
            cadence.kind
        )));
    }
    if cadence.merge_after_hour_utc > 23 {
        return Err(ReleaseStewardError::invalid(
            "cadence.merge_after_hour_utc must be 0..=23",
        ));
    }
    if cadence.max_merges_per_utc_day == 0 {
        return Err(ReleaseStewardError::invalid(
            "cadence.max_merges_per_utc_day must be at least 1",
        ));
    }
    Ok(())
}

/// Why a merge is blocked by cadence, or `None` when it is permitted.
pub fn cadence_block_reason(
    observed_at: DateTime<Utc>,
    cadence: &CadencePolicyWire,
    merges: &[MergeEventWire],
) -> Result<Option<String>, ReleaseStewardError> {
    validate_cadence(cadence)?;
    if observed_at.hour() < cadence.merge_after_hour_utc {
        return Ok(Some(format!(
            "before {:02}:00 UTC",
            cadence.merge_after_hour_utc
        )));
    }
    let today = observed_at.date_naive();
    let mut used = 0u32;
    for merge in merges {
        let merged_at = parse_observed_at(&merge.merged_at)?;
        if merged_at.date_naive() != today {
            continue;
        }
        if merge.abandoned_after {
            continue;
        }
        used = used.saturating_add(1);
    }
    if used >= cadence.max_merges_per_utc_day {
        return Ok(Some(
            "daily merge already used for this UTC day".to_string(),
        ));
    }
    Ok(None)
}
