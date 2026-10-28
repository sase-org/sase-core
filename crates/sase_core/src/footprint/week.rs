//! ISO-week helpers for Footprint export packs.

use std::sync::LazyLock;

use chrono::{Datelike, Duration as ChronoDuration, NaiveDate, Weekday};
use regex::Regex;

use super::wire::ISO_WEEK_PATTERN;
use super::FootprintError;

static WEEK_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(ISO_WEEK_PATTERN).expect("iso week regex"));

/// Monday and Sunday of the ISO week that contains `day`.
pub(crate) fn week_bounds(
    day: NaiveDate,
) -> Result<(NaiveDate, NaiveDate), FootprintError> {
    let iso = day.iso_week();
    week_bounds_of(iso.year(), iso.week())
}

/// Monday and Sunday of `YYYY-Www`.
pub(crate) fn parse_week(
    label: &str,
) -> Result<(NaiveDate, NaiveDate), FootprintError> {
    if !WEEK_RE.is_match(label) {
        return Err(FootprintError::validation(format!(
            "invalid ISO week {label:?}"
        )));
    }
    let (year_s, week_s) = label.split_once("-W").ok_or_else(|| {
        FootprintError::validation(format!("invalid ISO week {label:?}"))
    })?;
    let year: i32 = year_s.parse().map_err(|_| {
        FootprintError::validation(format!("invalid ISO week {label:?}"))
    })?;
    let week: u32 = week_s.parse().map_err(|_| {
        FootprintError::validation(format!("invalid ISO week {label:?}"))
    })?;
    week_bounds_of(year, week)
}

fn week_bounds_of(
    year: i32,
    week: u32,
) -> Result<(NaiveDate, NaiveDate), FootprintError> {
    let start = NaiveDate::from_isoywd_opt(year, week, Weekday::Mon)
        .ok_or_else(|| {
            FootprintError::validation(format!(
                "invalid ISO week {year:04}-W{week:02}"
            ))
        })?;
    let end = NaiveDate::from_isoywd_opt(year, week, Weekday::Sun).ok_or_else(
        || {
            FootprintError::validation(format!(
                "invalid ISO week {year:04}-W{week:02}"
            ))
        },
    )?;
    Ok((start, end))
}

/// Format `day` as `YYYY-Www`.
pub(crate) fn week_label(day: NaiveDate) -> String {
    let iso = day.iso_week();
    format!("{:04}-W{:02}", iso.year(), iso.week())
}

/// True when `[week_start, week_end]` overlaps `[window_start, today]`.
pub(crate) fn week_overlaps_window(
    week_start: NaiveDate,
    week_end: NaiveDate,
    window_start: NaiveDate,
    today: NaiveDate,
) -> bool {
    week_end >= window_start && week_start <= today
}

/// ISO week labels overlapping `[window_start, today]`.
pub(crate) fn weeks_in_window(
    window_start: NaiveDate,
    today: NaiveDate,
) -> Vec<(String, NaiveDate, NaiveDate)> {
    if today < window_start {
        return Vec::new();
    }
    let mut weeks = Vec::new();
    let mut seen = std::collections::BTreeSet::new();
    let mut day = window_start;
    while day <= today {
        if let Ok((start, end)) = week_bounds(day) {
            let label = week_label(day);
            if seen.insert(label.clone()) {
                weeks.push((label, start, end));
            }
            day = end
                .checked_add_signed(ChronoDuration::days(1))
                .unwrap_or(today);
            if day == end {
                break;
            }
            continue;
        }
        break;
    }
    weeks
}
