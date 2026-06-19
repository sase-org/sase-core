//! Plan search: substring matching, filtering, and ranking.
//!
//! Builds on the [`super::read`] discovery layer to answer `sase plan search`.
//! Where `bead/search.rs` returns matches in storage order, plan search adds:
//!
//! - an **optional** query — present, it does bead-parity case-insensitive
//!   substring matching across every searchable field; absent (or blank), it
//!   becomes a filter/browse tool listing plans by recency;
//! - **ranking** — field weights (title > frontmatter > body), a small repo
//!   boost so committed `sdd/` plans outrank the local archive on ties, and a
//!   recency tie-break so stale duplicates sink;
//! - extra **filters** — frontmatter `status`, `source` (repo/local), and a
//!   `created_at` date range whose bounds accept `YYYY-MM-DD`, `YYYY-MM` /
//!   `YYYYMM`, and relative `Nd`/`Nw`/`Nm` forms.
//!
//! The module is free of PyO3 types so the binding/TUI/web frontends can reuse
//! it, mirroring the bead-search architecture.

use std::borrow::Cow;
use std::path::Path;
use std::time::SystemTime;

use chrono::{DateTime, Datelike, Duration, NaiveDate, NaiveDateTime, Utc};

use super::read::read_plans;
use super::wire::{PlanError, PlanSearchMatchWire, PlanWire};

pub const PLAN_SEARCH_WIRE_SCHEMA_VERSION: u64 = 1;

const REPO_SOURCE: &str = "repo";

/// Ordered searchable fields. The query is matched (case-insensitively) against
/// each; a hit contributes the field's weight to the relevance score and its
/// name to `matched_fields`. Order is fixed so `matched_fields` is deterministic.
pub const PLAN_SEARCH_FIELD_NAMES: &[&str] = &[
    "title",
    "name",
    "status",
    "kind",
    "path",
    "frontmatter",
    "body",
];

const WEIGHT_TITLE: f64 = 100.0;
const WEIGHT_NAME: f64 = 60.0;
const WEIGHT_STATUS: f64 = 40.0;
const WEIGHT_FRONTMATTER: f64 = 40.0;
const WEIGHT_KIND: f64 = 30.0;
const WEIGHT_PATH: f64 = 20.0;
const WEIGHT_BODY: f64 = 10.0;

/// Additive boost applied to repo plans so they outrank local plans on
/// otherwise-equal relevance. Smaller than the gap between any two field
/// weights, so it only ever breaks near-ties (never overrides relevance).
const REPO_BOOST: f64 = 1.0;

/// Sort order for results. Resolved from the optional `sort` argument; `None`
/// means relevance when a query is present, recency when it is not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SortMode {
    Relevance,
    Recent,
    Title,
}

/// Whether a partial date bound expands to the start or the end of its period.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DateBound {
    Since,
    Until,
}

/// Search and rank plans discovered under a repo `sdd/` tree and/or the local
/// archive.
///
/// Either root may be `None` (so callers can scope by `--source`). `kinds`
/// narrows the repo corpus at read time (local plans are unaffected, governed
/// by `sources`). `query` is optional: `Some(non-blank)` does case-insensitive
/// substring matching and ranks by relevance; `None`/blank lists every plan
/// (browse mode). The remaining arguments filter by frontmatter `statuses`,
/// `sources`, and a `[since, until]` `created_at` range, then `sort`/`limit`
/// shape the output (`limit` of `0`/`None` is unlimited).
#[allow(clippy::too_many_arguments)]
pub fn search_plans(
    repo_sdd_root: Option<&Path>,
    local_plans_dir: Option<&Path>,
    query: Option<&str>,
    kinds: Option<&[String]>,
    statuses: Option<&[String]>,
    sources: Option<&[String]>,
    since: Option<&str>,
    until: Option<&str>,
    sort: Option<&str>,
    limit: Option<usize>,
) -> Result<Vec<PlanSearchMatchWire>, PlanError> {
    let plans = read_plans(repo_sdd_root, local_plans_dir, kinds)?;
    search_in_plans(
        plans,
        query,
        statuses,
        sources,
        since,
        until,
        sort,
        limit,
        current_date(),
    )
}

/// Filesystem-free core of [`search_plans`]: filter, match, rank, and limit an
/// already-discovered plan set. `now` anchors relative date bounds (the public
/// entry point passes the system clock; tests pin it for determinism).
#[allow(clippy::too_many_arguments)]
pub(crate) fn search_in_plans(
    plans: Vec<PlanWire>,
    query: Option<&str>,
    statuses: Option<&[String]>,
    sources: Option<&[String]>,
    since: Option<&str>,
    until: Option<&str>,
    sort: Option<&str>,
    limit: Option<usize>,
    now: NaiveDate,
) -> Result<Vec<PlanSearchMatchWire>, PlanError> {
    let needle = query
        .map(str::trim)
        .filter(|query| !query.is_empty())
        .map(str::to_lowercase);
    let since_bound = since
        .map(|raw| parse_date_bound(raw, now, DateBound::Since))
        .transpose()?;
    let until_bound = until
        .map(|raw| parse_date_bound(raw, now, DateBound::Until))
        .transpose()?;
    let mode = resolve_sort(sort, needle.is_some())?;

    let mut matches = Vec::new();
    for plan in plans {
        if !source_selected(&plan, sources) {
            continue;
        }
        if !status_selected(&plan, statuses) {
            continue;
        }
        if !date_selected(&plan, since_bound, until_bound) {
            continue;
        }
        let (matched_fields, relevance) = match &needle {
            Some(needle) => match score_query(&plan, needle) {
                Some(scored) => scored,
                None => continue,
            },
            None => (Vec::new(), 0.0),
        };
        let score = relevance + repo_boost(&plan);
        matches.push(PlanSearchMatchWire {
            plan,
            matched_fields,
            score,
        });
    }

    sort_matches(&mut matches, mode);
    if let Some(max) = limit {
        if max > 0 && matches.len() > max {
            matches.truncate(max);
        }
    }
    Ok(matches)
}

/// Match `needle` against every searchable field. Returns the matched field
/// names (in [`PLAN_SEARCH_FIELD_NAMES`] order) and the summed field weight, or
/// `None` when nothing matched.
fn score_query(plan: &PlanWire, needle: &str) -> Option<(Vec<String>, f64)> {
    let mut matched_fields = Vec::new();
    let mut relevance = 0.0;
    for field in searchable_fields(plan) {
        if field.value.to_lowercase().contains(needle) {
            matched_fields.push(field.name.to_string());
            relevance += field.weight;
        }
    }
    (!matched_fields.is_empty()).then_some((matched_fields, relevance))
}

struct SearchField<'a> {
    name: &'static str,
    weight: f64,
    value: Cow<'a, str>,
}

fn searchable_fields(plan: &PlanWire) -> Vec<SearchField<'_>> {
    fn borrowed<'a>(
        name: &'static str,
        weight: f64,
        value: &'a str,
    ) -> SearchField<'a> {
        SearchField {
            name,
            weight,
            value: Cow::Borrowed(value),
        }
    }
    vec![
        borrowed("title", WEIGHT_TITLE, &plan.title),
        borrowed("name", WEIGHT_NAME, &plan.name),
        borrowed("status", WEIGHT_STATUS, &plan.status),
        borrowed("kind", WEIGHT_KIND, &plan.kind),
        borrowed("path", WEIGHT_PATH, &plan.relpath),
        SearchField {
            name: "frontmatter",
            weight: WEIGHT_FRONTMATTER,
            value: Cow::Owned(frontmatter_blob(plan)),
        },
        borrowed("body", WEIGHT_BODY, &plan.body),
    ]
}

/// All frontmatter values except `status`, which is searched as its own field
/// (`plan.status`); excluding it here avoids double-counting a status match.
fn frontmatter_blob(plan: &PlanWire) -> String {
    plan.frontmatter
        .iter()
        .filter(|(key, _)| key.as_str() != "status")
        .map(|(_, value)| value.as_str())
        .collect::<Vec<_>>()
        .join("\n")
}

fn repo_boost(plan: &PlanWire) -> f64 {
    if plan.source == REPO_SOURCE {
        REPO_BOOST
    } else {
        0.0
    }
}

fn source_selected(plan: &PlanWire, sources: Option<&[String]>) -> bool {
    match sources {
        None => true,
        Some(values) => contains_ci(values, &plan.source),
    }
}

fn status_selected(plan: &PlanWire, statuses: Option<&[String]>) -> bool {
    match statuses {
        None => true,
        Some(values) => contains_ci(values, &plan.status),
    }
}

fn date_selected(
    plan: &PlanWire,
    since: Option<NaiveDate>,
    until: Option<NaiveDate>,
) -> bool {
    if since.is_none() && until.is_none() {
        return true;
    }
    // A plan with no parseable date cannot be proven in-range; exclude it.
    let Some(date) = parse_plan_date(&plan.created_at) else {
        return false;
    };
    if let Some(since) = since {
        if date < since {
            return false;
        }
    }
    if let Some(until) = until {
        if date > until {
            return false;
        }
    }
    true
}

fn contains_ci(values: &[String], target: &str) -> bool {
    let target = target.to_lowercase();
    values.iter().any(|value| value.to_lowercase() == target)
}

fn resolve_sort(
    sort: Option<&str>,
    has_query: bool,
) -> Result<SortMode, PlanError> {
    match sort.map(str::trim) {
        None | Some("") => Ok(if has_query {
            SortMode::Relevance
        } else {
            SortMode::Recent
        }),
        Some("relevance") => Ok(SortMode::Relevance),
        Some("recent") => Ok(SortMode::Recent),
        Some("title") => Ok(SortMode::Title),
        Some(other) => {
            Err(PlanError::validation(format!("unknown sort mode: {other}")))
        }
    }
}

/// Sort matches in place. Every mode ends in the same deterministic tie-break
/// chain (relpath, then source) so ordering is stable across runs.
fn sort_matches(matches: &mut [PlanSearchMatchWire], mode: SortMode) {
    matches.sort_by(|a, b| {
        let primary = match mode {
            // Higher score first; equal scores fall back to recency.
            SortMode::Relevance => b
                .score
                .total_cmp(&a.score)
                .then_with(|| b.plan.created_at.cmp(&a.plan.created_at)),
            // Newer first (canonical timestamps compare chronologically).
            SortMode::Recent => b.plan.created_at.cmp(&a.plan.created_at),
            // Title A→Z, case-insensitive.
            SortMode::Title => a
                .plan
                .title
                .to_lowercase()
                .cmp(&b.plan.title.to_lowercase()),
        };
        primary
            .then_with(|| source_rank(&a.plan).cmp(&source_rank(&b.plan)))
            .then_with(|| a.plan.relpath.cmp(&b.plan.relpath))
            .then_with(|| a.plan.source.cmp(&b.plan.source))
    });
}

/// Repo plans (`0`) sort ahead of local plans (`1`) on ties.
fn source_rank(plan: &PlanWire) -> u8 {
    if plan.source == REPO_SOURCE {
        0
    } else {
        1
    }
}

fn current_date() -> NaiveDate {
    let now: DateTime<Utc> = SystemTime::now().into();
    now.date_naive()
}

/// The date portion of a canonical `created_at` (`%Y-%m-%dT%H:%M:%S`), with a
/// date-only fallback. `None` when empty or unparseable.
fn parse_plan_date(created_at: &str) -> Option<NaiveDate> {
    let raw = created_at.trim();
    if raw.is_empty() {
        return None;
    }
    NaiveDateTime::parse_from_str(raw, "%Y-%m-%dT%H:%M:%S")
        .map(|datetime| datetime.date())
        .ok()
        .or_else(|| NaiveDate::parse_from_str(raw, "%Y-%m-%d").ok())
}

/// Parse a `--since`/`--until` bound. Accepts relative `Nd`/`Nw`/`Nm`,
/// `YYYY-MM-DD`, and `YYYY-MM`/`YYYYMM` (the partial-month forms expand to the
/// first day for `Since` and the last day for `Until`).
fn parse_date_bound(
    raw: &str,
    now: NaiveDate,
    bound: DateBound,
) -> Result<NaiveDate, PlanError> {
    let raw = raw.trim();
    if let Some(date) = parse_relative(raw, now)? {
        return Ok(date);
    }
    if let Ok(date) = NaiveDate::parse_from_str(raw, "%Y-%m-%d") {
        return Ok(date);
    }
    if let Some((year, month)) = parse_year_month(raw) {
        return Ok(month_bound(year, month, bound));
    }
    Err(PlanError::validation(format!("invalid date: {raw}")))
}

/// Parse a relative `Nd`/`Nw`/`Nm` bound as `now` minus the period. Returns
/// `Ok(None)` when `raw` is not a relative form (so other formats are tried).
fn parse_relative(
    raw: &str,
    now: NaiveDate,
) -> Result<Option<NaiveDate>, PlanError> {
    let Some(unit) = raw.chars().last() else {
        return Ok(None);
    };
    if !matches!(unit, 'd' | 'w' | 'm') {
        return Ok(None);
    }
    let digits = &raw[..raw.len() - unit.len_utf8()];
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return Ok(None);
    }
    let count: i64 = digits
        .parse()
        .map_err(|_| PlanError::validation(format!("invalid date: {raw}")))?;
    let date = match unit {
        'd' => now.checked_sub_signed(Duration::days(count)),
        'w' => now.checked_sub_signed(Duration::weeks(count)),
        'm' => subtract_months(now, count),
        _ => unreachable!("unit guarded above"),
    };
    date.map(Some).ok_or_else(|| {
        PlanError::validation(format!("date out of range: {raw}"))
    })
}

/// Parse a `YYYY-MM` or `YYYYMM` partial date into `(year, month)`.
fn parse_year_month(raw: &str) -> Option<(i32, u32)> {
    let (year_str, month_str) = match raw.len() {
        7 if raw.as_bytes()[4] == b'-' => (&raw[..4], &raw[5..]),
        6 if !raw.contains('-') => (&raw[..4], &raw[4..]),
        _ => return None,
    };
    if !year_str
        .bytes()
        .chain(month_str.bytes())
        .all(|b| b.is_ascii_digit())
    {
        return None;
    }
    let year: i32 = year_str.parse().ok()?;
    let month: u32 = month_str.parse().ok()?;
    (1..=12).contains(&month).then_some((year, month))
}

fn month_bound(year: i32, month: u32, bound: DateBound) -> NaiveDate {
    let day = match bound {
        DateBound::Since => 1,
        DateBound::Until => last_day_of_month(year, month),
    };
    NaiveDate::from_ymd_opt(year, month, day)
        .expect("month validated to 1..=12 and day within range")
}

fn last_day_of_month(year: i32, month: u32) -> u32 {
    let (next_year, next_month) = if month == 12 {
        (year + 1, 1)
    } else {
        (year, month + 1)
    };
    NaiveDate::from_ymd_opt(next_year, next_month, 1)
        .and_then(|first| first.pred_opt())
        .map(|last| last.day())
        .unwrap_or(28)
}

/// `date` shifted back `months`, clamping the day to the target month's length.
fn subtract_months(date: NaiveDate, months: i64) -> Option<NaiveDate> {
    let total = (date.year() as i64) * 12 + (date.month0() as i64) - months;
    let year = i32::try_from(total.div_euclid(12)).ok()?;
    let month = total.rem_euclid(12) as u32 + 1;
    let day = date.day().min(last_day_of_month(year, month));
    NaiveDate::from_ymd_opt(year, month, day)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::fs;

    use tempfile::tempdir;

    /// Build a neutral plan whose every searchable field is inert, then apply
    /// `update`. Tests flip exactly the fields they exercise.
    fn plan_with(update: impl FnOnce(&mut PlanWire)) -> PlanWire {
        let mut plan = PlanWire {
            source: REPO_SOURCE.to_string(),
            kind: "tale".to_string(),
            path: "/repo/sdd/tales/202606/neutral.md".to_string(),
            relpath: "tales/202606/neutral.md".to_string(),
            name: "neutral".to_string(),
            title: "Neutral plan".to_string(),
            status: "wip".to_string(),
            created_at: "2026-06-01T00:00:00".to_string(),
            prompt_link: String::new(),
            summary: String::new(),
            body: "Neutral body.".to_string(),
            frontmatter: BTreeMap::new(),
        };
        update(&mut plan);
        plan
    }

    fn relpaths(matches: &[PlanSearchMatchWire]) -> Vec<&str> {
        matches
            .iter()
            .map(|result| result.plan.relpath.as_str())
            .collect()
    }

    fn search(
        plans: Vec<PlanWire>,
        query: Option<&str>,
    ) -> Vec<PlanSearchMatchWire> {
        search_in_plans(
            plans,
            query,
            None,
            None,
            None,
            None,
            None,
            None,
            NaiveDate::from_ymd_opt(2026, 6, 18).unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn matches_every_searchable_field() {
        let cases: Vec<(&str, PlanWire, &str)> = vec![
            (
                "title",
                plan_with(|p| p.title = "Findme title".to_string()),
                "findme",
            ),
            (
                "name",
                plan_with(|p| p.name = "findme_name".to_string()),
                "findme",
            ),
            (
                "status",
                plan_with(|p| p.status = "findstatus".to_string()),
                "findstatus",
            ),
            (
                "kind",
                plan_with(|p| p.kind = "findkind".to_string()),
                "findkind",
            ),
            (
                "path",
                plan_with(|p| {
                    p.relpath = "tales/202606/findpath.md".to_string()
                }),
                "findpath",
            ),
            (
                "frontmatter",
                plan_with(|p| {
                    p.frontmatter
                        .insert("prompt".to_string(), "findfm".to_string());
                }),
                "findfm",
            ),
            (
                "body",
                plan_with(|p| p.body = "deep in the findbody text".to_string()),
                "findbody",
            ),
        ];

        for (field_name, plan, query) in cases {
            let results = search(vec![plan], Some(query));
            assert_eq!(results.len(), 1, "field {field_name}");
            assert_eq!(
                results[0].matched_fields,
                vec![field_name.to_string()],
                "field {field_name}"
            );
            assert!(results[0].score > 0.0, "field {field_name}");
        }
    }

    #[test]
    fn field_names_constant_matches_searchable_fields() {
        let names: Vec<&str> = searchable_fields(&plan_with(|_| {}))
            .into_iter()
            .map(|field| field.name)
            .collect();
        assert_eq!(names, PLAN_SEARCH_FIELD_NAMES);
    }

    #[test]
    fn status_value_is_reported_as_status_not_frontmatter() {
        // The frontmatter blob excludes `status`, so a status-value query
        // reports only the dedicated `status` field (no double count).
        let plan = plan_with(|p| {
            p.status = "blocked".to_string();
            p.frontmatter
                .insert("status".to_string(), "blocked".to_string());
        });

        let results = search(vec![plan], Some("blocked"));

        assert_eq!(results[0].matched_fields, vec!["status".to_string()]);
    }

    #[test]
    fn matches_case_insensitive_unicode_substrings() {
        let plan = plan_with(|p| p.title = "CAFÉ auth".to_string());

        let results = search(vec![plan], Some("café"));

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].matched_fields, vec!["title".to_string()]);
    }

    #[test]
    fn no_match_returns_empty_results() {
        let results = search(vec![plan_with(|_| {})], Some("absent-needle"));
        assert!(results.is_empty());
    }

    #[test]
    fn ranks_title_above_frontmatter_above_body() {
        let title = plan_with(|p| {
            p.relpath = "a-title.md".to_string();
            p.title = "needle title".to_string();
        });
        let frontmatter = plan_with(|p| {
            p.relpath = "b-frontmatter.md".to_string();
            p.frontmatter
                .insert("prompt".to_string(), "needle".to_string());
        });
        let body = plan_with(|p| {
            p.relpath = "c-body.md".to_string();
            p.body = "needle deep in body".to_string();
        });

        // Insertion order is intentionally not the expected ranked order.
        let results = search(vec![body, frontmatter, title], Some("needle"));

        assert_eq!(
            relpaths(&results),
            vec!["a-title.md", "b-frontmatter.md", "c-body.md"]
        );
        assert!(results[0].score > results[1].score);
        assert!(results[1].score > results[2].score);
    }

    #[test]
    fn repo_outranks_local_on_equal_relevance() {
        let local = plan_with(|p| {
            p.source = "local".to_string();
            p.kind = "local".to_string();
            p.relpath = "local.md".to_string();
            p.title = "needle".to_string();
        });
        let repo = plan_with(|p| {
            p.source = REPO_SOURCE.to_string();
            p.relpath = "repo.md".to_string();
            p.title = "needle".to_string();
        });

        let results = search(vec![local, repo], Some("needle"));

        assert_eq!(relpaths(&results), vec!["repo.md", "local.md"]);
        // Repo's edge is exactly the boost; relevance is identical.
        assert!(results[0].score > results[1].score);
        assert_eq!(results[0].score - results[1].score, REPO_BOOST);
    }

    #[test]
    fn recency_breaks_relevance_ties() {
        let older = plan_with(|p| {
            p.relpath = "older.md".to_string();
            p.title = "needle".to_string();
            p.created_at = "2026-01-01T00:00:00".to_string();
        });
        let newer = plan_with(|p| {
            p.relpath = "newer.md".to_string();
            p.title = "needle".to_string();
            p.created_at = "2026-06-01T00:00:00".to_string();
        });

        let results = search(vec![older, newer], Some("needle"));

        assert_eq!(relpaths(&results), vec!["newer.md", "older.md"]);
        // Same relevance and source → identical score; recency is the tie-break.
        assert_eq!(results[0].score, results[1].score);
    }

    #[test]
    fn browse_mode_returns_all_plans_sorted_by_recency() {
        let older = plan_with(|p| {
            p.relpath = "older.md".to_string();
            p.created_at = "2026-01-01T00:00:00".to_string();
        });
        let newer = plan_with(|p| {
            p.relpath = "newer.md".to_string();
            p.created_at = "2026-06-01T00:00:00".to_string();
        });

        let results = search(vec![older, newer], None);

        assert_eq!(relpaths(&results), vec!["newer.md", "older.md"]);
        for result in &results {
            assert!(result.matched_fields.is_empty());
        }
    }

    #[test]
    fn blank_query_is_treated_as_browse() {
        for query in ["", "   ", "\t\n"] {
            let results = search(vec![plan_with(|_| {})], Some(query));
            assert_eq!(results.len(), 1, "query {query:?}");
            assert!(results[0].matched_fields.is_empty(), "query {query:?}");
        }
    }

    #[test]
    fn filters_status_source_and_date_together() {
        let keep = plan_with(|p| {
            p.relpath = "keep.md".to_string();
            p.source = REPO_SOURCE.to_string();
            p.status = "wip".to_string();
            p.created_at = "2026-03-15T00:00:00".to_string();
            p.title = "auth keep".to_string();
        });
        let wrong_status = plan_with(|p| {
            p.relpath = "done.md".to_string();
            p.source = REPO_SOURCE.to_string();
            p.status = "done".to_string();
            p.created_at = "2026-03-15T00:00:00".to_string();
            p.title = "auth done".to_string();
        });
        let wrong_source = plan_with(|p| {
            p.relpath = "local.md".to_string();
            p.source = "local".to_string();
            p.status = "wip".to_string();
            p.created_at = "2026-03-15T00:00:00".to_string();
            p.title = "auth local".to_string();
        });
        let wrong_date = plan_with(|p| {
            p.relpath = "old.md".to_string();
            p.source = REPO_SOURCE.to_string();
            p.status = "wip".to_string();
            p.created_at = "2026-01-01T00:00:00".to_string();
            p.title = "auth old".to_string();
        });

        let results = search_in_plans(
            vec![keep, wrong_status, wrong_source, wrong_date],
            Some("auth"),
            Some(&["wip".to_string()]),
            Some(&[REPO_SOURCE.to_string()]),
            Some("2026-02"),
            Some("2026-04"),
            None,
            None,
            NaiveDate::from_ymd_opt(2026, 6, 18).unwrap(),
        )
        .unwrap();

        assert_eq!(relpaths(&results), vec!["keep.md"]);
    }

    #[test]
    fn status_filter_is_case_insensitive() {
        let plan = plan_with(|p| p.status = "WIP".to_string());

        let results = search_in_plans(
            vec![plan],
            None,
            Some(&["wip".to_string()]),
            None,
            None,
            None,
            None,
            None,
            NaiveDate::from_ymd_opt(2026, 6, 18).unwrap(),
        )
        .unwrap();

        assert_eq!(results.len(), 1);
    }

    #[test]
    fn date_range_filters_inclusively_on_day_bounds() {
        let plans: Vec<PlanWire> = ["2026-03-09", "2026-03-10", "2026-03-20"]
            .iter()
            .map(|day| {
                plan_with(|p| {
                    p.relpath = format!("{day}.md");
                    p.created_at = format!("{day}T12:00:00");
                })
            })
            .collect();

        let results = search_in_plans(
            plans,
            None,
            None,
            None,
            Some("2026-03-10"),
            Some("2026-03-20"),
            Some("recent"),
            None,
            NaiveDate::from_ymd_opt(2026, 6, 18).unwrap(),
        )
        .unwrap();

        // Boundary days are inclusive; the earlier day is dropped.
        assert_eq!(relpaths(&results), vec!["2026-03-20.md", "2026-03-10.md"]);
    }

    #[test]
    fn excludes_plans_without_dates_when_date_filter_active() {
        let dated = plan_with(|p| {
            p.relpath = "dated.md".to_string();
            p.created_at = "2026-03-10T00:00:00".to_string();
        });
        let undated = plan_with(|p| {
            p.relpath = "undated.md".to_string();
            p.created_at = String::new();
        });

        let results = search_in_plans(
            vec![dated, undated],
            None,
            None,
            None,
            Some("2026-01"),
            None,
            None,
            None,
            NaiveDate::from_ymd_opt(2026, 6, 18).unwrap(),
        )
        .unwrap();

        assert_eq!(relpaths(&results), vec!["dated.md"]);
    }

    #[test]
    fn parses_relative_and_partial_date_bounds() {
        let now = NaiveDate::from_ymd_opt(2026, 6, 18).unwrap();

        assert_eq!(
            parse_date_bound("14d", now, DateBound::Since).unwrap(),
            NaiveDate::from_ymd_opt(2026, 6, 4).unwrap()
        );
        assert_eq!(
            parse_date_bound("2w", now, DateBound::Since).unwrap(),
            NaiveDate::from_ymd_opt(2026, 6, 4).unwrap()
        );
        assert_eq!(
            parse_date_bound("3m", now, DateBound::Since).unwrap(),
            NaiveDate::from_ymd_opt(2026, 3, 18).unwrap()
        );
        assert_eq!(
            parse_date_bound("2026-06-18", now, DateBound::Since).unwrap(),
            NaiveDate::from_ymd_opt(2026, 6, 18).unwrap()
        );
        // Partial months expand to the first day for Since, the last for Until.
        assert_eq!(
            parse_date_bound("2026-02", now, DateBound::Since).unwrap(),
            NaiveDate::from_ymd_opt(2026, 2, 1).unwrap()
        );
        assert_eq!(
            parse_date_bound("2026-02", now, DateBound::Until).unwrap(),
            NaiveDate::from_ymd_opt(2026, 2, 28).unwrap()
        );
        assert_eq!(
            parse_date_bound("202612", now, DateBound::Until).unwrap(),
            NaiveDate::from_ymd_opt(2026, 12, 31).unwrap()
        );
    }

    #[test]
    fn relative_months_clamp_to_month_length() {
        let now = NaiveDate::from_ymd_opt(2026, 3, 31).unwrap();
        // One month before March 31 lands on the last valid February day.
        assert_eq!(
            parse_date_bound("1m", now, DateBound::Since).unwrap(),
            NaiveDate::from_ymd_opt(2026, 2, 28).unwrap()
        );
    }

    #[test]
    fn rejects_invalid_date_bound() {
        let err = search_in_plans(
            vec![plan_with(|_| {})],
            None,
            None,
            None,
            Some("not-a-date"),
            None,
            None,
            None,
            NaiveDate::from_ymd_opt(2026, 6, 18).unwrap(),
        )
        .unwrap_err();

        assert_eq!(err.kind, "validation");
        assert!(err.message.contains("invalid date"));
    }

    #[test]
    fn rejects_unknown_sort_mode() {
        let err = search_in_plans(
            vec![plan_with(|_| {})],
            None,
            None,
            None,
            None,
            None,
            Some("sideways"),
            None,
            NaiveDate::from_ymd_opt(2026, 6, 18).unwrap(),
        )
        .unwrap_err();

        assert_eq!(err.kind, "validation");
        assert!(err.message.contains("unknown sort mode"));
    }

    #[test]
    fn explicit_sort_modes_override_defaults() {
        let alpha = plan_with(|p| {
            p.relpath = "a.md".to_string();
            p.title = "Alpha needle".to_string();
            p.created_at = "2026-01-01T00:00:00".to_string();
        });
        let zulu = plan_with(|p| {
            p.relpath = "z.md".to_string();
            p.title = "Zulu needle".to_string();
            p.created_at = "2026-06-01T00:00:00".to_string();
        });

        let by_title = search_in_plans(
            vec![zulu.clone(), alpha.clone()],
            Some("needle"),
            None,
            None,
            None,
            None,
            Some("title"),
            None,
            NaiveDate::from_ymd_opt(2026, 6, 18).unwrap(),
        )
        .unwrap();
        assert_eq!(relpaths(&by_title), vec!["a.md", "z.md"]);

        // recent sort even though a query is present (overrides relevance).
        let by_recent = search_in_plans(
            vec![alpha, zulu],
            Some("needle"),
            None,
            None,
            None,
            None,
            Some("recent"),
            None,
            NaiveDate::from_ymd_opt(2026, 6, 18).unwrap(),
        )
        .unwrap();
        assert_eq!(relpaths(&by_recent), vec!["z.md", "a.md"]);
    }

    #[test]
    fn applies_limit_after_ranking() {
        let plans: Vec<PlanWire> = (1..=3)
            .map(|idx| {
                plan_with(|p| {
                    p.relpath = format!("{idx}.md");
                    p.title = "needle".to_string();
                    p.created_at = format!("2026-06-0{idx}T00:00:00");
                })
            })
            .collect();

        let results = search_in_plans(
            plans,
            Some("needle"),
            None,
            None,
            None,
            None,
            None,
            Some(2),
            NaiveDate::from_ymd_opt(2026, 6, 18).unwrap(),
        )
        .unwrap();

        // Ranked by recency tie-break, then truncated to the top two.
        assert_eq!(relpaths(&results), vec!["3.md", "2.md"]);
    }

    #[test]
    fn zero_limit_is_unlimited() {
        let plans: Vec<PlanWire> = (1..=3)
            .map(|idx| plan_with(|p| p.relpath = format!("{idx}.md")))
            .collect();

        let results = search_in_plans(
            plans,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(0),
            NaiveDate::from_ymd_opt(2026, 6, 18).unwrap(),
        )
        .unwrap();

        assert_eq!(results.len(), 3);
    }

    #[test]
    fn source_filter_scopes_results() {
        let repo = plan_with(|p| {
            p.relpath = "repo.md".to_string();
            p.source = REPO_SOURCE.to_string();
        });
        let local = plan_with(|p| {
            p.relpath = "local.md".to_string();
            p.source = "local".to_string();
            p.kind = "local".to_string();
        });

        let results = search_in_plans(
            vec![repo, local],
            None,
            None,
            Some(&["local".to_string()]),
            None,
            None,
            None,
            None,
            NaiveDate::from_ymd_opt(2026, 6, 18).unwrap(),
        )
        .unwrap();

        assert_eq!(relpaths(&results), vec!["local.md"]);
    }

    #[test]
    fn public_search_reads_ranks_and_prioritizes_repo() {
        let temp = tempdir().unwrap();
        let sdd = temp.path().join("sdd");
        let local = temp.path().join("plans");
        write(
            &sdd.join("epics").join("202606").join("repo_auth.md"),
            "---\ncreate_time: 2026-06-01 00:00:00\nstatus: wip\n---\n\
             # Repo auth epic\n\nNeedle in the repo body.\n",
        );
        write(
            &sdd.join("tales").join("202606").join("unrelated.md"),
            "# Unrelated\n\nNothing to see.\n",
        );
        write(
            &local.join("local_auth.md"),
            "---\ncreate_time: 2026-06-10 00:00:00\n---\n\
             # Local auth note\n\nNeedle in the local body.\n",
        );

        let results = search_plans(
            Some(&sdd),
            Some(&local),
            Some("needle"),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();

        // Both auth plans match; the unrelated plan does not. Repo first.
        assert_eq!(
            relpaths(&results),
            vec!["epics/202606/repo_auth.md", "local_auth.md"]
        );
        assert_eq!(results[0].plan.source, "repo");
        assert_eq!(results[1].plan.source, "local");
    }

    #[test]
    fn public_search_kind_filter_narrows_repo_only() {
        let temp = tempdir().unwrap();
        let sdd = temp.path().join("sdd");
        let local = temp.path().join("plans");
        write(&sdd.join("tales").join("202606").join("t.md"), "# Tale\n");
        write(&sdd.join("epics").join("202606").join("e.md"), "# Epic\n");
        write(&local.join("l.md"), "# Local\n");

        let results = search_plans(
            Some(&sdd),
            Some(&local),
            None,
            Some(&["epic".to_string()]),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();

        // Repo tale is filtered out by --kind; the epic and the (kind-agnostic)
        // local plan remain.
        let kinds: Vec<&str> =
            results.iter().map(|r| r.plan.kind.as_str()).collect();
        assert!(kinds.contains(&"epic"));
        assert!(kinds.contains(&"local"));
        assert!(!kinds.contains(&"tale"));
    }

    fn write(path: &Path, content: &str) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(path, content).unwrap();
    }
}
