//! Owner-candidate projection: policy, lane equality, and wire contract.

use std::fs;
use std::path::{Path, PathBuf};

use super::{
    parse_close_timestamp, triage_owner_candidates,
    triage_owner_candidates_in_issues, BeadTriageOwnerCandidateWire,
    BeadTriageOwnerCandidatesWire, BeadTriageOwnerStatusWire,
    TRIAGE_OWNER_CLOSED_LOOKBACK_SECS,
};
use crate::bead::events::import_issues_to_event_streams;
use crate::bead::jsonl::{parse_issues_jsonl, write_event_store};
use crate::bead::mutation::store_io_stats;
use crate::bead::read::read_event_store_issues;
use crate::bead::read_model::{
    cached_triage_owner_candidates, read_model_cache_path_for_store,
    rebuild_read_model_at,
};
use crate::bead::wire::IssueWire;
use crate::tool_run::{
    match_owners, ToolRunTriageOwnerCandidateWire,
    TOOL_RUN_TRIAGE_LOOKBACK_SECS,
};

/// 2026-03-10T00:00:00Z.
const NOW: i64 = 1_773_100_800;
const DAY: i64 = 86_400;

fn iso(ts: i64) -> String {
    chrono::DateTime::from_timestamp(ts, 0)
        .unwrap()
        .format("%Y-%m-%dT%H:%M:%SZ")
        .to_string()
}

fn row(id: &str, created: &str, extra: serde_json::Value) -> String {
    let mut value = serde_json::json!({
        "id": id,
        "title": format!("Title {id}"),
        "status": "open",
        "issue_type": "task",
        "created_at": created,
        "updated_at": created,
    });
    for (key, field) in extra.as_object().unwrap() {
        value[key] = field.clone();
    }
    serde_json::to_string(&value).unwrap()
}

/// Representative store: every status, close-date edge, field precedence,
/// ignored issue/task types, and heavy irrelevant history.
fn fixture_jsonl(history: usize) -> String {
    let heavy = "x".repeat(16 * 1024);
    let mut rows = vec![
        row(
            "o.ci",
            "2026-01-01T00:00:01Z",
            serde_json::json!({
                "task_type": "ci",
                "task_type_fields": {
                    "node_id": "tests/test_a.py::test_one",
                    "location": "tests/test_a.py",
                },
            }),
        ),
        row(
            "o.ready",
            "2026-01-01T00:00:02Z",
            serde_json::json!({
                "status": "ready",
                "task_type": "flake",
                "task_type_fields": {"location": "src/pkg/mod.py"},
            }),
        ),
        row(
            "o.claimed",
            "2026-01-01T00:00:03Z",
            serde_json::json!({"status": "claimed", "task_type": "bug"}),
        ),
        row(
            "o.prog",
            "2026-01-01T00:00:04Z",
            serde_json::json!({
                "status": "in_progress",
                "task_type": "ci",
                "title": "Fix tests/test_b.py failure",
            }),
        ),
        row(
            "o.snooze",
            "2026-01-01T00:00:05Z",
            serde_json::json!({
                "status": "snoozed",
                "task_type": "flake",
                "snooze": {
                    "until": "2026-04-01T00:00:00+00:00",
                    "snoozed_at": "2026-03-01T00:00:00+00:00",
                    "snoozed_by": "fixture",
                },
            }),
        ),
        row(
            "c.recent",
            "2026-01-01T00:00:06Z",
            serde_json::json!({
                "status": "closed",
                "task_type": "bug",
                "closed_at": iso(NOW - DAY),
                "task_type_fields": {"node_id": "tests/test_c.py::test_two"},
            }),
        ),
        row(
            "c.edge",
            "2026-01-01T00:00:07Z",
            serde_json::json!({
                "status": "closed",
                "task_type": "ci",
                "closed_at": iso(NOW - TRIAGE_OWNER_CLOSED_LOOKBACK_SECS),
            }),
        ),
        row(
            "c.expired",
            "2026-01-01T00:00:08Z",
            serde_json::json!({
                "status": "closed",
                "task_type": "ci",
                "closed_at": iso(NOW - TRIAGE_OWNER_CLOSED_LOOKBACK_SECS - 1),
            }),
        ),
        row(
            "c.unknown",
            "2026-01-01T00:00:09Z",
            serde_json::json!({"status": "closed", "task_type": "flake"}),
        ),
        row(
            "c.garbled",
            "2026-01-01T00:00:10Z",
            serde_json::json!({
                "status": "closed",
                "task_type": "bug",
                "closed_at": "not a date",
            }),
        ),
        row(
            "c.naive",
            "2026-01-01T00:00:11Z",
            serde_json::json!({
                "status": "closed",
                "task_type": "ci",
                "closed_at": "2026-03-09 12:00:00",
            }),
        ),
        row(
            "x.feature",
            "2026-01-01T00:00:12Z",
            serde_json::json!({
                "task_type": "feature",
                "task_type_fields": {"proposal": "tests/test_a.py"},
            }),
        ),
        row("x.untyped", "2026-01-01T00:00:13Z", serde_json::json!({})),
        row(
            "x.plan",
            "2026-01-01T00:00:14Z",
            serde_json::json!({"issue_type": "plan", "tier": "plan"}),
        ),
        // Same created_at as o.ci: replay position breaks the tie.
        row(
            "o.tie",
            "2026-01-01T00:00:01Z",
            serde_json::json!({"task_type": "bug"}),
        ),
    ];
    for index in 0..history {
        rows.push(row(
            &format!("h.{index}"),
            "2025-06-01T00:00:00Z",
            serde_json::json!({
                "status": "closed",
                "task_type": if index % 2 == 0 { "ci" } else { "memory" },
                "closed_at": "2025-06-02T00:00:00Z",
                "description": heavy,
                "notes": heavy,
                "design": heavy,
            }),
        ));
    }
    rows.join("\n") + "\n"
}

fn fixture_issues(history: usize) -> Vec<IssueWire> {
    let outcome = parse_issues_jsonl(&fixture_jsonl(history));
    assert_eq!(
        outcome.loaded_rows,
        15 + history,
        "invalid lines: {:?} {:?}; loaded {:?}",
        outcome.invalid_json_lines,
        outcome.invalid_record_lines,
        outcome
            .issues
            .iter()
            .map(|issue| issue.id.as_str())
            .collect::<Vec<_>>()
    );
    outcome.issues
}

fn write_store(root: &Path, with_git: bool, history: usize) -> PathBuf {
    if with_git {
        fs::create_dir_all(root.join(".git")).unwrap();
    }
    let beads_dir = root.join("beads");
    fs::create_dir_all(&beads_dir).unwrap();
    fs::write(beads_dir.join("config.json"), "{}\n").unwrap();
    let streams =
        import_issues_to_event_streams(&fixture_issues(history)).unwrap();
    write_event_store(&beads_dir, &streams).unwrap();
    beads_dir
}

fn candidate(
    node_id: &str,
    location: Option<&str>,
    title: &str,
    status: BeadTriageOwnerStatusWire,
    closed_ts: Option<i64>,
) -> BeadTriageOwnerCandidateWire {
    BeadTriageOwnerCandidateWire {
        node_id: node_id.to_string(),
        location: location.map(str::to_string),
        title: Some(title.to_string()),
        status,
        closed_ts,
    }
}

fn expected_candidates() -> Vec<BeadTriageOwnerCandidateWire> {
    use BeadTriageOwnerStatusWire::{Closed, Open};
    vec![
        candidate(
            "o.ci",
            Some("tests/test_a.py::test_one"),
            "Title o.ci",
            Open,
            None,
        ),
        candidate("o.tie", None, "Title o.tie", Open, None),
        candidate(
            "o.ready",
            Some("src/pkg/mod.py"),
            "Title o.ready",
            Open,
            None,
        ),
        candidate("o.claimed", None, "Title o.claimed", Open, None),
        candidate("o.prog", None, "Fix tests/test_b.py failure", Open, None),
        candidate("o.snooze", None, "Title o.snooze", Open, None),
        candidate(
            "c.recent",
            Some("tests/test_c.py::test_two"),
            "Title c.recent",
            Closed,
            Some(NOW - DAY),
        ),
        candidate(
            "c.edge",
            None,
            "Title c.edge",
            Closed,
            Some(NOW - TRIAGE_OWNER_CLOSED_LOOKBACK_SECS),
        ),
        candidate(
            "c.naive",
            None,
            "Title c.naive",
            Closed,
            Some(NOW - DAY / 2),
        ),
    ]
}

fn ids(wire: &BeadTriageOwnerCandidatesWire) -> Vec<&str> {
    wire.candidates
        .iter()
        .map(|candidate| candidate.node_id.as_str())
        .collect()
}

#[test]
fn replay_projection_applies_eligibility_recency_and_field_policy() {
    let projected = triage_owner_candidates_in_issues(&fixture_issues(4), NOW);

    assert_eq!(projected.candidates, expected_candidates());
    assert_eq!(
        projected.diagnostics,
        vec![
            "owner candidates skipped 1 closed task(s) with an unparseable closed_at: c.garbled"
                .to_string()
        ]
    );
}

#[test]
fn explicit_now_ts_moves_the_closed_lookback() {
    let issues = fixture_issues(0);

    let later = triage_owner_candidates_in_issues(&issues, NOW + 1);
    assert!(!ids(&later).contains(&"c.edge"));
    assert!(ids(&later).contains(&"c.recent"));

    let earlier = triage_owner_candidates_in_issues(&issues, NOW - 1);
    assert!(ids(&earlier).contains(&"c.expired"));
    assert!(ids(&earlier).contains(&"c.edge"));

    // Long after every close, only open tasks remain.
    let distant = triage_owner_candidates_in_issues(&issues, NOW + 30 * DAY);
    assert_eq!(
        ids(&distant),
        vec![
            "o.ci",
            "o.tie",
            "o.ready",
            "o.claimed",
            "o.prog",
            "o.snooze"
        ]
    );
}

#[test]
fn indexed_lane_matches_replay_without_hydrating_issues() {
    let temp = tempfile::tempdir().unwrap();
    let beads_dir = write_store(temp.path(), true, 64);
    let cache_path = read_model_cache_path_for_store(&beads_dir).unwrap();
    rebuild_read_model_at(&beads_dir, &cache_path).unwrap();

    store_io_stats::reset();
    let indexed = cached_triage_owner_candidates(&beads_dir, NOW)
        .unwrap()
        .expect("fresh cache serves the projection")
        .unwrap();
    let served = triage_owner_candidates(&beads_dir, Some(NOW)).unwrap();
    // Neither the indexed lane nor the public entry point replayed or
    // loaded full issue rows on a fresh cache.
    assert_eq!(store_io_stats::reads(), 0);
    assert_eq!(store_io_stats::snapshot_loads(), 0);
    assert_eq!(store_io_stats::full_replays(), 0);

    let replayed = triage_owner_candidates_in_issues(
        &read_event_store_issues(&beads_dir).unwrap(),
        NOW,
    );
    assert_eq!(indexed, replayed);
    assert_eq!(served, replayed);
    assert_eq!(indexed.candidates, expected_candidates());

    // Only the five candidate keys cross the wire, never heavy text.
    let value = serde_json::to_value(&indexed).unwrap();
    for row in value["candidates"].as_array().unwrap() {
        let mut keys: Vec<&str> = row
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            vec!["closed_ts", "location", "node_id", "status", "title"]
        );
    }
}

#[test]
fn store_without_cache_location_replays_identically() {
    let cached_temp = tempfile::tempdir().unwrap();
    let cached_dir = write_store(cached_temp.path(), true, 8);
    let plain_temp = tempfile::tempdir().unwrap();
    let plain_dir = write_store(plain_temp.path(), false, 8);

    assert!(read_model_cache_path_for_store(&plain_dir).is_none());
    assert!(cached_triage_owner_candidates(&plain_dir, NOW)
        .unwrap()
        .is_none());
    let replayed = triage_owner_candidates(&plain_dir, Some(NOW)).unwrap();
    let cached = triage_owner_candidates(&cached_dir, Some(NOW)).unwrap();
    assert_eq!(replayed, cached);
    assert_eq!(replayed.candidates, expected_candidates());
}

#[test]
fn missing_store_is_an_error_not_an_empty_projection() {
    let temp = tempfile::tempdir().unwrap();
    let missing = temp.path().join("absent");
    assert!(triage_owner_candidates(&missing, Some(NOW)).is_err());
}

#[test]
fn candidates_feed_the_classifier_owner_matcher_unchanged() {
    let projected = triage_owner_candidates_in_issues(&fixture_issues(0), NOW);
    let value = serde_json::to_value(&projected.candidates).unwrap();
    let wires: Vec<ToolRunTriageOwnerCandidateWire> =
        serde_json::from_value(value).unwrap();
    assert_eq!(wires.len(), projected.candidates.len());

    let owners = match_owners(&["tests/test_a.py".to_string()], &wires, NOW);
    assert_eq!(owners.len(), 1);
    assert_eq!(owners[0]["id"], "o.ci");
    assert_eq!(owners[0]["matched_on"], "location");

    let fixed = match_owners(&["tests/test_c.py".to_string()], &wires, NOW);
    assert_eq!(fixed[0]["id"], "c.recent");
    assert_eq!(fixed[0]["status"], "closed");

    // A bare stage label is not a locator and matches nothing.
    assert!(
        match_owners(&["lint (symvision)".to_string()], &wires, NOW).is_empty()
    );
}

#[test]
fn closed_lookback_matches_the_classifier() {
    assert_eq!(
        TRIAGE_OWNER_CLOSED_LOOKBACK_SECS,
        TOOL_RUN_TRIAGE_LOOKBACK_SECS
    );
}

#[test]
fn close_timestamps_parse_like_python_fromisoformat() {
    let base = NOW; // 2026-03-10T00:00:00Z
    for (text, expected) in [
        ("2026-03-10T00:00:00Z", Some(base)),
        ("2026-03-10T00:00:00+00:00", Some(base)),
        ("2026-03-10T00:00:00.750000+00:00", Some(base)),
        ("2026-03-09T19:00:00-05:00", Some(base)),
        ("2026-03-10 00:00:00+00:00", Some(base)),
        ("2026-03-10T05:30:00+0530", Some(base)),
        ("2026-03-10T00:00:00", Some(base)),
        ("2026-03-10 00:00:00.5", Some(base)),
        ("2026-03-10T00:00", Some(base)),
        ("2026-03-10", Some(base)),
        ("  2026-03-10T00:00:00Z  ", Some(base)),
        ("", None),
        ("   ", None),
        ("yesterday", None),
        ("2026-13-40", None),
    ] {
        assert_eq!(parse_close_timestamp(text), expected, "{text:?}");
    }
}
