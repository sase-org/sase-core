//! Parity gate for the `vcs_log` parser + aggregator: mirror the Python
//! golden tests in `sase/tests/test_core_vcs_log.py` so a parity drift
//! fails the Rust-side `cargo test` before the Python facade ever sees the
//! binding.
//!
//! The fixtures are inlined byte-for-byte from the Python source. If
//! `sase/tests/test_core_vcs_log.py` changes, mirror the change here.

use sase_core::vcs_log::parsers::{RECORD_SEP, UNIT_SEP};
use sase_core::vcs_log::{
    aggregate_commit_log, parse_git_log, AggregatedCommitWire, VcsCommitWire,
    VCS_LOG_WIRE_SCHEMA_VERSION,
};
use serde_json::json;

fn record(
    full: &str,
    short: &str,
    name: &str,
    email: &str,
    ts: &str,
    subject: &str,
    body: &str,
) -> String {
    format!(
        "{full}{US}{short}{US}{name}{US}{email}{US}{ts}{US}{subject}{US}{body}{RS}",
        US = UNIT_SEP,
        RS = RECORD_SEP,
    )
}

fn commit(full: &str, ts: i64, subject: &str) -> VcsCommitWire {
    VcsCommitWire {
        full_id: full.to_string(),
        short_id: full.chars().take(7).collect(),
        author_name: "bryan".to_string(),
        author_email: "bryan@example.com".to_string(),
        timestamp: ts,
        subject: subject.to_string(),
        body: String::new(),
    }
}

// -- parse_git_log --------------------------------------------------------

#[test]
fn parse_empty_stream_returns_empty_list() {
    assert!(parse_git_log("").is_empty());
}

#[test]
fn parse_single_commit_all_fields() {
    let stream = record(
        "a1b2c3d4e5f6",
        "a1b2c3d",
        "bryan",
        "bryan@example.com",
        "1700000000",
        "fix(sdd): link store",
        "",
    );
    assert_eq!(
        parse_git_log(&stream),
        vec![VcsCommitWire {
            full_id: "a1b2c3d4e5f6".to_string(),
            short_id: "a1b2c3d".to_string(),
            author_name: "bryan".to_string(),
            author_email: "bryan@example.com".to_string(),
            timestamp: 1700000000,
            subject: "fix(sdd): link store".to_string(),
            body: String::new(),
        }]
    );
}

#[test]
fn parse_strips_newline_git_inserts_between_records() {
    let stream = format!(
        "{}\n{}\n",
        record("h1", "s1", "bryan", "bryan@example.com", "300", "first", ""),
        record(
            "h2",
            "s2",
            "bryan",
            "bryan@example.com",
            "200",
            "second",
            ""
        ),
    );
    let parsed = parse_git_log(&stream);
    assert_eq!(parsed.len(), 2);
    assert_eq!(parsed[0].full_id, "h1");
    assert_eq!(parsed[1].full_id, "h2");
}

#[test]
fn parse_trailing_record_separator_yields_no_blank() {
    let stream =
        record("h1", "s1", "bryan", "bryan@example.com", "300", "only", "");
    assert_eq!(parse_git_log(&stream).len(), 1);
}

#[test]
fn parse_multiline_body_preserved() {
    let body = "detail line one\ndetail line two";
    let stream = record(
        "h1",
        "s1",
        "bryan",
        "bryan@example.com",
        "300",
        "subject",
        body,
    );
    let parsed = parse_git_log(&stream);
    assert_eq!(parsed[0].body, body);
}

#[test]
fn parse_drops_record_with_too_few_fields() {
    let malformed = format!(
        "h1{US}s1{US}bryan{US}bryan@example.com{US}300{US}subject{RS}",
        US = UNIT_SEP,
        RS = RECORD_SEP,
    );
    let good =
        record("h2", "s2", "bryan", "bryan@example.com", "200", "ok", "");
    let parsed = parse_git_log(&format!("{malformed}{good}"));
    assert_eq!(parsed.len(), 1);
    assert_eq!(parsed[0].full_id, "h2");
}

#[test]
fn parse_drops_record_with_bad_timestamp() {
    let bad = record(
        "h1",
        "s1",
        "bryan",
        "bryan@example.com",
        "not-a-number",
        "x",
        "",
    );
    let good =
        record("h2", "s2", "bryan", "bryan@example.com", "200", "ok", "");
    let parsed = parse_git_log(&format!("{bad}{good}"));
    assert_eq!(parsed.len(), 1);
    assert_eq!(parsed[0].full_id, "h2");
}

// -- aggregate_commit_log -------------------------------------------------

#[test]
fn aggregate_empty_returns_empty() {
    assert!(aggregate_commit_log(Vec::new(), 20).is_empty());
}

#[test]
fn aggregate_interleaves_by_timestamp_desc() {
    let repos = vec![
        (
            "sase".to_string(),
            vec![
                commit("a", 300, "newest sase"),
                commit("b", 100, "old sase"),
            ],
        ),
        ("sase-core".to_string(), vec![commit("c", 200, "mid core")]),
    ];
    let out = aggregate_commit_log(repos, 20);
    let order: Vec<(&str, &str)> = out
        .iter()
        .map(|r| (r.repo.as_str(), r.commit.full_id.as_str()))
        .collect();
    assert_eq!(
        order,
        vec![("sase", "a"), ("sase-core", "c"), ("sase", "b")]
    );
}

#[test]
fn aggregate_tie_break_repo_then_full_id() {
    let repos = vec![
        ("zebra".to_string(), vec![commit("x", 500, "z")]),
        (
            "alpha".to_string(),
            vec![commit("m", 500, "am"), commit("a", 500, "aa")],
        ),
    ];
    let out = aggregate_commit_log(repos, 20);
    let order: Vec<(&str, &str)> = out
        .iter()
        .map(|r| (r.repo.as_str(), r.commit.full_id.as_str()))
        .collect();
    assert_eq!(order, vec![("alpha", "a"), ("alpha", "m"), ("zebra", "x")]);
}

#[test]
fn aggregate_truncates_to_limit() {
    let repos = vec![(
        "sase".to_string(),
        vec![
            commit("a", 500, "a"),
            commit("b", 400, "b"),
            commit("c", 300, "c"),
        ],
    )];
    let out = aggregate_commit_log(repos, 2);
    assert_eq!(out.len(), 2);
    assert_eq!(out[0].commit.full_id, "a");
    assert_eq!(out[1].commit.full_id, "b");
}

// -- Wire helpers ---------------------------------------------------------

#[test]
fn vcs_log_wire_schema_version_is_one() {
    assert_eq!(VCS_LOG_WIRE_SCHEMA_VERSION, 1);
}

#[test]
fn vcs_commit_wire_serializes_to_python_shape() {
    let row = VcsCommitWire {
        full_id: "a1b2c3d4".to_string(),
        short_id: "a1b2c3d".to_string(),
        author_name: "bryan".to_string(),
        author_email: "bryan@example.com".to_string(),
        timestamp: 1700000000,
        subject: "fix: thing".to_string(),
        body: "body text".to_string(),
    };
    let value = serde_json::to_value(&row).unwrap();
    assert_eq!(
        value,
        json!({
            "full_id": "a1b2c3d4",
            "short_id": "a1b2c3d",
            "author_name": "bryan",
            "author_email": "bryan@example.com",
            "timestamp": 1700000000,
            "subject": "fix: thing",
            "body": "body text",
        })
    );
}

#[test]
fn aggregated_commit_wire_serializes_flat() {
    let row = AggregatedCommitWire {
        repo: "sase".to_string(),
        commit: VcsCommitWire {
            full_id: "a1b2c3d4".to_string(),
            short_id: "a1b2c3d".to_string(),
            author_name: "bryan".to_string(),
            author_email: "bryan@example.com".to_string(),
            timestamp: 1700000000,
            subject: "fix: thing".to_string(),
            body: String::new(),
        },
    };
    let value = serde_json::to_value(&row).unwrap();
    assert_eq!(
        value,
        json!({
            "repo": "sase",
            "full_id": "a1b2c3d4",
            "short_id": "a1b2c3d",
            "author_name": "bryan",
            "author_email": "bryan@example.com",
            "timestamp": 1700000000,
            "subject": "fix: thing",
            "body": "",
        })
    );
}

#[test]
fn aggregated_commit_wire_round_trips_through_json() {
    let row = AggregatedCommitWire {
        repo: "sase-core".to_string(),
        commit: commit("deadbeef", 42, "subject"),
    };
    let value = serde_json::to_value(&row).unwrap();
    let back: AggregatedCommitWire = serde_json::from_value(value).unwrap();
    assert_eq!(back, row);
}
