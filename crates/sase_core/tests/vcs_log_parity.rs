//! Parity gate for the `vcs_log` parser + aggregator: mirror the Python
//! golden tests in `sase/tests/test_core_vcs_log.py` so a parity drift
//! fails the Rust-side `cargo test` before the Python facade ever sees the
//! binding.
//!
//! The fixtures are inlined byte-for-byte from the Python source. If
//! `sase/tests/test_core_vcs_log.py` changes, mirror the change here.

use sase_core::vcs_log::parsers::{RECORD_SEP, UNIT_SEP};
use sase_core::vcs_log::{
    aggregate_commit_log, classify_commit_origin, classify_commit_presence,
    parse_git_log, AggregatedCommitWire, CommitOriginWire, CommitPresenceWire,
    VcsCommitWire, VCS_LOG_WIRE_SCHEMA_VERSION,
};
use serde_json::json;

fn record(
    full: &str,
    short: &str,
    ts: &str,
    parents: &str,
    subject: &str,
    body: &str,
) -> String {
    let name = "bryan";
    let email = "bryan@example.com";
    format!(
        "{full}{US}{short}{US}{name}{US}{email}{US}{ts}{US}{parents}{US}{subject}{US}{body}{RS}",
        US = UNIT_SEP,
        RS = RECORD_SEP,
    )
}

fn legacy_record(
    full: &str,
    short: &str,
    ts: &str,
    subject: &str,
    body: &str,
) -> String {
    let name = "bryan";
    let email = "bryan@example.com";
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
        parent_ids: Vec::new(),
        subject: subject.to_string(),
        body: String::new(),
        presence: CommitPresenceWire::Unknown,
        origin: CommitOriginWire::Manual,
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
        "1700000000",
        "p0",
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
            parent_ids: vec!["p0".to_string()],
            subject: "fix(sdd): link store".to_string(),
            body: String::new(),
            presence: CommitPresenceWire::Unknown,
            origin: CommitOriginWire::Manual,
        }]
    );
}

#[test]
fn parse_legacy_single_commit_defaults_parent_ids() {
    let stream = legacy_record(
        "a1b2c3d4e5f6",
        "a1b2c3d",
        "1700000000",
        "fix(sdd): link store",
        "",
    );
    let parsed = parse_git_log(&stream);
    assert_eq!(parsed.len(), 1);
    assert_eq!(parsed[0].parent_ids, Vec::<String>::new());
    assert_eq!(parsed[0].subject, "fix(sdd): link store");
}

#[test]
fn parse_root_commit_empty_parent_field() {
    let stream = record(
        "a1b2c3d4e5f6",
        "a1b2c3d",
        "1700000000",
        "",
        "initial commit",
        "",
    );
    let parsed = parse_git_log(&stream);
    assert_eq!(parsed.len(), 1);
    assert!(parsed[0].parent_ids.is_empty());
}

#[test]
fn parse_octopus_commit_parent_ids() {
    let stream = record(
        "a1b2c3d4e5f6",
        "a1b2c3d",
        "1700000000",
        "p1 p2 p3",
        "Merge branches",
        "",
    );
    let parsed = parse_git_log(&stream);
    assert_eq!(
        parsed[0].parent_ids,
        vec!["p1".to_string(), "p2".to_string(), "p3".to_string()]
    );
    assert!(parsed[0].is_merge());
}

#[test]
fn parse_strips_newline_git_inserts_between_records() {
    let stream = format!(
        "{}\n{}\n",
        record("h1", "s1", "300", "p0", "first", ""),
        record("h2", "s2", "200", "p1", "second", ""),
    );
    let parsed = parse_git_log(&stream);
    assert_eq!(parsed.len(), 2);
    assert_eq!(parsed[0].full_id, "h1");
    assert_eq!(parsed[1].full_id, "h2");
}

#[test]
fn parse_trailing_record_separator_yields_no_blank() {
    let stream = record("h1", "s1", "300", "p0", "only", "");
    assert_eq!(parse_git_log(&stream).len(), 1);
}

#[test]
fn parse_multiline_body_preserved() {
    let body = "detail line one\ndetail line two";
    let stream = record("h1", "s1", "300", "p0", "subject", body);
    let parsed = parse_git_log(&stream);
    assert_eq!(parsed[0].body, body);
}

#[test]
fn parse_stitch_type_footer_sets_stitch_origin() {
    let stream = record(
        "h1",
        "s1",
        "300",
        "p0",
        "fix: tracked",
        "detail\n\nSASE_TYPE=stitch",
    );
    let parsed = parse_git_log(&stream);
    assert_eq!(parsed[0].origin, CommitOriginWire::Stitch);
}

#[test]
fn parse_drops_record_with_too_few_fields() {
    let malformed = format!(
        "h1{US}s1{US}bryan{US}bryan@example.com{US}300{US}subject{RS}",
        US = UNIT_SEP,
        RS = RECORD_SEP,
    );
    let good = record("h2", "s2", "200", "p0", "ok", "");
    let parsed = parse_git_log(&format!("{malformed}{good}"));
    assert_eq!(parsed.len(), 1);
    assert_eq!(parsed[0].full_id, "h2");
}

#[test]
fn parse_drops_record_with_bad_timestamp() {
    let bad = record("h1", "s1", "not-a-number", "p0", "x", "");
    let good = record("h2", "s2", "200", "p1", "ok", "");
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
fn vcs_log_wire_schema_version_is_four() {
    assert_eq!(VCS_LOG_WIRE_SCHEMA_VERSION, 4);
}

#[test]
fn vcs_commit_wire_serializes_to_python_shape() {
    let row = VcsCommitWire {
        full_id: "a1b2c3d4".to_string(),
        short_id: "a1b2c3d".to_string(),
        author_name: "bryan".to_string(),
        author_email: "bryan@example.com".to_string(),
        timestamp: 1700000000,
        parent_ids: vec!["p1".to_string(), "p2".to_string()],
        subject: "fix: thing".to_string(),
        body: "body text".to_string(),
        presence: CommitPresenceWire::LocalOnly,
        origin: CommitOriginWire::Stitch,
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
            "parent_ids": ["p1", "p2"],
            "subject": "fix: thing",
            "body": "body text",
            "presence": "local_only",
            "origin": "stitch",
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
            parent_ids: vec!["p1".to_string(), "p2".to_string()],
            subject: "fix: thing".to_string(),
            body: String::new(),
            presence: CommitPresenceWire::RemoteOnly,
            origin: CommitOriginWire::Manual,
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
            "parent_ids": ["p1", "p2"],
            "subject": "fix: thing",
            "body": "",
            "presence": "remote_only",
            "origin": "manual",
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

#[test]
fn vcs_commit_wire_defaults_presence_to_unknown() {
    let value = json!({
        "full_id": "a1b2c3d4",
        "short_id": "a1b2c3d",
        "author_name": "bryan",
        "author_email": "bryan@example.com",
        "timestamp": 1700000000,
        "subject": "fix: thing",
        "body": "",
    });
    let row: VcsCommitWire = serde_json::from_value(value).unwrap();
    assert_eq!(row.presence, CommitPresenceWire::Unknown);
    assert_eq!(row.origin, CommitOriginWire::Manual);
    assert!(row.parent_ids.is_empty());
}

// -- classify_commit_origin ----------------------------------------------

#[test]
fn classify_commit_origin_uses_terminal_type_footer() {
    assert_eq!(
        classify_commit_origin("fix: tracked\n\nSASE_TYPE=stitch"),
        CommitOriginWire::Stitch,
    );
    assert_eq!(
        classify_commit_origin("fix: manual\n\nSASE_TYPE=stitch\n\nMore"),
        CommitOriginWire::Manual,
    );
}

#[test]
fn classify_commit_origin_distinguishes_auto_and_legacy_stitch() {
    assert_eq!(
        classify_commit_origin("fix: automatic\n\nSASE_TYPE=sase init"),
        CommitOriginWire::Auto,
    );
    assert_eq!(
        classify_commit_origin("fix: legacy\n\nSASE_AGENT=sase-1"),
        CommitOriginWire::Stitch,
    );
}

// -- classify_commit_presence --------------------------------------------

#[test]
fn classify_commit_presence_marks_synced_local_and_remote() {
    let out = classify_commit_presence(
        vec![
            commit("synced", 300, "synced"),
            commit("local", 200, "local"),
            commit("remote", 100, "remote"),
        ],
        vec!["local".to_string()],
        vec!["remote".to_string()],
    );
    let states: Vec<CommitPresenceWire> =
        out.iter().map(|commit| commit.presence).collect();
    assert_eq!(
        states,
        vec![
            CommitPresenceWire::Synced,
            CommitPresenceWire::LocalOnly,
            CommitPresenceWire::RemoteOnly,
        ]
    );
}
