//! Phase 1C parity gate: parse the canonical golden corpus and compare
//! against the same JSON shape `sase_100`'s Python parser produces (via
//! `tests/test_core_golden.py::test_changespec_wire_json_snapshot`).
//!
//! The fixtures under `tests/fixtures/` are byte-for-byte copies of
//! `sase_100/tests/core_golden/*.sase`. The expected JSON is embedded as
//! literal `serde_json::Value` so this crate stays runnable without the
//! Python toolchain.
//!
//! ## Documented normalization: `source_span.end_line`
//!
//! Python's `changespec_to_wire` defaults `end_line == start_line` because
//! the Python parser doesn't track end positions. Rust's parser tracks
//! real end lines, which the plan in
//! `sase_100/plans/202604/rust_backend_phase1.md` explicitly calls out as
//! intentional.
//!
//! Until Phase 1F decides whether to backfill end-line tracking in Python
//! or normalize at the parity boundary, this test normalizes by setting
//! `end_line = start_line` on the Rust side before comparing. Real-end-line
//! parsing is exercised by parser unit tests instead.

use sase_core::parse_project_bytes;
use serde_json::{json, Value};

const MYPROJ_SASE: &[u8] = include_bytes!("fixtures/myproj.sase");
const MYPROJ_ARCHIVE_SASE: &[u8] =
    include_bytes!("fixtures/myproj-archive.sase");

fn parse_to_json(path: &str, data: &[u8]) -> Vec<Value> {
    let specs = parse_project_bytes(path, data).expect("parse should succeed");
    specs
        .into_iter()
        .map(|s| {
            let mut v = serde_json::to_value(s).unwrap();
            // See module docstring: Rust tracks real end_line; Python does not.
            // Normalize so the comparison is meaningful for the rest of the wire.
            let span =
                v.get_mut("source_span").unwrap().as_object_mut().unwrap();
            let start = span.get("start_line").unwrap().clone();
            span.insert("end_line".to_string(), start);
            v
        })
        .collect()
}

#[test]
fn project_corpus_matches_python_golden_after_end_line_normalization() {
    let actual = parse_to_json("myproj.sase", MYPROJ_SASE);
    let expected: Value = json!([
        {
            "schema_version": 2,
            "name": "alpha",
            "project_basename": "myproj",
            "file_path": "myproj.sase",
            "source_span": {
                "file_path": "myproj.sase",
                "start_line": 2,
                "end_line": 2
            },
            "status": "Submitted",
            "parent": null,
            "cl_or_pr": "https://example.test/repo/pull/1",
            "bug": "BUG-100",
            "description": "Initial feature work.\nSpans multiple lines.",
            "commits": [
                {
                    "number": 1,
                    "note": "[run] Initial Commit",
                    "chat": "~/.sase/chats/alpha.md (0s)",
                    "diff": "~/.sase/diffs/alpha.diff",
                    "plan": null,
                    "proposal_letter": null,
                    "suffix": null,
                    "suffix_type": null,
                    "body": []
                }
            ],
            "hooks": [
                {
                    "command": "just lint",
                    "status_lines": [
                        {
                            "commit_entry_num": "1",
                            "timestamp": "260101_120000",
                            "status": "PASSED",
                            "duration": "3s",
                            "suffix": null,
                            "suffix_type": null,
                            "summary": null
                        }
                    ]
                }
            ],
            "comments": [
                {
                    "reviewer": "critique",
                    "file_path": "~/.sase/comments/alpha.json",
                    "suffix": "Unresolved Critique Comments",
                    "suffix_type": "error"
                }
            ],
            "mentors": [
                {
                    "entry_id": "1",
                    "profiles": ["profileA"],
                    "status_lines": [
                        {
                            "profile_name": "profileA",
                            "mentor_name": "mentor1",
                            "status": "PASSED",
                            "timestamp": "260101_130000",
                            "duration": "1m0s",
                            "suffix": null,
                            "suffix_type": "plain"
                        }
                    ],
                    "is_draft": false
                }
            ],
            "timestamps": [
                {
                    "timestamp": "260101_120000",
                    "event_type": "STATUS",
                    "detail": "WIP -> Submitted"
                }
            ],
            "deltas": [
                {"path": "src/alpha.py", "change_type": "A"},
                {"path": "src/util.py", "change_type": "M"}
            ]
        },
        {
            "schema_version": 2,
            "name": "beta",
            "project_basename": "myproj",
            "file_path": "myproj.sase",
            "source_span": {
                "file_path": "myproj.sase",
                "start_line": 29,
                "end_line": 29
            },
            "status": "WIP",
            "parent": "alpha",
            "cl_or_pr": null,
            "bug": null,
            "description": "Sibling feature.",
            "commits": [],
            "hooks": [],
            "comments": [],
            "mentors": [],
            "timestamps": [],
            "deltas": []
        },
        {
            "schema_version": 2,
            "name": "beta__260102_010101",
            "project_basename": "myproj",
            "file_path": "myproj.sase",
            "source_span": {
                "file_path": "myproj.sase",
                "start_line": 36,
                "end_line": 36
            },
            "status": "Reverted",
            "parent": "alpha",
            "cl_or_pr": null,
            "bug": null,
            "description": "Reverted retry of beta.",
            "commits": [],
            "hooks": [],
            "comments": [],
            "mentors": [],
            "timestamps": [],
            "deltas": []
        },
        {
            "schema_version": 2,
            "name": "gamma",
            "project_basename": "myproj",
            "file_path": "myproj.sase",
            "source_span": {
                "file_path": "myproj.sase",
                "start_line": 43,
                "end_line": 43
            },
            "status": "Ready",
            "parent": null,
            "cl_or_pr": null,
            "bug": null,
            "description": "Ready feature with running agent.",
            "commits": [],
            "hooks": [
                {
                    "command": "just test",
                    "status_lines": [
                        {
                            "commit_entry_num": "1",
                            "timestamp": "260103_140000",
                            "status": "RUNNING",
                            "duration": null,
                            "suffix": "ace-260103_140000",
                            "suffix_type": "running_agent",
                            "summary": null
                        }
                    ]
                }
            ],
            "comments": [],
            "mentors": [],
            "timestamps": [],
            "deltas": []
        }
    ]);

    pretty_assert(&Value::Array(actual), &expected);
}

#[test]
fn archive_corpus_matches_python_golden_after_end_line_normalization() {
    let actual = parse_to_json("myproj-archive.sase", MYPROJ_ARCHIVE_SASE);
    let expected: Value = json!([
        {
            "schema_version": 2,
            "name": "archived_one",
            "project_basename": "myproj",
            "file_path": "myproj-archive.sase",
            "source_span": {
                "file_path": "myproj-archive.sase",
                "start_line": 1,
                "end_line": 1
            },
            "status": "Archived",
            "parent": null,
            "cl_or_pr": "https://example.test/repo/pull/99",
            "bug": null,
            "description": "An archived spec.",
            "commits": [
                {
                    "number": 1,
                    "note": "[run] Initial Commit",
                    "chat": "~/.sase/chats/archived_one.md (0s)",
                    "diff": null,
                    "plan": null,
                    "proposal_letter": null,
                    "suffix": null,
                    "suffix_type": null,
                    "body": []
                }
            ],
            "hooks": [],
            "comments": [],
            "mentors": [],
            "timestamps": [],
            "deltas": []
        },
        {
            "schema_version": 2,
            "name": "reverted_two",
            "project_basename": "myproj",
            "file_path": "myproj-archive.sase",
            "source_span": {
                "file_path": "myproj-archive.sase",
                "start_line": 11,
                "end_line": 11
            },
            "status": "Reverted",
            "parent": null,
            "cl_or_pr": null,
            "bug": null,
            "description": "A reverted spec.",
            "commits": [],
            "hooks": [],
            "comments": [],
            "mentors": [],
            "timestamps": [],
            "deltas": []
        }
    ]);

    pretty_assert(&Value::Array(actual), &expected);
}

#[test]
fn rust_real_end_line_is_strictly_greater_than_python_placeholder() {
    // Sanity check on the documented normalization: at least one spec in
    // the corpus should have a real end_line that's > start_line, proving
    // Rust's parser is actually doing better than Python's placeholder.
    let specs = parse_project_bytes("myproj.sase", MYPROJ_SASE).unwrap();
    let alpha = specs.iter().find(|s| s.name == "alpha").unwrap();
    assert_eq!(alpha.source_span.start_line, 2);
    assert!(
        alpha.source_span.end_line > alpha.source_span.start_line,
        "expected real end_line > start_line for `alpha`, got {}",
        alpha.source_span.end_line
    );
}

/// Print a structural diff if assertion fails — useful when one nested
/// dict differs and `assert_eq!` truncates the Display output.
fn pretty_assert(actual: &Value, expected: &Value) {
    if actual != expected {
        let a = serde_json::to_string_pretty(actual).unwrap();
        let e = serde_json::to_string_pretty(expected).unwrap();
        panic!("JSON mismatch.\n--- actual ---\n{a}\n--- expected ---\n{e}\n");
    }
}
