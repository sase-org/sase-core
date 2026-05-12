//! Wire records mirroring `sase_100/src/sase/core/wire.py`.
//!
//! These types are the stable boundary between the Rust parser and Python.
//! JSON shape rules:
//!
//! - `Option<T>::None` serializes as JSON `null` (not omitted).
//! - Empty list fields serialize as `[]` (never `null`).
//! - All field names are lowercase `snake_case` (serde default).
//! - `schema_version` lives at the top of `ChangeSpecWire` so a Rust parser
//!   can refuse to deserialize newer records.

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Schema version mirrored from `wire.py::CHANGESPEC_WIRE_SCHEMA_VERSION`.
pub const CHANGESPEC_WIRE_SCHEMA_VERSION: u32 = 1;

/// Inclusive 1-based line range pointing into the source file.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SourceSpanWire {
    pub file_path: String,
    pub start_line: u32,
    pub end_line: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitWire {
    pub number: u32,
    pub note: String,
    #[serde(default)]
    pub chat: Option<String>,
    #[serde(default)]
    pub diff: Option<String>,
    #[serde(default)]
    pub plan: Option<String>,
    #[serde(default)]
    pub proposal_letter: Option<String>,
    #[serde(default)]
    pub suffix: Option<String>,
    #[serde(default)]
    pub suffix_type: Option<String>,
    #[serde(default)]
    pub body: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HookStatusLineWire {
    pub commit_entry_num: String,
    pub timestamp: String,
    pub status: String,
    #[serde(default)]
    pub duration: Option<String>,
    #[serde(default)]
    pub suffix: Option<String>,
    #[serde(default)]
    pub suffix_type: Option<String>,
    #[serde(default)]
    pub summary: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HookWire {
    pub command: String,
    #[serde(default)]
    pub status_lines: Vec<HookStatusLineWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommentWire {
    pub reviewer: String,
    pub file_path: String,
    #[serde(default)]
    pub suffix: Option<String>,
    #[serde(default)]
    pub suffix_type: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MentorStatusLineWire {
    pub profile_name: String,
    pub mentor_name: String,
    pub status: String,
    #[serde(default)]
    pub timestamp: Option<String>,
    #[serde(default)]
    pub duration: Option<String>,
    #[serde(default)]
    pub suffix: Option<String>,
    #[serde(default)]
    pub suffix_type: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MentorWire {
    pub entry_id: String,
    #[serde(default)]
    pub profiles: Vec<String>,
    #[serde(default)]
    pub status_lines: Vec<MentorStatusLineWire>,
    #[serde(default)]
    pub is_draft: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimestampWire {
    pub timestamp: String,
    pub event_type: String,
    pub detail: String,
}

/// `change_type` uses the long form ("A", "M", "D"). On-disk glyphs
/// (`+`, `~`, `-`) are a formatting concern and stay out of the wire shape.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeltaWire {
    pub path: String,
    pub change_type: String,
}

/// The full parsed wire form of one ChangeSpec.
///
/// Field order matches `wire.py::ChangeSpecWire` so JSON output is identical
/// when serialized with order-preserving serializers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangeSpecWire {
    pub schema_version: u32,
    pub name: String,
    pub project_basename: String,
    pub file_path: String,
    pub source_span: SourceSpanWire,
    pub status: String,
    pub parent: Option<String>,
    pub cl_or_pr: Option<String>,
    pub bug: Option<String>,
    pub description: String,
    #[serde(default)]
    pub test_targets: Vec<String>,
    #[serde(default)]
    pub commits: Vec<CommitWire>,
    #[serde(default)]
    pub hooks: Vec<HookWire>,
    #[serde(default)]
    pub comments: Vec<CommentWire>,
    #[serde(default)]
    pub mentors: Vec<MentorWire>,
    #[serde(default)]
    pub timestamps: Vec<TimestampWire>,
    #[serde(default)]
    pub deltas: Vec<DeltaWire>,
}

/// Structured error a Rust parser may emit instead of a `ChangeSpecWire`.
///
/// Also exposed as a `thiserror::Error` so callers can use `?` on parser
/// results without a manual conversion. `kind` is a stable string tag (e.g.
/// `"io"`, `"syntax"`) the Python side can branch on.
#[derive(Debug, Clone, PartialEq, Eq, Error, Serialize, Deserialize)]
#[error("{kind}: {message} ({file_path})")]
pub struct ParseErrorWire {
    pub kind: String,
    pub message: String,
    pub file_path: String,
    #[serde(default)]
    pub line: Option<u32>,
    #[serde(default)]
    pub column: Option<u32>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

    fn empty_span() -> SourceSpanWire {
        SourceSpanWire {
            file_path: "p.sase".to_string(),
            start_line: 1,
            end_line: 10,
        }
    }

    #[test]
    fn source_span_round_trips() {
        let span = empty_span();
        let json = serde_json::to_value(&span).unwrap();
        assert_eq!(
            json,
            json!({
                "file_path": "p.sase",
                "start_line": 1,
                "end_line": 10,
            })
        );
        let back: SourceSpanWire = serde_json::from_value(json).unwrap();
        assert_eq!(back, span);
    }

    #[test]
    fn empty_lists_serialize_as_arrays_not_null() {
        // Mirrors the Python invariant: empty list fields are `[]`, not `null`.
        let cs = ChangeSpecWire {
            schema_version: CHANGESPEC_WIRE_SCHEMA_VERSION,
            name: "my_cl".to_string(),
            project_basename: "myproj".to_string(),
            file_path: "myproj.sase".to_string(),
            source_span: empty_span(),
            status: "WIP".to_string(),
            parent: None,
            cl_or_pr: None,
            bug: None,
            description: "".to_string(),
            test_targets: vec![],
            commits: vec![],
            hooks: vec![],
            comments: vec![],
            mentors: vec![],
            timestamps: vec![],
            deltas: vec![],
        };
        let json = serde_json::to_value(&cs).unwrap();
        for key in [
            "test_targets",
            "commits",
            "hooks",
            "comments",
            "mentors",
            "timestamps",
            "deltas",
        ] {
            assert!(
                matches!(json.get(key), Some(Value::Array(_))),
                "{key} must serialize as an array, got {:?}",
                json.get(key)
            );
        }
    }

    #[test]
    fn none_fields_serialize_as_json_null() {
        let cs = ChangeSpecWire {
            schema_version: 1,
            name: "n".to_string(),
            project_basename: "p".to_string(),
            file_path: "p.sase".to_string(),
            source_span: empty_span(),
            status: "WIP".to_string(),
            parent: None,
            cl_or_pr: None,
            bug: None,
            description: "".to_string(),
            test_targets: vec![],
            commits: vec![],
            hooks: vec![],
            comments: vec![],
            mentors: vec![],
            timestamps: vec![],
            deltas: vec![],
        };
        let json = serde_json::to_value(&cs).unwrap();
        for key in ["parent", "cl_or_pr", "bug"] {
            assert_eq!(json.get(key), Some(&Value::Null), "{key} must be null");
        }
    }

    #[test]
    fn changespec_field_order_matches_python() {
        // Python uses `dataclasses.asdict`, which preserves declaration order.
        // We replicate that order so byte-for-byte JSON parity is reachable.
        let cs = ChangeSpecWire {
            schema_version: 1,
            name: "n".to_string(),
            project_basename: "p".to_string(),
            file_path: "p.sase".to_string(),
            source_span: empty_span(),
            status: "WIP".to_string(),
            parent: None,
            cl_or_pr: None,
            bug: None,
            description: "".to_string(),
            test_targets: vec![],
            commits: vec![],
            hooks: vec![],
            comments: vec![],
            mentors: vec![],
            timestamps: vec![],
            deltas: vec![],
        };
        let s = serde_json::to_string(&cs).unwrap();
        let expected_order = [
            "schema_version",
            "name",
            "project_basename",
            "file_path",
            "source_span",
            "status",
            "parent",
            "cl_or_pr",
            "bug",
            "description",
            "test_targets",
            "commits",
            "hooks",
            "comments",
            "mentors",
            "timestamps",
            "deltas",
        ];
        let mut cursor = 0usize;
        for key in expected_order {
            let needle = format!("\"{key}\"");
            let idx = s[cursor..].find(&needle).unwrap_or_else(|| {
                panic!("expected key {key} after position {cursor} in {s}");
            });
            cursor += idx + needle.len();
        }
    }

    #[test]
    fn populated_changespec_round_trips() {
        let cs = ChangeSpecWire {
            schema_version: 1,
            name: "rust_workspace".to_string(),
            project_basename: "myproj".to_string(),
            file_path: "myproj.sase".to_string(),
            source_span: SourceSpanWire {
                file_path: "myproj.sase".to_string(),
                start_line: 5,
                end_line: 42,
            },
            status: "WIP".to_string(),
            parent: Some("parent_cl".to_string()),
            cl_or_pr: Some("123".to_string()),
            bug: None,
            description: "first line\nsecond line".to_string(),
            test_targets: vec!["//foo:bar".to_string()],
            commits: vec![CommitWire {
                number: 1,
                note: "init".to_string(),
                chat: None,
                diff: None,
                plan: None,
                proposal_letter: None,
                suffix: Some("@".to_string()),
                suffix_type: Some("running".to_string()),
                body: vec!["body line".to_string()],
            }],
            hooks: vec![HookWire {
                command: "just lint".to_string(),
                status_lines: vec![HookStatusLineWire {
                    commit_entry_num: "1".to_string(),
                    timestamp: "20260429_010101".to_string(),
                    status: "OK".to_string(),
                    duration: Some("3s".to_string()),
                    suffix: None,
                    suffix_type: None,
                    summary: None,
                }],
            }],
            comments: vec![CommentWire {
                reviewer: "alice".to_string(),
                file_path: "src/foo.rs".to_string(),
                suffix: None,
                suffix_type: None,
            }],
            mentors: vec![MentorWire {
                entry_id: "m1".to_string(),
                profiles: vec!["default".to_string()],
                status_lines: vec![MentorStatusLineWire {
                    profile_name: "default".to_string(),
                    mentor_name: "claude".to_string(),
                    status: "OK".to_string(),
                    timestamp: Some("20260429_010101".to_string()),
                    duration: None,
                    suffix: None,
                    suffix_type: None,
                }],
                is_draft: false,
            }],
            timestamps: vec![TimestampWire {
                timestamp: "20260429_010101".to_string(),
                event_type: "created".to_string(),
                detail: "".to_string(),
            }],
            deltas: vec![DeltaWire {
                path: "src/lib.rs".to_string(),
                change_type: "A".to_string(),
            }],
        };
        let s = serde_json::to_string(&cs).unwrap();
        let back: ChangeSpecWire = serde_json::from_str(&s).unwrap();
        assert_eq!(back, cs);
    }

    #[test]
    fn parse_error_wire_shape() {
        let err = ParseErrorWire {
            kind: "syntax".to_string(),
            message: "unexpected EOF".to_string(),
            file_path: "p.sase".to_string(),
            line: Some(7),
            column: None,
        };
        let json = serde_json::to_value(&err).unwrap();
        assert_eq!(
            json,
            json!({
                "kind": "syntax",
                "message": "unexpected EOF",
                "file_path": "p.sase",
                "line": 7,
                "column": null,
            })
        );
        // Also implements std::error::Error via thiserror.
        let s = format!("{err}");
        assert!(s.contains("syntax"));
        assert!(s.contains("unexpected EOF"));
    }

    #[test]
    fn delta_wire_uses_long_form() {
        // The wire contract states `change_type` is "A"/"M"/"D", not glyphs.
        let d = DeltaWire {
            path: "x".to_string(),
            change_type: "A".to_string(),
        };
        let json = serde_json::to_value(&d).unwrap();
        assert_eq!(json["change_type"], json!("A"));
    }
}
