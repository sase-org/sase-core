//! Wire records mirroring `sase_100/src/sase/core/wire.py`.
//!
//! These types are the stable boundary between the Rust parser and Python.
//! JSON shape rules:
//!
//! - `Option<T>::None` serializes as JSON `null` (not omitted).
//! - Empty list fields serialize as `[]` (never `null`).
//! - All field names are lowercase `snake_case` (serde default).
//! - `schema_version` lives at the top of `PatchWire` / `ChangeSpecWire` so a
//!   Rust parser can refuse to deserialize newer records.

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Schema version mirrored from `wire.py::CHANGESPEC_WIRE_SCHEMA_VERSION`.
pub const CHANGESPEC_WIRE_SCHEMA_VERSION: u32 = 5;

/// Canonical Patch/Stitch schema version.
///
/// The serialized fields are compatible with `CHANGESPEC_WIRE_SCHEMA_VERSION`;
/// this alias lets canonical callers stop depending on the legacy constant
/// name without implying a storage-format bump.
pub const PATCH_WIRE_SCHEMA_VERSION: u32 = CHANGESPEC_WIRE_SCHEMA_VERSION;

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
pub struct StitchWire {
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

impl From<CommitWire> for StitchWire {
    fn from(commit: CommitWire) -> Self {
        Self {
            number: commit.number,
            note: commit.note,
            chat: commit.chat,
            diff: commit.diff,
            plan: commit.plan,
            proposal_letter: commit.proposal_letter,
            suffix: commit.suffix,
            suffix_type: commit.suffix_type,
            body: commit.body,
        }
    }
}

impl From<StitchWire> for CommitWire {
    fn from(stitch: StitchWire) -> Self {
        Self {
            number: stitch.number,
            note: stitch.note,
            chat: stitch.chat,
            diff: stitch.diff,
            plan: stitch.plan,
            proposal_letter: stitch.proposal_letter,
            suffix: stitch.suffix,
            suffix_type: stitch.suffix_type,
            body: stitch.body,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HookStatusLineWire {
    #[serde(alias = "stitch_id")]
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
pub struct PatchHookStatusLineWire {
    #[serde(alias = "commit_entry_num")]
    pub stitch_id: String,
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

impl From<HookStatusLineWire> for PatchHookStatusLineWire {
    fn from(status_line: HookStatusLineWire) -> Self {
        Self {
            stitch_id: status_line.commit_entry_num,
            timestamp: status_line.timestamp,
            status: status_line.status,
            duration: status_line.duration,
            suffix: status_line.suffix,
            suffix_type: status_line.suffix_type,
            summary: status_line.summary,
        }
    }
}

impl From<PatchHookStatusLineWire> for HookStatusLineWire {
    fn from(status_line: PatchHookStatusLineWire) -> Self {
        Self {
            commit_entry_num: status_line.stitch_id,
            timestamp: status_line.timestamp,
            status: status_line.status,
            duration: status_line.duration,
            suffix: status_line.suffix,
            suffix_type: status_line.suffix_type,
            summary: status_line.summary,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HookWire {
    pub command: String,
    #[serde(default)]
    pub status_lines: Vec<HookStatusLineWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PatchHookWire {
    pub command: String,
    #[serde(default)]
    pub status_lines: Vec<PatchHookStatusLineWire>,
}

impl From<HookWire> for PatchHookWire {
    fn from(hook: HookWire) -> Self {
        Self {
            command: hook.command,
            status_lines: hook
                .status_lines
                .into_iter()
                .map(Into::into)
                .collect(),
        }
    }
}

impl From<PatchHookWire> for HookWire {
    fn from(hook: PatchHookWire) -> Self {
        Self {
            command: hook.command,
            status_lines: hook
                .status_lines
                .into_iter()
                .map(Into::into)
                .collect(),
        }
    }
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
    #[serde(alias = "stitch_id")]
    pub entry_id: String,
    #[serde(default)]
    pub profiles: Vec<String>,
    #[serde(default)]
    pub status_lines: Vec<MentorStatusLineWire>,
    #[serde(default)]
    pub is_draft: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PatchMentorWire {
    #[serde(alias = "entry_id")]
    pub stitch_id: String,
    #[serde(default)]
    pub profiles: Vec<String>,
    #[serde(default)]
    pub status_lines: Vec<MentorStatusLineWire>,
    #[serde(default)]
    pub is_draft: bool,
}

impl From<MentorWire> for PatchMentorWire {
    fn from(mentor: MentorWire) -> Self {
        Self {
            stitch_id: mentor.entry_id,
            profiles: mentor.profiles,
            status_lines: mentor.status_lines,
            is_draft: mentor.is_draft,
        }
    }
}

impl From<PatchMentorWire> for MentorWire {
    fn from(mentor: PatchMentorWire) -> Self {
        Self {
            entry_id: mentor.stitch_id,
            profiles: mentor.profiles,
            status_lines: mentor.status_lines,
            is_draft: mentor.is_draft,
        }
    }
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

/// The full parsed wire form of one Patch.
///
/// This is the canonical Rust contract for new callers. It serializes
/// stitch-bearing fields with canonical names while accepting the legacy
/// `ChangeSpecWire` shape during deserialization.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PatchWire {
    pub schema_version: u32,
    pub name: String,
    pub project_basename: String,
    /// Configured user-facing project name from the ProjectSpec metadata
    /// header. Query evaluation falls back to the canonical directory key.
    #[serde(default)]
    pub project_display_name: Option<String>,
    pub file_path: String,
    pub source_span: SourceSpanWire,
    pub status: String,
    pub parent: Option<String>,
    #[serde(alias = "cl_or_pr")]
    pub pr_url: Option<String>,
    pub bug: Option<String>,
    pub description: String,
    #[serde(default)]
    pub refs: Vec<String>,
    #[serde(default, alias = "commits")]
    pub stitches: Vec<StitchWire>,
    #[serde(default)]
    pub hooks: Vec<PatchHookWire>,
    #[serde(default)]
    pub comments: Vec<CommentWire>,
    #[serde(default)]
    pub mentors: Vec<PatchMentorWire>,
    #[serde(default)]
    pub timestamps: Vec<TimestampWire>,
    #[serde(default)]
    pub deltas: Vec<DeltaWire>,
}

impl From<ChangeSpecWire> for PatchWire {
    fn from(spec: ChangeSpecWire) -> Self {
        Self {
            schema_version: spec.schema_version,
            name: spec.name,
            project_basename: spec.project_basename,
            project_display_name: spec.project_display_name,
            file_path: spec.file_path,
            source_span: spec.source_span,
            status: spec.status,
            parent: spec.parent,
            pr_url: spec.pr_url,
            bug: spec.bug,
            description: spec.description,
            refs: spec.refs,
            stitches: spec.commits.into_iter().map(Into::into).collect(),
            hooks: spec.hooks.into_iter().map(Into::into).collect(),
            comments: spec.comments,
            mentors: spec.mentors.into_iter().map(Into::into).collect(),
            timestamps: spec.timestamps,
            deltas: spec.deltas,
        }
    }
}

impl From<PatchWire> for ChangeSpecWire {
    fn from(patch: PatchWire) -> Self {
        Self {
            schema_version: patch.schema_version,
            name: patch.name,
            project_basename: patch.project_basename,
            project_display_name: patch.project_display_name,
            file_path: patch.file_path,
            source_span: patch.source_span,
            status: patch.status,
            parent: patch.parent,
            pr_url: patch.pr_url,
            bug: patch.bug,
            description: patch.description,
            refs: patch.refs,
            commits: patch.stitches.into_iter().map(Into::into).collect(),
            hooks: patch.hooks.into_iter().map(Into::into).collect(),
            comments: patch.comments,
            mentors: patch.mentors.into_iter().map(Into::into).collect(),
            timestamps: patch.timestamps,
            deltas: patch.deltas,
        }
    }
}

/// Legacy parsed wire form of one ChangeSpec.
///
/// Field order matches `wire.py::ChangeSpecWire` so JSON output is identical
/// when serialized with order-preserving serializers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangeSpecWire {
    pub schema_version: u32,
    pub name: String,
    pub project_basename: String,
    /// Configured user-facing project name from the ProjectSpec metadata
    /// header. Query evaluation falls back to the canonical directory key.
    #[serde(default)]
    pub project_display_name: Option<String>,
    pub file_path: String,
    pub source_span: SourceSpanWire,
    pub status: String,
    pub parent: Option<String>,
    #[serde(alias = "cl_or_pr")]
    pub pr_url: Option<String>,
    pub bug: Option<String>,
    pub description: String,
    #[serde(default)]
    pub refs: Vec<String>,
    #[serde(default, alias = "stitches")]
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
            project_display_name: None,
            file_path: "myproj.sase".to_string(),
            source_span: empty_span(),
            status: "WIP".to_string(),
            parent: None,
            pr_url: None,
            bug: None,
            description: "".to_string(),
            refs: vec![],
            commits: vec![],
            hooks: vec![],
            comments: vec![],
            mentors: vec![],
            timestamps: vec![],
            deltas: vec![],
        };
        let json = serde_json::to_value(&cs).unwrap();
        for key in [
            "refs",
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
            schema_version: CHANGESPEC_WIRE_SCHEMA_VERSION,
            name: "n".to_string(),
            project_basename: "p".to_string(),
            project_display_name: None,
            file_path: "p.sase".to_string(),
            source_span: empty_span(),
            status: "WIP".to_string(),
            parent: None,
            pr_url: None,
            bug: None,
            description: "".to_string(),
            refs: vec![],
            commits: vec![],
            hooks: vec![],
            comments: vec![],
            mentors: vec![],
            timestamps: vec![],
            deltas: vec![],
        };
        let json = serde_json::to_value(&cs).unwrap();
        for key in ["parent", "pr_url", "bug"] {
            assert_eq!(json.get(key), Some(&Value::Null), "{key} must be null");
        }
    }

    #[test]
    fn legacy_cl_or_pr_key_deserializes_as_pr_url() {
        let json = json!({
            "schema_version": 2,
            "name": "n",
            "project_basename": "p",
            "file_path": "p.sase",
            "source_span": {
                "file_path": "p.sase",
                "start_line": 1,
                "end_line": 10,
            },
            "status": "WIP",
            "parent": null,
            "cl_or_pr": "https://example.test/repo/pull/1",
            "bug": null,
            "description": "",
            "commits": [],
            "hooks": [],
            "comments": [],
            "mentors": [],
            "timestamps": [],
            "deltas": [],
        });
        let cs: ChangeSpecWire = serde_json::from_value(json).unwrap();
        assert_eq!(
            cs.pr_url.as_deref(),
            Some("https://example.test/repo/pull/1")
        );
        assert_eq!(cs.project_display_name, None);
    }

    #[test]
    fn changespec_field_order_matches_python() {
        // Python uses `dataclasses.asdict`, which preserves declaration order.
        // We replicate that order so byte-for-byte JSON parity is reachable.
        let cs = ChangeSpecWire {
            schema_version: CHANGESPEC_WIRE_SCHEMA_VERSION,
            name: "n".to_string(),
            project_basename: "p".to_string(),
            project_display_name: None,
            file_path: "p.sase".to_string(),
            source_span: empty_span(),
            status: "WIP".to_string(),
            parent: None,
            pr_url: None,
            bug: None,
            description: "".to_string(),
            refs: vec![],
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
            "project_display_name",
            "file_path",
            "source_span",
            "status",
            "parent",
            "pr_url",
            "bug",
            "description",
            "refs",
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
    fn patch_wire_serializes_canonical_stitch_keys() {
        let patch = PatchWire {
            schema_version: PATCH_WIRE_SCHEMA_VERSION,
            name: "patch_one".to_string(),
            project_basename: "proj".to_string(),
            project_display_name: Some("Widgets".to_string()),
            file_path: "proj.sase".to_string(),
            source_span: empty_span(),
            status: "WIP".to_string(),
            parent: None,
            pr_url: None,
            bug: None,
            description: "work".to_string(),
            refs: vec![],
            stitches: vec![StitchWire {
                number: 2,
                note: "proposal".to_string(),
                chat: None,
                diff: None,
                plan: None,
                proposal_letter: Some("a".to_string()),
                suffix: None,
                suffix_type: None,
                body: vec![],
            }],
            hooks: vec![PatchHookWire {
                command: "just test".to_string(),
                status_lines: vec![PatchHookStatusLineWire {
                    stitch_id: "2a".to_string(),
                    timestamp: "260101_120000".to_string(),
                    status: "PASSED".to_string(),
                    duration: Some("3s".to_string()),
                    suffix: None,
                    suffix_type: None,
                    summary: None,
                }],
            }],
            comments: vec![],
            mentors: vec![PatchMentorWire {
                stitch_id: "2a".to_string(),
                profiles: vec!["default".to_string()],
                status_lines: vec![],
                is_draft: false,
            }],
            timestamps: vec![],
            deltas: vec![],
        };

        let json = serde_json::to_value(&patch).unwrap();
        assert!(json.get("stitches").is_some());
        assert!(json.get("commits").is_none());
        assert_eq!(json["stitches"][0]["proposal_letter"], json!("a"));
        assert_eq!(
            json["hooks"][0]["status_lines"][0]["stitch_id"],
            json!("2a")
        );
        assert!(json["hooks"][0]["status_lines"][0]
            .get("commit_entry_num")
            .is_none());
        assert_eq!(json["mentors"][0]["stitch_id"], json!("2a"));
        assert!(json["mentors"][0].get("entry_id").is_none());
    }

    #[test]
    fn legacy_changespec_wire_deserializes_canonical_patch_shape() {
        let json = json!({
            "schema_version": PATCH_WIRE_SCHEMA_VERSION,
            "name": "patch_one",
            "project_basename": "proj",
            "project_display_name": "Widgets",
            "file_path": "proj.sase",
            "source_span": {
                "file_path": "proj.sase",
                "start_line": 1,
                "end_line": 10,
            },
            "status": "WIP",
            "parent": null,
            "pr_url": null,
            "bug": null,
            "description": "work",
            "refs": [],
            "stitches": [{
                "number": 2,
                "note": "proposal",
                "proposal_letter": "a",
                "body": [],
            }],
            "hooks": [{
                "command": "just test",
                "status_lines": [{
                    "stitch_id": "2a",
                    "timestamp": "260101_120000",
                    "status": "PASSED",
                }],
            }],
            "comments": [],
            "mentors": [{
                "stitch_id": "2a",
                "profiles": ["default"],
                "status_lines": [],
                "is_draft": false,
            }],
            "timestamps": [],
            "deltas": [],
        });

        let spec: ChangeSpecWire = serde_json::from_value(json).unwrap();
        assert_eq!(spec.commits.len(), 1);
        assert_eq!(spec.commits[0].proposal_letter.as_deref(), Some("a"));
        assert_eq!(
            spec.hooks[0].status_lines[0].commit_entry_num,
            "2a".to_string()
        );
        assert_eq!(spec.mentors[0].entry_id, "2a".to_string());
    }

    #[test]
    fn patch_wire_deserializes_legacy_changespec_shape() {
        let json = json!({
            "schema_version": CHANGESPEC_WIRE_SCHEMA_VERSION,
            "name": "legacy_spec",
            "project_basename": "proj",
            "file_path": "proj.sase",
            "source_span": {
                "file_path": "proj.sase",
                "start_line": 1,
                "end_line": 10,
            },
            "status": "WIP",
            "parent": null,
            "pr_url": null,
            "bug": null,
            "description": "work",
            "commits": [{
                "number": 1,
                "note": "initial",
                "body": [],
            }],
            "hooks": [{
                "command": "just test",
                "status_lines": [{
                    "commit_entry_num": "1",
                    "timestamp": "260101_120000",
                    "status": "PASSED",
                }],
            }],
            "comments": [],
            "mentors": [{
                "entry_id": "1",
                "profiles": ["default"],
                "status_lines": [],
                "is_draft": false,
            }],
            "timestamps": [],
            "deltas": [],
        });

        let patch: PatchWire = serde_json::from_value(json).unwrap();
        assert_eq!(patch.stitches.len(), 1);
        assert_eq!(patch.stitches[0].note, "initial");
        assert_eq!(patch.hooks[0].status_lines[0].stitch_id, "1".to_string());
        assert_eq!(patch.mentors[0].stitch_id, "1".to_string());
    }

    #[test]
    fn populated_changespec_round_trips() {
        let cs = ChangeSpecWire {
            schema_version: CHANGESPEC_WIRE_SCHEMA_VERSION,
            name: "rust_workspace".to_string(),
            project_basename: "myproj".to_string(),
            project_display_name: Some("widgets".to_string()),
            file_path: "myproj.sase".to_string(),
            source_span: SourceSpanWire {
                file_path: "myproj.sase".to_string(),
                start_line: 5,
                end_line: 42,
            },
            status: "WIP".to_string(),
            parent: Some("parent_cl".to_string()),
            pr_url: Some("123".to_string()),
            bug: None,
            description: "first line\nsecond line".to_string(),
            refs: vec!["research:202607/report.md".to_string()],
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
