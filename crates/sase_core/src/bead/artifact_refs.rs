//! Artifact IDs referenced by bead state.
//!
//! `projection-off` takes `issues.jsonl` off the per-mutation path, so the
//! artifact-prune protection scan in sase's `artifact_file_protection.py`
//! can no longer read bead references from that file. This query returns the
//! same IDs from current state, served by the read model through
//! `read_store_issues`, by scanning the canonical projection text — the
//! exact bytes `export_issues_to_jsonl` writes — for `file:` artifact IDs.
//! Scanning the serialized form instead of only the structured `refs` lists
//! keeps protection coverage identical to the old text scan: IDs in notes,
//! descriptions, and link rows stay protected.

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::OnceLock;

use regex::Regex;

use super::jsonl::export_issues_to_jsonl;
use super::read::read_store_issues;
use super::wire::{BeadError, IssueWire};

/// Wire schema version for the `bead_referenced_artifact_ids` output.
pub const BEAD_REFERENCED_ARTIFACT_IDS_WIRE_SCHEMA_VERSION: u32 = 1;

fn artifact_id_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"(?:file:)?((?:default|explicit):[0-9a-f]{24})")
            .expect("bead artifact ID pattern must compile")
    })
}

/// Return the artifact IDs named anywhere in one projection text.
///
/// This matches sase's `artifact_file_protection.py` scan, which finds
/// `(?:file:)?((?:default|explicit):[0-9a-f]{24})` in `issues.jsonl` text
/// and protects the captured ID without the `file:` prefix. Output is
/// sorted and deduplicated.
pub fn referenced_artifact_ids_in_projection_text(text: &str) -> Vec<String> {
    let mut ids = BTreeSet::new();
    for captures in artifact_id_re().captures_iter(text) {
        ids.insert(captures[1].to_string());
    }
    ids.into_iter().collect()
}

/// Return the artifact IDs referenced by `issues`, via their canonical
/// projection text.
pub fn referenced_artifact_ids_in_issues(
    issues: &[IssueWire],
) -> Result<Vec<String>, BeadError> {
    let jsonl = export_issues_to_jsonl(issues)?;
    Ok(referenced_artifact_ids_in_projection_text(&jsonl))
}

/// Return the sorted, deduplicated artifact IDs referenced by the current
/// bead state at `beads_dir`.
///
/// Reads through the read model, so this never replays history on the hot
/// path and never touches `issues.jsonl`.
pub fn bead_referenced_artifact_ids(
    beads_dir: &Path,
) -> Result<Vec<String>, BeadError> {
    let issues = read_store_issues(beads_dir)?;
    referenced_artifact_ids_in_issues(&issues)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    fn write(path: &std::path::Path, text: &str) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(path, text).unwrap();
    }

    #[test]
    fn projection_text_scan_matches_protection_coverage() {
        let id_a = "default:0123456789abcdef01234567";
        let id_b = "explicit:89abcdef0123456789abcdef";
        let text = format!(
            "{{\"id\":\"demo-1\",\"refs\":[\"file:{id_a}\"],\
             \"description\":\"see file:{id_b} for {id_a}\"}}\n\
             {{\"id\":\"demo-2\",\"notes\":\"no references here\"}}\n"
        );
        assert_eq!(
            referenced_artifact_ids_in_projection_text(&text),
            vec![id_a.to_string(), id_b.to_string()],
        );
    }

    #[test]
    fn projection_text_scan_ignores_non_ids() {
        let text = "{\"id\":\"demo-1\",\"refs\":[\"research:202610/notes.md\",\
                   \"plan:sase-1h8\",\"default:short\"]}\n";
        assert!(referenced_artifact_ids_in_projection_text(text).is_empty());
    }

    #[test]
    fn legacy_store_reports_bead_referenced_ids() {
        let temp = tempdir().unwrap();
        let beads_dir = temp.path().join("beads");
        let id = "default:0123456789abcdef01234567";
        write(
            &beads_dir.join("issues.jsonl"),
            &format!(
                "{{\"id\":\"health-1\",\"title\":\"Health\",\"status\":\"open\",\
                  \"issue_type\":\"plan\",\"parent_id\":null,\"owner\":\"\",\
                  \"assignee\":\"\",\"created_at\":\"2026-01-01T00:00:00Z\",\
                  \"created_by\":\"\",\"updated_at\":\"2026-01-01T00:00:00Z\",\
                  \"closed_at\":null,\"close_reason\":null,\
                  \"description\":\"\",\"notes\":\"\",\"design\":\"\",\
                  \"is_ready_to_work\":false,\"changespec_name\":\"\",\
                  \"changespec_bug_id\":\"\",\"dependencies\":[],\
                  \"refs\":[\"file:{id}\"]}}\n"
            ),
        );
        assert_eq!(
            bead_referenced_artifact_ids(&beads_dir).unwrap(),
            vec![id.to_string()],
        );
    }

    #[test]
    fn store_without_references_reports_empty() {
        let temp = tempdir().unwrap();
        let beads_dir = temp.path().join("beads");
        write(&beads_dir.join("issues.jsonl"), "");
        assert!(bead_referenced_artifact_ids(&beads_dir).unwrap().is_empty());
    }

    #[test]
    fn missing_store_is_an_error() {
        let temp = tempdir().unwrap();
        let error = bead_referenced_artifact_ids(&temp.path().join("missing"))
            .unwrap_err();
        assert_eq!(error.kind, "io");
    }
}
