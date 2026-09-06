//! Batched exact-producer artifact-context queries for waited dependencies.
//!
//! This is the Rust half of the `wait.artifacts` contract: given an ordered
//! list of resolved producer groups (a requested wait name plus the exact
//! artifact directories that satisfy it, in stable producer order), return
//! the non-chat indexed files those producers registered. Matching is by
//! exact `agent_artifacts_dir` equality only; this deliberately does not
//! widen the existing `agent` name filter into prefix/family matching,
//! which stays a caller responsibility (see the wait-context phase).

use std::collections::HashSet;
use std::path::Path;

use serde::{Deserialize, Serialize};

use super::{
    parse_artifact_time, read_artifact_file_index, ArtifactFileQueryError,
    ArtifactFileWire,
};

pub const ARTIFACT_CONTEXT_QUERY_WIRE_SCHEMA_VERSION: u64 = 1;

/// One resolved named dependency: its requested wait name and the exact
/// producer artifact directories that satisfy it, in stable producer order.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactContextProducerGroupWire {
    pub wait_name: String,
    #[serde(default)]
    pub agent_artifacts_dirs: Vec<String>,
}

/// One projected `wait.artifacts` row for a waited producer's non-chat file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactContextEntryWire {
    pub wait_name: String,
    pub agent_name: Option<String>,
    #[serde(rename = "ref")]
    pub ref_: String,
    pub kind: Option<String>,
    pub label: Option<String>,
    pub explicit: bool,
    pub path: Option<String>,
    pub source_path: Option<String>,
    pub vcs_repo: Option<String>,
    pub vcs_sha: Option<String>,
    pub vcs_relpath: Option<String>,
}

/// Batch-query non-chat artifact metadata for waited producers' exact
/// artifact directories.
///
/// Reads the tolerant index at most once, and only when at least one
/// producer directory is requested across all groups; an all-empty batch
/// returns without touching the index. Rows are matched by exact
/// `agent_artifacts_dir` equality (never by agent name, and never a prefix
/// match), chat rows are excluded, and no file content is read, stat'd, or
/// resolved.
///
/// Results follow dependency (group) order, then producer order within a
/// dependency, then artifact creation time and ID within a producer.
/// Duplicate artifact IDs are deduplicated, retaining only the first
/// requested dependency that names them.
pub fn query_artifact_context(
    index_path: &Path,
    groups: &[ArtifactContextProducerGroupWire],
) -> Result<Vec<ArtifactContextEntryWire>, ArtifactFileQueryError> {
    if groups
        .iter()
        .all(|group| group.agent_artifacts_dirs.is_empty())
    {
        return Ok(Vec::new());
    }

    let rows = read_artifact_file_index(index_path)?;
    let mut seen_ids: HashSet<&str> = HashSet::new();
    let mut results = Vec::new();

    for group in groups {
        for producer_dir in &group.agent_artifacts_dirs {
            let mut matches: Vec<&ArtifactFileWire> = rows
                .iter()
                .filter(|row| {
                    row.agent_artifacts_dir.as_deref()
                        == Some(producer_dir.as_str())
                })
                .filter(|row| row.kind.as_deref() != Some("chat"))
                .collect();
            matches.sort_by(|left, right| {
                match (
                    left.created_at.as_deref().and_then(parse_artifact_time),
                    right.created_at.as_deref().and_then(parse_artifact_time),
                ) {
                    (Some(left_time), Some(right_time)) => {
                        left_time.cmp(&right_time)
                    }
                    (Some(_), None) => std::cmp::Ordering::Less,
                    (None, Some(_)) => std::cmp::Ordering::Greater,
                    (None, None) => std::cmp::Ordering::Equal,
                }
                .then_with(|| left.id.cmp(&right.id))
            });
            for row in matches {
                if !seen_ids.insert(row.id.as_str()) {
                    continue;
                }
                results.push(ArtifactContextEntryWire {
                    wait_name: group.wait_name.clone(),
                    agent_name: row.agent_name.clone(),
                    ref_: format!("file:{}", row.id),
                    kind: row.kind.clone(),
                    label: row.label.clone(),
                    explicit: row.explicit,
                    path: row.path.clone(),
                    source_path: row.source_path.clone(),
                    vcs_repo: row.vcs_repo.clone(),
                    vcs_sha: row.vcs_sha.clone(),
                    vcs_relpath: row.vcs_relpath.clone(),
                });
            }
        }
    }

    Ok(results)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use serde_json::json;
    use tempfile::tempdir;

    use super::*;

    fn write_index(lines: &[serde_json::Value]) -> (tempfile::TempDir, String) {
        let temp = tempdir().unwrap();
        let index = temp.path().join("index.jsonl");
        let mut content = lines
            .iter()
            .map(serde_json::Value::to_string)
            .collect::<Vec<_>>()
            .join("\n");
        content.push_str("\n{malformed\n");
        fs::write(&index, content).unwrap();
        (temp, index.to_string_lossy().into_owned())
    }

    fn row(
        id: &str,
        dir: &str,
        kind: &str,
        created_at: Option<&str>,
    ) -> serde_json::Value {
        json!({
            "schema_version": 1,
            "artifact": {
                "id": id,
                "label": format!("Label {id}"),
                "kind": kind,
                "path": format!("/stored/{id}.md"),
                "source_path": format!("/source/{id}.md"),
                "created_at": created_at,
                "project": "sase",
                "agent_artifacts_dir": dir,
                "agent_name": "producer.one",
                "explicit": false
            }
        })
    }

    fn group(
        wait_name: &str,
        dirs: &[&str],
    ) -> ArtifactContextProducerGroupWire {
        ArtifactContextProducerGroupWire {
            wait_name: wait_name.to_string(),
            agent_artifacts_dirs: dirs
                .iter()
                .map(|dir| dir.to_string())
                .collect(),
        }
    }

    #[test]
    fn empty_batch_never_opens_the_index() {
        let temp = tempdir().unwrap();
        // A directory in place of the index file: reading it as a file
        // surfaces an IO error distinct from "missing index", so an empty
        // batch returning Ok(vec![]) here proves the index was not opened.
        let index = temp.path().join("index.jsonl");
        fs::create_dir(&index).unwrap();

        assert_eq!(query_artifact_context(&index, &[]).unwrap(), Vec::new());
        assert_eq!(
            query_artifact_context(&index, &[group("a", &[]), group("b", &[])])
                .unwrap(),
            Vec::new()
        );
    }

    #[test]
    fn nonempty_batch_against_unreadable_index_is_an_error() {
        let temp = tempdir().unwrap();
        let index = temp.path().join("index.jsonl");
        fs::create_dir(&index).unwrap();

        let error =
            query_artifact_context(&index, &[group("a", &["/producers/a"])])
                .unwrap_err();
        assert!(matches!(error, ArtifactFileQueryError::Io(_)));
    }

    #[test]
    fn missing_index_tolerates_nonempty_batch_as_empty_result() {
        let temp = tempdir().unwrap();
        let index = temp.path().join("does-not-exist.jsonl");
        let rows =
            query_artifact_context(&index, &[group("a", &["/producers/a"])])
                .unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn matches_exact_producer_directory_despite_reused_agent_names() {
        let (temp, index) = write_index(&[
            row(
                "keep",
                "/producers/a-gen2",
                "markdown",
                Some("2026-07-01T00:00:00Z"),
            ),
            row(
                "stale",
                "/producers/a-gen1",
                "markdown",
                Some("2026-07-01T00:00:00Z"),
            ),
        ]);
        let rows = query_artifact_context(
            Path::new(&index),
            &[group("researcher.a", &["/producers/a-gen2"])],
        )
        .unwrap();
        assert_eq!(
            rows.iter().map(|row| row.ref_.as_str()).collect::<Vec<_>>(),
            ["file:keep"]
        );
        assert_eq!(rows[0].wait_name, "researcher.a");
        drop(temp);
    }

    #[test]
    fn excludes_chat_rows_even_when_explicitly_indexed() {
        let (temp, index) = write_index(&[
            row(
                "report",
                "/producers/a",
                "markdown",
                Some("2026-07-01T00:00:00Z"),
            ),
            row(
                "transcript",
                "/producers/a",
                "chat",
                Some("2026-07-01T00:00:00Z"),
            ),
        ]);
        let rows = query_artifact_context(
            Path::new(&index),
            &[group("a", &["/producers/a"])],
        )
        .unwrap();
        assert_eq!(
            rows.iter().map(|row| row.ref_.as_str()).collect::<Vec<_>>(),
            ["file:report"]
        );
        drop(temp);
    }

    #[test]
    fn orders_by_dependency_then_producer_then_creation_time_and_id() {
        let (temp, index) = write_index(&[
            row(
                "a-second",
                "/producers/a",
                "markdown",
                Some("2026-07-01T02:00:00Z"),
            ),
            row(
                "a-first",
                "/producers/a",
                "markdown",
                Some("2026-07-01T01:00:00Z"),
            ),
            row(
                "b-only",
                "/producers/b",
                "markdown",
                Some("2026-07-01T00:30:00Z"),
            ),
        ]);
        let rows = query_artifact_context(
            Path::new(&index),
            &[
                group("second-dep", &["/producers/b"]),
                group("first-dep", &["/producers/a"]),
            ],
        )
        .unwrap();
        assert_eq!(
            rows.iter().map(|row| row.ref_.as_str()).collect::<Vec<_>>(),
            ["file:b-only", "file:a-first", "file:a-second"]
        );
        drop(temp);
    }

    #[test]
    fn deduplicates_by_id_retaining_first_requested_dependency() {
        let (temp, index) = write_index(&[row(
            "shared",
            "/producers/a",
            "markdown",
            Some("2026-07-01T00:00:00Z"),
        )]);
        let rows = query_artifact_context(
            Path::new(&index),
            &[
                group("first-dep", &["/producers/a"]),
                group("second-dep", &["/producers/a"]),
            ],
        )
        .unwrap();
        assert_eq!(
            rows.iter()
                .map(|row| (row.wait_name.as_str(), row.ref_.as_str()))
                .collect::<Vec<_>>(),
            [("first-dep", "file:shared")]
        );
        drop(temp);
    }

    #[test]
    fn handles_multiple_producers_within_one_dependency_in_given_order() {
        let (temp, index) = write_index(&[
            row(
                "from-second",
                "/producers/gen2",
                "markdown",
                Some("2026-07-02T00:00:00Z"),
            ),
            row(
                "from-first",
                "/producers/gen1",
                "markdown",
                Some("2026-07-01T00:00:00Z"),
            ),
        ]);
        let rows = query_artifact_context(
            Path::new(&index),
            &[group(
                "family.researcher",
                &["/producers/gen1", "/producers/gen2"],
            )],
        )
        .unwrap();
        assert_eq!(
            rows.iter().map(|row| row.ref_.as_str()).collect::<Vec<_>>(),
            ["file:from-first", "file:from-second"]
        );
        drop(temp);
    }

    #[test]
    fn different_projects_and_generations_are_independent_producers() {
        let mut project_a = row(
            "proj-a",
            "/producers/a",
            "markdown",
            Some("2026-07-01T00:00:00Z"),
        );
        project_a["artifact"]["project"] = json!("alpha");
        let mut project_b = row(
            "proj-b",
            "/producers/b",
            "markdown",
            Some("2026-07-01T00:00:00Z"),
        );
        project_b["artifact"]["project"] = json!("beta");
        let (temp, index) = write_index(&[project_a, project_b]);
        let rows = query_artifact_context(
            Path::new(&index),
            &[group("a", &["/producers/a"]), group("b", &["/producers/b"])],
        )
        .unwrap();
        assert_eq!(
            rows.iter().map(|row| row.ref_.as_str()).collect::<Vec<_>>(),
            ["file:proj-a", "file:proj-b"]
        );
        drop(temp);
    }

    #[test]
    fn tolerates_malformed_index_lines() {
        // `write_index` always appends a trailing malformed line; a
        // successful parse here is the assertion.
        let (temp, index) = write_index(&[row(
            "ok",
            "/producers/a",
            "markdown",
            Some("2026-07-01T00:00:00Z"),
        )]);
        let rows = query_artifact_context(
            Path::new(&index),
            &[group("a", &["/producers/a"])],
        )
        .unwrap();
        assert_eq!(
            rows.iter().map(|row| row.ref_.as_str()).collect::<Vec<_>>(),
            ["file:ok"]
        );
        drop(temp);
    }

    #[test]
    fn preserves_explicit_flag() {
        let mut explicit_row = row(
            "explicit-one",
            "/producers/a",
            "markdown",
            Some("2026-07-01T00:00:00Z"),
        );
        explicit_row["artifact"]["explicit"] = json!(true);
        let (temp, index) = write_index(&[explicit_row]);
        let rows = query_artifact_context(
            Path::new(&index),
            &[group("a", &["/producers/a"])],
        )
        .unwrap();
        assert!(rows[0].explicit);
        drop(temp);
    }

    #[test]
    fn vcs_backed_rows_have_no_stored_path() {
        let vcs_row = json!({
            "schema_version": 2,
            "artifact": {
                "id": "vcs",
                "label": "Versioned report",
                "kind": "markdown",
                "path": null,
                "vcs_repo": "sase--research",
                "vcs_sha": "0123456789abcdef0123456789abcdef01234567",
                "vcs_relpath": "202609/topic__a.md",
                "created_at": "2026-07-01T00:00:00Z",
                "project": "sase",
                "agent_artifacts_dir": "/producers/a",
                "agent_name": "producer.one",
                "explicit": true
            }
        });
        let (temp, index) = write_index(&[vcs_row]);
        let rows = query_artifact_context(
            Path::new(&index),
            &[group("a", &["/producers/a"])],
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].path, None);
        assert_eq!(rows[0].vcs_repo.as_deref(), Some("sase--research"));
        assert_eq!(
            rows[0].vcs_sha.as_deref(),
            Some("0123456789abcdef0123456789abcdef01234567")
        );
        assert_eq!(rows[0].vcs_relpath.as_deref(), Some("202609/topic__a.md"));
        drop(temp);
    }
}
