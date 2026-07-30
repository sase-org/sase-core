//! Tolerant artifact-file index reading and cross-frontend query semantics.

mod economics;
mod retention;
mod trash;
mod vcs;

use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::artifact_consumption::{
    consumed_artifact_file_refs, read_artifact_consumption_log,
};
use crate::plan::search::{current_date, parse_since_date_bound};

pub use economics::{
    artifact_file_store_economics, ArtifactFileEconomicsGroupWire,
    ArtifactFileEconomicsOptionsWire, ArtifactFileEconomicsWire,
    ArtifactFileGenerationProjectionWire,
};
pub use retention::{
    plan_artifact_file_retention, ArtifactFileProtectedItemWire,
    ArtifactFileRetentionCountsWire, ArtifactFileRetentionItemWire,
    ArtifactFileRetentionPlanWire, ArtifactFileRetentionPolicyWire,
};
pub use trash::{
    list_artifact_file_trash, purge_artifact_file_trash,
    restore_artifact_file_trash, trash_artifact_file,
    ArtifactFileTrashEntryWire, ArtifactFileTrashListWire,
    ArtifactFileTrashPurgeRequestWire, ArtifactFileTrashPurgeWire,
    ArtifactFileTrashRequestWire, ArtifactFileTrashRestoreRequestWire,
    ArtifactFileTrashRestoreWire,
};
pub use vcs::{
    materialize_vcs_artifact_file, ArtifactFileVcsMaterializationRequestWire,
    ArtifactFileVcsMaterializationWire,
};

pub const ARTIFACT_FILE_INDEX_MIN_SCHEMA_VERSION: u64 = 1;
pub const ARTIFACT_FILE_INDEX_MAX_SCHEMA_VERSION: u64 = 2;
pub const ARTIFACT_FILE_QUERY_WIRE_SCHEMA_VERSION: u64 = 3;
pub const ARTIFACT_FILE_LIFECYCLE_WIRE_SCHEMA_VERSION: u64 = 1;

#[derive(Debug, Error)]
pub enum ArtifactFileQueryError {
    #[error("artifact-file index I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("{0}")]
    InvalidDate(String),
    #[error("{0}")]
    InvalidWire(String),
}

/// One complete artifact-file row, annotated with its envelope version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactFileWire {
    #[serde(default)]
    pub schema_version: u64,
    pub id: String,
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub path: Option<String>,
    #[serde(default)]
    pub vcs_repo: Option<String>,
    #[serde(default)]
    pub vcs_sha: Option<String>,
    #[serde(default)]
    pub vcs_relpath: Option<String>,
    #[serde(default)]
    pub source_path: Option<String>,
    #[serde(default)]
    pub workspace_dir: Option<String>,
    #[serde(default)]
    pub created_at: Option<String>,
    #[serde(default)]
    pub agent_artifacts_dir: Option<String>,
    #[serde(default)]
    pub project: Option<String>,
    #[serde(default)]
    pub workflow: Option<String>,
    #[serde(default)]
    pub raw_timestamp: Option<String>,
    #[serde(default)]
    pub agent_name: Option<String>,
    #[serde(default)]
    pub explicit: bool,
    #[serde(default)]
    pub sha256: Option<String>,
    #[serde(default)]
    pub size_bytes: Option<u64>,
    #[serde(default)]
    pub mime_type: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactFileQueryFiltersWire {
    #[serde(default)]
    pub kinds: Option<Vec<String>>,
    #[serde(default)]
    pub project: Option<String>,
    #[serde(default)]
    pub agent: Option<String>,
    #[serde(default)]
    pub since: Option<String>,
    #[serde(default)]
    pub explicit_only: bool,
    #[serde(default)]
    pub query: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub unused_only: bool,
    #[serde(default)]
    pub consumption_log_path: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ArtifactIndexEnvelope {
    schema_version: u64,
    artifact: ArtifactFileWire,
}

/// Read all valid rows in supported envelopes.
///
/// Empty and malformed lines, unsupported envelope versions, and rows missing
/// required `id`/`path` fields are skipped. Unknown JSON fields are ignored by
/// serde.
pub fn read_artifact_file_index(
    index_path: &Path,
) -> Result<Vec<ArtifactFileWire>, std::io::Error> {
    let content = match fs::read_to_string(index_path) {
        Ok(content) => content,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(Vec::new());
        }
        Err(error) => return Err(error),
    };
    Ok(parse_artifact_file_index(&content))
}

fn parse_artifact_file_index(content: &str) -> Vec<ArtifactFileWire> {
    content
        .lines()
        .filter_map(|line| {
            let line = line.trim();
            if line.is_empty() {
                return None;
            }
            let envelope =
                serde_json::from_str::<ArtifactIndexEnvelope>(line).ok()?;
            if !artifact_file_index_schema_supported(envelope.schema_version) {
                return None;
            }
            let mut artifact = envelope.artifact;
            artifact.schema_version = envelope.schema_version;
            artifact_file_row_is_admissible(&artifact).then_some(artifact)
        })
        .collect()
}

/// Return whether an artifact-file row is backed by version-control content.
///
/// VCS-backed rows deliberately omit a stored path and carry the complete
/// repository, commit, and relative-path provenance triple instead.
pub fn artifact_file_is_vcs_backed(row: &ArtifactFileWire) -> bool {
    row.path.is_none() && artifact_file_vcs_provenance_is_complete(row)
}

fn artifact_file_row_is_admissible(row: &ArtifactFileWire) -> bool {
    nonempty(Some(row.id.as_str()))
        && (row.path.as_deref().is_some_and(|path| nonempty(Some(path)))
            || artifact_file_is_vcs_backed(row))
}

fn artifact_file_vcs_provenance_is_complete(row: &ArtifactFileWire) -> bool {
    [
        row.vcs_repo.as_deref(),
        row.vcs_sha.as_deref(),
        row.vcs_relpath.as_deref(),
    ]
    .into_iter()
    .all(nonempty)
}

fn nonempty(value: Option<&str>) -> bool {
    value.is_some_and(|value| !value.trim().is_empty())
}

pub fn query_artifact_files(
    index_path: &Path,
    filters: &ArtifactFileQueryFiltersWire,
) -> Result<Vec<ArtifactFileWire>, ArtifactFileQueryError> {
    query_artifact_files_at(index_path, filters, current_date())
}

fn query_artifact_files_at(
    index_path: &Path,
    filters: &ArtifactFileQueryFiltersWire,
    now: NaiveDate,
) -> Result<Vec<ArtifactFileWire>, ArtifactFileQueryError> {
    let since = filters
        .since
        .as_deref()
        .map(|raw| {
            parse_since_date_bound(raw, now).map_err(|error| {
                ArtifactFileQueryError::InvalidDate(error.to_string())
            })
        })
        .transpose()?;
    let needle = filters
        .query
        .as_deref()
        .map(str::trim)
        .filter(|query| !query.is_empty())
        .map(str::to_lowercase);
    let kinds = filters.kinds.as_deref().filter(|kinds| !kinds.is_empty());
    let consumed_refs = filters.unused_only.then(|| {
        filters
            .consumption_log_path
            .as_deref()
            .map(Path::new)
            .map(read_artifact_consumption_log)
            .transpose()
            .unwrap_or_default()
            .map_or_else(BTreeSet::new, |events| {
                consumed_artifact_file_refs(&events)
            })
    });

    let mut rows =
        read_artifact_file_index(index_path)?
            .into_iter()
            .filter(|row| {
                kinds.map_or(true, |values| {
                    row.kind.as_ref().is_some_and(|kind| values.contains(kind))
                })
            })
            .filter(|row| {
                filters.project.as_ref().map_or(true, |project| {
                    row.project.as_ref() == Some(project)
                })
            })
            .filter(|row| {
                filters.agent.as_ref().map_or(true, |agent| {
                    row.agent_name.as_ref() == Some(agent)
                })
            })
            .filter(|row| !filters.explicit_only || row.explicit)
            .filter(|row| {
                since.map_or(true, |bound| {
                    row.created_at
                        .as_deref()
                        .and_then(parse_artifact_date)
                        .is_some_and(|date| date >= bound)
                })
            })
            .filter(|row| {
                needle.as_ref().map_or(true, |query| {
                    [
                        row.label.as_deref(),
                        row.path.as_deref(),
                        row.source_path.as_deref(),
                        row.vcs_relpath.as_deref(),
                    ]
                    .into_iter()
                    .flatten()
                    .any(|value| value.to_lowercase().contains(query))
                })
            })
            .filter(|row| match &consumed_refs {
                Some(consumed_refs) => {
                    !consumed_refs.contains(&format!("file:{}", row.id))
                }
                None => true,
            })
            .collect::<Vec<_>>();

    rows.sort_by(|left, right| {
        match (
            left.created_at.as_deref().and_then(parse_artifact_time),
            right.created_at.as_deref().and_then(parse_artifact_time),
        ) {
            (Some(left_time), Some(right_time)) => right_time.cmp(&left_time),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => std::cmp::Ordering::Equal,
        }
        .then_with(|| left.id.cmp(&right.id))
        .then_with(|| left.path.is_none().cmp(&right.path.is_none()))
        .then_with(|| left.path.cmp(&right.path))
    });
    if let Some(limit) = filters.limit {
        if limit > 0 && rows.len() > limit {
            rows.truncate(limit);
        }
    }
    Ok(rows)
}

fn artifact_file_index_schema_supported(version: u64) -> bool {
    (ARTIFACT_FILE_INDEX_MIN_SCHEMA_VERSION
        ..=ARTIFACT_FILE_INDEX_MAX_SCHEMA_VERSION)
        .contains(&version)
}

fn parse_artifact_date(value: &str) -> Option<NaiveDate> {
    parse_artifact_datetime(value).map(|datetime| datetime.date())
}

fn parse_artifact_time(value: &str) -> Option<i64> {
    parse_artifact_datetime(value)
        .map(|datetime| datetime.and_utc().timestamp_micros())
}

fn parse_artifact_datetime(value: &str) -> Option<NaiveDateTime> {
    let value = value.trim();
    if value.is_empty() {
        return None;
    }
    DateTime::parse_from_rfc3339(value)
        .map(|datetime| datetime.with_timezone(&Utc).naive_utc())
        .ok()
        .or_else(|| {
            NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f").ok()
        })
        .or_else(|| {
            NaiveDate::parse_from_str(value, "%Y-%m-%d")
                .ok()
                .and_then(|date| date.and_hms_opt(0, 0, 0))
        })
}

#[cfg(test)]
mod tests {
    use std::fs;

    use chrono::NaiveDate;
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
        version: u64,
        id: &str,
        created_at: Option<&str>,
    ) -> serde_json::Value {
        json!({
            "schema_version": version,
            "artifact": {
                "id": id,
                "label": format!("Label {id}"),
                "kind": "image",
                "path": format!("/stored/{id}.png"),
                "source_path": format!("/source/{id}.png"),
                "created_at": created_at,
                "project": "sase",
                "agent_name": "agent.one",
                "explicit": true,
                "future_field": "ignored"
            },
            "future_envelope_field": true
        })
    }

    #[test]
    fn parser_accepts_supported_range_and_skips_bad_rows() {
        let (temp, index) = write_index(&[
            row(1, "v1", Some("2026-07-01T00:00:00+00:00")),
            row(2, "v2", Some("2026-07-02T00:00:00+00:00")),
            row(3, "v3", Some("2026-07-03T00:00:00+00:00")),
            json!({"schema_version": 1, "artifact": {"id": "no-path"}}),
            json!({
                "schema_version": 2,
                "artifact": {
                    "id": "vcs",
                    "path": null,
                    "vcs_repo": "sase",
                    "vcs_sha": "0123456789abcdef0123456789abcdef01234567",
                    "vcs_relpath": "docs/image.png"
                }
            }),
            json!({
                "schema_version": 2,
                "artifact": {
                    "id": "partial",
                    "path": null,
                    "vcs_repo": "sase",
                    "vcs_sha": "0123456789abcdef0123456789abcdef01234567"
                }
            }),
            json!({
                "schema_version": 2,
                "artifact": {
                    "id": "",
                    "path": "/stored/empty-id.png"
                }
            }),
        ]);
        let rows = read_artifact_file_index(Path::new(&index)).unwrap();
        assert_eq!(
            rows.iter().map(|row| row.id.as_str()).collect::<Vec<_>>(),
            ["v1", "v2", "vcs"]
        );
        assert_eq!(rows[0].schema_version, 1);
        assert_eq!(rows[1].schema_version, 2);
        assert!(!artifact_file_is_vcs_backed(&rows[0]));
        assert!(artifact_file_is_vcs_backed(&rows[2]));
        drop(temp);
    }

    #[test]
    fn query_applies_every_filter_individually_and_combined() {
        let mut first = row(1, "first", Some("2026-07-01T10:00:00+00:00"));
        first["artifact"]["kind"] = json!("markdown");
        first["artifact"]["label"] = json!("Architecture Notes");
        let mut second = row(1, "second", Some("2026-07-02T10:00:00+00:00"));
        second["artifact"]["project"] = json!("other");
        second["artifact"]["agent_name"] = json!("agent.two");
        second["artifact"]["explicit"] = json!(false);
        second["artifact"]["source_path"] = json!("/source/target.png");
        let vcs = json!({
            "schema_version": 2,
            "artifact": {
                "id": "vcs",
                "label": "Versioned",
                "kind": "image",
                "path": null,
                "vcs_repo": "sase",
                "vcs_sha": "0123456789abcdef0123456789abcdef01234567",
                "vcs_relpath": "docs/vcs-needle.png",
                "created_at": "2026-07-03T10:00:00+00:00",
                "project": "vcs-project",
                "agent_name": "agent.vcs",
                "explicit": false
            }
        });
        let (temp, index) = write_index(&[first, second, vcs]);
        let index = Path::new(&index);

        let cases = [
            (
                ArtifactFileQueryFiltersWire {
                    kinds: Some(vec!["markdown".to_string()]),
                    ..Default::default()
                },
                vec!["first"],
            ),
            (
                ArtifactFileQueryFiltersWire {
                    project: Some("other".to_string()),
                    ..Default::default()
                },
                vec!["second"],
            ),
            (
                ArtifactFileQueryFiltersWire {
                    agent: Some("agent.one".to_string()),
                    ..Default::default()
                },
                vec!["first"],
            ),
            (
                ArtifactFileQueryFiltersWire {
                    explicit_only: true,
                    ..Default::default()
                },
                vec!["first"],
            ),
            (
                ArtifactFileQueryFiltersWire {
                    query: Some("SOURCE/TARGET".to_string()),
                    ..Default::default()
                },
                vec!["second"],
            ),
            (
                ArtifactFileQueryFiltersWire {
                    query: Some("STORED/FIRST".to_string()),
                    ..Default::default()
                },
                vec!["first"],
            ),
            (
                ArtifactFileQueryFiltersWire {
                    query: Some("VCS-NEEDLE".to_string()),
                    ..Default::default()
                },
                vec!["vcs"],
            ),
            (
                ArtifactFileQueryFiltersWire {
                    query: Some("architecture".to_string()),
                    kinds: Some(vec!["markdown".to_string()]),
                    project: Some("sase".to_string()),
                    agent: Some("agent.one".to_string()),
                    since: Some("2026-07".to_string()),
                    explicit_only: true,
                    limit: Some(1),
                    ..Default::default()
                },
                vec!["first"],
            ),
        ];
        for (filters, expected) in cases {
            let rows = query_artifact_files(index, &filters).unwrap();
            assert_eq!(
                rows.iter().map(|row| row.id.as_str()).collect::<Vec<_>>(),
                expected
            );
        }
        drop(temp);
    }

    #[test]
    fn since_uses_plan_search_forms_and_excludes_missing_dates() {
        let (temp, index) = write_index(&[
            row(1, "old", Some("2026-05-01T00:00:00Z")),
            row(1, "recent", Some("2026-06-10T00:00:00Z")),
            row(1, "missing", None),
            row(1, "invalid", Some("not-a-timestamp")),
        ]);
        let now = NaiveDate::from_ymd_opt(2026, 6, 18).unwrap();
        for since in ["2026-06-01", "2026-06", "202606", "14d", "2w", "1m"] {
            let filters = ArtifactFileQueryFiltersWire {
                since: Some(since.to_string()),
                ..Default::default()
            };
            let rows =
                query_artifact_files_at(Path::new(&index), &filters, now)
                    .unwrap();
            assert_eq!(
                rows.iter().map(|row| row.id.as_str()).collect::<Vec<_>>(),
                ["recent"],
                "since={since}"
            );
            assert!(parse_since_date_bound(since, now).is_ok());
        }
        drop(temp);
    }

    #[test]
    fn query_sorts_newest_first_missing_last_and_applies_limit() {
        let (temp, index) = write_index(&[
            row(1, "old", Some("2026-07-01T00:00:00-04:00")),
            row(1, "new", Some("2026-07-01T05:00:00Z")),
            row(1, "missing", None),
        ]);
        let rows = query_artifact_files(
            Path::new(&index),
            &ArtifactFileQueryFiltersWire {
                limit: Some(2),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(
            rows.iter().map(|row| row.id.as_str()).collect::<Vec<_>>(),
            ["new", "old"]
        );
        drop(temp);
    }

    #[test]
    fn unused_filter_runs_before_limit_and_tolerates_missing_ledgers() {
        let (temp, index) = write_index(&[
            row(1, "oldest", Some("2026-07-01T00:00:00Z")),
            row(1, "older", Some("2026-07-02T00:00:00Z")),
            row(1, "used", Some("2026-07-03T00:00:00Z")),
            row(1, "newest-used", Some("2026-07-04T00:00:00Z")),
        ]);
        let consumption_log = temp.path().join("consumption.jsonl");
        fs::write(
            &consumption_log,
            concat!(
                "{\"schema_version\":1,\"consumption\":{",
                "\"id\":\"one\",\"timestamp\":\"2026-07-30T10:00:00Z\",",
                "\"ref\":\"file:used\",\"ref_kind\":\"file\",",
                "\"fragment\":null,\"role\":\"image\",",
                "\"artifact_id\":\"used\",\"resolved_path\":\"/stored/used.png\",",
                "\"resolution_status\":\"exact\",\"agent_name\":\"agent.one\",",
                "\"agent_source\":\"SASE_AGENT_NAME\",",
                "\"artifacts_dir\":null,\"project\":\"sase\"}}\n",
                "{\"schema_version\":1,\"consumption\":{",
                "\"id\":\"two\",\"timestamp\":\"2026-07-30T10:01:00Z\",",
                "\"ref\":\"file:newest-used\",\"ref_kind\":\"file\",",
                "\"fragment\":null,\"role\":\"image\",",
                "\"artifact_id\":\"newest-used\",",
                "\"resolved_path\":\"/stored/newest-used.png\",",
                "\"resolution_status\":\"exact\",\"agent_name\":\"agent.one\",",
                "\"agent_source\":\"SASE_AGENT_NAME\",",
                "\"artifacts_dir\":null,\"project\":\"sase\"}}\n"
            ),
        )
        .unwrap();

        let rows = query_artifact_files(
            Path::new(&index),
            &ArtifactFileQueryFiltersWire {
                unused_only: true,
                consumption_log_path: Some(
                    consumption_log.to_string_lossy().into_owned(),
                ),
                limit: Some(2),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(
            rows.iter().map(|row| row.id.as_str()).collect::<Vec<_>>(),
            ["older", "oldest"]
        );

        for consumption_log_path in [
            None,
            Some(
                temp.path()
                    .join("missing-consumption.jsonl")
                    .to_string_lossy()
                    .into_owned(),
            ),
            Some(temp.path().to_string_lossy().into_owned()),
        ] {
            let rows = query_artifact_files(
                Path::new(&index),
                &ArtifactFileQueryFiltersWire {
                    unused_only: true,
                    consumption_log_path,
                    ..Default::default()
                },
            )
            .unwrap();
            assert_eq!(rows.len(), 4);
        }
        drop(temp);
    }

    #[test]
    fn absent_index_is_empty_and_invalid_since_is_an_error() {
        let temp = tempdir().unwrap();
        assert!(query_artifact_files(
            &temp.path().join("missing.jsonl"),
            &ArtifactFileQueryFiltersWire::default()
        )
        .unwrap()
        .is_empty());
        let error = query_artifact_files(
            &temp.path().join("missing.jsonl"),
            &ArtifactFileQueryFiltersWire {
                since: Some("yesterday".to_string()),
                ..Default::default()
            },
        )
        .unwrap_err();
        assert!(error.to_string().contains("invalid date: yesterday"));
    }
}
