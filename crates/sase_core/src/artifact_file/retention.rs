//! Deterministic artifact-file retention planning.

use std::collections::{BTreeMap, HashSet};
use std::path::Path;

use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};

use crate::plan::search::parse_since_date_bound;

use super::{
    artifact_file_is_vcs_backed, parse_artifact_date, parse_artifact_datetime,
    read_artifact_file_index, ArtifactFileQueryError, ArtifactFileWire,
    ARTIFACT_FILE_LIFECYCLE_WIRE_SCHEMA_VERSION,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactFileRetentionPolicyWire {
    pub schema_version: u64,
    pub now: String,
    #[serde(default)]
    pub keep_per_label: u64,
    #[serde(default)]
    pub before: Option<String>,
    #[serde(default)]
    pub kinds: Option<Vec<String>>,
    #[serde(default)]
    pub project: Option<String>,
    #[serde(default)]
    pub min_size_bytes: Option<u64>,
    #[serde(default)]
    pub protected_ids: Vec<String>,
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactFileRetentionItemWire {
    pub id: String,
    pub label: Option<String>,
    pub kind: Option<String>,
    pub project: Option<String>,
    pub agent_name: Option<String>,
    pub path: Option<String>,
    pub size_bytes: Option<u64>,
    pub created_at: Option<String>,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactFileProtectedItemWire {
    pub id: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactFileRetentionCountsWire {
    pub candidates: u64,
    pub selected: u64,
    pub protected: u64,
    pub byte_backed_selected: u64,
    pub byte_free_selected: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactFileRetentionPlanWire {
    pub schema_version: u64,
    pub selected: Vec<ArtifactFileRetentionItemWire>,
    pub protected: Vec<ArtifactFileProtectedItemWire>,
    pub counts: ArtifactFileRetentionCountsWire,
    pub reclaimable_bytes: u64,
    pub truncated: u64,
    pub summary_lines: Vec<String>,
}

pub fn plan_artifact_file_retention(
    index_path: &Path,
    policy: &ArtifactFileRetentionPolicyWire,
) -> Result<ArtifactFileRetentionPlanWire, ArtifactFileQueryError> {
    require_schema(policy.schema_version)?;
    let now = parse_now(&policy.now)?;
    let before = policy
        .before
        .as_deref()
        .map(|raw| {
            parse_since_date_bound(raw, now).map_err(|error| {
                ArtifactFileQueryError::InvalidDate(error.to_string())
            })
        })
        .transpose()?;
    let protected_ids = policy
        .protected_ids
        .iter()
        .map(String::as_str)
        .collect::<HashSet<_>>();
    let all_rows = read_artifact_file_index(index_path)?;
    let mut protected = Vec::new();
    let mut eligible = Vec::new();
    for row in &all_rows {
        let reason = if row.explicit {
            Some("explicit")
        } else if protected_ids.contains(row.id.as_str()) {
            Some("referenced")
        } else {
            None
        };
        if let Some(reason) = reason {
            protected.push(ArtifactFileProtectedItemWire {
                id: row.id.clone(),
                reason: reason.to_string(),
            });
        } else {
            eligible.push(row);
        }
    }
    protected.sort_by(|left, right| {
        left.id
            .cmp(&right.id)
            .then_with(|| left.reason.cmp(&right.reason))
    });
    let candidates = eligible.len() as u64;

    let any_predicate = policy.keep_per_label > 0
        || policy.before.is_some()
        || policy.kinds.as_ref().is_some_and(|kinds| !kinds.is_empty())
        || policy.project.is_some()
        || policy.min_size_bytes.is_some();
    let effective_keep = any_predicate.then(|| policy.keep_per_label.max(1));
    let mut generation_selected = Vec::new();
    if let Some(keep) = effective_keep {
        let mut groups = BTreeMap::<
            (Option<&str>, Option<&str>),
            Vec<&ArtifactFileWire>,
        >::new();
        for row in eligible {
            groups
                .entry((row.project.as_deref(), row.label.as_deref()))
                .or_default()
                .push(row);
        }
        for group in groups.values_mut() {
            group.sort_by(|left, right| {
                row_recency_key(right).cmp(&row_recency_key(left))
            });
            generation_selected
                .extend(group.iter().skip(keep as usize).copied());
        }
    }

    let kinds = policy.kinds.as_ref().filter(|values| !values.is_empty());
    let mut selected = generation_selected
        .into_iter()
        .filter(|row| {
            before.map_or(true, |cutoff| {
                row.created_at
                    .as_deref()
                    .and_then(parse_artifact_date)
                    .is_some_and(|created| created < cutoff)
            })
        })
        .filter(|row| {
            kinds.map_or(true, |values| {
                row.kind.as_ref().is_some_and(|kind| values.contains(kind))
            })
        })
        .filter(|row| {
            policy
                .project
                .as_ref()
                .map_or(true, |project| row.project.as_ref() == Some(project))
        })
        .filter(|row| {
            policy.min_size_bytes.map_or(true, |minimum| {
                !artifact_file_is_vcs_backed(row)
                    && row.size_bytes.is_some_and(|size| size >= minimum)
            })
        })
        .collect::<Vec<_>>();
    selected.sort_by(|left, right| {
        row_recency_key(left).cmp(&row_recency_key(right))
    });

    let selected_before_limit = selected.len();
    if let Some(limit) = policy.limit {
        selected.truncate(limit);
    }
    let truncated = selected_before_limit.saturating_sub(selected.len()) as u64;
    let selected = selected
        .into_iter()
        .map(|row| retention_item(row, policy, effective_keep))
        .collect::<Vec<_>>();
    let byte_free_selected =
        selected.iter().filter(|item| item.path.is_none()).count() as u64;
    let byte_backed_selected = selected.len() as u64 - byte_free_selected;
    let reclaimable_bytes = selected
        .iter()
        .filter(|item| item.path.is_some())
        .filter_map(|item| item.size_bytes)
        .sum();
    let counts = ArtifactFileRetentionCountsWire {
        candidates,
        selected: selected.len() as u64,
        protected: protected.len() as u64,
        byte_backed_selected,
        byte_free_selected,
    };
    let summary_lines = vec![
        format!(
            "{} of {} eligible artifact rows selected",
            counts.selected, counts.candidates
        ),
        format!(
            "{} byte-backed and {} byte-free rows selected",
            counts.byte_backed_selected, counts.byte_free_selected
        ),
        format!("{reclaimable_bytes} recorded bytes reclaimable"),
        format!(
            "{} rows protected; {truncated} matching rows truncated",
            counts.protected
        ),
    ];

    Ok(ArtifactFileRetentionPlanWire {
        schema_version: ARTIFACT_FILE_LIFECYCLE_WIRE_SCHEMA_VERSION,
        selected,
        protected,
        counts,
        reclaimable_bytes,
        truncated,
        summary_lines,
    })
}

fn require_schema(schema_version: u64) -> Result<(), ArtifactFileQueryError> {
    if schema_version != ARTIFACT_FILE_LIFECYCLE_WIRE_SCHEMA_VERSION {
        return Err(ArtifactFileQueryError::InvalidWire(format!(
            "artifact-file lifecycle wire schema mismatch for retention \
             policy: got {schema_version}, expected \
             {ARTIFACT_FILE_LIFECYCLE_WIRE_SCHEMA_VERSION}"
        )));
    }
    Ok(())
}

fn parse_now(raw: &str) -> Result<NaiveDate, ArtifactFileQueryError> {
    DateTime::parse_from_rfc3339(raw)
        .map(|value| value.with_timezone(&Utc).date_naive())
        .map_err(|_| {
            ArtifactFileQueryError::InvalidDate(format!(
                "invalid RFC3339 retention now: {raw}"
            ))
        })
}

fn row_recency_key(row: &ArtifactFileWire) -> (i64, &str) {
    (
        row.created_at
            .as_deref()
            .and_then(parse_artifact_datetime)
            .map(|value| value.and_utc().timestamp_micros())
            .unwrap_or(i64::MIN),
        row.id.as_str(),
    )
}

fn retention_item(
    row: &ArtifactFileWire,
    policy: &ArtifactFileRetentionPolicyWire,
    effective_keep: Option<u64>,
) -> ArtifactFileRetentionItemWire {
    let mut reasons = Vec::new();
    if let Some(keep) = effective_keep {
        reasons.push(format!("older than newest {keep} generation(s)"));
    }
    if let Some(before) = &policy.before {
        reasons.push(format!("created on or before {before}"));
    }
    if let Some(kinds) = policy.kinds.as_ref().filter(|kinds| !kinds.is_empty())
    {
        reasons.push(format!("kind in {}", kinds.join(",")));
    }
    if let Some(project) = &policy.project {
        reasons.push(format!("project {project}"));
    }
    if let Some(minimum) = policy.min_size_bytes {
        reasons.push(format!("at least {minimum} bytes"));
    }
    ArtifactFileRetentionItemWire {
        id: row.id.clone(),
        label: row.label.clone(),
        kind: row.kind.clone(),
        project: row.project.clone(),
        agent_name: row.agent_name.clone(),
        path: row.path.clone(),
        size_bytes: row.size_bytes,
        created_at: row.created_at.clone(),
        reason: reasons.join("; "),
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use serde_json::json;
    use tempfile::tempdir;

    use super::*;

    fn write_index() -> (tempfile::TempDir, std::path::PathBuf) {
        let temp = tempdir().unwrap();
        let index = temp.path().join("index.jsonl");
        let rows = [
            json!({"schema_version":2,"artifact":{
                "id":"new","label":"x","kind":"image","path":"/new",
                "project":"p","created_at":"2026-07-30T00:00:00Z",
                "size_bytes":5
            }}),
            json!({"schema_version":2,"artifact":{
                "id":"middle","label":"x","kind":"image","path":"/middle",
                "project":"p","created_at":"2026-07-20T00:00:00Z",
                "size_bytes":15
            }}),
            json!({"schema_version":2,"artifact":{
                "id":"old","label":"x","kind":"markdown","path":"/old",
                "project":"p","created_at":"2026-06-01T00:00:00Z",
                "size_bytes":25
            }}),
            json!({"schema_version":2,"artifact":{
                "id":"free","label":"y","kind":"image","path":null,
                "vcs_repo":"p","vcs_sha":"abc","vcs_relpath":"x",
                "project":"p","created_at":"2026-06-01T00:00:00Z",
                "size_bytes":100
            }}),
            json!({"schema_version":2,"artifact":{
                "id":"free-new","label":"y","kind":"image","path":null,
                "vcs_repo":"p","vcs_sha":"def","vcs_relpath":"x",
                "project":"p","created_at":"2026-07-30T00:00:00Z",
                "size_bytes":100
            }}),
            json!({"schema_version":2,"artifact":{
                "id":"explicit","label":"z","kind":"image","path":"/explicit",
                "project":"p","created_at":"2026-06-01T00:00:00Z",
                "explicit":true,"size_bytes":50
            }}),
            json!({"schema_version":2,"artifact":{
                "id":"referenced","label":"r","kind":"image","path":"/referenced",
                "project":"p","created_at":"2026-06-01T00:00:00Z",
                "size_bytes":50
            }}),
        ];
        fs::write(
            &index,
            rows.iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join("\n"),
        )
        .unwrap();
        (temp, index)
    }

    fn policy() -> ArtifactFileRetentionPolicyWire {
        ArtifactFileRetentionPolicyWire {
            schema_version: ARTIFACT_FILE_LIFECYCLE_WIRE_SCHEMA_VERSION,
            now: "2026-07-30T12:00:00Z".to_string(),
            keep_per_label: 1,
            before: None,
            kinds: None,
            project: None,
            min_size_bytes: None,
            protected_ids: vec!["referenced".to_string()],
            limit: None,
        }
    }

    #[test]
    fn protects_explicit_and_referenced_and_separates_byte_free_rows() {
        let (_temp, index) = write_index();
        let plan = plan_artifact_file_retention(&index, &policy()).unwrap();
        assert_eq!(
            plan.selected
                .iter()
                .map(|item| item.id.as_str())
                .collect::<Vec<_>>(),
            vec!["free", "old", "middle"]
        );
        assert_eq!(plan.counts.candidates, 5);
        assert_eq!(plan.counts.selected, 3);
        assert_eq!(plan.counts.protected, 2);
        assert_eq!(plan.counts.byte_backed_selected, 2);
        assert_eq!(plan.counts.byte_free_selected, 1);
        assert_eq!(plan.reclaimable_bytes, 40);
        assert_eq!(
            plan.protected,
            vec![
                ArtifactFileProtectedItemWire {
                    id: "explicit".to_string(),
                    reason: "explicit".to_string(),
                },
                ArtifactFileProtectedItemWire {
                    id: "referenced".to_string(),
                    reason: "referenced".to_string(),
                },
            ]
        );
    }

    #[test]
    fn predicates_compose_clamp_generation_floor_and_limit_deterministically() {
        let (_temp, index) = write_index();
        let mut policy = policy();
        policy.keep_per_label = 0;
        policy.before = Some("7d".to_string());
        policy.kinds = Some(vec!["image".to_string()]);
        policy.project = Some("p".to_string());
        policy.min_size_bytes = Some(10);
        policy.limit = Some(1);
        let first = plan_artifact_file_retention(&index, &policy).unwrap();
        let second = plan_artifact_file_retention(&index, &policy).unwrap();
        assert_eq!(first, second);
        assert_eq!(
            first
                .selected
                .iter()
                .map(|item| item.id.as_str())
                .collect::<Vec<_>>(),
            vec!["middle"]
        );
        assert_eq!(first.truncated, 0);
        assert!(!first.selected.iter().any(|item| item.id == "new"));

        policy.kinds = None;
        policy.min_size_bytes = None;
        let limited = plan_artifact_file_retention(&index, &policy).unwrap();
        assert_eq!(limited.selected[0].id, "free");
        assert_eq!(limited.truncated, 2);
    }

    #[test]
    fn each_additional_predicate_filters_generation_candidates() {
        let (_temp, index) = write_index();

        let mut before = policy();
        before.keep_per_label = 0;
        before.before = Some("7d".to_string());
        assert_eq!(
            selected_ids(
                &plan_artifact_file_retention(&index, &before).unwrap()
            ),
            vec!["free", "old", "middle"]
        );

        let mut kinds = policy();
        kinds.keep_per_label = 0;
        kinds.kinds = Some(vec!["markdown".to_string()]);
        assert_eq!(
            selected_ids(
                &plan_artifact_file_retention(&index, &kinds).unwrap()
            ),
            vec!["old"]
        );

        let mut project = policy();
        project.keep_per_label = 0;
        project.project = Some("p".to_string());
        assert_eq!(
            selected_ids(
                &plan_artifact_file_retention(&index, &project).unwrap()
            ),
            vec!["free", "old", "middle"]
        );

        let mut minimum = policy();
        minimum.keep_per_label = 0;
        minimum.min_size_bytes = Some(20);
        assert_eq!(
            selected_ids(
                &plan_artifact_file_retention(&index, &minimum).unwrap()
            ),
            vec!["old"]
        );
    }

    #[test]
    fn zero_keep_without_other_predicates_disables_selection() {
        let (_temp, index) = write_index();
        let mut policy = policy();
        policy.keep_per_label = 0;
        let plan = plan_artifact_file_retention(&index, &policy).unwrap();
        assert!(plan.selected.is_empty());
        assert_eq!(plan.truncated, 0);
    }

    #[test]
    fn invalid_now_and_schema_are_rejected() {
        let (_temp, index) = write_index();
        let mut invalid = policy();
        invalid.now = "today".to_string();
        assert!(plan_artifact_file_retention(&index, &invalid)
            .unwrap_err()
            .to_string()
            .contains("RFC3339"));
        invalid = policy();
        invalid.schema_version = 2;
        assert!(plan_artifact_file_retention(&index, &invalid)
            .unwrap_err()
            .to_string()
            .contains("schema mismatch"));
    }

    fn selected_ids(plan: &ArtifactFileRetentionPlanWire) -> Vec<&str> {
        plan.selected.iter().map(|item| item.id.as_str()).collect()
    }
}
