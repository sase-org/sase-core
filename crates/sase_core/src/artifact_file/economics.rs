//! Pure aggregation of artifact-file store economics.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;

use serde::{Deserialize, Serialize};

use super::{
    artifact_file_is_vcs_backed, parse_artifact_datetime,
    read_artifact_file_index, ArtifactFileQueryError, ArtifactFileWire,
    ARTIFACT_FILE_LIFECYCLE_WIRE_SCHEMA_VERSION,
};

fn default_top_n() -> usize {
    10
}

fn default_generation_projections() -> Vec<u64> {
    vec![1, 3, 5]
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactFileEconomicsOptionsWire {
    pub schema_version: u64,
    #[serde(default)]
    pub project: Option<String>,
    #[serde(default = "default_top_n")]
    pub top_n: usize,
    #[serde(default = "default_generation_projections")]
    pub generation_projections: Vec<u64>,
}

impl Default for ArtifactFileEconomicsOptionsWire {
    fn default() -> Self {
        Self {
            schema_version: ARTIFACT_FILE_LIFECYCLE_WIRE_SCHEMA_VERSION,
            project: None,
            top_n: default_top_n(),
            generation_projections: default_generation_projections(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactFileEconomicsGroupWire {
    pub key: String,
    pub rows: u64,
    pub bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactFileGenerationProjectionWire {
    pub keep_per_label: u64,
    pub rows_freed: u64,
    pub bytes_freed: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ArtifactFileEconomicsWire {
    pub schema_version: u64,
    pub total_rows: u64,
    pub explicit_rows: u64,
    pub automatic_rows: u64,
    pub vcs_backed_rows: u64,
    pub rows_missing_size: u64,
    pub total_bytes: u64,
    pub explicit_bytes: u64,
    pub automatic_bytes: u64,
    pub vcs_backed_bytes: u64,
    pub by_kind: Vec<ArtifactFileEconomicsGroupWire>,
    pub by_project: Vec<ArtifactFileEconomicsGroupWire>,
    pub by_agent: Vec<ArtifactFileEconomicsGroupWire>,
    pub by_agent_truncated_groups: u64,
    pub by_agent_truncated_bytes: u64,
    pub first_created_at: Option<String>,
    pub last_created_at: Option<String>,
    pub window_days: u64,
    pub bytes_per_day: f64,
    pub rows_per_day: f64,
    pub duplicate_digest_groups: u64,
    pub redundant_digest_rows: u64,
    pub redundant_digest_bytes: u64,
    pub distinct_labels: u64,
    pub label_generation_projections: Vec<ArtifactFileGenerationProjectionWire>,
    pub source_inside_workspace_rows: u64,
    pub source_inside_workspace_bytes: u64,
}

pub fn artifact_file_store_economics(
    index_path: &Path,
    options: &ArtifactFileEconomicsOptionsWire,
) -> Result<ArtifactFileEconomicsWire, ArtifactFileQueryError> {
    require_schema(options.schema_version, "economics options")?;
    let rows = read_artifact_file_index(index_path)?
        .into_iter()
        .filter(|row| {
            options
                .project
                .as_ref()
                .map_or(true, |project| row.project.as_ref() == Some(project))
        })
        .collect::<Vec<_>>();

    let total_rows = rows.len() as u64;
    let explicit_rows = rows.iter().filter(|row| row.explicit).count() as u64;
    let automatic_rows = total_rows.saturating_sub(explicit_rows);
    let vcs_backed_rows = rows
        .iter()
        .filter(|row| artifact_file_is_vcs_backed(row))
        .count() as u64;
    let rows_missing_size =
        rows.iter().filter(|row| row.size_bytes.is_none()).count() as u64;
    let total_bytes = sum_bytes(rows.iter());
    let explicit_bytes = sum_bytes(rows.iter().filter(|row| row.explicit));
    let automatic_bytes = sum_bytes(rows.iter().filter(|row| !row.explicit));
    let vcs_backed_bytes =
        sum_bytes(rows.iter().filter(|row| artifact_file_is_vcs_backed(row)));

    let by_kind = group_rows(&rows, |row| row.kind.as_deref());
    let by_project = group_rows(&rows, |row| row.project.as_deref());
    let all_agents = group_rows(&rows, |row| row.agent_name.as_deref());
    let by_agent = all_agents
        .iter()
        .take(options.top_n)
        .cloned()
        .collect::<Vec<_>>();
    let truncated_agents = all_agents.iter().skip(options.top_n);
    let by_agent_truncated_groups = truncated_agents.clone().count() as u64;
    let by_agent_truncated_bytes =
        truncated_agents.map(|group| group.bytes).sum();

    let mut dated = rows
        .iter()
        .filter_map(|row| {
            row.created_at
                .as_deref()
                .and_then(parse_artifact_datetime)
                .map(|date| (date, row.created_at.clone().unwrap_or_default()))
        })
        .collect::<Vec<_>>();
    dated.sort_by_key(|item| item.0);
    let first_created_at = dated.first().map(|(_, raw)| raw.clone());
    let last_created_at = dated.last().map(|(_, raw)| raw.clone());
    let window_days = dated
        .first()
        .zip(dated.last())
        .map(|(first, last)| {
            (last.0.date() - first.0.date()).num_days().max(0) as u64 + 1
        })
        .unwrap_or(0);
    let divisor = window_days.max(1) as f64;
    let bytes_per_day = if window_days == 0 {
        0.0
    } else {
        total_bytes as f64 / divisor
    };
    let rows_per_day = if window_days == 0 {
        0.0
    } else {
        total_rows as f64 / divisor
    };

    let (
        duplicate_digest_groups,
        redundant_digest_rows,
        redundant_digest_bytes,
    ) = redundancy(&rows);
    let distinct_labels = rows
        .iter()
        .filter(|row| !row.explicit)
        .filter_map(|row| row.label.as_deref())
        .filter(|label| !label.is_empty())
        .collect::<HashSet<_>>()
        .len() as u64;
    let label_generation_projections = options
        .generation_projections
        .iter()
        .map(|keep| generation_projection(&rows, *keep))
        .collect();

    let reclaimable_upper_bound = rows.iter().filter(|row| {
        !row.explicit
            && !artifact_file_is_vcs_backed(row)
            && row.path.is_some()
            && row
                .source_path
                .as_deref()
                .zip(row.workspace_dir.as_deref())
                .is_some_and(|(source, workspace)| {
                    Path::new(source).starts_with(Path::new(workspace))
                })
    });
    let reclaimable_upper_bound = reclaimable_upper_bound.collect::<Vec<_>>();
    let source_inside_workspace_rows = reclaimable_upper_bound.len() as u64;
    let source_inside_workspace_bytes =
        sum_bytes(reclaimable_upper_bound.into_iter());

    Ok(ArtifactFileEconomicsWire {
        schema_version: ARTIFACT_FILE_LIFECYCLE_WIRE_SCHEMA_VERSION,
        total_rows,
        explicit_rows,
        automatic_rows,
        vcs_backed_rows,
        rows_missing_size,
        total_bytes,
        explicit_bytes,
        automatic_bytes,
        vcs_backed_bytes,
        by_kind,
        by_project,
        by_agent,
        by_agent_truncated_groups,
        by_agent_truncated_bytes,
        first_created_at,
        last_created_at,
        window_days,
        bytes_per_day,
        rows_per_day,
        duplicate_digest_groups,
        redundant_digest_rows,
        redundant_digest_bytes,
        distinct_labels,
        label_generation_projections,
        source_inside_workspace_rows,
        source_inside_workspace_bytes,
    })
}

fn require_schema(
    schema_version: u64,
    wire_name: &str,
) -> Result<(), ArtifactFileQueryError> {
    if schema_version != ARTIFACT_FILE_LIFECYCLE_WIRE_SCHEMA_VERSION {
        return Err(ArtifactFileQueryError::InvalidWire(format!(
            "artifact-file lifecycle wire schema mismatch for {wire_name}: \
             got {schema_version}, expected \
             {ARTIFACT_FILE_LIFECYCLE_WIRE_SCHEMA_VERSION}"
        )));
    }
    Ok(())
}

fn sum_bytes<'a>(rows: impl Iterator<Item = &'a ArtifactFileWire>) -> u64 {
    rows.filter_map(|row| row.size_bytes).sum()
}

fn group_rows(
    rows: &[ArtifactFileWire],
    key: impl Fn(&ArtifactFileWire) -> Option<&str>,
) -> Vec<ArtifactFileEconomicsGroupWire> {
    let mut groups = BTreeMap::<String, (u64, u64)>::new();
    for row in rows {
        let key = key(row)
            .filter(|value| !value.is_empty())
            .unwrap_or("(unknown)")
            .to_string();
        let group = groups.entry(key).or_default();
        group.0 += 1;
        group.1 += row.size_bytes.unwrap_or(0);
    }
    let mut groups = groups
        .into_iter()
        .map(|(key, (rows, bytes))| ArtifactFileEconomicsGroupWire {
            key,
            rows,
            bytes,
        })
        .collect::<Vec<_>>();
    groups.sort_by(|left, right| {
        right
            .bytes
            .cmp(&left.bytes)
            .then_with(|| right.rows.cmp(&left.rows))
            .then_with(|| left.key.cmp(&right.key))
    });
    groups
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

fn redundancy(rows: &[ArtifactFileWire]) -> (u64, u64, u64) {
    let mut groups = HashMap::<&str, Vec<&ArtifactFileWire>>::new();
    for row in rows {
        if let Some(digest) =
            row.sha256.as_deref().filter(|value| !value.is_empty())
        {
            groups.entry(digest).or_default().push(row);
        }
    }
    let mut duplicate_groups = 0;
    let mut redundant_rows = 0;
    let mut redundant_bytes = 0;
    for group in groups.values_mut().filter(|group| group.len() > 1) {
        duplicate_groups += 1;
        group.sort_by(|left, right| {
            row_recency_key(right).cmp(&row_recency_key(left))
        });
        redundant_rows += (group.len() - 1) as u64;
        redundant_bytes += group
            .iter()
            .skip(1)
            .filter_map(|row| row.size_bytes)
            .sum::<u64>();
    }
    (duplicate_groups, redundant_rows, redundant_bytes)
}

fn generation_projection(
    rows: &[ArtifactFileWire],
    keep_per_label: u64,
) -> ArtifactFileGenerationProjectionWire {
    let mut groups =
        BTreeMap::<(Option<&str>, Option<&str>), Vec<&ArtifactFileWire>>::new();
    for row in rows {
        if row.explicit {
            continue;
        }
        groups
            .entry((row.project.as_deref(), row.label.as_deref()))
            .or_default()
            .push(row);
    }
    let mut rows_freed = 0;
    let mut bytes_freed = 0;
    for group in groups.values_mut() {
        group.sort_by(|left, right| {
            row_recency_key(right).cmp(&row_recency_key(left))
        });
        for row in group.iter().skip(keep_per_label as usize) {
            rows_freed += 1;
            bytes_freed += row.size_bytes.unwrap_or(0);
        }
    }
    ArtifactFileGenerationProjectionWire {
        keep_per_label,
        rows_freed,
        bytes_freed,
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use serde_json::json;
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn aggregates_mixed_rows_truncation_redundancy_and_projections() {
        let temp = tempdir().unwrap();
        let index = temp.path().join("index.jsonl");
        let rows = [
            json!({"schema_version":2,"artifact":{
                "id":"explicit","label":"logo","kind":"image","path":"/store/a",
                "source_path":"/ws/a","workspace_dir":"/ws","project":"p",
                "agent_name":"a","created_at":"2026-07-30T10:00:00Z",
                "explicit":true,"sha256":"same","size_bytes":10
            }}),
            json!({"schema_version":2,"artifact":{
                "id":"automatic-new","label":"logo","kind":"image","path":"/store/b",
                "source_path":"/ws/b","workspace_dir":"/ws","project":"p",
                "agent_name":"b","created_at":"2026-07-30T09:00:00Z",
                "sha256":"same","size_bytes":20
            }}),
            json!({"schema_version":2,"artifact":{
                "id":"automatic-old","label":"logo","kind":"markdown","path":"/store/c",
                "source_path":"/outside/c","workspace_dir":"/ws","project":"p",
                "agent_name":"c","created_at":"2026-07-29T09:00:00Z",
                "sha256":"same","size_bytes":30
            }}),
            json!({"schema_version":2,"artifact":{
                "id":"vcs","label":"notes","kind":"markdown","path":null,
                "vcs_repo":"p","vcs_sha":"abc","vcs_relpath":"notes.md",
                "project":"p","agent_name":"c",
                "created_at":"2026-07-29T08:00:00Z","size_bytes":40
            }}),
            json!({"schema_version":2,"artifact":{
                "id":"unknown-size","label":"notes","kind":"markdown","path":"/store/e",
                "project":"p","agent_name":"c",
                "created_at":"2026-07-29T07:00:00Z"
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
        let economics = artifact_file_store_economics(
            &index,
            &ArtifactFileEconomicsOptionsWire {
                top_n: 1,
                generation_projections: vec![1, 3, 5],
                ..Default::default()
            },
        )
        .unwrap();

        assert_eq!(economics.total_rows, 5);
        assert_eq!(economics.explicit_rows, 1);
        assert_eq!(economics.automatic_rows, 4);
        assert_eq!(economics.vcs_backed_rows, 1);
        assert_eq!(economics.rows_missing_size, 1);
        assert_eq!(economics.total_bytes, 100);
        assert_eq!(economics.by_agent[0].key, "c");
        assert_eq!(economics.by_agent_truncated_groups, 2);
        assert_eq!(economics.by_agent_truncated_bytes, 30);
        assert_eq!(economics.window_days, 2);
        assert_eq!(economics.bytes_per_day, 50.0);
        assert_eq!(economics.duplicate_digest_groups, 1);
        assert_eq!(economics.redundant_digest_rows, 2);
        assert_eq!(economics.redundant_digest_bytes, 50);
        assert_eq!(economics.distinct_labels, 2);
        assert_eq!(
            economics.label_generation_projections,
            vec![
                ArtifactFileGenerationProjectionWire {
                    keep_per_label: 1,
                    rows_freed: 2,
                    bytes_freed: 30,
                },
                ArtifactFileGenerationProjectionWire {
                    keep_per_label: 3,
                    rows_freed: 0,
                    bytes_freed: 0,
                },
                ArtifactFileGenerationProjectionWire {
                    keep_per_label: 5,
                    rows_freed: 0,
                    bytes_freed: 0,
                },
            ]
        );
        assert_eq!(economics.source_inside_workspace_rows, 1);
        assert_eq!(economics.source_inside_workspace_bytes, 20);
    }

    #[test]
    fn single_day_window_has_finite_rates_and_schema_is_checked() {
        let temp = tempdir().unwrap();
        let index = temp.path().join("index.jsonl");
        fs::write(
            &index,
            "{\"schema_version\":2,\"artifact\":{\"id\":\"one\",\
             \"path\":\"/one\",\"created_at\":\"2026-07-30T00:00:00Z\",\
             \"size_bytes\":7}}\n",
        )
        .unwrap();
        let result = artifact_file_store_economics(
            &index,
            &ArtifactFileEconomicsOptionsWire::default(),
        )
        .unwrap();
        assert_eq!(result.window_days, 1);
        assert_eq!(result.bytes_per_day, 7.0);
        assert_eq!(result.rows_per_day, 1.0);

        let error = artifact_file_store_economics(
            &index,
            &ArtifactFileEconomicsOptionsWire {
                schema_version: 99,
                ..Default::default()
            },
        )
        .unwrap_err();
        assert!(error.to_string().contains("schema mismatch"));
    }
}
