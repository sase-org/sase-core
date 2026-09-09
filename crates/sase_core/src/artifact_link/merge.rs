//! Conservative three-way merges for per-artifact link indexes.

use std::collections::{BTreeMap, BTreeSet};

use super::wire::{
    artifact_link_dedup_key, canonicalize_artifact_link_ref,
    validate_artifact_link_row, ArtifactLinkDedupKeyWire, ArtifactLinkError,
    ArtifactLinkIndexWire, ArtifactLinkRowWire,
    ARTIFACT_LINK_ROW_SCHEMA_VERSION,
};

#[derive(Debug, Clone, PartialEq, Eq)]
struct ValidatedIndex {
    artifact_ref: String,
    rows: BTreeMap<ArtifactLinkDedupKeyWire, ArtifactLinkRowWire>,
    order: Vec<ArtifactLinkDedupKeyWire>,
}

/// Merge three validated schema-v2 per-artifact link indexes.
///
/// This resolver is deliberately narrower than a generic JSON merge. It only
/// understands the legacy v2 link-index contract: one row per relation-aware
/// dedup key, all rows touching the same `artifact_ref`, and no guessed
/// resolution for same-key concurrent edits or modify/delete conflicts.
pub fn merge_artifact_link_indexes(
    base: &ArtifactLinkIndexWire,
    ours: &ArtifactLinkIndexWire,
    theirs: &ArtifactLinkIndexWire,
) -> Result<ArtifactLinkIndexWire, ArtifactLinkError> {
    let base = validate_index("base", base)?;
    let ours = validate_index("ours", ours)?;
    let theirs = validate_index("theirs", theirs)?;
    if base.artifact_ref != ours.artifact_ref
        || base.artifact_ref != theirs.artifact_ref
    {
        return Err(ArtifactLinkError::validation(format!(
            "artifact link indexes must name the same artifact_ref: base={}, ours={}, theirs={}",
            base.artifact_ref, ours.artifact_ref, theirs.artifact_ref
        )));
    }

    let mut all_keys: BTreeSet<ArtifactLinkDedupKeyWire> = BTreeSet::new();
    all_keys.extend(base.rows.keys().cloned());
    all_keys.extend(ours.rows.keys().cloned());
    all_keys.extend(theirs.rows.keys().cloned());

    let mut merged_by_key: BTreeMap<
        ArtifactLinkDedupKeyWire,
        ArtifactLinkRowWire,
    > = BTreeMap::new();
    let mut ambiguous: Vec<String> = Vec::new();
    for key in all_keys {
        match merge_row(
            base.rows.get(&key),
            ours.rows.get(&key),
            theirs.rows.get(&key),
        ) {
            RowMerge::Keep(row) => {
                merged_by_key.insert(key, row);
            }
            RowMerge::Delete => {}
            RowMerge::Conflict => ambiguous.push(dedup_key_label(&key)),
        }
    }

    if !ambiguous.is_empty() {
        ambiguous.sort();
        return Err(ArtifactLinkError::conflict(format!(
            "ambiguous artifact-link index merge keys: {}",
            ambiguous.join(", ")
        )));
    }

    let mut rows: Vec<ArtifactLinkRowWire> = Vec::new();
    for key in &base.order {
        if let Some(row) = merged_by_key.get(key) {
            rows.push(row.clone());
        }
    }
    let base_keys: BTreeSet<ArtifactLinkDedupKeyWire> =
        base.order.iter().cloned().collect();
    let mut added: Vec<(
        String,
        ArtifactLinkDedupKeyWire,
        ArtifactLinkRowWire,
    )> = merged_by_key
        .into_iter()
        .filter(|(key, _row)| !base_keys.contains(key))
        .map(|(key, row)| (row.created_at.clone(), key, row))
        .collect();
    added.sort_by(|left, right| {
        left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1))
    });
    rows.extend(added.into_iter().map(|(_created_at, _key, row)| row));

    Ok(ArtifactLinkIndexWire {
        schema_version: ARTIFACT_LINK_ROW_SCHEMA_VERSION,
        artifact_ref: base.artifact_ref,
        rows,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RowMerge {
    Keep(ArtifactLinkRowWire),
    Delete,
    Conflict,
}

fn merge_row(
    base: Option<&ArtifactLinkRowWire>,
    ours: Option<&ArtifactLinkRowWire>,
    theirs: Option<&ArtifactLinkRowWire>,
) -> RowMerge {
    match (base, ours, theirs) {
        (Some(base), Some(ours), Some(theirs))
            if ours == base && theirs == base =>
        {
            RowMerge::Keep(base.clone())
        }
        (Some(base), Some(ours), Some(theirs)) if ours == base => {
            RowMerge::Keep(theirs.clone())
        }
        (Some(base), Some(ours), Some(theirs)) if theirs == base => {
            RowMerge::Keep(ours.clone())
        }
        (Some(_base), Some(ours), Some(theirs)) if ours == theirs => {
            RowMerge::Keep(ours.clone())
        }
        (Some(base), None, Some(theirs)) if theirs == base => RowMerge::Delete,
        (Some(base), Some(ours), None) if ours == base => RowMerge::Delete,
        (Some(_base), None, None) => RowMerge::Delete,
        (Some(_base), None, Some(_)) | (Some(_base), Some(_), None) => {
            RowMerge::Conflict
        }
        (Some(_base), Some(_ours), Some(_theirs)) => RowMerge::Conflict,
        (None, Some(ours), None) => RowMerge::Keep(ours.clone()),
        (None, None, Some(theirs)) => RowMerge::Keep(theirs.clone()),
        (None, Some(ours), Some(theirs)) if ours == theirs => {
            RowMerge::Keep(ours.clone())
        }
        (None, Some(_ours), Some(_theirs)) => RowMerge::Conflict,
        (None, None, None) => RowMerge::Delete,
    }
}

fn validate_index(
    label: &str,
    index: &ArtifactLinkIndexWire,
) -> Result<ValidatedIndex, ArtifactLinkError> {
    if index.schema_version != ARTIFACT_LINK_ROW_SCHEMA_VERSION {
        return Err(ArtifactLinkError::validation(format!(
            "{label} artifact link index schema_version {} is unsupported; expected {}",
            index.schema_version, ARTIFACT_LINK_ROW_SCHEMA_VERSION
        )));
    }
    let artifact_ref = canonicalize_artifact_link_ref(&index.artifact_ref)?;
    let mut rows = BTreeMap::new();
    let mut order = Vec::new();
    for (position, raw) in index.rows.iter().enumerate() {
        let row = validate_artifact_link_row(raw).map_err(|error| {
            ArtifactLinkError {
                kind: error.kind,
                message: format!(
                    "{label} artifact link index rows[{position}]: {}",
                    error.message
                ),
            }
        })?;
        if row.source_ref != artifact_ref && row.target_ref != artifact_ref {
            return Err(ArtifactLinkError::validation(format!(
                "{label} artifact link index rows[{position}] does not touch artifact_ref {artifact_ref}"
            )));
        }
        let key = artifact_link_dedup_key(&row)?;
        if rows.insert(key.clone(), row).is_some() {
            return Err(ArtifactLinkError::validation(format!(
                "{label} artifact link index contains duplicate dedup key {}",
                dedup_key_label(&key)
            )));
        }
        order.push(key);
    }
    Ok(ValidatedIndex {
        artifact_ref,
        rows,
        order,
    })
}

fn dedup_key_label(key: &ArtifactLinkDedupKeyWire) -> String {
    match key {
        ArtifactLinkDedupKeyWire::Directed {
            source_ref,
            relation,
            target_ref,
        } => format!("{source_ref} {relation} {target_ref}"),
        ArtifactLinkDedupKeyWire::Undirected {
            relation,
            left_ref,
            right_ref,
        } => format!("{left_ref} {relation} {right_ref}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(
        source: &str,
        relation: &str,
        target: &str,
        description: &str,
        created_at: &str,
    ) -> ArtifactLinkRowWire {
        ArtifactLinkRowWire {
            schema_version: ARTIFACT_LINK_ROW_SCHEMA_VERSION,
            source_ref: source.to_string(),
            relation: relation.to_string(),
            target_ref: target.to_string(),
            description: description.to_string(),
            origin: super::super::wire::ArtifactLinkOriginWire::Manual,
            created_by: "test-agent".to_string(),
            created_at: created_at.to_string(),
            uses: 1,
        }
    }

    fn index(rows: Vec<ArtifactLinkRowWire>) -> ArtifactLinkIndexWire {
        ArtifactLinkIndexWire {
            schema_version: ARTIFACT_LINK_ROW_SCHEMA_VERSION,
            artifact_ref: "plan:202609/a.md".to_string(),
            rows,
        }
    }

    fn base_a() -> ArtifactLinkRowWire {
        row(
            "agent:base",
            "cites",
            "plan:202609/a.md",
            "base citation",
            "2026-09-01T00:00:00Z",
        )
    }

    fn local_b() -> ArtifactLinkRowWire {
        row(
            "agent:local",
            "cites",
            "plan:202609/a.md",
            "local citation",
            "2026-09-03T00:00:00Z",
        )
    }

    fn upstream_c() -> ArtifactLinkRowWire {
        row(
            "agent:upstream",
            "cites",
            "plan:202609/a.md",
            "upstream citation",
            "2026-09-02T00:00:00Z",
        )
    }

    #[test]
    fn validates_schema_artifact_ref_and_duplicates() {
        let mut wrong_schema = index(vec![]);
        wrong_schema.schema_version = 1;
        let err = merge_artifact_link_indexes(
            &wrong_schema,
            &index(vec![]),
            &index(vec![]),
        )
        .unwrap_err();
        assert_eq!(err.kind, "validation");
        assert!(err.message.contains("schema_version"));

        let mut mismatch = index(vec![]);
        mismatch.artifact_ref = "plan:202609/other.md".to_string();
        let err = merge_artifact_link_indexes(
            &index(vec![]),
            &mismatch,
            &index(vec![]),
        )
        .unwrap_err();
        assert!(err.message.contains("same artifact_ref"));

        let duplicate = index(vec![base_a(), base_a()]);
        let err = merge_artifact_link_indexes(
            &index(vec![]),
            &duplicate,
            &index(vec![]),
        )
        .unwrap_err();
        assert!(err.message.contains("duplicate dedup key"));

        let untouched = row(
            "agent:base",
            "cites",
            "plan:202609/other.md",
            "base citation",
            "2026-09-01T00:00:00Z",
        );
        let err = merge_artifact_link_indexes(
            &index(vec![]),
            &index(vec![untouched]),
            &index(vec![]),
        )
        .unwrap_err();
        assert!(err.message.contains("does not touch artifact_ref"));
    }

    #[test]
    fn unions_distinct_additions_in_deterministic_order_after_base_rows() {
        let base = index(vec![base_a()]);
        let ours = index(vec![base_a(), local_b()]);
        let theirs = index(vec![base_a(), upstream_c()]);

        let merged =
            merge_artifact_link_indexes(&base, &ours, &theirs).unwrap();

        assert_eq!(
            merged
                .rows
                .iter()
                .map(|row| row.created_by.as_str())
                .collect::<Vec<_>>(),
            ["test-agent", "test-agent", "test-agent"]
        );
        assert_eq!(
            merged
                .rows
                .iter()
                .map(|row| row.source_ref.as_str())
                .collect::<Vec<_>>(),
            ["agent:base", "agent:upstream", "agent:local"]
        );
        let swapped =
            merge_artifact_link_indexes(&base, &theirs, &ours).unwrap();
        assert_eq!(merged, swapped);
    }

    #[test]
    fn accepts_one_sided_and_identical_edits() {
        let base_row = base_a();
        let mut edited = base_row.clone();
        edited.description = "updated citation".to_string();
        let base = index(vec![base_row.clone()]);

        let merged = merge_artifact_link_indexes(
            &base,
            &index(vec![edited.clone()]),
            &base,
        )
        .unwrap();
        assert_eq!(merged.rows, vec![edited.clone()]);

        let merged = merge_artifact_link_indexes(
            &base,
            &index(vec![edited.clone()]),
            &index(vec![edited.clone()]),
        )
        .unwrap();
        assert_eq!(merged.rows, vec![edited]);
    }

    #[test]
    fn accepts_one_sided_and_identical_deletes() {
        let base_row = base_a();
        let base = index(vec![base_row.clone()]);

        let merged =
            merge_artifact_link_indexes(&base, &index(vec![]), &base).unwrap();
        assert!(merged.rows.is_empty());

        let merged =
            merge_artifact_link_indexes(&base, &index(vec![]), &index(vec![]))
                .unwrap();
        assert!(merged.rows.is_empty());
    }

    #[test]
    fn rejects_competing_same_key_edits_and_modify_delete() {
        let base_row = base_a();
        let mut local = base_row.clone();
        local.description = "local edit".to_string();
        let mut upstream = base_row.clone();
        upstream.description = "upstream edit".to_string();
        let base = index(vec![base_row]);

        let err = merge_artifact_link_indexes(
            &base,
            &index(vec![local]),
            &index(vec![upstream]),
        )
        .unwrap_err();
        assert_eq!(err.kind, "conflict");
        assert!(err.message.contains("agent:base cites plan:202609/a.md"));

        let mut modified = base.rows[0].clone();
        modified.description = "modified".to_string();
        let err = merge_artifact_link_indexes(
            &base,
            &index(vec![]),
            &index(vec![modified]),
        )
        .unwrap_err();
        assert_eq!(err.kind, "conflict");
    }

    #[test]
    fn merge_is_idempotent() {
        let base = index(vec![base_a()]);
        let ours = index(vec![base_a(), local_b()]);
        let theirs = index(vec![base_a(), upstream_c()]);
        let merged =
            merge_artifact_link_indexes(&base, &ours, &theirs).unwrap();

        let again =
            merge_artifact_link_indexes(&base, &merged, &merged).unwrap();

        assert_eq!(again, merged);
    }
}
