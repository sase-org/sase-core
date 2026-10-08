//! Mutation-side bridge to snapshot-free read-model publication.
//!
//! After the durable event append, cached create/note/update mutations
//! publish their delta through
//! [`crate::bead::read_model::publish_mutation_write`] instead of
//! re-sweeping the store: the admission witness plus the
//! writer-captured stream signatures is the only pre-write state the
//! commit trusts. The bridge never fails the mutation: a lost race
//! skips, a fault invalidates for the next read to repair, and the
//! events stand either way.

use std::path::Path;

use crate::bead::read_model::{
    fingerprint_manifest_config, publish_mutation_write, AppendedStream,
    CacheWitness, ManifestConfigFingerprint, PublishOutcome,
};
use crate::bead::wire::IssueWire;

/// Publish one cached mutation write; never fails the mutation.
///
/// `witness` is the admission witness, `appended` the new tail events
/// per changed stream with writer-captured signatures, and `expected`
/// the mutation's overlaid rows. `Skipped` and `Invalidated` both
/// leave the durable events standing for the read path to repair.
pub(crate) fn publish_cached_write(
    beads_dir: &Path,
    cache_path: &Path,
    witness: Option<&CacheWitness>,
    appended: &[AppendedStream],
    expected: &[(String, IssueWire)],
) -> PublishOutcome {
    let Some(witness) = witness else {
        return PublishOutcome::Skipped;
    };
    let fingerprint: ManifestConfigFingerprint =
        match fingerprint_manifest_config(beads_dir) {
            Ok(fingerprint) => fingerprint,
            Err(_) => return PublishOutcome::Skipped,
        };
    publish_mutation_write(
        beads_dir,
        cache_path,
        witness,
        appended,
        &fingerprint,
        expected,
    )
}

/// Apply reducer-truth corrections to outcome rows, keyed by issue ID.
pub(crate) fn apply_corrections(
    rows: &mut [IssueWire],
    corrected: &[(String, IssueWire)],
) {
    for row in rows.iter_mut() {
        if let Some((_, truth)) = corrected.iter().find(|(id, _)| id == &row.id)
        {
            *row = truth.clone();
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::bead::wire::IssueWire;

    fn test_issue(id: &str) -> IssueWire {
        serde_json::from_value(serde_json::json!({
            "id": id,
            "title": "t",
            "status": "open",
            "issue_type": "task",
            "tier": null,
            "parent_id": null,
            "owner": "o",
            "assignee": "",
            "created_at": "2026-01-01T00:00:00Z",
            "created_by": "o",
            "updated_at": "2026-01-01T00:00:00Z",
            "closed_at": null,
            "close_reason": null,
            "resolution": null,
            "close_history": [],
            "description": "",
            "notes": [],
            "design": null,
            "refs": [],
            "links": [],
            "plus_one_evidence": [],
            "snooze": null,
            "model": "m",
            "size": "small",
            "task_type": "bug",
            "task_type_fields": {},
            "is_ready_to_work": false,
            "changespec_name": null,
            "changespec_bug_id": null,
            "external_ref": "",
            "creation_reason": "",
            "dependencies": []
        }))
        .unwrap()
    }

    #[test]
    fn corrections_replace_only_matching_rows() {
        let mut rows = vec![test_issue("a-1"), test_issue("a-2")];
        let mut truth = test_issue("a-1");
        truth.title = "corrected".to_string();
        super::apply_corrections(
            &mut rows,
            &[("a-1".to_string(), truth.clone())],
        );
        assert_eq!(rows[0].title, "corrected");
        assert_eq!(rows[1].title, "t");
    }
}
