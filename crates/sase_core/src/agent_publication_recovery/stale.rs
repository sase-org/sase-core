//! Report-first repair classification for stale publication revisions.
//!
//! Rows enqueued before the post-commit revision fix name a pre-commit
//! primary revision that no published run carries, so they never
//! acknowledge. The host gathers per-row facts — the agent's recorded run
//! commits in order, which of those the requested revision is an ancestor
//! of, and whether the revision resolves to a linked-repo commit — and this
//! module classifies the row without touching git or the outbox:
//!
//! - `unchanged`: the requested revision is already among the agent's run
//!   commits, or the row already carries linked scope.
//! - `linked`: the requested revision is a linked-repo commit, so the row
//!   takes linked scope.
//! - `repaired`: the earliest recorded run commit descending from the
//!   requested revision replaces it as the fulfilling revision.
//! - `unresolved`: no evidence, so the row is kept exactly as it is.

use crate::commit_sha::commit_shas_equivalent;

use super::support::{require_nonempty, require_schema};
use super::wire::{
    PublicationRecoveryError, StalePublicationRepairRequestWire,
    StalePublicationRepairResponseWire,
    PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION, PUBLICATION_SCOPE_LINKED,
    PUBLICATION_SCOPE_REVISION, STALE_REPAIR_ACTION_LINKED,
    STALE_REPAIR_ACTION_REPAIRED, STALE_REPAIR_ACTION_UNCHANGED,
    STALE_REPAIR_ACTION_UNRESOLVED,
};

pub fn classify_stale_publication_request(
    request: &StalePublicationRepairRequestWire,
) -> Result<StalePublicationRepairResponseWire, PublicationRecoveryError> {
    require_schema(request.schema_version)?;
    let requested =
        require_nonempty("requested_revision", &request.requested_revision)?;
    let scope = normalize_scope(request.scope.as_str())?;
    if scope == PUBLICATION_SCOPE_LINKED {
        return Ok(response(
            STALE_REPAIR_ACTION_UNCHANGED,
            None,
            "linked-scope requests fulfill without revision association",
        ));
    }
    if request
        .run_commit_shas
        .iter()
        .any(|sha| commit_shas_equivalent(&requested, sha))
    {
        return Ok(response(
            STALE_REPAIR_ACTION_UNCHANGED,
            None,
            "requested revision is already among the agent's run commits",
        ));
    }
    if request.linked_revision {
        return Ok(response(
            STALE_REPAIR_ACTION_LINKED,
            None,
            "requested revision belongs to a linked repository",
        ));
    }
    if let Some(fulfilling) = request.run_commit_shas.iter().find(|sha| {
        request
            .ancestor_commit_shas
            .iter()
            .any(|ancestor| *ancestor == **sha)
    }) {
        return Ok(response(
            STALE_REPAIR_ACTION_REPAIRED,
            Some(fulfilling.to_string()),
            &format!(
                "requested revision is an ancestor of recorded run commit \
                 {fulfilling}"
            ),
        ));
    }
    if request.run_commit_shas.is_empty() {
        return Ok(response(
            STALE_REPAIR_ACTION_UNRESOLVED,
            None,
            "no recorded run commits for this agent",
        ));
    }
    Ok(response(
        STALE_REPAIR_ACTION_UNRESOLVED,
        None,
        "no recorded run commit descends from the requested revision",
    ))
}

fn normalize_scope(
    scope: &str,
) -> Result<&'static str, PublicationRecoveryError> {
    match scope.trim() {
        "" | PUBLICATION_SCOPE_REVISION => Ok(PUBLICATION_SCOPE_REVISION),
        PUBLICATION_SCOPE_LINKED => Ok(PUBLICATION_SCOPE_LINKED),
        other => Err(PublicationRecoveryError::InvalidRequest(format!(
            "unknown publication scope {other:?}"
        ))),
    }
}

fn response(
    action: &str,
    fulfilling: Option<String>,
    reason: &str,
) -> StalePublicationRepairResponseWire {
    StalePublicationRepairResponseWire {
        schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
        action: action.to_string(),
        fulfilling_revision: fulfilling.clone(),
        evidence_commit: fulfilling,
        reason: reason.to_string(),
    }
}
