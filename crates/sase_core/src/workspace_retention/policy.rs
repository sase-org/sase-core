//! Pure classification of workspace-retention facts.

use super::wire::{
    WorkspaceRetentionClaimWire, WorkspaceRetentionDecisionWire,
    WorkspaceRetentionRequestWire, WorkspaceRetentionSuccessorWire,
    WORKSPACE_RETENTION_DISPOSITION_OWNERSHIP_MOVED,
    WORKSPACE_RETENTION_DISPOSITION_RELEASE,
    WORKSPACE_RETENTION_DISPOSITION_RETAIN,
    WORKSPACE_RETENTION_REASON_CLAIM_OWNERSHIP_MOVED,
    WORKSPACE_RETENTION_REASON_CREATOR_RESTORED,
    WORKSPACE_RETENTION_REASON_EVIDENCE_UNAVAILABLE,
    WORKSPACE_RETENTION_REASON_GATE_RELEASED_WORKSPACE,
    WORKSPACE_RETENTION_REASON_MISSING_BASELINE_WITH_CUSTODY,
    WORKSPACE_RETENTION_REASON_PROVEN_CLEAN,
    WORKSPACE_RETENTION_REASON_SAME_WORKSPACE_SUCCESSOR,
    WORKSPACE_RETENTION_REASON_SESSION_OWNED_DIRT,
    WORKSPACE_RETENTION_WIRE_SCHEMA_VERSION,
};

/// Classify whether the settling owner should retain, release, or stand down.
#[must_use]
pub fn decide_workspace_retention(
    request: &WorkspaceRetentionRequestWire,
) -> WorkspaceRetentionDecisionWire {
    let claim_matches = observed_matches(
        &request.expected_claim,
        request.observed_claim.as_ref(),
    );

    if !claim_matches {
        let reason = if request.gate_released_workspace {
            WORKSPACE_RETENTION_REASON_GATE_RELEASED_WORKSPACE
        } else {
            WORKSPACE_RETENTION_REASON_CLAIM_OWNERSHIP_MOVED
        };
        return decision(
            WORKSPACE_RETENTION_DISPOSITION_OWNERSHIP_MOVED,
            reason,
        );
    }

    if successor_takes_workspace(&request.successor) {
        return decision(
            WORKSPACE_RETENTION_DISPOSITION_RELEASE,
            WORKSPACE_RETENTION_REASON_SAME_WORKSPACE_SUCCESSOR,
        );
    }

    if request.creator_restored {
        return decision(
            WORKSPACE_RETENTION_DISPOSITION_RELEASE,
            WORKSPACE_RETENTION_REASON_CREATOR_RESTORED,
        );
    }

    if request.probe_failed {
        return decision(
            WORKSPACE_RETENTION_DISPOSITION_RETAIN,
            WORKSPACE_RETENTION_REASON_EVIDENCE_UNAVAILABLE,
        );
    }

    if request.dirt.session_owned {
        return decision(
            WORKSPACE_RETENTION_DISPOSITION_RETAIN,
            WORKSPACE_RETENTION_REASON_SESSION_OWNED_DIRT,
        );
    }

    if request.dirt.missing_baseline && request.dirt.positive_custody {
        return decision(
            WORKSPACE_RETENTION_DISPOSITION_RETAIN,
            WORKSPACE_RETENTION_REASON_MISSING_BASELINE_WITH_CUSTODY,
        );
    }

    if request.dirt.all_proven_clean || request.dirt.only_unchanged_foreign {
        return decision(
            WORKSPACE_RETENTION_DISPOSITION_RELEASE,
            WORKSPACE_RETENTION_REASON_PROVEN_CLEAN,
        );
    }

    decision(
        WORKSPACE_RETENTION_DISPOSITION_RETAIN,
        WORKSPACE_RETENTION_REASON_EVIDENCE_UNAVAILABLE,
    )
}

fn successor_takes_workspace(
    successor: &WorkspaceRetentionSuccessorWire,
) -> bool {
    successor.confirmed_active
        && successor.same_workspace
        && successor.inherited_obligations
}

fn observed_matches(
    expected: &WorkspaceRetentionClaimWire,
    observed: Option<&WorkspaceRetentionClaimWire>,
) -> bool {
    let Some(observed) = observed else {
        return false;
    };
    observed.workspace_num == expected.workspace_num
        && observed.workflow == expected.workflow
        && observed.cl_name == expected.cl_name
        && observed.artifacts_timestamp == expected.artifacts_timestamp
        && observed.pid == expected.pid
}

fn decision(disposition: &str, reason: &str) -> WorkspaceRetentionDecisionWire {
    WorkspaceRetentionDecisionWire {
        schema_version: WORKSPACE_RETENTION_WIRE_SCHEMA_VERSION,
        disposition: disposition.to_string(),
        reason: reason.to_string(),
    }
}
