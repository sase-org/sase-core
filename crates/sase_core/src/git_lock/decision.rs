//! Stop / retry / wait / remove decisions for index-lock recovery.

use super::delays::{delay_seconds_to_ns, GitLockError};
use super::wire::{
    GitLockEagerCleanupRequestWire, GitLockRecoveryDecisionWire,
    GitLockRecoveryRequestWire, GitLockSnapshotWire,
    GIT_LOCK_ACTION_REMOVE_THEN_RETRY, GIT_LOCK_ACTION_RETRY,
    GIT_LOCK_ACTION_RETRY_WITHOUT_REMOVAL, GIT_LOCK_ACTION_STOP,
    GIT_LOCK_ACTION_WAIT, GIT_LOCK_FAILURE_INDEX_LOCK,
    GIT_LOCK_FAILURE_OTHER_LOCK, GIT_LOCK_FAILURE_SUCCESS,
    GIT_LOCK_FILE_REGULAR, GIT_LOCK_OWNER_ABSENT, GIT_LOCK_OWNER_ACTIVE,
    GIT_LOCK_OWNER_UNKNOWN, GIT_LOCK_WIRE_SCHEMA_VERSION,
    STALE_GIT_INDEX_LOCK_MIN_AGE_NS,
};

/// Decide the next recovery action from classified failure and observations.
pub fn decide_git_lock_recovery(
    request: &GitLockRecoveryRequestWire,
) -> Result<GitLockRecoveryDecisionWire, GitLockError> {
    if request.schema_version != GIT_LOCK_WIRE_SCHEMA_VERSION {
        return Err(GitLockError::Schema {
            expected: GIT_LOCK_WIRE_SCHEMA_VERSION,
            actual: request.schema_version,
        });
    }
    let identity_disqualified = request.identity_disqualified
        || snapshots_conflict(
            request.first_snapshot.as_ref(),
            request.current_snapshot.as_ref(),
        );
    let lock_path = request.path_match.matched_path.clone().or_else(|| {
        request
            .current_snapshot
            .as_ref()
            .map(|snapshot| snapshot.path.clone())
    });

    if request.cancelled {
        return Ok(stop("cancelled", lock_path, identity_disqualified));
    }

    match request.failure.kind.as_str() {
        GIT_LOCK_FAILURE_SUCCESS => {
            Ok(stop("command succeeded", lock_path, identity_disqualified))
        }
        GIT_LOCK_FAILURE_OTHER_LOCK => {
            Ok(retry_or_give_up(request, lock_path, identity_disqualified))
        }
        GIT_LOCK_FAILURE_INDEX_LOCK => {
            Ok(decide_index_lock(request, lock_path, identity_disqualified))
        }
        _ => Ok(stop(
            request.failure.reason.clone(),
            lock_path,
            identity_disqualified,
        )),
    }
}

/// Decide whether an eager pre-command cleanup may remove an aged lock.
pub fn decide_eager_git_lock_cleanup(
    request: &GitLockEagerCleanupRequestWire,
) -> Result<GitLockRecoveryDecisionWire, GitLockError> {
    if request.schema_version != GIT_LOCK_WIRE_SCHEMA_VERSION {
        return Err(GitLockError::Schema {
            expected: GIT_LOCK_WIRE_SCHEMA_VERSION,
            actual: request.schema_version,
        });
    }
    let Some(snapshot) = request.snapshot.as_ref() else {
        return Ok(stop("no index lock present", None, false));
    };
    if snapshot.file_type != GIT_LOCK_FILE_REGULAR {
        return Ok(stop(
            "index lock is not a regular file",
            Some(snapshot.path.clone()),
            false,
        ));
    }
    if request.owner.status == GIT_LOCK_OWNER_ACTIVE {
        return Ok(stop(
            "live owner holds the index lock",
            Some(snapshot.path.clone()),
            false,
        ));
    }
    if request.owner.status != GIT_LOCK_OWNER_ABSENT {
        return Ok(stop(
            request.owner.reason.clone(),
            Some(snapshot.path.clone()),
            false,
        ));
    }
    let age = request.now_unix_ns.saturating_sub(snapshot.mtime_ns);
    if age < request.min_age_ns {
        return Ok(stop(
            "index lock is younger than the stale-age threshold",
            Some(snapshot.path.clone()),
            false,
        ));
    }
    Ok(GitLockRecoveryDecisionWire {
        schema_version: GIT_LOCK_WIRE_SCHEMA_VERSION,
        action: GIT_LOCK_ACTION_REMOVE_THEN_RETRY.to_string(),
        delay_ns: None,
        reason: "aged owner-free index lock".to_string(),
        lock_path: Some(snapshot.path.clone()),
        claim_lock_removed: true,
        identity_disqualified: false,
    })
}

fn decide_index_lock(
    request: &GitLockRecoveryRequestWire,
    lock_path: Option<String>,
    identity_disqualified: bool,
) -> GitLockRecoveryDecisionWire {
    if let Some(decision) =
        maybe_backoff(request, lock_path.clone(), identity_disqualified)
    {
        return decision;
    }
    if request.removal_already_attempted {
        return stop(
            "final retry after lock recovery already used",
            lock_path,
            identity_disqualified,
        );
    }
    if !request.allow_lock_removal {
        return stop("lock removal disabled", lock_path, identity_disqualified);
    }
    if !request.path_match.eligible_for_removal {
        return stop(
            request.path_match.reason.clone(),
            lock_path,
            identity_disqualified,
        );
    }
    if identity_disqualified {
        return stop("lock path or file identity changed", lock_path, true);
    }
    match request.current_snapshot.as_ref() {
        None => GitLockRecoveryDecisionWire {
            schema_version: GIT_LOCK_WIRE_SCHEMA_VERSION,
            action: GIT_LOCK_ACTION_RETRY_WITHOUT_REMOVAL.to_string(),
            delay_ns: None,
            reason: "index lock already gone".to_string(),
            lock_path,
            claim_lock_removed: false,
            identity_disqualified: false,
        },
        Some(snapshot) if snapshot.file_type != GIT_LOCK_FILE_REGULAR => stop(
            "index lock is not a regular file",
            Some(snapshot.path.clone()),
            false,
        ),
        Some(snapshot) => match request.owner.status.as_str() {
            GIT_LOCK_OWNER_ACTIVE => stop(
                "live owner holds the index lock",
                Some(snapshot.path.clone()),
                false,
            ),
            GIT_LOCK_OWNER_UNKNOWN => stop(
                request.owner.reason.clone(),
                Some(snapshot.path.clone()),
                false,
            ),
            GIT_LOCK_OWNER_ABSENT => decide_absent_owner(request, snapshot),
            _ => stop(
                "invalid owner observation",
                Some(snapshot.path.clone()),
                false,
            ),
        },
    }
}

fn decide_absent_owner(
    request: &GitLockRecoveryRequestWire,
    snapshot: &GitLockSnapshotWire,
) -> GitLockRecoveryDecisionWire {
    let age = request.now_unix_ns.saturating_sub(snapshot.mtime_ns);
    if age >= STALE_GIT_INDEX_LOCK_MIN_AGE_NS {
        if request.remaining_recovery_budget_ns <= 0 {
            return stop(
                "recovery budget exhausted",
                Some(snapshot.path.clone()),
                false,
            );
        }
        return GitLockRecoveryDecisionWire {
            schema_version: GIT_LOCK_WIRE_SCHEMA_VERSION,
            action: GIT_LOCK_ACTION_REMOVE_THEN_RETRY.to_string(),
            delay_ns: None,
            reason: "aged owner-free index lock".to_string(),
            lock_path: Some(snapshot.path.clone()),
            claim_lock_removed: true,
            identity_disqualified: false,
        };
    }
    let needed = STALE_GIT_INDEX_LOCK_MIN_AGE_NS - age;
    let mut cap = request.remaining_recovery_budget_ns;
    if let Some(deadline) = request.remaining_command_deadline_ns {
        cap = cap.min(deadline);
    }
    if cap <= 0 || needed > cap {
        return stop(
            "index lock is too young and the wait budget is insufficient",
            Some(snapshot.path.clone()),
            false,
        );
    }
    GitLockRecoveryDecisionWire {
        schema_version: GIT_LOCK_WIRE_SCHEMA_VERSION,
        action: GIT_LOCK_ACTION_WAIT.to_string(),
        delay_ns: Some(needed.min(cap)),
        reason: "waiting for owner-free index lock to reach stale age"
            .to_string(),
        lock_path: Some(snapshot.path.clone()),
        claim_lock_removed: false,
        identity_disqualified: false,
    }
}

fn retry_or_give_up(
    request: &GitLockRecoveryRequestWire,
    lock_path: Option<String>,
    identity_disqualified: bool,
) -> GitLockRecoveryDecisionWire {
    maybe_backoff(request, lock_path.clone(), identity_disqualified)
        .unwrap_or_else(|| {
            stop("retries exhausted", lock_path, identity_disqualified)
        })
}

fn maybe_backoff(
    request: &GitLockRecoveryRequestWire,
    lock_path: Option<String>,
    identity_disqualified: bool,
) -> Option<GitLockRecoveryDecisionWire> {
    let index = request.attempts.saturating_sub(1) as usize;
    let delay = *request.delays.get(index)?;
    let delay_ns = delay_seconds_to_ns(delay)?;
    Some(GitLockRecoveryDecisionWire {
        schema_version: GIT_LOCK_WIRE_SCHEMA_VERSION,
        action: GIT_LOCK_ACTION_RETRY.to_string(),
        delay_ns: Some(delay_ns),
        reason: format!("retry after {delay:.3}s backoff"),
        lock_path,
        claim_lock_removed: false,
        identity_disqualified,
    })
}

pub(crate) fn snapshots_conflict(
    first: Option<&GitLockSnapshotWire>,
    current: Option<&GitLockSnapshotWire>,
) -> bool {
    match (first, current) {
        (None, Some(_)) => true,
        (Some(first), Some(current)) => !identities_equal(first, current),
        _ => false,
    }
}

pub(crate) fn identities_equal(
    left: &GitLockSnapshotWire,
    right: &GitLockSnapshotWire,
) -> bool {
    left.path == right.path
        && left.device == right.device
        && left.inode == right.inode
        && left.size == right.size
        && left.mtime_ns == right.mtime_ns
        && left.ctime_ns == right.ctime_ns
}

fn stop(
    reason: impl Into<String>,
    lock_path: Option<String>,
    identity_disqualified: bool,
) -> GitLockRecoveryDecisionWire {
    GitLockRecoveryDecisionWire {
        schema_version: GIT_LOCK_WIRE_SCHEMA_VERSION,
        action: GIT_LOCK_ACTION_STOP.to_string(),
        delay_ns: None,
        reason: reason.into(),
        lock_path,
        claim_lock_removed: false,
        identity_disqualified,
    }
}
