//! Classify a Git command failure as index-lock, other-lock, or unrelated.

use super::wire::{
    GitLockFailureClassWire, GitLockFailureObservationWire,
    GIT_LOCK_FAILURE_INDEX_LOCK, GIT_LOCK_FAILURE_NOT_LOCK,
    GIT_LOCK_FAILURE_OTHER_LOCK, GIT_LOCK_FAILURE_SUCCESS,
    GIT_LOCK_WIRE_SCHEMA_VERSION,
};

/// Classify one observed Git failure for lock-recovery purposes.
pub fn classify_git_lock_failure(
    observation: &GitLockFailureObservationWire,
) -> GitLockFailureClassWire {
    if observation.timed_out {
        return not_lock("timed_out", "command timed out");
    }
    if observation.signal_terminated {
        return not_lock("signal_terminated", "command was signal-terminated");
    }
    if let Some(os_error) = observation.os_error.as_deref() {
        let kind = os_error.trim().to_ascii_lowercase();
        if kind == "permission_denied" || kind == "eacces" {
            return not_lock(
                "permission_denied",
                "operating-system permission denied",
            );
        }
    }
    if observation.exit_status == Some(0) {
        return GitLockFailureClassWire {
            schema_version: GIT_LOCK_WIRE_SCHEMA_VERSION,
            kind: GIT_LOCK_FAILURE_SUCCESS.to_string(),
            retryable: false,
            parsed_lock_path: None,
            reason: "command succeeded".to_string(),
        };
    }

    let parsed_lock_path = parse_unable_to_create_lock_path(
        &observation.stderr,
        &observation.stdout,
    );
    let combined = normalize(&observation.stderr, &observation.stdout);
    let explicit_lock_failure =
        combined.contains("unable to create") && combined.contains(".lock");
    let mentions_index_lock = combined.contains("index.lock");
    let matches_resolved = paths_match(
        parsed_lock_path.as_deref(),
        observation.resolved_index_lock_path.as_deref(),
    );

    let retryable_exit = matches!(observation.exit_status, Some(1) | Some(128));
    if !retryable_exit {
        return not_lock(
            "unretryable_exit",
            "exit status is not a lock-contention failure",
        );
    }

    if observation.exit_status == Some(1) && !explicit_lock_failure {
        return not_lock(
            "incidental_index_lock",
            "exit 1 mentioned a lock without an explicit creation failure",
        );
    }

    if matches_resolved || mentions_index_lock {
        return GitLockFailureClassWire {
            schema_version: GIT_LOCK_WIRE_SCHEMA_VERSION,
            kind: GIT_LOCK_FAILURE_INDEX_LOCK.to_string(),
            retryable: true,
            parsed_lock_path,
            reason: "index lock contention".to_string(),
        };
    }
    if explicit_lock_failure {
        return GitLockFailureClassWire {
            schema_version: GIT_LOCK_WIRE_SCHEMA_VERSION,
            kind: GIT_LOCK_FAILURE_OTHER_LOCK.to_string(),
            retryable: true,
            parsed_lock_path,
            reason: "non-index lock contention".to_string(),
        };
    }
    not_lock("unclassified", "failure is not lock contention")
}

fn not_lock(reason: &str, message: &str) -> GitLockFailureClassWire {
    GitLockFailureClassWire {
        schema_version: GIT_LOCK_WIRE_SCHEMA_VERSION,
        kind: GIT_LOCK_FAILURE_NOT_LOCK.to_string(),
        retryable: false,
        parsed_lock_path: None,
        reason: format!("{reason}: {message}"),
    }
}

fn normalize(stderr: &str, stdout: &str) -> String {
    format!("{stderr}\n{stdout}").to_ascii_lowercase()
}

fn paths_match(left: Option<&str>, right: Option<&str>) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => left == right,
        _ => false,
    }
}

pub(crate) fn parse_unable_to_create_lock_path(
    stderr: &str,
    stdout: &str,
) -> Option<String> {
    parse_one_stream(stderr).or_else(|| parse_one_stream(stdout))
}

fn parse_one_stream(stream: &str) -> Option<String> {
    let lower = stream.to_ascii_lowercase();
    let marker = "unable to create";
    let index = lower.find(marker)?;
    let rest = stream[index + marker.len()..].trim_start();
    let quote = rest.chars().next()?;
    if quote != '\'' && quote != '"' {
        return None;
    }
    let quoted = &rest[quote.len_utf8()..];
    let end = quoted.find(quote)?;
    let path = &quoted[..end];
    if path.to_ascii_lowercase().ends_with(".lock") {
        Some(path.to_string())
    } else {
        None
    }
}
