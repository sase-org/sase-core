//! Match an error-reported lock path against the independently resolved index.

use super::delays::GitLockError;
use super::wire::{
    GitLockPathMatchRequestWire, GitLockPathMatchWire,
    GIT_LOCK_WIRE_SCHEMA_VERSION,
};

/// Decide whether a parsed error path may authorize deletion of the resolved
/// index lock.
pub fn match_git_lock_path(
    request: &GitLockPathMatchRequestWire,
) -> Result<GitLockPathMatchWire, GitLockError> {
    if request.schema_version != GIT_LOCK_WIRE_SCHEMA_VERSION {
        return Err(GitLockError::Schema {
            expected: GIT_LOCK_WIRE_SCHEMA_VERSION,
            actual: request.schema_version,
        });
    }
    if !request.git_context_supported {
        return Ok(unmatched(
            "unsupported Git directory or index-file context",
        ));
    }
    if request.git_context_contradictory {
        return Ok(unmatched(
            "GIT_DIR or GIT_INDEX_FILE contradicts the resolved index",
        ));
    }
    let Some(resolved) = request
        .resolved_index_lock_path
        .as_deref()
        .map(str::trim)
        .filter(|path| !path.is_empty())
    else {
        return Ok(unmatched("could not independently resolve the index lock"));
    };
    match request.parsed_error_path.as_deref().map(str::trim) {
        Some(parsed) if !parsed.is_empty() => {
            if parsed == resolved {
                Ok(matched(resolved, "error path matches resolved index lock"))
            } else {
                Ok(unmatched(
                    "error path does not match the independently resolved index lock",
                ))
            }
        }
        _ => Ok(matched(
            resolved,
            "no explicit error path; using independently resolved index lock",
        )),
    }
}

fn matched(path: &str, reason: &str) -> GitLockPathMatchWire {
    GitLockPathMatchWire {
        schema_version: GIT_LOCK_WIRE_SCHEMA_VERSION,
        matched_path: Some(path.to_string()),
        eligible_for_removal: true,
        reason: reason.to_string(),
    }
}

fn unmatched(reason: &str) -> GitLockPathMatchWire {
    GitLockPathMatchWire {
        schema_version: GIT_LOCK_WIRE_SCHEMA_VERSION,
        matched_path: None,
        eligible_for_removal: false,
        reason: reason.to_string(),
    }
}
