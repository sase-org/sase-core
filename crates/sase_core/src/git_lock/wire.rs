//! Wire records for Git index-lock recovery policy.

use serde::{Deserialize, Serialize};

pub const GIT_LOCK_WIRE_SCHEMA_VERSION: u32 = 1;

pub const GIT_LOCK_FAILURE_SUCCESS: &str = "success";
pub const GIT_LOCK_FAILURE_INDEX_LOCK: &str = "index_lock";
pub const GIT_LOCK_FAILURE_OTHER_LOCK: &str = "other_lock";
pub const GIT_LOCK_FAILURE_NOT_LOCK: &str = "not_lock";

pub const GIT_LOCK_OWNER_ACTIVE: &str = "active";
pub const GIT_LOCK_OWNER_ABSENT: &str = "absent";
pub const GIT_LOCK_OWNER_UNKNOWN: &str = "unknown";

pub const GIT_LOCK_ACTION_STOP: &str = "stop";
pub const GIT_LOCK_ACTION_RETRY: &str = "retry";
pub const GIT_LOCK_ACTION_WAIT: &str = "wait";
pub const GIT_LOCK_ACTION_REMOVE_THEN_RETRY: &str = "remove_then_retry";
pub const GIT_LOCK_ACTION_RETRY_WITHOUT_REMOVAL: &str = "retry_without_removal";

pub const GIT_LOCK_FILE_REGULAR: &str = "regular";
pub const GIT_LOCK_FILE_SYMLINK: &str = "symlink";
pub const GIT_LOCK_FILE_DIRECTORY: &str = "directory";
pub const GIT_LOCK_FILE_OTHER: &str = "other";
pub const GIT_LOCK_FILE_MISSING: &str = "missing";

pub const GIT_LOCK_PROBE_COMPLETE: &str = "complete";
pub const GIT_LOCK_PROBE_INCOMPLETE: &str = "incomplete";

pub const DEFAULT_GIT_LOCK_RETRY_DELAYS: [f64; 6] =
    [0.1, 0.2, 0.4, 0.8, 1.6, 3.2];
pub const STALE_GIT_INDEX_LOCK_MIN_AGE_NS: i64 = 15_000_000_000;
pub const DEFAULT_GIT_LOCK_RECOVERY_WAIT_CAP_NS: i64 = 30_000_000_000;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GitLockFailureObservationWire {
    pub exit_status: Option<i32>,
    #[serde(default)]
    pub stdout: String,
    #[serde(default)]
    pub stderr: String,
    #[serde(default)]
    pub timed_out: bool,
    #[serde(default)]
    pub signal_terminated: bool,
    #[serde(default)]
    pub os_error: Option<String>,
    #[serde(default)]
    pub resolved_index_lock_path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitLockFailureClassWire {
    pub schema_version: u32,
    pub kind: String,
    pub retryable: bool,
    pub parsed_lock_path: Option<String>,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitLockPathMatchRequestWire {
    pub schema_version: u32,
    pub parsed_error_path: Option<String>,
    pub resolved_index_lock_path: Option<String>,
    #[serde(default)]
    pub git_context_supported: bool,
    #[serde(default)]
    pub git_context_contradictory: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitLockPathMatchWire {
    pub schema_version: u32,
    pub matched_path: Option<String>,
    pub eligible_for_removal: bool,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitLockSnapshotWire {
    pub path: String,
    pub file_type: String,
    pub device: u64,
    pub inode: u64,
    pub size: u64,
    pub mtime_ns: i64,
    pub ctime_ns: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitLockProcessFactWire {
    pub pid: u32,
    #[serde(default)]
    pub ppid: Option<u32>,
    #[serde(default)]
    pub executable: Option<String>,
    #[serde(default)]
    pub argv: Vec<String>,
    #[serde(default)]
    pub cwd: Option<String>,
    #[serde(default)]
    pub git_dir_env: Option<String>,
    #[serde(default)]
    pub git_index_file_env: Option<String>,
    #[serde(default)]
    pub git_work_tree_env: Option<String>,
    #[serde(default)]
    pub holds_lock_descriptor: bool,
    #[serde(default)]
    pub association_ambiguous: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitLockOwnerFactsWire {
    pub schema_version: u32,
    pub probe_status: String,
    pub probe_reason: String,
    #[serde(default)]
    pub processes: Vec<GitLockProcessFactWire>,
    #[serde(default)]
    pub resolved_git_dir: Option<String>,
    #[serde(default)]
    pub resolved_index_lock_path: Option<String>,
    #[serde(default)]
    pub resolved_worktree: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitLockOwnerObservationWire {
    pub schema_version: u32,
    pub status: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GitLockRecoveryRequestWire {
    pub schema_version: u32,
    pub failure: GitLockFailureClassWire,
    #[serde(default)]
    pub delays: Vec<f64>,
    #[serde(default = "default_allow_lock_removal")]
    pub allow_lock_removal: bool,
    pub attempts: u32,
    pub now_unix_ns: i64,
    #[serde(default)]
    pub remaining_command_deadline_ns: Option<i64>,
    pub remaining_recovery_budget_ns: i64,
    #[serde(default)]
    pub cancelled: bool,
    #[serde(default)]
    pub removal_already_attempted: bool,
    #[serde(default)]
    pub identity_disqualified: bool,
    #[serde(default)]
    pub first_snapshot: Option<GitLockSnapshotWire>,
    #[serde(default)]
    pub current_snapshot: Option<GitLockSnapshotWire>,
    pub path_match: GitLockPathMatchWire,
    pub owner: GitLockOwnerObservationWire,
}

fn default_allow_lock_removal() -> bool {
    true
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitLockRecoveryDecisionWire {
    pub schema_version: u32,
    pub action: String,
    pub delay_ns: Option<i64>,
    pub reason: String,
    pub lock_path: Option<String>,
    pub claim_lock_removed: bool,
    pub identity_disqualified: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitLockEagerCleanupRequestWire {
    pub schema_version: u32,
    pub now_unix_ns: i64,
    #[serde(default = "default_min_age_ns")]
    pub min_age_ns: i64,
    #[serde(default)]
    pub snapshot: Option<GitLockSnapshotWire>,
    pub owner: GitLockOwnerObservationWire,
}

fn default_min_age_ns() -> i64 {
    STALE_GIT_INDEX_LOCK_MIN_AGE_NS
}
