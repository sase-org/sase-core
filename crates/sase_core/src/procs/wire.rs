use serde::{Deserialize, Deserializer, Serialize};

pub const PROC_WIRE_SCHEMA_VERSION: u32 = 3;
pub const SUPPORTED_PROC_WIRE_SCHEMA_VERSIONS: [u32; 3] =
    [1, 2, PROC_WIRE_SCHEMA_VERSION];

/// One durable background proc record.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ProcWire {
    #[serde(default = "legacy_proc_row_schema_version")]
    pub schema_version: u32,
    #[serde(alias = "task_id")]
    pub proc_id: String,
    pub label: String,
    pub kind: String,
    pub status: String,
    #[serde(default)]
    pub lifecycle: String,
    #[serde(default)]
    pub argv: Vec<String>,
    pub command: Vec<String>,
    pub cwd: String,
    pub project: Option<String>,
    pub workspace_num: Option<u32>,
    pub session_id: Option<String>,
    pub session_label: Option<String>,
    pub origin: String,
    pub cl_name: Option<String>,
    pub tags: Vec<String>,
    pub pid: Option<u32>,
    pub pgid: Option<u32>,
    pub exit_code: Option<i32>,
    pub phase: Option<String>,
    pub message: Option<String>,
    pub created_at: String,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
    pub log_path: String,
    #[serde(default = "default_log_owner")]
    pub log_owner: String,
    #[serde(default)]
    pub shell_name: Option<String>,
    #[serde(default)]
    pub shell_kind: Option<String>,
    #[serde(default)]
    pub concurrency_keys: Vec<String>,
    #[serde(default)]
    pub request_fingerprint: Option<String>,
    #[serde(default)]
    pub reserved_by: Option<String>,
    #[serde(default)]
    pub reserved_at: Option<String>,
    #[serde(default)]
    pub supervisor_id: Option<String>,
    #[serde(default)]
    pub supervisor_claimed_at: Option<String>,
    #[serde(default)]
    pub stop_requested_by: Option<String>,
    #[serde(default)]
    pub stop_requested_at: Option<String>,
    #[serde(default)]
    pub stop_reason: Option<String>,
    #[serde(default)]
    pub timeout_seconds: Option<u64>,
    #[serde(default)]
    pub idle_timeout_seconds: Option<u64>,
    #[serde(default)]
    pub settling_started_at: Option<String>,
    #[serde(default)]
    pub settled_by: Option<String>,
    #[serde(default)]
    pub settled_at: Option<String>,
    #[serde(default)]
    pub finished_by: Option<String>,
    #[serde(default)]
    pub result: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcStoreStatsWire {
    pub total_lines: u64,
    pub blank_lines: u64,
    pub invalid_json_lines: u64,
    pub invalid_record_lines: u64,
    pub loaded_rows: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ProcStoreSnapshotWire {
    pub schema_version: u32,
    #[serde(alias = "tasks")]
    pub procs: Vec<ProcWire>,
    pub stats: ProcStoreStatsWire,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ProcAppendOutcomeWire {
    pub schema_version: u32,
    pub snapshot: ProcStoreSnapshotWire,
    #[serde(alias = "pruned_task_ids")]
    pub pruned_proc_ids: Vec<String>,
    #[serde(default)]
    pub pruned_log_proc_ids: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ProcReserveWire {
    #[serde(default = "current_proc_row_schema_version")]
    pub schema_version: u32,
    pub proc_id: String,
    pub label: String,
    #[serde(default = "default_proc_kind")]
    pub kind: String,
    pub argv: Vec<String>,
    pub cwd: String,
    pub project: Option<String>,
    pub workspace_num: Option<u32>,
    pub session_id: Option<String>,
    pub session_label: Option<String>,
    #[serde(default = "default_origin")]
    pub origin: String,
    pub cl_name: Option<String>,
    #[serde(default)]
    pub tags: Vec<String>,
    pub created_at: String,
    pub log_path: String,
    #[serde(default = "default_log_owner")]
    pub log_owner: String,
    pub shell_name: Option<String>,
    #[serde(default = "default_shell_kind")]
    pub shell_kind: Option<String>,
    #[serde(default)]
    pub concurrency_keys: Vec<String>,
    pub request_fingerprint: String,
    pub reserved_by: String,
    pub timeout_seconds: Option<u64>,
    pub idle_timeout_seconds: Option<u64>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ProcReserveOutcomeWire {
    pub schema_version: u32,
    pub proc: ProcWire,
    pub snapshot: ProcStoreSnapshotWire,
    pub reserved: bool,
    pub replayed: bool,
    pub pruned_proc_ids: Vec<String>,
    pub pruned_log_proc_ids: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcSupervisorClaimWire {
    pub proc_id: String,
    pub supervisor_id: String,
    pub claimed_at: String,
    pub pid: Option<u32>,
    pub pgid: Option<u32>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcStopRequestWire {
    pub proc_id: String,
    pub requested_by: String,
    pub requested_at: String,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcSettlementWire {
    pub proc_id: String,
    pub supervisor_id: String,
    pub settling_at: String,
    pub exit_code: Option<i32>,
    pub message: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ProcFinishWire {
    pub proc_id: String,
    pub supervisor_id: String,
    pub status: String,
    pub finished_at: String,
    pub exit_code: Option<i32>,
    pub message: Option<String>,
    pub result: Option<serde_json::Value>,
}

/// Partial mutation of a proc identified by `proc_id`.
///
/// Nullable fields use a nested option so callers can distinguish an omitted
/// field (`None`) from an explicit JSON null (`Some(None)`).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ProcUpdateWire {
    #[serde(alias = "task_id")]
    pub proc_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_version: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lifecycle: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub argv: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    #[serde(
        default,
        deserialize_with = "deserialize_present_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub project: Option<Option<String>>,
    #[serde(
        default,
        deserialize_with = "deserialize_present_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub workspace_num: Option<Option<u32>>,
    #[serde(
        default,
        deserialize_with = "deserialize_present_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub session_id: Option<Option<String>>,
    #[serde(
        default,
        deserialize_with = "deserialize_present_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub session_label: Option<Option<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin: Option<String>,
    #[serde(
        default,
        deserialize_with = "deserialize_present_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub cl_name: Option<Option<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tags: Option<Vec<String>>,
    #[serde(
        default,
        deserialize_with = "deserialize_present_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub pid: Option<Option<u32>>,
    #[serde(
        default,
        deserialize_with = "deserialize_present_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub pgid: Option<Option<u32>>,
    #[serde(
        default,
        deserialize_with = "deserialize_present_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub exit_code: Option<Option<i32>>,
    #[serde(
        default,
        deserialize_with = "deserialize_present_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub phase: Option<Option<String>>,
    #[serde(
        default,
        deserialize_with = "deserialize_present_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub message: Option<Option<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,
    #[serde(
        default,
        deserialize_with = "deserialize_present_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub started_at: Option<Option<String>>,
    #[serde(
        default,
        deserialize_with = "deserialize_present_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub finished_at: Option<Option<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub log_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub log_owner: Option<String>,
    #[serde(
        default,
        deserialize_with = "deserialize_present_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub shell_name: Option<Option<String>>,
    #[serde(
        default,
        deserialize_with = "deserialize_present_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub shell_kind: Option<Option<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub concurrency_keys: Option<Vec<String>>,
    #[serde(
        default,
        deserialize_with = "deserialize_present_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub request_fingerprint: Option<Option<String>>,
    #[serde(
        default,
        deserialize_with = "deserialize_present_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub reserved_by: Option<Option<String>>,
    #[serde(
        default,
        deserialize_with = "deserialize_present_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub reserved_at: Option<Option<String>>,
    #[serde(
        default,
        deserialize_with = "deserialize_present_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub supervisor_id: Option<Option<String>>,
    #[serde(
        default,
        deserialize_with = "deserialize_present_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub supervisor_claimed_at: Option<Option<String>>,
    #[serde(
        default,
        deserialize_with = "deserialize_present_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub stop_requested_by: Option<Option<String>>,
    #[serde(
        default,
        deserialize_with = "deserialize_present_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub stop_requested_at: Option<Option<String>>,
    #[serde(
        default,
        deserialize_with = "deserialize_present_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub stop_reason: Option<Option<String>>,
    #[serde(
        default,
        deserialize_with = "deserialize_present_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub timeout_seconds: Option<Option<u64>>,
    #[serde(
        default,
        deserialize_with = "deserialize_present_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub idle_timeout_seconds: Option<Option<u64>>,
    #[serde(
        default,
        deserialize_with = "deserialize_present_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub settling_started_at: Option<Option<String>>,
    #[serde(
        default,
        deserialize_with = "deserialize_present_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub settled_by: Option<Option<String>>,
    #[serde(
        default,
        deserialize_with = "deserialize_present_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub settled_at: Option<Option<String>>,
    #[serde(
        default,
        deserialize_with = "deserialize_present_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub finished_by: Option<Option<String>>,
    #[serde(
        default,
        deserialize_with = "deserialize_present_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub result: Option<Option<serde_json::Value>>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ProcUpdateOutcomeWire {
    pub schema_version: u32,
    #[serde(alias = "task")]
    pub proc: Option<ProcWire>,
    pub matched: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ProcPruneOutcomeWire {
    pub schema_version: u32,
    pub snapshot: ProcStoreSnapshotWire,
    #[serde(alias = "pruned_task_ids")]
    pub pruned_proc_ids: Vec<String>,
    #[serde(default)]
    pub pruned_log_proc_ids: Vec<String>,
}

fn legacy_proc_row_schema_version() -> u32 {
    2
}

fn current_proc_row_schema_version() -> u32 {
    PROC_WIRE_SCHEMA_VERSION
}

fn default_proc_kind() -> String {
    "command".to_string()
}

fn default_origin() -> String {
    "proc-shell".to_string()
}

fn default_log_owner() -> String {
    "proc-store".to_string()
}

fn default_shell_kind() -> Option<String> {
    Some("proc".to_string())
}

fn deserialize_present_option<'de, D, T>(
    deserializer: D,
) -> Result<Option<Option<T>>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer).map(Some)
}
