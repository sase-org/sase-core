use serde::{Deserialize, Deserializer, Serialize};

pub const PROC_WIRE_SCHEMA_VERSION: u32 = 2;

/// One durable background proc record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcWire {
    #[serde(alias = "task_id")]
    pub proc_id: String,
    pub label: String,
    pub kind: String,
    pub status: String,
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
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcStoreStatsWire {
    pub total_lines: u64,
    pub blank_lines: u64,
    pub invalid_json_lines: u64,
    pub invalid_record_lines: u64,
    pub loaded_rows: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcStoreSnapshotWire {
    pub schema_version: u32,
    #[serde(alias = "tasks")]
    pub procs: Vec<ProcWire>,
    pub stats: ProcStoreStatsWire,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcAppendOutcomeWire {
    pub schema_version: u32,
    pub snapshot: ProcStoreSnapshotWire,
    #[serde(alias = "pruned_task_ids")]
    pub pruned_proc_ids: Vec<String>,
}

/// Partial mutation of a proc identified by `proc_id`.
///
/// Nullable fields use a nested option so callers can distinguish an omitted
/// field (`None`) from an explicit JSON null (`Some(None)`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcUpdateWire {
    #[serde(alias = "task_id")]
    pub proc_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
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
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcUpdateOutcomeWire {
    pub schema_version: u32,
    #[serde(alias = "task")]
    pub proc: Option<ProcWire>,
    pub matched: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcPruneOutcomeWire {
    pub schema_version: u32,
    pub snapshot: ProcStoreSnapshotWire,
    #[serde(alias = "pruned_task_ids")]
    pub pruned_proc_ids: Vec<String>,
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
