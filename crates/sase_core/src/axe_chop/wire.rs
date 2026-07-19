//! Serde-friendly wire records for the chop engine.

use std::collections::BTreeMap;

use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use thiserror::Error;

/// Version for chop results written by scripts.
pub const CHOP_RESULT_SCHEMA_VERSION: u32 = 1;
/// Version for JSON-in/JSON-out engine requests and responses.
pub const CHOP_ENGINE_SCHEMA_VERSION: u32 = 1;
/// Version for checkpoint and once-per state documents.
pub const CHOP_STATE_SCHEMA_VERSION: u32 = 1;

/// An actionable validation failure.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
#[error("{code} at {path}: {message}")]
pub struct ChopEngineError {
    pub code: String,
    pub path: String,
    pub message: String,
}

impl ChopEngineError {
    pub(crate) fn new(
        code: impl Into<String>,
        path: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            code: code.into(),
            path: path.into(),
            message: message.into(),
        }
    }
}

/// Script-level health outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChopResultStatusWire {
    Ok,
    NoOp,
    CheckError,
}

/// A reference to an earlier proposal in the same result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ProposalWaitOnWire {
    Index(usize),
    Id(String),
}

/// One runner-executed agent launch proposed by a chop script.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChopLaunchProposalWire {
    #[serde(default)]
    pub id: Option<String>,
    pub prompt: String,
    #[serde(alias = "workspace_ref")]
    pub workspace: String,
    #[serde(default)]
    pub agent_name: Option<String>,
    #[serde(default)]
    pub tribe: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub effort: Option<String>,
    #[serde(default, alias = "extra_env")]
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub dedupe_key: Option<String>,
    #[serde(default)]
    pub wait_on: Option<ProposalWaitOnWire>,
}

/// Versioned document written to `SASE_CHOP_RESULT_FILE`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChopResultDocumentWire {
    pub schema_version: u32,
    pub status: ChopResultStatusWire,
    #[serde(default)]
    pub summary: Option<String>,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub counters: BTreeMap<String, i64>,
    #[serde(default)]
    pub evidence: Vec<String>,
    #[serde(default)]
    pub proposed_launches: Vec<ChopLaunchProposalWire>,
}

/// Guard providers supported by v1.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "provider", deny_unknown_fields)]
pub enum ChopGuardConfigWire {
    #[serde(rename = "changespec")]
    Changespec {
        #[serde(default)]
        name_prefix: String,
        #[serde(default)]
        statuses: Vec<String>,
    },
    #[serde(rename = "agent_hood")]
    AgentHood {
        #[serde(alias = "name")]
        hood: String,
    },
}

/// Runner-owned checkpoint advancement policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChopCheckpointPolicyWire {
    OnObservation,
    OnActionAccepted,
    OnActionSuccess,
}

/// Trigger providers supported by v1.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "provider", deny_unknown_fields)]
pub enum ChopTriggerConfigWire {
    #[serde(rename = "always")]
    #[default]
    Always,
    #[serde(rename = "git.commits_since")]
    GitCommitsSince {
        project: String,
        threshold: u64,
        #[serde(default = "default_checkpoint_policy")]
        checkpoint_policy: ChopCheckpointPolicyWire,
    },
}

fn default_checkpoint_policy() -> ChopCheckpointPolicyWire {
    ChopCheckpointPolicyWire::OnObservation
}

/// One ChangeSpec row supplied by the Python host.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChopChangespecSnapshotWire {
    pub name: String,
    pub status: String,
}

/// One agent row supplied by the Python host.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChopAgentSnapshotWire {
    pub name: String,
    #[serde(default)]
    pub hood: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub active: bool,
}

/// Git observation computed by the host for one project.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChopGitSnapshotWire {
    pub project: String,
    pub head: String,
    pub commits_since_checkpoint: u64,
    #[serde(default)]
    pub checkpoint_found: bool,
}

/// Request for guard + trigger evaluation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChopDecisionRequestWire {
    pub schema_version: u32,
    #[serde(default)]
    pub inhibit_if: Vec<ChopGuardConfigWire>,
    #[serde(default)]
    pub trigger: ChopTriggerConfigWire,
    #[serde(default)]
    pub changespecs: Vec<ChopChangespecSnapshotWire>,
    #[serde(default)]
    pub agents: Vec<ChopAgentSnapshotWire>,
    #[serde(default)]
    pub git: Vec<ChopGitSnapshotWire>,
    #[serde(default)]
    pub checkpoint: ChopCheckpointDocumentWire,
    pub now: String,
}

/// Result of guard + trigger evaluation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChopDecisionWire {
    pub schema_version: u32,
    /// `fire`, `skip`, or `check_error`.
    pub outcome: String,
    pub reason: String,
    #[serde(default)]
    pub provider: Option<String>,
    #[serde(default)]
    pub checkpoint_key: Option<String>,
    #[serde(default)]
    pub checkpoint_cursor: Option<String>,
    #[serde(default)]
    pub checkpoint_policy: Option<ChopCheckpointPolicyWire>,
}

/// One committed or pending checkpoint cursor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChopCheckpointEntryWire {
    pub cursor: String,
    pub updated_at: String,
    #[serde(default)]
    pub pending_cursor: Option<String>,
    #[serde(default)]
    pub pending_at: Option<String>,
}

/// Runner-owned per-chop checkpoint document.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChopCheckpointDocumentWire {
    pub schema_version: u32,
    #[serde(default)]
    pub entries: BTreeMap<String, ChopCheckpointEntryWire>,
}

impl Default for ChopCheckpointDocumentWire {
    fn default() -> Self {
        Self {
            schema_version: CHOP_STATE_SCHEMA_VERSION,
            entries: BTreeMap::new(),
        }
    }
}

/// Lifecycle event offered to the checkpoint transform.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChopCheckpointEventWire {
    Observed,
    ActionAccepted,
    ActionSucceeded,
    ActionFailed,
}

/// Request to transform one checkpoint document.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChopCheckpointUpdateRequestWire {
    pub schema_version: u32,
    #[serde(default)]
    pub document: ChopCheckpointDocumentWire,
    pub key: String,
    pub cursor: String,
    pub now: String,
    pub policy: ChopCheckpointPolicyWire,
    pub event: ChopCheckpointEventWire,
}

/// One bounded once-per event key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChopSeenEntryWire {
    pub key: String,
    pub seen_at: String,
}

/// Runner-owned bounded once-per store.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChopSeenStoreDocumentWire {
    pub schema_version: u32,
    #[serde(default)]
    pub entries: Vec<ChopSeenEntryWire>,
}

impl Default for ChopSeenStoreDocumentWire {
    fn default() -> Self {
        Self {
            schema_version: CHOP_STATE_SCHEMA_VERSION,
            entries: Vec::new(),
        }
    }
}

/// Request to test and possibly record one once-per key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChopOncePerRequestWire {
    pub schema_version: u32,
    #[serde(default)]
    pub document: ChopSeenStoreDocumentWire,
    pub key: String,
    pub now: String,
    #[serde(default = "default_seen_capacity")]
    pub capacity: usize,
}

fn default_seen_capacity() -> usize {
    1024
}

/// Request to release exact keys from the once-per store.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChopOncePerReleaseRequestWire {
    pub schema_version: u32,
    #[serde(default)]
    pub document: ChopSeenStoreDocumentWire,
    pub keys: Vec<String>,
}

/// Once-per release result plus the transformed store.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChopOncePerReleaseWire {
    pub document: ChopSeenStoreDocumentWire,
    pub released: usize,
}

/// Declarative per-proposal event-dedupe configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChopOncePerConfigWire {
    pub key: String,
    #[serde(default = "default_seen_capacity")]
    pub capacity: usize,
}

/// Once-per decision plus the transformed store.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChopOncePerDecisionWire {
    pub schema_version: u32,
    /// `accept` or `duplicate`.
    pub outcome: String,
    pub reason: String,
    pub document: ChopSeenStoreDocumentWire,
}

/// Filters for a host-provided target source.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct ChopTargetSourceFiltersWire {
    #[serde(default)]
    pub names: Vec<String>,
    #[serde(default)]
    pub vcs: Vec<String>,
}

impl<'de> Deserialize<'de> for ChopTargetSourceFiltersWire {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        let Value::Object(mut map) = value else {
            return Err(serde::de::Error::custom(
                "target source filters must be an object",
            ));
        };
        let names = map
            .remove("names")
            .or_else(|| map.remove("name"))
            .map(|value| string_or_list(value, "name"))
            .transpose()
            .map_err(serde::de::Error::custom)?
            .unwrap_or_default();
        let vcs = map
            .remove("vcs")
            .map(|value| string_or_list(value, "vcs"))
            .transpose()
            .map_err(serde::de::Error::custom)?
            .unwrap_or_default();
        if let Some(key) = map.keys().next() {
            return Err(serde::de::Error::custom(format!(
                "unknown target source filter `{key}`"
            )));
        }
        Ok(Self { names, vcs })
    }
}

fn string_or_list(value: Value, label: &str) -> Result<Vec<String>, String> {
    match value {
        Value::String(item) if !item.trim().is_empty() => Ok(vec![item]),
        Value::Array(items) => items
            .into_iter()
            .enumerate()
            .map(|(index, item)| {
                item.as_str()
                    .filter(|text| !text.trim().is_empty())
                    .map(str::to_string)
                    .ok_or_else(|| {
                        format!(
                            "{label} filter at index {index} must be a non-blank string"
                        )
                    })
            })
            .collect(),
        _ => Err(format!(
            "{label} filter must be a non-blank string or string array"
        )),
    }
}

/// Normalized for-each configuration.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ChopForEachConfigWire {
    Literal {
        targets: Vec<BTreeMap<String, Value>>,
    },
    Source {
        source: String,
        #[serde(default)]
        filters: ChopTargetSourceFiltersWire,
    },
}

impl<'de> Deserialize<'de> for ChopForEachConfigWire {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        parse_for_each_value(value).map_err(serde::de::Error::custom)
    }
}

fn parse_for_each_value(value: Value) -> Result<ChopForEachConfigWire, String> {
    if let Value::Array(rows) = value {
        return Ok(ChopForEachConfigWire::Literal {
            targets: rows_to_target_maps(rows)?,
        });
    }
    let Value::Object(mut map) = value else {
        return Err("for_each must be an array or object".to_string());
    };
    if map.get("kind").and_then(Value::as_str) == Some("literal")
        || map.contains_key("targets")
    {
        let rows = map
            .remove("targets")
            .ok_or_else(|| "literal for_each requires targets".to_string())?;
        let Value::Array(rows) = rows else {
            return Err("for_each.targets must be an array".to_string());
        };
        let allowed = ["kind"];
        if let Some(key) =
            map.keys().find(|key| !allowed.contains(&key.as_str()))
        {
            return Err(format!("unknown literal for_each key `{key}`"));
        }
        return Ok(ChopForEachConfigWire::Literal {
            targets: rows_to_target_maps(rows)?,
        });
    }
    let source = map
        .remove("source")
        .and_then(|item| item.as_str().map(str::to_string))
        .ok_or_else(|| {
            "source for_each requires a string source".to_string()
        })?;
    map.remove("kind");
    let filters = if let Some(filters) = map.remove("filters") {
        serde_json::from_value(filters)
            .map_err(|error| format!("invalid for_each filters: {error}"))?
    } else {
        let names = map
            .remove("names")
            .or_else(|| map.remove("name"))
            .unwrap_or(Value::Array(Vec::new()));
        let vcs = map.remove("vcs").unwrap_or(Value::Array(Vec::new()));
        serde_json::from_value(serde_json::json!({"names": names, "vcs": vcs}))
            .map_err(|error| format!("invalid for_each filters: {error}"))?
    };
    if let Some(key) = map.keys().next() {
        return Err(format!("unknown source for_each key `{key}`"));
    }
    Ok(ChopForEachConfigWire::Source { source, filters })
}

fn rows_to_target_maps(
    rows: Vec<Value>,
) -> Result<Vec<BTreeMap<String, Value>>, String> {
    rows.into_iter()
        .enumerate()
        .map(|(index, row)| {
            serde_json::from_value(row).map_err(|_| {
                format!("for_each target at index {index} must be an object")
            })
        })
        .collect()
}

/// Target expansion request. Source rows are discovered by the host.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChopTargetExpansionRequestWire {
    pub schema_version: u32,
    pub chop_name: String,
    #[serde(default)]
    pub for_each: Option<ChopForEachConfigWire>,
    #[serde(default)]
    pub source_rows: Vec<BTreeMap<String, Value>>,
}

/// One stable per-target chop instance.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChopTargetInstanceWire {
    pub instance_id: String,
    pub target_key: String,
    #[serde(default)]
    pub target: BTreeMap<String, Value>,
    #[serde(default)]
    pub overrides: BTreeMap<String, Value>,
}

/// Expanded target instances.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChopTargetExpansionWire {
    pub schema_version: u32,
    pub instances: Vec<ChopTargetInstanceWire>,
}

/// Strict axe config validation request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AxeConfigValidationRequestWire {
    pub schema_version: u32,
    pub config: Value,
    /// Dotted config path to source/provenance label.
    #[serde(default)]
    pub provenance: BTreeMap<String, String>,
}
