//! Wire records for deterministic episodic-memory episodes.
//!
//! These structs are the stable Rust/Python boundary for the episode MVP.
//! They intentionally model source-grounded episode data only; collection,
//! rendering, storage, and CLI behavior live in later phases.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

pub const EPISODE_WIRE_SCHEMA_VERSION: u32 = 2;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EpisodeSourceRefWire {
    pub id: String,
    pub kind: String,
    pub path: String,
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default)]
    pub exists: bool,
    #[serde(default)]
    pub size_bytes: Option<u64>,
    #[serde(default)]
    pub sha256: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EpisodeNodeWire {
    pub id: String,
    pub kind: String,
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default)]
    pub source_id: Option<String>,
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EpisodeEdgeWire {
    pub id: String,
    pub from_node_id: String,
    pub to_node_id: String,
    pub kind: String,
    #[serde(default)]
    pub evidence_ids: Vec<String>,
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EpisodeEventWire {
    pub id: String,
    pub kind: String,
    pub title: String,
    #[serde(default)]
    pub timestamp: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub evidence_ids: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EpisodeLessonWire {
    pub id: String,
    pub kind: String,
    pub text: String,
    #[serde(default)]
    pub evidence_ids: Vec<String>,
    #[serde(default = "default_source_confidence")]
    pub source_confidence: String,
}

fn default_source_confidence() -> String {
    "deterministic".to_string()
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EpisodeImportanceFactorWire {
    pub kind: String,
    pub label: String,
    #[serde(default)]
    pub score: u32,
    #[serde(default)]
    pub evidence_ids: Vec<String>,
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EpisodeSafetyWire {
    #[serde(default)]
    pub untrusted_transcript_text: bool,
    #[serde(default)]
    pub prompt_injection_phrase_hits: Vec<String>,
    #[serde(default)]
    pub redaction_hits: Vec<String>,
    #[serde(default)]
    pub private_or_missing_source_flags: Vec<String>,
    #[serde(default)]
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EpisodeWeakRefsWire {
    #[serde(default)]
    pub changespec_names: Vec<String>,
    #[serde(default)]
    pub bead_ids: Vec<String>,
    #[serde(default)]
    pub agent_families: Vec<String>,
    #[serde(default)]
    pub touched_paths: Vec<String>,
    #[serde(default)]
    pub metadata: BTreeMap<String, Vec<String>>,
}

fn default_episode_status() -> String {
    "legacy".to_string()
}

fn default_importance_band() -> String {
    "unknown".to_string()
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EpisodeWire {
    pub schema_version: u32,
    pub episode_id: String,
    pub project: String,
    pub title: String,
    pub summary: String,
    pub root_source_id: String,
    #[serde(default)]
    pub component_key: String,
    #[serde(default)]
    pub component_root_kind: String,
    #[serde(default = "default_episode_status")]
    pub status: String,
    #[serde(default)]
    pub importance_score: u32,
    #[serde(default = "default_importance_band")]
    pub importance_band: String,
    #[serde(default)]
    pub importance_factors: Vec<EpisodeImportanceFactorWire>,
    #[serde(default)]
    pub safety: EpisodeSafetyWire,
    #[serde(default)]
    pub weak_refs: EpisodeWeakRefsWire,
    #[serde(default)]
    pub sources: Vec<EpisodeSourceRefWire>,
    #[serde(default)]
    pub nodes: Vec<EpisodeNodeWire>,
    #[serde(default)]
    pub edges: Vec<EpisodeEdgeWire>,
    #[serde(default)]
    pub events: Vec<EpisodeEventWire>,
    #[serde(default)]
    pub lessons: Vec<EpisodeLessonWire>,
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EpisodeBuildRequestWire {
    pub schema_version: u32,
    pub project: String,
    #[serde(default)]
    pub selector_kind: Option<String>,
    #[serde(default)]
    pub selector_value: Option<String>,
    #[serde(default)]
    pub since: Option<String>,
    #[serde(default)]
    pub until: Option<String>,
    #[serde(default)]
    pub limit: Option<u32>,
    #[serde(default)]
    pub dry_run: bool,
    #[serde(default)]
    pub force: bool,
    #[serde(default)]
    pub source_refs: Vec<EpisodeSourceRefWire>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EpisodeBuildReportWire {
    pub schema_version: u32,
    pub project: String,
    pub source_count: u64,
    pub lesson_count: u64,
    #[serde(default)]
    pub episode_id: Option<String>,
    #[serde(default)]
    pub would_write: bool,
    #[serde(default)]
    pub changed: bool,
    #[serde(default)]
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EpisodeStorageIndexRowWire {
    pub schema_version: u32,
    pub episode_id: String,
    pub project: String,
    pub title: String,
    #[serde(default)]
    pub component_key: String,
    #[serde(default = "default_episode_status")]
    pub status: String,
    #[serde(default)]
    pub summary_excerpt: String,
    #[serde(default)]
    pub source_count: u64,
    #[serde(default)]
    pub chat_count: u64,
    #[serde(default)]
    pub agent_count: u64,
    #[serde(default)]
    pub importance_score: u32,
    #[serde(default = "default_importance_band")]
    pub importance_band: String,
    #[serde(default)]
    pub lesson_path: String,
    #[serde(default)]
    pub legacy_lesson_path: Option<String>,
    pub content_sha256: String,
    #[serde(default)]
    pub root_agent_names: Vec<String>,
    #[serde(default)]
    pub changespec_name: Option<String>,
    #[serde(default)]
    pub bead_ids: Vec<String>,
    #[serde(default)]
    pub outcome: Option<String>,
    #[serde(default)]
    pub first_event_at: Option<String>,
    #[serde(default)]
    pub last_event_at: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EpisodeSourceVerifyResultWire {
    pub source_id: String,
    pub path: String,
    pub expected_exists: bool,
    pub actual_exists: bool,
    pub status: String,
    #[serde(default)]
    pub expected_size_bytes: Option<u64>,
    #[serde(default)]
    pub actual_size_bytes: Option<u64>,
    #[serde(default)]
    pub expected_sha256: Option<String>,
    #[serde(default)]
    pub actual_sha256: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EpisodeVerifyReportWire {
    pub schema_version: u32,
    pub episode_id: String,
    pub ok: bool,
    pub source_count: u64,
    pub ok_count: u64,
    pub missing_count: u64,
    pub changed_count: u64,
    #[serde(default)]
    pub results: Vec<EpisodeSourceVerifyResultWire>,
}
