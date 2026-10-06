//! Instruction manifest v1 wire types and closed vocabulary.
//!
//! Names are exact per the E2 plan decision 8. Every struct uses
//! `deny_unknown_fields`; readers accept `schema_version` in
//! `1..=current`.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Wire schema version for the instruction manifest contract.
pub const INSTRUCTION_MANIFEST_WIRE_SCHEMA_VERSION: u32 = 1;

/// Return the instruction manifest wire schema version.
pub fn instruction_manifest_wire_schema_version() -> u32 {
    INSTRUCTION_MANIFEST_WIRE_SCHEMA_VERSION
}

/// Bundle layer vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LayerWire {
    Frame,
    Package,
    Plugin,
    Home,
    Project,
    Launch,
}

/// Section inclusion status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SectionStatusWire {
    Included,
    Excluded,
}

/// Exclusion reason vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SectionReasonWire {
    SupersededInput,
    Shadowed,
    Overlay,
    Mode,
    NoDirective,
    Empty,
}

/// Section lifecycle overlay.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleWire {
    Neutral,
    Root,
    Helper,
}

/// Source scope vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceScopeWire {
    Package,
    Plugin,
    Home,
    Project,
    Launch,
}

/// Source kind vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceKindWire {
    PackageTemplate,
    HelperTemplate,
    ProviderDirective,
    MemoryNote,
    MemoryWeb,
    MemoryStrands,
    Config,
    Generated,
    LegacyFallback,
}

/// Fact actor vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActorWire {
    SaseRoot,
    NativeHelper,
    Interactive,
}

/// Fact mode vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModeWire {
    Runtime,
    Interactive,
    Export,
}

/// Fact purpose vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PurposeWire {
    Ordinary,
    DeclarationRecovery,
    ConflictRepair,
}

/// Delivery status vocabulary (E3 values frozen now, unused in E2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryStatusWire {
    Preview,
    Shadow,
    Explicit,
    InheritsNative,
    InheritsRoot,
    None,
}

/// Render-cache outcome vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CacheWire {
    Hit,
    Miss,
    Bypass,
}

/// Observation status vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObservationStatusWire {
    Unobserved,
    Observed,
    Partial,
    Unavailable,
}

/// Compiler identity block.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompilerWire {
    pub name: String,
    pub version: u32,
    pub sase_version: String,
}

/// Host-set facts block. Project content cannot set these.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FactsWire {
    pub actor: ActorWire,
    pub mode: ModeWire,
    pub purpose: PurposeWire,
    pub provider: String,
    pub project: Option<String>,
    pub host: String,
    pub vcs: Option<String>,
}

/// Bundle identity block. `common_digest` is filled by normalize when null.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BundleWire {
    pub sha256: String,
    pub common_digest: Option<String>,
    pub bytes: u64,
    pub lines: u64,
    pub tokens_est: u64,
    pub store_path: String,
}

/// Per-layer budget entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LayerBudgetWire {
    pub bytes: u64,
    pub tokens_est: u64,
}

/// Budget block keyed by layer name.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BudgetWire {
    pub by_layer: BTreeMap<String, LayerBudgetWire>,
}

/// One source that fed a section.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceWire {
    pub scope: SourceScopeWire,
    pub kind: SourceKindWire,
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blob_oid: Option<String>,
}

/// One bundle section, included or excluded with a reason.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SectionWire {
    pub id: String,
    pub layer: LayerWire,
    pub status: SectionStatusWire,
    pub lifecycle: LifecycleWire,
    pub required: bool,
    pub provider_specific: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub length: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
    pub tokens_est: u64,
    #[serde(default)]
    pub sources: Vec<SourceWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<SectionReasonWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shadowed_by: Option<String>,
}

/// Delivery identity block.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeliveryWire {
    pub status: DeliveryStatusWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
    pub invocation_id: String,
    pub invocation_seq: u64,
    pub attempt: u64,
    pub agent_name: String,
    pub agent_type: String,
    pub model: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_invocation_id: Option<String>,
    #[serde(default)]
    pub session_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_cli_version: Option<String>,
    pub rendered_at: String,
    pub render_ms: f64,
    pub cache: CacheWire,
}

/// Observation block.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObservationWire {
    pub status: ObservationStatusWire,
}

/// Top-level instruction manifest wire.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstructionManifestWire {
    pub schema_version: u32,
    pub compiler: CompilerWire,
    pub facts: FactsWire,
    pub bundle: BundleWire,
    pub budget: BudgetWire,
    pub sections: Vec<SectionWire>,
    pub delivery: DeliveryWire,
    pub observation: ObservationWire,
}
