//! Serde wires for the stranded-work reducer.
//!
//! Field shapes mirror the Python collector facts: every field is always
//! present on the way out, `Option` is explicit, and unknown fields are
//! rejected so wire drift fails loudly.

use crate::bead::wire::BeadCompletionFailureWire;
use serde::{Deserialize, Serialize};

/// Wire schema version for [`StrandedWorkInputWire`]/[`StrandedWorkReportWire`].
pub const STRANDED_WORK_WIRE_SCHEMA_VERSION: u32 = 1;

/// Verdict states, one per candidate bead.
pub const STRANDED_STATE_OWNED: &str = "owned";
pub const STRANDED_STATE_DELEGATED: &str = "delegated";
pub const STRANDED_STATE_SETTLING: &str = "settling";
pub const STRANDED_STATE_STRANDED: &str = "stranded";
pub const STRANDED_STATE_BLOCKED: &str = "blocked";
pub const STRANDED_STATE_UNKNOWN: &str = "unknown";

/// Stranded cause codes, in precedence order (first match wins).
pub const STRANDED_CAUSE_FOLLOWUP_FAILED: &str = "followup_failed";
pub const STRANDED_CAUSE_FAILED: &str = "failed";
pub const STRANDED_CAUSE_STOPPED: &str = "stopped";
pub const STRANDED_CAUSE_LOST: &str = "lost";
pub const STRANDED_CAUSE_CHILD_LANDED: &str = "child_landed";
pub const STRANDED_CAUSE_CLOSE_FAILED: &str = "close_failed";
pub const STRANDED_CAUSE_KEPT: &str = "kept";
pub const STRANDED_CAUSE_UNCLOSED: &str = "unclosed";

/// Unknown reasons: no session records were found at all.
pub const STRANDED_UNKNOWN_NO_RECORDS: &str = "no_records";
/// Unknown reasons: the collector could not read this session's records.
pub const STRANDED_UNKNOWN_RECORDS_UNAVAILABLE: &str = "records_unavailable";

fn default_schema_version() -> u32 {
    STRANDED_WORK_WIRE_SCHEMA_VERSION
}

fn default_settle_grace_seconds() -> f64 {
    120.0
}

/// Facts about one work bead plus non-candidate context beads.
///
/// Non-candidate beads (closed or open child plans) carry no verdict of
/// their own; they are context for delegation and `child_landed`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StrandedWorkBeadFactsWire {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub kind: String,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub candidate: bool,
    #[serde(default)]
    pub assignee: Option<String>,
    #[serde(default)]
    pub parent_id: Option<String>,
    #[serde(default)]
    pub closed_at: Option<f64>,
    #[serde(default)]
    pub records_available: bool,
    #[serde(default)]
    pub members: Vec<StrandedWorkMemberFactsWire>,
    /// Explicit recovery evidence survives loss of the agent artifact index.
    #[serde(default)]
    pub completion_failure: Option<BeadCompletionFailureWire>,
}

/// Facts about one member of the bead's owning agent session.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StrandedWorkMemberFactsWire {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub artifact_dir: String,
    #[serde(default)]
    pub turn_kind: String,
    #[serde(default)]
    pub lifecycle: String,
    #[serde(default)]
    pub process_live: bool,
    #[serde(default)]
    pub remote: bool,
    #[serde(default)]
    pub finalizing: bool,
    #[serde(default)]
    pub pending_handoff: Option<String>,
    #[serde(default)]
    pub staged_handoff: Option<String>,
    #[serde(default)]
    pub staged_receipt_outcome: Option<String>,
    #[serde(default)]
    pub handoff_inflight: bool,
    #[serde(default)]
    pub recovery_in_flight: bool,
    #[serde(default)]
    pub turn_state: Option<String>,
    #[serde(default)]
    pub followup_pending: bool,
    #[serde(default)]
    pub followup_outcome: Option<String>,
    #[serde(default)]
    pub followup_agent: Option<String>,
    #[serde(default)]
    pub declared_bead_action: Option<String>,
    #[serde(default)]
    pub started_at: Option<f64>,
    #[serde(default)]
    pub ended_at: Option<f64>,
}

/// Reducer input: every bead plus the settle grace.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StrandedWorkInputWire {
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
    #[serde(default)]
    pub now: f64,
    #[serde(default = "default_settle_grace_seconds")]
    pub settle_grace_seconds: f64,
    #[serde(default)]
    pub beads: Vec<StrandedWorkBeadFactsWire>,
}

impl Default for StrandedWorkInputWire {
    fn default() -> Self {
        Self {
            schema_version: default_schema_version(),
            now: 0.0,
            settle_grace_seconds: default_settle_grace_seconds(),
            beads: Vec::new(),
        }
    }
}

/// One verdict per candidate bead.
///
/// `reason` holds the cause code for `stranded`, the owner kind for `owned`
/// (`running`/`asking`/`waiting`/`queued`/`finalizing`/`handoff`/`monitor`/
/// `gate`/`recovery`), and `no_records`/`records_unavailable` for `unknown`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StrandedWorkVerdictWire {
    #[serde(default)]
    pub bead_id: String,
    #[serde(default)]
    pub kind: String,
    #[serde(default)]
    pub state: String,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub owner: Option<String>,
    #[serde(default)]
    pub delegates: Vec<String>,
    #[serde(default)]
    pub session: Option<String>,
    #[serde(default)]
    pub last_turn: Option<String>,
    #[serde(default)]
    pub last_turn_artifact_dir: Option<String>,
    #[serde(default)]
    pub completion_failure: Option<BeadCompletionFailureWire>,
    #[serde(default)]
    pub declared_bead_action: Option<String>,
    #[serde(default)]
    pub since: Option<f64>,
    #[serde(default)]
    pub parent_id: Option<String>,
    #[serde(default)]
    pub blocked_by: Vec<String>,
    #[serde(default)]
    pub blocks: Vec<String>,
}

/// Reducer output: verdicts sorted by bead id plus sorted id sets.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StrandedWorkReportWire {
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
    #[serde(default)]
    pub verdicts: Vec<StrandedWorkVerdictWire>,
    #[serde(default)]
    pub stranded_ids: Vec<String>,
    #[serde(default)]
    pub blocked_ids: Vec<String>,
}

impl StrandedWorkVerdictWire {
    /// True when propagation is still allowed to switch this state to
    /// `blocked`: the bead has an owner or a delegate, or is still settling.
    pub fn is_blockable(&self) -> bool {
        matches!(
            self.state.as_str(),
            STRANDED_STATE_OWNED
                | STRANDED_STATE_DELEGATED
                | STRANDED_STATE_SETTLING
        )
    }

    /// True for the leaves propagation walks upward from.
    pub fn is_stranded_leaf(&self) -> bool {
        self.state == STRANDED_STATE_STRANDED
    }

    /// True for ancestors propagation walks through without switching.
    pub fn is_walk_through(&self) -> bool {
        matches!(
            self.state.as_str(),
            STRANDED_STATE_STRANDED
                | STRANDED_STATE_UNKNOWN
                | STRANDED_STATE_BLOCKED
        )
    }
}
