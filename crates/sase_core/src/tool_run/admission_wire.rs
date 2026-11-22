//! Host-wide named-tool admission wires.
//!
//! The admission queue is the ToolRun ledger itself: a run's record lives
//! under `demand.admission`, holders are unsettled runs whose record says
//! `admitted`, and waiters are unsettled runs whose record says `waiting`,
//! ordered FIFO by `(requested_ts_ms, seq)`. Settlement releases a slot.
//!
//! The stored record and the projection wires are lenient (no
//! `deny_unknown_fields`) because older readers must keep parsing them.
//! Only the request wires are strict.

use serde::{Deserialize, Serialize};

use super::wire::TOOL_RUN_WIRE_SCHEMA_VERSION;

/// Version of the stored `demand.admission` record.
pub const TOOL_RUN_ADMISSION_RECORD_SCHEMA_VERSION: u32 = 1;

/// Admission keys (tool names) cap here.
pub const TOOL_RUN_ADMISSION_KEY_MAX_CHARS: usize = 128;

fn schema_version() -> u32 {
    TOOL_RUN_WIRE_SCHEMA_VERSION
}

fn record_schema_version() -> u32 {
    TOOL_RUN_ADMISSION_RECORD_SCHEMA_VERSION
}

fn empty_diagnostics() -> Vec<String> {
    Vec::new()
}

/// Where a run stands in its key's admission queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolRunAdmissionStateWire {
    /// Recorded and queued; the child has not started.
    Waiting,
    /// Granted a slot, forced over the limit, or recorded as an
    /// unavailable bypass; the child may start.
    Admitted,
    /// Settled while still waiting; the child never started.
    Abandoned,
}

impl ToolRunAdmissionStateWire {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Waiting => "waiting",
            Self::Admitted => "admitted",
            Self::Abandoned => "abandoned",
        }
    }
}

/// How an admitted run got its slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolRunAdmittedByWire {
    /// A free slot under the limit, in FIFO order.
    Slot,
    /// `tool_run_admission_force` after the caller's max wait elapsed.
    MaxWait,
    /// Fail-open execution after admission became unavailable. Distinct
    /// from a slot grant and from `max_wait`; still a holder until the
    /// run settles.
    Unavailable,
}

impl ToolRunAdmittedByWire {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Slot => "slot",
            Self::MaxWait => "max_wait",
            Self::Unavailable => "unavailable",
        }
    }
}

/// The stored per-run admission record, `run.demand.admission`.
///
/// Only the admission operations write it; `tool_run_record_demand`
/// preserves it. Times are epoch milliseconds and say so. `wait_ms` is set
/// at admission, or at settlement when the run is abandoned, and is never
/// part of `duration_ms`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolRunAdmissionWire {
    #[serde(default = "record_schema_version")]
    pub schema_version: u32,
    /// The admission key: the named tool's name, counted host-wide.
    pub key: String,
    /// The limit this run waited under.
    pub limit: u32,
    pub state: ToolRunAdmissionStateWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admitted_by: Option<ToolRunAdmittedByWire>,
    /// Host-monotonic FIFO tiebreaker assigned inside the enter transaction.
    pub seq: i64,
    pub requested_ts_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admitted_ts_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wait_ms: Option<u64>,
    /// Waiters ahead plus holders over the limit, at entry.
    #[serde(default)]
    pub ahead_at_request: u32,
}

/// `tool_run_admission_enter`: record a `waiting` admission for a running
/// run (idempotent per run), then evaluate it as `poll` does.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolRunAdmissionEnterRequestWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    pub run_id: String,
    pub key: String,
    pub limit: u32,
    /// Test clock override, epoch milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub now_ms: Option<i64>,
}

/// `tool_run_admission_poll`: report the run's queue position and grant a
/// free slot when the run is eligible.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolRunAdmissionPollRequestWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    pub run_id: String,
    /// Test clock override, epoch milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub now_ms: Option<i64>,
}

/// Why a run is admitted regardless of the limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolRunAdmissionForceReasonWire {
    MaxWait,
    /// The Python wrapper executed after admission became unavailable.
    /// Pass the pre-spawn bypass time as `now_ms` so a later recovery
    /// write does not charge child runtime to `wait_ms`.
    Unavailable,
}

/// `tool_run_admission_force`: admit a waiting run over the limit, or
/// record an unavailable bypass. `key` and `limit` seed a holder when
/// `reason` is `unavailable` and the run has no admission record yet.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolRunAdmissionForceRequestWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    pub run_id: String,
    pub reason: ToolRunAdmissionForceReasonWire,
    /// Test clock override, epoch milliseconds. For `unavailable`, the
    /// pre-spawn bypass timestamp so recovery preserves elapsed wait.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub now_ms: Option<i64>,
    /// Admission key used only to seed an `unavailable` holder when the
    /// run has no record. Falls back to the run's `tool_name`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    /// Limit used only to seed an `unavailable` holder when the run has
    /// no record.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolRunAdmissionOutcomeWire {
    /// The run holds a slot; its child may start.
    Admitted,
    /// The run is queued behind `ahead` others.
    Waiting,
    /// The run already settled; nothing was written.
    Settled,
    /// No run has this id; nothing was written.
    NotFound,
}

/// One queued or holding run, with the identity the caller needs to probe
/// its wrapper's liveness without listing the store.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolRunAdmissionPeerWire {
    pub run_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wrapper_pid: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub boot_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process_start_identity: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub launch_mode: Option<String>,
    pub seq: i64,
    pub requested_ts_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admitted_ts_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admitted_by: Option<ToolRunAdmittedByWire>,
}

/// Shared result of `enter`, `poll`, and `force`.
///
/// `holders` are every unsettled `admitted` run under `key` (including this
/// run once admitted). `waiters_ahead` are the `waiting` runs ordered before
/// this one. `ahead` is `waiters_ahead.len()` plus holders over the limit,
/// so a waiting run with `ahead == 0` takes the next free slot. `changed`
/// says whether this call wrote the store.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolRunAdmissionResultWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    pub run_id: String,
    pub outcome: ToolRunAdmissionOutcomeWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
    #[serde(default)]
    pub ahead: u32,
    #[serde(default)]
    pub holders: Vec<ToolRunAdmissionPeerWire>,
    #[serde(default)]
    pub waiters_ahead: Vec<ToolRunAdmissionPeerWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admission: Option<ToolRunAdmissionWire>,
    #[serde(default)]
    pub changed: bool,
    #[serde(default = "empty_diagnostics")]
    pub diagnostics: Vec<String>,
}

/// The live admission projection on show, glance, brief, node-summary, and
/// detail rows. Present only for unsettled runs that carry a record.
/// `ahead` is 0 for admitted runs; `holders` counts the key's admitted
/// unsettled runs host-wide.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolRunAdmissionViewWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    pub state: ToolRunAdmissionStateWire,
    pub key: String,
    pub limit: u32,
    #[serde(default)]
    pub ahead: u32,
    #[serde(default)]
    pub holders: u32,
    pub requested_ts_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admitted_ts_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admitted_by: Option<ToolRunAdmittedByWire>,
}
