//! Hand-off reservation wires: launch envelope, claim, stop, and owner facts.
//!
//! All new request wires are `deny_unknown_fields` like their siblings.
//! Result wires and `ToolRunWire` stay lenient so older readers keep working.

use serde::{Deserialize, Serialize};

use super::wire::{
    ToolDefinitionWire, ToolRunStateWire, ToolRunWire,
    TOOL_RUN_WIRE_SCHEMA_VERSION,
};

fn handoff_schema_version() -> u32 {
    TOOL_RUN_WIRE_SCHEMA_VERSION
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolRunLaunchModeWire {
    Foreground,
    Handoff,
}

impl ToolRunLaunchModeWire {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Foreground => "foreground",
            Self::Handoff => "handoff",
        }
    }

    pub fn from_db(value: &str) -> Result<Self, String> {
        match value {
            "foreground" => Ok(Self::Foreground),
            "handoff" => Ok(Self::Handoff),
            other => Err(format!("unknown tool run launch mode {other:?}")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolRunTerminalCauseWire {
    Exited,
    Signal,
    Interrupt,
    StopRequested,
    Timeout,
    LaunchFailed,
    OwnerLost,
    WrapperLost,
}

impl ToolRunTerminalCauseWire {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Exited => "exited",
            Self::Signal => "signal",
            Self::Interrupt => "interrupt",
            Self::StopRequested => "stop_requested",
            Self::Timeout => "timeout",
            Self::LaunchFailed => "launch_failed",
            Self::OwnerLost => "owner_lost",
            Self::WrapperLost => "wrapper_lost",
        }
    }

    pub fn from_db(value: &str) -> Result<Self, String> {
        match value {
            "exited" => Ok(Self::Exited),
            "signal" => Ok(Self::Signal),
            "interrupt" => Ok(Self::Interrupt),
            "stop_requested" => Ok(Self::StopRequested),
            "timeout" => Ok(Self::Timeout),
            "launch_failed" => Ok(Self::LaunchFailed),
            "owner_lost" => Ok(Self::OwnerLost),
            "wrapper_lost" => Ok(Self::WrapperLost),
            other => Err(format!("unknown tool run terminal cause {other:?}")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolRunSettledByWire {
    Wrapper,
    Reconcile,
    Owner,
}

impl ToolRunSettledByWire {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Wrapper => "wrapper",
            Self::Reconcile => "reconcile",
            Self::Owner => "owner",
        }
    }

    pub fn from_db(value: &str) -> Result<Self, String> {
        match value {
            "wrapper" => Ok(Self::Wrapper),
            "reconcile" => Ok(Self::Reconcile),
            "owner" => Ok(Self::Owner),
            other => Err(format!("unknown tool run settled by {other:?}")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolRunOwnerStateWire {
    Active,
    Terminal,
    Missing,
    Unknown,
}

impl ToolRunOwnerStateWire {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Terminal => "terminal",
            Self::Missing => "missing",
            Self::Unknown => "unknown",
        }
    }

    pub fn from_db(value: &str) -> Result<Self, String> {
        match value {
            "active" => Ok(Self::Active),
            "terminal" => Ok(Self::Terminal),
            "missing" => Ok(Self::Missing),
            "unknown" => Ok(Self::Unknown),
            other => Err(format!("unknown tool run owner state {other:?}")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolRunClaimOutcomeWire {
    Claimed,
    Refused,
    Stopped,
}

impl ToolRunClaimOutcomeWire {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Claimed => "claimed",
            Self::Refused => "refused",
            Self::Stopped => "stopped",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolRunClaimRefusalWire {
    NotCreated,
    OwnerMismatch,
    AlreadyClaimed,
    NotHandoff,
}

impl ToolRunClaimRefusalWire {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NotCreated => "not_created",
            Self::OwnerMismatch => "owner_mismatch",
            Self::AlreadyClaimed => "already_claimed",
            Self::NotHandoff => "not_handoff",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolRunStopOutcomeWire {
    Recorded,
    AlreadyRequested,
    AlreadySettled,
}

impl ToolRunStopOutcomeWire {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Recorded => "recorded",
            Self::AlreadyRequested => "already_requested",
            Self::AlreadySettled => "already_settled",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolRunLaunchEnvelopeWire {
    pub argv: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_name: Option<String>,
    #[serde(default)]
    pub extra_args: Vec<String>,
    pub display_argv: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub private_argv: Option<Vec<String>>,
    pub definition: ToolDefinitionWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub digest: Option<String>,
    #[serde(default)]
    pub adhoc: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolRunProcessIdentityWire {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pid: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub boot_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process_start_identity: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolRunStopRecordWire {
    pub requested_ts: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_by: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolRunOwnerFactWire {
    pub kind: String,
    pub id: String,
    pub state: ToolRunOwnerStateWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub termination_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stop_requested: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolRunClaimRequestWire {
    #[serde(default = "handoff_schema_version")]
    pub schema_version: u32,
    pub run_id: String,
    pub owner_kind: String,
    pub owner_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wrapper_pid: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub boot_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process_start_identity: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_log_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub running_event_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub now_ts: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolRunClaimResultWire {
    #[serde(default = "handoff_schema_version")]
    pub schema_version: u32,
    pub outcome: ToolRunClaimOutcomeWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refusal: Option<ToolRunClaimRefusalWire>,
    pub replayed: bool,
    pub run: ToolRunWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub launch: Option<ToolRunLaunchEnvelopeWire>,
    #[serde(default)]
    pub diagnostics: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolRunStopRequestWire {
    #[serde(default = "handoff_schema_version")]
    pub schema_version: u32,
    pub run_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_by: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub now_ts: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolRunStopResultWire {
    #[serde(default = "handoff_schema_version")]
    pub schema_version: u32,
    pub outcome: ToolRunStopOutcomeWire,
    pub run: ToolRunWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stop_request: Option<ToolRunStopRecordWire>,
    #[serde(default)]
    pub diagnostics: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolRunReconcileSettlementWire {
    pub run_id: String,
    pub state: ToolRunStateWire,
    pub terminal_cause: String,
    pub settled_by: String,
}
