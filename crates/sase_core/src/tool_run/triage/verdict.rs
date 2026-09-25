//! Pure run-level failure-kind and verdict functions.
//!
//! Kind is computed from terminal causes (or the legacy mapping when the
//! cause is absent); verdict is the ordered table over kind, classes,
//! completion facts, and triage presence.

use serde::{Deserialize, Serialize};

use super::super::wire::TOOL_RUN_WIRE_SCHEMA_VERSION;
use super::super::ToolRunError;

fn schema_version() -> u32 {
    TOOL_RUN_WIRE_SCHEMA_VERSION
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolRunTriageFailureKindWire {
    Control,
    Infrastructure,
    Environment,
    Verification,
    None,
}

impl ToolRunTriageFailureKindWire {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Control => "control",
            Self::Infrastructure => "infrastructure",
            Self::Environment => "environment",
            Self::Verification => "verification",
            Self::None => "none",
        }
    }

    pub fn from_db(value: &str) -> Result<Self, String> {
        match value {
            "control" => Ok(Self::Control),
            "infrastructure" => Ok(Self::Infrastructure),
            "environment" => Ok(Self::Environment),
            "verification" => Ok(Self::Verification),
            "none" => Ok(Self::None),
            other => Err(format!("unknown failure kind {other:?}")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolRunTriageVerdictWire {
    Pass,
    NewFailures,
    Undetermined,
    NoNewFailures,
}

impl ToolRunTriageVerdictWire {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pass => "pass",
            Self::NewFailures => "new_failures",
            Self::Undetermined => "undetermined",
            Self::NoNewFailures => "no_new_failures",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolRunTriageVerdictItemWire {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub class: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolRunTriageVerdictRequestWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal_cause: Option<String>,
    // Legacy-row inputs used only when terminal_cause is absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub legacy_state: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub legacy_exit_code: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub legacy_signal: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub legacy_interruption_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub legacy_lost_reason: Option<String>,
    /// True when at least one stage completed (needed for environment).
    #[serde(default)]
    pub has_completed_stage: bool,
    /// True when the output of record carries a recognized _setup marker.
    #[serde(default)]
    pub has_setup_marker: bool,
    /// True when a failed stage exists (verification needs items).
    #[serde(default)]
    pub has_failed_stage: bool,
    /// True when every stage ran to completion.
    #[serde(default)]
    pub all_stages_complete: bool,
    /// True when a recipe_finished fact exists.
    #[serde(default)]
    pub recipe_finished: bool,
    /// False for `stages: none` tools (v1 cannot prove full execution).
    #[serde(default = "default_true")]
    pub is_stageful_tool: bool,
    /// False when the run was never triaged.
    #[serde(default)]
    pub triaged: bool,
    /// True when at least one failed stage produced no parsed item
    /// (becomes one generic UNKNOWN item).
    #[serde(default)]
    pub has_unparsed_failed_stage: bool,
    #[serde(default)]
    pub items: Vec<ToolRunTriageVerdictItemWire>,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolRunTriageVerdictResultWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    pub kind: ToolRunTriageFailureKindWire,
    pub verdict: ToolRunTriageVerdictWire,
    pub reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remedy: Option<String>,
}

fn validate_schema(version: u32) -> Result<(), ToolRunError> {
    if version != TOOL_RUN_WIRE_SCHEMA_VERSION {
        return Err(ToolRunError::SchemaVersion {
            expected: TOOL_RUN_WIRE_SCHEMA_VERSION,
            actual: version,
        });
    }
    Ok(())
}

fn legacy_kind(
    request: &ToolRunTriageVerdictRequestWire,
) -> (ToolRunTriageFailureKindWire, String) {
    let state = request.legacy_state.as_deref().unwrap_or_default();
    match state {
        "succeeded" => (
            ToolRunTriageFailureKindWire::None,
            "legacy_succeeded".to_string(),
        ),
        "failed" => match request.legacy_exit_code {
            Some(code) if code != 126 && code != 127 => (
                ToolRunTriageFailureKindWire::Verification,
                "legacy_failed_exit".to_string(),
            ),
            _ => (
                ToolRunTriageFailureKindWire::Infrastructure,
                "legacy_unmapped".to_string(),
            ),
        },
        "interrupted" => (
            ToolRunTriageFailureKindWire::Control,
            "legacy_interrupted".to_string(),
        ),
        "signaled" => (
            ToolRunTriageFailureKindWire::Infrastructure,
            "legacy_unmapped".to_string(),
        ),
        "lost" => (
            ToolRunTriageFailureKindWire::Infrastructure,
            "legacy_lost".to_string(),
        ),
        "created" | "running" => (
            ToolRunTriageFailureKindWire::Infrastructure,
            "legacy_unsettled".to_string(),
        ),
        _ => (
            ToolRunTriageFailureKindWire::Infrastructure,
            "legacy_unmapped".to_string(),
        ),
    }
}

fn failure_kind(
    request: &ToolRunTriageVerdictRequestWire,
) -> (ToolRunTriageFailureKindWire, String, Option<String>) {
    if let Some(cause) = request.terminal_cause.as_deref() {
        match cause {
            "stop_requested" | "interrupt" | "timeout" => {
                return (
                    ToolRunTriageFailureKindWire::Control,
                    cause.to_string(),
                    None,
                );
            }
            "launch_failed" | "owner_lost" | "wrapper_lost" | "signal" => {
                return (
                    ToolRunTriageFailureKindWire::Infrastructure,
                    cause.to_string(),
                    None,
                );
            }
            "exited" => {
                if request.exit_code == Some(0) {
                    return (
                        ToolRunTriageFailureKindWire::None,
                        "exited_zero".to_string(),
                        None,
                    );
                }
                if !request.has_completed_stage && request.has_setup_marker {
                    return (
                        ToolRunTriageFailureKindWire::Environment,
                        "environment".to_string(),
                        Some(environment_remedy().to_string()),
                    );
                }
                return (
                    ToolRunTriageFailureKindWire::Verification,
                    "exited_nonzero".to_string(),
                    None,
                );
            }
            _ => {
                return (
                    ToolRunTriageFailureKindWire::Infrastructure,
                    "legacy_unmapped".to_string(),
                    None,
                );
            }
        }
    }
    // No terminal cause: exit 0 still passes; otherwise legacy mapping.
    if request.exit_code == Some(0) {
        return (
            ToolRunTriageFailureKindWire::None,
            "exited_zero".to_string(),
            None,
        );
    }
    let (kind, reason) = legacy_kind(request);
    (kind, reason, None)
}

fn environment_remedy() -> &'static str {
    "run `just install` or `sase update` and retry"
}

/// Pure failure-kind plus verdict evaluation in contract order.
pub fn tool_run_triage_verdict(
    request: ToolRunTriageVerdictRequestWire,
) -> Result<ToolRunTriageVerdictResultWire, ToolRunError> {
    validate_schema(request.schema_version)?;
    let (kind, kind_reason, remedy) = failure_kind(&request);
    // Verdict table in order.
    if kind == ToolRunTriageFailureKindWire::None {
        return Ok(ToolRunTriageVerdictResultWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            kind,
            verdict: ToolRunTriageVerdictWire::Pass,
            reason: kind_reason,
            remedy,
        });
    }
    if matches!(
        kind,
        ToolRunTriageFailureKindWire::Control
            | ToolRunTriageFailureKindWire::Infrastructure
            | ToolRunTriageFailureKindWire::Environment
    ) {
        return Ok(ToolRunTriageVerdictResultWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            kind,
            verdict: ToolRunTriageVerdictWire::Undetermined,
            reason: kind_reason,
            remedy,
        });
    }
    // kind == verification from here.
    let classes: Vec<String> = request
        .items
        .iter()
        .filter_map(|item| item.class.clone())
        .collect();
    if classes.iter().any(|class| class == "new") {
        return Ok(ToolRunTriageVerdictResultWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            kind,
            verdict: ToolRunTriageVerdictWire::NewFailures,
            reason: "has_new_item".to_string(),
            remedy: None,
        });
    }
    // Missing labels or untriaged rows yield undetermined.
    if !request.triaged
        || request.items.is_empty() && request.has_failed_stage
        || request.items.len() != classes.len()
        || classes.iter().any(|class| class == "unknown")
        || request.has_unparsed_failed_stage
        || !request.recipe_finished
        || !request.is_stageful_tool
    {
        let reason = if !request.triaged {
            "not_triaged"
        } else if request.has_unparsed_failed_stage
            || (request.items.is_empty() && request.has_failed_stage)
        {
            "failed_stage_without_items"
        } else if request.items.len() != classes.len() {
            "missing_labels"
        } else if classes.iter().any(|class| class == "unknown") {
            "has_unknown_item"
        } else if !request.recipe_finished {
            "recipe_not_finished"
        } else {
            "stages_none_tool"
        };
        return Ok(ToolRunTriageVerdictResultWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            kind,
            verdict: ToolRunTriageVerdictWire::Undetermined,
            reason: reason.to_string(),
            remedy: None,
        });
    }
    if !request.all_stages_complete {
        return Ok(ToolRunTriageVerdictResultWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            kind,
            verdict: ToolRunTriageVerdictWire::Undetermined,
            reason: "stages_incomplete".to_string(),
            remedy: None,
        });
    }
    if classes
        .iter()
        .all(|class| class == "known" || class == "flaky")
        && !classes.is_empty()
    {
        return Ok(ToolRunTriageVerdictResultWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            kind,
            verdict: ToolRunTriageVerdictWire::NoNewFailures,
            reason: "all_known_or_flaky".to_string(),
            remedy: None,
        });
    }
    Ok(ToolRunTriageVerdictResultWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        kind,
        verdict: ToolRunTriageVerdictWire::Undetermined,
        reason: "undetermined".to_string(),
        remedy: None,
    })
}
