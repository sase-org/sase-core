//! Wire records and deterministic helpers for agent launch.

mod admission;
mod condition;
mod conditional;
mod directive_scan;
mod fanout;
mod identity;
mod launch_hold;
mod launch_prep;
mod plan_resolution;
mod proc_runtime;
mod typed_units;
mod wires;
mod workspace_claims;

#[cfg(test)]
mod tests;

pub use admission::{
    admission_unit_results, agent_unit_dispatch_prompt,
    agent_unit_dispatch_prompt_with_flags, dispatch_fingerprint,
    next_admission_actions, next_admission_actions_with_holds,
    reconcile_admission_journal, summarize_admission, wait_target_key,
    LaunchAdmissionActionWire, LaunchAdmissionHoldBlockWire,
    LaunchAdmissionJournalEntryWire, LaunchAdmissionSummaryWire,
    LaunchAdmissionUnitStateWire, LaunchAdmissionWaitFactWire,
    LaunchUnitPhaseWire, WaitedOutcomeWire,
    LAUNCH_ADMISSION_JOURNAL_SCHEMA_VERSION,
};
pub use condition::{
    build_condition_context, classify_condition_status, condition_command_argv,
    condition_context_digest, evaluate_launch_condition, sanitize_safe_inputs,
    sanitized_condition_env, ConditionCheckWire, ConditionContextWire,
    ConditionEvalRequestWire, ConditionEvalResultWire,
    ConditionLogicalUnitWire, ConditionWaitedOutcomeWire,
    CONDITION_CONTEXT_SCHEMA_VERSION, CONDITION_DEFAULT_TIMEOUT_SECONDS,
    CONDITION_EVAL_WIRE_SCHEMA_VERSION, CONDITION_MAX_TIMEOUT_SECONDS,
    CONDITION_OUTPUT_CAP_BYTES,
};
pub use conditional::{
    filter_conditional_launch_segments, ConditionalLaunchSegmentFilterWire,
    ConditionalLaunchSegmentWire,
    CONDITIONAL_LAUNCH_SEGMENT_FILTER_SCHEMA_VERSION,
};
pub(crate) use directive_scan::{
    alt_directive_starts, directive_occurrences, find_matching_delimiter,
    launch_literal_zone_ranges,
};
pub use fanout::{bind_batch_predecessor_waits, plan_agent_launch_fanout};
pub use launch_hold::{launch_unit_hold_armer, launch_unit_hold_key};
pub use launch_prep::{prepare_agent_launch, safe_launch_name};
pub use plan_resolution::prompt_has_identity_directive;
pub use proc_runtime::{
    cleanup_proc_private_inputs, parse_proc_duration_seconds,
    prepare_proc_script, proc_script_argv, resolve_proc_execution_cwd,
    sanitized_proc_env, validate_proc_workspace_intent,
    validate_standalone_named_proc_name, ProcDispatchPreparedWire,
    ProcDispatchRequestWire, PROC_DISPATCH_WIRE_SCHEMA_VERSION,
    PROC_PHASE_ACQUIRING_WORKSPACE, PROC_PHASE_CHECKING,
    PROC_PHASE_PREPARING_SCRIPT, PROC_PHASE_RUNNING, PROC_PHASE_SETTLING,
    PROC_PHASE_WAITING, XPROMPT_PROC_ORIGIN,
};
pub use typed_units::{
    plan_typed_launch_units, plan_typed_launch_units_with_flags,
};
pub use wires::{
    allocate_launch_timestamp_batch, AgentLaunchFanoutPlanError,
    AgentLaunchPreparationError, AgentLaunchPreparedWire,
    AgentLaunchRequestWire, AgentUnitWire, BatchPredecessorContextWire,
    BatchPredecessorWaitBindingWire, LaunchConditionWire, LaunchFanoutPlanWire,
    LaunchFanoutSlotWire, LaunchOutcomeWire, LaunchPlanDiagnosticWire,
    LaunchPlanWire, LaunchUnitPayloadWire, LaunchUnitResultWire,
    LaunchUnitWire, OccupancyCallerWire, OccupancyConflictDecisionWire,
    OccupantRecordWire, ProcUnitWire, TimestampBatchAllocationError,
    WaitTargetWire, WorkspaceClaimOutcomeWire, WorkspaceClaimPlanWire,
    WorkspaceClaimRequestWire, WorkspaceClaimWire,
    AGENT_LAUNCH_WIRE_SCHEMA_VERSION, BATCH_PREDECESSOR_CONTEXT_SCHEMA_VERSION,
    LAUNCH_PLAN_WIRE_SCHEMA_VERSION,
};
pub use workspace_claims::{
    allocate_and_claim_workspace_from_content,
    decide_workspace_occupant_conflict, list_workspace_claims_from_content,
    plan_claim_workspace_from_content,
    plan_transfer_workspace_claim_from_content,
};
