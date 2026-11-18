//! Pure projection from full artifact records onto compact list rows.

use std::collections::BTreeMap;

use serde_json::{Map, Value};

use super::wire::{
    AgentArtifactListDoneWire, AgentArtifactListFinalizerInstanceWire,
    AgentArtifactListFinalizerWire, AgentArtifactListGateWire,
    AgentArtifactListMetaWire, AgentArtifactListMonitorWire,
    AgentArtifactListOutputFactsWire, AgentArtifactListPendingQuestionWire,
    AgentArtifactListRecordWire, AgentArtifactListRecoveryWire,
    AgentArtifactListRunningWire, AgentArtifactListShapeWire,
    AgentArtifactListSnapshotWire, AgentArtifactListStepWire,
    AgentArtifactListTurnWire, AgentArtifactListWaitingWire,
    AgentArtifactListWorkflowStepWire, AgentArtifactListWorkflowWire,
    AGENT_ARTIFACT_LIST_WIRE_SCHEMA_VERSION,
};
use crate::agent_auto_restart::AgentRecoveryWire;
use crate::agent_runtime::is_real_monitor_member_record;
use crate::agent_scan::wire::{
    AgentArtifactRecordWire, AgentArtifactScanWire, AgentMetaWire,
    AgentSessionTurnWire, DoneMarkerWire, FinalizerStatusSummaryWire,
    PendingQuestionMarkerWire, PromptStepMarkerWire, RunningMarkerWire,
    WaitingMarkerWire, WorkflowStateWire, WorkflowStepStateWire,
};

/// Project a full scan/index snapshot onto the compact list contract.
///
/// Selection, ordering, clan context, and completeness metadata are
/// copied through. Nested full records do not appear in the result.
pub fn project_scan_for_list(
    snapshot: AgentArtifactScanWire,
) -> AgentArtifactListSnapshotWire {
    AgentArtifactListSnapshotWire {
        schema_version: AGENT_ARTIFACT_LIST_WIRE_SCHEMA_VERSION,
        projects_root: snapshot.projects_root,
        options: snapshot.options,
        stats: snapshot.stats,
        index_window: snapshot.index_window,
        records: snapshot
            .records
            .iter()
            .map(project_record_for_list_compact)
            .collect(),
        clan_context: snapshot.clan_context,
        index_completeness: snapshot.index_completeness,
    }
}

/// Project one full record onto a compact list row.
///
/// Marker presence is preserved: `None` means the marker was absent,
/// including inconsistent combinations such as `has_done_marker` with
/// `done == None`.
pub fn project_record_for_list_compact(
    record: &AgentArtifactRecordWire,
) -> AgentArtifactListRecordWire {
    AgentArtifactListRecordWire {
        project_name: record.project_name.clone(),
        project_dir: record.project_dir.clone(),
        project_file: record.project_file.clone(),
        workflow_dir_name: record.workflow_dir_name.clone(),
        artifact_dir: record.artifact_dir.clone(),
        timestamp: record.timestamp.clone(),
        has_done_marker: record.has_done_marker,
        record_shape: AgentArtifactListShapeWire::Compact,
        meta: record
            .agent_meta
            .as_ref()
            .map(|meta| project_meta(record, meta)),
        done: record.done.as_ref().map(project_done),
        running: record.running.as_ref().map(project_running),
        waiting: record.waiting.as_ref().map(project_waiting),
        pending_question: record
            .pending_question
            .as_ref()
            .map(project_pending_question),
        workflow: record.workflow_state.as_ref().map(project_workflow),
        plan_path: record
            .plan_path
            .as_ref()
            .and_then(|marker| marker.plan_path.clone()),
        prompt_steps: record
            .prompt_steps
            .iter()
            .map(project_prompt_step)
            .collect(),
    }
}

fn project_meta(
    record: &AgentArtifactRecordWire,
    meta: &AgentMetaWire,
) -> AgentArtifactListMetaWire {
    AgentArtifactListMetaWire {
        name: meta.name.clone(),
        cl_name: meta.cl_name.clone(),
        process_identity: compact_process_identity(record, meta),
        runner_claim_owner_key: meta.runner_claim_owner_key.clone(),
        pid: meta.pid,
        model: meta.model.clone(),
        llm_provider: meta.llm_provider.clone(),
        reasoning_effort: meta.reasoning_effort.clone(),
        model_alias: meta.model_alias.clone(),
        vcs_provider: meta.vcs_provider.clone(),
        workspace_dir: meta.workspace_dir.clone(),
        workspace_num: meta.workspace_num,
        plan_path: meta.plan_path.clone(),
        sdd_plan_path: meta.sdd_plan_path.clone(),
        epic_plan_ref: meta.epic_plan_ref.clone(),
        plan: meta.plan,
        plan_approved: meta.plan_approved,
        plan_action: meta.plan_action.clone(),
        plan_committed: meta.plan_committed,
        plan_submitted_at: meta.plan_submitted_at.clone(),
        epic_bead_id: meta.epic_bead_id.clone(),
        phase_bead_id: meta.phase_bead_id.clone(),
        created_epics: meta.created_epics.clone(),
        commit_diff_path: meta.commit_diff_path.clone(),
        tribe: meta.tribe.clone(),
        agent_tab: meta.agent_tab.clone(),
        wait_for: meta.wait_for.clone(),
        wait_for_beads: meta.wait_for_beads.clone(),
        wait_for_hoods: meta.wait_for_hoods.clone(),
        wait_for_epics_of: meta.wait_for_epics_of.clone(),
        wait_epic_follows: meta.wait_epic_follows.clone(),
        wait_turn_bindings: meta.wait_turn_bindings.clone(),
        wait_duration: meta.wait_duration,
        wait_until: meta.wait_until.clone(),
        queue_capacity: meta.queue_capacity,
        queue_capacity_explicit: meta.queue_capacity_explicit,
        queue_capacity_multiplier: meta.queue_capacity_multiplier,
        wait_priority: meta.wait_priority,
        queue_weight: meta.queue_weight,
        queue_weight_explicit: meta.queue_weight_explicit,
        queue_weight_invalid: meta.queue_weight_invalid,
        queue_weight_error: meta.queue_weight_error.clone(),
        approve: meta.approve,
        auto_approve_plan_action: meta.auto_approve_plan_action.clone(),
        auto_approve_argument: meta.auto_approve_argument.clone(),
        hidden: meta.hidden,
        role_suffix: meta.role_suffix.clone(),
        agent_session: meta.agent_session.clone(),
        agent_session_role: meta.agent_session_role.clone(),
        agent_session_parallel: meta.agent_session_parallel,
        plan_chain_root: meta.plan_chain_root,
        agent_clan: meta.agent_clan.clone(),
        agent_clan_generation: meta.agent_clan_generation.clone(),
        clan_tribe: meta.clan_tribe.clone(),
        clan_summary: meta.clan_summary.clone(),
        parent_timestamp: meta.parent_timestamp.clone(),
        retry_of_timestamp: meta.retry_of_timestamp.clone(),
        retry_attempt: meta.retry_attempt,
        retry_chain_root_timestamp: meta.retry_chain_root_timestamp.clone(),
        retried_as_timestamp: meta.retried_as_timestamp.clone(),
        retry_terminal: meta.retry_terminal,
        retry_error_category: meta.retry_error_category.clone(),
        retry_started_at: meta.retry_started_at.clone(),
        epic_started_at: meta.epic_started_at.clone(),
        feedback_submitted_at: meta.feedback_submitted_at.clone(),
        questions_submitted_at: meta.questions_submitted_at.clone(),
        run_started_at: meta.run_started_at.clone(),
        stopped_at: meta.stopped_at.clone(),
        wait_completed_at: meta.wait_completed_at.clone(),
        question_request_path: meta.question_request_path.clone(),
        question_response_path: meta.question_response_path.clone(),
        question_session_id: meta.question_session_id.clone(),
        imported_source_owner: meta.imported_source_owner.clone(),
        source_machine: meta.source_machine.clone(),
        output_path: meta.output_path.clone(),
        finalizer_status: meta.finalizer_status.as_ref().map(project_finalizer),
        auto_restart: meta.auto_restart.clone(),
        agent_session_turn: meta.agent_session_turn.as_ref().map(project_turn),
    }
}

/// Keep the full loader's monitor PID-reuse precedence in the compact row.
/// A supervisor stamp is authoritative when present, including an empty
/// string; a missing stamp falls back to the ordinary metadata identity.
fn compact_process_identity(
    record: &AgentArtifactRecordWire,
    meta: &AgentMetaWire,
) -> Option<String> {
    if !is_real_monitor_member_record(record) {
        return meta.process_identity.clone();
    }

    meta.agent_session_turn
        .as_ref()
        .filter(|turn| turn.kind == "monitor")
        .and_then(|turn| turn.monitor.as_ref())
        .and_then(|monitor| monitor.supervisor_identity.clone())
        .or_else(|| meta.process_identity.clone())
}

fn project_done(done: &DoneMarkerWire) -> AgentArtifactListDoneWire {
    AgentArtifactListDoneWire {
        outcome: done.outcome.clone(),
        cl_name: done.cl_name.clone(),
        project_file: done.project_file.clone(),
        workspace_num: done.workspace_num,
        workspace_dir: done.workspace_dir.clone(),
        model: done.model.clone(),
        llm_provider: done.llm_provider.clone(),
        vcs_provider: done.vcs_provider.clone(),
        name: done.name.clone(),
        plan_path: done.plan_path.clone(),
        diff_path: done.diff_path.clone(),
        step_output: project_output_facts(done.step_output.as_ref()),
        error: done.error.clone(),
        retried_as_timestamp: done.retried_as_timestamp.clone(),
        retry_chain_root_timestamp: done.retry_chain_root_timestamp.clone(),
        retry_error_category: done.retry_error_category.clone(),
        approve: done.approve,
        hidden: done.hidden,
        repeat_stopped: done.repeat_stopped,
        status_label: done.status_label.clone(),
        imported_source_owner: done.imported_source_owner.clone(),
        source_machine: done.source_machine.clone(),
        recovery: done.recovery.as_ref().map(project_recovery),
        agent_session_turn: done.agent_session_turn.as_ref().map(project_turn),
    }
}

fn project_running(
    running: &RunningMarkerWire,
) -> AgentArtifactListRunningWire {
    AgentArtifactListRunningWire {
        pid: running.pid,
        process_identity: running.process_identity.clone(),
        cl_name: running.cl_name.clone(),
        model: running.model.clone(),
        llm_provider: running.llm_provider.clone(),
        vcs_provider: running.vcs_provider.clone(),
        workspace_dir: running.workspace_dir.clone(),
    }
}

fn project_waiting(
    waiting: &WaitingMarkerWire,
) -> AgentArtifactListWaitingWire {
    AgentArtifactListWaitingWire {
        waiting_for: waiting.waiting_for.clone(),
        wait_for_beads: waiting.wait_for_beads.clone(),
        wait_for_hoods: waiting.wait_for_hoods.clone(),
        wait_for_epics_of: waiting.wait_for_epics_of.clone(),
        wait_epic_follows: waiting.wait_epic_follows.clone(),
        wait_turn_bindings: waiting.wait_turn_bindings.clone(),
        wait_duration: waiting.wait_duration,
        wait_until: waiting.wait_until.clone(),
        queue_capacity: waiting.queue_capacity,
        queue_capacity_multiplier: waiting.queue_capacity_multiplier,
        queue_capacity_explicit: waiting.queue_capacity_explicit,
        wait_priority: waiting.wait_priority,
        wait_priority_explicit: waiting.wait_priority_explicit,
        queue_weight: waiting.queue_weight,
        queue_weight_explicit: waiting.queue_weight_explicit,
        queue_weight_invalid: waiting.queue_weight_invalid,
        queue_weight_error: waiting.queue_weight_error.clone(),
        slot_requested_at: waiting.slot_requested_at.clone(),
    }
}

fn project_pending_question(
    marker: &PendingQuestionMarkerWire,
) -> AgentArtifactListPendingQuestionWire {
    AgentArtifactListPendingQuestionWire {
        session_id: marker.session_id.clone(),
        request_path: marker.request_path.clone(),
        submitted_at: marker.submitted_at.clone(),
    }
}

fn project_workflow(
    workflow: &WorkflowStateWire,
) -> AgentArtifactListWorkflowWire {
    AgentArtifactListWorkflowWire {
        workflow_name: workflow.workflow_name.clone(),
        cl_name: workflow.cl_name.clone(),
        status: workflow.status.clone(),
        pid: workflow.pid,
        appears_as_agent: workflow.appears_as_agent,
        is_anonymous: workflow.is_anonymous,
        hidden: workflow.hidden,
        current_step_index: workflow.current_step_index,
        start_time: workflow.start_time.clone(),
        error: workflow.error.clone(),
        activity: workflow.activity.clone(),
        steps: workflow.steps.iter().map(project_workflow_step).collect(),
    }
}

fn project_workflow_step(
    step: &WorkflowStepStateWire,
) -> AgentArtifactListWorkflowStepWire {
    AgentArtifactListWorkflowStepWire {
        name: step.name.clone(),
        status: step.status.clone(),
        output: project_output_facts(step.output.as_ref()),
        error: step.error.clone(),
    }
}

fn project_prompt_step(
    step: &PromptStepMarkerWire,
) -> AgentArtifactListStepWire {
    AgentArtifactListStepWire {
        file_name: step.file_name.clone(),
        workflow_name: step.workflow_name.clone(),
        step_name: step.step_name.clone(),
        step_type: step.step_type.clone(),
        step_source: step.step_source.clone(),
        step_index: step.step_index,
        total_steps: step.total_steps,
        parent_step_index: step.parent_step_index,
        parent_total_steps: step.parent_total_steps,
        status: step.status.clone(),
        hidden: step.hidden,
        is_pre_prompt_step: step.is_pre_prompt_step,
        embedded_workflow_name: step.embedded_workflow_name.clone(),
        artifacts_dir: step.artifacts_dir.clone(),
        diff_path: step.diff_path.clone(),
        error: step.error.clone(),
        model: step.model.clone(),
        llm_provider: step.llm_provider.clone(),
        reasoning_effort: step.reasoning_effort.clone(),
        model_alias: step.model_alias.clone(),
        output: project_output_facts(step.output.as_ref()),
    }
}

fn project_turn(turn: &AgentSessionTurnWire) -> AgentArtifactListTurnWire {
    AgentArtifactListTurnWire {
        kind: turn.kind.clone(),
        id: turn.id.clone(),
        state: turn.state.clone(),
        label: turn.label.clone(),
        start_status: turn.start_status.clone(),
        stop_status: turn.stop_status.clone(),
        followup_outcome: turn.followup_outcome.clone(),
        followup_error: turn.followup_error.clone(),
        followup_agent: turn.followup_agent.clone(),
        followup_degraded_reason: turn.followup_degraded_reason.clone(),
        host_completion_status: turn.host_completion_status.clone(),
        monitor: turn.monitor.as_ref().map(|monitor| {
            AgentArtifactListMonitorWire {
                exit_code: monitor.exit_code,
            }
        }),
        gate: turn.gate.as_ref().map(|gate| AgentArtifactListGateWire {
            kind: gate.kind.clone(),
            accent: gate.accent.clone(),
            bundle_path: gate.bundle_path.clone(),
            notification_id: gate.notification_id.clone(),
        }),
    }
}

fn project_recovery(
    recovery: &AgentRecoveryWire,
) -> AgentArtifactListRecoveryWire {
    AgentArtifactListRecoveryWire {
        state: recovery.state.clone(),
        reason: recovery.reason.clone(),
        reason_text: recovery.reason_text.clone(),
        requested_at: recovery.requested_at.clone(),
        updated_at: recovery.updated_at.clone(),
        episode_id: recovery.episode_id.clone(),
        hijack_id: recovery.hijack_id.clone(),
    }
}

fn project_finalizer(
    summary: &FinalizerStatusSummaryWire,
) -> AgentArtifactListFinalizerWire {
    AgentArtifactListFinalizerWire {
        phase: summary.phase.clone(),
        status: summary.status.clone(),
        updated_at: summary.updated_at,
        instance_count: summary.instance_count,
        instances: summary
            .instances
            .iter()
            .map(|instance| AgentArtifactListFinalizerInstanceWire {
                id: instance.id.clone(),
                status: instance.status.clone(),
                op: instance.op.clone(),
                step: instance.step.clone(),
            })
            .collect(),
    }
}

fn project_output_facts(
    output: Option<&Map<String, Value>>,
) -> Option<AgentArtifactListOutputFactsWire> {
    let output = output?;
    let mut meta = BTreeMap::new();
    for (key, value) in output {
        if list_output_meta_fact(key, value) {
            meta.insert(key.clone(), value.clone());
        }
    }
    let diff_path = match output.get("diff_path") {
        Some(Value::String(path))
            if path.len() <= MAX_LIST_FACT_STRING_BYTES =>
        {
            Some(path.clone())
        }
        _ => None,
    };
    Some(AgentArtifactListOutputFactsWire { meta, diff_path })
}

const MAX_LIST_FACT_STRING_BYTES: usize = 4096;

/// Return whether one output value is a bounded, inventoried list/action fact.
fn list_output_meta_fact(key: &str, value: &Value) -> bool {
    match key {
        "meta_workspace" => {
            value.as_i64().is_some()
                || value.as_u64().is_some()
                || bounded_output_string(value)
        }
        "meta_project" | "meta_patch" | "meta_changespec" | "meta_new_cl"
        | "meta_new_pr" => bounded_output_string(value),
        _ => false,
    }
}

fn bounded_output_string(value: &Value) -> bool {
    value
        .as_str()
        .is_some_and(|text| text.len() <= MAX_LIST_FACT_STRING_BYTES)
}
