use super::agent_hijack_wire_schema_version;
use super::eligibility::agent_hijack_eligibility;
use super::ledger::advance_agent_hijack_record;
use super::status::*;
use super::wire::*;
use crate::agent_auto_restart::AgentRecoveryWire;

fn record(state: &str) -> AgentHijackRecordWire {
    AgentHijackRecordWire {
        schema_version: AGENT_HIJACK_WIRE_SCHEMA_VERSION,
        hijack_id: "h-1".to_string(),
        agent_name: "agent.1".to_string(),
        state: state.to_string(),
        ..Default::default()
    }
}

fn event(event: &str) -> AgentHijackEventWire {
    AgentHijackEventWire {
        event: event.to_string(),
        ..Default::default()
    }
}

#[test]
fn latch_transitions_preserve_identity_and_boundary_evidence() {
    let mut requested = record(HIJACK_REQUESTED);
    requested.target_key = "target-1".to_owned();
    let latched = advance_agent_hijack_record(
        &requested,
        &AgentHijackEventWire {
            event: HIJACK_EVENT_LATCH.to_owned(),
            owner_pid: Some(42),
            owner_identity: Some("boot:123".to_owned()),
            latched_at: Some(10_000),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(latched.state, HIJACK_LATCHED);
    assert_eq!(latched.owner_pid, Some(42));
    assert_eq!(latched.owner_identity.as_deref(), Some("boot:123"));
    assert_eq!(latched.latched_at, Some(10_000));

    let stopping = advance_agent_hijack_record(
        &latched,
        &AgentHijackEventWire {
            event: HIJACK_EVENT_APPLY_LATCH.to_owned(),
            boundary: Some("staged_adoption".to_owned()),
            boundary_outcome: Some("handoff".to_owned()),
            suppressed_followup: Some(AgentHijackSuppressedFollowupWire {
                kind: "pipe".to_owned(),
                reference: "/tmp/payload.json".to_owned(),
            }),
            at: Some("2026-11-16T10:00:00Z".to_owned()),
            forced: Some(true),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(stopping.state, HIJACK_STOPPING);
    assert_eq!(stopping.boundary.as_deref(), Some("staged_adoption"));
    assert_eq!(stopping.boundary_outcome.as_deref(), Some("handoff"));
    assert_eq!(stopping.suppressed_followup.as_ref().unwrap().kind, "pipe");
    assert_eq!(
        stopping.suppressed_followup.as_ref().unwrap().reference,
        "/tmp/payload.json"
    );
    assert!(stopping.forced);

    let held = advance_agent_hijack_record(
        &stopping,
        &event(HIJACK_EVENT_CONFIRMED_STOP),
    )
    .unwrap();
    assert_eq!(held.state, HIJACK_HELD);
    assert_eq!(held.owner_identity.as_deref(), Some("boot:123"));
}

#[test]
fn latch_cancel_and_moot_are_terminal_and_invalid_edges_are_rejected() {
    let mut requested = record(HIJACK_REQUESTED);
    requested.target_key = "target-1".to_owned();
    let latch_event = AgentHijackEventWire {
        event: HIJACK_EVENT_LATCH.to_owned(),
        owner_pid: Some(42),
        owner_identity: Some("boot:123".to_owned()),
        latched_at: Some(10_000),
        ..Default::default()
    };
    let latched =
        advance_agent_hijack_record(&requested, &latch_event).unwrap();
    let cancelled = advance_agent_hijack_record(
        &latched,
        &event(HIJACK_EVENT_CANCEL_LATCH),
    )
    .unwrap();
    assert_eq!(cancelled.state, HIJACK_CANCELLED);
    assert!(advance_agent_hijack_record(
        &cancelled,
        &event(HIJACK_EVENT_CANCEL_LATCH)
    )
    .is_err());

    let moot = advance_agent_hijack_record(
        &latched,
        &AgentHijackEventWire {
            event: HIJACK_EVENT_MOOT_LATCH.to_owned(),
            boundary: Some("run_end".to_owned()),
            boundary_outcome: Some("completed".to_owned()),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(moot.state, HIJACK_FINISHED_FIRST);
    assert_eq!(moot.boundary_outcome.as_deref(), Some("completed"));

    assert!(advance_agent_hijack_record(
        &latched,
        &event(HIJACK_EVENT_FINISHED_FIRST)
    )
    .is_err());
    let finished_first = advance_agent_hijack_record(
        &latched,
        &AgentHijackEventWire {
            event: HIJACK_EVENT_FINISHED_FIRST.to_owned(),
            boundary: Some("run_end".to_owned()),
            boundary_outcome: Some("completed".to_owned()),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(finished_first.state, HIJACK_FINISHED_FIRST);
    assert!(advance_agent_hijack_record(
        &requested,
        &event(HIJACK_EVENT_APPLY_LATCH)
    )
    .is_err());
    assert!(advance_agent_hijack_record(
        &latched,
        &event(HIJACK_EVENT_CONFIRMED_STOP)
    )
    .is_err());
    assert!(advance_agent_hijack_record(
        &requested,
        &event(HIJACK_EVENT_LATCH)
    )
    .is_err());
}

#[test]
fn legacy_hijack_records_default_new_latch_fields() {
    let legacy = r#"{"schema_version":1,"hijack_id":"old","agent_name":"agent.old","state":"held"}"#;
    let record: AgentHijackRecordWire = serde_json::from_str(legacy).unwrap();
    assert_eq!(record.schema_version, 1);
    assert_eq!(record.latched_at, None);
    assert_eq!(record.applied_at, None);
    assert_eq!(record.boundary, None);
    assert_eq!(record.boundary_outcome, None);
    assert_eq!(record.suppressed_followup, None);
    assert!(!record.forced);
    assert_eq!(record.owner_pid, None);
    assert_eq!(record.owner_identity, None);
}

fn resolution(state: &str, action: &str) -> AgentHijackRecordWire {
    AgentHijackRecordWire {
        resolution_action: Some(action.to_string()),
        resolution_origin_state: Some(state.to_string()),
        ..record(HIJACK_RESOLVING)
    }
}

#[test]
fn wire_schema_and_done_statuses_are_stable() {
    assert_eq!(agent_hijack_wire_schema_version(), 1);
    assert_eq!(
        agent_hijack_status_for_done_marker(Some("hijacked"), None),
        Some(HIJACKED_STATUS)
    );
    assert_eq!(
        agent_hijack_status_for_done_marker(
            Some("hijacked"),
            Some(&AgentRecoveryWire {
                state: Some("pending".to_string()),
                ..Default::default()
            }),
        ),
        Some(HIJACKED_STATUS)
    );
    assert_eq!(
        agent_hijack_status_for_done_marker(
            Some("hijacked"),
            Some(&AgentRecoveryWire {
                state: Some("expired".to_string()),
                ..Default::default()
            }),
        ),
        Some(HIJACKED_STATUS)
    );
    assert_eq!(
        agent_hijack_status_for_done_marker(
            Some("hijacked"),
            Some(&AgentRecoveryWire {
                state: Some("resolved".to_string()),
                model_changed: Some(true),
                ..Default::default()
            }),
        ),
        Some(ESCALATED_STATUS)
    );
    assert_eq!(
        agent_hijack_status_for_done_marker(
            Some("hijacked"),
            Some(&AgentRecoveryWire {
                state: Some("resolved".to_string()),
                model_changed: Some(false),
                ..Default::default()
            }),
        ),
        Some(CONTINUED_STATUS)
    );
    assert_eq!(
        agent_hijack_status_for_done_marker(
            Some("hijacked"),
            Some(&AgentRecoveryWire {
                state: Some("dismissed".to_string()),
                ..Default::default()
            }),
        ),
        Some(INTERRUPTED_STATUS)
    );
    assert_eq!(
        agent_hijack_status_for_done_marker(Some("failed"), None),
        None
    );
}

#[test]
fn status_rows_keep_their_archive_buckets() {
    for status in [HIJACKED_STATUS, INTERRUPTED_STATUS] {
        assert_eq!(
            crate::agent_archive::status_bucket_for_status(status),
            "Stopped"
        );
        assert_eq!(
            crate::agent_archive::archive_outcome_for_status(status),
            "interrupted"
        );
    }
    for status in [ESCALATED_STATUS, CONTINUED_STATUS] {
        assert_eq!(
            crate::agent_archive::status_bucket_for_status(status),
            "Running"
        );
        assert_eq!(
            crate::agent_archive::archive_outcome_for_status(status),
            "interrupted"
        );
    }
}

#[test]
fn ledger_accepts_each_lifecycle_transition_and_bumps_revision() {
    let requested = record(HIJACK_REQUESTED);
    let stopping = advance_agent_hijack_record(
        &requested,
        &event(HIJACK_EVENT_BEGIN_STOP),
    )
    .unwrap();
    assert_eq!(stopping.state, HIJACK_STOPPING);
    assert_eq!(stopping.revision, 1);

    let finished = advance_agent_hijack_record(
        &requested,
        &event(HIJACK_EVENT_FINISHED_FIRST),
    )
    .unwrap();
    assert_eq!(finished.state, HIJACK_FINISHED_FIRST);

    let mut confirmed = event(HIJACK_EVENT_CONFIRMED_STOP);
    confirmed.at = Some("2026-11-02T12:00:00Z".to_string());
    let held = advance_agent_hijack_record(&stopping, &confirmed).unwrap();
    assert_eq!(held.state, HIJACK_HELD);
    assert_eq!(held.revision, 2);
    assert_eq!(held.updated_at.as_deref(), confirmed.at.as_deref());

    let mut failed = event(HIJACK_EVENT_STOP_FAILED);
    failed.error = Some("survivor pid 3".to_string());
    let stop_failed = advance_agent_hijack_record(&stopping, &failed).unwrap();
    assert_eq!(stop_failed.state, HIJACK_STOP_FAILED);
    assert_eq!(stop_failed.error.as_deref(), Some("survivor pid 3"));

    let expired =
        advance_agent_hijack_record(&held, &event(HIJACK_EVENT_EXPIRE))
            .unwrap();
    assert_eq!(expired.state, HIJACK_EXPIRED);
}

#[test]
fn held_resolution_actions_settle_once_and_view_returns_to_hold() {
    for action in [
        HIJACK_ACTION_ESCALATE,
        HIJACK_ACTION_CONTINUE,
        HIJACK_ACTION_INSTRUCT,
        HIJACK_ACTION_RESTART,
        HIJACK_ACTION_EDIT,
        HIJACK_ACTION_DISMISS,
    ] {
        let mut begin = event(HIJACK_EVENT_BEGIN_RESOLUTION);
        begin.action = Some(action.to_string());
        begin.reserved_successor_name = Some("agent.2".to_string());
        begin.concrete_target_model = Some("openai/gpt@xhigh".to_string());
        begin.model_changed = Some(true);
        let resolving =
            advance_agent_hijack_record(&record(HIJACK_HELD), &begin).unwrap();
        assert_eq!(resolving.state, HIJACK_RESOLVING, "{action}");
        assert_eq!(resolving.resolution_action.as_deref(), Some(action));
        assert_eq!(
            resolving.reserved_successor_name.as_deref(),
            Some("agent.2")
        );
        let completed = advance_agent_hijack_record(
            &resolving,
            &event(HIJACK_EVENT_COMPLETE_RESOLUTION),
        )
        .unwrap();
        let expected = if action == HIJACK_ACTION_DISMISS {
            HIJACK_DISMISSED
        } else {
            HIJACK_RESOLVED
        };
        assert_eq!(completed.state, expected, "{action}");
        assert_eq!(completed.revision, 2, "{action}");
    }

    let mut begin_view = event(HIJACK_EVENT_BEGIN_RESOLUTION);
    begin_view.action = Some(HIJACK_ACTION_VIEW.to_string());
    let resolving =
        advance_agent_hijack_record(&record(HIJACK_HELD), &begin_view).unwrap();
    let viewed = advance_agent_hijack_record(
        &resolving,
        &event(HIJACK_EVENT_COMPLETE_RESOLUTION),
    )
    .unwrap();
    assert_eq!(viewed.state, HIJACK_HELD);
    assert_eq!(viewed.resolution_action, None);

    let mut failed = event(HIJACK_EVENT_RESOLUTION_FAILED);
    failed.error = Some("target disappeared".to_string());
    let returned = advance_agent_hijack_record(
        &resolution(HIJACK_HELD, HIJACK_ACTION_RESTART),
        &failed,
    )
    .unwrap();
    assert_eq!(returned.state, HIJACK_HELD);
    assert_eq!(returned.error.as_deref(), Some("target disappeared"));
}

#[test]
fn expired_records_allow_release_and_view_but_not_keep_work() {
    for action in [
        HIJACK_ACTION_RESTART,
        HIJACK_ACTION_EDIT,
        HIJACK_ACTION_VIEW,
        HIJACK_ACTION_DISMISS,
    ] {
        let mut begin = event(HIJACK_EVENT_BEGIN_RESOLUTION);
        begin.action = Some(action.to_string());
        let resolving =
            advance_agent_hijack_record(&record(HIJACK_EXPIRED), &begin)
                .unwrap();
        assert_eq!(resolving.state, HIJACK_RESOLVING, "{action}");
    }
    for action in [
        HIJACK_ACTION_ESCALATE,
        HIJACK_ACTION_CONTINUE,
        HIJACK_ACTION_INSTRUCT,
    ] {
        let mut begin = event(HIJACK_EVENT_BEGIN_RESOLUTION);
        begin.action = Some(action.to_string());
        assert!(advance_agent_hijack_record(&record(HIJACK_EXPIRED), &begin)
            .is_err());
    }
    let expired_view = AgentHijackRecordWire {
        resolution_action: Some(HIJACK_ACTION_VIEW.to_string()),
        resolution_origin_state: Some(HIJACK_EXPIRED.to_string()),
        ..record(HIJACK_RESOLVING)
    };
    let viewed = advance_agent_hijack_record(
        &expired_view,
        &event(HIJACK_EVENT_COMPLETE_RESOLUTION),
    )
    .unwrap();
    assert_eq!(viewed.state, HIJACK_EXPIRED);
}

#[test]
fn ledger_rejects_illegal_events_bad_actions_and_stale_schemas() {
    assert!(advance_agent_hijack_record(
        &record(HIJACK_REQUESTED),
        &event(HIJACK_EVENT_CONFIRMED_STOP),
    )
    .is_err());
    assert!(advance_agent_hijack_record(
        &record(HIJACK_HELD),
        &event(HIJACK_EVENT_BEGIN_RESOLUTION),
    )
    .is_err());
    let mut bad_action = event(HIJACK_EVENT_BEGIN_RESOLUTION);
    bad_action.action = Some("unknown".to_string());
    assert!(
        advance_agent_hijack_record(&record(HIJACK_HELD), &bad_action).is_err()
    );
    let mut stale = record(HIJACK_HELD);
    stale.schema_version = 2;
    assert!(
        advance_agent_hijack_record(&stale, &event(HIJACK_EVENT_EXPIRE))
            .is_err()
    );
}

fn eligible_facts() -> AgentHijackEligibilityFactsWire {
    AgentHijackEligibilityFactsWire {
        schema_version: AGENT_HIJACK_WIRE_SCHEMA_VERSION,
        is_local: true,
        is_llm_agent: true,
        workspace_num: Some(4),
        status: Some("RUNNING".to_string()),
        lifecycle_phase: Some("provider_running".to_string()),
        has_hijack_capability: true,
        pid: Some(42),
        ..Default::default()
    }
}

#[test]
fn eligibility_accepts_local_capable_agent_turns() {
    for status in ["RUNNING", "STARTING", "RETRYING"] {
        let mut facts = eligible_facts();
        facts.status = Some(status.to_string());
        let decision = agent_hijack_eligibility(&facts).unwrap();
        assert!(decision.eligible, "{status}");
        assert_eq!(decision.code, None);
        assert_eq!(decision.message, None);
    }
}

#[test]
fn eligibility_reports_every_refusal_case() {
    let mut cases = Vec::new();
    for phase in ["declaring", "executing", "finalizing"] {
        let mut facts = eligible_facts();
        facts.lifecycle_phase = Some(phase.to_string());
        cases.push((facts, "finalizing"));
    }
    for (field, expected) in [
        ("monitor", "monitor_turn"),
        ("gate", "gate_turn"),
        ("proc", "named_proc"),
        ("home", "home_mode"),
        ("remote", "remote_row"),
        ("clan", "clan_container"),
        ("bulk", "bulk_mark"),
    ] {
        let mut facts = eligible_facts();
        match field {
            "monitor" => facts.is_monitor_turn = true,
            "gate" => facts.is_gate_turn = true,
            "proc" => facts.is_named_proc = true,
            "home" => facts.is_home_mode = true,
            "remote" => facts.is_remote = true,
            "clan" => facts.is_clan_container = true,
            _ => facts.is_bulk = true,
        }
        cases.push((facts, expected));
    }
    let mut facts = eligible_facts();
    facts.is_local = false;
    cases.push((facts, "not_local_llm_agent"));
    let mut facts = eligible_facts();
    facts.is_llm_agent = false;
    cases.push((facts, "not_local_llm_agent"));
    let mut facts = eligible_facts();
    facts.has_hijack_capability = false;
    cases.push((facts, "missing_capability"));
    let mut facts = eligible_facts();
    facts.workspace_num = Some(0);
    cases.push((facts, "unnumbered_workspace"));
    let mut facts = eligible_facts();
    facts.status = Some("WAITING".to_string());
    cases.push((facts, "ineligible_status"));
    let mut facts = eligible_facts();
    facts.pid = None;
    cases.push((facts, "missing_pid"));

    for (facts, expected) in cases {
        let decision = agent_hijack_eligibility(&facts).unwrap();
        assert!(!decision.eligible, "{expected}");
        assert_eq!(decision.code.as_deref(), Some(expected));
        assert!(decision
            .message
            .as_deref()
            .is_some_and(|message| !message.is_empty()));
    }
}

#[test]
fn eligibility_rejects_unsupported_schema() {
    let mut facts = eligible_facts();
    facts.schema_version += 1;
    assert!(agent_hijack_eligibility(&facts).is_err());
}

#[test]
fn old_v1_records_round_trip_without_recovery_identity() {
    let json =
        r#"{"schema_version":1,"state":"held","revision":3,"hijack_id":"h-1"}"#;
    let record: AgentHijackRecordWire = serde_json::from_str(json).unwrap();
    assert_eq!(record.state, HIJACK_HELD);
    assert_eq!(record.revision, 3);
    assert!(record.resolver_pid.is_none());
    assert!(record.resolver_process_identity_token.is_none());
    assert!(record.launch_identity.is_none());
    assert!(record.admission_request_id.is_none());
    let encoded = serde_json::to_value(&record).unwrap();
    let again: AgentHijackRecordWire = serde_json::from_value(encoded).unwrap();
    assert_eq!(again, record);
}

#[test]
fn selected_model_and_reservation_survive_launch_progress_and_complete() {
    let mut begin = event(HIJACK_EVENT_BEGIN_RESOLUTION);
    begin.action = Some(HIJACK_ACTION_ESCALATE.to_string());
    begin.reserved_successor_name = Some("agent.2".to_string());
    begin.concrete_target_model = Some("openai/gpt@xhigh".to_string());
    begin.model_changed = Some(true);
    begin.resolver_pid = Some(77);
    begin.resolver_process_identity_token = Some("boot:1".to_string());
    let resolving =
        advance_agent_hijack_record(&record(HIJACK_HELD), &begin).unwrap();
    assert_eq!(resolving.state, HIJACK_RESOLVING);
    assert_eq!(resolving.resolver_pid, Some(77));
    assert_eq!(
        resolving.resolver_process_identity_token.as_deref(),
        Some("boot:1")
    );
    assert_eq!(
        resolving.reserved_successor_name.as_deref(),
        Some("agent.2")
    );
    assert_eq!(
        resolving.concrete_target_model.as_deref(),
        Some("openai/gpt@xhigh")
    );

    let mut progress = event(HIJACK_EVENT_RECORD_LAUNCH);
    progress.launch_identity = Some("launch-abc".to_string());
    progress.admission_request_id = Some("req-1".to_string());
    progress.admission_identity = Some("agent.2".to_string());
    progress.launch_stage = Some(HIJACK_LAUNCH_QUEUED.to_string());
    let queued = advance_agent_hijack_record(&resolving, &progress).unwrap();
    assert_eq!(queued.state, HIJACK_RESOLVING);
    assert_eq!(queued.revision, resolving.revision + 1);
    assert_eq!(queued.launch_identity.as_deref(), Some("launch-abc"));
    assert_eq!(queued.launch_stage.as_deref(), Some(HIJACK_LAUNCH_QUEUED));
    assert_eq!(
        queued.concrete_target_model.as_deref(),
        Some("openai/gpt@xhigh")
    );
    assert_eq!(queued.reserved_successor_name.as_deref(), Some("agent.2"));
    assert_eq!(queued.resolver_pid, Some(77));

    let mut confirm = event(HIJACK_EVENT_RECORD_LAUNCH);
    confirm.launch_stage = Some(HIJACK_LAUNCH_CONFIRMED.to_string());
    confirm.successor_pid = Some(101);
    confirm.successor_process_identity_token = Some("boot:9".to_string());
    confirm.successor_artifacts_dir = Some("/tmp/a".to_string());
    confirm.inheritance_acknowledged = Some(true);
    let confirmed = advance_agent_hijack_record(&queued, &confirm).unwrap();
    assert_eq!(confirmed.state, HIJACK_RESOLVING);
    assert_eq!(confirmed.launch_identity.as_deref(), Some("launch-abc"));
    assert!(confirmed.inheritance_acknowledged.unwrap());

    let completed = advance_agent_hijack_record(
        &confirmed,
        &event(HIJACK_EVENT_COMPLETE_RESOLUTION),
    )
    .unwrap();
    assert_eq!(completed.state, HIJACK_RESOLVED);
    assert_eq!(
        completed.concrete_target_model.as_deref(),
        Some("openai/gpt@xhigh")
    );
    assert_eq!(
        completed.reserved_successor_name.as_deref(),
        Some("agent.2")
    );
    assert_eq!(completed.launch_identity.as_deref(), Some("launch-abc"));
    assert_eq!(completed.resolver_pid, Some(77));
}

#[test]
fn record_launch_is_illegal_outside_resolving_and_failure_clears_launch_evidence(
) {
    let mut progress = event(HIJACK_EVENT_RECORD_LAUNCH);
    progress.launch_identity = Some("launch-abc".to_string());
    assert!(
        advance_agent_hijack_record(&record(HIJACK_HELD), &progress).is_err()
    );

    let mut begin = event(HIJACK_EVENT_BEGIN_RESOLUTION);
    begin.action = Some(HIJACK_ACTION_CONTINUE.to_string());
    begin.reserved_successor_name = Some("agent.2".to_string());
    begin.concrete_target_model = Some("claude/opus@high".to_string());
    begin.resolver_pid = Some(5);
    begin.launch_identity = Some("launch-abc".to_string());
    let resolving =
        advance_agent_hijack_record(&record(HIJACK_HELD), &begin).unwrap();
    let mut failed = event(HIJACK_EVENT_RESOLUTION_FAILED);
    failed.error = Some("spawn failed".to_string());
    let held = advance_agent_hijack_record(&resolving, &failed).unwrap();
    assert_eq!(held.state, HIJACK_HELD);
    assert!(held.launch_identity.is_none());
    assert!(held.resolver_pid.is_none());
    assert_eq!(held.error.as_deref(), Some("spawn failed"));
    assert_eq!(
        held.concrete_target_model.as_deref(),
        Some("claude/opus@high")
    );
}

#[test]
fn eligibility_wire_schema_is_split_from_the_ledger_schema() {
    assert_eq!(agent_hijack_wire_schema_version(), 1);
    assert_eq!(super::agent_hijack_eligibility_wire_schema_version(), 2);
    assert_eq!(AGENT_HIJACK_ELIGIBILITY_WIRE_SCHEMA_VERSION, 2);
}

fn facts_v2() -> AgentHijackEligibilityFactsV2Wire {
    AgentHijackEligibilityFactsV2Wire {
        schema_version: AGENT_HIJACK_ELIGIBILITY_WIRE_SCHEMA_VERSION,
        is_local: true,
        occupying_kind: HIJACK_OCCUPANT_LLM.to_string(),
        status: Some("RUNNING".to_string()),
        workspace_num: Some(4),
        has_numbered_claim: true,
        pid: Some(42),
        process_alive: true,
        has_hijack_capability: true,
        successor_attachable: true,
        ..Default::default()
    }
}

fn decide(
    facts: &AgentHijackEligibilityFactsV2Wire,
) -> AgentHijackEligibilityDecisionV2Wire {
    super::agent_hijack_decide(facts).unwrap()
}

#[test]
fn decide_accepts_a_live_capable_llm_occupant() {
    for status in ["RUNNING", "STARTING", "RETRYING", "running"] {
        let mut facts = facts_v2();
        facts.status = Some(status.to_string());
        let decision = decide(&facts);
        assert_eq!(decision.mode, HIJACK_DECISION_IMMEDIATE, "{status}");
        assert!(decision.eligible, "{status}");
        assert_eq!(decision.adapter.as_deref(), Some(HIJACK_ADAPTER_LLM));
        assert_eq!(decision.code, None);
        assert_eq!(decision.message, None);
        assert_eq!(decision.next_action, None);
        assert_eq!(decision.redirect_op, None);
    }
}

#[test]
fn decide_pre_provider_turns_select_the_live_or_processless_adapter() {
    for (kind, status, phase) in [
        (HIJACK_OCCUPANT_WAIT, "WAITING", "waiting"),
        (HIJACK_OCCUPANT_QUEUE, "QUEUED", "waiting"),
        (HIJACK_OCCUPANT_LLM, "STARTING", "booting"),
        (HIJACK_OCCUPANT_LLM, "LOST", "preparing"),
    ] {
        for alive in [true, false] {
            let mut facts = facts_v2();
            facts.occupying_kind = kind.to_owned();
            facts.status = Some(status.to_owned());
            facts.turn_phase = Some(phase.to_owned());
            facts.process_alive = alive;
            facts.has_hijack_capability = alive;
            facts.pid = alive.then_some(42);
            let decision = decide(&facts);
            assert_eq!(decision.mode, HIJACK_DECISION_IMMEDIATE);
            assert!(decision.eligible);
            assert_eq!(
                decision.adapter.as_deref(),
                Some(if alive {
                    HIJACK_ADAPTER_LLM
                } else {
                    HIJACK_ADAPTER_PROCESSLESS
                })
            );
        }
    }
}

#[test]
fn decide_pre_provider_live_runner_still_requires_safe_signalling_facts() {
    let mut facts = facts_v2();
    facts.occupying_kind = HIJACK_OCCUPANT_WAIT.to_owned();
    facts.status = Some("WAITING".to_owned());
    facts.has_hijack_capability = false;
    assert_eq!(decide(&facts).code.as_deref(), Some("missing_capability"));
    facts.has_hijack_capability = true;
    facts.pid = None;
    assert_eq!(decide(&facts).code.as_deref(), Some("missing_pid"));
    facts.pid = Some(42);
    facts.successor_attachable = false;
    assert_eq!(decide(&facts).code.as_deref(), Some("no_session"));
}

#[test]
fn decide_reports_every_redirect() {
    let mut cases = Vec::new();

    let mut facts = facts_v2();
    facts.is_remote = true;
    facts.is_local = false;
    cases.push((facts, "remote_row", HIJACK_REDIRECT_COPY_REMOTE_COMMAND));

    let mut facts = facts_v2();
    facts.is_clan_container = true;
    cases.push((facts, "clan_container", HIJACK_REDIRECT_SELECT_MEMBER));

    let mut facts = facts_v2();
    facts.done_outcome = Some("hijacked".to_string());
    cases.push((facts, "already_hijacked", HIJACK_REDIRECT_OPEN_RECOVERY));

    for state in ["requested", "stopping", "stop_blocked", "held", "resolving"]
    {
        let mut facts = facts_v2();
        facts.active_hijack_state = Some(state.to_string());
        cases.push((
            facts,
            "active_hijack",
            HIJACK_REDIRECT_SHOW_ACTIVE_HIJACK,
        ));
    }

    let mut facts = facts_v2();
    facts.occupying_kind = HIJACK_OCCUPANT_GATE.to_string();
    facts.gate_state = Some("pending".to_string());
    cases.push((facts, "gate_pending", HIJACK_REDIRECT_GATE_REVIEW));

    for kind in [HIJACK_OCCUPANT_WAIT, HIJACK_OCCUPANT_QUEUE] {
        let mut facts = facts_v2();
        facts.occupying_kind = kind.to_owned();
        facts.has_numbered_claim = false;
        facts.workspace_num = None;
        cases.push((facts, "nothing_to_keep", HIJACK_REDIRECT_KILL_AND_EDIT));
    }

    for (facts, expected_code, expected_op) in cases {
        let decision = decide(&facts);
        assert_eq!(decision.mode, HIJACK_DECISION_REDIRECT, "{expected_code}");
        assert!(!decision.eligible, "{expected_code}");
        assert_eq!(decision.code.as_deref(), Some(expected_code));
        assert_eq!(
            decision.redirect_op.as_deref(),
            Some(expected_op),
            "{expected_code}"
        );
        assert!(
            decision
                .message
                .as_deref()
                .is_some_and(|message| !message.is_empty()),
            "{expected_code}"
        );
        assert!(
            decision
                .next_action
                .as_deref()
                .is_some_and(|action| !action.is_empty()),
            "{expected_code}"
        );
        assert_eq!(decision.adapter, None, "{expected_code}");
    }
}

#[test]
fn decide_reports_every_refusal_code() {
    let mut cases = Vec::new();

    let mut facts = facts_v2();
    facts.is_bulk = true;
    cases.push((facts, "bulk_mark"));

    let mut facts = facts_v2();
    facts.is_named_proc = true;
    cases.push((facts, "named_proc"));

    let mut facts = facts_v2();
    facts.is_home_mode = true;
    cases.push((facts, "home_mode"));

    let mut facts = facts_v2();
    facts.is_local = false;
    cases.push((facts, "not_local_llm_agent"));

    let mut facts = facts_v2();
    facts.done_outcome = Some("done".to_string());
    cases.push((facts, "already_completed"));

    for gate_state in ["settling", "executing", "", "done"] {
        let mut facts = facts_v2();
        facts.occupying_kind = HIJACK_OCCUPANT_GATE.to_string();
        facts.gate_state = Some(gate_state.to_string());
        cases.push((facts, "gate_executing"));
    }
    let mut facts = facts_v2();
    facts.occupying_kind = HIJACK_OCCUPANT_GATE.to_string();
    cases.push((facts, "gate_executing"));

    for kind in [HIJACK_OCCUPANT_WAIT, HIJACK_OCCUPANT_QUEUE] {
        let mut facts = facts_v2();
        facts.occupying_kind = kind.to_string();
        facts.turn_phase = Some("provider_running".to_owned());
        cases.push((facts, "ineligible_status"));
    }

    let mut facts = facts_v2();
    facts.occupying_kind = HIJACK_OCCUPANT_WORKFLOW_STEP.to_string();
    cases.push((facts, "workflow_step"));

    let mut facts = facts_v2();
    facts.occupying_kind = HIJACK_OCCUPANT_NONE.to_string();
    cases.push((facts, "nothing_to_keep"));

    let mut facts = facts_v2();
    facts.occupying_kind = "bogus".to_string();
    cases.push((facts, "unknown_occupant"));

    let mut facts = facts_v2();
    facts.successor_attachable = false;
    cases.push((facts, "no_session"));

    let mut facts = facts_v2();
    facts.has_hijack_capability = false;
    cases.push((facts, "missing_capability"));

    let mut facts = facts_v2();
    facts.workspace_num = Some(0);
    cases.push((facts, "unnumbered_workspace"));
    let mut facts = facts_v2();
    facts.has_numbered_claim = false;
    cases.push((facts, "unnumbered_workspace"));

    let mut facts = facts_v2();
    facts.status = Some("WAITING".to_string());
    facts.turn_phase = Some("provider_running".to_owned());
    cases.push((facts, "ineligible_status"));

    let mut facts = facts_v2();
    facts.pid = None;
    cases.push((facts, "missing_pid"));
    let mut facts = facts_v2();
    facts.process_alive = false;
    cases.push((facts, "missing_pid"));

    for (facts, expected) in cases {
        let decision = decide(&facts);
        assert_eq!(decision.mode, HIJACK_DECISION_REFUSED, "{expected}");
        assert!(!decision.eligible, "{expected}");
        assert_eq!(decision.code.as_deref(), Some(expected));
        assert_eq!(decision.redirect_op, None, "{expected}");
        assert_eq!(decision.adapter, None, "{expected}");
        assert!(
            decision
                .message
                .as_deref()
                .is_some_and(|message| !message.is_empty()),
            "{expected}"
        );
        assert!(
            decision
                .next_action
                .as_deref()
                .is_some_and(|action| !action.is_empty()),
            "{expected}"
        );
    }
}

#[test]
fn decide_latches_finalizing_and_staged_with_boundary_hint() {
    for (field, value) in [
        ("turn", "provider_done"),
        ("turn", "finalizing"),
        ("turn", "handoff"),
        ("turn", "FINALIZING"),
        ("finalizer", "declaring"),
        ("finalizer", "executing"),
    ] {
        let mut facts = facts_v2();
        if field == "turn" {
            facts.turn_phase = Some(value.to_string());
        } else {
            facts.finalizer_phase = Some(value.to_string());
        }
        let decision = decide(&facts);
        assert_eq!(decision.mode, HIJACK_DECISION_LATCHED, "{value}");
        assert!(decision.eligible, "{value}");
        assert_eq!(decision.code.as_deref(), Some("finalizing"), "{value}");
        assert_eq!(decision.adapter, None, "{value}");
        assert_eq!(decision.boundary.as_deref(), Some("finalizer"), "{value}");
        assert!(
            decision
                .message
                .as_deref()
                .is_some_and(|message| !message.is_empty()),
            "{value}"
        );
        assert!(
            decision
                .next_action
                .as_deref()
                .is_some_and(|action| !action.is_empty()),
            "{value}"
        );
    }

    let mut staged = facts_v2();
    staged.has_staged_handoff = true;
    staged.staged_kind = Some("pipe".to_string());
    let decision = decide(&staged);
    assert_eq!(decision.mode, HIJACK_DECISION_LATCHED);
    assert!(decision.eligible);
    assert_eq!(decision.code.as_deref(), Some("staged_pending"));
    assert_eq!(decision.adapter, None);
    assert_eq!(decision.boundary.as_deref(), Some("staged_adoption"));

    // A done marker with a genuinely pending handoff is not a completed turn.
    let mut done_staged = facts_v2();
    done_staged.has_staged_handoff = true;
    done_staged.done_outcome = Some("succeeded".to_string());
    let decision = decide(&done_staged);
    assert_eq!(decision.mode, HIJACK_DECISION_LATCHED);
    assert_eq!(decision.code.as_deref(), Some("staged_pending"));

    // Without a staged payload the same done marker stays completed.
    let mut done = facts_v2();
    done.done_outcome = Some("succeeded".to_string());
    let decision = decide(&done);
    assert_eq!(decision.mode, HIJACK_DECISION_REFUSED);
    assert_eq!(decision.code.as_deref(), Some("already_completed"));

    // Locality, capability, numbered-custody, and successor constraints win.
    let mut unnumbered = facts_v2();
    unnumbered.turn_phase = Some("finalizing".to_string());
    unnumbered.workspace_num = Some(0);
    unnumbered.has_numbered_claim = false;
    let decision = decide(&unnumbered);
    assert_eq!(decision.mode, HIJACK_DECISION_REFUSED);
    assert_eq!(decision.code.as_deref(), Some("unnumbered_workspace"));

    let mut incapable = facts_v2();
    incapable.has_staged_handoff = true;
    incapable.has_hijack_capability = false;
    let decision = decide(&incapable);
    assert_eq!(decision.mode, HIJACK_DECISION_REFUSED);
    assert_eq!(decision.code.as_deref(), Some("missing_capability"));
}

#[test]
fn decide_accepts_a_running_monitor_with_the_monitor_adapter() {
    for status in ["RUNNING", "STARTING", "RETRYING", "running"] {
        let mut facts = facts_v2();
        facts.occupying_kind = HIJACK_OCCUPANT_MONITOR.to_string();
        facts.monitor_state = Some("running".to_string());
        facts.status = Some(status.to_string());
        facts.has_hijack_capability = false;
        facts.pid = None;
        facts.process_alive = false;
        let decision = decide(&facts);
        assert_eq!(decision.mode, HIJACK_DECISION_IMMEDIATE, "{status}");
        assert!(decision.eligible, "{status}");
        assert_eq!(
            decision.adapter.as_deref(),
            Some(HIJACK_ADAPTER_MONITOR),
            "{status}"
        );
        assert_eq!(decision.code, None, "{status}");
        assert_eq!(decision.boundary, None, "{status}");
    }
}

#[test]
fn decide_latches_pending_monitor_followup_before_already_completed() {
    let mut pending = facts_v2();
    pending.occupying_kind = HIJACK_OCCUPANT_MONITOR.to_string();
    pending.monitor_state = Some("completed".to_string());
    pending.monitor_followup_pending = true;
    pending.done_outcome = Some("succeeded".to_string());
    pending.status = Some("DONE".to_string());
    pending.has_hijack_capability = false;
    pending.pid = None;
    pending.process_alive = false;
    let decision = decide(&pending);
    assert_eq!(decision.mode, HIJACK_DECISION_LATCHED);
    assert!(decision.eligible);
    assert_eq!(decision.code.as_deref(), Some("monitor_followup_pending"));
    assert_eq!(decision.adapter, None);
    assert_eq!(decision.boundary.as_deref(), Some("monitor"));

    let mut unnumbered = pending.clone();
    unnumbered.has_numbered_claim = false;
    unnumbered.workspace_num = None;
    assert_eq!(
        decide(&unnumbered).code.as_deref(),
        Some("unnumbered_workspace")
    );

    let mut no_session = pending.clone();
    no_session.successor_attachable = false;
    assert_eq!(decide(&no_session).code.as_deref(), Some("no_session"));
}

#[test]
fn decide_refuses_delivered_and_unclaimed_monitors() {
    let mut delivered = facts_v2();
    delivered.occupying_kind = HIJACK_OCCUPANT_MONITOR.to_string();
    delivered.monitor_state = Some("completed".to_string());
    delivered.done_outcome = Some("succeeded".to_string());
    delivered.status = Some("DONE".to_string());
    let decision = decide(&delivered);
    assert_eq!(decision.mode, HIJACK_DECISION_REFUSED);
    assert_eq!(decision.code.as_deref(), Some("already_completed"));

    let mut unclaimed = facts_v2();
    unclaimed.occupying_kind = HIJACK_OCCUPANT_MONITOR.to_string();
    unclaimed.monitor_state = Some("running".to_string());
    unclaimed.has_numbered_claim = false;
    unclaimed.workspace_num = None;
    assert_eq!(
        decide(&unclaimed).code.as_deref(),
        Some("unnumbered_workspace")
    );

    let mut idle = facts_v2();
    idle.occupying_kind = HIJACK_OCCUPANT_MONITOR.to_string();
    idle.monitor_state = Some("completed".to_string());
    idle.status = Some("DONE".to_string());
    assert_eq!(decide(&idle).code.as_deref(), Some("ineligible_status"));
}

#[test]
fn decide_rejects_unsupported_schema() {
    let mut facts = facts_v2();
    facts.schema_version += 1;
    assert!(super::agent_hijack_decide(&facts).is_err());
}

#[test]
fn stop_blocked_keeps_custody_until_retry_or_explicit_abandon() {
    let stopping = advance_agent_hijack_record(
        &record(HIJACK_REQUESTED),
        &event(HIJACK_EVENT_BEGIN_STOP),
    )
    .unwrap();
    let mut blocked_event = event(HIJACK_EVENT_STOP_BLOCKED);
    blocked_event.error = Some("2 processes survived".to_string());
    let blocked =
        advance_agent_hijack_record(&stopping, &blocked_event).unwrap();
    assert_eq!(blocked.state, HIJACK_STOP_BLOCKED);
    assert_eq!(blocked.revision, stopping.revision + 1);
    assert_eq!(blocked.error.as_deref(), Some("2 processes survived"));

    let retried =
        advance_agent_hijack_record(&blocked, &event(HIJACK_EVENT_RETRY_STOP))
            .unwrap();
    assert_eq!(retried.state, HIJACK_STOPPING);

    let reblocked = advance_agent_hijack_record(
        &retried,
        &event(HIJACK_EVENT_STOP_BLOCKED),
    )
    .unwrap();
    assert_eq!(reblocked.state, HIJACK_STOP_BLOCKED);

    let mut abandon = event(HIJACK_EVENT_ABANDON_STOP);
    abandon.error = Some("operator released the claim".to_string());
    let failed = advance_agent_hijack_record(&reblocked, &abandon).unwrap();
    assert_eq!(failed.state, HIJACK_STOP_FAILED);
    assert_eq!(failed.error.as_deref(), Some("operator released the claim"));
}

#[test]
fn stop_blocked_transitions_reject_foreign_states_and_events() {
    assert!(advance_agent_hijack_record(
        &record(HIJACK_REQUESTED),
        &event(HIJACK_EVENT_STOP_BLOCKED),
    )
    .is_err());
    assert!(advance_agent_hijack_record(
        &record(HIJACK_STOP_BLOCKED),
        &event(HIJACK_EVENT_CONFIRMED_STOP),
    )
    .is_err());
    assert!(advance_agent_hijack_record(
        &record(HIJACK_STOPPING),
        &event(HIJACK_EVENT_RETRY_STOP),
    )
    .is_err());
    assert!(advance_agent_hijack_record(
        &record(HIJACK_HELD),
        &event(HIJACK_EVENT_ABANDON_STOP),
    )
    .is_err());
}

#[test]
fn v1_records_without_the_new_fields_still_advance() {
    let json = r#"{"schema_version":1,"state":"requested","revision":0,"hijack_id":"h-9"}"#;
    let parsed: AgentHijackRecordWire = serde_json::from_str(json).unwrap();
    assert_eq!(parsed.target_key, "");
    assert_eq!(parsed.scope, None);
    let stopping = advance_agent_hijack_record(
        &parsed,
        &AgentHijackEventWire {
            event: HIJACK_EVENT_BEGIN_STOP.to_string(),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(stopping.state, HIJACK_STOPPING);
    assert_eq!(stopping.scope, None);
}

fn occupant_member(name: &str, order: i64) -> AgentHijackOccupantFactsWire {
    AgentHijackOccupantFactsWire {
        name: name.to_string(),
        order,
        ..Default::default()
    }
}

fn occupant_request(
    members: Vec<AgentHijackOccupantFactsWire>,
) -> AgentHijackOccupantSelectionRequestWire {
    AgentHijackOccupantSelectionRequestWire {
        schema_version: AGENT_HIJACK_ELIGIBILITY_WIRE_SCHEMA_VERSION,
        members,
    }
}

fn select(
    members: Vec<AgentHijackOccupantFactsWire>,
) -> AgentHijackOccupantSelectionWire {
    super::agent_hijack_select_occupant(&occupant_request(members)).unwrap()
}

#[test]
fn occupant_selection_prefers_claim_holder_then_newest_then_earliest() {
    let mut older = occupant_member("s.t.1", 1);
    older.in_flight = true;
    let mut newer = occupant_member("s.t.2", 2);
    newer.in_flight = true;
    newer.holds_claim = true;
    let selection = select(vec![older, newer]);
    assert_eq!(selection.selected_index, Some(1));
    assert!(selection.reason.contains("s.t.2"));
    assert!(selection.reason.contains("claim"));

    let mut older = occupant_member("s.t.1", 1);
    older.in_flight = true;
    let mut newer = occupant_member("s.t.2", 2);
    newer.in_flight = true;
    let selection = select(vec![older, newer]);
    assert_eq!(selection.selected_index, Some(1));
    assert!(selection.reason.contains("s.t.2"));
    assert!(selection.reason.contains("newest"));

    let mut first = occupant_member("s.t.1", 1);
    first.waiting = true;
    let mut second = occupant_member("s.t.2", 2);
    second.waiting = true;
    let selection = select(vec![first, second]);
    assert_eq!(selection.selected_index, Some(0));
    assert!(selection.reason.contains("s.t.1"));
    assert!(selection.reason.contains("waiting"));

    let mut live = occupant_member("s.t.1", 1);
    live.waiting = true;
    let mut settled = occupant_member("s.t.2", 2);
    settled.settled = true;
    settled.in_flight = true;
    let selection = select(vec![live, settled]);
    assert_eq!(selection.selected_index, Some(0));

    let mut settled = occupant_member("s.t.9", 9);
    settled.settled = true;
    let selection = select(vec![settled]);
    assert_eq!(selection.selected_index, None);

    let selection = select(Vec::new());
    assert_eq!(selection.selected_index, None);
}

#[test]
fn occupant_selection_rejects_unsupported_schema() {
    let mut request = occupant_request(vec![occupant_member("s.t.1", 1)]);
    request.schema_version += 1;
    assert!(super::agent_hijack_select_occupant(&request).is_err());
}
