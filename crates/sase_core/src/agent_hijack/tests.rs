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
