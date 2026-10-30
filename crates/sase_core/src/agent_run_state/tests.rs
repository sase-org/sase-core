use super::*;

fn identity(run_id: &str) -> AgentRunIdentityWire {
    AgentRunIdentityWire {
        project: "project-a".to_string(),
        owner: "alice@athena".to_string(),
        global_name: "alice.athena.worker".to_string(),
        source_run_id: run_id.to_string(),
    }
}

fn resolve_request(
    published_state: &str,
    evidence_kind: &str,
    evidence_state: Option<&str>,
) -> AgentRunStateResolveRequestWire {
    AgentRunStateResolveRequestWire {
        schema_version: AGENT_RUN_STATE_WIRE_SCHEMA_VERSION,
        identity: identity("run-1"),
        published_state: published_state.to_string(),
        published_started_at: None,
        published_finished_at: None,
        evidence_kind: evidence_kind.to_string(),
        evidence_identity: Some(identity("run-1")),
        evidence_state: evidence_state.map(str::to_string),
        evidence_started_at: Some("2026-10-29T12:00:00Z".to_string()),
        evidence_finished_at: Some("2026-10-29T12:01:00Z".to_string()),
    }
}

fn window_row(hood: &str, run: &str) -> AgentRunStateWindowRowWire {
    AgentRunStateWindowRowWire {
        local_hood: hood.to_string(),
        source_run_id: run.to_string(),
    }
}

fn window_request(
    rows: Vec<AgentRunStateWindowRowWire>,
    limit: u32,
    after: Option<String>,
) -> AgentRunStateWindowRequestWire {
    AgentRunStateWindowRequestWire {
        schema_version: AGENT_RUN_STATE_WIRE_SCHEMA_VERSION,
        project: "project-a".to_string(),
        owner: "alice@athena".to_string(),
        limit,
        after,
        rows,
    }
}

fn project(
    project: &str,
    owner: &str,
    enabled: bool,
) -> AgentRunStateProjectWire {
    AgentRunStateProjectWire {
        identity: AgentRunStateProjectIdentityWire {
            project: project.to_string(),
            owner: owner.to_string(),
        },
        enabled,
        configured: true,
        cursor: None,
        failure_count: 0,
        retry_after_ms: 0,
    }
}

#[test]
fn resolves_completed_failed_stopped_and_dismissed_terminal_evidence() {
    for (state, kind) in [
        ("completed", AGENT_RUN_STATE_EVIDENCE_TERMINAL_MARKER),
        ("failed", AGENT_RUN_STATE_EVIDENCE_TERMINAL_MARKER),
        ("stopped", AGENT_RUN_STATE_EVIDENCE_TERMINAL_MARKER),
        ("dismissed", AGENT_RUN_STATE_EVIDENCE_DISMISSED),
    ] {
        let mut request = resolve_request("waiting", kind, Some(state));
        if state == "dismissed" {
            request.evidence_finished_at = None;
        }
        let response = resolve_agent_run_state(request).unwrap();
        assert_eq!(response.selected_state, state);
        assert!(response.changed);
    }
}

#[test]
fn a_done_marker_overrides_stale_waiting_metadata_and_repeats_are_unchanged() {
    let request = resolve_request(
        "waiting",
        AGENT_RUN_STATE_EVIDENCE_TERMINAL_MARKER,
        Some("completed"),
    );
    let response = resolve_agent_run_state(request.clone()).unwrap();
    assert_eq!(response.selected_state, "completed");
    assert_eq!(
        response.reason,
        AGENT_RUN_STATE_REASON_UPDATED_TERMINAL_MARKER
    );

    let repeated = resolve_agent_run_state(AgentRunStateResolveRequestWire {
        published_state: response.selected_state,
        published_started_at: response.selected_started_at,
        published_finished_at: response.selected_finished_at,
        ..request
    })
    .unwrap();
    assert!(!repeated.changed);
    assert_eq!(repeated.reason, AGENT_RUN_STATE_REASON_UNCHANGED);
}

#[test]
fn live_and_missing_evidence_never_infer_a_terminal_state() {
    let live =
        resolve_request("active", AGENT_RUN_STATE_EVIDENCE_UNFINISHED, None);
    let response = resolve_agent_run_state(live).unwrap();
    assert_eq!(response.selected_state, "active");
    assert_eq!(response.reason, AGENT_RUN_STATE_REASON_LIVE_EVIDENCE);

    let mut missing =
        resolve_request("waiting", AGENT_RUN_STATE_EVIDENCE_MISSING, None);
    missing.evidence_identity = None;
    let response = resolve_agent_run_state(missing).unwrap();
    assert_eq!(response.selected_state, "waiting");
    assert_eq!(response.reason, AGENT_RUN_STATE_REASON_MISSING_EVIDENCE);
}

#[test]
fn malformed_and_same_name_different_run_evidence_are_diagnostic() {
    let mut malformed = resolve_request(
        "active",
        AGENT_RUN_STATE_EVIDENCE_TERMINAL_MARKER,
        Some("running"),
    );
    malformed.evidence_finished_at = Some("not a timestamp".to_string());
    let response = resolve_agent_run_state(malformed).unwrap();
    assert_eq!(response.reason, AGENT_RUN_STATE_REASON_MALFORMED_EVIDENCE);
    assert!(!response.changed);

    let mut reused_name = resolve_request(
        "active",
        AGENT_RUN_STATE_EVIDENCE_TERMINAL_MARKER,
        Some("completed"),
    );
    reused_name.evidence_identity = Some(identity("different-run"));
    let response = resolve_agent_run_state(reused_name).unwrap();
    assert_eq!(response.reason, AGENT_RUN_STATE_REASON_IDENTITY_CONFLICT);
    assert_eq!(response.selected_state, "active");

    let mut wrong_live_identity =
        resolve_request("active", AGENT_RUN_STATE_EVIDENCE_UNFINISHED, None);
    wrong_live_identity.evidence_identity = Some(identity("different-run"));
    let response = resolve_agent_run_state(wrong_live_identity).unwrap();
    assert_eq!(response.reason, AGENT_RUN_STATE_REASON_IDENTITY_CONFLICT);
}

#[test]
fn terminal_state_resists_stale_live_observations_and_conflicting_outcomes() {
    let mut stale_live =
        resolve_request("completed", AGENT_RUN_STATE_EVIDENCE_UNFINISHED, None);
    stale_live.published_finished_at = Some("2026-10-29T12:01:00Z".to_string());
    let response = resolve_agent_run_state(stale_live).unwrap();
    assert_eq!(response.selected_state, "completed");
    assert_eq!(response.reason, AGENT_RUN_STATE_REASON_PRESERVED_TERMINAL);

    let mut conflict = resolve_request(
        "completed",
        AGENT_RUN_STATE_EVIDENCE_TERMINAL_MARKER,
        Some("failed"),
    );
    conflict.published_finished_at = Some("2026-10-29T12:01:00Z".to_string());
    let response = resolve_agent_run_state(conflict).unwrap();
    assert_eq!(response.selected_state, "completed");
    assert_eq!(response.reason, AGENT_RUN_STATE_REASON_CONFLICTING_TERMINAL);
}

#[test]
fn later_verified_dismissal_supersedes_a_terminal_outcome_for_the_same_run() {
    let mut request = resolve_request(
        "completed",
        AGENT_RUN_STATE_EVIDENCE_DISMISSED,
        Some("dismissed"),
    );
    request.published_finished_at = Some("2026-10-29T12:01:00Z".to_string());
    request.evidence_finished_at = Some("2026-10-29T12:02:00Z".to_string());
    let response = resolve_agent_run_state(request).unwrap();
    assert_eq!(response.selected_state, "dismissed");
    assert_eq!(response.reason, AGENT_RUN_STATE_REASON_LATER_DISMISSAL);
}

#[test]
fn supported_runtime_timestamp_formats_are_accepted_without_inventing_times() {
    let mut request = resolve_request(
        "waiting",
        AGENT_RUN_STATE_EVIDENCE_TERMINAL_MARKER,
        Some("failed"),
    );
    request.evidence_started_at = Some("1793275200".to_string());
    request.evidence_finished_at = Some("2026-10-29 12:01:00.250".to_string());
    let response = resolve_agent_run_state(request).unwrap();
    assert_eq!(response.selected_started_at.as_deref(), Some("1793275200"));
    assert_eq!(
        response.selected_finished_at.as_deref(),
        Some("2026-10-29 12:01:00.250")
    );

    let mut no_finish = resolve_request(
        "active",
        AGENT_RUN_STATE_EVIDENCE_TERMINAL_MARKER,
        Some("failed"),
    );
    no_finish.evidence_finished_at = None;
    assert_eq!(
        resolve_agent_run_state(no_finish)
            .unwrap()
            .selected_finished_at,
        None
    );
}

#[test]
fn window_orders_rows_and_advances_past_live_or_unresolvable_rows() {
    let first = select_agent_run_state_window(window_request(
        vec![window_row("hood-b", "run-2"), window_row("hood-a", "run-1")],
        1,
        None,
    ))
    .unwrap();
    assert_eq!(first.selected, vec![window_row("hood-a", "run-1")]);
    assert!(!first.pass_complete);
    let cursor = first.next_cursor.unwrap();

    // The first selected row can be deleted and an irreparable row inserted
    // before the cursor without making the pagination loop over either row.
    let second = select_agent_run_state_window(window_request(
        vec![
            window_row("hood-a", "run-0"),
            window_row("hood-b", "run-2"),
            window_row("hood-c", "run-3"),
        ],
        2,
        Some(cursor),
    ))
    .unwrap();
    assert_eq!(
        second.selected,
        vec![window_row("hood-b", "run-2"), window_row("hood-c", "run-3")]
    );
    assert!(second.pass_complete);
}

#[test]
fn window_cursors_are_bound_to_project_and_owner_and_limits_are_validated() {
    let first = select_agent_run_state_window(window_request(
        vec![window_row("hood-a", "run-1")],
        1,
        None,
    ))
    .unwrap();
    let cursor = first.next_cursor.unwrap();

    let mut wrong_owner = window_request(vec![], 10, Some(cursor.clone()));
    wrong_owner.owner = "bob@athena".to_string();
    assert!(select_agent_run_state_window(wrong_owner).is_err());

    let mut wrong_project = window_request(vec![], 10, Some(cursor));
    wrong_project.project = "project-b".to_string();
    assert!(select_agent_run_state_window(wrong_project).is_err());

    assert!(
        select_agent_run_state_window(window_request(vec![], 0, None)).is_err()
    );
    assert!(
        select_agent_run_state_window(window_request(vec![], 501, None))
            .is_err()
    );
    assert!(select_agent_run_state_window(window_request(
        vec![],
        10,
        Some("not-a-cursor".to_string()),
    ))
    .is_err());
}

#[test]
fn scheduler_rotates_projects_and_applies_bounded_exponential_backoff() {
    let projects = vec![
        project("a", "alice", true),
        project("b", "alice", true),
        project("c", "alice", true),
        project("disabled", "alice", false),
        {
            let mut project = project("unconfigured", "alice", true);
            project.configured = false;
            project
        },
    ];
    let initial =
        advance_agent_run_state_schedule(AgentRunStateScheduleRequestWire {
            schema_version: AGENT_RUN_STATE_WIRE_SCHEMA_VERSION,
            now_ms: 1_000,
            last_selected: None,
            projects: projects.clone(),
            outcome: None,
        })
        .unwrap();
    assert_eq!(initial.next_project.unwrap().project, "a");

    let failed_a =
        advance_agent_run_state_schedule(AgentRunStateScheduleRequestWire {
            schema_version: AGENT_RUN_STATE_WIRE_SCHEMA_VERSION,
            now_ms: 1_000,
            last_selected: Some(AgentRunStateProjectIdentityWire {
                project: "a".to_string(),
                owner: "alice".to_string(),
            }),
            projects,
            outcome: Some(AgentRunStateScheduleOutcomeWire {
                identity: AgentRunStateProjectIdentityWire {
                    project: "a".to_string(),
                    owner: "alice".to_string(),
                },
                result: AGENT_RUN_STATE_OUTCOME_FAILURE.to_string(),
                next_cursor: None,
                pass_complete: false,
            }),
        })
        .unwrap();
    assert_eq!(failed_a.next_project.unwrap().project, "b");
    let checkpoint = failed_a.updated_checkpoint.unwrap();
    assert_eq!(checkpoint.failure_count, 1);
    assert_eq!(checkpoint.retry_after_ms, 61_000);

    let mut retrying = project("a", "alice", true);
    retrying.failure_count = 1;
    retrying.retry_after_ms = 61_000;
    let second_failure =
        advance_agent_run_state_schedule(AgentRunStateScheduleRequestWire {
            schema_version: AGENT_RUN_STATE_WIRE_SCHEMA_VERSION,
            now_ms: 70_000,
            last_selected: Some(retrying.identity.clone()),
            projects: vec![retrying],
            outcome: Some(AgentRunStateScheduleOutcomeWire {
                identity: AgentRunStateProjectIdentityWire {
                    project: "a".to_string(),
                    owner: "alice".to_string(),
                },
                result: AGENT_RUN_STATE_OUTCOME_FAILURE.to_string(),
                next_cursor: None,
                pass_complete: false,
            }),
        })
        .unwrap();
    assert_eq!(
        second_failure.updated_checkpoint.unwrap().retry_after_ms,
        190_000
    );
}

#[test]
fn scheduler_separates_owners_and_persists_a_cursor_until_pass_completion() {
    let alice = project("a", "alice", true);
    let bob = project("a", "bob", true);
    let request = AgentRunStateScheduleRequestWire {
        schema_version: AGENT_RUN_STATE_WIRE_SCHEMA_VERSION,
        now_ms: 5_000,
        last_selected: Some(alice.identity.clone()),
        projects: vec![alice.clone(), bob.clone()],
        outcome: Some(AgentRunStateScheduleOutcomeWire {
            identity: alice.identity.clone(),
            result: AGENT_RUN_STATE_OUTCOME_SUCCESS.to_string(),
            next_cursor: Some("cursor-1".to_string()),
            pass_complete: false,
        }),
    };
    let response = advance_agent_run_state_schedule(request).unwrap();
    assert_eq!(response.next_project, Some(bob.identity));
    let checkpoint = response.updated_checkpoint.unwrap();
    assert_eq!(checkpoint.cursor.as_deref(), Some("cursor-1"));
    assert_eq!(checkpoint.failure_count, 0);

    let completed =
        advance_agent_run_state_schedule(AgentRunStateScheduleRequestWire {
            schema_version: AGENT_RUN_STATE_WIRE_SCHEMA_VERSION,
            now_ms: 6_000,
            last_selected: Some(alice.identity.clone()),
            projects: vec![alice.clone()],
            outcome: Some(AgentRunStateScheduleOutcomeWire {
                identity: alice.identity,
                result: AGENT_RUN_STATE_OUTCOME_SUCCESS.to_string(),
                next_cursor: Some("cursor-2".to_string()),
                pass_complete: true,
            }),
        })
        .unwrap();
    assert_eq!(completed.updated_checkpoint.unwrap().cursor, None);
}
