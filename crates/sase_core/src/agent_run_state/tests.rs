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

fn raw_observation(
    source: &str,
    read_status: &str,
    outcome: Option<&str>,
) -> AgentRunStateRawObservationWire {
    AgentRunStateRawObservationWire {
        source: source.to_string(),
        read_status: read_status.to_string(),
        identity_claims: None,
        outcome: outcome.map(str::to_string),
        state: None,
        repeat_stopped: false,
        monitor_state: None,
        gate_state: None,
        started_at: Some("2026-10-29T12:00:00Z".to_string()),
        finished_at: Some("2026-10-29T12:01:00Z".to_string()),
        imported: false,
    }
}

fn observations_request(
    published_state: &str,
    observations: Vec<AgentRunStateRawObservationWire>,
) -> AgentRunStateObservationsResolveRequestWire {
    AgentRunStateObservationsResolveRequestWire {
        schema_version: AGENT_RUN_STATE_WIRE_SCHEMA_VERSION,
        identity: identity("run-1"),
        local_name: Some("worker".to_string()),
        published_state: published_state.to_string(),
        published_started_at: None,
        published_finished_at: None,
        observations,
    }
}

fn discovered_hood(
    hood: &str,
    status: &str,
    run_ids: &[&str],
) -> AgentRunStateDiscoveredHoodWire {
    AgentRunStateDiscoveredHoodWire {
        local_hood: hood.to_string(),
        status: status.to_string(),
        source_run_ids: run_ids.iter().map(|run| (*run).to_string()).collect(),
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
fn raw_done_outcomes_are_classified_from_the_marker_vocabulary() {
    for (outcome, expected) in [
        ("completed", "completed"),
        ("noop", "completed"),
        ("epic_approved", "completed"),
        ("plan_committed", "completed"),
        ("plan_rejected", "completed"),
        ("failed", "failed"),
        ("killed", "failed"),
        ("epic_launch_failed", "failed"),
        ("stopped", "stopped"),
    ] {
        let result =
            resolve_agent_run_state_observations(observations_request(
                "active",
                vec![raw_observation(
                    AGENT_RUN_STATE_EVIDENCE_TERMINAL_MARKER,
                    AGENT_RUN_STATE_OBSERVATION_PRESENT,
                    Some(outcome),
                )],
            ))
            .unwrap();
        assert_eq!(result.selected_state, expected, "outcome {outcome}");
        assert!(result.changed, "outcome {outcome}");
        assert_eq!(
            result.observations[0].evidence_state.as_deref(),
            Some(expected)
        );
    }

    let mut repeat_stop = raw_observation(
        AGENT_RUN_STATE_EVIDENCE_TERMINAL_MARKER,
        AGENT_RUN_STATE_OBSERVATION_PRESENT,
        Some("completed"),
    );
    repeat_stop.repeat_stopped = true;
    let stopped = resolve_agent_run_state_observations(observations_request(
        "active",
        vec![repeat_stop],
    ))
    .unwrap();
    assert_eq!(stopped.selected_state, "stopped");
}

#[test]
fn empty_and_unknown_markers_are_malformed_and_never_failed() {
    for outcome in [None, Some("future_outcome")] {
        let result =
            resolve_agent_run_state_observations(observations_request(
                "active",
                vec![raw_observation(
                    AGENT_RUN_STATE_EVIDENCE_TERMINAL_MARKER,
                    AGENT_RUN_STATE_OBSERVATION_PRESENT,
                    outcome,
                )],
            ))
            .unwrap();
        assert_eq!(result.selected_state, "active");
        assert!(!result.changed);
        assert_eq!(result.reason, AGENT_RUN_STATE_REASON_MALFORMED_EVIDENCE);
        assert_eq!(
            result.observations[0].evidence_kind,
            AGENT_RUN_STATE_EVIDENCE_MALFORMED
        );
    }
}

#[test]
fn raw_shell_marker_states_and_unfinished_observations_are_explicit() {
    let mut monitored = raw_observation(
        AGENT_RUN_STATE_EVIDENCE_TERMINAL_MARKER,
        AGENT_RUN_STATE_OBSERVATION_PRESENT,
        Some("monitored"),
    );
    monitored.monitor_state = Some("completed".to_string());
    let mut gated = raw_observation(
        AGENT_RUN_STATE_EVIDENCE_TERMINAL_MARKER,
        AGENT_RUN_STATE_OBSERVATION_PRESENT,
        Some("gated"),
    );
    gated.gate_state = Some("stopped".to_string());
    for (observation, expected) in
        [(monitored, "completed"), (gated, "stopped")]
    {
        assert_eq!(
            resolve_agent_run_state_observations(observations_request(
                "active",
                vec![observation],
            ))
            .unwrap()
            .selected_state,
            expected
        );
    }

    let live = resolve_agent_run_state_observations(observations_request(
        "waiting",
        vec![raw_observation(
            AGENT_RUN_STATE_EVIDENCE_UNFINISHED,
            AGENT_RUN_STATE_OBSERVATION_PRESENT,
            None,
        )],
    ))
    .unwrap();
    assert_eq!(live.selected_state, "waiting");
    assert_eq!(live.reason, AGENT_RUN_STATE_REASON_LIVE_EVIDENCE);
}

#[test]
fn marker_identity_claims_are_optional_but_conflicts_are_rejected() {
    let mut older_marker = raw_observation(
        AGENT_RUN_STATE_EVIDENCE_TERMINAL_MARKER,
        AGENT_RUN_STATE_OBSERVATION_PRESENT,
        Some("completed"),
    );
    older_marker.finished_at = None;
    assert_eq!(
        resolve_agent_run_state_observations(observations_request(
            "active",
            vec![older_marker],
        ))
        .unwrap()
        .selected_finished_at,
        None
    );

    let mut matching_name = raw_observation(
        AGENT_RUN_STATE_EVIDENCE_TERMINAL_MARKER,
        AGENT_RUN_STATE_OBSERVATION_PRESENT,
        Some("completed"),
    );
    matching_name.identity_claims = Some(AgentRunStateIdentityClaimsWire {
        name: Some("worker".to_string()),
        ..AgentRunStateIdentityClaimsWire::default()
    });
    assert_eq!(
        resolve_agent_run_state_observations(observations_request(
            "active",
            vec![matching_name],
        ))
        .unwrap()
        .selected_state,
        "completed"
    );

    let mut wrong_run = raw_observation(
        AGENT_RUN_STATE_EVIDENCE_TERMINAL_MARKER,
        AGENT_RUN_STATE_OBSERVATION_PRESENT,
        Some("completed"),
    );
    wrong_run.identity_claims = Some(AgentRunStateIdentityClaimsWire {
        source_run_id: Some("different-run".to_string()),
        ..AgentRunStateIdentityClaimsWire::default()
    });
    let result = resolve_agent_run_state_observations(observations_request(
        "active",
        vec![wrong_run],
    ))
    .unwrap();
    assert_eq!(result.selected_state, "active");
    assert_eq!(result.reason, AGENT_RUN_STATE_REASON_IDENTITY_CONFLICT);
}

#[test]
fn conflicting_candidates_stay_unchanged_and_later_dismissal_uses_resolve_policy(
) {
    let completed = raw_observation(
        AGENT_RUN_STATE_EVIDENCE_TERMINAL_MARKER,
        AGENT_RUN_STATE_OBSERVATION_PRESENT,
        Some("completed"),
    );
    let failed = raw_observation(
        AGENT_RUN_STATE_EVIDENCE_TERMINAL_MARKER,
        AGENT_RUN_STATE_OBSERVATION_PRESENT,
        Some("failed"),
    );
    let conflict = resolve_agent_run_state_observations(observations_request(
        "active",
        vec![completed.clone(), failed],
    ))
    .unwrap();
    assert_eq!(conflict.selected_state, "active");
    assert!(!conflict.changed);
    assert_eq!(conflict.reason, AGENT_RUN_STATE_REASON_CONFLICTING_TERMINAL);

    let mut dismissal = raw_observation(
        AGENT_RUN_STATE_EVIDENCE_DISMISSED,
        AGENT_RUN_STATE_OBSERVATION_PRESENT,
        None,
    );
    dismissal.state = Some("dismissed".to_string());
    dismissal.finished_at = Some("2026-10-29T12:02:00Z".to_string());
    let later = resolve_agent_run_state_observations(observations_request(
        "active",
        vec![completed.clone(), dismissal.clone()],
    ))
    .unwrap();
    assert_eq!(later.selected_state, "dismissed");

    dismissal.finished_at = Some("2026-10-29T12:00:30Z".to_string());
    let earlier = resolve_agent_run_state_observations(observations_request(
        "active",
        vec![completed, dismissal],
    ))
    .unwrap();
    assert_eq!(earlier.selected_state, "active");
    assert_eq!(earlier.reason, AGENT_RUN_STATE_REASON_CONFLICTING_TERMINAL);
}

#[test]
fn missing_unreadable_and_corrupt_observations_have_distinct_classifications() {
    let result = resolve_agent_run_state_observations(observations_request(
        "completed",
        vec![
            raw_observation(
                AGENT_RUN_STATE_EVIDENCE_TERMINAL_MARKER,
                AGENT_RUN_STATE_OBSERVATION_MISSING,
                None,
            ),
            raw_observation(
                AGENT_RUN_STATE_EVIDENCE_TERMINAL_MARKER,
                AGENT_RUN_STATE_OBSERVATION_UNREADABLE,
                None,
            ),
        ],
    ))
    .unwrap();
    assert_eq!(result.selected_state, "completed");
    assert_eq!(result.reason, AGENT_RUN_STATE_REASON_UNREADABLE_EVIDENCE);
    assert_eq!(
        result.observations[0].evidence_kind,
        AGENT_RUN_STATE_EVIDENCE_MISSING
    );
    assert_eq!(
        result.observations[1].evidence_kind,
        AGENT_RUN_STATE_EVIDENCE_UNREADABLE
    );
}

#[test]
fn bounded_hood_discovery_advances_empty_and_corrupt_units_and_keeps_ars1() {
    let first =
        select_agent_run_state_hoods(AgentRunStateHoodWindowRequestWire {
            schema_version: AGENT_RUN_STATE_WIRE_SCHEMA_VERSION,
            project: "project-a".to_string(),
            owner: "alice@athena".to_string(),
            manifest_status: AGENT_RUN_STATE_MANIFEST_VALID.to_string(),
            after: None,
            hoods: vec![
                "hood-g".to_string(),
                "hood-c".to_string(),
                "hood-a".to_string(),
                "hood-f".to_string(),
                "hood-b".to_string(),
                "hood-e".to_string(),
                "hood-d".to_string(),
            ],
        })
        .unwrap();
    assert_eq!(
        first.selected_hoods,
        vec!["hood-a", "hood-b", "hood-c", "hood-d", "hood-e"]
    );
    assert!(first.has_more_hoods);

    let page = select_agent_run_state_discovery_window(
        AgentRunStateDiscoveryWindowRequestWire {
            schema_version: AGENT_RUN_STATE_WIRE_SCHEMA_VERSION,
            project: "project-a".to_string(),
            owner: "alice@athena".to_string(),
            manifest_status: AGENT_RUN_STATE_MANIFEST_VALID.to_string(),
            scan_status: AGENT_RUN_STATE_SCAN_COMPLETE.to_string(),
            limit: 2,
            after: None,
            manifest_hoods: vec![
                "hood-a".to_string(),
                "hood-b".to_string(),
                "hood-c".to_string(),
                "hood-d".to_string(),
                "hood-e".to_string(),
                "hood-f".to_string(),
                "hood-g".to_string(),
            ],
            hoods: vec![
                discovered_hood("hood-a", AGENT_RUN_STATE_HOOD_EMPTY, &[]),
                discovered_hood("hood-b", AGENT_RUN_STATE_HOOD_CORRUPT, &[]),
                discovered_hood(
                    "hood-c",
                    AGENT_RUN_STATE_HOOD_AVAILABLE,
                    &["r-2", "r-1", "r-3"],
                ),
                discovered_hood("hood-d", AGENT_RUN_STATE_HOOD_EMPTY, &[]),
                discovered_hood(
                    "hood-e",
                    AGENT_RUN_STATE_HOOD_AVAILABLE,
                    &["r-4"],
                ),
            ],
        },
    )
    .unwrap();
    assert_eq!(
        page.selected,
        vec![window_row("hood-c", "r-1"), window_row("hood-c", "r-2")]
    );
    assert_eq!(page.diagnostics.len(), 3);
    assert!(!page.pass_complete);
    let cursor = page.next_cursor.unwrap();
    assert!(cursor.starts_with("ars1:"));

    let resumed =
        select_agent_run_state_hoods(AgentRunStateHoodWindowRequestWire {
            schema_version: AGENT_RUN_STATE_WIRE_SCHEMA_VERSION,
            project: "project-a".to_string(),
            owner: "alice@athena".to_string(),
            manifest_status: AGENT_RUN_STATE_MANIFEST_VALID.to_string(),
            after: Some(cursor.clone()),
            hoods: vec![
                "hood-a".to_string(),
                "hood-b".to_string(),
                "hood-c".to_string(),
                "hood-d".to_string(),
                "hood-e".to_string(),
                "hood-f".to_string(),
                "hood-g".to_string(),
            ],
        })
        .unwrap();
    assert_eq!(resumed.selected_hoods[0], "hood-c");

    let second_page = select_agent_run_state_discovery_window(
        AgentRunStateDiscoveryWindowRequestWire {
            schema_version: AGENT_RUN_STATE_WIRE_SCHEMA_VERSION,
            project: "project-a".to_string(),
            owner: "alice@athena".to_string(),
            manifest_status: AGENT_RUN_STATE_MANIFEST_VALID.to_string(),
            scan_status: AGENT_RUN_STATE_SCAN_COMPLETE.to_string(),
            limit: 2,
            after: Some(cursor),
            manifest_hoods: vec![
                "hood-a".to_string(),
                "hood-b".to_string(),
                "hood-c".to_string(),
                "hood-d".to_string(),
                "hood-e".to_string(),
                "hood-f".to_string(),
                "hood-g".to_string(),
            ],
            hoods: vec![
                discovered_hood(
                    "hood-c",
                    AGENT_RUN_STATE_HOOD_AVAILABLE,
                    &["r-0", "r-1", "r-3", "r-5"],
                ),
                discovered_hood("hood-d", AGENT_RUN_STATE_HOOD_EMPTY, &[]),
                discovered_hood(
                    "hood-e",
                    AGENT_RUN_STATE_HOOD_AVAILABLE,
                    &["r-4"],
                ),
                discovered_hood("hood-f", AGENT_RUN_STATE_HOOD_EMPTY, &[]),
                discovered_hood("hood-g", AGENT_RUN_STATE_HOOD_EMPTY, &[]),
            ],
        },
    )
    .unwrap();
    // A row inserted before the cursor and the previously selected row do not
    // repeat; pagination continues within hood-c before hood-e.
    assert_eq!(
        second_page.selected,
        vec![window_row("hood-c", "r-3"), window_row("hood-c", "r-5")]
    );
    assert!(!second_page.pass_complete);
}

#[test]
fn discovery_progresses_past_empty_hoods_with_owner_bound_ars2_cursors() {
    let manifest = vec!["hood-a".to_string(), "hood-b".to_string()];
    let page = select_agent_run_state_discovery_window(
        AgentRunStateDiscoveryWindowRequestWire {
            schema_version: AGENT_RUN_STATE_WIRE_SCHEMA_VERSION,
            project: "project-a".to_string(),
            owner: "alice@athena".to_string(),
            manifest_status: AGENT_RUN_STATE_MANIFEST_VALID.to_string(),
            scan_status: AGENT_RUN_STATE_SCAN_COMPLETE.to_string(),
            limit: 10,
            after: None,
            manifest_hoods: manifest.clone(),
            hoods: vec![
                discovered_hood("hood-a", AGENT_RUN_STATE_HOOD_EMPTY, &[]),
                discovered_hood("hood-b", AGENT_RUN_STATE_HOOD_EMPTY, &[]),
            ],
        },
    )
    .unwrap();
    assert!(page.pass_complete);
    assert!(page.next_cursor.is_none());

    let many = (0..7).map(|index| format!("hood-{index}")).collect();
    let first_hoods =
        select_agent_run_state_hoods(AgentRunStateHoodWindowRequestWire {
            schema_version: AGENT_RUN_STATE_WIRE_SCHEMA_VERSION,
            project: "project-a".to_string(),
            owner: "alice@athena".to_string(),
            manifest_status: AGENT_RUN_STATE_MANIFEST_VALID.to_string(),
            after: None,
            hoods: many,
        })
        .unwrap();
    let all_hoods: Vec<_> =
        (0..7).map(|index| format!("hood-{index}")).collect();
    let hoods = first_hoods
        .selected_hoods
        .iter()
        .map(|hood| discovered_hood(hood, AGENT_RUN_STATE_HOOD_EMPTY, &[]))
        .collect();
    let first = select_agent_run_state_discovery_window(
        AgentRunStateDiscoveryWindowRequestWire {
            schema_version: AGENT_RUN_STATE_WIRE_SCHEMA_VERSION,
            project: "project-a".to_string(),
            owner: "alice@athena".to_string(),
            manifest_status: AGENT_RUN_STATE_MANIFEST_VALID.to_string(),
            scan_status: AGENT_RUN_STATE_SCAN_COMPLETE.to_string(),
            limit: 10,
            after: None,
            manifest_hoods: all_hoods.clone(),
            hoods,
        },
    )
    .unwrap();
    assert!(!first.pass_complete);
    let cursor = first.next_cursor.unwrap();
    assert!(cursor.starts_with("ars2:"));

    let next =
        select_agent_run_state_hoods(AgentRunStateHoodWindowRequestWire {
            schema_version: AGENT_RUN_STATE_WIRE_SCHEMA_VERSION,
            project: "project-a".to_string(),
            owner: "alice@athena".to_string(),
            manifest_status: AGENT_RUN_STATE_MANIFEST_VALID.to_string(),
            after: Some(cursor.clone()),
            hoods: all_hoods.clone(),
        })
        .unwrap();
    assert_eq!(next.selected_hoods, vec!["hood-5", "hood-6"]);
    let wrong_owner =
        select_agent_run_state_hoods(AgentRunStateHoodWindowRequestWire {
            schema_version: AGENT_RUN_STATE_WIRE_SCHEMA_VERSION,
            project: "project-a".to_string(),
            owner: "bob@athena".to_string(),
            manifest_status: AGENT_RUN_STATE_MANIFEST_VALID.to_string(),
            after: Some(cursor.clone()),
            hoods: all_hoods.clone(),
        });
    assert!(wrong_owner.is_err());
    let wrong_project =
        select_agent_run_state_hoods(AgentRunStateHoodWindowRequestWire {
            schema_version: AGENT_RUN_STATE_WIRE_SCHEMA_VERSION,
            project: "project-b".to_string(),
            owner: "alice@athena".to_string(),
            manifest_status: AGENT_RUN_STATE_MANIFEST_VALID.to_string(),
            after: Some(cursor),
            hoods: all_hoods,
        });
    assert!(wrong_project.is_err());
}

#[test]
fn discovery_limits_and_snapshot_window_membership_are_checked() {
    let request = |limit| AgentRunStateDiscoveryWindowRequestWire {
        schema_version: AGENT_RUN_STATE_WIRE_SCHEMA_VERSION,
        project: "project-a".to_string(),
        owner: "alice@athena".to_string(),
        manifest_status: AGENT_RUN_STATE_MANIFEST_VALID.to_string(),
        scan_status: AGENT_RUN_STATE_SCAN_COMPLETE.to_string(),
        limit,
        after: None,
        manifest_hoods: vec!["hood-a".to_string()],
        hoods: vec![discovered_hood(
            "hood-a",
            AGENT_RUN_STATE_HOOD_AVAILABLE,
            &["run-1"],
        )],
    };
    assert!(select_agent_run_state_discovery_window(request(0)).is_err());
    assert!(select_agent_run_state_discovery_window(request(501)).is_err());
    let mut unreadable_manifest = request(1);
    unreadable_manifest.manifest_status = "unreadable".to_string();
    assert!(
        select_agent_run_state_discovery_window(unreadable_manifest).is_err()
    );
    let mut interrupted_scan = request(1);
    interrupted_scan.scan_status = "interrupted".to_string();
    assert!(select_agent_run_state_discovery_window(interrupted_scan).is_err());
    let mut wrong_hood = request(1);
    wrong_hood.hoods[0].local_hood = "hood-b".to_string();
    assert!(select_agent_run_state_discovery_window(wrong_hood).is_err());
}

#[test]
fn only_a_valid_empty_manifest_can_complete_an_empty_discovery_pass() {
    let empty_manifest = AgentRunStateHoodWindowRequestWire {
        schema_version: AGENT_RUN_STATE_WIRE_SCHEMA_VERSION,
        project: "project-a".to_string(),
        owner: "alice@athena".to_string(),
        manifest_status: AGENT_RUN_STATE_MANIFEST_VALID.to_string(),
        after: None,
        hoods: vec![],
    };
    let window = select_agent_run_state_hoods(empty_manifest).unwrap();
    assert!(window.selected_hoods.is_empty());
    assert!(!window.has_more_hoods);

    let pass = select_agent_run_state_discovery_window(
        AgentRunStateDiscoveryWindowRequestWire {
            schema_version: AGENT_RUN_STATE_WIRE_SCHEMA_VERSION,
            project: "project-a".to_string(),
            owner: "alice@athena".to_string(),
            manifest_status: AGENT_RUN_STATE_MANIFEST_VALID.to_string(),
            scan_status: AGENT_RUN_STATE_SCAN_COMPLETE.to_string(),
            limit: AGENT_RUN_STATE_WINDOW_DEFAULT_LIMIT,
            after: None,
            manifest_hoods: vec![],
            hoods: vec![],
        },
    )
    .unwrap();
    assert!(pass.pass_complete);

    let unreadable = AgentRunStateHoodWindowRequestWire {
        schema_version: AGENT_RUN_STATE_WIRE_SCHEMA_VERSION,
        project: "project-a".to_string(),
        owner: "alice@athena".to_string(),
        manifest_status: "unreadable".to_string(),
        after: None,
        hoods: vec![],
    };
    assert!(select_agent_run_state_hoods(unreadable).is_err());
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
