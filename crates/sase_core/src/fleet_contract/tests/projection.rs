use super::super::*;
use super::support::*;
use crate::agent_scan::DoneMarkerWire;
use crate::fleet_contract::projection::current_project_locator_schema;
use serde_json::json;

#[test]
fn projection_outputs_safe_summary_and_detail_without_local_fields() {
    let locator = logical('a', "worker");
    let exact_locator = exact('a', "worker", "run-1");
    let mut record = record_running();
    if let Some(meta) = record.agent_meta.as_mut() {
        meta.queue_weight = Some(0.25);
        meta.queue_weight_explicit = true;
    }
    let mut request = projection_request(
        locator.clone(),
        Some(exact_locator.clone()),
        1,
        record,
    );
    request.owner_facts.capabilities = caps(&["stop", "content.read", "stop"]);
    request.owner_facts.content_handles = vec![handle(&locator)];
    request.owner_facts.connection_health = ConnectionHealthWire::Offline;
    request.owner_facts.freshness = ObservationFreshnessWire::Stale;
    let summary = project_resolved_agent_summary(&request).unwrap();
    assert_eq!(summary.lifecycle, FleetLifecycleWire::Running);
    assert_eq!(summary.liveness, OwnerLivenessWire::Alive);
    assert_eq!(summary.connection_health, ConnectionHealthWire::Offline);
    assert_eq!(summary.freshness, ObservationFreshnessWire::Stale);
    assert_eq!(summary.capabilities.resource, vec!["content.read", "stop"]);
    assert_eq!(summary.content.handle_count, 1);
    assert_eq!(summary.queue_weight, Some(0.25));
    assert!(summary.queue_weight_explicit);
    assert!(!summary.queue_weight_invalid);
    let value = serde_json::to_value(&summary).unwrap();
    assert_no_forbidden_local_fields(&value);

    let detail = project_resolved_agent_detail(&request).unwrap();
    assert_eq!(detail.content_handles.len(), 1);
    let detail_value = serde_json::to_value(&detail).unwrap();
    assert_no_forbidden_local_fields(&detail_value);
}
#[test]
fn summary_accepts_readable_capability_schema_versions_but_rejects_unnormalized_content(
) {
    let mut summary = project_resolved_agent_summary(&projection_request(
        logical('a', "caps"),
        Some(exact('a', "caps", "run-1")),
        1,
        record_running(),
    ))
    .unwrap();

    for version in [1, 2, FLEET_CONTRACT_SCHEMA_VERSION] {
        summary.capabilities.schema_version = version;
        assert_eq!(validate_resolved_agent_summary(&summary).unwrap(), summary);
    }

    for version in [0, FLEET_CONTRACT_SCHEMA_VERSION + 1] {
        summary.capabilities.schema_version = version;
        let err = validate_resolved_agent_summary(&summary).unwrap_err();
        assert!(err.to_string().contains("capability set schema_version"));
    }

    summary.capabilities.schema_version = 1;
    summary.capabilities.resource =
        vec!["stop".to_string(), "content.read".to_string()];
    let err = validate_resolved_agent_summary(&summary).unwrap_err();
    assert!(err
        .to_string()
        .contains("summary capabilities are not normalized"));

    summary.capabilities.resource =
        vec!["content.read".to_string(), "content.read".to_string()];
    let err = validate_resolved_agent_summary(&summary).unwrap_err();
    assert!(err
        .to_string()
        .contains("summary capabilities are not normalized"));
}
#[test]
fn v4_shaped_summary_deserializes_at_v5_and_carries_owner_facts() {
    let locator = logical('a', "worker");
    let request = projection_request(
        locator,
        Some(exact('a', "worker", "run-1")),
        1,
        record_running(),
    );
    let summary = project_resolved_agent_summary(&request).unwrap();
    let mut value = serde_json::to_value(&summary).unwrap();
    // A v4 payload has no `presentation` object at all.
    assert!(value.get("presentation").is_none());
    value["schema_version"] = serde_json::json!(4);
    let legacy: ResolvedAgentSummaryWire =
        serde_json::from_value(value).unwrap();
    assert!(legacy.presentation.is_empty());

    let mut with_facts = request;
    with_facts.owner_facts.presentation.gate_id = Some("g1".to_string());
    with_facts.owner_facts.presentation.question_answered = true;
    let summary = project_resolved_agent_summary(&with_facts).unwrap();
    let value = serde_json::to_value(&summary).unwrap();
    assert_eq!(value["presentation"]["gate_id"], "g1");
    assert_eq!(value["presentation"]["question_answered"], true);
}
#[test]
fn projection_carries_owner_presentation_facts_for_remote_rendering() {
    let locator = logical('a', "worker");
    let exact_locator = exact('a', "worker", "run-1");
    let mut request =
        projection_request(locator, Some(exact_locator), 1, record_running());
    request.owner_facts.display_status = Some("OWNER-RUNNING".to_string());
    request.owner_facts.started_at_unix = Some(100.25);
    request.owner_facts.run_started_at_unix = Some(101.75);
    request.owner_facts.stopped_at_unix = Some(125.5);
    request.owner_facts.workspace_num = Some(17);
    request.owner_facts.project_label = Some("sase".to_string());
    request.owner_facts.agent_clan = Some("fleet".to_string());
    request.owner_facts.agent_clan_generation = Some("20260913".to_string());
    request.owner_facts.clan_tribe = Some("parity".to_string());
    request.owner_facts.tribe = Some("review".to_string());

    let summary = project_resolved_agent_summary(&request).unwrap();

    assert_eq!(summary.labels.project_label, "sase");
    assert_eq!(summary.status, "OWNER-RUNNING");
    assert_eq!(summary.started_at_unix, Some(100.25));
    assert_eq!(summary.run_started_at_unix, Some(101.75));
    assert_eq!(summary.stopped_at_unix, Some(125.5));
    assert_eq!(summary.workspace_num, Some(17));
    assert_eq!(summary.agent_clan.as_deref(), Some("fleet"));
    assert_eq!(summary.agent_clan_generation.as_deref(), Some("20260913"));
    assert_eq!(summary.clan_tribe.as_deref(), Some("parity"));
    assert_eq!(summary.tribe.as_deref(), Some("review"));

    let mut invalid = summary;
    invalid.stopped_at_unix = Some(99.0);
    assert!(validate_resolved_agent_summary(&invalid).is_err());
}
#[test]
fn projection_applies_owner_resolved_lineage_and_normalizes_locator_schemas() {
    let mut locator = logical('a', "historical");
    locator.agent_session_id = None;
    let mut resolved_locator = locator.clone();
    resolved_locator.schema_version = 1;
    resolved_locator.project.schema_version = 1;
    resolved_locator.project.origin.schema_version = 1;
    resolved_locator.agent_session_id = Some("family-resolved".to_string());
    let exact_locator = AgentInstanceLocatorWire {
        schema_version: 1,
        logical: resolved_locator.clone(),
        shell_id: "shell-1".to_string(),
        run_id: "run-1".to_string(),
        attempt_id: "attempt-1".to_string(),
    };
    let mut record = record_running();
    record.running = None;
    record.done = Some(DoneMarkerWire {
        outcome: Some("completed".to_string()),
        status_label: Some("DONE".to_string()),
        ..DoneMarkerWire::default()
    });
    record.has_done_marker = true;
    if let Some(meta) = record.agent_meta.as_mut() {
        meta.agent_session = None;
        meta.parent_timestamp = None;
        meta.tribe = None;
        meta.clan_tribe = None;
    }
    let mut request = projection_request(locator, None, 1, record);
    request.owner_facts.exact_locator = Some(exact_locator);
    request.owner_facts.agent_session_id = Some("family-resolved".to_string());
    request.owner_facts.parent_timestamp = Some("20260906115900".to_string());
    request.owner_facts.tribe = Some("review".to_string());
    request.owner_facts.clan_tribe = Some("parity".to_string());
    request.owner_facts.liveness = OwnerLivenessWire::Dead;
    request.owner_facts.capabilities = caps(&[]);
    request.owner_facts.occupied_runner_slot = false;
    request.owner_facts.row_revision = revision(
        &LogicalAgentLocatorWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            project: current_project_locator_schema(&resolved_locator.project),
            agent_id: resolved_locator.agent_id.clone(),
            agent_session_id: Some("family-resolved".to_string()),
        },
        1,
    );

    let summary = project_resolved_agent_summary(&request).unwrap();

    assert_eq!(
        summary.logical_locator.schema_version,
        FLEET_CONTRACT_SCHEMA_VERSION
    );
    assert_eq!(
        summary.logical_locator.agent_session_id.as_deref(),
        Some("family-resolved")
    );
    assert_eq!(
        summary.exact_locator.as_ref().unwrap().schema_version,
        FLEET_CONTRACT_SCHEMA_VERSION
    );
    assert_eq!(summary.parent_timestamp.as_deref(), Some("20260906115900"));
    assert_eq!(
        summary.agent_session_role,
        FleetAgentSessionRoleWire::HistoricalShell
    );
    assert_eq!(summary.tribe.as_deref(), Some("review"));
    assert_eq!(summary.clan_tribe.as_deref(), Some("parity"));
}
#[test]
fn projection_carries_canonical_queue_capacity_from_metadata() {
    let locator = logical('a', "capacity");
    let exact_locator = exact('a', "capacity", "run-1");
    let mut record = record_running();
    if let Some(meta) = record.agent_meta.as_mut() {
        meta.queue_capacity = Some(100);
        meta.wait_runners = Some(0);
        meta.queue_capacity_explicit = true;
        meta.wait_runners_explicit = true;
    }
    let request = projection_request(locator, Some(exact_locator), 1, record);

    let summary = project_resolved_agent_summary(&request).unwrap();

    assert_eq!(summary.queue_capacity, Some(100));
    assert!(summary.queue_capacity_explicit);
    let value = serde_json::to_value(&summary).unwrap();
    assert_eq!(value["queue_capacity"], json!(100));
    assert_eq!(value["queue_capacity_explicit"], json!(true));
    assert!(value.get("wait_runners").is_none());
    let decoded: ResolvedAgentSummaryWire =
        serde_json::from_value(value).unwrap();
    assert_eq!(validate_resolved_agent_summary(&decoded).unwrap(), summary);
}
#[test]
fn projection_reads_legacy_queue_capacity_without_emitting_legacy_alias() {
    let locator = logical('a', "legacy-capacity");
    let exact_locator = exact('a', "legacy-capacity", "run-1");
    let mut record = record_running();
    if let Some(meta) = record.agent_meta.as_mut() {
        meta.wait_runners = Some(0);
        meta.wait_runners_explicit = true;
    }
    let request = projection_request(locator, Some(exact_locator), 1, record);

    let summary = project_resolved_agent_summary(&request).unwrap();

    assert_eq!(summary.queue_capacity, Some(0));
    assert!(summary.queue_capacity_explicit);
    let value = serde_json::to_value(&summary).unwrap();
    assert_eq!(value["queue_capacity"], json!(0));
    assert!(value.get("wait_runners").is_none());
}
#[test]
fn projection_prefers_waiting_queue_capacity_over_metadata() {
    let locator = logical('a', "waiting-capacity");
    let exact_locator = exact('a', "waiting-capacity", "run-1");
    let mut record = record_running();
    if let Some(meta) = record.agent_meta.as_mut() {
        meta.queue_capacity = Some(100);
        meta.queue_capacity_explicit = true;
    }
    record.waiting = Some(crate::agent_scan::WaitingMarkerWire {
        queue_capacity: Some(0),
        queue_capacity_explicit: true,
        ..crate::agent_scan::WaitingMarkerWire::default()
    });
    let request = projection_request(locator, Some(exact_locator), 1, record);

    let summary = project_resolved_agent_summary(&request).unwrap();

    assert_eq!(summary.queue_capacity, Some(0));
    assert!(summary.queue_capacity_explicit);
}
#[test]
fn projection_leaves_absent_queue_capacity_quiet_and_reads_schema_two() {
    let locator = logical('a', "absent-capacity");
    let exact_locator = exact('a', "absent-capacity", "run-1");
    let request =
        projection_request(locator, Some(exact_locator), 1, record_running());

    let summary = project_resolved_agent_summary(&request).unwrap();

    assert_eq!(summary.queue_capacity, None);
    assert!(!summary.queue_capacity_explicit);

    let mut old_value = serde_json::to_value(&summary).unwrap();
    let object = old_value.as_object_mut().unwrap();
    object.insert("schema_version".to_string(), json!(3));
    object.remove("queue_capacity");
    object.remove("queue_capacity_explicit");
    object.remove("run_started_at_unix");
    let old_summary: ResolvedAgentSummaryWire =
        serde_json::from_value(old_value).unwrap();

    assert_eq!(old_summary.queue_capacity, None);
    assert!(!old_summary.queue_capacity_explicit);
    assert_eq!(old_summary.run_started_at_unix, None);
    assert_eq!(
        validate_resolved_agent_summary(&old_summary).unwrap(),
        old_summary
    );
}
#[test]
fn projection_prefers_waiting_queue_weight_over_metadata() {
    let locator = logical('a', "weighted");
    let exact_locator = exact('a', "weighted", "run-1");
    let mut record = record_running();
    if let Some(meta) = record.agent_meta.as_mut() {
        meta.queue_weight = Some(2.0);
        meta.queue_weight_explicit = false;
    }
    record.waiting = Some(crate::agent_scan::WaitingMarkerWire {
        queue_weight: Some(0.5),
        queue_weight_explicit: true,
        ..crate::agent_scan::WaitingMarkerWire::default()
    });
    let request = projection_request(locator, Some(exact_locator), 1, record);

    let summary = project_resolved_agent_summary(&request).unwrap();

    assert_eq!(summary.queue_weight, Some(0.5));
    assert!(summary.queue_weight_explicit);
    assert!(!summary.queue_weight_invalid);
}
#[test]
fn projection_accepts_explicit_zero_queue_weight_but_rejects_implicit_zero() {
    let explicit_locator = logical('a', "epic-launch-monitor");
    let explicit_exact = exact('a', "epic-launch-monitor", "run-1");
    let mut explicit_record = record_running();
    if let Some(meta) = explicit_record.agent_meta.as_mut() {
        meta.queue_weight = Some(0.0);
        meta.queue_weight_explicit = true;
    }
    let explicit_request = projection_request(
        explicit_locator,
        Some(explicit_exact),
        1,
        explicit_record,
    );

    let explicit_summary =
        project_resolved_agent_summary(&explicit_request).unwrap();
    assert_eq!(explicit_summary.queue_weight, Some(0.0));
    assert!(explicit_summary.queue_weight_explicit);
    assert!(!explicit_summary.queue_weight_invalid);
    assert_eq!(
        validate_resolved_agent_summary(&explicit_summary).unwrap(),
        explicit_summary
    );

    let implicit_locator = logical('a', "no-directive");
    let implicit_exact = exact('a', "no-directive", "run-2");
    let mut implicit_record = record_running();
    if let Some(meta) = implicit_record.agent_meta.as_mut() {
        meta.queue_weight = Some(0.0);
        meta.queue_weight_explicit = false;
    }
    let implicit_request = projection_request(
        implicit_locator,
        Some(implicit_exact),
        1,
        implicit_record,
    );

    let implicit_summary =
        project_resolved_agent_summary(&implicit_request).unwrap();
    assert_eq!(implicit_summary.queue_weight, None);
    assert!(!implicit_summary.queue_weight_explicit);
    assert!(implicit_summary.queue_weight_invalid);
}
#[test]
fn projection_normalizes_control_characters_in_multiline_raw_prompt_intent() {
    let locator = logical('a', "multiline");
    let exact_locator = exact('a', "multiline", "run-1");
    let mut record = record_running();
    record.raw_prompt_snippet = Some(
        "Refactor the widget\nand update the tests\r\nplease\tthanks"
            .to_string(),
    );
    let request = projection_request(locator, Some(exact_locator), 1, record);

    let summary = project_resolved_agent_summary(&request).unwrap();

    let intent = summary.intent.as_deref().unwrap();
    assert!(!intent.chars().any(char::is_control));
    assert_eq!(
        intent,
        "Refactor the widget and update the tests  please thanks"
    );
}
#[test]
fn projection_normalizes_control_characters_in_plan_action_intent() {
    let locator = logical('a', "plan-multiline");
    let exact_locator = exact('a', "plan-multiline", "run-1");
    let mut record = record_running();
    if let Some(meta) = record.agent_meta.as_mut() {
        meta.plan_action = Some("Step 1: build\nStep 2: test".to_string());
    }
    let request = projection_request(locator, Some(exact_locator), 1, record);

    let summary = project_resolved_agent_summary(&request).unwrap();

    assert_eq!(
        summary.intent.as_deref(),
        Some("Step 1: build Step 2: test")
    );
}
#[test]
fn projection_bounds_multiline_unicode_intent_to_byte_limit() {
    let locator = logical('a', "byte-limit");
    let exact_locator = exact('a', "byte-limit", "run-1");
    let mut record = record_running();
    record.raw_prompt_snippet =
        Some(format!("line one\nline two\n{}", "é".repeat(400)));
    let request = projection_request(locator, Some(exact_locator), 1, record);

    let summary = project_resolved_agent_summary(&request).unwrap();

    let intent = summary.intent.unwrap();
    assert!(intent.len() <= MAX_INTENT_BYTES);
    assert!(!intent.chars().any(char::is_control));
    assert!(validate_label("intent", &intent, MAX_INTENT_BYTES).is_ok());
}
#[test]
fn projection_omits_intent_when_normalization_leaves_it_empty() {
    let locator = logical('a', "empty-intent");
    let exact_locator = exact('a', "empty-intent", "run-1");
    let mut record = record_running();
    record.raw_prompt_snippet = Some("\u{1}\u{2}\u{3}".to_string());
    let request = projection_request(locator, Some(exact_locator), 1, record);

    let summary = project_resolved_agent_summary(&request).unwrap();

    assert!(summary.intent.is_none());
}
#[test]
fn external_wire_summary_with_raw_control_character_intent_is_rejected() {
    let locator = logical('a', "external");
    let exact_locator = exact('a', "external", "run-1");
    let request =
        projection_request(locator, Some(exact_locator), 1, record_running());
    let mut summary = project_resolved_agent_summary(&request).unwrap();

    // A projected summary already carries a normalized intent; strict
    // external validation (the boundary used by federation imports and
    // any other externally supplied wire payload) must still reject a
    // raw control character regardless of how the value arrived.
    summary.intent = Some("bad\nintent".to_string());
    assert!(validate_resolved_agent_summary(&summary).is_err());
}
#[test]
fn projection_rejects_inconsistent_owner_facts_and_handles() {
    let locator = logical('a', "worker");
    let mut terminal = record_running();
    terminal.done = Some(DoneMarkerWire {
        outcome: Some("completed".to_string()),
        ..DoneMarkerWire::default()
    });
    terminal.running = None;
    let request = projection_request(
        locator.clone(),
        Some(exact('a', "worker", "run-1")),
        1,
        terminal,
    );
    assert!(project_resolved_agent_summary(&request).is_err());

    let mut missing_exact =
        projection_request(locator.clone(), None, 1, record_running());
    missing_exact.owner_facts.capabilities = caps(&["stop"]);
    assert!(project_resolved_agent_summary(&missing_exact).is_err());

    let mut missing_handle = projection_request(
        locator.clone(),
        Some(exact('a', "worker", "run-1")),
        1,
        record_running(),
    );
    missing_handle.owner_facts.capabilities = caps(&["content.read"]);
    assert!(project_resolved_agent_summary(&missing_handle).is_err());

    let mut bad_handle = projection_request(
        locator.clone(),
        Some(exact('a', "worker", "run-1")),
        1,
        record_running(),
    );
    bad_handle.owner_facts.capabilities = caps(&["content.read"]);
    let mut path_handle = handle(&locator);
    path_handle.id = "../secret".to_string();
    bad_handle.owner_facts.content_handles = vec![path_handle];
    assert!(project_resolved_agent_summary(&bad_handle).is_err());

    let other = logical('b', "worker");
    let mut wrong_revision = projection_request(
        locator,
        Some(exact('a', "worker", "run-1")),
        1,
        record_running(),
    );
    wrong_revision.owner_facts.row_revision = revision(&other, 1);
    assert!(project_resolved_agent_summary(&wrong_revision).is_err());
}
#[test]
fn agent_session_role_distinguishes_root_member_and_historical_shell() {
    // A live root: no tracked parent.
    let root_request = projection_request(
        logical('a', "root"),
        Some(exact('a', "root", "run-1")),
        1,
        record_running(),
    );
    let root = project_resolved_agent_summary(&root_request).unwrap();
    assert_eq!(root.agent_session_role, FleetAgentSessionRoleWire::Root);
    assert_eq!(root.parent_timestamp, None);
    assert_eq!(root.status_bucket, FleetStatusBucketWire::Running);

    // A live member: tracked parent_timestamp.
    let mut member_record = record_running();
    member_record.agent_meta.as_mut().unwrap().parent_timestamp =
        Some("20260906110000".to_string());
    let member_request = projection_request(
        logical('a', "member"),
        Some(exact('a', "member", "run-1")),
        1,
        member_record,
    );
    let member = project_resolved_agent_summary(&member_request).unwrap();
    assert_eq!(member.agent_session_role, FleetAgentSessionRoleWire::Member);
    assert_eq!(member.parent_timestamp, Some("20260906110000".to_string()));

    // A live --plan shell with agent_session_id and no parent_timestamp is a
    // nested member, never a root.
    let mut plan_record = record_running();
    plan_record.agent_meta.as_mut().unwrap().name =
        Some("0n--plan".to_string());
    plan_record.agent_meta.as_mut().unwrap().parent_timestamp = None;
    let plan_request = projection_request(
        logical('a', "plan"),
        Some(exact('a', "plan", "run-1")),
        1,
        plan_record,
    );
    let plan = project_resolved_agent_summary(&plan_request).unwrap();
    assert_eq!(plan.agent_session_role, FleetAgentSessionRoleWire::Member);
    assert_eq!(plan.parent_timestamp, None);

    // A genuinely completed record is a historical shell.
    let done = summary_done('a', "done", 1, 1000.0);
    assert_eq!(
        done.agent_session_role,
        FleetAgentSessionRoleWire::HistoricalShell
    );

    // A Dead active-tier record (not yet done, not protected) demotes
    // into a historical shell and a stopped bucket, never running.
    let mut demoted_request = projection_request(
        logical('a', "demoted"),
        Some(exact('a', "demoted", "run-1")),
        1,
        record_running(),
    );
    demoted_request.owner_facts.liveness = OwnerLivenessWire::Dead;
    demoted_request.owner_facts.current_instance = false;
    demoted_request.owner_facts.occupied_runner_slot = false;
    demoted_request.owner_facts.capabilities = caps(&[]);
    let demoted = project_resolved_agent_summary(&demoted_request).unwrap();
    assert_eq!(
        demoted.agent_session_role,
        FleetAgentSessionRoleWire::HistoricalShell
    );
    assert_eq!(demoted.status_bucket, FleetStatusBucketWire::Stopped);
    assert_eq!(demoted.lifecycle, FleetLifecycleWire::Running);

    // A waiting record protected by a marker stays Root/Waiting even if
    // its owner liveness is Dead: a waiting/question marker must never
    // be demoted.
    let mut protected_record = record_running();
    protected_record.running = None;
    protected_record.waiting =
        Some(crate::agent_scan::WaitingMarkerWire::default());
    let mut protected_request = projection_request(
        logical('a', "protected"),
        Some(exact('a', "protected", "run-1")),
        1,
        protected_record,
    );
    protected_request.owner_facts.liveness = OwnerLivenessWire::Dead;
    protected_request.owner_facts.current_instance = false;
    protected_request.owner_facts.occupied_runner_slot = false;
    protected_request.owner_facts.capabilities = caps(&[]);
    let protected = project_resolved_agent_summary(&protected_request).unwrap();
    assert_eq!(
        protected.agent_session_role,
        FleetAgentSessionRoleWire::Root
    );
    assert_eq!(protected.status_bucket, FleetStatusBucketWire::Waiting);
}
#[test]
fn dead_or_not_process_liveness_never_counts_as_running() {
    let mut alive_request = projection_request(
        logical('a', "alive"),
        Some(exact('a', "alive", "run-1")),
        1,
        record_running(),
    );
    alive_request.owner_facts.occupied_runner_slot = false;
    let alive = project_resolved_agent_summary(&alive_request).unwrap();

    let mut dead_request = projection_request(
        logical('a', "dead"),
        Some(exact('a', "dead", "run-1")),
        1,
        record_running(),
    );
    dead_request.owner_facts.liveness = OwnerLivenessWire::Dead;
    dead_request.owner_facts.occupied_runner_slot = false;
    dead_request.owner_facts.capabilities = caps(&[]);
    let dead = project_resolved_agent_summary(&dead_request).unwrap();
    assert_eq!(dead.status_bucket, FleetStatusBucketWire::Stopped);
    // Still counted as a logical agent (it is still a served row) even
    // though liveness alone keeps it out of the running count below:
    // this proves `counts_as_running` gates on liveness directly rather
    // than trusting the status bucket or `current_instance` alone.
    assert!(dead.current_instance);

    let counts = count_logical_agents(&FleetLogicalAgentCountsRequestWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        summaries: vec![alive.clone(), dead],
    })
    .unwrap();
    assert_eq!(counts.logical_agent_total, 2);
    assert_eq!(counts.running, 1);
}
