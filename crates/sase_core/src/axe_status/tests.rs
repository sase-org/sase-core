use serde_json::{json, Value};

use super::*;

fn absent_process() -> AxeProcessObservationWire {
    AxeProcessObservationWire {
        pid: None,
        live: None,
    }
}

fn process(pid: u32, live: bool) -> AxeProcessObservationWire {
    AxeProcessObservationWire {
        pid: Some(pid),
        live: Some(live),
    }
}

fn stopped_orchestrator() -> AxeOrchestratorObservationWire {
    AxeOrchestratorObservationWire {
        lifecycle_lock_held: false,
        lock_holder: absent_process(),
        orchestrator_pid_file: absent_process(),
        legacy_pid_file: absent_process(),
    }
}

fn running_orchestrator(pid: u32) -> AxeOrchestratorObservationWire {
    AxeOrchestratorObservationWire {
        lifecycle_lock_held: true,
        lock_holder: process(pid, true),
        orchestrator_pid_file: process(pid, true),
        legacy_pid_file: process(pid, true),
    }
}

fn request() -> AxeStatusRequestWire {
    AxeStatusRequestWire {
        schema_version: AXE_STATUS_SCHEMA_VERSION,
        generated_at: "2026-07-23T12:00:00-04:00".to_string(),
        desired_state: None,
        orchestrator: stopped_orchestrator(),
        maintenance: None,
        hook_runners: AxeRunnerOccupancyWire {
            current: 0,
            maximum: 3,
        },
        agent_runners: AxeRunnerOccupancyWire {
            current: 0,
            maximum: 4,
        },
        lumberjacks: Vec::new(),
        latest_lifecycle_event: None,
        collection_error: None,
    }
}

fn desired(state: AxeDesiredStateValueWire) -> AxeDesiredStateWire {
    AxeDesiredStateWire {
        state,
        source: "test".to_string(),
        timestamp: "2026-07-23T11:55:00-04:00".to_string(),
    }
}

fn healthy_lumberjack(name: &str) -> AxeLumberjackObservationWire {
    AxeLumberjackObservationWire {
        name: name.to_string(),
        configured: true,
        interval_seconds: Some(60),
        configured_chops: vec!["review".to_string(), "sync".to_string()],
        recorded_pid: Some(200),
        reported_state: Some(AxeLumberjackReportedStateWire::Running),
        process_live: Some(true),
        started_at: Some("2026-07-23T11:50:00-04:00".to_string()),
        start_age_seconds: Some(600),
        heartbeat_at: Some("2026-07-23T11:59:30-04:00".to_string()),
        heartbeat_age_seconds: Some(30),
        cycles_run: 10,
        errors_encountered: 0,
        uptime_seconds: 600,
    }
}

fn snapshot_with_worker(
    worker: AxeLumberjackObservationWire,
) -> AxeStatusSnapshotWire {
    let mut input = request();
    input.orchestrator = running_orchestrator(100);
    input.desired_state = Some(desired(AxeDesiredStateValueWire::Running));
    input.lumberjacks = vec![worker];
    classify_axe_status(&input).unwrap()
}

#[test]
fn lifecycle_matrix_preserves_intent_separately_from_health() {
    let mut input = request();
    input.orchestrator = running_orchestrator(100);
    input.desired_state = Some(desired(AxeDesiredStateValueWire::Running));
    input.lumberjacks = vec![healthy_lumberjack("hooks")];
    let running = classify_axe_status(&input).unwrap();
    assert_eq!(running.state, AxeStatusStateWire::Running);
    assert_eq!(running.health, AxeStatusHealthWire::Healthy);
    assert_eq!(running.exit_code, 0);
    assert_eq!(running.summary, "AXE is running and healthy.");
    assert!(running.issues.is_empty());

    input.orchestrator = stopped_orchestrator();
    input.desired_state = Some(desired(AxeDesiredStateValueWire::Stopped));
    input.lumberjacks[0].process_live = Some(false);
    let stopped = classify_axe_status(&input).unwrap();
    assert_eq!(stopped.state, AxeStatusStateWire::Stopped);
    assert_eq!(stopped.health, AxeStatusHealthWire::Healthy);
    assert_eq!(stopped.exit_code, 0);
    assert!(stopped.issues.is_empty());

    input.desired_state = None;
    let not_started = classify_axe_status(&input).unwrap();
    assert_eq!(not_started.state, AxeStatusStateWire::NotStarted);
    assert_eq!(not_started.health, AxeStatusHealthWire::Healthy);
    assert_eq!(not_started.exit_code, 0);

    input.desired_state = Some(desired(AxeDesiredStateValueWire::Running));
    let down = classify_axe_status(&input).unwrap();
    assert_eq!(down.state, AxeStatusStateWire::Down);
    assert_eq!(down.health, AxeStatusHealthWire::Unhealthy);
    assert_eq!(down.exit_code, 1);
    assert_eq!(down.issues.len(), 1);
    assert_eq!(down.issues[0].code, "desired_running_but_down");
    assert_eq!(
        down.issues[0].suggested_command.as_deref(),
        Some("sase axe ensure")
    );

    input.orchestrator = running_orchestrator(100);
    input.lumberjacks[0] = healthy_lumberjack("hooks");
    input.maintenance = Some(AxeMaintenanceWire {
        reason: "deploying".to_string(),
        owner_pid: 333,
        started_at: "2026-07-23T11:59:00-04:00".to_string(),
        age_seconds: 60,
    });
    let maintenance = classify_axe_status(&input).unwrap();
    assert_eq!(maintenance.state, AxeStatusStateWire::Maintenance);
    assert_eq!(maintenance.health, AxeStatusHealthWire::Healthy);
    assert_eq!(maintenance.exit_code, 0);
    assert_eq!(
        maintenance.maintenance.as_ref().unwrap().reason,
        "deploying"
    );
}

#[test]
fn orchestrator_lock_without_live_pid_has_exact_evidence() {
    let mut input = request();
    input.orchestrator.lifecycle_lock_held = true;
    input.orchestrator.lock_holder = process(100, false);
    let snapshot = classify_axe_status(&input).unwrap();
    assert_eq!(snapshot.state, AxeStatusStateWire::Degraded);
    assert_eq!(
        snapshot.orchestrator.state,
        AxeOrchestratorStateWire::Incoherent
    );
    assert!(snapshot.orchestrator.live_pids.is_empty());
    assert_eq!(
        snapshot.issues[0],
        AxeStatusIssueWire {
            code: "orchestrator_lock_without_live_pid".to_string(),
            severity: AxeStatusIssueSeverityWire::Error,
            subject: Some("orchestrator".to_string()),
            summary: "The AXE lifecycle lock is held, but no live orchestrator PID could be resolved.".to_string(),
            suggested_command: Some("sase axe stop --force".to_string()),
        }
    );
}

#[test]
fn orchestrator_live_pid_without_lock_has_exact_evidence() {
    let mut input = request();
    input.orchestrator.orchestrator_pid_file = process(100, true);
    let snapshot = classify_axe_status(&input).unwrap();
    assert_eq!(snapshot.state, AxeStatusStateWire::Degraded);
    assert_eq!(snapshot.orchestrator.live_pids, vec![100]);
    assert_eq!(
        snapshot.issues[0].code,
        "orchestrator_live_pid_without_lock"
    );
    assert_eq!(
        snapshot.issues[0].summary,
        "Live orchestrator PID 100 was observed, but the AXE lifecycle lock is not held."
    );
}

#[test]
fn orchestrator_conflicting_live_identities_are_sorted_and_exact() {
    let mut input = request();
    input.orchestrator = AxeOrchestratorObservationWire {
        lifecycle_lock_held: true,
        lock_holder: process(300, true),
        orchestrator_pid_file: process(100, true),
        legacy_pid_file: process(200, true),
    };
    let snapshot = classify_axe_status(&input).unwrap();
    assert_eq!(snapshot.state, AxeStatusStateWire::Degraded);
    assert_eq!(snapshot.orchestrator.live_pids, vec![100, 200, 300]);
    assert_eq!(
        snapshot.issues[0].code,
        "orchestrator_conflicting_live_pids"
    );
    assert_eq!(
        snapshot.issues[0].summary,
        "Conflicting live orchestrator PID identities were observed: 100, 200, 300."
    );
}

#[test]
fn lumberjack_state_precedence_and_heartbeat_boundaries_are_pinned() {
    struct Case {
        name: &'static str,
        mutate: fn(&mut AxeLumberjackObservationWire),
        expected: AxeLumberjackStateWire,
    }

    let cases = [
        Case {
            name: "missing report",
            mutate: |row| {
                row.recorded_pid = None;
                row.reported_state = None;
                row.process_live = None;
                row.started_at = None;
                row.start_age_seconds = None;
                row.heartbeat_at = None;
                row.heartbeat_age_seconds = None;
            },
            expected: AxeLumberjackStateWire::NotReporting,
        },
        Case {
            name: "dead pid",
            mutate: |row| row.process_live = Some(false),
            expected: AxeLumberjackStateWire::StaleProcess,
        },
        Case {
            name: "heartbeat at threshold",
            mutate: |row| row.heartbeat_age_seconds = Some(180),
            expected: AxeLumberjackStateWire::Running,
        },
        Case {
            name: "heartbeat past threshold",
            mutate: |row| row.heartbeat_age_seconds = Some(181),
            expected: AxeLumberjackStateWire::StaleHeartbeat,
        },
        Case {
            name: "no heartbeat inside startup grace",
            mutate: |row| {
                row.heartbeat_at = None;
                row.heartbeat_age_seconds = None;
                row.start_age_seconds = Some(180);
            },
            expected: AxeLumberjackStateWire::Running,
        },
        Case {
            name: "no heartbeat outside startup grace",
            mutate: |row| {
                row.heartbeat_at = None;
                row.heartbeat_age_seconds = None;
                row.start_age_seconds = Some(181);
            },
            expected: AxeLumberjackStateWire::StaleHeartbeat,
        },
        Case {
            name: "reported error",
            mutate: |row| {
                row.reported_state = Some(AxeLumberjackReportedStateWire::Error)
            },
            expected: AxeLumberjackStateWire::Error,
        },
        Case {
            name: "reported stopped",
            mutate: |row| {
                row.reported_state =
                    Some(AxeLumberjackReportedStateWire::Stopped)
            },
            expected: AxeLumberjackStateWire::Error,
        },
    ];

    for case in cases {
        let mut worker = healthy_lumberjack(case.name);
        (case.mutate)(&mut worker);
        let snapshot = snapshot_with_worker(worker);
        assert_eq!(
            snapshot.lumberjacks[0].state, case.expected,
            "{}",
            case.name
        );
        assert_eq!(
            snapshot.lumberjacks[0].stale_threshold_seconds,
            Some(180),
            "{}",
            case.name
        );
    }
}

#[test]
fn live_orphan_is_retained_and_dead_unconfigured_row_is_filtered() {
    let mut live = healthy_lumberjack("orphan");
    live.configured = false;
    live.interval_seconds = None;
    live.configured_chops.clear();
    live.reported_state = None;
    live.started_at = None;
    live.start_age_seconds = None;
    live.heartbeat_at = None;
    live.heartbeat_age_seconds = None;

    let mut dead = live.clone();
    dead.name = "old".to_string();
    dead.process_live = Some(false);

    let mut input = request();
    input.lumberjacks = vec![dead, live];
    let snapshot = classify_axe_status(&input).unwrap();
    assert_eq!(snapshot.state, AxeStatusStateWire::Degraded);
    assert_eq!(snapshot.lumberjacks.len(), 1);
    assert_eq!(snapshot.lumberjacks[0].name, "orphan");
    assert_eq!(
        snapshot.lumberjacks[0].state,
        AxeLumberjackStateWire::Orphaned
    );
    assert_eq!(snapshot.issues[0].code, "lumberjack_orphaned");
}

#[test]
fn historical_error_count_does_not_degrade_a_fresh_worker() {
    let mut worker = healthy_lumberjack("history");
    worker.errors_encountered = 19;
    let snapshot = snapshot_with_worker(worker);
    assert_eq!(snapshot.state, AxeStatusStateWire::Running);
    assert_eq!(snapshot.health, AxeStatusHealthWire::Healthy);
    assert_eq!(snapshot.lumberjacks[0].errors_encountered, 19);
    assert!(snapshot.issues.is_empty());
}

#[test]
fn desired_stopped_with_live_orchestrator_is_degraded() {
    let mut input = request();
    input.desired_state = Some(desired(AxeDesiredStateValueWire::Stopped));
    input.orchestrator = running_orchestrator(100);
    let snapshot = classify_axe_status(&input).unwrap();
    assert_eq!(snapshot.state, AxeStatusStateWire::Degraded);
    assert_eq!(
        snapshot
            .issues
            .iter()
            .map(|issue| issue.code.as_str())
            .collect::<Vec<_>>(),
        vec!["desired_stopped_but_running"]
    );
}

#[test]
fn live_lumberjack_without_coherent_orchestrator_is_degraded() {
    let mut input = request();
    input.lumberjacks = vec![healthy_lumberjack("partial")];
    let snapshot = classify_axe_status(&input).unwrap();
    assert_eq!(snapshot.state, AxeStatusStateWire::Degraded);
    assert_eq!(
        snapshot
            .issues
            .iter()
            .map(|issue| issue.code.as_str())
            .collect::<Vec<_>>(),
        vec!["lumberjack_without_orchestrator"]
    );
}

#[test]
fn collection_error_is_a_normal_error_snapshot_with_exit_two() {
    let mut input = request();
    input.collection_error = Some(AxeStatusCollectionErrorWire {
        code: "invalid_config".to_string(),
        message: "axe config could not be loaded".to_string(),
    });
    let snapshot = classify_axe_status(&input).unwrap();
    assert_eq!(snapshot.state, AxeStatusStateWire::Error);
    assert_eq!(snapshot.health, AxeStatusHealthWire::Error);
    assert_eq!(snapshot.exit_code, 2);
    assert_eq!(
        snapshot.summary,
        "AXE status collection failed: axe config could not be loaded"
    );
    assert_eq!(snapshot.issues[0].code, "collection_error");
    assert_eq!(
        snapshot.collection_error.as_ref().unwrap().code,
        "invalid_config"
    );
}

#[test]
fn structural_validation_returns_path_specific_errors() {
    let mut input = request();
    input.schema_version += 1;
    let error = classify_axe_status(&input).unwrap_err();
    assert_eq!(error.code, "schema_version_mismatch");
    assert_eq!(error.path, "$.schema_version");

    input = request();
    input.generated_at = " ".to_string();
    let error = classify_axe_status(&input).unwrap_err();
    assert_eq!(error.code, "blank_value");
    assert_eq!(error.path, "$.generated_at");

    input = request();
    input.lumberjacks =
        vec![healthy_lumberjack("same"), healthy_lumberjack("same")];
    let error = classify_axe_status(&input).unwrap_err();
    assert_eq!(error.code, "duplicate_lumberjack");
    assert_eq!(error.path, "$.lumberjacks[1].name");

    input = request();
    input.orchestrator.lock_holder = AxeProcessObservationWire {
        pid: None,
        live: Some(true),
    };
    let error = classify_axe_status(&input).unwrap_err();
    assert_eq!(error.code, "inconsistent_process_observation");
    assert_eq!(error.path, "$.orchestrator.lock_holder");

    input = request();
    let mut worker = healthy_lumberjack("missing-interval");
    worker.interval_seconds = None;
    input.lumberjacks = vec![worker];
    let error = classify_axe_status(&input).unwrap_err();
    assert_eq!(error.code, "missing_interval");
    assert_eq!(error.path, "$.lumberjacks[0].interval_seconds");

    input = request();
    let mut worker = healthy_lumberjack("report-without-pid");
    worker.recorded_pid = None;
    worker.process_live = None;
    input.lumberjacks = vec![worker];
    let error = classify_axe_status(&input).unwrap_err();
    assert_eq!(error.code, "report_without_pid");
    assert_eq!(error.path, "$.lumberjacks[0].reported_state");
}

#[test]
fn serde_rejects_unknown_fields_and_invalid_enum_values() {
    let mut value = serde_json::to_value(request()).unwrap();
    value
        .as_object_mut()
        .unwrap()
        .insert("surprise".to_string(), json!(true));
    let error = serde_json::from_value::<AxeStatusRequestWire>(value)
        .unwrap_err()
        .to_string();
    assert!(error.contains("unknown field `surprise`"));

    let mut value = serde_json::to_value(request()).unwrap();
    value["desired_state"] = json!({
        "state": "paused",
        "source": "test",
        "timestamp": "now"
    });
    let error = serde_json::from_value::<AxeStatusRequestWire>(value)
        .unwrap_err()
        .to_string();
    assert!(error.contains("unknown variant `paused`"));
}

#[test]
fn normalization_is_independent_of_input_order_and_duplicates() {
    let mut alpha = healthy_lumberjack("alpha");
    alpha.configured_chops =
        vec!["zeta".to_string(), "alpha".to_string(), "zeta".to_string()];
    alpha.heartbeat_age_seconds = Some(181);

    let mut beta = healthy_lumberjack("beta");
    beta.recorded_pid = None;
    beta.reported_state = None;
    beta.process_live = None;
    beta.started_at = None;
    beta.start_age_seconds = None;
    beta.heartbeat_at = None;
    beta.heartbeat_age_seconds = None;

    let mut first = request();
    first.orchestrator = AxeOrchestratorObservationWire {
        lifecycle_lock_held: true,
        lock_holder: process(300, true),
        orchestrator_pid_file: process(100, true),
        legacy_pid_file: process(300, true),
    };
    first.lumberjacks = vec![beta.clone(), alpha.clone()];

    let mut second = first.clone();
    second.orchestrator.lock_holder = process(100, true);
    second.orchestrator.orchestrator_pid_file = process(300, true);
    second.lumberjacks = vec![alpha, beta];

    let first = classify_axe_status(&first).unwrap();
    let second = classify_axe_status(&second).unwrap();
    assert_eq!(first.orchestrator.live_pids, vec![100, 300]);
    assert_eq!(first.orchestrator.live_pids, second.orchestrator.live_pids);
    assert_eq!(
        first
            .lumberjacks
            .iter()
            .map(|row| row.name.as_str())
            .collect::<Vec<_>>(),
        vec!["alpha", "beta"]
    );
    assert_eq!(first.lumberjacks[0].configured_chops, vec!["alpha", "zeta"]);
    assert_eq!(first.issues, second.issues);
    assert_eq!(
        first
            .issues
            .iter()
            .map(|issue| issue.code.as_str())
            .collect::<Vec<_>>(),
        vec![
            "orchestrator_conflicting_live_pids",
            "lumberjack_without_orchestrator",
            "lumberjack_not_reporting",
            "lumberjack_stale_heartbeat",
        ]
    );
}

#[test]
fn serialization_pins_public_field_order_nulls_lists_and_enum_values() {
    let snapshot = classify_axe_status(&request()).unwrap();
    let value = serde_json::to_value(&snapshot).unwrap();
    let keys = value
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect::<Vec<_>>();
    assert_eq!(
        keys,
        vec![
            "schema_version",
            "generated_at",
            "state",
            "health",
            "summary",
            "exit_code",
            "desired_state",
            "orchestrator",
            "maintenance",
            "hook_runners",
            "agent_runners",
            "lumberjacks",
            "latest_lifecycle_event",
            "issues",
            "collection_error",
        ]
    );
    assert_eq!(value["schema_version"], json!(1));
    assert_eq!(value["state"], json!("not_started"));
    assert_eq!(value["health"], json!("healthy"));
    assert_eq!(value["summary"], json!("AXE has not been started."));
    assert_eq!(value["exit_code"], json!(0));
    assert_eq!(value["desired_state"], Value::Null);
    assert_eq!(value["maintenance"], Value::Null);
    assert_eq!(value["lumberjacks"], json!([]));
    assert_eq!(value["latest_lifecycle_event"], Value::Null);
    assert_eq!(value["issues"], json!([]));
    assert_eq!(value["collection_error"], Value::Null);
    assert_eq!(value["orchestrator"]["coherence"], json!("coherent"));
    assert_eq!(value["orchestrator"]["live_pids"], json!([]));

    let round_trip: AxeStatusSnapshotWire =
        serde_json::from_value(value).unwrap();
    assert_eq!(round_trip, snapshot);
}
