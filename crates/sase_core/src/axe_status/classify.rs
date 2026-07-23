use std::collections::BTreeSet;

use super::wire::{
    AxeDesiredStateValueWire, AxeLumberjackObservationWire,
    AxeLumberjackReportedStateWire, AxeLumberjackStateWire,
    AxeLumberjackStatusWire, AxeOrchestratorCoherenceWire,
    AxeOrchestratorObservationWire, AxeOrchestratorStateWire,
    AxeOrchestratorStatusWire, AxeProcessObservationWire, AxeStatusError,
    AxeStatusHealthWire, AxeStatusIssueSeverityWire, AxeStatusIssueWire,
    AxeStatusRequestWire, AxeStatusSnapshotWire, AxeStatusStateWire,
    AXE_STATUS_SCHEMA_VERSION,
};

const FORCE_STOP_COMMAND: &str = "sase axe stop --force";
const ENSURE_COMMAND: &str = "sase axe ensure";
const DEEP_DOCTOR_COMMAND: &str = "sase doctor --deep";

/// Validate, normalize, and classify host-collected AXE runtime evidence.
///
/// This operation performs no filesystem, clock, or process I/O.
pub fn classify_axe_status(
    request: &AxeStatusRequestWire,
) -> Result<AxeStatusSnapshotWire, AxeStatusError> {
    validate_request(request)?;

    let orchestrator = classify_orchestrator(&request.orchestrator);
    let mut lumberjacks = request
        .lumberjacks
        .iter()
        .filter_map(classify_lumberjack)
        .collect::<Vec<_>>();
    lumberjacks.sort_by(|left, right| left.name.cmp(&right.name));

    let coherent_orchestrator =
        orchestrator.state == AxeOrchestratorStateWire::Running;
    let live_orchestrator = !orchestrator.live_pids.is_empty();
    let live_lumberjack_without_orchestrator = lumberjacks
        .iter()
        .any(|row| row.process_live == Some(true) && !coherent_orchestrator);
    let live_orphan = lumberjacks
        .iter()
        .any(|row| row.state == AxeLumberjackStateWire::Orphaned);
    let unhealthy_configured_lumberjack = lumberjacks.iter().any(|row| {
        row.configured && row.state != AxeLumberjackStateWire::Running
    });
    let orchestrator_incoherent =
        orchestrator.coherence == AxeOrchestratorCoherenceWire::Incoherent;
    let desired_stopped =
        request.desired_state.as_ref().is_some_and(|desired| {
            desired.state == AxeDesiredStateValueWire::Stopped
        });
    let desired_running =
        request.desired_state.as_ref().is_some_and(|desired| {
            desired.state == AxeDesiredStateValueWire::Running
        });

    let (state, health, exit_code) = if request.collection_error.is_some() {
        (AxeStatusStateWire::Error, AxeStatusHealthWire::Error, 2)
    } else if orchestrator_incoherent
        || live_lumberjack_without_orchestrator
        || live_orphan
        || (coherent_orchestrator && unhealthy_configured_lumberjack)
        || (coherent_orchestrator && desired_stopped)
    {
        (
            AxeStatusStateWire::Degraded,
            AxeStatusHealthWire::Unhealthy,
            1,
        )
    } else if desired_running && !coherent_orchestrator {
        (AxeStatusStateWire::Down, AxeStatusHealthWire::Unhealthy, 1)
    } else if coherent_orchestrator && request.maintenance.is_some() {
        (
            AxeStatusStateWire::Maintenance,
            AxeStatusHealthWire::Healthy,
            0,
        )
    } else if coherent_orchestrator {
        (AxeStatusStateWire::Running, AxeStatusHealthWire::Healthy, 0)
    } else if desired_stopped {
        (AxeStatusStateWire::Stopped, AxeStatusHealthWire::Healthy, 0)
    } else {
        (
            AxeStatusStateWire::NotStarted,
            AxeStatusHealthWire::Healthy,
            0,
        )
    };

    let mut issues = Vec::new();
    add_collection_issue(request, &mut issues);
    add_orchestrator_issue(&orchestrator, &mut issues);
    add_lumberjack_issues(
        &lumberjacks,
        live_orchestrator,
        coherent_orchestrator,
        &mut issues,
    );
    if coherent_orchestrator && desired_stopped {
        issues.push(issue(
            "desired_stopped_but_running",
            AxeStatusIssueSeverityWire::Warning,
            "desired_state",
            "AXE is running even though the desired state is `stopped`.",
            FORCE_STOP_COMMAND,
        ));
    }
    if state == AxeStatusStateWire::Down {
        issues.push(issue(
            "desired_running_but_down",
            AxeStatusIssueSeverityWire::Error,
            "desired_state",
            "AXE is not running even though the desired state is `running`.",
            ENSURE_COMMAND,
        ));
    }
    sort_and_deduplicate_issues(&mut issues);

    let summary = match state {
        AxeStatusStateWire::Running => {
            "AXE is running and healthy.".to_string()
        }
        AxeStatusStateWire::Maintenance => {
            "AXE is in maintenance and healthy.".to_string()
        }
        AxeStatusStateWire::Stopped => {
            "AXE is intentionally stopped.".to_string()
        }
        AxeStatusStateWire::NotStarted => {
            "AXE has not been started.".to_string()
        }
        AxeStatusStateWire::Down => {
            "AXE is down despite a requested running state.".to_string()
        }
        AxeStatusStateWire::Degraded => {
            "AXE is degraded and requires operator attention.".to_string()
        }
        AxeStatusStateWire::Error => format!(
            "AXE status collection failed: {}",
            request
                .collection_error
                .as_ref()
                .map_or("unknown collection error", |error| error
                    .message
                    .as_str())
        ),
    };

    Ok(AxeStatusSnapshotWire {
        schema_version: AXE_STATUS_SCHEMA_VERSION,
        generated_at: request.generated_at.clone(),
        state,
        health,
        summary,
        exit_code,
        desired_state: request.desired_state.clone(),
        orchestrator,
        maintenance: request.maintenance.clone(),
        hook_runners: request.hook_runners,
        agent_runners: request.agent_runners,
        lumberjacks,
        latest_lifecycle_event: request.latest_lifecycle_event.clone(),
        issues,
        collection_error: request.collection_error.clone(),
    })
}

fn validate_request(
    request: &AxeStatusRequestWire,
) -> Result<(), AxeStatusError> {
    if request.schema_version != AXE_STATUS_SCHEMA_VERSION {
        return Err(AxeStatusError::new(
            "schema_version_mismatch",
            "$.schema_version",
            format!(
                "got {}, expected {AXE_STATUS_SCHEMA_VERSION}",
                request.schema_version
            ),
        ));
    }
    require_nonblank(&request.generated_at, "$.generated_at", "generated_at")?;

    if let Some(desired) = &request.desired_state {
        require_nonblank(
            &desired.source,
            "$.desired_state.source",
            "desired-state source",
        )?;
        require_nonblank(
            &desired.timestamp,
            "$.desired_state.timestamp",
            "desired-state timestamp",
        )?;
    }

    validate_process_observation(
        &request.orchestrator.lock_holder,
        "$.orchestrator.lock_holder",
    )?;
    validate_process_observation(
        &request.orchestrator.orchestrator_pid_file,
        "$.orchestrator.orchestrator_pid_file",
    )?;
    validate_process_observation(
        &request.orchestrator.legacy_pid_file,
        "$.orchestrator.legacy_pid_file",
    )?;

    if let Some(maintenance) = &request.maintenance {
        require_nonblank(
            &maintenance.reason,
            "$.maintenance.reason",
            "maintenance reason",
        )?;
        require_positive_pid(maintenance.owner_pid, "$.maintenance.owner_pid")?;
        require_nonblank(
            &maintenance.started_at,
            "$.maintenance.started_at",
            "maintenance start timestamp",
        )?;
    }

    let mut names = BTreeSet::new();
    for (index, lumberjack) in request.lumberjacks.iter().enumerate() {
        validate_lumberjack(lumberjack, index)?;
        if !names.insert(lumberjack.name.clone()) {
            return Err(AxeStatusError::new(
                "duplicate_lumberjack",
                format!("$.lumberjacks[{index}].name"),
                format!(
                    "lumberjack name `{}` appears more than once",
                    lumberjack.name
                ),
            ));
        }
    }

    if let Some(event) = &request.latest_lifecycle_event {
        require_nonblank(
            &event.timestamp,
            "$.latest_lifecycle_event.timestamp",
            "lifecycle-event timestamp",
        )?;
        require_nonblank(
            &event.source,
            "$.latest_lifecycle_event.source",
            "lifecycle-event source",
        )?;
        require_nonblank(
            &event.outcome,
            "$.latest_lifecycle_event.outcome",
            "lifecycle-event outcome",
        )?;
        if let Some(reason) = &event.reason {
            require_nonblank(
                reason,
                "$.latest_lifecycle_event.reason",
                "lifecycle-event reason",
            )?;
        }
        if let Some(pid) = event.orchestrator_pid {
            require_positive_pid(
                pid,
                "$.latest_lifecycle_event.orchestrator_pid",
            )?;
        }
    }

    if let Some(error) = &request.collection_error {
        require_nonblank(
            &error.code,
            "$.collection_error.code",
            "collection-error code",
        )?;
        require_nonblank(
            &error.message,
            "$.collection_error.message",
            "collection-error message",
        )?;
    }

    Ok(())
}

fn validate_process_observation(
    observation: &AxeProcessObservationWire,
    path: &str,
) -> Result<(), AxeStatusError> {
    if let Some(pid) = observation.pid {
        require_positive_pid(pid, &format!("{path}.pid"))?;
    }
    if observation.pid.is_some() != observation.live.is_some() {
        return Err(AxeStatusError::new(
            "inconsistent_process_observation",
            path,
            "pid and live must either both be present or both be null",
        ));
    }
    Ok(())
}

fn validate_lumberjack(
    lumberjack: &AxeLumberjackObservationWire,
    index: usize,
) -> Result<(), AxeStatusError> {
    let path = format!("$.lumberjacks[{index}]");
    require_nonblank(
        &lumberjack.name,
        &format!("{path}.name"),
        "lumberjack name",
    )?;

    if lumberjack.interval_seconds == Some(0) {
        return Err(AxeStatusError::new(
            "non_positive_interval",
            format!("{path}.interval_seconds"),
            "lumberjack interval must be positive when present",
        ));
    }
    if lumberjack.configured && lumberjack.interval_seconds.is_none() {
        return Err(AxeStatusError::new(
            "missing_interval",
            format!("{path}.interval_seconds"),
            "configured lumberjack must have a positive interval",
        ));
    }
    for (chop_index, chop) in lumberjack.configured_chops.iter().enumerate() {
        require_nonblank(
            chop,
            &format!("{path}.configured_chops[{chop_index}]"),
            "configured chop name",
        )?;
    }

    if let Some(pid) = lumberjack.recorded_pid {
        require_positive_pid(pid, &format!("{path}.recorded_pid"))?;
    }
    if lumberjack.recorded_pid.is_some() != lumberjack.process_live.is_some() {
        return Err(AxeStatusError::new(
            "inconsistent_process_observation",
            format!("{path}.process_live"),
            "recorded_pid and process_live must either both be present or both be null",
        ));
    }
    if lumberjack.reported_state.is_some() && lumberjack.recorded_pid.is_none()
    {
        return Err(AxeStatusError::new(
            "report_without_pid",
            format!("{path}.reported_state"),
            "reported state requires a recorded PID",
        ));
    }

    validate_timestamp_age_pair(
        &lumberjack.started_at,
        lumberjack.start_age_seconds,
        &format!("{path}.started_at"),
        &format!("{path}.start_age_seconds"),
        "start",
    )?;
    validate_timestamp_age_pair(
        &lumberjack.heartbeat_at,
        lumberjack.heartbeat_age_seconds,
        &format!("{path}.heartbeat_at"),
        &format!("{path}.heartbeat_age_seconds"),
        "heartbeat",
    )?;

    if lumberjack.reported_state.is_none()
        && (lumberjack.started_at.is_some()
            || lumberjack.start_age_seconds.is_some()
            || lumberjack.heartbeat_at.is_some()
            || lumberjack.heartbeat_age_seconds.is_some())
    {
        return Err(AxeStatusError::new(
            "reporting_details_without_state",
            path,
            "reporting timestamps and ages require a reported state",
        ));
    }

    Ok(())
}

fn validate_timestamp_age_pair(
    timestamp: &Option<String>,
    age: Option<u64>,
    timestamp_path: &str,
    age_path: &str,
    label: &str,
) -> Result<(), AxeStatusError> {
    if let Some(timestamp) = timestamp {
        require_nonblank(
            timestamp,
            timestamp_path,
            &format!("{label} timestamp"),
        )?;
    }
    if timestamp.is_some() != age.is_some() {
        return Err(AxeStatusError::new(
            "inconsistent_timestamp_age",
            if timestamp.is_some() {
                age_path
            } else {
                timestamp_path
            },
            format!("{label} timestamp and age must both be present or both be null"),
        ));
    }
    Ok(())
}

fn require_nonblank(
    value: &str,
    path: &str,
    label: &str,
) -> Result<(), AxeStatusError> {
    if value.trim().is_empty() {
        return Err(AxeStatusError::new(
            "blank_value",
            path,
            format!("{label} must not be blank"),
        ));
    }
    Ok(())
}

fn require_positive_pid(pid: u32, path: &str) -> Result<(), AxeStatusError> {
    if pid == 0 {
        return Err(AxeStatusError::new(
            "non_positive_pid",
            path,
            "PID must be positive",
        ));
    }
    Ok(())
}

fn classify_orchestrator(
    observation: &AxeOrchestratorObservationWire,
) -> AxeOrchestratorStatusWire {
    let live_pids = [
        observation.lock_holder,
        observation.orchestrator_pid_file,
        observation.legacy_pid_file,
    ]
    .into_iter()
    .filter_map(|source| {
        (source.live == Some(true)).then_some(source.pid).flatten()
    })
    .collect::<BTreeSet<_>>()
    .into_iter()
    .collect::<Vec<_>>();

    let (state, coherence) =
        match (observation.lifecycle_lock_held, live_pids.len()) {
            (true, 1) => (
                AxeOrchestratorStateWire::Running,
                AxeOrchestratorCoherenceWire::Coherent,
            ),
            (false, 0) => (
                AxeOrchestratorStateWire::Stopped,
                AxeOrchestratorCoherenceWire::Coherent,
            ),
            _ => (
                AxeOrchestratorStateWire::Incoherent,
                AxeOrchestratorCoherenceWire::Incoherent,
            ),
        };

    AxeOrchestratorStatusWire {
        state,
        coherence,
        live_pids,
        lifecycle_lock_held: observation.lifecycle_lock_held,
        lock_holder: observation.lock_holder,
        orchestrator_pid_file: observation.orchestrator_pid_file,
        legacy_pid_file: observation.legacy_pid_file,
    }
}

fn classify_lumberjack(
    observation: &AxeLumberjackObservationWire,
) -> Option<AxeLumberjackStatusWire> {
    if !observation.configured && observation.process_live != Some(true) {
        return None;
    }

    let stale_threshold_seconds = observation
        .interval_seconds
        .map(|interval| 60_u64.max(interval.saturating_mul(3)));
    let state = if !observation.configured {
        AxeLumberjackStateWire::Orphaned
    } else if observation.reported_state.is_none() {
        AxeLumberjackStateWire::NotReporting
    } else if observation.process_live != Some(true) {
        AxeLumberjackStateWire::StaleProcess
    } else if matches!(
        observation.reported_state,
        Some(
            AxeLumberjackReportedStateWire::Error
                | AxeLumberjackReportedStateWire::Stopped
        )
    ) {
        AxeLumberjackStateWire::Error
    } else if stale_threshold_seconds.is_some_and(|threshold| {
        observation
            .heartbeat_age_seconds
            .is_some_and(|age| age > threshold)
            || (observation.heartbeat_at.is_none()
                && observation
                    .start_age_seconds
                    .is_some_and(|age| age > threshold))
    }) {
        AxeLumberjackStateWire::StaleHeartbeat
    } else {
        AxeLumberjackStateWire::Running
    };

    let mut configured_chops = observation.configured_chops.clone();
    configured_chops.sort();
    configured_chops.dedup();

    Some(AxeLumberjackStatusWire {
        name: observation.name.clone(),
        state,
        stale_threshold_seconds,
        configured: observation.configured,
        interval_seconds: observation.interval_seconds,
        configured_chops,
        recorded_pid: observation.recorded_pid,
        reported_state: observation.reported_state,
        process_live: observation.process_live,
        started_at: observation.started_at.clone(),
        start_age_seconds: observation.start_age_seconds,
        heartbeat_at: observation.heartbeat_at.clone(),
        heartbeat_age_seconds: observation.heartbeat_age_seconds,
        cycles_run: observation.cycles_run,
        errors_encountered: observation.errors_encountered,
        uptime_seconds: observation.uptime_seconds,
    })
}

fn add_collection_issue(
    request: &AxeStatusRequestWire,
    issues: &mut Vec<AxeStatusIssueWire>,
) {
    if let Some(error) = &request.collection_error {
        issues.push(issue(
            "collection_error",
            AxeStatusIssueSeverityWire::Error,
            "collection",
            format!(
                "AXE status collection failed [{}]: {}",
                error.code, error.message
            ),
            DEEP_DOCTOR_COMMAND,
        ));
    }
}

fn add_orchestrator_issue(
    orchestrator: &AxeOrchestratorStatusWire,
    issues: &mut Vec<AxeStatusIssueWire>,
) {
    if orchestrator.live_pids.len() > 1 {
        let pids = orchestrator
            .live_pids
            .iter()
            .map(u32::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        issues.push(issue(
            "orchestrator_conflicting_live_pids",
            AxeStatusIssueSeverityWire::Error,
            "orchestrator",
            format!(
                "Conflicting live orchestrator PID identities were observed: {pids}."
            ),
            FORCE_STOP_COMMAND,
        ));
    } else if orchestrator.lifecycle_lock_held
        && orchestrator.live_pids.is_empty()
    {
        issues.push(issue(
            "orchestrator_lock_without_live_pid",
            AxeStatusIssueSeverityWire::Error,
            "orchestrator",
            "The AXE lifecycle lock is held, but no live orchestrator PID could be resolved.",
            FORCE_STOP_COMMAND,
        ));
    } else if !orchestrator.lifecycle_lock_held
        && orchestrator.live_pids.len() == 1
    {
        issues.push(issue(
            "orchestrator_live_pid_without_lock",
            AxeStatusIssueSeverityWire::Error,
            "orchestrator",
            format!(
                "Live orchestrator PID {} was observed, but the AXE lifecycle lock is not held.",
                orchestrator.live_pids[0]
            ),
            FORCE_STOP_COMMAND,
        ));
    }
}

fn add_lumberjack_issues(
    lumberjacks: &[AxeLumberjackStatusWire],
    live_orchestrator: bool,
    coherent_orchestrator: bool,
    issues: &mut Vec<AxeStatusIssueWire>,
) {
    for lumberjack in lumberjacks {
        if lumberjack.state == AxeLumberjackStateWire::Orphaned {
            issues.push(issue(
                "lumberjack_orphaned",
                AxeStatusIssueSeverityWire::Warning,
                &lumberjack.name,
                format!(
                    "Live unconfigured lumberjack `{}` is orphaned (PID {}).",
                    lumberjack.name,
                    lumberjack.recorded_pid.unwrap_or_default()
                ),
                FORCE_STOP_COMMAND,
            ));
            continue;
        }

        if lumberjack.process_live == Some(true) && !coherent_orchestrator {
            issues.push(issue(
                "lumberjack_without_orchestrator",
                AxeStatusIssueSeverityWire::Warning,
                &lumberjack.name,
                format!(
                    "Lumberjack `{}` is live (PID {}) without a coherent orchestrator.",
                    lumberjack.name,
                    lumberjack.recorded_pid.unwrap_or_default()
                ),
                FORCE_STOP_COMMAND,
            ));
        }

        if !live_orchestrator {
            continue;
        }

        let (code, summary) = match lumberjack.state {
            AxeLumberjackStateWire::Running => continue,
            AxeLumberjackStateWire::NotReporting => (
                "lumberjack_not_reporting",
                format!(
                    "Configured lumberjack `{}` is not reporting status.",
                    lumberjack.name
                ),
            ),
            AxeLumberjackStateWire::StaleProcess => (
                "lumberjack_stale_process",
                format!(
                    "Configured lumberjack `{}` reports PID {}, but that process is not live.",
                    lumberjack.name,
                    lumberjack.recorded_pid.unwrap_or_default()
                ),
            ),
            AxeLumberjackStateWire::StaleHeartbeat => {
                let threshold =
                    lumberjack.stale_threshold_seconds.unwrap_or_default();
                if let Some(age) = lumberjack.heartbeat_age_seconds {
                    (
                        "lumberjack_stale_heartbeat",
                        format!(
                            "Configured lumberjack `{}` has a stale heartbeat ({age}s; threshold {threshold}s).",
                            lumberjack.name
                        ),
                    )
                } else {
                    (
                        "lumberjack_stale_heartbeat",
                        format!(
                            "Configured lumberjack `{}` has not reported a heartbeat after {}s (threshold {threshold}s).",
                            lumberjack.name,
                            lumberjack.start_age_seconds.unwrap_or_default()
                        ),
                    )
                }
            }
            AxeLumberjackStateWire::Error => (
                "lumberjack_error",
                format!(
                    "Configured lumberjack `{}` reports state `{}`.",
                    lumberjack.name,
                    match lumberjack.reported_state {
                        Some(AxeLumberjackReportedStateWire::Stopped) =>
                            "stopped",
                        Some(AxeLumberjackReportedStateWire::Error) => "error",
                        _ => "unknown",
                    }
                ),
            ),
            AxeLumberjackStateWire::Orphaned => unreachable!(),
        };
        issues.push(issue(
            code,
            AxeStatusIssueSeverityWire::Warning,
            &lumberjack.name,
            summary,
            DEEP_DOCTOR_COMMAND,
        ));
    }
}

fn issue(
    code: impl Into<String>,
    severity: AxeStatusIssueSeverityWire,
    subject: impl Into<String>,
    summary: impl Into<String>,
    command: impl Into<String>,
) -> AxeStatusIssueWire {
    AxeStatusIssueWire {
        code: code.into(),
        severity,
        subject: Some(subject.into()),
        summary: summary.into(),
        suggested_command: Some(command.into()),
    }
}

fn sort_and_deduplicate_issues(issues: &mut Vec<AxeStatusIssueWire>) {
    issues.sort_by(|left, right| {
        issue_rank(&left.code)
            .cmp(&issue_rank(&right.code))
            .then_with(|| left.subject.cmp(&right.subject))
            .then_with(|| left.code.cmp(&right.code))
            .then_with(|| left.summary.cmp(&right.summary))
    });
    issues.dedup_by(|left, right| {
        left.code == right.code && left.subject == right.subject
    });
}

fn issue_rank(code: &str) -> u8 {
    match code {
        "collection_error" => 0,
        "orchestrator_conflicting_live_pids" => 10,
        "orchestrator_lock_without_live_pid" => 11,
        "orchestrator_live_pid_without_lock" => 12,
        "desired_stopped_but_running" => 20,
        "desired_running_but_down" => 21,
        "lumberjack_without_orchestrator" => 30,
        "lumberjack_orphaned" => 31,
        "lumberjack_not_reporting" => 40,
        "lumberjack_stale_process" => 41,
        "lumberjack_stale_heartbeat" => 42,
        "lumberjack_error" => 43,
        _ => 255,
    }
}
