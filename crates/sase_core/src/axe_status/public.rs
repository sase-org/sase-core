use std::collections::BTreeMap;

use serde::Serialize;

use super::wire::{
    AxeDesiredStateWire, AxeLifecycleEventWire, AxeLumberjackReportedStateWire,
    AxeLumberjackStateWire, AxeMaintenanceWire, AxeOrchestratorStatusWire,
    AxeRunnerOccupancyWire, AxeStatusCollectionErrorWire, AxeStatusHealthWire,
    AxeStatusIssueWire, AxeStatusSnapshotWire, AxeStatusStateWire,
};

/// Public routine/job status JSON schema emitted by canonical AXE surfaces.
pub const AXE_PUBLIC_STATUS_SCHEMA_VERSION: u32 = 2;

/// AXE status snapshot using the public routine/job envelope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AxePublicStatusSnapshotWire {
    pub schema_version: u32,
    pub generated_at: String,
    pub state: AxeStatusStateWire,
    pub health: AxeStatusHealthWire,
    pub summary: String,
    pub exit_code: u8,
    pub desired_state: Option<AxeDesiredStateWire>,
    pub orchestrator: AxeOrchestratorStatusWire,
    pub maintenance: Option<AxeMaintenanceWire>,
    pub hook_runners: AxeRunnerOccupancyWire,
    pub agent_runners: AxeRunnerOccupancyWire,
    pub routines: Vec<AxePublicRoutineStatusWire>,
    pub latest_lifecycle_event: Option<AxeLifecycleEventWire>,
    pub issues: Vec<AxeStatusIssueWire>,
    pub collection_error: Option<AxeStatusCollectionErrorWire>,
}

/// One routine row in the public status JSON envelope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AxePublicRoutineStatusWire {
    pub name: String,
    pub routine_name: String,
    pub state: AxeLumberjackStateWire,
    pub stale_threshold_seconds: Option<u64>,
    pub configured: bool,
    pub interval_seconds: Option<u64>,
    pub configured_jobs: Vec<String>,
    pub recorded_pid: Option<u32>,
    pub reported_state: Option<AxeLumberjackReportedStateWire>,
    pub process_live: Option<bool>,
    pub started_at: Option<String>,
    pub start_age_seconds: Option<u64>,
    pub heartbeat_at: Option<String>,
    pub heartbeat_age_seconds: Option<u64>,
    pub cycles_run: u64,
    pub errors_encountered: u64,
    pub uptime_seconds: u64,
}

/// Project the internal AXE status snapshot to the public routine/job contract.
///
/// This projection deliberately renames only owned envelope keys and AXE-owned
/// issue templates. Names, paths, free text, collection errors, and configured
/// job values remain verbatim.
pub fn project_axe_status_public(
    snapshot: &AxeStatusSnapshotWire,
) -> AxePublicStatusSnapshotWire {
    let routine_by_name = snapshot
        .lumberjacks
        .iter()
        .map(|routine| (routine.name.as_str(), routine))
        .collect::<BTreeMap<_, _>>();
    AxePublicStatusSnapshotWire {
        schema_version: AXE_PUBLIC_STATUS_SCHEMA_VERSION,
        generated_at: snapshot.generated_at.clone(),
        state: snapshot.state,
        health: snapshot.health,
        summary: snapshot.summary.clone(),
        exit_code: snapshot.exit_code,
        desired_state: snapshot.desired_state.clone(),
        orchestrator: snapshot.orchestrator.clone(),
        maintenance: snapshot.maintenance.clone(),
        hook_runners: snapshot.hook_runners,
        agent_runners: snapshot.agent_runners,
        routines: snapshot
            .lumberjacks
            .iter()
            .map(|routine| AxePublicRoutineStatusWire {
                name: routine.name.clone(),
                routine_name: routine.name.clone(),
                state: routine.state,
                stale_threshold_seconds: routine.stale_threshold_seconds,
                configured: routine.configured,
                interval_seconds: routine.interval_seconds,
                configured_jobs: routine.configured_chops.clone(),
                recorded_pid: routine.recorded_pid,
                reported_state: routine.reported_state,
                process_live: routine.process_live,
                started_at: routine.started_at.clone(),
                start_age_seconds: routine.start_age_seconds,
                heartbeat_at: routine.heartbeat_at.clone(),
                heartbeat_age_seconds: routine.heartbeat_age_seconds,
                cycles_run: routine.cycles_run,
                errors_encountered: routine.errors_encountered,
                uptime_seconds: routine.uptime_seconds,
            })
            .collect(),
        latest_lifecycle_event: snapshot.latest_lifecycle_event.clone(),
        issues: snapshot
            .issues
            .iter()
            .map(|issue| public_issue(issue, &routine_by_name))
            .collect(),
        collection_error: snapshot.collection_error.clone(),
    }
}

fn public_issue(
    issue: &AxeStatusIssueWire,
    routines: &BTreeMap<&str, &super::wire::AxeLumberjackStatusWire>,
) -> AxeStatusIssueWire {
    let Some(subject) = issue.subject.as_ref() else {
        return issue.clone();
    };
    let Some(routine) = routines.get(subject.as_str()) else {
        return issue.clone();
    };
    let Some((code, summary)) = public_routine_issue(issue, routine) else {
        return issue.clone();
    };
    AxeStatusIssueWire {
        code: code.to_string(),
        severity: issue.severity,
        subject: issue.subject.clone(),
        summary,
        suggested_command: issue.suggested_command.clone(),
    }
}

fn public_routine_issue(
    issue: &AxeStatusIssueWire,
    routine: &super::wire::AxeLumberjackStatusWire,
) -> Option<(&'static str, String)> {
    let pid = routine.recorded_pid.unwrap_or_default();
    match issue.code.as_str() {
        "lumberjack_orphaned" => Some((
            "routine_orphaned",
            format!(
                "Live unconfigured routine `{}` is orphaned (PID {pid}).",
                routine.name
            ),
        )),
        "lumberjack_without_orchestrator" => Some((
            "routine_without_orchestrator",
            format!(
                "Routine `{}` is live (PID {pid}) without a coherent orchestrator.",
                routine.name
            ),
        )),
        "lumberjack_not_reporting" => Some((
            "routine_not_reporting",
            format!(
                "Configured routine `{}` is not reporting status.",
                routine.name
            ),
        )),
        "lumberjack_stale_process" => Some((
            "routine_stale_process",
            format!(
                "Configured routine `{}` reports PID {pid}, but that process is not live.",
                routine.name
            ),
        )),
        "lumberjack_stale_heartbeat" => Some((
            "routine_stale_heartbeat",
            public_stale_heartbeat_summary(routine),
        )),
        "lumberjack_error" => Some((
            "routine_error",
            format!(
                "Configured routine `{}` reports state `{}`.",
                routine.name,
                match routine.reported_state {
                    Some(AxeLumberjackReportedStateWire::Stopped) =>
                        "stopped",
                    Some(AxeLumberjackReportedStateWire::Error) => "error",
                    _ => "unknown",
                }
            ),
        )),
        _ => None,
    }
}

fn public_stale_heartbeat_summary(
    routine: &super::wire::AxeLumberjackStatusWire,
) -> String {
    let threshold = routine.stale_threshold_seconds.unwrap_or_default();
    if let Some(age) = routine.heartbeat_age_seconds {
        format!(
            "Configured routine `{}` has a stale heartbeat ({age}s; threshold {threshold}s).",
            routine.name
        )
    } else {
        format!(
            "Configured routine `{}` has not reported a heartbeat after {}s (threshold {threshold}s).",
            routine.name,
            routine.start_age_seconds.unwrap_or_default()
        )
    }
}
