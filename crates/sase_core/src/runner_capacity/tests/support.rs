//! Shared builders for runner-capacity tests.

use super::super::*;
use crate::agent_hold::{
    AgentHoldArmerKindWire, AgentHoldArmerWire, AgentHoldRecordWire,
    AgentHoldScopeWire, AgentHoldSelectorsWire,
};
use chrono::{DateTime, Utc};

pub(super) fn rec(name: &str) -> RunnerCapacityRecordWire {
    RunnerCapacityRecordWire {
        artifact_dir: format!("/tmp/{name}"),
        project_name: "proj".to_string(),
        workflow_dir_name: "ace-run".to_string(),
        timestamp: name.to_string(),
        agent_name: None,
        workflow: None,
        clan: None,
        tribe: None,
        tribes: Vec::new(),
        created_at: None,
        has_agent_meta: true,
        has_done_marker: false,
        appears_as_agent: true,
        live: true,
        pending_question: false,
        pid: None,
        run_started_at: None,
        parent_timestamp: None,
        agent_family: None,
        agent_family_role: None,
        agent_family_parallel: false,
        runner_claim_owner_key: None,
        family_shell_kind: None,
        family_shell_id: None,
        family_shell_state: None,
        queue_weight: None,
        queue_weight_explicit: false,
        queue_weight_invalid: false,
        slot_requested_at: None,
        queue_capacity: None,
        queue_capacity_explicit: false,
        wait_runners: None,
        wait_runners_explicit: false,
        wait_priority: None,
        eligible_since: None,
    }
}

pub(super) fn running(
    name: &str,
    weight: Option<f64>,
) -> RunnerCapacityRecordWire {
    let mut record = rec(name);
    record.run_started_at = Some("2026-09-10T00:00:00Z".to_string());
    record.queue_weight = weight;
    record
}

pub(super) fn waiting(
    name: &str,
    requested_at: &str,
    weight: Option<f64>,
) -> RunnerCapacityRecordWire {
    let mut record = rec(name);
    record.slot_requested_at = Some(requested_at.to_string());
    record.queue_weight = weight;
    record
}

pub(super) fn snapshot(
    effective_limit: f64,
    records: Vec<RunnerCapacityRecordWire>,
) -> RunnerCapacitySnapshotWire {
    snapshot_with_flags(effective_limit, records, &[])
}

pub(super) fn snapshot_with_flags(
    effective_limit: f64,
    records: Vec<RunnerCapacityRecordWire>,
    feature_flags: &[String],
) -> RunnerCapacitySnapshotWire {
    runner_capacity_snapshot(&RunnerCapacityRequestWire {
        schema_version: RUNNER_CAPACITY_POLICY_SCHEMA_VERSION,
        effective_limit,
        records,
        holds: Vec::new(),
        candidate: None,
        now: Some("2026-09-10T00:01:00Z".to_string()),
        deference_seconds_per_step: 5,
        deference_max_seconds: 60,
        feature_flags: feature_flags.to_vec(),
    })
}

pub(super) fn snapshot_with_candidate(
    effective_limit: f64,
    records: Vec<RunnerCapacityRecordWire>,
    candidate: RunnerCapacityRecordWire,
) -> RunnerCapacitySnapshotWire {
    runner_capacity_snapshot(&RunnerCapacityRequestWire {
        schema_version: RUNNER_CAPACITY_POLICY_SCHEMA_VERSION,
        effective_limit,
        records,
        holds: Vec::new(),
        candidate: Some(candidate),
        now: Some("2026-09-10T00:01:00Z".to_string()),
        deference_seconds_per_step: 5,
        deference_max_seconds: 60,
        feature_flags: Vec::new(),
    })
}

pub(super) fn snapshot_with_holds(
    effective_limit: f64,
    records: Vec<RunnerCapacityRecordWire>,
    holds: Vec<AgentHoldRecordWire>,
) -> RunnerCapacitySnapshotWire {
    runner_capacity_snapshot(&RunnerCapacityRequestWire {
        schema_version: RUNNER_CAPACITY_POLICY_SCHEMA_VERSION,
        effective_limit,
        records,
        holds,
        candidate: None,
        now: Some("2026-09-10T00:01:00Z".to_string()),
        deference_seconds_per_step: 5,
        deference_max_seconds: 60,
        feature_flags: Vec::new(),
    })
}

pub(super) fn snapshot_with_candidate_and_holds(
    effective_limit: f64,
    records: Vec<RunnerCapacityRecordWire>,
    candidate: RunnerCapacityRecordWire,
    holds: Vec<AgentHoldRecordWire>,
) -> RunnerCapacitySnapshotWire {
    runner_capacity_snapshot(&RunnerCapacityRequestWire {
        schema_version: RUNNER_CAPACITY_POLICY_SCHEMA_VERSION,
        effective_limit,
        records,
        holds,
        candidate: Some(candidate),
        now: Some("2026-09-10T00:01:00Z".to_string()),
        deference_seconds_per_step: 5,
        deference_max_seconds: 60,
        feature_flags: Vec::new(),
    })
}

pub(super) fn epoch(value: &str) -> f64 {
    DateTime::parse_from_rfc3339(value)
        .unwrap()
        .with_timezone(&Utc)
        .timestamp() as f64
}

pub(super) fn hold(
    key: &str,
    selectors: AgentHoldSelectorsWire,
) -> AgentHoldRecordWire {
    AgentHoldRecordWire {
        schema_version: crate::agent_hold::AGENT_HOLD_WIRE_SCHEMA_VERSION,
        armer: AgentHoldArmerWire {
            kind: AgentHoldArmerKindWire::Agent,
            key: key.to_string(),
            display: format!("{key} display"),
            project: "proj".to_string(),
            agent_name: Some("holder.agent--code".to_string()),
            family: Some("holder.agent".to_string()),
            clan: Some("holder-clan".to_string()),
            proc_id: None,
            pid: Some(123),
            done_marker_path: None,
        },
        scope: AgentHoldScopeWire::Project {
            project: "proj".to_string(),
        },
        selectors,
        created_at: epoch("2026-09-10T00:00:00Z"),
        expires_at: epoch("2026-09-10T00:03:00Z"),
        capture: None,
    }
}

pub(super) fn identity_waiter(name: &str) -> RunnerCapacityRecordWire {
    let mut record = waiting(name, "2026-09-10T00:00:30Z", Some(1.0));
    record.agent_name = Some("target.agent--code".to_string());
    record.agent_family = Some("target.agent".to_string());
    record.workflow = Some("build".to_string());
    record.clan = Some("blocked-clan".to_string());
    record.tribe = Some("ops".to_string());
    record.created_at = Some(epoch("2026-09-10T00:00:30Z"));
    record
}

pub(super) fn capacity_budget_flags() -> Vec<String> {
    vec!["queue_capacity_budget".to_string()]
}

pub(super) fn waiter<'a>(
    snapshot: &'a RunnerCapacitySnapshotWire,
    name: &str,
) -> &'a RunnerCapacityWaiterWire {
    let artifact_dir = format!("/tmp/{name}");
    snapshot
        .waiters
        .iter()
        .find(|waiter| waiter.artifact_dir == artifact_dir)
        .expect("waiter should be present")
}
