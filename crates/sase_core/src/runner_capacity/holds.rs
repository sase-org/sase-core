use chrono::{DateTime, Utc};

use crate::agent_hold::{
    hold_blocks_candidate, AgentHoldBlockWire, AgentHoldCandidateWire,
};

use super::wire::{
    blocker, RunnerCapacityBlockerWire, RunnerCapacityRecordWire,
    RunnerCapacityRequestWire,
};

pub(super) fn hold_barrier_blockers(
    request: &RunnerCapacityRequestWire,
    record: &RunnerCapacityRecordWire,
) -> Vec<RunnerCapacityBlockerWire> {
    if request.holds.is_empty() {
        return Vec::new();
    }
    let candidate = hold_candidate(record);
    let now = request.now.as_deref().and_then(epoch_seconds_from_rfc3339);
    let mut blockers = Vec::new();
    for hold in &request.holds {
        let Ok(block) = hold_blocks_candidate(hold, &candidate) else {
            continue;
        };
        if let Some(block) = block {
            blockers.push(hold_barrier_blocker(&block, now));
        }
    }
    blockers
}

fn hold_candidate(record: &RunnerCapacityRecordWire) -> AgentHoldCandidateWire {
    AgentHoldCandidateWire {
        project: record.project_name.clone(),
        created_at: record.created_at.unwrap_or(1.0),
        artifact_dirs: vec![record.artifact_dir.clone()],
        agent_name: record.agent_name.clone(),
        proc_shell: None,
        agent_session: record.agent_session.clone(),
        clan: record.clan.clone(),
        workflow: record.workflow.clone(),
        tribe: record.tribe.clone(),
        tribes: record.tribes.clone(),
        armer_key: None,
    }
}

fn hold_barrier_blocker(
    block: &AgentHoldBlockWire,
    now: Option<f64>,
) -> RunnerCapacityBlockerWire {
    let expires = format_hold_expires_in(block.expires_at, now);
    let message =
        format!("held by {} (expires in {expires})", block.armer.display);
    let mut blocker =
        blocker("hold-barrier", &message, None, None, None, None, None);
    blocker.held_by = Some(block.armer.key.clone());
    blocker.hold_expires_at = Some(block.expires_at);
    blocker
}

fn epoch_seconds_from_rfc3339(value: &str) -> Option<f64> {
    let parsed = DateTime::parse_from_rfc3339(value).ok()?;
    Some(parsed.with_timezone(&Utc).timestamp_millis() as f64 / 1000.0)
}

fn format_hold_expires_in(expires_at: f64, now: Option<f64>) -> String {
    let Some(now) = now else {
        return format!("{expires_at:.0}s");
    };
    let seconds = (expires_at - now).max(0.0).ceil();
    if seconds < 60.0 {
        return format!("{seconds:.0}s");
    }
    let minutes = (seconds / 60.0).ceil();
    if minutes < 60.0 {
        return format!("{minutes:.0}m");
    }
    let hours = (minutes / 60.0).ceil();
    if hours < 48.0 {
        return format!("{hours:.0}h");
    }
    let days = (hours / 24.0).ceil();
    format!("{days:.0}d")
}
