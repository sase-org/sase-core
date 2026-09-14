//! Stop-before-removal policy for forced agent-name reuse.
//!
//! The host owns process signalling and filesystem mutation. This module owns
//! the pure decision that a concrete forced-reuse cleanup batch may proceed to
//! destructive removal only after every live target has a verified stop
//! observation.

use serde::{Deserialize, Serialize};

pub const FORCE_REUSE_STOP_BARRIER_WIRE_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForceReuseStopBarrierRequestWire {
    pub schema_version: u32,
    #[serde(default)]
    pub targets: Vec<ForceReuseStopTargetWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForceReuseStopTargetWire {
    pub name: String,
    pub artifacts_dir: String,
    #[serde(default)]
    pub pid: Option<i64>,
    #[serde(default)]
    pub was_live: bool,
    #[serde(default)]
    pub stop_status: String,
    #[serde(default)]
    pub alive_after_stop: bool,
    #[serde(default)]
    pub detail: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForceReuseStopItemWire {
    pub name: String,
    pub artifacts_dir: String,
    #[serde(default)]
    pub pid: Option<i64>,
    pub stop_status: String,
    #[serde(default)]
    pub detail: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForceReuseStopBarrierDecisionWire {
    pub schema_version: u32,
    pub proceed: bool,
    #[serde(default)]
    pub stopped: Vec<ForceReuseStopItemWire>,
    #[serde(default)]
    pub unresolved: Vec<ForceReuseStopItemWire>,
    #[serde(default)]
    pub errors: Vec<String>,
}

pub fn decide_force_reuse_stop_barrier(
    request: &ForceReuseStopBarrierRequestWire,
) -> Result<ForceReuseStopBarrierDecisionWire, String> {
    if request.schema_version != FORCE_REUSE_STOP_BARRIER_WIRE_SCHEMA_VERSION {
        return Err(format!(
            "force-reuse stop barrier schema mismatch: got {}, expected {}",
            request.schema_version,
            FORCE_REUSE_STOP_BARRIER_WIRE_SCHEMA_VERSION
        ));
    }

    let mut stopped = Vec::new();
    let mut unresolved = Vec::new();
    let mut errors = Vec::new();

    for target in &request.targets {
        let item = ForceReuseStopItemWire {
            name: target.name.clone(),
            artifacts_dir: target.artifacts_dir.clone(),
            pid: target.pid,
            stop_status: target.stop_status.clone(),
            detail: target.detail.clone(),
        };
        if target.name.trim().is_empty() {
            errors.push(format!(
                "cleanup target at {} has no agent name",
                display_artifact(&target.artifacts_dir)
            ));
            unresolved.push(item);
            continue;
        }
        if target.artifacts_dir.trim().is_empty() {
            errors.push(format!(
                "cleanup target '{}' has no artifact directory",
                target.name
            ));
            unresolved.push(item);
            continue;
        }
        if target.was_live && target.pid.unwrap_or_default() <= 1 {
            errors.push(format!(
                "cleanup target '{}' at {} has no valid process id",
                target.name,
                display_artifact(&target.artifacts_dir)
            ));
            unresolved.push(item);
            continue;
        }
        if target.alive_after_stop {
            errors.push(format!(
                "cleanup target '{}' at {} is still live after stop attempt ({})",
                target.name,
                display_artifact(&target.artifacts_dir),
                status_label(&target.stop_status)
            ));
            unresolved.push(item);
            continue;
        }
        if stop_status_allows_removal(target) {
            stopped.push(item);
            continue;
        }

        errors.push(format!(
            "cleanup target '{}' at {} did not report a verified stop ({})",
            target.name,
            display_artifact(&target.artifacts_dir),
            status_label(&target.stop_status)
        ));
        unresolved.push(item);
    }

    Ok(ForceReuseStopBarrierDecisionWire {
        schema_version: FORCE_REUSE_STOP_BARRIER_WIRE_SCHEMA_VERSION,
        proceed: unresolved.is_empty() && errors.is_empty(),
        stopped,
        unresolved,
        errors,
    })
}

fn stop_status_allows_removal(target: &ForceReuseStopTargetWire) -> bool {
    matches!(
        target.stop_status.as_str(),
        "not_live"
            | "already_stopped"
            | "killed"
            | "force_killed"
            | "exited_after_sigterm"
            | "exited_after_sigkill"
    ) || (!target.was_live && target.stop_status == "identity_mismatch")
}

fn status_label(status: &str) -> &str {
    if status.is_empty() {
        "missing status"
    } else {
        status
    }
}

fn display_artifact(path: &str) -> &str {
    if path.is_empty() {
        "unknown artifact"
    } else {
        path
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn target(
        status: &str,
        alive_after_stop: bool,
    ) -> ForceReuseStopTargetWire {
        ForceReuseStopTargetWire {
            name: "worker".to_string(),
            artifacts_dir: "/tmp/worker".to_string(),
            pid: Some(1234),
            was_live: true,
            stop_status: status.to_string(),
            alive_after_stop,
            detail: None,
        }
    }

    #[test]
    fn stop_barrier_allows_verified_graceful_and_escalated_stops() {
        let request = ForceReuseStopBarrierRequestWire {
            schema_version: FORCE_REUSE_STOP_BARRIER_WIRE_SCHEMA_VERSION,
            targets: vec![
                target("killed", false),
                target("force_killed", false),
            ],
        };

        let decision = decide_force_reuse_stop_barrier(&request).unwrap();

        assert!(decision.proceed);
        assert_eq!(decision.stopped.len(), 2);
        assert!(decision.unresolved.is_empty());
        assert!(decision.errors.is_empty());
    }

    #[test]
    fn stop_barrier_blocks_when_process_is_still_live() {
        let request = ForceReuseStopBarrierRequestWire {
            schema_version: FORCE_REUSE_STOP_BARRIER_WIRE_SCHEMA_VERSION,
            targets: vec![target("force_killed", true)],
        };

        let decision = decide_force_reuse_stop_barrier(&request).unwrap();

        assert!(!decision.proceed);
        assert!(decision.stopped.is_empty());
        assert_eq!(decision.unresolved.len(), 1);
        assert!(decision.errors[0].contains("still live"));
    }

    #[test]
    fn stop_barrier_blocks_permission_failures() {
        let request = ForceReuseStopBarrierRequestWire {
            schema_version: FORCE_REUSE_STOP_BARRIER_WIRE_SCHEMA_VERSION,
            targets: vec![target("permission_denied", false)],
        };

        let decision = decide_force_reuse_stop_barrier(&request).unwrap();

        assert!(!decision.proceed);
        assert_eq!(decision.unresolved[0].stop_status, "permission_denied");
    }
}
