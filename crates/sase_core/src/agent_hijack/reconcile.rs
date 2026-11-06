//! Recovery reconciliation decisions for abandoned resolving records.
//!
//! Python observes process liveness, admission, and successor artifacts.
//! This module decides whether to keep resolving, adopt a proven successor,
//! return to held, or retain custody with a diagnostic. Names alone are
//! never proof of a successor.

use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::wire::*;

/// Wire schema version for recovery reconciliation facts/decisions.
pub const AGENT_HIJACK_RECONCILE_WIRE_SCHEMA_VERSION: u32 = 1;

fn default_min_age_seconds() -> f64 {
    60.0
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AgentHijackReconcileError {
    #[error("unsupported hijack recovery reconcile wire schema_version {0}")]
    UnsupportedSchema(u32),
}

/// Observed resolver process facts. Identity matching uses the existing
/// `boot_id:start_ticks` token convention, not a pid alone.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentHijackResolverObservationWire {
    #[serde(default)]
    pub pid_live: bool,
    /// True when the live pid's token matches the recorded resolver token.
    #[serde(default)]
    pub identity_matches: bool,
    #[serde(default)]
    pub observed_pid: Option<i64>,
    #[serde(default)]
    pub observed_identity_token: Option<String>,
}

/// Observed successor facts. `name_only` means the caller found a name
/// without launch or process identity and must not be treated as proof.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentHijackSuccessorObservationWire {
    #[serde(default)]
    pub kind: String,
    #[serde(default)]
    pub launch_identity: Option<String>,
    #[serde(default)]
    pub admission_request_id: Option<String>,
    #[serde(default)]
    pub admission_identity: Option<String>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub pid: Option<i64>,
    #[serde(default)]
    pub process_identity_token: Option<String>,
    #[serde(default)]
    pub artifacts_dir: Option<String>,
    #[serde(default)]
    pub workspace_num: Option<i64>,
    #[serde(default)]
    pub workspace_dir: Option<String>,
    #[serde(default)]
    pub launch_identity_matches: bool,
    #[serde(default)]
    pub workspace_matches: bool,
    #[serde(default)]
    pub lineage_matches: bool,
    #[serde(default)]
    pub inheritance_acknowledged: bool,
    #[serde(default)]
    pub name_only: bool,
}

/// Facts gathered for one reconciliation decision.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentHijackReconcileFactsWire {
    pub schema_version: u32,
    #[serde(default)]
    pub now_unix: f64,
    #[serde(default)]
    pub resolving_age_seconds: f64,
    #[serde(default = "default_min_age_seconds")]
    pub min_age_seconds: f64,
    #[serde(default)]
    pub resolver: AgentHijackResolverObservationWire,
    #[serde(default)]
    pub successor: AgentHijackSuccessorObservationWire,
}

/// Reconciliation decision. `retain_with_error` keeps custody.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentHijackReconcileDecisionWire {
    pub schema_version: u32,
    #[serde(default)]
    pub outcome: String,
    #[serde(default)]
    pub code: Option<String>,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default)]
    pub successor_name: Option<String>,
    #[serde(default)]
    pub launch_identity: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResolverLiveness {
    Live,
    Dead,
    Unknown,
}

/// Decide how to repair a resolving hijack record.
pub fn hijack_recovery_reconcile(
    record: &AgentHijackRecordWire,
    facts: &AgentHijackReconcileFactsWire,
) -> Result<AgentHijackReconcileDecisionWire, AgentHijackReconcileError> {
    if facts.schema_version != AGENT_HIJACK_RECONCILE_WIRE_SCHEMA_VERSION {
        return Err(AgentHijackReconcileError::UnsupportedSchema(
            facts.schema_version,
        ));
    }
    if record.schema_version != AGENT_HIJACK_WIRE_SCHEMA_VERSION {
        return Err(AgentHijackReconcileError::UnsupportedSchema(
            record.schema_version,
        ));
    }
    if record.state != HIJACK_RESOLVING {
        return Ok(retain(
            "not_resolving",
            "reconciliation only applies to resolving hijack records",
            record,
        ));
    }

    match resolver_liveness(record, &facts.resolver) {
        ResolverLiveness::Live => {
            return Ok(keep(
                "live_resolver",
                "the winning resolver is still live; leave the reservation in place",
                record,
            ));
        }
        ResolverLiveness::Unknown => {
            return Ok(retain(
                "incomplete",
                "resolver identity is missing or unverifiable; keeping custody to avoid a duplicate launch",
                record,
            ));
        }
        ResolverLiveness::Dead => {}
    }

    let min_age = if facts.min_age_seconds > 0.0 {
        facts.min_age_seconds
    } else {
        default_min_age_seconds()
    };
    if facts.resolving_age_seconds < min_age {
        return Ok(keep(
            "too_young",
            "the resolver is younger than the stale threshold; wait before repairing",
            record,
        ));
    }

    let successor = &facts.successor;
    // A proven-not-dispatched observation carries no successor evidence to
    // mistake for a name; it must reach the return-to-held branch below.
    if successor.name_only && successor.kind != HIJACK_SUCCESSOR_NOT_DISPATCHED
    {
        return Ok(retain(
            "uncertain",
            "successor evidence is a name without launch or process identity; keeping custody",
            record,
        ));
    }
    match successor.kind.as_str() {
        HIJACK_SUCCESSOR_QUEUED => {
            if successor_queued_matches(record, successor) {
                Ok(keep(
                    "queued",
                    "a matching queued admission request remains authoritative; do not resubmit",
                    record,
                ))
            } else {
                Ok(retain(
                    "uncertain",
                    "queued successor evidence does not match the reserved admission identity",
                    record,
                ))
            }
        }
        HIJACK_SUCCESSOR_PREPARED
        | HIJACK_SUCCESSOR_LIVE
        | HIJACK_SUCCESSOR_COMPLETED => {
            if successor_proven(record, successor) {
                Ok(adopt(record, successor))
            } else {
                Ok(retain(
                    "uncertain",
                    "observed successor does not match the reserved launch identity, workspace, or process token",
                    record,
                ))
            }
        }
        HIJACK_SUCCESSOR_MISMATCHED => Ok(retain(
            "mismatched",
            "observed successor identity does not match the reservation; keeping custody",
            record,
        )),
        HIJACK_SUCCESSOR_UNCERTAIN => Ok(retain(
            "uncertain",
            "successor observation is uncertain; keeping custody rather than launching again",
            record,
        )),
        HIJACK_SUCCESSOR_ABSENT | "" => {
            if has_recorded_launch_evidence(record) {
                Ok(retain(
                    "uncertain",
                    "launch evidence was recorded but no matching successor was observed; keeping custody",
                    record,
                ))
            } else {
                Ok(return_to_held(record))
            }
        }
        HIJACK_SUCCESSOR_NOT_DISPATCHED => Ok(return_to_held_with_code(
            "not_dispatched",
            "the admission journal proves the reserved request was never dispatched; return to held",
            record,
        )),
        other => Ok(retain(
            "uncertain",
            &format!(
                "unknown successor observation {other}; keeping custody"
            ),
            record,
        )),
    }
}

fn resolver_liveness(
    record: &AgentHijackRecordWire,
    observed: &AgentHijackResolverObservationWire,
) -> ResolverLiveness {
    let Some(pid) = record.resolver_pid.filter(|pid| *pid > 0) else {
        return ResolverLiveness::Unknown;
    };
    let recorded_token =
        nonempty(record.resolver_process_identity_token.as_deref());
    if !observed.pid_live {
        return ResolverLiveness::Dead;
    }
    if observed
        .observed_pid
        .is_some_and(|observed_pid| observed_pid != pid)
    {
        return ResolverLiveness::Dead;
    }
    match recorded_token {
        None => ResolverLiveness::Unknown,
        Some(_) if observed.identity_matches => ResolverLiveness::Live,
        Some(_) => ResolverLiveness::Dead,
    }
}

fn successor_queued_matches(
    record: &AgentHijackRecordWire,
    observed: &AgentHijackSuccessorObservationWire,
) -> bool {
    if observed.name_only {
        return false;
    }
    match nonempty(record.admission_request_id.as_deref()) {
        Some(recorded) => {
            nonempty(observed.admission_request_id.as_deref()) == Some(recorded)
        }
        None => false,
    }
}

fn successor_proven(
    record: &AgentHijackRecordWire,
    observed: &AgentHijackSuccessorObservationWire,
) -> bool {
    if observed.name_only {
        return false;
    }
    if let Some(recorded) = nonempty(record.launch_identity.as_deref()) {
        return observed.launch_identity_matches
            && nonempty(observed.launch_identity.as_deref()) == Some(recorded);
    }
    if let Some(recorded) = nonempty(record.admission_request_id.as_deref()) {
        if nonempty(observed.admission_request_id.as_deref()) == Some(recorded)
        {
            return observed.workspace_matches || observed.lineage_matches;
        }
    }
    false
}

fn has_recorded_launch_evidence(record: &AgentHijackRecordWire) -> bool {
    nonempty(record.launch_identity.as_deref()).is_some()
        || nonempty(record.admission_request_id.as_deref()).is_some()
        || nonempty(record.admission_identity.as_deref()).is_some()
}

fn nonempty(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

fn keep(
    code: &str,
    message: &str,
    record: &AgentHijackRecordWire,
) -> AgentHijackReconcileDecisionWire {
    decision(
        HIJACK_RECONCILE_KEEP_RESOLVING,
        code,
        message,
        record.reserved_successor_name.clone(),
        record.launch_identity.clone(),
    )
}

fn adopt(
    record: &AgentHijackRecordWire,
    observed: &AgentHijackSuccessorObservationWire,
) -> AgentHijackReconcileDecisionWire {
    decision(
        HIJACK_RECONCILE_ADOPT,
        "adopt",
        "a matching successor already exists; adopt it and repair settlement",
        observed
            .name
            .clone()
            .or_else(|| record.reserved_successor_name.clone()),
        observed
            .launch_identity
            .clone()
            .or_else(|| record.launch_identity.clone()),
    )
}

fn return_to_held(
    record: &AgentHijackRecordWire,
) -> AgentHijackReconcileDecisionWire {
    return_to_held_with_code(
        "absent",
        "the resolver is dead and no queued or executing successor exists; return to held",
        record,
    )
}

fn return_to_held_with_code(
    code: &str,
    message: &str,
    record: &AgentHijackRecordWire,
) -> AgentHijackReconcileDecisionWire {
    decision(
        HIJACK_RECONCILE_RETURN_TO_HELD,
        code,
        message,
        record.reserved_successor_name.clone(),
        None,
    )
}

fn retain(
    code: &str,
    message: &str,
    record: &AgentHijackRecordWire,
) -> AgentHijackReconcileDecisionWire {
    decision(
        HIJACK_RECONCILE_RETAIN_WITH_ERROR,
        code,
        message,
        record.reserved_successor_name.clone(),
        record.launch_identity.clone(),
    )
}

fn decision(
    outcome: &str,
    code: &str,
    message: &str,
    successor_name: Option<String>,
    launch_identity: Option<String>,
) -> AgentHijackReconcileDecisionWire {
    AgentHijackReconcileDecisionWire {
        schema_version: AGENT_HIJACK_RECONCILE_WIRE_SCHEMA_VERSION,
        outcome: outcome.to_owned(),
        code: Some(code.to_owned()),
        message: Some(message.to_owned()),
        successor_name,
        launch_identity,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resolving() -> AgentHijackRecordWire {
        AgentHijackRecordWire {
            schema_version: AGENT_HIJACK_WIRE_SCHEMA_VERSION,
            hijack_id: "h-1".to_owned(),
            agent_name: "agent.1".to_owned(),
            state: HIJACK_RESOLVING.to_owned(),
            revision: 4,
            resolution_action: Some(HIJACK_ACTION_ESCALATE.to_owned()),
            reserved_successor_name: Some("agent.2".to_owned()),
            concrete_target_model: Some("claude/opus@xhigh".to_owned()),
            model_changed: Some(true),
            resolver_pid: Some(99),
            resolver_process_identity_token: Some("boot:12345".to_owned()),
            ..Default::default()
        }
    }

    fn facts() -> AgentHijackReconcileFactsWire {
        AgentHijackReconcileFactsWire {
            schema_version: AGENT_HIJACK_RECONCILE_WIRE_SCHEMA_VERSION,
            now_unix: 1_000.0,
            resolving_age_seconds: 90.0,
            min_age_seconds: 60.0,
            resolver: AgentHijackResolverObservationWire {
                pid_live: false,
                identity_matches: false,
                observed_pid: None,
                observed_identity_token: None,
            },
            successor: AgentHijackSuccessorObservationWire {
                kind: HIJACK_SUCCESSOR_ABSENT.to_owned(),
                ..Default::default()
            },
        }
    }

    #[test]
    fn live_resolver_keeps_resolving() {
        let mut observed = facts();
        observed.resolver.pid_live = true;
        observed.resolver.identity_matches = true;
        observed.resolver.observed_pid = Some(99);
        observed.resolver.observed_identity_token =
            Some("boot:12345".to_owned());
        let decision =
            hijack_recovery_reconcile(&resolving(), &observed).unwrap();
        assert_eq!(decision.outcome, HIJACK_RECONCILE_KEEP_RESOLVING);
        assert_eq!(decision.code.as_deref(), Some("live_resolver"));
    }

    #[test]
    fn reused_resolver_pid_is_dead() {
        let mut observed = facts();
        observed.resolver.pid_live = true;
        observed.resolver.identity_matches = false;
        observed.resolver.observed_pid = Some(99);
        observed.resolver.observed_identity_token =
            Some("boot:99999".to_owned());
        let decision =
            hijack_recovery_reconcile(&resolving(), &observed).unwrap();
        assert_eq!(decision.outcome, HIJACK_RECONCILE_RETURN_TO_HELD);
        assert_eq!(decision.code.as_deref(), Some("absent"));
    }

    #[test]
    fn too_young_dead_resolver_waits() {
        let mut observed = facts();
        observed.resolving_age_seconds = 12.0;
        let decision =
            hijack_recovery_reconcile(&resolving(), &observed).unwrap();
        assert_eq!(decision.outcome, HIJACK_RECONCILE_KEEP_RESOLVING);
        assert_eq!(decision.code.as_deref(), Some("too_young"));
    }

    #[test]
    fn matching_queued_request_stays_authoritative() {
        let mut record = resolving();
        record.admission_request_id = Some("req-1".to_owned());
        record.admission_identity = Some("agent.2".to_owned());
        record.launch_stage = Some(HIJACK_LAUNCH_QUEUED.to_owned());
        let mut observed = facts();
        observed.successor = AgentHijackSuccessorObservationWire {
            kind: HIJACK_SUCCESSOR_QUEUED.to_owned(),
            admission_request_id: Some("req-1".to_owned()),
            admission_identity: Some("agent.2".to_owned()),
            name: Some("agent.2".to_owned()),
            ..Default::default()
        };
        let decision = hijack_recovery_reconcile(&record, &observed).unwrap();
        assert_eq!(decision.outcome, HIJACK_RECONCILE_KEEP_RESOLVING);
        assert_eq!(decision.code.as_deref(), Some("queued"));
    }

    #[test]
    fn absent_without_launch_evidence_returns_to_held() {
        let decision =
            hijack_recovery_reconcile(&resolving(), &facts()).unwrap();
        assert_eq!(decision.outcome, HIJACK_RECONCILE_RETURN_TO_HELD);
        assert_eq!(decision.code.as_deref(), Some("absent"));
    }

    #[test]
    fn confirmed_matching_launch_identity_is_adopted() {
        let mut record = resolving();
        record.launch_identity = Some("launch-abc".to_owned());
        record.launch_stage = Some(HIJACK_LAUNCH_CONFIRMED.to_owned());
        let mut observed = facts();
        observed.successor = AgentHijackSuccessorObservationWire {
            kind: HIJACK_SUCCESSOR_LIVE.to_owned(),
            launch_identity: Some("launch-abc".to_owned()),
            launch_identity_matches: true,
            workspace_matches: true,
            lineage_matches: true,
            name: Some("agent.2".to_owned()),
            ..Default::default()
        };
        let decision = hijack_recovery_reconcile(&record, &observed).unwrap();
        assert_eq!(decision.outcome, HIJACK_RECONCILE_ADOPT);
        assert_eq!(decision.launch_identity.as_deref(), Some("launch-abc"));
    }

    #[test]
    fn name_only_and_mismatched_observations_retain_custody() {
        let mut observed = facts();
        observed.successor.name_only = true;
        observed.successor.kind = HIJACK_SUCCESSOR_LIVE.to_owned();
        observed.successor.name = Some("agent.2".to_owned());
        let decision =
            hijack_recovery_reconcile(&resolving(), &observed).unwrap();
        assert_eq!(decision.outcome, HIJACK_RECONCILE_RETAIN_WITH_ERROR);
        assert_eq!(decision.code.as_deref(), Some("uncertain"));

        let mut record = resolving();
        record.launch_identity = Some("launch-abc".to_owned());
        let mut observed = facts();
        observed.successor = AgentHijackSuccessorObservationWire {
            kind: HIJACK_SUCCESSOR_MISMATCHED.to_owned(),
            launch_identity: Some("launch-other".to_owned()),
            name: Some("agent.9".to_owned()),
            ..Default::default()
        };
        let decision = hijack_recovery_reconcile(&record, &observed).unwrap();
        assert_eq!(decision.outcome, HIJACK_RECONCILE_RETAIN_WITH_ERROR);
        assert_eq!(decision.code.as_deref(), Some("mismatched"));
    }

    #[test]
    fn missing_resolver_identity_is_incomplete() {
        let mut record = resolving();
        record.resolver_pid = None;
        record.resolver_process_identity_token = None;
        let decision = hijack_recovery_reconcile(&record, &facts()).unwrap();
        assert_eq!(decision.outcome, HIJACK_RECONCILE_RETAIN_WITH_ERROR);
        assert_eq!(decision.code.as_deref(), Some("incomplete"));
    }

    #[test]
    fn recorded_launch_without_observation_is_uncertain() {
        let mut record = resolving();
        record.launch_identity = Some("launch-abc".to_owned());
        let decision = hijack_recovery_reconcile(&record, &facts()).unwrap();
        assert_eq!(decision.outcome, HIJACK_RECONCILE_RETAIN_WITH_ERROR);
        assert_eq!(decision.code.as_deref(), Some("uncertain"));
    }

    #[test]
    fn not_dispatched_with_launch_evidence_returns_to_held() {
        let mut record = resolving();
        record.launch_identity = Some("launch-abc".to_owned());
        record.admission_request_id = Some("req-1".to_owned());
        let mut observed = facts();
        observed.successor = AgentHijackSuccessorObservationWire {
            kind: HIJACK_SUCCESSOR_NOT_DISPATCHED.to_owned(),
            name: Some("agent.2".to_owned()),
            ..Default::default()
        };
        let decision = hijack_recovery_reconcile(&record, &observed).unwrap();
        assert_eq!(decision.outcome, HIJACK_RECONCILE_RETURN_TO_HELD);
        assert_eq!(decision.code.as_deref(), Some("not_dispatched"));
    }

    #[test]
    fn not_dispatched_young_resolver_still_waits() {
        let mut record = resolving();
        record.launch_identity = Some("launch-abc".to_owned());
        let mut observed = facts();
        observed.resolving_age_seconds = 5.0;
        observed.successor = AgentHijackSuccessorObservationWire {
            kind: HIJACK_SUCCESSOR_NOT_DISPATCHED.to_owned(),
            name: Some("agent.2".to_owned()),
            ..Default::default()
        };
        let decision = hijack_recovery_reconcile(&record, &observed).unwrap();
        assert_eq!(decision.outcome, HIJACK_RECONCILE_KEEP_RESOLVING);
        assert_eq!(decision.code.as_deref(), Some("too_young"));
    }
}
