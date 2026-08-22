//! Durable launch-admission journal, fingerprints, and next-action planner.
//!
//! The coordinator journals per-unit phases before it waits, evaluates a
//! condition, or dispatches. Replay is pure: terminal outcomes are never
//! silently re-run, and dispatch uses a stable request fingerprint.

use super::{
    AgentUnitWire, LaunchOutcomeWire, LaunchPlanWire, LaunchUnitPayloadWire,
    LaunchUnitResultWire, WaitTargetWire,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

pub const LAUNCH_ADMISSION_JOURNAL_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LaunchUnitPhaseWire {
    Reserved,
    Waiting,
    Checking,
    Eligible,
    Dispatching,
    Launched,
    Skipped,
    ConditionError,
    LaunchError,
    Cancelled,
}

impl LaunchUnitPhaseWire {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Launched
                | Self::Skipped
                | Self::ConditionError
                | Self::LaunchError
                | Self::Cancelled
        )
    }

    pub fn outcome(self) -> Option<LaunchOutcomeWire> {
        match self {
            Self::Eligible => Some(LaunchOutcomeWire::Eligible),
            Self::Launched => Some(LaunchOutcomeWire::Launched),
            Self::Skipped => Some(LaunchOutcomeWire::Skipped),
            Self::ConditionError => Some(LaunchOutcomeWire::ConditionError),
            Self::LaunchError | Self::Cancelled => {
                Some(LaunchOutcomeWire::LaunchError)
            }
            Self::Reserved
            | Self::Waiting
            | Self::Checking
            | Self::Dispatching => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WaitedOutcomeWire {
    pub target: WaitTargetWire,
    pub outcome: LaunchOutcomeWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LaunchAdmissionJournalEntryWire {
    pub schema_version: u32,
    pub seq: u64,
    pub logical_id: String,
    pub phase: LaunchUnitPhaseWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fingerprint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub waited_outcomes: Option<Vec<WaitedOutcomeWire>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    pub recorded_at_unix: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LaunchAdmissionUnitStateWire {
    pub logical_id: String,
    pub phase: LaunchUnitPhaseWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fingerprint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity: Option<String>,
    #[serde(default)]
    pub waited_outcomes: Vec<WaitedOutcomeWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    pub last_seq: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LaunchAdmissionWaitFactWire {
    pub target: WaitTargetWire,
    pub resolved: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome: Option<LaunchOutcomeWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LaunchAdmissionSummaryWire {
    pub total: u32,
    pub eligible: u32,
    pub launched: u32,
    pub skipped: u32,
    pub condition_errors: u32,
    pub launch_errors: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum LaunchAdmissionActionWire {
    Reserve {
        logical_id: String,
    },
    Wait {
        logical_id: String,
    },
    Check {
        logical_id: String,
        waited_outcomes: Vec<WaitedOutcomeWire>,
    },
    Eligible {
        logical_id: String,
        waited_outcomes: Vec<WaitedOutcomeWire>,
    },
    Dispatch {
        logical_id: String,
        fingerprint: String,
        unit_kind: String,
    },
    FailCheck {
        logical_id: String,
        message: String,
    },
    FailDispatch {
        logical_id: String,
        message: String,
    },
    RecordLaunched {
        logical_id: String,
        identity: String,
    },
}

pub fn wait_target_key(target: &WaitTargetWire) -> String {
    match target {
        WaitTargetWire::Logical { logical_id, .. } => {
            format!("logical:{logical_id}")
        }
        WaitTargetWire::Agent { name } => format!("agent:{name}"),
        WaitTargetWire::Proc { identifier } => format!("proc:{identifier}"),
        WaitTargetWire::Bead { bead_id } => format!("bead:{bead_id}"),
        WaitTargetWire::Time { value } => format!("time:{value}"),
    }
}

pub fn dispatch_fingerprint(
    plan_digest: &str,
    logical_id: &str,
    payload: &LaunchUnitPayloadWire,
) -> String {
    let value = serde_json::json!({
        "plan_digest": plan_digest,
        "logical_id": logical_id,
        "payload": payload,
    });
    hex::encode(Sha256::digest(value.to_string().as_bytes()))
}

pub fn reconcile_admission_journal(
    entries: &[LaunchAdmissionJournalEntryWire],
) -> BTreeMap<String, LaunchAdmissionUnitStateWire> {
    let mut states = BTreeMap::new();
    for entry in entries {
        let state =
            states.entry(entry.logical_id.clone()).or_insert_with(|| {
                LaunchAdmissionUnitStateWire {
                    logical_id: entry.logical_id.clone(),
                    phase: entry.phase,
                    fingerprint: None,
                    identity: None,
                    waited_outcomes: Vec::new(),
                    message: None,
                    last_seq: entry.seq,
                }
            });
        state.phase = entry.phase;
        state.last_seq = entry.seq;
        if let Some(fingerprint) = &entry.fingerprint {
            state.fingerprint = Some(fingerprint.clone());
        }
        if let Some(identity) = &entry.identity {
            state.identity = Some(identity.clone());
        }
        if let Some(waited) = &entry.waited_outcomes {
            state.waited_outcomes = waited.clone();
        }
        if let Some(message) = &entry.message {
            state.message = Some(message.clone());
        }
    }
    states
}

pub fn next_admission_actions(
    plan: &LaunchPlanWire,
    states: &BTreeMap<String, LaunchAdmissionUnitStateWire>,
    wait_facts: &[LaunchAdmissionWaitFactWire],
) -> Vec<LaunchAdmissionActionWire> {
    let facts = facts_by_key(wait_facts);
    let mut actions = Vec::new();
    for unit in &plan.units {
        match states.get(&unit.logical_id) {
            None => actions.push(LaunchAdmissionActionWire::Reserve {
                logical_id: unit.logical_id.clone(),
            }),
            Some(state) => match state.phase {
                LaunchUnitPhaseWire::Reserved => {
                    actions.push(LaunchAdmissionActionWire::Wait {
                        logical_id: unit.logical_id.clone(),
                    });
                }
                LaunchUnitPhaseWire::Waiting => {
                    if let Some(waited) =
                        resolved_wait_outcomes(unit, states, &facts)
                    {
                        if unit.condition.is_some() {
                            actions.push(LaunchAdmissionActionWire::Check {
                                logical_id: unit.logical_id.clone(),
                                waited_outcomes: waited,
                            });
                        } else {
                            actions.push(LaunchAdmissionActionWire::Eligible {
                                logical_id: unit.logical_id.clone(),
                                waited_outcomes: waited,
                            });
                        }
                    }
                }
                LaunchUnitPhaseWire::Checking => {
                    actions.push(LaunchAdmissionActionWire::FailCheck {
                        logical_id: unit.logical_id.clone(),
                        message: "check_interrupted".to_string(),
                    });
                }
                LaunchUnitPhaseWire::Eligible => {
                    actions.push(dispatch_action(plan, unit));
                }
                LaunchUnitPhaseWire::Dispatching => {
                    if let Some(identity) = &state.identity {
                        actions.push(
                            LaunchAdmissionActionWire::RecordLaunched {
                                logical_id: unit.logical_id.clone(),
                                identity: identity.clone(),
                            },
                        );
                    } else {
                        actions.push(LaunchAdmissionActionWire::FailDispatch {
                            logical_id: unit.logical_id.clone(),
                            message: "dispatch_interrupted".to_string(),
                        });
                    }
                }
                LaunchUnitPhaseWire::Launched
                | LaunchUnitPhaseWire::Skipped
                | LaunchUnitPhaseWire::ConditionError
                | LaunchUnitPhaseWire::LaunchError
                | LaunchUnitPhaseWire::Cancelled => {}
            },
        }
    }
    actions
}

pub fn summarize_admission(
    plan: &LaunchPlanWire,
    states: &BTreeMap<String, LaunchAdmissionUnitStateWire>,
) -> LaunchAdmissionSummaryWire {
    let mut summary = LaunchAdmissionSummaryWire {
        total: plan.units.len() as u32,
        eligible: 0,
        launched: 0,
        skipped: 0,
        condition_errors: 0,
        launch_errors: 0,
    };
    for unit in &plan.units {
        let Some(state) = states.get(&unit.logical_id) else {
            continue;
        };
        match state.phase {
            LaunchUnitPhaseWire::Eligible
            | LaunchUnitPhaseWire::Dispatching => {
                summary.eligible += 1;
            }
            LaunchUnitPhaseWire::Launched => {
                summary.eligible += 1;
                summary.launched += 1;
            }
            LaunchUnitPhaseWire::Skipped => summary.skipped += 1,
            LaunchUnitPhaseWire::ConditionError => {
                summary.condition_errors += 1
            }
            LaunchUnitPhaseWire::LaunchError
            | LaunchUnitPhaseWire::Cancelled => summary.launch_errors += 1,
            LaunchUnitPhaseWire::Reserved
            | LaunchUnitPhaseWire::Waiting
            | LaunchUnitPhaseWire::Checking => {}
        }
    }
    summary
}

pub fn admission_unit_results(
    plan: &LaunchPlanWire,
    states: &BTreeMap<String, LaunchAdmissionUnitStateWire>,
) -> Vec<LaunchUnitResultWire> {
    plan.units
        .iter()
        .filter_map(|unit| {
            let state = states.get(&unit.logical_id)?;
            let outcome = state.phase.outcome()?;
            Some(LaunchUnitResultWire {
                logical_id: unit.logical_id.clone(),
                outcome,
                message: state.message.clone(),
            })
        })
        .collect()
}

pub fn agent_unit_dispatch_prompt(agent: &AgentUnitWire) -> String {
    let mut lines = Vec::new();
    if agent.identity_explicit {
        if let Some(identity) = &agent.identity {
            if let Some(bead_id) = &agent.bead_id {
                lines.push(format!("%id({identity}, bead={bead_id})"));
            } else {
                lines.push(format!("%id:{identity}"));
            }
        }
    } else if let Some(bead_id) = &agent.bead_id {
        lines.push(format!("%id(bead={bead_id})"));
    }
    match (&agent.model, &agent.reasoning_effort) {
        (Some(model), Some(effort)) => {
            lines.push(format!("%model:{model}@{effort}"));
        }
        (Some(model), None) => lines.push(format!("%model:{model}")),
        (None, Some(effort)) => lines.push(format!("%effort:{effort}")),
        (None, None) => {}
    }
    if agent.auto_enabled {
        match &agent.auto_mode {
            Some(mode) if mode != "plan" => {
                lines.push(format!("%auto:{mode}"));
            }
            _ => lines.push("%auto".to_string()),
        }
    }
    if !agent.finalizers.is_empty() {
        lines.push(format!("%final:{}", agent.finalizers.join(",")));
    }
    if agent.hidden {
        lines.push("%hide".to_string());
    }
    if let Some(runners) = agent.wait_runners {
        lines.push(format!("%wait(runners={runners})"));
    }
    if let Some(priority) = agent.wait_priority {
        lines.push(format!("%wait(priority={priority})"));
    }
    if !agent.prompt.is_empty() {
        lines.push(agent.prompt.clone());
    }
    lines.join("\n")
}

fn dispatch_action(
    plan: &LaunchPlanWire,
    unit: &super::LaunchUnitWire,
) -> LaunchAdmissionActionWire {
    let unit_kind = match unit.payload {
        LaunchUnitPayloadWire::Agent(_) => "agent",
        LaunchUnitPayloadWire::Proc(_) => "proc",
    };
    LaunchAdmissionActionWire::Dispatch {
        logical_id: unit.logical_id.clone(),
        fingerprint: dispatch_fingerprint(
            &plan.content_digest,
            &unit.logical_id,
            &unit.payload,
        ),
        unit_kind: unit_kind.to_string(),
    }
}

fn facts_by_key(
    wait_facts: &[LaunchAdmissionWaitFactWire],
) -> BTreeMap<String, LaunchAdmissionWaitFactWire> {
    let mut facts = BTreeMap::new();
    for fact in wait_facts {
        facts.insert(wait_target_key(&fact.target), fact.clone());
    }
    facts
}

fn resolved_wait_outcomes(
    unit: &super::LaunchUnitWire,
    states: &BTreeMap<String, LaunchAdmissionUnitStateWire>,
    facts: &BTreeMap<String, LaunchAdmissionWaitFactWire>,
) -> Option<Vec<WaitedOutcomeWire>> {
    let mut waited = Vec::new();
    for target in &unit.waits {
        waited.push(resolved_wait_outcome(target, states, facts)?);
    }
    Some(waited)
}

fn resolved_wait_outcome(
    target: &WaitTargetWire,
    states: &BTreeMap<String, LaunchAdmissionUnitStateWire>,
    facts: &BTreeMap<String, LaunchAdmissionWaitFactWire>,
) -> Option<WaitedOutcomeWire> {
    if let WaitTargetWire::Logical { logical_id, .. } = target {
        let state = states.get(logical_id)?;
        let outcome = state.phase.outcome()?;
        return Some(WaitedOutcomeWire {
            target: target.clone(),
            outcome,
            identity: state.identity.clone(),
            message: state.message.clone(),
        });
    }
    let fact = facts.get(&wait_target_key(target))?;
    if !fact.resolved {
        return None;
    }
    Some(WaitedOutcomeWire {
        target: target.clone(),
        outcome: fact.outcome.unwrap_or(LaunchOutcomeWire::Launched),
        identity: fact.identity.clone(),
        message: fact.message.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent_launch::{
        AgentUnitWire, LaunchConditionWire, LaunchUnitWire, ProcUnitWire,
        LAUNCH_PLAN_WIRE_SCHEMA_VERSION,
    };
    use crate::fenced_code::CodeValueWire;

    fn agent_unit(logical_id: &str, source_order: u32) -> LaunchUnitWire {
        LaunchUnitWire {
            logical_id: logical_id.to_string(),
            source_order,
            waits: Vec::new(),
            condition: None,
            payload: LaunchUnitPayloadWire::Agent(AgentUnitWire {
                prompt: "Do work".to_string(),
                identity: Some("reviewer".to_string()),
                identity_explicit: true,
                model: Some("opus".to_string()),
                reasoning_effort: None,
                bead_id: None,
                hidden: false,
                auto_enabled: false,
                auto_mode: None,
                finalizers: Vec::new(),
                wait_runners: None,
                wait_priority: None,
            }),
        }
    }

    fn plan_with(units: Vec<LaunchUnitWire>) -> LaunchPlanWire {
        LaunchPlanWire {
            schema_version: LAUNCH_PLAN_WIRE_SCHEMA_VERSION,
            launch_kind: "multi_prompt".to_string(),
            selected_project: Some("sase".to_string()),
            units,
            approval_preview: Vec::new(),
            content_digest: "d".repeat(64),
            diagnostics: Vec::new(),
        }
    }

    fn entry(
        seq: u64,
        logical_id: &str,
        phase: LaunchUnitPhaseWire,
    ) -> LaunchAdmissionJournalEntryWire {
        LaunchAdmissionJournalEntryWire {
            schema_version: LAUNCH_ADMISSION_JOURNAL_SCHEMA_VERSION,
            seq,
            logical_id: logical_id.to_string(),
            phase,
            fingerprint: None,
            identity: None,
            waited_outcomes: None,
            message: None,
            recorded_at_unix: seq as f64,
        }
    }

    #[test]
    fn dispatch_fingerprint_is_stable_for_same_payload() {
        let payload = LaunchUnitPayloadWire::Agent(AgentUnitWire {
            prompt: "Review".to_string(),
            identity: None,
            identity_explicit: false,
            model: None,
            reasoning_effort: None,
            bead_id: None,
            hidden: false,
            auto_enabled: false,
            auto_mode: None,
            finalizers: Vec::new(),
            wait_runners: None,
            wait_priority: None,
        });
        let first = dispatch_fingerprint("abc", "unit-1", &payload);
        let second = dispatch_fingerprint("abc", "unit-1", &payload);
        assert_eq!(first, second);
        assert_eq!(first.len(), 64);
        assert_ne!(first, dispatch_fingerprint("abc", "unit-2", &payload));
    }

    #[test]
    fn reconcile_keeps_latest_phase_and_identity() {
        let mut second = entry(2, "unit-1", LaunchUnitPhaseWire::Launched);
        second.identity = Some("reviewer".to_string());
        second.fingerprint = Some("fp".to_string());
        let states = reconcile_admission_journal(&[
            entry(1, "unit-1", LaunchUnitPhaseWire::Dispatching),
            second,
        ]);
        let state = states.get("unit-1").unwrap();
        assert_eq!(state.phase, LaunchUnitPhaseWire::Launched);
        assert_eq!(state.identity.as_deref(), Some("reviewer"));
        assert_eq!(state.fingerprint.as_deref(), Some("fp"));
    }

    #[test]
    fn next_actions_reserve_then_wait_then_dispatch_agent() {
        let plan = plan_with(vec![agent_unit("unit-1", 0)]);
        let empty = BTreeMap::new();
        let reserved = next_admission_actions(&plan, &empty, &[]);
        assert_eq!(
            reserved,
            vec![LaunchAdmissionActionWire::Reserve {
                logical_id: "unit-1".to_string()
            }]
        );

        let mut states = BTreeMap::new();
        states.insert(
            "unit-1".to_string(),
            LaunchAdmissionUnitStateWire {
                logical_id: "unit-1".to_string(),
                phase: LaunchUnitPhaseWire::Reserved,
                fingerprint: None,
                identity: None,
                waited_outcomes: Vec::new(),
                message: None,
                last_seq: 1,
            },
        );
        assert_eq!(
            next_admission_actions(&plan, &states, &[]),
            vec![LaunchAdmissionActionWire::Wait {
                logical_id: "unit-1".to_string()
            }]
        );

        states.get_mut("unit-1").unwrap().phase = LaunchUnitPhaseWire::Waiting;
        match &next_admission_actions(&plan, &states, &[])[0] {
            LaunchAdmissionActionWire::Eligible {
                logical_id,
                waited_outcomes,
            } => {
                assert_eq!(logical_id, "unit-1");
                assert!(waited_outcomes.is_empty());
            }
            other => panic!("expected eligible, got {other:?}"),
        }

        states.get_mut("unit-1").unwrap().phase = LaunchUnitPhaseWire::Eligible;
        match &next_admission_actions(&plan, &states, &[])[0] {
            LaunchAdmissionActionWire::Dispatch {
                logical_id,
                unit_kind,
                fingerprint,
            } => {
                assert_eq!(logical_id, "unit-1");
                assert_eq!(unit_kind, "agent");
                assert_eq!(fingerprint.len(), 64);
            }
            other => panic!("expected dispatch, got {other:?}"),
        }
    }

    #[test]
    fn skipped_predecessor_is_terminal_and_does_not_retarget() {
        let mut dependent = agent_unit("unit-2", 1);
        dependent.waits = vec![WaitTargetWire::Logical {
            logical_id: "unit-1".to_string(),
            source: Some("%wait".to_string()),
        }];
        let plan = plan_with(vec![agent_unit("unit-1", 0), dependent]);
        let mut states = BTreeMap::new();
        states.insert(
            "unit-1".to_string(),
            LaunchAdmissionUnitStateWire {
                logical_id: "unit-1".to_string(),
                phase: LaunchUnitPhaseWire::Skipped,
                fingerprint: None,
                identity: None,
                waited_outcomes: Vec::new(),
                message: Some("predicate exited 1".to_string()),
                last_seq: 4,
            },
        );
        states.insert(
            "unit-2".to_string(),
            LaunchAdmissionUnitStateWire {
                logical_id: "unit-2".to_string(),
                phase: LaunchUnitPhaseWire::Waiting,
                fingerprint: None,
                identity: None,
                waited_outcomes: Vec::new(),
                message: None,
                last_seq: 3,
            },
        );

        match &next_admission_actions(&plan, &states, &[])[0] {
            LaunchAdmissionActionWire::Eligible {
                logical_id,
                waited_outcomes,
            } => {
                assert_eq!(logical_id, "unit-2");
                assert_eq!(waited_outcomes.len(), 1);
                assert_eq!(
                    waited_outcomes[0].target,
                    WaitTargetWire::Logical {
                        logical_id: "unit-1".to_string(),
                        source: Some("%wait".to_string()),
                    }
                );
                assert_eq!(
                    waited_outcomes[0].outcome,
                    LaunchOutcomeWire::Skipped
                );
            }
            other => panic!("expected eligible on unit-2, got {other:?}"),
        }
    }

    #[test]
    fn condition_units_check_after_waits_and_fail_interrupted_checks() {
        let mut unit = agent_unit("unit-1", 0);
        unit.condition = Some(LaunchConditionWire {
            code: CodeValueWire {
                schema_version: 1,
                source: "true".to_string(),
                language: "bash".to_string(),
                info_string: None,
                digest: "c".repeat(64),
                preview: "true".to_string(),
            },
            cwd: None,
            context_fields: vec!["waited_outcomes".to_string()],
        });
        let plan = plan_with(vec![unit]);
        let mut states = BTreeMap::new();
        states.insert(
            "unit-1".to_string(),
            LaunchAdmissionUnitStateWire {
                logical_id: "unit-1".to_string(),
                phase: LaunchUnitPhaseWire::Waiting,
                fingerprint: None,
                identity: None,
                waited_outcomes: Vec::new(),
                message: None,
                last_seq: 2,
            },
        );
        match &next_admission_actions(&plan, &states, &[])[0] {
            LaunchAdmissionActionWire::Check { logical_id, .. } => {
                assert_eq!(logical_id, "unit-1");
            }
            other => panic!("expected check, got {other:?}"),
        }

        states.get_mut("unit-1").unwrap().phase = LaunchUnitPhaseWire::Checking;
        assert_eq!(
            next_admission_actions(&plan, &states, &[])[0],
            LaunchAdmissionActionWire::FailCheck {
                logical_id: "unit-1".to_string(),
                message: "check_interrupted".to_string(),
            }
        );
    }

    #[test]
    fn dispatching_without_identity_fails_instead_of_redoing_spawn() {
        let plan = plan_with(vec![agent_unit("unit-1", 0)]);
        let mut states = BTreeMap::new();
        states.insert(
            "unit-1".to_string(),
            LaunchAdmissionUnitStateWire {
                logical_id: "unit-1".to_string(),
                phase: LaunchUnitPhaseWire::Dispatching,
                fingerprint: Some("fp".to_string()),
                identity: None,
                waited_outcomes: Vec::new(),
                message: None,
                last_seq: 5,
            },
        );
        assert_eq!(
            next_admission_actions(&plan, &states, &[])[0],
            LaunchAdmissionActionWire::FailDispatch {
                logical_id: "unit-1".to_string(),
                message: "dispatch_interrupted".to_string(),
            }
        );
        states.get_mut("unit-1").unwrap().identity =
            Some("reviewer".to_string());
        assert_eq!(
            next_admission_actions(&plan, &states, &[])[0],
            LaunchAdmissionActionWire::RecordLaunched {
                logical_id: "unit-1".to_string(),
                identity: "reviewer".to_string(),
            }
        );
    }

    #[test]
    fn external_wait_facts_gate_admission() {
        let mut unit = agent_unit("unit-1", 0);
        unit.waits = vec![WaitTargetWire::Agent {
            name: "builder".to_string(),
        }];
        let plan = plan_with(vec![unit]);
        let mut states = BTreeMap::new();
        states.insert(
            "unit-1".to_string(),
            LaunchAdmissionUnitStateWire {
                logical_id: "unit-1".to_string(),
                phase: LaunchUnitPhaseWire::Waiting,
                fingerprint: None,
                identity: None,
                waited_outcomes: Vec::new(),
                message: None,
                last_seq: 2,
            },
        );
        assert!(next_admission_actions(&plan, &states, &[]).is_empty());
        let facts = vec![LaunchAdmissionWaitFactWire {
            target: WaitTargetWire::Agent {
                name: "builder".to_string(),
            },
            resolved: true,
            outcome: Some(LaunchOutcomeWire::Launched),
            identity: Some("builder".to_string()),
            message: None,
        }];
        match &next_admission_actions(&plan, &states, &facts)[0] {
            LaunchAdmissionActionWire::Eligible {
                waited_outcomes, ..
            } => {
                assert_eq!(
                    waited_outcomes[0].identity.as_deref(),
                    Some("builder")
                );
            }
            other => panic!("expected eligible, got {other:?}"),
        }
    }

    #[test]
    fn summary_counts_partial_success_without_collapsing_errors() {
        let plan = plan_with(vec![
            agent_unit("unit-1", 0),
            agent_unit("unit-2", 1),
            agent_unit("unit-3", 2),
            agent_unit("unit-4", 3),
        ]);
        let mut states = BTreeMap::new();
        for (id, phase) in [
            ("unit-1", LaunchUnitPhaseWire::Launched),
            ("unit-2", LaunchUnitPhaseWire::Skipped),
            ("unit-3", LaunchUnitPhaseWire::ConditionError),
            ("unit-4", LaunchUnitPhaseWire::LaunchError),
        ] {
            states.insert(
                id.to_string(),
                LaunchAdmissionUnitStateWire {
                    logical_id: id.to_string(),
                    phase,
                    fingerprint: None,
                    identity: None,
                    waited_outcomes: Vec::new(),
                    message: None,
                    last_seq: 1,
                },
            );
        }
        let summary = summarize_admission(&plan, &states);
        assert_eq!(
            summary,
            LaunchAdmissionSummaryWire {
                total: 4,
                eligible: 1,
                launched: 1,
                skipped: 1,
                condition_errors: 1,
                launch_errors: 1,
            }
        );
        assert_eq!(admission_unit_results(&plan, &states).len(), 4);
    }

    #[test]
    fn agent_dispatch_prompt_restores_identity_without_waits() {
        let prompt = agent_unit_dispatch_prompt(&AgentUnitWire {
            prompt: "Review the diff".to_string(),
            identity: Some("reviewer".to_string()),
            identity_explicit: true,
            model: Some("opus".to_string()),
            reasoning_effort: Some("high".to_string()),
            bead_id: Some("sase-1".to_string()),
            hidden: true,
            auto_enabled: true,
            auto_mode: Some("plan".to_string()),
            finalizers: vec!["commit".to_string()],
            wait_runners: Some(2),
            wait_priority: Some(1),
        });
        assert!(prompt.contains("%id(reviewer, bead=sase-1)"));
        assert!(prompt.contains("%model:opus@high"));
        assert!(prompt.contains("%auto"));
        assert!(prompt.contains("%final:commit"));
        assert!(prompt.contains("%hide"));
        assert!(prompt.contains("%wait(runners=2)"));
        assert!(prompt.contains("%wait(priority=1)"));
        assert!(prompt.contains("Review the diff"));
        assert!(!prompt.contains("%wait:"));
        assert!(!prompt.contains("%if"));
    }

    #[test]
    fn proc_payload_fingerprint_uses_code_digest() {
        let payload = LaunchUnitPayloadWire::Proc(ProcUnitWire {
            code: CodeValueWire {
                schema_version: 1,
                source: "just check".to_string(),
                language: "bash".to_string(),
                info_string: None,
                digest: "b".repeat(64),
                preview: "just check".to_string(),
            },
            shell_name: Some("check".to_string()),
            label: None,
            timeout: None,
            idle_timeout: None,
            cwd: None,
            workspace: true,
            workspace_explicit: false,
            selected_project: Some("sase".to_string()),
        });
        assert_eq!(dispatch_fingerprint("plan", "unit-1", &payload).len(), 64);
    }
}
