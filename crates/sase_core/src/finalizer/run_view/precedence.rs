//! Source-precedence rules 1–10.
//!
//! A pure function from decoded facts plus `turn_terminal`/`runner_live`
//! to the run disposition and per-instance statuses. Operation records,
//! steps, and live tails supply content only, never status (rule 6), and
//! the `agent_meta` summary is a row hint, never detail truth (rule 7), so
//! neither appears here.

use std::collections::{BTreeMap, BTreeSet};

use super::decode::{
    cap_output, decode_authority, decode_context, decode_journal, decode_meta,
    decode_plan, decode_result, decode_submission, decode_submission_timeline,
    map_journal_instance_status, map_terminal_instance_status, text_facts,
    AuthorityFacts, ContextFacts, ContextRequirementFacts, JournalFacts,
    JournalSegmentFacts, MetaFacts, PlanFacts, ResultFacts, SubmissionFacts,
    TextFacts, PATH_AGENT_META, PATH_AUTHORITY_PLAN, PATH_CONTEXT,
    PATH_JOURNAL, PATH_PLAN, PATH_RESULT, PATH_SUBMISSION,
    PATH_SUBMISSION_ATTEMPTS,
};
use super::wire::{
    RunViewDispositionWire, RunViewRunInputWire, RunViewTextInputWire,
};

/// Everything the rules read for one run.
pub(crate) struct RunEvidence {
    pub too_large: Option<&'static str>,
    pub plan: Result<PlanFacts, String>,
    pub authority: AuthorityFacts,
    pub context: ContextFacts,
    pub submission: SubmissionFacts,
    pub timeline_from_attempts: Vec<super::wire::RunViewDeclarationWire>,
    pub journal: JournalFacts,
    pub result: Option<ResultFacts>,
    pub meta: MetaFacts,
    pub turn_terminal: bool,
    pub runner_live: Option<bool>,
    pub run_id: String,
}

impl RunEvidence {
    pub(crate) fn collect(input: &RunViewRunInputWire) -> Self {
        let mut too_large: Option<&'static str> = None;
        let wires: [&RunViewTextInputWire; 8] = [
            &input.agent_meta,
            &input.plan,
            &input.authority_plan,
            &input.context,
            &input.submission,
            &input.submission_attempts,
            &input.journal,
            &input.result,
        ];
        let paths: [&'static str; 8] = [
            PATH_AGENT_META,
            PATH_PLAN,
            PATH_AUTHORITY_PLAN,
            PATH_CONTEXT,
            PATH_SUBMISSION,
            PATH_SUBMISSION_ATTEMPTS,
            PATH_JOURNAL,
            PATH_RESULT,
        ];
        let mut texts: [Option<String>; 8] = Default::default();
        for (index, wire) in wires.iter().enumerate() {
            match text_facts(wire, paths[index]) {
                TextFacts::TooLarge(raw) => {
                    if too_large.is_none() {
                        too_large = Some(raw);
                    }
                }
                TextFacts::Text(text) => texts[index] = Some(text),
                TextFacts::Empty => {}
            }
        }
        let [meta, plan_text, authority_text, context_text, submission_text, attempts_text, journal_text, result_text] =
            texts;
        let plan = match plan_text.as_deref() {
            Some(text) => decode_plan(text),
            None => Err("plan is missing".to_string()),
        };
        Self {
            too_large,
            plan,
            authority: authority_text
                .as_deref()
                .map(decode_authority)
                .unwrap_or_else(|| decode_authority("")),
            context: context_text
                .as_deref()
                .map(decode_context)
                .unwrap_or_else(|| decode_context("")),
            submission: submission_text
                .as_deref()
                .map(decode_submission)
                .unwrap_or_else(|| decode_submission("")),
            timeline_from_attempts: attempts_text
                .as_deref()
                .map(decode_submission_timeline)
                .unwrap_or_default(),
            journal: journal_text
                .as_deref()
                .map(decode_journal)
                .unwrap_or_default(),
            result: result_text.as_deref().and_then(decode_result),
            meta: meta
                .as_deref()
                .map(decode_meta)
                .unwrap_or_else(|| decode_meta("")),
            turn_terminal: input.turn_terminal,
            runner_live: input.runner_live,
            run_id: input.run_id.clone(),
        }
    }

    fn last_segment(&self) -> Option<&JournalSegmentFacts> {
        self.journal.segments.last()
    }
}

/// The decided run disposition with its reason.
pub(crate) struct DecidedRun {
    pub disposition: RunViewDispositionWire,
    pub reason: Option<String>,
    pub result_status: Option<String>,
}

fn unavailable(reason: String) -> DecidedRun {
    DecidedRun {
        disposition: RunViewDispositionWire::Unavailable,
        reason: Some(reason),
        result_status: None,
    }
}

/// Encode rules 1–10 for one run's disposition.
pub(crate) fn decide_run(evidence: &RunEvidence) -> DecidedRun {
    // Ceilings: an over-4 MiB input is never parsed and yields an
    // `unavailable` sub-state naming the raw path.
    if let Some(path) = evidence.too_large {
        return unavailable(format!("too large: {path}"));
    }
    // Rule 1, strict leg: the plan parses strictly, or the run is
    // `unavailable`. A missing plan is the same state with its own reason.
    let plan = match &evidence.plan {
        Ok(plan) => plan,
        Err(reason) => return unavailable(reason.clone()),
    };
    // Rule 1, digest and scope legs.
    if let Some(authority_canonical) = &evidence.authority.plan_canonical {
        if authority_canonical != &plan.canonical {
            return unavailable(
                "plan drifted from host-owned authority".to_string(),
            );
        }
    }
    if let Some(context_digest) = &evidence.context.plan_digest {
        if context_digest != &plan.plan.plan_digest {
            return unavailable("context plan_digest mismatch".to_string());
        }
    }
    if let Some(submission_digest) = &evidence.submission.plan_digest {
        if submission_digest != &plan.plan.plan_digest {
            return unavailable("submission plan_digest mismatch".to_string());
        }
    }
    let selected: BTreeSet<&str> = plan
        .plan
        .entries
        .iter()
        .map(|entry| entry.instance_id.as_str())
        .collect();
    for requirement in &evidence.context.requirements {
        if !selected.contains(requirement.instance_id.as_str()) {
            return unavailable(format!(
                "context covers unselected instance '{}'",
                requirement.instance_id
            ));
        }
    }
    // Rule 2: a valid terminal result with matching identity is
    // authoritative for outcomes. The result file is collected per run,
    // so presence is the identity match.
    if let Some(result) = &evidence.result {
        return DecidedRun {
            disposition: RunViewDispositionWire::Ran,
            reason: None,
            result_status: Some(result.status.clone()),
        };
    }
    let segment = evidence.last_segment();
    // Rule 3: a journal `phase_skipped` means the shell sealed a plan that
    // was correctly skipped for a pending handoff.
    if let Some(segment) = segment {
        if let Some(reason) = &segment.skipped_reason {
            return DecidedRun {
                disposition: RunViewDispositionWire::Skipped,
                reason: Some(format!("skipped · {reason}")),
                result_status: None,
            };
        }
    }
    match segment {
        Some(segment)
            if segment.phase_started_run_id.is_some()
                || !segment.cycles.is_empty()
                || segment.finished_status.is_some()
                || !segment.declaration_statuses.is_empty() =>
        {
            decide_open_or_closed(evidence, segment)
        }
        _ => {
            // Rules 9 and planned: no journal and no result.
            if evidence.turn_terminal {
                DecidedRun {
                    disposition: RunViewDispositionWire::NotReached,
                    reason: Some(
                        "plan sealed but finalizers never ran".to_string(),
                    ),
                    result_status: None,
                }
            } else {
                DecidedRun {
                    disposition: RunViewDispositionWire::Active,
                    reason: Some("planned".to_string()),
                    result_status: None,
                }
            }
        }
    }
}

fn decide_open_or_closed(
    evidence: &RunEvidence,
    segment: &JournalSegmentFacts,
) -> DecidedRun {
    let open = segment.finished_status.is_none();
    if !open {
        // A closed segment without a result file is anomalous: the
        // controller journals `phase_finished` only after the aggregate
        // result write, so the result was lost. On a live turn the run
        // still counts as ran with the journal's status.
        let status = segment.finished_status.clone();
        if evidence.turn_terminal {
            return DecidedRun {
                disposition: RunViewDispositionWire::Interrupted,
                reason: Some("result missing".to_string()),
                result_status: status,
            };
        }
        return DecidedRun {
            disposition: RunViewDispositionWire::Ran,
            reason: None,
            result_status: status,
        };
    }
    // The segment is open.
    if segment.truncated {
        return DecidedRun {
            disposition: RunViewDispositionWire::Interrupted,
            reason: Some("observability truncated".to_string()),
            result_status: None,
        };
    }
    // Rule 4: the journal plus a live runner is authoritative for the
    // active phase, instance, attempt, and op.
    match evidence.runner_live {
        Some(true) => DecidedRun {
            disposition: RunViewDispositionWire::Active,
            reason: active_reason(segment),
            result_status: None,
        },
        // Rule 8: an open journal with a dead runner — or a journal whose
        // `phase_started` run id names a different run — is interrupted.
        Some(false) => DecidedRun {
            disposition: RunViewDispositionWire::Interrupted,
            reason: Some("runner dead".to_string()),
            result_status: None,
        },
        None => {
            if runner_mismatched(evidence, segment) {
                return DecidedRun {
                    disposition: RunViewDispositionWire::Interrupted,
                    reason: Some("runner mismatch".to_string()),
                    result_status: None,
                };
            }
            // Rule 8, second leg: an open journal on a terminal turn.
            if evidence.turn_terminal {
                DecidedRun {
                    disposition: RunViewDispositionWire::Interrupted,
                    reason: Some("journal open on terminal turn".to_string()),
                    result_status: None,
                }
            } else {
                DecidedRun {
                    disposition: RunViewDispositionWire::Active,
                    reason: active_reason(segment),
                    result_status: None,
                }
            }
        }
    }
}

fn runner_mismatched(
    evidence: &RunEvidence,
    segment: &JournalSegmentFacts,
) -> bool {
    match &segment.phase_started_run_id {
        Some(journal_run_id) => *journal_run_id != evidence.run_id,
        None => false,
    }
}

fn active_reason(segment: &JournalSegmentFacts) -> Option<String> {
    if let Some(instance_id) = &segment.active_instance_id {
        if let Some(op) = &segment.active_op {
            return Some(cap_output(&format!("{instance_id} · {op}")));
        }
        return Some(cap_output(instance_id));
    }
    if !segment.declaration_statuses.is_empty()
        && segment.finished_status.is_none()
    {
        return Some("declaration".to_string());
    }
    Some("executing".to_string())
}

/// Per-instance journal position inside the last segment.
pub(crate) struct JournalInstancePosition {
    pub finished_status: Option<String>,
    pub active: bool,
    pub attempt: Option<u32>,
    pub max_attempts: Option<u32>,
    pub op: Option<String>,
}

pub(crate) fn journal_positions(
    evidence: &RunEvidence,
) -> BTreeMap<String, JournalInstancePosition> {
    let mut positions = BTreeMap::new();
    let Some(segment) = evidence.last_segment() else {
        return positions;
    };
    for (instance_id, status) in &segment.finished_instances {
        positions.insert(
            instance_id.clone(),
            JournalInstancePosition {
                finished_status: Some(status.clone()),
                active: false,
                attempt: None,
                max_attempts: None,
                op: None,
            },
        );
    }
    if let Some(active_id) = &segment.active_instance_id {
        positions.insert(
            active_id.clone(),
            JournalInstancePosition {
                finished_status: None,
                active: true,
                attempt: segment.active_attempt,
                max_attempts: segment.active_max_attempts,
                op: segment.active_op.clone(),
            },
        );
    }
    positions
}

/// Decide every selected instance's status for a run.
///
/// Sources, in order: the terminal result (rule 2), the journal's active
/// and finished positions (rules 4 and 8), context triggers (rule 5), and
/// the planned default. Rule 10 then marks planned instances downstream
/// of a terminal failed or refused upstream as `not run · blocked`.
pub(crate) fn decide_instance_statuses(
    evidence: &RunEvidence,
    decided: &DecidedRun,
    order: &[String],
) -> BTreeMap<String, InstanceStatus> {
    let mut statuses = BTreeMap::new();
    let plan_entries = match &evidence.plan {
        Ok(plan) => plan.plan.entries.clone(),
        Err(_) => Vec::new(),
    };
    let after: BTreeMap<&str, &[String]> = plan_entries
        .iter()
        .map(|entry| (entry.instance_id.as_str(), entry.after.as_slice()))
        .collect();
    let triggers: BTreeMap<&str, &ContextRequirementFacts> = evidence
        .context
        .requirements
        .iter()
        .map(|requirement| (requirement.instance_id.as_str(), requirement))
        .collect();
    let positions = journal_positions(evidence);
    let result_instances: BTreeMap<&str, &super::decode::ResultInstanceFacts> =
        evidence
            .result
            .as_ref()
            .map(|result| {
                result
                    .instances
                    .iter()
                    .map(|instance| (instance.instance_id.as_str(), instance))
                    .collect()
            })
            .unwrap_or_default();

    for instance_id in order {
        let status = match decided.disposition {
            RunViewDispositionWire::Unavailable => {
                InstanceStatus::new("planned")
            }
            RunViewDispositionWire::Skipped => InstanceStatus::new("skipped"),
            RunViewDispositionWire::NotReached => {
                InstanceStatus::new("planned")
            }
            RunViewDispositionWire::Ran => {
                if let Some(result_instance) =
                    result_instances.get(instance_id.as_str())
                {
                    if map_terminal_instance_status(&result_instance.status)
                        == "planned"
                        && triggers.get(instance_id.as_str()).is_some_and(
                            |trigger| trigger.trigger == "not_triggered",
                        )
                    {
                        InstanceStatus::new("not_triggered")
                    } else {
                        InstanceStatus::new(map_terminal_instance_status(
                            &result_instance.status,
                        ))
                    }
                } else if let Some(journal_status) = evidence
                    .last_segment()
                    .and_then(|segment| segment.finished_status.clone())
                {
                    // Closed journal segment without a result file but on a
                    // live turn: carry the journal's aggregate word.
                    InstanceStatus::new(map_journal_instance_status(
                        &journal_status,
                    ))
                } else {
                    InstanceStatus::new("planned")
                }
            }
            RunViewDispositionWire::Active
            | RunViewDispositionWire::Interrupted => {
                status_from_journal_or_plan(
                    instance_id,
                    &positions,
                    triggers.get(instance_id.as_str()).copied(),
                    decided.disposition == RunViewDispositionWire::Interrupted,
                )
            }
        };
        statuses.insert(instance_id.clone(), status);
    }
    // Rule 10: a planned instance never reached after an upstream terminal
    // failed or refused outcome is `not run · blocked by X`.
    apply_blocked_rule(&mut statuses, &after, order);
    statuses
}

/// One instance's decided status with its journal position.
#[derive(Debug, Clone)]
pub(crate) struct InstanceStatus {
    pub status: String,
    pub waiting_on: Option<String>,
    pub blocked_by: Option<String>,
    pub attempt: Option<u32>,
    pub max_attempts: Option<u32>,
    pub op: Option<String>,
}

impl InstanceStatus {
    pub(crate) fn new(status: &str) -> Self {
        Self {
            status: status.to_string(),
            waiting_on: None,
            blocked_by: None,
            attempt: None,
            max_attempts: None,
            op: None,
        }
    }
}

fn status_from_journal_or_plan(
    instance_id: &str,
    positions: &BTreeMap<String, JournalInstancePosition>,
    trigger: Option<&ContextRequirementFacts>,
    interrupted: bool,
) -> InstanceStatus {
    if let Some(position) = positions.get(instance_id) {
        if position.active {
            let mut status = InstanceStatus::new(if interrupted {
                "interrupted"
            } else {
                "running"
            });
            status.attempt = position.attempt;
            status.max_attempts = position.max_attempts;
            status.op = position.op.clone();
            return status;
        }
        if let Some(finished) = &position.finished_status {
            return InstanceStatus::new(map_journal_instance_status(finished));
        }
    }
    if trigger.is_some_and(|trigger| trigger.trigger == "not_triggered") {
        return InstanceStatus::new("not_triggered");
    }
    InstanceStatus::new("planned")
}

fn is_terminal_blocker(status: &str) -> bool {
    matches!(status, "failed" | "refused")
}

fn apply_blocked_rule(
    statuses: &mut BTreeMap<String, InstanceStatus>,
    after: &BTreeMap<&str, &[String]>,
    order: &[String],
) {
    for instance_id in order {
        let reached =
            statuses.get(instance_id.as_str()).is_some_and(|status| {
                !matches!(
                    status.status.as_str(),
                    "planned" | "waiting" | "not_triggered"
                )
            });
        if reached {
            continue;
        }
        if let Some(blocker) =
            nearest_upstream_blocker(instance_id, statuses, after, order)
        {
            if let Some(status) = statuses.get_mut(instance_id.as_str()) {
                status.status = "not_run".to_string();
                status.blocked_by = Some(blocker);
                status.waiting_on = None;
            }
        }
    }
}

/// The earliest-order terminal failed/refused instance in the transitive
/// `after` closure of `instance_id`, if any.
fn nearest_upstream_blocker(
    instance_id: &str,
    statuses: &BTreeMap<String, InstanceStatus>,
    after: &BTreeMap<&str, &[String]>,
    order: &[String],
) -> Option<String> {
    let mut closure = BTreeSet::new();
    let mut stack: Vec<&str> = after
        .get(instance_id)
        .map(|deps| deps.iter().map(String::as_str).collect())
        .unwrap_or_default();
    while let Some(dependency) = stack.pop() {
        if closure.insert(dependency.to_string()) {
            if let Some(transitive) = after.get(dependency) {
                stack.extend(transitive.iter().map(String::as_str));
            }
        }
    }
    order
        .iter()
        .filter(|candidate| closure.contains(candidate.as_str()))
        .find(|candidate| {
            statuses
                .get(candidate.as_str())
                .is_some_and(|status| is_terminal_blocker(&status.status))
        })
        .cloned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blocked_rule_pins_first_terminal_blocker() {
        let mut statuses = BTreeMap::from([
            ("check".to_string(), InstanceStatus::new("failed")),
            ("tasks".to_string(), InstanceStatus::new("planned")),
        ]);
        let check_after: Vec<String> = Vec::new();
        let tasks_after = vec!["check".to_string()];
        let after: BTreeMap<&str, &[String]> = BTreeMap::from([
            ("check", check_after.as_slice()),
            ("tasks", tasks_after.as_slice()),
        ]);
        apply_blocked_rule(
            &mut statuses,
            &after,
            &["check".to_string(), "tasks".to_string()],
        );
        assert_eq!(statuses["tasks"].status, "not_run");
        assert_eq!(statuses["tasks"].blocked_by.as_deref(), Some("check"));
    }
}
