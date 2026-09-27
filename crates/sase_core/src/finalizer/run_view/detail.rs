//! The node-view projector: DAG order, timeline, cycles, drift,
//! diagnostics, per-instance detail, and the attention hint.
//!
//! `project_finalizer_node_view` is the single entry point behind the
//! `project_finalizer_node_view` Python binding. Multi-run node
//! composition follows the D10 supersede rule: runs after the newest
//! successful settled run decide the aggregate.

use std::collections::{BTreeMap, BTreeSet};

use super::decode::{RunViewError, SubmissionFacts};
use super::detail_content::{
    build_attempts, build_deferral, build_diagnostics, build_evidence,
    build_failure_reason, build_operations, build_protocol_files,
    collect_stderr_tails, count_warnings, instance_files, latest_attempt,
    result_instance,
};
use super::precedence::{
    decide_instance_statuses, decide_run, DecidedRun, InstanceStatus,
    RunEvidence,
};
use super::selection::{replay_selection, unselected_instances};
use super::wire::{
    FinalizerNodeViewRequestWire, FinalizerNodeViewWire, RunViewAppearanceWire,
    RunViewDeclarationWire, RunViewDispositionWire, RunViewNodeInstanceWire,
    RunViewRecoveryTurnWire, RunViewRunInstanceWire, RunViewRunWire,
    RUN_VIEW_WIRE_SCHEMA_VERSION,
};

/// Project one node view from already-collected artifact text.
pub fn project_finalizer_node_view(
    request: &FinalizerNodeViewRequestWire,
) -> Result<FinalizerNodeViewWire, RunViewError> {
    if request.schema_version != RUN_VIEW_WIRE_SCHEMA_VERSION {
        return Err(RunViewError::validation(format!(
            "unsupported node view schema_version {}",
            request.schema_version
        )));
    }
    if request.runs.is_empty() {
        return Err(RunViewError::validation("node view needs one run"));
    }
    let mut runs = Vec::with_capacity(request.runs.len());
    for input in &request.runs {
        runs.push(project_run(input, request.tail_lines));
    }
    runs.sort_by_key(|run| run.number);
    let (status, glyph) = node_status_glyph(&runs);
    let attention_instance_id = runs
        .iter()
        .rev()
        .find_map(attention_for_run)
        .or_else(|| latest_attention_fallback(&runs));
    let run_level_trouble = runs.iter().any(|run| {
        run.recovery_turn.is_some()
            || !run.drift.is_empty()
            || matches!(
                run.disposition,
                RunViewDispositionWire::Interrupted
                    | RunViewDispositionWire::Unavailable
            )
            || run.diagnostics.iter().any(|diagnostic| {
                matches!(
                    diagnostic.severity,
                    super::super::wire::FinalizerDiagnosticSeverityWire::Error
                )
            })
    });
    let (instances, unselected) = compose_node_instances(request, &runs);
    Ok(FinalizerNodeViewWire {
        schema_version: RUN_VIEW_WIRE_SCHEMA_VERSION,
        status,
        glyph,
        attention_instance_id,
        run_level_trouble,
        instances,
        unselected,
        runs,
    })
}

fn project_run(
    input: &super::wire::RunViewRunInputWire,
    tail_lines: u32,
) -> RunViewRunWire {
    let evidence = RunEvidence::collect(input);
    let decided = decide_run(&evidence);
    let order = dag_order(&evidence);
    let statuses = decide_instance_statuses(&evidence, &decided, &order);
    let reasons = selection_reasons(&evidence);
    let declarations = declaration_timeline(&evidence);
    let recovery_turn = recovery_turn(&evidence, input);
    let cycles = run_cycles(&evidence, &decided);
    let drift = match &evidence.plan {
        Ok(_) => evidence.meta.drift.clone(),
        Err(_) => Vec::new(),
    };
    let diagnostics = evidence
        .result
        .as_ref()
        .map(|result| result.diagnostics.clone())
        .unwrap_or_default();
    let plan_digest = evidence
        .plan
        .as_ref()
        .ok()
        .map(|plan| plan.plan.plan_digest.clone());
    let waiting = waiting_on_map(&evidence, &order, &statuses);
    let instances = order
        .iter()
        .map(|instance_id| {
            project_run_instance(
                &evidence,
                input,
                instance_id,
                statuses.get(instance_id.as_str()),
                reasons.get(instance_id.as_str()),
                waiting.get(instance_id.as_str()).cloned().flatten(),
                tail_lines,
            )
        })
        .collect();
    RunViewRunWire {
        run_id: input.run_id.clone(),
        number: input.number,
        label: input.label.clone(),
        kind: input.kind,
        disposition: decided.disposition,
        reason: decided.reason,
        plan_digest,
        result_status: decided.result_status,
        cycles,
        earlier_segments: evidence.journal.earlier_segments,
        reactivated: evidence.journal.earlier_segments > 0,
        declarations,
        recovery_turn,
        drift,
        diagnostics,
        instances,
    }
}

/// DAG order from plan `resolved_index`, falling back to entry order when
/// the plan is unavailable. Unknown `after` ids are ignored tolerantly.
pub(crate) fn dag_order(evidence: &RunEvidence) -> Vec<String> {
    let plan = match &evidence.plan {
        Ok(plan) => &plan.plan,
        Err(_) => return Vec::new(),
    };
    let mut entries = plan.entries.clone();
    entries.sort_by_key(|entry| entry.resolved_index);
    let known: BTreeSet<&str> = entries
        .iter()
        .map(|entry| entry.instance_id.as_str())
        .collect();
    // Stable topo pass in resolved_index order: entries whose known deps
    // are already emitted go first; anything left (a cycle) keeps plan
    // order rather than vanishing.
    let mut emitted = BTreeSet::new();
    let mut order = Vec::with_capacity(entries.len());
    let mut remaining: Vec<_> = entries.iter().collect();
    while !remaining.is_empty() {
        let mut progressed = false;
        let mut rest = Vec::with_capacity(remaining.len());
        for entry in remaining {
            let ready = entry
                .after
                .iter()
                .filter(|dependency| known.contains(dependency.as_str()))
                .all(|dependency| emitted.contains(dependency.as_str()));
            if ready {
                emitted.insert(entry.instance_id.as_str());
                order.push(entry.instance_id.clone());
                progressed = true;
            } else {
                rest.push(entry);
            }
        }
        if !progressed {
            for entry in rest {
                emitted.insert(entry.instance_id.as_str());
                order.push(entry.instance_id.clone());
            }
            break;
        }
        remaining = rest;
    }
    order
}

fn selection_reasons(evidence: &RunEvidence) -> BTreeMap<String, String> {
    match &evidence.plan {
        Ok(plan) => replay_selection(&plan.plan, &evidence.authority)
            .into_iter()
            .map(|item| (item.instance_id, item.reason))
            .collect(),
        Err(_) => BTreeMap::new(),
    }
}

/// The declaration timeline: submission attempts first, journal
/// declaration events when no attempts were collected.
fn declaration_timeline(evidence: &RunEvidence) -> Vec<RunViewDeclarationWire> {
    if !evidence.timeline_from_attempts.is_empty() {
        return evidence.timeline_from_attempts.clone();
    }
    evidence
        .journal
        .segments
        .last()
        .map(|segment| segment.declaration_statuses.clone())
        .unwrap_or_default()
}

fn recovery_turn(
    evidence: &RunEvidence,
    input: &super::wire::RunViewRunInputWire,
) -> Option<RunViewRecoveryTurnWire> {
    if let Some(recovery) = evidence
        .journal
        .segments
        .last()
        .and_then(|segment| segment.recovery.clone())
    {
        return Some(recovery);
    }
    let names = [
        "final_declaration_recovery_evidence.md",
        "final_declaration_recovery_prompt.md",
        "final_declaration_recovery_response.md",
    ];
    if input
        .recovery_files
        .iter()
        .any(|name| names.contains(&name.as_str()))
    {
        return Some(RunViewRecoveryTurnWire {
            ok: None,
            code: None,
        });
    }
    None
}

fn run_cycles(evidence: &RunEvidence, decided: &DecidedRun) -> u32 {
    let journal_cycles = evidence
        .journal
        .segments
        .last()
        .map(|segment| {
            segment
                .cycles
                .iter()
                .max()
                .copied()
                .unwrap_or(segment.cycles.len() as u32)
        })
        .unwrap_or(0);
    let result_cycles = evidence
        .result
        .as_ref()
        .map(|result| result.cycles)
        .unwrap_or(0);
    let journal_finished = evidence
        .journal
        .segments
        .last()
        .and_then(|segment| segment.finished_cycles)
        .unwrap_or(0);
    let cycles = result_cycles.max(journal_cycles).max(journal_finished);
    if cycles > 0 {
        return cycles;
    }
    // A run that started work ran at least one cycle.
    if decided.result_status.is_some()
        || evidence
            .journal
            .segments
            .last()
            .is_some_and(|segment| segment.phase_started_run_id.is_some())
    {
        return 1;
    }
    0
}

fn project_run_instance(
    evidence: &RunEvidence,
    input: &super::wire::RunViewRunInputWire,
    instance_id: &str,
    status: Option<&InstanceStatus>,
    reason: Option<&String>,
    waiting_on: Option<String>,
    tail_lines: u32,
) -> RunViewRunInstanceWire {
    let plan_entry = evidence.plan.as_ref().ok().and_then(|plan| {
        plan.plan
            .entries
            .iter()
            .find(|entry| entry.instance_id == instance_id)
    });
    let trigger = evidence
        .context
        .requirements
        .iter()
        .find(|requirement| requirement.instance_id == instance_id);
    let summary = lookup_submission_summary(&evidence.submission, instance_id);
    let status = status
        .cloned()
        .unwrap_or_else(|| InstanceStatus::new("planned"));
    let result = evidence
        .result
        .as_ref()
        .and_then(|result| result_instance(&result.instances, instance_id));
    let segment = evidence.journal.segments.last();
    let journal_attempts: Vec<_> = segment
        .map(|segment| {
            segment
                .attempt_events
                .iter()
                .filter(|event| event.instance_id == instance_id)
                .cloned()
                .collect()
        })
        .unwrap_or_default();
    let journal_ops: Vec<_> = segment
        .map(|segment| {
            segment
                .op_events
                .iter()
                .filter(|event| event.instance_id == instance_id)
                .cloned()
                .collect()
        })
        .unwrap_or_default();
    let files = instance_files(&input.instances, instance_id);
    let attempts = build_attempts(result, &journal_attempts);
    let latest = latest_attempt(result, &journal_attempts);
    let operations = build_operations(files, &journal_ops, tail_lines);
    let (typed_evidence, headline) = build_evidence(result);
    let run_diagnostics: Vec<_> = evidence
        .result
        .as_ref()
        .map(|result| {
            result
                .diagnostics
                .iter()
                .filter(|diagnostic| {
                    diagnostic.instance_id.as_deref() == Some(instance_id)
                })
                .cloned()
                .collect()
        })
        .unwrap_or_default();
    let instance_diagnostics = result
        .map(|result| result.diagnostics.as_slice())
        .unwrap_or(&[]);
    let terminal_success = status.status == "success";
    let diagnostics = build_diagnostics(
        instance_diagnostics,
        &run_diagnostics,
        terminal_success,
        latest,
    );
    let warnings = count_warnings(&operations, &diagnostics, latest);
    let failure_reason = build_failure_reason(
        &status.status,
        &diagnostics,
        &collect_stderr_tails(files),
    );
    RunViewRunInstanceWire {
        instance_id: instance_id.to_string(),
        provider_ref: plan_entry.map(|entry| entry.provider_ref.clone()),
        selection_reason: reason.cloned(),
        after: plan_entry
            .map(|entry| entry.after.clone())
            .unwrap_or_default(),
        status: status.status,
        waiting_on: waiting_on.or(status.waiting_on),
        blocked_by: status.blocked_by,
        trigger_kind: trigger
            .map(|requirement| requirement.trigger.clone())
            .filter(|trigger| !trigger.is_empty()),
        submission_required: trigger
            .map(|requirement| requirement.submission_required)
            .unwrap_or(false),
        obligation_count: evidence.context.obligation_count,
        payload_summary: summary,
        attempt: status.attempt,
        max_attempts: status.max_attempts,
        op: status.op,
        evidence: typed_evidence,
        headline,
        attempts,
        operations,
        diagnostics,
        warnings,
        refusal_reason: result.and_then(|result| result.refusal_reason.clone()),
        deferral: build_deferral(result),
        failure_reason,
        protocol_files: build_protocol_files(files),
    }
}

/// For planned instances whose DAG deps are not all successful, the
/// first such dep is the `waiting_on` target.
fn waiting_on_map(
    evidence: &RunEvidence,
    order: &[String],
    statuses: &BTreeMap<String, InstanceStatus>,
) -> BTreeMap<String, Option<String>> {
    let mut waiting = BTreeMap::new();
    let Ok(plan) = evidence.plan.as_ref() else {
        return waiting;
    };
    for instance_id in order {
        let reached = statuses
            .get(instance_id.as_str())
            .is_none_or(|status| status.status != "planned");
        if reached {
            waiting.insert(instance_id.clone(), None);
            continue;
        }
        let target = plan
            .plan
            .entries
            .iter()
            .find(|entry| entry.instance_id == *instance_id)
            .and_then(|entry| {
                entry
                    .after
                    .iter()
                    .find(|dependency| {
                        statuses
                            .get(dependency.as_str())
                            .is_none_or(|dep| dep.status != "success")
                    })
                    .cloned()
            });
        waiting.insert(instance_id.clone(), target);
    }
    waiting
}

fn lookup_submission_summary(
    submission: &SubmissionFacts,
    instance_id: &str,
) -> std::collections::BTreeMap<String, String> {
    submission
        .summaries
        .get(instance_id)
        .cloned()
        .unwrap_or_default()
}

/// One run's status word for the node aggregate.
fn run_status_word(run: &RunViewRunWire) -> String {
    match run.disposition {
        RunViewDispositionWire::Active => "running".to_string(),
        RunViewDispositionWire::Ran => run
            .result_status
            .clone()
            .unwrap_or_else(|| "success".to_string()),
        RunViewDispositionWire::Skipped => "skipped".to_string(),
        RunViewDispositionWire::NotReached => "not_reached".to_string(),
        RunViewDispositionWire::Interrupted => "interrupted".to_string(),
        RunViewDispositionWire::Unavailable => "unavailable".to_string(),
    }
}

/// Severity rank for the aggregate: failed first, then refused,
/// interrupted, and unavailable together, then deferred, skipped,
/// not reached, and success last.
fn run_severity(run: &RunViewRunWire) -> u32 {
    match run_status_word(run).as_str() {
        "running" => 70,
        "failed" => 60,
        "refused" => 50,
        "interrupted" | "unavailable" => 45,
        "deferred" => 40,
        "pending" => 35,
        "skipped" => 30,
        "not_reached" => 20,
        _ => 10,
    }
}

/// Node status/glyph from the runs: active wins, else the D10 supersede
/// rule decides — only runs after the newest successful settled run count,
/// by highest severity with ties going to the latest run. A newest run
/// that settled successful leaves no later runs and reports success.
fn node_status_glyph(runs: &[RunViewRunWire]) -> (String, String) {
    if runs
        .iter()
        .any(|run| matches!(run.disposition, RunViewDispositionWire::Active))
    {
        return ("running".to_string(), "running".to_string());
    }
    let newest_success = runs
        .iter()
        .filter(|run| {
            matches!(run.disposition, RunViewDispositionWire::Ran)
                && run.result_status.as_deref() == Some("success")
        })
        .map(|run| run.number)
        .max();
    let considered: Vec<&RunViewRunWire> = match newest_success {
        Some(number) => runs.iter().filter(|run| run.number > number).collect(),
        None => runs.iter().collect(),
    };
    let Some(winner) = considered
        .into_iter()
        .max_by_key(|run| (run_severity(run), run.number))
    else {
        return ("success".to_string(), "success".to_string());
    };
    let status = run_status_word(winner);
    (status.clone(), status)
}

/// Attention: the first failing instance in DAG order, else refused,
/// deferred, interrupted, then the active one.
fn attention_for_run(run: &RunViewRunWire) -> Option<String> {
    for status in ["failed", "refused", "deferred", "interrupted"] {
        if let Some(instance) =
            run.instances.iter().find(|item| item.status == status)
        {
            return Some(instance.instance_id.clone());
        }
    }
    None
}

fn latest_attention_fallback(runs: &[RunViewRunWire]) -> Option<String> {
    let latest = runs.iter().max_by_key(|run| run.number)?;
    latest
        .instances
        .iter()
        .find(|item| item.status == "running")
        .map(|item| item.instance_id.clone())
}

/// Cached identity facts per node instance: provider ref, selection
/// reason, and `after` ids from the first run that reported them.
type NodeInstanceMeta = (Option<String>, Option<String>, Vec<String>);

/// Node instances: the union across runs in DAG order with per-run
/// appearances; unselected from the first run's authority snapshot.
fn compose_node_instances(
    request: &FinalizerNodeViewRequestWire,
    runs: &[RunViewRunWire],
) -> (
    Vec<RunViewNodeInstanceWire>,
    Vec<super::wire::RunViewUnselectedWire>,
) {
    let mut order: Vec<String> = Vec::new();
    let mut appearances: BTreeMap<String, Vec<RunViewAppearanceWire>> =
        BTreeMap::new();
    let mut meta: BTreeMap<String, NodeInstanceMeta> = BTreeMap::new();
    for run in runs {
        for instance in &run.instances {
            if !appearances.contains_key(&instance.instance_id) {
                order.push(instance.instance_id.clone());
            }
            appearances
                .entry(instance.instance_id.clone())
                .or_default()
                .push(RunViewAppearanceWire {
                    run_id: run.run_id.clone(),
                    status: instance.status.clone(),
                });
            meta.entry(instance.instance_id.clone()).or_insert_with(|| {
                (
                    instance.provider_ref.clone(),
                    instance.selection_reason.clone(),
                    instance.after.clone(),
                )
            });
        }
    }
    let latest_status = |instance_id: &str| {
        runs.iter()
            .rev()
            .find_map(|run| {
                run.instances
                    .iter()
                    .find(|item| item.instance_id == instance_id)
            })
            .map(|item| item.status.clone())
            .unwrap_or_else(|| "planned".to_string())
    };
    let instances = order
        .into_iter()
        .map(|instance_id| {
            let (provider_ref, selection_reason, after) = meta
                .remove(&instance_id)
                .unwrap_or((None, None, Vec::new()));
            RunViewNodeInstanceWire {
                status: latest_status(&instance_id),
                appearances: appearances
                    .remove(&instance_id)
                    .unwrap_or_default(),
                instance_id,
                provider_ref,
                selection_reason: selection_reason
                    .unwrap_or_else(|| "default".to_string()),
                after,
            }
        })
        .collect();
    let unselected = request
        .runs
        .first()
        .map(|input| {
            let evidence = RunEvidence::collect(input);
            match &evidence.plan {
                Ok(plan) => {
                    let selected: BTreeSet<String> = plan
                        .plan
                        .entries
                        .iter()
                        .map(|entry| entry.instance_id.clone())
                        .collect();
                    unselected_instances(
                        &plan.plan,
                        &evidence.authority,
                        &selected,
                    )
                }
                Err(_) => Vec::new(),
            }
        })
        .unwrap_or_default();
    (instances, unselected)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_request_is_rejected() {
        let request = FinalizerNodeViewRequestWire {
            schema_version: RUN_VIEW_WIRE_SCHEMA_VERSION,
            runs: Vec::new(),
            tail_lines: 12,
        };
        assert!(project_finalizer_node_view(&request).is_err());
    }

    #[test]
    fn wrong_schema_version_is_rejected() {
        let request = FinalizerNodeViewRequestWire {
            schema_version: 99,
            runs: Vec::new(),
            tail_lines: 12,
        };
        assert!(project_finalizer_node_view(&request).is_err());
    }
}
