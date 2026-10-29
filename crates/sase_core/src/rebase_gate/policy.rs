//! Deterministic policy for rebase eligibility and checker verdicts.

use std::collections::BTreeSet;
use std::path::Path;

use super::wire::{
    RebaseGateChangeKindWire, RebaseGateChangedPathWire,
    RebaseGateClassifyRequestWire, RebaseGateContextUpdateWire,
    RebaseGateContextWire, RebaseGateDecisionWire, RebaseGateError,
    RebaseGateFactsWire, RebaseGateFindingWire, RebaseGateInputIdsWire,
    RebaseGateRebaseOutcomeWire, RebaseGateReportStatusWire,
    RebaseGateReportWire, RebaseGateVerdictWire,
    REBASE_GATE_DEFAULT_TIMEOUT_SECONDS, REBASE_GATE_MAX_TIMEOUT_SECONDS,
    REBASE_GATE_WIRE_SCHEMA_VERSION,
};

fn invalid(message: impl Into<String>) -> RebaseGateError {
    RebaseGateError(message.into())
}

fn validate_oid(value: &str, field: &str) -> Result<(), RebaseGateError> {
    if !matches!(value.len(), 40 | 64)
        || !value.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(invalid(format!(
            "{field} must be a full 40- or 64-character hexadecimal object ID"
        )));
    }
    Ok(())
}

fn validate_repository_root(root: &str) -> Result<(), RebaseGateError> {
    if root.trim().is_empty() || !Path::new(root).is_absolute() {
        return Err(invalid("repository_root must be an absolute path"));
    }
    if root.as_bytes().contains(&0) {
        return Err(invalid("repository_root cannot contain NUL"));
    }
    Ok(())
}

fn validate_input_ids(
    ids: &RebaseGateInputIdsWire,
) -> Result<(), RebaseGateError> {
    validate_oid(&ids.old_head, "old_head")?;
    validate_oid(&ids.upstream_tip, "upstream_tip")?;
    validate_oid(&ids.merge_base, "merge_base")
}

fn validate_path(path: &str, field: &str) -> Result<(), RebaseGateError> {
    if path.is_empty() || path.starts_with('/') || path.as_bytes().contains(&0)
    {
        return Err(invalid(format!(
            "{field} must be a nonempty repository-relative path"
        )));
    }
    if path.split('/').any(|component| {
        component.is_empty() || component == "." || component == ".."
    }) {
        return Err(invalid(format!(
            "{field} must use normalized repository-relative path components"
        )));
    }
    Ok(())
}

fn validate_change(
    change: &RebaseGateChangedPathWire,
) -> Result<(), RebaseGateError> {
    validate_path(&change.path, "changed path")?;
    match (change.kind, change.old_path.as_deref()) {
        (
            RebaseGateChangeKindWire::Renamed
            | RebaseGateChangeKindWire::Copied,
            Some(old),
        ) => {
            validate_path(old, "old_path")?;
            if old == change.path {
                return Err(invalid(
                    "rename/copy old_path must differ from path",
                ));
            }
        }
        (
            RebaseGateChangeKindWire::Renamed
            | RebaseGateChangeKindWire::Copied,
            None,
        ) => {
            return Err(invalid("rename/copy changes require old_path"));
        }
        (_, Some(_)) => {
            return Err(invalid(
                "old_path is valid only for rename/copy changes",
            ));
        }
        (_, None) => {}
    }
    Ok(())
}

fn canonical_changes(
    changes: impl IntoIterator<Item = RebaseGateChangedPathWire>,
) -> Result<Vec<RebaseGateChangedPathWire>, RebaseGateError> {
    let mut unique = BTreeSet::new();
    for change in changes {
        validate_change(&change)?;
        unique.insert(change);
    }
    Ok(unique.into_iter().collect())
}

fn validate_context(
    context: &RebaseGateContextWire,
) -> Result<(), RebaseGateError> {
    if context.schema_version != REBASE_GATE_WIRE_SCHEMA_VERSION {
        return Err(invalid(format!(
            "unsupported context schema_version {}; expected {}",
            context.schema_version, REBASE_GATE_WIRE_SCHEMA_VERSION
        )));
    }
    validate_repository_root(&context.repository_root)?;
    if context.inputs.is_empty() {
        return Err(invalid("context must retain at least one input ID set"));
    }
    for ids in &context.inputs {
        validate_input_ids(ids)?;
    }
    validate_oid(&context.current_head, "current_head")?;
    validate_oid(&context.current_tree, "current_tree")?;
    for change in &context.changed_paths {
        validate_change(change)?;
    }
    Ok(())
}

/// Build a pending context only for divergent work that Git actually applied
/// or stopped for manual conflict resolution. A HEAD change by itself is not
/// evidence that a rebase combined both sides.
pub fn build_rebase_gate_context(
    facts: &RebaseGateFactsWire,
) -> Result<Option<RebaseGateContextWire>, RebaseGateError> {
    if facts.schema_version != REBASE_GATE_WIRE_SCHEMA_VERSION {
        return Err(invalid(format!(
            "unsupported facts schema_version {}; expected {}",
            facts.schema_version, REBASE_GATE_WIRE_SCHEMA_VERSION
        )));
    }
    validate_repository_root(&facts.repository_root)?;
    let input_ids = RebaseGateInputIdsWire {
        old_head: facts.old_head.clone(),
        upstream_tip: facts.upstream_tip.clone(),
        merge_base: facts.merge_base.clone(),
    };
    validate_input_ids(&input_ids)?;
    validate_oid(&facts.current_head, "current_head")?;
    validate_oid(&facts.current_tree, "current_tree")?;

    let changed_paths = canonical_changes(
        facts
            .local_changes
            .iter()
            .chain(&facts.upstream_changes)
            .cloned(),
    )?;

    let divergent = facts.merge_base != facts.old_head
        && facts.merge_base != facts.upstream_tip;
    let left_pending_context = matches!(
        facts.rebase_outcome,
        RebaseGateRebaseOutcomeWire::Applied
            | RebaseGateRebaseOutcomeWire::Conflict
    );
    if !divergent || !left_pending_context {
        return Ok(None);
    }

    let context = RebaseGateContextWire {
        schema_version: REBASE_GATE_WIRE_SCHEMA_VERSION,
        repository_root: facts.repository_root.clone(),
        inputs: vec![input_ids],
        current_head: facts.current_head.clone(),
        current_tree: facts.current_tree.clone(),
        changed_paths,
    };
    validate_context(&context)?;
    Ok(Some(context))
}

/// Merge a newly eligible context into pending state. The latest context
/// supplies the HEAD/tree; immutable input IDs and all changed-path records
/// accumulate in deterministic order.
pub fn merge_rebase_gate_contexts(
    existing: Option<&RebaseGateContextWire>,
    incoming: Option<&RebaseGateContextWire>,
) -> Result<Option<RebaseGateContextWire>, RebaseGateError> {
    match (existing, incoming) {
        (None, None) => Ok(None),
        (Some(context), None) | (None, Some(context)) => {
            validate_context(context)?;
            Ok(Some(context.clone()))
        }
        (Some(existing), Some(incoming)) => {
            validate_context(existing)?;
            validate_context(incoming)?;
            if existing.repository_root != incoming.repository_root {
                return Err(invalid(
                    "cannot merge contexts from different repository roots",
                ));
            }
            let mut inputs = existing.inputs.clone();
            for input in &incoming.inputs {
                if !inputs.contains(input) {
                    inputs.push(input.clone());
                }
            }
            let changed_paths = canonical_changes(
                existing
                    .changed_paths
                    .iter()
                    .chain(&incoming.changed_paths)
                    .cloned(),
            )?;
            let merged = RebaseGateContextWire {
                schema_version: REBASE_GATE_WIRE_SCHEMA_VERSION,
                repository_root: existing.repository_root.clone(),
                inputs,
                current_head: incoming.current_head.clone(),
                current_tree: incoming.current_tree.clone(),
                changed_paths,
            };
            validate_context(&merged)?;
            Ok(Some(merged))
        }
    }
}

/// Refresh pending context after conflict repair or an amend without
/// replacing the immutable rebase inputs that made the check necessary.
pub fn update_rebase_gate_context(
    context: &RebaseGateContextWire,
    update: &RebaseGateContextUpdateWire,
) -> Result<RebaseGateContextWire, RebaseGateError> {
    validate_context(context)?;
    if update.schema_version != REBASE_GATE_WIRE_SCHEMA_VERSION {
        return Err(invalid(format!(
            "unsupported update schema_version {}; expected {}",
            update.schema_version, REBASE_GATE_WIRE_SCHEMA_VERSION
        )));
    }
    validate_repository_root(&update.repository_root)?;
    if update.repository_root != context.repository_root {
        return Err(invalid("context update repository_root does not match"));
    }
    validate_oid(&update.current_head, "current_head")?;
    validate_oid(&update.current_tree, "current_tree")?;
    let changed_paths = canonical_changes(
        context
            .changed_paths
            .iter()
            .chain(&update.changed_paths)
            .cloned(),
    )?;
    let updated = RebaseGateContextWire {
        schema_version: REBASE_GATE_WIRE_SCHEMA_VERSION,
        repository_root: context.repository_root.clone(),
        inputs: context.inputs.clone(),
        current_head: update.current_head.clone(),
        current_tree: update.current_tree.clone(),
        changed_paths,
    };
    validate_context(&updated)?;
    Ok(updated)
}

fn decision(
    verdict: RebaseGateVerdictWire,
    reason: impl Into<String>,
    findings: Vec<RebaseGateFindingWire>,
    diagnostics: Option<String>,
) -> RebaseGateDecisionWire {
    RebaseGateDecisionWire {
        schema_version: REBASE_GATE_WIRE_SCHEMA_VERSION,
        verdict,
        reason: reason.into(),
        findings,
        diagnostics,
    }
}

fn warn(
    reason: impl Into<String>,
    diagnostics: Option<String>,
) -> RebaseGateDecisionWire {
    decision(RebaseGateVerdictWire::Warn, reason, Vec::new(), diagnostics)
}

fn validate_report_findings(
    report: &RebaseGateReportWire,
    context: &RebaseGateContextWire,
) -> Result<(), String> {
    for finding in &report.findings {
        if finding.line == 0
            || finding.module.trim().is_empty()
            || finding.imported_name.trim().is_empty()
            || finding.explanation.trim().is_empty()
        {
            return Err(
                "finding is missing a positive line or concrete import detail"
                    .into(),
            );
        }
        validate_path(&finding.path, "finding path")
            .map_err(|error| error.to_string())?;
        let is_current_changed_path =
            context.changed_paths.iter().any(|change| {
                change.path == finding.path
                    && change.kind != RebaseGateChangeKindWire::Deleted
            });
        if !is_current_changed_path {
            return Err(format!(
                "finding path {:?} is not a changed path in the pending context",
                finding.path
            ));
        }
    }
    Ok(())
}

/// Classify process facts and the checker's untrusted JSON report. Only a
/// matching clean report or concrete matching findings can affect the push;
/// every infrastructure or protocol problem warns and fails open.
pub fn classify_rebase_gate_result(
    request: &RebaseGateClassifyRequestWire,
) -> RebaseGateDecisionWire {
    if request.schema_version != REBASE_GATE_WIRE_SCHEMA_VERSION {
        return warn("unsupported classify-request schema; gate skipped", None);
    }
    if request.config.hook_command.trim().is_empty() {
        return decision(
            RebaseGateVerdictWire::Skip,
            "after-rebase hook is disabled",
            Vec::new(),
            None,
        );
    }
    if !request.config.timeout_seconds.is_finite()
        || request.config.timeout_seconds <= 0.0
        || request.config.timeout_seconds > REBASE_GATE_MAX_TIMEOUT_SECONDS
    {
        return warn(
            "invalid after-rebase timeout; gate skipped",
            Some(format!(
                "timeout_seconds must be greater than 0 and at most {REBASE_GATE_MAX_TIMEOUT_SECONDS}; default is {REBASE_GATE_DEFAULT_TIMEOUT_SECONDS}"
            )),
        );
    }
    if let Err(error) = validate_context(&request.context) {
        return warn(
            "invalid pending rebase context; gate skipped",
            Some(error.to_string()),
        );
    }
    if request.command.timed_out {
        return warn("after-rebase hook timed out; allowing publication", None);
    }
    if let Some(signal) = request.command.signal {
        return warn(
            "after-rebase hook was terminated by a signal; allowing publication",
            Some(format!("signal {signal}")),
        );
    }
    if let Some(error) = &request.command.spawn_error {
        return warn(
            "could not start after-rebase hook; allowing publication",
            Some(error.clone()),
        );
    }
    let Some(exit_code) = request.command.exit_code else {
        return warn(
            "after-rebase hook has no exit status; allowing publication",
            None,
        );
    };
    if !matches!(exit_code, 0 | 1) {
        return warn(
            "after-rebase hook returned an unsupported exit status; allowing publication",
            Some(format!("exit code {exit_code}")),
        );
    }
    let Some(raw_report) = request.report.clone() else {
        return warn("after-rebase hook did not write a result report; allowing publication", None);
    };
    let report: RebaseGateReportWire = match serde_json::from_value(raw_report)
    {
        Ok(report) => report,
        Err(error) => {
            return warn(
                "after-rebase hook wrote a malformed result report; allowing publication",
                Some(error.to_string()),
            );
        }
    };
    if report.schema_version != REBASE_GATE_WIRE_SCHEMA_VERSION {
        return warn("after-rebase result report has an unsupported schema; allowing publication", None);
    }
    if report.checked_head != request.context.current_head
        || report.checked_tree != request.context.current_tree
    {
        return warn(
            "after-rebase result report is stale; allowing publication",
            None,
        );
    }
    if let Err(error) = validate_report_findings(&report, &request.context) {
        return warn("after-rebase result report has invalid findings; allowing publication", Some(error));
    }

    match (exit_code, report.status) {
        (0, RebaseGateReportStatusWire::Clean) if report.findings.is_empty() => decision(
            RebaseGateVerdictWire::Pass,
            "after-rebase hook reported a clean matching tree",
            Vec::new(),
            report.diagnostics,
        ),
        (1, RebaseGateReportStatusWire::Findings) if !report.findings.is_empty() => decision(
            RebaseGateVerdictWire::Block,
            "after-rebase hook reported confirmed findings",
            report.findings,
            report.diagnostics,
        ),
        (_, RebaseGateReportStatusWire::Error) => warn(
            "after-rebase hook reported an infrastructure error; allowing publication",
            report.diagnostics,
        ),
        _ => warn(
            "after-rebase exit status does not match its result report; allowing publication",
            report.diagnostics,
        ),
    }
}
