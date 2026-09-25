//! Typed-plan resolution: wait binding, hold-cycle validation, proc
//! shell/workspace/project policy, dispatch validation, and the approval
//! preview plus content digest rendered from the resolved plan.
use super::directive_scan::{
    directive_occurrences, disabled_region_ranges,
    launch_inline_literal_ranges, leading_blank_line_re, position_in_ranges,
    DirectiveOccurrence,
};
use super::typed_units::{RawLaunchUnit, RawWaitTargetKind};
use super::wires::{
    LaunchPlanDiagnosticWire, LaunchUnitPayloadWire, LaunchUnitWire,
    ProcUnitWire, WaitTargetWire, LAUNCH_PLAN_WIRE_SCHEMA_VERSION,
};
use crate::agent_identity::agent_name_in_hood;
use crate::fenced_code::fenced_block_ranges;
use crate::hold_directive::{format_hold_directive, HoldFieldsWire};
use crate::queue_directive::{
    format_queue_capacity_multiplier, format_queue_weight,
};
use regex::Regex;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::OnceLock;

pub(crate) fn resolve_typed_waits(
    raw_units: &mut [RawLaunchUnit],
    diagnostics: &mut Vec<LaunchPlanDiagnosticWire>,
) {
    let logical_ids: BTreeSet<String> = raw_units
        .iter()
        .map(|raw| raw.unit.logical_id.clone())
        .collect();
    let mut agent_names: BTreeMap<String, String> = BTreeMap::new();
    let mut proc_names: BTreeMap<String, String> = BTreeMap::new();
    for raw in raw_units.iter() {
        match &raw.unit.payload {
            LaunchUnitPayloadWire::Agent(agent) => {
                if let Some(identity) = agent.effective_identity() {
                    agent_names.insert(identity, raw.unit.logical_id.clone());
                }
            }
            LaunchUnitPayloadWire::Proc(proc_unit) => {
                if let Some(shell_name) = proc_unit.shell_name.as_ref() {
                    proc_names.insert(
                        shell_name.clone(),
                        raw.unit.logical_id.clone(),
                    );
                }
            }
        }
    }

    for index in 0..raw_units.len() {
        let logical_id = raw_units[index].unit.logical_id.clone();
        let mut waits = Vec::new();
        for wait in raw_units[index].raw_waits.clone() {
            match wait.target {
                RawWaitTargetKind::Previous => {
                    if index == 0 {
                        diagnostics.push(typed_unit_diagnostic(
                            "bare-wait-without-predecessor",
                            "Bare %wait requires a preceding launch unit.",
                            &logical_id,
                            wait.source_span,
                        ));
                    } else {
                        waits.push(WaitTargetWire::Logical {
                            logical_id: raw_units[index - 1]
                                .unit
                                .logical_id
                                .clone(),
                            source: wait.source,
                        });
                    }
                }
                RawWaitTargetKind::Unit(target) => {
                    if logical_ids.contains(&target) {
                        waits.push(WaitTargetWire::Logical {
                            logical_id: target,
                            source: wait.source,
                        });
                    } else {
                        diagnostics.push(typed_unit_diagnostic(
                            "unknown-logical-wait",
                            &format!(
                                "Unknown launch unit wait target {target:?}."
                            ),
                            &logical_id,
                            wait.source_span,
                        ));
                    }
                }
                RawWaitTargetKind::Agent(target) => {
                    if let Some(unit) = agent_names.get(&target) {
                        waits.push(WaitTargetWire::Logical {
                            logical_id: unit.clone(),
                            source: wait.source,
                        });
                    } else {
                        waits.push(WaitTargetWire::Agent { name: target });
                    }
                }
                RawWaitTargetKind::Proc(target) => {
                    if let Some(unit) = proc_names.get(&target) {
                        waits.push(WaitTargetWire::Logical {
                            logical_id: unit.clone(),
                            source: wait.source,
                        });
                    } else if logical_ids.contains(&target) {
                        waits.push(WaitTargetWire::Logical {
                            logical_id: target,
                            source: wait.source,
                        });
                    } else {
                        waits.push(WaitTargetWire::Proc { identifier: target });
                    }
                }
                RawWaitTargetKind::Bead(bead_id) => {
                    waits.push(WaitTargetWire::Bead { bead_id });
                }
                RawWaitTargetKind::Time(value) => {
                    waits.push(WaitTargetWire::Time { value });
                }
            }
        }
        raw_units[index].unit.waits = waits;
    }
}

pub(crate) fn validate_typed_wait_cycles(
    raw_units: &[RawLaunchUnit],
    diagnostics: &mut Vec<LaunchPlanDiagnosticWire>,
) {
    let index_by_id: BTreeMap<String, usize> = raw_units
        .iter()
        .enumerate()
        .map(|(index, raw)| (raw.unit.logical_id.clone(), index))
        .collect();
    let mut graph: Vec<Vec<usize>> = vec![Vec::new(); raw_units.len()];
    for (index, raw) in raw_units.iter().enumerate() {
        for wait in &raw.unit.waits {
            if let WaitTargetWire::Logical { logical_id, .. } = wait {
                if let Some(target) = index_by_id.get(logical_id) {
                    graph[index].push(*target);
                }
            }
        }
    }
    let mut state = vec![0_u8; raw_units.len()];
    for index in 0..raw_units.len() {
        if state[index] == 0
            && wait_cycle_visit(index, &graph, &mut state).is_some()
        {
            diagnostics.push(typed_plan_diagnostic(
                "wait-cycle",
                "Typed launch waits contain a cycle.",
                None,
            ));
            return;
        }
    }
    let mut graph_with_holds = graph;
    add_hold_cycle_edges(raw_units, &index_by_id, &mut graph_with_holds);
    let mut state = vec![0_u8; raw_units.len()];
    for index in 0..raw_units.len() {
        if state[index] == 0
            && wait_cycle_visit(index, &graph_with_holds, &mut state).is_some()
        {
            diagnostics.push(typed_plan_diagnostic(
                "hold-cycle",
                "Typed launch holds and waits contain a cycle (a `future` hold fences every other unit in the plan).",
                None,
            ));
            return;
        }
    }
}

#[derive(Debug)]
struct UnitHoldFacts {
    identity: Option<String>,
    agent_session: Option<String>,
    clan: Option<String>,
    tribe: Option<String>,
    workflow: Option<String>,
}

fn add_hold_cycle_edges(
    raw_units: &[RawLaunchUnit],
    index_by_id: &BTreeMap<String, usize>,
    graph: &mut [Vec<usize>],
) {
    let facts: Vec<UnitHoldFacts> =
        raw_units.iter().map(unit_hold_facts).collect();
    for (holder_index, raw) in raw_units.iter().enumerate() {
        let Some(hold) = unit_hold_fields(&raw.unit.payload) else {
            continue;
        };
        let holder_facts = &facts[holder_index];
        for (target_index, target_facts) in facts.iter().enumerate() {
            if holder_index == target_index
                || hold_kin_excluded(holder_facts, target_facts)
            {
                continue;
            }
            if hold_matches_unit(hold, target_facts) {
                let holder_id = &raw.unit.logical_id;
                if let Some(holder_graph_index) = index_by_id.get(holder_id) {
                    graph[target_index].push(*holder_graph_index);
                }
            }
        }
    }
}

fn unit_hold_fields(
    payload: &LaunchUnitPayloadWire,
) -> Option<&HoldFieldsWire> {
    match payload {
        LaunchUnitPayloadWire::Agent(agent) => agent.hold.as_ref(),
        LaunchUnitPayloadWire::Proc(proc_unit) => proc_unit.hold.as_ref(),
    }
}

fn unit_hold_facts(raw: &RawLaunchUnit) -> UnitHoldFacts {
    match &raw.unit.payload {
        LaunchUnitPayloadWire::Agent(agent) => {
            let identity = agent.effective_identity();
            let identity_ref = identity.as_deref();
            let agent_session = identity_ref
                .and_then(|name| {
                    crate::agent_identity::parse_agent_session_name(name).ok()
                })
                .map(|parsed| parsed.agent_session_name);
            UnitHoldFacts {
                identity,
                agent_session,
                clan: agent.clan.clone(),
                tribe: agent.tribe.clone().or_else(|| agent.clan_tribe.clone()),
                workflow: agent.workspace_reference.clone(),
            }
        }
        LaunchUnitPayloadWire::Proc(proc_unit) => UnitHoldFacts {
            identity: proc_unit.shell_name.clone(),
            agent_session: None,
            clan: None,
            tribe: None,
            workflow: proc_unit.selected_project.clone(),
        },
    }
}

fn hold_matches_unit(hold: &HoldFieldsWire, target: &UnitHoldFacts) -> bool {
    if hold.future {
        return true;
    }
    hold.names.iter().any(|name| {
        target.identity.as_deref() == Some(name.as_str())
            || target.agent_session.as_deref() == Some(name.as_str())
            || target.clan.as_deref() == Some(name.as_str())
            || target.workflow.as_deref() == Some(name.as_str())
    }) || hold
        .tribes
        .iter()
        .any(|tribe| target.tribe.as_deref() == Some(tribe.as_str()))
        || hold.hoods.iter().any(|hood| {
            target.identity.as_deref().is_some_and(|name| {
                agent_name_in_hood(name, hood).unwrap_or(false)
            })
        })
}

fn hold_kin_excluded(holder: &UnitHoldFacts, target: &UnitHoldFacts) -> bool {
    if holder.identity.is_some() && holder.identity == target.identity {
        return true;
    }
    if holder.clan.is_some() && holder.clan == target.clan {
        return true;
    }
    match (
        holder.agent_session.as_deref(),
        target.agent_session.as_deref(),
    ) {
        (Some(holder_agent_session), Some(target_agent_session)) => {
            target_agent_session == holder_agent_session
                || target_agent_session
                    .strip_prefix(holder_agent_session)
                    .is_some_and(|rest| rest.starts_with('.'))
        }
        _ => false,
    }
}

fn wait_cycle_visit(
    index: usize,
    graph: &[Vec<usize>],
    state: &mut [u8],
) -> Option<usize> {
    state[index] = 1;
    for target in &graph[index] {
        if state[*target] == 1 {
            return Some(*target);
        }
        if state[*target] == 0
            && wait_cycle_visit(*target, graph, state).is_some()
        {
            return Some(*target);
        }
    }
    state[index] = 2;
    None
}

pub(crate) fn validate_proc_shell_name(
    shell_name: Option<&str>,
    logical_id: &str,
    diagnostics: &mut Vec<LaunchPlanDiagnosticWire>,
) {
    let Some(shell_name) = shell_name else {
        return;
    };
    if shell_name.contains("--") {
        diagnostics.push(typed_unit_diagnostic(
            "invalid-proc-shell-name",
            "Proc %id names cannot use the agent-session `--` convention.",
            logical_id,
            None,
        ));
    }
    if !is_valid_proc_shell_name(shell_name) {
        diagnostics.push(typed_unit_diagnostic(
            "invalid-proc-shell-name",
            "Proc %id names must be bare identifiers containing only letters, digits, `_`, `.`, or `-`.",
            logical_id,
            None,
        ));
    }
}

fn is_valid_proc_shell_name(value: &str) -> bool {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    (first.is_ascii_alphabetic() || first == '_')
        && chars.all(|ch| {
            ch.is_ascii_alphanumeric() || matches!(ch, '_' | '.' | '-')
        })
}

pub(crate) fn parse_proc_workspace(
    raw: Option<&str>,
    selected_project: Option<&str>,
    logical_id: &str,
    diagnostics: &mut Vec<LaunchPlanDiagnosticWire>,
) -> bool {
    let Some(raw) = raw else {
        return selected_project.is_some();
    };
    match raw.to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => true,
        "false" | "0" | "no" | "off" => false,
        _ => {
            diagnostics.push(typed_unit_diagnostic(
                "invalid-proc-workspace",
                "%proc workspace= must be a Boolean.",
                logical_id,
                None,
            ));
            false
        }
    }
}

pub(crate) fn validate_proc_project_policy(
    selected_project: Option<&str>,
    workspace: bool,
    cwd: Option<&str>,
    logical_id: &str,
    diagnostics: &mut Vec<LaunchPlanDiagnosticWire>,
) {
    if workspace && selected_project.is_none() {
        diagnostics.push(typed_unit_diagnostic(
            "workspace-without-project",
            "%proc workspace=true requires a selected project.",
            logical_id,
            None,
        ));
    }
    if selected_project.is_none() && !workspace && cwd.is_none() {
        diagnostics.push(typed_unit_diagnostic(
            "proc-cwd-required",
            "%proc without a selected project requires an explicit cwd=.",
            logical_id,
            None,
        ));
    }
}

pub(crate) fn typed_directive_ignored_ranges(
    prompt: &str,
) -> Vec<(usize, usize)> {
    let mut ranges = fenced_block_ranges(prompt);
    ranges.extend(disabled_region_ranges(prompt));
    if prompt.contains('`') {
        ranges.extend(launch_inline_literal_ranges(prompt));
    }
    ranges
}

pub(crate) fn strip_prompt_regions(
    prompt: &str,
    regions: &[(usize, usize)],
) -> String {
    let mut merged = merge_ranges(regions);
    merged.sort_by_key(|range| std::cmp::Reverse(range.0));
    let mut cleaned = prompt.to_string();
    for (start, end) in merged {
        if start <= end && end <= cleaned.len() {
            cleaned.replace_range(start..end, "");
        }
    }
    leading_blank_line_re().replace(&cleaned, "").to_string()
}

fn merge_ranges(regions: &[(usize, usize)]) -> Vec<(usize, usize)> {
    let mut sorted: Vec<(usize, usize)> = regions
        .iter()
        .copied()
        .filter(|(start, end)| start < end)
        .collect();
    sorted.sort_by_key(|range| range.0);
    let mut merged: Vec<(usize, usize)> = Vec::new();
    for (start, end) in sorted {
        if let Some((_, last_end)) = merged.last_mut() {
            if start <= *last_end {
                *last_end = (*last_end).max(end);
                continue;
            }
        }
        merged.push((start, end));
    }
    merged
}

pub(crate) struct ProjectRefCapture {
    pub(crate) provider: String,
    pub(crate) reference: String,
    pub(crate) span: (usize, usize),
}

pub(crate) fn project_context_from_prompt(prompt: &str) -> Option<String> {
    project_ref_captures(prompt)
        .into_iter()
        .next()
        .map(|capture| capture.reference)
}

pub(crate) fn project_ref_captures(prompt: &str) -> Vec<ProjectRefCapture> {
    let ignored = typed_directive_ignored_ranges(prompt);
    project_ref_re()
        .captures_iter(prompt)
        .filter_map(|captures| {
            let marker = captures.get(2)?;
            if position_in_ranges(marker.start(), &ignored) {
                return None;
            }
            let reference = marker.as_str();
            let provider = reference
                .strip_prefix("#")
                .and_then(|rest| rest.split(':').next())
                .unwrap_or_default()
                .to_string();
            if provider.is_empty() {
                return None;
            }
            Some(ProjectRefCapture {
                provider,
                reference: reference.to_string(),
                span: (marker.start(), marker.end()),
            })
        })
        .collect()
}

pub(crate) fn parse_dispatch_target(
    directive: &DirectiveOccurrence,
) -> Result<String, LaunchPlanDiagnosticWire> {
    let span = [directive.start, directive.end];
    if directive.has_plus_suffix {
        return Err(typed_plan_diagnostic(
            "invalid-dispatch-form",
            "%dispatch does not support '+'; use %dispatch:<machine>.",
            Some(span),
        ));
    }
    let raw = directive
        .args
        .iter()
        .find(|arg| !arg.is_empty())
        .cloned()
        .unwrap_or_default();
    let target = raw.trim();
    if target.is_empty() {
        return Err(typed_plan_diagnostic(
            "invalid-dispatch-target",
            "'%dispatch' requires a configured machine alias",
            Some(span),
        ));
    }
    if !dispatch_alias_re().is_match(target) {
        return Err(typed_plan_diagnostic(
            "invalid-dispatch-target",
            "'%dispatch' requires a configured machine alias",
            Some(span),
        ));
    }
    if target.eq_ignore_ascii_case("local") {
        return Err(typed_plan_diagnostic(
            "dispatch-local-reserved",
            "'%dispatch:local' is reserved; omit %dispatch for local launch",
            Some(span),
        ));
    }
    Ok(target.to_string())
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct DispatchCombinationFacts {
    pub(crate) is_proc: bool,
    pub(crate) has_waits: bool,
    pub(crate) has_queue: bool,
    pub(crate) has_hold: bool,
    pub(crate) has_clan: bool,
    pub(crate) has_agent_session: bool,
}

pub(crate) fn validate_dispatch_combinations(
    logical_id: &str,
    facts: DispatchCombinationFacts,
    diagnostics: &mut Vec<LaunchPlanDiagnosticWire>,
) {
    let mut forbidden = Vec::new();
    if facts.is_proc {
        forbidden.push("%proc");
    }
    if facts.has_waits {
        forbidden.push("%wait");
    }
    if facts.has_queue {
        forbidden.push("%queue");
    }
    if facts.has_hold {
        forbidden.push("%hold");
    }
    if facts.has_clan {
        forbidden.push("%clan");
    }
    if facts.has_agent_session {
        forbidden.push("%id(..., session=...)");
    }
    if forbidden.is_empty() {
        return;
    }
    diagnostics.push(typed_unit_diagnostic(
        "dispatch-unsupported-combination",
        &format!(
            "%dispatch cannot be combined with {} in V1 remote launch.",
            forbidden.join(", ")
        ),
        logical_id,
        None,
    ));
}

pub(crate) fn line_after_directive_is_blank(
    prompt: &str,
    directive_end: usize,
) -> bool {
    let rest = prompt.get(directive_end..).unwrap_or("");
    let line = rest.split_once('\n').map(|(line, _)| line).unwrap_or(rest);
    line.trim().is_empty()
}

fn dispatch_alias_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$").unwrap())
}

/// Return true when *prompt* already carries an active `%id` identity.
///
/// Fenced, disabled, and inline-literal occurrences stay inert. Remote
/// admission uses this so gateway `name` and prompt identity never both
/// own the same launch.
pub fn prompt_has_identity_directive(prompt: &str) -> bool {
    if !prompt.contains('%') {
        return false;
    }
    let ignored = typed_directive_ignored_ranges(prompt);
    directive_occurrences(prompt)
        .unwrap_or_default()
        .iter()
        .any(|directive| {
            directive.canonical_name == "id"
                && !position_in_ranges(directive.start, &ignored)
        })
}

fn project_ref_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r#"(?m)(^|[\s\(\[\{"'])(#(?:gh|git):[A-Za-z0-9_.~,+/@-]+)"#)
            .unwrap()
    })
}

pub(crate) fn render_launch_approval_preview(
    launch_kind: &str,
    selected_project: Option<&str>,
    units: &[LaunchUnitWire],
) -> Vec<String> {
    let mut lines = Vec::new();
    lines.push(format!(
        "LaunchPlan v{} kind={} units={} project={}",
        LAUNCH_PLAN_WIRE_SCHEMA_VERSION,
        launch_kind,
        units.len(),
        selected_project.unwrap_or("none")
    ));
    for unit in units {
        let waits = if unit.waits.is_empty() {
            "none".to_string()
        } else {
            unit.waits
                .iter()
                .map(wait_preview)
                .collect::<Vec<_>>()
                .join(",")
        };
        let condition = unit
            .condition
            .as_ref()
            .map(|condition| {
                format!(
                    " if={}:{}",
                    condition.code.language, condition.code.digest
                )
            })
            .unwrap_or_default();
        match &unit.payload {
            LaunchUnitPayloadWire::Agent(agent) => lines.push(format!(
                "{} agent identity={} model={} workspace={} machine={} waits={}{}{} prompt={:?}",
                unit.logical_id,
                agent
                    .effective_identity()
                    .as_deref()
                    .unwrap_or("auto"),
                agent.model.as_deref().unwrap_or("default"),
                agent.workspace_reference.as_deref().unwrap_or("none"),
                agent.dispatch_target.as_deref().unwrap_or("local"),
                waits,
                condition,
                hold_preview(agent.hold.as_ref()),
                agent.prompt
            )),
            LaunchUnitPayloadWire::Proc(proc_unit) => lines.push(format!(
                "{} proc shell={} project={} workspace={}{} waits={}{}{} code={}:{} preview={:?}",
                unit.logical_id,
                proc_unit.shell_name.as_deref().unwrap_or("auto"),
                proc_unit.selected_project.as_deref().unwrap_or("none"),
                proc_unit.workspace,
                proc_queue_preview(proc_unit)
                    .map(|queue| format!(" queue={queue}"))
                    .unwrap_or_default(),
                waits,
                condition,
                hold_preview(proc_unit.hold.as_ref()),
                proc_unit.code.language,
                proc_unit.code.digest,
                proc_unit.code.preview
            )),
        }
    }
    lines
}

fn hold_preview(hold: Option<&HoldFieldsWire>) -> String {
    hold.and_then(format_hold_directive)
        .map(|directive| format!(" hold={directive}"))
        .unwrap_or_default()
}

fn proc_queue_preview(proc_unit: &ProcUnitWire) -> Option<String> {
    if !proc_unit.has_authored_queue_fields() {
        return None;
    }
    let mut parts = Vec::new();
    if let Some(capacity) = proc_unit.queue_capacity {
        parts.push(format!("capacity={capacity}"));
    } else if let Some(formatted) = proc_unit
        .queue_capacity_multiplier
        .and_then(format_queue_capacity_multiplier)
    {
        parts.push(format!("capacity={formatted}"));
    }
    if let Some(priority) = proc_unit.wait_priority {
        parts.push(format!("priority={priority}"));
    }
    if let Some(weight) = proc_unit.queue_weight {
        let suffix = if proc_unit.queue_weight_explicit {
            ""
        } else {
            " implicit"
        };
        parts.push(format!("weight={}{}", format_queue_weight(weight), suffix));
    }
    if parts.is_empty() {
        None
    } else {
        Some(format!("({})", parts.join(", ")))
    }
}

fn wait_preview(wait: &WaitTargetWire) -> String {
    match wait {
        WaitTargetWire::Logical { logical_id, .. } => {
            format!("unit:{logical_id}")
        }
        WaitTargetWire::Agent { name } => format!("agent:{name}"),
        WaitTargetWire::Proc { identifier } => format!("proc:{identifier}"),
        WaitTargetWire::Bead { bead_id } => format!("bead:{bead_id}"),
        WaitTargetWire::Time { value } => format!("time:{value}"),
    }
}

pub(crate) fn launch_plan_content_digest(
    launch_kind: &str,
    selected_project: Option<&str>,
    units: &[LaunchUnitWire],
) -> String {
    let value = serde_json::json!({
        "schema_version": LAUNCH_PLAN_WIRE_SCHEMA_VERSION,
        "launch_kind": launch_kind,
        "selected_project": selected_project,
        "units": units,
    });
    hex::encode(Sha256::digest(value.to_string().as_bytes()))
}

pub(crate) fn typed_plan_diagnostic(
    code: &str,
    message: &str,
    source_span: Option<[usize; 2]>,
) -> LaunchPlanDiagnosticWire {
    LaunchPlanDiagnosticWire {
        code: code.to_string(),
        severity: "error".to_string(),
        message: message.to_string(),
        source_span,
        logical_id: None,
    }
}

pub(crate) fn typed_unit_diagnostic(
    code: &str,
    message: &str,
    logical_id: &str,
    source_span: Option<[usize; 2]>,
) -> LaunchPlanDiagnosticWire {
    LaunchPlanDiagnosticWire {
        code: code.to_string(),
        severity: "error".to_string(),
        message: message.to_string(),
        source_span,
        logical_id: Some(logical_id.to_string()),
    }
}

pub(crate) fn with_logical_id(
    mut diagnostic: LaunchPlanDiagnosticWire,
    logical_id: &str,
) -> LaunchPlanDiagnosticWire {
    diagnostic.logical_id = Some(logical_id.to_string());
    diagnostic
}
