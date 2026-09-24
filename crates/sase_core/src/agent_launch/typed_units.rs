//! Typed launch-unit classification: parse `%proc`, `%agent`, `%wait`,
//! queue, and hold directives in a prompt into raw units and plan them.
use super::directive_scan::{
    directive_occurrences, find_matching_paren,
    parse_directive_args_with_names, position_in_ranges,
    split_named_directive_arg, unquote_directive_arg_value,
    DirectiveOccurrence,
};
use super::fanout::plan_agent_launch_fanout;
use super::identity::{
    apply_parsed_identity, parse_clan_directive, parse_id_directive,
    validate_typed_unit_identities, ParsedClanDirective, ParsedIdDirective,
};
use super::plan_resolution::{
    launch_plan_content_digest, line_after_directive_is_blank,
    parse_dispatch_target, parse_proc_workspace, project_context_from_prompt,
    project_ref_captures, render_launch_approval_preview, resolve_typed_waits,
    strip_prompt_regions, typed_directive_ignored_ranges,
    typed_plan_diagnostic, typed_unit_diagnostic,
    validate_dispatch_combinations, validate_proc_project_policy,
    validate_proc_shell_name, validate_typed_wait_cycles, with_logical_id,
    DispatchCombinationFacts,
};
use super::wires::{
    AgentLaunchFanoutPlanError, AgentUnitWire, LaunchConditionWire,
    LaunchFanoutSlotWire, LaunchPlanDiagnosticWire, LaunchPlanWire,
    LaunchUnitPayloadWire, LaunchUnitWire, ProcUnitWire,
    LAUNCH_PLAN_WIRE_SCHEMA_VERSION,
};
use crate::effort::split_model_effort;
use crate::fenced_code::{
    language_from_info_string, scan_directive_owned_fences, CodeLanguage,
    CodeValue, CodeValueWire,
};
use crate::hold_directive::{
    collect_hold_fields_with_flags, HoldArgWire, HoldFieldsWire,
    HoldOccurrenceWire,
};
use crate::queue_directive::{
    collect_queue_fields_with_flags, QueueArgWire, QueueFieldsWire,
    QueueOccurrenceWire,
};
use std::collections::{BTreeMap, BTreeSet};

pub fn plan_typed_launch_units(
    prompt: &str,
    launch_kind: Option<&str>,
    selected_project: Option<&str>,
) -> Result<LaunchPlanWire, AgentLaunchFanoutPlanError> {
    plan_typed_launch_units_with_flags(
        prompt,
        launch_kind,
        selected_project,
        &[],
    )
}

pub fn plan_typed_launch_units_with_flags(
    prompt: &str,
    launch_kind: Option<&str>,
    selected_project: Option<&str>,
    _enabled_feature_flags: &[String],
) -> Result<LaunchPlanWire, AgentLaunchFanoutPlanError> {
    let fanout = plan_agent_launch_fanout(prompt, launch_kind)?;
    let plan_project = selected_project
        .map(str::to_string)
        .or_else(|| project_context_from_prompt(prompt));
    let mut diagnostics = Vec::new();
    let mut raw_units = Vec::with_capacity(fanout.slots.len());

    for slot in &fanout.slots {
        raw_units.push(classify_typed_launch_unit(
            slot,
            plan_project.as_deref(),
            _enabled_feature_flags,
            &mut diagnostics,
        ));
    }
    validate_typed_unit_identities(&raw_units, &mut diagnostics);
    resolve_typed_waits(&mut raw_units, &mut diagnostics);
    validate_typed_wait_cycles(&raw_units, &mut diagnostics);

    if !diagnostics.is_empty() {
        return Err(AgentLaunchFanoutPlanError::TypedLaunchPlan {
            diagnostics,
        });
    }

    let units: Vec<LaunchUnitWire> =
        raw_units.into_iter().map(|raw| raw.unit).collect();
    let approval_preview = render_launch_approval_preview(
        &fanout.launch_kind,
        plan_project.as_deref(),
        &units,
    );
    let content_digest = launch_plan_content_digest(
        &fanout.launch_kind,
        plan_project.as_deref(),
        &units,
    );
    Ok(LaunchPlanWire {
        schema_version: LAUNCH_PLAN_WIRE_SCHEMA_VERSION,
        launch_kind: fanout.launch_kind,
        selected_project: plan_project,
        units,
        approval_preview,
        content_digest,
        diagnostics: Vec::new(),
    })
}

#[derive(Debug, Clone)]
pub(crate) struct RawLaunchUnit {
    pub(crate) unit: LaunchUnitWire,
    pub(crate) raw_waits: Vec<RawWaitTarget>,
}

#[derive(Debug, Clone)]
pub(crate) struct RawWaitTarget {
    pub(crate) target: RawWaitTargetKind,
    pub(crate) source: Option<String>,
    pub(crate) source_span: Option<[usize; 2]>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RawWaitTargetKind {
    Previous,
    Unit(String),
    Agent(String),
    Proc(String),
    Bead(String),
    Time(String),
}

#[derive(Debug, Clone)]
struct ParsedProcDirective {
    code: Option<CodeValueWire>,
    options: BTreeMap<String, String>,
}

fn classify_typed_launch_unit(
    slot: &LaunchFanoutSlotWire,
    selected_project: Option<&str>,
    enabled_feature_flags: &[String],
    diagnostics: &mut Vec<LaunchPlanDiagnosticWire>,
) -> RawLaunchUnit {
    let prompt = slot.prompt.as_str();
    let logical_id = format!("unit-{}", slot.slot_index + 1);
    let project_refs = project_ref_captures(prompt);
    let mut regions_to_remove: Vec<(usize, usize)> =
        project_refs.iter().map(|capture| capture.span).collect();
    let (workspace_provider, workspace_reference) = match project_refs.first() {
        Some(capture) => (
            Some(capture.provider.clone()),
            Some(capture.reference.clone()),
        ),
        None => (None, None),
    };
    let mut dispatch_target: Option<String> = None;
    let mut condition: Option<LaunchConditionWire> = None;
    let mut proc_code: Option<CodeValueWire> = None;
    let mut proc_options: BTreeMap<String, String> = BTreeMap::new();
    let mut raw_waits = Vec::new();
    let mut agent_identity = slot.repeat_name.clone();
    let mut agent_identity_explicit = agent_identity.is_some();
    let mut agent_identity_force_reuse = false;
    let mut agent_clan: Option<String> = None;
    let mut agent_clan_declared = false;
    let mut agent_clan_tribe: Option<String> = None;
    let mut agent_clan_summary: Option<String> = None;
    let mut agent_clan_summary_script: Option<String> = None;
    let mut agent_session_parent: Option<String> = None;
    let mut agent_session_suffix: Option<String> = None;
    let mut agent_tribe: Option<String> = None;
    let mut parsed_id: Option<ParsedIdDirective> = None;
    let mut parsed_clan: Option<ParsedClanDirective> = None;
    let mut agent_model = slot.model.clone();
    let mut agent_effort: Option<String> = None;
    let mut agent_bead_id = slot.bead_id.clone();
    let mut agent_hidden = false;
    let mut auto_enabled = false;
    let mut auto_mode: Option<String> = None;
    let mut finalizers = Vec::new();
    let mut wait_queue = QueueFieldsWire::default();
    let mut queue_occurrences = Vec::new();
    let mut hold_occurrences = Vec::new();
    let mut hold_fields: Option<HoldFieldsWire> = None;
    let mut saw_repeat_directive = false;
    let mut proc_forbidden_directives = Vec::new();

    let scan = scan_directive_owned_fences(prompt);
    let owned_spans: Vec<(usize, usize)> = scan
        .directives
        .iter()
        .map(|directive| (directive.span[0], directive.span[1]))
        .collect();
    for diagnostic in scan.diagnostics {
        diagnostics.push(LaunchPlanDiagnosticWire {
            code: diagnostic.code,
            severity: "error".to_string(),
            message: diagnostic.message,
            source_span: Some(diagnostic.span),
            logical_id: Some(logical_id.clone()),
        });
    }
    for directive in scan.directives {
        regions_to_remove.push((directive.span[0], directive.span[1]));
        match directive.name.as_str() {
            "if" => {
                if condition.is_some() {
                    diagnostics.push(typed_unit_diagnostic(
                        "duplicate-condition",
                        "Only one %if is allowed per launch unit.",
                        &logical_id,
                        Some(directive.span),
                    ));
                    continue;
                }
                if let Some(code) = directive.code {
                    condition = Some(LaunchConditionWire {
                        code,
                        cwd: None,
                        context_fields: vec![
                            "logical_unit".to_string(),
                            "selected_project".to_string(),
                            "safe_inputs".to_string(),
                            "waited_outcomes".to_string(),
                        ],
                    });
                }
            }
            "proc" => {
                if proc_code.is_some() {
                    diagnostics.push(typed_unit_diagnostic(
                        "duplicate-proc",
                        "Only one %proc is allowed per launch unit.",
                        &logical_id,
                        Some(directive.span),
                    ));
                    continue;
                }
                proc_code = directive.code;
            }
            _ => {}
        }
    }

    let ignored_ranges = typed_directive_ignored_ranges(prompt);
    for directive in directive_occurrences(prompt).unwrap_or_default() {
        if position_in_ranges(directive.start, &ignored_ranges) {
            continue;
        }
        // `%if::` and bare `%proc::` are owned by the fence scanner. Re-parsing
        // them here would emit false diagnostics after the scanner already
        // captured the code. `%proc(...)::` still needs this loop so
        // parenthesized options survive.
        if directive.canonical_name == "if"
            && position_in_ranges(directive.start, &owned_spans)
        {
            continue;
        }
        if directive.canonical_name == "proc"
            && directive.is_bare
            && position_in_ranges(directive.start, &owned_spans)
        {
            continue;
        }
        let span = [directive.start, directive.end];
        match directive.canonical_name.as_str() {
            "proc" => {
                if directive.is_bare
                    && !prompt[directive.end..].starts_with("::")
                {
                    if line_after_directive_is_blank(prompt, directive.end) {
                        regions_to_remove
                            .push((directive.start, directive.end));
                        diagnostics.push(typed_unit_diagnostic(
                            "invalid-proc-form",
                            "%proc requires a body: %proc(\"cmd\"), %proc(bash=...|python=...), or %proc:: plus a fence.",
                            &logical_id,
                            Some(span),
                        ));
                    }
                    continue;
                }
                regions_to_remove.push((directive.start, directive.end));
                match parse_proc_directive(
                    prompt,
                    &directive,
                    proc_code.is_some(),
                ) {
                    Ok(parsed) => {
                        if let Some(code) = parsed.code {
                            if proc_code.is_some() {
                                diagnostics.push(typed_unit_diagnostic(
                                    "duplicate-proc-body",
                                    "%proc cannot combine a parenthesized body with a fenced body.",
                                    &logical_id,
                                    Some(span),
                                ));
                            } else {
                                proc_code = Some(code);
                            }
                        }
                        for (key, value) in parsed.options {
                            proc_options.insert(key, value);
                        }
                    }
                    Err(diagnostic) => {
                        diagnostics
                            .push(with_logical_id(diagnostic, &logical_id));
                    }
                }
            }
            "if" => {
                if directive.is_bare
                    && !prompt[directive.end..].starts_with("::")
                {
                    continue;
                }
                regions_to_remove.push((directive.start, directive.end));
                diagnostics.push(typed_unit_diagnostic(
                    "invalid-if-form",
                    "%if requires %if:: followed by exactly one closed bash or python fence.",
                    &logical_id,
                    Some(span),
                ));
            }
            "wait" => {
                regions_to_remove.push((directive.start, directive.end));
                parse_wait_directive(
                    prompt,
                    &directive,
                    &logical_id,
                    &mut raw_waits,
                    diagnostics,
                );
            }
            "queue" => {
                regions_to_remove.push((directive.start, directive.end));
                queue_occurrences
                    .push(queue_occurrence_from_directive(prompt, &directive));
            }
            "hold" => {
                regions_to_remove.push((directive.start, directive.end));
                hold_occurrences
                    .push(hold_occurrence_from_directive(prompt, &directive));
            }
            "id" => {
                regions_to_remove.push((directive.start, directive.end));
                if parsed_id.is_some() {
                    diagnostics.push(typed_unit_diagnostic(
                        "duplicate-id",
                        "Duplicate directive '%id' in prompt; use %id(<id>, tribe=<tribe>) to assign a tribe to an explicitly named agent, and add bead=<bead> to that same directive when needed.",
                        &logical_id,
                        Some(span),
                    ));
                } else {
                    let parsed = parse_id_directive(
                        &directive,
                        &logical_id,
                        diagnostics,
                    );
                    if parsed.unsupported_on_proc {
                        proc_forbidden_directives.push(
                            "%id(..., clan=|family=|session=|tribe=|bead=...)"
                                .to_string(),
                        );
                    }
                    parsed_id = Some(parsed);
                }
            }
            "model" => {
                regions_to_remove.push((directive.start, directive.end));
                proc_forbidden_directives.push("%model".to_string());
                if let Some(value) =
                    directive.args.first().filter(|arg| !arg.is_empty())
                {
                    let (model, effort) = split_model_effort(value);
                    agent_model = Some(model.to_string());
                    if let Some(effort) = effort {
                        agent_effort = Some(effort.to_string());
                    }
                }
            }
            "effort" => {
                regions_to_remove.push((directive.start, directive.end));
                proc_forbidden_directives.push("%effort".to_string());
                if let Some(value) =
                    directive.args.first().filter(|arg| !arg.is_empty())
                {
                    agent_effort = Some(value.clone());
                }
            }
            "auto" => {
                regions_to_remove.push((directive.start, directive.end));
                proc_forbidden_directives.push("%auto".to_string());
                auto_enabled = true;
                auto_mode = Some(
                    directive
                        .args
                        .first()
                        .filter(|arg| !arg.is_empty() && arg.as_str() != "true")
                        .cloned()
                        .unwrap_or_else(|| "plan".to_string()),
                );
            }
            "final" => {
                regions_to_remove.push((directive.start, directive.end));
                proc_forbidden_directives.push("%final".to_string());
                finalizers.extend(
                    directive
                        .args
                        .iter()
                        .filter(|arg| !arg.is_empty())
                        .cloned(),
                );
            }
            "clan" => {
                proc_forbidden_directives.push("%clan".to_string());
                if parsed_clan.is_some() {
                    regions_to_remove.push((directive.start, directive.end));
                    diagnostics.push(typed_unit_diagnostic(
                        "duplicate-clan",
                        "Duplicate directive '%clan' in prompt.",
                        &logical_id,
                        Some(span),
                    ));
                } else {
                    let parsed = parse_clan_directive(
                        prompt,
                        &directive,
                        &logical_id,
                        &ignored_ranges,
                        diagnostics,
                    );
                    regions_to_remove
                        .push((directive.start, parsed.region_end));
                    parsed_clan = Some(parsed);
                }
            }
            "hide" => {
                regions_to_remove.push((directive.start, directive.end));
                proc_forbidden_directives.push("%hide".to_string());
                agent_hidden = true;
            }
            "dispatch" => {
                regions_to_remove.push((directive.start, directive.end));
                if dispatch_target.is_some() {
                    diagnostics.push(typed_unit_diagnostic(
                        "duplicate-dispatch",
                        "Only one %dispatch directive is allowed per launch unit.",
                        &logical_id,
                        Some(span),
                    ));
                } else {
                    match parse_dispatch_target(&directive) {
                        Ok(target) => dispatch_target = Some(target),
                        Err(diagnostic) => diagnostics
                            .push(with_logical_id(diagnostic, &logical_id)),
                    }
                }
            }
            "repeat" => {
                regions_to_remove.push((directive.start, directive.end));
                saw_repeat_directive = true;
            }
            _ => {}
        }
    }

    if !queue_occurrences.is_empty() {
        let collected = collect_queue_fields_with_flags(
            &queue_occurrences,
            enabled_feature_flags,
        );
        for error in collected.errors {
            diagnostics.push(typed_unit_diagnostic(
                &error.code,
                &error.message,
                &logical_id,
                error.source_span,
            ));
        }
        if let Some(fields) = collected.fields {
            wait_queue = fields;
        }
    }

    if !hold_occurrences.is_empty() {
        let collected = collect_hold_fields_with_flags(
            &hold_occurrences,
            enabled_feature_flags,
        );
        for error in collected.errors {
            diagnostics.push(typed_unit_diagnostic(
                &error.code,
                &error.message,
                &logical_id,
                error.source_span,
            ));
        }
        hold_fields = collected.fields;
    }

    apply_parsed_identity(
        parsed_id.as_ref(),
        parsed_clan.as_ref(),
        &logical_id,
        &mut agent_identity,
        &mut agent_identity_explicit,
        &mut agent_identity_force_reuse,
        &mut agent_clan,
        &mut agent_clan_declared,
        &mut agent_clan_tribe,
        &mut agent_clan_summary,
        &mut agent_clan_summary_script,
        &mut agent_session_parent,
        &mut agent_session_suffix,
        &mut agent_tribe,
        &mut agent_bead_id,
        diagnostics,
    );

    if dispatch_target.is_some() {
        validate_dispatch_combinations(
            &logical_id,
            DispatchCombinationFacts {
                is_proc: proc_code.is_some(),
                has_waits: !raw_waits.is_empty(),
                has_queue: !queue_occurrences.is_empty(),
                has_hold: hold_fields.is_some(),
                has_clan: parsed_clan.is_some() || agent_clan.is_some(),
                has_agent_session: agent_session_parent.is_some(),
            },
            diagnostics,
        );
    }

    if hold_fields.is_some()
        && (slot.launch_kind == "repeat" || saw_repeat_directive)
    {
        diagnostics.push(typed_unit_diagnostic(
            "hold-with-repeat",
            "%hold cannot be combined with %repeat; use `sase agent hold run` for repeated work under a hold.",
            &logical_id,
            None,
        ));
    }

    let cleaned_prompt = strip_prompt_regions(prompt, &regions_to_remove)
        .trim()
        .to_string();
    let unit_project = selected_project
        .map(str::to_string)
        .or_else(|| project_context_from_prompt(prompt));
    let payload = if let Some(code) = proc_code {
        if !cleaned_prompt.is_empty() {
            diagnostics.push(typed_unit_diagnostic(
                "proc-residual-prompt",
                "%proc launch units cannot include residual prompt prose; put launch text in an agent unit.",
                &logical_id,
                None,
            ));
        }
        if !proc_forbidden_directives.is_empty() {
            proc_forbidden_directives.sort();
            proc_forbidden_directives.dedup();
            diagnostics.push(typed_unit_diagnostic(
                "agent-directive-on-proc",
                &format!(
                    "{} {} not valid on %proc launch units.",
                    proc_forbidden_directives.join(", "),
                    if proc_forbidden_directives.len() == 1 {
                        "is"
                    } else {
                        "are"
                    }
                ),
                &logical_id,
                None,
            ));
        }
        let shell_name = agent_identity.clone();
        validate_hold_self(
            hold_fields.as_ref(),
            &logical_id,
            shell_name.as_deref(),
            agent_identity_explicit,
            None,
            None,
            diagnostics,
        );
        validate_proc_shell_name(
            shell_name.as_deref(),
            &logical_id,
            diagnostics,
        );
        let workspace = parse_proc_workspace(
            proc_options.get("workspace").map(String::as_str),
            unit_project.as_deref(),
            &logical_id,
            diagnostics,
        );
        validate_proc_project_policy(
            unit_project.as_deref(),
            workspace,
            proc_options.get("cwd").map(String::as_str),
            &logical_id,
            diagnostics,
        );
        let proc_queue_weight = wait_queue.weight.or_else(|| {
            (wait_queue.queue_capacity.is_some()
                || wait_queue.priority.is_some())
            .then_some(0.0)
        });
        LaunchUnitPayloadWire::Proc(ProcUnitWire {
            code,
            shell_name,
            label: proc_options.get("label").cloned().filter(|v| !v.is_empty()),
            timeout: proc_options
                .get("timeout")
                .cloned()
                .filter(|v| !v.is_empty()),
            idle_timeout: proc_options
                .get("idle_timeout")
                .cloned()
                .filter(|v| !v.is_empty()),
            cwd: proc_options.get("cwd").cloned().filter(|v| !v.is_empty()),
            workspace,
            workspace_explicit: proc_options.contains_key("workspace"),
            selected_project: unit_project,
            queue_capacity: wait_queue.queue_capacity,
            wait_priority: wait_queue.priority,
            queue_weight: proc_queue_weight,
            queue_weight_explicit: wait_queue.weight.is_some(),
            hold: hold_fields.clone(),
        })
    } else {
        validate_hold_self(
            hold_fields.as_ref(),
            &logical_id,
            agent_identity.as_deref(),
            agent_identity_explicit,
            agent_session_parent.as_deref(),
            agent_clan.as_deref(),
            diagnostics,
        );
        LaunchUnitPayloadWire::Agent(AgentUnitWire {
            prompt: cleaned_prompt,
            identity: agent_identity,
            identity_explicit: agent_identity_explicit,
            identity_force_reuse: agent_identity_force_reuse,
            clan: agent_clan,
            clan_declared: agent_clan_declared,
            clan_tribe: agent_clan_tribe,
            clan_summary: agent_clan_summary,
            clan_summary_script: agent_clan_summary_script,
            agent_session_attach_parent: agent_session_parent,
            agent_session_attach_suffix: agent_session_suffix,
            tribe: agent_tribe,
            model: agent_model,
            reasoning_effort: agent_effort,
            bead_id: agent_bead_id,
            hidden: agent_hidden,
            auto_enabled,
            auto_mode,
            finalizers,
            queue_capacity: wait_queue.queue_capacity,
            wait_runners: None,
            wait_priority: wait_queue.priority,
            queue_weight: wait_queue.weight,
            queue_weight_explicit: wait_queue.weight.is_some(),
            workspace_provider,
            workspace_reference,
            dispatch_target,
            hold: hold_fields,
        })
    };

    RawLaunchUnit {
        unit: LaunchUnitWire {
            logical_id,
            source_order: slot.slot_index,
            waits: Vec::new(),
            condition,
            payload,
        },
        raw_waits,
    }
}

fn parse_proc_directive(
    prompt: &str,
    directive: &DirectiveOccurrence,
    fenced_body_present: bool,
) -> Result<ParsedProcDirective, LaunchPlanDiagnosticWire> {
    let source = &prompt[directive.start..directive.end];
    let Some(open_rel) = source.find('(') else {
        return Err(typed_plan_diagnostic(
            "invalid-proc-form",
            "%proc requires a body: %proc(\"cmd\"), %proc(bash=...|python=...), or %proc:: plus a fence.",
            Some([directive.start, directive.end]),
        ));
    };
    let open = directive.start + open_rel;
    let Some(close) = find_matching_paren(prompt, open) else {
        return Err(typed_plan_diagnostic(
            "malformed-proc",
            "Malformed %proc(...) directive: missing closing ')'.",
            Some([directive.start, directive.end]),
        ));
    };
    let args = parse_directive_args_with_names(&prompt[open + 1..close], ',');
    let allowed: BTreeSet<&str> = [
        "bash",
        "python",
        "timeout",
        "idle_timeout",
        "cwd",
        "workspace",
        "label",
    ]
    .into_iter()
    .collect();
    let mut positional_body: Option<String> = None;
    let mut named_body: Option<(String, String)> = None;
    let mut options = BTreeMap::new();
    for arg in args {
        match arg.name.as_deref() {
            None => {
                if !arg.value.is_empty() {
                    if positional_body.is_some() {
                        return Err(typed_plan_diagnostic(
                            "duplicate-proc-body",
                            "%proc accepts exactly one body.",
                            Some([directive.start, directive.end]),
                        ));
                    }
                    positional_body = Some(arg.value);
                }
            }
            Some("bash") | Some("python") => {
                if named_body.is_some() {
                    return Err(typed_plan_diagnostic(
                        "duplicate-proc-body",
                        "%proc cannot combine bash= and python=.",
                        Some([directive.start, directive.end]),
                    ));
                }
                named_body = Some((arg.name.clone().unwrap(), arg.value));
            }
            Some(key) if allowed.contains(key) => {
                options.insert(key.to_string(), arg.value);
            }
            Some(key) => {
                return Err(typed_plan_diagnostic(
                    "unknown-proc-option",
                    &format!(
                        "Unsupported keyword on %proc: {key}=. Only bash=, python=, timeout=, idle_timeout=, cwd=, workspace=, and label= are supported."
                    ),
                    Some([directive.start, directive.end]),
                ));
            }
        }
    }
    if positional_body.is_some() && named_body.is_some() {
        return Err(typed_plan_diagnostic(
            "duplicate-proc-body",
            "%proc cannot combine a positional body with bash= or python=.",
            Some([directive.start, directive.end]),
        ));
    }
    let code = match (positional_body, named_body) {
        (Some(source), None) => Some(make_proc_code_value(source, CodeLanguage::Bash)?),
        (None, Some((language, source))) => {
            Some(make_proc_code_value(source, parse_code_language(&language)?)?)
        }
        (None, None) if fenced_body_present => None,
        (None, None) => {
            return Err(typed_plan_diagnostic(
                "missing-proc-body",
                "%proc requires a body: %proc(\"cmd\"), %proc(bash=...|python=...), or %proc:: plus a fence.",
                Some([directive.start, directive.end]),
            ))
        }
        _ => unreachable!("duplicate combinations are validated above"),
    };
    Ok(ParsedProcDirective { code, options })
}

fn make_proc_code_value(
    source: String,
    language: CodeLanguage,
) -> Result<CodeValueWire, LaunchPlanDiagnosticWire> {
    if source.trim().is_empty() {
        return Err(typed_plan_diagnostic(
            "empty-proc-body",
            "%proc requires a non-empty body.",
            None,
        ));
    }
    Ok(CodeValue {
        source,
        language,
        info_string: None,
    }
    .to_wire())
}

fn parse_code_language(
    value: &str,
) -> Result<CodeLanguage, LaunchPlanDiagnosticWire> {
    language_from_info_string(Some(value)).map_err(|message| {
        typed_plan_diagnostic("unknown-code-language", &message, None)
    })
}

fn queue_occurrence_from_directive(
    prompt: &str,
    directive: &DirectiveOccurrence,
) -> QueueOccurrenceWire {
    let args = directive
        .args
        .iter()
        .map(|arg| {
            let (name, value_raw) = split_named_directive_arg(arg);
            QueueArgWire {
                name,
                value: unquote_directive_arg_value(value_raw.trim()),
            }
        })
        .filter(|arg| arg.name.is_some() || !arg.value.is_empty())
        .collect();
    QueueOccurrenceWire {
        source: prompt[directive.start..directive.end].to_string(),
        source_span: [directive.start, directive.end],
        args,
        has_plus_suffix: directive.has_plus_suffix,
    }
}

fn hold_occurrence_from_directive(
    prompt: &str,
    directive: &DirectiveOccurrence,
) -> HoldOccurrenceWire {
    let args = directive
        .args
        .iter()
        .map(|arg| {
            let (name, value_raw) = split_named_directive_arg(arg);
            HoldArgWire {
                name,
                value: unquote_directive_arg_value(value_raw.trim()),
            }
        })
        .filter(|arg| arg.name.is_some() || !arg.value.is_empty())
        .collect();
    HoldOccurrenceWire {
        source: prompt[directive.start..directive.end].to_string(),
        source_span: [directive.start, directive.end],
        args,
        has_plus_suffix: directive.has_plus_suffix,
    }
}

fn validate_hold_self(
    hold: Option<&HoldFieldsWire>,
    logical_id: &str,
    identity: Option<&str>,
    identity_explicit: bool,
    agent_session: Option<&str>,
    clan: Option<&str>,
    diagnostics: &mut Vec<LaunchPlanDiagnosticWire>,
) {
    let Some(hold) = hold else {
        return;
    };
    let mut own = BTreeSet::new();
    if identity_explicit {
        if let Some(identity) = identity {
            own.insert(identity.to_string());
            if let Ok(parsed) =
                crate::agent_identity::parse_agent_session_name(identity)
            {
                own.insert(parsed.agent_session_name);
            }
        }
    }
    if let Some(agent_session) = agent_session {
        own.insert(agent_session.to_string());
    }
    if let Some(clan) = clan {
        own.insert(clan.to_string());
    }
    if own.is_empty() {
        return;
    }
    if let Some(name) = hold.names.iter().find(|name| own.contains(*name)) {
        diagnostics.push(typed_unit_diagnostic(
            "hold-self",
            &format!(
                "%hold target {name:?} matches this launch unit's own identity, agent session, or clan."
            ),
            logical_id,
            None,
        ));
    }
}

fn parse_wait_directive(
    prompt: &str,
    directive: &DirectiveOccurrence,
    logical_id: &str,
    raw_waits: &mut Vec<RawWaitTarget>,
    diagnostics: &mut Vec<LaunchPlanDiagnosticWire>,
) {
    let span = [directive.start, directive.end];
    let source = Some(prompt[directive.start..directive.end].to_string());
    let mut args = Vec::new();
    for arg in &directive.args {
        if arg.contains(',') && !arg.contains('=') {
            args.extend(
                arg.split(',')
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(str::to_string),
            );
        } else {
            args.push(arg.clone());
        }
    }
    if args.is_empty() || args.iter().all(|arg| arg.is_empty()) {
        raw_waits.push(RawWaitTarget {
            target: RawWaitTargetKind::Previous,
            source,
            source_span: Some(span),
        });
        return;
    }
    for arg in args {
        let (name, value_raw) = split_named_directive_arg(&arg);
        let value = unquote_directive_arg_value(value_raw.trim());
        match name.as_deref() {
            Some("unit") => raw_waits.push(raw_wait("unit", value, span, source.clone())),
            Some("agent") => raw_waits.push(raw_wait("agent", value, span, source.clone())),
            Some("proc") => raw_waits.push(raw_wait("proc", value, span, source.clone())),
            Some("bead") => raw_waits.push(raw_wait("bead", value, span, source.clone())),
            Some("time") => raw_waits.push(raw_wait("time", value, span, source.clone())),
            Some("runners") => diagnostics.push(typed_unit_diagnostic(
                "wait-queue-runners-moved",
                "%wait(runners=...) has moved to %queue. Use %queue(capacity=N) or %q:N, and keep dependencies on %wait.",
                logical_id,
                Some(span),
            )),
            Some("capacity") => diagnostics.push(typed_unit_diagnostic(
                "wait-queue-capacity-moved",
                "%wait(capacity=...) belongs on %queue. Use %queue(capacity=N) or %q:N, and keep dependencies on %wait.",
                logical_id,
                Some(span),
            )),
            Some("priority") => diagnostics.push(typed_unit_diagnostic(
                "wait-queue-priority-moved",
                "%wait(priority=...) has moved to %queue. Use %queue(priority=N) or %q(p=N), and keep dependencies on %wait.",
                logical_id,
                Some(span),
            )),
            Some("p") => diagnostics.push(typed_unit_diagnostic(
                "wait-queue-p-unsupported",
                "%wait(p=...) is unsupported. Use %queue(priority=...) or %q(p=...).",
                logical_id,
                Some(span),
            )),
            Some("weight") => diagnostics.push(typed_unit_diagnostic(
                "wait-queue-weight-moved",
                "%wait(weight=...) has moved to %queue. Use %queue(weight=W) or %q(w=W), and keep dependencies on %wait.",
                logical_id,
                Some(span),
            )),
            Some("w") => diagnostics.push(typed_unit_diagnostic(
                "wait-queue-w-unsupported",
                "%wait(w=...) is unsupported. Use %queue(weight=W) or %q(w=W).",
                logical_id,
                Some(span),
            )),
            Some(key) => diagnostics.push(typed_unit_diagnostic(
                "unknown-wait-target",
                &format!(
                    "Unsupported keyword on %wait: {key}=. Use unit=, agent=, proc=, bead=, or time=. Queue controls belong on %queue."
                ),
                logical_id,
                Some(span),
            )),
            None => raw_waits.push(RawWaitTarget {
                target: RawWaitTargetKind::Agent(value),
                source: source.clone(),
                source_span: Some(span),
            }),
        }
    }
}

fn raw_wait(
    kind: &str,
    value: String,
    span: [usize; 2],
    source: Option<String>,
) -> RawWaitTarget {
    let target = match kind {
        "unit" => RawWaitTargetKind::Unit(value),
        "agent" => RawWaitTargetKind::Agent(value),
        "proc" => RawWaitTargetKind::Proc(value),
        "bead" => RawWaitTargetKind::Bead(value),
        "time" => RawWaitTargetKind::Time(value),
        _ => RawWaitTargetKind::Agent(value),
    };
    RawWaitTarget {
        target,
        source,
        source_span: Some(span),
    }
}
