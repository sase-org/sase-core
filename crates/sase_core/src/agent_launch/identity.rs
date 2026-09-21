//! Agent identity directives: `%id` / `%clan` parsing plus per-unit
//! identity validation for typed launch plans.
use super::directive_scan::{
    position_in_ranges, split_named_directive_arg, unquote_directive_arg_value,
    DirectiveOccurrence,
};
use super::plan_resolution::typed_unit_diagnostic;
use super::typed_units::RawLaunchUnit;
use super::wires::{LaunchPlanDiagnosticWire, LaunchUnitPayloadWire};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Default)]
pub(crate) struct ParsedIdDirective {
    pub(crate) identity: Option<String>,
    pub(crate) bead_id: Option<String>,
    pub(crate) clan: Option<String>,
    pub(crate) tribe: Option<String>,
    pub(crate) family_parent: Option<String>,
    pub(crate) family_suffix: Option<String>,
    pub(crate) force_reuse: bool,
    pub(crate) unsupported_on_proc: bool,
}

#[derive(Debug, Default)]
pub(crate) struct ParsedClanDirective {
    pub(crate) clan: Option<String>,
    pub(crate) tribe: Option<String>,
    pub(crate) summary: Option<String>,
    pub(crate) summary_script: Option<String>,
    pub(crate) region_end: usize,
}

pub(crate) fn parse_id_directive(
    directive: &DirectiveOccurrence,
    logical_id: &str,
    diagnostics: &mut Vec<LaunchPlanDiagnosticWire>,
) -> ParsedIdDirective {
    let mut parsed = ParsedIdDirective::default();
    let span = [directive.start, directive.end];
    if let Some(keys) = duplicate_named_args(&directive.args) {
        diagnostics.push(typed_unit_diagnostic(
            "duplicate-id-keyword",
            &format!("Duplicate keyword argument '{keys}' on %id."),
            logical_id,
            Some(span),
        ));
        parsed.unsupported_on_proc = true;
        return parsed;
    }

    let mut positional: Vec<String> = Vec::new();
    let mut named: BTreeMap<String, String> = BTreeMap::new();
    for (index, arg) in directive.args.iter().enumerate() {
        let (name, value_raw) = split_named_directive_arg(arg);
        let value = unquote_directive_arg_value(value_raw.trim());
        match name.as_deref() {
            Some("bead") | Some("clan") | Some("family") | Some("tribe") => {
                parsed.unsupported_on_proc = true;
                named.insert(name.unwrap(), value);
            }
            Some(other) => {
                parsed.unsupported_on_proc = true;
                diagnostics.push(typed_unit_diagnostic(
                    "invalid-id-keyword",
                    &format!(
                        "Unsupported keyword on %id: {other}=. Only bead=, clan=, family=, and tribe= are supported."
                    ),
                    logical_id,
                    Some(span),
                ));
            }
            None if index == 0 || !value.is_empty() => positional.push(value),
            None => {}
        }
    }

    let membership: Vec<&str> = ["clan", "family", "tribe"]
        .into_iter()
        .filter(|key| named.contains_key(*key))
        .collect();
    if membership.len() > 1 {
        diagnostics.push(typed_unit_diagnostic(
            "id-keyword-conflict",
            "The clan=, family=, and tribe= keywords on %id are mutually exclusive; set at most one.",
            logical_id,
            Some(span),
        ));
        return parsed;
    }
    if positional.len() > 1 {
        diagnostics.push(typed_unit_diagnostic(
            "invalid-id-form",
            "The positional family form on %id is no longer supported; use %id(<suffix>, family=<parent>) instead.",
            logical_id,
            Some(span),
        ));
        return parsed;
    }

    if let Some(bead_id) = named.get("bead") {
        if bead_id.is_empty() || bead_id.chars().any(char::is_whitespace) {
            diagnostics.push(typed_unit_diagnostic(
                "invalid-id-bead",
                "The bead= keyword on %id requires a non-empty, whitespace-free bead ID.",
                logical_id,
                Some(span),
            ));
        } else {
            parsed.bead_id = Some(bead_id.clone());
        }
    }

    if let Some(clan) = named.get("clan") {
        if positional.len() != 1 {
            diagnostics.push(typed_unit_diagnostic(
                "invalid-id-clan",
                "The clan= keyword on %id requires exactly one positional member id, e.g. %id(worker, clan=research).",
                logical_id,
                Some(span),
            ));
            return parsed;
        }
        let (force_reuse, member_id) = strip_force_reuse(&positional[0]);
        if member_id.is_empty() {
            diagnostics.push(typed_unit_diagnostic(
                "invalid-id-clan",
                "The clan= keyword on %id requires a non-empty member id.",
                logical_id,
                Some(span),
            ));
            return parsed;
        }
        if clan.trim().is_empty() {
            diagnostics.push(typed_unit_diagnostic(
                "invalid-id-clan",
                "The clan= keyword on %id requires a non-empty clan name.",
                logical_id,
                Some(span),
            ));
            return parsed;
        }
        parsed.identity = Some(member_id);
        parsed.clan = Some(clan.clone());
        parsed.force_reuse = force_reuse;
        return parsed;
    }

    if let Some(family) = named.get("family") {
        if positional.len() != 1 {
            diagnostics.push(typed_unit_diagnostic(
                "invalid-id-family",
                "The family= keyword on %id requires exactly one positional suffix; use %id(<suffix>, family=<family>) or %id(@, family=<family>).",
                logical_id,
                Some(span),
            ));
            return parsed;
        }
        let (force_reuse, suffix) = strip_force_reuse(&positional[0]);
        let parent = family.trim();
        if parent.is_empty() {
            diagnostics.push(typed_unit_diagnostic(
                "invalid-id-family",
                "The family= keyword on %id requires a non-empty family name.",
                logical_id,
                Some(span),
            ));
            return parsed;
        }
        if suffix.is_empty() {
            diagnostics.push(typed_unit_diagnostic(
                "invalid-id-family",
                "The family= keyword on %id requires a non-empty suffix.",
                logical_id,
                Some(span),
            ));
            return parsed;
        }
        if let Some(message) = invalid_family_suffix_reason(&suffix) {
            diagnostics.push(typed_unit_diagnostic(
                "invalid-id-family",
                &message,
                logical_id,
                Some(span),
            ));
            return parsed;
        }
        parsed.family_parent = Some(parent.to_string());
        parsed.family_suffix = Some(suffix);
        parsed.force_reuse = force_reuse;
        return parsed;
    }

    if let Some(tribe) = named.get("tribe") {
        let raw = positional.first().cloned().unwrap_or_default();
        let (force_reuse, identity) = strip_force_reuse(&raw);
        if !positional.is_empty() && identity.is_empty() {
            diagnostics.push(typed_unit_diagnostic(
                "invalid-id-tribe",
                "The tribe= keyword on %id requires a non-empty id when a positional id is supplied.",
                logical_id,
                Some(span),
            ));
            return parsed;
        }
        if tribe.trim().is_empty() {
            diagnostics.push(typed_unit_diagnostic(
                "invalid-id-tribe",
                "The tribe= keyword on %id requires a non-empty tribe name.",
                logical_id,
                Some(span),
            ));
            return parsed;
        }
        if let Some(message) = invalid_tribe_reason(tribe, "%id") {
            diagnostics.push(typed_unit_diagnostic(
                "invalid-id-tribe",
                &message,
                logical_id,
                Some(span),
            ));
            return parsed;
        }
        parsed.identity = if identity.is_empty() {
            None
        } else {
            Some(identity)
        };
        parsed.tribe = Some(tribe.clone());
        parsed.force_reuse = force_reuse;
        return parsed;
    }

    if let Some(raw) = positional.first() {
        let (force_reuse, identity) = strip_force_reuse(raw);
        if !identity.is_empty() {
            parsed.identity = Some(identity);
            parsed.force_reuse = force_reuse;
        } else if force_reuse {
            diagnostics.push(typed_unit_diagnostic(
                "invalid-id-form",
                "The tribe= keyword on %id requires a non-empty id when a positional id is supplied.",
                logical_id,
                Some(span),
            ));
        }
    }
    parsed
}

pub(crate) fn parse_clan_directive(
    prompt: &str,
    directive: &DirectiveOccurrence,
    logical_id: &str,
    ignored_ranges: &[(usize, usize)],
    diagnostics: &mut Vec<LaunchPlanDiagnosticWire>,
) -> ParsedClanDirective {
    let mut parsed = ParsedClanDirective {
        region_end: directive.end,
        ..ParsedClanDirective::default()
    };
    let span = [directive.start, directive.end];
    if directive.has_plus_suffix {
        diagnostics.push(typed_unit_diagnostic(
            "invalid-clan-form",
            "%clan does not support '+'; use %clan:<name> or %clan(<name>, tribe=<tribe>).",
            logical_id,
            Some(span),
        ));
        return parsed;
    }
    if let Some(keys) = duplicate_named_args(&directive.args) {
        diagnostics.push(typed_unit_diagnostic(
            "duplicate-clan-keyword",
            &format!("Duplicate keyword argument '{keys}' on %clan."),
            logical_id,
            Some(span),
        ));
        return parsed;
    }

    let mut positional: Vec<String> = Vec::new();
    let mut named: BTreeMap<String, (String, bool)> = BTreeMap::new();
    for (index, arg) in directive.args.iter().enumerate() {
        let (name, value_raw) = split_named_directive_arg(arg);
        let trimmed_raw = value_raw.trim();
        let from_text_block = trimmed_raw.starts_with("[[");
        let value = unquote_directive_arg_value(trimmed_raw);
        match name.as_deref() {
            Some("tribe") | Some("summary") | Some("summary_script") => {
                named.insert(name.unwrap(), (value, from_text_block));
            }
            Some(other) => {
                diagnostics.push(typed_unit_diagnostic(
                    "invalid-clan-keyword",
                    &format!(
                        "Unsupported keyword on %clan: {other}=. Only summary=, summary_script=, and tribe= are supported."
                    ),
                    logical_id,
                    Some(span),
                ));
            }
            None if index == 0 || !value.is_empty() => positional.push(value),
            None => {}
        }
    }
    if positional.len() > 1 {
        diagnostics.push(typed_unit_diagnostic(
            "invalid-clan-form",
            "%clan accepts exactly one positional clan name argument.",
            logical_id,
            Some(span),
        ));
        return parsed;
    }
    let clan = positional.first().cloned().unwrap_or_default();
    if clan.trim().is_empty() {
        diagnostics.push(typed_unit_diagnostic(
            "invalid-clan-form",
            "'%clan' directive requires a clan name argument (e.g., %clan:research.@).",
            logical_id,
            Some(span),
        ));
        return parsed;
    }
    parsed.clan = Some(clan);

    if named.contains_key("summary") && named.contains_key("summary_script") {
        diagnostics.push(typed_unit_diagnostic(
            "clan-summary-conflict",
            "'%clan' summary= and summary_script= are mutually exclusive.",
            logical_id,
            Some(span),
        ));
        return parsed;
    }
    if let Some((tribe, _)) = named.get("tribe") {
        if tribe.trim().is_empty() {
            diagnostics.push(typed_unit_diagnostic(
                "invalid-clan-tribe",
                "'%clan(..., tribe=...)' requires a non-empty tribe name.",
                logical_id,
                Some(span),
            ));
        } else if let Some(message) = invalid_tribe_reason(tribe, "%clan") {
            diagnostics.push(typed_unit_diagnostic(
                "invalid-clan-tribe",
                &message,
                logical_id,
                Some(span),
            ));
        } else {
            parsed.tribe = Some(tribe.clone());
        }
    }
    if let Some((summary, from_text_block)) = named.get("summary") {
        if summary.trim().is_empty() {
            diagnostics.push(typed_unit_diagnostic(
                "invalid-clan-summary",
                "'%clan(..., summary=...)' requires a non-empty value.",
                logical_id,
                Some(span),
            ));
        } else {
            parsed.summary =
                Some(normalize_clan_summary(summary, *from_text_block));
        }
    }
    if let Some((script, _)) = named.get("summary_script") {
        if script.trim().is_empty() {
            diagnostics.push(typed_unit_diagnostic(
                "invalid-clan-summary-script",
                "'%clan(..., summary_script=...)' requires a non-empty value.",
                logical_id,
                Some(span),
            ));
        } else {
            parsed.summary_script = Some(script.trim().to_string());
        }
    }

    if prompt[directive.end..].starts_with(":: ") {
        if parsed.summary.is_some() || parsed.summary_script.is_some() {
            diagnostics.push(typed_unit_diagnostic(
                "clan-shorthand-conflict",
                "Cannot combine %clan(...):: shorthand with explicit summary= or summary_script=.",
                logical_id,
                Some(span),
            ));
            return parsed;
        }
        let text_start = directive.end + 3;
        let text_end =
            clan_double_colon_text_end(prompt, text_start, ignored_ranges);
        let text = prompt[text_start..text_end].trim_end();
        if text.is_empty() {
            diagnostics.push(typed_unit_diagnostic(
                "invalid-clan-summary",
                "'%clan(..., summary=...)' requires a non-empty value.",
                logical_id,
                Some(span),
            ));
        } else {
            parsed.summary = Some(normalize_clan_summary(text, true));
            parsed.region_end = text_end;
        }
    }
    parsed
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn apply_parsed_identity(
    parsed_id: Option<&ParsedIdDirective>,
    parsed_clan: Option<&ParsedClanDirective>,
    logical_id: &str,
    agent_identity: &mut Option<String>,
    agent_identity_explicit: &mut bool,
    agent_identity_force_reuse: &mut bool,
    agent_clan: &mut Option<String>,
    agent_clan_declared: &mut bool,
    agent_clan_tribe: &mut Option<String>,
    agent_clan_summary: &mut Option<String>,
    agent_clan_summary_script: &mut Option<String>,
    agent_family_parent: &mut Option<String>,
    agent_family_suffix: &mut Option<String>,
    agent_tribe: &mut Option<String>,
    agent_bead_id: &mut Option<String>,
    diagnostics: &mut Vec<LaunchPlanDiagnosticWire>,
) {
    if let Some(id) = parsed_id {
        if id.family_parent.is_some() {
            *agent_identity = None;
            *agent_identity_explicit = false;
            *agent_family_parent = id.family_parent.clone();
            *agent_family_suffix = id.family_suffix.clone();
        } else if let Some(identity) = id.identity.as_ref() {
            *agent_identity = Some(identity.clone());
            *agent_identity_explicit = true;
        }
        if let Some(bead_id) = id.bead_id.as_ref() {
            *agent_bead_id = Some(bead_id.clone());
        }
        if let Some(clan) = id.clan.as_ref() {
            *agent_clan = Some(clan.clone());
        }
        *agent_tribe = id.tribe.clone();
        *agent_identity_force_reuse = id.force_reuse;
    }
    if let Some(clan) = parsed_clan {
        *agent_clan = clan.clan.clone();
        *agent_clan_declared = clan.clan.is_some();
        *agent_clan_tribe = clan.tribe.clone();
        *agent_clan_summary = clan.summary.clone();
        *agent_clan_summary_script = clan.summary_script.clone();
    }

    let join_clan = parsed_id.and_then(|id| id.clan.as_ref());
    let family = parsed_id.and_then(|id| id.family_parent.as_ref());
    let id_tribe = parsed_id.and_then(|id| id.tribe.as_ref());
    if parsed_clan.is_some() && join_clan.is_some() {
        diagnostics.push(typed_unit_diagnostic(
            "clan-id-conflict",
            "Cannot combine %clan with %id(..., clan=...); a declaring prompt uses %clan(<clan>, tribe=<tribe>) with a full %id:<clan>.<id>, while a joining prompt uses only %id(<id>, clan=<clan>).",
            logical_id,
            None,
        ));
    }
    if parsed_clan.is_some() && id_tribe.is_some() {
        diagnostics.push(typed_unit_diagnostic(
            "clan-id-conflict",
            "Cannot combine %clan with %id(..., tribe=...); use %clan(<clan>, tribe=<tribe>) to set the clan's tribe.",
            logical_id,
            None,
        ));
    }
    if parsed_clan.is_some() && family.is_some() {
        diagnostics.push(typed_unit_diagnostic(
            "clan-id-conflict",
            "Cannot combine %clan with %id(..., family=...); choose clan membership or serial family attachment.",
            logical_id,
            None,
        ));
    }
}

fn duplicate_named_args(args: &[String]) -> Option<String> {
    let mut seen = BTreeSet::new();
    let mut duplicates = BTreeSet::new();
    for arg in args {
        if let Some(name) = split_named_directive_arg(arg).0 {
            if !seen.insert(name.clone()) {
                duplicates.insert(name);
            }
        }
    }
    if duplicates.is_empty() {
        None
    } else {
        Some(duplicates.into_iter().collect::<Vec<_>>().join(", "))
    }
}

fn strip_force_reuse(raw: &str) -> (bool, String) {
    let trimmed = raw.trim();
    if let Some(rest) = trimmed.strip_prefix('!') {
        (true, rest.to_string())
    } else {
        (false, trimmed.to_string())
    }
}

fn invalid_family_suffix_reason(suffix: &str) -> Option<String> {
    if suffix == "@" {
        return None;
    }
    if suffix.starts_with('.')
        || suffix.starts_with('-')
        || suffix.contains("--")
    {
        return Some(format!(
            "Invalid %i family suffix '{suffix}'. Pass the bare suffix without a family separator, e.g. %i(reviewer, family=parent)."
        ));
    }
    if !suffix
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        return Some(format!(
            "Invalid %i family suffix '{suffix}'. Use letters, numbers, and underscores only, or @ to allocate the next free suffix."
        ));
    }
    None
}

fn invalid_tribe_reason(tribe: &str, directive: &str) -> Option<String> {
    if tribe.starts_with('@') {
        return Some(format!(
            "Invalid '{directive}' tribe= value: tribe name {tribe:?} must not start with '@' (the '@' is added on display only — drop it from the input)"
        ));
    }
    if !tribe
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '.' | '-'))
    {
        return Some(format!(
            "Invalid '{directive}' tribe= value: tribe name {tribe:?} must match ^[A-Za-z0-9_.-]+$ (letters, digits, underscore, dot, dash)"
        ));
    }
    None
}

fn normalize_clan_summary(raw: &str, from_text_block: bool) -> String {
    if !from_text_block {
        return raw.trim().to_string();
    }
    let lines: Vec<&str> = raw.split('\n').collect();
    if lines.is_empty() {
        return String::new();
    }
    let first = lines[0].trim_start();
    let continuation = &lines[1..];
    let min_indent = continuation
        .iter()
        .filter(|line| !line.trim().is_empty())
        .map(|line| line.len() - line.trim_start().len())
        .min()
        .unwrap_or(0);
    let mut out = vec![first.to_string()];
    for line in continuation {
        if line.trim().is_empty() {
            out.push(String::new());
        } else {
            out.push(line[min_indent.min(line.len())..].to_string());
        }
    }
    out.join("\n").trim().to_string()
}

fn clan_double_colon_text_end(
    prompt: &str,
    start: usize,
    ignored_ranges: &[(usize, usize)],
) -> usize {
    let bytes = prompt.as_bytes();
    let mut idx = start;
    while idx < bytes.len() {
        if bytes[idx] == b'\n' {
            let item_start = idx + 1;
            if item_start < bytes.len()
                && !position_in_ranges(item_start, ignored_ranges)
                && is_prompt_item_start(&prompt[item_start..])
            {
                return idx;
            }
        }
        idx += 1;
    }
    prompt.len()
}

fn is_prompt_item_start(text: &str) -> bool {
    let rest = match text.as_bytes().first() {
        Some(b'%') => &text[1..],
        Some(b'#') => &text[1..],
        _ => return false,
    };
    let mut chars = rest.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !first.is_ascii_alphabetic() && first != '_' {
        return false;
    }
    let mut consumed = first.len_utf8();
    for ch in chars {
        if ch.is_ascii_alphanumeric() || ch == '_' || ch == '/' {
            consumed += ch.len_utf8();
            continue;
        }
        return ch.is_whitespace() || matches!(ch, '(' | ':' | '+' | '[');
    }
    consumed == rest.len()
}

pub(crate) fn validate_typed_unit_identities(
    raw_units: &[RawLaunchUnit],
    diagnostics: &mut Vec<LaunchPlanDiagnosticWire>,
) {
    let mut seen: BTreeMap<String, String> = BTreeMap::new();
    for raw in raw_units {
        let identity = match &raw.unit.payload {
            LaunchUnitPayloadWire::Agent(agent) => agent.effective_identity(),
            LaunchUnitPayloadWire::Proc(proc_unit) => {
                proc_unit.shell_name.clone()
            }
        };
        let Some(identity) = identity else {
            continue;
        };
        if let Some(first) =
            seen.insert(identity.clone(), raw.unit.logical_id.clone())
        {
            diagnostics.push(typed_unit_diagnostic(
                "identity-collision",
                &format!(
                    "Launch identity {identity:?} is ambiguous between {first} and {}.",
                    raw.unit.logical_id
                ),
                &raw.unit.logical_id,
                None,
            ));
        }
    }
}
