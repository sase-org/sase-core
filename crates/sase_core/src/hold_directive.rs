//! Shared `%hold` directive argument contract.
//!
//! This module owns hold-field normalization, validation, collection across
//! occurrences, canonical formatting, and pure selector expansion. It never
//! reads global configuration or arms a hold.

use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

use crate::agent_hold::{normalize_hood_vec, AgentHoldSelectorsWire};
use crate::agent_identity::parse_agent_session_name;
use crate::agent_launch::parse_proc_duration_seconds;
use crate::agent_tribe::{
    canonicalize_public_tribe_name, resolve_agent_tribe_identity,
    validate_tribe_name, AgentTribeDisplayLayerWire,
    AgentTribeIdentityResolutionRequestWire,
};

/// One already-split hold argument. `name` is absent for positionals.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct HoldArgWire {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default)]
    pub value: String,
}

/// One `%hold` occurrence supplied by a caller-owned scan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HoldOccurrenceWire {
    pub source: String,
    pub source_span: [usize; 2],
    #[serde(default)]
    pub args: Vec<HoldArgWire>,
    #[serde(default)]
    pub has_plus_suffix: bool,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum HoldScopeWire {
    Project,
    Host,
}

impl HoldScopeWire {
    fn as_str(self) -> &'static str {
        match self {
            Self::Project => "project",
            Self::Host => "host",
        }
    }
}

/// Canonical hold fields. Omitted fields serialize away so hold-free launch
/// wires retain their historical shape.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct HoldFieldsWire {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub names: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tribes: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub hoods: Vec<String>,
    #[serde(default, skip_serializing_if = "skip_if_false")]
    pub pending: bool,
    #[serde(default, skip_serializing_if = "skip_if_false")]
    pub future: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl_seconds: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<HoldScopeWire>,
}

fn skip_if_false(value: &bool) -> bool {
    !*value
}

impl HoldFieldsWire {
    fn has_selector(&self) -> bool {
        !self.names.is_empty()
            || !self.tribes.is_empty()
            || !self.hoods.is_empty()
            || self.pending
            || self.future
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HoldParseErrorWire {
    pub code: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_span: Option<[usize; 2]>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HoldCollectResultWire {
    #[serde(default)]
    pub fields: Option<HoldFieldsWire>,
    #[serde(default)]
    pub errors: Vec<HoldParseErrorWire>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HoldForm {
    Bare,
    Plus,
    Colon,
    Parenthesized { closed: bool },
}

pub fn collect_hold_fields(
    occurrences: &[HoldOccurrenceWire],
) -> HoldCollectResultWire {
    collect_hold_fields_with_flags(occurrences, &[])
}

pub fn collect_hold_fields_with_flags(
    occurrences: &[HoldOccurrenceWire],
    _enabled_feature_flags: &[String],
) -> HoldCollectResultWire {
    let mut fields = HoldFieldsWire::default();
    let mut errors = Vec::new();

    for occurrence in occurrences {
        let form = hold_form(&occurrence.source, occurrence.has_plus_suffix);
        match parse_hold_occurrence(occurrence, form) {
            Ok(part) => {
                if let Err(error) =
                    merge_hold_part(&mut fields, part, occurrence.source_span)
                {
                    errors.push(error);
                }
            }
            Err(error) => errors.push(error),
        }
    }

    normalize_sets(&mut fields);
    if errors.is_empty() && fields.has_selector() {
        HoldCollectResultWire {
            fields: Some(fields),
            errors: Vec::new(),
        }
    } else if errors.is_empty() {
        HoldCollectResultWire {
            fields: None,
            errors: Vec::new(),
        }
    } else {
        HoldCollectResultWire {
            fields: None,
            errors,
        }
    }
}

pub fn format_hold_directive(fields: &HoldFieldsWire) -> Option<String> {
    if !fields.has_selector() {
        return None;
    }
    let mut parts = Vec::new();
    for name in &fields.names {
        parts.push(name.clone());
    }
    for tribe in &fields.tribes {
        parts.push(format!("@{tribe}"));
    }
    if fields.pending {
        parts.push("pending".to_string());
    }
    if fields.future {
        parts.push("future".to_string());
    }
    for hood in &fields.hoods {
        parts.push(format!("hood={hood}"));
    }
    if let Some(ttl) = &fields.ttl {
        parts.push(format!("ttl={ttl}"));
    }
    if let Some(scope) = fields.scope {
        parts.push(format!("scope={}", scope.as_str()));
    }
    Some(format!("%hold({})", parts.join(", ")))
}

/// Optional stored/config evidence for expanding public tribe names.
///
/// An empty identity keeps the context-free `job -> chop` automation alias.
/// Callers that have assignment or config evidence must supply it so an
/// independently stored `job` tribe is not silently equated with `chop`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HoldSelectorIdentityWire {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub stored_tribes: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub layers: Vec<AgentTribeDisplayLayerWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_tribe: Option<String>,
}

pub fn hold_fields_to_selectors(
    fields: &HoldFieldsWire,
    pending_artifact_dirs: &[String],
) -> Result<AgentHoldSelectorsWire, HoldParseErrorWire> {
    hold_fields_to_selectors_with_identity(
        fields,
        pending_artifact_dirs,
        &HoldSelectorIdentityWire::default(),
    )
}

pub fn hold_fields_to_selectors_with_identity(
    fields: &HoldFieldsWire,
    pending_artifact_dirs: &[String],
    identity: &HoldSelectorIdentityWire,
) -> Result<AgentHoldSelectorsWire, HoldParseErrorWire> {
    let mut selectors = AgentHoldSelectorsWire::default();
    if fields.pending {
        selectors.artifact_dirs =
            sorted_dedup(pending_artifact_dirs.iter().cloned());
    }
    selectors.future = fields.future;
    selectors.names = sorted_dedup(fields.names.iter().cloned());
    selectors.clans = sorted_dedup(fields.names.iter().cloned());
    selectors.workflows = sorted_dedup(fields.names.iter().cloned());
    selectors.agent_sessions =
        sorted_dedup(fields.names.iter().filter_map(|name| {
            if name.contains("--") {
                return None;
            }
            parse_agent_session_name(name).ok().map(|_| name.clone())
        }));
    let mut tribes = Vec::new();
    for tribe in &fields.tribes {
        tribes.push(resolve_hold_selector_tribe(tribe, identity)?);
    }
    selectors.tribes = sorted_dedup(tribes);
    selectors.hoods = normalize_hood_vec("selectors.hoods", &fields.hoods)
        .map_err(|error| {
            hold_error("invalid-hold-hood", &format!("{error}"), None)
        })?;
    Ok(selectors)
}

fn resolve_hold_selector_tribe(
    value: &str,
    identity: &HoldSelectorIdentityWire,
) -> Result<String, HoldParseErrorWire> {
    let stripped = value.strip_prefix('@').unwrap_or(value).trim();
    if identity.stored_tribes.is_empty()
        && identity.layers.is_empty()
        && identity.current_tribe.is_none()
    {
        return canonicalize_public_tribe_name(stripped).map_err(|error| {
            hold_error(
                "invalid-hold-tribe",
                &format!("hold tribe selector is not valid: {error}"),
                None,
            )
        });
    }
    let resolved = resolve_agent_tribe_identity(
        &AgentTribeIdentityResolutionRequestWire {
            tribe: stripped.to_string(),
            layers: identity.layers.clone(),
            stored_tribes: identity.stored_tribes.clone(),
            current_tribe: identity.current_tribe.clone(),
        },
    )
    .map_err(|error| {
        hold_error(
            "invalid-hold-tribe",
            &format!("hold tribe selector is not valid: {error}"),
            None,
        )
    })?;
    resolved.tribe.ok_or_else(|| {
        let detail = resolved
            .diagnostics
            .iter()
            .map(|diagnostic| diagnostic.message.as_str())
            .collect::<Vec<_>>()
            .join("; ");
        hold_error(
            "invalid-hold-tribe",
            &format!(
                "hold tribe selector {stripped:?} is ambiguous{}",
                if detail.is_empty() {
                    String::new()
                } else {
                    format!(": {detail}")
                }
            ),
            None,
        )
    })
}

fn parse_hold_occurrence(
    occurrence: &HoldOccurrenceWire,
    form: HoldForm,
) -> Result<HoldFieldsWire, HoldParseErrorWire> {
    let span = Some(occurrence.source_span);
    match form {
        HoldForm::Bare | HoldForm::Plus => Err(selector_required_error(span)),
        HoldForm::Colon => {
            parse_hold_args(&expand_colon_args(&occurrence.args), span)
        }
        HoldForm::Parenthesized { closed: false } => Err(hold_error(
            "malformed-hold",
            "Malformed %hold(...) directive: missing closing ')'.",
            span,
        )),
        HoldForm::Parenthesized { closed: true } => {
            parse_hold_args(&occurrence.args, span)
        }
    }
}

fn parse_hold_args(
    args: &[HoldArgWire],
    span: Option<[usize; 2]>,
) -> Result<HoldFieldsWire, HoldParseErrorWire> {
    if args.is_empty()
        || args
            .iter()
            .all(|arg| arg.name.is_none() && arg.value.trim().is_empty())
    {
        return Err(selector_required_error(span));
    }
    let mut fields = HoldFieldsWire::default();
    for arg in args {
        let value = arg.value.trim();
        match arg.name.as_deref().map(str::trim) {
            None => parse_hold_positional(&mut fields, value, span)?,
            Some("hood") => {
                if value.is_empty() {
                    return Err(selector_required_error(span));
                }
                fields.hoods.push(value.to_string());
            }
            Some("tribe") => {
                fields.tribes.push(parse_tribe_value(value, span)?);
            }
            Some("ttl") => assign_ttl(&mut fields, value, span)?,
            Some("scope") => assign_scope(&mut fields, value, span)?,
            Some("") => {
                return Err(unknown_keyword_error("empty name", span));
            }
            Some(other) => return Err(unknown_keyword_error(other, span)),
        }
    }
    normalize_sets(&mut fields);
    if !fields.has_selector() {
        return Err(selector_required_error(span));
    }
    Ok(fields)
}

fn parse_hold_positional(
    fields: &mut HoldFieldsWire,
    value: &str,
    span: Option<[usize; 2]>,
) -> Result<(), HoldParseErrorWire> {
    if value.is_empty() {
        return Ok(());
    }
    match value {
        "pending" => fields.pending = true,
        "future" => fields.future = true,
        "all" => return Err(hold_error(
            "hold-all-unsupported",
            "%hold(all) is not supported. Select a name, @tribe, pending, future, hood=, or tribe=; for the broadest hold use %hold(pending, future, scope=host).",
            span,
        )),
        _ if value.starts_with('@') => {
            fields.tribes.push(parse_tribe_value(&value[1..], span)?);
        }
        _ => {
            parse_agent_session_name(value).map_err(|error| {
                hold_error(
                    "invalid-hold-name",
                    &format!("%hold name selector {value:?} is not valid: {error}"),
                    span,
                )
            })?;
            fields.names.push(value.to_string());
        }
    }
    Ok(())
}

fn assign_ttl(
    fields: &mut HoldFieldsWire,
    value: &str,
    span: Option<[usize; 2]>,
) -> Result<(), HoldParseErrorWire> {
    if fields.ttl.is_some() {
        return Err(duplicate_field_error("ttl", span));
    }
    let seconds = parse_proc_duration_seconds(value).map_err(|_| {
        hold_error(
            "invalid-hold-ttl",
            "%hold ttl= must use the SASE duration grammar, such as 90m, 1h30m, or 45s.",
            span,
        )
    })?;
    fields.ttl = Some(value.to_string());
    fields.ttl_seconds = Some(seconds);
    Ok(())
}

fn assign_scope(
    fields: &mut HoldFieldsWire,
    value: &str,
    span: Option<[usize; 2]>,
) -> Result<(), HoldParseErrorWire> {
    if fields.scope.is_some() {
        return Err(duplicate_field_error("scope", span));
    }
    fields.scope = Some(match value {
        "project" => HoldScopeWire::Project,
        "host" => HoldScopeWire::Host,
        _ => {
            return Err(hold_error(
                "invalid-hold-scope",
                "%hold scope= must be either project or host.",
                span,
            ))
        }
    });
    Ok(())
}

fn merge_hold_part(
    fields: &mut HoldFieldsWire,
    part: HoldFieldsWire,
    span: [usize; 2],
) -> Result<(), HoldParseErrorWire> {
    fields.names.extend(part.names);
    fields.tribes.extend(part.tribes);
    fields.hoods.extend(part.hoods);
    fields.pending |= part.pending;
    fields.future |= part.future;
    if let Some(ttl) = part.ttl {
        if fields.ttl.is_some() {
            return Err(duplicate_field_error("ttl", Some(span)));
        }
        fields.ttl = Some(ttl);
        fields.ttl_seconds = part.ttl_seconds;
    }
    if let Some(scope) = part.scope {
        if fields.scope.is_some() {
            return Err(duplicate_field_error("scope", Some(span)));
        }
        fields.scope = Some(scope);
    }
    Ok(())
}

fn expand_colon_args(args: &[HoldArgWire]) -> Vec<HoldArgWire> {
    args.iter()
        .flat_map(|arg| {
            if arg.name.is_some() {
                return vec![arg.clone()];
            }
            arg.value
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(|value| HoldArgWire {
                    name: None,
                    value: value.to_string(),
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

fn parse_tribe_value(
    value: &str,
    span: Option<[usize; 2]>,
) -> Result<String, HoldParseErrorWire> {
    let stripped = value.strip_prefix('@').unwrap_or(value);
    validate_tribe_name(stripped)
        .map(str::to_string)
        .map_err(|error| {
            hold_error(
                "invalid-hold-tribe",
                &format!("%hold tribe selector is not valid: {error}"),
                span,
            )
        })
}

fn normalize_sets(fields: &mut HoldFieldsWire) {
    fields.names = sorted_dedup(fields.names.drain(..));
    fields.tribes = sorted_dedup(fields.tribes.drain(..));
    fields.hoods = sorted_dedup(fields.hoods.drain(..));
}

fn sorted_dedup(values: impl IntoIterator<Item = String>) -> Vec<String> {
    values
        .into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn selector_required_error(span: Option<[usize; 2]>) -> HoldParseErrorWire {
    hold_error(
        "hold-selector-required",
        "%hold requires a selector: name, @tribe, pending, future, hood=, or tribe=.",
        span,
    )
}

fn duplicate_field_error(
    field: &str,
    span: Option<[usize; 2]>,
) -> HoldParseErrorWire {
    hold_error(
        "duplicate-hold-field",
        &format!(
            "Duplicate %hold {field} assignment is not allowed, even when the values match."
        ),
        span,
    )
}

fn unknown_keyword_error(
    keyword: &str,
    span: Option<[usize; 2]>,
) -> HoldParseErrorWire {
    hold_error(
        "unknown-hold-keyword",
        &format!(
            "Unsupported keyword on %hold: {keyword}=. Use hood=, scope=, ttl=, or tribe=."
        ),
        span,
    )
}

fn hold_error(
    code: &str,
    message: &str,
    source_span: Option<[usize; 2]>,
) -> HoldParseErrorWire {
    HoldParseErrorWire {
        code: code.to_string(),
        message: message.to_string(),
        source_span,
    }
}

fn hold_form(source: &str, has_plus_suffix: bool) -> HoldForm {
    if has_plus_suffix {
        return HoldForm::Plus;
    }
    match suffix_after_directive_name(source).chars().next() {
        Some('(') => HoldForm::Parenthesized {
            closed: parenthesized_source_is_closed(source),
        },
        Some(':') => HoldForm::Colon,
        Some('+') => HoldForm::Plus,
        _ => HoldForm::Bare,
    }
}

fn suffix_after_directive_name(source: &str) -> &str {
    let trimmed = source.trim_start();
    let body = trimmed.strip_prefix('%').unwrap_or(trimmed);
    let end = body
        .find(|ch: char| !ch.is_ascii_alphanumeric() && ch != '_')
        .unwrap_or(body.len());
    &body[end..]
}

fn parenthesized_source_is_closed(source: &str) -> bool {
    let Some(open) = source.find('(') else {
        return true;
    };
    let bytes = source.as_bytes();
    let mut depth = 1usize;
    let mut index = open + 1;
    let mut quote: Option<u8> = None;
    while index < bytes.len() {
        let byte = bytes[index];
        if quote.is_none() && bytes.get(index..index + 2) == Some(b"[[") {
            if let Some(close) = source[index + 2..].find("]]") {
                index += 2 + close + 2;
                continue;
            }
            return false;
        }
        match quote {
            Some(q) if byte == q => quote = None,
            Some(_) => {}
            None if byte == b'"' || byte == b'`' => quote = Some(byte),
            None if byte == b'(' => depth += 1,
            None if byte == b')' => {
                depth -= 1;
                if depth == 0 {
                    return true;
                }
            }
            None => {}
        }
        index += 1;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    fn occ(source: &str, args: Vec<HoldArgWire>) -> HoldOccurrenceWire {
        HoldOccurrenceWire {
            source: source.to_string(),
            source_span: [0, source.len()],
            args,
            has_plus_suffix: false,
        }
    }

    fn positional(value: &str) -> HoldArgWire {
        HoldArgWire {
            name: None,
            value: value.to_string(),
        }
    }

    fn named(name: &str, value: &str) -> HoldArgWire {
        HoldArgWire {
            name: Some(name.to_string()),
            value: value.to_string(),
        }
    }

    fn collect_ok(occurrences: &[HoldOccurrenceWire]) -> HoldFieldsWire {
        let result = collect_hold_fields_with_flags(occurrences, &[]);
        assert!(result.errors.is_empty(), "{:?}", result.errors);
        result.fields.expect("fields")
    }

    fn collect_err(
        occurrences: &[HoldOccurrenceWire],
        flags: &[String],
    ) -> Vec<HoldParseErrorWire> {
        let result = collect_hold_fields_with_flags(occurrences, flags);
        assert!(result.fields.is_none(), "{result:?}");
        result.errors
    }

    #[test]
    fn enabled_flags_are_not_required() {
        let fields =
            collect_ok(&[occ("%hold:planner", vec![positional("planner")])]);
        assert_eq!(fields.names, vec!["planner"]);
    }

    #[test]
    fn parses_unions_and_canonical_round_trip() {
        let fields = collect_ok(&[
            occ("%hold:reviewer,planner", vec![positional("reviewer,planner")]),
            occ(
                "%hold(pending, future, hood=sase-11l, tribe=nightly, ttl=1h30m, scope=host)",
                vec![
                    positional("pending"),
                    positional("future"),
                    named("hood", "sase-11l"),
                    named("tribe", "nightly"),
                    named("ttl", "1h30m"),
                    named("scope", "host"),
                ],
            ),
            occ("%hold:@job", vec![positional("@job")]),
        ]);
        assert_eq!(fields.names, vec!["planner", "reviewer"]);
        assert_eq!(fields.tribes, vec!["job", "nightly"]);
        assert_eq!(fields.hoods, vec!["sase-11l"]);
        assert!(fields.pending);
        assert!(fields.future);
        assert_eq!(fields.ttl.as_deref(), Some("1h30m"));
        assert_eq!(fields.ttl_seconds, Some(5_400));
        assert_eq!(fields.scope, Some(HoldScopeWire::Host));

        let formatted = format_hold_directive(&fields).expect("formatted");
        let reparsed = collect_ok(&[occ(
            &formatted,
            vec![
                positional("planner"),
                positional("reviewer"),
                positional("@job"),
                positional("@nightly"),
                positional("pending"),
                positional("future"),
                named("hood", "sase-11l"),
                named("ttl", "1h30m"),
                named("scope", "host"),
            ],
        )]);
        assert_eq!(reparsed, fields);
    }

    #[test]
    fn rejects_required_errors() {
        for (source, args, code) in [
            ("%hold", vec![positional("")], "hold-selector-required"),
            ("%hold()", Vec::new(), "hold-selector-required"),
            ("%hold+", vec![positional("true")], "hold-selector-required"),
            ("%hold:all", vec![positional("all")], "hold-all-unsupported"),
            (
                "%hold(scope=global)",
                vec![named("scope", "global")],
                "invalid-hold-scope",
            ),
            (
                "%hold(ttl=tomorrow)",
                vec![named("ttl", "tomorrow")],
                "invalid-hold-ttl",
            ),
            (
                "%hold(foo=bar)",
                vec![named("foo", "bar")],
                "unknown-hold-keyword",
            ),
        ] {
            let errors = collect_err(&[occ(source, args)], &[]);
            assert_eq!(errors[0].code, code);
        }
    }

    #[test]
    fn duplicate_scope_and_ttl_are_errors_across_occurrences() {
        let errors = collect_err(
            &[
                occ(
                    "%hold(planner, ttl=5m)",
                    vec![positional("planner"), named("ttl", "5m")],
                ),
                occ(
                    "%hold(future, ttl=5m)",
                    vec![positional("future"), named("ttl", "5m")],
                ),
            ],
            &[],
        );
        assert_eq!(errors[0].code, "duplicate-hold-field");

        let errors = collect_err(
            &[
                occ(
                    "%hold(planner, scope=project)",
                    vec![positional("planner"), named("scope", "project")],
                ),
                occ(
                    "%hold(future, scope=host)",
                    vec![positional("future"), named("scope", "host")],
                ),
            ],
            &[],
        );
        assert_eq!(errors[0].code, "duplicate-hold-field");
    }

    #[test]
    fn expands_names_to_selectors_without_turn_agent_session() {
        let fields = collect_ok(&[occ(
            "%hold(builder, builder--mon, hood=sase-11l, pending, future)",
            vec![
                positional("builder"),
                positional("builder--mon"),
                named("hood", "sase-11l"),
                positional("pending"),
                positional("future"),
            ],
        )]);
        let selectors = hold_fields_to_selectors(
            &fields,
            &["/tmp/a".to_string(), "/tmp/a".to_string()],
        )
        .expect("selectors");
        assert_eq!(selectors.names, vec!["builder", "builder--mon"]);
        assert_eq!(selectors.clans, vec!["builder", "builder--mon"]);
        assert_eq!(selectors.workflows, vec!["builder", "builder--mon"]);
        assert_eq!(selectors.agent_sessions, vec!["builder"]);
        assert_eq!(selectors.artifact_dirs, vec!["/tmp/a"]);
        assert!(selectors.future);
    }

    #[test]
    fn expands_job_to_chop_without_stored_job_identity() {
        let fields = collect_ok(&[occ("%hold:@job", vec![positional("@job")])]);
        assert_eq!(fields.tribes, vec!["job"]);
        let selectors =
            hold_fields_to_selectors(&fields, &[]).expect("selectors");
        assert_eq!(selectors.tribes, vec!["chop"]);
    }

    #[test]
    fn keeps_independently_stored_job_tribe() {
        let fields = collect_ok(&[occ("%hold:@job", vec![positional("@job")])]);
        let selectors = hold_fields_to_selectors_with_identity(
            &fields,
            &[],
            &HoldSelectorIdentityWire {
                stored_tribes: vec!["job".to_string()],
                ..HoldSelectorIdentityWire::default()
            },
        )
        .expect("selectors");
        assert_eq!(selectors.tribes, vec!["job"]);
    }
}
