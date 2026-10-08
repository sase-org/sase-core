//! Frontmatter validation for the Plan Decision authoring grammar.
//!
//! Covers every rule in Section 2 of `plan:202610/plan_decisions.md` for both
//! tale and epic plans: decision ids, fields, kinds, defaults, memory
//! selectors, requested quotes, system-written answers and stamps, the
//! reserved `phases[].when`, and the Archived completeness rule. Body checks
//! (callouts, unreferenced decisions, uncovered memory edits) live here too
//! so frontmatter and body share one outcome.

use serde_json::json;
use serde_yaml::{Mapping, Value as YamlValue};

use super::super::validate::{
    PlanDiagnosticWire, PlanValidationMode, SourceIndex,
};
use super::callout::{
    callout_wire, parse_decision_callouts, uncovered_memory_edits,
    whole_word_mentions_body,
};
use super::wire::{PlanDecisionCalloutWire, PlanDecisionWire};
use super::wire::{PlanDecisionChoiceWire, PlanDecisionMemoryWire};

/// Decision ids the host owns; planners must not claim them.
const RESERVED_IDS: &[&str] = &[
    "approve",
    "commit",
    "reject",
    "feedback",
    "coder_prompt",
    "coder_model",
    "wait",
    "epic_launch_mode",
    "capacity",
];

/// YAML 1.1 bool/null words, banned as decision and choice-key spellings.
const YAML_WORDS: &[&str] =
    &["y", "n", "yes", "no", "on", "off", "true", "false", "null"];

const DECISION_FIELDS: &[&str] = &[
    "ask",
    "choices",
    "default",
    "why",
    "memory",
    "requested",
    "answer",
];

/// A successfully parsed decision for body validation.
#[derive(Debug, Clone)]
pub struct DecisionBodyInfo {
    pub id: String,
    pub kind: String,
    pub choice_keys: Vec<String>,
    pub memory_selectors: Vec<String>,
    pub is_memory: bool,
}

/// Everything the frontmatter pass learned: wire records, body-validation
/// inputs, system stamps, and diagnostics.
#[derive(Debug, Clone)]
pub struct DecisionFrontmatterOutcome {
    pub decisions: Vec<PlanDecisionWire>,
    pub infos: Vec<DecisionBodyInfo>,
    pub decided_by: Option<String>,
    pub decided_via: Option<String>,
    pub diagnostics: Vec<PlanDiagnosticWire>,
}

/// Everything the body pass learned: callout wire records and diagnostics.
#[derive(Debug, Clone)]
pub struct DecisionBodyOutcome {
    pub callouts: Vec<PlanDecisionCalloutWire>,
    pub diagnostics: Vec<PlanDiagnosticWire>,
}

fn diagnostic(
    severity: &str,
    code: &str,
    field_path: &str,
    message: String,
    line: Option<u64>,
) -> PlanDiagnosticWire {
    PlanDiagnosticWire {
        severity: severity.to_string(),
        code: code.to_string(),
        field_path: field_path.to_string(),
        message,
        line,
    }
}

fn mapping_value<'a>(mapping: &'a Mapping, key: &str) -> Option<&'a YamlValue> {
    mapping.get(YamlValue::String(key.to_string()))
}

fn yaml_type_name(value: &YamlValue) -> &'static str {
    match value {
        YamlValue::Null => "null",
        YamlValue::Bool(_) => "boolean",
        YamlValue::Number(_) => "number",
        YamlValue::String(_) => "string",
        YamlValue::Sequence(_) => "list",
        YamlValue::Mapping(_) => "mapping",
        YamlValue::Tagged(_) => "tagged value",
    }
}

fn is_one_line(text: &str) -> bool {
    !text.contains('\n') && !text.contains('\r')
}

fn valid_id_spelling(id: &str, max_chars: usize) -> bool {
    let mut chars = id.chars();
    match chars.next() {
        Some(first)
            if first.is_ascii_lowercase()
                && id.chars().count() <= max_chars =>
        {
            chars.all(|char| {
                char.is_ascii_lowercase()
                    || char.is_ascii_digit()
                    || char == '_'
            })
        }
        _ => false,
    }
}

fn is_yaml_word(id: &str) -> bool {
    let lower = id.to_lowercase();
    YAML_WORDS.contains(&lower.as_str())
}

/// Validate the top-level system stamps. In Authoring both fields are
/// forbidden; in Launch/Archived their values are checked.
fn validate_system_stamps(
    mapping: &Mapping,
    index: &SourceIndex,
    mode: PlanValidationMode,
    diagnostics: &mut Vec<PlanDiagnosticWire>,
) -> (Option<String>, Option<String>) {
    let mut decided_by = None;
    let mut decided_via = None;
    for field in ["decided_by", "decided_via"] {
        let Some(value) = mapping_value(mapping, field) else {
            continue;
        };
        if mode == PlanValidationMode::Authoring {
            diagnostics.push(diagnostic(
                "error",
                "decision-system-field-forbidden",
                field,
                format!(
                    "system-written field `{field}` is forbidden in Authoring mode; SASE writes it when the plan is reviewed"
                ),
                index.line_for(field),
            ));
            continue;
        }
        let Some(raw) = value.as_str() else {
            diagnostics.push(diagnostic(
                "error",
                "decision-provenance-invalid",
                field,
                format!(
                    "field `{field}` must be a string, found {}",
                    yaml_type_name(value)
                ),
                index.line_for(field),
            ));
            continue;
        };
        let trimmed = raw.trim().to_string();
        let allowed: &[&str] = match field {
            "decided_by" => &["reviewer", "auto", "agent"],
            _ => &["tui", "telegram", "mobile", "cli"],
        };
        if !allowed.contains(&trimmed.as_str()) {
            diagnostics.push(diagnostic(
                "error",
                "decision-provenance-invalid",
                field,
                format!(
                    "field `{field}` must be one of {}, found `{trimmed}`",
                    allowed.join(" | ")
                ),
                index.line_for(field),
            ));
            continue;
        }
        if field == "decided_by" {
            decided_by = Some(trimmed);
        } else {
            decided_via = Some(trimmed);
        }
    }
    if decided_by.as_deref() == Some("auto") && decided_via.is_some() {
        diagnostics.push(diagnostic(
            "error",
            "decision-provenance-invalid",
            "decided_via",
            "`decided_via` must be absent when `decided_by` is `auto`"
                .to_string(),
            index.line_for("decided_via"),
        ));
        decided_via = None;
    }
    (decided_by, decided_via)
}

/// Validate `decisions`, `decided_by`, and `decided_via` in plan frontmatter.
pub fn validate_decision_frontmatter(
    mapping: &Mapping,
    index: &SourceIndex,
    mode: PlanValidationMode,
) -> DecisionFrontmatterOutcome {
    let mut diagnostics = Vec::new();
    let (decided_by, decided_via) =
        validate_system_stamps(mapping, index, mode, &mut diagnostics);

    let Some(value) = mapping_value(mapping, "decisions") else {
        return DecisionFrontmatterOutcome {
            decisions: Vec::new(),
            infos: Vec::new(),
            decided_by,
            decided_via,
            diagnostics,
        };
    };
    let Some(entries) = value.as_mapping() else {
        diagnostics.push(diagnostic(
            "error",
            "decision-invalid",
            "decisions",
            format!(
                "field `decisions` must be a mapping of decision ids, found {}",
                yaml_type_name(value)
            ),
            index.line_for("decisions"),
        ));
        return DecisionFrontmatterOutcome {
            decisions: Vec::new(),
            infos: Vec::new(),
            decided_by,
            decided_via,
            diagnostics,
        };
    };
    if entries.len() > 5 {
        diagnostics.push(diagnostic(
            "error",
            "decision-limit",
            "decisions",
            format!("at most 5 decisions are allowed, found {}", entries.len()),
            index.line_for("decisions"),
        ));
    }
    if entries.is_empty() {
        return DecisionFrontmatterOutcome {
            decisions: Vec::new(),
            infos: Vec::new(),
            decided_by,
            decided_via,
            diagnostics,
        };
    }

    let mut outcome = DecisionFrontmatterOutcome {
        decisions: Vec::new(),
        infos: Vec::new(),
        decided_by,
        decided_via,
        diagnostics,
    };
    // serde_yaml mappings preserve author order, including duplicate-key
    // rejection at parse time, so iteration order is the accepted vector.
    for (key, entry) in entries {
        let id = key.as_str().unwrap_or("").to_string();
        let field_path = format!("decisions.{id}");
        if !key.is_string() || !valid_id_spelling(&id, 32) || is_yaml_word(&id)
        {
            let detail = if !key.is_string() {
                format!(
                    "decision ids must be strings, found {}",
                    yaml_type_name(key)
                )
            } else if id.is_empty() {
                "decision ids must not be empty".to_string()
            } else if is_yaml_word(&id) {
                format!(
                    "decision id `{id}` is a YAML 1.1 bool/null word; rename it"
                )
            } else {
                format!(
                    "decision id `{id}` must match ^[a-z][a-z0-9_]*$ with at most 32 characters"
                )
            };
            outcome.diagnostics.push(diagnostic(
                "error",
                "decision-id-invalid",
                if id.is_empty() {
                    "decisions"
                } else {
                    &field_path
                },
                detail,
                index.line_for("decisions"),
            ));
            continue;
        }
        if RESERVED_IDS.contains(&id.as_str()) {
            outcome.diagnostics.push(diagnostic(
                "error",
                "decision-id-reserved",
                &field_path,
                format!(
                    "decision id `{id}` is reserved for host review controls; rename it"
                ),
                index.line_for(&field_path),
            ));
            continue;
        }
        let Some(entry) = entry.as_mapping() else {
            outcome.diagnostics.push(diagnostic(
                "error",
                "decision-invalid",
                &field_path,
                format!(
                    "decision `{id}` must be a mapping, found {}",
                    yaml_type_name(entry)
                ),
                index.line_for(&field_path),
            ));
            continue;
        };
        let errors_before = outcome.diagnostics.len();
        let parsed =
            validate_one_decision(&id, entry, index, mode, &mut outcome);
        if outcome.diagnostics.len() == errors_before {
            outcome.decisions.push(parsed.wire);
        }
        outcome.infos.push(parsed.info);
    }

    if mode == PlanValidationMode::Archived {
        apply_archived_completeness(mapping, index, &mut outcome);
    }
    outcome
}

fn decision_error(
    diagnostics: &mut Vec<PlanDiagnosticWire>,
    index: &SourceIndex,
    code: &str,
    path: String,
    message: String,
) {
    let line = index.line_for(&path);
    diagnostics.push(diagnostic("error", code, &path, message, line));
}

struct ParsedDecision {
    wire: PlanDecisionWire,
    info: DecisionBodyInfo,
}

#[allow(clippy::too_many_lines)]
fn validate_one_decision(
    id: &str,
    entry: &Mapping,
    index: &SourceIndex,
    mode: PlanValidationMode,
    outcome: &mut DecisionFrontmatterOutcome,
) -> ParsedDecision {
    let field_path = format!("decisions.{id}");
    for key in entry.keys() {
        let name = key.as_str().unwrap_or("");
        if !DECISION_FIELDS.contains(&name) {
            decision_error(&mut outcome.diagnostics, index,
                "decision-unknown-field",
                format!("{field_path}.{name}"),
                format!(
                    "unknown decision field `{name}`; allowed fields are ask, choices, default, why, memory, requested, answer"
                ),
            );
        }
    }

    let is_choice = mapping_value(entry, "choices").is_some();
    let kind = if is_choice { "choice" } else { "toggle" };
    let has_memory = mapping_value(entry, "memory").is_some();

    let ask = match mapping_value(entry, "ask") {
        None => {
            decision_error(
                &mut outcome.diagnostics,
                index,
                "decision-ask-invalid",
                format!("{field_path}.ask"),
                format!("decision `{id}` is missing required field `ask`"),
            );
            String::new()
        }
        Some(value) => match value.as_str() {
            None => {
                decision_error(&mut outcome.diagnostics, index,
                    "decision-ask-invalid",
                    format!("{field_path}.ask"),
                    format!(
                        "decision `{id}` field `ask` must be a string, found {}",
                        yaml_type_name(value)
                    ),
                );
                String::new()
            }
            Some(raw) => {
                let ask = raw.trim().to_string();
                if ask.is_empty()
                    || !is_one_line(&ask)
                    || ask.chars().count() > 120
                {
                    decision_error(&mut outcome.diagnostics, index,
                        "decision-ask-invalid",
                        format!("{field_path}.ask"),
                        format!(
                            "decision `{id}` field `ask` must be non-empty one-line text of at most 120 characters"
                        ),
                    );
                } else if !ask.ends_with('?') {
                    outcome.diagnostics.push(diagnostic(
                        "warning",
                        "decision-ask-not-question",
                        &format!("{field_path}.ask"),
                        format!(
                            "decision `{id}` field `ask` should end with `?`"
                        ),
                        index.line_for(&format!("{field_path}.ask")),
                    ));
                }
                ask
            }
        },
    };

    let mut choice_keys = Vec::new();
    let mut choices = Vec::new();
    if is_choice {
        // Presence of `choices` selects the choice kind, even when malformed.
        match mapping_value(entry, "choices").and_then(YamlValue::as_mapping)
        {
            None => decision_error(&mut outcome.diagnostics, index,
                "decision-choices-count",
                format!("{field_path}.choices"),
                format!(
                    "decision `{id}` field `choices` must be a map with 2-5 entries"
                ),
            ),
            Some(options) => {
                if options.len() < 2 || options.len() > 5 {
                    decision_error(&mut outcome.diagnostics, index,
                        "decision-choices-count",
                        format!("{field_path}.choices"),
                        format!(
                            "decision `{id}` field `choices` must have 2-5 entries, found {}",
                            options.len()
                        ),
                    );
                }
                for (key, label) in options {
                    let name = key.as_str().unwrap_or("").to_string();
                    if !key.is_string()
                        || !valid_id_spelling(&name, 24)
                        || is_yaml_word(&name)
                    {
                        decision_error(&mut outcome.diagnostics, index,
                            "decision-choice-invalid",
                            format!("{field_path}.choices.{name}"),
                            format!(
                                "decision `{id}` choice keys must match ^[a-z][a-z0-9_]*$ with at most 24 characters and must not be YAML 1.1 bool/null words"
                            ),
                        );
                        continue;
                    }
                    match label.as_str() {
                        Some(raw) => {
                            let label = raw.trim().to_string();
                            if label.is_empty()
                                || !is_one_line(&label)
                                || label.chars().count() > 100
                            {
                                decision_error(&mut outcome.diagnostics, index,
                                    "decision-choice-invalid",
                                    format!("{field_path}.choices.{name}"),
                                    format!(
                                        "decision `{id}` choice `{name}` must be a non-empty one-line consequence of at most 100 characters"
                                    ),
                                );
                                continue;
                            }
                            choice_keys.push(name.clone());
                            choices.push(PlanDecisionChoiceWire {
                                key: name,
                                label,
                            });
                        }
                        None => decision_error(&mut outcome.diagnostics, index,
                            "decision-choice-invalid",
                            format!("{field_path}.choices.{name}"),
                            format!(
                                "decision `{id}` choice `{name}` must be a string, found {}",
                                yaml_type_name(label)
                            ),
                        ),
                    }
                }
            }
        }
    }

    let mut default = json!(null);
    let mut default_bool = None;
    match mapping_value(entry, "default") {
        None => decision_error(
            &mut outcome.diagnostics,
            index,
            "decision-default-missing",
            format!("{field_path}.default"),
            format!("decision `{id}` is missing required field `default`"),
        ),
        Some(value) => {
            if is_choice {
                match value.as_str() {
                    Some(raw) if choice_keys.contains(&raw.to_string()) => {
                        default = json!(raw);
                    }
                    _ => decision_error(&mut outcome.diagnostics, index,
                        "decision-default-invalid",
                        format!("{field_path}.default"),
                        format!(
                            "decision `{id}` default must exactly name one of its choice keys ({})",
                            choice_keys.join(", ")
                        ),
                    ),
                }
            } else if let Some(flag) = value.as_bool() {
                default = json!(flag);
                default_bool = Some(flag);
            } else {
                let hint = match value.as_str() {
                    Some(_) => " use `true`/`false`, never `yes`/`on` strings",
                    None => "",
                };
                decision_error(&mut outcome.diagnostics, index,
                    "decision-default-invalid",
                    format!("{field_path}.default"),
                    format!(
                        "decision `{id}` toggle default must be a YAML boolean{hint}"
                    ),
                );
            }
        }
    }

    let mut why = None;
    if let Some(value) = mapping_value(entry, "why") {
        if has_memory {
            decision_error(&mut outcome.diagnostics, index,
                "decision-why-on-memory",
                format!("{field_path}.why"),
                format!(
                    "decision `{id}` is a memory decision and must not carry `why`; the requested quote explains it"
                ),
            );
        }
        match value.as_str() {
            Some(raw) => {
                let text = raw.trim().to_string();
                if text.is_empty()
                    || !is_one_line(&text)
                    || text.chars().count() > 100
                {
                    decision_error(&mut outcome.diagnostics, index,
                        "decision-why-invalid",
                        format!("{field_path}.why"),
                        format!(
                            "decision `{id}` field `why` must be non-empty one-line text of at most 100 characters"
                        ),
                    );
                } else {
                    why = Some(text);
                }
            }
            None => decision_error(
                &mut outcome.diagnostics,
                index,
                "decision-why-invalid",
                format!("{field_path}.why"),
                format!(
                    "decision `{id}` field `why` must be a string, found {}",
                    yaml_type_name(value)
                ),
            ),
        }
    }

    let mut memory_selectors = Vec::new();
    if has_memory {
        if is_choice {
            decision_error(
                &mut outcome.diagnostics,
                index,
                "decision-memory-on-choice",
                format!("{field_path}.memory"),
                format!(
                    "decision `{id}` field `memory` is allowed on toggles only"
                ),
            );
        } else {
            let mut selectors = Vec::new();
            let mut selectors_valid = true;
            match mapping_value(entry, "memory")
                .and_then(YamlValue::as_sequence)
            {
                None => {
                    decision_error(&mut outcome.diagnostics, index,
                        "decision-memory-selector-invalid",
                        format!("{field_path}.memory"),
                        format!(
                            "decision `{id}` field `memory` must be a non-empty list of read selectors"
                        ),
                    );
                    selectors_valid = false;
                }
                Some(items) => {
                    if items.is_empty() {
                        decision_error(&mut outcome.diagnostics, index,
                            "decision-memory-selector-invalid",
                            format!("{field_path}.memory"),
                            format!(
                                "decision `{id}` field `memory` must be a non-empty list of read selectors"
                            ),
                        );
                        selectors_valid = false;
                    }
                    for (item_index, item) in items.iter().enumerate() {
                        let path = format!("{field_path}.memory[{item_index}]");
                        match item.as_str() {
                            Some(raw) => {
                                let selector = raw.trim().to_string();
                                if selector.is_empty()
                                    || !is_one_line(&selector)
                                    || !valid_memory_selector(&selector)
                                {
                                    decision_error(&mut outcome.diagnostics, index,
                                        "decision-memory-selector-invalid",
                                        path,
                                        format!(
                                            "decision `{id}` memory selectors look like `note.md`, `web`, or `web:keyword` (nested note paths and keyword aliases allowed); both sides of `:` must be non-empty and path segments must not be empty"
                                        ),
                                    );
                                    selectors_valid = false;
                                } else {
                                    selectors.push(selector);
                                }
                            }
                            None => {
                                decision_error(&mut outcome.diagnostics, index,
                                    "decision-memory-selector-invalid",
                                    path,
                                    format!(
                                        "decision `{id}` memory selectors must be strings, found {}",
                                        yaml_type_name(item)
                                    ),
                                );
                                selectors_valid = false;
                            }
                        }
                    }
                }
            }
            if selectors_valid {
                memory_selectors = selectors;
            }
        }
    }

    let mut requested = None;
    match mapping_value(entry, "requested") {
        None => {
            if has_memory && !is_choice && default_bool == Some(true) {
                decision_error(&mut outcome.diagnostics, index,
                    "decision-requested-missing",
                    format!("{field_path}.requested"),
                    format!(
                        "decision `{id}` is a memory decision defaulting to true and must quote the human request in `requested`"
                    ),
                );
            }
        }
        Some(value) => {
            if !has_memory || is_choice {
                decision_error(&mut outcome.diagnostics, index,
                    "decision-requested-not-memory",
                    format!("{field_path}.requested"),
                    format!(
                        "decision `{id}` field `requested` is allowed on memory toggles only"
                    ),
                );
            }
            match value.as_str() {
                Some(raw) => {
                    let quote = raw.trim().to_string();
                    let chars = quote.chars().count();
                    if quote.is_empty()
                        || !is_one_line(&quote)
                        || !(3..=300).contains(&chars)
                    {
                        decision_error(&mut outcome.diagnostics, index,
                            "decision-requested-invalid",
                            format!("{field_path}.requested"),
                            format!(
                                "decision `{id}` field `requested` must be one line of 3-300 characters"
                            ),
                        );
                    } else {
                        requested = Some(quote);
                    }
                }
                None => decision_error(&mut outcome.diagnostics, index,
                    "decision-requested-invalid",
                    format!("{field_path}.requested"),
                    format!(
                        "decision `{id}` field `requested` must be a string, found {}",
                        yaml_type_name(value)
                    ),
                ),
            }
        }
    }

    let mut answer = None;
    if let Some(value) = mapping_value(entry, "answer") {
        if mode == PlanValidationMode::Authoring {
            decision_error(&mut outcome.diagnostics, index,
                "decision-answer-forbidden",
                format!("{field_path}.answer"),
                format!(
                    "decision `{id}` field `answer` is system-written and forbidden in Authoring mode"
                ),
            );
        } else if is_choice {
            match value.as_str() {
                Some(raw) if choice_keys.contains(&raw.to_string()) => {
                    answer = Some(json!(raw));
                }
                _ => decision_error(&mut outcome.diagnostics, index,
                    "decision-answer-invalid",
                    format!("{field_path}.answer"),
                    format!(
                        "decision `{id}` answer must exactly name one of its choice keys ({})",
                        choice_keys.join(", ")
                    ),
                ),
            }
        } else if let Some(flag) = value.as_bool() {
            answer = Some(json!(flag));
        } else {
            decision_error(&mut outcome.diagnostics, index,
                "decision-answer-invalid",
                format!("{field_path}.answer"),
                format!(
                    "decision `{id}` toggle answer must be a YAML boolean, found {}",
                    yaml_type_name(value)
                ),
            );
        }
    }

    let wire = PlanDecisionWire {
        id: id.to_string(),
        kind: kind.to_string(),
        ask,
        why,
        choices,
        default,
        memory: if has_memory && !is_choice && !memory_selectors.is_empty() {
            Some(PlanDecisionMemoryWire {
                selectors: memory_selectors.clone(),
            })
        } else {
            None
        },
        requested,
        answer,
    };
    let info = DecisionBodyInfo {
        id: id.to_string(),
        kind: kind.to_string(),
        choice_keys,
        memory_selectors,
        is_memory: has_memory && !is_choice,
    };
    ParsedDecision { wire, info }
}

/// Host read-selector syntax: `note.md`, `web`, `web:keyword`, nested note
/// paths, and keyword aliases. Core checks syntax only.
fn valid_memory_selector(selector: &str) -> bool {
    if let Some(cut) = selector.find(':') {
        let (head, tail) = selector.split_at(cut);
        let tail = &tail[1..];
        if head.trim().is_empty() || tail.trim().is_empty() {
            return false;
        }
    }
    if selector.contains('/')
        && (selector.starts_with('/')
            || selector.ends_with('/')
            || selector.contains("//"))
    {
        return false;
    }
    true
}

/// In Archived mode a stamped plan needs `decided_by` and a valid answer on
/// every decision together; an unstamped archived plan stays valid.
fn apply_archived_completeness(
    mapping: &Mapping,
    index: &SourceIndex,
    outcome: &mut DecisionFrontmatterOutcome,
) {
    let any_answer = mapping_value(mapping, "decisions")
        .and_then(YamlValue::as_mapping)
        .map(|entries| {
            entries.values().any(|entry| {
                entry
                    .as_mapping()
                    .is_some_and(|decision| decision.contains_key("answer"))
            })
        })
        .unwrap_or(false);
    if outcome.decided_by.is_none() && !any_answer {
        return;
    }
    if outcome.decided_by.is_none() {
        outcome.diagnostics.push(diagnostic(
            "error",
            "decision-answer-incomplete",
            "decided_by",
            "archived plans with answers must record top-level `decided_by` alongside every decision `answer`".to_string(),
            index.line_for("decided_by"),
        ));
    }
    for info in &outcome.infos {
        // Presence (not validity) matters here: an invalid answer already
        // reports `decision-answer-invalid`, so completeness only fires for
        // decisions carrying no `answer` key at all.
        let present = mapping_value(mapping, "decisions")
            .and_then(YamlValue::as_mapping)
            .and_then(|entries| entries.get(YamlValue::String(info.id.clone())))
            .and_then(YamlValue::as_mapping)
            .is_some_and(|decision| decision.contains_key("answer"));
        if !present {
            let path = format!("decisions.{}.answer", info.id);
            outcome.diagnostics.push(diagnostic(
                "error",
                "decision-answer-incomplete",
                &path,
                format!(
                    "archived decision `{}` needs a valid `answer` now that the plan carries review stamps",
                    info.id
                ),
                index.line_for(&path),
            ));
        }
    }
}

/// Validate callouts, unreferenced decisions, and uncovered memory edits in
/// the plan body. `body_base` is the original-document line of `body[0]`.
pub fn validate_decision_body(
    body: &str,
    body_base: u64,
    infos: &[DecisionBodyInfo],
    index: &SourceIndex,
) -> DecisionBodyOutcome {
    let mut outcome = DecisionBodyOutcome {
        callouts: Vec::new(),
        diagnostics: Vec::new(),
    };
    let raw = parse_decision_callouts(body, body_base);
    for callout in &raw {
        match infos.iter().find(|info| info.id == callout.id) {
            None => {
                let path = if callout.id.is_empty() {
                    String::new()
                } else {
                    format!("decisions.{}", callout.id)
                };
                outcome.diagnostics.push(diagnostic(
                    "error",
                    "decision-branch-unknown",
                    &path,
                    if callout.id.is_empty() {
                        "decision callout is missing its decision id"
                            .to_string()
                    } else {
                        format!(
                            "decision callout names unknown decision `{}`",
                            callout.id
                        )
                    },
                    Some(callout.header_line),
                ));
            }
            Some(info) => {
                let resolved = match (&info.kind[..], callout.value.as_deref())
                {
                    ("toggle", None) => Some(("yes", None)),
                    ("toggle", Some("yes")) => Some(("yes", None)),
                    ("toggle", Some("no")) => Some(("no", None)),
                    ("choice", Some(key))
                        if info.choice_keys.contains(&key.to_string()) =>
                    {
                        Some(("choice", Some(key)))
                    }
                    _ => None,
                };
                match resolved {
                    Some((branch, key)) => outcome.callouts.push(callout_wire(
                        &info.id,
                        key,
                        branch,
                        callout.start_line,
                        callout.end_line,
                    )),
                    None => outcome.diagnostics.push(diagnostic(
                        "error",
                        "decision-branch-unknown",
                        &format!("decisions.{}", info.id),
                        if info.kind == "choice" {
                            format!(
                                "decision callout for choice `{}` must name one of its keys ({})",
                                info.id,
                                info.choice_keys.join(", ")
                            )
                        } else {
                            format!(
                                "decision callout for toggle `{}` must be bare (yes) or `= no`",
                                info.id
                            )
                        },
                        Some(callout.header_line),
                    )),
                }
            }
        }
    }

    for info in infos {
        let referenced = raw.iter().any(|callout| callout.id == info.id);
        if !referenced && !whole_word_mentions_body(body, &info.id) {
            let path = format!("decisions.{}", info.id);
            outcome.diagnostics.push(diagnostic(
                "warning",
                "decision-unreferenced",
                &path,
                format!(
                    "decision `{}` appears in neither a callout nor the body text",
                    info.id
                ),
                index.line_for(&path),
            ));
        }
    }

    for signal in uncovered_memory_edits(body, body_base) {
        let covered = if signal.is_init {
            infos.iter().any(|info| info.is_memory)
        } else {
            infos.iter().any(|info| {
                info.is_memory
                    && info.memory_selectors.iter().any(|selector| {
                        covers_note(
                            selector,
                            signal.note.as_deref().unwrap_or(""),
                        )
                    })
            })
        };
        if !covered {
            outcome.diagnostics.push(diagnostic(
                "warning",
                "decision-memory-uncovered",
                "",
                if signal.is_init {
                    "the body runs `sase memory init` without a covering memory decision".to_string()
                } else {
                    format!(
                        "the body edits `{}` without a covering memory decision",
                        signal.note.as_deref().unwrap_or("memory")
                    )
                },
                Some(signal.line),
            ));
        }
    }
    outcome
}

/// A memory selector covers a body note when it names the same relative path
/// or its basename.
fn covers_note(selector: &str, note: &str) -> bool {
    if selector == note {
        return true;
    }
    note.rsplit('/').next().is_some_and(|base| base == selector)
}
