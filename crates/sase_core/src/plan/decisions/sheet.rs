//! Decision Sheet, summary sentence, and implementer prompt block.
//!
//! This module implements the `sheet` phase of
//! `plan:202610/core_plan_decisions.md` (outer design Sections 1.6, 4,
//! and 6.1 items 8-9). It builds on the frozen definitions from
//! `resolver`: the sheet pairs each definition with its accepted value,
//! the summary renders the one sentence every surface shares, and the
//! prompt block renders the host-owned implementer instructions.

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use super::resolver::PlanDecisionDefinitionWire;
use super::resolver::PlanDecisionMemoryRecordWire;
use super::wire::PlanDecisionChoiceWire;
use crate::plan::wire::PlanError;

/// Verdicts `plan_decision_summary` renders.
pub const DECISION_VERDICTS: &[&str] =
    &["coder + commit", "coder", "commit", "epic launch"];

/// Summary forms `plan_decision_summary` renders.
pub const DECISION_SUMMARY_FORMS: &[&str] = &["short", "full"];

/// Audiences `plan_decisions_prompt_block` renders.
pub const DECISION_AUDIENCES: &[&str] =
    &["tale_coder", "epic_phase", "epic_land"];

/// Transports `plan_decisions_prompt_block` names in headers.
pub const DECISION_SURFACES: &[&str] = &["tui", "telegram", "mobile", "cli"];

/// One memory row's frozen authorization context.
///
/// `quote` is the authored human quote (`requested` on the frozen
/// definition). `quote_not_found` and `inherited` provenance stay
/// visible here; an unverified quote is never authorization.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanDecisionSheetMemoryWire {
    pub selectors: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resolved: Vec<PlanDecisionMemoryRecordWire>,
    pub provenance: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quote: Option<String>,
}

/// One sheet row: a frozen definition paired with its accepted value.
///
/// `default` is the review default (the definition's effective
/// default, so reset, Enter, and `%auto` agree); the authored default
/// stays on the frozen definition for diagnostics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanDecisionSheetRowWire {
    pub id: String,
    /// `toggle` or `choice`.
    pub kind: String,
    pub ask: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub why: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub choices: Vec<PlanDecisionChoiceWire>,
    pub default: JsonValue,
    pub value: JsonValue,
    pub changed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory: Option<PlanDecisionSheetMemoryWire>,
}

/// The shared reviewer output: every row in author order plus counts.
///
/// `memory_count` counts memory decisions (rows carrying `memory`),
/// whether enabled or not; `changed_count` counts rows whose value
/// differs from the review default, memory rows included.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanDecisionSheetWire {
    pub count: u64,
    pub memory_count: u64,
    pub changed_count: u64,
    pub review_revision: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rows: Vec<PlanDecisionSheetRowWire>,
}

/// Inherited epic authorization for the implementer block.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanDecisionInheritedWire {
    pub sheet: PlanDecisionSheetWire,
    pub epic_title: String,
}

/// Build the Decision Sheet from frozen definitions and an accepted
/// answer vector.
///
/// Every definition id needs exactly one answer; unknown ids,
/// missing answers, and malformed values fail rather than coercing
/// into display values. Choice answers match a full key
/// case-insensitively and resolve to the authored spelling, with no
/// prefix matching. Counts derive from the rows, including an empty
/// sheet.
pub fn plan_decision_sheet(
    definitions: &[PlanDecisionDefinitionWire],
    values: &JsonValue,
    review_revision: u64,
) -> Result<PlanDecisionSheetWire, PlanError> {
    let submitted = values.as_object().ok_or_else(|| {
        PlanError::validation(
            "decision answer vector must be an object keyed by \
             decision id",
        )
    })?;
    let known: Vec<&str> = definitions
        .iter()
        .map(|definition| definition.id.as_str())
        .collect();
    for unknown in submitted.keys() {
        if !known.contains(&unknown.as_str()) {
            return Err(PlanError::validation(format!(
                "unknown decision id \"{unknown}\" in answer vector; \
                 known decisions: {}",
                known.join(", "),
            )));
        }
    }
    let mut rows = Vec::with_capacity(definitions.len());
    for definition in definitions {
        let raw = submitted.get(&definition.id).ok_or_else(|| {
            PlanError::validation(format!(
                "missing answer for decision \"{}\" in answer vector",
                definition.id,
            ))
        })?;
        let value = canonical_sheet_value(definition, raw)?;
        let changed = value != definition.effective_default;
        let memory = definition.memory.as_ref().map(|memory| {
            PlanDecisionSheetMemoryWire {
                selectors: memory.selectors.clone(),
                resolved: definition.resolved.clone(),
                provenance: definition
                    .provenance
                    .clone()
                    .unwrap_or_else(|| "not_asked".to_string()),
                quote: definition.requested.clone(),
            }
        });
        rows.push(PlanDecisionSheetRowWire {
            id: definition.id.clone(),
            kind: definition.kind.clone(),
            ask: definition.ask.clone(),
            why: definition.why.clone(),
            choices: definition.choices.clone(),
            default: definition.effective_default.clone(),
            value,
            changed,
            memory,
        });
    }
    let memory_count =
        rows.iter().filter(|row| row.memory.is_some()).count() as u64;
    let changed_count = rows.iter().filter(|row| row.changed).count() as u64;
    Ok(PlanDecisionSheetWire {
        count: rows.len() as u64,
        memory_count,
        changed_count,
        review_revision,
        rows,
    })
}

fn canonical_sheet_value(
    definition: &PlanDecisionDefinitionWire,
    raw: &JsonValue,
) -> Result<JsonValue, PlanError> {
    let invalid = |expected: &str| {
        let allowed = allowed_sheet_values(definition);
        PlanError::validation(format!(
            "invalid answer for decision \"{}\"; expected {expected} \
             ({allowed}); got {raw}",
            definition.id,
        ))
    };
    match definition.kind.as_str() {
        "toggle" => raw
            .as_bool()
            .map(JsonValue::Bool)
            .ok_or_else(|| invalid("a JSON boolean")),
        "choice" => match raw.as_str() {
            Some(text) => definition
                .choices
                .iter()
                .find(|choice| choice.key.to_lowercase() == text.to_lowercase())
                .map(|choice| JsonValue::String(choice.key.clone()))
                .ok_or_else(|| invalid("one of its choice keys")),
            None => Err(invalid("one of its choice keys")),
        },
        kind => Err(PlanError::validation(format!(
            "unknown decision kind \"{kind}\" for \"{}\"; expected \
             toggle or choice",
            definition.id,
        ))),
    }
}

fn allowed_sheet_values(definition: &PlanDecisionDefinitionWire) -> String {
    if definition.kind == "choice" {
        let keys: Vec<&str> = definition
            .choices
            .iter()
            .map(|choice| choice.key.as_str())
            .collect();
        format!("one of {}", keys.join(", "))
    } else {
        "true, false".to_string()
    }
}

/// Render the one summary sentence every surface shares.
///
/// Full form: `→ <verdict> · <id>=<value>[ ●] · ... · 🧠 <notes>`.
/// Non-memory decisions appear in author order (toggles as `yes`/`no`,
/// choices as keys, with ` ●` where changed); memory decisions appear
/// only in the trailing clause: one ordered, deduplicated selector
/// list for enabled memory decisions, `🧠 no memory edits` when every
/// memory decision is off, and no clause when the plan has no memory
/// decisions. Short form: `defaults`, `1 change`, or `N changes`,
/// plus ` · 🧠` when any memory decision is enabled.
pub fn plan_decision_summary(
    sheet: &PlanDecisionSheetWire,
    verdict: &str,
    form: &str,
) -> Result<String, PlanError> {
    if !DECISION_VERDICTS.contains(&verdict) {
        return Err(PlanError::validation(format!(
            "unknown decision verdict \"{verdict}\"; expected coder + \
             commit, coder, commit, or epic launch"
        )));
    }
    if !DECISION_SUMMARY_FORMS.contains(&form) {
        return Err(PlanError::validation(format!(
            "unknown decision summary form \"{form}\"; expected short \
             or full"
        )));
    }
    check_sheet(sheet)?;
    if form == "short" {
        return Ok(short_summary(sheet));
    }
    let mut parts = vec![format!("→ {verdict}")];
    for row in sheet.rows.iter().filter(|row| row.memory.is_none()) {
        let mark = if row.changed { " ●" } else { "" };
        parts.push(format!("{}={}{mark}", row.id, display_sheet_value(row)?,));
    }
    let mut selectors: Vec<&str> = Vec::new();
    for row in sheet.rows.iter().filter(|row| row.memory.is_some()) {
        if row.value != JsonValue::Bool(true) {
            continue;
        }
        let memory = row.memory.as_ref().expect("memory row");
        for selector in &memory.selectors {
            if !selectors.contains(&selector.as_str()) {
                selectors.push(selector);
            }
        }
    }
    if sheet.rows.iter().any(|row| row.memory.is_some()) {
        if selectors.is_empty() {
            parts.push("🧠 no memory edits".to_string());
        } else {
            parts.push(format!("🧠 {}", selectors.join(", ")));
        }
    }
    Ok(parts.join(" · "))
}

fn short_summary(sheet: &PlanDecisionSheetWire) -> String {
    let changed = sheet.rows.iter().filter(|row| row.changed).count();
    let base = match changed {
        0 => "defaults".to_string(),
        1 => "1 change".to_string(),
        _ => format!("{changed} changes"),
    };
    let memory_on = sheet
        .rows
        .iter()
        .any(|row| row.memory.is_some() && row.value == JsonValue::Bool(true));
    if memory_on {
        format!("{base} · 🧠")
    } else {
        base
    }
}

/// Render the host-owned implementer block.
///
/// Lines use validated ids, enum keys, booleans, memory selectors,
/// and safely quoted asks/titles: frontmatter strings can never
/// introduce extra instruction lines through raw interpolation. The
/// header names the actual surface (`reviewer via ACE` for `tui`);
/// the auto header says no human reviewed the plan, and the agent
/// header identifies agent approval honestly. Unknown
/// author/transport/audience values and malformed inherited sheets
/// fail with an actionable usage error.
pub fn plan_decisions_prompt_block(
    sheet: &PlanDecisionSheetWire,
    decided_by: &str,
    decided_via: Option<&str>,
    audience: &str,
    inherited: Option<&PlanDecisionInheritedWire>,
) -> Result<String, PlanError> {
    if !matches!(decided_by, "reviewer" | "auto" | "agent") {
        return Err(PlanError::validation(format!(
            "unknown decided_by \"{decided_by}\"; expected reviewer, \
             auto, or agent"
        )));
    }
    if let Some(via) = decided_via {
        if !DECISION_SURFACES.contains(&via) {
            return Err(PlanError::validation(format!(
                "unknown decided_via \"{via}\"; expected tui, telegram, \
                 mobile, or cli"
            )));
        }
        if decided_by == "auto" {
            return Err(PlanError::validation(
                "decided_via must be absent when decided_by is auto",
            ));
        }
    } else if decided_by != "auto" {
        return Err(PlanError::validation(format!(
            "decided_via names the review surface; {decided_by} \
             decisions need one of tui, telegram, mobile, or cli"
        )));
    }
    if !DECISION_AUDIENCES.contains(&audience) {
        return Err(PlanError::validation(format!(
            "unknown decision audience \"{audience}\"; expected \
             tale_coder, epic_phase, or epic_land"
        )));
    }
    check_sheet(sheet)?;
    if let Some(grants) = inherited {
        // Titles stay safely quoted by `quoted()`, so newlines flatten
        // rather than introducing extra instruction lines.
        if grants.epic_title.trim().is_empty() {
            return Err(PlanError::validation(
                "inherited decisions need a non-empty epic title",
            ));
        }
        check_sheet(&grants.sheet)?;
    }

    let mut lines = vec![block_header(decided_by, decided_via)];
    for row in &sheet.rows {
        lines.push(block_row(row, decided_by, audience)?);
    }
    if let Some(grants) = inherited {
        for row in &grants.sheet.rows {
            lines.push(inherited_line(&grants.epic_title, row)?);
        }
    }
    let local_memory = sheet.rows.iter().any(|row| row.memory.is_some());
    let inherited_memory = inherited
        .map(|grants| grants.sheet.rows.iter().any(|row| row.memory.is_some()))
        .unwrap_or(false);
    if local_memory || inherited_memory {
        lines.push("No other memory note may be edited.".to_string());
    }
    lines.push("Implement only the branches selected above.".to_string());
    Ok(lines.join("\n"))
}

fn block_header(decided_by: &str, decided_via: Option<&str>) -> String {
    match decided_by {
        "reviewer" => format!(
            "Reviewer decisions for this plan (final · reviewer via \
             {}):",
            display_surface(decided_via.expect("reviewer needs via")),
        ),
        "auto" => "Auto decisions for this plan (final · no human \
             reviewed this plan):"
            .to_string(),
        _ => format!(
            "Agent decisions for this plan (final · approved by an \
             agent via {}):",
            display_surface(decided_via.expect("agent needs via")),
        ),
    }
}

fn display_surface(via: &str) -> &str {
    match via {
        "tui" => "ACE",
        "telegram" => "Telegram",
        "cli" => "CLI",
        _ => "mobile",
    }
}

fn block_row(
    row: &PlanDecisionSheetRowWire,
    decided_by: &str,
    audience: &str,
) -> Result<String, PlanError> {
    let ask = quoted(&row.ask);
    if let Some(memory) = row.memory.as_ref() {
        let selectors = memory.selectors.join(", ");
        if row.value == JsonValue::Bool(true) {
            return Ok(format!(
                "- {} = yes 🧠. Memory edits are authorized for \
                 {selectors} only. Context: {ask}.",
                row.id,
            ));
        }
        let mut line = format!(
            "- {} = no. Do not edit {selectors}. Context: {ask}.",
            row.id,
        );
        // Only an unrequested memory change left off without human review
        // files skipped work. Verified requested (`asked`) or inherited rows
        // left off, and every human-declined row, stay quiet.
        let unrequested = matches!(
            memory.provenance.as_str(),
            "not_asked" | "quote_not_found"
        );
        if decided_by == "auto" && unrequested {
            if audience == "tale_coder" {
                line.push_str(
                    " Record the skipped memory change with \
                     /sase_new_task.",
                );
            } else {
                line.push_str(
                    " Record a PROPOSED FOLLOW-UP: for the skipped \
                     memory change on the assigned bead.",
                );
            }
        }
        return Ok(line);
    }
    let value = display_sheet_value(row)?;
    let default = display_json_scalar(&row.default);
    if row.kind == "choice" {
        let excluded: Vec<String> = row
            .choices
            .iter()
            .filter(|choice| JsonValue::String(choice.key.clone()) != row.value)
            .map(|choice| format!("\"{} = {}\"", row.id, choice.key))
            .collect();
        return Ok(format!(
            "- {id} = {value} (planner default: {default}). Implement \
             the \"{id} = {value}\" branch; ignore {excluded}. \
             Context: {ask}.",
            id = row.id,
            excluded = excluded.join(", "),
        ));
    }
    let other = if value == "yes" { "no" } else { "yes" };
    Ok(format!(
        "- {id} = {value} (planner default: {default}). Implement the \
         \"{id} = {value}\" branch; ignore \"{id} = {other}\". \
         Context: {ask}.",
        id = row.id,
    ))
}

fn inherited_line(
    epic_title: &str,
    row: &PlanDecisionSheetRowWire,
) -> Result<String, PlanError> {
    let title = quoted(epic_title);
    let mut line = format!(
        "Inherited from epic {title}: {} = {}",
        row.id,
        display_sheet_value(row)?,
    );
    if let Some(memory) = row.memory.as_ref() {
        line.push_str(&format!(" 🧠 ({})", memory.selectors.join(", ")));
    }
    line.push('.');
    Ok(line)
}

/// Quote a frontmatter string for display: flatten newlines so it can
/// never introduce extra instruction lines, and escape quotes.
fn quoted(text: &str) -> String {
    let flat = text.replace("\r\n", " ").replace(['\r', '\n'], " ");
    let escaped = flat.replace('\\', "\\\\").replace('"', "\\\"");
    format!("\"{escaped}\"")
}

fn display_sheet_value(
    row: &PlanDecisionSheetRowWire,
) -> Result<String, PlanError> {
    match row.kind.as_str() {
        "toggle" => row.value.as_bool().map(bool_word).ok_or_else(|| {
            PlanError::validation(format!(
                "invalid sheet value for toggle \"{}\"; expected a JSON \
                 boolean",
                row.id,
            ))
        }),
        "choice" => row.value.as_str().map(str::to_string).ok_or_else(|| {
            PlanError::validation(format!(
                "invalid sheet value for choice \"{}\"; expected a \
                     choice key",
                row.id,
            ))
        }),
        kind => Err(PlanError::validation(format!(
            "unknown decision kind \"{kind}\" for \"{}\"; expected \
             toggle or choice",
            row.id,
        ))),
    }
    .and_then(|value| {
        if row.kind == "choice"
            && !row.choices.iter().any(|choice| choice.key == value)
        {
            return Err(PlanError::validation(format!(
                "unknown choice key \"{value}\" for \"{}\"; expected \
                 one of {}",
                row.id,
                row.choices
                    .iter()
                    .map(|choice| choice.key.as_str())
                    .collect::<Vec<_>>()
                    .join(", "),
            )));
        }
        Ok(value)
    })
}

fn is_one_line(text: &str) -> bool {
    !text.contains('\n') && !text.contains('\r')
}

fn check_sheet_row(row: &PlanDecisionSheetRowWire) -> Result<(), PlanError> {
    if row.id.trim().is_empty() || !is_one_line(&row.id) {
        return Err(PlanError::validation(format!(
            "invalid decision id {:?}; ids must be non-empty one-line text",
            row.id,
        )));
    }
    if !matches!(row.kind.as_str(), "toggle" | "choice") {
        return Err(PlanError::validation(format!(
            "unknown decision kind {:?} for {:?}; expected toggle or choice",
            row.kind, row.id,
        )));
    }
    // Value shape also checks the kind and, for choices, key membership.
    display_sheet_value(row)?;
    // Default shape mirrors the value shape so contradictory wire state
    // cannot render a review default no surface could have shown.
    match row.kind.as_str() {
        "toggle" => {
            if row.default.as_bool().is_none() {
                return Err(PlanError::validation(format!(
                    "invalid sheet default for toggle {:?}; expected a JSON boolean, got {}",
                    row.id, row.default,
                )));
            }
        }
        _ => match row.default.as_str() {
            Some(default) => {
                if !row.choices.iter().any(|choice| choice.key == default) {
                    return Err(PlanError::validation(format!(
                            "unknown sheet default {:?} for {:?}; expected one of {}",
                            default,
                            row.id,
                            row.choices
                                .iter()
                                .map(|choice| choice.key.as_str())
                                .collect::<Vec<_>>()
                                .join(", "),
                        )));
                }
            }
            None => {
                return Err(PlanError::validation(format!(
                        "invalid sheet default for choice {:?}; expected one of its choice keys, got {}",
                        row.id, row.default,
                    )));
            }
        },
    }
    for choice in &row.choices {
        if choice.key.trim().is_empty() || !is_one_line(&choice.key) {
            return Err(PlanError::validation(format!(
                "invalid choice key {:?} for {:?}; keys must be non-empty one-line text",
                choice.key, row.id,
            )));
        }
    }
    if let Some(memory) = row.memory.as_ref() {
        if row.kind != "toggle" {
            return Err(PlanError::validation(format!(
                "memory is allowed on toggles only; decision {:?} is {:?}",
                row.id, row.kind,
            )));
        }
        if row.value.as_bool().is_none() || row.default.as_bool().is_none() {
            return Err(PlanError::validation(format!(
                "invalid memory row {:?}; memory decisions must be boolean toggles",
                row.id,
            )));
        }
        if memory.selectors.is_empty() {
            return Err(PlanError::validation(format!(
                "missing memory selectors for decision {:?}",
                row.id,
            )));
        }
        for selector in &memory.selectors {
            if selector.trim().is_empty() || !is_one_line(selector) {
                return Err(PlanError::validation(format!(
                    "invalid memory selector {:?} for {:?}; selectors must be non-empty one-line text",
                    selector, row.id,
                )));
            }
        }
        match memory.provenance.as_str() {
            "asked" | "not_asked" | "quote_not_found" | "inherited" => {}
            other => {
                return Err(PlanError::validation(format!(
                    "unknown memory provenance {:?} for {:?}; expected asked, not_asked, quote_not_found, or inherited",
                    other, row.id,
                )));
            }
        }
    }
    let expected_changed = row.value != row.default;
    if row.changed != expected_changed {
        return Err(PlanError::validation(format!(
            "contradictory changed flag for {:?}; value {} {} default {}",
            row.id,
            row.value,
            if expected_changed {
                "differs from"
            } else {
                "equals"
            },
            row.default,
        )));
    }
    Ok(())
}

fn check_sheet(sheet: &PlanDecisionSheetWire) -> Result<(), PlanError> {
    if sheet.count != sheet.rows.len() as u64 {
        return Err(PlanError::validation(format!(
            "contradictory sheet count {}; rows carry {} decisions",
            sheet.count,
            sheet.rows.len(),
        )));
    }
    let memory_count =
        sheet.rows.iter().filter(|row| row.memory.is_some()).count() as u64;
    if sheet.memory_count != memory_count {
        return Err(PlanError::validation(format!(
            "contradictory sheet memory_count {}; rows carry {memory_count} memory decisions",
            sheet.memory_count,
        )));
    }
    let changed_count =
        sheet.rows.iter().filter(|row| row.changed).count() as u64;
    if sheet.changed_count != changed_count {
        return Err(PlanError::validation(format!(
            "contradictory sheet changed_count {}; rows carry {changed_count} changed decisions",
            sheet.changed_count,
        )));
    }
    for row in &sheet.rows {
        check_sheet_row(row)?;
    }
    Ok(())
}

fn bool_word(flag: bool) -> String {
    if flag {
        "yes".to_string()
    } else {
        "no".to_string()
    }
}

fn display_json_scalar(value: &JsonValue) -> String {
    match value {
        JsonValue::Bool(flag) => bool_word(*flag),
        JsonValue::String(text) => text.clone(),
        _ => value.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::decisions::PlanDecisionMemoryWire;
    use serde_json::json;

    fn choice_definition() -> PlanDecisionDefinitionWire {
        PlanDecisionDefinitionWire {
            id: "grouping".to_string(),
            kind: "choice".to_string(),
            ask: "How should the overlay group bindings?".to_string(),
            why: Some("Keeps order".to_string()),
            choices: vec![
                PlanDecisionChoiceWire {
                    key: "pane".to_string(),
                    label: "By pane".to_string(),
                },
                PlanDecisionChoiceWire {
                    key: "mode".to_string(),
                    label: "By mode".to_string(),
                },
            ],
            default: json!("pane"),
            effective_default: json!("pane"),
            memory: None,
            requested: None,
            provenance: None,
            requested_verified: false,
            resolved: Vec::new(),
        }
    }

    fn memory_record() -> PlanDecisionMemoryRecordWire {
        PlanDecisionMemoryRecordWire {
            selector: "tui.md".to_string(),
            kind: "note".to_string(),
            scope: "project".to_string(),
            path: "sase/memory/tui.md".to_string(),
            record_type: "reference".to_string(),
            exists: true,
            strands: None,
        }
    }

    fn memory_definition() -> PlanDecisionDefinitionWire {
        PlanDecisionDefinitionWire {
            id: "tui_note".to_string(),
            kind: "toggle".to_string(),
            ask: "Record the overlay conventions in the tui note?".to_string(),
            why: None,
            choices: Vec::new(),
            default: json!(true),
            effective_default: json!(true),
            memory: Some(PlanDecisionMemoryWire {
                selectors: vec!["tui.md".to_string()],
            }),
            requested: Some(
                "and note the convention in the tui memory".to_string(),
            ),
            provenance: Some("asked".to_string()),
            requested_verified: true,
            resolved: vec![memory_record()],
        }
    }

    fn toggle_definition() -> PlanDecisionDefinitionWire {
        PlanDecisionDefinitionWire {
            id: "notify".to_string(),
            kind: "toggle".to_string(),
            ask: "Send a notification?".to_string(),
            why: None,
            choices: Vec::new(),
            default: json!(false),
            effective_default: json!(false),
            memory: None,
            requested: None,
            provenance: None,
            requested_verified: false,
            resolved: Vec::new(),
        }
    }

    fn definitions() -> Vec<PlanDecisionDefinitionWire> {
        vec![
            choice_definition(),
            memory_definition(),
            toggle_definition(),
        ]
    }

    fn accepted_values() -> JsonValue {
        json!({"grouping": "mode", "tui_note": true, "notify": false})
    }

    fn sheet() -> PlanDecisionSheetWire {
        plan_decision_sheet(&definitions(), &accepted_values(), 4).unwrap()
    }

    #[test]
    fn sheet_pairs_values_with_effective_defaults() {
        let sheet = sheet();
        assert_eq!(sheet.count, 3);
        assert_eq!(sheet.memory_count, 1);
        assert_eq!(sheet.changed_count, 1);
        assert_eq!(sheet.review_revision, 4);
        assert_eq!(sheet.rows[0].default, json!("pane"));
        assert_eq!(sheet.rows[0].value, json!("mode"));
        assert!(sheet.rows[0].changed);
        assert_eq!(sheet.rows[2].value, json!(false));
        assert!(!sheet.rows[2].changed);
        // The frozen authored default is not the review default, and
        // the memory row keeps its resolved identities and quote.
        let memory = sheet.rows[1].memory.as_ref().unwrap();
        assert_eq!(memory.selectors, vec!["tui.md".to_string()]);
        assert_eq!(memory.resolved, vec![memory_record()]);
        assert_eq!(memory.provenance, "asked");
        assert_eq!(
            memory.quote,
            Some("and note the convention in the tui memory".to_string()),
        );
    }

    #[test]
    fn sheet_clamps_unverified_memory_but_keeps_authored_default() {
        let mut memory = memory_definition();
        memory.effective_default = json!(false);
        memory.requested_verified = false;
        memory.provenance = Some("quote_not_found".to_string());
        let sheet =
            plan_decision_sheet(&[memory], &json!({"tui_note": false}), 7)
                .unwrap();
        assert_eq!(sheet.rows[0].default, json!(false));
        assert!(!sheet.rows[0].changed);
        assert_eq!(sheet.changed_count, 0);
        let frozen = sheet.rows[0].memory.as_ref().unwrap();
        assert_eq!(frozen.provenance, "quote_not_found");
    }

    #[test]
    fn sheet_canonicalizes_choice_keys_case_insensitively() {
        let sheet = plan_decision_sheet(
            &[choice_definition()],
            &json!({"grouping": "MODE"}),
            1,
        )
        .unwrap();
        assert_eq!(sheet.rows[0].value, json!("mode"));
        assert!(sheet.rows[0].changed);
    }

    #[test]
    fn sheet_rejects_malformed_vectors_instead_of_coercing() {
        let definitions = definitions();
        // Missing answers fail.
        plan_decision_sheet(&definitions, &json!({}), 1)
            .expect_err("missing answers must fail");
        // Unknown ids fail.
        plan_decision_sheet(
            &definitions,
            &json!({
                "grouping": "mode",
                "tui_note": true,
                "notify": false,
                "extra": true,
            }),
            1,
        )
        .expect_err("unknown ids must fail");
        // Non-objects fail.
        plan_decision_sheet(&definitions, &json!([]), 1)
            .expect_err("non-object vectors must fail");
        // Toggle strings and unknown choice keys fail.
        plan_decision_sheet(
            &definitions,
            &json!({
                "grouping": "mode",
                "tui_note": "yes",
                "notify": false,
            }),
            1,
        )
        .expect_err("toggle strings must fail");
        plan_decision_sheet(
            &definitions,
            &json!({
                "grouping": "split",
                "tui_note": true,
                "notify": false,
            }),
            1,
        )
        .expect_err("unknown choice keys must fail");
        // Unknown definition kinds fail.
        let mut bad = choice_definition();
        bad.kind = "freetext".to_string();
        plan_decision_sheet(&[bad], &json!({"grouping": "x"}), 1)
            .expect_err("unknown kinds must fail");
    }

    #[test]
    fn empty_sheet_counts_zero_but_keeps_revision() {
        let sheet = plan_decision_sheet(&[], &json!({}), 9).unwrap();
        assert_eq!(sheet.count, 0);
        assert_eq!(sheet.memory_count, 0);
        assert_eq!(sheet.changed_count, 0);
        assert_eq!(sheet.review_revision, 9);
        assert!(sheet.rows.is_empty());
    }

    #[test]
    fn summary_full_matches_the_design_sentence() {
        let summary =
            plan_decision_summary(&sheet(), "coder + commit", "full").unwrap();
        assert_eq!(
            summary,
            "→ coder + commit · grouping=mode ● · notify=no · 🧠 tui.md",
        );
    }

    #[test]
    fn summary_full_renders_every_verdict() {
        let sheet = sheet();
        assert_eq!(
            plan_decision_summary(&sheet, "coder", "full").unwrap(),
            "→ coder · grouping=mode ● · notify=no · 🧠 tui.md",
        );
        assert_eq!(
            plan_decision_summary(&sheet, "commit", "full").unwrap(),
            "→ commit · grouping=mode ● · notify=no · 🧠 tui.md",
        );
        assert_eq!(
            plan_decision_summary(&sheet, "epic launch", "full").unwrap(),
            "→ epic launch · grouping=mode ● · notify=no · 🧠 tui.md",
        );
    }

    #[test]
    fn summary_full_names_declined_memory_honestly() {
        let sheet = plan_decision_sheet(
            &definitions(),
            &json!({
                "grouping": "pane",
                "tui_note": false,
                "notify": false,
            }),
            2,
        )
        .unwrap();
        assert_eq!(
            plan_decision_summary(&sheet, "coder", "full").unwrap(),
            "→ coder · grouping=pane · notify=no · 🧠 no memory edits",
        );
    }

    #[test]
    fn summary_full_omits_the_clause_without_memory_decisions() {
        let sheet = plan_decision_sheet(
            &[choice_definition(), toggle_definition()],
            &json!({"grouping": "pane", "notify": true}),
            2,
        )
        .unwrap();
        assert_eq!(
            plan_decision_summary(&sheet, "commit", "full").unwrap(),
            "→ commit · grouping=pane · notify=yes ●",
        );
        assert_eq!(
            plan_decision_summary(&sheet, "commit", "short").unwrap(),
            "1 change",
        );
    }

    #[test]
    fn summary_full_dedupes_memory_selectors_in_order() {
        let mut second = memory_definition();
        second.id = "glossary_note".to_string();
        second.memory = Some(PlanDecisionMemoryWire {
            selectors: vec![
                "tui.md".to_string(),
                "glossary:stitch".to_string(),
            ],
        });
        let sheet = plan_decision_sheet(
            &[memory_definition(), second],
            &json!({"tui_note": true, "glossary_note": true}),
            3,
        )
        .unwrap();
        assert_eq!(sheet.memory_count, 2);
        assert_eq!(
            plan_decision_summary(&sheet, "coder + commit", "full").unwrap(),
            "→ coder + commit · 🧠 tui.md, glossary:stitch",
        );
    }

    #[test]
    fn summary_short_counts_memory_changes_too() {
        let sheet = sheet();
        assert_eq!(
            plan_decision_summary(&sheet, "coder", "short").unwrap(),
            "1 change · 🧠",
        );
        // Everything on its review default and no memory enabled.
        let mut clamped = memory_definition();
        clamped.effective_default = json!(false);
        clamped.requested_verified = false;
        clamped.provenance = Some("not_asked".to_string());
        let calm = plan_decision_sheet(
            &[choice_definition(), clamped, toggle_definition()],
            &json!({
                "grouping": "pane",
                "tui_note": false,
                "notify": false,
            }),
            2,
        )
        .unwrap();
        assert_eq!(calm.changed_count, 0);
        assert_eq!(
            plan_decision_summary(&calm, "coder", "short").unwrap(),
            "defaults",
        );
        // Two rows off their review defaults, one of them memory:
        // memory changes count toward changed_count too.
        let mut clamped = memory_definition();
        clamped.effective_default = json!(false);
        clamped.requested_verified = false;
        clamped.provenance = Some("quote_not_found".to_string());
        let working = plan_decision_sheet(
            &[choice_definition(), clamped, toggle_definition()],
            &json!({
                "grouping": "mode",
                "tui_note": true,
                "notify": false,
            }),
            2,
        )
        .unwrap();
        assert_eq!(working.changed_count, 2);
        assert_eq!(
            plan_decision_summary(&working, "coder", "short").unwrap(),
            "2 changes · 🧠",
        );
    }

    #[test]
    fn summary_rejects_unknown_verdicts_and_forms() {
        let sheet = sheet();
        plan_decision_summary(&sheet, "launch", "full")
            .expect_err("unknown verdicts must fail, never interpolate");
        plan_decision_summary(&sheet, "coder", "long")
            .expect_err("unknown forms must fail");
    }

    #[test]
    fn block_reviewer_names_branches_and_memory_grants() {
        let block = plan_decisions_prompt_block(
            &sheet(),
            "reviewer",
            Some("tui"),
            "tale_coder",
            None,
        )
        .unwrap();
        assert_eq!(
            block,
            "Reviewer decisions for this plan (final · reviewer via \
             ACE):\n\
             - grouping = mode (planner default: pane). Implement the \
             \"grouping = mode\" branch; ignore \"grouping = pane\". \
             Context: \"How should the overlay group bindings?\".\n\
             - tui_note = yes 🧠. Memory edits are authorized for tui.md \
             only. Context: \"Record the overlay conventions in the tui \
             note?\".\n\
             - notify = no (planner default: no). Implement the \
             \"notify = no\" branch; ignore \"notify = yes\". Context: \
             \"Send a notification?\".\n\
             No other memory note may be edited.\n\
             Implement only the branches selected above.",
        );
    }

    #[test]
    fn block_headers_name_every_surface_and_caller_honestly() {
        let sheet = sheet();
        let header = |block: &str| block.lines().next().unwrap().to_string();
        assert_eq!(
            header(
                &plan_decisions_prompt_block(
                    &sheet,
                    "reviewer",
                    Some("telegram"),
                    "tale_coder",
                    None,
                )
                .unwrap(),
            ),
            "Reviewer decisions for this plan (final · reviewer via \
             Telegram):",
        );
        assert_eq!(
            header(
                &plan_decisions_prompt_block(
                    &sheet,
                    "reviewer",
                    Some("mobile"),
                    "epic_phase",
                    None,
                )
                .unwrap(),
            ),
            "Reviewer decisions for this plan (final · reviewer via \
             mobile):",
        );
        assert_eq!(
            header(
                &plan_decisions_prompt_block(
                    &sheet,
                    "reviewer",
                    Some("cli"),
                    "epic_land",
                    None,
                )
                .unwrap(),
            ),
            "Reviewer decisions for this plan (final · reviewer via \
             CLI):",
        );
        assert_eq!(
            header(
                &plan_decisions_prompt_block(
                    &sheet,
                    "auto",
                    None,
                    "tale_coder",
                    None,
                )
                .unwrap(),
            ),
            "Auto decisions for this plan (final · no human reviewed \
             this plan):",
        );
        assert_eq!(
            header(
                &plan_decisions_prompt_block(
                    &sheet,
                    "agent",
                    Some("tui"),
                    "tale_coder",
                    None,
                )
                .unwrap(),
            ),
            "Agent decisions for this plan (final · approved by an \
             agent via ACE):",
        );
    }

    #[test]
    fn block_routes_declined_memory_follow_ups_by_audience() {
        let mut memory = memory_definition();
        memory.effective_default = json!(false);
        memory.provenance = Some("quote_not_found".to_string());
        memory.requested_verified = false;
        let sheet =
            plan_decision_sheet(&[memory], &json!({"tui_note": false}), 5)
                .unwrap();
        let coder = plan_decisions_prompt_block(
            &sheet,
            "auto",
            None,
            "tale_coder",
            None,
        )
        .unwrap();
        assert!(
            coder.contains("/sase_new_task"),
            "coder audience files a task: {coder}",
        );
        assert!(!coder.contains("PROPOSED FOLLOW-UP"));
        for audience in ["epic_phase", "epic_land"] {
            let block = plan_decisions_prompt_block(
                &sheet, "auto", None, audience, None,
            )
            .unwrap();
            assert!(
                block.contains("PROPOSED FOLLOW-UP:"),
                "{audience} records a follow-up: {block}",
            );
            assert!(!block.contains("/sase_new_task"));
        }
        // A human-declined change gets no follow-up instruction.
        let declined = plan_decisions_prompt_block(
            &sheet,
            "reviewer",
            Some("cli"),
            "tale_coder",
            None,
        )
        .unwrap();
        assert!(!declined.contains("/sase_new_task"));
        assert!(!declined.contains("PROPOSED FOLLOW-UP"));
        assert!(declined.contains("Do not edit tui.md."));
    }

    #[test]
    fn block_carries_inherited_authorization_without_local_rows() {
        let granted = plan_decision_sheet(
            &[memory_definition()],
            &json!({"tui_note": true}),
            11,
        )
        .unwrap();
        let empty = plan_decision_sheet(&[], &json!({}), 12).unwrap();
        let block = plan_decisions_prompt_block(
            &empty,
            "reviewer",
            Some("tui"),
            "epic_phase",
            Some(&PlanDecisionInheritedWire {
                sheet: granted,
                epic_title: "Keymap help overlay".to_string(),
            }),
        )
        .unwrap();
        assert_eq!(
            block,
            "Reviewer decisions for this plan (final · reviewer via \
             ACE):\n\
             Inherited from epic \"Keymap help overlay\": tui_note = yes \
             🧠 (tui.md).\n\
             No other memory note may be edited.\n\
             Implement only the branches selected above.",
        );
    }

    #[test]
    fn block_rejects_unknown_enums_and_malformed_inherited() {
        let sheet = sheet();
        plan_decisions_prompt_block(
            &sheet,
            "owner",
            Some("tui"),
            "tale_coder",
            None,
        )
        .expect_err("unknown authors must fail");
        plan_decisions_prompt_block(
            &sheet,
            "reviewer",
            Some("pager"),
            "tale_coder",
            None,
        )
        .expect_err("unknown transports must fail");
        plan_decisions_prompt_block(
            &sheet,
            "auto",
            Some("tui"),
            "tale_coder",
            None,
        )
        .expect_err("auto never carries a transport");
        plan_decisions_prompt_block(
            &sheet,
            "reviewer",
            None,
            "tale_coder",
            None,
        )
        .expect_err("reviewers need a named surface");
        plan_decisions_prompt_block(
            &sheet,
            "reviewer",
            Some("tui"),
            "tablet",
            None,
        )
        .expect_err("unknown audiences must fail");
        let granted = plan_decision_sheet(
            &[memory_definition()],
            &json!({"tui_note": true}),
            11,
        )
        .unwrap();
        plan_decisions_prompt_block(
            &sheet,
            "reviewer",
            Some("tui"),
            "tale_coder",
            Some(&PlanDecisionInheritedWire {
                sheet: granted,
                epic_title: "  ".to_string(),
            }),
        )
        .expect_err("blank epic titles must fail");
    }

    #[test]
    fn block_quotes_frontmatter_text_without_new_lines() {
        let mut choice = choice_definition();
        choice.ask = "Group \"now\"\nby pane?".to_string();
        let sheet =
            plan_decision_sheet(&[choice], &json!({"grouping": "pane"}), 1)
                .unwrap();
        let granted = plan_decision_sheet(
            &[memory_definition()],
            &json!({"tui_note": true}),
            11,
        )
        .unwrap();
        let block = plan_decisions_prompt_block(
            &sheet,
            "reviewer",
            Some("cli"),
            "tale_coder",
            Some(&PlanDecisionInheritedWire {
                sheet: granted,
                epic_title: "Epic\n\"quoted\"".to_string(),
            }),
        )
        .unwrap();
        for line in block.lines().skip(1) {
            assert!(
                !line.contains('\n'),
                "one source line renders as one output line"
            );
        }
        assert!(block.contains("\\\"now\\\""));
        assert!(block.contains("Epic \\\"quoted\\\""));
    }

    #[test]
    fn sheet_row_keeps_unverified_quotes_out_of_authorization() {
        // The sheet carries the provenance; it never upgrades the
        // quote into an authorization on its own.
        let mut memory = memory_definition();
        memory.requested_verified = false;
        memory.effective_default = json!(false);
        memory.provenance = Some("quote_not_found".to_string());
        let sheet =
            plan_decision_sheet(&[memory], &json!({"tui_note": false}), 1)
                .unwrap();
        let frozen = sheet.rows[0].memory.as_ref().unwrap();
        assert_eq!(frozen.provenance, "quote_not_found");
        assert!(!sheet.rows[0].changed);
        assert_eq!(
            plan_decision_summary(&sheet, "coder", "full").unwrap(),
            "→ coder · 🧠 no memory edits",
        );
    }

    #[test]
    fn sheet_rejects_unknown_provenance_kinds_and_shapes() {
        // Unknown provenance fails on both summary and prompt paths.
        let mut bad = sheet();
        bad.rows[1].memory.as_mut().unwrap().provenance = "typed".to_string();
        plan_decision_summary(&bad, "coder", "full")
            .expect_err("unknown provenance must fail");
        plan_decisions_prompt_block(
            &bad,
            "reviewer",
            Some("tui"),
            "tale_coder",
            None,
        )
        .expect_err("unknown provenance must fail");
        // Malformed inherited memory rows fail too.
        let granted = sheet();
        let mut inherited_bad = granted.clone();
        inherited_bad.rows[1].memory.as_mut().unwrap().provenance =
            "typed".to_string();
        let empty = plan_decision_sheet(&[], &json!({}), 1).unwrap();
        plan_decisions_prompt_block(
            &empty,
            "reviewer",
            Some("tui"),
            "epic_phase",
            Some(&PlanDecisionInheritedWire {
                sheet: inherited_bad,
                epic_title: "Epic".to_string(),
            }),
        )
        .expect_err("malformed inherited rows must fail");
        // Invalid kinds fail.
        let mut kind_bad = sheet();
        kind_bad.rows[0].kind = "freetext".to_string();
        plan_decision_summary(&kind_bad, "coder", "full")
            .expect_err("unknown kinds must fail");
        // Memory rows must be boolean toggles with selectors.
        let mut toggle_bad = sheet();
        toggle_bad.rows[1].value = json!("yes");
        plan_decision_summary(&toggle_bad, "coder", "full")
            .expect_err("toggle strings must fail");
        let mut selector_bad = sheet();
        selector_bad.rows[1]
            .memory
            .as_mut()
            .unwrap()
            .selectors
            .clear();
        plan_decision_summary(&selector_bad, "coder", "full")
            .expect_err("empty selectors must fail");
        let mut newline_bad = sheet();
        newline_bad.rows[1].memory.as_mut().unwrap().selectors =
            vec!["bad\nselector".to_string()];
        plan_decision_summary(&newline_bad, "coder", "full")
            .expect_err("multiline selectors must fail");
        let mut id_bad = sheet();
        id_bad.rows[0].id = "bad\nid".to_string();
        plan_decision_summary(&id_bad, "coder", "full")
            .expect_err("multiline ids must fail");
        // Contradictory counts and changed flags fail instead of rendering.
        let mut count_bad = sheet();
        count_bad.count = 99;
        plan_decision_summary(&count_bad, "coder", "full")
            .expect_err("contradictory counts must fail");
        let mut changed_bad = sheet();
        changed_bad.rows[0].changed = !changed_bad.rows[0].changed;
        plan_decision_summary(&changed_bad, "coder", "full")
            .expect_err("contradictory changed flags must fail");
    }

    #[test]
    fn auto_follow_ups_only_for_unrequested_memory_off() {
        for (provenance, files_task) in [
            ("not_asked", true),
            ("quote_not_found", true),
            ("asked", false),
            ("inherited", false),
        ] {
            let mut memory = memory_definition();
            memory.effective_default = json!(false);
            memory.provenance = Some(provenance.to_string());
            memory.requested_verified = provenance == "asked";
            let sheet =
                plan_decision_sheet(&[memory], &json!({"tui_note": false}), 5)
                    .unwrap();
            for audience in ["tale_coder", "epic_phase", "epic_land"] {
                let block = plan_decisions_prompt_block(
                    &sheet, "auto", None, audience, None,
                )
                .unwrap();
                let has_task = block.contains("/sase_new_task")
                    || block.contains("PROPOSED FOLLOW-UP:");
                assert_eq!(
                    has_task, files_task,
                    "{provenance}/{audience}: {block}",
                );
                if files_task {
                    if audience == "tale_coder" {
                        assert!(block.contains("/sase_new_task"));
                    } else {
                        assert!(block.contains("PROPOSED FOLLOW-UP:"));
                    }
                }
            }
            // Human-declined rows never file, whatever the provenance.
            let quiet = plan_decisions_prompt_block(
                &sheet,
                "reviewer",
                Some("cli"),
                "tale_coder",
                None,
            )
            .unwrap();
            assert!(!quiet.contains("/sase_new_task"));
            assert!(!quiet.contains("PROPOSED FOLLOW-UP:"));
        }
    }
}
