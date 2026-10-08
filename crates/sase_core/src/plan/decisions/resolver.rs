//! Frozen decision definitions, digest, and strict value resolution.
//!
//! This module implements the `resolve` phase of
//! `plan:202610/core_plan_decisions.md` (outer design Sections 3 and 4).
//! The Python host supplies resolved memory identities and human-authored
//! verification facts; Rust freezes them into per-review definitions,
//! digests the frozen vector for the edit freeze, and resolves one accepted
//! answer vector with the agent memory boundary enforced.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use super::wire::{PlanDecisionChoiceWire, PlanDecisionMemoryWire};
use crate::plan::validate::ValidatedPlanWire;
use crate::plan::wire::PlanError;

/// Provenance values the host may report for a memory decision.
pub const DECISION_PROVENANCES: &[&str] =
    &["asked", "not_asked", "quote_not_found", "inherited"];

/// One host-resolved memory record frozen into a decision definition.
///
/// The host resolves these records; Rust never discovers or reads memory
/// files. The optional frozen strand list and all durable scope/path
/// identities are preserved verbatim.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanDecisionMemoryRecordWire {
    pub selector: String,
    /// `note`, `web`, or `strand`.
    pub kind: String,
    /// `project` or `home`.
    pub scope: String,
    pub path: String,
    /// `core`, `reference`, `web`, or `strand`.
    #[serde(rename = "type")]
    pub record_type: String,
    pub exists: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub strands: Option<Vec<String>>,
}

/// Typed host facts for one decision, keyed by decision id.
///
/// A memory decision authored with default true has effective default false
/// unless `requested_verified` is true. Missing verification facts fail
/// closed. Inherited authorization arrives here as trusted host context,
/// never inferred from quote text or a submitted answer.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanDecisionHostFactWire {
    #[serde(default)]
    pub requested_verified: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provenance: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resolved: Vec<PlanDecisionMemoryRecordWire>,
}

/// One frozen decision definition for a review.
///
/// System-written answers are not question definitions, so no answer field
/// participates here. The authored default is preserved for explanation
/// while `effective_default` drives review, resolution, and display.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanDecisionDefinitionWire {
    pub id: String,
    /// `toggle` or `choice`.
    pub kind: String,
    pub ask: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub why: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub choices: Vec<PlanDecisionChoiceWire>,
    /// The authored default: a YAML boolean for toggles, a choice key.
    pub default: JsonValue,
    /// The review default after the unverified-memory clamp.
    pub effective_default: JsonValue,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory: Option<PlanDecisionMemoryWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested: Option<String>,
    /// Verification provenance for memory decisions; absent otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provenance: Option<String>,
    #[serde(default)]
    pub requested_verified: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resolved: Vec<PlanDecisionMemoryRecordWire>,
}

/// One resolved row: the canonical value, where it came from, and whether
/// it changed from the effective review default.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanDecisionResolveRowWire {
    pub id: String,
    pub value: JsonValue,
    /// `default`, `submitted`, or `clamped`.
    pub source: String,
    pub changed: bool,
}

/// One resolution failure. Every error carries the allowed values and the
/// effective default so surfaces can render `★` without another call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanDecisionResolveErrorWire {
    pub id: String,
    pub code: String,
    pub message: String,
    pub allowed: Vec<JsonValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<JsonValue>,
}

/// The strict resolution response. When `errors` is non-empty, `values` is
/// empty: an error result can never be consumed as an accepted vector.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanDecisionResolveWire {
    #[serde(default)]
    pub values: BTreeMap<String, JsonValue>,
    #[serde(default)]
    pub rows: Vec<PlanDecisionResolveRowWire>,
    #[serde(default)]
    pub errors: Vec<PlanDecisionResolveErrorWire>,
}

fn is_unverified_memory_clamp(
    is_memory: bool,
    authored: &JsonValue,
    effective: &JsonValue,
) -> bool {
    is_memory
        && *authored == JsonValue::Bool(true)
        && *effective == JsonValue::Bool(false)
}

/// Freeze the normalized validated plan's decisions into an ordered review
/// vector using the host-supplied verification facts.
///
/// Takes the normalized validated plan object, not the outer validation
/// envelope. Unknown provenance strings are usage errors; absent facts fail
/// closed to unverified with `not_asked` provenance.
pub fn plan_decisions_payload(
    validated: &ValidatedPlanWire,
    host_facts: &BTreeMap<String, PlanDecisionHostFactWire>,
) -> Result<Vec<PlanDecisionDefinitionWire>, PlanError> {
    let mut definitions = Vec::with_capacity(validated.decisions.len());
    for decision in &validated.decisions {
        let is_memory = decision.memory.is_some();
        let fact = host_facts.get(&decision.id);
        let mut requested_verified = false;
        let mut provenance: Option<String> = None;
        let mut resolved = Vec::new();
        if is_memory {
            requested_verified =
                fact.map(|fact| fact.requested_verified).unwrap_or(false);
            let raw = fact.and_then(|fact| fact.provenance.clone());
            let name = raw.unwrap_or_else(|| "not_asked".to_string());
            if !DECISION_PROVENANCES.contains(&name.as_str()) {
                return Err(PlanError::validation(format!(
                    "unknown decision provenance \"{name}\" for \"{}\"; \
                     expected asked, not_asked, quote_not_found, or inherited",
                    decision.id,
                )));
            }
            provenance = Some(name);
            resolved =
                fact.map(|fact| fact.resolved.clone()).unwrap_or_default();
        }
        let effective_default = if is_memory
            && decision.default == JsonValue::Bool(true)
            && !requested_verified
        {
            JsonValue::Bool(false)
        } else {
            decision.default.clone()
        };
        definitions.push(PlanDecisionDefinitionWire {
            id: decision.id.clone(),
            kind: decision.kind.clone(),
            ask: decision.ask.clone(),
            why: decision.why.clone(),
            choices: decision.choices.clone(),
            default: decision.default.clone(),
            effective_default,
            memory: decision.memory.clone(),
            requested: decision.requested.clone(),
            provenance,
            requested_verified,
            resolved,
        });
    }
    Ok(definitions)
}

/// Digest the frozen definitions for the review edit freeze.
///
/// SHA-256 over canonical definition JSON: object keys sort recursively
/// while every ordered array is preserved. The ask, labels, why, defaults,
/// selector scope, and frozen strand list move the digest; object insertion
/// order, answer vectors, plan prose, and review revision do not
/// participate. Missing optional fields round-trip consistently, so
/// JSON-to-Rust-to-JSON yields the same digest.
pub fn plan_decisions_digest(
    definitions: &[PlanDecisionDefinitionWire],
) -> Result<String, PlanError> {
    let value = serde_json::to_value(definitions).map_err(|error| {
        PlanError::validation(format!(
            "unable to encode decision definitions for digest: {error}"
        ))
    })?;
    crate::finalizer::canonical_json_sha256(&value)
        .map_err(|error| PlanError::validation(error.to_string()))
}

/// Strictly resolve one accepted answer vector in author order.
///
/// Omitted values take the effective default. Toggles require JSON
/// booleans; choice values match a full key case-insensitively and resolve
/// to the authored spelling, with no prefix matching. Unknown ids and
/// invalid values accumulate as errors listing the allowed values and the
/// effective default. An agent cannot switch on a memory decision whose
/// effective default is false (`memory_decision_requires_human`); `auto`
/// takes effective defaults only and reports valid overrides as `default`
/// or `clamped`. Unknown callers and non-object submissions are usage
/// errors, never human callers.
pub fn plan_decisions_resolve(
    definitions: &[PlanDecisionDefinitionWire],
    submitted: &JsonValue,
    caller: &str,
) -> Result<PlanDecisionResolveWire, PlanError> {
    if !matches!(caller, "human" | "agent" | "auto") {
        return Err(PlanError::validation(format!(
            "unknown decision caller \"{caller}\"; \
             expected human, agent, or auto"
        )));
    }
    let submitted_map = submitted.as_object().ok_or_else(|| {
        PlanError::validation(
            "submitted decisions must be an object keyed by decision id",
        )
    })?;
    let known_ids: Vec<&str> = definitions
        .iter()
        .map(|definition| definition.id.as_str())
        .collect();

    let mut values = BTreeMap::new();
    let mut rows = Vec::new();
    let mut errors = Vec::new();

    for unknown in submitted_map.keys() {
        if !known_ids.contains(&unknown.as_str()) {
            errors.push(PlanDecisionResolveErrorWire {
                id: unknown.clone(),
                code: "decision-resolve-unknown-id".to_string(),
                message: format!(
                    "unknown decision id \"{unknown}\"; known decisions: {}",
                    known_ids.join(", "),
                ),
                allowed: known_ids
                    .iter()
                    .map(|id| JsonValue::String((*id).to_string()))
                    .collect(),
                default: None,
            });
        }
    }

    for definition in definitions {
        let is_memory = definition.memory.is_some();
        let clamped = is_unverified_memory_clamp(
            is_memory,
            &definition.default,
            &definition.effective_default,
        );
        let allowed_values: Vec<JsonValue> = if definition.kind == "choice" {
            definition
                .choices
                .iter()
                .map(|choice| JsonValue::String(choice.key.clone()))
                .collect()
        } else {
            vec![JsonValue::Bool(true), JsonValue::Bool(false)]
        };
        let invalid = |raw: &JsonValue| PlanDecisionResolveErrorWire {
            id: definition.id.clone(),
            code: "decision-resolve-invalid".to_string(),
            message: format!(
                "invalid value for decision \"{}\"; expected {}; got {raw}",
                definition.id,
                describe_allowed(definition),
            ),
            allowed: allowed_values.clone(),
            default: Some(definition.effective_default.clone()),
        };
        match submitted_map.get(&definition.id) {
            None => {
                rows.push(PlanDecisionResolveRowWire {
                    id: definition.id.clone(),
                    value: definition.effective_default.clone(),
                    source: if clamped {
                        "clamped".to_string()
                    } else {
                        "default".to_string()
                    },
                    changed: false,
                });
            }
            Some(raw) => {
                let canonical = if definition.kind == "choice" {
                    match raw.as_str() {
                        Some(text) => {
                            match definition.choices.iter().find(|choice| {
                                choice.key.to_lowercase() == text.to_lowercase()
                            }) {
                                Some(choice) => {
                                    JsonValue::String(choice.key.clone())
                                }
                                None => {
                                    errors.push(invalid(raw));
                                    continue;
                                }
                            }
                        }
                        None => {
                            errors.push(invalid(raw));
                            continue;
                        }
                    }
                } else if let Some(flag) = raw.as_bool() {
                    JsonValue::Bool(flag)
                } else {
                    errors.push(invalid(raw));
                    continue;
                };
                if caller == "auto" {
                    rows.push(PlanDecisionResolveRowWire {
                        id: definition.id.clone(),
                        value: definition.effective_default.clone(),
                        source: if clamped {
                            "clamped".to_string()
                        } else {
                            "default".to_string()
                        },
                        changed: false,
                    });
                    continue;
                }
                if caller == "agent"
                    && is_memory
                    && canonical == JsonValue::Bool(true)
                    && definition.effective_default == JsonValue::Bool(false)
                {
                    errors.push(PlanDecisionResolveErrorWire {
                        id: definition.id.clone(),
                        code: "memory_decision_requires_human".to_string(),
                        message: format!(
                            "decision \"{}\" needs a human to switch it on; \
                             agent callers cannot enable a memory decision \
                             whose effective default is off",
                            definition.id,
                        ),
                        allowed: allowed_values.clone(),
                        default: Some(definition.effective_default.clone()),
                    });
                    continue;
                }
                let changed = canonical != definition.effective_default;
                rows.push(PlanDecisionResolveRowWire {
                    id: definition.id.clone(),
                    value: canonical,
                    source: "submitted".to_string(),
                    changed,
                });
            }
        }
    }

    if !errors.is_empty() {
        return Ok(PlanDecisionResolveWire {
            values: BTreeMap::new(),
            rows,
            errors,
        });
    }
    for row in &rows {
        values.insert(row.id.clone(), row.value.clone());
    }
    Ok(PlanDecisionResolveWire {
        values,
        rows,
        errors,
    })
}

fn describe_allowed(definition: &PlanDecisionDefinitionWire) -> String {
    if definition.kind == "choice" {
        let keys: Vec<&str> = definition
            .choices
            .iter()
            .map(|choice| choice.key.as_str())
            .collect();
        format!("one of {}", keys.join(", "))
    } else {
        "a JSON boolean".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::decisions::PlanDecisionChoiceWire;
    use crate::plan::decisions::PlanDecisionMemoryWire;
    use serde_json::json;

    fn toggle(
        id: &str,
        default: bool,
    ) -> crate::plan::decisions::PlanDecisionWire {
        crate::plan::decisions::PlanDecisionWire {
            id: id.to_string(),
            kind: "toggle".to_string(),
            ask: format!("Do {id}?"),
            why: None,
            choices: Vec::new(),
            default: json!(default),
            memory: None,
            requested: None,
            answer: None,
        }
    }

    fn memory_toggle(id: &str) -> crate::plan::decisions::PlanDecisionWire {
        crate::plan::decisions::PlanDecisionWire {
            id: id.to_string(),
            kind: "toggle".to_string(),
            ask: format!("Edit {id}?"),
            why: None,
            choices: Vec::new(),
            default: json!(true),
            memory: Some(PlanDecisionMemoryWire {
                selectors: vec!["tui.md".to_string()],
            }),
            requested: Some(
                "and note the convention in the tui memory".to_string(),
            ),
            answer: None,
        }
    }

    fn choice() -> crate::plan::decisions::PlanDecisionWire {
        crate::plan::decisions::PlanDecisionWire {
            id: "grouping".to_string(),
            kind: "choice".to_string(),
            ask: "How to group?".to_string(),
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
            memory: None,
            requested: None,
            answer: None,
        }
    }

    fn validated(
        decisions: Vec<crate::plan::decisions::PlanDecisionWire>,
    ) -> ValidatedPlanWire {
        ValidatedPlanWire {
            tier: "tale".to_string(),
            goal: "G".to_string(),
            size: Some("small".to_string()),
            model: None,
            title: Some("T".to_string()),
            phases: Vec::new(),
            patch: None,
            bug_id: None,
            parent_bead: None,
            bead: None,
            proposed_by: None,
            parent: None,
            decisions,
            decision_callouts: Vec::new(),
            decided_by: None,
            decided_via: None,
        }
    }

    fn record(selector: &str) -> PlanDecisionMemoryRecordWire {
        PlanDecisionMemoryRecordWire {
            selector: selector.to_string(),
            kind: "note".to_string(),
            scope: "project".to_string(),
            path: "sase/memory/tui.md".to_string(),
            record_type: "reference".to_string(),
            exists: true,
            strands: None,
        }
    }

    fn definitions() -> Vec<PlanDecisionDefinitionWire> {
        let plan = validated(vec![choice(), memory_toggle("tui_note")]);
        let mut facts = BTreeMap::new();
        facts.insert(
            "tui_note".to_string(),
            PlanDecisionHostFactWire {
                requested_verified: true,
                provenance: Some("asked".to_string()),
                resolved: vec![record("tui.md")],
            },
        );
        plan_decisions_payload(&plan, &facts).unwrap()
    }

    #[test]
    fn payload_freezes_verified_memory_true() {
        let definitions = definitions();
        let memory = &definitions[1];
        assert_eq!(memory.effective_default, json!(true));
        assert_eq!(memory.default, json!(true));
        assert_eq!(memory.provenance, Some("asked".to_string()));
        assert!(memory.requested_verified);
        assert_eq!(memory.resolved.len(), 1);
        assert_eq!(
            memory.requested,
            Some("and note the convention in the tui memory".to_string())
        );
        // Non-memory decisions carry no provenance or resolved records.
        assert_eq!(definitions[0].provenance, None);
        assert!(definitions[0].resolved.is_empty());
        assert_eq!(definitions[0].effective_default, json!("pane"));
    }

    #[test]
    fn payload_clamps_unverified_memory_but_keeps_authored_default() {
        let plan = validated(vec![memory_toggle("tui_note")]);
        let mut facts = BTreeMap::new();
        facts.insert(
            "tui_note".to_string(),
            PlanDecisionHostFactWire {
                requested_verified: false,
                provenance: Some("quote_not_found".to_string()),
                resolved: vec![record("tui.md")],
            },
        );
        let definitions = plan_decisions_payload(&plan, &facts).unwrap();
        assert_eq!(definitions[0].default, json!(true));
        assert_eq!(definitions[0].effective_default, json!(false));
        assert_eq!(
            definitions[0].provenance,
            Some("quote_not_found".to_string())
        );
    }

    #[test]
    fn payload_missing_facts_fail_closed() {
        let plan = validated(vec![memory_toggle("tui_note")]);
        let definitions =
            plan_decisions_payload(&plan, &BTreeMap::new()).unwrap();
        assert_eq!(definitions[0].effective_default, json!(false));
        assert!(!definitions[0].requested_verified);
        assert_eq!(definitions[0].provenance, Some("not_asked".to_string()));
        assert!(definitions[0].resolved.is_empty());
    }

    #[test]
    fn payload_inherited_provenance_keeps_true_default() {
        let plan = validated(vec![memory_toggle("tui_note")]);
        let mut facts = BTreeMap::new();
        facts.insert(
            "tui_note".to_string(),
            PlanDecisionHostFactWire {
                requested_verified: true,
                provenance: Some("inherited".to_string()),
                resolved: vec![record("tui.md")],
            },
        );
        let definitions = plan_decisions_payload(&plan, &facts).unwrap();
        assert_eq!(definitions[0].effective_default, json!(true));
        assert_eq!(definitions[0].provenance, Some("inherited".to_string()));
    }

    #[test]
    fn payload_rejects_unknown_provenance() {
        let plan = validated(vec![memory_toggle("tui_note")]);
        let mut facts = BTreeMap::new();
        facts.insert(
            "tui_note".to_string(),
            PlanDecisionHostFactWire {
                requested_verified: true,
                provenance: Some("typed".to_string()),
                resolved: Vec::new(),
            },
        );
        plan_decisions_payload(&plan, &facts)
            .expect_err("unknown provenance must fail");
    }

    #[test]
    fn payload_preserves_frozen_strand_lists() {
        let plan = validated(vec![memory_toggle("web_note")]);
        let mut facts = BTreeMap::new();
        facts.insert(
            "web_note".to_string(),
            PlanDecisionHostFactWire {
                requested_verified: true,
                provenance: Some("asked".to_string()),
                resolved: vec![PlanDecisionMemoryRecordWire {
                    selector: "web:stitch".to_string(),
                    kind: "web".to_string(),
                    scope: "project".to_string(),
                    path: "sase/memory/glossary/stitch.md".to_string(),
                    record_type: "web".to_string(),
                    exists: true,
                    strands: Some(vec!["stitch".to_string()]),
                }],
            },
        );
        let definitions = plan_decisions_payload(&plan, &facts).unwrap();
        assert_eq!(
            definitions[0].resolved[0].strands,
            Some(vec!["stitch".to_string()])
        );
    }

    #[test]
    fn digest_ignores_key_order_but_moves_on_content() {
        let definitions = definitions();
        let first = plan_decisions_digest(&definitions).unwrap();
        assert_eq!(first.len(), 64);
        // Rebuild through JSON with shuffled object keys: same digest.
        let mut reordered = serde_json::to_value(&definitions).unwrap();
        for definition in reordered.as_array_mut().unwrap() {
            let object = definition.as_object_mut().unwrap();
            let mut keys: Vec<String> = object.keys().cloned().collect();
            keys.reverse();
            let rebuilt: serde_json::Map<String, JsonValue> = keys
                .into_iter()
                .map(|key| {
                    let value = object.remove(&key).unwrap();
                    (key, value)
                })
                .collect();
            *object = rebuilt;
        }
        let shuffled: Vec<PlanDecisionDefinitionWire> =
            serde_json::from_value(reordered).unwrap();
        assert_eq!(plan_decisions_digest(&shuffled).unwrap(), first);
        // Changing the ask moves the digest.
        let mut changed = definitions.clone();
        changed[0].ask = "How to group instead?".to_string();
        assert_ne!(plan_decisions_digest(&changed).unwrap(), first);
    }

    #[test]
    fn digest_round_trips_missing_optionals() {
        let definitions = definitions();
        let first = plan_decisions_digest(&definitions).unwrap();
        let value = serde_json::to_value(&definitions).unwrap();
        let back: Vec<PlanDecisionDefinitionWire> =
            serde_json::from_value(value).unwrap();
        assert_eq!(plan_decisions_digest(&back).unwrap(), first);
    }

    #[test]
    fn resolve_omitted_takes_effective_defaults() {
        let outcome =
            plan_decisions_resolve(&definitions(), &json!({}), "human")
                .unwrap();
        assert!(outcome.errors.is_empty());
        assert_eq!(
            outcome.values,
            BTreeMap::from([
                ("grouping".to_string(), json!("pane")),
                ("tui_note".to_string(), json!(true)),
            ])
        );
        assert_eq!(outcome.rows[0].source, "default");
        assert!(!outcome.rows[0].changed);
    }

    #[test]
    fn resolve_marks_clamped_omitted_memory() {
        let plan = validated(vec![memory_toggle("tui_note")]);
        let definitions =
            plan_decisions_payload(&plan, &BTreeMap::new()).unwrap();
        let outcome =
            plan_decisions_resolve(&definitions, &json!({}), "human").unwrap();
        assert_eq!(outcome.values["tui_note"], json!(false));
        assert_eq!(outcome.rows[0].source, "clamped");
        assert!(!outcome.rows[0].changed);
    }

    #[test]
    fn resolve_canonicalizes_choice_case_without_prefix_match() {
        let outcome = plan_decisions_resolve(
            &definitions(),
            &json!({"grouping": "MODE"}),
            "human",
        )
        .unwrap();
        assert!(outcome.errors.is_empty());
        assert_eq!(outcome.values["grouping"], json!("mode"));
        assert_eq!(outcome.rows[0].source, "submitted");
        assert!(outcome.rows[0].changed);

        let prefixed = plan_decisions_resolve(
            &definitions(),
            &json!({"grouping": "mod"}),
            "human",
        )
        .unwrap();
        assert_eq!(prefixed.errors.len(), 1);
        assert_eq!(prefixed.errors[0].code, "decision-resolve-invalid");
        assert_eq!(
            prefixed.errors[0].allowed,
            vec![json!("pane"), json!("mode")]
        );
        assert_eq!(prefixed.errors[0].default, Some(json!("pane")));
        assert!(prefixed.values.is_empty());
    }

    #[test]
    fn resolve_rejects_non_boolean_toggles() {
        for raw in [json!("yes"), json!(1), json!(null)] {
            let outcome = plan_decisions_resolve(
                &definitions(),
                &json!({"tui_note": raw}),
                "human",
            )
            .unwrap();
            assert_eq!(outcome.errors.len(), 1, "raw={raw}");
            assert_eq!(outcome.errors[0].code, "decision-resolve-invalid");
            assert!(outcome.values.is_empty());
        }
    }

    #[test]
    fn resolve_accumulates_unknown_ids_and_invalid_values() {
        let outcome = plan_decisions_resolve(
            &definitions(),
            &json!({"nope": true, "grouping": "sideways"}),
            "human",
        )
        .unwrap();
        assert_eq!(outcome.errors.len(), 2);
        assert_eq!(outcome.errors[0].code, "decision-resolve-unknown-id");
        assert_eq!(outcome.errors[0].default, None);
        assert_eq!(outcome.errors[1].code, "decision-resolve-invalid");
        assert!(outcome.values.is_empty());
    }

    #[test]
    fn resolve_agent_boundary() {
        // An agent cannot switch on an unverified memory decision.
        let plan = validated(vec![memory_toggle("tui_note")]);
        let clamped = plan_decisions_payload(&plan, &BTreeMap::new()).unwrap();
        let refused = plan_decisions_resolve(
            &clamped,
            &json!({"tui_note": true}),
            "agent",
        )
        .unwrap();
        assert_eq!(refused.errors.len(), 1);
        assert_eq!(refused.errors[0].code, "memory_decision_requires_human");
        assert_eq!(refused.errors[0].default, Some(json!(false)));
        assert!(refused.values.is_empty());

        // An agent may retain a verified true default or turn it off.
        let verified = definitions();
        let keep = plan_decisions_resolve(
            &verified,
            &json!({"tui_note": true}),
            "agent",
        )
        .unwrap();
        assert!(keep.errors.is_empty());
        assert_eq!(keep.values["tui_note"], json!(true));
        assert!(!keep.rows[1].changed);
        let off = plan_decisions_resolve(
            &verified,
            &json!({"tui_note": false}),
            "agent",
        )
        .unwrap();
        assert!(off.errors.is_empty());
        assert!(off.rows[1].changed);

        // A human may enable an unrequested decision.
        let human = plan_decisions_resolve(
            &clamped,
            &json!({"tui_note": true}),
            "human",
        )
        .unwrap();
        assert!(human.errors.is_empty());
        assert_eq!(human.values["tui_note"], json!(true));
        assert!(human.rows[0].changed);
    }

    #[test]
    fn resolve_auto_ignores_valid_overrides_but_keeps_errors() {
        let outcome = plan_decisions_resolve(
            &definitions(),
            &json!({"grouping": "mode", "tui_note": false}),
            "auto",
        )
        .unwrap();
        assert!(outcome.errors.is_empty());
        assert_eq!(outcome.values["grouping"], json!("pane"));
        assert_eq!(outcome.values["tui_note"], json!(true));
        assert!(outcome.rows.iter().all(|row| row.source == "default"));

        let bad = plan_decisions_resolve(
            &definitions(),
            &json!({"grouping": "mode", "nope": true}),
            "auto",
        )
        .unwrap();
        assert_eq!(bad.errors.len(), 1);
        assert!(bad.values.is_empty());
    }

    #[test]
    fn resolve_rejects_unknown_caller_and_non_object() {
        plan_decisions_resolve(&definitions(), &json!({}), "robot")
            .expect_err("unknown caller must fail");
        plan_decisions_resolve(&definitions(), &json!([]), "human")
            .expect_err("non-object submission must fail");
    }

    #[test]
    fn resolve_omitted_and_explicit_defaults_agree_on_values() {
        let definitions = definitions();
        let omitted =
            plan_decisions_resolve(&definitions, &json!({}), "human").unwrap();
        let explicit = plan_decisions_resolve(
            &definitions,
            &json!({"grouping": "pane", "tui_note": true}),
            "human",
        )
        .unwrap();
        assert_eq!(omitted.values, explicit.values);
        assert_eq!(explicit.rows[0].source, "submitted");
    }

    #[test]
    fn toggle_helper_builds_plain_decisions() {
        let plan = validated(vec![toggle("notify", false)]);
        let definitions =
            plan_decisions_payload(&plan, &BTreeMap::new()).unwrap();
        assert_eq!(definitions[0].effective_default, json!(false));
        let outcome =
            plan_decisions_resolve(&definitions, &json!({}), "auto").unwrap();
        assert_eq!(outcome.values["notify"], json!(false));
    }
}
