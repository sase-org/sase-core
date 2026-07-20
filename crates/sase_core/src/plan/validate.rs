//! Strict, tier-aware validation for authored SASE plan documents.
//!
//! Plan discovery deliberately stays tolerant. This module is its strict
//! sibling for proposal, approval, commit, and CI boundaries: it validates a
//! complete markdown document, reports every independently discoverable
//! problem, and returns a normalized typed plan only when no errors remain.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};
use serde_yaml::{Mapping, Value as YamlValue};

use crate::bead::validate_model_value;

use super::read::split_frontmatter;
use super::wire::{PlanError, PLAN_WIRE_SCHEMA_VERSION};

const COMMON_FIELDS: &[&str] = &["tier", "title", "goal", "model"];
const SYSTEM_FIELDS: &[&str] = &["create_time", "status", "prompt", "bead_id"];
const EPIC_FIELDS: &[&str] = &["phases", "changespec", "bug_id", "parent_bead"];
const PHASE_FIELDS: &[&str] =
    &["id", "title", "depends_on", "description", "size", "model"];

const PHASE_DESCRIPTION_DESCRIPTION: &str = "Phase bead description: name this phase's section in the plan body and briefly summarize that section. Do not reference the plan file itself; `sase bead show` already displays it.";
const PHASE_SIZE_DESCRIPTION: &str = "Estimated phase scope. Use `medium` when the phase is potentially a lot of work and justifies its own plan file, `large` when that plan would itself be large enough for an epic tier, and `small` otherwise. Medium and large phases receive `#plan`; a large phase without an explicit `model` uses `@smartest`.";
const PHASE_MODEL_DESCRIPTION: &str = "Model for this phase's agent. Only set this explicitly when the user's prompt requested a specific model, or when this phase's agent does not do real consequential work (for example, a phase that exercises or tests the feature's own functionality). Otherwise omit it so size-derived model routing applies (`@smartest` for large phases and the configured `@phase_worker` role alias otherwise).";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PlanTier {
    Tale,
    Epic,
}

impl PlanTier {
    fn parse(value: &str) -> Result<Self, PlanError> {
        match value {
            "tale" => Ok(Self::Tale),
            "epic" => Ok(Self::Epic),
            _ => Err(PlanError::validation(format!(
                "unsupported plan tier `{value}`; expected `tale` or `epic`"
            ))),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Tale => "tale",
            Self::Epic => "epic",
        }
    }
}

/// Validation policy for authored plans versus legacy launch consumption.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanValidationMode {
    /// Require every field in the current authoring schema.
    Authoring,
    /// Accept a missing legacy phase size as `small` with a warning.
    Launch,
}

impl PlanValidationMode {
    fn parse(value: &str) -> Result<Self, PlanError> {
        match value {
            "authoring" => Ok(Self::Authoring),
            "launch" => Ok(Self::Launch),
            _ => Err(PlanError::validation(format!(
                "unsupported plan validation mode `{value}`; expected `authoring` or `launch`"
            ))),
        }
    }
}

/// One actionable validation problem.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanDiagnosticWire {
    pub severity: String,
    pub code: String,
    pub field_path: String,
    pub message: String,
    pub line: Option<u64>,
}

/// Ordered machine-readable metadata for one accepted frontmatter field.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlanFrontmatterFieldSpecWire {
    pub name: String,
    #[serde(rename = "type")]
    pub field_type: String,
    pub required: bool,
    pub description: String,
    pub example: JsonValue,
}

/// A normalized epic phase.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanPhaseWire {
    pub id: String,
    pub title: String,
    pub depends_on: Vec<String>,
    pub description: Option<String>,
    pub size: String,
    pub model: Option<String>,
}

/// The normalized plan structure returned after successful validation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidatedPlanWire {
    pub tier: String,
    pub goal: String,
    pub model: Option<String>,
    pub title: Option<String>,
    pub phases: Vec<PlanPhaseWire>,
    pub changespec: Option<String>,
    pub bug_id: Option<i64>,
    pub parent_bead: Option<String>,
}

/// Complete validation result. Warnings do not make `ok` false.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanValidationResultWire {
    pub schema_version: u64,
    pub ok: bool,
    pub diagnostics: Vec<PlanDiagnosticWire>,
    pub plan: Option<ValidatedPlanWire>,
}

/// Validate one complete markdown plan document against an explicit tier.
pub fn plan_validate(
    content: &str,
    tier: &str,
) -> Result<PlanValidationResultWire, PlanError> {
    plan_validate_with_mode(content, tier, "authoring")
}

/// Validate a plan with an explicit authoring or launch-consumption policy.
pub fn plan_validate_with_mode(
    content: &str,
    tier: &str,
    mode: &str,
) -> Result<PlanValidationResultWire, PlanError> {
    let tier = PlanTier::parse(tier)?;
    let mode = PlanValidationMode::parse(mode)?;
    Ok(Validator::new(content, tier, mode).validate())
}

/// Return the ordered authoritative frontmatter field metadata for a tier.
pub fn plan_frontmatter_schema(
    tier: &str,
) -> Result<Vec<PlanFrontmatterFieldSpecWire>, PlanError> {
    let tier = PlanTier::parse(tier)?;
    let mut fields = vec![
        field_spec(
            "tier",
            "tale | epic",
            true,
            "Plan-file tier. It must match the tier supplied by the caller.",
            json!(tier.as_str()),
        ),
        field_spec(
            "title",
            "non-empty string",
            true,
            "Human-readable plan title.",
            json!(match tier {
                PlanTier::Tale => "Ship the requested capability",
                PlanTier::Epic => "Workspace GC rewrite",
            }),
        ),
        field_spec(
            "goal",
            "non-empty string",
            true,
            "Outcome the plan is designed to achieve.",
            json!("The requested capability works end to end."),
        ),
        field_spec(
            "model",
            "non-empty string",
            false,
            match tier {
                PlanTier::Tale => "Model for the tale's coder follow-up.",
                PlanTier::Epic => "Model for the epic's land agent.",
            },
            json!("codex/gpt-5.6-sol"),
        ),
    ];

    if tier == PlanTier::Epic {
        fields.extend([
            field_spec(
                "phases",
                "non-empty list of phase mappings",
                true,
                "Epic phases in creation and execution order.",
                json!([{
                    "id": "core",
                    "title": "Core implementation",
                    "depends_on": [],
                    "size": "small"
                }]),
            ),
            field_spec(
                "phases[].id",
                "unique slug string",
                true,
                "Stable phase identifier used by dependency references.",
                json!("core"),
            ),
            field_spec(
                "phases[].title",
                "non-empty string",
                true,
                "Title used for the phase bead.",
                json!("Core implementation"),
            ),
            field_spec(
                "phases[].depends_on",
                "list of earlier phase ids",
                true,
                "Dependency phase ids. Use an empty list for an unblocked phase.",
                json!([]),
            ),
            field_spec(
                "phases[].description",
                "string",
                false,
                PHASE_DESCRIPTION_DESCRIPTION,
                json!("'Core validator' section: build the shared validation engine and its JSON wire."),
            ),
            field_spec(
                "phases[].size",
                "small | medium | large",
                true,
                PHASE_SIZE_DESCRIPTION,
                json!("small"),
            ),
            field_spec(
                "phases[].model",
                "non-empty string",
                false,
                PHASE_MODEL_DESCRIPTION,
                json!("claude/haiku"),
            ),
            field_spec(
                "changespec",
                "non-empty string",
                false,
                "ChangeSpec metadata forwarded to the epic bead.",
                json!("workspace_gc"),
            ),
            field_spec(
                "bug_id",
                "integer",
                false,
                "Bug metadata forwarded to the epic bead; requires `changespec`.",
                json!(12345),
            ),
            field_spec(
                "parent_bead",
                "non-empty string",
                false,
                "SASE-managed parent bead id used when creating a child epic.",
                json!("sase-7z.1"),
            ),
        ]);
    }

    fields.extend([
        field_spec(
            "create_time",
            "system-managed value",
            false,
            "Creation timestamp maintained by SASE.",
            json!("2026-07-14 12:43:46"),
        ),
        field_spec(
            "status",
            "system-managed value",
            false,
            "Plan lifecycle status maintained by SASE.",
            json!("wip"),
        ),
        field_spec(
            "prompt",
            "system-managed value",
            false,
            "Prompt snapshot reference maintained by SASE.",
            json!("202607/prompts/example.md"),
        ),
        field_spec(
            "bead_id",
            "system-managed value",
            false,
            "Epic bead id written by SASE after creation.",
            json!("sase-61"),
        ),
    ]);
    Ok(fields)
}

fn field_spec(
    name: &str,
    field_type: &str,
    required: bool,
    description: &str,
    example: JsonValue,
) -> PlanFrontmatterFieldSpecWire {
    PlanFrontmatterFieldSpecWire {
        name: name.to_string(),
        field_type: field_type.to_string(),
        required,
        description: description.to_string(),
        example,
    }
}

struct Validator<'a> {
    content: &'a str,
    tier: PlanTier,
    mode: PlanValidationMode,
    diagnostics: Vec<PlanDiagnosticWire>,
}

impl<'a> Validator<'a> {
    fn new(content: &'a str, tier: PlanTier, mode: PlanValidationMode) -> Self {
        Self {
            content,
            tier,
            mode,
            diagnostics: Vec::new(),
        }
    }

    fn validate(mut self) -> PlanValidationResultWire {
        let Some((yaml, body)) = self.strict_frontmatter() else {
            return self.finish(None);
        };
        let index = SourceIndex::new(&yaml);

        if body.trim().is_empty() {
            self.push_direct(
                "error",
                "body-empty",
                "",
                "plan markdown body must not be empty",
                None,
            );
        }

        let value = match serde_yaml::from_str::<YamlValue>(&yaml) {
            Ok(value) => value,
            Err(error) => {
                let line =
                    error.location().map(|location| location.line() as u64 + 1);
                self.push_direct(
                    "error",
                    "yaml-invalid",
                    "",
                    format!("frontmatter is not valid YAML: {error}"),
                    line,
                );
                return self.finish(None);
            }
        };
        let Some(mapping) = value.as_mapping() else {
            self.push_direct(
                "error",
                "frontmatter-not-mapping",
                "",
                "frontmatter YAML must be a mapping",
                Some(2),
            );
            return self.finish(None);
        };

        self.validate_top_level_keys(mapping, &index);
        let authored_tier = self.validate_tier(mapping, &index);
        let title = self.required_non_empty_string(mapping, "title", &index);
        let goal = self.required_non_empty_string(mapping, "goal", &index);
        let model = self.optional_model(mapping, "model", &index);

        let (phases, changespec, bug_id, parent_bead) = match self.tier {
            PlanTier::Tale => (Vec::new(), None, None, None),
            PlanTier::Epic => {
                let changespec = self.optional_non_empty_string(
                    mapping,
                    "changespec",
                    &index,
                );
                let bug_id = self.optional_integer(mapping, "bug_id", &index);
                if mapping_value(mapping, "bug_id").is_some()
                    && changespec.is_none()
                {
                    self.push(
                        "error",
                        "bug-id-without-changespec",
                        "bug_id",
                        "`bug_id` requires a non-empty `changespec`",
                        &index,
                    );
                }
                let parent_bead = self.optional_non_empty_string(
                    mapping,
                    "parent_bead",
                    &index,
                );
                let phases = self.validate_phases(mapping, &index);
                (phases, changespec, bug_id, parent_bead)
            }
        };

        let has_errors = self
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.severity == "error");
        let plan = (!has_errors).then(|| ValidatedPlanWire {
            tier: authored_tier.expect("valid tier must be present"),
            goal: goal.expect("valid goal must be present"),
            model,
            title,
            phases,
            changespec,
            bug_id,
            parent_bead,
        });
        self.finish(plan)
    }

    fn strict_frontmatter(&mut self) -> Option<(String, String)> {
        let opens = self.content == "---"
            || self.content.starts_with("---\n")
            || self.content.starts_with("---\r\n");
        if !opens {
            self.push_direct(
                "error",
                "frontmatter-missing",
                "",
                "frontmatter must open with `---` at byte 0",
                Some(1),
            );
            return None;
        }
        let (frontmatter, body) = split_frontmatter(self.content);
        let Some(frontmatter) = frontmatter else {
            self.push_direct(
                "error",
                "frontmatter-unclosed",
                "",
                "frontmatter is missing its closing `---` marker",
                Some(1),
            );
            return None;
        };
        Some((frontmatter, body))
    }

    fn validate_top_level_keys(
        &mut self,
        mapping: &Mapping,
        index: &SourceIndex,
    ) {
        for key in mapping.keys() {
            let Some(key) = key.as_str() else {
                self.push_direct(
                    "error",
                    "unknown-key",
                    "",
                    "frontmatter keys must be strings",
                    Some(2),
                );
                continue;
            };
            if COMMON_FIELDS.contains(&key) || SYSTEM_FIELDS.contains(&key) {
                continue;
            }
            if EPIC_FIELDS.contains(&key) {
                if self.tier == PlanTier::Tale {
                    self.push(
                        "warning",
                        "tale-inert-field",
                        key,
                        format!(
                            "epic-only field `{key}` is inert when validating a tale"
                        ),
                        index,
                    );
                }
                continue;
            }
            self.push(
                "error",
                "unknown-key",
                key,
                format!("unknown frontmatter field `{key}`"),
                index,
            );
        }
    }

    fn validate_tier(
        &mut self,
        mapping: &Mapping,
        index: &SourceIndex,
    ) -> Option<String> {
        let tier = self.required_non_empty_string(mapping, "tier", index)?;
        if tier != "tale" && tier != "epic" {
            self.push(
                "error",
                "tier-invalid",
                "tier",
                "`tier` must be exactly `tale` or `epic`",
                index,
            );
            return None;
        }
        if tier != self.tier.as_str() {
            self.push(
                "error",
                "tier-mismatch",
                "tier",
                format!(
                    "authored tier `{tier}` does not match requested tier `{}`",
                    self.tier.as_str()
                ),
                index,
            );
        }
        Some(tier)
    }

    fn validate_phases(
        &mut self,
        mapping: &Mapping,
        index: &SourceIndex,
    ) -> Vec<PlanPhaseWire> {
        let Some(value) = mapping_value(mapping, "phases") else {
            self.push(
                "error",
                "required-missing",
                "phases",
                "required field `phases` is missing",
                index,
            );
            return Vec::new();
        };
        let Some(sequence) = value.as_sequence() else {
            self.push(
                "error",
                "type-mismatch",
                "phases",
                format!(
                    "field `phases` must be a list, found {}",
                    yaml_type_name(value)
                ),
                index,
            );
            return Vec::new();
        };
        if sequence.is_empty() {
            self.push(
                "error",
                "phases-empty",
                "phases",
                "epic `phases` must contain at least one phase",
                index,
            );
            return Vec::new();
        }

        let mut ids = vec![None; sequence.len()];
        let mut first_id_index = BTreeMap::new();
        for (phase_index, value) in sequence.iter().enumerate() {
            let phase_path = format!("phases[{phase_index}]");
            let Some(phase) = value.as_mapping() else {
                self.push(
                    "error",
                    "phase-type-mismatch",
                    &phase_path,
                    format!(
                        "phase entry must be a mapping, found {}",
                        yaml_type_name(value)
                    ),
                    index,
                );
                continue;
            };
            self.validate_phase_keys(phase, phase_index, index);
            let id_path = format!("{phase_path}.id");
            let Some(id) =
                self.required_non_empty_string_at(phase, "id", &id_path, index)
            else {
                continue;
            };
            if !is_slug(&id) {
                self.push(
                    "error",
                    "phase-id-invalid",
                    &id_path,
                    "phase `id` must be a lowercase slug using letters, digits, `-`, or `_`",
                    index,
                );
            }
            if let Some(first_index) = first_id_index.get(&id) {
                self.push(
                    "error",
                    "phase-id-duplicate",
                    &id_path,
                    format!(
                        "phase id `{id}` duplicates phases[{first_index}].id"
                    ),
                    index,
                );
            } else {
                first_id_index.insert(id.clone(), phase_index);
            }
            ids[phase_index] = Some(id);
        }

        let mut phases = Vec::with_capacity(sequence.len());
        for (phase_index, value) in sequence.iter().enumerate() {
            let Some(phase) = value.as_mapping() else {
                continue;
            };
            let phase_path = format!("phases[{phase_index}]");
            let title_path = format!("{phase_path}.title");
            let title = self.required_non_empty_string_at(
                phase,
                "title",
                &title_path,
                index,
            );
            let depends_on =
                self.validate_dependencies(phase, phase_index, &ids, index);
            let description_path = format!("{phase_path}.description");
            if mapping_value(phase, "description").is_none() {
                let phase_id =
                    ids[phase_index].as_deref().unwrap_or("<unknown>");
                self.push(
                    "warning",
                    "phase-description-missing",
                    &description_path,
                    format!(
                        "phase `{phase_id}` has no `description`; add one naming its plan-body section and briefly summarizing that section"
                    ),
                    index,
                );
            }
            let description = self.optional_string_at(
                phase,
                "description",
                &description_path,
                index,
            );
            let size = self.validate_phase_size(phase, phase_index, index);
            let model_path = format!("{phase_path}.model");
            let model =
                self.optional_model_at(phase, "model", &model_path, index);
            phases.push(PlanPhaseWire {
                id: ids[phase_index].clone().unwrap_or_default(),
                title: title.unwrap_or_default(),
                depends_on,
                description,
                size,
                model,
            });
        }
        phases
    }

    fn validate_phase_size(
        &mut self,
        mapping: &Mapping,
        phase_index: usize,
        index: &SourceIndex,
    ) -> String {
        let field_path = format!("phases[{phase_index}].size");
        let Some(value) = mapping_value(mapping, "size") else {
            let (severity, message) = match self.mode {
                PlanValidationMode::Authoring => (
                    "error",
                    "required phase field `size` is missing; expected `small`, `medium`, or `large`",
                ),
                PlanValidationMode::Launch => (
                    "warning",
                    "legacy phase field `size` is missing; treating the phase as `small` for launch",
                ),
            };
            self.push(
                severity,
                "phase-size-missing",
                &field_path,
                message,
                index,
            );
            return match self.mode {
                PlanValidationMode::Authoring => String::new(),
                PlanValidationMode::Launch => "small".to_string(),
            };
        };
        let Some(value) = value.as_str() else {
            self.push(
                "error",
                "phase-size-invalid",
                &field_path,
                format!(
                    "phase `size` must be `small`, `medium`, or `large`, found {}",
                    yaml_type_name(value)
                ),
                index,
            );
            return String::new();
        };
        let value = value.trim();
        if !matches!(value, "small" | "medium" | "large") {
            self.push(
                "error",
                "phase-size-invalid",
                &field_path,
                format!(
                    "phase `size` must be `small`, `medium`, or `large`, found `{value}`"
                ),
                index,
            );
            return String::new();
        }
        value.to_string()
    }

    fn validate_phase_keys(
        &mut self,
        mapping: &Mapping,
        phase_index: usize,
        index: &SourceIndex,
    ) {
        for key in mapping.keys() {
            let Some(key) = key.as_str() else {
                self.push(
                    "error",
                    "unknown-key",
                    &format!("phases[{phase_index}]"),
                    "phase keys must be strings",
                    index,
                );
                continue;
            };
            if !PHASE_FIELDS.contains(&key) {
                let path = format!("phases[{phase_index}].{key}");
                self.push(
                    "error",
                    "unknown-key",
                    &path,
                    format!("unknown phase field `{key}`"),
                    index,
                );
            }
        }
    }

    fn validate_dependencies(
        &mut self,
        mapping: &Mapping,
        phase_index: usize,
        phase_ids: &[Option<String>],
        index: &SourceIndex,
    ) -> Vec<String> {
        let field_path = format!("phases[{phase_index}].depends_on");
        let Some(value) = mapping_value(mapping, "depends_on") else {
            self.push(
                "error",
                "required-missing",
                &field_path,
                "required field `depends_on` is missing",
                index,
            );
            return Vec::new();
        };
        let Some(sequence) = value.as_sequence() else {
            self.push(
                "error",
                "type-mismatch",
                &field_path,
                format!(
                    "field `depends_on` must be a list, found {}",
                    yaml_type_name(value)
                ),
                index,
            );
            return Vec::new();
        };

        let current_id = phase_ids[phase_index].as_deref();
        let mut seen = BTreeSet::new();
        let mut dependencies = Vec::with_capacity(sequence.len());
        for (dependency_index, value) in sequence.iter().enumerate() {
            let path = format!("{field_path}[{dependency_index}]");
            let Some(raw) = value.as_str() else {
                self.push(
                    "error",
                    "type-mismatch",
                    &path,
                    format!(
                        "dependency reference must be a string, found {}",
                        yaml_type_name(value)
                    ),
                    index,
                );
                continue;
            };
            let dependency = raw.trim().to_string();
            if dependency.is_empty() {
                self.push(
                    "error",
                    "value-empty",
                    &path,
                    "dependency reference must not be empty",
                    index,
                );
                continue;
            }
            if !seen.insert(dependency.clone()) {
                self.push(
                    "error",
                    "dep-duplicate",
                    &path,
                    format!(
                        "dependency `{dependency}` is listed more than once"
                    ),
                    index,
                );
            }
            if current_id == Some(dependency.as_str()) {
                self.push(
                    "error",
                    "dep-self",
                    &path,
                    format!("phase `{dependency}` cannot depend on itself"),
                    index,
                );
            } else if let Some(target_index) = phase_ids
                .iter()
                .position(|candidate| candidate.as_deref() == Some(&dependency))
            {
                if target_index >= phase_index {
                    self.push(
                        "error",
                        "dep-forward",
                        &path,
                        format!(
                            "dependency `{dependency}` must refer to an earlier-listed phase"
                        ),
                        index,
                    );
                }
            } else {
                self.push(
                    "error",
                    "dep-unknown",
                    &path,
                    format!("dependency `{dependency}` does not name a phase"),
                    index,
                );
            }
            dependencies.push(dependency);
        }
        dependencies
    }

    fn required_non_empty_string(
        &mut self,
        mapping: &Mapping,
        key: &str,
        index: &SourceIndex,
    ) -> Option<String> {
        self.required_non_empty_string_at(mapping, key, key, index)
    }

    fn required_non_empty_string_at(
        &mut self,
        mapping: &Mapping,
        key: &str,
        field_path: &str,
        index: &SourceIndex,
    ) -> Option<String> {
        let Some(value) = mapping_value(mapping, key) else {
            self.push(
                "error",
                "required-missing",
                field_path,
                format!("required field `{key}` is missing"),
                index,
            );
            return None;
        };
        self.non_empty_string_value(value, key, field_path, index)
    }

    fn optional_non_empty_string(
        &mut self,
        mapping: &Mapping,
        key: &str,
        index: &SourceIndex,
    ) -> Option<String> {
        let value = mapping_value(mapping, key)?;
        self.non_empty_string_value(value, key, key, index)
    }

    fn non_empty_string_value(
        &mut self,
        value: &YamlValue,
        key: &str,
        field_path: &str,
        index: &SourceIndex,
    ) -> Option<String> {
        let Some(value) = value.as_str() else {
            self.push(
                "error",
                "type-mismatch",
                field_path,
                format!(
                    "field `{key}` must be a string, found {}",
                    yaml_type_name(value)
                ),
                index,
            );
            return None;
        };
        let value = value.trim().to_string();
        if value.is_empty() {
            self.push(
                "error",
                "value-empty",
                field_path,
                format!("field `{key}` must not be empty"),
                index,
            );
            return None;
        }
        Some(value)
    }

    fn optional_string_at(
        &mut self,
        mapping: &Mapping,
        key: &str,
        field_path: &str,
        index: &SourceIndex,
    ) -> Option<String> {
        let value = mapping_value(mapping, key)?;
        let Some(value) = value.as_str() else {
            self.push(
                "error",
                "type-mismatch",
                field_path,
                format!(
                    "field `{key}` must be a string, found {}",
                    yaml_type_name(value)
                ),
                index,
            );
            return None;
        };
        Some(value.to_string())
    }

    fn optional_model(
        &mut self,
        mapping: &Mapping,
        key: &str,
        index: &SourceIndex,
    ) -> Option<String> {
        self.optional_model_at(mapping, key, key, index)
    }

    fn optional_model_at(
        &mut self,
        mapping: &Mapping,
        key: &str,
        field_path: &str,
        index: &SourceIndex,
    ) -> Option<String> {
        let value = mapping_value(mapping, key)?;
        let value =
            self.non_empty_string_value(value, key, field_path, index)?;
        if let Err(error) = validate_model_value(&value) {
            self.push(
                "error",
                "model-invalid",
                field_path,
                error.message,
                index,
            );
            return None;
        }
        Some(value)
    }

    fn optional_integer(
        &mut self,
        mapping: &Mapping,
        key: &str,
        index: &SourceIndex,
    ) -> Option<i64> {
        let value = mapping_value(mapping, key)?;
        let Some(value) = value.as_i64() else {
            self.push(
                "error",
                "type-mismatch",
                key,
                format!(
                    "field `{key}` must be an integer, found {}",
                    yaml_type_name(value)
                ),
                index,
            );
            return None;
        };
        Some(value)
    }

    fn push(
        &mut self,
        severity: &str,
        code: &str,
        field_path: &str,
        message: impl Into<String>,
        index: &SourceIndex,
    ) {
        self.push_direct(
            severity,
            code,
            field_path,
            message,
            index.line_for(field_path),
        );
    }

    fn push_direct(
        &mut self,
        severity: &str,
        code: &str,
        field_path: &str,
        message: impl Into<String>,
        line: Option<u64>,
    ) {
        self.diagnostics.push(PlanDiagnosticWire {
            severity: severity.to_string(),
            code: code.to_string(),
            field_path: field_path.to_string(),
            message: message.into(),
            line,
        });
    }

    fn finish(
        self,
        plan: Option<ValidatedPlanWire>,
    ) -> PlanValidationResultWire {
        let ok = !self
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.severity == "error");
        PlanValidationResultWire {
            schema_version: PLAN_WIRE_SCHEMA_VERSION,
            ok,
            diagnostics: self.diagnostics,
            plan: ok.then_some(plan).flatten(),
        }
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

fn is_slug(value: &str) -> bool {
    let mut previous_was_separator = true;
    for character in value.chars() {
        if character.is_ascii_lowercase() || character.is_ascii_digit() {
            previous_was_separator = false;
        } else if matches!(character, '-' | '_') && !previous_was_separator {
            previous_was_separator = true;
        } else {
            return false;
        }
    }
    !previous_was_separator
}

/// Best-effort YAML source index. Exact source spans are not available from
/// `serde_yaml::Value`, so this records key lines and falls back to the nearest
/// containing phase or top-level field for flow-style YAML.
struct SourceIndex {
    lines: BTreeMap<String, u64>,
}

impl SourceIndex {
    fn new(yaml: &str) -> Self {
        let mut lines = BTreeMap::new();
        let mut in_phases = false;
        let mut phase_index = None;

        for (yaml_line, raw_line) in yaml.lines().enumerate() {
            let file_line = yaml_line as u64 + 2;
            let raw_line = raw_line.trim_end_matches('\r');
            let trimmed = raw_line.trim_start();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }
            let indent = raw_line.len() - trimmed.len();
            if indent == 0 {
                phase_index = None;
                if let Some(key) = yaml_key(trimmed) {
                    lines.entry(key.to_string()).or_insert(file_line);
                    in_phases = key == "phases";
                } else {
                    in_phases = false;
                }
                continue;
            }
            if !in_phases {
                continue;
            }
            if let Some(after_dash) = trimmed.strip_prefix('-') {
                let next = phase_index.map_or(0, |index| index + 1);
                phase_index = Some(next);
                lines.insert(format!("phases[{next}]"), file_line);
                if let Some(key) = yaml_key(after_dash.trim_start()) {
                    lines.insert(format!("phases[{next}].{key}"), file_line);
                }
            } else if let (Some(phase_index), Some(key)) =
                (phase_index, yaml_key(trimmed))
            {
                lines.insert(format!("phases[{phase_index}].{key}"), file_line);
            }
        }
        Self { lines }
    }

    fn line_for(&self, field_path: &str) -> Option<u64> {
        if field_path.is_empty() {
            return Some(1);
        }
        let mut candidate = field_path;
        loop {
            if let Some(line) = self.lines.get(candidate) {
                return Some(*line);
            }
            if let Some(index) = candidate.rfind('[') {
                if candidate.ends_with(']') {
                    candidate = &candidate[..index];
                    continue;
                }
            }
            if let Some(index) = candidate.rfind('.') {
                candidate = &candidate[..index];
                continue;
            }
            if candidate.starts_with("phases[") {
                candidate = "phases";
                continue;
            }
            return Some(1);
        }
    }
}

fn yaml_key(text: &str) -> Option<&str> {
    let colon = text.find(':')?;
    let key = text[..colon].trim();
    if key.is_empty() || key.starts_with(['{', '[']) {
        return None;
    }
    Some(
        key.strip_prefix('"')
            .and_then(|key| key.strip_suffix('"'))
            .or_else(|| {
                key.strip_prefix('\'')
                    .and_then(|key| key.strip_suffix('\''))
            })
            .unwrap_or(key),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn codes(result: &PlanValidationResultWire) -> Vec<&str> {
        result
            .diagnostics
            .iter()
            .map(|diagnostic| diagnostic.code.as_str())
            .collect()
    }

    fn valid_tale() -> &'static str {
        "---\ntier: tale\ntitle: Ship the feature\ngoal: Ship the feature\n---\n# Plan\nDo it.\n"
    }

    fn valid_epic() -> &'static str {
        "---\ntier: epic\ntitle: Validation engine\ngoal: Plans validate deterministically\nmodel: claude/opus\nchangespec: plan_validate\nbug_id: 61\nparent_bead: ' sase-7z.1 '\nphases:\n  - id: core\n    title: Core validator\n    depends_on: []\n    description: Core validator section builds the shared validation engine.\n    size: medium\n  - id: parity\n    title: Parity coverage\n    depends_on: [core]\n    description: Parity coverage section exercises the binding.\n    size: small\n    model: claude/haiku\n---\n# Plan\nImplement it.\n"
    }

    #[test]
    fn valid_tale_returns_normalized_plan_and_accepts_system_fields() {
        let content = "---\ncreate_time: [system, value]\nstatus: {state: wip}\nprompt: null\nbead_id: 42\ntier: tale\ntitle: ' Ship the feature '\ngoal: ' Ship the feature '\nmodel: codex/gpt-5.6-sol\n---\n# Plan\nDo it.\n";
        let result = plan_validate(content, "tale").unwrap();
        assert!(result.ok);
        assert!(result.diagnostics.is_empty());
        assert_eq!(result.schema_version, PLAN_WIRE_SCHEMA_VERSION);
        assert_eq!(
            result.plan,
            Some(ValidatedPlanWire {
                tier: "tale".to_string(),
                goal: "Ship the feature".to_string(),
                model: Some("codex/gpt-5.6-sol".to_string()),
                title: Some("Ship the feature".to_string()),
                phases: Vec::new(),
                changespec: None,
                bug_id: None,
                parent_bead: None,
            })
        );
    }

    #[test]
    fn structural_diagnostics_cover_each_rule() {
        let missing = plan_validate("# Plan\n", "tale").unwrap();
        assert_eq!(codes(&missing), ["frontmatter-missing"]);

        let unclosed = plan_validate("---\ntier: tale\n", "tale").unwrap();
        assert_eq!(codes(&unclosed), ["frontmatter-unclosed"]);

        let invalid_yaml =
            plan_validate("---\ntier: [tale\n---\n", "tale").unwrap();
        assert_eq!(codes(&invalid_yaml), ["body-empty", "yaml-invalid"]);
        assert_eq!(invalid_yaml.diagnostics[1].line, Some(3));

        let not_mapping =
            plan_validate("---\n- tale\n---\nbody\n", "tale").unwrap();
        assert_eq!(codes(&not_mapping), ["frontmatter-not-mapping"]);

        let empty_body =
            plan_validate("---\ntier: tale\ntitle: x\ngoal: x\n---\n", "tale")
                .unwrap();
        assert_eq!(codes(&empty_body), ["body-empty"]);
    }

    #[test]
    fn common_field_rules_report_together_with_locations() {
        let content = "---\ntier: epic\ntitle: Plan title\ngoal: '   '\nmodel: |\n  bad\n  model\ntyop: value\n---\n# Plan\n";
        let result = plan_validate(content, "tale").unwrap();
        assert!(!result.ok);
        assert_eq!(
            codes(&result),
            [
                "unknown-key",
                "tier-mismatch",
                "value-empty",
                "model-invalid"
            ]
        );
        assert_eq!(result.diagnostics[0].field_path, "tyop");
        assert_eq!(result.diagnostics[0].line, Some(8));
        assert!(result.plan.is_none());
    }

    #[test]
    fn missing_wrong_type_and_invalid_tier_are_distinct() {
        let content =
            "---\ntier: story\ntitle: Plan title\ngoal: [not, text]\n---\nbody\n";
        let result = plan_validate(content, "tale").unwrap();
        assert_eq!(codes(&result), ["tier-invalid", "type-mismatch"]);

        let missing =
            plan_validate("---\nstatus: wip\n---\nbody\n", "tale").unwrap();
        assert_eq!(
            codes(&missing),
            ["required-missing", "required-missing", "required-missing"]
        );
        assert_eq!(missing.diagnostics[0].field_path, "tier");
        assert_eq!(missing.diagnostics[1].field_path, "title");
        assert_eq!(missing.diagnostics[2].field_path, "goal");
    }

    #[test]
    fn tale_epic_fields_are_inert_warnings() {
        let content = "---\ntier: tale\ntitle: Small plan\ngoal: Small outcome\nphases: nonsense\nchangespec: ''\nbug_id: nope\nparent_bead: sase-7z.1\n---\nbody\n";
        let result = plan_validate(content, "tale").unwrap();
        assert!(result.ok);
        assert_eq!(
            codes(&result),
            [
                "tale-inert-field",
                "tale-inert-field",
                "tale-inert-field",
                "tale-inert-field"
            ]
        );
        assert!(result
            .diagnostics
            .iter()
            .all(|diagnostic| diagnostic.severity == "warning"));
        assert!(result.plan.is_some());
    }

    #[test]
    fn both_tiers_require_a_non_empty_string_title() {
        for (tier, extra) in [
            ("tale", ""),
            (
                "epic",
                "phases:\n  - id: core\n    title: Core\n    depends_on: []\n    size: small\n",
            ),
        ] {
            for (title_line, expected_code) in [
                ("", "required-missing"),
                ("title: '   '\n", "value-empty"),
                ("title: [not, text]\n", "type-mismatch"),
            ] {
                let content = format!(
                    "---\ntier: {tier}\n{title_line}goal: outcome\n{extra}---\nbody\n"
                );
                let result = plan_validate(&content, tier).unwrap();
                assert!(!result.ok, "{tier}: {title_line:?}");
                assert!(result.diagnostics.iter().any(|diagnostic| {
                    diagnostic.field_path == "title"
                        && diagnostic.code == expected_code
                        && diagnostic.line.is_some()
                }));
            }
        }
    }

    #[test]
    fn valid_epic_returns_all_normalized_fields() {
        let result = plan_validate(valid_epic(), "epic").unwrap();
        assert!(result.ok, "{:?}", result.diagnostics);
        assert!(result.diagnostics.is_empty());
        let plan = result.plan.unwrap();
        assert_eq!(plan.title.as_deref(), Some("Validation engine"));
        assert_eq!(plan.changespec.as_deref(), Some("plan_validate"));
        assert_eq!(plan.bug_id, Some(61));
        assert_eq!(plan.parent_bead.as_deref(), Some("sase-7z.1"));
        assert_eq!(plan.phases.len(), 2);
        assert_eq!(plan.phases[0].size, "medium");
        assert_eq!(plan.phases[1].depends_on, ["core"]);
        assert_eq!(plan.phases[1].model.as_deref(), Some("claude/haiku"));
    }

    #[test]
    fn missing_phase_description_warns_without_changing_the_plan() {
        let content = "---\ntier: epic\ntitle: Validation engine\ngoal: Plans validate deterministically\nphases:\n  - id: core\n    title: Core validator\n    depends_on: []\n    size: small\n---\n# Plan\nImplement it.\n";
        let result = plan_validate(content, "epic").unwrap();

        assert!(result.ok);
        assert_eq!(result.schema_version, PLAN_WIRE_SCHEMA_VERSION);
        assert_eq!(
            result.diagnostics,
            [PlanDiagnosticWire {
                severity: "warning".to_string(),
                code: "phase-description-missing".to_string(),
                field_path: "phases[0].description".to_string(),
                message: "phase `core` has no `description`; add one naming its plan-body section and briefly summarizing that section".to_string(),
                line: Some(6),
            }]
        );
        assert_eq!(
            result.plan,
            Some(ValidatedPlanWire {
                tier: "epic".to_string(),
                goal: "Plans validate deterministically".to_string(),
                model: None,
                title: Some("Validation engine".to_string()),
                phases: vec![PlanPhaseWire {
                    id: "core".to_string(),
                    title: "Core validator".to_string(),
                    depends_on: Vec::new(),
                    description: None,
                    size: "small".to_string(),
                    model: None,
                }],
                changespec: None,
                bug_id: None,
                parent_bead: None,
            })
        );
    }

    #[test]
    fn epic_top_level_rules_all_report() {
        let content = "---\ntier: epic\ngoal: outcome\ntitle: 42\nchangespec: ''\nbug_id: nope\nphases: []\n---\nbody\n";
        let result = plan_validate(content, "epic").unwrap();
        assert_eq!(
            codes(&result),
            [
                "type-mismatch",
                "value-empty",
                "type-mismatch",
                "bug-id-without-changespec",
                "phases-empty"
            ]
        );

        let bug_without_changespec = "---\ntier: epic\ngoal: outcome\ntitle: title\nbug_id: 7\nphases:\n  - id: core\n    title: Core\n    depends_on: []\n    size: small\n---\nbody\n";
        assert!(
            codes(&plan_validate(bug_without_changespec, "epic").unwrap())
                .contains(&"bug-id-without-changespec")
        );
    }

    #[test]
    fn phase_shape_keys_and_required_fields_report() {
        let content = "---\ntier: epic\ngoal: outcome\ntitle: title\nphases:\n  - nope\n  - id: core\n    surprise: true\n---\nbody\n";
        let result = plan_validate(content, "epic").unwrap();
        assert_eq!(
            codes(&result),
            [
                "phase-type-mismatch",
                "unknown-key",
                "required-missing",
                "required-missing",
                "phase-description-missing",
                "phase-size-missing"
            ]
        );
        assert_eq!(result.diagnostics[1].field_path, "phases[1].surprise");
    }

    #[test]
    fn phase_ids_and_dependency_graph_rules_report_in_one_pass() {
        let content = "---\ntier: epic\ngoal: outcome\ntitle: title\nphases:\n  - id: Bad slug\n    title: First\n    depends_on: [Bad slug, future, missing, missing]\n    size: small\n  - id: future\n    title: Future\n    depends_on: []\n    size: medium\n  - id: future\n    title: Duplicate\n    depends_on: nope\n    size: large\n---\nbody\n";
        let result = plan_validate(content, "epic").unwrap();
        let codes = codes(&result);
        for expected in [
            "phase-id-invalid",
            "phase-id-duplicate",
            "dep-self",
            "dep-forward",
            "dep-unknown",
            "dep-duplicate",
            "type-mismatch",
        ] {
            assert!(codes.contains(&expected), "missing {expected}: {codes:?}");
        }
        assert!(result.diagnostics.iter().any(|diagnostic| {
            diagnostic.field_path == "phases[0].depends_on[1]"
                && diagnostic.line == Some(8)
        }));
    }

    #[test]
    fn phase_optional_fields_validate_types_and_model_syntax() {
        let content = "---\ntier: epic\ngoal: outcome\ntitle: title\nphases:\n  - id: core\n    title: Core\n    depends_on: []\n    description: [wrong]\n    size: small\n    model: |\n      bad\n      model\n---\nbody\n";
        let result = plan_validate(content, "epic").unwrap();
        assert_eq!(codes(&result), ["type-mismatch", "model-invalid"]);
    }

    #[test]
    fn epic_missing_collection_and_phase_scalar_rules_report() {
        let missing = plan_validate(
            "---\ntier: epic\ngoal: outcome\n---\nbody\n",
            "epic",
        )
        .unwrap();
        assert_eq!(codes(&missing), ["required-missing", "required-missing"]);
        assert_eq!(missing.diagnostics[0].field_path, "title");
        assert_eq!(missing.diagnostics[1].field_path, "phases");

        let wrong_collection = "---\ntier: epic\ngoal: outcome\ntitle: title\nphases: {}\n---\nbody\n";
        assert_eq!(
            codes(&plan_validate(wrong_collection, "epic").unwrap()),
            ["type-mismatch"]
        );

        let invalid_scalars = "---\ntier: epic\ngoal: outcome\ntitle: title\nmodel: ''\nphases:\n  - id: ''\n    title: ''\n    depends_on: [1, '']\n    size: enormous\n    model: ''\n---\nbody\n";
        assert_eq!(
            codes(&plan_validate(invalid_scalars, "epic").unwrap()),
            [
                "value-empty",
                "value-empty",
                "value-empty",
                "type-mismatch",
                "value-empty",
                "phase-description-missing",
                "phase-size-invalid",
                "value-empty"
            ]
        );
    }

    #[test]
    fn phase_size_is_strict_for_authoring_and_legacy_safe_for_launch() {
        let missing = "---\ntier: epic\ntitle: Legacy epic\ngoal: Resume it safely\nphases:\n  - id: core\n    title: Core\n    depends_on: []\n    description: Core section resumes legacy work.\n---\nbody\n";
        let authoring = plan_validate(missing, "epic").unwrap();
        assert!(!authoring.ok);
        assert_eq!(codes(&authoring), ["phase-size-missing"]);
        assert_eq!(authoring.diagnostics[0].severity, "error");

        let launch =
            plan_validate_with_mode(missing, "epic", "launch").unwrap();
        assert!(launch.ok);
        assert_eq!(codes(&launch), ["phase-size-missing"]);
        assert_eq!(launch.diagnostics[0].severity, "warning");
        assert_eq!(
            launch.plan.unwrap().phases[0].size,
            "small",
            "legacy launch validation must normalize missing size"
        );

        for invalid_size in ["enormous", "[]"] {
            let invalid = missing.replace(
                "    description: Core section resumes legacy work.\n",
                &format!(
                    "    description: Core section resumes legacy work.\n    size: {invalid_size}\n"
                ),
            );
            for mode in ["authoring", "launch"] {
                let result =
                    plan_validate_with_mode(&invalid, "epic", mode).unwrap();
                assert!(!result.ok, "{mode}: {invalid_size}");
                assert_eq!(codes(&result), ["phase-size-invalid"]);
            }
        }
    }

    #[test]
    fn parent_bead_is_optional_epic_only_and_type_checked() {
        let result = plan_validate(valid_epic(), "epic").unwrap();
        assert_eq!(
            result.plan.unwrap().parent_bead.as_deref(),
            Some("sase-7z.1")
        );

        let invalid = valid_epic()
            .replace("parent_bead: ' sase-7z.1 '", "parent_bead: [sase-7z.1]");
        let result = plan_validate(&invalid, "epic").unwrap();
        assert!(!result.ok);
        assert!(result.diagnostics.iter().any(|diagnostic| {
            diagnostic.field_path == "parent_bead"
                && diagnostic.code == "type-mismatch"
        }));
    }

    #[test]
    fn schema_is_ordered_and_contains_exact_phase_guidance() {
        let tale = plan_frontmatter_schema("tale").unwrap();
        assert_eq!(
            tale.iter()
                .map(|field| field.name.as_str())
                .collect::<Vec<_>>(),
            [
                "tier",
                "title",
                "goal",
                "model",
                "create_time",
                "status",
                "prompt",
                "bead_id"
            ]
        );
        let epic = plan_frontmatter_schema("epic").unwrap();
        assert_eq!(
            epic.iter()
                .map(|field| field.name.as_str())
                .collect::<Vec<_>>(),
            [
                "tier",
                "title",
                "goal",
                "model",
                "phases",
                "phases[].id",
                "phases[].title",
                "phases[].depends_on",
                "phases[].description",
                "phases[].size",
                "phases[].model",
                "changespec",
                "bug_id",
                "parent_bead",
                "create_time",
                "status",
                "prompt",
                "bead_id"
            ]
        );
        let phase_description = epic
            .iter()
            .find(|field| field.name == "phases[].description")
            .unwrap();
        assert_eq!(
            phase_description.description,
            PHASE_DESCRIPTION_DESCRIPTION
        );
        assert_eq!(
            phase_description.example,
            json!("'Core validator' section: build the shared validation engine and its JSON wire.")
        );
        let phase_model = epic
            .iter()
            .find(|field| field.name == "phases[].model")
            .unwrap();
        assert_eq!(phase_model.description, PHASE_MODEL_DESCRIPTION);
        let phase_size = epic
            .iter()
            .find(|field| field.name == "phases[].size")
            .unwrap();
        assert_eq!(phase_size.description, PHASE_SIZE_DESCRIPTION);
        assert_eq!(phase_size.field_type, "small | medium | large");
        assert!(phase_size.required);
        assert!(
            !epic
                .iter()
                .find(|field| field.name == "parent_bead")
                .unwrap()
                .required
        );
        assert!(
            epic.iter()
                .find(|field| field.name == "phases[].depends_on")
                .unwrap()
                .required
        );
    }

    #[test]
    fn unsupported_caller_tier_is_a_usage_error() {
        let error = plan_validate(valid_tale(), "story").unwrap_err();
        assert_eq!(error.kind, "validation");
        assert!(error.message.contains("tale"));
        let error = plan_validate_with_mode(valid_tale(), "tale", "resume")
            .unwrap_err();
        assert!(error.message.contains("authoring"));
        assert!(plan_frontmatter_schema("story").is_err());
    }
}
