//! Repository-creation form evaluation: availability, defaults, and
//! strict vs lenient option errors.

use std::collections::{BTreeMap, HashMap, HashSet};

use super::wire::{
    CreationFormErrorWire, CreationFormFieldStateWire, CreationFormRequestWire,
    CreationFormStateWire, RepoCreationChoiceWire, RepoCreationFieldWire,
    ERROR_CONFLICT, ERROR_DISABLED_CHOICE, ERROR_INVALID_CHOICE,
    ERROR_REQUIRED, ERROR_REQUIRES, ERROR_UNKNOWN_OPTION, FIELD_KIND_BOOL,
    FIELD_KIND_CHOICE, FIELD_KIND_LINE, FIELD_KIND_SEARCH, FIELD_KIND_TEXT,
    FORM_MODE_LENIENT, FORM_MODE_STRICT,
    PROJECT_ONBOARDING_WIRE_SCHEMA_VERSION,
};
use super::ProjectOnboardingError;

/// Evaluate a provider creation form against caller-supplied values.
pub fn evaluate_repo_creation_form(
    request: &CreationFormRequestWire,
) -> Result<CreationFormStateWire, ProjectOnboardingError> {
    ProjectOnboardingError::check_schema(request.schema_version)?;
    let strict = match request.mode.trim() {
        "" | FORM_MODE_LENIENT => false,
        FORM_MODE_STRICT => true,
        other => {
            return Err(ProjectOnboardingError::Validation(format!(
                "unsupported creation form mode: {other}"
            )));
        }
    };
    let fields_by_name = unique_fields(&request.form.fields)?;
    let mut errors = Vec::new();
    for key in request.values.keys() {
        if !fields_by_name.contains_key(key.as_str()) {
            errors.push(CreationFormErrorWire {
                field: key.clone(),
                code: ERROR_UNKNOWN_OPTION.to_string(),
                message: format!("unknown option {key}"),
            });
        }
    }

    let mut resolved = HashMap::<&str, ResolvedField<'_>>::new();
    for field in &request.form.fields {
        let supplied = request.values.get(&field.name).cloned();
        let raw = supplied.clone().unwrap_or_else(|| field.default.clone());
        let (value, bool_ok) = normalize_value(field, &raw);
        resolved.insert(
            field.name.as_str(),
            ResolvedField {
                field,
                supplied,
                value,
                bool_ok,
            },
        );
    }

    let set_names = set_field_names(&resolved);
    let mut states = Vec::new();
    let mut effective = BTreeMap::new();
    for field in &request.form.fields {
        let Some(item) = resolved.get(field.name.as_str()) else {
            continue;
        };
        let (available, reason, unavail_kind) =
            availability(item.field, &resolved, &set_names);
        let is_set = field_is_set(item.field, &item.value);
        if !item.bool_ok {
            errors.push(CreationFormErrorWire {
                field: field.name.clone(),
                code: ERROR_INVALID_CHOICE.to_string(),
                message: format!(
                    "{} is not a valid boolean",
                    field_label(item.field)
                ),
            });
        }
        if available {
            push_choice_errors(&mut errors, item);
            if item.field.required && !is_set {
                errors.push(CreationFormErrorWire {
                    field: field.name.clone(),
                    code: ERROR_REQUIRED.to_string(),
                    message: format!("{} is required", field_label(item.field)),
                });
            }
            if is_set
                || (!item.value.is_empty() && item.value == item.field.default)
            {
                effective.insert(field.name.clone(), item.value.clone());
            }
        } else if strict {
            if let Some(supplied) = item.supplied.as_ref() {
                let (normalized, _) = normalize_value(item.field, supplied);
                if normalized != item.field.default {
                    match unavail_kind {
                        UnavailKind::Requires => {
                            errors.push(CreationFormErrorWire {
                                field: field.name.clone(),
                                code: ERROR_REQUIRES.to_string(),
                                message: reason.clone(),
                            });
                        }
                        UnavailKind::Conflict | UnavailKind::Disabled => {
                            errors.push(CreationFormErrorWire {
                                field: field.name.clone(),
                                code: ERROR_CONFLICT.to_string(),
                                message: reason.clone(),
                            });
                        }
                    }
                }
            }
        }
        states.push(CreationFormFieldStateWire {
            name: field.name.clone(),
            available,
            reason,
            value: item.value.clone(),
            is_set,
        });
    }

    let valid = errors.is_empty();
    Ok(CreationFormStateWire {
        schema_version: PROJECT_ONBOARDING_WIRE_SCHEMA_VERSION,
        fields: states,
        effective_options: effective,
        errors,
        valid,
    })
}

struct ResolvedField<'a> {
    field: &'a RepoCreationFieldWire,
    supplied: Option<String>,
    value: String,
    bool_ok: bool,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum UnavailKind {
    Disabled,
    Conflict,
    Requires,
}

fn unique_fields(
    fields: &[RepoCreationFieldWire],
) -> Result<HashMap<&str, &RepoCreationFieldWire>, ProjectOnboardingError> {
    let mut map = HashMap::new();
    for field in fields {
        if field.name.is_empty() {
            return Err(ProjectOnboardingError::Validation(
                "creation form field is missing a name".to_string(),
            ));
        }
        if !is_known_kind(&field.kind) {
            return Err(ProjectOnboardingError::Validation(format!(
                "unsupported creation field kind: {}",
                field.kind
            )));
        }
        if map.insert(field.name.as_str(), field).is_some() {
            return Err(ProjectOnboardingError::Validation(format!(
                "duplicate creation form field: {}",
                field.name
            )));
        }
    }
    Ok(map)
}

fn is_known_kind(kind: &str) -> bool {
    matches!(
        kind,
        FIELD_KIND_LINE
            | FIELD_KIND_TEXT
            | FIELD_KIND_BOOL
            | FIELD_KIND_CHOICE
            | FIELD_KIND_SEARCH
            | ""
    )
}

fn normalize_value(field: &RepoCreationFieldWire, raw: &str) -> (String, bool) {
    if field.kind == FIELD_KIND_BOOL {
        if raw.trim().is_empty() {
            return (String::new(), true);
        }
        match parse_bool(raw) {
            Some(true) => ("true".to_string(), true),
            Some(false) => ("false".to_string(), true),
            None => (raw.to_string(), false),
        }
    } else {
        (raw.to_string(), true)
    }
}

fn parse_bool(raw: &str) -> Option<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "true" | "yes" | "on" | "1" => Some(true),
        "false" | "no" | "off" | "0" => Some(false),
        _ => None,
    }
}

fn field_is_set(field: &RepoCreationFieldWire, value: &str) -> bool {
    if field.kind == FIELD_KIND_BOOL {
        value == "true"
    } else {
        !value.is_empty()
    }
}

fn set_field_names<'a>(
    resolved: &HashMap<&'a str, ResolvedField<'a>>,
) -> HashSet<&'a str> {
    resolved
        .iter()
        .filter(|(_, item)| field_is_set(item.field, &item.value))
        .map(|(name, _)| *name)
        .collect()
}

fn availability(
    field: &RepoCreationFieldWire,
    resolved: &HashMap<&str, ResolvedField<'_>>,
    set_names: &HashSet<&str>,
) -> (bool, String, UnavailKind) {
    if !field.disabled_reason.is_empty() {
        return (false, field.disabled_reason.clone(), UnavailKind::Disabled);
    }
    for (name, item) in resolved {
        if *name == field.name.as_str() {
            continue;
        }
        if !set_names.contains(name) {
            continue;
        }
        if item.field.excludes.iter().any(|other| other == &field.name) {
            return (
                false,
                format!("not available with {}", field_label(item.field)),
                UnavailKind::Conflict,
            );
        }
    }
    for required in &field.requires {
        if !set_names.contains(required.as_str()) {
            let label = resolved
                .get(required.as_str())
                .map(|item| field_label(item.field))
                .unwrap_or(required.as_str());
            return (false, format!("requires {label}"), UnavailKind::Requires);
        }
    }
    (true, String::new(), UnavailKind::Disabled)
}

fn push_choice_errors(
    errors: &mut Vec<CreationFormErrorWire>,
    item: &ResolvedField<'_>,
) {
    if !field_is_set(item.field, &item.value) {
        return;
    }
    let kind = item.field.kind.as_str();
    if kind == FIELD_KIND_CHOICE {
        match find_choice(item.field, &item.value) {
            None => errors.push(CreationFormErrorWire {
                field: item.field.name.clone(),
                code: ERROR_INVALID_CHOICE.to_string(),
                message: format!(
                    "{} is not a valid choice",
                    field_label(item.field)
                ),
            }),
            Some(choice) if !choice.disabled_reason.is_empty() => {
                errors.push(CreationFormErrorWire {
                    field: item.field.name.clone(),
                    code: ERROR_DISABLED_CHOICE.to_string(),
                    message: choice.disabled_reason.clone(),
                });
            }
            Some(_) => {}
        }
        return;
    }
    if kind == FIELD_KIND_SEARCH {
        if let Some(choice) = find_choice(item.field, &item.value) {
            if !choice.disabled_reason.is_empty() {
                errors.push(CreationFormErrorWire {
                    field: item.field.name.clone(),
                    code: ERROR_DISABLED_CHOICE.to_string(),
                    message: choice.disabled_reason.clone(),
                });
            }
        }
    }
}

fn find_choice<'a>(
    field: &'a RepoCreationFieldWire,
    value: &str,
) -> Option<&'a RepoCreationChoiceWire> {
    field.choices.iter().find(|choice| choice.value == value)
}

fn field_label(field: &RepoCreationFieldWire) -> &str {
    if field.label.is_empty() {
        field.name.as_str()
    } else {
        field.label.as_str()
    }
}
