//! Fail-closed `%auto`/`%a` spelling classifier.
//!
//! Shared backend behavior: the typed launch extractor, editor metadata,
//! editor/LSP diagnostics, and the Python extractor all classify through
//! this module, so every surface accepts and rejects the same spellings
//! with the same message text.

use serde::{Deserialize, Serialize};

/// Colon values that enable automatic gate resolution.
pub const AUTO_APPROVAL_MODES: &[&str] = &["plan", "tale", "epic"];

/// Colon values that explicitly disable automatic approval, exactly as if
/// no `%auto` were present (the token is still stripped from the prompt).
pub const AUTO_MANUAL_VALUES: &[&str] = &["manual", "off"];

/// Stable diagnostic code for every rejected `%auto`/`%a` spelling.
pub const INVALID_AUTO_CODE: &str = "invalid-auto";

/// Which surface form an `%auto`/`%a` occurrence took.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutoDirectiveForm {
    Bare,
    Plus,
    Colon,
    Paren,
}

/// Accepted `%auto`/`%a` spelling, in launch-extractor field shape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AutoDirectiveClassification {
    pub enabled: bool,
    pub mode: Option<String>,
    pub argument: Option<String>,
}

/// Rejected `%auto`/`%a` spelling: a stable code plus the exact message
/// every surface shows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AutoDirectiveDiagnostic {
    pub code: &'static str,
    pub message: String,
}

/// Classify one `%auto`/`%a` occurrence.
///
/// `form` is the occurrence's surface form, `raw_value` its colon value
/// (`""` for bare, `"true"` for `+`), and `spelling` the source text shown
/// in rejection messages.
pub fn classify_auto_directive(
    form: AutoDirectiveForm,
    raw_value: &str,
    spelling: &str,
) -> Result<AutoDirectiveClassification, AutoDirectiveDiagnostic> {
    if form == AutoDirectiveForm::Paren {
        return Err(AutoDirectiveDiagnostic {
            code: INVALID_AUTO_CODE,
            message: format!(
                "Invalid %auto spelling '{spelling}': parenthesized \
                arguments are not supported yet. Use %auto, %auto+, or \
                %auto:<mode> with mode plan, tale, or epic; %auto:manual \
                and %auto:off disable automatic approval.",
            ),
        });
    }
    if raw_value.is_empty() || raw_value == "true" {
        return Ok(AutoDirectiveClassification {
            enabled: true,
            mode: Some("plan".to_string()),
            argument: None,
        });
    }
    if AUTO_APPROVAL_MODES.contains(&raw_value) {
        return Ok(AutoDirectiveClassification {
            enabled: true,
            mode: Some(raw_value.to_string()),
            argument: Some(raw_value.to_string()),
        });
    }
    if AUTO_MANUAL_VALUES.contains(&raw_value) {
        return Ok(AutoDirectiveClassification {
            enabled: false,
            mode: None,
            argument: None,
        });
    }
    Err(AutoDirectiveDiagnostic {
        code: INVALID_AUTO_CODE,
        message: format!(
            "Invalid %auto spelling '{spelling}': unknown auto mode \
            '{raw_value}'. Use %auto, %auto+, or %auto:<mode> with mode \
            plan, tale, or epic; %auto:manual and %auto:off disable \
            automatic approval.",
        ),
    })
}

/// Closed `%auto`/`%a` vocabulary, for completion metadata and parity tests.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AutoDirectiveVocabularyWire {
    pub modes: Vec<String>,
    pub manual_values: Vec<String>,
    pub diagnostic_code: String,
}

/// Return the closed `%auto`/`%a` vocabulary.
pub fn auto_directive_vocabulary() -> AutoDirectiveVocabularyWire {
    AutoDirectiveVocabularyWire {
        modes: AUTO_APPROVAL_MODES
            .iter()
            .map(ToString::to_string)
            .collect(),
        manual_values: AUTO_MANUAL_VALUES
            .iter()
            .map(ToString::to_string)
            .collect(),
        diagnostic_code: INVALID_AUTO_CODE.to_string(),
    }
}

/// Classification request wire for the `classify_auto_directive` binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AutoDirectiveClassifyRequestWire {
    pub form: String,
    pub value: String,
    pub spelling: String,
}

/// Classification result wire for the `classify_auto_directive` binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AutoDirectiveClassificationWire {
    pub enabled: bool,
    pub mode: Option<String>,
    pub argument: Option<String>,
}

/// Classify a binding request, mapping the form name to [`AutoDirectiveForm`].
pub fn classify_auto_directive_request(
    request: &AutoDirectiveClassifyRequestWire,
) -> Result<AutoDirectiveClassificationWire, AutoDirectiveDiagnostic> {
    let form = match request.form.as_str() {
        "bare" => AutoDirectiveForm::Bare,
        "plus" => AutoDirectiveForm::Plus,
        "colon" => AutoDirectiveForm::Colon,
        "paren" => AutoDirectiveForm::Paren,
        _ => {
            return Err(AutoDirectiveDiagnostic {
                code: INVALID_AUTO_CODE,
                message: format!(
                    "Invalid %auto spelling '{}': parenthesized arguments \
                    are not supported yet. Use %auto, %auto+, or \
                    %auto:<mode> with mode plan, tale, or epic; \
                    %auto:manual and %auto:off disable automatic approval.",
                    request.spelling,
                ),
            });
        }
    };
    classify_auto_directive(form, &request.value, &request.spelling).map(
        |classified| AutoDirectiveClassificationWire {
            enabled: classified.enabled,
            mode: classified.mode,
            argument: classified.argument,
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepted_spellings_classify_to_launch_fields() {
        for (form, value, enabled, mode, argument) in [
            (AutoDirectiveForm::Bare, "", true, Some("plan"), None),
            (AutoDirectiveForm::Plus, "true", true, Some("plan"), None),
            (
                AutoDirectiveForm::Colon,
                "true",
                true,
                Some("plan"),
                None::<&str>,
            ),
            (
                AutoDirectiveForm::Colon,
                "plan",
                true,
                Some("plan"),
                Some("plan"),
            ),
            (
                AutoDirectiveForm::Colon,
                "tale",
                true,
                Some("tale"),
                Some("tale"),
            ),
            (
                AutoDirectiveForm::Colon,
                "epic",
                true,
                Some("epic"),
                Some("epic"),
            ),
            (AutoDirectiveForm::Colon, "manual", false, None, None),
            (AutoDirectiveForm::Colon, "off", false, None, None),
        ] {
            let classified =
                classify_auto_directive(form, value, "%auto").unwrap();
            assert_eq!(classified.enabled, enabled, "value {value:?}");
            assert_eq!(classified.mode.as_deref(), mode, "value {value:?}");
            assert_eq!(
                classified.argument.as_deref(),
                argument,
                "value {value:?}"
            );
        }
    }

    #[test]
    fn rejected_spellings_name_the_spelling_and_list_modes() {
        for (form, value, spelling) in [
            (AutoDirectiveForm::Paren, "", "%auto(plan=ask)"),
            (AutoDirectiveForm::Paren, "", "%auto()"),
            (AutoDirectiveForm::Colon, "foo", "%auto:foo"),
            (AutoDirectiveForm::Colon, "first", "%auto:first"),
            (AutoDirectiveForm::Colon, "epic_plan", "%auto:epic_plan"),
            (AutoDirectiveForm::Colon, "x(plan", "%auto:x(plan=ask)"),
        ] {
            let diagnostic =
                classify_auto_directive(form, value, spelling).unwrap_err();
            assert_eq!(diagnostic.code, "invalid-auto");
            assert!(
                diagnostic.message.contains(spelling),
                "message names {spelling:?}: {}",
                diagnostic.message
            );
            for mode in ["plan", "tale", "epic"] {
                assert!(
                    diagnostic.message.contains(mode),
                    "message lists {mode:?}: {}",
                    diagnostic.message
                );
            }
        }
    }
}
