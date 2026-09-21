use super::super::wire::{
    directive_feature_flag, DirectiveContractEntry, DirectiveMetadata,
    DirectiveSyntaxForm,
};
use super::metadata::{
    DIRECTIVES, HIDDEN_COMPLETION_DIRECTIVES, IF_DIRECTIVE_OFF,
    IF_DIRECTIVE_ON, QUEUE_DIRECTIVE_OFF, QUEUE_DIRECTIVE_ON,
};
use crate::queue_directive::queue_capacity_budget_enabled;

pub fn directive_is_hidden_from_name_completion(name: &str) -> bool {
    directive_is_hidden_from_name_completion_with_flags(name, &[])
}

/// Hide gated directives unless *enabled_feature_flags* contains their flag.
pub fn directive_is_hidden_from_name_completion_with_flags(
    name: &str,
    enabled_feature_flags: &[String],
) -> bool {
    if HIDDEN_COMPLETION_DIRECTIVES.contains(&name) {
        return true;
    }
    match directive_feature_flag(name) {
        Some(flag) => !enabled_feature_flags.iter().any(|value| value == flag),
        None => false,
    }
}

pub fn canonical_directive_name(raw: &str) -> Option<&'static str> {
    if raw == "(" || raw == "{" {
        return Some("alt");
    }
    DIRECTIVES.iter().find_map(|directive| {
        if directive.name == raw || directive.alias == Some(raw) {
            Some(directive.name)
        } else {
            None
        }
    })
}

pub fn directive_metadata(raw: &str) -> Option<&'static DirectiveMetadata> {
    directive_metadata_with_flags(raw, &[])
}

pub fn directive_metadata_with_flags(
    raw: &str,
    enabled_feature_flags: &[String],
) -> Option<&'static DirectiveMetadata> {
    let canonical = canonical_directive_name(raw)?;
    if canonical == "if" {
        return Some(if_directive_metadata(enabled_feature_flags));
    }
    if canonical == "queue" {
        return Some(queue_directive_metadata(enabled_feature_flags));
    }
    DIRECTIVES
        .iter()
        .find(|directive| directive.name == canonical)
}

pub fn if_directive_metadata(
    enabled_feature_flags: &[String],
) -> &'static DirectiveMetadata {
    if enabled_feature_flags
        .iter()
        .any(|value| value == "typed_launch_units")
    {
        &IF_DIRECTIVE_ON
    } else {
        &IF_DIRECTIVE_OFF
    }
}

pub fn queue_directive_metadata(
    enabled_feature_flags: &[String],
) -> &'static DirectiveMetadata {
    if queue_capacity_budget_enabled(enabled_feature_flags) {
        &QUEUE_DIRECTIVE_ON
    } else {
        &QUEUE_DIRECTIVE_OFF
    }
}

/// Owned JSON-shaped copy of the canonical directive completion contract.
pub fn directive_contract() -> Vec<DirectiveContractEntry> {
    directive_contract_with_flags(&[])
}

pub fn directive_contract_with_flags(
    enabled_feature_flags: &[String],
) -> Vec<DirectiveContractEntry> {
    DIRECTIVES
        .iter()
        .map(|metadata| {
            let metadata = if metadata.name == "queue" {
                queue_directive_metadata(enabled_feature_flags)
            } else if metadata.name == "if" {
                if_directive_metadata(enabled_feature_flags)
            } else {
                metadata
            };
            let mut entry = DirectiveContractEntry::from(metadata);
            if metadata.name == "queue" || metadata.name == "if" {
                entry.recipes =
                    crate::editor::wire::directive_snippet_recipes_with_flags(
                        metadata.name,
                        enabled_feature_flags,
                    );
            }
            entry
        })
        .collect()
}

pub fn directive_allows_keywords(
    metadata: &DirectiveMetadata,
    syntax_form: DirectiveSyntaxForm,
) -> bool {
    syntax_form == DirectiveSyntaxForm::Parenthesized
        && (!metadata.keywords.is_empty()
            || metadata.dynamic_keyword_role.is_some())
}

pub(crate) fn directive_argument_open_colon_at(
    text: &str,
    colon_idx: usize,
) -> bool {
    if text.as_bytes().get(colon_idx) != Some(&b':') {
        return false;
    }
    let Some(line_start) = text[..colon_idx]
        .rfind('\n')
        .map_or(Some(0), |idx| idx.checked_add(1))
    else {
        return false;
    };
    let before_colon = &text[line_start..colon_idx];
    let Some(percent_rel) = before_colon.rfind('%') else {
        return false;
    };
    let percent_idx = line_start + percent_rel;
    if !directive_left_boundary(text, percent_idx) {
        return false;
    }
    let name = &text[percent_idx + 1..colon_idx];
    if name.is_empty()
        || !name
            .bytes()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == b'_')
    {
        return false;
    }
    let Some(metadata) = directive_metadata(name) else {
        return false;
    };
    metadata.syntax_forms.contains(&DirectiveSyntaxForm::Colon)
        && metadata
            .syntax_forms
            .contains(&DirectiveSyntaxForm::Parenthesized)
}

pub(crate) fn directive_argument_open_double_colon_at(
    text: &str,
    colon_idx: usize,
) -> bool {
    if text.as_bytes().get(colon_idx..colon_idx + 2) != Some(b"::") {
        return false;
    }
    let Some(line_start) = text[..colon_idx]
        .rfind('\n')
        .map_or(Some(0), |idx| idx.checked_add(1))
    else {
        return false;
    };
    let before_colon = &text[line_start..colon_idx];
    let Some(percent_rel) = before_colon.rfind('%') else {
        return false;
    };
    let percent_idx = line_start + percent_rel;
    if !directive_left_boundary(text, percent_idx) {
        return false;
    }
    let name = &text[percent_idx + 1..colon_idx];
    if name.is_empty()
        || !name
            .bytes()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == b'_')
    {
        return false;
    }
    let Some(metadata) = directive_metadata(name) else {
        return false;
    };
    metadata
        .syntax_forms
        .contains(&DirectiveSyntaxForm::Parenthesized)
        && (metadata
            .syntax_forms
            .contains(&DirectiveSyntaxForm::DoubleColon)
            || metadata.name == "clan")
}

fn directive_left_boundary(text: &str, percent_idx: usize) -> bool {
    if percent_idx == 0 {
        return true;
    }
    let Some(previous) = text[..percent_idx].chars().next_back() else {
        return true;
    };
    previous.is_whitespace() || "([{\"'".contains(previous)
}
