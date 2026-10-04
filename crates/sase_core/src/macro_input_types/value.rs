//! Enum membership checks for bound values and closed-set defaults.

use super::suggest::{did_you_mean_suffix, suggest_closest};
use super::{InputChoice, ResolvedInputType};

const MAX_LISTED_CHOICES: usize = 8;

/// Check one element against a resolved closed set.
///
/// With no choices this is a no-op. Matching is exact and case-sensitive;
/// a label is never a value. Near misses include did-you-mean.
pub fn check_input_value(
    resolved: &ResolvedInputType,
    name: &str,
    value: &str,
) -> Result<(), String> {
    if resolved.choices.is_empty() {
        return Ok(());
    }
    if resolved.choices.iter().any(|choice| choice.value == value) {
        return Ok(());
    }
    let listed: Vec<&str> = resolved
        .choices
        .iter()
        .map(|choice| choice.value.as_str())
        .collect();
    let suggestions = suggest_closest(
        value,
        resolved.choices.iter().map(|choice| choice.value.as_str()),
    );
    let expectation = format_expectation(resolved, &listed);
    Err(format!(
        "Argument `{name}` expects {expectation}, got `{value}`{}",
        did_you_mean_suffix(&suggestions)
    ))
}

/// Error when a closed-set default is not a string member of `choices`.
pub fn check_closed_set_default(
    default: &str,
    choices: &[InputChoice],
) -> Option<String> {
    if choices.iter().any(|choice| choice.value == default) {
        return None;
    }
    let listed: Vec<&str> =
        choices.iter().map(|choice| choice.value.as_str()).collect();
    Some(format!(
        "default `{default}` is not one of {}",
        format_choice_list(&listed)
    ))
}

fn format_expectation(resolved: &ResolvedInputType, listed: &[&str]) -> String {
    if listed.len() > MAX_LISTED_CHOICES {
        if let Some(named_type) = &resolved.named_type {
            return format!("a {named_type} value ({} choices)", listed.len());
        }
        return format!("one of {} choices", listed.len());
    }
    format!("one of {}", format_choice_list(listed))
}

pub(crate) fn format_choice_list(listed: &[&str]) -> String {
    if listed.len() > MAX_LISTED_CHOICES {
        return format!("{} choices", listed.len());
    }
    listed.join(" | ")
}
