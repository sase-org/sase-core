//! Scope variables backing the unknown-variable lint.
//!
//! `known` holds every variable-slot name that is available or
//! conditional, ignoring template locals (the lint's Jinja AST already
//! handles those), plus `loop`. `positional_pattern` means any
//! `_<digits>` is known. `unavailable` pairs each unavailable name
//! with its reason.

use std::collections::HashSet;

use super::assist::{
    evaluate_rule, macro_only_reason, unavailable_reason, Availability,
};
use super::catalog::jinja_catalog;
use super::scope::jinja_document_scope;
use super::wire::{
    JinjaScopeKind, JinjaScopeRequestWire, JinjaScopeVariablesWire,
    JinjaUnavailableWire,
};

/// Scope-variable list for lint and input inference.
pub fn jinja_scope_variables(
    req: &JinjaScopeRequestWire,
) -> JinjaScopeVariablesWire {
    let scope = jinja_document_scope(
        &req.text,
        req.text.len(),
        req.frontmatter.as_deref(),
    );
    let mut known = Vec::new();
    let mut seen = HashSet::new();
    let mut unavailable = Vec::new();
    let mut unavailable_seen = HashSet::new();
    for input in &scope.inputs {
        if seen.insert(input.name.clone()) {
            known.push(input.name.clone());
        }
    }
    let catalog = jinja_catalog();
    for variable in &catalog.variables {
        if variable.name == "loop" {
            continue;
        }
        let availability =
            evaluate_rule(variable.availability_rule, req.scope, &scope);
        match availability {
            Availability::Available | Availability::Conditional => {
                if seen.insert(variable.name.clone()) {
                    known.push(variable.name.clone());
                }
            }
            Availability::Unavailable => {
                // A declared input (or otherwise known name) wins over
                // the builtin: never list a known name as unavailable.
                if seen.contains(&variable.name) {
                    continue;
                }
                push_unavailable(
                    &mut unavailable,
                    &mut unavailable_seen,
                    variable.name.clone(),
                    unavailable_reason(
                        variable.availability_rule,
                        req.scope,
                        &scope,
                    ),
                );
            }
        }
    }
    // Dynamic positional `_1` .. `_k`.
    let input_count = scope.inputs.len();
    let positional_count = input_count.max(1);
    if req.scope == JinjaScopeKind::Xprompt {
        if seen.insert("_args".to_string()) {
            // `_args` already pushed via catalog when available; keep order.
        }
        for index in 1..=positional_count {
            let name = format!("_{index}");
            if seen.insert(name.clone()) {
                known.push(name);
            }
        }
    } else {
        if !seen.contains("_args") {
            push_unavailable(
                &mut unavailable,
                &mut unavailable_seen,
                "_args".to_string(),
                macro_only_reason(),
            );
        }
        for index in 1..=positional_count {
            let name = format!("_{index}");
            if seen.contains(&name) {
                continue;
            }
            push_unavailable(
                &mut unavailable,
                &mut unavailable_seen,
                name,
                macro_only_reason(),
            );
        }
    }
    for global in &catalog.jinja_globals {
        if seen.insert(global.name.clone()) {
            known.push(global.name.clone());
        }
    }
    if seen.insert("loop".to_string()) {
        known.push("loop".to_string());
    }
    JinjaScopeVariablesWire {
        known,
        positional_pattern: req.scope == JinjaScopeKind::Xprompt,
        unavailable,
    }
}

fn push_unavailable(
    out: &mut Vec<JinjaUnavailableWire>,
    seen: &mut HashSet<String>,
    name: String,
    reason: String,
) {
    if seen.insert(name.clone()) {
        out.push(JinjaUnavailableWire { name, reason });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scope_variables_include_loop_and_globals() {
        let variables = jinja_scope_variables(&JinjaScopeRequestWire {
            text: "{{ x }}".to_string(),
            scope: JinjaScopeKind::Prompt,
            frontmatter: None,
        });
        assert!(variables.known.contains(&"root".to_string()));
        assert!(variables.known.contains(&"range".to_string()));
        assert!(variables.known.contains(&"loop".to_string()));
        assert!(!variables.positional_pattern);
    }

    #[test]
    fn prompt_with_inputs_lists_run_names_as_unavailable() {
        let variables = jinja_scope_variables(&JinjaScopeRequestWire {
            text: "---\ninput:\n  topic: word\n---\n{{ x }}".to_string(),
            scope: JinjaScopeKind::Prompt,
            frontmatter: None,
        });
        assert!(variables.known.contains(&"topic".to_string()));
        assert!(variables
            .unavailable
            .iter()
            .any(|entry| entry.name == "patch_name"));
    }

    #[test]
    fn shadowed_builtin_is_known_not_unavailable() {
        let variables = jinja_scope_variables(&JinjaScopeRequestWire {
            text: "---\ninput:\n  n: int\n---\n{{ x }}".to_string(),
            scope: JinjaScopeKind::Prompt,
            frontmatter: None,
        });
        assert!(variables.known.contains(&"n".to_string()));
        assert!(
            !variables.unavailable.iter().any(|entry| entry.name == "n"),
            "{:?}",
            variables.unavailable
        );
    }
}
