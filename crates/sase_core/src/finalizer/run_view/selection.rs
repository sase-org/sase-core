//! Selection explanation and the unselected list.
//!
//! The explanation replays plan `selectors`/`required`, snapshot
//! `defaults`, and entry `selector_index` over the authority snapshot's
//! configured instance ids. Reasons are `default`, `required`, or
//! `%final:<id>`; unselected reasons are `%final:!<id>`, `%final:none`,
//! or `not default`. Only ids, provider refs, and flags are read, so the
//! secrecy rule holds: config values never reach the response.

use std::collections::{BTreeMap, BTreeSet};

use super::super::wire::{FinalizerPlanWire, FinalizerSelectorOpWire};
use super::decode::AuthorityFacts;
use super::wire::RunViewUnselectedWire;

/// One selected instance with its explanation.
#[derive(Debug, Clone)]
pub(crate) struct SelectedInstance {
    pub instance_id: String,
    pub reason: String,
}

fn push_unique(
    selected: &mut Vec<SelectedInstance>,
    instance_id: &str,
    reason: String,
) {
    // The first reason wins: an explicit default or required entry keeps
    // its own explanation when a later selector or dependency would also
    // pull it in.
    if !selected.iter().any(|item| item.instance_id == instance_id) {
        selected.push(SelectedInstance {
            instance_id: instance_id.to_string(),
            reason,
        });
    }
}

/// Replay defaults, required, selectors, and the dependency closure in
/// `resolve_finalizer_plan` order, recording why each instance runs.
pub(crate) fn replay_selection(
    plan: &FinalizerPlanWire,
    authority: &AuthorityFacts,
) -> Vec<SelectedInstance> {
    let mut selected: Vec<SelectedInstance> = Vec::new();

    let defaults = if authority.defaults.is_empty() {
        // Without a snapshot, every non-required entry reads as default.
        plan.entries
            .iter()
            .filter(|entry| !plan.required.contains(&entry.instance_id))
            .map(|entry| entry.instance_id.clone())
            .collect()
    } else {
        authority.defaults.clone()
    };
    for instance_id in &defaults {
        push_unique(&mut selected, instance_id, "default".to_string());
    }
    let required = if authority.required.is_empty() {
        plan.required.clone()
    } else {
        authority.required.clone()
    };
    for instance_id in &required {
        push_unique(&mut selected, instance_id, "required".to_string());
    }
    for selector in &plan.selectors {
        match selector {
            FinalizerSelectorOpWire::Add { instance_id } => {
                push_unique(
                    &mut selected,
                    instance_id,
                    format!("%final:{instance_id}"),
                );
            }
            FinalizerSelectorOpWire::Remove { instance_id } => {
                selected.retain(|item| item.instance_id != *instance_id);
            }
            FinalizerSelectorOpWire::Clear => {
                selected.retain(|item| required.contains(&item.instance_id));
            }
        }
    }
    // Dependency closure: a pulled-in dependency runs because its
    // depender runs, so it inherits the depender's reason.
    let after: BTreeMap<&str, &[String]> = plan
        .entries
        .iter()
        .map(|entry| (entry.instance_id.as_str(), entry.after.as_slice()))
        .collect();
    let mut cursor = 0;
    while cursor < selected.len() {
        let depender = selected[cursor].clone();
        if let Some(dependencies) = after.get(depender.instance_id.as_str()) {
            for dependency in dependencies.iter() {
                push_unique(&mut selected, dependency, depender.reason.clone());
            }
        }
        cursor += 1;
    }
    // Entries the replay missed (stale plan shapes) still get an honest
    // reason instead of vanishing from the explanation.
    for entry in &plan.entries {
        if !selected
            .iter()
            .any(|item| item.instance_id == entry.instance_id)
        {
            let reason = if plan.required.contains(&entry.instance_id) {
                "required".to_string()
            } else {
                "default".to_string()
            };
            selected.push(SelectedInstance {
                instance_id: entry.instance_id.clone(),
                reason,
            });
        }
    }
    selected
}

/// Configured-but-unselected instances with reasons, read from the
/// authority snapshot only.
pub(crate) fn unselected_instances(
    plan: &FinalizerPlanWire,
    authority: &AuthorityFacts,
    selected: &BTreeSet<String>,
) -> Vec<RunViewUnselectedWire> {
    let removed: BTreeSet<&str> = plan
        .selectors
        .iter()
        .filter_map(|selector| match selector {
            FinalizerSelectorOpWire::Remove { instance_id } => {
                Some(instance_id.as_str())
            }
            _ => None,
        })
        .collect();
    let cleared = plan
        .selectors
        .iter()
        .any(|selector| matches!(selector, FinalizerSelectorOpWire::Clear));
    let mut unselected: Vec<RunViewUnselectedWire> = authority
        .instance_refs
        .iter()
        .filter(|(instance_id, _)| !selected.contains(instance_id.as_str()))
        .map(|(instance_id, provider_ref)| {
            let reason = if removed.contains(instance_id.as_str()) {
                format!("%final:!{instance_id}")
            } else if cleared {
                "%final:none".to_string()
            } else {
                "not default".to_string()
            };
            RunViewUnselectedWire {
                instance_id: instance_id.clone(),
                provider_ref: provider_ref.clone(),
                reason,
            }
        })
        .collect();
    unselected.sort_by(|left, right| left.instance_id.cmp(&right.instance_id));
    unselected
}

#[cfg(test)]
mod tests {
    use super::super::super::selection::resolve_finalizer_plan;
    use super::super::super::wire::{
        FinalizerInstanceSpecWire, FinalizerPlanInputWire,
        FINALIZER_WIRE_SCHEMA_VERSION,
    };
    use super::super::decode::decode_plan;
    use super::*;

    fn spec(instance_id: &str, after: &[&str]) -> FinalizerInstanceSpecWire {
        serde_json::from_value(serde_json::json!({
            "schema_version": FINALIZER_WIRE_SCHEMA_VERSION,
            "instance_id": instance_id,
            "provider_ref": format!("builtin@{instance_id}"),
            "after": after,
        }))
        .unwrap()
    }

    #[test]
    fn empty_plan_selects_nothing() {
        let plan = resolve_finalizer_plan(&FinalizerPlanInputWire {
            schema_version: FINALIZER_WIRE_SCHEMA_VERSION,
            instances: Vec::new(),
            defaults: Vec::new(),
            required: Vec::new(),
            selectors: Vec::new(),
        })
        .unwrap();
        let text = serde_json::to_string(&plan).unwrap();
        let plan = decode_plan(&text).unwrap().plan;
        let authority = AuthorityFacts {
            plan_canonical: None,
            instance_refs: BTreeMap::new(),
            defaults: Vec::new(),
            required: Vec::new(),
        };
        let selected = replay_selection(&plan, &authority);
        assert!(selected.is_empty());
        assert!(unselected_instances(&plan, &authority, &BTreeSet::new())
            .is_empty());
    }

    #[test]
    fn replay_explains_required_and_selector_adds() {
        use super::super::super::wire::FinalizerSelectorOpWire;
        let plan = resolve_finalizer_plan(&FinalizerPlanInputWire {
            schema_version: FINALIZER_WIRE_SCHEMA_VERSION,
            instances: vec![
                spec("commit", &[]),
                spec("check", &["commit"]),
                spec("lint", &[]),
            ],
            defaults: vec!["commit".to_string()],
            required: vec!["check".to_string()],
            selectors: vec![FinalizerSelectorOpWire::Add {
                instance_id: "lint".to_string(),
            }],
        })
        .unwrap();
        let text = serde_json::to_string(&plan).unwrap();
        let plan = decode_plan(&text).unwrap().plan;
        let authority = AuthorityFacts {
            plan_canonical: None,
            instance_refs: BTreeMap::new(),
            defaults: vec!["commit".to_string()],
            required: vec!["check".to_string()],
        };
        let selected: BTreeMap<String, String> =
            replay_selection(&plan, &authority)
                .into_iter()
                .map(|item| (item.instance_id, item.reason))
                .collect();
        assert_eq!(
            selected,
            BTreeMap::from([
                ("commit".to_string(), "default".to_string()),
                ("check".to_string(), "required".to_string()),
                ("lint".to_string(), "%final:lint".to_string()),
            ])
        );
    }
}
