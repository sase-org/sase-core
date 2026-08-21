use std::collections::{BTreeMap, BTreeSet};

use super::digest::finalizer_digest_serializable;
use super::wire::{
    FinalizerInstanceSpecWire, FinalizerPlanEntryWire, FinalizerPlanInputWire,
    FinalizerPlanWire, FinalizerProviderSpecWire, FinalizerSelectorOpWire,
    FINALIZER_INSTANCE_ID_MAX_LEN, FINALIZER_LIST_MAX_LEN,
    FINALIZER_PROVENANCE_MAX_LEN, FINALIZER_PROVIDER_REF_MAX_LEN,
    FINALIZER_WIRE_SCHEMA_VERSION,
};
use super::FinalizerError;

pub fn validate_finalizer_provider_spec(
    spec: &FinalizerProviderSpecWire,
) -> Result<(), FinalizerError> {
    validate_schema(spec.schema_version, "provider spec")?;
    validate_provider_ref(&spec.provider_ref)?;
    validate_optional_bounded_text(
        spec.provider_version.as_deref(),
        "provider_version",
        FINALIZER_PROVENANCE_MAX_LEN,
    )?;
    validate_optional_digest(
        spec.config_schema_digest.as_deref(),
        "config_schema_digest",
    )?;
    validate_optional_digest(
        spec.submission_schema_digest.as_deref(),
        "submission_schema_digest",
    )?;
    validate_optional_digest(
        spec.result_schema_digest.as_deref(),
        "result_schema_digest",
    )?;
    validate_optional_bounded_text(
        spec.provenance_id.as_deref(),
        "provenance_id",
        FINALIZER_PROVENANCE_MAX_LEN,
    )?;
    validate_list_len(spec.capabilities.len(), "capabilities")?;
    Ok(())
}

pub fn finalizer_provider_spec_digest(
    spec: &FinalizerProviderSpecWire,
) -> Result<String, FinalizerError> {
    validate_finalizer_provider_spec(spec)?;
    finalizer_digest_serializable(spec)
}

pub fn validate_finalizer_instance_spec(
    spec: &FinalizerInstanceSpecWire,
) -> Result<(), FinalizerError> {
    validate_schema(spec.schema_version, "instance spec")?;
    validate_instance_id(&spec.instance_id)?;
    validate_provider_ref(&spec.provider_ref)?;
    validate_list_len(spec.after.len(), "after")?;
    let mut seen_after = BTreeSet::new();
    for dependency in &spec.after {
        validate_instance_id(dependency)?;
        if !seen_after.insert(dependency.as_str()) {
            return Err(FinalizerError::validation(format!(
                "instance '{}' repeats dependency '{}'",
                spec.instance_id, dependency
            )));
        }
        if dependency == &spec.instance_id {
            return Err(FinalizerError::validation(format!(
                "instance '{}' cannot depend on itself",
                spec.instance_id
            )));
        }
    }
    if spec.policy.max_attempts == 0 || spec.policy.max_attempts > 16 {
        return Err(FinalizerError::validation(format!(
            "instance '{}' max_attempts must be between 1 and 16",
            spec.instance_id
        )));
    }
    validate_optional_digest(spec.config_digest.as_deref(), "config_digest")?;
    validate_optional_bounded_text(
        spec.provenance_id.as_deref(),
        "provenance_id",
        FINALIZER_PROVENANCE_MAX_LEN,
    )?;
    Ok(())
}

pub fn finalizer_instance_spec_digest(
    spec: &FinalizerInstanceSpecWire,
) -> Result<String, FinalizerError> {
    validate_finalizer_instance_spec(spec)?;
    finalizer_digest_serializable(spec)
}

pub fn resolve_finalizer_plan(
    input: &FinalizerPlanInputWire,
) -> Result<FinalizerPlanWire, FinalizerError> {
    validate_schema(input.schema_version, "plan input")?;
    validate_list_len(input.instances.len(), "instances")?;
    validate_list_len(input.defaults.len(), "defaults")?;
    validate_list_len(input.required.len(), "required")?;
    validate_list_len(input.selectors.len(), "selectors")?;

    let mut instances = BTreeMap::new();
    for spec in &input.instances {
        validate_finalizer_instance_spec(spec)?;
        if instances
            .insert(spec.instance_id.clone(), spec.clone())
            .is_some()
        {
            return Err(FinalizerError::validation(format!(
                "duplicate finalizer instance '{}'",
                spec.instance_id
            )));
        }
    }

    for spec in instances.values() {
        for dependency in &spec.after {
            if !instances.contains_key(dependency) {
                return Err(FinalizerError::validation(format!(
                    "instance '{}' depends on unknown instance '{}'",
                    spec.instance_id, dependency
                )));
            }
        }
    }

    let mut required = Vec::new();
    let mut required_set = BTreeSet::new();
    for instance_id in &input.required {
        validate_known_instance(instance_id, &instances, "required")?;
        if required_set.insert(instance_id.clone()) {
            required.push(instance_id.clone());
        } else {
            return Err(FinalizerError::validation(format!(
                "required repeats instance '{instance_id}'"
            )));
        }
    }

    let mut selected = Vec::new();
    for instance_id in &input.defaults {
        validate_known_instance(instance_id, &instances, "defaults")?;
        push_unique(&mut selected, instance_id);
    }
    for instance_id in &required {
        push_unique(&mut selected, instance_id);
    }

    for selector in &input.selectors {
        match selector {
            FinalizerSelectorOpWire::Add { instance_id } => {
                validate_known_instance(instance_id, &instances, "selector")?;
                push_unique(&mut selected, instance_id);
            }
            FinalizerSelectorOpWire::Remove { instance_id } => {
                validate_known_instance(instance_id, &instances, "selector")?;
                if required_set.contains(instance_id) {
                    return Err(FinalizerError::validation(format!(
                        "required instance '{instance_id}' cannot be removed"
                    )));
                }
                selected.retain(|value| value != instance_id);
            }
            FinalizerSelectorOpWire::Clear => {
                if let Some(required_id) = required.first() {
                    return Err(FinalizerError::validation(format!(
                        "selector clear would remove required instance '{required_id}'"
                    )));
                }
                selected.clear();
            }
        }
    }

    let mut cursor = 0;
    while cursor < selected.len() {
        let instance_id = selected[cursor].clone();
        let spec = instances
            .get(&instance_id)
            .expect("selected instances already validated");
        for dependency in &spec.after {
            push_unique(&mut selected, dependency);
        }
        cursor += 1;
    }

    let selected_set: BTreeSet<String> = selected.iter().cloned().collect();
    for instance_id in &selected {
        let spec = instances
            .get(instance_id)
            .expect("selected instances already validated");
        for dependency in &spec.after {
            if !selected_set.contains(dependency) {
                return Err(FinalizerError::validation(format!(
                    "selected instance '{}' requires unselected dependency '{}'",
                    spec.instance_id, dependency
                )));
            }
        }
    }

    let ordered = topological_order(&selected, &instances)?;
    let mut selector_index = BTreeMap::new();
    for (idx, instance_id) in selected.iter().enumerate() {
        selector_index.insert(instance_id.clone(), idx as u32);
    }

    let entries = ordered
        .iter()
        .enumerate()
        .map(|(idx, instance_id)| {
            let spec = instances
                .get(instance_id)
                .expect("ordered instances already validated");
            FinalizerPlanEntryWire {
                instance_id: spec.instance_id.clone(),
                provider_ref: spec.provider_ref.clone(),
                after: spec.after.clone(),
                policy: spec.policy.clone(),
                config_digest: spec.config_digest.clone(),
                provenance_id: spec.provenance_id.clone(),
                selector_index: *selector_index
                    .get(instance_id)
                    .expect("selected index"),
                resolved_index: idx as u32,
            }
        })
        .collect::<Vec<_>>();

    let mut plan = FinalizerPlanWire {
        schema_version: FINALIZER_WIRE_SCHEMA_VERSION,
        entries,
        required,
        selectors: input.selectors.clone(),
        plan_digest: String::new(),
    };
    plan.plan_digest = finalizer_plan_digest(&plan)?;
    Ok(plan)
}

pub fn finalizer_plan_digest(
    plan: &FinalizerPlanWire,
) -> Result<String, FinalizerError> {
    validate_schema(plan.schema_version, "plan")?;
    let mut normalized = plan.clone();
    normalized.plan_digest.clear();
    finalizer_digest_serializable(&normalized)
}

pub fn validate_finalizer_plan(
    plan: &FinalizerPlanWire,
) -> Result<String, FinalizerError> {
    validate_schema(plan.schema_version, "plan")?;
    validate_list_len(plan.entries.len(), "entries")?;
    validate_list_len(plan.required.len(), "required")?;
    validate_list_len(plan.selectors.len(), "selectors")?;
    validate_digest(&plan.plan_digest, "plan_digest")?;

    let mut seen_ids: BTreeSet<String> = BTreeSet::new();
    let mut seen_selector_index = BTreeSet::new();
    for (idx, entry) in plan.entries.iter().enumerate() {
        validate_instance_id(&entry.instance_id)?;
        validate_provider_ref(&entry.provider_ref)?;
        validate_list_len(entry.after.len(), "after")?;
        let mut seen_after = BTreeSet::new();
        for dependency in &entry.after {
            validate_instance_id(dependency)?;
            if !seen_after.insert(dependency.as_str()) {
                return Err(FinalizerError::validation(format!(
                    "plan entry '{}' repeats dependency '{}'",
                    entry.instance_id, dependency
                )));
            }
            if dependency == &entry.instance_id {
                return Err(FinalizerError::validation(format!(
                    "plan entry '{}' cannot depend on itself",
                    entry.instance_id
                )));
            }
        }
        if entry.policy.max_attempts == 0 || entry.policy.max_attempts > 16 {
            return Err(FinalizerError::validation(format!(
                "plan entry '{}' max_attempts must be between 1 and 16",
                entry.instance_id
            )));
        }
        validate_optional_digest(
            entry.config_digest.as_deref(),
            "config_digest",
        )?;
        validate_optional_bounded_text(
            entry.provenance_id.as_deref(),
            "provenance_id",
            FINALIZER_PROVENANCE_MAX_LEN,
        )?;
        if !seen_ids.insert(entry.instance_id.clone()) {
            return Err(FinalizerError::validation(format!(
                "duplicate plan entry '{}'",
                entry.instance_id
            )));
        }
        if entry.resolved_index as usize != idx {
            return Err(FinalizerError::validation(format!(
                "plan entry '{}' resolved_index {} does not match order {idx}",
                entry.instance_id, entry.resolved_index
            )));
        }
        if (entry.selector_index as usize) >= plan.entries.len()
            || !seen_selector_index.insert(entry.selector_index)
        {
            return Err(FinalizerError::validation(format!(
                "plan entry '{}' has invalid selector_index {}",
                entry.instance_id, entry.selector_index
            )));
        }
    }

    for entry in &plan.entries {
        for dependency in &entry.after {
            if !seen_ids.contains(dependency) {
                return Err(FinalizerError::validation(format!(
                    "plan entry '{}' depends on unselected instance '{}'",
                    entry.instance_id, dependency
                )));
            }
        }
    }

    let mut required_set = BTreeSet::new();
    for instance_id in &plan.required {
        validate_instance_id(instance_id)?;
        if !seen_ids.contains(instance_id) {
            return Err(FinalizerError::validation(format!(
                "required instance '{instance_id}' is not selected"
            )));
        }
        if !required_set.insert(instance_id.as_str()) {
            return Err(FinalizerError::validation(format!(
                "required repeats instance '{instance_id}'"
            )));
        }
    }

    for selector in &plan.selectors {
        match selector {
            FinalizerSelectorOpWire::Add { instance_id }
            | FinalizerSelectorOpWire::Remove { instance_id } => {
                validate_instance_id(instance_id)?;
            }
            FinalizerSelectorOpWire::Clear => {}
        }
    }

    let actual = finalizer_plan_digest(plan)?;
    if actual != plan.plan_digest {
        return Err(FinalizerError::validation(
            "plan_digest does not match the resolved plan",
        ));
    }
    Ok(actual)
}

pub fn authenticate_finalizer_plan(
    plan: &FinalizerPlanWire,
    expected_digest: &str,
) -> Result<String, FinalizerError> {
    let actual = validate_finalizer_plan(plan)?;
    validate_digest(expected_digest, "expected plan digest")?;
    if expected_digest != actual {
        return Err(FinalizerError::validation(
            "expected plan digest does not match the authenticated plan",
        ));
    }
    Ok(actual)
}

pub(crate) fn validate_schema(
    actual: u64,
    what: &str,
) -> Result<(), FinalizerError> {
    if actual == FINALIZER_WIRE_SCHEMA_VERSION {
        Ok(())
    } else {
        Err(FinalizerError::validation(format!(
            "unsupported finalizer {what} schema_version {actual}; expected {FINALIZER_WIRE_SCHEMA_VERSION}"
        )))
    }
}

pub(crate) fn validate_instance_id(value: &str) -> Result<(), FinalizerError> {
    if !value.is_empty()
        && value.len() <= FINALIZER_INSTANCE_ID_MAX_LEN
        && is_lower_slug(value)
    {
        return Ok(());
    }
    Err(FinalizerError::validation(format!(
        "finalizer instance id must be a lowercase slug at most {FINALIZER_INSTANCE_ID_MAX_LEN} bytes: {value:?}"
    )))
}

pub(crate) fn validate_provider_ref(value: &str) -> Result<(), FinalizerError> {
    if value.len() > FINALIZER_PROVIDER_REF_MAX_LEN {
        return Err(FinalizerError::validation(format!(
            "provider_ref is longer than {FINALIZER_PROVIDER_REF_MAX_LEN} bytes"
        )));
    }
    let Some((distribution, provider)) = value.split_once('@') else {
        return Err(FinalizerError::validation(format!(
            "provider_ref must be distribution@provider: {value:?}"
        )));
    };
    if distribution.contains('@') || provider.contains('@') {
        return Err(FinalizerError::validation(format!(
            "provider_ref must contain exactly one '@': {value:?}"
        )));
    }
    if is_provider_segment(distribution) && is_provider_segment(provider) {
        Ok(())
    } else {
        Err(FinalizerError::validation(format!(
            "provider_ref segments must be lowercase slugs: {value:?}"
        )))
    }
}

pub(crate) fn validate_digest(
    value: &str,
    field: &str,
) -> Result<(), FinalizerError> {
    if value.len() == 64
        && value
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
    {
        Ok(())
    } else {
        Err(FinalizerError::validation(format!(
            "{field} must be a 64-character lowercase sha256 hex digest"
        )))
    }
}

pub(crate) fn validate_optional_digest(
    value: Option<&str>,
    field: &str,
) -> Result<(), FinalizerError> {
    if let Some(value) = value {
        validate_digest(value, field)?;
    }
    Ok(())
}

pub(crate) fn validate_list_len(
    len: usize,
    field: &str,
) -> Result<(), FinalizerError> {
    if len <= FINALIZER_LIST_MAX_LEN {
        Ok(())
    } else {
        Err(FinalizerError::validation(format!(
            "{field} has {len} entries; maximum is {FINALIZER_LIST_MAX_LEN}"
        )))
    }
}

pub(crate) fn validate_required_text(
    value: &str,
    field: &str,
) -> Result<(), FinalizerError> {
    let chars = value.chars().count();
    if !value.trim().is_empty()
        && chars <= super::wire::FINALIZER_TEXT_MAX_CHARS
    {
        Ok(())
    } else {
        Err(FinalizerError::validation(format!(
            "{field} must be nonblank and at most {} characters",
            super::wire::FINALIZER_TEXT_MAX_CHARS
        )))
    }
}

pub(crate) fn validate_optional_bounded_text(
    value: Option<&str>,
    field: &str,
    max_len: usize,
) -> Result<(), FinalizerError> {
    if let Some(value) = value {
        if value.trim().is_empty() || value.len() > max_len {
            return Err(FinalizerError::validation(format!(
                "{field} must be nonblank and at most {max_len} bytes"
            )));
        }
    }
    Ok(())
}

fn validate_known_instance(
    instance_id: &str,
    instances: &BTreeMap<String, FinalizerInstanceSpecWire>,
    field: &str,
) -> Result<(), FinalizerError> {
    validate_instance_id(instance_id)?;
    if instances.contains_key(instance_id) {
        Ok(())
    } else {
        Err(FinalizerError::validation(format!(
            "{field} references unknown instance '{instance_id}'"
        )))
    }
}

fn push_unique(selected: &mut Vec<String>, instance_id: &str) {
    if selected.iter().all(|value| value != instance_id) {
        selected.push(instance_id.to_string());
    }
}

fn topological_order(
    selected: &[String],
    instances: &BTreeMap<String, FinalizerInstanceSpecWire>,
) -> Result<Vec<String>, FinalizerError> {
    let selected_set: BTreeSet<String> = selected.iter().cloned().collect();
    let mut resolved = BTreeSet::new();
    let mut ordered = Vec::new();

    while ordered.len() < selected.len() {
        let ready = selected.iter().find(|instance_id| {
            if resolved.contains(*instance_id) {
                return false;
            }
            let spec = instances.get(*instance_id).expect("selected instance");
            spec.after
                .iter()
                .filter(|dependency| selected_set.contains(*dependency))
                .all(|dependency| resolved.contains(dependency))
        });
        if let Some(instance_id) = ready {
            resolved.insert(instance_id.clone());
            ordered.push(instance_id.clone());
        } else {
            let unresolved = selected
                .iter()
                .filter(|instance_id| !resolved.contains(*instance_id))
                .cloned()
                .collect::<Vec<_>>()
                .join(", ");
            return Err(FinalizerError::validation(format!(
                "finalizer dependency cycle among selected instances: {unresolved}"
            )));
        }
    }
    Ok(ordered)
}

fn is_lower_slug(value: &str) -> bool {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !first.is_ascii_lowercase() {
        return false;
    }
    let mut previous_separator = false;
    for character in chars {
        if character == '-' || character == '_' {
            if previous_separator {
                return false;
            }
            previous_separator = true;
        } else if character.is_ascii_lowercase() || character.is_ascii_digit() {
            previous_separator = false;
        } else {
            return false;
        }
    }
    !previous_separator
}

fn is_provider_segment(value: &str) -> bool {
    !value.is_empty()
        && value.bytes().all(|byte| {
            matches!(
                byte,
                b'a'..=b'z' | b'0'..=b'9' | b'_' | b'-' | b'.'
            )
        })
        && value
            .bytes()
            .next()
            .is_some_and(|byte| byte.is_ascii_lowercase() || byte == b'_')
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::finalizer::wire::{
        FinalizerInstancePolicyWire, FinalizerRefusalPolicyWire,
    };

    fn instance(id: &str, after: &[&str]) -> FinalizerInstanceSpecWire {
        FinalizerInstanceSpecWire {
            schema_version: FINALIZER_WIRE_SCHEMA_VERSION,
            instance_id: id.to_string(),
            provider_ref: format!("builtin@{id}"),
            after: after.iter().map(|value| (*value).to_string()).collect(),
            policy: FinalizerInstancePolicyWire {
                max_attempts: 2,
                refusal: FinalizerRefusalPolicyWire::Fail,
            },
            config_digest: None,
            provenance_id: Some("default_config".to_string()),
        }
    }

    fn input(
        selectors: Vec<FinalizerSelectorOpWire>,
    ) -> FinalizerPlanInputWire {
        FinalizerPlanInputWire {
            schema_version: FINALIZER_WIRE_SCHEMA_VERSION,
            instances: vec![
                instance("commit", &["lint"]),
                instance("lint", &[]),
                instance("audit", &[]),
            ],
            defaults: vec!["commit".to_string()],
            required: Vec::new(),
            selectors,
        }
    }

    #[test]
    fn selector_replay_handles_add_remove_and_clear() {
        let plan = resolve_finalizer_plan(&input(vec![
            FinalizerSelectorOpWire::Add {
                instance_id: "lint".to_string(),
            },
            FinalizerSelectorOpWire::Remove {
                instance_id: "commit".to_string(),
            },
            FinalizerSelectorOpWire::Clear,
            FinalizerSelectorOpWire::Add {
                instance_id: "audit".to_string(),
            },
        ]))
        .unwrap();

        assert_eq!(
            plan.entries
                .iter()
                .map(|entry| entry.instance_id.as_str())
                .collect::<Vec<_>>(),
            ["audit"]
        );
        assert_eq!(plan.plan_digest, finalizer_plan_digest(&plan).unwrap());
    }

    #[test]
    fn stable_topological_order_preserves_selector_order_among_ready_nodes() {
        let plan = resolve_finalizer_plan(&input(vec![
            FinalizerSelectorOpWire::Add {
                instance_id: "lint".to_string(),
            },
            FinalizerSelectorOpWire::Add {
                instance_id: "audit".to_string(),
            },
        ]))
        .unwrap();

        assert_eq!(
            plan.entries
                .iter()
                .map(|entry| entry.instance_id.as_str())
                .collect::<Vec<_>>(),
            ["lint", "commit", "audit"]
        );
    }

    #[test]
    fn required_instances_are_selected_and_cannot_be_removed() {
        let mut request = input(Vec::new());
        request.defaults.clear();
        request.required = vec!["commit".to_string()];
        let plan = resolve_finalizer_plan(&request).unwrap();
        assert_eq!(
            plan.entries
                .iter()
                .map(|entry| entry.instance_id.as_str())
                .collect::<Vec<_>>(),
            ["lint", "commit"]
        );

        request.selectors = vec![FinalizerSelectorOpWire::Remove {
            instance_id: "commit".to_string(),
        }];
        assert!(resolve_finalizer_plan(&request)
            .unwrap_err()
            .to_string()
            .contains("required instance"));
    }

    #[test]
    fn cycles_and_missing_dependencies_are_diagnostics() {
        let mut request = FinalizerPlanInputWire {
            schema_version: FINALIZER_WIRE_SCHEMA_VERSION,
            instances: vec![
                instance("one", &["two"]),
                instance("two", &["one"]),
                instance("orphan", &["missing"]),
            ],
            defaults: vec!["one".to_string(), "two".to_string()],
            required: Vec::new(),
            selectors: Vec::new(),
        };
        assert!(resolve_finalizer_plan(&request)
            .unwrap_err()
            .to_string()
            .contains("unknown instance 'missing'"));

        request.instances.pop();
        assert!(resolve_finalizer_plan(&request)
            .unwrap_err()
            .to_string()
            .contains("dependency cycle"));
    }

    #[test]
    fn validates_slug_and_size_limits() {
        let mut spec = instance("commit", &[]);
        spec.instance_id = "Commit".to_string();
        assert!(validate_finalizer_instance_spec(&spec)
            .unwrap_err()
            .to_string()
            .contains("lowercase slug"));

        spec.instance_id = "commit".to_string();
        spec.after = (0..=FINALIZER_LIST_MAX_LEN)
            .map(|idx| format!("dep{idx}"))
            .collect();
        assert!(validate_finalizer_instance_spec(&spec)
            .unwrap_err()
            .to_string()
            .contains("maximum"));
    }

    #[test]
    fn validate_finalizer_plan_accepts_resolved_plans() {
        let plan = resolve_finalizer_plan(&input(Vec::new())).unwrap();
        assert_eq!(validate_finalizer_plan(&plan).unwrap(), plan.plan_digest);
        assert_eq!(
            authenticate_finalizer_plan(&plan, &plan.plan_digest).unwrap(),
            plan.plan_digest
        );
    }

    #[test]
    fn validate_finalizer_plan_rejects_forged_or_omitted_digest() {
        let mut plan = resolve_finalizer_plan(&input(Vec::new())).unwrap();
        plan.plan_digest = "0".repeat(64);
        assert!(validate_finalizer_plan(&plan)
            .unwrap_err()
            .to_string()
            .contains("plan_digest does not match"));

        plan.plan_digest.clear();
        assert!(validate_finalizer_plan(&plan)
            .unwrap_err()
            .to_string()
            .contains("plan_digest"));
    }

    #[test]
    fn authenticate_finalizer_plan_rejects_independent_expected_digest() {
        let plan = resolve_finalizer_plan(&input(Vec::new())).unwrap();
        let other = "ab".repeat(32);
        assert!(authenticate_finalizer_plan(&plan, &other)
            .unwrap_err()
            .to_string()
            .contains("expected plan digest"));
    }

    #[test]
    fn validate_finalizer_plan_rejects_mutated_entries_without_new_digest() {
        let mut forged = resolve_finalizer_plan(&input(Vec::new())).unwrap();
        forged.entries[0].provider_ref = "builtin@command".to_string();
        assert!(validate_finalizer_plan(&forged)
            .unwrap_err()
            .to_string()
            .contains("plan_digest does not match"));

        let mut missing_required = resolve_finalizer_plan(&{
            let mut request = input(Vec::new());
            request.required = vec!["commit".to_string()];
            request
        })
        .unwrap();
        missing_required.required.clear();
        assert!(validate_finalizer_plan(&missing_required)
            .unwrap_err()
            .to_string()
            .contains("plan_digest does not match"));
    }

    #[test]
    fn validate_finalizer_plan_rejects_duplicate_or_shifted_indices() {
        let mut plan = resolve_finalizer_plan(&input(Vec::new())).unwrap();
        plan.entries[0].resolved_index = 3;
        assert!(validate_finalizer_plan(&plan)
            .unwrap_err()
            .to_string()
            .contains("resolved_index"));
    }
}
