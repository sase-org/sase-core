//! Store-neutral bead target routing policy.
//!
//! Hosts still own project discovery and store I/O.  This module receives
//! already-collected store descriptors and issue ID snapshots, then applies the
//! shared policy for local-first lookup, full-ID fallback, ambiguity, and
//! single-store batch validation.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

pub const BEAD_TARGET_ROUTING_WIRE_SCHEMA_VERSION: u64 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadTargetRoutingRequestWire {
    #[serde(default = "default_schema_version")]
    pub schema_version: u64,
    #[serde(default)]
    pub targets: Vec<String>,
    #[serde(default)]
    pub local_store: Option<BeadTargetStoreDescriptorWire>,
    #[serde(default)]
    pub candidate_stores: Vec<BeadTargetStoreDescriptorWire>,
    #[serde(default)]
    pub project_pinned: bool,
    #[serde(default)]
    pub require_single_store: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadTargetStoreDescriptorWire {
    #[serde(default)]
    pub store_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_label: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub primary_workspace: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub beads_dir: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub issue_prefix: Option<String>,
    #[serde(default)]
    pub project_refs: Vec<String>,
    #[serde(default)]
    pub issue_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unavailable_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadTargetRoutingOutcomeWire {
    pub schema_version: u64,
    pub routes: Vec<BeadTargetRouteWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub batch_error: Option<BeadTargetRouteErrorWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadTargetRouteWire {
    pub requested_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub store: Option<BeadTargetStoreRouteWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<BeadTargetRouteErrorWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadTargetStoreRouteWire {
    pub store_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_label: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub primary_workspace: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub beads_dir: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadTargetRouteErrorWire {
    pub kind: String,
    pub message: String,
    #[serde(default)]
    pub candidates: Vec<BeadTargetStoreRouteWire>,
}

#[derive(Debug, Clone)]
struct StoreSnapshot {
    store_key: String,
    project_key: Option<String>,
    project_label: Option<String>,
    primary_workspace: Option<String>,
    beads_dir: Option<String>,
    issue_prefix: Option<String>,
    project_refs: BTreeSet<String>,
    issue_ids: BTreeSet<String>,
    unavailable_reason: Option<String>,
}

#[derive(Debug, Clone)]
enum StoreResolve {
    Resolved(String),
    Missing,
    Ambiguous(Vec<String>),
    Unavailable,
}

fn default_schema_version() -> u64 {
    BEAD_TARGET_ROUTING_WIRE_SCHEMA_VERSION
}

pub fn route_bead_targets(
    request: &BeadTargetRoutingRequestWire,
) -> BeadTargetRoutingOutcomeWire {
    let local = request
        .local_store
        .as_ref()
        .map(|store| normalize_store(store, "local".to_string()));
    let candidates = normalize_candidate_stores(&request.candidate_stores);
    let routes = request
        .targets
        .iter()
        .map(|target| route_one(target, local.as_ref(), &candidates, request))
        .collect::<Vec<_>>();
    let batch_error = if request.require_single_store {
        single_store_batch_error(&routes)
    } else {
        None
    };
    BeadTargetRoutingOutcomeWire {
        schema_version: BEAD_TARGET_ROUTING_WIRE_SCHEMA_VERSION,
        routes,
        batch_error,
    }
}

fn route_one(
    target: &str,
    local: Option<&StoreSnapshot>,
    candidates: &[StoreSnapshot],
    request: &BeadTargetRoutingRequestWire,
) -> BeadTargetRouteWire {
    if request.project_pinned {
        return match local {
            Some(store) => route_from_selected_store(target, store),
            None => error_route(
                target,
                "unavailable",
                "no pinned bead store is available".to_string(),
                Vec::new(),
            ),
        };
    }

    if let Some(store) = local {
        match resolve_in_store(store, target) {
            StoreResolve::Resolved(resolved) => {
                return resolved_route(target, resolved, store)
            }
            StoreResolve::Ambiguous(matches) => {
                return error_route(
                    target,
                    "ambiguous",
                    format!(
                        "ambiguous bead ID shorthand {target:?}: {}",
                        matches.join(", ")
                    ),
                    vec![store.route()],
                )
            }
            StoreResolve::Unavailable if !is_full_bead_id(target) => {
                return error_route(
                    target,
                    "unavailable",
                    unavailable_message(target, &[store.route()]),
                    vec![store.route()],
                )
            }
            StoreResolve::Missing if !is_full_bead_id(target) => {
                return missing_route(target)
            }
            StoreResolve::Missing | StoreResolve::Unavailable => {}
        }
    } else if !is_full_bead_id(target) {
        return error_route(
            target,
            "unavailable",
            "no local bead store is available".to_string(),
            Vec::new(),
        );
    }

    if !is_full_bead_id(target) {
        return missing_route(target);
    }

    route_foreign_full_id(target, candidates)
}

fn route_from_selected_store(
    target: &str,
    store: &StoreSnapshot,
) -> BeadTargetRouteWire {
    match resolve_in_store(store, target) {
        StoreResolve::Resolved(resolved) => {
            resolved_route(target, resolved, store)
        }
        StoreResolve::Ambiguous(matches) => error_route(
            target,
            "ambiguous",
            format!(
                "ambiguous bead ID shorthand {target:?}: {}",
                matches.join(", ")
            ),
            vec![store.route()],
        ),
        StoreResolve::Unavailable => error_route(
            target,
            "unavailable",
            unavailable_message(target, &[store.route()]),
            vec![store.route()],
        ),
        StoreResolve::Missing => missing_route(target),
    }
}

fn route_foreign_full_id(
    target: &str,
    candidates: &[StoreSnapshot],
) -> BeadTargetRouteWire {
    let matches = candidates
        .iter()
        .filter(|store| store.issue_ids.contains(target))
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [store] => resolved_route(target, target.to_string(), store),
        [] => {
            let unavailable = relevant_unavailable_stores(target, candidates);
            if unavailable.is_empty() {
                missing_route(target)
            } else {
                error_route(
                    target,
                    "unavailable",
                    unavailable_message(target, &unavailable),
                    unavailable,
                )
            }
        }
        _ => {
            let candidates: Vec<BeadTargetStoreRouteWire> =
                matches.iter().map(|store| store.route()).collect();
            error_route(
                target,
                "ambiguous",
                ambiguity_message(target, &candidates),
                candidates,
            )
        }
    }
}

fn resolve_in_store(store: &StoreSnapshot, target: &str) -> StoreResolve {
    if store.unavailable_reason.is_some() {
        return StoreResolve::Unavailable;
    }
    if is_full_bead_id(target) {
        return if store.issue_ids.contains(target) {
            StoreResolve::Resolved(target.to_string())
        } else {
            StoreResolve::Missing
        };
    }

    let mut matches = store
        .issue_ids
        .iter()
        .filter(|issue_id| issue_id_suffix(issue_id) == Some(target))
        .cloned()
        .collect::<Vec<_>>();
    matches.sort();
    matches.dedup();
    match matches.as_slice() {
        [resolved] => StoreResolve::Resolved(resolved.clone()),
        [] => StoreResolve::Missing,
        _ => StoreResolve::Ambiguous(matches),
    }
}

fn normalize_candidate_stores(
    stores: &[BeadTargetStoreDescriptorWire],
) -> Vec<StoreSnapshot> {
    let mut by_key: BTreeMap<String, StoreSnapshot> = BTreeMap::new();
    let mut order: Vec<String> = Vec::new();
    for (index, store) in stores.iter().enumerate() {
        let normalized = normalize_store(store, format!("candidate:{index}"));
        let key = normalized.store_key.clone();
        if let Some(existing) = by_key.get_mut(&key) {
            existing.merge(normalized);
        } else {
            order.push(key.clone());
            by_key.insert(key, normalized);
        }
    }
    order
        .into_iter()
        .filter_map(|key| by_key.remove(&key))
        .collect()
}

fn normalize_store(
    store: &BeadTargetStoreDescriptorWire,
    fallback_key: String,
) -> StoreSnapshot {
    let store_key = first_non_empty([
        Some(store.store_key.as_str()),
        store.beads_dir.as_deref(),
        store.project_key.as_deref(),
    ])
    .unwrap_or(fallback_key);
    let issue_ids = store
        .issue_ids
        .iter()
        .filter_map(|issue_id| non_empty_string(issue_id))
        .collect::<BTreeSet<_>>();
    let project_refs = store
        .project_refs
        .iter()
        .filter_map(|project_ref| non_empty_string(project_ref))
        .collect::<BTreeSet<_>>();
    StoreSnapshot {
        store_key,
        project_key: store
            .project_key
            .as_ref()
            .and_then(|s| non_empty_string(s)),
        project_label: store
            .project_label
            .as_ref()
            .and_then(|s| non_empty_string(s)),
        primary_workspace: store
            .primary_workspace
            .as_ref()
            .and_then(|s| non_empty_string(s)),
        beads_dir: store.beads_dir.as_ref().and_then(|s| non_empty_string(s)),
        issue_prefix: store
            .issue_prefix
            .as_ref()
            .and_then(|s| non_empty_string(s)),
        project_refs,
        issue_ids,
        unavailable_reason: store
            .unavailable_reason
            .as_ref()
            .and_then(|s| non_empty_string(s)),
    }
}

fn first_non_empty<const N: usize>(
    values: [Option<&str>; N],
) -> Option<String> {
    values
        .into_iter()
        .find_map(|value| value.and_then(non_empty_str))
}

fn non_empty_str(value: &str) -> Option<String> {
    let trimmed = value.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

fn non_empty_string(value: &str) -> Option<String> {
    non_empty_str(value)
}

fn is_full_bead_id(target: &str) -> bool {
    full_bead_id_prefix(target).is_some()
}

fn full_bead_id_prefix(target: &str) -> Option<&str> {
    if target.is_empty() || target.chars().any(char::is_whitespace) {
        return None;
    }
    let mut parts = target.split('.');
    let top_level = parts.next()?;
    if parts.any(|part| {
        part.is_empty() || !part.chars().all(|c| c.is_ascii_digit())
    }) {
        return None;
    }
    let (prefix, counter) = top_level.rsplit_once('-')?;
    if prefix.is_empty()
        || counter.is_empty()
        || !counter
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
    {
        return None;
    }
    Some(prefix)
}

fn issue_id_suffix(issue_id: &str) -> Option<&str> {
    issue_id.rsplit_once('-').map(|(_, suffix)| suffix)
}

fn relevant_unavailable_stores(
    target: &str,
    candidates: &[StoreSnapshot],
) -> Vec<BeadTargetStoreRouteWire> {
    let Some(prefix) = full_bead_id_prefix(target) else {
        return Vec::new();
    };
    candidates
        .iter()
        .filter(|store| {
            store.unavailable_reason.is_some()
                && (store.issue_prefix.as_deref() == Some(prefix)
                    || store.project_refs.contains(prefix))
        })
        .map(StoreSnapshot::route)
        .collect()
}

fn resolved_route(
    requested_id: &str,
    resolved_id: String,
    store: &StoreSnapshot,
) -> BeadTargetRouteWire {
    BeadTargetRouteWire {
        requested_id: requested_id.to_string(),
        resolved_id: Some(resolved_id),
        store: Some(store.route()),
        error: None,
    }
}

fn missing_route(requested_id: &str) -> BeadTargetRouteWire {
    error_route(
        requested_id,
        "not_found",
        format!("issue not found: {requested_id}"),
        Vec::new(),
    )
}

fn error_route(
    requested_id: &str,
    kind: &str,
    message: String,
    candidates: Vec<BeadTargetStoreRouteWire>,
) -> BeadTargetRouteWire {
    BeadTargetRouteWire {
        requested_id: requested_id.to_string(),
        resolved_id: None,
        store: None,
        error: Some(BeadTargetRouteErrorWire {
            kind: kind.to_string(),
            message,
            candidates,
        }),
    }
}

fn unavailable_message(
    requested_id: &str,
    candidates: &[BeadTargetStoreRouteWire],
) -> String {
    match candidates {
        [candidate] => format!(
            "project '{}' owns '{requested_id}', but its bead store is not materialized on this machine",
            candidate.label()
        ),
        [] => format!("bead store for '{requested_id}' is unavailable"),
        _ => format!(
            "bead stores that may own '{requested_id}' are unavailable: {}",
            candidate_labels(candidates)
        ),
    }
}

fn ambiguity_message(
    requested_id: &str,
    candidates: &[BeadTargetStoreRouteWire],
) -> String {
    format!(
        "ambiguous bead ID '{requested_id}' matched multiple enabled project stores: {}; use -P/--project",
        candidate_labels(candidates)
    )
}

fn candidate_labels(candidates: &[BeadTargetStoreRouteWire]) -> String {
    candidates
        .iter()
        .map(BeadTargetStoreRouteWire::display_label)
        .collect::<Vec<_>>()
        .join(", ")
}

fn single_store_batch_error(
    routes: &[BeadTargetRouteWire],
) -> Option<BeadTargetRouteErrorWire> {
    if routes.iter().any(|route| route.error.is_some()) {
        return None;
    }
    let mut by_store: BTreeMap<String, BeadTargetStoreRouteWire> =
        BTreeMap::new();
    for route in routes {
        let Some(store) = &route.store else {
            continue;
        };
        by_store
            .entry(store.store_key.clone())
            .or_insert_with(|| store.clone());
    }
    if by_store.len() <= 1 {
        return None;
    }
    let candidates = by_store.into_values().collect::<Vec<_>>();
    Some(BeadTargetRouteErrorWire {
        kind: "incompatible_stores".to_string(),
        message: format!(
            "bead targets resolve to multiple stores: {}; split the command by project",
            candidate_labels(&candidates)
        ),
        candidates,
    })
}

impl StoreSnapshot {
    fn merge(&mut self, other: StoreSnapshot) {
        if self.project_key.is_none() {
            self.project_key = other.project_key;
        }
        if self.project_label.is_none() {
            self.project_label = other.project_label;
        }
        if self.primary_workspace.is_none() {
            self.primary_workspace = other.primary_workspace;
        }
        if self.beads_dir.is_none() {
            self.beads_dir = other.beads_dir;
        }
        if self.issue_prefix.is_none() {
            self.issue_prefix = other.issue_prefix;
        }
        self.project_refs.extend(other.project_refs);
        self.issue_ids.extend(other.issue_ids);
        if self.unavailable_reason.is_some()
            && other.unavailable_reason.is_none()
        {
            self.unavailable_reason = None;
        }
    }

    fn route(&self) -> BeadTargetStoreRouteWire {
        BeadTargetStoreRouteWire {
            store_key: self.store_key.clone(),
            project_key: self.project_key.clone(),
            project_label: self.project_label.clone(),
            primary_workspace: self.primary_workspace.clone(),
            beads_dir: self.beads_dir.clone(),
        }
    }
}

impl BeadTargetStoreRouteWire {
    fn label(&self) -> String {
        self.project_label
            .clone()
            .or_else(|| self.project_key.clone())
            .unwrap_or_else(|| self.store_key.clone())
    }

    fn display_label(&self) -> String {
        let label = self.label();
        match &self.project_key {
            Some(project_key) if project_key != &label => {
                format!("{label} ({project_key})")
            }
            _ => label,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store(
        key: &str,
        label: &str,
        issue_ids: &[&str],
    ) -> BeadTargetStoreDescriptorWire {
        BeadTargetStoreDescriptorWire {
            store_key: key.to_string(),
            project_key: Some(key.to_string()),
            project_label: Some(label.to_string()),
            primary_workspace: Some(format!("/work/{key}")),
            beads_dir: Some(format!("/work/{key}/sdd/beads")),
            issue_prefix: Some(label.to_string()),
            project_refs: vec![key.to_string(), label.to_string()],
            issue_ids: issue_ids
                .iter()
                .map(|value| value.to_string())
                .collect(),
            unavailable_reason: None,
        }
    }

    fn unavailable(key: &str, label: &str) -> BeadTargetStoreDescriptorWire {
        BeadTargetStoreDescriptorWire {
            unavailable_reason: Some("not materialized".to_string()),
            ..store(key, label, &[])
        }
    }

    fn route(
        targets: &[&str],
        local_store: Option<BeadTargetStoreDescriptorWire>,
        candidate_stores: Vec<BeadTargetStoreDescriptorWire>,
    ) -> BeadTargetRoutingOutcomeWire {
        route_bead_targets(&BeadTargetRoutingRequestWire {
            schema_version: BEAD_TARGET_ROUTING_WIRE_SCHEMA_VERSION,
            targets: targets.iter().map(|value| value.to_string()).collect(),
            local_store,
            candidate_stores,
            project_pinned: false,
            require_single_store: false,
        })
    }

    #[test]
    fn local_full_id_hit_wins_without_candidates() {
        let outcome = route(
            &["bob-cli-1"],
            Some(store("local", "bob-cli", &["bob-cli-1"])),
            Vec::new(),
        );

        let first = &outcome.routes[0];
        assert_eq!(first.resolved_id.as_deref(), Some("bob-cli-1"));
        assert_eq!(
            first.store.as_ref().map(|store| store.store_key.as_str()),
            Some("local")
        );
    }

    #[test]
    fn full_id_fallback_checks_exact_membership() {
        let outcome = route(
            &["sase-xe.16.11.7.15.7"],
            Some(store("local", "sase", &[])),
            vec![store(
                "renamed",
                "renamed-project",
                &["sase-xe.16.11.7.15.7"],
            )],
        );

        let first = &outcome.routes[0];
        assert_eq!(first.resolved_id.as_deref(), Some("sase-xe.16.11.7.15.7"));
        assert_eq!(
            first.store.as_ref().map(|store| store.store_key.as_str()),
            Some("renamed")
        );
    }

    #[test]
    fn shorthand_never_falls_back_to_foreign_store() {
        let outcome = route(
            &["1"],
            Some(store("local", "sase", &[])),
            vec![store("foreign", "bob-cli", &["bob-cli-1"])],
        );

        let first = &outcome.routes[0];
        assert!(first.resolved_id.is_none());
        assert_eq!(
            first.error.as_ref().map(|error| error.kind.as_str()),
            Some("not_found")
        );
    }

    #[test]
    fn unavailable_relevant_store_is_actionable() {
        let outcome = route(
            &["bob-cli-1"],
            Some(store("local", "sase", &[])),
            vec![unavailable("bob", "bob-cli")],
        );

        let error = outcome.routes[0].error.as_ref().unwrap();
        assert_eq!(error.kind, "unavailable");
        assert!(error.message.contains("bob-cli"));
        assert_eq!(error.candidates.len(), 1);
    }

    #[test]
    fn exact_duplicate_ids_are_ambiguous_across_distinct_stores() {
        let outcome = route(
            &["bob-cli-1"],
            Some(store("local", "sase", &[])),
            vec![
                store("first", "first", &["bob-cli-1"]),
                store("second", "second", &["bob-cli-1"]),
            ],
        );

        let error = outcome.routes[0].error.as_ref().unwrap();
        assert_eq!(error.kind, "ambiguous");
        assert_eq!(error.candidates.len(), 2);
    }

    #[test]
    fn aliases_to_same_store_are_deduplicated() {
        let first = store("same", "first", &["bob-cli-1"]);
        let mut alias = store("same", "alias", &["bob-cli-1"]);
        alias.project_key = Some("alias".to_string());
        let outcome = route(
            &["bob-cli-1"],
            Some(store("local", "sase", &[])),
            vec![first, alias],
        );

        assert_eq!(outcome.routes[0].resolved_id.as_deref(), Some("bob-cli-1"));
        assert!(outcome.routes[0].error.is_none());
    }

    #[test]
    fn single_store_batches_reject_mixed_owners() {
        let mut request = BeadTargetRoutingRequestWire {
            schema_version: BEAD_TARGET_ROUTING_WIRE_SCHEMA_VERSION,
            targets: vec!["bob-cli-1".to_string(), "gold-1".to_string()],
            local_store: Some(store("local", "sase", &[])),
            candidate_stores: vec![
                store("bob", "bob-cli", &["bob-cli-1"]),
                store("gold", "gold", &["gold-1"]),
            ],
            project_pinned: false,
            require_single_store: true,
        };

        let outcome = route_bead_targets(&request);
        assert_eq!(
            outcome
                .batch_error
                .as_ref()
                .map(|error| error.kind.as_str()),
            Some("incompatible_stores")
        );

        request.targets = vec!["bob-cli-1".to_string()];
        assert!(route_bead_targets(&request).batch_error.is_none());
    }
}
