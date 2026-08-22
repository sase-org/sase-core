//! Pure agent cleanup planner.
//!
//! This mirrors the deterministic partitioning used by the Python Agents
//! tab: classify kill side effects, select by scope, cascade workflow-parent
//! decisions to child rows, and return skipped reasons. No processes are
//! signalled and no files are read or written here.

use std::collections::{BTreeMap, BTreeSet};

use super::wire::{
    AgentCleanupArtifactDeleteIntentWire, AgentCleanupBundleSaveIntentWire,
    AgentCleanupCountsWire, AgentCleanupDismissItemWire,
    AgentCleanupIdentityWire, AgentCleanupKillItemWire,
    AgentCleanupMonitorStopIntentWire,
    AgentCleanupNotificationDismissIntentWire, AgentCleanupPlanWire,
    AgentCleanupRequestWire, AgentCleanupSideEffectsWire,
    AgentCleanupSkippedItemWire, AgentCleanupTargetWire,
    AgentCleanupWorkspaceReleaseIntentWire, AGENT_CLEANUP_WIRE_SCHEMA_VERSION,
    CLEANUP_MODE_DISMISS_COMPLETED, CLEANUP_MODE_KILL_AND_DISMISS,
    CLEANUP_MODE_PREVIEW_ONLY, CLEANUP_SCOPE_ALL_PANELS, CLEANUP_SCOPE_CLAN,
    CLEANUP_SCOPE_CUSTOM_SELECTION, CLEANUP_SCOPE_EXPLICIT_IDENTITIES,
    CLEANUP_SCOPE_FOCUSED_GROUP, CLEANUP_SCOPE_FOCUSED_PANEL,
    CLEANUP_SCOPE_TRIBE, CONFIRMATION_SEVERITY_DESTRUCTIVE,
    CONFIRMATION_SEVERITY_DISMISS, CONFIRMATION_SEVERITY_NONE, KILL_KIND_CRS,
    KILL_KIND_HOOK, KILL_KIND_MENTOR, KILL_KIND_MONITOR, KILL_KIND_RUNNING,
    KILL_KIND_WORKFLOW, SKIPPED_DUPLICATE, SKIPPED_NOT_DISMISSABLE,
    SKIPPED_NOT_IN_SCOPE, SKIPPED_NOT_KILLABLE, SKIPPED_UNKNOWN_KILL_KIND,
    SKIPPED_WORKFLOW_CHILD_CASCADE_ONLY,
};

const DISMISSABLE_STATUSES: &[&str] = &[
    "DONE",
    "FAILED",
    "PLAN COMMITTED",
    "PLAN DONE",
    "TALE DONE",
    "PLAN REJECTED",
    "EPIC CREATED",
    // Repeat-chain STOP: a terminal, non-error slot skipped by a
    // predecessor's STOP. Dismissable like other finished rows; mirrors the
    // Python TUI ``DISMISSABLE_STATUSES``.
    "STOPPED",
];

fn is_dismissable_status(status: &str) -> bool {
    DISMISSABLE_STATUSES.contains(&status)
}

/// True for any child row: workflow steps, sequential family members, and
/// monitor proc shells. The wire's `is_workflow_child` flag is a historical
/// alias for this broader predicate.
fn is_child_row(target: &AgentCleanupTargetWire) -> bool {
    target.is_workflow_child
        || target.parent_workflow.is_some()
        || target.parent_timestamp.is_some()
}

/// Mirrors `AgentChildLinkage::WORKFLOW_STEP`: only a workflow step child is
/// covered by its parent's cascade. Family members and monitor proc shells
/// carry a `parent_timestamp` but are independent agent rows with their own
/// PID, artifacts, and dismissal record.
fn is_workflow_step_child(target: &AgentCleanupTargetWire) -> bool {
    target.parent_workflow.is_some()
}

fn effective_tribe(
    target: &AgentCleanupTargetWire,
    parent_tribes: &BTreeMap<String, Option<String>>,
) -> Option<String> {
    if is_child_row(target) {
        if let Some(parent_ts) = &target.parent_timestamp {
            if let Some(tribe) = parent_tribes.get(parent_ts) {
                return tribe.clone();
            }
        }
    }
    target.tribe.clone()
}

fn selected_by_scope(
    target: &AgentCleanupTargetWire,
    request: &AgentCleanupRequestWire,
    selected_ids: &BTreeSet<AgentCleanupIdentityWire>,
    parent_tribes: &BTreeMap<String, Option<String>>,
) -> bool {
    match request.scope.as_str() {
        CLEANUP_SCOPE_ALL_PANELS => true,
        CLEANUP_SCOPE_FOCUSED_PANEL => {
            effective_tribe(target, parent_tribes)
                == request.focused_panel_tribe
        }
        CLEANUP_SCOPE_TRIBE => {
            effective_tribe(target, parent_tribes) == request.tribe
        }
        CLEANUP_SCOPE_CLAN => {
            request.clan_name.is_some()
                && target.agent_clan == request.clan_name
                && match request.clan_generation.as_deref() {
                    Some(generation) => {
                        target.agent_clan_generation.as_deref()
                            == Some(generation)
                    }
                    None => true,
                }
        }
        CLEANUP_SCOPE_EXPLICIT_IDENTITIES
        | CLEANUP_SCOPE_FOCUSED_GROUP
        | CLEANUP_SCOPE_CUSTOM_SELECTION => {
            selected_ids.contains(&target.identity)
        }
        _ => false,
    }
}

fn scope_allows_direct_child_targets(scope: &str) -> bool {
    matches!(
        scope,
        CLEANUP_SCOPE_EXPLICIT_IDENTITIES | CLEANUP_SCOPE_CUSTOM_SELECTION
    )
}

fn parent_matches_child(
    parent: &AgentCleanupTargetWire,
    child: &AgentCleanupTargetWire,
) -> bool {
    if is_child_row(parent) {
        return false;
    }
    if parent.raw_suffix.as_deref() != child.parent_timestamp.as_deref() {
        return false;
    }
    match child.parent_workflow.as_deref() {
        Some(parent_workflow) => {
            parent.workflow.as_deref() == Some(parent_workflow)
        }
        None => true,
    }
}

fn parent_selected_for_child(
    child: &AgentCleanupTargetWire,
    targets: &[AgentCleanupTargetWire],
    request: &AgentCleanupRequestWire,
    selected_ids: &BTreeSet<AgentCleanupIdentityWire>,
    parent_tribes: &BTreeMap<String, Option<String>>,
) -> bool {
    if child.parent_timestamp.is_none() {
        return false;
    }
    targets.iter().any(|candidate| {
        parent_matches_child(candidate, child)
            && selected_by_scope(
                candidate,
                request,
                selected_ids,
                parent_tribes,
            )
    })
}

fn is_direct_child_target(
    target: &AgentCleanupTargetWire,
    targets: &[AgentCleanupTargetWire],
    request: &AgentCleanupRequestWire,
    selected_ids: &BTreeSet<AgentCleanupIdentityWire>,
    parent_tribes: &BTreeMap<String, Option<String>>,
) -> bool {
    is_workflow_step_child(target)
        && scope_allows_direct_child_targets(&request.scope)
        && selected_ids.contains(&target.identity)
        && !parent_selected_for_child(
            target,
            targets,
            request,
            selected_ids,
            parent_tribes,
        )
}

fn classify_kill_kind(target: &AgentCleanupTargetWire) -> Option<&'static str> {
    if target.is_live_monitor {
        return Some(KILL_KIND_MONITOR);
    }
    let workflow = target.workflow.as_deref().unwrap_or("");
    if target.agent_type == "workflow" {
        return Some(KILL_KIND_WORKFLOW);
    }
    if workflow.starts_with("axe(fix-hook)")
        || workflow == "fix-hook"
        || workflow == "summarize-hook"
    {
        return Some(KILL_KIND_HOOK);
    }
    if workflow.starts_with("axe(mentor)")
        || workflow.starts_with("mentor(")
        || workflow == "mentor"
    {
        return Some(KILL_KIND_MENTOR);
    }
    if workflow.starts_with("axe(crs)") || workflow == "crs" {
        return Some(KILL_KIND_CRS);
    }
    if target.agent_type == "run" {
        return Some(KILL_KIND_RUNNING);
    }
    None
}

fn parallel_members_by_parent(
    targets: &[AgentCleanupTargetWire],
) -> BTreeMap<String, Vec<&AgentCleanupTargetWire>> {
    let mut members: BTreeMap<String, Vec<&AgentCleanupTargetWire>> =
        BTreeMap::new();
    for target in targets {
        if !target.agent_family_parallel || target.parent_workflow.is_some() {
            continue;
        }
        let Some(parent_timestamp) = &target.parent_timestamp else {
            continue;
        };
        members
            .entry(parent_timestamp.clone())
            .or_default()
            .push(target);
    }
    members
}

fn parallel_family_members<'a>(
    root: &AgentCleanupTargetWire,
    members_by_parent: &'a BTreeMap<String, Vec<&'a AgentCleanupTargetWire>>,
) -> &'a [&'a AgentCleanupTargetWire] {
    if !root.agent_family_parallel || is_child_row(root) {
        return &[];
    }
    let Some(raw_suffix) = &root.raw_suffix else {
        return &[];
    };
    members_by_parent
        .get(raw_suffix)
        .map(Vec::as_slice)
        .unwrap_or(&[])
}

fn target_is_dismissable(
    target: &AgentCleanupTargetWire,
    request: &AgentCleanupRequestWire,
) -> bool {
    is_dismissable_status(&target.status)
        || (request.include_pidless_as_dismissable && target.pid.is_none())
}

fn workflow_children_by_parent(
    targets: &[AgentCleanupTargetWire],
) -> BTreeMap<(String, Option<String>), Vec<&AgentCleanupTargetWire>> {
    let mut children: BTreeMap<
        (String, Option<String>),
        Vec<&AgentCleanupTargetWire>,
    > = BTreeMap::new();
    for target in targets {
        if !is_child_row(target) {
            continue;
        }
        let Some(parent_ts) = &target.parent_timestamp else {
            continue;
        };
        children
            .entry((parent_ts.clone(), target.parent_workflow.clone()))
            .or_default()
            .push(target);
    }
    children
}

fn children_by_parent_timestamp(
    targets: &[AgentCleanupTargetWire],
) -> BTreeMap<String, Vec<&AgentCleanupTargetWire>> {
    let mut children: BTreeMap<String, Vec<&AgentCleanupTargetWire>> =
        BTreeMap::new();
    for target in targets {
        let Some(parent_ts) = &target.parent_timestamp else {
            continue;
        };
        children.entry(parent_ts.clone()).or_default().push(target);
    }
    children
}

fn collect_live_monitor_descendants(
    owner: &AgentCleanupTargetWire,
    children_by_parent_ts: &BTreeMap<String, Vec<&AgentCleanupTargetWire>>,
    owned: &mut BTreeSet<AgentCleanupIdentityWire>,
) {
    let Some(raw_suffix) = &owner.raw_suffix else {
        return;
    };
    let mut stack = vec![raw_suffix.clone()];
    let mut seen = BTreeSet::new();
    while let Some(ts) = stack.pop() {
        if !seen.insert(ts.clone()) {
            continue;
        }
        let Some(children) = children_by_parent_ts.get(&ts) else {
            continue;
        };
        for child in children {
            if child.is_live_monitor {
                owned.insert(child.identity.clone());
            }
            if let Some(child_ts) = &child.raw_suffix {
                stack.push(child_ts.clone());
            }
        }
    }
}

fn owned_live_monitor_identities(
    targets: &[AgentCleanupTargetWire],
    request: &AgentCleanupRequestWire,
    selected_ids: &BTreeSet<AgentCleanupIdentityWire>,
    parent_tribes: &BTreeMap<String, Option<String>>,
    children_by_parent_ts: &BTreeMap<String, Vec<&AgentCleanupTargetWire>>,
) -> BTreeSet<AgentCleanupIdentityWire> {
    let mut owned = BTreeSet::new();
    if request.mode != CLEANUP_MODE_KILL_AND_DISMISS {
        return owned;
    }
    for target in targets {
        if !selected_by_scope(target, request, selected_ids, parent_tribes) {
            continue;
        }
        if is_workflow_step_child(target)
            && !is_direct_child_target(
                target,
                targets,
                request,
                selected_ids,
                parent_tribes,
            )
        {
            continue;
        }
        collect_live_monitor_descendants(
            target,
            children_by_parent_ts,
            &mut owned,
        );
    }
    owned
}

fn monitor_id_for_kill(target: &AgentCleanupTargetWire) -> Option<String> {
    target
        .monitor_id
        .as_ref()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn push_monitor_kill_item(
    kill_items: &mut Vec<AgentCleanupKillItemWire>,
    target: &AgentCleanupTargetWire,
) -> bool {
    let Some(monitor_id) = monitor_id_for_kill(target) else {
        return false;
    };
    kill_items.push(AgentCleanupKillItemWire {
        identity: target.identity.clone(),
        kind: KILL_KIND_MONITOR.to_string(),
        pid: None,
        display_name: target.display_name.clone(),
        monitor_id: Some(monitor_id),
    });
    true
}

fn sort_monitor_kills_first(kill_items: &mut [AgentCleanupKillItemWire]) {
    kill_items.sort_by(|left, right| {
        (left.kind != KILL_KIND_MONITOR).cmp(&(right.kind != KILL_KIND_MONITOR))
    });
}

fn parent_tribes_by_suffix(
    targets: &[AgentCleanupTargetWire],
) -> BTreeMap<String, Option<String>> {
    let mut tribes = BTreeMap::new();
    for target in targets {
        if is_child_row(target) {
            continue;
        }
        if let Some(raw_suffix) = &target.raw_suffix {
            tribes.insert(raw_suffix.clone(), target.tribe.clone());
        }
    }
    tribes
}

fn add_skip(
    skipped: &mut Vec<AgentCleanupSkippedItemWire>,
    target: &AgentCleanupTargetWire,
    reason: &str,
    detail: Option<String>,
) {
    skipped.push(AgentCleanupSkippedItemWire {
        identity: target.identity.clone(),
        reason: reason.to_string(),
        detail,
    });
}

fn push_summary_line(lines: &mut Vec<String>, count: u64, noun: &str) {
    if count == 0 {
        return;
    }
    let suffix = if count == 1 { "" } else { "s" };
    lines.push(format!("{count} {noun}{suffix}"));
}

fn target_by_identity(
    targets: &[AgentCleanupTargetWire],
) -> BTreeMap<AgentCleanupIdentityWire, &AgentCleanupTargetWire> {
    let mut by_id = BTreeMap::new();
    for target in targets {
        by_id.entry(target.identity.clone()).or_insert(target);
    }
    by_id
}

fn add_index_identity(
    side_effects: &mut AgentCleanupSideEffectsWire,
    seen: &mut BTreeSet<AgentCleanupIdentityWire>,
    target: &AgentCleanupTargetWire,
) {
    if seen.insert(target.identity.clone()) {
        side_effects
            .dismissed_index_additions
            .push(target.identity.clone());
    }
}

fn add_bundle_candidate(
    side_effects: &mut AgentCleanupSideEffectsWire,
    seen: &mut BTreeSet<AgentCleanupIdentityWire>,
    target: &AgentCleanupTargetWire,
) {
    if target.from_patch {
        return;
    }
    if seen.insert(target.identity.clone()) {
        side_effects.bundle_save_candidates.push(
            AgentCleanupBundleSaveIntentWire {
                identity: target.identity.clone(),
            },
        );
    }
}

fn add_artifact_delete(
    side_effects: &mut AgentCleanupSideEffectsWire,
    seen: &mut BTreeSet<(AgentCleanupIdentityWire, String)>,
    target: &AgentCleanupTargetWire,
) {
    let Some(path) = &target.artifacts_dir else {
        return;
    };
    let key = (target.identity.clone(), path.clone());
    if seen.insert(key) {
        side_effects.artifact_delete_paths.push(
            AgentCleanupArtifactDeleteIntentWire {
                identity: target.identity.clone(),
                artifacts_dir: path.clone(),
            },
        );
    }
}

fn add_notification_candidate(
    side_effects: &mut AgentCleanupSideEffectsWire,
    seen: &mut BTreeSet<AgentCleanupIdentityWire>,
    target: &AgentCleanupTargetWire,
) {
    if seen.insert(target.identity.clone()) {
        side_effects.notification_dismiss_candidates.push(
            AgentCleanupNotificationDismissIntentWire {
                identity: target.identity.clone(),
                cl_name: target.identity.cl_name.clone(),
                raw_suffix: target.raw_suffix.clone(),
            },
        );
    }
}

fn add_workspace_release(
    side_effects: &mut AgentCleanupSideEffectsWire,
    seen: &mut BTreeSet<AgentCleanupIdentityWire>,
    target: &AgentCleanupTargetWire,
    kind: &str,
) {
    if !seen.insert(target.identity.clone()) {
        return;
    }
    if kind == KILL_KIND_RUNNING {
        if target.workspace.is_none() {
            return;
        }
        side_effects.workspace_release_requests.push(
            AgentCleanupWorkspaceReleaseIntentWire {
                identity: target.identity.clone(),
                project_file: target.project_file.clone().unwrap_or_default(),
                workspace: target.workspace,
                workflow: target.workflow.clone(),
                cl_name: Some(target.identity.cl_name.clone()),
                lookup_workflow: false,
                lookup_timestamp: false,
                artifacts_timestamp: None,
            },
        );
        return;
    }
    if kind == KILL_KIND_WORKFLOW {
        if is_child_row(target) {
            return;
        }
        let workflow_name = target.workflow.clone();
        let Some(workflow_name) = workflow_name else {
            return;
        };
        let lookup_cl = if target.identity.cl_name != "unknown" {
            Some(target.identity.cl_name.clone())
        } else {
            None
        };
        side_effects.workspace_release_requests.push(
            AgentCleanupWorkspaceReleaseIntentWire {
                identity: target.identity.clone(),
                project_file: target.project_file.clone().unwrap_or_default(),
                workspace: target.workspace,
                workflow: Some(workflow_name),
                cl_name: lookup_cl,
                lookup_workflow: target.workspace.is_none(),
                lookup_timestamp: false,
                artifacts_timestamp: None,
            },
        );
    }
}

fn add_monitor_stop(
    side_effects: &mut AgentCleanupSideEffectsWire,
    seen: &mut BTreeSet<String>,
    target: &AgentCleanupTargetWire,
    monitor_id: &str,
) {
    if !seen.insert(monitor_id.to_string()) {
        return;
    }
    side_effects.monitor_stop_requests.push(
        AgentCleanupMonitorStopIntentWire {
            identity: target.identity.clone(),
            monitor_id: monitor_id.to_string(),
        },
    );
}

fn add_held_workspace_release(
    side_effects: &mut AgentCleanupSideEffectsWire,
    seen: &mut BTreeSet<AgentCleanupIdentityWire>,
    target: &AgentCleanupTargetWire,
) {
    if is_child_row(target) || !seen.insert(target.identity.clone()) {
        return;
    }
    let Some(artifacts_timestamp) = target.raw_suffix.clone() else {
        return;
    };
    side_effects.workspace_release_requests.push(
        AgentCleanupWorkspaceReleaseIntentWire {
            identity: target.identity.clone(),
            project_file: target.project_file.clone().unwrap_or_default(),
            workspace: None,
            workflow: target.workflow.clone(),
            cl_name: Some(target.identity.cl_name.clone()),
            lookup_workflow: false,
            lookup_timestamp: true,
            artifacts_timestamp: Some(artifacts_timestamp),
        },
    );
}

fn related_workflow_targets<'a>(
    target: &'a AgentCleanupTargetWire,
    children_by_parent: &BTreeMap<
        (String, Option<String>),
        Vec<&'a AgentCleanupTargetWire>,
    >,
) -> Vec<&'a AgentCleanupTargetWire> {
    let mut related = vec![target];
    if target.agent_type == "workflow" && !is_child_row(target) {
        if let Some(raw_suffix) = &target.raw_suffix {
            let key = (raw_suffix.clone(), target.workflow.clone());
            if let Some(children) = children_by_parent.get(&key) {
                related.extend(children.iter().copied());
            }
        }
    }
    related
}

fn build_side_effects(
    targets: &[AgentCleanupTargetWire],
    request: &AgentCleanupRequestWire,
    kill_items: &[AgentCleanupKillItemWire],
    dismiss_items: &[AgentCleanupDismissItemWire],
    children_by_parent: &BTreeMap<
        (String, Option<String>),
        Vec<&AgentCleanupTargetWire>,
    >,
) -> AgentCleanupSideEffectsWire {
    if request.mode == CLEANUP_MODE_PREVIEW_ONLY {
        return AgentCleanupSideEffectsWire::default();
    }

    let by_id = target_by_identity(targets);
    let mut side_effects = AgentCleanupSideEffectsWire::default();
    let mut seen_index = BTreeSet::new();
    let mut seen_bundle = BTreeSet::new();
    let mut seen_artifacts = BTreeSet::new();
    let mut seen_workspace = BTreeSet::new();
    let mut seen_held_workspace = BTreeSet::new();
    let mut seen_notifications = BTreeSet::new();
    let mut seen_monitor_stops = BTreeSet::new();

    for dismiss in dismiss_items {
        let Some(target) = by_id.get(&dismiss.identity).copied() else {
            continue;
        };
        let related = related_workflow_targets(target, children_by_parent);
        for item in related {
            add_index_identity(&mut side_effects, &mut seen_index, item);
            add_bundle_candidate(&mut side_effects, &mut seen_bundle, item);
            add_artifact_delete(&mut side_effects, &mut seen_artifacts, item);
            add_notification_candidate(
                &mut side_effects,
                &mut seen_notifications,
                item,
            );
            if item.agent_type == "run" || item.agent_type == "workflow" {
                add_held_workspace_release(
                    &mut side_effects,
                    &mut seen_held_workspace,
                    item,
                );
            }
            if item.agent_type == "workflow" {
                add_workspace_release(
                    &mut side_effects,
                    &mut seen_workspace,
                    item,
                    KILL_KIND_WORKFLOW,
                );
            }
        }
    }

    for kill in kill_items {
        let Some(target) = by_id.get(&kill.identity).copied() else {
            continue;
        };
        if kill.kind == KILL_KIND_MONITOR {
            if let Some(monitor_id) = &kill.monitor_id {
                add_monitor_stop(
                    &mut side_effects,
                    &mut seen_monitor_stops,
                    target,
                    monitor_id,
                );
            }
            add_index_identity(&mut side_effects, &mut seen_index, target);
            add_notification_candidate(
                &mut side_effects,
                &mut seen_notifications,
                target,
            );
            continue;
        }
        let related = related_workflow_targets(target, children_by_parent);
        for item in related {
            add_index_identity(&mut side_effects, &mut seen_index, item);
            add_notification_candidate(
                &mut side_effects,
                &mut seen_notifications,
                item,
            );
            if kill.kind == KILL_KIND_WORKFLOW {
                add_bundle_candidate(&mut side_effects, &mut seen_bundle, item);
                add_artifact_delete(
                    &mut side_effects,
                    &mut seen_artifacts,
                    item,
                );
            }
        }
        add_workspace_release(
            &mut side_effects,
            &mut seen_workspace,
            target,
            &kill.kind,
        );
    }

    side_effects
}

pub fn plan_agent_cleanup(
    targets: &[AgentCleanupTargetWire],
    request: &AgentCleanupRequestWire,
) -> Result<AgentCleanupPlanWire, String> {
    if request.schema_version != AGENT_CLEANUP_WIRE_SCHEMA_VERSION {
        return Err(format!(
            "agent cleanup wire schema mismatch: got {}, expected {}",
            request.schema_version, AGENT_CLEANUP_WIRE_SCHEMA_VERSION
        ));
    }
    if !matches!(
        request.mode.as_str(),
        CLEANUP_MODE_DISMISS_COMPLETED
            | CLEANUP_MODE_KILL_AND_DISMISS
            | CLEANUP_MODE_PREVIEW_ONLY
    ) {
        return Err(format!("unknown agent cleanup mode: {}", request.mode));
    }

    let selected_ids: BTreeSet<AgentCleanupIdentityWire> =
        request.identities.iter().cloned().collect();
    let parent_tribes = parent_tribes_by_suffix(targets);
    let children_by_parent = workflow_children_by_parent(targets);
    let children_by_parent_ts = children_by_parent_timestamp(targets);
    let parallel_members_by_parent = parallel_members_by_parent(targets);
    let owned_live_monitors = owned_live_monitor_identities(
        targets,
        request,
        &selected_ids,
        &parent_tribes,
        &children_by_parent_ts,
    );

    let mut seen_live = BTreeSet::new();
    let mut selected = Vec::new();
    let mut kill_items = Vec::new();
    let mut dismiss_items = Vec::new();
    let mut cascaded_children = Vec::new();
    let mut skipped_items = Vec::new();
    let mut counts = AgentCleanupCountsWire {
        candidates: targets.len() as u64,
        ..AgentCleanupCountsWire::default()
    };

    for target in targets {
        if target.status == "FAILED" {
            counts.failed += 1;
        }
        if is_dismissable_status(&target.status) {
            counts.completed += 1;
        }
        if target.pid.is_some() && !is_dismissable_status(&target.status) {
            counts.running += 1;
        }

        let in_scope =
            selected_by_scope(target, request, &selected_ids, &parent_tribes);
        let cascaded_monitor =
            !in_scope && owned_live_monitors.contains(&target.identity);
        if !in_scope && !cascaded_monitor {
            add_skip(&mut skipped_items, target, SKIPPED_NOT_IN_SCOPE, None);
            continue;
        }

        let direct_child_target = is_direct_child_target(
            target,
            targets,
            request,
            &selected_ids,
            &parent_tribes,
        );
        if is_workflow_step_child(target) && !direct_child_target {
            add_skip(
                &mut skipped_items,
                target,
                SKIPPED_WORKFLOW_CHILD_CASCADE_ONLY,
                None,
            );
            continue;
        }

        if !seen_live.insert(target.identity.clone()) {
            add_skip(&mut skipped_items, target, SKIPPED_DUPLICATE, None);
            continue;
        }

        if !cascaded_monitor {
            selected.push(target.identity.clone());
        }

        if request.mode == CLEANUP_MODE_PREVIEW_ONLY {
            add_skip(
                &mut skipped_items,
                target,
                SKIPPED_NOT_KILLABLE,
                Some("preview_only".to_string()),
            );
            continue;
        }

        let dismissable =
            !target.is_live_monitor && target_is_dismissable(target, request);
        let killable = target.is_live_monitor
            || (target.pid.is_some() && !is_dismissable_status(&target.status));

        if request.mode == CLEANUP_MODE_DISMISS_COMPLETED {
            if dismissable {
                let family_still_active = parallel_family_members(
                    target,
                    &parallel_members_by_parent,
                )
                .iter()
                .any(|member| !target_is_dismissable(member, request));
                if family_still_active {
                    add_skip(
                        &mut skipped_items,
                        target,
                        SKIPPED_NOT_DISMISSABLE,
                        Some("parallel family still active".to_string()),
                    );
                    continue;
                }
                dismiss_items.push(AgentCleanupDismissItemWire {
                    identity: target.identity.clone(),
                    display_name: target.display_name.clone(),
                });
            } else {
                add_skip(
                    &mut skipped_items,
                    target,
                    SKIPPED_NOT_DISMISSABLE,
                    Some(target.status.clone()),
                );
            }
            continue;
        }

        if target.is_live_monitor {
            if !push_monitor_kill_item(&mut kill_items, target) {
                add_skip(
                    &mut skipped_items,
                    target,
                    SKIPPED_UNKNOWN_KILL_KIND,
                    Some("live monitor missing monitor_id".to_string()),
                );
            }
            continue;
        }

        if dismissable {
            dismiss_items.push(AgentCleanupDismissItemWire {
                identity: target.identity.clone(),
                display_name: target.display_name.clone(),
            });
            continue;
        }

        if !killable {
            add_skip(
                &mut skipped_items,
                target,
                SKIPPED_NOT_KILLABLE,
                Some(target.status.clone()),
            );
            continue;
        }

        let Some(kind) = classify_kill_kind(target) else {
            add_skip(
                &mut skipped_items,
                target,
                SKIPPED_UNKNOWN_KILL_KIND,
                Some(target.agent_type.clone()),
            );
            continue;
        };

        kill_items.push(AgentCleanupKillItemWire {
            identity: target.identity.clone(),
            kind: kind.to_string(),
            pid: target.pid,
            display_name: target.display_name.clone(),
            monitor_id: None,
        });

        if kind == KILL_KIND_WORKFLOW {
            let Some(raw_suffix) = &target.raw_suffix else {
                continue;
            };
            let key = (raw_suffix.clone(), target.workflow.clone());
            if let Some(children) = children_by_parent.get(&key) {
                for child in children {
                    if seen_live.insert(child.identity.clone()) {
                        cascaded_children.push(child.identity.clone());
                    }
                }
            }
        }
    }

    let mut action_identities: BTreeSet<AgentCleanupIdentityWire> = kill_items
        .iter()
        .map(|item| item.identity.clone())
        .chain(dismiss_items.iter().map(|item| item.identity.clone()))
        .collect();
    for root in targets {
        if !action_identities.contains(&root.identity) {
            continue;
        }
        for member in parallel_family_members(root, &parallel_members_by_parent)
        {
            if action_identities.contains(&member.identity) {
                continue;
            }
            if target_is_dismissable(member, request) {
                dismiss_items.push(AgentCleanupDismissItemWire {
                    identity: member.identity.clone(),
                    display_name: member.display_name.clone(),
                });
                action_identities.insert(member.identity.clone());
                continue;
            }
            if request.mode != CLEANUP_MODE_KILL_AND_DISMISS {
                continue;
            }
            if member.is_live_monitor {
                if push_monitor_kill_item(&mut kill_items, member) {
                    action_identities.insert(member.identity.clone());
                }
                continue;
            }
            if member.pid.is_none() {
                continue;
            }
            let Some(kind) = classify_kill_kind(member) else {
                continue;
            };
            kill_items.push(AgentCleanupKillItemWire {
                identity: member.identity.clone(),
                kind: kind.to_string(),
                pid: member.pid,
                display_name: member.display_name.clone(),
                monitor_id: None,
            });
            action_identities.insert(member.identity.clone());
        }
    }

    sort_monitor_kills_first(&mut kill_items);

    counts.selected = selected.len() as u64;
    counts.kill = kill_items.len() as u64;
    counts.dismiss = dismiss_items.len() as u64;
    counts.cascaded_workflow_children = cascaded_children.len() as u64;
    counts.skipped = skipped_items.len() as u64;

    let confirmation_severity = if !kill_items.is_empty() {
        CONFIRMATION_SEVERITY_DESTRUCTIVE
    } else if !dismiss_items.is_empty() {
        CONFIRMATION_SEVERITY_DISMISS
    } else {
        CONFIRMATION_SEVERITY_NONE
    };

    let mut summary_lines = Vec::new();
    push_summary_line(&mut summary_lines, counts.kill, "agent to kill");
    push_summary_line(&mut summary_lines, counts.dismiss, "agent to dismiss");
    push_summary_line(
        &mut summary_lines,
        counts.cascaded_workflow_children,
        "workflow child to hide",
    );
    if summary_lines.is_empty() {
        summary_lines.push("No agents selected for cleanup".to_string());
    }

    let side_effects = build_side_effects(
        targets,
        request,
        &kill_items,
        &dismiss_items,
        &children_by_parent,
    );

    Ok(AgentCleanupPlanWire {
        schema_version: AGENT_CLEANUP_WIRE_SCHEMA_VERSION,
        selected_identities: selected,
        kill_items,
        dismiss_items,
        cascaded_workflow_children: cascaded_children,
        skipped_items,
        counts,
        confirmation_severity: confirmation_severity.to_string(),
        summary_lines,
        side_effects,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(
        agent_type: &str,
        cl_name: &str,
        raw_suffix: Option<&str>,
    ) -> AgentCleanupIdentityWire {
        AgentCleanupIdentityWire {
            agent_type: agent_type.to_string(),
            cl_name: cl_name.to_string(),
            raw_suffix: raw_suffix.map(str::to_string),
        }
    }

    fn target(
        agent_type: &str,
        cl_name: &str,
        raw_suffix: Option<&str>,
        status: &str,
        pid: Option<i64>,
    ) -> AgentCleanupTargetWire {
        AgentCleanupTargetWire {
            identity: id(agent_type, cl_name, raw_suffix),
            agent_type: agent_type.to_string(),
            status: status.to_string(),
            pid,
            workflow: None,
            parent_workflow: None,
            parent_timestamp: None,
            raw_suffix: raw_suffix.map(str::to_string),
            project_file: Some("/tmp/project.sase".to_string()),
            artifacts_dir: Some("/tmp/artifacts".to_string()),
            from_patch: false,
            workspace: None,
            tribe: None,
            agent_clan: None,
            agent_clan_generation: None,
            agent_name: None,
            display_name: Some(cl_name.to_string()),
            start_time: None,
            stop_time: None,
            is_workflow_child: false,
            agent_family_parallel: false,
            appears_as_agent: false,
            step_type: None,
            monitor_id: None,
            is_live_monitor: false,
        }
    }

    fn req(scope: &str, mode: &str) -> AgentCleanupRequestWire {
        AgentCleanupRequestWire {
            schema_version: AGENT_CLEANUP_WIRE_SCHEMA_VERSION,
            scope: scope.to_string(),
            mode: mode.to_string(),
            focused_panel_tribe: None,
            tribe: None,
            clan_name: None,
            clan_generation: None,
            identities: vec![],
            include_pidless_as_dismissable: false,
        }
    }

    #[test]
    fn focused_panel_selects_matching_tribe_and_dismisses_completed() {
        let mut no_tribe = target("run", "no-tribe", Some("1"), "DONE", None);
        no_tribe.tribe = None;
        let mut in_tribe = target("run", "in-tribe", Some("2"), "DONE", None);
        in_tribe.tribe = Some("ops".to_string());
        let mut request =
            req(CLEANUP_SCOPE_FOCUSED_PANEL, CLEANUP_MODE_DISMISS_COMPLETED);
        request.focused_panel_tribe = Some("ops".to_string());

        let plan = plan_agent_cleanup(&[no_tribe, in_tribe], &request).unwrap();

        assert_eq!(plan.dismiss_items.len(), 1);
        assert_eq!(plan.dismiss_items[0].identity.cl_name, "in-tribe");
        assert_eq!(plan.counts.selected, 1);
        assert_eq!(plan.confirmation_severity, CONFIRMATION_SEVERITY_DISMISS);
    }

    #[test]
    fn all_panels_kill_and_dismiss_partitions_targets() {
        let running = target("run", "running", Some("1"), "RUNNING", Some(123));
        let done = target("run", "done", Some("2"), "FAILED", None);

        let plan = plan_agent_cleanup(
            &[running, done],
            &req(CLEANUP_SCOPE_ALL_PANELS, CLEANUP_MODE_KILL_AND_DISMISS),
        )
        .unwrap();

        assert_eq!(plan.kill_items.len(), 1);
        assert_eq!(plan.kill_items[0].kind, KILL_KIND_RUNNING);
        assert_eq!(plan.dismiss_items.len(), 1);
        assert_eq!(plan.counts.running, 1);
        assert_eq!(plan.counts.failed, 1);
        assert_eq!(
            plan.confirmation_severity,
            CONFIRMATION_SEVERITY_DESTRUCTIVE
        );
        assert_eq!(plan.side_effects.workspace_release_requests.len(), 1);
        let held = &plan.side_effects.workspace_release_requests[0];
        assert_eq!(held.identity.cl_name, "done");
        assert!(held.lookup_timestamp);
        assert_eq!(held.artifacts_timestamp.as_deref(), Some("2"));
    }

    #[test]
    fn completed_workflow_parent_gets_timestamp_and_workflow_releases() {
        let mut workflow = target(
            "workflow",
            "feature",
            Some("20260712120000"),
            "FAILED",
            None,
        );
        workflow.workflow = Some("deploy".to_string());
        workflow.workspace = Some(17);

        let plan = plan_agent_cleanup(
            std::slice::from_ref(&workflow),
            &req(CLEANUP_SCOPE_ALL_PANELS, CLEANUP_MODE_DISMISS_COMPLETED),
        )
        .unwrap();

        assert_eq!(plan.side_effects.workspace_release_requests.len(), 2);
        assert!(
            plan.side_effects.workspace_release_requests[0].lookup_timestamp
        );
        assert_eq!(
            plan.side_effects.workspace_release_requests[0]
                .artifacts_timestamp
                .as_deref(),
            Some("20260712120000")
        );
        assert!(
            !plan.side_effects.workspace_release_requests[1].lookup_timestamp
        );
        assert_eq!(
            plan.side_effects.workspace_release_requests[1].workspace,
            Some(17)
        );
    }

    #[test]
    fn explicit_identities_select_only_marked_targets() {
        let a = target("run", "a", Some("1"), "RUNNING", Some(1));
        let b = target("run", "b", Some("2"), "RUNNING", Some(2));
        let mut request = req(
            CLEANUP_SCOPE_EXPLICIT_IDENTITIES,
            CLEANUP_MODE_KILL_AND_DISMISS,
        );
        request.identities = vec![b.identity.clone()];

        let plan = plan_agent_cleanup(&[a, b], &request).unwrap();

        assert_eq!(plan.kill_items.len(), 1);
        assert_eq!(plan.kill_items[0].identity.cl_name, "b");
        assert!(plan
            .skipped_items
            .iter()
            .any(|s| s.reason == SKIPPED_NOT_IN_SCOPE));
    }

    #[test]
    fn tribe_scope_uses_parent_tribe_for_workflow_children_but_skips_child_directly(
    ) {
        let mut parent =
            target("workflow", "parent", Some("p1"), "RUNNING", Some(8));
        parent.workflow = Some("deploy".to_string());
        parent.tribe = Some("ops".to_string());
        let mut child =
            target("workflow", "child", Some("c1"), "RUNNING", Some(9));
        child.parent_timestamp = Some("p1".to_string());
        child.parent_workflow = Some("deploy".to_string());
        child.is_workflow_child = true;
        let mut request =
            req(CLEANUP_SCOPE_TRIBE, CLEANUP_MODE_KILL_AND_DISMISS);
        request.tribe = Some("ops".to_string());

        let plan = plan_agent_cleanup(&[parent, child], &request).unwrap();

        assert_eq!(plan.kill_items.len(), 1);
        assert_eq!(plan.cascaded_workflow_children.len(), 1);
        assert_eq!(plan.cascaded_workflow_children[0].cl_name, "child");
        assert!(plan
            .skipped_items
            .iter()
            .any(|s| s.reason == SKIPPED_WORKFLOW_CHILD_CASCADE_ONLY));
    }

    #[test]
    fn clan_scope_filters_generation_and_partitions_with_workflow_cascade() {
        let mut parent = target(
            "workflow",
            "release",
            Some("parent-ts"),
            "RUNNING",
            Some(101),
        );
        parent.workflow = Some("release".to_string());
        parent.agent_clan = Some("shipping".to_string());
        parent.agent_clan_generation = Some("current-gen".to_string());

        let mut child = target(
            "workflow",
            "release-step",
            Some("child-ts"),
            "RUNNING",
            Some(102),
        );
        child.workflow = Some("release".to_string());
        child.parent_workflow = Some("release".to_string());
        child.parent_timestamp = Some("parent-ts".to_string());
        child.is_workflow_child = true;
        child.agent_clan = Some("shipping".to_string());
        child.agent_clan_generation = Some("current-gen".to_string());

        let mut done = target("run", "verified", Some("done-ts"), "DONE", None);
        done.agent_clan = Some("shipping".to_string());
        done.agent_clan_generation = Some("current-gen".to_string());

        let mut stale =
            target("run", "stale", Some("stale-ts"), "RUNNING", Some(201));
        stale.agent_clan = Some("shipping".to_string());
        stale.agent_clan_generation = Some("stale-gen".to_string());

        let mut other =
            target("run", "other", Some("other-ts"), "RUNNING", Some(202));
        other.agent_clan = Some("research".to_string());
        other.agent_clan_generation = Some("current-gen".to_string());

        let mut request =
            req(CLEANUP_SCOPE_CLAN, CLEANUP_MODE_KILL_AND_DISMISS);
        request.clan_name = Some("shipping".to_string());
        request.clan_generation = Some("current-gen".to_string());

        let plan =
            plan_agent_cleanup(&[parent, child, done, stale, other], &request)
                .unwrap();

        assert_eq!(
            plan.kill_items
                .iter()
                .map(|item| item.identity.cl_name.as_str())
                .collect::<Vec<_>>(),
            vec!["release"]
        );
        assert_eq!(
            plan.dismiss_items
                .iter()
                .map(|item| item.identity.cl_name.as_str())
                .collect::<Vec<_>>(),
            vec!["verified"]
        );
        assert_eq!(
            plan.cascaded_workflow_children
                .iter()
                .map(|identity| identity.cl_name.as_str())
                .collect::<Vec<_>>(),
            vec!["release-step"]
        );
        assert!(plan.skipped_items.iter().any(|item| {
            item.identity.cl_name == "stale"
                && item.reason == SKIPPED_NOT_IN_SCOPE
        }));
        assert!(plan.skipped_items.iter().any(|item| {
            item.identity.cl_name == "other"
                && item.reason == SKIPPED_NOT_IN_SCOPE
        }));
    }

    #[test]
    fn clan_scope_without_generation_selects_all_generations() {
        let mut current =
            target("run", "current", Some("current-ts"), "RUNNING", Some(101));
        current.agent_clan = Some("research".to_string());
        current.agent_clan_generation = Some("current-gen".to_string());
        let mut stale =
            target("run", "stale", Some("stale-ts"), "RUNNING", Some(102));
        stale.agent_clan = Some("research".to_string());
        stale.agent_clan_generation = Some("stale-gen".to_string());
        let mut request =
            req(CLEANUP_SCOPE_CLAN, CLEANUP_MODE_KILL_AND_DISMISS);
        request.clan_name = Some("research".to_string());

        let plan = plan_agent_cleanup(&[current, stale], &request).unwrap();

        assert_eq!(
            plan.kill_items
                .iter()
                .map(|item| item.identity.cl_name.as_str())
                .collect::<Vec<_>>(),
            vec!["current", "stale"]
        );
    }

    #[test]
    fn clan_scope_keeps_active_parallel_family_root_from_dismissal() {
        let mut root = target("run", "family", Some("root-ts"), "DONE", None);
        root.agent_family_parallel = true;
        root.agent_clan = Some("research".to_string());
        root.agent_clan_generation = Some("generation".to_string());
        let mut member =
            target("run", "family.1", Some("member-ts"), "RUNNING", Some(101));
        member.agent_family_parallel = true;
        member.parent_timestamp = Some("root-ts".to_string());
        member.agent_clan = Some("research".to_string());
        member.agent_clan_generation = Some("generation".to_string());
        let mut request =
            req(CLEANUP_SCOPE_CLAN, CLEANUP_MODE_DISMISS_COMPLETED);
        request.clan_name = Some("research".to_string());
        request.clan_generation = Some("generation".to_string());

        let plan = plan_agent_cleanup(&[root, member], &request).unwrap();

        assert!(plan.dismiss_items.is_empty());
        assert!(plan.skipped_items.iter().any(|item| {
            item.identity.cl_name == "family"
                && item.reason == SKIPPED_NOT_DISMISSABLE
                && item.detail.as_deref()
                    == Some("parallel family still active")
        }));
    }

    #[test]
    fn workflow_parent_cascade_deduplicates_child_inputs() {
        let mut parent =
            target("workflow", "wf", Some("root"), "RUNNING", Some(10));
        parent.workflow = Some("build".to_string());
        let mut child =
            target("workflow", "step", Some("child"), "RUNNING", Some(11));
        child.parent_timestamp = Some("root".to_string());
        child.parent_workflow = Some("build".to_string());
        child.is_workflow_child = true;
        let mut request = req(
            CLEANUP_SCOPE_EXPLICIT_IDENTITIES,
            CLEANUP_MODE_KILL_AND_DISMISS,
        );
        request.identities =
            vec![parent.identity.clone(), child.identity.clone()];

        let plan = plan_agent_cleanup(&[child, parent], &request).unwrap();

        assert_eq!(plan.kill_items.len(), 1);
        assert_eq!(plan.kill_items[0].identity.cl_name, "wf");
        assert_eq!(plan.cascaded_workflow_children.len(), 1);
        assert_eq!(
            plan.cascaded_workflow_children[0].raw_suffix.as_deref(),
            Some("child")
        );
    }

    #[test]
    fn parallel_family_root_kill_cascades_to_live_members_only() {
        let mut root =
            target("run", "sase-6g", Some("root-ts"), "RUNNING", Some(10));
        root.agent_family_parallel = true;
        let mut phase_one = target(
            "run",
            "sase-6g.1",
            Some("phase-one-ts"),
            "RUNNING",
            Some(11),
        );
        phase_one.agent_family_parallel = true;
        phase_one.parent_timestamp = Some("root-ts".to_string());
        let mut phase_two = target(
            "run",
            "sase-6g.2",
            Some("phase-two-ts"),
            "RUNNING",
            Some(12),
        );
        phase_two.agent_family_parallel = true;
        phase_two.parent_timestamp = Some("root-ts".to_string());
        let mut serial_child = target(
            "run",
            "sase-6g--code",
            Some("serial-ts"),
            "RUNNING",
            Some(13),
        );
        serial_child.parent_timestamp = Some("root-ts".to_string());
        let mut request = req(
            CLEANUP_SCOPE_EXPLICIT_IDENTITIES,
            CLEANUP_MODE_KILL_AND_DISMISS,
        );
        request.identities = vec![root.identity.clone()];

        let plan = plan_agent_cleanup(
            &[root, phase_one, phase_two, serial_child],
            &request,
        )
        .unwrap();

        assert_eq!(
            plan.kill_items
                .iter()
                .map(|item| item.identity.cl_name.as_str())
                .collect::<Vec<_>>(),
            vec!["sase-6g", "sase-6g.1", "sase-6g.2"]
        );
        assert!(!plan
            .kill_items
            .iter()
            .any(|item| item.identity.cl_name == "sase-6g--code"));
    }

    #[test]
    fn killing_one_parallel_member_leaves_root_and_siblings_untouched() {
        let mut root =
            target("run", "sase-6g", Some("root-ts"), "RUNNING", Some(10));
        root.agent_family_parallel = true;
        let mut selected = target(
            "run",
            "sase-6g.1",
            Some("phase-one-ts"),
            "RUNNING",
            Some(11),
        );
        selected.agent_family_parallel = true;
        selected.parent_timestamp = Some("root-ts".to_string());
        let mut sibling = target(
            "run",
            "sase-6g.2",
            Some("phase-two-ts"),
            "RUNNING",
            Some(12),
        );
        sibling.agent_family_parallel = true;
        sibling.parent_timestamp = Some("root-ts".to_string());
        let mut request = req(
            CLEANUP_SCOPE_EXPLICIT_IDENTITIES,
            CLEANUP_MODE_KILL_AND_DISMISS,
        );
        request.identities = vec![selected.identity.clone()];

        let plan =
            plan_agent_cleanup(&[root, selected, sibling], &request).unwrap();

        assert_eq!(plan.kill_items.len(), 1);
        assert_eq!(plan.kill_items[0].identity.cl_name, "sase-6g.1");
    }

    #[test]
    fn dismissing_parallel_root_cascades_only_after_members_finish() {
        let mut root = target("run", "root", Some("root-ts"), "DONE", None);
        root.agent_family_parallel = true;
        let mut member =
            target("run", "member", Some("member-ts"), "RUNNING", Some(11));
        member.agent_family_parallel = true;
        member.parent_timestamp = Some("root-ts".to_string());
        let mut request = req(
            CLEANUP_SCOPE_EXPLICIT_IDENTITIES,
            CLEANUP_MODE_DISMISS_COMPLETED,
        );
        request.identities = vec![root.identity.clone()];

        let active_plan =
            plan_agent_cleanup(&[root.clone(), member.clone()], &request)
                .unwrap();
        assert!(active_plan.dismiss_items.is_empty());
        assert!(active_plan.skipped_items.iter().any(|item| {
            item.identity == root.identity
                && item.reason == SKIPPED_NOT_DISMISSABLE
                && item.detail.as_deref()
                    == Some("parallel family still active")
        }));

        member.status = "DONE".to_string();
        member.pid = None;
        let finished_plan =
            plan_agent_cleanup(&[root, member], &request).unwrap();
        assert_eq!(
            finished_plan
                .dismiss_items
                .iter()
                .map(|item| item.identity.cl_name.as_str())
                .collect::<Vec<_>>(),
            vec!["root", "member"]
        );
    }

    #[test]
    fn explicit_child_only_running_target_becomes_kill_item() {
        let parent = target("run", "parent", Some("root"), "RUNNING", Some(10));
        let mut child =
            target("run", "child", Some("child"), "RUNNING", Some(11));
        child.parent_timestamp = Some("root".to_string());
        let mut request = req(
            CLEANUP_SCOPE_EXPLICIT_IDENTITIES,
            CLEANUP_MODE_KILL_AND_DISMISS,
        );
        request.identities = vec![child.identity.clone()];

        let plan = plan_agent_cleanup(&[parent, child], &request).unwrap();

        assert_eq!(plan.kill_items.len(), 1);
        assert_eq!(plan.kill_items[0].identity.cl_name, "child");
        assert_eq!(plan.kill_items[0].kind, KILL_KIND_RUNNING);
        assert!(plan.dismiss_items.is_empty());
        assert!(plan.cascaded_workflow_children.is_empty());
    }

    #[test]
    fn explicit_child_only_completed_target_becomes_dismiss_item() {
        let parent = target("run", "parent", Some("root"), "RUNNING", Some(10));
        let mut child = target("run", "child", Some("child"), "DONE", None);
        child.parent_timestamp = Some("root".to_string());
        let mut request = req(
            CLEANUP_SCOPE_EXPLICIT_IDENTITIES,
            CLEANUP_MODE_KILL_AND_DISMISS,
        );
        request.identities = vec![child.identity.clone()];

        let plan = plan_agent_cleanup(&[parent, child], &request).unwrap();

        assert!(plan.kill_items.is_empty());
        assert_eq!(plan.dismiss_items.len(), 1);
        assert_eq!(plan.dismiss_items[0].identity.cl_name, "child");
        assert!(plan.cascaded_workflow_children.is_empty());
    }

    fn broad_scope_kill_requests() -> Vec<AgentCleanupRequestWire> {
        let mut focused =
            req(CLEANUP_SCOPE_FOCUSED_PANEL, CLEANUP_MODE_KILL_AND_DISMISS);
        focused.focused_panel_tribe = Some("ops".to_string());
        let mut tribe_request =
            req(CLEANUP_SCOPE_TRIBE, CLEANUP_MODE_KILL_AND_DISMISS);
        tribe_request.tribe = Some("ops".to_string());
        vec![
            req(CLEANUP_SCOPE_ALL_PANELS, CLEANUP_MODE_KILL_AND_DISMISS),
            focused,
            tribe_request,
        ]
    }

    #[test]
    fn broad_scopes_act_on_family_member_child_rows_directly() {
        let mut child =
            target("run", "child", Some("child"), "RUNNING", Some(11));
        child.parent_timestamp = Some("root".to_string());
        child.tribe = Some("ops".to_string());

        for request in broad_scope_kill_requests() {
            let plan =
                plan_agent_cleanup(std::slice::from_ref(&child), &request)
                    .unwrap();

            assert_eq!(plan.kill_items.len(), 1);
            assert_eq!(plan.kill_items[0].identity.cl_name, "child");
            assert!(plan.dismiss_items.is_empty());
            assert!(plan.cascaded_workflow_children.is_empty());
            assert!(!plan.skipped_items.iter().any(|item| {
                item.reason == SKIPPED_WORKFLOW_CHILD_CASCADE_ONLY
            }));
        }
    }

    #[test]
    fn broad_scopes_keep_workflow_step_children_cascade_only() {
        let mut child =
            target("run", "child", Some("child"), "RUNNING", Some(11));
        child.parent_timestamp = Some("root".to_string());
        child.parent_workflow = Some("build".to_string());
        child.tribe = Some("ops".to_string());

        for request in broad_scope_kill_requests() {
            let plan =
                plan_agent_cleanup(std::slice::from_ref(&child), &request)
                    .unwrap();

            assert!(plan.kill_items.is_empty());
            assert!(plan.dismiss_items.is_empty());
            assert!(plan.cascaded_workflow_children.is_empty());
            assert_eq!(
                plan.skipped_items[0].reason,
                SKIPPED_WORKFLOW_CHILD_CASCADE_ONLY
            );
        }
    }

    fn clan_sequential_family_chain() -> (
        AgentCleanupTargetWire,
        AgentCleanupTargetWire,
        AgentCleanupTargetWire,
    ) {
        let mut plan_root =
            target("run", "sase-ps.plan", Some("20260818102050"), "DONE", None);
        plan_root.agent_clan = Some("sase-ps".to_string());
        plan_root.agent_clan_generation = Some("20260818102050".to_string());
        plan_root.agent_family_parallel = false;

        let mut family_root = target(
            "run",
            "sase-ps.plan--1",
            Some("20260818114621"),
            "DONE",
            None,
        );
        family_root.parent_timestamp = Some("20260818102050".to_string());
        family_root.agent_clan = Some("sase-ps".to_string());
        family_root.agent_clan_generation = Some("20260818102050".to_string());
        family_root.agent_family_parallel = false;

        let mut monitor = target(
            "run",
            "sase-ps.plan--mon",
            Some("20260818114457"),
            "DONE",
            None,
        );
        monitor.parent_timestamp = Some("20260818114621".to_string());
        monitor.agent_clan = Some("sase-ps".to_string());
        monitor.agent_clan_generation = Some("20260818102050".to_string());
        monitor.agent_family_parallel = false;

        (plan_root, family_root, monitor)
    }

    fn assert_clan_sequential_family_dismissed(plan: &AgentCleanupPlanWire) {
        assert_eq!(
            plan.dismiss_items
                .iter()
                .map(|item| item.identity.cl_name.as_str())
                .collect::<Vec<_>>(),
            vec!["sase-ps.plan", "sase-ps.plan--1", "sase-ps.plan--mon"]
        );
        assert_eq!(
            plan.side_effects
                .dismissed_index_additions
                .iter()
                .map(|identity| identity.cl_name.as_str())
                .collect::<Vec<_>>(),
            vec!["sase-ps.plan", "sase-ps.plan--1", "sase-ps.plan--mon"]
        );
        assert!(!plan.skipped_items.iter().any(|item| {
            item.reason == SKIPPED_WORKFLOW_CHILD_CASCADE_ONLY
        }));
    }

    #[test]
    fn clan_scope_dismisses_sequential_family_and_monitor_rows() {
        let (plan_root, family_root, monitor) = clan_sequential_family_chain();
        let mut request =
            req(CLEANUP_SCOPE_CLAN, CLEANUP_MODE_KILL_AND_DISMISS);
        request.clan_name = Some("sase-ps".to_string());
        request.clan_generation = Some("20260818102050".to_string());

        let plan =
            plan_agent_cleanup(&[plan_root, family_root, monitor], &request)
                .unwrap();
        assert_clan_sequential_family_dismissed(&plan);
    }

    #[test]
    fn explicit_identities_dismiss_sequential_family_and_monitor_rows() {
        let (plan_root, family_root, monitor) = clan_sequential_family_chain();
        let mut request = req(
            CLEANUP_SCOPE_EXPLICIT_IDENTITIES,
            CLEANUP_MODE_KILL_AND_DISMISS,
        );
        request.identities = vec![
            plan_root.identity.clone(),
            family_root.identity.clone(),
            monitor.identity.clone(),
        ];

        let plan =
            plan_agent_cleanup(&[plan_root, family_root, monitor], &request)
                .unwrap();
        assert_clan_sequential_family_dismissed(&plan);
    }

    #[test]
    fn direct_child_side_effects_include_child_not_siblings() {
        let parent = target("run", "parent", Some("root"), "RUNNING", Some(10));
        let mut child =
            target("run", "child", Some("child"), "RUNNING", Some(11));
        child.parent_timestamp = Some("root".to_string());
        child.workspace = Some(7);
        let mut sibling =
            target("run", "sibling", Some("sibling"), "RUNNING", Some(12));
        sibling.parent_timestamp = Some("root".to_string());
        sibling.workspace = Some(8);
        let mut request = req(
            CLEANUP_SCOPE_EXPLICIT_IDENTITIES,
            CLEANUP_MODE_KILL_AND_DISMISS,
        );
        request.identities = vec![child.identity.clone()];

        let plan =
            plan_agent_cleanup(&[parent, child, sibling], &request).unwrap();

        assert_eq!(
            plan.side_effects
                .dismissed_index_additions
                .iter()
                .map(|identity| identity.cl_name.as_str())
                .collect::<Vec<_>>(),
            vec!["child"]
        );
        assert_eq!(
            plan.side_effects
                .workspace_release_requests
                .iter()
                .map(|intent| intent.identity.cl_name.as_str())
                .collect::<Vec<_>>(),
            vec!["child"]
        );
        assert_eq!(
            plan.side_effects
                .notification_dismiss_candidates
                .iter()
                .map(|intent| intent.identity.cl_name.as_str())
                .collect::<Vec<_>>(),
            vec!["child"]
        );
    }

    #[test]
    fn direct_workflow_child_does_not_release_parent_workspace_claim() {
        let mut parent =
            target("workflow", "wf", Some("root"), "RUNNING", Some(10));
        parent.workflow = Some("build".to_string());
        let mut child =
            target("workflow", "step", Some("child"), "RUNNING", Some(11));
        child.parent_timestamp = Some("root".to_string());
        child.parent_workflow = Some("build".to_string());
        child.is_workflow_child = true;
        child.workspace = Some(7);
        let mut request = req(
            CLEANUP_SCOPE_EXPLICIT_IDENTITIES,
            CLEANUP_MODE_KILL_AND_DISMISS,
        );
        request.identities = vec![child.identity.clone()];

        let plan = plan_agent_cleanup(&[parent, child], &request).unwrap();

        assert_eq!(plan.kill_items.len(), 1);
        assert_eq!(plan.kill_items[0].identity.cl_name, "step");
        assert_eq!(plan.kill_items[0].kind, KILL_KIND_WORKFLOW);
        assert_eq!(
            plan.side_effects
                .dismissed_index_additions
                .iter()
                .map(|identity| identity.cl_name.as_str())
                .collect::<Vec<_>>(),
            vec!["step"]
        );
        assert!(plan.side_effects.workspace_release_requests.is_empty());
    }

    #[test]
    fn no_op_plan_reports_none_severity() {
        let done = target("run", "done", Some("1"), "DONE", None);
        let plan = plan_agent_cleanup(
            &[done],
            &req(CLEANUP_SCOPE_ALL_PANELS, CLEANUP_MODE_PREVIEW_ONLY),
        )
        .unwrap();

        assert!(plan.kill_items.is_empty());
        assert!(plan.dismiss_items.is_empty());
        assert_eq!(plan.confirmation_severity, CONFIRMATION_SEVERITY_NONE);
        assert_eq!(plan.summary_lines, vec!["No agents selected for cleanup"]);
    }

    #[test]
    fn unknown_kill_kind_is_skipped() {
        let unknown = target("mystery", "odd", Some("1"), "RUNNING", Some(4));
        let plan = plan_agent_cleanup(
            &[unknown],
            &req(CLEANUP_SCOPE_ALL_PANELS, CLEANUP_MODE_KILL_AND_DISMISS),
        )
        .unwrap();

        assert!(plan.kill_items.is_empty());
        assert_eq!(plan.skipped_items[0].reason, SKIPPED_UNKNOWN_KILL_KIND);
    }

    #[test]
    fn dismiss_side_effects_preserve_names() {
        let mut parent =
            target("workflow", "wf", Some("20260428100000"), "DONE", None);
        parent.workflow = Some("deploy".to_string());
        parent.agent_name = Some("root".to_string());
        let mut child =
            target("workflow", "wf", Some("20260428100000_c0"), "DONE", None);
        child.parent_timestamp = Some("20260428100000".to_string());
        child.parent_workflow = Some("deploy".to_string());
        child.agent_name = Some("root.plan".to_string());
        child.is_workflow_child = true;
        child.artifacts_dir = Some("/tmp/child".to_string());

        let plan = plan_agent_cleanup(
            &[parent, child],
            &req(CLEANUP_SCOPE_ALL_PANELS, CLEANUP_MODE_DISMISS_COMPLETED),
        )
        .unwrap();

        assert_eq!(plan.side_effects.dismissed_index_additions.len(), 2);
        assert_eq!(plan.side_effects.bundle_save_candidates.len(), 2);
        assert_eq!(plan.side_effects.artifact_delete_paths.len(), 2);
    }

    #[test]
    fn dismissable_statuses_include_tale_done() {
        // ``TALE DONE`` is the terminal display for completed tale plan
        // workflows; it must be treated identically to ``PLAN DONE`` by the
        // cleanup planner so a tale finishing also surfaces ``x dismiss``.
        assert!(is_dismissable_status("TALE DONE"));
        assert!(is_dismissable_status("PLAN DONE"));
        assert!(!is_dismissable_status("TALE APPROVED"));
        assert!(!is_dismissable_status("RUNNING"));
    }

    #[test]
    fn dismissable_statuses_include_stopped() {
        // ``STOPPED`` is the terminal display for a repeat-chain slot skipped
        // by a predecessor's STOP. It is a non-error finished row, so the
        // cleanup planner must treat it as dismissable (mirroring the Python
        // TUI) — never as failed or running.
        assert!(is_dismissable_status("STOPPED"));
    }

    #[test]
    fn stopped_row_is_dismissed_not_killed_or_failed() {
        // A pidless STOPPED row must dismiss cleanly and never count as
        // failed or running.
        let stopped = target("run", "skipped", Some("1"), "STOPPED", None);
        let plan = plan_agent_cleanup(
            &[stopped],
            &req(CLEANUP_SCOPE_ALL_PANELS, CLEANUP_MODE_DISMISS_COMPLETED),
        )
        .unwrap();

        assert_eq!(plan.dismiss_items.len(), 1);
        assert_eq!(plan.dismiss_items[0].identity.cl_name, "skipped");
        assert!(plan.kill_items.is_empty());
        assert_eq!(plan.counts.completed, 1);
        assert_eq!(plan.counts.failed, 0);
        assert_eq!(plan.counts.running, 0);
        assert_eq!(plan.confirmation_severity, CONFIRMATION_SEVERITY_DISMISS);
    }

    #[test]
    fn dismiss_side_effects_allow_duplicate_historical_names() {
        let mut first =
            target("run", "cl_a", Some("20260428100000"), "DONE", None);
        first.agent_name = Some("foo".to_string());
        let mut second =
            target("run", "cl_b", Some("20260428110000"), "DONE", None);
        second.agent_name = Some("foo".to_string());
        let plan = plan_agent_cleanup(
            &[first, second],
            &req(CLEANUP_SCOPE_ALL_PANELS, CLEANUP_MODE_DISMISS_COMPLETED),
        )
        .unwrap();

        assert_eq!(plan.side_effects.dismissed_index_additions.len(), 2);
    }

    fn live_monitor(
        cl_name: &str,
        raw_suffix: &str,
        parent_timestamp: &str,
        monitor_id: &str,
        pid: Option<i64>,
    ) -> AgentCleanupTargetWire {
        let mut monitor =
            target("run", cl_name, Some(raw_suffix), "MONITORING", pid);
        monitor.parent_timestamp = Some(parent_timestamp.to_string());
        monitor.monitor_id = Some(monitor_id.to_string());
        monitor.is_live_monitor = true;
        monitor.workspace = Some(15);
        monitor
    }

    #[test]
    fn rejects_previous_cleanup_wire_schema() {
        let mut request =
            req(CLEANUP_SCOPE_ALL_PANELS, CLEANUP_MODE_KILL_AND_DISMISS);
        request.schema_version = 3;
        let err = plan_agent_cleanup(&[], &request).unwrap_err();
        assert!(err.contains("schema mismatch"));
        assert!(err.contains("expected 4"));
    }

    #[test]
    fn direct_live_monitor_selection_is_a_monitor_stop() {
        let owner = target("run", "owner", Some("owner-ts"), "DONE", None);
        let monitor = live_monitor(
            "owner--mon",
            "mon-ts",
            "owner-ts",
            "monid123456",
            Some(1_665_545),
        );
        let mut request = req(
            CLEANUP_SCOPE_EXPLICIT_IDENTITIES,
            CLEANUP_MODE_KILL_AND_DISMISS,
        );
        request.identities = vec![monitor.identity.clone()];

        let plan = plan_agent_cleanup(&[owner, monitor], &request).unwrap();

        assert_eq!(plan.kill_items.len(), 1);
        assert_eq!(plan.kill_items[0].kind, KILL_KIND_MONITOR);
        assert_eq!(
            plan.kill_items[0].monitor_id.as_deref(),
            Some("monid123456")
        );
        assert_eq!(plan.kill_items[0].pid, None);
        assert_eq!(plan.side_effects.monitor_stop_requests.len(), 1);
        assert_eq!(
            plan.side_effects.monitor_stop_requests[0].monitor_id,
            "monid123456"
        );
        assert!(plan.side_effects.workspace_release_requests.is_empty());
        assert_eq!(
            plan.side_effects
                .dismissed_index_additions
                .iter()
                .map(|identity| identity.cl_name.as_str())
                .collect::<Vec<_>>(),
            vec!["owner--mon"]
        );
    }

    #[test]
    fn selected_owner_cascades_to_nested_live_monitor() {
        let plan_root =
            target("run", "sase-ru.6", Some("root-ts"), "DONE", None);
        let mut family =
            target("run", "sase-ru.6--1", Some("family-ts"), "DONE", None);
        family.parent_timestamp = Some("root-ts".to_string());
        let monitor = live_monitor(
            "sase-ru.6--mon-1",
            "mon-ts",
            "family-ts",
            "0fmbm91hgytw",
            Some(99),
        );
        let mut request = req(
            CLEANUP_SCOPE_EXPLICIT_IDENTITIES,
            CLEANUP_MODE_KILL_AND_DISMISS,
        );
        request.identities = vec![plan_root.identity.clone()];

        let plan = plan_agent_cleanup(&[plan_root, family, monitor], &request)
            .unwrap();

        assert_eq!(
            plan.kill_items
                .iter()
                .map(|item| (
                    item.identity.cl_name.as_str(),
                    item.kind.as_str()
                ))
                .collect::<Vec<_>>(),
            vec![("sase-ru.6--mon-1", KILL_KIND_MONITOR)]
        );
        assert_eq!(
            plan.dismiss_items
                .iter()
                .map(|item| item.identity.cl_name.as_str())
                .collect::<Vec<_>>(),
            vec!["sase-ru.6"]
        );
        assert_eq!(plan.side_effects.monitor_stop_requests.len(), 1);
        assert!(plan
            .side_effects
            .workspace_release_requests
            .iter()
            .all(|intent| intent.identity.cl_name != "sase-ru.6--mon-1"));
    }

    #[test]
    fn clan_scope_deduplicates_already_selected_live_monitor() {
        let mut owner =
            target("run", "clan.one", Some("owner-ts"), "DONE", None);
        owner.agent_clan = Some("shipping".to_string());
        owner.agent_clan_generation = Some("gen".to_string());
        let mut monitor = live_monitor(
            "clan.one--mon",
            "mon-ts",
            "owner-ts",
            "monabc123456",
            Some(7),
        );
        monitor.agent_clan = Some("shipping".to_string());
        monitor.agent_clan_generation = Some("gen".to_string());
        let mut request =
            req(CLEANUP_SCOPE_CLAN, CLEANUP_MODE_KILL_AND_DISMISS);
        request.clan_name = Some("shipping".to_string());
        request.clan_generation = Some("gen".to_string());

        let plan = plan_agent_cleanup(&[owner, monitor], &request).unwrap();

        assert_eq!(
            plan.kill_items
                .iter()
                .map(|item| item.identity.cl_name.as_str())
                .collect::<Vec<_>>(),
            vec!["clan.one--mon"]
        );
        assert_eq!(plan.side_effects.monitor_stop_requests.len(), 1);
    }

    #[test]
    fn custom_scope_owner_does_not_stop_unrelated_sibling_monitor() {
        let owner = target("run", "lane.a", Some("a-ts"), "RUNNING", Some(1));
        let sibling = target("run", "lane.b", Some("b-ts"), "RUNNING", Some(2));
        let owned = live_monitor(
            "lane.a--mon",
            "a-mon-ts",
            "a-ts",
            "mona11111111",
            Some(3),
        );
        let unrelated = live_monitor(
            "lane.b--mon",
            "b-mon-ts",
            "b-ts",
            "monb22222222",
            Some(4),
        );
        let mut request = req(
            CLEANUP_SCOPE_CUSTOM_SELECTION,
            CLEANUP_MODE_KILL_AND_DISMISS,
        );
        request.identities = vec![owner.identity.clone()];

        let plan =
            plan_agent_cleanup(&[owner, sibling, owned, unrelated], &request)
                .unwrap();

        let killed: Vec<&str> = plan
            .kill_items
            .iter()
            .map(|item| item.identity.cl_name.as_str())
            .collect();
        assert!(killed.contains(&"lane.a"));
        assert!(killed.contains(&"lane.a--mon"));
        assert!(!killed.contains(&"lane.b"));
        assert!(!killed.contains(&"lane.b--mon"));
        assert_eq!(
            plan.side_effects
                .monitor_stop_requests
                .iter()
                .map(|intent| intent.monitor_id.as_str())
                .collect::<Vec<_>>(),
            vec!["mona11111111"]
        );
    }

    #[test]
    fn terminal_monitor_is_dismissed_not_stopped() {
        let mut monitor =
            target("run", "owner--mon", Some("mon-ts"), "DONE", None);
        monitor.parent_timestamp = Some("owner-ts".to_string());
        monitor.monitor_id = Some("monid123456".to_string());
        monitor.is_live_monitor = false;
        let mut request = req(
            CLEANUP_SCOPE_EXPLICIT_IDENTITIES,
            CLEANUP_MODE_KILL_AND_DISMISS,
        );
        request.identities = vec![monitor.identity.clone()];

        let plan = plan_agent_cleanup(std::slice::from_ref(&monitor), &request)
            .unwrap();

        assert!(plan.kill_items.is_empty());
        assert_eq!(plan.dismiss_items.len(), 1);
        assert!(plan.side_effects.monitor_stop_requests.is_empty());
    }

    #[test]
    fn running_owner_kill_releases_owner_workspace_not_monitor_claim() {
        let mut owner =
            target("run", "owner", Some("owner-ts"), "RUNNING", Some(11));
        owner.workspace = Some(4);
        let monitor = live_monitor(
            "owner--mon",
            "mon-ts",
            "owner-ts",
            "monid123456",
            Some(12),
        );
        let mut request = req(
            CLEANUP_SCOPE_EXPLICIT_IDENTITIES,
            CLEANUP_MODE_KILL_AND_DISMISS,
        );
        request.identities = vec![owner.identity.clone()];

        let plan = plan_agent_cleanup(&[owner, monitor], &request).unwrap();

        assert_eq!(plan.kill_items[0].kind, KILL_KIND_MONITOR);
        assert_eq!(plan.kill_items[1].kind, KILL_KIND_RUNNING);
        assert_eq!(
            plan.side_effects
                .workspace_release_requests
                .iter()
                .map(|intent| intent.identity.cl_name.as_str())
                .collect::<Vec<_>>(),
            vec!["owner"]
        );
        assert_eq!(plan.side_effects.monitor_stop_requests.len(), 1);
    }
}
