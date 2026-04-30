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
    AgentCleanupDismissalRenameIntentWire, AgentCleanupIdentityWire,
    AgentCleanupKillItemWire, AgentCleanupNotificationDismissIntentWire,
    AgentCleanupPlanWire, AgentCleanupRequestWire, AgentCleanupSideEffectsWire,
    AgentCleanupSkippedItemWire, AgentCleanupTargetWire,
    AgentCleanupWorkspaceReleaseIntentWire, AGENT_CLEANUP_WIRE_SCHEMA_VERSION,
    CLEANUP_MODE_DISMISS_COMPLETED, CLEANUP_MODE_KILL_AND_DISMISS,
    CLEANUP_MODE_PREVIEW_ONLY, CLEANUP_SCOPE_ALL_PANELS,
    CLEANUP_SCOPE_CUSTOM_SELECTION, CLEANUP_SCOPE_EXPLICIT_IDENTITIES,
    CLEANUP_SCOPE_FOCUSED_GROUP, CLEANUP_SCOPE_FOCUSED_PANEL,
    CLEANUP_SCOPE_TAG, CONFIRMATION_SEVERITY_DESTRUCTIVE,
    CONFIRMATION_SEVERITY_DISMISS, CONFIRMATION_SEVERITY_NONE, KILL_KIND_CRS,
    KILL_KIND_HOOK, KILL_KIND_MENTOR, KILL_KIND_RUNNING, KILL_KIND_WORKFLOW,
    SKIPPED_DUPLICATE, SKIPPED_NOT_DISMISSABLE, SKIPPED_NOT_IN_SCOPE,
    SKIPPED_NOT_KILLABLE, SKIPPED_UNKNOWN_KILL_KIND,
    SKIPPED_WORKFLOW_CHILD_CASCADE_ONLY,
};

const DISMISSABLE_STATUSES: &[&str] = &[
    "DONE",
    "FAILED",
    "PLAN COMMITTED",
    "PLAN DONE",
    "PLAN REJECTED",
    "EPIC CREATED",
];

fn is_dismissable_status(status: &str) -> bool {
    DISMISSABLE_STATUSES.contains(&status)
}

fn is_workflow_child(target: &AgentCleanupTargetWire) -> bool {
    target.is_workflow_child
        || target.parent_workflow.is_some()
        || target.parent_timestamp.is_some()
}

fn effective_tag(
    target: &AgentCleanupTargetWire,
    parent_tags: &BTreeMap<String, Option<String>>,
) -> Option<String> {
    if is_workflow_child(target) {
        if let Some(parent_ts) = &target.parent_timestamp {
            if let Some(tag) = parent_tags.get(parent_ts) {
                return tag.clone();
            }
        }
    }
    target.tag.clone()
}

fn selected_by_scope(
    target: &AgentCleanupTargetWire,
    request: &AgentCleanupRequestWire,
    selected_ids: &BTreeSet<AgentCleanupIdentityWire>,
    parent_tags: &BTreeMap<String, Option<String>>,
) -> bool {
    match request.scope.as_str() {
        CLEANUP_SCOPE_ALL_PANELS => true,
        CLEANUP_SCOPE_FOCUSED_PANEL => {
            effective_tag(target, parent_tags) == request.focused_panel_tag
        }
        CLEANUP_SCOPE_TAG => effective_tag(target, parent_tags) == request.tag,
        CLEANUP_SCOPE_EXPLICIT_IDENTITIES
        | CLEANUP_SCOPE_FOCUSED_GROUP
        | CLEANUP_SCOPE_CUSTOM_SELECTION => {
            selected_ids.contains(&target.identity)
        }
        _ => false,
    }
}

fn classify_kill_kind(target: &AgentCleanupTargetWire) -> Option<&'static str> {
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

fn workflow_children_by_parent(
    targets: &[AgentCleanupTargetWire],
) -> BTreeMap<(String, Option<String>), Vec<&AgentCleanupTargetWire>> {
    let mut children: BTreeMap<
        (String, Option<String>),
        Vec<&AgentCleanupTargetWire>,
    > = BTreeMap::new();
    for target in targets {
        if !is_workflow_child(target) {
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

fn parent_tags_by_suffix(
    targets: &[AgentCleanupTargetWire],
) -> BTreeMap<String, Option<String>> {
    let mut tags = BTreeMap::new();
    for target in targets {
        if is_workflow_child(target) {
            continue;
        }
        if let Some(raw_suffix) = &target.raw_suffix {
            tags.insert(raw_suffix.clone(), target.tag.clone());
        }
    }
    tags
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

fn is_dismissed_prefixed(name: &str) -> bool {
    let bytes = name.as_bytes();
    bytes.len() > 7
        && bytes[0..6].iter().all(u8::is_ascii_digit)
        && bytes[6] == b'.'
}

fn strip_dismissed_prefix(name: &str) -> &str {
    if is_dismissed_prefixed(name) {
        &name[7..]
    } else {
        name
    }
}

fn completion_date_prefix(target: &AgentCleanupTargetWire) -> String {
    for value in [&target.stop_time, &target.start_time]
        .into_iter()
        .flatten()
    {
        if value.len() >= 10 {
            let bytes = value.as_bytes();
            if bytes[0..4].iter().all(u8::is_ascii_digit)
                && bytes[4] == b'-'
                && bytes[5..7].iter().all(u8::is_ascii_digit)
                && bytes[7] == b'-'
                && bytes[8..10].iter().all(u8::is_ascii_digit)
            {
                return format!(
                    "{}{}{}",
                    &value[2..4],
                    &value[5..7],
                    &value[8..10]
                );
            }
        }
    }
    if let Some(raw_suffix) = &target.raw_suffix {
        if raw_suffix.len() >= 8
            && raw_suffix.as_bytes()[0..8].iter().all(u8::is_ascii_digit)
        {
            return format!(
                "{}{}{}",
                &raw_suffix[2..4],
                &raw_suffix[4..6],
                &raw_suffix[6..8]
            );
        }
    }
    "000101".to_string()
}

fn base_for_dismissal(target: &AgentCleanupTargetWire) -> String {
    if let Some(name) = &target.agent_name {
        if !name.is_empty() {
            return strip_dismissed_prefix(name).to_string();
        }
    }
    if target.identity.cl_name != "unknown"
        && !target.identity.cl_name.is_empty()
    {
        return target.identity.cl_name.clone();
    }
    if let Some(raw_suffix) = &target.raw_suffix {
        if !raw_suffix.is_empty() {
            return raw_suffix.clone();
        }
    }
    "agent".to_string()
}

fn add_dismissed_prefix(name: &str, date_prefix: &str) -> String {
    if is_dismissed_prefixed(name) {
        name.to_string()
    } else {
        format!("{date_prefix}.{name}")
    }
}

fn allocate_dismissed_name(
    base: &str,
    date_prefix: &str,
    taken: &mut BTreeSet<String>,
) -> String {
    let stripped = strip_dismissed_prefix(base);
    let primary = add_dismissed_prefix(stripped, date_prefix);
    if !taken.contains(&primary) {
        taken.insert(primary.clone());
        return primary;
    }
    let mut n = 2;
    loop {
        let candidate = format!("{primary}.{n}");
        if !taken.contains(&candidate) {
            taken.insert(candidate.clone());
            return candidate;
        }
        n += 1;
    }
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
    if target.from_changespec {
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
            },
        );
        return;
    }
    if kind == KILL_KIND_WORKFLOW {
        let workflow_name = if target.is_workflow_child {
            target
                .parent_workflow
                .clone()
                .or_else(|| target.workflow.clone())
        } else {
            target.workflow.clone()
        };
        let Some(workflow_name) = workflow_name else {
            return;
        };
        let lookup_cl = if !target.is_workflow_child
            && target.identity.cl_name != "unknown"
        {
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
            },
        );
    }
}

fn related_workflow_targets<'a>(
    target: &'a AgentCleanupTargetWire,
    children_by_parent: &BTreeMap<
        (String, Option<String>),
        Vec<&'a AgentCleanupTargetWire>,
    >,
) -> Vec<&'a AgentCleanupTargetWire> {
    let mut related = vec![target];
    if target.agent_type == "workflow" && !is_workflow_child(target) {
        if let Some(raw_suffix) = &target.raw_suffix {
            let key = (raw_suffix.clone(), target.workflow.clone());
            if let Some(children) = children_by_parent.get(&key) {
                related.extend(children.iter().copied());
            }
        }
    }
    related
}

fn add_dismissal_renames(
    side_effects: &mut AgentCleanupSideEffectsWire,
    related: &[&AgentCleanupTargetWire],
    taken_names: &mut BTreeSet<String>,
) {
    let Some(parent) = related.first() else {
        return;
    };
    let date_prefix = completion_date_prefix(parent);
    let old_name = parent.agent_name.clone();
    let base = base_for_dismissal(parent);
    let new_name = allocate_dismissed_name(&base, &date_prefix, taken_names);
    side_effects.dismissal_rename_allocations.push(
        AgentCleanupDismissalRenameIntentWire {
            identity: parent.identity.clone(),
            old_name: old_name.clone(),
            new_name: new_name.clone(),
        },
    );
    if let Some(old) = old_name {
        if old != new_name {
            side_effects
                .wait_reference_rewrite_map
                .push((old, new_name));
        }
    }

    if parent.agent_type != "workflow" || is_workflow_child(parent) {
        return;
    }
    for child in related.iter().skip(1) {
        let Some(old_child_name) = &child.agent_name else {
            continue;
        };
        let new_child_name = add_dismissed_prefix(old_child_name, &date_prefix);
        if old_child_name == &new_child_name {
            continue;
        }
        side_effects.dismissal_rename_allocations.push(
            AgentCleanupDismissalRenameIntentWire {
                identity: child.identity.clone(),
                old_name: Some(old_child_name.clone()),
                new_name: new_child_name.clone(),
            },
        );
        side_effects
            .wait_reference_rewrite_map
            .push((old_child_name.clone(), new_child_name));
    }
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
    let mut taken_names: BTreeSet<String> =
        request.taken_dismissed_names.iter().cloned().collect();
    let mut seen_index = BTreeSet::new();
    let mut seen_bundle = BTreeSet::new();
    let mut seen_artifacts = BTreeSet::new();
    let mut seen_workspace = BTreeSet::new();
    let mut seen_notifications = BTreeSet::new();

    for dismiss in dismiss_items {
        let Some(target) = by_id.get(&dismiss.identity).copied() else {
            continue;
        };
        let related = related_workflow_targets(target, children_by_parent);
        add_dismissal_renames(&mut side_effects, &related, &mut taken_names);
        for item in related {
            add_index_identity(&mut side_effects, &mut seen_index, item);
            add_bundle_candidate(&mut side_effects, &mut seen_bundle, item);
            add_artifact_delete(&mut side_effects, &mut seen_artifacts, item);
            add_notification_candidate(
                &mut side_effects,
                &mut seen_notifications,
                item,
            );
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
    let parent_tags = parent_tags_by_suffix(targets);
    let children_by_parent = workflow_children_by_parent(targets);

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

        if !selected_by_scope(target, request, &selected_ids, &parent_tags) {
            add_skip(&mut skipped_items, target, SKIPPED_NOT_IN_SCOPE, None);
            continue;
        }

        if is_workflow_child(target) {
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

        selected.push(target.identity.clone());

        if request.mode == CLEANUP_MODE_PREVIEW_ONLY {
            add_skip(
                &mut skipped_items,
                target,
                SKIPPED_NOT_KILLABLE,
                Some("preview_only".to_string()),
            );
            continue;
        }

        let dismissable = is_dismissable_status(&target.status)
            || (request.include_pidless_as_dismissable && target.pid.is_none());
        let killable =
            target.pid.is_some() && !is_dismissable_status(&target.status);

        if request.mode == CLEANUP_MODE_DISMISS_COMPLETED {
            if dismissable {
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
            project_file: Some("/tmp/project.gp".to_string()),
            artifacts_dir: Some("/tmp/artifacts".to_string()),
            from_changespec: false,
            workspace: None,
            tag: None,
            agent_name: None,
            display_name: Some(cl_name.to_string()),
            start_time: None,
            stop_time: None,
            is_workflow_child: false,
            appears_as_agent: false,
            step_type: None,
        }
    }

    fn req(scope: &str, mode: &str) -> AgentCleanupRequestWire {
        AgentCleanupRequestWire {
            schema_version: AGENT_CLEANUP_WIRE_SCHEMA_VERSION,
            scope: scope.to_string(),
            mode: mode.to_string(),
            focused_panel_tag: None,
            tag: None,
            identities: vec![],
            include_pidless_as_dismissable: false,
            taken_dismissed_names: vec![],
        }
    }

    #[test]
    fn focused_panel_selects_matching_tag_and_dismisses_completed() {
        let mut untagged = target("run", "untagged", Some("1"), "DONE", None);
        untagged.tag = None;
        let mut tagged = target("run", "tagged", Some("2"), "DONE", None);
        tagged.tag = Some("ops".to_string());
        let mut request =
            req(CLEANUP_SCOPE_FOCUSED_PANEL, CLEANUP_MODE_DISMISS_COMPLETED);
        request.focused_panel_tag = Some("ops".to_string());

        let plan = plan_agent_cleanup(&[untagged, tagged], &request).unwrap();

        assert_eq!(plan.dismiss_items.len(), 1);
        assert_eq!(plan.dismiss_items[0].identity.cl_name, "tagged");
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
    fn tag_scope_uses_parent_tag_for_workflow_children_but_skips_child_directly(
    ) {
        let mut parent =
            target("workflow", "parent", Some("p1"), "RUNNING", Some(8));
        parent.workflow = Some("deploy".to_string());
        parent.tag = Some("ops".to_string());
        let mut child =
            target("workflow", "child", Some("c1"), "RUNNING", Some(9));
        child.parent_timestamp = Some("p1".to_string());
        child.parent_workflow = Some("deploy".to_string());
        child.is_workflow_child = true;
        let mut request = req(CLEANUP_SCOPE_TAG, CLEANUP_MODE_KILL_AND_DISMISS);
        request.tag = Some("ops".to_string());

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

        let plan = plan_agent_cleanup(&[parent, child], &request).unwrap();

        assert_eq!(plan.kill_items.len(), 1);
        assert_eq!(plan.cascaded_workflow_children.len(), 1);
        assert_eq!(
            plan.cascaded_workflow_children[0].raw_suffix.as_deref(),
            Some("child")
        );
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
    fn dismiss_side_effects_allocate_names_and_children() {
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

        assert_eq!(
            plan.side_effects.dismissal_rename_allocations[0].new_name,
            "260428.root"
        );
        assert_eq!(
            plan.side_effects.dismissal_rename_allocations[1].new_name,
            "260428.root.plan"
        );
        assert_eq!(
            plan.side_effects.wait_reference_rewrite_map,
            vec![
                ("root".to_string(), "260428.root".to_string()),
                ("root.plan".to_string(), "260428.root.plan".to_string())
            ]
        );
        assert_eq!(plan.side_effects.dismissed_index_additions.len(), 2);
        assert_eq!(plan.side_effects.bundle_save_candidates.len(), 2);
        assert_eq!(plan.side_effects.artifact_delete_paths.len(), 2);
    }

    #[test]
    fn dismissed_name_allocation_uses_taken_names() {
        let mut first =
            target("run", "cl_a", Some("20260428100000"), "DONE", None);
        first.agent_name = Some("foo".to_string());
        let mut second =
            target("run", "cl_b", Some("20260428110000"), "DONE", None);
        second.agent_name = Some("foo".to_string());
        let mut request =
            req(CLEANUP_SCOPE_ALL_PANELS, CLEANUP_MODE_DISMISS_COMPLETED);
        request.taken_dismissed_names = vec!["260428.foo".to_string()];

        let plan = plan_agent_cleanup(&[first, second], &request).unwrap();

        let names: Vec<String> = plan
            .side_effects
            .dismissal_rename_allocations
            .iter()
            .map(|intent| intent.new_name.clone())
            .collect();
        assert_eq!(names, vec!["260428.foo.2", "260428.foo.3"]);
    }
}
