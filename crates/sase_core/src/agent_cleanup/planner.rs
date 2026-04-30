//! Pure agent cleanup planner.
//!
//! This mirrors the deterministic partitioning used by the Python Agents
//! tab: classify kill side effects, select by scope, cascade workflow-parent
//! decisions to child rows, and return skipped reasons. No processes are
//! signalled and no files are read or written here.

use std::collections::{BTreeMap, BTreeSet};

use super::wire::{
    AgentCleanupCountsWire, AgentCleanupDismissItemWire,
    AgentCleanupIdentityWire, AgentCleanupKillItemWire, AgentCleanupPlanWire,
    AgentCleanupRequestWire, AgentCleanupSkippedItemWire,
    AgentCleanupTargetWire, AGENT_CLEANUP_WIRE_SCHEMA_VERSION,
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
}
