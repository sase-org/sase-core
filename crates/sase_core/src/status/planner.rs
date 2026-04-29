//! Pure decision engine for the status state machine.
//!
//! Mirrors `plan_status_transition_python` in
//! `sase_100/src/sase/core/status_wire_conversion.py` branch-for-branch.
//! Side effects (file writes, archive moves, timestamps, mentor flags,
//! suffix renames, VCS) stay in Python — this function only decides what
//! the host should do.

use std::collections::HashSet;

use crate::query::matchers::strip_reverted_suffix;

use super::constants::{
    is_valid_transition, remove_workspace_suffix, valid_transitions_from,
    ARCHIVE_STATUSES,
};
use super::name::{get_next_suffix_number, has_suffix};
use super::wire::{
    StatusTransitionPlanWire, StatusTransitionRequestWire,
    ARCHIVE_ACTION_FROM_ARCHIVE, ARCHIVE_ACTION_NONE,
    ARCHIVE_ACTION_TO_ARCHIVE, MENTOR_ACTION_CLEAR, MENTOR_ACTION_NONE,
    MENTOR_ACTION_SET, STATUS_WIRE_SCHEMA_VERSION, SUFFIX_ACTION_APPEND,
    SUFFIX_ACTION_NONE, SUFFIX_ACTION_STRIP,
};

/// Reproduce the `transition_changespec_status_python` rejection string.
/// Format must be byte-for-byte identical to
/// `_format_invalid_transition_error` in `status_wire_conversion.py`.
fn format_invalid_transition_error(
    changespec_name: &str,
    old_status: &str,
    new_status: &str,
) -> String {
    let allowed = valid_transitions_from(old_status);
    // Match Python's list repr: `['Draft', 'Ready']` / `[]`.
    let allowed_repr = if allowed.is_empty() {
        "[]".to_string()
    } else {
        let inner = allowed
            .iter()
            .map(|s| format!("'{s}'"))
            .collect::<Vec<_>>()
            .join(", ");
        format!("[{inner}]")
    };
    format!(
        "Invalid status transition for '{changespec_name}': '{old_status}' -> \
         '{new_status}'. Allowed transitions from '{old_status}': {allowed_repr}"
    )
}

/// Mirrors `_classify_archive_action` in `status_wire_conversion.py` —
/// preserves the literal `old_status in ARCHIVE_STATUSES` compare on the
/// raw value (workspace suffix included).
fn classify_archive_action(
    old_status: Option<&str>,
    new_status: &str,
) -> &'static str {
    let old_is_archive = old_status
        .map(|s| ARCHIVE_STATUSES.contains(&s))
        .unwrap_or(false);
    let new_is_archive = ARCHIVE_STATUSES.contains(&new_status);
    if new_is_archive == old_is_archive {
        ARCHIVE_ACTION_NONE
    } else if new_is_archive {
        ARCHIVE_ACTION_TO_ARCHIVE
    } else {
        ARCHIVE_ACTION_FROM_ARCHIVE
    }
}

fn failure_plan(
    request: &StatusTransitionRequestWire,
    error: String,
) -> StatusTransitionPlanWire {
    StatusTransitionPlanWire {
        schema_version: STATUS_WIRE_SCHEMA_VERSION,
        success: false,
        old_status: Some(request.old_status.clone()),
        error: Some(error),
        status_update_target: None,
        suffix_action: SUFFIX_ACTION_NONE.into(),
        suffixed_name: None,
        base_name: None,
        mentor_draft_action: MENTOR_ACTION_NONE.into(),
        archive_action: ARCHIVE_ACTION_NONE.into(),
        timestamp_event: None,
        timestamp_target_name: None,
        revert_siblings: false,
    }
}

/// Pure Rust implementation of the status transition planner. Mirrors
/// `plan_status_transition_python` in `status_wire_conversion.py`
/// branch-for-branch.
///
/// Returns either a success plan describing the side effects the host
/// should perform, or a failure plan whose `error` matches the verbatim
/// Python rejection string.
///
/// The function is deterministic and does **no** I/O.
pub fn plan_status_transition(
    request: &StatusTransitionRequestWire,
) -> Result<StatusTransitionPlanWire, String> {
    if request.schema_version != STATUS_WIRE_SCHEMA_VERSION {
        return Err(format!(
            "status wire schema mismatch: got {}, expected {}",
            request.schema_version, STATUS_WIRE_SCHEMA_VERSION
        ));
    }

    let changespec_name = request.changespec_name.as_str();
    let old_status = request.old_status.as_str();
    let new_status = request.new_status.as_str();
    let validate = request.validate;
    let base_old_status = remove_workspace_suffix(old_status);

    // Branch: Ready -> Draft (suffix append + mentor set)
    if new_status == "Draft" && base_old_status == "Ready" {
        if !request.blocking_children.is_empty() {
            let child_info = request
                .blocking_children
                .iter()
                .map(|c| format!("{} ({})", c.name, c.status))
                .collect::<Vec<_>>()
                .join(", ");
            return Ok(failure_plan(
                request,
                format!(
                    "Cannot transition '{changespec_name}' to Draft: \
                     children must be WIP, Draft, or Reverted. \
                     Invalid children: {child_info}"
                ),
            ));
        }
        if validate && !is_valid_transition(old_status, new_status) {
            return Ok(failure_plan(
                request,
                format_invalid_transition_error(
                    changespec_name,
                    old_status,
                    new_status,
                ),
            ));
        }
        let existing: HashSet<String> =
            request.existing_names.iter().cloned().collect();
        let suffix_num = get_next_suffix_number(changespec_name, &existing);
        let suffixed_name = format!("{changespec_name}_{suffix_num}");
        return Ok(StatusTransitionPlanWire {
            schema_version: STATUS_WIRE_SCHEMA_VERSION,
            success: true,
            old_status: Some(old_status.to_string()),
            error: None,
            status_update_target: Some(new_status.to_string()),
            suffix_action: SUFFIX_ACTION_APPEND.into(),
            suffixed_name: Some(suffixed_name.clone()),
            base_name: Some(changespec_name.to_string()),
            mentor_draft_action: MENTOR_ACTION_SET.into(),
            archive_action: classify_archive_action(
                Some(old_status),
                new_status,
            )
            .into(),
            timestamp_event: Some(format!("{old_status} -> {new_status}")),
            timestamp_target_name: Some(suffixed_name),
            revert_siblings: false,
        });
    }

    // Branch: WIP -> Draft (no suffix, no mentor)
    if new_status == "Draft" && base_old_status == "WIP" {
        if validate && !is_valid_transition(old_status, new_status) {
            return Ok(failure_plan(
                request,
                format_invalid_transition_error(
                    changespec_name,
                    old_status,
                    new_status,
                ),
            ));
        }
        return Ok(StatusTransitionPlanWire {
            schema_version: STATUS_WIRE_SCHEMA_VERSION,
            success: true,
            old_status: Some(old_status.to_string()),
            error: None,
            status_update_target: Some(new_status.to_string()),
            suffix_action: SUFFIX_ACTION_NONE.into(),
            suffixed_name: None,
            base_name: None,
            mentor_draft_action: MENTOR_ACTION_NONE.into(),
            archive_action: classify_archive_action(
                Some(old_status),
                new_status,
            )
            .into(),
            timestamp_event: Some(format!("{old_status} -> {new_status}")),
            timestamp_target_name: Some(changespec_name.to_string()),
            revert_siblings: false,
        });
    }

    // Branch: Reverted (terminal) — validate only
    if new_status == "Reverted" {
        if validate && !is_valid_transition(old_status, new_status) {
            return Ok(failure_plan(
                request,
                format_invalid_transition_error(
                    changespec_name,
                    old_status,
                    new_status,
                ),
            ));
        }
        return Ok(StatusTransitionPlanWire {
            schema_version: STATUS_WIRE_SCHEMA_VERSION,
            success: true,
            old_status: Some(old_status.to_string()),
            error: None,
            status_update_target: Some(new_status.to_string()),
            suffix_action: SUFFIX_ACTION_NONE.into(),
            suffixed_name: None,
            base_name: None,
            mentor_draft_action: MENTOR_ACTION_NONE.into(),
            archive_action: classify_archive_action(
                Some(old_status),
                new_status,
            )
            .into(),
            timestamp_event: Some(format!("{old_status} -> {new_status}")),
            timestamp_target_name: Some(changespec_name.to_string()),
            revert_siblings: false,
        });
    }

    // Branch: Archived (terminal) — validate only
    if new_status == "Archived" {
        if validate && !is_valid_transition(old_status, new_status) {
            return Ok(failure_plan(
                request,
                format_invalid_transition_error(
                    changespec_name,
                    old_status,
                    new_status,
                ),
            ));
        }
        return Ok(StatusTransitionPlanWire {
            schema_version: STATUS_WIRE_SCHEMA_VERSION,
            success: true,
            old_status: Some(old_status.to_string()),
            error: None,
            status_update_target: Some(new_status.to_string()),
            suffix_action: SUFFIX_ACTION_NONE.into(),
            suffixed_name: None,
            base_name: None,
            mentor_draft_action: MENTOR_ACTION_NONE.into(),
            archive_action: classify_archive_action(
                Some(old_status),
                new_status,
            )
            .into(),
            timestamp_event: Some(format!("{old_status} -> {new_status}")),
            timestamp_target_name: Some(changespec_name.to_string()),
            revert_siblings: false,
        });
    }

    // Generic "ready" branch: covers new_status=Ready, Mailed, Submitted, ...

    // 1) Parent constraint: if in-project parent is WIP/Draft, the child can
    //    only become WIP/Draft/Reverted.
    let parent_blocks =
        matches!(
            request.parent_status.as_deref(),
            Some("WIP") | Some("Draft")
        ) && !matches!(new_status, "WIP" | "Draft" | "Reverted");
    if parent_blocks {
        let parent_status = request
            .parent_status
            .as_deref()
            .expect("parent_status checked above");
        return Ok(failure_plan(
            request,
            format!(
                "Cannot transition '{changespec_name}' to {new_status}: \
                 parent is {parent_status}. \
                 Children of WIP/Draft ChangeSpecs must be WIP, Draft, or Reverted."
            ),
        ));
    }

    // 2) Validate the transition itself.
    if validate && !is_valid_transition(old_status, new_status) {
        return Ok(failure_plan(
            request,
            format_invalid_transition_error(
                changespec_name,
                old_status,
                new_status,
            ),
        ));
    }

    // 3) Sibling-unreverted-children check on WIP/Draft -> Ready with suffix.
    if new_status == "Ready"
        && (base_old_status == "WIP" || base_old_status == "Draft")
        && has_suffix(changespec_name)
        && !request.siblings_with_unreverted_children.is_empty()
    {
        let sibling = &request.siblings_with_unreverted_children[0];
        return Ok(failure_plan(
            request,
            format!(
                "Cannot transition '{changespec_name}' to Ready: \
                 sibling ChangeSpec '{sibling}' has unreverted children."
            ),
        ));
    }

    // Suffix strip on WIP/Draft -> Ready with suffix.
    let mut suffix_action: &str = SUFFIX_ACTION_NONE;
    let mut strip_suffixed_name: Option<String> = None;
    let mut strip_base_name: Option<String> = None;
    let mut mentor_draft_action: &str = MENTOR_ACTION_NONE;
    let mut revert_siblings = false;
    if new_status == "Ready"
        && (base_old_status == "WIP" || base_old_status == "Draft")
        && has_suffix(changespec_name)
    {
        suffix_action = SUFFIX_ACTION_STRIP;
        strip_suffixed_name = Some(changespec_name.to_string());
        strip_base_name = Some(strip_reverted_suffix(changespec_name));
        revert_siblings = true;
    }

    // Mentor flag clear on Draft -> Ready (independent of suffix).
    if base_old_status == "Draft" && new_status == "Ready" {
        mentor_draft_action = MENTOR_ACTION_CLEAR;
    }

    // Post-rename name for the timestamp event.
    let timestamp_target_name = if suffix_action == SUFFIX_ACTION_STRIP {
        strip_base_name.clone()
    } else {
        Some(changespec_name.to_string())
    };

    Ok(StatusTransitionPlanWire {
        schema_version: STATUS_WIRE_SCHEMA_VERSION,
        success: true,
        old_status: Some(old_status.to_string()),
        error: None,
        status_update_target: Some(new_status.to_string()),
        suffix_action: suffix_action.into(),
        suffixed_name: strip_suffixed_name,
        base_name: strip_base_name,
        mentor_draft_action: mentor_draft_action.into(),
        archive_action: classify_archive_action(Some(old_status), new_status)
            .into(),
        timestamp_event: Some(format!("{old_status} -> {new_status}")),
        timestamp_target_name,
        revert_siblings,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::status::wire::ChangespecChildWire;

    fn req(
        changespec_name: &str,
        old_status: &str,
        new_status: &str,
    ) -> StatusTransitionRequestWire {
        StatusTransitionRequestWire {
            schema_version: STATUS_WIRE_SCHEMA_VERSION,
            changespec_name: changespec_name.into(),
            old_status: old_status.into(),
            new_status: new_status.into(),
            validate: true,
            parent_status: None,
            blocking_children: vec![],
            siblings_with_unreverted_children: vec![],
            existing_names: vec![],
        }
    }

    #[test]
    fn invalid_transition_validate_true_rejects() {
        let plan = plan_status_transition(&req("proj_x", "Ready", "Submitted"))
            .unwrap();
        assert!(!plan.success);
        let err = plan.error.unwrap();
        assert!(err.contains("Invalid status transition"));
        assert!(err.contains("'Ready' -> 'Submitted'"));
        assert_eq!(plan.old_status.as_deref(), Some("Ready"));
        assert!(plan.status_update_target.is_none());
    }

    #[test]
    fn invalid_transition_validate_false_allows() {
        let mut r = req("proj_x", "Ready", "Submitted");
        r.validate = false;
        let plan = plan_status_transition(&r).unwrap();
        assert!(plan.success);
        assert_eq!(plan.status_update_target.as_deref(), Some("Submitted"));
    }

    #[test]
    fn unknown_status_rejected_under_validation() {
        let plan =
            plan_status_transition(&req("proj_x", "Bogus", "Mailed")).unwrap();
        assert!(!plan.success);
    }

    #[test]
    fn workspace_suffix_normalised_for_validation() {
        let plan = plan_status_transition(&req(
            "proj_thing",
            "Ready (proj_2)",
            "Mailed",
        ))
        .unwrap();
        assert!(plan.success);
        assert_eq!(plan.status_update_target.as_deref(), Some("Mailed"));
        // Plan echoes the raw old_status verbatim.
        assert_eq!(plan.old_status.as_deref(), Some("Ready (proj_2)"));
        // Timestamp uses the raw old_status.
        assert_eq!(
            plan.timestamp_event.as_deref(),
            Some("Ready (proj_2) -> Mailed")
        );
    }

    #[test]
    fn legacy_ready_to_mail_suffix_stripped_for_validation() {
        let plan = plan_status_transition(&req(
            "proj_thing",
            "Ready - (!: READY TO MAIL)",
            "Mailed",
        ))
        .unwrap();
        assert!(plan.success);
        assert_eq!(plan.status_update_target.as_deref(), Some("Mailed"));
    }

    #[test]
    fn wip_to_draft_no_suffix_no_mentor() {
        let plan = plan_status_transition(&req("proj_feature", "WIP", "Draft"))
            .unwrap();
        assert!(plan.success);
        assert_eq!(plan.status_update_target.as_deref(), Some("Draft"));
        assert_eq!(plan.suffix_action, SUFFIX_ACTION_NONE);
        assert_eq!(plan.mentor_draft_action, MENTOR_ACTION_NONE);
        assert_eq!(plan.archive_action, ARCHIVE_ACTION_NONE);
        assert_eq!(plan.timestamp_event.as_deref(), Some("WIP -> Draft"));
        assert_eq!(plan.timestamp_target_name.as_deref(), Some("proj_feature"));
    }

    #[test]
    fn ready_to_draft_appends_suffix_and_sets_mentor() {
        let mut r = req("proj_feature", "Ready", "Draft");
        r.existing_names = vec!["proj_feature".into()];
        let plan = plan_status_transition(&r).unwrap();
        assert!(plan.success);
        assert_eq!(plan.suffix_action, SUFFIX_ACTION_APPEND);
        assert_eq!(plan.suffixed_name.as_deref(), Some("proj_feature_1"));
        assert_eq!(plan.base_name.as_deref(), Some("proj_feature"));
        assert_eq!(plan.mentor_draft_action, MENTOR_ACTION_SET);
        assert_eq!(plan.timestamp_event.as_deref(), Some("Ready -> Draft"));
        assert_eq!(
            plan.timestamp_target_name.as_deref(),
            Some("proj_feature_1")
        );
    }

    #[test]
    fn ready_to_draft_picks_lowest_free_suffix() {
        let mut r = req("proj_feature", "Ready", "Draft");
        r.existing_names = vec![
            "proj_feature".into(),
            "proj_feature_1".into(),
            "proj_feature_2".into(),
            "proj_feature__3".into(),
        ];
        let plan = plan_status_transition(&r).unwrap();
        assert!(plan.success);
        assert_eq!(plan.suffixed_name.as_deref(), Some("proj_feature_4"));
    }

    #[test]
    fn ready_to_draft_blocked_by_invalid_children() {
        let mut r = req("proj_feature", "Ready", "Draft");
        r.blocking_children = vec![
            ChangespecChildWire {
                name: "proj_child_a".into(),
                status: "Mailed".into(),
            },
            ChangespecChildWire {
                name: "proj_child_b".into(),
                status: "Submitted".into(),
            },
        ];
        let plan = plan_status_transition(&r).unwrap();
        assert!(!plan.success);
        let err = plan.error.unwrap();
        assert!(err.contains("children must be WIP, Draft, or Reverted"));
        assert!(err.contains("proj_child_a (Mailed)"));
        assert!(err.contains("proj_child_b (Submitted)"));
    }

    #[test]
    fn wip_to_ready_with_suffix_strips() {
        let plan =
            plan_status_transition(&req("proj_feature_2", "WIP", "Ready"))
                .unwrap();
        assert!(plan.success);
        assert_eq!(plan.suffix_action, SUFFIX_ACTION_STRIP);
        assert_eq!(plan.suffixed_name.as_deref(), Some("proj_feature_2"));
        assert_eq!(plan.base_name.as_deref(), Some("proj_feature"));
        assert!(plan.revert_siblings);
        // WIP -> Ready does not touch mentor draft flags.
        assert_eq!(plan.mentor_draft_action, MENTOR_ACTION_NONE);
        assert_eq!(plan.timestamp_target_name.as_deref(), Some("proj_feature"));
    }

    #[test]
    fn draft_to_ready_with_suffix_strips_and_clears_mentors() {
        let plan =
            plan_status_transition(&req("proj_feature_3", "Draft", "Ready"))
                .unwrap();
        assert!(plan.success);
        assert_eq!(plan.suffix_action, SUFFIX_ACTION_STRIP);
        assert_eq!(plan.base_name.as_deref(), Some("proj_feature"));
        assert_eq!(plan.mentor_draft_action, MENTOR_ACTION_CLEAR);
        assert!(plan.revert_siblings);
    }

    #[test]
    fn draft_to_ready_no_suffix_clears_mentors_only() {
        let plan =
            plan_status_transition(&req("proj_feature", "Draft", "Ready"))
                .unwrap();
        assert!(plan.success);
        assert_eq!(plan.suffix_action, SUFFIX_ACTION_NONE);
        assert_eq!(plan.mentor_draft_action, MENTOR_ACTION_CLEAR);
        assert!(!plan.revert_siblings);
    }

    #[test]
    fn wip_to_ready_blocked_by_sibling_unreverted_children() {
        let mut r = req("proj_feature_2", "WIP", "Ready");
        r.siblings_with_unreverted_children = vec!["proj_feature_1".into()];
        let plan = plan_status_transition(&r).unwrap();
        assert!(!plan.success);
        let err = plan.error.unwrap();
        assert!(err.contains("sibling ChangeSpec 'proj_feature_1'"));
        assert!(err.contains("unreverted children"));
    }

    #[test]
    fn parent_wip_blocks_child_to_mailed() {
        let mut r = req("proj_child", "Ready", "Mailed");
        r.parent_status = Some("WIP".into());
        let plan = plan_status_transition(&r).unwrap();
        assert!(!plan.success);
        let err = plan.error.unwrap();
        assert!(err.contains("parent is WIP"));
        assert!(err.contains("WIP, Draft, or Reverted"));
    }

    #[test]
    fn parent_constraint_skipped_for_reverted_branch() {
        let mut r = req("proj_child", "Ready", "Reverted");
        r.parent_status = Some("WIP".into());
        r.validate = false;
        let plan = plan_status_transition(&r).unwrap();
        assert!(plan.success);
        assert_eq!(plan.status_update_target.as_deref(), Some("Reverted"));
    }

    #[test]
    fn parent_ready_does_not_block_mailed() {
        let mut r = req("proj_child", "Ready", "Mailed");
        r.parent_status = Some("Ready".into());
        let plan = plan_status_transition(&r).unwrap();
        assert!(plan.success);
    }

    #[test]
    fn reverted_terminal_no_further_transitions() {
        let plan =
            plan_status_transition(&req("x", "Reverted", "WIP")).unwrap();
        assert!(!plan.success);
        assert!(plan.error.unwrap().contains("Invalid status transition"));
    }

    #[test]
    fn archived_terminal_no_further_transitions() {
        let plan =
            plan_status_transition(&req("x", "Archived", "WIP")).unwrap();
        assert!(!plan.success);
    }

    #[test]
    fn submitted_terminal_no_further_transitions() {
        let plan =
            plan_status_transition(&req("x", "Submitted", "Mailed")).unwrap();
        assert!(!plan.success);
    }

    #[test]
    fn archive_action_to_archive_on_submitted() {
        let plan =
            plan_status_transition(&req("x", "Mailed", "Submitted")).unwrap();
        assert!(plan.success);
        assert_eq!(plan.archive_action, ARCHIVE_ACTION_TO_ARCHIVE);
    }

    #[test]
    fn archive_action_from_archive_under_no_validate() {
        let mut r = req("x", "Submitted", "Ready");
        r.validate = false;
        let plan = plan_status_transition(&r).unwrap();
        assert!(plan.success);
        assert_eq!(plan.archive_action, ARCHIVE_ACTION_FROM_ARCHIVE);
    }

    #[test]
    fn archive_action_none_within_main_class() {
        let plan = plan_status_transition(&req("x", "WIP", "Draft")).unwrap();
        assert!(plan.success);
        assert_eq!(plan.archive_action, ARCHIVE_ACTION_NONE);
    }

    #[test]
    fn archive_action_none_within_archive_class() {
        let mut r = req("x", "Submitted", "Archived");
        r.validate = false;
        let plan = plan_status_transition(&r).unwrap();
        assert!(plan.success);
        assert_eq!(plan.archive_action, ARCHIVE_ACTION_NONE);
    }

    #[test]
    fn schema_version_mismatch_returns_error() {
        let mut r = req("x", "WIP", "Draft");
        r.schema_version = STATUS_WIRE_SCHEMA_VERSION + 99;
        let err = plan_status_transition(&r).unwrap_err();
        assert!(err.contains("schema mismatch"));
    }

    #[test]
    fn invalid_transition_error_format_matches_python() {
        // Python format: "Invalid status transition for 'X': 'A' -> 'B'.
        // Allowed transitions from 'A': ['Draft', 'Ready']" (or `[]`).
        let plan =
            plan_status_transition(&req("proj_x", "Submitted", "Mailed"))
                .unwrap();
        assert!(!plan.success);
        let err = plan.error.unwrap();
        assert_eq!(
            err,
            "Invalid status transition for 'proj_x': 'Submitted' -> 'Mailed'. \
             Allowed transitions from 'Submitted': []"
        );

        let plan = plan_status_transition(&req("proj_x", "Ready", "Submitted"))
            .unwrap();
        let err = plan.error.unwrap();
        assert_eq!(
            err,
            "Invalid status transition for 'proj_x': 'Ready' -> 'Submitted'. \
             Allowed transitions from 'Ready': ['Mailed', 'Draft']"
        );
    }
}
