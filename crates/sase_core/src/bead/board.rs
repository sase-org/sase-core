//! One-read board snapshot for the TUI Beads pane.
//!
//! `load_project_beads` used to serve the board with three full store reads
//! (`list_issues`, `ready`, `blocked`). This module reads the store once and
//! derives all three views from that single read, reusing the same
//! `*_in_issues` helpers the separate queries use so ready/blocked semantics
//! cannot drift between the board and the individual queries.

use std::path::Path;

use serde::{Deserialize, Serialize};

use super::read::{
    blocked_issues_in_issues, list_issues_in_issues, read_store_issues,
    ready_issues_in_issues,
};
use super::wire::{BeadError, IssueWire};

/// Wire schema version for [`BeadBoardSnapshotWire`].
pub const BEAD_BOARD_SNAPSHOT_WIRE_SCHEMA_VERSION: u32 = 1;

/// The TUI board views served from a single store read.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadBoardSnapshotWire {
    /// Schema version of this wire type.
    pub schema_version: u32,
    /// Every issue, exactly as `list_issues` with no filters returns them.
    pub issues: Vec<IssueWire>,
    /// IDs of `ready_issues`, in the same order.
    pub ready_ids: Vec<String>,
    /// IDs of `blocked_issues`, in the same order.
    pub blocked_ids: Vec<String>,
}

/// Read the store once and return the board views.
///
/// The two `Vec` clones below reuse the owned-`Vec` query helpers so the
/// board shares their semantics by construction. Each clone is bounded by
/// the live issue count and is dwarfed by the two full replays this
/// replaces (about a second on today's store); the parse-once work removes
/// replay cost itself, not these clones.
pub fn board_snapshot(
    beads_dir: &Path,
) -> Result<BeadBoardSnapshotWire, BeadError> {
    // Indexed lane first: the list plus ready/blocked IDs resolve from
    // the same indexed lanes the separate queries use, so the board keeps
    // matching them exactly without replaying history.
    if let Some(cached) = super::read_model::cached_board(beads_dir)? {
        return cached.map(|view| BeadBoardSnapshotWire {
            schema_version: BEAD_BOARD_SNAPSHOT_WIRE_SCHEMA_VERSION,
            issues: view.issues,
            ready_ids: view.ready_ids,
            blocked_ids: view.blocked_ids,
        });
    }
    board_snapshot_in_issues(read_store_issues(beads_dir)?)
}

/// Derive the board views from one already-read issue list.
pub(crate) fn board_snapshot_in_issues(
    issues: Vec<IssueWire>,
) -> Result<BeadBoardSnapshotWire, BeadError> {
    let ready_ids = ready_issues_in_issues(issues.clone())?
        .into_iter()
        .map(|issue| issue.id)
        .collect();
    let blocked_ids = blocked_issues_in_issues(issues.clone())?
        .into_iter()
        .map(|issue| issue.id)
        .collect();
    let issues = list_issues_in_issues(issues, None, None, None, None, None)?;
    Ok(BeadBoardSnapshotWire {
        schema_version: BEAD_BOARD_SNAPSHOT_WIRE_SCHEMA_VERSION,
        issues,
        ready_ids,
        blocked_ids,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bead::wire::{DependencyWire, IssueTypeWire, StatusWire};
    use std::collections::BTreeMap;

    fn issue(
        id: &str,
        issue_type: IssueTypeWire,
        status: StatusWire,
    ) -> IssueWire {
        IssueWire {
            id: id.to_string(),
            title: id.to_string(),
            status,
            issue_type,
            tier: None,
            parent_id: None,
            owner: String::new(),
            assignee: String::new(),
            created_at: String::new(),
            created_by: String::new(),
            updated_at: String::new(),
            closed_at: None,
            close_reason: None,
            resolution: None,
            close_history: Vec::new(),
            description: String::new(),
            notes: Vec::new(),
            design: String::new(),
            refs: Vec::new(),
            links: Vec::new(),
            plus_one_evidence: Vec::new(),
            snooze: None,
            model: String::new(),
            size: None,
            task_type: None,
            task_type_fields: BTreeMap::new(),
            is_ready_to_work: false,
            changespec_name: String::new(),
            changespec_bug_id: String::new(),
            external_ref: String::new(),
            creation_reason: String::new(),
            dependencies: Vec::new(),
        }
    }

    #[test]
    fn board_snapshot_matches_separate_queries() {
        let blocker = issue("blocker", IssueTypeWire::Task, StatusWire::Ready);
        let mut waiting =
            issue("waiting", IssueTypeWire::Task, StatusWire::Ready);
        waiting.dependencies.push(DependencyWire {
            issue_id: waiting.id.clone(),
            depends_on_id: blocker.id.clone(),
            created_at: String::new(),
            created_by: String::new(),
        });
        let epic = issue("epic", IssueTypeWire::Plan, StatusWire::Open);
        let open_task = issue("draft", IssueTypeWire::Task, StatusWire::Open);
        let issues = vec![
            blocker.clone(),
            waiting.clone(),
            epic.clone(),
            open_task.clone(),
        ];

        let snapshot = board_snapshot_in_issues(issues.clone()).unwrap();

        assert_eq!(
            snapshot.schema_version,
            BEAD_BOARD_SNAPSHOT_WIRE_SCHEMA_VERSION
        );
        assert_eq!(
            snapshot.issues,
            list_issues_in_issues(issues.clone(), None, None, None, None, None)
                .unwrap()
        );
        assert_eq!(
            snapshot.ready_ids,
            ready_issues_in_issues(issues.clone())
                .unwrap()
                .iter()
                .map(|issue| issue.id.clone())
                .collect::<Vec<_>>()
        );
        assert_eq!(
            snapshot.blocked_ids,
            blocked_issues_in_issues(issues)
                .unwrap()
                .iter()
                .map(|issue| issue.id.clone())
                .collect::<Vec<_>>()
        );
        assert_eq!(snapshot.ready_ids, vec!["blocker".to_string()]);
        assert_eq!(snapshot.blocked_ids, vec!["waiting".to_string()]);
    }
}
