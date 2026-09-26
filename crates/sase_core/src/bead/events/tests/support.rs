//! Shared bead event test fixtures: issue builders used by every test file.

use std::collections::BTreeMap;

use crate::bead::wire::{BeadTierWire, IssueTypeWire, IssueWire, StatusWire};

pub(super) fn issue_with_refs(refs: Vec<String>) -> IssueWire {
    IssueWire {
        id: "sase-1".to_string(),
        title: "Plan".to_string(),
        status: StatusWire::Open,
        issue_type: IssueTypeWire::Plan,
        tier: Some(BeadTierWire::Epic),
        parent_id: None,
        owner: "owner@example.com".to_string(),
        assignee: String::new(),
        created_at: "2026-01-01T00:00:00Z".to_string(),
        created_by: "owner@example.com".to_string(),
        updated_at: "2026-01-01T00:00:00Z".to_string(),
        closed_at: None,
        close_reason: None,
        resolution: None,
        close_history: Vec::new(),
        description: String::new(),
        notes: Vec::new(),
        design: String::new(),
        refs,
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
