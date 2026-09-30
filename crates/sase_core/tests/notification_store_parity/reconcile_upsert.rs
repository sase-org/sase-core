//! Field-scoped reconcile writes and empty `raw_suffix` matcher parity.
//!
//! Covers the lock-held reconcile write the remote-attention reconciler uses:
//! lost-update interleaves against concurrent dismissals, resurfacing of
//! marker-dismissed rows, user dismissals staying dismissed without gaining a
//! marker, owned-field-only refreshes, no-op writes, creates, and treating an
//! empty notification `raw_suffix` like a missing key.

use super::support::*;
use sase_core::notifications::{
    apply_notification_state_update, read_notifications_snapshot,
    reconcile_notification_rows, rewrite_notifications,
    NotificationAgentKeyWire, NotificationReconcileRequestWire,
    NotificationStateUpdateWire, NotificationWire,
};
use std::fs;
use tempfile::tempdir;

const MARKER_KEY: &str = "test_auto_dismissed";

fn reconcile_request(
    rows: Vec<NotificationWire>,
) -> NotificationReconcileRequestWire {
    NotificationReconcileRequestWire {
        notifications: rows,
        reversible_dismiss_marker_key: Some(MARKER_KEY.to_string()),
    }
}

fn marked(row: &mut NotificationWire) {
    row.action_data
        .insert(MARKER_KEY.to_string(), "true".to_string());
}

fn completion_row(id: &str, raw_suffix: Option<&str>) -> NotificationWire {
    let mut row = notification(id);
    row.sender = "user-agent".to_string();
    row.action = Some("JumpToAgent".to_string());
    row.action_data
        .insert("cl_name".to_string(), "feature".to_string());
    if let Some(suffix) = raw_suffix {
        row.action_data
            .insert("raw_suffix".to_string(), suffix.to_string());
    }
    row
}

fn agent_key(raw_suffix: Option<&str>) -> NotificationAgentKeyWire {
    NotificationAgentKeyWire {
        cl_name: "feature".to_string(),
        raw_suffix: raw_suffix.map(str::to_string),
    }
}

#[test]
fn reconcile_preserves_concurrent_dismissals_of_stale_rows() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    let completion = completion_row("completion", Some("20260501010203"));
    let mut remote = notification("remote-1");
    remote.notes = vec!["old notes".to_string()];
    rewrite_notifications(&path, &[completion, remote]).unwrap();

    // Stale snapshot taken before the concurrent writer runs.
    let stale = read_notifications_snapshot(&path, true)
        .unwrap()
        .notifications;

    // Another writer dismisses both rows between the snapshot and the write.
    for id in ["completion", "remote-1"] {
        apply_notification_state_update(
            &path,
            &NotificationStateUpdateWire::MarkDismissed { id: id.to_string() },
        )
        .unwrap();
    }

    // The reconciler only sends the row it refreshed, with stale state.
    let mut refreshed = stale
        .iter()
        .find(|row| row.id == "remote-1")
        .unwrap()
        .clone();
    refreshed.notes = vec!["new notes".to_string()];
    let outcome =
        reconcile_notification_rows(&path, &reconcile_request(vec![refreshed]))
            .unwrap();
    assert_eq!(outcome.created, 0);
    assert_eq!(outcome.dismissed, 0);
    assert_eq!(outcome.resurfaced, 0);

    let rows = read_notifications_snapshot(&path, true)
        .unwrap()
        .notifications;
    let by_id: std::collections::HashMap<_, _> =
        rows.iter().map(|row| (row.id.as_str(), row)).collect();
    // The completion row was never in the input: untouched.
    assert!(by_id["completion"].dismissed);
    // The stale remote copy cannot clobber the user dismissal, gains no
    // marker, but still picks up the refreshed owned fields.
    assert!(by_id["remote-1"].dismissed);
    assert_eq!(by_id["remote-1"].action_data.get(MARKER_KEY), None);
    assert_eq!(by_id["remote-1"].notes, vec!["new notes".to_string()]);
}

#[test]
fn reconcile_resurfaces_marker_dismissed_rows() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    let mut row = notification("remote-1");
    row.dismissed = true;
    row.read = true;
    marked(&mut row);
    rewrite_notifications(&path, &[row]).unwrap();

    let mut incoming = notification("remote-1");
    incoming.notes = vec!["still pending".to_string()];
    let outcome =
        reconcile_notification_rows(&path, &reconcile_request(vec![incoming]))
            .unwrap();
    assert_eq!(outcome.resurfaced, 1);
    assert_eq!(outcome.updated, 0);
    assert_eq!(outcome.dismissed, 0);

    let rows = read_notifications_snapshot(&path, true)
        .unwrap()
        .notifications;
    assert_eq!(rows.len(), 1);
    assert!(!rows[0].dismissed);
    assert!(!rows[0].read);
    // The marker drops because the input payload carries none.
    assert_eq!(rows[0].action_data.get(MARKER_KEY), None);
}

#[test]
fn reconcile_never_marks_a_user_dismissal_reversible() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    let mut row = notification("remote-1");
    row.dismissed = true;
    rewrite_notifications(&path, &[row]).unwrap();

    // Even a dismissed input carrying the marker cannot flip a user
    // dismissal into a reversible one.
    let mut incoming = notification("remote-1");
    incoming.dismissed = true;
    marked(&mut incoming);
    let outcome =
        reconcile_notification_rows(&path, &reconcile_request(vec![incoming]))
            .unwrap();
    assert_eq!(outcome.dismissed, 0);
    assert_eq!(outcome.resurfaced, 0);
    assert_eq!(outcome.updated, 0);

    let rows = read_notifications_snapshot(&path, true)
        .unwrap()
        .notifications;
    assert!(rows[0].dismissed);
    assert_eq!(rows[0].action_data.get(MARKER_KEY), None);
}

#[test]
fn reconcile_refresh_copies_only_owned_fields() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    let mut row = notification("remote-1");
    row.read = true;
    row.muted = true;
    row.snooze_until = Some("2026-06-01T00:00:00+00:00".to_string());
    row.files = vec!["plan.md".to_string()];
    row.dedup_key = Some("dedup".to_string());
    rewrite_notifications(&path, &[row]).unwrap();

    let mut incoming = notification("remote-1");
    incoming.timestamp = "1999-01-01T00:00:00+00:00".to_string();
    incoming.sender = "impostor".to_string();
    incoming.icon = Some("?".to_string());
    incoming.color = Some("#87D7FF".to_string());
    incoming.notes = vec!["new".to_string()];
    incoming.tags = vec!["attention".to_string()];
    incoming.action = Some("RemoteAttention".to_string());
    incoming
        .action_data
        .insert("k".to_string(), "v".to_string());
    incoming.silent = true;
    incoming.read = true;
    incoming.dismissed = true;
    incoming.muted = false;
    incoming.files = vec!["evil.md".to_string()];
    let outcome =
        reconcile_notification_rows(&path, &reconcile_request(vec![incoming]))
            .unwrap();
    assert_eq!(outcome.updated, 1);

    let rows = read_notifications_snapshot(&path, true)
        .unwrap()
        .notifications;
    assert_eq!(rows.len(), 1);
    let merged = &rows[0];
    assert_eq!(merged.icon, Some("?".to_string()));
    assert_eq!(merged.color, Some("#87D7FF".to_string()));
    assert_eq!(merged.notes, vec!["new".to_string()]);
    assert_eq!(merged.tags, vec!["attention".to_string()]);
    assert_eq!(merged.action, Some("RemoteAttention".to_string()));
    assert_eq!(merged.action_data.get("k"), Some(&"v".to_string()));
    assert!(merged.silent);
    // Everything outside the owned set stays on disk; a marker-less
    // dismissed input never dismisses.
    assert_eq!(merged.timestamp, "2026-05-01T01:02:03+00:00");
    assert_eq!(merged.sender, "test-sender");
    assert_eq!(merged.files, vec!["plan.md".to_string()]);
    assert!(merged.muted);
    assert_eq!(
        merged.snooze_until,
        Some("2026-06-01T00:00:00+00:00".to_string())
    );
    assert_eq!(merged.dedup_key, Some("dedup".to_string()));
    assert!(merged.read);
    assert!(!merged.dismissed);
}

#[test]
fn reconcile_auto_dismisses_with_the_marker() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    rewrite_notifications(&path, &[notification("remote-1")]).unwrap();

    let mut incoming = notification("remote-1");
    incoming.dismissed = true;
    marked(&mut incoming);
    let outcome =
        reconcile_notification_rows(&path, &reconcile_request(vec![incoming]))
            .unwrap();
    assert_eq!(outcome.dismissed, 1);
    assert_eq!(outcome.updated, 0);

    let rows = read_notifications_snapshot(&path, true)
        .unwrap()
        .notifications;
    assert!(rows[0].dismissed);
    assert_eq!(
        rows[0].action_data.get(MARKER_KEY),
        Some(&"true".to_string())
    );
}

#[test]
fn reconcile_appends_unknown_rows_and_skips_empty_ids() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    rewrite_notifications(&path, &[notification("existing")]).unwrap();

    let mut empty = notification("");
    empty.notes = vec!["no id".to_string()];
    let outcome = reconcile_notification_rows(
        &path,
        &reconcile_request(vec![notification("created"), empty]),
    )
    .unwrap();
    assert_eq!(outcome.created, 1);

    let rows = read_notifications_snapshot(&path, true)
        .unwrap()
        .notifications;
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().any(|row| row.id == "created"));
    assert!(rows.iter().any(|row| row.id == "existing"));
}

#[test]
fn reconcile_skips_the_file_write_when_nothing_changed() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    rewrite_notifications(
        &path,
        &[completion_row("completion", Some("20260501010203"))],
    )
    .unwrap();
    let before = fs::read(&path).unwrap();

    let unchanged = read_notifications_snapshot(&path, true)
        .unwrap()
        .notifications;
    let outcome =
        reconcile_notification_rows(&path, &reconcile_request(unchanged))
            .unwrap();
    assert_eq!(outcome.created, 0);
    assert_eq!(outcome.updated, 0);
    assert_eq!(outcome.dismissed, 0);
    assert_eq!(outcome.resurfaced, 0);
    assert_eq!(fs::read(&path).unwrap(), before);
}

#[test]
fn reconcile_without_a_marker_key_never_changes_dismissals() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    let mut row = notification("remote-1");
    row.dismissed = true;
    row.action_data
        .insert(MARKER_KEY.to_string(), "true".to_string());
    rewrite_notifications(&path, &[row]).unwrap();

    let request = NotificationReconcileRequestWire {
        notifications: vec![notification("remote-1")],
        reversible_dismiss_marker_key: None,
    };
    let outcome = reconcile_notification_rows(&path, &request).unwrap();
    assert_eq!(outcome.resurfaced, 0);
    assert_eq!(outcome.dismissed, 0);

    let rows = read_notifications_snapshot(&path, true)
        .unwrap()
        .notifications;
    assert!(rows[0].dismissed);
}

#[test]
fn empty_raw_suffix_matches_like_a_missing_key() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    let empty_suffix = completion_row("empty-suffix", Some(""));
    let missing_suffix = completion_row("missing-suffix", None);
    // A non-empty suffix on another row keeps exact matching working.
    let other_suffix = completion_row("other-suffix", Some("20260501010204"));
    let mut other_cl = completion_row("other-cl", Some(""));
    other_cl
        .action_data
        .insert("cl_name".to_string(), "elsewhere".to_string());
    // `matches_agent_notification` only treats ViewErrorReport from
    // user-agent the same way; cover that branch too.
    let mut error = completion_row("error-empty-suffix", Some(""));
    error.action = Some("ViewErrorReport".to_string());
    rewrite_notifications(
        &path,
        &[empty_suffix, missing_suffix, other_suffix, other_cl, error],
    )
    .unwrap();

    let outcome = apply_notification_state_update(
        &path,
        &NotificationStateUpdateWire::DismissAgentCompletionsMatchingAgents {
            agents: vec![agent_key(Some("20260501010203"))],
        },
    )
    .unwrap();
    assert_eq!(outcome.matched_count, 3);
    assert_eq!(outcome.changed_count, 3);
    let by_id: std::collections::HashMap<_, _> = outcome
        .notifications
        .iter()
        .map(|row| (row.id.as_str(), row.dismissed))
        .collect();
    assert!(by_id["empty-suffix"]);
    assert!(by_id["missing-suffix"]);
    assert!(by_id["error-empty-suffix"]);
    assert!(!by_id["other-suffix"]);
    assert!(!by_id["other-cl"]);
}

#[test]
fn empty_raw_suffix_matches_like_a_missing_key_for_matching_agents() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    // `matches_agent_notification` has no sender gate on JumpToAgent.
    rewrite_notifications(&path, &[completion_row("empty-suffix", Some(""))])
        .unwrap();

    let outcome = apply_notification_state_update(
        &path,
        &NotificationStateUpdateWire::DismissMatchingAgents {
            agents: vec![agent_key(Some("20260501010203"))],
        },
    )
    .unwrap();
    assert_eq!(outcome.changed_count, 1);
    assert!(outcome.notifications[0].dismissed);
}
