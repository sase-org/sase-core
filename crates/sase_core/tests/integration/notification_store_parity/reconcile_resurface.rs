//! Revision-keyed resurfacing for field-scoped reconcile writes.
//!
//! Covers `resurface_on_change_key`, which the remote-attention reconciler
//! uses to keep one row per request: a new revision takes the input row's
//! `read`, `dismissed` and `resurfaced_at` (so it resurfaces even a user
//! dismissal), an equal revision keeps a user dismissal, an identical
//! re-emission writes nothing and leaves the store generation alone, and an
//! unset key keeps today's behavior.

use super::support::*;
use sase_core::notifications::{
    apply_notification_state_update, read_notifications_snapshot,
    reconcile_notification_rows, rewrite_notifications,
    NotificationReconcileRequestWire, NotificationStateUpdateWire,
    NotificationWire,
};
use std::fs;
use tempfile::tempdir;

const MARKER_KEY: &str = "test_auto_dismissed";
const REVISION_KEY: &str = "test_revision";

fn revision_request(
    rows: Vec<NotificationWire>,
) -> NotificationReconcileRequestWire {
    NotificationReconcileRequestWire {
        notifications: rows,
        reversible_dismiss_marker_key: Some(MARKER_KEY.to_string()),
        resurface_on_change_key: Some(REVISION_KEY.to_string()),
        refresh_files: false,
    }
}

fn at_revision(id: &str, revision: &str) -> NotificationWire {
    let mut row = notification(id);
    row.action_data
        .insert(REVISION_KEY.to_string(), revision.to_string());
    row
}

fn only_row(path: &std::path::Path) -> NotificationWire {
    let rows = read_notifications_snapshot(path, true)
        .unwrap()
        .notifications;
    assert_eq!(rows.len(), 1);
    rows.into_iter().next().unwrap()
}

fn generation(path: &std::path::Path) -> u64 {
    read_notifications_snapshot(path, true).unwrap().generation
}

#[test]
fn new_revision_resurfaces_a_user_dismissal() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    let mut row = at_revision("remote-1", "1");
    row.read = true;
    row.dismissed = true;
    row.muted = true;
    rewrite_notifications(&path, &[row]).unwrap();

    let mut incoming = at_revision("remote-1", "2");
    incoming.notes = vec!["revised".to_string()];
    incoming.resurfaced_at = Some("2026-05-02T00:00:00+00:00".to_string());
    let outcome =
        reconcile_notification_rows(&path, &revision_request(vec![incoming]))
            .unwrap();
    assert_eq!(outcome.resurfaced, 1);
    assert_eq!(outcome.updated, 0);
    assert_eq!(outcome.created, 0);

    let stored = only_row(&path);
    assert!(!stored.dismissed);
    assert!(!stored.read);
    assert_eq!(stored.action_data[REVISION_KEY], "2");
    assert_eq!(stored.notes, vec!["revised".to_string()]);
    assert_eq!(
        stored.resurfaced_at.as_deref(),
        Some("2026-05-02T00:00:00+00:00")
    );
    // Disk-owned local choices other than read/dismissed survive.
    assert!(stored.muted);
    assert_eq!(stored.timestamp, "2026-05-01T01:02:03+00:00");
}

#[test]
fn new_revision_marks_a_read_row_unread() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    let mut row = at_revision("remote-1", "1");
    row.read = true;
    rewrite_notifications(&path, &[row]).unwrap();

    let outcome = reconcile_notification_rows(
        &path,
        &revision_request(vec![at_revision("remote-1", "2")]),
    )
    .unwrap();
    assert_eq!(outcome.updated, 1);
    assert_eq!(outcome.resurfaced, 0);

    let stored = only_row(&path);
    assert!(!stored.read);
    assert!(!stored.dismissed);
}

#[test]
fn same_revision_keeps_a_user_dismissal() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    rewrite_notifications(&path, &[at_revision("remote-1", "2")]).unwrap();
    // The user dismisses the current revision after the caller's snapshot.
    apply_notification_state_update(
        &path,
        &NotificationStateUpdateWire::MarkDismissed {
            id: "remote-1".to_string(),
        },
    )
    .unwrap();

    // A stale, undismissed copy of the same revision cannot undo it.
    let mut incoming = at_revision("remote-1", "2");
    incoming.notes = vec!["refreshed".to_string()];
    let outcome =
        reconcile_notification_rows(&path, &revision_request(vec![incoming]))
            .unwrap();
    assert_eq!(outcome.resurfaced, 0);
    assert_eq!(outcome.updated, 1);

    let stored = only_row(&path);
    assert!(stored.dismissed);
    assert_eq!(stored.action_data.get(MARKER_KEY), None);
    assert_eq!(stored.notes, vec!["refreshed".to_string()]);
}

#[test]
fn concurrent_writer_already_on_the_new_revision_keeps_its_dismissal() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    // Another reconciler already wrote revision 2 and the user dismissed it.
    let mut row = at_revision("remote-1", "2");
    row.dismissed = true;
    row.read = true;
    rewrite_notifications(&path, &[row]).unwrap();

    // This caller saw revision 1 on disk, so it sends revision 2 as a
    // resurface; under the lock the revisions match and the dismissal wins.
    let outcome = reconcile_notification_rows(
        &path,
        &revision_request(vec![at_revision("remote-1", "2")]),
    )
    .unwrap();
    assert_eq!(outcome.resurfaced, 0);

    let stored = only_row(&path);
    assert!(stored.dismissed);
    assert!(stored.read);
}

#[test]
fn identical_re_emission_writes_nothing_and_keeps_the_generation() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    let mut row = at_revision("remote-1", "2");
    row.dismissed = true;
    rewrite_notifications(&path, &[row.clone()]).unwrap();
    let before = fs::read(&path).unwrap();
    let generation_before = generation(&path);

    let outcome =
        reconcile_notification_rows(&path, &revision_request(vec![row]))
            .unwrap();
    assert_eq!(outcome.created, 0);
    assert_eq!(outcome.updated, 0);
    assert_eq!(outcome.dismissed, 0);
    assert_eq!(outcome.resurfaced, 0);
    assert_eq!(fs::read(&path).unwrap(), before);
    assert_eq!(generation(&path), generation_before);
}

#[test]
fn unset_change_key_keeps_a_user_dismissal_across_revisions() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    let mut row = at_revision("remote-1", "1");
    row.dismissed = true;
    rewrite_notifications(&path, &[row]).unwrap();

    let mut request = revision_request(vec![at_revision("remote-1", "2")]);
    request.resurface_on_change_key = None;
    let outcome = reconcile_notification_rows(&path, &request).unwrap();
    assert_eq!(outcome.resurfaced, 0);
    assert_eq!(outcome.updated, 1);

    let stored = only_row(&path);
    assert!(stored.dismissed);
    assert_eq!(stored.action_data[REVISION_KEY], "2");
}

#[test]
fn change_key_is_omitted_from_the_wire_when_unset() {
    let request = NotificationReconcileRequestWire::default();
    let value = serde_json::to_value(&request).unwrap();
    assert!(value.get("resurface_on_change_key").is_none());

    let parsed: NotificationReconcileRequestWire =
        serde_json::from_value(serde_json::json!({
            "notifications": [],
            "resurface_on_change_key": REVISION_KEY,
        }))
        .unwrap();
    assert_eq!(
        parsed.resurface_on_change_key.as_deref(),
        Some(REVISION_KEY)
    );
}
