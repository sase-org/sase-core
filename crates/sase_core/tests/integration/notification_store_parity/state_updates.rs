//! State updates, per-tab reads, batch dismiss, and undismiss.

use super::support::*;
use sase_core::notifications::{
    apply_notification_state_update, apply_notification_state_update_counts,
    read_notifications_snapshot, rewrite_notifications,
    NotificationStateUpdateWire,
};
use tempfile::tempdir;

#[test]
fn notification_counts_match_python_priority_rules() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    let mut plan = notification("plan");
    plan.action = Some("PlanApproval".to_string());
    let mut crs = notification("crs");
    crs.sender = "crs".to_string();
    let mut error = notification("error");
    error.sender = "user-agent".to_string();
    error.action = Some("ViewErrorReport".to_string());
    let rest = notification("rest");
    let mut muted = notification("muted");
    muted.muted = true;
    let mut read_plan = notification("read-plan");
    read_plan.action = Some("PlanApproval".to_string());
    read_plan.read = true;
    let mut silent_rest = notification("silent-rest");
    silent_rest.silent = true;
    rewrite_notifications(
        &path,
        &[plan, crs, error, rest, muted, read_plan, silent_rest],
    )
    .unwrap();

    let snapshot = read_notifications_snapshot(&path, false).unwrap();
    assert_eq!(snapshot.counts.priority, 2);
    assert_eq!(snapshot.counts.errors, 1);
    assert_eq!(snapshot.counts.rest, 1);
    assert_eq!(snapshot.counts.muted, 1);
}

#[test]
fn notification_state_updates_mutate_only_intended_rows() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    rewrite_notifications(&path, &[notification("a"), notification("b")])
        .unwrap();

    let outcome = apply_notification_state_update(
        &path,
        &NotificationStateUpdateWire::MarkRead {
            id: "a".to_string(),
        },
    )
    .unwrap();
    assert_eq!(outcome.matched_count, 1);
    assert_eq!(outcome.changed_count, 1);
    assert!(
        outcome
            .notifications
            .iter()
            .find(|n| n.id == "a")
            .unwrap()
            .read
    );
    assert!(
        !outcome
            .notifications
            .iter()
            .find(|n| n.id == "b")
            .unwrap()
            .read
    );

    let outcome = apply_notification_state_update(
        &path,
        &NotificationStateUpdateWire::MarkAllRead,
    )
    .unwrap();
    assert_eq!(outcome.changed_count, 1);
    assert!(outcome.notifications.iter().all(|n| n.read));

    let outcome = apply_notification_state_update(
        &path,
        &NotificationStateUpdateWire::MarkDismissed {
            id: "b".to_string(),
        },
    )
    .unwrap();
    assert_eq!(outcome.changed_count, 1);
    assert!(
        outcome
            .notifications
            .iter()
            .find(|n| n.id == "b")
            .unwrap()
            .dismissed
    );
}

#[test]
fn notification_state_update_counts_skips_returned_snapshot() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    rewrite_notifications(&path, &[notification("a"), notification("b")])
        .unwrap();

    let outcome = apply_notification_state_update_counts(
        &path,
        &NotificationStateUpdateWire::MarkAllRead,
    )
    .unwrap();

    assert_eq!(outcome.matched_count, 2);
    assert_eq!(outcome.changed_count, 2);
    assert!(outcome.rewritten);
    assert!(outcome.notifications.is_empty());
    assert_eq!(outcome.counts.priority, 0);
    assert_eq!(outcome.stats.loaded_rows, 0);

    let snapshot = read_notifications_snapshot(&path, true).unwrap();
    assert!(snapshot.notifications.iter().all(|n| n.read));
}

#[test]
fn notification_mark_tab_read_marks_only_unread_target_tab() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    let mut alpha = notification("alpha");
    alpha.tags = vec!["alpha".to_string()];
    let mut read_alpha = notification("read-alpha");
    read_alpha.tags = vec!["alpha".to_string()];
    read_alpha.read = true;
    let mut beta = notification("beta");
    beta.tags = vec!["beta".to_string()];
    let general = notification("general");
    rewrite_notifications(&path, &[alpha, read_alpha, beta, general]).unwrap();

    let outcome = apply_notification_state_update_counts(
        &path,
        &NotificationStateUpdateWire::MarkTabRead {
            tab_key: "alpha".to_string(),
        },
    )
    .unwrap();

    assert_eq!(outcome.matched_count, 1);
    assert_eq!(outcome.changed_count, 1);
    assert!(outcome.rewritten);
    assert!(outcome.notifications.is_empty());

    let snapshot = read_notifications_snapshot(&path, true).unwrap();
    for id in ["alpha", "read-alpha"] {
        let row = snapshot
            .notifications
            .iter()
            .find(|notification| notification.id == id)
            .unwrap();
        assert!(row.read);
    }
    for id in ["beta", "general"] {
        let row = snapshot
            .notifications
            .iter()
            .find(|notification| notification.id == id)
            .unwrap();
        assert!(!row.read);
    }

    let repeat = apply_notification_state_update_counts(
        &path,
        &NotificationStateUpdateWire::MarkTabRead {
            tab_key: "alpha".to_string(),
        },
    )
    .unwrap();
    assert_eq!(repeat.matched_count, 0);
    assert_eq!(repeat.changed_count, 0);
    assert!(!repeat.rewritten);
}

#[test]
fn notification_mark_tab_read_uses_general_tab_for_untagged_rows() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    let general = notification("general");
    let mut tagged = notification("tagged");
    tagged.tags = vec!["alpha".to_string()];
    rewrite_notifications(&path, &[general, tagged]).unwrap();

    let outcome = apply_notification_state_update(
        &path,
        &NotificationStateUpdateWire::MarkTabRead {
            tab_key: "general".to_string(),
        },
    )
    .unwrap();

    assert_eq!(outcome.matched_count, 1);
    assert_eq!(outcome.changed_count, 1);
    assert!(
        outcome
            .notifications
            .iter()
            .find(|notification| notification.id == "general")
            .unwrap()
            .read
    );
    assert!(
        !outcome
            .notifications
            .iter()
            .find(|notification| notification.id == "tagged")
            .unwrap()
            .read
    );
}

#[test]
fn notification_batch_dismiss_and_rewrite_all_update_the_store() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    rewrite_notifications(
        &path,
        &[notification("a"), notification("b"), notification("c")],
    )
    .unwrap();

    let outcome = apply_notification_state_update(
        &path,
        &NotificationStateUpdateWire::MarkManyDismissed {
            ids: vec!["a".to_string(), "c".to_string()],
        },
    )
    .unwrap();
    assert_eq!(outcome.changed_count, 2);
    assert_eq!(
        outcome.notifications.iter().filter(|n| n.dismissed).count(),
        2
    );

    let outcome = apply_notification_state_update(
        &path,
        &NotificationStateUpdateWire::RewriteAll {
            notifications: vec![notification("replacement")],
        },
    )
    .unwrap();
    let ids: Vec<&str> = outcome
        .notifications
        .iter()
        .map(|n| n.id.as_str())
        .collect();
    assert_eq!(ids, vec!["replacement", "a", "b", "c"]);
    assert_eq!(
        outcome.notifications.iter().filter(|n| n.dismissed).count(),
        2
    );
}

#[test]
fn notification_undismiss_restores_dismissed_rows() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    rewrite_notifications(
        &path,
        &[notification("a"), notification("b"), notification("c")],
    )
    .unwrap();

    let dismissed = apply_notification_state_update(
        &path,
        &NotificationStateUpdateWire::MarkManyDismissed {
            ids: vec!["a".to_string(), "b".to_string()],
        },
    )
    .unwrap();
    assert_eq!(dismissed.changed_count, 2);

    let outcome = apply_notification_state_update(
        &path,
        &NotificationStateUpdateWire::MarkUndismissed {
            id: "a".to_string(),
        },
    )
    .unwrap();
    assert_eq!(outcome.matched_count, 1);
    assert_eq!(outcome.changed_count, 1);
    assert!(
        !outcome
            .notifications
            .iter()
            .find(|n| n.id == "a")
            .unwrap()
            .dismissed
    );
    assert!(
        outcome
            .notifications
            .iter()
            .find(|n| n.id == "b")
            .unwrap()
            .dismissed
    );

    // Undismissing a visible row still matches but changes nothing.
    let outcome = apply_notification_state_update(
        &path,
        &NotificationStateUpdateWire::MarkUndismissed {
            id: "c".to_string(),
        },
    )
    .unwrap();
    assert_eq!(outcome.matched_count, 1);
    assert_eq!(outcome.changed_count, 0);

    // Unknown ids match nothing.
    let outcome = apply_notification_state_update(
        &path,
        &NotificationStateUpdateWire::MarkUndismissed {
            id: "missing".to_string(),
        },
    )
    .unwrap();
    assert_eq!(outcome.matched_count, 0);
    assert_eq!(outcome.changed_count, 0);

    let outcome = apply_notification_state_update(
        &path,
        &NotificationStateUpdateWire::MarkManyUndismissed {
            ids: vec!["a".to_string(), "b".to_string(), "missing".to_string()],
        },
    )
    .unwrap();
    assert_eq!(outcome.matched_count, 2);
    assert_eq!(outcome.changed_count, 1);
    assert!(outcome
        .notifications
        .iter()
        .filter(|n| n.id == "a" || n.id == "b")
        .all(|n| !n.dismissed));

    // The restored rows are visible to readers that exclude dismissed rows.
    let snapshot = read_notifications_snapshot(&path, false).unwrap();
    let ids: Vec<&str> = snapshot
        .notifications
        .iter()
        .map(|n| n.id.as_str())
        .collect();
    assert!(ids.contains(&"a"));
    assert!(ids.contains(&"b"));
    assert!(ids.contains(&"c"));
}
