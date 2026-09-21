//! Mute, snooze, expiry, compaction, and retention.
//!
//! Covers bulk mute/unmute/snooze, offset normalization, atomic validation,
//! legacy-state recovery, dismissal cancelling snooze, the activity cursor,
//! and archive compaction under retention thresholds.

use super::support::*;
use chrono::{DateTime, Duration as ChronoDuration, SecondsFormat, Utc};
use sase_core::notifications::{
    apply_notification_state_update, notification_activity_cursor,
    read_current_notifications_snapshot, read_notifications_snapshot,
    rewrite_notifications, NotificationStateUpdateWire, NotificationWire,
};
use std::fs;
use std::path::Path;
use std::time::SystemTime;
use tempfile::tempdir;

fn timestamp_days_from_now(days: i64) -> String {
    (DateTime::<Utc>::from(SystemTime::now()) + ChronoDuration::days(days))
        .to_rfc3339_opts(SecondsFormat::Secs, false)
}

fn write_jsonl(path: &Path, rows: &[NotificationWire]) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    let mut body = String::new();
    for row in rows {
        body.push_str(&serde_json::to_string(row).unwrap());
        body.push('\n');
    }
    fs::write(path, body).unwrap();
}

#[test]
fn notification_mute_and_snooze_follow_python_semantics() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    rewrite_notifications(&path, &[notification("a")]).unwrap();

    let deadline = "2099-05-01T03:00:00+00:00".to_string();
    let outcome = apply_notification_state_update(
        &path,
        &NotificationStateUpdateWire::MarkSnoozed {
            id: "a".to_string(),
            until: deadline.clone(),
        },
    )
    .unwrap();
    let n = &outcome.notifications[0];
    assert!(n.muted);
    assert_eq!(n.snooze_until.as_deref(), Some(deadline.as_str()));

    let outcome = apply_notification_state_update(
        &path,
        &NotificationStateUpdateWire::MarkMuted {
            id: "a".to_string(),
            muted: false,
        },
    )
    .unwrap();
    let n = &outcome.notifications[0];
    assert!(!n.muted);
    assert_eq!(n.snooze_until, None);
}

#[test]
fn notification_bulk_mute_deduplicates_ids_and_reports_counts() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    let mut already_muted = notification("b");
    already_muted.muted = true;
    rewrite_notifications(
        &path,
        &[
            notification("a"),
            already_muted,
            notification("c"),
            notification("d"),
        ],
    )
    .unwrap();

    let outcome = apply_notification_state_update(
        &path,
        &NotificationStateUpdateWire::MarkManyMuted {
            ids: vec![
                "a".to_string(),
                "missing".to_string(),
                "b".to_string(),
                "a".to_string(),
            ],
            muted: true,
        },
    )
    .unwrap();

    assert_eq!(outcome.matched_count, 2);
    assert_eq!(outcome.changed_count, 1);
    assert!(
        outcome
            .notifications
            .iter()
            .find(|n| n.id == "a")
            .unwrap()
            .muted
    );
    assert!(
        outcome
            .notifications
            .iter()
            .find(|n| n.id == "b")
            .unwrap()
            .muted
    );
    assert!(
        !outcome
            .notifications
            .iter()
            .find(|n| n.id == "c")
            .unwrap()
            .muted
    );
}

#[test]
fn notification_bulk_unmute_cancels_snoozes_and_reports_counts() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    let mut snoozed = notification("a");
    snoozed.muted = true;
    snoozed.snooze_until = Some("2026-05-01T03:00:00+00:00".to_string());
    let mut muted = notification("b");
    muted.muted = true;
    let unmuted = notification("c");
    rewrite_notifications(&path, &[snoozed, muted, unmuted]).unwrap();

    let outcome = apply_notification_state_update(
        &path,
        &NotificationStateUpdateWire::MarkManyMuted {
            ids: vec![
                "a".to_string(),
                "b".to_string(),
                "c".to_string(),
                "missing".to_string(),
                "a".to_string(),
            ],
            muted: false,
        },
    )
    .unwrap();

    assert_eq!(outcome.matched_count, 3);
    assert_eq!(outcome.changed_count, 2);
    for id in ["a", "b", "c"] {
        let row = outcome.notifications.iter().find(|n| n.id == id).unwrap();
        assert!(!row.muted);
        assert_eq!(row.snooze_until, None);
    }
}

#[test]
fn notification_bulk_snooze_uses_one_deadline_and_reports_counts() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    let deadline = "2099-05-01T03:00:00+00:00".to_string();
    let mut already_snoozed = notification("b");
    already_snoozed.muted = true;
    already_snoozed.snooze_until = Some(deadline.clone());
    rewrite_notifications(
        &path,
        &[notification("a"), already_snoozed, notification("c")],
    )
    .unwrap();

    let outcome = apply_notification_state_update(
        &path,
        &NotificationStateUpdateWire::MarkManySnoozed {
            ids: vec!["a".to_string(), "b".to_string(), "a".to_string()],
            until: deadline.clone(),
        },
    )
    .unwrap();

    assert_eq!(outcome.matched_count, 2);
    assert_eq!(outcome.changed_count, 1);
    for id in ["a", "b"] {
        let row = outcome.notifications.iter().find(|n| n.id == id).unwrap();
        assert!(row.muted);
        assert_eq!(row.snooze_until.as_deref(), Some(deadline.as_str()));
    }
    let untouched = outcome.notifications.iter().find(|n| n.id == "c").unwrap();
    assert!(!untouched.muted);
    assert_eq!(untouched.snooze_until, None);
}

#[test]
fn notification_expire_snoozes_handles_aware_and_naive_timestamps() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    let mut aware = notification("aware");
    aware.muted = true;
    aware.read = true;
    aware.snooze_until = Some("2026-05-01T01:00:00+00:00".to_string());
    let mut naive = notification("naive");
    naive.muted = true;
    naive.read = true;
    naive.snooze_until = Some("2026-05-01T01:00:00".to_string());
    let mut future = notification("future");
    future.muted = true;
    future.snooze_until = Some("2026-05-01T05:00:00+00:00".to_string());
    rewrite_notifications(&path, &[aware, naive, future]).unwrap();

    let outcome = apply_notification_state_update(
        &path,
        &NotificationStateUpdateWire::ExpireSnoozes {
            now: "2026-05-01T02:00:00+00:00".to_string(),
        },
    )
    .unwrap();
    assert_eq!(outcome.changed_count, 2);
    assert_eq!(outcome.expired_ids, vec!["aware", "naive"]);
    assert_eq!(
        outcome.next_snooze_deadline.as_deref(),
        Some("2026-05-01T05:00:00+00:00")
    );
    for id in ["aware", "naive"] {
        let row = outcome.notifications.iter().find(|n| n.id == id).unwrap();
        assert!(!row.read);
        assert_eq!(
            row.resurfaced_at.as_deref(),
            Some("2026-05-01T02:00:00+00:00")
        );
    }
    assert!(
        !outcome
            .notifications
            .iter()
            .find(|n| n.id == "aware")
            .unwrap()
            .muted
    );
    assert!(
        !outcome
            .notifications
            .iter()
            .find(|n| n.id == "naive")
            .unwrap()
            .muted
    );
    assert!(
        outcome
            .notifications
            .iter()
            .find(|n| n.id == "future")
            .unwrap()
            .muted
    );
}

#[test]
fn notification_snooze_normalizes_offsets_and_projects_earliest_deadline() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    rewrite_notifications(
        &path,
        &[notification("a"), notification("b"), notification("c")],
    )
    .unwrap();

    let first = apply_notification_state_update(
        &path,
        &NotificationStateUpdateWire::MarkSnoozed {
            id: "a".to_string(),
            until: "2099-01-01T04:00:00-05:00".to_string(),
        },
    )
    .unwrap();
    assert_eq!(
        first.notifications[0].snooze_until.as_deref(),
        Some("2099-01-01T09:00:00+00:00")
    );
    assert_eq!(
        first.next_snooze_deadline.as_deref(),
        Some("2099-01-01T09:00:00+00:00")
    );

    let equivalent = apply_notification_state_update(
        &path,
        &NotificationStateUpdateWire::MarkSnoozed {
            id: "a".to_string(),
            until: "2099-01-01T10:00:00+01:00".to_string(),
        },
    )
    .unwrap();
    assert_eq!(equivalent.matched_count, 1);
    assert_eq!(equivalent.changed_count, 0);

    let later = apply_notification_state_update(
        &path,
        &NotificationStateUpdateWire::MarkSnoozed {
            id: "b".to_string(),
            until: "2099-01-01T10:00:00+00:00".to_string(),
        },
    )
    .unwrap();
    assert_eq!(
        later.next_snooze_deadline.as_deref(),
        Some("2099-01-01T09:00:00+00:00")
    );

    let microsecond = apply_notification_state_update(
        &path,
        &NotificationStateUpdateWire::MarkSnoozed {
            id: "c".to_string(),
            until: "2099-01-01T11:00:00.296000+00:00".to_string(),
        },
    )
    .unwrap();
    assert_eq!(
        microsecond
            .notifications
            .iter()
            .find(|notification| notification.id == "c")
            .unwrap()
            .snooze_until
            .as_deref(),
        Some("2099-01-01T11:00:00.296000+00:00")
    );
}

#[test]
fn notification_snooze_validation_is_atomic_and_skips_ineligible_targets() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    let mut dismissed = notification("dismissed");
    dismissed.dismissed = true;
    rewrite_notifications(&path, &[notification("active"), dismissed]).unwrap();

    for deadline in [
        "not-a-date",
        "2099-01-01T09:00:00",
        "2000-01-01T09:00:00+00:00",
    ] {
        let error = apply_notification_state_update(
            &path,
            &NotificationStateUpdateWire::MarkSnoozed {
                id: "active".to_string(),
                until: deadline.to_string(),
            },
        )
        .unwrap_err();
        assert!(error.contains("snooze deadline"));
    }
    let invalid_bulk = apply_notification_state_update(
        &path,
        &NotificationStateUpdateWire::MarkManySnoozed {
            ids: vec!["active".to_string(), "dismissed".to_string()],
            until: "2099-01-01T09:00:00".to_string(),
        },
    )
    .unwrap_err();
    assert!(invalid_bulk.contains("timezone-aware"));
    assert!(read_notifications_snapshot(&path, true)
        .unwrap()
        .notifications
        .iter()
        .all(|notification| notification.snooze_until.is_none()));

    let partial = apply_notification_state_update(
        &path,
        &NotificationStateUpdateWire::MarkManySnoozed {
            ids: vec!["active".to_string(), "missing".to_string()],
            until: "2099-01-01T09:00:00+00:00".to_string(),
        },
    )
    .unwrap();
    assert_eq!(partial.matched_count, 1);
    assert_eq!(partial.changed_count, 1);

    let dismissed = apply_notification_state_update(
        &path,
        &NotificationStateUpdateWire::MarkSnoozed {
            id: "dismissed".to_string(),
            until: "2099-01-01T09:00:00+00:00".to_string(),
        },
    )
    .unwrap();
    assert_eq!(dismissed.matched_count, 0);
    assert_eq!(dismissed.changed_count, 0);

    let snapshot = read_notifications_snapshot(&path, true).unwrap();
    assert_eq!(
        snapshot
            .notifications
            .iter()
            .find(|notification| notification.id == "active")
            .unwrap()
            .snooze_until
            .as_deref(),
        Some("2099-01-01T09:00:00+00:00")
    );
    assert!(snapshot
        .notifications
        .iter()
        .find(|notification| notification.id == "dismissed")
        .unwrap()
        .snooze_until
        .is_none());
}

#[test]
fn notification_current_read_recovers_legacy_state_and_preserves_cancellations()
{
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    let mut due_read = notification("due-read");
    due_read.muted = true;
    due_read.read = true;
    due_read.snooze_until = Some("2000-01-01T00:00:00+00:00".to_string());
    let mut malformed = notification("malformed");
    malformed.muted = true;
    malformed.snooze_until = Some("not-a-deadline".to_string());
    let mut naive = notification("naive");
    naive.muted = true;
    naive.snooze_until = Some("2099-01-01T00:00:00".to_string());
    let mut dismissed = notification("dismissed");
    dismissed.dismissed = true;
    dismissed.muted = true;
    dismissed.snooze_until = Some("2000-01-01T00:00:00+00:00".to_string());
    let mut permanent = notification("permanent");
    permanent.muted = true;
    let mut future = notification("future");
    future.muted = true;
    future.snooze_until = Some("2099-01-02T00:00:00+00:00".to_string());
    rewrite_notifications(
        &path,
        &[due_read, malformed, naive, dismissed, permanent, future],
    )
    .unwrap();

    let snapshot = read_current_notifications_snapshot(&path, true).unwrap();
    assert_eq!(snapshot.expired_ids, vec!["due-read", "malformed", "naive"]);
    assert_eq!(
        snapshot.next_snooze_deadline.as_deref(),
        Some("2099-01-02T00:00:00+00:00")
    );
    let rows = snapshot
        .notifications
        .iter()
        .map(|row| (row.id.as_str(), row))
        .collect::<std::collections::BTreeMap<_, _>>();
    for id in ["due-read", "malformed", "naive"] {
        assert!(!rows[id].muted);
        assert!(!rows[id].read);
        assert!(rows[id].snooze_until.is_none());
        assert!(rows[id].resurfaced_at.is_some());
    }
    assert!(rows["dismissed"].muted);
    assert!(rows["dismissed"].snooze_until.is_some());
    assert!(rows["permanent"].muted);
    assert!(rows["permanent"].snooze_until.is_none());

    let second = read_current_notifications_snapshot(&path, true).unwrap();
    assert!(second.expired_ids.is_empty());
}

#[test]
fn notification_snapshot_compacts_old_dismissed_rows_to_archive() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    let archive = archive_path(temp.path());
    let old_timestamp = timestamp_days_from_now(-30);
    let recent_timestamp = timestamp_days_from_now(-3);
    let future_snooze = timestamp_days_from_now(1);

    let mut live = notification("live");
    live.timestamp = old_timestamp.clone();
    let mut recent_dismissed = notification("recent-dismissed");
    recent_dismissed.timestamp = recent_timestamp;
    recent_dismissed.dismissed = true;
    let mut snoozed = notification("snoozed");
    snoozed.timestamp = old_timestamp.clone();
    snoozed.muted = true;
    snoozed.snooze_until = Some(future_snooze.clone());
    let mut dismissed_snoozed = notification("dismissed-snoozed");
    dismissed_snoozed.timestamp = old_timestamp.clone();
    dismissed_snoozed.dismissed = true;
    dismissed_snoozed.muted = true;
    dismissed_snoozed.snooze_until = Some(future_snooze);

    let mut rows = vec![
        live.clone(),
        recent_dismissed.clone(),
        snoozed.clone(),
        dismissed_snoozed.clone(),
    ];
    for index in 0..1_001 {
        let mut row = notification(&format!("old-dismissed-{index:04}"));
        row.timestamp = old_timestamp.clone();
        row.dismissed = true;
        rows.push(row);
    }
    write_jsonl(&path, &rows);

    let active = read_notifications_snapshot(&path, false).unwrap();
    assert_eq!(
        active
            .notifications
            .iter()
            .map(|row| row.id.as_str())
            .collect::<Vec<_>>(),
        vec!["live", "snoozed"]
    );
    assert_eq!(active.counts.rest, 1);
    assert_eq!(active.counts.muted, 1);
    assert_eq!(active.stats.total_lines, 4);
    assert_eq!(active.stats.dismissed_filtered, 2);

    let all = read_notifications_snapshot(&path, true).unwrap();
    assert_eq!(
        all.notifications
            .iter()
            .map(|row| row.id.as_str())
            .collect::<Vec<_>>(),
        vec!["live", "recent-dismissed", "snoozed", "dismissed-snoozed"]
    );
    assert_eq!(all.notifications[1], recent_dismissed);
    assert_eq!(all.notifications[2], snoozed);
    assert_eq!(all.notifications[3], dismissed_snoozed);

    let archived_lines = fs::read_to_string(&archive).unwrap();
    let archived: Vec<NotificationWire> = archived_lines
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(archived.len(), 1_001);
    assert!(archived
        .iter()
        .all(|row| row.dismissed && row.snooze_until.is_none()));

    let reread = read_notifications_snapshot(&path, false).unwrap();
    assert_eq!(reread.counts, active.counts);
    assert_eq!(reread.stats.total_lines, 4);
}

#[test]
fn notification_rewrite_compacts_when_store_crosses_retention_threshold() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    let archive = archive_path(temp.path());
    let old_timestamp = timestamp_days_from_now(-30);

    let mut rows = Vec::new();
    for index in 0..1_000 {
        let mut row = notification(&format!("old-dismissed-{index:04}"));
        row.timestamp = old_timestamp.clone();
        row.dismissed = true;
        rows.push(row);
    }
    write_jsonl(&path, &rows);

    let replacement = notification("replacement");
    rewrite_notifications(&path, std::slice::from_ref(&replacement)).unwrap();

    let all = read_notifications_snapshot(&path, true).unwrap();
    assert_eq!(all.notifications, vec![replacement]);
    assert_eq!(all.stats.total_lines, 1);
    assert_eq!(fs::read_to_string(&archive).unwrap().lines().count(), 1_000);
}

#[test]
fn notification_dismissal_cancels_snooze_without_resurfacing() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    let mut snoozed = notification("snoozed");
    snoozed.muted = true;
    snoozed.snooze_until = Some("2099-01-01T00:00:00+00:00".to_string());
    let mut bulk_one = notification("bulk-one");
    bulk_one.muted = true;
    bulk_one.snooze_until = Some("2099-01-02T00:00:00+00:00".to_string());
    let mut bulk_two = notification("bulk-two");
    bulk_two.muted = true;
    bulk_two.snooze_until = Some("2099-01-03T00:00:00+00:00".to_string());
    rewrite_notifications(&path, &[snoozed, bulk_one, bulk_two]).unwrap();

    let dismissed = apply_notification_state_update(
        &path,
        &NotificationStateUpdateWire::MarkDismissed {
            id: "snoozed".to_string(),
        },
    )
    .unwrap();
    let snoozed = dismissed
        .notifications
        .iter()
        .find(|notification| notification.id == "snoozed")
        .unwrap();
    assert!(snoozed.dismissed);
    assert!(snoozed.snooze_until.is_none());

    let bulk = apply_notification_state_update(
        &path,
        &NotificationStateUpdateWire::MarkManyDismissed {
            ids: vec!["bulk-one".to_string(), "bulk-two".to_string()],
        },
    )
    .unwrap();
    assert!(bulk.next_snooze_deadline.is_none());
    for id in ["bulk-one", "bulk-two"] {
        let row = bulk
            .notifications
            .iter()
            .find(|notification| notification.id == id)
            .unwrap();
        assert!(row.dismissed);
        assert!(row.snooze_until.is_none());
    }

    let expired = apply_notification_state_update(
        &path,
        &NotificationStateUpdateWire::ExpireSnoozes {
            now: "2100-01-01T00:00:00+00:00".to_string(),
        },
    )
    .unwrap();
    assert!(expired.expired_ids.is_empty());
}

#[test]
fn notification_activity_cursor_uses_resurface_time_and_id_tiebreaker() {
    let mut first = notification("a");
    first.resurfaced_at = Some("2026-06-01T00:00:00+00:00".to_string());
    let mut second = notification("b");
    second.resurfaced_at = first.resurfaced_at.clone();

    assert_eq!(
        notification_activity_cursor(&first),
        ("2026-06-01T00:00:00+00:00", "a")
    );
    assert!(
        notification_activity_cursor(&first)
            < notification_activity_cursor(&second)
    );
    assert_eq!(
        notification_activity_cursor(&notification("legacy")),
        ("2026-05-01T01:02:03+00:00", "legacy")
    );
}
