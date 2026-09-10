use std::fs::{self, File, FileTimes};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, SystemTime};

use chrono::{DateTime, Duration as ChronoDuration, SecondsFormat, Utc};
use sase_core::notifications::{
    append_notification, append_notification_counts,
    append_notification_plus_one, apply_notification_state_update,
    apply_notification_state_update_counts, notification_activity_cursor,
    read_current_notifications_snapshot, read_notifications_snapshot,
    rewrite_notifications, rewrite_notifications_counts, upsert_notification,
    NotificationAgentKeyWire, NotificationPlusOneActionWire,
    NotificationPlusOneRequestWire, NotificationPlusOneWire,
    NotificationStateUpdateWire, NotificationUpsertActionWire,
    NotificationUpsertRequestWire, NotificationWire,
    NOTIFICATION_PLUS_ONE_MAX_ENTRIES, NOTIFICATION_PLUS_ONE_NOTE_MAX_CHARS,
    NOTIFICATION_STORE_WIRE_SCHEMA_VERSION,
};
use serde_json::json;
use tempfile::tempdir;

const CONTRACT_FIXTURE: &str =
    include_str!("fixtures/notifications/store_contract.jsonl");

fn store_path(root: &Path) -> PathBuf {
    root.join("notifications").join("notifications.jsonl")
}

fn archive_path(root: &Path) -> PathBuf {
    root.join("notifications")
        .join("notifications-archive.jsonl")
}

fn notification(id: &str) -> NotificationWire {
    NotificationWire {
        id: id.to_string(),
        timestamp: "2026-05-01T01:02:03+00:00".to_string(),
        sender: "test-sender".to_string(),
        ..NotificationWire::default()
    }
}

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
fn notification_missing_file_returns_empty_snapshot() {
    let temp = tempdir().unwrap();
    let snapshot =
        read_notifications_snapshot(&store_path(temp.path()), false).unwrap();
    assert!(snapshot.notifications.is_empty());
    assert_eq!(snapshot.counts.priority, 0);
    assert_eq!(snapshot.stats.loaded_rows, 0);
}

#[test]
fn notification_loads_legacy_defaults_and_skips_bad_rows() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(
        &path,
        [
            "",
            "NOT JSON",
            r#"{"id":"missing-timestamp","sender":"test"}"#,
            r#"{"id":"legacy","timestamp":"2026-05-01T01:02:03","sender":"test"}"#,
            r#"{"id":"dismissed","timestamp":"2026-05-01T01:02:03","sender":"test","dismissed":true}"#,
        ]
        .join("\n"),
    )
    .unwrap();

    let snapshot = read_notifications_snapshot(&path, false).unwrap();
    assert_eq!(snapshot.notifications.len(), 1);
    let loaded = &snapshot.notifications[0];
    assert_eq!(loaded.id, "legacy");
    assert!(!loaded.silent);
    assert!(!loaded.muted);
    assert_eq!(loaded.snooze_until, None);
    assert!(loaded.tags.is_empty());
    assert_eq!(snapshot.stats.blank_lines, 1);
    assert_eq!(snapshot.stats.invalid_json_lines, 1);
    assert_eq!(snapshot.stats.invalid_record_lines, 1);
    assert_eq!(snapshot.stats.dismissed_filtered, 1);
}

#[test]
fn notification_phase1_contract_fixture_loads_with_expected_counts() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(&path, CONTRACT_FIXTURE).unwrap();

    let active = read_notifications_snapshot(&path, false).unwrap();
    let all = read_notifications_snapshot(&path, true).unwrap();

    assert_eq!(active.notifications.len(), 12);
    assert_eq!(all.notifications.len(), 13);
    assert!(active.notifications.iter().all(|n| n.id != "dismissed-row"));
    assert!(all.notifications.iter().any(|n| n.id == "dismissed-row"));
    assert!(all.notifications.iter().all(|n| n.id != "missing-required"));
    assert_eq!(all.stats.invalid_json_lines, 1);
    assert_eq!(all.stats.invalid_record_lines, 1);

    let legacy = all
        .notifications
        .iter()
        .find(|n| n.id == "legacy-minimal")
        .unwrap();
    assert!(legacy.notes.is_empty());
    assert!(legacy.files.is_empty());
    assert!(legacy.tags.is_empty());
    assert_eq!(legacy.icon, None);
    assert!(legacy.action_data.is_empty());
    assert!(!legacy.read);
    assert!(!legacy.dismissed);
    assert!(!legacy.silent);
    assert!(!legacy.muted);
    assert_eq!(legacy.snooze_until, None);

    let tagged = all
        .notifications
        .iter()
        .find(|n| n.id == "valid-full")
        .unwrap();
    assert_eq!(tagged.tags, vec!["done", "review"]);
    assert_eq!(tagged.icon.as_deref(), Some("🔔"));

    assert_eq!(active.counts.priority, 4);
    assert_eq!(active.counts.errors, 2);
    assert_eq!(active.counts.rest, 2);
    assert_eq!(active.counts.muted, 2);
}

#[test]
fn notification_append_and_rewrite_round_trip_jsonl() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    let mut n = notification("one");
    n.sender = "crs".to_string();
    n.tags = vec!["done".to_string(), "alpha".to_string()];
    append_notification(&path, &n.clone()).unwrap();

    let mut added = notification("two");
    added.silent = true;
    added.tags = vec!["beta".to_string()];
    rewrite_notifications(&path, &[added.clone()]).unwrap();

    let snapshot = read_notifications_snapshot(&path, true).unwrap();
    assert_eq!(snapshot.notifications, vec![added, n]);
    assert_eq!(snapshot.stats.loaded_rows, 2);
}

#[test]
fn notification_rewrite_reaps_only_targeted_stale_temp_siblings() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    let stale = path
        .parent()
        .unwrap()
        .join(".notifications.jsonl.stale.tmp");
    let fresh = path
        .parent()
        .unwrap()
        .join(".notifications.jsonl.fresh.tmp");
    let unrelated = path.parent().unwrap().join(".other.jsonl.stale.tmp");
    let near_match = path.parent().unwrap().join(".notifications.jsonl.tmp");
    for temp_path in [&stale, &fresh, &unrelated, &near_match] {
        fs::write(temp_path, b"temp").unwrap();
    }
    let old = SystemTime::now() - Duration::from_secs(25 * 60 * 60);
    let old_times = FileTimes::new().set_modified(old);
    for temp_path in [&stale, &unrelated, &near_match] {
        File::options()
            .write(true)
            .open(temp_path)
            .unwrap()
            .set_times(old_times)
            .unwrap();
    }

    rewrite_notifications(&path, &[notification("one")]).unwrap();

    assert!(!stale.exists());
    assert!(fresh.exists());
    assert!(unrelated.exists());
    assert!(near_match.exists());
}

#[test]
fn notification_tags_round_trip_through_append_load_and_rewrite() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    let mut n = notification("tagged");
    n.tags = vec!["done".to_string(), "review".to_string()];

    append_notification(&path, &n).unwrap();
    let snapshot = read_notifications_snapshot(&path, true).unwrap();
    assert_eq!(snapshot.notifications[0].tags, vec!["done", "review"]);

    let mut rewritten = snapshot.notifications[0].clone();
    rewritten.read = true;
    rewrite_notifications(&path, &[rewritten]).unwrap();

    let snapshot = read_notifications_snapshot(&path, true).unwrap();
    assert_eq!(snapshot.notifications[0].tags, vec!["done", "review"]);
    assert!(snapshot.notifications[0].read);
}

#[test]
fn notification_icon_round_trips_through_append_load_and_rewrite() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    let mut n = notification("icon");
    n.icon = Some("🧭".to_string());

    append_notification(&path, &n).unwrap();
    let snapshot = read_notifications_snapshot(&path, true).unwrap();
    assert_eq!(snapshot.notifications[0].icon.as_deref(), Some("🧭"));

    let mut rewritten = snapshot.notifications[0].clone();
    rewritten.icon = Some("✨".to_string());
    rewrite_notifications(&path, &[rewritten]).unwrap();

    let snapshot = read_notifications_snapshot(&path, true).unwrap();
    assert_eq!(snapshot.notifications[0].icon.as_deref(), Some("✨"));
}

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

#[test]
fn notification_concurrent_append_and_expiry_converge_on_one_transition() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    let mut due = notification("due");
    due.muted = true;
    due.read = true;
    due.snooze_until = Some("2000-01-01T00:00:00+00:00".to_string());
    rewrite_notifications(&path, &[due]).unwrap();

    let barrier = Arc::new(Barrier::new(3));
    let append_path = path.clone();
    let append_barrier = Arc::clone(&barrier);
    let append_thread = thread::spawn(move || {
        append_barrier.wait();
        for idx in 0..50 {
            append_notification(
                &append_path,
                &notification(&format!("append-{idx}")),
            )
            .unwrap();
        }
    });

    let readers = (0..2)
        .map(|_| {
            let read_path = path.clone();
            let read_barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                read_barrier.wait();
                read_current_notifications_snapshot(&read_path, true)
                    .unwrap()
                    .expired_ids
            })
        })
        .collect::<Vec<_>>();

    append_thread.join().unwrap();
    let transition_count: usize = readers
        .into_iter()
        .map(|reader| reader.join().unwrap().len())
        .sum();
    assert_eq!(transition_count, 1);

    let snapshot = read_notifications_snapshot(&path, true).unwrap();
    assert_eq!(snapshot.notifications.len(), 51);
    let due = snapshot
        .notifications
        .iter()
        .find(|notification| notification.id == "due")
        .unwrap();
    assert!(!due.muted);
    assert!(!due.read);
    assert!(due.snooze_until.is_none());
    assert!(due.resurfaced_at.is_some());
    assert!(snapshot
        .notifications
        .iter()
        .any(|notification| notification.id == "append-49"));
}

#[test]
fn notification_dismiss_matching_agents_covers_notification_action_shapes() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    let mut jump = notification("jump");
    jump.action = Some("JumpToAgent".to_string());
    jump.action_data
        .insert("cl_name".to_string(), "feature".to_string());
    jump.action_data
        .insert("raw_suffix".to_string(), "20260501010203".to_string());
    let mut plan = notification("plan");
    plan.action = Some("PlanApproval".to_string());
    plan.action_data
        .insert("agent_cl_name".to_string(), "feature".to_string());
    plan.action_data
        .insert("agent_timestamp".to_string(), "260501_010203".to_string());
    let mut epic = notification("epic");
    epic.action = Some("EpicApproval".to_string());
    epic.action_data
        .insert("agent_cl_name".to_string(), "feature".to_string());
    epic.action_data
        .insert("agent_timestamp".to_string(), "260501_010203".to_string());
    let mut launch = notification("launch");
    launch.action = Some("LaunchApproval".to_string());
    launch
        .action_data
        .insert("agent_cl_name".to_string(), "feature".to_string());
    launch
        .action_data
        .insert("agent_timestamp".to_string(), "260501_010203".to_string());
    let mut question = notification("question");
    question.action = Some("UserQuestion".to_string());
    question
        .action_data
        .insert("agent_cl_name".to_string(), "other".to_string());
    let untouched = notification("untouched");
    rewrite_notifications(
        &path,
        &[jump, plan, epic, launch, question, untouched],
    )
    .unwrap();

    let outcome = apply_notification_state_update(
        &path,
        &NotificationStateUpdateWire::DismissMatchingAgents {
            agents: vec![NotificationAgentKeyWire {
                cl_name: "feature".to_string(),
                raw_suffix: Some("20260501010203".to_string()),
            }],
        },
    )
    .unwrap();
    assert_eq!(outcome.changed_count, 4);
    assert!(
        outcome
            .notifications
            .iter()
            .find(|n| n.id == "jump")
            .unwrap()
            .dismissed
    );
    assert!(
        outcome
            .notifications
            .iter()
            .find(|n| n.id == "epic")
            .unwrap()
            .dismissed
    );
    assert!(
        outcome
            .notifications
            .iter()
            .find(|n| n.id == "launch")
            .unwrap()
            .dismissed
    );
    assert!(
        outcome
            .notifications
            .iter()
            .find(|n| n.id == "plan")
            .unwrap()
            .dismissed
    );
    assert!(
        !outcome
            .notifications
            .iter()
            .find(|n| n.id == "question")
            .unwrap()
            .dismissed
    );
}

#[test]
fn notification_dismiss_matching_agents_matches_question_root_identity() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());

    let mut question = notification("question-root-match");
    question.action = Some("UserQuestion".to_string());
    question
        .action_data
        .insert("agent_cl_name".to_string(), "feature".to_string());
    question
        .action_data
        .insert("agent_timestamp".to_string(), "260501_010203".to_string());
    question.action_data.insert(
        "agent_root_timestamp".to_string(),
        "20260501030405".to_string(),
    );

    let mut plan = notification("plan-root-match");
    plan.action = Some("PlanApproval".to_string());
    plan.action_data
        .insert("agent_cl_name".to_string(), "feature".to_string());
    plan.action_data
        .insert("agent_timestamp".to_string(), "20260501010203".to_string());
    plan.action_data.insert(
        "agent_root_timestamp".to_string(),
        "260501_030405".to_string(),
    );

    let mut legacy = notification("legacy-name-match");
    legacy.action = Some("UserQuestion".to_string());
    legacy
        .action_data
        .insert("agent_cl_name".to_string(), "feature".to_string());

    let mut wrong_cl = notification("wrong-cl");
    wrong_cl.action = Some("UserQuestion".to_string());
    wrong_cl
        .action_data
        .insert("agent_cl_name".to_string(), "other".to_string());
    wrong_cl.action_data.insert(
        "agent_root_timestamp".to_string(),
        "260501_030405".to_string(),
    );

    let mut wrong_timestamp = notification("wrong-timestamp");
    wrong_timestamp.action = Some("UserQuestion".to_string());
    wrong_timestamp
        .action_data
        .insert("agent_cl_name".to_string(), "feature".to_string());
    wrong_timestamp
        .action_data
        .insert("agent_timestamp".to_string(), "260501_010204".to_string());
    wrong_timestamp.action_data.insert(
        "agent_root_timestamp".to_string(),
        "260501_030406".to_string(),
    );

    let mut already_dismissed = notification("already-dismissed");
    already_dismissed.action = Some("UserQuestion".to_string());
    already_dismissed
        .action_data
        .insert("agent_cl_name".to_string(), "feature".to_string());
    already_dismissed.action_data.insert(
        "agent_root_timestamp".to_string(),
        "260501_030405".to_string(),
    );
    already_dismissed.dismissed = true;

    rewrite_notifications(
        &path,
        &[
            question,
            plan,
            legacy,
            wrong_cl,
            wrong_timestamp,
            already_dismissed,
        ],
    )
    .unwrap();

    let update = NotificationStateUpdateWire::DismissMatchingAgents {
        agents: vec![NotificationAgentKeyWire {
            cl_name: "feature".to_string(),
            raw_suffix: Some("20260501030405".to_string()),
        }],
    };
    let outcome = apply_notification_state_update(&path, &update).unwrap();
    assert_eq!(outcome.matched_count, 3);
    assert_eq!(outcome.changed_count, 3);

    let by_id: std::collections::HashMap<_, _> = outcome
        .notifications
        .iter()
        .map(|notification| (notification.id.as_str(), notification.dismissed))
        .collect();
    assert_eq!(by_id.get("question-root-match"), Some(&true));
    assert_eq!(by_id.get("plan-root-match"), Some(&true));
    assert_eq!(by_id.get("legacy-name-match"), Some(&true));
    assert_eq!(by_id.get("wrong-cl"), Some(&false));
    assert_eq!(by_id.get("wrong-timestamp"), Some(&false));
    assert_eq!(by_id.get("already-dismissed"), Some(&true));

    let repeated = apply_notification_state_update(&path, &update).unwrap();
    assert_eq!(repeated.matched_count, 0);
    assert_eq!(repeated.changed_count, 0);
    assert!(
        repeated
            .notifications
            .iter()
            .find(|notification| notification.id == "already-dismissed")
            .unwrap()
            .dismissed
    );
}

#[test]
fn notification_dismiss_matching_agents_matches_question_child_identity() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    let mut question = notification("question-child-match");
    question.action = Some("UserQuestion".to_string());
    question
        .action_data
        .insert("agent_cl_name".to_string(), "feature".to_string());
    question
        .action_data
        .insert("agent_timestamp".to_string(), "260501_010203".to_string());
    question.action_data.insert(
        "agent_root_timestamp".to_string(),
        "260501_030405".to_string(),
    );
    rewrite_notifications(&path, &[question]).unwrap();

    let outcome = apply_notification_state_update(
        &path,
        &NotificationStateUpdateWire::DismissMatchingAgents {
            agents: vec![NotificationAgentKeyWire {
                cl_name: "feature".to_string(),
                raw_suffix: Some("20260501010203".to_string()),
            }],
        },
    )
    .unwrap();

    assert_eq!(outcome.matched_count, 1);
    assert_eq!(outcome.changed_count, 1);
    assert!(outcome.notifications[0].dismissed);
}

#[test]
fn notification_dismiss_matching_agents_covers_custom_gates() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    let mut custom = notification("custom-gate");
    custom.action = Some("CustomGate".to_string());
    custom
        .action_data
        .insert("agent_cl_name".to_string(), "feature".to_string());
    custom
        .action_data
        .insert("agent_timestamp".to_string(), "260501_010203".to_string());
    rewrite_notifications(&path, &[custom]).unwrap();

    let outcome = apply_notification_state_update(
        &path,
        &NotificationStateUpdateWire::DismissMatchingAgents {
            agents: vec![NotificationAgentKeyWire {
                cl_name: "feature".to_string(),
                raw_suffix: Some("20260501010203".to_string()),
            }],
        },
    )
    .unwrap();

    assert_eq!(outcome.matched_count, 1);
    assert_eq!(outcome.changed_count, 1);
    assert!(outcome.notifications[0].dismissed);
}

#[test]
fn notification_dismiss_matching_agents_covers_user_agent_view_error_report() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    let mut error = notification("error");
    error.sender = "user-agent".to_string();
    error.action = Some("ViewErrorReport".to_string());
    error
        .action_data
        .insert("cl_name".to_string(), "feature".to_string());
    error
        .action_data
        .insert("raw_suffix".to_string(), "20260501010203".to_string());
    let mut axe_error = notification("axe-error");
    axe_error.sender = "axe".to_string();
    axe_error.action = Some("ViewErrorReport".to_string());
    axe_error
        .action_data
        .insert("error_report_path".to_string(), "/tmp/x".to_string());
    rewrite_notifications(&path, &[error, axe_error]).unwrap();

    let outcome = apply_notification_state_update(
        &path,
        &NotificationStateUpdateWire::DismissMatchingAgents {
            agents: vec![NotificationAgentKeyWire {
                cl_name: "feature".to_string(),
                raw_suffix: Some("20260501010203".to_string()),
            }],
        },
    )
    .unwrap();
    assert_eq!(outcome.changed_count, 1);
    assert!(
        outcome
            .notifications
            .iter()
            .find(|n| n.id == "error")
            .unwrap()
            .dismissed
    );
    assert!(
        !outcome
            .notifications
            .iter()
            .find(|n| n.id == "axe-error")
            .unwrap()
            .dismissed
    );
}

#[test]
fn notification_dismiss_agent_completions_matching_agents_is_completion_only() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());

    let mut jump = notification("jump");
    jump.sender = "user-agent".to_string();
    jump.action = Some("JumpToAgent".to_string());
    jump.action_data
        .insert("cl_name".to_string(), "feature".to_string());
    jump.action_data
        .insert("raw_suffix".to_string(), "20260501010203".to_string());

    let mut error = notification("error");
    error.sender = "user-agent".to_string();
    error.action = Some("ViewErrorReport".to_string());
    error
        .action_data
        .insert("cl_name".to_string(), "feature".to_string());
    error
        .action_data
        .insert("raw_suffix".to_string(), "20260501010203".to_string());

    let mut other = notification("other");
    other.sender = "user-agent".to_string();
    other.action = Some("JumpToAgent".to_string());
    other
        .action_data
        .insert("cl_name".to_string(), "feature".to_string());
    other
        .action_data
        .insert("raw_suffix".to_string(), "20260501010204".to_string());

    let mut plan = notification("plan");
    plan.sender = "user-agent".to_string();
    plan.action = Some("PlanApproval".to_string());
    plan.action_data
        .insert("agent_cl_name".to_string(), "feature".to_string());
    plan.action_data
        .insert("agent_timestamp".to_string(), "260501_010203".to_string());

    rewrite_notifications(&path, &[jump, error, other, plan]).unwrap();

    let outcome = apply_notification_state_update(
        &path,
        &NotificationStateUpdateWire::DismissAgentCompletionsMatchingAgents {
            agents: vec![NotificationAgentKeyWire {
                cl_name: "feature".to_string(),
                raw_suffix: Some("20260501010203".to_string()),
            }],
        },
    )
    .unwrap();
    assert_eq!(outcome.matched_count, 2);
    assert_eq!(outcome.changed_count, 2);

    let by_id: std::collections::HashMap<_, _> = outcome
        .notifications
        .iter()
        .map(|n| (n.id.clone(), n.dismissed))
        .collect();
    assert_eq!(by_id.get("jump"), Some(&true));
    assert_eq!(by_id.get("error"), Some(&true));
    assert_eq!(by_id.get("other"), Some(&false));
    assert_eq!(by_id.get("plan"), Some(&false));
}

#[test]
fn notification_dismiss_agent_completions_matches_user_agent_jump_and_error() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());

    let mut jump = notification("jump");
    jump.sender = "user-agent".to_string();
    jump.action = Some("JumpToAgent".to_string());
    jump.action_data
        .insert("cl_name".to_string(), "feature-a".to_string());
    jump.action_data
        .insert("raw_suffix".to_string(), "20260501010203".to_string());

    let mut error = notification("error");
    error.sender = "user-agent".to_string();
    error.action = Some("ViewErrorReport".to_string());
    error
        .action_data
        .insert("cl_name".to_string(), "feature-b".to_string());
    error
        .action_data
        .insert("raw_suffix".to_string(), "20260501010204".to_string());

    let mut plan = notification("plan");
    plan.sender = "user-agent".to_string();
    plan.action = Some("PlanApproval".to_string());
    plan.action_data
        .insert("agent_cl_name".to_string(), "feature-c".to_string());

    let mut question = notification("question");
    question.sender = "user-agent".to_string();
    question.action = Some("UserQuestion".to_string());
    question
        .action_data
        .insert("agent_cl_name".to_string(), "feature-d".to_string());

    let mut mentor = notification("mentor");
    mentor.sender = "user-agent".to_string();
    mentor.action = Some("JumpToMentorReview".to_string());
    mentor
        .action_data
        .insert("cl_name".to_string(), "feature-e".to_string());

    let mut axe_error = notification("axe-error");
    axe_error.sender = "axe".to_string();
    axe_error.action = Some("ViewErrorReport".to_string());
    axe_error
        .action_data
        .insert("error_report_path".to_string(), "/tmp/x".to_string());

    let mut crs = notification("crs");
    crs.sender = "crs".to_string();
    crs.action = Some("JumpToAgent".to_string());
    crs.action_data
        .insert("cl_name".to_string(), "feature-f".to_string());

    let mut already_dismissed = notification("already-dismissed");
    already_dismissed.sender = "user-agent".to_string();
    already_dismissed.action = Some("JumpToAgent".to_string());
    already_dismissed
        .action_data
        .insert("cl_name".to_string(), "feature-g".to_string());
    already_dismissed.dismissed = true;

    let mut no_cl = notification("no-cl");
    no_cl.sender = "user-agent".to_string();
    no_cl.action = Some("JumpToAgent".to_string());

    rewrite_notifications(
        &path,
        &[
            jump,
            error,
            plan,
            question,
            mentor,
            axe_error,
            crs,
            already_dismissed,
            no_cl,
        ],
    )
    .unwrap();

    let outcome = apply_notification_state_update(
        &path,
        &NotificationStateUpdateWire::DismissAgentCompletions,
    )
    .unwrap();
    assert_eq!(outcome.matched_count, 2);
    assert_eq!(outcome.changed_count, 2);

    let by_id: std::collections::HashMap<_, _> = outcome
        .notifications
        .iter()
        .map(|n| (n.id.clone(), n.dismissed))
        .collect();
    assert_eq!(by_id.get("jump"), Some(&true));
    assert_eq!(by_id.get("error"), Some(&true));
    assert_eq!(by_id.get("plan"), Some(&false));
    assert_eq!(by_id.get("question"), Some(&false));
    assert_eq!(by_id.get("mentor"), Some(&false));
    assert_eq!(by_id.get("axe-error"), Some(&false));
    assert_eq!(by_id.get("crs"), Some(&false));
    assert_eq!(by_id.get("already-dismissed"), Some(&true));
    assert_eq!(by_id.get("no-cl"), Some(&false));
}

#[test]
fn notification_dismiss_agent_completions_no_op_when_already_dismissed() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());

    let mut already = notification("already");
    already.sender = "user-agent".to_string();
    already.action = Some("JumpToAgent".to_string());
    already
        .action_data
        .insert("cl_name".to_string(), "feature".to_string());
    already.dismissed = true;

    rewrite_notifications(&path, &[already]).unwrap();

    let outcome = apply_notification_state_update(
        &path,
        &NotificationStateUpdateWire::DismissAgentCompletions,
    )
    .unwrap();
    assert_eq!(outcome.matched_count, 0);
    assert_eq!(outcome.changed_count, 0);
    assert!(!outcome.rewritten);
}

#[test]
fn notification_append_plus_rewrite_concurrency_preserves_valid_rows() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    rewrite_notifications(&path, &[notification("seed")]).unwrap();

    let append_path = path.clone();
    let append_thread = thread::spawn(move || {
        for idx in 0..80 {
            append_notification(
                &append_path,
                &notification(&format!("append-{idx}")),
            )
            .unwrap();
        }
    });

    let rewrite_path = path.clone();
    let rewrite_thread = thread::spawn(move || {
        for idx in 0..30 {
            let snapshot =
                read_notifications_snapshot(&rewrite_path, true).unwrap();
            let mut rows = snapshot.notifications;
            rows.push(notification(&format!("rewrite-{idx}")));
            rewrite_notifications(&rewrite_path, &rows).unwrap();
        }
    });

    append_thread.join().unwrap();
    rewrite_thread.join().unwrap();

    let content = fs::read_to_string(&path).unwrap();
    assert!(!content.is_empty());
    for line in content.lines() {
        let value: serde_json::Value = serde_json::from_str(line).unwrap();
        assert!(value.get("id").is_some());
    }
    let snapshot = read_notifications_snapshot(&path, true).unwrap();
    assert!(snapshot.notifications.iter().any(|n| n.id == "seed"));
    assert!(snapshot.notifications.iter().any(|n| n.id == "append-79"));
    assert!(snapshot.notifications.iter().any(|n| n.id == "rewrite-29"));
}

#[test]
fn notification_json_shape_uses_expected_wire_keys() {
    let mut n = notification("shape");
    n.action = Some("JumpToMentorReview".to_string());
    n.action_data
        .insert("entry_id".to_string(), "2".to_string());
    let value = serde_json::to_value(&n).unwrap();
    assert_eq!(
        value,
        json!({
            "id": "shape",
            "timestamp": "2026-05-01T01:02:03+00:00",
            "sender": "test-sender",
            "icon": null,
            "notes": [],
            "files": [],
            "tags": [],
            "action": "JumpToMentorReview",
            "action_data": {"entry_id": "2"},
            "read": false,
            "dismissed": false,
            "silent": false,
            "muted": false,
            "snooze_until": null,
            "resurfaced_at": null
        })
    );
}

#[test]
fn notification_append_counts_returns_metadata_without_rows() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    let outcome =
        append_notification_counts(&path, &notification("only")).unwrap();
    assert_eq!(outcome.appended_count, 1);
    assert_eq!(outcome.matched_count, 0);
    assert_eq!(outcome.changed_count, 0);
    assert!(!outcome.rewritten);
    assert!(outcome.notifications.is_empty());
    assert!(outcome.expired_ids.is_empty());
    assert_eq!(outcome.stats.loaded_rows, 0);
    assert_eq!(outcome.counts.priority, 0);

    let snapshot = read_notifications_snapshot(&path, true).unwrap();
    assert_eq!(snapshot.notifications.len(), 1);
    assert_eq!(snapshot.notifications[0].id, "only");
}

#[test]
fn notification_rewrite_counts_returns_metadata_without_rows() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    let outcome = rewrite_notifications_counts(
        &path,
        &[notification("a"), notification("b"), notification("c")],
    )
    .unwrap();
    assert_eq!(outcome.matched_count, 3);
    assert_eq!(outcome.changed_count, 3);
    assert_eq!(outcome.appended_count, 0);
    assert!(outcome.rewritten);
    assert!(outcome.notifications.is_empty());

    let snapshot = read_notifications_snapshot(&path, true).unwrap();
    assert_eq!(snapshot.notifications.len(), 3);
    let ids: Vec<&str> = snapshot
        .notifications
        .iter()
        .map(|n| n.id.as_str())
        .collect();
    assert_eq!(ids, vec!["a", "b", "c"]);
}

#[test]
fn notification_append_counts_produces_byte_identical_jsonl() {
    let temp_baseline = tempdir().unwrap();
    let temp_counts = tempdir().unwrap();
    let path_baseline = store_path(temp_baseline.path());
    let path_counts = store_path(temp_counts.path());

    let mut n1 = notification("one");
    n1.sender = "axe".to_string();
    n1.action = Some("PlanApproval".to_string());
    let mut n2 = notification("two");
    n2.read = true;

    append_notification(&path_baseline, &n1).unwrap();
    append_notification(&path_baseline, &n2).unwrap();
    append_notification_counts(&path_counts, &n1).unwrap();
    append_notification_counts(&path_counts, &n2).unwrap();

    let baseline = fs::read_to_string(&path_baseline).unwrap();
    let counts = fs::read_to_string(&path_counts).unwrap();
    assert_eq!(baseline, counts);
}

#[test]
fn notification_rewrite_counts_produces_byte_identical_jsonl() {
    let temp_baseline = tempdir().unwrap();
    let temp_counts = tempdir().unwrap();
    let path_baseline = store_path(temp_baseline.path());
    let path_counts = store_path(temp_counts.path());

    let mut n1 = notification("alpha");
    n1.sender = "axe".to_string();
    let mut n2 = notification("beta");
    n2.action = Some("PlanApproval".to_string());
    let mut n3 = notification("gamma");
    n3.muted = true;
    n3.snooze_until = Some("2026-05-12T10:00:00+00:00".to_string());

    let payload = [n1, n2, n3];
    rewrite_notifications(&path_baseline, &payload).unwrap();
    rewrite_notifications_counts(&path_counts, &payload).unwrap();

    let baseline = fs::read_to_string(&path_baseline).unwrap();
    let counts = fs::read_to_string(&path_counts).unwrap();
    assert_eq!(baseline, counts);
}

#[test]
fn notification_rewrite_preserves_unseen_rows() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    rewrite_notifications(
        &path,
        &[notification("a"), notification("b"), notification("c")],
    )
    .unwrap();

    let mut a_modified = notification("a");
    a_modified.read = true;
    rewrite_notifications(&path, &[a_modified, notification("d")]).unwrap();

    let snapshot = read_notifications_snapshot(&path, true).unwrap();
    let ids: Vec<&str> = snapshot
        .notifications
        .iter()
        .map(|n| n.id.as_str())
        .collect();
    assert_eq!(ids, vec!["a", "d", "b", "c"]);
    let a = snapshot.notifications.iter().find(|n| n.id == "a").unwrap();
    assert!(a.read);
}

#[test]
fn notification_rewrite_counts_preserves_unseen_rows() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    rewrite_notifications_counts(
        &path,
        &[notification("a"), notification("b"), notification("c")],
    )
    .unwrap();

    let mut a_modified = notification("a");
    a_modified.read = true;
    rewrite_notifications_counts(&path, &[a_modified, notification("d")])
        .unwrap();

    let snapshot = read_notifications_snapshot(&path, true).unwrap();
    let ids: Vec<&str> = snapshot
        .notifications
        .iter()
        .map(|n| n.id.as_str())
        .collect();
    assert_eq!(ids, vec!["a", "d", "b", "c"]);
    let a = snapshot.notifications.iter().find(|n| n.id == "a").unwrap();
    assert!(a.read);
}

#[test]
fn notification_rewrite_all_preserves_unseen_rows() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    rewrite_notifications(
        &path,
        &[notification("a"), notification("b"), notification("c")],
    )
    .unwrap();

    let mut a_modified = notification("a");
    a_modified.read = true;
    apply_notification_state_update(
        &path,
        &NotificationStateUpdateWire::RewriteAll {
            notifications: vec![a_modified, notification("d")],
        },
    )
    .unwrap();

    let snapshot = read_notifications_snapshot(&path, true).unwrap();
    let ids: Vec<&str> = snapshot
        .notifications
        .iter()
        .map(|n| n.id.as_str())
        .collect();
    assert_eq!(ids, vec!["a", "d", "b", "c"]);
    let a = snapshot.notifications.iter().find(|n| n.id == "a").unwrap();
    assert!(a.read);
}

#[test]
fn notification_append_plus_rewrite_counts_concurrency_preserves_valid_rows() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    rewrite_notifications_counts(&path, &[notification("seed")]).unwrap();

    let append_path = path.clone();
    let append_thread = thread::spawn(move || {
        for idx in 0..80 {
            append_notification_counts(
                &append_path,
                &notification(&format!("append-{idx}")),
            )
            .unwrap();
        }
    });

    let rewrite_path = path.clone();
    let rewrite_thread = thread::spawn(move || {
        for idx in 0..30 {
            let snapshot =
                read_notifications_snapshot(&rewrite_path, true).unwrap();
            let mut rows = snapshot.notifications;
            rows.push(notification(&format!("rewrite-{idx}")));
            rewrite_notifications_counts(&rewrite_path, &rows).unwrap();
        }
    });

    append_thread.join().unwrap();
    rewrite_thread.join().unwrap();

    let content = fs::read_to_string(&path).unwrap();
    assert!(!content.is_empty());
    for line in content.lines() {
        let value: serde_json::Value = serde_json::from_str(line).unwrap();
        assert!(value.get("id").is_some());
    }
    let snapshot = read_notifications_snapshot(&path, true).unwrap();
    assert!(snapshot.notifications.iter().any(|n| n.id == "seed"));
    assert!(snapshot.notifications.iter().any(|n| n.id == "append-79"));
    assert!(snapshot.notifications.iter().any(|n| n.id == "rewrite-29"));
}

fn plus_one_by_id(id: &str, note: &str) -> NotificationPlusOneRequestWire {
    NotificationPlusOneRequestWire {
        id: Some(id.to_string()),
        timestamp: "2026-05-01T03:00:00+00:00".to_string(),
        sender: "test-sender".to_string(),
        note: note.to_string(),
        ..NotificationPlusOneRequestWire::default()
    }
}

fn plus_one_by_key(key: &str, note: &str) -> NotificationPlusOneRequestWire {
    NotificationPlusOneRequestWire {
        dedup_key: Some(key.to_string()),
        timestamp: "2026-05-01T03:00:00+00:00".to_string(),
        sender: "test-sender".to_string(),
        note: note.to_string(),
        ..NotificationPlusOneRequestWire::default()
    }
}

fn assert_state_flags_untouched(
    before: &NotificationWire,
    after: &NotificationWire,
) {
    assert_eq!(after.timestamp, before.timestamp);
    assert_eq!(after.resurfaced_at, before.resurfaced_at);
    assert_eq!(after.read, before.read);
    assert_eq!(after.dismissed, before.dismissed);
    assert_eq!(after.silent, before.silent);
    assert_eq!(after.muted, before.muted);
    assert_eq!(after.snooze_until, before.snooze_until);
}

#[test]
fn notification_plus_one_fields_default_and_skip_on_legacy_rows() {
    let parsed: NotificationWire = serde_json::from_str(
        r#"{"id":"legacy","timestamp":"2026-05-01T01:02:03+00:00","sender":"test","future_field":true}"#,
    )
    .unwrap();
    assert!(parsed.plus_ones.is_empty());
    assert_eq!(parsed.plus_ones_dropped, 0);
    assert_eq!(parsed.dedup_key, None);
    assert_eq!(parsed.plus_one_count(), 0);

    let encoded = serde_json::to_value(notification("one")).unwrap();
    assert!(encoded.get("plus_ones").is_none());
    assert!(encoded.get("plus_ones_dropped").is_none());
    assert!(encoded.get("dedup_key").is_none());
}

#[test]
fn notification_plus_one_round_trip_and_legacy_jsonl_omits_empty_fields() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    append_notification(&path, &notification("plain")).unwrap();

    let mut keyed = notification("keyed");
    keyed.dedup_key = Some("ci-failure/a,b".to_string());
    keyed.plus_ones = vec![NotificationPlusOneWire {
        timestamp: "2026-05-01T03:00:00+00:00".to_string(),
        sender: "ci_watch".to_string(),
        note: "fingerprint churn".to_string(),
    }];
    keyed.plus_ones_dropped = 2;
    append_notification(&path, &keyed).unwrap();

    let content = fs::read_to_string(&path).unwrap();
    let mut lines = content.lines();
    let plain_line = lines.next().unwrap();
    assert!(!plain_line.contains("plus_ones"));
    assert!(!plain_line.contains("plus_ones_dropped"));
    assert!(!plain_line.contains("dedup_key"));
    let keyed_line = lines.next().unwrap();
    assert!(keyed_line.contains("plus_ones"));
    assert!(keyed_line.contains("plus_ones_dropped"));
    assert!(keyed_line.contains("ci-failure/a,b"));

    let snapshot = read_notifications_snapshot(&path, true).unwrap();
    assert_eq!(
        snapshot.schema_version,
        NOTIFICATION_STORE_WIRE_SCHEMA_VERSION
    );
    assert_eq!(snapshot.schema_version, 1);
    let plain = snapshot
        .notifications
        .iter()
        .find(|row| row.id == "plain")
        .unwrap();
    assert!(plain.plus_ones.is_empty());
    assert_eq!(plain.dedup_key, None);
    let loaded = snapshot
        .notifications
        .iter()
        .find(|row| row.id == "keyed")
        .unwrap();
    assert_eq!(loaded.plus_ones.len(), 1);
    assert_eq!(loaded.plus_ones[0].note, "fingerprint churn");
    assert_eq!(loaded.plus_ones_dropped, 2);
    assert_eq!(loaded.plus_one_count(), 3);
    assert_eq!(loaded.dedup_key.as_deref(), Some("ci-failure/a,b"));
}

#[test]
fn notification_plus_one_appends_by_id_and_preserves_cursor() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    let mut row = notification("target");
    row.read = true;
    row.muted = true;
    row.snooze_until = Some("2026-05-12T10:00:00+00:00".to_string());
    row.resurfaced_at = Some("2026-05-01T01:30:00+00:00".to_string());
    append_notification(&path, &row).unwrap();
    let before_cursor = notification_activity_cursor(&row);

    let outcome = append_notification_plus_one(
        &path,
        &plus_one_by_id("target", "  again   soon  "),
    )
    .unwrap();
    assert_eq!(outcome.action, NotificationPlusOneActionWire::Applied);
    assert_eq!(outcome.id.as_deref(), Some("target"));
    assert_eq!(outcome.plus_one_count, 1);
    assert_eq!(outcome.schema_version, 1);

    let snapshot = read_notifications_snapshot(&path, true).unwrap();
    let updated = &snapshot.notifications[0];
    assert_eq!(updated.plus_ones.len(), 1);
    assert_eq!(updated.plus_ones[0].note, "again soon");
    assert_eq!(updated.plus_ones[0].sender, "test-sender");
    assert_state_flags_untouched(&row, updated);
    assert_eq!(notification_activity_cursor(updated), before_cursor);
}

#[test]
fn notification_plus_one_by_key_picks_newest_including_dismissed() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    let mut older = notification("older");
    older.timestamp = "2026-05-01T01:00:00+00:00".to_string();
    older.dedup_key = Some("combo".to_string());
    let mut newer = notification("newer");
    newer.timestamp = "2026-05-01T02:00:00+00:00".to_string();
    newer.dedup_key = Some("combo".to_string());
    newer.dismissed = true;
    append_notification(&path, &older).unwrap();
    append_notification(&path, &newer).unwrap();

    let outcome =
        append_notification_plus_one(&path, &plus_one_by_key("combo", "quiet"))
            .unwrap();
    assert_eq!(outcome.action, NotificationPlusOneActionWire::Applied);
    assert_eq!(outcome.id.as_deref(), Some("newer"));

    let snapshot = read_notifications_snapshot(&path, true).unwrap();
    let older = snapshot
        .notifications
        .iter()
        .find(|row| row.id == "older")
        .unwrap();
    let newer = snapshot
        .notifications
        .iter()
        .find(|row| row.id == "newer")
        .unwrap();
    assert!(older.plus_ones.is_empty());
    assert_eq!(newer.plus_ones.len(), 1);
    assert!(newer.dismissed);
}

#[test]
fn notification_plus_one_no_match_and_invalid_requests() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    append_notification(&path, &notification("only")).unwrap();

    let missing =
        append_notification_plus_one(&path, &plus_one_by_id("missing", "note"))
            .unwrap();
    assert_eq!(missing.action, NotificationPlusOneActionWire::NoMatch);
    assert_eq!(missing.notification, None);

    let missing_key =
        append_notification_plus_one(&path, &plus_one_by_key("nope", "note"))
            .unwrap();
    assert_eq!(missing_key.action, NotificationPlusOneActionWire::NoMatch);

    let err = append_notification_plus_one(
        &path,
        &NotificationPlusOneRequestWire {
            note: "note".to_string(),
            timestamp: "2026-05-01T03:00:00+00:00".to_string(),
            sender: "test-sender".to_string(),
            ..NotificationPlusOneRequestWire::default()
        },
    )
    .unwrap_err();
    assert!(err.contains("id or dedup_key"));

    let blank = append_notification_plus_one(
        &path,
        &plus_one_by_id("only", "   \n\t  "),
    )
    .unwrap_err();
    assert!(blank.contains("empty or blank"));

    let naive = append_notification_plus_one(
        &path,
        &NotificationPlusOneRequestWire {
            id: Some("only".to_string()),
            timestamp: "2026-05-01T03:00:00".to_string(),
            sender: "test-sender".to_string(),
            note: "note".to_string(),
            ..NotificationPlusOneRequestWire::default()
        },
    )
    .unwrap_err();
    assert!(naive.contains("timezone-aware"));
}

#[test]
fn notification_plus_one_caps_entries_and_counts_drops() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    let mut row = notification("cap");
    row.plus_ones = (0..NOTIFICATION_PLUS_ONE_MAX_ENTRIES)
        .map(|index| NotificationPlusOneWire {
            timestamp: "2026-05-01T02:00:00+00:00".to_string(),
            sender: "test-sender".to_string(),
            note: format!("n{index}"),
        })
        .collect();
    rewrite_notifications(&path, &[row]).unwrap();

    let outcome =
        append_notification_plus_one(&path, &plus_one_by_id("cap", "newest"))
            .unwrap();
    assert_eq!(
        outcome.plus_one_count,
        NOTIFICATION_PLUS_ONE_MAX_ENTRIES as u64 + 1
    );
    assert_eq!(outcome.plus_ones_dropped, 1);

    let snapshot = read_notifications_snapshot(&path, true).unwrap();
    let updated = &snapshot.notifications[0];
    assert_eq!(updated.plus_ones.len(), NOTIFICATION_PLUS_ONE_MAX_ENTRIES);
    assert_eq!(updated.plus_ones[0].note, "n1");
    assert_eq!(
        updated.plus_ones[NOTIFICATION_PLUS_ONE_MAX_ENTRIES - 1].note,
        "newest"
    );
    assert_eq!(updated.plus_one_count(), outcome.plus_one_count);
}

#[test]
fn notification_plus_one_note_is_capped_at_max_chars() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    append_notification(&path, &notification("long")).unwrap();
    let oversize = "é".repeat(NOTIFICATION_PLUS_ONE_NOTE_MAX_CHARS + 8);
    let outcome =
        append_notification_plus_one(&path, &plus_one_by_id("long", &oversize))
            .unwrap();
    let note = &outcome.notification.unwrap().plus_ones[0].note;
    assert_eq!(note.chars().count(), NOTIFICATION_PLUS_ONE_NOTE_MAX_CHARS);
}

#[test]
fn notification_upsert_without_dedup_key_matches_append_bytes() {
    let temp_append = tempdir().unwrap();
    let temp_upsert = tempdir().unwrap();
    let path_append = store_path(temp_append.path());
    let path_upsert = store_path(temp_upsert.path());
    let row = notification("fresh");

    append_notification(&path_append, &row).unwrap();
    let outcome = upsert_notification(
        &path_upsert,
        &NotificationUpsertRequestWire {
            notification: row,
            ..NotificationUpsertRequestWire::default()
        },
    )
    .unwrap();
    assert_eq!(outcome.action, NotificationUpsertActionWire::Created);
    assert_eq!(
        fs::read_to_string(&path_append).unwrap(),
        fs::read_to_string(&path_upsert).unwrap()
    );
}

#[test]
fn notification_upsert_creates_then_plus_ones_by_dedup_key() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    let mut created = notification("first");
    created.dedup_key = Some("combo".to_string());
    created.notes = vec!["CI failure: a, b".to_string()];

    let first = upsert_notification(
        &path,
        &NotificationUpsertRequestWire {
            notification: created.clone(),
            plus_one_note: Some("initial".to_string()),
            plus_one_timestamp: Some("2026-05-01T03:00:00+00:00".to_string()),
            supersedes: None,
        },
    )
    .unwrap();
    assert_eq!(first.action, NotificationUpsertActionWire::Created);
    assert_eq!(first.id.as_deref(), Some("first"));
    assert!(first.notification.unwrap().plus_ones.is_empty());

    let mut second = notification("second");
    second.dedup_key = Some("combo".to_string());
    second.notes = vec!["should be discarded".to_string()];
    let plus = upsert_notification(
        &path,
        &NotificationUpsertRequestWire {
            notification: second,
            plus_one_note: Some("churn".to_string()),
            plus_one_timestamp: Some("2026-05-01T04:00:00+00:00".to_string()),
            supersedes: None,
        },
    )
    .unwrap();
    assert_eq!(plus.action, NotificationUpsertActionWire::PlusOned);
    assert_eq!(plus.id.as_deref(), Some("first"));

    let snapshot = read_notifications_snapshot(&path, true).unwrap();
    assert_eq!(snapshot.notifications.len(), 1);
    let row = &snapshot.notifications[0];
    assert_eq!(row.id, "first");
    assert_eq!(row.notes, vec!["CI failure: a, b".to_string()]);
    assert_eq!(row.plus_ones.len(), 1);
    assert_eq!(row.plus_ones[0].note, "churn");
    assert_eq!(row.plus_ones[0].timestamp, "2026-05-01T04:00:00+00:00");
}

#[test]
fn notification_upsert_rejects_dedup_key_without_plus_one_note() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    let mut row = notification("needs-note");
    row.dedup_key = Some("combo".to_string());
    let err = upsert_notification(
        &path,
        &NotificationUpsertRequestWire {
            notification: row,
            plus_one_note: None,
            plus_one_timestamp: None,
            supersedes: None,
        },
    )
    .unwrap_err();
    assert!(err.contains("plus_one_note"));
}

#[test]
fn notification_upsert_matches_dismissed_and_snoozed_rows() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    let mut dismissed = notification("quiet");
    dismissed.dedup_key = Some("combo".to_string());
    dismissed.dismissed = true;
    dismissed.muted = true;
    dismissed.snooze_until = Some("2026-05-12T10:00:00+00:00".to_string());
    append_notification(&path, &dismissed).unwrap();

    let mut incoming = notification("ignored-id");
    incoming.dedup_key = Some("combo".to_string());
    let outcome = upsert_notification(
        &path,
        &NotificationUpsertRequestWire {
            notification: incoming,
            plus_one_note: Some("still accruing".to_string()),
            plus_one_timestamp: Some("2026-05-01T04:00:00+00:00".to_string()),
            supersedes: None,
        },
    )
    .unwrap();
    assert_eq!(outcome.action, NotificationUpsertActionWire::PlusOned);
    assert_eq!(outcome.id.as_deref(), Some("quiet"));

    let snapshot = read_notifications_snapshot(&path, true).unwrap();
    assert_eq!(snapshot.notifications.len(), 1);
    assert_state_flags_untouched(&dismissed, &snapshot.notifications[0]);
    assert_eq!(
        snapshot.notifications[0].plus_ones[0].note,
        "still accruing"
    );
}

#[test]
fn notification_upsert_supersedes_matching_rows_only_on_create() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    let mut old_a = notification("old-a");
    old_a.dedup_key = Some("old".to_string());
    old_a.timestamp = "2026-05-01T01:00:00+00:00".to_string();
    let mut old_b = notification("old-b");
    old_b.dedup_key = Some("old".to_string());
    old_b.timestamp = "2026-05-01T01:30:00+00:00".to_string();
    let mut other = notification("other");
    other.sender = "someone-else".to_string();
    other.dedup_key = Some("old".to_string());
    append_notification(&path, &old_a).unwrap();
    append_notification(&path, &old_b).unwrap();
    append_notification(&path, &other).unwrap();

    let mut created = notification("new");
    created.dedup_key = Some("new".to_string());
    created.notes = vec!["CI failure: a, b, c".to_string()];
    let outcome = upsert_notification(
        &path,
        &NotificationUpsertRequestWire {
            notification: created,
            plus_one_note: Some("rolled".to_string()),
            plus_one_timestamp: Some("2026-05-01T04:00:00+00:00".to_string()),
            supersedes: Some("old".to_string()),
        },
    )
    .unwrap();
    assert_eq!(outcome.action, NotificationUpsertActionWire::Created);
    let mut superseded = outcome.superseded_ids;
    superseded.sort();
    assert_eq!(superseded, vec!["old-a", "old-b"]);

    let snapshot = read_notifications_snapshot(&path, true).unwrap();
    let old_a = snapshot
        .notifications
        .iter()
        .find(|row| row.id == "old-a")
        .unwrap();
    let old_b = snapshot
        .notifications
        .iter()
        .find(|row| row.id == "old-b")
        .unwrap();
    let other = snapshot
        .notifications
        .iter()
        .find(|row| row.id == "other")
        .unwrap();
    let created = snapshot
        .notifications
        .iter()
        .find(|row| row.id == "new")
        .unwrap();
    assert!(old_a.dismissed);
    assert!(old_b.dismissed);
    assert!(!other.dismissed);
    assert!(old_a.plus_ones[0].note.starts_with("superseded by:"));
    assert!(old_a.plus_ones[0].note.contains("CI failure: a, b, c"));
    assert!(!created.dismissed);
    assert!(created.plus_ones.is_empty());
    assert_eq!(created.dedup_key.as_deref(), Some("new"));
}

#[test]
fn notification_upsert_supersede_no_match_is_silent() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    let mut created = notification("new");
    created.dedup_key = Some("new".to_string());
    let outcome = upsert_notification(
        &path,
        &NotificationUpsertRequestWire {
            notification: created,
            plus_one_note: Some("first".to_string()),
            plus_one_timestamp: Some("2026-05-01T04:00:00+00:00".to_string()),
            supersedes: Some("missing".to_string()),
        },
    )
    .unwrap();
    assert_eq!(outcome.action, NotificationUpsertActionWire::Created);
    assert!(outcome.superseded_ids.is_empty());
    let snapshot = read_notifications_snapshot(&path, true).unwrap();
    assert_eq!(snapshot.notifications.len(), 1);
    assert!(!snapshot.notifications[0].dismissed);
}

#[test]
fn notification_upsert_does_not_supersede_when_plus_oning() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    let mut live = notification("live");
    live.dedup_key = Some("combo".to_string());
    let mut old = notification("old");
    old.dedup_key = Some("old".to_string());
    append_notification(&path, &live).unwrap();
    append_notification(&path, &old).unwrap();

    let mut incoming = notification("ignored");
    incoming.dedup_key = Some("combo".to_string());
    incoming.notes = vec!["unused".to_string()];
    upsert_notification(
        &path,
        &NotificationUpsertRequestWire {
            notification: incoming,
            plus_one_note: Some("delta".to_string()),
            plus_one_timestamp: Some("2026-05-01T04:00:00+00:00".to_string()),
            supersedes: Some("old".to_string()),
        },
    )
    .unwrap();

    let snapshot = read_notifications_snapshot(&path, true).unwrap();
    let old = snapshot
        .notifications
        .iter()
        .find(|row| row.id == "old")
        .unwrap();
    assert!(!old.dismissed);
    assert!(old.plus_ones.is_empty());
}

#[test]
fn notification_append_plus_upsert_concurrency_preserves_valid_rows() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    let mut seed = notification("seed");
    seed.dedup_key = Some("combo".to_string());
    rewrite_notifications(&path, &[seed]).unwrap();

    let append_path = path.clone();
    let append_thread = thread::spawn(move || {
        for idx in 0..40 {
            append_notification(
                &append_path,
                &notification(&format!("append-{idx}")),
            )
            .unwrap();
        }
    });

    let upsert_path = path.clone();
    let upsert_thread = thread::spawn(move || {
        for idx in 0..40 {
            let mut incoming = notification(&format!("upsert-{idx}"));
            incoming.dedup_key = Some("combo".to_string());
            upsert_notification(
                &upsert_path,
                &NotificationUpsertRequestWire {
                    notification: incoming,
                    plus_one_note: Some(format!("tick {idx}")),
                    plus_one_timestamp: Some(
                        "2026-05-01T04:00:00+00:00".to_string(),
                    ),
                    supersedes: None,
                },
            )
            .unwrap();
        }
    });

    append_thread.join().unwrap();
    upsert_thread.join().unwrap();

    let content = fs::read_to_string(&path).unwrap();
    for line in content.lines() {
        let value: serde_json::Value = serde_json::from_str(line).unwrap();
        assert!(value.get("id").is_some());
    }
    let snapshot = read_notifications_snapshot(&path, true).unwrap();
    let seed = snapshot
        .notifications
        .iter()
        .find(|row| row.id == "seed")
        .unwrap();
    assert_eq!(seed.plus_ones.len(), 40);
    assert_eq!(seed.timestamp, "2026-05-01T01:02:03+00:00");
    assert!(snapshot.notifications.iter().any(|n| n.id == "append-39"));
    assert!(!snapshot
        .notifications
        .iter()
        .any(|n| n.id.starts_with("upsert-")));
}
