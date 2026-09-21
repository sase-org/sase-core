//! Store loading, legacy defaults, and JSONL round trips.
//!
//! Covers missing-file/legacy loading, the phase-1 contract fixture,
//! append/rewrite round trips, stale temp-sibling reaping, tag and icon
//! round trips, counts metadata, byte-identical JSONL output, and
//! unseen-row preservation.

use super::support::*;
use sase_core::notifications::{
    append_notification, append_notification_counts,
    apply_notification_state_update, read_notifications_snapshot,
    rewrite_notifications, rewrite_notifications_counts,
    NotificationStateUpdateWire,
};
use serde_json::json;
use std::fs::{self, File, FileTimes};
use std::time::{Duration, SystemTime};
use tempfile::tempdir;

const CONTRACT_FIXTURE: &str =
    include_str!("../fixtures/notifications/store_contract.jsonl");

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
