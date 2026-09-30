//! Plus-one reactions and upsert/supersede semantics.

use super::support::*;
use sase_core::notifications::{
    append_notification, append_notification_plus_one,
    notification_activity_cursor, read_notifications_snapshot,
    rewrite_notifications, upsert_notification, NotificationPlusOneActionWire,
    NotificationPlusOneRequestWire, NotificationPlusOneWire,
    NotificationUpsertActionWire, NotificationUpsertRequestWire,
    NotificationWire, NOTIFICATION_PLUS_ONE_MAX_ENTRIES,
    NOTIFICATION_PLUS_ONE_NOTE_MAX_CHARS,
    NOTIFICATION_STORE_WIRE_SCHEMA_VERSION,
    NOTIFICATION_WAIT_CHECKS_PLUS_ONE_MAX_ENTRIES,
};
use std::fs;
use tempfile::tempdir;

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
fn notification_wait_checks_plus_one_cap_is_32_with_exact_drops() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    let mut row = notification("wait-row");
    row.sender = "wait_checks".to_string();
    row.plus_ones = (0..NOTIFICATION_WAIT_CHECKS_PLUS_ONE_MAX_ENTRIES)
        .map(|index| NotificationPlusOneWire {
            timestamp: "2026-05-01T02:00:00+00:00".to_string(),
            sender: "wait_checks".to_string(),
            note: format!("n{index}"),
        })
        .collect();
    rewrite_notifications(&path, &[row]).unwrap();

    let outcome = append_notification_plus_one(
        &path,
        &plus_one_by_id("wait-row", "newest"),
    )
    .unwrap();
    assert_eq!(
        outcome.plus_one_count,
        NOTIFICATION_WAIT_CHECKS_PLUS_ONE_MAX_ENTRIES as u64 + 1
    );
    assert_eq!(outcome.plus_ones_dropped, 1);

    let snapshot = read_notifications_snapshot(&path, true).unwrap();
    let updated = &snapshot.notifications[0];
    assert_eq!(
        updated.plus_ones.len(),
        NOTIFICATION_WAIT_CHECKS_PLUS_ONE_MAX_ENTRIES
    );
    assert_eq!(updated.plus_ones[0].note, "n1");
    assert_eq!(
        updated.plus_ones[NOTIFICATION_WAIT_CHECKS_PLUS_ONE_MAX_ENTRIES - 1]
            .note,
        "newest"
    );
    assert_eq!(updated.plus_one_count(), outcome.plus_one_count);
}

#[test]
fn notification_wait_checks_upsert_caps_at_32_while_others_keep_500() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    let mut wait_row = notification("wait-row");
    wait_row.sender = "wait_checks".to_string();
    wait_row.dedup_key = Some("wait-combo".to_string());
    wait_row.plus_ones = (0..NOTIFICATION_WAIT_CHECKS_PLUS_ONE_MAX_ENTRIES)
        .map(|index| NotificationPlusOneWire {
            timestamp: "2026-05-01T02:00:00+00:00".to_string(),
            sender: "wait_checks".to_string(),
            note: format!("w{index}"),
        })
        .collect();
    let mut other_row = notification("other-row");
    other_row.dedup_key = Some("other-combo".to_string());
    other_row.plus_ones = (0..NOTIFICATION_PLUS_ONE_MAX_ENTRIES)
        .map(|index| NotificationPlusOneWire {
            timestamp: "2026-05-01T02:00:00+00:00".to_string(),
            sender: "test-sender".to_string(),
            note: format!("o{index}"),
        })
        .collect();
    rewrite_notifications(&path, &[wait_row, other_row]).unwrap();

    let mut wait_incoming = notification("wait-ignored");
    wait_incoming.sender = "wait_checks".to_string();
    wait_incoming.dedup_key = Some("wait-combo".to_string());
    let wait_outcome = upsert_notification(
        &path,
        &NotificationUpsertRequestWire {
            notification: wait_incoming,
            plus_one_note: Some("wait-newest".to_string()),
            plus_one_timestamp: Some("2026-05-01T04:00:00+00:00".to_string()),
            supersedes: None,
        },
    )
    .unwrap();
    assert_eq!(wait_outcome.action, NotificationUpsertActionWire::PlusOned);
    assert_eq!(wait_outcome.plus_ones_dropped, 1);

    let mut other_incoming = notification("other-ignored");
    other_incoming.dedup_key = Some("other-combo".to_string());
    let other_outcome = upsert_notification(
        &path,
        &NotificationUpsertRequestWire {
            notification: other_incoming,
            plus_one_note: Some("other-newest".to_string()),
            plus_one_timestamp: Some("2026-05-01T04:00:00+00:00".to_string()),
            supersedes: None,
        },
    )
    .unwrap();
    assert_eq!(other_outcome.action, NotificationUpsertActionWire::PlusOned);
    assert_eq!(other_outcome.plus_ones_dropped, 1);

    let snapshot = read_notifications_snapshot(&path, true).unwrap();
    let wait_row = snapshot
        .notifications
        .iter()
        .find(|row| row.id == "wait-row")
        .unwrap();
    assert_eq!(
        wait_row.plus_ones.len(),
        NOTIFICATION_WAIT_CHECKS_PLUS_ONE_MAX_ENTRIES
    );
    assert_eq!(wait_row.plus_ones[0].note, "w1");
    assert_eq!(
        wait_row.plus_ones[NOTIFICATION_WAIT_CHECKS_PLUS_ONE_MAX_ENTRIES - 1]
            .note,
        "wait-newest"
    );
    let other_row = snapshot
        .notifications
        .iter()
        .find(|row| row.id == "other-row")
        .unwrap();
    assert_eq!(other_row.plus_ones.len(), NOTIFICATION_PLUS_ONE_MAX_ENTRIES);
    assert_eq!(other_row.plus_ones[0].note, "o1");
    assert_eq!(
        other_row.plus_ones[NOTIFICATION_PLUS_ONE_MAX_ENTRIES - 1].note,
        "other-newest"
    );
}

#[test]
fn notification_compaction_trims_legacy_wait_checks_rows_preserving_newest() {
    use std::fs;

    use sase_core::notifications::NotificationWire;

    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    let mut wait_row = notification("legacy-wait");
    wait_row.sender = "wait_checks".to_string();
    wait_row.plus_ones = (0..100)
        .map(|index| NotificationPlusOneWire {
            timestamp: "2026-05-01T02:00:00+00:00".to_string(),
            sender: "wait_checks".to_string(),
            note: format!("legacy-{index}"),
        })
        .collect();
    wait_row.plus_ones_dropped = 5;
    let mut other_row = notification("legacy-other");
    other_row.plus_ones = (0..10)
        .map(|index| NotificationPlusOneWire {
            timestamp: "2026-05-01T02:00:00+00:00".to_string(),
            sender: "test-sender".to_string(),
            note: format!("other-{index}"),
        })
        .collect();
    let mut body = format!(
        "{}\n{}\n",
        serde_json::to_string(&wait_row).unwrap(),
        serde_json::to_string(&other_row).unwrap(),
    );
    for index in 0..1_000 {
        let mut filler = notification(&format!("filler-{index:04}"));
        filler.timestamp = "2020-01-01T00:00:00+00:00".to_string();
        filler.dismissed = true;
        body.push_str(&serde_json::to_string(&filler).unwrap());
        body.push('\n');
    }
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(&path, body).unwrap();

    let before = read_notifications_snapshot(&path, true).unwrap();
    let trimmed = before
        .notifications
        .iter()
        .find(|row| row.id == "legacy-wait")
        .unwrap();
    assert_eq!(
        trimmed.plus_ones.len(),
        NOTIFICATION_WAIT_CHECKS_PLUS_ONE_MAX_ENTRIES
    );
    assert_eq!(trimmed.plus_ones[0].note, "legacy-68");
    assert_eq!(
        trimmed.plus_ones[NOTIFICATION_WAIT_CHECKS_PLUS_ONE_MAX_ENTRIES - 1]
            .note,
        "legacy-99"
    );
    assert_eq!(trimmed.plus_ones_dropped, 5 + (100 - 32));
    assert_eq!(
        trimmed.plus_one_count(),
        NOTIFICATION_WAIT_CHECKS_PLUS_ONE_MAX_ENTRIES as u64
            + 5
            + (100 - 32) as u64
    );
    let untouched = before
        .notifications
        .iter()
        .find(|row| row.id == "legacy-other")
        .unwrap();
    assert_eq!(untouched.plus_ones.len(), 10);
    assert_eq!(untouched.plus_ones_dropped, 0);

    let after = read_notifications_snapshot(&path, true).unwrap();
    assert_eq!(after.generation, before.generation);
    assert_eq!(after.notifications, before.notifications);
    let stored: NotificationWire = serde_json::from_str(
        fs::read_to_string(&path).unwrap().lines().next().unwrap(),
    )
    .unwrap();
    assert_eq!(
        stored.plus_ones.len(),
        NOTIFICATION_WAIT_CHECKS_PLUS_ONE_MAX_ENTRIES
    );
}
