use super::support::*;
use sase_core::notifications::{
    append_notification, append_notification_with_options,
    compact_notification_store, read_notifications_snapshot,
    reconcile_notification_rows, resolve_notification_payloads,
    upsert_notification, NotificationAppendRequestWire,
    NotificationCompactionRequestWire, NotificationReconcileRequestWire,
    NotificationUpsertRequestWire,
};
use std::fs;
use tempfile::tempdir;

#[test]
fn notification_sidecars_round_trip_deduplicate_and_respect_the_byte_limit() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    let large = "payload-🙂".repeat(40);
    let mut row = notification("sidecar");
    row.notes = vec![large.clone()];
    row.action_data.insert("boundary".into(), "abc".into());
    row.action_data
        .insert("over_boundary".into(), "abcd".into());
    row.action_data.insert("payload".into(), large.clone());
    row.action_data.insert("same_payload".into(), large.clone());

    append_notification_with_options(
        &path,
        &NotificationAppendRequestWire {
            notification: row,
            externalize_limit_bytes: Some(3),
        },
    )
    .unwrap();

    let snapshot = read_notifications_snapshot(&path, true).unwrap();
    let stored = &snapshot.notifications[0];
    assert_eq!(stored.action_data["boundary"], "abc");
    assert!(stored.action_data["over_boundary"]
        .starts_with("sase-notification-blob:v1:"));
    assert_eq!(
        stored.action_data["payload"],
        stored.action_data["same_payload"]
    );
    assert_eq!(
        stored.notes[0].chars().take(80).collect::<String>(),
        large.chars().take(80).collect::<String>()
    );
    assert!(stored.notes[0].ends_with(&stored.action_data["payload"]));

    let blobs = path.parent().unwrap().join("blobs");
    assert_eq!(fs::read_dir(blobs).unwrap().count(), 2);
    let missing = format!("sase-notification-blob:v1:{}:3", "f".repeat(64));
    let resolved = resolve_notification_payloads(
        &path,
        &[
            stored.action_data["boundary"].clone(),
            stored.action_data["over_boundary"].clone(),
            stored.action_data["payload"].clone(),
            stored.notes[0].clone(),
            missing,
        ],
    )
    .unwrap();
    assert_eq!(resolved[0].as_deref(), Some("abc"));
    assert_eq!(resolved[1].as_deref(), Some("abcd"));
    assert_eq!(resolved[2].as_deref(), Some(large.as_str()));
    assert_eq!(resolved[3].as_deref(), Some(large.as_str()));
    assert_eq!(resolved[4], None);
}

#[test]
fn notification_compaction_externalizes_live_values_and_prunes_broken_files() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    let existing_file = temp.path().join("evidence.txt");
    fs::write(&existing_file, "evidence").unwrap();
    let mut row = notification("compact");
    row.notes = vec!["a long note to externalize".into()];
    row.files = vec![
        existing_file.to_string_lossy().into_owned(),
        temp.path()
            .join("missing.txt")
            .to_string_lossy()
            .into_owned(),
    ];
    row.action_data
        .insert("payload".into(), "a long action value".into());
    append_notification(&path, &row).unwrap();

    let outcome = compact_notification_store(
        &path,
        &NotificationCompactionRequestWire {
            externalize_limit_bytes: Some(8),
        },
    )
    .unwrap();
    assert_eq!(outcome.externalized_count, 2);
    assert_eq!(outcome.broken_files_dropped, 1);
    assert!(outcome.rewritten);

    let stored = &read_notifications_snapshot(&path, true)
        .unwrap()
        .notifications[0];
    assert_eq!(
        stored.files,
        vec![existing_file.to_string_lossy().into_owned()]
    );
    assert!(
        stored.action_data["payload"].starts_with("sase-notification-blob:v1:")
    );
    assert!(stored.notes[0].starts_with("a long n"));
}

#[test]
fn notification_upsert_and_reconcile_accept_the_opt_in_limit() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    let mut upserted = notification("upserted");
    upserted
        .action_data
        .insert("payload".into(), "oversized upsert value".into());
    upsert_notification(
        &path,
        &NotificationUpsertRequestWire {
            notification: upserted,
            externalize_limit_bytes: Some(3),
            ..NotificationUpsertRequestWire::default()
        },
    )
    .unwrap();

    let mut reconciled = notification("reconciled");
    reconciled.notes = vec!["oversized reconcile note".into()];
    reconcile_notification_rows(
        &path,
        &NotificationReconcileRequestWire {
            notifications: vec![reconciled],
            externalize_limit_bytes: Some(3),
            ..NotificationReconcileRequestWire::default()
        },
    )
    .unwrap();

    let snapshot = read_notifications_snapshot(&path, true).unwrap();
    let upserted = snapshot
        .notifications
        .iter()
        .find(|row| row.id == "upserted")
        .unwrap();
    assert!(upserted.action_data["payload"]
        .starts_with("sase-notification-blob:v1:"));
    let reconciled = snapshot
        .notifications
        .iter()
        .find(|row| row.id == "reconciled")
        .unwrap();
    assert!(reconciled.notes[0].starts_with("ove"));
    assert!(reconciled.notes[0].contains("sase-notification-blob:v1:"));
}
