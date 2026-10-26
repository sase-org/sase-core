//! Convergence of concurrent writers on a single store.
//!
//! These stay together instead of beside the behavior each one stresses
//! because they share one story — concurrent append/rewrite/upsert/expiry
//! threads converging on one valid store — and one set of thread/barrier
//! imports that would otherwise be duplicated across four files.

use super::support::*;
use sase_core::notifications::{
    append_notification, append_notification_counts,
    read_current_notifications_snapshot, read_notifications_snapshot,
    rewrite_notifications, rewrite_notifications_counts, upsert_notification,
    NotificationUpsertRequestWire,
};
use std::fs;
use std::sync::{Arc, Barrier};
use std::thread;
use tempfile::tempdir;

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
