//! Store generations, ack dismissals, and the lean unread index.
//!
//! Every successful store write bumps the persisted generation by one;
//! no-op writes do not. Ack dismisses completion and settlement rows for
//! the request's agent keys and reports the ids it newly dismissed with
//! the post-write generation. The lean index lists the same rows with
//! their flags under the generation they were observed at.

use super::support::*;
use sase_core::notifications::{
    ack_agent_completions, apply_notification_state_update,
    read_notifications_snapshot, read_unread_completion_index,
    reconcile_notification_rows, rewrite_notifications,
    NotificationAckRequestWire, NotificationAgentKeyWire,
    NotificationReconcileRequestWire, NotificationStateUpdateWire,
    NotificationWire,
};
use std::collections::HashMap;
use tempfile::tempdir;

fn completion_notification(
    id: &str,
    cl_name: &str,
    raw_suffix: Option<&str>,
) -> NotificationWire {
    let mut n = notification(id);
    n.sender = "user-agent".to_string();
    n.action = Some("JumpToAgent".to_string());
    n.action_data
        .insert("cl_name".to_string(), cl_name.to_string());
    if let Some(raw_suffix) = raw_suffix {
        n.action_data
            .insert("raw_suffix".to_string(), raw_suffix.to_string());
    }
    n
}

fn settlement_notification(
    id: &str,
    sender: &str,
    cl_name: &str,
    raw_suffix: Option<&str>,
) -> NotificationWire {
    let mut n = notification(id);
    n.sender = sender.to_string();
    n.action_data
        .insert("cl_name".to_string(), cl_name.to_string());
    if let Some(raw_suffix) = raw_suffix {
        n.action_data
            .insert("raw_suffix".to_string(), raw_suffix.to_string());
    }
    n
}

fn generation(path: &std::path::Path) -> u64 {
    read_notifications_snapshot(path, true).unwrap().generation
}

fn agent_key(
    cl_name: &str,
    raw_suffix: Option<&str>,
) -> NotificationAgentKeyWire {
    NotificationAgentKeyWire {
        cl_name: cl_name.to_string(),
        raw_suffix: raw_suffix.map(str::to_string),
    }
}

#[test]
fn notification_store_writes_bump_generation_and_noops_do_not() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());

    // An empty read of a missing store reports generation 0.
    assert_eq!(generation(&path), 0);

    let mut first = notification("first");
    first.read = false;
    sase_core::notifications::append_notification(&path, &first).unwrap();
    assert_eq!(generation(&path), 1);

    // A rewrite is a successful write: one more bump.
    rewrite_notifications(&path, &[first.clone()]).unwrap();
    assert_eq!(generation(&path), 2);

    // A real state update bumps once.
    let outcome = apply_notification_state_update(
        &path,
        &NotificationStateUpdateWire::MarkRead {
            id: "first".to_string(),
        },
    )
    .unwrap();
    assert_eq!(outcome.changed_count, 1);
    assert_eq!(generation(&path), 3);

    // A no-op state update writes nothing and bumps nothing.
    let outcome = apply_notification_state_update(
        &path,
        &NotificationStateUpdateWire::MarkRead {
            id: "first".to_string(),
        },
    )
    .unwrap();
    assert_eq!(outcome.changed_count, 0);
    assert_eq!(generation(&path), 3);

    // A no-op reconcile writes nothing and bumps nothing.
    let outcome = reconcile_notification_rows(
        &path,
        &NotificationReconcileRequestWire {
            notifications: Vec::new(),
            reversible_dismiss_marker_key: None,
            refresh_files: false,
        },
    )
    .unwrap();
    assert_eq!(outcome.created, 0);
    assert_eq!(outcome.updated, 0);
    assert_eq!(outcome.dismissed, 0);
    assert_eq!(outcome.resurfaced, 0);
    assert_eq!(generation(&path), 3);

    // The generation file survives reopening the path with canonical
    // contents: one decimal u64 and a trailing newline.
    assert_eq!(generation(&path), 3);
    let gen_path = path.with_file_name("notifications.jsonl.generation");
    assert_eq!(
        std::fs::read_to_string(&gen_path).unwrap(),
        "3\n".to_string()
    );
}

#[test]
fn notification_store_rejects_corrupt_generation_file() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    sase_core::notifications::append_notification(
        &path,
        &notification("first"),
    )
    .unwrap();
    assert_eq!(generation(&path), 1);

    let gen_path = path.with_file_name("notifications.jsonl.generation");
    std::fs::write(&gen_path, "not-a-number\n").unwrap();
    assert!(read_notifications_snapshot(&path, true).is_err());
    assert!(read_unread_completion_index(&path).is_err());
}

fn ack_fixture_rows() -> Vec<NotificationWire> {
    let mut empty_suffix =
        completion_notification("completion-empty-suffix", "proj", None);
    empty_suffix.read = true;
    vec![
        completion_notification(
            "completion-exact",
            "proj",
            Some("20260501010203"),
        ),
        empty_suffix,
        completion_notification(
            "completion-other-suffix",
            "proj",
            Some("20260501010204"),
        ),
        settlement_notification(
            "settle-exact",
            "epic-launch",
            "proj",
            Some("20260501010203"),
        ),
        settlement_notification(
            "settle-no-suffix",
            "epic-launch",
            "proj",
            None,
        ),
        settlement_notification(
            "settle-other-cl",
            "epic-launch",
            "elsewhere",
            Some("20260501010203"),
        ),
        settlement_notification(
            "settle-ax",
            "axe",
            "proj",
            Some("20260501010203"),
        ),
        settlement_notification(
            "unrelated",
            "remote-attention",
            "proj",
            Some("20260501010203"),
        ),
    ]
}

#[test]
fn notification_ack_dismisses_exact_rows_and_bumps_once() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    rewrite_notifications(&path, &ack_fixture_rows()).unwrap();
    let before = generation(&path);

    let outcome = ack_agent_completions(
        &path,
        &NotificationAckRequestWire {
            agents: vec![agent_key("proj", Some("20260501010203"))],
        },
    )
    .unwrap();
    // The empty-suffix completion matches on cl_name alone; the
    // cl_name-only settlement rows and the unrelated row stay active.
    assert_eq!(
        outcome.dismissed_ids,
        vec![
            "completion-exact".to_string(),
            "completion-empty-suffix".to_string(),
            "settle-exact".to_string(),
        ]
    );
    assert_eq!(outcome.generation, before + 1);
    assert_eq!(generation(&path), before + 1);

    // A repeat ack with the same keys returns no ids and the same
    // generation: no write, no bump.
    let repeat = ack_agent_completions(
        &path,
        &NotificationAckRequestWire {
            agents: vec![agent_key("proj", Some("20260501010203"))],
        },
    )
    .unwrap();
    assert!(repeat.dismissed_ids.is_empty());
    assert_eq!(repeat.generation, outcome.generation);
    assert_eq!(generation(&path), outcome.generation);

    // An empty agents request is a no-op read of the current generation.
    let empty = ack_agent_completions(
        &path,
        &NotificationAckRequestWire { agents: Vec::new() },
    )
    .unwrap();
    assert!(empty.dismissed_ids.is_empty());
    assert_eq!(empty.generation, outcome.generation);
}

#[test]
fn notification_unread_index_lists_live_completion_and_settlement_rows() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    rewrite_notifications(&path, &ack_fixture_rows()).unwrap();

    let outcome = ack_agent_completions(
        &path,
        &NotificationAckRequestWire {
            agents: vec![agent_key("proj", Some("20260501010203"))],
        },
    )
    .unwrap();

    let index = read_unread_completion_index(&path).unwrap();
    assert_eq!(index.schema_version, 1);
    assert_eq!(index.generation, outcome.generation);

    // Dismissed rows are included with their flags; rows from other
    // senders and settlement rows without a suffix stay out.
    let by_id: HashMap<_, _> = index
        .rows
        .iter()
        .map(|row| {
            (
                row.id.clone(),
                (
                    row.agent.cl_name.clone(),
                    row.agent.raw_suffix.clone(),
                    row.read,
                    row.dismissed,
                ),
            )
        })
        .collect();
    assert_eq!(
        by_id.get("completion-exact"),
        Some(&(
            "proj".to_string(),
            Some("20260501010203".to_string()),
            false,
            true
        ))
    );
    assert_eq!(
        by_id.get("completion-empty-suffix"),
        Some(&("proj".to_string(), None, true, true))
    );
    assert_eq!(
        by_id.get("completion-other-suffix"),
        Some(&(
            "proj".to_string(),
            Some("20260501010204".to_string()),
            false,
            false
        ))
    );
    assert_eq!(
        by_id.get("settle-exact"),
        Some(&(
            "proj".to_string(),
            Some("20260501010203".to_string()),
            false,
            true
        ))
    );
    // A settlement row without a suffix is not an index row at all, and
    // rows from other senders stay out.
    assert!(!by_id.contains_key("settle-no-suffix"));
    assert!(!by_id.contains_key("settle-ax"));
    assert!(!by_id.contains_key("unrelated"));
    // A live settlement row for another key is listed, still active.
    assert_eq!(
        by_id.get("settle-other-cl"),
        Some(&(
            "elsewhere".to_string(),
            Some("20260501010203".to_string()),
            false,
            false
        ))
    );
    // File order is preserved.
    let ids: Vec<_> = index.rows.iter().map(|row| row.id.as_str()).collect();
    assert_eq!(
        ids,
        vec![
            "completion-exact",
            "completion-empty-suffix",
            "completion-other-suffix",
            "settle-exact",
            "settle-other-cl",
        ]
    );
}

#[test]
fn notification_ack_and_index_generations_are_monotonic_across_threads() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    rewrite_notifications(&path, &ack_fixture_rows()).unwrap();

    let ack_path = path.clone();
    let ack_handle = std::thread::spawn(move || {
        ack_agent_completions(
            &ack_path,
            &NotificationAckRequestWire {
                agents: vec![agent_key("proj", Some("20260501010203"))],
            },
        )
        .unwrap()
    });

    let mut observations = Vec::new();
    for _ in 0..50 {
        let index = read_unread_completion_index(&path).unwrap();
        let dismissed: HashMap<String, bool> = index
            .rows
            .iter()
            .map(|row| (row.id.clone(), row.dismissed))
            .collect();
        observations.push((index.generation, dismissed));
    }
    let outcome = ack_handle.join().unwrap();

    // Every index observation at or past the ack's returned generation
    // shows that ack's ids dismissed: generations are monotonic with the
    // rows they describe.
    for (observed_generation, dismissed) in observations {
        if observed_generation >= outcome.generation {
            for id in &outcome.dismissed_ids {
                assert_eq!(
                    dismissed.get(id),
                    Some(&true),
                    "index at generation {observed_generation} hides acked {id}"
                );
            }
        }
    }
}
