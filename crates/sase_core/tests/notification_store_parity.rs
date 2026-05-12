use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::thread;

use sase_core::notifications::{
    append_notification, apply_notification_state_update,
    apply_notification_state_update_counts, read_notifications_snapshot,
    rewrite_notifications, NotificationAgentKeyWire,
    NotificationStateUpdateWire, NotificationWire,
};
use serde_json::json;
use tempfile::tempdir;

const CONTRACT_FIXTURE: &str =
    include_str!("fixtures/notifications/store_contract.jsonl");

fn store_path(root: &Path) -> PathBuf {
    root.join("notifications").join("notifications.jsonl")
}

fn notification(id: &str) -> NotificationWire {
    NotificationWire {
        id: id.to_string(),
        timestamp: "2026-05-01T01:02:03+00:00".to_string(),
        sender: "test-sender".to_string(),
        notes: Vec::new(),
        files: Vec::new(),
        action: None,
        action_data: BTreeMap::new(),
        read: false,
        dismissed: false,
        silent: false,
        muted: false,
        snooze_until: None,
    }
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
    assert!(legacy.action_data.is_empty());
    assert!(!legacy.read);
    assert!(!legacy.dismissed);
    assert!(!legacy.silent);
    assert!(!legacy.muted);
    assert_eq!(legacy.snooze_until, None);

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
    append_notification(&path, &n).unwrap();

    let mut replacement = notification("two");
    replacement.silent = true;
    rewrite_notifications(&path, &[replacement.clone()]).unwrap();

    let snapshot = read_notifications_snapshot(&path, true).unwrap();
    assert_eq!(snapshot.notifications, vec![replacement]);
    assert_eq!(snapshot.stats.loaded_rows, 1);
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
    assert_eq!(outcome.notifications.len(), 1);
    assert_eq!(outcome.notifications[0].id, "replacement");
}

#[test]
fn notification_mute_and_snooze_follow_python_semantics() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    rewrite_notifications(&path, &[notification("a")]).unwrap();

    let deadline = "2026-05-01T03:00:00+00:00".to_string();
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
fn notification_expire_snoozes_handles_aware_and_naive_timestamps() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    let mut aware = notification("aware");
    aware.muted = true;
    aware.snooze_until = Some("2026-05-01T01:00:00+00:00".to_string());
    let mut naive = notification("naive");
    naive.muted = true;
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
    let mut question = notification("question");
    question.action = Some("UserQuestion".to_string());
    question
        .action_data
        .insert("agent_cl_name".to_string(), "other".to_string());
    let untouched = notification("untouched");
    rewrite_notifications(&path, &[jump, plan, question, untouched]).unwrap();

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
    assert_eq!(outcome.changed_count, 2);
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
            "notes": [],
            "files": [],
            "action": "JumpToMentorReview",
            "action_data": {"entry_id": "2"},
            "read": false,
            "dismissed": false,
            "silent": false,
            "muted": false,
            "snooze_until": null
        })
    );
}
