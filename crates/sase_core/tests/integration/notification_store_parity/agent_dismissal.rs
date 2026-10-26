//! Dismiss-matching-agents and agent-completion dismissal.
//!
//! Covers notification action shapes, question root/child identity, custom
//! gates, user-agent view error reports, and exact-settlement-row matching.

use super::support::*;
use sase_core::notifications::{
    apply_notification_state_update, rewrite_notifications,
    NotificationAgentKeyWire, NotificationStateUpdateWire, NotificationWire,
};
use tempfile::tempdir;

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

fn settlement_notification(
    id: &str,
    sender: &str,
    cl_name: &str,
    raw_suffix: Option<&str>,
) -> NotificationWire {
    let mut n = notification(id);
    n.sender = sender.to_string();
    n.action = None;
    n.action_data
        .insert("cl_name".to_string(), cl_name.to_string());
    if let Some(raw_suffix) = raw_suffix {
        n.action_data
            .insert("raw_suffix".to_string(), raw_suffix.to_string());
    }
    n
}

fn settlement_fixture_rows() -> Vec<NotificationWire> {
    let mut completion = notification("completion");
    completion.sender = "user-agent".to_string();
    completion.action = Some("JumpToAgent".to_string());
    completion
        .action_data
        .insert("cl_name".to_string(), "proj".to_string());
    completion
        .action_data
        .insert("raw_suffix".to_string(), "20260501010203".to_string());

    let mut already = settlement_notification(
        "settle-already",
        "epic-launch",
        "proj",
        Some("20260501010203"),
    );
    already.dismissed = true;

    vec![
        settlement_notification(
            "settle-epic",
            "epic-launch",
            "proj",
            Some("20260501010203"),
        ),
        settlement_notification(
            "settle-monitor",
            "monitor-settlement",
            "proj",
            Some("20260501010203"),
        ),
        settlement_notification(
            "settle-other-suffix",
            "epic-launch",
            "proj",
            Some("20260501010204"),
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
        already,
        completion,
        settlement_notification("axe", "axe", "proj", Some("20260501010203")),
        settlement_notification("crs", "crs", "proj", Some("20260501010203")),
    ]
}

#[test]
fn notification_dismiss_agent_completions_matching_agents_matches_exact_settlement_rows(
) {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    rewrite_notifications(&path, &settlement_fixture_rows()).unwrap();

    let outcome = apply_notification_state_update(
        &path,
        &NotificationStateUpdateWire::DismissAgentCompletionsMatchingAgents {
            agents: vec![NotificationAgentKeyWire {
                cl_name: "proj".to_string(),
                raw_suffix: Some("20260501010203".to_string()),
            }],
        },
    )
    .unwrap();
    assert_eq!(outcome.matched_count, 3);
    assert_eq!(outcome.changed_count, 3);

    let by_id: std::collections::HashMap<_, _> = outcome
        .notifications
        .iter()
        .map(|n| (n.id.clone(), n.dismissed))
        .collect();
    assert_eq!(by_id.get("settle-epic"), Some(&true));
    assert_eq!(by_id.get("settle-monitor"), Some(&true));
    assert_eq!(by_id.get("completion"), Some(&true));
    // Different raw_suffix under the same cl_name stays active.
    assert_eq!(by_id.get("settle-other-suffix"), Some(&false));
    // No cl_name-only fallback for settlement rows without a raw_suffix.
    assert_eq!(by_id.get("settle-no-suffix"), Some(&false));
    assert_eq!(by_id.get("settle-other-cl"), Some(&false));
    assert_eq!(by_id.get("axe"), Some(&false));
    assert_eq!(by_id.get("crs"), Some(&false));
    // Already dismissed rows stay dismissed and are not counted above.
    assert_eq!(by_id.get("settle-already"), Some(&true));
}

#[test]
fn notification_dismiss_agent_completions_matching_agents_skips_unmatched_settlement_rows(
) {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    rewrite_notifications(&path, &settlement_fixture_rows()).unwrap();

    let outcome = apply_notification_state_update(
        &path,
        &NotificationStateUpdateWire::DismissAgentCompletionsMatchingAgents {
            agents: vec![NotificationAgentKeyWire {
                cl_name: "proj".to_string(),
                raw_suffix: Some("20269999999999".to_string()),
            }],
        },
    )
    .unwrap();
    assert_eq!(outcome.matched_count, 0);
    assert_eq!(outcome.changed_count, 0);
    assert!(!outcome.rewritten);

    let outcome = apply_notification_state_update(
        &path,
        &NotificationStateUpdateWire::DismissAgentCompletionsMatchingAgents {
            agents: vec![],
        },
    )
    .unwrap();
    assert_eq!(outcome.matched_count, 0);
}

#[test]
fn notification_dismiss_agent_completions_leaves_settlement_rows_alone() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    rewrite_notifications(&path, &settlement_fixture_rows()).unwrap();

    let outcome = apply_notification_state_update(
        &path,
        &NotificationStateUpdateWire::DismissAgentCompletions,
    )
    .unwrap();
    assert_eq!(outcome.matched_count, 1);
    let by_id: std::collections::HashMap<_, _> = outcome
        .notifications
        .iter()
        .map(|n| (n.id.clone(), n.dismissed))
        .collect();
    assert_eq!(by_id.get("completion"), Some(&true));
    assert_eq!(by_id.get("settle-epic"), Some(&false));
    assert_eq!(by_id.get("settle-monitor"), Some(&false));
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
