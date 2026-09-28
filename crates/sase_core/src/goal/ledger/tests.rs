//! Tests for goal ledger I/O: ordering, stale heads, projection,
//! fail-closed stores, isolation, doctor repair, and the I/O probe.

use std::path::{Path, PathBuf};

use tempfile::TempDir;

use super::super::actions::GoalActionWire;
use super::super::wire::GOAL_LEDGER_SCHEMA_VERSION;
use super::super::wire::{GoalActorKindWire, GoalActorWire, GoalEventKindWire};
use super::append::{
    GoalLedgerAppendRequestWire, GOAL_APPEND_APPLIED, GOAL_APPEND_REFUSED,
    GOAL_APPEND_STALE_BASIS,
};
use super::layout::{goal_ledger_init, goal_marker_path};
use super::read::{GoalHistoryFilterWire, GoalListFilterWire};
use super::{goal_ledger_doctor, GoalDoctorRequestWire};

fn human() -> GoalActorWire {
    GoalActorWire {
        principal: "bryan.athena".to_string(),
        kind: GoalActorKindWire::Human,
        agent: None,
    }
}

fn setup() -> (TempDir, PathBuf) {
    let dir = TempDir::new().expect("tempdir");
    let root = dir.path().join("goals");
    goal_ledger_init(&root).expect("init");
    (dir, root)
}

fn new_request(
    title: &str,
    goal_id: &str,
    key: &str,
) -> GoalLedgerAppendRequestWire {
    GoalLedgerAppendRequestWire {
        action: GoalActionWire::New {
            title: title.to_string(),
            outcome: "the outcome".to_string(),
            criteria: Vec::new(),
            project: "sase".to_string(),
            origin: None,
            via: Some("cli".to_string()),
            idempotency_key: Some(key.to_string()),
        },
        actor: human(),
        expected_head: None,
        idempotency_key: None,
        lock_path: None,
        now: Some("2026-09-28T14:00:00.000Z".to_string()),
        new_goal_id: Some(goal_id.to_string()),
        fault_after_event_write: None,
    }
}

fn append(
    root: &Path,
    request: &GoalLedgerAppendRequestWire,
) -> super::append::GoalLedgerAppendOutcomeWire {
    super::append::goal_ledger_append(root, request).expect("append")
}

fn edit_request(
    goal_id: &str,
    title: Option<&str>,
    note: Option<&str>,
    expected_head: Option<&str>,
    key: &str,
) -> GoalLedgerAppendRequestWire {
    GoalLedgerAppendRequestWire {
        action: GoalActionWire::Edit {
            goal_id: goal_id.to_string(),
            title: title.map(str::to_string),
            outcome: None,
            criteria_added: Vec::new(),
            criteria_removed: Vec::new(),
            note: note.map(str::to_string),
            expected_head: expected_head.map(str::to_string),
            idempotency_key: Some(key.to_string()),
        },
        actor: human(),
        expected_head: None,
        idempotency_key: None,
        lock_path: None,
        now: Some("2026-09-28T14:01:00.000Z".to_string()),
        new_goal_id: None,
        fault_after_event_write: None,
    }
}

fn drop_request(
    goal_id: &str,
    expected_head: Option<&str>,
    key: &str,
) -> GoalLedgerAppendRequestWire {
    GoalLedgerAppendRequestWire {
        action: GoalActionWire::Drop {
            goal_id: goal_id.to_string(),
            why: "no longer needed".to_string(),
            expected_head: expected_head.map(str::to_string),
            idempotency_key: Some(key.to_string()),
        },
        actor: human(),
        expected_head: None,
        idempotency_key: None,
        lock_path: None,
        now: Some("2026-09-28T14:02:00.000Z".to_string()),
        new_goal_id: None,
        fault_after_event_write: None,
    }
}

#[test]
fn append_new_creates_marker_and_lists_hot() {
    let (_tmp, root) = setup();
    let outcome = append(&root, &new_request("First goal", "7k2mq", "k1"));
    assert_eq!(outcome.status, GOAL_APPEND_APPLIED);
    assert_eq!(outcome.events.len(), 1);
    assert!(goal_marker_path(&root, "7k2mq").exists());
    assert!(outcome
        .created_paths
        .iter()
        .any(|path| path.contains("7k2mq")));

    let list = super::read::goal_ledger_list(
        &root,
        &GoalListFilterWire {
            status: None,
            limit: None,
        },
        None,
    )
    .expect("list");
    assert_eq!(list.goals.len(), 1);
    assert_eq!(list.goals[0].id, "7k2mq");
    assert_eq!(list.stale_markers, 0);
}

#[test]
fn drop_removes_marker_after_event_write() {
    let (_tmp, root) = setup();
    let created = append(&root, &new_request("Gone goal", "7k2mq", "k1"));
    assert_eq!(created.status, GOAL_APPEND_APPLIED);
    let dropped = append(&root, &drop_request("7k2mq", None, "k2"));
    assert_eq!(dropped.status, GOAL_APPEND_APPLIED);
    assert!(!goal_marker_path(&root, "7k2mq").exists());
    assert!(dropped
        .removed_paths
        .iter()
        .any(|path| path.contains("7k2mq")));

    let list = super::read::goal_ledger_list(
        &root,
        &GoalListFilterWire {
            status: None,
            limit: None,
        },
        None,
    )
    .expect("list");
    assert!(list.goals.is_empty());

    let history = super::read::goal_ledger_history(
        &root,
        &GoalHistoryFilterWire {
            status: Some("dropped".to_string()),
            limit: Some(20),
        },
        None,
    )
    .expect("history");
    assert_eq!(history.goals.len(), 1);
    assert_eq!(history.goals[0].id, "7k2mq");
}

#[test]
fn marker_superset_holds_under_fault_hook() {
    let (_tmp, root) = setup();
    let mut request = new_request("Crash goal", "7k2mq", "k1");
    request.fault_after_event_write = Some(true);
    let outcome = append(&root, &request);
    assert_eq!(outcome.status, GOAL_APPEND_APPLIED);

    // The event is durable and the pre-created marker keeps the
    // marker-superset invariant, so the hot read stays correct.
    let list = super::read::goal_ledger_list(
        &root,
        &GoalListFilterWire {
            status: None,
            limit: None,
        },
        None,
    )
    .expect("list");
    assert_eq!(list.goals.len(), 1);

    // A fault between event and marker steps on a settlement leaves an
    // extra marker; the next append still converges it.
    let mut settle = drop_request("7k2mq", None, "k2");
    settle.fault_after_event_write = Some(true);
    let faulted = append(&root, &settle);
    assert_eq!(faulted.status, GOAL_APPEND_APPLIED);
    assert!(goal_marker_path(&root, "7k2mq").exists());
    let list = super::read::goal_ledger_list(
        &root,
        &GoalListFilterWire {
            status: None,
            limit: None,
        },
        None,
    )
    .expect("list");
    assert!(list.goals.is_empty());
    assert_eq!(list.stale_markers, 1);

    // Doctor repairs the leftover marker idempotently.
    let scope = GoalDoctorRequestWire {
        repair: true,
        ..Default::default()
    };
    let first = goal_ledger_doctor(&root, &scope).expect("doctor repair");
    assert!(!goal_marker_path(&root, "7k2mq").exists());
    assert!(first.changed_paths.iter().any(|p| p.contains("7k2mq")));
    let second =
        goal_ledger_doctor(&root, &scope).expect("doctor repair again");
    assert!(second.changed_paths.is_empty());
}

#[test]
fn stale_basis_refuses_noncommutative_and_replans_commutative() {
    let (_tmp, root) = setup();
    let created = append(&root, &new_request("Edit me", "7k2mq", "k1"));
    assert_eq!(created.status, GOAL_APPEND_APPLIED);
    let head = created.states[0].head.clone().expect("head");

    // A title edit against a moved head is not commutative.
    let refused = append(
        &root,
        &edit_request(
            "7k2mq",
            Some("New title"),
            None,
            Some("bogus-head"),
            "k2",
        ),
    );
    assert_eq!(refused.status, GOAL_APPEND_STALE_BASIS);
    assert!(!refused.states.is_empty());

    // A note-only edit against a moved head re-plans and proceeds.
    let note = append(
        &root,
        &edit_request("7k2mq", None, Some("a note"), Some("bogus"), "k3"),
    );
    assert_eq!(note.status, GOAL_APPEND_APPLIED);

    // A drop against a stale head reports stale_basis with current state.
    let stale_drop =
        append(&root, &drop_request("7k2mq", Some(head.as_str()), "k4"));
    assert_eq!(stale_drop.status, GOAL_APPEND_STALE_BASIS);

    // The same drop against the current head applies.
    let shown =
        super::read::goal_ledger_show(&root, "7k2mq", None).expect("show");
    let fresh_drop =
        append(&root, &drop_request("7k2mq", shown.head.as_deref(), "k5"));
    assert_eq!(fresh_drop.status, GOAL_APPEND_APPLIED);
}

#[test]
fn refused_edit_reports_code() {
    let (_tmp, root) = setup();
    let created = append(&root, &new_request("Settled", "7k2mq", "k1"));
    assert_eq!(created.status, GOAL_APPEND_APPLIED);
    let dropped = append(&root, &drop_request("7k2mq", None, "k2"));
    assert_eq!(dropped.status, GOAL_APPEND_APPLIED);
    let edit = append(
        &root,
        &edit_request("7k2mq", Some("late title"), None, None, "k3"),
    );
    assert_eq!(edit.status, GOAL_APPEND_REFUSED);
    assert!(edit.code.is_some());
}

#[test]
fn projection_invalidates_by_signature() {
    let (_tmp, root) = setup();
    append(&root, &new_request("Proj goal", "7k2mq", "k1"));
    let dir = TempDir::new().expect("tempdir");
    let projection = dir.path().join("goals-hot.json");

    let first = super::projection::refresh_goal_projection(
        &root,
        &projection,
        "sase",
        "local",
        "",
        "",
        super::projection::GOAL_DEFAULT_FETCH_TTL_SECONDS,
    )
    .expect("refresh");
    assert!(first.wrote);
    assert_eq!(first.projection.goals.len(), 1);

    let status = super::projection::goal_projection_status(&root, &projection)
        .expect("status");
    assert_eq!(
        status.status,
        super::projection::GoalProjectionStatusNameWire::Fresh
    );

    // A second refresh with no changes skips the write.
    let second = super::projection::refresh_goal_projection(
        &root,
        &projection,
        "sase",
        "local",
        "",
        "",
        super::projection::GOAL_DEFAULT_FETCH_TTL_SECONDS,
    )
    .expect("refresh again");
    assert!(!second.wrote);

    // A new event changes the signature and the projection goes stale.
    let shown =
        super::read::goal_ledger_show(&root, "7k2mq", None).expect("show");
    let _ = append(&root, &drop_request("7k2mq", shown.head.as_deref(), "k2"));
    let status = super::projection::goal_projection_status(&root, &projection)
        .expect("status after drop");
    assert_eq!(
        status.status,
        super::projection::GoalProjectionStatusNameWire::Stale
    );
    let third = super::projection::refresh_goal_projection(
        &root,
        &projection,
        "sase",
        "local",
        "",
        "",
        super::projection::GOAL_DEFAULT_FETCH_TTL_SECONDS,
    )
    .expect("refresh after drop");
    assert!(third.wrote);
    assert!(third.projection.goals.is_empty());
}

#[test]
fn unsupported_store_fails_closed() {
    let (_tmp, root) = setup();
    let store_path = root.join("STORE.json");
    std::fs::write(
        &store_path,
        r#"{"schema_version":2,"layout":"sase-goal-ledger","created_at":"t"}"#,
    )
    .expect("write store");
    let list = super::read::goal_ledger_list(
        &root,
        &GoalListFilterWire {
            status: None,
            limit: None,
        },
        None,
    );
    assert!(list.is_err());
    assert!(list.unwrap_err().to_string().contains("sase update"));
    let append_result = super::append::goal_ledger_append(
        &root,
        &new_request("Blocked", "7k2mq", "k1"),
    );
    assert!(append_result.is_err());
    let _ = GOAL_LEDGER_SCHEMA_VERSION;
}

#[test]
fn unreadable_goal_isolated_from_neighbors() {
    let (_tmp, root) = setup();
    append(&root, &new_request("Good one", "7k2mq", "k1"));
    append(&root, &new_request("Bad one", "3fq9t", "k2"));
    // Inject an unknown event kind directly: only that goal goes
    // unreadable, and its neighbors still render.
    let events_dir = root.join("items").join("3fq9t").join("events");
    std::fs::write(
        events_dir.join("zzzzzzzzzzzzzzzzzzzzzzzzzz.json"),
        r#"{"schema_version":1,"event_id":"zzzzzzzzzzzzzzzzzzzzzzzzzz","goal_id":"3fq9t","kind":"time_travel","at":"2026-09-28T14:00:00.000Z","actor":{"principal":"x.y","kind":"human"},"basis":null,"idempotency_key":"evil","payload":{}}"#,
    )
    .expect("inject");
    let shown =
        super::read::goal_ledger_show(&root, "3fq9t", None).expect("show bad");
    assert!(!shown.readable);
    let good =
        super::read::goal_ledger_show(&root, "7k2mq", None).expect("show good");
    assert!(good.readable);
    let list = super::read::goal_ledger_list(
        &root,
        &GoalListFilterWire {
            status: Some("all".to_string()),
            limit: None,
        },
        None,
    )
    .expect("list");
    assert_eq!(list.goals.len(), 2);
}

#[test]
fn doctor_repairs_missing_markers() {
    let (_tmp, root) = setup();
    append(&root, &new_request("Lost marker", "7k2mq", "k1"));
    std::fs::remove_file(goal_marker_path(&root, "7k2mq"))
        .expect("remove marker");
    let report = goal_ledger_doctor(
        &root,
        &GoalDoctorRequestWire {
            repair: false,
            ..Default::default()
        },
    )
    .expect("doctor");
    assert!(!report.ok);
    assert_eq!(report.missing_markers, vec!["7k2mq".to_string()]);
    let repaired = goal_ledger_doctor(
        &root,
        &GoalDoctorRequestWire {
            repair: true,
            ..Default::default()
        },
    )
    .expect("repair");
    assert!(goal_marker_path(&root, "7k2mq").exists());
    assert!(repaired.changed_paths.iter().any(|p| p.contains("7k2mq")));
}

#[test]
fn probe_shows_zero_settled_opens() {
    let (_tmp, root) = setup();
    // Seed settled history plus a few live goals.
    for index in 0..20 {
        let id = format!("s{index:04}");
        append(
            &root,
            &new_request(
                &format!("Settled {index}"),
                &id,
                &format!("k{index}"),
            ),
        );
        let shown =
            super::read::goal_ledger_show(&root, &id, None).expect("show");
        let dropped = append(
            &root,
            &drop_request(&id, shown.head.as_deref(), &format!("d{index}")),
        );
        assert_eq!(dropped.status, GOAL_APPEND_APPLIED);
    }
    for index in 0..3 {
        let id = format!("a{index:04}");
        let created = append(
            &root,
            &new_request(&format!("Live {index}"), &id, &format!("q{index}")),
        );
        assert_eq!(created.status, GOAL_APPEND_APPLIED);
    }
    let probe = super::probe::probe_goal_ledger_list(
        &root,
        &GoalListFilterWire {
            status: None,
            limit: None,
        },
    )
    .expect("probe");
    assert_eq!(probe.list.goals.len(), 3);
    assert_eq!(probe.counts.settled_event_opens, 0);
    assert!(!probe.counts.history_scan);
    // Only live goals' events were opened.
    assert!(probe.counts.event_files_opened >= 3);
    assert!(probe.counts.event_files_opened < 20);
}

#[test]
fn unknown_kind_wire_parses_as_unsupported() {
    let kind: GoalEventKindWire =
        serde_json::from_str(r#""time_travel""#).expect("parse kind");
    assert!(matches!(kind, GoalEventKindWire::Unsupported(_)));
}

fn reopen_request(goal_id: &str, key: &str) -> GoalLedgerAppendRequestWire {
    GoalLedgerAppendRequestWire {
        action: GoalActionWire::Reopen {
            goal_id: goal_id.to_string(),
            message: "another pass".to_string(),
            expected_head: None,
            idempotency_key: Some(key.to_string()),
        },
        actor: human(),
        expected_head: None,
        idempotency_key: None,
        lock_path: None,
        now: Some("2026-09-28T14:03:00.000Z".to_string()),
        new_goal_id: None,
        fault_after_event_write: None,
    }
}

fn merge_request(
    source: &str,
    target: &str,
    key: &str,
) -> GoalLedgerAppendRequestWire {
    GoalLedgerAppendRequestWire {
        action: GoalActionWire::Merge {
            source_id: source.to_string(),
            target_id: target.to_string(),
            target_state: None,
            why: Some("same thread".to_string()),
            expected_head: None,
            idempotency_key: Some(key.to_string()),
        },
        actor: human(),
        expected_head: None,
        idempotency_key: None,
        lock_path: None,
        now: Some("2026-09-28T14:04:00.000Z".to_string()),
        new_goal_id: None,
        fault_after_event_write: None,
    }
}

fn assert_no_trace(root: &Path, goal_id: &str) {
    assert!(
        !root.join("items").join(goal_id).exists(),
        "refused append writes no items/{goal_id}"
    );
    assert!(
        !goal_marker_path(root, goal_id).exists(),
        "refused append writes no live/{goal_id}"
    );
}

#[test]
fn unknown_ids_refuse_without_writing() {
    let (_tmp, root) = setup();
    let created = append(&root, &new_request("Real goal", "7k2mq", "k1"));
    assert_eq!(created.status, GOAL_APPEND_APPLIED);

    let edit = append(
        &root,
        &edit_request("zzzzz", Some("Ghost edit"), None, None, "k2"),
    );
    assert_eq!(edit.status, GOAL_APPEND_REFUSED);
    assert_eq!(edit.code.as_deref(), Some("goal_not_found"));
    assert_no_trace(&root, "zzzzz");

    let dropped = append(&root, &drop_request("zzzzz", None, "k3"));
    assert_eq!(dropped.status, GOAL_APPEND_REFUSED);
    assert_eq!(dropped.code.as_deref(), Some("goal_not_found"));
    assert_no_trace(&root, "zzzzz");

    let reopened = append(&root, &reopen_request("zzzzz", "k4"));
    assert_eq!(reopened.status, GOAL_APPEND_REFUSED);
    assert_eq!(reopened.code.as_deref(), Some("goal_not_found"));
    assert_no_trace(&root, "zzzzz");

    let bad_source = append(&root, &merge_request("zzzzz", "7k2mq", "k5"));
    assert_eq!(bad_source.status, GOAL_APPEND_REFUSED);
    assert_eq!(bad_source.code.as_deref(), Some("goal_not_found"));
    assert_no_trace(&root, "zzzzz");

    let bad_target = append(&root, &merge_request("7k2mq", "zzzzz", "k6"));
    assert_eq!(bad_target.status, GOAL_APPEND_REFUSED);
    assert_eq!(bad_target.code.as_deref(), Some("target_not_found"));
    assert_no_trace(&root, "zzzzz");

    // The existing goal is untouched by every refusal above.
    let shown =
        super::read::goal_ledger_show(&root, "7k2mq", None).expect("show");
    assert_eq!(shown.title, "Real goal");

    // A `new` that reuses a live id refuses instead of forking it.
    let events_dir = root.join("items").join("7k2mq").join("events");
    let before = std::fs::read_dir(&events_dir).expect("list").count();
    let duplicate = append(&root, &new_request("Fork", "7k2mq", "k7"));
    assert_eq!(duplicate.status, GOAL_APPEND_REFUSED);
    assert_eq!(duplicate.code.as_deref(), Some("goal_already_exists"));
    let after = std::fs::read_dir(&events_dir).expect("list").count();
    assert_eq!(before, after);
    assert!(goal_marker_path(&root, "7k2mq").exists());
}

#[test]
fn normalized_edit_writes_only_under_the_canonical_id() {
    let (_tmp, root) = setup();
    let created = append(&root, &new_request("Cased goal", "7k2mq", "k1"));
    assert_eq!(created.status, GOAL_APPEND_APPLIED);

    let edited = append(
        &root,
        &edit_request("7K2MQ", Some("Retitled"), None, None, "k2"),
    );
    assert_eq!(edited.status, GOAL_APPEND_APPLIED);
    assert_eq!(edited.events[0].goal_id, "7k2mq");
    assert!(!root.join("items").join("7K2MQ").exists());
    assert!(!goal_marker_path(&root, "7K2MQ").exists());

    let shown =
        super::read::goal_ledger_show(&root, "7k2mq", None).expect("show");
    assert_eq!(shown.title, "Retitled");
}

#[test]
fn merge_writes_normalized_ids_for_into_and_from() {
    let (_tmp, root) = setup();
    append(&root, &new_request("Source goal", "7k2mq", "k1"));
    append(&root, &new_request("Target goal", "3fq9t", "k2"));

    let merged = append(&root, &merge_request("7K2MQ", "3FQ9T", "k3"));
    assert_eq!(merged.status, GOAL_APPEND_APPLIED);
    assert!(merged
        .events
        .iter()
        .all(|event| event.goal_id == "7k2mq" || event.goal_id == "3fq9t"));
    assert!(!root.join("items").join("7K2MQ").exists());
    assert!(!root.join("items").join("3FQ9T").exists());

    let source =
        super::read::goal_ledger_show(&root, "7k2mq", None).expect("show");
    assert_eq!(source.merged_into.as_deref(), Some("3fq9t"));
    let target =
        super::read::goal_ledger_show(&root, "3fq9t", None).expect("show");
    assert_eq!(target.merged_from, vec!["7k2mq".to_string()]);
}

#[test]
fn merge_with_a_normalized_self_refuses() {
    let (_tmp, root) = setup();
    let created = append(&root, &new_request("Solo goal", "7k2mq", "k1"));
    assert_eq!(created.status, GOAL_APPEND_APPLIED);

    let merged = append(&root, &merge_request("7K2MQ", "7k2mq", "k2"));
    assert_eq!(merged.status, GOAL_APPEND_REFUSED);
    assert_eq!(merged.code.as_deref(), Some("merge_into_self"));

    let shown =
        super::read::goal_ledger_show(&root, "7k2mq", None).expect("show");
    assert_eq!(shown.status, crate::goal::wire::GoalStatusWire::Active);
}

#[test]
fn corrupt_event_file_isolates_one_goal() {
    let (_tmp, root) = setup();
    append(&root, &new_request("Good one", "7k2mq", "k1"));
    append(&root, &new_request("Bad one", "3fq9t", "k2"));
    std::fs::write(
        root.join("items")
            .join("3fq9t")
            .join("events")
            .join("corrupt.json"),
        b"{ this is not json",
    )
    .expect("inject corruption");

    let list = super::read::goal_ledger_list(
        &root,
        &GoalListFilterWire {
            status: Some("all".to_string()),
            limit: None,
        },
        None,
    )
    .expect("list keeps going");
    assert_eq!(list.goals.len(), 2);
    let bad = list
        .goals
        .iter()
        .find(|state| state.id == "3fq9t")
        .expect("bad goal");
    assert!(!bad.readable);
    let reason = bad.unreadable_reason.as_deref().unwrap_or_default();
    assert!(reason.contains("unparseable_event"), "reason: {reason}");
    assert!(reason.contains("corrupt.json"), "reason: {reason}");
    let good = list
        .goals
        .iter()
        .find(|state| state.id == "7k2mq")
        .expect("good");
    assert!(good.readable);

    let shown =
        super::read::goal_ledger_show(&root, "3fq9t", None).expect("show bad");
    assert!(!shown.readable);
    let healthy =
        super::read::goal_ledger_show(&root, "7k2mq", None).expect("show good");
    assert!(healthy.readable);

    let history = super::read::goal_ledger_history(
        &root,
        &GoalHistoryFilterWire {
            status: Some("all".to_string()),
            limit: Some(20),
        },
        None,
    )
    .expect("history keeps going");
    assert!(history.goals.iter().any(|state| state.id == "3fq9t"));

    let report = goal_ledger_doctor(
        &root,
        &GoalDoctorRequestWire {
            repair: false,
            ..Default::default()
        },
    )
    .expect("doctor keeps going");
    assert!(!report.ok);
    assert_eq!(report.unreadable, vec!["3fq9t".to_string()]);
    assert!(report.checks.iter().any(|check| check.code == "unreadable"
        && check.goal_id.as_deref() == Some("3fq9t")));
}

#[test]
fn corrupt_projection_reports_and_rebuilds() {
    let (_tmp, root) = setup();
    append(&root, &new_request("Proj goal", "7k2mq", "k1"));
    let dir = TempDir::new().expect("tempdir");
    let projection = dir.path().join("goals-hot.json");
    std::fs::write(&projection, b"\x00\x01 garbage bytes").expect("inject");

    let status = super::projection::goal_projection_status(&root, &projection)
        .expect("status reports instead of erroring");
    assert_eq!(
        status.status,
        super::projection::GoalProjectionStatusNameWire::SchemaMismatch
    );

    let rebuilt = super::projection::refresh_goal_projection(
        &root,
        &projection,
        "sase",
        "local",
        "",
        "",
        super::projection::GOAL_DEFAULT_FETCH_TTL_SECONDS,
    )
    .expect("refresh rebuilds");
    assert!(rebuilt.wrote);
    assert_eq!(rebuilt.projection.goals.len(), 1);
    let status = super::projection::goal_projection_status(&root, &projection)
        .expect("status after rebuild");
    assert_eq!(
        status.status,
        super::projection::GoalProjectionStatusNameWire::Fresh
    );
}

#[test]
fn doctor_repair_keeps_the_projection_header() {
    let (_tmp, root) = setup();
    append(&root, &new_request("Header goal", "7k2mq", "k1"));
    let dir = TempDir::new().expect("tempdir");
    let projection = dir.path().join("goals-hot.json");
    let seeded = super::projection::refresh_goal_projection(
        &root,
        &projection,
        "sase",
        "shared",
        "wm-path",
        "ob-path",
        30.0,
    )
    .expect("seed projection");
    assert!(seeded.wrote);

    // Break a marker so the repair has marker work plus a stale
    // projection to rebuild.
    std::fs::remove_file(goal_marker_path(&root, "7k2mq"))
        .expect("remove marker");
    let repaired = goal_ledger_doctor(
        &root,
        &GoalDoctorRequestWire {
            repair: true,
            projection_path: Some(projection.display().to_string()),
            ..Default::default()
        },
    )
    .expect("repair");
    assert!(repaired.changed_paths.iter().any(|p| p.contains("7k2mq")));
    assert!(goal_marker_path(&root, "7k2mq").exists());

    let body = std::fs::read_to_string(&projection).expect("read rebuilt");
    let value: serde_json::Value =
        serde_json::from_str(&body).expect("rebuilt parses");
    assert_eq!(value["project"], serde_json::json!("sase"));
    assert_eq!(value["mode"], serde_json::json!("shared"));
    assert_eq!(value["watermark_path"], serde_json::json!("wm-path"));
    assert_eq!(value["outbox_path"], serde_json::json!("ob-path"));
    assert_eq!(value["fetch_ttl_seconds"], serde_json::json!(30.0));
}

fn write_direct_event(
    root: &Path,
    goal_id: &str,
    event_id: &str,
    kind: &str,
    basis: Option<&str>,
    payload: serde_json::Value,
) {
    let dir = root.join("items").join(goal_id).join("events");
    std::fs::create_dir_all(&dir).expect("events dir");
    let body = serde_json::json!({
        "schema_version": 1,
        "event_id": event_id,
        "goal_id": goal_id,
        "kind": kind,
        "at": "2026-09-28T14:00:00.000Z",
        "actor": {"principal": "t.t", "kind": "human"},
        "basis": basis,
        "idempotency_key": format!("k:{event_id}"),
        "payload": payload,
    });
    std::fs::write(
        dir.join(format!("{event_id}.json")),
        serde_json::to_string(&body).unwrap(),
    )
    .expect("write event");
}

#[test]
fn probe_counts_real_opens_across_a_thousand_goals() {
    let (_tmp, root) = setup();
    for index in 0..1000 {
        let id = format!("s{index:04}");
        let create = format!("1{index:025}");
        let settle = format!("2{index:025}");
        write_direct_event(
            &root,
            &id,
            &create,
            "created",
            None,
            serde_json::json!({
                "title": format!("Settled {index}"),
                "outcome": "done",
                "criteria": [],
                "draft": false,
                "project": "sase",
            }),
        );
        write_direct_event(
            &root,
            &id,
            &settle,
            "settled",
            Some(&create),
            serde_json::json!({"flavor": "canceled"}),
        );
    }
    for index in 0..10 {
        let id = format!("a{index:04}");
        let create = format!("3{index:025}");
        write_direct_event(
            &root,
            &id,
            &create,
            "created",
            None,
            serde_json::json!({
                "title": format!("Live {index}"),
                "outcome": "open",
                "criteria": [],
                "draft": false,
                "project": "sase",
            }),
        );
        std::fs::write(goal_marker_path(&root, &id), b"").expect("marker");
    }

    let probe = super::probe::probe_goal_ledger_list(
        &root,
        &GoalListFilterWire {
            status: None,
            limit: None,
        },
    )
    .expect("probe");
    assert_eq!(probe.list.goals.len(), 10);
    assert_eq!(probe.counts.settled_event_opens, 0);
    assert_eq!(probe.counts.event_dir_stats, 10);
    assert_eq!(probe.counts.store_reads, 1);
    assert!(!probe.counts.history_scan);

    // The history scan is the negative control: it opens settled
    // directories by design, and the probe counts them.
    let scanned = super::probe::probe_goal_ledger_history(
        &root,
        &GoalHistoryFilterWire {
            status: Some("all".to_string()),
            limit: Some(20),
        },
    )
    .expect("probed history");
    assert!(scanned.counts.history_scan);
    assert_eq!(scanned.counts.settled_event_opens, 1000);
}
