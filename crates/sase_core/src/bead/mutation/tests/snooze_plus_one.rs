use super::super::*;
use super::support::*;
use crate::bead::events::BeadEventOperationWire;
use crate::bead::events::BeadEventPayloadWire;
use crate::bead::jsonl::read_event_store;
use crate::bead::jsonl::write_event_store;
use crate::bead::mutation::store::mutation_status_value;
use crate::bead::mutation::store::MutableStore;
use crate::bead::read::read_store_issues;
use crate::bead::wire::BeadResolutionWire;
use crate::bead::wire::IssueTypeWire;
use crate::bead::wire::StatusWire;
use std::sync::Arc;
use std::sync::Barrier;
use std::thread;
use tempfile::tempdir;

use crate::bead::events::reduce_event_streams;
use crate::bead::jsonl::import_issues_from_jsonl;
use crate::bead::mutation::plus_one_snooze::deferral_length_label;
use crate::bead::wire::BeadReopenCauseWire;
use std::collections::BTreeSet;
#[test]
fn snooze_task_records_wake_conditions_and_replays_from_events() {
    let (_temp, beads_dir, task_id) =
        snoozed_task_fixture("2026-01-04T09:00:00-05:00", Some(2));

    let issue = read_store_issues(&beads_dir)
        .unwrap()
        .into_iter()
        .find(|issue| issue.id == task_id)
        .unwrap();
    assert_eq!(issue.status, StatusWire::Snoozed);
    let snooze = issue.snooze.clone().unwrap();
    assert_eq!(snooze.until, "2026-01-04T09:00:00-05:00");
    assert_eq!(snooze.snoozed_at, "2026-01-01T00:02:00Z");
    assert_eq!(snooze.snoozed_by, "bryanbugyi34@gmail.com");
    assert_eq!(snooze.reason, "needs the upstream fix first");
    assert_eq!(snooze.plus_one_baseline, Some(0));
    assert_eq!(snooze.plus_one_target, Some(2));
    // The wake time is 3d13h58m after the snoozed_at fixture timestamp,
    // which the deferral-length ladder rounds up to `4d` — asserting the
    // value the ladder actually produces rather than reverse-engineering
    // it to read `3d`.
    assert_eq!(
            note_text(&issue),
            "[2026-01-01T00:02:00Z · bryanbugyi34@gmail.com] Snoozed until 2026-01-04T09:00:00-05:00 (in 4d). Also wakes at 2 more +1s. Reason: needs the upstream fix first"
        );

    assert_eq!(reduces_to_store(&beads_dir), vec![issue.clone()]);
    // The generated projection carries the record too, so a reader that
    // only has issues.jsonl still sees the wake conditions.
    let projected =
        import_issues_from_jsonl(&beads_dir.join("issues.jsonl")).unwrap();
    assert_eq!(projected.issues, vec![issue]);
}

#[test]
fn snooze_task_with_no_reason_or_target_still_names_the_wake_conditions() {
    let (_temp, beads_dir, task_id) = task_plus_one_fixture(StatusWire::Open);

    let issue = snooze_task(
        &beads_dir,
        &task_id,
        "2026-01-01T00:32:00Z",
        None,
        "",
        "owner",
        Some("2026-01-01T00:02:00Z".to_string()),
    )
    .unwrap()
    .issue
    .unwrap();

    assert_eq!(
            note_text(&issue),
            "[2026-01-01T00:02:00Z · owner] Snoozed until 2026-01-01T00:32:00Z (in 30m)."
        );
}

#[test]
fn re_snoozing_appends_a_second_note_naming_the_replaced_wake_time() {
    let (_temp, beads_dir, task_id) =
        snoozed_task_fixture("2026-01-04T00:00:00Z", Some(5));
    add_task_plus_one(
        &beads_dir,
        &task_id,
        "reporter-one",
        "hit this too",
        &[],
        Some("2026-01-02T00:00:00Z".to_string()),
        None,
    )
    .unwrap();

    let issue = snooze_task(
        &beads_dir,
        &task_id,
        "2026-01-11T00:00:00Z",
        Some(2),
        "still waiting",
        "bryanbugyi34@gmail.com",
        Some("2026-01-03T00:00:00Z".to_string()),
    )
    .unwrap()
    .issue
    .unwrap();

    assert_eq!(issue.status, StatusWire::Snoozed);
    let note_blob = note_text(&issue);
    let notes: Vec<&str> = note_blob.split("\n\n").collect();
    assert_eq!(notes.len(), 2);
    assert!(notes[0].contains("Snoozed until 2026-01-04T00:00:00Z"));
    assert_eq!(
            notes[1],
            "[2026-01-03T00:00:00Z · bryanbugyi34@gmail.com] Re-snoozed until 2026-01-11T00:00:00Z (in 8d), replacing the wake time 2026-01-04T00:00:00Z. Also wakes at 2 more +1s (3 total). Reason: still waiting"
        );
    assert_eq!(reduces_to_store(&beads_dir), vec![issue]);
}

#[test]
fn deferral_length_label_buckets_and_rounds_half_away_from_zero() {
    assert_eq!(deferral_length_label(1), "1s");
    assert_eq!(deferral_length_label(59), "59s");
    assert_eq!(deferral_length_label(60), "1m");
    assert_eq!(deferral_length_label(90), "2m");
    assert_eq!(deferral_length_label(89), "1m");
    assert_eq!(deferral_length_label(59 * 60), "59m");
    assert_eq!(deferral_length_label(60 * 60), "1h");
    assert_eq!(deferral_length_label(23 * 3_600), "23h");
    assert_eq!(deferral_length_label(24 * 3_600), "1d");
    assert_eq!(deferral_length_label(29 * 86_400), "29d");
    assert_eq!(deferral_length_label(30 * 86_400), "1mo");
    assert_eq!(deferral_length_label(364 * 86_400), "12mo");
    assert_eq!(deferral_length_label(365 * 86_400), "1y");
    assert_eq!(deferral_length_label(400 * 86_400), "1y");
}

#[test]
fn snooze_task_rejects_bad_targets_times_and_statuses() {
    let (_temp, beads_dir, task_id) = task_plus_one_fixture(StatusWire::Open);
    let now = || Some("2026-01-01T00:02:00Z".to_string());

    let error = snooze_task(
        &beads_dir,
        &task_id,
        "2026-01-04T09:00:00",
        None,
        "",
        "owner",
        now(),
    )
    .unwrap_err();
    assert!(error
        .message
        .contains("until must be an RFC-3339 timestamp"));

    let error = snooze_task(
        &beads_dir,
        &task_id,
        "2026-01-01T00:01:00Z",
        None,
        "",
        "owner",
        now(),
    )
    .unwrap_err();
    assert!(error.message.contains("wake time must be in the future"));

    let error = snooze_task(
        &beads_dir,
        &task_id,
        "2026-01-04T00:00:00Z",
        Some(0),
        "",
        "owner",
        now(),
    )
    .unwrap_err();
    assert!(error.message.contains("+1 target must be at least 1"));

    let error = snooze_task(
        &beads_dir,
        &task_id,
        "2026-01-04T00:00:00Z",
        None,
        "",
        "   ",
        now(),
    )
    .unwrap_err();
    assert!(error.message.contains("actor cannot be empty"));

    // An in-progress task is being worked on, so deferring it is a
    // contradiction rather than a deferral.
    claim_for_agent_launch(&beads_dir, &task_id, "agent-a", now()).unwrap();
    let error = snooze_task(
        &beads_dir,
        &task_id,
        "2026-01-04T00:00:00Z",
        None,
        "",
        "owner",
        now(),
    )
    .unwrap_err();
    assert!(error
        .message
        .contains("only open, ready, and already snoozed task beads"));
    assert!(error.message.contains("current status is in_progress"));
}

#[test]
fn snooze_task_rejects_non_task_beads() {
    let temp = tempdir().unwrap();
    init_store(temp.path(), "beads", "sase", "owner@example.com").unwrap();
    let beads_dir = temp.path().join("beads");
    let epic = create_issue(
        &beads_dir,
        BeadCreateRequestWire {
            title: "An epic".to_string(),
            issue_type: IssueTypeWire::Plan,
            now: Some("2026-01-01T00:00:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap();

    let error = snooze_task(
        &beads_dir,
        &epic.id,
        "2026-01-04T00:00:00Z",
        None,
        "",
        "owner",
        Some("2026-01-01T00:02:00Z".to_string()),
    )
    .unwrap_err();
    assert!(error.message.contains("only applies to task beads"));
}

#[test]
fn re_snoozing_replaces_the_record_and_appends_a_second_event() {
    let (_temp, beads_dir, task_id) =
        snoozed_task_fixture("2026-01-04T00:00:00Z", Some(2));
    snooze_task(
        &beads_dir,
        &task_id,
        "2026-01-09T00:00:00Z",
        None,
        "still blocked",
        "owner",
        Some("2026-01-03T00:00:00Z".to_string()),
    )
    .unwrap();

    let issue = read_store_issues(&beads_dir).unwrap().pop().unwrap();
    let snooze = issue.snooze.clone().unwrap();
    assert_eq!(snooze.until, "2026-01-09T00:00:00Z");
    assert_eq!(snooze.reason, "still blocked");
    assert_eq!(snooze.plus_one_target, None);
    assert_eq!(snooze.plus_one_baseline, None);

    let (_manifest, streams) = read_event_store(&beads_dir).unwrap();
    let snoozed_events = streams
        .iter()
        .flat_map(|stream| &stream.events)
        .filter(|event| event.operation == BeadEventOperationWire::TaskSnoozed)
        .count();
    assert_eq!(snoozed_events, 2);
    assert_eq!(reduces_to_store(&beads_dir), vec![issue]);
}

#[test]
fn cancel_task_snooze_returns_the_bead_to_ready() {
    let (_temp, beads_dir, task_id) =
        snoozed_task_fixture("2026-01-04T00:00:00Z", None);

    let issue = cancel_task_snooze(
        &beads_dir,
        &task_id,
        "owner",
        Some("2026-01-02T00:00:00Z".to_string()),
    )
    .unwrap()
    .issue
    .unwrap();
    assert_eq!(issue.status, StatusWire::Ready);
    assert_eq!(issue.snooze, None);
    assert_eq!(reduces_to_store(&beads_dir), vec![issue]);

    let error =
        cancel_task_snooze(&beads_dir, &task_id, "owner", None).unwrap_err();
    assert!(error.message.contains("is not snoozed"));
}

#[test]
fn closing_a_snoozed_task_drops_the_record_and_the_store_reloads() {
    let (_temp, beads_dir, task_id) =
        snoozed_task_fixture("2026-01-04T00:00:00Z", Some(2));

    close_issues(
        &beads_dir,
        std::slice::from_ref(&task_id),
        Some("no longer needed".to_string()),
        Some(BeadResolutionWire::Canceled),
        false,
        Some("2026-01-02T00:00:00Z".to_string()),
    )
    .unwrap();

    // The reload is the assertion that matters: before the fix the close
    // itself raised, and every later load raised with it.
    let issue = read_store_issues(&beads_dir)
        .unwrap()
        .into_iter()
        .find(|issue| issue.id == task_id)
        .unwrap();
    assert_eq!(issue.status, StatusWire::Closed);
    assert_eq!(issue.snooze, None);
    assert_eq!(issue.closed_at.as_deref(), Some("2026-01-02T00:00:00Z"));
    assert_eq!(issue.resolution, Some(BeadResolutionWire::Canceled));

    // The reducer and the mutation must not drift; that divergence is the
    // invariant whose breakage bricked the store.
    assert_eq!(reduces_to_store(&beads_dir), vec![issue]);
}

#[test]
fn a_store_bricked_by_a_close_over_a_snooze_loads_again() {
    let (_temp, beads_dir, task_id) =
        snoozed_task_fixture("2026-01-04T00:00:00Z", None);

    // Build the poisoned stream from raw event records rather than by
    // calling the fixed close, so this stays a real recovery test: an
    // `issue_closed` event lands on disk while issues.jsonl still reads
    // `snoozed`, exactly the shape the old close left behind.
    let mut store = MutableStore::load(&beads_dir).unwrap();
    store
        .append_issue_event(
            &task_id,
            BeadEventOperationWire::IssueClosed,
            BeadEventPayloadWire::IssueClosed {
                close_reason: Some("bricked by the old close".to_string()),
                resolution: Some(BeadResolutionWire::Canceled),
                forced_descendant_ids: Vec::new(),
                closed_by: None,
            },
            "2026-01-02T00:00:00Z",
            "owner",
        )
        .unwrap();
    write_event_store(&beads_dir, store.streams.all()).unwrap();

    let issue = read_store_issues(&beads_dir)
        .unwrap()
        .into_iter()
        .find(|issue| issue.id == task_id)
        .unwrap();
    assert_eq!(issue.status, StatusWire::Closed);
    assert_eq!(issue.snooze, None);
}

#[test]
fn an_invalid_derived_state_leaves_the_event_streams_untouched() {
    let (_temp, beads_dir, task_id) =
        snoozed_task_fixture("2026-01-04T00:00:00Z", None);
    let before = read_event_store(&beads_dir).unwrap().1;

    let mut store = MutableStore::load(&beads_dir).unwrap();
    store
        .append_issue_event(
            &task_id,
            BeadEventOperationWire::IssueOpened,
            BeadEventPayloadWire::IssueOpened,
            "2026-01-02T00:00:00Z",
            "owner",
        )
        .unwrap();
    // A non-snoozed issue that still carries snooze metadata: the derived
    // state the close used to persist an event ahead of.
    let index = store.issue_index(&task_id).unwrap();
    store.issues[index].status = StatusWire::Open;
    let error = store.save().unwrap_err();
    assert!(error
        .message
        .contains("Only snoozed issues can carry snooze metadata"));

    // Nothing durable was written, so the store still opens.
    assert_eq!(read_event_store(&beads_dir).unwrap().1, before);
    assert_eq!(
        read_store_issues(&beads_dir)
            .unwrap()
            .into_iter()
            .find(|issue| issue.id == task_id)
            .unwrap()
            .status,
        StatusWire::Snoozed
    );
}

#[test]
fn reopening_and_launch_claiming_a_snoozed_task_drop_the_record() {
    let (_temp, beads_dir, task_id) =
        snoozed_task_fixture("2026-01-04T00:00:00Z", None);
    let reopened = open_issue(
        &beads_dir,
        &task_id,
        Some("2026-01-02T00:00:00Z".to_string()),
    )
    .unwrap()
    .issue
    .unwrap();
    assert_eq!(reopened.status, StatusWire::Open);
    assert_eq!(reopened.snooze, None);
    assert_eq!(reduces_to_store(&beads_dir), vec![reopened]);

    let (_temp, beads_dir, task_id) =
        snoozed_task_fixture("2026-01-04T00:00:00Z", None);
    let claimed = claim_for_agent_launch(
        &beads_dir,
        &task_id,
        "agent-a",
        Some("2026-01-02T00:00:00Z".to_string()),
    )
    .unwrap()
    .issue
    .unwrap();
    assert_eq!(claimed.status, StatusWire::InProgress);
    assert_eq!(claimed.assignee, "agent-a");
    assert_eq!(claimed.snooze, None);
    assert_eq!(reduces_to_store(&beads_dir), vec![claimed]);
}

#[test]
fn plus_one_below_the_target_leaves_a_snoozed_bead_snoozed() {
    let (_temp, beads_dir, task_id) =
        snoozed_task_fixture("2026-01-04T00:00:00Z", Some(2));

    let issue = add_task_plus_one(
        &beads_dir,
        &task_id,
        "reporter-one",
        "hit this too",
        &[],
        Some("2026-01-02T00:00:00Z".to_string()),
        None,
    )
    .unwrap()
    .issue
    .unwrap();
    assert_eq!(issue.status, StatusWire::Snoozed);
    assert_eq!(issue.plus_one_count(), 1);
    assert!(issue.snooze.is_some());
    assert_eq!(reduces_to_store(&beads_dir), vec![issue]);
}

#[test]
fn plus_one_at_the_target_wakes_a_snoozed_bead_with_a_preset_note() {
    let (_temp, beads_dir, task_id) =
        snoozed_task_fixture("2026-01-04T00:00:00Z", Some(2));
    add_task_plus_one(
        &beads_dir,
        &task_id,
        "reporter-one",
        "hit this too",
        &[],
        Some("2026-01-02T00:00:00Z".to_string()),
        None,
    )
    .unwrap();

    let issue = add_task_plus_one(
        &beads_dir,
        &task_id,
        "reporter-two",
        "and again",
        &[],
        Some("2026-01-03T00:00:00Z".to_string()),
        None,
    )
    .unwrap()
    .issue
    .unwrap();
    assert_eq!(issue.status, StatusWire::Ready);
    assert_eq!(issue.snooze, None);
    let note_text = note_text(&issue);
    assert!(note_text.contains(
            "Reopened by +1 threshold: reached 2 +1s while snoozed until 2026-01-04T00:00:00Z."
        ));
    assert!(note_text.contains("reporter-two"));
    assert_eq!(reduces_to_store(&beads_dir), vec![issue]);
}

#[test]
fn plus_one_never_wakes_a_snoozed_bead_that_set_no_target() {
    let (_temp, beads_dir, task_id) =
        snoozed_task_fixture("2026-01-04T00:00:00Z", None);

    for (reporter, timestamp) in [
        ("reporter-one", "2026-01-02T00:00:00Z"),
        ("reporter-two", "2026-01-02T01:00:00Z"),
        ("reporter-three", "2026-01-02T02:00:00Z"),
    ] {
        add_task_plus_one(
            &beads_dir,
            &task_id,
            reporter,
            "me too",
            &[],
            Some(timestamp.to_string()),
            None,
        )
        .unwrap();
    }

    let issue = read_store_issues(&beads_dir).unwrap().pop().unwrap();
    assert_eq!(issue.status, StatusWire::Snoozed);
    assert_eq!(issue.plus_one_count(), 3);
}

#[test]
fn task_plus_one_is_atomic_normalized_and_promotes_closed_task() {
    let (_temp, beads_dir, task_id) = task_plus_one_fixture(StatusWire::Closed);

    let result = add_task_plus_one(
        &beads_dir,
        task_id.rsplit('-').next().unwrap(),
        " reporter-agent ",
        " reproduced on a clean checkout ",
        &[
            "research:202608/repro.md".to_string(),
            "research:202608/repro.md".to_string(),
            "bead:sase-related".to_string(),
        ],
        Some("2026-01-02T00:00:00Z".to_string()),
        None,
    )
    .unwrap();

    assert!(result.changed);
    assert!(!result.reopen_withheld);
    assert_eq!(result.reopen_withheld_closed_at, None);
    let issue = result.issue.unwrap();
    assert_eq!(issue.status, StatusWire::Ready);
    assert_eq!(issue.closed_at, None);
    assert_eq!(issue.close_reason, None);
    assert_eq!(issue.resolution, None);
    assert_eq!(issue.plus_one_count(), 1);
    assert_eq!(issue.plus_one_evidence[0].observed_since, None);
    assert_eq!(issue.plus_one_evidence[0].reporter, "reporter-agent");
    assert_eq!(
        issue.plus_one_evidence[0].note,
        "reproduced on a clean checkout"
    );
    assert_eq!(issue.refs, issue.plus_one_evidence[0].refs);

    let (_manifest, streams) = read_event_store(&beads_dir).unwrap();
    let event = streams
        .iter()
        .flat_map(|stream| &stream.events)
        .find(|event| {
            event.operation == BeadEventOperationWire::TaskPlusOneRecorded
        })
        .unwrap();
    assert_eq!(event.actor, "reporter-agent");
    assert_eq!(reduce_event_streams(&streams).unwrap(), vec![issue]);
}

#[test]
fn task_plus_one_stale_observation_window_records_without_reopening_closed_task(
) {
    let (_temp, beads_dir, task_id) = task_plus_one_fixture_with_assignee(
        StatusWire::Closed,
        "finished-agent",
    );

    let result = add_task_plus_one(
        &beads_dir,
        &task_id,
        "reporter-agent",
        "saw this before the close landed",
        &[],
        Some("2026-01-01T00:02:00Z".to_string()),
        Some("2026-01-01T00:00:30Z".to_string()),
    )
    .unwrap();

    assert!(result.changed);
    assert!(result.reopen_withheld);
    assert_eq!(
        result.reopen_withheld_closed_at.as_deref(),
        Some("2026-01-01T00:01:00Z")
    );
    let issue = result.issue.unwrap();
    assert_eq!(issue.status, StatusWire::Closed);
    assert_eq!(issue.closed_at.as_deref(), Some("2026-01-01T00:01:00Z"));
    assert_eq!(issue.assignee, "finished-agent");
    assert_eq!(issue.plus_one_count(), 1);
    assert_eq!(
        issue.plus_one_evidence[0].observed_since.as_deref(),
        Some("2026-01-01T00:00:30Z")
    );
    let (projected, reduced) = projected_and_reduced(&beads_dir, &task_id);
    assert_eq!(projected, issue);
    assert_eq!(reduced, issue);
}

#[test]
fn task_plus_one_fresh_observation_window_reopens_closed_task_and_clears_assignee(
) {
    let (_temp, beads_dir, task_id) = task_plus_one_fixture_with_assignee(
        StatusWire::Closed,
        "finished-agent",
    );

    let result = add_task_plus_one(
        &beads_dir,
        &task_id,
        "reporter-agent",
        "reproduced after the close landed",
        &[],
        Some("2026-01-01T00:03:00Z".to_string()),
        Some("2026-01-01T00:02:00Z".to_string()),
    )
    .unwrap();

    assert!(result.changed);
    assert!(!result.reopen_withheld);
    assert_eq!(result.reopen_withheld_closed_at, None);
    let issue = result.issue.unwrap();
    assert_eq!(issue.status, StatusWire::Ready);
    assert_eq!(issue.closed_at, None);
    assert_eq!(issue.assignee, "");
    assert_eq!(issue.close_history.len(), 1);
    assert_eq!(
        issue.close_history[0].closed_at.as_str(),
        "2026-01-01T00:01:00Z"
    );
    assert_eq!(
        issue.close_history[0].reopened_at.as_str(),
        "2026-01-01T00:03:00Z"
    );
    assert_eq!(
        issue.close_history[0].reopened_via,
        BeadReopenCauseWire::PlusOne
    );
    assert_eq!(
        issue.plus_one_evidence[0].observed_since.as_deref(),
        Some("2026-01-01T00:02:00Z")
    );
    let (projected, reduced) = projected_and_reduced(&beads_dir, &task_id);
    assert_eq!(projected, issue);
    assert_eq!(reduced, issue);
}

#[test]
fn task_plus_one_open_and_active_statuses_preserve_existing_contract() {
    let (_temp, beads_dir, task_id) = task_plus_one_fixture(StatusWire::Open);
    let open_result = add_task_plus_one(
        &beads_dir,
        &task_id,
        "reporter-agent",
        "draft task reproduction",
        &[],
        Some("2026-01-01T00:02:00Z".to_string()),
        Some("2025-12-31T00:00:00Z".to_string()),
    )
    .unwrap();
    let open_issue = open_result.issue.unwrap();
    assert_eq!(open_issue.status, StatusWire::Ready);
    assert!(!open_result.reopen_withheld);

    for status in [
        StatusWire::Claimed,
        StatusWire::Ready,
        StatusWire::InProgress,
    ] {
        let (_temp, beads_dir, task_id) =
            task_plus_one_fixture_with_assignee(status.clone(), "active-agent");
        let result = add_task_plus_one(
            &beads_dir,
            &task_id,
            "reporter-agent",
            "active task reproduction",
            &[],
            Some("2026-01-01T00:02:00Z".to_string()),
            Some("2026-01-01T00:02:00Z".to_string()),
        )
        .unwrap();
        assert!(result.changed);
        assert!(!result.reopen_withheld);
        let issue = result.issue.unwrap();
        assert_eq!(issue.status, status);
        assert_eq!(issue.assignee, "active-agent");
        assert_eq!(issue.plus_one_count(), 1);
        assert_reopen_parity(
            &beads_dir,
            &task_id,
            mutation_status_value(&issue.status),
        );
    }
}

#[test]
fn task_plus_one_creator_and_repeat_are_byte_identical_noops() {
    let (_temp, beads_dir, task_id) = task_plus_one_fixture(StatusWire::Open);
    let before_creator = persisted_claim_state(&beads_dir);

    let creator = add_task_plus_one(
        &beads_dir,
        &task_id,
        "creator-agent",
        "creator retry",
        &[],
        None,
        None,
    )
    .unwrap();
    assert!(!creator.changed);
    assert_eq!(persisted_claim_state(&beads_dir), before_creator);

    add_task_plus_one(
        &beads_dir,
        &task_id,
        "reporter-agent",
        "first independent report",
        &[],
        Some("2026-01-02T00:00:00Z".to_string()),
        None,
    )
    .unwrap();
    let before_repeat = persisted_claim_state(&beads_dir);
    let repeat = add_task_plus_one(
        &beads_dir,
        &task_id,
        "reporter-agent",
        "later supplemental detail",
        &["research:202608/later.md".to_string()],
        Some("2026-01-03T00:00:00Z".to_string()),
        None,
    )
    .unwrap();
    assert!(!repeat.changed);
    assert_eq!(persisted_claim_state(&beads_dir), before_repeat);
}

#[test]
fn concurrent_task_plus_ones_preserve_reporters_and_deduplicate_retries() {
    let (_temp, beads_dir, task_id) = task_plus_one_fixture(StatusWire::Open);
    let barrier = Arc::new(Barrier::new(4));
    let mut handles = Vec::new();
    for reporter in ["agent-a", "agent-b", "agent-a"] {
        let beads_dir = beads_dir.clone();
        let task_id = task_id.clone();
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait();
            add_task_plus_one(
                &beads_dir,
                &task_id,
                reporter,
                "independent reproduction",
                &[],
                Some("2026-01-02T00:00:00Z".to_string()),
                None,
            )
            .unwrap()
        }));
    }
    barrier.wait();
    for handle in handles {
        handle.join().unwrap();
    }

    let issue = MutableStore::load(&beads_dir)
        .unwrap()
        .get_issue(&task_id)
        .unwrap()
        .clone();
    assert_eq!(issue.plus_one_count(), 2);
    assert_eq!(
        issue
            .plus_one_evidence
            .iter()
            .map(|evidence| evidence.reporter.as_str())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(["agent-a", "agent-b"])
    );
}
