//! Reducer tests: status table, races, and diagnostics.

use serde_json::json;

use crate::goal::reduce::{
    goal_event_publish_class, reduce_goal_events, GoalPublishClassWire,
};
use crate::goal::wire::{
    GoalClaimStatusWire, GoalEventKindWire, GoalStatusWire,
    GoalTimelineEffectWire,
};

use super::support::{
    agent, claimed, created, eid, event, host, human, linear_history,
    progressed, reopened, retitle, settled, GOAL, OTHER_GOAL, T0, T1, T2, T3,
    T4, T5,
};

fn draft_created(
    goal: &str,
    event_id: &str,
    at: &str,
    key: &str,
) -> crate::goal::wire::GoalEventWire {
    event(
        goal,
        event_id,
        GoalEventKindWire::Created,
        at,
        human(),
        None,
        key,
        json!({
            "title": "Draft",
            "outcome": "Something local",
            "criteria": [],
            "draft": true,
            "project": "sase",
        }),
    )
}

fn named(
    goal: &str,
    event_id: &str,
    at: &str,
    basis: &str,
    key: &str,
    title: &str,
) -> crate::goal::wire::GoalEventWire {
    event(
        goal,
        event_id,
        GoalEventKindWire::Named,
        at,
        human(),
        Some(basis),
        key,
        json!({
            "title": title,
            "outcome": "Named outcome",
            "auto": false,
        }),
    )
}

fn attached_agent(
    goal: &str,
    event_id: &str,
    at: &str,
    basis: &str,
    key: &str,
    name: &str,
) -> crate::goal::wire::GoalEventWire {
    event(
        goal,
        event_id,
        GoalEventKindWire::AgentAttached,
        at,
        host(),
        Some(basis),
        key,
        json!({"agent": name}),
    )
}

fn retracted(
    goal: &str,
    event_id: &str,
    at: &str,
    basis: &str,
    key: &str,
    claim_no: u32,
) -> crate::goal::wire::GoalEventWire {
    event(
        goal,
        event_id,
        GoalEventKindWire::ClaimRetracted,
        at,
        agent("agent-a"),
        Some(basis),
        key,
        json!({"claim_no": claim_no, "reason": "followup"}),
    )
}

fn merged_record(
    goal: &str,
    event_id: &str,
    at: &str,
    basis: &str,
    key: &str,
    from: &str,
) -> crate::goal::wire::GoalEventWire {
    event(
        goal,
        event_id,
        GoalEventKindWire::Merged,
        at,
        human(),
        Some(basis),
        key,
        json!({"from": from}),
    )
}

fn codes(state: &crate::goal::wire::GoalStateWire) -> Vec<String> {
    state
        .diagnostics
        .iter()
        .map(|diagnostic| diagnostic.code.clone())
        .collect()
}

#[test]
fn created_active_and_draft_statuses() {
    let live = reduce_goal_events(
        GOAL,
        &[created(
            GOAL,
            &eid(1000, 1),
            T0,
            human(),
            "k:c",
            "T",
            "O",
            "sase",
        )],
    );
    assert_eq!(live.status, GoalStatusWire::Active);
    assert_eq!(live.revision, 1);
    assert_eq!(live.criteria.len(), 1);
    assert_eq!(live.criteria[0].id, format!("{}.0", eid(1000, 1)));
    assert!(live.readable);

    let draft = reduce_goal_events(
        GOAL,
        &[draft_created(GOAL, &eid(1000, 1), T0, "k:c")],
    );
    assert_eq!(draft.status, GoalStatusWire::Draft);
}

#[test]
fn named_moves_draft_and_active_to_active() {
    let create = eid(1000, 1);
    let name = eid(2000, 2);
    let state = reduce_goal_events(
        GOAL,
        &[
            draft_created(GOAL, &create, T0, "k:c"),
            named(GOAL, &name, T1, &create, "k:n", "Named"),
        ],
    );
    assert_eq!(state.status, GoalStatusWire::Active);
    assert_eq!(state.title, "Named");
    assert_eq!(state.revision, 2);
}

#[test]
fn named_on_done_is_illegal() {
    let (_, events) = linear_history();
    let name = eid(4000, 4);
    let mut with_named = events.clone();
    with_named.push(named(
        GOAL,
        &name,
        T3,
        &events[2].event_id.clone(),
        "k:n",
        "X",
    ));
    let state = reduce_goal_events(GOAL, &with_named);
    assert_eq!(state.status, GoalStatusWire::Dropped);
    assert!(codes(&state).contains(&"illegal_transition".to_string()));
    let last = state.timeline.last().unwrap();
    assert_eq!(last.effect, GoalTimelineEffectWire::Ignored);
}

#[test]
fn claim_cycle_moves_through_review() {
    let create = eid(1000, 1);
    let claim = eid(2000, 2);
    let retract = eid(3000, 3);
    let state = reduce_goal_events(
        GOAL,
        &[
            created(GOAL, &create, T0, human(), "k:c", "T", "O", "sase"),
            claimed(GOAL, &claim, T1, agent("agent-a"), &create, "k:cl", 1),
            retracted(GOAL, &retract, T2, &claim, "k:r", 1),
        ],
    );
    assert_eq!(state.status, GoalStatusWire::Active);
    assert_eq!(state.claims.len(), 1);
    assert_eq!(
        state.claims[0].status,
        crate::goal::wire::GoalClaimStatusWire::Retracted
    );
}

#[test]
fn second_claim_while_in_review_is_illegal() {
    let create = eid(1000, 1);
    let first = eid(2000, 2);
    let second = eid(3000, 3);
    let state = reduce_goal_events(
        GOAL,
        &[
            created(GOAL, &create, T0, human(), "k:c", "T", "O", "sase"),
            claimed(GOAL, &first, T1, agent("agent-a"), &create, "k:1", 1),
            claimed(GOAL, &second, T2, agent("agent-a"), &first, "k:2", 2),
        ],
    );
    assert_eq!(state.status, GoalStatusWire::Review);
    assert_eq!(state.claims.len(), 1);
}

#[test]
fn settle_flavors_land_done_or_dropped() {
    for (flavor, done) in [
        ("verified", true),
        ("acknowledged", true),
        ("canceled", false),
        ("merged", false),
        ("superseded", false),
    ] {
        let create = eid(1000, 1);
        let settle = eid(2000, 2);
        let into = (flavor == "merged").then_some(OTHER_GOAL);
        let state = reduce_goal_events(
            GOAL,
            &[
                created(GOAL, &create, T0, human(), "k:c", "T", "O", "sase"),
                settled(
                    GOAL,
                    &settle,
                    T1,
                    human(),
                    &create,
                    "k:s",
                    flavor,
                    into,
                    None,
                ),
            ],
        );
        assert_eq!(
            state.status,
            if done {
                GoalStatusWire::Done
            } else {
                GoalStatusWire::Dropped
            },
            "flavor {flavor}"
        );
    }
}

#[test]
fn reopen_needs_a_settled_goal() {
    let (_, events) = linear_history();
    let settle_id = events[2].event_id.clone();
    let open = eid(4000, 4);
    let state = reduce_goal_events(
        GOAL,
        &[
            events.clone(),
            vec![reopened(GOAL, &open, T3, human(), &settle_id, "k:r")],
        ]
        .concat(),
    );
    assert_eq!(state.status, GoalStatusWire::Active);
    assert_eq!(state.flavor, None);

    let early = reduce_goal_events(
        GOAL,
        &[
            created(GOAL, &eid(1000, 1), T0, human(), "k:c", "T", "O", "sase"),
            reopened(GOAL, &eid(2000, 2), T1, human(), &eid(1000, 1), "k:r"),
        ],
    );
    assert_eq!(early.status, GoalStatusWire::Active);
    assert!(codes(&early).contains(&"illegal_transition".to_string()));
}

#[test]
fn concurrent_settlements_first_in_order_wins() {
    let create = eid(1000, 1);
    let first = eid(2000, 2);
    let second = eid(3000, 3);
    let state = reduce_goal_events(
        GOAL,
        &[
            created(GOAL, &create, T0, human(), "k:c", "T", "O", "sase"),
            settled(
                GOAL,
                &first,
                T1,
                human(),
                &create,
                "k:1",
                "verified",
                None,
                None,
            ),
            settled(
                GOAL,
                &second,
                T2,
                human(),
                &create,
                "k:2",
                "acknowledged",
                None,
                None,
            ),
        ],
    );
    assert_eq!(state.status, GoalStatusWire::Done);
    assert_eq!(
        state.flavor,
        Some(crate::goal::wire::GoalSettleFlavorWire::Verified)
    );
    assert!(codes(&state).contains(&"superseded_settlement".to_string()));
    let effects: Vec<GoalTimelineEffectWire> = state
        .timeline
        .iter()
        .filter(|entry| entry.kind == GoalEventKindWire::Settled)
        .map(|entry| entry.effect)
        .collect();
    assert_eq!(
        effects,
        vec![
            GoalTimelineEffectWire::Applied,
            GoalTimelineEffectWire::Superseded,
        ]
    );
}

#[test]
fn human_canceled_beats_an_earlier_agent_settlement() {
    let create = eid(1000, 1);
    let agent_settle = eid(2000, 2);
    let human_drop = eid(3000, 3);
    let state = reduce_goal_events(
        GOAL,
        &[
            created(GOAL, &create, T0, human(), "k:c", "T", "O", "sase"),
            settled(
                GOAL,
                &agent_settle,
                T1,
                agent("agent-a"),
                &create,
                "k:1",
                "verified",
                None,
                None,
            ),
            settled(
                GOAL,
                &human_drop,
                T2,
                human(),
                &create,
                "k:2",
                "canceled",
                None,
                Some("stop"),
            ),
        ],
    );
    assert_eq!(state.status, GoalStatusWire::Dropped);
    assert_eq!(
        state.flavor,
        Some(crate::goal::wire::GoalSettleFlavorWire::Canceled)
    );
}

#[test]
fn concurrent_claims_first_in_order_wins() {
    let create = eid(1000, 1);
    let first = eid(2000, 2);
    let second = eid(3000, 3);
    let state = reduce_goal_events(
        GOAL,
        &[
            created(GOAL, &create, T0, human(), "k:c", "T", "O", "sase"),
            claimed(GOAL, &first, T1, agent("a"), &create, "k:1", 1),
            claimed(GOAL, &second, T2, agent("b"), &create, "k:2", 2),
        ],
    );
    assert_eq!(state.status, GoalStatusWire::Review);
    assert_eq!(state.claims.len(), 1);
    assert_eq!(state.claims[0].claim_no, 1);
    assert!(codes(&state).contains(&"superseded_claim".to_string()));
}

#[test]
fn concurrent_canceled_beats_a_claim() {
    let create = eid(1000, 1);
    let claim = eid(2000, 2);
    let drop = eid(3000, 3);
    let state = reduce_goal_events(
        GOAL,
        &[
            created(GOAL, &create, T0, human(), "k:c", "T", "O", "sase"),
            claimed(GOAL, &claim, T1, agent("a"), &create, "k:cl", 1),
            settled(
                GOAL,
                &drop,
                T2,
                human(),
                &create,
                "k:s",
                "canceled",
                None,
                None,
            ),
        ],
    );
    assert_eq!(state.status, GoalStatusWire::Dropped);
    assert!(codes(&state).contains(&"canceled_beats_claim".to_string()));

    let claim_late = eid(4000, 4);
    let state = reduce_goal_events(
        GOAL,
        &[
            created(GOAL, &create, T0, human(), "k:c", "T", "O", "sase"),
            settled(
                GOAL,
                &drop,
                T1,
                human(),
                &create,
                "k:s",
                "canceled",
                None,
                None,
            ),
            claimed(GOAL, &claim_late, T2, agent("a"), &create, "k:cl", 1),
        ],
    );
    assert_eq!(state.status, GoalStatusWire::Dropped);
    assert!(codes(&state).contains(&"superseded_claim".to_string()));
}

#[test]
fn concurrent_edits_share_stable_criterion_ids() {
    let create = eid(1000, 1);
    let second = eid(2000, 2);
    let third = eid(3000, 3);
    let created_event =
        created(GOAL, &create, T0, human(), "k:c", "T", "O", "sase");
    let edit_two = event(
        GOAL,
        &second,
        GoalEventKindWire::Edited,
        T1,
        human(),
        Some(&create),
        "k:e2",
        json!({"criteria_added": [{"text": "Second"}]}),
    );
    let edit_three = event(
        GOAL,
        &third,
        GoalEventKindWire::Edited,
        T2,
        human(),
        Some(&create),
        "k:e3",
        json!({"criteria_added": [{"text": "Third"}]}),
    );
    // Two edits share one basis, so they are concurrent.
    let pair =
        reduce_goal_events(GOAL, &[created_event.clone(), edit_three.clone()]);
    let trio = reduce_goal_events(GOAL, &[created_event, edit_two, edit_three]);
    let pair_id = pair
        .criteria
        .iter()
        .find(|criterion| criterion.text == "Third")
        .map(|criterion| criterion.id.clone())
        .expect("third criterion in the pair");
    let trio_id = trio
        .criteria
        .iter()
        .find(|criterion| criterion.text == "Third")
        .map(|criterion| criterion.id.clone())
        .expect("third criterion in the trio");
    assert_eq!(pair_id, trio_id);
    assert_eq!(pair_id, format!("{third}.0"));
}

#[test]
fn reopen_clears_the_merge_target_but_keeps_history() {
    let create = eid(1000, 1);
    let settle = eid(2000, 2);
    let open = eid(3000, 3);
    let source = reduce_goal_events(
        GOAL,
        &[
            created(GOAL, &create, T0, human(), "k:c", "T", "O", "sase"),
            settled(
                GOAL,
                &settle,
                T1,
                human(),
                &create,
                "k:s",
                "merged",
                Some(OTHER_GOAL),
                None,
            ),
            reopened(GOAL, &open, T2, human(), &settle, "k:r"),
        ],
    );
    assert_eq!(source.status, GoalStatusWire::Active);
    assert_eq!(source.merged_into, None);

    // The target's `merged_from` entry is that goal's own history.
    let target_create = eid(1100, 11);
    let merge = eid(2100, 12);
    let target = reduce_goal_events(
        OTHER_GOAL,
        &[
            created(
                OTHER_GOAL,
                &target_create,
                T0,
                human(),
                "k:c",
                "Target",
                "O",
                "sase",
            ),
            merged_record(OTHER_GOAL, &merge, T1, &target_create, "k:m", GOAL),
        ],
    );
    assert_eq!(target.merged_from, vec![GOAL.to_string()]);
}

#[test]
fn claim_beaten_by_a_later_canceled_drop_is_superseded() {
    let create = eid(1000, 1);
    let claim = eid(2000, 2);
    let drop = eid(3000, 3);
    let state = reduce_goal_events(
        GOAL,
        &[
            created(GOAL, &create, T0, human(), "k:c", "T", "O", "sase"),
            claimed(GOAL, &claim, T1, agent("a"), &create, "k:cl", 1),
            settled(
                GOAL,
                &drop,
                T2,
                human(),
                &create,
                "k:s",
                "canceled",
                None,
                None,
            ),
        ],
    );
    assert_eq!(state.status, GoalStatusWire::Dropped);
    assert!(
        !state
            .claims
            .iter()
            .any(|entry| entry.status == GoalClaimStatusWire::Active),
        "no claim stays active under a winning canceled drop"
    );
    assert!(
        state.claims.iter().any(|entry| entry.claim_no == 1
            && entry.status == GoalClaimStatusWire::Superseded),
        "the beaten claim reads as superseded"
    );
    let entry = state
        .timeline
        .iter()
        .find(|entry| entry.event_id == claim)
        .expect("claim timeline entry");
    assert_eq!(entry.effect, GoalTimelineEffectWire::Superseded);
    assert!(codes(&state).contains(&"canceled_beats_claim".to_string()));
}

#[test]
fn late_attachment_is_recorded_without_unsettling() {
    let create = eid(1000, 1);
    let settle = eid(2000, 2);
    let attach = eid(3000, 3);
    let state = reduce_goal_events(
        GOAL,
        &[
            created(GOAL, &create, T0, human(), "k:c", "T", "O", "sase"),
            settled(
                GOAL,
                &settle,
                T1,
                human(),
                &create,
                "k:s",
                "verified",
                None,
                None,
            ),
            attached_agent(GOAL, &attach, T2, &settle, "k:a", "agent-a"),
        ],
    );
    assert_eq!(state.status, GoalStatusWire::Done);
    assert!(state.contributors.contains(&"agent-a".to_string()));
    assert!(codes(&state).contains(&"late_attachment".to_string()));
    let last = state.timeline.last().unwrap();
    assert_eq!(last.effect, GoalTimelineEffectWire::Applied);
}

#[test]
fn content_after_a_seen_settlement_is_ignored() {
    let create = eid(1000, 1);
    let settle = eid(2000, 2);
    let edit = eid(3000, 3);
    let state = reduce_goal_events(
        GOAL,
        &[
            created(GOAL, &create, T0, human(), "k:c", "Title", "O", "sase"),
            settled(
                GOAL,
                &settle,
                T1,
                human(),
                &create,
                "k:s",
                "verified",
                None,
                None,
            ),
            retitle(GOAL, &edit, T2, human(), &settle, "k:e", "Changed"),
        ],
    );
    assert_eq!(state.title, "Title");
    assert!(codes(&state).contains(&"on_settled".to_string()));
}

#[test]
fn content_concurrent_with_settlement_still_applies() {
    let create = eid(1000, 1);
    let edit = eid(2000, 2);
    let settle = eid(3000, 3);
    let state = reduce_goal_events(
        GOAL,
        &[
            created(GOAL, &create, T0, human(), "k:c", "Title", "O", "sase"),
            retitle(GOAL, &edit, T1, human(), &create, "k:e", "Changed"),
            settled(
                GOAL,
                &settle,
                T2,
                human(),
                &create,
                "k:s",
                "verified",
                None,
                None,
            ),
        ],
    );
    assert_eq!(state.title, "Changed");
    assert_eq!(state.status, GoalStatusWire::Done);
}

#[test]
fn second_created_is_an_id_collision() {
    let first = eid(1000, 1);
    let second = eid(2000, 2);
    let state = reduce_goal_events(
        GOAL,
        &[
            created(GOAL, &first, T0, human(), "k:1", "T", "O", "sase"),
            created(GOAL, &second, T1, human(), "k:2", "T2", "O2", "sase"),
        ],
    );
    assert!(!state.readable);
    assert!(state
        .unreadable_reason
        .as_deref()
        .unwrap()
        .contains("id_collision"));
    assert!(codes(&state).contains(&"id_collision".to_string()));
}

#[test]
fn missing_basis_is_a_diagnostic_not_a_failure() {
    let create = eid(9000, 9);
    let edit = eid(1000, 1);
    let state = reduce_goal_events(
        GOAL,
        &[
            created(GOAL, &create, T0, human(), "k:c", "Title", "O", "sase"),
            retitle(GOAL, &edit, T1, human(), "missing-id", "k:e", "Changed"),
        ],
    );
    assert!(codes(&state).contains(&"missing_basis".to_string()));
    assert_eq!(state.title, "Title");
    assert!(state.readable);
}

#[test]
fn idempotency_dedupe_counts_only_the_first() {
    let create = eid(1000, 1);
    let first = eid(2000, 2);
    let second = eid(3000, 3);
    let state = reduce_goal_events(
        GOAL,
        &[
            created(GOAL, &create, T0, human(), "k:c", "Title", "O", "sase"),
            retitle(GOAL, &first, T1, human(), &create, "same", "One"),
            retitle(GOAL, &second, T2, human(), &first, "same", "Two"),
        ],
    );
    assert_eq!(state.title, "One");
    assert!(codes(&state).contains(&"duplicate".to_string()));
    let last = state.timeline.last().unwrap();
    assert_eq!(last.effect, GoalTimelineEffectWire::Duplicate);
}

#[test]
fn unknown_kind_marks_only_that_goal_unreadable() {
    let create = eid(1000, 1);
    let weird = eid(2000, 2);
    let edit = eid(3000, 3);
    let state = reduce_goal_events(
        GOAL,
        &[
            created(GOAL, &create, T0, human(), "k:c", "Title", "O", "sase"),
            event(
                GOAL,
                &weird,
                GoalEventKindWire::parse("time_traveled"),
                T1,
                human(),
                Some(&create),
                "k:w",
                json!({}),
            ),
            retitle(GOAL, &edit, T2, human(), &weird, "k:e", "Changed"),
        ],
    );
    assert!(!state.readable);
    assert!(state
        .unreadable_reason
        .as_deref()
        .unwrap()
        .contains("time_traveled"));
    assert_eq!(state.title, "Changed");

    let clean = reduce_goal_events(
        OTHER_GOAL,
        &[created(
            OTHER_GOAL,
            &create,
            T0,
            human(),
            "k:c",
            "T",
            "O",
            "sase",
        )],
    );
    assert!(clean.readable);
}

#[test]
fn newer_schema_version_marks_the_goal_unreadable() {
    let mut event = created(
        GOAL,
        &eid(1000, 1),
        T0,
        human(),
        "k:c",
        "Title",
        "Outcome",
        "sase",
    );
    event.schema_version = 99;
    let state = reduce_goal_events(GOAL, &[event]);
    assert!(!state.readable);
    assert!(state.unreadable_reason.as_deref().unwrap().contains("99"));
}

#[test]
fn wrong_goal_events_are_ignored() {
    let create = eid(1000, 1);
    let foreign = eid(2000, 2);
    let state = reduce_goal_events(
        GOAL,
        &[
            created(GOAL, &create, T0, human(), "k:c", "Title", "O", "sase"),
            retitle(OTHER_GOAL, &foreign, T1, human(), &create, "k:e", "X"),
        ],
    );
    assert_eq!(state.title, "Title");
    assert!(codes(&state).contains(&"goal_id_mismatch".to_string()));
    assert_eq!(state.head.as_deref(), Some(create.as_str()));
}

#[test]
fn agent_cannot_remove_user_criteria() {
    let create = eid(1000, 1);
    let edit = eid(2000, 2);
    let criterion = format!("{create}.0");
    let state = reduce_goal_events(
        GOAL,
        &[
            created(GOAL, &create, T0, human(), "k:c", "T", "O", "sase"),
            event(
                GOAL,
                &edit,
                GoalEventKindWire::Edited,
                T1,
                agent("agent-a"),
                Some(&create),
                "k:e",
                json!({"criteria_removed": [criterion]}),
            ),
        ],
    );
    assert_eq!(state.criteria.len(), 1);
    assert!(
        codes(&state).contains(&"agent_cannot_remove_criterion".to_string())
    );
}

#[test]
fn merged_target_record_accumulates_sources() {
    let create = eid(1000, 1);
    let merge = eid(2000, 2);
    let state = reduce_goal_events(
        GOAL,
        &[
            created(GOAL, &create, T0, human(), "k:c", "T", "O", "sase"),
            merged_record(GOAL, &merge, T1, &create, "k:m", OTHER_GOAL),
        ],
    );
    assert_eq!(state.merged_from, vec![OTHER_GOAL.to_string()]);
    assert_eq!(state.status, GoalStatusWire::Active);
}

#[test]
fn merged_settlement_without_a_target_is_flagged() {
    let create = eid(1000, 1);
    let settle = eid(2000, 2);
    let state = reduce_goal_events(
        GOAL,
        &[
            created(GOAL, &create, T0, human(), "k:c", "T", "O", "sase"),
            settled(
                GOAL,
                &settle,
                T1,
                human(),
                &create,
                "k:s",
                "merged",
                None,
                None,
            ),
        ],
    );
    assert_eq!(state.status, GoalStatusWire::Dropped);
    assert!(codes(&state).contains(&"missing_merge_target".to_string()));
}

#[test]
fn shuffled_inputs_reduce_identically() {
    let create = eid(1000, 1);
    let edit = eid(2000, 2);
    let progress = eid(3000, 3);
    let plan = eid(4000, 4);
    let attach = eid(5000, 5);
    let claim = eid(6000, 6);
    let retract = eid(7000, 7);
    let edit_two = eid(8000, 8);
    let events = vec![
        created(
            GOAL,
            &create,
            T0,
            human(),
            "k:c",
            "Title",
            "Outcome",
            "sase",
        ),
        retitle(GOAL, &edit, T1, human(), &create, "k:e", "Changed"),
        progressed(GOAL, &progress, T2, &edit, "k:p", "Did a thing"),
        event(
            GOAL,
            &plan,
            GoalEventKindWire::PlanAttached,
            T3,
            host(),
            Some(&edit),
            "k:pa",
            json!({"plan_ref": "plan:202609/x.md"}),
        ),
        attached_agent(GOAL, &attach, T4, &progress, "k:a", "agent-a"),
        claimed(GOAL, &claim, T5, agent("agent-a"), &plan, "k:cl", 1),
        retracted(GOAL, &retract, T5, &claim, "k:r", 1),
        retitle(GOAL, &edit_two, T5, human(), &retract, "k:e2", "Final"),
    ];
    let expected = reduce_goal_events(GOAL, &events);
    assert_eq!(expected.status, GoalStatusWire::Active);
    assert_eq!(expected.title, "Final");

    let orders: Vec<Vec<usize>> = vec![
        vec![7, 6, 5, 4, 3, 2, 1, 0],
        vec![1, 0, 3, 2, 5, 4, 7, 6],
        vec![4, 0, 6, 2, 7, 1, 5, 3],
        vec![2, 5, 0, 7, 3, 6, 1, 4],
        vec![7, 0, 6, 1, 5, 2, 4, 3],
        vec![3, 4, 5, 1, 6, 0, 7, 2],
    ];
    for order in orders {
        let shuffled: Vec<_> =
            order.iter().map(|slot| events[*slot].clone()).collect();
        assert_eq!(
            reduce_goal_events(GOAL, &shuffled),
            expected,
            "order {order:?}"
        );
    }
}

#[test]
fn publish_classes_split_sync_and_batched() {
    assert_eq!(
        goal_event_publish_class(&GoalEventKindWire::Created),
        GoalPublishClassWire::Sync
    );
    assert_eq!(
        goal_event_publish_class(&GoalEventKindWire::Settled),
        GoalPublishClassWire::Sync
    );
    assert_eq!(
        goal_event_publish_class(&GoalEventKindWire::Claimed),
        GoalPublishClassWire::Sync
    );
    assert_eq!(
        goal_event_publish_class(&GoalEventKindWire::Merged),
        GoalPublishClassWire::Sync
    );
    assert_eq!(
        goal_event_publish_class(&GoalEventKindWire::AgentAttached),
        GoalPublishClassWire::Batched
    );
    assert_eq!(
        goal_event_publish_class(&GoalEventKindWire::Progress),
        GoalPublishClassWire::Batched
    );
    assert_eq!(
        goal_event_publish_class(&GoalEventKindWire::parse("nope")),
        GoalPublishClassWire::Sync
    );
    assert_eq!(GoalPublishClassWire::Sync.as_str(), "sync");
    assert_eq!(GoalPublishClassWire::Batched.as_str(), "batched");
}
