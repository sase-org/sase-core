//! Action validation tests.

use crate::goal::actions::{
    plan_goal_action, DeterministicGoalIdMint, GoalActionWire, OsGoalIdMint,
};
use crate::goal::reduce::reduce_goal_events;
use crate::goal::wire::{
    GoalActorWire, GoalEventKindWire, GoalOriginWire, GoalStatusWire,
};

use super::support::{
    agent, created, eid, human, settled, GOAL, OTHER_GOAL, T0, T1, T2,
};

fn mint() -> DeterministicGoalIdMint {
    DeterministicGoalIdMint::new(
        vec!["abcde".to_string()],
        vec![
            "e0000000000000000000000001".to_string(),
            "e0000000000000000000000002".to_string(),
        ],
    )
}

fn live_state() -> crate::goal::wire::GoalStateWire {
    let create = eid(1000, 1);
    reduce_goal_events(
        GOAL,
        &[created(
            GOAL,
            &create,
            T0,
            human(),
            "k:c",
            "Title",
            "Outcome",
            "sase",
        )],
    )
}

fn settled_state() -> crate::goal::wire::GoalStateWire {
    let create = eid(1000, 1);
    let settle = eid(2000, 2);
    reduce_goal_events(
        GOAL,
        &[
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
            settled(
                GOAL,
                &settle,
                T1,
                human(),
                &create,
                "k:s",
                "canceled",
                None,
                Some("done"),
            ),
        ],
    )
}

#[test]
fn new_plans_a_created_event() {
    let events = plan_goal_action(
        None,
        &GoalActionWire::New {
            title: "  Title  ".to_string(),
            outcome: "Outcome".to_string(),
            criteria: vec![],
            project: "sase".to_string(),
            origin: None,
            via: None,
            idempotency_key: None,
        },
        &human(),
        T1,
        &mut mint(),
    )
    .unwrap();
    assert_eq!(events.len(), 1);
    let event = &events[0];
    assert_eq!(event.kind, GoalEventKindWire::Created);
    assert_eq!(event.basis, None);
    assert_eq!(event.goal_id, "abcde");
    assert_eq!(event.at, T1);
    let payload = event.parse_payload().unwrap();
    assert!(matches!(
        payload,
        crate::goal::wire::GoalEventPayloadWire::Created(_)
    ));
}

#[test]
fn new_reduces_to_a_live_goal() {
    let events = plan_goal_action(
        None,
        &GoalActionWire::New {
            title: "Title".to_string(),
            outcome: "Outcome".to_string(),
            criteria: vec![],
            project: "sase".to_string(),
            origin: Some(GoalOriginWire {
                kind: "cli".to_string(),
                principal: "bryan.athena".to_string(),
                machine: "athena".to_string(),
                at: T1.to_string(),
                via: "cli".to_string(),
                agent: None,
                unit_prompt_digest: None,
                root_prompt_digest: None,
            }),
            via: None,
            idempotency_key: Some("k:1".to_string()),
        },
        &human(),
        T1,
        &mut mint(),
    )
    .unwrap();
    let state = reduce_goal_events(&events[0].goal_id, &events);
    assert_eq!(state.status, GoalStatusWire::Active);
    assert_eq!(state.title, "Title");
}

#[test]
fn new_refusals_are_stable() {
    let live = live_state();
    let refusal = plan_goal_action(
        Some(&live),
        &GoalActionWire::New {
            title: "T".to_string(),
            outcome: "O".to_string(),
            criteria: vec![],
            project: "sase".to_string(),
            origin: None,
            via: None,
            idempotency_key: None,
        },
        &human(),
        T1,
        &mut mint(),
    )
    .unwrap_err();
    assert_eq!(refusal.code, "goal_already_exists");

    for (title, outcome, project, code) in [
        ("", "O", "sase", "title_empty"),
        ("   ", "O", "sase", "title_empty"),
        (&"t".repeat(61), "O", "sase", "title_too_long"),
        ("T", "", "sase", "outcome_empty"),
        ("T", "   ", "sase", "outcome_empty"),
        ("T", &"o".repeat(281), "sase", "outcome_too_long"),
        ("T", "one\ntwo", "sase", "outcome_must_be_one_line"),
        ("T", "O", "", "project_required"),
    ] {
        let refusal = plan_goal_action(
            None,
            &GoalActionWire::New {
                title: title.to_string(),
                outcome: outcome.to_string(),
                criteria: vec![],
                project: project.to_string(),
                origin: None,
                via: None,
                idempotency_key: None,
            },
            &human(),
            T1,
            &mut mint(),
        )
        .unwrap_err();
        assert_eq!(refusal.code, code, "title={title:?}");
    }
}

#[test]
fn new_refuses_too_many_criteria() {
    let criteria: Vec<_> = (0..11)
        .map(|index| crate::goal::wire::GoalCriterionInputWire {
            text: format!("criterion {index}"),
            source: crate::goal::wire::GoalCriterionSourceWire::User,
        })
        .collect();
    let refusal = plan_goal_action(
        None,
        &GoalActionWire::New {
            title: "T".to_string(),
            outcome: "O".to_string(),
            criteria,
            project: "sase".to_string(),
            origin: None,
            via: None,
            idempotency_key: None,
        },
        &human(),
        T1,
        &mut mint(),
    )
    .unwrap_err();
    assert_eq!(refusal.code, "too_many_criteria");
}

#[test]
fn edit_plans_an_edited_event_on_its_head() {
    let live = live_state();
    let events = plan_goal_action(
        Some(&live),
        &GoalActionWire::Edit {
            goal_id: GOAL.to_string(),
            title: Some("New title".to_string()),
            outcome: None,
            criteria_added: vec![],
            criteria_removed: vec![],
            note: None,
            expected_head: None,
            idempotency_key: None,
        },
        &human(),
        T1,
        &mut mint(),
    )
    .unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].kind, GoalEventKindWire::Edited);
    assert_eq!(events[0].basis, live.head);
}

#[test]
fn edit_refusals_are_stable() {
    let live = live_state();
    let settled = settled_state();

    let refusal = plan_goal_action(
        None,
        &GoalActionWire::Edit {
            goal_id: GOAL.to_string(),
            title: Some("X".to_string()),
            outcome: None,
            criteria_added: vec![],
            criteria_removed: vec![],
            note: None,
            expected_head: None,
            idempotency_key: None,
        },
        &human(),
        T1,
        &mut mint(),
    )
    .unwrap_err();
    assert_eq!(refusal.code, "goal_not_found");

    let refusal = plan_goal_action(
        Some(&settled),
        &GoalActionWire::Edit {
            goal_id: GOAL.to_string(),
            title: Some("X".to_string()),
            outcome: None,
            criteria_added: vec![],
            criteria_removed: vec![],
            note: None,
            expected_head: None,
            idempotency_key: None,
        },
        &human(),
        T1,
        &mut mint(),
    )
    .unwrap_err();
    assert_eq!(refusal.code, "already_settled");

    let refusal = plan_goal_action(
        Some(&live),
        &GoalActionWire::Edit {
            goal_id: GOAL.to_string(),
            title: None,
            outcome: None,
            criteria_added: vec![],
            criteria_removed: vec![],
            note: None,
            expected_head: None,
            idempotency_key: None,
        },
        &human(),
        T1,
        &mut mint(),
    )
    .unwrap_err();
    assert_eq!(refusal.code, "no_changes");

    let refusal = plan_goal_action(
        Some(&live),
        &GoalActionWire::Edit {
            goal_id: GOAL.to_string(),
            title: Some("X".to_string()),
            outcome: None,
            criteria_added: vec![],
            criteria_removed: vec![],
            note: None,
            expected_head: Some("stale".to_string()),
            idempotency_key: None,
        },
        &human(),
        T1,
        &mut mint(),
    )
    .unwrap_err();
    assert_eq!(refusal.code, "stale_basis");
}

#[test]
fn drop_plans_a_canceled_settlement() {
    let live = live_state();
    let events = plan_goal_action(
        Some(&live),
        &GoalActionWire::Drop {
            goal_id: GOAL.to_string(),
            why: "No longer needed".to_string(),
            expected_head: None,
            idempotency_key: None,
        },
        &human(),
        T1,
        &mut mint(),
    )
    .unwrap();
    assert_eq!(events.len(), 1);
    let payload = events[0].parse_payload().unwrap();
    match payload {
        crate::goal::wire::GoalEventPayloadWire::Settled(settled) => {
            assert_eq!(
                settled.flavor,
                Some(crate::goal::wire::GoalSettleFlavorWire::Canceled)
            );
            assert_eq!(settled.note.as_deref(), Some("No longer needed"));
        }
        _ => panic!("drop plans settled"),
    }

    for (why, code) in
        [("   ", "why_required"), (&"w".repeat(281), "why_too_long")]
    {
        let refusal = plan_goal_action(
            Some(&live),
            &GoalActionWire::Drop {
                goal_id: GOAL.to_string(),
                why: why.to_string(),
                expected_head: None,
                idempotency_key: None,
            },
            &human(),
            T1,
            &mut mint(),
        )
        .unwrap_err();
        assert_eq!(refusal.code, code);
    }
}

#[test]
fn reopen_plans_a_reopened_event() {
    let settled = settled_state();
    let events = plan_goal_action(
        Some(&settled),
        &GoalActionWire::Reopen {
            goal_id: GOAL.to_string(),
            message: "Again".to_string(),
            expected_head: None,
            idempotency_key: None,
        },
        &human(),
        T2,
        &mut mint(),
    )
    .unwrap();
    assert_eq!(events[0].kind, GoalEventKindWire::Reopened);

    let live = live_state();
    let refusal = plan_goal_action(
        Some(&live),
        &GoalActionWire::Reopen {
            goal_id: GOAL.to_string(),
            message: "Again".to_string(),
            expected_head: None,
            idempotency_key: None,
        },
        &human(),
        T1,
        &mut mint(),
    )
    .unwrap_err();
    assert_eq!(refusal.code, "not_settled");
}

#[test]
fn merge_plans_source_settlement_plus_target_record() {
    let source = live_state();
    let target = reduce_goal_events(
        OTHER_GOAL,
        &[created(
            OTHER_GOAL,
            &eid(1100, 11),
            T0,
            human(),
            "k:c",
            "Target",
            "Outcome",
            "sase",
        )],
    );
    let events = plan_goal_action(
        Some(&source),
        &GoalActionWire::Merge {
            source_id: GOAL.to_string(),
            target_id: OTHER_GOAL.to_string(),
            target_state: Some(target.clone()),
            why: Some("Same thread".to_string()),
            expected_head: None,
            idempotency_key: None,
        },
        &human(),
        T1,
        &mut mint(),
    )
    .unwrap();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].goal_id, GOAL);
    assert_eq!(events[0].kind, GoalEventKindWire::Settled);
    assert_eq!(events[0].basis, source.head);
    assert_eq!(events[1].goal_id, OTHER_GOAL);
    assert_eq!(events[1].kind, GoalEventKindWire::Merged);
    assert_eq!(events[1].basis, target.head);
}

#[test]
fn merge_refusals_are_stable() {
    let source = live_state();
    let target = reduce_goal_events(
        OTHER_GOAL,
        &[created(
            OTHER_GOAL,
            &eid(1100, 11),
            T0,
            human(),
            "k:c",
            "Target",
            "Outcome",
            "sase",
        )],
    );
    let settled_target = reduce_goal_events(
        OTHER_GOAL,
        &[
            created(
                OTHER_GOAL,
                &eid(1100, 11),
                T0,
                human(),
                "k:c",
                "Target",
                "Outcome",
                "sase",
            ),
            settled(
                OTHER_GOAL,
                &eid(1200, 12),
                T1,
                human(),
                &eid(1100, 11),
                "k:s",
                "canceled",
                None,
                None,
            ),
        ],
    );

    let refusal = plan_goal_action(
        Some(&source),
        &GoalActionWire::Merge {
            source_id: GOAL.to_string(),
            target_id: GOAL.to_string(),
            target_state: Some(target.clone()),
            why: None,
            expected_head: None,
            idempotency_key: None,
        },
        &human(),
        T1,
        &mut mint(),
    )
    .unwrap_err();
    assert_eq!(refusal.code, "merge_into_self");

    let refusal = plan_goal_action(
        Some(&source),
        &GoalActionWire::Merge {
            source_id: GOAL.to_string(),
            target_id: OTHER_GOAL.to_string(),
            target_state: None,
            why: None,
            expected_head: None,
            idempotency_key: None,
        },
        &human(),
        T1,
        &mut mint(),
    )
    .unwrap_err();
    assert_eq!(refusal.code, "target_not_found");

    let refusal = plan_goal_action(
        Some(&source),
        &GoalActionWire::Merge {
            source_id: GOAL.to_string(),
            target_id: OTHER_GOAL.to_string(),
            target_state: Some(settled_target),
            why: None,
            expected_head: None,
            idempotency_key: None,
        },
        &human(),
        T1,
        &mut mint(),
    )
    .unwrap_err();
    assert_eq!(refusal.code, "target_settled");
}

#[test]
fn edit_normalizes_the_goal_id() {
    let live = live_state();
    let events = plan_goal_action(
        Some(&live),
        &GoalActionWire::Edit {
            goal_id: "7K2MQ".to_string(),
            title: Some("New title".to_string()),
            outcome: None,
            criteria_added: vec![],
            criteria_removed: vec![],
            note: None,
            expected_head: None,
            idempotency_key: None,
        },
        &human(),
        T1,
        &mut mint(),
    )
    .unwrap();
    assert_eq!(events[0].goal_id, GOAL);
}

#[test]
fn edit_refuses_unknown_criteria() {
    let live = live_state();
    let refusal = plan_goal_action(
        Some(&live),
        &GoalActionWire::Edit {
            goal_id: GOAL.to_string(),
            title: None,
            outcome: None,
            criteria_added: vec![],
            criteria_removed: vec!["nope".to_string()],
            note: None,
            expected_head: None,
            idempotency_key: None,
        },
        &human(),
        T1,
        &mut mint(),
    )
    .unwrap_err();
    assert_eq!(refusal.code, "criterion_not_found");
}

#[test]
fn edit_refuses_unchanged_fields() {
    let live = live_state();
    assert_eq!(live.title, "Title");
    for (title, outcome) in [
        (Some("Title".to_string()), None),
        (None, Some("Outcome".to_string())),
        (Some("  Title  ".to_string()), None),
    ] {
        let refusal = plan_goal_action(
            Some(&live),
            &GoalActionWire::Edit {
                goal_id: GOAL.to_string(),
                title: title.clone(),
                outcome: outcome.clone(),
                criteria_added: vec![],
                criteria_removed: vec![],
                note: None,
                expected_head: None,
                idempotency_key: None,
            },
            &human(),
            T1,
            &mut mint(),
        )
        .unwrap_err();
        assert_eq!(
            refusal.code, "no_changes",
            "title={title:?} outcome={outcome:?}"
        );
    }
}

#[test]
fn edit_empty_title_and_outcome_name_their_codes() {
    let live = live_state();
    let refusal = plan_goal_action(
        Some(&live),
        &GoalActionWire::Edit {
            goal_id: GOAL.to_string(),
            title: Some("   ".to_string()),
            outcome: None,
            criteria_added: vec![],
            criteria_removed: vec![],
            note: None,
            expected_head: None,
            idempotency_key: None,
        },
        &human(),
        T1,
        &mut mint(),
    )
    .unwrap_err();
    assert_eq!(refusal.code, "title_empty");

    let refusal = plan_goal_action(
        Some(&live),
        &GoalActionWire::Edit {
            goal_id: GOAL.to_string(),
            title: None,
            outcome: Some(String::new()),
            criteria_added: vec![],
            criteria_removed: vec![],
            note: None,
            expected_head: None,
            idempotency_key: None,
        },
        &human(),
        T1,
        &mut mint(),
    )
    .unwrap_err();
    assert_eq!(refusal.code, "outcome_empty");
}

#[test]
fn edit_cap_counts_removals_before_additions() {
    use crate::goal::wire::GoalEventKindWire;
    use serde_json::json;

    use super::support::event;
    let create = eid(1000, 1);
    let criteria: Vec<_> = (0..10)
        .map(|index| json!({"text": format!("c{index}")}))
        .collect();
    let full = reduce_goal_events(
        GOAL,
        &[event(
            GOAL,
            &create,
            GoalEventKindWire::Created,
            T0,
            human(),
            None,
            "k:c",
            json!({
                "title": "Title",
                "outcome": "Outcome",
                "criteria": criteria,
                "draft": false,
                "project": "sase",
            }),
        )],
    );
    assert_eq!(full.criteria.len(), 10);
    let first = full.criteria[0].id.clone();

    // Ten minus one valid removal plus one addition still fits.
    let added = vec![crate::goal::wire::GoalCriterionInputWire {
        text: "fresh".to_string(),
        source: crate::goal::wire::GoalCriterionSourceWire::User,
    }];
    let events = plan_goal_action(
        Some(&full),
        &GoalActionWire::Edit {
            goal_id: GOAL.to_string(),
            title: None,
            outcome: None,
            criteria_added: added.clone(),
            criteria_removed: vec![first],
            note: None,
            expected_head: None,
            idempotency_key: None,
        },
        &human(),
        T1,
        &mut mint(),
    )
    .unwrap();
    assert_eq!(events.len(), 1);

    // Ten plus one without a removal does not.
    let refusal = plan_goal_action(
        Some(&full),
        &GoalActionWire::Edit {
            goal_id: GOAL.to_string(),
            title: None,
            outcome: None,
            criteria_added: added,
            criteria_removed: vec![],
            note: None,
            expected_head: None,
            idempotency_key: None,
        },
        &human(),
        T1,
        &mut mint(),
    )
    .unwrap_err();
    assert_eq!(refusal.code, "too_many_criteria");
}

#[test]
fn merge_compares_normalized_ids_for_self() {
    let source = live_state();
    let refusal = plan_goal_action(
        Some(&source),
        &GoalActionWire::Merge {
            source_id: "7K2MQ".to_string(),
            target_id: GOAL.to_string(),
            target_state: None,
            why: None,
            expected_head: None,
            idempotency_key: None,
        },
        &human(),
        T1,
        &mut mint(),
    )
    .unwrap_err();
    assert_eq!(refusal.code, "merge_into_self");
}

#[test]
fn planned_events_carry_the_actor() {
    let agent: GoalActorWire = agent("agent-a");
    let events = plan_goal_action(
        None,
        &GoalActionWire::New {
            title: "T".to_string(),
            outcome: "O".to_string(),
            criteria: vec![],
            project: "sase".to_string(),
            origin: None,
            via: None,
            idempotency_key: None,
        },
        &agent,
        T1,
        &mut OsGoalIdMint,
    )
    .unwrap();
    assert_eq!(events[0].actor, agent);
}
