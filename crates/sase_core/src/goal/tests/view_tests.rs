//! View model tests.

use crate::goal::reduce::reduce_goal_events;
use crate::goal::view::{
    goal_card_view, goal_row_view, goal_row_view_at, relative_age,
    GOAL_ACCENT_HEX, GOAL_GLYPH,
};
use crate::goal::wire::GoalStatusWire;

use super::support::{
    created, eid, event, human, progressed, settled, GOAL, T0, T1, T2,
};

fn live_state() -> crate::goal::wire::GoalStateWire {
    let create = eid(1000, 1);
    let progress = eid(2000, 2);
    reduce_goal_events(
        GOAL,
        &[
            created(
                GOAL,
                &create,
                T0,
                human(),
                "k:c",
                "The title",
                "The outcome",
                "sase",
            ),
            progressed(GOAL, &progress, T2, &create, "k:p", "Did a thing"),
        ],
    )
}

#[test]
fn glyph_and_accent_are_frozen() {
    assert_eq!(GOAL_GLYPH, "⌖");
    assert_eq!(GOAL_ACCENT_HEX, "#FF87AF");
}

#[test]
fn row_carries_glyph_status_title_and_age() {
    let state = live_state();
    let row = goal_row_view_at(&state, T2);
    assert_eq!(row.glyph, "⌖");
    assert_eq!(row.id, GOAL);
    assert_eq!(row.status, "active");
    assert_eq!(row.title, "The title");
    assert_eq!(row.age, "0s");
    assert!(row.readable);
    assert_eq!(row.updated_at, T2);
    assert_eq!(row.last_progress_preview.as_deref(), Some("Did a thing"));

    let row = goal_row_view_at(&state, "2026-09-28T14:05:00.000Z");
    assert_eq!(row.age, "3m");
}

#[test]
fn row_without_now_covers_its_lifespan() {
    let state = live_state();
    let row = goal_row_view(&state);
    assert_eq!(row.age, "2m");
}

#[test]
fn card_header_names_title_status_ref_and_revision() {
    let state = live_state();
    let card = goal_card_view(&state, T2);
    assert_eq!(card.title, "The title");
    assert_eq!(card.status_badge, "ACTIVE");
    assert_eq!(card.goal_ref, format!("goal:{GOAL}"));
    assert_eq!(card.project, "sase");
    assert_eq!(card.opened_by, "bryan.athena");
    assert_eq!(card.opened_at, T0);
    assert_eq!(card.opened_age, "2m");
    assert_eq!(card.revision, 1);
    assert_eq!(card.mode_label, None);
    assert_eq!(card.outcome.as_deref(), Some("The outcome"));
    assert_eq!(card.criteria.len(), 1);
    assert_eq!(card.criteria[0].source, "user");
    assert_eq!(card.timeline.len(), 2);
    assert_eq!(card.timeline[0].age, "2m");
}

#[test]
fn card_omits_empty_sections() {
    let state = crate::goal::wire::GoalStateWire::empty(GOAL);
    let card = goal_card_view(&state, T0);
    assert_eq!(card.outcome, None);
    assert!(card.criteria.is_empty());
    assert!(card.merged.is_empty());
    assert_eq!(card.plan, None);
    assert!(card.claims.is_empty());
    assert!(card.timeline.is_empty());
    let value = serde_json::to_value(&card).unwrap();
    for missing in [
        "outcome",
        "criteria",
        "merged",
        "plan",
        "claims",
        "timeline",
        "mode_label",
    ] {
        assert!(value.get(missing).is_none(), "section {missing} is omitted");
    }
}

#[test]
fn draft_card_is_labeled_local() {
    let create = eid(1000, 1);
    let state = reduce_goal_events(
        GOAL,
        &[event(
            GOAL,
            &create,
            crate::goal::wire::GoalEventKindWire::Created,
            T0,
            human(),
            None,
            "k:c",
            serde_json::json!({
                "title": "Draft",
                "outcome": "Local",
                "draft": true,
                "project": "sase",
            }),
        )],
    );
    assert_eq!(state.status, GoalStatusWire::Draft);
    let card = goal_card_view(&state, T1);
    assert_eq!(card.mode_label.as_deref(), Some("local"));
}

#[test]
fn settled_card_keeps_its_badge() {
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
                "verified",
                None,
                None,
            ),
        ],
    );
    let card = goal_card_view(&state, T1);
    assert_eq!(card.status_badge, "DONE");
}

#[test]
fn relative_age_steps_through_units() {
    let base = "2026-09-28T14:00:00.000Z";
    for (now, want) in [
        ("2026-09-28T14:00:07.000Z", "7s"),
        ("2026-09-28T14:03:00.000Z", "3m"),
        ("2026-09-28T16:00:00.000Z", "2h"),
        ("2026-09-29T14:00:00.000Z", "1d"),
        ("2026-10-28T14:00:00.000Z", "1mo"),
        ("2027-09-28T14:00:00.000Z", "1y"),
    ] {
        assert_eq!(relative_age(base, now), want, "now={now}");
    }
    assert_eq!(relative_age("nope", base), "");
    assert_eq!(relative_age(base, "nope"), "");
}
