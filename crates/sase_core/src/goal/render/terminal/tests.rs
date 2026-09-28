//! Golden text tests for the terminal goal renderer (not PNGs).

use unicode_width::UnicodeWidthStr;

use super::super::super::view::GOAL_GLYPH;
use super::super::super::wire::{
    GoalStateWire, GoalStatusWire, GOAL_WIRE_SCHEMA_VERSION,
};
use super::{
    render_goal_card, render_goal_list, GoalRenderCardRequestWire,
    GoalRenderListRequestWire,
};

fn state(
    id: &str,
    status: GoalStatusWire,
    title: &str,
    updated_at: &str,
) -> GoalStateWire {
    GoalStateWire {
        schema_version: GOAL_WIRE_SCHEMA_VERSION,
        id: id.to_string(),
        project: "sase".to_string(),
        status,
        title: title.to_string(),
        outcome: "Done means done.".to_string(),
        created_at: "2026-09-28T13:00:00Z".to_string(),
        updated_at: updated_at.to_string(),
        ..GoalStateWire::empty(id)
    }
}

fn list_request() -> GoalRenderListRequestWire {
    GoalRenderListRequestWire {
        project: "sase".to_string(),
        mode: "shared".to_string(),
        synced_ago_seconds: Some(12.0),
        now: "2026-09-28T14:00:00Z".to_string(),
        ..GoalRenderListRequestWire::default()
    }
}

#[test]
fn glyph_is_one_cell_wide() {
    assert_eq!(UnicodeWidthStr::width(GOAL_GLYPH), 1);
}

#[test]
fn tty_list_groups_lanes_with_header_and_counts() {
    let mut request = list_request();
    request.goals = vec![
        state(
            "7k2mq",
            GoalStatusWire::Review,
            "Goals feature design",
            "2026-09-28T13:57:00Z",
        ),
        state(
            "3fq9t",
            GoalStatusWire::Active,
            "Tailnet dispatch mesh",
            "2026-09-28T12:00:00Z",
        ),
    ];
    let text = render_goal_list(&request);
    assert!(
        text.contains("⌖ Goals · sase  1 review · 1 active · synced 12s ago")
    );
    assert!(text.contains("REVIEW"));
    assert!(text.contains("⌖ 7k2mq  Goals feature design"));
}

#[test]
fn compact_list_prints_one_line_per_goal() {
    let mut request = list_request();
    request.compact = true;
    request.goals = vec![state(
        "7k2mq",
        GoalStatusWire::Review,
        "Goals feature design",
        "2026-09-28T13:57:00Z",
    )];
    let text = render_goal_list(&request);
    assert!(text.contains("⌖ 7k2mq  review  Goals feature design  · 3m"));
}

#[test]
fn empty_list_prints_start_hint() {
    let request = list_request();
    let text = render_goal_list(&request);
    assert!(text.contains("No active goals in sase. Start one:"));
    assert!(text.contains("sase goal new"));
}

#[test]
fn footer_chips_report_unpublished_and_unreadable() {
    let mut request = list_request();
    request.unpublished = true;
    request.mode = "local".to_string();
    request.synced_ago_seconds = None;
    let mut bad = state(
        "zz999",
        GoalStatusWire::Active,
        "Broken goal",
        "2026-09-28T13:00:00Z",
    );
    bad.readable = false;
    request.goals = vec![bad];
    let text = render_goal_list(&request);
    assert!(text.contains("local only"));
    assert!(text.contains("↑ unpublished"));
    assert!(text.contains("⚠ 1 unreadable (run sase goal doctor)"));
}

#[test]
fn card_renders_header_outcome_and_timeline() {
    let request = GoalRenderCardRequestWire {
        state: state(
            "7k2mq",
            GoalStatusWire::Active,
            "Goals feature design",
            "2026-09-28T13:57:00Z",
        ),
        now: "2026-09-28T14:00:00Z".to_string(),
        color: false,
    };
    let text = render_goal_card(&request);
    assert!(text.contains("⌖ Goals feature design  ACTIVE"));
    assert!(text.contains("goal:7k2mq · sase · opened 1h ago by"));
    assert!(text.contains("OUTCOME"));
    assert!(text.contains("rev 0"));
}

#[test]
fn colorized_list_wraps_glyph_and_ids() {
    let mut request = list_request();
    request.color = true;
    request.compact = true;
    request.goals = vec![state(
        "7k2mq",
        GoalStatusWire::Review,
        "Goals feature design",
        "2026-09-28T13:57:00Z",
    )];
    let text = render_goal_list(&request);
    assert!(text.contains("\u{1b}[95m⌖\u{1b}[0m"));
    assert!(!render_goal_list(&list_request()).contains("\u{1b}["));
}
