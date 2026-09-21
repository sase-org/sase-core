//! Read-command handler tests, covering `super::super::read_commands`:
//! list, show, search, ready, blocked, and stats behavior.

use super::support::*;
use crate::bead::wire::{BeadTierWire, DependencyWire, StatusWire};

#[test]
fn claimed_status_is_in_default_list_with_claim_details_and_color() {
    let mut claimed = phase_issue(
        "beads-1.1",
        "Claimed phase",
        "Waiting to start.",
        StatusWire::Claimed,
        "2026-01-01T00:01:00Z",
    );
    claimed.assignee = "agent-one".to_string();
    let store = seed_issues(vec![
        claimed,
        phase_issue(
            "beads-1.2",
            "Closed phase",
            "",
            StatusWire::Closed,
            "2026-01-01T00:02:00Z",
        ),
    ]);

    let list = execute_search(&store.beads_dir, &["list"]);
    assert_eq!(list.stdout, "↳ ◎ beads-1.1 · Claimed phase ← beads-1\n");

    let show = execute_search(&store.beads_dir, &["show", "beads-1.1"]);
    assert!(show
        .stdout
        .starts_with("◎ beads-1.1 · Claimed phase   [CLAIMED]\n"));
    assert!(show.stdout.contains(
        "Claimed by: agent-one (agent has not started working yet)\n"
    ));

    let search = execute_search(
        &store.beads_dir,
        &["search", "waiting", "--color", "always"],
    );
    assert!(search.stdout.contains("\x1b[35m◎\x1b[0m"));
}

#[test]
fn stats_prints_ready_and_task_rows() {
    let store = seed_issues(vec![
        phase_issue(
            "beads-1.1",
            "Open phase",
            "",
            StatusWire::Open,
            "2026-01-01T00:01:00Z",
        ),
        phase_issue(
            "beads-1.2",
            "Claimed phase",
            "",
            StatusWire::Claimed,
            "2026-01-01T00:02:00Z",
        ),
        phase_issue(
            "beads-1.3",
            "Active phase",
            "",
            StatusWire::InProgress,
            "2026-01-01T00:03:00Z",
        ),
        task_issue(
            "beads-2",
            "Ready task",
            "",
            StatusWire::Ready,
            "2026-01-01T00:04:00Z",
        ),
    ]);

    let stats = execute_search(&store.beads_dir, &["stats"]);
    assert_eq!(
        stats.stdout,
        concat!(
            "Issue Statistics\n",
            "  Total:       4\n",
            "  Open:        1\n",
            "  Claimed:     1\n",
            "  Ready:       1\n",
            "  In Progress: 1\n",
            "  Closed:      0\n",
            "  Plans:       0\n",
            "  Phases:      3\n",
            "  Tasks:       1\n",
            "  Flags:       0\n",
            "  Due Flags:   0\n",
        )
    );
}

#[test]
fn ready_lists_only_unblocked_ready_tasks_with_ready_glyph() {
    let blocker = task_issue(
        "beads-1",
        "Blocking task",
        "",
        StatusWire::Ready,
        "2026-01-01T00:01:00Z",
    );
    let mut blocked = task_issue(
        "beads-2",
        "Blocked task",
        "",
        StatusWire::Ready,
        "2026-01-01T00:02:00Z",
    );
    blocked.dependencies.push(DependencyWire {
        issue_id: blocked.id.clone(),
        depends_on_id: blocker.id.clone(),
        created_at: "2026-01-01T00:02:00Z".to_string(),
        created_by: String::new(),
    });
    let store = seed_issues(vec![
        blocker,
        blocked,
        task_issue(
            "beads-3",
            "Draft task",
            "",
            StatusWire::Open,
            "2026-01-01T00:03:00Z",
        ),
    ]);

    let ready = execute_search(&store.beads_dir, &["ready"]);
    assert_eq!(
        ready.stdout,
        concat!(
            "◇ beads-1 · Blocking task\n",
            "\n",
            "------------------------------------------------------------\n",
            "Ready: 1 task bead with no active blockers\n",
        )
    );

    let list = execute_search(&store.beads_dir, &["list"]);
    assert!(list.stdout.contains("◆ ◇ beads-1 · Blocking task"));
    assert!(list.stdout.contains("◆ ◇ beads-2 · Blocked task"));
    assert!(list.stdout.contains("◆ ○ beads-3 · Draft task"));
    let colored = execute_search(
        &store.beads_dir,
        &["search", "blocking", "--color", "always"],
    );
    assert!(colored.stdout.contains("\x1b[96m◇\x1b[0m"));
}

#[test]
fn ready_empty_state_explains_epic_preassignment() {
    let store = seed_issues(Vec::new());

    let ready = execute_search(&store.beads_dir, &["ready"]);

    assert_eq!(
        ready.stdout,
        "No ready task beads (epic work is preassigned at launch).\n"
    );
}

#[test]
fn search_compact_orders_matches_newest_first() {
    // Seed oldest-first to prove ordering follows `created_at`, not the
    // stored/input order.
    let store = seed_issues(vec![
        phase_issue(
            "beads-1.1",
            "Auth older",
            "Older item.",
            StatusWire::Open,
            "2026-01-01T00:01:00Z",
        ),
        phase_issue(
            "beads-1.2",
            "Auth newer",
            "Newer item.",
            StatusWire::Open,
            "2026-01-01T00:02:00Z",
        ),
    ]);

    let outcome = execute_search(
        &store.beads_dir,
        &["search", "auth", "--format", "compact", "--color", "never"],
    );

    assert_eq!(outcome.exit_code, 0);
    assert_eq!(
        outcome.stdout,
        "↳ ○ beads-1.2 · Auth newer\n  Newer item.\n\
         ↳ ○ beads-1.1 · Auth older\n  Older item.\n"
    );
}

#[test]
fn search_full_reuses_show_rendering_for_single_result() {
    let plan = plan_issue(
        "beads-1",
        "Parent plan",
        "Plan description",
        StatusWire::Open,
        "2026-01-01T00:00:00Z",
    );
    let phase = phase_issue(
        "beads-1.1",
        "Full term phase",
        "Phase description",
        StatusWire::Open,
        "2026-01-01T00:01:00Z",
    );
    let store = seed_issues(vec![plan, phase]);

    let search = execute_search(
        &store.beads_dir,
        &["search", "full term", "--format", "full"],
    );
    let show = execute_search(&store.beads_dir, &["show", "beads-1.1"]);

    assert_eq!(search.exit_code, 0);
    assert_eq!(search.stdout, show.stdout);
}

#[test]
fn search_applies_filters_and_limit() {
    let mut epic = plan_issue(
        "beads-2",
        "Auth epic",
        "Plan description",
        StatusWire::Open,
        "2026-01-01T00:03:00Z",
    );
    epic.tier = Some(BeadTierWire::Epic);
    let store = seed_issues(vec![
        phase_issue(
            "beads-1.1",
            "Auth phase",
            "Phase description",
            StatusWire::Open,
            "2026-01-01T00:01:00Z",
        ),
        plan_issue(
            "beads-1",
            "Auth closed plan",
            "Closed description",
            StatusWire::Closed,
            "2026-01-01T00:02:00Z",
        ),
        epic,
    ]);

    let outcome = execute_search(
        &store.beads_dir,
        &[
            "search", "auth", "--status", "open", "--type", "plan", "--tier",
            "epic", "--limit", "1", "--color", "never",
        ],
    );

    assert_eq!(outcome.exit_code, 0);
    assert_eq!(
        outcome.stdout,
        "▸ ○ beads-2 · Auth epic\n  Plan description\n"
    );
}

#[test]
fn search_no_match_is_successful() {
    let store = seed_issues(vec![phase_issue(
        "beads-1.1",
        "Auth phase",
        "Phase description",
        StatusWire::Open,
        "2026-01-01T00:01:00Z",
    )]);

    let outcome = execute_search(
        &store.beads_dir,
        &["search", "missing", "--format", "full"],
    );

    assert_eq!(outcome.exit_code, 0);
    assert_eq!(outcome.stdout, "No beads match \"missing\".\n");
}

#[test]
fn search_whitespace_query_is_usage_error() {
    let store = seed_issues(Vec::new());

    let outcome = execute_search(&store.beads_dir, &["search", "   "]);

    assert_eq!(outcome.exit_code, 2);
    assert_eq!(outcome.stderr, "Error: search query cannot be empty\n");
}
