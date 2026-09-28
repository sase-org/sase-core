//! Tests for the lean `sase goal` fast path.

use std::fs;

use tempfile::TempDir;

use super::super::ledger::goal_ledger_init;
use super::{
    discover_project, goal_fast_path, parse_argv, split_goal_token,
    GoalFastPathRequestWire,
};

fn argv(tokens: &[&str]) -> Vec<String> {
    tokens.iter().map(|token| token.to_string()).collect()
}

#[test]
fn parse_accepts_bare_list_and_show() {
    assert!(parse_argv(&[]).is_some());
    assert!(parse_argv(&argv(&["list"])).is_some());
    assert!(parse_argv(&argv(&["list", "-j"])).is_some());
    assert!(parse_argv(&argv(&["list", "-s", "active"])).is_some());
    assert!(parse_argv(&argv(&["show", "7k2mq"])).is_some());
    assert!(parse_argv(&argv(&["show", "⌖7k2mq", "--json"])).is_some());
}

#[test]
fn parse_declines_slow_path_flags() {
    assert!(parse_argv(&argv(&["list", "-h"])).is_none());
    assert!(parse_argv(&argv(&["list", "-a"])).is_none());
    assert!(parse_argv(&argv(&["list", "-f"])).is_none());
    assert!(parse_argv(&argv(&["list", "-s", "done"])).is_none());
    assert!(parse_argv(&argv(&["list", "-n", "5"])).is_none());
    assert!(parse_argv(&argv(&["doctor"])).is_none());
    assert!(parse_argv(&argv(&["new"])).is_none());
    assert!(parse_argv(&argv(&["show"])).is_none());
    assert!(parse_argv(&argv(&["show", "a", "b"])).is_none());
}

#[test]
fn token_split_accepts_every_id_form() {
    assert_eq!(split_goal_token("7k2mq"), (None, "7k2mq".to_string()));
    assert_eq!(split_goal_token("⌖7k2mq"), (None, "7k2mq".to_string()));
    assert_eq!(split_goal_token("goal:7k2mq"), (None, "7k2mq".to_string()));
    assert_eq!(
        split_goal_token("goal:sase@7k2mq"),
        (Some("sase".to_string()), "7k2mq".to_string())
    );
}

#[test]
fn empty_ledger_lists_empty_state_with_delegation_notice() {
    let home = TempDir::new().expect("home");
    let work = TempDir::new().expect("work");
    let root = home.path().join("goals");
    goal_ledger_init(&root).expect("init");
    fs::create_dir_all(work.path().join(".sase")).expect("sase dir");
    fs::write(
        work.path().join(".sase").join("checkout.json"),
        r#"{"project_name":"sase"}"#,
    )
    .expect("checkout");
    let hot_dir = home.path().join("projects").join("sase");
    fs::create_dir_all(&hot_dir).expect("hot dir");
    fs::write(
        hot_dir.join("goals-hot.json"),
        format!(
            r#"{{"schema_version":1,"project":"sase","mode":"local","ledger_root":"{}","watermark_path":"","outbox_path":"","generated_at":"2026-09-28T14:00:00Z","goals":{{}}}}"#,
            root.display(),
        ),
    )
    .expect("hot");
    let request = GoalFastPathRequestWire {
        argv: vec![],
        cwd: work.path().display().to_string(),
        sase_home: home.path().display().to_string(),
        now: "2026-09-28T14:00:00Z".to_string(),
        ..GoalFastPathRequestWire::default()
    };
    let response = goal_fast_path(&request).expect("fast path");
    assert!(response.handled);
    assert_eq!(response.exit_code, 0);
    assert!(!response.spawn_fetch);
    assert!(response.stdout.contains(
        "No subcommand provided for 'sase goal'; delegating to 'sase goal list'."
    ));
    assert!(response.stdout.contains("No active goals in sase."));
}

#[test]
fn missing_projection_declines_to_slow_path() {
    let work = TempDir::new().expect("work");
    let home = TempDir::new().expect("home");
    fs::create_dir_all(work.path().join(".sase")).expect("sase dir");
    fs::write(
        work.path().join(".sase").join("checkout.json"),
        r#"{"project_name":"sase"}"#,
    )
    .expect("checkout");
    let request = GoalFastPathRequestWire {
        argv: vec![],
        cwd: work.path().display().to_string(),
        sase_home: home.path().display().to_string(),
        now: "2026-09-28T14:00:00Z".to_string(),
        ..GoalFastPathRequestWire::default()
    };
    let response = goal_fast_path(&request).expect("fast path");
    assert!(!response.handled);
    assert_eq!(response.project, "sase");
}

#[test]
fn discovery_rejects_non_canonical_projects() {
    let work = TempDir::new().expect("work");
    fs::create_dir_all(work.path().join(".sase")).expect("sase dir");
    fs::write(
        work.path().join(".sase").join("checkout.json"),
        r#"{"project_name":"../evil"}"#,
    )
    .expect("checkout");
    assert_eq!(discover_project(&work.path().display().to_string()), None);
}
