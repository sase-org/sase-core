use super::super::*;
use super::support::dual_tempdir as tempdir;
use super::support::*;
use crate::bead::events::BeadEventOperationWire;
use crate::bead::jsonl::read_event_store;
use crate::bead::mutation::store::MutableStore;
use crate::bead::wire::IssueTypeWire;
use crate::bead::wire::PhaseSizeWire;
use std::fs;

use crate::bead::config::default_config;
use crate::bead::config::save_config;

#[test]
fn open_stamps_the_supplied_actor_on_every_opened_event() {
    run_dual_mode_test(|_mode| {
        let temp = tempdir();
        let beads_dir = temp.path().join("sdd/beads");
        fs::create_dir_all(&beads_dir).unwrap();
        save_config(&beads_dir, &default_config("sase", "owner@example.com"))
            .unwrap();
        fs::write(beads_dir.join("issues.jsonl"), "").unwrap();
        let epic_id = create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "Epic".to_string(),
                issue_type: IssueTypeWire::Plan,
                now: Some("2026-01-01T00:00:00Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap()
        .id;
        let phase_id = create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "Phase".to_string(),
                issue_type: IssueTypeWire::Phase,
                parent_id: Some(epic_id.clone()),
                size: Some(PhaseSizeWire::Small),
                created_by: Some("creator-agent".to_string()),
                now: Some("2026-01-01T00:01:00Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap()
        .id;
        close_issues(
            &beads_dir,
            std::slice::from_ref(&phase_id),
            Some("done".to_string()),
            None,
            false,
            Some("2026-01-01T00:02:00Z".to_string()),
        )
        .unwrap();
        close_issues(
            &beads_dir,
            std::slice::from_ref(&epic_id),
            Some("done".to_string()),
            None,
            false,
            Some("2026-01-01T00:03:00Z".to_string()),
        )
        .unwrap();

        let outcome = open_issue_with_actor(
            &beads_dir,
            &phase_id,
            Some("opener-agent".to_string()),
            Some("2026-01-01T00:04:00Z".to_string()),
        )
        .unwrap();

        assert!(outcome.changed);
        assert_eq!(outcome.reopened_ancestor_ids, vec![epic_id.clone()]);
        // The creator never leaks onto the reopen: the target and every
        // reopened ancestor name the acting opener.
        let (_manifest, streams) = read_event_store(&beads_dir).unwrap();
        let mut opened_actors = streams
            .iter()
            .flat_map(|stream| &stream.events)
            .filter(|event| {
                event.operation == BeadEventOperationWire::IssueOpened
            })
            .map(|event| (event.issue_id.clone(), event.actor.clone()))
            .collect::<Vec<_>>();
        opened_actors.sort();
        assert_eq!(
            opened_actors,
            vec![
                (epic_id.clone(), "opener-agent".to_string()),
                (phase_id.clone(), "opener-agent".to_string()),
            ]
        );
        assert_dual_mode_parity_for_current_mode();
    });
}

#[test]
fn open_falls_back_to_the_store_owner_without_an_actor() {
    run_dual_mode_test(|_mode| {
        let temp = tempdir();
        let beads_dir = temp.path().join("sdd/beads");
        fs::create_dir_all(&beads_dir).unwrap();
        save_config(&beads_dir, &default_config("sase", "owner@example.com"))
            .unwrap();
        fs::write(beads_dir.join("issues.jsonl"), "").unwrap();
        let issue_id = create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "Reopenable".to_string(),
                issue_type: IssueTypeWire::Task,
                size: Some(PhaseSizeWire::Small),
                task_type: Some("bug".to_string()),
                created_by: Some("creator-agent".to_string()),
                now: Some("2026-01-01T00:00:00Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap()
        .id;
        close_issues(
            &beads_dir,
            std::slice::from_ref(&issue_id),
            Some("done".to_string()),
            None,
            false,
            Some("2026-01-01T00:01:00Z".to_string()),
        )
        .unwrap();

        open_issue(
            &beads_dir,
            &issue_id,
            Some("2026-01-01T00:02:00Z".to_string()),
        )
        .unwrap();

        let (_manifest, streams) = read_event_store(&beads_dir).unwrap();
        let opened = streams
            .iter()
            .flat_map(|stream| &stream.events)
            .find(|event| {
                event.issue_id == issue_id
                    && event.operation == BeadEventOperationWire::IssueOpened
            })
            .expect("one opened event");
        assert_eq!(opened.actor, "owner@example.com");
        assert_dual_mode_parity_for_current_mode();
    });
}

#[test]
fn reopening_an_open_bead_is_a_noop() {
    run_dual_mode_test(|_mode| {
        let temp = tempdir();
        let beads_dir = temp.path().join("sdd/beads");
        fs::create_dir_all(&beads_dir).unwrap();
        save_config(&beads_dir, &default_config("sase", "owner@example.com"))
            .unwrap();
        fs::write(beads_dir.join("issues.jsonl"), "").unwrap();
        let issue_id = create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "Already open".to_string(),
                issue_type: IssueTypeWire::Task,
                size: Some(PhaseSizeWire::Small),
                task_type: Some("bug".to_string()),
                created_by: Some("creator-agent".to_string()),
                now: Some("2026-01-01T00:00:00Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap()
        .id;

        let outcome = open_issue_with_actor(
            &beads_dir,
            &issue_id,
            Some("opener-agent".to_string()),
            Some("2026-01-01T00:01:00Z".to_string()),
        )
        .unwrap();

        assert!(!outcome.changed);
        assert_eq!(outcome.issue_ids, vec![issue_id.clone()]);
        assert!(!outcome.message.is_empty());
        assert!(outcome.issues.is_empty());
        assert_eq!(
            outcome.issue.as_ref().expect("no-op returns the issue").id,
            issue_id
        );
        // No event was minted and the row is untouched.
        let (_manifest, streams) = read_event_store(&beads_dir).unwrap();
        let opened_count = streams
            .iter()
            .flat_map(|stream| &stream.events)
            .filter(|event| {
                event.operation == BeadEventOperationWire::IssueOpened
            })
            .count();
        assert_eq!(opened_count, 0);
        let stored = MutableStore::load(&beads_dir)
            .unwrap()
            .get_issue(&issue_id)
            .unwrap()
            .clone();
        assert_eq!(stored.updated_at, "2026-01-01T00:00:00Z");
        assert_dual_mode_parity_for_current_mode();
    });
}
