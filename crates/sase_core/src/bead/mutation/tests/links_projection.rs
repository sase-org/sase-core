use super::super::*;
use super::support::dual_tempdir as tempdir;
use super::support::*;
use crate::bead::config::default_config;
use crate::bead::config::save_config;
use crate::bead::mutation::store::MutableStore;
use crate::bead::wire::BeadReopenCauseWire;
use crate::bead::wire::BeadResolutionWire;
use crate::bead::wire::IssueTypeWire;
use crate::bead::wire::StatusWire;
use std::fs;

#[test]
fn event_store_mutation_leaves_issues_jsonl_without_a_diff() {
    run_dual_mode_test(|_mode| {
        // projection-off acceptance: once a store has an event store, a
        // mutation appends events but never rewrites the compatibility
        // projection. The on-demand export picks the mutation up instead.
        let temp = tempdir();
        let beads_dir = temp.path().join("sdd/beads");
        fs::create_dir_all(&beads_dir).unwrap();
        save_config(&beads_dir, &default_config("sase", "owner@example.com"))
            .unwrap();
        fs::write(beads_dir.join("issues.jsonl"), "").unwrap();

        let epic = create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "Exported epic".to_string(),
                issue_type: IssueTypeWire::Plan,
                now: Some("2026-01-01T00:00:00Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap();
        export_jsonl(&beads_dir).unwrap();
        let before = fs::read(beads_dir.join("issues.jsonl")).unwrap();

        update_issue(
            &beads_dir,
            &epic.id,
            BeadUpdateFieldsWire {
                title: Some("Renamed epic".to_string()),
                now: Some("2026-01-01T00:01:00Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(
            fs::read(beads_dir.join("issues.jsonl")).unwrap(),
            before,
            "mutation must not rewrite issues.jsonl"
        );

        export_jsonl(&beads_dir).unwrap();
        let regenerated =
            fs::read_to_string(beads_dir.join("issues.jsonl")).unwrap();
        assert!(regenerated.contains(r#""title":"Renamed epic""#));
        assert_dual_mode_parity_for_current_mode();
    });
}

#[test]
fn open_issue_no_longer_leaves_stale_close_metadata_in_the_projection() {
    run_dual_mode_test(|_mode| {
        let (_temp, beads_dir, ids) = close_history_fixture();
        let phase_id = ids[1].clone();
        close_for_history(&beads_dir, &phase_id, "2026-01-02T00:00:00Z");
        open_issue(
            &beads_dir,
            &phase_id,
            Some("2026-01-04T00:00:00Z".to_string()),
        )
        .unwrap();

        let (projected, _) = projected_and_reduced(&beads_dir, &phase_id);
        assert_eq!(projected.status, StatusWire::Open);
        assert_eq!(projected.closed_at, None);
        assert_eq!(projected.close_reason, None);
        assert_eq!(projected.close_history.len(), 1);
        assert_eq!(
            projected.close_history[0].reopened_via,
            BeadReopenCauseWire::Open
        );
        assert_eq!(projected.close_history[0].reopened_by, None);
        assert_eq!(
            projected.close_history[0].reopened_at.as_str(),
            "2026-01-04T00:00:00Z"
        );
        assert_dual_mode_parity_for_current_mode();
    });
}

#[test]
fn a_pre_close_history_projection_recovers_its_reason_on_the_next_load() {
    run_dual_mode_test(|_mode| {
        let (_temp, beads_dir, ids) = close_history_fixture();
        let task_id = ids[2].clone();
        close_for_history(&beads_dir, &task_id, "2026-01-02T00:00:00Z");
        add_task_plus_one(
            &beads_dir,
            &task_id,
            "claude.probe",
            "Still flaky.",
            &[],
            Some("2026-01-03T00:00:00Z".to_string()),
            None,
            None,
        )
        .unwrap();

        // Rewrite issues.jsonl the way a pre-change build left it: the close
        // reason destroyed, no close_history key at all. Only the event log
        // still holds the bytes.
        let jsonl = fs::read_to_string(beads_dir.join("issues.jsonl")).unwrap();
        let damaged = jsonl
            .lines()
            .map(|line| {
                let mut row: serde_json::Value =
                    serde_json::from_str(line).unwrap();
                row.as_object_mut().unwrap().remove("close_history");
                serde_json::to_string(&row).unwrap()
            })
            .collect::<Vec<_>>()
            .join("\n")
            + "\n";
        assert!(!damaged.contains("close_history"));
        fs::write(beads_dir.join("issues.jsonl"), damaged).unwrap();

        let recovered = MutableStore::load(&beads_dir)
            .unwrap()
            .get_issue(&task_id)
            .unwrap()
            .clone();
        assert_eq!(recovered.close_history.len(), 1);
        assert_eq!(
            recovered.close_history[0].close_reason.as_deref(),
            Some("Not reproducible on main.")
        );
        assert_eq!(
            recovered.close_history[0].resolution,
            Some(BeadResolutionWire::Canceled)
        );
        assert_dual_mode_parity_for_current_mode();
    });
}
