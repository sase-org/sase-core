use serde::{Deserialize, Serialize};

use super::agents::AgentProjectionApplier;
use super::beads::BeadProjectionApplier;
use super::catalogs::CatalogProjectionApplier;
use super::changespec::ChangeSpecProjectionApplier;
use super::db::{append_event_tx, ProjectionDb};
use super::error::ProjectionError;
use super::event::{EventAppendRequestWire, PROJECTION_EVENT_SCHEMA_VERSION};
use super::migrations::{recreate_projection_tables, MigrationWire};
use super::notifications::NotificationProjectionApplier;
use super::replay::{ProjectionApplier, ProjectionReplayReportWire};
use super::workflows::WorkflowProjectionApplier;

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ProjectionRebuildOptionsWire {
    #[serde(default)]
    pub seed_events: Vec<EventAppendRequestWire>,
    #[serde(default)]
    pub source_diffs: Vec<ProjectionSourceDiffWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectionTableResetReportWire {
    pub schema_version: u32,
    pub event_count_before: i64,
    pub event_count_after: i64,
    pub recreated_migrations: Vec<MigrationWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectionSourceDiffWire {
    pub schema_version: u32,
    pub project_id: String,
    pub domain: String,
    pub handle: String,
    pub diff_kind: String,
    pub source_fingerprint: Option<String>,
    pub projection_fingerprint: Option<String>,
    pub guidance: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProjectionRebuildReportWire {
    pub schema_version: u32,
    pub seeded_events: u64,
    pub duplicate_seed_events: u64,
    pub reset: ProjectionTableResetReportWire,
    pub replay: ProjectionReplayReportWire,
    pub source_diffs: Vec<ProjectionSourceDiffWire>,
}

impl ProjectionDb {
    pub fn rebuild_all_projections(
        &mut self,
        options: ProjectionRebuildOptionsWire,
    ) -> Result<ProjectionRebuildReportWire, ProjectionError> {
        let mut changespec = ChangeSpecProjectionApplier;
        let mut notifications = NotificationProjectionApplier;
        let mut agents = AgentProjectionApplier;
        let mut beads = BeadProjectionApplier;
        let mut workflows = WorkflowProjectionApplier;
        let mut catalogs = CatalogProjectionApplier;
        self.rebuild_projections(
            options,
            &mut [
                &mut changespec,
                &mut notifications,
                &mut agents,
                &mut beads,
                &mut workflows,
                &mut catalogs,
            ],
        )
    }

    pub fn rebuild_projections(
        &mut self,
        options: ProjectionRebuildOptionsWire,
        appliers: &mut [&mut dyn ProjectionApplier],
    ) -> Result<ProjectionRebuildReportWire, ProjectionError> {
        let source_diffs = options.source_diffs;
        let mut seeded_events = 0;
        let mut duplicate_seed_events = 0;
        for request in options.seed_events {
            let outcome = self.with_immediate_transaction(|conn| {
                append_event_tx(conn, request)
            })?;
            if outcome.duplicate {
                duplicate_seed_events += 1;
            } else {
                seeded_events += 1;
            }
        }

        let reset = self.reset_projection_tables()?;
        let replay = self.replay_events(0, appliers)?;
        Ok(ProjectionRebuildReportWire {
            schema_version: PROJECTION_EVENT_SCHEMA_VERSION,
            seeded_events,
            duplicate_seed_events,
            reset,
            replay,
            source_diffs,
        })
    }

    pub fn reset_projection_tables(
        &mut self,
    ) -> Result<ProjectionTableResetReportWire, ProjectionError> {
        let event_count_before = self.event_count()?;
        let recreated_migrations = self.with_immediate_transaction(|conn| {
            recreate_projection_tables(conn)
        })?;
        let event_count_after = self.event_count()?;
        Ok(ProjectionTableResetReportWire {
            schema_version: PROJECTION_EVENT_SCHEMA_VERSION,
            event_count_before,
            event_count_after,
            recreated_migrations,
        })
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::bead::{IssueTypeWire, IssueWire, StatusWire};
    use crate::projections::beads::{
        bead_created_event_request, bead_projection_issues,
        BeadProjectionEventContextWire,
    };
    use crate::projections::event::EventSourceWire;

    fn source() -> EventSourceWire {
        EventSourceWire {
            source_type: "test".to_string(),
            name: "projection-rebuild-test".to_string(),
            ..EventSourceWire::default()
        }
    }

    fn raw_event(event_type: &str, id: usize) -> EventAppendRequestWire {
        EventAppendRequestWire {
            created_at: Some(format!("2026-05-13T22:{id:02}:00.000Z")),
            source: source(),
            host_id: "host-a".to_string(),
            project_id: "project-a".to_string(),
            event_type: event_type.to_string(),
            payload: json!({"id": id}),
            idempotency_key: Some(format!("raw-{event_type}-{id}")),
            causality: Vec::new(),
            source_path: None,
            source_revision: None,
        }
    }

    fn bead_context(key: &str) -> BeadProjectionEventContextWire {
        BeadProjectionEventContextWire {
            created_at: Some("2026-05-13T23:00:00.000Z".to_string()),
            source: source(),
            host_id: "host-a".to_string(),
            project_id: "project-a".to_string(),
            idempotency_key: Some(key.to_string()),
            causality: Vec::new(),
            source_path: None,
            source_revision: None,
        }
    }

    fn issue(id: &str, title: &str) -> IssueWire {
        IssueWire {
            id: id.to_string(),
            title: title.to_string(),
            status: StatusWire::Open,
            issue_type: IssueTypeWire::Phase,
            tier: None,
            parent_id: Some("epic-1".to_string()),
            owner: "owner@example.com".to_string(),
            assignee: String::new(),
            created_at: "2026-05-13T23:00:00Z".to_string(),
            created_by: "owner@example.com".to_string(),
            updated_at: "2026-05-13T23:00:00Z".to_string(),
            closed_at: None,
            close_reason: None,
            description: String::new(),
            notes: String::new(),
            design: String::new(),
            model: String::new(),
            is_ready_to_work: false,
            epic_count: None,
            changespec_name: String::new(),
            changespec_bug_id: String::new(),
            dependencies: Vec::new(),
        }
    }

    #[test]
    fn reset_and_rebuild_preserves_event_log_and_materialized_rows() {
        let mut db = ProjectionDb::open_in_memory().unwrap();
        db.append_bead_event(
            bead_created_event_request(
                bead_context("bead-a"),
                issue("phase-a", "Phase A"),
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(
            bead_projection_issues(db.connection(), "project-a")
                .unwrap()
                .len(),
            1
        );

        db.reset_projection_tables().unwrap();
        assert_eq!(db.event_count().unwrap(), 1);
        assert!(bead_projection_issues(db.connection(), "project-a")
            .unwrap()
            .is_empty());

        let report = db
            .rebuild_all_projections(ProjectionRebuildOptionsWire::default())
            .unwrap();
        assert_eq!(report.reset.event_count_before, 1);
        assert_eq!(report.reset.event_count_after, 1);
        assert_eq!(report.replay.last_seq, 1);
        assert_eq!(
            bead_projection_issues(db.connection(), "project-a")
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn mixed_domain_event_log_rebuilds_deterministically() {
        let mut db = ProjectionDb::open_in_memory().unwrap();
        let event_types = [
            "changespec.ephemeral_tick",
            "notification.ephemeral_tick",
            "agent.ephemeral_tick",
            "bead.ephemeral_tick",
            "workflow.ephemeral_tick",
            "catalog.ephemeral_tick",
        ];
        for idx in 0..30 {
            db.append_event(raw_event(
                event_types[idx % event_types.len()],
                idx,
            ))
            .unwrap();
        }

        let report = db
            .rebuild_all_projections(ProjectionRebuildOptionsWire::default())
            .unwrap();

        assert_eq!(report.replay.events_seen, 30);
        assert_eq!(report.replay.events_applied, 30);
        assert_eq!(report.replay.last_seq, 30);
        assert_eq!(db.event_count().unwrap(), 30);
    }
}
