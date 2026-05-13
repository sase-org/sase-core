use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::db::{
    projection_last_seq_tx, set_projection_last_seq_tx, ProjectionDb,
};
use super::error::ProjectionError;
use super::event::{EventEnvelopeWire, PROJECTION_EVENT_SCHEMA_VERSION};
use super::migrations::{applied_migrations, PROJECTION_SCHEMA_VERSION};

pub trait ProjectionApplier {
    fn projection_name(&self) -> &str;

    fn apply(
        &mut self,
        event: &EventEnvelopeWire,
        conn: &rusqlite::Connection,
    ) -> Result<(), ProjectionError>;
}

pub struct ReplayProjectionApplier<F>
where
    F: FnMut(
        &EventEnvelopeWire,
        &rusqlite::Connection,
    ) -> Result<(), ProjectionError>,
{
    projection_name: String,
    apply: F,
}

impl<F> ReplayProjectionApplier<F>
where
    F: FnMut(
        &EventEnvelopeWire,
        &rusqlite::Connection,
    ) -> Result<(), ProjectionError>,
{
    pub fn new(projection_name: impl Into<String>, apply: F) -> Self {
        Self {
            projection_name: projection_name.into(),
            apply,
        }
    }
}

impl<F> ProjectionApplier for ReplayProjectionApplier<F>
where
    F: FnMut(
        &EventEnvelopeWire,
        &rusqlite::Connection,
    ) -> Result<(), ProjectionError>,
{
    fn projection_name(&self) -> &str {
        &self.projection_name
    }

    fn apply(
        &mut self,
        event: &EventEnvelopeWire,
        conn: &rusqlite::Connection,
    ) -> Result<(), ProjectionError> {
        (self.apply)(event, conn)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectionReplayReportWire {
    pub schema_version: u32,
    pub events_seen: u64,
    pub events_applied: u64,
    pub last_seq: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectionGapWire {
    pub projection: String,
    pub last_seq: i64,
    pub max_event_seq: i64,
    pub missing_events: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectionRecoveryIssueWire {
    pub projection: String,
    pub kind: String,
    pub guidance: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectionStartupRepairReportWire {
    pub schema_version: u32,
    pub max_event_seq: i64,
    pub gaps: Vec<ProjectionGapWire>,
    pub replay: ProjectionReplayReportWire,
    pub recovery_issues: Vec<ProjectionRecoveryIssueWire>,
}

impl ProjectionDb {
    pub fn replay_events(
        &mut self,
        after_seq: i64,
        appliers: &mut [&mut dyn ProjectionApplier],
    ) -> Result<ProjectionReplayReportWire, ProjectionError> {
        let events = self.events_after(after_seq, None)?;
        let mut report = ProjectionReplayReportWire {
            schema_version: PROJECTION_EVENT_SCHEMA_VERSION,
            events_seen: events.len() as u64,
            events_applied: 0,
            last_seq: after_seq,
        };

        for event in events {
            self.with_immediate_transaction(|conn| {
                for applier in appliers.iter_mut() {
                    applier.apply(&event, conn)?;
                    set_projection_last_seq_tx(
                        conn,
                        applier.projection_name(),
                        event.seq,
                    )?;
                }
                Ok(())
            })?;
            report.events_applied += 1;
            report.last_seq = event.seq;
        }

        Ok(report)
    }

    pub fn repair_projection_gaps(
        &mut self,
        appliers: &mut [&mut dyn ProjectionApplier],
    ) -> Result<ProjectionStartupRepairReportWire, ProjectionError> {
        let max_event_seq = self.max_event_seq()?;
        let mut last_by_projection = BTreeMap::new();
        let mut gaps = Vec::new();
        for applier in appliers.iter_mut() {
            let projection = applier.projection_name().to_string();
            let last_seq =
                projection_last_seq_tx(self.connection(), &projection)?;
            if last_seq < max_event_seq {
                gaps.push(ProjectionGapWire {
                    projection: projection.clone(),
                    last_seq,
                    max_event_seq,
                    missing_events: (max_event_seq - last_seq) as u64,
                });
            }
            last_by_projection.insert(projection, last_seq);
        }

        let mut recovery_issues = schema_recovery_issues(self.connection())?;
        let after_seq = last_by_projection
            .values()
            .copied()
            .min()
            .unwrap_or(max_event_seq);
        let events = self.events_after(after_seq, None)?;
        let mut replay = ProjectionReplayReportWire {
            schema_version: PROJECTION_EVENT_SCHEMA_VERSION,
            events_seen: events.len() as u64,
            events_applied: 0,
            last_seq: after_seq,
        };

        for event in events {
            self.with_immediate_transaction(|conn| {
                for applier in appliers.iter_mut() {
                    let projection = applier.projection_name().to_string();
                    let last_seq = last_by_projection
                        .get(&projection)
                        .copied()
                        .unwrap_or(0);
                    if event.seq <= last_seq {
                        continue;
                    }
                    applier.apply(&event, conn)?;
                    set_projection_last_seq_tx(conn, &projection, event.seq)?;
                    last_by_projection.insert(projection, event.seq);
                }
                Ok(())
            })?;
            replay.events_applied += 1;
            replay.last_seq = event.seq;
        }

        for (projection, last_seq) in last_by_projection {
            if last_seq < max_event_seq {
                recovery_issues.push(ProjectionRecoveryIssueWire {
                    projection,
                    kind: "non_replayable_gap".to_string(),
                    guidance: "drop and rebuild this projection from the retained event log".to_string(),
                });
            }
        }

        Ok(ProjectionStartupRepairReportWire {
            schema_version: PROJECTION_EVENT_SCHEMA_VERSION,
            max_event_seq,
            gaps,
            replay,
            recovery_issues,
        })
    }
}

fn schema_recovery_issues(
    conn: &rusqlite::Connection,
) -> Result<Vec<ProjectionRecoveryIssueWire>, ProjectionError> {
    let latest = applied_migrations(conn)?
        .last()
        .map(|migration| migration.version)
        .unwrap_or(0);
    if latest == PROJECTION_SCHEMA_VERSION {
        return Ok(Vec::new());
    }
    Ok(vec![ProjectionRecoveryIssueWire {
        projection: "all".to_string(),
        kind: "schema_version_mismatch".to_string(),
        guidance: format!(
            "run projection migrations before replay; database is at {latest}, code expects {PROJECTION_SCHEMA_VERSION}"
        ),
    }])
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::projections::event::{EventAppendRequestWire, EventSourceWire};

    fn request(event_type: &str) -> EventAppendRequestWire {
        EventAppendRequestWire {
            created_at: Some("2026-05-13T21:00:00.000Z".to_string()),
            source: EventSourceWire {
                source_type: "test".to_string(),
                name: "projection-test".to_string(),
                ..EventSourceWire::default()
            },
            host_id: "host-a".to_string(),
            project_id: "project-a".to_string(),
            event_type: event_type.to_string(),
            payload: json!({}),
            idempotency_key: None,
            causality: vec![],
            source_path: None,
            source_revision: None,
        }
    }

    #[test]
    fn replay_dispatches_events_in_sequence() {
        let mut db = ProjectionDb::open_in_memory().unwrap();
        db.append_event(request("one")).unwrap();
        db.append_event(request("two")).unwrap();

        db.connection()
            .execute(
                "CREATE TABLE replay_seen(seq INTEGER PRIMARY KEY, event_type TEXT NOT NULL)",
                [],
            )
            .unwrap();

        let mut applier = ReplayProjectionApplier::new(
            "replay_seen",
            |event: &EventEnvelopeWire, conn: &rusqlite::Connection| {
                conn.execute(
                    "INSERT INTO replay_seen(seq, event_type) VALUES (?1, ?2)",
                    rusqlite::params![event.seq, event.event_type],
                )?;
                Ok(())
            },
        );
        let report = db.replay_events(0, &mut [&mut applier]).unwrap();

        assert_eq!(report.events_seen, 2);
        assert_eq!(report.events_applied, 2);
        assert_eq!(report.last_seq, 2);
        assert_eq!(db.projection_last_seq("replay_seen").unwrap(), 2);

        let rows: Vec<String> = {
            let mut stmt = db
                .connection()
                .prepare("SELECT event_type FROM replay_seen ORDER BY seq")
                .unwrap();
            stmt.query_map([], |row| row.get::<_, String>(0))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap()
        };
        assert_eq!(rows, vec!["one".to_string(), "two".to_string()]);
    }
}
