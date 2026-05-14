use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use rusqlite::{params, Connection, OpenFlags};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use super::db::{max_event_seq_tx, ProjectionDb};
use super::error::ProjectionError;
use super::event::PROJECTION_EVENT_SCHEMA_VERSION;
use super::indexing::{
    SourceChangeOperationWire, SourceChangeWire, SourceIdentityWire,
    INDEXING_WIRE_SCHEMA_VERSION,
};
use super::migrations::PROJECTION_SCHEMA_VERSION;

pub const DEFAULT_WAL_SOFT_CAP_BYTES: u64 = 1_073_741_824;
pub const DEFAULT_CHECKPOINT_INTERVAL_SECS: u64 = 600;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionWalCheckpointModeWire {
    Passive,
    Full,
    Restart,
    Truncate,
}

impl ProjectionWalCheckpointModeWire {
    fn pragma_name(self) -> &'static str {
        match self {
            Self::Passive => "PASSIVE",
            Self::Full => "FULL",
            Self::Restart => "RESTART",
            Self::Truncate => "TRUNCATE",
        }
    }
}

impl Default for ProjectionWalCheckpointModeWire {
    fn default() -> Self {
        Self::Passive
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectionCheckpointPolicyWire {
    pub wal_soft_cap_bytes: u64,
    pub checkpoint_interval_secs: u64,
}

impl Default for ProjectionCheckpointPolicyWire {
    fn default() -> Self {
        Self {
            wal_soft_cap_bytes: DEFAULT_WAL_SOFT_CAP_BYTES,
            checkpoint_interval_secs: DEFAULT_CHECKPOINT_INTERVAL_SECS,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectionCheckpointDecisionWire {
    pub schema_version: u32,
    pub should_checkpoint: bool,
    pub wal_bytes: u64,
    pub seconds_since_last_checkpoint: u64,
    pub reasons: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectionCheckpointReportWire {
    pub schema_version: u32,
    pub mode: ProjectionWalCheckpointModeWire,
    pub busy: i64,
    pub log_frames: i64,
    pub checkpointed_frames: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectionBackupReportWire {
    pub schema_version: u32,
    pub path: String,
    pub metadata_path: String,
    pub bytes: u64,
    pub metadata: ProjectionBackupMetadataWire,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectionBackupMetadataWire {
    pub schema_version: u32,
    pub backup_format_version: u32,
    pub daemon_version: String,
    pub core_version: String,
    pub host_identity: String,
    pub source_sase_home: String,
    pub created_at: String,
    pub projection_schema: u32,
    pub event_max_sequence: i64,
    pub source_export_summary: JsonValue,
    pub original_db_path: String,
    pub snapshot_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectionBackupListWire {
    pub schema_version: u32,
    pub backups: Vec<ProjectionBackupReportWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectionRestoreReportWire {
    pub schema_version: u32,
    pub backup_path: String,
    pub restored_path: String,
    pub bytes: u64,
    pub replaced_existing: bool,
    pub projection_only: bool,
    pub metadata: ProjectionBackupMetadataWire,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectionRetentionPolicyWire {
    #[serde(default)]
    pub ephemeral_event_types: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compact_before_seq: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compact_created_before: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectionRetentionReportWire {
    pub schema_version: u32,
    pub deleted_events: u64,
    pub retained_events: i64,
    pub max_event_seq: i64,
    pub compacted_event_types: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectionReconciliationDiagnosticWire {
    pub schema_version: u32,
    pub domain: String,
    pub source_path: String,
    pub reason: String,
    pub guidance: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectionReconciliationPlanWire {
    pub schema_version: u32,
    pub scanned_sources: u64,
    pub known_sources: u64,
    pub created_sources: u64,
    pub stale_sources: u64,
    pub deleted_sources: u64,
    pub bounded: bool,
    pub changes: Vec<SourceChangeWire>,
    pub diagnostics: Vec<ProjectionReconciliationDiagnosticWire>,
}

pub fn projection_checkpoint_decision(
    wal_bytes: u64,
    seconds_since_last_checkpoint: u64,
    policy: ProjectionCheckpointPolicyWire,
) -> ProjectionCheckpointDecisionWire {
    let mut reasons = Vec::new();
    if wal_bytes >= policy.wal_soft_cap_bytes {
        reasons.push("wal_soft_cap_bytes".to_string());
    }
    if seconds_since_last_checkpoint >= policy.checkpoint_interval_secs {
        reasons.push("checkpoint_interval_secs".to_string());
    }
    ProjectionCheckpointDecisionWire {
        schema_version: PROJECTION_EVENT_SCHEMA_VERSION,
        should_checkpoint: !reasons.is_empty(),
        wal_bytes,
        seconds_since_last_checkpoint,
        reasons,
    }
}

pub fn projection_reconciliation_plan(
    known_sources: impl IntoIterator<Item = SourceIdentityWire>,
    discovered_sources: impl IntoIterator<Item = SourceIdentityWire>,
    max_changes: Option<usize>,
) -> ProjectionReconciliationPlanWire {
    let known = known_sources
        .into_iter()
        .map(|identity| (identity.stable_key(), identity))
        .collect::<BTreeMap<_, _>>();
    let discovered = discovered_sources
        .into_iter()
        .map(|identity| (identity.stable_key(), identity))
        .collect::<BTreeMap<_, _>>();

    let mut plan = ProjectionReconciliationPlanWire {
        schema_version: INDEXING_WIRE_SCHEMA_VERSION,
        scanned_sources: discovered.len() as u64,
        known_sources: known.len() as u64,
        ..ProjectionReconciliationPlanWire::default()
    };

    for (key, discovered_identity) in &discovered {
        match known.get(key) {
            None => {
                plan.created_sources += 1;
                plan.changes.push(reconciliation_change(
                    discovered_identity.clone(),
                    SourceChangeOperationWire::Reconcile,
                    "source discovered during reconciliation",
                ));
            }
            Some(known_identity)
                if known_identity.fingerprint
                    != discovered_identity.fingerprint =>
            {
                plan.stale_sources += 1;
                plan.changes.push(reconciliation_change(
                    discovered_identity.clone(),
                    SourceChangeOperationWire::Reconcile,
                    "source fingerprint changed since last index",
                ));
            }
            Some(_) => {}
        }
    }

    for (key, known_identity) in &known {
        if discovered.contains_key(key) {
            continue;
        }
        plan.deleted_sources += 1;
        plan.changes.push(reconciliation_change(
            known_identity.clone(),
            SourceChangeOperationWire::Delete,
            "source missing during reconciliation",
        ));
    }

    plan.changes.sort_by(|left, right| {
        left.identity
            .stable_key()
            .cmp(&right.identity.stable_key())
            .then_with(|| {
                operation_sort_key(&left.operation)
                    .cmp(&operation_sort_key(&right.operation))
            })
    });

    if let Some(max_changes) = max_changes {
        if plan.changes.len() > max_changes {
            plan.bounded = true;
            plan.changes.truncate(max_changes);
            plan.diagnostics.push(ProjectionReconciliationDiagnosticWire {
                schema_version: INDEXING_WIRE_SCHEMA_VERSION,
                domain: "indexing".to_string(),
                source_path: String::new(),
                reason: "reconciliation_change_bound_exceeded".to_string(),
                guidance: format!(
                    "Reconciliation found more than {max_changes} source changes; run a daemon indexing rebuild or raise the reconciliation bound."
                ),
            });
        }
    }

    plan
}

fn reconciliation_change(
    identity: SourceIdentityWire,
    operation: SourceChangeOperationWire,
    reason: &str,
) -> SourceChangeWire {
    SourceChangeWire {
        schema_version: INDEXING_WIRE_SCHEMA_VERSION,
        identity,
        operation,
        reason: Some(reason.to_string()),
    }
}

fn operation_sort_key(operation: &SourceChangeOperationWire) -> u8 {
    match operation {
        SourceChangeOperationWire::Delete => 0,
        SourceChangeOperationWire::Reconcile => 1,
        SourceChangeOperationWire::Rewrite => 2,
        SourceChangeOperationWire::Upsert => 3,
    }
}

impl ProjectionDb {
    pub fn wal_checkpoint(
        &self,
        mode: ProjectionWalCheckpointModeWire,
    ) -> Result<ProjectionCheckpointReportWire, ProjectionError> {
        let sql = format!("PRAGMA wal_checkpoint({})", mode.pragma_name());
        let (busy, log_frames, checkpointed_frames) =
            self.connection().query_row(&sql, [], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            })?;
        Ok(ProjectionCheckpointReportWire {
            schema_version: PROJECTION_EVENT_SCHEMA_VERSION,
            mode,
            busy,
            log_frames,
            checkpointed_frames,
        })
    }

    pub fn backup_vacuum_into(
        &self,
        backup_path: &Path,
    ) -> Result<ProjectionBackupReportWire, ProjectionError> {
        let metadata = ProjectionBackupMetadataWire {
            schema_version: PROJECTION_EVENT_SCHEMA_VERSION,
            backup_format_version: 1,
            daemon_version: "unknown".to_string(),
            core_version: env!("CARGO_PKG_VERSION").to_string(),
            host_identity: "unknown".to_string(),
            source_sase_home: String::new(),
            created_at: String::new(),
            projection_schema: PROJECTION_SCHEMA_VERSION,
            event_max_sequence: max_event_seq_tx(self.connection())?,
            source_export_summary: JsonValue::Null,
            original_db_path: String::new(),
            snapshot_path: backup_path.display().to_string(),
        };
        self.backup_vacuum_into_with_metadata(backup_path, metadata)
    }

    pub fn backup_vacuum_into_with_metadata(
        &self,
        backup_path: &Path,
        mut metadata: ProjectionBackupMetadataWire,
    ) -> Result<ProjectionBackupReportWire, ProjectionError> {
        if let Some(parent) = backup_path.parent() {
            fs::create_dir_all(parent)?;
        }
        let path = backup_path.to_string_lossy().to_string();
        self.connection()
            .execute("VACUUM main INTO ?1", params![path])?;
        let bytes = fs::metadata(backup_path)?.len();
        metadata.schema_version = PROJECTION_EVENT_SCHEMA_VERSION;
        metadata.backup_format_version = 1;
        metadata.projection_schema = PROJECTION_SCHEMA_VERSION;
        metadata.event_max_sequence = max_event_seq_tx(self.connection())?;
        metadata.snapshot_path = backup_path.display().to_string();
        let metadata_path = projection_backup_metadata_path(backup_path);
        let metadata_json = serde_json::to_vec_pretty(&metadata)?;
        fs::write(&metadata_path, metadata_json)?;
        Ok(ProjectionBackupReportWire {
            schema_version: PROJECTION_EVENT_SCHEMA_VERSION,
            path: backup_path.display().to_string(),
            metadata_path: metadata_path.display().to_string(),
            bytes,
            metadata,
        })
    }

    pub fn compact_ephemeral_events(
        &mut self,
        policy: ProjectionRetentionPolicyWire,
    ) -> Result<ProjectionRetentionReportWire, ProjectionError> {
        let mut compacted_event_types = policy.ephemeral_event_types.clone();
        compacted_event_types.sort();
        compacted_event_types.dedup();

        let deleted_events = self.with_immediate_transaction(|conn| {
            let mut deleted_events = 0;
            for event_type in &compacted_event_types {
                conn.execute(
                    r#"
                    DELETE FROM event_log
                    WHERE event_type = ?1
                      AND (?2 IS NULL OR seq < ?2)
                      AND (?3 IS NULL OR created_at < ?3)
                    "#,
                    params![
                        event_type,
                        policy.compact_before_seq,
                        policy.compact_created_before,
                    ],
                )?;
                deleted_events += conn.changes();
            }
            Ok(deleted_events)
        })?;

        Ok(ProjectionRetentionReportWire {
            schema_version: PROJECTION_EVENT_SCHEMA_VERSION,
            deleted_events,
            retained_events: self.event_count()?,
            max_event_seq: max_event_seq_tx(self.connection())?,
            compacted_event_types,
        })
    }
}

pub fn projection_backup_metadata_path(backup_path: &Path) -> PathBuf {
    let file_name = backup_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("projection.sqlite");
    backup_path.with_file_name(format!("{file_name}.json"))
}

pub fn list_projection_backups(
    backups_dir: &Path,
) -> Result<ProjectionBackupListWire, ProjectionError> {
    let mut backups = Vec::new();
    if !backups_dir.exists() {
        return Ok(ProjectionBackupListWire {
            schema_version: PROJECTION_EVENT_SCHEMA_VERSION,
            backups,
        });
    }
    for entry in fs::read_dir(backups_dir)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file()
            || path.extension().and_then(|ext| ext.to_str()) != Some("sqlite")
        {
            continue;
        }
        let metadata_path = projection_backup_metadata_path(&path);
        if !metadata_path.is_file() {
            continue;
        }
        let metadata = read_projection_backup_metadata(&metadata_path)?;
        let bytes = fs::metadata(&path)?.len();
        backups.push(ProjectionBackupReportWire {
            schema_version: PROJECTION_EVENT_SCHEMA_VERSION,
            path: path.display().to_string(),
            metadata_path: metadata_path.display().to_string(),
            bytes,
            metadata,
        });
    }
    backups.sort_by(|left, right| {
        right
            .metadata
            .created_at
            .cmp(&left.metadata.created_at)
            .then_with(|| right.path.cmp(&left.path))
    });
    Ok(ProjectionBackupListWire {
        schema_version: PROJECTION_EVENT_SCHEMA_VERSION,
        backups,
    })
}

pub fn read_projection_backup_metadata(
    metadata_path: &Path,
) -> Result<ProjectionBackupMetadataWire, ProjectionError> {
    let bytes = fs::read(metadata_path)?;
    Ok(serde_json::from_slice(&bytes)?)
}

pub fn validate_projection_backup_file(
    backup_path: &Path,
) -> Result<(), ProjectionError> {
    if !backup_path.is_file() {
        return Err(ProjectionError::Invariant(format!(
            "projection backup {} does not exist or is not a file",
            backup_path.display()
        )));
    }
    let conn = Connection::open_with_flags(
        backup_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY,
    )?;
    let event_log_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'event_log'",
        [],
        |row| row.get(0),
    )?;
    if event_log_count != 1 {
        return Err(ProjectionError::Invariant(format!(
            "projection backup {} is missing event_log",
            backup_path.display()
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use tempfile::tempdir;

    use super::*;
    use crate::projections::event::{EventAppendRequestWire, EventSourceWire};
    use crate::projections::{SourceFingerprintWire, SourceIdentityWire};

    fn request(event_type: &str, key: &str) -> EventAppendRequestWire {
        EventAppendRequestWire {
            created_at: Some("2026-05-13T22:00:00.000Z".to_string()),
            source: EventSourceWire {
                source_type: "test".to_string(),
                name: "maintenance-test".to_string(),
                ..EventSourceWire::default()
            },
            host_id: "host-a".to_string(),
            project_id: "project-a".to_string(),
            event_type: event_type.to_string(),
            payload: json!({}),
            idempotency_key: Some(key.to_string()),
            causality: Vec::new(),
            source_path: None,
            source_revision: None,
        }
    }

    fn identity(path: &str, size: u64) -> SourceIdentityWire {
        SourceIdentityWire {
            schema_version: INDEXING_WIRE_SCHEMA_VERSION,
            domain: "changespec".to_string(),
            project_id: Some("project-a".to_string()),
            source_path: path.to_string(),
            is_archive: false,
            fingerprint: Some(SourceFingerprintWire {
                schema_version: INDEXING_WIRE_SCHEMA_VERSION,
                file_size: Some(size),
                modified_at_unix_millis: Some(1),
                inode: Some(size),
                content_sha256: None,
            }),
            last_indexed_event_seq: None,
        }
    }

    #[test]
    fn checkpoint_decision_uses_size_or_age_soft_cap() {
        let policy = ProjectionCheckpointPolicyWire::default();
        assert!(
            !projection_checkpoint_decision(1, 1, policy.clone())
                .should_checkpoint
        );
        assert!(projection_checkpoint_decision(
            DEFAULT_WAL_SOFT_CAP_BYTES,
            1,
            policy.clone()
        )
        .reasons
        .contains(&"wal_soft_cap_bytes".to_string()));
        assert!(projection_checkpoint_decision(1, 600, policy)
            .reasons
            .contains(&"checkpoint_interval_secs".to_string()));
    }

    #[test]
    fn checkpoint_and_backup_work_against_sqlite_file() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("projection.sqlite");
        let backup_path = dir.path().join("backup.sqlite");
        let mut db = ProjectionDb::open(&db_path).unwrap();
        db.append_event(request("durable.user_event", "durable"))
            .unwrap();

        let checkpoint = db
            .wal_checkpoint(ProjectionWalCheckpointModeWire::Truncate)
            .unwrap();
        assert_eq!(checkpoint.mode, ProjectionWalCheckpointModeWire::Truncate);

        let backup = db.backup_vacuum_into(&backup_path).unwrap();
        assert!(backup.bytes > 0);
        assert!(backup_path.is_file());
        assert!(Path::new(&backup.metadata_path).is_file());
        validate_projection_backup_file(&backup_path).unwrap();

        let list = list_projection_backups(dir.path()).unwrap();
        assert_eq!(list.backups.len(), 1);
        assert_eq!(list.backups[0].metadata.event_max_sequence, 1);
    }

    #[test]
    fn retention_compacts_only_declared_ephemeral_event_types() {
        let mut db = ProjectionDb::open_in_memory().unwrap();
        db.append_event(request("daemon.tick", "tick-1")).unwrap();
        db.append_event(request("durable.user_event", "durable"))
            .unwrap();
        db.append_event(request("daemon.tick", "tick-2")).unwrap();

        let report = db
            .compact_ephemeral_events(ProjectionRetentionPolicyWire {
                ephemeral_event_types: vec!["daemon.tick".to_string()],
                compact_before_seq: Some(3),
                compact_created_before: None,
            })
            .unwrap();

        assert_eq!(report.deleted_events, 1);
        assert_eq!(report.retained_events, 2);
        assert!(db
            .get_event(2)
            .unwrap()
            .expect("durable event is retained")
            .event_type
            .starts_with("durable."));
        assert!(db.get_event(3).unwrap().is_some());
    }

    #[test]
    fn reconciliation_plan_detects_created_stale_and_deleted_sources() {
        let unchanged = identity("/tmp/unchanged.sase", 10);
        let stale_known = identity("/tmp/stale.sase", 20);
        let mut stale_discovered = identity("/tmp/stale.sase", 21);
        stale_discovered
            .fingerprint
            .as_mut()
            .unwrap()
            .modified_at_unix_millis = Some(2);
        let deleted = identity("/tmp/deleted.sase", 30);
        let created = identity("/tmp/created.sase", 40);

        let plan = projection_reconciliation_plan(
            [unchanged.clone(), stale_known, deleted],
            [unchanged, stale_discovered, created],
            None,
        );

        assert_eq!(plan.created_sources, 1);
        assert_eq!(plan.stale_sources, 1);
        assert_eq!(plan.deleted_sources, 1);
        assert_eq!(plan.changes.len(), 3);
        assert!(plan.changes.iter().any(|change| {
            change.identity.source_path == "/tmp/deleted.sase"
                && change.operation == SourceChangeOperationWire::Delete
        }));
        assert!(plan.changes.iter().any(|change| {
            change.identity.source_path == "/tmp/stale.sase"
                && change.operation == SourceChangeOperationWire::Reconcile
        }));
    }

    #[test]
    fn reconciliation_plan_reports_bounded_changes() {
        let plan = projection_reconciliation_plan(
            Vec::<SourceIdentityWire>::new(),
            [identity("/tmp/a.sase", 1), identity("/tmp/b.sase", 2)],
            Some(1),
        );

        assert!(plan.bounded);
        assert_eq!(plan.changes.len(), 1);
        assert_eq!(plan.diagnostics.len(), 1);
        assert_eq!(
            plan.diagnostics[0].reason,
            "reconciliation_change_bound_exceeded"
        );
    }
}
