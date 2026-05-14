use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use fs2::FileExt;
use rusqlite::{params, Connection, OptionalExtension, Row};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};

use super::{
    source_fingerprint_from_path, EventEnvelopeWire, ProjectionError,
    SourceFingerprintWire, PROJECTION_EVENT_SCHEMA_VERSION,
};

pub const MUTATION_WIRE_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MutationActorWire {
    pub schema_version: u32,
    pub actor_type: String,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime: Option<String>,
}

impl Default for MutationActorWire {
    fn default() -> Self {
        Self {
            schema_version: MUTATION_WIRE_SCHEMA_VERSION,
            actor_type: String::new(),
            name: String::new(),
            version: None,
            runtime: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MutationConflictKindWire {
    ConflictStaleSource,
    IdempotencyConflict,
    SourceLockBusy,
    UnsupportedMutation,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MutationConflictWire {
    pub schema_version: u32,
    pub kind: MutationConflictKindWire,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_fingerprint: Option<SourceFingerprintWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actual_fingerprint: Option<SourceFingerprintWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<JsonValue>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceExportKindWire {
    AtomicJson,
    JsonlAppend,
    ProjectFile,
}

impl SourceExportKindWire {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::AtomicJson => "atomic_json",
            Self::JsonlAppend => "jsonl_append",
            Self::ProjectFile => "project_file",
        }
    }
}

impl std::str::FromStr for SourceExportKindWire {
    type Err = ProjectionError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "atomic_json" => Ok(Self::AtomicJson),
            "jsonl_append" => Ok(Self::JsonlAppend),
            "project_file" => Ok(Self::ProjectFile),
            other => Err(ProjectionError::Invariant(format!(
                "unknown source export kind {other:?}"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SourceExportPlanWire {
    pub schema_version: u32,
    pub target_path: String,
    pub kind: SourceExportKindWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_fingerprint: Option<SourceFingerprintWire>,
    pub content_sha256: String,
    pub content_utf8: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repair_context: Option<JsonValue>,
}

impl SourceExportPlanWire {
    pub fn new(
        target_path: impl Into<String>,
        kind: SourceExportKindWire,
        content_utf8: impl Into<String>,
    ) -> Self {
        let content_utf8 = content_utf8.into();
        Self {
            schema_version: MUTATION_WIRE_SCHEMA_VERSION,
            target_path: target_path.into(),
            kind,
            expected_fingerprint: None,
            content_sha256: sha256_hex(content_utf8.as_bytes()),
            content_utf8,
            repair_context: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceExportStatusWire {
    Pending,
    Applied,
    Failed,
    Conflict,
}

impl SourceExportStatusWire {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Applied => "applied",
            Self::Failed => "failed",
            Self::Conflict => "conflict",
        }
    }
}

impl std::str::FromStr for SourceExportStatusWire {
    type Err = ProjectionError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "pending" => Ok(Self::Pending),
            "applied" => Ok(Self::Applied),
            "failed" => Ok(Self::Failed),
            "conflict" => Ok(Self::Conflict),
            other => Err(ProjectionError::Invariant(format!(
                "unknown source export status {other:?}"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SourceExportReportWire {
    pub schema_version: u32,
    pub export_id: i64,
    pub event_seq: i64,
    pub target_path: String,
    pub kind: SourceExportKindWire,
    pub status: SourceExportStatusWire,
    pub attempts: i64,
    pub content_sha256: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actual_fingerprint: Option<SourceFingerprintWire>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SourceExportOutboxRowWire {
    pub schema_version: u32,
    pub export_id: i64,
    pub event_seq: i64,
    pub plan: SourceExportPlanWire,
    pub status: SourceExportStatusWire,
    pub attempts: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LocalDaemonMutationOutcomeWire {
    pub schema_version: u32,
    pub event_seq: i64,
    pub event_type: String,
    pub duplicate: bool,
    pub changed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource_handle: Option<String>,
    #[serde(default)]
    pub source_exports: Vec<SourceExportReportWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub projection_snapshot: Option<JsonValue>,
}

pub(crate) fn enqueue_source_exports_tx(
    conn: &Connection,
    event: &EventEnvelopeWire,
    plans: &[SourceExportPlanWire],
) -> Result<Vec<SourceExportReportWire>, ProjectionError> {
    let mut reports = Vec::with_capacity(plans.len());
    for plan in plans {
        if plan.schema_version != MUTATION_WIRE_SCHEMA_VERSION {
            return Err(ProjectionError::Invariant(format!(
                "source export plan schema {} is unsupported",
                plan.schema_version
            )));
        }
        let plan_json = serde_json::to_string(plan)?;
        let expected_json = serde_json::to_string(&plan.expected_fingerprint)?;
        conn.execute(
            r#"
            INSERT INTO source_export_outbox (
                event_seq, target_path, export_kind, expected_fingerprint_json,
                content_sha256, plan_json, status, repair_context_json
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'pending', ?7)
            "#,
            params![
                event.seq,
                plan.target_path,
                plan.kind.as_str(),
                expected_json,
                plan.content_sha256,
                plan_json,
                serde_json::to_string(&plan.repair_context)?,
            ],
        )?;
        let export_id = conn.last_insert_rowid();
        reports.push(SourceExportReportWire {
            schema_version: MUTATION_WIRE_SCHEMA_VERSION,
            export_id,
            event_seq: event.seq,
            target_path: plan.target_path.clone(),
            kind: plan.kind.clone(),
            status: SourceExportStatusWire::Pending,
            attempts: 0,
            content_sha256: plan.content_sha256.clone(),
            message: None,
            actual_fingerprint: None,
        });
    }
    Ok(reports)
}

pub(crate) fn source_exports_for_event_tx(
    conn: &Connection,
    event_seq: i64,
) -> Result<Vec<SourceExportReportWire>, ProjectionError> {
    let mut stmt = conn.prepare(
        r#"
        SELECT export_id, event_seq, target_path, export_kind, status, attempts,
               content_sha256, last_error
        FROM source_export_outbox
        WHERE event_seq = ?1
        ORDER BY export_id
        "#,
    )?;
    let rows = stmt.query_map([event_seq], report_from_row)?;
    let mut reports = Vec::new();
    for row in rows {
        reports.push(row?);
    }
    Ok(reports)
}

pub fn mark_source_export_applied_tx(
    conn: &Connection,
    export_id: i64,
) -> Result<SourceExportReportWire, ProjectionError> {
    update_export_status_tx(
        conn,
        export_id,
        SourceExportStatusWire::Applied,
        None,
        source_export_row_tx(conn, export_id)
            .ok()
            .flatten()
            .and_then(|row| {
                source_fingerprint_from_path(
                    Path::new(&row.plan.target_path),
                    true,
                )
            }),
    )
}

pub fn mark_source_export_failed_tx(
    conn: &Connection,
    export_id: i64,
    message: impl Into<String>,
) -> Result<SourceExportReportWire, ProjectionError> {
    update_export_status_tx(
        conn,
        export_id,
        SourceExportStatusWire::Failed,
        Some(message.into()),
        None,
    )
}

pub fn list_pending_source_exports_tx(
    conn: &Connection,
) -> Result<Vec<SourceExportOutboxRowWire>, ProjectionError> {
    let mut stmt = conn.prepare(
        r#"
        SELECT export_id, event_seq, plan_json, status, attempts, last_error
        FROM source_export_outbox
        WHERE status IN ('pending', 'failed', 'conflict')
        ORDER BY event_seq, export_id
        "#,
    )?;
    let rows = stmt.query_map([], outbox_row_from_row)?;
    let mut exports = Vec::new();
    for row in rows {
        exports.push(row?);
    }
    Ok(exports)
}

pub fn source_export_row_tx(
    conn: &Connection,
    export_id: i64,
) -> Result<Option<SourceExportOutboxRowWire>, ProjectionError> {
    Ok(conn
        .query_row(
            r#"
            SELECT export_id, event_seq, plan_json, status, attempts, last_error
            FROM source_export_outbox
            WHERE export_id = ?1
            "#,
            [export_id],
            outbox_row_from_row,
        )
        .optional()?)
}

pub fn retry_source_export_once_tx(
    conn: &Connection,
    export_id: i64,
) -> Result<SourceExportReportWire, ProjectionError> {
    let row = source_export_row_tx(conn, export_id)?.ok_or_else(|| {
        ProjectionError::Invariant(format!(
            "source export outbox row {export_id} does not exist"
        ))
    })?;
    match apply_source_export(&row.plan) {
        Ok(actual_fingerprint) => update_export_status_tx(
            conn,
            export_id,
            SourceExportStatusWire::Applied,
            None,
            actual_fingerprint,
        ),
        Err(SourceExportApplyError::Conflict {
            message,
            actual_fingerprint,
        }) => update_export_status_tx(
            conn,
            export_id,
            SourceExportStatusWire::Conflict,
            Some(message),
            actual_fingerprint,
        ),
        Err(SourceExportApplyError::Io(error)) => update_export_status_tx(
            conn,
            export_id,
            SourceExportStatusWire::Failed,
            Some(error.to_string()),
            None,
        ),
    }
}

pub fn apply_source_export(
    plan: &SourceExportPlanWire,
) -> Result<Option<SourceFingerprintWire>, SourceExportApplyError> {
    let target = Path::new(&plan.target_path);
    if source_export_create_new(plan) && target.exists() {
        let existing = fs::read(target).map_err(SourceExportApplyError::Io)?;
        if sha256_hex(&existing) == plan.content_sha256 {
            return Ok(source_fingerprint_from_path(target, true));
        }
        return Err(SourceExportApplyError::Conflict {
            message: "source export target already exists".to_string(),
            actual_fingerprint: source_fingerprint_from_path(target, true),
        });
    }
    if let Some(expected) = plan.expected_fingerprint.as_ref() {
        let actual = source_fingerprint_from_path(target, true);
        if !source_fingerprint_matches(actual.as_ref(), expected) {
            return Err(SourceExportApplyError::Conflict {
                message: "source fingerprint changed before export".to_string(),
                actual_fingerprint: actual,
            });
        }
    }
    if plan.content_sha256 != sha256_hex(plan.content_utf8.as_bytes()) {
        return Err(SourceExportApplyError::Conflict {
            message: "source export content hash does not match plan"
                .to_string(),
            actual_fingerprint: source_fingerprint_from_path(target, true),
        });
    }
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent).map_err(SourceExportApplyError::Io)?;
    }
    let lock = acquire_export_lock(target)?;
    match plan.kind {
        SourceExportKindWire::AtomicJson
        | SourceExportKindWire::ProjectFile => {
            atomic_replace(target, plan.content_utf8.as_bytes())?;
        }
        SourceExportKindWire::JsonlAppend => {
            append_jsonl(target, plan.content_utf8.as_bytes())?;
        }
    }
    lock.unlock().map_err(SourceExportApplyError::Io)?;
    Ok(source_fingerprint_from_path(target, true))
}

fn source_export_create_new(plan: &SourceExportPlanWire) -> bool {
    plan.repair_context
        .as_ref()
        .and_then(|context| context.get("create_new"))
        .and_then(JsonValue::as_bool)
        .unwrap_or(false)
}

fn source_fingerprint_matches(
    actual: Option<&SourceFingerprintWire>,
    expected: &SourceFingerprintWire,
) -> bool {
    let Some(actual) = actual else {
        return false;
    };
    expected
        .file_size
        .map_or(true, |value| actual.file_size == Some(value))
        && expected
            .modified_at_unix_millis
            .map_or(true, |value| actual.modified_at_unix_millis == Some(value))
        && expected
            .inode
            .map_or(true, |value| actual.inode == Some(value))
        && expected
            .content_sha256
            .as_ref()
            .map_or(true, |value| actual.content_sha256.as_ref() == Some(value))
}

#[derive(Debug)]
pub enum SourceExportApplyError {
    Conflict {
        message: String,
        actual_fingerprint: Option<SourceFingerprintWire>,
    },
    Io(std::io::Error),
}

fn update_export_status_tx(
    conn: &Connection,
    export_id: i64,
    status: SourceExportStatusWire,
    message: Option<String>,
    actual_fingerprint: Option<SourceFingerprintWire>,
) -> Result<SourceExportReportWire, ProjectionError> {
    conn.execute(
        r#"
        UPDATE source_export_outbox
        SET status = ?2,
            attempts = attempts + 1,
            last_error = ?3,
            updated_at = CURRENT_TIMESTAMP,
            applied_at = CASE WHEN ?2 = 'applied' THEN CURRENT_TIMESTAMP ELSE applied_at END
        WHERE export_id = ?1
        "#,
        params![export_id, status.as_str(), message],
    )?;
    conn.execute(
        r#"
        INSERT INTO source_export_attempts (
            export_id, status, message, actual_fingerprint_json
        ) VALUES (?1, ?2, ?3, ?4)
        "#,
        params![
            export_id,
            status.as_str(),
            message,
            serde_json::to_string(&actual_fingerprint)?,
        ],
    )?;
    let mut report =
        source_export_report_tx(conn, export_id)?.ok_or_else(|| {
            ProjectionError::Invariant(format!(
                "source export outbox row {export_id} disappeared after update"
            ))
        })?;
    report.actual_fingerprint = actual_fingerprint;
    Ok(report)
}

fn source_export_report_tx(
    conn: &Connection,
    export_id: i64,
) -> Result<Option<SourceExportReportWire>, ProjectionError> {
    Ok(conn
        .query_row(
            r#"
            SELECT export_id, event_seq, target_path, export_kind, status,
                   attempts, content_sha256, last_error
            FROM source_export_outbox
            WHERE export_id = ?1
            "#,
            [export_id],
            report_from_row,
        )
        .optional()?)
}

fn report_from_row(row: &Row<'_>) -> rusqlite::Result<SourceExportReportWire> {
    let kind: String = row.get(3)?;
    let status: String = row.get(4)?;
    Ok(SourceExportReportWire {
        schema_version: MUTATION_WIRE_SCHEMA_VERSION,
        export_id: row.get(0)?,
        event_seq: row.get(1)?,
        target_path: row.get(2)?,
        kind: kind.parse().map_err(invariant_decode_error)?,
        status: status.parse().map_err(invariant_decode_error)?,
        attempts: row.get(5)?,
        content_sha256: row.get(6)?,
        message: row.get(7)?,
        actual_fingerprint: None,
    })
}

fn outbox_row_from_row(
    row: &Row<'_>,
) -> rusqlite::Result<SourceExportOutboxRowWire> {
    let plan_json: String = row.get(2)?;
    let status: String = row.get(3)?;
    Ok(SourceExportOutboxRowWire {
        schema_version: MUTATION_WIRE_SCHEMA_VERSION,
        export_id: row.get(0)?,
        event_seq: row.get(1)?,
        plan: serde_json::from_str(&plan_json).map_err(json_decode_error)?,
        status: status.parse().map_err(invariant_decode_error)?,
        attempts: row.get(4)?,
        last_error: row.get(5)?,
    })
}

fn acquire_export_lock(target: &Path) -> Result<File, SourceExportApplyError> {
    let lock_path = lock_path_for(target);
    if let Some(parent) = lock_path.parent() {
        fs::create_dir_all(parent).map_err(SourceExportApplyError::Io)?;
    }
    let lock = OpenOptions::new()
        .create(true)
        .write(true)
        .open(lock_path)
        .map_err(SourceExportApplyError::Io)?;
    lock.lock_exclusive().map_err(SourceExportApplyError::Io)?;
    Ok(lock)
}

fn atomic_replace(
    target: &Path,
    bytes: &[u8],
) -> Result<(), SourceExportApplyError> {
    let tmp = temp_path_for(target);
    {
        let mut file =
            File::create(&tmp).map_err(SourceExportApplyError::Io)?;
        file.write_all(bytes).map_err(SourceExportApplyError::Io)?;
        file.sync_all().map_err(SourceExportApplyError::Io)?;
    }
    fs::rename(&tmp, target).map_err(SourceExportApplyError::Io)?;
    sync_parent_dir(target)?;
    Ok(())
}

fn append_jsonl(
    target: &Path,
    bytes: &[u8],
) -> Result<(), SourceExportApplyError> {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(target)
        .map_err(SourceExportApplyError::Io)?;
    file.write_all(bytes).map_err(SourceExportApplyError::Io)?;
    if !bytes.ends_with(b"\n") {
        file.write_all(b"\n").map_err(SourceExportApplyError::Io)?;
    }
    file.sync_all().map_err(SourceExportApplyError::Io)?;
    Ok(())
}

fn sync_parent_dir(path: &Path) -> Result<(), SourceExportApplyError> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    File::open(parent)
        .and_then(|file| file.sync_all())
        .map_err(SourceExportApplyError::Io)
}

fn lock_path_for(target: &Path) -> PathBuf {
    let mut value = target.as_os_str().to_os_string();
    value.push(".sase-export.lock");
    PathBuf::from(value)
}

fn temp_path_for(target: &Path) -> PathBuf {
    let mut value = target.as_os_str().to_os_string();
    value.push(format!(".tmp.{}", std::process::id()));
    PathBuf::from(value)
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn json_decode_error(error: serde_json::Error) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        0,
        rusqlite::types::Type::Text,
        Box::new(error),
    )
}

fn invariant_decode_error(error: ProjectionError) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        0,
        rusqlite::types::Type::Text,
        Box::new(error),
    )
}

pub(crate) fn mutation_outcome_for_event(
    event: &EventEnvelopeWire,
    duplicate: bool,
    resource_handle: Option<String>,
    source_exports: Vec<SourceExportReportWire>,
    projection_snapshot: Option<JsonValue>,
) -> LocalDaemonMutationOutcomeWire {
    LocalDaemonMutationOutcomeWire {
        schema_version: PROJECTION_EVENT_SCHEMA_VERSION,
        event_seq: event.seq,
        event_type: event.event_type.clone(),
        duplicate,
        changed: !duplicate,
        resource_handle,
        source_exports,
        projection_snapshot,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::projections::{
        EventAppendRequestWire, EventSourceWire, ProjectionDb,
    };
    use serde_json::json;
    use tempfile::tempdir;

    fn event_request(key: &str) -> EventAppendRequestWire {
        EventAppendRequestWire {
            created_at: Some("2026-05-14T00:00:00.000Z".to_string()),
            source: EventSourceWire {
                source_type: "test".to_string(),
                name: "mutation-test".to_string(),
                ..EventSourceWire::default()
            },
            host_id: "host-a".to_string(),
            project_id: "project-a".to_string(),
            event_type: "mutation.test".to_string(),
            payload: json!({"ok": true}),
            idempotency_key: Some(key.to_string()),
            causality: vec![],
            source_path: None,
            source_revision: None,
        }
    }

    #[test]
    fn retry_pending_export_after_simulated_crash() {
        let dir = tempdir().unwrap();
        let target = dir.path().join("source.json");
        let mut db = ProjectionDb::open_in_memory().unwrap();

        let outcome = db
            .append_mutation_event_with_outbox(
                event_request("export-crash"),
                Some("resource-a".to_string()),
                vec![SourceExportPlanWire::new(
                    target.display().to_string(),
                    SourceExportKindWire::AtomicJson,
                    "{\"ok\":true}\n",
                )],
                None,
            )
            .unwrap();

        assert_eq!(outcome.source_exports.len(), 1);
        assert_eq!(
            outcome.source_exports[0].status,
            SourceExportStatusWire::Pending
        );
        assert!(!target.exists());

        let pending = db.list_pending_source_exports().unwrap();
        assert_eq!(pending.len(), 1);
        let report = db.retry_source_export_once(pending[0].export_id).unwrap();

        assert_eq!(report.status, SourceExportStatusWire::Applied);
        assert_eq!(fs::read_to_string(target).unwrap(), "{\"ok\":true}\n");
        assert!(db.list_pending_source_exports().unwrap().is_empty());
    }

    #[test]
    fn stale_source_fingerprint_returns_conflict_report() {
        let dir = tempdir().unwrap();
        let target = dir.path().join("source.json");
        fs::write(&target, "{\"old\":true}\n").unwrap();
        let expected = source_fingerprint_from_path(&target, true).unwrap();
        fs::write(&target, "{\"legacy\":true}\n").unwrap();

        let mut plan = SourceExportPlanWire::new(
            target.display().to_string(),
            SourceExportKindWire::AtomicJson,
            "{\"new\":true}\n",
        );
        plan.expected_fingerprint = Some(expected);

        let error = apply_source_export(&plan).unwrap_err();
        match error {
            SourceExportApplyError::Conflict { message, .. } => {
                assert!(message.contains("fingerprint changed"));
            }
            other => panic!("expected conflict, got {other:?}"),
        }
    }
}
