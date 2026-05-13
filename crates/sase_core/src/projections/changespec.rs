use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use super::db::{append_event_tx, set_projection_last_seq_tx, ProjectionDb};
use super::error::ProjectionError;
use super::event::{
    EventAppendOutcomeWire, EventAppendRequestWire, EventEnvelopeWire,
    EventSourceWire, PROJECTION_EVENT_SCHEMA_VERSION,
};
use super::replay::ProjectionApplier;
use crate::parser::parse_project_bytes;
use crate::query::get_searchable_text;
use crate::wire::{
    ChangeSpecWire, ParseErrorWire, SourceSpanWire,
    CHANGESPEC_WIRE_SCHEMA_VERSION,
};

pub const CHANGESPEC_PROJECTION_NAME: &str = "changespec";
pub const CHANGESPEC_SOURCE_FILE_OBSERVED: &str =
    "changespec.source_file_observed";
pub const CHANGESPEC_SOURCE_FILE_REPARSED: &str =
    "changespec.source_file_reparsed";
pub const CHANGESPEC_SPEC_CREATED: &str = "changespec.spec_created";
pub const CHANGESPEC_SPEC_UPDATED: &str = "changespec.spec_updated";
pub const CHANGESPEC_SPEC_DELETED: &str = "changespec.spec_deleted";
pub const CHANGESPEC_ACTIVE_ARCHIVE_MOVED: &str =
    "changespec.active_archive_moved";
pub const CHANGESPEC_STATUS_TRANSITIONED: &str =
    "changespec.status_transitioned";
pub const CHANGESPEC_SECTIONS_UPDATED: &str = "changespec.sections_updated";

const PAYLOAD_SCHEMA_VERSION: u32 = 1;
const MAX_FTS_CONTENT_BYTES: usize = 16 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct SourcePayloadWire {
    schema_version: u32,
    path: String,
    content_hex: String,
    #[serde(default)]
    is_archive: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct SnapshotPayloadWire {
    schema_version: u32,
    spec: ChangeSpecWire,
    #[serde(default)]
    is_archive: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct DeletedPayloadWire {
    schema_version: u32,
    handle: String,
    name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct StatusPayloadWire {
    schema_version: u32,
    spec: ChangeSpecWire,
    from_status: String,
    to_status: String,
    #[serde(default)]
    is_archive: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct MovePayloadWire {
    schema_version: u32,
    spec: ChangeSpecWire,
    from_path: String,
    to_path: String,
    #[serde(default)]
    is_archive: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct SectionsPayloadWire {
    schema_version: u32,
    spec: ChangeSpecWire,
    section_names: Vec<String>,
    #[serde(default)]
    is_archive: bool,
}

#[derive(Debug, Clone, Default)]
pub struct ChangeSpecProjectionApplier;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangeSpecListRequestWire {
    pub schema_version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_before: Option<String>,
    #[serde(default)]
    pub limit: u32,
    #[serde(default)]
    pub offset: u32,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangeSpecSearchRequestWire {
    pub schema_version: u32,
    pub query: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
    #[serde(default)]
    pub limit: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangeSpecSummaryWire {
    pub schema_version: u32,
    pub handle: String,
    pub project_id: String,
    pub name: String,
    pub project_basename: String,
    pub file_path: String,
    pub source_path: String,
    pub is_archive: bool,
    pub status: String,
    pub parent: Option<String>,
    pub cl_or_pr: Option<String>,
    pub bug: Option<String>,
    pub updated_at: String,
    pub last_seq: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangeSpecListPageWire {
    pub schema_version: u32,
    pub entries: Vec<ChangeSpecSummaryWire>,
    pub next_offset: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangeSpecSectionRowWire {
    pub schema_version: u32,
    pub handle: String,
    pub section_name: String,
    pub section_index: u32,
    pub body: JsonValue,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangeSpecDetailWire {
    pub schema_version: u32,
    pub summary: ChangeSpecSummaryWire,
    pub spec: ChangeSpecWire,
    pub sections: Vec<ChangeSpecSectionRowWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangeSpecProjectionErrorWire {
    pub schema_version: u32,
    pub seq: i64,
    pub project_id: String,
    pub source_path: String,
    pub error_kind: String,
    pub message: String,
    pub line: Option<u32>,
    pub column: Option<u32>,
    pub created_at: String,
}

pub fn changespec_handle(project_id: &str, name: &str) -> String {
    format!("changespec:{project_id}:{name}")
}

pub fn source_file_observed_event(
    source: EventSourceWire,
    host_id: impl Into<String>,
    project_id: impl Into<String>,
    path: impl Into<String>,
    data: &[u8],
    is_archive: bool,
    idempotency_key: Option<String>,
) -> EventAppendRequestWire {
    source_event(
        CHANGESPEC_SOURCE_FILE_OBSERVED,
        source,
        host_id,
        project_id,
        path,
        data,
        is_archive,
        idempotency_key,
    )
}

pub fn source_file_reparsed_event(
    source: EventSourceWire,
    host_id: impl Into<String>,
    project_id: impl Into<String>,
    path: impl Into<String>,
    data: &[u8],
    is_archive: bool,
    idempotency_key: Option<String>,
) -> EventAppendRequestWire {
    source_event(
        CHANGESPEC_SOURCE_FILE_REPARSED,
        source,
        host_id,
        project_id,
        path,
        data,
        is_archive,
        idempotency_key,
    )
}

pub fn spec_created_event(
    source: EventSourceWire,
    host_id: impl Into<String>,
    project_id: impl Into<String>,
    spec: ChangeSpecWire,
    is_archive: bool,
    idempotency_key: Option<String>,
) -> EventAppendRequestWire {
    snapshot_event(
        CHANGESPEC_SPEC_CREATED,
        source,
        host_id,
        project_id,
        spec,
        is_archive,
        idempotency_key,
    )
}

pub fn spec_updated_event(
    source: EventSourceWire,
    host_id: impl Into<String>,
    project_id: impl Into<String>,
    spec: ChangeSpecWire,
    is_archive: bool,
    idempotency_key: Option<String>,
) -> EventAppendRequestWire {
    snapshot_event(
        CHANGESPEC_SPEC_UPDATED,
        source,
        host_id,
        project_id,
        spec,
        is_archive,
        idempotency_key,
    )
}

pub fn spec_deleted_event(
    source: EventSourceWire,
    host_id: impl Into<String>,
    project_id: impl Into<String>,
    name: impl Into<String>,
    idempotency_key: Option<String>,
) -> EventAppendRequestWire {
    let project_id = project_id.into();
    let name = name.into();
    let handle = changespec_handle(&project_id, &name);
    EventAppendRequestWire {
        created_at: None,
        source,
        host_id: host_id.into(),
        project_id,
        event_type: CHANGESPEC_SPEC_DELETED.to_string(),
        payload: serde_json::to_value(DeletedPayloadWire {
            schema_version: PAYLOAD_SCHEMA_VERSION,
            handle,
            name,
        })
        .expect("changespec deleted payload serializes"),
        idempotency_key,
        causality: vec![],
        source_path: None,
        source_revision: None,
    }
}

#[allow(clippy::too_many_arguments)]
pub fn active_archive_moved_event(
    source: EventSourceWire,
    host_id: impl Into<String>,
    project_id: impl Into<String>,
    spec: ChangeSpecWire,
    from_path: impl Into<String>,
    to_path: impl Into<String>,
    is_archive: bool,
    idempotency_key: Option<String>,
) -> EventAppendRequestWire {
    let source_path = spec.file_path.clone();
    EventAppendRequestWire {
        created_at: None,
        source,
        host_id: host_id.into(),
        project_id: project_id.into(),
        event_type: CHANGESPEC_ACTIVE_ARCHIVE_MOVED.to_string(),
        payload: serde_json::to_value(MovePayloadWire {
            schema_version: PAYLOAD_SCHEMA_VERSION,
            spec,
            from_path: from_path.into(),
            to_path: to_path.into(),
            is_archive,
        })
        .expect("changespec move payload serializes"),
        idempotency_key,
        causality: vec![],
        source_path: Some(source_path),
        source_revision: None,
    }
}

#[allow(clippy::too_many_arguments)]
pub fn status_transitioned_event(
    source: EventSourceWire,
    host_id: impl Into<String>,
    project_id: impl Into<String>,
    spec: ChangeSpecWire,
    from_status: impl Into<String>,
    to_status: impl Into<String>,
    is_archive: bool,
    idempotency_key: Option<String>,
) -> EventAppendRequestWire {
    let source_path = spec.file_path.clone();
    EventAppendRequestWire {
        created_at: None,
        source,
        host_id: host_id.into(),
        project_id: project_id.into(),
        event_type: CHANGESPEC_STATUS_TRANSITIONED.to_string(),
        payload: serde_json::to_value(StatusPayloadWire {
            schema_version: PAYLOAD_SCHEMA_VERSION,
            spec,
            from_status: from_status.into(),
            to_status: to_status.into(),
            is_archive,
        })
        .expect("changespec status payload serializes"),
        idempotency_key,
        causality: vec![],
        source_path: Some(source_path),
        source_revision: None,
    }
}

pub fn sections_updated_event(
    source: EventSourceWire,
    host_id: impl Into<String>,
    project_id: impl Into<String>,
    spec: ChangeSpecWire,
    section_names: Vec<String>,
    is_archive: bool,
    idempotency_key: Option<String>,
) -> EventAppendRequestWire {
    let source_path = spec.file_path.clone();
    EventAppendRequestWire {
        created_at: None,
        source,
        host_id: host_id.into(),
        project_id: project_id.into(),
        event_type: CHANGESPEC_SECTIONS_UPDATED.to_string(),
        payload: serde_json::to_value(SectionsPayloadWire {
            schema_version: PAYLOAD_SCHEMA_VERSION,
            spec,
            section_names,
            is_archive,
        })
        .expect("changespec sections payload serializes"),
        idempotency_key,
        causality: vec![],
        source_path: Some(source_path),
        source_revision: None,
    }
}

impl ProjectionDb {
    pub fn append_changespec_event(
        &mut self,
        request: EventAppendRequestWire,
    ) -> Result<EventAppendOutcomeWire, ProjectionError> {
        self.with_immediate_transaction(|conn| {
            let outcome = append_event_tx(conn, request)?;
            let mut applier = ChangeSpecProjectionApplier;
            applier.apply(&outcome.event, conn)?;
            set_projection_last_seq_tx(
                conn,
                CHANGESPEC_PROJECTION_NAME,
                outcome.event.seq,
            )?;
            Ok(outcome)
        })
    }

    pub fn list_changespecs(
        &self,
        request: &ChangeSpecListRequestWire,
    ) -> Result<ChangeSpecListPageWire, ProjectionError> {
        list_changespecs_tx(self.connection(), request)
    }

    pub fn fetch_changespec_detail(
        &self,
        handle: &str,
    ) -> Result<Option<ChangeSpecDetailWire>, ProjectionError> {
        fetch_changespec_detail_tx(self.connection(), handle)
    }

    pub fn search_changespecs(
        &self,
        request: &ChangeSpecSearchRequestWire,
    ) -> Result<ChangeSpecListPageWire, ProjectionError> {
        search_changespecs_tx(self.connection(), request)
    }

    pub fn list_changespec_projection_errors(
        &self,
    ) -> Result<Vec<ChangeSpecProjectionErrorWire>, ProjectionError> {
        list_projection_errors_tx(self.connection())
    }
}

impl ProjectionApplier for ChangeSpecProjectionApplier {
    fn projection_name(&self) -> &str {
        CHANGESPEC_PROJECTION_NAME
    }

    fn apply(
        &mut self,
        event: &EventEnvelopeWire,
        conn: &Connection,
    ) -> Result<(), ProjectionError> {
        apply_changespec_event_tx(conn, event)
    }
}

#[allow(clippy::too_many_arguments)]
fn source_event(
    event_type: &str,
    source: EventSourceWire,
    host_id: impl Into<String>,
    project_id: impl Into<String>,
    path: impl Into<String>,
    data: &[u8],
    is_archive: bool,
    idempotency_key: Option<String>,
) -> EventAppendRequestWire {
    let path = path.into();
    EventAppendRequestWire {
        created_at: None,
        source,
        host_id: host_id.into(),
        project_id: project_id.into(),
        event_type: event_type.to_string(),
        payload: serde_json::to_value(SourcePayloadWire {
            schema_version: PAYLOAD_SCHEMA_VERSION,
            path: path.clone(),
            content_hex: hex::encode(data),
            is_archive,
        })
        .expect("changespec source payload serializes"),
        idempotency_key,
        causality: vec![],
        source_path: Some(path),
        source_revision: None,
    }
}

fn snapshot_event(
    event_type: &str,
    source: EventSourceWire,
    host_id: impl Into<String>,
    project_id: impl Into<String>,
    spec: ChangeSpecWire,
    is_archive: bool,
    idempotency_key: Option<String>,
) -> EventAppendRequestWire {
    let source_path = spec.file_path.clone();
    EventAppendRequestWire {
        created_at: None,
        source,
        host_id: host_id.into(),
        project_id: project_id.into(),
        event_type: event_type.to_string(),
        payload: serde_json::to_value(SnapshotPayloadWire {
            schema_version: PAYLOAD_SCHEMA_VERSION,
            spec,
            is_archive,
        })
        .expect("changespec snapshot payload serializes"),
        idempotency_key,
        causality: vec![],
        source_path: Some(source_path),
        source_revision: None,
    }
}

fn apply_changespec_event_tx(
    conn: &Connection,
    event: &EventEnvelopeWire,
) -> Result<(), ProjectionError> {
    match event.event_type.as_str() {
        CHANGESPEC_SOURCE_FILE_OBSERVED | CHANGESPEC_SOURCE_FILE_REPARSED => {
            let payload: SourcePayloadWire =
                serde_json::from_value(event.payload.clone())?;
            apply_source_payload_tx(conn, event, payload)
        }
        CHANGESPEC_SPEC_CREATED | CHANGESPEC_SPEC_UPDATED => {
            let payload: SnapshotPayloadWire =
                serde_json::from_value(event.payload.clone())?;
            upsert_changespec_tx(
                conn,
                &event.project_id,
                &payload.spec,
                payload.is_archive,
                event.seq,
                &event.created_at,
            )
        }
        CHANGESPEC_SPEC_DELETED => {
            let payload: DeletedPayloadWire =
                serde_json::from_value(event.payload.clone())?;
            delete_changespec_tx(conn, &payload.handle)
        }
        CHANGESPEC_ACTIVE_ARCHIVE_MOVED => {
            let payload: MovePayloadWire =
                serde_json::from_value(event.payload.clone())?;
            upsert_changespec_tx(
                conn,
                &event.project_id,
                &payload.spec,
                payload.is_archive,
                event.seq,
                &event.created_at,
            )
        }
        CHANGESPEC_STATUS_TRANSITIONED => {
            let payload: StatusPayloadWire =
                serde_json::from_value(event.payload.clone())?;
            upsert_changespec_tx(
                conn,
                &event.project_id,
                &payload.spec,
                payload.is_archive,
                event.seq,
                &event.created_at,
            )
        }
        CHANGESPEC_SECTIONS_UPDATED => {
            let payload: SectionsPayloadWire =
                serde_json::from_value(event.payload.clone())?;
            upsert_changespec_tx(
                conn,
                &event.project_id,
                &payload.spec,
                payload.is_archive,
                event.seq,
                &event.created_at,
            )
        }
        _ => Ok(()),
    }
}

fn apply_source_payload_tx(
    conn: &Connection,
    event: &EventEnvelopeWire,
    payload: SourcePayloadWire,
) -> Result<(), ProjectionError> {
    conn.execute(
        "DELETE FROM changespec_projection_errors WHERE seq = ?1",
        [event.seq],
    )?;
    let bytes = hex::decode(&payload.content_hex).map_err(|error| {
        ProjectionError::Invariant(format!(
            "invalid changespec source payload hex: {error}"
        ))
    })?;

    let specs = match parse_project_bytes(&payload.path, &bytes) {
        Ok(specs) => specs,
        Err(error) => {
            insert_projection_error_tx(conn, event, &payload.path, &error)?;
            return Ok(());
        }
    };

    let handles: Vec<String> = specs
        .iter()
        .map(|spec| changespec_handle(&event.project_id, &spec.name))
        .collect();
    delete_source_specs_not_in_tx(
        conn,
        &event.project_id,
        &payload.path,
        &handles,
    )?;
    for spec in specs {
        upsert_changespec_tx(
            conn,
            &event.project_id,
            &spec,
            payload.is_archive,
            event.seq,
            &event.created_at,
        )?;
    }
    Ok(())
}

fn upsert_changespec_tx(
    conn: &Connection,
    project_id: &str,
    spec: &ChangeSpecWire,
    is_archive: bool,
    seq: i64,
    updated_at: &str,
) -> Result<(), ProjectionError> {
    let handle = changespec_handle(project_id, &spec.name);
    let searchable_text = get_searchable_text(spec);
    let fts_content = bounded_fts_content(&searchable_text);
    conn.execute(
        r#"
        INSERT INTO changespecs (
            handle, schema_version, project_id, name, project_basename,
            file_path, source_path, is_archive, source_start_line,
            source_end_line, status, parent, cl_or_pr, bug, description,
            searchable_text, updated_at, last_seq
        ) VALUES (
            ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14,
            ?15, ?16, ?17, ?18
        )
        ON CONFLICT(handle) DO UPDATE SET
            schema_version = excluded.schema_version,
            project_id = excluded.project_id,
            name = excluded.name,
            project_basename = excluded.project_basename,
            file_path = excluded.file_path,
            source_path = excluded.source_path,
            is_archive = excluded.is_archive,
            source_start_line = excluded.source_start_line,
            source_end_line = excluded.source_end_line,
            status = excluded.status,
            parent = excluded.parent,
            cl_or_pr = excluded.cl_or_pr,
            bug = excluded.bug,
            description = excluded.description,
            searchable_text = excluded.searchable_text,
            updated_at = excluded.updated_at,
            last_seq = excluded.last_seq
        "#,
        params![
            &handle,
            CHANGESPEC_WIRE_SCHEMA_VERSION,
            project_id,
            &spec.name,
            &spec.project_basename,
            &spec.file_path,
            &spec.file_path,
            is_archive,
            spec.source_span.start_line,
            spec.source_span.end_line,
            &spec.status,
            &spec.parent,
            &spec.cl_or_pr,
            &spec.bug,
            &spec.description,
            &searchable_text,
            updated_at,
            seq,
        ],
    )?;
    replace_edges_tx(conn, project_id, spec, &handle, seq)?;
    replace_sections_tx(conn, spec, &handle, seq)?;
    replace_fts_tx(conn, project_id, spec, &handle, &fts_content)?;
    Ok(())
}

fn delete_changespec_tx(
    conn: &Connection,
    handle: &str,
) -> Result<(), ProjectionError> {
    conn.execute(
        "DELETE FROM changespec_search_fts WHERE handle = ?1",
        [handle],
    )?;
    conn.execute("DELETE FROM changespecs WHERE handle = ?1", [handle])?;
    Ok(())
}

fn delete_source_specs_not_in_tx(
    conn: &Connection,
    project_id: &str,
    source_path: &str,
    keep_handles: &[String],
) -> Result<(), ProjectionError> {
    let mut stmt = conn.prepare(
        "SELECT handle FROM changespecs WHERE project_id = ?1 AND source_path = ?2",
    )?;
    let rows = stmt.query_map(params![project_id, source_path], |row| {
        row.get::<_, String>(0)
    })?;
    let existing = rows.collect::<Result<Vec<_>, _>>()?;
    drop(stmt);
    for handle in existing {
        if !keep_handles.iter().any(|keep| keep == &handle) {
            delete_changespec_tx(conn, &handle)?;
        }
    }
    Ok(())
}

fn replace_edges_tx(
    conn: &Connection,
    project_id: &str,
    spec: &ChangeSpecWire,
    handle: &str,
    seq: i64,
) -> Result<(), ProjectionError> {
    conn.execute(
        "DELETE FROM changespec_edges WHERE source_handle = ?1",
        [handle],
    )?;
    if let Some(parent) = spec.parent.as_ref().filter(|value| !value.is_empty())
    {
        conn.execute(
            r#"
            INSERT INTO changespec_edges (
                source_handle, edge_type, target_handle, target_name, last_seq
            ) VALUES (?1, 'parent', ?2, ?3, ?4)
            "#,
            params![handle, changespec_handle(project_id, parent), parent, seq],
        )?;
    }
    Ok(())
}

fn replace_sections_tx(
    conn: &Connection,
    spec: &ChangeSpecWire,
    handle: &str,
    seq: i64,
) -> Result<(), ProjectionError> {
    conn.execute(
        "DELETE FROM changespec_sections WHERE handle = ?1",
        [handle],
    )?;
    insert_section_rows_tx(conn, handle, "commits", &spec.commits, seq)?;
    insert_section_rows_tx(conn, handle, "hooks", &spec.hooks, seq)?;
    insert_section_rows_tx(conn, handle, "comments", &spec.comments, seq)?;
    insert_section_rows_tx(conn, handle, "mentors", &spec.mentors, seq)?;
    insert_section_rows_tx(conn, handle, "timestamps", &spec.timestamps, seq)?;
    insert_section_rows_tx(conn, handle, "deltas", &spec.deltas, seq)?;
    Ok(())
}

fn insert_section_rows_tx<T: Serialize>(
    conn: &Connection,
    handle: &str,
    section_name: &str,
    rows: &[T],
    seq: i64,
) -> Result<(), ProjectionError> {
    for (idx, row) in rows.iter().enumerate() {
        conn.execute(
            r#"
            INSERT INTO changespec_sections (
                handle, section_name, section_index, body_json, last_seq
            ) VALUES (?1, ?2, ?3, ?4, ?5)
            "#,
            params![
                handle,
                section_name,
                idx as i64,
                serde_json::to_string(row)?,
                seq,
            ],
        )?;
    }
    Ok(())
}

fn replace_fts_tx(
    conn: &Connection,
    project_id: &str,
    spec: &ChangeSpecWire,
    handle: &str,
    content: &str,
) -> Result<(), ProjectionError> {
    conn.execute(
        "DELETE FROM changespec_search_fts WHERE handle = ?1",
        [handle],
    )?;
    conn.execute(
        r#"
        INSERT INTO changespec_search_fts (
            handle, project_id, name, status, content
        ) VALUES (?1, ?2, ?3, ?4, ?5)
        "#,
        params![handle, project_id, &spec.name, &spec.status, content],
    )?;
    Ok(())
}

fn insert_projection_error_tx(
    conn: &Connection,
    event: &EventEnvelopeWire,
    source_path: &str,
    error: &ParseErrorWire,
) -> Result<(), ProjectionError> {
    conn.execute(
        r#"
        INSERT INTO changespec_projection_errors (
            seq, project_id, source_path, error_kind, message, line, column,
            created_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
        ON CONFLICT(seq) DO UPDATE SET
            project_id = excluded.project_id,
            source_path = excluded.source_path,
            error_kind = excluded.error_kind,
            message = excluded.message,
            line = excluded.line,
            column = excluded.column,
            created_at = excluded.created_at
        "#,
        params![
            event.seq,
            &event.project_id,
            source_path,
            &error.kind,
            &error.message,
            error.line,
            error.column,
            &event.created_at,
        ],
    )?;
    Ok(())
}

fn list_changespecs_tx(
    conn: &Connection,
    request: &ChangeSpecListRequestWire,
) -> Result<ChangeSpecListPageWire, ProjectionError> {
    let limit = normalized_limit(request.limit);
    let offset = request.offset;
    let sql = r#"
        SELECT handle, schema_version, project_id, name, project_basename,
               file_path, source_path, is_archive, status, parent, cl_or_pr,
               bug, updated_at, last_seq
        FROM changespecs
        WHERE (?1 IS NULL OR project_id = ?1)
          AND (?2 IS NULL OR status = ?2)
          AND (?3 IS NULL OR updated_at < ?3)
        ORDER BY updated_at DESC, handle ASC
        LIMIT ?4 OFFSET ?5
    "#;
    let entries = query_summaries(
        conn,
        sql,
        params![
            &request.project_id,
            &request.status,
            &request.updated_before,
            limit + 1,
            offset,
        ],
    )?;
    page_from_entries(entries, limit, offset)
}

fn fetch_changespec_detail_tx(
    conn: &Connection,
    handle: &str,
) -> Result<Option<ChangeSpecDetailWire>, ProjectionError> {
    let row = conn
        .query_row(
            r#"
            SELECT handle, schema_version, project_id, name, project_basename,
                   file_path, source_path, is_archive, source_start_line,
                   source_end_line, status, parent, cl_or_pr, bug, description,
                   updated_at, last_seq
            FROM changespecs
            WHERE handle = ?1
            "#,
            [handle],
            |row| {
                let summary = ChangeSpecSummaryWire {
                    schema_version: row.get(1)?,
                    handle: row.get(0)?,
                    project_id: row.get(2)?,
                    name: row.get(3)?,
                    project_basename: row.get(4)?,
                    file_path: row.get(5)?,
                    source_path: row.get(6)?,
                    is_archive: row.get::<_, i64>(7)? != 0,
                    status: row.get(10)?,
                    parent: row.get(11)?,
                    cl_or_pr: row.get(12)?,
                    bug: row.get(13)?,
                    updated_at: row.get(15)?,
                    last_seq: row.get(16)?,
                };
                let spec = ChangeSpecWire {
                    schema_version: row.get(1)?,
                    name: summary.name.clone(),
                    project_basename: summary.project_basename.clone(),
                    file_path: summary.file_path.clone(),
                    source_span: SourceSpanWire {
                        file_path: summary.file_path.clone(),
                        start_line: row.get(8)?,
                        end_line: row.get(9)?,
                    },
                    status: summary.status.clone(),
                    parent: summary.parent.clone(),
                    cl_or_pr: summary.cl_or_pr.clone(),
                    bug: summary.bug.clone(),
                    description: row.get(14)?,
                    commits: vec![],
                    hooks: vec![],
                    comments: vec![],
                    mentors: vec![],
                    timestamps: vec![],
                    deltas: vec![],
                };
                Ok((summary, spec))
            },
        )
        .optional()?;

    let Some((summary, mut spec)) = row else {
        return Ok(None);
    };
    let sections = list_sections_tx(conn, handle)?;
    hydrate_sections(&mut spec, &sections)?;
    Ok(Some(ChangeSpecDetailWire {
        schema_version: PROJECTION_EVENT_SCHEMA_VERSION,
        summary,
        spec,
        sections,
    }))
}

fn search_changespecs_tx(
    conn: &Connection,
    request: &ChangeSpecSearchRequestWire,
) -> Result<ChangeSpecListPageWire, ProjectionError> {
    let limit = normalized_limit(request.limit);
    let entries = query_summaries(
        conn,
        r#"
        SELECT c.handle, c.schema_version, c.project_id, c.name,
               c.project_basename, c.file_path, c.source_path, c.is_archive,
               c.status, c.parent, c.cl_or_pr, c.bug, c.updated_at, c.last_seq
        FROM changespec_search_fts f
        JOIN changespecs c ON c.handle = f.handle
        WHERE changespec_search_fts MATCH ?1
          AND (?2 IS NULL OR f.project_id = ?2)
        ORDER BY rank, c.updated_at DESC, c.handle ASC
        LIMIT ?3
        "#,
        params![&request.query, &request.project_id, limit + 1],
    )?;
    page_from_entries(entries, limit, 0)
}

fn list_projection_errors_tx(
    conn: &Connection,
) -> Result<Vec<ChangeSpecProjectionErrorWire>, ProjectionError> {
    let mut stmt = conn.prepare(
        r#"
        SELECT seq, project_id, source_path, error_kind, message, line, column,
               created_at
        FROM changespec_projection_errors
        ORDER BY seq
        "#,
    )?;
    let rows = stmt.query_map([], |row| {
        Ok(ChangeSpecProjectionErrorWire {
            schema_version: PROJECTION_EVENT_SCHEMA_VERSION,
            seq: row.get(0)?,
            project_id: row.get(1)?,
            source_path: row.get(2)?,
            error_kind: row.get(3)?,
            message: row.get(4)?,
            line: row.get(5)?,
            column: row.get(6)?,
            created_at: row.get(7)?,
        })
    })?;
    Ok(rows.collect::<Result<Vec<_>, _>>()?)
}

fn query_summaries<P: rusqlite::Params>(
    conn: &Connection,
    sql: &str,
    params: P,
) -> Result<Vec<ChangeSpecSummaryWire>, ProjectionError> {
    let mut stmt = conn.prepare(sql)?;
    let rows = stmt.query_map(params, summary_from_row)?;
    Ok(rows.collect::<Result<Vec<_>, _>>()?)
}

fn summary_from_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<ChangeSpecSummaryWire> {
    Ok(ChangeSpecSummaryWire {
        schema_version: row.get(1)?,
        handle: row.get(0)?,
        project_id: row.get(2)?,
        name: row.get(3)?,
        project_basename: row.get(4)?,
        file_path: row.get(5)?,
        source_path: row.get(6)?,
        is_archive: row.get::<_, i64>(7)? != 0,
        status: row.get(8)?,
        parent: row.get(9)?,
        cl_or_pr: row.get(10)?,
        bug: row.get(11)?,
        updated_at: row.get(12)?,
        last_seq: row.get(13)?,
    })
}

fn page_from_entries(
    mut entries: Vec<ChangeSpecSummaryWire>,
    limit: u32,
    offset: u32,
) -> Result<ChangeSpecListPageWire, ProjectionError> {
    let next_offset = if entries.len() > limit as usize {
        entries.truncate(limit as usize);
        Some(offset + limit)
    } else {
        None
    };
    Ok(ChangeSpecListPageWire {
        schema_version: PROJECTION_EVENT_SCHEMA_VERSION,
        entries,
        next_offset,
    })
}

fn list_sections_tx(
    conn: &Connection,
    handle: &str,
) -> Result<Vec<ChangeSpecSectionRowWire>, ProjectionError> {
    let mut stmt = conn.prepare(
        r#"
        SELECT handle, section_name, section_index, body_json
        FROM changespec_sections
        WHERE handle = ?1
        ORDER BY section_name, section_index
        "#,
    )?;
    let rows = stmt.query_map([handle], |row| {
        let body_json: String = row.get(3)?;
        let body = serde_json::from_str(&body_json).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                3,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })?;
        Ok(ChangeSpecSectionRowWire {
            schema_version: PROJECTION_EVENT_SCHEMA_VERSION,
            handle: row.get(0)?,
            section_name: row.get(1)?,
            section_index: row.get::<_, i64>(2)? as u32,
            body,
        })
    })?;
    Ok(rows.collect::<Result<Vec<_>, _>>()?)
}

fn hydrate_sections(
    spec: &mut ChangeSpecWire,
    sections: &[ChangeSpecSectionRowWire],
) -> Result<(), ProjectionError> {
    for section in sections {
        match section.section_name.as_str() {
            "commits" => spec
                .commits
                .push(serde_json::from_value(section.body.clone())?),
            "hooks" => spec
                .hooks
                .push(serde_json::from_value(section.body.clone())?),
            "comments" => spec
                .comments
                .push(serde_json::from_value(section.body.clone())?),
            "mentors" => spec
                .mentors
                .push(serde_json::from_value(section.body.clone())?),
            "timestamps" => spec
                .timestamps
                .push(serde_json::from_value(section.body.clone())?),
            "deltas" => spec
                .deltas
                .push(serde_json::from_value(section.body.clone())?),
            _ => {}
        }
    }
    Ok(())
}

fn bounded_fts_content(text: &str) -> String {
    if text.len() <= MAX_FTS_CONTENT_BYTES {
        return text.to_string();
    }
    let mut end = MAX_FTS_CONTENT_BYTES;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text[..end].to_string()
}

fn normalized_limit(limit: u32) -> u32 {
    match limit {
        0 => 50,
        1..=200 => limit,
        _ => 200,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::projections::EventSourceWire;

    fn source() -> EventSourceWire {
        EventSourceWire {
            source_type: "test".to_string(),
            name: "changespec-projection-test".to_string(),
            ..EventSourceWire::default()
        }
    }

    fn fixture() -> &'static [u8] {
        include_bytes!("../../tests/fixtures/myproj.sase")
    }

    #[test]
    fn source_file_event_projects_specs_and_search() {
        let mut db = ProjectionDb::open_in_memory().unwrap();
        db.append_changespec_event(source_file_observed_event(
            source(),
            "host",
            "project",
            "myproj.sase",
            fixture(),
            false,
            Some("source:myproj".to_string()),
        ))
        .unwrap();

        let page = db
            .list_changespecs(&ChangeSpecListRequestWire {
                schema_version: PROJECTION_EVENT_SCHEMA_VERSION,
                project_id: Some("project".to_string()),
                status: Some("WIP".to_string()),
                updated_before: None,
                limit: 10,
                offset: 0,
            })
            .unwrap();
        assert_eq!(page.entries.len(), 1);
        assert_eq!(page.entries[0].name, "beta");

        let search = db
            .search_changespecs(&ChangeSpecSearchRequestWire {
                schema_version: PROJECTION_EVENT_SCHEMA_VERSION,
                query: "Sibling".to_string(),
                project_id: Some("project".to_string()),
                limit: 10,
            })
            .unwrap();
        assert_eq!(search.entries.len(), 1);
        assert_eq!(search.entries[0].name, "beta");

        let detail = db
            .fetch_changespec_detail(&changespec_handle("project", "alpha"))
            .unwrap()
            .unwrap();
        assert_eq!(detail.spec.name, "alpha");
        assert_eq!(detail.spec.commits.len(), 1);
        assert_eq!(detail.spec.hooks.len(), 1);
        assert_eq!(detail.spec.deltas.len(), 2);
    }

    #[test]
    fn replay_matches_live_projection_rows() {
        let request = source_file_observed_event(
            source(),
            "host",
            "project",
            "myproj.sase",
            fixture(),
            false,
            None,
        );

        let mut live = ProjectionDb::open_in_memory().unwrap();
        live.append_changespec_event(request.clone()).unwrap();

        let mut replayed = ProjectionDb::open_in_memory().unwrap();
        replayed.append_event(request).unwrap();
        let mut applier = ChangeSpecProjectionApplier;
        replayed.replay_events(0, &mut [&mut applier]).unwrap();

        assert_eq!(
            dump_changespec_rows(&live),
            dump_changespec_rows(&replayed)
        );
    }

    #[test]
    fn archive_move_updates_single_identity() {
        let specs = parse_project_bytes("myproj.sase", fixture()).unwrap();
        let mut alpha =
            specs.into_iter().find(|spec| spec.name == "alpha").unwrap();

        let mut db = ProjectionDb::open_in_memory().unwrap();
        db.append_changespec_event(spec_created_event(
            source(),
            "host",
            "project",
            alpha.clone(),
            false,
            None,
        ))
        .unwrap();

        alpha.file_path = "myproj-archive.sase".to_string();
        alpha.source_span.file_path = alpha.file_path.clone();
        alpha.status = "Archived".to_string();
        db.append_changespec_event(active_archive_moved_event(
            source(),
            "host",
            "project",
            alpha,
            "myproj.sase",
            "myproj-archive.sase",
            true,
            None,
        ))
        .unwrap();

        let count: i64 = db
            .connection()
            .query_row(
                "SELECT COUNT(*) FROM changespecs WHERE name = 'alpha'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);

        let detail = db
            .fetch_changespec_detail(&changespec_handle("project", "alpha"))
            .unwrap()
            .unwrap();
        assert!(detail.summary.is_archive);
        assert_eq!(detail.summary.status, "Archived");
    }

    #[test]
    fn corrupt_source_records_projection_error() {
        let mut db = ProjectionDb::open_in_memory().unwrap();
        db.append_changespec_event(source_file_observed_event(
            source(),
            "host",
            "project",
            "broken.sase",
            &[0xff, 0xfe],
            false,
            None,
        ))
        .unwrap();

        assert_eq!(db.event_count().unwrap(), 1);
        assert_eq!(
            db.list_changespecs(&ChangeSpecListRequestWire::default())
                .unwrap()
                .entries
                .len(),
            0
        );
        let errors = db.list_changespec_projection_errors().unwrap();
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].error_kind, "encoding");
    }

    fn dump_changespec_rows(
        db: &ProjectionDb,
    ) -> Vec<(String, String, String, i64)> {
        let mut stmt = db
            .connection()
            .prepare(
                "SELECT handle, status, searchable_text, last_seq FROM changespecs ORDER BY handle",
            )
            .unwrap();
        stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
            ))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
    }
}
