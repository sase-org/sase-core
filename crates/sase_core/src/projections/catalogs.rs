use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::editor::build_file_history_completion_candidates;
use crate::{CompletionList, MobileXpromptCatalogEntryWire};

use super::db::{append_event_tx, set_projection_last_seq_tx, ProjectionDb};
use super::error::ProjectionError;
use super::event::{
    EventAppendOutcomeWire, EventAppendRequestWire, EventCausalityWire,
    EventEnvelopeWire, EventSourceWire,
};
use super::replay::ProjectionApplier;

pub const CATALOG_PROJECTION_NAME: &str = "catalogs";

pub const CATALOG_EVENT_XPROMPT_SOURCE_OBSERVED: &str =
    "catalog.xprompt_source_observed";
pub const CATALOG_EVENT_XPROMPT_SOURCE_REMOVED: &str =
    "catalog.xprompt_source_removed";
pub const CATALOG_EVENT_CONFIG_SOURCE_OBSERVED: &str =
    "catalog.config_source_observed";
pub const CATALOG_EVENT_CONFIG_SOURCE_REMOVED: &str =
    "catalog.config_source_removed";
pub const CATALOG_EVENT_MEMORY_SOURCE_OBSERVED: &str =
    "catalog.memory_source_observed";
pub const CATALOG_EVENT_MEMORY_SOURCE_REMOVED: &str =
    "catalog.memory_source_removed";
pub const CATALOG_EVENT_FILE_HISTORY_UPDATED: &str =
    "catalog.file_history_updated";
pub const CATALOG_EVENT_FILE_HISTORY_REMOVED: &str =
    "catalog.file_history_removed";

const CATALOG_WIRE_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct CatalogProjectionEventContextWire {
    #[serde(default = "catalog_schema_version")]
    pub schema_version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_at: Option<String>,
    pub source: EventSourceWire,
    pub host_id: String,
    pub project_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    #[serde(default)]
    pub causality: Vec<EventCausalityWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_revision: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct XpromptCatalogProjectionWire {
    #[serde(default = "catalog_schema_version")]
    pub schema_version: u32,
    pub catalog_id: String,
    pub entry: MobileXpromptCatalogEntryWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
    #[serde(default)]
    pub exists: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigCatalogProjectionWire {
    #[serde(default = "catalog_schema_version")]
    pub schema_version: u32,
    pub config_id: String,
    pub path: String,
    pub scope: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_preview: Option<String>,
    #[serde(default)]
    pub exists: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryCatalogProjectionWire {
    #[serde(default = "catalog_schema_version")]
    pub schema_version: u32,
    pub memory_id: String,
    pub path: String,
    pub tier: String,
    pub source_name: String,
    #[serde(default)]
    pub keywords: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_preview: Option<String>,
    #[serde(default)]
    pub exists: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileHistoryProjectionWire {
    #[serde(default = "catalog_schema_version")]
    pub schema_version: u32,
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch_or_workspace: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_used_at: Option<String>,
    #[serde(default)]
    pub use_count: u64,
    #[serde(default)]
    pub exists: bool,
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[allow(clippy::large_enum_variant)]
enum CatalogProjectionEventPayloadWire {
    XpromptSourceObserved {
        schema_version: u32,
        xprompt: XpromptCatalogProjectionWire,
    },
    XpromptSourceRemoved {
        schema_version: u32,
        catalog_id: String,
    },
    ConfigSourceObserved {
        schema_version: u32,
        config: ConfigCatalogProjectionWire,
    },
    ConfigSourceRemoved {
        schema_version: u32,
        config_id: String,
    },
    MemorySourceObserved {
        schema_version: u32,
        memory: MemoryCatalogProjectionWire,
    },
    MemorySourceRemoved {
        schema_version: u32,
        memory_id: String,
    },
    FileHistoryUpdated {
        schema_version: u32,
        file: FileHistoryProjectionWire,
    },
    FileHistoryRemoved {
        schema_version: u32,
        path: String,
        branch_or_workspace: Option<String>,
    },
}

pub struct CatalogProjectionApplier;

impl ProjectionApplier for CatalogProjectionApplier {
    fn projection_name(&self) -> &str {
        CATALOG_PROJECTION_NAME
    }

    fn apply(
        &mut self,
        event: &EventEnvelopeWire,
        conn: &Connection,
    ) -> Result<(), ProjectionError> {
        apply_catalog_event_tx(conn, event)
    }
}

impl ProjectionDb {
    pub fn append_catalog_event(
        &mut self,
        request: EventAppendRequestWire,
    ) -> Result<EventAppendOutcomeWire, ProjectionError> {
        self.with_immediate_transaction(|conn| {
            let outcome = append_event_tx(conn, request)?;
            if !outcome.duplicate {
                apply_catalog_event_tx(conn, &outcome.event)?;
                set_projection_last_seq_tx(
                    conn,
                    CATALOG_PROJECTION_NAME,
                    outcome.event.seq,
                )?;
            }
            Ok(outcome)
        })
    }
}

pub fn catalog_xprompt_source_observed_event_request(
    context: CatalogProjectionEventContextWire,
    xprompt: XpromptCatalogProjectionWire,
) -> Result<EventAppendRequestWire, ProjectionError> {
    catalog_event_request(
        context,
        CATALOG_EVENT_XPROMPT_SOURCE_OBSERVED,
        CatalogProjectionEventPayloadWire::XpromptSourceObserved {
            schema_version: CATALOG_WIRE_SCHEMA_VERSION,
            xprompt,
        },
    )
}

pub fn catalog_xprompt_source_removed_event_request(
    context: CatalogProjectionEventContextWire,
    catalog_id: String,
) -> Result<EventAppendRequestWire, ProjectionError> {
    catalog_event_request(
        context,
        CATALOG_EVENT_XPROMPT_SOURCE_REMOVED,
        CatalogProjectionEventPayloadWire::XpromptSourceRemoved {
            schema_version: CATALOG_WIRE_SCHEMA_VERSION,
            catalog_id,
        },
    )
}

pub fn catalog_config_source_observed_event_request(
    context: CatalogProjectionEventContextWire,
    config: ConfigCatalogProjectionWire,
) -> Result<EventAppendRequestWire, ProjectionError> {
    catalog_event_request(
        context,
        CATALOG_EVENT_CONFIG_SOURCE_OBSERVED,
        CatalogProjectionEventPayloadWire::ConfigSourceObserved {
            schema_version: CATALOG_WIRE_SCHEMA_VERSION,
            config,
        },
    )
}

pub fn catalog_config_source_removed_event_request(
    context: CatalogProjectionEventContextWire,
    config_id: String,
) -> Result<EventAppendRequestWire, ProjectionError> {
    catalog_event_request(
        context,
        CATALOG_EVENT_CONFIG_SOURCE_REMOVED,
        CatalogProjectionEventPayloadWire::ConfigSourceRemoved {
            schema_version: CATALOG_WIRE_SCHEMA_VERSION,
            config_id,
        },
    )
}

pub fn catalog_memory_source_observed_event_request(
    context: CatalogProjectionEventContextWire,
    memory: MemoryCatalogProjectionWire,
) -> Result<EventAppendRequestWire, ProjectionError> {
    catalog_event_request(
        context,
        CATALOG_EVENT_MEMORY_SOURCE_OBSERVED,
        CatalogProjectionEventPayloadWire::MemorySourceObserved {
            schema_version: CATALOG_WIRE_SCHEMA_VERSION,
            memory,
        },
    )
}

pub fn catalog_memory_source_removed_event_request(
    context: CatalogProjectionEventContextWire,
    memory_id: String,
) -> Result<EventAppendRequestWire, ProjectionError> {
    catalog_event_request(
        context,
        CATALOG_EVENT_MEMORY_SOURCE_REMOVED,
        CatalogProjectionEventPayloadWire::MemorySourceRemoved {
            schema_version: CATALOG_WIRE_SCHEMA_VERSION,
            memory_id,
        },
    )
}

pub fn catalog_file_history_updated_event_request(
    context: CatalogProjectionEventContextWire,
    file: FileHistoryProjectionWire,
) -> Result<EventAppendRequestWire, ProjectionError> {
    catalog_event_request(
        context,
        CATALOG_EVENT_FILE_HISTORY_UPDATED,
        CatalogProjectionEventPayloadWire::FileHistoryUpdated {
            schema_version: CATALOG_WIRE_SCHEMA_VERSION,
            file,
        },
    )
}

pub fn catalog_file_history_removed_event_request(
    context: CatalogProjectionEventContextWire,
    path: String,
    branch_or_workspace: Option<String>,
) -> Result<EventAppendRequestWire, ProjectionError> {
    catalog_event_request(
        context,
        CATALOG_EVENT_FILE_HISTORY_REMOVED,
        CatalogProjectionEventPayloadWire::FileHistoryRemoved {
            schema_version: CATALOG_WIRE_SCHEMA_VERSION,
            path,
            branch_or_workspace,
        },
    )
}

pub fn catalog_projection_xprompts(
    conn: &Connection,
    project_id: &str,
    query: Option<&str>,
    include_removed: bool,
) -> Result<Vec<MobileXpromptCatalogEntryWire>, ProjectionError> {
    let normalized = query.map(str::to_lowercase);
    let mut stmt = conn.prepare(
        r#"
        SELECT entry_json
        FROM xprompt_catalog
        WHERE project_id = ?1 AND (?2 OR is_present = 1)
        ORDER BY name ASC, source_bucket ASC, catalog_id ASC
        "#,
    )?;
    let rows = stmt.query_map(params![project_id, include_removed], |row| {
        row.get::<_, String>(0)
    })?;
    let mut entries = Vec::new();
    for row in rows {
        let entry: MobileXpromptCatalogEntryWire = serde_json::from_str(&row?)?;
        if let Some(query) = normalized.as_deref() {
            let haystack = format!(
                "{} {} {} {}",
                entry.name,
                entry.description.as_deref().unwrap_or(""),
                entry.source_bucket,
                entry.content_preview.as_deref().unwrap_or("")
            )
            .to_lowercase();
            if !haystack.contains(query) {
                continue;
            }
        }
        entries.push(entry);
    }
    Ok(entries)
}

pub fn catalog_projection_config_sources(
    conn: &Connection,
    project_id: &str,
    include_removed: bool,
) -> Result<Vec<ConfigCatalogProjectionWire>, ProjectionError> {
    let mut stmt = conn.prepare(
        r#"
        SELECT config_json, is_present
        FROM config_catalog
        WHERE project_id = ?1 AND (?2 OR is_present = 1)
        ORDER BY scope ASC, path ASC
        "#,
    )?;
    let rows = stmt.query_map(params![project_id, include_removed], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? != 0))
    })?;
    let mut entries = Vec::new();
    for row in rows {
        let (json, exists) = row?;
        let mut config: ConfigCatalogProjectionWire =
            serde_json::from_str(&json)?;
        config.exists = exists;
        entries.push(config);
    }
    Ok(entries)
}

pub fn catalog_projection_memory_sources(
    conn: &Connection,
    project_id: &str,
    include_removed: bool,
) -> Result<Vec<MemoryCatalogProjectionWire>, ProjectionError> {
    let mut stmt = conn.prepare(
        r#"
        SELECT memory_json, is_present
        FROM memory_catalog
        WHERE project_id = ?1 AND (?2 OR is_present = 1)
        ORDER BY tier ASC, path ASC
        "#,
    )?;
    let rows = stmt.query_map(params![project_id, include_removed], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? != 0))
    })?;
    let mut entries = Vec::new();
    for row in rows {
        let (json, exists) = row?;
        let mut memory: MemoryCatalogProjectionWire =
            serde_json::from_str(&json)?;
        memory.exists = exists;
        entries.push(memory);
    }
    Ok(entries)
}

pub fn catalog_projection_file_history_rows(
    conn: &Connection,
    project_id: &str,
    branch_or_workspace: Option<&str>,
    include_removed: bool,
) -> Result<Vec<FileHistoryProjectionWire>, ProjectionError> {
    let mut stmt = conn.prepare(
        r#"
        SELECT file_json, is_present
        FROM file_history
        WHERE project_id = ?1
          AND (?2 OR is_present = 1)
          AND (?3 IS NULL OR branch_or_workspace = ?3)
        ORDER BY COALESCE(last_used_at, '') DESC, path ASC
        "#,
    )?;
    let rows = stmt.query_map(
        params![project_id, include_removed, branch_or_workspace],
        |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? != 0)),
    )?;
    let mut entries = Vec::new();
    for row in rows {
        let (json, exists) = row?;
        let mut file: FileHistoryProjectionWire = serde_json::from_str(&json)?;
        file.exists = exists;
        entries.push(file);
    }
    Ok(entries)
}

pub fn catalog_projection_file_history_completion(
    conn: &Connection,
    project_id: &str,
    branch_or_workspace: Option<&str>,
) -> Result<CompletionList, ProjectionError> {
    let rows = catalog_projection_file_history_rows(
        conn,
        project_id,
        branch_or_workspace,
        false,
    )?;
    Ok(build_file_history_completion_candidates(
        rows.into_iter().map(|row| row.path),
    ))
}

fn apply_catalog_event_tx(
    conn: &Connection,
    event: &EventEnvelopeWire,
) -> Result<(), ProjectionError> {
    if !is_catalog_event(&event.event_type) {
        return Ok(());
    }
    let payload: CatalogProjectionEventPayloadWire =
        serde_json::from_value(event.payload.clone())?;
    match payload {
        CatalogProjectionEventPayloadWire::XpromptSourceObserved {
            xprompt,
            ..
        } => upsert_xprompt_tx(conn, event, &xprompt)?,
        CatalogProjectionEventPayloadWire::XpromptSourceRemoved {
            catalog_id,
            ..
        } => mark_xprompt_removed_tx(conn, event, &catalog_id)?,
        CatalogProjectionEventPayloadWire::ConfigSourceObserved {
            config,
            ..
        } => upsert_config_tx(conn, event, &config)?,
        CatalogProjectionEventPayloadWire::ConfigSourceRemoved {
            config_id,
            ..
        } => mark_config_removed_tx(conn, event, &config_id)?,
        CatalogProjectionEventPayloadWire::MemorySourceObserved {
            memory,
            ..
        } => upsert_memory_tx(conn, event, &memory)?,
        CatalogProjectionEventPayloadWire::MemorySourceRemoved {
            memory_id,
            ..
        } => mark_memory_removed_tx(conn, event, &memory_id)?,
        CatalogProjectionEventPayloadWire::FileHistoryUpdated {
            file, ..
        } => upsert_file_history_tx(conn, event, &file)?,
        CatalogProjectionEventPayloadWire::FileHistoryRemoved {
            path,
            branch_or_workspace,
            ..
        } => mark_file_history_removed_tx(
            conn,
            event,
            &path,
            branch_or_workspace.as_deref(),
        )?,
    }
    insert_catalog_event_tx(conn, event)?;
    Ok(())
}

fn is_catalog_event(event_type: &str) -> bool {
    matches!(
        event_type,
        CATALOG_EVENT_XPROMPT_SOURCE_OBSERVED
            | CATALOG_EVENT_XPROMPT_SOURCE_REMOVED
            | CATALOG_EVENT_CONFIG_SOURCE_OBSERVED
            | CATALOG_EVENT_CONFIG_SOURCE_REMOVED
            | CATALOG_EVENT_MEMORY_SOURCE_OBSERVED
            | CATALOG_EVENT_MEMORY_SOURCE_REMOVED
            | CATALOG_EVENT_FILE_HISTORY_UPDATED
            | CATALOG_EVENT_FILE_HISTORY_REMOVED
    )
}

fn upsert_xprompt_tx(
    conn: &Connection,
    event: &EventEnvelopeWire,
    xprompt: &XpromptCatalogProjectionWire,
) -> Result<(), ProjectionError> {
    let mut stored = xprompt.clone();
    stored.exists = true;
    let entry_json = serde_json::to_string(&stored.entry)?;
    let xprompt_json = serde_json::to_string(&stored)?;
    conn.execute(
        r#"
        INSERT INTO xprompt_catalog (
            project_id, catalog_id, name, source_bucket, project,
            kind, entry_json, xprompt_json, source_path, definition_path,
            content_preview, is_skill, content_hash, is_present, updated_at,
            last_seq
        ) VALUES (
            ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10,
            ?11, ?12, ?13, ?14, ?15, ?16
        )
        ON CONFLICT(project_id, catalog_id) DO UPDATE SET
            name = excluded.name,
            source_bucket = excluded.source_bucket,
            project = excluded.project,
            kind = excluded.kind,
            entry_json = excluded.entry_json,
            xprompt_json = excluded.xprompt_json,
            source_path = excluded.source_path,
            definition_path = excluded.definition_path,
            content_preview = excluded.content_preview,
            is_skill = excluded.is_skill,
            content_hash = excluded.content_hash,
            is_present = excluded.is_present,
            updated_at = excluded.updated_at,
            last_seq = excluded.last_seq
        "#,
        params![
            event.project_id,
            stored.catalog_id,
            stored.entry.name,
            stored.entry.source_bucket,
            stored.entry.project,
            stored.entry.kind,
            entry_json,
            xprompt_json,
            stored
                .source_path
                .as_deref()
                .or(stored.entry.definition_path.as_deref()),
            stored.entry.definition_path,
            stored.entry.content_preview,
            stored.entry.is_skill,
            stored.content_hash,
            stored.exists,
            stored.updated_at.as_deref().unwrap_or(&event.created_at),
            event.seq,
        ],
    )?;
    Ok(())
}

fn upsert_config_tx(
    conn: &Connection,
    event: &EventEnvelopeWire,
    config: &ConfigCatalogProjectionWire,
) -> Result<(), ProjectionError> {
    let mut stored = config.clone();
    stored.exists = true;
    let metadata_json = serde_json::to_string(&stored.metadata)?;
    let config_json = serde_json::to_string(&stored)?;
    conn.execute(
        r#"
        INSERT INTO config_catalog (
            project_id, config_id, path, scope, content_preview,
            metadata_json, config_json, is_present, updated_at, last_seq
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
        ON CONFLICT(project_id, config_id) DO UPDATE SET
            path = excluded.path,
            scope = excluded.scope,
            content_preview = excluded.content_preview,
            metadata_json = excluded.metadata_json,
            config_json = excluded.config_json,
            is_present = excluded.is_present,
            updated_at = excluded.updated_at,
            last_seq = excluded.last_seq
        "#,
        params![
            event.project_id,
            stored.config_id,
            stored.path,
            stored.scope,
            stored.content_preview,
            metadata_json,
            config_json,
            stored.exists,
            stored.updated_at.as_deref().unwrap_or(&event.created_at),
            event.seq,
        ],
    )?;
    Ok(())
}

fn upsert_memory_tx(
    conn: &Connection,
    event: &EventEnvelopeWire,
    memory: &MemoryCatalogProjectionWire,
) -> Result<(), ProjectionError> {
    let mut stored = memory.clone();
    stored.exists = true;
    let keywords_json = serde_json::to_string(&stored.keywords)?;
    let metadata_json = serde_json::to_string(&stored.metadata)?;
    let memory_json = serde_json::to_string(&stored)?;
    conn.execute(
        r#"
        INSERT INTO memory_catalog (
            project_id, memory_id, path, tier, source_name, keywords_json,
            content_preview, metadata_json, memory_json, is_present, updated_at,
            last_seq
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
        ON CONFLICT(project_id, memory_id) DO UPDATE SET
            path = excluded.path,
            tier = excluded.tier,
            source_name = excluded.source_name,
            keywords_json = excluded.keywords_json,
            content_preview = excluded.content_preview,
            metadata_json = excluded.metadata_json,
            memory_json = excluded.memory_json,
            is_present = excluded.is_present,
            updated_at = excluded.updated_at,
            last_seq = excluded.last_seq
        "#,
        params![
            event.project_id,
            stored.memory_id,
            stored.path,
            stored.tier,
            stored.source_name,
            keywords_json,
            stored.content_preview,
            metadata_json,
            memory_json,
            stored.exists,
            stored.updated_at.as_deref().unwrap_or(&event.created_at),
            event.seq,
        ],
    )?;
    Ok(())
}

fn upsert_file_history_tx(
    conn: &Connection,
    event: &EventEnvelopeWire,
    file: &FileHistoryProjectionWire,
) -> Result<(), ProjectionError> {
    let mut stored = file.clone();
    stored.exists = true;
    let metadata_json = serde_json::to_string(&stored.metadata)?;
    let file_json = serde_json::to_string(&stored)?;
    conn.execute(
        r#"
        INSERT INTO file_history (
            project_id, path, branch_or_workspace, workspace_name,
            last_used_at, use_count, metadata_json, file_json, is_present,
            last_seq
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
        ON CONFLICT(project_id, path, branch_or_workspace) DO UPDATE SET
            workspace_name = excluded.workspace_name,
            last_used_at = excluded.last_used_at,
            use_count = excluded.use_count,
            metadata_json = excluded.metadata_json,
            file_json = excluded.file_json,
            is_present = excluded.is_present,
            last_seq = excluded.last_seq
        "#,
        params![
            event.project_id,
            stored.path,
            stored.branch_or_workspace.as_deref().unwrap_or(""),
            stored.workspace_name,
            stored.last_used_at.as_deref().unwrap_or(&event.created_at),
            stored.use_count as i64,
            metadata_json,
            file_json,
            stored.exists,
            event.seq,
        ],
    )?;
    Ok(())
}

fn mark_xprompt_removed_tx(
    conn: &Connection,
    event: &EventEnvelopeWire,
    catalog_id: &str,
) -> Result<(), ProjectionError> {
    conn.execute(
        "UPDATE xprompt_catalog SET is_present = 0, last_seq = ?3 WHERE project_id = ?1 AND catalog_id = ?2",
        params![event.project_id, catalog_id, event.seq],
    )?;
    Ok(())
}

fn mark_config_removed_tx(
    conn: &Connection,
    event: &EventEnvelopeWire,
    config_id: &str,
) -> Result<(), ProjectionError> {
    conn.execute(
        "UPDATE config_catalog SET is_present = 0, last_seq = ?3 WHERE project_id = ?1 AND config_id = ?2",
        params![event.project_id, config_id, event.seq],
    )?;
    Ok(())
}

fn mark_memory_removed_tx(
    conn: &Connection,
    event: &EventEnvelopeWire,
    memory_id: &str,
) -> Result<(), ProjectionError> {
    conn.execute(
        "UPDATE memory_catalog SET is_present = 0, last_seq = ?3 WHERE project_id = ?1 AND memory_id = ?2",
        params![event.project_id, memory_id, event.seq],
    )?;
    Ok(())
}

fn mark_file_history_removed_tx(
    conn: &Connection,
    event: &EventEnvelopeWire,
    path: &str,
    branch_or_workspace: Option<&str>,
) -> Result<(), ProjectionError> {
    conn.execute(
        r#"
        UPDATE file_history
        SET is_present = 0, last_seq = ?4
        WHERE project_id = ?1 AND path = ?2 AND branch_or_workspace = ?3
        "#,
        params![
            event.project_id,
            path,
            branch_or_workspace.unwrap_or(""),
            event.seq,
        ],
    )?;
    Ok(())
}

fn insert_catalog_event_tx(
    conn: &Connection,
    event: &EventEnvelopeWire,
) -> Result<(), ProjectionError> {
    let payload_json = serde_json::to_string(&event.payload)?;
    conn.execute(
        r#"
        INSERT INTO catalog_events (
            seq, project_id, event_type, payload_json, created_at
        ) VALUES (?1, ?2, ?3, ?4, ?5)
        ON CONFLICT(seq) DO UPDATE SET
            project_id = excluded.project_id,
            event_type = excluded.event_type,
            payload_json = excluded.payload_json,
            created_at = excluded.created_at
        "#,
        params![
            event.seq,
            event.project_id,
            event.event_type,
            payload_json,
            event.created_at,
        ],
    )?;
    Ok(())
}

fn catalog_event_request(
    context: CatalogProjectionEventContextWire,
    event_type: &str,
    payload: CatalogProjectionEventPayloadWire,
) -> Result<EventAppendRequestWire, ProjectionError> {
    Ok(EventAppendRequestWire {
        created_at: context.created_at,
        source: context.source,
        host_id: context.host_id,
        project_id: context.project_id,
        event_type: event_type.to_string(),
        payload: serde_json::to_value(payload)?,
        idempotency_key: context.idempotency_key,
        causality: context.causality,
        source_path: context.source_path,
        source_revision: context.source_revision,
    })
}

fn catalog_schema_version() -> u32 {
    CATALOG_WIRE_SCHEMA_VERSION
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::projections::event::EventSourceWire;
    use crate::xprompt_catalog::{
        load_editor_xprompt_catalog, XpromptCatalogLoadOptions,
    };
    use crate::{EditorXpromptCatalogRequestWire, MobileHelperStatusWire};
    use tempfile::tempdir;

    fn context(key: &str) -> CatalogProjectionEventContextWire {
        CatalogProjectionEventContextWire {
            schema_version: CATALOG_WIRE_SCHEMA_VERSION,
            created_at: Some("2026-05-13T22:45:00.000Z".to_string()),
            source: EventSourceWire {
                source_type: "test".to_string(),
                name: "catalog-test".to_string(),
                ..EventSourceWire::default()
            },
            host_id: "host-a".to_string(),
            project_id: "project-a".to_string(),
            idempotency_key: Some(key.to_string()),
            causality: Vec::new(),
            source_path: None,
            source_revision: None,
        }
    }

    #[test]
    fn xprompt_projection_can_store_existing_loader_results() {
        let dir = tempdir().unwrap();
        let xprompt_dir = dir.path().join("xprompts");
        std::fs::create_dir(&xprompt_dir).unwrap();
        std::fs::write(
            xprompt_dir.join("review.md"),
            "---\ndescription: Review code\ntags: [review]\n---\nReview this.",
        )
        .unwrap();

        let response = load_editor_xprompt_catalog(
            &EditorXpromptCatalogRequestWire {
                schema_version: 1,
                project: None,
                source: None,
                tag: None,
                query: None,
                include_pdf: false,
                limit: None,
                device_id: None,
            },
            &XpromptCatalogLoadOptions::new(Some(dir.path().to_path_buf())),
        )
        .unwrap();
        assert_eq!(response.result.status, MobileHelperStatusWire::Success);
        let entry = response
            .entries
            .into_iter()
            .find(|entry| entry.name == "review")
            .unwrap();

        let mut db = ProjectionDb::open_in_memory().unwrap();
        db.append_catalog_event(
            catalog_xprompt_source_observed_event_request(
                context("xprompt-review"),
                XpromptCatalogProjectionWire {
                    schema_version: CATALOG_WIRE_SCHEMA_VERSION,
                    catalog_id: "project:review".to_string(),
                    source_path: entry.definition_path.clone(),
                    content_hash: None,
                    exists: true,
                    updated_at: None,
                    entry: entry.clone(),
                },
            )
            .unwrap(),
        )
        .unwrap();

        assert_eq!(
            catalog_projection_xprompts(
                db.connection(),
                "project-a",
                None,
                false
            )
            .unwrap(),
            vec![entry]
        );
    }

    #[test]
    fn memory_and_file_history_sources_can_be_invalidated() {
        let mut db = ProjectionDb::open_in_memory().unwrap();
        db.append_catalog_event(
            catalog_memory_source_observed_event_request(
                context("memory-observed"),
                MemoryCatalogProjectionWire {
                    schema_version: CATALOG_WIRE_SCHEMA_VERSION,
                    memory_id: "short/build".to_string(),
                    path: "memory/short/build_and_run.md".to_string(),
                    tier: "short".to_string(),
                    source_name: "build_and_run".to_string(),
                    keywords: vec!["build".to_string()],
                    content_preview: Some("just check".to_string()),
                    exists: true,
                    updated_at: None,
                    metadata: BTreeMap::new(),
                },
            )
            .unwrap(),
        )
        .unwrap();
        db.append_catalog_event(
            catalog_file_history_updated_event_request(
                context("file-history"),
                FileHistoryProjectionWire {
                    schema_version: CATALOG_WIRE_SCHEMA_VERSION,
                    path: "src/lib.rs".to_string(),
                    branch_or_workspace: Some("main".to_string()),
                    workspace_name: None,
                    last_used_at: Some("2026-05-13T22:00:00.000Z".to_string()),
                    use_count: 2,
                    exists: true,
                    metadata: BTreeMap::new(),
                },
            )
            .unwrap(),
        )
        .unwrap();

        assert_eq!(
            catalog_projection_file_history_completion(
                db.connection(),
                "project-a",
                Some("main")
            )
            .unwrap()
            .candidates[0]
                .insertion,
            "src/lib.rs"
        );

        db.append_catalog_event(
            catalog_memory_source_removed_event_request(
                context("memory-removed"),
                "short/build".to_string(),
            )
            .unwrap(),
        )
        .unwrap();
        db.append_catalog_event(
            catalog_file_history_removed_event_request(
                context("file-removed"),
                "src/lib.rs".to_string(),
                Some("main".to_string()),
            )
            .unwrap(),
        )
        .unwrap();

        assert!(catalog_projection_memory_sources(
            db.connection(),
            "project-a",
            false
        )
        .unwrap()
        .is_empty());
        assert!(catalog_projection_file_history_completion(
            db.connection(),
            "project-a",
            Some("main")
        )
        .unwrap()
        .candidates
        .is_empty());
        assert!(
            !catalog_projection_memory_sources(
                db.connection(),
                "project-a",
                true
            )
            .unwrap()[0]
                .exists
        );
    }
}
