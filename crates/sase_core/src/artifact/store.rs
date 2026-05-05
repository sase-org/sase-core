//! SQLite store initialization for the unified artifact graph.

use std::fs;
use std::path::{Path, PathBuf};

use rusqlite::{params, Connection, OptionalExtension, Transaction};

use super::wire::{
    ArtifactLinkRemoveWire, ArtifactLinkUpsertWire, ArtifactLinkWire,
    ArtifactMutationResultWire, ArtifactNodeRemoveWire, ArtifactNodeUpsertWire,
    ArtifactNodeWire, ArtifactPayloadWire, ARTIFACT_LINK_PARENT,
    ARTIFACT_PROVENANCE_DERIVED, ARTIFACT_PROVENANCE_MANUAL, ARTIFACT_ROOT_ID,
    ARTIFACT_TOMBSTONE_LINK, ARTIFACT_TOMBSTONE_NODE,
    ARTIFACT_WIRE_SCHEMA_VERSION,
};

const STALE_DERIVED_REASON: &str = "stale_derived";

pub struct ArtifactStore {
    index_path: PathBuf,
    conn: Connection,
}

impl ArtifactStore {
    pub fn index_path(&self) -> &Path {
        &self.index_path
    }

    pub fn connection(&self) -> &Connection {
        &self.conn
    }

    pub fn connection_mut(&mut self) -> &mut Connection {
        &mut self.conn
    }
}

pub fn open_artifact_store(index_path: &Path) -> Result<ArtifactStore, String> {
    if let Some(parent) = index_path.parent() {
        fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    let conn = Connection::open(index_path).map_err(|e| e.to_string())?;
    conn.busy_timeout(std::time::Duration::from_secs(5))
        .map_err(|e| e.to_string())?;
    init_schema(&conn)?;
    ensure_root_artifact(&conn)?;
    Ok(ArtifactStore {
        index_path: index_path.to_path_buf(),
        conn,
    })
}

fn init_schema(conn: &Connection) -> Result<(), String> {
    conn.execute_batch(
        r#"
        PRAGMA journal_mode = WAL;
        PRAGMA foreign_keys = ON;
        CREATE TABLE IF NOT EXISTS meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS artifacts (
            id TEXT PRIMARY KEY,
            kind TEXT NOT NULL,
            display_title TEXT NOT NULL,
            subtitle TEXT,
            provenance TEXT NOT NULL,
            source_kind TEXT,
            source_id TEXT,
            source_version TEXT,
            search_text TEXT NOT NULL DEFAULT '',
            node_json TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS artifact_links (
            id TEXT PRIMARY KEY,
            link_type TEXT NOT NULL,
            source_id TEXT NOT NULL,
            target_id TEXT NOT NULL,
            provenance TEXT NOT NULL,
            source_kind TEXT,
            source_id_hint TEXT,
            source_version TEXT,
            link_json TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(source_id) REFERENCES artifacts(id) ON DELETE CASCADE,
            FOREIGN KEY(target_id) REFERENCES artifacts(id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS artifact_payloads (
            artifact_id TEXT NOT NULL,
            payload_type TEXT NOT NULL,
            provenance TEXT NOT NULL,
            source_kind TEXT NOT NULL DEFAULT '',
            source_id TEXT NOT NULL DEFAULT '',
            source_version TEXT,
            payload_json TEXT NOT NULL,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (
                artifact_id,
                payload_type,
                provenance,
                source_kind,
                source_id
            ),
            FOREIGN KEY(artifact_id) REFERENCES artifacts(id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS source_watermarks (
            source_kind TEXT NOT NULL,
            source_id TEXT NOT NULL,
            watermark TEXT NOT NULL,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (source_kind, source_id)
        );
        CREATE TABLE IF NOT EXISTS manual_tombstones (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tombstone_type TEXT NOT NULL,
            artifact_id TEXT,
            link_id TEXT,
            source_kind TEXT,
            source_id TEXT,
            reason TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_artifacts_id
            ON artifacts(id);
        CREATE INDEX IF NOT EXISTS idx_artifacts_kind
            ON artifacts(kind, display_title, id);
        CREATE INDEX IF NOT EXISTS idx_artifacts_source
            ON artifacts(provenance, source_kind, source_id);
        CREATE INDEX IF NOT EXISTS idx_artifacts_text
            ON artifacts(search_text, display_title, id);
        CREATE INDEX IF NOT EXISTS idx_artifact_links_type
            ON artifact_links(link_type, source_id, target_id);
        CREATE INDEX IF NOT EXISTS idx_artifact_links_source
            ON artifact_links(source_id, link_type, target_id);
        CREATE INDEX IF NOT EXISTS idx_artifact_links_target
            ON artifact_links(target_id, link_type, source_id);
        CREATE INDEX IF NOT EXISTS idx_artifact_links_parent_children
            ON artifact_links(target_id, source_id)
            WHERE link_type = 'parent';
        CREATE INDEX IF NOT EXISTS idx_artifact_links_source_marker
            ON artifact_links(provenance, source_kind, source_id_hint);
        CREATE INDEX IF NOT EXISTS idx_artifact_payloads_source
            ON artifact_payloads(provenance, source_kind, source_id);
        CREATE INDEX IF NOT EXISTS idx_source_watermarks_updated
            ON source_watermarks(updated_at);
        CREATE INDEX IF NOT EXISTS idx_manual_tombstones_node
            ON manual_tombstones(tombstone_type, artifact_id);
        CREATE INDEX IF NOT EXISTS idx_manual_tombstones_link
            ON manual_tombstones(tombstone_type, link_id);
        "#,
    )
    .map_err(|e| e.to_string())?;
    conn.execute(
        "INSERT OR REPLACE INTO meta(key, value) VALUES ('schema_version', ?1)",
        [ARTIFACT_WIRE_SCHEMA_VERSION.to_string()],
    )
    .map_err(|e| e.to_string())?;
    Ok(())
}

fn ensure_root_artifact(conn: &Connection) -> Result<(), String> {
    let root = ArtifactNodeWire::root();
    let node_json = serde_json::to_string(&root).map_err(|e| e.to_string())?;
    conn.execute(
        r#"
        INSERT INTO artifacts (
            id, kind, display_title, subtitle, provenance, source_kind,
            source_id, source_version, search_text, node_json
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
        ON CONFLICT(id) DO NOTHING
        "#,
        params![
            root.id,
            root.kind,
            root.display_title,
            root.subtitle,
            root.provenance,
            root.source_kind,
            root.source_id,
            root.source_version,
            root.search_text,
            node_json,
        ],
    )
    .map_err(|e| e.to_string())?;
    Ok(())
}

pub fn deterministic_artifact_link_id(
    link_type: &str,
    source_id: &str,
    target_id: &str,
) -> String {
    format!(
        "link:{}:{}:{}:{}:{}:{}",
        link_type.len(),
        link_type,
        source_id.len(),
        source_id,
        target_id.len(),
        target_id
    )
}

pub fn upsert_artifact_node(
    store: &mut ArtifactStore,
    request: ArtifactNodeUpsertWire,
) -> Result<ArtifactMutationResultWire, String> {
    validate_schema_version(request.schema_version)?;
    validate_node(&request.node)?;

    let tx = store.conn.transaction().map_err(|e| e.to_string())?;
    if request.node.provenance == ARTIFACT_PROVENANCE_MANUAL {
        clear_node_tombstones(&tx, &request.node.id)?;
    } else if request.node.provenance == ARTIFACT_PROVENANCE_DERIVED {
        clear_stale_node_tombstones(&tx, &request.node.id)?;
        if node_has_manual_tombstone(&tx, &request.node.id)? {
            tx.commit().map_err(|e| e.to_string())?;
            return Ok(ArtifactMutationResultWire {
                operation: "upsert_node".to_string(),
                ..ArtifactMutationResultWire::default()
            });
        }
    }

    let node_json =
        serde_json::to_string(&request.node).map_err(|e| e.to_string())?;
    let existing_json: Option<String> = tx
        .query_row(
            "SELECT node_json FROM artifacts WHERE id = ?1",
            [&request.node.id],
            |row| row.get(0),
        )
        .optional()
        .map_err(|e| e.to_string())?;

    let mut result = ArtifactMutationResultWire {
        operation: "upsert_node".to_string(),
        affected_node_ids: vec![request.node.id.clone()],
        ..ArtifactMutationResultWire::default()
    };

    match existing_json {
        Some(existing) if existing == node_json => {}
        Some(_) => {
            tx.execute(
                r#"
                UPDATE artifacts
                SET kind = ?2,
                    display_title = ?3,
                    subtitle = ?4,
                    provenance = ?5,
                    source_kind = ?6,
                    source_id = ?7,
                    source_version = ?8,
                    search_text = ?9,
                    node_json = ?10,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?1
                "#,
                params![
                    request.node.id,
                    request.node.kind,
                    request.node.display_title,
                    request.node.subtitle,
                    request.node.provenance,
                    request.node.source_kind,
                    request.node.source_id,
                    request.node.source_version,
                    request.node.search_text,
                    node_json,
                ],
            )
            .map_err(|e| e.to_string())?;
            result.nodes_updated = 1;
        }
        None => {
            tx.execute(
                r#"
                INSERT INTO artifacts (
                    id, kind, display_title, subtitle, provenance, source_kind,
                    source_id, source_version, search_text, node_json
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
                "#,
                params![
                    request.node.id,
                    request.node.kind,
                    request.node.display_title,
                    request.node.subtitle,
                    request.node.provenance,
                    request.node.source_kind,
                    request.node.source_id,
                    request.node.source_version,
                    request.node.search_text,
                    node_json,
                ],
            )
            .map_err(|e| e.to_string())?;
            result.nodes_added = 1;
        }
    }

    if request.replace_payloads {
        tx.execute(
            "DELETE FROM artifact_payloads WHERE artifact_id = ?1",
            [&request.node.id],
        )
        .map_err(|e| e.to_string())?;
    }

    tx.commit().map_err(|e| e.to_string())?;
    Ok(result)
}

pub fn remove_artifact_node(
    store: &mut ArtifactStore,
    request: ArtifactNodeRemoveWire,
) -> Result<ArtifactMutationResultWire, String> {
    validate_schema_version(request.schema_version)?;
    if request.id == ARTIFACT_ROOT_ID {
        return Err(
            "artifact root '/' cannot be removed or tombstoned".to_string()
        );
    }

    let tx = store.conn.transaction().map_err(|e| e.to_string())?;
    let row: Option<(String, Option<String>, Option<String>)> = tx
        .query_row(
            "SELECT provenance, source_kind, source_id FROM artifacts WHERE id = ?1",
            [&request.id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .map_err(|e| e.to_string())?;

    let mut result = ArtifactMutationResultWire {
        operation: "remove_node".to_string(),
        affected_node_ids: vec![request.id.clone()],
        ..ArtifactMutationResultWire::default()
    };

    let Some((provenance, row_source_kind, row_source_id)) = row else {
        tx.commit().map_err(|e| e.to_string())?;
        return Ok(result);
    };
    let requested_provenance =
        request.provenance.as_deref().unwrap_or(&provenance);
    if requested_provenance == ARTIFACT_PROVENANCE_MANUAL {
        result.nodes_removed = tx
            .execute(
                "DELETE FROM artifacts WHERE id = ?1 AND provenance = ?2",
                params![request.id, ARTIFACT_PROVENANCE_MANUAL],
            )
            .map_err(|e| e.to_string())? as u64;
    } else {
        let source_kind = request.source_kind.or(row_source_kind);
        let source_id = request.source_id.or(row_source_id);
        if !node_has_tombstone(&tx, &request.id)? {
            let tombstone_id = insert_tombstone(
                &tx,
                ARTIFACT_TOMBSTONE_NODE,
                Some(&request.id),
                None,
                source_kind.as_deref(),
                source_id.as_deref(),
                request.reason.as_deref(),
            )?;
            result.tombstones_added = 1;
            result.tombstone_ids.push(tombstone_id);
        }
    }

    tx.commit().map_err(|e| e.to_string())?;
    Ok(result)
}

pub fn upsert_artifact_link(
    store: &mut ArtifactStore,
    request: ArtifactLinkUpsertWire,
) -> Result<ArtifactMutationResultWire, String> {
    validate_schema_version(request.schema_version)?;
    let mut link = request.link;
    validate_link(&link)?;
    if link.id.trim().is_empty() {
        link.id = deterministic_artifact_link_id(
            &link.link_type,
            &link.source_id,
            &link.target_id,
        );
    }

    let tx = store.conn.transaction().map_err(|e| e.to_string())?;
    ensure_endpoint_exists(&tx, &link.source_id)?;
    ensure_endpoint_exists(&tx, &link.target_id)?;

    if link.provenance == ARTIFACT_PROVENANCE_MANUAL {
        clear_link_tombstones(&tx, &link.id)?;
    } else if link.provenance == ARTIFACT_PROVENANCE_DERIVED {
        clear_stale_link_tombstones(&tx, &link.id)?;
        if link_has_manual_tombstone(&tx, &link.id)? {
            tx.commit().map_err(|e| e.to_string())?;
            return Ok(ArtifactMutationResultWire {
                operation: "upsert_link".to_string(),
                ..ArtifactMutationResultWire::default()
            });
        }
    }

    let link_json = serde_json::to_string(&link).map_err(|e| e.to_string())?;
    let existing_json: Option<String> = tx
        .query_row(
            "SELECT link_json FROM artifact_links WHERE id = ?1",
            [&link.id],
            |row| row.get(0),
        )
        .optional()
        .map_err(|e| e.to_string())?;

    let mut result = ArtifactMutationResultWire {
        operation: "upsert_link".to_string(),
        affected_link_ids: vec![link.id.clone()],
        ..ArtifactMutationResultWire::default()
    };

    match existing_json {
        Some(existing) if existing == link_json => {}
        Some(_) => {
            tx.execute(
                r#"
                UPDATE artifact_links
                SET link_type = ?2,
                    source_id = ?3,
                    target_id = ?4,
                    provenance = ?5,
                    source_kind = ?6,
                    source_id_hint = ?7,
                    source_version = ?8,
                    link_json = ?9,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?1
                "#,
                params![
                    link.id,
                    link.link_type,
                    link.source_id,
                    link.target_id,
                    link.provenance,
                    link.source_kind,
                    link.source_id_hint,
                    link.source_version,
                    link_json,
                ],
            )
            .map_err(|e| e.to_string())?;
            result.links_updated = 1;
        }
        None => {
            tx.execute(
                r#"
                INSERT INTO artifact_links (
                    id, link_type, source_id, target_id, provenance,
                    source_kind, source_id_hint, source_version, link_json
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
                "#,
                params![
                    link.id,
                    link.link_type,
                    link.source_id,
                    link.target_id,
                    link.provenance,
                    link.source_kind,
                    link.source_id_hint,
                    link.source_version,
                    link_json,
                ],
            )
            .map_err(|e| e.to_string())?;
            result.links_added = 1;
        }
    }

    tx.commit().map_err(|e| e.to_string())?;
    Ok(result)
}

pub fn remove_artifact_link(
    store: &mut ArtifactStore,
    request: ArtifactLinkRemoveWire,
) -> Result<ArtifactMutationResultWire, String> {
    validate_schema_version(request.schema_version)?;
    let tx = store.conn.transaction().map_err(|e| e.to_string())?;
    let link_id = resolve_link_remove_id(&tx, &request)?;

    let mut result = ArtifactMutationResultWire {
        operation: "remove_link".to_string(),
        ..ArtifactMutationResultWire::default()
    };

    let Some(link_id) = link_id else {
        tx.commit().map_err(|e| e.to_string())?;
        return Ok(result);
    };
    result.affected_link_ids.push(link_id.clone());

    let row: Option<(String, Option<String>, Option<String>)> = tx
        .query_row(
            "SELECT provenance, source_kind, source_id_hint FROM artifact_links WHERE id = ?1",
            [&link_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .map_err(|e| e.to_string())?;
    let Some((provenance, row_source_kind, row_source_id)) = row else {
        tx.commit().map_err(|e| e.to_string())?;
        return Ok(result);
    };

    let requested_provenance =
        request.provenance.as_deref().unwrap_or(&provenance);
    if requested_provenance == ARTIFACT_PROVENANCE_MANUAL {
        result.links_removed = tx
            .execute(
                "DELETE FROM artifact_links WHERE id = ?1 AND provenance = ?2",
                params![link_id, ARTIFACT_PROVENANCE_MANUAL],
            )
            .map_err(|e| e.to_string())? as u64;
    } else if !link_has_tombstone(&tx, &link_id)? {
        let source_kind = request.source_kind.or(row_source_kind);
        let source_id = request.source_id_hint.or(row_source_id);
        let tombstone_id = insert_tombstone(
            &tx,
            ARTIFACT_TOMBSTONE_LINK,
            None,
            Some(&link_id),
            source_kind.as_deref(),
            source_id.as_deref(),
            request.reason.as_deref(),
        )?;
        result.tombstones_added = 1;
        result.tombstone_ids.push(tombstone_id);
    }

    tx.commit().map_err(|e| e.to_string())?;
    Ok(result)
}

pub fn upsert_artifact_payload(
    store: &mut ArtifactStore,
    payload: ArtifactPayloadWire,
) -> Result<ArtifactMutationResultWire, String> {
    let tx = store.conn.transaction().map_err(|e| e.to_string())?;
    ensure_endpoint_exists(&tx, &payload.artifact_id)?;

    let source_kind = payload.source_kind.clone().unwrap_or_default();
    let source_id = payload.source_id.clone().unwrap_or_default();
    let payload_json =
        serde_json::to_string(&payload).map_err(|e| e.to_string())?;
    let existing_json: Option<String> = tx
        .query_row(
            r#"
            SELECT payload_json
            FROM artifact_payloads
            WHERE artifact_id = ?1
              AND payload_type = ?2
              AND provenance = ?3
              AND source_kind = ?4
              AND source_id = ?5
            "#,
            params![
                payload.artifact_id,
                payload.payload_type,
                payload.provenance,
                source_kind,
                source_id,
            ],
            |row| row.get(0),
        )
        .optional()
        .map_err(|e| e.to_string())?;

    let mut result = ArtifactMutationResultWire {
        operation: "upsert_payload".to_string(),
        affected_node_ids: vec![payload.artifact_id.clone()],
        ..ArtifactMutationResultWire::default()
    };

    if existing_json.as_deref() != Some(payload_json.as_str()) {
        tx.execute(
            r#"
            INSERT INTO artifact_payloads (
                artifact_id, payload_type, provenance, source_kind,
                source_id, source_version, payload_json
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
            ON CONFLICT (
                artifact_id, payload_type, provenance, source_kind, source_id
            ) DO UPDATE SET
                source_version = excluded.source_version,
                payload_json = excluded.payload_json,
                updated_at = CURRENT_TIMESTAMP
            "#,
            params![
                payload.artifact_id,
                payload.payload_type,
                payload.provenance,
                source_kind,
                source_id,
                payload.source_version,
                payload_json,
            ],
        )
        .map_err(|e| e.to_string())?;
        result.nodes_updated = 1;
    }

    tx.commit().map_err(|e| e.to_string())?;
    Ok(result)
}

fn validate_schema_version(schema_version: u32) -> Result<(), String> {
    if schema_version != ARTIFACT_WIRE_SCHEMA_VERSION {
        return Err(format!(
            "unsupported artifact schema_version {schema_version}; expected {ARTIFACT_WIRE_SCHEMA_VERSION}"
        ));
    }
    Ok(())
}

fn validate_node(node: &ArtifactNodeWire) -> Result<(), String> {
    if node.id.trim().is_empty() {
        return Err("artifact node id cannot be empty".to_string());
    }
    if node.kind.trim().is_empty() {
        return Err("artifact node kind cannot be empty".to_string());
    }
    if node.display_title.trim().is_empty() {
        return Err("artifact node display_title cannot be empty".to_string());
    }
    validate_provenance(&node.provenance)?;
    Ok(())
}

fn validate_link(link: &ArtifactLinkWire) -> Result<(), String> {
    if link.link_type.trim().is_empty() {
        return Err("artifact link_type cannot be empty".to_string());
    }
    if link.source_id.trim().is_empty() {
        return Err("artifact link source_id cannot be empty".to_string());
    }
    if link.target_id.trim().is_empty() {
        return Err("artifact link target_id cannot be empty".to_string());
    }
    validate_provenance(&link.provenance)?;
    if link.link_type == ARTIFACT_LINK_PARENT
        && link.source_id == link.target_id
    {
        return Err(
            "parent link cannot point an artifact to itself".to_string()
        );
    }
    Ok(())
}

fn validate_provenance(provenance: &str) -> Result<(), String> {
    if provenance == ARTIFACT_PROVENANCE_MANUAL
        || provenance == ARTIFACT_PROVENANCE_DERIVED
    {
        Ok(())
    } else {
        Err(format!(
            "unsupported artifact provenance {provenance:?}; expected manual or derived"
        ))
    }
}

fn ensure_endpoint_exists(
    tx: &Transaction<'_>,
    artifact_id: &str,
) -> Result<(), String> {
    let exists: bool = tx
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM artifacts WHERE id = ?1)",
            [artifact_id],
            |row| row.get(0),
        )
        .map_err(|e| e.to_string())?;
    if exists {
        Ok(())
    } else {
        Err(format!(
            "artifact link endpoint {artifact_id:?} does not exist"
        ))
    }
}

fn node_has_tombstone(
    tx: &Transaction<'_>,
    artifact_id: &str,
) -> Result<bool, String> {
    tx.query_row(
        r#"
        SELECT EXISTS(
            SELECT 1 FROM manual_tombstones
            WHERE tombstone_type = ?1 AND artifact_id = ?2
        )
        "#,
        params![ARTIFACT_TOMBSTONE_NODE, artifact_id],
        |row| row.get(0),
    )
    .map_err(|e| e.to_string())
}

fn node_has_manual_tombstone(
    tx: &Transaction<'_>,
    artifact_id: &str,
) -> Result<bool, String> {
    tx.query_row(
        r#"
        SELECT EXISTS(
            SELECT 1 FROM manual_tombstones
            WHERE tombstone_type = ?1
              AND artifact_id = ?2
              AND COALESCE(reason, '') <> ?3
        )
        "#,
        params![ARTIFACT_TOMBSTONE_NODE, artifact_id, STALE_DERIVED_REASON],
        |row| row.get(0),
    )
    .map_err(|e| e.to_string())
}

fn link_has_tombstone(
    tx: &Transaction<'_>,
    link_id: &str,
) -> Result<bool, String> {
    tx.query_row(
        r#"
        SELECT EXISTS(
            SELECT 1 FROM manual_tombstones
            WHERE tombstone_type = ?1 AND link_id = ?2
        )
        "#,
        params![ARTIFACT_TOMBSTONE_LINK, link_id],
        |row| row.get(0),
    )
    .map_err(|e| e.to_string())
}

fn link_has_manual_tombstone(
    tx: &Transaction<'_>,
    link_id: &str,
) -> Result<bool, String> {
    tx.query_row(
        r#"
        SELECT EXISTS(
            SELECT 1 FROM manual_tombstones
            WHERE tombstone_type = ?1
              AND link_id = ?2
              AND COALESCE(reason, '') <> ?3
        )
        "#,
        params![ARTIFACT_TOMBSTONE_LINK, link_id, STALE_DERIVED_REASON],
        |row| row.get(0),
    )
    .map_err(|e| e.to_string())
}

fn clear_node_tombstones(
    tx: &Transaction<'_>,
    artifact_id: &str,
) -> Result<(), String> {
    tx.execute(
        "DELETE FROM manual_tombstones WHERE tombstone_type = ?1 AND artifact_id = ?2",
        params![ARTIFACT_TOMBSTONE_NODE, artifact_id],
    )
    .map_err(|e| e.to_string())?;
    Ok(())
}

fn clear_link_tombstones(
    tx: &Transaction<'_>,
    link_id: &str,
) -> Result<(), String> {
    tx.execute(
        "DELETE FROM manual_tombstones WHERE tombstone_type = ?1 AND link_id = ?2",
        params![ARTIFACT_TOMBSTONE_LINK, link_id],
    )
    .map_err(|e| e.to_string())?;
    Ok(())
}

fn clear_stale_node_tombstones(
    tx: &Transaction<'_>,
    artifact_id: &str,
) -> Result<(), String> {
    tx.execute(
        r#"
        DELETE FROM manual_tombstones
        WHERE tombstone_type = ?1
          AND artifact_id = ?2
          AND reason = ?3
        "#,
        params![ARTIFACT_TOMBSTONE_NODE, artifact_id, STALE_DERIVED_REASON],
    )
    .map_err(|e| e.to_string())?;
    Ok(())
}

fn clear_stale_link_tombstones(
    tx: &Transaction<'_>,
    link_id: &str,
) -> Result<(), String> {
    tx.execute(
        r#"
        DELETE FROM manual_tombstones
        WHERE tombstone_type = ?1
          AND link_id = ?2
          AND reason = ?3
        "#,
        params![ARTIFACT_TOMBSTONE_LINK, link_id, STALE_DERIVED_REASON],
    )
    .map_err(|e| e.to_string())?;
    Ok(())
}

fn insert_tombstone(
    tx: &Transaction<'_>,
    tombstone_type: &str,
    artifact_id: Option<&str>,
    link_id: Option<&str>,
    source_kind: Option<&str>,
    source_id: Option<&str>,
    reason: Option<&str>,
) -> Result<String, String> {
    tx.execute(
        r#"
        INSERT INTO manual_tombstones (
            tombstone_type, artifact_id, link_id, source_kind, source_id, reason
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
        "#,
        params![
            tombstone_type,
            artifact_id,
            link_id,
            source_kind,
            source_id,
            reason,
        ],
    )
    .map_err(|e| e.to_string())?;
    Ok(tx.last_insert_rowid().to_string())
}

fn resolve_link_remove_id(
    tx: &Transaction<'_>,
    request: &ArtifactLinkRemoveWire,
) -> Result<Option<String>, String> {
    if let Some(id) = request.id.as_ref().filter(|id| !id.trim().is_empty()) {
        return Ok(Some(id.clone()));
    }
    let (Some(link_type), Some(source_id), Some(target_id)) = (
        request.link_type.as_ref(),
        request.source_id.as_ref(),
        request.target_id.as_ref(),
    ) else {
        return Err(
            "artifact link remove requires id or link_type/source_id/target_id"
                .to_string(),
        );
    };
    let deterministic_id =
        deterministic_artifact_link_id(link_type, source_id, target_id);
    let found: Option<String> = tx
        .query_row(
            r#"
            SELECT id
            FROM artifact_links
            WHERE id = ?1 OR (link_type = ?2 AND source_id = ?3 AND target_id = ?4)
            ORDER BY id
            LIMIT 1
            "#,
            params![deterministic_id, link_type, source_id, target_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(|e| e.to_string())?;
    Ok(found)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use serde_json::json;
    use tempfile::tempdir;

    use crate::artifact::wire::{
        ARTIFACT_KIND_FILE, ARTIFACT_KIND_PROJECT, ARTIFACT_LINK_CREATED,
    };

    use super::*;

    fn collect_names(
        conn: &Connection,
        kind: &str,
    ) -> Result<BTreeSet<String>, String> {
        let mut stmt = conn
            .prepare(
                "SELECT name FROM sqlite_master WHERE type = ?1 ORDER BY name",
            )
            .map_err(|e| e.to_string())?;
        let rows = stmt
            .query_map([kind], |row| row.get::<_, String>(0))
            .map_err(|e| e.to_string())?;
        rows.collect::<Result<BTreeSet<_>, _>>()
            .map_err(|e| e.to_string())
    }

    fn manual_node(id: &str) -> ArtifactNodeWire {
        ArtifactNodeWire {
            id: id.to_string(),
            kind: ARTIFACT_KIND_FILE.to_string(),
            display_title: id.to_string(),
            subtitle: None,
            provenance: ARTIFACT_PROVENANCE_MANUAL.to_string(),
            source_kind: None,
            source_id: None,
            source_version: None,
            search_text: id.to_string(),
            metadata: serde_json::Map::new(),
            created_at: None,
            updated_at: None,
        }
    }

    fn derived_node(id: &str, source_id: &str) -> ArtifactNodeWire {
        ArtifactNodeWire {
            id: id.to_string(),
            kind: ARTIFACT_KIND_PROJECT.to_string(),
            display_title: id.to_string(),
            subtitle: None,
            provenance: ARTIFACT_PROVENANCE_DERIVED.to_string(),
            source_kind: Some("project".to_string()),
            source_id: Some(source_id.to_string()),
            source_version: Some("v1".to_string()),
            search_text: id.to_string(),
            metadata: serde_json::Map::new(),
            created_at: None,
            updated_at: None,
        }
    }

    fn node_upsert(node: ArtifactNodeWire) -> ArtifactNodeUpsertWire {
        ArtifactNodeUpsertWire {
            schema_version: ARTIFACT_WIRE_SCHEMA_VERSION,
            node,
            replace_payloads: false,
        }
    }

    fn manual_link(
        link_type: &str,
        source_id: &str,
        target_id: &str,
    ) -> ArtifactLinkWire {
        ArtifactLinkWire {
            id: deterministic_artifact_link_id(link_type, source_id, target_id),
            link_type: link_type.to_string(),
            source_id: source_id.to_string(),
            target_id: target_id.to_string(),
            provenance: ARTIFACT_PROVENANCE_MANUAL.to_string(),
            source_kind: None,
            source_id_hint: None,
            source_version: None,
            metadata: serde_json::Map::new(),
            created_at: None,
            updated_at: None,
        }
    }

    fn derived_link(
        link_type: &str,
        source_id: &str,
        target_id: &str,
        source_hint: &str,
    ) -> ArtifactLinkWire {
        ArtifactLinkWire {
            id: deterministic_artifact_link_id(link_type, source_id, target_id),
            link_type: link_type.to_string(),
            source_id: source_id.to_string(),
            target_id: target_id.to_string(),
            provenance: ARTIFACT_PROVENANCE_DERIVED.to_string(),
            source_kind: Some("scan".to_string()),
            source_id_hint: Some(source_hint.to_string()),
            source_version: Some("v1".to_string()),
            metadata: serde_json::Map::new(),
            created_at: None,
            updated_at: None,
        }
    }

    fn link_upsert(link: ArtifactLinkWire) -> ArtifactLinkUpsertWire {
        ArtifactLinkUpsertWire {
            schema_version: ARTIFACT_WIRE_SCHEMA_VERSION,
            link,
        }
    }

    #[test]
    fn schema_init_creates_tables_indexes_meta_wal_and_root() {
        let tmp = tempdir().unwrap();
        let db = tmp.path().join("artifacts.sqlite");
        let store = open_artifact_store(&db).unwrap();
        let conn = store.connection();

        let tables = collect_names(conn, "table").unwrap();
        for table in [
            "artifacts",
            "artifact_links",
            "artifact_payloads",
            "source_watermarks",
            "manual_tombstones",
            "meta",
        ] {
            assert!(tables.contains(table), "missing table {table}");
        }

        let indexes = collect_names(conn, "index").unwrap();
        for index in [
            "idx_artifacts_id",
            "idx_artifacts_kind",
            "idx_artifacts_source",
            "idx_artifacts_text",
            "idx_artifact_links_type",
            "idx_artifact_links_source",
            "idx_artifact_links_target",
            "idx_artifact_links_parent_children",
            "idx_artifact_links_source_marker",
            "idx_artifact_payloads_source",
            "idx_source_watermarks_updated",
            "idx_manual_tombstones_node",
            "idx_manual_tombstones_link",
        ] {
            assert!(indexes.contains(index), "missing index {index}");
        }

        let schema_version: String = conn
            .query_row(
                "SELECT value FROM meta WHERE key = 'schema_version'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(schema_version, ARTIFACT_WIRE_SCHEMA_VERSION.to_string());

        let journal_mode: String = conn
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .unwrap();
        assert_eq!(journal_mode.to_lowercase(), "wal");
        let foreign_keys: i64 = conn
            .query_row("PRAGMA foreign_keys", [], |row| row.get(0))
            .unwrap();
        assert_eq!(foreign_keys, 1);

        let (kind, display_title, node_json): (String, String, String) = conn
            .query_row(
                "SELECT kind, display_title, node_json FROM artifacts WHERE id = ?1",
                [ARTIFACT_ROOT_ID],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(kind, "root");
        assert_eq!(display_title, ARTIFACT_ROOT_ID);
        let root: ArtifactNodeWire = serde_json::from_str(&node_json).unwrap();
        assert_eq!(root, ArtifactNodeWire::root());
        assert_eq!(store.index_path(), db.as_path());
    }

    #[test]
    fn reopening_existing_db_is_idempotent() {
        let tmp = tempdir().unwrap();
        let db = tmp.path().join("artifacts.sqlite");
        {
            let store = open_artifact_store(&db).unwrap();
            let root_count: i64 = store
                .connection()
                .query_row(
                    "SELECT COUNT(*) FROM artifacts WHERE id = ?1",
                    [ARTIFACT_ROOT_ID],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(root_count, 1);
        }

        let store = open_artifact_store(&db).unwrap();
        let root_count: i64 = store
            .connection()
            .query_row(
                "SELECT COUNT(*) FROM artifacts WHERE id = ?1",
                [ARTIFACT_ROOT_ID],
                |row| row.get(0),
            )
            .unwrap();
        let artifact_count: i64 = store
            .connection()
            .query_row("SELECT COUNT(*) FROM artifacts", [], |row| row.get(0))
            .unwrap();
        assert_eq!(root_count, 1);
        assert_eq!(artifact_count, 1);
    }

    #[test]
    fn duplicate_node_and_link_upserts_are_idempotent() {
        let tmp = tempdir().unwrap();
        let db = tmp.path().join("artifacts.sqlite");
        let mut store = open_artifact_store(&db).unwrap();

        let first = upsert_artifact_node(
            &mut store,
            node_upsert(manual_node("/tmp/a")),
        )
        .unwrap();
        assert_eq!(first.nodes_added, 1);
        let second = upsert_artifact_node(
            &mut store,
            node_upsert(manual_node("/tmp/a")),
        )
        .unwrap();
        assert_eq!(second.nodes_added, 0);
        assert_eq!(second.nodes_updated, 0);

        let link =
            manual_link(ARTIFACT_LINK_PARENT, "/tmp/a", ARTIFACT_ROOT_ID);
        let first = upsert_artifact_link(&mut store, link_upsert(link.clone()))
            .unwrap();
        assert_eq!(first.links_added, 1);
        let second =
            upsert_artifact_link(&mut store, link_upsert(link.clone()))
                .unwrap();
        assert_eq!(second.links_added, 0);
        assert_eq!(second.links_updated, 0);

        let link_count: i64 = store
            .connection()
            .query_row("SELECT COUNT(*) FROM artifact_links", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(link_count, 1);
    }

    #[test]
    fn manual_node_and_link_removal_deletes_rows() {
        let tmp = tempdir().unwrap();
        let db = tmp.path().join("artifacts.sqlite");
        let mut store = open_artifact_store(&db).unwrap();
        upsert_artifact_node(&mut store, node_upsert(manual_node("/tmp/a")))
            .unwrap();
        upsert_artifact_node(&mut store, node_upsert(manual_node("/tmp/b")))
            .unwrap();
        let link = manual_link(ARTIFACT_LINK_CREATED, "/tmp/a", "/tmp/b");
        upsert_artifact_link(&mut store, link_upsert(link.clone())).unwrap();

        let removed = remove_artifact_link(
            &mut store,
            ArtifactLinkRemoveWire {
                schema_version: ARTIFACT_WIRE_SCHEMA_VERSION,
                id: Some(link.id.clone()),
                link_type: None,
                source_id: None,
                target_id: None,
                provenance: None,
                source_kind: None,
                source_id_hint: None,
                reason: None,
            },
        )
        .unwrap();
        assert_eq!(removed.links_removed, 1);
        let removed = remove_artifact_node(
            &mut store,
            ArtifactNodeRemoveWire {
                schema_version: ARTIFACT_WIRE_SCHEMA_VERSION,
                id: "/tmp/b".to_string(),
                provenance: None,
                source_kind: None,
                source_id: None,
                reason: None,
            },
        )
        .unwrap();
        assert_eq!(removed.nodes_removed, 1);

        let node_exists: bool = store
            .connection()
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM artifacts WHERE id = '/tmp/b')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let link_exists: bool = store
            .connection()
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM artifact_links WHERE id = ?1)",
                [&link.id],
                |row| row.get(0),
            )
            .unwrap();
        assert!(!node_exists);
        assert!(!link_exists);
    }

    #[test]
    fn derived_removal_tombstones_without_deleting_payload() {
        let tmp = tempdir().unwrap();
        let db = tmp.path().join("artifacts.sqlite");
        let mut store = open_artifact_store(&db).unwrap();
        upsert_artifact_node(
            &mut store,
            node_upsert(derived_node("project-a", "project-a.gp")),
        )
        .unwrap();
        upsert_artifact_payload(
            &mut store,
            ArtifactPayloadWire {
                artifact_id: "project-a".to_string(),
                payload_type: "summary".to_string(),
                provenance: ARTIFACT_PROVENANCE_DERIVED.to_string(),
                source_kind: Some("project".to_string()),
                source_id: Some("project-a.gp".to_string()),
                source_version: Some("v1".to_string()),
                payload: json!({"title": "Project A"}),
                updated_at: None,
            },
        )
        .unwrap();

        let removed = remove_artifact_node(
            &mut store,
            ArtifactNodeRemoveWire {
                schema_version: ARTIFACT_WIRE_SCHEMA_VERSION,
                id: "project-a".to_string(),
                provenance: None,
                source_kind: None,
                source_id: None,
                reason: Some("hidden".to_string()),
            },
        )
        .unwrap();
        assert_eq!(removed.nodes_removed, 0);
        assert_eq!(removed.tombstones_added, 1);

        let node_exists: bool = store
            .connection()
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM artifacts WHERE id = 'project-a')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let payload_count: i64 = store
            .connection()
            .query_row(
                "SELECT COUNT(*) FROM artifact_payloads WHERE artifact_id = 'project-a'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(node_exists);
        assert_eq!(payload_count, 1);

        let ignored = upsert_artifact_node(
            &mut store,
            node_upsert(derived_node("project-a", "project-a.gp")),
        )
        .unwrap();
        assert_eq!(ignored.nodes_added, 0);
        assert_eq!(ignored.nodes_updated, 0);
    }

    #[test]
    fn manual_readd_clears_derived_node_tombstone() {
        let tmp = tempdir().unwrap();
        let db = tmp.path().join("artifacts.sqlite");
        let mut store = open_artifact_store(&db).unwrap();
        upsert_artifact_node(
            &mut store,
            node_upsert(derived_node("project-a", "project-a.gp")),
        )
        .unwrap();
        remove_artifact_node(
            &mut store,
            ArtifactNodeRemoveWire {
                schema_version: ARTIFACT_WIRE_SCHEMA_VERSION,
                id: "project-a".to_string(),
                provenance: None,
                source_kind: None,
                source_id: None,
                reason: None,
            },
        )
        .unwrap();

        let result = upsert_artifact_node(
            &mut store,
            node_upsert(manual_node("project-a")),
        )
        .unwrap();
        assert_eq!(result.nodes_updated, 1);
        let tombstone_count: i64 = store
            .connection()
            .query_row("SELECT COUNT(*) FROM manual_tombstones", [], |row| {
                row.get(0)
            })
            .unwrap();
        let provenance: String = store
            .connection()
            .query_row(
                "SELECT provenance FROM artifacts WHERE id = 'project-a'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(tombstone_count, 0);
        assert_eq!(provenance, ARTIFACT_PROVENANCE_MANUAL);
    }

    #[test]
    fn derived_link_tombstone_blocks_source_reupsert_until_manual_readd() {
        let tmp = tempdir().unwrap();
        let db = tmp.path().join("artifacts.sqlite");
        let mut store = open_artifact_store(&db).unwrap();
        upsert_artifact_node(&mut store, node_upsert(manual_node("/tmp/a")))
            .unwrap();
        upsert_artifact_node(&mut store, node_upsert(manual_node("/tmp/b")))
            .unwrap();
        let derived =
            derived_link(ARTIFACT_LINK_CREATED, "/tmp/a", "/tmp/b", "scan-1");
        upsert_artifact_link(&mut store, link_upsert(derived.clone())).unwrap();

        let removed = remove_artifact_link(
            &mut store,
            ArtifactLinkRemoveWire {
                schema_version: ARTIFACT_WIRE_SCHEMA_VERSION,
                id: Some(derived.id.clone()),
                link_type: None,
                source_id: None,
                target_id: None,
                provenance: None,
                source_kind: None,
                source_id_hint: None,
                reason: None,
            },
        )
        .unwrap();
        assert_eq!(removed.tombstones_added, 1);

        let ignored =
            upsert_artifact_link(&mut store, link_upsert(derived.clone()))
                .unwrap();
        assert_eq!(ignored.links_added, 0);
        assert_eq!(ignored.links_updated, 0);

        let manual = manual_link(ARTIFACT_LINK_CREATED, "/tmp/a", "/tmp/b");
        let result =
            upsert_artifact_link(&mut store, link_upsert(manual)).unwrap();
        assert_eq!(result.links_updated, 1);
        let tombstone_count: i64 = store
            .connection()
            .query_row("SELECT COUNT(*) FROM manual_tombstones", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(tombstone_count, 0);
    }

    #[test]
    fn root_removal_is_rejected() {
        let tmp = tempdir().unwrap();
        let db = tmp.path().join("artifacts.sqlite");
        let mut store = open_artifact_store(&db).unwrap();
        let err = remove_artifact_node(
            &mut store,
            ArtifactNodeRemoveWire {
                schema_version: ARTIFACT_WIRE_SCHEMA_VERSION,
                id: ARTIFACT_ROOT_ID.to_string(),
                provenance: None,
                source_kind: None,
                source_id: None,
                reason: None,
            },
        )
        .unwrap_err();
        assert!(err.contains("root"));
    }

    #[test]
    fn parent_links_are_child_to_parent() {
        let tmp = tempdir().unwrap();
        let db = tmp.path().join("artifacts.sqlite");
        let mut store = open_artifact_store(&db).unwrap();
        upsert_artifact_node(
            &mut store,
            node_upsert(manual_node("/tmp/child")),
        )
        .unwrap();
        let link =
            manual_link(ARTIFACT_LINK_PARENT, "/tmp/child", ARTIFACT_ROOT_ID);
        upsert_artifact_link(&mut store, link_upsert(link)).unwrap();

        let (source_id, target_id): (String, String) = store
            .connection()
            .query_row(
                "SELECT source_id, target_id FROM artifact_links WHERE link_type = ?1",
                [ARTIFACT_LINK_PARENT],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(source_id, "/tmp/child");
        assert_eq!(target_id, ARTIFACT_ROOT_ID);
    }

    #[test]
    fn invalid_endpoint_rolls_back_link_transaction() {
        let tmp = tempdir().unwrap();
        let db = tmp.path().join("artifacts.sqlite");
        let mut store = open_artifact_store(&db).unwrap();
        upsert_artifact_node(&mut store, node_upsert(manual_node("/tmp/a")))
            .unwrap();
        let link = manual_link(ARTIFACT_LINK_CREATED, "/tmp/a", "/tmp/missing");

        let err =
            upsert_artifact_link(&mut store, link_upsert(link)).unwrap_err();
        assert!(err.contains("does not exist"));
        let link_count: i64 = store
            .connection()
            .query_row("SELECT COUNT(*) FROM artifact_links", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(link_count, 0);
    }
}
