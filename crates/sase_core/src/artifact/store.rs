//! SQLite store initialization for the unified artifact graph.

use std::fs;
use std::path::{Path, PathBuf};

use rusqlite::{params, Connection};

use super::wire::{ArtifactNodeWire, ARTIFACT_WIRE_SCHEMA_VERSION};

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

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use tempfile::tempdir;

    use crate::artifact::wire::ARTIFACT_ROOT_ID;

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
}
