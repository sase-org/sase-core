use rusqlite::{params, Connection, Transaction};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::project_spec::is_archive_project_spec;
use crate::wire::ChangeSpecWire;

use super::{
    ChangeSpecEventKind, ProjectionError, ProjectionEvent, ProjectionEventType,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangeSpecSnapshotUpsertPayload {
    pub changespecs: Vec<ChangeSpecWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangeSpecMutationPayload {
    pub changespec: ChangeSpecWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mutation: Option<JsonValue>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangeSpecStatusTransitionPayload {
    pub name: String,
    pub status: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangeSpecArchiveMovedPayload {
    pub changespec: ChangeSpecWire,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangeSpecSectionUpdatedPayload {
    pub name: String,
    pub section_kind: String,
    #[serde(default)]
    pub entries: Vec<JsonValue>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangeSpecTombstonePayload {
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangeSpecProjectionSnapshot {
    pub changespecs: Vec<ChangeSpecProjectionRow>,
    pub edges: Vec<ChangeSpecEdgeRow>,
    pub sections: Vec<ChangeSpecSectionRow>,
    pub search: Vec<ChangeSpecSearchRow>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangeSpecProjectionRow {
    pub name: String,
    pub project_basename: String,
    pub file_path: String,
    pub source_start_line: u32,
    pub source_end_line: u32,
    pub status: String,
    pub parent: Option<String>,
    pub cl_or_pr: Option<String>,
    pub bug: Option<String>,
    pub description: String,
    pub archived: bool,
    pub wire: ChangeSpecWire,
    pub updated_seq: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangeSpecEdgeRow {
    pub changespec_name: String,
    pub edge_kind: String,
    pub target: String,
    pub updated_seq: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangeSpecSectionRow {
    pub changespec_name: String,
    pub section_kind: String,
    pub ordinal: i64,
    pub entry: JsonValue,
    pub updated_seq: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangeSpecSearchRow {
    pub name: String,
    pub summary: String,
}

pub fn apply_changespec_event(
    tx: &Transaction<'_>,
    event: &ProjectionEvent,
) -> Result<(), ProjectionError> {
    let ProjectionEventType::ChangeSpec(kind) = &event.event_type else {
        return Ok(());
    };
    match kind {
        ChangeSpecEventKind::ParsedSnapshotUpsert => {
            let payload: ChangeSpecSnapshotUpsertPayload =
                serde_json::from_value(event.payload.clone())?;
            for changespec in payload.changespecs {
                upsert_changespec(
                    tx,
                    event.seq,
                    &event.timestamp,
                    &changespec,
                )?;
            }
        }
        ChangeSpecEventKind::MutationApplied => {
            let payload: ChangeSpecMutationPayload =
                serde_json::from_value(event.payload.clone())?;
            upsert_changespec(
                tx,
                event.seq,
                &event.timestamp,
                &payload.changespec,
            )?;
        }
        ChangeSpecEventKind::StatusTransition => {
            let payload: ChangeSpecStatusTransitionPayload =
                serde_json::from_value(event.payload.clone())?;
            tx.execute(
                "UPDATE changespecs
                 SET status = ?1, updated_seq = ?2, updated_at = ?3
                 WHERE name = ?4",
                params![
                    payload.status,
                    event.seq,
                    event.timestamp,
                    payload.name
                ],
            )?;
        }
        ChangeSpecEventKind::ArchiveMoved => {
            let payload: ChangeSpecArchiveMovedPayload =
                serde_json::from_value(event.payload.clone())?;
            upsert_changespec(
                tx,
                event.seq,
                &event.timestamp,
                &payload.changespec,
            )?;
        }
        ChangeSpecEventKind::SectionUpdated => {
            let payload: ChangeSpecSectionUpdatedPayload =
                serde_json::from_value(event.payload.clone())?;
            replace_section(
                tx,
                event.seq,
                &payload.name,
                &payload.section_kind,
                &payload.entries,
            )?;
        }
        ChangeSpecEventKind::Tombstone => {
            let payload: ChangeSpecTombstonePayload =
                serde_json::from_value(event.payload.clone())?;
            delete_changespec(tx, &payload.name)?;
        }
    }
    Ok(())
}

pub fn changespec_projection_snapshot(
    conn: &Connection,
) -> Result<ChangeSpecProjectionSnapshot, ProjectionError> {
    Ok(ChangeSpecProjectionSnapshot {
        changespecs: changespec_rows(conn)?,
        edges: edge_rows(conn)?,
        sections: section_rows(conn)?,
        search: search_rows(conn)?,
    })
}

pub fn changespec_wires(
    conn: &Connection,
) -> Result<Vec<ChangeSpecWire>, ProjectionError> {
    let mut stmt =
        conn.prepare("SELECT wire_json FROM changespecs ORDER BY name")?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
    let mut wires = Vec::new();
    for row in rows {
        wires.push(serde_json::from_str(&row?)?);
    }
    Ok(wires)
}

fn upsert_changespec(
    tx: &Transaction<'_>,
    seq: i64,
    timestamp: &str,
    changespec: &ChangeSpecWire,
) -> Result<(), ProjectionError> {
    let wire_json = serde_json::to_string(changespec)?;
    let archived = is_archive_project_spec(&changespec.file_path);
    tx.execute(
        "INSERT INTO changespecs(
             name,
             project_basename,
             file_path,
             source_start_line,
             source_end_line,
             status,
             parent,
             cl_or_pr,
             bug,
             description,
             archived,
             wire_json,
             updated_seq,
             updated_at
         )
         VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)
         ON CONFLICT(name) DO UPDATE SET
             project_basename = excluded.project_basename,
             file_path = excluded.file_path,
             source_start_line = excluded.source_start_line,
             source_end_line = excluded.source_end_line,
             status = excluded.status,
             parent = excluded.parent,
             cl_or_pr = excluded.cl_or_pr,
             bug = excluded.bug,
             description = excluded.description,
             archived = excluded.archived,
             wire_json = excluded.wire_json,
             updated_seq = excluded.updated_seq,
             updated_at = excluded.updated_at",
        params![
            changespec.name,
            changespec.project_basename,
            changespec.file_path,
            changespec.source_span.start_line,
            changespec.source_span.end_line,
            changespec.status,
            changespec.parent,
            changespec.cl_or_pr,
            changespec.bug,
            changespec.description,
            if archived { 1 } else { 0 },
            wire_json,
            seq,
            timestamp,
        ],
    )?;
    replace_edges(tx, seq, changespec)?;
    replace_sections(tx, seq, changespec)?;
    replace_search_row(tx, changespec)?;
    Ok(())
}

fn delete_changespec(
    tx: &Transaction<'_>,
    name: &str,
) -> Result<(), ProjectionError> {
    tx.execute("DELETE FROM changespec_search_fts WHERE name = ?1", [name])?;
    tx.execute("DELETE FROM changespecs WHERE name = ?1", [name])?;
    Ok(())
}

fn replace_edges(
    tx: &Transaction<'_>,
    seq: i64,
    changespec: &ChangeSpecWire,
) -> Result<(), ProjectionError> {
    tx.execute(
        "DELETE FROM changespec_edges WHERE changespec_name = ?1",
        [&changespec.name],
    )?;
    if let Some(parent) = &changespec.parent {
        insert_edge(tx, seq, &changespec.name, "parent", parent)?;
    }
    if let Some(cl_or_pr) = &changespec.cl_or_pr {
        insert_edge(tx, seq, &changespec.name, "cl_or_pr", cl_or_pr)?;
    }
    if let Some(bug) = &changespec.bug {
        insert_edge(tx, seq, &changespec.name, "bug", bug)?;
    }
    for commit in &changespec.commits {
        if let Some(chat) = &commit.chat {
            insert_edge(tx, seq, &changespec.name, "commit_chat", chat)?;
        }
        if let Some(diff) = &commit.diff {
            insert_edge(tx, seq, &changespec.name, "commit_diff", diff)?;
        }
        if let Some(plan) = &commit.plan {
            insert_edge(tx, seq, &changespec.name, "commit_plan", plan)?;
        }
        if let Some(proposal_letter) = &commit.proposal_letter {
            insert_edge(
                tx,
                seq,
                &changespec.name,
                "commit_proposal_letter",
                proposal_letter,
            )?;
        }
    }
    for delta in &changespec.deltas {
        insert_edge(tx, seq, &changespec.name, "delta_path", &delta.path)?;
    }
    Ok(())
}

fn insert_edge(
    tx: &Transaction<'_>,
    seq: i64,
    name: &str,
    edge_kind: &str,
    target: &str,
) -> Result<(), ProjectionError> {
    tx.execute(
        "INSERT OR REPLACE INTO changespec_edges(
             changespec_name, edge_kind, target, updated_seq
         )
         VALUES(?1, ?2, ?3, ?4)",
        params![name, edge_kind, target, seq],
    )?;
    Ok(())
}

fn replace_sections(
    tx: &Transaction<'_>,
    seq: i64,
    changespec: &ChangeSpecWire,
) -> Result<(), ProjectionError> {
    tx.execute(
        "DELETE FROM changespec_sections WHERE changespec_name = ?1",
        [&changespec.name],
    )?;
    insert_section_values(
        tx,
        seq,
        &changespec.name,
        "commits",
        &changespec.commits,
    )?;
    insert_section_values(
        tx,
        seq,
        &changespec.name,
        "hooks",
        &changespec.hooks,
    )?;
    insert_section_values(
        tx,
        seq,
        &changespec.name,
        "comments",
        &changespec.comments,
    )?;
    insert_section_values(
        tx,
        seq,
        &changespec.name,
        "mentors",
        &changespec.mentors,
    )?;
    insert_section_values(
        tx,
        seq,
        &changespec.name,
        "timestamps",
        &changespec.timestamps,
    )?;
    insert_section_values(
        tx,
        seq,
        &changespec.name,
        "deltas",
        &changespec.deltas,
    )?;
    Ok(())
}

fn replace_section(
    tx: &Transaction<'_>,
    seq: i64,
    name: &str,
    section_kind: &str,
    entries: &[JsonValue],
) -> Result<(), ProjectionError> {
    tx.execute(
        "DELETE FROM changespec_sections
         WHERE changespec_name = ?1 AND section_kind = ?2",
        params![name, section_kind],
    )?;
    for (ordinal, entry) in entries.iter().enumerate() {
        tx.execute(
            "INSERT INTO changespec_sections(
                 changespec_name, section_kind, ordinal, entry_json, updated_seq
             )
             VALUES(?1, ?2, ?3, ?4, ?5)",
            params![
                name,
                section_kind,
                ordinal as i64,
                serde_json::to_string(entry)?,
                seq
            ],
        )?;
    }
    Ok(())
}

fn insert_section_values<T: Serialize>(
    tx: &Transaction<'_>,
    seq: i64,
    name: &str,
    section_kind: &str,
    entries: &[T],
) -> Result<(), ProjectionError> {
    for (ordinal, entry) in entries.iter().enumerate() {
        tx.execute(
            "INSERT INTO changespec_sections(
                 changespec_name, section_kind, ordinal, entry_json, updated_seq
             )
             VALUES(?1, ?2, ?3, ?4, ?5)",
            params![
                name,
                section_kind,
                ordinal as i64,
                serde_json::to_string(entry)?,
                seq
            ],
        )?;
    }
    Ok(())
}

fn replace_search_row(
    tx: &Transaction<'_>,
    changespec: &ChangeSpecWire,
) -> Result<(), ProjectionError> {
    tx.execute(
        "DELETE FROM changespec_search_fts WHERE name = ?1",
        [&changespec.name],
    )?;
    tx.execute(
        "INSERT INTO changespec_search_fts(name, summary) VALUES(?1, ?2)",
        params![changespec.name, changespec_summary(changespec)],
    )?;
    Ok(())
}

fn changespec_summary(changespec: &ChangeSpecWire) -> String {
    let mut parts = vec![
        changespec.name.as_str(),
        changespec.status.as_str(),
        changespec.description.as_str(),
    ];
    if let Some(parent) = &changespec.parent {
        parts.push(parent.as_str());
    }
    if let Some(cl_or_pr) = &changespec.cl_or_pr {
        parts.push(cl_or_pr.as_str());
    }
    if let Some(bug) = &changespec.bug {
        parts.push(bug.as_str());
    }
    let summary = parts.join("\n");
    summary.chars().take(4096).collect()
}

fn changespec_rows(
    conn: &Connection,
) -> Result<Vec<ChangeSpecProjectionRow>, ProjectionError> {
    let mut stmt = conn.prepare(
        "SELECT
             name,
             project_basename,
             file_path,
             source_start_line,
             source_end_line,
             status,
             parent,
             cl_or_pr,
             bug,
             description,
             archived,
             wire_json,
             updated_seq
         FROM changespecs
         ORDER BY name",
    )?;
    let rows = stmt.query_map([], |row| {
        let wire_json: String = row.get(11)?;
        let wire = serde_json::from_str(&wire_json).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(
                wire_json.len(),
                rusqlite::types::Type::Text,
                Box::new(e),
            )
        })?;
        Ok(ChangeSpecProjectionRow {
            name: row.get(0)?,
            project_basename: row.get(1)?,
            file_path: row.get(2)?,
            source_start_line: row.get(3)?,
            source_end_line: row.get(4)?,
            status: row.get(5)?,
            parent: row.get(6)?,
            cl_or_pr: row.get(7)?,
            bug: row.get(8)?,
            description: row.get(9)?,
            archived: row.get::<_, i64>(10)? != 0,
            wire,
            updated_seq: row.get(12)?,
        })
    })?;
    collect_rows(rows)
}

fn edge_rows(
    conn: &Connection,
) -> Result<Vec<ChangeSpecEdgeRow>, ProjectionError> {
    let mut stmt = conn.prepare(
        "SELECT changespec_name, edge_kind, target, updated_seq
         FROM changespec_edges
         ORDER BY changespec_name, edge_kind, target",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok(ChangeSpecEdgeRow {
            changespec_name: row.get(0)?,
            edge_kind: row.get(1)?,
            target: row.get(2)?,
            updated_seq: row.get(3)?,
        })
    })?;
    collect_rows(rows)
}

fn section_rows(
    conn: &Connection,
) -> Result<Vec<ChangeSpecSectionRow>, ProjectionError> {
    let mut stmt = conn.prepare(
        "SELECT changespec_name, section_kind, ordinal, entry_json, updated_seq
         FROM changespec_sections
         ORDER BY changespec_name, section_kind, ordinal",
    )?;
    let rows = stmt.query_map([], |row| {
        let entry_json: String = row.get(3)?;
        let entry = serde_json::from_str(&entry_json).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(
                entry_json.len(),
                rusqlite::types::Type::Text,
                Box::new(e),
            )
        })?;
        Ok(ChangeSpecSectionRow {
            changespec_name: row.get(0)?,
            section_kind: row.get(1)?,
            ordinal: row.get(2)?,
            entry,
            updated_seq: row.get(4)?,
        })
    })?;
    collect_rows(rows)
}

fn search_rows(
    conn: &Connection,
) -> Result<Vec<ChangeSpecSearchRow>, ProjectionError> {
    let mut stmt = conn.prepare(
        "SELECT name, summary FROM changespec_search_fts ORDER BY name",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok(ChangeSpecSearchRow {
            name: row.get(0)?,
            summary: row.get(1)?,
        })
    })?;
    collect_rows(rows)
}

fn collect_rows<T>(
    rows: rusqlite::MappedRows<
        '_,
        impl FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
    >,
) -> Result<Vec<T>, ProjectionError> {
    let mut out = Vec::new();
    for row in rows {
        out.push(row?);
    }
    Ok(out)
}
