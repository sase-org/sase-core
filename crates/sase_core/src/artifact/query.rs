//! Deterministic read APIs for the unified artifact graph.

use std::collections::{BTreeSet, HashSet};

use rusqlite::{params, Connection, OptionalExtension};

use super::store::ArtifactStore;
use super::wire::{
    ArtifactDetailWire, ArtifactDoctorIssueWire, ArtifactDoctorOptionsWire,
    ArtifactDoctorWire, ArtifactLinkWire, ArtifactNodeWire,
    ArtifactPayloadWire, ArtifactQueryWire, ARTIFACT_LINK_PARENT,
    ARTIFACT_PROVENANCE_DERIVED, ARTIFACT_ROOT_ID, ARTIFACT_TOMBSTONE_LINK,
    ARTIFACT_TOMBSTONE_NODE, ARTIFACT_WIRE_SCHEMA_VERSION,
};

const STALE_DERIVED_REASON: &str = "stale_derived";

pub fn artifact_show(
    store: &ArtifactStore,
    artifact_id: &str,
) -> Result<ArtifactDetailWire, String> {
    artifact_detail(store, artifact_id)
}

pub fn artifact_detail(
    store: &ArtifactStore,
    artifact_id: &str,
) -> Result<ArtifactDetailWire, String> {
    let conn = store.connection();
    let node = load_visible_node(conn, artifact_id)?;
    let payloads = load_payloads(conn, artifact_id)?;
    let outbound_links = artifact_outbound_neighbors(store, artifact_id)?;
    let inbound_links = artifact_inbound_neighbors(store, artifact_id)?;
    let children = artifact_children(store, artifact_id)?;
    let path_to_root = artifact_path_to_root(store, artifact_id)?;
    let diagnostics = path_diagnostics(conn, artifact_id)?;

    Ok(ArtifactDetailWire {
        schema_version: ARTIFACT_WIRE_SCHEMA_VERSION,
        node,
        payloads,
        outbound_links,
        inbound_links,
        children,
        path_to_root,
        diagnostics,
    })
}

pub fn artifact_list(
    store: &ArtifactStore,
    query: ArtifactQueryWire,
) -> Result<Vec<ArtifactNodeWire>, String> {
    validate_schema_version(query.schema_version)?;
    let conn = store.connection();
    let mut nodes = load_all_nodes(conn, query.include_tombstoned)?;

    if let Some(text) = query
        .text
        .as_ref()
        .map(|text| text.trim())
        .filter(|text| !text.is_empty())
    {
        let needle = text.to_lowercase();
        nodes.retain(|node| {
            node.id.to_lowercase().contains(&needle)
                || node.display_title.to_lowercase().contains(&needle)
                || node
                    .subtitle
                    .as_deref()
                    .unwrap_or_default()
                    .to_lowercase()
                    .contains(&needle)
                || node.search_text.to_lowercase().contains(&needle)
        });
    }
    if !query.kinds.is_empty() {
        let kinds: BTreeSet<_> =
            query.kinds.iter().map(String::as_str).collect();
        nodes.retain(|node| kinds.contains(node.kind.as_str()));
    }
    if let Some(provenance) = query
        .provenance
        .as_ref()
        .filter(|value| !value.trim().is_empty())
    {
        nodes.retain(|node| node.provenance == *provenance);
    }
    if !query.source_kinds.is_empty() {
        let source_kinds: BTreeSet<_> =
            query.source_kinds.iter().map(String::as_str).collect();
        nodes.retain(|node| {
            node.source_kind
                .as_deref()
                .is_some_and(|source_kind| source_kinds.contains(source_kind))
        });
    }
    if !query.source_ids.is_empty() {
        let source_ids: BTreeSet<_> =
            query.source_ids.iter().map(String::as_str).collect();
        nodes.retain(|node| {
            node.source_id
                .as_deref()
                .is_some_and(|source_id| source_ids.contains(source_id))
        });
    }
    if !query.link_types.is_empty() {
        let linked_ids = linked_artifact_ids(
            conn,
            &query.link_types,
            query.include_tombstoned,
        )?;
        nodes.retain(|node| linked_ids.contains(&node.id));
    }
    if let Some(root_id) = query
        .root_id
        .as_ref()
        .filter(|value| !value.trim().is_empty())
    {
        nodes.retain(|node| {
            node.id == *root_id
                || path_to_root_ids(conn, &node.id)
                    .map(|path| path.iter().any(|id| id == root_id))
                    .unwrap_or(false)
        });
    }

    sort_nodes(&mut nodes);
    let offset = query.offset.unwrap_or(0) as usize;
    let limit = query.limit.map(|limit| limit as usize);
    let iter = nodes.into_iter().skip(offset);
    Ok(match limit {
        Some(limit) => iter.take(limit).collect(),
        None => iter.collect(),
    })
}

pub fn artifact_search(
    store: &ArtifactStore,
    query: ArtifactQueryWire,
) -> Result<Vec<ArtifactNodeWire>, String> {
    artifact_list(store, query)
}

pub fn artifact_children(
    store: &ArtifactStore,
    artifact_id: &str,
) -> Result<Vec<ArtifactNodeWire>, String> {
    let mut children = load_linked_nodes(
        store.connection(),
        r#"
        SELECT a.node_json
        FROM artifact_links l
        JOIN artifacts a ON a.id = l.source_id
        WHERE l.link_type = ?1
          AND l.target_id = ?2
          AND NOT EXISTS (
              SELECT 1 FROM manual_tombstones t
              WHERE (t.tombstone_type = ?3 AND t.artifact_id = a.id)
                 OR (t.tombstone_type = ?4 AND t.link_id = l.id)
          )
        "#,
        params![
            ARTIFACT_LINK_PARENT,
            artifact_id,
            ARTIFACT_TOMBSTONE_NODE,
            ARTIFACT_TOMBSTONE_LINK,
        ],
    )?;
    sort_nodes(&mut children);
    Ok(children)
}

pub fn artifact_outbound_neighbors(
    store: &ArtifactStore,
    artifact_id: &str,
) -> Result<Vec<ArtifactLinkWire>, String> {
    load_links(
        store.connection(),
        r#"
        SELECT l.link_json
        FROM artifact_links l
        WHERE l.source_id = ?1
          AND NOT EXISTS (
              SELECT 1 FROM manual_tombstones t
              WHERE t.tombstone_type = ?2 AND t.link_id = l.id
          )
        "#,
        params![artifact_id, ARTIFACT_TOMBSTONE_LINK],
    )
}

pub fn artifact_inbound_neighbors(
    store: &ArtifactStore,
    artifact_id: &str,
) -> Result<Vec<ArtifactLinkWire>, String> {
    load_links(
        store.connection(),
        r#"
        SELECT l.link_json
        FROM artifact_links l
        WHERE l.target_id = ?1
          AND NOT EXISTS (
              SELECT 1 FROM manual_tombstones t
              WHERE t.tombstone_type = ?2 AND t.link_id = l.id
          )
        "#,
        params![artifact_id, ARTIFACT_TOMBSTONE_LINK],
    )
}

pub fn artifact_path_to_root(
    store: &ArtifactStore,
    artifact_id: &str,
) -> Result<Vec<ArtifactNodeWire>, String> {
    let ids = path_to_root_ids(store.connection(), artifact_id)?;
    let mut nodes = Vec::new();
    for id in ids {
        if let Some(node) = load_visible_node(store.connection(), &id)? {
            nodes.push(node);
        }
    }
    Ok(nodes)
}

pub fn artifact_is_reachable_to_root(
    store: &ArtifactStore,
    artifact_id: &str,
) -> Result<bool, String> {
    Ok(path_to_root_ids(store.connection(), artifact_id)?
        .iter()
        .any(|id| id == ARTIFACT_ROOT_ID))
}

pub fn artifact_doctor(
    store: &ArtifactStore,
    options: ArtifactDoctorOptionsWire,
) -> Result<ArtifactDoctorWire, String> {
    validate_schema_version(options.schema_version)?;
    let conn = store.connection();
    let mut issues = Vec::new();

    if options.check_root_presence {
        let root_exists: bool = conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM artifacts WHERE id = ?1)",
                [ARTIFACT_ROOT_ID],
                |row| row.get(0),
            )
            .map_err(|e| e.to_string())?;
        if !root_exists {
            issues.push(issue(
                "missing_root",
                "error",
                Some(ARTIFACT_ROOT_ID),
                None,
                "artifact root '/' is missing",
            ));
        }
    }

    if options.check_dangling_links {
        issues.extend(dangling_link_issues(conn)?);
    }
    if options.check_duplicate_parents {
        issues.extend(duplicate_parent_issues(conn)?);
    }
    if options.check_reachability {
        issues.extend(reachability_issues(conn)?);
    }
    if options.check_tombstones {
        issues.extend(tombstone_issues(conn)?);
        issues.extend(stale_derived_marker_issues(conn)?);
        issues.extend(fallback_agent_id_issues(conn)?);
        issues.extend(unresolved_payload_issues(conn)?);
    }

    issues.sort_by(|a, b| {
        (
            a.severity.as_str(),
            a.issue_type.as_str(),
            a.artifact_id.as_deref().unwrap_or_default(),
            a.link_id.as_deref().unwrap_or_default(),
            a.message.as_str(),
        )
            .cmp(&(
                b.severity.as_str(),
                b.issue_type.as_str(),
                b.artifact_id.as_deref().unwrap_or_default(),
                b.link_id.as_deref().unwrap_or_default(),
                b.message.as_str(),
            ))
    });
    Ok(ArtifactDoctorWire {
        schema_version: ARTIFACT_WIRE_SCHEMA_VERSION,
        ok: issues.iter().all(|issue| {
            issue.severity != "error" && issue.severity != "warning"
        }),
        issues,
    })
}

fn validate_schema_version(schema_version: u32) -> Result<(), String> {
    if schema_version != ARTIFACT_WIRE_SCHEMA_VERSION {
        return Err(format!(
            "unsupported artifact schema_version {schema_version}; expected {ARTIFACT_WIRE_SCHEMA_VERSION}"
        ));
    }
    Ok(())
}

fn load_visible_node(
    conn: &Connection,
    artifact_id: &str,
) -> Result<Option<ArtifactNodeWire>, String> {
    let node_json: Option<String> = conn
        .query_row(
            r#"
            SELECT node_json
            FROM artifacts a
            WHERE a.id = ?1
              AND NOT EXISTS (
                  SELECT 1 FROM manual_tombstones t
                  WHERE t.tombstone_type = ?2 AND t.artifact_id = a.id
              )
            "#,
            params![artifact_id, ARTIFACT_TOMBSTONE_NODE],
            |row| row.get(0),
        )
        .optional()
        .map_err(|e| e.to_string())?;
    node_json
        .map(|json| serde_json::from_str(&json).map_err(|e| e.to_string()))
        .transpose()
}

fn load_all_nodes(
    conn: &Connection,
    include_tombstoned: bool,
) -> Result<Vec<ArtifactNodeWire>, String> {
    let sql = if include_tombstoned {
        "SELECT node_json FROM artifacts"
    } else {
        r#"
        SELECT node_json
        FROM artifacts a
        WHERE NOT EXISTS (
            SELECT 1 FROM manual_tombstones t
            WHERE t.tombstone_type = 'node' AND t.artifact_id = a.id
        )
        "#
    };
    let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
    let rows = stmt
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(|e| e.to_string())?;
    let mut nodes = Vec::new();
    for row in rows {
        let json = row.map_err(|e| e.to_string())?;
        nodes.push(serde_json::from_str(&json).map_err(|e| e.to_string())?);
    }
    Ok(nodes)
}

fn load_payloads(
    conn: &Connection,
    artifact_id: &str,
) -> Result<Vec<ArtifactPayloadWire>, String> {
    let mut stmt = conn
        .prepare(
            r#"
            SELECT payload_json
            FROM artifact_payloads
            WHERE artifact_id = ?1
            ORDER BY payload_type, provenance, source_kind, source_id
            "#,
        )
        .map_err(|e| e.to_string())?;
    let rows = stmt
        .query_map([artifact_id], |row| row.get::<_, String>(0))
        .map_err(|e| e.to_string())?;
    let mut payloads = Vec::new();
    for row in rows {
        let json = row.map_err(|e| e.to_string())?;
        payloads.push(serde_json::from_str(&json).map_err(|e| e.to_string())?);
    }
    Ok(payloads)
}

fn load_linked_nodes<P>(
    conn: &Connection,
    sql: &str,
    params: P,
) -> Result<Vec<ArtifactNodeWire>, String>
where
    P: rusqlite::Params,
{
    let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
    let rows = stmt
        .query_map(params, |row| row.get::<_, String>(0))
        .map_err(|e| e.to_string())?;
    let mut nodes = Vec::new();
    for row in rows {
        let json = row.map_err(|e| e.to_string())?;
        nodes.push(serde_json::from_str(&json).map_err(|e| e.to_string())?);
    }
    Ok(nodes)
}

fn load_links<P>(
    conn: &Connection,
    sql: &str,
    params: P,
) -> Result<Vec<ArtifactLinkWire>, String>
where
    P: rusqlite::Params,
{
    let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
    let rows = stmt
        .query_map(params, |row| row.get::<_, String>(0))
        .map_err(|e| e.to_string())?;
    let mut links = Vec::new();
    for row in rows {
        let json = row.map_err(|e| e.to_string())?;
        links.push(serde_json::from_str(&json).map_err(|e| e.to_string())?);
    }
    sort_links(&mut links);
    Ok(links)
}

fn linked_artifact_ids(
    conn: &Connection,
    link_types: &[String],
    include_tombstoned: bool,
) -> Result<HashSet<String>, String> {
    let mut ids = HashSet::new();
    for link_type in link_types {
        let mut stmt = conn
            .prepare(
                r#"
                SELECT source_id, target_id, id
                FROM artifact_links
                WHERE link_type = ?1
                "#,
            )
            .map_err(|e| e.to_string())?;
        let rows = stmt
            .query_map([link_type], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })
            .map_err(|e| e.to_string())?;
        for row in rows {
            let (source_id, target_id, link_id) =
                row.map_err(|e| e.to_string())?;
            if include_tombstoned || !link_is_tombstoned(conn, &link_id)? {
                ids.insert(source_id);
                ids.insert(target_id);
            }
        }
    }
    Ok(ids)
}

fn path_to_root_ids(
    conn: &Connection,
    artifact_id: &str,
) -> Result<Vec<String>, String> {
    let mut path = Vec::new();
    let mut seen = HashSet::new();
    let mut current = artifact_id.to_string();

    loop {
        if !seen.insert(current.clone()) {
            break;
        }
        path.push(current.clone());
        if current == ARTIFACT_ROOT_ID {
            break;
        }
        let parents = parent_ids(conn, &current)?;
        let Some(next) = parents.into_iter().next() else {
            break;
        };
        current = next;
    }

    Ok(path)
}

fn path_diagnostics(
    conn: &Connection,
    artifact_id: &str,
) -> Result<Vec<ArtifactDoctorIssueWire>, String> {
    let mut diagnostics = Vec::new();
    let mut seen = HashSet::new();
    let mut current = artifact_id.to_string();

    loop {
        if !seen.insert(current.clone()) {
            diagnostics.push(issue(
                "parent_cycle",
                "error",
                Some(&current),
                None,
                "parent path contains a cycle",
            ));
            break;
        }
        if current == ARTIFACT_ROOT_ID {
            break;
        }
        let parents = parent_ids(conn, &current)?;
        if parents.is_empty() {
            break;
        }
        if parents.len() > 1 {
            diagnostics.push(issue(
                "duplicate_parent",
                "warning",
                Some(&current),
                None,
                "artifact has more than one parent link",
            ));
        }
        current = parents[0].clone();
    }

    Ok(diagnostics)
}

fn parent_ids(
    conn: &Connection,
    artifact_id: &str,
) -> Result<Vec<String>, String> {
    let mut stmt = conn
        .prepare(
            r#"
            SELECT target_id
            FROM artifact_links l
            WHERE l.link_type = ?1
              AND l.source_id = ?2
              AND NOT EXISTS (
                  SELECT 1 FROM manual_tombstones t
                  WHERE t.tombstone_type = ?3 AND t.link_id = l.id
              )
            ORDER BY target_id, id
            "#,
        )
        .map_err(|e| e.to_string())?;
    let rows = stmt
        .query_map(
            params![ARTIFACT_LINK_PARENT, artifact_id, ARTIFACT_TOMBSTONE_LINK],
            |row| row.get::<_, String>(0),
        )
        .map_err(|e| e.to_string())?;
    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())
}

fn dangling_link_issues(
    conn: &Connection,
) -> Result<Vec<ArtifactDoctorIssueWire>, String> {
    let mut stmt = conn
        .prepare(
            r#"
            SELECT l.id, l.source_id, l.target_id
            FROM artifact_links l
            LEFT JOIN artifacts s ON s.id = l.source_id
            LEFT JOIN artifacts t ON t.id = l.target_id
            WHERE s.id IS NULL OR t.id IS NULL
            ORDER BY l.link_type, l.source_id, l.target_id, l.id
            "#,
        )
        .map_err(|e| e.to_string())?;
    let rows = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .map_err(|e| e.to_string())?;
    let mut issues = Vec::new();
    for row in rows {
        let (link_id, source_id, target_id) = row.map_err(|e| e.to_string())?;
        issues.push(issue(
            "dangling_link",
            "error",
            None,
            Some(&link_id),
            &format!(
                "link references missing endpoint(s): {source_id} -> {target_id}"
            ),
        ));
    }
    Ok(issues)
}

fn duplicate_parent_issues(
    conn: &Connection,
) -> Result<Vec<ArtifactDoctorIssueWire>, String> {
    let mut stmt = conn
        .prepare(
            r#"
            SELECT source_id, COUNT(*) AS parent_count
            FROM artifact_links l
            WHERE link_type = ?1
              AND NOT EXISTS (
                  SELECT 1 FROM manual_tombstones t
                  WHERE t.tombstone_type = ?2 AND t.link_id = l.id
              )
            GROUP BY source_id
            HAVING parent_count > 1
            ORDER BY source_id
            "#,
        )
        .map_err(|e| e.to_string())?;
    let rows = stmt
        .query_map(
            params![ARTIFACT_LINK_PARENT, ARTIFACT_TOMBSTONE_LINK],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
        )
        .map_err(|e| e.to_string())?;
    let mut issues = Vec::new();
    for row in rows {
        let (artifact_id, parent_count) = row.map_err(|e| e.to_string())?;
        issues.push(issue(
            "duplicate_parent",
            "warning",
            Some(&artifact_id),
            None,
            &format!("artifact has {parent_count} parent links"),
        ));
    }
    Ok(issues)
}

fn reachability_issues(
    conn: &Connection,
) -> Result<Vec<ArtifactDoctorIssueWire>, String> {
    let nodes = load_all_nodes(conn, false)?;
    let mut issues = Vec::new();
    for node in nodes {
        if node.id == ARTIFACT_ROOT_ID {
            continue;
        }
        let (reaches_root, has_cycle) = reachability_state(conn, &node.id)?;
        if has_cycle {
            issues.push(issue(
                "parent_cycle",
                "error",
                Some(&node.id),
                None,
                "parent path contains a cycle",
            ));
        } else if !reaches_root {
            issues.push(issue(
                "unreachable",
                "warning",
                Some(&node.id),
                None,
                "artifact does not reach the root through parent links",
            ));
        }
    }
    Ok(issues)
}

fn reachability_state(
    conn: &Connection,
    artifact_id: &str,
) -> Result<(bool, bool), String> {
    let mut seen = HashSet::new();
    let mut current = artifact_id.to_string();

    loop {
        if current == ARTIFACT_ROOT_ID {
            return Ok((true, false));
        }
        if !seen.insert(current.clone()) {
            return Ok((false, true));
        }
        let parents = parent_ids(conn, &current)?;
        let Some(next) = parents.into_iter().next() else {
            return Ok((false, false));
        };
        current = next;
    }
}

fn tombstone_issues(
    conn: &Connection,
) -> Result<Vec<ArtifactDoctorIssueWire>, String> {
    let mut stmt = conn
        .prepare(
            r#"
            SELECT tombstone_type, artifact_id, link_id
            FROM manual_tombstones
            ORDER BY tombstone_type, artifact_id, link_id, id
            "#,
        )
        .map_err(|e| e.to_string())?;
    let rows = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<String>>(2)?,
            ))
        })
        .map_err(|e| e.to_string())?;
    let mut issues = Vec::new();
    for row in rows {
        let (tombstone_type, artifact_id, link_id) =
            row.map_err(|e| e.to_string())?;
        match tombstone_type.as_str() {
            ARTIFACT_TOMBSTONE_NODE => issues.push(issue(
                "tombstoned_artifact",
                "info",
                artifact_id.as_deref(),
                None,
                "artifact is hidden by a manual tombstone",
            )),
            ARTIFACT_TOMBSTONE_LINK => issues.push(issue(
                "tombstoned_link",
                "info",
                None,
                link_id.as_deref(),
                "link is hidden by a manual tombstone",
            )),
            _ => {}
        }
    }
    Ok(issues)
}

fn stale_derived_marker_issues(
    conn: &Connection,
) -> Result<Vec<ArtifactDoctorIssueWire>, String> {
    let mut issues = Vec::new();
    let mut stale_stmt = conn
        .prepare(
            r#"
            SELECT artifact_id, link_id, source_kind, source_id
            FROM manual_tombstones
            WHERE reason = ?1
            ORDER BY tombstone_type, artifact_id, link_id, source_kind, source_id
            "#,
        )
        .map_err(|e| e.to_string())?;
    let stale_rows = stale_stmt
        .query_map([STALE_DERIVED_REASON], |row| {
            Ok((
                row.get::<_, Option<String>>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<String>>(3)?,
            ))
        })
        .map_err(|e| e.to_string())?;
    for row in stale_rows {
        let (artifact_id, link_id, source_kind, source_id) =
            row.map_err(|e| e.to_string())?;
        issues.push(issue(
            "stale_derived",
            "warning",
            artifact_id.as_deref(),
            link_id.as_deref(),
            &format!(
                "derived artifact/link is stale for source {}:{}",
                source_kind.as_deref().unwrap_or_default(),
                source_id.as_deref().unwrap_or_default()
            ),
        ));
    }

    let mut node_stmt = conn
        .prepare(
            r#"
            SELECT id
            FROM artifacts
            WHERE provenance = ?1
              AND (source_kind IS NULL OR source_kind = ''
                   OR source_id IS NULL OR source_id = '')
            ORDER BY id
            "#,
        )
        .map_err(|e| e.to_string())?;
    let node_rows = node_stmt
        .query_map([ARTIFACT_PROVENANCE_DERIVED], |row| row.get::<_, String>(0))
        .map_err(|e| e.to_string())?;
    for row in node_rows {
        let artifact_id = row.map_err(|e| e.to_string())?;
        issues.push(issue(
            "stale_derived",
            "warning",
            Some(&artifact_id),
            None,
            "derived artifact is missing a source marker",
        ));
    }

    let mut link_stmt = conn
        .prepare(
            r#"
            SELECT id
            FROM artifact_links
            WHERE provenance = ?1
              AND (source_kind IS NULL OR source_kind = ''
                   OR source_id_hint IS NULL OR source_id_hint = '')
            ORDER BY id
            "#,
        )
        .map_err(|e| e.to_string())?;
    let link_rows = link_stmt
        .query_map([ARTIFACT_PROVENANCE_DERIVED], |row| row.get::<_, String>(0))
        .map_err(|e| e.to_string())?;
    for row in link_rows {
        let link_id = row.map_err(|e| e.to_string())?;
        issues.push(issue(
            "stale_derived",
            "warning",
            None,
            Some(&link_id),
            "derived link is missing a source marker",
        ));
    }

    Ok(issues)
}

fn fallback_agent_id_issues(
    conn: &Connection,
) -> Result<Vec<ArtifactDoctorIssueWire>, String> {
    let mut stmt = conn
        .prepare(
            r#"
            SELECT id, node_json
            FROM artifacts
            WHERE kind = 'agent'
            ORDER BY id
            "#,
        )
        .map_err(|e| e.to_string())?;
    let rows = stmt
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(|e| e.to_string())?;
    let mut issues = Vec::new();
    for row in rows {
        let (artifact_id, node_json) = row.map_err(|e| e.to_string())?;
        let Ok(node) = serde_json::from_str::<ArtifactNodeWire>(&node_json)
        else {
            continue;
        };
        if node
            .metadata
            .get("uses_fallback_agent_id")
            .and_then(serde_json::Value::as_bool)
            == Some(true)
        {
            let source_dir = node
                .metadata
                .get("source_artifact_dir")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            issues.push(issue(
                "fallback_agent_id",
                "warning",
                Some(&artifact_id),
                None,
                &format!(
                    "agent uses fallback id; source artifact directory: {source_dir}"
                ),
            ));
        }
    }
    Ok(issues)
}

fn unresolved_payload_issues(
    conn: &Connection,
) -> Result<Vec<ArtifactDoctorIssueWire>, String> {
    let mut stmt = conn
        .prepare(
            r#"
            SELECT artifact_id, payload_type, payload_json
            FROM artifact_payloads
            WHERE provenance = ?1
            ORDER BY artifact_id, payload_type, source_kind, source_id
            "#,
        )
        .map_err(|e| e.to_string())?;
    let rows = stmt
        .query_map([ARTIFACT_PROVENANCE_DERIVED], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .map_err(|e| e.to_string())?;
    let mut issues = Vec::new();
    for row in rows {
        let (artifact_id, payload_type, payload_json) =
            row.map_err(|e| e.to_string())?;
        let Ok(payload) =
            serde_json::from_str::<serde_json::Value>(&payload_json)
        else {
            continue;
        };
        let Some(inner) = payload.get("payload") else {
            continue;
        };
        if let Some(worker_id) = inner
            .get("pending_worker_agent_id")
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.is_empty())
        {
            issues.push(issue(
                "unresolved_bead_reference",
                "warning",
                Some(&artifact_id),
                None,
                &format!("pending worker agent {worker_id} is not present"),
            ));
        }
        if let Some(unresolved) = inner
            .get("unresolved_related")
            .and_then(serde_json::Value::as_array)
        {
            for value in unresolved {
                if let Some(target) =
                    value.as_str().filter(|value| !value.is_empty())
                {
                    let issue_type =
                        unresolved_related_issue_type(&payload_type, target);
                    issues.push(issue(
                        issue_type,
                        "warning",
                        Some(&artifact_id),
                        None,
                        &format!("related target {target} is not present"),
                    ));
                }
            }
        }
    }
    Ok(issues)
}

fn unresolved_related_issue_type(
    payload_type: &str,
    target: &str,
) -> &'static str {
    if target_is_timestamp_reference(target) {
        "unresolved_timestamp_link"
    } else if matches!(payload_type, "bead" | "agent") {
        "unresolved_changespec_reference"
    } else {
        "unresolved_related"
    }
}

fn target_is_timestamp_reference(target: &str) -> bool {
    let value = target
        .strip_prefix("ambiguous timestamp ")
        .unwrap_or(target);
    value.chars().take_while(|ch| ch.is_ascii_digit()).count() >= 14
}

fn link_is_tombstoned(
    conn: &Connection,
    link_id: &str,
) -> Result<bool, String> {
    conn.query_row(
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

fn sort_nodes(nodes: &mut [ArtifactNodeWire]) {
    nodes.sort_by(|a, b| {
        (a.kind.as_str(), a.display_title.as_str(), a.id.as_str()).cmp(&(
            b.kind.as_str(),
            b.display_title.as_str(),
            b.id.as_str(),
        ))
    });
}

fn sort_links(links: &mut [ArtifactLinkWire]) {
    links.sort_by(|a, b| {
        (
            a.link_type.as_str(),
            a.source_id.as_str(),
            a.target_id.as_str(),
            a.id.as_str(),
        )
            .cmp(&(
                b.link_type.as_str(),
                b.source_id.as_str(),
                b.target_id.as_str(),
                b.id.as_str(),
            ))
    });
}

fn issue(
    issue_type: &str,
    severity: &str,
    artifact_id: Option<&str>,
    link_id: Option<&str>,
    message: &str,
) -> ArtifactDoctorIssueWire {
    ArtifactDoctorIssueWire {
        issue_type: issue_type.to_string(),
        severity: severity.to_string(),
        artifact_id: artifact_id.map(str::to_string),
        link_id: link_id.map(str::to_string),
        message: message.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use tempfile::tempdir;

    use crate::artifact::store::{
        deterministic_artifact_link_id, open_artifact_store,
        remove_artifact_link, remove_artifact_node, upsert_artifact_link,
        upsert_artifact_node, upsert_artifact_payload,
    };
    use crate::artifact::wire::{
        ArtifactLinkRemoveWire, ArtifactLinkUpsertWire, ArtifactNodeRemoveWire,
        ArtifactNodeUpsertWire, ArtifactPayloadWire, ARTIFACT_KIND_AGENT,
        ARTIFACT_KIND_FILE, ARTIFACT_KIND_PROJECT, ARTIFACT_LINK_CREATED,
        ARTIFACT_LINK_RELATED, ARTIFACT_LINK_WORKER,
        ARTIFACT_PROVENANCE_MANUAL,
    };

    use super::*;

    fn manual_node(id: &str, kind: &str, title: &str) -> ArtifactNodeWire {
        ArtifactNodeWire {
            id: id.to_string(),
            kind: kind.to_string(),
            display_title: title.to_string(),
            subtitle: None,
            provenance: ARTIFACT_PROVENANCE_MANUAL.to_string(),
            source_kind: None,
            source_id: None,
            source_version: None,
            search_text: format!("{title} {id}"),
            metadata: serde_json::Map::new(),
            created_at: None,
            updated_at: None,
        }
    }

    fn derived_node_without_marker(id: &str) -> ArtifactNodeWire {
        ArtifactNodeWire {
            provenance: ARTIFACT_PROVENANCE_DERIVED.to_string(),
            ..manual_node(id, ARTIFACT_KIND_PROJECT, id)
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

    fn upsert_node(store: &mut ArtifactStore, node: ArtifactNodeWire) {
        upsert_artifact_node(
            store,
            ArtifactNodeUpsertWire {
                schema_version: ARTIFACT_WIRE_SCHEMA_VERSION,
                node,
                replace_payloads: false,
            },
        )
        .unwrap();
    }

    fn upsert_link(store: &mut ArtifactStore, link: ArtifactLinkWire) {
        upsert_artifact_link(
            store,
            ArtifactLinkUpsertWire {
                schema_version: ARTIFACT_WIRE_SCHEMA_VERSION,
                link,
            },
        )
        .unwrap();
    }

    fn fixture_store() -> ArtifactStore {
        let tmp = tempdir().unwrap();
        let db = tmp.path().join("artifacts.sqlite");
        let mut store = open_artifact_store(&db).unwrap();
        upsert_node(&mut store, manual_node("/tmp", ARTIFACT_KIND_FILE, "tmp"));
        upsert_node(
            &mut store,
            manual_node("/tmp/a.md", ARTIFACT_KIND_FILE, "a markdown"),
        );
        upsert_node(
            &mut store,
            manual_node("/tmp/b.md", ARTIFACT_KIND_FILE, "b markdown"),
        );
        upsert_node(
            &mut store,
            manual_node("agent-1", ARTIFACT_KIND_AGENT, "agent one"),
        );
        upsert_link(
            &mut store,
            manual_link(ARTIFACT_LINK_PARENT, "/tmp", ARTIFACT_ROOT_ID),
        );
        upsert_link(
            &mut store,
            manual_link(ARTIFACT_LINK_PARENT, "/tmp/a.md", "/tmp"),
        );
        upsert_link(
            &mut store,
            manual_link(ARTIFACT_LINK_PARENT, "/tmp/b.md", "/tmp"),
        );
        upsert_link(
            &mut store,
            manual_link(ARTIFACT_LINK_CREATED, "agent-1", "/tmp/a.md"),
        );
        store
    }

    #[test]
    fn children_walk_reverse_parent_links() {
        let store = fixture_store();

        let children = artifact_children(&store, "/tmp").unwrap();
        let ids: Vec<_> =
            children.iter().map(|node| node.id.as_str()).collect();

        assert_eq!(ids, vec!["/tmp/a.md", "/tmp/b.md"]);
    }

    #[test]
    fn path_to_root_walks_forward_parent_links_and_stops_on_cycles() {
        let mut store = fixture_store();

        let path = artifact_path_to_root(&store, "/tmp/a.md").unwrap();
        let ids: Vec<_> = path.iter().map(|node| node.id.as_str()).collect();
        assert_eq!(ids, vec!["/tmp/a.md", "/tmp", "/"]);

        upsert_node(&mut store, manual_node("c1", ARTIFACT_KIND_FILE, "c1"));
        upsert_node(&mut store, manual_node("c2", ARTIFACT_KIND_FILE, "c2"));
        upsert_link(&mut store, manual_link(ARTIFACT_LINK_PARENT, "c1", "c2"));
        upsert_link(&mut store, manual_link(ARTIFACT_LINK_PARENT, "c2", "c1"));
        let detail = artifact_detail(&store, "c1").unwrap();
        assert!(detail
            .diagnostics
            .iter()
            .any(|issue| issue.issue_type == "parent_cycle"));
        assert!(detail.path_to_root.len() <= 2);
    }

    #[test]
    fn neighbor_queries_preserve_direction_and_group_by_type_order() {
        let mut store = fixture_store();
        upsert_link(
            &mut store,
            manual_link(ARTIFACT_LINK_RELATED, "/tmp/a.md", "agent-1"),
        );

        let outbound = artifact_outbound_neighbors(&store, "agent-1").unwrap();
        assert_eq!(outbound[0].link_type, ARTIFACT_LINK_CREATED);
        assert_eq!(outbound[0].source_id, "agent-1");
        assert_eq!(outbound[0].target_id, "/tmp/a.md");

        let inbound = artifact_inbound_neighbors(&store, "agent-1").unwrap();
        assert_eq!(inbound[0].link_type, ARTIFACT_LINK_RELATED);
        assert_eq!(inbound[0].source_id, "/tmp/a.md");
        assert_eq!(inbound[0].target_id, "agent-1");
    }

    #[test]
    fn list_and_search_ordering_is_deterministic() {
        let store = fixture_store();

        let listed = artifact_list(
            &store,
            ArtifactQueryWire {
                kinds: vec![ARTIFACT_KIND_FILE.to_string()],
                limit: None,
                ..ArtifactQueryWire::default()
            },
        )
        .unwrap();
        let ids: Vec<_> = listed.iter().map(|node| node.id.as_str()).collect();
        assert_eq!(ids, vec!["/tmp/a.md", "/tmp/b.md", "/tmp"]);

        let searched = artifact_search(
            &store,
            ArtifactQueryWire {
                text: Some("markdown".to_string()),
                limit: Some(1),
                offset: Some(1),
                ..ArtifactQueryWire::default()
            },
        )
        .unwrap();
        assert_eq!(searched[0].id, "/tmp/b.md");
    }

    #[test]
    fn list_filters_by_provenance_source_link_type_and_root() {
        let mut store = fixture_store();
        let mut derived =
            manual_node("project:alpha", ARTIFACT_KIND_PROJECT, "Alpha");
        derived.provenance = ARTIFACT_PROVENANCE_DERIVED.to_string();
        derived.source_kind = Some("project_file".to_string());
        derived.source_id = Some("alpha.gp".to_string());
        upsert_node(&mut store, derived);
        upsert_link(
            &mut store,
            manual_link(
                ARTIFACT_LINK_PARENT,
                "project:alpha",
                ARTIFACT_ROOT_ID,
            ),
        );
        upsert_link(
            &mut store,
            manual_link(ARTIFACT_LINK_WORKER, "project:alpha", "agent-1"),
        );

        let filtered = artifact_list(
            &store,
            ArtifactQueryWire {
                link_types: vec![ARTIFACT_LINK_WORKER.to_string()],
                provenance: Some(ARTIFACT_PROVENANCE_DERIVED.to_string()),
                source_kinds: vec!["project_file".to_string()],
                source_ids: vec!["alpha.gp".to_string()],
                root_id: Some(ARTIFACT_ROOT_ID.to_string()),
                limit: None,
                ..ArtifactQueryWire::default()
            },
        )
        .unwrap();

        assert_eq!(
            filtered
                .iter()
                .map(|node| node.id.as_str())
                .collect::<Vec<_>>(),
            vec!["project:alpha"]
        );
    }

    #[test]
    fn detail_includes_payloads_links_children_and_path() {
        let mut store = fixture_store();
        upsert_artifact_payload(
            &mut store,
            ArtifactPayloadWire {
                artifact_id: "/tmp/a.md".to_string(),
                payload_type: "summary".to_string(),
                provenance: ARTIFACT_PROVENANCE_MANUAL.to_string(),
                source_kind: None,
                source_id: None,
                source_version: None,
                payload: json!({"title": "A"}),
                updated_at: None,
            },
        )
        .unwrap();

        let detail = artifact_show(&store, "/tmp/a.md").unwrap();
        assert_eq!(detail.node.unwrap().id, "/tmp/a.md");
        assert_eq!(detail.payloads.len(), 1);
        assert_eq!(detail.outbound_links[0].target_id, "/tmp");
        assert_eq!(detail.inbound_links[0].source_id, "agent-1");
        assert_eq!(
            detail
                .path_to_root
                .iter()
                .map(|node| node.id.as_str())
                .collect::<Vec<_>>(),
            vec!["/tmp/a.md", "/tmp", "/"]
        );
    }

    #[test]
    fn tombstoned_nodes_are_hidden_unless_requested() {
        let mut store = fixture_store();
        remove_artifact_node(
            &mut store,
            ArtifactNodeRemoveWire {
                schema_version: ARTIFACT_WIRE_SCHEMA_VERSION,
                id: "/tmp/a.md".to_string(),
                provenance: Some(ARTIFACT_PROVENANCE_DERIVED.to_string()),
                source_kind: None,
                source_id: None,
                reason: None,
            },
        )
        .unwrap();

        assert!(artifact_show(&store, "/tmp/a.md").unwrap().node.is_none());
        assert!(!artifact_list(&store, ArtifactQueryWire::default())
            .unwrap()
            .iter()
            .any(|node| node.id == "/tmp/a.md"));
        assert!(artifact_list(
            &store,
            ArtifactQueryWire {
                include_tombstoned: true,
                ..ArtifactQueryWire::default()
            },
        )
        .unwrap()
        .iter()
        .any(|node| node.id == "/tmp/a.md"));
    }

    #[test]
    fn path_and_reachability_cover_root_missing_and_orphan_nodes() {
        let mut store = fixture_store();
        upsert_node(
            &mut store,
            manual_node("orphan", ARTIFACT_KIND_FILE, "orphan"),
        );

        assert_eq!(
            artifact_path_to_root(&store, ARTIFACT_ROOT_ID)
                .unwrap()
                .iter()
                .map(|node| node.id.as_str())
                .collect::<Vec<_>>(),
            vec![ARTIFACT_ROOT_ID]
        );
        assert!(artifact_path_to_root(&store, "missing").unwrap().is_empty());
        assert!(artifact_is_reachable_to_root(&store, "/tmp/a.md").unwrap());
        assert!(!artifact_is_reachable_to_root(&store, "orphan").unwrap());
    }

    #[test]
    fn doctor_reports_graph_health_cases() {
        let tmp = tempdir().unwrap();
        let db = tmp.path().join("artifacts.sqlite");
        let mut store = open_artifact_store(&db).unwrap();
        upsert_node(&mut store, manual_node("a", ARTIFACT_KIND_FILE, "a file"));
        upsert_node(&mut store, manual_node("b", ARTIFACT_KIND_FILE, "b file"));
        upsert_node(&mut store, derived_node_without_marker("derived"));
        upsert_link(&mut store, manual_link(ARTIFACT_LINK_PARENT, "a", "b"));
        upsert_link(&mut store, manual_link(ARTIFACT_LINK_PARENT, "b", "a"));
        upsert_link(
            &mut store,
            manual_link(ARTIFACT_LINK_PARENT, "derived", "a"),
        );
        store
            .connection()
            .execute("PRAGMA foreign_keys = OFF", [])
            .unwrap();
        let dangling = manual_link(ARTIFACT_LINK_RELATED, "a", "missing");
        let dangling_json = serde_json::to_string(&dangling).unwrap();
        store
            .connection()
            .execute(
                r#"
                INSERT INTO artifact_links (
                    id, link_type, source_id, target_id, provenance, link_json
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                "#,
                params![
                    dangling.id,
                    dangling.link_type,
                    dangling.source_id,
                    dangling.target_id,
                    dangling.provenance,
                    dangling_json,
                ],
            )
            .unwrap();

        let report =
            artifact_doctor(&store, ArtifactDoctorOptionsWire::default())
                .unwrap();
        let issue_types: BTreeSet<_> = report
            .issues
            .iter()
            .map(|issue| issue.issue_type.as_str())
            .collect();

        assert!(!report.ok);
        assert!(issue_types.contains("dangling_link"));
        assert!(issue_types.contains("parent_cycle"));
        assert!(issue_types.contains("stale_derived"));
        assert!(report.issues.iter().any(|issue| {
            issue.issue_type == "dangling_link" && issue.severity == "error"
        }));
        assert!(report.issues.iter().any(|issue| {
            issue.issue_type == "stale_derived" && issue.severity == "warning"
        }));
    }

    #[test]
    fn doctor_reports_duplicate_parent_and_unreachable_nodes() {
        let mut store = fixture_store();
        upsert_node(
            &mut store,
            manual_node("orphan", ARTIFACT_KIND_FILE, "orphan"),
        );
        let mut second_parent =
            manual_link(ARTIFACT_LINK_PARENT, "/tmp/a.md", ARTIFACT_ROOT_ID);
        second_parent.id = "second-parent".to_string();
        upsert_link(&mut store, second_parent);

        let report =
            artifact_doctor(&store, ArtifactDoctorOptionsWire::default())
                .unwrap();

        assert!(report.issues.iter().any(|issue| {
            issue.issue_type == "duplicate_parent"
                && issue.artifact_id.as_deref() == Some("/tmp/a.md")
        }));
        assert!(report.issues.iter().any(|issue| {
            issue.issue_type == "unreachable"
                && issue.artifact_id.as_deref() == Some("orphan")
        }));
    }

    #[test]
    fn doctor_ignores_tombstoned_parent_links_when_checking_duplicates() {
        let mut store = fixture_store();
        let derived_parent =
            manual_link(ARTIFACT_LINK_PARENT, "/tmp/a.md", ARTIFACT_ROOT_ID);
        let mut derived_parent_to_tombstone = derived_parent.clone();
        derived_parent_to_tombstone.id = "derived-parent".to_string();
        derived_parent_to_tombstone.provenance =
            ARTIFACT_PROVENANCE_DERIVED.to_string();
        derived_parent_to_tombstone.source_kind = Some("scan".to_string());
        derived_parent_to_tombstone.source_id_hint = Some("scan-1".to_string());
        upsert_link(&mut store, derived_parent_to_tombstone.clone());
        remove_artifact_link(
            &mut store,
            ArtifactLinkRemoveWire {
                schema_version: ARTIFACT_WIRE_SCHEMA_VERSION,
                id: Some(derived_parent_to_tombstone.id.clone()),
                link_type: None,
                source_id: None,
                target_id: None,
                provenance: Some(ARTIFACT_PROVENANCE_DERIVED.to_string()),
                source_kind: None,
                source_id_hint: None,
                reason: Some("hidden".to_string()),
            },
        )
        .unwrap();

        let report =
            artifact_doctor(&store, ArtifactDoctorOptionsWire::default())
                .unwrap();

        assert!(!report.issues.iter().any(|issue| {
            issue.issue_type == "duplicate_parent"
                && issue.artifact_id.as_deref() == Some("/tmp/a.md")
        }));
    }
}
