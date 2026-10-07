//! Indexed queries over the read model (`read-model-queries` phase).
//!
//! Every function here serves one hot query from the read-model tables,
//! touching only the rows it needs, and returns `Ok(None)` whenever the
//! caller should fall back to the replay path: no cache location, a legacy
//! store, or any cache fault. Genuine store errors surface as `Err`,
//! exactly as the replay the caller falls back to would fail with them.
//!
//! Result equality with replay is structural: row order reproduces the
//! replay path's stable `created_at` sort by breaking ties on the stored
//! `position` (replay order), filters validate through the same `read`
//! parsers, and shorthand resolution reports the same `not_found` and
//! `ambiguous` errors.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use rusqlite::Connection;

use crate::artifact_link::{
    ArtifactLinkOriginWire, ArtifactLinkRowWire, BeadLinkDirectionWire,
};
use crate::bead::events::{
    artifact_link_row_from_provenance, ActiveLinkProvenance,
};
use crate::bead::read::{
    parse_issue_types, parse_statuses, parse_tiers, sort_artifact_link_rows,
    task_type_matches, BeadIssueDetailWire,
};
use crate::bead::wire::{BeadError, BeadSearchMatchWire, IssueWire};

use super::store::{
    drop_cache_file, ensure_cache_ready_at, open_read_only, read_meta,
    SERVE_BUSY_TIMEOUT,
};

/// Open a read-only connection to a fresh cache, or `None` on fallback.
///
/// Ensures freshness first (token-only on warm stores, so no row is
/// touched), then opens read-only. A version mismatch or unreadable row
/// drops the file and reports fallback: the next read rebuilds.
fn fresh_connection(
    beads_dir: &Path,
    cache_path: &Path,
) -> Result<Option<Connection>, BeadError> {
    if !ensure_cache_ready_at(beads_dir, cache_path)? {
        return Ok(None);
    }
    let connection = match open_read_only(cache_path, SERVE_BUSY_TIMEOUT) {
        Ok(connection) => connection,
        Err(_) => return Ok(None),
    };
    let meta = match read_meta(&connection) {
        Ok(meta) => meta,
        Err(_) => {
            drop_cache_file(cache_path);
            return Ok(None);
        }
    };
    if meta.schema_version != super::store::READ_MODEL_SCHEMA_VERSION
        || meta.reducer_version != super::store::READ_MODEL_REDUCER_VERSION
        || meta.crate_version != env!("CARGO_PKG_VERSION")
    {
        drop(connection);
        drop_cache_file(cache_path);
        return Ok(None);
    }
    Ok(Some(connection))
}

/// Ensure freshness through the default cache location, then run `query`.
///
/// Returns `Ok(None)` when there is no cache location (plain replay serves
/// the read) or the cache is unusable; the caller replays instead.
fn with_fresh_cache<T>(
    beads_dir: &Path,
    query: impl FnOnce(&Connection) -> Result<T, String>,
) -> Result<Option<T>, BeadError> {
    let Some(cache_path) =
        super::location::read_model_cache_path_for_store(beads_dir)
    else {
        return Ok(None);
    };
    with_fresh_cache_at(beads_dir, &cache_path, query)
}

/// Ensure freshness at an explicit cache path (tests), then run `query`.
pub fn with_fresh_cache_at<T>(
    beads_dir: &Path,
    cache_path: &Path,
    query: impl FnOnce(&Connection) -> Result<T, String>,
) -> Result<Option<T>, BeadError> {
    let Some(connection) = fresh_connection(beads_dir, cache_path)? else {
        return Ok(None);
    };
    match query(&connection) {
        Ok(value) => Ok(Some(value)),
        Err(_) => {
            drop(connection);
            drop_cache_file(cache_path);
            Ok(None)
        }
    }
}

/// Decode one cached `row` cell, mapping corruption to a cache fault.
fn decode_issue(row: String) -> Result<IssueWire, String> {
    serde_json::from_str(&row)
        .map_err(|error| format!("cached issue row is not valid: {error}"))
}

/// Load one issue by exact ID, or `None` when the row is absent.
fn load_issue(
    connection: &Connection,
    issue_id: &str,
) -> Result<Option<IssueWire>, String> {
    let mut statement = connection
        .prepare("SELECT row FROM issues WHERE id = ?1")
        .map_err(|error| error.to_string())?;
    let mut rows = statement
        .query([issue_id])
        .map_err(|error| error.to_string())?;
    match rows.next().map_err(|error| error.to_string())? {
        Some(row) => {
            let text: String = row.get(0).map_err(|error| error.to_string())?;
            decode_issue(text).map(Some)
        }
        None => Ok(None),
    }
}

/// Resolve shorthand through the suffix catalog, mirroring
/// `resolve_issue_id_in_issues` exactly: full IDs (and empty strings) pass
/// through, unique suffixes resolve, unknown suffixes fail `not_found`,
/// and collisions fail `ambiguous` with the same message.
fn resolve_in(
    connection: &Connection,
    issue_id: &str,
) -> Result<String, BeadError> {
    if issue_id.is_empty() || issue_id.contains('-') {
        return Ok(issue_id.to_string());
    }
    let mut statement = connection
        .prepare(
            "SELECT issue_id FROM suffix_catalog WHERE suffix = ?1 ORDER BY issue_id",
        )
        .map_err(|error| BeadError::io(error.to_string()))?;
    let rows = statement
        .query_map([issue_id], |row| row.get::<_, String>(0))
        .map_err(|error| BeadError::io(error.to_string()))?;
    let mut candidates = Vec::new();
    for row in rows {
        candidates.push(row.map_err(|error| BeadError::io(error.to_string()))?);
    }
    match candidates.as_slice() {
        [resolved] => Ok(resolved.clone()),
        [] => Err(BeadError {
            kind: "not_found".to_string(),
            message: format!("Issue not found: {issue_id}"),
        }),
        _ => Err(BeadError {
            kind: "ambiguous".to_string(),
            message: format!(
                "ambiguous bead ID shorthand {issue_id:?}: {}",
                candidates.join(", ")
            ),
        }),
    }
}

/// Resolve one bead ID through the cache, or `None` on fallback.
///
/// Resolution failures (`not_found`, `ambiguous`) propagate instead of
/// falling back: they are answers, not cache faults. Only cache faults
/// return `None`.
pub fn cached_resolve(
    beads_dir: &Path,
    issue_id: &str,
) -> Result<Option<Result<String, BeadError>>, BeadError> {
    let issue_id = issue_id.to_string();
    with_fresh_cache(beads_dir, |connection| {
        Ok(resolve_in(connection, &issue_id))
    })
}

/// Load one issue by ID through the cache, or `None` on fallback.
///
/// Resolution failures and missing rows are answers, not faults: they
/// propagate as the replay path's errors. Only cache faults return `None`.
pub fn cached_show(
    beads_dir: &Path,
    issue_id: &str,
) -> Result<Option<Result<IssueWire, BeadError>>, BeadError> {
    let issue_id = issue_id.to_string();
    with_fresh_cache(beads_dir, |connection| {
        Ok(match resolve_in(connection, &issue_id) {
            Ok(resolved) => match load_issue(connection, &resolved) {
                Ok(Some(issue)) => Ok(issue),
                Ok(None) => Err(BeadError {
                    kind: "not_found".to_string(),
                    message: format!("Issue not found: {resolved}"),
                }),
                Err(reason) => Err(BeadError::io(reason)),
            },
            Err(error) => Err(error),
        })
    })
}

/// Serve one issue's detail graph from the indexes, or `None` on fallback.
///
/// Replaces the replay path's full-snapshot scans with a point lookup, the
/// children index, the reverse-dependency edge table, the parent chain,
/// the suffix catalog, and provenance-by-target rows. Ordering matches the
/// replay path: children and reverse dependents sort by `created_at` with
/// replay-order ties, and `depends_on` follows the stored dependency
/// order.
pub fn cached_detail(
    beads_dir: &Path,
    issue_id: &str,
    include_links: bool,
) -> Result<Option<Result<BeadIssueDetailWire, BeadError>>, BeadError> {
    let issue_id = issue_id.to_string();
    with_fresh_cache(beads_dir, |connection| {
        Ok(detail_in(connection, &issue_id, include_links))
    })
}

fn detail_in(
    connection: &Connection,
    issue_id: &str,
    include_links: bool,
) -> Result<BeadIssueDetailWire, BeadError> {
    let resolved_id = resolve_in(connection, issue_id)?;
    let issue = load_issue(connection, &resolved_id)
        .map_err(BeadError::io)?
        .ok_or_else(|| BeadError {
            kind: "not_found".to_string(),
            message: format!("Issue not found: {resolved_id}"),
        })?;
    let ancestors = ancestors_in(connection, &issue)?;
    let children = children_in(connection, &resolved_id)?;
    let mut depends_on = Vec::new();
    for dependency in &issue.dependencies {
        depends_on
            .push(optional_issue_in(connection, &dependency.depends_on_id)?);
    }
    let blocks = dependents_in(connection, &resolved_id)?;
    let artifact_links = if include_links {
        neighborhood_in(connection, &resolved_id)?
    } else {
        Vec::new()
    };
    Ok(BeadIssueDetailWire {
        issue,
        ancestors,
        children,
        depends_on,
        blocks,
        artifact_links,
    })
}

/// Walk the parent chain through point lookups, mirroring
/// `issue_ancestors_in_issues`: cycles and dangling parents resolve to a
/// trailing `None`, while genuine store errors propagate.
fn ancestors_in(
    connection: &Connection,
    issue: &IssueWire,
) -> Result<Vec<Option<IssueWire>>, BeadError> {
    let mut ancestors = Vec::new();
    let mut parent_id = issue.parent_id.clone();
    let mut seen = BTreeSet::from([issue.id.clone()]);
    while let Some(current_parent_id) = parent_id {
        if !seen.insert(current_parent_id.clone()) {
            ancestors.push(None);
            break;
        }
        let Some(parent) = optional_issue_in(connection, &current_parent_id)?
        else {
            ancestors.push(None);
            break;
        };
        parent_id = parent.parent_id.clone();
        ancestors.push(Some(parent));
    }
    Ok(ancestors)
}

/// Resolve one optional reference, mirroring
/// `resolve_optional_issue_in_issues`: `not_found` becomes `None`, every
/// other error propagates.
fn optional_issue_in(
    connection: &Connection,
    issue_id: &str,
) -> Result<Option<IssueWire>, BeadError> {
    let resolved_id = match resolve_in(connection, issue_id) {
        Ok(resolved_id) => resolved_id,
        Err(error) if error.kind == "not_found" => return Ok(None),
        Err(error) => return Err(error),
    };
    load_issue(connection, &resolved_id).map_err(BeadError::io)
}

/// Children through the parent index in replay order.
fn children_in(
    connection: &Connection,
    issue_id: &str,
) -> Result<Vec<IssueWire>, BeadError> {
    load_ordered_rows(
        connection,
        "SELECT row FROM issues WHERE parent = ?1 ORDER BY created_at ASC, position ASC",
        [issue_id],
    )
}

/// Reverse dependents through the edge table in replay order.
fn dependents_in(
    connection: &Connection,
    issue_id: &str,
) -> Result<Vec<IssueWire>, BeadError> {
    let mut statement = connection
        .prepare(
            "SELECT issues.row FROM issues JOIN edges ON issues.id = edges.src WHERE edges.dst = ?1 AND edges.kind = 'depends_on' ORDER BY issues.created_at ASC, issues.position ASC",
        )
        .map_err(|error| BeadError::io(error.to_string()))?;
    let rows = statement
        .query_map([issue_id], |row| row.get::<_, String>(0))
        .map_err(|error| BeadError::io(error.to_string()))?;
    let mut dependents = Vec::new();
    for row in rows {
        let text: String =
            row.map_err(|error| BeadError::io(error.to_string()))?;
        dependents.push(decode_issue(text).map_err(BeadError::io)?);
    }
    Ok(dependents)
}

/// Load rows for one ordered `SELECT row ...` query.
fn load_ordered_rows(
    connection: &Connection,
    sql: &str,
    id: [&str; 1],
) -> Result<Vec<IssueWire>, BeadError> {
    let mut statement = connection
        .prepare(sql)
        .map_err(|error| BeadError::io(error.to_string()))?;
    let rows = statement
        .query_map(id, |row| row.get::<_, String>(0))
        .map_err(|error| BeadError::io(error.to_string()))?;
    let mut issues = Vec::new();
    for row in rows {
        let text: String =
            row.map_err(|error| BeadError::io(error.to_string()))?;
        issues.push(decode_issue(text).map_err(BeadError::io)?);
    }
    Ok(issues)
}

/// Provenance neighborhood for one bead, mirroring
/// `neighborhood_from_provenance`: surviving sources only, the same touch
/// rule (which also fixes the replay path's `O(links x issues)` shorthand
/// scan with catalog lookups), and the same row sort.
fn neighborhood_in(
    connection: &Connection,
    issue_id: &str,
) -> Result<Vec<ArtifactLinkRowWire>, BeadError> {
    let surviving: BTreeSet<String> = {
        let mut statement = connection
            .prepare("SELECT id FROM issues")
            .map_err(|error| BeadError::io(error.to_string()))?;
        let rows = statement
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(|error| BeadError::io(error.to_string()))?;
        let mut surviving = BTreeSet::new();
        for row in rows {
            surviving
                .insert(row.map_err(|error| BeadError::io(error.to_string()))?);
        }
        surviving
    };
    let mut statement = connection
        .prepare(
            "SELECT target_ref, source_issue_id, relation, description, origin, direction, uses, actor, timestamp FROM link_provenance",
        )
        .map_err(|error| BeadError::io(error.to_string()))?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, i64>(6)?,
                row.get::<_, String>(7)?,
                row.get::<_, String>(8)?,
            ))
        })
        .map_err(|error| BeadError::io(error.to_string()))?;
    let canonical = format!("bead:{issue_id}");
    let mut links = Vec::new();
    for row in rows {
        let (
            target_ref,
            source_issue_id,
            relation,
            description,
            origin,
            direction,
            uses,
            actor,
            timestamp,
        ) = row.map_err(|error| BeadError::io(error.to_string()))?;
        if !surviving.contains(&source_issue_id) {
            continue;
        }
        if !row_touches_in(
            connection,
            &format!("bead:{source_issue_id}"),
            &target_ref,
            &canonical,
            issue_id,
        )? {
            continue;
        }
        let origin: ArtifactLinkOriginWire =
            serde_json::from_value(serde_json::Value::String(origin))
                .map_err(|error| BeadError::io(error.to_string()))?;
        let direction: BeadLinkDirectionWire =
            serde_json::from_value(serde_json::Value::String(direction))
                .map_err(|error| BeadError::io(error.to_string()))?;
        links.push(artifact_link_row_from_provenance(&ActiveLinkProvenance {
            source_issue_id,
            target_ref,
            relation,
            description,
            origin,
            direction,
            uses: u64::try_from(uses).unwrap_or(0),
            actor,
            timestamp,
        }));
    }
    sort_artifact_link_rows(&mut links);
    Ok(links)
}

/// Whether one provenance row touches a bead, mirroring `row_touches_bead`
/// with catalog lookups instead of a full-issue scan.
fn row_touches_in(
    connection: &Connection,
    source_ref: &str,
    target_ref: &str,
    canonical: &str,
    issue_id: &str,
) -> Result<bool, BeadError> {
    if source_ref == canonical || target_ref == canonical {
        return Ok(true);
    }
    for candidate in [source_ref, target_ref] {
        let Some(raw_id) = candidate.strip_prefix("bead:") else {
            continue;
        };
        if resolve_in(connection, raw_id).ok().as_deref() == Some(issue_id) {
            return Ok(true);
        }
    }
    Ok(false)
}

/// List filters pushed down into the cache: status, type, tier, and task
/// type narrow the rows read, and `limit` keeps only the newest matches.
///
/// `limit` mirrors the CLI's `issues[-limit:]` slice: the result stays in
/// ascending `created_at` order. Task-type matching mirrors Python's
/// `issue_matches_task_types`: an empty stored type counts as `untyped`,
/// empty wanted entries are ignored, and comparison is case-insensitive.
#[allow(clippy::too_many_arguments)]
pub fn cached_list(
    beads_dir: &Path,
    statuses: Option<&[String]>,
    issue_types: Option<&[String]>,
    tiers: Option<&[String]>,
    task_types: Option<&[String]>,
    limit: Option<usize>,
) -> Result<Option<Result<ListPage, BeadError>>, BeadError> {
    let owned =
        OwnedListQuery::new(statuses, issue_types, tiers, task_types, limit);
    with_fresh_cache(beads_dir, |connection| Ok(list_in(connection, &owned)))
}

/// One filtered list view: the pre-limit match count plus the rows.
pub struct ListPage {
    /// Matches before `limit`, backing the CLI's `matched` summary count.
    pub total: usize,
    /// Matching rows in ascending `created_at` order.
    pub issues: Vec<IssueWire>,
}

struct OwnedListQuery {
    statuses: Option<Vec<String>>,
    issue_types: Option<Vec<String>>,
    tiers: Option<Vec<String>>,
    task_types: Option<Vec<String>>,
    limit: Option<usize>,
}

impl OwnedListQuery {
    fn new(
        statuses: Option<&[String]>,
        issue_types: Option<&[String]>,
        tiers: Option<&[String]>,
        task_types: Option<&[String]>,
        limit: Option<usize>,
    ) -> Self {
        Self {
            statuses: statuses.map(<[String]>::to_vec),
            issue_types: issue_types.map(<[String]>::to_vec),
            tiers: tiers.map(<[String]>::to_vec),
            task_types: task_types.map(<[String]>::to_vec),
            limit,
        }
    }
}

fn list_in(
    connection: &Connection,
    query: &OwnedListQuery,
) -> Result<ListPage, BeadError> {
    // Validation first, through the same parsers as the replay path, so
    // invalid filters fail identically on both lanes.
    parse_statuses(query.statuses.as_deref())?;
    parse_issue_types(query.issue_types.as_deref())?;
    parse_tiers(query.tiers.as_deref())?;
    let task_filter_active = query
        .task_types
        .as_deref()
        .is_some_and(|wanted| !wanted.is_empty());
    let mut clauses = Vec::new();
    let mut values: Vec<String> = Vec::new();
    push_in_clause(&mut clauses, &mut values, "status", &query.statuses);
    push_in_clause(&mut clauses, &mut values, "issue_type", &query.issue_types);
    push_in_clause(&mut clauses, &mut values, "tier", &query.tiers);
    let filter = if clauses.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", clauses.join(" AND "))
    };
    // Newest-first scan when a limit applies so only matching rows cross
    // the wire; the survivors reverse back to ascending order below. A
    // zero limit slices nothing, mirroring the CLI's `if limit:` guard.
    let limit = query.limit.filter(|limit| *limit > 0);
    let descending = limit.is_some();
    let order = if descending {
        "ORDER BY created_at DESC, position DESC"
    } else {
        "ORDER BY created_at ASC, position ASC"
    };
    let sql = format!("SELECT id, task_type FROM issues {filter} {order}");
    let mut statement = connection
        .prepare(&sql)
        .map_err(|error| BeadError::io(error.to_string()))?;
    let params: Vec<&dyn rusqlite::ToSql> = values
        .iter()
        .map(|value| value as &dyn rusqlite::ToSql)
        .collect();
    let rows = statement
        .query_map(params.as_slice(), |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(|error| BeadError::io(error.to_string()))?;
    let mut matching: Vec<String> = Vec::new();
    for row in rows {
        let (id, task_type) =
            row.map_err(|error| BeadError::io(error.to_string()))?;
        if task_filter_active
            && !task_type_matches(
                (!task_type.is_empty()).then_some(task_type.as_str()),
                query.task_types.as_deref(),
            )
        {
            continue;
        }
        matching.push(id);
        if descending && limit.is_some_and(|limit| matching.len() >= limit) {
            break;
        }
    }
    // The ascending scan has no limit to apply; the descending scan kept
    // the newest matches and reverses back to ascending order here.
    let total = if descending {
        count_list_total(connection, query)?
    } else {
        matching.len()
    };
    if descending {
        matching.reverse();
    } else if let Some(limit) = limit {
        let start = matching.len().saturating_sub(limit);
        matching = matching[start..].to_vec();
    }
    let mut issues = Vec::with_capacity(matching.len());
    for id in &matching {
        let Some(issue) = load_issue(connection, id).map_err(BeadError::io)?
        else {
            return Err(BeadError::io(format!(
                "cached list row vanished mid-query: {id}"
            )));
        };
        issues.push(issue);
    }
    Ok(ListPage { total, issues })
}

/// Count pre-limit matches for a limited query without loading rows.
fn count_list_total(
    connection: &Connection,
    query: &OwnedListQuery,
) -> Result<usize, BeadError> {
    let task_filter_active = query
        .task_types
        .as_deref()
        .is_some_and(|wanted| !wanted.is_empty());
    if !task_filter_active {
        let mut clauses = Vec::new();
        let mut values: Vec<String> = Vec::new();
        push_in_clause(&mut clauses, &mut values, "status", &query.statuses);
        push_in_clause(
            &mut clauses,
            &mut values,
            "issue_type",
            &query.issue_types,
        );
        push_in_clause(&mut clauses, &mut values, "tier", &query.tiers);
        let filter = if clauses.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", clauses.join(" AND "))
        };
        let mut statement = connection
            .prepare(&format!("SELECT COUNT(*) FROM issues {filter}"))
            .map_err(|error| BeadError::io(error.to_string()))?;
        let params: Vec<&dyn rusqlite::ToSql> = values
            .iter()
            .map(|value| value as &dyn rusqlite::ToSql)
            .collect();
        return statement
            .query_row(params.as_slice(), |row| row.get::<_, i64>(0))
            .map(|count| usize::try_from(count).unwrap_or(0))
            .map_err(|error| BeadError::io(error.to_string()));
    }
    // A task-type filter needs the column values; count without the row
    // payloads.
    let mut clauses = Vec::new();
    let mut values: Vec<String> = Vec::new();
    push_in_clause(&mut clauses, &mut values, "status", &query.statuses);
    push_in_clause(&mut clauses, &mut values, "issue_type", &query.issue_types);
    push_in_clause(&mut clauses, &mut values, "tier", &query.tiers);
    let filter = if clauses.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", clauses.join(" AND "))
    };
    let mut statement = connection
        .prepare(&format!("SELECT task_type FROM issues {filter}"))
        .map_err(|error| BeadError::io(error.to_string()))?;
    let params: Vec<&dyn rusqlite::ToSql> = values
        .iter()
        .map(|value| value as &dyn rusqlite::ToSql)
        .collect();
    let rows = statement
        .query_map(params.as_slice(), |row| row.get::<_, String>(0))
        .map_err(|error| BeadError::io(error.to_string()))?;
    let mut total = 0;
    for row in rows {
        let task_type: String =
            row.map_err(|error| BeadError::io(error.to_string()))?;
        if task_type_matches(
            (!task_type.is_empty()).then_some(task_type.as_str()),
            query.task_types.as_deref(),
        ) {
            total += 1;
        }
    }
    Ok(total)
}

/// Append one `column IN (...)` clause with positional placeholders.
fn push_in_clause(
    clauses: &mut Vec<String>,
    values: &mut Vec<String>,
    column: &str,
    wanted: &Option<Vec<String>>,
) {
    let Some(wanted) = wanted else { return };
    if wanted.is_empty() {
        clauses.push("1 = 0".to_string());
        return;
    }
    let placeholders = wanted.iter().map(|_| "?").collect::<Vec<_>>().join(",");
    clauses.push(format!("{column} IN ({placeholders})"));
    values.extend(wanted.iter().cloned());
}

/// Ready bead IDs in replay order, reading active rows only.
pub fn cached_ready_ids(
    beads_dir: &Path,
) -> Result<Option<Result<Vec<String>, BeadError>>, BeadError> {
    with_fresh_cache(beads_dir, |connection| Ok(ready_ids_in(connection)))
}

/// Ready beads in replay order, reading active rows only.
pub fn cached_ready(
    beads_dir: &Path,
) -> Result<Option<Result<Vec<IssueWire>, BeadError>>, BeadError> {
    with_fresh_cache(beads_dir, |connection| {
        Ok(ready_ids_in(connection)
            .and_then(|ids| load_ids_in(connection, &ids)))
    })
}

fn ready_ids_in(connection: &Connection) -> Result<Vec<String>, BeadError> {
    let mut statement = connection
        .prepare(
            "SELECT id FROM issues WHERE status = 'ready' AND issue_type = 'task' ORDER BY created_at ASC, position ASC",
        )
        .map_err(|error| BeadError::io(error.to_string()))?;
    let rows = statement
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(|error| BeadError::io(error.to_string()))?;
    let mut ready = Vec::new();
    for row in rows {
        let id: String =
            row.map_err(|error| BeadError::io(error.to_string()))?;
        if !has_active_blocker_in(connection, &id)? {
            ready.push(id);
        }
    }
    Ok(ready)
}

/// Blocked bead IDs in replay order: every issue with a dependency on an
/// active bead, exactly as `blocked_issues_in_issues` computes them.
pub fn cached_blocked_ids(
    beads_dir: &Path,
) -> Result<Option<Result<Vec<String>, BeadError>>, BeadError> {
    with_fresh_cache(beads_dir, |connection| Ok(blocked_ids_in(connection)))
}

/// Blocked beads in replay order.
pub fn cached_blocked(
    beads_dir: &Path,
) -> Result<Option<Result<Vec<IssueWire>, BeadError>>, BeadError> {
    with_fresh_cache(beads_dir, |connection| {
        Ok(blocked_ids_in(connection)
            .and_then(|ids| load_ids_in(connection, &ids)))
    })
}

fn blocked_ids_in(connection: &Connection) -> Result<Vec<String>, BeadError> {
    // Light scan: IDs, statuses, and order keys without the row payloads.
    let mut statement = connection
        .prepare("SELECT id, status, created_at, position FROM issues")
        .map_err(|error| BeadError::io(error.to_string()))?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
            ))
        })
        .map_err(|error| BeadError::io(error.to_string()))?;
    let mut status_by_id: BTreeMap<String, String> = BTreeMap::new();
    let mut order: Vec<(String, String, i64)> = Vec::new();
    for row in rows {
        let (id, status, created_at, position) =
            row.map_err(|error| BeadError::io(error.to_string()))?;
        status_by_id.insert(id.clone(), status);
        order.push((id, created_at, position));
    }
    let mut statement = connection
        .prepare("SELECT src, dst FROM edges WHERE kind = 'depends_on'")
        .map_err(|error| BeadError::io(error.to_string()))?;
    let rows = statement
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(|error| BeadError::io(error.to_string()))?;
    let mut blocked: BTreeSet<String> = BTreeSet::new();
    for row in rows {
        let (src, dst) =
            row.map_err(|error| BeadError::io(error.to_string()))?;
        if status_by_id
            .get(&dst)
            .is_some_and(|status| is_active_status(status))
        {
            blocked.insert(src);
        }
    }
    order.sort_by(|left, right| {
        (left.1.as_str(), left.2).cmp(&(right.1.as_str(), right.2))
    });
    Ok(order
        .into_iter()
        .filter_map(|(id, _, _)| blocked.contains(&id).then_some(id))
        .collect())
}

/// Whether one stored status wire string counts as an active blocker,
/// mirroring `has_active_blocker`.
fn is_active_status(status: &str) -> bool {
    matches!(
        status,
        "open" | "claimed" | "ready" | "snoozed" | "in_progress"
    )
}

/// Whether one bead has a dependency on an active bead.
fn has_active_blocker_in(
    connection: &Connection,
    issue_id: &str,
) -> Result<bool, BeadError> {
    let mut statement = connection
        .prepare("SELECT dst FROM edges WHERE src = ?1 AND kind = 'depends_on'")
        .map_err(|error| BeadError::io(error.to_string()))?;
    let rows = statement
        .query_map([issue_id], |row| row.get::<_, String>(0))
        .map_err(|error| BeadError::io(error.to_string()))?;
    for row in rows {
        let dst: String =
            row.map_err(|error| BeadError::io(error.to_string()))?;
        let mut status_statement = connection
            .prepare("SELECT status FROM issues WHERE id = ?1")
            .map_err(|error| BeadError::io(error.to_string()))?;
        let status: Option<String> =
            status_statement.query_row([dst], |row| row.get(0)).ok();
        if status.as_deref().is_some_and(is_active_status) {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Load full rows for IDs that are already in replay order.
fn load_ids_in(
    connection: &Connection,
    ids: &[String],
) -> Result<Vec<IssueWire>, BeadError> {
    let mut issues = Vec::with_capacity(ids.len());
    for id in ids {
        let Some(issue) = load_issue(connection, id).map_err(BeadError::io)?
        else {
            return Err(BeadError::io(format!(
                "cached query row vanished mid-query: {id}"
            )));
        };
        issues.push(issue);
    }
    Ok(issues)
}

/// Index aggregates for `stats`, or `None` on fallback.
///
/// Status, type, flag, and plus-one counts come from `GROUP BY` aggregates
/// without touching a row payload; only flag beads' rows load, to evaluate
/// the date-sensitive `due_flag` count. Keys match `stats_for_issues`
/// exactly: `flag` and `due_flag` appear only when nonzero.
#[allow(clippy::type_complexity)]
pub fn cached_stats(
    beads_dir: &Path,
) -> Result<Option<Result<BTreeMap<String, usize>, BeadError>>, BeadError> {
    with_fresh_cache(beads_dir, |connection| Ok(stats_in(connection)))
}

fn stats_in(
    connection: &Connection,
) -> Result<BTreeMap<String, usize>, BeadError> {
    let mut stats = BTreeMap::new();
    for column in ["status", "issue_type"] {
        let mut statement = connection
            .prepare(&format!(
                "SELECT {column}, COUNT(*) FROM issues GROUP BY {column}"
            ))
            .map_err(|error| BeadError::io(error.to_string()))?;
        let rows = statement
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })
            .map_err(|error| BeadError::io(error.to_string()))?;
        for row in rows {
            let (value, count) =
                row.map_err(|error| BeadError::io(error.to_string()))?;
            stats.insert(value, usize::try_from(count).unwrap_or(0));
        }
    }
    let total: i64 = connection
        .query_row("SELECT COUNT(*) FROM issues", [], |row| row.get(0))
        .map_err(|error| BeadError::io(error.to_string()))?;
    stats.insert("total".to_string(), usize::try_from(total).unwrap_or(0));
    let plus_one: Option<i64> = connection
        .query_row("SELECT SUM(plus_one) FROM issues", [], |row| row.get(0))
        .map_err(|error| BeadError::io(error.to_string()))?;
    stats.insert(
        "plus_one".to_string(),
        usize::try_from(plus_one.unwrap_or(0)).unwrap_or(0),
    );
    let flag_count: i64 = connection
        .query_row("SELECT COUNT(*) FROM issues WHERE is_flag = 1", [], |row| {
            row.get(0)
        })
        .map_err(|error| BeadError::io(error.to_string()))?;
    if flag_count > 0 {
        stats.insert(
            "flag".to_string(),
            usize::try_from(flag_count).unwrap_or(0),
        );
        let mut statement = connection
            .prepare("SELECT row FROM issues WHERE is_flag = 1")
            .map_err(|error| BeadError::io(error.to_string()))?;
        let rows = statement
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(|error| BeadError::io(error.to_string()))?;
        let today = current_date();
        let release = env!("CARGO_PKG_VERSION");
        let mut due_flag = 0;
        for row in rows {
            let text: String =
                row.map_err(|error| BeadError::io(error.to_string()))?;
            let issue = decode_issue(text).map_err(BeadError::io)?;
            if issue.flag_is_due(today, release) {
                due_flag += 1;
            }
        }
        if due_flag > 0 {
            stats.insert("due_flag".to_string(), due_flag);
        }
    }
    Ok(stats)
}

/// Today's date in UTC, mirroring the replay path's `current_date`.
fn current_date() -> chrono::NaiveDate {
    chrono::Utc::now().date_naive()
}

/// Search cached rows with today's substring and regex semantics, or
/// `None` on fallback.
///
/// Status, type, and tier filters narrow the rows in SQL; the matcher runs
/// over the survivors in Rust. Order mirrors the replay path: candidates
/// ascend by `created_at`, matches emit newest-first, and `limit` keeps
/// the newest matches.
#[allow(clippy::too_many_arguments)]
pub fn cached_search(
    beads_dir: &Path,
    query: &str,
    statuses: Option<&[String]>,
    issue_types: Option<&[String]>,
    tiers: Option<&[String]>,
    limit: Option<usize>,
    regex: bool,
) -> Result<Option<Result<Vec<BeadSearchMatchWire>, BeadError>>, BeadError> {
    let owned = OwnedSearchQuery::new(
        query,
        statuses,
        issue_types,
        tiers,
        limit,
        regex,
    );
    with_fresh_cache(beads_dir, |connection| Ok(search_in(connection, &owned)))
}

struct OwnedSearchQuery {
    query: String,
    statuses: Option<Vec<String>>,
    issue_types: Option<Vec<String>>,
    tiers: Option<Vec<String>>,
    limit: Option<usize>,
    regex: bool,
}

impl OwnedSearchQuery {
    fn new(
        query: &str,
        statuses: Option<&[String]>,
        issue_types: Option<&[String]>,
        tiers: Option<&[String]>,
        limit: Option<usize>,
        regex: bool,
    ) -> Self {
        Self {
            query: query.to_string(),
            statuses: statuses.map(<[String]>::to_vec),
            issue_types: issue_types.map(<[String]>::to_vec),
            tiers: tiers.map(<[String]>::to_vec),
            limit,
            regex,
        }
    }
}

fn search_in(
    connection: &Connection,
    query: &OwnedSearchQuery,
) -> Result<Vec<BeadSearchMatchWire>, BeadError> {
    use crate::bead::search::{matched_field_names, SearchMatcher};
    parse_statuses(query.statuses.as_deref())?;
    parse_issue_types(query.issue_types.as_deref())?;
    parse_tiers(query.tiers.as_deref())?;
    let matcher = SearchMatcher::new(&query.query, query.regex)?;
    let mut clauses = Vec::new();
    let mut values: Vec<String> = Vec::new();
    push_in_clause(&mut clauses, &mut values, "status", &query.statuses);
    push_in_clause(&mut clauses, &mut values, "issue_type", &query.issue_types);
    push_in_clause(&mut clauses, &mut values, "tier", &query.tiers);
    let filter = if clauses.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", clauses.join(" AND "))
    };
    let mut statement = connection
        .prepare(&format!(
            "SELECT row FROM issues {filter} ORDER BY created_at ASC, position ASC"
        ))
        .map_err(|error| BeadError::io(error.to_string()))?;
    let params: Vec<&dyn rusqlite::ToSql> = values
        .iter()
        .map(|value| value as &dyn rusqlite::ToSql)
        .collect();
    let rows = statement
        .query_map(params.as_slice(), |row| row.get::<_, String>(0))
        .map_err(|error| BeadError::io(error.to_string()))?;
    let mut candidates = Vec::new();
    for row in rows {
        let text: String =
            row.map_err(|error| BeadError::io(error.to_string()))?;
        candidates.push(decode_issue(text).map_err(BeadError::io)?);
    }
    let max = query.limit.unwrap_or(0);
    let mut matches = Vec::new();
    for issue in candidates.into_iter().rev() {
        let matched_fields = matched_field_names(&issue, &matcher);
        if matched_fields.is_empty() {
            continue;
        }
        matches.push(BeadSearchMatchWire {
            issue,
            matched_fields,
        });
        if max > 0 && matches.len() >= max {
            break;
        }
    }
    Ok(matches)
}

/// Closed bead IDs in replay order, or `None` on fallback.
pub fn cached_closed_ids(
    beads_dir: &Path,
) -> Result<Option<Result<Vec<String>, BeadError>>, BeadError> {
    with_fresh_cache(beads_dir, |connection| Ok(closed_ids_in(connection)))
}

fn closed_ids_in(connection: &Connection) -> Result<Vec<String>, BeadError> {
    let mut statement = connection
        .prepare(
            "SELECT id FROM issues WHERE status = 'closed' ORDER BY created_at ASC, position ASC",
        )
        .map_err(|error| BeadError::io(error.to_string()))?;
    let rows = statement
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(|error| BeadError::io(error.to_string()))?;
    let mut ids = Vec::new();
    for row in rows {
        ids.push(row.map_err(|error| BeadError::io(error.to_string()))?);
    }
    Ok(ids)
}

/// One epic's direct children in replay order, or `None` on fallback.
pub fn cached_epic_children(
    beads_dir: &Path,
    epic_id: &str,
) -> Result<Option<Result<Vec<IssueWire>, BeadError>>, BeadError> {
    let epic_id = epic_id.to_string();
    with_fresh_cache(beads_dir, |connection| {
        Ok(children_in(connection, &epic_id))
    })
}

/// Requested IDs mapped to status wire strings, omitting unknown and
/// ambiguous IDs, or `None` on fallback.
///
/// Semantics mirror Python's `bead_statuses_for_project`: exact IDs match
/// first, then unique suffixes; anything else is omitted, never an error.
#[allow(clippy::type_complexity)]
pub fn cached_statuses_for_ids(
    beads_dir: &Path,
    issue_ids: &[String],
) -> Result<Option<Result<BTreeMap<String, String>, BeadError>>, BeadError> {
    let wanted = issue_ids.to_vec();
    with_fresh_cache(beads_dir, |connection| {
        Ok(statuses_for_ids_in(connection, &wanted))
    })
}

fn statuses_for_ids_in(
    connection: &Connection,
    wanted: &[String],
) -> Result<BTreeMap<String, String>, BeadError> {
    let mut statuses = BTreeMap::new();
    for bead_id in wanted {
        if let Some(status) = status_for_id_in(connection, bead_id)? {
            statuses.insert(bead_id.clone(), status);
        }
    }
    Ok(statuses)
}

fn status_for_id_in(
    connection: &Connection,
    bead_id: &str,
) -> Result<Option<String>, BeadError> {
    let mut statement = connection
        .prepare("SELECT status FROM issues WHERE id = ?1")
        .map_err(|error| BeadError::io(error.to_string()))?;
    let exact: Option<String> =
        statement.query_row([bead_id], |row| row.get(0)).ok();
    if exact.is_some() {
        return Ok(exact);
    }
    if bead_id.contains('-') {
        return Ok(None);
    }
    let mut statement = connection
        .prepare(
            "SELECT issue_id FROM suffix_catalog WHERE suffix = ?1 ORDER BY issue_id",
        )
        .map_err(|error| BeadError::io(error.to_string()))?;
    let rows = statement
        .query_map([bead_id], |row| row.get::<_, String>(0))
        .map_err(|error| BeadError::io(error.to_string()))?;
    let mut candidates = Vec::new();
    for row in rows {
        candidates.push(row.map_err(|error| BeadError::io(error.to_string()))?);
    }
    if candidates.len() != 1 {
        return Ok(None);
    }
    let mut statement = connection
        .prepare("SELECT status FROM issues WHERE id = ?1")
        .map_err(|error| BeadError::io(error.to_string()))?;
    Ok(status_statement_query(&mut statement, &candidates[0]))
}

fn status_statement_query(
    statement: &mut rusqlite::Statement<'_>,
    issue_id: &str,
) -> Option<String> {
    statement.query_row([issue_id], |row| row.get(0)).ok()
}

/// Board views from the indexes: the full list plus ready/blocked IDs.
///
/// Shares the indexed `list`/`ready`/`blocked` lanes by construction, so
/// the board still matches the separate queries exactly.
pub fn cached_board(
    beads_dir: &Path,
) -> Result<Option<Result<BoardView, BeadError>>, BeadError> {
    with_fresh_cache(beads_dir, |connection| Ok(board_in(connection)))
}

/// Board rows without the wire wrapper: issues plus ready/blocked IDs.
pub struct BoardView {
    /// Every issue, exactly as an unfiltered `list` returns them.
    pub issues: Vec<IssueWire>,
    /// IDs of the ready beads, in the same order.
    pub ready_ids: Vec<String>,
    /// IDs of the blocked beads, in the same order.
    pub blocked_ids: Vec<String>,
}

fn board_in(connection: &Connection) -> Result<BoardView, BeadError> {
    let unfiltered = OwnedListQuery::new(None, None, None, None, None);
    let page = list_in(connection, &unfiltered)?;
    let ready_ids = ready_ids_in(connection)?;
    let blocked_ids = blocked_ids_in(connection)?;
    Ok(BoardView {
        issues: page.issues,
        ready_ids,
        blocked_ids,
    })
}

/// Lineage stream files holding one bead's history, or `None` when the
/// caller should read every stream instead.
///
/// Each mutation appends to its issue's stream — the issue's own file for
/// plans, the parent's file otherwise, mirroring `stream_id_for_issue` —
/// and both the issue type and the parent link are immutable, so one
/// issue's events always live in one lineage file. The closure starts
/// from the issue's file plus its ancestors' files, then pulls in the
/// files of foreign IDs the loaded events name (dependency targets, for
/// the existence check in `apply_event`, and removal cascades), iterated
/// to a fixpoint. A removal that strips the target's dependencies always
/// names a removed bead, whose missing row falls back: the full replay
/// owns validation-error parity, and the manifest the freshness gate just
/// confirmed keeps the file set trustworthy.
pub fn lineage_history_streams(
    beads_dir: &Path,
    issue_id: &str,
) -> Result<Option<Vec<PathBuf>>, BeadError> {
    use crate::bead::jsonl::event_store_present;
    if !event_store_present(beads_dir) {
        return Ok(None);
    }
    let Some(cache_path) =
        super::location::read_model_cache_path_for_store(beads_dir)
    else {
        return Ok(None);
    };
    if !ensure_cache_ready_at(beads_dir, &cache_path)? {
        return Ok(None);
    }
    let connection = match open_read_only(&cache_path, SERVE_BUSY_TIMEOUT) {
        Ok(connection) => connection,
        Err(_) => return Ok(None),
    };
    match history_lineage_files(&connection, beads_dir, issue_id) {
        Ok(files) => Ok(files),
        Err(_) => {
            drop(connection);
            Ok(None)
        }
    }
}

fn history_lineage_files(
    connection: &Connection,
    beads_dir: &Path,
    issue_id: &str,
) -> Result<Option<Vec<PathBuf>>, String> {
    use crate::bead::jsonl::event_streams_dir;
    let resolved =
        resolve_in(connection, issue_id).map_err(|error| error.message)?;
    if load_issue(connection, &resolved)?.is_none() {
        return Err(format!("Issue not found: {resolved}"));
    }
    let streams_dir = event_streams_dir(beads_dir);
    // Seed closure: the stream files of the issue and every cached
    // ancestor. A missing row or a cycle falls back: without the full
    // chain the file set cannot be proven complete.
    let mut lineage: Vec<String> = vec![resolved.clone()];
    let mut seen = BTreeSet::from([resolved.clone()]);
    let mut parent = parent_of(connection, &resolved)?;
    while let Some(current) = parent {
        if !seen.insert(current.clone()) {
            return Ok(None);
        }
        if load_issue(connection, &current)?.is_none() {
            return Ok(None);
        }
        lineage.push(current.clone());
        parent = parent_of(connection, &current)?;
    }
    let mut files: BTreeSet<String> = BTreeSet::new();
    for id in &lineage {
        let stream_file = stream_file_for(connection, id)?;
        require_stream_file(&streams_dir, &stream_file)?;
        files.insert(stream_file);
    }
    // Reference closure: the stream file of every foreign ID the loaded
    // events name joins the set, iterated to a fixpoint. Parsing uses the
    // same single-validation-point parser as the full read.
    let mut parsed: BTreeSet<String> = BTreeSet::new();
    loop {
        let mut referenced = BTreeSet::new();
        for stream_id in files.clone() {
            if !parsed.insert(stream_id.clone()) {
                continue;
            }
            let path = streams_dir.join(format!("{stream_id}.jsonl"));
            let events = parse_stream_file(&path)?;
            for event in &events {
                collect_event_refs(event, &mut referenced);
            }
        }
        let mut grown = false;
        for foreign_id in referenced {
            if load_issue(connection, &foreign_id)?.is_none() {
                return Ok(None);
            }
            let stream_file = stream_file_for(connection, &foreign_id)?;
            if files.contains(&stream_file) {
                continue;
            }
            require_stream_file(&streams_dir, &stream_file)?;
            files.insert(stream_file);
            grown = true;
        }
        if !grown {
            break;
        }
    }
    Ok(Some(
        files
            .into_iter()
            .map(|stream_id| streams_dir.join(format!("{stream_id}.jsonl")))
            .collect(),
    ))
}

/// The stream file holding one issue's events, mirroring
/// `stream_id_for_issue`: plans own their file, other beads use the live
/// parent's file, and parentless beads own theirs.
fn stream_file_for(
    connection: &Connection,
    issue_id: &str,
) -> Result<String, String> {
    let mut statement = connection
        .prepare("SELECT parent, issue_type FROM issues WHERE id = ?1")
        .map_err(|error| error.to_string())?;
    let (parent, issue_type): (String, String) = statement
        .query_row([issue_id], |row| Ok((row.get(0)?, row.get(1)?)))
        .map_err(|error| error.to_string())?;
    if issue_type == "plan" || parent.is_empty() {
        Ok(issue_id.to_string())
    } else {
        Ok(parent)
    }
}

fn require_stream_file(
    streams_dir: &Path,
    stream_file: &str,
) -> Result<(), String> {
    if streams_dir.join(format!("{stream_file}.jsonl")).is_file() {
        Ok(())
    } else {
        Err(format!("lineage stream file is missing: {stream_file}"))
    }
}

fn parent_of(
    connection: &Connection,
    issue_id: &str,
) -> Result<Option<String>, String> {
    let mut statement = connection
        .prepare("SELECT parent FROM issues WHERE id = ?1")
        .map_err(|error| error.to_string())?;
    let parent: Option<String> = statement
        .query_row([issue_id], |row| row.get(0))
        .map_err(|error| error.to_string())?;
    Ok(parent.filter(|value| !value.is_empty()))
}

fn parse_stream_file(
    path: &Path,
) -> Result<Vec<crate::bead::events::BeadEventRecordWire>, String> {
    use crate::bead::jsonl::{
        classify_flag_stream, parse_event_stream_bytes, FlagStreamKind,
    };
    let stream_id = path
        .file_stem()
        .and_then(|name| name.to_str())
        .unwrap_or_default()
        .to_string();
    let contents = std::fs::read(path).map_err(|error| error.to_string())?;
    match parse_event_stream_bytes(path, &contents) {
        Ok(events) => Ok(events),
        Err(error) => match classify_flag_stream(path) {
            Ok(FlagStreamKind::RemovedFlag) => Ok(Vec::new()),
            Ok(FlagStreamKind::LiveFlag) => Err(format!(
                "bead event store still has live flag issue-type streams: {stream_id}; migrate or remove them before loading"
            )),
            Ok(FlagStreamKind::Other) | Err(_) => Err(error.message),
        }
    }
}

fn collect_event_refs(
    event: &crate::bead::events::BeadEventRecordWire,
    referenced: &mut BTreeSet<String>,
) {
    use crate::bead::events::BeadEventPayloadWire;
    referenced.insert(event.issue_id.clone());
    match &event.payload {
        BeadEventPayloadWire::DependencyAdded { dependency } => {
            referenced.insert(dependency.depends_on_id.clone());
        }
        BeadEventPayloadWire::IssueRemoved {
            cascade_removed_issue_ids,
        } => {
            referenced.extend(cascade_removed_issue_ids.iter().cloned());
        }
        _ => {}
    }
}
