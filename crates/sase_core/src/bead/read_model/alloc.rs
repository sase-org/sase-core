//! Disposable versioned allocation metadata for bead IDs.
//!
//! Top-level allocation is `max(config.next_counter, next valid base36
//! top-level ID for that prefix)`. Child allocation matches
//! `store.rs::next_child_id`: textual `<parent>.` prefix with a direct
//! numeric suffix, regardless of a row's `parent_id` field. The prototype
//! `WHERE parent = ?1` scan is therefore insufficient (mismatched parent
//! fields, nested IDs).
//!
//! Tables are rebuilt from the full reduced issue list and updated
//! transactionally on tail refresh and local writes, including
//! removals/collapse and config changes. Older caches rebuild via the
//! disposable schema version bump; no event migration is needed.

use std::collections::BTreeMap;

use crate::bead::wire::BeadError;

/// Parse a top-level ID `"<prefix>-<suffix>"` with no `.` in the suffix.
///
/// Returns `(prefix, base36 counter)` when the suffix decodes. Mirrors
/// `store.rs::max_top_level_counter` exactly: only the suffix must not
/// contain `.` (a `.` elsewhere, e.g. in a dotted prefix, does not
/// disqualify the ID), and decoding is base36.
pub fn top_prefix_and_counter(issue_id: &str) -> Option<(String, u64)> {
    let dash = issue_id.rfind('-')?;
    let prefix = &issue_id[..dash];
    let suffix = &issue_id[dash + 1..];
    if prefix.is_empty() || suffix.is_empty() {
        return None;
    }
    if suffix.contains('.') {
        return None;
    }
    let counter = u64::from_str_radix(suffix, 36).ok()?;
    Some((prefix.to_string(), counter))
}

/// Parse a child ID `"<parent>.<suffix>"` with a direct numeric suffix.
///
/// Returns `(parent, suffix)` when the text after the final `.` parses as
/// decimal with no further `.`. Mirrors `store.rs::direct_child_counter`
/// exactly: the check is textual, regardless of the row's `parent_id`.
pub fn child_parent_and_suffix(issue_id: &str) -> Option<(String, u64)> {
    let dot = issue_id.rfind('.')?;
    let parent = &issue_id[..dot];
    let suffix = &issue_id[dot + 1..];
    if parent.is_empty() || suffix.is_empty() {
        return None;
    }
    if suffix.contains('.') {
        return None;
    }
    let counter: u64 = suffix.parse().ok()?;
    Some((parent.to_string(), counter))
}

/// Compute per-prefix top-level maxima from an ID list.
///
/// Only IDs whose suffix (after the final `-`) has no `.` and base36-decodes
/// contribute, exactly as the replay oracle does. Malformed IDs, nested IDs
/// (`.` in the suffix), and multiple prefixes are handled by ignoring
/// non-matching IDs.
pub fn top_maxima<I, S>(ids: I) -> BTreeMap<String, u64>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut maxima: BTreeMap<String, u64> = BTreeMap::new();
    for id in ids {
        if let Some((prefix, counter)) = top_prefix_and_counter(id.as_ref()) {
            maxima
                .entry(prefix)
                .and_modify(|max| *max = (*max).max(counter))
                .or_insert(counter);
        }
    }
    maxima
}

/// Compute per-parent child maxima from an ID list.
///
/// Only IDs with a textual `<parent>.` prefix and direct decimal suffix
/// contribute, regardless of any row's `parent_id` field. Mismatched
/// parent fields, malformed IDs, nested IDs (suffix containing `.`),
/// and empty stores are handled by ignoring non-matching IDs.
pub fn child_maxima<I, S>(ids: I) -> BTreeMap<String, u64>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut maxima: BTreeMap<String, u64> = BTreeMap::new();
    for id in ids {
        if let Some((parent, counter)) = child_parent_and_suffix(id.as_ref()) {
            maxima
                .entry(parent)
                .and_modify(|max| *max = (*max).max(counter))
                .or_insert(counter);
        }
    }
    maxima
}

/// Ensure the allocation tables exist.
pub fn ensure_alloc_schema(
    connection: &rusqlite::Connection,
) -> Result<(), BeadError> {
    connection
        .execute_batch(
            "
            CREATE TABLE IF NOT EXISTS alloc_top (
                prefix TEXT PRIMARY KEY,
                max_counter INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS alloc_child (
                parent_id TEXT PRIMARY KEY,
                max_suffix INTEGER NOT NULL
            );
            ",
        )
        .map_err(|error| BeadError::io(error.to_string()))
}

/// Rebuild allocation metadata from a complete ID list.
///
/// Called during a full rebuild inside the same transaction that writes
/// rows. Clears both tables first so stale prefixes/parents disappear.
pub fn rebuild_allocation<I, S>(
    connection: &rusqlite::Connection,
    ids: I,
) -> Result<(), BeadError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let ids: Vec<String> =
        ids.into_iter().map(|id| id.as_ref().to_string()).collect();
    let tops = top_maxima(&ids);
    let children = child_maxima(&ids);
    connection
        .execute("DELETE FROM alloc_top", [])
        .map_err(|error| BeadError::io(error.to_string()))?;
    connection
        .execute("DELETE FROM alloc_child", [])
        .map_err(|error| BeadError::io(error.to_string()))?;
    {
        let mut top_stmt = connection
            .prepare(
                "INSERT INTO alloc_top (prefix, max_counter) VALUES (?1, ?2)",
            )
            .map_err(|error| BeadError::io(error.to_string()))?;
        for (prefix, max_counter) in &tops {
            top_stmt
                .execute(rusqlite::params![prefix, *max_counter as i64])
                .map_err(|error| BeadError::io(error.to_string()))?;
        }
    }
    {
        let mut child_stmt = connection
            .prepare(
                "INSERT INTO alloc_child (parent_id, max_suffix) VALUES (?1, ?2)",
            )
            .map_err(|error| BeadError::io(error.to_string()))?;
        for (parent_id, max_suffix) in &children {
            child_stmt
                .execute(rusqlite::params![parent_id, *max_suffix as i64])
                .map_err(|error| BeadError::io(error.to_string()))?;
        }
    }
    Ok(())
}

/// Stored top-level maximum for one prefix, if any.
pub fn stored_top_max(
    connection: &rusqlite::Connection,
    prefix: &str,
) -> Result<Option<u64>, BeadError> {
    let mut stmt = connection
        .prepare("SELECT max_counter FROM alloc_top WHERE prefix = ?1")
        .map_err(|error| BeadError::io(error.to_string()))?;
    let mut rows = stmt
        .query([prefix])
        .map_err(|error| BeadError::io(error.to_string()))?;
    match rows
        .next()
        .map_err(|error| BeadError::io(error.to_string()))?
    {
        Some(row) => {
            let value: i64 = row
                .get(0)
                .map_err(|error| BeadError::io(error.to_string()))?;
            Ok(Some(value.max(0) as u64))
        }
        None => Ok(None),
    }
}

/// Stored child maximum for one parent, if any.
pub fn stored_child_max(
    connection: &rusqlite::Connection,
    parent_id: &str,
) -> Result<Option<u64>, BeadError> {
    let mut stmt = connection
        .prepare("SELECT max_suffix FROM alloc_child WHERE parent_id = ?1")
        .map_err(|error| BeadError::io(error.to_string()))?;
    let mut rows = stmt
        .query([parent_id])
        .map_err(|error| BeadError::io(error.to_string()))?;
    match rows
        .next()
        .map_err(|error| BeadError::io(error.to_string()))?
    {
        Some(row) => {
            let value: i64 = row
                .get(0)
                .map_err(|error| BeadError::io(error.to_string()))?;
            Ok(Some(value.max(0) as u64))
        }
        None => Ok(None),
    }
}

/// Record one created ID in the allocation tables.
///
/// Only raises stored maxima; never lowers them. Removals use
/// [`note_delete`] so removing the maximum recomputes from survivors.
pub fn note_upsert(
    connection: &rusqlite::Connection,
    issue_id: &str,
) -> Result<(), BeadError> {
    if let Some((prefix, counter)) = top_prefix_and_counter(issue_id) {
        let current = stored_top_max(connection, &prefix)?.unwrap_or(0);
        if counter > current {
            connection
                .execute(
                    "INSERT INTO alloc_top (prefix, max_counter) VALUES (?1, ?2) ON CONFLICT(prefix) DO UPDATE SET max_counter = excluded.max_counter",
                    rusqlite::params![prefix, counter as i64],
                )
                .map_err(|error| BeadError::io(error.to_string()))?;
        }
    }
    if let Some((parent, counter)) = child_parent_and_suffix(issue_id) {
        let current = stored_child_max(connection, &parent)?.unwrap_or(0);
        if counter > current {
            connection
                .execute(
                    "INSERT INTO alloc_child (parent_id, max_suffix) VALUES (?1, ?2) ON CONFLICT(parent_id) DO UPDATE SET max_suffix = excluded.max_suffix",
                    rusqlite::params![parent, counter as i64],
                )
                .map_err(|error| BeadError::io(error.to_string()))?;
        }
    }
    Ok(())
}

/// Record one removed ID, recomputing the affected maximum when needed.
///
/// When the removed ID was the stored maximum for its prefix/parent,
/// the new maximum is recomputed with a prefix-scoped index range scan
/// over surviving rows — never a full-store scan and never `LIKE` (which
/// cannot use the BINARY primary-key index and mis-handles `%`/`_`).
/// Removing a non-maximum leaves the tables untouched, preserving the
/// replay oracle's reuse behavior: the next allocation reuses a freed
/// maximum suffix exactly as `store.rs::next_child_id` would.
pub fn note_delete(
    connection: &rusqlite::Connection,
    issue_id: &str,
) -> Result<(), BeadError> {
    if let Some((prefix, counter)) = top_prefix_and_counter(issue_id) {
        let current = stored_top_max(connection, &prefix)?.unwrap_or(0);
        if counter == current && counter > 0 {
            let lower = format!("{prefix}-");
            let upper = format!("{prefix}.");
            let mut stmt = connection
                .prepare("SELECT id FROM issues WHERE id >= ?1 AND id < ?2")
                .map_err(|error| BeadError::io(error.to_string()))?;
            let mapped = stmt
                .query_map([lower, upper], |row| row.get::<_, String>(0))
                .map_err(|error| BeadError::io(error.to_string()))?;
            let mut new_max: u64 = 0;
            for row in mapped {
                let id: String =
                    row.map_err(|error| BeadError::io(error.to_string()))?;
                if let Some((candidate_prefix, candidate)) =
                    top_prefix_and_counter(&id)
                {
                    if candidate_prefix == prefix {
                        new_max = new_max.max(candidate);
                    }
                }
            }
            if new_max == 0 {
                connection
                    .execute(
                        "DELETE FROM alloc_top WHERE prefix = ?1",
                        [prefix],
                    )
                    .map_err(|error| BeadError::io(error.to_string()))?;
            } else {
                connection
                    .execute(
                        "INSERT INTO alloc_top (prefix, max_counter) VALUES (?1, ?2) ON CONFLICT(prefix) DO UPDATE SET max_counter = excluded.max_counter",
                        rusqlite::params![prefix, new_max as i64],
                    )
                    .map_err(|error| BeadError::io(error.to_string()))?;
            }
        }
    }
    if let Some((parent, counter)) = child_parent_and_suffix(issue_id) {
        let current = stored_child_max(connection, &parent)?.unwrap_or(0);
        if counter == current && counter > 0 {
            let lower = format!("{parent}.");
            let upper = format!("{parent}/");
            let mut stmt = connection
                .prepare("SELECT id FROM issues WHERE id >= ?1 AND id < ?2")
                .map_err(|error| BeadError::io(error.to_string()))?;
            let mapped = stmt
                .query_map([lower, upper], |row| row.get::<_, String>(0))
                .map_err(|error| BeadError::io(error.to_string()))?;
            let mut new_max: u64 = 0;
            for row in mapped {
                let id: String =
                    row.map_err(|error| BeadError::io(error.to_string()))?;
                if let Some((candidate_parent, candidate)) =
                    child_parent_and_suffix(&id)
                {
                    if candidate_parent == parent {
                        new_max = new_max.max(candidate);
                    }
                }
            }
            if new_max == 0 {
                connection
                    .execute(
                        "DELETE FROM alloc_child WHERE parent_id = ?1",
                        [parent],
                    )
                    .map_err(|error| BeadError::io(error.to_string()))?;
            } else {
                connection
                    .execute(
                        "INSERT INTO alloc_child (parent_id, max_suffix) VALUES (?1, ?2) ON CONFLICT(parent_id) DO UPDATE SET max_suffix = excluded.max_suffix",
                        rusqlite::params![parent, new_max as i64],
                    )
                    .map_err(|error| BeadError::io(error.to_string()))?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        child_maxima, child_parent_and_suffix, top_maxima,
        top_prefix_and_counter,
    };

    #[test]
    fn top_level_parsing_matches_replay_oracle() {
        assert_eq!(
            top_prefix_and_counter("sase-1"),
            Some(("sase".to_string(), 1))
        );
        assert_eq!(top_prefix_and_counter("sase-1.2"), None);
        assert_eq!(top_prefix_and_counter("bad"), None);
        assert_eq!(top_prefix_and_counter("sase-zz!"), None);
    }

    #[test]
    fn child_parsing_is_textual_not_parent_field() {
        assert_eq!(
            child_parent_and_suffix("sase-1.2"),
            Some(("sase-1".to_string(), 2))
        );
        assert_eq!(child_parent_and_suffix("a.b.c"), None);
        assert_eq!(child_parent_and_suffix("sase-1"), None);
        assert_eq!(child_parent_and_suffix("sase-1.x"), None);
    }

    #[test]
    fn maxima_ignore_malformed_nested_and_empty() {
        let ids = vec![
            "sase-1",
            "sase-2",
            "sase-1.1",
            "sase-1.2",
            "sase-1.2.3",
            "bad",
            "other-1",
        ];
        let tops = top_maxima(&ids);
        assert_eq!(tops.get("sase"), Some(&2));
        assert_eq!(tops.get("other"), Some(&1));
        let children = child_maxima(&ids);
        assert_eq!(children.get("sase-1"), Some(&2));
        assert_eq!(children.get("sase-1.2"), Some(&3));
        assert!(top_maxima(Vec::<String>::new()).is_empty());
    }
}
