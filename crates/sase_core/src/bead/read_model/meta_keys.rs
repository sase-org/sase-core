//! Meta-key helpers shared by cache healing and direct publication.
//!
//! Pure move from `store.rs`: the default key table, the pre-tail
//! backfill, and the post-write token recorder. No behavior change.

use rusqlite::Connection;

use super::store::{
    crate_version, WriteFault, READ_MODEL_REDUCER_VERSION,
    READ_MODEL_SCHEMA_VERSION,
};

pub(super) fn tail_meta_defaults() -> [(&'static str, String); 17] {
    [
        ("schema_version", READ_MODEL_SCHEMA_VERSION.to_string()),
        ("reducer_version", READ_MODEL_REDUCER_VERSION.to_string()),
        ("crate_version", crate_version()),
        ("token", String::new()),
        ("last_sweep_ns", "0".to_string()),
        ("frontier", String::new()),
        ("generation", "0".to_string()),
        ("content_generation", "0".to_string()),
        ("invalidated_generation", "0".to_string()),
        ("manifest_schema_version", "0".to_string()),
        ("manifest_stream_count", "0".to_string()),
        ("config_canonical", String::new()),
        ("outcome_serve", "0".to_string()),
        ("outcome_tail", "0".to_string()),
        ("outcome_rebuild", "0".to_string()),
        ("last_refresh", String::new()),
        ("last_refresh_reason", String::new()),
    ]
}

/// Heal pre-tail caches: insert any missing meta key without touching the
/// rows, counters, or generation. Runs on every read before the freshness
/// decision, so a cache written by `read-model-store` gains tail support
/// on its next read instead of rebuilding.
pub(super) fn backfill_meta_keys(
    connection: &Connection,
) -> Result<(), WriteFault> {
    let defaults = tail_meta_defaults();
    let mut present = std::collections::BTreeSet::new();
    let mut statement = connection
        .prepare("SELECT key FROM meta NOT INDEXED")
        .map_err(WriteFault::from_sqlite)?;
    let rows = statement
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(WriteFault::from_sqlite)?;
    for row in rows {
        present.insert(row.map_err(WriteFault::from_sqlite)?);
    }
    drop(statement);
    if defaults.iter().all(|(key, _)| present.contains(*key)) {
        return Ok(());
    }
    for (key, value) in &defaults {
        if present.contains(*key) {
            continue;
        }
        connection
            .execute(
                "INSERT INTO meta (key, value) VALUES (?1, ?2) ON CONFLICT(key) DO NOTHING",
                rusqlite::params![key, value],
            )
            .map_err(WriteFault::from_sqlite)?;
    }
    Ok(())
}

/// Record the post-write freshness token without moving the sweep time.
///
/// Direct publication recomputes the token after its own append: the
/// stored token must describe the published files so the next read
/// serves token-only, while `last_sweep_ns` stays at the admission
/// sweep so out-of-band writers stay bounded by the 60 s rule.
pub(super) fn set_token_in_txn(
    connection: &Connection,
    token: &str,
) -> Result<(), WriteFault> {
    connection
        .execute("UPDATE meta SET value = ?1 WHERE key = 'token'", [token])
        .map_err(WriteFault::from_sqlite)?;
    Ok(())
}
