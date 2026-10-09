//! Meta-key helpers shared by cache healing and direct publication.
//!
//! Pure move from `store.rs`: the default key table, the pre-tail
//! backfill, and the post-write token recorder. No behavior change.

use rusqlite::Connection;

use super::store::{
    crate_version, Fault, WriteFault, READ_MODEL_REDUCER_VERSION,
    READ_MODEL_SCHEMA_VERSION,
};

pub(super) fn tail_meta_defaults() -> [(&'static str, String); 16] {
    [
        ("schema_version", READ_MODEL_SCHEMA_VERSION.to_string()),
        ("reducer_version", READ_MODEL_REDUCER_VERSION.to_string()),
        ("crate_version", crate_version()),
        ("token", String::new()),
        ("last_sweep_ns", "0".to_string()),
        ("frontier", String::new()),
        ("generation", "0".to_string()),
        ("content_generation", "0".to_string()),
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
pub(super) fn backfill_meta_keys(connection: &Connection) -> Result<(), Fault> {
    let defaults = tail_meta_defaults();
    let placeholders =
        defaults.iter().map(|_| "?").collect::<Vec<_>>().join(",");
    let mut params: Vec<&dyn rusqlite::ToSql> = Vec::new();
    for (key, _) in &defaults {
        params.push(key);
    }
    let present: i64 = connection
        .query_row(
            &format!("SELECT COUNT(*) FROM meta WHERE key IN ({placeholders})"),
            params.as_slice(),
            |row| row.get(0),
        )
        .map_err(|_| Fault::Cache)?;
    if present as usize >= defaults.len() {
        return Ok(());
    }
    for (key, value) in &defaults {
        connection
            .execute(
                "INSERT INTO meta (key, value) VALUES (?1, ?2) ON CONFLICT(key) DO NOTHING",
                rusqlite::params![key, value],
            )
            .map_err(|_| Fault::Cache)?;
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
        .map_err(|_| WriteFault::Other)?;
    Ok(())
}
