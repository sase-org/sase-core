//! Catalog JSONL parsing, revision check, and overlay shape validation.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;
use std::sync::LazyLock;
use std::time::Duration;

use chrono::NaiveDate;
use regex::Regex;
use rusqlite::{params, OptionalExtension};
use serde_json::Value;
use sha2::{Digest, Sha256};

use super::spool::feature_id_valid;
use super::store::{bounded_busy_timeout, open_read_store, with_write};
use super::wire::{
    ns_of, require_member, CatalogEntryWire, CatalogHeaderWire,
    CatalogStatusWire, CATALOG_SCHEMA, DAY_PATTERN,
    FOOTPRINT_WIRE_SCHEMA_VERSION, KEEP_REASONS, KINDS, OVERLAY_SCHEMA,
    PROTECTED_CLASSES,
};
use super::FootprintError;

static DAY_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(DAY_PATTERN).expect("day regex"));

/// Parsed catalog file with a verified revision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogFile {
    pub header: CatalogHeaderWire,
    pub entries: Vec<CatalogEntryWire>,
}

/// Return whether `rev` from `catalog_path` is already ingested.
pub fn catalog_status(
    store_path: &Path,
    catalog_path: &Path,
    busy_timeout: Duration,
) -> Result<CatalogStatusWire, FootprintError> {
    let catalog = parse_catalog(catalog_path)?;
    let timeout = bounded_busy_timeout(busy_timeout);
    if !store_path.exists() {
        return Ok(CatalogStatusWire {
            schema_version: FOOTPRINT_WIRE_SCHEMA_VERSION,
            rev: catalog.header.rev,
            known: false,
            count: catalog.header.count,
        });
    }
    let conn = open_read_store(store_path, timeout)?;
    let known: bool = conn
        .query_row(
            "SELECT 1 FROM catalog_revs WHERE rev = ?1",
            params![catalog.header.rev],
            |_| Ok(true),
        )
        .optional()
        .map_err(FootprintError::from_sqlite)?
        .unwrap_or(false);
    Ok(CatalogStatusWire {
        schema_version: FOOTPRINT_WIRE_SCHEMA_VERSION,
        rev: catalog.header.rev,
        known,
        count: catalog.header.count,
    })
}

/// Upsert catalog features and record the revision.
pub fn ingest_catalog(
    store_path: &Path,
    catalog_path: &Path,
    enrichment: &BTreeMap<String, i64>,
    today: &str,
    busy_timeout: Duration,
) -> Result<CatalogStatusWire, FootprintError> {
    let today = parse_day(today)?;
    let catalog = parse_catalog(catalog_path)?;
    let timeout = bounded_busy_timeout(busy_timeout);
    with_write(store_path, timeout, |conn| {
        let tx = conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(FootprintError::from_sqlite)?;
        let today_s = today.to_string();
        for entry in &catalog.entries {
            let loc = enrichment.get(&entry.id).copied();
            tx.execute(
                r#"
                INSERT INTO features(
                    feature_id, ns, area, kind, label, parent, src, loc,
                    first_in_catalog, last_in_catalog
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
                ON CONFLICT(feature_id) DO UPDATE SET
                    ns = excluded.ns,
                    area = excluded.area,
                    kind = excluded.kind,
                    label = excluded.label,
                    parent = excluded.parent,
                    src = excluded.src,
                    loc = COALESCE(excluded.loc, features.loc),
                    last_in_catalog = excluded.last_in_catalog
                "#,
                params![
                    entry.id,
                    entry.ns,
                    entry.area,
                    entry.kind,
                    entry.label,
                    entry.parent,
                    entry.src,
                    loc,
                    today_s,
                    today_s,
                ],
            )
            .map_err(FootprintError::from_sqlite)?;
        }
        let sase_version =
            catalog.header.sase_version.clone().unwrap_or_default();
        let prior: Option<(Option<String>, i64)> = tx
            .query_row(
                "SELECT first_seen_day, entry_count FROM catalog_revs WHERE rev = ?1",
                params![catalog.header.rev],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(FootprintError::from_sqlite)?;
        let first_seen = prior
            .as_ref()
            .and_then(|(day, _)| day.clone())
            .filter(|day| day.as_str() <= today_s.as_str())
            .unwrap_or_else(|| today_s.clone());
        tx.execute(
            r#"
            INSERT INTO catalog_revs(
                rev, sase_version, entry_count, first_seen_day, last_seen_day
            ) VALUES (?1, ?2, ?3, ?4, ?5)
            ON CONFLICT(rev) DO UPDATE SET
                sase_version = excluded.sase_version,
                entry_count = excluded.entry_count,
                last_seen_day = excluded.last_seen_day
            "#,
            params![
                catalog.header.rev,
                sase_version,
                catalog.header.count as i64,
                first_seen,
                today_s,
            ],
        )
        .map_err(FootprintError::from_sqlite)?;
        tx.execute(
            "INSERT OR REPLACE INTO meta(key, value) VALUES ('catalog_rev', ?1)",
            params![catalog.header.rev],
        )
        .map_err(FootprintError::from_sqlite)?;
        tx.execute(
            "INSERT OR REPLACE INTO meta(key, value) VALUES ('catalog_count', ?1)",
            params![catalog.header.count.to_string()],
        )
        .map_err(FootprintError::from_sqlite)?;
        tx.commit().map_err(FootprintError::from_sqlite)?;
        Ok(())
    })?;
    Ok(CatalogStatusWire {
        schema_version: FOOTPRINT_WIRE_SCHEMA_VERSION,
        rev: catalog.header.rev,
        known: true,
        count: catalog.header.count,
    })
}

/// Parse a catalog JSONL file and verify the header revision.
pub fn parse_catalog(
    catalog_path: &Path,
) -> Result<CatalogFile, FootprintError> {
    let bytes = fs::read(catalog_path)?;
    let text = std::str::from_utf8(&bytes).map_err(|error| {
        FootprintError::validation(format!("catalog is not utf-8: {error}"))
    })?;
    let mut lines = text.split('\n');
    let header_line = lines
        .next()
        .ok_or_else(|| FootprintError::validation("catalog is empty"))?;
    if header_line.is_empty() {
        return Err(FootprintError::validation("catalog is empty"));
    }
    let header: CatalogHeaderWire =
        serde_json::from_str(header_line).map_err(|error| {
            FootprintError::validation(format!(
                "catalog header is invalid: {error}"
            ))
        })?;
    if header.schema != CATALOG_SCHEMA {
        return Err(FootprintError::validation(format!(
            "catalog schema must be {CATALOG_SCHEMA}, got {}",
            header.schema
        )));
    }
    let remainder_start = header_line.len() + 1;
    let remainder = if remainder_start <= bytes.len() {
        &bytes[remainder_start..]
    } else {
        &[]
    };
    let computed = catalog_rev(remainder);
    if header.rev != computed {
        return Err(FootprintError::validation(format!(
            "catalog rev {} does not match computed {computed}",
            header.rev
        )));
    }
    let mut entries = Vec::new();
    let mut seen = BTreeSet::new();
    for (index, line) in lines.enumerate() {
        if line.is_empty() {
            continue;
        }
        let entry: CatalogEntryWire =
            serde_json::from_str(line).map_err(|error| {
                FootprintError::validation(format!(
                    "catalog entry {} is invalid: {error}",
                    index + 1
                ))
            })?;
        validate_entry(&entry)?;
        if !seen.insert(entry.id.clone()) {
            return Err(FootprintError::validation(format!(
                "duplicate catalog id {}",
                entry.id
            )));
        }
        entries.push(entry);
    }
    if header.count != entries.len() as u64 {
        return Err(FootprintError::validation(format!(
            "catalog count {} does not match {} entries",
            header.count,
            entries.len()
        )));
    }
    Ok(CatalogFile { header, entries })
}

/// First 12 hex characters of sha256 over the canonical entry bytes.
pub fn catalog_rev(entry_bytes: &[u8]) -> String {
    let digest = Sha256::digest(entry_bytes);
    hex::encode(&digest[..6])
}

/// Validate overlay JSON produced from YAML by Python.
pub fn validate_overlay_shape(overlay: &Value) -> Result<(), FootprintError> {
    let object = overlay.as_object().ok_or_else(|| {
        FootprintError::validation("overlay must be an object")
    })?;
    let schema =
        object
            .get("schema")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                FootprintError::validation("overlay.schema is required")
            })?;
    if schema != OVERLAY_SCHEMA {
        return Err(FootprintError::validation(format!(
            "overlay schema must be {OVERLAY_SCHEMA}, got {schema}"
        )));
    }
    for key in object.keys() {
        if !matches!(key.as_str(), "schema" | "features" | "retire_with") {
            return Err(FootprintError::validation(format!(
                "overlay has unknown key {key}"
            )));
        }
    }
    if let Some(features) = object.get("features") {
        let features = features.as_object().ok_or_else(|| {
            FootprintError::validation("overlay.features must be an object")
        })?;
        for (key, value) in features {
            validate_overlay_key(key)?;
            validate_overlay_feature(value, key)?;
        }
    }
    if let Some(retire_with) = object.get("retire_with") {
        let groups = retire_with.as_array().ok_or_else(|| {
            FootprintError::validation("overlay.retire_with must be an array")
        })?;
        for (index, group) in groups.iter().enumerate() {
            let ids = group.as_array().ok_or_else(|| {
                FootprintError::validation(format!(
                    "overlay.retire_with[{index}] must be an array of ids"
                ))
            })?;
            if ids.len() < 2 {
                return Err(FootprintError::validation(format!(
                    "overlay.retire_with[{index}] needs at least two ids"
                )));
            }
            for id in ids {
                let id = id.as_str().ok_or_else(|| {
                    FootprintError::validation(format!(
                        "overlay.retire_with[{index}] entries must be strings"
                    ))
                })?;
                if !feature_id_valid(id) {
                    return Err(FootprintError::validation(format!(
                        "overlay.retire_with[{index}] has invalid id {id}"
                    )));
                }
            }
        }
    }
    Ok(())
}

fn validate_overlay_key(key: &str) -> Result<(), FootprintError> {
    if let Some(prefix) = key.strip_suffix('*') {
        if prefix.is_empty() {
            return Err(FootprintError::validation(
                "overlay glob must have a non-empty prefix",
            ));
        }
        if prefix.contains('*') {
            return Err(FootprintError::validation(format!(
                "overlay glob {key} may only use a trailing *"
            )));
        }
        Ok(())
    } else if feature_id_valid(key) {
        Ok(())
    } else {
        Err(FootprintError::validation(format!(
            "overlay feature key {key} is not an id or trailing-* glob"
        )))
    }
}

fn validate_overlay_feature(
    value: &Value,
    key: &str,
) -> Result<(), FootprintError> {
    let object = value.as_object().ok_or_else(|| {
        FootprintError::validation(format!(
            "overlay.features[{key}] must be an object"
        ))
    })?;
    for field in object.keys() {
        if !matches!(field.as_str(), "protected" | "keep" | "why") {
            return Err(FootprintError::validation(format!(
                "overlay.features[{key}] has unknown field {field}"
            )));
        }
    }
    if let Some(protected) = object.get("protected") {
        let protected = protected.as_str().ok_or_else(|| {
            FootprintError::validation(format!(
                "overlay.features[{key}].protected must be a string"
            ))
        })?;
        require_member(protected, PROTECTED_CLASSES, "protected")?;
    }
    if let Some(keep) = object.get("keep") {
        let keep = keep.as_str().ok_or_else(|| {
            FootprintError::validation(format!(
                "overlay.features[{key}].keep must be a string"
            ))
        })?;
        require_member(keep, KEEP_REASONS, "keep")?;
    }
    if let Some(why) = object.get("why") {
        if !why.is_string() {
            return Err(FootprintError::validation(format!(
                "overlay.features[{key}].why must be a string"
            )));
        }
    }
    Ok(())
}

fn validate_entry(entry: &CatalogEntryWire) -> Result<(), FootprintError> {
    if !feature_id_valid(&entry.id) {
        return Err(FootprintError::validation(format!(
            "catalog id {} is invalid",
            entry.id
        )));
    }
    let ns = ns_of(&entry.id).unwrap_or("");
    if ns != entry.ns {
        return Err(FootprintError::validation(format!(
            "catalog id {} ns {} does not match field {}",
            entry.id, ns, entry.ns
        )));
    }
    require_member(&entry.kind, KINDS, "kind")?;
    Ok(())
}

pub(crate) fn valid_day(day: &str) -> bool {
    if !DAY_RE.is_match(day) {
        return false;
    }
    NaiveDate::parse_from_str(day, "%Y-%m-%d").is_ok()
}

pub(crate) fn parse_day(day: &str) -> Result<NaiveDate, FootprintError> {
    NaiveDate::parse_from_str(day, "%Y-%m-%d")
        .map_err(|_| FootprintError::validation(format!("invalid day {day}")))
}

pub(crate) fn known_feature_ids(
    conn: &rusqlite::Connection,
) -> Result<BTreeSet<String>, FootprintError> {
    let mut stmt = conn
        .prepare("SELECT feature_id FROM features")
        .map_err(FootprintError::from_sqlite)?;
    let ids = stmt
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(FootprintError::from_sqlite)?;
    let mut set = BTreeSet::new();
    for id in ids {
        set.insert(id.map_err(FootprintError::from_sqlite)?);
    }
    Ok(set)
}
