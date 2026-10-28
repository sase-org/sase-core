//! Replace-all import of identity-free weekly packs.

use std::collections::BTreeSet;
use std::path::Path;
use std::time::Duration;

use rusqlite::{params, Transaction};

use super::catalog::parse_day;
use super::export::{bucket_min, exportable_feature};
use super::store::{begin_immediate, bounded_busy_timeout, open_write_store};
use super::week::parse_week;
use super::wire::{
    ns_of, require_member, ImportResultWire, PackWire, ACTORS, EXPORT_OS,
    EXPORT_SCHEMA, FOOTPRINT_WIRE_SCHEMA_VERSION, N_BUCKETS, SURFACES,
};
use super::FootprintError;

/// Replace the imported tables with `packs`, deduping by nonce.
pub fn import(
    store_path: &Path,
    packs: &[PackWire],
    today: &str,
    dry_run: bool,
    busy_timeout: Duration,
) -> Result<ImportResultWire, FootprintError> {
    let today = parse_day(today)?;
    let timeout = bounded_busy_timeout(busy_timeout);
    let mut validated = Vec::new();
    let mut seen = BTreeSet::new();
    let mut deduped = 0u64;
    for pack in packs {
        validate_pack(pack)?;
        if !seen.insert(pack.nonce.clone()) {
            deduped += 1;
            continue;
        }
        validated.push(pack);
    }
    let feature_rows = validated
        .iter()
        .map(|pack| pack.features.len() as u64)
        .sum();
    if dry_run {
        return Ok(ImportResultWire {
            schema_version: FOOTPRINT_WIRE_SCHEMA_VERSION,
            packs_imported: validated.len() as u64,
            packs_deduped: deduped,
            feature_rows,
            dry_run: true,
        });
    }
    let mut conn = open_write_store(store_path, timeout)?;
    let tx = begin_immediate(&mut conn)?;
    tx.execute("DELETE FROM imported_weekly", [])
        .map_err(FootprintError::from_sqlite)?;
    tx.execute("DELETE FROM imported_observed", [])
        .map_err(FootprintError::from_sqlite)?;
    tx.execute("DELETE FROM import_packs", [])
        .map_err(FootprintError::from_sqlite)?;
    for pack in &validated {
        insert_pack(&tx, pack, &today.to_string())?;
    }
    tx.commit().map_err(FootprintError::from_sqlite)?;
    Ok(ImportResultWire {
        schema_version: FOOTPRINT_WIRE_SCHEMA_VERSION,
        packs_imported: validated.len() as u64,
        packs_deduped: deduped,
        feature_rows,
        dry_run: false,
    })
}

fn validate_pack(pack: &PackWire) -> Result<(), FootprintError> {
    if pack.schema != EXPORT_SCHEMA {
        return Err(FootprintError::validation(format!(
            "invalid export schema {:?}",
            pack.schema
        )));
    }
    if pack.nonce.is_empty()
        || pack.nonce.len() > 80
        || pack.nonce.contains(' ')
    {
        return Err(FootprintError::validation("invalid export nonce"));
    }
    parse_week(&pack.week)?;
    if pack.sase.split('.').count() != 2 {
        return Err(FootprintError::validation(format!(
            "invalid sase version {:?}",
            pack.sase
        )));
    }
    require_member(&pack.os, EXPORT_OS, "os")?;
    for (surface, days) in &pack.observed {
        require_member(surface, SURFACES, "surface")?;
        if *days > 7 {
            return Err(FootprintError::validation(format!(
                "observed days {days} exceed 7"
            )));
        }
    }
    for surface in &pack.not_measured {
        require_member(surface, SURFACES, "surface")?;
    }
    for feature in &pack.features {
        let ns = ns_of(&feature.id).unwrap_or("");
        if !exportable_feature(&feature.id, ns, "") {
            return Err(FootprintError::validation(format!(
                "feature {} is not exportable",
                feature.id
            )));
        }
        require_member(&feature.actor, ACTORS, "actor")?;
        if feature.days == 0 || feature.days > 7 {
            return Err(FootprintError::validation(format!(
                "invalid feature days {}",
                feature.days
            )));
        }
        require_member(&feature.n, N_BUCKETS, "n bucket")?;
        if bucket_min(&feature.n) == 0 {
            return Err(FootprintError::validation(format!(
                "invalid n bucket {}",
                feature.n
            )));
        }
    }
    Ok(())
}

fn insert_pack(
    tx: &Transaction<'_>,
    pack: &PackWire,
    imported_day: &str,
) -> Result<(), FootprintError> {
    tx.execute(
        "INSERT INTO import_packs(nonce, week, imported_day) VALUES (?1, ?2, ?3)",
        params![pack.nonce, pack.week, imported_day],
    )
    .map_err(FootprintError::from_sqlite)?;
    for feature in &pack.features {
        tx.execute(
            r#"
            INSERT INTO imported_weekly(
                nonce, week, feature_id, actor, days, n_bucket
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
            "#,
            params![
                pack.nonce,
                pack.week,
                feature.id,
                feature.actor,
                feature.days as i64,
                feature.n
            ],
        )
        .map_err(FootprintError::from_sqlite)?;
    }
    for (surface, days) in &pack.observed {
        tx.execute(
            r#"
            INSERT INTO imported_observed(nonce, week, surface, days)
            VALUES (?1, ?2, ?3, ?4)
            "#,
            params![pack.nonce, pack.week, surface, *days as i64],
        )
        .map_err(FootprintError::from_sqlite)?;
    }
    Ok(())
}
