//! Checkpointed fold of spool JSONL files into the daily ledger.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::time::Duration;

use chrono::{Duration as ChronoDuration, NaiveDate, SecondsFormat, Utc};
use rusqlite::{params, OptionalExtension, Transaction};

use super::catalog::{known_feature_ids, parse_day};
use super::spool::{
    parse_spool_line, HitRecord, SpoolLine, SPOOL_DROP_SLACK_BYTES,
    SPOOL_FILE_MAX_BYTES,
};
use super::store::{
    begin_immediate, bounded_busy_timeout, file_id_for, meta_set,
    open_write_store,
};
use super::wire::{
    remap_unknown_id, surface_for_ns, FoldResultWire,
    FOOTPRINT_WIRE_SCHEMA_VERSION,
};
use super::FootprintError;

#[derive(Clone, Default)]
struct DailyAgg {
    n: i64,
    dur_ms_sum: i64,
    dur_ms_max: i64,
}

#[derive(Clone, Default)]
struct CoverageAgg {
    measured: i64,
    human_active: i64,
    records: i64,
    torn: i64,
    dropped: i64,
    unknown_ids: i64,
}

type DailyKey = (String, String, String, String, String, String);
type CoverageKey = (String, String, String);

/// Fold every `spool/*.jsonl` file into the ledger.
pub fn fold(
    store_path: &Path,
    spool_dir: &Path,
    today: &str,
    spool_keep_days: u32,
    spool_max_age_days: u32,
    busy_timeout: Duration,
) -> Result<FoldResultWire, FootprintError> {
    let today = parse_day(today)?;
    let timeout = bounded_busy_timeout(busy_timeout);
    let files = list_spool_files(spool_dir)?;
    let mut result = FoldResultWire {
        schema_version: FOOTPRINT_WIRE_SCHEMA_VERSION,
        ..FoldResultWire::default()
    };
    let mut conn = open_write_store(store_path, timeout)?;
    let known = known_feature_ids(&conn)?;
    for path in &files {
        let stats = fold_one_file(&mut conn, path, today, &known)?;
        result.files_folded += 1;
        result.records_folded += stats.records;
        result.torn += stats.torn;
        result.dropped += stats.dropped;
        result.unknown_ids += stats.unknown_ids;
    }
    let (deleted, expired) = retain_spool_files(
        &mut conn,
        spool_dir,
        today,
        spool_keep_days,
        spool_max_age_days,
    )?;
    result.files_deleted = deleted;
    result.files_expired = expired;
    let folded_at = Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true);
    meta_set(&conn, "last_fold_at", &folded_at)?;
    refresh_first_record_day(&conn)?;
    Ok(result)
}

#[derive(Default)]
pub(crate) struct FileFoldStats {
    pub records: u64,
    pub torn: u64,
    pub dropped: u64,
    pub unknown_ids: u64,
}

pub(crate) fn fold_one_file(
    conn: &mut rusqlite::Connection,
    path: &Path,
    today: NaiveDate,
    known: &BTreeSet<String>,
) -> Result<FileFoldStats, FootprintError> {
    let tx = begin_immediate(conn)?;
    let stats = fold_file_in_tx(&tx, path, today, known)?;
    tx.commit().map_err(FootprintError::from_sqlite)?;
    Ok(stats)
}

/// Fold one file inside an already-open write transaction.
pub(crate) fn fold_file_in_tx(
    tx: &Transaction<'_>,
    path: &Path,
    today: NaiveDate,
    known: &BTreeSet<String>,
) -> Result<FileFoldStats, FootprintError> {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            FootprintError::validation(format!(
                "spool file {} has a non-utf8 name",
                path.display()
            ))
        })?
        .to_string();
    let file_day = day_from_spool_name(&file_name)?;
    let file_id = file_id_for(path)?;
    let size = fs::metadata(path)?.len();
    let checkpoint = read_checkpoint(tx, &file_name)?;
    let mut offset = match checkpoint {
        Some((stored_id, offset, _)) if stored_id == file_id => offset,
        _ => 0,
    };
    if offset > size {
        offset = 0;
    }
    let (bytes, consumed) = read_complete_lines(path, offset, size)?;
    let mut daily: BTreeMap<DailyKey, DailyAgg> = BTreeMap::new();
    let mut coverage: BTreeMap<CoverageKey, CoverageAgg> = BTreeMap::new();
    let mut stats = FileFoldStats::default();
    for line in bytes.split(|byte| *byte == b'\n') {
        if line.is_empty() {
            continue;
        }
        let text = match std::str::from_utf8(line) {
            Ok(text) => text,
            Err(_) => {
                add_torn(&mut coverage, &file_day.to_string(), None);
                stats.torn += 1;
                continue;
            }
        };
        apply_line(
            text,
            &file_day.to_string(),
            known,
            &mut daily,
            &mut coverage,
            &mut stats,
        );
    }
    upsert_daily(tx, &daily)?;
    upsert_coverage(tx, &coverage)?;
    let new_offset = offset + consumed;
    let complete = file_day < today
        && new_offset == size
        && size == fs::metadata(path)?.len();
    let folded_at = Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true);
    tx.execute(
        r#"
        INSERT INTO fold_checkpoints(file, file_id, offset, complete, folded_at)
        VALUES (?1, ?2, ?3, ?4, ?5)
        ON CONFLICT(file) DO UPDATE SET
            file_id = excluded.file_id,
            offset = excluded.offset,
            complete = excluded.complete,
            folded_at = excluded.folded_at
        "#,
        params![
            file_name,
            file_id,
            new_offset as i64,
            i64::from(complete),
            folded_at
        ],
    )
    .map_err(FootprintError::from_sqlite)?;
    Ok(stats)
}

fn apply_line(
    text: &str,
    file_day: &str,
    known: &BTreeSet<String>,
    daily: &mut BTreeMap<DailyKey, DailyAgg>,
    coverage: &mut BTreeMap<CoverageKey, CoverageAgg>,
    stats: &mut FileFoldStats,
) {
    match parse_spool_line(text) {
        SpoolLine::Hit(hit) => {
            apply_hit(hit, known, daily, coverage, stats);
        }
        SpoolLine::Probe { day, surface, .. } => {
            let row = coverage
                .entry((day, surface, "live".to_string()))
                .or_default();
            row.measured = 1;
        }
        SpoolLine::Drop { day, count, .. } => {
            let row = coverage
                .entry((day, "cli".to_string(), "live".to_string()))
                .or_default();
            row.dropped += count as i64;
            row.measured = 1;
            stats.dropped += count;
        }
        SpoolLine::Torn { surface } => {
            add_torn(coverage, file_day, surface.as_deref());
            stats.torn += 1;
        }
    }
}

fn apply_hit(
    hit: HitRecord,
    known: &BTreeSet<String>,
    daily: &mut BTreeMap<DailyKey, DailyAgg>,
    coverage: &mut BTreeMap<CoverageKey, CoverageAgg>,
    stats: &mut FileFoldStats,
) {
    let mut unknown = 0u64;
    let feature_id = if known.contains(&hit.feature_id) {
        hit.feature_id.clone()
    } else {
        unknown += 1;
        remap_unknown_id(&hit.feature_id)
    };
    add_daily(
        daily,
        &hit,
        &feature_id,
        hit.n,
        hit.dur_ms.unwrap_or(0),
        hit.dur_ms.unwrap_or(0),
    );
    stats.records += 1;
    for dest in &hit.opt {
        let option_id = format!("{}#{dest}", hit.feature_id);
        let mapped = if known.contains(&option_id) {
            option_id
        } else {
            unknown += 1;
            remap_unknown_id(&option_id)
        };
        add_daily(daily, &hit, &mapped, hit.n, 0, 0);
        stats.records += 1;
    }
    let surface =
        surface_for_ns(super::wire::ns_of(&hit.feature_id).unwrap_or("cli"))
            .unwrap_or("cli");
    let row = coverage
        .entry((hit.day.clone(), surface.to_string(), "live".to_string()))
        .or_default();
    row.measured = 1;
    row.records += hit.n + hit.n * hit.opt.len() as i64;
    row.unknown_ids += unknown as i64;
    if hit.actor == "human" {
        row.human_active = 1;
    }
    stats.unknown_ids += unknown;
}

fn add_daily(
    daily: &mut BTreeMap<DailyKey, DailyAgg>,
    hit: &HitRecord,
    feature_id: &str,
    n: i64,
    dur_sum: i64,
    dur_max: i64,
) {
    let key = (
        hit.day.clone(),
        feature_id.to_string(),
        hit.actor.clone(),
        hit.via.clone(),
        hit.outcome.clone(),
        "live".to_string(),
    );
    let agg = daily.entry(key).or_default();
    agg.n += n;
    agg.dur_ms_sum += dur_sum;
    agg.dur_ms_max = agg.dur_ms_max.max(dur_max);
}

fn add_torn(
    coverage: &mut BTreeMap<CoverageKey, CoverageAgg>,
    day: &str,
    surface: Option<&str>,
) {
    let surface = surface.unwrap_or("cli");
    let row = coverage
        .entry((day.to_string(), surface.to_string(), "live".to_string()))
        .or_default();
    row.torn += 1;
    row.measured = 1;
}

fn upsert_daily(
    tx: &Transaction<'_>,
    daily: &BTreeMap<DailyKey, DailyAgg>,
) -> Result<(), FootprintError> {
    for ((day, feature_id, actor, via, outcome, origin), agg) in daily {
        tx.execute(
            r#"
            INSERT INTO daily(
                day, feature_id, actor, via, outcome, origin,
                n, dur_ms_sum, dur_ms_max
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
            ON CONFLICT(day, feature_id, actor, via, outcome, origin)
            DO UPDATE SET
                n = n + excluded.n,
                dur_ms_sum = dur_ms_sum + excluded.dur_ms_sum,
                dur_ms_max = MAX(dur_ms_max, excluded.dur_ms_max)
            "#,
            params![
                day,
                feature_id,
                actor,
                via,
                outcome,
                origin,
                agg.n,
                agg.dur_ms_sum,
                agg.dur_ms_max
            ],
        )
        .map_err(FootprintError::from_sqlite)?;
    }
    Ok(())
}

fn upsert_coverage(
    tx: &Transaction<'_>,
    coverage: &BTreeMap<CoverageKey, CoverageAgg>,
) -> Result<(), FootprintError> {
    for ((day, surface, origin), agg) in coverage {
        tx.execute(
            r#"
            INSERT INTO coverage(
                day, surface, origin, measured, human_active, records,
                torn, dropped, unknown_ids, incomplete
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 0)
            ON CONFLICT(day, surface, origin) DO UPDATE SET
                measured = MAX(measured, excluded.measured),
                human_active = MAX(human_active, excluded.human_active),
                records = records + excluded.records,
                torn = torn + excluded.torn,
                dropped = dropped + excluded.dropped,
                unknown_ids = unknown_ids + excluded.unknown_ids
            "#,
            params![
                day,
                surface,
                origin,
                agg.measured,
                agg.human_active,
                agg.records,
                agg.torn,
                agg.dropped,
                agg.unknown_ids
            ],
        )
        .map_err(FootprintError::from_sqlite)?;
    }
    Ok(())
}

fn read_checkpoint(
    tx: &Transaction<'_>,
    file: &str,
) -> Result<Option<(String, u64, bool)>, FootprintError> {
    tx.query_row(
        "SELECT file_id, offset, complete FROM fold_checkpoints WHERE file = ?1",
        params![file],
        |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)? as u64,
                row.get::<_, i64>(2)? != 0,
            ))
        },
    )
    .optional()
    .map_err(FootprintError::from_sqlite)
}

fn read_complete_lines(
    path: &Path,
    offset: u64,
    size: u64,
) -> Result<(Vec<u8>, u64), FootprintError> {
    let mut file = fs::File::open(path)?;
    file.seek(SeekFrom::Start(offset))?;
    let cap = SPOOL_FILE_MAX_BYTES
        .saturating_add(SPOOL_DROP_SLACK_BYTES)
        .saturating_sub(offset);
    let to_read = (size.saturating_sub(offset)).min(cap) as usize;
    let mut buf = vec![0_u8; to_read];
    let mut read = 0usize;
    while read < to_read {
        match file.read(&mut buf[read..])? {
            0 => break,
            n => read += n,
        }
    }
    buf.truncate(read);
    let last_nl = buf.iter().rposition(|byte| *byte == b'\n');
    let consumed = match last_nl {
        Some(index) => index + 1,
        None => 0,
    };
    buf.truncate(consumed);
    Ok((buf, consumed as u64))
}

pub(crate) fn list_spool_files(
    spool_dir: &Path,
) -> Result<Vec<PathBuf>, FootprintError> {
    if !spool_dir.exists() {
        return Ok(Vec::new());
    }
    let mut files = Vec::new();
    for entry in fs::read_dir(spool_dir)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let name = match path.file_name().and_then(|name| name.to_str()) {
            Some(name) => name,
            None => continue,
        };
        if day_from_spool_name(name).is_ok() {
            files.push(path);
        }
    }
    files.sort();
    Ok(files)
}

fn day_from_spool_name(name: &str) -> Result<NaiveDate, FootprintError> {
    let stem = name.strip_suffix(".jsonl").ok_or_else(|| {
        FootprintError::validation(format!("spool file {name} is not jsonl"))
    })?;
    parse_day(stem)
}

fn retain_spool_files(
    conn: &mut rusqlite::Connection,
    spool_dir: &Path,
    today: NaiveDate,
    spool_keep_days: u32,
    spool_max_age_days: u32,
) -> Result<(u64, u64), FootprintError> {
    let files = list_spool_files(spool_dir)?;
    let keep_before = today
        .checked_sub_signed(ChronoDuration::days(i64::from(spool_keep_days)))
        .unwrap_or(NaiveDate::from_ymd_opt(1, 1, 1).unwrap());
    let expire_before = today
        .checked_sub_signed(ChronoDuration::days(i64::from(spool_max_age_days)))
        .unwrap_or(NaiveDate::from_ymd_opt(1, 1, 1).unwrap());
    let mut deleted = 0u64;
    let mut expired = 0u64;
    let tx = begin_immediate(conn)?;
    for path in files {
        let name = path.file_name().unwrap().to_string_lossy().into_owned();
        let day = day_from_spool_name(&name)?;
        let complete: Option<i64> = tx
            .query_row(
                "SELECT complete FROM fold_checkpoints WHERE file = ?1",
                params![name],
                |row| row.get(0),
            )
            .optional()
            .map_err(FootprintError::from_sqlite)?;
        let is_complete = complete == Some(1);
        if day < expire_before {
            mark_incomplete(&tx, &day.to_string())?;
            let _ = fs::remove_file(&path);
            tx.execute(
                "DELETE FROM fold_checkpoints WHERE file = ?1",
                params![name],
            )
            .map_err(FootprintError::from_sqlite)?;
            expired += 1;
            continue;
        }
        if is_complete && day < keep_before {
            let _ = fs::remove_file(&path);
            tx.execute(
                "DELETE FROM fold_checkpoints WHERE file = ?1",
                params![name],
            )
            .map_err(FootprintError::from_sqlite)?;
            deleted += 1;
        }
    }
    tx.commit().map_err(FootprintError::from_sqlite)?;
    Ok((deleted, expired))
}

fn mark_incomplete(
    tx: &Transaction<'_>,
    day: &str,
) -> Result<(), FootprintError> {
    for surface in super::wire::SURFACES {
        tx.execute(
            r#"
            INSERT INTO coverage(
                day, surface, origin, measured, human_active, records,
                torn, dropped, unknown_ids, incomplete
            ) VALUES (?1, ?2, 'live', 0, 0, 0, 0, 0, 0, 1)
            ON CONFLICT(day, surface, origin) DO UPDATE SET
                incomplete = 1
            "#,
            params![day, surface],
        )
        .map_err(FootprintError::from_sqlite)?;
    }
    Ok(())
}

pub(crate) fn refresh_first_record_day(
    conn: &rusqlite::Connection,
) -> Result<(), FootprintError> {
    let first: Option<String> = conn
        .query_row("SELECT MIN(day) FROM daily", [], |row| row.get(0))
        .map_err(FootprintError::from_sqlite)?;
    match first {
        Some(day) => meta_set(conn, "first_record_day", &day)?,
        None => {
            conn.execute("DELETE FROM meta WHERE key = 'first_record_day'", [])
                .map_err(FootprintError::from_sqlite)?;
        }
    }
    Ok(())
}
