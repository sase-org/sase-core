use std::collections::BTreeMap;
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rusqlite::{
    params, params_from_iter, Connection, OpenFlags, OptionalExtension,
    Transaction,
};

use super::wire::{
    TelemetryCleanupReportWire, TelemetryCleanupRequestWire,
    TelemetryHistogramBucketWire, TelemetryInstantQueryResultWire,
    TelemetryInstantQueryWire, TelemetryInstantValueWire,
    TelemetryMetricKindWire, TelemetryPointWire, TelemetryPruneReportWire,
    TelemetryPruneRequestWire, TelemetryRangeQueryResultWire,
    TelemetryRangeQueryWire, TelemetryRecordBatchResultWire,
    TelemetryRecordBatchWire, TelemetryRetentionWire, TelemetrySampleWire,
    TelemetrySeriesWire, TelemetryStoreStatsWire,
    TELEMETRY_WIRE_SCHEMA_VERSION,
};

const ROLLUP_5M_SECONDS: i64 = 5 * 60;
const ROLLUP_1H_SECONDS: i64 = 60 * 60;
const DEFAULT_BUSY_TIMEOUT: Duration = Duration::from_millis(250);
pub const TELEMETRY_MAX_BUSY_TIMEOUT: Duration = Duration::from_secs(30);

/// Insert one process flush atomically and opportunistically enforce retention.
pub fn record_batch(
    store_path: &Path,
    batch: TelemetryRecordBatchWire,
    busy_timeout: Duration,
) -> Result<TelemetryRecordBatchResultWire, String> {
    validate_batch(&batch)?;
    let now_ts = batch.now_ts.unwrap_or_else(unix_now);
    with_store(store_path, busy_timeout, |conn| {
        let tx = conn.transaction().map_err(|e| e.to_string())?;
        for sample in &batch.samples {
            insert_sample(&tx, sample)?;
        }
        update_write_metadata(&tx, &batch.samples, now_ts)?;
        let maintenance = maintain(&tx, now_ts, &batch.retention)?;
        tx.commit().map_err(|e| e.to_string())?;
        Ok(TelemetryRecordBatchResultWire {
            schema_version: TELEMETRY_WIRE_SCHEMA_VERSION,
            samples_recorded: batch.samples.len() as u64,
            maintenance,
        })
    })
}

/// Query current metric values. Gauge sources older than the staleness window
/// are excluded; counters and histograms aggregate retained deltas.
pub fn query_instant(
    store_path: &Path,
    request: TelemetryInstantQueryWire,
    busy_timeout: Duration,
) -> Result<TelemetryInstantQueryResultWire, String> {
    validate_metric_name(&request.metric)?;
    let now_ts = request.now_ts.unwrap_or_else(unix_now);
    with_store(store_path, busy_timeout, |conn| {
        let loaded = load_metric_rows(conn, &request.metric, None)?;
        instant_from_rows(&request, now_ts, loaded.rows)
    })
}

/// Query one metric over a time range, transparently merging raw, 5-minute,
/// and 1-hour rows into caller-sized output buckets.
pub fn query_range(
    store_path: &Path,
    request: TelemetryRangeQueryWire,
    busy_timeout: Duration,
) -> Result<TelemetryRangeQueryResultWire, String> {
    validate_range_query(&request)?;
    with_store(store_path, busy_timeout, |conn| {
        let loaded = load_metric_rows(
            conn,
            &request.metric,
            Some((request.start_ts, request.end_ts)),
        )?;
        range_from_rows(&request, loaded)
    })
}

/// Fold expired rows down through both rollup tiers and delete expired 1-hour
/// rows. Readers never invoke this function implicitly.
pub fn prune(
    store_path: &Path,
    request: TelemetryPruneRequestWire,
    busy_timeout: Duration,
) -> Result<TelemetryPruneReportWire, String> {
    let now_ts = request.now_ts.unwrap_or_else(unix_now);
    with_store(store_path, busy_timeout, |conn| {
        let tx = conn.transaction().map_err(|e| e.to_string())?;
        let report = maintain(&tx, now_ts, &request.retention)?;
        tx.commit().map_err(|e| e.to_string())?;
        Ok(report)
    })
}

/// Return lightweight storage and freshness information for status surfaces.
pub fn store_stats(
    store_path: &Path,
    busy_timeout: Duration,
) -> Result<TelemetryStoreStatsWire, String> {
    with_store(store_path, busy_timeout, |conn| {
        let raw_sample_count = count_rows(conn, "samples")?;
        let rollup_5m_count = count_rows(conn, "rollup_5m")?;
        let rollup_1h_count = count_rows(conn, "rollup_1h")?;
        let (earliest_sample_ts, latest_sample_ts) = sample_bounds(conn)?;
        let last_write_ts = read_meta_i64(conn, "last_write_ts")?;
        let last_write_by_subsystem = read_subsystem_write_metadata(conn)?;
        let db_size_bytes = fs::metadata(store_path)
            .map(|metadata| metadata.len())
            .unwrap_or(0);
        Ok(TelemetryStoreStatsWire {
            schema_version: TELEMETRY_WIRE_SCHEMA_VERSION,
            store_path: store_path.to_string_lossy().into_owned(),
            db_size_bytes,
            raw_sample_count,
            rollup_5m_count,
            rollup_1h_count,
            earliest_sample_ts,
            latest_sample_ts,
            last_write_ts,
            last_write_by_subsystem,
        })
    })
}

/// Preview or delete rows matching any caller-supplied exact label value.
///
/// Dry runs open an existing store read-only. Mutating runs delete from all
/// three tiers in one transaction, rebuild freshness metadata from the rows
/// that remain, and vacuum only after a successful non-empty commit.
pub fn cleanup_matching_labels(
    store_path: &Path,
    request: TelemetryCleanupRequestWire,
    busy_timeout: Duration,
) -> Result<TelemetryCleanupReportWire, String> {
    validate_cleanup_request(&request)?;
    let size_before = store_size(store_path);
    if !store_path.exists() {
        return Ok(cleanup_report(&request, [0, 0, 0], 0, 0, false));
    }
    if request.dry_run {
        let conn = Connection::open_with_flags(
            store_path,
            OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .map_err(|e| e.to_string())?;
        conn.busy_timeout(bounded_busy_timeout(busy_timeout))
            .map_err(|e| e.to_string())?;
        let counts = cleanup_counts(&conn, &request)?;
        return Ok(cleanup_report(
            &request,
            counts,
            size_before,
            size_before,
            false,
        ));
    }

    let timeout = bounded_busy_timeout(busy_timeout);
    let mut conn = open_store(store_path, timeout)?;
    let counts = cleanup_counts(&conn, &request)?;
    let total = counts.iter().sum::<u64>();
    if total == 0 {
        return Ok(cleanup_report(
            &request,
            counts,
            size_before,
            store_size(store_path),
            false,
        ));
    }

    let tx = conn.transaction().map_err(|e| e.to_string())?;
    for (table, expected) in ["samples", "rollup_5m", "rollup_1h"]
        .into_iter()
        .zip(counts)
    {
        let deleted = delete_cleanup_rows(&tx, table, &request)?;
        if deleted != expected {
            return Err(format!(
                "telemetry cleanup count changed for {table}: expected {expected}, deleted {deleted}"
            ));
        }
    }
    refresh_write_metadata(&tx)?;
    tx.commit().map_err(|e| e.to_string())?;

    conn.execute_batch("VACUUM").map_err(|e| e.to_string())?;
    let size_after = store_size(store_path);
    Ok(cleanup_report(
        &request,
        counts,
        size_before,
        size_after,
        true,
    ))
}

fn validate_cleanup_request(
    request: &TelemetryCleanupRequestWire,
) -> Result<(), String> {
    if request.label_matches.is_empty() {
        return Err(
            "telemetry cleanup requires at least one exact label match".into(),
        );
    }
    for (label, values) in &request.label_matches {
        if label.is_empty() {
            return Err(
                "telemetry cleanup label names must not be empty".into()
            );
        }
        if values.is_empty() {
            return Err(format!(
                "telemetry cleanup label {label:?} requires at least one exact value"
            ));
        }
    }
    Ok(())
}

fn cleanup_report(
    request: &TelemetryCleanupRequestWire,
    counts: [u64; 3],
    size_before: u64,
    size_after: u64,
    vacuumed: bool,
) -> TelemetryCleanupReportWire {
    TelemetryCleanupReportWire {
        schema_version: TELEMETRY_WIRE_SCHEMA_VERSION,
        dry_run: request.dry_run,
        raw_rows: counts[0],
        rollup_5m_rows: counts[1],
        rollup_1h_rows: counts[2],
        total_rows: counts.iter().sum(),
        store_size_before_bytes: size_before,
        store_size_after_bytes: size_after,
        reclaimed_bytes: size_before.saturating_sub(size_after),
        vacuumed,
    }
}

fn cleanup_counts(
    conn: &Connection,
    request: &TelemetryCleanupRequestWire,
) -> Result<[u64; 3], String> {
    Ok([
        count_cleanup_rows(conn, "samples", request)?,
        count_cleanup_rows(conn, "rollup_5m", request)?,
        count_cleanup_rows(conn, "rollup_1h", request)?,
    ])
}

fn count_cleanup_rows(
    conn: &Connection,
    table: &str,
    request: &TelemetryCleanupRequestWire,
) -> Result<u64, String> {
    validate_stats_table(table)?;
    let (predicate, values) = cleanup_predicate(request);
    let sql = format!("SELECT COUNT(*) FROM {table} WHERE {predicate}");
    let count: i64 = conn
        .query_row(&sql, params_from_iter(values.iter()), |row| row.get(0))
        .map_err(|e| e.to_string())?;
    i64_to_u64(count)
}

fn delete_cleanup_rows(
    tx: &Transaction<'_>,
    table: &str,
    request: &TelemetryCleanupRequestWire,
) -> Result<u64, String> {
    validate_stats_table(table)?;
    let (predicate, values) = cleanup_predicate(request);
    let sql = format!("DELETE FROM {table} WHERE {predicate}");
    let deleted = tx
        .execute(&sql, params_from_iter(values.iter()))
        .map_err(|e| e.to_string())?;
    Ok(deleted as u64)
}

fn cleanup_predicate(
    request: &TelemetryCleanupRequestWire,
) -> (String, Vec<String>) {
    let mut clauses = Vec::new();
    let mut params = Vec::new();
    for (label, values) in &request.label_matches {
        let placeholders = std::iter::repeat("?")
            .take(values.len())
            .collect::<Vec<_>>()
            .join(", ");
        clauses.push(format!(
            "EXISTS (SELECT 1 FROM json_each(labels_json) WHERE key = ? AND value IN ({placeholders}))"
        ));
        params.push(label.clone());
        params.extend(values.iter().cloned());
    }
    (clauses.join(" OR "), params)
}

fn refresh_write_metadata(tx: &Transaction<'_>) -> Result<(), String> {
    tx.execute(
        "DELETE FROM meta WHERE key = 'last_write_ts' OR key LIKE 'last_write_subsystem:%'",
        [],
    )
    .map_err(|e| e.to_string())?;

    let mut stmt = tx
        .prepare(
            r#"
            SELECT subsystem, MAX(last_ts) FROM (
                SELECT subsystem, ts AS last_ts FROM samples
                UNION ALL
                SELECT subsystem, last_ts FROM rollup_5m
                UNION ALL
                SELECT subsystem, last_ts FROM rollup_1h
            )
            GROUP BY subsystem
            ORDER BY subsystem
            "#,
        )
        .map_err(|e| e.to_string())?;
    let rows = stmt
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })
        .map_err(|e| e.to_string())?;
    let mut latest = None;
    let mut by_subsystem = Vec::new();
    for row in rows {
        let (subsystem, timestamp) = row.map_err(|e| e.to_string())?;
        latest =
            Some(latest.map_or(timestamp, |prior: i64| prior.max(timestamp)));
        by_subsystem.push((subsystem, timestamp));
    }
    drop(stmt);

    if let Some(timestamp) = latest {
        tx.execute(
            "INSERT INTO meta(key, value) VALUES ('last_write_ts', ?1)",
            [timestamp.to_string()],
        )
        .map_err(|e| e.to_string())?;
    }
    for (subsystem, timestamp) in by_subsystem {
        tx.execute(
            "INSERT INTO meta(key, value) VALUES (?1, ?2)",
            params![
                format!("last_write_subsystem:{subsystem}"),
                timestamp.to_string()
            ],
        )
        .map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn store_size(store_path: &Path) -> u64 {
    fs::metadata(store_path)
        .map(|metadata| metadata.len())
        .unwrap_or(0)
}

fn with_store<T>(
    store_path: &Path,
    busy_timeout: Duration,
    mut operation: impl FnMut(&mut Connection) -> Result<T, String>,
) -> Result<T, String> {
    let timeout = bounded_busy_timeout(busy_timeout);
    match open_store(store_path, timeout)
        .and_then(|mut conn| operation(&mut conn))
    {
        Ok(value) => Ok(value),
        Err(error)
            if store_path.exists() && is_sqlite_corruption_error(&error) =>
        {
            quarantine_corrupt_store(store_path)?;
            let mut conn =
                open_store(store_path, timeout).map_err(|retry| {
                    format!(
                    "{retry} (after replacing corrupt telemetry store: {error})"
                )
                })?;
            operation(&mut conn).map_err(|retry| {
                format!(
                    "{retry} (after replacing corrupt telemetry store: {error})"
                )
            })
        }
        Err(error) => Err(error),
    }
}

fn bounded_busy_timeout(timeout: Duration) -> Duration {
    if timeout.is_zero() {
        DEFAULT_BUSY_TIMEOUT
    } else {
        timeout.min(TELEMETRY_MAX_BUSY_TIMEOUT)
    }
}

fn open_store(
    store_path: &Path,
    busy_timeout: Duration,
) -> Result<Connection, String> {
    if let Some(parent) = store_path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).map_err(|e| e.to_string())?;
        }
    }
    let conn = Connection::open(store_path).map_err(|e| e.to_string())?;
    conn.busy_timeout(busy_timeout).map_err(|e| e.to_string())?;
    conn.execute_batch(
        r#"
        PRAGMA journal_mode = WAL;
        PRAGMA synchronous = NORMAL;
        PRAGMA foreign_keys = ON;
        CREATE TABLE IF NOT EXISTS meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS samples (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts INTEGER NOT NULL,
            metric TEXT NOT NULL,
            kind TEXT NOT NULL,
            labels_json TEXT NOT NULL,
            source TEXT NOT NULL,
            value REAL NOT NULL,
            histogram_count INTEGER NOT NULL DEFAULT 0,
            histogram_sum REAL NOT NULL DEFAULT 0,
            histogram_min REAL,
            histogram_max REAL,
            buckets_json TEXT NOT NULL DEFAULT '[]',
            subsystem TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_telemetry_samples_metric_ts
            ON samples(metric, ts);
        CREATE INDEX IF NOT EXISTS idx_telemetry_samples_ts
            ON samples(ts);
        CREATE TABLE IF NOT EXISTS rollup_5m (
            bucket_ts INTEGER NOT NULL,
            metric TEXT NOT NULL,
            kind TEXT NOT NULL,
            labels_json TEXT NOT NULL,
            source TEXT NOT NULL,
            subsystem TEXT NOT NULL,
            sample_count INTEGER NOT NULL,
            value_sum REAL NOT NULL,
            value_min REAL,
            value_max REAL,
            last_ts INTEGER NOT NULL,
            last_value REAL NOT NULL,
            histogram_count INTEGER NOT NULL,
            histogram_sum REAL NOT NULL,
            histogram_min REAL,
            histogram_max REAL,
            buckets_json TEXT NOT NULL,
            PRIMARY KEY (bucket_ts, metric, kind, labels_json, source)
        );
        CREATE INDEX IF NOT EXISTS idx_telemetry_rollup_5m_metric_ts
            ON rollup_5m(metric, bucket_ts);
        CREATE TABLE IF NOT EXISTS rollup_1h (
            bucket_ts INTEGER NOT NULL,
            metric TEXT NOT NULL,
            kind TEXT NOT NULL,
            labels_json TEXT NOT NULL,
            source TEXT NOT NULL,
            subsystem TEXT NOT NULL,
            sample_count INTEGER NOT NULL,
            value_sum REAL NOT NULL,
            value_min REAL,
            value_max REAL,
            last_ts INTEGER NOT NULL,
            last_value REAL NOT NULL,
            histogram_count INTEGER NOT NULL,
            histogram_sum REAL NOT NULL,
            histogram_min REAL,
            histogram_max REAL,
            buckets_json TEXT NOT NULL,
            PRIMARY KEY (bucket_ts, metric, kind, labels_json, source)
        );
        CREATE INDEX IF NOT EXISTS idx_telemetry_rollup_1h_metric_ts
            ON rollup_1h(metric, bucket_ts);
        "#,
    )
    .map_err(|e| e.to_string())?;

    let prior_version: Option<u32> = conn
        .query_row(
            "SELECT value FROM meta WHERE key = 'schema_version'",
            [],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(|e| e.to_string())?
        .and_then(|raw| raw.parse::<u32>().ok());
    if prior_version
        .is_some_and(|version| version > TELEMETRY_WIRE_SCHEMA_VERSION)
    {
        return Err(format!(
            "telemetry store schema version {} is newer than supported version {}",
            prior_version.unwrap_or_default(),
            TELEMETRY_WIRE_SCHEMA_VERSION
        ));
    }
    conn.execute(
        "INSERT OR REPLACE INTO meta(key, value) VALUES ('schema_version', ?1)",
        [TELEMETRY_WIRE_SCHEMA_VERSION.to_string()],
    )
    .map_err(|e| e.to_string())?;
    Ok(conn)
}

fn validate_batch(batch: &TelemetryRecordBatchWire) -> Result<(), String> {
    for (index, sample) in batch.samples.iter().enumerate() {
        validate_sample(sample).map_err(|error| {
            format!("invalid telemetry sample {index}: {error}")
        })?;
    }
    Ok(())
}

fn validate_sample(sample: &TelemetrySampleWire) -> Result<(), String> {
    validate_metric_name(&sample.metric)?;
    if !sample.value.is_finite() {
        return Err("value must be finite".to_string());
    }
    if !sample.sum.is_finite()
        || sample.min.is_some_and(|value| !value.is_finite())
        || sample.max.is_some_and(|value| !value.is_finite())
    {
        return Err("histogram aggregates must be finite".to_string());
    }
    if sample
        .min
        .zip(sample.max)
        .is_some_and(|(min, max)| min > max)
    {
        return Err("histogram min must be <= max".to_string());
    }
    if sample.kind == TelemetryMetricKindWire::Counter && sample.value < 0.0 {
        return Err("counter deltas must be non-negative".to_string());
    }
    if sample.kind != TelemetryMetricKindWire::Histogram
        && (sample.count != 0
            || sample.sum != 0.0
            || sample.min.is_some()
            || sample.max.is_some()
            || !sample.buckets.is_empty())
    {
        return Err(
            "histogram aggregates are only valid for histogram samples"
                .to_string(),
        );
    }
    let mut prior_bound: Option<f64> = None;
    let mut prior_count = 0;
    for bucket in &sample.buckets {
        if !bucket.le.is_finite() {
            return Err("histogram bucket bounds must be finite".to_string());
        }
        if prior_bound.is_some_and(|bound| bucket.le <= bound) {
            return Err("histogram bucket bounds must be strictly increasing"
                .to_string());
        }
        if bucket.count < prior_count || bucket.count > sample.count {
            return Err(
                "histogram bucket counts must be cumulative and <= count"
                    .to_string(),
            );
        }
        prior_bound = Some(bucket.le);
        prior_count = bucket.count;
    }
    Ok(())
}

fn validate_metric_name(metric: &str) -> Result<(), String> {
    if metric.trim().is_empty() {
        Err("telemetry metric name must not be empty".to_string())
    } else {
        Ok(())
    }
}

fn validate_range_query(
    request: &TelemetryRangeQueryWire,
) -> Result<(), String> {
    validate_metric_name(&request.metric)?;
    if request.end_ts < request.start_ts {
        return Err("telemetry range end_ts must be >= start_ts".to_string());
    }
    if request.step_seconds == 0 || request.step_seconds > i64::MAX as u64 {
        return Err("telemetry range step_seconds must be positive".to_string());
    }
    Ok(())
}

fn insert_sample(
    tx: &Transaction<'_>,
    sample: &TelemetrySampleWire,
) -> Result<(), String> {
    let labels_json = labels_to_json(&sample.labels)?;
    let buckets_json = serde_json::to_string(&sample.buckets)
        .map_err(|e| format!("failed to serialize histogram buckets: {e}"))?;
    let source = normalized_source(&sample.source);
    let subsystem = sample
        .subsystem
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned)
        .unwrap_or_else(|| derive_subsystem(&sample.metric));
    tx.execute(
        r#"
        INSERT INTO samples (
            ts, metric, kind, labels_json, source, value,
            histogram_count, histogram_sum, histogram_min, histogram_max,
            buckets_json, subsystem
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
        "#,
        params![
            sample.ts,
            sample.metric,
            sample.kind.as_str(),
            labels_json,
            source,
            sample.value,
            u64_to_i64(sample.count)?,
            sample.sum,
            sample.min,
            sample.max,
            buckets_json,
            subsystem,
        ],
    )
    .map_err(|e| e.to_string())?;
    Ok(())
}

fn update_write_metadata(
    tx: &Transaction<'_>,
    samples: &[TelemetrySampleWire],
    now_ts: i64,
) -> Result<(), String> {
    if samples.is_empty() {
        return Ok(());
    }
    tx.execute(
        "INSERT OR REPLACE INTO meta(key, value) VALUES ('last_write_ts', ?1)",
        [now_ts.to_string()],
    )
    .map_err(|e| e.to_string())?;

    let mut latest_by_subsystem: BTreeMap<String, i64> = BTreeMap::new();
    for sample in samples {
        let subsystem = sample
            .subsystem
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .map(str::to_owned)
            .unwrap_or_else(|| derive_subsystem(&sample.metric));
        latest_by_subsystem
            .entry(subsystem)
            .and_modify(|timestamp| *timestamp = (*timestamp).max(sample.ts))
            .or_insert(sample.ts);
    }
    for (subsystem, timestamp) in latest_by_subsystem {
        let key = format!("last_write_subsystem:{subsystem}");
        let prior = read_meta_i64(tx, &key)?.unwrap_or(i64::MIN);
        tx.execute(
            "INSERT OR REPLACE INTO meta(key, value) VALUES (?1, ?2)",
            params![key, prior.max(timestamp).to_string()],
        )
        .map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn maintain(
    tx: &Transaction<'_>,
    now_ts: i64,
    retention: &TelemetryRetentionWire,
) -> Result<TelemetryPruneReportWire, String> {
    let raw_cutoff = retention_cutoff(now_ts, retention.raw_seconds);
    let rollup_5m_cutoff =
        retention_cutoff(now_ts, retention.rollup_5m_seconds);
    let rollup_1h_cutoff =
        retention_cutoff(now_ts, retention.rollup_1h_seconds);

    let raw_rows_folded = fold_raw_rows(tx, raw_cutoff)?;
    let rollup_5m_rows_folded = fold_rollup_rows(
        tx,
        "rollup_5m",
        "rollup_1h",
        rollup_5m_cutoff,
        ROLLUP_1H_SECONDS,
    )?;
    let rollup_1h_rows_deleted = tx
        .execute(
            "DELETE FROM rollup_1h WHERE bucket_ts < ?1",
            [rollup_1h_cutoff],
        )
        .map_err(|e| e.to_string())? as u64;

    Ok(TelemetryPruneReportWire {
        schema_version: TELEMETRY_WIRE_SCHEMA_VERSION,
        raw_rows_folded,
        rollup_5m_rows_folded,
        rollup_1h_rows_deleted,
    })
}

fn fold_raw_rows(tx: &Transaction<'_>, cutoff: i64) -> Result<u64, String> {
    let rows =
        select_raw_rows(tx, None, Some((i64::MIN, cutoff.saturating_sub(1))))?;
    let row_count = rows.len() as u64;
    let mut grouped: BTreeMap<RollupKey, AggregateRow> = BTreeMap::new();
    for mut row in rows {
        row.bucket_ts = floor_bucket(row.bucket_ts, ROLLUP_5M_SECONDS);
        merge_grouped(&mut grouped, row)?;
    }
    for row in grouped.into_values() {
        upsert_rollup(tx, "rollup_5m", row)?;
    }
    tx.execute("DELETE FROM samples WHERE ts < ?1", [cutoff])
        .map_err(|e| e.to_string())?;
    Ok(row_count)
}

fn fold_rollup_rows(
    tx: &Transaction<'_>,
    source_table: &str,
    target_table: &str,
    cutoff: i64,
    target_resolution: i64,
) -> Result<u64, String> {
    let mut rows = select_rollup_rows(
        tx,
        source_table,
        None,
        Some((i64::MIN, cutoff.saturating_sub(1))),
        0,
    )?;
    let row_count = rows.len() as u64;
    let mut grouped: BTreeMap<RollupKey, AggregateRow> = BTreeMap::new();
    for row in &mut rows {
        row.bucket_ts = floor_bucket(row.bucket_ts, target_resolution);
        merge_grouped(&mut grouped, row.clone())?;
    }
    for row in grouped.into_values() {
        upsert_rollup(tx, target_table, row)?;
    }
    tx.execute(
        &format!("DELETE FROM {source_table} WHERE bucket_ts < ?1"),
        [cutoff],
    )
    .map_err(|e| e.to_string())?;
    Ok(row_count)
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct RollupKey {
    bucket_ts: i64,
    metric: String,
    kind: TelemetryMetricKindWire,
    labels_json: String,
    source: String,
}

#[derive(Debug, Clone)]
struct AggregateRow {
    bucket_ts: i64,
    metric: String,
    kind: TelemetryMetricKindWire,
    labels_json: String,
    source: String,
    subsystem: String,
    sample_count: u64,
    value_sum: f64,
    value_min: Option<f64>,
    value_max: Option<f64>,
    last_ts: i64,
    last_value: f64,
    histogram_count: u64,
    histogram_sum: f64,
    histogram_min: Option<f64>,
    histogram_max: Option<f64>,
    buckets: Vec<TelemetryHistogramBucketWire>,
}

impl AggregateRow {
    fn key(&self) -> RollupKey {
        RollupKey {
            bucket_ts: self.bucket_ts,
            metric: self.metric.clone(),
            kind: self.kind,
            labels_json: self.labels_json.clone(),
            source: self.source.clone(),
        }
    }

    fn merge(&mut self, other: Self) -> Result<(), String> {
        if self.metric != other.metric
            || self.kind != other.kind
            || self.labels_json != other.labels_json
        {
            return Err(
                "attempted to merge incompatible telemetry rows".to_string()
            );
        }
        self.sample_count =
            self.sample_count.saturating_add(other.sample_count);
        self.value_sum += other.value_sum;
        self.value_min = min_option(self.value_min, other.value_min);
        self.value_max = max_option(self.value_max, other.value_max);
        if other.last_ts >= self.last_ts {
            self.last_ts = other.last_ts;
            self.last_value = other.last_value;
        }
        self.histogram_count =
            self.histogram_count.saturating_add(other.histogram_count);
        self.histogram_sum += other.histogram_sum;
        self.histogram_min =
            min_option(self.histogram_min, other.histogram_min);
        self.histogram_max =
            max_option(self.histogram_max, other.histogram_max);
        merge_histogram_buckets(&mut self.buckets, &other.buckets);
        Ok(())
    }

    fn latest_gauge(mut self) -> Self {
        self.sample_count = 1;
        self.value_sum = self.last_value;
        self.value_min = Some(self.last_value);
        self.value_max = Some(self.last_value);
        self
    }
}

fn merge_grouped(
    grouped: &mut BTreeMap<RollupKey, AggregateRow>,
    row: AggregateRow,
) -> Result<(), String> {
    let key = row.key();
    match grouped.get_mut(&key) {
        Some(existing) => existing.merge(row),
        None => {
            grouped.insert(key, row);
            Ok(())
        }
    }
}

fn upsert_rollup(
    tx: &Transaction<'_>,
    table: &str,
    mut row: AggregateRow,
) -> Result<(), String> {
    validate_rollup_table(table)?;
    if let Some(existing) = select_existing_rollup(tx, table, &row.key())? {
        row.merge(existing)?;
    }
    let buckets_json = serde_json::to_string(&row.buckets)
        .map_err(|e| format!("failed to serialize histogram buckets: {e}"))?;
    tx.execute(
        &format!(
            r#"
            INSERT OR REPLACE INTO {table} (
                bucket_ts, metric, kind, labels_json, source, subsystem,
                sample_count, value_sum, value_min, value_max,
                last_ts, last_value, histogram_count, histogram_sum,
                histogram_min, histogram_max, buckets_json
            ) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10,
                ?11, ?12, ?13, ?14, ?15, ?16, ?17
            )
            "#
        ),
        params![
            row.bucket_ts,
            row.metric,
            row.kind.as_str(),
            row.labels_json,
            row.source,
            row.subsystem,
            u64_to_i64(row.sample_count)?,
            row.value_sum,
            row.value_min,
            row.value_max,
            row.last_ts,
            row.last_value,
            u64_to_i64(row.histogram_count)?,
            row.histogram_sum,
            row.histogram_min,
            row.histogram_max,
            buckets_json,
        ],
    )
    .map_err(|e| e.to_string())?;
    Ok(())
}

fn select_existing_rollup(
    conn: &Connection,
    table: &str,
    key: &RollupKey,
) -> Result<Option<AggregateRow>, String> {
    validate_rollup_table(table)?;
    let sql = format!(
        "SELECT bucket_ts, metric, kind, labels_json, source, subsystem, \
         sample_count, value_sum, value_min, value_max, last_ts, last_value, \
         histogram_count, histogram_sum, histogram_min, histogram_max, buckets_json \
         FROM {table} WHERE bucket_ts = ?1 AND metric = ?2 AND kind = ?3 \
         AND labels_json = ?4 AND source = ?5"
    );
    let row = conn
        .query_row(
            &sql,
            params![
                key.bucket_ts,
                key.metric,
                key.kind.as_str(),
                key.labels_json,
                key.source,
            ],
            rollup_db_row,
        )
        .optional()
        .map_err(|e| e.to_string())?;
    row.map(aggregate_from_rollup_db).transpose()
}

fn select_raw_rows(
    conn: &Connection,
    metric: Option<&str>,
    range: Option<(i64, i64)>,
) -> Result<Vec<AggregateRow>, String> {
    let (sql, values): (String, Vec<rusqlite::types::Value>) =
        match (metric, range) {
            (Some(metric), Some((start, end))) => (
                "SELECT ts, metric, kind, labels_json, source, subsystem, value, \
                 histogram_count, histogram_sum, histogram_min, histogram_max, buckets_json \
                 FROM samples WHERE metric = ?1 AND ts >= ?2 AND ts <= ?3"
                    .to_string(),
                vec![metric.to_string().into(), start.into(), end.into()],
            ),
            (Some(metric), None) => (
                "SELECT ts, metric, kind, labels_json, source, subsystem, value, \
                 histogram_count, histogram_sum, histogram_min, histogram_max, buckets_json \
                 FROM samples WHERE metric = ?1"
                    .to_string(),
                vec![metric.to_string().into()],
            ),
            (None, Some((start, end))) => (
                "SELECT ts, metric, kind, labels_json, source, subsystem, value, \
                 histogram_count, histogram_sum, histogram_min, histogram_max, buckets_json \
                 FROM samples WHERE ts >= ?1 AND ts <= ?2"
                    .to_string(),
                vec![start.into(), end.into()],
            ),
            (None, None) => (
                "SELECT ts, metric, kind, labels_json, source, subsystem, value, \
                 histogram_count, histogram_sum, histogram_min, histogram_max, buckets_json \
                 FROM samples"
                    .to_string(),
                Vec::new(),
            ),
        };
    let mut stmt = conn.prepare(&sql).map_err(|e| e.to_string())?;
    let rows = stmt
        .query_map(rusqlite::params_from_iter(values), raw_db_row)
        .map_err(|e| e.to_string())?;
    rows.map(|row| {
        row.map_err(|e| e.to_string())
            .and_then(aggregate_from_raw_db)
    })
    .collect()
}

fn select_rollup_rows(
    conn: &Connection,
    table: &str,
    metric: Option<&str>,
    range: Option<(i64, i64)>,
    resolution: i64,
) -> Result<Vec<AggregateRow>, String> {
    validate_rollup_table(table)?;
    let (where_sql, values): (String, Vec<rusqlite::types::Value>) =
        match (metric, range) {
            (Some(metric), Some((start, end))) => (
                "metric = ?1 AND bucket_ts <= ?2 AND bucket_ts + ?3 > ?4"
                    .to_string(),
                vec![
                    metric.to_string().into(),
                    end.into(),
                    resolution.into(),
                    start.into(),
                ],
            ),
            (Some(metric), None) => {
                ("metric = ?1".to_string(), vec![metric.to_string().into()])
            }
            (None, Some((start, end))) => (
                "bucket_ts <= ?1 AND bucket_ts + ?2 > ?3".to_string(),
                vec![end.into(), resolution.into(), start.into()],
            ),
            (None, None) => ("1 = 1".to_string(), Vec::new()),
        };
    let sql = format!(
        "SELECT bucket_ts, metric, kind, labels_json, source, subsystem, \
         sample_count, value_sum, value_min, value_max, last_ts, last_value, \
         histogram_count, histogram_sum, histogram_min, histogram_max, buckets_json \
         FROM {table} WHERE {where_sql}"
    );
    let mut stmt = conn.prepare(&sql).map_err(|e| e.to_string())?;
    let rows = stmt
        .query_map(rusqlite::params_from_iter(values), rollup_db_row)
        .map_err(|e| e.to_string())?;
    rows.map(|row| {
        row.map_err(|e| e.to_string())
            .and_then(aggregate_from_rollup_db)
    })
    .collect()
}

struct RawDbRow {
    ts: i64,
    metric: String,
    kind: String,
    labels_json: String,
    source: String,
    subsystem: String,
    value: f64,
    histogram_count: i64,
    histogram_sum: f64,
    histogram_min: Option<f64>,
    histogram_max: Option<f64>,
    buckets_json: String,
}

fn raw_db_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<RawDbRow> {
    Ok(RawDbRow {
        ts: row.get(0)?,
        metric: row.get(1)?,
        kind: row.get(2)?,
        labels_json: row.get(3)?,
        source: row.get(4)?,
        subsystem: row.get(5)?,
        value: row.get(6)?,
        histogram_count: row.get(7)?,
        histogram_sum: row.get(8)?,
        histogram_min: row.get(9)?,
        histogram_max: row.get(10)?,
        buckets_json: row.get(11)?,
    })
}

fn aggregate_from_raw_db(row: RawDbRow) -> Result<AggregateRow, String> {
    let kind = TelemetryMetricKindWire::from_db(&row.kind)?;
    let histogram_count = i64_to_u64(row.histogram_count)?;
    let buckets = parse_buckets(&row.buckets_json)?;
    Ok(AggregateRow {
        bucket_ts: row.ts,
        metric: row.metric,
        kind,
        labels_json: row.labels_json,
        source: row.source,
        subsystem: row.subsystem,
        sample_count: 1,
        value_sum: row.value,
        value_min: Some(row.value),
        value_max: Some(row.value),
        last_ts: row.ts,
        last_value: row.value,
        histogram_count,
        histogram_sum: row.histogram_sum,
        histogram_min: row.histogram_min,
        histogram_max: row.histogram_max,
        buckets,
    })
}

struct RollupDbRow {
    bucket_ts: i64,
    metric: String,
    kind: String,
    labels_json: String,
    source: String,
    subsystem: String,
    sample_count: i64,
    value_sum: f64,
    value_min: Option<f64>,
    value_max: Option<f64>,
    last_ts: i64,
    last_value: f64,
    histogram_count: i64,
    histogram_sum: f64,
    histogram_min: Option<f64>,
    histogram_max: Option<f64>,
    buckets_json: String,
}

fn rollup_db_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<RollupDbRow> {
    Ok(RollupDbRow {
        bucket_ts: row.get(0)?,
        metric: row.get(1)?,
        kind: row.get(2)?,
        labels_json: row.get(3)?,
        source: row.get(4)?,
        subsystem: row.get(5)?,
        sample_count: row.get(6)?,
        value_sum: row.get(7)?,
        value_min: row.get(8)?,
        value_max: row.get(9)?,
        last_ts: row.get(10)?,
        last_value: row.get(11)?,
        histogram_count: row.get(12)?,
        histogram_sum: row.get(13)?,
        histogram_min: row.get(14)?,
        histogram_max: row.get(15)?,
        buckets_json: row.get(16)?,
    })
}

fn aggregate_from_rollup_db(row: RollupDbRow) -> Result<AggregateRow, String> {
    Ok(AggregateRow {
        bucket_ts: row.bucket_ts,
        metric: row.metric,
        kind: TelemetryMetricKindWire::from_db(&row.kind)?,
        labels_json: row.labels_json,
        source: row.source,
        subsystem: row.subsystem,
        sample_count: i64_to_u64(row.sample_count)?,
        value_sum: row.value_sum,
        value_min: row.value_min,
        value_max: row.value_max,
        last_ts: row.last_ts,
        last_value: row.last_value,
        histogram_count: i64_to_u64(row.histogram_count)?,
        histogram_sum: row.histogram_sum,
        histogram_min: row.histogram_min,
        histogram_max: row.histogram_max,
        buckets: parse_buckets(&row.buckets_json)?,
    })
}

struct LoadedRows {
    rows: Vec<AggregateRow>,
    raw: bool,
    rollup_5m: bool,
    rollup_1h: bool,
}

type OutputGroups =
    BTreeMap<(String, i64), (BTreeMap<String, String>, AggregateRow)>;

fn load_metric_rows(
    conn: &Connection,
    metric: &str,
    range: Option<(i64, i64)>,
) -> Result<LoadedRows, String> {
    let mut raw_rows = select_raw_rows(conn, Some(metric), range)?;
    let mut rollup_5m_rows = select_rollup_rows(
        conn,
        "rollup_5m",
        Some(metric),
        range,
        ROLLUP_5M_SECONDS,
    )?;
    let mut rollup_1h_rows = select_rollup_rows(
        conn,
        "rollup_1h",
        Some(metric),
        range,
        ROLLUP_1H_SECONDS,
    )?;
    let raw = !raw_rows.is_empty();
    let rollup_5m = !rollup_5m_rows.is_empty();
    let rollup_1h = !rollup_1h_rows.is_empty();
    raw_rows.append(&mut rollup_5m_rows);
    raw_rows.append(&mut rollup_1h_rows);
    Ok(LoadedRows {
        rows: raw_rows,
        raw,
        rollup_5m,
        rollup_1h,
    })
}

fn instant_from_rows(
    request: &TelemetryInstantQueryWire,
    now_ts: i64,
    rows: Vec<AggregateRow>,
) -> Result<TelemetryInstantQueryResultWire, String> {
    let mut rows = filter_rows(rows, request.kind, &request.filters)?;
    let kind = single_kind(&rows, request.kind)?;
    let (aggregation, quantile) = normalize_aggregation(
        kind,
        request.aggregation.as_deref(),
        request.quantile,
        false,
    )?;
    if kind == Some(TelemetryMetricKindWire::Gauge) {
        let cutoff = retention_cutoff(now_ts, request.staleness_seconds);
        rows = latest_gauge_rows(rows)
            .into_iter()
            .filter(|row| row.last_ts >= cutoff)
            .collect();
    }

    let mut grouped: BTreeMap<
        String,
        (BTreeMap<String, String>, AggregateRow),
    > = BTreeMap::new();
    for mut row in rows {
        let labels = labels_from_json(&row.labels_json)?;
        let projected = project_labels(&labels, request.group_by.as_deref());
        let key = labels_to_json(&projected)?;
        row.labels_json = key.clone();
        row.source.clear();
        match grouped.get_mut(&key) {
            Some((_, aggregate)) => aggregate.merge(row)?,
            None => {
                grouped.insert(key, (projected, row));
            }
        }
    }

    let mut values = Vec::new();
    for (_, (labels, row)) in grouped {
        let Some(value) = aggregate_value(&row, &aggregation, quantile, None)
        else {
            continue;
        };
        values.push(TelemetryInstantValueWire {
            metric: request.metric.clone(),
            kind: row.kind.as_str().to_string(),
            labels,
            ts: row.last_ts,
            value,
            count: if row.kind == TelemetryMetricKindWire::Histogram {
                row.histogram_count
            } else {
                row.sample_count
            },
            sum: if row.kind == TelemetryMetricKindWire::Histogram {
                row.histogram_sum
            } else {
                row.value_sum
            },
            min: if row.kind == TelemetryMetricKindWire::Histogram {
                row.histogram_min
            } else {
                row.value_min
            },
            max: if row.kind == TelemetryMetricKindWire::Histogram {
                row.histogram_max
            } else {
                row.value_max
            },
            buckets: row.buckets,
        });
    }
    Ok(TelemetryInstantQueryResultWire {
        schema_version: TELEMETRY_WIRE_SCHEMA_VERSION,
        metric: request.metric.clone(),
        aggregation,
        values,
    })
}

fn range_from_rows(
    request: &TelemetryRangeQueryWire,
    loaded: LoadedRows,
) -> Result<TelemetryRangeQueryResultWire, String> {
    let resolution = loaded_resolution(&loaded);
    let rows = filter_rows(loaded.rows, request.kind, &request.filters)?;
    let kind = single_kind(&rows, request.kind)?;
    let (aggregation, quantile) = normalize_aggregation(
        kind,
        request.aggregation.as_deref(),
        request.quantile,
        true,
    )?;
    let step = request.step_seconds as i64;

    let mut grouped: OutputGroups = BTreeMap::new();
    if kind == Some(TelemetryMetricKindWire::Gauge) {
        let mut latest_by_source: BTreeMap<
            (String, i64, String),
            AggregateRow,
        > = BTreeMap::new();
        for row in rows {
            let labels = labels_from_json(&row.labels_json)?;
            let projected =
                project_labels(&labels, request.group_by.as_deref());
            let labels_key = labels_to_json(&projected)?;
            let output_ts = output_bucket(
                row.last_ts,
                request.start_ts,
                request.end_ts,
                step,
            );
            let source_key =
                (labels_key.clone(), output_ts, row.source.clone());
            let candidate = row.latest_gauge();
            match latest_by_source.get_mut(&source_key) {
                Some(existing) if candidate.last_ts >= existing.last_ts => {
                    *existing = candidate;
                }
                Some(_) => {}
                None => {
                    latest_by_source.insert(source_key, candidate);
                }
            }
        }
        for ((labels_key, output_ts, _), mut row) in latest_by_source {
            let labels = labels_from_json(&labels_key)?;
            row.labels_json = labels_key.clone();
            row.source.clear();
            merge_output_group(
                &mut grouped,
                labels_key,
                output_ts,
                labels,
                row,
            )?;
        }
    } else {
        for mut row in rows {
            let labels = labels_from_json(&row.labels_json)?;
            let projected =
                project_labels(&labels, request.group_by.as_deref());
            let labels_key = labels_to_json(&projected)?;
            let output_ts = output_bucket(
                row.bucket_ts,
                request.start_ts,
                request.end_ts,
                step,
            );
            row.labels_json = labels_key.clone();
            row.source.clear();
            merge_output_group(
                &mut grouped,
                labels_key,
                output_ts,
                projected,
                row,
            )?;
        }
    }

    let mut by_series: BTreeMap<String, TelemetrySeriesWire> = BTreeMap::new();
    for ((labels_key, ts), (labels, row)) in grouped {
        let Some(value) = aggregate_value(
            &row,
            &aggregation,
            quantile,
            Some(request.step_seconds),
        ) else {
            continue;
        };
        by_series
            .entry(labels_key)
            .or_insert_with(|| TelemetrySeriesWire {
                labels,
                points: Vec::new(),
            })
            .points
            .push(TelemetryPointWire { ts, value });
    }
    for series in by_series.values_mut() {
        series.points.sort_by_key(|point| point.ts);
    }

    Ok(TelemetryRangeQueryResultWire {
        schema_version: TELEMETRY_WIRE_SCHEMA_VERSION,
        metric: request.metric.clone(),
        kind: kind
            .map(|value| value.as_str().to_string())
            .unwrap_or_else(|| "unknown".to_string()),
        aggregation,
        start_ts: request.start_ts,
        end_ts: request.end_ts,
        step_seconds: request.step_seconds,
        resolution,
        series: by_series.into_values().collect(),
    })
}

fn merge_output_group(
    grouped: &mut OutputGroups,
    labels_key: String,
    output_ts: i64,
    labels: BTreeMap<String, String>,
    row: AggregateRow,
) -> Result<(), String> {
    match grouped.get_mut(&(labels_key.clone(), output_ts)) {
        Some((_, aggregate)) => aggregate.merge(row),
        None => {
            grouped.insert((labels_key, output_ts), (labels, row));
            Ok(())
        }
    }
}

fn filter_rows(
    rows: Vec<AggregateRow>,
    requested_kind: Option<TelemetryMetricKindWire>,
    filters: &BTreeMap<String, String>,
) -> Result<Vec<AggregateRow>, String> {
    rows.into_iter()
        .filter(|row| requested_kind.map_or(true, |kind| row.kind == kind))
        .filter_map(|row| match labels_from_json(&row.labels_json) {
            Ok(labels)
                if filters
                    .iter()
                    .all(|(name, value)| labels.get(name) == Some(value)) =>
            {
                Some(Ok(row))
            }
            Ok(_) => None,
            Err(error) => Some(Err(error)),
        })
        .collect()
}

fn latest_gauge_rows(rows: Vec<AggregateRow>) -> Vec<AggregateRow> {
    let mut latest: BTreeMap<(String, String), AggregateRow> = BTreeMap::new();
    for row in rows {
        let key = (row.labels_json.clone(), row.source.clone());
        let candidate = row.latest_gauge();
        match latest.get_mut(&key) {
            Some(existing) if candidate.last_ts >= existing.last_ts => {
                *existing = candidate;
            }
            Some(_) => {}
            None => {
                latest.insert(key, candidate);
            }
        }
    }
    latest.into_values().collect()
}

fn single_kind(
    rows: &[AggregateRow],
    requested: Option<TelemetryMetricKindWire>,
) -> Result<Option<TelemetryMetricKindWire>, String> {
    let mut kind = requested;
    for row in rows {
        match kind {
            Some(existing) if existing != row.kind => {
                return Err(
                    "telemetry metric has mixed stored kinds".to_string()
                );
            }
            None => kind = Some(row.kind),
            _ => {}
        }
    }
    Ok(kind)
}

fn normalize_aggregation(
    kind: Option<TelemetryMetricKindWire>,
    requested: Option<&str>,
    requested_quantile: Option<f64>,
    is_range: bool,
) -> Result<(String, Option<f64>), String> {
    let default = match kind {
        Some(TelemetryMetricKindWire::Histogram) => "avg",
        _ => "sum",
    };
    let aggregation = requested.unwrap_or(default).trim().to_ascii_lowercase();
    let parsed_percentile = aggregation
        .strip_prefix('p')
        .and_then(|value| value.parse::<f64>().ok())
        .map(|value| value / 100.0);
    let quantile = requested_quantile.or(parsed_percentile);
    let normalized = if parsed_percentile.is_some() {
        "quantile".to_string()
    } else {
        aggregation
    };
    if normalized == "quantile" {
        if kind.is_some_and(|value| value != TelemetryMetricKindWire::Histogram)
        {
            return Err(
                "quantile aggregation requires a histogram metric".to_string()
            );
        }
        let value = quantile.unwrap_or(0.95);
        if !value.is_finite() || !(0.0..=1.0).contains(&value) {
            return Err(
                "telemetry quantile must be between 0 and 1".to_string()
            );
        }
        return Ok((normalized, Some(value)));
    }
    let allowed = ["sum", "avg", "min", "max", "count", "last"];
    if normalized == "rate" {
        if !is_range {
            return Err("rate aggregation requires a range query".to_string());
        }
        if kind == Some(TelemetryMetricKindWire::Gauge) {
            return Err(
                "rate aggregation is not valid for gauge metrics".to_string()
            );
        }
    } else if !allowed.contains(&normalized.as_str()) {
        return Err(format!(
            "unsupported telemetry aggregation {normalized:?}"
        ));
    }
    Ok((normalized, None))
}

fn aggregate_value(
    row: &AggregateRow,
    aggregation: &str,
    quantile: Option<f64>,
    step_seconds: Option<u64>,
) -> Option<f64> {
    let histogram = row.kind == TelemetryMetricKindWire::Histogram;
    match aggregation {
        "sum" => Some(if histogram {
            row.histogram_sum
        } else {
            row.value_sum
        }),
        "avg" => {
            let (sum, count) = if histogram {
                (row.histogram_sum, row.histogram_count)
            } else {
                (row.value_sum, row.sample_count)
            };
            (count > 0).then_some(sum / count as f64)
        }
        "min" => {
            if histogram {
                row.histogram_min
            } else {
                row.value_min
            }
        }
        "max" => {
            if histogram {
                row.histogram_max
            } else {
                row.value_max
            }
        }
        "last" => Some(row.last_value),
        "count" => Some(if histogram {
            row.histogram_count as f64
        } else {
            row.sample_count as f64
        }),
        "rate" => {
            let seconds = step_seconds.unwrap_or_default();
            if seconds == 0 {
                None
            } else {
                Some(
                    if histogram {
                        row.histogram_count as f64
                    } else {
                        row.value_sum
                    } / seconds as f64,
                )
            }
        }
        "quantile" => histogram_quantile(row, quantile.unwrap_or(0.95)),
        _ => None,
    }
}

fn histogram_quantile(row: &AggregateRow, quantile: f64) -> Option<f64> {
    if row.histogram_count == 0 {
        return None;
    }
    if quantile <= 0.0 {
        return row.histogram_min;
    }
    if quantile >= 1.0 {
        return row.histogram_max;
    }
    let target = quantile * row.histogram_count as f64;
    let mut previous_count = 0.0;
    let mut previous_bound = row
        .buckets
        .first()
        .map(|bucket| if bucket.le > 0.0 { 0.0 } else { bucket.le })
        .unwrap_or(0.0);
    for bucket in &row.buckets {
        let count = bucket.count as f64;
        if count >= target {
            if count <= previous_count {
                return Some(bucket.le);
            }
            let fraction = (target - previous_count) / (count - previous_count);
            return Some(
                previous_bound + (bucket.le - previous_bound) * fraction,
            );
        }
        previous_count = count;
        previous_bound = bucket.le;
    }
    row.histogram_max.or(Some(previous_bound))
}

fn project_labels(
    labels: &BTreeMap<String, String>,
    group_by: Option<&[String]>,
) -> BTreeMap<String, String> {
    match group_by {
        None => labels.clone(),
        Some(names) => names
            .iter()
            .filter_map(|name| {
                labels.get(name).map(|value| (name.clone(), value.clone()))
            })
            .collect(),
    }
}

fn output_bucket(ts: i64, start: i64, end: i64, step: i64) -> i64 {
    let bounded = ts.clamp(start, end);
    start + (bounded - start).div_euclid(step) * step
}

fn loaded_resolution(loaded: &LoadedRows) -> String {
    match (loaded.raw, loaded.rollup_5m, loaded.rollup_1h) {
        (true, false, false) => "raw",
        (false, true, false) => "5m",
        (false, false, true) => "1h",
        (false, false, false) => "raw",
        _ => "mixed",
    }
    .to_string()
}

fn merge_histogram_buckets(
    target: &mut Vec<TelemetryHistogramBucketWire>,
    incoming: &[TelemetryHistogramBucketWire],
) {
    for bucket in incoming {
        match target
            .iter_mut()
            .find(|existing| existing.le.to_bits() == bucket.le.to_bits())
        {
            Some(existing) => {
                existing.count = existing.count.saturating_add(bucket.count);
            }
            None => target.push(bucket.clone()),
        }
    }
    target.sort_by(|left, right| left.le.total_cmp(&right.le));
}

fn labels_to_json(labels: &BTreeMap<String, String>) -> Result<String, String> {
    serde_json::to_string(labels)
        .map_err(|e| format!("failed to serialize telemetry labels: {e}"))
}

fn labels_from_json(value: &str) -> Result<BTreeMap<String, String>, String> {
    serde_json::from_str(value)
        .map_err(|e| format!("invalid stored telemetry labels: {e}"))
}

fn parse_buckets(
    value: &str,
) -> Result<Vec<TelemetryHistogramBucketWire>, String> {
    serde_json::from_str(value)
        .map_err(|e| format!("invalid stored telemetry buckets: {e}"))
}

fn normalized_source(source: &str) -> String {
    let trimmed = source.trim();
    if trimmed.is_empty() {
        "unknown".to_string()
    } else {
        trimmed.to_string()
    }
}

fn derive_subsystem(metric: &str) -> String {
    metric
        .strip_prefix("sase_")
        .unwrap_or(metric)
        .split('_')
        .next()
        .filter(|value| !value.is_empty())
        .unwrap_or("other")
        .to_string()
}

fn floor_bucket(timestamp: i64, resolution: i64) -> i64 {
    timestamp.div_euclid(resolution) * resolution
}

fn retention_cutoff(now_ts: i64, seconds: u64) -> i64 {
    now_ts.saturating_sub(seconds.min(i64::MAX as u64) as i64)
}

fn min_option(left: Option<f64>, right: Option<f64>) -> Option<f64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn max_option(left: Option<f64>, right: Option<f64>) -> Option<f64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn u64_to_i64(value: u64) -> Result<i64, String> {
    i64::try_from(value)
        .map_err(|_| "telemetry count exceeds SQLite integer range".to_string())
}

fn i64_to_u64(value: i64) -> Result<u64, String> {
    u64::try_from(value)
        .map_err(|_| "stored telemetry count is negative".to_string())
}

fn validate_rollup_table(table: &str) -> Result<(), String> {
    if matches!(table, "rollup_5m" | "rollup_1h") {
        Ok(())
    } else {
        Err(format!("invalid telemetry rollup table {table:?}"))
    }
}

fn count_rows(conn: &Connection, table: &str) -> Result<u64, String> {
    validate_stats_table(table)?;
    let count: i64 = conn
        .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
            row.get(0)
        })
        .map_err(|e| e.to_string())?;
    i64_to_u64(count)
}

fn validate_stats_table(table: &str) -> Result<(), String> {
    if matches!(table, "samples" | "rollup_5m" | "rollup_1h") {
        Ok(())
    } else {
        Err(format!("invalid telemetry stats table {table:?}"))
    }
}

fn sample_bounds(
    conn: &Connection,
) -> Result<(Option<i64>, Option<i64>), String> {
    conn.query_row(
        r#"
        SELECT MIN(first_ts), MAX(last_ts) FROM (
            SELECT ts AS first_ts, ts AS last_ts FROM samples
            UNION ALL
            SELECT bucket_ts AS first_ts, last_ts FROM rollup_5m
            UNION ALL
            SELECT bucket_ts AS first_ts, last_ts FROM rollup_1h
        )
        "#,
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )
    .map_err(|e| e.to_string())
}

fn read_meta_i64(conn: &Connection, key: &str) -> Result<Option<i64>, String> {
    let raw: Option<String> = conn
        .query_row("SELECT value FROM meta WHERE key = ?1", [key], |row| {
            row.get(0)
        })
        .optional()
        .map_err(|e| e.to_string())?;
    raw.map(|value| {
        value
            .parse::<i64>()
            .map_err(|e| format!("invalid telemetry metadata {key:?}: {e}"))
    })
    .transpose()
}

fn read_subsystem_write_metadata(
    conn: &Connection,
) -> Result<BTreeMap<String, i64>, String> {
    let mut stmt = conn
        .prepare(
            "SELECT key, value FROM meta WHERE key LIKE 'last_write_subsystem:%' ORDER BY key",
        )
        .map_err(|e| e.to_string())?;
    let rows = stmt
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(|e| e.to_string())?;
    let mut result = BTreeMap::new();
    for row in rows {
        let (key, value) = row.map_err(|e| e.to_string())?;
        let subsystem = key
            .strip_prefix("last_write_subsystem:")
            .unwrap_or(&key)
            .to_string();
        let timestamp = value.parse::<i64>().map_err(|e| {
            format!("invalid telemetry subsystem metadata {key:?}: {e}")
        })?;
        result.insert(subsystem, timestamp);
    }
    Ok(result)
}

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs().min(i64::MAX as u64) as i64)
        .unwrap_or(0)
}

fn is_sqlite_corruption_error(error: &str) -> bool {
    let lower = error.to_ascii_lowercase();
    lower.contains("database disk image is malformed")
        || lower.contains("file is not a database")
        || lower.contains("not a database")
        || lower.contains("malformed database schema")
        || lower.contains("unsupported file format")
}

fn quarantine_corrupt_store(store_path: &Path) -> Result<(), String> {
    let quarantined = corrupt_store_quarantine_path(store_path);
    match fs::rename(store_path, &quarantined) {
        Ok(()) => {}
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(format!(
                "failed to quarantine unusable telemetry store {}: {error}",
                store_path.display()
            ));
        }
    }
    for suffix in ["-wal", "-shm"] {
        let source = sqlite_sidecar_path(store_path, suffix);
        let target = sqlite_sidecar_path(&quarantined, suffix);
        match fs::rename(&source, &target) {
            Ok(()) => {}
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => {
                return Err(format!(
                    "failed to quarantine telemetry store sidecar {}: {error}",
                    source.display()
                ));
            }
        }
    }
    Ok(())
}

fn corrupt_store_quarantine_path(store_path: &Path) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let file_name = store_path
        .file_name()
        .map(|name| name.to_string_lossy())
        .unwrap_or_else(|| "metrics.sqlite".into());
    store_path.with_file_name(format!("{file_name}.corrupt-{nanos}"))
}

fn sqlite_sidecar_path(store_path: &Path, suffix: &str) -> PathBuf {
    let mut raw = store_path.as_os_str().to_os_string();
    raw.push(suffix);
    PathBuf::from(raw)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};
    use std::thread;
    use tempfile::tempdir;

    fn sample(
        ts: i64,
        metric: &str,
        kind: TelemetryMetricKindWire,
        source: &str,
        value: f64,
    ) -> TelemetrySampleWire {
        TelemetrySampleWire {
            ts,
            metric: metric.to_string(),
            kind,
            labels: BTreeMap::new(),
            source: source.to_string(),
            value,
            count: 0,
            sum: 0.0,
            min: None,
            max: None,
            buckets: Vec::new(),
            subsystem: None,
        }
    }

    fn batch(
        samples: Vec<TelemetrySampleWire>,
        now_ts: i64,
    ) -> TelemetryRecordBatchWire {
        TelemetryRecordBatchWire {
            samples,
            now_ts: Some(now_ts),
            retention: TelemetryRetentionWire::default(),
        }
    }

    fn instant_request(metric: &str, now_ts: i64) -> TelemetryInstantQueryWire {
        TelemetryInstantQueryWire {
            metric: metric.to_string(),
            kind: None,
            filters: BTreeMap::new(),
            group_by: None,
            aggregation: None,
            quantile: None,
            now_ts: Some(now_ts),
            staleness_seconds: 30,
        }
    }

    fn test_data_cleanup_request(dry_run: bool) -> TelemetryCleanupRequestWire {
        TelemetryCleanupRequestWire {
            label_matches: BTreeMap::from([
                (
                    "llm_provider".into(),
                    vec!["test-provider".into(), "fakey".into()],
                ),
                ("workflow".into(), vec!["test-workflow".into()]),
            ]),
            dry_run,
        }
    }

    #[test]
    fn counter_deltas_aggregate_and_group() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("metrics.sqlite");
        let mut first = sample(
            100,
            "sase_agent_runs_total",
            TelemetryMetricKindWire::Counter,
            "runner-1",
            2.0,
        );
        first.labels.insert("provider".into(), "claude".into());
        let mut second = first.clone();
        second.ts = 110;
        second.source = "runner-2".into();
        second.value = 3.0;
        let mut third = first.clone();
        third.ts = 115;
        third.labels.insert("provider".into(), "codex".into());
        third.value = 4.0;

        let result = record_batch(
            &path,
            batch(vec![first, second, third], 120),
            Duration::from_secs(1),
        )
        .unwrap();
        assert_eq!(result.samples_recorded, 3);

        let mut request = instant_request("sase_agent_runs_total", 120);
        request.group_by = Some(vec!["provider".into()]);
        let result =
            query_instant(&path, request, Duration::from_secs(1)).unwrap();
        assert_eq!(result.values.len(), 2);
        assert_eq!(result.values[0].labels["provider"], "claude");
        assert_eq!(result.values[0].value, 5.0);
        assert_eq!(result.values[1].labels["provider"], "codex");
        assert_eq!(result.values[1].value, 4.0);

        let range = query_range(
            &path,
            TelemetryRangeQueryWire {
                metric: "sase_agent_runs_total".into(),
                kind: None,
                start_ts: 100,
                end_ts: 159,
                step_seconds: 60,
                filters: BTreeMap::new(),
                group_by: Some(Vec::new()),
                aggregation: Some("rate".into()),
                quantile: None,
            },
            Duration::from_secs(1),
        )
        .unwrap();
        assert_eq!(range.series.len(), 1);
        assert_eq!(range.series[0].points[0].value, 9.0 / 60.0);
    }

    #[test]
    fn gauge_instant_query_uses_latest_live_value_per_source() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("metrics.sqlite");
        let samples = vec![
            sample(
                100,
                "sase_agent_active",
                TelemetryMetricKindWire::Gauge,
                "runner-1",
                2.0,
            ),
            sample(
                108,
                "sase_agent_active",
                TelemetryMetricKindWire::Gauge,
                "runner-1",
                3.0,
            ),
            sample(
                105,
                "sase_agent_active",
                TelemetryMetricKindWire::Gauge,
                "runner-2",
                4.0,
            ),
            sample(
                60,
                "sase_agent_active",
                TelemetryMetricKindWire::Gauge,
                "dead-runner",
                20.0,
            ),
        ];
        record_batch(&path, batch(samples, 110), Duration::from_secs(1))
            .unwrap();

        let mut request = instant_request("sase_agent_active", 110);
        request.group_by = Some(Vec::new());
        let result =
            query_instant(&path, request, Duration::from_secs(1)).unwrap();
        assert_eq!(result.values.len(), 1);
        assert_eq!(result.values[0].value, 7.0);
        assert_eq!(result.values[0].ts, 108);

        let mut stale = instant_request("sase_agent_active", 200);
        stale.group_by = Some(Vec::new());
        assert!(query_instant(&path, stale, Duration::from_secs(1))
            .unwrap()
            .values
            .is_empty());
    }

    #[test]
    fn histogram_quantile_interpolates_cumulative_buckets() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("metrics.sqlite");
        let mut histogram = sample(
            100,
            "sase_agent_run_duration_seconds",
            TelemetryMetricKindWire::Histogram,
            "runner-1",
            0.0,
        );
        histogram.count = 10;
        histogram.sum = 12.0;
        histogram.min = Some(0.2);
        histogram.max = Some(3.0);
        histogram.buckets = vec![
            TelemetryHistogramBucketWire { le: 1.0, count: 5 },
            TelemetryHistogramBucketWire { le: 2.0, count: 9 },
        ];
        record_batch(
            &path,
            batch(vec![histogram], 110),
            Duration::from_secs(1),
        )
        .unwrap();

        let mut request =
            instant_request("sase_agent_run_duration_seconds", 110);
        request.aggregation = Some("p50".into());
        request.group_by = Some(Vec::new());
        let result =
            query_instant(&path, request, Duration::from_secs(1)).unwrap();
        assert_eq!(result.aggregation, "quantile");
        assert_eq!(result.values[0].value, 1.0);
        assert_eq!(result.values[0].count, 10);
        assert_eq!(result.values[0].sum, 12.0);
    }

    #[test]
    fn retention_folds_through_both_rollup_tiers_before_deletion() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("metrics.sqlite");
        record_batch(
            &path,
            batch(
                vec![sample(
                    100,
                    "sase_agent_runs_total",
                    TelemetryMetricKindWire::Counter,
                    "runner-1",
                    4.0,
                )],
                100,
            ),
            Duration::from_secs(1),
        )
        .unwrap();

        let first = prune(
            &path,
            TelemetryPruneRequestWire {
                now_ts: Some(1_000),
                retention: TelemetryRetentionWire {
                    raw_seconds: 100,
                    rollup_5m_seconds: 10_000,
                    rollup_1h_seconds: 100_000,
                },
            },
            Duration::from_secs(1),
        )
        .unwrap();
        assert_eq!(first.raw_rows_folded, 1);
        let stats = store_stats(&path, Duration::from_secs(1)).unwrap();
        assert_eq!(stats.raw_sample_count, 0);
        assert_eq!(stats.rollup_5m_count, 1);

        let range = query_range(
            &path,
            TelemetryRangeQueryWire {
                metric: "sase_agent_runs_total".into(),
                kind: None,
                start_ts: 0,
                end_ts: 299,
                step_seconds: 300,
                filters: BTreeMap::new(),
                group_by: Some(Vec::new()),
                aggregation: Some("sum".into()),
                quantile: None,
            },
            Duration::from_secs(1),
        )
        .unwrap();
        assert_eq!(range.resolution, "5m");
        assert_eq!(range.series[0].points[0].value, 4.0);

        let second = prune(
            &path,
            TelemetryPruneRequestWire {
                now_ts: Some(20_000),
                retention: TelemetryRetentionWire {
                    raw_seconds: 100,
                    rollup_5m_seconds: 100,
                    rollup_1h_seconds: 100_000,
                },
            },
            Duration::from_secs(1),
        )
        .unwrap();
        assert_eq!(second.rollup_5m_rows_folded, 1);
        let stats = store_stats(&path, Duration::from_secs(1)).unwrap();
        assert_eq!(stats.rollup_5m_count, 0);
        assert_eq!(stats.rollup_1h_count, 1);

        let third = prune(
            &path,
            TelemetryPruneRequestWire {
                now_ts: Some(200_000),
                retention: TelemetryRetentionWire {
                    raw_seconds: 100,
                    rollup_5m_seconds: 100,
                    rollup_1h_seconds: 100,
                },
            },
            Duration::from_secs(1),
        )
        .unwrap();
        assert_eq!(third.rollup_1h_rows_deleted, 1);
    }

    #[test]
    fn exact_label_cleanup_previews_and_deletes_every_tier() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("metrics.sqlite");
        let metric = "sase_agent_runs_total";

        let mut old_test = sample(
            100,
            metric,
            TelemetryMetricKindWire::Counter,
            "old-test",
            1.0,
        );
        old_test
            .labels
            .insert("llm_provider".into(), "test-provider".into());
        let mut old_production = old_test.clone();
        old_production.source = "old-production".into();
        old_production
            .labels
            .insert("llm_provider".into(), "codex".into());

        let mut middle_test = sample(
            1_000,
            metric,
            TelemetryMetricKindWire::Counter,
            "middle-test",
            1.0,
        );
        middle_test
            .labels
            .insert("workflow".into(), "test-workflow".into());
        let mut middle_near_miss = middle_test.clone();
        middle_near_miss.source = "middle-near-miss".into();
        middle_near_miss
            .labels
            .insert("workflow".into(), "test-workflow-copy".into());

        record_batch(
            &path,
            batch(
                vec![old_test, old_production, middle_test, middle_near_miss],
                2_000,
            ),
            Duration::from_secs(1),
        )
        .unwrap();
        prune(
            &path,
            TelemetryPruneRequestWire {
                now_ts: Some(2_000),
                retention: TelemetryRetentionWire {
                    raw_seconds: 500,
                    rollup_5m_seconds: 100_000,
                    rollup_1h_seconds: 100_000,
                },
            },
            Duration::from_secs(1),
        )
        .unwrap();
        prune(
            &path,
            TelemetryPruneRequestWire {
                now_ts: Some(10_000),
                retention: TelemetryRetentionWire {
                    raw_seconds: 100_000,
                    rollup_5m_seconds: 9_500,
                    rollup_1h_seconds: 100_000,
                },
            },
            Duration::from_secs(1),
        )
        .unwrap();

        let mut raw_test = sample(
            10_000,
            metric,
            TelemetryMetricKindWire::Counter,
            "raw-test",
            1.0,
        );
        raw_test
            .labels
            .insert("llm_provider".into(), "fakey".into());
        let mut raw_near_miss = sample(
            9_000,
            metric,
            TelemetryMetricKindWire::Counter,
            "raw-near-miss",
            1.0,
        );
        raw_near_miss
            .labels
            .insert("llm_provider".into(), "fakey-preview".into());
        record_batch(
            &path,
            batch(vec![raw_test, raw_near_miss], 11_000),
            Duration::from_secs(1),
        )
        .unwrap();

        let stats_before = store_stats(&path, Duration::from_secs(1)).unwrap();
        assert_eq!(stats_before.raw_sample_count, 2);
        assert_eq!(stats_before.rollup_5m_count, 2);
        assert_eq!(stats_before.rollup_1h_count, 2);
        assert_eq!(stats_before.last_write_ts, Some(11_000));

        let preview = cleanup_matching_labels(
            &path,
            test_data_cleanup_request(true),
            Duration::from_secs(1),
        )
        .unwrap();
        assert_eq!(preview.raw_rows, 1);
        assert_eq!(preview.rollup_5m_rows, 1);
        assert_eq!(preview.rollup_1h_rows, 1);
        assert_eq!(preview.total_rows, 3);
        assert!(preview.dry_run);
        assert!(!preview.vacuumed);
        assert_eq!(
            preview.store_size_before_bytes,
            preview.store_size_after_bytes
        );
        assert_eq!(
            store_stats(&path, Duration::from_secs(1)).unwrap(),
            stats_before
        );

        let deleted = cleanup_matching_labels(
            &path,
            test_data_cleanup_request(false),
            Duration::from_secs(1),
        )
        .unwrap();
        assert_eq!(deleted.raw_rows, 1);
        assert_eq!(deleted.rollup_5m_rows, 1);
        assert_eq!(deleted.rollup_1h_rows, 1);
        assert_eq!(deleted.total_rows, 3);
        assert!(deleted.vacuumed);
        assert!(
            deleted.store_size_after_bytes <= deleted.store_size_before_bytes
        );
        assert_eq!(
            deleted.reclaimed_bytes,
            deleted
                .store_size_before_bytes
                .saturating_sub(deleted.store_size_after_bytes)
        );

        let stats_after = store_stats(&path, Duration::from_secs(1)).unwrap();
        assert_eq!(stats_after.raw_sample_count, 1);
        assert_eq!(stats_after.rollup_5m_count, 1);
        assert_eq!(stats_after.rollup_1h_count, 1);
        assert_eq!(stats_after.last_write_ts, Some(9_000));
        assert_eq!(stats_after.last_write_by_subsystem["agent"], 9_000);

        let repeated = cleanup_matching_labels(
            &path,
            test_data_cleanup_request(false),
            Duration::from_secs(1),
        )
        .unwrap();
        assert_eq!(repeated.total_rows, 0);
        assert!(!repeated.vacuumed);
        assert_eq!(
            store_stats(&path, Duration::from_secs(1)).unwrap(),
            stats_after
        );
    }

    #[test]
    fn concurrent_writers_preserve_every_delta() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("metrics.sqlite");
        let barrier = Arc::new(Barrier::new(4));
        let mut handles = Vec::new();
        for worker in 0..4 {
            let path = path.clone();
            let barrier = barrier.clone();
            handles.push(thread::spawn(move || {
                barrier.wait();
                let samples = (0..25)
                    .map(|offset| {
                        sample(
                            1_000 + offset,
                            "sase_agent_runs_total",
                            TelemetryMetricKindWire::Counter,
                            &format!("runner-{worker}"),
                            1.0,
                        )
                    })
                    .collect();
                record_batch(
                    &path,
                    batch(samples, 1_100),
                    Duration::from_secs(5),
                )
                .unwrap();
            }));
        }
        for handle in handles {
            handle.join().unwrap();
        }
        let mut request = instant_request("sase_agent_runs_total", 1_100);
        request.group_by = Some(Vec::new());
        let result =
            query_instant(&path, request, Duration::from_secs(1)).unwrap();
        assert_eq!(result.values[0].value, 100.0);
    }

    #[test]
    fn corrupt_store_is_quarantined_and_recreated() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("metrics.sqlite");
        fs::write(&path, b"not a sqlite database").unwrap();

        record_batch(
            &path,
            batch(
                vec![sample(
                    100,
                    "sase_agent_runs_total",
                    TelemetryMetricKindWire::Counter,
                    "runner-1",
                    1.0,
                )],
                100,
            ),
            Duration::from_secs(1),
        )
        .unwrap();
        assert_eq!(
            store_stats(&path, Duration::from_secs(1))
                .unwrap()
                .raw_sample_count,
            1
        );
        let quarantined = fs::read_dir(temp.path())
            .unwrap()
            .filter_map(Result::ok)
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .filter(|name| name.starts_with("metrics.sqlite.corrupt-"))
            .collect::<Vec<_>>();
        assert_eq!(quarantined.len(), 1);
    }
}
