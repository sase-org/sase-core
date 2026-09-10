use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::{DateTime, Duration as ChronoDuration, SecondsFormat, Utc};
use fs2::FileExt;

use super::tabs::{tab_key_for, tabs_and_counts_for};
use super::wire::{
    notification_activity_at, NotificationAgentKeyWire, NotificationCountsWire,
    NotificationPlusOneActionWire, NotificationPlusOneOutcomeWire,
    NotificationPlusOneRequestWire, NotificationPlusOneWire,
    NotificationStateUpdateWire, NotificationStoreSnapshotWire,
    NotificationStoreStatsWire, NotificationUpdateOutcomeWire,
    NotificationUpsertActionWire, NotificationUpsertOutcomeWire,
    NotificationUpsertRequestWire, NotificationWire,
    NOTIFICATION_PLUS_ONE_MAX_ENTRIES, NOTIFICATION_PLUS_ONE_NOTE_MAX_CHARS,
    NOTIFICATION_STORE_WIRE_SCHEMA_VERSION,
};

const STALE_TEMP_MAX_AGE: Duration = Duration::from_secs(24 * 60 * 60);
const NOTIFICATION_COMPACTION_RETENTION_DAYS: i64 = 14;
const NOTIFICATION_COMPACTION_DISMISSED_ROW_THRESHOLD: usize = 1_000;
const NOTIFICATION_COMPACTION_FILE_SIZE_THRESHOLD_BYTES: u64 = 4 * 1024 * 1024;

pub fn read_notifications_snapshot(
    path: &Path,
    include_dismissed: bool,
) -> Result<NotificationStoreSnapshotWire, String> {
    read_notifications_snapshot_with_options(path, include_dismissed, false)
}

/// Read the user-facing current notification state, atomically reconciling
/// every due or malformed legacy snooze before returning the snapshot.
pub fn read_current_notifications_snapshot(
    path: &Path,
    include_dismissed: bool,
) -> Result<NotificationStoreSnapshotWire, String> {
    read_notifications_snapshot_expiring_snoozes(
        path,
        include_dismissed,
        DateTime::<Utc>::from(SystemTime::now()),
    )
}

#[allow(clippy::incompatible_msrv)]
pub fn read_notifications_snapshot_with_options(
    path: &Path,
    include_dismissed: bool,
    expire_due_snoozes: bool,
) -> Result<NotificationStoreSnapshotWire, String> {
    if expire_due_snoozes {
        return read_current_notifications_snapshot(path, include_dismissed);
    }
    let Some(parent) = path.parent() else {
        return Err(format!(
            "notification path has no parent: {}",
            path.display()
        ));
    };
    if !path.exists() {
        return Ok(snapshot_from_rows(
            Vec::new(),
            NotificationStoreStatsWire::default(),
        ));
    }

    fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    let lock = open_lock_file(path)?;
    if notification_file_size(path)
        >= NOTIFICATION_COMPACTION_FILE_SIZE_THRESHOLD_BYTES
    {
        lock.lock_exclusive().map_err(|e| e.to_string())?;
        let result = read_notifications_snapshot_rewriting_if_needed(
            path,
            include_dismissed,
            DateTime::<Utc>::from(SystemTime::now()),
            false,
        );
        unlock(lock)?;
        return result;
    }

    FileExt::lock_shared(&lock).map_err(|e| e.to_string())?;
    let result = read_rows(path, include_dismissed);
    unlock(lock)?;
    let (notifications, stats) = result?;
    if should_recheck_for_notification_compaction(
        path,
        &notifications,
        &stats,
        include_dismissed,
    ) {
        return read_notifications_snapshot_with_exclusive_compaction(
            path,
            include_dismissed,
            DateTime::<Utc>::from(SystemTime::now()),
        );
    }
    Ok(snapshot_from_rows(notifications, stats))
}

fn read_notifications_snapshot_expiring_snoozes(
    path: &Path,
    include_dismissed: bool,
    now: DateTime<Utc>,
) -> Result<NotificationStoreSnapshotWire, String> {
    let lock = open_lock_file(path)?;
    lock.lock_exclusive().map_err(|e| e.to_string())?;

    let result = read_notifications_snapshot_rewriting_if_needed(
        path,
        include_dismissed,
        now,
        true,
    );

    unlock(lock)?;
    result
}

fn read_notifications_snapshot_with_exclusive_compaction(
    path: &Path,
    include_dismissed: bool,
    now: DateTime<Utc>,
) -> Result<NotificationStoreSnapshotWire, String> {
    let lock = open_lock_file(path)?;
    lock.lock_exclusive().map_err(|e| e.to_string())?;
    let result = read_notifications_snapshot_rewriting_if_needed(
        path,
        include_dismissed,
        now,
        false,
    );
    unlock(lock)?;
    result
}

fn read_notifications_snapshot_rewriting_if_needed(
    path: &Path,
    include_dismissed: bool,
    now: DateTime<Utc>,
    expire_due_snoozes: bool,
) -> Result<NotificationStoreSnapshotWire, String> {
    let (mut rows, _) = read_rows_unlocked(path, true)?;
    let expired_ids = if expire_due_snoozes {
        expire_snoozes_in_rows(&mut rows, now)
    } else {
        Vec::new()
    };
    let compaction = maybe_compact_notifications_unlocked(path, rows, now)?;
    if !expired_ids.is_empty() || compaction.archived_count > 0 {
        write_notifications_atomic(path, &compaction.rows)?;
    }
    let (notifications, stats) = read_rows_unlocked(path, include_dismissed)?;
    let mut snapshot = snapshot_from_rows(notifications, stats);
    snapshot.expired_ids = expired_ids;
    Ok(snapshot)
}

pub fn append_notification(
    path: &Path,
    notification: &NotificationWire,
) -> Result<NotificationUpdateOutcomeWire, String> {
    append_notification_with_options(path, notification, true)
}

pub fn append_notification_counts(
    path: &Path,
    notification: &NotificationWire,
) -> Result<NotificationUpdateOutcomeWire, String> {
    append_notification_with_options(path, notification, false)
}

fn append_notification_with_options(
    path: &Path,
    notification: &NotificationWire,
    include_notifications: bool,
) -> Result<NotificationUpdateOutcomeWire, String> {
    let lock = open_lock_file(path)?;
    lock.lock_exclusive().map_err(|e| e.to_string())?;

    let append_result = append_notification_unlocked(path, notification);

    let result = match append_result {
        Ok(()) => {
            if !include_notifications {
                Ok(outcome_without_rows(0, 0, 1, false, Vec::new()))
            } else {
                let (notifications, stats) = read_rows_unlocked(path, true)?;
                Ok(outcome_from_rows(
                    notifications,
                    stats,
                    0,
                    0,
                    1,
                    false,
                    Vec::new(),
                ))
            }
        }
        Err(e) => Err(e),
    };
    unlock(lock)?;
    result
}

fn append_notification_unlocked(
    path: &Path,
    notification: &NotificationWire,
) -> Result<(), String> {
    let parent = ensure_parent(path)?;
    fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|e| e.to_string())?;
    serde_json::to_writer(&mut file, notification)
        .map_err(|e| format!("failed to serialize notification: {e}"))?;
    file.write_all(b"\n").map_err(|e| e.to_string())?;
    file.flush().map_err(|e| e.to_string())?;
    Ok(())
}

pub fn rewrite_notifications(
    path: &Path,
    notifications: &[NotificationWire],
) -> Result<NotificationUpdateOutcomeWire, String> {
    rewrite_notifications_with_options(path, notifications, true)
}

pub fn rewrite_notifications_counts(
    path: &Path,
    notifications: &[NotificationWire],
) -> Result<NotificationUpdateOutcomeWire, String> {
    rewrite_notifications_with_options(path, notifications, false)
}

fn rewrite_notifications_with_options(
    path: &Path,
    notifications: &[NotificationWire],
    include_notifications: bool,
) -> Result<NotificationUpdateOutcomeWire, String> {
    let lock = open_lock_file(path)?;
    lock.lock_exclusive().map_err(|e| e.to_string())?;
    let result = merge_and_rewrite_notifications_unlocked(path, notifications)
        .and_then(|()| {
            if !include_notifications {
                return Ok(outcome_without_rows(
                    notifications.len() as u64,
                    notifications.len() as u64,
                    0,
                    true,
                    Vec::new(),
                ));
            }
            let (rows, stats) = read_rows_unlocked(path, true)?;
            Ok(outcome_from_rows(
                rows,
                stats,
                notifications.len() as u64,
                notifications.len() as u64,
                0,
                true,
                Vec::new(),
            ))
        });
    unlock(lock)?;
    result
}

pub fn apply_notification_state_update(
    path: &Path,
    update: &NotificationStateUpdateWire,
) -> Result<NotificationUpdateOutcomeWire, String> {
    apply_notification_state_update_with_options(path, update, true)
}

pub fn apply_notification_state_update_counts(
    path: &Path,
    update: &NotificationStateUpdateWire,
) -> Result<NotificationUpdateOutcomeWire, String> {
    apply_notification_state_update_with_options(path, update, false)
}

fn apply_notification_state_update_with_options(
    path: &Path,
    update: &NotificationStateUpdateWire,
    include_notifications: bool,
) -> Result<NotificationUpdateOutcomeWire, String> {
    let lock = open_lock_file(path)?;
    lock.lock_exclusive().map_err(|e| e.to_string())?;

    let result = (|| {
        if let NotificationStateUpdateWire::RewriteAll { notifications } =
            update
        {
            merge_and_rewrite_notifications_unlocked(path, notifications)?;
            if !include_notifications {
                return Ok(outcome_without_rows(
                    notifications.len() as u64,
                    notifications.len() as u64,
                    0,
                    true,
                    Vec::new(),
                ));
            }
            let (rows, stats) = read_rows_unlocked(path, true)?;
            return Ok(outcome_from_rows(
                rows,
                stats,
                notifications.len() as u64,
                notifications.len() as u64,
                0,
                true,
                Vec::new(),
            ));
        }

        let (mut rows, stats_before) = read_rows_unlocked(path, true)?;
        let mut matched_count = 0_u64;
        let mut changed_count = 0_u64;
        let mut expired_ids = Vec::new();

        match update {
            NotificationStateUpdateWire::MarkRead { id } => {
                for n in &mut rows {
                    if n.id == *id {
                        matched_count += 1;
                        if !n.read {
                            n.read = true;
                            changed_count += 1;
                        }
                        break;
                    }
                }
            }
            NotificationStateUpdateWire::MarkAllRead => {
                for n in &mut rows {
                    if !n.read {
                        matched_count += 1;
                        n.read = true;
                        changed_count += 1;
                    }
                }
            }
            NotificationStateUpdateWire::MarkTabRead { tab_key } => {
                for n in &mut rows {
                    if n.read {
                        continue;
                    }
                    if tab_key_for(n).0 == *tab_key {
                        matched_count += 1;
                        n.read = true;
                        changed_count += 1;
                    }
                }
            }
            NotificationStateUpdateWire::MarkDismissed { id } => {
                for n in &mut rows {
                    if n.id == *id {
                        matched_count += 1;
                        if !n.dismissed || n.snooze_until.is_some() {
                            n.dismissed = true;
                            n.snooze_until = None;
                            changed_count += 1;
                        }
                        break;
                    }
                }
            }
            NotificationStateUpdateWire::MarkManyDismissed { ids } => {
                let ids: BTreeSet<&str> =
                    ids.iter().map(String::as_str).collect();
                for n in &mut rows {
                    if ids.contains(n.id.as_str()) {
                        matched_count += 1;
                        if !n.dismissed || n.snooze_until.is_some() {
                            n.dismissed = true;
                            n.snooze_until = None;
                            changed_count += 1;
                        }
                    }
                }
            }
            NotificationStateUpdateWire::MarkMuted { id, muted } => {
                for n in &mut rows {
                    if n.id == *id {
                        matched_count += 1;
                        if n.muted != *muted
                            || (!*muted && n.snooze_until.is_some())
                        {
                            n.muted = *muted;
                            if !*muted {
                                n.snooze_until = None;
                            }
                            changed_count += 1;
                        }
                        break;
                    }
                }
            }
            NotificationStateUpdateWire::MarkManyMuted { ids, muted } => {
                let ids: BTreeSet<&str> =
                    ids.iter().map(String::as_str).collect();
                for n in &mut rows {
                    if ids.contains(n.id.as_str()) {
                        matched_count += 1;
                        if n.muted != *muted
                            || (!*muted && n.snooze_until.is_some())
                        {
                            n.muted = *muted;
                            if !*muted {
                                n.snooze_until = None;
                            }
                            changed_count += 1;
                        }
                    }
                }
            }
            NotificationStateUpdateWire::MarkSnoozed { id, until } => {
                let until = validated_snooze_deadline(until)?;
                for n in &mut rows {
                    if n.id == *id {
                        if n.dismissed {
                            break;
                        }
                        matched_count += 1;
                        if !n.muted
                            || n.snooze_until.as_deref() != Some(until.as_str())
                        {
                            n.muted = true;
                            n.snooze_until = Some(until.clone());
                            changed_count += 1;
                        }
                        break;
                    }
                }
            }
            NotificationStateUpdateWire::MarkManySnoozed { ids, until } => {
                let until = validated_snooze_deadline(until)?;
                let ids: BTreeSet<&str> =
                    ids.iter().map(String::as_str).collect();
                for n in &mut rows {
                    if ids.contains(n.id.as_str()) && !n.dismissed {
                        matched_count += 1;
                        if !n.muted
                            || n.snooze_until.as_deref() != Some(until.as_str())
                        {
                            n.muted = true;
                            n.snooze_until = Some(until.clone());
                            changed_count += 1;
                        }
                    }
                }
            }
            NotificationStateUpdateWire::ExpireSnoozes { now } => {
                let now = parse_aware_utc(now, "expiry instant")?;
                expired_ids = expire_snoozes_in_rows(&mut rows, now);
                matched_count = expired_ids.len() as u64;
                changed_count = matched_count;
            }
            NotificationStateUpdateWire::DismissMatchingAgents { agents } => {
                for n in &mut rows {
                    if n.dismissed {
                        continue;
                    }
                    if matches_agent_notification(n, agents) {
                        matched_count += 1;
                        n.dismissed = true;
                        n.snooze_until = None;
                        changed_count += 1;
                    }
                }
            }
            NotificationStateUpdateWire::DismissAgentCompletionsMatchingAgents {
                agents,
            } => {
                for n in &mut rows {
                    if n.dismissed {
                        continue;
                    }
                    if matches_agent_completion_notification_for_agents(n, agents) {
                        matched_count += 1;
                        n.dismissed = true;
                        n.snooze_until = None;
                        changed_count += 1;
                    }
                }
            }
            NotificationStateUpdateWire::DismissAgentCompletions => {
                for n in &mut rows {
                    if n.dismissed {
                        continue;
                    }
                    if matches_agent_completion_notification(n) {
                        matched_count += 1;
                        n.dismissed = true;
                        n.snooze_until = None;
                        changed_count += 1;
                    }
                }
            }
            NotificationStateUpdateWire::RewriteAll { .. } => unreachable!(),
        }

        if changed_count > 0 {
            merge_and_rewrite_notifications_unlocked(path, &rows)?;
            if !include_notifications {
                return Ok(outcome_without_rows(
                    matched_count,
                    changed_count,
                    0,
                    true,
                    expired_ids,
                ));
            }
            let (rows, stats) = read_rows_unlocked(path, true)?;
            Ok(outcome_from_rows(
                rows,
                stats,
                matched_count,
                changed_count,
                0,
                true,
                expired_ids,
            ))
        } else if !include_notifications {
            Ok(outcome_without_rows(
                matched_count,
                changed_count,
                0,
                false,
                expired_ids,
            ))
        } else {
            Ok(outcome_from_rows(
                rows,
                stats_before,
                matched_count,
                changed_count,
                0,
                false,
                expired_ids,
            ))
        }
    })();

    unlock(lock)?;
    result
}

/// Append one plus-one entry, addressed by exact id or `(sender, dedup_key)`.
///
/// A +1 never mutates `timestamp`, `resurfaced_at`, or any state flag. Missing
/// targets return `action: no_match` without writing. Invalid notes, senders,
/// or timestamps are errors.
pub fn append_notification_plus_one(
    path: &Path,
    request: &NotificationPlusOneRequestWire,
) -> Result<NotificationPlusOneOutcomeWire, String> {
    validate_plus_one_target(request)?;
    let prepared = PreparedPlusOne::from_parts(
        &request.timestamp,
        &request.sender,
        &request.note,
    )?;
    let lock = open_lock_file(path)?;
    lock.lock_exclusive().map_err(|e| e.to_string())?;

    let result = (|| {
        let (mut rows, _) = read_rows_unlocked(path, true)?;
        let Some(index) = find_plus_one_target(&rows, request) else {
            return Ok(NotificationPlusOneOutcomeWire {
                schema_version: NOTIFICATION_STORE_WIRE_SCHEMA_VERSION,
                action: NotificationPlusOneActionWire::NoMatch,
                id: None,
                plus_one_count: 0,
                plus_ones_dropped: 0,
                notification: None,
            });
        };
        apply_prepared_plus_one(&mut rows[index], &prepared);
        let notification = rows[index].clone();
        merge_and_rewrite_notifications_unlocked(path, &rows)?;
        Ok(applied_plus_one_outcome(notification))
    })();

    unlock(lock)?;
    result
}

/// Create a minted row, or +1 the newest matching `(sender, dedup_key)` row.
///
/// No `dedup_key` is today's append. A matching key appends a +1 instead of
/// creating. `supersedes` retires matching rows only on the create branch.
pub fn upsert_notification(
    path: &Path,
    request: &NotificationUpsertRequestWire,
) -> Result<NotificationUpsertOutcomeWire, String> {
    let notification = &request.notification;
    let dedup_key = notification
        .dedup_key
        .as_deref()
        .filter(|key| !key.is_empty());
    let Some(dedup_key) = dedup_key else {
        return append_as_created(path, notification);
    };

    let plus_one_note = request
        .plus_one_note
        .as_deref()
        .ok_or_else(|| "dedup_key requires plus_one_note".to_string())?;
    let plus_one_timestamp = request
        .plus_one_timestamp
        .as_deref()
        .filter(|value| !value.is_empty())
        .unwrap_or(notification.timestamp.as_str());
    let prepared = PreparedPlusOne::from_parts(
        plus_one_timestamp,
        &notification.sender,
        plus_one_note,
    )?;
    let retirement = request
        .supersedes
        .as_deref()
        .filter(|key| !key.is_empty())
        .map(|old_key| {
            PreparedPlusOne::from_parts(
                plus_one_timestamp,
                &notification.sender,
                &supersede_note(notification),
            )
            .map(|prepared| (old_key.to_string(), prepared))
        })
        .transpose()?;

    let lock = open_lock_file(path)?;
    lock.lock_exclusive().map_err(|e| e.to_string())?;

    let result = (|| {
        let (mut rows, _) = read_rows_unlocked(path, true)?;
        if let Some(index) = find_newest_matching_index(&rows, |row| {
            row.sender == notification.sender
                && row.dedup_key.as_deref() == Some(dedup_key)
        }) {
            apply_prepared_plus_one(&mut rows[index], &prepared);
            let updated = rows[index].clone();
            merge_and_rewrite_notifications_unlocked(path, &rows)?;
            return Ok(plus_oned_upsert_outcome(updated));
        }

        let mut superseded_ids = Vec::new();
        if let Some((old_key, retirement)) = retirement.as_ref() {
            for row in &mut rows {
                if row.sender == notification.sender
                    && row.dedup_key.as_deref() == Some(old_key.as_str())
                {
                    apply_prepared_plus_one(row, retirement);
                    row.dismissed = true;
                    superseded_ids.push(row.id.clone());
                }
            }
        }

        rows.push(notification.clone());
        merge_and_rewrite_notifications_unlocked(path, &rows)?;
        Ok(created_upsert_outcome(notification.clone(), superseded_ids))
    })();

    unlock(lock)?;
    result
}

#[derive(Debug, Clone)]
struct PreparedPlusOne {
    timestamp: String,
    sender: String,
    note: String,
}

impl PreparedPlusOne {
    fn from_parts(
        timestamp: &str,
        sender: &str,
        note: &str,
    ) -> Result<Self, String> {
        let sender = sender.trim();
        if sender.is_empty() {
            return Err("plus-one sender cannot be empty or blank".to_string());
        }
        parse_aware_utc(timestamp, "plus-one timestamp")?;
        Ok(Self {
            timestamp: timestamp.to_string(),
            sender: sender.to_string(),
            note: normalize_plus_one_note(note)?,
        })
    }
}

fn normalize_plus_one_note(note: &str) -> Result<String, String> {
    let collapsed = note.split_whitespace().collect::<Vec<_>>().join(" ");
    if collapsed.is_empty() {
        return Err("plus-one note cannot be empty or blank".to_string());
    }
    Ok(collapsed
        .chars()
        .take(NOTIFICATION_PLUS_ONE_NOTE_MAX_CHARS)
        .collect())
}

fn validate_plus_one_target(
    request: &NotificationPlusOneRequestWire,
) -> Result<(), String> {
    let has_id = request
        .id
        .as_deref()
        .map(str::trim)
        .is_some_and(|id| !id.is_empty());
    let has_key = request
        .dedup_key
        .as_deref()
        .is_some_and(|key| !key.is_empty());
    if has_id || has_key {
        Ok(())
    } else {
        Err("plus-one request requires id or dedup_key".to_string())
    }
}

fn find_plus_one_target(
    rows: &[NotificationWire],
    request: &NotificationPlusOneRequestWire,
) -> Option<usize> {
    let id = request
        .id
        .as_deref()
        .map(str::trim)
        .filter(|id| !id.is_empty());
    if let Some(id) = id {
        return find_newest_matching_index(rows, |row| row.id == id);
    }
    let key = request.dedup_key.as_deref().filter(|key| !key.is_empty())?;
    let sender = request.sender.trim();
    if sender.is_empty() {
        return None;
    }
    find_newest_matching_index(rows, |row| {
        row.sender == sender && row.dedup_key.as_deref() == Some(key)
    })
}

fn find_newest_matching_index(
    rows: &[NotificationWire],
    predicate: impl Fn(&NotificationWire) -> bool,
) -> Option<usize> {
    let mut best: Option<(usize, DateTime<Utc>)> = None;
    for (index, row) in rows.iter().enumerate() {
        if !predicate(row) {
            continue;
        }
        let timestamp = DateTime::parse_from_rfc3339(&row.timestamp)
            .map(|value| value.with_timezone(&Utc))
            .unwrap_or_else(|_| DateTime::<Utc>::from(UNIX_EPOCH));
        match best {
            None => best = Some((index, timestamp)),
            Some((_, best_timestamp)) if timestamp >= best_timestamp => {
                best = Some((index, timestamp));
            }
            _ => {}
        }
    }
    best.map(|(index, _)| index)
}

fn apply_prepared_plus_one(
    row: &mut NotificationWire,
    plus_one: &PreparedPlusOne,
) {
    row.plus_ones.push(NotificationPlusOneWire {
        timestamp: plus_one.timestamp.clone(),
        sender: plus_one.sender.clone(),
        note: plus_one.note.clone(),
    });
    while row.plus_ones.len() > NOTIFICATION_PLUS_ONE_MAX_ENTRIES {
        row.plus_ones.remove(0);
        row.plus_ones_dropped = row.plus_ones_dropped.saturating_add(1);
    }
}

fn supersede_note(notification: &NotificationWire) -> String {
    let summary = notification
        .notes
        .first()
        .map(String::as_str)
        .map(str::trim)
        .filter(|note| !note.is_empty())
        .unwrap_or(notification.id.as_str());
    format!("superseded by: {summary}")
}

fn append_as_created(
    path: &Path,
    notification: &NotificationWire,
) -> Result<NotificationUpsertOutcomeWire, String> {
    let outcome = append_notification(path, notification)?;
    let stored = outcome
        .notifications
        .into_iter()
        .find(|row| row.id == notification.id)
        .unwrap_or_else(|| notification.clone());
    Ok(created_upsert_outcome(stored, Vec::new()))
}

fn applied_plus_one_outcome(
    notification: NotificationWire,
) -> NotificationPlusOneOutcomeWire {
    NotificationPlusOneOutcomeWire {
        schema_version: NOTIFICATION_STORE_WIRE_SCHEMA_VERSION,
        action: NotificationPlusOneActionWire::Applied,
        id: Some(notification.id.clone()),
        plus_one_count: notification.plus_one_count(),
        plus_ones_dropped: notification.plus_ones_dropped,
        notification: Some(notification),
    }
}

fn plus_oned_upsert_outcome(
    notification: NotificationWire,
) -> NotificationUpsertOutcomeWire {
    NotificationUpsertOutcomeWire {
        schema_version: NOTIFICATION_STORE_WIRE_SCHEMA_VERSION,
        action: NotificationUpsertActionWire::PlusOned,
        id: Some(notification.id.clone()),
        plus_one_count: notification.plus_one_count(),
        plus_ones_dropped: notification.plus_ones_dropped,
        superseded_ids: Vec::new(),
        notification: Some(notification),
    }
}

fn created_upsert_outcome(
    notification: NotificationWire,
    superseded_ids: Vec<String>,
) -> NotificationUpsertOutcomeWire {
    NotificationUpsertOutcomeWire {
        schema_version: NOTIFICATION_STORE_WIRE_SCHEMA_VERSION,
        action: NotificationUpsertActionWire::Created,
        id: Some(notification.id.clone()),
        plus_one_count: notification.plus_one_count(),
        plus_ones_dropped: notification.plus_ones_dropped,
        superseded_ids,
        notification: Some(notification),
    }
}

fn read_rows(
    path: &Path,
    include_dismissed: bool,
) -> Result<(Vec<NotificationWire>, NotificationStoreStatsWire), String> {
    read_rows_unlocked(path, include_dismissed)
}

fn read_rows_unlocked(
    path: &Path,
    include_dismissed: bool,
) -> Result<(Vec<NotificationWire>, NotificationStoreStatsWire), String> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok((Vec::new(), NotificationStoreStatsWire::default()));
        }
        Err(e) => return Err(e.to_string()),
    };

    let reader = BufReader::new(file);
    let mut rows = Vec::new();
    let mut stats = NotificationStoreStatsWire::default();
    for line in reader.lines() {
        let line = line.map_err(|e| e.to_string())?;
        stats.total_lines += 1;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            stats.blank_lines += 1;
            continue;
        }
        let value: serde_json::Value = match serde_json::from_str(trimmed) {
            Ok(value) => value,
            Err(_) => {
                stats.invalid_json_lines += 1;
                continue;
            }
        };
        let notification: NotificationWire =
            match serde_json::from_value::<NotificationWire>(value) {
                Ok(notification)
                    if !notification.id.is_empty()
                        && !notification.timestamp.is_empty()
                        && !notification.sender.is_empty() =>
                {
                    notification
                }
                _ => {
                    stats.invalid_record_lines += 1;
                    continue;
                }
            };
        stats.loaded_rows += 1;
        if !include_dismissed && notification.dismissed {
            stats.dismissed_filtered += 1;
            continue;
        }
        rows.push(notification);
    }
    Ok((rows, stats))
}

#[derive(Debug)]
struct NotificationCompaction {
    rows: Vec<NotificationWire>,
    archived_count: u64,
}

fn should_recheck_for_notification_compaction(
    path: &Path,
    notifications: &[NotificationWire],
    stats: &NotificationStoreStatsWire,
    include_dismissed: bool,
) -> bool {
    if notification_file_size(path)
        >= NOTIFICATION_COMPACTION_FILE_SIZE_THRESHOLD_BYTES
    {
        return true;
    }
    let dismissed_count = if include_dismissed {
        notification_compaction_candidate_count(notifications)
    } else {
        stats.dismissed_filtered as usize
    };
    dismissed_count >= NOTIFICATION_COMPACTION_DISMISSED_ROW_THRESHOLD
}

fn notification_compaction_candidate_count(
    notifications: &[NotificationWire],
) -> usize {
    notifications
        .iter()
        .filter(|notification| {
            notification.dismissed && notification.snooze_until.is_none()
        })
        .count()
}

fn notification_file_size(path: &Path) -> u64 {
    path.metadata().map(|metadata| metadata.len()).unwrap_or(0)
}

fn maybe_compact_notifications_unlocked(
    path: &Path,
    rows: Vec<NotificationWire>,
    now: DateTime<Utc>,
) -> Result<NotificationCompaction, String> {
    if notification_file_size(path)
        < NOTIFICATION_COMPACTION_FILE_SIZE_THRESHOLD_BYTES
        && notification_compaction_candidate_count(&rows)
            < NOTIFICATION_COMPACTION_DISMISSED_ROW_THRESHOLD
    {
        return Ok(NotificationCompaction {
            rows,
            archived_count: 0,
        });
    }

    let cutoff =
        now - ChronoDuration::days(NOTIFICATION_COMPACTION_RETENTION_DAYS);
    let mut kept = Vec::with_capacity(rows.len());
    let mut archived = Vec::new();
    for row in rows {
        if notification_should_archive(&row, cutoff) {
            archived.push(row);
        } else {
            kept.push(row);
        }
    }

    if archived.is_empty() {
        return Ok(NotificationCompaction {
            rows: kept,
            archived_count: 0,
        });
    }

    append_notifications_to_archive(path, &archived)?;
    Ok(NotificationCompaction {
        rows: kept,
        archived_count: archived.len() as u64,
    })
}

fn notification_should_archive(
    notification: &NotificationWire,
    cutoff: DateTime<Utc>,
) -> bool {
    if !notification.dismissed || notification.snooze_until.is_some() {
        return false;
    }
    DateTime::parse_from_rfc3339(notification_activity_at(notification))
        .map(|value| value.with_timezone(&Utc) < cutoff)
        .unwrap_or(false)
}

// Rewrite is a _merge_: caller's rows win on id collision; rows present on
// disk but absent from the input are preserved (they may be concurrent appends
// from another thread). Callers cannot use this to delete rows by passing a
// shorter list — if replacement semantics are ever needed, add a separate API.
fn merge_and_rewrite_notifications_unlocked(
    path: &Path,
    input: &[NotificationWire],
) -> Result<(), String> {
    let (existing, _) = read_rows_unlocked(path, true)?;
    let input_ids: BTreeSet<&str> =
        input.iter().map(|n| n.id.as_str()).collect();
    let mut merged: Vec<NotificationWire> = input.to_vec();
    for row in existing {
        if !input_ids.contains(row.id.as_str()) {
            merged.push(row);
        }
    }
    let compaction = maybe_compact_notifications_unlocked(
        path,
        merged,
        DateTime::<Utc>::from(SystemTime::now()),
    )?;
    write_notifications_atomic(path, &compaction.rows)
}

fn append_notifications_to_archive(
    path: &Path,
    notifications: &[NotificationWire],
) -> Result<(), String> {
    if notifications.is_empty() {
        return Ok(());
    }
    let parent = ensure_parent(path)?;
    fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    let archive_path = archive_path_for(path);
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&archive_path)
        .map_err(|e| e.to_string())?;
    for notification in notifications {
        serde_json::to_writer(&mut file, notification).map_err(|e| {
            format!("failed to serialize archived notification: {e}")
        })?;
        file.write_all(b"\n").map_err(|e| e.to_string())?;
    }
    file.flush().map_err(|e| e.to_string())?;
    file.sync_all().map_err(|e| e.to_string())?;
    if let Ok(dir) = File::open(parent) {
        let _ = dir.sync_all();
    }
    Ok(())
}

fn archive_path_for(path: &Path) -> PathBuf {
    let filename = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("notifications.jsonl");
    let archive_name =
        if let Some((stem, extension)) = filename.rsplit_once('.') {
            format!("{stem}-archive.{extension}")
        } else {
            format!("{filename}-archive")
        };
    path.with_file_name(archive_name)
}

fn write_notifications_atomic(
    path: &Path,
    notifications: &[NotificationWire],
) -> Result<(), String> {
    let parent = ensure_parent(path)?;
    fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    reap_stale_temp_siblings(path, SystemTime::now());
    let tmp_path = temp_path_for(path);
    let write_result = (|| {
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&tmp_path)
            .map_err(|e| e.to_string())?;
        let mut writer = BufWriter::new(file);
        for notification in notifications {
            serde_json::to_writer(&mut writer, notification).map_err(|e| {
                format!("failed to serialize notification: {e}")
            })?;
            writer.write_all(b"\n").map_err(|e| e.to_string())?;
        }
        writer.flush().map_err(|e| e.to_string())?;
        writer.get_ref().sync_all().map_err(|e| e.to_string())?;
        fs::rename(&tmp_path, path).map_err(|e| e.to_string())?;
        if let Ok(dir) = File::open(parent) {
            let _ = dir.sync_all();
        }
        Ok(())
    })();
    if write_result.is_err() {
        let _ = fs::remove_file(&tmp_path);
    }
    write_result
}

fn reap_stale_temp_siblings(path: &Path, now: SystemTime) {
    let Some(parent) = path.parent() else {
        return;
    };
    let filename = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("notifications.jsonl");
    let prefix = format!(".{filename}.");
    let Ok(entries) = fs::read_dir(parent) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if !name.starts_with(&prefix)
            || !name.ends_with(".tmp")
            || name.len() <= prefix.len() + ".tmp".len()
        {
            continue;
        }
        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        if !file_type.is_file() {
            continue;
        }
        let Ok(metadata) = entry.metadata() else {
            continue;
        };
        let Ok(modified) = metadata.modified() else {
            continue;
        };
        let Ok(age) = now.duration_since(modified) else {
            continue;
        };
        if age <= STALE_TEMP_MAX_AGE {
            continue;
        }
        let _ = fs::remove_file(entry.path());
    }
}

fn snapshot_from_rows(
    notifications: Vec<NotificationWire>,
    stats: NotificationStoreStatsWire,
) -> NotificationStoreSnapshotWire {
    let next_snooze_deadline = next_snooze_deadline_for(&notifications);
    let (tabs, counts) = tabs_and_counts_for(&notifications);
    NotificationStoreSnapshotWire {
        schema_version: NOTIFICATION_STORE_WIRE_SCHEMA_VERSION,
        counts,
        tabs,
        notifications,
        expired_ids: Vec::new(),
        next_snooze_deadline,
        stats,
    }
}

fn outcome_from_rows(
    notifications: Vec<NotificationWire>,
    stats: NotificationStoreStatsWire,
    matched_count: u64,
    changed_count: u64,
    appended_count: u64,
    rewritten: bool,
    expired_ids: Vec<String>,
) -> NotificationUpdateOutcomeWire {
    let next_snooze_deadline = next_snooze_deadline_for(&notifications);
    NotificationUpdateOutcomeWire {
        schema_version: NOTIFICATION_STORE_WIRE_SCHEMA_VERSION,
        counts: counts_for(&notifications),
        notifications,
        stats,
        matched_count,
        changed_count,
        appended_count,
        rewritten,
        expired_ids,
        next_snooze_deadline,
    }
}

fn outcome_without_rows(
    matched_count: u64,
    changed_count: u64,
    appended_count: u64,
    rewritten: bool,
    expired_ids: Vec<String>,
) -> NotificationUpdateOutcomeWire {
    NotificationUpdateOutcomeWire {
        schema_version: NOTIFICATION_STORE_WIRE_SCHEMA_VERSION,
        matched_count,
        changed_count,
        appended_count,
        rewritten,
        notifications: Vec::new(),
        counts: NotificationCountsWire::default(),
        expired_ids,
        next_snooze_deadline: None,
        stats: NotificationStoreStatsWire::default(),
    }
}

/// Count unread rows the same way the tab pass does, discarding the tabs.
fn counts_for(notifications: &[NotificationWire]) -> NotificationCountsWire {
    tabs_and_counts_for(notifications).1
}

fn matches_agent_notification(
    notification: &NotificationWire,
    agents: &[NotificationAgentKeyWire],
) -> bool {
    if agents.is_empty() {
        return false;
    }
    match notification.action.as_deref() {
        Some("JumpToAgent") => {
            let cl_name = notification.action_data.get("cl_name");
            let raw_suffix = notification.action_data.get("raw_suffix");
            match raw_suffix {
                None => {
                    agents.iter().any(|agent| Some(&agent.cl_name) == cl_name)
                }
                Some(raw_suffix) => agents.iter().any(|agent| {
                    Some(&agent.cl_name) == cl_name
                        && agent.raw_suffix.as_deref()
                            == Some(raw_suffix.as_str())
                }),
            }
        }
        Some("ViewErrorReport") if notification.sender == "user-agent" => {
            let cl_name = notification.action_data.get("cl_name");
            if cl_name.is_none() {
                return false;
            }
            let raw_suffix = notification.action_data.get("raw_suffix");
            match raw_suffix {
                None => {
                    agents.iter().any(|agent| Some(&agent.cl_name) == cl_name)
                }
                Some(raw_suffix) => agents.iter().any(|agent| {
                    Some(&agent.cl_name) == cl_name
                        && agent.raw_suffix.as_deref()
                            == Some(raw_suffix.as_str())
                }),
            }
        }
        Some(action)
            if super::mobile::MobileActionKindWire::from_notification_action(
                Some(action),
            )
            .is_agent_dismissable_gate() =>
        {
            let cl_name = notification.action_data.get("agent_cl_name");
            let agent_timestamp = notification
                .action_data
                .get("agent_timestamp")
                .and_then(|value| normalize_to_14_digit(value));
            let agent_root_timestamp = notification
                .action_data
                .get("agent_root_timestamp")
                .and_then(|value| normalize_to_14_digit(value));
            if agent_timestamp.is_none() && agent_root_timestamp.is_none() {
                agents.iter().any(|agent| Some(&agent.cl_name) == cl_name)
            } else {
                agents.iter().any(|agent| {
                    Some(&agent.cl_name) == cl_name
                        && agent.raw_suffix.as_deref().is_some_and(
                            |raw_suffix| {
                                agent_timestamp.as_deref() == Some(raw_suffix)
                                    || agent_root_timestamp.as_deref()
                                        == Some(raw_suffix)
                            },
                        )
                })
            }
        }
        _ => false,
    }
}

fn matches_agent_completion_notification(
    notification: &NotificationWire,
) -> bool {
    if notification.sender != "user-agent" {
        return false;
    }
    match notification.action.as_deref() {
        Some("JumpToAgent") | Some("ViewErrorReport") => notification
            .action_data
            .get("cl_name")
            .map(|value| !value.is_empty())
            .unwrap_or(false),
        _ => false,
    }
}

fn matches_agent_completion_notification_for_agents(
    notification: &NotificationWire,
    agents: &[NotificationAgentKeyWire],
) -> bool {
    if agents.is_empty() || !matches_agent_completion_notification(notification)
    {
        return false;
    }
    let cl_name = notification.action_data.get("cl_name");
    let raw_suffix = notification.action_data.get("raw_suffix");
    match raw_suffix {
        None => agents.iter().any(|agent| Some(&agent.cl_name) == cl_name),
        Some(raw_suffix) => agents.iter().any(|agent| {
            Some(&agent.cl_name) == cl_name
                && agent.raw_suffix.as_deref() == Some(raw_suffix.as_str())
        }),
    }
}

fn normalize_to_14_digit(ts: &str) -> Option<String> {
    if ts.len() == 14 && ts.bytes().all(|b| b.is_ascii_digit()) {
        return Some(ts.to_string());
    }
    if ts.len() == 13
        && ts.as_bytes().get(6) == Some(&b'_')
        && ts[..6].bytes().all(|b| b.is_ascii_digit())
        && ts[7..].bytes().all(|b| b.is_ascii_digit())
    {
        return Some(format!("20{}{}", &ts[..6], &ts[7..]));
    }
    None
}

fn parse_aware_utc(value: &str, field: &str) -> Result<DateTime<Utc>, String> {
    DateTime::parse_from_rfc3339(value)
        .map(|value| value.with_timezone(&Utc))
        .map_err(|_| {
            format!("{field} must be a timezone-aware RFC-3339 timestamp")
        })
}

fn validated_snooze_deadline(value: &str) -> Result<String, String> {
    let deadline = parse_aware_utc(value, "snooze deadline")?;
    if deadline <= DateTime::<Utc>::from(SystemTime::now()) {
        return Err("snooze deadline must be in the future".to_string());
    }
    Ok(format_utc_python_iso(deadline))
}

fn expire_snoozes_in_rows(
    rows: &mut [NotificationWire],
    now: DateTime<Utc>,
) -> Vec<String> {
    let resurfaced_at = format_utc_python_iso(now);
    let mut expired_ids = Vec::new();
    for notification in rows {
        if notification.dismissed {
            continue;
        }
        let Some(deadline) = notification.snooze_until.as_deref() else {
            continue;
        };
        let is_due_or_invalid = DateTime::parse_from_rfc3339(deadline)
            .map(|deadline| deadline.with_timezone(&Utc) <= now)
            .unwrap_or(true);
        if !is_due_or_invalid {
            continue;
        }
        notification.muted = false;
        notification.snooze_until = None;
        notification.read = false;
        notification.resurfaced_at = Some(resurfaced_at.clone());
        expired_ids.push(notification.id.clone());
    }
    expired_ids
}

fn next_snooze_deadline_for(
    notifications: &[NotificationWire],
) -> Option<String> {
    notifications
        .iter()
        .filter(|notification| !notification.dismissed)
        .filter_map(|notification| notification.snooze_until.as_deref())
        .filter_map(|deadline| DateTime::parse_from_rfc3339(deadline).ok())
        .map(|deadline| deadline.with_timezone(&Utc))
        .min()
        .map(format_utc_python_iso)
}

fn format_utc_python_iso(value: DateTime<Utc>) -> String {
    let nanos = value.timestamp_subsec_nanos();
    if nanos == 0 {
        return value.to_rfc3339_opts(SecondsFormat::Secs, false);
    }
    if nanos % 1_000 == 0 {
        return value.to_rfc3339_opts(SecondsFormat::Micros, false);
    }
    value.to_rfc3339_opts(SecondsFormat::Nanos, false)
}

fn open_lock_file(path: &Path) -> Result<File, String> {
    let parent = ensure_parent(path)?;
    fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(lock_path_for(path))
        .map_err(|e| e.to_string())
}

fn lock_path_for(path: &Path) -> PathBuf {
    let filename = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("notifications.jsonl");
    path.with_file_name(format!("{filename}.lock"))
}

fn temp_path_for(path: &Path) -> PathBuf {
    let filename = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("notifications.jsonl");
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    path.with_file_name(format!(".{filename}.{}.{}.tmp", process::id(), nanos))
}

fn ensure_parent(path: &Path) -> Result<&Path, String> {
    path.parent().ok_or_else(|| {
        format!("notification path has no parent: {}", path.display())
    })
}

#[allow(clippy::incompatible_msrv)]
fn unlock(lock: File) -> Result<(), String> {
    lock.unlock().map_err(|e| e.to_string())
}
