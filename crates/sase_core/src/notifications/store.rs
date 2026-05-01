use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process;
use std::time::{SystemTime, UNIX_EPOCH};

use chrono::{DateTime, NaiveDateTime, Utc};
use fs2::FileExt;

use super::wire::{
    NotificationAgentKeyWire, NotificationCountsWire,
    NotificationStateUpdateWire, NotificationStoreSnapshotWire,
    NotificationStoreStatsWire, NotificationUpdateOutcomeWire,
    NotificationWire, NOTIFICATION_STORE_WIRE_SCHEMA_VERSION,
};

pub fn read_notifications_snapshot(
    path: &Path,
    include_dismissed: bool,
) -> Result<NotificationStoreSnapshotWire, String> {
    read_notifications_snapshot_with_options(path, include_dismissed, false)
}

pub fn read_notifications_snapshot_with_options(
    path: &Path,
    include_dismissed: bool,
    expire_due_snoozes: bool,
) -> Result<NotificationStoreSnapshotWire, String> {
    if expire_due_snoozes {
        return read_notifications_snapshot_expiring_snoozes(
            path,
            include_dismissed,
        );
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
    lock.lock_shared().map_err(|e| e.to_string())?;
    let result = read_rows(path, include_dismissed);
    unlock(lock)?;
    let (notifications, stats) = result?;
    Ok(snapshot_from_rows(notifications, stats))
}

fn read_notifications_snapshot_expiring_snoozes(
    path: &Path,
    include_dismissed: bool,
) -> Result<NotificationStoreSnapshotWire, String> {
    let lock = open_lock_file(path)?;
    lock.lock_exclusive().map_err(|e| e.to_string())?;

    let result = (|| {
        let (mut rows, _) = read_rows_unlocked(path, true)?;
        let now = DateTime::<Utc>::from(SystemTime::now()).to_rfc3339();
        let mut expired_ids = Vec::new();
        for n in &mut rows {
            let Some(deadline) = &n.snooze_until else {
                continue;
            };
            if iso_timestamp_due(deadline, &now) {
                n.muted = false;
                n.snooze_until = None;
                expired_ids.push(n.id.clone());
            }
        }
        if !expired_ids.is_empty() {
            rewrite_notifications_unlocked(path, &rows)?;
        }
        let (notifications, stats) =
            read_rows_unlocked(path, include_dismissed)?;
        let mut snapshot = snapshot_from_rows(notifications, stats);
        snapshot.expired_ids = expired_ids;
        Ok(snapshot)
    })();

    unlock(lock)?;
    result
}

pub fn append_notification(
    path: &Path,
    notification: &NotificationWire,
) -> Result<NotificationUpdateOutcomeWire, String> {
    let parent = ensure_parent(path)?;
    let lock = open_lock_file(path)?;
    lock.lock_exclusive().map_err(|e| e.to_string())?;

    let append_result = (|| {
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
    })();

    let result = match append_result {
        Ok(()) => {
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
        Err(e) => Err(e),
    };
    unlock(lock)?;
    result
}

pub fn rewrite_notifications(
    path: &Path,
    notifications: &[NotificationWire],
) -> Result<NotificationUpdateOutcomeWire, String> {
    let lock = open_lock_file(path)?;
    lock.lock_exclusive().map_err(|e| e.to_string())?;
    let result =
        rewrite_notifications_unlocked(path, notifications).and_then(|()| {
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
    let lock = open_lock_file(path)?;
    lock.lock_exclusive().map_err(|e| e.to_string())?;

    let result = (|| {
        if let NotificationStateUpdateWire::RewriteAll { notifications } =
            update
        {
            rewrite_notifications_unlocked(path, notifications)?;
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
            NotificationStateUpdateWire::MarkDismissed { id } => {
                for n in &mut rows {
                    if n.id == *id {
                        matched_count += 1;
                        if !n.dismissed {
                            n.dismissed = true;
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
                        if !n.dismissed {
                            n.dismissed = true;
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
            NotificationStateUpdateWire::MarkSnoozed { id, until } => {
                for n in &mut rows {
                    if n.id == *id {
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
            NotificationStateUpdateWire::ExpireSnoozes { now } => {
                for n in &mut rows {
                    let Some(deadline) = &n.snooze_until else {
                        continue;
                    };
                    if iso_timestamp_due(deadline, now) {
                        matched_count += 1;
                        if n.muted || n.snooze_until.is_some() {
                            n.muted = false;
                            n.snooze_until = None;
                            expired_ids.push(n.id.clone());
                            changed_count += 1;
                        }
                    }
                }
            }
            NotificationStateUpdateWire::DismissMatchingAgents { agents } => {
                for n in &mut rows {
                    if n.dismissed {
                        continue;
                    }
                    if matches_agent_notification(n, agents) {
                        matched_count += 1;
                        n.dismissed = true;
                        changed_count += 1;
                    }
                }
            }
            NotificationStateUpdateWire::RewriteAll { .. } => unreachable!(),
        }

        if changed_count > 0 {
            rewrite_notifications_unlocked(path, &rows)?;
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

fn rewrite_notifications_unlocked(
    path: &Path,
    notifications: &[NotificationWire],
) -> Result<(), String> {
    let parent = ensure_parent(path)?;
    fs::create_dir_all(parent).map_err(|e| e.to_string())?;
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

fn snapshot_from_rows(
    notifications: Vec<NotificationWire>,
    stats: NotificationStoreStatsWire,
) -> NotificationStoreSnapshotWire {
    NotificationStoreSnapshotWire {
        schema_version: NOTIFICATION_STORE_WIRE_SCHEMA_VERSION,
        counts: counts_for(&notifications),
        notifications,
        expired_ids: Vec::new(),
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
    }
}

fn counts_for(notifications: &[NotificationWire]) -> NotificationCountsWire {
    let mut counts = NotificationCountsWire::default();
    for n in notifications {
        if n.read || n.silent {
            continue;
        }
        if n.muted {
            counts.muted += 1;
        } else if is_priority(n) {
            counts.priority += 1;
        } else {
            counts.rest += 1;
        }
    }
    counts
}

fn is_priority(notification: &NotificationWire) -> bool {
    matches!(
        notification.action.as_deref(),
        Some("PlanApproval" | "UserQuestion" | "JumpToMentorReview")
    ) || matches!(notification.sender.as_str(), "axe" | "crs")
        || (notification.sender == "user-agent"
            && notification.action.as_deref() == Some("ViewErrorReport"))
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
        Some("PlanApproval" | "UserQuestion") => {
            let cl_name = notification.action_data.get("agent_cl_name");
            let timestamp = notification
                .action_data
                .get("agent_timestamp")
                .and_then(|value| normalize_to_14_digit(value));
            match timestamp {
                None => {
                    agents.iter().any(|agent| Some(&agent.cl_name) == cl_name)
                }
                Some(timestamp) => agents.iter().any(|agent| {
                    Some(&agent.cl_name) == cl_name
                        && agent.raw_suffix.as_deref()
                            == Some(timestamp.as_str())
                }),
            }
        }
        _ => false,
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

fn iso_timestamp_due(deadline: &str, now: &str) -> bool {
    match (parse_iso_moment(deadline), parse_iso_moment(now)) {
        (Some(IsoMoment::Aware(deadline)), Some(IsoMoment::Aware(now))) => {
            deadline <= now
        }
        (Some(IsoMoment::Naive(deadline)), Some(IsoMoment::Naive(now))) => {
            deadline <= now
        }
        (Some(IsoMoment::Aware(deadline)), Some(IsoMoment::Naive(now))) => {
            deadline <= now.and_utc().timestamp_micros()
        }
        (Some(IsoMoment::Naive(deadline)), Some(IsoMoment::Aware(now))) => {
            deadline.and_utc().timestamp_micros() <= now
        }
        _ => false,
    }
}

enum IsoMoment {
    Aware(i64),
    Naive(NaiveDateTime),
}

fn parse_iso_moment(value: &str) -> Option<IsoMoment> {
    if let Ok(dt) = DateTime::parse_from_rfc3339(value) {
        return Some(IsoMoment::Aware(dt.timestamp_micros()));
    }
    NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f")
        .ok()
        .map(IsoMoment::Naive)
}

fn open_lock_file(path: &Path) -> Result<File, String> {
    let parent = ensure_parent(path)?;
    fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    OpenOptions::new()
        .create(true)
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

fn unlock(lock: File) -> Result<(), String> {
    lock.unlock().map_err(|e| e.to_string())
}
