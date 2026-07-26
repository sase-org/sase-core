use std::cmp::Ordering;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::process;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use chrono::{DateTime, FixedOffset};
use fs2::FileExt;

use super::wire::{
    BackgroundTaskWire, TaskAppendOutcomeWire, TaskPruneOutcomeWire,
    TaskStoreSnapshotWire, TaskStoreStatsWire, TaskUpdateOutcomeWire,
    TaskUpdateWire, TASK_WIRE_SCHEMA_VERSION,
};

/// Every task kind the store accepts on write.
const TASK_KINDS: [&str; 3] = ["command", "tui", "detached"];

const LOCK_TIMEOUT: Duration = Duration::from_secs(2);
const LOCK_RETRY_MIN_MS: u64 = 10;
const LOCK_RETRY_JITTER_MS: u64 = 20;

#[derive(Debug, thiserror::Error)]
pub enum TaskStoreError {
    #[error(
        "task store lock timed out after {waited_ms}ms waiting for {mode} lock: {}",
        path.display()
    )]
    LockTimeout {
        mode: &'static str,
        path: PathBuf,
        waited_ms: u128,
    },
    #[error(
        "task {task_id:?} cannot transition from terminal status {from:?} to {to:?}"
    )]
    InvalidTransition {
        task_id: String,
        from: String,
        to: String,
    },
    #[error("invalid task {task_id:?}: {reason}")]
    InvalidTask { task_id: String, reason: String },
    #[error("{0}")]
    Store(String),
}

impl From<String> for TaskStoreError {
    fn from(message: String) -> Self {
        Self::Store(message)
    }
}

type TaskStoreResult<T> = Result<T, TaskStoreError>;

#[derive(Clone, Copy)]
enum LockMode {
    Shared,
    Exclusive,
}

impl LockMode {
    fn label(self) -> &'static str {
        match self {
            Self::Shared => "shared",
            Self::Exclusive => "exclusive",
        }
    }
}

/// Read a stable, newest-first snapshot of the task store.
#[allow(clippy::incompatible_msrv)]
pub fn read_tasks_snapshot(
    path: &Path,
) -> TaskStoreResult<TaskStoreSnapshotWire> {
    if !path.exists() {
        return Ok(snapshot_from_rows(
            Vec::new(),
            TaskStoreStatsWire::default(),
        ));
    }
    let lock = open_lock_file(path)?;
    lock_with_timeout(&lock, path, LockMode::Shared, LOCK_TIMEOUT)?;
    let result = read_rows_unlocked(path);
    unlock(lock)?;
    let (tasks, stats) = result?;
    Ok(snapshot_from_rows(tasks, stats))
}

/// Append a task and enforce terminal-row retention atomically.
pub fn append_task(
    path: &Path,
    task: &BackgroundTaskWire,
    history_limit: i64,
) -> TaskStoreResult<TaskAppendOutcomeWire> {
    let mut task = task.clone();
    normalize_and_validate_task(&mut task)?;
    let lock = open_lock_file(path)?;
    lock_with_timeout(&lock, path, LockMode::Exclusive, LOCK_TIMEOUT)?;
    let result: TaskStoreResult<TaskAppendOutcomeWire> = (|| {
        let (mut rows, _) = read_rows_unlocked(path)?;
        rows.push(task);
        let (kept, pruned_task_ids) =
            apply_retention(rows, clamped_history_limit(history_limit));
        write_tasks_atomic(path, &kept)?;
        let (rows, stats) = read_rows_unlocked(path)?;
        Ok(TaskAppendOutcomeWire {
            schema_version: TASK_WIRE_SCHEMA_VERSION,
            snapshot: snapshot_from_rows(rows, stats),
            pruned_task_ids,
        })
    })();
    unlock(lock)?;
    result
}

/// Apply a partial task update. Missing ids are successful no-ops.
pub fn update_task(
    path: &Path,
    update: &TaskUpdateWire,
) -> TaskStoreResult<TaskUpdateOutcomeWire> {
    if update.task_id.is_empty() {
        return Err(TaskStoreError::InvalidTask {
            task_id: String::new(),
            reason: "task_id must not be empty".to_string(),
        });
    }
    let lock = open_lock_file(path)?;
    lock_with_timeout(&lock, path, LockMode::Exclusive, LOCK_TIMEOUT)?;
    let result: TaskStoreResult<TaskUpdateOutcomeWire> = (|| {
        let (mut rows, _) = read_rows_unlocked(path)?;
        let matched_index =
            rows.iter().position(|row| row.task_id == update.task_id);
        let Some(index) = matched_index else {
            // An update is a rewrite operation even when retention has already
            // removed the id, so malformed rows observed above are cleaned up.
            write_tasks_atomic(path, &rows)?;
            return Ok(TaskUpdateOutcomeWire {
                schema_version: TASK_WIRE_SCHEMA_VERSION,
                task: None,
                matched: false,
            });
        };

        apply_update(&mut rows[index], update)?;
        let task = rows[index].clone();
        write_tasks_atomic(path, &rows)?;
        Ok(TaskUpdateOutcomeWire {
            schema_version: TASK_WIRE_SCHEMA_VERSION,
            task: Some(task),
            matched: true,
        })
    })();
    unlock(lock)?;
    result
}

/// Enforce terminal-row retention without appending a task.
pub fn prune_tasks(
    path: &Path,
    history_limit: i64,
) -> TaskStoreResult<TaskPruneOutcomeWire> {
    let lock = open_lock_file(path)?;
    lock_with_timeout(&lock, path, LockMode::Exclusive, LOCK_TIMEOUT)?;
    let result: TaskStoreResult<TaskPruneOutcomeWire> = (|| {
        let (rows, _) = read_rows_unlocked(path)?;
        let (kept, pruned_task_ids) =
            apply_retention(rows, clamped_history_limit(history_limit));
        write_tasks_atomic(path, &kept)?;
        let (rows, stats) = read_rows_unlocked(path)?;
        Ok(TaskPruneOutcomeWire {
            schema_version: TASK_WIRE_SCHEMA_VERSION,
            snapshot: snapshot_from_rows(rows, stats),
            pruned_task_ids,
        })
    })();
    unlock(lock)?;
    result
}

fn read_rows_unlocked(
    path: &Path,
) -> Result<(Vec<BackgroundTaskWire>, TaskStoreStatsWire), String> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == ErrorKind::NotFound => {
            return Ok((Vec::new(), TaskStoreStatsWire::default()));
        }
        Err(error) => return Err(error.to_string()),
    };

    let reader = BufReader::new(file);
    let mut rows = Vec::new();
    let mut stats = TaskStoreStatsWire::default();
    for line in reader.lines() {
        let line = line.map_err(|error| error.to_string())?;
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
        let mut task = match serde_json::from_value::<BackgroundTaskWire>(value)
        {
            Ok(task) => task,
            Err(_) => {
                stats.invalid_record_lines += 1;
                continue;
            }
        };
        if normalize_and_validate_task(&mut task).is_err() {
            stats.invalid_record_lines += 1;
            continue;
        }
        stats.loaded_rows += 1;
        rows.push(task);
    }
    Ok((rows, stats))
}

fn snapshot_from_rows(
    rows: Vec<BackgroundTaskWire>,
    stats: TaskStoreStatsWire,
) -> TaskStoreSnapshotWire {
    TaskStoreSnapshotWire {
        schema_version: TASK_WIRE_SCHEMA_VERSION,
        tasks: newest_first(rows),
        stats,
    }
}

fn newest_first(rows: Vec<BackgroundTaskWire>) -> Vec<BackgroundTaskWire> {
    let mut indexed: Vec<(usize, BackgroundTaskWire)> =
        rows.into_iter().enumerate().collect();
    indexed.sort_by(|(left_index, left), (right_index, right)| {
        compare_task_recency(*left_index, left, *right_index, right).reverse()
    });
    indexed.into_iter().map(|(_, task)| task).collect()
}

fn compare_task_recency(
    left_index: usize,
    left: &BackgroundTaskWire,
    right_index: usize,
    right: &BackgroundTaskWire,
) -> Ordering {
    let left_created = parse_utc_timestamp(&left.created_at)
        .expect("validated task timestamp must parse");
    let right_created = parse_utc_timestamp(&right.created_at)
        .expect("validated task timestamp must parse");
    left_created
        .cmp(&right_created)
        .then_with(|| left_index.cmp(&right_index))
}

fn apply_retention(
    rows: Vec<BackgroundTaskWire>,
    history_limit: usize,
) -> (Vec<BackgroundTaskWire>, Vec<String>) {
    let mut terminals: Vec<(usize, &BackgroundTaskWire)> = rows
        .iter()
        .enumerate()
        .filter(|(_, task)| is_terminal_status(&task.status))
        .collect();
    terminals.sort_by(|(left_index, left), (right_index, right)| {
        compare_task_recency(*left_index, left, *right_index, right).reverse()
    });

    let mut keep = vec![true; rows.len()];
    for (index, _) in terminals.into_iter().skip(history_limit) {
        keep[index] = false;
    }

    let mut kept = Vec::with_capacity(rows.len());
    let mut pruned_task_ids = Vec::new();
    for (index, task) in rows.into_iter().enumerate() {
        if keep[index] {
            kept.push(task);
        } else {
            pruned_task_ids.push(task.task_id);
        }
    }
    (kept, pruned_task_ids)
}

fn clamped_history_limit(history_limit: i64) -> usize {
    usize::try_from(history_limit.max(1)).unwrap_or(usize::MAX)
}

fn apply_update(
    task: &mut BackgroundTaskWire,
    update: &TaskUpdateWire,
) -> TaskStoreResult<()> {
    let was_terminal = is_terminal_status(&task.status);
    let finished_at_was_set = was_terminal && task.finished_at.is_some();

    if let Some(status) = update.status.as_deref() {
        validate_status(status).map_err(|reason| invalid_task(task, reason))?;
        if was_terminal && !is_terminal_status(status) {
            return Err(TaskStoreError::InvalidTransition {
                task_id: task.task_id.clone(),
                from: task.status.clone(),
                to: status.to_string(),
            });
        }
        // The first terminal status is final. Later terminal reports may
        // refine outcome fields, but cannot change the recorded disposition.
        if !was_terminal {
            task.status = status.to_string();
        }
    }
    if let Some(value) = &update.label {
        task.label.clone_from(value);
    }
    if let Some(value) = &update.kind {
        task.kind.clone_from(value);
    }
    if let Some(value) = &update.command {
        task.command.clone_from(value);
    }
    if let Some(value) = &update.cwd {
        task.cwd.clone_from(value);
    }
    if let Some(value) = &update.project {
        task.project.clone_from(value);
    }
    if let Some(value) = update.workspace_num {
        task.workspace_num = value;
    }
    if let Some(value) = &update.session_id {
        task.session_id.clone_from(value);
    }
    if let Some(value) = &update.session_label {
        task.session_label.clone_from(value);
    }
    if let Some(value) = &update.origin {
        task.origin.clone_from(value);
    }
    if let Some(value) = &update.cl_name {
        task.cl_name.clone_from(value);
    }
    if let Some(value) = &update.tags {
        task.tags.clone_from(value);
    }
    if let Some(value) = update.pid {
        task.pid = value;
    }
    if let Some(value) = update.pgid {
        task.pgid = value;
    }
    if let Some(value) = update.exit_code {
        task.exit_code = value;
    }
    if let Some(value) = &update.phase {
        task.phase.clone_from(value);
    }
    if let Some(value) = &update.message {
        task.message.clone_from(value);
    }
    if let Some(value) = &update.created_at {
        task.created_at.clone_from(value);
    }
    if let Some(value) = &update.started_at {
        task.started_at.clone_from(value);
    }
    if !finished_at_was_set {
        if let Some(value) = &update.finished_at {
            task.finished_at.clone_from(value);
        }
    }
    if let Some(value) = &update.log_path {
        task.log_path.clone_from(value);
    }

    normalize_and_validate_task(task)
}

fn normalize_and_validate_task(
    task: &mut BackgroundTaskWire,
) -> TaskStoreResult<()> {
    task.tags.sort();
    task.tags.dedup();

    for (field, value) in [
        ("task_id", task.task_id.as_str()),
        ("label", task.label.as_str()),
        ("cwd", task.cwd.as_str()),
        ("origin", task.origin.as_str()),
        ("created_at", task.created_at.as_str()),
        ("log_path", task.log_path.as_str()),
    ] {
        if value.is_empty() {
            return Err(invalid_task(
                task,
                format!("{field} must not be empty"),
            ));
        }
    }
    validate_kind(&task.kind).map_err(|reason| invalid_task(task, reason))?;
    validate_status(&task.status)
        .map_err(|reason| invalid_task(task, reason))?;
    validate_timestamp_field("created_at", Some(&task.created_at))
        .map_err(|reason| invalid_task(task, reason))?;
    validate_timestamp_field("started_at", task.started_at.as_ref())
        .map_err(|reason| invalid_task(task, reason))?;
    validate_timestamp_field("finished_at", task.finished_at.as_ref())
        .map_err(|reason| invalid_task(task, reason))?;
    Ok(())
}

fn validate_kind(kind: &str) -> Result<(), String> {
    if TASK_KINDS.contains(&kind) {
        Ok(())
    } else {
        Err(format!("unknown kind {kind:?}"))
    }
}

fn validate_status(status: &str) -> Result<(), String> {
    if matches!(
        status,
        "pending" | "running" | "success" | "error" | "killed"
    ) {
        Ok(())
    } else {
        Err(format!("unknown status {status:?}"))
    }
}

fn is_terminal_status(status: &str) -> bool {
    matches!(status, "success" | "error" | "killed")
}

fn validate_timestamp_field(
    field: &str,
    value: Option<&String>,
) -> Result<(), String> {
    if let Some(value) = value {
        parse_utc_timestamp(value)
            .map(|_| ())
            .map_err(|reason| format!("{field} {reason}"))
    } else {
        Ok(())
    }
}

fn parse_utc_timestamp(value: &str) -> Result<DateTime<FixedOffset>, String> {
    let timestamp = DateTime::parse_from_rfc3339(value)
        .map_err(|error| format!("must be an RFC3339 timestamp: {error}"))?;
    if timestamp.offset().local_minus_utc() != 0 {
        return Err("must use a UTC offset".to_string());
    }
    Ok(timestamp)
}

fn invalid_task(task: &BackgroundTaskWire, reason: String) -> TaskStoreError {
    TaskStoreError::InvalidTask {
        task_id: task.task_id.clone(),
        reason,
    }
}

fn write_tasks_atomic(
    path: &Path,
    tasks: &[BackgroundTaskWire],
) -> Result<(), String> {
    let parent = ensure_parent(path)?;
    fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    let tmp_path = temp_path_for(path);
    let write_result = (|| {
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&tmp_path)
            .map_err(|error| error.to_string())?;
        let mut writer = BufWriter::new(file);
        for task in tasks {
            serde_json::to_writer(&mut writer, task).map_err(|error| {
                format!("failed to serialize background task: {error}")
            })?;
            writer.write_all(b"\n").map_err(|error| error.to_string())?;
        }
        writer.flush().map_err(|error| error.to_string())?;
        writer
            .get_ref()
            .sync_all()
            .map_err(|error| error.to_string())?;
        fs::rename(&tmp_path, path).map_err(|error| error.to_string())?;
        if let Ok(directory) = File::open(parent) {
            let _ = directory.sync_all();
        }
        Ok(())
    })();
    if write_result.is_err() {
        let _ = fs::remove_file(&tmp_path);
    }
    write_result
}

fn open_lock_file(path: &Path) -> Result<File, String> {
    let parent = ensure_parent(path)?;
    fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(lock_path_for(path))
        .map_err(|error| error.to_string())
}

fn lock_with_timeout(
    lock: &File,
    path: &Path,
    mode: LockMode,
    timeout: Duration,
) -> TaskStoreResult<()> {
    let started = Instant::now();
    let mut attempt = 0_u64;
    loop {
        let result = match mode {
            LockMode::Shared => FileExt::try_lock_shared(lock),
            LockMode::Exclusive => FileExt::try_lock_exclusive(lock),
        };
        match result {
            Ok(()) => return Ok(()),
            Err(error) if error.kind() == ErrorKind::WouldBlock => {
                let elapsed = started.elapsed();
                if elapsed >= timeout {
                    return Err(TaskStoreError::LockTimeout {
                        mode: mode.label(),
                        path: lock_path_for(path),
                        waited_ms: elapsed.as_millis(),
                    });
                }
                let delay = retry_delay(attempt).min(timeout - elapsed);
                thread::sleep(delay);
                attempt = attempt.saturating_add(1);
            }
            Err(error) => return Err(TaskStoreError::Store(error.to_string())),
        }
    }
}

fn retry_delay(attempt: u64) -> Duration {
    let clock_jitter = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| u64::from(duration.subsec_nanos()))
        .unwrap_or(0);
    let jitter = clock_jitter.wrapping_add(attempt.wrapping_mul(17))
        % (LOCK_RETRY_JITTER_MS + 1);
    Duration::from_millis(LOCK_RETRY_MIN_MS + jitter)
}

fn lock_path_for(path: &Path) -> PathBuf {
    let filename = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("tasks.jsonl");
    path.with_file_name(format!("{filename}.lock"))
}

fn temp_path_for(path: &Path) -> PathBuf {
    let filename = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("tasks.jsonl");
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    path.with_file_name(format!(".{filename}.{}.{}.tmp", process::id(), nanos))
}

fn ensure_parent(path: &Path) -> Result<&Path, String> {
    path.parent().ok_or_else(|| {
        format!("task store path has no parent: {}", path.display())
    })
}

#[allow(clippy::incompatible_msrv)]
fn unlock(lock: File) -> Result<(), String> {
    FileExt::unlock(&lock).map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Barrier};

    use serde_json::json;
    use tempfile::tempdir;

    use super::*;

    fn task(
        task_id: &str,
        status: &str,
        created_at: &str,
    ) -> BackgroundTaskWire {
        BackgroundTaskWire {
            task_id: task_id.to_string(),
            label: format!("Task {task_id}"),
            kind: "command".to_string(),
            status: status.to_string(),
            command: vec!["true".to_string()],
            cwd: "/tmp".to_string(),
            project: Some("sase".to_string()),
            workspace_num: Some(1),
            session_id: None,
            session_label: None,
            origin: "test".to_string(),
            cl_name: None,
            tags: vec![
                "zeta".to_string(),
                "alpha".to_string(),
                "zeta".to_string(),
            ],
            pid: None,
            pgid: None,
            exit_code: None,
            phase: None,
            message: None,
            created_at: created_at.to_string(),
            started_at: None,
            finished_at: None,
            log_path: format!("/tmp/{task_id}.log"),
        }
    }

    #[test]
    fn append_and_read_round_trip_is_newest_first_and_normalizes_tags() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("tasks.jsonl");
        append_task(
            &path,
            &task("older", "pending", "2026-07-25T12:00:00Z"),
            10,
        )
        .unwrap();
        let outcome = append_task(
            &path,
            &task("newer", "running", "2026-07-25T12:00:01Z"),
            10,
        )
        .unwrap();

        assert_eq!(
            outcome
                .snapshot
                .tasks
                .iter()
                .map(|task| task.task_id.as_str())
                .collect::<Vec<_>>(),
            vec!["newer", "older"]
        );
        assert_eq!(outcome.snapshot.tasks[0].tags, vec!["alpha", "zeta"]);
        assert_eq!(read_tasks_snapshot(&path).unwrap(), outcome.snapshot);
    }

    #[test]
    fn detached_kind_round_trips_and_unknown_kinds_are_rejected() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("tasks.jsonl");
        let mut detached = task("detached", "running", "2026-07-25T12:00:00Z");
        detached.kind = "detached".to_string();
        let outcome = append_task(&path, &detached, 10).unwrap();
        assert_eq!(outcome.snapshot.tasks[0].kind, "detached");
        assert_eq!(
            read_tasks_snapshot(&path).unwrap().tasks[0].kind,
            "detached"
        );

        let mut unknown = task("unknown", "running", "2026-07-25T12:00:01Z");
        unknown.kind = "daemon".to_string();
        let error = append_task(&path, &unknown, 10).unwrap_err();
        assert!(matches!(
            &error,
            TaskStoreError::InvalidTask { task_id, reason }
                if task_id == "unknown"
                    && reason == "unknown kind \"daemon\""
        ));
    }

    #[test]
    fn updating_an_existing_row_to_the_detached_kind_validates() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("tasks.jsonl");
        append_task(
            &path,
            &task("promoted", "running", "2026-07-25T12:00:00Z"),
            10,
        )
        .unwrap();

        let updated = update_task(
            &path,
            &TaskUpdateWire {
                task_id: "promoted".to_string(),
                kind: Some("detached".to_string()),
                ..TaskUpdateWire::default()
            },
        )
        .unwrap();
        assert_eq!(updated.task.unwrap().kind, "detached");

        let error = update_task(
            &path,
            &TaskUpdateWire {
                task_id: "promoted".to_string(),
                kind: Some("daemon".to_string()),
                ..TaskUpdateWire::default()
            },
        )
        .unwrap_err();
        assert!(matches!(error, TaskStoreError::InvalidTask { .. }));
        assert_eq!(
            read_tasks_snapshot(&path).unwrap().tasks[0].kind,
            "detached"
        );
    }

    #[test]
    fn retention_keeps_newest_terminal_rows_at_and_beyond_limit() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("tasks.jsonl");
        for (id, created_at) in [
            ("one", "2026-07-25T12:00:01Z"),
            ("two", "2026-07-25T12:00:02Z"),
            ("three", "2026-07-25T12:00:03Z"),
        ] {
            append_task(&path, &task(id, "success", created_at), 2).unwrap();
        }

        let snapshot = read_tasks_snapshot(&path).unwrap();
        assert_eq!(
            snapshot
                .tasks
                .iter()
                .map(|task| task.task_id.as_str())
                .collect::<Vec<_>>(),
            vec!["three", "two"]
        );

        let outcome = prune_tasks(&path, 1).unwrap();
        assert_eq!(outcome.pruned_task_ids, vec!["two"]);
        assert_eq!(outcome.snapshot.tasks[0].task_id, "three");
        assert!(prune_tasks(&path, 0).unwrap().pruned_task_ids.is_empty());
    }

    #[test]
    fn running_rows_survive_retention() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("tasks.jsonl");
        append_task(
            &path,
            &task("running-old", "running", "2026-07-25T12:00:00Z"),
            1,
        )
        .unwrap();
        append_task(
            &path,
            &task("done-old", "success", "2026-07-25T12:00:01Z"),
            1,
        )
        .unwrap();
        let outcome = append_task(
            &path,
            &task("done-new", "error", "2026-07-25T12:00:02Z"),
            1,
        )
        .unwrap();

        assert_eq!(outcome.pruned_task_ids, vec!["done-old"]);
        assert!(outcome
            .snapshot
            .tasks
            .iter()
            .any(|task| task.task_id == "running-old"));
    }

    #[test]
    fn terminal_transition_guards_and_repeat_writes_preserve_final_fields() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("tasks.jsonl");
        let mut finished = task("finished", "success", "2026-07-25T12:00:00Z");
        finished.finished_at = Some("2026-07-25T12:00:10Z".to_string());
        finished.message = Some("first".to_string());
        append_task(&path, &finished, 10).unwrap();

        let backward = TaskUpdateWire {
            task_id: "finished".to_string(),
            status: Some("running".to_string()),
            ..TaskUpdateWire::default()
        };
        assert!(matches!(
            update_task(&path, &backward),
            Err(TaskStoreError::InvalidTransition { .. })
        ));

        let repeated = TaskUpdateWire {
            task_id: "finished".to_string(),
            status: Some("error".to_string()),
            exit_code: Some(Some(7)),
            message: Some(Some("second".to_string())),
            finished_at: Some(Some("2026-07-25T12:00:20Z".to_string())),
            ..TaskUpdateWire::default()
        };
        let updated = update_task(&path, &repeated).unwrap().task.unwrap();
        assert_eq!(updated.status, "success");
        assert_eq!(updated.exit_code, Some(7));
        assert_eq!(updated.message.as_deref(), Some("second"));
        assert_eq!(
            updated.finished_at.as_deref(),
            Some("2026-07-25T12:00:10Z")
        );

        let missing = update_task(
            &path,
            &TaskUpdateWire {
                task_id: "pruned".to_string(),
                ..TaskUpdateWire::default()
            },
        )
        .unwrap();
        assert!(!missing.matched);
        assert!(missing.task.is_none());
    }

    #[test]
    fn unknown_fields_are_tolerated_and_malformed_rows_are_dropped_on_rewrite()
    {
        let temp = tempdir().unwrap();
        let path = temp.path().join("tasks.jsonl");
        let mut value = serde_json::to_value(task(
            "valid",
            "pending",
            "2026-07-25T12:00:00Z",
        ))
        .unwrap();
        value["future_field"] = json!({"anything": true});
        fs::write(
            &path,
            format!("not-json\n{}\n{{\"task_id\":\"broken\"}}\n", value),
        )
        .unwrap();

        let snapshot = read_tasks_snapshot(&path).unwrap();
        assert_eq!(snapshot.tasks.len(), 1);
        assert_eq!(snapshot.stats.invalid_json_lines, 1);
        assert_eq!(snapshot.stats.invalid_record_lines, 1);

        prune_tasks(&path, 10).unwrap();
        let rewritten = fs::read_to_string(&path).unwrap();
        assert_eq!(rewritten.lines().count(), 1);
        assert!(!rewritten.contains("future_field"));
    }

    #[test]
    fn concurrent_writers_do_not_lose_rows() {
        let temp = tempdir().unwrap();
        let path = Arc::new(temp.path().join("tasks.jsonl"));
        let barrier = Arc::new(Barrier::new(8));
        let mut writers = Vec::new();
        for index in 0..8 {
            let path = Arc::clone(&path);
            let barrier = Arc::clone(&barrier);
            writers.push(thread::spawn(move || {
                barrier.wait();
                append_task(
                    &path,
                    &task(
                        &format!("task-{index}"),
                        "running",
                        &format!("2026-07-25T12:00:{index:02}Z"),
                    ),
                    1,
                )
                .unwrap();
            }));
        }
        for writer in writers {
            writer.join().unwrap();
        }

        assert_eq!(read_tasks_snapshot(&path).unwrap().tasks.len(), 8);
    }

    #[test]
    fn held_exclusive_lock_bounds_reader_and_writer_waits() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("tasks.jsonl");
        let holder = open_lock_file(&path).unwrap();
        holder.lock_exclusive().unwrap();

        for mode in [LockMode::Shared, LockMode::Exclusive] {
            let contender = open_lock_file(&path).unwrap();
            let started = Instant::now();
            let error = lock_with_timeout(
                &contender,
                &path,
                mode,
                Duration::from_millis(50),
            )
            .unwrap_err();
            assert!(matches!(error, TaskStoreError::LockTimeout { .. }));
            assert!(started.elapsed() < Duration::from_secs(1));
        }

        holder.unlock().unwrap();
    }
}
