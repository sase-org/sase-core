use std::{
    collections::BTreeMap,
    fmt,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, RwLock},
    time::Instant,
};

use sase_core::projections::{
    list_projection_backups, read_projection_backup_metadata,
    scheduler_health_summary, validate_projection_backup_file,
    AgentProjectionApplier, BeadProjectionApplier, CatalogProjectionApplier,
    ChangeSpecProjectionApplier, EventAppendOutcomeWire,
    EventAppendRequestWire, LocalDaemonMutationOutcomeWire,
    NotificationProjectionApplier, ProjectionApplier, ProjectionBackupListWire,
    ProjectionBackupMetadataWire, ProjectionBackupReportWire,
    ProjectionCheckpointReportWire, ProjectionDb, ProjectionError,
    ProjectionRebuildOptionsWire, ProjectionRebuildReportWire,
    ProjectionRecoveryIssueWire, ProjectionRestoreReportWire,
    ProjectionStartupRepairReportWire, ProjectionWalCheckpointModeWire,
    SchedulerProjectionApplier, SourceExportPlanWire, SourceExportStatusWire,
    WorkflowProjectionApplier,
};
use serde_json::{json, Value as JsonValue};
use thiserror::Error;

use crate::metrics::DaemonMetrics;

#[derive(Clone)]
pub struct ProjectionService {
    inner: Arc<ProjectionServiceInner>,
}

impl fmt::Debug for ProjectionService {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProjectionService")
            .field("status", &self.status())
            .finish_non_exhaustive()
    }
}

struct ProjectionServiceInner {
    db: Arc<RwLock<Option<Arc<Mutex<ProjectionDb>>>>>,
    status: Arc<RwLock<ProjectionServiceStatus>>,
    metrics: DaemonMetrics,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProjectionServiceStatus {
    pub state: ProjectionServiceState,
    pub path: PathBuf,
    pub schema_initialized: bool,
    pub migrations_applied: bool,
    pub repair_needed: bool,
    pub max_event_seq: Option<i64>,
    pub gap_count: usize,
    pub recovery_issue_count: usize,
    pub message: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProjectionServiceState {
    Ok,
    Degraded,
}

#[derive(Debug, Error)]
pub enum ProjectionServiceError {
    #[error("projection database is unavailable: {0}")]
    Unavailable(String),
    #[error("projection database lock was poisoned")]
    LockPoisoned,
    #[error("projection blocking task failed: {0}")]
    Join(String),
    #[error(transparent)]
    Projection(#[from] ProjectionError),
}

pub fn default_projection_db_path(run_root: &Path) -> PathBuf {
    run_root.join("projections").join("projection.sqlite")
}

impl ProjectionService {
    pub async fn initialize(path: PathBuf) -> Self {
        Self::initialize_with_metrics(path, DaemonMetrics::default()).await
    }

    pub async fn initialize_with_metrics(
        path: PathBuf,
        metrics: DaemonMetrics,
    ) -> Self {
        let opened_path = path.clone();
        let started = Instant::now();
        let result = tokio::task::spawn_blocking(move || {
            let mut db = ProjectionDb::open(&opened_path)?;
            let report = run_startup_repair_checks(&mut db)?;
            Ok::<_, ProjectionError>((db, report))
        })
        .await;

        match result {
            Ok(Ok((db, report))) => {
                metrics.record_projection_query(started.elapsed());
                let status = status_from_report(path, &report);
                Self::from_db(db, status, metrics)
            }
            Ok(Err(error)) => {
                metrics.record_projection_query(started.elapsed());
                Self::unavailable_with_metrics(path, error.to_string(), metrics)
            }
            Err(error) => {
                metrics.record_projection_query(started.elapsed());
                Self::unavailable_with_metrics(
                    path,
                    format!("projection initialization task failed: {error}"),
                    metrics,
                )
            }
        }
    }

    pub fn unavailable(path: PathBuf, message: impl Into<String>) -> Self {
        Self::unavailable_with_metrics(path, message, DaemonMetrics::default())
    }

    pub fn unavailable_with_metrics(
        path: PathBuf,
        message: impl Into<String>,
        metrics: DaemonMetrics,
    ) -> Self {
        Self {
            inner: Arc::new(ProjectionServiceInner {
                db: Arc::new(RwLock::new(None)),
                status: Arc::new(RwLock::new(ProjectionServiceStatus {
                    state: ProjectionServiceState::Degraded,
                    path,
                    schema_initialized: false,
                    migrations_applied: false,
                    repair_needed: true,
                    max_event_seq: None,
                    gap_count: 0,
                    recovery_issue_count: 1,
                    message: Some(message.into()),
                })),
                metrics,
            }),
        }
    }

    pub fn status(&self) -> ProjectionServiceStatus {
        self.inner
            .status
            .read()
            .map(|status| status.clone())
            .unwrap_or_else(|_| ProjectionServiceStatus {
                state: ProjectionServiceState::Degraded,
                path: PathBuf::new(),
                schema_initialized: false,
                migrations_applied: false,
                repair_needed: true,
                max_event_seq: None,
                gap_count: 0,
                recovery_issue_count: 1,
                message: Some(
                    "projection status lock was poisoned".to_string(),
                ),
            })
    }

    pub fn health_details(&self) -> JsonValue {
        let status = self.status();
        let source_exports =
            self.source_export_health_summary().unwrap_or_else(|error| {
                json!({
                    "state": "unknown",
                    "message": error.to_string(),
                    "total": 0,
                    "pending": 0,
                    "failed": 0,
                    "conflict": 0,
                    "by_surface": [],
                    "examples": [],
                })
            });
        let scheduler =
            self.scheduler_health_summary().unwrap_or_else(|error| {
                json!({
                    "schema_version": 1,
                    "state": "unknown",
                    "queue_depth": 0,
                    "active_tasks": 0,
                    "running_tasks": 0,
                    "starting_tasks": 0,
                    "blocked_tasks": 0,
                    "stale_starts": 0,
                    "host_bridge": {
                        "available": false,
                        "mode": "unknown",
                    },
                    "projection_lag": {
                        "last_scheduler_event_seq": 0,
                        "last_applied_seq": 0,
                        "pending_events": 0,
                    },
                    "by_queue": [],
                    "message": error.to_string(),
                })
            });
        json!({
            "projection_db": {
                "state": match status.state {
                    ProjectionServiceState::Ok => "ok",
                    ProjectionServiceState::Degraded => "degraded",
                },
                "path_kind": "host_local",
                "schema_initialized": status.schema_initialized,
                "migrations_applied": status.migrations_applied,
                "repair_needed": status.repair_needed,
                "max_event_seq": status.max_event_seq,
                "gap_count": status.gap_count,
                "recovery_issue_count": status.recovery_issue_count,
                "source_exports": source_exports,
                "message": status.message,
            },
            "scheduler": scheduler,
        })
    }

    fn scheduler_health_summary(
        &self,
    ) -> Result<JsonValue, ProjectionServiceError> {
        self.read_blocking(|db| scheduler_health_summary(db.connection()))
    }

    fn source_export_health_summary(
        &self,
    ) -> Result<JsonValue, ProjectionServiceError> {
        self.read_blocking(source_export_health_summary)
    }

    pub async fn read<T, F>(&self, f: F) -> Result<T, ProjectionServiceError>
    where
        T: Send + 'static,
        F: FnOnce(&ProjectionDb) -> Result<T, ProjectionError> + Send + 'static,
    {
        let db = self.db()?;
        let metrics = self.inner.metrics.clone();
        tokio::task::spawn_blocking(move || {
            let started = Instant::now();
            let guard = db
                .lock()
                .map_err(|_| ProjectionServiceError::LockPoisoned)?;
            let result = f(&guard).map_err(ProjectionServiceError::from);
            metrics.record_projection_query(started.elapsed());
            result
        })
        .await
        .map_err(|error| ProjectionServiceError::Join(error.to_string()))?
    }

    pub fn read_blocking<T, F>(&self, f: F) -> Result<T, ProjectionServiceError>
    where
        F: FnOnce(&ProjectionDb) -> Result<T, ProjectionError>,
    {
        let db = self.db()?;
        let started = Instant::now();
        let guard = db
            .lock()
            .map_err(|_| ProjectionServiceError::LockPoisoned)?;
        let result = f(&guard).map_err(ProjectionServiceError::from);
        self.inner
            .metrics
            .record_projection_query(started.elapsed());
        result
    }

    pub fn write_blocking<T, F>(
        &self,
        f: F,
    ) -> Result<T, ProjectionServiceError>
    where
        F: FnOnce(&mut ProjectionDb) -> Result<T, ProjectionError>,
    {
        let db = self.db()?;
        let started = Instant::now();
        let mut guard = db
            .lock()
            .map_err(|_| ProjectionServiceError::LockPoisoned)?;
        let result = f(&mut guard).map_err(ProjectionServiceError::from);
        self.inner
            .metrics
            .record_projection_query(started.elapsed());
        result
    }

    pub async fn write<T, F>(&self, f: F) -> Result<T, ProjectionServiceError>
    where
        T: Send + 'static,
        F: FnOnce(&mut ProjectionDb) -> Result<T, ProjectionError>
            + Send
            + 'static,
    {
        let db = self.db()?;
        let metrics = self.inner.metrics.clone();
        tokio::task::spawn_blocking(move || {
            let started = Instant::now();
            let mut guard = db
                .lock()
                .map_err(|_| ProjectionServiceError::LockPoisoned)?;
            let result = f(&mut guard).map_err(ProjectionServiceError::from);
            metrics.record_projection_query(started.elapsed());
            result
        })
        .await
        .map_err(|error| ProjectionServiceError::Join(error.to_string()))?
    }

    pub async fn append_event(
        &self,
        request: EventAppendRequestWire,
    ) -> Result<EventAppendOutcomeWire, ProjectionServiceError> {
        let started = Instant::now();
        let result = self.write(move |db| db.append_event(request)).await;
        self.inner
            .metrics
            .record_projection_event_append(started.elapsed());
        result
    }

    pub async fn append_projected_event(
        &self,
        request: EventAppendRequestWire,
    ) -> Result<EventAppendOutcomeWire, ProjectionServiceError> {
        let started = Instant::now();
        let result = self
            .write(move |db| db.append_projected_event(request))
            .await;
        self.inner
            .metrics
            .record_projection_event_append(started.elapsed());
        result
    }

    pub fn append_mutation_event_with_outbox_blocking(
        &self,
        request: EventAppendRequestWire,
        resource_handle: Option<String>,
        source_exports: Vec<SourceExportPlanWire>,
        projection_snapshot: Option<JsonValue>,
    ) -> Result<LocalDaemonMutationOutcomeWire, ProjectionServiceError> {
        let started = Instant::now();
        let result = self.write_blocking(move |db| {
            let mut outcome = db.append_mutation_event_with_outbox(
                request,
                resource_handle,
                source_exports,
                projection_snapshot,
            )?;
            let export_ids: Vec<i64> = outcome
                .source_exports
                .iter()
                .filter(|report| {
                    matches!(
                        report.status,
                        sase_core::projections::SourceExportStatusWire::Pending
                            | sase_core::projections::SourceExportStatusWire::Failed
                            | sase_core::projections::SourceExportStatusWire::Conflict
                    )
                })
                .map(|report| report.export_id)
                .collect();
            if !export_ids.is_empty() {
                let mut reports = Vec::with_capacity(export_ids.len());
                for export_id in export_ids {
                    reports.push(db.retry_source_export_once(export_id)?);
                }
                outcome.source_exports = reports;
            }
            Ok(outcome)
        });
        self.inner
            .metrics
            .record_projection_event_append(started.elapsed());
        result
    }

    pub async fn rebuild_storage_reset_only(
        &self,
    ) -> Result<ProjectionRebuildReportWire, ProjectionServiceError> {
        let db = self.db()?;
        let metrics = self.inner.metrics.clone();
        let status_lock = Arc::clone(&self.inner.status);
        tokio::task::spawn_blocking(move || {
            let started = Instant::now();
            let mut guard = db
                .lock()
                .map_err(|_| ProjectionServiceError::LockPoisoned)?;
            let report =
                guard
                    .rebuild_all_projections(
                        ProjectionRebuildOptionsWire::default(),
                    )
                    .map_err(ProjectionServiceError::from)?;
            let repair = run_startup_repair_checks(&mut guard)
                .map_err(ProjectionServiceError::from)?;
            let status =
                status_from_report(status_lock_path(&status_lock), &repair);
            if let Ok(mut guard) = status_lock.write() {
                *guard = status;
            }
            metrics.record_projection_query(started.elapsed());
            Ok(report)
        })
        .await
        .map_err(|error| ProjectionServiceError::Join(error.to_string()))?
    }

    pub async fn retry_pending_source_exports(
        &self,
    ) -> Result<JsonValue, ProjectionServiceError> {
        self.write(retry_pending_source_exports_with_diagnostics)
            .await
    }

    pub async fn checkpoint(
        &self,
        mode: ProjectionWalCheckpointModeWire,
    ) -> Result<ProjectionCheckpointReportWire, ProjectionServiceError> {
        self.write(move |db| db.wal_checkpoint(mode)).await
    }

    pub async fn backup(
        &self,
        backup_path: PathBuf,
        metadata: ProjectionBackupMetadataWire,
    ) -> Result<ProjectionBackupReportWire, ProjectionServiceError> {
        self.read(move |db| {
            db.backup_vacuum_into_with_metadata(&backup_path, metadata)
        })
        .await
    }

    pub fn list_backups(
        &self,
        backups_dir: &Path,
    ) -> Result<ProjectionBackupListWire, ProjectionServiceError> {
        list_projection_backups(backups_dir)
            .map_err(ProjectionServiceError::from)
    }

    pub async fn restore_backup(
        &self,
        backup_path: PathBuf,
        target_path: PathBuf,
    ) -> Result<ProjectionRestoreReportWire, ProjectionServiceError> {
        let db_lock = Arc::clone(&self.inner.db);
        let status_lock = Arc::clone(&self.inner.status);
        let metrics = self.inner.metrics.clone();
        tokio::task::spawn_blocking(move || {
            let started = Instant::now();
            validate_projection_backup_file(&backup_path)
                .map_err(ProjectionServiceError::from)?;
            let metadata_path =
                sase_core::projections::projection_backup_metadata_path(
                    &backup_path,
                );
            let metadata = read_projection_backup_metadata(&metadata_path)
                .map_err(ProjectionServiceError::from)?;
            let mut db_slot = db_lock
                .write()
                .map_err(|_| ProjectionServiceError::LockPoisoned)?;
            if let Some(active_db) = db_slot.as_ref() {
                let guard = active_db
                    .lock()
                    .map_err(|_| ProjectionServiceError::LockPoisoned)?;
                guard
                    .wal_checkpoint(ProjectionWalCheckpointModeWire::Truncate)
                    .map_err(ProjectionServiceError::from)?;
            }
            *db_slot = None;
            if let Some(parent) = target_path.parent() {
                std::fs::create_dir_all(parent).map_err(|error| {
                    ProjectionServiceError::Projection(error.into())
                })?;
            }
            let replaced_existing = target_path.exists();
            for sidecar in [
                target_path.with_file_name(format!(
                    "{}-wal",
                    target_path
                        .file_name()
                        .and_then(|name| name.to_str())
                        .unwrap_or("projection.sqlite")
                )),
                target_path.with_file_name(format!(
                    "{}-shm",
                    target_path
                        .file_name()
                        .and_then(|name| name.to_str())
                        .unwrap_or("projection.sqlite")
                )),
            ] {
                if sidecar.exists() {
                    std::fs::remove_file(&sidecar).map_err(|error| {
                        ProjectionServiceError::Projection(error.into())
                    })?;
                }
            }
            std::fs::copy(&backup_path, &target_path).map_err(|error| {
                ProjectionServiceError::Projection(error.into())
            })?;
            let mut restored = ProjectionDb::open(&target_path)
                .map_err(ProjectionServiceError::from)?;
            let repair = run_startup_repair_checks(&mut restored)
                .map_err(ProjectionServiceError::from)?;
            let status = status_from_report(target_path.clone(), &repair);
            *db_slot = Some(Arc::new(Mutex::new(restored)));
            if let Ok(mut guard) = status_lock.write() {
                *guard = status;
            }
            let bytes = std::fs::metadata(&target_path)
                .map_err(|error| {
                    ProjectionServiceError::Projection(error.into())
                })?
                .len();
            metrics.record_projection_query(started.elapsed());
            Ok(ProjectionRestoreReportWire {
                schema_version:
                    sase_core::projections::PROJECTION_EVENT_SCHEMA_VERSION,
                backup_path: backup_path.display().to_string(),
                restored_path: target_path.display().to_string(),
                bytes,
                replaced_existing,
                projection_only: true,
                metadata,
            })
        })
        .await
        .map_err(|error| ProjectionServiceError::Join(error.to_string()))?
    }

    fn from_db(
        db: ProjectionDb,
        status: ProjectionServiceStatus,
        metrics: DaemonMetrics,
    ) -> Self {
        Self {
            inner: Arc::new(ProjectionServiceInner {
                db: Arc::new(RwLock::new(Some(Arc::new(Mutex::new(db))))),
                status: Arc::new(RwLock::new(status)),
                metrics,
            }),
        }
    }

    fn db(&self) -> Result<Arc<Mutex<ProjectionDb>>, ProjectionServiceError> {
        self.inner
            .db
            .read()
            .map_err(|_| ProjectionServiceError::LockPoisoned)?
            .clone()
            .ok_or_else(|| {
                let status = self.status();
                ProjectionServiceError::Unavailable(
                    status.message.unwrap_or_else(|| {
                        "projection database is degraded".to_string()
                    }),
                )
            })
    }
}

fn status_lock_path(status_lock: &RwLock<ProjectionServiceStatus>) -> PathBuf {
    status_lock
        .read()
        .map(|status| status.path.clone())
        .unwrap_or_default()
}

fn status_from_report(
    path: PathBuf,
    report: &ProjectionStartupRepairReportWire,
) -> ProjectionServiceStatus {
    let repair_needed =
        !report.gaps.is_empty() || !report.recovery_issues.is_empty();
    ProjectionServiceStatus {
        state: if repair_needed {
            ProjectionServiceState::Degraded
        } else {
            ProjectionServiceState::Ok
        },
        path,
        schema_initialized: true,
        migrations_applied: true,
        repair_needed,
        max_event_seq: Some(report.max_event_seq),
        gap_count: report.gaps.len(),
        recovery_issue_count: report.recovery_issues.len(),
        message: if repair_needed {
            Some(
                "projection replay could not fully repair startup gaps"
                    .to_string(),
            )
        } else {
            None
        },
    }
}

fn run_startup_repair_checks(
    db: &mut ProjectionDb,
) -> Result<ProjectionStartupRepairReportWire, ProjectionError> {
    let mut changespec = ChangeSpecProjectionApplier;
    let mut notifications = NotificationProjectionApplier;
    let mut beads = BeadProjectionApplier;
    let mut agents = AgentProjectionApplier;
    let mut workflows = WorkflowProjectionApplier;
    let mut catalogs = CatalogProjectionApplier;
    let mut scheduler = SchedulerProjectionApplier;
    let mut appliers: [&mut dyn ProjectionApplier; 7] = [
        &mut changespec,
        &mut notifications,
        &mut beads,
        &mut agents,
        &mut workflows,
        &mut catalogs,
        &mut scheduler,
    ];
    let mut report = db.repair_projection_gaps(&mut appliers)?;
    report
        .recovery_issues
        .extend(repair_pending_source_exports(db)?);
    Ok(report)
}

fn repair_pending_source_exports(
    db: &mut ProjectionDb,
) -> Result<Vec<ProjectionRecoveryIssueWire>, ProjectionError> {
    retry_pending_source_exports_with_diagnostics(db)?;

    let mut issues = Vec::new();
    for export in db.list_pending_source_exports()? {
        let event_type = db
            .get_event(export.event_seq)?
            .map(|event| event.event_type)
            .unwrap_or_else(|| "unknown".to_string());
        issues.push(ProjectionRecoveryIssueWire {
            projection: format!(
                "source_export:{}",
                source_export_surface(Some(&event_type), &export.plan.target_path)
            ),
            kind: format!("source_export_{:?}", export.status).to_lowercase(),
            guidance: format!(
                "export {} for {} is {:?}: {}; resolve the source conflict or rerun `sase daemon doctor`/`sase daemon rebuild` after the target is safe to rewrite",
                export.export_id,
                export.plan.target_path,
                export.status,
                export
                    .last_error
                    .as_deref()
                    .unwrap_or("no last export error recorded")
            ),
        });
    }
    Ok(issues)
}

fn retry_pending_source_exports_with_diagnostics(
    db: &mut ProjectionDb,
) -> Result<JsonValue, ProjectionError> {
    let exports = db.list_pending_source_exports()?;
    let mut attempted = 0_u64;
    let mut applied = 0_u64;
    let mut failed = 0_u64;
    let mut conflict = 0_u64;
    let mut preserved_conflicts = 0_u64;
    let mut examples = Vec::new();

    for export in exports {
        if matches!(
            export.status,
            SourceExportStatusWire::Pending | SourceExportStatusWire::Failed
        ) {
            attempted += 1;
            let report = db.retry_source_export_once(export.export_id)?;
            match report.status {
                SourceExportStatusWire::Applied => applied += 1,
                SourceExportStatusWire::Failed => failed += 1,
                SourceExportStatusWire::Conflict => conflict += 1,
                SourceExportStatusWire::Pending => {}
            }
            if examples.len() < 10 {
                examples.push(source_export_retry_example(
                    report.export_id,
                    report.event_seq,
                    &report.target_path,
                    source_export_status_name(&report.status),
                    report.attempts,
                    report.message.as_deref(),
                    db,
                )?);
            }
        } else if matches!(export.status, SourceExportStatusWire::Conflict) {
            preserved_conflicts += 1;
            if examples.len() < 10 {
                examples.push(source_export_retry_example(
                    export.export_id,
                    export.event_seq,
                    &export.plan.target_path,
                    "conflict",
                    export.attempts,
                    export.last_error.as_deref(),
                    db,
                )?);
            }
        }
    }

    Ok(json!({
        "attempted": attempted,
        "applied": applied,
        "failed": failed,
        "conflict": conflict,
        "preserved_conflicts": preserved_conflicts,
        "examples": examples,
        "next_command": if conflict > 0 || preserved_conflicts > 0 || failed > 0 {
            Some("sase daemon diff --surface all --json")
        } else {
            None
        },
    }))
}

fn source_export_retry_example(
    export_id: i64,
    event_seq: i64,
    target_path: &str,
    status: &str,
    attempts: i64,
    message: Option<&str>,
    db: &ProjectionDb,
) -> Result<JsonValue, ProjectionError> {
    let event_type = db.get_event(event_seq)?.map(|event| event.event_type);
    Ok(json!({
        "export_id": export_id,
        "event_seq": event_seq,
        "surface": source_export_surface(event_type.as_deref(), target_path),
        "status": status,
        "target_path": target_path,
        "attempts": attempts,
        "message": message,
    }))
}

fn source_export_health_summary(
    db: &ProjectionDb,
) -> Result<JsonValue, ProjectionError> {
    let exports = db.list_pending_source_exports()?;
    let mut pending = 0_u64;
    let mut failed = 0_u64;
    let mut conflict = 0_u64;
    let mut by_surface: BTreeMap<String, (u64, u64, u64, u64)> =
        BTreeMap::new();
    let mut examples = Vec::new();

    for export in exports {
        let event_type = db
            .get_event(export.event_seq)?
            .map(|event| event.event_type);
        let surface = source_export_surface(
            event_type.as_deref(),
            &export.plan.target_path,
        )
        .to_string();
        let entry = by_surface.entry(surface.clone()).or_default();
        entry.0 += 1;
        match &export.status {
            SourceExportStatusWire::Pending => {
                pending += 1;
                entry.1 += 1;
            }
            SourceExportStatusWire::Failed => {
                failed += 1;
                entry.2 += 1;
            }
            SourceExportStatusWire::Conflict => {
                conflict += 1;
                entry.3 += 1;
            }
            SourceExportStatusWire::Applied => {}
        }
        if examples.len() < 10 {
            examples.push(json!({
                "export_id": export.export_id,
                "event_seq": export.event_seq,
                "surface": surface,
                "status": source_export_status_name(&export.status),
                "target_path": export.plan.target_path,
                "attempts": export.attempts,
                "message": export.last_error,
            }));
        }
    }

    let by_surface = by_surface
        .into_iter()
        .map(|(surface, (total, pending, failed, conflict))| {
            json!({
                "surface": surface,
                "total": total,
                "pending": pending,
                "failed": failed,
                "conflict": conflict,
            })
        })
        .collect::<Vec<_>>();
    let total = pending + failed + conflict;
    let message = if total == 0 {
        None
    } else if conflict > 0 {
        Some(format!(
            "{total} source export(s) still need repair; {conflict} conflict(s) require `sase daemon diff --surface all --json` or manual source review"
        ))
    } else {
        Some(format!(
            "{total} source export(s) still need repair; rerun `sase daemon doctor --json` or `sase daemon rebuild --surface all --json`"
        ))
    };
    Ok(json!({
        "state": if total == 0 { "ok" } else { "degraded" },
        "message": message,
        "total": total,
        "pending": pending,
        "failed": failed,
        "conflict": conflict,
        "by_surface": by_surface,
        "examples": examples,
    }))
}

fn source_export_surface(
    event_type: Option<&str>,
    target_path: &str,
) -> &'static str {
    if let Some(event_type) = event_type {
        if event_type.starts_with("notification.")
            || event_type.starts_with("pending_action.")
        {
            return "notifications";
        }
        if event_type.starts_with("changespec.") {
            return "changespecs";
        }
        if event_type.starts_with("agent.") {
            return "agents";
        }
        if event_type.starts_with("bead.") {
            return "beads";
        }
        if event_type.starts_with("workflow.") {
            return "workflows";
        }
    }
    if target_path.ends_with("notifications.jsonl")
        || target_path.contains("pending_actions")
    {
        "notifications"
    } else if target_path.ends_with(".sase") || target_path.ends_with(".gp") {
        "changespecs"
    } else if target_path.contains("dismissed") || target_path.contains("agent")
    {
        "agents"
    } else if target_path.ends_with("issues.jsonl")
        || target_path.contains("/beads/")
    {
        "beads"
    } else if target_path.contains("workflow")
        || target_path.contains("hitl")
        || target_path.contains("response")
    {
        "workflows"
    } else {
        "unknown"
    }
}

fn source_export_status_name(status: &SourceExportStatusWire) -> &'static str {
    match status {
        SourceExportStatusWire::Pending => "pending",
        SourceExportStatusWire::Applied => "applied",
        SourceExportStatusWire::Failed => "failed",
        SourceExportStatusWire::Conflict => "conflict",
    }
}

#[cfg(test)]
mod tests {
    use sase_core::bead::wire::BeadTierWire;
    use sase_core::bead::{IssueTypeWire, IssueWire, StatusWire};
    use sase_core::projections::{
        bead_created_event_request, bead_projection_show,
        source_fingerprint_from_path, BeadProjectionEventContextWire,
        EventAppendRequestWire, EventSourceWire, SourceExportKindWire,
    };
    use serde_json::json;
    use tempfile::tempdir;

    use super::*;

    #[tokio::test]
    async fn opens_projection_db_and_reports_ok_health() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("run").join("projection.sqlite");

        let service = ProjectionService::initialize(path.clone()).await;
        let status = service.status();

        assert_eq!(status.state, ProjectionServiceState::Ok);
        assert_eq!(status.path, path);
        assert!(status.schema_initialized);
        assert!(status.migrations_applied);
        assert!(!status.repair_needed);
        assert_eq!(status.max_event_seq, Some(0));
        assert!(path.exists());
        assert_eq!(
            service.health_details()["projection_db"]["state"],
            json!("ok")
        );
    }

    #[tokio::test]
    async fn reports_degraded_when_projection_db_cannot_open() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("not-a-directory");
        std::fs::write(&path, b"file").unwrap();
        let db_path = path.join("projection.sqlite");

        let service = ProjectionService::initialize(db_path).await;
        let status = service.status();

        assert_eq!(status.state, ProjectionServiceState::Degraded);
        assert!(status.repair_needed);
        let message = status.message.unwrap();
        assert!(!message.is_empty());
    }

    #[tokio::test]
    async fn reads_run_through_blocking_manager() {
        let dir = tempdir().unwrap();
        let service =
            ProjectionService::initialize(dir.path().join("projection.sqlite"))
                .await;

        let count = service.read(|db| db.event_count()).await;
        let max_seq = service.read(|db| db.max_event_seq()).await;

        assert_eq!(count.unwrap(), 0);
        assert_eq!(max_seq.unwrap(), 0);
    }

    #[tokio::test]
    async fn projected_append_runs_domain_apply_on_blocking_manager() {
        let dir = tempdir().unwrap();
        let service =
            ProjectionService::initialize(dir.path().join("projection.sqlite"))
                .await;
        let issue = issue("sase-1");
        let request =
            bead_created_event_request(context(), issue.clone()).unwrap();

        service.append_projected_event(request).await.unwrap();
        let loaded = service
            .read(|db| {
                bead_projection_show(db.connection(), "project-a", "sase-1")
            })
            .await
            .unwrap();

        assert_eq!(loaded, issue);
    }

    #[tokio::test]
    async fn rebuild_resets_and_replays_projection_tables() {
        let dir = tempdir().unwrap();
        let service =
            ProjectionService::initialize(dir.path().join("projection.sqlite"))
                .await;

        let report = service.rebuild_storage_reset_only().await.unwrap();

        assert_eq!(report.schema_version, 1);
        assert_eq!(report.reset.event_count_before, 0);
        assert_eq!(report.reset.event_count_after, 0);
        assert_eq!(service.status().state, ProjectionServiceState::Ok);
    }

    #[tokio::test]
    async fn startup_repairs_pending_source_exports() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("projection.sqlite");
        let target = dir.path().join("notifications.jsonl");
        {
            let mut db = ProjectionDb::open(&db_path).unwrap();
            db.append_mutation_event_with_outbox(
                event_request("mutation.test", "export-ok"),
                Some("notification:n1".to_string()),
                vec![SourceExportPlanWire::new(
                    target.display().to_string(),
                    SourceExportKindWire::JsonlAppend,
                    "{\"id\":\"n1\"}\n",
                )],
                None,
            )
            .unwrap();
        }

        let service = ProjectionService::initialize(db_path).await;

        assert_eq!(
            std::fs::read_to_string(&target).unwrap(),
            "{\"id\":\"n1\"}\n"
        );
        let source_exports =
            &service.health_details()["projection_db"]["source_exports"];
        assert_eq!(source_exports["state"], json!("ok"));
        assert_eq!(source_exports["total"], json!(0));
    }

    #[tokio::test]
    async fn startup_reports_conflicted_source_exports_by_surface() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("projection.sqlite");
        let target = dir.path().join("notifications.jsonl");
        std::fs::write(&target, "{\"id\":\"old\"}\n").unwrap();
        let expected = source_fingerprint_from_path(&target, true).unwrap();
        std::fs::write(&target, "{\"id\":\"legacy\"}\n").unwrap();
        {
            let mut db = ProjectionDb::open(&db_path).unwrap();
            let mut plan = SourceExportPlanWire::new(
                target.display().to_string(),
                SourceExportKindWire::AtomicJson,
                "{\"id\":\"new\"}\n",
            );
            plan.expected_fingerprint = Some(expected);
            db.append_mutation_event_with_outbox(
                event_request("mutation.test", "export-conflict"),
                Some("notification:n1".to_string()),
                vec![plan],
                None,
            )
            .unwrap();
        }

        let service = ProjectionService::initialize(db_path).await;
        let source_exports =
            &service.health_details()["projection_db"]["source_exports"];

        assert_eq!(service.status().state, ProjectionServiceState::Degraded);
        assert_eq!(source_exports["state"], json!("degraded"));
        assert_eq!(source_exports["conflict"], json!(1));
        assert_eq!(
            source_exports["by_surface"][0]["surface"],
            json!("notifications")
        );
        assert_eq!(source_exports["examples"][0]["status"], json!("conflict"));
        assert!(source_exports["message"]
            .as_str()
            .unwrap()
            .contains("manual source review"));
    }

    #[tokio::test]
    async fn live_retry_reports_preserved_conflicts_with_targets() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("projection.sqlite");
        let target = dir.path().join("notifications.jsonl");
        std::fs::write(&target, "{\"id\":\"old\"}\n").unwrap();
        let expected = source_fingerprint_from_path(&target, true).unwrap();
        std::fs::write(&target, "{\"id\":\"legacy\"}\n").unwrap();
        {
            let mut db = ProjectionDb::open(&db_path).unwrap();
            let mut plan = SourceExportPlanWire::new(
                target.display().to_string(),
                SourceExportKindWire::AtomicJson,
                "{\"id\":\"new\"}\n",
            );
            plan.expected_fingerprint = Some(expected);
            db.append_mutation_event_with_outbox(
                event_request("notification.rewrite", "export-conflict-live"),
                Some("notification:n1".to_string()),
                vec![plan],
                None,
            )
            .unwrap();
        }

        let service = ProjectionService::initialize(db_path).await;
        let retry = service.retry_pending_source_exports().await.unwrap();

        assert_eq!(retry["attempted"], json!(0));
        assert_eq!(retry["preserved_conflicts"], json!(1));
        assert_eq!(retry["examples"][0]["surface"], json!("notifications"));
        assert_eq!(
            retry["examples"][0]["target_path"],
            json!(target.display().to_string())
        );
    }

    #[tokio::test]
    async fn backup_and_restore_are_projection_only() {
        let dir = tempdir().unwrap();
        let db_path = dir
            .path()
            .join("run")
            .join("projections")
            .join("projection.sqlite");
        let backup_path = dir
            .path()
            .join("run")
            .join("backups")
            .join("restore.sqlite");
        let source_store = dir.path().join("projects").join("demo.sase");
        std::fs::create_dir_all(source_store.parent().unwrap()).unwrap();
        std::fs::write(&source_store, "NAME: source\n").unwrap();
        let service = ProjectionService::initialize(db_path.clone()).await;

        let backup = service
            .backup(
                backup_path.clone(),
                ProjectionBackupMetadataWire {
                    schema_version: 1,
                    backup_format_version: 1,
                    daemon_version: "test".to_string(),
                    core_version: "test".to_string(),
                    host_identity: "host-a".to_string(),
                    source_sase_home: dir.path().display().to_string(),
                    created_at: "2026-05-14T00:00:00Z".to_string(),
                    projection_schema: 0,
                    event_max_sequence: 0,
                    source_export_summary: json!({"state": "ok"}),
                    original_db_path: db_path.display().to_string(),
                    snapshot_path: backup_path.display().to_string(),
                },
            )
            .await
            .unwrap();
        assert!(Path::new(&backup.metadata_path).is_file());

        let second_issue = issue("sase-restore-2");
        service
            .append_projected_event(
                bead_created_event_request(context(), second_issue).unwrap(),
            )
            .await
            .unwrap();
        let restored = service
            .restore_backup(backup_path.clone(), db_path.clone())
            .await
            .unwrap();

        assert!(restored.projection_only);
        assert_eq!(restored.metadata.event_max_sequence, 0);
        assert_eq!(
            std::fs::read_to_string(&source_store).unwrap(),
            "NAME: source\n"
        );
        assert_eq!(service.read(|db| db.event_count()).await.unwrap(), 0);
        assert!(service
            .read(|db| {
                bead_projection_show(
                    db.connection(),
                    "project-a",
                    "sase-restore-2",
                )
            })
            .await
            .is_err());
        assert_eq!(service.status().state, ProjectionServiceState::Ok);
    }

    fn event_request(event_type: &str, key: &str) -> EventAppendRequestWire {
        EventAppendRequestWire {
            created_at: Some("2026-05-14T00:00:00.000Z".to_string()),
            source: EventSourceWire {
                source_type: "test".to_string(),
                name: "projection-service-test".to_string(),
                ..EventSourceWire::default()
            },
            host_id: "host-a".to_string(),
            project_id: "project-a".to_string(),
            event_type: event_type.to_string(),
            payload: json!({"ok": true}),
            idempotency_key: Some(key.to_string()),
            causality: vec![],
            source_path: None,
            source_revision: None,
        }
    }

    fn context() -> BeadProjectionEventContextWire {
        BeadProjectionEventContextWire {
            created_at: Some("2026-05-13T21:00:00.000Z".to_string()),
            source: EventSourceWire {
                source_type: "test".to_string(),
                name: "projection-service-test".to_string(),
                ..EventSourceWire::default()
            },
            host_id: "host-a".to_string(),
            project_id: "project-a".to_string(),
            idempotency_key: None,
            causality: vec![],
            source_path: Some("sdd/beads/issues.jsonl".to_string()),
            source_revision: None,
        }
    }

    fn issue(id: &str) -> IssueWire {
        IssueWire {
            id: id.to_string(),
            title: "Projected".to_string(),
            status: StatusWire::Open,
            issue_type: IssueTypeWire::Plan,
            tier: Some(BeadTierWire::Epic),
            parent_id: None,
            owner: String::new(),
            assignee: String::new(),
            created_at: "2026-05-13T21:00:00Z".to_string(),
            created_by: String::new(),
            updated_at: "2026-05-13T21:00:00Z".to_string(),
            closed_at: None,
            close_reason: None,
            description: String::new(),
            notes: String::new(),
            design: String::new(),
            model: String::new(),
            is_ready_to_work: false,
            epic_count: None,
            changespec_name: String::new(),
            changespec_bug_id: String::new(),
            dependencies: vec![],
        }
    }
}
