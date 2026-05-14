use std::{
    fmt,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, RwLock},
    time::Instant,
};

use sase_core::projections::{
    AgentProjectionApplier, BeadProjectionApplier, CatalogProjectionApplier,
    ChangeSpecProjectionApplier, EventAppendOutcomeWire,
    EventAppendRequestWire, NotificationProjectionApplier, ProjectionApplier,
    ProjectionDb, ProjectionError, ProjectionRebuildOptionsWire,
    ProjectionRebuildReportWire, ProjectionStartupRepairReportWire,
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
    db: Option<Arc<Mutex<ProjectionDb>>>,
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
                db: None,
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
                "message": status.message,
            }
        })
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

    fn from_db(
        db: ProjectionDb,
        status: ProjectionServiceStatus,
        metrics: DaemonMetrics,
    ) -> Self {
        Self {
            inner: Arc::new(ProjectionServiceInner {
                db: Some(Arc::new(Mutex::new(db))),
                status: Arc::new(RwLock::new(status)),
                metrics,
            }),
        }
    }

    fn db(&self) -> Result<Arc<Mutex<ProjectionDb>>, ProjectionServiceError> {
        self.inner.db.clone().ok_or_else(|| {
            let status = self.status();
            ProjectionServiceError::Unavailable(status.message.unwrap_or_else(
                || "projection database is degraded".to_string(),
            ))
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
    let mut appliers: [&mut dyn ProjectionApplier; 6] = [
        &mut changespec,
        &mut notifications,
        &mut beads,
        &mut agents,
        &mut workflows,
        &mut catalogs,
    ];
    db.repair_projection_gaps(&mut appliers)
}

#[cfg(test)]
mod tests {
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
}
