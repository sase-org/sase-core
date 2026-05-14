use std::{
    collections::BTreeMap,
    fmt,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, RwLock},
    time::Duration,
};

use notify::{
    event::ModifyKind, EventKind, RecommendedWatcher, RecursiveMode, Watcher,
};
use sase_core::projections::{
    agent_projection_event_request_for_artifact_dir,
    agent_projection_shadow_diff, agent_source_change_from_path,
    bead_backfill_snapshot, bead_backfill_snapshot_event_request,
    bead_projection_shadow_diff, catalog_backfill_plan, catalog_shadow_diff,
    changespec_event_request_for_source_change,
    changespec_source_change_from_path, notification_backfill_event_requests,
    notification_event_requests_for_source_change, notification_shadow_diff,
    notification_source_change_from_path, projection_reconciliation_plan,
    source_fingerprint_from_path, AgentProjectionEventContextWire,
    BeadProjectionEventContextWire, CatalogBackfillOptionsWire,
    EventAppendRequestWire, EventSourceWire, IndexingDomainReportWire,
    ProjectionReconciliationPlanWire, ShadowDiffCountsWire,
    ShadowDiffReportWire, SourceChangeOperationWire, SourceChangeWire,
    SourceIdentityWire, AGENT_PROJECTION_NAME, BEAD_INDEXING_DOMAIN,
    BEAD_PROJECTION_NAME, CATALOG_PROJECTION_NAME, CHANGESPEC_PROJECTION_NAME,
    INDEXING_WIRE_SCHEMA_VERSION, NOTIFICATION_PROJECTION_NAME,
};
use sase_core::{
    agent_scan::{scan_agent_artifacts, AgentArtifactScanOptionsWire},
    projections::{
        backfill_agent_projection_from_scan, discover_bead_stores,
        ProjectionDb, ProjectionError,
    },
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};
use thiserror::Error;
use tokio::sync::{mpsc, Semaphore};

use crate::{
    daemon::{DaemonShutdown, LocalEventHub},
    metrics::{DaemonMetrics, IndexingMetricsReport},
    projection_service::ProjectionService,
    wire::{
        LocalDaemonCollectionWire, LocalDaemonDeltaOperationWire,
        LocalDaemonEventPayloadWire, LocalDaemonIndexingDiffRequestWire,
        LocalDaemonIndexingDiffResponseWire,
        LocalDaemonIndexingStatusRequestWire,
        LocalDaemonIndexingStatusResponseWire,
        LocalDaemonIndexingSurfaceSummaryWire, LocalDaemonIndexingSurfaceWire,
        LocalDaemonIndexingVerifyRequestWire,
        LocalDaemonIndexingVerifyResponseWire, LocalDaemonPayloadBoundWire,
        LocalDaemonRebuildRequestWire, LocalDaemonRebuildResponseWire,
        LOCAL_DAEMON_MAX_PAGE_LIMIT, LOCAL_DAEMON_MAX_PAYLOAD_BYTES,
        LOCAL_DAEMON_WIRE_SCHEMA_VERSION,
    },
};

const DEFAULT_INDEXING_QUEUE_CAPACITY: usize = 512;
const DEFAULT_INDEXING_WORKER_CAPACITY: usize = 2;
const DEFAULT_DEBOUNCE_MS: u64 = 75;
const DEFAULT_RECONCILIATION_INTERVAL_SECS: u64 = 30;
const DEFAULT_RECONCILIATION_CHANGE_CAP: usize = 256;
const DEFAULT_INDEXING_BATCH_CAPACITY: usize = 128;
const RECENT_REPORT_LIMIT: usize = 8;

#[derive(Clone, Debug)]
pub struct IndexingConfig {
    pub enabled: bool,
    pub watch_roots: Vec<PathBuf>,
    pub projects_root: Option<PathBuf>,
    pub host_id: String,
    pub queue_capacity: usize,
    pub worker_capacity: usize,
    pub debounce: Duration,
    pub reconciliation_interval: Duration,
    pub max_reconciliation_changes: usize,
    pub max_batch_size: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexingServiceStatusWire {
    pub schema_version: u32,
    pub state: IndexingServiceStateWire,
    pub enabled: bool,
    pub watcher_active: bool,
    pub queued_changes: u64,
    pub dropped_changes: u64,
    pub coalesced_changes: u64,
    pub indexed_sources: u64,
    pub failed_parses: u64,
    pub diff_counts: ShadowDiffCountsWire,
    pub recent_reports: Vec<IndexingDomainReportWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IndexingServiceStateWire {
    Ok,
    Degraded,
    Stopped,
}

#[derive(Debug, Error)]
pub enum IndexingError {
    #[error("failed to initialize filesystem watcher: {0}")]
    WatcherInit(String),
    #[error("failed to watch {path}: {source}")]
    WatchPath {
        path: PathBuf,
        source: notify::Error,
    },
}

pub trait SourceWatcher: Send {}

pub trait SourceWatcherFactory: Send + Sync {
    fn start(
        &self,
        roots: &[PathBuf],
        sender: mpsc::Sender<SourceChangeWire>,
    ) -> Result<Box<dyn SourceWatcher>, IndexingError>;
}

#[derive(Clone, Debug, Default)]
pub struct NotifySourceWatcherFactory;

pub struct NotifySourceWatcher {
    _watcher: RecommendedWatcher,
}

impl SourceWatcher for NotifySourceWatcher {}

#[derive(Clone)]
pub struct IndexingService {
    inner: Arc<IndexingServiceInner>,
}

struct IndexingServiceInner {
    sender: Option<mpsc::Sender<SourceChangeWire>>,
    status: Arc<RwLock<IndexingServiceStatusWire>>,
    known_sources: Arc<RwLock<BTreeMap<String, SourceIdentityWire>>>,
    _watcher: Mutex<Option<Box<dyn SourceWatcher>>>,
    metrics: DaemonMetrics,
    projects_root: Option<PathBuf>,
    host_id: String,
}

impl fmt::Debug for IndexingService {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IndexingService")
            .field("status", &self.status())
            .finish_non_exhaustive()
    }
}

impl Default for IndexingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            watch_roots: Vec::new(),
            projects_root: None,
            host_id: "local".to_string(),
            queue_capacity: DEFAULT_INDEXING_QUEUE_CAPACITY,
            worker_capacity: DEFAULT_INDEXING_WORKER_CAPACITY,
            debounce: Duration::from_millis(DEFAULT_DEBOUNCE_MS),
            reconciliation_interval: Duration::from_secs(
                DEFAULT_RECONCILIATION_INTERVAL_SECS,
            ),
            max_reconciliation_changes: DEFAULT_RECONCILIATION_CHANGE_CAP,
            max_batch_size: DEFAULT_INDEXING_BATCH_CAPACITY,
        }
    }
}

impl IndexingService {
    pub fn disabled(metrics: DaemonMetrics) -> Self {
        let status = IndexingServiceStatusWire {
            schema_version: INDEXING_WIRE_SCHEMA_VERSION,
            state: IndexingServiceStateWire::Stopped,
            enabled: false,
            watcher_active: false,
            queued_changes: 0,
            dropped_changes: 0,
            coalesced_changes: 0,
            indexed_sources: 0,
            failed_parses: 0,
            diff_counts: ShadowDiffCountsWire::default(),
            recent_reports: Vec::new(),
            message: Some("indexing service is disabled".to_string()),
        };
        Self {
            inner: Arc::new(IndexingServiceInner {
                sender: None,
                status: Arc::new(RwLock::new(status)),
                known_sources: Arc::new(RwLock::new(BTreeMap::new())),
                _watcher: Mutex::new(None),
                metrics,
                projects_root: None,
                host_id: "local".to_string(),
            }),
        }
    }

    pub fn start(
        config: IndexingConfig,
        shutdown: DaemonShutdown,
        metrics: DaemonMetrics,
        local_events: LocalEventHub,
        projection_service: ProjectionService,
    ) -> Self {
        Self::start_with_factory(
            config,
            shutdown,
            metrics,
            local_events,
            projection_service,
            NotifySourceWatcherFactory,
        )
    }

    pub fn start_with_factory<F>(
        config: IndexingConfig,
        shutdown: DaemonShutdown,
        metrics: DaemonMetrics,
        local_events: LocalEventHub,
        projection_service: ProjectionService,
        factory: F,
    ) -> Self
    where
        F: SourceWatcherFactory + 'static,
    {
        if !config.enabled {
            return Self::disabled(metrics);
        }

        let queue_capacity = config.queue_capacity.max(1);
        let worker_capacity = config.worker_capacity.max(1);
        let max_batch_size = config.max_batch_size.max(1);
        let max_reconciliation_changes =
            config.max_reconciliation_changes.max(1);
        let (sender, receiver) = mpsc::channel(queue_capacity);
        let status = Arc::new(RwLock::new(IndexingServiceStatusWire {
            schema_version: INDEXING_WIRE_SCHEMA_VERSION,
            state: IndexingServiceStateWire::Ok,
            enabled: true,
            watcher_active: true,
            queued_changes: 0,
            dropped_changes: 0,
            coalesced_changes: 0,
            indexed_sources: 0,
            failed_parses: 0,
            diff_counts: ShadowDiffCountsWire::default(),
            recent_reports: Vec::new(),
            message: None,
        }));

        let watcher_result = factory.start(&config.watch_roots, sender.clone());
        let watcher = match watcher_result {
            Ok(watcher) => Some(watcher),
            Err(error) => {
                mark_degraded(&status, error.to_string());
                local_events
                    .append_resync_required("indexing_watcher_unavailable");
                None
            }
        };
        let known_sources = Arc::new(RwLock::new(BTreeMap::new()));

        spawn_changespec_backfill(
            config.projects_root.clone(),
            config.host_id.clone(),
            projection_service.clone(),
            status.clone(),
            metrics.clone(),
            local_events.clone(),
        );

        spawn_reconciliation_loop(
            sender.clone(),
            status.clone(),
            known_sources.clone(),
            metrics.clone(),
            local_events.clone(),
            shutdown.clone(),
            config.projects_root.clone(),
            config.reconciliation_interval,
            max_reconciliation_changes,
        );

        let service_projects_root = config.projects_root.clone();
        let service_host_id = config.host_id.clone();
        tokio::spawn(worker_loop(WorkerLoopArgs {
            receiver,
            status: status.clone(),
            known_sources: known_sources.clone(),
            metrics: metrics.clone(),
            local_events,
            shutdown,
            debounce: config.debounce,
            worker_capacity,
            max_batch_size,
            projects_root: config.projects_root.clone(),
            host_id: config.host_id.clone(),
            projection_service,
        }));

        Self {
            inner: Arc::new(IndexingServiceInner {
                sender: Some(sender),
                status,
                known_sources,
                _watcher: Mutex::new(watcher),
                metrics,
                projects_root: service_projects_root,
                host_id: service_host_id,
            }),
        }
    }

    pub fn status(&self) -> IndexingServiceStatusWire {
        let _known_source_count = self
            .inner
            .known_sources
            .read()
            .map(|sources| sources.len())
            .ok();
        self.inner
            .status
            .read()
            .map(|status| status.clone())
            .unwrap_or_else(|_| IndexingServiceStatusWire {
                schema_version: INDEXING_WIRE_SCHEMA_VERSION,
                state: IndexingServiceStateWire::Degraded,
                enabled: false,
                watcher_active: false,
                queued_changes: 0,
                dropped_changes: 0,
                coalesced_changes: 0,
                indexed_sources: 0,
                failed_parses: 0,
                diff_counts: ShadowDiffCountsWire::default(),
                recent_reports: Vec::new(),
                message: Some("indexing status lock was poisoned".to_string()),
            })
    }

    pub fn health_details(&self) -> JsonValue {
        serde_json::to_value(self.status()).unwrap_or_else(|error| {
            json!({
                "schema_version": INDEXING_WIRE_SCHEMA_VERSION,
                "state": "degraded",
                "message": format!("failed to encode indexing status: {error}"),
            })
        })
    }

    pub fn enqueue(&self, change: SourceChangeWire) -> bool {
        let Some(sender) = self.inner.sender.as_ref() else {
            self.inner.metrics.record_indexing_dropped_change();
            increment_dropped(&self.inner.status);
            return false;
        };
        match sender.try_send(change) {
            Ok(()) => true,
            Err(_) => {
                self.inner.metrics.record_indexing_dropped_change();
                increment_dropped(&self.inner.status);
                false
            }
        }
    }

    pub async fn rebuild(
        &self,
        request: LocalDaemonRebuildRequestWire,
        projection_service: ProjectionService,
    ) -> Result<LocalDaemonRebuildResponseWire, String> {
        if request.storage_reset_only {
            let report = projection_service
                .rebuild_storage_reset_only()
                .await
                .map_err(|error| error.to_string())?;
            return Ok(LocalDaemonRebuildResponseWire {
                schema_version: LOCAL_DAEMON_WIRE_SCHEMA_VERSION,
                mode: "projection_storage_rebuild".to_string(),
                storage_reset_only: true,
                surface: request.surface,
                project_id: request.project_id,
                limitation: Some(
                    "storage reset replayed retained projection events only"
                        .to_string(),
                ),
                report: serde_json::to_value(report).unwrap_or_else(
                    |error| json!({"error": error.to_string()}),
                ),
                summaries: Vec::new(),
            });
        }

        let selector = IndexingSelector {
            surface: request.surface,
            project_id: request.project_id.clone(),
        };
        let projects_root = self.inner.projects_root.clone();
        let host_id = self.inner.host_id.clone();
        let summaries = projection_service
            .write(move |db| {
                rebuild_selected_sources(db, projects_root, host_id, selector)
            })
            .await
            .map_err(|error| error.to_string())?;

        Ok(LocalDaemonRebuildResponseWire {
            schema_version: LOCAL_DAEMON_WIRE_SCHEMA_VERSION,
            mode: "source_backfill".to_string(),
            storage_reset_only: false,
            surface: request.surface,
            project_id: request.project_id,
            limitation: None,
            report: json!({"summaries": summaries}),
            summaries,
        })
    }

    pub async fn verify(
        &self,
        request: LocalDaemonIndexingVerifyRequestWire,
        projection_service: ProjectionService,
    ) -> Result<LocalDaemonIndexingVerifyResponseWire, String> {
        let selector = IndexingSelector {
            surface: request.surface,
            project_id: request.project_id.clone(),
        };
        let projects_root = self.inner.projects_root.clone();
        let summaries = projection_service
            .read(move |db| {
                verify_selected_sources(db, projects_root, selector)
            })
            .await
            .map_err(|error| error.to_string())?;
        let ok = summaries.iter().all(|summary| {
            summary.diff_counts.missing == 0
                && summary.diff_counts.stale == 0
                && summary.diff_counts.extra == 0
                && summary.diff_counts.corrupt == 0
        });
        Ok(LocalDaemonIndexingVerifyResponseWire {
            schema_version: LOCAL_DAEMON_WIRE_SCHEMA_VERSION,
            ok,
            summaries,
        })
    }

    pub async fn diff(
        &self,
        request: LocalDaemonIndexingDiffRequestWire,
        projection_service: ProjectionService,
    ) -> Result<LocalDaemonIndexingDiffResponseWire, String> {
        let selector = IndexingSelector {
            surface: request.surface,
            project_id: request.project_id.clone(),
        };
        let projects_root = self.inner.projects_root.clone();
        let limit = request.page.limit.clamp(1, LOCAL_DAEMON_MAX_PAGE_LIMIT);
        let offset = request
            .page
            .cursor
            .as_deref()
            .and_then(|cursor| cursor.parse::<usize>().ok())
            .unwrap_or(0);
        let max_payload_bytes = request
            .max_payload_bytes
            .unwrap_or(LOCAL_DAEMON_MAX_PAYLOAD_BYTES);
        let reports = projection_service
            .read(move |db| diff_selected_sources(db, projects_root, selector))
            .await
            .map_err(|error| error.to_string())?;

        let mut counts = ShadowDiffCountsWire::default();
        let mut records = Vec::new();
        for report in reports {
            counts.missing += report.counts.missing;
            counts.stale += report.counts.stale;
            counts.extra += report.counts.extra;
            counts.corrupt += report.counts.corrupt;
            records.extend(report.records);
        }
        records.sort_by(|left, right| {
            left.domain
                .cmp(&right.domain)
                .then(left.source_path.cmp(&right.source_path))
                .then(left.handle.cmp(&right.handle))
                .then(left.message.cmp(&right.message))
        });
        let total = records.len();
        let records = records
            .into_iter()
            .skip(offset)
            .take(limit as usize)
            .collect::<Vec<_>>();
        let next_offset = offset + records.len();
        Ok(LocalDaemonIndexingDiffResponseWire {
            schema_version: LOCAL_DAEMON_WIRE_SCHEMA_VERSION,
            surface: request.surface,
            project_id: request.project_id,
            records,
            counts,
            next_cursor: (next_offset < total).then(|| next_offset.to_string()),
            bounded: LocalDaemonPayloadBoundWire {
                max_payload_bytes,
                truncated: next_offset < total,
            },
        })
    }

    pub fn indexing_status(
        &self,
        request: LocalDaemonIndexingStatusRequestWire,
        projection_service: &ProjectionService,
    ) -> LocalDaemonIndexingStatusResponseWire {
        let service = self.status();
        let projection_status = projection_service.status();
        let summaries = selected_surfaces(request.surface)
            .into_iter()
            .map(|surface| LocalDaemonIndexingSurfaceSummaryWire {
                schema_version: LOCAL_DAEMON_WIRE_SCHEMA_VERSION,
                surface,
                project_id: request.project_id.clone(),
                state: match service.state {
                    IndexingServiceStateWire::Ok => "ok",
                    IndexingServiceStateWire::Degraded => "degraded",
                    IndexingServiceStateWire::Stopped => "stopped",
                }
                .to_string(),
                last_indexed_event_seq: projection_status.max_event_seq,
                queued_changes: service.queued_changes,
                watcher_active: service.watcher_active,
                diff_counts: service.diff_counts.clone(),
                message: service.message.clone(),
            })
            .collect();
        LocalDaemonIndexingStatusResponseWire {
            schema_version: LOCAL_DAEMON_WIRE_SCHEMA_VERSION,
            service: serde_json::to_value(service)
                .unwrap_or_else(|error| json!({"error": error.to_string()})),
            summaries,
        }
    }
}

#[derive(Clone, Debug)]
struct IndexingSelector {
    surface: LocalDaemonIndexingSurfaceWire,
    project_id: Option<String>,
}

fn selected_surfaces(
    surface: LocalDaemonIndexingSurfaceWire,
) -> Vec<LocalDaemonIndexingSurfaceWire> {
    match surface {
        LocalDaemonIndexingSurfaceWire::All => vec![
            LocalDaemonIndexingSurfaceWire::Changespecs,
            LocalDaemonIndexingSurfaceWire::Notifications,
            LocalDaemonIndexingSurfaceWire::Agents,
            LocalDaemonIndexingSurfaceWire::Beads,
            LocalDaemonIndexingSurfaceWire::Catalogs,
        ],
        other => vec![other],
    }
}

fn rebuild_selected_sources(
    db: &mut ProjectionDb,
    projects_root: Option<PathBuf>,
    host_id: String,
    selector: IndexingSelector,
) -> Result<Vec<LocalDaemonIndexingSurfaceSummaryWire>, ProjectionError> {
    let mut summaries = Vec::new();
    for surface in selected_surfaces(selector.surface) {
        match surface {
            LocalDaemonIndexingSurfaceWire::Changespecs => {
                let Some(root) = projects_root.as_deref() else {
                    summaries.push(empty_summary(
                        surface,
                        selector.project_id.clone(),
                        "degraded",
                        "projects root is unavailable",
                    ));
                    continue;
                };
                let source = EventSourceWire {
                    source_type: "daemon".to_string(),
                    name: "shadow-changespec-indexer".to_string(),
                    ..EventSourceWire::default()
                };
                let outcomes = db.backfill_changespec_sources(
                    root,
                    source,
                    host_id.clone(),
                )?;
                let diff = db.diff_changespec_projection(root)?;
                summaries.push(summary_from_report(
                    surface,
                    selector.project_id.clone(),
                    Some(db.projection_last_seq(CHANGESPEC_PROJECTION_NAME)?),
                    outcomes.len() as u64,
                    true,
                    &diff,
                    None,
                ));
            }
            LocalDaemonIndexingSurfaceWire::Agents => {
                let Some(root) = projects_root.as_deref() else {
                    summaries.push(empty_summary(
                        surface,
                        selector.project_id.clone(),
                        "degraded",
                        "projects root is unavailable",
                    ));
                    continue;
                };
                let scan = scan_agent_artifacts(
                    root,
                    AgentArtifactScanOptionsWire::default(),
                );
                let context = AgentProjectionEventContextWire {
                    schema_version: INDEXING_WIRE_SCHEMA_VERSION,
                    source: EventSourceWire {
                        source_type: "daemon".to_string(),
                        name: "shadow-agent-indexer".to_string(),
                        ..EventSourceWire::default()
                    },
                    host_id: host_id.clone(),
                    project_id: selector
                        .project_id
                        .clone()
                        .unwrap_or_else(|| "all".to_string()),
                    ..AgentProjectionEventContextWire::default()
                };
                let appended =
                    backfill_agent_projection_from_scan(db, context, &scan)?;
                let project_ids = selected_project_ids_from_scan(
                    selector.project_id.as_deref(),
                    &scan,
                );
                if project_ids.is_empty() {
                    summaries.push(empty_summary_for_projection(
                        db,
                        surface,
                        selector.project_id.clone(),
                        AGENT_PROJECTION_NAME,
                    )?);
                }
                for project_id in project_ids {
                    let diff = agent_projection_shadow_diff(
                        db.connection(),
                        &project_id,
                        &scan,
                    )?;
                    summaries.push(summary_from_report(
                        surface,
                        Some(project_id),
                        Some(db.projection_last_seq(AGENT_PROJECTION_NAME)?),
                        appended,
                        true,
                        &diff,
                        None,
                    ));
                }
            }
            LocalDaemonIndexingSurfaceWire::Beads => {
                let Some(root) = projects_root.as_deref() else {
                    summaries.push(empty_summary(
                        surface,
                        selector.project_id.clone(),
                        "degraded",
                        "projects root is unavailable",
                    ));
                    continue;
                };
                let stores =
                    selected_bead_stores(root, selector.project_id.as_deref());
                if stores.is_empty() {
                    summaries.push(empty_summary_for_projection(
                        db,
                        surface,
                        selector.project_id.clone(),
                        BEAD_PROJECTION_NAME,
                    )?);
                }
                for store in stores {
                    let context = BeadProjectionEventContextWire {
                        source: EventSourceWire {
                            source_type: "daemon".to_string(),
                            name: "shadow-bead-indexer".to_string(),
                            ..EventSourceWire::default()
                        },
                        host_id: host_id.clone(),
                        project_id: store.project_id.clone(),
                        source_path: Some(store.issues_path.clone()),
                        ..BeadProjectionEventContextWire::default()
                    };
                    let beads_dir = PathBuf::from(&store.beads_dir);
                    let outcome =
                        bead_backfill_snapshot(db, context, &beads_dir)?;
                    let diff = bead_projection_shadow_diff(
                        db.connection(),
                        &store.project_id,
                        &beads_dir,
                    )?;
                    summaries.push(summary_from_report(
                        surface,
                        Some(store.project_id),
                        Some(db.projection_last_seq(BEAD_PROJECTION_NAME)?),
                        u64::from(!outcome.duplicate),
                        true,
                        &diff,
                        None,
                    ));
                }
            }
            LocalDaemonIndexingSurfaceWire::Catalogs => {
                let options = catalog_options(
                    projects_root.as_deref(),
                    &host_id,
                    &selector,
                );
                let plan =
                    catalog_backfill_plan(db.connection(), options.clone())?;
                let event_count = plan.events.len() as u64;
                for event in plan.events {
                    db.append_projected_event(event)?;
                }
                let diff = catalog_shadow_diff(db.connection(), options)?;
                summaries.push(summary_from_report(
                    surface,
                    selector.project_id.clone(),
                    Some(db.projection_last_seq(CATALOG_PROJECTION_NAME)?),
                    event_count,
                    true,
                    &diff,
                    None,
                ));
            }
            LocalDaemonIndexingSurfaceWire::Notifications => {
                let Some(sase_home) =
                    sase_home_from_projects_root(projects_root.as_deref())
                else {
                    summaries.push(empty_summary(
                        surface,
                        selector.project_id.clone(),
                        "degraded",
                        "SASE home is unavailable",
                    ));
                    continue;
                };
                let source = EventSourceWire {
                    source_type: "daemon".to_string(),
                    name: "shadow-notification-indexer".to_string(),
                    ..EventSourceWire::default()
                };
                let events = notification_backfill_event_requests(
                    &sase_home,
                    source,
                    host_id.clone(),
                )?;
                let event_count = events.len() as u64;
                for event in events {
                    db.append_projected_event(event)?;
                }
                let diff =
                    notification_shadow_diff(db.connection(), &sase_home)?;
                summaries.push(summary_from_report(
                    surface,
                    selector.project_id.clone(),
                    Some(db.projection_last_seq(NOTIFICATION_PROJECTION_NAME)?),
                    event_count,
                    true,
                    &diff,
                    None,
                ));
            }
            LocalDaemonIndexingSurfaceWire::All => {}
        }
    }
    Ok(summaries)
}

fn verify_selected_sources(
    db: &ProjectionDb,
    projects_root: Option<PathBuf>,
    selector: IndexingSelector,
) -> Result<Vec<LocalDaemonIndexingSurfaceSummaryWire>, ProjectionError> {
    let mut summaries = Vec::new();
    for report in diff_selected_sources(db, projects_root, selector.clone())? {
        let surface = surface_for_domain(&report.domain);
        summaries.push(summary_from_report(
            surface,
            selector.project_id.clone(),
            Some(db.projection_last_seq(projection_name_for_surface(surface))?),
            0,
            true,
            &report,
            None,
        ));
    }
    if summaries.is_empty() {
        for surface in selected_surfaces(selector.surface) {
            summaries.push(empty_summary_for_projection(
                db,
                surface,
                selector.project_id.clone(),
                projection_name_for_surface(surface),
            )?);
        }
    }
    Ok(summaries)
}

fn diff_selected_sources(
    db: &ProjectionDb,
    projects_root: Option<PathBuf>,
    selector: IndexingSelector,
) -> Result<Vec<ShadowDiffReportWire>, ProjectionError> {
    let mut reports = Vec::new();
    for surface in selected_surfaces(selector.surface) {
        match surface {
            LocalDaemonIndexingSurfaceWire::Changespecs => {
                if let Some(root) = projects_root.as_deref() {
                    reports.push(db.diff_changespec_projection(root)?);
                }
            }
            LocalDaemonIndexingSurfaceWire::Agents => {
                if let Some(root) = projects_root.as_deref() {
                    let scan = scan_agent_artifacts(
                        root,
                        AgentArtifactScanOptionsWire::default(),
                    );
                    for project_id in selected_project_ids_from_scan(
                        selector.project_id.as_deref(),
                        &scan,
                    ) {
                        reports.push(agent_projection_shadow_diff(
                            db.connection(),
                            &project_id,
                            &scan,
                        )?);
                    }
                }
            }
            LocalDaemonIndexingSurfaceWire::Beads => {
                if let Some(root) = projects_root.as_deref() {
                    for store in selected_bead_stores(
                        root,
                        selector.project_id.as_deref(),
                    ) {
                        reports.push(bead_projection_shadow_diff(
                            db.connection(),
                            &store.project_id,
                            &PathBuf::from(store.beads_dir),
                        )?);
                    }
                }
            }
            LocalDaemonIndexingSurfaceWire::Catalogs => {
                let options = catalog_options(
                    projects_root.as_deref(),
                    "local",
                    &selector,
                );
                reports.push(catalog_shadow_diff(db.connection(), options)?);
            }
            LocalDaemonIndexingSurfaceWire::Notifications => {
                if let Some(sase_home) =
                    sase_home_from_projects_root(projects_root.as_deref())
                {
                    reports.push(notification_shadow_diff(
                        db.connection(),
                        &sase_home,
                    )?);
                }
            }
            LocalDaemonIndexingSurfaceWire::All => {}
        }
    }
    Ok(reports)
}

fn selected_project_ids_from_scan(
    project_id: Option<&str>,
    scan: &sase_core::agent_scan::AgentArtifactScanWire,
) -> Vec<String> {
    if let Some(project_id) = project_id {
        return vec![project_id.to_string()];
    }
    let mut ids = scan
        .records
        .iter()
        .map(|record| record.project_name.clone())
        .collect::<Vec<_>>();
    ids.sort();
    ids.dedup();
    ids
}

fn selected_bead_stores(
    projects_root: &Path,
    project_id: Option<&str>,
) -> Vec<sase_core::projections::BeadStoreSourceWire> {
    let mut stores = Vec::new();
    if let Some(project_id) = project_id {
        stores.extend(discover_bead_stores(
            &projects_root.join(project_id),
            Some(project_id),
        ));
        return stores;
    }
    let Ok(entries) = std::fs::read_dir(projects_root) else {
        return stores;
    };
    for entry in entries.flatten() {
        if !entry.file_type().map(|kind| kind.is_dir()).unwrap_or(false) {
            continue;
        }
        let project_id = entry.file_name().to_string_lossy().to_string();
        stores.extend(discover_bead_stores(&entry.path(), Some(&project_id)));
    }
    stores
}

fn catalog_options(
    projects_root: Option<&Path>,
    host_id: &str,
    selector: &IndexingSelector,
) -> CatalogBackfillOptionsWire {
    let root_dir = match (projects_root, selector.project_id.as_deref()) {
        (Some(root), Some(project_id)) => Some(root.join(project_id)),
        (Some(root), None) => Some(root.to_path_buf()),
        (None, _) => None,
    };
    CatalogBackfillOptionsWire {
        schema_version: INDEXING_WIRE_SCHEMA_VERSION,
        project_id: selector
            .project_id
            .clone()
            .unwrap_or_else(|| "all".to_string()),
        project: selector.project_id.clone(),
        root_dir: root_dir.map(|path| path.to_string_lossy().to_string()),
        host_id: host_id.to_string(),
        created_at: None,
    }
}

fn sase_home_from_projects_root(
    projects_root: Option<&Path>,
) -> Option<PathBuf> {
    projects_root
        .and_then(|root| root.parent())
        .map(Path::to_path_buf)
}

fn summary_from_report(
    surface: LocalDaemonIndexingSurfaceWire,
    project_id: Option<String>,
    last_indexed_event_seq: Option<i64>,
    queued_changes: u64,
    watcher_active: bool,
    report: &ShadowDiffReportWire,
    message: Option<String>,
) -> LocalDaemonIndexingSurfaceSummaryWire {
    let state = if report.counts.corrupt > 0
        || report.counts.missing > 0
        || report.counts.stale > 0
        || report.counts.extra > 0
    {
        "degraded"
    } else {
        "ok"
    };
    LocalDaemonIndexingSurfaceSummaryWire {
        schema_version: LOCAL_DAEMON_WIRE_SCHEMA_VERSION,
        surface,
        project_id,
        state: state.to_string(),
        last_indexed_event_seq,
        queued_changes,
        watcher_active,
        diff_counts: report.counts.clone(),
        message,
    }
}

fn empty_summary_for_projection(
    db: &ProjectionDb,
    surface: LocalDaemonIndexingSurfaceWire,
    project_id: Option<String>,
    projection_name: &str,
) -> Result<LocalDaemonIndexingSurfaceSummaryWire, ProjectionError> {
    Ok(LocalDaemonIndexingSurfaceSummaryWire {
        schema_version: LOCAL_DAEMON_WIRE_SCHEMA_VERSION,
        surface,
        project_id,
        state: "ok".to_string(),
        last_indexed_event_seq: Some(db.projection_last_seq(projection_name)?),
        queued_changes: 0,
        watcher_active: true,
        diff_counts: ShadowDiffCountsWire::default(),
        message: None,
    })
}

fn empty_summary(
    surface: LocalDaemonIndexingSurfaceWire,
    project_id: Option<String>,
    state: &str,
    message: &str,
) -> LocalDaemonIndexingSurfaceSummaryWire {
    LocalDaemonIndexingSurfaceSummaryWire {
        schema_version: LOCAL_DAEMON_WIRE_SCHEMA_VERSION,
        surface,
        project_id,
        state: state.to_string(),
        last_indexed_event_seq: None,
        queued_changes: 0,
        watcher_active: false,
        diff_counts: ShadowDiffCountsWire::default(),
        message: Some(message.to_string()),
    }
}

fn projection_name_for_surface(
    surface: LocalDaemonIndexingSurfaceWire,
) -> &'static str {
    match surface {
        LocalDaemonIndexingSurfaceWire::Changespecs => {
            CHANGESPEC_PROJECTION_NAME
        }
        LocalDaemonIndexingSurfaceWire::Notifications => {
            NOTIFICATION_PROJECTION_NAME
        }
        LocalDaemonIndexingSurfaceWire::Agents => AGENT_PROJECTION_NAME,
        LocalDaemonIndexingSurfaceWire::Beads => BEAD_PROJECTION_NAME,
        LocalDaemonIndexingSurfaceWire::Catalogs => CATALOG_PROJECTION_NAME,
        LocalDaemonIndexingSurfaceWire::All => CHANGESPEC_PROJECTION_NAME,
    }
}

fn surface_for_domain(domain: &str) -> LocalDaemonIndexingSurfaceWire {
    match domain {
        CHANGESPEC_PROJECTION_NAME => {
            LocalDaemonIndexingSurfaceWire::Changespecs
        }
        NOTIFICATION_PROJECTION_NAME => {
            LocalDaemonIndexingSurfaceWire::Notifications
        }
        AGENT_PROJECTION_NAME => LocalDaemonIndexingSurfaceWire::Agents,
        BEAD_INDEXING_DOMAIN => LocalDaemonIndexingSurfaceWire::Beads,
        CATALOG_PROJECTION_NAME => LocalDaemonIndexingSurfaceWire::Catalogs,
        _ => LocalDaemonIndexingSurfaceWire::All,
    }
}

impl SourceWatcherFactory for NotifySourceWatcherFactory {
    fn start(
        &self,
        roots: &[PathBuf],
        sender: mpsc::Sender<SourceChangeWire>,
    ) -> Result<Box<dyn SourceWatcher>, IndexingError> {
        let mut watcher = notify::recommended_watcher(move |result| {
            if let Ok(event) = result {
                for change in changes_from_notify_event(&event) {
                    let _ = sender.try_send(change);
                }
            }
        })
        .map_err(|error| IndexingError::WatcherInit(error.to_string()))?;

        for root in roots {
            if !root.exists() {
                continue;
            }
            watcher.watch(root, RecursiveMode::Recursive).map_err(
                |source| IndexingError::WatchPath {
                    path: root.clone(),
                    source,
                },
            )?;
        }

        Ok(Box::new(NotifySourceWatcher { _watcher: watcher }))
    }
}

fn changes_from_notify_event(event: &notify::Event) -> Vec<SourceChangeWire> {
    let operation = operation_from_event_kind(&event.kind);
    event
        .paths
        .iter()
        .map(|path| SourceChangeWire {
            schema_version: INDEXING_WIRE_SCHEMA_VERSION,
            identity: source_identity_from_path(
                source_domain_from_path(path),
                path,
            ),
            operation: operation.clone(),
            reason: Some(format!("{:?}", event.kind)),
        })
        .map(|fallback| {
            let path = Path::new(&fallback.identity.source_path);
            changespec_source_change_from_path(
                path,
                operation.clone(),
                fallback.reason.clone(),
            )
            .unwrap_or(fallback)
        })
        .map(|fallback| {
            agent_source_change_from_path(
                None,
                Path::new(&fallback.identity.source_path),
                operation.clone(),
                fallback.reason.clone(),
            )
            .unwrap_or(fallback)
        })
        .collect()
}

fn operation_from_event_kind(kind: &EventKind) -> SourceChangeOperationWire {
    match kind {
        EventKind::Create(_)
        | EventKind::Modify(ModifyKind::Data(_))
        | EventKind::Modify(ModifyKind::Metadata(_)) => {
            SourceChangeOperationWire::Upsert
        }
        EventKind::Modify(ModifyKind::Name(_)) => {
            SourceChangeOperationWire::Rewrite
        }
        EventKind::Remove(_) => SourceChangeOperationWire::Delete,
        _ => SourceChangeOperationWire::Reconcile,
    }
}

fn source_identity_from_path(
    domain: impl Into<String>,
    path: &Path,
) -> SourceIdentityWire {
    let domain = domain.into();
    let is_bead = domain == BEAD_INDEXING_DOMAIN;
    SourceIdentityWire {
        schema_version: INDEXING_WIRE_SCHEMA_VERSION,
        project_id: is_bead.then(|| bead_project_id_for_path(path)).flatten(),
        domain,
        source_path: path.to_string_lossy().to_string(),
        is_archive: false,
        fingerprint: source_fingerprint_from_path(path, is_bead),
        last_indexed_event_seq: None,
    }
}

fn source_domain_from_path(path: &Path) -> &'static str {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("");
    let normalized = path.to_string_lossy();
    if file_name == "workflow_state.json"
        || file_name.starts_with("prompt_step_")
    {
        return "workflows";
    }
    if file_name == "file_reference_history.json" {
        return "file_history";
    }
    if file_name == "agent_artifact_index.sqlite" {
        return "artifact_index";
    }
    if normalized.ends_with("/notifications/notifications.jsonl")
        || normalized.ends_with("/pending_actions/actions.json")
        || normalized.ends_with("/telegram/pending_actions.json")
    {
        return NOTIFICATION_PROJECTION_NAME;
    }
    if is_bead_indexed_file(path) {
        return BEAD_INDEXING_DOMAIN;
    }
    if file_name == "sase.yml"
        || file_name == "sase.yaml"
        || file_name.starts_with("sase_")
        || normalized.contains("/xprompts/")
        || normalized.contains("/.xprompts/")
        || normalized.contains("/memory/long/")
        || matches!(
            path.extension().and_then(|ext| ext.to_str()),
            Some("md" | "yml" | "yaml")
        )
    {
        return "catalogs";
    }
    "filesystem"
}

fn is_bead_indexed_file(path: &Path) -> bool {
    let Some(file_name) = path.file_name().and_then(|name| name.to_str())
    else {
        return false;
    };
    matches!(
        file_name,
        "issues.jsonl"
            | "config.json"
            | "beads.db"
            | "beads.db-wal"
            | "beads.db-shm"
    ) && bead_store_dir_for_path(path).is_some()
}

fn bead_project_id_for_path(path: &Path) -> Option<String> {
    let components: Vec<String> = path
        .components()
        .filter_map(|component| {
            component.as_os_str().to_str().map(str::to_string)
        })
        .collect();
    for (idx, pair) in components.windows(2).enumerate() {
        if pair[0] != "sdd" || pair[1] != "beads" {
            continue;
        }
        if idx >= 2
            && components.get(idx - 1).map(String::as_str) == Some(".sase")
        {
            return components.get(idx - 2).cloned();
        }
        return components.get(idx - 1).cloned();
    }
    None
}

fn spawn_changespec_backfill(
    projects_root: Option<PathBuf>,
    host_id: String,
    projection_service: ProjectionService,
    status: Arc<RwLock<IndexingServiceStatusWire>>,
    metrics: DaemonMetrics,
    local_events: LocalEventHub,
) {
    let Some(projects_root) = projects_root else {
        return;
    };
    if !projects_root.exists() {
        return;
    }

    tokio::spawn(async move {
        let source = EventSourceWire {
            source_type: "daemon".to_string(),
            name: "shadow-changespec-indexer".to_string(),
            ..EventSourceWire::default()
        };
        let result = projection_service
            .write({
                let projects_root = projects_root.clone();
                move |db| {
                    let outcomes = db.backfill_changespec_sources(
                        &projects_root,
                        source,
                        host_id,
                    )?;
                    let diff = db.diff_changespec_projection(&projects_root)?;
                    Ok((outcomes, diff.counts))
                }
            })
            .await;

        match result {
            Ok((outcomes, diff_counts)) => {
                let source_paths = outcomes
                    .iter()
                    .filter_map(|outcome| outcome.event.source_path.clone())
                    .collect::<Vec<_>>();
                let indexed_sources = outcomes
                    .iter()
                    .filter(|outcome| !outcome.duplicate)
                    .count() as u64;
                let report = IndexingDomainReportWire {
                    schema_version: INDEXING_WIRE_SCHEMA_VERSION,
                    domain: "changespec".to_string(),
                    queued_changes: outcomes.len() as u64,
                    coalesced_changes: 0,
                    indexed_sources,
                    failed_parses: diff_counts.corrupt,
                    shadow_diff_counts: diff_counts,
                    source_paths,
                };
                metrics
                    .record_indexing_report(indexing_metrics_report(&report));
                apply_report_to_status(&status, report.clone());
                local_events.append_delta(
                    LocalDaemonCollectionWire::Indexing,
                    "indexing:changespec:backfill",
                    LocalDaemonDeltaOperationWire::Upsert,
                    serde_json::to_value(report).unwrap_or_else(
                        |error| json!({"error": error.to_string()}),
                    ),
                );
            }
            Err(error) => {
                mark_degraded(
                    &status,
                    format!("changespec indexing backfill failed: {error}"),
                );
                local_events
                    .append_resync_required("changespec_backfill_failed");
            }
        }
    });
}

#[allow(clippy::too_many_arguments)]
fn spawn_reconciliation_loop(
    sender: mpsc::Sender<SourceChangeWire>,
    status: Arc<RwLock<IndexingServiceStatusWire>>,
    known_sources: Arc<RwLock<BTreeMap<String, SourceIdentityWire>>>,
    metrics: DaemonMetrics,
    local_events: LocalEventHub,
    shutdown: DaemonShutdown,
    projects_root: Option<PathBuf>,
    interval: Duration,
    max_changes: usize,
) {
    let Some(projects_root) = projects_root else {
        return;
    };
    if interval.is_zero() {
        return;
    }

    tokio::spawn(async move {
        loop {
            tokio::time::sleep(interval).await;
            if shutdown.is_requested() {
                return;
            }

            let root = projects_root.clone();
            let discovered = tokio::task::spawn_blocking(move || {
                discover_reconciliation_sources(&root, max_changes)
            })
            .await;

            let (discovered, scan_bounded) = match discovered {
                Ok(value) => value,
                Err(error) => {
                    mark_resync_required(
                        &status,
                        format!("indexing reconciliation scan failed: {error}"),
                    );
                    local_events.append_resync_required(
                        "indexing_reconciliation_failed",
                    );
                    continue;
                }
            };

            let known = known_sources
                .read()
                .map(|guard| guard.values().cloned().collect::<Vec<_>>())
                .unwrap_or_default();
            let plan = projection_reconciliation_plan(
                known,
                discovered,
                Some(max_changes),
            );
            publish_reconciliation_diagnostics(
                &status,
                &local_events,
                &plan,
                scan_bounded,
            );
            enqueue_reconciliation_plan(
                &sender,
                &status,
                &metrics,
                &known_sources,
                plan,
            );
        }
    });
}

fn discover_reconciliation_sources(
    projects_root: &Path,
    max_sources: usize,
) -> (Vec<SourceIdentityWire>, bool) {
    let mut sources = BTreeMap::new();
    let sase_home = sase_home_from_projects_root(Some(projects_root));
    let mut stack = vec![projects_root.to_path_buf()];
    if let Some(home) = sase_home.as_ref() {
        stack.push(home.join("notifications"));
        stack.push(home.join("pending_actions"));
        stack.push(home.join("telegram"));
    }
    let mut bounded = false;

    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        let mut entries = entries.flatten().collect::<Vec<_>>();
        entries.sort_by_key(|entry| entry.path());
        for entry in entries {
            let path = entry.path();
            let Ok(file_type) = entry.file_type() else {
                continue;
            };
            if file_type.is_dir() {
                stack.push(path);
                continue;
            }
            if !file_type.is_file() {
                continue;
            }
            let Some(change) = source_change_from_path(
                sase_home.as_deref(),
                &path,
                SourceChangeOperationWire::Reconcile,
                Some("periodic reconciliation scan".to_string()),
            ) else {
                continue;
            };
            if !is_reconciled_domain(&change.identity.domain) {
                continue;
            }
            sources.insert(change.identity.stable_key(), change.identity);
            if sources.len() >= max_sources {
                bounded = true;
                break;
            }
        }
        if bounded {
            break;
        }
    }

    (sources.into_values().collect(), bounded)
}

fn source_change_from_path(
    sase_home: Option<&Path>,
    path: &Path,
    operation: SourceChangeOperationWire,
    reason: Option<String>,
) -> Option<SourceChangeWire> {
    let fallback = SourceChangeWire {
        schema_version: INDEXING_WIRE_SCHEMA_VERSION,
        identity: source_identity_from_path(
            source_domain_from_path(path),
            path,
        ),
        operation: operation.clone(),
        reason: reason.clone(),
    };
    let change = changespec_source_change_from_path(
        path,
        operation.clone(),
        reason.clone(),
    )
    .unwrap_or(fallback);
    let change = agent_source_change_from_path(
        None,
        path,
        operation.clone(),
        reason.clone(),
    )
    .unwrap_or(change);
    if let Some(sase_home) = sase_home {
        return Some(
            notification_source_change_from_path(
                sase_home, path, operation, reason,
            )
            .unwrap_or(change),
        );
    }
    Some(change)
}

fn is_reconciled_domain(domain: &str) -> bool {
    matches!(
        domain,
        CHANGESPEC_PROJECTION_NAME
            | AGENT_PROJECTION_NAME
            | BEAD_INDEXING_DOMAIN
            | NOTIFICATION_PROJECTION_NAME
            | CATALOG_PROJECTION_NAME
    )
}

fn publish_reconciliation_diagnostics(
    status: &RwLock<IndexingServiceStatusWire>,
    local_events: &LocalEventHub,
    plan: &ProjectionReconciliationPlanWire,
    scan_bounded: bool,
) {
    if scan_bounded || plan.bounded {
        mark_resync_required(
            status,
            "indexing reconciliation exceeded its bounded scan or change limit",
        );
        local_events.append_resync_required("indexing_reconciliation_bounded");
    }
    for diagnostic in &plan.diagnostics {
        local_events.append_delta(
            LocalDaemonCollectionWire::Indexing,
            format!("indexing:reconciliation:{}", diagnostic.reason),
            LocalDaemonDeltaOperationWire::Upsert,
            serde_json::to_value(diagnostic)
                .unwrap_or_else(|error| json!({"error": error.to_string()})),
        );
    }
}

fn enqueue_reconciliation_plan(
    sender: &mpsc::Sender<SourceChangeWire>,
    status: &RwLock<IndexingServiceStatusWire>,
    metrics: &DaemonMetrics,
    known_sources: &RwLock<BTreeMap<String, SourceIdentityWire>>,
    plan: ProjectionReconciliationPlanWire,
) {
    for change in plan.changes {
        match sender.try_send(change.clone()) {
            Ok(()) => apply_known_source_changes(known_sources, &[change]),
            Err(_) => {
                metrics.record_indexing_dropped_change();
                increment_dropped(status);
                break;
            }
        }
    }
}

fn indexing_metrics_report(
    report: &IndexingDomainReportWire,
) -> IndexingMetricsReport {
    IndexingMetricsReport {
        indexed_sources: report.indexed_sources,
        failed_parses: report.failed_parses,
        coalesced_changes: report.coalesced_changes,
        missing: report.shadow_diff_counts.missing,
        stale: report.shadow_diff_counts.stale,
        extra: report.shadow_diff_counts.extra,
        corrupt: report.shadow_diff_counts.corrupt,
    }
}

struct WorkerLoopArgs {
    receiver: mpsc::Receiver<SourceChangeWire>,
    status: Arc<RwLock<IndexingServiceStatusWire>>,
    known_sources: Arc<RwLock<BTreeMap<String, SourceIdentityWire>>>,
    metrics: DaemonMetrics,
    local_events: LocalEventHub,
    shutdown: DaemonShutdown,
    debounce: Duration,
    worker_capacity: usize,
    max_batch_size: usize,
    projects_root: Option<PathBuf>,
    host_id: String,
    projection_service: ProjectionService,
}

async fn worker_loop(args: WorkerLoopArgs) {
    let WorkerLoopArgs {
        mut receiver,
        status,
        known_sources,
        metrics,
        local_events,
        shutdown,
        debounce,
        worker_capacity,
        max_batch_size,
        projects_root,
        host_id,
        projection_service,
    } = args;
    let semaphore = Arc::new(Semaphore::new(worker_capacity));
    loop {
        if shutdown.is_requested() {
            mark_stopped(&status);
            return;
        }

        let Some(change) = receiver.recv().await else {
            mark_stopped(&status);
            return;
        };

        metrics.record_indexing_queued_change();
        increment_queued(&status);
        let mut received_changes = 1_u64;
        let mut pending =
            BTreeMap::from([(change.identity.stable_key(), change)]);
        loop {
            if pending.len() >= max_batch_size {
                break;
            }
            tokio::select! {
                maybe_change = receiver.recv() => {
                    let Some(change) = maybe_change else {
                        break;
                    };
                    metrics.record_indexing_queued_change();
                    increment_queued(&status);
                    received_changes += 1;
                    pending.insert(change.identity.stable_key(), change);
                }
                _ = tokio::time::sleep(debounce) => break,
            }
        }

        let queued_changes = received_changes;
        let changes: Vec<SourceChangeWire> = pending.into_values().collect();
        apply_known_source_changes(&known_sources, &changes);
        let Ok(permit) = semaphore.clone().acquire_owned().await else {
            mark_degraded(&status, "indexing worker semaphore closed");
            return;
        };
        let task_projects_root = projects_root.clone();
        let task_host_id = host_id.clone();
        let output = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            build_indexing_output_with_context(
                changes,
                queued_changes,
                task_projects_root.as_deref(),
                &task_host_id,
            )
        })
        .await;

        match output {
            Ok(output) => {
                for request in output.event_requests {
                    if let Err(error) =
                        projection_service.append_projected_event(request).await
                    {
                        let message = format!(
                            "indexing projection append failed: {error}"
                        );
                        mark_degraded(&status, message);
                        local_events
                            .append_resync_required("indexing_append_failed");
                    }
                }
                for report in output.reports {
                    metrics.record_indexing_report(indexing_metrics_report(
                        &report,
                    ));
                    apply_report_to_status(&status, report.clone());
                    local_events.append_delta(
                        LocalDaemonCollectionWire::Indexing,
                        format!("indexing:{}", report.domain),
                        LocalDaemonDeltaOperationWire::Upsert,
                        serde_json::to_value(report).unwrap_or_else(
                            |error| json!({"error": error.to_string()}),
                        ),
                    );
                }
            }
            Err(error) => {
                let message = format!("indexing worker task failed: {error}");
                mark_degraded(&status, message);
                local_events.append_resync_required("indexing_worker_failed");
            }
        }
    }
}

#[derive(Debug, Default)]
struct IndexingBuildOutput {
    reports: Vec<IndexingDomainReportWire>,
    event_requests: Vec<EventAppendRequestWire>,
}

fn build_indexing_output_with_context(
    changes: Vec<SourceChangeWire>,
    queued_changes: u64,
    projects_root: Option<&Path>,
    host_id: &str,
) -> IndexingBuildOutput {
    let mut event_requests =
        changespec_event_requests_for_changes(&changes, host_id);
    event_requests.extend(agent_event_requests_for_changes(
        &changes,
        projects_root,
        host_id,
    ));
    event_requests.extend(bead_event_requests_for_changes(&changes, host_id));
    event_requests.extend(notification_event_requests_for_changes(
        &changes,
        projects_root.and_then(|root| root.parent()),
        host_id,
    ));
    event_requests.extend(catalog_event_requests_for_changes(
        &changes,
        projects_root,
        host_id,
    ));
    let mut by_domain: BTreeMap<String, Vec<SourceChangeWire>> =
        BTreeMap::new();
    for change in changes {
        by_domain
            .entry(change.identity.domain.clone())
            .or_default()
            .push(change);
    }

    let reports = by_domain
        .into_iter()
        .map(|(domain, changes)| {
            let source_paths = changes
                .iter()
                .map(|c| c.identity.source_path.clone())
                .collect();
            let indexed_sources = changes
                .iter()
                .filter(|change| {
                    !matches!(
                        change.operation,
                        SourceChangeOperationWire::Delete
                    )
                })
                .count() as u64;
            IndexingDomainReportWire {
                schema_version: INDEXING_WIRE_SCHEMA_VERSION,
                domain,
                queued_changes,
                coalesced_changes: queued_changes
                    .saturating_sub(changes.len() as u64),
                indexed_sources,
                failed_parses: 0,
                shadow_diff_counts: ShadowDiffCountsWire::default(),
                source_paths,
            }
        })
        .collect();
    IndexingBuildOutput {
        reports,
        event_requests,
    }
}

fn changespec_event_requests_for_changes(
    changes: &[SourceChangeWire],
    host_id: &str,
) -> Vec<EventAppendRequestWire> {
    changes
        .iter()
        .filter(|change| change.identity.domain == "changespec")
        .filter_map(|change| {
            changespec_event_request_for_source_change(
                change,
                EventSourceWire {
                    source_type: "daemon".to_string(),
                    name: "shadow-changespec-indexer".to_string(),
                    ..EventSourceWire::default()
                },
                host_id,
            )
            .ok()
            .flatten()
        })
        .collect()
}

fn bead_event_requests_for_changes(
    changes: &[SourceChangeWire],
    host_id: &str,
) -> Vec<EventAppendRequestWire> {
    changes
        .iter()
        .filter(|change| change.identity.domain == BEAD_INDEXING_DOMAIN)
        .filter(|change| {
            !matches!(change.operation, SourceChangeOperationWire::Delete)
        })
        .filter_map(|change| {
            let source_path = PathBuf::from(&change.identity.source_path);
            let beads_dir = bead_store_dir_for_path(&source_path)?;
            let context = BeadProjectionEventContextWire {
                created_at: None,
                source: EventSourceWire {
                    source_type: "daemon".to_string(),
                    name: "shadow-bead-indexer".to_string(),
                    ..EventSourceWire::default()
                },
                host_id: host_id.to_string(),
                project_id: change
                    .identity
                    .project_id
                    .clone()
                    .unwrap_or_else(|| "unknown".to_string()),
                idempotency_key: None,
                causality: Vec::new(),
                source_path: Some(change.identity.source_path.clone()),
                source_revision: None,
            };
            bead_backfill_snapshot_event_request(context, &beads_dir).ok()
        })
        .collect()
}

fn notification_event_requests_for_changes(
    changes: &[SourceChangeWire],
    sase_home: Option<&Path>,
    host_id: &str,
) -> Vec<EventAppendRequestWire> {
    let Some(sase_home) = sase_home else {
        return Vec::new();
    };
    changes
        .iter()
        .filter(|change| change.identity.domain == NOTIFICATION_PROJECTION_NAME)
        .flat_map(|change| {
            notification_event_requests_for_source_change(
                sase_home,
                change,
                EventSourceWire {
                    source_type: "daemon".to_string(),
                    name: "shadow-notification-indexer".to_string(),
                    ..EventSourceWire::default()
                },
                host_id,
            )
            .unwrap_or_default()
        })
        .collect()
}

fn catalog_event_requests_for_changes(
    changes: &[SourceChangeWire],
    projects_root: Option<&Path>,
    host_id: &str,
) -> Vec<EventAppendRequestWire> {
    if !changes
        .iter()
        .any(|change| change.identity.domain == CATALOG_PROJECTION_NAME)
    {
        return Vec::new();
    }
    let selector = IndexingSelector {
        surface: LocalDaemonIndexingSurfaceWire::Catalogs,
        project_id: None,
    };
    let options = catalog_options(projects_root, host_id, &selector);
    let Ok(db) = ProjectionDb::open_in_memory() else {
        return Vec::new();
    };
    catalog_backfill_plan(db.connection(), options)
        .map(|plan| plan.events)
        .unwrap_or_default()
}

fn agent_event_requests_for_changes(
    changes: &[SourceChangeWire],
    projects_root: Option<&Path>,
    host_id: &str,
) -> Vec<EventAppendRequestWire> {
    let Some(projects_root) = projects_root else {
        return Vec::new();
    };
    changes
        .iter()
        .filter(|change| change.identity.domain == "agents")
        .filter_map(|change| {
            let artifact_dir = PathBuf::from(&change.identity.source_path);
            let context = AgentProjectionEventContextWire {
                schema_version: INDEXING_WIRE_SCHEMA_VERSION,
                created_at: None,
                source: EventSourceWire {
                    source_type: "daemon".to_string(),
                    name: "shadow-agent-indexer".to_string(),
                    ..EventSourceWire::default()
                },
                host_id: host_id.to_string(),
                project_id: change
                    .identity
                    .project_id
                    .clone()
                    .unwrap_or_else(|| "unknown".to_string()),
                idempotency_key: None,
                causality: Vec::new(),
                source_path: Some(change.identity.source_path.clone()),
                source_revision: None,
            };
            agent_projection_event_request_for_artifact_dir(
                context,
                projects_root,
                &artifact_dir,
                &sase_core::agent_scan::AgentArtifactScanOptionsWire::default(),
            )
            .ok()
            .flatten()
        })
        .collect()
}

fn bead_store_dir_for_path(path: &Path) -> Option<PathBuf> {
    let mut current = if path.is_dir() {
        path.to_path_buf()
    } else {
        path.parent()?.to_path_buf()
    };
    loop {
        if current.file_name().and_then(|name| name.to_str()) == Some("beads")
            && current
                .parent()
                .and_then(|parent| parent.file_name())
                .and_then(|name| name.to_str())
                == Some("sdd")
        {
            return Some(current);
        }
        current = current.parent()?.to_path_buf();
    }
}

fn apply_report_to_status(
    status: &RwLock<IndexingServiceStatusWire>,
    report: IndexingDomainReportWire,
) {
    if let Ok(mut guard) = status.write() {
        guard.indexed_sources += report.indexed_sources;
        guard.failed_parses += report.failed_parses;
        guard.coalesced_changes += report.coalesced_changes;
        guard.diff_counts.missing += report.shadow_diff_counts.missing;
        guard.diff_counts.stale += report.shadow_diff_counts.stale;
        guard.diff_counts.extra += report.shadow_diff_counts.extra;
        guard.diff_counts.corrupt += report.shadow_diff_counts.corrupt;
        guard.recent_reports.push(report);
        while guard.recent_reports.len() > RECENT_REPORT_LIMIT {
            guard.recent_reports.remove(0);
        }
    }
}

fn apply_known_source_changes(
    known_sources: &RwLock<BTreeMap<String, SourceIdentityWire>>,
    changes: &[SourceChangeWire],
) {
    let Ok(mut guard) = known_sources.write() else {
        return;
    };
    for change in changes {
        let key = change.identity.stable_key();
        if matches!(change.operation, SourceChangeOperationWire::Delete) {
            guard.remove(&key);
        } else {
            guard.insert(key, change.identity.clone());
        }
    }
}

fn increment_queued(status: &RwLock<IndexingServiceStatusWire>) {
    if let Ok(mut guard) = status.write() {
        guard.queued_changes += 1;
    }
}

fn increment_dropped(status: &RwLock<IndexingServiceStatusWire>) {
    if let Ok(mut guard) = status.write() {
        guard.dropped_changes += 1;
        guard.state = IndexingServiceStateWire::Degraded;
        guard.message = Some("indexing queue is full".to_string());
    }
}

fn mark_degraded(
    status: &RwLock<IndexingServiceStatusWire>,
    message: impl Into<String>,
) {
    if let Ok(mut guard) = status.write() {
        guard.state = IndexingServiceStateWire::Degraded;
        guard.enabled = false;
        guard.watcher_active = false;
        guard.message = Some(message.into());
    }
}

fn mark_resync_required(
    status: &RwLock<IndexingServiceStatusWire>,
    message: impl Into<String>,
) {
    if let Ok(mut guard) = status.write() {
        guard.state = IndexingServiceStateWire::Degraded;
        guard.message = Some(message.into());
    }
}

fn mark_stopped(status: &RwLock<IndexingServiceStatusWire>) {
    if let Ok(mut guard) = status.write() {
        guard.state = IndexingServiceStateWire::Stopped;
        guard.watcher_active = false;
    }
}

impl LocalEventHub {
    pub(crate) fn append_delta(
        &self,
        collection: LocalDaemonCollectionWire,
        handle: impl Into<String>,
        operation: LocalDaemonDeltaOperationWire,
        fields: JsonValue,
    ) {
        let handle = handle.into();
        self.append(|_| LocalDaemonEventPayloadWire::Delta {
            collection,
            handle,
            operation,
            fields,
        });
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, fs, time::Duration};

    use sase_core::projections::SourceFingerprintWire;
    use tempfile::tempdir;

    use super::*;

    #[derive(Clone, Debug)]
    struct SyntheticWatcherFactory;

    struct SyntheticWatcher;

    impl SourceWatcher for SyntheticWatcher {}

    impl SourceWatcherFactory for SyntheticWatcherFactory {
        fn start(
            &self,
            _roots: &[PathBuf],
            _sender: mpsc::Sender<SourceChangeWire>,
        ) -> Result<Box<dyn SourceWatcher>, IndexingError> {
            Ok(Box::new(SyntheticWatcher))
        }
    }

    #[derive(Clone, Debug)]
    struct FailingWatcherFactory;

    impl SourceWatcherFactory for FailingWatcherFactory {
        fn start(
            &self,
            _roots: &[PathBuf],
            _sender: mpsc::Sender<SourceChangeWire>,
        ) -> Result<Box<dyn SourceWatcher>, IndexingError> {
            Err(IndexingError::WatcherInit("synthetic failure".to_string()))
        }
    }

    #[tokio::test]
    async fn synthetic_change_is_debounced_and_reported() {
        let metrics = DaemonMetrics::default();
        let events = LocalEventHub::new(8, metrics.clone());
        let service = IndexingService::start_with_factory(
            IndexingConfig {
                watch_roots: Vec::new(),
                projects_root: None,
                host_id: "local".to_string(),
                queue_capacity: 4,
                worker_capacity: 1,
                debounce: Duration::from_millis(5),
                reconciliation_interval: Duration::from_secs(0),
                max_reconciliation_changes: 16,
                max_batch_size: 16,
                enabled: true,
            },
            DaemonShutdown::default(),
            metrics,
            events.clone(),
            ProjectionService::unavailable(
                PathBuf::from(":memory:"),
                "synthetic test",
            ),
            SyntheticWatcherFactory,
        );

        assert!(service.enqueue(change("changespec", "/tmp/project.sase")));
        tokio::time::sleep(Duration::from_millis(50)).await;

        let status = service.status();
        assert_eq!(status.state, IndexingServiceStateWire::Ok);
        assert_eq!(status.queued_changes, 1);
        assert_eq!(status.indexed_sources, 1);
        assert_eq!(status.recent_reports[0].domain, "changespec");
        assert!(events
            .replay_after("0000000000000000", &[], 8)
            .unwrap()
            .iter()
            .any(|event| matches!(
                event.payload,
                LocalDaemonEventPayloadWire::Delta {
                    collection: LocalDaemonCollectionWire::Indexing,
                    ..
                }
            )));
    }

    #[tokio::test]
    async fn repeated_source_writes_are_coalesced_within_debounce_window() {
        let metrics = DaemonMetrics::default();
        let events = LocalEventHub::new(8, metrics.clone());
        let service = IndexingService::start_with_factory(
            IndexingConfig {
                watch_roots: Vec::new(),
                projects_root: None,
                host_id: "local".to_string(),
                queue_capacity: 8,
                worker_capacity: 1,
                debounce: Duration::from_millis(10),
                reconciliation_interval: Duration::from_secs(0),
                max_reconciliation_changes: 16,
                max_batch_size: 16,
                enabled: true,
            },
            DaemonShutdown::default(),
            metrics,
            events,
            ProjectionService::unavailable(
                PathBuf::from(":memory:"),
                "synthetic test",
            ),
            SyntheticWatcherFactory,
        );

        assert!(service.enqueue(change("catalogs", "/tmp/source.md")));
        assert!(service.enqueue(change("catalogs", "/tmp/source.md")));
        assert!(service.enqueue(change("catalogs", "/tmp/source.md")));
        let status = wait_for_status(&service, |status| {
            status.indexed_sources >= 1 && status.coalesced_changes >= 2
        })
        .await;

        assert_eq!(status.queued_changes, 3);
        assert_eq!(status.coalesced_changes, 2);
        assert_eq!(status.indexed_sources, 1);
    }

    #[tokio::test]
    async fn reconciliation_requeues_sources_missed_by_watcher() {
        let dir = tempdir().unwrap();
        let projects_root = dir.path().join("projects");
        let project_dir = projects_root.join("project-a");
        fs::create_dir_all(&project_dir).unwrap();
        let catalog_path = project_dir.join("xprompts").join("helper.md");
        fs::create_dir_all(catalog_path.parent().unwrap()).unwrap();
        fs::write(&catalog_path, "# Helper\n").unwrap();

        let metrics = DaemonMetrics::default();
        let events = LocalEventHub::new(16, metrics.clone());
        let projection_service =
            ProjectionService::initialize(dir.path().join("projection.sqlite"))
                .await;
        let service = IndexingService::start_with_factory(
            IndexingConfig {
                watch_roots: Vec::new(),
                projects_root: Some(projects_root),
                host_id: "local".to_string(),
                queue_capacity: 8,
                worker_capacity: 1,
                debounce: Duration::from_millis(5),
                reconciliation_interval: Duration::from_millis(5),
                max_reconciliation_changes: 16,
                max_batch_size: 16,
                enabled: true,
            },
            DaemonShutdown::default(),
            metrics,
            events,
            projection_service,
            SyntheticWatcherFactory,
        );

        let status = wait_for_status(&service, |status| {
            status.recent_reports.iter().any(|report| {
                report.domain == "catalogs"
                    && report.source_paths.iter().any(|path| {
                        path.ends_with("project-a/xprompts/helper.md")
                    })
            })
        })
        .await;
        assert!(status.recent_reports.iter().any(|report| {
            report.domain == "catalogs"
                && report
                    .source_paths
                    .iter()
                    .any(|path| path.ends_with("project-a/xprompts/helper.md"))
        }));
    }

    #[tokio::test]
    async fn watcher_failure_degrades_indexing_without_disabling_daemon() {
        let metrics = DaemonMetrics::default();
        let events = LocalEventHub::new(8, metrics.clone());
        let service = IndexingService::start_with_factory(
            IndexingConfig::default(),
            DaemonShutdown::default(),
            metrics,
            events.clone(),
            ProjectionService::unavailable(
                PathBuf::from(":memory:"),
                "synthetic test",
            ),
            FailingWatcherFactory,
        );

        let status = service.status();
        assert_eq!(status.state, IndexingServiceStateWire::Degraded);
        assert!(!status.enabled);
        assert!(status.message.unwrap().contains("synthetic failure"));
        assert!(events
            .replay_after("0000000000000000", &[], 8)
            .unwrap()
            .iter()
            .any(|event| matches!(
                event.payload,
                LocalDaemonEventPayloadWire::ResyncRequired { .. }
            )));
    }

    #[test]
    fn rebuild_notifications_uses_real_source_diff() {
        let dir = tempdir().unwrap();
        let projects_root = dir.path().join("projects");
        fs::create_dir_all(&projects_root).unwrap();
        let notification_path =
            dir.path().join("notifications").join("notifications.jsonl");
        fs::create_dir_all(notification_path.parent().unwrap()).unwrap();
        let notification = sase_core::notifications::NotificationWire {
            id: "abcdef01-notification".to_string(),
            timestamp: "2026-05-01T01:02:03+00:00".to_string(),
            sender: "test".to_string(),
            notes: vec!["hello".to_string()],
            files: Vec::new(),
            action: None,
            action_data: BTreeMap::new(),
            read: false,
            dismissed: false,
            silent: false,
            muted: false,
            snooze_until: None,
        };
        sase_core::notifications::append_notification(
            &notification_path,
            &notification,
        )
        .unwrap();

        let mut db = ProjectionDb::open_in_memory().unwrap();
        let summaries = rebuild_selected_sources(
            &mut db,
            Some(projects_root),
            "host-a".to_string(),
            IndexingSelector {
                surface: LocalDaemonIndexingSurfaceWire::Notifications,
                project_id: None,
            },
        )
        .unwrap();

        assert_eq!(summaries.len(), 1);
        assert_eq!(
            summaries[0].surface,
            LocalDaemonIndexingSurfaceWire::Notifications
        );
        assert_eq!(summaries[0].diff_counts, ShadowDiffCountsWire::default());
    }

    fn change(domain: &str, source_path: &str) -> SourceChangeWire {
        SourceChangeWire {
            schema_version: INDEXING_WIRE_SCHEMA_VERSION,
            identity: SourceIdentityWire {
                schema_version: INDEXING_WIRE_SCHEMA_VERSION,
                domain: domain.to_string(),
                project_id: Some("project-a".to_string()),
                source_path: source_path.to_string(),
                is_archive: false,
                fingerprint: Some(SourceFingerprintWire::default()),
                last_indexed_event_seq: None,
            },
            operation: SourceChangeOperationWire::Upsert,
            reason: Some("synthetic".to_string()),
        }
    }

    async fn wait_for_status(
        service: &IndexingService,
        predicate: impl Fn(&IndexingServiceStatusWire) -> bool,
    ) -> IndexingServiceStatusWire {
        let mut status = service.status();
        for _ in 0..50 {
            if predicate(&status) {
                return status;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
            status = service.status();
        }
        status
    }
}
