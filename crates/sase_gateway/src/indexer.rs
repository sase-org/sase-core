use std::{
    collections::BTreeMap,
    fmt,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, RwLock},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use notify::{
    event::ModifyKind, EventKind, RecommendedWatcher, RecursiveMode, Watcher,
};
use sase_core::projections::{
    IndexingDomainReportWire, ShadowDiffCountsWire, SourceChangeOperationWire,
    SourceChangeWire, SourceFingerprintWire, SourceIdentityWire,
    INDEXING_WIRE_SCHEMA_VERSION,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};
use thiserror::Error;
use tokio::sync::{mpsc, Semaphore};

use crate::{
    daemon::{DaemonShutdown, LocalEventHub},
    metrics::DaemonMetrics,
    wire::{
        LocalDaemonCollectionWire, LocalDaemonDeltaOperationWire,
        LocalDaemonEventPayloadWire,
    },
};

const DEFAULT_INDEXING_QUEUE_CAPACITY: usize = 512;
const DEFAULT_INDEXING_WORKER_CAPACITY: usize = 2;
const DEFAULT_DEBOUNCE_MS: u64 = 75;
const RECENT_REPORT_LIMIT: usize = 8;

#[derive(Clone, Debug)]
pub struct IndexingConfig {
    pub enabled: bool,
    pub watch_roots: Vec<PathBuf>,
    pub queue_capacity: usize,
    pub worker_capacity: usize,
    pub debounce: Duration,
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
    _watcher: Mutex<Option<Box<dyn SourceWatcher>>>,
    metrics: DaemonMetrics,
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
            queue_capacity: DEFAULT_INDEXING_QUEUE_CAPACITY,
            worker_capacity: DEFAULT_INDEXING_WORKER_CAPACITY,
            debounce: Duration::from_millis(DEFAULT_DEBOUNCE_MS),
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
                _watcher: Mutex::new(None),
                metrics,
            }),
        }
    }

    pub fn start(
        config: IndexingConfig,
        shutdown: DaemonShutdown,
        metrics: DaemonMetrics,
        local_events: LocalEventHub,
    ) -> Self {
        Self::start_with_factory(
            config,
            shutdown,
            metrics,
            local_events,
            NotifySourceWatcherFactory,
        )
    }

    pub fn start_with_factory<F>(
        config: IndexingConfig,
        shutdown: DaemonShutdown,
        metrics: DaemonMetrics,
        local_events: LocalEventHub,
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

        tokio::spawn(worker_loop(
            receiver,
            status.clone(),
            metrics.clone(),
            local_events,
            shutdown,
            config.debounce,
            worker_capacity,
        ));

        Self {
            inner: Arc::new(IndexingServiceInner {
                sender: Some(sender),
                status,
                _watcher: Mutex::new(watcher),
                metrics,
            }),
        }
    }

    pub fn status(&self) -> IndexingServiceStatusWire {
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
            identity: source_identity_from_path("filesystem", path),
            operation: operation.clone(),
            reason: Some(format!("{:?}", event.kind)),
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
    SourceIdentityWire {
        schema_version: INDEXING_WIRE_SCHEMA_VERSION,
        domain: domain.into(),
        project_id: None,
        source_path: path.to_string_lossy().to_string(),
        is_archive: false,
        fingerprint: file_fingerprint(path),
        last_indexed_event_seq: None,
    }
}

fn file_fingerprint(path: &Path) -> Option<SourceFingerprintWire> {
    let metadata = std::fs::metadata(path).ok()?;
    let modified_at_unix_millis =
        metadata.modified().ok().and_then(system_time_unix_millis);
    Some(SourceFingerprintWire {
        schema_version: INDEXING_WIRE_SCHEMA_VERSION,
        file_size: Some(metadata.len()),
        modified_at_unix_millis,
        inode: file_inode(&metadata),
        content_sha256: None,
    })
}

#[cfg(unix)]
fn file_inode(metadata: &std::fs::Metadata) -> Option<u64> {
    use std::os::unix::fs::MetadataExt;
    Some(metadata.ino())
}

#[cfg(not(unix))]
fn file_inode(_metadata: &std::fs::Metadata) -> Option<u64> {
    None
}

fn system_time_unix_millis(value: SystemTime) -> Option<i64> {
    let duration = value.duration_since(UNIX_EPOCH).ok()?;
    Some(duration.as_millis().min(i64::MAX as u128) as i64)
}

async fn worker_loop(
    mut receiver: mpsc::Receiver<SourceChangeWire>,
    status: Arc<RwLock<IndexingServiceStatusWire>>,
    metrics: DaemonMetrics,
    local_events: LocalEventHub,
    shutdown: DaemonShutdown,
    debounce: Duration,
    worker_capacity: usize,
) {
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
        let mut pending =
            BTreeMap::from([(change.identity.stable_key(), change)]);
        loop {
            tokio::select! {
                maybe_change = receiver.recv() => {
                    let Some(change) = maybe_change else {
                        break;
                    };
                    metrics.record_indexing_queued_change();
                    increment_queued(&status);
                    pending.insert(change.identity.stable_key(), change);
                }
                _ = tokio::time::sleep(debounce) => break,
            }
        }

        let queued_changes = pending.len() as u64;
        let changes: Vec<SourceChangeWire> = pending.into_values().collect();
        let Ok(permit) = semaphore.clone().acquire_owned().await else {
            mark_degraded(&status, "indexing worker semaphore closed");
            return;
        };
        let reports = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            build_reports(changes, queued_changes)
        })
        .await;

        match reports {
            Ok(reports) => {
                for report in reports {
                    metrics.record_indexing_report(
                        report.indexed_sources,
                        report.failed_parses,
                        report.coalesced_changes,
                        report.shadow_diff_counts.missing,
                        report.shadow_diff_counts.stale,
                        report.shadow_diff_counts.extra,
                        report.shadow_diff_counts.corrupt,
                    );
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

fn build_reports(
    changes: Vec<SourceChangeWire>,
    queued_changes: u64,
) -> Vec<IndexingDomainReportWire> {
    let mut by_domain: BTreeMap<String, Vec<SourceChangeWire>> =
        BTreeMap::new();
    for change in changes {
        by_domain
            .entry(change.identity.domain.clone())
            .or_default()
            .push(change);
    }

    by_domain
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
        .collect()
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
    use std::time::Duration;

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
                queue_capacity: 4,
                worker_capacity: 1,
                debounce: Duration::from_millis(5),
                enabled: true,
            },
            DaemonShutdown::default(),
            metrics,
            events.clone(),
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
    async fn watcher_failure_degrades_indexing_without_disabling_daemon() {
        let metrics = DaemonMetrics::default();
        let events = LocalEventHub::new(8, metrics.clone());
        let service = IndexingService::start_with_factory(
            IndexingConfig::default(),
            DaemonShutdown::default(),
            metrics,
            events.clone(),
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
}
