//! `FleetReadService`: snapshot-backed fleet read APIs with refresh
//! coalescing and stale retention.

use std::sync::atomic::AtomicU64;
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use sase_core::{
    agent_scan::{
        checkpoint_agent_artifact_index_wal_if_oversized,
        AgentArtifactIndexFreshnessWire, AgentArtifactIndexQueryWire,
        AgentArtifactRecordShapeWire, AgentArtifactScanOptionsWire,
        AgentSessionDismissalLineageCandidateWire,
        AGENT_ARTIFACT_INDEX_WAL_SIZE_LIMIT_BYTES,
    },
    fleet_catalog::select_fleet_presentation,
    fleet_contract::{
        ensure_installation_identity, fleet_content_read_limit,
        fleet_project_eligibility_limit, logical_locator_key,
        select_fleet_catalog_page, select_fleet_logical_batch,
        validate_fleet_catalog_query, validate_fleet_content_read_request,
        validate_fleet_detail_request, validate_fleet_logical_batch_request,
        validate_fleet_project_eligibility_request,
        FleetAuthoritativeSnapshotWire, FleetCatalogPageWire,
        FleetCatalogQueryWire, FleetCatalogScopeWire,
        FleetContentReadRequestWire, FleetContentReadResponseWire,
        FleetDetailRequestWire, FleetDetailResponseWire,
        FleetInvalidationEventWire, FleetInvalidationKindWire,
        FleetLogicalBatchRequestWire, FleetLogicalBatchResponseWire,
        FleetProjectEligibilityRequestWire,
        FleetProjectEligibilityResponseWire, FleetProjectEligibilityWire,
        FleetSummaryResponseWire, ObservationFreshnessWire, OwnerLivenessWire,
        ResolvedAgentDetailWire, ResourceRevisionWire, StoreCursorWire,
        FLEET_CONTRACT_SCHEMA_VERSION, FLEET_READ_DEFAULT_REPLAY_EVENTS,
    },
    fleet_owner_facts::{HostOwnerFileObserver, OwnerFileObserver},
    host_liveness::{HostOwnerLivenessObserver, OwnerLivenessObserver},
    list_project_records, query_agent_artifact_index,
    resolve_agent_session_dismissal_lineage,
};
use tokio::sync::{watch, Semaphore};

use super::{
    content::read_content_range,
    errors::FleetReadError,
    invalidation::{FleetEventSubscription, FleetInvalidationHub},
    resolution::{
        observation_freshness_for_age, resolve_record, safe_reason,
        FLEET_SNAPSHOT_STALE_SECONDS,
    },
    snapshot::{
        build_snapshot_blocking, BuildSnapshotRequest, CachedFleetSnapshot,
    },
};

use crate::fleet_auth::current_unix_time;
use crate::fleet_launch::FleetLaunchStore;

pub(super) const SNAPSHOT_REFRESH_TIMEOUT: Duration = Duration::from_secs(4);

/// First back-off window after one failed snapshot build. The window
/// doubles per consecutive failure up to [`SNAPSHOT_BACKOFF_MAX`].
const SNAPSHOT_BACKOFF_FIRST: Duration = Duration::from_secs(15);
/// Upper bound for the exponential snapshot-build back-off window.
const SNAPSHOT_BACKOFF_MAX: Duration = Duration::from_secs(120);
/// Gateway-wide cap on concurrent artifact-index operations held by
/// snapshot builds and the overlay pass.
const SNAPSHOT_INDEX_PERMITS: usize = 2;
/// Upper bound on how many in-flight builds a forced refresh joins before
/// it falls back to a timeout response instead of waiting forever on a
/// wedged build.
const FORCE_REFRESH_MAX_JOINS: u32 = 4;
/// A build still in flight this long gets one `warn` event. It cannot be
/// cancelled, so the log is the only signal for a wedged build.
const LONG_RUNNING_BUILD_WARN_AFTER: Duration = Duration::from_secs(60);
/// Tracing target for every fleet-refresh telemetry event.
const FLEET_READS_TRACE_TARGET: &str = "sase_gateway::fleet_reads";

fn scope_name(scope: SnapshotScope) -> &'static str {
    match scope {
        SnapshotScope::Presentation => "presentation",
        SnapshotScope::History => "history",
    }
}

#[derive(Clone)]
pub struct FleetReadService {
    inner: Arc<FleetReadServiceInner>,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum SnapshotScope {
    Presentation,
    History,
}

/// Outcome of one detached snapshot build, shared with every caller that
/// joined it while it was in flight.
#[derive(Clone, Debug)]
struct RefreshOutcome {
    /// Monotonic instant the build task was spawned. A forced refresh
    /// only accepts an outcome whose build began after the forced call,
    /// so `reconcile` always sees a post-settlement view.
    started: Instant,
    snapshot: Option<CachedFleetSnapshot>,
    error: Option<FleetReadError>,
}

/// Handle to the one running build for a scope. The build task owns the
/// watch sender and every waiter clones the receiver, so a completion can
/// never be missed between subscribing and waiting.
struct InFlightJoin {
    generation: u64,
    receiver: watch::Receiver<Option<RefreshOutcome>>,
    /// Waiters that hit `refresh_timeout` while this build ran. Counted,
    /// never logged per request; the finished-build event reports the total.
    timed_out: Arc<AtomicU64>,
}

/// Per-scope refresh state behind a plain mutex. The mutex is never held
/// across an `.await`.
struct ScopeState {
    cache: Option<CachedFleetSnapshot>,
    in_flight: Option<InFlightJoin>,
    last_failure_at: Option<Instant>,
    last_error: Option<FleetReadError>,
    consecutive_failures: u32,
    generation: u64,
    builds_started: u64,
}

impl ScopeState {
    fn new() -> Self {
        Self {
            cache: None,
            in_flight: None,
            last_failure_at: None,
            last_error: None,
            consecutive_failures: 0,
            generation: 0,
            builds_started: 0,
        }
    }

    /// Remaining back-off, if a recent failure still forbids new builds.
    fn backoff_remaining(&self) -> Option<Duration> {
        let last = self.last_failure_at?;
        let shift = self.consecutive_failures.saturating_sub(1).min(16);
        let window = SNAPSHOT_BACKOFF_FIRST
            .checked_mul(1u32.checked_shl(shift).unwrap_or(u32::MAX))?
            .min(SNAPSHOT_BACKOFF_MAX);
        window.checked_sub(last.elapsed())
    }
}

/// Coalescing and memo slot for the presentation overlay pass. At most one
/// overlay query runs at a time; concurrent callers skip, and a burst of
/// catalog pages against one snapshot replays one memoized result.
struct OverlayState {
    in_flight: bool,
    memo_key: Option<(u64, FleetCatalogScopeWire, Vec<String>)>,
    memo_details: Vec<ResolvedAgentDetailWire>,
}

impl OverlayState {
    fn new() -> Self {
        Self {
            in_flight: false,
            memo_key: None,
            memo_details: Vec::new(),
        }
    }
}

struct FleetReadServiceInner {
    sase_home: PathBuf,
    index_path: PathBuf,
    projects_root: PathBuf,
    refresh_timeout: Duration,
    presentation: Mutex<ScopeState>,
    history: Mutex<ScopeState>,
    index_permits: Arc<Semaphore>,
    overlay: Mutex<OverlayState>,
    events: FleetInvalidationHub,
    liveness: Arc<dyn OwnerLivenessObserver>,
    owner_files: Arc<dyn OwnerFileObserver>,
    #[cfg(test)]
    overlay_queries: AtomicUsize,
    /// Successful Presentation builds that ran the WAL checkpoint hook.
    /// Proves housekeeping runs on success and never on failure.
    #[cfg(test)]
    checkpoint_hook_calls: AtomicUsize,
}

impl std::fmt::Debug for FleetReadService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FleetReadService")
            .field("sase_home", &self.inner.sase_home)
            .field("index_path", &self.inner.index_path)
            .field("projects_root", &self.inner.projects_root)
            .field("refresh_timeout", &self.inner.refresh_timeout)
            .finish_non_exhaustive()
    }
}

impl FleetReadService {
    pub fn new(sase_home: impl Into<PathBuf>) -> Self {
        Self::new_with_timeout(sase_home, SNAPSHOT_REFRESH_TIMEOUT)
    }

    pub fn new_with_timeout(
        sase_home: impl Into<PathBuf>,
        refresh_timeout: Duration,
    ) -> Self {
        Self::new_with_liveness(
            sase_home,
            refresh_timeout,
            Arc::new(HostOwnerLivenessObserver::default()),
        )
    }

    pub(super) fn new_with_liveness(
        sase_home: impl Into<PathBuf>,
        refresh_timeout: Duration,
        liveness: Arc<dyn OwnerLivenessObserver>,
    ) -> Self {
        let sase_home = sase_home.into();
        Self {
            inner: Arc::new(FleetReadServiceInner {
                index_path: sase_home.join("agent_artifact_index.sqlite"),
                projects_root: sase_home.join("projects"),
                sase_home,
                refresh_timeout,
                presentation: Mutex::new(ScopeState::new()),
                history: Mutex::new(ScopeState::new()),
                index_permits: Arc::new(Semaphore::new(SNAPSHOT_INDEX_PERMITS)),
                overlay: Mutex::new(OverlayState::new()),
                events: FleetInvalidationHub::new(
                    FLEET_READ_DEFAULT_REPLAY_EVENTS,
                )
                .expect("default fleet replay capacity is valid"),
                liveness,
                owner_files: Arc::new(HostOwnerFileObserver::default()),
                #[cfg(test)]
                overlay_queries: AtomicUsize::new(0),
                #[cfg(test)]
                checkpoint_hook_calls: AtomicUsize::new(0),
            }),
        }
    }

    pub fn index_path(&self) -> &Path {
        &self.inner.index_path
    }

    pub fn projects_root(&self) -> &Path {
        &self.inner.projects_root
    }

    pub async fn summary(
        &self,
    ) -> Result<FleetSummaryResponseWire, FleetReadError> {
        let snapshot = self.stamped_snapshot(false).await?;
        Ok(FleetSummaryResponseWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            cursor: snapshot.wire.cursor,
            catalog_scope: snapshot.wire.catalog_scope,
            catalog_snapshot_id: snapshot.wire.catalog_snapshot_id,
            counts: snapshot.wire.counts,
            count_revision: snapshot.wire.count_revision,
            freshness: snapshot.wire.freshness,
        })
    }

    pub async fn catalog(
        &self,
        query: FleetCatalogQueryWire,
    ) -> Result<FleetCatalogPageWire, FleetReadError> {
        let query = validate_fleet_catalog_query(&query)
            .map_err(FleetReadError::from)?;
        let presentation = self.stamped_snapshot(false).await?;
        let mut catalog_snapshot = match query.scope {
            FleetCatalogScopeWire::Presentation => presentation.clone(),
            FleetCatalogScopeWire::History => {
                self.stamped_history_snapshot(false).await?
            }
        };
        if query.scope == FleetCatalogScopeWire::Presentation {
            self.overlay_missing_into_snapshot(&mut catalog_snapshot)
                .await;
        }
        let page =
            select_fleet_catalog_page(&query, &catalog_snapshot.wire.summaries)
                .map_err(FleetReadError::from)?;
        Ok(FleetCatalogPageWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            cursor: catalog_snapshot.wire.cursor,
            counts: presentation.wire.counts,
            count_revision: presentation.wire.count_revision,
            freshness: catalog_snapshot.wire.freshness,
            page,
        })
    }

    pub async fn batch_lookup(
        &self,
        request: FleetLogicalBatchRequestWire,
    ) -> Result<FleetLogicalBatchResponseWire, FleetReadError> {
        validate_fleet_logical_batch_request(&request)
            .map_err(FleetReadError::from)?;
        let snapshot = self.stamped_snapshot(false).await?;
        let entries =
            select_fleet_logical_batch(&request, &snapshot.wire.summaries)
                .map_err(FleetReadError::from)?;
        Ok(FleetLogicalBatchResponseWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            cursor: snapshot.wire.cursor,
            counts: snapshot.wire.counts,
            count_revision: snapshot.wire.count_revision,
            freshness: snapshot.wire.freshness,
            entries,
        })
    }

    pub async fn detail(
        &self,
        request: FleetDetailRequestWire,
    ) -> Result<FleetDetailResponseWire, FleetReadError> {
        let request = validate_fleet_detail_request(&request)
            .map_err(FleetReadError::from)?;
        let mut snapshot = self.stamped_snapshot(false).await?;
        if let Some(detail) = snapshot
            .details_by_logical_key
            .get(&request.logical_key)
            .cloned()
        {
            return Ok(FleetDetailResponseWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                cursor: snapshot.wire.cursor,
                freshness: snapshot.wire.freshness,
                detail,
            });
        }
        self.overlay_missing_into_snapshot(&mut snapshot).await;
        let detail = snapshot
            .details_by_logical_key
            .get(&request.logical_key)
            .cloned()
            .ok_or_else(|| {
                FleetReadError::NotFound("logical_key".to_string())
            })?;
        Ok(FleetDetailResponseWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            cursor: snapshot.wire.cursor,
            freshness: snapshot.wire.freshness,
            detail,
        })
    }

    pub async fn content(
        &self,
        request: FleetContentReadRequestWire,
    ) -> Result<FleetContentReadResponseWire, FleetReadError> {
        let request = validate_fleet_content_read_request(&request)
            .map_err(FleetReadError::from)?;
        let limit =
            fleet_content_read_limit(&request).map_err(FleetReadError::from)?;
        let snapshot = self.current_snapshot(false).await?;
        let source = snapshot
            .content_by_handle
            .get(&request.handle_id)
            .cloned()
            .ok_or_else(|| FleetReadError::NotFound("handle_id".to_string()))?;
        if request.row_revision != source.row_revision {
            return Err(FleetReadError::Stale("row_revision".to_string()));
        }
        let cursor = snapshot.wire.cursor.clone();
        read_content_range(source, request.offset, limit, cursor).await
    }

    pub async fn project_eligibility(
        &self,
        request: FleetProjectEligibilityRequestWire,
    ) -> Result<FleetProjectEligibilityResponseWire, FleetReadError> {
        let request = validate_fleet_project_eligibility_request(&request)
            .map_err(FleetReadError::from)?;
        let limit = fleet_project_eligibility_limit(&request)
            .map_err(FleetReadError::from)?;
        let projects_root = self.inner.projects_root.clone();
        let requested = request.project_ids.clone();
        let response = tokio::task::spawn_blocking(move || {
            project_eligibility_blocking(&projects_root, &requested, limit)
        })
        .await
        .map_err(|_| FleetReadError::Backend("project_join".to_string()))??;
        Ok(response)
    }

    pub async fn authoritative_snapshot(
        &self,
    ) -> Result<FleetAuthoritativeSnapshotWire, FleetReadError> {
        Ok(self.stamped_snapshot(false).await?.wire)
    }

    pub async fn reconcile(
        &self,
    ) -> Result<Vec<FleetInvalidationEventWire>, FleetReadError> {
        let before = self.current_snapshot(false).await?;
        let after = self.current_snapshot(true).await?;
        let mut events = Vec::new();
        let after_keys = after
            .wire
            .summaries
            .iter()
            .map(|summary| summary.logical_key.clone())
            .collect::<BTreeSet<_>>();
        for summary in &after.wire.summaries {
            match before.summaries_by_logical_key.get(&summary.logical_key) {
                None => events.push(self.publish_invalidation(
                    FleetInvalidationKindWire::Launched,
                    Some(summary.logical_key.clone()),
                    Some(summary.row_revision.clone()),
                    "launched",
                )?),
                Some(previous) => {
                    if previous.lifecycle != summary.lifecycle {
                        events.push(self.publish_invalidation(
                            FleetInvalidationKindWire::LifecycleChanged,
                            Some(summary.logical_key.clone()),
                            Some(summary.row_revision.clone()),
                            "lifecycle_changed",
                        )?);
                    } else if previous.needs_attention
                        != summary.needs_attention
                    {
                        events.push(self.publish_invalidation(
                            FleetInvalidationKindWire::AttentionChanged,
                            Some(summary.logical_key.clone()),
                            Some(summary.row_revision.clone()),
                            "attention_changed",
                        )?);
                    } else if previous.row_revision != summary.row_revision {
                        events.push(self.publish_invalidation(
                            FleetInvalidationKindWire::RevisionChanged,
                            Some(summary.logical_key.clone()),
                            Some(summary.row_revision.clone()),
                            "revision_changed",
                        )?);
                    } else if previous.liveness == OwnerLivenessWire::Alive
                        && summary.liveness == OwnerLivenessWire::Dead
                    {
                        events.push(self.publish_invalidation(
                            FleetInvalidationKindWire::ProcessExited,
                            Some(summary.logical_key.clone()),
                            Some(summary.row_revision.clone()),
                            "process_exited",
                        )?);
                    }
                }
            }
        }
        for key in before.summaries_by_logical_key.keys() {
            if !after_keys.contains(key) {
                events.push(self.publish_invalidation(
                    FleetInvalidationKindWire::Deleted,
                    Some(key.clone()),
                    None,
                    "deleted",
                )?);
            }
        }
        Ok(events)
    }

    pub fn current_event_cursor(&self) -> StoreCursorWire {
        self.inner.events.current_cursor()
    }

    pub async fn subscribe_events(
        &self,
        cursor: Option<StoreCursorWire>,
    ) -> Result<FleetEventSubscription, FleetReadError> {
        let snapshot = self.authoritative_snapshot().await?;
        self.inner.events.subscribe(cursor, snapshot)
    }

    pub fn publish_invalidation(
        &self,
        kind: FleetInvalidationKindWire,
        logical_key: Option<String>,
        row_revision: Option<ResourceRevisionWire>,
        reason: &str,
    ) -> Result<FleetInvalidationEventWire, FleetReadError> {
        self.inner
            .events
            .publish(kind, logical_key, row_revision, reason)
    }

    #[cfg(test)]
    pub fn replace_event_generation_for_test(&self) {
        self.inner.events.replace_generation();
    }

    #[cfg(test)]
    pub fn mark_deletion_history_incomplete_for_test(&self) {
        self.inner.events.mark_deletion_history_incomplete();
    }

    #[cfg(test)]
    pub fn refresh_count_for_test(&self) -> u64 {
        self.inner
            .presentation
            .lock()
            .ok()
            .and_then(|state| {
                state.cache.as_ref().map(|snapshot| snapshot.refresh_count)
            })
            .unwrap_or(0)
    }

    #[cfg(test)]
    pub fn history_refresh_count_for_test(&self) -> u64 {
        self.inner
            .history
            .lock()
            .ok()
            .and_then(|state| {
                state.cache.as_ref().map(|snapshot| snapshot.refresh_count)
            })
            .unwrap_or(0)
    }

    #[cfg(test)]
    pub fn history_cache_initialized_for_test(&self) -> bool {
        self.inner
            .history
            .lock()
            .ok()
            .and_then(|state| state.cache.as_ref().map(|_| ()))
            .is_some()
    }

    /// Push the cached snapshot's build instant backward so it reads as
    /// older than it really is, without a wall-clock sleep.
    #[cfg(test)]
    pub fn age_cache_for_test(&self, seconds: f64) {
        if let Ok(mut state) = self.inner.presentation.lock() {
            if let Some(snapshot) = state.cache.as_mut() {
                snapshot.build_instant -= Duration::from_secs_f64(seconds);
            }
        }
    }

    #[cfg(test)]
    pub fn age_history_cache_for_test(&self, seconds: f64) {
        if let Ok(mut state) = self.inner.history.lock() {
            if let Some(snapshot) = state.cache.as_mut() {
                snapshot.build_instant -= Duration::from_secs_f64(seconds);
            }
        }
    }

    /// Number of snapshot builds started for the presentation scope,
    /// including builds whose waiters timed out and builds that failed.
    #[cfg(test)]
    pub fn builds_started_for_test(&self) -> u64 {
        self.inner
            .presentation
            .lock()
            .ok()
            .map(|state| state.builds_started)
            .unwrap_or(0)
    }

    /// Number of snapshot builds started for the history scope.
    #[cfg(test)]
    pub fn history_builds_started_for_test(&self) -> u64 {
        self.inner
            .history
            .lock()
            .ok()
            .map(|state| state.builds_started)
            .unwrap_or(0)
    }

    /// Whether a presentation snapshot build is currently in flight.
    #[cfg(test)]
    pub fn snapshot_build_in_flight_for_test(&self) -> bool {
        self.inner
            .presentation
            .lock()
            .ok()
            .map(|state| state.in_flight.is_some())
            .unwrap_or(false)
    }

    /// Whether a history snapshot build is currently in flight.
    #[cfg(test)]
    pub fn history_build_in_flight_for_test(&self) -> bool {
        self.inner
            .history
            .lock()
            .ok()
            .map(|state| state.in_flight.is_some())
            .unwrap_or(false)
    }

    /// Consecutive presentation build failures still counting toward the
    /// back-off schedule.
    #[cfg(test)]
    pub fn consecutive_failures_for_test(&self) -> u32 {
        self.inner
            .presentation
            .lock()
            .ok()
            .map(|state| state.consecutive_failures)
            .unwrap_or(0)
    }

    /// Consecutive history build failures still counting toward back-off.
    #[cfg(test)]
    pub fn history_consecutive_failures_for_test(&self) -> u32 {
        self.inner
            .history
            .lock()
            .ok()
            .map(|state| state.consecutive_failures)
            .unwrap_or(0)
    }

    /// Expire the presentation back-off window without sleeping, so the
    /// next read attempts a new build.
    #[cfg(test)]
    pub fn expire_backoff_for_test(&self) {
        if let Ok(mut state) = self.inner.presentation.lock() {
            state.last_failure_at = None;
        }
    }

    /// Expire the history back-off window without sleeping.
    #[cfg(test)]
    pub fn expire_history_backoff_for_test(&self) {
        if let Ok(mut state) = self.inner.history.lock() {
            state.last_failure_at = None;
        }
    }

    /// Number of overlay index queries actually executed. Memo hits and
    /// contention skips do not count.
    #[cfg(test)]
    pub fn overlay_query_count_for_test(&self) -> usize {
        self.inner.overlay_queries.load(Ordering::Relaxed)
    }

    /// Successful Presentation builds that ran the WAL checkpoint hook.
    #[cfg(test)]
    pub fn checkpoint_hook_calls_for_test(&self) -> usize {
        self.inner.checkpoint_hook_calls.load(Ordering::Relaxed)
    }

    /// Hold every index permit for as long as the returned guards live, so
    /// tests can prove the overlay pass skips under contention.
    #[cfg(test)]
    pub async fn hold_all_index_permits_for_test(
        &self,
    ) -> Vec<tokio::sync::OwnedSemaphorePermit> {
        let mut permits = Vec::new();
        for _ in 0..SNAPSHOT_INDEX_PERMITS {
            permits.push(
                self.inner
                    .index_permits
                    .clone()
                    .acquire_owned()
                    .await
                    .expect("test index permits are acquirable"),
            );
        }
        permits
    }

    /// Return the current snapshot with every row and the envelope stamped
    /// with age-derived freshness. This is the accessor every read path that
    /// serves rows to a caller should use.
    async fn stamped_snapshot(
        &self,
        force: bool,
    ) -> Result<CachedFleetSnapshot, FleetReadError> {
        let mut snapshot = self.current_snapshot(force).await?;
        let freshness = observation_freshness_for_age(
            snapshot.build_instant.elapsed().as_secs_f64(),
        )?;
        snapshot.wire.freshness.freshness = freshness;
        for summary in &mut snapshot.wire.summaries {
            summary.freshness = freshness;
        }
        for summary in snapshot.summaries_by_logical_key.values_mut() {
            summary.freshness = freshness;
        }
        for detail in snapshot.details_by_logical_key.values_mut() {
            detail.summary.freshness = freshness;
        }
        Ok(snapshot)
    }

    async fn current_snapshot(
        &self,
        force: bool,
    ) -> Result<CachedFleetSnapshot, FleetReadError> {
        self.refresh_scope(SnapshotScope::Presentation, force).await
    }

    async fn stamped_history_snapshot(
        &self,
        force: bool,
    ) -> Result<CachedFleetSnapshot, FleetReadError> {
        let mut snapshot = self.current_history_snapshot(force).await?;
        let freshness = observation_freshness_for_age(
            snapshot.build_instant.elapsed().as_secs_f64(),
        )?;
        snapshot.wire.freshness.freshness = freshness;
        for summary in &mut snapshot.wire.summaries {
            summary.freshness = freshness;
        }
        for summary in snapshot.summaries_by_logical_key.values_mut() {
            summary.freshness = freshness;
        }
        for detail in snapshot.details_by_logical_key.values_mut() {
            detail.summary.freshness = freshness;
        }
        Ok(snapshot)
    }

    async fn current_history_snapshot(
        &self,
        force: bool,
    ) -> Result<CachedFleetSnapshot, FleetReadError> {
        self.refresh_scope(SnapshotScope::History, force).await
    }

    /// Single-flight snapshot refresh for one scope.
    ///
    /// At most one build runs per scope. The first caller that needs a
    /// rebuild spawns a detached task that always fills the cache when it
    /// finishes, even after every waiter has timed out; every other caller
    /// joins that build and waits at most `refresh_timeout`. No caller can
    /// drop, abort, or start a second build while one is in flight.
    async fn refresh_scope(
        &self,
        scope: SnapshotScope,
        force: bool,
    ) -> Result<CachedFleetSnapshot, FleetReadError> {
        let force_since = Instant::now();
        if !force {
            if let Some(snapshot) = self.unexpired_scoped_snapshot(scope)? {
                return Ok(snapshot);
            }
        }
        // Forced callers only accept a snapshot built after this call, so
        // `reconcile` always sees a post-settlement view. A wedged build
        // must not hang a forced caller forever, so re-joins are bounded.
        let mut joins = 0u32;
        loop {
            let decision = {
                let mut state =
                    self.scope_state(scope).lock().map_err(|_| {
                        FleetReadError::Backend("snapshot_cache".to_string())
                    })?;
                if let Some(cached) = state.cache.clone() {
                    let fresh = cached.build_instant.elapsed().as_secs_f64()
                        < FLEET_SNAPSHOT_STALE_SECONDS;
                    if fresh && (!force || cached.build_instant >= force_since)
                    {
                        return Ok(cached);
                    }
                }
                if let Some(in_flight) = state.in_flight.as_ref() {
                    RefreshDecision::Join {
                        receiver: in_flight.receiver.clone(),
                        timed_out: Arc::clone(&in_flight.timed_out),
                    }
                } else if let Some(remaining) = state.backoff_remaining() {
                    RefreshDecision::Backoff {
                        code: state
                            .last_error
                            .as_ref()
                            .map(FleetReadError::safe_code)
                            .unwrap_or_else(|| "backend".to_string()),
                        error: state.last_error.clone().unwrap_or_else(|| {
                            FleetReadError::Backend(
                                "snapshot_backoff".to_string(),
                            )
                        }),
                        consecutive_failures: state.consecutive_failures,
                        window_secs: remaining.as_secs(),
                    }
                } else {
                    let (sender, receiver) =
                        watch::channel::<Option<RefreshOutcome>>(None);
                    state.generation = state.generation.saturating_add(1);
                    let generation = state.generation;
                    state.builds_started =
                        state.builds_started.saturating_add(1);
                    let started = Instant::now();
                    let build = BuildSnapshotRequest {
                        sase_home: self.inner.sase_home.clone(),
                        index_path: self.inner.index_path.clone(),
                        projects_root: self.inner.projects_root.clone(),
                        cursor: self.inner.events.current_cursor(),
                        prior_refresh_count: state
                            .cache
                            .as_ref()
                            .map(|snapshot| snapshot.refresh_count)
                            .unwrap_or(0),
                        scope: match scope {
                            SnapshotScope::Presentation => {
                                FleetCatalogScopeWire::Presentation
                            }
                            SnapshotScope::History => {
                                FleetCatalogScopeWire::History
                            }
                        },
                        liveness: Arc::clone(&self.inner.liveness),
                        owner_files: Arc::clone(&self.inner.owner_files),
                    };
                    let timed_out = Arc::new(AtomicU64::new(0));
                    state.in_flight = Some(InFlightJoin {
                        generation,
                        receiver: receiver.clone(),
                        timed_out: Arc::clone(&timed_out),
                    });
                    RefreshDecision::Start {
                        sender,
                        receiver,
                        generation,
                        started,
                        build,
                        timed_out,
                    }
                }
            };
            match decision {
                RefreshDecision::Backoff {
                    code,
                    error,
                    consecutive_failures,
                    window_secs,
                } => {
                    tracing::warn!(
                        target: FLEET_READS_TRACE_TARGET,
                        scope = scope_name(scope),
                        consecutive_failures,
                        window_secs,
                        "fleet snapshot back-off engaged",
                    );
                    let latest = self.cached_scoped_snapshot(scope)?;
                    return match latest {
                        Some(snapshot) => Ok(mark_stale(snapshot, code)),
                        None => Err(error),
                    };
                }
                RefreshDecision::Join {
                    receiver,
                    timed_out,
                    ..
                } => {
                    if let Some(snapshot) = self
                        .wait_for_build(
                            scope,
                            receiver,
                            timed_out,
                            force,
                            force_since,
                            &mut joins,
                        )
                        .await?
                    {
                        return Ok(snapshot);
                    }
                }
                RefreshDecision::Start {
                    sender,
                    receiver,
                    generation,
                    started,
                    build,
                    timed_out,
                } => {
                    self.spawn_scoped_build(
                        scope,
                        sender,
                        generation,
                        started,
                        build,
                        timed_out.clone(),
                    );
                    if let Some(snapshot) = self
                        .wait_for_build(
                            scope,
                            receiver,
                            timed_out,
                            force,
                            force_since,
                            &mut joins,
                        )
                        .await?
                    {
                        return Ok(snapshot);
                    }
                }
            }
        }
    }

    /// Spawn the detached task for one snapshot build. The task holds a
    /// gateway-wide index permit for the whole blocking build, then
    /// records its outcome and wakes every waiter. It always fills the
    /// cache on success, even when every waiter already timed out.
    fn spawn_scoped_build(
        &self,
        scope: SnapshotScope,
        sender: watch::Sender<Option<RefreshOutcome>>,
        generation: u64,
        started: Instant,
        build: BuildSnapshotRequest,
        timed_out: Arc<AtomicU64>,
    ) {
        let inner = Arc::clone(&self.inner);
        let watchdog_inner = Arc::clone(&inner);
        tokio::spawn(async move {
            tokio::time::sleep(LONG_RUNNING_BUILD_WARN_AFTER).await;
            let in_flight = match scope_state_for(&watchdog_inner, scope).lock()
            {
                Ok(state) => {
                    state.in_flight.as_ref().is_some_and(|in_flight| {
                        in_flight.generation == generation
                    })
                }
                Err(_) => false,
            };
            if in_flight {
                tracing::warn!(
                    target: FLEET_READS_TRACE_TARGET,
                    scope = scope_name(scope),
                    elapsed_secs =
                        LONG_RUNNING_BUILD_WARN_AFTER.as_secs(),
                    "fleet snapshot build still running",
                );
            }
        });
        tokio::spawn(async move {
            let refresh_timeout = inner.refresh_timeout;
            let outcome = run_scoped_build(&inner, scope, build, started).await;
            let duration_ms = started.elapsed().as_millis() as u64;
            let timed_out_waiters =
                timed_out.load(std::sync::atomic::Ordering::Relaxed);
            let (outcome_str, served_rows, refresh_count) =
                match (&outcome.snapshot, &outcome.error) {
                    (Some(snapshot), _) => (
                        "ok".to_string(),
                        snapshot.wire.summaries.len() as u64,
                        snapshot.refresh_count,
                    ),
                    (None, Some(error)) => (error.safe_code(), 0, 0),
                    (None, None) => ("backend".to_string(), 0, 0),
                };
            let failed = outcome.snapshot.is_none();
            let slow = Duration::from_millis(duration_ms) > refresh_timeout;
            if failed || slow {
                tracing::warn!(
                    target: FLEET_READS_TRACE_TARGET,
                    scope = scope_name(scope),
                    outcome = outcome_str.as_str(),
                    duration_ms,
                    served_rows,
                    refresh_count,
                    timed_out_waiters,
                    "fleet snapshot build finished",
                );
            } else {
                tracing::info!(
                    target: FLEET_READS_TRACE_TARGET,
                    scope = scope_name(scope),
                    outcome = outcome_str.as_str(),
                    duration_ms,
                    served_rows,
                    refresh_count,
                    timed_out_waiters,
                    "fleet snapshot build finished",
                );
            }
            {
                let mut state = match scope_state_for(&inner, scope).lock() {
                    Ok(guard) => guard,
                    Err(poisoned) => poisoned.into_inner(),
                };
                let current =
                    state.in_flight.as_ref().is_some_and(|in_flight| {
                        in_flight.generation == generation
                    });
                if current {
                    commit_scoped_outcome(&mut state, &outcome);
                    state.in_flight = None;
                }
            }
            let _ = sender.send(Some(outcome));
        });
    }

    /// Wait at most `refresh_timeout` for the joined build. Returns `None`
    /// when a forced caller must loop and join or start a newer build;
    /// every other path returns its response directly. Timeouts only bump
    /// the shared waiter counter; they are never logged per request.
    async fn wait_for_build(
        &self,
        scope: SnapshotScope,
        receiver: watch::Receiver<Option<RefreshOutcome>>,
        timed_out: Arc<AtomicU64>,
        force: bool,
        force_since: Instant,
        joins: &mut u32,
    ) -> Result<Option<CachedFleetSnapshot>, FleetReadError> {
        let mut receiver = receiver;
        let wait_start = Instant::now();
        let outcome = tokio::time::timeout(
            self.inner.refresh_timeout,
            receiver.wait_for(|slot| slot.is_some()),
        )
        .await
        .map(|waited| waited.is_ok());
        match outcome {
            Ok(true) => {
                let completed = receiver.borrow().as_ref().cloned();
                match completed {
                    Some(outcome) if outcome.snapshot.is_some() => {
                        if !force || outcome.started >= force_since {
                            Ok(outcome.snapshot)
                        } else if *joins >= FORCE_REFRESH_MAX_JOINS {
                            Ok(Some(self.timeout_response(scope, wait_start)?))
                        } else {
                            *joins += 1;
                            Ok(None)
                        }
                    }
                    Some(outcome) => {
                        let latest = self.cached_scoped_snapshot(scope)?;
                        Ok(Some(match latest {
                            Some(snapshot) => mark_stale(
                                snapshot,
                                outcome
                                    .error
                                    .as_ref()
                                    .map(FleetReadError::safe_code)
                                    .unwrap_or_else(|| "backend".to_string()),
                            ),
                            None => {
                                return Err(outcome.error.unwrap_or_else(
                                    || {
                                        FleetReadError::Backend(
                                            "snapshot_join".to_string(),
                                        )
                                    },
                                ));
                            }
                        }))
                    }
                    None => {
                        let latest = self.cached_scoped_snapshot(scope)?;
                        Ok(Some(match latest {
                            Some(snapshot) => {
                                mark_stale(snapshot, "backend".to_string())
                            }
                            None => {
                                return Err(FleetReadError::Backend(
                                    "snapshot_join".to_string(),
                                ));
                            }
                        }))
                    }
                }
            }
            Ok(false) => {
                let latest = self.cached_scoped_snapshot(scope)?;
                Ok(Some(match latest {
                    Some(snapshot) => {
                        mark_stale(snapshot, "backend".to_string())
                    }
                    None => {
                        return Err(FleetReadError::Backend(
                            "snapshot_join".to_string(),
                        ));
                    }
                }))
            }
            Err(_) => {
                timed_out.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if !force {
                    return Ok(Some(self.timeout_response(scope, wait_start)?));
                }
                if *joins >= FORCE_REFRESH_MAX_JOINS {
                    return Ok(Some(self.timeout_response(scope, wait_start)?));
                }
                *joins += 1;
                Ok(None)
            }
        }
    }

    /// Timeout fallback: serve the build that landed during our wait when
    /// one did, otherwise the previous snapshot marked stale, or a timeout
    /// error on a cold cache. The cached entry is never rewritten.
    fn timeout_response(
        &self,
        scope: SnapshotScope,
        wait_start: Instant,
    ) -> Result<CachedFleetSnapshot, FleetReadError> {
        let latest = self.cached_scoped_snapshot(scope)?;
        match latest {
            Some(snapshot) if snapshot.build_instant >= wait_start => {
                Ok(snapshot)
            }
            Some(snapshot) => Ok(mark_stale(snapshot, "timeout".to_string())),
            None => {
                Err(FleetReadError::Timeout("snapshot_refresh".to_string()))
            }
        }
    }

    fn scope_state(&self, scope: SnapshotScope) -> &Mutex<ScopeState> {
        scope_state_for(&self.inner, scope)
    }

    fn cached_scoped_snapshot(
        &self,
        scope: SnapshotScope,
    ) -> Result<Option<CachedFleetSnapshot>, FleetReadError> {
        self.scope_state(scope)
            .lock()
            .map_err(|_| FleetReadError::Backend("snapshot_cache".to_string()))
            .map(|state| state.cache.clone())
    }

    /// The cached snapshot, unless it has reached the stale threshold: an
    /// aged-out entry is treated as a cache miss so the caller falls through
    /// to a genuine rebuild attempt instead of serving a frozen snapshot
    /// forever.
    fn unexpired_scoped_snapshot(
        &self,
        scope: SnapshotScope,
    ) -> Result<Option<CachedFleetSnapshot>, FleetReadError> {
        let Some(snapshot) = self.cached_scoped_snapshot(scope)? else {
            return Ok(None);
        };
        if snapshot.build_instant.elapsed().as_secs_f64()
            >= FLEET_SNAPSHOT_STALE_SECONDS
        {
            return Ok(None);
        }
        Ok(Some(snapshot))
    }

    /// Best-effort overlay of settled launches that are missing from the
    /// snapshot. The pass never waits for an index permit: it skips when
    /// none is free. At most one overlay query runs at a time and the
    /// result is memoized per snapshot, so a burst of catalog pages runs
    /// at most one index query.
    async fn overlay_missing_into_snapshot(
        &self,
        snapshot: &mut CachedFleetSnapshot,
    ) {
        let now_unix = current_unix_time();
        let store = FleetLaunchStore::new(self.inner.sase_home.clone());
        let settled = store.settled_unexpired(now_unix);
        if settled.is_empty() {
            return;
        }
        let mut missing: Vec<String> = Vec::new();
        for receipt in &settled {
            let Some(locator) = receipt.logical_locator.as_ref() else {
                continue;
            };
            let Ok(key) = logical_locator_key(locator) else {
                continue;
            };
            if !snapshot.summaries_by_logical_key.contains_key(&key)
                && !missing.contains(&key)
            {
                missing.push(key);
            }
        }
        if missing.is_empty() {
            return;
        }
        missing.sort();
        let memo_key = (
            snapshot.refresh_count,
            snapshot.wire.catalog_scope,
            missing.clone(),
        );
        {
            let Ok(mut overlay) = self.inner.overlay.lock() else {
                return;
            };
            if overlay.memo_key.as_ref() == Some(&memo_key) {
                let details = overlay.memo_details.clone();
                drop(overlay);
                merge_overlaid_details(snapshot, &details);
                return;
            }
            if overlay.in_flight {
                tracing::debug!(
                    target: FLEET_READS_TRACE_TARGET,
                    reason = "coalescing_slot_busy",
                    "fleet overlay skipped",
                );
                return;
            }
            overlay.in_flight = true;
        }
        let permit = match self.inner.index_permits.clone().try_acquire_owned()
        {
            Ok(permit) => permit,
            Err(_) => {
                if let Ok(mut overlay) = self.inner.overlay.lock() {
                    overlay.in_flight = false;
                }
                tracing::debug!(
                    target: FLEET_READS_TRACE_TARGET,
                    reason = "index_contention",
                    "fleet overlay skipped",
                );
                return;
            }
        };
        #[cfg(test)]
        self.inner.overlay_queries.fetch_add(1, Ordering::Relaxed);
        let sase_home = self.inner.sase_home.clone();
        let index_path = self.inner.index_path.clone();
        let projects_root = self.inner.projects_root.clone();
        let liveness = Arc::clone(&self.inner.liveness);
        let owner_files = Arc::clone(&self.inner.owner_files);
        let freshness = snapshot.wire.freshness.freshness;
        let overlaid = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            overlay_missing_blocking(
                &sase_home,
                &index_path,
                &projects_root,
                liveness,
                owner_files,
                &missing,
                freshness,
            )
        })
        .await
        .ok()
        .and_then(|result| result.ok())
        .unwrap_or_default();
        if let Ok(mut overlay) = self.inner.overlay.lock() {
            overlay.in_flight = false;
            overlay.memo_key = Some(memo_key);
            overlay.memo_details = overlaid.clone();
        }
        merge_overlaid_details(snapshot, &overlaid);
    }
}

/// One synchronous decision of the refresh loop, made under the scope
/// mutex and acted on after it is released.
enum RefreshDecision {
    Join {
        receiver: watch::Receiver<Option<RefreshOutcome>>,
        timed_out: Arc<AtomicU64>,
    },
    Start {
        sender: watch::Sender<Option<RefreshOutcome>>,
        receiver: watch::Receiver<Option<RefreshOutcome>>,
        generation: u64,
        started: Instant,
        build: BuildSnapshotRequest,
        timed_out: Arc<AtomicU64>,
    },
    Backoff {
        code: String,
        error: FleetReadError,
        consecutive_failures: u32,
        window_secs: u64,
    },
}

fn scope_state_for(
    inner: &FleetReadServiceInner,
    scope: SnapshotScope,
) -> &Mutex<ScopeState> {
    match scope {
        SnapshotScope::Presentation => &inner.presentation,
        SnapshotScope::History => &inner.history,
    }
}

/// Run one snapshot build while holding a gateway-wide index permit for
/// the whole blocking build. Successful Presentation builds checkpoint an
/// oversized index WAL best-effort while still holding the permit.
async fn run_scoped_build(
    inner: &Arc<FleetReadServiceInner>,
    scope: SnapshotScope,
    build: BuildSnapshotRequest,
    started: Instant,
) -> RefreshOutcome {
    let permit = inner.index_permits.clone().acquire_owned().await;
    let Ok(_permit) = permit else {
        return RefreshOutcome {
            started,
            snapshot: None,
            error: Some(FleetReadError::Backend("index_permits".to_string())),
        };
    };
    let index_path = build.index_path.clone();
    let is_presentation = matches!(scope, SnapshotScope::Presentation);
    #[cfg(test)]
    let hook_inner = Arc::clone(inner);
    let result = tokio::task::spawn_blocking(move || {
        let snapshot = build_snapshot_blocking(build)?;
        if is_presentation {
            #[cfg(test)]
            hook_inner
                .checkpoint_hook_calls
                .fetch_add(1, Ordering::Relaxed);
            match checkpoint_agent_artifact_index_wal_if_oversized(
                &index_path,
                AGENT_ARTIFACT_INDEX_WAL_SIZE_LIMIT_BYTES,
            ) {
                Ok(outcome) if outcome.checkpoint_attempted => {
                    tracing::info!(
                        target: FLEET_READS_TRACE_TARGET,
                        wal_bytes_before = outcome.wal_bytes_before,
                        wal_bytes_after = outcome.wal_bytes_after,
                        "fleet index WAL checkpointed",
                    );
                }
                Ok(_) => {}
                Err(error) => {
                    tracing::warn!(
                        target: FLEET_READS_TRACE_TARGET,
                        error = error.as_str(),
                        "fleet index WAL checkpoint failed",
                    );
                }
            }
        }
        Ok::<_, FleetReadError>(snapshot)
    })
    .await;
    match result {
        Ok(Ok(snapshot)) => RefreshOutcome {
            started,
            snapshot: Some(snapshot),
            error: None,
        },
        Ok(Err(error)) => RefreshOutcome {
            started,
            snapshot: None,
            error: Some(error),
        },
        Err(_) => RefreshOutcome {
            started,
            snapshot: None,
            error: Some(FleetReadError::Backend("snapshot_join".to_string())),
        },
    }
}

/// Record a finished build: a success fills the cache and resets the
/// failure tracking, while a failure records the error and extends the
/// back-off schedule. A late-but-successful build is a success.
fn commit_scoped_outcome(state: &mut ScopeState, outcome: &RefreshOutcome) {
    if let Some(snapshot) = outcome.snapshot.clone() {
        state.cache = Some(snapshot);
        state.last_failure_at = None;
        state.last_error = None;
        state.consecutive_failures = 0;
    } else {
        state.last_failure_at = Some(Instant::now());
        state.consecutive_failures =
            state.consecutive_failures.saturating_add(1);
        if let Some(error) = outcome.error.clone() {
            state.last_error = Some(error);
        }
    }
}

/// Mark a returned copy stale without touching the cached entry's
/// `build_instant`, so the cache keeps its real age.
fn mark_stale(
    mut snapshot: CachedFleetSnapshot,
    code: String,
) -> CachedFleetSnapshot {
    snapshot.wire.freshness.freshness = ObservationFreshnessWire::Stale;
    snapshot.wire.freshness.partial = true;
    snapshot.wire.freshness.error = Some(code);
    snapshot
}

fn merge_overlaid_details(
    snapshot: &mut CachedFleetSnapshot,
    overlaid: &[ResolvedAgentDetailWire],
) {
    for detail in overlaid {
        let key = detail.summary.logical_key.clone();
        if snapshot.summaries_by_logical_key.contains_key(&key) {
            continue;
        }
        snapshot.wire.summaries.push(detail.summary.clone());
        snapshot
            .summaries_by_logical_key
            .insert(key.clone(), detail.summary.clone());
        snapshot.details_by_logical_key.insert(key, detail.clone());
    }
    snapshot.wire.summaries.sort_by(|left, right| {
        left.logical_key
            .cmp(&right.logical_key)
            .then_with(|| left.exact_key.cmp(&right.exact_key))
    });
}

fn overlay_missing_blocking(
    sase_home: &Path,
    index_path: &Path,
    projects_root: &Path,
    liveness: Arc<dyn OwnerLivenessObserver>,
    owner_files: Arc<dyn OwnerFileObserver>,
    missing: &[String],
    freshness: ObservationFreshnessWire,
) -> Result<
    Vec<sase_core::fleet_contract::ResolvedAgentDetailWire>,
    FleetReadError,
> {
    use sase_core::fleet_contract::FleetCatalogScopeWire;
    use std::collections::BTreeMap;

    let installation = ensure_installation_identity(sase_home)
        .map_err(FleetReadError::from)?
        .record;
    let scan = query_agent_artifact_index(
        index_path,
        projects_root,
        AgentArtifactIndexQueryWire {
            include_active: true,
            include_recent_completed: true,
            include_full_history: false,
            active_limit: None,
            recent_completed_limit: Some(512),
            include_hidden: false,
            freshness: AgentArtifactIndexFreshnessWire::Cached,
            only_monitors: false,
            record_shape: AgentArtifactRecordShapeWire::Full,
            window_limit: Some(512),
            candidate_filter: None,
            agents_list_projection: false,
        },
        AgentArtifactScanOptionsWire {
            max_prompt_snippet_bytes: 512,
            ..AgentArtifactScanOptionsWire::default()
        },
    )
    .map_err(|_| FleetReadError::Backend("artifact_index".to_string()))?;
    let now_unix = current_unix_time();
    let project_labels = list_project_records(projects_root, &[], false, true)
        .map_err(|_| FleetReadError::Backend("project_lifecycle".to_string()))?
        .into_iter()
        .map(|record| {
            let name = record.project_name.clone();
            let display = record.display_name.unwrap_or_else(|| name.clone());
            (name, display)
        })
        .collect::<BTreeMap<_, _>>();
    let observation_by_identity: BTreeMap<String, _> = scan
        .records
        .iter()
        .map(|record| (record.artifact_dir.clone(), liveness.observe(record)))
        .collect();
    let lineage_candidates: Vec<AgentSessionDismissalLineageCandidateWire> =
        scan.records
            .iter()
            .map(|record| {
                let seed_dead = matches!(
                    observation_by_identity
                        .get(&record.artifact_dir)
                        .map(|observation| observation.liveness()),
                    Some(
                        OwnerLivenessWire::Dead | OwnerLivenessWire::NotProcess
                    )
                );
                AgentSessionDismissalLineageCandidateWire {
                    identity: record.artifact_dir.clone(),
                    project_name: record.project_name.clone(),
                    workflow_dir_name: record.workflow_dir_name.clone(),
                    timestamp: record.timestamp.clone(),
                    seed_definitively_dead: seed_dead,
                }
            })
            .collect();
    let dismissed_by_identity: BTreeMap<String, bool> =
        resolve_agent_session_dismissal_lineage(
            index_path,
            &lineage_candidates,
        )
        .map_err(|_| {
            FleetReadError::Backend(
                "agent_session_dismissal_lineage".to_string(),
            )
        })?
        .into_iter()
        .map(|result| (result.identity, result.agent_session_root_dismissed))
        .collect();
    let selection = select_fleet_presentation(
        &scan.records,
        &observation_by_identity,
        &dismissed_by_identity,
        now_unix,
        FleetCatalogScopeWire::Presentation,
        owner_files.as_ref(),
    )
    .map_err(FleetReadError::from)?;
    let mut overlaid = Vec::new();
    for record in &scan.records {
        if !selection.served.contains(&record.artifact_dir) {
            continue;
        }
        let liveness_value = observation_by_identity
            .get(&record.artifact_dir)
            .map(|observation| observation.liveness())
            .unwrap_or(OwnerLivenessWire::Unknown);
        let Ok(mut resolved) = resolve_record(
            &installation.installation_id,
            record,
            liveness_value,
            now_unix,
            &project_labels,
            &selection.context.facts_for_record(record),
        ) else {
            continue;
        };
        if !missing.contains(&resolved.detail.summary.logical_key) {
            continue;
        }
        resolved.detail.summary.freshness = freshness;
        overlaid.push(resolved.detail);
    }
    Ok(overlaid)
}

fn project_eligibility_blocking(
    projects_root: &Path,
    requested: &[String],
    limit: u32,
) -> Result<FleetProjectEligibilityResponseWire, FleetReadError> {
    let records = list_project_records(projects_root, &[], false, true)
        .map_err(|_| {
            FleetReadError::Backend("project_lifecycle".to_string())
        })?;
    let requested = requested.iter().cloned().collect::<BTreeSet<_>>();
    let mut projects = records
        .into_iter()
        .filter(|record| {
            requested.is_empty()
                || requested.contains(&record.project_name)
                || record.aliases.iter().any(|alias| requested.contains(alias))
        })
        .map(|record| {
            let reason = if record.launchable {
                None
            } else {
                record
                    .warnings
                    .first()
                    .cloned()
                    .or_else(|| Some("not_launchable".to_string()))
            };
            FleetProjectEligibilityWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                project_id: record.project_name,
                display_name: record.display_name,
                state: record.state,
                eligible: record.launchable,
                launchable: record.launchable,
                active_claim_count: record.active_claim_count,
                reason: reason.map(|value| safe_reason(&value)),
            }
        })
        .collect::<Vec<_>>();
    projects.sort_by(|left, right| left.project_id.cmp(&right.project_id));
    let total_matching_projects = projects.len() as u64;
    let limit = limit as usize;
    let truncated = projects.len() > limit;
    projects.truncate(limit);
    Ok(FleetProjectEligibilityResponseWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        projects,
        limit: limit as u32,
        total_matching_projects,
        truncated,
    })
}
