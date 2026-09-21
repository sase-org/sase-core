//! `FleetReadService`: snapshot-backed fleet read APIs with refresh
//! coalescing and stale retention.

use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};

use sase_core::{
    fleet_contract::{
        fleet_content_read_limit, fleet_project_eligibility_limit,
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
        ResourceRevisionWire, StoreCursorWire, FLEET_CONTRACT_SCHEMA_VERSION,
        FLEET_READ_DEFAULT_REPLAY_EVENTS,
    },
    fleet_owner_facts::{HostOwnerFileObserver, OwnerFileObserver},
    host_liveness::{HostOwnerLivenessObserver, OwnerLivenessObserver},
    list_project_records,
};
use tokio::sync::Mutex as AsyncMutex;

use super::{
    content::read_content_range,
    errors::FleetReadError,
    invalidation::{FleetEventSubscription, FleetInvalidationHub},
    resolution::{
        observation_freshness_for_age, safe_reason,
        FLEET_SNAPSHOT_STALE_SECONDS,
    },
    snapshot::{
        build_snapshot_blocking, BuildSnapshotRequest, CachedFleetSnapshot,
    },
};

pub(super) const SNAPSHOT_REFRESH_TIMEOUT: Duration = Duration::from_secs(4);

#[derive(Clone)]
pub struct FleetReadService {
    inner: Arc<FleetReadServiceInner>,
}

struct FleetReadServiceInner {
    sase_home: PathBuf,
    index_path: PathBuf,
    projects_root: PathBuf,
    refresh_timeout: Duration,
    cache: Mutex<Option<CachedFleetSnapshot>>,
    refresh_lock: AsyncMutex<()>,
    history_cache: Mutex<Option<CachedFleetSnapshot>>,
    history_refresh_lock: AsyncMutex<()>,
    events: FleetInvalidationHub,
    liveness: Arc<dyn OwnerLivenessObserver>,
    owner_files: Arc<dyn OwnerFileObserver>,
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
                cache: Mutex::new(None),
                refresh_lock: AsyncMutex::new(()),
                history_cache: Mutex::new(None),
                history_refresh_lock: AsyncMutex::new(()),
                events: FleetInvalidationHub::new(
                    FLEET_READ_DEFAULT_REPLAY_EVENTS,
                )
                .expect("default fleet replay capacity is valid"),
                liveness,
                owner_files: Arc::new(HostOwnerFileObserver::default()),
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
        let catalog_snapshot = match query.scope {
            FleetCatalogScopeWire::Presentation => presentation.clone(),
            FleetCatalogScopeWire::History => {
                self.stamped_history_snapshot(false).await?
            }
        };
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
        let snapshot = self.stamped_snapshot(false).await?;
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
            .cache
            .lock()
            .ok()
            .and_then(|cache| {
                cache.as_ref().map(|snapshot| snapshot.refresh_count)
            })
            .unwrap_or(0)
    }

    #[cfg(test)]
    pub fn history_refresh_count_for_test(&self) -> u64 {
        self.inner
            .history_cache
            .lock()
            .ok()
            .and_then(|cache| {
                cache.as_ref().map(|snapshot| snapshot.refresh_count)
            })
            .unwrap_or(0)
    }

    #[cfg(test)]
    pub fn history_cache_initialized_for_test(&self) -> bool {
        self.inner
            .history_cache
            .lock()
            .ok()
            .and_then(|cache| cache.as_ref().map(|_| ()))
            .is_some()
    }

    /// Push the cached snapshot's build instant backward so it reads as
    /// older than it really is, without a wall-clock sleep.
    #[cfg(test)]
    pub fn age_cache_for_test(&self, seconds: f64) {
        if let Ok(mut cache) = self.inner.cache.lock() {
            if let Some(snapshot) = cache.as_mut() {
                snapshot.build_instant -= Duration::from_secs_f64(seconds);
            }
        }
    }

    #[cfg(test)]
    pub fn age_history_cache_for_test(&self, seconds: f64) {
        if let Ok(mut cache) = self.inner.history_cache.lock() {
            if let Some(snapshot) = cache.as_mut() {
                snapshot.build_instant -= Duration::from_secs_f64(seconds);
            }
        }
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
        if !force {
            if let Some(snapshot) = self.unexpired_cached_snapshot()? {
                return Ok(snapshot);
            }
        }
        let _guard = self.inner.refresh_lock.lock().await;
        if !force {
            if let Some(snapshot) = self.unexpired_cached_snapshot()? {
                return Ok(snapshot);
            }
        }
        let previous = self.cached_snapshot()?;
        let build = BuildSnapshotRequest {
            sase_home: self.inner.sase_home.clone(),
            index_path: self.inner.index_path.clone(),
            projects_root: self.inner.projects_root.clone(),
            cursor: self.inner.events.current_cursor(),
            prior_refresh_count: previous
                .as_ref()
                .map(|snapshot| snapshot.refresh_count)
                .unwrap_or(0),
            scope: FleetCatalogScopeWire::Presentation,
            liveness: Arc::clone(&self.inner.liveness),
            owner_files: Arc::clone(&self.inner.owner_files),
        };
        let result = tokio::time::timeout(
            self.inner.refresh_timeout,
            tokio::task::spawn_blocking(move || build_snapshot_blocking(build)),
        )
        .await;
        let snapshot = match result {
            Ok(Ok(Ok(snapshot))) => snapshot,
            Ok(Ok(Err(error))) => {
                return self.retain_previous_or_error(previous, error)
            }
            Ok(Err(_)) => {
                return self.retain_previous_or_error(
                    previous,
                    FleetReadError::Backend("snapshot_join".to_string()),
                )
            }
            Err(_) => {
                return self.retain_previous_or_error(
                    previous,
                    FleetReadError::Timeout("snapshot_refresh".to_string()),
                )
            }
        };
        *self.inner.cache.lock().map_err(|_| {
            FleetReadError::Backend("snapshot_cache".to_string())
        })? = Some(snapshot.clone());
        Ok(snapshot)
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
        if !force {
            if let Some(snapshot) = self.unexpired_cached_history_snapshot()? {
                return Ok(snapshot);
            }
        }
        let _guard = self.inner.history_refresh_lock.lock().await;
        if !force {
            if let Some(snapshot) = self.unexpired_cached_history_snapshot()? {
                return Ok(snapshot);
            }
        }
        let previous = self.cached_history_snapshot()?;
        let build = BuildSnapshotRequest {
            sase_home: self.inner.sase_home.clone(),
            index_path: self.inner.index_path.clone(),
            projects_root: self.inner.projects_root.clone(),
            cursor: self.inner.events.current_cursor(),
            prior_refresh_count: previous
                .as_ref()
                .map(|snapshot| snapshot.refresh_count)
                .unwrap_or(0),
            scope: FleetCatalogScopeWire::History,
            liveness: Arc::clone(&self.inner.liveness),
            owner_files: Arc::clone(&self.inner.owner_files),
        };
        let result = tokio::time::timeout(
            self.inner.refresh_timeout,
            tokio::task::spawn_blocking(move || build_snapshot_blocking(build)),
        )
        .await;
        let snapshot = match result {
            Ok(Ok(Ok(snapshot))) => snapshot,
            Ok(Ok(Err(error))) => {
                return self.retain_previous_history_or_error(previous, error)
            }
            Ok(Err(_)) => {
                return self.retain_previous_history_or_error(
                    previous,
                    FleetReadError::Backend("snapshot_join".to_string()),
                )
            }
            Err(_) => {
                return self.retain_previous_history_or_error(
                    previous,
                    FleetReadError::Timeout("snapshot_refresh".to_string()),
                )
            }
        };
        *self.inner.history_cache.lock().map_err(|_| {
            FleetReadError::Backend("history_snapshot_cache".to_string())
        })? = Some(snapshot.clone());
        Ok(snapshot)
    }

    fn cached_snapshot(
        &self,
    ) -> Result<Option<CachedFleetSnapshot>, FleetReadError> {
        self.inner
            .cache
            .lock()
            .map_err(|_| FleetReadError::Backend("snapshot_cache".to_string()))
            .map(|snapshot| snapshot.clone())
    }

    fn cached_history_snapshot(
        &self,
    ) -> Result<Option<CachedFleetSnapshot>, FleetReadError> {
        self.inner
            .history_cache
            .lock()
            .map_err(|_| {
                FleetReadError::Backend("history_snapshot_cache".to_string())
            })
            .map(|snapshot| snapshot.clone())
    }

    /// The cached snapshot, unless it has reached the stale threshold: an
    /// aged-out entry is treated as a cache miss so the caller falls through
    /// to a genuine rebuild attempt instead of serving a frozen snapshot
    /// forever.
    fn unexpired_cached_snapshot(
        &self,
    ) -> Result<Option<CachedFleetSnapshot>, FleetReadError> {
        let Some(snapshot) = self.cached_snapshot()? else {
            return Ok(None);
        };
        if snapshot.build_instant.elapsed().as_secs_f64()
            >= FLEET_SNAPSHOT_STALE_SECONDS
        {
            return Ok(None);
        }
        Ok(Some(snapshot))
    }

    fn unexpired_cached_history_snapshot(
        &self,
    ) -> Result<Option<CachedFleetSnapshot>, FleetReadError> {
        let Some(snapshot) = self.cached_history_snapshot()? else {
            return Ok(None);
        };
        if snapshot.build_instant.elapsed().as_secs_f64()
            >= FLEET_SNAPSHOT_STALE_SECONDS
        {
            return Ok(None);
        }
        Ok(Some(snapshot))
    }

    fn retain_previous_or_error(
        &self,
        previous: Option<CachedFleetSnapshot>,
        error: FleetReadError,
    ) -> Result<CachedFleetSnapshot, FleetReadError> {
        let Some(mut snapshot) = previous else {
            return Err(error);
        };
        snapshot.wire.freshness.freshness = ObservationFreshnessWire::Stale;
        snapshot.wire.freshness.partial = true;
        snapshot.wire.freshness.error = Some(error.safe_code());
        *self.inner.cache.lock().map_err(|_| {
            FleetReadError::Backend("snapshot_cache".to_string())
        })? = Some(snapshot.clone());
        Ok(snapshot)
    }

    fn retain_previous_history_or_error(
        &self,
        previous: Option<CachedFleetSnapshot>,
        error: FleetReadError,
    ) -> Result<CachedFleetSnapshot, FleetReadError> {
        let Some(mut snapshot) = previous else {
            return Err(error);
        };
        snapshot.wire.freshness.freshness = ObservationFreshnessWire::Stale;
        snapshot.wire.freshness.partial = true;
        snapshot.wire.freshness.error = Some(error.safe_code());
        *self.inner.history_cache.lock().map_err(|_| {
            FleetReadError::Backend("history_snapshot_cache".to_string())
        })? = Some(snapshot.clone());
        Ok(snapshot)
    }
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
