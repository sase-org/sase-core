use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    fs,
    io::{Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use chrono::{NaiveDateTime, Utc};
use sase_core::{
    agent_scan::{
        AgentArtifactIndexFreshnessWire, AgentArtifactIndexQueryWire,
        AgentArtifactRecordShapeWire, AgentArtifactRecordWire,
        AgentArtifactScanOptionsWire, AgentMetaWire, DoneMarkerWire,
        FamilyDismissalLineageCandidateWire, FamilyShellWire,
    },
    fleet_attention::{
        FLEET_ATTENTION_CAPABILITY_ANSWER_QUESTION,
        FLEET_ATTENTION_CAPABILITY_APPROVE_GATE,
    },
    fleet_contract::{
        classify_cache_freshness, classify_cursor_replay, count_logical_agents,
        cursor_replay_reason_to_resync_reason, ensure_installation_identity,
        fleet_content_read_limit, fleet_count_revision,
        fleet_project_eligibility_limit, instance_locator_key,
        logical_locator_key, project_resolved_agent_detail,
        select_fleet_catalog_page, select_fleet_logical_batch,
        validate_fleet_authoritative_snapshot,
        validate_fleet_content_read_request, validate_fleet_detail_request,
        validate_fleet_invalidation_event,
        validate_fleet_logical_batch_request,
        validate_fleet_project_eligibility_request,
        validate_fleet_replay_capacity, AgentInstanceLocatorWire,
        CacheFreshnessRequestWire, CapabilitySetWire, ConnectionHealthWire,
        ContentHandleKindWire, ContentHandleWire,
        CursorReplayClassificationWire, CursorReplayRequestWire,
        FleetAuthoritativeSnapshotWire, FleetCatalogPageWire,
        FleetCatalogQueryWire, FleetContentReadRequestWire,
        FleetContentReadResponseWire, FleetContractError,
        FleetDetailRequestWire, FleetDetailResponseWire,
        FleetEventStreamItemWire, FleetInvalidationEventWire,
        FleetInvalidationKindWire, FleetLogicalAgentCountsRequestWire,
        FleetLogicalBatchRequestWire, FleetLogicalBatchResponseWire,
        FleetProjectEligibilityRequestWire,
        FleetProjectEligibilityResponseWire, FleetProjectEligibilityWire,
        FleetResyncReasonWire, FleetResyncRequiredWire, FleetRowKindWire,
        FleetSnapshotFreshnessWire, FleetSummaryResponseWire,
        LogicalAgentLocatorWire, ObservationFreshnessWire, OriginLocatorWire,
        OwnerLivenessWire, OwnerResolutionFactsWire, ProjectLocatorWire,
        ResolvedAgentDetailWire, ResolvedAgentProjectionRequestWire,
        ResourceRevisionWire, StoreCursorWire, FLEET_CONTRACT_SCHEMA_VERSION,
        FLEET_INITIAL_CURSOR_GENERATION, FLEET_READ_DEFAULT_REPLAY_EVENTS,
    },
    fleet_mutation::{
        FLEET_MUTATION_CAPABILITY_FORK, FLEET_MUTATION_CAPABILITY_RETRY,
        FLEET_MUTATION_CAPABILITY_STOP,
    },
    fleet_presentation::{
        decide_fleet_presentation, FleetPresentationCandidateWire,
        FleetPresentationRequestWire,
    },
    list_project_records, query_agent_artifact_index,
    resolve_family_dismissal_lineage,
};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::sync::{broadcast, Mutex as AsyncMutex};

const SNAPSHOT_REFRESH_TIMEOUT: Duration = Duration::from_secs(4);
const FLEET_EVENT_BROADCAST_CAPACITY: usize = 256;
/// Below this age a served snapshot reads as `Fresh`.
const FLEET_SNAPSHOT_FRESH_SECONDS: f64 = 5.0;
/// At or beyond this age a served snapshot reads as `Stale` and a read that
/// is not already holding the refresh lock requires a rebuild attempt
/// instead of serving the cached entry outright.
const FLEET_SNAPSHOT_STALE_SECONDS: f64 = 60.0;

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
    events: FleetInvalidationHub,
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
        let sase_home = sase_home.into();
        Self {
            inner: Arc::new(FleetReadServiceInner {
                index_path: sase_home.join("agent_artifact_index.sqlite"),
                projects_root: sase_home.join("projects"),
                sase_home,
                refresh_timeout,
                cache: Mutex::new(None),
                refresh_lock: AsyncMutex::new(()),
                events: FleetInvalidationHub::new(
                    FLEET_READ_DEFAULT_REPLAY_EVENTS,
                )
                .expect("default fleet replay capacity is valid"),
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
            counts: snapshot.wire.counts,
            count_revision: snapshot.wire.count_revision,
            freshness: snapshot.wire.freshness,
        })
    }

    pub async fn catalog(
        &self,
        query: FleetCatalogQueryWire,
    ) -> Result<FleetCatalogPageWire, FleetReadError> {
        let snapshot = self.stamped_snapshot(false).await?;
        let page = select_fleet_catalog_page(&query, &snapshot.wire.summaries)
            .map_err(FleetReadError::from)?;
        Ok(FleetCatalogPageWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            cursor: snapshot.wire.cursor,
            counts: snapshot.wire.counts,
            count_revision: snapshot.wire.count_revision,
            freshness: snapshot.wire.freshness,
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

    fn cached_snapshot(
        &self,
    ) -> Result<Option<CachedFleetSnapshot>, FleetReadError> {
        self.inner
            .cache
            .lock()
            .map_err(|_| FleetReadError::Backend("snapshot_cache".to_string()))
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
}

#[derive(Clone, Debug)]
struct CachedFleetSnapshot {
    wire: FleetAuthoritativeSnapshotWire,
    summaries_by_logical_key:
        BTreeMap<String, sase_core::fleet_contract::ResolvedAgentSummaryWire>,
    details_by_logical_key: BTreeMap<String, ResolvedAgentDetailWire>,
    content_by_handle: BTreeMap<String, FleetContentSource>,
    refresh_count: u64,
    /// Monotonic instant this snapshot was built (or last successfully
    /// rebuilt). Every row's `observed_at_unix` and the envelope's
    /// `refreshed_at_unix` are stamped from one build timestamp at the same
    /// moment; this field is the seam that lets a later read derive honest
    /// freshness from actual elapsed time instead of trusting a stored
    /// label.
    build_instant: Instant,
}

#[derive(Clone, Debug)]
struct FleetContentSource {
    handle: ContentHandleWire,
    row_revision: ResourceRevisionWire,
    canonical_path: PathBuf,
    artifact_root: PathBuf,
}

#[derive(Debug)]
struct BuildSnapshotRequest {
    sase_home: PathBuf,
    index_path: PathBuf,
    projects_root: PathBuf,
    cursor: StoreCursorWire,
    prior_refresh_count: u64,
}

#[derive(Debug, Error)]
pub enum FleetReadError {
    #[error("{0}")]
    Validation(String),
    #[error("fleet resource not found: {0}")]
    NotFound(String),
    #[error("fleet resource is stale: {0}")]
    Stale(String),
    #[error("fleet read timed out: {0}")]
    Timeout(String),
    #[error("fleet backend failed: {0}")]
    Backend(String),
}

impl FleetReadError {
    fn safe_code(&self) -> String {
        match self {
            Self::Validation(_) => "validation".to_string(),
            Self::NotFound(_) => "not_found".to_string(),
            Self::Stale(_) => "stale".to_string(),
            Self::Timeout(_) => "timeout".to_string(),
            Self::Backend(_) => "backend".to_string(),
        }
    }
}

impl From<FleetContractError> for FleetReadError {
    fn from(error: FleetContractError) -> Self {
        Self::Validation(error.to_string())
    }
}

fn build_snapshot_blocking(
    request: BuildSnapshotRequest,
) -> Result<CachedFleetSnapshot, FleetReadError> {
    let installation = ensure_installation_identity(&request.sase_home)
        .map_err(FleetReadError::from)?
        .record;
    let scan = query_agent_artifact_index(
        &request.index_path,
        &request.projects_root,
        AgentArtifactIndexQueryWire {
            include_active: true,
            include_recent_completed: true,
            include_full_history: false,
            active_limit: None,
            recent_completed_limit: Some(512),
            include_hidden: false,
            freshness: AgentArtifactIndexFreshnessWire::Revalidate,
            only_monitors: false,
            record_shape: AgentArtifactRecordShapeWire::Full,
            window_limit: Some(512),
            candidate_filter: None,
        },
        AgentArtifactScanOptionsWire {
            max_prompt_snippet_bytes: 512,
            ..AgentArtifactScanOptionsWire::default()
        },
    )
    .map_err(|_| FleetReadError::Backend("artifact_index".to_string()))?;

    let build_instant = Instant::now();
    let now_unix = current_unix_time();

    // Obtain dismissal-lineage facts through the bounded core index API and
    // resolve owner liveness once per candidate, so both are computed a
    // single time per record instead of being re-derived per read path.
    let lineage_candidates: Vec<FamilyDismissalLineageCandidateWire> = scan
        .records
        .iter()
        .map(|record| FamilyDismissalLineageCandidateWire {
            identity: record.artifact_dir.clone(),
            project_name: record.project_name.clone(),
            workflow_dir_name: record.workflow_dir_name.clone(),
            timestamp: record.timestamp.clone(),
        })
        .collect();
    let dismissed_by_identity: BTreeMap<String, bool> =
        resolve_family_dismissal_lineage(
            &request.index_path,
            &lineage_candidates,
        )
        .map_err(|_| {
            FleetReadError::Backend("family_dismissal_lineage".to_string())
        })?
        .into_iter()
        .map(|result| (result.identity, result.family_root_dismissed))
        .collect();

    let mut liveness_by_identity = BTreeMap::new();
    let mut presentation_candidates = Vec::with_capacity(scan.records.len());
    for record in &scan.records {
        let liveness = owner_liveness_for_record(record);
        liveness_by_identity.insert(record.artifact_dir.clone(), liveness);
        presentation_candidates.push(FleetPresentationCandidateWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            identity: record.artifact_dir.clone(),
            liveness,
            protected: record.waiting.is_some()
                || record.pending_question.is_some(),
            completion_time_unix: completion_time_for_record(record),
            family_root_dismissed: dismissed_by_identity
                .get(&record.artifact_dir)
                .copied()
                .unwrap_or(false),
        });
    }
    let decision = decide_fleet_presentation(&FleetPresentationRequestWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        now_unix,
        candidates: presentation_candidates,
    })
    .map_err(FleetReadError::from)?;
    // Select the presentable set, then build details, content handles,
    // summaries, and counts only from that set.
    let presentable: BTreeSet<String> = decision
        .current
        .into_iter()
        .chain(decision.recent_terminal)
        .collect();

    let mut details = Vec::new();
    let mut content_by_handle = BTreeMap::new();
    for record in scan.records {
        if !presentable.contains(&record.artifact_dir) {
            continue;
        }
        let liveness = liveness_by_identity
            .get(&record.artifact_dir)
            .copied()
            .unwrap_or(OwnerLivenessWire::Unknown);
        let resolved = resolve_record(
            &installation.installation_id,
            &record,
            liveness,
            now_unix,
        )?;
        for source in resolved.content_sources {
            content_by_handle.insert(source.handle.id.clone(), source);
        }
        details.push(resolved.detail);
    }
    details.sort_by(|left, right| {
        left.summary
            .logical_key
            .cmp(&right.summary.logical_key)
            .then_with(|| left.summary.exact_key.cmp(&right.summary.exact_key))
    });
    let summaries = details
        .iter()
        .map(|detail| detail.summary.clone())
        .collect::<Vec<_>>();
    let counts = count_logical_agents(&FleetLogicalAgentCountsRequestWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        summaries: summaries.clone(),
    })
    .map_err(FleetReadError::from)?;
    let wire = FleetAuthoritativeSnapshotWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        cursor: request.cursor,
        count_revision: fleet_count_revision(&counts),
        counts,
        summaries,
        freshness: FleetSnapshotFreshnessWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            freshness: ObservationFreshnessWire::Fresh,
            partial: false,
            refreshed_at_unix: Some(now_unix),
            error: None,
        },
    };
    validate_fleet_authoritative_snapshot(&wire)
        .map_err(FleetReadError::from)?;
    let summaries_by_logical_key = wire
        .summaries
        .iter()
        .map(|summary| (summary.logical_key.clone(), summary.clone()))
        .collect::<BTreeMap<_, _>>();
    let details_by_logical_key = details
        .into_iter()
        .map(|detail| (detail.summary.logical_key.clone(), detail))
        .collect::<BTreeMap<_, _>>();
    Ok(CachedFleetSnapshot {
        wire,
        summaries_by_logical_key,
        details_by_logical_key,
        content_by_handle,
        refresh_count: request.prior_refresh_count.saturating_add(1),
        build_instant,
    })
}

struct ResolvedRecord {
    detail: ResolvedAgentDetailWire,
    content_sources: Vec<FleetContentSource>,
}

fn resolve_record(
    installation_id: &str,
    record: &AgentArtifactRecordWire,
    liveness: OwnerLivenessWire,
    build_unix: f64,
) -> Result<ResolvedRecord, FleetReadError> {
    let logical_locator = logical_locator_for_record(installation_id, record);
    let logical_key =
        logical_locator_key(&logical_locator).map_err(FleetReadError::from)?;
    let row_revision = ResourceRevisionWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        logical_key: logical_key.clone(),
        revision: stable_revision(record),
    };
    let exact_locator =
        Some(exact_locator_for_record(logical_locator.clone(), record));
    let exact_key = exact_locator
        .as_ref()
        .map(instance_locator_key)
        .transpose()
        .map_err(FleetReadError::from)?;
    let (content_handles, content_sources) =
        content_handles_for_record(record, &logical_key, &row_revision)?;
    let row_kind = row_kind_for_record(record);
    let is_terminal = record.done.is_some()
        || record.workflow_state.as_ref().is_some_and(|state| {
            matches!(
                state.status.to_ascii_lowercase().as_str(),
                "completed" | "failed" | "cancelled" | "noop"
            )
        });
    // A Dead/NotProcess active-tier record (not yet marked done, not
    // protected by a waiting/question marker) is terminal for presentation:
    // it keeps its recorded lifecycle/status but loses current-instance and
    // action capabilities, the same as a genuinely completed record.
    let protected =
        record.waiting.is_some() || record.pending_question.is_some();
    let presentation_terminal = is_terminal
        || (!protected
            && matches!(
                liveness,
                OwnerLivenessWire::Dead | OwnerLivenessWire::NotProcess
            ));
    let resource_caps = lifecycle_and_content_capabilities(
        row_kind,
        presentation_terminal,
        liveness,
        &content_handles,
        record.pending_question.is_some(),
    );
    let detail =
        project_resolved_agent_detail(&ResolvedAgentProjectionRequestWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            record: record.clone(),
            logical_locator,
            owner_facts: OwnerResolutionFactsWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                exact_locator,
                row_revision,
                liveness,
                connection_health: match liveness {
                    OwnerLivenessWire::Alive => ConnectionHealthWire::Online,
                    OwnerLivenessWire::Dead => ConnectionHealthWire::Offline,
                    OwnerLivenessWire::NotProcess
                    | OwnerLivenessWire::Unknown => {
                        ConnectionHealthWire::Unknown
                    }
                },
                freshness: ObservationFreshnessWire::Fresh,
                observed_at_unix: build_unix,
                row_kind,
                current_instance: !presentation_terminal
                    && row_kind == FleetRowKindWire::AgentShell,
                dismissable: presentation_terminal,
                needs_attention: record.pending_question.is_some(),
                occupied_runner_slot: row_kind == FleetRowKindWire::AgentShell
                    && liveness == OwnerLivenessWire::Alive,
                container_projected_concrete_agent: false,
                capabilities: CapabilitySetWire {
                    schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                    resource: resource_caps,
                    host: Vec::new(),
                    protocol: vec!["fleet.v1".to_string()],
                },
                content_handles,
            },
        })
        .map_err(FleetReadError::from)?;
    if detail.summary.exact_key != exact_key {
        return Err(FleetReadError::Backend("exact_key".to_string()));
    }
    Ok(ResolvedRecord {
        detail,
        content_sources,
    })
}

fn logical_locator_for_record(
    installation_id: &str,
    record: &AgentArtifactRecordWire,
) -> LogicalAgentLocatorWire {
    let meta = record.agent_meta.as_ref();
    LogicalAgentLocatorWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        project: ProjectLocatorWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            origin: OriginLocatorWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                installation_id: installation_id.to_string(),
            },
            project_id: safe_identifier(&record.project_name, "project"),
        },
        agent_id: safe_identifier(
            first_non_empty([
                meta.and_then(|value| value.artifact_agent_id.as_deref()),
                meta.and_then(|value| value.name.as_deref()),
                record.done.as_ref().and_then(|value| value.name.as_deref()),
                family_shell(meta, record.done.as_ref())
                    .and_then(|value| value.id.as_deref()),
                Some(record.timestamp.as_str()),
            ])
            .unwrap_or("agent"),
            "agent",
        ),
        family_id: first_non_empty([
            meta.and_then(|value| value.agent_family.as_deref()),
            family_shell(meta, record.done.as_ref())
                .and_then(|value| value.label.as_deref()),
        ])
        .map(|value| safe_identifier(value, "family")),
    }
}

fn exact_locator_for_record(
    logical: LogicalAgentLocatorWire,
    record: &AgentArtifactRecordWire,
) -> AgentInstanceLocatorWire {
    let attempt = record
        .agent_meta
        .as_ref()
        .and_then(|meta| meta.retry_attempt)
        .map(|attempt| format!("attempt-{attempt}"))
        .unwrap_or_else(|| "attempt-0".to_string());
    AgentInstanceLocatorWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        logical,
        shell_id: safe_identifier(&record.workflow_dir_name, "shell"),
        run_id: safe_identifier(&record.timestamp, "run"),
        attempt_id: safe_identifier(&attempt, "attempt"),
    }
}

fn row_kind_for_record(record: &AgentArtifactRecordWire) -> FleetRowKindWire {
    let meta = record.agent_meta.as_ref();
    if meta.and_then(|value| value.proc_id.as_ref()).is_some() {
        return FleetRowKindWire::Proc;
    }
    match family_shell(meta, record.done.as_ref())
        .map(|shell| shell.kind.as_str())
    {
        Some("monitor") => FleetRowKindWire::Monitor,
        Some("gate") => FleetRowKindWire::Gate,
        _ => FleetRowKindWire::AgentShell,
    }
}

fn family_shell<'a>(
    meta: Option<&'a AgentMetaWire>,
    done: Option<&'a DoneMarkerWire>,
) -> Option<&'a FamilyShellWire> {
    meta.and_then(|value| value.family_shell.as_ref())
        .or_else(|| done.and_then(|value| value.family_shell.as_ref()))
}

fn lifecycle_and_content_capabilities(
    row_kind: FleetRowKindWire,
    is_terminal: bool,
    liveness: OwnerLivenessWire,
    content_handles: &[ContentHandleWire],
    has_pending_question: bool,
) -> Vec<String> {
    let mut caps = Vec::new();
    if row_kind == FleetRowKindWire::AgentShell && !is_terminal {
        caps.push(FLEET_MUTATION_CAPABILITY_RETRY.to_string());
        caps.push(FLEET_MUTATION_CAPABILITY_FORK.to_string());
        if liveness == OwnerLivenessWire::Alive {
            caps.push(FLEET_MUTATION_CAPABILITY_STOP.to_string());
        }
    }
    if !is_terminal && has_pending_question {
        caps.push(FLEET_ATTENTION_CAPABILITY_ANSWER_QUESTION.to_string());
    }
    if !is_terminal && row_kind == FleetRowKindWire::Gate {
        caps.push(FLEET_ATTENTION_CAPABILITY_APPROVE_GATE.to_string());
    }
    if !content_handles.is_empty() {
        caps.push("content.range".to_string());
        caps.push("content.read".to_string());
        if content_handles.iter().any(|handle| handle.supports_growth) {
            caps.push("content.tail".to_string());
        }
    }
    caps
}

fn owner_liveness_for_record(
    record: &AgentArtifactRecordWire,
) -> OwnerLivenessWire {
    if record.done.is_some() {
        return OwnerLivenessWire::Dead;
    }
    let Some(pid) = record
        .running
        .as_ref()
        .and_then(|running| running.pid)
        .or_else(|| record.agent_meta.as_ref().and_then(|meta| meta.pid))
    else {
        return OwnerLivenessWire::Unknown;
    };
    if pid <= 0 {
        return OwnerLivenessWire::NotProcess;
    }
    if process_is_alive(pid) {
        OwnerLivenessWire::Alive
    } else {
        OwnerLivenessWire::Dead
    }
}

fn process_is_alive(pid: i64) -> bool {
    #[cfg(unix)]
    {
        let pid = match libc::pid_t::try_from(pid) {
            Ok(pid) => pid,
            Err(_) => return false,
        };
        unsafe { libc::kill(pid, 0) == 0 }
    }
    #[cfg(not(unix))]
    {
        let _ = pid;
        false
    }
}

fn content_handles_for_record(
    record: &AgentArtifactRecordWire,
    logical_key: &str,
    row_revision: &ResourceRevisionWire,
) -> Result<(Vec<ContentHandleWire>, Vec<FleetContentSource>), FleetReadError> {
    let artifact_root = PathBuf::from(&record.artifact_dir);
    let Some(canonical_root) = fs::canonicalize(&artifact_root).ok() else {
        return Ok((Vec::new(), Vec::new()));
    };
    let mut seen = BTreeSet::new();
    let mut handles = Vec::new();
    let mut sources = Vec::new();
    for candidate in content_path_candidates(record) {
        let Some((canonical_path, relative_key)) =
            canonical_content_path(&canonical_root, &candidate.raw_path)
        else {
            continue;
        };
        if !seen.insert(canonical_path.clone()) {
            continue;
        }
        let metadata = fs::metadata(&canonical_path).map_err(|_| {
            FleetReadError::Backend("content_metadata".to_string())
        })?;
        if !metadata.is_file() {
            continue;
        }
        let handle_id = content_handle_id(
            logical_key,
            row_revision.revision,
            candidate.kind,
            &relative_key,
        );
        let handle = ContentHandleWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            id: handle_id,
            kind: candidate.kind,
            revision: Some(row_revision.clone()),
            digest: None,
            byte_len: Some(metadata.len()),
            supports_range: true,
            supports_growth: candidate.supports_growth,
        };
        sources.push(FleetContentSource {
            handle: handle.clone(),
            row_revision: row_revision.clone(),
            canonical_path,
            artifact_root: canonical_root.clone(),
        });
        handles.push(handle);
    }
    handles.sort_by(|left, right| {
        (left.kind, left.id.as_str()).cmp(&(right.kind, right.id.as_str()))
    });
    sources.sort_by(|left, right| left.handle.id.cmp(&right.handle.id));
    Ok((handles, sources))
}

struct ContentPathCandidate {
    kind: ContentHandleKindWire,
    raw_path: String,
    supports_growth: bool,
}

fn content_path_candidates(
    record: &AgentArtifactRecordWire,
) -> Vec<ContentPathCandidate> {
    let mut candidates = Vec::new();
    if let Some(meta) = &record.agent_meta {
        push_path(
            &mut candidates,
            ContentHandleKindWire::Output,
            meta.output_path.as_deref(),
            true,
        );
        push_path(
            &mut candidates,
            ContentHandleKindWire::Question,
            meta.question_response_path.as_deref(),
            true,
        );
        push_path(
            &mut candidates,
            ContentHandleKindWire::Log,
            meta.family_shell
                .as_ref()
                .and_then(|shell| shell.output_path.as_deref()),
            true,
        );
    }
    if let Some(done) = &record.done {
        push_path(
            &mut candidates,
            ContentHandleKindWire::Transcript,
            done.response_path.as_deref(),
            false,
        );
        push_path(
            &mut candidates,
            ContentHandleKindWire::Output,
            done.output_path.as_deref(),
            false,
        );
        push_path(
            &mut candidates,
            ContentHandleKindWire::Diff,
            done.diff_path.as_deref(),
            false,
        );
        push_path(
            &mut candidates,
            ContentHandleKindWire::Log,
            done.family_shell
                .as_ref()
                .and_then(|shell| shell.output_path.as_deref()),
            false,
        );
    }
    candidates
}

fn push_path(
    candidates: &mut Vec<ContentPathCandidate>,
    kind: ContentHandleKindWire,
    raw_path: Option<&str>,
    supports_growth: bool,
) {
    let Some(raw_path) =
        raw_path.map(str::trim).filter(|value| !value.is_empty())
    else {
        return;
    };
    candidates.push(ContentPathCandidate {
        kind,
        raw_path: raw_path.to_string(),
        supports_growth,
    });
}

fn canonical_content_path(
    artifact_root: &Path,
    raw_path: &str,
) -> Option<(PathBuf, String)> {
    let raw = Path::new(raw_path);
    let candidate = if raw.is_absolute() {
        raw.to_path_buf()
    } else {
        artifact_root.join(raw)
    };
    let canonical = fs::canonicalize(candidate).ok()?;
    if !canonical.starts_with(artifact_root) {
        return None;
    }
    let relative = canonical
        .strip_prefix(artifact_root)
        .ok()?
        .to_string_lossy()
        .replace('\\', "/");
    if relative.is_empty() {
        return None;
    }
    Some((canonical, relative))
}

fn content_handle_id(
    logical_key: &str,
    revision: u64,
    kind: ContentHandleKindWire,
    relative_key: &str,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"sase-fleet-content-handle-v1\0");
    hasher.update(logical_key.as_bytes());
    hasher.update(b"\0");
    hasher.update(revision.to_le_bytes());
    hasher.update(b"\0");
    hasher.update(format!("{kind:?}").as_bytes());
    hasher.update(b"\0");
    hasher.update(relative_key.as_bytes());
    format!("ch{}", hex::encode(hasher.finalize()))
}

async fn read_content_range(
    source: FleetContentSource,
    offset: u64,
    limit: u64,
    cursor: StoreCursorWire,
) -> Result<FleetContentReadResponseWire, FleetReadError> {
    tokio::task::spawn_blocking(move || {
        let canonical_path = fs::canonicalize(&source.canonical_path)
            .map_err(|_| FleetReadError::NotFound("handle_id".to_string()))?;
        if !canonical_path.starts_with(&source.artifact_root) {
            return Err(FleetReadError::Stale("handle_id".to_string()));
        }
        let metadata = fs::metadata(&canonical_path)
            .map_err(|_| FleetReadError::NotFound("handle_id".to_string()))?;
        let total_byte_len = metadata.len();
        if offset > total_byte_len {
            return Err(FleetReadError::Validation(
                "content offset exceeds current content length".to_string(),
            ));
        }
        let mut file = fs::File::open(&canonical_path)
            .map_err(|_| FleetReadError::NotFound("handle_id".to_string()))?;
        file.seek(SeekFrom::Start(offset))
            .map_err(|_| FleetReadError::Backend("content_seek".to_string()))?;
        let available = total_byte_len.saturating_sub(offset);
        let read_len = available.min(limit);
        let mut bytes = vec![0_u8; read_len as usize];
        file.read_exact(&mut bytes)
            .map_err(|_| FleetReadError::Backend("content_read".to_string()))?;
        let mut hasher = Sha256::new();
        hasher.update(&bytes);
        let returned_bytes = bytes.len() as u64;
        let next_offset = offset.saturating_add(returned_bytes);
        let eof = next_offset >= total_byte_len;
        Ok(FleetContentReadResponseWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            cursor,
            handle: ContentHandleWire {
                byte_len: Some(total_byte_len),
                ..source.handle
            },
            offset,
            returned_bytes,
            total_byte_len,
            next_offset: (!eof).then_some(next_offset),
            eof,
            supports_growth: source.handle.supports_growth,
            sha256: hex::encode(hasher.finalize()),
            data_base64: BASE64.encode(bytes),
        })
    })
    .await
    .map_err(|_| FleetReadError::Backend("content_join".to_string()))?
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

fn current_unix_time() -> f64 {
    let now = Utc::now();
    now.timestamp() as f64
        + f64::from(now.timestamp_subsec_micros()) / 1_000_000.0
}

fn observation_freshness_for_age(
    age_seconds: f64,
) -> Result<ObservationFreshnessWire, FleetReadError> {
    let decision = classify_cache_freshness(&CacheFreshnessRequestWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        viewer_monotonic_elapsed_seconds: Some(age_seconds),
        fresh_threshold_seconds: FLEET_SNAPSHOT_FRESH_SECONDS,
        stale_threshold_seconds: FLEET_SNAPSHOT_STALE_SECONDS,
    })
    .map_err(FleetReadError::from)?;
    Ok(decision.freshness)
}

/// Trustworthy completion time for presentation ranking/bounding:
/// `done.finished_at` when present, else the record's own timestamp parsed
/// to unix seconds. `record.timestamp` is always present and always in the
/// compact `%Y%m%d%H%M%S` artifact-directory format, so it is a safe final
/// fallback even when `done` is absent (a demoted dead-active record).
fn completion_time_for_record(record: &AgentArtifactRecordWire) -> f64 {
    if let Some(finished_at) =
        record.done.as_ref().and_then(|done| done.finished_at)
    {
        return finished_at;
    }
    parse_record_timestamp(&record.timestamp).unwrap_or_else(current_unix_time)
}

fn parse_record_timestamp(value: &str) -> Option<f64> {
    let parsed = NaiveDateTime::parse_from_str(value, "%Y%m%d%H%M%S").ok()?;
    Some(parsed.and_utc().timestamp() as f64)
}

fn stable_revision(record: &AgentArtifactRecordWire) -> u64 {
    let mut hasher = Sha256::new();
    hasher.update(b"sase-fleet-row-revision-v1\0");
    if let Ok(bytes) = serde_json::to_vec(record) {
        hasher.update(bytes);
    } else {
        hasher.update(record.artifact_dir.as_bytes());
    }
    let digest = hasher.finalize();
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&digest[..8]);
    u64::from_le_bytes(bytes).max(1)
}

fn safe_identifier(value: &str, fallback_prefix: &str) -> String {
    let trimmed = value.trim();
    if !trimmed.is_empty()
        && trimmed.len() <= 128
        && !trimmed.chars().any(char::is_control)
    {
        return trimmed.to_string();
    }
    let mut hasher = Sha256::new();
    hasher.update(trimmed.as_bytes());
    let digest = hex::encode(hasher.finalize());
    format!("{fallback_prefix}-{}", &digest[..16])
}

fn safe_reason(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return "not_launchable".to_string();
    }
    let mut safe = trimmed.replace(['/', '\\'], " ");
    if safe.len() > 256 {
        safe.truncate(256);
    }
    safe
}

fn first_non_empty<'a>(
    values: impl IntoIterator<Item = Option<&'a str>>,
) -> Option<&'a str> {
    values
        .into_iter()
        .flatten()
        .map(str::trim)
        .find(|value| !value.is_empty())
}

#[derive(Clone)]
pub struct FleetInvalidationHub {
    inner: Arc<Mutex<FleetInvalidationHubInner>>,
    sender: broadcast::Sender<FleetInvalidationEventWire>,
    capacity: usize,
}

impl std::fmt::Debug for FleetInvalidationHub {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FleetInvalidationHub")
            .field("capacity", &self.capacity)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
struct FleetInvalidationHubInner {
    store_generation: String,
    sequence: u64,
    ring: VecDeque<FleetInvalidationEventWire>,
    deletion_history_complete: bool,
}

pub struct FleetEventSubscription {
    pub initial: Vec<FleetEventStreamItemWire>,
    pub receiver: broadcast::Receiver<FleetInvalidationEventWire>,
}

impl FleetInvalidationHub {
    pub fn new(capacity: usize) -> Result<Self, FleetReadError> {
        validate_fleet_replay_capacity(capacity)
            .map_err(FleetReadError::from)?;
        let (sender, _) = broadcast::channel(FLEET_EVENT_BROADCAST_CAPACITY);
        Ok(Self {
            inner: Arc::new(Mutex::new(FleetInvalidationHubInner {
                store_generation: new_generation(),
                sequence: 0,
                ring: VecDeque::new(),
                deletion_history_complete: true,
            })),
            sender,
            capacity,
        })
    }

    pub fn current_cursor(&self) -> StoreCursorWire {
        let inner = self.inner.lock().expect("fleet event hub poisoned");
        StoreCursorWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            store_generation: inner.store_generation.clone(),
            sequence: inner.sequence,
        }
    }

    pub fn subscribe(
        &self,
        cursor: Option<StoreCursorWire>,
        snapshot: FleetAuthoritativeSnapshotWire,
    ) -> Result<FleetEventSubscription, FleetReadError> {
        let mut initial = Vec::new();
        let receiver = {
            let inner = self.inner.lock().map_err(|_| {
                FleetReadError::Backend("fleet_events".to_string())
            })?;
            let receiver = self.sender.subscribe();
            let cursor = cursor.unwrap_or(StoreCursorWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                store_generation: FLEET_INITIAL_CURSOR_GENERATION.to_string(),
                sequence: 0,
            });
            let oldest = inner
                .ring
                .front()
                .map(|event| event.cursor.sequence)
                .unwrap_or(inner.sequence);
            let decision = classify_cursor_replay(&CursorReplayRequestWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                cursor,
                current_generation: inner.store_generation.clone(),
                newest_sequence: inner.sequence,
                oldest_replayable_sequence: oldest,
                deletion_history_complete: inner.deletion_history_complete,
            })
            .map_err(FleetReadError::from)?;
            match decision.classification {
                CursorReplayClassificationWire::Current => {}
                CursorReplayClassificationWire::Replayable => {
                    let replay_from =
                        decision.replay_from_sequence.unwrap_or(0);
                    for event in &inner.ring {
                        if event.cursor.sequence >= replay_from {
                            initial.push(
                                FleetEventStreamItemWire::Invalidation(
                                    event.clone(),
                                ),
                            );
                        }
                    }
                }
                CursorReplayClassificationWire::ResyncRequired => {
                    initial.push(FleetEventStreamItemWire::ResyncRequired(
                        FleetResyncRequiredWire {
                            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                            reason: cursor_replay_reason_to_resync_reason(
                                decision.reason,
                            ),
                            snapshot,
                        },
                    ));
                }
            }
            receiver
        };
        Ok(FleetEventSubscription { initial, receiver })
    }

    pub fn publish(
        &self,
        kind: FleetInvalidationKindWire,
        logical_key: Option<String>,
        row_revision: Option<ResourceRevisionWire>,
        reason: &str,
    ) -> Result<FleetInvalidationEventWire, FleetReadError> {
        let event = {
            let mut inner = self.inner.lock().map_err(|_| {
                FleetReadError::Backend("fleet_events".to_string())
            })?;
            inner.sequence = inner.sequence.saturating_add(1);
            let event = FleetInvalidationEventWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                cursor: StoreCursorWire {
                    schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                    store_generation: inner.store_generation.clone(),
                    sequence: inner.sequence,
                },
                kind,
                logical_key,
                row_revision,
                reason: reason.to_string(),
            };
            validate_fleet_invalidation_event(&event)
                .map_err(FleetReadError::from)?;
            inner.ring.push_back(event.clone());
            while inner.ring.len() > self.capacity {
                inner.ring.pop_front();
            }
            event
        };
        let _ = self.sender.send(event.clone());
        Ok(event)
    }

    #[cfg(test)]
    fn replace_generation(&self) {
        if let Ok(mut inner) = self.inner.lock() {
            inner.store_generation = new_generation();
            inner.sequence = 0;
            inner.ring.clear();
            inner.deletion_history_complete = true;
        }
    }

    #[cfg(test)]
    fn mark_deletion_history_incomplete(&self) {
        if let Ok(mut inner) = self.inner.lock() {
            inner.deletion_history_complete = false;
        }
    }
}

fn new_generation() -> String {
    let now = Utc::now();
    let mut hasher = Sha256::new();
    hasher.update(now.timestamp_nanos_opt().unwrap_or(0).to_le_bytes());
    hasher.update(std::process::id().to_le_bytes());
    format!("gen-{}", &hex::encode(hasher.finalize())[..24])
}

pub fn resync_item(
    reason: FleetResyncReasonWire,
    snapshot: FleetAuthoritativeSnapshotWire,
) -> FleetEventStreamItemWire {
    FleetEventStreamItemWire::ResyncRequired(FleetResyncRequiredWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        reason,
        snapshot,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::engine::general_purpose::STANDARD as BASE64;
    use sase_core::fleet_contract::FleetStatusBucketWire;
    use serde_json::json;
    use tempfile::{tempdir, TempDir};

    fn write_json(path: &Path, payload: serde_json::Value) {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, serde_json::to_string(&payload).unwrap()).unwrap();
    }

    fn seed_home() -> (TempDir, FleetReadService) {
        let temp = tempdir().unwrap();
        let home = temp.path().to_path_buf();
        let projects = home.join("projects");
        seed_project(&projects, "proj");
        // Recent-relative-to-now timestamps: the fleet presentation policy
        // windows terminal presentation to the last seven days, so a fixed
        // historical date would eventually fall outside that window and
        // make these seeded rows silently vanish.
        let now = Utc::now();
        let alpha_ts = (now - chrono::Duration::minutes(2))
            .format("%Y%m%d%H%M%S")
            .to_string();
        let beta_ts = (now - chrono::Duration::minutes(1))
            .format("%Y%m%d%H%M%S")
            .to_string();
        seed_agent(&projects, &alpha_ts, "alpha", "alpha output");
        seed_agent(&projects, &beta_ts, "beta", "beta output");
        sase_core::rebuild_agent_artifact_index(
            &home.join("agent_artifact_index.sqlite"),
            &projects,
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        (temp, FleetReadService::new(home))
    }

    fn seed_project(projects: &Path, name: &str) {
        let project = projects.join(name);
        fs::create_dir_all(&project).unwrap();
        write_json(
            &project.join("placeholder.json"),
            json!({"created_for": "fleet_read_test"}),
        );
        fs::write(
            project.join(format!("{name}.sase")),
            format!(
                "NAME: {name}\nWORKSPACE_DIR: {}\nPROJECT_STATE: enabled\n",
                project.display()
            ),
        )
        .unwrap();
    }

    fn seed_agent(projects: &Path, timestamp: &str, name: &str, output: &str) {
        let artifact = projects
            .join("proj")
            .join("artifacts")
            .join("ace-run")
            .join(timestamp);
        fs::create_dir_all(&artifact).unwrap();
        fs::write(artifact.join("output.txt"), output).unwrap();
        write_json(
            &artifact.join("agent_meta.json"),
            json!({
                "name": name,
                "model": "gpt-5",
                "llm_provider": "codex",
                "output_path": "output.txt"
            }),
        );
        // Use this test process's own PID so `owner_liveness_for_record`
        // resolves `Alive`: these fixtures represent ordinary current
        // agents, and the presentation policy now treats a `NotProcess`
        // active-tier record as terminal-for-presentation.
        write_json(
            &artifact.join("running.json"),
            json!({"pid": std::process::id()}),
        );
    }

    /// Seed an active-tier record whose PID (`0`) resolves deterministically
    /// to `NotProcess` liveness, optionally as a tracked child of `parent`.
    fn seed_dead_agent(
        projects: &Path,
        timestamp: &str,
        name: &str,
        parent: Option<&str>,
    ) {
        let artifact = projects
            .join("proj")
            .join("artifacts")
            .join("ace-run")
            .join(timestamp);
        fs::create_dir_all(&artifact).unwrap();
        let mut meta = json!({"name": name});
        if let Some(parent) = parent {
            meta["parent_timestamp"] = json!(parent);
        }
        write_json(&artifact.join("agent_meta.json"), meta);
        write_json(&artifact.join("running.json"), json!({"pid": 0}));
    }

    #[tokio::test]
    async fn summary_counts_are_independent_of_catalog_page_size() {
        let (_temp, service) = seed_home();
        let summary = service.summary().await.unwrap();
        assert_eq!(summary.counts.logical_agent_total, 2);

        let first = service
            .catalog(FleetCatalogQueryWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                cursor: None,
                limit: Some(1),
                project_ids: Vec::new(),
                query: None,
                status_buckets: Vec::new(),
                include_terminal: true,
            })
            .await
            .unwrap();
        assert_eq!(first.page.rows.len(), 1);
        assert_eq!(first.counts.logical_agent_total, 2);
        assert!(first.page.has_more);
        let second = service
            .catalog(FleetCatalogQueryWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                cursor: first.page.next_cursor,
                limit: Some(1),
                project_ids: Vec::new(),
                query: None,
                status_buckets: Vec::new(),
                include_terminal: true,
            })
            .await
            .unwrap();
        assert_eq!(second.page.rows.len(), 1);
        assert_ne!(
            first.page.rows[0].logical_key,
            second.page.rows[0].logical_key
        );
    }

    #[tokio::test]
    async fn batch_lookup_reads_followed_id_outside_first_page() {
        let (_temp, service) = seed_home();
        let page = service
            .catalog(FleetCatalogQueryWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                cursor: None,
                limit: Some(1),
                project_ids: Vec::new(),
                query: None,
                status_buckets: Vec::new(),
                include_terminal: true,
            })
            .await
            .unwrap();
        let all = service
            .catalog(FleetCatalogQueryWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                cursor: None,
                limit: Some(10),
                project_ids: Vec::new(),
                query: None,
                status_buckets: Vec::new(),
                include_terminal: true,
            })
            .await
            .unwrap();
        let outside_first_page = all
            .page
            .rows
            .iter()
            .find(|row| row.logical_key != page.page.rows[0].logical_key)
            .unwrap()
            .logical_key
            .clone();
        let batch = service
            .batch_lookup(FleetLogicalBatchRequestWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                logical_keys: vec![outside_first_page.clone()],
            })
            .await
            .unwrap();
        assert_eq!(batch.entries.len(), 1);
        assert_eq!(
            batch.entries[0].summary.as_ref().unwrap().logical_key,
            outside_first_page
        );
    }

    #[tokio::test]
    async fn detail_and_content_reads_use_opaque_handles() {
        let (_temp, service) = seed_home();
        let all = service
            .catalog(FleetCatalogQueryWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                cursor: None,
                limit: Some(10),
                project_ids: Vec::new(),
                query: Some("alpha".to_string()),
                status_buckets: Vec::new(),
                include_terminal: true,
            })
            .await
            .unwrap();
        let key = all.page.rows[0].logical_key.clone();
        let detail = service
            .detail(FleetDetailRequestWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                logical_key: key,
            })
            .await
            .unwrap();
        let value = serde_json::to_value(&detail).unwrap();
        assert_no_paths_or_pids(&value);
        let handle = detail.detail.content_handles[0].clone();
        let revision = handle.revision.clone().unwrap();
        let read = service
            .content(FleetContentReadRequestWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                handle_id: handle.id.clone(),
                row_revision: revision.clone(),
                offset: 0,
                limit: Some(5),
            })
            .await
            .unwrap();
        assert_eq!(read.returned_bytes, 5);
        assert_eq!(BASE64.decode(read.data_base64).unwrap(), b"alpha");
        assert!(read.next_offset.is_some());
        assert!(service
            .content(FleetContentReadRequestWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                handle_id: "../secret".to_string(),
                row_revision: revision.clone(),
                offset: 0,
                limit: Some(5),
            })
            .await
            .is_err());
        assert!(matches!(
            service
                .content(FleetContentReadRequestWire {
                    schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                    handle_id: handle.id,
                    row_revision: ResourceRevisionWire {
                        revision: revision.revision.saturating_add(1),
                        ..revision
                    },
                    offset: 0,
                    limit: Some(5),
                })
                .await,
            Err(FleetReadError::Stale(_))
        ));
    }

    #[tokio::test]
    async fn project_eligibility_is_bounded_and_path_free() {
        let (_temp, service) = seed_home();
        let response = service
            .project_eligibility(FleetProjectEligibilityRequestWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                project_ids: Vec::new(),
                limit: Some(1),
            })
            .await
            .unwrap();
        assert_eq!(response.projects.len(), 1);
        let value = serde_json::to_value(&response).unwrap();
        assert_no_paths_or_pids(&value);
        assert!(service
            .project_eligibility(FleetProjectEligibilityRequestWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                project_ids: vec![
                    "proj".to_string();
                    sase_core::FLEET_READ_MAX_PROJECT_IDS + 1
                ],
                limit: None,
            })
            .await
            .is_err());
    }

    #[tokio::test]
    async fn fleet_events_publish_replay_and_resync_faults() {
        let (_temp, service) = seed_home();
        let cursor = service.current_event_cursor();
        let mut first = service
            .subscribe_events(Some(cursor.clone()))
            .await
            .unwrap();
        let mut second = service
            .subscribe_events(Some(cursor.clone()))
            .await
            .unwrap();
        assert!(first.initial.is_empty());
        service
            .publish_invalidation(
                FleetInvalidationKindWire::RevisionChanged,
                None,
                None,
                "revision_changed",
            )
            .unwrap();
        assert!(first.receiver.recv().await.is_ok());
        assert!(second.receiver.recv().await.is_ok());

        let replay = service
            .subscribe_events(Some(cursor.clone()))
            .await
            .unwrap();
        assert!(matches!(
            replay.initial.first(),
            Some(FleetEventStreamItemWire::Invalidation(_))
        ));

        for _ in 0..(sase_core::FLEET_READ_DEFAULT_REPLAY_EVENTS + 2) {
            service
                .publish_invalidation(
                    FleetInvalidationKindWire::RevisionChanged,
                    None,
                    None,
                    "revision_changed",
                )
                .unwrap();
        }
        let rolled = service
            .subscribe_events(Some(cursor.clone()))
            .await
            .unwrap();
        assert!(matches!(
            rolled.initial.first(),
            Some(FleetEventStreamItemWire::ResyncRequired(resync))
                if resync.reason == FleetResyncReasonWire::ReplayGap
        ));

        let old_generation = service.current_event_cursor();
        service.replace_event_generation_for_test();
        let replaced = service
            .subscribe_events(Some(old_generation))
            .await
            .unwrap();
        assert!(matches!(
            replaced.initial.first(),
            Some(FleetEventStreamItemWire::ResyncRequired(resync))
                if resync.reason == FleetResyncReasonWire::GenerationMismatch
        ));

        let current = service.current_event_cursor();
        service.mark_deletion_history_incomplete_for_test();
        let incomplete = service.subscribe_events(Some(current)).await.unwrap();
        assert!(matches!(
            incomplete.initial.first(),
            Some(FleetEventStreamItemWire::ResyncRequired(resync))
                if resync.reason == FleetResyncReasonWire::IncompleteDeletionHistory
        ));
    }

    #[tokio::test]
    async fn concurrent_snapshot_reads_are_coalesced() {
        let (_temp, service) = seed_home();
        let (left, right) = tokio::join!(service.summary(), service.summary());
        assert!(left.is_ok());
        assert!(right.is_ok());
        assert_eq!(service.refresh_count_for_test(), 1);
    }

    #[tokio::test]
    async fn aged_cache_triggers_exactly_one_coalesced_rebuild() {
        let (_temp, service) = seed_home();
        service.summary().await.unwrap();
        assert_eq!(service.refresh_count_for_test(), 1);

        service.age_cache_for_test(FLEET_SNAPSHOT_STALE_SECONDS + 1.0);
        let (left, right) = tokio::join!(service.summary(), service.summary());
        assert!(left.is_ok());
        assert!(right.is_ok());
        assert_eq!(
            service.refresh_count_for_test(),
            2,
            "a stale cache should trigger exactly one rebuild, coalesced \
             across concurrent readers"
        );
    }

    #[tokio::test]
    async fn failed_rebuild_retains_stale_partial_prior_snapshot() {
        let (_temp, service) = seed_home();
        let first = service.summary().await.unwrap();
        assert_eq!(first.counts.logical_agent_total, 2);
        assert_eq!(service.refresh_count_for_test(), 1);

        service.age_cache_for_test(FLEET_SNAPSHOT_STALE_SECONDS + 1.0);
        // Replace the index file with a directory so the next rebuild
        // attempt fails deterministically instead of relying on timing.
        fs::remove_file(service.index_path()).unwrap();
        fs::create_dir_all(service.index_path()).unwrap();

        let second = service.summary().await.unwrap();
        assert_eq!(
            second.counts.logical_agent_total, 2,
            "a failed rebuild must retain the previous snapshot's data"
        );
        assert_eq!(second.freshness.freshness, ObservationFreshnessWire::Stale);
        assert!(second.freshness.partial);
        assert!(second.freshness.error.is_some());
        assert_eq!(
            service.refresh_count_for_test(),
            1,
            "a failed rebuild must not be counted as a successful refresh"
        );
    }

    #[tokio::test]
    async fn dead_active_leftovers_are_demoted_and_window_bounded() {
        let temp = tempdir().unwrap();
        let home = temp.path().to_path_buf();
        let projects = home.join("projects");
        seed_project(&projects, "proj");
        let now = Utc::now();
        let recent_ts = (now - chrono::Duration::minutes(5))
            .format("%Y%m%d%H%M%S")
            .to_string();
        let old_ts = (now - chrono::Duration::days(8))
            .format("%Y%m%d%H%M%S")
            .to_string();
        seed_dead_agent(&projects, &recent_ts, "recent-dead", None);
        seed_dead_agent(&projects, &old_ts, "old-dead", None);
        sase_core::rebuild_agent_artifact_index(
            &home.join("agent_artifact_index.sqlite"),
            &projects,
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        let service = FleetReadService::new(home);

        let all = service
            .catalog(FleetCatalogQueryWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                cursor: None,
                limit: Some(10),
                project_ids: Vec::new(),
                query: None,
                status_buckets: Vec::new(),
                include_terminal: true,
            })
            .await
            .unwrap();

        let recent_row = all
            .page
            .rows
            .iter()
            .find(|row| {
                row.labels.agent_label.as_deref() == Some("recent-dead")
            })
            .expect("recent dead-active leftover should still be served");
        assert_eq!(recent_row.status_bucket, FleetStatusBucketWire::Stopped);
        assert_eq!(recent_row.liveness, OwnerLivenessWire::NotProcess);
        assert!(
            !all.page.rows.iter().any(
                |row| row.labels.agent_label.as_deref() == Some("old-dead")
            ),
            "a leftover outside the seven-day window must be excluded: {:?}",
            all.page.rows
        );
    }

    #[tokio::test]
    async fn dead_orphan_of_dismissed_family_is_excluded_from_catalog() {
        let temp = tempdir().unwrap();
        let home = temp.path().to_path_buf();
        let projects = home.join("projects");
        seed_project(&projects, "proj");
        let now = Utc::now();
        let root_ts = (now - chrono::Duration::minutes(10))
            .format("%Y%m%d%H%M%S")
            .to_string();
        let member_ts = (now - chrono::Duration::minutes(5))
            .format("%Y%m%d%H%M%S")
            .to_string();
        seed_dead_agent(&projects, &root_ts, "root", None);
        seed_dead_agent(&projects, &member_ts, "member", Some(&root_ts));
        let index = home.join("agent_artifact_index.sqlite");
        sase_core::rebuild_agent_artifact_index(
            &index,
            &projects,
            AgentArtifactScanOptionsWire::default(),
        )
        .unwrap();
        sase_core::replace_agent_artifact_index_dismissed_agents(
            &index,
            &[sase_core::AgentCleanupIdentityWire {
                agent_type: "run".to_string(),
                cl_name: "unknown".to_string(),
                raw_suffix: Some(root_ts.clone()),
            }],
        )
        .unwrap();
        let service = FleetReadService::new(home);

        let all = service
            .catalog(FleetCatalogQueryWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                cursor: None,
                limit: Some(10),
                project_ids: Vec::new(),
                query: None,
                status_buckets: Vec::new(),
                include_terminal: true,
            })
            .await
            .unwrap();

        assert!(
            all.page.rows.is_empty(),
            "a dead root and its dead member must both be excluded once the \
             family root is dismissed: {:?}",
            all.page.rows
        );
    }

    fn assert_no_paths_or_pids(value: &serde_json::Value) {
        match value {
            serde_json::Value::Object(map) => {
                for (key, value) in map {
                    let lower = key.to_ascii_lowercase();
                    assert!(!lower.contains("path"), "forbidden key {key}");
                    assert!(!lower.contains("pid"), "forbidden key {key}");
                    assert_no_paths_or_pids(value);
                }
            }
            serde_json::Value::Array(values) => {
                for value in values {
                    assert_no_paths_or_pids(value);
                }
            }
            serde_json::Value::String(text) => {
                assert!(
                    !text.contains(temp_root_hint()),
                    "forbidden path text {text}"
                );
            }
            _ => {}
        }
    }

    fn temp_root_hint() -> &'static str {
        "/tmp/"
    }
}
