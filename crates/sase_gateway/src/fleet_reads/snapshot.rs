//! Authoritative snapshot construction from the artifact index.

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};

use chrono::Utc;
use sase_core::{
    agent_scan::{
        AgentArtifactIndexFreshnessWire, AgentArtifactIndexQueryWire,
        AgentArtifactRecordShapeWire, AgentArtifactScanOptionsWire,
        AgentSessionDismissalLineageCandidateWire,
    },
    fleet_catalog::select_fleet_presentation,
    fleet_contract::{
        count_logical_agents, ensure_installation_identity,
        fleet_catalog_snapshot_id, fleet_count_revision,
        validate_fleet_authoritative_snapshot, FleetAuthoritativeSnapshotWire,
        FleetCatalogScopeWire, FleetLogicalAgentCountsRequestWire,
        FleetSnapshotFreshnessWire, ObservationFreshnessWire,
        OwnerLivenessWire, ResolvedAgentDetailWire, StoreCursorWire,
        FLEET_CONTRACT_SCHEMA_VERSION,
    },
    fleet_owner_facts::OwnerFileObserver,
    host_liveness::{OwnerLivenessObserver, OwnerProcessObservation},
    list_project_records, query_agent_artifact_index,
    resolve_agent_session_dismissal_lineage,
};

use super::{
    content::FleetContentSource, errors::FleetReadError,
    resolution::resolve_record,
};

#[derive(Clone, Debug)]
pub(super) struct CachedFleetSnapshot {
    pub(super) wire: FleetAuthoritativeSnapshotWire,
    pub(super) summaries_by_logical_key:
        BTreeMap<String, sase_core::fleet_contract::ResolvedAgentSummaryWire>,
    pub(super) details_by_logical_key:
        BTreeMap<String, ResolvedAgentDetailWire>,
    pub(super) content_by_handle: BTreeMap<String, FleetContentSource>,
    pub(super) refresh_count: u64,
    /// Monotonic instant this snapshot was built (or last successfully
    /// rebuilt). Every row's `observed_at_unix` and the envelope's
    /// `refreshed_at_unix` are stamped from one build timestamp at the same
    /// moment; this field is the seam that lets a later read derive honest
    /// freshness from actual elapsed time instead of trusting a stored
    /// label.
    pub(super) build_instant: Instant,
}

pub(super) struct BuildSnapshotRequest {
    pub(super) sase_home: PathBuf,
    pub(super) index_path: PathBuf,
    pub(super) projects_root: PathBuf,
    pub(super) cursor: StoreCursorWire,
    pub(super) prior_refresh_count: u64,
    pub(super) scope: FleetCatalogScopeWire,
    pub(super) liveness: Arc<dyn OwnerLivenessObserver>,
    pub(super) owner_files: Arc<dyn OwnerFileObserver>,
}

pub(super) fn build_snapshot_blocking(
    request: BuildSnapshotRequest,
) -> Result<CachedFleetSnapshot, FleetReadError> {
    let installation = ensure_installation_identity(&request.sase_home)
        .map_err(FleetReadError::from)?
        .record;
    let index_query = match request.scope {
        FleetCatalogScopeWire::Presentation => AgentArtifactIndexQueryWire {
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
            agents_list_projection: false,
        },
        FleetCatalogScopeWire::History => AgentArtifactIndexQueryWire {
            include_active: false,
            include_recent_completed: false,
            include_full_history: true,
            active_limit: None,
            recent_completed_limit: None,
            include_hidden: false,
            freshness: AgentArtifactIndexFreshnessWire::Revalidate,
            only_monitors: false,
            record_shape: AgentArtifactRecordShapeWire::Full,
            window_limit: None,
            candidate_filter: None,
            agents_list_projection: false,
        },
    };
    let scan = query_agent_artifact_index(
        &request.index_path,
        &request.projects_root,
        index_query,
        AgentArtifactScanOptionsWire {
            max_prompt_snippet_bytes: 512,
            ..AgentArtifactScanOptionsWire::default()
        },
    )
    .map_err(|_| FleetReadError::Backend("artifact_index".to_string()))?;

    let build_instant = Instant::now();
    let now_unix = current_unix_time();
    let project_labels = project_display_labels(&request.projects_root)?;

    // Resolve owner liveness once per indexed record and reuse it for
    // dismissal-lineage seeding, presentation candidates, and projected
    // details. Dismissal is resolved for every candidate before selection
    // and is not gated on liveness or protection.
    let observation_by_identity: BTreeMap<String, OwnerProcessObservation> =
        scan.records
            .iter()
            .map(|record| {
                (
                    record.artifact_dir.clone(),
                    request.liveness.observe(record),
                )
            })
            .collect();
    let liveness_by_identity: BTreeMap<String, OwnerLivenessWire> =
        observation_by_identity
            .iter()
            .map(|(identity, observation)| {
                (identity.clone(), observation.liveness())
            })
            .collect();
    let lineage_candidates: Vec<AgentSessionDismissalLineageCandidateWire> =
        scan.records
            .iter()
            .map(|record| AgentSessionDismissalLineageCandidateWire {
                identity: record.artifact_dir.clone(),
                project_name: record.project_name.clone(),
                workflow_dir_name: record.workflow_dir_name.clone(),
                timestamp: record.timestamp.clone(),
                seed_definitively_dead: matches!(
                    liveness_by_identity.get(&record.artifact_dir),
                    Some(
                        OwnerLivenessWire::Dead | OwnerLivenessWire::NotProcess
                    )
                ),
            })
            .collect();
    let dismissed_by_identity: BTreeMap<String, bool> =
        resolve_agent_session_dismissal_lineage(
            &request.index_path,
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

    // Select the served set, then build details, content handles, summaries,
    // and counts only from that set. Presentation remains bounded; explicit
    // history keeps the same safety filters without the terminal age/count
    // window.
    let selection = select_fleet_presentation(
        &scan.records,
        &observation_by_identity,
        &dismissed_by_identity,
        now_unix,
        request.scope,
        request.owner_files.as_ref(),
    )
    .map_err(FleetReadError::from)?;
    let served = selection.served;
    let presentation_context = selection.context;

    let mut details = Vec::new();
    let mut content_by_handle = BTreeMap::new();
    let mut unresolved_rows: u32 = 0;
    let mut unresolved_code: Option<String> = None;
    for record in scan.records {
        if !served.contains(&record.artifact_dir) {
            continue;
        }
        let liveness = liveness_by_identity
            .get(&record.artifact_dir)
            .copied()
            .unwrap_or(OwnerLivenessWire::Unknown);
        // One owner-produced record that cannot be projected must not erase
        // the host's whole presentable set. Drop only that row and report the
        // snapshot as partial, so hello/summary/catalog/detail keep serving
        // every row that is still valid instead of failing the whole read.
        let resolved = match resolve_record(
            &installation.installation_id,
            &record,
            liveness,
            now_unix,
            &project_labels,
            &presentation_context.facts_for_record(&record),
        ) {
            Ok(resolved) => resolved,
            Err(error) => {
                unresolved_rows = unresolved_rows.saturating_add(1);
                if unresolved_code.is_none() {
                    unresolved_code = Some(error.safe_code());
                }
                continue;
            }
        };
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
    let catalog_snapshot_id =
        fleet_catalog_snapshot_id(request.scope, &summaries)
            .map_err(FleetReadError::from)?;
    let counts = count_logical_agents(&FleetLogicalAgentCountsRequestWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        summaries: summaries.clone(),
    })
    .map_err(FleetReadError::from)?;
    let wire = FleetAuthoritativeSnapshotWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        cursor: request.cursor,
        catalog_scope: request.scope,
        catalog_snapshot_id,
        count_revision: fleet_count_revision(&counts),
        counts,
        summaries,
        freshness: FleetSnapshotFreshnessWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            freshness: ObservationFreshnessWire::Fresh,
            partial: unresolved_rows > 0,
            refreshed_at_unix: Some(now_unix),
            error: unresolved_code.map(|code| {
                format!("unresolved rows: {unresolved_rows} ({code})")
            }),
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

fn current_unix_time() -> f64 {
    let now = Utc::now();
    now.timestamp() as f64
        + f64::from(now.timestamp_subsec_micros()) / 1_000_000.0
}

fn project_display_labels(
    projects_root: &Path,
) -> Result<BTreeMap<String, String>, FleetReadError> {
    let records = list_project_records(projects_root, &[], false, true)
        .map_err(|_| {
            FleetReadError::Backend("project_lifecycle".to_string())
        })?;
    Ok(records
        .into_iter()
        .map(|record| {
            let project_name = record.project_name;
            let display_name =
                record.display_name.unwrap_or_else(|| project_name.clone());
            (project_name, display_name)
        })
        .collect())
}
