//! Catalog-from-index assembly shared by the gateway and Python oracle.
//!
//! Selection, root-versus-shell classification, bounded family context, and
//! row projection inputs live here. The gateway stays an observer/cache
//! wrapper; Python invokes the same builder without standing up HTTP.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::agent_scan::{
    query_agent_artifact_index, resolve_family_dismissal_lineage,
    AgentArtifactIndexFreshnessWire, AgentArtifactIndexQueryWire,
    AgentArtifactRecordShapeWire, AgentArtifactRecordWire,
    AgentArtifactScanOptionsWire, FamilyDismissalLineageCandidateWire,
};
use crate::fleet_contract::{
    ensure_installation_identity, instance_locator_key, logical_locator_key,
    project_resolved_agent_summary, AgentInstanceLocatorWire,
    CapabilitySetWire, ConnectionHealthWire, FleetCatalogScopeWire,
    FleetContractError, FleetRowKindWire, LogicalAgentLocatorWire,
    ObservationFreshnessWire, OriginLocatorWire, OwnerLivenessWire,
    OwnerResolutionFactsWire, ProjectLocatorWire,
    ResolvedAgentProjectionRequestWire, ResolvedAgentSummaryWire,
    ResourceRevisionWire, FLEET_CONTRACT_SCHEMA_VERSION,
};
use crate::fleet_family::{
    concrete_family_shell_kind, family_id_for_record, family_key_for_record,
    family_shell, record_is_concrete_family_shell, tracked_parent_timestamp,
    ConcreteFamilyShellKind,
};
use crate::fleet_presentation::{
    decide_fleet_presentation, FleetPresentationCandidateWire,
    FleetPresentationDecisionWire, FleetPresentationRequestWire,
};
use crate::host_liveness::{
    HostOwnerLivenessObserver, OwnerLivenessObserver, OwnerProcessObservation,
};
use crate::list_project_records;

#[derive(Debug, Clone, Default, PartialEq)]
pub struct PresentationRecordFacts {
    pub timestamp: String,
    pub family_id: Option<String>,
    pub parent_timestamp: Option<String>,
    pub agent_clan: Option<String>,
    pub agent_clan_generation: Option<String>,
    pub clan_tribe: Option<String>,
    pub tribe: Option<String>,
    pub started_at_unix: Option<f64>,
    pub run_started_at_unix: Option<f64>,
    pub display_status: Option<String>,
}

#[derive(Debug, Default, Clone)]
pub struct PresentationContext {
    by_timestamp: BTreeMap<String, PresentationRecordFacts>,
    root_by_family_id: BTreeMap<String, PresentationRecordFacts>,
    clan_tribe_by_key: BTreeMap<String, String>,
}

impl PresentationContext {
    pub fn from_records(
        records: &[AgentArtifactRecordWire],
        served: &BTreeSet<String>,
    ) -> Self {
        let mut context = Self::default();
        for record in records {
            if !served.contains(&record.artifact_dir) {
                continue;
            }
            let facts = direct_presentation_facts_for_record(record);
            if let Some(key) = clan_context_key(
                facts.agent_clan.as_deref(),
                facts.agent_clan_generation.as_deref(),
            ) {
                if let Some(clan_tribe) = &facts.clan_tribe {
                    context
                        .clan_tribe_by_key
                        .entry(key)
                        .or_insert_with(|| clan_tribe.clone());
                }
            }
            if !record_is_concrete_family_shell(record)
                && facts.parent_timestamp.is_none()
            {
                if let Some(family_id) = &facts.family_id {
                    context
                        .root_by_family_id
                        .entry(family_id.clone())
                        .or_insert_with(|| facts.clone());
                }
            }
            context
                .by_timestamp
                .insert(record.timestamp.clone(), facts.clone());
        }
        context
    }

    pub fn facts_for_record(
        &self,
        record: &AgentArtifactRecordWire,
    ) -> PresentationRecordFacts {
        let mut facts = direct_presentation_facts_for_record(record);
        let parent = facts
            .parent_timestamp
            .as_ref()
            .and_then(|timestamp| self.by_timestamp.get(timestamp))
            .cloned();
        let family_root = facts
            .family_id
            .as_ref()
            .and_then(|family_id| self.root_by_family_id.get(family_id))
            .cloned()
            .or_else(|| parent.clone());
        let related = parent.as_ref().or(family_root.as_ref());

        if facts.family_id.is_none() {
            facts.family_id = related.and_then(|value| value.family_id.clone());
        }
        if facts.parent_timestamp.is_none()
            && record_is_concrete_family_shell(record)
        {
            if let Some(root) = family_root.as_ref().filter(|root| {
                root.timestamp != record.timestamp && root.family_id.is_some()
            }) {
                facts.parent_timestamp = Some(root.timestamp.clone());
            }
        }
        if facts.tribe.is_none() {
            facts.tribe = related.and_then(|value| value.tribe.clone());
        }
        if facts.clan_tribe.is_none() {
            facts.clan_tribe = related
                .and_then(|value| value.clan_tribe.clone())
                .or_else(|| {
                    clan_context_key(
                        facts.agent_clan.as_deref(),
                        facts.agent_clan_generation.as_deref(),
                    )
                    .and_then(|key| self.clan_tribe_by_key.get(&key).cloned())
                });
        }
        if let Some(root_start) =
            family_root.as_ref().and_then(|value| value.started_at_unix)
        {
            facts.started_at_unix = Some(root_start);
        }
        facts
    }
}

#[derive(Debug, Clone)]
pub struct FleetPresentationSelection {
    pub decision: FleetPresentationDecisionWire,
    pub served: BTreeSet<String>,
    pub context: PresentationContext,
    pub observation_by_identity: BTreeMap<String, OwnerProcessObservation>,
}

pub fn select_fleet_presentation(
    records: &[AgentArtifactRecordWire],
    observation_by_identity: &BTreeMap<String, OwnerProcessObservation>,
    dismissed_by_identity: &BTreeMap<String, bool>,
    now_unix: f64,
    scope: FleetCatalogScopeWire,
) -> Result<FleetPresentationSelection, FleetContractError> {
    let mut presentation_candidates = Vec::with_capacity(records.len());
    for record in records {
        let observation = observation_by_identity
            .get(&record.artifact_dir)
            .copied()
            .unwrap_or(OwnerProcessObservation::Unknown);
        presentation_candidates.push(FleetPresentationCandidateWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            identity: record.artifact_dir.clone(),
            liveness: observation.liveness(),
            protected: record.waiting.is_some()
                || record.pending_question.is_some(),
            completion_time_unix: completion_time_for_record(record, now_unix),
            family_root_dismissed: dismissed_by_identity
                .get(&record.artifact_dir)
                .copied()
                .unwrap_or(false),
            family_member: record_is_concrete_family_shell(record),
            family_key: family_key_for_record(record),
            process_identity_mismatch: observation.process_identity_mismatch(),
        });
    }
    let served: BTreeSet<String> = match scope {
        FleetCatalogScopeWire::Presentation => {
            let decision =
                decide_fleet_presentation(&FleetPresentationRequestWire {
                    schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                    now_unix,
                    candidates: presentation_candidates.clone(),
                })?;
            let served = decision
                .current
                .iter()
                .chain(decision.recent_terminal.iter())
                .cloned()
                .collect();
            let context = PresentationContext::from_records(records, &served);
            return Ok(FleetPresentationSelection {
                decision,
                served,
                context,
                observation_by_identity: observation_by_identity.clone(),
            });
        }
        FleetCatalogScopeWire::History => presentation_candidates
            .iter()
            .filter(|candidate| {
                !candidate.family_root_dismissed
                    && !candidate.process_identity_mismatch
            })
            .map(|candidate| candidate.identity.clone())
            .collect(),
    };
    let context = PresentationContext::from_records(records, &served);
    Ok(FleetPresentationSelection {
        decision: FleetPresentationDecisionWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            current: Vec::new(),
            recent_terminal: Vec::new(),
            excluded: Vec::new(),
        },
        served,
        context,
        observation_by_identity: observation_by_identity.clone(),
    })
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AssembleFleetCatalogRequestWire {
    pub sase_home: String,
    #[serde(default)]
    pub agents_list_projection: bool,
    #[serde(default)]
    pub observations: BTreeMap<String, String>,
    #[serde(default)]
    pub now_unix: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AssembleFleetCatalogResponseWire {
    pub schema_version: u32,
    pub summaries: Vec<ResolvedAgentSummaryWire>,
    pub current: Vec<String>,
    pub recent_terminal: Vec<String>,
    pub excluded: Vec<String>,
}

pub fn assemble_fleet_catalog(
    request: &AssembleFleetCatalogRequestWire,
) -> Result<AssembleFleetCatalogResponseWire, FleetContractError> {
    let sase_home = PathBuf::from(&request.sase_home);
    let index_path = sase_home.join("agent_artifact_index.sqlite");
    let projects_root = sase_home.join("projects");
    let now_unix = request.now_unix.unwrap_or_else(wall_clock_unix);
    let installation = ensure_installation_identity(&sase_home)?.record;
    let query = AgentArtifactIndexQueryWire {
        include_active: true,
        include_recent_completed: true,
        include_full_history: false,
        active_limit: None,
        recent_completed_limit: Some(512),
        include_hidden: false,
        freshness: AgentArtifactIndexFreshnessWire::Revalidate,
        only_monitors: false,
        record_shape: if request.agents_list_projection {
            AgentArtifactRecordShapeWire::List
        } else {
            AgentArtifactRecordShapeWire::Full
        },
        window_limit: Some(512),
        candidate_filter: None,
        agents_list_projection: request.agents_list_projection,
    };
    let scan = query_agent_artifact_index(
        &index_path,
        &projects_root,
        query,
        AgentArtifactScanOptionsWire {
            max_prompt_snippet_bytes: 512,
            ..AgentArtifactScanOptionsWire::default()
        },
    )
    .map_err(|error| {
        FleetContractError::Validation(format!("artifact_index: {error}"))
    })?;
    let host = HostOwnerLivenessObserver::default();
    let injected = parse_observations(&request.observations);
    let observation_by_identity: BTreeMap<String, OwnerProcessObservation> =
        scan.records
            .iter()
            .map(|record| {
                (
                    record.artifact_dir.clone(),
                    observation_for_record(record, &injected, &host),
                )
            })
            .collect();
    let lineage_candidates: Vec<FamilyDismissalLineageCandidateWire> = scan
        .records
        .iter()
        .map(|record| FamilyDismissalLineageCandidateWire {
            identity: record.artifact_dir.clone(),
            project_name: record.project_name.clone(),
            workflow_dir_name: record.workflow_dir_name.clone(),
            timestamp: record.timestamp.clone(),
            seed_definitively_dead: matches!(
                observation_by_identity.get(&record.artifact_dir),
                Some(
                    OwnerProcessObservation::Dead
                        | OwnerProcessObservation::NotProcess
                        | OwnerProcessObservation::IdentityMismatch
                )
            ),
        })
        .collect();
    let dismissed_by_identity: BTreeMap<String, bool> =
        resolve_family_dismissal_lineage(&index_path, &lineage_candidates)
            .map_err(|error| {
                FleetContractError::Validation(format!(
                    "family_dismissal_lineage: {error}"
                ))
            })?
            .into_iter()
            .map(|result| (result.identity, result.family_root_dismissed))
            .collect();
    let selection = select_fleet_presentation(
        &scan.records,
        &observation_by_identity,
        &dismissed_by_identity,
        now_unix,
        FleetCatalogScopeWire::Presentation,
    )?;
    let project_labels = project_display_labels(&projects_root);
    let mut summaries = Vec::new();
    for record in &scan.records {
        if !selection.served.contains(&record.artifact_dir) {
            continue;
        }
        let observation = selection
            .observation_by_identity
            .get(&record.artifact_dir)
            .copied()
            .unwrap_or(OwnerProcessObservation::Unknown);
        match project_summary_for_record(
            &installation.installation_id,
            record,
            observation.liveness(),
            now_unix,
            &project_labels,
            &selection.context.facts_for_record(record),
        ) {
            Ok(summary) => summaries.push(summary),
            Err(_) => continue,
        }
    }
    summaries.sort_by(|left, right| {
        left.logical_key
            .cmp(&right.logical_key)
            .then_with(|| left.exact_key.cmp(&right.exact_key))
    });
    Ok(AssembleFleetCatalogResponseWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        summaries,
        current: selection.decision.current,
        recent_terminal: selection.decision.recent_terminal,
        excluded: selection.decision.excluded,
    })
}

fn project_summary_for_record(
    installation_id: &str,
    record: &AgentArtifactRecordWire,
    liveness: OwnerLivenessWire,
    now_unix: f64,
    project_labels: &BTreeMap<String, String>,
    presentation: &PresentationRecordFacts,
) -> Result<ResolvedAgentSummaryWire, FleetContractError> {
    let mut logical_locator =
        logical_locator_for_record(installation_id, record);
    if let Some(family_id) = &presentation.family_id {
        logical_locator.family_id = Some(family_id.clone());
    }
    let logical_key = logical_locator_key(&logical_locator)?;
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
        .transpose()?;
    let row_kind = row_kind_for_record(record);
    let is_terminal = record.done.is_some()
        || record.workflow_state.as_ref().is_some_and(|state| {
            matches!(
                state.status.to_ascii_lowercase().as_str(),
                "completed" | "failed" | "cancelled" | "noop"
            )
        });
    let presentation_terminal = is_terminal
        || matches!(
            liveness,
            OwnerLivenessWire::Dead | OwnerLivenessWire::NotProcess
        );
    let meta = record.agent_meta.as_ref();
    let summary =
        project_resolved_agent_summary(&ResolvedAgentProjectionRequestWire {
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
                observed_at_unix: now_unix,
                display_status: presentation.display_status.clone(),
                started_at_unix: presentation.started_at_unix,
                run_started_at_unix: presentation.run_started_at_unix,
                stopped_at_unix: stopped_at_unix_for_record(record),
                family_id: presentation.family_id.clone(),
                parent_timestamp: presentation.parent_timestamp.clone(),
                workspace_num: workspace_num_for_record(record),
                project_label: project_labels
                    .get(&record.project_name)
                    .cloned()
                    .or_else(|| Some(record.project_name.clone())),
                agent_clan: meta.and_then(|value| value.agent_clan.clone()),
                agent_clan_generation: meta
                    .and_then(|value| value.agent_clan_generation.clone()),
                clan_tribe: presentation.clan_tribe.clone(),
                tribe: presentation.tribe.clone(),
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
                    resource: Vec::new(),
                    host: Vec::new(),
                    protocol: vec!["fleet.v1".to_string()],
                },
                content_handles: Vec::new(),
            },
        })?;
    if summary.exact_key != exact_key {
        return Err(FleetContractError::Validation("exact_key".to_string()));
    }
    Ok(summary)
}

pub fn row_kind_for_record(
    record: &AgentArtifactRecordWire,
) -> FleetRowKindWire {
    match concrete_family_shell_kind(record) {
        Some(ConcreteFamilyShellKind::Proc) => FleetRowKindWire::Proc,
        Some(ConcreteFamilyShellKind::Monitor) => FleetRowKindWire::Monitor,
        Some(ConcreteFamilyShellKind::Gate) => FleetRowKindWire::Gate,
        _ => FleetRowKindWire::AgentShell,
    }
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
        family_id: family_id_for_record(record)
            .map(|value| safe_identifier(&value, "family")),
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

fn direct_presentation_facts_for_record(
    record: &AgentArtifactRecordWire,
) -> PresentationRecordFacts {
    let meta = record.agent_meta.as_ref();
    PresentationRecordFacts {
        timestamp: record.timestamp.clone(),
        family_id: family_id_for_record(record),
        parent_timestamp: tracked_parent_timestamp(record).map(str::to_string),
        agent_clan: meta.and_then(|value| value.agent_clan.clone()),
        agent_clan_generation: meta
            .and_then(|value| value.agent_clan_generation.clone()),
        clan_tribe: meta.and_then(|value| value.clan_tribe.clone()),
        tribe: meta.and_then(|value| value.tribe.clone()),
        started_at_unix: started_at_unix_for_record(record),
        run_started_at_unix: run_started_at_unix_for_record(record),
        display_status: Some(display_status_for_record(record)),
    }
}

fn display_status_for_record(record: &AgentArtifactRecordWire) -> String {
    if let Some(done) = &record.done {
        if done.repeat_stopped {
            return "STOPPED".to_string();
        }
        if let Some(status) = first_non_empty([done.status_label.as_deref()]) {
            return status.to_string();
        }
        if done
            .error
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty())
            || done.outcome.as_deref().is_some_and(|value| {
                value.starts_with("failed") || value == "failed"
            })
        {
            return "FAILED".to_string();
        }
        return "DONE".to_string();
    }
    if record.pending_question.is_some() {
        return "QUESTION".to_string();
    }
    if record.waiting.is_some() {
        return "WAITING".to_string();
    }
    "RUNNING".to_string()
}

fn started_at_unix_for_record(record: &AgentArtifactRecordWire) -> Option<f64> {
    record
        .workflow_state
        .as_ref()
        .and_then(|state| state.start_time.as_deref())
        .and_then(parse_rfc3339_unix)
        .or_else(|| parse_record_timestamp(&record.timestamp))
}

fn run_started_at_unix_for_record(
    record: &AgentArtifactRecordWire,
) -> Option<f64> {
    record.agent_meta.as_ref().and_then(|meta| {
        meta.run_started_at
            .as_deref()
            .or(meta.wait_completed_at.as_deref())
            .and_then(parse_rfc3339_unix)
    })
}

fn stopped_at_unix_for_record(record: &AgentArtifactRecordWire) -> Option<f64> {
    record
        .done
        .as_ref()
        .and_then(|done| done.finished_at)
        .or_else(|| {
            record
                .agent_meta
                .as_ref()
                .and_then(|meta| meta.stopped_at.as_deref())
                .and_then(parse_rfc3339_unix)
        })
}

fn workspace_num_for_record(record: &AgentArtifactRecordWire) -> Option<u32> {
    record
        .agent_meta
        .as_ref()
        .and_then(|meta| meta.workspace_num)
        .or_else(|| record.done.as_ref().and_then(|done| done.workspace_num))
        .and_then(|value| u32::try_from(value).ok())
}

fn completion_time_for_record(
    record: &AgentArtifactRecordWire,
    now_unix: f64,
) -> f64 {
    if let Some(finished_at) =
        record.done.as_ref().and_then(|done| done.finished_at)
    {
        return finished_at;
    }
    parse_record_timestamp(&record.timestamp).unwrap_or(now_unix)
}

fn parse_record_timestamp(value: &str) -> Option<f64> {
    let parsed = NaiveDateTime::parse_from_str(value, "%Y%m%d%H%M%S").ok()?;
    Some(parsed.and_utc().timestamp() as f64)
}

fn parse_rfc3339_unix(value: &str) -> Option<f64> {
    let parsed = chrono::DateTime::parse_from_rfc3339(value).ok()?;
    Some(
        parsed.timestamp() as f64
            + f64::from(parsed.timestamp_subsec_micros()) / 1_000_000.0,
    )
}

fn clan_context_key(
    agent_clan: Option<&str>,
    agent_clan_generation: Option<&str>,
) -> Option<String> {
    let clan = first_non_empty([agent_clan])?;
    let generation = first_non_empty([agent_clan_generation]).unwrap_or("");
    Some(format!("{clan}\0{generation}"))
}

fn project_display_labels(projects_root: &Path) -> BTreeMap<String, String> {
    list_project_records(projects_root, &[], false, true)
        .unwrap_or_default()
        .into_iter()
        .map(|record| {
            let project_name = record.project_name;
            let display_name =
                record.display_name.unwrap_or_else(|| project_name.clone());
            (project_name, display_name)
        })
        .collect()
}

fn observation_for_record(
    record: &AgentArtifactRecordWire,
    injected: &BTreeMap<String, OwnerProcessObservation>,
    host: &HostOwnerLivenessObserver,
) -> OwnerProcessObservation {
    if injected.is_empty() {
        return host.observe(record);
    }
    if let Some(observation) = injected.get(&record.artifact_dir) {
        return *observation;
    }
    if let Some(name) = record
        .agent_meta
        .as_ref()
        .and_then(|meta| meta.name.as_deref())
    {
        if let Some(observation) = injected.get(name) {
            return *observation;
        }
    }
    OwnerProcessObservation::Unknown
}

fn parse_observations(
    raw: &BTreeMap<String, String>,
) -> BTreeMap<String, OwnerProcessObservation> {
    raw.iter()
        .filter_map(|(key, value)| {
            parse_observation(value)
                .map(|observation| (key.clone(), observation))
        })
        .collect()
}

fn parse_observation(value: &str) -> Option<OwnerProcessObservation> {
    match value.trim().to_ascii_lowercase().as_str() {
        "alive" => Some(OwnerProcessObservation::Alive),
        "dead" => Some(OwnerProcessObservation::Dead),
        "not_process" | "notprocess" => {
            Some(OwnerProcessObservation::NotProcess)
        }
        "unknown" => Some(OwnerProcessObservation::Unknown),
        "identity_mismatch" | "mismatch" => {
            Some(OwnerProcessObservation::IdentityMismatch)
        }
        _ => None,
    }
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

fn first_non_empty<'a>(
    values: impl IntoIterator<Item = Option<&'a str>>,
) -> Option<&'a str> {
    values
        .into_iter()
        .flatten()
        .map(str::trim)
        .find(|value| !value.is_empty())
}

fn wall_clock_unix() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs_f64())
        .unwrap_or(0.0)
}
