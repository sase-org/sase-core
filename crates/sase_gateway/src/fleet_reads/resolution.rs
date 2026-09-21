//! Record resolution: project index records into served summaries,
//! details, and content handles.

use std::collections::BTreeMap;

use sase_core::{
    agent_scan::AgentArtifactRecordWire,
    fleet_attention::{
        FLEET_ATTENTION_CAPABILITY_ANSWER_QUESTION,
        FLEET_ATTENTION_CAPABILITY_APPROVE_GATE,
    },
    fleet_catalog::{
        row_kind_for_record, stable_revision, PresentationRecordFacts,
    },
    fleet_contract::{
        classify_cache_freshness, instance_locator_key, logical_locator_key,
        project_resolved_agent_detail, AgentInstanceLocatorWire,
        CacheFreshnessRequestWire, CapabilitySetWire, ConnectionHealthWire,
        ContentHandleWire, FleetRowKindWire, LogicalAgentLocatorWire,
        ObservationFreshnessWire, OriginLocatorWire, OwnerLivenessWire,
        OwnerResolutionFactsWire, ProjectLocatorWire, ResolvedAgentDetailWire,
        ResolvedAgentProjectionRequestWire, ResourceRevisionWire,
        FLEET_CONTRACT_SCHEMA_VERSION,
    },
    fleet_family::{family_id_for_record, family_shell},
    fleet_mutation::{
        FLEET_MUTATION_CAPABILITY_FORK, FLEET_MUTATION_CAPABILITY_RETRY,
        FLEET_MUTATION_CAPABILITY_STOP,
    },
};
use sha2::{Digest, Sha256};

use super::{
    content::{content_handles_for_record, FleetContentSource},
    errors::FleetReadError,
};

/// Below this age a served snapshot reads as `Fresh`.
pub(super) const FLEET_SNAPSHOT_FRESH_SECONDS: f64 = 5.0;
/// At or beyond this age a served snapshot reads as `Stale` and a read that
/// is not already holding the refresh lock requires a rebuild attempt
/// instead of serving the cached entry outright.
pub(super) const FLEET_SNAPSHOT_STALE_SECONDS: f64 = 60.0;

pub(super) struct ResolvedRecord {
    pub(super) detail: ResolvedAgentDetailWire,
    pub(super) content_sources: Vec<FleetContentSource>,
}

pub(super) fn resolve_record(
    installation_id: &str,
    record: &AgentArtifactRecordWire,
    liveness: OwnerLivenessWire,
    build_unix: f64,
    project_labels: &BTreeMap<String, String>,
    presentation: &PresentationRecordFacts,
) -> Result<ResolvedRecord, FleetReadError> {
    let mut logical_locator =
        logical_locator_for_record(installation_id, record);
    if let Some(family_id) = &presentation.family_id {
        logical_locator.family_id = Some(family_id.clone());
    }
    let logical_key =
        logical_locator_key(&logical_locator).map_err(FleetReadError::from)?;
    let row_revision = ResourceRevisionWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        logical_key: logical_key.clone(),
        revision: stable_revision(record, presentation),
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
    // A Dead/NotProcess record is terminal for presentation even when a
    // waiting/question marker is still on disk: it keeps its recorded
    // lifecycle/status but loses current-instance and action capabilities.
    let presentation_terminal = is_terminal
        || matches!(
            liveness,
            OwnerLivenessWire::Dead | OwnerLivenessWire::NotProcess
        );
    let resource_caps = lifecycle_and_content_capabilities(
        row_kind,
        presentation_terminal,
        liveness,
        &content_handles,
        record.pending_question.is_some(),
    );
    let meta = record.agent_meta.as_ref();
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
                display_status: presentation.display_status.clone(),
                presentation: presentation.owner.clone(),
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

pub(super) fn parse_rfc3339_unix(value: &str) -> Option<f64> {
    let parsed = chrono::DateTime::parse_from_rfc3339(value).ok()?;
    Some(
        parsed.timestamp() as f64
            + f64::from(parsed.timestamp_subsec_micros()) / 1_000_000.0,
    )
}

pub(super) fn observation_freshness_for_age(
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

#[cfg(test)]
pub(super) fn parse_record_timestamp(value: &str) -> Option<f64> {
    let parsed =
        chrono::NaiveDateTime::parse_from_str(value, "%Y%m%d%H%M%S").ok()?;
    Some(parsed.and_utc().timestamp() as f64)
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

pub(super) fn safe_reason(value: &str) -> String {
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
