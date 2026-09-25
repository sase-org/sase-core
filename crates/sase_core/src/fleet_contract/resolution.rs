use super::catalog::actionable_capability;
use super::content::CapabilitySetWire;
use super::content::ContentHandleWire;
use super::content::ContentMetadataWire;
use super::content::ResourceRevisionWire;
use super::error::FleetContractError;
use super::error::FLEET_CONTRACT_SCHEMA_VERSION;
use super::error::MAX_INTENT_BYTES;
use super::error::MAX_LABEL_BYTES;
use super::locators::instance_key_unchecked;
use super::locators::logical_key_unchecked;
use super::locators::AgentInstanceLocatorWire;
use super::locators::LogicalAgentLocatorWire;
use super::projection::agent_session_role_for_projection;
use super::projection::bucket_for_lifecycle;
use super::projection::content_metadata;
use super::projection::current_instance_locator_schema;
use super::projection::first_non_empty;
use super::projection::fleet_queue_weight_is_valid;
use super::projection::intent_for_record;
use super::projection::lifecycle_for_record;
use super::projection::model_for_record;
use super::projection::normalized_owner_facts;
use super::projection::owner_resolved_logical_locator;
use super::projection::provider_for_record;
use super::projection::queue_capacity_for_record;
use super::projection::queue_capacity_multiplier_for_record;
use super::projection::queue_weight_for_record;
use super::projection::reject_inconsistent_projection;
use super::projection::status_for_record;
use super::projection::terminal_lifecycle;
use super::projection::validate_projection_request;
use super::status::default_row_kind;
use super::status::ConnectionHealthWire;
use super::status::FleetAgentSessionRoleWire;
use super::status::FleetLifecycleWire;
use super::status::FleetRowKindWire;
use super::status::FleetStatusBucketWire;
use super::status::ObservationFreshnessWire;
use super::status::OwnerLivenessWire;
use super::validation::reject_secretish;
use super::validation::trim_to_limit;
use super::validation::validate_identifier;
use super::validation::validate_label;
use super::validation::validate_schema;
use super::validation::validate_timestamp;
use crate::agent_scan::AgentArtifactRecordWire;
use crate::fleet_owner_facts::OwnerPresentationFactsWire;
use serde::{Deserialize, Serialize};

/// Owner facts produced outside this pure projection layer.
///
/// These facts may come from PID checks, content availability checks, and
/// connection probes, but this module never performs those checks itself.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnerResolutionFactsWire {
    pub schema_version: u32,
    pub exact_locator: Option<AgentInstanceLocatorWire>,
    pub row_revision: ResourceRevisionWire,
    pub liveness: OwnerLivenessWire,
    pub connection_health: ConnectionHealthWire,
    pub freshness: ObservationFreshnessWire,
    pub observed_at_unix: f64,
    #[serde(default)]
    pub display_status: Option<String>,
    #[serde(default)]
    pub started_at_unix: Option<f64>,
    #[serde(default)]
    pub run_started_at_unix: Option<f64>,
    #[serde(default)]
    pub stopped_at_unix: Option<f64>,
    #[serde(default, rename = "family_id", alias = "agent_session_id")]
    pub agent_session_id: Option<String>,
    #[serde(default)]
    pub parent_timestamp: Option<String>,
    #[serde(default)]
    pub workspace_num: Option<u32>,
    #[serde(default)]
    pub project_label: Option<String>,
    #[serde(default)]
    pub agent_clan: Option<String>,
    #[serde(default)]
    pub agent_clan_generation: Option<String>,
    #[serde(default)]
    pub clan_tribe: Option<String>,
    #[serde(default)]
    pub tribe: Option<String>,
    /// Owner-derived shell, plan, question, retry, and lifecycle facts.
    #[serde(default)]
    pub presentation: OwnerPresentationFactsWire,
    #[serde(default = "default_row_kind")]
    pub row_kind: FleetRowKindWire,
    #[serde(default)]
    pub current_instance: bool,
    #[serde(default)]
    pub dismissable: bool,
    #[serde(default)]
    pub needs_attention: bool,
    #[serde(default)]
    pub occupied_runner_slot: bool,
    #[serde(default)]
    pub container_projected_concrete_agent: bool,
    pub capabilities: CapabilitySetWire,
    #[serde(default)]
    pub content_handles: Vec<ContentHandleWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HumanDisplayLabelsWire {
    pub schema_version: u32,
    pub project_label: String,
    pub agent_label: Option<String>,
    #[serde(rename = "family_label", alias = "agent_session_label")]
    pub agent_session_label: Option<String>,
    pub owner_label: Option<String>,
    pub alias: Option<String>,
}

/// Request to build a safe owner-resolved summary from an artifact record.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedAgentProjectionRequestWire {
    pub schema_version: u32,
    pub record: AgentArtifactRecordWire,
    pub logical_locator: LogicalAgentLocatorWire,
    pub owner_facts: OwnerResolutionFactsWire,
}

/// Safe row projection for fleet lists.
///
/// It carries locators, labels, provider/model display data, lifecycle,
/// liveness, health, freshness, row/resource revision, capabilities, and
/// bounded content metadata. It never serializes local artifact directories,
/// checkout paths, marker paths, output/response paths, PIDs, process groups,
/// raw credentials, or auth headers.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedAgentSummaryWire {
    pub schema_version: u32,
    pub logical_locator: LogicalAgentLocatorWire,
    pub exact_locator: Option<AgentInstanceLocatorWire>,
    pub logical_key: String,
    pub exact_key: Option<String>,
    pub row_kind: FleetRowKindWire,
    #[serde(rename = "family_role", alias = "agent_session_role")]
    pub agent_session_role: FleetAgentSessionRoleWire,
    /// Parent's record identity, when this row is a tracked agent session member.
    /// `None` for roots and rows with no tracked agent session lineage.
    pub parent_timestamp: Option<String>,
    pub labels: HumanDisplayLabelsWire,
    pub project_name: String,
    pub model: Option<String>,
    pub provider: Option<String>,
    pub status: String,
    pub status_bucket: FleetStatusBucketWire,
    pub intent: Option<String>,
    pub observed_at_unix: f64,
    #[serde(default)]
    pub started_at_unix: Option<f64>,
    #[serde(default)]
    pub run_started_at_unix: Option<f64>,
    #[serde(default)]
    pub stopped_at_unix: Option<f64>,
    #[serde(default)]
    pub workspace_num: Option<u32>,
    #[serde(default)]
    pub agent_clan: Option<String>,
    #[serde(default)]
    pub agent_clan_generation: Option<String>,
    #[serde(default)]
    pub clan_tribe: Option<String>,
    #[serde(default)]
    pub tribe: Option<String>,
    /// Owner-derived shell, plan, question, retry, and lifecycle facts. Never
    /// carries paths; omitted when empty so legacy consumers see no change.
    #[serde(
        default,
        skip_serializing_if = "OwnerPresentationFactsWire::is_empty"
    )]
    pub presentation: OwnerPresentationFactsWire,
    pub row_revision: ResourceRevisionWire,
    pub lifecycle: FleetLifecycleWire,
    pub liveness: OwnerLivenessWire,
    pub connection_health: ConnectionHealthWire,
    pub freshness: ObservationFreshnessWire,
    pub capabilities: CapabilitySetWire,
    pub content: ContentMetadataWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_capacity: Option<u32>,
    #[serde(default)]
    pub queue_capacity_explicit: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_capacity_multiplier: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_weight: Option<f64>,
    #[serde(default)]
    pub queue_weight_explicit: bool,
    #[serde(default)]
    pub queue_weight_invalid: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_weight_error: Option<String>,
    pub current_instance: bool,
    pub dismissable: bool,
    pub needs_attention: bool,
    pub occupied_runner_slot: bool,
    pub container_projected_concrete_agent: bool,
}

/// Lazy safe detail projection. Content is exposed through opaque handles.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedAgentDetailWire {
    pub schema_version: u32,
    pub summary: ResolvedAgentSummaryWire,
    pub content_handles: Vec<ContentHandleWire>,
}

pub fn project_resolved_agent_summary(
    request: &ResolvedAgentProjectionRequestWire,
) -> Result<ResolvedAgentSummaryWire, FleetContractError> {
    validate_projection_request(request)?;
    let facts = normalized_owner_facts(&request.owner_facts)?;
    let logical_locator =
        owner_resolved_logical_locator(&request.logical_locator, &facts);
    let exact_locator = facts
        .exact_locator
        .as_ref()
        .map(current_instance_locator_schema);
    let logical_key = logical_key_unchecked(&logical_locator);
    let exact_key = exact_locator.as_ref().map(instance_key_unchecked);
    let lifecycle = lifecycle_for_record(&request.record);
    reject_inconsistent_projection(&request.record, &facts, lifecycle)?;
    let status = facts
        .display_status
        .clone()
        .unwrap_or_else(|| status_for_record(&request.record));
    let meta = request.record.agent_meta.as_ref();
    let done = request.record.done.as_ref();
    let running = request.record.running.as_ref();
    let (
        queue_weight,
        queue_weight_explicit,
        queue_weight_invalid,
        queue_weight_error,
    ) = queue_weight_for_record(&request.record);
    let (queue_capacity, queue_capacity_explicit) =
        queue_capacity_for_record(&request.record);
    let queue_capacity_multiplier =
        queue_capacity_multiplier_for_record(&request.record);
    let agent_session = meta
        .and_then(|value| value.agent_session_shell.as_ref())
        .or_else(|| done.and_then(|value| value.agent_session_shell.as_ref()));
    let parent_timestamp = facts.parent_timestamp.clone().or_else(|| {
        meta.and_then(|value| {
            first_non_empty([
                value.parent_timestamp.as_deref(),
                value.parent_agent_timestamp.as_deref(),
            ])
            .map(str::to_string)
        })
    });
    let agent_session_role = agent_session_role_for_projection(
        facts.row_kind,
        lifecycle,
        facts.liveness,
        parent_timestamp.is_some()
            || crate::fleet_agent_session::record_is_concrete_agent_session_shell(
                &request.record,
            ),
    );
    let project_label = facts
        .project_label
        .as_deref()
        .unwrap_or(request.record.project_name.as_str());
    let labels = HumanDisplayLabelsWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        project_label: trim_to_limit(project_label, MAX_LABEL_BYTES),
        agent_label: first_non_empty([
            meta.and_then(|value| value.name.as_deref()),
            done.and_then(|value| value.name.as_deref()),
            agent_session.and_then(|value| value.label.as_deref()),
        ])
        .map(|value| trim_to_limit(value, MAX_LABEL_BYTES)),
        agent_session_label: first_non_empty([
            meta.and_then(|value| value.agent_session.as_deref()),
            agent_session.and_then(|value| value.label.as_deref()),
        ])
        .map(|value| trim_to_limit(value, MAX_LABEL_BYTES)),
        owner_label: None,
        alias: None,
    };
    let summary = ResolvedAgentSummaryWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        logical_locator,
        exact_locator,
        logical_key,
        exact_key,
        row_kind: facts.row_kind,
        agent_session_role,
        parent_timestamp,
        labels,
        project_name: trim_to_limit(
            &request.record.project_name,
            MAX_LABEL_BYTES,
        ),
        model: model_for_record(meta, done, running),
        provider: provider_for_record(meta, done, running),
        status,
        status_bucket: bucket_for_lifecycle(lifecycle, facts.liveness),
        intent: intent_for_record(&request.record),
        observed_at_unix: facts.observed_at_unix,
        started_at_unix: facts.started_at_unix,
        run_started_at_unix: facts.run_started_at_unix,
        stopped_at_unix: facts.stopped_at_unix,
        workspace_num: facts.workspace_num,
        agent_clan: facts.agent_clan.clone(),
        agent_clan_generation: facts.agent_clan_generation.clone(),
        clan_tribe: facts.clan_tribe.clone(),
        tribe: facts.tribe.clone(),
        presentation: facts.presentation.clone(),
        row_revision: facts.row_revision.clone(),
        lifecycle,
        liveness: facts.liveness,
        connection_health: facts.connection_health,
        freshness: facts.freshness,
        capabilities: facts.capabilities.clone(),
        content: content_metadata(&facts.content_handles)?,
        queue_capacity,
        queue_capacity_explicit,
        queue_capacity_multiplier,
        queue_weight,
        queue_weight_explicit,
        queue_weight_invalid,
        queue_weight_error,
        current_instance: facts.current_instance,
        dismissable: facts.dismissable,
        needs_attention: facts.needs_attention
            || lifecycle == FleetLifecycleWire::Asking,
        occupied_runner_slot: facts.occupied_runner_slot,
        container_projected_concrete_agent: facts
            .container_projected_concrete_agent,
    };
    validate_resolved_agent_summary(&summary)
}

pub fn project_resolved_agent_detail(
    request: &ResolvedAgentProjectionRequestWire,
) -> Result<ResolvedAgentDetailWire, FleetContractError> {
    let summary = project_resolved_agent_summary(request)?;
    let facts = normalized_owner_facts(&request.owner_facts)?;
    Ok(ResolvedAgentDetailWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        summary,
        content_handles: facts.content_handles,
    })
}

pub fn validate_resolved_agent_summary(
    summary: &ResolvedAgentSummaryWire,
) -> Result<ResolvedAgentSummaryWire, FleetContractError> {
    validate_schema("resolved agent summary", summary.schema_version)?;
    summary.logical_locator.validate()?;
    if let Some(exact) = &summary.exact_locator {
        exact.validate()?;
        if exact.logical != summary.logical_locator {
            return Err(FleetContractError::Validation(
                "exact locator logical identity does not match summary logical locator"
                    .to_string(),
            ));
        }
    }
    let logical_key = logical_key_unchecked(&summary.logical_locator);
    if summary.logical_key != logical_key {
        return Err(FleetContractError::Validation(
            "summary logical_key does not match logical locator".to_string(),
        ));
    }
    if summary.exact_key
        != summary.exact_locator.as_ref().map(instance_key_unchecked)
    {
        return Err(FleetContractError::Validation(
            "summary exact_key does not match exact locator".to_string(),
        ));
    }
    summary.row_revision.validate()?;
    if summary.row_revision.logical_key != summary.logical_key {
        return Err(FleetContractError::Validation(
            "row revision belongs to a different logical identity".to_string(),
        ));
    }
    validate_timestamp("observed_at_unix", summary.observed_at_unix)?;
    if let Some(started_at) = summary.started_at_unix {
        validate_timestamp("started_at_unix", started_at)?;
    }
    if let Some(run_started_at) = summary.run_started_at_unix {
        validate_timestamp("run_started_at_unix", run_started_at)?;
    }
    if let Some(stopped_at) = summary.stopped_at_unix {
        validate_timestamp("stopped_at_unix", stopped_at)?;
    }
    if let (Some(started_at), Some(stopped_at)) =
        (summary.started_at_unix, summary.stopped_at_unix)
    {
        if stopped_at < started_at {
            return Err(FleetContractError::Validation(
                "summary stopped_at_unix must be greater than or equal to started_at_unix"
                    .to_string(),
            ));
        }
    }
    if let (Some(run_started_at), Some(stopped_at)) =
        (summary.run_started_at_unix, summary.stopped_at_unix)
    {
        if stopped_at < run_started_at {
            return Err(FleetContractError::Validation(
                "summary stopped_at_unix must be greater than or equal to run_started_at_unix"
                    .to_string(),
            ));
        }
    }
    if let Some(parent_timestamp) = &summary.parent_timestamp {
        validate_identifier("parent_timestamp", parent_timestamp)?;
    }
    summary.presentation.validate()?;
    validate_label("project_name", &summary.project_name, MAX_LABEL_BYTES)?;
    validate_label(
        "labels.project_label",
        &summary.labels.project_label,
        MAX_LABEL_BYTES,
    )?;
    for (field, value) in [
        ("status", Some(summary.status.as_str())),
        ("model", summary.model.as_deref()),
        ("provider", summary.provider.as_deref()),
        ("intent", summary.intent.as_deref()),
        ("agent_clan", summary.agent_clan.as_deref()),
        (
            "agent_clan_generation",
            summary.agent_clan_generation.as_deref(),
        ),
        ("clan_tribe", summary.clan_tribe.as_deref()),
        ("tribe", summary.tribe.as_deref()),
        ("labels.agent_label", summary.labels.agent_label.as_deref()),
        (
            "labels.agent_session_label",
            summary.labels.agent_session_label.as_deref(),
        ),
        ("labels.owner_label", summary.labels.owner_label.as_deref()),
        ("labels.alias", summary.labels.alias.as_deref()),
    ] {
        if let Some(value) = value {
            validate_label(field, value, MAX_INTENT_BYTES)?;
            reject_secretish(field, value)?;
        }
    }
    if let Some(weight) = summary.queue_weight {
        if !fleet_queue_weight_is_valid(weight, summary.queue_weight_explicit) {
            return Err(FleetContractError::Validation(
                "summary queue_weight must be a positive finite capacity \
                 weight, or an explicit zero"
                    .to_string(),
            ));
        }
    }
    if summary.queue_weight_invalid && summary.queue_weight.is_some() {
        return Err(FleetContractError::Validation(
            "summary queue_weight cannot be present when queue_weight_invalid is true"
                .to_string(),
        ));
    }
    if !summary.queue_weight_invalid && summary.queue_weight_error.is_some() {
        return Err(FleetContractError::Validation(
            "summary queue_weight_error requires queue_weight_invalid"
                .to_string(),
        ));
    }
    if let Some(error) = &summary.queue_weight_error {
        validate_label("queue_weight_error", error, MAX_INTENT_BYTES)?;
        reject_secretish("queue_weight_error", error)?;
    }
    if summary.queue_capacity.is_none() && summary.queue_capacity_explicit {
        return Err(FleetContractError::Validation(
            "summary queue_capacity_explicit requires queue_capacity"
                .to_string(),
        ));
    }
    if !summary.capabilities.content_is_normalized()? {
        return Err(FleetContractError::Validation(
            "summary capabilities are not normalized".to_string(),
        ));
    }
    summary.content.validate()?;
    if terminal_lifecycle(summary.lifecycle)
        && summary.liveness == OwnerLivenessWire::Alive
    {
        return Err(FleetContractError::Validation(
            "terminal agent summary cannot have owner liveness alive"
                .to_string(),
        ));
    }
    if summary
        .capabilities
        .resource
        .iter()
        .any(|capability| actionable_capability(capability))
        && summary.exact_locator.is_none()
    {
        return Err(FleetContractError::Validation(
            "actionable resource capability requires an exact instance locator"
                .to_string(),
        ));
    }
    let agent_session_role_matches_row_kind = match summary.row_kind {
        FleetRowKindWire::Proc => {
            summary.agent_session_role == FleetAgentSessionRoleWire::Proc
        }
        FleetRowKindWire::Monitor => {
            summary.agent_session_role == FleetAgentSessionRoleWire::Monitor
        }
        FleetRowKindWire::Gate => {
            summary.agent_session_role == FleetAgentSessionRoleWire::Gate
        }
        FleetRowKindWire::AgentShell
        | FleetRowKindWire::ContainerHeader
        | FleetRowKindWire::HistoricalShell => matches!(
            summary.agent_session_role,
            FleetAgentSessionRoleWire::Root
                | FleetAgentSessionRoleWire::Member
                | FleetAgentSessionRoleWire::HistoricalShell
        ),
    };
    if !agent_session_role_matches_row_kind {
        return Err(FleetContractError::Validation(
            "summary agent session role is inconsistent with row_kind"
                .to_string(),
        ));
    }
    Ok(summary.clone())
}
