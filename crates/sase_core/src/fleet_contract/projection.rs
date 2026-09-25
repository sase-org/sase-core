use super::catalog::actionable_capability;
use super::catalog::content_capability;
use super::content::ContentHandleWire;
use super::content::ContentMetadataWire;
use super::error::FleetContractError;
use super::error::FLEET_CONTRACT_SCHEMA_VERSION;
use super::error::MAX_INTENT_BYTES;
use super::error::MAX_LABEL_BYTES;
use super::locators::logical_key_matches;
use super::locators::AgentInstanceLocatorWire;
use super::locators::LogicalAgentLocatorWire;
use super::locators::OriginLocatorWire;
use super::locators::ProjectLocatorWire;
use super::resolution::OwnerResolutionFactsWire;
use super::resolution::ResolvedAgentProjectionRequestWire;
use super::resolution::ResolvedAgentSummaryWire;
use super::status::FleetAgentSessionRoleWire;
use super::status::FleetLifecycleWire;
use super::status::FleetRowKindWire;
use super::status::FleetStatusBucketWire;
use super::status::OwnerLivenessWire;
use super::validation::reject_secretish;
use super::validation::trim_to_limit;
use super::validation::validate_identifier;
use super::validation::validate_label;
use super::validation::validate_schema;
use super::validation::validate_timestamp;
use crate::agent_scan::{
    AgentArtifactRecordWire, AgentMetaWire, DoneMarkerWire, RunningMarkerWire,
};
use crate::queue_directive::{
    authored_queue_weight_is_valid, queue_capacity_as_u32,
    queue_weight_is_valid, resolve_queue_capacity,
};
use std::collections::BTreeSet;

fn current_origin_locator_schema(
    origin: &OriginLocatorWire,
) -> OriginLocatorWire {
    OriginLocatorWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        installation_id: origin.installation_id.clone(),
    }
}

pub(crate) fn current_project_locator_schema(
    project: &ProjectLocatorWire,
) -> ProjectLocatorWire {
    ProjectLocatorWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        origin: current_origin_locator_schema(&project.origin),
        project_id: project.project_id.clone(),
    }
}

fn current_logical_locator_schema(
    logical: &LogicalAgentLocatorWire,
) -> LogicalAgentLocatorWire {
    LogicalAgentLocatorWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        project: current_project_locator_schema(&logical.project),
        agent_id: logical.agent_id.clone(),
        agent_session_id: logical.agent_session_id.clone(),
    }
}

pub(crate) fn owner_resolved_logical_locator(
    logical: &LogicalAgentLocatorWire,
    facts: &OwnerResolutionFactsWire,
) -> LogicalAgentLocatorWire {
    let mut logical = current_logical_locator_schema(logical);
    if let Some(agent_session_id) = &facts.agent_session_id {
        logical.agent_session_id = Some(agent_session_id.clone());
    }
    logical
}

pub(crate) fn current_instance_locator_schema(
    exact: &AgentInstanceLocatorWire,
) -> AgentInstanceLocatorWire {
    AgentInstanceLocatorWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        logical: current_logical_locator_schema(&exact.logical),
        shell_id: exact.shell_id.clone(),
        run_id: exact.run_id.clone(),
        attempt_id: exact.attempt_id.clone(),
    }
}

pub(crate) fn validate_projection_request(
    request: &ResolvedAgentProjectionRequestWire,
) -> Result<(), FleetContractError> {
    validate_schema(
        "resolved agent projection request",
        request.schema_version,
    )?;
    request.logical_locator.validate()?;
    let facts = normalized_owner_facts(&request.owner_facts)?;
    let logical_locator =
        owner_resolved_logical_locator(&request.logical_locator, &facts);
    facts.row_revision.validate()?;
    if !logical_key_matches(&facts.row_revision.logical_key, &logical_locator) {
        return Err(FleetContractError::Validation(
            "row revision belongs to a different logical identity".to_string(),
        ));
    }
    if let Some(exact) = &facts.exact_locator {
        if exact.logical != logical_locator {
            return Err(FleetContractError::Validation(
                "owner facts exact locator belongs to a different logical identity"
                    .to_string(),
            ));
        }
    }
    validate_label(
        "record.project_name",
        &request.record.project_name,
        MAX_LABEL_BYTES,
    )?;
    validate_label(
        "record.workflow_dir_name",
        &request.record.workflow_dir_name,
        MAX_LABEL_BYTES,
    )?;
    Ok(())
}

pub(crate) fn normalized_owner_facts(
    facts: &OwnerResolutionFactsWire,
) -> Result<OwnerResolutionFactsWire, FleetContractError> {
    validate_schema("owner resolution facts", facts.schema_version)?;
    if let Some(exact) = &facts.exact_locator {
        exact.validate()?;
    }
    facts.row_revision.validate()?;
    validate_timestamp("observed_at_unix", facts.observed_at_unix)?;
    if let Some(started_at) = facts.started_at_unix {
        validate_timestamp("started_at_unix", started_at)?;
    }
    if let Some(run_started_at) = facts.run_started_at_unix {
        validate_timestamp("run_started_at_unix", run_started_at)?;
    }
    if let Some(stopped_at) = facts.stopped_at_unix {
        validate_timestamp("stopped_at_unix", stopped_at)?;
    }
    if let (Some(run_started_at), Some(stopped_at)) =
        (facts.run_started_at_unix, facts.stopped_at_unix)
    {
        if stopped_at < run_started_at {
            return Err(FleetContractError::Validation(
                "owner facts stopped_at_unix must be greater than or equal to run_started_at_unix"
                    .to_string(),
            ));
        }
    }
    if let Some(agent_session_id) = &facts.agent_session_id {
        validate_identifier("family_id", agent_session_id)?;
    }
    if let Some(parent_timestamp) = &facts.parent_timestamp {
        validate_identifier("parent_timestamp", parent_timestamp)?;
    }
    for (field, value) in [
        ("display_status", facts.display_status.as_deref()),
        ("project_label", facts.project_label.as_deref()),
        ("agent_clan", facts.agent_clan.as_deref()),
        (
            "agent_clan_generation",
            facts.agent_clan_generation.as_deref(),
        ),
        ("clan_tribe", facts.clan_tribe.as_deref()),
        ("tribe", facts.tribe.as_deref()),
    ] {
        if let Some(value) = value {
            validate_label(field, value, MAX_LABEL_BYTES)?;
            reject_secretish(field, value)?;
        }
    }
    let capabilities = facts.capabilities.normalized()?;
    let mut content_handles = facts.content_handles.clone();
    for handle in &content_handles {
        handle.validate()?;
    }
    content_handles.sort_by(|left, right| {
        (left.kind, left.id.as_str()).cmp(&(right.kind, right.id.as_str()))
    });
    content_handles
        .dedup_by(|left, right| left.kind == right.kind && left.id == right.id);
    Ok(OwnerResolutionFactsWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        exact_locator: facts
            .exact_locator
            .as_ref()
            .map(current_instance_locator_schema),
        row_revision: facts.row_revision.clone(),
        liveness: facts.liveness,
        connection_health: facts.connection_health,
        freshness: facts.freshness,
        observed_at_unix: facts.observed_at_unix,
        display_status: facts
            .display_status
            .as_ref()
            .map(|value| trim_to_limit(value, MAX_LABEL_BYTES)),
        started_at_unix: facts.started_at_unix,
        run_started_at_unix: facts.run_started_at_unix,
        stopped_at_unix: facts.stopped_at_unix,
        agent_session_id: facts
            .agent_session_id
            .as_ref()
            .map(|value| value.trim().to_string()),
        parent_timestamp: facts
            .parent_timestamp
            .as_ref()
            .map(|value| value.trim().to_string()),
        workspace_num: facts.workspace_num,
        project_label: facts
            .project_label
            .as_ref()
            .map(|value| trim_to_limit(value, MAX_LABEL_BYTES)),
        agent_clan: facts
            .agent_clan
            .as_ref()
            .map(|value| trim_to_limit(value, MAX_LABEL_BYTES)),
        agent_clan_generation: facts
            .agent_clan_generation
            .as_ref()
            .map(|value| trim_to_limit(value, MAX_LABEL_BYTES)),
        clan_tribe: facts
            .clan_tribe
            .as_ref()
            .map(|value| trim_to_limit(value, MAX_LABEL_BYTES)),
        tribe: facts
            .tribe
            .as_ref()
            .map(|value| trim_to_limit(value, MAX_LABEL_BYTES)),
        presentation: facts.presentation.sanitized()?,
        row_kind: facts.row_kind,
        current_instance: facts.current_instance,
        dismissable: facts.dismissable,
        needs_attention: facts.needs_attention,
        occupied_runner_slot: facts.occupied_runner_slot,
        container_projected_concrete_agent: facts
            .container_projected_concrete_agent,
        capabilities,
        content_handles,
    })
}

pub(crate) fn reject_inconsistent_projection(
    record: &AgentArtifactRecordWire,
    facts: &OwnerResolutionFactsWire,
    lifecycle: FleetLifecycleWire,
) -> Result<(), FleetContractError> {
    if terminal_lifecycle(lifecycle)
        && facts.liveness == OwnerLivenessWire::Alive
    {
        return Err(FleetContractError::Validation(
            "owner facts mark a terminal record as live".to_string(),
        ));
    }
    if facts
        .capabilities
        .resource
        .iter()
        .any(|capability| actionable_capability(capability))
        && facts.exact_locator.is_none()
    {
        return Err(FleetContractError::Validation(
            "actionable resource capability requires an exact instance locator"
                .to_string(),
        ));
    }
    if facts
        .capabilities
        .resource
        .iter()
        .any(|capability| content_capability(capability))
        && facts.content_handles.is_empty()
    {
        return Err(FleetContractError::Validation(
            "content resource capability requires an opaque content handle"
                .to_string(),
        ));
    }
    if facts.row_kind == FleetRowKindWire::Proc
        && facts
            .capabilities
            .resource
            .iter()
            .any(|capability| actionable_capability(capability))
    {
        return Err(FleetContractError::Validation(
            "proc rows cannot expose agent mutation capabilities".to_string(),
        ));
    }
    if record
        .agent_meta
        .as_ref()
        .and_then(|meta| meta.proc_id.as_ref())
        .is_some()
        && facts.row_kind == FleetRowKindWire::AgentShell
    {
        return Err(FleetContractError::Validation(
            "proc artifact records must not be projected as agent shells"
                .to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn counts_as_running(summary: &ResolvedAgentSummaryWire) -> bool {
    // Require compatible live owner facts rather than trusting the status
    // bucket alone: a bucket that claims Running/Starting can never count
    // toward authoritative running counts when the owner-resolved liveness
    // is definitively Dead or NotProcess.
    let live_compatible = matches!(
        summary.liveness,
        OwnerLivenessWire::Alive | OwnerLivenessWire::Unknown
    );
    if !live_compatible {
        return false;
    }
    match summary.status_bucket {
        FleetStatusBucketWire::Running => !summary.dismissable,
        FleetStatusBucketWire::Starting => {
            summary.container_projected_concrete_agent
        }
        _ => false,
    }
}

/// Whether a row's presentation should be treated as terminal ("was
/// running") for agent-session-role and status-bucket purposes: genuinely terminal
/// lifecycle, or definitively `Dead`/`NotProcess` liveness — unless a
/// waiting/question marker protects it.
fn presentation_is_historical(
    lifecycle: FleetLifecycleWire,
    liveness: OwnerLivenessWire,
) -> bool {
    if matches!(
        lifecycle,
        FleetLifecycleWire::Waiting | FleetLifecycleWire::Asking
    ) {
        return false;
    }
    terminal_lifecycle(lifecycle)
        || matches!(
            liveness,
            OwnerLivenessWire::Dead | OwnerLivenessWire::NotProcess
        )
}

pub(crate) fn agent_session_role_for_projection(
    row_kind: FleetRowKindWire,
    lifecycle: FleetLifecycleWire,
    liveness: OwnerLivenessWire,
    has_parent: bool,
) -> FleetAgentSessionRoleWire {
    match row_kind {
        FleetRowKindWire::Proc => FleetAgentSessionRoleWire::Proc,
        FleetRowKindWire::Monitor => FleetAgentSessionRoleWire::Monitor,
        FleetRowKindWire::Gate => FleetAgentSessionRoleWire::Gate,
        FleetRowKindWire::AgentShell
        | FleetRowKindWire::ContainerHeader
        | FleetRowKindWire::HistoricalShell => {
            if presentation_is_historical(lifecycle, liveness) {
                FleetAgentSessionRoleWire::HistoricalShell
            } else if has_parent {
                FleetAgentSessionRoleWire::Member
            } else {
                FleetAgentSessionRoleWire::Root
            }
        }
    }
}

pub(crate) fn lifecycle_for_record(
    record: &AgentArtifactRecordWire,
) -> FleetLifecycleWire {
    if let Some(done) = &record.done {
        if done
            .error
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty())
            || done.outcome.as_deref().is_some_and(|value| {
                value.starts_with("failed") || value == "failed"
            })
            || done
                .status_label
                .as_deref()
                .is_some_and(|value| value.starts_with("FAILED"))
        {
            return FleetLifecycleWire::Failed;
        }
        return FleetLifecycleWire::Terminal;
    }
    if record.pending_question.is_some() {
        return FleetLifecycleWire::Asking;
    }
    if record.waiting.is_some() {
        return FleetLifecycleWire::Waiting;
    }
    if workflow_status_is_starting(record) {
        return FleetLifecycleWire::Starting;
    }
    if record.running.is_some() || workflow_status_is_running(record) {
        return FleetLifecycleWire::Running;
    }
    FleetLifecycleWire::Unknown
}

pub(crate) fn terminal_lifecycle(lifecycle: FleetLifecycleWire) -> bool {
    matches!(
        lifecycle,
        FleetLifecycleWire::Terminal | FleetLifecycleWire::Failed
    )
}

pub(crate) fn bucket_for_lifecycle(
    lifecycle: FleetLifecycleWire,
    liveness: OwnerLivenessWire,
) -> FleetStatusBucketWire {
    // A record whose owner-resolved liveness is definitively Dead or
    // NotProcess can never bucket as Running/Starting, no matter what its
    // lifecycle label says: the four-fact model presents it as stopped
    // instead of fabricating an active state.
    let liveness_stops = matches!(
        liveness,
        OwnerLivenessWire::Dead | OwnerLivenessWire::NotProcess
    );
    match lifecycle {
        FleetLifecycleWire::Starting if liveness_stops => {
            FleetStatusBucketWire::Stopped
        }
        FleetLifecycleWire::Starting => FleetStatusBucketWire::Starting,
        FleetLifecycleWire::Running | FleetLifecycleWire::Unknown
            if liveness_stops =>
        {
            FleetStatusBucketWire::Stopped
        }
        FleetLifecycleWire::Running | FleetLifecycleWire::Unknown => {
            FleetStatusBucketWire::Running
        }
        FleetLifecycleWire::Waiting => FleetStatusBucketWire::Waiting,
        FleetLifecycleWire::Asking => FleetStatusBucketWire::Stopped,
        FleetLifecycleWire::Terminal => FleetStatusBucketWire::Done,
        FleetLifecycleWire::Failed => FleetStatusBucketWire::Failed,
    }
}

pub(crate) fn status_for_record(record: &AgentArtifactRecordWire) -> String {
    if let Some(done) = &record.done {
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
    if workflow_status_is_starting(record) {
        return "STARTING".to_string();
    }
    if record.running.is_some() || workflow_status_is_running(record) {
        return "RUNNING".to_string();
    }
    "UNKNOWN".to_string()
}

fn workflow_status_is_starting(record: &AgentArtifactRecordWire) -> bool {
    record.workflow_state.as_ref().is_some_and(|state| {
        state.appears_as_agent && state.status.eq_ignore_ascii_case("starting")
    })
}

fn workflow_status_is_running(record: &AgentArtifactRecordWire) -> bool {
    record.workflow_state.as_ref().is_some_and(|state| {
        state.appears_as_agent
            && matches!(
                state.status.to_ascii_lowercase().as_str(),
                "running" | "waiting" | "queued"
            )
    })
}

pub(crate) fn model_for_record(
    meta: Option<&AgentMetaWire>,
    done: Option<&DoneMarkerWire>,
    running: Option<&RunningMarkerWire>,
) -> Option<String> {
    first_non_empty([
        meta.and_then(|value| value.model.as_deref()),
        running.and_then(|value| value.model.as_deref()),
        done.and_then(|value| value.model.as_deref()),
    ])
    .map(|value| trim_to_limit(value, MAX_LABEL_BYTES))
}

pub(crate) fn provider_for_record(
    meta: Option<&AgentMetaWire>,
    done: Option<&DoneMarkerWire>,
    running: Option<&RunningMarkerWire>,
) -> Option<String> {
    first_non_empty([
        meta.and_then(|value| value.llm_provider.as_deref()),
        running.and_then(|value| value.llm_provider.as_deref()),
        done.and_then(|value| value.llm_provider.as_deref()),
    ])
    .map(|value| trim_to_limit(value, MAX_LABEL_BYTES))
}

pub(crate) fn queue_weight_for_record(
    record: &AgentArtifactRecordWire,
) -> (Option<f64>, bool, bool, Option<String>) {
    if let Some(waiting) = &record.waiting {
        if waiting.queue_weight_invalid {
            return (
                None,
                waiting.queue_weight_explicit,
                true,
                waiting.queue_weight_error.clone(),
            );
        }
        if let Some(weight) = waiting.queue_weight {
            if fleet_queue_weight_is_valid(
                weight,
                waiting.queue_weight_explicit,
            ) {
                return (
                    Some(weight),
                    waiting.queue_weight_explicit,
                    false,
                    None,
                );
            }
            return (None, waiting.queue_weight_explicit, true, None);
        }
    }
    if let Some(meta) = &record.agent_meta {
        if meta.queue_weight_invalid {
            return (
                None,
                meta.queue_weight_explicit,
                true,
                meta.queue_weight_error.clone(),
            );
        }
        if let Some(weight) = meta.queue_weight {
            if fleet_queue_weight_is_valid(weight, meta.queue_weight_explicit) {
                return (Some(weight), meta.queue_weight_explicit, false, None);
            }
            return (None, meta.queue_weight_explicit, true, None);
        }
    }
    (None, false, false, None)
}

/// Fleet summary weight validity, mirroring `runner_capacity`'s and the
/// artifact scanner's `record_weight_is_valid`/`marker_queue_weight_is_valid`:
/// an explicit `0.0` is a valid non-occupying weight (e.g. the epic-launch
/// monitor); every other value, and every implicit weight, still follows the
/// strictly-positive `%queue`/`%q` weight contract in `queue_weight_is_valid`.
pub(crate) fn fleet_queue_weight_is_valid(weight: f64, explicit: bool) -> bool {
    if explicit {
        authored_queue_weight_is_valid(weight)
    } else {
        queue_weight_is_valid(weight)
    }
}

pub(crate) fn queue_capacity_for_record(
    record: &AgentArtifactRecordWire,
) -> (Option<u32>, bool) {
    if let Some(waiting) = &record.waiting {
        if waiting.queue_capacity.is_some() || waiting.wait_runners.is_some() {
            let (capacity, explicit) = resolve_queue_capacity(
                waiting.queue_capacity,
                waiting.wait_runners,
                waiting.queue_capacity_explicit,
                waiting.wait_runners_explicit,
            );
            let capacity = queue_capacity_as_u32(capacity);
            return (capacity, explicit && capacity.is_some());
        }
    }
    if let Some(meta) = &record.agent_meta {
        if meta.queue_capacity.is_some() || meta.wait_runners.is_some() {
            let (capacity, explicit) = resolve_queue_capacity(
                meta.queue_capacity,
                meta.wait_runners,
                meta.queue_capacity_explicit,
                meta.wait_runners_explicit,
            );
            let capacity = queue_capacity_as_u32(capacity);
            return (capacity, explicit && capacity.is_some());
        }
    }
    (None, false)
}

/// Resolve the persisted multiplier with the same waiting-over-metadata and
/// integer-over-multiplier precedence as queue capacity itself.
pub(crate) fn queue_capacity_multiplier_for_record(
    record: &AgentArtifactRecordWire,
) -> Option<f64> {
    if let Some(waiting) = &record.waiting {
        if waiting.queue_capacity.is_some() || waiting.wait_runners.is_some() {
            return None;
        }
        if waiting.queue_capacity_multiplier.is_some() {
            return waiting.queue_capacity_multiplier;
        }
    }
    if let Some(meta) = &record.agent_meta {
        if meta.queue_capacity.is_some() || meta.wait_runners.is_some() {
            return None;
        }
        if meta.queue_capacity_multiplier.is_some() {
            return meta.queue_capacity_multiplier;
        }
    }
    None
}

pub(crate) fn intent_for_record(
    record: &AgentArtifactRecordWire,
) -> Option<String> {
    let raw = first_non_empty([
        record
            .agent_meta
            .as_ref()
            .and_then(|value| value.plan_action.as_deref()),
        record.raw_prompt_snippet.as_deref(),
    ])?;
    // Owner-produced prompts and plan actions are ordinary free text and
    // routinely span multiple lines; strip control characters (newlines,
    // CR, tabs, ...) before byte-bounding so a normal multiline prompt
    // cannot make the display intent fail `validate_label`'s control
    // character rejection. A value that normalizes to nothing (e.g. only
    // control characters) becomes an omitted label instead of an invalid
    // empty one.
    let bounded =
        trim_to_limit(&replace_control_characters(raw), MAX_INTENT_BYTES);
    if bounded.is_empty() {
        None
    } else {
        Some(bounded)
    }
}

/// Replace control characters (including newline/CR/tab) with a plain space
/// so owner-produced free text is safe to display as a single-line label.
pub(crate) fn replace_control_characters(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_control() {
                ' '
            } else {
                character
            }
        })
        .collect()
}

pub(crate) fn first_non_empty<'a>(
    values: impl IntoIterator<Item = Option<&'a str>>,
) -> Option<&'a str> {
    values
        .into_iter()
        .flatten()
        .map(str::trim)
        .find(|value| !value.is_empty())
}

pub(crate) fn content_metadata(
    handles: &[ContentHandleWire],
) -> Result<ContentMetadataWire, FleetContractError> {
    let mut total: Option<u64> = Some(0);
    let mut kinds = BTreeSet::new();
    let mut supports_range = false;
    let mut supports_growth = false;
    for handle in handles {
        handle.validate()?;
        kinds.insert(handle.kind);
        supports_range |= handle.supports_range;
        supports_growth |= handle.supports_growth;
        match (total, handle.byte_len) {
            (Some(left), Some(right)) => total = left.checked_add(right),
            _ => total = None,
        }
    }
    Ok(ContentMetadataWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        handle_count: handles.len() as u64,
        total_byte_len: total,
        kinds: kinds.into_iter().collect(),
        supports_range,
        supports_growth,
    })
}
