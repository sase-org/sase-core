//! Transport-free fleet identity, projection, count, cursor, operation, and
//! connection contracts.
//!
//! Version 1 deliberately separates stable identity from every operational
//! label that may change. An installation ID is an opaque per-user origin ID;
//! it is not a hostname, configured `id.machine_name`, provider reference, or
//! display alias. Logical agent and agent session locators identify durable work
//! threads, while exact instance locators identify one shell/run/attempt and
//! are required for mutations. Lifecycle, owner-resolved process liveness,
//! connection health, and viewer freshness are distinct states because only
//! the owner can resolve local PIDs and content availability. Feed cursors
//! track store replay positions; resource revisions are separate mutation
//! preconditions. Operation keys provide scoped idempotency inside an
//! acceptance window, not indefinite or exactly-once execution.

mod catalog;
mod connection;
mod content;
mod cursors;
mod error;
mod federation;
mod follows;
mod identity;
mod launch;
mod locators;
mod operations;
mod projection;
mod reads;
mod resolution;
mod snapshot;
mod status;
mod validation;

#[cfg(test)]
mod tests;

pub use catalog::{
    accumulate_fleet_catalog_page, fleet_catalog_snapshot_id,
    select_fleet_catalog_page, select_fleet_logical_batch,
    validate_fleet_catalog_cursor, validate_fleet_catalog_query,
    validate_fleet_catalog_snapshot_id, validate_fleet_logical_batch_request,
    FleetCatalogAccumulationActionWire, FleetCatalogAccumulationDecisionWire,
    FleetCatalogAccumulationRequestWire, FleetCatalogAccumulationStateWire,
    FleetCatalogContinuationStateWire, FleetCatalogContinuationWire,
    FleetCatalogPageSelectionWire, FleetCatalogPageWire, FleetCatalogQueryWire,
    FleetCatalogResetReasonWire, FleetCatalogScopeWire,
    FleetLogicalBatchEntryWire, FleetLogicalBatchRequestWire,
    FleetLogicalBatchResponseWire,
};

pub use connection::{
    classify_cache_freshness, classify_runtime_duration,
    validate_connection_plan, CacheFreshnessRequestWire, CacheFreshnessWire,
    ConnectionPlanWire, FleetConnectionKindWire, RuntimeDurationRequestWire,
    RuntimeDurationStateWire, RuntimeDurationWire, TlsTrustModeWire,
    TlsTrustSettingsWire,
};

pub use content::{
    validate_content_handle, CapabilitySetWire, ContentHandleKindWire,
    ContentHandleWire, ContentMetadataWire, ResourceRevisionWire,
};

pub use cursors::{
    classify_cursor_replay, cursor_replay_reason_to_resync_reason,
    validate_fleet_invalidation_event, validate_fleet_replay_capacity,
    validate_store_cursor, CursorReplayClassificationWire,
    CursorReplayDecisionWire, CursorReplayReasonWire, CursorReplayRequestWire,
    FleetEventStreamItemWire, FleetInvalidationEventWire,
    FleetInvalidationKindWire, FleetResyncReasonWire, FleetResyncRequiredWire,
    StoreCursorWire,
};

pub use error::{
    fleet_contract_schema_version, FleetContractError,
    FLEET_CATALOG_SNAPSHOT_ID_PREFIX, FLEET_CONTRACT_SCHEMA_VERSION,
    FLEET_INITIAL_CURSOR_GENERATION, FLEET_INSTALLATION_IDENTITY_FILENAME,
    FLEET_INSTALLATION_IDENTITY_MAX_BYTES,
    FLEET_INSTALLATION_IDENTITY_SCHEMA_VERSION, FLEET_INSTALLATION_ID_PREFIX,
    FLEET_PROTOCOL_VERSION, FLEET_READ_DEFAULT_CONTENT_BYTES,
    FLEET_READ_DEFAULT_PAGE_ROWS, FLEET_READ_DEFAULT_REPLAY_EVENTS,
    FLEET_READ_MAX_BATCH_IDS, FLEET_READ_MAX_CONTENT_BYTES,
    FLEET_READ_MAX_FILTER_BYTES, FLEET_READ_MAX_PAGE_ROWS,
    FLEET_READ_MAX_PROJECT_IDS, FLEET_READ_MAX_QUERY_BYTES,
    FLEET_READ_MAX_REPLAY_EVENTS,
};

pub use federation::{
    count_focus_and_fleet_from_federation, normalize_fleet_federation_response,
    FleetEnvelopeDiagnosticWire, FleetFederationNormalizeRequestWire,
    FleetNormalizedHostWire, FleetNormalizedReadResponseWire,
    FocusFleetFederationCountsRequestWire,
};

pub use follows::{
    count_focus_and_fleet, follow_record_key, reconcile_follow_records,
    FleetHostCountInputWire, FleetHostCountWire, FleetScopeCountsWire,
    FocusFleetCountsRequestWire, FocusFleetCountsWire, FollowActivationWire,
    FollowAgentSessionPromotionWire, FollowCreatedByWire,
    FollowDiagnosticSeverityWire, FollowDiagnosticWire,
    FollowReconciliationRequestWire, FollowReconciliationWire,
    FollowRecordWire, FollowStateWire, FollowTombstoneWire,
};

pub use identity::{
    ensure_installation_identity, ensure_installation_identity_with_generator,
    installation_identity_path, load_installation_identity,
    migrate_installation_identity, rotate_installation_identity,
    rotate_installation_identity_with_generator,
    InstallationIdentityEnsureOutcomeWire, InstallationIdentityLoadOutcomeWire,
    InstallationIdentityMigrateOutcomeWire,
    InstallationIdentityMigrateRequestWire, InstallationIdentityRecordWire,
    InstallationIdentityRotateOutcomeWire,
    InstallationIdentityRotateRequestWire,
};

pub use launch::{
    decide_fleet_launch_replay, fleet_launch_payload_fingerprint,
    validate_fleet_launch_intent, validate_fleet_launch_request,
    DurableFleetLaunchRecordWire, FleetLaunchDecisionRequestWire,
    FleetLaunchDecisionWire, FleetLaunchIntentWire,
    FleetLaunchProjectContextWire, FleetLaunchReceiptWire,
    FleetLaunchReferenceKindWire, FleetLaunchReferenceWire,
    FleetLaunchRequestWire, FleetLaunchResponseWire,
};

pub use locators::{
    associate_owner_display_name, instance_locator_key, logical_locator_key,
    AgentInstanceLocatorWire, LogicalAgentLocatorWire, OriginLocatorWire,
    OwnerDisplayNameRequestWire, OwnerDisplayNameWire, ProjectLocatorWire,
};

pub use operations::{
    decide_operation_replay, operation_payload_fingerprint,
    DurableOperationRecordWire, OperationDecisionKindWire,
    OperationDecisionReasonWire, OperationDecisionRequestWire,
    OperationDecisionWire, OperationReceiptStateWire, OperationReceiptWire,
    PayloadFingerprintRequestWire, PayloadFingerprintWire,
    ScopedOperationKeyWire,
};

pub use reads::{
    count_logical_agents, fleet_content_read_limit,
    fleet_project_eligibility_limit, validate_fleet_content_read_request,
    validate_fleet_detail_request, validate_fleet_logical_agent_counts,
    validate_fleet_project_eligibility_request, FleetContentReadRequestWire,
    FleetContentReadResponseWire, FleetCountBasisWire, FleetDetailRequestWire,
    FleetDetailResponseWire, FleetLogicalAgentCountsRequestWire,
    FleetLogicalAgentCountsWire, FleetProjectEligibilityRequestWire,
    FleetProjectEligibilityResponseWire, FleetProjectEligibilityWire,
};

pub use resolution::{
    project_resolved_agent_detail, project_resolved_agent_summary,
    validate_resolved_agent_summary, HumanDisplayLabelsWire,
    OwnerResolutionFactsWire, ResolvedAgentDetailWire,
    ResolvedAgentProjectionRequestWire, ResolvedAgentSummaryWire,
};

pub use snapshot::{
    fleet_count_revision, validate_fleet_authoritative_snapshot,
    validate_fleet_snapshot_freshness, FleetAuthoritativeSnapshotWire,
    FleetSnapshotFreshnessWire, FleetSummaryResponseWire,
};

pub(crate) use error::{
    MAX_INTENT_BYTES, MAX_LABEL_BYTES, MAX_LAUNCH_PROMPT_BYTES,
};
pub(crate) use locators::canonical_logical_key;
pub use locators::fallback_turn_shell_ids_equal;
pub(crate) use locators::logical_key_unchecked;
pub use status::{
    ConnectionHealthWire, FleetAgentSessionRoleWire, FleetLifecycleWire,
    FleetRowKindWire, FleetStatusBucketWire, ObservationFreshnessWire,
    OwnerLivenessWire,
};
pub(crate) use validation::{
    duration_ms, reject_path_like, reject_secretish, timestamp_ms,
    trim_to_limit, validate_installation_id, validate_label,
    validate_non_negative_seconds, validate_schema, validate_timestamp,
};
