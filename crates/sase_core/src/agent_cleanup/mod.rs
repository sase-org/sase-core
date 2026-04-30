pub mod execution;
pub mod planner;
pub mod wire;

pub use execution::{
    bundle_filename_from_json, bundle_shard_for_filename,
    delete_agent_artifact_markers, mark_comment_agents_as_killed,
    mark_hook_agents_as_killed, mark_mentor_agents_as_killed,
    release_workspace_from_content, save_dismissed_agents_index,
    save_dismissed_bundle_json, AgentCleanupArtifactDeleteResultWire,
    AgentCleanupBundleWriteResultWire, AgentCleanupWorkspaceReleaseResultWire,
};
pub use planner::plan_agent_cleanup;
pub use wire::{
    cleanup_plan_from_json_value, cleanup_request_from_json_value,
    AgentCleanupArtifactDeleteIntentWire, AgentCleanupBundleSaveIntentWire,
    AgentCleanupCountsWire, AgentCleanupDismissItemWire,
    AgentCleanupDismissalRenameIntentWire, AgentCleanupIdentityWire,
    AgentCleanupKillItemWire, AgentCleanupNotificationDismissIntentWire,
    AgentCleanupPlanWire, AgentCleanupRequestWire, AgentCleanupSideEffectsWire,
    AgentCleanupSkippedItemWire, AgentCleanupTargetWire,
    AgentCleanupWorkspaceReleaseIntentWire, AGENT_CLEANUP_WIRE_SCHEMA_VERSION,
    CLEANUP_MODE_DISMISS_COMPLETED, CLEANUP_MODE_KILL_AND_DISMISS,
    CLEANUP_MODE_PREVIEW_ONLY, CLEANUP_SCOPE_ALL_PANELS,
    CLEANUP_SCOPE_CUSTOM_SELECTION, CLEANUP_SCOPE_EXPLICIT_IDENTITIES,
    CLEANUP_SCOPE_FOCUSED_GROUP, CLEANUP_SCOPE_FOCUSED_PANEL,
    CLEANUP_SCOPE_TAG, CONFIRMATION_SEVERITY_DESTRUCTIVE,
    CONFIRMATION_SEVERITY_DISMISS, CONFIRMATION_SEVERITY_NONE, KILL_KIND_CRS,
    KILL_KIND_HOOK, KILL_KIND_MENTOR, KILL_KIND_RUNNING, KILL_KIND_WORKFLOW,
    SKIPPED_DUPLICATE, SKIPPED_NOT_DISMISSABLE, SKIPPED_NOT_IN_SCOPE,
    SKIPPED_NOT_KILLABLE, SKIPPED_UNKNOWN_KILL_KIND,
    SKIPPED_WORKFLOW_CHILD_CASCADE_ONLY,
};
