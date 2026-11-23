//! Additive compact agent-list wire: DTOs, projection, and entry points.
//!
//! Full-record scan and index APIs stay unchanged. These types are a
//! distinct list contract so Python can construct Agents without
//! reconstituting nested `AgentMetaWire` / `DoneMarkerWire` trees.

mod project;
mod query;
mod wire;

pub use project::{project_record_for_list_compact, project_scan_for_list};
pub use query::{
    query_agent_artifact_index_list, query_agent_artifact_index_list_reusable,
    scan_agent_artifact_dir_list, scan_agent_artifact_dirs_list,
    scan_agent_artifacts_list,
};
pub use wire::{
    AgentArtifactListDoneWire, AgentArtifactListFinalizerInstanceWire,
    AgentArtifactListFinalizerWire, AgentArtifactListGateWire,
    AgentArtifactListMetaWire, AgentArtifactListMonitorWire,
    AgentArtifactListOutputFactsWire, AgentArtifactListPendingQuestionWire,
    AgentArtifactListRecordWire, AgentArtifactListRecoveryWire,
    AgentArtifactListReusableSnapshotWire, AgentArtifactListRunningWire,
    AgentArtifactListShapeWire, AgentArtifactListSnapshotWire,
    AgentArtifactListStepWire, AgentArtifactListTurnWire,
    AgentArtifactListWaitingWire, AgentArtifactListWorkflowStepWire,
    AgentArtifactListWorkflowWire, AGENT_ARTIFACT_LIST_WIRE_SCHEMA_VERSION,
};

#[cfg(test)]
mod tests;
