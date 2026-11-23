//! Compact list entry points over the existing scan and index pipelines.

use std::path::{Path, PathBuf};

use super::project::{project_record_for_list_compact, project_scan_for_list};
use super::wire::{
    AgentArtifactListRecordWire, AgentArtifactListReusableSnapshotWire,
    AgentArtifactListSnapshotWire,
};
use crate::agent_scan::index::{
    query_agent_artifact_index, query_agent_artifact_index_reusable_for_shape,
    AgentArtifactIndexQueryWire,
};
use crate::agent_scan::scanner::{
    scan_agent_artifact_dir, scan_agent_artifact_dirs, scan_agent_artifacts,
};
use crate::agent_scan::wire::{
    AgentArtifactRecordShapeWire, AgentArtifactScanOptionsWire,
};

/// Query the artifact index and return compact list rows.
///
/// Reuses the existing selection, revalidation, windowing, clan-context,
/// and completeness pipeline. `query.record_shape` is ignored; the
/// response is always the compact list contract. A temporary full Rust
/// record may exist during the index read and is not returned.
pub fn query_agent_artifact_index_list(
    index_path: &Path,
    projects_root: &Path,
    mut query: AgentArtifactIndexQueryWire,
    options: AgentArtifactScanOptionsWire,
) -> Result<AgentArtifactListSnapshotWire, String> {
    query.record_shape = AgentArtifactRecordShapeWire::Full;
    let snapshot =
        query_agent_artifact_index(index_path, projects_root, query, options)?;
    Ok(project_scan_for_list(snapshot))
}

/// Query compact list rows while allowing an owner to retain an unchanged
/// record payload from a prior response.
pub fn query_agent_artifact_index_list_reusable(
    index_path: &Path,
    projects_root: &Path,
    mut query: AgentArtifactIndexQueryWire,
    options: AgentArtifactScanOptionsWire,
    known_generation: Option<u64>,
) -> Result<AgentArtifactListReusableSnapshotWire, String> {
    query.record_shape = AgentArtifactRecordShapeWire::Full;
    let reusable = query_agent_artifact_index_reusable_for_shape(
        index_path,
        projects_root,
        query,
        options,
        known_generation,
        true,
    )?;
    Ok(AgentArtifactListReusableSnapshotWire {
        snapshot: project_scan_for_list(reusable.snapshot),
        payload_generation: reusable.payload_generation,
        records_reused: reusable.records_reused,
    })
}

/// Walk `projects_root` and return compact list rows.
pub fn scan_agent_artifacts_list(
    projects_root: &Path,
    options: AgentArtifactScanOptionsWire,
) -> AgentArtifactListSnapshotWire {
    project_scan_for_list(scan_agent_artifacts(projects_root, options))
}

/// Scan exact artifact directories and return compact list rows.
pub fn scan_agent_artifact_dirs_list(
    projects_root: &Path,
    artifact_dirs: &[PathBuf],
    options: AgentArtifactScanOptionsWire,
) -> AgentArtifactListSnapshotWire {
    project_scan_for_list(scan_agent_artifact_dirs(
        projects_root,
        artifact_dirs,
        options,
    ))
}

/// Scan one artifact directory and return a compact list row.
pub fn scan_agent_artifact_dir_list(
    projects_root: &Path,
    artifact_dir: &Path,
    options: &AgentArtifactScanOptionsWire,
) -> Option<AgentArtifactListRecordWire> {
    scan_agent_artifact_dir(projects_root, artifact_dir, options)
        .map(|record| project_record_for_list_compact(&record))
}
