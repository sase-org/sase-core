//! Serde wire records for the unified artifact graph.
//!
//! Field order is part of the wire contract for snapshot tests and Python
//! parity. Optional values serialize as JSON `null`; list fields serialize as
//! `[]`.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

pub const ARTIFACT_WIRE_SCHEMA_VERSION: u32 = 1;

pub const ARTIFACT_ROOT_ID: &str = "/";

pub type ArtifactKindWire = String;
pub type ArtifactLinkTypeWire = String;

pub const ARTIFACT_KIND_ROOT: &str = "root";
pub const ARTIFACT_KIND_FILE: &str = "file";
pub const ARTIFACT_KIND_DIRECTORY: &str = "directory";
pub const ARTIFACT_KIND_PROJECT: &str = "project";
pub const ARTIFACT_KIND_CHANGESPEC: &str = "changespec";
pub const ARTIFACT_KIND_COMMIT: &str = "commit";
pub const ARTIFACT_KIND_BEAD: &str = "bead";
pub const ARTIFACT_KIND_AGENT: &str = "agent";
pub const ARTIFACT_KIND_THOUGHT: &str = "thought";
pub const ARTIFACT_KIND_UNKNOWN: &str = "unknown";

pub const ARTIFACT_FILE_TYPE_METADATA_KEY: &str = "artifact_type";
pub const ARTIFACT_FILE_TYPE_PLAN: &str = "plan";
pub const ARTIFACT_FILE_TYPE_DIFF: &str = "diff";
pub const ARTIFACT_FILE_TYPE_CHAT: &str = "chat";
pub const ARTIFACT_FILE_TYPE_PROJECT: &str = "project";
pub const ARTIFACT_FILE_TYPE_PROMPT: &str = "prompt";
pub const ARTIFACT_FILE_TYPE_MISC: &str = "misc";

pub const ARTIFACT_LINK_PARENT: &str = "parent";
pub const ARTIFACT_LINK_CREATED: &str = "created";
pub const ARTIFACT_LINK_WORKER: &str = "worker";
pub const ARTIFACT_LINK_RELATED: &str = "related";

pub const ARTIFACT_PROVENANCE_MANUAL: &str = "manual";
pub const ARTIFACT_PROVENANCE_DERIVED: &str = "derived";

pub const ARTIFACT_TOMBSTONE_NODE: &str = "node";
pub const ARTIFACT_TOMBSTONE_LINK: &str = "link";

pub const ARTIFACT_STALE_CLEANUP_NONE: &str = "none";
pub const ARTIFACT_STALE_CLEANUP_MARK: &str = "mark";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ArtifactNodeWire {
    pub id: String,
    pub kind: ArtifactKindWire,
    pub display_title: String,
    #[serde(default)]
    pub subtitle: Option<String>,
    pub provenance: String,
    #[serde(default)]
    pub source_kind: Option<String>,
    #[serde(default)]
    pub source_id: Option<String>,
    #[serde(default)]
    pub source_version: Option<String>,
    #[serde(default)]
    pub search_text: String,
    #[serde(default)]
    pub metadata: Map<String, Value>,
    #[serde(default)]
    pub created_at: Option<String>,
    #[serde(default)]
    pub updated_at: Option<String>,
}

impl ArtifactNodeWire {
    pub fn root() -> Self {
        Self {
            id: ARTIFACT_ROOT_ID.to_string(),
            kind: ARTIFACT_KIND_ROOT.to_string(),
            display_title: ARTIFACT_ROOT_ID.to_string(),
            subtitle: Some("Artifact root".to_string()),
            provenance: ARTIFACT_PROVENANCE_MANUAL.to_string(),
            source_kind: None,
            source_id: None,
            source_version: None,
            search_text: "root /".to_string(),
            metadata: Map::new(),
            created_at: None,
            updated_at: None,
        }
    }
}

pub fn validate_file_artifact_type(file_type: &str) -> Result<(), String> {
    if canonical_file_artifact_types().contains(&file_type) {
        Ok(())
    } else {
        Err(format!(
            "unsupported file artifact type {file_type:?}; expected one of {}",
            canonical_file_artifact_types().join(", ")
        ))
    }
}

pub fn file_artifact_type(node: &ArtifactNodeWire) -> &str {
    node.metadata
        .get(ARTIFACT_FILE_TYPE_METADATA_KEY)
        .and_then(Value::as_str)
        .filter(|file_type| validate_file_artifact_type(file_type).is_ok())
        .unwrap_or(ARTIFACT_FILE_TYPE_MISC)
}

pub fn set_file_artifact_type(
    metadata: &mut Map<String, Value>,
    file_type: &str,
) -> Result<(), String> {
    validate_file_artifact_type(file_type)?;
    metadata.insert(
        ARTIFACT_FILE_TYPE_METADATA_KEY.to_string(),
        Value::String(file_type.to_string()),
    );
    Ok(())
}

pub fn canonical_file_artifact_types() -> [&'static str; 6] {
    [
        ARTIFACT_FILE_TYPE_PLAN,
        ARTIFACT_FILE_TYPE_DIFF,
        ARTIFACT_FILE_TYPE_CHAT,
        ARTIFACT_FILE_TYPE_PROJECT,
        ARTIFACT_FILE_TYPE_PROMPT,
        ARTIFACT_FILE_TYPE_MISC,
    ]
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ArtifactLinkWire {
    pub id: String,
    pub link_type: ArtifactLinkTypeWire,
    pub source_id: String,
    pub target_id: String,
    pub provenance: String,
    #[serde(default)]
    pub source_kind: Option<String>,
    #[serde(default)]
    pub source_id_hint: Option<String>,
    #[serde(default)]
    pub source_version: Option<String>,
    #[serde(default)]
    pub metadata: Map<String, Value>,
    #[serde(default)]
    pub created_at: Option<String>,
    #[serde(default)]
    pub updated_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ArtifactPayloadWire {
    pub artifact_id: String,
    pub payload_type: String,
    pub provenance: String,
    #[serde(default)]
    pub source_kind: Option<String>,
    #[serde(default)]
    pub source_id: Option<String>,
    #[serde(default)]
    pub source_version: Option<String>,
    #[serde(default)]
    pub payload: Value,
    #[serde(default)]
    pub updated_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactRebuildRequestWire {
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
    #[serde(default)]
    pub projects_root: Option<String>,
    #[serde(default)]
    pub workspace_root: Option<String>,
    #[serde(default)]
    pub beads_dir: Option<String>,
    #[serde(default)]
    pub include_sources: Vec<String>,
    #[serde(default)]
    pub exclude_sources: Vec<String>,
    #[serde(default)]
    pub target_path: Option<String>,
    #[serde(default)]
    pub artifact_dir: Option<String>,
    #[serde(default = "default_stale_cleanup")]
    pub stale_cleanup: String,
}

impl Default for ArtifactRebuildRequestWire {
    fn default() -> Self {
        Self {
            schema_version: ARTIFACT_WIRE_SCHEMA_VERSION,
            projects_root: None,
            workspace_root: None,
            beads_dir: None,
            include_sources: Vec::new(),
            exclude_sources: Vec::new(),
            target_path: None,
            artifact_dir: None,
            stale_cleanup: ARTIFACT_STALE_CLEANUP_NONE.to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ArtifactPathUpsertRequestWire {
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub display_title: Option<String>,
    #[serde(default)]
    pub subtitle: Option<String>,
    #[serde(default)]
    pub provenance: Option<String>,
    #[serde(default)]
    pub source_kind: Option<String>,
    #[serde(default)]
    pub source_id: Option<String>,
    #[serde(default)]
    pub source_version: Option<String>,
    #[serde(default)]
    pub search_text: Option<String>,
    #[serde(default)]
    pub metadata: Option<Map<String, Value>>,
}

impl Default for ArtifactPathUpsertRequestWire {
    fn default() -> Self {
        Self {
            schema_version: ARTIFACT_WIRE_SCHEMA_VERSION,
            kind: None,
            display_title: None,
            subtitle: None,
            provenance: None,
            source_kind: None,
            source_id: None,
            source_version: None,
            search_text: None,
            metadata: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ArtifactDetailWire {
    pub schema_version: u32,
    #[serde(default)]
    pub node: Option<ArtifactNodeWire>,
    #[serde(default)]
    pub payloads: Vec<ArtifactPayloadWire>,
    #[serde(default)]
    pub outbound_links: Vec<ArtifactLinkWire>,
    #[serde(default)]
    pub inbound_links: Vec<ArtifactLinkWire>,
    #[serde(default)]
    pub children: Vec<ArtifactNodeWire>,
    #[serde(default)]
    pub path_to_root: Vec<ArtifactNodeWire>,
    #[serde(default)]
    pub diagnostics: Vec<ArtifactDoctorIssueWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactPageRequestWire {
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
    #[serde(default)]
    pub group_key: Option<String>,
    #[serde(default)]
    pub relation: Option<String>,
    #[serde(default)]
    pub link_type: Option<ArtifactLinkTypeWire>,
    #[serde(default)]
    pub offset: u32,
    #[serde(default = "default_page_limit")]
    pub limit: u32,
}

impl Default for ArtifactPageRequestWire {
    fn default() -> Self {
        Self {
            schema_version: ARTIFACT_WIRE_SCHEMA_VERSION,
            group_key: None,
            relation: None,
            link_type: None,
            offset: 0,
            limit: default_page_limit(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactGroupSummaryWire {
    pub group_key: String,
    pub direction: String,
    #[serde(default)]
    pub link_type: Option<ArtifactLinkTypeWire>,
    pub total_count: u64,
    pub loaded_count: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ArtifactRelationPageWire {
    pub summary: ArtifactGroupSummaryWire,
    #[serde(default)]
    pub nodes: Vec<ArtifactNodeWire>,
    #[serde(default)]
    pub links: Vec<ArtifactLinkWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactTypeCountWire {
    pub artifact_type: String,
    pub total_count: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ArtifactDetailPagedWire {
    pub schema_version: u32,
    #[serde(default)]
    pub node: Option<ArtifactNodeWire>,
    #[serde(default)]
    pub payloads: Vec<ArtifactPayloadWire>,
    #[serde(default)]
    pub path_to_root: Vec<ArtifactNodeWire>,
    #[serde(default)]
    pub diagnostics: Vec<ArtifactDoctorIssueWire>,
    pub children_page: ArtifactRelationPageWire,
    #[serde(default)]
    pub outbound_pages: Vec<ArtifactRelationPageWire>,
    #[serde(default)]
    pub inbound_pages: Vec<ArtifactRelationPageWire>,
    #[serde(default)]
    pub type_counts: Vec<ArtifactTypeCountWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactQueryWire {
    pub schema_version: u32,
    #[serde(default)]
    pub text: Option<String>,
    #[serde(default)]
    pub kinds: Vec<ArtifactKindWire>,
    #[serde(default)]
    pub file_types: Vec<String>,
    #[serde(default)]
    pub link_types: Vec<ArtifactLinkTypeWire>,
    #[serde(default)]
    pub provenance: Option<String>,
    #[serde(default)]
    pub source_kinds: Vec<String>,
    #[serde(default)]
    pub source_ids: Vec<String>,
    #[serde(default)]
    pub root_id: Option<String>,
    #[serde(default)]
    pub include_tombstoned: bool,
    #[serde(default)]
    pub limit: Option<u32>,
    #[serde(default)]
    pub offset: Option<u32>,
}

impl Default for ArtifactQueryWire {
    fn default() -> Self {
        Self {
            schema_version: ARTIFACT_WIRE_SCHEMA_VERSION,
            text: None,
            kinds: Vec::new(),
            file_types: Vec::new(),
            link_types: Vec::new(),
            provenance: None,
            source_kinds: Vec::new(),
            source_ids: Vec::new(),
            root_id: None,
            include_tombstoned: false,
            limit: Some(200),
            offset: Some(0),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ArtifactGraphWire {
    pub schema_version: u32,
    #[serde(default)]
    pub root_id: Option<String>,
    #[serde(default)]
    pub nodes: Vec<ArtifactNodeWire>,
    #[serde(default)]
    pub links: Vec<ArtifactLinkWire>,
    pub node_count: u64,
    pub link_count: u64,
    pub truncated: bool,
    #[serde(default)]
    pub limit: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactGraphOptionsWire {
    pub schema_version: u32,
    #[serde(default)]
    pub root_id: Option<String>,
    #[serde(default)]
    pub max_depth: Option<u32>,
    #[serde(default)]
    pub link_types: Vec<ArtifactLinkTypeWire>,
    #[serde(default)]
    pub include_inbound: bool,
    #[serde(default = "default_true")]
    pub include_outbound: bool,
    #[serde(default)]
    pub full_graph: bool,
    #[serde(default)]
    pub limit: Option<u32>,
}

impl Default for ArtifactGraphOptionsWire {
    fn default() -> Self {
        Self {
            schema_version: ARTIFACT_WIRE_SCHEMA_VERSION,
            root_id: Some(ARTIFACT_ROOT_ID.to_string()),
            max_depth: Some(2),
            link_types: Vec::new(),
            include_inbound: false,
            include_outbound: true,
            full_graph: false,
            limit: Some(500),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ArtifactNodeUpsertWire {
    pub schema_version: u32,
    pub node: ArtifactNodeWire,
    #[serde(default)]
    pub replace_payloads: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactNodeRemoveWire {
    pub schema_version: u32,
    pub id: String,
    #[serde(default)]
    pub provenance: Option<String>,
    #[serde(default)]
    pub source_kind: Option<String>,
    #[serde(default)]
    pub source_id: Option<String>,
    #[serde(default)]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ArtifactLinkUpsertWire {
    pub schema_version: u32,
    pub link: ArtifactLinkWire,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactLinkRemoveWire {
    pub schema_version: u32,
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub link_type: Option<ArtifactLinkTypeWire>,
    #[serde(default)]
    pub source_id: Option<String>,
    #[serde(default)]
    pub target_id: Option<String>,
    #[serde(default)]
    pub provenance: Option<String>,
    #[serde(default)]
    pub source_kind: Option<String>,
    #[serde(default)]
    pub source_id_hint: Option<String>,
    #[serde(default)]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactMutationResultWire {
    pub schema_version: u32,
    pub operation: String,
    pub nodes_added: u64,
    pub nodes_updated: u64,
    pub nodes_removed: u64,
    pub links_added: u64,
    pub links_updated: u64,
    pub links_removed: u64,
    pub tombstones_added: u64,
    #[serde(default)]
    pub affected_node_ids: Vec<String>,
    #[serde(default)]
    pub affected_link_ids: Vec<String>,
    #[serde(default)]
    pub tombstone_ids: Vec<String>,
    #[serde(default)]
    pub errors: Vec<String>,
}

impl Default for ArtifactMutationResultWire {
    fn default() -> Self {
        Self {
            schema_version: ARTIFACT_WIRE_SCHEMA_VERSION,
            operation: String::new(),
            nodes_added: 0,
            nodes_updated: 0,
            nodes_removed: 0,
            links_added: 0,
            links_updated: 0,
            links_removed: 0,
            tombstones_added: 0,
            affected_node_ids: Vec::new(),
            affected_link_ids: Vec::new(),
            tombstone_ids: Vec::new(),
            errors: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactDoctorOptionsWire {
    pub schema_version: u32,
    #[serde(default = "default_true")]
    pub check_dangling_links: bool,
    #[serde(default = "default_true")]
    pub check_root_presence: bool,
    #[serde(default = "default_true")]
    pub check_reachability: bool,
    #[serde(default = "default_true")]
    pub check_duplicate_parents: bool,
    #[serde(default = "default_true")]
    pub check_tombstones: bool,
}

impl Default for ArtifactDoctorOptionsWire {
    fn default() -> Self {
        Self {
            schema_version: ARTIFACT_WIRE_SCHEMA_VERSION,
            check_dangling_links: true,
            check_root_presence: true,
            check_reachability: true,
            check_duplicate_parents: true,
            check_tombstones: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactDoctorIssueWire {
    pub issue_type: String,
    pub severity: String,
    #[serde(default)]
    pub artifact_id: Option<String>,
    #[serde(default)]
    pub link_id: Option<String>,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactDoctorWire {
    pub schema_version: u32,
    pub ok: bool,
    #[serde(default)]
    pub issues: Vec<ArtifactDoctorIssueWire>,
}

fn default_true() -> bool {
    true
}

fn default_schema_version() -> u32 {
    ARTIFACT_WIRE_SCHEMA_VERSION
}

fn default_page_limit() -> u32 {
    10
}

fn default_stale_cleanup() -> String {
    ARTIFACT_STALE_CLEANUP_NONE.to_string()
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn representative_node_json_shape_is_rectangular() {
        let node = ArtifactNodeWire {
            id: "/tmp/example.md".to_string(),
            kind: ARTIFACT_KIND_FILE.to_string(),
            display_title: "example.md".to_string(),
            subtitle: None,
            provenance: ARTIFACT_PROVENANCE_DERIVED.to_string(),
            source_kind: Some("agent".to_string()),
            source_id: Some("writer".to_string()),
            source_version: None,
            search_text: "example markdown".to_string(),
            metadata: Map::new(),
            created_at: None,
            updated_at: Some("2026-05-05T12:00:00Z".to_string()),
        };

        assert_eq!(
            serde_json::to_value(&node).unwrap(),
            json!({
                "id": "/tmp/example.md",
                "kind": "file",
                "display_title": "example.md",
                "subtitle": null,
                "provenance": "derived",
                "source_kind": "agent",
                "source_id": "writer",
                "source_version": null,
                "search_text": "example markdown",
                "metadata": {},
                "created_at": null,
                "updated_at": "2026-05-05T12:00:00Z"
            })
        );
    }

    #[test]
    fn detail_and_query_records_keep_nulls_and_empty_lists() {
        let detail = ArtifactDetailWire {
            schema_version: ARTIFACT_WIRE_SCHEMA_VERSION,
            node: None,
            payloads: Vec::new(),
            outbound_links: Vec::new(),
            inbound_links: Vec::new(),
            children: Vec::new(),
            path_to_root: Vec::new(),
            diagnostics: Vec::new(),
        };
        assert_eq!(
            serde_json::to_value(&detail).unwrap(),
            json!({
                "schema_version": 1,
                "node": null,
                "payloads": [],
                "outbound_links": [],
                "inbound_links": [],
                "children": [],
                "path_to_root": [],
                "diagnostics": []
            })
        );

        assert_eq!(
            serde_json::to_value(ArtifactQueryWire::default()).unwrap(),
            json!({
                "schema_version": 1,
                "text": null,
                "kinds": [],
                "file_types": [],
                "link_types": [],
                "provenance": null,
                "source_kinds": [],
                "source_ids": [],
                "root_id": null,
                "include_tombstoned": false,
                "limit": 200,
                "offset": 0
            })
        );

        assert_eq!(
            serde_json::to_value(ArtifactPageRequestWire::default()).unwrap(),
            json!({
                "schema_version": 1,
                "group_key": null,
                "relation": null,
                "link_type": null,
                "offset": 0,
                "limit": 10
            })
        );
    }

    #[test]
    fn graph_and_mutation_records_have_stable_top_level_shapes() {
        let graph = ArtifactGraphWire {
            schema_version: ARTIFACT_WIRE_SCHEMA_VERSION,
            root_id: Some(ARTIFACT_ROOT_ID.to_string()),
            nodes: vec![ArtifactNodeWire::root()],
            links: Vec::new(),
            node_count: 1,
            link_count: 0,
            truncated: false,
            limit: Some(100),
        };
        let value = serde_json::to_value(&graph).unwrap();
        assert_eq!(value["schema_version"], json!(1));
        assert_eq!(value["root_id"], json!("/"));
        assert_eq!(value["nodes"][0]["kind"], json!("root"));
        assert_eq!(value["links"], json!([]));
        assert_eq!(value["limit"], json!(100));

        let result = ArtifactMutationResultWire {
            schema_version: ARTIFACT_WIRE_SCHEMA_VERSION,
            operation: "upsert_node".to_string(),
            nodes_added: 1,
            affected_node_ids: vec!["/tmp/example.md".to_string()],
            ..ArtifactMutationResultWire::default()
        };
        assert_eq!(
            serde_json::to_value(&result).unwrap(),
            json!({
                "schema_version": 1,
                "operation": "upsert_node",
                "nodes_added": 1,
                "nodes_updated": 0,
                "nodes_removed": 0,
                "links_added": 0,
                "links_updated": 0,
                "links_removed": 0,
                "tombstones_added": 0,
                "affected_node_ids": ["/tmp/example.md"],
                "affected_link_ids": [],
                "tombstone_ids": [],
                "errors": []
            })
        );
    }

    #[test]
    fn rebuild_and_path_upsert_request_shapes_are_stable() {
        assert_eq!(
            serde_json::to_value(ArtifactRebuildRequestWire::default())
                .unwrap(),
            json!({
                "schema_version": 1,
                "projects_root": null,
                "workspace_root": null,
                "beads_dir": null,
                "include_sources": [],
                "exclude_sources": [],
                "target_path": null,
                "artifact_dir": null,
                "stale_cleanup": "none"
            })
        );

        assert_eq!(
            serde_json::to_value(ArtifactPathUpsertRequestWire {
                provenance: Some("derived".to_string()),
                source_kind: Some("directory".to_string()),
                source_id: Some("/tmp/example.md".to_string()),
                ..ArtifactPathUpsertRequestWire::default()
            })
            .unwrap(),
            json!({
                "schema_version": 1,
                "kind": null,
                "display_title": null,
                "subtitle": null,
                "provenance": "derived",
                "source_kind": "directory",
                "source_id": "/tmp/example.md",
                "source_version": null,
                "search_text": null,
                "metadata": null
            })
        );
    }
}
