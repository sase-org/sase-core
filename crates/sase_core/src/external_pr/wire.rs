//! Serde wire records for the external pull-request mirror classifier.

use serde::{Deserialize, Serialize};

pub const EXTERNAL_PR_WIRE_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemotePullRequestWire {
    pub number: u64,
    pub url: String,
    pub provider_id: String,
    pub title: String,
    pub body: String,
    pub state: String,
    pub is_draft: bool,
    pub author: String,
    pub head_ref: String,
    pub base_ref: String,
    pub created_at: String,
    pub updated_at: String,
    pub closed_at: String,
    pub merged_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalPatchWire {
    pub name: String,
    pub pr_url: String,
    pub pr_origin: String,
    pub status: String,
    pub archived: bool,
    pub reserved: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExternalPrImportRequestWire {
    pub schema_version: u32,
    pub remote: RemotePullRequestWire,
    #[serde(default)]
    pub local_patches: Vec<LocalPatchWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExternalPrImportPlanWire {
    pub action: String,
    pub reason: String,
    pub patch_name: Option<String>,
    pub name_base: Option<String>,
    pub pr_origin: String,
    pub status: String,
    pub destination: String,
    pub description: String,
    pub canonical_pr_url: Option<String>,
}
