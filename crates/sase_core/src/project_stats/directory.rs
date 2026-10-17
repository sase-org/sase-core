//! Read-only attribution directory seam for future artifact-backed metadata.

use std::collections::BTreeMap;

/// Member metadata distinguishes a missing directory row from an empty model.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct DirectoryMember {
    pub name: String,
    pub family: Option<String>,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub model_metadata_present: bool,
}

/// Name, family, and commit-SHA lookup indexes for attribution.
#[derive(Debug, Clone, Default)]
pub(crate) struct Directory {
    pub by_name: BTreeMap<String, DirectoryMember>,
    pub family_members: BTreeMap<String, Vec<String>>,
    pub by_sha: BTreeMap<String, String>,
}

/// The initial loader intentionally exposes an empty, read-only directory.
pub(crate) fn load_directory() -> Directory {
    Directory::default()
}
