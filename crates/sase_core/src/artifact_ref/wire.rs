//! Stable serde records for kind-tagged artifact references.

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const ARTIFACT_REF_PARSE_WIRE_SCHEMA_VERSION: u64 = 5;
pub const ARTIFACT_REF_RESOLUTION_WIRE_SCHEMA_VERSION: u64 = 5;
pub const ARTIFACT_REF_LIST_RESOLUTION_WIRE_SCHEMA_VERSION: u64 = 2;
pub const ARTIFACT_REF_CONTEXT_WIRE_SCHEMA_VERSION: u64 = 1;
pub const ARTIFACT_REF_PATH_FILTER_WIRE_SCHEMA_VERSION: u64 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("{kind}: {message}")]
pub struct ArtifactRefError {
    pub kind: String,
    pub message: String,
}

impl ArtifactRefError {
    pub fn validation(message: impl Into<String>) -> Self {
        Self {
            kind: "validation".to_string(),
            message: message.into(),
        }
    }

    pub fn io(message: impl Into<String>) -> Self {
        Self {
            kind: "io".to_string(),
            message: message.into(),
        }
    }
}

impl From<std::io::Error> for ArtifactRefError {
    fn from(error: std::io::Error) -> Self {
        Self::io(error.to_string())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ArtifactRefKindWire {
    Commit,
    Chat,
    Bug,
    File,
    Bead,
    Agent,
    Stitch,
    Patch,
    Document { role: String },
}

impl ArtifactRefKindWire {
    pub fn label(&self) -> &str {
        match self {
            Self::Commit => "commit",
            Self::Chat => "chat",
            Self::Bug => "bug",
            Self::File => "file",
            Self::Bead => "bead",
            Self::Agent => "agent",
            Self::Stitch => "stitch",
            Self::Patch => "patch",
            Self::Document { role } => role,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactFileSourceWire {
    Explicit,
    Default,
}

impl ArtifactFileSourceWire {
    pub fn label(self) -> &'static str {
        match self {
            Self::Explicit => "explicit",
            Self::Default => "default",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ArtifactRefPayloadWire {
    Commit {
        repo: String,
        sha: String,
    },
    Chat {
        path: String,
    },
    Bug {
        project: String,
        number: u64,
    },
    File {
        source: ArtifactFileSourceWire,
        digest: String,
    },
    FilePath {
        path: String,
    },
    Bead {
        id: String,
    },
    Agent {
        name: String,
    },
    Stitch {
        repo: Option<String>,
        sha: String,
    },
    Patch {
        name: String,
    },
    Document {
        path: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ArtifactRefFragmentWire {
    Lines { start: u64, end: u64 },
    Page { page: u64 },
    Time { seconds: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParsedArtifactRefWire {
    #[serde(default = "artifact_ref_parse_schema_version")]
    pub schema_version: u64,
    pub kind: ArtifactRefKindWire,
    pub payload: ArtifactRefPayloadWire,
    #[serde(default)]
    pub fragment: Option<ArtifactRefFragmentWire>,
    #[serde(default)]
    pub rendered: String,
}

fn artifact_ref_parse_schema_version() -> u64 {
    ARTIFACT_REF_PARSE_WIRE_SCHEMA_VERSION
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactRefDocumentRootWire {
    #[serde(alias = "role")]
    pub kind: String,
    #[serde(alias = "path")]
    pub root: String,
    /// Optional positive/negative POSIX globs applied to this document root.
    ///
    /// `None` means the caller omitted a policy and all safe repo-relative
    /// payloads are allowed. `Some(vec![])` is an explicit empty policy and
    /// allows nothing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path_globs: Option<Vec<String>>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactRefRepositoryWire {
    #[serde(default)]
    pub kind: String,
    pub name: String,
    #[serde(default)]
    pub aliases: Vec<String>,
    #[serde(default)]
    pub shas: Vec<String>,
    #[serde(default)]
    pub checkout_paths: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactRefProjectWire {
    pub name: String,
    #[serde(default)]
    pub key: String,
    #[serde(default)]
    pub aliases: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactRefBeadStoreWire {
    pub project: String,
    pub prefix: String,
    pub root: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactRefAgentRootWire {
    pub project: String,
    pub root: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactRefAgentOwnerWire {
    pub username: String,
    pub machine_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactRefContextWire {
    #[serde(default = "old_artifact_ref_context_schema_version")]
    pub schema_version: u64,
    #[serde(default)]
    pub document_roots: Vec<ArtifactRefDocumentRootWire>,
    #[serde(default)]
    pub chats_root: Option<String>,
    #[serde(default)]
    pub artifact_index_path: Option<String>,
    #[serde(default)]
    pub repositories: Vec<ArtifactRefRepositoryWire>,
    #[serde(default)]
    pub projects: Vec<ArtifactRefProjectWire>,
    #[serde(default)]
    pub bead_stores: Vec<ArtifactRefBeadStoreWire>,
    #[serde(default)]
    pub agent_roots: Vec<ArtifactRefAgentRootWire>,
    #[serde(default)]
    pub agent_owner: Option<ArtifactRefAgentOwnerWire>,
}

impl Default for ArtifactRefContextWire {
    fn default() -> Self {
        Self {
            schema_version: ARTIFACT_REF_CONTEXT_WIRE_SCHEMA_VERSION,
            document_roots: Vec::new(),
            chats_root: None,
            artifact_index_path: None,
            repositories: Vec::new(),
            projects: Vec::new(),
            bead_stores: Vec::new(),
            agent_roots: Vec::new(),
            agent_owner: None,
        }
    }
}

fn old_artifact_ref_context_schema_version() -> u64 {
    0
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactRefResolutionWire {
    pub schema_version: u64,
    pub status: String,
    pub rendered: String,
    pub locator: Option<String>,
    pub resolved_path: Option<String>,
    pub candidates: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnostic: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactRefPathFilterBatchWire {
    pub schema_version: u64,
    pub kind: String,
    pub allowed: Vec<String>,
    pub filtered: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactRefListEntryWire {
    pub rendered: String,
    pub resolution: ArtifactRefResolutionWire,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactRefListResolutionWire {
    pub schema_version: u64,
    pub entries: Vec<ArtifactRefListEntryWire>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ArtifactRefSpanWire {
    pub start: usize,
    pub end: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactRefPromptCandidateWire {
    pub schema_version: u64,
    pub text: String,
    pub reference: String,
    pub kind: String,
    pub well_formed: bool,
    pub candidate_span: ArtifactRefSpanWire,
    pub sigil_span: ArtifactRefSpanWire,
    pub kind_span: ArtifactRefSpanWire,
    pub separator_span: ArtifactRefSpanWire,
    pub payload_span: ArtifactRefSpanWire,
    pub fragment_span: Option<ArtifactRefSpanWire>,
    pub quoted: bool,
}
