//! Wire types for xprompt project tags.
//!
//! Byte offsets are UTF-8 byte offsets into the scanned text. The Python
//! bindings convert them to Python code-point offsets.

use serde::{Deserialize, Serialize};

/// One project the tag catalog knows about: every non-sibling project record
/// (enabled or disabled) plus the system `home` project.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectTagTargetWire {
    /// Directory key, e.g. `gh_sase-org__sase`.
    pub key: String,
    /// Display name, e.g. `sase`.
    pub name: String,
    /// Alternate names the project can be matched by.
    #[serde(default)]
    pub aliases: Vec<String>,
    /// VCS workflow type (`gh`, `git`, …). `None` when no provider was
    /// detected; such targets resolve but never expand.
    #[serde(default)]
    pub workflow_type: Option<String>,
    /// Lifecycle state (`enabled` | `disabled` | `system`). Older catalogs
    /// omit it and deserialize to `None`.
    #[serde(default)]
    pub state: Option<String>,
    /// Canonical workspace directory, when the catalog knows it. Older
    /// catalogs omit it and deserialize to `None`.
    #[serde(default)]
    pub workspace_dir: Option<String>,
}

/// One lexically valid project tag occurrence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectTagSpanWire {
    /// Byte offset of the `+`. The left boundary (start of text, whitespace,
    /// `{`, or `|`) was already checked.
    pub start: usize,
    /// Byte offset one past the tag name. Covers the `+`.
    pub end: usize,
    /// Byte offset of the first name character (`start + 1`).
    pub name_start: usize,
    /// The tag name without the `+`.
    pub name: String,
    /// True when the tag is the first word on its line (leading whitespace
    /// and `%directive` tokens don't count).
    pub anchored: bool,
}

/// Resolution of one tag name against the tag targets.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ProjectTagResolutionWire {
    /// Exactly one target matched (exact, then casefold).
    Resolved { target_index: usize },
    /// More than one target matched. The backend never guesses.
    Ambiguous { candidates: Vec<usize> },
    /// No target matched. `suggestions` holds up to 3 known-tag spellings
    /// ranked by edit distance.
    Unknown { suggestions: Vec<String> },
}

/// One tag's expansion report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectTagExpansionEntryWire {
    pub start: usize,
    pub end: usize,
    pub name_start: usize,
    pub name: String,
    pub anchored: bool,
    pub resolution: ProjectTagResolutionWire,
    /// The `#<workflow_type>:<key>` replacement. `Some` only for resolved
    /// tags whose target has a `workflow_type`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replacement: Option<String>,
}

/// Result of [`crate::project_tag::expand_project_tags`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectTagExpansionWire {
    pub text: String,
    pub tags: Vec<ProjectTagExpansionEntryWire>,
}

/// A live `+query` completion trigger.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectTagTriggerWire {
    /// Byte offset of the `+`.
    pub start: usize,
    /// Byte offset one past the typed token.
    pub end: usize,
    /// Text after the `+` up to the cursor, used to filter rows.
    pub query: String,
}

/// Result of [`crate::project_tag::apply_project_tag_selection`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectTagApplyWire {
    pub text: String,
    /// Byte offset where the caret lands (just past the insertion).
    pub cursor: usize,
}
