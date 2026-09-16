//! Wire records for the Ctrl+K prompt-history project filter.

use serde::{Deserialize, Serialize};

pub const PROMPT_HISTORY_FILTER_WIRE_SCHEMA_VERSION: u32 = 1;

/// One catalog entry describing a known project's identity, used to resolve
/// a raw ref/alias/display-name/Patch-derived reference to a canonical
/// project key. The catalog is expected to include disabled projects and
/// `home`; a raw ref shared by more than one distinct project key is
/// ambiguous rather than silently picking one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct PromptHistoryProjectIdentityWire {
    /// Canonical project key (directory / `.sase` file stem).
    pub key: String,
    /// Configured user-facing display name, when set.
    #[serde(default)]
    pub label: Option<String>,
    /// Registered aliases for this project.
    #[serde(default)]
    pub aliases: Vec<String>,
    /// Additional raw refs known to name this project: owner/repo and
    /// `gh_owner__repo` provider spellings, and Patch names owned by this
    /// project. Never solely derived by splitting a displayed basename.
    #[serde(default)]
    pub raw_refs: Vec<String>,
}

/// One compiled `project:<value>` + literal-text prompt-history query.
///
/// `raw_project_value` is `None` when the query carries no `project:`
/// qualifier at all (the existing unscoped substring behavior). It is
/// `Some` with `project_key: None` when a qualifier was typed but its value
/// did not resolve to exactly one known project -- an ordinary "unknown
/// value" (empty results, unless it matches a raw historical ref exactly)
/// when `diagnostic` is `None`, or an ambiguous label (empty results, with
/// a diagnostic hint) when `diagnostic` is `Some`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompiledPromptHistoryQueryWire {
    pub schema_version: u32,
    #[serde(default)]
    pub raw_project_value: Option<String>,
    #[serde(default)]
    pub project_key: Option<String>,
    #[serde(default)]
    pub project_label: Option<String>,
    /// Remaining literal (already-unescaped) substring to match row text.
    pub text: String,
    /// `false` for a malformed qualifier (empty value / unterminated
    /// quote); a malformed query must select nothing until it is fixed.
    pub valid: bool,
    /// User-facing hint for an ambiguous project value or a malformed
    /// qualifier. `None` when nothing needs to be shown.
    #[serde(default)]
    pub diagnostic: Option<String>,
}

/// One loaded history row's pre-extracted facts, ready for batch matching.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct PromptHistoryRowFactsWire {
    pub index: u32,
    pub canonical_text: String,
    pub display_text: String,
    /// One entry per multi-prompt segment: the segment's resolved canonical
    /// project key, when its active VCS ref matched a known project.
    #[serde(default)]
    pub segment_project_keys: Vec<Option<String>>,
    /// One entry per multi-prompt segment (same length/order as
    /// `segment_project_keys`): the segment's raw, unresolved VCS ref text,
    /// so a deleted or unregistered historical project ref stays
    /// searchable by its exact spelling.
    #[serde(default)]
    pub segment_raw_refs: Vec<Option<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct PromptHistoryMatchResultWire {
    pub schema_version: u32,
    pub matched_indices: Vec<u32>,
}

/// Facts about the leading workspace reference recognized in a prompt
/// draft, used to build the initial Ctrl+K history query ("seed").
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct PromptHistorySeedRequestWire {
    /// The raw ref/name extracted from the recognized VCS workflow tag
    /// (e.g. `"sase"`, `"sase-org/sase"`, `"gh_sase-org__sase"`), if any.
    #[serde(default)]
    pub raw_ref: Option<String>,
    /// The draft text with the recognized VCS span (and its redundant
    /// boundary whitespace) already removed.
    pub remainder_text: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromptHistorySeedWire {
    pub schema_version: u32,
    /// The text to pre-fill into the history filter input.
    pub seed_text: String,
    /// Non-error hint shown when the recognized ref could not be resolved
    /// to a known project scope.
    #[serde(default)]
    pub hint: Option<String>,
}
