use serde::{Deserialize, Serialize};

pub const EDITOR_WIRE_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EditorPosition {
    pub line: u32,
    pub character: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EditorRange {
    pub start: EditorPosition,
    pub end: EditorPosition,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EditorTextEdit {
    pub range: EditorRange,
    pub new_text: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenInfo {
    pub text: String,
    pub range: EditorRange,
    pub byte_start: usize,
    pub byte_end: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompletionContextKind {
    Xprompt,
    SlashSkill,
    FilePath,
    FileHistory,
    XpromptArgumentName,
    XpromptArgumentValue,
    XpromptArgumentPath,
    XpromptArgumentTypeHint,
    DirectiveName,
    DirectiveArgument,
    SnippetTrigger,
    VcsProject,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompletionContext {
    pub kind: CompletionContextKind,
    pub token: Option<TokenInfo>,
    #[serde(default)]
    pub active_xprompt: Option<String>,
    #[serde(default)]
    pub active_input: Option<String>,
    #[serde(default)]
    pub directive_name: Option<String>,
    pub replacement_range: EditorRange,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompletionCandidate {
    pub display: String,
    pub insertion: String,
    #[serde(default)]
    pub detail: Option<String>,
    #[serde(default)]
    pub documentation: Option<String>,
    #[serde(default)]
    pub is_dir: bool,
    pub name: String,
    #[serde(default)]
    pub replacement: Option<EditorTextEdit>,
    /// Secondary edits applied alongside `replacement` (the LSP
    /// `additionalTextEdits`). Used by `vcs_project` completion to prepend or
    /// replace the VCS workflow tag at the start of the document while the
    /// primary edit consumes the `+query` trigger token. Empty for every other
    /// completion kind.
    #[serde(default)]
    pub additional_edits: Vec<EditorTextEdit>,
    /// Optional entry discriminator for specialized completion surfaces.
    /// `vcs_project` uses `project` or `changespec`; generic completion kinds
    /// leave it empty.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub kind: String,
    /// Optional owning project context for specialized completion surfaces.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub project: String,
    /// Optional status context for specialized completion surfaces.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub status: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompletionList {
    pub candidates: Vec<CompletionCandidate>,
    pub shared_extension: String,
}

/// One active project or ChangeSpec completion candidate for the `+`
/// (`vcs_project`) completion kind.
///
/// This mirrors the Python `VcsProjectEntry` produced by
/// `build_vcs_project_completion_entries`; the LSP receives a JSON catalog of
/// these (materialized in Phase 4) and the TUI builds them in-process. The two
/// surfaces stay in sync via the shared golden test-vector table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VcsProjectEntry {
    /// Project name (e.g. `sase`) or ChangeSpec name.
    pub name: String,
    /// VCS workflow prefix (e.g. `gh`, `git`).
    pub vcs_prefix: String,
    /// The resulting VCS workflow tag, without a trailing space (e.g.
    /// `#gh:sase`).
    pub display_tag: String,
    /// Human-readable provider name (e.g. `GitHub`), falling back to
    /// `vcs_prefix` when no display name is registered.
    pub provider_display: String,
    /// Project description, when available (empty otherwise).
    #[serde(default)]
    pub description: String,
    /// Alternate names the project can be matched by.
    #[serde(default)]
    pub aliases: Vec<String>,
    /// `project` for project rows, `changespec` for PR rows.
    #[serde(default = "default_vcs_project_entry_kind")]
    pub kind: String,
    /// Owning project basename. For project rows, this equals `name` in v2
    /// catalogs and may be empty for v1 catalogs.
    #[serde(default)]
    pub project: String,
    /// Base ChangeSpec status for PR rows; empty for project rows.
    #[serde(default)]
    pub status: String,
}

fn default_vcs_project_entry_kind() -> String {
    "project".to_string()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct XpromptInputHint {
    pub name: String,
    #[serde(rename = "type")]
    pub r#type: String,
    #[serde(default)]
    pub description: Option<String>,
    pub required: bool,
    pub default_display: Option<String>,
    pub position: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct XpromptAssistEntry {
    pub name: String,
    pub display_label: String,
    pub insertion: String,
    pub reference_prefix: String,
    pub kind: Option<String>,
    pub source_bucket: String,
    pub project: Option<String>,
    pub tags: Vec<String>,
    pub input_signature: Option<String>,
    pub inputs: Vec<XpromptInputHint>,
    pub content_preview: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub source_path_display: Option<String>,
    #[serde(default)]
    pub definition_path: Option<String>,
    #[serde(default)]
    pub definition_range: Option<EditorRange>,
    pub is_skill: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectiveMetadata {
    pub name: &'static str,
    pub alias: Option<&'static str>,
    pub description: &'static str,
    pub takes_argument: bool,
    pub allows_multiple: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticSeverity {
    Error,
    Warning,
    Information,
    Hint,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EditorDiagnostic {
    pub range: EditorRange,
    pub severity: DiagnosticSeverity,
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HoverPayload {
    pub range: EditorRange,
    pub markdown: String,
}

/// Structural shape of a frontmatter field, used by the prompt frontmatter
/// panel to pick an appropriate editor for each property.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FrontmatterFieldKind {
    /// A single scalar value (e.g. `name`, `description`).
    Scalar,
    /// A comma-separated string or sequence of scalars (e.g. `tags`).
    List,
    /// `true`, `false`, or a sequence of scalars (e.g. `skill`).
    BoolOrList,
    /// `true`, `false`, or a single scalar trigger (e.g. `snippet`).
    BoolOrScalar,
    /// A nested, structured value with its own item editor (e.g. `input`,
    /// `xprompts`).
    Structured,
}

/// A panel-oriented descriptor for one supported frontmatter field.
///
/// This is the single source of truth that the prompt frontmatter panel and
/// the xprompt LSP share for "what fields exist and what they mean," so the
/// TUI and editor guidance never drift.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FrontmatterFieldSchema {
    pub name: String,
    pub kind: FrontmatterFieldKind,
    pub required: bool,
    /// One-line summary, shared with hover documentation.
    pub description: String,
    /// Optional human hint describing the allowed values.
    #[serde(default)]
    pub allowed_values: Option<String>,
    /// A short example value for the field.
    pub example: String,
}

/// A panel-oriented descriptor for one supported `input` type.
///
/// Drives the per-type guidance shown in the input collection modal. The
/// canonical name and aliases mirror the parser's accepted spellings so
/// validation and guidance stay in lockstep.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FrontmatterInputType {
    /// Canonical type name (e.g. `int`).
    pub name: String,
    /// Accepted aliases for the canonical name (e.g. `integer`).
    pub aliases: Vec<String>,
    /// One-line human rule describing what values the type accepts.
    pub rule: String,
}
