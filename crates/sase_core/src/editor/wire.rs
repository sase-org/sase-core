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
    Placeholder,
    Xprompt,
    SlashSkill,
    FilePath,
    FileHistory,
    XpromptArgumentName,
    XpromptArgumentValue,
    XpromptArgumentPath,
    XpromptArgumentAgent,
    XpromptArgumentTypeHint,
    DirectiveName,
    DirectiveArgument,
    DirectiveArgumentKeyword,
    SnippetTrigger,
    VcsProject,
    VcsRepo,
    VcsRef,
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
    #[serde(default)]
    pub selected_values: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vcs_repo: Option<VcsRepoTrigger>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vcs_ref: Option<VcsRefTrigger>,
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

/// One enabled project or ChangeSpec completion candidate for the `+`
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

/// One org/group-style namespace completion candidate for a VCS workflow's ref
/// root (`#gh:<namespace>/`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VcsNamespaceEntry {
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default = "default_vcs_namespace_kind_label")]
    pub kind_label: String,
}

fn default_vcs_namespace_kind_label() -> String {
    "org".to_string()
}

/// Detected repository-completion trigger for a VCS workflow ref.
///
/// Byte spans mirror the Python `VcsRepoTrigger` parity contract. The
/// `replacement_range` on [`CompletionContext`] covers `value_span`; the
/// namespace/query spans are carried separately so frontends can render and
/// filter without reparsing the token.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VcsRepoTrigger {
    pub start: usize,
    pub end: usize,
    pub workflow: String,
    pub separator: String,
    pub ref_start: usize,
    pub ref_end: usize,
    pub namespace: String,
    pub query: String,
    pub namespace_span: (usize, usize),
    pub query_span: (usize, usize),
}

/// Detected root-ref completion trigger for a VCS workflow ref.
///
/// This owns only the root segment (`#gh:sa` / `#gh(sa`) before any slash.
/// Repository-path refs containing `/` are handled by [`VcsRepoTrigger`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VcsRefTrigger {
    pub start: usize,
    pub end: usize,
    pub workflow: String,
    pub separator: String,
    pub ref_start: usize,
    pub ref_end: usize,
    pub query: String,
    pub query_span: (usize, usize),
}

/// One repository completion entry returned by the helper bridge.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VcsRepoEntry {
    pub name: String,
    #[serde(rename = "ref")]
    pub r#ref: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub visibility: String,
    #[serde(default)]
    pub is_fork: bool,
    #[serde(default)]
    pub is_archived: bool,
    #[serde(default)]
    pub pushed_at: Option<String>,
}

/// One prompt-referenceable agent returned by the editor helper bridge.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentCompletionEntry {
    pub name: String,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub project: String,
    /// `agent`, `family`, `clan`, or `tribe`. Missing values from older
    /// helpers intentionally retain the historical plain-agent behavior.
    #[serde(default)]
    pub kind: String,
    #[serde(default)]
    pub member_count: usize,
    #[serde(default)]
    pub detail: String,
}

pub const AGENT_CATALOG_SCHEMA_VERSION: u32 = 1;

/// Fresh agent catalog request sent to the Python editor helper bridge.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentCatalogRequest {
    pub schema_version: u32,
}

/// Fresh agent catalog returned by the Python editor helper bridge.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentCatalogResponse {
    pub schema_version: u32,
    pub status: String,
    #[serde(default)]
    pub message: String,
    #[serde(default)]
    pub entries: Vec<AgentCompletionEntry>,
}

pub const VCS_REPO_CATALOG_SCHEMA_VERSION: u32 = 1;

/// Repository completion catalog request sent to the Python helper bridge.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VcsRepoCatalogRequest {
    pub schema_version: u32,
    pub workflow: String,
    pub namespace: String,
}

/// Repository completion catalog returned by the Python helper bridge.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VcsRepoCatalogResponse {
    pub schema_version: u32,
    pub status: String,
    #[serde(default)]
    pub error_kind: Option<String>,
    #[serde(default)]
    pub message: String,
    #[serde(default)]
    pub provider_display: String,
    #[serde(default)]
    pub stale: bool,
    #[serde(default)]
    pub entries: Vec<VcsRepoEntry>,
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
    #[serde(default)]
    pub repeatable: bool,
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
