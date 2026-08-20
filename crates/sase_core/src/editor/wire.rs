use serde::{Deserialize, Serialize};

use crate::content_layout::MemoryTierWire;

pub const EDITOR_WIRE_SCHEMA_VERSION: u32 = 2;

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
    ArtifactRefKind,
    ArtifactRefPayload,
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
    DirectiveArgumentValue,
    SnippetTrigger,
    VcsProject,
    VcsRepo,
    VcsRef,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactRefCompletionMode {
    Kind,
    Payload,
}

/// Detected kind or payload completion trigger for an artifact reference.
///
/// Spans are UTF-8 byte offsets into the document. `candidate_span` covers
/// the complete `@kind:payload` candidate, `replacement_span` is the segment
/// frontends should replace on accept, and `query_span` is the prefix used to
/// filter completion rows at the active cursor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactRefCompletionTrigger {
    pub mode: ArtifactRefCompletionMode,
    pub candidate_span: (usize, usize),
    pub replacement_span: (usize, usize),
    pub query_span: (usize, usize),
    pub query: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
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
    /// Grammar-aware directive clause details. Absent for non-directive
    /// contexts and for directive-name completion.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub directive: Option<DirectiveClauseContext>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vcs_repo: Option<VcsRepoTrigger>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vcs_ref: Option<VcsRefTrigger>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_ref: Option<ArtifactRefCompletionTrigger>,
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
    /// `vcs_project` uses `project` or `patch`; generic completion kinds leave
    /// it empty. Legacy catalogs may still carry `changespec`.
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

/// One enabled project or patch completion candidate for the `+`
/// (`vcs_project`) completion kind.
///
/// This mirrors the Python `VcsProjectEntry` produced by
/// `build_vcs_project_completion_entries`; the LSP receives a JSON catalog of
/// these (materialized in Phase 4) and the TUI builds them in-process. The two
/// surfaces stay in sync via the shared golden test-vector table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VcsProjectEntry {
    /// Project name (e.g. `sase`) or patch name.
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
    /// Canonical entry discriminator. `project` for project rows, `patch` for
    /// patch rows. Present in v4+ catalogs.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub entry_kind: String,
    /// Legacy entry discriminator. `project` for project rows, `changespec` for
    /// patch rows in older catalogs.
    #[serde(default = "default_vcs_project_entry_kind")]
    pub kind: String,
    /// Owning project basename. For project rows, this equals `name` in v2
    /// catalogs and may be empty for v1 catalogs.
    #[serde(default)]
    pub project: String,
    /// Base patch status for patch rows; empty for project rows.
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
    /// Optional markdown block supplied by the Python editor helper, rendered
    /// in the editor's documentation popup. Empty when the helper has nothing
    /// to show.
    #[serde(default)]
    pub documentation: String,
}

pub const AGENT_CATALOG_SCHEMA_VERSION: u32 = 1;

/// Fresh agent catalog request sent to the Python editor helper bridge.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentCatalogRequest {
    pub schema_version: u32,
    /// Optional project hint so the helper can prefer that store for bead
    /// rows. Older helpers ignore unknown fields; newer helpers still return
    /// agents when this is omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
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
    /// Bounded open-bead rows for `%wait(bead=)` / `%id(..., bead=)`.
    /// Omitted by mixed-version helpers; empty when the store is unavailable.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub beads: Vec<BeadCompletionEntry>,
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
    /// Provider skill name (`foo`), as opposed to the xprompt reference `name`
    /// (`skill/foo`). Slash completion, slash diagnostics, hover, and
    /// definition lookup match this; `#` completion matches `name`.
    #[serde(default)]
    pub skill_name: Option<String>,
    /// Tier of the SASE memory note behind an xprompt memory, absent for every
    /// other entry. Memory entries are reachable only as `#memory/<stem>` and
    /// never participate in `/` skill completion.
    #[serde(default)]
    pub memory_type: Option<MemoryTierWire>,
}

/// Allowed surface syntax for one directive. Classifiers must not advertise
/// keywords in a form the runtime treats as positional-only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DirectiveSyntaxForm {
    Bare,
    Colon,
    Parenthesized,
    Plus,
    BraceShorthand,
}

/// Dynamic or static value provider role for a positional argument or
/// keyword value. Suggestions are assistance, never an accidental allowlist.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DirectiveValueRole {
    Model,
    ModelAliasKey,
    Agent,
    Clan,
    Family,
    Tribe,
    Bead,
    PathOrExecutable,
    Bool,
    NonNegativeInt,
    PositiveInt,
    WaitTime,
    FreeText,
    GateOwned,
}

/// Which part of a directive argument clause the cursor is in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DirectiveClauseKind {
    Positional,
    KeywordName,
    KeywordValue,
}

/// One documented example value for a positional argument or keyword.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct DirectiveSuggestedValue {
    pub value: &'static str,
    pub documentation: &'static str,
}

/// Owned suggested-value row for the serializable directive contract.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectiveSuggestedValueWire {
    pub value: String,
    pub documentation: String,
}

/// Static keyword specification for one directive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct DirectiveKeywordSpec {
    pub name: &'static str,
    pub description: &'static str,
    pub value_role: DirectiveValueRole,
    pub repeatable: bool,
    pub conflicts_with: &'static [&'static str],
    pub suggested_values: &'static [DirectiveSuggestedValue],
}

/// Owned keyword row for the serializable directive contract.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectiveKeywordContract {
    pub name: String,
    pub description: String,
    pub value_role: DirectiveValueRole,
    pub repeatable: bool,
    pub conflicts_with: Vec<String>,
    pub suggested_values: Vec<DirectiveSuggestedValueWire>,
}

/// Canonical editor/domain metadata for one xprompt directive.
///
/// Extra fields beyond the historical name/alias/description triple are the
/// shared completion contract consumed by ACE and the xprompt LSP.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DirectiveMetadata {
    pub name: &'static str,
    pub alias: Option<&'static str>,
    pub description: &'static str,
    pub argument_hint: &'static str,
    pub takes_argument: bool,
    pub allows_multiple: bool,
    pub syntax_forms: &'static [DirectiveSyntaxForm],
    pub positional_role: Option<DirectiveValueRole>,
    pub positional_suggestions: &'static [DirectiveSuggestedValue],
    pub keywords: &'static [DirectiveKeywordSpec],
    pub dynamic_keyword_role: Option<DirectiveValueRole>,
}

/// JSON-shaped owned copy of [`DirectiveMetadata`] for Python/ACE bindings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectiveContractEntry {
    pub name: String,
    pub alias: Option<String>,
    pub description: String,
    pub argument_hint: String,
    pub takes_argument: bool,
    pub allows_multiple: bool,
    pub syntax_forms: Vec<DirectiveSyntaxForm>,
    pub positional_role: Option<DirectiveValueRole>,
    pub positional_suggestions: Vec<DirectiveSuggestedValueWire>,
    pub keywords: Vec<DirectiveKeywordContract>,
    pub dynamic_keyword_role: Option<DirectiveValueRole>,
}

impl From<&DirectiveMetadata> for DirectiveContractEntry {
    fn from(metadata: &DirectiveMetadata) -> Self {
        Self {
            name: metadata.name.to_string(),
            alias: metadata.alias.map(str::to_string),
            description: metadata.description.to_string(),
            argument_hint: metadata.argument_hint.to_string(),
            takes_argument: metadata.takes_argument,
            allows_multiple: metadata.allows_multiple,
            syntax_forms: metadata.syntax_forms.to_vec(),
            positional_role: metadata.positional_role,
            positional_suggestions: metadata
                .positional_suggestions
                .iter()
                .map(|value| DirectiveSuggestedValueWire {
                    value: value.value.to_string(),
                    documentation: value.documentation.to_string(),
                })
                .collect(),
            keywords: metadata
                .keywords
                .iter()
                .map(|keyword| DirectiveKeywordContract {
                    name: keyword.name.to_string(),
                    description: keyword.description.to_string(),
                    value_role: keyword.value_role,
                    repeatable: keyword.repeatable,
                    conflicts_with: keyword
                        .conflicts_with
                        .iter()
                        .map(|name| (*name).to_string())
                        .collect(),
                    suggested_values: keyword
                        .suggested_values
                        .iter()
                        .map(|value| DirectiveSuggestedValueWire {
                            value: value.value.to_string(),
                            documentation: value.documentation.to_string(),
                        })
                        .collect(),
                })
                .collect(),
            dynamic_keyword_role: metadata.dynamic_keyword_role,
        }
    }
}

/// Grammar classification for the active directive clause at the cursor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectiveClauseContext {
    pub syntax_form: DirectiveSyntaxForm,
    pub clause_kind: DirectiveClauseKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_keyword: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value_role: Option<DirectiveValueRole>,
    #[serde(default)]
    pub selected_keywords: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub clause_range: Option<EditorRange>,
}

/// One open-bead inventory row supplied by the host for directive completion.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadCompletionEntry {
    pub id: String,
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub type_label: String,
    #[serde(default)]
    pub created_at: String,
    #[serde(default)]
    pub updated_at: String,
    #[serde(default)]
    pub task_type: String,
    #[serde(default)]
    pub project: String,
}

/// One model-catalog row supplied by the host for `%model` completion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectiveModelEntry {
    pub value: String,
    #[serde(default)]
    pub display: String,
    #[serde(default)]
    pub detail: String,
    #[serde(default)]
    pub documentation: String,
}

/// One configured model-alias key for `%model(...)` override completion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectiveModelAliasKey {
    pub name: String,
    #[serde(default)]
    pub documentation: String,
}

/// Host-supplied dynamic inventories for directive value completion.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectiveCompletionInventories {
    #[serde(default)]
    pub models: Vec<DirectiveModelEntry>,
    #[serde(default)]
    pub model_alias_keys: Vec<DirectiveModelAliasKey>,
    #[serde(default)]
    pub agents: Vec<AgentCompletionEntry>,
    #[serde(default)]
    pub beads: Vec<BeadCompletionEntry>,
    /// Bead IDs that must never be offered (for example the launching
    /// agent's own bead).
    #[serde(default)]
    pub excluded_bead_ids: Vec<String>,
}

impl CompletionContext {
    pub fn syntax_form(&self) -> Option<DirectiveSyntaxForm> {
        self.directive
            .as_ref()
            .map(|directive| directive.syntax_form)
    }

    pub fn clause_kind(&self) -> Option<DirectiveClauseKind> {
        self.directive
            .as_ref()
            .map(|directive| directive.clause_kind)
    }

    pub fn active_keyword(&self) -> Option<&str> {
        self.directive
            .as_ref()
            .and_then(|directive| directive.active_keyword.as_deref())
    }

    pub fn value_role(&self) -> Option<DirectiveValueRole> {
        self.directive
            .as_ref()
            .and_then(|directive| directive.value_role)
    }

    pub fn selected_keywords(&self) -> &[String] {
        self.directive
            .as_ref()
            .map(|directive| directive.selected_keywords.as_slice())
            .unwrap_or(&[])
    }
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
