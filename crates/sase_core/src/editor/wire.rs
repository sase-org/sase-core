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
    DoubleColon,
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
    FinalizerInstance,
    FreeText,
    GateOwned,
    Code,
    Duration,
    Language,
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

/// One directive authoring template advertised to editor frontends.
///
/// `insert_text` is LSP snippet syntax, while `template` uses SASE's prompt
/// snippet tabstop syntax (`$1`, `$2`, `$0`) for ACE. `plain_text` is the
/// non-snippet fallback for clients that cannot expand snippets.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectiveSnippetRecipeContract {
    pub label: String,
    pub detail: String,
    pub insert_text: String,
    pub template: String,
    pub plain_text: String,
    pub documentation: String,
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

/// How a directive binds a code body.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DirectiveBodyKind {
    None,
    FencedCode,
    OptionalFencedCode,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub feature_flag: Option<String>,
    pub body_kind: DirectiveBodyKind,
    pub synopsis: String,
    #[serde(default)]
    pub examples: Vec<String>,
    #[serde(default)]
    pub recipes: Vec<DirectiveSnippetRecipeContract>,
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
            feature_flag: directive_feature_flag(metadata.name)
                .map(str::to_string),
            body_kind: directive_body_kind(metadata.name),
            synopsis: directive_synopsis(metadata).to_string(),
            examples: directive_examples(metadata.name)
                .iter()
                .map(|example| (*example).to_string())
                .collect(),
            recipes: directive_snippet_recipes(metadata.name),
        }
    }
}

/// Feature flag that gates a directive, if any.
pub fn directive_feature_flag(name: &str) -> Option<&'static str> {
    match name {
        "if" | "proc" => Some("typed_launch_units"),
        _ => None,
    }
}

/// How a named directive binds its executable body.
pub fn directive_body_kind(name: &str) -> DirectiveBodyKind {
    match name {
        "if" => DirectiveBodyKind::FencedCode,
        "proc" => DirectiveBodyKind::OptionalFencedCode,
        _ => DirectiveBodyKind::None,
    }
}

/// One-line synopsis used by hover and completion documentation.
pub fn directive_synopsis(metadata: &DirectiveMetadata) -> &'static str {
    match metadata.name {
        "if" => "%if:: plus exactly one bash or python fence; attaches to the next launch unit",
        "proc" => "%proc(\"cmd\"), %proc(bash=|python=), or %proc:: plus one fence",
        _ => metadata.argument_hint,
    }
}

/// Copyable examples for a directive. Empty for ungated historical directives.
pub fn directive_examples(name: &str) -> &'static [&'static str] {
    match name {
        "if" => &[
            "%if::\n\n```bash\ntest -f pyproject.toml\n```",
            "%if::\n\n```python\nraise SystemExit(0)\n```",
        ],
        "proc" => &[
            "%proc(\"just check\")",
            "%proc(bash=\"just check\", timeout=\"20m\", label=\"Scoped verification\")",
            "%proc(python=\"print('ready')\", workspace=false)",
            "%proc(timeout=\"20m\")::\n\n```bash\njust check\n```",
        ],
        _ => &[],
    }
}

/// Snippet recipes for full directive forms and common option combinations.
pub fn directive_snippet_recipes(
    name: &str,
) -> Vec<DirectiveSnippetRecipeContract> {
    match name {
        "alt" => vec![recipe(
            "%alt:...",
            "directive snippet",
            "%{${1:A} | ${2:B}\\}$0",
            "%{$1 | $2}$0",
            "%{A | B}",
            "Branch fan-out between two prompt alternatives.",
        )],
        "clan" => vec![
            colon_recipe("clan", "name"),
            recipe(
                "%clan(..., tribe=...)",
                "directive snippet",
                "%clan(${1:name}, tribe=${2:tribe})$0",
                "%clan($1, tribe=$2)$0",
                "%clan(name, tribe=tribe)",
                "Declare a parallel clan and assign it to a tribe.",
            ),
        ],
        "wait" => vec![
            colon_recipe("wait", "value"),
            recipe(
                "%wait(..., bead=...)",
                "directive snippet",
                "%wait(${1:agent}, bead=${2:bead-id})$0",
                "%wait($1, bead=$2)$0",
                "%wait(agent, bead=bead-id)",
                "Wait on an agent and an open bead before launching.",
            ),
            recipe(
                "%wait(proc=...)",
                "typed launch unit snippet",
                "%wait(proc=${1:proc-id-or-shell-name})$0",
                "%wait(proc=$1)$0",
                "%wait(proc=proc-id-or-shell-name)",
                "Wait for a prompt-owned proc by ID or shell name.",
            ),
            recipe(
                "%wait(time=...)",
                "directive snippet",
                "%wait(time=${1:5m})$0",
                "%wait(time=$1)$0",
                "%wait(time=5m)",
                "Wait for a duration floor before launching.",
            ),
        ],
        "model" => vec![
            colon_recipe("model", "value"),
            recipe(
                "%model(..., alias=...)",
                "directive snippet",
                "%model(${1:model}, ${2:alias}=${3:model})$0",
                "%model($1, $2=$3)$0",
                "%model(model, alias=model)",
                "Select a model and bind an alias override.",
            ),
        ],
        "id" => vec![
            colon_recipe("id", "agent-id"),
            recipe(
                "%id(..., clan=...)",
                "directive snippet",
                "%id(${1:id}, clan=${2:clan})$0",
                "%id($1, clan=$2)$0",
                "%id(id, clan=clan)",
                "Assign an explicit ID inside an existing clan.",
            ),
            recipe(
                "%id(..., family=...)",
                "directive snippet",
                "%id(${1:suffix}, family=${2:family})$0",
                "%id($1, family=$2)$0",
                "%id(suffix, family=family)",
                "Assign a family child suffix.",
            ),
            recipe(
                "%id(tribe=...)",
                "directive snippet",
                "%id(tribe=${1:tribe})$0",
                "%id(tribe=$1)$0",
                "%id(tribe=tribe)",
                "Assign a tribe to an auto-named launch.",
            ),
        ],
        "final" => vec![
            colon_recipe("final", "instance"),
            recipe(
                "%final(...)",
                "directive snippet",
                "%final(${1:instance}, ${2:instance})$0",
                "%final($1, $2)$0",
                "%final(instance, instance)",
                "Select finalizer instances.",
            ),
        ],
        "if" => vec![
            recipe(
                "%if:: bash",
                "typed launch unit snippet",
                "%if::\n\n```bash\n${1:test -f pyproject.toml}\n```$0",
                "%if::\n\n```bash\n$1\n```$0",
                "%if::\n\n```bash\n\n```",
                "Attach a bash guard to the next launch unit.",
            ),
            recipe(
                "%if:: python",
                "typed launch unit snippet",
                "%if::\n\n```python\n${1:raise SystemExit(0)}\n```$0",
                "%if::\n\n```python\n$1\n```$0",
                "%if::\n\n```python\n\n```",
                "Attach a Python guard to the next launch unit.",
            ),
        ],
        "proc" => vec![
            recipe(
                "%proc(\"...\")",
                "typed launch unit snippet",
                "%proc(\"${1:just check}\")$0",
                "%proc(\"$1\")$0",
                "%proc(\"just check\")",
                "Run a prompt-owned process from a command string.",
            ),
            recipe(
                "%proc(bash=...)",
                "typed launch unit snippet",
                "%proc(bash=\"${1:just check}\")$0",
                "%proc(bash=\"$1\")$0",
                "%proc(bash=\"just check\")",
                "Run a bash prompt-owned process.",
            ),
            recipe(
                "%proc(python=...)",
                "typed launch unit snippet",
                "%proc(python=\"${1:print('ready')}\")$0",
                "%proc(python=\"$1\")$0",
                "%proc(python=\"print('ready')\")",
                "Run a Python prompt-owned process.",
            ),
            recipe(
                "%proc(..., timeout=...)",
                "typed launch unit snippet",
                "%proc(bash=\"${1:just check}\", timeout=\"${2:20m}\")$0",
                "%proc(bash=\"$1\", timeout=\"$2\")$0",
                "%proc(bash=\"just check\", timeout=\"20m\")",
                "Run a process with an explicit timeout.",
            ),
            recipe(
                "%proc:: bash",
                "typed launch unit snippet",
                "%proc(timeout=\"${1:20m}\")::\n\n```bash\n${2:just check}\n```$0",
                "%proc(timeout=\"$1\")::\n\n```bash\n$2\n```$0",
                "%proc(timeout=\"20m\")::\n\n```bash\n\n```",
                "Run a bash prompt-owned process from a fenced body.",
            ),
            recipe(
                "%proc:: python",
                "typed launch unit snippet",
                "%proc(timeout=\"${1:20m}\")::\n\n```python\n${2:print('ready')}\n```$0",
                "%proc(timeout=\"$1\")::\n\n```python\n$2\n```$0",
                "%proc(timeout=\"20m\")::\n\n```python\n\n```",
                "Run a Python prompt-owned process from a fenced body.",
            ),
        ],
        _ => directive_metadata_supports_colon(name)
            .then(|| colon_recipe(name, "value"))
            .into_iter()
            .collect(),
    }
}

fn directive_metadata_supports_colon(name: &str) -> bool {
    matches!(
        name,
        "model"
            | "effort"
            | "id"
            | "clan"
            | "wait"
            | "repeat"
            | "auto"
            | "final"
            | "xprompts_enabled"
    )
}

fn colon_recipe(
    name: &str,
    placeholder: &str,
) -> DirectiveSnippetRecipeContract {
    recipe(
        &format!("%{name}:..."),
        "directive snippet",
        &format!("%{name}:${{1:{placeholder}}}$0"),
        &format!("%{name}:$1$0"),
        &format!("%{name}:{placeholder}"),
        "Fill the directive's colon-form argument.",
    )
}

fn recipe(
    label: &str,
    detail: &str,
    insert_text: &str,
    template: &str,
    plain_text: &str,
    documentation: &str,
) -> DirectiveSnippetRecipeContract {
    DirectiveSnippetRecipeContract {
        label: label.to_string(),
        detail: detail.to_string(),
        insert_text: insert_text.to_string(),
        template: template.to_string(),
        plain_text: plain_text.to_string(),
        documentation: documentation.to_string(),
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

/// One configured finalizer instance supplied by the host for `%final`.
///
/// Additive and mixed-version safe: older helpers may omit policy, provider,
/// dependency, and retry fields, and older consumers ignore unknown extras.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectiveFinalizerEntry {
    /// Instance ID used as the selector token.
    pub value: String,
    #[serde(default)]
    pub display: String,
    /// Legacy provider display string. Prefer [`Self::provider_ref`].
    #[serde(default)]
    pub detail: String,
    #[serde(default)]
    pub documentation: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub provider_ref: String,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub required: bool,
    #[serde(
        default,
        rename = "default",
        skip_serializing_if = "std::ops::Not::not"
    )]
    pub is_default: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub after: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_attempts: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provenance_id: Option<String>,
}

pub const FINALIZER_CATALOG_SCHEMA_VERSION: u32 = 1;

/// Fresh finalizer catalog request sent to the Python editor helper bridge.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FinalizerCatalogRequest {
    pub schema_version: u32,
    /// Optional project hint so the helper can prefer that store. Older
    /// helpers ignore unknown fields; newer helpers still return rows when
    /// this is omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
}

/// Fresh finalizer catalog returned by the Python editor helper bridge.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FinalizerCatalogResponse {
    pub schema_version: u32,
    pub status: String,
    #[serde(default)]
    pub message: String,
    #[serde(default)]
    pub entries: Vec<DirectiveFinalizerEntry>,
}

impl FinalizerCatalogResponse {
    pub fn ok_empty() -> Self {
        Self {
            schema_version: FINALIZER_CATALOG_SCHEMA_VERSION,
            status: "ok".to_string(),
            message: String::new(),
            entries: Vec::new(),
        }
    }
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
    #[serde(default)]
    pub finalizers: Vec<DirectiveFinalizerEntry>,
    /// Bead IDs that must never be offered (for example the launching
    /// agent's own bead).
    #[serde(default)]
    pub excluded_bead_ids: Vec<String>,
    /// Startup-resolved feature-flag keys that are currently enabled.
    /// Completion never reads feature-flag state itself.
    #[serde(default)]
    pub enabled_feature_flags: Vec<String>,
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
    /// When false the type is parsed and transported but not advertised in
    /// public completion, pickers, or helper catalogs.
    #[serde(default = "default_true")]
    pub advertised: bool,
}

fn default_true() -> bool {
    true
}
