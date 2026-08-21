use super::token::DocumentSnapshot;
use super::wire::{
    BeadCompletionEntry, CompletionCandidate, CompletionContext,
    CompletionContextKind, CompletionList, DirectiveClauseContext,
    DirectiveClauseKind, DirectiveContractEntry, DirectiveKeywordSpec,
    DirectiveMetadata, DirectiveSuggestedValue, DirectiveSyntaxForm,
    DirectiveValueRole, EditorPosition, EditorRange, EditorTextEdit, TokenInfo,
};
const AUTO_COMPATIBILITY_ARGUMENT_SUGGESTIONS: &[DirectiveSuggestedValue] = &[
    DirectiveSuggestedValue {
        value: "plan",
        documentation: "Plan-gate compatibility alias for normal approval",
    },
    DirectiveSuggestedValue {
        value: "tale",
        documentation: "Plan-gate compatibility alias for SDD tale approval",
    },
    DirectiveSuggestedValue {
        value: "epic",
        documentation: "Plan-gate compatibility alias for SDD epic approval",
    },
];

const EFFORT_SUGGESTIONS: &[DirectiveSuggestedValue] = &[
    DirectiveSuggestedValue {
        value: "none",
        documentation: "No reasoning-effort override",
    },
    DirectiveSuggestedValue {
        value: "minimal",
        documentation: "Minimal reasoning effort",
    },
    DirectiveSuggestedValue {
        value: "low",
        documentation: "Low reasoning effort",
    },
    DirectiveSuggestedValue {
        value: "medium",
        documentation: "Medium reasoning effort",
    },
    DirectiveSuggestedValue {
        value: "high",
        documentation: "High reasoning effort",
    },
    DirectiveSuggestedValue {
        value: "xhigh",
        documentation: "Extra-high reasoning effort",
    },
    DirectiveSuggestedValue {
        value: "max",
        documentation: "Maximum reasoning effort",
    },
];

const BOOL_SUGGESTIONS: &[DirectiveSuggestedValue] = &[
    DirectiveSuggestedValue {
        value: "false",
        documentation: "Disable xprompt expansion",
    },
    DirectiveSuggestedValue {
        value: "true",
        documentation: "Enable xprompt expansion",
    },
];

const REPEAT_SUGGESTIONS: &[DirectiveSuggestedValue] = &[
    DirectiveSuggestedValue {
        value: "2",
        documentation: "Run the prompt twice",
    },
    DirectiveSuggestedValue {
        value: "3",
        documentation: "Run the prompt three times",
    },
];

const FINAL_SUGGESTIONS: &[DirectiveSuggestedValue] =
    &[DirectiveSuggestedValue {
        value: "none",
        documentation:
            "Clear the configured finalizer selection for this launch",
    }];

const WAIT_TIME_SUGGESTIONS: &[DirectiveSuggestedValue] = &[
    DirectiveSuggestedValue {
        value: "5m",
        documentation: "Duration: start after five minutes",
    },
    DirectiveSuggestedValue {
        value: "1430",
        documentation: "Wall clock: start at 14:30 today (or tomorrow if past)",
    },
];

const WAIT_RUNNERS_SUGGESTIONS: &[DirectiveSuggestedValue] = &[
    DirectiveSuggestedValue {
        value: "0",
        documentation: "Drain barrier: start after every running agent stops",
    },
    DirectiveSuggestedValue {
        value: "1",
        documentation: "Start when at most one agent is already running",
    },
];

const WAIT_PRIORITY_SUGGESTIONS: &[DirectiveSuggestedValue] = &[
    DirectiveSuggestedValue {
        value: "10",
        documentation: "Default runner-queue priority",
    },
    DirectiveSuggestedValue {
        value: "1",
        documentation: "Join the runner queue ahead of larger priorities",
    },
];

const BARE_PLUS: &[DirectiveSyntaxForm] =
    &[DirectiveSyntaxForm::Bare, DirectiveSyntaxForm::Plus];
const COLON: &[DirectiveSyntaxForm] = &[DirectiveSyntaxForm::Colon];
const COLON_BARE_PLUS: &[DirectiveSyntaxForm] = &[
    DirectiveSyntaxForm::Colon,
    DirectiveSyntaxForm::Bare,
    DirectiveSyntaxForm::Plus,
];
const COLON_PAREN: &[DirectiveSyntaxForm] = &[
    DirectiveSyntaxForm::Colon,
    DirectiveSyntaxForm::Parenthesized,
];
const COLON_PAREN_BARE: &[DirectiveSyntaxForm] = &[
    DirectiveSyntaxForm::Colon,
    DirectiveSyntaxForm::Parenthesized,
    DirectiveSyntaxForm::Bare,
];
const ALT_FORMS: &[DirectiveSyntaxForm] = &[
    DirectiveSyntaxForm::BraceShorthand,
    DirectiveSyntaxForm::Colon,
    DirectiveSyntaxForm::Parenthesized,
];

const ID_KEYWORDS: &[DirectiveKeywordSpec] = &[
    DirectiveKeywordSpec {
        name: "bead",
        description: "Associate this launch with a bead",
        value_role: DirectiveValueRole::Bead,
        repeatable: false,
        conflicts_with: &[],
        suggested_values: &[],
    },
    DirectiveKeywordSpec {
        name: "clan",
        description: "Derive the full ID and join this agent clan",
        value_role: DirectiveValueRole::Clan,
        repeatable: false,
        conflicts_with: &["family", "tribe"],
        suggested_values: &[],
    },
    DirectiveKeywordSpec {
        name: "family",
        description: "Attach this suffix to an existing agent family",
        value_role: DirectiveValueRole::Family,
        repeatable: false,
        conflicts_with: &["clan", "tribe"],
        suggested_values: &[],
    },
    DirectiveKeywordSpec {
        name: "tribe",
        description: "Assign this agent to a user-managed tribe",
        value_role: DirectiveValueRole::Tribe,
        repeatable: false,
        conflicts_with: &["clan", "family"],
        suggested_values: &[],
    },
];

const CLAN_KEYWORDS: &[DirectiveKeywordSpec] = &[
    DirectiveKeywordSpec {
        name: "summary",
        description: "Attach a Rich-markup summary to this clan",
        value_role: DirectiveValueRole::FreeText,
        repeatable: false,
        conflicts_with: &["summary_script"],
        suggested_values: &[],
    },
    DirectiveKeywordSpec {
        name: "summary_script",
        description: "Generate this clan's summary with an executable script",
        value_role: DirectiveValueRole::PathOrExecutable,
        repeatable: false,
        conflicts_with: &["summary"],
        suggested_values: &[],
    },
    DirectiveKeywordSpec {
        name: "tribe",
        description: "Assign this clan to a user-managed tribe",
        value_role: DirectiveValueRole::Tribe,
        repeatable: false,
        conflicts_with: &[],
        suggested_values: &[],
    },
];

const WAIT_KEYWORDS: &[DirectiveKeywordSpec] = &[
    DirectiveKeywordSpec {
        name: "bead",
        description: "Wait until this bead is closed",
        value_role: DirectiveValueRole::Bead,
        repeatable: false,
        conflicts_with: &[],
        suggested_values: &[],
    },
    DirectiveKeywordSpec {
        name: "priority",
        description: "Lower values start first; the default is 10",
        value_role: DirectiveValueRole::NonNegativeInt,
        repeatable: false,
        conflicts_with: &[],
        suggested_values: WAIT_PRIORITY_SUGGESTIONS,
    },
    DirectiveKeywordSpec {
        name: "runners",
        description: "Start when at most this many agents are already running",
        value_role: DirectiveValueRole::NonNegativeInt,
        repeatable: false,
        conflicts_with: &[],
        suggested_values: WAIT_RUNNERS_SUGGESTIONS,
    },
    DirectiveKeywordSpec {
        name: "time",
        description: "Start after a duration or absolute wall-clock time",
        value_role: DirectiveValueRole::WaitTime,
        repeatable: false,
        conflicts_with: &[],
        suggested_values: WAIT_TIME_SUGGESTIONS,
    },
];

pub const BEAD_COMPLETION_LIMIT: usize = 100;

const BEAD_STATUS_RANK: &[(&str, u8)] = &[
    ("in_progress", 0),
    ("claimed", 1),
    ("ready", 2),
    ("open", 3),
    ("snoozed", 4),
];

pub const DIRECTIVES: &[DirectiveMetadata] = &[
    DirectiveMetadata {
        name: "model",
        alias: Some("m"),
        description: "Override the LLM model for this prompt",
        argument_hint: ":model or (model, alias=model)",
        takes_argument: true,
        allows_multiple: false,
        syntax_forms: COLON_PAREN,
        positional_role: Some(DirectiveValueRole::Model),
        positional_suggestions: &[],
        keywords: &[],
        dynamic_keyword_role: Some(DirectiveValueRole::ModelAliasKey),
    },
    DirectiveMetadata {
        // `%e` is the advertised `%effort` alias. It canonicalizes to `effort`
        // for completion, hover, diagnostics, and fan-out parsing. Mirrors the
        // Python xprompt parser's `_DIRECTIVE_ALIASES["e"] = "effort"`.
        name: "effort",
        alias: Some("e"),
        description: "Set the reasoning-effort level for this prompt",
        argument_hint: ":level",
        takes_argument: true,
        allows_multiple: false,
        syntax_forms: COLON,
        positional_role: Some(DirectiveValueRole::FreeText),
        positional_suggestions: EFFORT_SUGGESTIONS,
        keywords: &[],
        dynamic_keyword_role: None,
    },
    DirectiveMetadata {
        name: "final",
        alias: None,
        description: "Select configured finalizer instances for this launch",
        argument_hint: ":instance|!instance|none or (instance, ...)",
        takes_argument: true,
        allows_multiple: true,
        syntax_forms: COLON_PAREN,
        positional_role: Some(DirectiveValueRole::FinalizerInstance),
        positional_suggestions: FINAL_SUGGESTIONS,
        keywords: &[],
        dynamic_keyword_role: None,
    },
    DirectiveMetadata {
        name: "id",
        alias: Some("i"),
        description: "Assign an agent ID with optional bead, clan, family, or user-managed tribe",
        argument_hint:
            ":agent-id or :name.{@key}; ([id], bead=, clan=/family=/tribe=)",
        takes_argument: true,
        allows_multiple: false,
        syntax_forms: COLON_PAREN_BARE,
        positional_role: Some(DirectiveValueRole::FreeText),
        positional_suggestions: &[],
        keywords: ID_KEYWORDS,
        dynamic_keyword_role: None,
    },
    DirectiveMetadata {
        name: "clan",
        alias: Some("c"),
        description: "Declare a new parallel agent clan",
        argument_hint:
            ":name or :name.{@key}, (name, tribe=/summary=/summary_script=), or :name:: summary",
        takes_argument: true,
        allows_multiple: false,
        syntax_forms: COLON_PAREN,
        positional_role: Some(DirectiveValueRole::Clan),
        positional_suggestions: &[],
        keywords: CLAN_KEYWORDS,
        dynamic_keyword_role: None,
    },
    DirectiveMetadata {
        name: "wait",
        alias: Some("w"),
        description: "Wait for another agent/workflow and/or a time floor",
        argument_hint: ":agent or (agent, bead=, time=, runners=, priority=)",
        takes_argument: true,
        allows_multiple: true,
        syntax_forms: COLON_PAREN_BARE,
        positional_role: Some(DirectiveValueRole::Agent),
        positional_suggestions: &[],
        keywords: WAIT_KEYWORDS,
        dynamic_keyword_role: None,
    },
    DirectiveMetadata {
        name: "auto",
        alias: Some("a"),
        description:
            "Request automatic gate resolution; arguments are interpreted by the gate kind",
        argument_hint: ":argument (e.g. plan|tale|epic)",
        takes_argument: true,
        allows_multiple: false,
        syntax_forms: COLON_BARE_PLUS,
        positional_role: Some(DirectiveValueRole::GateOwned),
        positional_suggestions: AUTO_COMPATIBILITY_ARGUMENT_SUGGESTIONS,
        keywords: &[],
        dynamic_keyword_role: None,
    },
    DirectiveMetadata {
        name: "hide",
        alias: Some("h"),
        description: "Hide the agent from the default Agents tab display",
        argument_hint: "flag",
        takes_argument: false,
        allows_multiple: false,
        syntax_forms: BARE_PLUS,
        positional_role: None,
        positional_suggestions: &[],
        keywords: &[],
        dynamic_keyword_role: None,
    },
    DirectiveMetadata {
        name: "repeat",
        alias: Some("r"),
        description: "Run the prompt multiple times",
        argument_hint: ":count",
        takes_argument: true,
        allows_multiple: false,
        syntax_forms: COLON,
        positional_role: Some(DirectiveValueRole::PositiveInt),
        positional_suggestions: REPEAT_SUGGESTIONS,
        keywords: &[],
        dynamic_keyword_role: None,
    },
    DirectiveMetadata {
        // The `%{A | B}` brace shorthand is the advertised alt spelling. The
        // legacy `%(...)` alias is kept parse-compatible via
        // `canonical_directive_name`, but is no longer surfaced here so editor
        // completion/hover stop advertising it.
        name: "alt",
        alias: None,
        description:
            "Split prompt into variants with different text; shorthand %{A | B}",
        argument_hint: "(variants)",
        takes_argument: true,
        allows_multiple: true,
        syntax_forms: ALT_FORMS,
        positional_role: Some(DirectiveValueRole::FreeText),
        positional_suggestions: &[],
        keywords: &[],
        dynamic_keyword_role: None,
    },
    DirectiveMetadata {
        name: "xprompts_enabled",
        alias: None,
        description: "Enable or disable xprompt expansion for a region",
        argument_hint: ":false|true",
        takes_argument: true,
        allows_multiple: true,
        syntax_forms: COLON,
        positional_role: Some(DirectiveValueRole::Bool),
        positional_suggestions: BOOL_SUGGESTIONS,
        keywords: &[],
        dynamic_keyword_role: None,
    },
];

const HIDDEN_COMPLETION_DIRECTIVES: &[&str] = &["final"];

pub fn directive_is_hidden_from_name_completion(name: &str) -> bool {
    HIDDEN_COMPLETION_DIRECTIVES.contains(&name)
}

pub fn canonical_directive_name(raw: &str) -> Option<&'static str> {
    if raw == "(" || raw == "{" {
        return Some("alt");
    }
    DIRECTIVES.iter().find_map(|directive| {
        if directive.name == raw || directive.alias == Some(raw) {
            Some(directive.name)
        } else {
            None
        }
    })
}

pub fn directive_metadata(raw: &str) -> Option<&'static DirectiveMetadata> {
    let canonical = canonical_directive_name(raw)?;
    DIRECTIVES
        .iter()
        .find(|directive| directive.name == canonical)
}

/// Owned JSON-shaped copy of the canonical directive completion contract.
pub fn directive_contract() -> Vec<DirectiveContractEntry> {
    DIRECTIVES
        .iter()
        .map(DirectiveContractEntry::from)
        .collect()
}

pub fn directive_allows_keywords(
    metadata: &DirectiveMetadata,
    syntax_form: DirectiveSyntaxForm,
) -> bool {
    syntax_form == DirectiveSyntaxForm::Parenthesized
        && (!metadata.keywords.is_empty()
            || metadata.dynamic_keyword_role.is_some())
}

pub fn build_directive_completion_candidates(token: &str) -> CompletionList {
    let partial = token.strip_prefix('%').unwrap_or(token).to_lowercase();
    let mut candidates = Vec::new();
    for directive in DIRECTIVES {
        if HIDDEN_COMPLETION_DIRECTIVES.contains(&directive.name) {
            continue;
        }
        if directive.name.starts_with(&partial)
            || directive
                .alias
                .is_some_and(|alias| alias.starts_with(&partial))
        {
            candidates.push(CompletionCandidate {
                display: format!("%{}", directive.name),
                insertion: format!("%{}", directive.name),
                detail: directive.alias.map(|alias| format!("alias %{alias}")),
                documentation: Some(directive.description.to_string()),
                is_dir: false,
                name: directive.name.to_string(),
                replacement: None,
                additional_edits: Vec::new(),
                kind: String::new(),
                project: String::new(),
                status: String::new(),
            });
        }
    }
    candidates.sort_by(|a, b| a.name.cmp(&b.name));
    CompletionList {
        candidates,
        shared_extension: String::new(),
    }
}

pub fn directive_argument_candidates(name: &str) -> CompletionList {
    let Some(metadata) = directive_metadata(name) else {
        return CompletionList {
            candidates: Vec::new(),
            shared_extension: String::new(),
        };
    };
    if !metadata.keywords.is_empty() {
        return CompletionList {
            candidates: metadata
                .keywords
                .iter()
                .map(|keyword| {
                    argument_candidate(
                        &format!("{}=", keyword.name),
                        keyword.description,
                    )
                })
                .collect(),
            shared_extension: String::new(),
        };
    }
    CompletionList {
        candidates: metadata
            .positional_suggestions
            .iter()
            .map(|value| argument_candidate(value.value, value.documentation))
            .collect(),
        shared_extension: String::new(),
    }
}

pub fn build_directive_keyword_candidates(
    metadata: &DirectiveMetadata,
    partial: &str,
    selected_keywords: &[String],
    replacement_range: Option<EditorRange>,
) -> CompletionList {
    let partial = partial.to_lowercase();
    let selected = selected_keyword_set(selected_keywords);
    let mut candidates = Vec::new();
    for keyword in metadata.keywords {
        if !keyword_is_available(keyword, &selected) {
            continue;
        }
        let insertion = format!("{}=", keyword.name);
        if !insertion.to_lowercase().starts_with(&partial)
            && !keyword.name.to_lowercase().starts_with(&partial)
        {
            continue;
        }
        candidates.push(keyword_candidate(
            &insertion,
            keyword.description,
            replacement_range,
        ));
    }
    CompletionList {
        candidates,
        shared_extension: String::new(),
    }
}

pub fn build_directive_static_value_candidates(
    values: &[DirectiveSuggestedValue],
    partial: &str,
    replacement_range: Option<EditorRange>,
) -> CompletionList {
    let partial = partial.to_lowercase();
    CompletionList {
        candidates: values
            .iter()
            .filter(|value| value.value.to_lowercase().starts_with(&partial))
            .map(|value| {
                let mut candidate =
                    argument_candidate(value.value, value.documentation);
                candidate.replacement =
                    replacement_range.map(|range| EditorTextEdit {
                        range,
                        new_text: value.value.to_string(),
                    });
                candidate
            })
            .collect(),
        shared_extension: String::new(),
    }
}

pub fn rank_and_filter_bead_entries<'a>(
    entries: &'a [BeadCompletionEntry],
    fragment: &str,
    selected_ids: &[String],
    excluded_ids: &[String],
    limit: usize,
) -> Vec<&'a BeadCompletionEntry> {
    let fragment = fragment.trim().to_lowercase();
    let selected: Vec<String> =
        selected_ids.iter().map(|id| id.to_lowercase()).collect();
    let excluded: Vec<String> =
        excluded_ids.iter().map(|id| id.to_lowercase()).collect();
    let mut matches: Vec<&BeadCompletionEntry> = entries
        .iter()
        .filter(|entry| {
            let id = entry.id.to_lowercase();
            if selected.iter().any(|value| value == &id)
                || excluded.iter().any(|value| value == &id)
            {
                return false;
            }
            if fragment.is_empty() {
                return true;
            }
            let search = format!("{} {}", entry.id, entry.title).to_lowercase();
            search.contains(&fragment)
        })
        .collect();
    matches.sort_by(|left, right| {
        bead_status_rank(&left.status)
            .cmp(&bead_status_rank(&right.status))
            .then_with(|| right.updated_at.cmp(&left.updated_at))
            .then_with(|| left.id.cmp(&right.id))
    });
    if matches.len() > limit {
        matches.truncate(limit);
    }
    matches
}

pub fn build_bead_completion_candidates(
    entries: &[BeadCompletionEntry],
    fragment: &str,
    selected_ids: &[String],
    excluded_ids: &[String],
    replacement_range: Option<EditorRange>,
) -> CompletionList {
    let ranked = rank_and_filter_bead_entries(
        entries,
        fragment,
        selected_ids,
        excluded_ids,
        BEAD_COMPLETION_LIMIT,
    );
    CompletionList {
        candidates: ranked
            .into_iter()
            .map(|entry| {
                let documentation = bead_documentation(entry);
                CompletionCandidate {
                    display: entry.id.clone(),
                    insertion: entry.id.clone(),
                    detail: Some(bead_detail(entry)),
                    documentation: Some(documentation)
                        .filter(|value| !value.is_empty()),
                    is_dir: false,
                    name: entry.id.clone(),
                    replacement: replacement_range.map(|range| {
                        EditorTextEdit {
                            range,
                            new_text: entry.id.clone(),
                        }
                    }),
                    additional_edits: Vec::new(),
                    kind: "bead".to_string(),
                    project: entry.project.clone(),
                    status: entry.status.clone(),
                }
            })
            .collect(),
        shared_extension: String::new(),
    }
}

fn bead_status_rank(status: &str) -> u8 {
    BEAD_STATUS_RANK
        .iter()
        .find(|(name, _)| *name == status)
        .map(|(_, rank)| *rank)
        .unwrap_or(u8::MAX)
}

fn bead_detail(entry: &BeadCompletionEntry) -> String {
    let mut parts = Vec::new();
    if !entry.status.is_empty() {
        parts.push(entry.status.as_str());
    }
    if !entry.type_label.is_empty() {
        parts.push(entry.type_label.as_str());
    }
    if !entry.task_type.is_empty() {
        parts.push(entry.task_type.as_str());
    }
    parts.join(" · ")
}

fn bead_documentation(entry: &BeadCompletionEntry) -> String {
    let mut lines = Vec::new();
    if !entry.title.is_empty() {
        lines.push(entry.title.clone());
    }
    let detail = bead_detail(entry);
    if !detail.is_empty() {
        lines.push(detail);
    }
    if !entry.project.is_empty() {
        lines.push(format!("project: {}", entry.project));
    }
    lines.join("\n\n")
}

fn argument_candidate(value: &str, doc: &str) -> CompletionCandidate {
    CompletionCandidate {
        display: value.to_string(),
        insertion: value.to_string(),
        detail: None,
        documentation: Some(doc.to_string()),
        is_dir: false,
        name: value.to_string(),
        replacement: None,
        additional_edits: Vec::new(),
        kind: String::new(),
        project: String::new(),
        status: String::new(),
    }
}

fn keyword_candidate(
    insertion: &str,
    documentation: &str,
    replacement_range: Option<EditorRange>,
) -> CompletionCandidate {
    CompletionCandidate {
        display: insertion.to_string(),
        insertion: insertion.to_string(),
        detail: None,
        documentation: Some(documentation.to_string()),
        is_dir: false,
        name: insertion.to_string(),
        replacement: replacement_range.map(|range| EditorTextEdit {
            range,
            new_text: insertion.to_string(),
        }),
        additional_edits: Vec::new(),
        kind: "keyword".to_string(),
        project: String::new(),
        status: String::new(),
    }
}

fn selected_keyword_set(selected: &[String]) -> Vec<String> {
    selected
        .iter()
        .map(|value| {
            value
                .split_once('=')
                .map(|(name, _)| name.trim())
                .unwrap_or(value.as_str())
                .to_lowercase()
        })
        .collect()
}

fn keyword_is_available(
    keyword: &DirectiveKeywordSpec,
    selected: &[String],
) -> bool {
    let name = keyword.name.to_lowercase();
    if selected.iter().any(|value| value == &name) && !keyword.repeatable {
        return false;
    }
    !keyword.conflicts_with.iter().any(|conflict| {
        selected
            .iter()
            .any(|value| value == &conflict.to_lowercase())
    })
}

pub fn is_directive_like_token(token: &str) -> bool {
    token.starts_with('%') || token == "%(" || token == "%{"
}

pub fn detect_directive_context_at_position(
    document: &DocumentSnapshot,
    position: EditorPosition,
) -> Option<CompletionContext> {
    let cursor = document.position_to_byte_offset(position)?;
    let line = document.line_text(position.line)?;
    let line_start = document.position_to_byte_offset(EditorPosition {
        line: position.line,
        character: 0,
    })?;
    let cursor_in_line = cursor.checked_sub(line_start)?;
    let before = line.get(..cursor_in_line)?;

    if let Some((start, token)) = directive_name_token(before) {
        let byte_start = line_start + start;
        let range = document.byte_range_to_range(byte_start, cursor)?;
        return Some(CompletionContext {
            kind: CompletionContextKind::DirectiveName,
            token: Some(TokenInfo {
                text: token.to_string(),
                range,
                byte_start,
                byte_end: cursor,
            }),
            active_xprompt: None,
            active_input: None,
            directive_name: None,
            selected_values: Vec::new(),
            directive: None,
            vcs_repo: None,
            vcs_ref: None,
            artifact_ref: None,
            replacement_range: range,
        });
    }

    let target = directive_arg_context(line, cursor_in_line)?;
    let mut directive_name = target.directive_name;
    let mut arg_start = target.arg_start;
    if directive_name == "model" {
        match before
            .get(target.arg_start..)
            .and_then(|text| text.rfind('@'))
        {
            Some(rel_at) if rel_at > 0 => {
                directive_name = "effort";
                arg_start = target.arg_start + rel_at + 1;
            }
            _ => {}
        }
    }
    let byte_start = line_start + arg_start;
    let byte_end = line_start + target.arg_end;
    let range = document.byte_range_to_range(byte_start, byte_end)?;
    let token_range = document.byte_range_to_range(byte_start, cursor)?;
    let mut clause = target.clause;
    if directive_name == "effort" && target.directive_name == "model" {
        clause.value_role = directive_metadata("effort")
            .and_then(|metadata| metadata.positional_role);
        clause.clause_kind = DirectiveClauseKind::Positional;
        clause.active_keyword = None;
    }
    Some(CompletionContext {
        kind: target.kind,
        token: Some(TokenInfo {
            text: before.get(arg_start..).unwrap_or_default().to_string(),
            range: token_range,
            byte_start,
            byte_end: cursor,
        }),
        active_xprompt: None,
        active_input: None,
        directive_name: Some(directive_name.to_string()),
        selected_values: target.selected_values,
        directive: Some(clause),
        vcs_repo: None,
        vcs_ref: None,
        artifact_ref: None,
        replacement_range: range,
    })
}

fn directive_name_token(before: &str) -> Option<(usize, &str)> {
    let start = before.rfind('%')?;
    let token = &before[start..];
    if token == "%("
        || token == "%{"
        || token[1..]
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        return Some((start, token));
    }
    None
}

struct DirectiveArgCompletionTarget {
    directive_name: &'static str,
    arg_start: usize,
    arg_end: usize,
    kind: CompletionContextKind,
    selected_values: Vec<String>,
    clause: DirectiveClauseContext,
}

fn directive_arg_context(
    line: &str,
    cursor: usize,
) -> Option<DirectiveArgCompletionTarget> {
    let before = line.get(..cursor)?;
    let percent = before.rfind('%')?;
    let rest = &before[percent + 1..];
    let split = rest.find([':', '(', '{'])?;
    let name = &rest[..split];
    let canonical = canonical_directive_name(name)?;
    let metadata = directive_metadata(canonical)?;
    let sep = rest.as_bytes().get(split).copied()?;
    let syntax_form = match sep {
        b':' => DirectiveSyntaxForm::Colon,
        b'(' => DirectiveSyntaxForm::Parenthesized,
        b'{' => DirectiveSyntaxForm::BraceShorthand,
        _ => return None,
    };
    let open_idx = percent + 1 + split;
    if syntax_form == DirectiveSyntaxForm::Parenthesized {
        return parenthesized_arg_context(line, cursor, metadata, open_idx);
    }
    colon_arg_context(line, cursor, metadata, syntax_form, open_idx)
}

fn colon_arg_context(
    line: &str,
    cursor: usize,
    metadata: &'static DirectiveMetadata,
    syntax_form: DirectiveSyntaxForm,
    colon_idx: usize,
) -> Option<DirectiveArgCompletionTarget> {
    let body_start = colon_idx + 1;
    if cursor < body_start {
        return None;
    }
    let body_end = if metadata.name == "wait" {
        line.len()
    } else {
        line[cursor..]
            .find(char::is_whitespace)
            .map(|offset| cursor + offset)
            .unwrap_or(line.len())
    };
    if metadata.name == "wait" {
        return comma_clause_context(
            line,
            cursor,
            metadata,
            syntax_form,
            body_start,
            body_end,
            false,
        );
    }
    Some(DirectiveArgCompletionTarget {
        directive_name: metadata.name,
        arg_start: body_start,
        arg_end: body_end.max(cursor),
        kind: CompletionContextKind::DirectiveArgument,
        selected_values: Vec::new(),
        clause: DirectiveClauseContext {
            syntax_form,
            clause_kind: DirectiveClauseKind::Positional,
            active_keyword: None,
            value_role: metadata.positional_role,
            selected_keywords: Vec::new(),
            clause_range: None,
        },
    })
}

fn parenthesized_arg_context(
    line: &str,
    cursor: usize,
    metadata: &'static DirectiveMetadata,
    open_idx: usize,
) -> Option<DirectiveArgCompletionTarget> {
    if line.as_bytes().get(open_idx) != Some(&b'(') {
        return None;
    }
    let close = find_matching_paren_quoted(line, open_idx);
    if close.is_some_and(|close| cursor > close) {
        return None;
    }
    let body_start = open_idx + 1;
    let body_end = close.unwrap_or(line.len());
    comma_clause_context(
        line,
        cursor,
        metadata,
        DirectiveSyntaxForm::Parenthesized,
        body_start,
        body_end,
        true,
    )
}

#[allow(clippy::too_many_arguments)]
fn comma_clause_context(
    line: &str,
    cursor: usize,
    metadata: &'static DirectiveMetadata,
    syntax_form: DirectiveSyntaxForm,
    body_start: usize,
    body_end: usize,
    keywords_allowed: bool,
) -> Option<DirectiveArgCompletionTarget> {
    if cursor < body_start || cursor > body_end {
        return None;
    }
    let body = line.get(body_start..body_end)?;
    let cursor_in_body = cursor - body_start;
    let clauses = split_top_level_clauses(body);
    let clause_index = clauses
        .iter()
        .position(|(start, end)| {
            cursor_in_body >= *start && cursor_in_body <= *end
        })
        .or_else(|| clauses.len().checked_sub(1))?;
    let (clause_start, clause_end) = clauses[clause_index];
    let clause = &body[clause_start..clause_end];
    let trimmed = clause.trim_start();
    let leading = clause.len() - trimmed.len();
    let trailing = clause.len() - clause.trim_end().len();
    let content_start = body_start + clause_start + leading;
    let content_end = body_start + clause_end - trailing;

    let mut selected_values = Vec::new();
    let mut selected_keywords = Vec::new();
    for (index, (start, end)) in clauses.iter().enumerate() {
        if index == clause_index {
            continue;
        }
        let other = body[*start..*end].trim();
        if other.is_empty() {
            continue;
        }
        selected_values.push(other.to_string());
        if let Some((name, _)) = split_keyword_clause(other) {
            selected_keywords.push(name.to_string());
        }
    }

    let keywords_in_form =
        keywords_allowed && directive_allows_keywords(metadata, syntax_form);
    let (kind, clause_kind, active_keyword, value_role, arg_start) =
        classify_active_clause(
            metadata,
            syntax_form,
            trimmed,
            keywords_in_form,
            clause_index,
            content_start,
        );

    Some(DirectiveArgCompletionTarget {
        directive_name: metadata.name,
        arg_start: arg_start.min(content_end.max(content_start)),
        arg_end: content_end.max(arg_start),
        kind,
        selected_values,
        clause: DirectiveClauseContext {
            syntax_form,
            clause_kind,
            active_keyword,
            value_role,
            selected_keywords,
            clause_range: None,
        },
    })
}

fn classify_active_clause(
    metadata: &'static DirectiveMetadata,
    syntax_form: DirectiveSyntaxForm,
    trimmed: &str,
    keywords_in_form: bool,
    clause_index: usize,
    content_start: usize,
) -> (
    CompletionContextKind,
    DirectiveClauseKind,
    Option<String>,
    Option<DirectiveValueRole>,
    usize,
) {
    if keywords_in_form {
        if let Some((name, value)) = split_keyword_clause(trimmed) {
            let keyword = metadata
                .keywords
                .iter()
                .find(|keyword| keyword.name == name);
            let value_leading = value.len() - value.trim_start().len();
            let name_len = trimmed.find('=').unwrap_or(name.len());
            let arg_start = content_start + name_len + 1 + value_leading;
            let value_role = keyword.map(|keyword| keyword.value_role).or({
                if metadata.dynamic_keyword_role
                    == Some(DirectiveValueRole::ModelAliasKey)
                {
                    Some(DirectiveValueRole::Model)
                } else {
                    None
                }
            });
            return (
                CompletionContextKind::DirectiveArgumentValue,
                DirectiveClauseKind::KeywordValue,
                Some(name.to_string()),
                value_role,
                arg_start,
            );
        }
        if metadata.name != "wait" && clause_index > 0 {
            return (
                CompletionContextKind::DirectiveArgumentKeyword,
                DirectiveClauseKind::KeywordName,
                None,
                metadata.dynamic_keyword_role,
                content_start,
            );
        }
    }
    let value_role = if syntax_form == DirectiveSyntaxForm::Parenthesized
        && metadata.name == "model"
        && clause_index == 0
    {
        Some(DirectiveValueRole::Model)
    } else {
        metadata.positional_role
    };
    (
        CompletionContextKind::DirectiveArgument,
        DirectiveClauseKind::Positional,
        None,
        value_role,
        content_start,
    )
}

fn split_keyword_clause(clause: &str) -> Option<(&str, &str)> {
    let equals = find_top_level_equals(clause)?;
    let name = clause[..equals].trim();
    if name.is_empty()
        || !name
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        return None;
    }
    Some((name, &clause[equals + 1..]))
}

fn split_top_level_clauses(body: &str) -> Vec<(usize, usize)> {
    let mut clauses = Vec::new();
    let mut start = 0usize;
    let bytes = body.as_bytes();
    let mut index = 0usize;
    let mut state = QuoteState::default();
    while index < bytes.len() {
        let consumed = state.consume(bytes, index);
        if !state.in_quotes() && !state.in_text_block && bytes[index] == b',' {
            clauses.push((start, index));
            start = index + 1;
        }
        index += consumed;
    }
    clauses.push((start, body.len()));
    clauses
}

fn find_top_level_equals(text: &str) -> Option<usize> {
    let bytes = text.as_bytes();
    let mut index = 0usize;
    let mut state = QuoteState::default();
    while index < bytes.len() {
        let consumed = state.consume(bytes, index);
        if !state.in_quotes() && !state.in_text_block && bytes[index] == b'=' {
            return Some(index);
        }
        index += consumed;
    }
    None
}

fn find_matching_paren_quoted(text: &str, open: usize) -> Option<usize> {
    if text.as_bytes().get(open) != Some(&b'(') {
        return None;
    }
    let bytes = text.as_bytes();
    let mut depth = 1usize;
    let mut index = open + 1;
    let mut state = QuoteState::default();
    while index < bytes.len() {
        let consumed = state.consume(bytes, index);
        if !state.in_quotes() && !state.in_text_block {
            match bytes[index] {
                b'(' => depth += 1,
                b')' => {
                    depth -= 1;
                    if depth == 0 {
                        return Some(index);
                    }
                }
                _ => {}
            }
        }
        index += consumed;
    }
    None
}

#[derive(Default)]
struct QuoteState {
    quote: Option<u8>,
    in_text_block: bool,
}

impl QuoteState {
    fn in_quotes(&self) -> bool {
        self.quote.is_some()
    }

    fn consume(&mut self, bytes: &[u8], index: usize) -> usize {
        if self.in_text_block {
            if bytes.get(index..index + 2) == Some(b"]]") {
                self.in_text_block = false;
                return 2;
            }
            return 1;
        }
        if self.quote.is_none() && bytes.get(index..index + 2) == Some(b"[[") {
            self.in_text_block = true;
            return 2;
        }
        let byte = bytes[index];
        match self.quote {
            None if byte == b'"' || byte == b'\'' || byte == b'`' => {
                self.quote = Some(byte);
                1
            }
            Some(quote) if byte == quote => {
                self.quote = None;
                1
            }
            _ => 1,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::effort::EFFORT_LEVELS_ORDERED;

    #[test]
    fn resolves_documented_aliases() {
        for (alias, canonical) in [
            ("m", "model"),
            ("e", "effort"),
            ("i", "id"),
            ("c", "clan"),
            ("w", "wait"),
            ("a", "auto"),
            ("(", "alt"),
            ("{", "alt"),
        ] {
            assert_eq!(canonical_directive_name(alias), Some(canonical));
        }
        assert!(directive_metadata("xprompts_enabled").is_some());
        assert_eq!(canonical_directive_name("p"), None);
        assert_eq!(canonical_directive_name("time"), None);
        assert_eq!(canonical_directive_name("approve"), None);
        // `%edit` was removed and is not an alias; `%e` now resolves to `effort`.
        assert_eq!(canonical_directive_name("edit"), None);
        assert_eq!(canonical_directive_name("e"), Some("effort"));
        assert_eq!(canonical_directive_name("name"), None);
        assert_eq!(canonical_directive_name("n"), None);
        assert_eq!(canonical_directive_name("tribe"), None);
        assert_eq!(canonical_directive_name("t"), None);

        let model = directive_metadata("model").expect("model metadata");
        assert!(!model.allows_multiple);
    }

    #[test]
    fn contract_covers_the_audited_directive_matrix() {
        let contract = directive_contract();
        let names: Vec<&str> =
            contract.iter().map(|entry| entry.name.as_str()).collect();
        assert_eq!(
            names,
            [
                "model",
                "effort",
                "final",
                "id",
                "clan",
                "wait",
                "auto",
                "hide",
                "repeat",
                "alt",
                "xprompts_enabled",
            ]
        );

        let wait = contract
            .iter()
            .find(|entry| entry.name == "wait")
            .expect("wait contract");
        assert_eq!(
            wait.keywords
                .iter()
                .map(|keyword| keyword.name.as_str())
                .collect::<Vec<_>>(),
            ["bead", "priority", "runners", "time"]
        );
        assert!(wait
            .syntax_forms
            .contains(&DirectiveSyntaxForm::Parenthesized));
        assert!(wait.syntax_forms.contains(&DirectiveSyntaxForm::Colon));
        assert_eq!(wait.positional_role, Some(DirectiveValueRole::Agent));
        assert_eq!(wait.keywords[0].value_role, DirectiveValueRole::Bead);

        let id = contract
            .iter()
            .find(|entry| entry.name == "id")
            .expect("id contract");
        assert_eq!(
            id.keywords
                .iter()
                .map(|keyword| (
                    keyword.name.as_str(),
                    keyword.conflicts_with.clone()
                ))
                .collect::<Vec<_>>(),
            [
                ("bead", Vec::new()),
                ("clan", vec!["family".to_string(), "tribe".to_string()]),
                ("family", vec!["clan".to_string(), "tribe".to_string()]),
                ("tribe", vec!["clan".to_string(), "family".to_string()]),
            ]
        );

        let clan = contract
            .iter()
            .find(|entry| entry.name == "clan")
            .expect("clan contract");
        assert_eq!(
            clan.keywords
                .iter()
                .map(|keyword| keyword.name.as_str())
                .collect::<Vec<_>>(),
            ["summary", "summary_script", "tribe"]
        );
        assert_eq!(
            clan.keywords[0].conflicts_with,
            vec!["summary_script".to_string()]
        );

        let enabled = contract
            .iter()
            .find(|entry| entry.name == "xprompts_enabled")
            .expect("xprompts_enabled contract");
        assert_eq!(enabled.syntax_forms, vec![DirectiveSyntaxForm::Colon]);
        assert_eq!(
            enabled
                .positional_suggestions
                .iter()
                .map(|value| value.value.as_str())
                .collect::<Vec<_>>(),
            ["false", "true"]
        );

        let model = contract
            .iter()
            .find(|entry| entry.name == "model")
            .expect("model contract");
        assert_eq!(
            model.dynamic_keyword_role,
            Some(DirectiveValueRole::ModelAliasKey)
        );
        assert_eq!(model.positional_role, Some(DirectiveValueRole::Model));

        let final_directive = contract
            .iter()
            .find(|entry| entry.name == "final")
            .expect("final contract");
        assert!(final_directive.allows_multiple);
        assert_eq!(
            final_directive.syntax_forms,
            vec![
                DirectiveSyntaxForm::Colon,
                DirectiveSyntaxForm::Parenthesized
            ]
        );
        assert_eq!(
            final_directive.positional_role,
            Some(DirectiveValueRole::FinalizerInstance)
        );
        assert_eq!(
            final_directive
                .positional_suggestions
                .iter()
                .map(|value| value.value.as_str())
                .collect::<Vec<_>>(),
            ["none"]
        );
    }

    #[test]
    fn id_metadata_and_completion_match_the_editor_contract() {
        let metadata = directive_metadata("id").expect("id metadata");
        assert_eq!(metadata.alias, Some("i"));
        assert!(metadata.takes_argument);
        assert!(!metadata.allows_multiple);
        assert_eq!(
            metadata.description,
            "Assign an agent ID with optional bead, clan, family, or user-managed tribe"
        );
        assert_eq!(canonical_directive_name("i"), Some("id"));
        assert_eq!(directive_metadata("i").map(|d| d.name), Some("id"));

        for token in ["%id", "%i"] {
            let completions = build_directive_completion_candidates(token);
            assert_eq!(completions.candidates.len(), 1, "{token} completion");
            let candidate = &completions.candidates[0];
            assert_eq!(candidate.insertion, "%id");
            assert_eq!(candidate.detail.as_deref(), Some("alias %i"));
            assert_eq!(
                candidate.documentation.as_deref(),
                Some(metadata.description)
            );
        }

        let id_args = directive_argument_candidates("id").candidates;
        assert_eq!(id_args.len(), 4);
        assert_eq!(
            id_args
                .iter()
                .map(|candidate| candidate.insertion.as_str())
                .collect::<Vec<_>>(),
            ["bead=", "clan=", "family=", "tribe="]
        );
        assert_eq!(
            id_args
                .iter()
                .map(|candidate| candidate.documentation.as_deref().unwrap())
                .collect::<Vec<_>>(),
            [
                "Associate this launch with a bead",
                "Derive the full ID and join this agent clan",
                "Attach this suffix to an existing agent family",
                "Assign this agent to a user-managed tribe",
            ]
        );
        assert_eq!(directive_argument_candidates("i").candidates, id_args);

        for removed in ["name", "n"] {
            assert_eq!(canonical_directive_name(removed), None);
            assert!(directive_metadata(removed).is_none());
            assert!(build_directive_completion_candidates(&format!(
                "%{removed}"
            ))
            .candidates
            .is_empty());
        }
    }

    #[test]
    fn clan_metadata_matches_the_editor_contract() {
        let metadata = directive_metadata("clan").expect("directive metadata");
        assert_eq!(metadata.alias, Some("c"));
        assert!(metadata.takes_argument);
        assert!(!metadata.allows_multiple);
        assert_eq!(canonical_directive_name("c"), Some("clan"));
        assert_eq!(directive_metadata("c").map(|d| d.name), Some("clan"));
        assert_eq!(metadata.description, "Declare a new parallel agent clan");

        for token in ["%cl", "%c"] {
            let completions = build_directive_completion_candidates(token);
            assert_eq!(completions.candidates.len(), 1, "{token} completion");
            let candidate = &completions.candidates[0];
            assert_eq!(candidate.insertion, "%clan");
            assert_eq!(candidate.detail.as_deref(), Some("alias %c"));
            assert_eq!(
                candidate.documentation.as_deref(),
                Some(metadata.description)
            );
        }

        let clan_args = directive_argument_candidates("clan").candidates;
        assert_eq!(
            clan_args
                .iter()
                .map(|candidate| candidate.insertion.as_str())
                .collect::<Vec<_>>(),
            ["summary=", "summary_script=", "tribe="]
        );
        assert_eq!(
            clan_args[0].documentation.as_deref(),
            Some("Attach a Rich-markup summary to this clan")
        );
        assert_eq!(
            clan_args[1].documentation.as_deref(),
            Some("Generate this clan's summary with an executable script")
        );
        assert_eq!(directive_argument_candidates("c").candidates, clan_args);
        assert!(directive_argument_candidates("tribe").candidates.is_empty());
    }

    #[test]
    fn removed_identity_directives_do_not_resolve_or_complete() {
        assert_eq!(canonical_directive_name("f"), None);
        assert!(directive_metadata("f").is_none());

        for name in ["family", "group", "g", "tribe", "t"] {
            assert_eq!(canonical_directive_name(name), None, "{name}");
            assert!(directive_metadata(name).is_none(), "{name}");
            assert!(
                build_directive_completion_candidates(&format!("%{name}"))
                    .candidates
                    .is_empty(),
                "{name}"
            );
        }
    }

    #[test]
    fn alt_metadata_advertises_brace_shorthand() {
        let alt = directive_metadata("alt").expect("alt metadata");
        // The legacy `(` alias is no longer advertised, but stays
        // parse-compatible through `canonical_directive_name`.
        assert_eq!(alt.alias, None);
        assert_eq!(canonical_directive_name("("), Some("alt"));
        assert!(
            alt.description.contains("%{"),
            "alt description should describe the brace shorthand: {}",
            alt.description
        );

        // Completing `%alt` surfaces the directive without an `alias %(` detail.
        let completions = build_directive_completion_candidates("%alt");
        let alt_candidate = completions
            .candidates
            .iter()
            .find(|candidate| candidate.name == "alt")
            .expect("alt completion candidate");
        assert_eq!(alt_candidate.detail, None);
    }

    #[test]
    fn auto_metadata_describes_gate_owned_resolution_and_offers_compatibility_suggestions(
    ) {
        let auto = directive_metadata("auto").expect("auto metadata");
        assert_eq!(auto.alias, Some("a"));
        assert!(auto.takes_argument);
        assert!(
            auto.description.contains("gate kind"),
            "auto description should assign validation to the gate kind: {}",
            auto.description
        );

        // These insertions stay aligned with Python's
        // AUTO_COMPATIBILITY_ARGUMENT_SUGGESTIONS. They are suggestions, not a
        // universal runtime allowlist.
        let candidates = directive_argument_candidates("auto").candidates;
        let values: Vec<&str> =
            candidates.iter().map(|c| c.insertion.as_str()).collect();
        assert_eq!(values, ["plan", "tale", "epic"]);
    }

    #[test]
    fn final_directive_is_parseable_but_not_advertised_by_name_completion() {
        assert_eq!(canonical_directive_name("final"), Some("final"));
        assert!(directive_metadata("final").is_some());
        assert!(build_directive_completion_candidates("%f")
            .candidates
            .is_empty());
        assert!(build_directive_completion_candidates("%final")
            .candidates
            .is_empty());
    }

    #[test]
    fn directive_completion_t_prefix_is_empty() {
        let t_completions = build_directive_completion_candidates("%t");
        assert!(t_completions.candidates.is_empty());

        for token in ["%ta", "%ti", "%time"] {
            assert!(
                build_directive_completion_candidates(token)
                    .candidates
                    .is_empty(),
                "{token} should not complete"
            );
        }
    }

    #[test]
    fn removed_auto_approve_aliases_do_not_resolve_or_complete() {
        assert_eq!(canonical_directive_name("approve"), None);
        assert_eq!(canonical_directive_name("p"), None);
        assert_eq!(canonical_directive_name("time"), None);

        assert!(build_directive_completion_candidates("%approve")
            .candidates
            .is_empty());
        assert!(build_directive_completion_candidates("%p")
            .candidates
            .is_empty());
        assert!(build_directive_completion_candidates("%ta")
            .candidates
            .is_empty());
        let a_completions = build_directive_completion_candidates("%a");
        let a_names: Vec<&str> = a_completions
            .candidates
            .iter()
            .map(|candidate| candidate.name.as_str())
            .collect();
        assert_eq!(a_names, ["alt", "auto"]);
    }

    #[test]
    fn effort_is_a_recognized_directive_with_e_alias() {
        let effort = directive_metadata("effort").expect("effort metadata");
        assert_eq!(effort.name, "effort");
        assert_eq!(effort.alias, Some("e"));
        assert!(effort.takes_argument);
        assert!(!effort.allows_multiple);
        // `%e` is the advertised `%effort` alias and canonicalizes to `effort`.
        assert_eq!(canonical_directive_name("e"), Some("effort"));
        assert_eq!(directive_metadata("e").map(|d| d.name), Some("effort"));

        for token in ["%e", "%eff"] {
            let completions = build_directive_completion_candidates(token);
            assert_eq!(completions.candidates.len(), 1, "{token} completion");
            assert_eq!(completions.candidates[0].insertion, "%effort");
            // The `%effort` candidate advertises its `%e` alias detail.
            assert_eq!(
                completions.candidates[0].detail.as_deref(),
                Some("alias %e"),
                "{token} alias detail"
            );
        }
    }

    #[test]
    fn effort_argument_candidates_are_the_canonical_vocabulary() {
        let candidates = directive_argument_candidates("effort").candidates;
        let levels: Vec<&str> =
            candidates.iter().map(|c| c.insertion.as_str()).collect();
        assert_eq!(levels, EFFORT_LEVELS_ORDERED);
    }

    #[test]
    fn wait_argument_candidates_use_runtime_keywords() {
        let candidates = directive_argument_candidates("wait").candidates;
        let values: Vec<&str> =
            candidates.iter().map(|c| c.insertion.as_str()).collect();
        assert_eq!(values, ["bead=", "priority=", "runners=", "time="]);
        assert!(directive_argument_candidates("time").candidates.is_empty());
    }

    #[test]
    fn keyword_candidates_suppress_selected_and_conflicting_names() {
        let id = directive_metadata("id").expect("id metadata");
        let available = build_directive_keyword_candidates(
            id,
            "",
            &["clan=research".to_string()],
            None,
        );
        let names: Vec<&str> = available
            .candidates
            .iter()
            .map(|candidate| candidate.insertion.as_str())
            .collect();
        assert_eq!(names, ["bead="]);

        let clan = directive_metadata("clan").expect("clan metadata");
        let remaining = build_directive_keyword_candidates(
            clan,
            "su",
            &["summary".to_string()],
            None,
        );
        assert!(remaining.candidates.is_empty());
    }

    #[test]
    fn bead_ranking_matches_wait_modal_order_and_filters() {
        let entries = vec![
            BeadCompletionEntry {
                id: "sase-b".to_string(),
                title: "Later open".to_string(),
                status: "open".to_string(),
                updated_at: "2026-08-20T12:00:00Z".to_string(),
                ..BeadCompletionEntry::default()
            },
            BeadCompletionEntry {
                id: "sase-a".to_string(),
                title: "Active bug".to_string(),
                status: "in_progress".to_string(),
                updated_at: "2026-08-19T12:00:00Z".to_string(),
                type_label: "task".to_string(),
                task_type: "bug".to_string(),
                project: "sase".to_string(),
                created_at: "2026-08-01T00:00:00Z".to_string(),
            },
            BeadCompletionEntry {
                id: "sase-c".to_string(),
                title: "Ready work".to_string(),
                status: "ready".to_string(),
                updated_at: "2026-08-21T12:00:00Z".to_string(),
                type_label: "task".to_string(),
                created_at: String::new(),
                task_type: String::new(),
                project: String::new(),
            },
        ];
        let ranked = rank_and_filter_bead_entries(&entries, "", &[], &[], 10);
        assert_eq!(
            ranked
                .iter()
                .map(|entry| entry.id.as_str())
                .collect::<Vec<_>>(),
            ["sase-a", "sase-c", "sase-b"]
        );

        let filtered =
            rank_and_filter_bead_entries(&entries, "bug", &[], &[], 10);
        assert_eq!(
            filtered
                .iter()
                .map(|entry| entry.id.as_str())
                .collect::<Vec<_>>(),
            ["sase-a"]
        );

        let excluded = rank_and_filter_bead_entries(
            &entries,
            "",
            &["sase-c".to_string()],
            &["sase-a".to_string()],
            10,
        );
        assert_eq!(
            excluded
                .iter()
                .map(|entry| entry.id.as_str())
                .collect::<Vec<_>>(),
            ["sase-b"]
        );
    }

    fn pos(character: u32) -> EditorPosition {
        EditorPosition { line: 0, character }
    }

    fn classify(text: &str, character: u32) -> CompletionContext {
        let document = DocumentSnapshot::new(text);
        detect_directive_context_at_position(&document, pos(character))
            .unwrap_or_else(|| panic!("expected directive context for {text}"))
    }

    #[test]
    fn wait_paren_keywords_are_not_offered_in_colon_form() {
        let colon = classify("%wait:t", 7);
        assert_eq!(colon.kind, CompletionContextKind::DirectiveArgument);
        assert_eq!(colon.syntax_form(), Some(DirectiveSyntaxForm::Colon));
        assert_eq!(colon.clause_kind(), Some(DirectiveClauseKind::Positional));
        assert!(!directive_allows_keywords(
            directive_metadata("wait").unwrap(),
            colon.syntax_form().unwrap()
        ));

        let paren = classify("%wait(t", 7);
        assert_eq!(paren.kind, CompletionContextKind::DirectiveArgument);
        assert_eq!(
            paren.syntax_form(),
            Some(DirectiveSyntaxForm::Parenthesized)
        );
        assert!(directive_allows_keywords(
            directive_metadata("wait").unwrap(),
            paren.syntax_form().unwrap()
        ));
    }

    #[test]
    fn wait_bead_value_is_a_keyword_value_clause() {
        let context = classify("%wait(bead=", 11);
        assert_eq!(context.kind, CompletionContextKind::DirectiveArgumentValue);
        assert_eq!(context.active_keyword(), Some("bead"));
        assert_eq!(context.value_role(), Some(DirectiveValueRole::Bead));
        assert_eq!(
            context.clause_kind(),
            Some(DirectiveClauseKind::KeywordValue)
        );
    }

    #[test]
    fn id_and_clan_keyword_values_and_conflicts_classify() {
        let value = classify("%id(worker, clan=re", 19);
        assert_eq!(value.kind, CompletionContextKind::DirectiveArgumentValue);
        assert_eq!(value.active_keyword(), Some("clan"));
        assert_eq!(value.value_role(), Some(DirectiveValueRole::Clan));
        assert_eq!(value.selected_values, vec!["worker"]);

        let first_keyword = classify("%id(tribe=", 10);
        assert_eq!(
            first_keyword.kind,
            CompletionContextKind::DirectiveArgumentValue
        );
        assert_eq!(first_keyword.active_keyword(), Some("tribe"));

        let clan_keyword = classify("%clan(research, su", 18);
        assert_eq!(
            clan_keyword.kind,
            CompletionContextKind::DirectiveArgumentKeyword
        );
        let suppressed = build_directive_keyword_candidates(
            directive_metadata("clan").unwrap(),
            "su",
            clan_keyword.selected_keywords(),
            None,
        );
        assert_eq!(
            suppressed
                .candidates
                .iter()
                .map(|candidate| candidate.insertion.as_str())
                .collect::<Vec<_>>(),
            ["summary=", "summary_script="]
        );
    }

    #[test]
    fn quoted_and_text_block_commas_do_not_split_clauses() {
        let quoted = classify("%clan(research, summary=\"a, b\", tr", 34);
        assert_eq!(
            quoted.kind,
            CompletionContextKind::DirectiveArgumentKeyword
        );
        assert_eq!(
            quoted.selected_keywords(),
            ["summary".to_string()].as_slice()
        );
        assert_eq!(quoted.token.as_ref().unwrap().text, "tr");

        let block =
            classify("%clan(research, summary=[[hello, world]], tr", 44);
        assert_eq!(block.kind, CompletionContextKind::DirectiveArgumentKeyword);
        assert_eq!(
            block.selected_keywords(),
            ["summary".to_string()].as_slice()
        );
    }

    #[test]
    fn utf16_positions_classify_the_active_wait_clause() {
        let text = "%wait(café, be";
        let document = DocumentSnapshot::new(text);
        let cursor = document
            .byte_offset_to_position(text.len())
            .expect("utf-16 cursor");
        assert_eq!(cursor.character, 14);
        let context = detect_directive_context_at_position(&document, cursor)
            .expect("wait unicode context");
        assert_eq!(context.directive_name.as_deref(), Some("wait"));
        assert_eq!(context.token.as_ref().unwrap().text, "be");
        assert_eq!(context.selected_values, vec!["café"]);
        assert_eq!(
            context.syntax_form(),
            Some(DirectiveSyntaxForm::Parenthesized)
        );
    }

    #[test]
    fn incomplete_and_malformed_calls_still_classify() {
        let empty = classify("%wait(", 6);
        assert_eq!(empty.kind, CompletionContextKind::DirectiveArgument);
        assert_eq!(empty.token.as_ref().unwrap().text, "");

        let trailing = classify("%id(worker, ", 12);
        assert_eq!(
            trailing.kind,
            CompletionContextKind::DirectiveArgumentKeyword
        );
        assert_eq!(trailing.selected_values, vec!["worker"]);

        let unclosed = classify("%clan(research, summary=\"hello", 30);
        assert_eq!(
            unclosed.kind,
            CompletionContextKind::DirectiveArgumentValue
        );
        assert_eq!(unclosed.active_keyword(), Some("summary"));
        assert_eq!(unclosed.token.as_ref().unwrap().text, "\"hello");
    }

    #[test]
    fn clause_candidates_cover_roles_conflicts_and_self_references() {
        use super::super::completion::build_directive_clause_candidates;
        use super::super::wire::{
            AgentCompletionEntry, DirectiveCompletionInventories,
            DirectiveModelAliasKey, DirectiveModelEntry,
        };

        let inventories = DirectiveCompletionInventories {
            models: vec![DirectiveModelEntry {
                value: "opus".to_string(),
                display: "opus".to_string(),
                detail: String::new(),
                documentation: "Claude".to_string(),
            }],
            model_alias_keys: vec![
                DirectiveModelAliasKey {
                    name: "coder".to_string(),
                    documentation: "Coder follow-up".to_string(),
                },
                DirectiveModelAliasKey {
                    name: "medium".to_string(),
                    documentation: "Medium alias".to_string(),
                },
            ],
            agents: vec![
                AgentCompletionEntry {
                    name: "planner".to_string(),
                    status: "RUNNING".to_string(),
                    project: "sase".to_string(),
                    kind: "agent".to_string(),
                    member_count: 1,
                    detail: String::new(),
                    documentation: String::new(),
                },
                AgentCompletionEntry {
                    name: "builders".to_string(),
                    status: "RUNNING".to_string(),
                    project: String::new(),
                    kind: "clan".to_string(),
                    member_count: 3,
                    detail: "clan · 3 members".to_string(),
                    documentation: String::new(),
                },
            ],
            beads: vec![BeadCompletionEntry {
                id: "sase-a".to_string(),
                title: "Active bug".to_string(),
                status: "in_progress".to_string(),
                type_label: "task".to_string(),
                created_at: "2026-08-01T00:00:00Z".to_string(),
                updated_at: "2026-08-20T12:00:00Z".to_string(),
                task_type: "bug".to_string(),
                project: "sase".to_string(),
            }],
            finalizers: Vec::new(),
            excluded_bead_ids: Vec::new(),
        };

        let insertions = |text: &str, character: u32| -> Vec<String> {
            let list = build_directive_clause_candidates(
                &classify(text, character),
                &inventories,
            );
            list.candidates
                .into_iter()
                .map(|candidate| candidate.insertion)
                .collect()
        };

        let at_end = |text: &str| insertions(text, text.len() as u32);
        assert_eq!(
            at_end("%wait("),
            [
                "bead=",
                "priority=",
                "runners=",
                "time=",
                "builders",
                "planner"
            ]
        );
        assert_eq!(at_end("%wait:"), ["builders", "planner"]);
        assert!(at_end("%wait:t").iter().all(|value| !value.ends_with('=')));
        assert_eq!(at_end("%wait(bead="), ["sase-a"]);
        assert_eq!(at_end("%wait(time="), ["5m", "1430"]);
        assert_eq!(at_end("%id(worker, clan="), ["builders"]);
        assert_eq!(at_end("%id(worker, clan=builders, "), ["bead="]);
        assert!(at_end("%clan(re").is_empty());
        assert_eq!(
            at_end("%clan(research, su"),
            ["summary=", "summary_script="]
        );
        assert_eq!(at_end("%clan(research, summary=hi, "), ["tribe="]);
        assert_eq!(at_end("%repeat:"), ["2", "3"]);
        assert_eq!(at_end("%xprompts_enabled:"), ["false", "true"]);
        assert_eq!(at_end("%model(opus, "), ["coder=", "medium="]);
        assert_eq!(at_end("%model(medium, c"), ["coder="]);
        assert_eq!(at_end("%model(opus, coder="), ["opus"]);
    }
}
