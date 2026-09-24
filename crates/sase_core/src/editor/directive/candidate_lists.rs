use super::super::wire::{
    BeadCompletionEntry, CompletionCandidate, CompletionList,
    DirectiveKeywordSpec, DirectiveMetadata, DirectiveSuggestedValue,
    EditorRange, EditorTextEdit,
};
use super::contract::{
    directive_is_hidden_from_name_completion_with_flags, directive_metadata,
    if_directive_metadata, queue_directive_metadata,
};
use super::metadata::{BEAD_COMPLETION_LIMIT, BEAD_STATUS_RANK, DIRECTIVES};

pub fn build_directive_completion_candidates(token: &str) -> CompletionList {
    build_directive_completion_candidates_with_flags(token, &[])
}

pub fn build_directive_completion_candidates_with_flags(
    token: &str,
    enabled_feature_flags: &[String],
) -> CompletionList {
    let partial = token.strip_prefix('%').unwrap_or(token).to_lowercase();
    let mut candidates = Vec::new();
    for directive in DIRECTIVES {
        let metadata = if directive.name == "queue" {
            queue_directive_metadata(enabled_feature_flags)
        } else if directive.name == "if" {
            if_directive_metadata(enabled_feature_flags)
        } else {
            directive
        };
        if directive_is_hidden_from_name_completion_with_flags(
            metadata.name,
            enabled_feature_flags,
        ) {
            continue;
        }
        if metadata.name.starts_with(&partial)
            || metadata
                .alias
                .is_some_and(|alias| alias.starts_with(&partial))
        {
            candidates.push(CompletionCandidate {
                display: format!("%{}", metadata.name),
                insertion: format!("%{}", metadata.name),
                detail: metadata.alias.map(|alias| format!("alias %{alias}")),
                documentation: Some(metadata.description.to_string()),
                is_dir: false,
                name: metadata.name.to_string(),
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

/// Legacy `family=` stays in the directive contract (and diagnostics) so
/// older readers keep working, but completion only ever suggests `session=`.
fn keyword_is_suggested(directive: &str, keyword: &str) -> bool {
    !(directive == "id" && keyword == "family")
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
                .filter(|keyword| {
                    keyword_is_suggested(metadata.name, keyword.name)
                })
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
    build_filtered_directive_keyword_candidates(
        metadata,
        partial,
        selected_keywords,
        replacement_range,
        &[],
    )
}

pub fn build_filtered_directive_keyword_candidates(
    metadata: &DirectiveMetadata,
    partial: &str,
    selected_keywords: &[String],
    replacement_range: Option<EditorRange>,
    enabled_feature_flags: &[String],
) -> CompletionList {
    let partial = partial.to_lowercase();
    let selected = selected_keyword_set(selected_keywords);
    let mut candidates = Vec::new();
    for keyword in metadata.keywords {
        if !keyword_is_suggested(metadata.name, keyword.name) {
            continue;
        }
        if metadata.name == "wait"
            && wait_queue_keyword_retired(keyword.name, enabled_feature_flags)
        {
            continue;
        }
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

pub(super) fn mixes_positional_and_keyword_clauses(
    metadata: &DirectiveMetadata,
) -> bool {
    matches!(metadata.name, "wait" | "queue" | "hold")
}

pub(in crate::editor) fn wait_queue_keyword_retired(
    keyword: &str,
    _enabled_feature_flags: &[String],
) -> bool {
    matches!(keyword, "runners" | "capacity" | "priority" | "weight")
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
