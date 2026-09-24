use std::collections::{BTreeMap, BTreeSet};

use crate::command_line::grammar::CommandLineGrammar;
use crate::command_line::resolve::looks_like_option;
use crate::command_line::tokenizer::lex_line;
use crate::command_line::wire::{
    CommandLineCompletionWire, CompletionItemWire, DynamicCandidateWire,
    COMMAND_LINE_WIRE_SCHEMA_VERSION,
};

struct RankedItem {
    value: String,
    display: String,
    description: String,
    badge: String,
    source: String,
    partial: bool,
    selected: bool,
    input_order: usize,
    tier: u8,
    score: i32,
    runs: Vec<Vec<u32>>,
    source_rank: u32,
}

pub fn complete_line(
    grammar: &CommandLineGrammar,
    line: &str,
    cursor: usize,
    dynamic: &[DynamicCandidateWire],
    selected: &[String],
    limit: usize,
) -> CommandLineCompletionWire {
    let char_count = line.chars().count();
    let cursor = cursor.min(char_count);
    let context =
        crate::command_line::resolve::resolve_line(grammar, line, cursor);
    let slot = context.slot;
    let kind = slot.kind.clone();
    let replace_start = slot.replace_start;
    let replace_end = slot.replace_end;
    let prefix = slot.prefix.clone();

    let selected_set: BTreeSet<String> = selected.iter().cloned().collect();

    let mut items: Vec<RankedItem> = Vec::new();
    let mut order = 0usize;

    match kind.as_str() {
        "subcommand" => {
            let node_id = lookup_node_for_complete(grammar, &context.path);
            if let Some(node_id) = node_id {
                let node = grammar.node(node_id);
                for &child_id in &node.subcommand_ids {
                    let child = grammar.node(child_id);
                    if child.hidden {
                        continue;
                    }
                    let badge = if child.subcommand_ids.is_empty() {
                        "cmd".to_string()
                    } else {
                        "group".to_string()
                    };
                    push_static(
                        &mut items,
                        &child.name,
                        &child.name,
                        &child.summary,
                        &badge,
                        &selected_set,
                        &mut order,
                    );
                    for alias in &child.aliases {
                        push_static(
                            &mut items,
                            alias,
                            alias,
                            &format!(
                                "alias of {} — {}",
                                child.name, child.summary
                            ),
                            &badge,
                            &selected_set,
                            &mut order,
                        );
                    }
                }
            }
        }
        "option_name" => {
            let node_id = lookup_node_for_complete(grammar, &context.path);
            if let Some(node_id) = node_id {
                let suppressed = suppressed_dests(
                    grammar,
                    node_id,
                    line,
                    cursor,
                    &context.path,
                );
                let node = grammar.node(node_id);
                for option in &node.options {
                    if option.hidden {
                        continue;
                    }
                    if suppressed.contains(&option.dest) {
                        continue;
                    }
                    let display = best_option_display(option, &prefix);
                    let mut description = option.summary.clone();
                    let others: Vec<String> = option
                        .strings
                        .iter()
                        .filter(|s| *s != &display)
                        .cloned()
                        .collect();
                    if !others.is_empty() {
                        description =
                            format!("{} ({})", description, others.join(", "));
                    }
                    let badge = if option.takes_value {
                        option
                            .metavar
                            .clone()
                            .unwrap_or_else(|| option.dest.to_uppercase())
                    } else {
                        "flag".to_string()
                    };
                    push_static(
                        &mut items,
                        &display,
                        &display,
                        &description,
                        &badge,
                        &selected_set,
                        &mut order,
                    );
                }
            }
        }
        "option_value" | "positional" => {
            if let Some(choices) = &slot.choices {
                for choice in choices {
                    push_static(
                        &mut items,
                        choice,
                        choice,
                        "",
                        "choice",
                        &selected_set,
                        &mut order,
                    );
                }
            }
            for candidate in dynamic {
                let display = candidate
                    .display
                    .clone()
                    .unwrap_or_else(|| candidate.value.clone());
                let description =
                    candidate.description.clone().unwrap_or_default();
                let badge = candidate.badge.clone().unwrap_or_default();
                let source = candidate.source.clone().unwrap_or_default();
                let partial = candidate.partial.unwrap_or(false);
                let sel = selected_set.contains(&candidate.value);
                items.push(RankedItem {
                    value: candidate.value.clone(),
                    display,
                    description,
                    badge,
                    source,
                    partial,
                    selected: sel,
                    input_order: order,
                    tier: 0,
                    score: 0,
                    runs: Vec::new(),
                    source_rank: 0,
                });
                order += 1;
            }
        }
        "remainder" => {
            for candidate in dynamic {
                let display = candidate
                    .display
                    .clone()
                    .unwrap_or_else(|| candidate.value.clone());
                let description =
                    candidate.description.clone().unwrap_or_default();
                let badge = candidate.badge.clone().unwrap_or_default();
                let source = candidate.source.clone().unwrap_or_default();
                let partial = candidate.partial.unwrap_or(false);
                let sel = selected_set.contains(&candidate.value);
                items.push(RankedItem {
                    value: candidate.value.clone(),
                    display,
                    description,
                    badge,
                    source,
                    partial,
                    selected: sel,
                    input_order: order,
                    tier: 0,
                    score: 0,
                    runs: Vec::new(),
                    source_rank: 0,
                });
                order += 1;
            }
        }
        _ => {}
    }

    for item in &mut items {
        item.source_rank = source_rank(&item.source);
    }

    let mut kept: Vec<RankedItem> = Vec::new();
    for mut item in items {
        if prefix.is_empty() {
            item.tier = 0;
            item.score = 0;
            item.runs = Vec::new();
            kept.push(item);
            continue;
        }
        if let Some(m) =
            crate::editor::fuzzy::fuzzy_match(&prefix, &item.display)
        {
            item.tier = m.tier;
            item.score = m.score;
            item.runs = m.runs.iter().map(|(s, e)| vec![*s, *e]).collect();
            kept.push(item);
        }
    }

    if prefix.is_empty() {
        kept.sort_by(|a, b| {
            (!a.selected)
                .cmp(&(!b.selected))
                .then_with(|| a.source_rank.cmp(&b.source_rank))
                .then_with(|| a.input_order.cmp(&b.input_order))
        });
    } else {
        kept.sort_by(|a, b| {
            (!a.selected)
                .cmp(&(!b.selected))
                .then_with(|| a.tier.cmp(&b.tier))
                .then_with(|| b.score.cmp(&a.score))
                .then_with(|| a.source_rank.cmp(&b.source_rank))
                .then_with(|| {
                    a.display.chars().count().cmp(&b.display.chars().count())
                })
                .then_with(|| {
                    a.display.to_lowercase().cmp(&b.display.to_lowercase())
                })
                .then_with(|| a.input_order.cmp(&b.input_order))
        });
    }

    let mut seen: BTreeMap<String, usize> = BTreeMap::new();
    let mut deduped: Vec<RankedItem> = Vec::new();
    for item in kept {
        if let Some(&idx) = seen.get(&item.value) {
            let sel = item.selected;
            deduped[idx].selected = deduped[idx].selected || sel;
        } else {
            seen.insert(item.value.clone(), deduped.len());
            deduped.push(item);
        }
    }

    let total = deduped.len();
    let truncated: Vec<CompletionItemWire> = deduped
        .into_iter()
        .take(limit)
        .map(|item| CompletionItemWire {
            insert_text: insert_text(&item.value, item.partial),
            display: item.display,
            description: item.description,
            badge: item.badge,
            source: item.source,
            match_runs: item.runs,
            selected: item.selected,
        })
        .collect();

    CommandLineCompletionWire {
        replace_start,
        replace_end,
        items: truncated,
        total,
        kind,
        schema_version: COMMAND_LINE_WIRE_SCHEMA_VERSION,
    }
}

fn push_static(
    items: &mut Vec<RankedItem>,
    value: &str,
    display: &str,
    description: &str,
    badge: &str,
    selected_set: &BTreeSet<String>,
    order: &mut usize,
) {
    items.push(RankedItem {
        value: value.to_string(),
        display: display.to_string(),
        description: description.to_string(),
        badge: badge.to_string(),
        source: "spec".to_string(),
        partial: false,
        selected: selected_set.contains(value),
        input_order: *order,
        tier: 0,
        score: 0,
        runs: Vec::new(),
        source_rank: 1,
    });
    *order += 1;
}

fn source_rank(source: &str) -> u32 {
    match source {
        "memory" => 0,
        "spec" => 1,
        "path" => 3,
        "" => 2,
        _ => 2,
    }
}

fn best_option_display(
    option: &crate::command_line::grammar::OptionNode,
    prefix: &str,
) -> String {
    if prefix.is_empty() || prefix.starts_with("--") {
        for s in &option.strings {
            if s.len() > 2 && s.starts_with("--") {
                return s.clone();
            }
        }
    }
    if prefix.is_empty() {
        return option.strings.first().cloned().unwrap_or_default();
    }
    let mut best: Option<(&String, u8, i32)> = None;
    for s in &option.strings {
        if let Some(m) = crate::editor::fuzzy::fuzzy_match(prefix, s) {
            let candidate = (s, m.tier, m.score);
            if best.as_ref().is_none_or(|b| {
                (candidate.1, std::cmp::Reverse(candidate.2))
                    < (b.1, std::cmp::Reverse(b.2))
            }) {
                best = Some(candidate);
            }
        }
    }
    best.map(|(s, _, _)| s.clone()).unwrap_or_else(|| {
        for s in &option.strings {
            if s.len() > 2 && s.starts_with("--") {
                return s.clone();
            }
        }
        option.strings.first().cloned().unwrap_or_default()
    })
}

fn lookup_node_for_complete(
    grammar: &CommandLineGrammar,
    path: &[String],
) -> Option<usize> {
    let mut id = grammar.root_id();
    for part in path {
        id = grammar.node(id).child_by_name.get(part).copied()?;
    }
    Some(id)
}

fn suppressed_dests(
    grammar: &CommandLineGrammar,
    node_id: usize,
    line: &str,
    cursor: usize,
    path: &[String],
) -> BTreeSet<String> {
    let raw = lex_line(line);
    let mut cursor_idx: Option<usize> = None;
    for (i, t) in raw.iter().enumerate() {
        if t.start <= cursor && cursor <= t.end {
            cursor_idx = Some(i);
            break;
        }
    }
    let context_without_cursor = if let Some(idx) = cursor_idx {
        let (cs, ce) = (raw[idx].start, raw[idx].end);
        let chars: Vec<char> = line.chars().collect();
        let mut masked = chars.clone();
        for pos in cs..ce.min(masked.len()) {
            masked[pos] = ' ';
        }
        let masked_line: String = masked.iter().collect();
        crate::command_line::resolve::resolve_line(
            grammar,
            &masked_line,
            cursor.min(cs),
        )
    } else {
        crate::command_line::resolve::resolve_line(grammar, line, cursor)
    };
    let _ = path;
    let used: BTreeSet<String> =
        context_without_cursor.used_dests.into_iter().collect();
    let node = grammar.node(node_id);
    let mut suppressed = BTreeSet::new();
    for option in &node.options {
        if !option.repeatable && used.contains(&option.dest) {
            suppressed.insert(option.dest.clone());
        }
    }
    for group in &node.mutex_groups {
        let used_members: Vec<&String> =
            group.iter().filter(|d| used.contains(*d)).collect();
        if used_members.is_empty() {
            continue;
        }
        for member in group {
            if !used.contains(member) {
                suppressed.insert(member.clone());
            }
        }
    }
    let _ = looks_like_option;
    suppressed
}

pub fn insert_text(value: &str, partial: bool) -> String {
    let mut quoted = shlex_quote(value);
    if !partial {
        quoted.push(' ');
    }
    quoted
}

pub fn shlex_quote(value: &str) -> String {
    if value.is_empty() {
        return "''".to_string();
    }
    let safe = value
        .chars()
        .all(|c| matches!(c, 'A'..='Z' | 'a'..='z' | '0'..='9' | '_' | '@' | '%' | '+' | '=' | ':' | ',' | '.' | '/' | '-' ));
    if safe {
        return value.to_string();
    }
    let mut out = String::from("'");
    for c in value.chars() {
        if c == '\'' {
            out.push_str("'\"'\"'");
        } else {
            out.push(c);
        }
    }
    out.push('\'');
    out
}
