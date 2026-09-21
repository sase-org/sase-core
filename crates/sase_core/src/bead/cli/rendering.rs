//! Search-result and dependency rendering: compact/json/full views,
//! snippets, and character-range helpers.

use std::fmt::Write as _;

use serde::Serialize;

use super::super::search::SearchMatcher;
use super::super::wire::{
    notes_text, BeadError, BeadSearchMatchWire, DependencyWire, IssueWire,
};
use super::presentation::{
    color_issue_id, color_issue_type_cell, color_status_icon,
    compact_type_width, dim_line, highlight_matches, issue_type_value,
    status_icon, status_upper, status_value, tier_value,
};
use super::resolution::find_issue;

pub(super) fn render_dependency(
    stdout: &mut String,
    issues: &[IssueWire],
    dep: &DependencyWire,
    arrow: &str,
) {
    if let Some(dep_issue) = find_issue(issues, &dep.depends_on_id) {
        writeln!(
            stdout,
            "  {arrow} {} {}: {}   [{}]",
            status_icon(&dep_issue.status),
            dep_issue.id,
            dep_issue.title,
            status_upper(&dep_issue.status)
        )
        .expect("writing to String cannot fail");
    } else {
        writeln!(stdout, "  {arrow} {} (not found)", dep.depends_on_id)
            .expect("writing to String cannot fail");
    }
}

pub(super) fn render_search_compact(
    matches: &[BeadSearchMatchWire],
    matcher: &SearchMatcher,
    color: bool,
) -> String {
    if matches.is_empty() {
        return format!("No beads match \"{}\".\n", matcher.query());
    }

    let mut stdout = String::new();
    let type_width = compact_type_width();
    for result in matches {
        let issue = &result.issue;
        writeln!(
            stdout,
            "{} {} {} · {}",
            color_issue_type_cell(&issue.issue_type, color, type_width),
            color_status_icon(&issue.status, color),
            color_issue_id(&issue.id, color),
            highlight_matches(&issue.title, matcher, color),
        )
        .expect("writing to String cannot fail");
        if let Some(snippet) = compact_snippet(result, matcher, color) {
            writeln!(stdout, "{}", dim_line(&format!("  {snippet}"), color))
                .expect("writing to String cannot fail");
        }
    }
    stdout
}

pub(super) fn render_search_json(
    matches: &[BeadSearchMatchWire],
    matcher: &SearchMatcher,
) -> Result<String, BeadError> {
    #[derive(Serialize)]
    struct SearchEnvelope<'a> {
        query: &'a str,
        regex: bool,
        count: usize,
        results: &'a [BeadSearchMatchWire],
    }

    let mut stdout = serde_json::to_string_pretty(&SearchEnvelope {
        query: matcher.query(),
        regex: matcher.is_regex(),
        count: matches.len(),
        results: matches,
    })?;
    stdout.push('\n');
    Ok(stdout)
}

fn compact_snippet(
    result: &BeadSearchMatchWire,
    matcher: &SearchMatcher,
    color: bool,
) -> Option<String> {
    let issue = &result.issue;
    let has_title_or_description_match = result
        .matched_fields
        .iter()
        .any(|field| field == "title" || field == "description");
    let description = single_line_snippet(&issue.description, matcher, 96);
    if has_title_or_description_match && !description.is_empty() {
        return Some(highlight_matches(&description, matcher, color));
    }

    result
        .matched_fields
        .iter()
        .filter(|field| field.as_str() != "title")
        .find_map(|field| {
            let value = search_field_display_value(issue, field)?;
            let snippet = single_line_snippet(&value, matcher, 96);
            (!snippet.is_empty()).then(|| {
                format!(
                    "{}: \"{}\"",
                    field,
                    highlight_matches(&snippet, matcher, color)
                )
            })
        })
}

fn search_field_display_value(
    issue: &IssueWire,
    field: &str,
) -> Option<String> {
    match field {
        "id" => Some(issue.id.clone()),
        "title" => Some(issue.title.clone()),
        "description" => Some(issue.description.clone()),
        "notes" => Some(notes_text(&issue.notes)),
        "design" => Some(issue.design.clone()),
        "refs" => Some(issue.refs.join("\n")),
        "owner" => Some(issue.owner.clone()),
        "assignee" => Some(issue.assignee.clone()),
        "model" => Some(issue.model.clone()),
        "size" => issue.size.as_ref().map(|size| size.as_str().to_string()),
        "changespec_name" => Some(issue.changespec_name.clone()),
        "changespec_bug_id" => Some(issue.changespec_bug_id.clone()),
        "external_ref" => Some(issue.external_ref.clone()),
        "status" => Some(status_value(&issue.status).to_string()),
        "type" => Some(issue_type_value(&issue.issue_type).to_string()),
        "tier" => issue.tier.as_ref().map(|tier| tier_value(tier).to_string()),
        _ => None,
    }
}

fn single_line_snippet(
    value: &str,
    matcher: &SearchMatcher,
    max_chars: usize,
) -> String {
    let line = value.lines().next().unwrap_or("").trim();
    if line.chars().count() <= max_chars {
        return line.to_string();
    }

    let ranges = matcher.byte_ranges(line);
    let Some((match_start, match_end)) = ranges.first().copied() else {
        return truncate_chars(line, max_chars);
    };
    let total_chars = line.chars().count();
    let match_start_char = byte_to_char_index(line, match_start);
    let match_end_char = byte_to_char_index(line, match_end);
    let match_len = match_end_char.saturating_sub(match_start_char);
    let context = max_chars.saturating_sub(match_len).saturating_div(2);
    let mut start = match_start_char.saturating_sub(context);
    let mut end = (start + max_chars).min(total_chars);
    if end < match_end_char {
        end = match_end_char.min(total_chars);
        start = end.saturating_sub(max_chars);
    }

    let mut snippet = String::new();
    if start > 0 {
        snippet.push_str("...");
    }
    snippet.push_str(&chars_range(line, start, end));
    if end < total_chars {
        snippet.push_str("...");
    }
    snippet
}

fn truncate_chars(value: &str, max_chars: usize) -> String {
    let mut iter = value.chars();
    let mut truncated = iter.by_ref().take(max_chars).collect::<String>();
    if iter.next().is_some() {
        truncated.push_str("...");
    }
    truncated
}

fn chars_range(value: &str, start: usize, end: usize) -> String {
    value
        .chars()
        .skip(start)
        .take(end.saturating_sub(start))
        .collect()
}

fn byte_to_char_index(value: &str, byte_idx: usize) -> usize {
    value
        .char_indices()
        .take_while(|(idx, _)| *idx < byte_idx)
        .count()
}
