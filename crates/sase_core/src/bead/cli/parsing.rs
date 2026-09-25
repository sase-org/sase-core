//! CLI argument parsing: list/search/update/close/create option
//! grammars and the status/type/tier value parsers behind them.

use std::env;
use std::io::IsTerminal;

use super::super::mutation::BeadUpdateFieldsWire;
use super::super::wire::{
    BeadResolutionWire, BeadTierWire, IssueTypeWire, StatusWire,
};

#[derive(Debug)]
pub(super) struct ListFilters {
    pub(super) statuses: Vec<StatusWire>,
    pub(super) issue_types: Option<Vec<IssueTypeWire>>,
    pub(super) tiers: Option<Vec<BeadTierWire>>,
    pub(super) color: ColorMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SearchFormat {
    Compact,
    Json,
    Full,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ColorMode {
    Auto,
    Always,
    Never,
}

impl ColorMode {
    pub(super) fn resolve_stdout(self) -> bool {
        match self {
            Self::Auto => {
                std::io::stdout().is_terminal()
                    && env::var_os("NO_COLOR").is_none()
            }
            Self::Always => true,
            Self::Never => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SearchArgs {
    pub(super) query: String,
    pub(super) format: SearchFormat,
    pub(super) statuses: Vec<String>,
    pub(super) issue_types: Vec<String>,
    pub(super) tiers: Vec<String>,
    pub(super) limit: Option<usize>,
    pub(super) color: ColorMode,
    pub(super) regex: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum SearchParseOutcome {
    Parsed(SearchArgs),
    UsageError(String),
    Defer,
}

pub(super) fn optional_filter(values: &[String]) -> Option<&[String]> {
    (!values.is_empty()).then_some(values)
}

pub(super) fn parse_list_filters(args: &[String]) -> Option<ListFilters> {
    let mut statuses = Vec::new();
    let mut issue_types = Vec::new();
    let mut tiers = Vec::new();
    let mut color = ColorMode::Auto;
    let mut idx = 0;
    while idx < args.len() {
        let arg = &args[idx];
        if arg == "-s" || arg == "--status" {
            idx += 1;
            let value = args.get(idx)?;
            statuses.push(parse_status(value)?);
        } else if let Some(value) = arg.strip_prefix("--status=") {
            statuses.push(parse_status(value)?);
        } else if arg == "-t" || arg == "--type" {
            idx += 1;
            let value = args.get(idx)?;
            issue_types.push(parse_issue_type(value)?);
        } else if let Some(value) = arg.strip_prefix("--type=") {
            issue_types.push(parse_issue_type(value)?);
        } else if arg == "--tier" {
            idx += 1;
            let value = args.get(idx)?;
            tiers.push(parse_tier(value)?);
        } else if arg == "-c" || arg == "--color" {
            idx += 1;
            color = parse_color_mode(args.get(idx)?)?;
        } else if let Some(value) = arg.strip_prefix("--color=") {
            color = parse_color_mode(value)?;
        } else if arg == "-f" || arg == "--format" {
            idx += 1;
            if args.get(idx)?.as_str() != "compact" {
                return None;
            }
        } else if let Some(value) = arg.strip_prefix("--format=") {
            if value != "compact" {
                return None;
            }
        } else {
            let value = arg.strip_prefix("--tier=")?;
            tiers.push(parse_tier(value)?);
        }
        idx += 1;
    }
    if statuses.is_empty() {
        statuses.push(StatusWire::Open);
        statuses.push(StatusWire::Claimed);
        statuses.push(StatusWire::Ready);
        statuses.push(StatusWire::Snoozed);
        statuses.push(StatusWire::InProgress);
    }
    Some(ListFilters {
        statuses,
        issue_types: (!issue_types.is_empty()).then_some(issue_types),
        tiers: (!tiers.is_empty()).then_some(tiers),
        color,
    })
}

pub(super) fn parse_search_args(args: &[String]) -> SearchParseOutcome {
    let mut query = None;
    let mut format = SearchFormat::Compact;
    let mut statuses = Vec::new();
    let mut issue_types = Vec::new();
    let mut tiers = Vec::new();
    let mut limit = None;
    let mut color = ColorMode::Auto;
    let mut regex = false;
    let mut idx = 0;
    while idx < args.len() {
        let arg = &args[idx];
        if arg == "-e" || arg == "--regex" {
            regex = true;
        } else if arg == "-f" || arg == "--format" {
            idx += 1;
            let Some(value) = args.get(idx) else {
                return SearchParseOutcome::Defer;
            };
            let Some(parsed) = parse_search_format(value) else {
                return SearchParseOutcome::Defer;
            };
            format = parsed;
        } else if let Some(value) = arg.strip_prefix("--format=") {
            let Some(parsed) = parse_search_format(value) else {
                return SearchParseOutcome::Defer;
            };
            format = parsed;
        } else if arg == "-s" || arg == "--status" {
            idx += 1;
            let Some(value) = args.get(idx) else {
                return SearchParseOutcome::Defer;
            };
            if parse_status(value).is_none() {
                return SearchParseOutcome::Defer;
            }
            statuses.push(value.clone());
        } else if let Some(value) = arg.strip_prefix("--status=") {
            if parse_status(value).is_none() {
                return SearchParseOutcome::Defer;
            }
            statuses.push(value.to_string());
        } else if arg == "-t" || arg == "--type" {
            idx += 1;
            let Some(value) = args.get(idx) else {
                return SearchParseOutcome::Defer;
            };
            if parse_issue_type(value).is_none() {
                return SearchParseOutcome::Defer;
            }
            issue_types.push(value.clone());
        } else if let Some(value) = arg.strip_prefix("--type=") {
            if parse_issue_type(value).is_none() {
                return SearchParseOutcome::Defer;
            }
            issue_types.push(value.to_string());
        } else if arg == "--tier" {
            idx += 1;
            let Some(value) = args.get(idx) else {
                return SearchParseOutcome::Defer;
            };
            if parse_tier(value).is_none() {
                return SearchParseOutcome::Defer;
            }
            tiers.push(value.clone());
        } else if let Some(value) = arg.strip_prefix("--tier=") {
            if parse_tier(value).is_none() {
                return SearchParseOutcome::Defer;
            }
            tiers.push(value.to_string());
        } else if arg == "-n" || arg == "--limit" {
            idx += 1;
            let Some(value) = args.get(idx) else {
                return SearchParseOutcome::Defer;
            };
            let Some(parsed) = parse_limit(value) else {
                return SearchParseOutcome::Defer;
            };
            limit = Some(parsed);
        } else if let Some(value) = arg.strip_prefix("--limit=") {
            let Some(parsed) = parse_limit(value) else {
                return SearchParseOutcome::Defer;
            };
            limit = Some(parsed);
        } else if arg == "-c" || arg == "--color" {
            idx += 1;
            let Some(value) = args.get(idx) else {
                return SearchParseOutcome::Defer;
            };
            let Some(parsed) = parse_color_mode(value) else {
                return SearchParseOutcome::Defer;
            };
            color = parsed;
        } else if let Some(value) = arg.strip_prefix("--color=") {
            let Some(parsed) = parse_color_mode(value) else {
                return SearchParseOutcome::Defer;
            };
            color = parsed;
        } else if arg.starts_with('-') {
            return SearchParseOutcome::Defer;
        } else if query.is_none() {
            query = Some(arg.clone());
        } else {
            return SearchParseOutcome::Defer;
        }
        idx += 1;
    }

    let Some(query) = query else {
        return SearchParseOutcome::UsageError(
            "search query cannot be empty".to_string(),
        );
    };
    if query.trim().is_empty() {
        return SearchParseOutcome::UsageError(
            "search query cannot be empty".to_string(),
        );
    }

    SearchParseOutcome::Parsed(SearchArgs {
        query,
        format,
        statuses,
        issue_types,
        tiers,
        limit,
        color,
        regex,
    })
}

pub(super) fn parse_search_format(value: &str) -> Option<SearchFormat> {
    match value {
        "compact" => Some(SearchFormat::Compact),
        "json" => Some(SearchFormat::Json),
        "full" => Some(SearchFormat::Full),
        _ => None,
    }
}

pub(super) fn parse_color_mode(value: &str) -> Option<ColorMode> {
    match value {
        "auto" => Some(ColorMode::Auto),
        "always" => Some(ColorMode::Always),
        "never" => Some(ColorMode::Never),
        _ => None,
    }
}

pub(super) fn parse_limit(value: &str) -> Option<usize> {
    value.parse::<usize>().ok()
}

pub(super) fn parse_update_args(
    args: &[String],
) -> Option<(Vec<String>, BeadUpdateFieldsWire)> {
    let mut ids = Vec::new();
    let mut fields = BeadUpdateFieldsWire::default();
    let mut clear_external_ref = false;
    let mut idx = 0;
    while idx < args.len() {
        let arg = &args[idx];
        if !arg.starts_with('-') {
            ids.push(arg.clone());
            idx += 1;
            continue;
        }
        let (name, value) = if matches!(
            arg.as_str(),
            "-s" | "--status"
                | "-t"
                | "--title"
                | "-d"
                | "--description"
                | "-n"
                | "--notes"
                | "-D"
                | "--design"
                | "-m"
                | "--model"
                | "-a"
                | "--assignee"
                | "-x"
                | "--external-ref"
                | "-E"
                | "--epic-count"
                | "--tier"
        ) {
            idx += 1;
            (arg.as_str(), args.get(idx)?.clone())
        } else if let Some(value) = arg.strip_prefix("--status=") {
            ("--status", value.to_string())
        } else if let Some(value) = arg.strip_prefix("--title=") {
            ("--title", value.to_string())
        } else if let Some(value) = arg.strip_prefix("--description=") {
            ("--description", value.to_string())
        } else if let Some(value) = arg.strip_prefix("--notes=") {
            ("--notes", value.to_string())
        } else if let Some(value) = arg.strip_prefix("--design=") {
            ("--design", value.to_string())
        } else if let Some(value) = arg.strip_prefix("--model=") {
            ("--model", value.to_string())
        } else if let Some(value) = arg.strip_prefix("--assignee=") {
            ("--assignee", value.to_string())
        } else if let Some(value) = arg.strip_prefix("--external-ref=") {
            ("--external-ref", value.to_string())
        } else if arg == "-X" || arg == "--clear-external-ref" {
            clear_external_ref = true;
            idx += 1;
            continue;
        } else {
            let value = arg.strip_prefix("--tier=")?;
            ("--tier", value.to_string())
        };
        match name {
            "-s" | "--status" => {
                parse_status(&value)?;
                fields.status = Some(value);
            }
            "-t" | "--title" => fields.title = Some(value),
            "-d" | "--description" => fields.description = Some(value),
            "-n" | "--notes" => fields.notes = Some(value),
            "-D" | "--design" => fields.design = Some(value),
            "-m" | "--model" => fields.model = Some(value),
            "-a" | "--assignee" => fields.assignee = Some(value),
            "-x" | "--external-ref" => fields.external_ref = Some(value),
            "--tier" => fields.tier = Some(parse_tier(&value)?),
            _ => return None,
        }
        idx += 1;
    }
    if clear_external_ref && fields.external_ref.is_some() {
        return None;
    }
    if clear_external_ref {
        fields.external_ref = Some(String::new());
    }
    Some((ids, fields))
}

pub(super) type ParsedCloseArgs = (
    Vec<String>,
    bool,
    Option<String>,
    Option<String>,
    Option<BeadResolutionWire>,
);

pub(super) fn parse_close_args(args: &[String]) -> Option<ParsedCloseArgs> {
    let mut ids = Vec::new();
    let mut force = false;
    let mut note = None;
    let mut reason = None;
    let mut resolution = None;
    let mut idx = 0;
    while idx < args.len() {
        let arg = &args[idx];
        if arg == "-f" || arg == "--force" {
            force = true;
        } else if arg == "-n" || arg == "--note" {
            idx += 1;
            note = Some(args.get(idx)?.clone());
        } else if let Some(value) = arg.strip_prefix("--note=") {
            note = Some(value.to_string());
        } else if arg == "-r" || arg == "--reason" {
            idx += 1;
            reason = Some(args.get(idx)?.clone());
        } else if let Some(value) = arg.strip_prefix("--reason=") {
            reason = Some(value.to_string());
        } else if arg == "-R" || arg == "--resolution" {
            idx += 1;
            resolution = Some(parse_resolution(args.get(idx)?)?);
        } else if let Some(value) = arg.strip_prefix("--resolution=") {
            resolution = Some(parse_resolution(value)?);
        } else if arg.starts_with('-') {
            return None;
        } else {
            ids.push(arg.clone());
        }
        idx += 1;
    }
    Some((ids, force, note, reason, resolution))
}

pub(super) fn close_note_author() -> Option<String> {
    // Mirrors Python's `discover_agent_identity`: the bare launcher flag
    // `SASE_AGENT=1` carries no identity.
    for key in ["SASE_AGENT_NAME", "SASE_AGENT"] {
        let Ok(value) = env::var(key) else {
            continue;
        };
        let trimmed = value.trim().to_string();
        if trimmed.is_empty() {
            continue;
        }
        if key == "SASE_AGENT" && trimmed == "1" {
            continue;
        }
        return Some(trimmed);
    }
    None
}

pub(super) fn parse_resolution(value: &str) -> Option<BeadResolutionWire> {
    match value {
        "done" => Some(BeadResolutionWire::Done),
        "canceled" => Some(BeadResolutionWire::Canceled),
        "superseded" => Some(BeadResolutionWire::Superseded),
        _ => None,
    }
}

pub(super) fn parse_status(value: &str) -> Option<StatusWire> {
    match value {
        "open" => Some(StatusWire::Open),
        "claimed" => Some(StatusWire::Claimed),
        "ready" => Some(StatusWire::Ready),
        "snoozed" => Some(StatusWire::Snoozed),
        "in_progress" => Some(StatusWire::InProgress),
        "closed" => Some(StatusWire::Closed),
        _ => None,
    }
}

pub(super) fn parse_issue_type(value: &str) -> Option<IssueTypeWire> {
    match value {
        "plan" => Some(IssueTypeWire::Plan),
        "phase" => Some(IssueTypeWire::Phase),
        "task" => Some(IssueTypeWire::Task),
        _ => None,
    }
}

pub(super) fn parse_tier(value: &str) -> Option<BeadTierWire> {
    match value {
        "plan" => Some(BeadTierWire::Plan),
        "epic" => Some(BeadTierWire::Epic),
        _ => None,
    }
}
