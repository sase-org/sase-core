//! Shared presentation vocabulary: status/type glyphs, ANSI colorizers,
//! and status/type/tier value formatters.

use super::super::search::SearchMatcher;
use super::super::wire::{BeadTierWire, IssueTypeWire, IssueWire, StatusWire};
use unicode_width::UnicodeWidthStr;

pub(super) const ANSI_RESET: &str = "\x1b[0m";
pub(super) const ANSI_DIM: &str = "\x1b[2m";
pub(super) const ANSI_BOLD_BLUE: &str = "\x1b[1;34m";
pub(super) const ANSI_HIGHLIGHT: &str = "\x1b[30;43m";
pub(super) const ANSI_HIGHLIGHT_RESET: &str = "\x1b[39;49m";
pub(super) const ANSI_GREEN: &str = "\x1b[32m";
pub(super) const ANSI_BRIGHT_CYAN: &str = "\x1b[96m";
pub(super) const ANSI_MAGENTA: &str = "\x1b[35m";
pub(super) const ANSI_YELLOW: &str = "\x1b[33m";
pub(super) const ANSI_CYAN: &str = "\x1b[36m";
pub(super) const ANSI_BRIGHT_BLACK: &str = "\x1b[90m";
pub(super) const ANSI_TYPE_PLAN: &str = "\x1b[38;5;220m";
pub(super) const ANSI_TYPE_PHASE: &str = "\x1b[38;5;117m";
pub(super) const ANSI_TYPE_TASK: &str = "\x1b[38;5;177m";

/// CLI glyph and ANSI metadata mirrored from SASE's shared Python
/// presentation modules. Keeping each glyph beside its style prevents the
/// Rust renderers from developing separate, internally inconsistent maps.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct CliPresentation {
    glyph: &'static str,
    cli_style: &'static str,
}

pub(super) fn status_presentation(status: &StatusWire) -> CliPresentation {
    match status {
        StatusWire::Open => CliPresentation {
            glyph: "○",
            cli_style: ANSI_CYAN,
        },
        StatusWire::Claimed => CliPresentation {
            glyph: "◎",
            cli_style: ANSI_MAGENTA,
        },
        StatusWire::Ready => CliPresentation {
            glyph: "◇",
            cli_style: ANSI_BRIGHT_CYAN,
        },
        StatusWire::Snoozed => CliPresentation {
            glyph: "◈",
            cli_style: ANSI_BRIGHT_BLACK,
        },
        StatusWire::InProgress => CliPresentation {
            glyph: "◐",
            cli_style: ANSI_YELLOW,
        },
        StatusWire::Closed => CliPresentation {
            glyph: "✓",
            cli_style: ANSI_GREEN,
        },
    }
}

pub(super) fn issue_type_presentation(
    issue_type: &IssueTypeWire,
) -> CliPresentation {
    match issue_type {
        IssueTypeWire::Plan => CliPresentation {
            glyph: "▸",
            cli_style: ANSI_TYPE_PLAN,
        },
        IssueTypeWire::Phase => CliPresentation {
            glyph: "↳",
            cli_style: ANSI_TYPE_PHASE,
        },
        IssueTypeWire::Task => CliPresentation {
            glyph: "◆",
            cli_style: ANSI_TYPE_TASK,
        },
    }
}

pub(super) fn color_cli_glyph(
    presentation: CliPresentation,
    color: bool,
) -> String {
    if color {
        format!(
            "{}{}{}",
            presentation.cli_style, presentation.glyph, ANSI_RESET
        )
    } else {
        presentation.glyph.to_string()
    }
}

pub(super) fn color_status_icon(status: &StatusWire, color: bool) -> String {
    color_cli_glyph(status_presentation(status), color)
}

pub(super) fn compact_type_width() -> usize {
    [
        IssueTypeWire::Plan,
        IssueTypeWire::Phase,
        IssueTypeWire::Task,
    ]
    .iter()
    .map(|issue_type| issue_type_presentation(issue_type).glyph.width())
    .max()
    .unwrap_or(0)
}

pub(super) fn color_issue_type_cell(
    issue_type: &IssueTypeWire,
    color: bool,
    width: usize,
) -> String {
    let presentation = issue_type_presentation(issue_type);
    let padding = " ".repeat(width.saturating_sub(presentation.glyph.width()));
    format!("{}{padding}", color_cli_glyph(presentation, color))
}

pub(super) fn color_issue_id(issue_id: &str, color: bool) -> String {
    if color {
        format!("{ANSI_BOLD_BLUE}{issue_id}{ANSI_RESET}")
    } else {
        issue_id.to_string()
    }
}

pub(super) fn dim_line(line: &str, color: bool) -> String {
    if color {
        format!("{ANSI_DIM}{line}{ANSI_RESET}")
    } else {
        line.to_string()
    }
}

pub(super) fn highlight_matches(
    text: &str,
    matcher: &SearchMatcher,
    color: bool,
) -> String {
    if !color {
        return text.to_string();
    }
    let ranges = matcher.byte_ranges(text);
    if ranges.is_empty() {
        return text.to_string();
    }

    let mut highlighted = String::new();
    let mut cursor = 0;
    for (start, end) in ranges {
        if start < cursor {
            continue;
        }
        highlighted.push_str(&text[cursor..start]);
        highlighted.push_str(ANSI_HIGHLIGHT);
        highlighted.push_str(&text[start..end]);
        highlighted.push_str(ANSI_HIGHLIGHT_RESET);
        cursor = end;
    }
    highlighted.push_str(&text[cursor..]);
    highlighted
}

pub(super) fn status_icon(status: &StatusWire) -> &'static str {
    status_presentation(status).glyph
}

pub(super) fn status_value(status: &StatusWire) -> &'static str {
    match status {
        StatusWire::Open => "open",
        StatusWire::Claimed => "claimed",
        StatusWire::Ready => "ready",
        StatusWire::Snoozed => "snoozed",
        StatusWire::InProgress => "in_progress",
        StatusWire::Closed => "closed",
    }
}

pub(super) fn status_upper(status: &StatusWire) -> &'static str {
    match status {
        StatusWire::Open => "OPEN",
        StatusWire::Claimed => "CLAIMED",
        StatusWire::Ready => "READY",
        StatusWire::Snoozed => "Snoozed",
        StatusWire::InProgress => "IN_PROGRESS",
        StatusWire::Closed => "CLOSED",
    }
}

pub(super) fn issue_type_value(issue_type: &IssueTypeWire) -> &'static str {
    match issue_type {
        IssueTypeWire::Plan => "plan",
        IssueTypeWire::Phase => "phase",
        IssueTypeWire::Task => "task",
    }
}

pub(super) fn issue_tier_suffix(issue: &IssueWire) -> String {
    if issue.issue_type != IssueTypeWire::Plan {
        return String::new();
    }
    issue
        .tier
        .as_ref()
        .map(|tier| format!(" · Tier: {}", tier_value(tier)))
        .unwrap_or_default()
}

pub(super) fn tier_value(tier: &BeadTierWire) -> &'static str {
    match tier {
        BeadTierWire::Plan => "plan",
        BeadTierWire::Epic => "epic",
    }
}
