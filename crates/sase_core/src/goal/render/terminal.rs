//! Terminal list and card rendering for `sase goal`.
//!
//! Pure functions over the row/card view models. No I/O, no clock:
//! the caller supplies `now` as an RFC3339 string so tests are
//! deterministic and the fast path can share one timestamp.

use serde::{Deserialize, Serialize};
use unicode_width::UnicodeWidthStr;

use super::super::view::{
    goal_card_view, goal_row_view_at, GoalCardViewWire, GoalRowViewWire,
    GOAL_GLYPH,
};
use super::super::wire::GoalStateWire;

/// Request wire for [`render_goal_list`].
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct GoalRenderListRequestWire {
    /// Reduced goals in display order (core list order is lane-stable).
    #[serde(default)]
    pub goals: Vec<GoalStateWire>,
    /// Owning project key, shown in the header.
    #[serde(default)]
    pub project: String,
    /// Ledger mode: `shared` or `local`.
    #[serde(default)]
    pub mode: String,
    /// Seconds since the last successful integration, if ever.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub synced_ago_seconds: Option<f64>,
    /// True while the outbox holds an unpublished write.
    #[serde(default)]
    pub unpublished: bool,
    /// True while a background fetch worker holds the fetch lock.
    #[serde(default)]
    pub refreshing: bool,
    /// Emit ANSI styling; the caller already honored TTY/`NO_COLOR`.
    #[serde(default)]
    pub color: bool,
    /// One compact line per goal (non-TTY and agent runs).
    #[serde(default)]
    pub compact: bool,
    /// RFC3339 now, used for relative ages.
    #[serde(default)]
    pub now: String,
    /// Word for the empty state (`active`, `done`, ...).
    #[serde(default = "default_empty_label")]
    pub empty_label: String,
}

/// Request wire for [`render_goal_card`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalRenderCardRequestWire {
    /// Reduced goal state.
    pub state: GoalStateWire,
    /// RFC3339 now, used for relative ages.
    #[serde(default)]
    pub now: String,
    /// Emit ANSI styling; the caller already honored TTY/`NO_COLOR`.
    #[serde(default)]
    pub color: bool,
}

/// Rendered text response for both list and card bindings.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalRenderTextWire {
    /// Rendered output without a trailing newline.
    #[serde(default)]
    pub text: String,
}

fn default_empty_label() -> String {
    "active".to_string()
}

/// Render the goal list from reduced states.
pub fn render_goal_list(request: &GoalRenderListRequestWire) -> String {
    let rows: Vec<GoalRowViewWire> = request
        .goals
        .iter()
        .map(|state| goal_row_view_at(state, &request.now))
        .collect();
    let mut out = String::new();
    out.push_str(&render_header(request, &rows));
    out.push('\n');
    if rows.is_empty() {
        out.push_str(&render_empty(request));
        out.push('\n');
    } else if request.compact {
        for row in &rows {
            out.push_str(&render_compact_row(request, row));
            out.push('\n');
        }
    } else {
        out.push_str(&render_lane_table(request, &rows));
    }
    let footer = render_footer(request, &rows);
    if !footer.is_empty() {
        out.push_str(&footer);
        out.push('\n');
    }
    trim_trailing_blank_line(&mut out);
    out
}

/// Render one goal card from its reduced state.
pub fn render_goal_card(request: &GoalRenderCardRequestWire) -> String {
    let card = goal_card_view(&request.state, &request.now);
    render_card(request, &request.state, &card)
}

fn render_header(
    request: &GoalRenderListRequestWire,
    rows: &[GoalRowViewWire],
) -> String {
    let mut counts: Vec<(String, usize)> = Vec::new();
    for lane in ["review", "active", "draft", "done", "dropped"] {
        let count = rows.iter().filter(|row| row.status == lane).count();
        if count > 0 {
            counts.push((lane.to_string(), count));
        }
    }
    let unreadable = rows.iter().filter(|row| !row.readable).count();
    let mut parts: Vec<String> = counts
        .iter()
        .map(|(lane, count)| format!("{count} {lane}"))
        .collect();
    parts.push(render_freshness(request));
    let _ = unreadable;
    format!(
        "{} Goals · {}  {}",
        paint(GOAL_GLYPH, ACCENT, request.color),
        request.project,
        parts.join(" · ")
    )
}

fn render_freshness(request: &GoalRenderListRequestWire) -> String {
    if request.mode == "local" {
        return "local only".to_string();
    }
    match request.synced_ago_seconds {
        None => "never synced".to_string(),
        Some(seconds) => {
            format!("synced {} ago", format_age_seconds(seconds))
        }
    }
}

fn render_empty(request: &GoalRenderListRequestWire) -> String {
    let label = if request.empty_label.is_empty() {
        "active".to_string()
    } else {
        request.empty_label.clone()
    };
    format!(
        "No {label} goals in {}. Start one: sase goal new -t \"<title>\" -o \"<what will be true when done>\"",
        request.project
    )
}

fn render_compact_row(
    request: &GoalRenderListRequestWire,
    row: &GoalRowViewWire,
) -> String {
    let status = render_status_word(request, &row.status, row.readable);
    format!(
        "{} {}  {}  {}  · {}",
        paint(GOAL_GLYPH, ACCENT, request.color),
        paint(&row.id, ACCENT, request.color),
        status,
        row.title,
        row.age
    )
}

fn render_lane_table(
    request: &GoalRenderListRequestWire,
    rows: &[GoalRowViewWire],
) -> String {
    let width = rows
        .iter()
        .map(|row| UnicodeWidthStr::width(row.title.as_str()))
        .max()
        .unwrap_or(0)
        .min(MAX_TITLE_WIDTH);
    let mut out = String::new();
    let mut current_lane = String::new();
    for row in rows {
        let lane = lane_label(&row.status, row.readable);
        if lane != current_lane {
            current_lane = lane.clone();
            out.push_str(&format!("  {lane}\n"));
        }
        out.push_str(&render_table_row(request, row, width));
        out.push('\n');
    }
    out
}

fn render_table_row(
    request: &GoalRenderListRequestWire,
    row: &GoalRowViewWire,
    width: usize,
) -> String {
    let title = fit_title(&row.title, width);
    format!(
        "  {} {}  {}  {}",
        paint(GOAL_GLYPH, ACCENT, request.color),
        paint(&row.id, ACCENT, request.color),
        title,
        dim(&row.age, request.color)
    )
}

fn render_footer(
    request: &GoalRenderListRequestWire,
    rows: &[GoalRowViewWire],
) -> String {
    let mut chips: Vec<String> = Vec::new();
    if request.unpublished {
        chips.push(paint("↑ unpublished", WARNING, request.color));
    }
    if request.mode == "local" {
        chips.push(dim("local only", request.color));
    }
    if request.refreshing {
        chips.push(dim("refreshing…", request.color));
    }
    let unreadable = rows.iter().filter(|row| !row.readable).count();
    if unreadable > 0 {
        chips.push(paint(
            &format!("⚠ {unreadable} unreadable (run sase goal doctor)"),
            WARNING,
            request.color,
        ));
    }
    chips.join(" · ")
}

fn render_card(
    request: &GoalRenderCardRequestWire,
    state: &GoalStateWire,
    card: &GoalCardViewWire,
) -> String {
    let mut out = String::new();
    let badge = paint(
        &card.status_badge,
        status_color(request.state.status.as_str(), state.readable),
        request.color,
    );
    out.push_str(&format!(
        "{} {}  {}\n",
        paint(GOAL_GLYPH, ACCENT, request.color),
        card.title,
        badge
    ));
    let mut byline = format!(
        "{} · {} · opened {} ago by {} · rev {}",
        card.goal_ref,
        card.project,
        card.opened_age,
        card.opened_by,
        card.revision
    );
    if let Some(mode) = card.mode_label.as_deref() {
        byline.push_str(&format!(" · {mode}"));
    }
    out.push_str(&format!("{}\n", dim(&byline, request.color)));
    if !state.readable {
        let reason = state.unreadable_reason.as_deref().unwrap_or("unknown");
        out.push_str(&format!(
            "\n{}\n",
            paint(
                &format!("⚠ unreadable: {reason} (run sase goal doctor)"),
                WARNING,
                request.color,
            )
        ));
    }
    if let Some(outcome) = card.outcome.as_deref() {
        out.push_str(&format!("\nOUTCOME    {outcome}\n"));
    }
    if !card.criteria.is_empty() {
        out.push_str("\nCRITERIA");
        for (index, criterion) in card.criteria.iter().enumerate() {
            out.push_str(&format!(
                "\n           {}  {}  {}",
                index + 1,
                criterion.text,
                dim(&criterion.source, request.color)
            ));
        }
        out.push('\n');
    }
    if let Some(target) = state.merged_into.as_deref() {
        out.push_str(&format!("\nMERGED     → goal:{target}\n"));
    }
    for source in &card.merged {
        out.push_str(&format!("\nMERGED     ← goal:{source}\n"));
    }
    if let Some(plan) = card.plan.as_deref() {
        out.push_str(&format!("\nPLAN       {plan}\n"));
    }
    if !card.claims.is_empty() {
        out.push_str(&format!(
            "\nCLAIMS     {} claim(s) awaiting review\n",
            card.claims.len()
        ));
    }
    if !card.timeline.is_empty() {
        out.push_str("\nTIMELINE");
        for entry in &card.timeline {
            out.push_str(&format!(
                "\n           {}  {}",
                entry.age, entry.summary
            ));
        }
        out.push('\n');
    }
    trim_trailing_blank_line(&mut out);
    out
}

fn lane_label(status: &str, readable: bool) -> String {
    if !readable {
        return "UNREADABLE".to_string();
    }
    status.to_uppercase()
}

fn render_status_word(
    request: &GoalRenderListRequestWire,
    status: &str,
    readable: bool,
) -> String {
    if !readable {
        return paint("unreadable", WARNING, request.color);
    }
    paint(status, status_color(status, true), request.color)
}

fn status_color(status: &str, readable: bool) -> &'static str {
    if !readable {
        return WARNING;
    }
    match status {
        "review" => BOLD_ACCENT,
        "done" => GREEN,
        "dropped" => GREY,
        _ => RESET_STYLE,
    }
}

fn fit_title(title: &str, width: usize) -> String {
    let current = UnicodeWidthStr::width(title);
    if current <= width {
        let pad = width - current;
        return format!("{title}{}", " ".repeat(pad));
    }
    let mut kept_width = 0;
    let mut kept = String::new();
    for ch in title.chars() {
        let ch_width = unicode_width::UnicodeWidthChar::width(ch).unwrap_or(0);
        if kept_width + ch_width + 1 > width {
            break;
        }
        kept.push(ch);
        kept_width += ch_width;
    }
    format!("{kept}…{}", " ".repeat(width - kept_width - 1))
}

fn format_age_seconds(seconds: f64) -> String {
    let seconds = seconds.max(0.0) as i64;
    if seconds < 60 {
        return format!("{seconds}s");
    }
    let minutes = seconds / 60;
    if minutes < 60 {
        return format!("{minutes}m");
    }
    let hours = minutes / 60;
    if hours < 24 {
        return format!("{hours}h");
    }
    let days = hours / 24;
    if days < 30 {
        return format!("{days}d");
    }
    format!("{}mo", days / 30)
}

fn trim_trailing_blank_line(out: &mut String) {
    while out.ends_with("\n\n") {
        out.pop();
    }
}

const ACCENT: &str = "\u{1b}[95m";
const BOLD_ACCENT: &str = "\u{1b}[1;95m";
const GREEN: &str = "\u{1b}[32m";
const GREY: &str = "\u{1b}[90m";
const WARNING: &str = "\u{1b}[33m";
const DIM: &str = "\u{1b}[2m";
const RESET: &str = "\u{1b}[0m";
const RESET_STYLE: &str = "";

const MAX_TITLE_WIDTH: usize = 60;

fn paint(text: &str, style: &str, enabled: bool) -> String {
    if !enabled || style.is_empty() {
        return text.to_string();
    }
    format!("{style}{text}{RESET}")
}

fn dim(text: &str, enabled: bool) -> String {
    paint(text, DIM, enabled)
}

#[cfg(test)]
mod tests;
