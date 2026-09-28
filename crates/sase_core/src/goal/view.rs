//! Presentation-neutral goal row and card view models.
//!
//! Every renderer consumes these structs: CLI terminal output, the
//! markdown card, prompt citations, and later the G5 TUI. Relative
//! ages are computed from the caller's `now`; persisted surfaces
//! keep the absolute times carried beside them.

use chrono::{DateTime, FixedOffset};
use serde::{Deserialize, Serialize};

use super::wire::{
    GoalClaimWire, GoalCriterionWire, GoalStateWire, GoalStatusWire,
};

/// Glyph shown before every goal id.
pub const GOAL_GLYPH: &str = "⌖";

/// Rose accent for the glyph and ids, absent from the pane palette.
pub const GOAL_ACCENT_HEX: &str = "#FF87AF";

/// One-line list row for a goal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalRowViewWire {
    /// Goal glyph.
    #[serde(default)]
    pub glyph: String,
    /// 5-char goal id.
    #[serde(default)]
    pub id: String,
    /// Lowercase status word.
    #[serde(default)]
    pub status: String,
    /// Goal title.
    #[serde(default)]
    pub title: String,
    /// Relative age of the last update (`3m`, `2h`).
    #[serde(default)]
    pub age: String,
    /// False when the goal needs `doctor`.
    #[serde(default = "default_readable")]
    pub readable: bool,
    /// Absolute RFC3339 last-update time.
    #[serde(default)]
    pub updated_at: String,
    /// Preview of the latest progress note.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_progress_preview: Option<String>,
}

fn default_readable() -> bool {
    true
}

/// Build a row view with a lifespan age.
///
/// The age covers creation to last update. Renderers that know the
/// current time use [`goal_row_view_at`] so the age reads
/// relative to now instead.
pub fn goal_row_view(state: &GoalStateWire) -> GoalRowViewWire {
    let mut row = goal_row_view_at(state, &state.updated_at);
    row.age = relative_age(&state.created_at, &state.updated_at);
    row
}

/// Build a row view with the age relative to `now`.
pub fn goal_row_view_at(state: &GoalStateWire, now: &str) -> GoalRowViewWire {
    GoalRowViewWire {
        glyph: GOAL_GLYPH.to_string(),
        id: state.id.clone(),
        status: state.status.as_str().to_string(),
        title: state.title.clone(),
        age: relative_age(&state.updated_at, now),
        readable: state.readable,
        updated_at: state.updated_at.clone(),
        last_progress_preview: state
            .last_progress
            .as_deref()
            .map(|note| truncate(note, 80)),
    }
}

/// One criterion row on the card.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalCardCriterionViewWire {
    /// Criterion id.
    #[serde(default)]
    pub id: String,
    /// Criterion text.
    #[serde(default)]
    pub text: String,
    /// Source word (`user`, `plan`, `agent`).
    #[serde(default)]
    pub source: String,
}

impl From<&GoalCriterionWire> for GoalCardCriterionViewWire {
    fn from(criterion: &GoalCriterionWire) -> Self {
        Self {
            id: criterion.id.clone(),
            text: criterion.text.clone(),
            source: criterion.source.as_str().to_string(),
        }
    }
}

/// One timeline row on the card.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalCardTimelineViewWire {
    /// Reduced event id.
    #[serde(default)]
    pub event_id: String,
    /// Absolute RFC3339 event time.
    #[serde(default)]
    pub at: String,
    /// Relative age from the card's `now` (`2h`).
    #[serde(default)]
    pub age: String,
    /// One-line human summary.
    #[serde(default)]
    pub summary: String,
}

/// Full goal card for `show`, artifact reads, and citations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalCardViewWire {
    /// Goal title.
    #[serde(default)]
    pub title: String,
    /// Uppercase status badge (`ACTIVE`).
    #[serde(default)]
    pub status_badge: String,
    /// Canonical `goal:<id>` ref.
    #[serde(default)]
    pub goal_ref: String,
    /// Owning project.
    #[serde(default)]
    pub project: String,
    /// Principal that opened the goal.
    #[serde(default)]
    pub opened_by: String,
    /// Absolute RFC3339 creation time.
    #[serde(default)]
    pub opened_at: String,
    /// Relative age of creation (`2h`).
    #[serde(default)]
    pub opened_age: String,
    /// Applied content revision.
    #[serde(default)]
    pub revision: u64,
    /// Mode label; `local` for machine-local drafts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode_label: Option<String>,
    /// Goal outcome; omitted when empty.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome: Option<String>,
    /// Criteria rows; omitted when empty.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub criteria: Vec<GoalCardCriterionViewWire>,
    /// Merge sources; omitted when empty.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub merged: Vec<String>,
    /// Attached plan ref; omitted when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan: Option<String>,
    /// Claims history; omitted when empty.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub claims: Vec<GoalClaimWire>,
    /// Timeline rows; omitted when empty.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub timeline: Vec<GoalCardTimelineViewWire>,
}

/// Build the card view for a state at `now`.
pub fn goal_card_view(state: &GoalStateWire, now: &str) -> GoalCardViewWire {
    let opened_by = state
        .origin
        .as_ref()
        .map(|origin| origin.principal.clone())
        .unwrap_or_default();
    GoalCardViewWire {
        title: state.title.clone(),
        status_badge: state.status.as_str().to_uppercase(),
        goal_ref: format!("goal:{}", state.id),
        project: state.project.clone(),
        opened_by,
        opened_at: state.created_at.clone(),
        opened_age: relative_age(&state.created_at, now),
        revision: state.revision,
        mode_label: match state.status {
            GoalStatusWire::Draft => Some("local".to_string()),
            _ => None,
        },
        outcome: if state.outcome.is_empty() {
            None
        } else {
            Some(state.outcome.clone())
        },
        criteria: state.criteria.iter().map(Into::into).collect(),
        merged: state.merged_from.clone(),
        plan: state.plan.clone(),
        claims: state.claims.clone(),
        timeline: state
            .timeline
            .iter()
            .map(|entry| GoalCardTimelineViewWire {
                event_id: entry.event_id.clone(),
                at: entry.at.clone(),
                age: relative_age(&entry.at, now),
                summary: entry.summary.clone(),
            })
            .collect(),
    }
}

/// Format the span from `from` to `now` as `3m` or `2h`.
///
/// Unparseable times yield an empty string so a bad clock never
/// breaks a renderer.
pub fn relative_age(from: &str, now: &str) -> String {
    let (Some(start), Some(end)) = (parse_time(from), parse_time(now)) else {
        return String::new();
    };
    let seconds = (end - start).num_seconds().max(0);
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
    let months = days / 30;
    if months < 12 {
        return format!("{months}mo");
    }
    format!("{}y", months / 12)
}

fn parse_time(value: &str) -> Option<DateTime<FixedOffset>> {
    if value.is_empty() {
        return None;
    }
    DateTime::parse_from_rfc3339(value).ok()
}

fn truncate(value: &str, max: usize) -> String {
    if value.chars().count() <= max {
        return value.to_string();
    }
    let kept: String = value.chars().take(max - 1).collect();
    format!("{kept}…")
}
