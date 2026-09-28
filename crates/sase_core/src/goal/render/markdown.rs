//! Goal markdown rendering for `sase artifact read` and `@goal` citations.
//!
//! Both renderers consume the presentation-neutral
//! [`GoalCardViewWire`](super::GoalCardViewWire) from `goal::view`, so the
//! artifact card and the one-line prompt citation always agree with
//! `sase goal show`. Times here are absolute, per the
//! `bead_time_presentation` convention for persisted surfaces.

use crate::goal::{GoalCardViewWire, GOAL_GLYPH};

/// Maximum characters of one [`goal_citation_line`].
pub const GOAL_CITATION_LINE_MAX: usize = 400;

/// Render the full goal card as markdown for `sase artifact read`.
///
/// Sections with no content are omitted.
pub fn goal_card_markdown(card: &GoalCardViewWire) -> String {
    let mut lines = Vec::new();
    lines.push(format!("{} {}", GOAL_GLYPH, card.title));
    lines.push(String::new());
    lines.push(header_line(card));
    if let Some(outcome) = card.outcome.as_deref() {
        lines.push(String::new());
        lines.push("## Outcome".to_string());
        lines.push(String::new());
        lines.push(outcome.to_string());
    }
    if !card.criteria.is_empty() {
        lines.push(String::new());
        lines.push("## Criteria".to_string());
        lines.push(String::new());
        for criterion in &card.criteria {
            lines.push(format!("- {} ({})", criterion.text, criterion.source));
        }
    }
    if !card.merged.is_empty() {
        lines.push(String::new());
        lines.push("## Merged".to_string());
        lines.push(String::new());
        for merged in &card.merged {
            lines.push(format!("- {merged}"));
        }
    }
    if let Some(plan) = card.plan.as_deref() {
        lines.push(String::new());
        lines.push("## Plan".to_string());
        lines.push(String::new());
        lines.push(plan.to_string());
    }
    if !card.claims.is_empty() {
        lines.push(String::new());
        lines.push("## Claims".to_string());
        lines.push(String::new());
        for claim in &card.claims {
            lines.push(format!(
                "- #{} {} — {} ({:?})",
                claim.claim_no, claim.claim, claim.agent, claim.status,
            ));
        }
    }
    if !card.timeline.is_empty() {
        lines.push(String::new());
        lines.push("## Timeline".to_string());
        lines.push(String::new());
        for entry in &card.timeline {
            lines.push(format!("- {} {}", entry.at, entry.summary));
        }
    }
    lines.push(String::new());
    lines.join("\n")
}

fn header_line(card: &GoalCardViewWire) -> String {
    let mut header = format!(
        "{} · {} · {} · opened {} by {} · rev {}",
        card.goal_ref,
        card.project,
        card.status_badge,
        card.opened_at,
        card.opened_by,
        card.revision,
    );
    if let Some(mode) = card.mode_label.as_deref() {
        header.push_str(&format!(" · {mode}"));
    }
    header
}

/// Render the one-line `@goal` prompt citation (at most 400 characters).
///
/// Example: `goal ⌖7k2mq "Goals feature design" in the sase project (active)
/// — outcome: A critiqued design exists. (+2 criteria: sase goal show 7k2mq)`.
/// A citation never binds; binding is G2's `%goal`.
pub fn goal_citation_line(card: &GoalCardViewWire) -> String {
    let id = card
        .goal_ref
        .strip_prefix("goal:")
        .unwrap_or(&card.goal_ref);
    let status = card.status_badge.to_lowercase();
    let mut line = format!(
        "goal {GOAL_GLYPH}{id} {:?} in the {} project ({status})",
        card.title, card.project,
    );
    if let Some(outcome) = card.outcome.as_deref() {
        line.push_str(&format!(" — outcome: {outcome}"));
    }
    if !card.criteria.is_empty() {
        line.push_str(&format!(
            " (+{} criteria: sase goal show {id})",
            card.criteria.len(),
        ));
    }
    truncate_chars(&line, GOAL_CITATION_LINE_MAX)
}

fn truncate_chars(value: &str, max: usize) -> String {
    if value.chars().count() <= max {
        return value.to_string();
    }
    let kept: String = value.chars().take(max - 1).collect();
    format!("{kept}…")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::goal::view::{
        GoalCardCriterionViewWire, GoalCardTimelineViewWire,
    };
    use crate::goal::{GoalClaimStatusWire, GoalClaimWire};

    fn sample_card() -> GoalCardViewWire {
        GoalCardViewWire {
            title: "Goals feature design".to_string(),
            status_badge: "ACTIVE".to_string(),
            goal_ref: "goal:7k2mq".to_string(),
            project: "sase".to_string(),
            opened_by: "bryan.athena".to_string(),
            opened_at: "2026-09-28T14:02:11.482Z".to_string(),
            opened_age: "2h".to_string(),
            revision: 3,
            mode_label: None,
            outcome: Some(
                "A critiqued, recommended design for Goals exists.".to_string(),
            ),
            criteria: vec![
                GoalCardCriterionViewWire {
                    id: "e1.0".to_string(),
                    text: "Covers storage and sync".to_string(),
                    source: "user".to_string(),
                },
                GoalCardCriterionViewWire {
                    id: "e1.1".to_string(),
                    text: "Names every open question".to_string(),
                    source: "user".to_string(),
                },
            ],
            merged: vec!["goal:3fq9t \"Old goals idea\"".to_string()],
            plan: None,
            claims: Vec::new(),
            timeline: vec![GoalCardTimelineViewWire {
                event_id: "e1".to_string(),
                at: "2026-09-28T14:02:11.482Z".to_string(),
                age: "2h".to_string(),
                summary: "created by bryan.athena".to_string(),
            }],
        }
    }

    #[test]
    fn card_renders_sections_and_omits_empty_ones() {
        let card = sample_card();
        let markdown = goal_card_markdown(&card);
        assert!(markdown.contains("⌖ Goals feature design"));
        assert!(markdown.contains(
            "goal:7k2mq · sase · ACTIVE · opened 2026-09-28T14:02:11.482Z \
             by bryan.athena · rev 3"
        ));
        assert!(markdown.contains("## Outcome"));
        assert!(markdown.contains("## Criteria"));
        assert!(markdown.contains("- Covers storage and sync (user)"));
        assert!(markdown.contains("## Merged"));
        assert!(markdown.contains("## Timeline"));
        assert!(markdown
            .contains("- 2026-09-28T14:02:11.482Z created by bryan.athena"));
        assert!(!markdown.contains("## Plan"));
        assert!(!markdown.contains("## Claims"));
    }

    #[test]
    fn citation_line_matches_contract_example_and_fits() {
        let card = sample_card();
        let line = goal_citation_line(&card);
        assert_eq!(
            line,
            "goal ⌖7k2mq \"Goals feature design\" in the sase project \
             (active) — outcome: A critiqued, recommended design for Goals \
             exists. (+2 criteria: sase goal show 7k2mq)"
        );
        assert!(line.chars().count() <= GOAL_CITATION_LINE_MAX);
    }

    #[test]
    fn citation_line_truncates_long_outcomes() {
        let mut card = sample_card();
        card.outcome = Some("x".repeat(500));
        let line = goal_citation_line(&card);
        assert!(line.chars().count() <= GOAL_CITATION_LINE_MAX);
        assert!(line.ends_with('…'));
    }

    #[test]
    fn empty_card_renders_header_only() {
        let mut card = sample_card();
        card.outcome = None;
        card.criteria.clear();
        card.merged.clear();
        card.timeline.clear();
        let markdown = goal_card_markdown(&card);
        assert!(markdown.contains("⌖ Goals feature design"));
        assert!(!markdown.contains("## "));
        let line = goal_citation_line(&card);
        assert!(!line.contains("outcome:"));
        assert!(!line.contains("criteria:"));
    }

    #[test]
    fn claims_render_with_number_and_status() {
        let mut card = sample_card();
        card.claims.push(GoalClaimWire {
            claim_no: 1,
            claim: "Design covers sync".to_string(),
            agent: "athena.0".to_string(),
            strength: None,
            status: GoalClaimStatusWire::Active,
        });
        let markdown = goal_card_markdown(&card);
        assert!(markdown.contains("## Claims"));
        assert!(
            markdown.contains("- #1 Design covers sync — athena.0 (Active)")
        );
    }
}
