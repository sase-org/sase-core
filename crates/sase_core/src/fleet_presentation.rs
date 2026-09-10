//! Deterministic fleet presentation policy.
//!
//! The owner-side fleet snapshot must not serve every visible index record as
//! a standalone row forever: a `Dead`/`NotProcess` active-tier record is
//! terminal for presentation rather than still-running, and terminal
//! presentation — both genuinely completed records and demoted dead-active
//! ones — is bounded by age and count so a machine's history does not grow
//! the served set without limit. This module makes that selection decision
//! from record facts (from the index) and owner observations (resolved by
//! the gateway); it never touches artifact files and never itself resolves
//! liveness or dismissal lineage.

use serde::{Deserialize, Serialize};

use crate::fleet_contract::{
    validate_schema, validate_timestamp, FleetContractError, OwnerLivenessWire,
    FLEET_CONTRACT_SCHEMA_VERSION,
};

/// Recent-terminal presentation age horizon: seven days, matching the local
/// listing's bounded recent-completion window and the local date model's
/// definition of the recent week.
pub const FLEET_PRESENTATION_RECENT_TERMINAL_WINDOW_SECONDS: f64 =
    7.0 * 24.0 * 60.0 * 60.0;

/// Recent-terminal presentation count bound, matching the local listing's
/// 200-row bounded tier.
pub const FLEET_PRESENTATION_RECENT_TERMINAL_MAX_ROWS: usize = 200;

/// One candidate row for the presentation decision.
///
/// `identity` is the only thing the decision hands back; it is never
/// interpreted, only echoed into whichever output bucket the candidate
/// belongs to.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetPresentationCandidateWire {
    pub schema_version: u32,
    pub identity: String,
    /// Owner-resolved liveness. `Alive`/`Unknown` candidates are never
    /// demoted or excluded.
    pub liveness: OwnerLivenessWire,
    /// Whether a waiting marker or pending question protects this record
    /// from demotion/exclusion regardless of liveness.
    pub protected: bool,
    /// Trustworthy completion time used to rank and bound the
    /// recent-terminal tier: `done.finished_at` when present, else a
    /// stopped/record timestamp fallback. The caller always populates this,
    /// even for a candidate that is not yet marked done in the index, so a
    /// demoted dead-active record ranks and windows the same way as a
    /// genuinely completed one.
    pub completion_time_unix: f64,
    /// Whether this candidate's family root is a dismissed identity,
    /// resolved by the caller through the bounded core index lineage API.
    pub family_root_dismissed: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetPresentationRequestWire {
    pub schema_version: u32,
    pub now_unix: f64,
    pub candidates: Vec<FleetPresentationCandidateWire>,
}

/// Presentation decision: every input identity appears in exactly one
/// bucket.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetPresentationDecisionWire {
    pub schema_version: u32,
    /// Identities selected for current (live) presentation.
    pub current: Vec<String>,
    /// Identities selected for recent-terminal presentation, ordered
    /// newest-completion-first.
    pub recent_terminal: Vec<String>,
    /// Identities excluded from presentation entirely.
    pub excluded: Vec<String>,
}

pub fn decide_fleet_presentation(
    request: &FleetPresentationRequestWire,
) -> Result<FleetPresentationDecisionWire, FleetContractError> {
    validate_schema("fleet presentation request", request.schema_version)?;
    validate_timestamp("now_unix", request.now_unix)?;

    let mut current = Vec::new();
    let mut terminal_candidates: Vec<&FleetPresentationCandidateWire> =
        Vec::new();
    let mut excluded = Vec::new();

    for candidate in &request.candidates {
        validate_schema(
            "fleet presentation candidate",
            candidate.schema_version,
        )?;
        if candidate.identity.trim().is_empty() {
            return Err(FleetContractError::Validation(
                "fleet presentation candidate identity must not be empty"
                    .to_string(),
            ));
        }
        validate_timestamp(
            "completion_time_unix",
            candidate.completion_time_unix,
        )?;
        if candidate.protected {
            current.push(candidate.identity.clone());
            continue;
        }
        if matches!(
            candidate.liveness,
            OwnerLivenessWire::Alive | OwnerLivenessWire::Unknown
        ) {
            current.push(candidate.identity.clone());
            continue;
        }
        // Liveness is definitively `Dead` or `NotProcess` and the record is
        // unprotected: it is terminal for presentation whether or not the
        // index already recorded it as done.
        if candidate.family_root_dismissed {
            excluded.push(candidate.identity.clone());
            continue;
        }
        terminal_candidates.push(candidate);
    }

    // Rank by trustworthy completion time, newest first, with a stable
    // identity tie-breaker; then apply one combined age/count bound so dead
    // active leftovers cannot evade the window that already-terminal
    // records are held to.
    terminal_candidates.sort_by(|left, right| {
        right
            .completion_time_unix
            .partial_cmp(&left.completion_time_unix)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.identity.cmp(&right.identity))
    });

    let mut recent_terminal = Vec::new();
    for (rank, candidate) in terminal_candidates.into_iter().enumerate() {
        let age = request.now_unix - candidate.completion_time_unix;
        let within_window =
            age <= FLEET_PRESENTATION_RECENT_TERMINAL_WINDOW_SECONDS;
        let within_cap = rank < FLEET_PRESENTATION_RECENT_TERMINAL_MAX_ROWS;
        if within_window && within_cap {
            recent_terminal.push(candidate.identity.clone());
        } else {
            excluded.push(candidate.identity.clone());
        }
    }

    Ok(FleetPresentationDecisionWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        current,
        recent_terminal,
        excluded,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const DAY: f64 = 24.0 * 60.0 * 60.0;

    fn candidate(
        identity: &str,
        liveness: OwnerLivenessWire,
        protected: bool,
        completion_time_unix: f64,
        family_root_dismissed: bool,
    ) -> FleetPresentationCandidateWire {
        FleetPresentationCandidateWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            identity: identity.to_string(),
            liveness,
            protected,
            completion_time_unix,
            family_root_dismissed,
        }
    }

    fn decide(
        now_unix: f64,
        candidates: Vec<FleetPresentationCandidateWire>,
    ) -> FleetPresentationDecisionWire {
        decide_fleet_presentation(&FleetPresentationRequestWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            now_unix,
            candidates,
        })
        .unwrap()
    }

    #[test]
    fn alive_and_unknown_rows_always_stay_current() {
        let decision = decide(
            1_000_000.0,
            vec![
                candidate("alive", OwnerLivenessWire::Alive, false, 0.0, false),
                candidate(
                    "unknown",
                    OwnerLivenessWire::Unknown,
                    false,
                    0.0,
                    false,
                ),
            ],
        );
        assert_eq!(decision.current, vec!["alive", "unknown"]);
        assert!(decision.recent_terminal.is_empty());
        assert!(decision.excluded.is_empty());
    }

    #[test]
    fn protected_dead_row_stays_current_despite_liveness() {
        let decision = decide(
            1_000_000.0,
            vec![candidate(
                "waiting-dead",
                OwnerLivenessWire::Dead,
                true,
                0.0,
                false,
            )],
        );
        assert_eq!(decision.current, vec!["waiting-dead"]);
    }

    #[test]
    fn dead_active_leftover_demotes_into_recent_terminal() {
        let now = 1_000_000.0;
        let decision = decide(
            now,
            vec![candidate(
                "dead-active",
                OwnerLivenessWire::Dead,
                false,
                now - DAY,
                false,
            )],
        );
        assert!(decision.current.is_empty());
        assert_eq!(decision.recent_terminal, vec!["dead-active"]);
    }

    #[test]
    fn terminal_and_demoted_rows_share_one_combined_window_and_cap() {
        let now = 1_000_000.0;
        let mut candidates = vec![candidate(
            "done-recent",
            OwnerLivenessWire::Dead,
            false,
            now - DAY,
            false,
        )];
        // 200 dead-active leftovers newer than the terminal row above; they
        // should fill the cap ahead of it under one combined bound.
        for i in 0..200 {
            candidates.push(candidate(
                &format!("dead-{i}"),
                OwnerLivenessWire::NotProcess,
                false,
                now - DAY + (i as f64) + 1.0,
                false,
            ));
        }
        let decision = decide(now, candidates);
        assert_eq!(decision.recent_terminal.len(), 200);
        assert!(!decision
            .recent_terminal
            .contains(&"done-recent".to_string()));
        assert!(decision.excluded.contains(&"done-recent".to_string()));
    }

    #[test]
    fn rows_outside_the_seven_day_window_are_excluded() {
        let now = 1_000_000.0;
        let decision = decide(
            now,
            vec![candidate(
                "old",
                OwnerLivenessWire::Dead,
                false,
                now - (8.0 * DAY),
                false,
            )],
        );
        assert!(decision.recent_terminal.is_empty());
        assert_eq!(decision.excluded, vec!["old"]);
    }

    #[test]
    fn dead_orphan_of_dismissed_family_is_excluded() {
        let now = 1_000_000.0;
        let decision = decide(
            now,
            vec![candidate(
                "orphan",
                OwnerLivenessWire::Dead,
                false,
                now - DAY,
                true,
            )],
        );
        assert!(decision.recent_terminal.is_empty());
        assert_eq!(decision.excluded, vec!["orphan"]);
    }

    #[test]
    fn live_member_of_dismissed_family_is_never_excluded() {
        let now = 1_000_000.0;
        let decision = decide(
            now,
            vec![candidate(
                "still-alive",
                OwnerLivenessWire::Alive,
                false,
                now - DAY,
                true,
            )],
        );
        assert_eq!(decision.current, vec!["still-alive"]);
        assert!(decision.excluded.is_empty());
    }

    #[test]
    fn ordering_is_stable_newest_completion_first_with_identity_tiebreak() {
        let now = 1_000_000.0;
        let decision = decide(
            now,
            vec![
                candidate(
                    "b",
                    OwnerLivenessWire::Dead,
                    false,
                    now - 100.0,
                    false,
                ),
                candidate(
                    "a",
                    OwnerLivenessWire::Dead,
                    false,
                    now - 100.0,
                    false,
                ),
                candidate(
                    "newest",
                    OwnerLivenessWire::Dead,
                    false,
                    now - 10.0,
                    false,
                ),
            ],
        );
        assert_eq!(decision.recent_terminal, vec!["newest", "a", "b"]);
    }

    #[test]
    fn rejects_unknown_schema_version() {
        let request = FleetPresentationRequestWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION + 1,
            now_unix: 1.0,
            candidates: Vec::new(),
        };
        assert!(decide_fleet_presentation(&request).is_err());
    }
}
