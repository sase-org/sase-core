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

use std::collections::BTreeSet;

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
    /// Owner-resolved liveness. Undismissed `Alive`/`Unknown` candidates
    /// stay current. `Dead`/`NotProcess` candidates take the bounded
    /// terminal path even when `protected` is set.
    pub liveness: OwnerLivenessWire,
    /// Whether a waiting marker or pending question protects this record.
    /// Protection keeps only non-definitively-dead (`Alive`/`Unknown`)
    /// candidates current; a protected `Dead`/`NotProcess` row retires
    /// through the same bounded terminal path as any other dead leftover.
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
    /// A resolved owner dismissal excludes the candidate regardless of
    /// protection or apparent liveness.
    pub family_root_dismissed: bool,
    /// Whether this candidate is a concrete family shell rather than a
    /// family root. Live and unknown members stay current. Pending
    /// (protected) dead members stay current. Other dead members of a
    /// currently presented family are served in the bounded terminal
    /// window so the viewer can nest them; orphan dead members stay
    /// excluded.
    #[serde(default)]
    pub family_member: bool,
    /// Grouping key for "currently presented family". A family is
    /// presented when a root or a live/unknown/pending member is already
    /// current, or a non-member terminal of the same key is inside the
    /// recent window.
    #[serde(default)]
    pub family_key: Option<String>,
    /// Owner observation that the recorded PID is live but is not this
    /// agent (wrong command line or claim/marker mismatch). Such a row is
    /// excluded from presentation and history rather than demoted into
    /// the recent-terminal window.
    #[serde(default)]
    pub process_identity_mismatch: bool,
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
    let mut presented_families = BTreeSet::new();

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
        if candidate.family_root_dismissed
            || candidate.process_identity_mismatch
        {
            excluded.push(candidate.identity.clone());
            continue;
        }
        if matches!(
            candidate.liveness,
            OwnerLivenessWire::Alive | OwnerLivenessWire::Unknown
        ) {
            mark_presented_family(&mut presented_families, candidate);
            current.push(candidate.identity.clone());
            continue;
        }
        // Liveness is definitively `Dead` or `NotProcess`. A pending
        // family shell whose creator PID is dead is not obsolete: keep
        // it current. Standalone protected leftovers still take the
        // bounded terminal path.
        if candidate.family_member && candidate.protected {
            mark_presented_family(&mut presented_families, candidate);
            current.push(candidate.identity.clone());
            continue;
        }
        terminal_candidates.push(candidate);
    }

    for candidate in &terminal_candidates {
        if candidate.family_member {
            continue;
        }
        let age = request.now_unix - candidate.completion_time_unix;
        if age <= FLEET_PRESENTATION_RECENT_TERMINAL_WINDOW_SECONDS {
            mark_presented_family(&mut presented_families, candidate);
        }
    }

    let mut ranked: Vec<&FleetPresentationCandidateWire> = Vec::new();
    for candidate in terminal_candidates {
        if candidate.family_member
            && !family_is_presented(&presented_families, candidate)
        {
            excluded.push(candidate.identity.clone());
            continue;
        }
        ranked.push(candidate);
    }

    // Rank by trustworthy completion time, newest first, with a stable
    // identity tie-breaker; then apply one combined age/count bound so dead
    // active leftovers cannot evade the window that already-terminal
    // records are held to.
    ranked.sort_by(|left, right| {
        right
            .completion_time_unix
            .partial_cmp(&left.completion_time_unix)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.identity.cmp(&right.identity))
    });

    let mut recent_terminal = Vec::new();
    for (rank, candidate) in ranked.into_iter().enumerate() {
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

fn mark_presented_family(
    presented: &mut BTreeSet<String>,
    candidate: &FleetPresentationCandidateWire,
) {
    if let Some(key) = candidate
        .family_key
        .as_deref()
        .filter(|value| !value.is_empty())
    {
        presented.insert(key.to_string());
    }
}

fn family_is_presented(
    presented: &BTreeSet<String>,
    candidate: &FleetPresentationCandidateWire,
) -> bool {
    candidate
        .family_key
        .as_deref()
        .is_some_and(|key| !key.is_empty() && presented.contains(key))
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
            family_member: false,
            family_key: None,
            process_identity_mismatch: false,
        }
    }

    fn mismatch_candidate(
        identity: &str,
        liveness: OwnerLivenessWire,
        protected: bool,
        completion_time_unix: f64,
        family_root_dismissed: bool,
    ) -> FleetPresentationCandidateWire {
        FleetPresentationCandidateWire {
            process_identity_mismatch: true,
            ..candidate(
                identity,
                liveness,
                protected,
                completion_time_unix,
                family_root_dismissed,
            )
        }
    }

    fn family_member_candidate(
        identity: &str,
        liveness: OwnerLivenessWire,
        protected: bool,
        completion_time_unix: f64,
    ) -> FleetPresentationCandidateWire {
        FleetPresentationCandidateWire {
            family_member: true,
            family_key: Some("lane".to_string()),
            ..candidate(
                identity,
                liveness,
                protected,
                completion_time_unix,
                false,
            )
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
    fn protected_dead_row_takes_the_bounded_terminal_path() {
        let now = 1_000_000.0;
        let decision = decide(
            now,
            vec![
                candidate(
                    "waiting-dead-recent",
                    OwnerLivenessWire::Dead,
                    true,
                    now - DAY,
                    false,
                ),
                candidate(
                    "waiting-dead-old",
                    OwnerLivenessWire::NotProcess,
                    true,
                    now - (8.0 * DAY),
                    false,
                ),
            ],
        );
        assert!(decision.current.is_empty());
        assert_eq!(decision.recent_terminal, vec!["waiting-dead-recent"]);
        assert_eq!(decision.excluded, vec!["waiting-dead-old"]);
    }

    #[test]
    fn dismissed_rows_are_excluded_regardless_of_protection_or_liveness() {
        let now = 1_000_000.0;
        let decision = decide(
            now,
            vec![
                candidate(
                    "dismissed-protected",
                    OwnerLivenessWire::Dead,
                    true,
                    now - DAY,
                    true,
                ),
                candidate(
                    "dismissed-alive",
                    OwnerLivenessWire::Alive,
                    false,
                    now - DAY,
                    true,
                ),
                candidate(
                    "dismissed-unknown",
                    OwnerLivenessWire::Unknown,
                    false,
                    now - DAY,
                    true,
                ),
            ],
        );
        assert!(decision.current.is_empty());
        assert!(decision.recent_terminal.is_empty());
        assert_eq!(
            decision.excluded,
            vec![
                "dismissed-protected",
                "dismissed-alive",
                "dismissed-unknown"
            ]
        );
    }

    #[test]
    fn identity_mismatch_is_excluded_even_when_apparently_alive() {
        let now = 1_000_000.0;
        let decision = decide(
            now,
            vec![mismatch_candidate(
                "recycled",
                OwnerLivenessWire::Dead,
                false,
                now - DAY,
                false,
            )],
        );
        assert!(decision.current.is_empty());
        assert!(decision.recent_terminal.is_empty());
        assert_eq!(decision.excluded, vec!["recycled"]);
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
    fn terminal_family_member_of_visible_family_is_served_for_nesting() {
        let now = 1_000_000.0;
        let mut root =
            candidate("root", OwnerLivenessWire::Dead, false, now - DAY, false);
        root.family_key = Some("lane".to_string());
        let decision = decide(
            now,
            vec![
                root,
                family_member_candidate(
                    "root--gate",
                    OwnerLivenessWire::Dead,
                    false,
                    now - (DAY / 2.0),
                ),
            ],
        );
        assert!(decision.current.is_empty());
        assert_eq!(decision.recent_terminal, vec!["root--gate", "root"]);
        assert!(decision.excluded.is_empty());
    }

    #[test]
    fn live_and_unknown_family_members_remain_current() {
        let now = 1_000_000.0;
        let decision = decide(
            now,
            vec![
                family_member_candidate(
                    "active-member",
                    OwnerLivenessWire::Alive,
                    false,
                    now - DAY,
                ),
                family_member_candidate(
                    "unknown-member",
                    OwnerLivenessWire::Unknown,
                    false,
                    now - DAY,
                ),
            ],
        );
        assert_eq!(decision.current, vec!["active-member", "unknown-member"]);
        assert!(decision.recent_terminal.is_empty());
        assert!(decision.excluded.is_empty());
    }

    #[test]
    fn pending_dead_creator_family_member_stays_current() {
        let now = 1_000_000.0;
        let decision = decide(
            now,
            vec![family_member_candidate(
                "waiting-member",
                OwnerLivenessWire::Dead,
                true,
                now - DAY,
            )],
        );
        assert_eq!(decision.current, vec!["waiting-member"]);
        assert!(decision.recent_terminal.is_empty());
        assert!(decision.excluded.is_empty());
    }

    #[test]
    fn orphan_terminal_family_member_stays_excluded() {
        let now = 1_000_000.0;
        let decision = decide(
            now,
            vec![family_member_candidate(
                "orphan--gate",
                OwnerLivenessWire::Dead,
                false,
                now - DAY,
            )],
        );
        assert!(decision.current.is_empty());
        assert!(decision.recent_terminal.is_empty());
        assert_eq!(decision.excluded, vec!["orphan--gate"]);
    }

    #[test]
    fn served_set_puts_every_identity_in_exactly_one_bucket() {
        let now = 1_000_000.0;
        let decision = decide(
            now,
            vec![
                candidate("live", OwnerLivenessWire::Alive, false, now, false),
                candidate(
                    "unknown",
                    OwnerLivenessWire::Unknown,
                    false,
                    now,
                    false,
                ),
                candidate(
                    "protected-live",
                    OwnerLivenessWire::Alive,
                    true,
                    now,
                    false,
                ),
                candidate(
                    "dead-recent",
                    OwnerLivenessWire::Dead,
                    false,
                    now - DAY,
                    false,
                ),
                candidate(
                    "protected-dead",
                    OwnerLivenessWire::Dead,
                    true,
                    now - DAY,
                    false,
                ),
                candidate(
                    "dead-old",
                    OwnerLivenessWire::NotProcess,
                    false,
                    now - (8.0 * DAY),
                    false,
                ),
                candidate(
                    "dismissed",
                    OwnerLivenessWire::Alive,
                    false,
                    now,
                    true,
                ),
                mismatch_candidate(
                    "recycled",
                    OwnerLivenessWire::Dead,
                    false,
                    now - DAY,
                    false,
                ),
            ],
        );
        let mut seen = decision.current.clone();
        seen.extend(decision.recent_terminal.iter().cloned());
        seen.extend(decision.excluded.iter().cloned());
        seen.sort();
        assert_eq!(
            seen,
            vec![
                "dead-old",
                "dead-recent",
                "dismissed",
                "live",
                "protected-dead",
                "protected-live",
                "recycled",
                "unknown",
            ]
        );
        assert_eq!(decision.current, vec!["live", "unknown", "protected-live"]);
        assert_eq!(
            decision.recent_terminal,
            vec!["dead-recent", "protected-dead"]
        );
        assert_eq!(
            decision.excluded,
            vec!["dismissed", "recycled", "dead-old"]
        );
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
