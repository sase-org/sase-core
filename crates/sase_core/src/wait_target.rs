//! Canonical wait-target vocabulary and pure binding rules.
//!
//! `#fork`, `%wait`, `sase agent wait`, and plan-gate coder options share
//! one list (`session` / `turn`) and one effective-target table. Python
//! gathers facts; this module parses values, applies the table, and
//! selects the current turn from already-classified member facts.

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::macro_input_types::{did_you_mean_suffix, suggest_closest};

/// Canonical values plus descriptions. This is the single source for the
/// `wait_target` NamedEnum catalog and later `%wait target=` suggestions.
pub const WAIT_TARGETS_WITH_DESCRIPTIONS: &[(&str, &str)] = &[
    (
        "session",
        "Wait for the whole agent session (default for bare names)",
    ),
    (
        "turn",
        "Wait for the session's current turn, captured when the wait is bound",
    ),
];

/// Stable code for an unknown wait-target spelling.
pub const WAIT_TARGET_INVALID_VALUE: &str = "wait-target-invalid-value";
/// Stable code for `target=turn` on a clan name.
pub const WAIT_TARGET_TURN_CLAN: &str = "wait-target-turn-clan";
/// Stable code for `target=turn` on a tribe reference.
pub const WAIT_TARGET_TURN_TRIBE: &str = "wait-target-turn-tribe";
/// Stable code for `target=turn` when the name is not found at bind time.
pub const WAIT_TARGET_TURN_UNRESOLVED: &str = "wait-target-turn-unresolved";

/// Explicit wait target: the whole session, or one pinned turn.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WaitTarget {
    Session,
    Turn,
}

impl WaitTarget {
    /// Canonical lowercase spelling.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Session => "session",
            Self::Turn => "turn",
        }
    }
}

/// Kind of wait reference the effective-target table keys on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WaitReferenceKind {
    BareAgent,
    ExactMember,
    Clan,
    Tribe,
    Proc,
    Unresolved,
}

/// Binding scope after the effective-target table is applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WaitScope {
    Session,
    CurrentTurn,
    Member,
    Single,
    Clan,
    Tribe,
    Name,
}

/// Result of [`effective_wait_target`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EffectiveWaitTarget {
    pub target: WaitTarget,
    pub explicit: bool,
    pub scope: WaitScope,
}

/// One session member's facts for [`select_current_wait_turn`].
///
/// `excluded` covers the waiter's own artifact and members that are
/// dependency-parked on their own waits. `pending` is still running,
/// slot-queued, or a gate/monitor turn without a settled done marker.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WaitTurnFacts {
    pub timestamp: String,
    #[serde(default)]
    pub pending: bool,
    #[serde(default)]
    pub excluded: bool,
}

/// Parse, table, and selection failures. `code` is stable; `message` is
/// the user-facing text every surface should show.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("{message}")]
pub struct WaitTargetError {
    pub code: &'static str,
    pub message: String,
}

/// Parse an exact, case-sensitive wait-target spelling.
pub fn parse_wait_target(raw: &str) -> Result<WaitTarget, WaitTargetError> {
    match raw {
        "session" => Ok(WaitTarget::Session),
        "turn" => Ok(WaitTarget::Turn),
        _ => {
            let listed: Vec<&str> = WAIT_TARGETS_WITH_DESCRIPTIONS
                .iter()
                .map(|(value, _)| *value)
                .collect();
            let suggestions = suggest_closest(
                raw,
                WAIT_TARGETS_WITH_DESCRIPTIONS
                    .iter()
                    .map(|(value, _)| *value),
            );
            Err(WaitTargetError {
                code: WAIT_TARGET_INVALID_VALUE,
                message: format!(
                    "Argument `target` expects one of {}, got `{raw}`{}",
                    listed.join(" | "),
                    did_you_mean_suffix(&suggestions)
                ),
            })
        }
    }
}

/// Apply the effective-target table for one reference.
///
/// `requested` is `None` when the caller omitted `target=`. Errors name
/// `reference` and suggest the fix.
pub fn effective_wait_target(
    requested: Option<WaitTarget>,
    kind: WaitReferenceKind,
    reference: &str,
) -> Result<EffectiveWaitTarget, WaitTargetError> {
    let explicit = requested.is_some();
    match (kind, requested) {
        (WaitReferenceKind::Clan, Some(WaitTarget::Turn)) => Err(turn_error(
            WAIT_TARGET_TURN_CLAN,
            reference,
            "a clan has no single turn. Use `target=session` or omit \
                 the target to wait for the clan generation",
        )),
        (WaitReferenceKind::Tribe, Some(WaitTarget::Turn)) => Err(turn_error(
            WAIT_TARGET_TURN_TRIBE,
            reference,
            "a tribe has no single turn. Omit the target or use \
                 `target=session` to wait for the next member",
        )),
        (WaitReferenceKind::Unresolved, Some(WaitTarget::Turn)) => {
            Err(turn_error(
                WAIT_TARGET_TURN_UNRESOLVED,
                reference,
                "turn needs an existing agent. Name a live agent or \
                 session, or omit the target to wait by name",
            ))
        }
        (WaitReferenceKind::BareAgent, Some(WaitTarget::Turn)) => {
            Ok(EffectiveWaitTarget {
                target: WaitTarget::Turn,
                explicit: true,
                scope: WaitScope::CurrentTurn,
            })
        }
        (WaitReferenceKind::BareAgent, _) => Ok(EffectiveWaitTarget {
            target: WaitTarget::Session,
            explicit,
            scope: WaitScope::Session,
        }),
        (WaitReferenceKind::ExactMember, Some(WaitTarget::Session)) => {
            Ok(EffectiveWaitTarget {
                target: WaitTarget::Session,
                explicit: true,
                scope: WaitScope::Session,
            })
        }
        (WaitReferenceKind::ExactMember, requested) => {
            Ok(EffectiveWaitTarget {
                target: WaitTarget::Turn,
                explicit: requested.is_some(),
                scope: WaitScope::Member,
            })
        }
        (WaitReferenceKind::Clan, _) => Ok(EffectiveWaitTarget {
            target: WaitTarget::Session,
            explicit,
            scope: WaitScope::Clan,
        }),
        (WaitReferenceKind::Tribe, _) => Ok(EffectiveWaitTarget {
            target: WaitTarget::Session,
            explicit,
            scope: WaitScope::Tribe,
        }),
        (WaitReferenceKind::Proc, requested) => Ok(EffectiveWaitTarget {
            target: requested.unwrap_or(WaitTarget::Session),
            explicit,
            scope: WaitScope::Single,
        }),
        (WaitReferenceKind::Unresolved, _) => Ok(EffectiveWaitTarget {
            target: WaitTarget::Session,
            explicit,
            scope: WaitScope::Name,
        }),
    }
}

/// Select the current turn from already-classified member facts.
///
/// Sorts by timestamp itself and returns an index into `members`. The
/// current turn is the newest remaining pending member; if none is
/// pending, it is the newest remaining member. Empty or fully excluded
/// input yields `None`.
pub fn select_current_wait_turn(members: &[WaitTurnFacts]) -> Option<usize> {
    let remaining: Vec<usize> = members
        .iter()
        .enumerate()
        .filter(|(_, member)| !member.excluded)
        .map(|(index, _)| index)
        .collect();
    if remaining.is_empty() {
        return None;
    }
    let pending: Vec<usize> = remaining
        .iter()
        .copied()
        .filter(|&index| members[index].pending)
        .collect();
    let pool = if pending.is_empty() {
        remaining
    } else {
        pending
    };
    pool.into_iter()
        .max_by_key(|&index| (members[index].timestamp.as_str(), index))
}

fn turn_error(
    code: &'static str,
    reference: &str,
    suggestion: &str,
) -> WaitTargetError {
    WaitTargetError {
        code,
        message: format!(
            "`target=turn` cannot wait on `{reference}`: {suggestion}."
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn facts(rows: &[(&str, bool, bool)]) -> Vec<WaitTurnFacts> {
        rows.iter()
            .map(|(timestamp, pending, excluded)| WaitTurnFacts {
                timestamp: (*timestamp).to_string(),
                pending: *pending,
                excluded: *excluded,
            })
            .collect()
    }

    fn table(
        requested: Option<WaitTarget>,
        kind: WaitReferenceKind,
    ) -> EffectiveWaitTarget {
        effective_wait_target(requested, kind, "16h").unwrap()
    }

    #[test]
    fn parses_exact_canonical_values() {
        assert_eq!(parse_wait_target("session").unwrap(), WaitTarget::Session);
        assert_eq!(parse_wait_target("turn").unwrap(), WaitTarget::Turn);
        assert_eq!(WaitTarget::Session.as_str(), "session");
        assert_eq!(WaitTarget::Turn.as_str(), "turn");
    }

    #[test]
    fn rejects_unknown_values_with_did_you_mean() {
        let error = parse_wait_target("Session").unwrap_err();
        assert_eq!(error.code, WAIT_TARGET_INVALID_VALUE);
        assert!(
            error.message.contains("Argument `target` expects one of session | turn, got `Session`"),
            "{}",
            error.message
        );
        assert!(
            error.message.contains("did you mean `session`"),
            "{}",
            error.message
        );

        let error = parse_wait_target("turnn").unwrap_err();
        assert_eq!(error.code, WAIT_TARGET_INVALID_VALUE);
        assert!(
            error.message.contains("did you mean `turn`"),
            "{}",
            error.message
        );

        let error = parse_wait_target("").unwrap_err();
        assert_eq!(error.code, WAIT_TARGET_INVALID_VALUE);
        assert!(!error.message.contains("did you mean"));
    }

    #[test]
    fn effective_table_covers_every_cell() {
        let cases = [
            (
                None,
                WaitReferenceKind::BareAgent,
                WaitTarget::Session,
                false,
                WaitScope::Session,
            ),
            (
                Some(WaitTarget::Session),
                WaitReferenceKind::BareAgent,
                WaitTarget::Session,
                true,
                WaitScope::Session,
            ),
            (
                Some(WaitTarget::Turn),
                WaitReferenceKind::BareAgent,
                WaitTarget::Turn,
                true,
                WaitScope::CurrentTurn,
            ),
            (
                None,
                WaitReferenceKind::ExactMember,
                WaitTarget::Turn,
                false,
                WaitScope::Member,
            ),
            (
                Some(WaitTarget::Session),
                WaitReferenceKind::ExactMember,
                WaitTarget::Session,
                true,
                WaitScope::Session,
            ),
            (
                Some(WaitTarget::Turn),
                WaitReferenceKind::ExactMember,
                WaitTarget::Turn,
                true,
                WaitScope::Member,
            ),
            (
                None,
                WaitReferenceKind::Clan,
                WaitTarget::Session,
                false,
                WaitScope::Clan,
            ),
            (
                Some(WaitTarget::Session),
                WaitReferenceKind::Clan,
                WaitTarget::Session,
                true,
                WaitScope::Clan,
            ),
            (
                None,
                WaitReferenceKind::Tribe,
                WaitTarget::Session,
                false,
                WaitScope::Tribe,
            ),
            (
                Some(WaitTarget::Session),
                WaitReferenceKind::Tribe,
                WaitTarget::Session,
                true,
                WaitScope::Tribe,
            ),
            (
                None,
                WaitReferenceKind::Proc,
                WaitTarget::Session,
                false,
                WaitScope::Single,
            ),
            (
                Some(WaitTarget::Session),
                WaitReferenceKind::Proc,
                WaitTarget::Session,
                true,
                WaitScope::Single,
            ),
            (
                Some(WaitTarget::Turn),
                WaitReferenceKind::Proc,
                WaitTarget::Turn,
                true,
                WaitScope::Single,
            ),
            (
                None,
                WaitReferenceKind::Unresolved,
                WaitTarget::Session,
                false,
                WaitScope::Name,
            ),
            (
                Some(WaitTarget::Session),
                WaitReferenceKind::Unresolved,
                WaitTarget::Session,
                true,
                WaitScope::Name,
            ),
        ];
        for (requested, kind, target, explicit, scope) in cases {
            let got = table(requested, kind);
            assert_eq!(got.target, target, "{kind:?} {requested:?}");
            assert_eq!(got.explicit, explicit, "{kind:?} {requested:?}");
            assert_eq!(got.scope, scope, "{kind:?} {requested:?}");
        }
    }

    #[test]
    fn turn_on_clan_tribe_and_unresolved_is_an_error() {
        let clan = effective_wait_target(
            Some(WaitTarget::Turn),
            WaitReferenceKind::Clan,
            "builders",
        )
        .unwrap_err();
        assert_eq!(clan.code, WAIT_TARGET_TURN_CLAN);
        assert!(clan.message.contains("`builders`"), "{}", clan.message);
        assert!(clan.message.contains("clan"), "{}", clan.message);
        assert!(clan.message.contains("target=session"), "{}", clan.message);

        let tribe = effective_wait_target(
            Some(WaitTarget::Turn),
            WaitReferenceKind::Tribe,
            "@job",
        )
        .unwrap_err();
        assert_eq!(tribe.code, WAIT_TARGET_TURN_TRIBE);
        assert!(tribe.message.contains("`@job`"), "{}", tribe.message);

        let unresolved = effective_wait_target(
            Some(WaitTarget::Turn),
            WaitReferenceKind::Unresolved,
            "missing",
        )
        .unwrap_err();
        assert_eq!(unresolved.code, WAIT_TARGET_TURN_UNRESOLVED);
        assert!(
            unresolved.message.contains("`missing`"),
            "{}",
            unresolved.message
        );
        assert!(
            unresolved.message.contains("existing agent"),
            "{}",
            unresolved.message
        );
    }

    #[test]
    fn selects_newest_pending_remaining_member() {
        let members = facts(&[
            ("t1", false, false),
            ("t3", true, false),
            ("t2", true, false),
        ]);
        assert_eq!(select_current_wait_turn(&members), Some(1));
    }

    #[test]
    fn all_members_finished_picks_newest_remaining() {
        let members = facts(&[
            ("t1", false, false),
            ("t3", false, false),
            ("t2", false, false),
        ]);
        assert_eq!(select_current_wait_turn(&members), Some(1));
    }

    #[test]
    fn pending_gate_wins_over_newer_finished_member() {
        let members = facts(&[
            ("t1", false, false),
            ("t2", true, false),
            ("t3", false, false),
        ]);
        assert_eq!(select_current_wait_turn(&members), Some(1));
    }

    #[test]
    fn excluded_waiter_is_skipped() {
        let members = facts(&[
            ("t1", true, false),
            ("t3", true, true),
            ("t2", false, false),
        ]);
        assert_eq!(select_current_wait_turn(&members), Some(0));
    }

    #[test]
    fn dependency_parked_sibling_is_skipped() {
        let members = facts(&[
            ("t1", false, false),
            ("t3", true, true),
            ("t2", true, false),
        ]);
        assert_eq!(select_current_wait_turn(&members), Some(2));
    }

    #[test]
    fn empty_or_fully_excluded_input_is_none() {
        assert_eq!(select_current_wait_turn(&[]), None);
        let members = facts(&[("t1", true, true), ("t2", false, true)]);
        assert_eq!(select_current_wait_turn(&members), None);
    }

    #[test]
    fn equal_timestamps_break_ties_by_input_index() {
        let members = facts(&[("t1", true, false), ("t1", true, false)]);
        assert_eq!(select_current_wait_turn(&members), Some(1));
    }
}
