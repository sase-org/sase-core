//! Legacy origin heuristic for prompt rows without a recorded origin.
//!
//! Rows with `origin == "generated"` are dropped by the compiler; rows with
//! no origin pass through this heuristic, which marks machine-generated
//! prompts from their template markers. Every marker is a named const with a
//! test, inventoried from the sase repo's `src/sase/xprompts/` templates.

/// Bead-work segment reference: every `sase bead work` segment references
/// exactly one bead xprompt through this marker.
pub const MARKER_WORK_PHASE_BEAD: &str = "#bd/work_phase_bead";
/// Task-work segment reference.
pub const MARKER_WORK_TASK: &str = "#bd/work_task";
/// Epic landing segment reference.
pub const MARKER_LAND_EPIC: &str = "#bd/land_epic";
/// Generic bead xprompt reference prefix.
pub const MARKER_BEAD_PREFIX: &str = "#bd/";
/// Agent identity directive, present alongside `#bd/…` in generated prompts.
pub const MARKER_ID_DIRECTIVE: &str = "%id(";
/// Deferred launch directive; a generated prompt starts with this.
pub const MARKER_WAIT_DIRECTIVE: &str = "%wait(";
/// Multi-prompt swarm expansion marker from the xprompt swarm templates.
pub const MARKER_SWARM: &str = "%swarm(";
/// Lead template marker from the lead orchestration templates.
pub const MARKER_LEAD: &str = "%lead(";
/// Single-turn agent instructions header stamped onto generated prompts.
pub const MARKER_SINGLE_TURN: &str = "SASE single-turn instructions for";
/// Agent clan declaration used by generated member prompts.
pub const MARKER_CLAN: &str = "clan=";

/// True when a prompt row with no recorded origin looks machine-generated.
pub fn looks_generated(text: &str) -> bool {
    if text.contains(MARKER_WORK_PHASE_BEAD) {
        return true;
    }
    if text.contains(MARKER_BEAD_PREFIX) && text.contains(MARKER_ID_DIRECTIVE) {
        return true;
    }
    if text.trim_start().starts_with(MARKER_WAIT_DIRECTIVE) {
        return true;
    }
    if text.contains(MARKER_SWARM) || text.contains(MARKER_LEAD) {
        return true;
    }
    if text.contains(MARKER_SINGLE_TURN) {
        return true;
    }
    // Clan member prompts carry an identity directive with a clan binding,
    // e.g. `%id(worker, tribe=quality)` or `%id(worker, clan=research)`.
    if text.contains(MARKER_ID_DIRECTIVE) && text.contains(MARKER_CLAN) {
        return true;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn work_phase_bead_token_is_generated() {
        assert!(looks_generated("Do the work for #bd/work_phase_bead now"));
    }

    #[test]
    fn bead_token_with_id_directive_is_generated() {
        assert!(looks_generated(
            "Review #bd/work_task with %id(worker, tribe=quality)"
        ));
        assert!(looks_generated(
            "Land this #bd/land_epic via %id(land, session=main)"
        ));
    }

    #[test]
    fn bead_token_without_id_directive_is_typed() {
        assert!(!looks_generated(
            "Can you help me file #bd/lunch-plans for Friday"
        ));
    }

    #[test]
    fn leading_wait_directive_is_generated() {
        assert!(looks_generated("%wait(time=60)\nDo the thing"));
        assert!(looks_generated("  %wait(time=60) then continue"));
    }

    #[test]
    fn mid_text_wait_is_not_enough_alone() {
        assert!(!looks_generated("I waited %wait(around) for lunch"));
    }

    #[test]
    fn swarm_and_lead_markers_are_generated() {
        assert!(looks_generated("expand %swarm(workers=4) now"));
        assert!(looks_generated("run %lead(plan) for this"));
    }

    #[test]
    fn single_turn_header_is_generated() {
        assert!(looks_generated(
            "SASE single-turn instructions for Muse Code: one turn"
        ));
    }

    #[test]
    fn clan_identity_is_generated() {
        assert!(looks_generated("%id(worker, clan=research) do this"));
    }

    #[test]
    fn plain_prose_is_typed() {
        for text in [
            "Can you help me implement it now",
            "Fix the parser and review the tests",
            "#gh:sase what changed",
            "%m:opus explain this",
        ] {
            assert!(!looks_generated(text), "text={text:?}");
        }
    }
}
