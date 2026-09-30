//! Legacy origin heuristic for prompt rows without a recorded origin.
//!
//! Rows with `origin == "generated"` are dropped by the compiler; rows with
//! no origin pass through this heuristic, which marks machine-generated
//! prompts from their template markers. Every marker is a named const with a
//! test, inventoried from the sase repo:
//!
//! - `#bd/work_phase_bead`, `#bd/work_task`, `#bd/land_epic` from
//!   `src/sase/agent/launch_cwd_bead_work.py` (every bead-work segment
//!   references exactly one bead xprompt)
//! - `%id(` from `src/sase/xprompts/tribe.md` (`%id(tribe=...)`) and the
//!   bead-work `%id` render assertions
//! - `%wait(` from `src/sase/xprompts/t.md` (`%wait(time=...)`)
//! - `SASE single-turn instructions for` from
//!   `src/sase/llm_provider/muse.py` (also codex/claude variants)
//! - `clan=` with `%id(` from `src/sase/xprompts/skills/sase_run.md`
//!   (`%id(worker, clan=research)`)
//! - `tribe=chop`, `%tribe:chop`, `%group:chop` (plus the `job` spellings of
//!   each; `job` is the public alias of the `chop` tribe) from routine member
//!   prompts (`%id(docs-agent, tribe=chop)`,
//!   `%clan(toobig-3j, tribe=chop, ...)`). Other `tribe=` values never match,
//!   because humans assign tribes by hand.

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
/// Single-turn agent instructions header stamped onto generated prompts.
pub const MARKER_SINGLE_TURN: &str = "SASE single-turn instructions for";
/// Agent clan declaration used by generated member prompts.
pub const MARKER_CLAN: &str = "clan=";
/// Routine-tribe binding rendered into every AXE automation member prompt
/// (`%id(docs-agent, tribe=chop)`, `%clan(toobig-3j, tribe=chop, ...)`).
pub const MARKER_ROUTINE_TRIBE_CHOP: &str = "tribe=chop";
/// Public-alias spelling of the routine-tribe binding (`job` canonicalizes
/// to the `chop` tribe).
pub const MARKER_ROUTINE_TRIBE_JOB: &str = "tribe=job";
/// Legacy routine-tribe directive form still present in the live store.
pub const MARKER_ROUTINE_TRIBE_DIRECTIVE_CHOP: &str = "%tribe:chop";
/// Public-alias spelling of the legacy routine-tribe directive.
pub const MARKER_ROUTINE_TRIBE_DIRECTIVE_JOB: &str = "%tribe:job";
/// Legacy routine-group directive form still present in the live store.
pub const MARKER_ROUTINE_GROUP_DIRECTIVE_CHOP: &str = "%group:chop";
/// Public-alias spelling of the legacy routine-group directive.
pub const MARKER_ROUTINE_GROUP_DIRECTIVE_JOB: &str = "%group:job";

/// True when `needle` occurs in `haystack` at a tribe-name boundary.
///
/// Tribe names admit `[A-Za-z0-9_.-]`, so a plain substring search for
/// `tribe=chop` would also match a hand-assigned `tribe=chopper`. The match
/// only counts when the next character cannot extend the tribe name.
fn contains_tribe_marker(haystack: &str, needle: &str) -> bool {
    haystack.match_indices(needle).any(|(start, _)| {
        !haystack[start + needle.len()..]
            .chars()
            .next()
            .is_some_and(|next| {
                next.is_ascii_alphanumeric()
                    || next == '_'
                    || next == '.'
                    || next == '-'
            })
    })
}

/// True when a prompt row with no recorded origin looks machine-generated.
pub fn looks_generated(text: &str) -> bool {
    if text.contains(MARKER_WORK_PHASE_BEAD) {
        return true;
    }
    if text.contains(MARKER_WORK_TASK) {
        return true;
    }
    if text.contains(MARKER_LAND_EPIC) {
        return true;
    }
    if text.contains(MARKER_BEAD_PREFIX) && text.contains(MARKER_ID_DIRECTIVE) {
        return true;
    }
    if text.trim_start().starts_with(MARKER_WAIT_DIRECTIVE) {
        return true;
    }
    if text.contains(MARKER_SINGLE_TURN) {
        return true;
    }
    // Clan member prompts carry an identity directive with a clan binding,
    // e.g. `%id(worker, clan=research)`.
    if text.contains(MARKER_ID_DIRECTIVE) && text.contains(MARKER_CLAN) {
        return true;
    }
    // Routine members render the automation tribe inline. Any other
    // hand-assigned `tribe=` value (e.g. `%id(worker, tribe=quality)`)
    // stays typed.
    if contains_tribe_marker(text, MARKER_ROUTINE_TRIBE_CHOP) {
        return true;
    }
    if contains_tribe_marker(text, MARKER_ROUTINE_TRIBE_JOB) {
        return true;
    }
    if contains_tribe_marker(text, MARKER_ROUTINE_TRIBE_DIRECTIVE_CHOP) {
        return true;
    }
    if contains_tribe_marker(text, MARKER_ROUTINE_TRIBE_DIRECTIVE_JOB) {
        return true;
    }
    if contains_tribe_marker(text, MARKER_ROUTINE_GROUP_DIRECTIVE_CHOP) {
        return true;
    }
    if contains_tribe_marker(text, MARKER_ROUTINE_GROUP_DIRECTIVE_JOB) {
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
    fn work_task_token_is_generated() {
        assert!(looks_generated("Do the work for #bd/work_task now"));
    }

    #[test]
    fn land_epic_token_is_generated() {
        assert!(looks_generated("Land this #bd/land_epic now"));
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
    fn hand_assigned_tribe_is_typed() {
        assert!(!looks_generated("%id(worker, tribe=quality) do this"));
        assert!(!looks_generated("%clan(review, tribe=quality) do this"));
    }

    #[test]
    fn routine_tribe_binding_is_generated() {
        assert!(looks_generated("%id(docs-agent, tribe=chop) do this"));
        assert!(looks_generated(
            "%clan(toobig-3j, tribe=chop, summary=[[[bold]Large modules[/bold]]])\nSplit."
        ));
        assert!(looks_generated("%id(worker, tribe=job) do this"));
        assert!(looks_generated("%clan(review-0, tribe=job)\nReview."));
    }

    #[test]
    fn routine_tribe_prefix_is_typed() {
        // `tribe=chopper` is a distinct hand-assigned tribe, not the
        // routine tribe.
        assert!(!looks_generated("%id(worker, tribe=chopper) do this"));
        assert!(!looks_generated("%id(worker, tribe=jobless) do this"));
    }

    #[test]
    fn legacy_routine_directives_are_generated() {
        for text in [
            "%tribe:chop\nDo work",
            "%tribe:job\nDo work",
            "%group:chop\nDo work",
            "%group:job\nDo work",
        ] {
            assert!(looks_generated(text), "text={text:?}");
        }
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
