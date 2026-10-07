//! Epic-follow reducer for `%wait(..., for_epic=)`.
//!
//! A pure facts-to-decision function. Python collects per-member facts over
//! the wait-dependency index; this module maps them to
//! `agent`/`none`/`launching`/`following`/`blocked` with the deadlock guard
//! and the cycle hook. It performs no I/O.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

fn default_launching_grace_seconds() -> f64 {
    600.0
}

fn default_launch_settle_seconds() -> f64 {
    120.0
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WaitEpicFollowMemberFactsWire {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub artifact_dir: String,
    #[serde(default)]
    pub recorded_epic_ids: Vec<String>,
    #[serde(default)]
    pub attributed_epic_ids: Vec<String>,
    #[serde(default)]
    pub legacy_epic_bead_id: Option<String>,
    #[serde(default)]
    pub is_epic_worker: bool,
    #[serde(default)]
    pub launch_reserved: bool,
    #[serde(default)]
    pub launch_argv_present: bool,
    #[serde(default)]
    pub launch_in_flight: bool,
    #[serde(default)]
    pub launch_reserved_age_seconds: Option<f64>,
    #[serde(default)]
    pub member_dismissed: bool,
    #[serde(default)]
    pub resume_command: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WaitEpicFollowTargetFactsWire {
    #[serde(default)]
    pub target: String,
    #[serde(default)]
    pub agent_resolved: bool,
    #[serde(default)]
    pub previous_state: Option<String>,
    #[serde(default)]
    pub previous_since: Option<f64>,
    #[serde(default)]
    pub cycle_epic_ids: Vec<String>,
    #[serde(default)]
    pub members: Vec<WaitEpicFollowMemberFactsWire>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WaitEpicFollowInputWire {
    #[serde(default)]
    pub waiter_own_bead_ids: Vec<String>,
    #[serde(default)]
    pub now: f64,
    #[serde(default = "default_launching_grace_seconds")]
    pub launching_grace_seconds: f64,
    #[serde(default = "default_launch_settle_seconds")]
    pub launch_settle_seconds: f64,
    #[serde(default)]
    pub targets: Vec<WaitEpicFollowTargetFactsWire>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WaitEpicFollowDecisionWire {
    #[serde(default)]
    pub target: String,
    #[serde(default)]
    pub state: String,
    #[serde(default)]
    pub epic_ids: Vec<String>,
    #[serde(default)]
    pub members: Vec<String>,
    #[serde(default)]
    pub since: f64,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub detail: Option<String>,
    #[serde(default)]
    pub resume_command: Option<String>,
    #[serde(default)]
    pub skipped_epic_ids: Vec<String>,
    #[serde(default)]
    pub launching_overdue: bool,
}

/// Map per-member facts to one follow decision per armed target.
///
/// Rule order follows the epic plan: unresolved agent first, then the
/// deduplicated epic union with the deadlock guard, then the cycle hook,
/// then launching, then the two blocked-reservation cases, else none.
/// `since` is kept when the new state equals the previous state.
pub fn wait_epic_follow_reduce(
    input: &WaitEpicFollowInputWire,
) -> Vec<WaitEpicFollowDecisionWire> {
    input
        .targets
        .iter()
        .map(|target| reduce_target(input, target))
        .collect()
}

fn normalize_state(value: Option<&str>) -> Option<String> {
    value
        .map(|state| state.trim().to_lowercase())
        .filter(|state| {
            matches!(
                state.as_str(),
                "agent" | "none" | "launching" | "following" | "blocked"
            )
        })
}

fn since_for(
    now: f64,
    new_state: &str,
    previous_state: Option<&str>,
    previous_since: Option<f64>,
) -> f64 {
    let normalized = normalize_state(previous_state);
    if normalized.as_deref() == Some(new_state) {
        previous_since.unwrap_or(now)
    } else {
        now
    }
}

fn dedup_union(values: Vec<String>) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut ordered = Vec::new();
    for value in values {
        let trimmed = value.trim().to_string();
        if trimmed.is_empty() || !seen.insert(trimmed.clone()) {
            continue;
        }
        ordered.push(trimmed);
    }
    ordered
}

fn member_epics(member: &WaitEpicFollowMemberFactsWire) -> Vec<String> {
    dedup_union(
        member
            .recorded_epic_ids
            .iter()
            .chain(member.attributed_epic_ids.iter())
            .cloned()
            .collect(),
    )
}

fn reduce_target(
    input: &WaitEpicFollowInputWire,
    target: &WaitEpicFollowTargetFactsWire,
) -> WaitEpicFollowDecisionWire {
    let previous = normalize_state(target.previous_state.as_deref());
    if !target.agent_resolved {
        let state = "agent".to_string();
        return WaitEpicFollowDecisionWire {
            target: target.target.clone(),
            since: since_for(
                input.now,
                &state,
                target.previous_state.as_deref(),
                target.previous_since,
            ),
            state,
            epic_ids: Vec::new(),
            members: Vec::new(),
            reason: None,
            detail: None,
            resume_command: None,
            skipped_epic_ids: Vec::new(),
            launching_overdue: false,
        };
    }

    let recorded_union: Vec<String> =
        dedup_union(target.members.iter().flat_map(member_epics).collect());
    let union_from_recorded = !recorded_union.is_empty();
    let mut union = recorded_union;
    if union.is_empty() {
        union = dedup_union(
            target
                .members
                .iter()
                .filter(|member| !member.is_epic_worker)
                .filter_map(|member| member.legacy_epic_bead_id.clone())
                .collect(),
        );
    }

    let mut skipped_epic_ids = Vec::new();
    let mut filtered = Vec::new();
    for epic in union {
        if is_own_bead(&input.waiter_own_bead_ids, &epic) {
            if !skipped_epic_ids.contains(&epic) {
                skipped_epic_ids.push(epic);
            }
            continue;
        }
        filtered.push(epic);
    }

    if !filtered.is_empty() {
        let cycle_hit = filtered.iter().any(|epic| {
            target.cycle_epic_ids.iter().any(|cycle| cycle == epic)
        });
        if cycle_hit {
            let state = "blocked".to_string();
            let contributors =
                contributors_for(target, &filtered, union_from_recorded);
            return WaitEpicFollowDecisionWire {
                target: target.target.clone(),
                since: since_for(
                    input.now,
                    &state,
                    target.previous_state.as_deref(),
                    target.previous_since,
                ),
                state,
                epic_ids: filtered,
                members: contributors,
                reason: Some("cycle".to_string()),
                detail: Some(format!(
                    "epic follow for '{}' would wait on the waiter (cycle)",
                    target.target
                )),
                resume_command: None,
                skipped_epic_ids,
                launching_overdue: false,
            };
        }
        let state = "following".to_string();
        let contributors =
            contributors_for(target, &filtered, union_from_recorded);
        let _ = previous;
        return WaitEpicFollowDecisionWire {
            target: target.target.clone(),
            since: since_for(
                input.now,
                &state,
                target.previous_state.as_deref(),
                target.previous_since,
            ),
            state,
            epic_ids: filtered,
            members: contributors,
            reason: None,
            detail: None,
            resume_command: None,
            skipped_epic_ids,
            launching_overdue: false,
        };
    }

    let reserved: Vec<&WaitEpicFollowMemberFactsWire> = target
        .members
        .iter()
        .filter(|member| member.launch_reserved)
        .collect();

    let launching = reserved.iter().any(|member| {
        member.launch_in_flight
            || member
                .launch_reserved_age_seconds
                .is_some_and(|age| age < input.launch_settle_seconds)
    });
    if launching {
        let state = "launching".to_string();
        let since = since_for(
            input.now,
            &state,
            target.previous_state.as_deref(),
            target.previous_since,
        );
        let launching_overdue =
            input.now - since > input.launching_grace_seconds;
        let members = reserved
            .iter()
            .map(|member| member.name.clone())
            .filter(|name| !name.is_empty())
            .collect::<Vec<_>>();
        return WaitEpicFollowDecisionWire {
            target: target.target.clone(),
            state,
            epic_ids: Vec::new(),
            members,
            since,
            reason: None,
            detail: None,
            resume_command: None,
            skipped_epic_ids,
            launching_overdue,
        };
    }

    if let Some(dismissed) =
        reserved.iter().find(|member| member.member_dismissed)
    {
        let state = "blocked".to_string();
        return WaitEpicFollowDecisionWire {
            target: target.target.clone(),
            since: since_for(
                input.now,
                &state,
                target.previous_state.as_deref(),
                target.previous_since,
            ),
            state,
            epic_ids: Vec::new(),
            members: vec![dismissed.name.clone()]
                .into_iter()
                .filter(|name| !name.is_empty())
                .collect(),
            reason: Some("target_dismissed_during_launch".to_string()),
            detail: Some(format!(
                "target '{}' was dismissed during its epic launch",
                target.target
            )),
            resume_command: dismissed.resume_command.clone(),
            skipped_epic_ids,
            launching_overdue: false,
        };
    }

    if let Some(first_reserved) = reserved.first() {
        let state = "blocked".to_string();
        let any_argv = reserved.iter().any(|member| member.launch_argv_present);
        let reason = if any_argv {
            "launch_ended_without_epic"
        } else {
            "launch_skipped"
        }
        .to_string();
        let detail = if any_argv {
            format!("epic launch for '{}' ended without an epic", target.target)
        } else {
            format!(
                "epic launch for '{}' was skipped (no epic_launch_argv.json)",
                target.target
            )
        };
        let resume_command = reserved
            .iter()
            .filter_map(|member| member.resume_command.clone())
            .next()
            .or_else(|| first_reserved.resume_command.clone());
        let members = reserved
            .iter()
            .map(|member| member.name.clone())
            .filter(|name| !name.is_empty())
            .collect::<Vec<_>>();
        return WaitEpicFollowDecisionWire {
            target: target.target.clone(),
            since: since_for(
                input.now,
                &state,
                target.previous_state.as_deref(),
                target.previous_since,
            ),
            state,
            epic_ids: Vec::new(),
            members,
            reason: Some(reason),
            detail: Some(detail),
            resume_command,
            skipped_epic_ids,
            launching_overdue: false,
        };
    }

    let state = "none".to_string();
    let detail = if skipped_epic_ids.is_empty() {
        None
    } else {
        Some(format!(
            "skipped epics already contain the waiter: {}",
            skipped_epic_ids.join(", ")
        ))
    };
    WaitEpicFollowDecisionWire {
        target: target.target.clone(),
        since: since_for(
            input.now,
            &state,
            target.previous_state.as_deref(),
            target.previous_since,
        ),
        state,
        epic_ids: Vec::new(),
        members: Vec::new(),
        reason: None,
        detail,
        resume_command: None,
        skipped_epic_ids,
        launching_overdue: false,
    }
}

fn contributors_for(
    target: &WaitEpicFollowTargetFactsWire,
    epics: &[String],
    union_from_recorded: bool,
) -> Vec<String> {
    let set: BTreeSet<&str> = epics.iter().map(String::as_str).collect();
    let mut names = Vec::new();
    for member in &target.members {
        let member_set: Vec<String> = if union_from_recorded {
            member_epics(member)
        } else if member.is_epic_worker {
            Vec::new()
        } else {
            member
                .legacy_epic_bead_id
                .clone()
                .map(|legacy| dedup_union(vec![legacy]))
                .unwrap_or_default()
        };
        if member_set.iter().any(|epic| set.contains(epic.as_str()))
            && !member.name.is_empty()
            && !names.contains(&member.name)
        {
            names.push(member.name.clone());
        }
    }
    names
}

fn is_own_bead(own_beads: &[String], epic: &str) -> bool {
    let epic = epic.trim();
    if epic.is_empty() {
        return false;
    }
    own_beads.iter().any(|own| {
        let own = own.trim();
        !own.is_empty() && (own == epic || own.starts_with(&format!("{epic}.")))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn member(name: &str) -> WaitEpicFollowMemberFactsWire {
        WaitEpicFollowMemberFactsWire {
            name: name.to_string(),
            artifact_dir: format!("/artifacts/{name}"),
            ..WaitEpicFollowMemberFactsWire::default()
        }
    }

    fn input(
        targets: Vec<WaitEpicFollowTargetFactsWire>,
    ) -> WaitEpicFollowInputWire {
        WaitEpicFollowInputWire {
            waiter_own_bead_ids: Vec::new(),
            now: 1_800_000_000.0,
            launching_grace_seconds: 600.0,
            launch_settle_seconds: 120.0,
            targets,
        }
    }

    fn target(
        name: &str,
        resolved: bool,
        members: Vec<WaitEpicFollowMemberFactsWire>,
    ) -> WaitEpicFollowTargetFactsWire {
        WaitEpicFollowTargetFactsWire {
            target: name.to_string(),
            agent_resolved: resolved,
            previous_state: None,
            previous_since: None,
            cycle_epic_ids: Vec::new(),
            members,
        }
    }

    #[test]
    fn unresolved_agent_stays_agent() {
        let decisions = wait_epic_follow_reduce(&input(vec![target(
            "planner",
            false,
            vec![member("planner")],
        )]));
        assert_eq!(decisions.len(), 1);
        assert_eq!(decisions[0].state, "agent");
        assert!(decisions[0].epic_ids.is_empty());
        assert_eq!(decisions[0].since, 1_800_000_000.0);
    }

    #[test]
    fn recorded_epics_follow_with_contributors() {
        let mut first = member("planner");
        first.recorded_epic_ids = vec!["sase-7k".to_string()];
        let mut second = member("planner--code");
        second.recorded_epic_ids =
            vec!["sase-7k".to_string(), "sase-7m".to_string()];
        let decisions = wait_epic_follow_reduce(&input(vec![target(
            "planner",
            true,
            vec![first, second],
        )]));
        assert_eq!(decisions[0].state, "following");
        assert_eq!(decisions[0].epic_ids, vec!["sase-7k", "sase-7m"]);
        assert_eq!(decisions[0].members, vec!["planner", "planner--code"]);
    }

    #[test]
    fn attributed_epics_join_the_union() {
        let mut solo = member("planner");
        solo.attributed_epic_ids = vec!["sase-7k".to_string()];
        let decisions = wait_epic_follow_reduce(&input(vec![target(
            "planner",
            true,
            vec![solo],
        )]));
        assert_eq!(decisions[0].state, "following");
        assert_eq!(decisions[0].epic_ids, vec!["sase-7k"]);
    }

    #[test]
    fn legacy_fallback_ignores_workers() {
        let mut worker = member("phase-1");
        worker.is_epic_worker = true;
        worker.legacy_epic_bead_id = Some("sase-7k".to_string());
        let decisions = wait_epic_follow_reduce(&input(vec![target(
            "phase-1",
            true,
            vec![worker],
        )]));
        assert_eq!(decisions[0].state, "none");
    }

    #[test]
    fn legacy_planner_follows() {
        let mut planner = member("planner");
        planner.legacy_epic_bead_id = Some("sase-7k".to_string());
        let decisions = wait_epic_follow_reduce(&input(vec![target(
            "planner",
            true,
            vec![planner],
        )]));
        assert_eq!(decisions[0].state, "following");
        assert_eq!(decisions[0].epic_ids, vec!["sase-7k"]);
    }

    #[test]
    fn deadlock_guard_skips_own_epic_with_prefix() {
        let mut base = input(vec![{
            let mut solo = member("phase-2");
            solo.recorded_epic_ids = vec!["sase-7k".to_string()];
            target("phase-2", true, vec![solo])
        }]);
        base.waiter_own_bead_ids = vec!["sase-7k.2".to_string()];
        let decisions = wait_epic_follow_reduce(&base);
        assert_eq!(decisions[0].state, "none");
        assert_eq!(decisions[0].skipped_epic_ids, vec!["sase-7k"]);
        assert!(decisions[0].detail.is_some());
    }

    #[test]
    fn exact_own_bead_is_skipped() {
        let mut base = input(vec![{
            let mut solo = member("phase-1");
            solo.recorded_epic_ids = vec!["sase-7k".to_string()];
            target("phase-1", true, vec![solo])
        }]);
        base.waiter_own_bead_ids = vec!["sase-7k".to_string()];
        let decisions = wait_epic_follow_reduce(&base);
        assert_eq!(decisions[0].state, "none");
        assert_eq!(decisions[0].skipped_epic_ids, vec!["sase-7k"]);
    }

    #[test]
    fn cycle_hook_blocks() {
        let mut solo = member("planner");
        solo.recorded_epic_ids = vec!["sase-7k".to_string()];
        let mut facts = target("planner", true, vec![solo]);
        facts.cycle_epic_ids = vec!["sase-7k".to_string()];
        let decisions = wait_epic_follow_reduce(&input(vec![facts]));
        assert_eq!(decisions[0].state, "blocked");
        assert_eq!(decisions[0].reason.as_deref(), Some("cycle"));
        assert_eq!(decisions[0].epic_ids, vec!["sase-7k"]);
    }

    #[test]
    fn launching_when_in_flight() {
        let mut solo = member("planner");
        solo.launch_reserved = true;
        solo.launch_in_flight = true;
        let decisions = wait_epic_follow_reduce(&input(vec![target(
            "planner",
            true,
            vec![solo],
        )]));
        assert_eq!(decisions[0].state, "launching");
        assert!(!decisions[0].launching_overdue);
    }

    #[test]
    fn launching_within_settle_window() {
        let mut solo = member("planner");
        solo.launch_reserved = true;
        solo.launch_reserved_age_seconds = Some(30.0);
        let decisions = wait_epic_follow_reduce(&input(vec![target(
            "planner",
            true,
            vec![solo],
        )]));
        assert_eq!(decisions[0].state, "launching");
    }

    #[test]
    fn launching_overdue_after_grace() {
        let mut solo = member("planner");
        solo.launch_reserved = true;
        solo.launch_in_flight = true;
        let mut facts = target("planner", true, vec![solo]);
        facts.previous_state = Some("launching".to_string());
        facts.previous_since = Some(1_800_000_000.0 - 601.0);
        let mut args = input(vec![facts]);
        args.now = 1_800_000_000.0;
        let decisions = wait_epic_follow_reduce(&args);
        assert_eq!(decisions[0].state, "launching");
        assert!(decisions[0].launching_overdue);
        assert_eq!(decisions[0].since, 1_800_000_000.0 - 601.0);
    }

    #[test]
    fn dismissed_reservation_blocks() {
        let mut solo = member("planner");
        solo.launch_reserved = true;
        solo.member_dismissed = true;
        solo.resume_command = Some("sase bead work plan.md".to_string());
        let decisions = wait_epic_follow_reduce(&input(vec![target(
            "planner",
            true,
            vec![solo],
        )]));
        assert_eq!(decisions[0].state, "blocked");
        assert_eq!(
            decisions[0].reason.as_deref(),
            Some("target_dismissed_during_launch")
        );
        assert_eq!(
            decisions[0].resume_command.as_deref(),
            Some("sase bead work plan.md")
        );
    }

    #[test]
    fn skipped_launch_blocks_with_hint() {
        let mut solo = member("planner");
        solo.launch_reserved = true;
        solo.launch_argv_present = false;
        solo.resume_command = Some("sase bead work plan.md".to_string());
        let decisions = wait_epic_follow_reduce(&input(vec![target(
            "planner",
            true,
            vec![solo],
        )]));
        assert_eq!(decisions[0].state, "blocked");
        assert_eq!(decisions[0].reason.as_deref(), Some("launch_skipped"));
    }

    #[test]
    fn ended_launch_blocks_with_hint() {
        let mut solo = member("planner");
        solo.launch_reserved = true;
        solo.launch_argv_present = true;
        solo.resume_command = Some("sase bead work plan.md".to_string());
        let decisions = wait_epic_follow_reduce(&input(vec![target(
            "planner",
            true,
            vec![solo],
        )]));
        assert_eq!(decisions[0].state, "blocked");
        assert_eq!(
            decisions[0].reason.as_deref(),
            Some("launch_ended_without_epic")
        );
    }

    #[test]
    fn empty_members_are_none() {
        let decisions = wait_epic_follow_reduce(&input(vec![target(
            "planner",
            true,
            Vec::new(),
        )]));
        assert_eq!(decisions[0].state, "none");
        assert!(decisions[0].skipped_epic_ids.is_empty());
    }

    #[test]
    fn since_preserved_on_same_state() {
        let mut facts = target("planner", true, Vec::new());
        facts.previous_state = Some("none".to_string());
        facts.previous_since = Some(1_799_999_000.0);
        let decisions = wait_epic_follow_reduce(&input(vec![facts]));
        assert_eq!(decisions[0].state, "none");
        assert_eq!(decisions[0].since, 1_799_999_000.0);
    }

    #[test]
    fn since_resets_on_transition() {
        let mut facts = target("planner", true, Vec::new());
        facts.previous_state = Some("launching".to_string());
        facts.previous_since = Some(1_799_999_000.0);
        let decisions = wait_epic_follow_reduce(&input(vec![facts]));
        assert_eq!(decisions[0].state, "none");
        assert_eq!(decisions[0].since, 1_800_000_000.0);
    }

    #[test]
    fn clan_with_several_members_unions() {
        let mut first = member("worker-a");
        first.recorded_epic_ids = vec!["sase-7k".to_string()];
        let mut second = member("worker-b");
        second.recorded_epic_ids =
            vec!["sase-7k".to_string(), "sase-7m".to_string()];
        let decisions = wait_epic_follow_reduce(&input(vec![target(
            "clan",
            true,
            vec![first, second],
        )]));
        assert_eq!(decisions[0].state, "following");
        assert_eq!(decisions[0].epic_ids, vec!["sase-7k", "sase-7m"]);
        assert_eq!(decisions[0].members, vec!["worker-a", "worker-b"]);
    }
}
