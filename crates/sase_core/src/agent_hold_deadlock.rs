//! Bounded wait-graph reachability for hold-deadlock detection.
//!
//! Python supplies the already-scanned relevant wait set. This module owns
//! complete branch traversal, shared identity matching (name, agent session, clan,
//! workflow, tribe, hood), waiter launch cutoffs, and self exclusion. It
//! never writes holds or auto-releases a cycle.

use std::collections::{BTreeSet, HashMap, VecDeque};

use serde::{Deserialize, Serialize};

use crate::agent_identity::{agent_name_in_hood, parse_agent_session_name};

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HoldDeadlockCandidateWire {
    #[serde(default)]
    pub artifact_dir: Option<String>,
    #[serde(default)]
    pub agent_name: Option<String>,
    #[serde(default)]
    pub agent_session: Option<String>,
    #[serde(default)]
    pub clan: Option<String>,
    #[serde(default)]
    pub workflow: Option<String>,
    #[serde(default)]
    pub timestamp: Option<String>,
    #[serde(default)]
    pub tribes: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HoldDeadlockWaitNodeWire {
    pub artifact_dir: String,
    #[serde(default)]
    pub agent_name: Option<String>,
    #[serde(default)]
    pub agent_session: Option<String>,
    #[serde(default)]
    pub clan: Option<String>,
    #[serde(default)]
    pub workflow: Option<String>,
    #[serde(default)]
    pub timestamp: Option<String>,
    #[serde(default)]
    pub waiting_for: Vec<String>,
    #[serde(default)]
    pub wait_for_hoods: Vec<String>,
    #[serde(default)]
    pub tribes: Vec<String>,
    #[serde(default)]
    pub running: bool,
    #[serde(default)]
    pub has_done_marker: bool,
}

/// Return whether `start_artifact_dir` can reach `candidate` through the
/// wait graph.
///
/// Running and settled nodes are never a mutual block and are not walked.
/// Unrelated branches that never hit the candidate are ignored. Cycles
/// among other waiters are visited once.
pub fn hold_deadlock_reaches_candidate(
    start_artifact_dir: &str,
    candidate: &HoldDeadlockCandidateWire,
    nodes: &[HoldDeadlockWaitNodeWire],
) -> bool {
    if start_artifact_dir.is_empty() {
        return false;
    }
    let by_dir: HashMap<&str, &HoldDeadlockWaitNodeWire> = nodes
        .iter()
        .filter(|node| !node.artifact_dir.is_empty())
        .map(|node| (node.artifact_dir.as_str(), node))
        .collect();
    let Some(start) = by_dir.get(start_artifact_dir).copied() else {
        return false;
    };
    if !node_is_pre_run(start) {
        return false;
    }

    let mut seen: BTreeSet<&str> = BTreeSet::new();
    seen.insert(start.artifact_dir.as_str());
    let mut queue: VecDeque<&HoldDeadlockWaitNodeWire> = VecDeque::new();
    queue.push_back(start);

    while let Some(current) = queue.pop_front() {
        if waiter_reaches_candidate(current, candidate) {
            return true;
        }
        for node in nodes {
            if node.artifact_dir.is_empty()
                || seen.contains(node.artifact_dir.as_str())
                || !node_is_pre_run(node)
                || !waiter_reaches_node(current, node)
            {
                continue;
            }
            seen.insert(node.artifact_dir.as_str());
            queue.push_back(node);
        }
    }
    false
}

fn node_is_pre_run(node: &HoldDeadlockWaitNodeWire) -> bool {
    !node.running && !node.has_done_marker
}

fn waiter_reaches_candidate(
    waiter: &HoldDeadlockWaitNodeWire,
    candidate: &HoldDeadlockCandidateWire,
) -> bool {
    let target_dir = candidate.artifact_dir.as_deref().unwrap_or("");
    if !target_dir.is_empty() && target_dir == waiter.artifact_dir {
        return false;
    }
    identity_hit(
        waiter,
        IdentityView {
            artifact_dir: target_dir,
            agent_name: candidate.agent_name.as_deref(),
            agent_session: candidate.agent_session.as_deref(),
            clan: candidate.clan.as_deref(),
            workflow: candidate.workflow.as_deref(),
            timestamp: candidate.timestamp.as_deref(),
            tribes: &candidate.tribes,
        },
    )
}

fn waiter_reaches_node(
    waiter: &HoldDeadlockWaitNodeWire,
    node: &HoldDeadlockWaitNodeWire,
) -> bool {
    identity_hit(
        waiter,
        IdentityView {
            artifact_dir: &node.artifact_dir,
            agent_name: node.agent_name.as_deref(),
            agent_session: node.agent_session.as_deref(),
            clan: node.clan.as_deref(),
            workflow: node.workflow.as_deref(),
            timestamp: node.timestamp.as_deref(),
            tribes: &node.tribes,
        },
    )
}

struct IdentityView<'a> {
    artifact_dir: &'a str,
    agent_name: Option<&'a str>,
    agent_session: Option<&'a str>,
    clan: Option<&'a str>,
    workflow: Option<&'a str>,
    timestamp: Option<&'a str>,
    tribes: &'a [String],
}

fn identity_hit(
    waiter: &HoldDeadlockWaitNodeWire,
    target: IdentityView<'_>,
) -> bool {
    if !target.artifact_dir.is_empty()
        && target.artifact_dir == waiter.artifact_dir
    {
        return false;
    }
    let names = identity_names(&target);
    for wait_name in &waiter.waiting_for {
        if wait_name_hits(wait_name, waiter, &target, &names) {
            return true;
        }
    }
    for hood in &waiter.wait_for_hoods {
        if hood_hits(hood, waiter, &target) {
            return true;
        }
    }
    false
}

fn identity_names(target: &IdentityView<'_>) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    if let Some(name) = target.agent_name.filter(|name| !name.is_empty()) {
        names.insert(name.to_string());
        if let Ok(parsed) = parse_agent_session_name(name) {
            if !parsed.agent_session_name.is_empty() {
                names.insert(parsed.agent_session_name);
            }
        }
    }
    if let Some(agent_session) =
        target.agent_session.filter(|name| !name.is_empty())
    {
        names.insert(agent_session.to_string());
        if let Ok(parsed) = parse_agent_session_name(agent_session) {
            if !parsed.agent_session_name.is_empty() {
                names.insert(parsed.agent_session_name);
            }
        }
    }
    if let Some(clan) = target.clan.filter(|name| !name.is_empty()) {
        names.insert(clan.to_string());
    }
    if let Some(workflow) = target.workflow.filter(|name| !name.is_empty()) {
        names.insert(workflow.to_string());
    }
    names
}

fn wait_name_hits(
    wait_name: &str,
    waiter: &HoldDeadlockWaitNodeWire,
    target: &IdentityView<'_>,
    names: &BTreeSet<String>,
) -> bool {
    if wait_name.is_empty() {
        return false;
    }
    if let Some(tribe) = wait_name.strip_prefix('@') {
        if tribe.is_empty() {
            return false;
        }
        if !target.tribes.iter().any(|value| value == tribe) {
            return false;
        }
        return timestamp_is_after(
            target.timestamp,
            waiter.timestamp.as_deref(),
        );
    }
    names.contains(wait_name)
}

fn hood_hits(
    hood: &str,
    waiter: &HoldDeadlockWaitNodeWire,
    target: &IdentityView<'_>,
) -> bool {
    if hood.is_empty() {
        return false;
    }
    if !timestamp_is_at_or_before(target.timestamp, waiter.timestamp.as_deref())
    {
        return false;
    }
    for name in [target.agent_name, target.agent_session]
        .into_iter()
        .flatten()
    {
        if agent_name_in_hood(name, hood).unwrap_or(false) {
            return true;
        }
    }
    false
}

fn timestamp_is_after(target: Option<&str>, waiter: Option<&str>) -> bool {
    match (target, waiter) {
        (Some(target), Some(waiter)) => target > waiter,
        _ => true,
    }
}

fn timestamp_is_at_or_before(
    target: Option<&str>,
    waiter: Option<&str>,
) -> bool {
    match (target, waiter) {
        (Some(target), Some(waiter)) => target <= waiter,
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(
        dir: &str,
        name: &str,
        waiting_for: &[&str],
        wait_for_hoods: &[&str],
    ) -> HoldDeadlockWaitNodeWire {
        HoldDeadlockWaitNodeWire {
            artifact_dir: dir.to_string(),
            agent_name: Some(name.to_string()),
            agent_session: parse_agent_session_name(name)
                .ok()
                .map(|parsed| parsed.agent_session_name),
            timestamp: Some(dir.rsplit('/').next().unwrap_or(dir).to_string()),
            waiting_for: waiting_for
                .iter()
                .map(|value| (*value).to_string())
                .collect(),
            wait_for_hoods: wait_for_hoods
                .iter()
                .map(|value| (*value).to_string())
                .collect(),
            ..HoldDeadlockWaitNodeWire::default()
        }
    }

    fn candidate(dir: &str, name: &str) -> HoldDeadlockCandidateWire {
        HoldDeadlockCandidateWire {
            artifact_dir: Some(dir.to_string()),
            agent_name: Some(name.to_string()),
            agent_session: parse_agent_session_name(name)
                .ok()
                .map(|parsed| parsed.agent_session_name),
            timestamp: Some(dir.rsplit('/').next().unwrap_or(dir).to_string()),
            ..HoldDeadlockCandidateWire::default()
        }
    }

    fn reaches(
        armer: HoldDeadlockWaitNodeWire,
        held: &HoldDeadlockCandidateWire,
        rest: Vec<HoldDeadlockWaitNodeWire>,
    ) -> bool {
        let start = armer.artifact_dir.clone();
        let mut nodes = vec![armer];
        nodes.extend(rest);
        hold_deadlock_reaches_candidate(&start, held, &nodes)
    }

    #[test]
    fn direct_wait_name_is_a_deadlock() {
        let armer = node(
            "/a/20260910120001",
            "armer.agent",
            &["candidate.agent"],
            &[],
        );
        let held = candidate("/a/20260910120000", "candidate.agent");
        assert!(reaches(armer, &held, vec![]));
    }

    #[test]
    fn first_harmless_branch_does_not_hide_the_second() {
        let armer = node(
            "/a/20260910120001",
            "armer.agent",
            &["safe.agent", "bridge.agent"],
            &[],
        );
        let safe =
            node("/a/20260910120002", "safe.agent", &["unrelated.agent"], &[]);
        let bridge = node(
            "/a/20260910120003",
            "bridge.agent",
            &["candidate.agent"],
            &[],
        );
        let held = candidate("/a/20260910120000", "candidate.agent");
        assert!(reaches(armer, &held, vec![safe, bridge]));
    }

    #[test]
    fn longer_cycle_with_repeated_vertices_still_reaches() {
        let armer = node("/a/20260910120001", "armer.agent", &["loop.a"], &[]);
        let loop_a = node("/a/20260910120002", "loop.a", &["loop.b"], &[]);
        let loop_b = node(
            "/a/20260910120003",
            "loop.b",
            &["loop.a", "candidate.agent"],
            &[],
        );
        let held = candidate("/a/20260910120000", "candidate.agent");
        assert!(reaches(armer, &held, vec![loop_a, loop_b]));
    }

    #[test]
    fn hood_wait_reaches_a_hood_member_candidate() {
        let armer = node(
            "/a/20260910120001",
            "armer.agent",
            &["safe.agent"],
            &["research"],
        );
        let safe =
            node("/a/20260910120002", "safe.agent", &["unrelated.agent"], &[]);
        let held = candidate("/a/20260910120000", "research.worker--code");
        assert!(reaches(armer, &held, vec![safe]));
    }

    #[test]
    fn no_cycle_is_not_a_deadlock() {
        let armer =
            node("/a/20260910120001", "armer.agent", &["someone.else"], &[]);
        let held = candidate("/a/20260910120000", "candidate.agent");
        assert!(!reaches(armer, &held, vec![]));
    }

    #[test]
    fn running_armer_is_immune() {
        let mut armer = node(
            "/a/20260910120001",
            "armer.agent",
            &["candidate.agent"],
            &[],
        );
        armer.running = true;
        let held = candidate("/a/20260910120000", "candidate.agent");
        assert!(!reaches(armer, &held, vec![]));
    }

    #[test]
    fn settled_branch_is_not_walked() {
        let armer =
            node("/a/20260910120001", "armer.agent", &["done.agent"], &[]);
        let mut done =
            node("/a/20260910120002", "done.agent", &["candidate.agent"], &[]);
        done.has_done_marker = true;
        let held = candidate("/a/20260910120000", "candidate.agent");
        assert!(!reaches(armer, &held, vec![done]));
    }

    #[test]
    fn agent_session_wait_name_matches_role_suffixed_candidate() {
        let armer = node("/a/20260910120001", "armer.agent", &["team"], &[]);
        let held = candidate("/a/20260910120000", "team--code");
        assert!(reaches(armer, &held, vec![]));
    }

    #[test]
    fn hood_does_not_match_a_member_launched_after_the_waiter() {
        let armer =
            node("/a/20260910120001", "armer.agent", &[], &["research"]);
        let held = candidate("/a/20260910120002", "research.worker--code");
        assert!(!reaches(armer, &held, vec![]));
    }

    #[test]
    fn waiter_does_not_deadlock_on_its_own_hood_membership() {
        let armer = node(
            "/a/20260910120001",
            "research.armer--code",
            &[],
            &["research"],
        );
        let held = candidate("/a/20260910120000", "other.agent");
        assert!(!reaches(armer, &held, vec![]));
    }

    #[test]
    fn unrelated_cycle_without_the_candidate_is_not_a_deadlock() {
        let armer =
            node("/a/20260910120001", "armer.agent", &["other.agent"], &[]);
        let other =
            node("/a/20260910120002", "other.agent", &["armer.agent"], &[]);
        let held = candidate("/a/20260910120000", "candidate.agent");
        assert!(!reaches(armer, &held, vec![other]));
    }

    #[test]
    fn tribe_wait_honors_launch_cutoff() {
        let mut armer =
            node("/a/20260910120001", "armer.agent", &["@nightly"], &[]);
        armer.timestamp = Some("20260910120001".into());
        let mut held = candidate("/a/20260910120002", "candidate.agent");
        held.timestamp = Some("20260910120002".into());
        held.tribes = vec!["nightly".into()];
        assert!(reaches(armer.clone(), &held, vec![]));

        held.timestamp = Some("20260910120000".into());
        assert!(!reaches(armer, &held, vec![]));
    }
}
