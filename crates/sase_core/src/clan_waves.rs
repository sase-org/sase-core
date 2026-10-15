//! Derived dependency layers for one recorded clan generation.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::dependency_waves::layer_dependency_waves;

/// One loaded direct member and the union of its declared waits.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClanWaveMemberWire {
    pub name: String,
    #[serde(default)]
    pub aliases: Vec<String>,
    /// Stable lexical launch order key supplied by the caller.
    #[serde(default)]
    pub launch_key: String,
    #[serde(default)]
    pub wait_for: Vec<String>,
    #[serde(default)]
    pub wait_for_beads: Vec<String>,
    #[serde(default)]
    pub wait_for_hoods: Vec<String>,
    #[serde(default)]
    pub phase_bead_id: Option<String>,
    #[serde(default)]
    pub retry_of: Option<String>,
    /// Candidate for the unnumbered final seal stage.
    #[serde(default)]
    pub seal: bool,
}

/// Inputs needed to project one clan generation.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClanWaveRequestWire {
    pub clan_name: String,
    #[serde(default)]
    pub members: Vec<ClanWaveMemberWire>,
}

/// Resolved waits and dependency layers for one clan generation.
///
/// `waves` and `wave_of` use one-based wave numbers. Ghost names participate in
/// both so a dismissed predecessor keeps subsequent wave numbers stable, but
/// never appears as a loaded row. Seal members are separated from `waves` only
/// when they make up the final resolved layer and another layer precedes them.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClanWavesWire {
    pub waves: Vec<Vec<String>>,
    pub wave_of: BTreeMap<String, usize>,
    pub preds: BTreeMap<String, Vec<String>>,
    pub external_waits: BTreeMap<String, usize>,
    pub ghosts: Vec<String>,
    pub unresolved: Vec<String>,
    pub seal: Vec<String>,
}

/// Project the declared intra-clan waits into deterministic dependency layers.
pub fn project_clan_waves(request: &ClanWaveRequestWire) -> ClanWavesWire {
    let mut members = request.members.iter().collect::<Vec<_>>();
    members.sort_by(|left, right| {
        left.launch_key
            .cmp(&right.launch_key)
            .then_with(|| left.name.cmp(&right.name))
    });
    // Names are the row identities in the wire contract. If malformed input
    // repeats one, keep the earliest launch deterministically.
    let mut seen_names = BTreeSet::new();
    members.retain(|member| seen_names.insert(member.name.as_str()));

    let member_by_name: BTreeMap<&str, &ClanWaveMemberWire> = members
        .iter()
        .map(|member| (member.name.as_str(), *member))
        .collect();
    let member_names: BTreeSet<String> = member_by_name
        .keys()
        .map(|name| (*name).to_string())
        .collect();

    let mut name_lookup: BTreeMap<String, String> = member_by_name
        .keys()
        .map(|name| ((*name).to_string(), (*name).to_string()))
        .collect();
    for member in &members {
        for alias in &member.aliases {
            name_lookup
                .entry(alias.clone())
                .or_insert_with(|| member.name.clone());
        }
    }

    let mut retry_parent = BTreeMap::new();
    for member in &members {
        if let Some(parent) = member
            .retry_of
            .as_deref()
            .and_then(|name| name_lookup.get(name))
        {
            retry_parent.insert(member.name.clone(), parent.clone());
        }
    }
    let slot_of: BTreeMap<String, String> = member_by_name
        .keys()
        .map(|name| {
            let name = (*name).to_string();
            let root = retry_slot_root(&name, &retry_parent);
            (name, root)
        })
        .collect();

    let mut members_by_slot: BTreeMap<String, Vec<&ClanWaveMemberWire>> =
        BTreeMap::new();
    for member in &members {
        let slot = slot_of
            .get(member.name.as_str())
            .cloned()
            .unwrap_or_else(|| member.name.clone());
        members_by_slot.entry(slot).or_default().push(member);
    }
    for (slot, slot_members) in &mut members_by_slot {
        let slot = slot.as_str();
        slot_members.sort_by(|left, right| {
            let left_is_root = left.name == slot;
            let right_is_root = right.name == slot;
            right_is_root
                .cmp(&left_is_root)
                .then_with(|| left.launch_key.cmp(&right.launch_key))
                .then_with(|| left.name.cmp(&right.name))
        });
    }

    let slot_order: BTreeMap<String, (String, String)> = members_by_slot
        .iter()
        .map(|(slot, slot_members)| {
            let root = slot_members
                .iter()
                .find(|member| member.name == *slot)
                .copied()
                .unwrap_or(slot_members[0]);
            (slot.clone(), (root.launch_key.clone(), slot.clone()))
        })
        .collect();
    let mut nodes: BTreeSet<String> = members_by_slot.keys().cloned().collect();
    let mut dependencies: BTreeMap<String, BTreeSet<String>> = nodes
        .iter()
        .cloned()
        .map(|node| (node, BTreeSet::new()))
        .collect();
    let mut external_by_member: BTreeMap<String, BTreeSet<String>> =
        member_names
            .iter()
            .cloned()
            .map(|name| (name, BTreeSet::new()))
            .collect();

    let bead_to_slot: BTreeMap<&str, String> = members
        .iter()
        .filter_map(|member| {
            member
                .phase_bead_id
                .as_deref()
                .map(|bead| (bead, slot_of[member.name.as_str()].clone()))
        })
        .collect();
    let mut ghosts = BTreeSet::new();

    for member in &members {
        let owner_slot = &slot_of[member.name.as_str()];
        let mut member_external = BTreeSet::new();
        for target in &member.wait_for {
            if target == &request.clan_name {
                add_hood_snapshot(
                    &mut dependencies,
                    owner_slot,
                    &request.clan_name,
                    &member.launch_key,
                    &members,
                    &slot_of,
                );
                continue;
            }
            let target_name = strip_session_suffix(target);
            let resolved = name_lookup
                .get(target)
                .or_else(|| name_lookup.get(target_name));
            if let Some(target_member) = resolved {
                add_dependency(
                    &mut dependencies,
                    owner_slot,
                    &slot_of[target_member],
                );
                continue;
            }
            if target_name == request.clan_name {
                add_hood_snapshot(
                    &mut dependencies,
                    owner_slot,
                    &request.clan_name,
                    &member.launch_key,
                    &members,
                    &slot_of,
                );
                continue;
            }
            if is_inside_clan_hood(&request.clan_name, target_name) {
                let ghost = target_name.to_string();
                ghosts.insert(ghost.clone());
                nodes.insert(ghost.clone());
                dependencies.entry(ghost.clone()).or_default();
                add_dependency(&mut dependencies, owner_slot, &ghost);
                continue;
            }
            if !is_time_or_epic_follow(target) {
                member_external.insert(format!("agent:{target}"));
            }
        }

        for bead_id in &member.wait_for_beads {
            if let Some(predecessor) = bead_to_slot.get(bead_id.as_str()) {
                add_dependency(&mut dependencies, owner_slot, predecessor);
            } else {
                member_external.insert(format!("bead:{bead_id}"));
            }
        }

        for hood in &member.wait_for_hoods {
            if is_inside_clan_hood(&request.clan_name, hood) {
                add_hood_snapshot(
                    &mut dependencies,
                    owner_slot,
                    hood,
                    &member.launch_key,
                    &members,
                    &slot_of,
                );
            } else {
                member_external.insert(format!("hood:{hood}"));
            }
        }
        external_by_member.insert(member.name.clone(), member_external);
    }

    let mut order = slot_order;
    for ghost in &ghosts {
        nodes.insert(ghost.clone());
        dependencies.entry(ghost.clone()).or_default();
        order.insert(ghost.clone(), (String::new(), ghost.clone()));
    }
    let layered = layer_dependency_waves(&nodes, &dependencies, &order);

    let seal_slots: BTreeSet<String> = members_by_slot
        .iter()
        .filter(|(_, slot_members)| {
            slot_members.iter().any(|member| member.seal)
        })
        .map(|(slot, _)| slot.clone())
        .collect();
    let mut resolved_slot_waves = layered.waves;
    let mut seal_slots_out = Vec::new();
    if resolved_slot_waves.len() > 1
        && resolved_slot_waves.last().is_some_and(|last| {
            !last.is_empty()
                && last.iter().all(|node| seal_slots.contains(node))
        })
    {
        seal_slots_out = resolved_slot_waves.pop().unwrap_or_default();
    }

    let mut waves = Vec::new();
    let mut wave_of = BTreeMap::new();
    for (wave_index, slots) in resolved_slot_waves.iter().enumerate() {
        let wave_number = wave_index + 1;
        let mut wave_members = Vec::new();
        for slot in slots {
            if ghosts.contains(slot) {
                wave_members.push(slot.clone());
            } else if let Some(slot_members) = members_by_slot.get(slot) {
                wave_members.extend(
                    slot_members.iter().map(|member| member.name.clone()),
                );
            }
        }
        for name in &wave_members {
            wave_of.insert(name.clone(), wave_number);
        }
        waves.push(wave_members);
    }

    let seal = flatten_slots(&seal_slots_out, &members_by_slot);
    let mut unresolved_slots =
        layered.unresolved.iter().cloned().collect::<Vec<_>>();
    unresolved_slots.sort_by(|left, right| {
        order
            .get(left)
            .cmp(&order.get(right))
            .then_with(|| left.cmp(right))
    });
    let unresolved = flatten_slots(&unresolved_slots, &members_by_slot);

    let mut preds = BTreeMap::new();
    let mut external_waits = BTreeMap::new();
    for member in &members {
        let slot = &slot_of[member.name.as_str()];
        let mut member_preds = dependencies
            .get(slot)
            .into_iter()
            .flat_map(|predecessors| predecessors.iter())
            .cloned()
            .collect::<Vec<_>>();
        member_preds.sort_by(|left, right| {
            order
                .get(left)
                .cmp(&order.get(right))
                .then_with(|| left.cmp(right))
        });
        preds.insert(member.name.clone(), member_preds);
        external_waits.insert(
            member.name.clone(),
            external_by_member
                .get(&member.name)
                .map_or(0, BTreeSet::len),
        );
    }

    ClanWavesWire {
        waves,
        wave_of,
        preds,
        external_waits,
        ghosts: ghosts.into_iter().collect(),
        unresolved,
        seal,
    }
}

fn retry_slot_root(
    name: &str,
    retry_parent: &BTreeMap<String, String>,
) -> String {
    let mut path = Vec::new();
    let mut current = name.to_string();
    loop {
        let Some(parent) = retry_parent.get(&current) else {
            return current;
        };
        if let Some(cycle_start) = path.iter().position(|seen| seen == &current)
        {
            return path[cycle_start..]
                .iter()
                .min()
                .cloned()
                .unwrap_or(current);
        }
        path.push(current);
        current = parent.clone();
        if current == name {
            return path.iter().min().cloned().unwrap_or(current);
        }
    }
}

fn strip_session_suffix(name: &str) -> &str {
    match name.rsplit_once("--") {
        Some((base, suffix)) if !base.is_empty() && !suffix.is_empty() => base,
        _ => name,
    }
}

fn is_inside_clan_hood(clan_name: &str, name: &str) -> bool {
    name == clan_name
        || name
            .strip_prefix(clan_name)
            .is_some_and(|suffix| suffix.starts_with('.'))
}

fn member_is_in_hood(member_name: &str, hood: &str) -> bool {
    member_name == hood
        || member_name
            .strip_prefix(hood)
            .is_some_and(|suffix| suffix.starts_with('.'))
}

fn is_time_or_epic_follow(target: &str) -> bool {
    let target = target.trim();
    target.starts_with("time=")
        || target.starts_with("wait_duration=")
        || target.starts_with("wait_until=")
        || target.starts_with("for_epic=")
        || target.starts_with("epic=")
}

fn add_dependency(
    dependencies: &mut BTreeMap<String, BTreeSet<String>>,
    owner: &str,
    predecessor: &str,
) {
    if owner != predecessor {
        dependencies
            .entry(owner.to_string())
            .or_default()
            .insert(predecessor.to_string());
    }
}

fn add_hood_snapshot(
    dependencies: &mut BTreeMap<String, BTreeSet<String>>,
    owner_slot: &str,
    hood: &str,
    owner_launch_key: &str,
    members: &[&ClanWaveMemberWire],
    slot_of: &BTreeMap<String, String>,
) {
    for candidate in members.iter().filter(|candidate| {
        member_is_in_hood(&candidate.name, hood)
            && candidate.launch_key.as_str() < owner_launch_key
    }) {
        if let Some(predecessor) = slot_of.get(candidate.name.as_str()) {
            add_dependency(dependencies, owner_slot, predecessor);
        }
    }
}

fn flatten_slots(
    slots: &[String],
    members_by_slot: &BTreeMap<String, Vec<&ClanWaveMemberWire>>,
) -> Vec<String> {
    let mut flattened = Vec::new();
    for slot in slots {
        if let Some(members) = members_by_slot.get(slot) {
            flattened.extend(members.iter().map(|member| member.name.clone()));
        }
    }
    flattened
}

#[cfg(test)]
mod tests {
    use super::*;

    fn member(name: &str, launch_key: &str) -> ClanWaveMemberWire {
        ClanWaveMemberWire {
            name: name.to_string(),
            launch_key: launch_key.to_string(),
            ..ClanWaveMemberWire::default()
        }
    }

    fn request(
        clan_name: &str,
        members: Vec<ClanWaveMemberWire>,
    ) -> ClanWaveRequestWire {
        ClanWaveRequestWire {
            clan_name: clan_name.to_string(),
            members,
        }
    }

    fn names(waves: &[Vec<String>]) -> Vec<Vec<&str>> {
        waves
            .iter()
            .map(|wave| wave.iter().map(String::as_str).collect())
            .collect()
    }

    #[test]
    fn bob_cli_epic_projects_five_numbered_waves_and_a_land_seal() {
        let clan = "bob-cli-7v";
        let mut members = (1..=7)
            .map(|number| {
                member(&format!("{clan}.{number}"), &format!("{number:02}"))
            })
            .collect::<Vec<_>>();
        members[3].wait_for = vec![format!("{clan}.1"), format!("{clan}.2")];
        members[4].wait_for = vec![format!("{clan}.3"), format!("{clan}.4")];
        members[5].wait_for = vec![format!("{clan}.5")];
        members[6].wait_for = vec![format!("{clan}.6")];
        let mut land = member(&format!("{clan}.land"), "08");
        land.wait_for =
            (1..=7).map(|number| format!("{clan}.{number}")).collect();
        land.seal = true;
        members.push(land);

        let result = project_clan_waves(&request(clan, members));

        assert_eq!(
            names(&result.waves),
            vec![
                vec!["bob-cli-7v.1", "bob-cli-7v.2", "bob-cli-7v.3"],
                vec!["bob-cli-7v.4"],
                vec!["bob-cli-7v.5"],
                vec!["bob-cli-7v.6"],
                vec!["bob-cli-7v.7"],
            ]
        );
        assert_eq!(result.seal, vec!["bob-cli-7v.land"]);
        assert!(!result.wave_of.contains_key("bob-cli-7v.land"));
        assert_eq!(
            result.preds["bob-cli-7v.4"],
            vec!["bob-cli-7v.1", "bob-cli-7v.2"]
        );
    }

    #[test]
    fn research_swarm_layers_media_before_linker() {
        let clan = "research.4j";
        let mut members = [
            "cdx", "cld", "grk", "gem", "final", "image", "audio", "linker",
        ]
        .iter()
        .enumerate()
        .map(|(index, suffix)| {
            member(&format!("{clan}.{suffix}"), &format!("{index:02}"))
        })
        .collect::<Vec<_>>();
        members[4].wait_for = ["cdx", "cld", "grk", "gem"]
            .iter()
            .map(|suffix| format!("{clan}.{suffix}"))
            .collect();
        members[5].wait_for = vec![format!("{clan}.final")];
        members[6].wait_for = vec![format!("{clan}.final")];
        members[7].wait_for = ["final", "image", "audio"]
            .iter()
            .map(|suffix| format!("{clan}.{suffix}"))
            .collect();

        let result = project_clan_waves(&request(clan, members));

        assert_eq!(
            names(&result.waves),
            vec![
                vec![
                    "research.4j.cdx",
                    "research.4j.cld",
                    "research.4j.grk",
                    "research.4j.gem",
                ],
                vec!["research.4j.final"],
                vec!["research.4j.image", "research.4j.audio"],
                vec!["research.4j.linker"],
            ]
        );
    }

    #[test]
    fn toobig_chain_and_flat_clan_cover_long_and_one_wave_shapes() {
        let clan = "toobig-5y";
        let mut chain = (1..=14)
            .map(|number| {
                member(&format!("{clan}.{number}"), &format!("{number:02}"))
            })
            .collect::<Vec<_>>();
        for (number, phase) in chain.iter_mut().enumerate().skip(1) {
            phase.wait_for = vec![format!("{clan}.{number}")];
        }
        let chain_result = project_clan_waves(&request(clan, chain));
        assert_eq!(chain_result.waves.len(), 14);
        assert_eq!(chain_result.wave_of["toobig-5y.14"], 14);

        let flat = project_clan_waves(&request(
            "review.2",
            vec![
                member("review.2.a", "01"),
                member("review.2.b", "02"),
                member("review.2.c", "03"),
            ],
        ));
        assert_eq!(
            names(&flat.waves),
            vec![vec!["review.2.a", "review.2.b", "review.2.c"]]
        );
    }

    #[test]
    fn aliases_session_turns_and_retry_members_resolve_to_one_slot() {
        let mut root = member("clan.a", "01");
        root.aliases = vec!["a".to_string()];
        let mut retry = member("clan.a.r1", "03");
        retry.retry_of = Some("a".to_string());
        retry.wait_for = vec!["clan.b".to_string()];
        let mut child = member("clan.c", "04");
        child.wait_for =
            vec!["clan.a.r1--turn-2".to_string(), "a--plan".to_string()];
        let result = project_clan_waves(&request(
            "clan",
            vec![root, member("clan.b", "02"), retry, child],
        ));

        assert_eq!(
            names(&result.waves),
            vec![vec!["clan.b"], vec!["clan.a", "clan.a.r1"], vec!["clan.c"],]
        );
        assert_eq!(result.wave_of["clan.a"], result.wave_of["clan.a.r1"]);
        assert_eq!(result.preds["clan.a"], vec!["clan.b"]);
        assert_eq!(result.preds["clan.a.r1"], vec!["clan.b"]);
        assert_eq!(result.preds["clan.c"], vec!["clan.a"]);
    }

    #[test]
    fn dismissed_member_is_a_ghost_and_external_waits_are_counted() {
        let mut final_member = member("clan.final", "02");
        final_member.wait_for = vec!["clan.dismissed".to_string()];
        let mut external = member("clan.external", "03");
        external.wait_for = vec![
            "other-clan.member".to_string(),
            "time=5m".to_string(),
            "for_epic=e2".to_string(),
        ];
        external.wait_for_beads = vec!["foreign-bead".to_string()];
        external.wait_for_hoods = vec!["another-hood".to_string()];

        let result =
            project_clan_waves(&request("clan", vec![final_member, external]));

        assert_eq!(result.ghosts, vec!["clan.dismissed"]);
        assert_eq!(result.wave_of["clan.dismissed"], 1);
        assert_eq!(result.wave_of["clan.final"], 2);
        assert_eq!(result.external_waits["clan.external"], 3);
        assert_eq!(result.external_waits["clan.final"], 0);
    }

    #[test]
    fn bead_only_edges_repair_a_reworked_epic_and_preserve_the_seal() {
        let mut first = member("clan.first", "01");
        first.phase_bead_id = Some("phase-1".to_string());
        let mut land = member("clan.land", "02");
        land.wait_for_beads = vec!["phase-1".to_string()];
        land.seal = true;

        let result = project_clan_waves(&request("clan", vec![first, land]));

        assert_eq!(names(&result.waves), vec![vec!["clan.first"]]);
        assert_eq!(result.preds["clan.land"], vec!["clan.first"]);
        assert_eq!(result.seal, vec!["clan.land"]);
    }

    #[test]
    fn clan_name_and_hood_waits_use_launch_snapshot_semantics() {
        let mut second = member("clan.second", "02");
        second.wait_for = vec!["clan".to_string()];
        let mut third = member("clan.third", "03");
        third.wait_for_hoods = vec!["clan".to_string()];

        let result = project_clan_waves(&request(
            "clan",
            vec![member("clan.first", "01"), second, third],
        ));

        assert_eq!(
            names(&result.waves),
            vec![vec!["clan.first"], vec!["clan.second"], vec!["clan.third"]]
        );
        assert_eq!(result.preds["clan.second"], vec!["clan.first"]);
        assert_eq!(
            result.preds["clan.third"],
            vec!["clan.first", "clan.second"]
        );
    }

    #[test]
    fn late_joiners_without_waits_stay_in_the_first_layer() {
        let mut dependent = member("clan.dependent", "02");
        dependent.wait_for = vec!["clan.first".to_string()];
        let result = project_clan_waves(&request(
            "clan",
            vec![
                dependent,
                member("clan.late", "03"),
                member("clan.first", "01"),
            ],
        ));

        assert_eq!(
            names(&result.waves),
            vec![vec!["clan.first", "clan.late"], vec!["clan.dependent"]]
        );
    }

    #[test]
    fn cycle_members_and_dependents_are_unresolved_not_dropped() {
        let mut a = member("clan.a", "03");
        let mut b = member("clan.b", "02");
        let mut c = member("clan.c", "01");
        a.wait_for = vec!["clan.b".to_string()];
        b.wait_for = vec!["clan.a".to_string()];
        c.wait_for = vec!["clan.b".to_string()];

        let result = project_clan_waves(&request("clan", vec![a, b, c]));

        assert!(result.waves.is_empty());
        assert_eq!(result.unresolved, vec!["clan.c", "clan.b", "clan.a"]);
    }
}
