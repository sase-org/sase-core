//! Derived dependency layers for one recorded clan generation.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::dependency_waves::layer_dependency_waves;

/// One loaded direct member and the union of its declared waits.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClanWaveMemberWire {
    /// Stable identity, independent of the names used to present this member.
    pub key: String,
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
    /// The retry chain root's member key.
    #[serde(default)]
    pub retry_of: Option<String>,
    /// Candidate for the unnumbered final seal stage.
    #[serde(default)]
    pub seal: bool,
}

/// Inputs needed to project one clan generation.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClanWaveRequestWire {
    #[serde(default)]
    pub clan_names: Vec<String>,
    #[serde(default)]
    pub members: Vec<ClanWaveMemberWire>,
}

/// One numbered dependency layer or the optional seal layer.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClanWaveWire {
    /// One-based position in the full layer sequence, including the seal.
    pub index: u32,
    /// Numbered wave, or None for the seal.
    #[serde(default)]
    pub number: Option<u32>,
    #[serde(default)]
    pub seal: bool,
    #[serde(default)]
    pub member_keys: Vec<String>,
    #[serde(default)]
    pub ghost_names: Vec<String>,
}

/// Placement and dependency detail for one loaded direct member.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClanWaveMemberPlacementWire {
    pub key: String,
    /// One-based layer position. None means unresolved; seal placements have an index.
    #[serde(default)]
    pub wave_index: Option<u32>,
    /// Zero-based position in the final loaded-member display order.
    #[serde(default)]
    pub rank: u32,
    #[serde(default)]
    pub predecessor_keys: Vec<String>,
    #[serde(default)]
    pub ghost_predecessors: Vec<String>,
    #[serde(default)]
    pub external_wait_count: u32,
}

/// Deterministic key-based layout for one clan generation.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClanWavesWire {
    #[serde(default)]
    pub layered: bool,
    #[serde(default)]
    pub numbered_wave_count: u32,
    #[serde(default)]
    pub has_seal: bool,
    #[serde(default)]
    pub waves: Vec<ClanWaveWire>,
    #[serde(default)]
    pub members: Vec<ClanWaveMemberPlacementWire>,
    #[serde(default)]
    pub unresolved: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum LayoutNode {
    Slot(String),
    Ghost(String),
}

/// Project declared intra-clan waits into deterministic dependency layers.
pub fn project_clan_waves(request: &ClanWaveRequestWire) -> ClanWavesWire {
    let mut members = request.members.iter().collect::<Vec<_>>();
    members.sort_by(|left, right| {
        left.launch_key
            .cmp(&right.launch_key)
            .then_with(|| left.name.cmp(&right.name))
            .then_with(|| left.key.cmp(&right.key))
    });

    let member_by_key: BTreeMap<String, &ClanWaveMemberWire> = members
        .iter()
        .map(|member| (member.key.clone(), *member))
        .collect();
    let member_keys: BTreeSet<String> = member_by_key.keys().cloned().collect();

    // A presented spelling can identify more than one loaded unit. Keep every
    // key so duplicate display names never discard a member or its wait edge.
    let mut name_lookup: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for member in &members {
        for spelling in member_spellings(member) {
            if !spelling.is_empty() {
                name_lookup
                    .entry(spelling.to_string())
                    .or_default()
                    .insert(member.key.clone());
            }
        }
    }

    let mut retry_parent = BTreeMap::new();
    for member in &members {
        if let Some(parent) = member
            .retry_of
            .as_deref()
            .filter(|key| member_keys.contains(*key))
        {
            retry_parent.insert(member.key.clone(), parent.to_string());
        }
    }
    let slot_of: BTreeMap<String, String> = member_by_key
        .keys()
        .map(|key| {
            let root = retry_slot_root(key, &retry_parent);
            (key.clone(), root)
        })
        .collect();

    let mut members_by_slot: BTreeMap<String, Vec<&ClanWaveMemberWire>> =
        BTreeMap::new();
    for member in &members {
        let slot = slot_of
            .get(&member.key)
            .cloned()
            .unwrap_or_else(|| member.key.clone());
        members_by_slot.entry(slot).or_default().push(member);
    }
    for (slot, slot_members) in &mut members_by_slot {
        slot_members.sort_by(|left, right| {
            let left_is_root = left.key == *slot;
            let right_is_root = right.key == *slot;
            right_is_root
                .cmp(&left_is_root)
                .then_with(|| left.launch_key.cmp(&right.launch_key))
                .then_with(|| left.name.cmp(&right.name))
                .then_with(|| left.key.cmp(&right.key))
        });
    }

    let slot_order: BTreeMap<String, (String, String)> = members_by_slot
        .iter()
        .map(|(slot, slot_members)| {
            let root =
                member_by_key.get(slot).copied().unwrap_or(slot_members[0]);
            (slot.clone(), (root.launch_key.clone(), root.name.clone()))
        })
        .collect();

    let mut nodes: BTreeSet<LayoutNode> = members_by_slot
        .keys()
        .cloned()
        .map(LayoutNode::Slot)
        .collect();
    let mut dependencies: BTreeMap<LayoutNode, BTreeSet<LayoutNode>> = nodes
        .iter()
        .map(|node| (node.clone(), BTreeSet::new()))
        .collect();
    let mut external_by_member: BTreeMap<String, BTreeSet<String>> =
        member_keys
            .iter()
            .cloned()
            .map(|key| (key, BTreeSet::new()))
            .collect();

    let mut bead_to_slots: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for member in &members {
        if let Some(bead_id) = &member.phase_bead_id {
            bead_to_slots
                .entry(bead_id.clone())
                .or_default()
                .insert(slot_of[&member.key].clone());
        }
    }

    let clan_names: BTreeSet<&str> = request
        .clan_names
        .iter()
        .map(String::as_str)
        .filter(|name| !name.is_empty())
        .collect();
    let mut ghosts = BTreeSet::new();

    for member in &members {
        let owner_slot = &slot_of[&member.key];
        let mut member_external = BTreeSet::new();

        for target in &member.wait_for {
            if clan_names.contains(target.as_str()) {
                add_hood_snapshot(
                    &mut dependencies,
                    owner_slot,
                    target,
                    &member.launch_key,
                    &members,
                    &slot_of,
                );
                continue;
            }

            let target_name = strip_session_suffix(target);
            if let Some(target_keys) = name_lookup
                .get(target)
                .or_else(|| name_lookup.get(target_name))
            {
                for target_key in target_keys {
                    add_dependency(
                        &mut dependencies,
                        owner_slot,
                        &slot_of[target_key],
                    );
                }
                continue;
            }

            if clan_names.contains(target_name) {
                add_hood_snapshot(
                    &mut dependencies,
                    owner_slot,
                    target_name,
                    &member.launch_key,
                    &members,
                    &slot_of,
                );
                continue;
            }

            if is_inside_any_clan_hood(&request.clan_names, target_name) {
                let ghost =
                    canonical_ghost_name(&request.clan_names, target_name);
                ghosts.insert(ghost.clone());
                let ghost_node = LayoutNode::Ghost(ghost);
                nodes.insert(ghost_node.clone());
                dependencies.entry(ghost_node.clone()).or_default();
                add_dependency_node(&mut dependencies, owner_slot, &ghost_node);
                continue;
            }

            if !is_time_or_epic_follow(target) {
                member_external.insert(format!("agent:{target}"));
            }
        }

        for bead_id in &member.wait_for_beads {
            if let Some(predecessor_slots) = bead_to_slots.get(bead_id) {
                for predecessor in predecessor_slots {
                    add_dependency(&mut dependencies, owner_slot, predecessor);
                }
            } else {
                member_external.insert(format!("bead:{bead_id}"));
            }
        }

        for hood in &member.wait_for_hoods {
            if is_inside_any_clan_hood(&request.clan_names, hood) {
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

        external_by_member.insert(member.key.clone(), member_external);
    }

    let mut order = BTreeMap::new();
    for (slot, (launch_key, root_name)) in &slot_order {
        order.insert(
            LayoutNode::Slot(slot.clone()),
            (launch_key.clone(), root_name.clone()),
        );
    }
    for ghost in &ghosts {
        let node = LayoutNode::Ghost(ghost.clone());
        nodes.insert(node.clone());
        dependencies.entry(node.clone()).or_default();
        order.insert(node, (String::new(), ghost.clone()));
    }

    let layout = layer_dependency_waves(&nodes, &dependencies, &order);
    let seal_slots: BTreeSet<String> = members_by_slot
        .iter()
        .filter(|(_, slot_members)| {
            slot_members.iter().any(|member| member.seal)
        })
        .map(|(slot, _)| slot.clone())
        .collect();
    let mut resolved_slot_waves = layout.waves;
    let mut seal_nodes = Vec::new();
    if resolved_slot_waves.len() > 1
        && resolved_slot_waves.last().is_some_and(|last| {
            !last.is_empty()
                && last.iter().all(|node| match node {
                    LayoutNode::Slot(slot) => seal_slots.contains(slot),
                    LayoutNode::Ghost(_) => false,
                })
        })
    {
        seal_nodes = resolved_slot_waves.pop().unwrap_or_default();
    }

    let mut waves = Vec::new();
    let mut ordered_member_keys = Vec::new();
    let mut slot_wave_index = BTreeMap::new();
    for (layer_index, layer) in resolved_slot_waves.iter().enumerate() {
        let index = layer_index as u32 + 1;
        let mut member_keys_in_wave = Vec::new();
        let mut ghost_names = Vec::new();
        for node in layer {
            match node {
                LayoutNode::Slot(slot) => {
                    slot_wave_index.insert(slot.clone(), index);
                    if let Some(slot_members) = members_by_slot.get(slot) {
                        member_keys_in_wave.extend(
                            slot_members
                                .iter()
                                .map(|member| member.key.clone()),
                        );
                    }
                }
                LayoutNode::Ghost(ghost) => ghost_names.push(ghost.clone()),
            }
        }
        ghost_names.sort();
        ordered_member_keys.extend(member_keys_in_wave.iter().cloned());
        waves.push(ClanWaveWire {
            index,
            number: Some(index),
            seal: false,
            member_keys: member_keys_in_wave,
            ghost_names,
        });
    }

    if !seal_nodes.is_empty() {
        let index = waves.len() as u32 + 1;
        let mut member_keys_in_wave = Vec::new();
        for node in &seal_nodes {
            if let LayoutNode::Slot(slot) = node {
                slot_wave_index.insert(slot.clone(), index);
                if let Some(slot_members) = members_by_slot.get(slot) {
                    member_keys_in_wave.extend(
                        slot_members.iter().map(|member| member.key.clone()),
                    );
                }
            }
        }
        ordered_member_keys.extend(member_keys_in_wave.iter().cloned());
        waves.push(ClanWaveWire {
            index,
            number: None,
            seal: true,
            member_keys: member_keys_in_wave,
            ghost_names: Vec::new(),
        });
    }

    let mut unresolved_slots = layout
        .unresolved
        .iter()
        .filter_map(|node| match node {
            LayoutNode::Slot(slot) => Some(slot.clone()),
            LayoutNode::Ghost(_) => None,
        })
        .collect::<Vec<_>>();
    unresolved_slots.sort_by(|left, right| {
        order
            .get(&LayoutNode::Slot(left.clone()))
            .cmp(&order.get(&LayoutNode::Slot(right.clone())))
            .then_with(|| left.cmp(right))
    });
    unresolved_slots.dedup();

    let mut unresolved = Vec::new();
    for slot in unresolved_slots {
        if let Some(slot_members) = members_by_slot.get(&slot) {
            unresolved
                .extend(slot_members.iter().map(|member| member.key.clone()));
        }
    }
    ordered_member_keys.extend(unresolved.iter().cloned());

    let display_rank: BTreeMap<String, u32> = ordered_member_keys
        .iter()
        .enumerate()
        .map(|(rank, key)| (key.clone(), rank as u32))
        .collect();

    let mut placements = Vec::with_capacity(ordered_member_keys.len());
    for key in &ordered_member_keys {
        let slot = &slot_of[key];
        let node = LayoutNode::Slot(slot.clone());
        let mut predecessor_keys = BTreeSet::new();
        let mut ghost_predecessors = BTreeSet::new();
        if let Some(predecessors) = dependencies.get(&node) {
            for predecessor in predecessors {
                match predecessor {
                    LayoutNode::Slot(predecessor_slot) => {
                        predecessor_keys.insert(predecessor_slot.clone());
                    }
                    LayoutNode::Ghost(ghost) => {
                        ghost_predecessors.insert(ghost.clone());
                    }
                }
            }
        }
        let mut predecessor_keys =
            predecessor_keys.into_iter().collect::<Vec<_>>();
        predecessor_keys.sort_by(|left, right| {
            display_rank
                .get(left)
                .cmp(&display_rank.get(right))
                .then_with(|| left.cmp(right))
        });
        placements.push(ClanWaveMemberPlacementWire {
            key: key.clone(),
            wave_index: slot_wave_index.get(slot).copied(),
            rank: display_rank[key],
            predecessor_keys,
            ghost_predecessors: ghost_predecessors.into_iter().collect(),
            external_wait_count: external_by_member
                .get(key)
                .map_or(0, |waits| waits.len() as u32),
        });
    }

    let numbered_wave_count =
        waves.iter().filter(|wave| wave.number.is_some()).count() as u32;
    let has_seal = waves.iter().any(|wave| wave.seal);
    let layered = waves.len() >= 2 || !unresolved.is_empty();

    ClanWavesWire {
        layered,
        numbered_wave_count,
        has_seal,
        waves,
        members: placements,
        unresolved,
    }
}

fn member_spellings(member: &ClanWaveMemberWire) -> impl Iterator<Item = &str> {
    std::iter::once(member.name.as_str())
        .chain(member.aliases.iter().map(String::as_str))
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

fn is_inside_hood(hood: &str, name: &str) -> bool {
    name == hood
        || name
            .strip_prefix(hood)
            .is_some_and(|suffix| suffix.starts_with('.'))
}

fn is_inside_any_clan_hood(clan_names: &[String], name: &str) -> bool {
    clan_names.iter().any(|clan| is_inside_hood(clan, name))
}

fn canonical_ghost_name(clan_names: &[String], name: &str) -> String {
    let Some(canonical_clan) = clan_names.iter().find(|clan| !clan.is_empty())
    else {
        return name.to_string();
    };
    for clan in clan_names.iter().filter(|clan| !clan.is_empty()) {
        if name == clan {
            return canonical_clan.clone();
        }
        if let Some(suffix) = name
            .strip_prefix(clan)
            .filter(|suffix| suffix.starts_with('.'))
        {
            return format!("{canonical_clan}{suffix}");
        }
    }
    name.to_string()
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
    dependencies: &mut BTreeMap<LayoutNode, BTreeSet<LayoutNode>>,
    owner: &str,
    predecessor: &str,
) {
    if owner != predecessor {
        dependencies
            .entry(LayoutNode::Slot(owner.to_string()))
            .or_default()
            .insert(LayoutNode::Slot(predecessor.to_string()));
    }
}

fn add_dependency_node(
    dependencies: &mut BTreeMap<LayoutNode, BTreeSet<LayoutNode>>,
    owner: &str,
    predecessor: &LayoutNode,
) {
    let owner = LayoutNode::Slot(owner.to_string());
    if &owner != predecessor {
        dependencies
            .entry(owner)
            .or_default()
            .insert(predecessor.clone());
    }
}

fn add_hood_snapshot(
    dependencies: &mut BTreeMap<LayoutNode, BTreeSet<LayoutNode>>,
    owner_slot: &str,
    hood: &str,
    owner_launch_key: &str,
    members: &[&ClanWaveMemberWire],
    slot_of: &BTreeMap<String, String>,
) {
    for candidate in members.iter().filter(|candidate| {
        candidate.launch_key.as_str() < owner_launch_key
            && member_spellings(candidate)
                .any(|spelling| is_inside_hood(hood, spelling))
    }) {
        if let Some(predecessor) = slot_of.get(&candidate.key) {
            add_dependency(dependencies, owner_slot, predecessor);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn member(key: &str, name: &str, launch_key: &str) -> ClanWaveMemberWire {
        ClanWaveMemberWire {
            key: key.to_string(),
            name: name.to_string(),
            launch_key: launch_key.to_string(),
            ..ClanWaveMemberWire::default()
        }
    }

    fn request(
        clan_names: &[&str],
        members: Vec<ClanWaveMemberWire>,
    ) -> ClanWaveRequestWire {
        ClanWaveRequestWire {
            clan_names: clan_names
                .iter()
                .map(|name| (*name).to_string())
                .collect(),
            members,
        }
    }

    fn names(waves: &[ClanWaveWire]) -> Vec<Vec<&str>> {
        waves
            .iter()
            .map(|wave| wave.member_keys.iter().map(String::as_str).collect())
            .collect()
    }

    fn placement<'a>(
        wire: &'a ClanWavesWire,
        key: &str,
    ) -> &'a ClanWaveMemberPlacementWire {
        wire.members
            .iter()
            .find(|member| member.key == key)
            .unwrap()
    }

    #[test]
    fn epic_layers_five_numbered_waves_and_a_seal() {
        let clan = "bob-cli-7v";
        let mut members = (1..=7)
            .map(|number| {
                let name = format!("{clan}.{number}");
                member(&name, &name, &format!("{number:02}"))
            })
            .collect::<Vec<_>>();
        members[3].wait_for = vec![format!("{clan}.1"), format!("{clan}.2")];
        members[4].wait_for = vec![format!("{clan}.3"), format!("{clan}.4")];
        members[5].wait_for = vec![format!("{clan}.5")];
        members[6].wait_for = vec![format!("{clan}.6")];
        let mut land =
            member(&format!("{clan}.land"), &format!("{clan}.land"), "08");
        land.wait_for =
            (1..=7).map(|number| format!("{clan}.{number}")).collect();
        land.seal = true;
        members.push(land);

        let result = project_clan_waves(&request(&[clan], members));

        assert_eq!(
            names(&result.waves),
            vec![
                vec!["bob-cli-7v.1", "bob-cli-7v.2", "bob-cli-7v.3"],
                vec!["bob-cli-7v.4"],
                vec!["bob-cli-7v.5"],
                vec!["bob-cli-7v.6"],
                vec!["bob-cli-7v.7"],
                vec!["bob-cli-7v.land"],
            ]
        );
        assert_eq!(result.numbered_wave_count, 5);
        assert!(result.has_seal);
        let seal = result.waves.last().unwrap();
        assert_eq!((seal.index, seal.number, seal.seal), (6, None, true));
        assert_eq!(placement(&result, "bob-cli-7v.land").wave_index, Some(6));
        assert_eq!(
            placement(&result, "bob-cli-7v.4").predecessor_keys,
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
            let name = format!("{clan}.{suffix}");
            member(&name, &name, &format!("{index:02}"))
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

        let result = project_clan_waves(&request(&[clan], members));

        assert_eq!(
            names(&result.waves),
            vec![
                vec![
                    "research.4j.cdx",
                    "research.4j.cld",
                    "research.4j.grk",
                    "research.4j.gem"
                ],
                vec!["research.4j.final"],
                vec!["research.4j.image", "research.4j.audio"],
                vec!["research.4j.linker"],
            ]
        );
    }

    #[test]
    fn long_chain_and_flat_fanout_report_layered_state() {
        let clan = "toobig-5y";
        let mut chain = (1..=14)
            .map(|number| {
                let name = format!("{clan}.{number}");
                member(&name, &name, &format!("{number:02}"))
            })
            .collect::<Vec<_>>();
        for (number, phase) in chain.iter_mut().enumerate().skip(1) {
            phase.wait_for = vec![format!("{clan}.{number}")];
        }
        let chain_result = project_clan_waves(&request(&[clan], chain));
        assert_eq!(chain_result.numbered_wave_count, 14);
        assert!(chain_result.layered);
        assert_eq!(
            placement(&chain_result, "toobig-5y.14").wave_index,
            Some(14)
        );

        let flat = project_clan_waves(&request(
            &["review.2"],
            vec![
                member("review-a", "review.2.a", "01"),
                member("review-b", "review.2.b", "02"),
                member("review-c", "review.2.c", "03"),
            ],
        ));
        assert_eq!(
            names(&flat.waves),
            vec![vec!["review-a", "review-b", "review-c"]]
        );
        assert!(!flat.layered);
    }

    #[test]
    fn aliases_sessions_and_retry_members_resolve_by_identity() {
        let mut root = member("root-key", "clan.a", "01");
        root.aliases = vec!["a".to_string()];
        let mut retry = member("retry-key", "clan.a.r1", "03");
        retry.retry_of = Some("root-key".to_string());
        retry.wait_for = vec!["clan.b".to_string()];
        let mut child = member("child-key", "clan.c", "04");
        child.wait_for =
            vec!["clan.a.r1--turn-2".to_string(), "a--plan".to_string()];
        let result = project_clan_waves(&request(
            &["clan"],
            vec![root, member("blocker-key", "clan.b", "02"), retry, child],
        ));

        assert_eq!(
            names(&result.waves),
            vec![
                vec!["blocker-key"],
                vec!["root-key", "retry-key"],
                vec!["child-key"]
            ]
        );
        assert_eq!(placement(&result, "retry-key").wave_index, Some(2));
        assert_eq!(
            placement(&result, "root-key").predecessor_keys,
            vec!["blocker-key"]
        );
        assert_eq!(
            placement(&result, "retry-key").predecessor_keys,
            vec!["blocker-key"]
        );
        assert_eq!(
            placement(&result, "child-key").predecessor_keys,
            vec!["root-key"]
        );
    }

    #[test]
    fn ghost_only_wave_preserves_numbering_and_external_wait_counts() {
        let mut final_member = member("final", "clan.final", "02");
        final_member.wait_for = vec!["clan.dismissed".to_string()];
        let mut external = member("external", "clan.external", "03");
        external.wait_for = vec![
            "clan.final".to_string(),
            "other-clan.member".to_string(),
            "time=5m".to_string(),
            "for_epic=e2".to_string(),
        ];
        external.wait_for_beads = vec!["foreign-bead".to_string()];
        external.wait_for_hoods = vec!["another-hood".to_string()];

        let result = project_clan_waves(&request(
            &["clan"],
            vec![final_member, external],
        ));

        assert_eq!(result.waves[0].member_keys, Vec::<String>::new());
        assert_eq!(result.waves[0].ghost_names, vec!["clan.dismissed"]);
        assert_eq!(
            (result.waves[0].index, result.waves[0].number),
            (1, Some(1))
        );
        assert_eq!(result.numbered_wave_count, 3);
        assert_eq!(placement(&result, "final").wave_index, Some(2));
        assert_eq!(placement(&result, "external").wave_index, Some(3));
        assert_eq!(
            placement(&result, "final").ghost_predecessors,
            vec!["clan.dismissed"]
        );
        assert_eq!(placement(&result, "external").external_wait_count, 3);
    }

    #[test]
    fn bead_edges_keep_the_land_on_the_seal_layer() {
        let mut first = member("first", "clan.first", "01");
        first.phase_bead_id = Some("phase-1".to_string());
        let mut land = member("land", "clan.land", "02");
        land.wait_for_beads = vec!["phase-1".to_string()];
        land.seal = true;

        let result = project_clan_waves(&request(&["clan"], vec![first, land]));

        assert_eq!(result.numbered_wave_count, 1);
        assert!(result.has_seal);
        assert_eq!(result.waves[0].member_keys, vec!["first"]);
        assert_eq!(result.waves[1].member_keys, vec!["land"]);
        assert_eq!(placement(&result, "land").wave_index, Some(2));
        assert_eq!(placement(&result, "land").predecessor_keys, vec!["first"]);
    }

    #[test]
    fn clan_name_and_subhood_waits_use_launch_snapshot_semantics() {
        let mut presented_first = member("first", "clan.first", "01");
        presented_first.aliases = vec!["fleet/clan.first".to_string()];
        let mut second = member("second", "clan.second", "02");
        second.aliases = vec!["fleet/clan.second".to_string()];
        second.wait_for = vec!["fleet/clan".to_string()];
        let mut nested = member("nested", "clan.feature.root", "03");
        let mut fourth = member("fourth", "clan.fourth", "04");
        fourth.wait_for_hoods = vec!["fleet/clan.feature".to_string()];
        let mut fifth = member("fifth", "clan.fifth", "05");
        fifth.wait_for_hoods = vec!["clan".to_string()];
        nested.aliases = vec!["fleet/clan.feature.root".to_string()];

        let result = project_clan_waves(&request(
            &["clan", "fleet/clan"],
            vec![presented_first, second, nested, fourth, fifth],
        ));

        assert_eq!(
            placement(&result, "second").predecessor_keys,
            vec!["first"]
        );
        assert_eq!(
            placement(&result, "fourth").predecessor_keys,
            vec!["nested"]
        );
        assert_eq!(
            placement(&result, "fifth").predecessor_keys,
            vec!["first", "nested", "second", "fourth"]
        );
        assert_eq!(placement(&result, "second").wave_index, Some(2));
        assert_eq!(placement(&result, "fourth").wave_index, Some(2));
        assert_eq!(placement(&result, "fifth").wave_index, Some(3));
    }

    #[test]
    fn late_joiners_without_waits_stay_in_the_first_layer() {
        let mut dependent = member("dependent", "clan.dependent", "02");
        dependent.wait_for = vec!["clan.first".to_string()];
        let result = project_clan_waves(&request(
            &["clan"],
            vec![
                dependent,
                member("late", "clan.late", "03"),
                member("first", "clan.first", "01"),
            ],
        ));

        assert_eq!(
            names(&result.waves),
            vec![vec!["first", "late"], vec!["dependent"]]
        );
    }

    #[test]
    fn cycles_and_their_dependents_are_unresolved_without_dropping_members() {
        let mut a = member("a", "clan.a", "03");
        let mut b = member("b", "clan.b", "02");
        let mut c = member("c", "clan.c", "01");
        a.wait_for = vec!["clan.b".to_string()];
        b.wait_for = vec!["clan.a".to_string()];
        c.wait_for = vec!["clan.b".to_string()];

        let result = project_clan_waves(&request(&["clan"], vec![a, b, c]));

        assert!(result.waves.is_empty());
        assert_eq!(result.unresolved, vec!["c", "b", "a"]);
        assert!(result.layered);
        assert!(result
            .members
            .iter()
            .all(|member| member.wave_index.is_none()));
    }

    #[test]
    fn diamond_and_asymmetric_overlap_keep_predecessors_in_display_order() {
        let mut left_branch = member("left", "diamond.left", "02");
        left_branch.wait_for = vec!["diamond.root".to_string()];
        let mut right_branch = member("right", "diamond.right", "03");
        right_branch.wait_for = vec!["diamond.root".to_string()];
        let mut join_branch = member("join", "diamond.join", "04");
        join_branch.wait_for =
            vec!["diamond.left".to_string(), "diamond.right".to_string()];
        let diamond = project_clan_waves(&request(
            &["diamond"],
            vec![
                join_branch,
                right_branch,
                left_branch,
                member("root", "diamond.root", "01"),
            ],
        ));
        assert_eq!(
            names(&diamond.waves),
            vec![vec!["root"], vec!["left", "right"], vec!["join"]]
        );

        let mut left = member("p", "clan.p", "02");
        left.wait_for = vec!["clan.r".to_string()];
        let mut m = member("m", "clan.m", "04");
        m.wait_for = vec!["clan.p".to_string(), "clan.q".to_string()];
        let q = member("q", "clan.q", "03");
        let r = member("r", "clan.r", "01");

        let result =
            project_clan_waves(&request(&["clan"], vec![m, q, left, r]));

        assert_eq!(
            names(&result.waves),
            vec![vec!["r", "q"], vec!["p"], vec!["m"]]
        );
        assert_eq!(placement(&result, "m").predecessor_keys, vec!["q", "p"]);

        let mut branch = member("branch", "asym.branch", "02");
        branch.wait_for = vec!["asym.root".to_string()];
        let mut join = member("join", "asym.join", "04");
        join.wait_for =
            vec!["asym.branch".to_string(), "asym.free".to_string()];
        let asym = project_clan_waves(&request(
            &["asym"],
            vec![
                join,
                member("free", "asym.free", "03"),
                branch,
                member("root", "asym.root", "01"),
            ],
        ));
        assert_eq!(
            names(&asym.waves),
            vec![vec!["root", "free"], vec!["branch"], vec!["join"]]
        );
    }

    #[test]
    fn duplicate_names_resolve_to_every_member_key() {
        let mut first = member("first-key", "clan.same", "01");
        first.aliases = vec!["presented/same".to_string()];
        let mut second = member("second-key", "clan.other", "02");
        second.aliases = vec!["presented/same".to_string()];
        let mut waiter = member("waiter", "clan.waiter", "03");
        waiter.wait_for = vec!["presented/same".to_string()];

        let result = project_clan_waves(&request(
            &["clan"],
            vec![first, second, waiter],
        ));

        assert_eq!(result.members.len(), 3);
        assert_eq!(
            placement(&result, "waiter").predecessor_keys,
            vec!["first-key", "second-key"]
        );
        assert_eq!(placement(&result, "waiter").wave_index, Some(2));
    }

    #[test]
    fn retries_stay_after_the_root_even_when_another_member_launched_between_them(
    ) {
        let root = member("root", "clan.root", "01");
        let other = member("other", "clan.other", "02");
        let mut retry_one = member("retry-one", "clan.root.retry-1", "03");
        retry_one.retry_of = Some("root".to_string());
        let mut retry_two = member("retry-two", "clan.root.retry-2", "04");
        retry_two.retry_of = Some("root".to_string());

        let result = project_clan_waves(&request(
            &["clan"],
            vec![retry_two, other, retry_one, root],
        ));

        assert_eq!(
            names(&result.waves),
            vec![vec!["root", "retry-one", "retry-two", "other"]]
        );
        assert_eq!(placement(&result, "retry-two").wave_index, Some(1));
    }

    #[test]
    fn land_sharing_the_final_layer_stays_numbered() {
        let first = member("first", "clan.first", "01");
        let mut second = member("second", "clan.second", "02");
        second.wait_for = vec!["clan.first".to_string()];
        let mut land = member("land", "clan.land", "03");
        land.wait_for = vec!["clan.first".to_string()];
        land.seal = true;
        let result =
            project_clan_waves(&request(&["clan"], vec![first, second, land]));

        assert_eq!(result.numbered_wave_count, 2);
        assert!(!result.has_seal);
        assert!(result.waves.iter().all(|wave| !wave.seal));
        assert_eq!(placement(&result, "land").wave_index, Some(2));
        assert_eq!(
            names(&result.waves),
            vec![vec!["first"], vec!["second", "land"]]
        );
    }

    #[test]
    fn serde_defaults_cover_optional_and_list_fields() {
        let wire: ClanWaveMemberWire =
            serde_json::from_value(serde_json::json!({
                "key": "k",
                "name": "clan.member",
            }))
            .unwrap();
        assert!(wire.aliases.is_empty());
        assert!(wire.retry_of.is_none());
        assert!(!wire.seal);

        let request: ClanWaveRequestWire =
            serde_json::from_value(serde_json::json!({
                "members": [{"key": "k", "name": "clan.member"}],
            }))
            .unwrap();
        assert!(request.clan_names.is_empty());
    }
}
