//! Ancestor propagation: every stranded leaf marks its candidate ancestors.
//!
//! Each stranded leaf walks `parent_id` upward through candidate beads. At
//! every ancestor the leaf appends itself to `blocked_by` and the ancestor to
//! its own `blocks` (nearest first); `owned`/`delegated`/`settling`
//! ancestors switch to `blocked`. `stranded` and `unknown` ancestors keep
//! their state, and the walk continues through them so the root sees every
//! leaf. Every verdict also records its full candidate ancestor chain in
//! `blocks`, nearest first.

use std::collections::{BTreeMap, BTreeSet};

use super::wire::{
    StrandedWorkBeadFactsWire, StrandedWorkVerdictWire, STRANDED_STATE_BLOCKED,
};

/// Candidate ancestor chain above `start_id`, nearest first.
///
/// The walk stops at a missing parent, an empty parent id, a non-candidate
/// bead, or a cycle. Only candidates are recorded: a non-candidate parent
/// closes the chain because it carries no verdict to switch.
fn ancestor_chain(
    beads: &[StrandedWorkBeadFactsWire],
    candidates: &BTreeSet<String>,
    start_id: &str,
) -> Vec<String> {
    let parents: BTreeMap<&str, &str> = beads
        .iter()
        .filter_map(|bead| {
            let parent =
                bead.parent_id.as_deref().filter(|id| !id.is_empty())?;
            Some((bead.id.as_str(), parent))
        })
        .collect();
    let mut chain = Vec::new();
    let mut visited = BTreeSet::new();
    visited.insert(start_id.to_string());
    let mut current = parents.get(start_id).copied();
    while let Some(id) = current {
        if !visited.insert(id.to_string()) {
            break;
        }
        if !candidates.contains(id) {
            break;
        }
        chain.push(id.to_string());
        current = parents.get(id).copied();
    }
    chain
}

/// Apply ancestor propagation in place. `blocked_by` lists come out sorted
/// and deduplicated; `blocks` stays nearest-first.
pub fn apply_stranded_propagation(
    verdicts: &mut [StrandedWorkVerdictWire],
    beads: &[StrandedWorkBeadFactsWire],
) {
    let candidates: BTreeSet<String> = verdicts
        .iter()
        .map(|verdict| verdict.bead_id.clone())
        .collect();
    let index: BTreeMap<String, usize> = verdicts
        .iter()
        .enumerate()
        .map(|(position, verdict)| (verdict.bead_id.clone(), position))
        .collect();

    for verdict in verdicts.iter_mut() {
        verdict.blocks = ancestor_chain(beads, &candidates, &verdict.bead_id);
    }

    let leaves: Vec<(String, Vec<String>)> = verdicts
        .iter()
        .filter(|verdict| verdict.is_stranded_leaf())
        .map(|verdict| (verdict.bead_id.clone(), verdict.blocks.clone()))
        .collect();
    for (leaf_id, ancestors) in leaves {
        for ancestor_id in ancestors {
            if ancestor_id == leaf_id {
                continue;
            }
            if let Some(&position) = index.get(&ancestor_id) {
                let ancestor = &mut verdicts[position];
                if !ancestor.blocked_by.contains(&leaf_id) {
                    ancestor.blocked_by.push(leaf_id.clone());
                }
                if ancestor.is_blockable() {
                    ancestor.state = STRANDED_STATE_BLOCKED.to_string();
                }
            }
        }
    }

    for verdict in verdicts.iter_mut() {
        verdict.blocked_by.sort();
        verdict.blocked_by.dedup();
    }
}
