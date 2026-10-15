//! Total, deterministic Kahn layering for dependency graphs.

use std::collections::{BTreeMap, BTreeSet};

/// Dependency layers plus every node that could not be placed because it
/// belongs to, or depends on, a cycle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DependencyWaves<T> {
    pub waves: Vec<Vec<T>>,
    pub unresolved: BTreeSet<T>,
}

/// Layer the nodes in `ids` by their longest dependency path.
///
/// Dependencies outside `ids` are ignored. Ties within a layer use the
/// supplied order key and then the node itself, so callers get stable output
/// even when an order key is missing or shared. Cycles do not fail the
/// projection: all nodes left after Kahn's algorithm, including their
/// downstream dependents, are returned in `unresolved`.
pub fn layer_dependency_waves<T, K>(
    ids: &BTreeSet<T>,
    dependencies: &BTreeMap<T, BTreeSet<T>>,
    order: &BTreeMap<T, K>,
) -> DependencyWaves<T>
where
    T: Clone + Ord,
    K: Ord,
{
    let mut waves = Vec::new();
    let mut placed = BTreeSet::new();
    let mut remaining = ids.clone();

    while !remaining.is_empty() {
        let mut ready: Vec<T> = remaining
            .iter()
            .filter(|id| {
                dependencies.get(*id).is_none_or(|blockers| {
                    blockers
                        .iter()
                        .filter(|blocker| ids.contains(*blocker))
                        .all(|blocker| placed.contains(blocker))
                })
            })
            .cloned()
            .collect();
        if ready.is_empty() {
            break;
        }
        ready.sort_by(|left, right| {
            order
                .get(left)
                .cmp(&order.get(right))
                .then_with(|| left.cmp(right))
        });
        placed.extend(ready.iter().cloned());
        for id in &ready {
            remaining.remove(id);
        }
        waves.push(ready);
    }

    DependencyWaves {
        waves,
        unresolved: remaining,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layers_diamond_by_longest_dependency_path_and_order_key() {
        let ids = BTreeSet::from(["a", "b", "c", "d"]);
        let dependencies = BTreeMap::from([
            ("a", BTreeSet::new()),
            ("b", BTreeSet::from(["a"])),
            ("c", BTreeSet::from(["a"])),
            ("d", BTreeSet::from(["b", "c"])),
        ]);
        let order = BTreeMap::from([("a", 0), ("b", 2), ("c", 1), ("d", 0)]);

        let result = layer_dependency_waves(&ids, &dependencies, &order);

        assert_eq!(result.waves, vec![vec!["a"], vec!["c", "b"], vec!["d"]]);
        assert!(result.unresolved.is_empty());
    }

    #[test]
    fn keeps_cycles_and_their_dependents_in_a_deterministic_unresolved_set() {
        let ids = BTreeSet::from(["a", "b", "c", "d"]);
        let dependencies = BTreeMap::from([
            ("a", BTreeSet::from(["b"])),
            ("b", BTreeSet::from(["a"])),
            ("c", BTreeSet::from(["b"])),
            ("d", BTreeSet::new()),
        ]);
        let order: BTreeMap<&str, usize> = BTreeMap::new();

        let result = layer_dependency_waves(&ids, &dependencies, &order);

        assert_eq!(result.waves, vec![vec!["d"]]);
        assert_eq!(result.unresolved, BTreeSet::from(["a", "b", "c"]));
    }

    #[test]
    fn ignores_dependencies_outside_the_projected_graph() {
        let ids = BTreeSet::from(["a"]);
        let dependencies =
            BTreeMap::from([("a", BTreeSet::from(["external"]))]);

        let result = layer_dependency_waves(
            &ids,
            &dependencies,
            &BTreeMap::<&str, usize>::new(),
        );

        assert_eq!(result.waves, vec![vec!["a"]]);
        assert!(result.unresolved.is_empty());
    }
}
