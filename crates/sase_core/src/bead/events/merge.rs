//! Three-way stream merge and bead-id relocation.
//!
//! Owns `merge_bead_event_streams`, duplicate-creation splitting,
//! subtree remapping, the `StreamHead` interleave ordering, and the
//! deterministic event-id mint shared with import.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::super::wire::{BeadError, DependencyWire};
use super::wire::{
    remap_link_target_ref, BeadEventOperationWire, BeadEventPayloadWire,
    BeadEventRecordWire, BeadEventStreamWire, BEAD_EVENT_SCHEMA_VERSION,
};

/// Outcome of merging one conflicted bead event stream.
///
/// Two clones minting from their own counters can allocate the same bead id,
/// so both sides add an `issue_created` event for it. The union of those
/// branches is unreducible, and before relocation that error wedged every
/// later sync behind the same conflicted store. Renumbering the losing side
/// keeps the merge total.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadEventStreamMergeWire {
    /// Merged stream, keeping the winning side of any id collision.
    pub merged: BeadEventStreamWire,
    /// Losing side of a top-level id collision, renamed onto a free id.
    #[serde(default)]
    pub relocated: Option<BeadEventStreamWire>,
    /// Every `(old_id, new_id)` pair this merge had to renumber.
    #[serde(default)]
    pub relocations: Vec<(String, String)>,
    /// Typed relocation records for callers that need to resolve created ids.
    #[serde(default)]
    pub relocation_records: Vec<BeadIdRelocationWire>,
}

/// One bead id remapped during conflict repair.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadIdRelocationWire {
    pub old_id: String,
    pub new_id: String,
    pub kind: BeadIdRelocationKindWire,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BeadIdRelocationKindWire {
    TopLevelDuplicate,
    ChildDuplicate,
}

/// Which merge input contributed an event, used to attribute id collisions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BranchTag {
    Base,
    Ours,
    Theirs,
    Both,
}

pub fn merge_bead_event_streams(
    base: &BeadEventStreamWire,
    ours: &BeadEventStreamWire,
    theirs: &BeadEventStreamWire,
) -> Result<BeadEventStreamWire, BeadError> {
    Ok(
        merge_bead_event_streams_with_relocation(base, ours, theirs, None)?
            .merged,
    )
}

/// Merge one stream, relocating a duplicated top-level bead onto a free id.
///
/// `relocation_issue_id` must be an id no stream in the store uses yet; the
/// caller owns that allocation because a single stream cannot see the rest of
/// the store. Passing `None` keeps the historical behavior of failing on a
/// duplicate `issue_created`. Duplicated *child* ids need no caller input:
/// the next free sibling number is derivable from the stream itself.
///
/// Argument roles: `ours` is the local branch and `theirs` is the published
/// upstream branch. The Python resolver calls this as
/// `merge(base, local, upstream)` (`src/sase/bead/conflict_resolver.py`), so
/// `theirs` is upstream. When both sides share additions in orders that
/// cannot be reconciled, the upstream (`theirs`) order wins.
pub fn merge_bead_event_streams_with_relocation(
    base: &BeadEventStreamWire,
    ours: &BeadEventStreamWire,
    theirs: &BeadEventStreamWire,
    relocation_issue_id: Option<&str>,
) -> Result<BeadEventStreamMergeWire, BeadError> {
    base.validate()?;
    ours.validate()?;
    theirs.validate()?;
    if ours.stream_id != theirs.stream_id
        || ours.root_issue_id != theirs.root_issue_id
    {
        return Err(BeadError::validation(format!(
            "cannot merge bead event streams with different ids: {} != {}",
            ours.stream_id, theirs.stream_id
        )));
    }
    if base.stream_id != ours.stream_id
        || base.root_issue_id != ours.root_issue_id
    {
        return Err(BeadError::validation(format!(
            "cannot merge base bead event stream {} into {}",
            base.stream_id, ours.stream_id
        )));
    }

    let ours_base_indexes = validate_append_only_branch(base, ours, "ours")?;
    let theirs_base_indexes =
        validate_append_only_branch(base, theirs, "theirs")?;
    let base_events = event_keys(&base.events)?;
    let ours_additions =
        branch_additions(ours, &ours_base_indexes, &base_events)?;
    let theirs_additions =
        branch_additions(theirs, &theirs_base_indexes, &base_events)?;
    let ordered = interleave_additions(ours_additions, theirs_additions)?;
    let mut merged = base.clone();
    let mut provenance = vec![BranchTag::Base; base.events.len()];
    for (_, (event, tag)) in ordered {
        merged.events.push(event);
        provenance.push(tag);
    }

    let (relocated, relocation_records) = split_duplicate_creations(
        &mut merged,
        provenance,
        relocation_issue_id,
    )?;
    merged.validate()?;
    if let Some(stream) = &relocated {
        stream.validate()?;
    }
    let relocations = relocation_records
        .iter()
        .map(|record| (record.old_id.clone(), record.new_id.clone()))
        .collect();
    Ok(BeadEventStreamMergeWire {
        merged,
        relocated,
        relocations,
        relocation_records,
    })
}

/// A relocated stream, if any, plus every id relocation applied.
type DuplicateCreationSplit =
    (Option<BeadEventStreamWire>, Vec<BeadIdRelocationWire>);

/// Renumber every bead whose `issue_created` event was minted twice.
///
/// Each round resolves exactly one collision and then re-scans, because
/// relocating a colliding root also carries away its children, which is often
/// what made those children look duplicated in the first place.
fn split_duplicate_creations(
    merged: &mut BeadEventStreamWire,
    provenance: Vec<BranchTag>,
    relocation_issue_id: Option<&str>,
) -> Result<DuplicateCreationSplit, BeadError> {
    let mut tagged: Vec<(BeadEventRecordWire, BranchTag)> =
        merged.events.drain(..).zip(provenance).collect();
    let mut relocated: Option<BeadEventStreamWire> = None;
    let mut relocations: Vec<BeadIdRelocationWire> = Vec::new();

    while let Some((issue_id, loser_tag)) = losing_creation(&tagged)? {
        if issue_id == merged.stream_id {
            let Some(new_id) = relocation_issue_id else {
                return Err(BeadError::validation(format!(
                    "duplicate issue_created event for {issue_id}"
                )));
            };
            if new_id.is_empty() || new_id == issue_id {
                return Err(BeadError::validation(format!(
                    "invalid relocation id for duplicate bead {issue_id}"
                )));
            }
            let moved = extract_subtree(&mut tagged, &issue_id, loser_tag);
            relocated = Some(relocated_stream(new_id, &issue_id, moved)?);
            relocations.push(BeadIdRelocationWire {
                old_id: issue_id,
                new_id: new_id.to_string(),
                kind: BeadIdRelocationKindWire::TopLevelDuplicate,
            });
        } else {
            let new_id = next_sibling_issue_id(&tagged, &issue_id)?;
            remap_subtree(
                &mut tagged,
                &issue_id,
                loser_tag,
                &new_id,
                &merged.stream_id,
            )?;
            relocations.push(BeadIdRelocationWire {
                old_id: issue_id,
                new_id,
                kind: BeadIdRelocationKindWire::ChildDuplicate,
            });
        }
    }

    merged.events = tagged.into_iter().map(|(event, _)| event).collect();
    Ok((relocated, relocations))
}

/// Return the issue id and contributing branch of the next creation to move.
fn losing_creation(
    tagged: &[(BeadEventRecordWire, BranchTag)],
) -> Result<Option<(String, BranchTag)>, BeadError> {
    let mut creations: BTreeMap<&str, Vec<usize>> = BTreeMap::new();
    for (index, (event, _)) in tagged.iter().enumerate() {
        if event.operation == BeadEventOperationWire::IssueCreated {
            creations
                .entry(event.issue_id.as_str())
                .or_default()
                .push(index);
        }
    }
    for (issue_id, indexes) in creations {
        if indexes.len() < 2 {
            continue;
        }
        // Whichever creation the merge base already carried is authoritative;
        // otherwise the older creation keeps the id so both clones agree
        // regardless of which side git happened to call "ours".
        let winner = indexes
            .iter()
            .copied()
            .find(|index| tagged[*index].1 == BranchTag::Base)
            .unwrap_or_else(|| {
                *indexes
                    .iter()
                    .min_by_key(|index| {
                        let event = &tagged[**index].0;
                        (event.timestamp.as_str(), event.event_id.as_str())
                    })
                    .expect("collision groups are non-empty")
            });
        let loser = indexes
            .iter()
            .copied()
            .find(|index| *index != winner)
            .expect("collision groups hold at least two creations");
        let loser_tag = tagged[loser].1;
        // A single side that minted the same id twice, or two byte-identical
        // creations, cannot be told apart by provenance, so there is nothing
        // safe to relocate.
        let ambiguous = matches!(loser_tag, BranchTag::Base | BranchTag::Both)
            || indexes
                .iter()
                .filter(|index| **index != winner)
                .any(|index| tagged[*index].1 != loser_tag);
        if ambiguous {
            return Err(BeadError::validation(format!(
                "duplicate issue_created event for {issue_id}"
            )));
        }
        return Ok(Some((issue_id.to_string(), loser_tag)));
    }
    Ok(None)
}

/// Remove and return the losing branch's events for `issue_id` and its children.
fn extract_subtree(
    tagged: &mut Vec<(BeadEventRecordWire, BranchTag)>,
    issue_id: &str,
    tag: BranchTag,
) -> Vec<BeadEventRecordWire> {
    let mut moved = Vec::new();
    let mut kept = Vec::with_capacity(tagged.len());
    for (event, event_tag) in tagged.drain(..) {
        if event_tag == tag && issue_id_in_subtree(&event.issue_id, issue_id) {
            moved.push(event);
        } else {
            kept.push((event, event_tag));
        }
    }
    *tagged = kept;
    moved
}

fn relocated_stream(
    new_id: &str,
    old_id: &str,
    events: Vec<BeadEventRecordWire>,
) -> Result<BeadEventStreamWire, BeadError> {
    let events = events
        .into_iter()
        .enumerate()
        .map(|(index, event)| {
            remapped_event(event, old_id, new_id, new_id, index + 1)
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(BeadEventStreamWire {
        stream_id: new_id.to_string(),
        root_issue_id: new_id.to_string(),
        events,
    })
}

/// Rewrite a duplicated child bead onto a free sibling number, in place.
fn remap_subtree(
    tagged: &mut [(BeadEventRecordWire, BranchTag)],
    issue_id: &str,
    tag: BranchTag,
    new_id: &str,
    stream_id: &str,
) -> Result<(), BeadError> {
    for (index, (event, event_tag)) in tagged.iter_mut().enumerate() {
        if *event_tag != tag || !issue_id_in_subtree(&event.issue_id, issue_id)
        {
            continue;
        }
        *event = remapped_event(
            event.clone(),
            issue_id,
            new_id,
            stream_id,
            index + 1,
        )?;
    }
    Ok(())
}

fn issue_id_in_subtree(issue_id: &str, root: &str) -> bool {
    issue_id == root || issue_id.starts_with(&format!("{root}."))
}

pub(super) fn remapped_id(
    issue_id: &str,
    old_id: &str,
    new_id: &str,
) -> String {
    if issue_id == old_id {
        return new_id.to_string();
    }
    match issue_id.strip_prefix(&format!("{old_id}.")) {
        Some(rest) => format!("{new_id}.{rest}"),
        None => issue_id.to_string(),
    }
}

/// Rewrite every id a single event carries, then re-mint its event id.
///
/// The event id embeds the stream, ordinal, and issue id, so leaving the
/// original in place would let a relocated event collide with the bead it was
/// moved away from.
fn remapped_event(
    mut event: BeadEventRecordWire,
    old_id: &str,
    new_id: &str,
    stream_id: &str,
    ordinal: usize,
) -> Result<BeadEventRecordWire, BeadError> {
    event.issue_id = remapped_id(&event.issue_id, old_id, new_id);
    match &mut event.payload {
        BeadEventPayloadWire::IssueCreated { issue } => {
            issue.id = remapped_id(&issue.id, old_id, new_id);
            if let Some(parent_id) = &issue.parent_id {
                issue.parent_id = Some(remapped_id(parent_id, old_id, new_id));
            }
            for dependency in &mut issue.dependencies {
                remap_dependency(dependency, old_id, new_id);
            }
        }
        BeadEventPayloadWire::IssueClosed {
            forced_descendant_ids,
            ..
        } => {
            for descendant_id in forced_descendant_ids.iter_mut() {
                *descendant_id = remapped_id(descendant_id, old_id, new_id);
            }
        }
        BeadEventPayloadWire::IssueRemoved {
            cascade_removed_issue_ids,
        } => {
            for removed_id in cascade_removed_issue_ids.iter_mut() {
                *removed_id = remapped_id(removed_id, old_id, new_id);
            }
        }
        BeadEventPayloadWire::DependencyAdded { dependency }
        | BeadEventPayloadWire::DependencyRemoved { dependency } => {
            remap_dependency(dependency, old_id, new_id);
        }
        BeadEventPayloadWire::LinkAdded { target_ref, .. }
        | BeadEventPayloadWire::LinkRemoved { target_ref, .. } => {
            *target_ref = remap_link_target_ref(target_ref, old_id, new_id);
        }
        _ => {}
    }
    event.event_id = mint_bead_event_id(
        stream_id,
        ordinal,
        &event.timestamp,
        &event.actor,
        event.operation,
        &event.issue_id,
        &event.payload,
    )?;
    event.validate()?;
    Ok(event)
}

fn remap_dependency(
    dependency: &mut DependencyWire,
    old_id: &str,
    new_id: &str,
) {
    dependency.issue_id = remapped_id(&dependency.issue_id, old_id, new_id);
    dependency.depends_on_id =
        remapped_id(&dependency.depends_on_id, old_id, new_id);
}

/// Return the next unused direct-child id next to a duplicated child bead.
fn next_sibling_issue_id(
    tagged: &[(BeadEventRecordWire, BranchTag)],
    issue_id: &str,
) -> Result<String, BeadError> {
    let Some((parent_id, _)) = issue_id.rsplit_once('.') else {
        return Err(BeadError::validation(format!(
            "duplicate issue_created event for {issue_id}"
        )));
    };
    let child_prefix = format!("{parent_id}.");
    let mut max_child = 0u64;
    for (event, _) in tagged {
        for candidate in event_issue_ids(event) {
            let Some(suffix) = candidate.strip_prefix(&child_prefix) else {
                continue;
            };
            if let Ok(counter) = suffix.parse::<u64>() {
                max_child = max_child.max(counter);
            }
        }
    }
    Ok(format!("{child_prefix}{}", max_child + 1))
}

fn event_issue_ids(event: &BeadEventRecordWire) -> Vec<String> {
    let mut ids = vec![event.issue_id.clone()];
    if let BeadEventPayloadWire::IssueCreated { issue } = &event.payload {
        ids.push(issue.id.clone());
    }
    ids
}

/// Serialized event key plus its record, in branch order.
type BranchAdditions = Vec<(String, BeadEventRecordWire)>;
/// Interleaved additions with per-event branch provenance.
type OrderedAdditions = Vec<(String, (BeadEventRecordWire, BranchTag))>;
/// Heap key for deterministic Kahn ties: the union key plus the node key.
type UnionHeapKey = (String, usize, String, String, String);

/// One branch's genuine additions in branch order, deduplicated.
///
/// `base_indexes` marks the branch positions that carry base events (in any
/// order after pure-reorder tolerance); everything else whose serialization
/// is not a base event is a genuine addition. Within-side duplicates keep
/// their first occurrence so the interleave graph stays a chain.
fn branch_additions(
    branch: &BeadEventStreamWire,
    base_indexes: &BTreeSet<usize>,
    base_events: &BTreeSet<String>,
) -> Result<BranchAdditions, BeadError> {
    let mut additions = Vec::new();
    let mut seen = BTreeSet::new();
    for (index, event) in branch.events.iter().enumerate() {
        if base_indexes.contains(&index) {
            continue;
        }
        let key = serde_json::to_string(event)?;
        if base_events.contains(&key) {
            continue;
        }
        if seen.insert(key.clone()) {
            additions.push((key, event.clone()));
        }
    }
    Ok(additions)
}

/// Order-preserving interleave of the two branches' additions.
///
/// Each side keeps its own relative order; when the heads disagree the
/// smaller `event_union_key` wins so the result stays deterministic. An
/// event present on both sides appears once. The union of both orders is
/// topologically sorted, which for disjoint additions is exactly a
/// head-picking k-way merge. When both sides share additions in conflicting
/// orders the graph cycles, and the upstream (`theirs`) order wins: only
/// `theirs` edges plus `ours` unique-to-unique edges are kept, so the result
/// always contains `theirs` as an exact ordered subsequence, and contains
/// `ours` whenever such an order exists.
fn interleave_additions(
    ours_additions: BranchAdditions,
    theirs_additions: BranchAdditions,
) -> Result<OrderedAdditions, BeadError> {
    let mut events: BTreeMap<String, BeadEventRecordWire> = BTreeMap::new();
    for (key, event) in ours_additions.iter().cloned() {
        events.insert(key, event);
    }
    for (key, event) in theirs_additions.iter().cloned() {
        events.entry(key).or_insert(event);
    }
    if events.is_empty() {
        return Ok(Vec::new());
    }
    let ours_keys: Vec<String> =
        ours_additions.iter().map(|(key, _)| key.clone()).collect();
    let theirs_keys: Vec<String> = theirs_additions
        .iter()
        .map(|(key, _)| key.clone())
        .collect();
    let shared: BTreeSet<String> = ours_keys
        .iter()
        .filter(|key| theirs_keys.contains(key))
        .cloned()
        .collect();
    let ours_shared_order: Vec<&String> = ours_keys
        .iter()
        .filter(|key| shared.contains(*key))
        .collect();
    let theirs_shared_order: Vec<&String> = theirs_keys
        .iter()
        .filter(|key| shared.contains(*key))
        .collect();
    let reconciled = ours_shared_order == theirs_shared_order;

    let mut ordered_keys =
        topo_interleave(&ours_keys, &theirs_keys, &events, reconciled);
    if ordered_keys.is_none() {
        ordered_keys =
            topo_interleave(&Vec::new(), &theirs_keys, &events, false);
    }
    let ordered_keys = ordered_keys.unwrap_or_else(|| {
        let mut keys: Vec<String> = theirs_keys.clone();
        for key in &ours_keys {
            if !keys.contains(key) {
                keys.push(key.clone());
            }
        }
        keys
    });
    let mut ordered = Vec::with_capacity(ordered_keys.len());
    for key in ordered_keys {
        let event = events.get(&key).cloned().ok_or_else(|| {
            BeadError::validation("merge interleave missing event")
        })?;
        let tag = if ours_keys.contains(&key) && theirs_keys.contains(&key) {
            BranchTag::Both
        } else if ours_keys.contains(&key) {
            BranchTag::Ours
        } else {
            BranchTag::Theirs
        };
        ordered.push((key, (event, tag)));
    }
    Ok(ordered)
}

/// Kahn topological sort of two chains, breaking ties by `event_union_key`.
///
/// Returns `None` when the combined order cycles. `reconciled` selects the
/// edge set: with reconciled shared orders every consecutive edge from both
/// sides is kept; on conflict only `theirs` edges plus `ours`
/// unique-to-unique edges are kept so upstream wins.
fn topo_interleave(
    ours_keys: &[String],
    theirs_keys: &[String],
    events: &BTreeMap<String, BeadEventRecordWire>,
    reconciled: bool,
) -> Option<Vec<String>> {
    let theirs_set: BTreeSet<&String> = theirs_keys.iter().collect();
    let mut successors: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut indegree: BTreeMap<String, usize> = BTreeMap::new();
    for key in ours_keys.iter().chain(theirs_keys.iter()) {
        indegree.entry(key.clone()).or_insert(0);
        successors.entry(key.clone()).or_default();
    }
    let mut add_edge = |prev: &str, next: &str| {
        if prev == next {
            return;
        }
        let entry = successors
            .get_mut(prev)
            .expect("interleave nodes are pre-registered");
        if entry.insert(next.to_string()) {
            *indegree
                .get_mut(next)
                .expect("interleave nodes are pre-registered") += 1;
        }
    };
    let keep_ours_edge = |prev: &String, next: &String| {
        if reconciled {
            return true;
        }
        // Conflict: upstream wins, so keep only ours edges between events
        // that are unique to ours. Shared-order edges from ours are dropped.
        !theirs_set.contains(prev) && !theirs_set.contains(next)
    };
    for window in ours_keys.windows(2) {
        if keep_ours_edge(&window[0], &window[1]) {
            add_edge(&window[0], &window[1]);
        }
    }
    for window in theirs_keys.windows(2) {
        add_edge(&window[0], &window[1]);
    }
    let key_of = |key: &str| {
        let event = events.get(key).expect("interleave nodes have events");
        event_union_key(event, key)
    };
    let mut heap: BinaryHeap<Reverse<UnionHeapKey>> = BinaryHeap::new();
    for (key, degree) in &indegree {
        if *degree == 0 {
            let (timestamp, priority, event_id, serialized) = key_of(key);
            heap.push(Reverse((
                timestamp,
                priority,
                event_id,
                serialized,
                key.clone(),
            )));
        }
    }
    let mut ordered = Vec::with_capacity(indegree.len());
    while let Some(Reverse((_, _, _, _, key))) = heap.pop() {
        ordered.push(key.clone());
        let nexts: Vec<String> = successors
            .get(&key)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .collect();
        for next in nexts {
            let degree = indegree
                .get_mut(&next)
                .expect("interleave nodes are pre-registered");
            *degree -= 1;
            if *degree == 0 {
                let (timestamp, priority, event_id, serialized) = key_of(&next);
                heap.push(Reverse((
                    timestamp, priority, event_id, serialized, next,
                )));
            }
        }
    }
    if ordered.len() == indegree.len() {
        Some(ordered)
    } else {
        None
    }
}

fn validate_append_only_branch(
    base: &BeadEventStreamWire,
    branch: &BeadEventStreamWire,
    branch_name: &str,
) -> Result<BTreeSet<usize>, BeadError> {
    for (base_index, base_event) in base.events.iter().enumerate() {
        if branch.events.iter().any(|branch_event| {
            branch_event.event_id == base_event.event_id
                && branch_event != base_event
        }) {
            return Err(BeadError::validation(format!(
                "cannot merge non-append-only bead event stream {}: {branch_name} rewrote base event {}",
                base.stream_id,
                base_index + 1
            )));
        }
    }
    // Accept a pure-reorder branch: every base event exactly once, in any
    // order. Streams written by the old timestamp-sorting merge look exactly
    // like this. The merge canonicalizes them back to base order plus the
    // branch's genuine additions, so wedged clones can heal.
    let mut counts: Vec<usize> = vec![0; base.events.len()];
    let mut matched_indexes = BTreeSet::new();
    for (branch_index, branch_event) in branch.events.iter().enumerate() {
        if let Some(base_index) = base
            .events
            .iter()
            .position(|base_event| base_event == branch_event)
        {
            counts[base_index] += 1;
            matched_indexes.insert(branch_index);
        }
    }
    for (base_index, count) in counts.iter().enumerate() {
        if *count == 0 {
            return Err(BeadError::validation(format!(
                "cannot merge non-append-only bead event stream {}: {branch_name} missing base event {}",
                base.stream_id,
                base_index + 1
            )));
        }
        if *count > 1 {
            return Err(BeadError::validation(format!(
                "cannot merge non-append-only bead event stream {}: {branch_name} duplicate base event {}",
                base.stream_id,
                base_index + 1
            )));
        }
    }
    Ok(matched_indexes)
}

fn event_union_key(
    event: &BeadEventRecordWire,
    serialized: &str,
) -> (String, usize, String, String) {
    (
        event.timestamp.clone(),
        event_operation_priority(event.operation),
        event.event_id.clone(),
        serialized.to_string(),
    )
}

fn event_keys(
    events: &[BeadEventRecordWire],
) -> Result<BTreeSet<String>, BeadError> {
    events
        .iter()
        .map(serde_json::to_string)
        .collect::<Result<BTreeSet<_>, _>>()
        .map_err(BeadError::from)
}

pub(in crate::bead) fn mint_bead_event_id(
    stream_id: &str,
    ordinal: usize,
    timestamp: &str,
    actor: &str,
    operation: BeadEventOperationWire,
    issue_id: &str,
    payload: &BeadEventPayloadWire,
) -> Result<String, BeadError> {
    let operation_label = serde_json::to_string(&operation)?
        .trim_matches('"')
        .to_string();
    let content = serde_json::to_vec(&(
        BEAD_EVENT_SCHEMA_VERSION,
        timestamp,
        actor,
        operation,
        issue_id,
        payload,
    ))?;
    let digest = hex::encode(Sha256::digest(content));
    Ok(format!(
        "{stream_id}:{ordinal:06}:{operation_label}:{issue_id}:{digest}"
    ))
}

/// Interleave events from every stream into one deterministic apply order.
///
/// Events within a stream must apply in recorded order: stream merges append
/// events whose timestamps can predate earlier entries, so intra-stream
/// position is the causal order while timestamps only decide how independent
/// streams interleave. No single comparator can express both rules (mixing
/// index order with timestamp order is not a total order), so a k-way merge
/// keeps one cursor per stream and always emits the smallest head event by
/// (timestamp, operation priority, event_id, stream index).
pub(in crate::bead) fn merge_stream_events(
    streams: &[BeadEventStreamWire],
) -> Vec<&BeadEventRecordWire> {
    let mut heads: BinaryHeap<Reverse<StreamHead<'_>>> = streams
        .iter()
        .enumerate()
        .filter_map(|(stream_index, stream)| {
            stream.events.first().map(|event| {
                Reverse(StreamHead {
                    event,
                    stream_index,
                    event_index: 0,
                })
            })
        })
        .collect();
    let mut ordered = Vec::with_capacity(
        streams.iter().map(|stream| stream.events.len()).sum(),
    );
    while let Some(Reverse(head)) = heads.pop() {
        ordered.push(head.event);
        let event_index = head.event_index + 1;
        if let Some(event) = streams[head.stream_index].events.get(event_index)
        {
            heads.push(Reverse(StreamHead {
                event,
                stream_index: head.stream_index,
                event_index,
            }));
        }
    }
    ordered
}

struct StreamHead<'a> {
    event: &'a BeadEventRecordWire,
    stream_index: usize,
    event_index: usize,
}

impl StreamHead<'_> {
    fn merge_key(&self) -> (&str, usize, &str, usize) {
        (
            self.event.timestamp.as_str(),
            event_operation_priority(self.event.operation),
            self.event.event_id.as_str(),
            self.stream_index,
        )
    }
}

impl PartialEq for StreamHead<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.merge_key() == other.merge_key()
    }
}

impl Eq for StreamHead<'_> {}

impl PartialOrd for StreamHead<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for StreamHead<'_> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.merge_key().cmp(&other.merge_key())
    }
}

fn event_operation_priority(operation: BeadEventOperationWire) -> usize {
    match operation {
        BeadEventOperationWire::IssueCreated => 0,
        BeadEventOperationWire::DependencyAdded => 2,
        BeadEventOperationWire::DependencyRemoved => 3,
        BeadEventOperationWire::ReferenceAdded => 4,
        BeadEventOperationWire::ReferenceRemoved => 5,
        BeadEventOperationWire::LinkAdded => 6,
        BeadEventOperationWire::LinkRemoved => 7,
        _ => 1,
    }
}
