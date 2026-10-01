//! Changesets and the time-merged feed.
//!
//! [`build_changesets`] groups classified versions by
//! `(scope_key, commit)` and splits each commit's entries into
//! authored work and consequences. A version is a consequence when its
//! subject is generated, or when it is an instruction version whose
//! class is `rendered`, `config`, or `regen_only`; role decides
//! folding, not class, so a created generated note still folds while a
//! `hand_edited` instruction version and an ordinary promotion stay
//! authored. [`build_feed`] merges changesets across scopes newest
//! first.

use std::collections::{BTreeMap, BTreeSet};

use super::wire::{
    MemoryHistoryChangesetWire, MemoryHistoryClassWire,
    MemoryHistoryFeedEntryWire, MemoryHistoryFeedWire,
    MemoryHistorySubjectKindWire, MemoryHistorySubjectWire,
    MEMORY_HISTORY_WIRE_SCHEMA_VERSION,
};

/// One scope's subjects for [`build_feed`]: the owning scope key plus
/// its classified, cause-attributed subjects.
pub struct FeedScope<'a> {
    /// Owning scope key, copied onto each changeset.
    pub scope_key: &'a str,
    /// Classified subjects of this scope.
    pub subjects: &'a [MemoryHistorySubjectWire],
}

/// Group one scope's versions into one changeset per commit, newest
/// first (committer time descending, then commit ascending). Entries
/// within a changeset sort by subject id, then ordinal. A changeset is
/// `regen_only` when, after the hidden filter, it has no authored
/// entry left and every remaining consequence is class `regen_only`
/// or `regenerated`.
pub fn build_changesets(
    scope_key: &str,
    subjects: &[MemoryHistorySubjectWire],
) -> Vec<MemoryHistoryChangesetWire> {
    let mut groups: BTreeMap<String, Vec<(&MemoryHistorySubjectWire, usize)>> =
        BTreeMap::new();
    for subject in subjects {
        for (index, version) in subject.versions.iter().enumerate() {
            groups
                .entry(version.commit.clone())
                .or_default()
                .push((subject, index));
        }
    }
    let mut changesets: Vec<MemoryHistoryChangesetWire> = groups
        .into_iter()
        .map(|(commit, rows)| build_changeset(scope_key, &commit, &rows))
        .collect();
    changesets.sort_by(|left, right| {
        right
            .committer_time
            .cmp(&left.committer_time)
            .then(left.commit.cmp(&right.commit))
    });
    changesets
}

/// Merge changesets across scopes newest first: committer time
/// descending, then scope key ascending, then commit ascending.
/// `since` is an optional inclusive epoch-second lower bound on
/// committer time. `include_hidden: false` drops `hidden_by_default`
/// versions and drops `regen_only` changesets (plus changesets left
/// with no visible entry), counting every dropped changeset in
/// `hidden_changeset_count`; `include_hidden: true` keeps both and
/// reports zero. `limit` applies after filtering.
pub fn build_feed(
    scopes: &[FeedScope<'_>],
    since: Option<i64>,
    limit: Option<usize>,
    include_hidden: bool,
) -> MemoryHistoryFeedWire {
    let mut hidden: BTreeMap<String, BTreeSet<u64>> = BTreeMap::new();
    for scope in scopes {
        for subject in scope.subjects {
            for version in &subject.versions {
                if version.hidden_by_default {
                    hidden
                        .entry(subject.id.clone())
                        .or_default()
                        .insert(version.ordinal);
                }
            }
        }
    }
    let is_visible = |entry: &MemoryHistoryFeedEntryWire| {
        !hidden
            .get(&entry.subject_id)
            .is_some_and(|ordinals| ordinals.contains(&entry.ordinal))
    };
    let mut changesets: Vec<MemoryHistoryChangesetWire> = scopes
        .iter()
        .flat_map(|scope| build_changesets(scope.scope_key, scope.subjects))
        .collect();
    changesets.sort_by(|left, right| {
        right
            .committer_time
            .cmp(&left.committer_time)
            .then(left.scope_key.cmp(&right.scope_key))
            .then(left.commit.cmp(&right.commit))
    });
    if let Some(lower) = since {
        changesets.retain(|changeset| changeset.committer_time >= lower);
    }
    let mut hidden_changeset_count: u64 = 0;
    if !include_hidden {
        let mut kept = Vec::with_capacity(changesets.len());
        for mut changeset in changesets {
            changeset.authored.retain(|entry| is_visible(entry));
            changeset.consequences.retain(|entry| is_visible(entry));
            if changeset.regen_only
                || (changeset.authored.is_empty()
                    && changeset.consequences.is_empty())
            {
                hidden_changeset_count += 1;
            } else {
                kept.push(changeset);
            }
        }
        changesets = kept;
    }
    if let Some(max) = limit {
        changesets.truncate(max);
    }
    MemoryHistoryFeedWire {
        schema_version: MEMORY_HISTORY_WIRE_SCHEMA_VERSION,
        changesets,
        hidden_changeset_count: if include_hidden {
            0
        } else {
            hidden_changeset_count
        },
    }
}

fn build_changeset(
    scope_key: &str,
    commit: &str,
    rows: &[(&MemoryHistorySubjectWire, usize)],
) -> MemoryHistoryChangesetWire {
    let mut sorted = rows.to_vec();
    sorted.sort_by(|left, right| {
        left.0.id.cmp(&right.0.id).then(left.1.cmp(&right.1))
    });
    let first = &sorted[0].0.versions[sorted[0].1];
    let mut authored = Vec::new();
    let mut consequences = Vec::new();
    // Visibility after the hidden filter, for the `regen_only` rule.
    let mut visible_authored = 0usize;
    let mut visible_consequences = 0usize;
    let mut visible_consequence_foldable = 0usize;
    for (subject, index) in sorted {
        let version = &subject.versions[index];
        let entry = MemoryHistoryFeedEntryWire {
            subject_id: subject.id.clone(),
            ordinal: version.ordinal,
            class: version.class,
            summary: version.summary.clone(),
            path: version.path.clone(),
        };
        let consequence = is_consequence(subject, version.class);
        if consequence {
            consequences.push(entry);
        } else {
            authored.push(entry);
        }
        if !version.hidden_by_default {
            if consequence {
                visible_consequences += 1;
                if matches!(
                    version.class,
                    MemoryHistoryClassWire::RegenOnly
                        | MemoryHistoryClassWire::Regenerated
                ) {
                    visible_consequence_foldable += 1;
                }
            } else {
                visible_authored += 1;
            }
        }
    }
    MemoryHistoryChangesetWire {
        scope_key: scope_key.to_string(),
        commit: commit.to_string(),
        committer_time: first.committer_time,
        provenance: first.provenance.clone(),
        boilerplate: rows
            .iter()
            .any(|(subject, index)| subject.versions[*index].boilerplate),
        regen_only: visible_authored == 0
            && visible_consequences > 0
            && visible_consequence_foldable == visible_consequences,
        authored,
        consequences,
    }
}

/// Role decides folding, not class.
fn is_consequence(
    subject: &MemoryHistorySubjectWire,
    class: MemoryHistoryClassWire,
) -> bool {
    subject.generated
        || (subject.kind == MemoryHistorySubjectKindWire::Instructions
            && matches!(
                class,
                MemoryHistoryClassWire::Rendered
                    | MemoryHistoryClassWire::Config
                    | MemoryHistoryClassWire::RegenOnly
            ))
}
