//! Rename-aware lineage fold over parsed raw commits.
//!
//! The fold walks commits oldest-first and assigns stable numeric
//! lineage ids in creation order, so a full rebuild and an incremental
//! fold of the same history produce identical lineages. Rename pairs
//! continue the source lineage across the whole pathspec (never
//! `--follow`, which follows only one file). A delete followed by a
//! re-add at the same path resumes the same lineage with an explicit
//! gap; a commit touching an unknown path starts a lineage whose oldest
//! version is not a creation (the incompleteness signal shallow and
//! truncated walks rely on). Output versions are newest-first and are
//! never re-sorted by wall-clock time.
//!
//! Defensive approximations (unreachable with the pinned walk argv,
//! which never emits copies): a copy keeps its source lineage live at
//! the old path and starts a fresh lineage at the new path.
//!
//! The `complete` flag here means only that the oldest folded version
//! is a creation. The index layer refines it: truncation forces it
//! false, and in shallow clones a creation at a boundary commit (whose
//! parent side is missing, so git reports an add either way) also
//! counts as incomplete.

use std::collections::HashMap;

use super::parse::{RawChangeEntry, RawChangeKind, RawCommit};
use super::wire::{FileChangeKindWire, FileLineageWire, FileVersionWire};

/// Fold newest-first raw commits into lineages in id order.
pub fn fold_lineages(
    commits_newest_first: &[RawCommit],
) -> Vec<FileLineageWire> {
    let mut folder = Folder::new();
    for commit in commits_newest_first.iter().rev() {
        folder.apply_commit(commit);
    }
    folder.finish()
}

/// Fold newest-first raw commits onto cached lineages, assigning fresh
/// ids from one past the cached maximum. The result equals a full
/// rebuild over the concatenated history whenever neither walk was
/// truncated.
pub fn fold_new_commits(
    cached: &[FileLineageWire],
    commits_newest_first: &[RawCommit],
) -> Vec<FileLineageWire> {
    let mut folder = Folder::from_cached(cached);
    for commit in commits_newest_first.iter().rev() {
        folder.apply_commit(commit);
    }
    folder.finish()
}

/// Map every historical path to its lineage id.
pub fn path_alias_map(
    lineages: &[FileLineageWire],
) -> std::collections::BTreeMap<String, u64> {
    let mut map = std::collections::BTreeMap::new();
    for lineage in lineages {
        for path in &lineage.paths {
            map.insert(path.clone(), lineage.id);
        }
    }
    map
}

struct LineageBuilder {
    id: u64,
    paths: Vec<String>,
    current: String,
    tombstone: bool,
    versions: Vec<FileVersionWire>,
}

struct Folder {
    lineages: Vec<LineageBuilder>,
    live: HashMap<String, usize>,
    entombed: HashMap<String, usize>,
    next_id: u64,
}

impl Folder {
    fn new() -> Self {
        Self {
            lineages: Vec::new(),
            live: HashMap::new(),
            entombed: HashMap::new(),
            next_id: 0,
        }
    }

    fn from_cached(cached: &[FileLineageWire]) -> Self {
        let mut folder = Self::new();
        for lineage in cached {
            let mut versions = lineage.versions.clone();
            versions.reverse();
            let position = folder.lineages.len();
            if lineage.tombstone {
                folder.entombed.insert(
                    lineage.versions.first().map_or_else(
                        || lineage.current_path.clone(),
                        |version| version.path.clone(),
                    ),
                    position,
                );
            } else {
                folder.live.insert(lineage.current_path.clone(), position);
            }
            folder.next_id = folder.next_id.max(lineage.id + 1);
            folder.lineages.push(LineageBuilder {
                id: lineage.id,
                paths: lineage.paths.clone(),
                current: lineage.current_path.clone(),
                tombstone: lineage.tombstone,
                versions,
            });
        }
        folder
    }

    fn apply_commit(&mut self, commit: &RawCommit) {
        for entry in &commit.entries {
            let path = entry.path.clone();
            match &entry.kind {
                RawChangeKind::Added => {
                    if let Some(&position) = self.live.get(&path) {
                        self.push_edited(position, commit, entry, &path);
                    } else if let Some(position) = self.entombed.remove(&path) {
                        self.push_resumed(position, commit, entry, &path);
                        self.live.insert(path, position);
                    } else {
                        let position = self.create_lineage(vec![path.clone()]);
                        self.push_version(
                            position,
                            version_for(
                                commit,
                                entry,
                                &path,
                                entry.new_oid.clone(),
                                None,
                                FileChangeKindWire::Created,
                                None,
                                false,
                            ),
                        );
                        self.live.insert(path, position);
                    }
                }
                RawChangeKind::Modified | RawChangeKind::Typechange => {
                    if let Some(&position) = self.live.get(&path) {
                        self.push_edited(position, commit, entry, &path);
                    } else {
                        let position = self.create_lineage(vec![path.clone()]);
                        self.push_version(
                            position,
                            version_for(
                                commit,
                                entry,
                                &path,
                                entry.new_oid.clone(),
                                entry.old_oid.clone(),
                                FileChangeKindWire::Edited,
                                None,
                                false,
                            ),
                        );
                        self.live.insert(path, position);
                    }
                }
                RawChangeKind::Deleted => {
                    if let Some(position) = self.live.remove(&path) {
                        let blob = entry.old_oid.clone();
                        self.push_version(
                            position,
                            version_for(
                                commit,
                                entry,
                                &path,
                                blob.clone(),
                                blob,
                                FileChangeKindWire::Deleted,
                                None,
                                false,
                            ),
                        );
                        self.lineages[position].tombstone = true;
                        self.entombed.insert(path, position);
                    }
                }
                RawChangeKind::Renamed { score }
                | RawChangeKind::Copied { score } => {
                    let old = entry.old_path.clone().unwrap_or_default();
                    let copied =
                        matches!(entry.kind, RawChangeKind::Copied { .. });
                    let source = if copied {
                        self.live.get(&old).copied()
                    } else {
                        self.live.remove(&old)
                    };
                    if let Some(position) = source {
                        record_alias(&mut self.lineages[position].paths, &old);
                        record_alias(&mut self.lineages[position].paths, &path);
                        self.push_version(
                            position,
                            version_for(
                                commit,
                                entry,
                                &path,
                                entry.new_oid.clone(),
                                entry.old_oid.clone(),
                                FileChangeKindWire::Moved,
                                *score,
                                false,
                            ),
                        );
                        self.lineages[position].current = path.clone();
                        self.live.insert(path, position);
                    } else {
                        let mut initial = Vec::new();
                        if !old.is_empty() {
                            initial.push(old.clone());
                        }
                        initial.push(path.clone());
                        let position = self.create_lineage(initial);
                        self.push_version(
                            position,
                            version_for(
                                commit,
                                entry,
                                &path,
                                entry.new_oid.clone(),
                                entry.old_oid.clone(),
                                FileChangeKindWire::Moved,
                                *score,
                                false,
                            ),
                        );
                        self.lineages[position].current = path.clone();
                        self.live.insert(path, position);
                    }
                }
            }
        }
    }

    fn create_lineage(&mut self, paths: Vec<String>) -> usize {
        let position = self.lineages.len();
        let current = paths.last().cloned().unwrap_or_default();
        self.lineages.push(LineageBuilder {
            id: self.next_id,
            paths,
            current,
            tombstone: false,
            versions: Vec::new(),
        });
        self.next_id += 1;
        position
    }

    fn push_edited(
        &mut self,
        position: usize,
        commit: &RawCommit,
        entry: &RawChangeEntry,
        path: &str,
    ) {
        self.push_version(
            position,
            version_for(
                commit,
                entry,
                path,
                entry.new_oid.clone(),
                entry.old_oid.clone(),
                FileChangeKindWire::Edited,
                None,
                false,
            ),
        );
    }

    fn push_resumed(
        &mut self,
        position: usize,
        commit: &RawCommit,
        entry: &RawChangeEntry,
        path: &str,
    ) {
        self.lineages[position].tombstone = false;
        self.push_version(
            position,
            version_for(
                commit,
                entry,
                path,
                entry.new_oid.clone(),
                None,
                FileChangeKindWire::Created,
                None,
                true,
            ),
        );
    }

    fn push_version(&mut self, position: usize, version: FileVersionWire) {
        self.lineages[position].versions.push(version);
    }

    fn finish(mut self) -> Vec<FileLineageWire> {
        self.lineages.sort_by_key(|lineage| lineage.id);
        self.lineages
            .into_iter()
            .map(|mut builder| {
                for (index, version) in builder.versions.iter_mut().enumerate()
                {
                    version.ordinal = index as u64 + 1;
                }
                let complete =
                    builder.versions.first().is_some_and(|version| {
                        version.kind == FileChangeKindWire::Created
                    });
                builder.versions.reverse();
                FileLineageWire {
                    id: builder.id,
                    paths: builder.paths,
                    current_path: builder.current,
                    tombstone: builder.tombstone,
                    complete,
                    versions: builder.versions,
                }
            })
            .collect()
    }
}

#[allow(clippy::too_many_arguments)]
fn version_for(
    commit: &RawCommit,
    entry: &RawChangeEntry,
    path: &str,
    blob_oid: Option<String>,
    prev_blob_oid: Option<String>,
    kind: FileChangeKindWire,
    similarity: Option<u8>,
    gap_before: bool,
) -> FileVersionWire {
    FileVersionWire {
        ordinal: 0,
        commit: commit.commit.clone(),
        parents: commit.parents.clone(),
        committer_time: commit.committer_time,
        author_time: commit.author_time,
        author_name: commit.author_name.clone(),
        author_email: commit.author_email.clone(),
        subject: commit.subject.clone(),
        body: commit.body.clone(),
        path: path.to_string(),
        blob_oid,
        prev_blob_oid,
        kind,
        mode_old: entry.mode_old.clone(),
        mode_new: entry.mode_new.clone(),
        similarity,
        gap_before,
    }
}

fn record_alias(paths: &mut Vec<String>, path: &str) {
    if paths.last().is_none_or(|last| last != path) {
        paths.push(path.to_string());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const OID_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const OID_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const OID_C: &str = "cccccccccccccccccccccccccccccccccccccccc";

    fn commit(
        sha: &str,
        parents: &[&str],
        entries: Vec<RawChangeEntry>,
    ) -> RawCommit {
        RawCommit {
            commit: sha.to_string(),
            parents: parents.iter().map(ToString::to_string).collect(),
            committer_time: 100,
            author_time: 100,
            author_name: "T".to_string(),
            author_email: "t@e.com".to_string(),
            subject: sha.to_string(),
            body: String::new(),
            entries,
        }
    }

    fn added(path: &str, new_oid: &str) -> RawChangeEntry {
        RawChangeEntry {
            kind: RawChangeKind::Added,
            mode_old: "000000".to_string(),
            mode_new: "100644".to_string(),
            old_oid: None,
            new_oid: Some(new_oid.to_string()),
            old_path: None,
            path: path.to_string(),
        }
    }

    fn modified(path: &str, old_oid: &str, new_oid: &str) -> RawChangeEntry {
        RawChangeEntry {
            kind: RawChangeKind::Modified,
            mode_old: "100644".to_string(),
            mode_new: "100644".to_string(),
            old_oid: Some(old_oid.to_string()),
            new_oid: Some(new_oid.to_string()),
            old_path: None,
            path: path.to_string(),
        }
    }

    fn deleted(path: &str, old_oid: &str) -> RawChangeEntry {
        RawChangeEntry {
            kind: RawChangeKind::Deleted,
            mode_old: "100644".to_string(),
            mode_new: "000000".to_string(),
            old_oid: Some(old_oid.to_string()),
            new_oid: None,
            old_path: None,
            path: path.to_string(),
        }
    }

    fn renamed(
        old: &str,
        new: &str,
        old_oid: &str,
        new_oid: &str,
    ) -> RawChangeEntry {
        RawChangeEntry {
            kind: RawChangeKind::Renamed { score: Some(63) },
            mode_old: "100644".to_string(),
            mode_new: "100644".to_string(),
            old_oid: Some(old_oid.to_string()),
            new_oid: Some(new_oid.to_string()),
            old_path: Some(old.to_string()),
            path: new.to_string(),
        }
    }

    #[test]
    fn rename_chain_keeps_one_lineage_with_aliases() {
        let commits = vec![
            commit("c3", &["c2"], vec![renamed("b", "c", OID_B, OID_C)]),
            commit("c2", &["c1"], vec![renamed("a", "b", OID_A, OID_B)]),
            commit("c1", &[], vec![added("a", OID_A)]),
        ];
        let lineages = fold_lineages(&commits);
        assert_eq!(lineages.len(), 1);
        assert_eq!(lineages[0].id, 0);
        assert_eq!(lineages[0].paths, vec!["a", "b", "c"]);
        assert_eq!(lineages[0].current_path, "c");
        assert!(!lineages[0].tombstone);
        assert!(lineages[0].complete);
        let kinds: Vec<FileChangeKindWire> = lineages[0]
            .versions
            .iter()
            .map(|version| version.kind)
            .collect();
        assert_eq!(
            kinds,
            vec![
                FileChangeKindWire::Moved,
                FileChangeKindWire::Moved,
                FileChangeKindWire::Created,
            ]
        );
        let ordinals: Vec<u64> = lineages[0]
            .versions
            .iter()
            .map(|version| version.ordinal)
            .collect();
        assert_eq!(ordinals, vec![3, 2, 1]);
    }

    #[test]
    fn delete_and_recreate_resumes_with_gap() {
        let commits = vec![
            commit("c3", &["c2"], vec![added("f", OID_B)]),
            commit("c2", &["c1"], vec![deleted("f", OID_A)]),
            commit("c1", &[], vec![added("f", OID_A)]),
        ];
        let lineages = fold_lineages(&commits);
        assert_eq!(lineages.len(), 1);
        assert!(!lineages[0].tombstone);
        assert_eq!(lineages[0].versions.len(), 3);
        assert!(lineages[0].versions[0].gap_before);
        assert_eq!(lineages[0].versions[0].kind, FileChangeKindWire::Created);
        assert_eq!(lineages[0].versions[1].kind, FileChangeKindWire::Deleted);
        assert_eq!(lineages[0].versions[1].blob_oid.as_deref(), Some(OID_A));
    }

    #[test]
    fn modify_first_is_incomplete() {
        let commits =
            vec![commit("c1", &[], vec![modified("f", OID_A, OID_B)])];
        let lineages = fold_lineages(&commits);
        assert_eq!(lineages.len(), 1);
        assert!(!lineages[0].complete);
    }

    #[test]
    fn delete_without_history_is_ignored() {
        let commits = vec![commit("c1", &[], vec![deleted("f", OID_A)])];
        assert!(fold_lineages(&commits).is_empty());
    }

    #[test]
    fn rename_from_unknown_starts_moved_lineage() {
        let commits =
            vec![commit("c1", &[], vec![renamed("old", "new", OID_A, OID_A)])];
        let lineages = fold_lineages(&commits);
        assert_eq!(lineages.len(), 1);
        assert_eq!(lineages[0].paths, vec!["old", "new"]);
        assert!(!lineages[0].complete);
        assert_eq!(lineages[0].versions[0].kind, FileChangeKindWire::Moved);
    }

    #[test]
    fn incremental_fold_matches_full_rebuild() {
        let old = vec![
            commit("c2", &["c1"], vec![modified("f", OID_A, OID_B)]),
            commit("c1", &[], vec![added("f", OID_A)]),
        ];
        let new = vec![
            commit("c4", &["c3"], vec![added("g", OID_C)]),
            commit("c3", &["c2"], vec![renamed("f", "h", OID_B, OID_B)]),
        ];
        let cached = fold_lineages(&old);
        let folded = fold_new_commits(&cached, &new);
        let mut all = new.clone();
        all.extend(old.clone());
        let rebuilt = fold_lineages(&all);
        assert_eq!(folded.len(), rebuilt.len());
        for (left, right) in folded.iter().zip(rebuilt.iter()) {
            assert_eq!(left.id, right.id);
            assert_eq!(left.paths, right.paths);
            assert_eq!(left.current_path, right.current_path);
            assert_eq!(left.tombstone, right.tombstone);
            let left_commits: Vec<&str> = left
                .versions
                .iter()
                .map(|version| version.commit.as_str())
                .collect();
            let right_commits: Vec<&str> = right
                .versions
                .iter()
                .map(|version| version.commit.as_str())
                .collect();
            assert_eq!(left_commits, right_commits);
        }
        assert_eq!(path_alias_map(&folded), path_alias_map(&rebuilt));
    }
}
