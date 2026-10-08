//! Indexed mutation view over cached and replay backings.
//!
//! Warm mutations must not replay the whole store or hydrate every row:
//! a note/update loads its target row, any actual related rows, and its
//! affected stream only. This view is the shared lookup layer for that
//! path, with two backings behind one API:
//!
//! - `Cached`: indexed SQLite rows through the read model (no full
//!   snapshot load; one `SELECT row` per affected ID, index scans for
//!   children/dependents/suffixes).
//! - `Replay`: the full in-memory issue slice (uncached or unusable-cache
//!   stores keep today's replay behavior).
//!
//! Both backings share an in-memory overlay for loaded, changed, created,
//! and removed rows, so batch candidates validate against the final
//! overlay instead of the unchanged database. The fallback uses the same
//! mutation algorithms: callers branch on the backing only for row
//! retrieval, never for operation semantics.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use rusqlite::{Connection, OpenFlags};

use crate::artifact_link::BeadLinkDirectionWire;
use crate::bead::events::{BeadEventOperationWire, BeadEventPayloadWire};
use crate::bead::jsonl::{event_streams_dir, read_event_stream_file};
use crate::bead::read_model::alloc;
use crate::bead::read_model::{
    ensure_cache_ready_for_mutation_at, read_model_cache_path_for_store,
    CacheWitness,
};
use crate::bead::wire::{BeadError, IssueWire};

/// How rows are retrieved: indexed cache rows or a replay slice.
#[allow(dead_code)]
pub(crate) enum MutationViewBacking {
    Cached {
        cache_path: PathBuf,
        witness: CacheWitness,
    },
    Replay,
}

/// One lookup view for a single locked mutation.
///
/// `replay_issues` backs resolution when the cache is unusable; the
/// cached path never hydrates it. The overlay maps an ID to
/// `Some(changed/created row)` or `None` (pending removal). Hydrated
/// cached rows are memoized so a batch reads each affected row once and
/// always sees its staged final state through the overlay.
///
/// The lookup surface (children/descendants/ancestors/reverse-dependents,
/// stream routing, receipt checks) is consumed incrementally as operation
/// ports land; until the full mutation surface is ported, not every
/// method has a production caller yet.
#[allow(dead_code)]
pub(crate) struct MutationView<'a> {
    beads_dir: PathBuf,
    backing: MutationViewBacking,
    replay_issues: Option<&'a [IssueWire]>,
    overlay: BTreeMap<String, Option<IssueWire>>,
    memoized: RefCell<BTreeMap<String, IssueWire>>,
}

#[allow(dead_code)]
impl<'a> MutationView<'a> {
    /// Open the warmest usable view inside the mutation flock.
    ///
    /// Forces the full signature sweep (never the 60 s token-only skip),
    /// then serves indexed rows when the cache is ready. A cold,
    /// externally changed, or unusable cache falls back to the replay
    /// slice the caller already loaded; genuine store corruption is an
    /// error, exactly as the replay load would fail with it.
    pub(crate) fn load(
        beads_dir: &Path,
        replay_issues: &'a [IssueWire],
    ) -> Result<Self, BeadError> {
        if let Some(view) = Self::load_cached(beads_dir)? {
            return Ok(view);
        }
        Ok(Self::with_replay(beads_dir, replay_issues))
    }

    /// Admit the cached path without loading any replay.
    ///
    /// Runs inside the unchanged `beads.db` flock after the mandatory
    /// full freshness sweep. Returns `Ok(None)` when the caller must load
    /// the replay backing instead (no cache location, legacy store,
    /// unusable cache). Never replays just to supply an otherwise unused
    /// constructor argument: the full replay backing is instantiated only
    /// on this `None` or when a proven repair path requires it.
    pub(crate) fn load_cached(
        beads_dir: &Path,
    ) -> Result<Option<Self>, BeadError> {
        let Some(cache_path) = read_model_cache_path_for_store(beads_dir)
        else {
            return Ok(None);
        };
        match ensure_cache_ready_for_mutation_at(beads_dir, &cache_path) {
            Ok(true) => {
                #[cfg(test)]
                crate::bead::mutation::store::store_io_stats::record_load();
                let witness =
                    read_cache_witness(&cache_path).unwrap_or_default();
                Ok(Some(Self {
                    beads_dir: beads_dir.to_path_buf(),
                    backing: MutationViewBacking::Cached {
                        cache_path,
                        witness,
                    },
                    replay_issues: None,
                    overlay: BTreeMap::new(),
                    memoized: RefCell::new(BTreeMap::new()),
                }))
            }
            Ok(false) => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// Replay backing for uncached stores and proven repair paths.
    pub(crate) fn with_replay(
        beads_dir: &Path,
        replay_issues: &'a [IssueWire],
    ) -> Self {
        Self {
            beads_dir: beads_dir.to_path_buf(),
            backing: MutationViewBacking::Replay,
            replay_issues: Some(replay_issues),
            overlay: BTreeMap::new(),
            memoized: RefCell::new(BTreeMap::new()),
        }
    }

    /// Baseline witness for pre-write validation, if cached.
    pub(crate) fn witness(&self) -> Option<&CacheWitness> {
        match &self.backing {
            MutationViewBacking::Cached { witness, .. } => Some(witness),
            MutationViewBacking::Replay => None,
        }
    }

    /// True when indexed rows serve this mutation (no full replay).
    pub(crate) fn is_cached(&self) -> bool {
        matches!(self.backing, MutationViewBacking::Cached { .. })
    }

    /// Cache path for the staged commit, when cached.
    pub(crate) fn cache_path(&self) -> Option<&Path> {
        match &self.backing {
            MutationViewBacking::Cached { cache_path, .. } => {
                Some(cache_path.as_path())
            }
            MutationViewBacking::Replay => None,
        }
    }

    /// Record a changed or created row in the overlay.
    pub(crate) fn stage_issue(&mut self, issue: IssueWire) {
        self.memoized
            .borrow_mut()
            .insert(issue.id.clone(), issue.clone());
        self.overlay.insert(issue.id.clone(), Some(issue));
    }

    /// Record a pending removal tombstone in the overlay.
    pub(crate) fn stage_removal(&mut self, issue_id: &str) {
        self.memoized.borrow_mut().remove(issue_id);
        self.overlay.insert(issue_id.to_string(), None);
    }

    fn open_cached(&self) -> Result<Connection, BeadError> {
        match &self.backing {
            MutationViewBacking::Cached { cache_path, .. } => {
                Connection::open_with_flags(
                    cache_path,
                    OpenFlags::SQLITE_OPEN_READ_ONLY,
                )
                .map_err(|error| BeadError::io(error.to_string()))
            }
            MutationViewBacking::Replay => Err(BeadError::io(
                "mutation view has no cached backing".to_string(),
            )),
        }
    }

    /// Exact ID and shorthand resolution, preserving not-found/ambiguity
    /// errors. Overlay removals resolve (the row existed); overlay creates
    /// participate in suffix collisions exactly as loaded rows do.
    pub(crate) fn resolve(&self, raw_id: &str) -> Result<String, BeadError> {
        if raw_id.is_empty() || raw_id.contains('-') {
            return Ok(raw_id.to_string());
        }
        let candidates = self.suffix_candidates(raw_id)?;
        match candidates.as_slice() {
            [resolved] => Ok(resolved.clone()),
            [] => Err(BeadError {
                kind: "not_found".to_string(),
                message: format!("Issue not found: {raw_id}"),
            }),
            _ => Err(BeadError {
                kind: "ambiguous".to_string(),
                message: format!(
                    "ambiguous bead ID shorthand {raw_id:?}: {}",
                    candidates.join(", ")
                ),
            }),
        }
    }

    fn suffix_candidates(
        &self,
        suffix: &str,
    ) -> Result<Vec<String>, BeadError> {
        let mut candidates: BTreeSet<String> = BTreeSet::new();
        match &self.backing {
            MutationViewBacking::Cached { .. } => {
                let connection = self.open_cached()?;
                let mut statement = connection
                    .prepare(
                        "SELECT issue_id FROM suffix_catalog WHERE suffix = ?1 ORDER BY issue_id",
                    )
                    .map_err(|error| BeadError::io(error.to_string()))?;
                let rows = statement
                    .query_map([suffix], |row| row.get::<_, String>(0))
                    .map_err(|error| BeadError::io(error.to_string()))?;
                for row in rows {
                    candidates.insert(
                        row.map_err(|error| BeadError::io(error.to_string()))?,
                    );
                }
            }
            MutationViewBacking::Replay => {
                let issues = self.replay_issues.unwrap_or(&[]);
                for issue in issues {
                    if issue.id.ends_with(suffix)
                        && issue.id.len() > suffix.len()
                        && issue.id[..issue.id.len() - suffix.len()]
                            .ends_with('-')
                    {
                        candidates.insert(issue.id.clone());
                    }
                }
            }
        }
        for (id, entry) in &self.overlay {
            if let Some(_issue) = entry {
                if id.ends_with(suffix)
                    && id.len() > suffix.len()
                    && id[..id.len() - suffix.len()].ends_with('-')
                {
                    candidates.insert(id.clone());
                }
            } else {
                candidates.remove(id);
            }
        }
        Ok(candidates.into_iter().collect())
    }

    /// Load one row by exact ID, overlay-aware.
    ///
    /// Overlay entries win so a batch reads its staged final state.
    /// Memoized rows avoid re-hydrating the same affected row twice.
    /// SQLite and row-decode faults stay `io` errors and are never
    /// collapsed into semantic `not_found`.
    pub(crate) fn get(&self, issue_id: &str) -> Result<IssueWire, BeadError> {
        if let Some(entry) = self.overlay.get(issue_id) {
            return entry.clone().ok_or_else(|| BeadError {
                kind: "not_found".to_string(),
                message: format!("Issue not found: {issue_id}"),
            });
        }
        if let Some(cached) = self.memoized.borrow().get(issue_id) {
            return Ok(cached.clone());
        }
        match &self.backing {
            MutationViewBacking::Cached { .. } => {
                let connection = self.open_cached()?;
                let mut statement = connection
                    .prepare("SELECT row FROM issues WHERE id = ?1")
                    .map_err(|error| BeadError::io(error.to_string()))?;
                let mut rows = statement
                    .query([issue_id])
                    .map_err(|error| BeadError::io(error.to_string()))?;
                match rows
                    .next()
                    .map_err(|error| BeadError::io(error.to_string()))?
                {
                    Some(row) => {
                        let text: String = row.get(0).map_err(|error| {
                            BeadError::io(error.to_string())
                        })?;
                        let issue: IssueWire = serde_json::from_str(&text)
                            .map_err(|error| {
                                BeadError::io(format!(
                                    "cached issue row is not valid: {error}"
                                ))
                            })?;
                        #[cfg(test)]
                        crate::bead::mutation::store::store_io_stats::record_hydrated_rows(
                            1,
                        );
                        self.memoized
                            .borrow_mut()
                            .insert(issue_id.to_string(), issue.clone());
                        Ok(issue)
                    }
                    None => Err(BeadError {
                        kind: "not_found".to_string(),
                        message: format!("Issue not found: {issue_id}"),
                    }),
                }
            }
            MutationViewBacking::Replay => self
                .replay_issues
                .unwrap_or(&[])
                .iter()
                .find(|issue| issue.id == issue_id)
                .cloned()
                .ok_or_else(|| BeadError {
                    kind: "not_found".to_string(),
                    message: format!("Issue not found: {issue_id}"),
                }),
        }
    }

    /// Ordered direct children, overlay-consistent, in
    /// creation-time/replay-position order.
    pub(crate) fn children(
        &self,
        parent_id: &str,
    ) -> Result<Vec<IssueWire>, BeadError> {
        let mut rows: BTreeMap<String, IssueWire> = BTreeMap::new();
        match &self.backing {
            MutationViewBacking::Cached { .. } => {
                let connection = self.open_cached()?;
                let mut statement = connection
                    .prepare(
                        "SELECT row FROM issues WHERE parent = ?1 ORDER BY created_at ASC, position ASC",
                    )
                    .map_err(|error| BeadError::io(error.to_string()))?;
                let mapped = statement
                    .query_map([parent_id], |row| row.get::<_, String>(0))
                    .map_err(|error| BeadError::io(error.to_string()))?;
                let mut hydrated: u64 = 0;
                for row in mapped {
                    let text: String =
                        row.map_err(|error| BeadError::io(error.to_string()))?;
                    let issue: IssueWire = serde_json::from_str(&text)
                        .map_err(|error| {
                            BeadError::io(format!(
                                "cached issue row is not valid: {error}"
                            ))
                        })?;
                    hydrated = hydrated.saturating_add(1);
                    rows.insert(issue.id.clone(), issue);
                }
                #[cfg(test)]
                crate::bead::mutation::store::store_io_stats::record_hydrated_rows(
                    hydrated,
                );
            }
            MutationViewBacking::Replay => {
                for issue in self.replay_issues.unwrap_or(&[]) {
                    if issue.parent_id.as_deref() == Some(parent_id) {
                        rows.insert(issue.id.clone(), issue.clone());
                    }
                }
            }
        }
        for (id, entry) in &self.overlay {
            match entry {
                Some(issue)
                    if issue.parent_id.as_deref() == Some(parent_id) =>
                {
                    rows.insert(id.clone(), issue.clone());
                }
                _ => {
                    if let Some(existing) = rows.get(id) {
                        if existing.parent_id.as_deref() == Some(parent_id) {
                            rows.remove(id);
                        }
                    }
                }
            }
        }
        let mut children: Vec<IssueWire> = rows.into_values().collect();
        children.sort_by(|a, b| {
            a.created_at.cmp(&b.created_at).then(a.id.cmp(&b.id))
        });
        Ok(children)
    }

    /// Recursive descendants in post-order (children before parents),
    /// mirroring the replay collector.
    pub(crate) fn descendants(
        &self,
        parent_id: &str,
    ) -> Result<Vec<IssueWire>, BeadError> {
        let mut out = Vec::new();
        let mut visited = BTreeSet::from([parent_id.to_string()]);
        self.collect_descendants(parent_id, &mut visited, &mut out)?;
        Ok(out)
    }

    fn collect_descendants(
        &self,
        parent_id: &str,
        visited: &mut BTreeSet<String>,
        out: &mut Vec<IssueWire>,
    ) -> Result<(), BeadError> {
        for child in self.children(parent_id)? {
            if !visited.insert(child.id.clone()) {
                continue;
            }
            self.collect_descendants(&child.id.clone(), visited, out)?;
            out.push(child);
        }
        Ok(())
    }

    /// Ancestor chain from the direct parent upward.
    pub(crate) fn ancestors(
        &self,
        issue_id: &str,
    ) -> Result<Vec<IssueWire>, BeadError> {
        let mut chain = Vec::new();
        let mut seen = BTreeSet::from([issue_id.to_string()]);
        let mut next = self.get(issue_id)?.parent_id.clone();
        while let Some(parent_id) = next {
            if !seen.insert(parent_id.clone()) {
                break;
            }
            let Ok(parent) = self.get(&parent_id) else {
                break;
            };
            next = parent.parent_id.clone();
            chain.push(parent);
        }
        Ok(chain)
    }

    /// Dependency targets of one bead, in stored order.
    pub(crate) fn dependency_targets(
        &self,
        issue_id: &str,
    ) -> Result<Vec<IssueWire>, BeadError> {
        let issue = self.get(issue_id)?;
        let mut targets = Vec::new();
        for dependency in &issue.dependencies {
            if let Ok(target) = self.get(&dependency.depends_on_id) {
                targets.push(target);
            }
        }
        Ok(targets)
    }

    /// Reverse dependents through the edge table (cached) or a scan of
    /// the replay slice, overlay-consistent, in creation order.
    pub(crate) fn reverse_dependents(
        &self,
        issue_id: &str,
    ) -> Result<Vec<IssueWire>, BeadError> {
        let mut ids: BTreeSet<String> = BTreeSet::new();
        match &self.backing {
            MutationViewBacking::Cached { .. } => {
                let connection = self.open_cached()?;
                let mut statement = connection
                    .prepare(
                        "SELECT src FROM edges WHERE dst = ?1 AND kind = 'depends_on' ORDER BY src ASC",
                    )
                    .map_err(|error| BeadError::io(error.to_string()))?;
                let mapped = statement
                    .query_map([issue_id], |row| row.get::<_, String>(0))
                    .map_err(|error| BeadError::io(error.to_string()))?;
                for row in mapped {
                    ids.insert(
                        row.map_err(|error| BeadError::io(error.to_string()))?,
                    );
                }
            }
            MutationViewBacking::Replay => {
                for issue in self.replay_issues.unwrap_or(&[]) {
                    if issue
                        .dependencies
                        .iter()
                        .any(|dependency| dependency.depends_on_id == issue_id)
                    {
                        ids.insert(issue.id.clone());
                    }
                }
            }
        }
        for (id, entry) in &self.overlay {
            match entry {
                Some(issue)
                    if issue.dependencies.iter().any(|dependency| {
                        dependency.depends_on_id == issue_id
                    }) =>
                {
                    ids.insert(id.clone());
                }
                _ => {
                    if let Ok(current) =
                        self.backing_dependent_has(id, issue_id)
                    {
                        if !current {
                            ids.remove(id);
                        }
                    }
                }
            }
        }
        let mut out = Vec::new();
        for id in ids {
            if let Ok(issue) = self.get(&id) {
                out.push(issue);
            }
        }
        out.sort_by(|a, b| {
            a.created_at.cmp(&b.created_at).then(a.id.cmp(&b.id))
        });
        Ok(out)
    }

    fn backing_dependent_has(
        &self,
        src_id: &str,
        dst_id: &str,
    ) -> Result<bool, BeadError> {
        match &self.backing {
            MutationViewBacking::Cached { .. } => {
                let connection = self.open_cached()?;
                let count: i64 = connection
                    .query_row(
                        "SELECT COUNT(*) FROM edges WHERE src = ?1 AND dst = ?2 AND kind = 'depends_on'",
                        [src_id, dst_id],
                        |row| row.get(0),
                    )
                    .map_err(|error| BeadError::io(error.to_string()))?;
                Ok(count > 0)
            }
            MutationViewBacking::Replay => Ok(self
                .replay_issues
                .unwrap_or(&[])
                .iter()
                .find(|issue| issue.id == src_id)
                .is_some_and(|issue| {
                    issue
                        .dependencies
                        .iter()
                        .any(|dependency| dependency.depends_on_id == dst_id)
                })),
        }
    }

    /// Owner of a normalized external ref, if any.
    pub(crate) fn external_ref_owner(
        &self,
        normalized_ref: &str,
    ) -> Result<Option<IssueWire>, BeadError> {
        if normalized_ref.is_empty() {
            return Ok(None);
        }
        let mut candidate: Option<IssueWire> = None;
        match &self.backing {
            MutationViewBacking::Cached { .. } => {
                let connection = self.open_cached()?;
                let mut statement = connection
                    .prepare("SELECT row FROM issues WHERE external_ref = ?1")
                    .map_err(|error| BeadError::io(error.to_string()))?;
                let mapped = statement
                    .query_map([normalized_ref], |row| row.get::<_, String>(0))
                    .map_err(|error| BeadError::io(error.to_string()))?;
                let mut hydrated: u64 = 0;
                for row in mapped {
                    let text: String =
                        row.map_err(|error| BeadError::io(error.to_string()))?;
                    hydrated = hydrated.saturating_add(1);
                    candidate =
                        Some(serde_json::from_str(&text).map_err(|error| {
                            BeadError::io(format!(
                                "cached issue row is not valid: {error}"
                            ))
                        })?);
                }
                #[cfg(test)]
                crate::bead::mutation::store::store_io_stats::record_hydrated_rows(
                    hydrated,
                );
            }
            MutationViewBacking::Replay => {
                candidate = self
                    .replay_issues
                    .unwrap_or(&[])
                    .iter()
                    .find(|issue| issue.external_ref == normalized_ref)
                    .cloned();
            }
        }
        for (id, entry) in &self.overlay {
            match entry {
                Some(issue) if issue.external_ref == normalized_ref => {
                    candidate = Some(issue.clone());
                }
                _ => {
                    if candidate.as_ref().is_some_and(|row| &row.id == id) {
                        candidate = None;
                    }
                }
            }
        }
        Ok(candidate)
    }

    /// Next top-level counter for `prefix`, honoring the config counter,
    /// removed IDs, and the overlay — from disposable allocation metadata.
    ///
    /// Matches `store.rs::next_top_level_counter` exactly: the maximum of
    /// `config.next_counter` and the next valid base36 top-level ID for
    /// that prefix. Malformed IDs, nested IDs (containing `.`), multiple
    /// prefixes, and empty stores contribute nothing. Staged creates raise
    /// the maximum; removing the maximum recomputes from surviving rows
    /// with a prefix-scoped query so a freed counter is reused exactly as
    /// the replay oracle would.
    pub(crate) fn next_top_level_counter(
        &self,
        issue_prefix: &str,
        config_counter: u64,
    ) -> Result<u64, BeadError> {
        match &self.backing {
            MutationViewBacking::Cached { .. } => {
                let connection = self.open_cached()?;
                let mut stored =
                    alloc::stored_top_max(&connection, issue_prefix)?
                        .unwrap_or(0);
                let mut overlay_max: u64 = 0;
                let mut removes_max = false;
                for (id, entry) in &self.overlay {
                    if entry.is_some() {
                        if let Some((prefix, counter)) =
                            alloc::top_prefix_and_counter(id)
                        {
                            if prefix == issue_prefix {
                                overlay_max = overlay_max.max(counter);
                            }
                        }
                    } else if let Some((prefix, counter)) =
                        alloc::top_prefix_and_counter(id)
                    {
                        if prefix == issue_prefix && counter == stored {
                            removes_max = true;
                        }
                    }
                }
                if removes_max {
                    stored =
                        self.recompute_top_max(&connection, issue_prefix)?;
                }
                let max_seen = stored.max(overlay_max);
                Ok(config_counter.max(max_seen.saturating_add(1)))
            }
            MutationViewBacking::Replay => {
                let mut max_seen: u64 = 0;
                let mut ids: Vec<String> = self
                    .replay_issues
                    .unwrap_or(&[])
                    .iter()
                    .map(|issue| issue.id.clone())
                    .collect();
                for (id, entry) in &self.overlay {
                    if entry.is_some() {
                        ids.push(id.clone());
                    } else {
                        ids.retain(|candidate| candidate != id);
                    }
                }
                let expected = format!("{issue_prefix}-");
                for id in ids {
                    let Some(suffix) = id.strip_prefix(&expected) else {
                        continue;
                    };
                    if suffix.contains('.') {
                        continue;
                    }
                    if let Ok(counter) = u64::from_str_radix(suffix, 36) {
                        max_seen = max_seen.max(counter);
                    }
                }
                Ok(config_counter.max(max_seen.saturating_add(1)))
            }
        }
    }

    fn recompute_top_max(
        &self,
        connection: &Connection,
        issue_prefix: &str,
    ) -> Result<u64, BeadError> {
        let lower = format!("{issue_prefix}-");
        let upper = format!("{issue_prefix}.");
        let mut stmt = connection
            .prepare("SELECT id FROM issues WHERE id >= ?1 AND id < ?2")
            .map_err(|error| BeadError::io(error.to_string()))?;
        let mapped = stmt
            .query_map([lower, upper], |row| row.get::<_, String>(0))
            .map_err(|error| BeadError::io(error.to_string()))?;
        let mut max_seen: u64 = 0;
        for row in mapped {
            let id: String =
                row.map_err(|error| BeadError::io(error.to_string()))?;
            if self.overlay.get(&id).is_some_and(|entry| entry.is_none()) {
                continue;
            }
            if let Some((prefix, counter)) = alloc::top_prefix_and_counter(&id)
            {
                if prefix == issue_prefix {
                    max_seen = max_seen.max(counter);
                }
            }
        }
        Ok(max_seen)
    }

    /// Next direct child ID under `parent_id`, overlay-consistent.
    ///
    /// Matches `store.rs::next_child_id` exactly: IDs with the textual
    /// `<parent>.` prefix and a direct decimal suffix, regardless of a
    /// row's `parent_id` field. The prototype's `WHERE parent = ?1` is
    /// therefore insufficient for mismatched parent fields. Staged
    /// creates/removes and removal of the maximum suffix behave exactly
    /// as the replay oracle: a freed maximum is reused.
    pub(crate) fn next_child_id(
        &self,
        parent_id: &str,
    ) -> Result<String, BeadError> {
        match &self.backing {
            MutationViewBacking::Cached { .. } => {
                let connection = self.open_cached()?;
                let mut stored =
                    alloc::stored_child_max(&connection, parent_id)?
                        .unwrap_or(0);
                let mut overlay_max: u64 = 0;
                let mut removes_max = false;
                for (id, entry) in &self.overlay {
                    if entry.is_some() {
                        if let Some((parent, counter)) =
                            alloc::child_parent_and_suffix(id)
                        {
                            if parent == parent_id {
                                overlay_max = overlay_max.max(counter);
                            }
                        }
                    } else if let Some((parent, counter)) =
                        alloc::child_parent_and_suffix(id)
                    {
                        if parent == parent_id && counter == stored {
                            removes_max = true;
                        }
                    }
                }
                if removes_max {
                    stored =
                        self.recompute_child_max(&connection, parent_id)?;
                }
                let local_max = stored.max(overlay_max);
                Ok(format!("{parent_id}.{}", local_max + 1))
            }
            MutationViewBacking::Replay => {
                let prefix = format!("{parent_id}.");
                let mut local_max: u64 = 0;
                let mut ids: Vec<String> = Vec::new();
                for issue in self.replay_issues.unwrap_or(&[]) {
                    ids.push(issue.id.clone());
                }
                for (id, entry) in &self.overlay {
                    if entry.is_some() {
                        ids.push(id.clone());
                    } else {
                        ids.retain(|candidate| candidate != id);
                    }
                }
                for id in ids {
                    let Some(suffix) = id.strip_prefix(&prefix) else {
                        continue;
                    };
                    if suffix.contains('.') {
                        continue;
                    }
                    if let Ok(counter) = suffix.parse::<u64>() {
                        local_max = local_max.max(counter);
                    }
                }
                Ok(format!("{parent_id}.{}", local_max + 1))
            }
        }
    }

    fn recompute_child_max(
        &self,
        connection: &Connection,
        parent_id: &str,
    ) -> Result<u64, BeadError> {
        let lower = format!("{parent_id}.");
        let upper = format!("{parent_id}/");
        let mut stmt = connection
            .prepare("SELECT id FROM issues WHERE id >= ?1 AND id < ?2")
            .map_err(|error| BeadError::io(error.to_string()))?;
        let mapped = stmt
            .query_map([lower, upper], |row| row.get::<_, String>(0))
            .map_err(|error| BeadError::io(error.to_string()))?;
        let mut local_max: u64 = 0;
        for row in mapped {
            let id: String =
                row.map_err(|error| BeadError::io(error.to_string()))?;
            if self.overlay.get(&id).is_some_and(|entry| entry.is_none()) {
                continue;
            }
            if let Some((parent, counter)) = alloc::child_parent_and_suffix(&id)
            {
                if parent == parent_id {
                    local_max = local_max.max(counter);
                }
            }
        }
        Ok(local_max)
    }

    /// Physical event-stream owner, preserving the `stream_id_for_issue`
    /// routing rule (plan roots own their stream; other beads follow an
    /// existing parent link, never the read-model lineage root).
    pub(crate) fn stream_id_for_issue(
        &self,
        issue_id: &str,
    ) -> Result<String, BeadError> {
        use crate::bead::wire::IssueTypeWire;
        let issue = self.get(issue_id)?;
        if issue.issue_type == IssueTypeWire::Plan {
            return Ok(issue.id.clone());
        }
        if let Some(parent_id) = issue.parent_id.as_deref() {
            if self.get(parent_id).is_ok() {
                return Ok(parent_id.to_string());
            }
        }
        Ok(issue.id.clone())
    }

    /// Projection-receipt check over the affected stream only: loads the
    /// single physical stream file instead of every stream.
    pub(crate) fn projection_receipt_seen(
        &self,
        issue_id: &str,
        operation_id: &str,
        operation: BeadEventOperationWire,
        target_ref: &str,
        relation: &str,
        direction: BeadLinkDirectionWire,
    ) -> Result<bool, BeadError> {
        use crate::artifact_link::canonicalize_artifact_link_ref;
        let canonical =
            canonicalize_artifact_link_ref(target_ref).map_err(|error| {
                BeadError {
                    kind: error.kind,
                    message: error.message,
                }
            })?;
        let stream_id = self.stream_id_for_issue(issue_id)?;
        let path = event_streams_dir(&self.beads_dir)
            .join(format!("{stream_id}.jsonl"));
        if !path.is_file() {
            return Ok(false);
        }
        let (stream, _signature) = read_event_stream_file(&path)?;
        #[cfg(test)]
        crate::bead::mutation::store::store_io_stats::record_stream_reads(1);
        Ok(stream.events.iter().any(|event| match &event.payload {
            BeadEventPayloadWire::LinkAdded {
                operation_id: Some(existing),
                target_ref: existing_target,
                relation: existing_relation,
                direction: existing_direction,
                ..
            } if operation == BeadEventOperationWire::LinkAdded => {
                existing == operation_id
                    && existing_relation == relation
                    && *existing_direction == direction
                    && canonicalize_artifact_link_ref(existing_target)
                        .map(|value| value == canonical)
                        .unwrap_or(false)
            }
            BeadEventPayloadWire::LinkRemoved {
                operation_id: Some(existing),
                target_ref: existing_target,
                relation: existing_relation,
                direction: existing_direction,
                ..
            } if operation == BeadEventOperationWire::LinkRemoved => {
                existing == operation_id
                    && existing_relation == relation
                    && *existing_direction == direction
                    && canonicalize_artifact_link_ref(existing_target)
                        .map(|value| value == canonical)
                        .unwrap_or(false)
            }
            _ => false,
        }))
    }
}

/// Read the baseline witness for a freshly admitted cache.
///
/// Best-effort: any fault yields the default witness and the later
/// pre-write validation treats it as stale (repair path) rather than
/// failing the mutation. Never turns a cache fault into a semantic
/// error here; admission already proved freshness.
fn read_cache_witness(cache_path: &Path) -> Result<CacheWitness, BeadError> {
    let connection = Connection::open_with_flags(
        cache_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .map_err(|error| BeadError::io(error.to_string()))?;
    let mut values: std::collections::BTreeMap<String, String> =
        std::collections::BTreeMap::new();
    for key in ["generation", "content_generation", "frontier", "token"] {
        let mut stmt = connection
            .prepare("SELECT value FROM meta WHERE key = ?1")
            .map_err(|error| BeadError::io(error.to_string()))?;
        let mut rows = stmt
            .query([key])
            .map_err(|error| BeadError::io(error.to_string()))?;
        if let Some(row) = rows
            .next()
            .map_err(|error| BeadError::io(error.to_string()))?
        {
            values.insert(
                key.to_string(),
                row.get::<_, String>(0)
                    .map_err(|error| BeadError::io(error.to_string()))?,
            );
        }
    }
    let counter = |key: &str| {
        values
            .get(key)
            .and_then(|value| value.parse().ok())
            .unwrap_or(0)
    };
    Ok(CacheWitness {
        generation: counter("generation"),
        content_generation: counter("content_generation"),
        frontier: values.get("frontier").cloned().unwrap_or_default(),
        token: values.get("token").cloned().unwrap_or_default(),
    })
}

/// Read the admission witness for a cache path outside the view.
///
/// Best-effort like [`MutationView::load_cached`]: any fault yields an
/// all-zero witness, and the later CAS treats it as stale (skip and
/// repair) rather than failing the mutation. Used by tests only; the
/// warm paths read the witness from their admitted view.
#[allow(dead_code)]
pub(crate) fn read_admission_witness(cache_path: &Path) -> CacheWitness {
    read_cache_witness(cache_path).unwrap_or_default()
}
