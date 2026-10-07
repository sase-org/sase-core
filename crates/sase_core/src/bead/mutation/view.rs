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

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use rusqlite::{Connection, OpenFlags};

use crate::artifact_link::BeadLinkDirectionWire;
use crate::bead::events::{BeadEventOperationWire, BeadEventPayloadWire};
use crate::bead::jsonl::{event_streams_dir, read_event_stream_file};
use crate::bead::read_model::{
    ensure_cache_ready_for_mutation_at, read_model_cache_path_for_store,
};
use crate::bead::wire::{BeadError, IssueWire};

/// How rows are retrieved: indexed cache rows or a replay slice.
pub(crate) enum MutationViewBacking {
    Cached { cache_path: PathBuf },
    Replay,
}

/// One lookup view for a single locked mutation.
///
/// `replay_issues` backs resolution when the cache is unusable; the
/// cached path never hydrates it. The overlay maps an ID to
/// `Some(changed/created row)` or `None` (pending removal).
pub(crate) struct MutationView<'a> {
    beads_dir: PathBuf,
    backing: MutationViewBacking,
    replay_issues: Option<&'a [IssueWire]>,
    overlay: BTreeMap<String, Option<IssueWire>>,
}

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
        let cache_path = read_model_cache_path_for_store(beads_dir);
        if let Some(cache_path) = cache_path {
            match ensure_cache_ready_for_mutation_at(beads_dir, &cache_path) {
                Ok(true) => {
                    return Ok(Self {
                        beads_dir: beads_dir.to_path_buf(),
                        backing: MutationViewBacking::Cached { cache_path },
                        replay_issues: None,
                        overlay: BTreeMap::new(),
                    });
                }
                Ok(false) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(Self {
            beads_dir: beads_dir.to_path_buf(),
            backing: MutationViewBacking::Replay,
            replay_issues: Some(replay_issues),
            overlay: BTreeMap::new(),
        })
    }

    /// True when indexed rows serve this mutation (no full replay).
    pub(crate) fn is_cached(&self) -> bool {
        matches!(self.backing, MutationViewBacking::Cached { .. })
    }

    /// Record a changed or created row in the overlay.
    pub(crate) fn stage_issue(&mut self, issue: IssueWire) {
        self.overlay.insert(issue.id.clone(), Some(issue));
    }

    /// Record a pending removal tombstone in the overlay.
    pub(crate) fn stage_removal(&mut self, issue_id: &str) {
        self.overlay.insert(issue_id.to_string(), None);
    }

    fn open_cached(&self) -> Result<Connection, BeadError> {
        match &self.backing {
            MutationViewBacking::Cached { cache_path } => {
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
    pub(crate) fn get(&self, issue_id: &str) -> Result<IssueWire, BeadError> {
        if let Some(entry) = self.overlay.get(issue_id) {
            return entry.clone().ok_or_else(|| BeadError {
                kind: "not_found".to_string(),
                message: format!("Issue not found: {issue_id}"),
            });
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
                        serde_json::from_str(&text).map_err(|error| {
                            BeadError::io(format!(
                                "cached issue row is not valid: {error}"
                            ))
                        })
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
                for row in mapped {
                    let text: String =
                        row.map_err(|error| BeadError::io(error.to_string()))?;
                    let issue: IssueWire = serde_json::from_str(&text)
                        .map_err(|error| {
                            BeadError::io(format!(
                                "cached issue row is not valid: {error}"
                            ))
                        })?;
                    rows.insert(issue.id.clone(), issue);
                }
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
                for row in mapped {
                    let text: String =
                        row.map_err(|error| BeadError::io(error.to_string()))?;
                    candidate =
                        Some(serde_json::from_str(&text).map_err(|error| {
                            BeadError::io(format!(
                                "cached issue row is not valid: {error}"
                            ))
                        })?);
                }
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
    /// removed IDs, and the overlay — without hydrating every row.
    pub(crate) fn next_top_level_counter(
        &self,
        issue_prefix: &str,
        config_counter: u64,
    ) -> Result<u64, BeadError> {
        let expected = format!("{issue_prefix}-");
        let mut max_seen: u64 = 0;
        let mut ids: Vec<String> = Vec::new();
        match &self.backing {
            MutationViewBacking::Cached { .. } => {
                let connection = self.open_cached()?;
                let mut statement = connection
                    .prepare("SELECT id FROM issues")
                    .map_err(|error| BeadError::io(error.to_string()))?;
                let mapped = statement
                    .query_map([], |row| row.get::<_, String>(0))
                    .map_err(|error| BeadError::io(error.to_string()))?;
                for row in mapped {
                    ids.push(
                        row.map_err(|error| BeadError::io(error.to_string()))?,
                    );
                }
            }
            MutationViewBacking::Replay => {
                ids.extend(
                    self.replay_issues
                        .unwrap_or(&[])
                        .iter()
                        .map(|issue| issue.id.clone()),
                );
            }
        }
        for (id, entry) in &self.overlay {
            if entry.is_some() {
                ids.push(id.clone());
            } else {
                ids.retain(|candidate| candidate != id);
            }
        }
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

    /// Next direct child ID under `parent_id`, overlay-consistent.
    pub(crate) fn next_child_id(
        &self,
        parent_id: &str,
    ) -> Result<String, BeadError> {
        let prefix = format!("{parent_id}.");
        let mut local_max: u64 = 0;
        let mut ids: Vec<String> = Vec::new();
        match &self.backing {
            MutationViewBacking::Cached { .. } => {
                let connection = self.open_cached()?;
                let mut statement = connection
                    .prepare("SELECT id FROM issues WHERE parent = ?1")
                    .map_err(|error| BeadError::io(error.to_string()))?;
                let mapped = statement
                    .query_map([parent_id], |row| row.get::<_, String>(0))
                    .map_err(|error| BeadError::io(error.to_string()))?;
                for row in mapped {
                    ids.push(
                        row.map_err(|error| BeadError::io(error.to_string()))?,
                    );
                }
            }
            MutationViewBacking::Replay => {
                ids.extend(
                    self.replay_issues
                        .unwrap_or(&[])
                        .iter()
                        .filter(|issue| {
                            issue.parent_id.as_deref() == Some(parent_id)
                        })
                        .map(|issue| issue.id.clone()),
                );
            }
        }
        for (id, entry) in &self.overlay {
            if entry.as_ref().is_some_and(|issue| {
                issue.parent_id.as_deref() == Some(parent_id)
            }) {
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
