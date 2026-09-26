//! Bounded read-only receipt opportunity report.
//!
//! Rust port of `sase.tool.receipt_report`: retained receipt counts plus
//! content-equivalent repeat groups over the newest 500 runs. Reads only;
//! never migrates, quarantines, or writes the ledger.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use rusqlite::params;
use sha2::{Digest, Sha256};

use super::super::receipt::is_safe_relative_path;
use super::super::wire::{
    ToolFingerprintWire, ToolRunReceiptsReportGroupWire,
    ToolRunReceiptsReportItemWire, ToolRunReceiptsReportOpportunitiesWire,
    ToolRunReceiptsReportReceiptsWire, ToolRunReceiptsReportRequestWire,
    ToolRunReceiptsReportResultWire, ToolRunReceiptsReportTopToolWire,
    ToolRunReceiptsReportUncomparableRunWire,
    ToolRunReceiptsReportUncomparableWire, ToolRunReceiptsReportWindowWire,
    TOOL_RUN_WIRE_SCHEMA_VERSION,
};
use super::super::ToolRunError;
use super::connection::{unix_now, validate_schema, with_read_store};

pub const RECEIPTS_REPORT_MAX_RUNS: usize = 500;
pub const RECEIPTS_REPORT_MAX_UNCOMPARABLE: usize = 50;
pub const RECEIPTS_REPORT_MAX_GIT_CALLS: usize = 400;
const RECEIPTS_REPORT_GIT_TIMEOUT: Duration = Duration::from_secs(10);
const RECEIPTS_REPORT_GIT_POLL: Duration = Duration::from_millis(5);

const RECEIPTS_REPORT_NOTE: &str = "opportunities are measurement only; every `sase tool run` still executes its child. An opportunity is not a covering receipt: run `sase tool receipt TOOL` to ask whether the current tree is covered.";
const DELETED_MARK: &str = "<deleted>";

#[derive(Debug, Clone)]
struct LedgerReceipt {
    receipt_id: String,
    source_run_id: String,
    tool_name: String,
    verdict: String,
    mint_ts: i64,
    expiry_ts: i64,
    status: String,
}

#[derive(Debug, Clone)]
struct LedgerRun {
    run_id: String,
    tool: String,
    definition_digest: String,
    extra_args_digest: String,
    created_ts: i64,
    duration_ms: Option<i64>,
    fingerprint: Option<ToolFingerprintWire>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BaseKey {
    tool: String,
    definition_digest: String,
    extra_args_digest: String,
    toolchain: Vec<(String, Option<String>, Option<i32>)>,
    env: Vec<(String, Option<String>)>,
    inputs: Vec<(String, String, Option<String>)>,
}

type LedgerLoad = (Vec<LedgerReceipt>, Vec<LedgerRun>, bool, Option<String>);
type RawRunRow = (
    String,
    String,
    String,
    String,
    i64,
    Option<i64>,
    Option<String>,
);
type RepoViews = BTreeMap<String, (Option<String>, BTreeMap<String, String>)>;
type OpportunityOutcome = (Vec<Vec<LedgerRun>>, Vec<(String, String, String)>);

pub fn tool_run_receipts_report(
    store_path: &Path,
    request: ToolRunReceiptsReportRequestWire,
    busy_timeout: Duration,
) -> Result<ToolRunReceiptsReportResultWire, ToolRunError> {
    validate_schema(request.schema_version)?;
    if request.days < 0 {
        return Err(ToolRunError::invalid("-d/--days must be >= 0"));
    }
    let now = request.now_ts.unwrap_or_else(unix_now);
    let since = now.saturating_sub(request.days.saturating_mul(86_400));
    let root = if request.project_root.trim().is_empty() {
        PathBuf::from(".")
    } else {
        PathBuf::from(&request.project_root)
    };
    if !store_path.exists() {
        return Ok(empty_report(
            &request.project,
            request.days,
            since,
            now,
            vec!["tool run store does not exist".to_string()],
        ));
    }
    let (receipts, runs, truncated, ledger_diagnostic) =
        with_read_store(store_path, busy_timeout, |conn| {
            load_ledger(conn, &request.project, since)
        })?;
    if let Some(diagnostic) = ledger_diagnostic {
        return Ok(empty_report(
            &request.project,
            request.days,
            since,
            now,
            vec![diagnostic],
        ));
    }
    let mut index = GitContentIndex::new(request.project.clone(), root);
    let (groups, uncomparable) = find_opportunities(&runs, &mut index);
    Ok(assemble_envelope(
        &request.project,
        request.days,
        since,
        now,
        &receipts,
        &runs,
        truncated,
        groups,
        uncomparable,
    ))
}

fn empty_report(
    project: &str,
    days: i64,
    since_ts: i64,
    now_ts: i64,
    diagnostics: Vec<String>,
) -> ToolRunReceiptsReportResultWire {
    ToolRunReceiptsReportResultWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        project: project.to_string(),
        window: ToolRunReceiptsReportWindowWire {
            days,
            since_ts,
            now_ts,
        },
        receipts: ToolRunReceiptsReportReceiptsWire {
            count: 0,
            active: 0,
            expired: 0,
            superseded: 0,
            items: Vec::new(),
        },
        opportunities: ToolRunReceiptsReportOpportunitiesWire {
            group_count: 0,
            repeat_runs: 0,
            repeat_duration_ms: 0,
            repeat_hours: 0.0,
            groups: Vec::new(),
            top_tools: Vec::new(),
        },
        uncomparable: ToolRunReceiptsReportUncomparableWire {
            count: 0,
            truncated: false,
            runs: Vec::new(),
        },
        runs_scanned: 0,
        runs_truncated: false,
        note: RECEIPTS_REPORT_NOTE.to_string(),
        diagnostics,
    }
}

fn load_ledger(
    conn: &rusqlite::Connection,
    project: &str,
    since_ts: i64,
) -> Result<LedgerLoad, ToolRunError> {
    let runs_present: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'runs'",
            [],
            |row| row.get(0),
        )
        .unwrap_or(0);
    if runs_present == 0 {
        return Ok((
            Vec::new(),
            Vec::new(),
            false,
            Some("receipt ledger unavailable: no such table: runs".to_string()),
        ));
    }
    let receipts_present: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'tool_receipts'",
            [],
            |row| row.get(0),
        )
        .unwrap_or(0);
    let receipts = if receipts_present == 0 {
        Vec::new()
    } else {
        match load_receipts(conn, project, since_ts) {
            Ok(rows) => rows,
            Err(error) => {
                if error.to_string().contains("no such table") {
                    return Ok((
                        Vec::new(),
                        Vec::new(),
                        false,
                        Some(format!("receipt ledger unavailable: {error}")),
                    ));
                }
                return Err(error);
            }
        }
    };
    let (runs, truncated) = match load_runs(conn, project, since_ts) {
        Ok(value) => value,
        Err(error) => {
            if error.to_string().contains("no such table") {
                return Ok((
                    Vec::new(),
                    Vec::new(),
                    false,
                    Some(format!("receipt ledger unavailable: {error}")),
                ));
            }
            return Err(error);
        }
    };
    Ok((receipts, runs, truncated, None))
}

fn load_receipts(
    conn: &rusqlite::Connection,
    project: &str,
    since_ts: i64,
) -> Result<Vec<LedgerReceipt>, ToolRunError> {
    let mut stmt = conn.prepare(
        "SELECT receipt_id, source_run_id, tool_name, verdict, mint_ts, expiry_ts, status
         FROM tool_receipts WHERE project = ?1 AND mint_ts >= ?2 ORDER BY mint_ts DESC",
    )?;
    let rows = stmt.query_map(params![project, since_ts], |row| {
        Ok(LedgerReceipt {
            receipt_id: row.get(0)?,
            source_run_id: row.get(1)?,
            tool_name: row.get(2)?,
            verdict: row.get(3)?,
            mint_ts: row.get(4)?,
            expiry_ts: row.get(5)?,
            status: row.get(6)?,
        })
    })?;
    let mut out = Vec::new();
    for row in rows {
        out.push(row?);
    }
    Ok(out)
}

fn load_runs(
    conn: &rusqlite::Connection,
    project: &str,
    since_ts: i64,
) -> Result<(Vec<LedgerRun>, bool), ToolRunError> {
    let fetch = RECEIPTS_REPORT_MAX_RUNS + 1;
    let mut stmt = conn.prepare(
        "SELECT run_id, tool_name, definition_digest, extra_args_digest, created_ts, duration_ms, fingerprint_after_json
         FROM runs WHERE project = ?1 AND created_ts >= ?2 ORDER BY created_ts DESC LIMIT ?3",
    )?;
    let rows =
        stmt.query_map(params![project, since_ts, fetch as i64], |row| {
            let tool_name: Option<String> = row.get(1)?;
            let definition_digest: Option<String> = row.get(2)?;
            let extra_args_digest: Option<String> = row.get(3)?;
            let fingerprint_raw: Option<String> = row.get(6)?;
            Ok((
                row.get::<_, String>(0)?,
                tool_name.unwrap_or_default(),
                definition_digest.unwrap_or_default(),
                extra_args_digest.unwrap_or_default(),
                row.get::<_, i64>(4)?,
                row.get::<_, Option<i64>>(5)?,
                fingerprint_raw,
            ))
        })?;
    let mut collected: Vec<RawRunRow> = Vec::new();
    for row in rows {
        collected.push(row?);
    }
    let truncated = collected.len() > RECEIPTS_REPORT_MAX_RUNS;
    let mut runs = Vec::new();
    for (
        run_id,
        tool,
        definition_digest,
        extra_args_digest,
        created_ts,
        duration_ms,
        raw,
    ) in collected.into_iter().take(RECEIPTS_REPORT_MAX_RUNS)
    {
        runs.push(LedgerRun {
            run_id,
            tool,
            definition_digest,
            extra_args_digest,
            created_ts,
            duration_ms,
            fingerprint: parse_fingerprint(raw.as_deref()),
        });
    }
    Ok((runs, truncated))
}

fn parse_fingerprint(raw: Option<&str>) -> Option<ToolFingerprintWire> {
    let text = raw?.trim();
    if text.is_empty() {
        return None;
    }
    let value: serde_json::Value = serde_json::from_str(text).ok()?;
    if !value.is_object() {
        return None;
    }
    serde_json::from_value(value).ok()
}

fn base_key(run: &LedgerRun) -> Result<BaseKey, String> {
    let fingerprint = run
        .fingerprint
        .as_ref()
        .ok_or_else(|| "missing fingerprint".to_string())?;
    if !fingerprint.completeness.complete {
        let detail = if fingerprint.completeness.missing.is_empty() {
            "incomplete".to_string()
        } else {
            fingerprint.completeness.missing.join(", ")
        };
        return Err(format!("incomplete fingerprint: {detail}"));
    }
    let mut toolchain = Vec::new();
    for (name, probe) in &fingerprint.toolchain {
        if let Some(reason) = probe.incomplete.as_deref() {
            let _ = reason;
            return Err(format!("incomplete toolchain probe '{name}'"));
        }
        toolchain.push((name.clone(), probe.output.clone(), probe.exit_code));
    }
    toolchain.sort();
    let mut env: Vec<(String, Option<String>)> = fingerprint
        .env
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    env.sort();
    let mut inputs = Vec::new();
    for record in &fingerprint.inputs {
        if record.incomplete.is_some() {
            return Err(format!("incomplete input '{}'", record.pattern));
        }
        for entry in &record.matches {
            if entry.incomplete.is_some() {
                return Err(format!("incomplete input '{}'", record.pattern));
            }
            inputs.push((
                record.pattern.clone(),
                entry.path.clone(),
                entry.content_hash.clone(),
            ));
        }
    }
    inputs.sort();
    Ok(BaseKey {
        tool: run.tool.clone(),
        definition_digest: run.definition_digest.clone(),
        extra_args_digest: run.extra_args_digest.clone(),
        toolchain,
        env,
        inputs,
    })
}

fn repo_views(run: &LedgerRun) -> Result<RepoViews, String> {
    let fingerprint = run
        .fingerprint
        .as_ref()
        .ok_or_else(|| "missing fingerprint".to_string())?;
    let mut views = BTreeMap::new();
    for repo in &fingerprint.repos {
        let identity = repo.identity.clone();
        if repo.incomplete.is_some() {
            return Err(format!("incomplete repo '{identity}'"));
        }
        let mut dirty = BTreeMap::new();
        for entry in &repo.dirty_paths {
            let path = entry.path.clone();
            if entry.incomplete.is_some()
                || (entry.kind != "deleted" && entry.content_hash.is_none())
            {
                return Err(format!("unhashed dirty path '{path}'"));
            }
            let value = if entry.kind == "deleted" {
                DELETED_MARK.to_string()
            } else {
                entry.content_hash.clone().unwrap_or_default()
            };
            dirty.insert(path, value);
        }
        views.insert(identity, (repo.head.clone(), dirty));
    }
    Ok(views)
}

struct GitContentIndex {
    project: String,
    root: PathBuf,
    repo_paths: HashMap<String, Option<PathBuf>>,
    head_exists: HashMap<(String, String), bool>,
    diff_names: HashMap<(String, String, String), Option<BTreeSet<String>>>,
    blob_hashes: HashMap<(String, String, String), Option<String>>,
    calls: usize,
}

impl GitContentIndex {
    fn new(project: String, root: PathBuf) -> Self {
        Self {
            project,
            root,
            repo_paths: HashMap::new(),
            head_exists: HashMap::new(),
            diff_names: HashMap::new(),
            blob_hashes: HashMap::new(),
            calls: 0,
        }
    }

    fn budgeted(&self) -> Result<(), String> {
        if self.calls >= RECEIPTS_REPORT_MAX_GIT_CALLS {
            return Err("content comparison budget exceeded".to_string());
        }
        Ok(())
    }

    fn exec_git(&mut self, repo: &Path, args: &[&str]) -> Option<Vec<u8>> {
        if self.budgeted().is_err() {
            return None;
        }
        self.calls += 1;
        run_git_bytes(repo, args)
    }

    fn git_root(&mut self, start: &Path) -> Result<Option<PathBuf>, String> {
        self.budgeted()?;
        self.calls += 1;
        match run_git_bytes(start, &["rev-parse", "--show-toplevel"]) {
            Some(bytes) => {
                let text = String::from_utf8_lossy(&bytes).trim().to_string();
                if text.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(PathBuf::from(text)))
                }
            }
            None => Ok(None),
        }
    }

    fn repo_path(&mut self, identity: &str) -> Result<PathBuf, String> {
        if let Some(cached) = self.repo_paths.get(identity) {
            return cached
                .clone()
                .ok_or_else(|| format!("unresolvable repo '{identity}'"));
        }
        let resolved = if identity == self.project
            || identity == "current"
            || identity == "."
        {
            let root = self.root.clone();
            match self.git_root(&root) {
                Ok(Some(candidate)) => Some(candidate),
                Ok(None) => Some(root),
                Err(reason) => {
                    self.repo_paths.insert(identity.to_string(), None);
                    return Err(reason);
                }
            }
        } else {
            if identity.is_empty()
                || identity.contains('/')
                || identity.contains('\\')
                || identity.contains('\0')
                || identity == ".."
                || identity.contains("..")
            {
                self.repo_paths.insert(identity.to_string(), None);
                return Err(format!("unresolvable repo '{identity}'"));
            }
            let clone = self
                .root
                .join("sase")
                .join("repos")
                .join("linked")
                .join(identity);
            if is_git_dir(&clone) {
                Some(clone)
            } else {
                None
            }
        };
        self.repo_paths
            .insert(identity.to_string(), resolved.clone());
        resolved.ok_or_else(|| format!("unresolvable repo '{identity}'"))
    }

    fn head_exists(&mut self, repo: &Path, head: &str) -> Result<bool, String> {
        let key = (repo.to_string_lossy().into_owned(), head.to_string());
        if let Some(cached) = self.head_exists.get(&key) {
            return Ok(*cached);
        }
        self.budgeted()?;
        let repo_owned = repo.to_path_buf();
        let head_owned = head.to_string();
        let present = self
            .exec_git(&repo_owned, &["cat-file", "-e", &head_owned])
            .is_some();
        // exec_git returns None on both budget exhaustion and missing object;
        // distinguish by checking the budget after the call.
        if !present && self.calls >= RECEIPTS_REPORT_MAX_GIT_CALLS {
            // If we just exhausted the budget, surface it as uncomparable
            // only when the object check itself consumed the last call.
            // A genuinely missing object also returns None, so keep the
            // missing-object reading unless the budget is now exceeded and
            // the call count shows the check was the budget breaker. The
            // budgeted() gate above already fails closed before the call,
            // so reaching here with None means the object is missing.
        }
        self.head_exists.insert(key, present);
        Ok(present)
    }

    fn diff_name_set(
        &mut self,
        repo: &Path,
        first: &str,
        second: &str,
    ) -> Result<BTreeSet<String>, String> {
        let key = (
            repo.to_string_lossy().into_owned(),
            first.to_string(),
            second.to_string(),
        );
        if let Some(cached) = self.diff_names.get(&key) {
            return cached
                .clone()
                .ok_or_else(|| "cannot diff commits".to_string());
        }
        self.budgeted()?;
        let repo_owned = repo.to_path_buf();
        let first_owned = first.to_string();
        let second_owned = second.to_string();
        let output = self.exec_git(
            &repo_owned,
            &["diff", "--name-only", &first_owned, &second_owned],
        );
        match output {
            None => {
                self.diff_names.insert(key, None);
                Err("cannot diff commits".to_string())
            }
            Some(bytes) => {
                let text = String::from_utf8_lossy(&bytes);
                let mut names = BTreeSet::new();
                for line in text.split('\n') {
                    // split('\n') leaves a trailing empty item; match
                    // Python splitlines filtering of blank lines.
                    let stripped = line.strip_suffix('\r').unwrap_or(line);
                    if stripped.trim().is_empty() {
                        continue;
                    }
                    names.insert(stripped.to_string());
                }
                self.diff_names.insert(key, Some(names.clone()));
                Ok(names)
            }
        }
    }

    fn blob_sha256(
        &mut self,
        repo: &Path,
        head: &str,
        path: &str,
    ) -> Result<Option<String>, String> {
        if !is_safe_relative_path(path) {
            return Err(format!("unsafe path '{path}'"));
        }
        let key = (
            repo.to_string_lossy().into_owned(),
            head.to_string(),
            path.to_string(),
        );
        if let Some(cached) = self.blob_hashes.get(&key) {
            return Ok(cached.clone());
        }
        self.budgeted()?;
        let repo_owned = repo.to_path_buf();
        let spec = format!("{head}:{path}");
        let output = self.exec_git(&repo_owned, &["show", &spec]);
        let digest = output.map(|bytes| {
            let mut hasher = Sha256::new();
            hasher.update(&bytes);
            hex::encode(hasher.finalize())
        });
        self.blob_hashes.insert(key, digest.clone());
        Ok(digest)
    }
}

fn is_git_dir(path: &Path) -> bool {
    path.is_dir() && (path.join(".git").exists() || path.join("HEAD").exists())
}

fn run_git_bytes(repo: &Path, args: &[&str]) -> Option<Vec<u8>> {
    let mut stdout_file = tempfile::tempfile().ok()?;
    let writer = stdout_file.try_clone().ok()?;
    let mut child = Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::from(writer))
        .stderr(Stdio::null())
        .spawn()
        .ok()?;
    let started = Instant::now();
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) if started.elapsed() < RECEIPTS_REPORT_GIT_TIMEOUT => {
                thread::sleep(RECEIPTS_REPORT_GIT_POLL);
            }
            Ok(None) => {
                let _ = child.kill();
                let _ = child.wait();
                return None;
            }
            Err(_) => {
                let _ = child.kill();
                let _ = child.wait();
                return None;
            }
        }
    };
    if !status.success() {
        return None;
    }
    stdout_file.seek(SeekFrom::Start(0)).ok()?;
    let mut output = Vec::new();
    stdout_file.read_to_end(&mut output).ok()?;
    Some(output)
}

fn runs_equivalent(
    left: &LedgerRun,
    right: &LedgerRun,
    index: &mut GitContentIndex,
) -> Result<bool, String> {
    let left_views = repo_views(left)?;
    let right_views = repo_views(right)?;
    if left_views.keys().collect::<BTreeSet<_>>()
        != right_views.keys().collect::<BTreeSet<_>>()
    {
        return Ok(false);
    }
    for identity in left_views.keys() {
        let (left_head, left_dirty) = &left_views[identity];
        let (right_head, right_dirty) = &right_views[identity];
        if left_head == right_head && left_head.is_some() {
            if left_dirty != right_dirty {
                return Ok(false);
            }
            continue;
        }
        let repo = index.repo_path(identity)?;
        for head in [left_head, right_head] {
            match head {
                Some(value) if !value.is_empty() => {
                    if !index.head_exists(&repo, value)? {
                        return Err(format!("missing Git object '{value}'"));
                    }
                }
                _ => {
                    return Err("missing Git object 'HEAD'".to_string());
                }
            }
        }
        let left_commit = left_head.clone().unwrap_or_default();
        let right_commit = right_head.clone().unwrap_or_default();
        let changed =
            index.diff_name_set(&repo, &left_commit, &right_commit)?;
        let mut union = BTreeSet::new();
        for path in left_dirty.keys() {
            union.insert(path.clone());
        }
        for path in right_dirty.keys() {
            union.insert(path.clone());
        }
        if !changed.is_subset(&union) {
            return Ok(false);
        }
        for path in union {
            if left_dirty.get(&path) == right_dirty.get(&path) {
                continue;
            }
            if !dirty_matches_other_side(
                index,
                &repo,
                &path,
                &left_commit,
                left_dirty,
                &right_commit,
                right_dirty,
            )? {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

fn dirty_matches_other_side(
    index: &mut GitContentIndex,
    repo: &Path,
    path: &str,
    left_head: &str,
    left_dirty: &BTreeMap<String, String>,
    right_head: &str,
    right_dirty: &BTreeMap<String, String>,
) -> Result<bool, String> {
    let left_value = left_dirty.get(path);
    let right_value = right_dirty.get(path);
    let left_committed = index.blob_sha256(repo, left_head, path)?;
    let right_committed = index.blob_sha256(repo, right_head, path)?;
    let left_actual = match left_value {
        Some(value) => value.clone(),
        None => left_committed.unwrap_or_else(|| DELETED_MARK.to_string()),
    };
    let right_actual = match right_value {
        Some(value) => value.clone(),
        None => right_committed.unwrap_or_else(|| DELETED_MARK.to_string()),
    };
    Ok(left_actual == right_actual)
}

fn find_opportunities(
    runs: &[LedgerRun],
    index: &mut GitContentIndex,
) -> OpportunityOutcome {
    let mut comparable = Vec::new();
    let mut keys: HashMap<String, BaseKey> = HashMap::new();
    let mut uncomparable = Vec::new();
    for run in runs {
        match base_key(run) {
            Ok(key) => {
                keys.insert(run.run_id.clone(), key);
                comparable.push(run.clone());
            }
            Err(reason) => {
                uncomparable.push((
                    run.run_id.clone(),
                    run.tool.clone(),
                    reason,
                ));
            }
        }
    }
    let mut parents: HashMap<String, String> = HashMap::new();
    for run in &comparable {
        parents.insert(run.run_id.clone(), run.run_id.clone());
    }
    let find = |id: &str, parents: &mut HashMap<String, String>| -> String {
        let mut current = id.to_string();
        while parents.get(&current).map(String::as_str)
            != Some(current.as_str())
        {
            let next = parents
                .get(&current)
                .cloned()
                .unwrap_or_else(|| current.clone());
            // Path compression one hop.
            if let Some(grand) = parents.get(&next).cloned() {
                parents.insert(current.clone(), grand.clone());
                current = grand;
            } else {
                current = next;
            }
        }
        current
    };
    for position in 0..comparable.len() {
        let left = comparable[position].clone();
        for right in comparable.iter().skip(position + 1) {
            let left_key = keys.get(&left.run_id);
            let right_key = keys.get(&right.run_id);
            if left_key != right_key {
                continue;
            }
            match runs_equivalent(&left, right, index) {
                Ok(true) => {
                    let left_root = find(&left.run_id, &mut parents);
                    let right_root = find(&right.run_id, &mut parents);
                    parents.insert(left_root, right_root);
                }
                Ok(false) => {}
                Err(reason) => {
                    uncomparable.push((
                        right.run_id.clone(),
                        right.tool.clone(),
                        reason,
                    ));
                }
            }
        }
    }
    let mut buckets: HashMap<String, Vec<LedgerRun>> = HashMap::new();
    for run in &comparable {
        if uncomparable.iter().any(|(id, _, _)| id == &run.run_id) {
            continue;
        }
        let root = find(&run.run_id, &mut parents);
        buckets.entry(root).or_default().push(run.clone());
    }
    let mut groups: Vec<Vec<LedgerRun>> = buckets
        .into_values()
        .filter(|bucket| bucket.len() > 1)
        .collect();
    groups.sort_by_key(|bucket| {
        bucket.iter().map(|run| run.created_ts).min().unwrap_or(0)
    });
    (groups, uncomparable)
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn assemble_envelope(
    project: &str,
    days: i64,
    since_ts: i64,
    now_ts: i64,
    receipts: &[LedgerReceipt],
    runs: &[LedgerRun],
    runs_truncated: bool,
    groups: Vec<Vec<LedgerRun>>,
    uncomparable: Vec<(String, String, String)>,
) -> ToolRunReceiptsReportResultWire {
    let mut items = Vec::new();
    let mut active = 0usize;
    let mut expired = 0usize;
    let mut superseded = 0usize;
    for receipt in receipts {
        let is_expired = receipt.expiry_ts <= now_ts;
        if receipt.status == "active" && !is_expired {
            active += 1;
        } else if is_expired {
            expired += 1;
        } else {
            superseded += 1;
        }
        items.push(ToolRunReceiptsReportItemWire {
            receipt_id: receipt.receipt_id.clone(),
            run_id: receipt.source_run_id.clone(),
            tool: receipt.tool_name.clone(),
            verdict: receipt.verdict.clone(),
            age_seconds: (now_ts - receipt.mint_ts).max(0),
            expired: is_expired,
            status: receipt.status.clone(),
        });
    }
    let mut group_entries = Vec::new();
    let mut per_tool: BTreeMap<String, (usize, usize, i64)> = BTreeMap::new();
    let mut repeat_runs_total = 0usize;
    let mut repeat_ms_total = 0i64;
    for (position, group) in groups.iter().enumerate() {
        let mut ordered = group.clone();
        ordered.sort_by(|left, right| {
            (left.created_ts, &left.run_id)
                .cmp(&(right.created_ts, &right.run_id))
        });
        let repeats = &ordered[1..];
        let saved_ms: i64 =
            repeats.iter().map(|run| run.duration_ms.unwrap_or(0)).sum();
        let mut heads = BTreeSet::new();
        for run in &ordered {
            for head in run_heads(run) {
                heads.insert(head);
            }
        }
        let mut repos = BTreeSet::new();
        for run in &ordered {
            for repo in run_repos(run) {
                repos.insert(repo);
            }
        }
        let tool = ordered[0].tool.clone();
        let entry = ToolRunReceiptsReportGroupWire {
            group_id: position + 1,
            tool: tool.clone(),
            run_ids: ordered.iter().map(|run| run.run_id.clone()).collect(),
            first_ts: ordered[0].created_ts,
            last_ts: ordered[ordered.len() - 1].created_ts,
            runs: ordered.len(),
            repeat_runs: repeats.len(),
            repeat_duration_ms: saved_ms,
            spans_commits: heads.len() > 1,
            repos: repos.into_iter().collect(),
        };
        repeat_runs_total += repeats.len();
        repeat_ms_total += saved_ms;
        let slot = per_tool.entry(tool).or_insert((0, 0, 0));
        slot.0 += 1;
        slot.1 += repeats.len();
        slot.2 += saved_ms;
        group_entries.push(entry);
    }
    let mut top_tools: Vec<ToolRunReceiptsReportTopToolWire> = per_tool
        .into_iter()
        .map(|(tool, (groups, repeat_runs, repeat_ms))| {
            ToolRunReceiptsReportTopToolWire {
                tool,
                groups,
                repeat_runs,
                repeat_duration_ms: repeat_ms,
            }
        })
        .collect();
    top_tools.sort_by(|left, right| {
        (right.repeat_runs, right.repeat_duration_ms)
            .cmp(&(left.repeat_runs, left.repeat_duration_ms))
    });
    let truncated = uncomparable.len() > RECEIPTS_REPORT_MAX_UNCOMPARABLE;
    let shown: Vec<ToolRunReceiptsReportUncomparableRunWire> = uncomparable
        .iter()
        .take(RECEIPTS_REPORT_MAX_UNCOMPARABLE)
        .map(|(run_id, tool, reason)| {
            ToolRunReceiptsReportUncomparableRunWire {
                run_id: run_id.clone(),
                tool: tool.clone(),
                reason: reason.clone(),
            }
        })
        .collect();
    let repeat_hours =
        (repeat_ms_total as f64 / 3_600_000.0 * 100.0).round() / 100.0;
    ToolRunReceiptsReportResultWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        project: project.to_string(),
        window: ToolRunReceiptsReportWindowWire {
            days,
            since_ts,
            now_ts,
        },
        receipts: ToolRunReceiptsReportReceiptsWire {
            count: items.len(),
            active,
            expired,
            superseded,
            items,
        },
        opportunities: ToolRunReceiptsReportOpportunitiesWire {
            group_count: group_entries.len(),
            repeat_runs: repeat_runs_total,
            repeat_duration_ms: repeat_ms_total,
            repeat_hours,
            groups: group_entries,
            top_tools,
        },
        uncomparable: ToolRunReceiptsReportUncomparableWire {
            count: uncomparable.len(),
            truncated,
            runs: shown,
        },
        runs_scanned: runs.len(),
        runs_truncated,
        note: RECEIPTS_REPORT_NOTE.to_string(),
        diagnostics: Vec::new(),
    }
}

fn run_heads(run: &LedgerRun) -> Vec<String> {
    let Some(fingerprint) = run.fingerprint.as_ref() else {
        return Vec::new();
    };
    fingerprint
        .repos
        .iter()
        .filter_map(|repo| repo.head.clone())
        .filter(|head| !head.is_empty())
        .collect()
}

fn run_repos(run: &LedgerRun) -> Vec<String> {
    let Some(fingerprint) = run.fingerprint.as_ref() else {
        return Vec::new();
    };
    fingerprint
        .repos
        .iter()
        .map(|repo| repo.identity.clone())
        .collect()
}

#[cfg(test)]
mod tests;
