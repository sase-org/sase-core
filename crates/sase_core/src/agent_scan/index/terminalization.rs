//! Bounded stale-row terminalization with witnessed process and claim checks.
//!
//! Background index maintenance classifies marker-free and leftover-running
//! rows from fresh filesystem and process evidence. It never writes source
//! done markers. A persisted keyset cursor advances past skipped rows so a
//! bounded pass cannot starve on a protected prefix.

use super::maintenance::{upsert_record, ABANDONED_DONE_OUTCOME};
use super::record_summary::{MarkerSignatures, RecordSummary};
use super::refresh::projects_root_key;
use crate::agent_launch::{
    list_workspace_claims_from_content, WorkspaceClaimWire,
};
use crate::agent_runtime::parse_runtime_timestamp;
use crate::agent_scan::scanner::scan_agent_artifact_dir;
use crate::agent_scan::wire::{
    decode_agent_artifact_record_json, AgentArtifactRecordWire,
    AgentArtifactScanOptionsWire, DoneMarkerWire,
};
use crate::host_liveness::{
    owner_process_pid, process_is_provably_missing, status_is_zombie,
};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub(super) const TERMINALIZATION_CURSOR_KEY_PREFIX: &str =
    "terminalization_cursor:";

const CANDIDATE_WHERE: &str = "has_done_marker = 0 \
     AND has_waiting_marker = 0 \
     AND has_workflow_state = 0 \
     AND pending_question_sig IS NULL";

/// Process identity observation used by terminalization.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ProcessIdentityObservation {
    MatchingLive,
    Missing,
    Zombie,
    ReusedPid,
    PreviousBoot,
    Unknown,
}

impl ProcessIdentityObservation {
    fn is_dead(self) -> bool {
        matches!(
            self,
            Self::Missing | Self::Zombie | Self::ReusedPid | Self::PreviousBoot
        )
    }
}

pub(super) trait ProcessIdentityProbe {
    fn observe(
        &self,
        pid: i64,
        recorded_identity: Option<&str>,
    ) -> ProcessIdentityObservation;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum ClaimRead {
    Claims(Vec<WorkspaceClaimWire>),
    Missing,
    Unreadable,
}

pub(super) trait ClaimProbe {
    fn claims_for(&self, project_file: &Path) -> ClaimRead;
    fn refresh(&self, project_file: &Path) -> ClaimRead;
}

pub(super) struct TerminalizationWitnesses {
    pub process: Box<dyn ProcessIdentityProbe>,
    pub claims: Box<dyn ClaimProbe>,
    pub now: SystemTime,
    pub local_machine: Option<String>,
    pub on_after_observe: Option<Box<dyn Fn()>>,
}

impl Default for TerminalizationWitnesses {
    fn default() -> Self {
        Self {
            process: Box::new(HostProcessIdentityProbe::default()),
            claims: Box::new(FilesystemClaimProbe::default()),
            now: SystemTime::now(),
            local_machine: local_machine_name(),
            on_after_observe: None,
        }
    }
}

#[derive(Debug)]
pub(super) struct HostProcessIdentityProbe {
    proc_root: PathBuf,
}

impl HostProcessIdentityProbe {
    #[cfg(test)]
    pub(super) fn at(proc_root: PathBuf) -> Self {
        Self { proc_root }
    }
}

impl Default for HostProcessIdentityProbe {
    fn default() -> Self {
        Self {
            proc_root: PathBuf::from("/proc"),
        }
    }
}

impl ProcessIdentityProbe for HostProcessIdentityProbe {
    fn observe(
        &self,
        pid: i64,
        recorded_identity: Option<&str>,
    ) -> ProcessIdentityObservation {
        observe_process_identity(&self.proc_root, pid, recorded_identity)
    }
}

#[cfg(test)]
#[derive(Debug, Clone)]
pub(super) struct MapProcessProbe {
    by_pid: BTreeMap<i64, ProcessIdentityObservation>,
    default: ProcessIdentityObservation,
}

#[cfg(test)]
impl MapProcessProbe {
    pub(super) fn all(observation: ProcessIdentityObservation) -> Self {
        Self {
            by_pid: BTreeMap::new(),
            default: observation,
        }
    }

    pub(super) fn with(
        pid: i64,
        observation: ProcessIdentityObservation,
    ) -> Self {
        let mut probe = Self::all(ProcessIdentityObservation::Unknown);
        probe.by_pid.insert(pid, observation);
        probe
    }

    pub(super) fn insert(
        &mut self,
        pid: i64,
        observation: ProcessIdentityObservation,
    ) {
        self.by_pid.insert(pid, observation);
    }
}

#[cfg(test)]
impl ProcessIdentityProbe for MapProcessProbe {
    fn observe(
        &self,
        pid: i64,
        _recorded_identity: Option<&str>,
    ) -> ProcessIdentityObservation {
        self.by_pid.get(&pid).copied().unwrap_or(self.default)
    }
}

#[derive(Debug, Default)]
pub(super) struct FilesystemClaimProbe {
    cache: Mutex<BTreeMap<PathBuf, ClaimRead>>,
}

impl ClaimProbe for FilesystemClaimProbe {
    fn claims_for(&self, project_file: &Path) -> ClaimRead {
        let mut cache = match self.cache.lock() {
            Ok(cache) => cache,
            Err(_) => return read_project_claims(project_file),
        };
        cache
            .entry(project_file.to_path_buf())
            .or_insert_with(|| read_project_claims(project_file))
            .clone()
    }

    fn refresh(&self, project_file: &Path) -> ClaimRead {
        let fresh = read_project_claims(project_file);
        if let Ok(mut cache) = self.cache.lock() {
            cache.insert(project_file.to_path_buf(), fresh.clone());
        }
        fresh
    }
}

#[cfg(test)]
#[derive(Debug, Clone)]
pub(super) struct MapClaimProbe {
    by_file: BTreeMap<PathBuf, ClaimRead>,
    default: ClaimRead,
}

#[cfg(test)]
impl MapClaimProbe {
    pub(super) fn unreadable() -> Self {
        Self {
            by_file: BTreeMap::new(),
            default: ClaimRead::Unreadable,
        }
    }

    pub(super) fn claims(claims: Vec<WorkspaceClaimWire>) -> Self {
        Self {
            by_file: BTreeMap::new(),
            default: ClaimRead::Claims(claims),
        }
    }
}

#[cfg(test)]
impl ClaimProbe for MapClaimProbe {
    fn claims_for(&self, project_file: &Path) -> ClaimRead {
        self.by_file
            .get(project_file)
            .cloned()
            .unwrap_or_else(|| self.default.clone())
    }

    fn refresh(&self, project_file: &Path) -> ClaimRead {
        self.claims_for(project_file)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SkipReason {
    Live,
    Claim,
    Fresh,
    Unknown,
    Race,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(super) struct TerminalizationCounters {
    pub examined: u64,
    pub source_done: u64,
    pub synthesized: u64,
    pub skipped_live: u64,
    pub skipped_claim: u64,
    pub skipped_fresh: u64,
    pub skipped_unknown: u64,
    pub skipped_race: u64,
    pub wrapped: bool,
}

impl TerminalizationCounters {
    fn record_skip(&mut self, reason: SkipReason) {
        match reason {
            SkipReason::Live => self.skipped_live += 1,
            SkipReason::Claim => self.skipped_claim += 1,
            SkipReason::Fresh => self.skipped_fresh += 1,
            SkipReason::Unknown => self.skipped_unknown += 1,
            SkipReason::Race => self.skipped_race += 1,
        }
    }

    pub(super) fn skipped_total(&self) -> u64 {
        self.skipped_live
            + self.skipped_claim
            + self.skipped_fresh
            + self.skipped_unknown
            + self.skipped_race
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct TerminalizationCursor {
    timestamp: String,
    artifact_dir: String,
}

#[derive(Debug, Clone)]
struct TerminalizationCandidate {
    timestamp: String,
    artifact_dir: String,
    row_projects_root: String,
    record_json: String,
    stored: MarkerSignatures,
}

enum Repair {
    SourceDone(AgentArtifactRecordWire),
    Synthesize(AgentArtifactRecordWire),
}

struct Observation {
    candidate: TerminalizationCandidate,
    observed_signatures: MarkerSignatures,
    repair: Option<Repair>,
    skip: Option<SkipReason>,
    claim_depends_on_absence: bool,
}

pub(super) fn run_terminalization_pass(
    conn: &mut Connection,
    projects_root: &Path,
    options: &AgentArtifactScanOptionsWire,
    stale_after: Duration,
    max_rows: Option<u32>,
    witnesses: &TerminalizationWitnesses,
) -> Result<TerminalizationCounters, String> {
    let mut counters = TerminalizationCounters::default();
    if max_rows == Some(0) {
        return Ok(counters);
    }

    let cursor_key = cursor_meta_key(projects_root);
    let start_cursor = load_cursor(conn, &cursor_key)?;
    let candidates = select_terminalization_candidates(
        conn,
        projects_root,
        start_cursor.as_ref(),
        max_rows,
    )?;
    if candidates.is_empty() {
        if start_cursor.is_some() {
            clear_cursor(conn, &cursor_key)?;
            counters.wrapped = true;
        } else {
            counters.wrapped = true;
        }
        return Ok(counters);
    }

    let mut observations = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        observations.push(observe_candidate(
            candidate,
            options,
            stale_after,
            witnesses,
        ));
    }
    if let Some(hook) = &witnesses.on_after_observe {
        hook();
    }

    let last_key = observations.last().map(|observation| {
        (
            observation.candidate.timestamp.clone(),
            observation.candidate.artifact_dir.clone(),
        )
    });
    let reached_end = max_rows.is_none_or(|limit| {
        observations.len() < usize::try_from(limit).unwrap_or(usize::MAX)
    });

    let tx = conn.transaction().map_err(|e| e.to_string())?;
    for observation in observations {
        counters.examined += 1;
        match apply_observation(&tx, observation, options, witnesses)? {
            ApplyResult::SourceDone => counters.source_done += 1,
            ApplyResult::Synthesize => counters.synthesized += 1,
            ApplyResult::Skip(reason) => counters.record_skip(reason),
        }
    }
    if reached_end {
        delete_cursor_in(&tx, &cursor_key)?;
        counters.wrapped = true;
    } else if let Some((timestamp, artifact_dir)) = last_key {
        persist_cursor_in(
            &tx,
            &cursor_key,
            &TerminalizationCursor {
                timestamp,
                artifact_dir,
            },
        )?;
    }
    tx.commit().map_err(|e| e.to_string())?;
    Ok(counters)
}

pub(super) fn clear_terminalization_cursors(
    conn: &Connection,
) -> Result<(), String> {
    conn.execute(
        "DELETE FROM meta WHERE key LIKE ?1",
        [format!("{TERMINALIZATION_CURSOR_KEY_PREFIX}%")],
    )
    .map_err(|e| e.to_string())?;
    Ok(())
}

pub(super) fn terminalized_abandoned_record(
    mut record: AgentArtifactRecordWire,
    latest_modified: Option<SystemTime>,
) -> AgentArtifactRecordWire {
    let summary = RecordSummary::from_record(&record);
    let meta = record.agent_meta.as_ref();
    let finished_at = meta
        .and_then(|value| value.stopped_at.as_deref())
        .and_then(parse_runtime_timestamp)
        .or_else(|| {
            latest_modified.and_then(system_time_to_unix_timestamp_secs)
        });
    record.running = None;
    record.waiting = None;
    record.pending_question = None;
    record.has_done_marker = true;
    record.done = Some(DoneMarkerWire {
        outcome: Some(ABANDONED_DONE_OUTCOME.to_string()),
        finished_at,
        finished_at_estimated: true,
        cl_name: summary
            .cl_name
            .clone()
            .or_else(|| Some("unknown".to_string())),
        project_file: Some(record.project_file.clone()),
        workspace_num: meta.and_then(|m| m.workspace_num),
        workspace_dir: meta.and_then(|m| m.workspace_dir.clone()),
        pid: meta.and_then(|m| m.pid),
        model: summary.model.clone(),
        llm_provider: summary.llm_provider.clone(),
        vcs_provider: meta.and_then(|m| m.vcs_provider.clone()),
        name: summary.agent_name.clone(),
        hidden: true,
        ..DoneMarkerWire::default()
    });
    record
}

fn observe_candidate(
    candidate: TerminalizationCandidate,
    options: &AgentArtifactScanOptionsWire,
    stale_after: Duration,
    witnesses: &TerminalizationWitnesses,
) -> Observation {
    let current = MarkerSignatures::from_artifact_dir(&candidate.artifact_dir);
    let projects_root = PathBuf::from(&candidate.row_projects_root);
    let artifact_dir = PathBuf::from(&candidate.artifact_dir);
    let refreshed = if candidate.stored == current {
        match decode_agent_artifact_record_json(&candidate.record_json) {
            Ok(record) => record,
            Err(_) => {
                return Observation {
                    candidate,
                    observed_signatures: current,
                    repair: None,
                    skip: Some(SkipReason::Unknown),
                    claim_depends_on_absence: false,
                };
            }
        }
    } else {
        match scan_agent_artifact_dir(&projects_root, &artifact_dir, options) {
            Some(record) => record,
            None => {
                return Observation {
                    candidate,
                    observed_signatures: current,
                    repair: None,
                    skip: Some(SkipReason::Unknown),
                    claim_depends_on_absence: false,
                };
            }
        }
    };

    if refreshed.has_done_marker || refreshed.done.is_some() {
        return Observation {
            candidate,
            observed_signatures: current,
            repair: Some(Repair::SourceDone(refreshed)),
            skip: None,
            claim_depends_on_absence: false,
        };
    }
    if refreshed.waiting.is_some()
        || refreshed.pending_question.is_some()
        || refreshed.workflow_state.is_some()
    {
        return Observation {
            candidate,
            observed_signatures: current,
            repair: None,
            skip: Some(SkipReason::Live),
            claim_depends_on_absence: false,
        };
    }

    let Some(latest_modified) =
        artifact_dir_latest_modified(&candidate.artifact_dir)
    else {
        return Observation {
            candidate,
            observed_signatures: current,
            repair: None,
            skip: Some(SkipReason::Unknown),
            claim_depends_on_absence: false,
        };
    };
    if !artifact_dir_is_stale(latest_modified, stale_after, witnesses.now) {
        return Observation {
            candidate,
            observed_signatures: current,
            repair: None,
            skip: Some(SkipReason::Fresh),
            claim_depends_on_absence: false,
        };
    }
    if record_is_remote(&refreshed, witnesses.local_machine.as_deref()) {
        return Observation {
            candidate,
            observed_signatures: current,
            repair: None,
            skip: Some(SkipReason::Unknown),
            claim_depends_on_absence: false,
        };
    }

    let project_file = PathBuf::from(&refreshed.project_file);
    match claim_protection(
        &refreshed,
        &witnesses.claims.claims_for(&project_file),
    ) {
        ClaimProtection::Protect => {
            return Observation {
                candidate,
                observed_signatures: current,
                repair: None,
                skip: Some(SkipReason::Claim),
                claim_depends_on_absence: false,
            };
        }
        ClaimProtection::Unreadable => {
            return Observation {
                candidate,
                observed_signatures: current,
                repair: None,
                skip: Some(SkipReason::Unknown),
                claim_depends_on_absence: false,
            };
        }
        ClaimProtection::None => {}
    }

    match process_observation(&refreshed, witnesses.process.as_ref()) {
        ProcessIdentityObservation::MatchingLive => {
            return Observation {
                candidate,
                observed_signatures: current,
                repair: None,
                skip: Some(SkipReason::Live),
                claim_depends_on_absence: false,
            };
        }
        obs if obs.is_dead() => {}
        ProcessIdentityObservation::Unknown => {
            return Observation {
                candidate,
                observed_signatures: current,
                repair: None,
                skip: Some(SkipReason::Unknown),
                claim_depends_on_absence: false,
            };
        }
        _ => {
            return Observation {
                candidate,
                observed_signatures: current,
                repair: None,
                skip: Some(SkipReason::Unknown),
                claim_depends_on_absence: false,
            };
        }
    }

    let terminalized =
        terminalized_abandoned_record(refreshed, Some(latest_modified));
    Observation {
        candidate,
        observed_signatures: current,
        repair: Some(Repair::Synthesize(terminalized)),
        skip: None,
        claim_depends_on_absence: true,
    }
}

enum ApplyResult {
    SourceDone,
    Synthesize,
    Skip(SkipReason),
}

fn apply_observation(
    conn: &Connection,
    observation: Observation,
    options: &AgentArtifactScanOptionsWire,
    witnesses: &TerminalizationWitnesses,
) -> Result<ApplyResult, String> {
    if let Some(reason) = observation.skip {
        return Ok(ApplyResult::Skip(reason));
    }
    let current = MarkerSignatures::from_artifact_dir(
        &observation.candidate.artifact_dir,
    );
    let projects_root = PathBuf::from(&observation.candidate.row_projects_root);
    let artifact_dir = PathBuf::from(&observation.candidate.artifact_dir);

    let stored_json: Option<String> = conn
        .query_row(
            "SELECT record_json FROM agent_artifacts WHERE artifact_dir = ?1",
            [&observation.candidate.artifact_dir],
            |row| row.get(0),
        )
        .optional()
        .map_err(|e| e.to_string())?;
    let Some(stored_json) = stored_json else {
        return Ok(ApplyResult::Skip(SkipReason::Race));
    };
    if stored_json != observation.candidate.record_json {
        return Ok(ApplyResult::Skip(SkipReason::Race));
    }

    match observation.repair {
        Some(Repair::SourceDone(record)) => {
            if current.done.is_none() {
                return Ok(ApplyResult::Skip(SkipReason::Race));
            }
            let applied = if current == observation.observed_signatures {
                record
            } else {
                match scan_agent_artifact_dir(
                    &projects_root,
                    &artifact_dir,
                    options,
                ) {
                    Some(fresh)
                        if fresh.has_done_marker || fresh.done.is_some() =>
                    {
                        fresh
                    }
                    _ => return Ok(ApplyResult::Skip(SkipReason::Race)),
                }
            };
            upsert_record(conn, &projects_root, &applied)?;
            Ok(ApplyResult::SourceDone)
        }
        Some(Repair::Synthesize(record)) => {
            if current != observation.observed_signatures {
                return Ok(ApplyResult::Skip(SkipReason::Race));
            }
            if observation.claim_depends_on_absence {
                let project_file = PathBuf::from(&record.project_file);
                match claim_protection(
                    &record,
                    &witnesses.claims.refresh(&project_file),
                ) {
                    ClaimProtection::None => {}
                    ClaimProtection::Protect => {
                        return Ok(ApplyResult::Skip(SkipReason::Race));
                    }
                    ClaimProtection::Unreadable => {
                        return Ok(ApplyResult::Skip(SkipReason::Race));
                    }
                }
            }
            upsert_record(conn, &projects_root, &record)?;
            Ok(ApplyResult::Synthesize)
        }
        None => Ok(ApplyResult::Skip(SkipReason::Unknown)),
    }
}

fn select_terminalization_candidates(
    conn: &Connection,
    projects_root: &Path,
    cursor: Option<&TerminalizationCursor>,
    max_rows: Option<u32>,
) -> Result<Vec<TerminalizationCandidate>, String> {
    let root = projects_root_key(projects_root);
    let mut sql = format!(
        "SELECT timestamp, artifact_dir, projects_root, record_json, \
         agent_meta_sig, done_sig, running_sig, waiting_sig, \
         pending_question_sig, workflow_state_sig, plan_path_sig, \
         prompt_steps_sig, macros_sig FROM agent_artifacts \
         WHERE {CANDIDATE_WHERE} AND projects_root = ?1"
    );
    if cursor.is_some() {
        sql.push_str(
            " AND (timestamp > ?2 OR (timestamp = ?2 AND artifact_dir > ?3))",
        );
    }
    sql.push_str(" ORDER BY timestamp ASC, artifact_dir ASC");
    if max_rows.is_some() {
        if cursor.is_some() {
            sql.push_str(" LIMIT ?4");
        } else {
            sql.push_str(" LIMIT ?2");
        }
    }

    let mut stmt = conn.prepare(&sql).map_err(|e| e.to_string())?;
    let mut rows = match (cursor, max_rows) {
        (Some(cursor), Some(limit)) => stmt
            .query(params![root, cursor.timestamp, cursor.artifact_dir, limit])
            .map_err(|e| e.to_string())?,
        (Some(cursor), None) => stmt
            .query(params![root, cursor.timestamp, cursor.artifact_dir])
            .map_err(|e| e.to_string())?,
        (None, Some(limit)) => stmt
            .query(params![root, limit])
            .map_err(|e| e.to_string())?,
        (None, None) => stmt.query(params![root]).map_err(|e| e.to_string())?,
    };
    let mut candidates = Vec::new();
    while let Some(row) = rows.next().map_err(|e| e.to_string())? {
        candidates.push(candidate_from_sql(row)?);
    }
    Ok(candidates)
}

fn candidate_from_sql(
    row: &rusqlite::Row<'_>,
) -> Result<TerminalizationCandidate, String> {
    Ok(TerminalizationCandidate {
        timestamp: row.get(0).map_err(|e| e.to_string())?,
        artifact_dir: row.get(1).map_err(|e| e.to_string())?,
        row_projects_root: row.get(2).map_err(|e| e.to_string())?,
        record_json: row.get(3).map_err(|e| e.to_string())?,
        stored: MarkerSignatures {
            agent_meta: row.get(4).map_err(|e| e.to_string())?,
            done: row.get(5).map_err(|e| e.to_string())?,
            running: row.get(6).map_err(|e| e.to_string())?,
            waiting: row.get(7).map_err(|e| e.to_string())?,
            pending_question: row.get(8).map_err(|e| e.to_string())?,
            workflow_state: row.get(9).map_err(|e| e.to_string())?,
            plan_path: row.get(10).map_err(|e| e.to_string())?,
            prompt_steps: row.get(11).map_err(|e| e.to_string())?,
            macros: row.get(12).map_err(|e| e.to_string())?,
        },
    })
}

fn cursor_meta_key(projects_root: &Path) -> String {
    format!(
        "{TERMINALIZATION_CURSOR_KEY_PREFIX}{}",
        projects_root_key(projects_root)
    )
}

fn load_cursor(
    conn: &Connection,
    key: &str,
) -> Result<Option<TerminalizationCursor>, String> {
    let raw: Option<String> = conn
        .query_row("SELECT value FROM meta WHERE key = ?1", [key], |row| {
            row.get(0)
        })
        .optional()
        .map_err(|e| e.to_string())?;
    let Some(raw) = raw else {
        return Ok(None);
    };
    match serde_json::from_str::<TerminalizationCursor>(&raw) {
        Ok(cursor)
            if !cursor.timestamp.is_empty()
                && !cursor.artifact_dir.is_empty() =>
        {
            Ok(Some(cursor))
        }
        _ => Ok(None),
    }
}

fn persist_cursor_in(
    conn: &Connection,
    key: &str,
    cursor: &TerminalizationCursor,
) -> Result<(), String> {
    let value = serde_json::to_string(cursor).map_err(|e| e.to_string())?;
    conn.execute(
        "INSERT OR REPLACE INTO meta(key, value) VALUES (?1, ?2)",
        params![key, value],
    )
    .map_err(|e| e.to_string())?;
    Ok(())
}

fn delete_cursor_in(conn: &Connection, key: &str) -> Result<(), String> {
    conn.execute("DELETE FROM meta WHERE key = ?1", [key])
        .map_err(|e| e.to_string())?;
    Ok(())
}

fn clear_cursor(conn: &Connection, key: &str) -> Result<(), String> {
    delete_cursor_in(conn, key)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClaimProtection {
    Protect,
    None,
    Unreadable,
}

fn claim_protection(
    record: &AgentArtifactRecordWire,
    claims: &ClaimRead,
) -> ClaimProtection {
    match claims {
        ClaimRead::Unreadable => ClaimProtection::Unreadable,
        ClaimRead::Missing => ClaimProtection::None,
        ClaimRead::Claims(claims) if claims.is_empty() => ClaimProtection::None,
        ClaimRead::Claims(claims) => classify_claims(record, claims),
    }
}

fn classify_claims(
    record: &AgentArtifactRecordWire,
    claims: &[WorkspaceClaimWire],
) -> ClaimProtection {
    let workspace_num = record_workspace_num(record);
    for claim in claims {
        let timestamp_matches = claim
            .artifacts_timestamp
            .as_deref()
            .is_some_and(|timestamp| timestamp == record.timestamp);
        if timestamp_matches {
            return ClaimProtection::Protect;
        }
        if (claim.pinned || claim.pid == 0)
            && claim.artifacts_timestamp.is_none()
            && workspace_num.is_some_and(|num| num == claim.workspace_num)
        {
            return ClaimProtection::Protect;
        }
    }
    for claim in claims {
        if claim.artifacts_timestamp.is_some() {
            continue;
        }
        if workspace_num.is_some_and(|num| num == claim.workspace_num) {
            return ClaimProtection::Protect;
        }
    }
    ClaimProtection::None
}

fn process_observation(
    record: &AgentArtifactRecordWire,
    probe: &dyn ProcessIdentityProbe,
) -> ProcessIdentityObservation {
    let recorded_identity = record
        .running
        .as_ref()
        .and_then(|running| running.process_identity.as_deref())
        .or_else(|| {
            record
                .agent_meta
                .as_ref()
                .and_then(|meta| meta.process_identity.as_deref())
        });
    let Some(pid) = owner_process_pid(record) else {
        return ProcessIdentityObservation::Unknown;
    };
    if pid <= 0 {
        return ProcessIdentityObservation::Unknown;
    }
    probe.observe(pid, recorded_identity)
}

fn record_is_remote(
    record: &AgentArtifactRecordWire,
    local_machine: Option<&str>,
) -> bool {
    let summary = RecordSummary::from_record(record);
    let remote = summary
        .source_machine
        .as_deref()
        .or(summary.imported_owner_machine.as_deref());
    let Some(remote) = remote.filter(|value| !value.is_empty()) else {
        return false;
    };
    match local_machine {
        Some(local) => remote != local,
        None => true,
    }
}

fn record_workspace_num(record: &AgentArtifactRecordWire) -> Option<u32> {
    record
        .agent_meta
        .as_ref()
        .and_then(|meta| meta.workspace_num)
        .or_else(|| record.done.as_ref().and_then(|done| done.workspace_num))
        .and_then(|num| u32::try_from(num).ok())
}

fn artifact_dir_is_stale(
    latest: SystemTime,
    stale_after: Duration,
    now: SystemTime,
) -> bool {
    now.duration_since(latest)
        .map(|age| age >= stale_after)
        .unwrap_or(false)
}

fn artifact_dir_latest_modified(artifact_dir: &str) -> Option<SystemTime> {
    let dir = Path::new(artifact_dir);
    let mut latest = fs::metadata(dir).and_then(|m| m.modified()).ok();
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let modified = entry.metadata().and_then(|m| m.modified()).ok();
            latest = max_system_time(latest, modified);
        }
    }
    latest
}

fn max_system_time(
    left: Option<SystemTime>,
    right: Option<SystemTime>,
) -> Option<SystemTime> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn system_time_to_unix_timestamp_secs(value: SystemTime) -> Option<f64> {
    value
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_secs_f64())
}

fn read_project_claims(project_file: &Path) -> ClaimRead {
    match fs::read_to_string(project_file) {
        Ok(content) => {
            ClaimRead::Claims(list_workspace_claims_from_content(&content))
        }
        Err(error) if error.kind() == ErrorKind::NotFound => ClaimRead::Missing,
        Err(_) => ClaimRead::Unreadable,
    }
}

fn local_machine_name() -> Option<String> {
    std::env::var("HOSTNAME")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .or_else(|| {
            fs::read_to_string("/etc/hostname")
                .ok()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
        })
}

fn recorded_identity_is_verifiable(recorded: &str) -> bool {
    let Some((_boot_id, start_ticks)) = recorded.split_once(':') else {
        return false;
    };
    start_ticks.parse::<u64>().is_ok()
}

fn identity_from_previous_boot(
    recorded: &str,
    current_boot_id: Option<&str>,
) -> bool {
    if !recorded_identity_is_verifiable(recorded) {
        return false;
    }
    let boot_id = recorded.split_once(':').map_or("", |(boot, _)| boot);
    matches!(
        current_boot_id,
        Some(current)
            if !boot_id.is_empty()
                && !current.is_empty()
                && boot_id != current
    )
}

fn read_boot_id(proc_root: &Path) -> Option<String> {
    fs::read_to_string(proc_root.join("sys/kernel/random/boot_id"))
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn parse_start_ticks(stat: &str) -> Option<u64> {
    let close_paren = stat.rfind(')')?;
    let fields: Vec<&str> =
        stat[close_paren + 1..].split_whitespace().collect();
    fields.get(19)?.parse().ok()
}

fn process_identity_token_at(
    proc_root: &Path,
    pid: i64,
) -> Result<String, ErrorKind> {
    let path = proc_root.join(pid.to_string()).join("stat");
    let stat = match fs::read_to_string(path) {
        Ok(stat) => stat,
        Err(error) => return Err(error.kind()),
    };
    let start_ticks = parse_start_ticks(&stat).ok_or(ErrorKind::InvalidData)?;
    let boot_id = read_boot_id(proc_root).unwrap_or_default();
    Ok(format!("{boot_id}:{start_ticks}"))
}

fn procfs_is_available(proc_root: &Path) -> bool {
    proc_root.join("sys/kernel/random/boot_id").is_file()
        || proc_root.join("self").exists()
        || (proc_root == Path::new("/proc") && proc_root.exists())
}

fn observe_process_identity(
    proc_root: &Path,
    pid: i64,
    recorded_identity: Option<&str>,
) -> ProcessIdentityObservation {
    if pid <= 0 {
        return ProcessIdentityObservation::Unknown;
    }
    let current_boot = read_boot_id(proc_root);
    if let Some(recorded) = recorded_identity {
        if identity_from_previous_boot(recorded, current_boot.as_deref()) {
            return ProcessIdentityObservation::PreviousBoot;
        }
    }

    if !procfs_is_available(proc_root) {
        // Custom/fake proc roots must never signal the host.
        if proc_root != Path::new("/proc") {
            return ProcessIdentityObservation::Unknown;
        }
        let Ok(pid_i32) = i32::try_from(pid) else {
            return ProcessIdentityObservation::Unknown;
        };
        if process_is_provably_missing(pid_i32) {
            return ProcessIdentityObservation::Missing;
        }
        return ProcessIdentityObservation::Unknown;
    }

    let pid_dir = proc_root.join(pid.to_string());
    match fs::read_to_string(pid_dir.join("status")) {
        Ok(status) if status_is_zombie(&status) => {
            return ProcessIdentityObservation::Zombie;
        }
        Err(error) if error.kind() == ErrorKind::NotFound => {
            return ProcessIdentityObservation::Missing;
        }
        Err(error) if error.kind() == ErrorKind::PermissionDenied => {
            return ProcessIdentityObservation::Unknown;
        }
        Ok(_) | Err(_) => {}
    }
    if !pid_dir.exists() {
        return ProcessIdentityObservation::Missing;
    }

    match process_identity_token_at(proc_root, pid) {
        Ok(current) => match recorded_identity {
            Some(recorded) if recorded_identity_is_verifiable(recorded) => {
                if current == recorded {
                    ProcessIdentityObservation::MatchingLive
                } else {
                    ProcessIdentityObservation::ReusedPid
                }
            }
            Some(_) | None => ProcessIdentityObservation::Unknown,
        },
        Err(ErrorKind::NotFound) => ProcessIdentityObservation::Missing,
        Err(ErrorKind::PermissionDenied | ErrorKind::InvalidData) => {
            ProcessIdentityObservation::Unknown
        }
        Err(_) => ProcessIdentityObservation::Unknown,
    }
}
