//! `@`-reference completions: kind and payload candidates, payload
//! inventory, commit-log backed payloads, and the trigger contexts that feed
//! the shared at-reference menu.

use crate::artifact_file::{
    artifact_file_is_vcs_backed, read_artifact_file_index,
};
use crate::editor::at_reference::{
    build_at_reference_menu, detect_at_reference_context,
    is_builtin_at_reference_kind, AtReferenceContextWire,
    AtReferenceInventoryWire, AtReferenceKindRowWire,
    AtReferencePayloadRowWire, AtReferenceStage,
};
use crate::editor::token::DocumentSnapshot;
use crate::editor::wire::{
    ArtifactRefCompletionMode, ArtifactRefCompletionTrigger,
    CompletionCandidate, CompletionContext, CompletionContextKind,
    CompletionList, EditorPosition, EditorRange, EditorTextEdit, TokenInfo,
};
use crate::plan::read::split_frontmatter;
use crate::{ArtifactRefContextWire, ArtifactRefError};
use chrono::{DateTime, Utc};
use std::collections::BTreeSet;
use std::fs;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const ARTIFACT_REF_MAX_DEPTH: usize = 8;
const ARTIFACT_REF_MAX_VISITED: usize = 20_000;
pub(crate) const ARTIFACT_REF_MAX_SCAN_RESULTS: usize = 5_000;
pub const ARTIFACT_REF_COMMIT_ABBREV: usize = 12;
pub const ARTIFACT_REF_COMMIT_SCAN_LIMIT: usize = 200;
pub const ARTIFACT_REF_COMMIT_MAX_ROWS: usize = 1_000;
/// Environment override for the artifact-ref commit-log wall-clock budget,
/// expressed in seconds as a positive, finite decimal number. Anything else
/// (including an unset or empty value) falls back to the default budget.
pub const ARTIFACT_REF_COMMIT_TIMEOUT_ENV: &str =
    "SASE_ARTIFACT_REF_COMMIT_TIMEOUT";
/// Generous enough that a heavily oversubscribed host still produces commit
/// rows. An expired budget yields an empty inventory with no in-band error, so
/// this is a runaway-`git` backstop rather than a responsiveness knob.
pub const ARTIFACT_REF_COMMIT_TIMEOUT_DEFAULT: Duration =
    Duration::from_secs(30);
const ARTIFACT_REF_COMMIT_POLL_INTERVAL: Duration = Duration::from_millis(10);
pub(crate) const ARTIFACT_REF_REPOSITORY_KIND_SIDECAR: &str = "sidecar";
pub fn detect_artifact_ref_context_at_position(
    document: &DocumentSnapshot,
    position: EditorPosition,
    context: &ArtifactRefContextWire,
) -> Option<CompletionContext> {
    let detected = detect_at_reference_context(
        document,
        position,
        &known_artifact_ref_kinds(context),
    )?;
    artifact_ref_completion_context(document, &detected)
}
fn artifact_ref_completion_context(
    document: &DocumentSnapshot,
    detected: &AtReferenceContextWire,
) -> Option<CompletionContext> {
    let mode = match detected.stage {
        AtReferenceStage::Kind => ArtifactRefCompletionMode::Kind,
        AtReferenceStage::Payload => ArtifactRefCompletionMode::Payload,
    };
    let token_range = document.byte_range_to_range(
        detected.candidate_span.0,
        detected.candidate_span.1,
    )?;
    let replacement_range = document.byte_range_to_range(
        detected.replacement_span.0,
        detected.replacement_span.1,
    )?;
    Some(CompletionContext {
        kind: match mode {
            ArtifactRefCompletionMode::Kind => {
                CompletionContextKind::ArtifactRefKind
            }
            ArtifactRefCompletionMode::Payload => {
                CompletionContextKind::ArtifactRefPayload
            }
        },
        token: Some(TokenInfo {
            text: document
                .text()
                .get(detected.candidate_span.0..detected.candidate_span.1)?
                .to_string(),
            range: token_range,
            byte_start: detected.candidate_span.0,
            byte_end: detected.candidate_span.1,
        }),
        active_xprompt: None,
        active_input: None,
        directive_name: None,
        selected_values: Vec::new(),
        directive: None,
        vcs_repo: None,
        vcs_ref: None,
        artifact_ref: Some(ArtifactRefCompletionTrigger {
            mode,
            candidate_span: detected.candidate_span,
            replacement_span: detected.replacement_span,
            query_span: detected.query_span,
            query: detected.query.clone(),
            kind: detected.kind.clone(),
        }),
        replacement_range,
    })
}
fn legacy_at_reference_context(
    trigger: &ArtifactRefCompletionTrigger,
) -> AtReferenceContextWire {
    let stage = match trigger.mode {
        ArtifactRefCompletionMode::Kind => AtReferenceStage::Kind,
        ArtifactRefCompletionMode::Payload => AtReferenceStage::Payload,
    };
    let path_query = (stage == AtReferenceStage::Kind).then(|| {
        let (directory, partial) = trigger
            .query
            .rfind('/')
            .map(|separator| trigger.query.split_at(separator + 1))
            .unwrap_or(("", &trigger.query));
        crate::editor::at_reference::AtReferencePathQueryWire {
            directory: directory.to_string(),
            partial: partial.to_string(),
            show_hidden: partial.starts_with('.'),
        }
    });
    AtReferenceContextWire {
        stage,
        candidate_span: trigger.candidate_span,
        replacement_span: trigger.replacement_span,
        query_span: trigger.query_span,
        query: trigger.query.clone(),
        kind: trigger.kind.clone(),
        path_query,
    }
}
pub fn build_artifact_ref_kind_completion_candidates(
    trigger: &ArtifactRefCompletionTrigger,
    replacement_range: Option<EditorRange>,
    context: &ArtifactRefContextWire,
) -> CompletionList {
    if trigger.mode != ArtifactRefCompletionMode::Kind {
        return empty_artifact_ref_completion_list();
    }
    let detected = legacy_at_reference_context(trigger);
    let inventory = AtReferenceInventoryWire {
        kinds: known_artifact_ref_kinds(context)
            .into_iter()
            .map(|kind| {
                let builtin = is_builtin_at_reference_kind(&kind);
                let detail = if builtin {
                    "builtin artifact kind".to_string()
                } else {
                    context
                        .document_roots
                        .iter()
                        .find(|root| root.kind == kind)
                        .map(|root| {
                            format!("document artifact · {}", root.root)
                        })
                        .unwrap_or_else(|| "document artifact".to_string())
                };
                AtReferenceKindRowWire {
                    kind,
                    builtin,
                    detail,
                }
            })
            .collect(),
        ..Default::default()
    };
    let menu = build_at_reference_menu(&detected, &inventory);
    let candidates = menu
        .rows
        .into_iter()
        .map(|row| {
            artifact_ref_candidate(
                row.label.clone(),
                row.insertion.trim_start_matches('@').to_string(),
                row.detail,
                replacement_range,
                "artifact_kind",
                None,
            )
        })
        .collect();
    CompletionList {
        candidates,
        shared_extension: menu.shared_extension,
    }
}
pub fn build_artifact_ref_payload_completion_candidates(
    trigger: &ArtifactRefCompletionTrigger,
    replacement_range: Option<EditorRange>,
    context: &ArtifactRefContextWire,
) -> CompletionList {
    if trigger.mode != ArtifactRefCompletionMode::Payload {
        return empty_artifact_ref_completion_list();
    }
    let Some(kind) = trigger.kind.as_deref() else {
        return empty_artifact_ref_completion_list();
    };
    if kind == "bug" {
        return empty_artifact_ref_completion_list();
    }

    let Ok(inventory) = build_artifact_ref_payload_inventory(kind, context)
    else {
        return empty_artifact_ref_completion_list();
    };
    let detected = legacy_at_reference_context(trigger);
    let menu = build_at_reference_menu(&detected, &inventory);
    let prefix = format!("@{kind}:");
    let candidates = menu
        .rows
        .into_iter()
        .map(|row| {
            artifact_ref_candidate(
                row.title,
                row.insertion
                    .strip_prefix(&prefix)
                    .unwrap_or(&row.insertion)
                    .to_string(),
                row.detail,
                replacement_range,
                "artifact_payload",
                Some(kind),
            )
        })
        .collect();
    CompletionList {
        candidates,
        shared_extension: menu.shared_extension,
    }
}
/// Enumerate and title the query-independent payload inventory for one kind.
///
/// Filesystem-backed roots scan up to [`ARTIFACT_REF_MAX_SCAN_RESULTS`] rows
/// instead of limiting the corpus to the 200 rows an editor displays. The
/// shared at-reference menu applies fuzzy matching and its display cap after
/// this inventory is built, so a memorable match beyond the first 200 files
/// remains reachable. Callers should cache this inventory: document titles
/// require bounded file reads across the scanned corpus.
///
/// Commit payload inventory enumerates non-sidecar repositories. SDD sidecars
/// are excluded because they are machine-written stores, not human-authored
/// code history.
pub fn build_artifact_ref_payload_inventory(
    kind: &str,
    context: &ArtifactRefContextWire,
) -> Result<AtReferenceInventoryWire, ArtifactRefError> {
    crate::artifact_ref::validate_artifact_ref_context(context)?;
    if kind == "bug" {
        return Ok(AtReferenceInventoryWire::default());
    }

    let mut payloads = Vec::new();
    let mut seen = BTreeSet::new();
    let mut truncated_payloads = 0usize;
    if kind == "commit" {
        truncated_payloads +=
            append_commit_candidates(&mut payloads, &mut seen, context);
    } else if kind == "chat" {
        if let Some(root) = context.chats_root.as_deref() {
            truncated_payloads += append_artifact_path_candidates(
                &mut payloads,
                &mut seen,
                kind,
                Path::new(root),
                None,
            )?;
        }
    } else if kind == "bead" {
        truncated_payloads +=
            append_bead_page_candidates(&mut payloads, &mut seen, context);
    } else if kind == "agent" {
        append_agent_page_candidates(&mut payloads, &mut seen, context);
    } else if kind == "file" {
        append_artifact_index_candidates(&mut payloads, &mut seen, context);
    } else {
        for root in context
            .document_roots
            .iter()
            .filter(|root| root.kind == kind)
        {
            truncated_payloads += append_artifact_path_candidates(
                &mut payloads,
                &mut seen,
                kind,
                Path::new(&root.root),
                root.path_globs.as_deref(),
            )?;
        }
    }
    Ok(AtReferenceInventoryWire {
        payloads,
        truncated_payloads,
        ..Default::default()
    })
}
#[derive(Debug)]
pub(crate) struct CommitCandidate {
    pub(crate) repository: String,
    pub(crate) abbreviated_sha: String,
    pub(crate) timestamp: i64,
    pub(crate) subject: String,
    pub(crate) body: String,
}
fn append_commit_candidates(
    payloads: &mut Vec<AtReferencePayloadRowWire>,
    seen: &mut BTreeSet<String>,
    context: &ArtifactRefContextWire,
) -> usize {
    let budget = artifact_ref_commit_timeout();
    let mut commits = Vec::new();
    for repository in &context.repositories {
        if repository_is_sdd_sidecar(repository) {
            continue;
        }
        let Some(checkout) = repository.checkout_paths.first() else {
            continue;
        };
        let checkout = Path::new(checkout);
        let git_entry = checkout.join(".git");
        if !checkout.is_dir() || (!git_entry.is_dir() && !git_entry.is_file()) {
            continue;
        }
        let output = match commit_log_output(checkout, budget) {
            Ok(output) => output,
            Err(failure) => {
                // Diagnostics go to stderr because a dropped repository is
                // indistinguishable from one with no commits in the returned
                // inventory, which otherwise makes an empty completion menu
                // impossible to explain.
                eprintln!(
                    "artifact-ref commit inventory: skipping repository {} \
                     at {}: {}",
                    repository.name,
                    checkout.display(),
                    failure.describe(budget)
                );
                continue;
            }
        };
        commits.extend(parse_commit_log(&repository.name, &output));
    }

    sort_commit_candidates(&mut commits);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_secs()).ok())
        .unwrap_or_default();
    append_ranked_commit_candidates(
        payloads,
        seen,
        commits,
        now,
        context.utc_offset_seconds.unwrap_or(0),
    )
}
fn repository_is_sdd_sidecar(
    repository: &crate::ArtifactRefRepositoryWire,
) -> bool {
    repository.kind == ARTIFACT_REF_REPOSITORY_KIND_SIDECAR
}
pub(crate) fn sort_commit_candidates(commits: &mut [CommitCandidate]) {
    commits.sort_by(|left, right| {
        right
            .timestamp
            .cmp(&left.timestamp)
            .then_with(|| left.repository.cmp(&right.repository))
            .then_with(|| left.abbreviated_sha.cmp(&right.abbreviated_sha))
    });
}
pub(crate) fn append_ranked_commit_candidates(
    payloads: &mut Vec<AtReferencePayloadRowWire>,
    seen: &mut BTreeSet<String>,
    commits: Vec<CommitCandidate>,
    now: i64,
    utc_offset_seconds: i32,
) -> usize {
    let mut unique = Vec::new();
    for commit in commits {
        let payload =
            format!("{}@{}", commit.repository, commit.abbreviated_sha);
        if crate::parse_artifact_ref(&format!("commit:{payload}")).is_err() {
            continue;
        }
        if seen.insert(payload.clone()) {
            unique.push((payload, commit));
        }
    }
    let truncated = unique.len().saturating_sub(ARTIFACT_REF_COMMIT_MAX_ROWS);
    unique.truncate(ARTIFACT_REF_COMMIT_MAX_ROWS);
    payloads.extend(unique.into_iter().enumerate().map(
        |(rank, (payload, commit))| AtReferencePayloadRowWire {
            payload,
            label: if commit.subject.is_empty() {
                commit.abbreviated_sha.clone()
            } else {
                commit.subject
            },
            detail: String::new(),
            age: commit_age_label(commit.timestamp, now, utc_offset_seconds),
            scope: commit.repository,
            rank: Some(rank as u32),
            body: commit.body,
        },
    ));
    truncated
}
/// The operating-system error behind a `CommitLogFailure`.
///
/// The failure variants used to discard their `io::Error` through
/// `map_err(|_| ...)`, which left every diagnostic guessing at the cause. The
/// errno is the one piece of evidence that separates, say, descriptor
/// exhaustion from a full filesystem, so it is carried through instead. Only
/// the `Copy` parts of `io::Error` are kept, so the failure type stays cheap
/// and comparable; the human-readable text is reconstructed on demand.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CommitLogIoCause {
    pub(crate) kind: io::ErrorKind,
    pub(crate) raw_os_error: Option<i32>,
}
impl CommitLogIoCause {
    pub(crate) fn new(error: &io::Error) -> Self {
        Self {
            kind: error.kind(),
            raw_os_error: error.raw_os_error(),
        }
    }

    pub(crate) fn describe(self) -> String {
        match self.raw_os_error {
            // `io::Error`'s `Display` for a raw errno renders both the
            // strerror text and the number, e.g.
            // "Too many open files (os error 24)".
            Some(errno) => io::Error::from_raw_os_error(errno).to_string(),
            None => format!("{:?}", self.kind),
        }
    }
}
/// Which scratch-file syscall failed.
///
/// `tempfile::tempfile()` is an `open` under `TMPDIR` and `try_clone()` is a
/// `dup`; both fail with `EMFILE`, so naming the call site is what tells a
/// reader whether the process ran out of descriptors before or after the file
/// existed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ScratchStep {
    Create,
    Clone,
}
impl ScratchStep {
    fn describe(self) -> &'static str {
        match self {
            Self::Create => {
                "could not create a scratch file for `git log` output under \
                 TMPDIR"
            }
            Self::Clone => {
                "could not duplicate the scratch-file descriptor for `git \
                 log` output"
            }
        }
    }
}
/// Why one repository contributed no rows to the commit inventory.
///
/// Every variant used to collapse into a bare `None`, which surfaced as an
/// empty completion menu with no way to tell a genuine absence of commits from
/// a `git` invocation that never produced any output.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CommitLogFailure {
    Scratch(ScratchStep, CommitLogIoCause),
    Spawn(CommitLogIoCause),
    Budget,
    Wait(CommitLogIoCause),
    ExitStatus,
    Read(CommitLogIoCause),
}
impl CommitLogFailure {
    pub(crate) fn describe(self, budget: Duration) -> String {
        match self {
            Self::Scratch(step, cause) => {
                format!("{}: {}", step.describe(), cause.describe())
            }
            Self::Spawn(cause) => format!(
                "could not spawn `git`; check that it is installed and on \
                 PATH: {}",
                cause.describe()
            ),
            Self::Budget => format!(
                "`git log` exceeded its {:?} budget and was killed; raise \
                 {} to allow more time",
                budget, ARTIFACT_REF_COMMIT_TIMEOUT_ENV
            ),
            Self::Wait(cause) => format!(
                "could not wait on the `git log` child process: {}",
                cause.describe()
            ),
            Self::ExitStatus => {
                "`git log` exited with a failure status".to_string()
            }
            Self::Read(cause) => format!(
                "could not read the `git log` output back: {}",
                cause.describe()
            ),
        }
    }
}
pub(crate) fn artifact_ref_commit_timeout() -> Duration {
    parse_commit_timeout(
        std::env::var(ARTIFACT_REF_COMMIT_TIMEOUT_ENV)
            .ok()
            .as_deref(),
    )
    .unwrap_or(ARTIFACT_REF_COMMIT_TIMEOUT_DEFAULT)
}
pub(crate) fn parse_commit_timeout(value: Option<&str>) -> Option<Duration> {
    let seconds = value?.trim().parse::<f64>().ok()?;
    if !seconds.is_finite() || seconds <= 0.0 {
        return None;
    }
    Duration::try_from_secs_f64(seconds).ok()
}
pub(crate) fn commit_log_output(
    checkout: &Path,
    budget: Duration,
) -> Result<Vec<u8>, CommitLogFailure> {
    let mut stdout = tempfile::tempfile().map_err(|error| {
        CommitLogFailure::Scratch(
            ScratchStep::Create,
            CommitLogIoCause::new(&error),
        )
    })?;
    let stdout_writer = stdout.try_clone().map_err(|error| {
        CommitLogFailure::Scratch(
            ScratchStep::Clone,
            CommitLogIoCause::new(&error),
        )
    })?;
    let mut child = Command::new("git")
        .arg("--no-pager")
        .arg("-C")
        .arg(checkout)
        .arg("log")
        .arg("--no-color")
        .arg("-n")
        .arg(ARTIFACT_REF_COMMIT_SCAN_LIMIT.to_string())
        .arg("-z")
        .arg("--format=%H%x1f%h%x1f%at%x1f%s%x1f%b")
        .arg("HEAD")
        .env("GIT_OPTIONAL_LOCKS", "0")
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout_writer))
        .stderr(Stdio::null())
        .spawn()
        .map_err(|error| {
            CommitLogFailure::Spawn(CommitLogIoCause::new(&error))
        })?;
    let started = Instant::now();
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) if started.elapsed() < budget => {
                thread::sleep(ARTIFACT_REF_COMMIT_POLL_INTERVAL);
            }
            outcome => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(match outcome {
                    Err(error) => {
                        CommitLogFailure::Wait(CommitLogIoCause::new(&error))
                    }
                    Ok(_) => CommitLogFailure::Budget,
                });
            }
        }
    };
    if !status.success() {
        return Err(CommitLogFailure::ExitStatus);
    }
    stdout.seek(SeekFrom::Start(0)).map_err(|error| {
        CommitLogFailure::Read(CommitLogIoCause::new(&error))
    })?;
    let mut output = Vec::new();
    stdout.read_to_end(&mut output).map_err(|error| {
        CommitLogFailure::Read(CommitLogIoCause::new(&error))
    })?;
    Ok(output)
}
fn parse_commit_log(repository: &str, output: &[u8]) -> Vec<CommitCandidate> {
    output
        .split(|byte| *byte == 0)
        .filter_map(|record| {
            let record = std::str::from_utf8(record).ok()?;
            let mut fields = record.splitn(5, '\u{1f}');
            let full_sha = fields.next()?.trim();
            let short_sha = fields.next()?.trim();
            let timestamp = fields.next()?.trim().parse::<i64>().ok()?;
            let subject = fields.next()?.trim();
            let body = fields.next()?.trim();
            if full_sha.len() < ARTIFACT_REF_COMMIT_ABBREV
                || !full_sha.bytes().all(|byte| {
                    byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase()
                })
                || !short_sha.bytes().all(|byte| {
                    byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase()
                })
            {
                return None;
            }
            let abbreviated_sha =
                if short_sha.len() >= ARTIFACT_REF_COMMIT_ABBREV {
                    short_sha.to_string()
                } else {
                    full_sha[..ARTIFACT_REF_COMMIT_ABBREV].to_string()
                };
            Some(CommitCandidate {
                repository: repository.to_string(),
                abbreviated_sha,
                timestamp,
                subject: subject.to_string(),
                body: body.to_string(),
            })
        })
        .collect()
}
pub(crate) fn commit_age_label(
    timestamp: i64,
    now: i64,
    utc_offset_seconds: i32,
) -> String {
    if timestamp == 0 {
        return String::new();
    }
    let seconds = now.saturating_sub(timestamp).max(0);
    if seconds < 60 {
        "now".to_string()
    } else if seconds < 3_600 {
        format!("{}m", seconds / 60)
    } else if seconds < 86_400 {
        format!("{}h", seconds / 3_600)
    } else if seconds < 7 * 86_400 {
        format!("{}d", seconds / 86_400)
    } else {
        // Shift by the caller's configured-tz offset before taking the date so
        // the displayed calendar day matches their wall clock, not UTC's.
        DateTime::<Utc>::from_timestamp(
            timestamp + i64::from(utc_offset_seconds),
            0,
        )
        .map(|datetime| datetime.date_naive().format("%Y-%m-%d").to_string())
        .unwrap_or_default()
    }
}
fn known_artifact_ref_kinds(context: &ArtifactRefContextWire) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut kinds = Vec::new();
    for kind in crate::editor::at_reference::BUILTIN_ARTIFACT_REF_KINDS
        .iter()
        .copied()
        .chain(context.document_roots.iter().map(|root| root.kind.as_str()))
    {
        if !kind.is_empty() && seen.insert(kind.to_string()) {
            kinds.push(kind.to_string());
        }
    }
    kinds
}
/// Collect every scanned relative path under `root` as a payload row.
fn append_artifact_path_candidates(
    payloads: &mut Vec<AtReferencePayloadRowWire>,
    seen: &mut BTreeSet<String>,
    kind: &str,
    root: &Path,
    path_globs: Option<&[String]>,
) -> Result<usize, ArtifactRefError> {
    let scan = bounded_relative_files(root);
    let filtered = crate::artifact_ref::filter_artifact_ref_path_payloads(
        kind,
        path_globs,
        &scan.files,
    )?;
    for path in filtered.allowed {
        if !seen.insert(path.clone()) {
            continue;
        }
        payloads.push(AtReferencePayloadRowWire {
            label: artifact_path_title(kind, root, &path),
            payload: path,
            detail: format!("{kind} · {}", root.display()),
            age: String::new(),
            scope: String::new(),
            rank: None,
            body: String::new(),
        });
    }
    Ok(scan.truncated)
}
fn append_bead_page_candidates(
    payloads: &mut Vec<AtReferencePayloadRowWire>,
    seen: &mut BTreeSet<String>,
    context: &ArtifactRefContextWire,
) -> usize {
    let mut truncated = 0usize;
    for store in &context.bead_stores {
        let pages_root = Path::new(&store.root).join("pages");
        let scan = bounded_relative_files(&pages_root);
        truncated += scan.truncated;
        for path in scan.files {
            let Some(id) = bead_id_from_page_relative_path(&path) else {
                continue;
            };
            if !seen.insert(id.clone()) {
                continue;
            }
            let page_path = pages_root.join(&path);
            payloads.push(AtReferencePayloadRowWire {
                label: bead_page_title(&page_path, &id),
                payload: id,
                detail: format!("bead · {}", store.project),
                age: String::new(),
                scope: String::new(),
                rank: None,
                body: String::new(),
            });
        }
    }
    truncated
}
/// Collect all published agent pages. Matching and ranking happen in the shared
/// at-reference menu after this inventory is cached.
fn append_agent_page_candidates(
    payloads: &mut Vec<AtReferencePayloadRowWire>,
    seen: &mut BTreeSet<String>,
    context: &ArtifactRefContextWire,
) {
    for root in &context.agent_roots {
        let agents_root = Path::new(&root.root).join("agents");
        if !agents_root.is_dir() {
            continue;
        }
        let Ok(read_dir) = fs::read_dir(&agents_root) else {
            continue;
        };
        let mut entries = read_dir.filter_map(Result::ok).collect::<Vec<_>>();
        entries.sort_by_key(|entry| entry.file_name());
        for entry in entries {
            let Ok(file_type) = entry.file_type() else {
                continue;
            };
            if !file_type.is_dir() || !entry.path().join("README.md").is_file()
            {
                continue;
            }
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.is_empty() {
                continue;
            }
            if !seen.insert(name.clone()) {
                continue;
            }
            payloads.push(AtReferencePayloadRowWire {
                label: agent_short_name(&name),
                payload: name,
                detail: format!("agent · {}", root.project),
                age: String::new(),
                scope: String::new(),
                rank: None,
                body: String::new(),
            });
        }
    }
}
fn bead_id_from_page_relative_path(path: &str) -> Option<String> {
    let mut parts = path.split('/');
    let lineage = parts.next()?;
    let file_name = parts.next()?;
    if parts.next().is_some() || lineage.is_empty() {
        return None;
    }
    if file_name == "README.md" {
        return Some(lineage.to_string());
    }
    file_name
        .strip_suffix(".md")
        .filter(|id| !id.is_empty())
        .map(str::to_string)
}
/// Collect all indexed artifact files. Matching and ranking happen in the
/// shared at-reference menu after this inventory is cached.
fn append_artifact_index_candidates(
    payloads: &mut Vec<AtReferencePayloadRowWire>,
    seen: &mut BTreeSet<String>,
    context: &ArtifactRefContextWire,
) {
    let Some(index_path) = context.artifact_index_path.as_deref() else {
        return;
    };
    let Ok(mut entries) = read_artifact_file_index(Path::new(index_path))
    else {
        return;
    };
    entries.sort_by(|left, right| {
        left.id
            .cmp(&right.id)
            .then_with(|| left.path.cmp(&right.path))
    });
    for entry in entries {
        let id = entry.id.clone();
        if seen.insert(id.clone()) {
            let display_path = entry
                .path
                .as_deref()
                .or(entry.vcs_relpath.as_deref())
                .unwrap_or(&id);
            let detail = if artifact_file_is_vcs_backed(&entry) {
                format!(
                    "file · {}@{}:{}",
                    entry.vcs_repo.as_deref().unwrap_or_default(),
                    entry.vcs_sha.as_deref().unwrap_or_default(),
                    entry.vcs_relpath.as_deref().unwrap_or_default(),
                )
            } else {
                format!("file · {display_path}")
            };
            payloads.push(AtReferencePayloadRowWire {
                label: path_basename(display_path)
                    .unwrap_or_else(|| id.clone()),
                payload: id,
                detail,
                age: String::new(),
                scope: String::new(),
                rank: None,
                body: String::new(),
            });
        }
    }
}
fn artifact_path_title(kind: &str, root: &Path, payload: &str) -> String {
    if kind == "chat" {
        return path_basename(payload).unwrap_or_else(|| payload.to_string());
    }
    document_frontmatter_title(&root.join(payload))
        .or_else(|| path_basename(payload))
        .unwrap_or_else(|| payload.to_string())
}
fn document_frontmatter_title(path: &Path) -> Option<String> {
    let content = fs::read_to_string(path).ok()?;
    let (frontmatter, _) = split_frontmatter(&content);
    let frontmatter = frontmatter?;
    let value = serde_yaml::from_str::<serde_yaml::Value>(&frontmatter).ok()?;
    let mapping = value.as_mapping()?;
    let title = mapping.get(serde_yaml::Value::String("title".to_string()))?;
    nonempty_title(title.as_str()?)
}
fn bead_page_title(path: &Path, id: &str) -> String {
    fs::read_to_string(path)
        .ok()
        .and_then(|content| bead_page_title_from_content(&content))
        .unwrap_or_else(|| id.to_string())
}
fn bead_page_title_from_content(content: &str) -> Option<String> {
    let heading = content.lines().next()?.strip_prefix("# Bead: ")?;
    let (_, title) = heading.split_once(" \u{2014} ")?;
    nonempty_title(title)
}
fn agent_short_name(name: &str) -> String {
    name.rsplit('.')
        .next()
        .and_then(nonempty_title)
        .unwrap_or_else(|| name.to_string())
}
fn path_basename(path: &str) -> Option<String> {
    path.rsplit(['/', '\\']).next().and_then(nonempty_title)
}
fn nonempty_title(value: &str) -> Option<String> {
    let value = value.trim();
    (!value.is_empty()).then(|| value.to_string())
}
struct BoundedRelativeFiles {
    files: Vec<String>,
    /// Number of payloads known to have been omitted. A positive value is a
    /// lower bound because the scan stops as soon as a configured bound bites.
    truncated: usize,
}
fn bounded_relative_files(root: &Path) -> BoundedRelativeFiles {
    if !root.is_dir() {
        return BoundedRelativeFiles {
            files: Vec::new(),
            truncated: 0,
        };
    }
    let mut pending = vec![(root.to_path_buf(), 0usize)];
    let mut visited = 0usize;
    let mut files = Vec::new();
    let mut truncated = 0usize;
    while let Some((directory, depth)) = pending.pop() {
        if depth > ARTIFACT_REF_MAX_DEPTH {
            truncated = truncated.saturating_add(1);
            break;
        }
        if visited >= ARTIFACT_REF_MAX_VISITED
            || files.len() >= ARTIFACT_REF_MAX_SCAN_RESULTS
        {
            truncated = truncated.saturating_add(1);
            break;
        }
        let Ok(read_dir) = fs::read_dir(&directory) else {
            continue;
        };
        let mut entries = read_dir.filter_map(Result::ok).collect::<Vec<_>>();
        entries.sort_by_key(|entry| entry.file_name());
        let entry_count = entries.len();
        let mut directories = Vec::<PathBuf>::new();
        for (entry_index, entry) in entries.into_iter().enumerate() {
            visited += 1;
            if visited > ARTIFACT_REF_MAX_VISITED {
                truncated = truncated.saturating_add(1);
                break;
            }
            let Ok(file_type) = entry.file_type() else {
                continue;
            };
            if file_type.is_dir() {
                if depth < ARTIFACT_REF_MAX_DEPTH {
                    directories.push(entry.path());
                } else {
                    truncated = truncated.saturating_add(1);
                }
            } else if file_type.is_file() {
                let path = entry.path();
                let Ok(relative) = path.strip_prefix(root) else {
                    continue;
                };
                let payload = relative
                    .components()
                    .map(|component| component.as_os_str().to_string_lossy())
                    .collect::<Vec<_>>()
                    .join("/");
                if !payload.is_empty() {
                    files.push(payload);
                }
                if files.len() >= ARTIFACT_REF_MAX_SCAN_RESULTS {
                    if entry_index + 1 < entry_count
                        || !pending.is_empty()
                        || !directories.is_empty()
                    {
                        truncated = truncated.saturating_add(1);
                    }
                    break;
                }
            }
        }
        if truncated > 0 {
            break;
        }
        for directory in directories.into_iter().rev() {
            pending.push((directory, depth + 1));
        }
    }
    files.sort();
    BoundedRelativeFiles { files, truncated }
}
fn artifact_ref_candidate(
    name: String,
    insertion: String,
    detail: String,
    replacement_range: Option<EditorRange>,
    kind: &str,
    payload_kind: Option<&str>,
) -> CompletionCandidate {
    let display = if kind == "artifact_payload" && !name.is_empty() {
        name.clone()
    } else {
        insertion.clone()
    };
    CompletionCandidate {
        display,
        insertion: insertion.clone(),
        detail: Some(detail),
        documentation: None,
        is_dir: false,
        name,
        replacement: replacement_range.map(|range| EditorTextEdit {
            range,
            new_text: insertion,
        }),
        additional_edits: Vec::new(),
        kind: kind.to_string(),
        project: String::new(),
        status: payload_kind.unwrap_or_default().to_string(),
    }
}
fn empty_artifact_ref_completion_list() -> CompletionList {
    CompletionList {
        candidates: Vec::new(),
        shared_extension: String::new(),
    }
}
