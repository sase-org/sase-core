use std::collections::BTreeSet;
use std::fs;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use chrono::{DateTime, Utc};
use regex::Regex;
use std::sync::OnceLock;

use crate::artifact_file::{
    artifact_file_is_vcs_backed, read_artifact_file_index,
};
use crate::plan::read::split_frontmatter;
use crate::{
    ArtifactRefContextWire, ArtifactRefError, EditorSnippetEntryWire,
    EditorXpromptCatalogEntryWire,
};

use super::at_reference::{
    build_at_reference_menu, detect_at_reference_context,
    is_builtin_at_reference_kind, AtReferenceContextWire,
    AtReferenceInventoryWire, AtReferenceKindRowWire,
    AtReferencePayloadRowWire, AtReferenceStage,
};
use super::directive::{
    build_bead_completion_candidates, build_directive_keyword_candidates,
    build_directive_static_value_candidates,
    detect_directive_context_at_position, directive_allows_keywords,
    directive_argument_candidates, directive_metadata,
};
use super::placeholder::detect_placeholder_context_at_position;
use super::token::{
    extract_token_at_position, is_path_like_token, is_slash_skill_like_token,
    is_snippet_trigger_token, is_xprompt_like_token, vcs_project_trigger_token,
    DocumentSnapshot,
};
use super::wire::{
    AgentCompletionEntry, ArtifactRefCompletionMode,
    ArtifactRefCompletionTrigger, CompletionCandidate, CompletionContext,
    CompletionContextKind, CompletionList, DirectiveClauseKind,
    DirectiveCompletionInventories, DirectiveSyntaxForm, DirectiveValueRole,
    EditorPosition, EditorRange, EditorTextEdit, TokenInfo, VcsNamespaceEntry,
    VcsProjectEntry, VcsRefTrigger, VcsRepoEntry, VcsRepoTrigger,
    XpromptAssistEntry, XpromptInputHint,
};

const ARTIFACT_REF_MAX_DEPTH: usize = 8;
const ARTIFACT_REF_MAX_VISITED: usize = 20_000;
const ARTIFACT_REF_MAX_SCAN_RESULTS: usize = 5_000;
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
const ARTIFACT_REF_REPOSITORY_KIND_SIDECAR: &str = "sidecar";

pub fn assist_entries_from_catalog(
    entries: &[EditorXpromptCatalogEntryWire],
) -> Vec<XpromptAssistEntry> {
    entries
        .iter()
        .map(|entry| {
            let reference_prefix =
                entry.reference_prefix.as_deref().unwrap_or("#").to_string();
            let insertion = entry
                .insertion
                .clone()
                .unwrap_or_else(|| format!("{reference_prefix}{}", entry.name));
            XpromptAssistEntry {
                name: entry.name.clone(),
                display_label: entry.display_label.clone(),
                insertion,
                reference_prefix,
                kind: entry.kind.clone(),
                source_bucket: entry.source_bucket.clone(),
                project: entry.project.clone(),
                tags: entry.tags.clone(),
                input_signature: entry.input_signature.clone(),
                inputs: entry
                    .inputs
                    .iter()
                    .map(|input| XpromptInputHint {
                        name: input.name.clone(),
                        r#type: input.r#type.clone(),
                        description: input.description.clone(),
                        required: input.required,
                        default_display: input.default_display.clone(),
                        position: input.position,
                        repeatable: input.repeatable,
                    })
                    .collect(),
                content_preview: entry.content_preview.clone(),
                description: entry.description.clone(),
                source_path_display: entry.source_path_display.clone(),
                definition_path: entry.definition_path.clone(),
                definition_range: entry.definition_range,
                is_skill: entry.is_skill,
                skill_name: entry.skill_name.clone(),
                memory_type: entry.memory_type,
            }
        })
        .collect()
}

pub fn classify_completion_context(
    document: &DocumentSnapshot,
    position: EditorPosition,
    entries: &[XpromptAssistEntry],
) -> Option<CompletionContext> {
    classify_completion_context_with_workflows(document, position, entries, &[])
}

pub fn classify_completion_context_with_workflows(
    document: &DocumentSnapshot,
    position: EditorPosition,
    entries: &[XpromptAssistEntry],
    known_workflow_names: &[String],
) -> Option<CompletionContext> {
    classify_completion_context_with_artifacts_and_workflows(
        document,
        position,
        entries,
        known_workflow_names,
        None,
    )
}

pub fn classify_completion_context_with_artifacts_and_workflows(
    document: &DocumentSnapshot,
    position: EditorPosition,
    entries: &[XpromptAssistEntry],
    known_workflow_names: &[String],
    artifact_context: Option<&ArtifactRefContextWire>,
) -> Option<CompletionContext> {
    if let Some(placeholder) =
        detect_placeholder_context_at_position(document, position)
    {
        return Some(CompletionContext {
            kind: CompletionContextKind::Placeholder,
            token: Some(TokenInfo {
                text: placeholder.prefix,
                range: placeholder.prefix_range,
                byte_start: placeholder.prefix_byte_start,
                byte_end: placeholder.cursor_byte,
            }),
            active_xprompt: None,
            active_input: None,
            directive_name: None,
            selected_values: Vec::new(),
            directive: None,
            vcs_repo: None,
            vcs_ref: None,
            artifact_ref: None,
            replacement_range: placeholder.replacement_range,
        });
    }
    if let Some(context) = artifact_context.and_then(|context| {
        detect_artifact_ref_context_at_position(document, position, context)
    }) {
        return Some(context);
    }
    if let Some(context) = detect_vcs_repo_context_at_position(
        document,
        position,
        known_workflow_names,
    ) {
        return Some(context);
    }
    if let Some(context) = detect_vcs_ref_context_at_position(
        document,
        position,
        known_workflow_names,
    ) {
        return Some(context);
    }
    if let Some(context) =
        detect_xprompt_arg_completion_at_position(document, position, entries)
    {
        return Some(context);
    }
    if let Some(context) =
        detect_directive_context_at_position(document, position)
    {
        return Some(context);
    }
    if let Some(context) =
        detect_vcs_project_context_at_position(document, position)
    {
        return Some(context);
    }

    let token = extract_token_at_position(document, position);
    match token {
        None => {
            let byte = document.position_to_byte_offset(position)?;
            if byte > 0 && document.text()[..byte].ends_with('+') {
                return None;
            }
            Some(CompletionContext {
                kind: CompletionContextKind::FileHistory,
                token: None,
                active_xprompt: None,
                active_input: None,
                directive_name: None,
                selected_values: Vec::new(),
                directive: None,
                vcs_repo: None,
                vcs_ref: None,
                artifact_ref: None,
                replacement_range: document.byte_range_to_range(byte, byte)?,
            })
        }
        Some(token) if is_xprompt_like_token(&token.text) => {
            Some(context_for_token(CompletionContextKind::Xprompt, token))
        }
        Some(token) if is_slash_skill_like_token(&token.text) => {
            Some(context_for_token(CompletionContextKind::SlashSkill, token))
        }
        Some(token) if is_path_like_token(&token.text) => {
            Some(context_for_token(CompletionContextKind::FilePath, token))
        }
        Some(token) if is_snippet_trigger_token(&token.text) => Some(
            context_for_token(CompletionContextKind::SnippetTrigger, token),
        ),
        _ => None,
    }
}

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
        super::at_reference::AtReferencePathQueryWire {
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
struct CommitCandidate {
    repository: String,
    abbreviated_sha: String,
    timestamp: i64,
    subject: String,
    body: String,
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
    append_ranked_commit_candidates(payloads, seen, commits, now)
}

fn repository_is_sdd_sidecar(
    repository: &crate::ArtifactRefRepositoryWire,
) -> bool {
    repository.kind == ARTIFACT_REF_REPOSITORY_KIND_SIDECAR
}

fn sort_commit_candidates(commits: &mut [CommitCandidate]) {
    commits.sort_by(|left, right| {
        right
            .timestamp
            .cmp(&left.timestamp)
            .then_with(|| left.repository.cmp(&right.repository))
            .then_with(|| left.abbreviated_sha.cmp(&right.abbreviated_sha))
    });
}

fn append_ranked_commit_candidates(
    payloads: &mut Vec<AtReferencePayloadRowWire>,
    seen: &mut BTreeSet<String>,
    commits: Vec<CommitCandidate>,
    now: i64,
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
            age: commit_age_label(commit.timestamp, now),
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
struct CommitLogIoCause {
    kind: io::ErrorKind,
    raw_os_error: Option<i32>,
}

impl CommitLogIoCause {
    fn new(error: &io::Error) -> Self {
        Self {
            kind: error.kind(),
            raw_os_error: error.raw_os_error(),
        }
    }

    fn describe(self) -> String {
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
enum ScratchStep {
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
enum CommitLogFailure {
    Scratch(ScratchStep, CommitLogIoCause),
    Spawn(CommitLogIoCause),
    Budget,
    Wait(CommitLogIoCause),
    ExitStatus,
    Read(CommitLogIoCause),
}

impl CommitLogFailure {
    fn describe(self, budget: Duration) -> String {
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

fn artifact_ref_commit_timeout() -> Duration {
    parse_commit_timeout(
        std::env::var(ARTIFACT_REF_COMMIT_TIMEOUT_ENV)
            .ok()
            .as_deref(),
    )
    .unwrap_or(ARTIFACT_REF_COMMIT_TIMEOUT_DEFAULT)
}

fn parse_commit_timeout(value: Option<&str>) -> Option<Duration> {
    let seconds = value?.trim().parse::<f64>().ok()?;
    if !seconds.is_finite() || seconds <= 0.0 {
        return None;
    }
    Duration::try_from_secs_f64(seconds).ok()
}

fn commit_log_output(
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

fn commit_age_label(timestamp: i64, now: i64) -> String {
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
        DateTime::<Utc>::from_timestamp(timestamp, 0)
            .map(|datetime| {
                datetime.date_naive().format("%Y-%m-%d").to_string()
            })
            .unwrap_or_default()
    }
}

fn known_artifact_ref_kinds(context: &ArtifactRefContextWire) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut kinds = Vec::new();
    for kind in super::at_reference::BUILTIN_ARTIFACT_REF_KINDS
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

pub fn build_xprompt_completion_candidates(
    token: &str,
    replacement_range: Option<EditorRange>,
    entries: &[XpromptAssistEntry],
) -> CompletionList {
    let slash_skill = token.starts_with('/');
    let standalone_only = token.starts_with("#!");
    let partial = if slash_skill {
        token.strip_prefix('/').unwrap_or_default()
    } else if standalone_only {
        token.strip_prefix("#!").unwrap_or_default()
    } else {
        token.strip_prefix('#').unwrap_or(token)
    };
    let partial_lower = partial.to_lowercase();
    let mut candidates = Vec::new();

    for entry in entries {
        // Slash completion is keyed on the provider skill name (`/foo`) while
        // `#` completion is keyed on the xprompt reference (`#skill/foo`).
        let match_name = if slash_skill {
            let Some(skill_name) = entry.skill_name.as_deref() else {
                continue;
            };
            skill_name
        } else {
            entry.name.as_str()
        };
        if slash_skill && !entry.is_skill {
            continue;
        }
        if standalone_only && entry.reference_prefix != "#!" {
            continue;
        }
        if !match_name.to_lowercase().starts_with(&partial_lower) {
            continue;
        }
        let insertion = if slash_skill {
            format!("/{match_name}")
        } else {
            entry.insertion.clone()
        };
        candidates.push(CompletionCandidate {
            display: insertion.clone(),
            insertion: insertion.clone(),
            detail: entry
                .input_signature
                .clone()
                .or_else(|| entry.kind.clone()),
            documentation: entry
                .description
                .clone()
                .or_else(|| entry.content_preview.clone()),
            is_dir: false,
            name: match_name.to_string(),
            replacement: replacement_range.map(|range| EditorTextEdit {
                range,
                new_text: insertion,
            }),
            additional_edits: Vec::new(),
            kind: String::new(),
            project: String::new(),
            status: String::new(),
        });
    }
    candidates.sort_by_key(|candidate| candidate.name.to_lowercase());
    CompletionList {
        shared_extension: shared_extension(&candidates, partial),
        candidates,
    }
}

pub fn build_xprompt_arg_name_candidates(
    entry: &XpromptAssistEntry,
    used_arg_names: &BTreeSet<String>,
    token: &str,
    replacement_range: Option<EditorRange>,
) -> CompletionList {
    let partial = token.to_lowercase();
    let mut candidates = Vec::new();
    for input in &entry.inputs {
        if used_arg_names.contains(&input.name) {
            continue;
        }
        if !input.name.to_lowercase().starts_with(&partial) {
            continue;
        }
        let insertion = format!("{}=", input.name);
        candidates.push(CompletionCandidate {
            display: insertion.clone(),
            insertion: insertion.clone(),
            detail: Some(input_label(input)),
            documentation: input_documentation(input),
            is_dir: false,
            name: input.name.clone(),
            replacement: replacement_range.map(|range| EditorTextEdit {
                range,
                new_text: insertion,
            }),
            additional_edits: Vec::new(),
            kind: String::new(),
            project: String::new(),
            status: String::new(),
        });
    }
    CompletionList {
        candidates,
        shared_extension: String::new(),
    }
}

pub fn build_agent_completion_candidates(
    token: &str,
    replacement_range: Option<EditorRange>,
    entries: &[AgentCompletionEntry],
    selected_values: &[String],
) -> CompletionList {
    if token.contains('=') {
        return CompletionList {
            candidates: Vec::new(),
            shared_extension: String::new(),
        };
    }

    let partial = token.to_lowercase();
    let selected = selected_values
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let mut seen = BTreeSet::new();
    let mut candidates = Vec::new();
    let mut ordered_entries = entries.iter().collect::<Vec<_>>();
    ordered_entries
        .sort_by_key(|entry| agent_kind_rank(agent_entry_kind(entry)));
    for entry in ordered_entries {
        let kind = agent_entry_kind(entry);
        let insertion = entry.name.trim();
        if insertion.is_empty()
            || selected.contains(insertion)
            || !seen.insert(insertion.to_string())
            || !agent_entry_matches(kind, insertion, &partial)
        {
            continue;
        }
        let filter_name = if kind == "tribe" && !partial.starts_with('@') {
            insertion.strip_prefix('@').unwrap_or(insertion).to_string()
        } else {
            insertion.to_string()
        };
        let detail = agent_entry_detail(entry, kind);
        candidates.push(CompletionCandidate {
            display: insertion.to_string(),
            insertion: insertion.to_string(),
            detail,
            documentation: (!entry.documentation.is_empty())
                .then(|| entry.documentation.clone()),
            is_dir: false,
            name: filter_name,
            replacement: replacement_range.map(|range| EditorTextEdit {
                range,
                new_text: insertion.to_string(),
            }),
            additional_edits: Vec::new(),
            kind: kind.to_string(),
            project: entry.project.clone(),
            status: entry.status.clone(),
        });
    }
    CompletionList {
        shared_extension: shared_extension(&candidates, token),
        candidates,
    }
}

pub fn build_wait_completion_candidates(
    token: &str,
    replacement_range: Option<EditorRange>,
    entries: &[AgentCompletionEntry],
    selected_values: &[String],
) -> CompletionList {
    build_wait_completion_candidates_for_form(
        token,
        replacement_range,
        entries,
        selected_values,
        DirectiveSyntaxForm::Parenthesized,
    )
}

pub fn build_wait_completion_candidates_for_form(
    token: &str,
    replacement_range: Option<EditorRange>,
    entries: &[AgentCompletionEntry],
    selected_values: &[String],
    syntax_form: DirectiveSyntaxForm,
) -> CompletionList {
    let mut candidates = Vec::new();
    let wait = directive_metadata("wait");
    if !token.contains('=')
        && wait.is_some_and(|metadata| {
            directive_allows_keywords(metadata, syntax_form)
        })
    {
        let selected_keywords: Vec<String> = selected_values
            .iter()
            .filter(|value| value.contains('='))
            .cloned()
            .collect();
        if let Some(metadata) = wait {
            candidates.extend(
                build_directive_keyword_candidates(
                    metadata,
                    token,
                    &selected_keywords,
                    replacement_range,
                )
                .candidates,
            );
        }
    }
    candidates.extend(
        build_agent_completion_candidates(
            token,
            replacement_range,
            entries,
            selected_values,
        )
        .candidates,
    );
    CompletionList {
        shared_extension: shared_extension(&candidates, token),
        candidates,
    }
}

pub fn build_identity_target_candidates(
    token: &str,
    replacement_range: Option<EditorRange>,
    entries: &[AgentCompletionEntry],
    required_kind: &str,
    selected_values: &[String],
) -> CompletionList {
    let filtered: Vec<AgentCompletionEntry> = entries
        .iter()
        .filter(|entry| agent_entry_kind(entry) == required_kind)
        .cloned()
        .collect();
    build_agent_completion_candidates(
        token,
        replacement_range,
        &filtered,
        selected_values,
    )
}

pub fn build_directive_clause_candidates(
    context: &CompletionContext,
    inventories: &DirectiveCompletionInventories,
) -> CompletionList {
    let token = context
        .token
        .as_ref()
        .map(|token| token.text.as_str())
        .unwrap_or_default();
    let replacement = Some(context.replacement_range);
    match context.kind {
        CompletionContextKind::DirectiveName => {
            return super::directive::build_directive_completion_candidates(
                token,
            );
        }
        CompletionContextKind::DirectiveArgumentKeyword => {
            if let Some(metadata) = context
                .directive_name
                .as_deref()
                .and_then(directive_metadata)
            {
                let mut list = build_directive_keyword_candidates(
                    metadata,
                    token,
                    context.selected_keywords(),
                    replacement,
                );
                if metadata.dynamic_keyword_role
                    == Some(DirectiveValueRole::ModelAliasKey)
                {
                    list.candidates.extend(model_alias_key_candidates(
                        token,
                        inventories,
                        context.selected_keywords(),
                        replacement,
                    ));
                }
                return list;
            }
        }
        CompletionContextKind::DirectiveArgumentValue => {
            return build_directive_value_candidates(
                context,
                inventories,
                token,
                replacement,
            );
        }
        CompletionContextKind::DirectiveArgument => {}
        _ => {
            return CompletionList {
                candidates: Vec::new(),
                shared_extension: String::new(),
            };
        }
    }

    let Some(name) = context.directive_name.as_deref() else {
        return CompletionList {
            candidates: Vec::new(),
            shared_extension: String::new(),
        };
    };
    if name == "wait" {
        return build_wait_completion_candidates_for_form(
            token,
            replacement,
            &inventories.agents,
            &context.selected_values,
            context
                .syntax_form()
                .unwrap_or(DirectiveSyntaxForm::Parenthesized),
        );
    }
    if name == "model" {
        let mut candidates =
            model_value_candidates(token, inventories, replacement);
        if context.syntax_form() == Some(DirectiveSyntaxForm::Parenthesized)
            && context.clause_kind() == Some(DirectiveClauseKind::Positional)
        {
            candidates.extend(model_alias_key_candidates(
                token,
                inventories,
                context.selected_keywords(),
                replacement,
            ));
        }
        return CompletionList {
            shared_extension: shared_extension(&candidates, token),
            candidates,
        };
    }
    context
        .directive_name
        .as_deref()
        .map(directive_argument_candidates)
        .unwrap_or_else(|| CompletionList {
            candidates: Vec::new(),
            shared_extension: String::new(),
        })
}

fn build_directive_value_candidates(
    context: &CompletionContext,
    inventories: &DirectiveCompletionInventories,
    token: &str,
    replacement: Option<EditorRange>,
) -> CompletionList {
    match context.value_role() {
        Some(DirectiveValueRole::Bead) => build_bead_completion_candidates(
            &inventories.beads,
            token,
            &context.selected_values,
            &inventories.excluded_bead_ids,
            replacement,
        ),
        Some(DirectiveValueRole::Agent) => build_agent_completion_candidates(
            token,
            replacement,
            &inventories.agents,
            &context.selected_values,
        ),
        Some(DirectiveValueRole::Clan) => build_identity_target_candidates(
            token,
            replacement,
            &inventories.agents,
            "clan",
            &context.selected_values,
        ),
        Some(DirectiveValueRole::Family) => build_identity_target_candidates(
            token,
            replacement,
            &inventories.agents,
            "family",
            &context.selected_values,
        ),
        Some(DirectiveValueRole::Tribe) => build_identity_target_candidates(
            token,
            replacement,
            &inventories.agents,
            "tribe",
            &context.selected_values,
        ),
        Some(DirectiveValueRole::Model) => CompletionList {
            candidates: model_value_candidates(token, inventories, replacement),
            shared_extension: String::new(),
        },
        _ => {
            let Some(metadata) = context
                .directive_name
                .as_deref()
                .and_then(directive_metadata)
            else {
                return CompletionList {
                    candidates: Vec::new(),
                    shared_extension: String::new(),
                };
            };
            let values = context
                .active_keyword()
                .and_then(|name| {
                    metadata
                        .keywords
                        .iter()
                        .find(|keyword| keyword.name == name)
                        .map(|keyword| keyword.suggested_values)
                })
                .unwrap_or(metadata.positional_suggestions);
            build_directive_static_value_candidates(values, token, replacement)
        }
    }
}

fn model_value_candidates(
    token: &str,
    inventories: &DirectiveCompletionInventories,
    replacement: Option<EditorRange>,
) -> Vec<CompletionCandidate> {
    let partial = token.to_lowercase();
    inventories
        .models
        .iter()
        .filter(|entry| {
            entry.value.to_lowercase().starts_with(&partial)
                || entry.display.to_lowercase().starts_with(&partial)
        })
        .map(|entry| {
            let display = if entry.display.is_empty() {
                entry.value.clone()
            } else {
                entry.display.clone()
            };
            CompletionCandidate {
                display,
                insertion: entry.value.clone(),
                detail: (!entry.detail.is_empty())
                    .then(|| entry.detail.clone()),
                documentation: (!entry.documentation.is_empty())
                    .then(|| entry.documentation.clone()),
                is_dir: false,
                name: entry.value.clone(),
                replacement: replacement.map(|range| EditorTextEdit {
                    range,
                    new_text: entry.value.clone(),
                }),
                additional_edits: Vec::new(),
                kind: "model".to_string(),
                project: String::new(),
                status: String::new(),
            }
        })
        .collect()
}

fn model_alias_key_candidates(
    token: &str,
    inventories: &DirectiveCompletionInventories,
    selected_keywords: &[String],
    replacement: Option<EditorRange>,
) -> Vec<CompletionCandidate> {
    let partial = token.to_lowercase();
    let selected = selected_keywords
        .iter()
        .map(|value| {
            value
                .split_once('=')
                .map(|(name, _)| name.trim())
                .unwrap_or(value.as_str())
                .to_lowercase()
        })
        .collect::<Vec<_>>();
    inventories
        .model_alias_keys
        .iter()
        .filter(|entry| {
            let name = entry.name.to_lowercase();
            !selected.iter().any(|value| value == &name)
                && (name.starts_with(&partial)
                    || format!("{}=", name).starts_with(&partial))
        })
        .map(|entry| {
            let insertion = format!("{}=", entry.name);
            CompletionCandidate {
                display: insertion.clone(),
                insertion: insertion.clone(),
                detail: None,
                documentation: (!entry.documentation.is_empty())
                    .then(|| entry.documentation.clone()),
                is_dir: false,
                name: insertion.clone(),
                replacement: replacement.map(|range| EditorTextEdit {
                    range,
                    new_text: insertion,
                }),
                additional_edits: Vec::new(),
                kind: "keyword".to_string(),
                project: String::new(),
                status: String::new(),
            }
        })
        .collect()
}

fn agent_entry_kind(entry: &AgentCompletionEntry) -> &str {
    match entry.kind.as_str() {
        "family" => "family",
        "clan" => "clan",
        "tribe" => "tribe",
        _ => "agent",
    }
}

fn agent_kind_rank(kind: &str) -> u8 {
    match kind {
        "keyword" => 0,
        "tribe" => 1,
        "clan" => 2,
        "family" => 3,
        _ => 4,
    }
}

fn agent_entry_matches(kind: &str, insertion: &str, partial: &str) -> bool {
    let insertion = insertion.to_lowercase();
    if kind != "tribe" {
        return insertion.starts_with(partial);
    }
    let bare = insertion.strip_prefix('@').unwrap_or(&insertion);
    insertion.starts_with(partial) || bare.starts_with(partial)
}

fn agent_entry_detail(
    entry: &AgentCompletionEntry,
    kind: &str,
) -> Option<String> {
    if !entry.detail.is_empty() {
        return Some(entry.detail.clone());
    }
    if kind != "agent" {
        return (entry.member_count > 0).then(|| {
            let suffix = if entry.member_count == 1 {
                "member"
            } else {
                "members"
            };
            format!("{kind} · {} {suffix}", entry.member_count)
        });
    }
    match (entry.status.is_empty(), entry.project.is_empty()) {
        (false, false) => Some(format!("{} · {}", entry.status, entry.project)),
        (false, true) => Some(entry.status.clone()),
        (true, false) => Some(entry.project.clone()),
        (true, true) => None,
    }
}

pub fn build_snippet_completion_candidates(
    token: &str,
    replacement_range: Option<EditorRange>,
    entries: &[EditorSnippetEntryWire],
) -> CompletionList {
    let partial_lower = token.to_lowercase();
    let mut candidates = Vec::new();
    for entry in entries {
        if !entry.trigger.to_lowercase().starts_with(&partial_lower) {
            continue;
        }
        candidates.push(CompletionCandidate {
            display: entry.trigger.clone(),
            insertion: entry.template.clone(),
            detail: Some(entry.source.clone()),
            documentation: snippet_documentation(entry),
            is_dir: false,
            name: entry.trigger.clone(),
            replacement: replacement_range.map(|range| EditorTextEdit {
                range,
                new_text: entry.template.clone(),
            }),
            additional_edits: Vec::new(),
            kind: String::new(),
            project: String::new(),
            status: String::new(),
        });
    }
    candidates.sort_by_key(|candidate| candidate.name.to_lowercase());
    CompletionList {
        shared_extension: shared_extension(&candidates, token),
        candidates,
    }
}

// --- vcs_repo (`#gh:owner/`) completion -----------------------------------

pub fn build_vcs_repo_completion_candidates(
    document: &DocumentSnapshot,
    context: &CompletionContext,
    entries: &[VcsRepoEntry],
) -> CompletionList {
    let Some(trigger) = context.vcs_repo.as_ref() else {
        return CompletionList {
            candidates: Vec::new(),
            shared_extension: String::new(),
        };
    };
    let Some(replacement_range) =
        document.byte_range_to_range(trigger.ref_start, trigger.ref_end)
    else {
        return CompletionList {
            candidates: Vec::new(),
            shared_extension: String::new(),
        };
    };

    let candidates = entries
        .iter()
        .map(|entry| {
            let new_text = vcs_repo_replacement_text(
                document.text(),
                trigger,
                &entry.r#ref,
            );
            CompletionCandidate {
                display: entry.name.clone(),
                insertion: entry.r#ref.clone(),
                detail: (!entry.visibility.is_empty())
                    .then(|| entry.visibility.clone()),
                documentation: (!entry.description.is_empty())
                    .then(|| entry.description.clone()),
                is_dir: false,
                name: entry.name.clone(),
                replacement: Some(EditorTextEdit {
                    range: replacement_range,
                    new_text,
                }),
                additional_edits: Vec::new(),
                kind: "repo".to_string(),
                project: trigger.namespace.clone(),
                status: entry.visibility.clone(),
            }
        })
        .collect();

    CompletionList {
        candidates,
        shared_extension: String::new(),
    }
}

pub fn apply_vcs_repo_selection(
    text: &str,
    trigger: &VcsRepoTrigger,
    selected_ref: &str,
) -> String {
    let replacement = vcs_repo_replacement_text(text, trigger, selected_ref);
    format!(
        "{}{}{}",
        &text[..trigger.ref_start],
        replacement,
        &text[trigger.ref_end..]
    )
}

fn vcs_repo_replacement_text(
    text: &str,
    trigger: &VcsRepoTrigger,
    selected_ref: &str,
) -> String {
    let after = &text[trigger.ref_end..];
    if trigger.separator == "(" {
        let suffix = if after.starts_with(')') { "" } else { ")" };
        return format!("{selected_ref}{suffix}");
    }

    let suffix = if after.starts_with([' ', '\t'])
        || after.starts_with('\r') && after != "\r" && after != "\r\n"
        || after.starts_with('\n') && after != "\n"
    {
        ""
    } else {
        " "
    };
    format!("{selected_ref}{suffix}")
}

pub fn detect_vcs_repo_context_at_position(
    document: &DocumentSnapshot,
    position: EditorPosition,
    known_workflow_names: &[String],
) -> Option<CompletionContext> {
    let text = document.text();
    let cursor = document.position_to_byte_offset(position)?;
    let mut names: Vec<&str> = known_workflow_names
        .iter()
        .map(String::as_str)
        .filter(|name| !name.is_empty())
        .collect();
    if names.is_empty() {
        return None;
    }
    names.sort_by(|left, right| {
        right.len().cmp(&left.len()).then_with(|| left.cmp(right))
    });

    let mut start = cursor;
    while start > 0 {
        let prev = previous_char_boundary(text, start)?;
        if text[prev..].chars().next()?.is_whitespace() {
            break;
        }
        start = prev;
    }

    if start >= text.len() || text.get(start..start + 1) != Some("#") {
        return None;
    }

    let workflow = names
        .iter()
        .copied()
        .find(|name| text[start + 1..].starts_with(name))?;
    let mut pos = start + 1 + workflow.len();
    if text[pos..].starts_with("!!") || text[pos..].starts_with("??") {
        pos += 2;
    }
    let separator = match text[pos..].chars().next()? {
        ':' => ":",
        '(' => "(",
        _ => return None,
    };
    let ref_start = pos + 1;
    if cursor < ref_start {
        return None;
    }
    if separator == "(" && text[ref_start..cursor].contains(')') {
        return None;
    }

    let (ref_end, token_end) = find_vcs_repo_ref_end(text, cursor, separator)?;
    if cursor > ref_end {
        return None;
    }
    let ref_before_cursor = &text[ref_start..cursor];
    let slash_offset = ref_before_cursor.rfind('/')?;
    let full_ref = &text[ref_start..ref_end];
    if full_ref.contains("://") {
        return None;
    }

    let namespace = &ref_before_cursor[..slash_offset];
    if namespace.is_empty()
        || namespace.starts_with('~')
        || namespace.starts_with('.')
    {
        return None;
    }

    let query_start = ref_start + slash_offset + 1;
    let trigger = VcsRepoTrigger {
        start,
        end: token_end,
        workflow: workflow.to_string(),
        separator: separator.to_string(),
        ref_start,
        ref_end,
        namespace: namespace.to_string(),
        query: ref_before_cursor[slash_offset + 1..].to_string(),
        namespace_span: (ref_start, ref_start + slash_offset),
        query_span: (query_start, cursor),
    };
    let token_range = document.byte_range_to_range(start, token_end)?;
    let replacement_range =
        document.byte_range_to_range(trigger.ref_start, trigger.ref_end)?;
    Some(CompletionContext {
        kind: CompletionContextKind::VcsRepo,
        token: Some(TokenInfo {
            text: text[start..token_end].to_string(),
            range: token_range,
            byte_start: start,
            byte_end: token_end,
        }),
        active_xprompt: None,
        active_input: None,
        directive_name: None,
        selected_values: Vec::new(),
        directive: None,
        vcs_repo: Some(trigger),
        vcs_ref: None,
        artifact_ref: None,
        replacement_range,
    })
}

fn find_vcs_repo_ref_end(
    text: &str,
    cursor: usize,
    separator: &str,
) -> Option<(usize, usize)> {
    let mut end = cursor;
    if separator == "(" {
        while end < text.len() {
            let ch = text[end..].chars().next()?;
            if ch.is_whitespace() || ch == ')' {
                break;
            }
            end += ch.len_utf8();
        }
        let token_end = if end < text.len()
            && text[end..].chars().next().is_some_and(|ch| ch == ')')
        {
            end + 1
        } else {
            end
        };
        return Some((end, token_end));
    }

    while end < text.len() {
        let ch = text[end..].chars().next()?;
        if ch.is_whitespace() {
            break;
        }
        end += ch.len_utf8();
    }
    Some((end, end))
}

// --- vcs_ref (`#gh:` / `#gh(` root-ref completion) ------------------------

pub fn build_vcs_ref_completion_candidates(
    document: &DocumentSnapshot,
    context: &CompletionContext,
    entries: &[VcsProjectEntry],
    namespaces: &[VcsNamespaceEntry],
) -> CompletionList {
    let Some(trigger) = context.vcs_ref.as_ref() else {
        return CompletionList {
            candidates: Vec::new(),
            shared_extension: String::new(),
        };
    };
    let Some(replacement_range) =
        document.byte_range_to_range(trigger.ref_start, trigger.ref_end)
    else {
        return CompletionList {
            candidates: Vec::new(),
            shared_extension: String::new(),
        };
    };

    let query = trigger.query.to_lowercase();
    let mut candidates = Vec::new();
    for include_patches in [false, true] {
        for entry in entries {
            let entry_kind = vcs_project_entry_kind(entry);
            let is_patch = entry_kind == "patch";
            if is_patch != include_patches
                || entry.vcs_prefix != trigger.workflow
                || !vcs_ref_project_matches(entry, &query)
            {
                continue;
            }

            let new_text = vcs_ref_replacement_text(
                document.text(),
                trigger,
                &entry.name,
                false,
            );
            candidates.push(CompletionCandidate {
                display: entry.name.clone(),
                insertion: entry.name.clone(),
                detail: Some(format!(
                    "{} · {}",
                    entry.provider_display, entry.display_tag
                )),
                documentation: (!entry.description.is_empty())
                    .then(|| entry.description.clone()),
                is_dir: false,
                name: entry.name.clone(),
                replacement: Some(EditorTextEdit {
                    range: replacement_range,
                    new_text,
                }),
                additional_edits: Vec::new(),
                kind: entry_kind.to_string(),
                project: entry.project.clone(),
                status: entry.status.clone(),
            });
        }
    }

    for namespace in namespaces {
        if !prefix_matches(&namespace.name, &query) {
            continue;
        }
        let insertion = vcs_ref_namespace_insertion(&namespace.name);
        let new_text = vcs_ref_replacement_text(
            document.text(),
            trigger,
            &insertion,
            true,
        );
        candidates.push(CompletionCandidate {
            display: insertion.clone(),
            insertion,
            detail: (!namespace.description.is_empty())
                .then(|| namespace.description.clone()),
            documentation: None,
            is_dir: true,
            name: namespace.name.clone(),
            replacement: Some(EditorTextEdit {
                range: replacement_range,
                new_text,
            }),
            additional_edits: Vec::new(),
            kind: "namespace".to_string(),
            project: trigger.workflow.clone(),
            status: if namespace.kind_label.is_empty() {
                "org".to_string()
            } else {
                namespace.kind_label.clone()
            },
        });
    }

    CompletionList {
        candidates,
        shared_extension: String::new(),
    }
}

fn vcs_project_entry_kind(entry: &VcsProjectEntry) -> &str {
    let raw_kind = if entry.entry_kind.is_empty() {
        entry.kind.as_str()
    } else {
        entry.entry_kind.as_str()
    };
    match raw_kind {
        // Legacy completion metadata maps to the canonical patch kind.
        "changespec" => "patch",
        "patch" => "patch",
        _ => "project",
    }
}

pub fn apply_vcs_ref_selection(
    text: &str,
    trigger: &VcsRefTrigger,
    selected_ref: &str,
    chain: bool,
) -> String {
    let replacement =
        vcs_ref_replacement_text(text, trigger, selected_ref, chain);
    format!(
        "{}{}{}",
        &text[..trigger.ref_start],
        replacement,
        &text[trigger.ref_end..]
    )
}

fn vcs_ref_replacement_text(
    text: &str,
    trigger: &VcsRefTrigger,
    selected_ref: &str,
    chain: bool,
) -> String {
    let selected_ref = if chain {
        vcs_ref_namespace_insertion(selected_ref)
    } else {
        selected_ref.to_string()
    };
    if chain {
        return selected_ref;
    }

    let after = &text[trigger.ref_end..];
    if trigger.separator == "(" {
        let suffix = if after.starts_with(')') { "" } else { ")" };
        return format!("{selected_ref}{suffix}");
    }

    let suffix = if after.starts_with([' ', '\t'])
        || after.starts_with('\r') && after != "\r" && after != "\r\n"
        || after.starts_with('\n') && after != "\n"
    {
        ""
    } else {
        " "
    };
    format!("{selected_ref}{suffix}")
}

fn vcs_ref_namespace_insertion(name: &str) -> String {
    format!("{}/", name.trim_end_matches('/'))
}

fn vcs_ref_project_matches(entry: &VcsProjectEntry, query: &str) -> bool {
    prefix_matches(&entry.name, query)
        || entry
            .aliases
            .iter()
            .any(|alias| prefix_matches(alias, query))
}

fn prefix_matches(value: &str, query_lower: &str) -> bool {
    query_lower.is_empty() || value.to_lowercase().starts_with(query_lower)
}

pub fn detect_vcs_ref_context_at_position(
    document: &DocumentSnapshot,
    position: EditorPosition,
    known_workflow_names: &[String],
) -> Option<CompletionContext> {
    let text = document.text();
    let cursor = document.position_to_byte_offset(position)?;
    let mut names: Vec<&str> = known_workflow_names
        .iter()
        .map(String::as_str)
        .filter(|name| !name.is_empty())
        .collect();
    if names.is_empty() {
        return None;
    }
    names.sort_by(|left, right| {
        right.len().cmp(&left.len()).then_with(|| left.cmp(right))
    });

    let mut start = cursor;
    while start > 0 {
        let prev = previous_char_boundary(text, start)?;
        if text[prev..].chars().next()?.is_whitespace() {
            break;
        }
        start = prev;
    }

    if start >= text.len() || text.get(start..start + 1) != Some("#") {
        return None;
    }

    let workflow = names
        .iter()
        .copied()
        .find(|name| text[start + 1..].starts_with(name))?;
    let mut pos = start + 1 + workflow.len();
    if text[pos..].starts_with("!!") || text[pos..].starts_with("??") {
        pos += 2;
    }
    let separator = match text[pos..].chars().next()? {
        ':' => ":",
        '(' => "(",
        _ => return None,
    };
    let ref_start = pos + 1;
    if cursor < ref_start {
        return None;
    }
    let ref_before_cursor = &text[ref_start..cursor];
    if ref_before_cursor.contains(')') || ref_before_cursor.contains('/') {
        return None;
    }

    let (ref_end, token_end) = find_vcs_repo_ref_end(text, cursor, separator)?;
    if cursor > ref_end {
        return None;
    }
    let full_ref = &text[ref_start..ref_end];
    if full_ref.contains('/')
        || full_ref.contains("://")
        || full_ref.starts_with(['~', '.'])
        || full_ref.contains(')')
    {
        return None;
    }

    let trigger = VcsRefTrigger {
        start,
        end: token_end,
        workflow: workflow.to_string(),
        separator: separator.to_string(),
        ref_start,
        ref_end,
        query: ref_before_cursor.to_string(),
        query_span: (ref_start, cursor),
    };
    let token_range = document.byte_range_to_range(start, token_end)?;
    let replacement_range =
        document.byte_range_to_range(trigger.ref_start, trigger.ref_end)?;
    Some(CompletionContext {
        kind: CompletionContextKind::VcsRef,
        token: Some(TokenInfo {
            text: text[start..token_end].to_string(),
            range: token_range,
            byte_start: start,
            byte_end: token_end,
        }),
        active_xprompt: None,
        active_input: None,
        directive_name: None,
        selected_values: Vec::new(),
        directive: None,
        vcs_repo: None,
        vcs_ref: Some(trigger),
        artifact_ref: None,
        replacement_range,
    })
}

fn previous_char_boundary(text: &str, byte_idx: usize) -> Option<usize> {
    text.get(..byte_idx)?
        .char_indices()
        .last()
        .map(|(idx, _)| idx)
}

// --- vcs_project (`+`) completion -----------------------------------------

/// Build `vcs_project` completion candidates for a `+query` trigger token.
///
/// Each candidate expands the selected project into the prompt via the
/// canonical VCS-tag expansion algorithm (see [`apply_vcs_project_selection`]),
/// represented as a primary edit that consumes the trigger span plus
/// `additional_edits` that prepend/replace the VCS workflow tag at the start of
/// the document. When those edits would overlap they are merged into a single
/// primary edit. The output is byte-for-byte identical to the Python
/// `apply_vcs_project_selection` for the shared golden test vectors.
pub fn build_vcs_project_completion_candidates(
    token: &TokenInfo,
    document: &DocumentSnapshot,
    position: EditorPosition,
    entries: &[VcsProjectEntry],
    known_workflow_names: &[String],
) -> CompletionList {
    let text = document.text();
    let t0 = token.byte_start;
    let t1 = token.byte_end;
    let cursor = document
        .position_to_byte_offset(position)
        .unwrap_or(t1)
        .clamp(t0, t1);
    // Filter query is the text after the plus up to the cursor (empty for a
    // bare `+`), matching the Python `find_vcs_project_trigger`.
    let query = text.get(t0 + 1..cursor).unwrap_or("").to_lowercase();

    let replace_re = vcs_replace_regex(known_workflow_names);
    let mut candidates = Vec::new();
    for entry in entries {
        let matches_query = query.is_empty()
            || entry.name.to_lowercase().starts_with(&query)
            || entry
                .aliases
                .iter()
                .any(|alias| alias.to_lowercase().starts_with(&query));
        if !matches_query {
            continue;
        }

        let edits = vcs_project_byte_edits(
            text,
            t0,
            t1,
            &entry.display_tag,
            &replace_re,
        );
        let Some(primary) = byte_edit_to_text_edit(document, &edits.primary)
        else {
            continue;
        };
        let additional: Option<Vec<EditorTextEdit>> = edits
            .additional
            .iter()
            .map(|edit| byte_edit_to_text_edit(document, edit))
            .collect();
        let Some(additional) = additional else {
            continue;
        };

        candidates.push(CompletionCandidate {
            display: entry.name.clone(),
            insertion: entry.display_tag.clone(),
            detail: Some(format!(
                "{} · {}",
                entry.provider_display, entry.display_tag
            )),
            documentation: (!entry.description.is_empty())
                .then(|| entry.description.clone()),
            is_dir: false,
            name: entry.name.clone(),
            replacement: Some(primary),
            additional_edits: additional,
            kind: vcs_project_entry_kind(entry).to_string(),
            project: entry.project.clone(),
            status: entry.status.clone(),
        });
    }
    CompletionList {
        candidates,
        shared_extension: String::new(),
    }
}

/// Apply a selected project's VCS tag to `text`, returning the new full text.
///
/// This is the canonical expansion algorithm (the cross-language parity
/// contract). It mirrors the Python `apply_vcs_project_selection`: remove the
/// `[t0, t1)` trigger token, collapse one adjacent space, then either replace
/// every line-start VCS workflow tag with `display_tag` or -- when none exist --
/// prepend `display_tag` after any leading frontmatter / whitespace /
/// `%directive` tokens.
pub fn apply_vcs_project_selection(
    text: &str,
    t0: usize,
    t1: usize,
    display_tag: &str,
    replace_re: &Regex,
) -> String {
    let (d0, d1) = strip_trigger_region(text, t0, t1);
    let base = format!("{}{}", &text[..d0], &text[d1..]);
    let tag_with_space = format!("{display_tag} ");

    if replace_re.is_match(&base) {
        return replace_re
            .replace_all(&base, |caps: &regex::Captures| {
                let prefix = caps.get(1).map_or("", |m| m.as_str());
                format!("{prefix}{tag_with_space}")
            })
            .into_owned();
    }

    let offset = vcs_prepend_offset(&base);
    format!("{}{}{}", &base[..offset], tag_with_space, &base[offset..])
}

fn detect_vcs_project_context_at_position(
    document: &DocumentSnapshot,
    position: EditorPosition,
) -> Option<CompletionContext> {
    let token = vcs_project_trigger_token(document, position)?;
    Some(CompletionContext {
        kind: CompletionContextKind::VcsProject,
        replacement_range: token.range,
        token: Some(token),
        active_xprompt: None,
        active_input: None,
        directive_name: None,
        selected_values: Vec::new(),
        directive: None,
        vcs_repo: None,
        vcs_ref: None,
        artifact_ref: None,
    })
}

/// A single byte-range edit: replace `text[start..end]` with `new_text`.
struct VcsByteEdit {
    start: usize,
    end: usize,
    new_text: String,
}

struct VcsProjectByteEdits {
    primary: VcsByteEdit,
    additional: Vec<VcsByteEdit>,
}

/// Compute the primary + additional edits for one project selection.
///
/// Edits are expressed in original-document byte coordinates and are guaranteed
/// not to overlap (overlapping cases are either merged into the primary edit or,
/// defensively, collapsed into a single full-document replacement).
fn vcs_project_byte_edits(
    text: &str,
    trigger_start: usize,
    trigger_end: usize,
    display_tag: &str,
    replace_re: &Regex,
) -> VcsProjectByteEdits {
    let (d0, d1) = strip_trigger_region(text, trigger_start, trigger_end);
    let base = format!("{}{}", &text[..d0], &text[d1..]);
    let gap = d1 - d0;
    // Map a `base` byte offset back to original-document coordinates.
    let to_original = |p: usize| if p <= d0 { p } else { p + gap };
    let tag_with_space = format!("{display_tag} ");

    let tag_matches: Vec<(usize, usize, usize)> = replace_re
        .captures_iter(&base)
        .map(|caps| {
            let whole = caps.get(0).expect("group 0 always present");
            let prefix_len = caps.get(1).map_or(0, |m| m.len());
            (whole.start(), whole.end(), prefix_len)
        })
        .collect();

    let (primary, additional) = if tag_matches.is_empty() {
        // Prepend branch: insert the tag at the frontmatter/directive-aware
        // offset.
        let insert_at = to_original(vcs_prepend_offset(&base));
        if insert_at == d0 {
            // The prepend point coincides with the trigger-deletion start;
            // merge into one edit (the deleted region collapses to the tag).
            (
                VcsByteEdit {
                    start: d0,
                    end: d1,
                    new_text: tag_with_space,
                },
                Vec::new(),
            )
        } else {
            (
                VcsByteEdit {
                    start: d0,
                    end: d1,
                    new_text: String::new(),
                },
                vec![VcsByteEdit {
                    start: insert_at,
                    end: insert_at,
                    new_text: tag_with_space,
                }],
            )
        }
    } else {
        // Replace branch: rewrite every line-start tag, preserving any leading
        // `%directive` prefix captured in group 1.
        let additional = tag_matches
            .into_iter()
            .map(|(match_start, match_end, prefix_len)| {
                let prefix =
                    base[match_start..match_start + prefix_len].to_string();
                VcsByteEdit {
                    start: to_original(match_start),
                    end: to_original(match_end),
                    new_text: format!("{prefix}{tag_with_space}"),
                }
            })
            .collect();
        (
            VcsByteEdit {
                start: d0,
                end: d1,
                new_text: String::new(),
            },
            additional,
        )
    };

    // Defensive guard: if the edits would overlap (no realistic input produces
    // this, but LSP forbids overlapping ranges), fall back to a single
    // full-document replacement with the canonical result.
    if vcs_edits_conflict(&primary, &additional) {
        let canonical = apply_vcs_project_selection(
            text,
            trigger_start,
            trigger_end,
            display_tag,
            replace_re,
        );
        return VcsProjectByteEdits {
            primary: VcsByteEdit {
                start: 0,
                end: text.len(),
                new_text: canonical,
            },
            additional: Vec::new(),
        };
    }

    VcsProjectByteEdits {
        primary,
        additional,
    }
}

fn vcs_edits_conflict(
    primary: &VcsByteEdit,
    additional: &[VcsByteEdit],
) -> bool {
    let mut spans: Vec<(usize, usize)> =
        std::iter::once((primary.start, primary.end))
            .chain(additional.iter().map(|edit| (edit.start, edit.end)))
            .collect();
    spans.sort_by_key(|&(start, end)| (start, end));
    spans.windows(2).any(|pair| pair[1].0 < pair[0].1)
}

fn byte_edit_to_text_edit(
    document: &DocumentSnapshot,
    edit: &VcsByteEdit,
) -> Option<EditorTextEdit> {
    Some(EditorTextEdit {
        range: document.byte_range_to_range(edit.start, edit.end)?,
        new_text: edit.new_text.clone(),
    })
}

/// Remove the `[t0, t1)` trigger span, collapsing one adjacent space, and
/// return the resulting deletion region `[d0, d1)`. Mirrors the Python
/// `_strip_trigger_token`.
fn strip_trigger_region(text: &str, t0: usize, t1: usize) -> (usize, usize) {
    let before = &text[..t0];
    let after = &text[t1..];
    let before_space = before.ends_with(' ');
    let after_space = after.starts_with(' ');

    if before_space && after_space {
        // Token sat between two spaces; drop the following one.
        (t0, t1 + 1)
    } else if before_space
        && (after.is_empty() || after.starts_with(['\r', '\n']))
    {
        // A trailing space would be orphaned at end of line/prompt.
        (t0 - 1, t1)
    } else if after_space
        && (before.is_empty() || before.ends_with(['\r', '\n']))
    {
        // A leading space would be orphaned at start of line/prompt.
        (t0, t1 + 1)
    } else {
        (t0, t1)
    }
}

/// Where a leading VCS workflow tag should be inserted: after any leading YAML
/// frontmatter block, leading horizontal whitespace, and leading `%directive`
/// tokens.
/// Mirrors the Python `find_vcs_workflow_tag_prepend_offset`.
fn vcs_prepend_offset(text: &str) -> usize {
    let frontmatter_len = frontmatter_block_len(text);
    let body = &text[frontmatter_len..];
    let leading_ws = body
        .char_indices()
        .find(|(_, ch)| !ch.is_whitespace() || matches!(ch, '\n' | '\r'))
        .map_or(body.len(), |(idx, _)| idx);
    let after_ws = &body[leading_ws..];
    let directive_len = directive_prefix_regex()
        .find(after_ws)
        .map_or(0, |m| m.end());
    frontmatter_len + leading_ws + directive_len
}

/// Byte length of a leading YAML frontmatter block (`---` ... `---`), or 0 when
/// `text` does not begin with one. Mirrors the Python `_split_frontmatter_block`.
fn frontmatter_block_len(text: &str) -> usize {
    let lines: Vec<&str> = text.split_inclusive('\n').collect();
    let Some(first) = lines.first() else {
        return 0;
    };
    if first.trim() != "---" {
        return 0;
    }
    let mut consumed = first.len();
    for line in &lines[1..] {
        consumed += line.len();
        if line.trim() == "---" {
            return consumed;
        }
    }
    0
}

fn directive_prefix_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"^(?:%\S+[\s]+)+").unwrap())
}

/// Build the multiline pattern matching VCS workflow tags at the start of any
/// line, mirroring the Python `_get_vcs_replace_pattern`. Group 1 captures any
/// leading `%directive` prefix to preserve it during replacement.
fn vcs_replace_regex(known_workflow_names: &[String]) -> Regex {
    let mut names: Vec<&str> =
        known_workflow_names.iter().map(String::as_str).collect();
    names.sort_unstable();
    let alternation = names
        .iter()
        .map(|name| regex::escape(name))
        .collect::<Vec<_>>()
        .join("|");
    // The boundary after a tag is whitespace OR end-of-input. `\s` is tried
    // first, so any actual whitespace (including a newline) is consumed and
    // replaced exactly as before; `$` only wins at true EOF, letting a
    // line-start tag with no trailing whitespace (e.g. `#gh:sase` alone) still
    // be replaced rather than treated as absent.
    let pattern = format!(
        r"(?m)^((?:%\S+[\s]+)*)#(?:{alternation})(?:!!|\?\?)?(?:\([^)]*\)|\+|[_:][^\s]*|)(?:\s|$)"
    );
    Regex::new(&pattern).expect("valid vcs replace pattern")
}

pub fn named_args_skeleton(entry: &XpromptAssistEntry) -> String {
    let required: Vec<_> =
        entry.inputs.iter().filter(|input| input.required).collect();
    if required.is_empty() {
        return entry.insertion.clone();
    }
    let args = required
        .iter()
        .enumerate()
        .map(|(idx, input)| format!("{}=${}", input.name, idx + 1))
        .collect::<Vec<_>>()
        .join(", ");
    format!("{}({args})$0", entry.insertion)
}

pub fn colon_args_skeleton(entry: &XpromptAssistEntry) -> String {
    format!("{}:$0", entry.insertion)
}

fn detect_xprompt_arg_completion_at_position(
    document: &DocumentSnapshot,
    position: EditorPosition,
    entries: &[XpromptAssistEntry],
) -> Option<CompletionContext> {
    let cursor = document.position_to_byte_offset(position)?;
    let text = document.text();
    let prefix = text.get(..cursor)?;
    let captures = xprompt_ref_re().captures_iter(prefix);
    for caps in captures {
        let whole = caps.get(0)?;
        let name = caps.name("name")?.as_str().replace("__", "/");
        let entry = entries.iter().find(|entry| entry.name == name)?;
        if entry.inputs.is_empty() {
            continue;
        }
        let base_end = whole.end();
        let suffix = text.get(base_end..cursor)?;
        if suffix.starts_with(':') {
            let target =
                colon_arg_context(entry, text, base_end, cursor, suffix)?;
            return arg_context(document, cursor, entry, target);
        }
        if suffix.starts_with('(') {
            let target =
                paren_arg_context(entry, text, base_end, cursor, suffix)?;
            return arg_context(document, cursor, entry, target);
        }
    }
    None
}

struct XpromptArgCompletionTarget {
    kind: CompletionContextKind,
    active_input: XpromptInputHint,
    token_start: usize,
    token_end: usize,
    selected_values: Vec<String>,
}

fn colon_arg_context(
    entry: &XpromptAssistEntry,
    text: &str,
    base_end: usize,
    cursor: usize,
    suffix: &str,
) -> Option<XpromptArgCompletionTarget> {
    let value = suffix.strip_prefix(':')?;
    if value.chars().any(char::is_whitespace)
        || value.contains('+')
        || value.contains('(')
        || value.contains(')')
    {
        return None;
    }
    let index = value.matches(',').count().min(entry.inputs.len() - 1);
    let active_input = entry.inputs.get(index)?.clone();
    let body_start = base_end + 1;
    let cursor_in_body = cursor.checked_sub(body_start)?;
    let clause_start = value.rfind(',').map(|idx| idx + 1).unwrap_or(0);
    let body_end = if cursor_in_body == clause_start {
        cursor
    } else {
        text[cursor..]
            .find(char::is_whitespace)
            .map(|offset| cursor + offset)
            .unwrap_or(text.len())
    };
    let body = text.get(body_start..body_end)?;
    let clause_end = body[cursor_in_body..]
        .find(',')
        .map(|offset| cursor_in_body + offset)
        .unwrap_or(body.len());
    let token_start = body_start + clause_start;
    let token_end = body_start + clause_end;
    Some(XpromptArgCompletionTarget {
        kind: completion_kind_for_input(&active_input),
        active_input,
        token_start,
        token_end,
        selected_values: selected_positional_values(body, clause_start),
    })
}

fn paren_arg_context(
    entry: &XpromptAssistEntry,
    text: &str,
    base_end: usize,
    cursor: usize,
    suffix: &str,
) -> Option<XpromptArgCompletionTarget> {
    let prefix_body = suffix.strip_prefix('(')?;
    if prefix_body.contains(')') {
        return None;
    }
    let body_start = base_end + 1;
    let cursor_in_body = cursor.checked_sub(body_start)?;
    let body_end = find_matching_paren(text, base_end).unwrap_or(cursor);
    let body = text.get(body_start..body_end)?;
    let clause_start = prefix_body.rfind(',').map(|idx| idx + 1).unwrap_or(0);
    let clause_end = body[cursor_in_body..]
        .find(',')
        .map(|offset| cursor_in_body + offset)
        .unwrap_or(body.len());
    let clause = &body[clause_start..clause_end];
    let stripped = clause.trim_start();
    let leading_ws = clause.len() - stripped.len();
    let value_start = base_end + 1 + clause_start + leading_ws;
    let value_end = trim_end(text, value_start, body_start + clause_end);
    let selected = selected_positional_values(body, clause_start);

    if !stripped.contains('=') {
        let token = text.get(value_start..cursor)?;
        if token.chars().any(char::is_whitespace) {
            return None;
        }
        let positional_index = body[..clause_start]
            .split(',')
            .filter(|clause| !clause.trim().is_empty() && !clause.contains('='))
            .count();
        if let Some(active_input) = entry
            .inputs
            .get(positional_index)
            .or_else(|| entry.inputs.last().filter(|input| input.repeatable))
            .filter(|input| input.repeatable)
            .cloned()
        {
            return Some(XpromptArgCompletionTarget {
                kind: completion_kind_for_input(&active_input),
                active_input,
                token_start: value_start,
                token_end: value_end,
                selected_values: selected,
            });
        }
        let placeholder = XpromptInputHint {
            name: String::new(),
            r#type: String::new(),
            description: None,
            required: false,
            default_display: None,
            position: 0,
            repeatable: false,
        };
        return Some(XpromptArgCompletionTarget {
            kind: CompletionContextKind::XpromptArgumentName,
            active_input: placeholder,
            token_start: value_start,
            token_end: value_end,
            selected_values: selected,
        });
    }

    let (name_part, value_part) = stripped.split_once('=')?;
    let name = name_part.trim();
    let active_input = entry
        .inputs
        .iter()
        .find(|input| input.name == name)?
        .clone();
    let value_leading_ws = value_part.len() - value_part.trim_start().len();
    let token_start = value_start + name_part.len() + 1 + value_leading_ws;
    Some(XpromptArgCompletionTarget {
        kind: completion_kind_for_input(&active_input),
        active_input,
        token_start,
        token_end: value_end,
        selected_values: Vec::new(),
    })
}

fn arg_context(
    document: &DocumentSnapshot,
    cursor: usize,
    entry: &XpromptAssistEntry,
    target: XpromptArgCompletionTarget,
) -> Option<CompletionContext> {
    let token_range =
        document.byte_range_to_range(target.token_start, cursor)?;
    let replacement_range =
        document.byte_range_to_range(target.token_start, target.token_end)?;
    Some(CompletionContext {
        kind: target.kind,
        token: Some(TokenInfo {
            text: document.text().get(target.token_start..cursor)?.to_string(),
            range: token_range,
            byte_start: target.token_start,
            byte_end: cursor,
        }),
        active_xprompt: Some(entry.name.clone()),
        active_input: (!target.active_input.name.is_empty())
            .then_some(target.active_input.name),
        directive_name: None,
        selected_values: target.selected_values,
        directive: None,
        vcs_repo: None,
        vcs_ref: None,
        artifact_ref: None,
        replacement_range,
    })
}

fn find_matching_paren(text: &str, open: usize) -> Option<usize> {
    if text.as_bytes().get(open) != Some(&b'(') {
        return None;
    }
    let mut depth = 1usize;
    for (offset, byte) in text.as_bytes()[open + 1..].iter().enumerate() {
        match byte {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(open + 1 + offset);
                }
            }
            _ => {}
        }
    }
    None
}

fn trim_end(text: &str, start: usize, mut end: usize) -> usize {
    while end > start && text.as_bytes()[end - 1].is_ascii_whitespace() {
        end -= 1;
    }
    end
}

fn selected_positional_values(
    body: &str,
    active_clause_start: usize,
) -> Vec<String> {
    let mut values = Vec::new();
    let mut clause_start = 0usize;
    for clause in body.split(',') {
        if clause_start != active_clause_start && !clause.contains('=') {
            let value = clause.trim();
            if !value.is_empty() {
                values.push(value.to_string());
            }
        }
        clause_start += clause.len() + 1;
    }
    values
}

fn context_for_token(
    kind: CompletionContextKind,
    token: TokenInfo,
) -> CompletionContext {
    CompletionContext {
        kind,
        replacement_range: token.range,
        token: Some(token),
        active_xprompt: None,
        active_input: None,
        directive_name: None,
        selected_values: Vec::new(),
        directive: None,
        vcs_repo: None,
        vcs_ref: None,
        artifact_ref: None,
    }
}

fn completion_kind_for_input(
    input: &XpromptInputHint,
) -> CompletionContextKind {
    match input.r#type.as_str() {
        "path" => CompletionContextKind::XpromptArgumentPath,
        "bool" => CompletionContextKind::XpromptArgumentValue,
        "agent" => CompletionContextKind::XpromptArgumentAgent,
        _ => CompletionContextKind::XpromptArgumentTypeHint,
    }
}

fn input_label(input: &XpromptInputHint) -> String {
    let suffix = if input.required { "" } else { "?" };
    let repeatable = if input.repeatable { "…" } else { "" };
    format!("{}{repeatable}{suffix}: {}", input.name, input.r#type)
}

fn input_documentation(input: &XpromptInputHint) -> Option<String> {
    let mut parts = Vec::new();
    if let Some(description) = input
        .description
        .as_deref()
        .filter(|value| !value.is_empty())
    {
        parts.push(description.to_string());
    }
    if let Some(default) = &input.default_display {
        parts.push(format!("default: {default}"));
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts.join("\n\n"))
    }
}

fn snippet_documentation(entry: &EditorSnippetEntryWire) -> Option<String> {
    let mut parts = Vec::new();
    if let Some(description) = entry
        .description
        .as_deref()
        .filter(|value| !value.is_empty())
    {
        parts.push(description.to_string());
    }
    if let Some(source_path) = entry
        .source_path_display
        .as_deref()
        .filter(|value| !value.is_empty())
    {
        parts.push(format!("Source: {source_path}"));
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts.join("\n\n"))
    }
}

fn shared_extension(
    candidates: &[CompletionCandidate],
    partial: &str,
) -> String {
    if candidates.len() <= 1 {
        return String::new();
    }
    let mut prefix = candidates[0].name.clone();
    for candidate in &candidates[1..] {
        prefix = common_prefix(&prefix, &candidate.name);
    }
    if prefix.len() > partial.len() {
        prefix[partial.len()..].to_string()
    } else {
        String::new()
    }
}

fn common_prefix(left: &str, right: &str) -> String {
    let mut end = 0;
    for ((left_idx, left_ch), (_, right_ch)) in
        left.char_indices().zip(right.char_indices())
    {
        if left_ch != right_ch {
            break;
        }
        end = left_idx + left_ch.len_utf8();
    }
    left[..end].to_string()
}

fn xprompt_ref_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(
            r#"(?m)(?:^|[\s\(\[\{"'])(?P<marker>#!|#)(?P<name>[A-Za-z_][A-Za-z0-9_]*(?:(?:/|__)[A-Za-z_][A-Za-z0-9_]*)*)(?:!!|\?\?)?"#,
        )
        .unwrap()
    })
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;
    use crate::editor::directive::directive_argument_candidates;
    use crate::effort::EFFORT_LEVELS_ORDERED;
    use crate::MemoryTierWire;
    use crate::{
        ArtifactRefAgentRootWire, ArtifactRefBeadStoreWire,
        ArtifactRefDocumentRootWire, ArtifactRefPayloadWire,
        ArtifactRefRepositoryWire, EditorXpromptCatalogEntryWire,
        MobileXpromptInputWire,
    };

    fn pos(character: u32) -> EditorPosition {
        EditorPosition { line: 0, character }
    }

    fn agent_target(
        name: &str,
        kind: &str,
        member_count: usize,
        detail: &str,
    ) -> AgentCompletionEntry {
        AgentCompletionEntry {
            name: name.to_string(),
            status: String::new(),
            project: String::new(),
            kind: kind.to_string(),
            member_count,
            detail: detail.to_string(),
            documentation: String::new(),
        }
    }

    fn entries() -> Vec<XpromptAssistEntry> {
        assist_entries_from_catalog(&[
            EditorXpromptCatalogEntryWire {
                name: "review".to_string(),
                display_label: "review".to_string(),
                insertion: Some("#review".to_string()),
                reference_prefix: Some("#".to_string()),
                kind: Some("xprompt".to_string()),
                description: Some("Review code".to_string()),
                source_bucket: "builtin".to_string(),
                project: None,
                tags: vec![],
                input_signature: Some("(path: path, deep?: bool)".to_string()),
                inputs: vec![
                    MobileXpromptInputWire {
                        name: "path".to_string(),
                        r#type: "path".to_string(),
                        description: Some("Path to review".to_string()),
                        required: true,
                        default_display: None,
                        position: 0,
                        repeatable: false,
                        choices: Vec::new(),
                    },
                    MobileXpromptInputWire {
                        name: "deep".to_string(),
                        r#type: "bool".to_string(),
                        description: Some("Run a deeper pass".to_string()),
                        required: false,
                        default_display: Some("false".to_string()),
                        position: 1,
                        repeatable: false,
                        choices: Vec::new(),
                    },
                ],
                is_skill: false,
                skill_name: None,
                memory_type: None,
                content_preview: Some("review body".to_string()),
                source_path_display: Some(
                    "sase/xprompts/review.md".to_string(),
                ),
                definition_path: Some(
                    "/tmp/sase/xprompts/review.md".to_string(),
                ),
                definition_range: None,
            },
            EditorXpromptCatalogEntryWire {
                name: "run".to_string(),
                display_label: "run".to_string(),
                insertion: Some("#!run".to_string()),
                reference_prefix: Some("#!".to_string()),
                kind: Some("workflow".to_string()),
                description: None,
                source_bucket: "project".to_string(),
                project: None,
                tags: vec![],
                input_signature: None,
                inputs: vec![],
                is_skill: false,
                skill_name: None,
                memory_type: None,
                content_preview: None,
                source_path_display: None,
                definition_path: None,
                definition_range: None,
            },
            EditorXpromptCatalogEntryWire {
                name: "memory/glossary".to_string(),
                display_label: "memory/glossary".to_string(),
                insertion: Some("#memory/glossary".to_string()),
                reference_prefix: Some("#".to_string()),
                kind: Some("memory".to_string()),
                description: Some("SASE terms".to_string()),
                source_bucket: "project".to_string(),
                project: None,
                tags: vec![],
                input_signature: None,
                inputs: vec![],
                is_skill: false,
                skill_name: None,
                memory_type: Some(MemoryTierWire::Short),
                content_preview: Some("Glossary body".to_string()),
                source_path_display: Some(
                    "sase/memory/glossary.md".to_string(),
                ),
                definition_path: Some(
                    "/tmp/sase/memory/glossary.md".to_string(),
                ),
                definition_range: None,
            },
            EditorXpromptCatalogEntryWire {
                name: "skill/plan".to_string(),
                display_label: "skill/plan".to_string(),
                insertion: Some("#skill/plan".to_string()),
                reference_prefix: Some("#".to_string()),
                kind: Some("xprompt".to_string()),
                description: None,
                source_bucket: "builtin".to_string(),
                project: None,
                tags: vec![],
                input_signature: None,
                inputs: vec![],
                is_skill: true,
                skill_name: Some("plan".to_string()),
                memory_type: None,
                content_preview: None,
                source_path_display: None,
                definition_path: None,
                definition_range: None,
            },
        ])
    }

    fn artifact_context(root: &Path) -> ArtifactRefContextWire {
        ArtifactRefContextWire {
            document_roots: vec![ArtifactRefDocumentRootWire {
                kind: "designs".to_string(),
                root: root.join("designs").to_string_lossy().into_owned(),
                path_globs: None,
            }],
            chats_root: Some(root.join("chats").to_string_lossy().into_owned()),
            artifact_index_path: Some(
                root.join("artifact-index.jsonl")
                    .to_string_lossy()
                    .into_owned(),
            ),
            ..Default::default()
        }
    }

    fn git(repo: &Path, args: &[&str]) -> String {
        let output = Command::new("git")
            .arg("-C")
            .arg(repo)
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {args:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap().trim().to_string()
    }

    fn init_git_repo(repo: &Path) {
        fs::create_dir_all(repo).unwrap();
        git(repo, &["init", "--quiet"]);
        git(repo, &["config", "user.name", "Commit Test"]);
        git(repo, &["config", "user.email", "commit@example.com"]);
        git(repo, &["config", "core.abbrev", "7"]);
        // Keep background git maintenance from racing fixture construction (a
        // known class of interference, not a confirmed cause of the commit_at
        // flake seen in CI).
        git(repo, &["config", "gc.auto", "0"]);
        git(repo, &["config", "maintenance.auto", "false"]);
    }

    fn commit_at(
        repo: &Path,
        timestamp: i64,
        subject: &str,
        body: &str,
    ) -> String {
        let date = format!("{timestamp} +0000");
        let mut command = Command::new("git");
        command.arg("-C").arg(repo).args([
            "commit",
            "--quiet",
            "--allow-empty",
            "-m",
            subject,
        ]);
        if !body.is_empty() {
            command.args(["-m", body]);
        }
        let output = command
            .env("GIT_AUTHOR_DATE", &date)
            .env("GIT_COMMITTER_DATE", &date)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git commit in {} failed: {}",
            repo.display(),
            String::from_utf8_lossy(&output.stderr)
        );
        git(repo, &["rev-parse", "HEAD"])
    }

    /// Build one repository's entire commit history with a single
    /// `git fast-import` invocation instead of one `git commit` subprocess
    /// per commit. `fast-import` defaults the author identity to the
    /// committer identity, so `%at` and `%an` come out correct without
    /// separate author fields, and omitting the `from` command on every
    /// `commit` block still chains each commit onto the branch's current
    /// tip within the same stream.
    fn commit_batch(repo: &Path, commits: &[(i64, &str, &str)]) {
        let branch_ref = git(repo, &["symbolic-ref", "HEAD"]);
        let mut stream = Vec::new();
        for (timestamp, subject, body) in commits {
            let message = if body.is_empty() {
                format!("{subject}\n")
            } else {
                format!("{subject}\n\n{body}\n")
            };
            stream.extend_from_slice(
                format!(
                    "commit {branch_ref}\n\
                     committer Commit Test <commit@example.com> {timestamp} +0000\n\
                     data {}\n",
                    message.len()
                )
                .as_bytes(),
            );
            stream.extend_from_slice(message.as_bytes());
            stream.push(b'\n');
        }
        let mut child = Command::new("git")
            .arg("-C")
            .arg(repo)
            .args(["fast-import", "--quiet"])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        child.stdin.take().unwrap().write_all(&stream).unwrap();
        let output = child.wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "git fast-import into {} ({} commits) failed: {}",
            repo.display(),
            commits.len(),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn repository(name: &str, checkout: &Path) -> ArtifactRefRepositoryWire {
        ArtifactRefRepositoryWire {
            name: name.to_string(),
            checkout_paths: vec![checkout.to_string_lossy().into_owned()],
            ..Default::default()
        }
    }

    fn repository_with_kind(
        name: &str,
        kind: &str,
        checkout: &Path,
    ) -> ArtifactRefRepositoryWire {
        ArtifactRefRepositoryWire {
            kind: kind.to_string(),
            ..repository(name, checkout)
        }
    }

    fn artifact_completion_context(
        text: &str,
        cursor: usize,
        context: &ArtifactRefContextWire,
    ) -> CompletionContext {
        let document = DocumentSnapshot::new(text);
        let position = document.byte_offset_to_position(cursor).unwrap();
        classify_completion_context_with_artifacts_and_workflows(
            &document,
            position,
            &entries(),
            &[],
            Some(context),
        )
        .unwrap_or_else(|| {
            panic!(
                "expected artifact completion context for {text:?} at {cursor}"
            )
        })
    }

    #[test]
    fn classifies_artifact_kind_and_payload_at_every_cursor_position() {
        let context = artifact_context(Path::new("/tmp/artifacts"));
        let incomplete = "@des";
        for cursor in 1..=incomplete.len() {
            let completion =
                artifact_completion_context(incomplete, cursor, &context);
            assert_eq!(
                completion.kind,
                CompletionContextKind::ArtifactRefKind,
                "cursor {cursor}"
            );
        }

        let reference = "@designs:202607/guide.md";
        let separator = reference.find(':').unwrap();
        for cursor in 1..=reference.len() {
            let completion =
                artifact_completion_context(reference, cursor, &context);
            let expected = if cursor <= separator {
                CompletionContextKind::ArtifactRefKind
            } else {
                CompletionContextKind::ArtifactRefPayload
            };
            assert_eq!(completion.kind, expected, "cursor {cursor}");
            let trigger = completion.artifact_ref.unwrap();
            assert_eq!(trigger.candidate_span, (0, reference.len()));
            assert_eq!(
                trigger.kind.as_deref(),
                (cursor > separator).then_some("designs")
            );
        }
    }

    #[test]
    fn artifact_kind_candidates_list_builtins_in_documented_order() {
        let context = ArtifactRefContextWire::default();
        let completion = artifact_completion_context("@", 1, &context);
        let list = build_artifact_ref_kind_completion_candidates(
            completion.artifact_ref.as_ref().unwrap(),
            Some(completion.replacement_range),
            &context,
        );

        assert_eq!(
            list.candidates
                .iter()
                .map(|candidate| candidate.insertion.as_str())
                .collect::<Vec<_>>(),
            vec!["commit:", "chat:", "bug:", "file:", "bead:", "agent:"]
        );
        assert!(list.candidates.iter().all(|candidate| {
            candidate.detail.as_deref() == Some("builtin artifact kind")
        }));
    }

    #[test]
    fn builds_dynamic_kind_and_payload_candidates() {
        let temp = tempfile::tempdir().unwrap();
        let designs = temp.path().join("designs");
        fs::create_dir_all(designs.join("202607")).unwrap();
        fs::write(
            designs.join("202607/Guide.md"),
            "---\ntitle: Product Guide\n---\nguide",
        )
        .unwrap();
        fs::write(designs.join("202607/other.md"), "other").unwrap();
        let context = artifact_context(temp.path());

        let kind_context = artifact_completion_context("@DES", 4, &context);
        let kind_list = build_artifact_ref_kind_completion_candidates(
            kind_context.artifact_ref.as_ref().unwrap(),
            Some(kind_context.replacement_range),
            &context,
        );
        assert_eq!(kind_list.candidates.len(), 1);
        assert_eq!(kind_list.candidates[0].insertion, "designs:");

        let payload_text = "@designs:product";
        let payload_context = artifact_completion_context(
            payload_text,
            payload_text.len(),
            &context,
        );
        let payload_list = build_artifact_ref_payload_completion_candidates(
            payload_context.artifact_ref.as_ref().unwrap(),
            Some(payload_context.replacement_range),
            &context,
        );
        assert_eq!(payload_list.candidates.len(), 1);
        assert_eq!(payload_list.candidates[0].insertion, "202607/Guide.md");
        assert_eq!(payload_list.candidates[0].name, "Product Guide");
        assert!(payload_list.candidates[0]
            .detail
            .as_deref()
            .unwrap()
            .contains("designs"));

        let fallback_text = "@designs:other";
        let fallback_context = artifact_completion_context(
            fallback_text,
            fallback_text.len(),
            &context,
        );
        let fallback_list = build_artifact_ref_payload_completion_candidates(
            fallback_context.artifact_ref.as_ref().unwrap(),
            Some(fallback_context.replacement_range),
            &context,
        );
        assert_eq!(fallback_list.candidates.len(), 1);
        assert_eq!(fallback_list.candidates[0].insertion, "202607/other.md");
        assert_eq!(fallback_list.candidates[0].name, "other.md");
    }

    #[test]
    fn commit_inventory_merges_repositories_by_recency_and_assigns_rank() {
        let temp = tempfile::tempdir().unwrap();
        let first = temp.path().join("first");
        let second = temp.path().join("second");
        init_git_repo(&first);
        init_git_repo(&second);
        let old_sha = commit_at(&first, 1_700_000_000, "oldest", "");
        let middle_sha = commit_at(&second, 1_700_000_100, "middle", "");
        let new_sha = commit_at(&first, 1_700_000_200, "newest", "");
        let context = ArtifactRefContextWire {
            repositories: vec![
                repository("alpha", &first),
                repository("beta", &second),
                repository("alpha", &first),
            ],
            ..Default::default()
        };

        let inventory =
            build_artifact_ref_payload_inventory("commit", &context).unwrap();

        assert_eq!(inventory.truncated_payloads, 0);
        assert_eq!(inventory.payloads.len(), 3);
        assert_eq!(
            inventory
                .payloads
                .iter()
                .map(|row| row.label.as_str())
                .collect::<Vec<_>>(),
            vec!["newest", "middle", "oldest"]
        );
        assert_eq!(
            inventory
                .payloads
                .iter()
                .map(|row| row.rank)
                .collect::<Vec<_>>(),
            vec![Some(0), Some(1), Some(2)]
        );
        assert_eq!(
            inventory
                .payloads
                .iter()
                .map(|row| row.payload.as_str())
                .collect::<Vec<_>>(),
            vec![
                format!("alpha@{}", &new_sha[..ARTIFACT_REF_COMMIT_ABBREV]),
                format!("beta@{}", &middle_sha[..ARTIFACT_REF_COMMIT_ABBREV]),
                format!("alpha@{}", &old_sha[..ARTIFACT_REF_COMMIT_ABBREV]),
            ]
        );
        assert!(inventory.payloads.iter().all(|row| {
            row.detail.is_empty()
                && row.scope == row.payload.split_once('@').unwrap().0
        }));

        for row in &inventory.payloads {
            let parsed =
                crate::parse_artifact_ref(&format!("commit:{}", row.payload))
                    .unwrap();
            let ArtifactRefPayloadWire::Commit { repo, sha } = parsed.payload
            else {
                panic!("expected commit payload");
            };
            assert!(context
                .repositories
                .iter()
                .any(|entry| entry.name == repo));
            assert_eq!(sha.len(), ARTIFACT_REF_COMMIT_ABBREV);
        }

        let completion = artifact_completion_context("@commit:", 8, &context);
        let list = build_artifact_ref_payload_completion_candidates(
            completion.artifact_ref.as_ref().unwrap(),
            None,
            &context,
        );
        assert_eq!(list.candidates.len(), 3);
    }

    #[test]
    fn commit_inventory_keeps_non_sidecar_repository_kinds() {
        let temp = tempfile::tempdir().unwrap();
        let unclassified = temp.path().join("unclassified");
        let primary = temp.path().join("primary");
        let linked = temp.path().join("linked");
        let external = temp.path().join("external");
        let sidecar = temp.path().join("sidecar");
        init_git_repo(&unclassified);
        init_git_repo(&primary);
        init_git_repo(&linked);
        init_git_repo(&external);
        init_git_repo(&sidecar);
        let unclassified_sha =
            commit_at(&unclassified, 1_700_000_000, "unclassified", "");
        let primary_sha = commit_at(&primary, 1_700_000_100, "primary", "");
        let linked_sha = commit_at(&linked, 1_700_000_200, "linked", "");
        let external_sha = commit_at(&external, 1_700_000_300, "external", "");
        commit_at(&sidecar, 1_700_000_400, "sidecar", "");
        let context = ArtifactRefContextWire {
            repositories: vec![
                repository("unclassified", &unclassified),
                repository_with_kind("primary", "primary", &primary),
                repository_with_kind("linked", "linked", &linked),
                repository_with_kind("external", "external", &external),
                repository_with_kind(
                    "plans",
                    ARTIFACT_REF_REPOSITORY_KIND_SIDECAR,
                    &sidecar,
                ),
            ],
            ..Default::default()
        };

        let inventory =
            build_artifact_ref_payload_inventory("commit", &context).unwrap();

        assert_eq!(inventory.truncated_payloads, 0);
        assert_eq!(
            inventory
                .payloads
                .iter()
                .map(|row| row.payload.as_str())
                .collect::<Vec<_>>(),
            vec![
                format!(
                    "external@{}",
                    &external_sha[..ARTIFACT_REF_COMMIT_ABBREV]
                ),
                format!("linked@{}", &linked_sha[..ARTIFACT_REF_COMMIT_ABBREV]),
                format!(
                    "primary@{}",
                    &primary_sha[..ARTIFACT_REF_COMMIT_ABBREV]
                ),
                format!(
                    "unclassified@{}",
                    &unclassified_sha[..ARTIFACT_REF_COMMIT_ABBREV]
                ),
            ]
        );
        assert!(!inventory.payloads.iter().any(|row| row.scope == "plans"));
    }

    #[test]
    fn commit_inventory_is_empty_for_sidecar_only_context() {
        let temp = tempfile::tempdir().unwrap();
        let sidecar = temp.path().join("sidecar");
        init_git_repo(&sidecar);
        commit_at(&sidecar, 1_700_000_000, "sidecar", "");
        let context = ArtifactRefContextWire {
            repositories: vec![repository_with_kind(
                "plans", "sidecar", &sidecar,
            )],
            ..Default::default()
        };

        let inventory =
            build_artifact_ref_payload_inventory("commit", &context).unwrap();

        assert!(inventory.payloads.is_empty());
        assert_eq!(inventory.truncated_payloads, 0);
    }

    #[test]
    fn commit_inventory_skips_sidecars_before_reporting_the_row_cap() {
        let temp = tempfile::tempdir().unwrap();
        let mut repositories = Vec::new();
        for repo_index in
            0..(ARTIFACT_REF_COMMIT_MAX_ROWS / ARTIFACT_REF_COMMIT_SCAN_LIMIT)
        {
            let repo = temp.path().join(format!("code-{repo_index}"));
            init_git_repo(&repo);
            let subjects = (0..ARTIFACT_REF_COMMIT_SCAN_LIMIT)
                .map(|commit_index| format!("code {repo_index} {commit_index}"))
                .collect::<Vec<_>>();
            let commits = subjects
                .iter()
                .enumerate()
                .map(|(commit_index, subject)| {
                    let timestamp = 1_700_000_000
                        + (repo_index * ARTIFACT_REF_COMMIT_SCAN_LIMIT
                            + commit_index) as i64;
                    (timestamp, subject.as_str(), "")
                })
                .collect::<Vec<_>>();
            commit_batch(&repo, &commits);
            repositories.push(repository_with_kind(
                &format!("code-{repo_index}"),
                "human-code",
                &repo,
            ));
        }
        let sidecar = temp.path().join("sidecar");
        init_git_repo(&sidecar);
        commit_at(&sidecar, 1_800_000_000, "newer sidecar", "");
        repositories.push(repository_with_kind("plans", "sidecar", &sidecar));
        let context = ArtifactRefContextWire {
            repositories,
            ..Default::default()
        };

        let inventory =
            build_artifact_ref_payload_inventory("commit", &context).unwrap();

        assert_eq!(inventory.payloads.len(), ARTIFACT_REF_COMMIT_MAX_ROWS);
        assert_eq!(inventory.truncated_payloads, 0);
        assert!(!inventory.payloads.iter().any(|row| row.scope == "plans"));
        assert!(!inventory
            .payloads
            .iter()
            .any(|row| row.label == "newer sidecar"));
    }

    #[test]
    fn commit_inventory_preserves_subject_and_multiline_body() {
        let temp = tempfile::tempdir().unwrap();
        let repo = temp.path().join("repo");
        init_git_repo(&repo);
        let subject = "fix \"quoted\"\t日本語";
        let body = "first body line\nsecond\tline";
        commit_at(&repo, 1_700_000_000, subject, body);
        let context = ArtifactRefContextWire {
            repositories: vec![repository("sase-core", &repo)],
            ..Default::default()
        };

        let inventory =
            build_artifact_ref_payload_inventory("commit", &context).unwrap();

        assert_eq!(inventory.payloads.len(), 1);
        assert_eq!(inventory.payloads[0].label, subject);
        assert_eq!(inventory.payloads[0].body, body);
    }

    #[test]
    fn commit_inventory_enforces_the_per_repository_scan_limit() {
        let temp = tempfile::tempdir().unwrap();
        let repo = temp.path().join("repo");
        init_git_repo(&repo);
        let subjects = (0..=ARTIFACT_REF_COMMIT_SCAN_LIMIT)
            .map(|index| format!("commit {index}"))
            .collect::<Vec<_>>();
        let commits = subjects
            .iter()
            .enumerate()
            .map(|(index, subject)| {
                (1_700_000_000 + index as i64, subject.as_str(), "")
            })
            .collect::<Vec<_>>();
        commit_batch(&repo, &commits);
        let context = ArtifactRefContextWire {
            repositories: vec![repository("sase", &repo)],
            ..Default::default()
        };

        let inventory =
            build_artifact_ref_payload_inventory("commit", &context).unwrap();

        assert_eq!(inventory.payloads.len(), ARTIFACT_REF_COMMIT_SCAN_LIMIT);
        assert_eq!(inventory.payloads[0].label, "commit 200");
        assert_eq!(inventory.payloads.last().unwrap().label, "commit 1");
        assert!(!inventory.payloads.iter().any(|row| row.label == "commit 0"));
    }

    #[test]
    fn commit_inventory_skips_unusable_checkouts_and_bug_stays_empty() {
        let temp = tempfile::tempdir().unwrap();
        let missing = temp.path().join("missing");
        let not_git = temp.path().join("not-git");
        let empty_git = temp.path().join("empty-git");
        let populated_git = temp.path().join("populated-git");
        fs::create_dir_all(&not_git).unwrap();
        init_git_repo(&empty_git);
        init_git_repo(&populated_git);
        commit_at(&populated_git, 1_700_000_000, "hidden second path", "");
        let context = ArtifactRefContextWire {
            repositories: vec![
                repository("missing", &missing),
                repository("not-git", &not_git),
                repository("empty-git", &empty_git),
                repository("bad@repo", &populated_git),
                ArtifactRefRepositoryWire {
                    name: "first-only".to_string(),
                    checkout_paths: vec![
                        missing.to_string_lossy().into_owned(),
                        populated_git.to_string_lossy().into_owned(),
                    ],
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        assert!(build_artifact_ref_payload_inventory("commit", &context)
            .unwrap()
            .payloads
            .is_empty());
        assert_eq!(
            build_artifact_ref_payload_inventory("bug", &context).unwrap(),
            AtReferenceInventoryWire::default()
        );
    }

    #[test]
    fn commit_inventory_reports_the_merged_row_cap() {
        let commits = (0..ARTIFACT_REF_COMMIT_MAX_ROWS + 3)
            .map(|index| CommitCandidate {
                repository: "sase".to_string(),
                abbreviated_sha: format!("{index:012x}"),
                timestamp: 10_000 - index as i64,
                subject: format!("commit {index}"),
                body: String::new(),
            })
            .collect();
        let mut payloads = Vec::new();
        let mut seen = BTreeSet::new();

        let truncated = append_ranked_commit_candidates(
            &mut payloads,
            &mut seen,
            commits,
            10_000,
        );

        assert_eq!(payloads.len(), ARTIFACT_REF_COMMIT_MAX_ROWS);
        assert_eq!(truncated, 3);
        assert_eq!(payloads.last().unwrap().rank, Some(999));
    }

    #[test]
    fn commit_merge_ties_break_by_repository_then_sha() {
        let mut commits = vec![
            CommitCandidate {
                repository: "zeta".to_string(),
                abbreviated_sha: "000000000001".to_string(),
                timestamp: 100,
                subject: String::new(),
                body: String::new(),
            },
            CommitCandidate {
                repository: "alpha".to_string(),
                abbreviated_sha: "000000000002".to_string(),
                timestamp: 100,
                subject: String::new(),
                body: String::new(),
            },
            CommitCandidate {
                repository: "alpha".to_string(),
                abbreviated_sha: "000000000001".to_string(),
                timestamp: 100,
                subject: String::new(),
                body: String::new(),
            },
            CommitCandidate {
                repository: "zeta".to_string(),
                abbreviated_sha: "000000000002".to_string(),
                timestamp: 200,
                subject: String::new(),
                body: String::new(),
            },
        ];

        sort_commit_candidates(&mut commits);

        assert_eq!(
            commits
                .iter()
                .map(|commit| {
                    format!("{}@{}", commit.repository, commit.abbreviated_sha)
                })
                .collect::<Vec<_>>(),
            vec![
                "zeta@000000000002",
                "alpha@000000000001",
                "alpha@000000000002",
                "zeta@000000000001",
            ]
        );
    }

    #[test]
    fn commit_age_labels_match_prompt_bar_thresholds() {
        let now = 1_700_000_000;
        assert_eq!(commit_age_label(0, now), "");
        assert_eq!(commit_age_label(now + 1, now), "now");
        assert_eq!(commit_age_label(now - 59, now), "now");
        assert_eq!(commit_age_label(now - 60, now), "1m");
        assert_eq!(commit_age_label(now - 3_600, now), "1h");
        assert_eq!(commit_age_label(now - 86_400, now), "1d");
        assert_eq!(commit_age_label(now - 7 * 86_400, now), "2023-11-07");
    }

    /// Make `git` block forever inside this repository.
    ///
    /// A configured `include.path` pointing at a FIFO with no writer stalls
    /// git during start-up config parsing, which is deterministic in a way
    /// that a merely tiny budget is not: with a small budget the child can
    /// still win the race and exit before the first poll observes it.
    #[cfg(unix)]
    fn wedge_git_forever(repo: &Path) {
        use std::ffi::CString;

        let blocker = repo.join("blocker.fifo");
        let path = CString::new(blocker.as_os_str().as_encoded_bytes())
            .expect("fifo path should not contain a NUL byte");
        // SAFETY: `path` is a valid NUL-terminated string that outlives the
        // call, and the mode is a plain permission bitmask.
        let created = unsafe { libc::mkfifo(path.as_ptr(), 0o600) };
        assert_eq!(
            created,
            0,
            "mkfifo failed: {}",
            std::io::Error::last_os_error()
        );
        git(
            repo,
            &["config", "include.path", &blocker.to_string_lossy()],
        );
    }

    #[cfg(unix)]
    #[test]
    fn commit_log_reports_an_expired_budget_instead_of_empty_output() {
        let temp = tempfile::tempdir().unwrap();
        let healthy = temp.path().join("healthy");
        let wedged = temp.path().join("wedged");
        init_git_repo(&healthy);
        init_git_repo(&wedged);
        commit_at(&healthy, 1_700_000_000, "only", "");
        commit_at(&wedged, 1_700_000_000, "only", "");
        wedge_git_forever(&wedged);

        // This is the R6 mechanism in isolation: a `git log` that outlives the
        // budget is killed and the repository silently contributes zero rows.
        let budget = Duration::from_millis(250);
        let started = Instant::now();
        assert_eq!(
            commit_log_output(&wedged, budget),
            Err(CommitLogFailure::Budget)
        );
        assert!(started.elapsed() >= budget, "the budget was not honoured");

        // The row-producing path is unaffected by the new plumbing.
        assert!(!commit_log_output(
            &healthy,
            ARTIFACT_REF_COMMIT_TIMEOUT_DEFAULT
        )
        .expect("default budget should complete")
        .is_empty());
    }

    #[test]
    fn commit_log_distinguishes_every_unusable_repository_outcome() {
        let temp = tempfile::tempdir().unwrap();
        let broken_repo = temp.path().join("broken");
        fs::create_dir_all(&broken_repo).unwrap();
        // A malformed `.git` file makes git fail in place instead of walking
        // up to whatever repository happens to contain TMPDIR.
        fs::write(broken_repo.join(".git"), "not a gitfile\n").unwrap();

        assert_eq!(
            commit_log_output(
                &broken_repo,
                ARTIFACT_REF_COMMIT_TIMEOUT_DEFAULT
            ),
            Err(CommitLogFailure::ExitStatus)
        );

        let budget = Duration::from_secs(30);
        let cause =
            CommitLogIoCause::new(&io::Error::from(io::ErrorKind::NotFound));
        let descriptions = [
            CommitLogFailure::Scratch(ScratchStep::Create, cause),
            CommitLogFailure::Scratch(ScratchStep::Clone, cause),
            CommitLogFailure::Spawn(cause),
            CommitLogFailure::Budget,
            CommitLogFailure::Wait(cause),
            CommitLogFailure::ExitStatus,
            CommitLogFailure::Read(cause),
        ]
        .map(|failure| failure.describe(budget));
        assert!(descriptions.iter().all(|text| !text.is_empty()));
        assert_eq!(
            descriptions.iter().collect::<BTreeSet<_>>().len(),
            descriptions.len()
        );

        let budget_text = CommitLogFailure::Budget.describe(budget);
        assert!(budget_text.contains("30s"));
        assert!(budget_text.contains(ARTIFACT_REF_COMMIT_TIMEOUT_ENV));
    }

    #[cfg(unix)]
    #[test]
    fn commit_log_failures_report_the_underlying_os_error() {
        // The investigation behind this plumbing stalled because
        // `CommitLogFailure::Scratch` guessed at "check that TMPDIR exists and
        // is writable" while discarding the errno: EMFILE (descriptor
        // exhaustion) and ENOSPC (no space or inodes) both reach the same two
        // syscalls and were indistinguishable in the message.
        let budget = Duration::from_secs(30);
        let emfile =
            CommitLogIoCause::new(&io::Error::from_raw_os_error(libc::EMFILE));
        let enospc =
            CommitLogIoCause::new(&io::Error::from_raw_os_error(libc::ENOSPC));

        let create = CommitLogFailure::Scratch(ScratchStep::Create, emfile)
            .describe(budget);
        assert!(create.contains("os error 24"), "{create}");
        assert!(
            !create.contains("TMPDIR exists and is writable"),
            "the disproved TMPDIR guess should not have come back: {create}"
        );

        let full = CommitLogFailure::Scratch(ScratchStep::Create, enospc)
            .describe(budget);
        assert!(full.contains("os error 28"), "{full}");
        assert_ne!(create, full, "distinct errnos must read differently");

        // The `dup` call site is named separately from the `open` one.
        let clone = CommitLogFailure::Scratch(ScratchStep::Clone, emfile)
            .describe(budget);
        assert!(clone.contains("os error 24"), "{clone}");
        assert_ne!(create, clone);

        // A cause with no errno still renders something usable.
        let kind_only =
            CommitLogIoCause::new(&io::Error::from(io::ErrorKind::BrokenPipe));
        assert_eq!(kind_only.raw_os_error, None);
        assert!(!kind_only.describe().is_empty());
    }

    #[test]
    fn commit_timeout_override_accepts_only_positive_finite_seconds() {
        assert_eq!(
            parse_commit_timeout(Some("0.25")),
            Some(Duration::from_millis(250))
        );
        assert_eq!(
            parse_commit_timeout(Some("  120  ")),
            Some(Duration::from_secs(120))
        );
        for rejected in [
            None,
            Some(""),
            Some("0"),
            Some("-5"),
            Some("nan"),
            Some("inf"),
        ] {
            assert_eq!(parse_commit_timeout(rejected), None);
        }
        assert_eq!(
            ARTIFACT_REF_COMMIT_TIMEOUT_DEFAULT,
            Duration::from_secs(30)
        );
    }

    #[test]
    fn commit_timeout_reads_the_documented_environment_override() {
        // Deliberately not mutating the process environment: these tests share
        // it with every other test in the binary. Assert the wiring instead.
        assert_eq!(
            ARTIFACT_REF_COMMIT_TIMEOUT_ENV,
            "SASE_ARTIFACT_REF_COMMIT_TIMEOUT"
        );
        let observed = artifact_ref_commit_timeout();
        let expected = parse_commit_timeout(
            std::env::var(ARTIFACT_REF_COMMIT_TIMEOUT_ENV)
                .ok()
                .as_deref(),
        )
        .unwrap_or(ARTIFACT_REF_COMMIT_TIMEOUT_DEFAULT);
        assert_eq!(observed, expected);
    }

    #[test]
    fn builds_bead_payload_candidates_from_published_pages() {
        let temp = tempfile::tempdir().unwrap();
        let bead_root = temp.path().join("beads");
        fs::create_dir_all(bead_root.join("pages/sase-9z")).unwrap();
        fs::write(
            bead_root.join("pages/sase-9z/README.md"),
            "# Bead: sase-9z \u{2014} Root bead\n",
        )
        .unwrap();
        fs::write(
            bead_root.join("pages/sase-9z/sase-9z.1.md"),
            "# Bead: sase-9z.1 \u{2014} Phase bead\n",
        )
        .unwrap();
        fs::write(bead_root.join("pages/sase-9z/notes.txt"), "ignore").unwrap();
        let context = ArtifactRefContextWire {
            bead_stores: vec![ArtifactRefBeadStoreWire {
                project: "sase".to_string(),
                prefix: "sase".to_string(),
                root: bead_root.to_string_lossy().into_owned(),
            }],
            ..Default::default()
        };

        let completion =
            artifact_completion_context("@bead:sase-9z", 13, &context);
        let list = build_artifact_ref_payload_completion_candidates(
            completion.artifact_ref.as_ref().unwrap(),
            Some(completion.replacement_range),
            &context,
        );

        assert_eq!(
            list.candidates
                .iter()
                .map(|candidate| candidate.insertion.as_str())
                .collect::<Vec<_>>(),
            vec!["sase-9z", "sase-9z.1"]
        );
        assert_eq!(
            list.candidates
                .iter()
                .map(|candidate| candidate.name.as_str())
                .collect::<Vec<_>>(),
            vec!["Root bead", "Phase bead"]
        );
        assert!(list
            .candidates
            .iter()
            .all(
                |candidate| candidate.detail.as_deref() == Some("bead · sase")
            ));
    }

    #[test]
    fn builds_agent_payload_candidates_from_published_pages() {
        let temp = tempfile::tempdir().unwrap();
        let agent_root = temp.path().join("agents-sidecar");
        fs::create_dir_all(agent_root.join("agents/bbugyi200.athena.9w--code"))
            .unwrap();
        fs::create_dir_all(agent_root.join("agents/bbugyi200.athena.9w"))
            .unwrap();
        fs::create_dir_all(agent_root.join("agents/bbugyi200.athena.skip"))
            .unwrap();
        fs::write(
            agent_root.join("agents/bbugyi200.athena.9w--code/README.md"),
            "member",
        )
        .unwrap();
        fs::write(
            agent_root.join("agents/bbugyi200.athena.9w/README.md"),
            "agent",
        )
        .unwrap();
        let context = ArtifactRefContextWire {
            agent_roots: vec![ArtifactRefAgentRootWire {
                project: "sase".to_string(),
                root: agent_root.to_string_lossy().into_owned(),
            }],
            ..Default::default()
        };

        let text = "@agent:bbugyi200.athena.9w";
        let completion =
            artifact_completion_context(text, text.len(), &context);
        let list = build_artifact_ref_payload_completion_candidates(
            completion.artifact_ref.as_ref().unwrap(),
            Some(completion.replacement_range),
            &context,
        );

        assert_eq!(
            list.candidates
                .iter()
                .map(|candidate| candidate.insertion.as_str())
                .collect::<Vec<_>>(),
            vec!["bbugyi200.athena.9w", "bbugyi200.athena.9w--code"]
        );
        assert_eq!(
            list.candidates
                .iter()
                .map(|candidate| candidate.name.as_str())
                .collect::<Vec<_>>(),
            vec!["9w", "9w--code"]
        );
        assert!(list
            .candidates
            .iter()
            .all(
                |candidate| candidate.detail.as_deref() == Some("agent · sase")
            ));
    }

    #[test]
    fn agent_and_indexed_file_payloads_match_mid_name_fragments() {
        let temp = tempfile::tempdir().unwrap();
        let agent_root = temp.path().join("agents-sidecar");
        for name in [
            "bbugyi200.athena.sase-b3.5",
            "bbugyi200.athena.9w--code",
            "bbugyi200.athena.other",
        ] {
            fs::create_dir_all(agent_root.join("agents").join(name)).unwrap();
            fs::write(
                agent_root.join("agents").join(name).join("README.md"),
                "agent",
            )
            .unwrap();
        }
        let context = ArtifactRefContextWire {
            agent_roots: vec![ArtifactRefAgentRootWire {
                project: "sase".to_string(),
                root: agent_root.to_string_lossy().into_owned(),
            }],
            artifact_index_path: Some(
                temp.path()
                    .join("artifact-index.jsonl")
                    .to_string_lossy()
                    .into_owned(),
            ),
            ..Default::default()
        };
        fs::write(
            temp.path().join("artifact-index.jsonl"),
            "{\"schema_version\":1,\"artifact\":{\"id\":\"default:52895d68931185056fd0e49f\",\"path\":\"/tmp/panel.png\"}}\n",
        )
        .unwrap();

        for (text, expected) in [
            ("@agent:sase-b3", "bbugyi200.athena.sase-b3.5"),
            ("@file:931185", "default:52895d68931185056fd0e49f"),
        ] {
            let completion =
                artifact_completion_context(text, text.len(), &context);
            let list = build_artifact_ref_payload_completion_candidates(
                completion.artifact_ref.as_ref().unwrap(),
                None,
                &context,
            );
            assert_eq!(
                list.candidates
                    .iter()
                    .map(|candidate| candidate.insertion.as_str())
                    .collect::<Vec<_>>(),
                vec![expected],
                "{text}"
            );
        }
    }

    #[test]
    fn agent_prefix_query_survives_a_corpus_of_fuzzy_matches() {
        let temp = tempfile::tempdir().unwrap();
        let agent_root = temp.path().join("agents-sidecar");
        // Every name below fuzzy-matches "zq", but only the last one — sorted
        // last in walk order — matches it as a prefix.
        for index in
            0..super::super::at_reference::AT_REFERENCE_MAX_GROUP_ROWS + 5
        {
            let name = format!("aaz{index:04}q");
            fs::create_dir_all(agent_root.join("agents").join(&name)).unwrap();
            fs::write(
                agent_root.join("agents").join(&name).join("README.md"),
                "agent",
            )
            .unwrap();
        }
        fs::create_dir_all(agent_root.join("agents/zq-target")).unwrap();
        fs::write(agent_root.join("agents/zq-target/README.md"), "agent")
            .unwrap();
        let context = ArtifactRefContextWire {
            agent_roots: vec![ArtifactRefAgentRootWire {
                project: "sase".to_string(),
                root: agent_root.to_string_lossy().into_owned(),
            }],
            ..Default::default()
        };

        let text = "@agent:zq";
        let completion =
            artifact_completion_context(text, text.len(), &context);
        let list = build_artifact_ref_payload_completion_candidates(
            completion.artifact_ref.as_ref().unwrap(),
            None,
            &context,
        );

        assert_eq!(list.candidates[0].insertion, "zq-target");
    }

    #[test]
    fn payload_enumeration_is_bounded_and_deduplicated() {
        let temp = tempfile::tempdir().unwrap();
        let first = temp.path().join("first");
        let second = temp.path().join("second");
        fs::create_dir_all(&first).unwrap();
        fs::create_dir_all(&second).unwrap();
        for index in 0..205 {
            fs::write(first.join(format!("{index:03}.md")), "x").unwrap();
        }
        fs::write(second.join("000.md"), "duplicate").unwrap();
        fs::write(second.join("unique.md"), "unique").unwrap();
        let context = ArtifactRefContextWire {
            document_roots: vec![
                ArtifactRefDocumentRootWire {
                    kind: "designs".to_string(),
                    root: first.to_string_lossy().into_owned(),
                    path_globs: None,
                },
                ArtifactRefDocumentRootWire {
                    kind: "designs".to_string(),
                    root: second.to_string_lossy().into_owned(),
                    path_globs: None,
                },
            ],
            ..Default::default()
        };
        let completion = artifact_completion_context("@designs:", 9, &context);
        let list = build_artifact_ref_payload_completion_candidates(
            completion.artifact_ref.as_ref().unwrap(),
            None,
            &context,
        );

        assert_eq!(
            list.candidates.len(),
            super::super::at_reference::AT_REFERENCE_MAX_GROUP_ROWS
        );
        assert_eq!(
            list.candidates
                .iter()
                .filter(|candidate| candidate.insertion == "000.md")
                .count(),
            1
        );
    }

    #[test]
    fn payload_inventory_reaches_past_the_editor_display_cap() {
        let temp = tempfile::tempdir().unwrap();
        let designs = temp.path().join("designs");
        fs::create_dir_all(&designs).unwrap();
        for index in 0..205 {
            fs::write(designs.join(format!("{index:03}.md")), "x").unwrap();
        }
        fs::write(designs.join("zzz-needle.md"), "x").unwrap();
        let context = ArtifactRefContextWire {
            document_roots: vec![ArtifactRefDocumentRootWire {
                kind: "designs".to_string(),
                root: designs.to_string_lossy().into_owned(),
                path_globs: None,
            }],
            ..Default::default()
        };

        let inventory =
            build_artifact_ref_payload_inventory("designs", &context).unwrap();
        assert_eq!(inventory.payloads.len(), 206);
        assert_eq!(inventory.truncated_payloads, 0);

        let completion =
            artifact_completion_context("@designs:needle", 15, &context);
        let list = build_artifact_ref_payload_completion_candidates(
            completion.artifact_ref.as_ref().unwrap(),
            None,
            &context,
        );
        assert_eq!(list.candidates.len(), 1);
        assert_eq!(list.candidates[0].insertion, "zzz-needle.md");
    }

    #[test]
    fn payload_inventory_applies_document_root_path_globs() {
        let temp = tempfile::tempdir().unwrap();
        let designs = temp.path().join("designs");
        fs::create_dir_all(designs.join("allowed/private")).unwrap();
        fs::write(designs.join("allowed/keep.md"), "keep").unwrap();
        fs::write(designs.join("allowed/private/skip.md"), "skip").unwrap();
        fs::write(designs.join("other.md"), "other").unwrap();
        let context = ArtifactRefContextWire {
            document_roots: vec![ArtifactRefDocumentRootWire {
                kind: "designs".to_string(),
                root: designs.to_string_lossy().into_owned(),
                path_globs: Some(vec![
                    "allowed/**".to_string(),
                    "!allowed/private/**".to_string(),
                ]),
            }],
            ..Default::default()
        };

        let inventory =
            build_artifact_ref_payload_inventory("designs", &context).unwrap();

        assert_eq!(
            inventory
                .payloads
                .iter()
                .map(|row| row.payload.as_str())
                .collect::<Vec<_>>(),
            vec!["allowed/keep.md"]
        );
    }

    #[test]
    fn payload_inventory_discloses_the_scan_bound() {
        let temp = tempfile::tempdir().unwrap();
        let designs = temp.path().join("designs");
        fs::create_dir_all(&designs).unwrap();
        for index in 0..ARTIFACT_REF_MAX_SCAN_RESULTS + 1 {
            fs::write(designs.join(format!("{index:05}.md")), "").unwrap();
        }
        let context = ArtifactRefContextWire {
            document_roots: vec![ArtifactRefDocumentRootWire {
                kind: "designs".to_string(),
                root: designs.to_string_lossy().into_owned(),
                path_globs: None,
            }],
            ..Default::default()
        };

        let inventory =
            build_artifact_ref_payload_inventory("designs", &context).unwrap();

        assert_eq!(inventory.payloads.len(), ARTIFACT_REF_MAX_SCAN_RESULTS);
        assert_eq!(inventory.truncated_payloads, 1);
    }

    #[test]
    fn builds_chat_and_indexed_file_payloads_but_not_remote_kinds() {
        let temp = tempfile::tempdir().unwrap();
        let context = artifact_context(temp.path());
        fs::create_dir_all(temp.path().join("chats/202607")).unwrap();
        fs::write(temp.path().join("chats/202607/agent.md"), "chat").unwrap();
        fs::write(
            temp.path().join("artifact-index.jsonl"),
            "{\"schema_version\":1,\"artifact\":{\"id\":\"default:52895d68931185056fd0e49f\",\"path\":\"/tmp/panel-screenshot.png\"}}\n",
        )
        .unwrap();

        for (text, expected, expected_title) in [
            ("@chat:202607/a", "202607/agent.md", "agent.md"),
            (
                "@file:default:",
                "default:52895d68931185056fd0e49f",
                "panel-screenshot.png",
            ),
            (
                "@file:panel",
                "default:52895d68931185056fd0e49f",
                "panel-screenshot.png",
            ),
        ] {
            let completion =
                artifact_completion_context(text, text.len(), &context);
            let list = build_artifact_ref_payload_completion_candidates(
                completion.artifact_ref.as_ref().unwrap(),
                None,
                &context,
            );
            assert_eq!(list.candidates.len(), 1, "{text}");
            assert_eq!(list.candidates[0].insertion, expected, "{text}");
            assert_eq!(list.candidates[0].name, expected_title, "{text}");
        }

        for text in ["@commit:sase@0123456", "@bug:sase#1", "@unknown:value"] {
            let completion =
                artifact_completion_context(text, text.len(), &context);
            let list = build_artifact_ref_payload_completion_candidates(
                completion.artifact_ref.as_ref().unwrap(),
                None,
                &context,
            );
            assert!(list.candidates.is_empty(), "{text}: {list:?}");
        }
    }

    #[test]
    fn artifact_replacement_ranges_are_utf16_safe_and_at_paths_stay_references()
    {
        let temp = tempfile::tempdir().unwrap();
        let context = artifact_context(temp.path());
        let text = "é @designs:guidé.md";
        let completion =
            artifact_completion_context(text, text.len(), &context);
        assert_eq!(
            completion.replacement_range,
            EditorRange {
                start: EditorPosition {
                    line: 0,
                    character: 11,
                },
                end: EditorPosition {
                    line: 0,
                    character: 19,
                },
            }
        );

        let document = DocumentSnapshot::new("@src/foo");
        let context = classify_completion_context_with_artifacts_and_workflows(
            &document,
            pos(8),
            &entries(),
            &[],
            Some(&context),
        )
        .unwrap();
        assert_eq!(context.kind, CompletionContextKind::ArtifactRefKind);
        let trigger = context.artifact_ref.unwrap();
        assert_eq!(trigger.query, "src/foo");
        assert_eq!(trigger.candidate_span, (0, 8));
    }

    #[test]
    fn classifies_primary_completion_modes() {
        let catalog = entries();
        for (text, col, kind) in [
            ("#re", 3, CompletionContextKind::Xprompt),
            ("/ru", 3, CompletionContextKind::SlashSkill),
            ("./sr", 4, CompletionContextKind::FilePath),
            ("", 0, CompletionContextKind::FileHistory),
            ("%mo", 3, CompletionContextKind::DirectiveName),
            ("%model:", 7, CompletionContextKind::DirectiveArgument),
            ("foo", 3, CompletionContextKind::SnippetTrigger),
            ("foo_1", 5, CompletionContextKind::SnippetTrigger),
        ] {
            let doc = DocumentSnapshot::new(text);
            let context =
                classify_completion_context(&doc, pos(col), &catalog).unwrap();
            assert_eq!(context.kind, kind, "{text}");
        }
    }

    #[test]
    fn placeholder_context_precedes_other_explicit_completion_modes() {
        let catalog = entries();
        let workflow_names = vec!["gh".to_string()];
        for (text, col) in [
            ("<", 1),
            ("%model:<", 8),
            ("#review(path=<", 14),
            ("#gh:<", 5),
        ] {
            let document = DocumentSnapshot::new(text);
            let context = classify_completion_context_with_workflows(
                &document,
                pos(col),
                &catalog,
                &workflow_names,
            )
            .unwrap();
            assert_eq!(
                context.kind,
                CompletionContextKind::Placeholder,
                "{text}"
            );
            assert_eq!(context.token.as_ref().unwrap().text, "", "{text}");
        }
    }

    #[test]
    fn closed_placeholder_does_not_steal_following_context() {
        let document = DocumentSnapshot::new("<done> %mo");
        let context =
            classify_completion_context(&document, pos(10), &entries())
                .unwrap();
        assert_eq!(context.kind, CompletionContextKind::DirectiveName);
    }

    #[test]
    fn effort_and_auto_directive_arguments_classify_with_candidates() {
        let catalog = entries();
        let cases: Vec<(&str, u32, &str, &str, Vec<&str>)> = vec![
            ("%effort:", 8, "effort", "", EFFORT_LEVELS_ORDERED.to_vec()),
            (
                "%effort:xh",
                10,
                "effort",
                "xh",
                EFFORT_LEVELS_ORDERED.to_vec(),
            ),
            // The `%e` alias classifies under the canonical `effort` context.
            ("%e:", 3, "effort", "", EFFORT_LEVELS_ORDERED.to_vec()),
            ("%e:xh", 5, "effort", "xh", EFFORT_LEVELS_ORDERED.to_vec()),
            ("%auto:", 6, "auto", "", vec!["plan", "tale", "epic"]),
            ("%auto:t", 7, "auto", "t", vec!["plan", "tale", "epic"]),
        ];

        for (text, col, directive_name, token, expected_values) in cases {
            let doc = DocumentSnapshot::new(text);
            let context =
                classify_completion_context(&doc, pos(col), &catalog).unwrap();
            assert_eq!(context.kind, CompletionContextKind::DirectiveArgument);
            assert_eq!(context.directive_name.as_deref(), Some(directive_name));
            assert_eq!(context.token.as_ref().unwrap().text, token);

            let candidates =
                directive_argument_candidates(directive_name).candidates;
            let values: Vec<&str> =
                candidates.iter().map(|c| c.insertion.as_str()).collect();
            assert_eq!(values, expected_values, "{text}");
        }
    }

    #[test]
    fn directive_keyword_completion_targets_only_the_post_comma_fragment() {
        let catalog = entries();
        for (text, cursor, expected_start, directive_name, keyword) in [
            ("%clan(research, tr)", 18, 16, "clan", "tribe="),
            ("%c(research, tr)", 15, 13, "clan", "tribe="),
            ("%clan(research, su)", 18, 16, "clan", "summary="),
            ("%clan(research, su)", 18, 16, "clan", "summary_script="),
            ("%id(worker, cl)", 14, 12, "id", "clan="),
            ("%i(worker, cl)", 13, 11, "id", "clan="),
            ("%id(worker, fa)", 14, 12, "id", "family="),
            ("%i(worker, fa)", 13, 11, "id", "family="),
            ("%id(worker, tr)", 14, 12, "id", "tribe="),
            ("%i(worker, tr)", 13, 11, "id", "tribe="),
        ] {
            let doc = DocumentSnapshot::new(text);
            let context =
                classify_completion_context(&doc, pos(cursor), &catalog)
                    .expect("directive keyword completion context");
            assert_eq!(
                context.kind,
                CompletionContextKind::DirectiveArgumentKeyword,
                "{text}"
            );
            assert_eq!(context.directive_name.as_deref(), Some(directive_name));
            assert_eq!(context.token.as_ref().unwrap().text, &keyword[..2]);
            assert_eq!(
                context.replacement_range,
                doc.byte_range_to_range(expected_start, cursor as usize)
                    .unwrap()
            );

            let candidates =
                directive_argument_candidates(directive_name).candidates;
            assert!(
                candidates
                    .iter()
                    .any(|candidate| candidate.insertion == keyword),
                "missing {keyword} candidate for {text}: {candidates:?}"
            );
        }
    }

    #[test]
    fn directive_keyword_completion_stays_out_of_positional_and_value_positions(
    ) {
        let catalog = entries();
        for (open, closed_text, directive_name) in [
            ("%clan(re", "%clan(research, tribe=blue)", "clan"),
            ("%id(wo", "%id(worker, clan=research)", "id"),
        ] {
            let doc = DocumentSnapshot::new(open);
            let positional_context = classify_completion_context(
                &doc,
                pos(open.len() as u32),
                &catalog,
            )
            .unwrap();
            assert_eq!(
                positional_context.kind,
                CompletionContextKind::DirectiveArgument
            );
            assert_eq!(
                positional_context.directive_name.as_deref(),
                Some(directive_name)
            );

            let value = if directive_name == "clan" {
                "blue"
            } else {
                "research"
            };
            let value_start = closed_text.find(value).unwrap();
            let doc = DocumentSnapshot::new(closed_text);
            let value_context = classify_completion_context(
                &doc,
                pos((closed_text.len() - 1) as u32),
                &catalog,
            )
            .unwrap();
            assert_eq!(
                value_context.kind,
                CompletionContextKind::DirectiveArgumentValue
            );
            assert_eq!(
                value_context.directive_name.as_deref(),
                Some(directive_name)
            );
            assert_eq!(value_context.token.as_ref().unwrap().text, value);
            assert_eq!(
                value_context.replacement_range,
                doc.byte_range_to_range(value_start, closed_text.len() - 1)
                    .unwrap()
            );

            let closed = DocumentSnapshot::new(closed_text);
            let closed_context = classify_completion_context(
                &closed,
                pos(closed.text().len() as u32),
                &catalog,
            );
            assert!(!closed_context.is_some_and(|context| matches!(
                context.kind,
                CompletionContextKind::DirectiveArgument
                    | CompletionContextKind::DirectiveArgumentKeyword
                    | CompletionContextKind::DirectiveArgumentValue
            )));
        }
    }

    #[test]
    fn builds_catalog_completions_with_marker_filters() {
        let catalog = entries();
        let inline = build_xprompt_completion_candidates("#r", None, &catalog);
        assert_eq!(
            inline
                .candidates
                .iter()
                .map(|c| c.insertion.as_str())
                .collect::<Vec<_>>(),
            vec!["#review", "#!run"]
        );

        let standalone =
            build_xprompt_completion_candidates("#!r", None, &catalog);
        assert_eq!(standalone.candidates[0].insertion, "#!run");

        // Slash completion offers the provider skill name, never the
        // namespaced xprompt reference, and a non-skill workflow that happens
        // to share the prefix is not a slash candidate.
        let skill = build_xprompt_completion_candidates("/p", None, &catalog);
        assert_eq!(
            skill
                .candidates
                .iter()
                .map(|c| c.insertion.as_str())
                .collect::<Vec<_>>(),
            vec!["/plan"]
        );
        assert!(build_xprompt_completion_candidates("/r", None, &catalog)
            .candidates
            .is_empty());

        // The same skill is reachable inline only through `#skill/plan`.
        let namespaced =
            build_xprompt_completion_candidates("#skill/", None, &catalog);
        assert_eq!(
            namespaced
                .candidates
                .iter()
                .map(|c| c.insertion.as_str())
                .collect::<Vec<_>>(),
            vec!["#skill/plan"]
        );
        assert!(build_xprompt_completion_candidates(
            "#skills/", None, &catalog
        )
        .candidates
        .is_empty());
        assert!(build_xprompt_completion_candidates("#plan", None, &catalog)
            .candidates
            .is_empty());
    }

    #[test]
    fn memory_completes_only_through_the_memory_namespace() {
        let catalog = entries();

        let namespaced =
            build_xprompt_completion_candidates("#memory/", None, &catalog);
        assert_eq!(
            namespaced
                .candidates
                .iter()
                .map(|c| c.insertion.as_str())
                .collect::<Vec<_>>(),
            vec!["#memory/glossary"]
        );
        // No bare alias exists, and a memory note is never a slash skill.
        assert!(build_xprompt_completion_candidates(
            "#glossary",
            None,
            &catalog
        )
        .candidates
        .is_empty());
        assert!(build_xprompt_completion_candidates(
            "/glossary",
            None,
            &catalog
        )
        .candidates
        .is_empty());
        // The tier survives the catalog-to-assist projection.
        let entry = catalog
            .iter()
            .find(|entry| entry.name == "memory/glossary")
            .unwrap();
        assert_eq!(entry.memory_type, Some(MemoryTierWire::Short));
    }

    #[test]
    fn snippet_context_does_not_steal_higher_priority_tokens() {
        let catalog = entries();
        for text in ["#foo", "/foo", "@foo", "%model", "./foo", ""] {
            let doc = DocumentSnapshot::new(text);
            let context = classify_completion_context(
                &doc,
                pos(text.len() as u32),
                &catalog,
            );
            assert_ne!(
                context.map(|context| context.kind),
                Some(CompletionContextKind::SnippetTrigger),
                "{text}"
            );
        }
    }

    #[test]
    fn builds_snippet_completions_by_case_insensitive_prefix() {
        let list = build_snippet_completion_candidates(
            "fo",
            None,
            &[
                snippet_entry("Foo", "body $1$0", "ace.snippets"),
                snippet_entry("bar", "bar", "xprompt"),
            ],
        );

        assert_eq!(list.candidates.len(), 1);
        assert_eq!(list.candidates[0].display, "Foo");
        assert_eq!(list.candidates[0].insertion, "body $1$0");
        assert_eq!(list.candidates[0].detail.as_deref(), Some("ace.snippets"));
    }

    fn snippet_entry(
        trigger: &str,
        template: &str,
        source: &str,
    ) -> EditorSnippetEntryWire {
        EditorSnippetEntryWire {
            trigger: trigger.to_string(),
            template: template.to_string(),
            source: source.to_string(),
            xprompt_name: None,
            description: None,
            source_path_display: None,
        }
    }

    #[test]
    fn detects_narrow_argument_contexts() {
        let catalog = entries();
        for (text, col, kind, active_input) in [
            (
                "#review:",
                8,
                CompletionContextKind::XpromptArgumentPath,
                Some("path"),
            ),
            (
                "#review(path=",
                13,
                CompletionContextKind::XpromptArgumentPath,
                Some("path"),
            ),
            (
                "#review(de",
                10,
                CompletionContextKind::XpromptArgumentName,
                None,
            ),
            (
                "#review!!:",
                10,
                CompletionContextKind::XpromptArgumentPath,
                Some("path"),
            ),
        ] {
            let doc = DocumentSnapshot::new(text);
            let context =
                classify_completion_context(&doc, pos(col), &catalog).unwrap();
            assert_eq!(context.kind, kind, "{text}");
            assert_eq!(context.active_input.as_deref(), active_input);
        }

        let doc = DocumentSnapshot::new("#ns__foo(arg=");
        let ns_entry = XpromptAssistEntry {
            name: "ns/foo".to_string(),
            display_label: "ns/foo".to_string(),
            insertion: "#ns/foo".to_string(),
            reference_prefix: "#".to_string(),
            kind: None,
            source_bucket: "project".to_string(),
            project: None,
            tags: Vec::new(),
            input_signature: None,
            inputs: vec![XpromptInputHint {
                name: "arg".to_string(),
                r#type: "word".to_string(),
                description: None,
                required: true,
                default_display: None,
                position: 0,
                repeatable: false,
            }],
            content_preview: None,
            description: None,
            skill_name: None,
            memory_type: None,
            source_path_display: None,
            definition_path: None,
            definition_range: None,
            is_skill: false,
        };
        assert!(
            classify_completion_context(&doc, pos(13), &[ns_entry]).is_some()
        );
    }

    #[test]
    fn repeatable_positionals_keep_the_tail_input_and_active_element_range() {
        let mut fork = entries()[0].clone();
        fork.name = "fork".to_string();
        fork.display_label = "fork".to_string();
        fork.insertion = "#fork".to_string();
        fork.inputs = vec![XpromptInputHint {
            name: "names".to_string(),
            r#type: "agent".to_string(),
            description: None,
            required: false,
            default_display: None,
            position: 0,
            repeatable: true,
        }];

        for text in ["😀 #fork:planner,co", "😀 #fork(planner, co"] {
            let doc = DocumentSnapshot::new(text);
            let cursor = doc.byte_offset_to_position(text.len()).unwrap();
            let context =
                classify_completion_context(&doc, cursor, &[fork.clone()])
                    .unwrap();
            assert_eq!(
                context.kind,
                CompletionContextKind::XpromptArgumentAgent
            );
            assert_eq!(context.active_input.as_deref(), Some("names"));
            let token_start = text.rfind("co").unwrap();
            assert_eq!(
                context.replacement_range,
                doc.byte_range_to_range(token_start, text.len()).unwrap()
            );
            assert_eq!(context.selected_values, vec!["planner"]);
        }
    }

    #[test]
    fn repeatable_agent_context_replaces_earlier_element_and_filters_selected()
    {
        let mut fork = entries()[0].clone();
        fork.name = "fork".to_string();
        fork.inputs = vec![XpromptInputHint {
            name: "names".to_string(),
            r#type: "agent".to_string(),
            description: None,
            required: false,
            default_display: None,
            position: 0,
            repeatable: true,
        }];
        let text = "😀 #fork(co, planner)";
        let doc = DocumentSnapshot::new(text);
        let cursor = doc
            .byte_offset_to_position(text.find("co").unwrap() + 2)
            .unwrap();
        let context =
            classify_completion_context(&doc, cursor, &[fork]).unwrap();
        assert_eq!(context.kind, CompletionContextKind::XpromptArgumentAgent);
        assert_eq!(context.selected_values, vec!["planner"]);
        assert_eq!(
            context.replacement_range,
            doc.byte_range_to_range(
                text.find("co").unwrap(),
                text.find(", planner").unwrap(),
            )
            .unwrap()
        );

        let entries = vec![
            AgentCompletionEntry {
                name: "planner".to_string(),
                status: "RUNNING".to_string(),
                project: "sase".to_string(),
                kind: String::new(),
                member_count: 0,
                detail: String::new(),
                documentation: String::new(),
            },
            AgentCompletionEntry {
                name: "coder".to_string(),
                status: "DONE".to_string(),
                project: "sase-core".to_string(),
                kind: String::new(),
                member_count: 0,
                detail: String::new(),
                documentation: String::new(),
            },
            AgentCompletionEntry {
                name: "reviewer.@".to_string(),
                status: "DONE".to_string(),
                project: "sase".to_string(),
                kind: String::new(),
                member_count: 0,
                detail: String::new(),
                documentation: String::new(),
            },
        ];
        let list = build_agent_completion_candidates(
            "",
            Some(context.replacement_range),
            &entries,
            &context.selected_values,
        );
        assert_eq!(
            list.candidates
                .iter()
                .map(|candidate| candidate.name.as_str())
                .collect::<Vec<_>>(),
            vec!["coder", "reviewer.@"]
        );
        assert_eq!(
            list.candidates[0].detail.as_deref(),
            Some("DONE · sase-core")
        );
    }

    #[test]
    fn agent_candidates_are_kind_aware_ordered_and_compatible() {
        let old_entry: AgentCompletionEntry =
            serde_json::from_value(serde_json::json!({
                "name": "legacy",
                "status": "DONE",
                "project": "sase"
            }))
            .unwrap();
        assert_eq!(old_entry.kind, "");

        let entries = vec![
            old_entry,
            agent_target("review", "agent", 1, "DONE · sase"),
            agent_target("review", "family", 3, "family · 3 members"),
            agent_target("builders", "clan", 2, "clan · 2 members"),
            agent_target("@reviewers", "tribe", 4, "tribe · 4 agents"),
        ];
        let list = build_agent_completion_candidates("", None, &entries, &[]);
        assert_eq!(
            list.candidates
                .iter()
                .map(|candidate| (
                    candidate.kind.as_str(),
                    candidate.insertion.as_str()
                ))
                .collect::<Vec<_>>(),
            vec![
                ("tribe", "@reviewers"),
                ("clan", "builders"),
                ("family", "review"),
                ("agent", "legacy"),
            ]
        );

        let bare_tribe =
            build_agent_completion_candidates("rev", None, &entries, &[]);
        assert_eq!(bare_tribe.candidates[0].insertion, "@reviewers");
        assert_eq!(bare_tribe.candidates[0].name, "reviewers");
        let sigil_tribe =
            build_agent_completion_candidates("@rev", None, &entries, &[]);
        assert_eq!(sigil_tribe.candidates[0].insertion, "@reviewers");
        assert_eq!(sigil_tribe.candidates[0].name, "@reviewers");
    }

    #[test]
    fn agent_candidates_carry_documentation_only_when_present() {
        let mut documented =
            agent_target("review", "family", 3, "family · 3 members");
        documented.documentation = "# review\n\nplan preview".to_string();
        let bare = agent_target("builders", "clan", 2, "clan · 2 members");
        let entries = vec![documented, bare];

        let list = build_agent_completion_candidates("", None, &entries, &[]);
        let documentation = |name: &str| {
            list.candidates
                .iter()
                .find(|candidate| candidate.insertion == name)
                .and_then(|candidate| candidate.documentation.clone())
        };
        assert_eq!(
            documentation("review"),
            Some("# review\n\nplan preview".to_string())
        );
        assert_eq!(documentation("builders"), None);
    }

    #[test]
    fn wait_candidates_merge_keywords_and_exclude_selected_values() {
        let entries = vec![
            agent_target("worker", "agent", 1, "RUNNING · sase"),
            agent_target("review", "family", 2, "family · 2 members"),
            agent_target("builders", "clan", 3, "clan · 3 members"),
            agent_target("@ops", "tribe", 4, "tribe · 4 agents"),
        ];
        let all = build_wait_completion_candidates("", None, &entries, &[]);
        assert_eq!(
            all.candidates
                .iter()
                .map(|candidate| candidate.insertion.as_str())
                .collect::<Vec<_>>(),
            vec![
                "bead=",
                "priority=",
                "runners=",
                "time=",
                "@ops",
                "builders",
                "review",
                "worker"
            ]
        );

        let selected = vec!["time=5m".to_string(), "builders".to_string()];
        let narrowed =
            build_wait_completion_candidates("", None, &entries, &selected);
        assert_eq!(
            narrowed
                .candidates
                .iter()
                .map(|candidate| candidate.insertion.as_str())
                .collect::<Vec<_>>(),
            vec!["bead=", "priority=", "runners=", "@ops", "review", "worker"]
        );

        let colon = build_wait_completion_candidates_for_form(
            "t",
            None,
            &entries,
            &[],
            DirectiveSyntaxForm::Colon,
        );
        assert_eq!(
            colon
                .candidates
                .iter()
                .map(|candidate| candidate.insertion.as_str())
                .collect::<Vec<_>>(),
            Vec::<&str>::new()
        );
    }

    #[test]
    fn wait_context_narrows_to_active_clause_and_tracks_selected_values() {
        for text in [
            "%wait:planner,@ops, bu",
            "%wait(planner, @ops, bu",
            "%w(planner, @ops, bu",
        ] {
            let doc = DocumentSnapshot::new(text);
            let context = classify_completion_context(
                &doc,
                pos(text.len() as u32),
                &entries(),
            )
            .expect("wait completion context");
            let token_start = text.rfind("bu").unwrap();
            assert_eq!(context.kind, CompletionContextKind::DirectiveArgument);
            assert_eq!(context.directive_name.as_deref(), Some("wait"));
            assert_eq!(context.token.as_ref().unwrap().text, "bu");
            assert_eq!(
                context.replacement_range,
                doc.byte_range_to_range(token_start, text.len()).unwrap()
            );
            assert_eq!(context.selected_values, vec!["planner", "@ops"]);
        }

        for text in [
            "%wait:planner,@ops,builders",
            "%wait(planner, @ops, builders)",
        ] {
            let cursor = text.find("pl").unwrap() + 2;
            let doc = DocumentSnapshot::new(text);
            let context = classify_completion_context(
                &doc,
                pos(cursor as u32),
                &entries(),
            )
            .expect("earlier wait clause completion context");
            assert_eq!(context.token.as_ref().unwrap().text, "pl");
            assert_eq!(context.selected_values, vec!["@ops", "builders"]);
            assert_eq!(
                context.replacement_range,
                doc.byte_range_to_range(
                    text.find("pl").unwrap(),
                    text.find("planner").unwrap() + "planner".len(),
                )
                .unwrap()
            );
        }
    }

    #[test]
    fn model_at_suffix_completes_effort_vocabulary() {
        let catalog = entries();

        // Right after the `@`, the context targets the effort vocabulary.
        let doc = DocumentSnapshot::new("%model:opus@");
        let context =
            classify_completion_context(&doc, pos(12), &catalog).unwrap();
        assert_eq!(context.kind, CompletionContextKind::DirectiveArgument);
        assert_eq!(context.directive_name.as_deref(), Some("effort"));
        assert_eq!(context.token.as_ref().unwrap().text, "");

        // A partially-typed level keeps the effort context; the token after the
        // `@` is what the editor filters the effort vocabulary against.
        let doc = DocumentSnapshot::new("%model:opus@xh");
        let context =
            classify_completion_context(&doc, pos(14), &catalog).unwrap();
        assert_eq!(context.directive_name.as_deref(), Some("effort"));
        assert_eq!(context.token.as_ref().unwrap().text, "xh");

        // Before the `@`, it is still the model argument.
        let doc = DocumentSnapshot::new("%model:opus");
        let context =
            classify_completion_context(&doc, pos(11), &catalog).unwrap();
        assert_eq!(context.directive_name.as_deref(), Some("model"));

        // Provider-qualified models keep the slash-bearing model token.
        let doc = DocumentSnapshot::new("%model:claude/");
        let context =
            classify_completion_context(&doc, pos(14), &catalog).unwrap();
        assert_eq!(context.directive_name.as_deref(), Some("model"));
        assert_eq!(context.token.as_ref().unwrap().text, "claude/");

        let doc = DocumentSnapshot::new("%model:claude/opus@");
        let context =
            classify_completion_context(&doc, pos(19), &catalog).unwrap();
        assert_eq!(context.directive_name.as_deref(), Some("effort"));
        assert_eq!(context.token.as_ref().unwrap().text, "");

        // A leading `@` is the alias marker, not an effort suffix.
        let doc = DocumentSnapshot::new("%model:@oth");
        let context =
            classify_completion_context(&doc, pos(11), &catalog).unwrap();
        assert_eq!(context.directive_name.as_deref(), Some("model"));
        assert_eq!(context.token.as_ref().unwrap().text, "@oth");

        let doc = DocumentSnapshot::new("%model:@");
        let context =
            classify_completion_context(&doc, pos(8), &catalog).unwrap();
        assert_eq!(context.directive_name.as_deref(), Some("model"));
        assert_eq!(context.token.as_ref().unwrap().text, "@");
    }

    #[test]
    fn builds_argument_name_completions() {
        let catalog = entries();
        let list = build_xprompt_arg_name_candidates(
            &catalog[0],
            &BTreeSet::from(["path".to_string()]),
            "d",
            None,
        );
        assert_eq!(list.candidates[0].insertion, "deep=");
        assert_eq!(
            list.candidates[0].documentation.as_deref(),
            Some("Run a deeper pass\n\ndefault: false")
        );
    }

    // --- vcs_repo (`#gh:owner/`) completion -------------------------------

    const VCS_REPO_CURSOR: &str = "<CURSOR>";

    fn workflow_names(names: &[&str]) -> Vec<String> {
        names.iter().map(|name| (*name).to_string()).collect()
    }

    fn gh_entry() -> XpromptAssistEntry {
        XpromptAssistEntry {
            name: "gh".to_string(),
            display_label: "gh".to_string(),
            insertion: "#gh".to_string(),
            reference_prefix: "#".to_string(),
            kind: Some("workflow".to_string()),
            source_bucket: "builtin".to_string(),
            project: None,
            tags: Vec::new(),
            input_signature: Some("(gh_ref: word)".to_string()),
            inputs: vec![XpromptInputHint {
                name: "gh_ref".to_string(),
                r#type: "word".to_string(),
                description: None,
                required: true,
                default_display: None,
                position: 0,
                repeatable: false,
            }],
            content_preview: None,
            description: None,
            skill_name: None,
            memory_type: None,
            source_path_display: None,
            definition_path: None,
            definition_range: None,
            is_skill: false,
        }
    }

    fn repo_entry(name: &str, full_ref: &str) -> VcsRepoEntry {
        VcsRepoEntry {
            name: name.to_string(),
            r#ref: full_ref.to_string(),
            description: format!("{name} repo"),
            visibility: "public".to_string(),
            is_fork: false,
            is_archived: false,
            pushed_at: None,
        }
    }

    fn vcs_repo_context(
        text: &str,
        cursor: usize,
        names: &[&str],
    ) -> CompletionContext {
        let doc = DocumentSnapshot::new(text);
        let position = doc.byte_offset_to_position(cursor).unwrap();
        detect_vcs_repo_context_at_position(
            &doc,
            position,
            &workflow_names(names),
        )
        .unwrap_or_else(|| panic!("expected repo context for {text:?}"))
    }

    fn apply_text_edit(text: &str, edit: &EditorTextEdit) -> String {
        let doc = DocumentSnapshot::new(text);
        let start = doc.position_to_byte_offset(edit.range.start).unwrap();
        let end = doc.position_to_byte_offset(edit.range.end).unwrap();
        format!("{}{}{}", &text[..start], edit.new_text, &text[end..])
    }

    #[test]
    fn vcs_repo_golden_vectors() {
        // The cross-language parity contract -- identical to the Python
        // `VCS_REPO_GOLDEN_VECTORS` table. `<CURSOR>` marks the cursor.
        let cases = [
            (
                "#gh:bbugyi200/<CURSOR>",
                vec!["gh"],
                "bbugyi200/sase",
                "#gh:bbugyi200/sase ",
            ),
            (
                "#gh:bbugyi200/sa<CURSOR>",
                vec!["gh"],
                "bbugyi200/sase",
                "#gh:bbugyi200/sase ",
            ),
            (
                "Fix #gh:bbugyi200/sa<CURSOR> now",
                vec!["gh"],
                "bbugyi200/sase",
                "Fix #gh:bbugyi200/sase now",
            ),
            (
                "#gh!!:bbugyi200/sa<CURSOR>",
                vec!["gh"],
                "bbugyi200/sase",
                "#gh!!:bbugyi200/sase ",
            ),
            (
                "#gh(bbugyi200/sa<CURSOR>",
                vec!["gh"],
                "bbugyi200/sase",
                "#gh(bbugyi200/sase)",
            ),
            (
                "#gh(bbugyi200/sa<CURSOR>) next",
                vec!["gh"],
                "bbugyi200/sase",
                "#gh(bbugyi200/sase) next",
            ),
            (
                "#gh??(bbugyi200/<CURSOR>",
                vec!["gh"],
                "bbugyi200/sase",
                "#gh??(bbugyi200/sase)",
            ),
            (
                "#gl:group/sub/re<CURSOR>",
                vec!["gl"],
                "group/sub/repo",
                "#gl:group/sub/repo ",
            ),
            (
                "#gh:bbugyi200/s<CURSOR>asex",
                vec!["gh"],
                "bbugyi200/sase",
                "#gh:bbugyi200/sase ",
            ),
            (
                "#gh:bbugyi200/sa<CURSOR>\n",
                vec!["gh"],
                "bbugyi200/sase",
                "#gh:bbugyi200/sase \n",
            ),
        ];

        for (marked, names, selected_ref, expected) in cases {
            let cursor = marked.find(VCS_REPO_CURSOR).unwrap();
            let text = marked.replace(VCS_REPO_CURSOR, "");
            let context = vcs_repo_context(&text, cursor, &names);
            let trigger = context.vcs_repo.as_ref().unwrap();
            assert_eq!(
                apply_vcs_repo_selection(&text, trigger, selected_ref),
                expected,
                "{marked}"
            );
        }
    }

    #[test]
    fn detects_vcs_repo_colon_spans() {
        let context = vcs_repo_context("#gh:bbugyi200/sa", 16, &["gh"]);
        let trigger = context.vcs_repo.as_ref().unwrap();

        assert_eq!(context.kind, CompletionContextKind::VcsRepo);
        assert_eq!(trigger.workflow, "gh");
        assert_eq!(trigger.separator, ":");
        assert_eq!(trigger.namespace, "bbugyi200");
        assert_eq!(trigger.query, "sa");
        assert_eq!((trigger.ref_start, trigger.ref_end), (4, 16));
        assert_eq!(trigger.namespace_span, (4, 13));
        assert_eq!(trigger.query_span, (14, 16));
        assert_eq!(context.replacement_range.start, pos(4));
        assert_eq!(context.replacement_range.end, pos(16));
    }

    #[test]
    fn detects_vcs_repo_paren_hitl_and_nested_namespaces() {
        let context = vcs_repo_context("#gh??(bbugyi200/sa", 18, &["gh"]);
        let trigger = context.vcs_repo.as_ref().unwrap();
        assert_eq!(trigger.workflow, "gh");
        assert_eq!(trigger.separator, "(");
        assert_eq!(trigger.namespace, "bbugyi200");
        assert_eq!(trigger.query, "sa");

        let context = vcs_repo_context("#gl:group/subgroup/sa", 21, &["gl"]);
        let trigger = context.vcs_repo.as_ref().unwrap();
        assert_eq!(trigger.namespace, "group/subgroup");
        assert_eq!(trigger.query, "sa");
    }

    #[test]
    fn classifies_vcs_repo_then_vcs_ref_before_xprompt_argument_hints() {
        let catalog = vec![gh_entry()];
        let names = workflow_names(&["gh"]);

        let doc = DocumentSnapshot::new("#gh:bbugyi200/");
        let context = classify_completion_context_with_workflows(
            &doc,
            pos(14),
            &catalog,
            &names,
        )
        .unwrap();
        assert_eq!(context.kind, CompletionContextKind::VcsRepo);

        let doc = DocumentSnapshot::new("#gh:bbugyi200");
        let context = classify_completion_context_with_workflows(
            &doc,
            pos(13),
            &catalog,
            &names,
        )
        .unwrap();
        assert_eq!(context.kind, CompletionContextKind::VcsRef);
        assert_eq!(context.active_xprompt.as_deref(), None);

        let doc = DocumentSnapshot::new("#foo:bbugyi200/");
        let context = classify_completion_context_with_workflows(
            &doc,
            pos(15),
            &[],
            &names,
        );
        assert_ne!(
            context.map(|context| context.kind),
            Some(CompletionContextKind::VcsRepo)
        );
    }

    #[test]
    fn vcs_repo_trigger_negatives() {
        for prompt in [
            "#gh:bbugyi200",
            "#gh:/sa",
            "#gh:~/sa",
            "#gh:./sa",
            "#gh:https://github.com/bbugyi200/sase",
            "#gh_bbugyi200/sase",
            "word#gh:bbugyi200/sa",
            "#foo:bbugyi200/sa",
            "#gh(bbugyi200/sa)",
        ] {
            let doc = DocumentSnapshot::new(prompt);
            let context = detect_vcs_repo_context_at_position(
                &doc,
                doc.byte_offset_to_position(prompt.len()).unwrap(),
                &workflow_names(&["gh"]),
            );
            assert!(context.is_none(), "{prompt}");
        }
    }

    #[test]
    fn vcs_repo_builder_replaces_only_the_ref_value() {
        let marked = "Fix #gh:bbugyi200/s<CURSOR>asex";
        let cursor = marked.find(VCS_REPO_CURSOR).unwrap();
        let text = marked.replace(VCS_REPO_CURSOR, "");
        let doc = DocumentSnapshot::new(text.clone());
        let context = vcs_repo_context(&text, cursor, &["gh"]);
        let list = build_vcs_repo_completion_candidates(
            &doc,
            &context,
            &[repo_entry("sase", "bbugyi200/sase")],
        );

        assert_eq!(list.candidates.len(), 1);
        let edit = list.candidates[0].replacement.as_ref().unwrap();
        assert_eq!(edit.new_text, "bbugyi200/sase ");
        assert_eq!(apply_text_edit(&text, edit), "Fix #gh:bbugyi200/sase ");
        assert!(list.candidates[0].additional_edits.is_empty());
    }

    // --- vcs_ref (`#gh:` / `#gh(` root-ref completion) ---------------------

    const VCS_REF_CURSOR: &str = "<CURSOR>";

    fn vcs_ref_context(
        text: &str,
        cursor: usize,
        names: &[&str],
    ) -> CompletionContext {
        let doc = DocumentSnapshot::new(text);
        let position = doc.byte_offset_to_position(cursor).unwrap();
        detect_vcs_ref_context_at_position(
            &doc,
            position,
            &workflow_names(names),
        )
        .unwrap_or_else(|| panic!("expected ref context for {text:?}"))
    }

    fn namespace_entry(name: &str, description: &str) -> VcsNamespaceEntry {
        VcsNamespaceEntry {
            name: name.to_string(),
            description: description.to_string(),
            kind_label: "org".to_string(),
        }
    }

    #[test]
    fn vcs_ref_golden_vectors() {
        // The cross-language parity contract -- identical to the Python
        // `VCS_REF_GOLDEN_VECTORS` table. `<CURSOR>` marks the cursor.
        let cases: &[(&str, &[&str], &str, bool, &str)] = &[
            ("#gh:<CURSOR>", &["gh"], "sase", false, "#gh:sase "),
            ("#gh:sa<CURSOR>", &["gh"], "sase", false, "#gh:sase "),
            (
                "Fix #gh:sa<CURSOR> now",
                &["gh"],
                "sase",
                false,
                "Fix #gh:sase now",
            ),
            ("#gh!!:sa<CURSOR>", &["gh"], "sase", false, "#gh!!:sase "),
            ("#gh:s<CURSOR>asex", &["gh"], "sase", false, "#gh:sase "),
            (
                "#git:sa<CURSOR>suffix",
                &["git"],
                "sase",
                false,
                "#git:sase ",
            ),
            ("#gh(s<CURSOR>", &["gh"], "sase", false, "#gh(sase)"),
            (
                "#gh(s<CURSOR>) next",
                &["gh"],
                "sase",
                false,
                "#gh(sase) next",
            ),
            ("#gh??(s<CURSOR>", &["gh"], "sase", false, "#gh??(sase)"),
            ("#gh:<CURSOR>", &["gh"], "sase-org", true, "#gh:sase-org/"),
            ("#gh:<CURSOR>", &["gh"], "sase-org/", true, "#gh:sase-org/"),
            (
                "Fix #gh:sa<CURSOR> now",
                &["gh"],
                "sase-org",
                true,
                "Fix #gh:sase-org/ now",
            ),
            ("#gh(sa<CURSOR>", &["gh"], "sase-org", true, "#gh(sase-org/"),
            (
                "#gh(sa<CURSOR>) next",
                &["gh"],
                "sase-org",
                true,
                "#gh(sase-org/) next",
            ),
        ];

        for (marked, names, selected_ref, chain, expected) in cases {
            let cursor = marked.find(VCS_REF_CURSOR).unwrap();
            let text = marked.replace(VCS_REF_CURSOR, "");
            let context = vcs_ref_context(&text, cursor, names);
            let trigger = context.vcs_ref.as_ref().unwrap();
            assert_eq!(
                apply_vcs_ref_selection(&text, trigger, selected_ref, *chain),
                *expected,
                "{marked}"
            );
        }
    }

    #[test]
    fn vcs_ref_accept_preserves_visible_space_before_document_final_newline() {
        // Neovim documents include a final newline; the editor path treats that
        // as end-of-input so the accepted visible line still gains a space.
        let marked = "#gh:sa<CURSOR>\n";
        let cursor = marked.find(VCS_REF_CURSOR).unwrap();
        let text = marked.replace(VCS_REF_CURSOR, "");
        let context = vcs_ref_context(&text, cursor, &["gh"]);
        let trigger = context.vcs_ref.as_ref().unwrap();

        assert_eq!(
            apply_vcs_ref_selection(&text, trigger, "sase", false),
            "#gh:sase \n",
        );
    }

    #[test]
    fn detects_vcs_ref_colon_spans() {
        let context = vcs_ref_context("#gh:sa", 6, &["gh"]);
        let trigger = context.vcs_ref.as_ref().unwrap();

        assert_eq!(context.kind, CompletionContextKind::VcsRef);
        assert_eq!(trigger.workflow, "gh");
        assert_eq!(trigger.separator, ":");
        assert_eq!(trigger.query, "sa");
        assert_eq!((trigger.ref_start, trigger.ref_end), (4, 6));
        assert_eq!(trigger.query_span, (4, 6));
        assert_eq!(context.replacement_range.start, pos(4));
        assert_eq!(context.replacement_range.end, pos(6));

        let context = vcs_ref_context("#gh:", 4, &["gh"]);
        let trigger = context.vcs_ref.as_ref().unwrap();
        assert_eq!(trigger.query, "");
        assert_eq!((trigger.ref_start, trigger.ref_end), (4, 4));
    }

    #[test]
    fn detects_vcs_ref_paren_hitl() {
        let context = vcs_ref_context("#gh??(sa", 8, &["gh"]);
        let trigger = context.vcs_ref.as_ref().unwrap();
        assert_eq!(trigger.workflow, "gh");
        assert_eq!(trigger.separator, "(");
        assert_eq!(trigger.query, "sa");
        assert_eq!((trigger.ref_start, trigger.ref_end), (6, 8));
    }

    #[test]
    fn classifies_vcs_repo_then_vcs_ref_then_xprompt_args() {
        let catalog = vec![gh_entry()];
        let names = workflow_names(&["gh"]);

        let doc = DocumentSnapshot::new("#gh:owner/repo");
        let context = classify_completion_context_with_workflows(
            &doc,
            pos(14),
            &catalog,
            &names,
        )
        .unwrap();
        assert_eq!(context.kind, CompletionContextKind::VcsRepo);

        let doc = DocumentSnapshot::new("#gh:");
        let context = classify_completion_context_with_workflows(
            &doc,
            pos(4),
            &catalog,
            &names,
        )
        .unwrap();
        assert_eq!(context.kind, CompletionContextKind::VcsRef);

        let doc = DocumentSnapshot::new("#gh:sa");
        let context = classify_completion_context_with_workflows(
            &doc,
            pos(6),
            &catalog,
            &names,
        )
        .unwrap();
        assert_eq!(context.kind, CompletionContextKind::VcsRef);

        let doc = DocumentSnapshot::new("#gh:~/x");
        let context = classify_completion_context_with_workflows(
            &doc,
            pos(7),
            &catalog,
            &names,
        )
        .unwrap();
        assert_eq!(
            context.kind,
            CompletionContextKind::XpromptArgumentTypeHint
        );
    }

    #[test]
    fn vcs_ref_trigger_negatives() {
        for prompt in [
            "#gh:/sa",
            "#gh:~/x",
            "#gh:./x",
            "#gh:https://github.com/bbugyi200/sase",
            "#gh:owner/repo",
            "#gh_bbugyi200",
            "word#gh:sa",
            "#foo:sa",
            "#gh(sa)",
            "#gh:123)",
        ] {
            let doc = DocumentSnapshot::new(prompt);
            let context = detect_vcs_ref_context_at_position(
                &doc,
                doc.byte_offset_to_position(prompt.len()).unwrap(),
                &workflow_names(&["gh"]),
            );
            assert!(context.is_none(), "{prompt}");
        }
    }

    #[test]
    fn vcs_ref_builder_groups_rows_and_replaces_root_ref() {
        let text = "#gh:";
        let doc = DocumentSnapshot::new(text);
        let context = vcs_ref_context(text, text.len(), &["gh"]);
        let list = build_vcs_ref_completion_candidates(
            &doc,
            &context,
            &[
                patch_entry("ship-completion", "sase", "Ready"),
                project_entry("sase", "gh"),
                project_entry("bob", "git"),
            ],
            &[namespace_entry("sase-org", "2 enabled projects")],
        );

        assert_eq!(
            list.candidates
                .iter()
                .map(|candidate| candidate.name.as_str())
                .collect::<Vec<_>>(),
            vec!["sase", "ship-completion", "sase-org"]
        );
        assert_eq!(list.candidates[0].kind, "project");
        assert_eq!(list.candidates[1].kind, "patch");
        assert_eq!(list.candidates[1].project, "sase");
        assert_eq!(list.candidates[1].status, "Ready");
        assert_eq!(list.candidates[2].display, "sase-org/");
        assert_eq!(list.candidates[2].kind, "namespace");
        assert_eq!(list.candidates[2].status, "org");

        let project_edit = list.candidates[0].replacement.as_ref().unwrap();
        assert_eq!(apply_text_edit(text, project_edit), "#gh:sase ");
        let namespace_edit = list.candidates[2].replacement.as_ref().unwrap();
        assert_eq!(apply_text_edit(text, namespace_edit), "#gh:sase-org/");
    }

    #[test]
    fn vcs_ref_builder_filters_by_query_and_alias() {
        let text = "#gh:sea";
        let doc = DocumentSnapshot::new(text);
        let context = vcs_ref_context(text, text.len(), &["gh"]);
        let mut entry = project_entry("sase", "gh");
        entry.aliases = vec!["seaside".to_string()];
        let list = build_vcs_ref_completion_candidates(
            &doc,
            &context,
            &[entry, project_entry("bob", "gh")],
            &[namespace_entry("sase-org", "")],
        );

        assert_eq!(
            list.candidates
                .iter()
                .map(|candidate| candidate.name.as_str())
                .collect::<Vec<_>>(),
            vec!["sase"]
        );
    }

    // --- vcs_project (`+`) completion --------------------------------------

    fn vcs_names() -> Vec<String> {
        ["gh", "git", "hg"].iter().map(|s| s.to_string()).collect()
    }

    fn project_entry(name: &str, prefix: &str) -> VcsProjectEntry {
        VcsProjectEntry {
            name: name.to_string(),
            vcs_prefix: prefix.to_string(),
            display_tag: format!("#{prefix}:{name}"),
            provider_display: "GitHub".to_string(),
            description: String::new(),
            aliases: Vec::new(),
            entry_kind: "project".to_string(),
            kind: "project".to_string(),
            project: name.to_string(),
            status: String::new(),
        }
    }

    fn patch_entry(name: &str, project: &str, status: &str) -> VcsProjectEntry {
        VcsProjectEntry {
            name: name.to_string(),
            vcs_prefix: "gh".to_string(),
            display_tag: format!("#gh:{name}"),
            provider_display: "GitHub".to_string(),
            description: String::new(),
            aliases: Vec::new(),
            entry_kind: "patch".to_string(),
            // Legacy backing kind remains in fixtures for compatibility.
            kind: "changespec".to_string(),
            project: project.to_string(),
            status: status.to_string(),
        }
    }

    fn legacy_patch_entry(
        name: &str,
        project: &str,
        status: &str,
    ) -> VcsProjectEntry {
        VcsProjectEntry {
            name: name.to_string(),
            vcs_prefix: "gh".to_string(),
            display_tag: format!("#gh:{name}"),
            provider_display: "GitHub".to_string(),
            description: String::new(),
            aliases: Vec::new(),
            entry_kind: String::new(),
            // Legacy backing kind remains accepted for compatibility.
            kind: "changespec".to_string(),
            project: project.to_string(),
            status: status.to_string(),
        }
    }

    fn apply_test_edits(text: &str, edits: &VcsProjectByteEdits) -> String {
        let mut all: Vec<&VcsByteEdit> = std::iter::once(&edits.primary)
            .chain(edits.additional.iter())
            .collect();
        all.sort_by_key(|edit| (edit.start, edit.end));
        let mut out = String::new();
        let mut pos = 0;
        for edit in all {
            out.push_str(&text[pos..edit.start]);
            out.push_str(&edit.new_text);
            pos = edit.end;
        }
        out.push_str(&text[pos..]);
        out
    }

    /// Detect the trigger in `marked` (where `‸` is the cursor), then expand
    /// the `#gh:sase` selection both via the canonical transform and via the
    /// applied byte edits. Returns `(canonical, via_edits)`.
    fn expand_via_both(marked: &str) -> (String, String) {
        let cursor_byte = marked.find('‸').expect("cursor marker");
        let text = marked.replacen('‸', "", 1);
        let doc = DocumentSnapshot::new(text.clone());
        let position = doc
            .byte_offset_to_position(cursor_byte)
            .expect("cursor on a char boundary");
        let token =
            vcs_project_trigger_token(&doc, position).expect("a trigger token");
        let re = vcs_replace_regex(&vcs_names());

        let canonical = apply_vcs_project_selection(
            &text,
            token.byte_start,
            token.byte_end,
            "#gh:sase",
            &re,
        );
        let edits = vcs_project_byte_edits(
            &text,
            token.byte_start,
            token.byte_end,
            "#gh:sase",
            &re,
        );
        (canonical, apply_test_edits(&text, &edits))
    }

    #[test]
    fn vcs_project_golden_vectors() {
        // The cross-language parity contract -- identical to the Python
        // `_GOLDEN_VECTORS` table. `‸` marks the cursor.
        let cases = [
            ("Describe this repo. +‸", "#gh:sase Describe this repo."),
            ("+‸", "#gh:sase "),
            ("+sa‸", "#gh:sase "),
            ("+s‸\n", "#gh:sase \n"),
            ("+s‸\nmore text", "#gh:sase \nmore text"),
            ("#git:foo Fix bug +‸", "#gh:sase Fix bug"),
            ("#gh!!:foo do X +‸", "#gh:sase do X"),
            // Existing leading VCS tag at end-of-input (no trailing text): the
            // trigger strip leaves the bare tag at EOF, which must still be
            // replaced -- not doubled.
            ("#gh:sase +‸", "#gh:sase "),
            ("#gh:sase +foo‸", "#gh:sase "),
            ("#git:foo +‸", "#gh:sase "),
            ("Fix +bug‸ here", "#gh:sase Fix here"),
            ("Line one\n +‸", "#gh:sase Line one\n"),
            (
                "---\nname: x\n---\nBody +‸",
                "---\nname: x\n---\n#gh:sase Body",
            ),
            ("%model:opus Body +‸", "%model:opus #gh:sase Body"),
            ("+sa‸ Fix", "#gh:sase Fix"),
            // The cursor-local query is `sa`, while selection consumes the
            // entire `+sase` token.
            ("Fix +sa‸se now", "#gh:sase Fix now"),
        ];
        for (marked, expected) in cases {
            let (canonical, via_edits) = expand_via_both(marked);
            assert_eq!(canonical, expected, "canonical: {marked:?}");
            assert_eq!(via_edits, expected, "via edits: {marked:?}");
        }
    }

    #[test]
    fn vcs_prepend_offset_skips_horizontal_whitespace_only() {
        assert_eq!(vcs_prepend_offset("\n"), 0);
        assert_eq!(vcs_prepend_offset("\nmore"), 0);
        assert_eq!(vcs_prepend_offset("  Body"), 2);
        assert_eq!(vcs_prepend_offset("\tBody"), 1);
    }

    #[test]
    fn classifies_vcs_project_trigger() {
        for (text, col) in [
            ("+", 1),
            ("+sa", 3),
            ("Fix +", 5),
            ("Fix +sa", 7),
            ("2 + 2", 3),
        ] {
            let doc = DocumentSnapshot::new(text);
            let context =
                classify_completion_context(&doc, pos(col), &[]).unwrap();
            assert_eq!(
                context.kind,
                CompletionContextKind::VcsProject,
                "{text}"
            );
        }

        for (text, col) in [
            ("#+", 2),
            ("Fix #+", 6),
            ("line\n+", 1),
            ("\t+", 2),
            ("word+", 5),
            ("a+b", 3),
            ("c++", 3),
            ("c#+x", 4),
        ] {
            let doc = DocumentSnapshot::new(text);
            let position = if text == "line\n+" {
                EditorPosition {
                    line: 1,
                    character: col,
                }
            } else {
                pos(col)
            };
            let context = classify_completion_context(&doc, position, &[]);
            assert_ne!(
                context.map(|context| context.kind),
                Some(CompletionContextKind::VcsProject),
                "{text}"
            );
        }

        let doc = DocumentSnapshot::new("line\n +");
        let context = classify_completion_context(
            &doc,
            EditorPosition {
                line: 1,
                character: 2,
            },
            &[],
        )
        .unwrap();
        assert_eq!(context.kind, CompletionContextKind::VcsProject);
    }

    #[test]
    fn bof_trigger_merges_into_single_primary_edit() {
        let doc = DocumentSnapshot::new("+");
        let context = classify_completion_context(&doc, pos(1), &[]).unwrap();
        let token = context.token.as_ref().unwrap();
        let list = build_vcs_project_completion_candidates(
            token,
            &doc,
            pos(1),
            &[project_entry("sase", "gh")],
            &vcs_names(),
        );

        assert_eq!(list.candidates.len(), 1);
        let candidate = &list.candidates[0];
        assert_eq!(candidate.name, "sase");
        assert_eq!(candidate.insertion, "#gh:sase");
        // BOF `+`: the prepend point coincides with the trigger deletion, so
        // the edits merge into one primary edit with no additional edits.
        assert!(candidate.additional_edits.is_empty());
        assert_eq!(
            candidate.replacement.as_ref().unwrap().new_text,
            "#gh:sase "
        );
    }

    #[test]
    fn trailing_trigger_emits_primary_plus_additional_edit() {
        let doc = DocumentSnapshot::new("Describe this repo. +");
        let cursor = pos(21);
        let context = classify_completion_context(&doc, cursor, &[]).unwrap();
        let token = context.token.as_ref().unwrap();
        let list = build_vcs_project_completion_candidates(
            token,
            &doc,
            cursor,
            &[project_entry("sase", "gh")],
            &vcs_names(),
        );

        let candidate = &list.candidates[0];
        // The primary edit consumes the trigger token; the additional edit
        // prepends the tag at the start of the document.
        assert_eq!(candidate.replacement.as_ref().unwrap().new_text, "");
        assert_eq!(candidate.additional_edits.len(), 1);
        assert_eq!(candidate.additional_edits[0].new_text, "#gh:sase ");
        let prepend_range = candidate.additional_edits[0].range;
        assert_eq!(prepend_range.start, prepend_range.end);
    }

    #[test]
    fn vcs_project_candidates_filter_preserves_catalog_order() {
        let doc = DocumentSnapshot::new("Fix +sa");
        let cursor = pos(7);
        let context = classify_completion_context(&doc, cursor, &[]).unwrap();
        let token = context.token.as_ref().unwrap();
        let list = build_vcs_project_completion_candidates(
            token,
            &doc,
            cursor,
            &[
                project_entry("sase", "gh"),
                project_entry("saseling", "gh"),
                project_entry("bob", "git"),
            ],
            &vcs_names(),
        );

        assert_eq!(
            list.candidates
                .iter()
                .map(|candidate| candidate.name.as_str())
                .collect::<Vec<_>>(),
            vec!["sase", "saseling"]
        );
    }

    #[test]
    fn vcs_project_candidates_include_patch_context() {
        let doc = DocumentSnapshot::new("Review +ship");
        let cursor = pos(12);
        let context = classify_completion_context(&doc, cursor, &[]).unwrap();
        let token = context.token.as_ref().unwrap();
        let list = build_vcs_project_completion_candidates(
            token,
            &doc,
            cursor,
            &[
                project_entry("sase", "gh"),
                patch_entry("ship-completion", "sase", "Ready"),
            ],
            &vcs_names(),
        );

        assert_eq!(list.candidates.len(), 1);
        let candidate = &list.candidates[0];
        assert_eq!(candidate.name, "ship-completion");
        assert_eq!(candidate.insertion, "#gh:ship-completion");
        assert_eq!(candidate.kind, "patch");
        assert_eq!(candidate.project, "sase");
        assert_eq!(candidate.status, "Ready");
    }

    #[test]
    fn vcs_project_candidates_accept_legacy_changespec_kind() {
        let doc = DocumentSnapshot::new("Review +ship");
        let cursor = pos(12);
        let context = classify_completion_context(&doc, cursor, &[]).unwrap();
        let token = context.token.as_ref().unwrap();
        let list = build_vcs_project_completion_candidates(
            token,
            &doc,
            cursor,
            &[legacy_patch_entry("ship-completion", "sase", "Ready")],
            &vcs_names(),
        );

        assert_eq!(list.candidates.len(), 1);
        assert_eq!(list.candidates[0].kind, "patch");
    }

    #[test]
    fn vcs_project_candidates_filter_for_bare_plus_query() {
        // The query for a BOF `+sa` token is `sa` (prefix length 1), so the
        // candidate list filters in catalog order.
        let doc = DocumentSnapshot::new("+sa");
        let cursor = pos(3);
        let context = classify_completion_context(&doc, cursor, &[]).unwrap();
        assert_eq!(context.kind, CompletionContextKind::VcsProject);
        let token = context.token.as_ref().unwrap();
        let list = build_vcs_project_completion_candidates(
            token,
            &doc,
            cursor,
            &[
                project_entry("sase", "gh"),
                project_entry("saseling", "gh"),
                project_entry("bob", "git"),
            ],
            &vcs_names(),
        );

        assert_eq!(
            list.candidates
                .iter()
                .map(|candidate| candidate.name.as_str())
                .collect::<Vec<_>>(),
            vec!["sase", "saseling"]
        );
    }

    #[test]
    fn vcs_project_candidates_match_aliases() {
        let doc = DocumentSnapshot::new("Find +sea");
        let cursor = pos(9);
        let context = classify_completion_context(&doc, cursor, &[]).unwrap();
        let token = context.token.as_ref().unwrap();
        let mut entry = project_entry("sase", "gh");
        entry.aliases = vec!["seaside".to_string()];
        let list = build_vcs_project_completion_candidates(
            token,
            &doc,
            cursor,
            &[entry, project_entry("bob", "git")],
            &vcs_names(),
        );

        assert_eq!(
            list.candidates
                .iter()
                .map(|candidate| candidate.name.as_str())
                .collect::<Vec<_>>(),
            vec!["sase"]
        );
    }

    #[test]
    fn vcs_project_edits_never_overlap() {
        // Every golden input must yield non-overlapping edits (LSP requires
        // it); `vcs_edits_conflict` is the guard.
        for marked in [
            "Describe this repo. +‸",
            "+‸",
            "+sa‸",
            "#git:foo Fix bug +‸",
            // Existing tag at EOF: the replace edit (tag span) and the primary
            // trigger-deletion edit are adjacent and must not overlap.
            "#git:foo +‸",
            "#gh:sase +‸",
            "%model:opus Body +‸",
            "+sa‸ Fix",
            "Fix +sa‸se now",
        ] {
            let cursor_byte = marked.find('‸').unwrap();
            let text = marked.replacen('‸', "", 1);
            let doc = DocumentSnapshot::new(text.clone());
            let position = doc.byte_offset_to_position(cursor_byte).unwrap();
            let token = vcs_project_trigger_token(&doc, position).unwrap();
            let re = vcs_replace_regex(&vcs_names());
            let edits = vcs_project_byte_edits(
                &text,
                token.byte_start,
                token.byte_end,
                "#gh:sase",
                &re,
            );
            assert!(
                !vcs_edits_conflict(&edits.primary, &edits.additional),
                "overlapping edits for {marked:?}"
            );
        }
    }
}
