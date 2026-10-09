//! Byte goldens for replay-backed bead mutations (`sase-1h8.13.1.9.2`).
//!
//! Every ordinary mutation entry point runs here once per backing — a no-git
//! event store and a legacy `issues.jsonl` store (no `events/` dir) — with
//! explicit `now` values and a fixed owner/actor, recording the serialized
//! outcome-or-error plus the exact bytes of every file under the beads dir
//! (`beads.db` and the `.bead-mutation-lock.holder` file excluded: the first
//! is the advisory flock, the second its holder record).
//!
//! The goldens are generated from the current, untouched replay code: these
//! stores have no `.git` dir, so `MutationView::load_cached` declines and the
//! `MutableStore::load` replay path runs. Later unification phases must keep
//! every golden byte-identical; any regeneration needs explicit justification
//! in that phase's bead note. Do not regenerate to get a green run.
//!
//! Regenerate with:
//! `UPDATE_MUTATION_GOLDENS=1 just test -p sase_core bead::mutation::tests::replay_goldens`
//!
//! `remove_issues`/`remove_issue` mint their event timestamp from the wall
//! clock (`now_utc`, no `now` parameter), so those scenarios store bytes with
//! the `issue_removed` event timestamps replaced by `<REMOVE_NOW>` and the
//! trailing content hash of their `event_id`s (which covers the timestamp)
//! replaced by `<EVENT_HASH>`; every other byte is exact. The comparison
//! normalizes the same way, so later phases still pin routing, payloads,
//! ordering and counts.
//!
//! `lock_wait_ms` is flock timing telemetry, not mutation behavior:
//! every golden `act` helper returns through `outcome_string`, which pins
//! it at zero at the source (real contention coverage lives in
//! `links.rs`). Every other byte stays exact.
//!
//! `cached_golden_bytes_match_replay` reruns every scenario below on a
//! git-backed store, where the single view algorithm takes the cached
//! path, and pins the identical outcome and bytes. It never regenerates
//! the goldens: a mismatch is a defect in the unified algorithm.

use super::super::*;
use crate::artifact_link::ArtifactLinkOriginWire;
use crate::artifact_link::BeadLinkDirectionWire;
use crate::bead::config::default_config;
use crate::bead::config::save_config;
use crate::bead::wire::BeadError;
use crate::bead::wire::BeadResolutionWire;
use crate::bead::wire::BeadTierWire;
use crate::bead::wire::IssueTypeWire;
use crate::bead::wire::PhaseSizeWire;
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use tempfile::tempdir;

use super::replay_golden_cases::{cases, Case};

pub(super) const OWNER: &str = "owner@example.com";
pub(super) const AGENT: &str = "test-agent";
const ACT: &str = "2026-01-02T00:00:00Z";
const GOLDENS_DIR: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/src/bead/mutation/tests/goldens"
);

#[derive(Default)]
pub(super) struct SeedIds {
    pub(super) epic: String,
    pub(super) phase: String,
    pub(super) phase2: String,
    pub(super) task: String,
    pub(super) alpha: String,
    pub(super) beta: String,
    pub(super) note_id: String,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Backing {
    Event,
    Legacy,
}

impl Backing {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Backing::Event => "event",
            Backing::Legacy => "legacy",
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Backings {
    Both,
    EventOnly,
    LegacyOnly,
}

impl Backings {
    pub(super) fn list(self) -> &'static [Backing] {
        match self {
            Backings::Both => &[Backing::Event, Backing::Legacy],
            Backings::EventOnly => &[Backing::Event],
            Backings::LegacyOnly => &[Backing::Legacy],
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum SeedKind {
    Empty,
    Plan,
    TwoPlans,
    Tree,
    TreePhaseClosed,
    TreeAllClosed,
    TreePhaseClaimed,
    TreeTwoPhases,
    TreeEpicReady,
    TaskOpen,
    TaskClosed,
    TaskWithNote,
    TaskSnoozed,
    TaskPlusOned,
    PlansWithDep,
    PlansWithRef,
    PlansWithLink,
    PlansWithProjection,
    LegacyMixedPrefix,
}

pub(super) fn outcome_string(
    result: Result<BeadMutationOutcomeWire, BeadError>,
) -> String {
    match result {
        Ok(mut outcome) => {
            // `lock_wait_ms` is environmental flock-wait telemetry, not
            // mutation semantics (cf. `bead_read_model_mutation_proof`).
            outcome.lock_wait_ms = 0;
            format!("ok {}", serde_json::to_string(&outcome).unwrap())
        }
        Err(error) => {
            format!("err kind={} message={}", error.kind, error.message)
        }
    }
}

/// Every file under the beads dir, keyed by `/`-separated relative path.
/// Skips the flock database and its holder record; everything else is
/// pinned, so a stray new file fails loudly instead of slipping through.
fn snapshot_store(beads_dir: &Path) -> BTreeMap<String, String> {
    fn walk(root: &Path, dir: &Path, out: &mut BTreeMap<String, String>) {
        let mut entries: Vec<_> = fs::read_dir(dir)
            .unwrap_or_else(|_| panic!("read_dir {}", dir.display()))
            .map(|entry| entry.unwrap())
            .collect();
        entries.sort_by_key(|entry| entry.file_name());
        for entry in entries {
            let name = entry.file_name().to_string_lossy().into_owned();
            if name == "beads.db" || name == ".bead-mutation-lock.holder" {
                continue;
            }
            let path = entry.path();
            if path.is_dir() {
                walk(root, &path, out);
            } else {
                let rel = path
                    .strip_prefix(root)
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .replace('\\', "/");
                let bytes = fs::read(&path).unwrap();
                out.insert(
                    rel,
                    String::from_utf8(bytes)
                        .expect("golden store files are UTF-8"),
                );
            }
        }
    }

    let mut out = BTreeMap::new();
    walk(beads_dir, beads_dir, &mut out);
    out
}

/// Replace wall-clock-derived bytes on `issue_removed` events with stable
/// placeholders: the `timestamp` field and the trailing content hash of
/// `event_id` (which `mint_bead_event_id` covers the timestamp). Only the
/// remove entry points mint `now_utc()`; ordinals, routing, payloads, key
/// order and every other byte stay exact.
fn normalize_remove_timestamps(files: &mut BTreeMap<String, String>) {
    for content in files.values_mut() {
        let mut normalized = Vec::new();
        for line in content.lines() {
            if line.contains("\"operation\":\"issue_removed\"") {
                let value: serde_json::Value =
                    serde_json::from_str(line).unwrap();
                let stamp = value
                    .get("timestamp")
                    .and_then(|stamp| stamp.as_str())
                    .unwrap()
                    .to_string();
                let event_id = value
                    .get("event_id")
                    .and_then(|id| id.as_str())
                    .unwrap()
                    .to_string();
                let stable_id = match event_id.rsplit_once(':') {
                    Some((head, _)) => format!("{head}:<EVENT_HASH>"),
                    None => "<EVENT_ID>".to_string(),
                };
                let line = line.replace(
                    &format!(
                        "\"timestamp\":{}",
                        serde_json::to_string(&stamp).unwrap()
                    ),
                    "\"timestamp\":\"<REMOVE_NOW>\"",
                );
                normalized.push(line.replace(&event_id, &stable_id));
            } else {
                normalized.push(line.to_string());
            }
        }
        let trailing = content.ends_with('\n');
        *content = normalized.join("\n");
        if trailing {
            content.push('\n');
        }
    }
}

fn make_plan(beads_dir: &Path, title: &str, now: &str) -> String {
    create_issue(
        beads_dir,
        BeadCreateRequestWire {
            title: title.to_string(),
            issue_type: IssueTypeWire::Plan,
            created_by: Some("creator-agent".to_string()),
            now: Some(now.to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap()
    .id
}

fn make_task(beads_dir: &Path, title: &str, now: &str) -> String {
    create_issue(
        beads_dir,
        BeadCreateRequestWire {
            title: title.to_string(),
            issue_type: IssueTypeWire::Task,
            size: Some(PhaseSizeWire::Small),
            task_type: Some("bug".to_string()),
            created_by: Some("creator-agent".to_string()),
            now: Some(now.to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap()
    .id
}

fn close_id(beads_dir: &Path, issue_id: &str, now: &str) {
    close_issues(
        beads_dir,
        &[issue_id.to_string()],
        Some("done".to_string()),
        Some(BeadResolutionWire::Done),
        false,
        Some(now.to_string()),
    )
    .unwrap();
}

fn seed_tree(beads_dir: &Path) -> SeedIds {
    let epic = create_issue(
        beads_dir,
        BeadCreateRequestWire {
            title: "Epic".to_string(),
            issue_type: IssueTypeWire::Plan,
            tier: Some(BeadTierWire::Epic),
            created_by: Some("creator-agent".to_string()),
            now: Some("2026-01-01T00:00:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap()
    .id;
    let phase = create_issue(
        beads_dir,
        BeadCreateRequestWire {
            title: "Phase".to_string(),
            issue_type: IssueTypeWire::Phase,
            parent_id: Some(epic.clone()),
            size: Some(PhaseSizeWire::Small),
            created_by: Some("creator-agent".to_string()),
            now: Some("2026-01-01T00:01:00Z".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .issue
    .unwrap()
    .id;
    let task = make_task(beads_dir, "Task", "2026-01-01T00:02:00Z");
    SeedIds {
        epic,
        phase,
        task,
        ..Default::default()
    }
}

/// Build the event-store half of a seed; the legacy half is transplanted
/// by the runner (export + copy), except `Empty` and `LegacyMixedPrefix`.
fn build_event_seed(kind: SeedKind, beads_dir: &Path) -> SeedIds {
    match kind {
        SeedKind::Empty => SeedIds::default(),
        SeedKind::Plan => {
            let alpha = make_plan(beads_dir, "Alpha", "2026-01-01T00:00:00Z");
            SeedIds {
                alpha,
                ..Default::default()
            }
        }
        SeedKind::TwoPlans => {
            let alpha = make_plan(beads_dir, "Alpha", "2026-01-01T00:00:00Z");
            let beta = make_plan(beads_dir, "Beta", "2026-01-01T00:01:00Z");
            SeedIds {
                alpha,
                beta,
                ..Default::default()
            }
        }
        SeedKind::Tree => seed_tree(beads_dir),
        SeedKind::TreePhaseClosed => {
            let seed = seed_tree(beads_dir);
            close_id(beads_dir, &seed.phase, "2026-01-01T00:03:00Z");
            seed
        }
        SeedKind::TreeAllClosed => {
            let seed = seed_tree(beads_dir);
            close_id(beads_dir, &seed.phase, "2026-01-01T00:03:00Z");
            close_id(beads_dir, &seed.epic, "2026-01-01T00:04:00Z");
            seed
        }
        SeedKind::TreePhaseClaimed => {
            let seed = seed_tree(beads_dir);
            claim_for_agent_wait(
                beads_dir,
                &seed.phase,
                AGENT,
                Some("2026-01-01T00:03:00Z".to_string()),
            )
            .unwrap();
            seed
        }
        SeedKind::TreeTwoPhases => {
            let mut seed = seed_tree(beads_dir);
            seed.phase2 = create_issue(
                beads_dir,
                BeadCreateRequestWire {
                    title: "Phase Two".to_string(),
                    issue_type: IssueTypeWire::Phase,
                    parent_id: Some(seed.epic.clone()),
                    size: Some(PhaseSizeWire::Small),
                    created_by: Some("creator-agent".to_string()),
                    now: Some("2026-01-01T00:03:00Z".to_string()),
                    ..Default::default()
                },
            )
            .unwrap()
            .issue
            .unwrap()
            .id;
            seed
        }
        SeedKind::TreeEpicReady => {
            let seed = seed_tree(beads_dir);
            mark_ready_to_work(
                beads_dir,
                &seed.epic,
                Some("2026-01-01T00:03:00Z".to_string()),
            )
            .unwrap();
            seed
        }
        SeedKind::TaskOpen => {
            let task = make_task(beads_dir, "Task", "2026-01-01T00:00:00Z");
            SeedIds {
                task,
                ..Default::default()
            }
        }
        SeedKind::TaskClosed => {
            let task = make_task(beads_dir, "Task", "2026-01-01T00:00:00Z");
            close_id(beads_dir, &task, "2026-01-01T00:01:00Z");
            SeedIds {
                task,
                ..Default::default()
            }
        }
        SeedKind::TaskWithNote => {
            let task = make_task(beads_dir, "Task", "2026-01-01T00:00:00Z");
            let note_id = append_issue_note(
                beads_dir,
                &task,
                "first note",
                Some("agent-1".to_string()),
                Some("2026-01-01T00:01:00Z".to_string()),
                None,
            )
            .unwrap()
            .issue
            .unwrap()
            .notes
            .pop()
            .unwrap()
            .id;
            SeedIds {
                task,
                note_id,
                ..Default::default()
            }
        }
        SeedKind::TaskSnoozed => {
            let task = make_task(beads_dir, "Task", "2026-01-01T00:00:00Z");
            snooze_task(
                beads_dir,
                &task,
                "2026-03-01T00:00:00Z",
                None,
                "needs the upstream fix",
                AGENT,
                Some("2026-01-01T00:01:00Z".to_string()),
            )
            .unwrap();
            SeedIds {
                task,
                ..Default::default()
            }
        }
        SeedKind::TaskPlusOned => {
            let task = make_task(beads_dir, "Task", "2026-01-01T00:00:00Z");
            add_task_plus_one(
                beads_dir,
                &task,
                "reporter-one",
                "hit this too",
                &[],
                Some("2026-01-01T00:01:00Z".to_string()),
                None,
                None,
            )
            .unwrap();
            SeedIds {
                task,
                ..Default::default()
            }
        }
        SeedKind::PlansWithDep => {
            let seed = build_event_seed(SeedKind::TwoPlans, beads_dir);
            add_dependency(
                beads_dir,
                &seed.alpha,
                &seed.beta,
                Some("2026-01-01T00:02:00Z".to_string()),
            )
            .unwrap();
            seed
        }
        SeedKind::PlansWithRef => {
            let seed = build_event_seed(SeedKind::TwoPlans, beads_dir);
            add_bead_references(
                beads_dir,
                &seed.alpha,
                &["agent:test-agent.1".to_string()],
                Some("2026-01-01T00:02:00Z".to_string()),
            )
            .unwrap();
            seed
        }
        SeedKind::PlansWithLink => {
            let seed = build_event_seed(SeedKind::TwoPlans, beads_dir);
            add_bead_link(
                beads_dir,
                &seed.alpha,
                &format!("bead:{}", seed.beta),
                "related",
                "shares a root cause",
                ArtifactLinkOriginWire::Manual,
                BeadLinkDirectionWire::Out,
                1,
                Some("2026-01-01T00:02:00Z".to_string()),
                None,
            )
            .unwrap();
            seed
        }
        SeedKind::PlansWithProjection => {
            let seed = build_event_seed(SeedKind::TwoPlans, beads_dir);
            set_bead_link_projection(
                beads_dir,
                &seed.alpha,
                &format!("bead:{}", seed.beta),
                "related",
                BeadLinkDirectionWire::Out,
                true,
                Some("projected link".to_string()),
                Some(ArtifactLinkOriginWire::Manual),
                1,
                Some("2026-01-01T00:02:00Z".to_string()),
                "00000000000000000000000000000001".to_string(),
            )
            .unwrap();
            seed
        }
        SeedKind::LegacyMixedPrefix => {
            panic!("LegacyMixedPrefix builds its own legacy store")
        }
    }
}

/// Copy a seeded event store into a fresh legacy store: the on-demand
/// `issues.jsonl` projection plus config, with no `events/` dir, so the
/// first act mutation materializes the event store on save.
fn transplant_as_legacy(src: &Path) -> (tempfile::TempDir, PathBuf) {
    export_jsonl(src).unwrap();
    let temp = tempdir().unwrap();
    let dest = temp.path().join("beads");
    fs::create_dir_all(&dest).unwrap();
    let mut dirs = vec![src.to_path_buf()];
    while let Some(dir) = dirs.pop() {
        let mut entries: Vec<_> = fs::read_dir(&dir)
            .unwrap()
            .map(|entry| entry.unwrap())
            .collect();
        entries.sort_by_key(|entry| entry.file_name());
        for entry in entries {
            let name = entry.file_name().to_string_lossy().into_owned();
            if name == "beads.db" || name == ".bead-mutation-lock.holder" {
                continue;
            }
            let rel = entry.path().strip_prefix(src).unwrap().to_path_buf();
            if rel.starts_with("events") {
                continue;
            }
            let target = dest.join(&rel);
            if entry.path().is_dir() {
                fs::create_dir_all(&target).unwrap();
                dirs.push(entry.path());
            } else {
                fs::copy(entry.path(), &target).unwrap();
            }
        }
    }
    (temp, dest)
}

/// A legacy store with two issues sharing the ID suffix `1`, so the
/// shorthand `1` is ambiguous. Built by transplanting two plans, then
/// rewriting the second id.
fn legacy_mixed_prefix_store() -> (tempfile::TempDir, PathBuf) {
    let event_temp = tempdir().unwrap();
    let event_beads = event_temp.path().join("beads");
    init_store(event_temp.path(), "beads", "sase", OWNER).unwrap();
    build_event_seed(SeedKind::TwoPlans, &event_beads);
    let (temp, dest) = transplant_as_legacy(&event_beads);
    let jsonl_path = dest.join("issues.jsonl");
    let jsonl = fs::read_to_string(&jsonl_path).unwrap();
    let beta_id = {
        let value: serde_json::Value =
            serde_json::from_str(jsonl.lines().nth(1).unwrap()).unwrap();
        value
            .get("id")
            .and_then(|id| id.as_str())
            .unwrap()
            .to_string()
    };
    let rewritten = jsonl.replacen(
        &format!("\"id\":\"{beta_id}\""),
        "\"id\":\"other-1\"",
        1,
    );
    assert_ne!(rewritten, jsonl, "beta id appears once in issues.jsonl");
    fs::write(&jsonl_path, rewritten).unwrap();
    (temp, dest)
}

fn fresh_legacy_store() -> (tempfile::TempDir, PathBuf) {
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("beads");
    fs::create_dir_all(&beads_dir).unwrap();
    save_config(&beads_dir, &default_config("sase", OWNER)).unwrap();
    fs::write(beads_dir.join("issues.jsonl"), "").unwrap();
    (temp, beads_dir)
}

pub(super) fn some_now() -> Option<String> {
    Some(ACT.to_string())
}

fn diff_report(
    scenario: &str,
    expected_outcome: &str,
    expected_files: &BTreeMap<String, String>,
    committed: &str,
) -> String {
    let mut lines = vec![format!("scenario {scenario}")];
    let parsed: serde_json::Value =
        serde_json::from_str(committed).unwrap_or(serde_json::Value::Null);
    let committed_outcome =
        parsed.get("outcome").and_then(|value| value.as_str());
    if committed_outcome != Some(expected_outcome) {
        lines.push(format!(
            "  outcome differs:\n    committed: {}\n    expected:  {}",
            committed_outcome.unwrap_or("<unparseable>"),
            expected_outcome
        ));
    }
    let empty = serde_json::Map::new();
    let committed_files = parsed
        .get("files")
        .and_then(|value| value.as_object())
        .unwrap_or(&empty);
    for path in expected_files.keys() {
        if !committed_files.contains_key(path) {
            lines.push(format!("  file only in actual: {path}"));
        }
    }
    for path in committed_files.keys() {
        if !expected_files.contains_key(path) {
            lines.push(format!("  file only in golden: {path}"));
        }
    }
    for (path, expected) in expected_files {
        let Some(committed_value) = committed_files.get(path) else {
            continue;
        };
        let Some(committed_text) = committed_value.as_str() else {
            lines.push(format!("  file {path}: golden entry is not a string"));
            continue;
        };
        if committed_text == expected {
            continue;
        }
        let mut excerpt = String::from("  file {path}: bytes differ");
        for (index, (left, right)) in
            committed_text.lines().zip(expected.lines()).enumerate()
        {
            if left != right {
                excerpt = format!(
                    "  file {path} line {} differs:\n    golden:   {}\n    actual:   {}",
                    index + 1,
                    left.chars().take(300).collect::<String>(),
                    right.chars().take(300).collect::<String>(),
                );
                break;
            }
        }
        if excerpt.starts_with("  file {path}") {
            excerpt = format!(
                "  file {path}: line counts differ (golden {} lines, actual {} lines)",
                committed_text.lines().count(),
                expected.lines().count()
            );
        }
        lines.push(excerpt);
    }
    lines.join("\n")
}

fn run_case(
    case: &Case,
    backing: Backing,
    update: bool,
    failures: &mut Vec<String>,
) {
    run_case_inner(case, backing, false, update, failures);
}

/// Run one golden scenario on a git-backed store and compare it against
/// the same committed replay golden.
///
/// The `.git` dir admits the cached path, so the seed and the act run the
/// single view algorithm through the cached backing (legacy stores still
/// decline to the replay backing, exactly as in production). The
/// read-model cache lives under `<git-dir>/sase/`, outside the beads dir,
/// so `snapshot_store` never sees it and every compared file must match
/// the replay golden byte for byte. `update` is never honored here:
/// goldens regenerate from the replay oracle only.
fn run_cached_case(case: &Case, backing: Backing, failures: &mut Vec<String>) {
    run_case_inner(case, backing, true, false, failures);
}

fn run_case_inner(
    case: &Case,
    backing: Backing,
    cached: bool,
    update: bool,
    failures: &mut Vec<String>,
) {
    let scenario = format!("{}_{}", case.name, backing.as_str());
    // Every tempdir below must outlive the act + snapshot.
    let event_temp = tempdir().expect("event seed tempdir");
    if cached {
        fs::create_dir_all(event_temp.path().join(".git")).unwrap();
    }
    let mut keep: Vec<tempfile::TempDir> = Vec::new();
    let (seed, beads_dir) = if case.seed == SeedKind::LegacyMixedPrefix {
        let (temp, dest) = legacy_mixed_prefix_store();
        if cached {
            fs::create_dir_all(temp.path().join(".git")).unwrap();
        }
        keep.push(temp);
        (SeedIds::default(), dest)
    } else {
        let event_beads = event_temp.path().join("beads");
        init_store(event_temp.path(), "beads", "sase", OWNER).unwrap();
        let seed = build_event_seed(case.seed, &event_beads);
        match backing {
            Backing::Event => (seed, event_beads),
            Backing::Legacy => {
                let (temp, dest) = if case.seed == SeedKind::Empty {
                    fresh_legacy_store()
                } else {
                    transplant_as_legacy(&event_beads)
                };
                if cached {
                    fs::create_dir_all(temp.path().join(".git")).unwrap();
                }
                keep.push(temp);
                (seed, dest)
            }
        }
    };
    if cached && backing == Backing::Event && case.seed != SeedKind::Empty {
        // Warm the read-model cache the way production reads do, so the
        // act below takes the cached path. (An `Empty` seed has no event
        // store yet, so its act is legitimately the store's first
        // mutation and runs the replay decline, exactly as in
        // production; those scenarios prove git-indifference only.)
        let cache_path =
            crate::bead::read_model::read_model_cache_path_for_store(
                &beads_dir,
            )
            .expect("cached golden run must admit the cached path");
        assert!(
            crate::bead::read_model::ensure_cache_ready_at(
                &beads_dir,
                &cache_path
            )
            .unwrap(),
            "scenario {scenario}: cache warm-up failed",
        );
    }
    let outcome = (case.act)(&beads_dir, &seed);
    if cached
        && backing == Backing::Event
        && case.cached_act
        && case.seed != SeedKind::Empty
    {
        // Without this the comparison could pass vacuously as
        // replay-vs-replay; the cache file proves the cached path ran.
        let cache_path =
            crate::bead::read_model::read_model_cache_path_for_store(
                &beads_dir,
            )
            .expect("cached golden run must admit the cached path");
        assert!(
            cache_path.is_file(),
            "scenario {scenario}: cached run left no cache file",
        );
    }
    let mut files = snapshot_store(&beads_dir);
    if case.normalize_remove {
        normalize_remove_timestamps(&mut files);
    }
    let golden = serde_json::json!({ "scenario": scenario, "outcome": outcome, "files": files });
    let expected = serde_json::to_string_pretty(&golden).unwrap() + "\n";
    let path = PathBuf::from(GOLDENS_DIR).join(format!("{scenario}.json"));
    if update {
        fs::create_dir_all(GOLDENS_DIR).unwrap();
        fs::write(&path, &expected).unwrap();
        return;
    }
    let committed = match fs::read_to_string(&path) {
        Ok(committed) => committed,
        Err(_) => {
            failures.push(format!(
                "scenario {scenario}: golden missing at {path:?}; regenerate with UPDATE_MUTATION_GOLDENS=1"
            ));
            return;
        }
    };
    if committed != expected {
        failures.push(diff_report(&scenario, &outcome, &files, &committed));
    }
}

#[test]
fn replay_golden_bytes_are_pinned() {
    let update =
        std::env::var("UPDATE_MUTATION_GOLDENS").ok().as_deref() == Some("1");
    let mut failures = Vec::new();
    for case in &cases() {
        for backing in case.backings.list() {
            run_case(case, *backing, update, &mut failures);
        }
    }
    assert!(
        failures.is_empty(),
        "{} replay golden mismatch(es):\n{}",
        failures.len(),
        failures.join("\n\n")
    );
}

/// Cached-mode runs produce the committed replay golden bytes
/// (`sase-1h8.13.1.9.8`).
///
/// Every golden scenario runs again on a git-backed store, where the
/// single view algorithm takes the cached path, and must produce the
/// identical outcome and identical bytes for every non-cache file. Any
/// mismatch is a defect in the unified algorithm: this test never
/// regenerates goldens, even under `UPDATE_MUTATION_GOLDENS=1`.
#[test]
fn cached_golden_bytes_match_replay() {
    let mut failures = Vec::new();
    for case in &cases() {
        for backing in case.backings.list() {
            run_cached_case(case, *backing, &mut failures);
        }
    }
    assert!(
        failures.is_empty(),
        "{} cached-vs-golden mismatch(es):\n{}",
        failures.len(),
        failures.join("\n\n")
    );
}
