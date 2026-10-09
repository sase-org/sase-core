//! Proof-phase parity across every mutation family (sase-1h8.13.1.7).
//!
//! The existing `bead_read_model_parity` harness randomizes over five
//! operations (note append, title update, top-level create, dependency
//! add, close). This sibling file extends the randomized
//! production-mutation sequence to every family: creates (top-level,
//! child, plan), update batches with external-ref exchange, note
//! append/edit/retract, close/open/close-with-note (including
//! descendant rejection, ancestor reopen and delegated completion),
//! removal cascades, claims and preclaim, ready marking, dependencies
//! and references, links and projections, +1/snooze/cancel, writes to
//! closed beads, and ID allocation including removal of the maximum
//! suffix. Every step runs the identical mutation against a cache-mode
//! store and a replay-mode store, then compares outcomes,
//! before-images, ordering and error kinds, plus the full cached state
//! against a full replay. Wall-clock stamps (`remove_*`) are normalized
//! before comparison so a second boundary cannot flake the run; every
//! other operation carries an explicit stamp and compares exactly.
//!
//! `bench_corpus_sampled_mutations` extends the bench-corpus sampled
//! parity to a sampled mutation sequence. Like the other bench-corpus
//! tests it runs only when `SASE_BEAD_BENCH_STORE` names a corpus.

use std::fs;
use std::path::{Path, PathBuf};

use sase_core::bead::events::import_issues_to_event_streams;
use sase_core::bead::jsonl::{parse_issues_jsonl, write_event_store};
use sase_core::bead::BeadError;
use sase_core::bead::{
    add_bead_link, add_bead_references, add_dependency, add_task_plus_one,
    append_issue_note, cancel_task_snooze, claim_for_agent_launch,
    claim_for_agent_wait, close_issues, close_issues_with_note, create_issue,
    edit_issue_note, mark_ready_to_work, open_issue, preclaim_epic_work_plan,
    read_model_status, read_model_verify_cache, read_store_issues,
    ready_issues, release_agent_claim, remove_bead_link,
    remove_bead_references, remove_dependencies, remove_issue,
    remove_issue_note, set_bead_link_projections, snooze_task,
    unmark_ready_to_work, update_issue, update_issues, BeadCreateRequestWire,
    BeadLinkProjectionRequestWire, BeadMutationOutcomeWire,
    BeadPreclaimAssignmentWire, BeadUpdateFieldsWire, IssueTypeWire,
    PhaseSizeWire,
};
use sase_core::bead::{blocked_issues, stats};
use sase_core::{ArtifactLinkOriginWire, BeadLinkDirectionWire};
use tempfile::tempdir;

const SEED_ISSUES_JSONL: &str = concat!(
    "{\"id\":\"bench-1\",\"title\":\"Epic one\",\"status\":\"open\",\"issue_type\":\"plan\",\"tier\":\"epic\",\"created_at\":\"2026-01-01T00:00:00Z\"}\n",
    "{\"id\":\"bench-1.1\",\"title\":\"Phase one\",\"status\":\"open\",\"issue_type\":\"phase\",\"parent_id\":\"bench-1\",\"created_at\":\"2026-01-01T00:01:00Z\"}\n",
    "{\"id\":\"bench-1.2\",\"title\":\"Phase two\",\"status\":\"closed\",\"issue_type\":\"phase\",\"parent_id\":\"bench-1\",\"created_at\":\"2026-01-01T00:02:00Z\"}\n",
    "{\"id\":\"bench-2\",\"title\":\"Ready task\",\"status\":\"ready\",\"issue_type\":\"task\",\"task_type\":\"feature\",\"created_at\":\"2026-01-01T00:03:00Z\"}\n",
    "{\"id\":\"bench-3\",\"title\":\"Blocked task\",\"status\":\"ready\",\"issue_type\":\"task\",\"task_type\":\"bug\",\"created_at\":\"2026-01-01T00:04:00Z\",\"dependencies\":[{\"issue_id\":\"bench-3\",\"depends_on_id\":\"bench-2\",\"created_at\":\"2026-01-01T00:04:00Z\",\"created_by\":\"\"}]}\n",
    "{\"id\":\"bench-4\",\"title\":\"External\",\"status\":\"open\",\"issue_type\":\"task\",\"created_at\":\"2026-01-01T00:05:00Z\",\"external_ref\":\"ext-1\"}\n",
    "{\"id\":\"bench-a1\",\"title\":\"Suffix task\",\"status\":\"open\",\"issue_type\":\"task\",\"task_type\":\"feature\",\"created_at\":\"2026-01-01T00:06:00Z\"}\n",
    "{\"id\":\"zz-a1\",\"title\":\"Suffix collision\",\"status\":\"open\",\"issue_type\":\"task\",\"created_at\":\"2026-01-01T00:07:00Z\"}\n",
    "{\"id\":\"bench-5\",\"title\":\"Flag task\",\"status\":\"open\",\"issue_type\":\"task\",\"task_type\":\"flag\",\"task_type_fields\":{\"remove_by_date\":\"2020-01-01\",\"remove_by_release\":\"0.0.0\"},\"created_at\":\"2026-01-01T00:08:00Z\"}\n",
    "{\"id\":\"bench-6\",\"title\":\"Closed task\",\"status\":\"closed\",\"issue_type\":\"task\",\"created_at\":\"2026-01-01T00:09:00Z\"}\n",
);

/// One step's mutation result, normalized for comparison.
type StepResult = Result<BeadMutationOutcomeWire, (String, String)>;

fn run_step(result: Result<BeadMutationOutcomeWire, BeadError>) -> StepResult {
    result.map_err(|error| (error.kind, error.message))
}

/// Replace every timestamp-shaped string with a placeholder so
/// wall-clock stamps (`remove_*` carries no explicit stamp) compare
/// without flaking across a second boundary. Explicit-stamp operations
/// still compare exactly: identical stamps normalize identically.
fn normalize(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::String(text) => {
            if is_timestamp(&text) {
                serde_json::Value::from("T")
            } else {
                serde_json::Value::String(text)
            }
        }
        serde_json::Value::Array(items) => {
            serde_json::Value::Array(items.into_iter().map(normalize).collect())
        }
        serde_json::Value::Object(map) => serde_json::Value::Object(
            map.into_iter()
                // `lock_wait_ms` is contention telemetry, not mutation
                // semantics: two back-to-back runs never agree on it.
                .filter(|(key, _)| key != "lock_wait_ms")
                .map(|(key, val)| (key, normalize(val)))
                .collect(),
        ),
        other => other,
    }
}

fn is_timestamp(text: &str) -> bool {
    let bytes = text.as_bytes();
    let is_date = bytes.len() == 10
        && bytes[4] == b'-'
        && bytes[7] == b'-'
        && bytes[..4].iter().all(|b| b.is_ascii_digit())
        && bytes[5..7].iter().all(|b| b.is_ascii_digit())
        && bytes[8..10].iter().all(|b| b.is_ascii_digit());
    let is_datetime = bytes.len() >= 20
        && bytes[4] == b'-'
        && bytes[7] == b'-'
        && bytes[10] == b'T'
        && bytes[13] == b':'
        && bytes[16] == b':'
        && (bytes.ends_with(b"Z") || bytes.contains(&b'.'));
    is_date || is_datetime
}

fn normalized_json(value: &serde_json::Value) -> String {
    serde_json::to_string(&normalize(value.clone())).unwrap()
}

fn normalized_outcome(result: &StepResult) -> serde_json::Value {
    match result {
        Ok(outcome) => serde_json::to_value(outcome).unwrap(),
        Err((kind, message)) => serde_json::json!([kind, message]),
    }
}

/// A cache-mode store plus its lockstep replay-mode twin. Both are
/// seeded identically; every mutation runs against both with identical
/// arguments.
struct FamilyStores {
    _temp: tempfile::TempDir,
    cache_dir: PathBuf,
    replay_dir: PathBuf,
}

fn seed_one(dir: &Path, git: bool) {
    if git {
        fs::create_dir_all(dir.join(".git")).unwrap();
    }
    fs::create_dir_all(dir).unwrap();
    fs::write(dir.join("config.json"), "{}\n").unwrap();
    let outcome = parse_issues_jsonl(SEED_ISSUES_JSONL);
    assert_eq!(outcome.loaded_rows, 10);
    let streams = import_issues_to_event_streams(&outcome.issues).unwrap();
    write_event_store(dir, &streams).unwrap();
}

fn seed_stores() -> FamilyStores {
    let temp = tempdir().unwrap();
    let cache_dir = temp.path().join("cache-store");
    let replay_dir = temp.path().join("replay-store");
    for dir in [&cache_dir, &replay_dir] {
        seed_one(dir, dir == &cache_dir);
    }
    // Identical stamped history on both backings: a link, a +1 and a
    // note, so provenance and evidence neighborhoods start non-empty.
    for dir in [&cache_dir, &replay_dir] {
        add_bead_link(
            dir,
            "bench-2",
            "artifact:demo",
            "related",
            "family seed link",
            ArtifactLinkOriginWire::Manual,
            BeadLinkDirectionWire::Out,
            1,
            Some("2026-01-01T00:06:00Z".to_string()),
            None,
        )
        .unwrap();
        add_task_plus_one(
            dir,
            "bench-2",
            "family-seed",
            "family evidence",
            &[],
            Some("2026-01-01T00:06:45Z".to_string()),
            None,
            None,
        )
        .unwrap();
        append_issue_note(
            dir,
            "bench-1.1",
            "family seed note",
            Some("family".to_string()),
            Some("2026-01-01T00:07:00Z".to_string()),
            None,
        )
        .unwrap();
    }
    FamilyStores {
        _temp: temp,
        cache_dir,
        replay_dir,
    }
}

/// Run one mutation against both backings, compare outcomes (success
/// values with before-images, or error kinds and text), then compare
/// the full cached state against a full replay and require the
/// read-model verifier to match.
fn assert_family_step(
    stores: &FamilyStores,
    label: &str,
    op: impl Fn(&Path) -> Result<BeadMutationOutcomeWire, BeadError>,
) {
    let cached = run_step(op(&stores.cache_dir));
    let replayed = run_step(op(&stores.replay_dir));
    assert_eq!(
        normalized_json(&normalized_outcome(&cached)),
        normalized_json(&normalized_outcome(&replayed)),
        "{label}: outcome differs between cache and replay"
    );
    let cached_state = normalized_json(
        &serde_json::to_value(read_store_issues(&stores.cache_dir).unwrap())
            .unwrap(),
    );
    let replayed_state = normalized_json(
        &serde_json::to_value(read_store_issues(&stores.replay_dir).unwrap())
            .unwrap(),
    );
    assert_eq!(
        cached_state, replayed_state,
        "{label}: cached state differs from full replay"
    );
    let verify = read_model_verify_cache(&stores.cache_dir);
    assert!(verify.compared, "{label}: {}", verify.reason);
    assert!(verify.matched, "{label}: {}", verify.reason);
}

/// Live IDs by role, reread from the cache store before each step. The
/// replay twin holds identical IDs while the stores stay in lockstep.
struct Snapshot {
    ids: Vec<String>,
    open: Vec<String>,
    closed: Vec<String>,
    plans: Vec<String>,
    phases: Vec<String>,
    with_notes: Vec<(String, String)>,
    with_deps: Vec<(String, String)>,
    with_refs: Vec<(String, String)>,
}

fn snapshot(dir: &Path) -> Snapshot {
    let issues = read_store_issues(dir).unwrap();
    let mut snap = Snapshot {
        ids: Vec::new(),
        open: Vec::new(),
        closed: Vec::new(),
        plans: Vec::new(),
        phases: Vec::new(),
        with_notes: Vec::new(),
        with_deps: Vec::new(),
        with_refs: Vec::new(),
    };
    for issue in &issues {
        snap.ids.push(issue.id.clone());
        let value = serde_json::to_value(issue).unwrap();
        let status = value.get("status").and_then(|s| s.as_str()).unwrap_or("");
        if status == "closed" {
            snap.closed.push(issue.id.clone());
        } else {
            snap.open.push(issue.id.clone());
        }
        let kind = value
            .get("issue_type")
            .and_then(|s| s.as_str())
            .unwrap_or("");
        if kind == "plan" {
            snap.plans.push(issue.id.clone());
        }
        if kind == "phase" {
            snap.phases.push(issue.id.clone());
        }
        if let Some(notes) = value.get("notes").and_then(|n| n.as_array()) {
            for note in notes {
                if let Some(id) = note.get("id").and_then(|i| i.as_str()) {
                    snap.with_notes.push((issue.id.clone(), id.to_string()));
                }
            }
        }
        if let Some(deps) = value.get("dependencies").and_then(|d| d.as_array())
        {
            for dep in deps {
                if let Some(target) =
                    dep.get("depends_on_id").and_then(|i| i.as_str())
                {
                    snap.with_deps.push((issue.id.clone(), target.to_string()));
                }
            }
        }
        if let Some(text) = value.get("external_ref").and_then(|r| r.as_str()) {
            if !text.is_empty() {
                snap.with_refs.push((issue.id.clone(), text.to_string()));
            }
        }
    }
    snap
}

/// A monotonic future clock: every stamped step sorts after the stored
/// frontier, so steps exercise the cached path instead of the
/// backdated-rebuild path (which the adversarial test already covers).
fn step_stamp(step: usize) -> String {
    format!("2027-01-01T00:{:02}:{:02}Z", (step / 60) % 60, step % 60)
}

#[test]
fn cache_matches_replay_after_every_family_mutation() {
    let stores = seed_stores();
    // The read model starts warm and matching on both lanes.
    let verify = read_model_verify_cache(&stores.cache_dir);
    assert!(verify.matched, "{}", verify.reason);
    let mut rng: u64 = 0x9e37_79b9_7f4a_7c15;
    let mut next_rng = || {
        rng = rng
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (rng >> 33) as usize
    };
    for step in 0..60 {
        let snap = snapshot(&stores.cache_dir);
        assert!(!snap.ids.is_empty(), "step {step}: store ran dry");
        // Lockstep: the replay twin must hold identical IDs.
        let replay_ids: Vec<String> = read_store_issues(&stores.replay_dir)
            .unwrap()
            .iter()
            .map(|issue| issue.id.clone())
            .collect();
        assert_eq!(
            snap.ids, replay_ids,
            "step {step}: twins diverged before the mutation"
        );
        let id = |n: usize| snap.ids[n % snap.ids.len()].clone();
        let open_id = |n: usize| {
            if snap.open.is_empty() {
                id(n)
            } else {
                snap.open[n % snap.open.len()].clone()
            }
        };
        let stamp = step_stamp(step + 10);
        let arm = next_rng() % 30;
        match arm {
            // Notes family.
            0 => {
                let target = open_id(next_rng());
                let text = format!("family note {step}");
                assert_family_step(
                    &stores,
                    &format!("note append {step}"),
                    |dir| {
                        append_issue_note(
                            dir,
                            &target,
                            &text,
                            Some("family".to_string()),
                            Some(stamp.clone()),
                            None,
                        )
                    },
                );
            }
            1 => {
                if let Some((issue_id, note_id)) = snap
                    .with_notes
                    .get(next_rng() % snap.with_notes.len().max(1))
                    .cloned()
                {
                    assert_family_step(
                        &stores,
                        &format!("note edit {step}"),
                        |dir| {
                            edit_issue_note(
                                dir,
                                &issue_id,
                                &note_id,
                                &format!("family note {step} edited"),
                                Some("family".to_string()),
                                Some(stamp.clone()),
                                None,
                            )
                        },
                    );
                } else {
                    let target = open_id(next_rng());
                    assert_family_step(
                        &stores,
                        &format!("note append {step}"),
                        |dir| {
                            append_issue_note(
                                dir,
                                &target,
                                &format!("family note {step}"),
                                Some("family".to_string()),
                                Some(stamp.clone()),
                                None,
                            )
                        },
                    );
                }
            }
            2 => {
                if let Some((issue_id, note_id)) = snap
                    .with_notes
                    .get(next_rng() % snap.with_notes.len().max(1))
                    .cloned()
                {
                    assert_family_step(
                        &stores,
                        &format!("note retract {step}"),
                        |dir| {
                            remove_issue_note(
                                dir,
                                &issue_id,
                                &note_id,
                                Some("family".to_string()),
                                Some(stamp.clone()),
                            )
                        },
                    );
                } else {
                    let target = open_id(next_rng());
                    assert_family_step(
                        &stores,
                        &format!("note append {step}"),
                        |dir| {
                            append_issue_note(
                                dir,
                                &target,
                                &format!("family note {step}"),
                                Some("family".to_string()),
                                Some(stamp.clone()),
                                None,
                            )
                        },
                    );
                }
            }
            // Update batches, including external-ref exchange.
            3 => {
                let first = id(next_rng());
                let second = id(next_rng() + 1);
                let title = format!("Renamed at step {step}");
                assert_family_step(
                    &stores,
                    &format!("update batch {step}"),
                    |dir| {
                        update_issues(
                            dir,
                            &[first.clone(), second.clone()],
                            BeadUpdateFieldsWire {
                                title: Some(title.clone()),
                                now: Some(stamp.clone()),
                                ..Default::default()
                            },
                        )
                    },
                );
            }
            4 => {
                if snap.with_refs.len() >= 2 {
                    let (first_id, first_ref) = snap.with_refs
                        [next_rng() % snap.with_refs.len()]
                    .clone();
                    let (second_id, second_ref) = snap.with_refs
                        [(next_rng() + 1) % snap.with_refs.len()]
                    .clone();
                    // A true exchange through a temporary free ref: each
                    // intermediate overlay stays unique on both lanes,
                    // and the endpoints end up swapped.
                    let stages = vec![
                        (first_id.clone(), format!("tmp:family:{step}")),
                        (second_id.clone(), first_ref.clone()),
                        (first_id.clone(), second_ref.clone()),
                    ];
                    for (index, (issue_id, new_ref)) in
                        stages.into_iter().enumerate()
                    {
                        assert_family_step(
                            &stores,
                            &format!("external-ref stage {step}.{index}"),
                            |dir| {
                                update_issue(
                                    dir,
                                    &issue_id,
                                    BeadUpdateFieldsWire {
                                        external_ref: Some(new_ref.clone()),
                                        now: Some(stamp.clone()),
                                        ..Default::default()
                                    },
                                )
                            },
                        );
                    }
                } else {
                    let target = open_id(next_rng());
                    let new_ref = format!("ext-family-{step}");
                    assert_family_step(
                        &stores,
                        &format!("external-ref set {step}"),
                        |dir| {
                            update_issue(
                                dir,
                                &target,
                                BeadUpdateFieldsWire {
                                    external_ref: Some(new_ref.clone()),
                                    now: Some(stamp.clone()),
                                    ..Default::default()
                                },
                            )
                        },
                    );
                }
            }
            // Creates: top-level task, child phase, plan.
            5 => {
                assert_family_step(
                    &stores,
                    &format!("create task {step}"),
                    |dir| {
                        create_issue(
                            dir,
                            BeadCreateRequestWire {
                                title: format!("family task {step}"),
                                issue_type: IssueTypeWire::Task,
                                size: Some(PhaseSizeWire::Small),
                                task_type: Some("feature".to_string()),
                                now: Some(stamp.clone()),
                                ..Default::default()
                            },
                        )
                    },
                );
            }
            6 => {
                let parent = if snap.phases.is_empty() {
                    if snap.plans.is_empty() {
                        open_id(next_rng())
                    } else {
                        snap.plans[next_rng() % snap.plans.len()].clone()
                    }
                } else {
                    snap.phases[next_rng() % snap.phases.len()].clone()
                };
                assert_family_step(
                    &stores,
                    &format!("create child {step}"),
                    |dir| {
                        create_issue(
                            dir,
                            BeadCreateRequestWire {
                                title: format!("family child {step}"),
                                issue_type: IssueTypeWire::Phase,
                                parent_id: Some(parent.clone()),
                                size: Some(PhaseSizeWire::Small),
                                now: Some(stamp.clone()),
                                ..Default::default()
                            },
                        )
                    },
                );
            }
            7 => {
                assert_family_step(
                    &stores,
                    &format!("create plan {step}"),
                    |dir| {
                        create_issue(
                            dir,
                            BeadCreateRequestWire {
                                title: format!("family plan {step}"),
                                issue_type: IssueTypeWire::Plan,
                                now: Some(stamp.clone()),
                                ..Default::default()
                            },
                        )
                    },
                );
            }
            // Lifecycle: close, close with note, open, remove.
            8 => {
                let target = open_id(next_rng());
                assert_family_step(&stores, &format!("close {step}"), |dir| {
                    close_issues(
                        dir,
                        std::slice::from_ref(&target),
                        Some(format!("family close {step}")),
                        None,
                        false,
                        Some(stamp.clone()),
                    )
                });
            }
            9 => {
                let first = open_id(next_rng());
                let second = open_id(next_rng() + 1);
                assert_family_step(
                    &stores,
                    &format!("close with note {step}"),
                    |dir| {
                        close_issues_with_note(
                            dir,
                            &[first.clone(), second.clone()],
                            Some(format!("family close {step}")),
                            None,
                            false,
                            Some(format!("closing evidence {step}")),
                            Some("family".to_string()),
                            Some(stamp.clone()),
                            None,
                        )
                    },
                );
            }
            10 => {
                let target = if snap.closed.is_empty() {
                    id(next_rng())
                } else {
                    snap.closed[next_rng() % snap.closed.len()].clone()
                };
                assert_family_step(&stores, &format!("open {step}"), |dir| {
                    open_issue(dir, &target, Some(stamp.clone()))
                });
            }
            11 => {
                // Never drain the store: keep at least two beads.
                if snap.ids.len() <= 2 {
                    assert_family_step(
                        &stores,
                        &format!("create task {step}"),
                        |dir| {
                            create_issue(
                                dir,
                                BeadCreateRequestWire {
                                    title: format!("family task {step}"),
                                    issue_type: IssueTypeWire::Task,
                                    size: Some(PhaseSizeWire::Small),
                                    task_type: Some("bug".to_string()),
                                    now: Some(stamp.clone()),
                                    ..Default::default()
                                },
                            )
                        },
                    );
                } else {
                    let target = id(next_rng());
                    assert_family_step(
                        &stores,
                        &format!("remove {step}"),
                        |dir| remove_issue(dir, &target),
                    );
                }
            }
            // Claims, preclaim, ready marking.
            12 => {
                let target = open_id(next_rng());
                let agent = format!("family-agent-{}", step % 3);
                assert_family_step(
                    &stores,
                    &format!("claim launch {step}"),
                    |dir| {
                        claim_for_agent_launch(
                            dir,
                            &target,
                            &agent,
                            Some(stamp.clone()),
                        )
                    },
                );
            }
            13 => {
                let target = open_id(next_rng() + 1);
                assert_family_step(
                    &stores,
                    &format!("claim wait {step}"),
                    |dir| {
                        claim_for_agent_wait(
                            dir,
                            &target,
                            "family-waiter",
                            Some(stamp.clone()),
                        )
                    },
                );
            }
            14 => {
                let target = open_id(next_rng() + 2);
                assert_family_step(
                    &stores,
                    &format!("claim release {step}"),
                    |dir| {
                        release_agent_claim(
                            dir,
                            &target,
                            &format!("family-agent-{}", step % 3),
                            Some(stamp.clone()),
                        )
                    },
                );
            }
            15 => {
                let open_phases: Vec<String> = snap
                    .phases
                    .iter()
                    .filter(|id| snap.open.contains(id))
                    .cloned()
                    .collect();
                let assignments: Vec<BeadPreclaimAssignmentWire> = open_phases
                    .iter()
                    .take(2)
                    .enumerate()
                    .map(|(index, phase)| BeadPreclaimAssignmentWire {
                        bead_id: phase.clone(),
                        agent_name: format!("family-worker-{index}"),
                    })
                    .collect();
                let epic = snap
                    .plans
                    .iter()
                    .find(|id| snap.open.contains(id))
                    .cloned()
                    .unwrap_or_else(|| open_id(next_rng()));
                assert_family_step(
                    &stores,
                    &format!("preclaim {step}"),
                    |dir| {
                        preclaim_epic_work_plan(
                            dir,
                            &epic,
                            &assignments,
                            None,
                            Some(stamp.clone()),
                        )
                    },
                );
            }
            16 => {
                let target = if snap.plans.is_empty() {
                    open_id(next_rng())
                } else {
                    snap.plans[next_rng() % snap.plans.len()].clone()
                };
                assert_family_step(
                    &stores,
                    &format!("mark ready {step}"),
                    |dir| mark_ready_to_work(dir, &target, Some(stamp.clone())),
                );
            }
            17 => {
                let target = if snap.plans.is_empty() {
                    open_id(next_rng())
                } else {
                    snap.plans[next_rng() % snap.plans.len()].clone()
                };
                assert_family_step(
                    &stores,
                    &format!("unmark ready {step}"),
                    |dir| {
                        unmark_ready_to_work(dir, &target, Some(stamp.clone()))
                    },
                );
            }
            // Dependencies and references.
            18 => {
                let first = id(next_rng());
                let second = id(next_rng() + 1);
                assert_family_step(
                    &stores,
                    &format!("dependency add {step}"),
                    |dir| {
                        add_dependency(
                            dir,
                            &first,
                            &second,
                            Some(stamp.clone()),
                        )
                    },
                );
            }
            19 => {
                if let Some((issue_id, target)) = snap
                    .with_deps
                    .get(next_rng() % snap.with_deps.len().max(1))
                    .cloned()
                {
                    assert_family_step(
                        &stores,
                        &format!("dependency remove {step}"),
                        |dir| {
                            remove_dependencies(
                                dir,
                                &issue_id,
                                std::slice::from_ref(&target),
                                Some(stamp.clone()),
                            )
                        },
                    );
                } else {
                    let target = open_id(next_rng());
                    assert_family_step(
                        &stores,
                        &format!("reference add {step}"),
                        |dir| {
                            add_bead_references(
                                dir,
                                &target,
                                &[format!("artifact:family:{step}")],
                                Some(stamp.clone()),
                            )
                        },
                    );
                }
            }
            20 => {
                let target = open_id(next_rng());
                assert_family_step(
                    &stores,
                    &format!("reference add {step}"),
                    |dir| {
                        add_bead_references(
                            dir,
                            &target,
                            &[format!("artifact:family:{step}")],
                            Some(stamp.clone()),
                        )
                    },
                );
            }
            21 => {
                let target = open_id(next_rng() + 1);
                assert_family_step(
                    &stores,
                    &format!("reference remove {step}"),
                    |dir| {
                        remove_bead_references(
                            dir,
                            &target,
                            &[format!("artifact:family:{step}")],
                            Some(stamp.clone()),
                        )
                    },
                );
            }
            // Links, projections, receipts.
            22 => {
                let target = open_id(next_rng());
                let link_target = if step % 3 == 0 {
                    "artifact:stable".to_string()
                } else {
                    format!("artifact:family:{step}")
                };
                assert_family_step(
                    &stores,
                    &format!("link add {step}"),
                    |dir| {
                        add_bead_link(
                            dir,
                            &target,
                            &link_target,
                            "related",
                            &format!("family link {step}"),
                            ArtifactLinkOriginWire::Manual,
                            BeadLinkDirectionWire::Out,
                            1,
                            Some(stamp.clone()),
                            None,
                        )
                    },
                );
            }
            23 => {
                let target = open_id(next_rng());
                let request = BeadLinkProjectionRequestWire {
                    issue_id: target.clone(),
                    target_ref: "artifact:stable".to_string(),
                    relation: "related".to_string(),
                    direction: BeadLinkDirectionWire::Out,
                    present: step % 2 == 0,
                    operation_id: format!("{step:032x}"),
                    description: Some(format!("family projection {step}")),
                    origin: Some(ArtifactLinkOriginWire::Manual),
                    uses: 1,
                    now: Some(stamp.clone()),
                };
                assert_family_step(
                    &stores,
                    &format!("link projection {step}"),
                    |dir| {
                        set_bead_link_projections(
                            dir,
                            std::slice::from_ref(&request),
                        )
                    },
                );
            }
            24 => {
                let target = open_id(next_rng() + 1);
                assert_family_step(
                    &stores,
                    &format!("link remove {step}"),
                    |dir| {
                        remove_bead_link(
                            dir,
                            &target,
                            "artifact:stable",
                            Some("related"),
                            BeadLinkDirectionWire::Out,
                            Some(stamp.clone()),
                            None,
                        )
                    },
                );
            }
            // +1 evidence, snooze, cancel.
            25 => {
                let target = open_id(next_rng());
                let reporter = format!("family-reporter-{}", step % 4);
                assert_family_step(
                    &stores,
                    &format!("plus one {step}"),
                    |dir| {
                        add_task_plus_one(
                            dir,
                            &target,
                            &reporter,
                            &format!("family evidence {step}"),
                            &[],
                            Some(stamp.clone()),
                            None,
                            None,
                        )
                    },
                );
            }
            26 => {
                let target = open_id(next_rng() + 1);
                assert_family_step(&stores, &format!("snooze {step}"), |dir| {
                    snooze_task(
                        dir,
                        &target,
                        "2027-06-01T00:00:00Z",
                        None,
                        &format!("family snooze {step}"),
                        "family",
                        Some(stamp.clone()),
                    )
                });
            }
            27 => {
                let target = open_id(next_rng() + 2);
                assert_family_step(
                    &stores,
                    &format!("snooze cancel {step}"),
                    |dir| {
                        cancel_task_snooze(
                            dir,
                            &target,
                            "family",
                            Some(stamp.clone()),
                        )
                    },
                );
            }
            // Writes to closed beads and single updates.
            28 => {
                let target = if snap.closed.is_empty() {
                    id(next_rng())
                } else {
                    snap.closed[next_rng() % snap.closed.len()].clone()
                };
                assert_family_step(
                    &stores,
                    &format!("closed-bead note {step}"),
                    |dir| {
                        append_issue_note(
                            dir,
                            &target,
                            &format!("family closed note {step}"),
                            Some("family".to_string()),
                            Some(stamp.clone()),
                            None,
                        )
                    },
                );
            }
            _ => {
                let target = id(next_rng());
                assert_family_step(
                    &stores,
                    &format!("single update {step}"),
                    |dir| {
                        update_issue(
                            dir,
                            &target,
                            BeadUpdateFieldsWire {
                                assignee: Some(format!(
                                    "family-assignee-{step}"
                                )),
                                now: Some(stamp.clone()),
                                ..Default::default()
                            },
                        )
                    },
                );
            }
        }
    }
    // Final read-surface comparison: ready/blocked lists and stats agree.
    for (label, cached, replayed) in [
        (
            "ready list",
            serde_json::to_value(ready_issues(&stores.cache_dir).unwrap())
                .unwrap(),
            serde_json::to_value(ready_issues(&stores.replay_dir).unwrap())
                .unwrap(),
        ),
        (
            "blocked list",
            serde_json::to_value(blocked_issues(&stores.cache_dir).unwrap())
                .unwrap(),
            serde_json::to_value(blocked_issues(&stores.replay_dir).unwrap())
                .unwrap(),
        ),
        (
            "stats",
            serde_json::to_value(stats(&stores.cache_dir).unwrap()).unwrap(),
            serde_json::to_value(stats(&stores.replay_dir).unwrap()).unwrap(),
        ),
    ] {
        assert_eq!(
            normalized_json(&cached),
            normalized_json(&replayed),
            "{label} differs after the family sequence"
        );
    }
    let status = read_model_status(&stores.cache_dir);
    assert!(status.fresh, "{}", status.reason);
    let verify = read_model_verify_cache(&stores.cache_dir);
    assert!(verify.compared, "{}", verify.reason);
    assert!(verify.matched, "{}", verify.reason);
}

/// Sampled mutation parity over a real bench corpus when one is
/// provided: a short mutation sequence touches stride-sampled beads
/// and asserts cache-equals-replay after every mutation. Set
/// `SASE_BEAD_BENCH_STORE` to a bench corpus directory (see
/// `just bead-perf-scale`); without the variable the test passes
/// trivially. The corpus is copied to scratch first: the sequence
/// mutates. CI's record-only 4x run sets the variable.
#[test]
fn bench_corpus_sampled_mutations() {
    let Some(source) = std::env::var("SASE_BEAD_BENCH_STORE")
        .ok()
        .map(PathBuf::from)
    else {
        return;
    };
    let scratch = tempdir().unwrap();
    let target = scratch.path().join("corpus");
    copy_tree(&source, &target);
    let beads_dir = target.join("store");
    let cached = read_store_issues(&beads_dir).unwrap();
    assert!(!cached.is_empty());
    let stride = (cached.len() / 12).max(1);
    let sampled: Vec<String> = cached
        .iter()
        .step_by(stride)
        .take(12)
        .map(|issue| issue.id.clone())
        .collect();
    assert!(!sampled.is_empty());
    // Warm the cache, then mutate each sampled bead once through a
    // different family and compare against a full replay every time.
    // Most corpus beads are closed: closed beads take the proven
    // closed-bead note path, open beads rotate through four families.
    read_store_issues(&beads_dir).unwrap();
    for (index, id) in sampled.iter().enumerate() {
        let stamp = format!("2031-01-01T00:{index:02}:00Z");
        let is_closed = cached
            .iter()
            .find(|issue| &issue.id == id)
            .and_then(|issue| serde_json::to_value(issue).ok())
            .and_then(|value| {
                value
                    .get("status")
                    .and_then(|s| s.as_str())
                    .map(str::to_string)
            })
            .as_deref()
            == Some("closed");
        match (is_closed, index % 4) {
            (true, _) | (false, 0) => {
                append_issue_note(
                    &beads_dir,
                    id,
                    &format!("corpus parity note {index}"),
                    Some("family".to_string()),
                    Some(stamp),
                    None,
                )
                .unwrap();
            }
            (false, 1) => {
                update_issue(
                    &beads_dir,
                    id,
                    BeadUpdateFieldsWire {
                        title: Some(format!("Corpus rename {index}")),
                        now: Some(stamp),
                        ..Default::default()
                    },
                )
                .unwrap();
            }
            (false, 2) => {
                add_task_plus_one(
                    &beads_dir,
                    id,
                    &format!("corpus-reporter-{index}"),
                    "corpus evidence",
                    &[],
                    Some(stamp),
                    None,
                    None,
                )
                .unwrap();
            }
            _ => {
                add_bead_link(
                    &beads_dir,
                    id,
                    &format!("artifact:corpus:{index}"),
                    "related",
                    "corpus link",
                    ArtifactLinkOriginWire::Manual,
                    BeadLinkDirectionWire::Out,
                    1,
                    Some(stamp),
                    None,
                )
                .unwrap();
            }
        }
        let replayed =
            sase_core::bead::read_event_store_issues(&beads_dir).unwrap();
        let live = read_store_issues(&beads_dir).unwrap();
        assert_eq!(
            serde_json::to_value(&live).unwrap(),
            serde_json::to_value(&replayed).unwrap(),
            "corpus mutation {index} on {id} differs from full replay"
        );
        let verify = read_model_verify_cache(&beads_dir);
        assert!(verify.compared, "{}", verify.reason);
        assert!(verify.matched, "{}", verify.reason);
    }
}

fn copy_tree(source: &Path, dest: &Path) {
    fs::create_dir_all(dest).unwrap();
    for entry in fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let from = entry.path();
        let to = dest.join(entry.file_name());
        if from.is_dir() {
            copy_tree(&from, &to);
        } else {
            fs::copy(&from, &to).unwrap();
        }
    }
}
