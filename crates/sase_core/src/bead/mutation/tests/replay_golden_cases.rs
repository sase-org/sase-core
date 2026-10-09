//! Scenario table for the replay golden tests (`sase-1h8.13.1.9.2`).
//!
//! Pure move from `replay_goldens.rs`: the `Case` rows and their act
//! helpers live here so both golden files stay at or under 1,500 lines.
//! No behavior change; the harness and both `#[test]` functions stay in
//! `replay_goldens.rs`.

use super::super::*;
use super::replay_goldens::{
    outcome_string, some_now, Backings, SeedIds, SeedKind, AGENT, OWNER,
};
use crate::artifact_link::ArtifactLinkOriginWire;
use crate::artifact_link::BeadLinkDirectionWire;
use crate::bead::wire::BeadResolutionWire;
use crate::bead::wire::IssueTypeWire;
use crate::bead::wire::PhaseSizeWire;
use std::path::Path;

pub(super) struct Case {
    pub(super) name: &'static str,
    pub(super) seed: SeedKind,
    pub(super) backings: Backings,
    pub(super) normalize_remove: bool,
    /// The act runs through `run_mutation` (and, on a git-backed store,
    /// through the cached path). Controls such as `init_store` and
    /// `export_jsonl` bypass the runner, so the cached-byte test compares
    /// their bytes without asserting the cached path ran.
    pub(super) cached_act: bool,
    pub(super) act: fn(&Path, &SeedIds) -> String,
}

pub(super) fn case(
    name: &'static str,
    seed: SeedKind,
    backings: Backings,
    act: fn(&Path, &SeedIds) -> String,
) -> Case {
    Case {
        name,
        seed,
        backings,
        normalize_remove: false,
        cached_act: true,
        act,
    }
}

/// A control scenario whose act bypasses the mutation runner.
pub(super) fn control_case(
    name: &'static str,
    seed: SeedKind,
    backings: Backings,
    act: fn(&Path, &SeedIds) -> String,
) -> Case {
    Case {
        name,
        seed,
        backings,
        normalize_remove: false,
        cached_act: false,
        act,
    }
}

pub(super) fn remove_case(
    name: &'static str,
    seed: SeedKind,
    act: fn(&Path, &SeedIds) -> String,
) -> Case {
    Case {
        name,
        seed,
        backings: Backings::Both,
        normalize_remove: true,
        cached_act: true,
        act,
    }
}

// Case-row wrappers: each pins the shared ACT timestamp and returns the
// serialized outcome-or-error, so rows stay one-liners.
fn mk(
    dir: &Path,
    title: &str,
    issue_type: IssueTypeWire,
    size: Option<PhaseSizeWire>,
    task_type: Option<&str>,
    parent: Option<&str>,
) -> String {
    outcome_string(create_issue(
        dir,
        BeadCreateRequestWire {
            title: title.to_string(),
            issue_type,
            parent_id: parent.map(str::to_string),
            size,
            task_type: task_type.map(str::to_string),
            created_by: Some("creator-agent".to_string()),
            now: some_now(),
            ..Default::default()
        },
    ))
}

fn upd_title(dir: &Path, id: &str, title: &str) -> String {
    outcome_string(update_issue(
        dir,
        id,
        BeadUpdateFieldsWire {
            title: Some(title.to_string()),
            now: some_now(),
            ..Default::default()
        },
    ))
}

fn upd_pair(dir: &Path, first: &str, second: &str, title: &str) -> String {
    outcome_string(update_issues(
        dir,
        &[first.to_string(), second.to_string()],
        BeadUpdateFieldsWire {
            title: Some(title.to_string()),
            now: some_now(),
            ..Default::default()
        },
    ))
}

fn nt_append(dir: &Path, id: &str, entry: &str) -> String {
    outcome_string(append_issue_note(
        dir,
        id,
        entry,
        Some("agent-1".to_string()),
        some_now(),
        None,
    ))
}

fn nt_edit(dir: &Path, id: &str, note: &str, text: &str) -> String {
    outcome_string(edit_issue_note(
        dir,
        id,
        note,
        text,
        Some("agent-2".to_string()),
        some_now(),
        None,
    ))
}

fn nt_remove(dir: &Path, id: &str, note: &str) -> String {
    outcome_string(remove_issue_note(
        dir,
        id,
        note,
        Some("agent-2".to_string()),
        some_now(),
    ))
}

fn op_open(dir: &Path, id: &str) -> String {
    outcome_string(open_issue(dir, id, some_now()))
}

fn cl_close(
    dir: &Path,
    ids: &[String],
    reason: &str,
    resolution: BeadResolutionWire,
    force: bool,
) -> String {
    outcome_string(close_issues(
        dir,
        ids,
        Some(reason.to_string()),
        Some(resolution),
        force,
        some_now(),
    ))
}

fn cl_close1(
    dir: &Path,
    id: &str,
    reason: &str,
    resolution: BeadResolutionWire,
    force: bool,
) -> String {
    let ids = [id.to_string()];
    outcome_string(close_issues(
        dir,
        &ids,
        Some(reason.to_string()),
        Some(resolution),
        force,
        some_now(),
    ))
}

fn cl_note(dir: &Path, id: &str, note: &str) -> String {
    outcome_string(close_issues_with_note(
        dir,
        &[id.to_string()],
        Some("done".to_string()),
        Some(BeadResolutionWire::Done),
        false,
        Some(note.to_string()),
        Some("agent-1".to_string()),
        some_now(),
        None,
    ))
}

fn rm_many(dir: &Path, ids: &[String]) -> String {
    outcome_string(remove_issues(dir, ids))
}

fn rm_one(dir: &Path, id: &str) -> String {
    outcome_string(remove_issue(dir, id))
}

fn cl_launch(dir: &Path, id: &str, agent: &str) -> String {
    outcome_string(claim_for_agent_launch(dir, id, agent, some_now()))
}

fn cl_wait(dir: &Path, id: &str, agent: &str) -> String {
    outcome_string(claim_for_agent_wait(dir, id, agent, some_now()))
}

fn cl_release(dir: &Path, id: &str, agent: &str) -> String {
    outcome_string(release_agent_claim(dir, id, agent, some_now()))
}

fn preclaim_ok(dir: &Path, epic: &str, first: &str, second: &str) -> String {
    outcome_string(preclaim_epic_work_plan(
        dir,
        epic,
        &[
            BeadPreclaimAssignmentWire {
                bead_id: first.to_string(),
                agent_name: "agent-1".to_string(),
            },
            BeadPreclaimAssignmentWire {
                bead_id: second.to_string(),
                agent_name: "agent-2".to_string(),
            },
        ],
        Some("land-agent".to_string()),
        some_now(),
    ))
}

fn preclaim_bad(dir: &Path, phase: &str, task: &str) -> String {
    outcome_string(preclaim_epic_work_plan(
        dir,
        phase,
        &[BeadPreclaimAssignmentWire {
            bead_id: task.to_string(),
            agent_name: "agent-2".to_string(),
        }],
        Some("land-agent".to_string()),
        some_now(),
    ))
}

fn mk_ready(dir: &Path, id: &str) -> String {
    outcome_string(mark_ready_to_work(dir, id, some_now()))
}

fn unmk_ready(dir: &Path, id: &str) -> String {
    outcome_string(unmark_ready_to_work(dir, id, some_now()))
}

fn dep_add(dir: &Path, first: &str, second: &str) -> String {
    outcome_string(add_dependency(dir, first, second, some_now()))
}

fn dep_rm(dir: &Path, first: &str, second: &str) -> String {
    outcome_string(remove_dependencies(
        dir,
        first,
        &[second.to_string()],
        some_now(),
    ))
}

fn ref_add(dir: &Path, id: &str, reference: &str) -> String {
    outcome_string(add_bead_references(
        dir,
        id,
        &[reference.to_string()],
        some_now(),
    ))
}

fn ref_rm(dir: &Path, id: &str, reference: &str) -> String {
    outcome_string(remove_bead_references(
        dir,
        id,
        &[reference.to_string()],
        some_now(),
    ))
}

fn lnk_add(dir: &Path, first: &str, target: &str, relation: &str) -> String {
    outcome_string(add_bead_link(
        dir,
        first,
        target,
        relation,
        "shares a root cause",
        ArtifactLinkOriginWire::Manual,
        BeadLinkDirectionWire::Out,
        1,
        some_now(),
        None,
    ))
}

fn lnk_rm(dir: &Path, first: &str, target: &str, relation: &str) -> String {
    outcome_string(remove_bead_link(
        dir,
        first,
        target,
        Some(relation),
        BeadLinkDirectionWire::Out,
        some_now(),
        None,
    ))
}

fn proj(dir: &Path, first: &str, target: &str, operation: &str) -> String {
    outcome_string(set_bead_link_projection(
        dir,
        first,
        target,
        "related",
        BeadLinkDirectionWire::Out,
        true,
        Some("projected link".to_string()),
        Some(ArtifactLinkOriginWire::Manual),
        1,
        some_now(),
        operation.to_string(),
    ))
}

fn plus1(dir: &Path, id: &str, reporter: &str) -> String {
    outcome_string(add_task_plus_one(
        dir,
        id,
        reporter,
        "hit this too",
        &[],
        some_now(),
        None,
        None,
    ))
}

fn snz(dir: &Path, id: &str, plus_ones: Option<u32>) -> String {
    outcome_string(snooze_task(
        dir,
        id,
        "2026-03-01T00:00:00Z",
        plus_ones,
        "needs the upstream fix",
        AGENT,
        some_now(),
    ))
}

fn snz_cancel(dir: &Path, id: &str) -> String {
    outcome_string(cancel_task_snooze(dir, id, AGENT, some_now()))
}

pub(super) fn cases() -> Vec<Case> {
    use Backings::*;
    use SeedKind::*;
    vec![
        control_case("init_again", Empty, Both, |dir, _| {
            let root = dir.parent().unwrap();
            let name = dir.file_name().unwrap().to_str().unwrap();
            outcome_string(init_store(root, name, "sase", OWNER))
        }),
        case("create_plan", Empty, Both, |d, _| {
            mk(d, "Golden plan", IssueTypeWire::Plan, None, None, None)
        }),
        case("create_task", Empty, Both, |d, _| {
            mk(
                d,
                "Golden task",
                IssueTypeWire::Task,
                Some(PhaseSizeWire::Small),
                Some("bug"),
                None,
            )
        }),
        case("create_task_without_size", Empty, Both, |d, _| {
            mk(
                d,
                "Sizeless task",
                IssueTypeWire::Task,
                None,
                Some("bug"),
                None,
            )
        }),
        case("create_task_without_type", Empty, Both, |d, _| {
            mk(
                d,
                "Typeless task",
                IssueTypeWire::Task,
                Some(PhaseSizeWire::Small),
                None,
                None,
            )
        }),
        case("create_unknown_parent", Empty, Both, |d, _| {
            mk(
                d,
                "Orphan phase",
                IssueTypeWire::Phase,
                Some(PhaseSizeWire::Small),
                None,
                Some("sase-999"),
            )
        }),
        case("update_title", Plan, Both, |d, s| {
            upd_title(d, &s.alpha, "Renamed alpha")
        }),
        case("update_not_found", Plan, Both, |d, _| {
            upd_title(d, "sase-999", "Ghost")
        }),
        case("update_ambiguous", LegacyMixedPrefix, LegacyOnly, |d, _| {
            upd_title(d, "1", "Ghost")
        }),
        case("update_ready_rejected", Plan, Both, |d, s| {
            outcome_string(update_issue(
                d,
                &s.alpha,
                BeadUpdateFieldsWire {
                    is_ready_to_work: Some(true),
                    now: some_now(),
                    ..Default::default()
                },
            ))
        }),
        case("update_notes_rejected", Plan, Both, |d, s| {
            outcome_string(update_issue(
                d,
                &s.alpha,
                BeadUpdateFieldsWire {
                    notes: Some("direct notes".to_string()),
                    now: some_now(),
                    ..Default::default()
                },
            ))
        }),
        case("update_two", TwoPlans, Both, |d, s| {
            upd_pair(d, &s.alpha, &s.beta, "Renamed pair")
        }),
        case("update_atomic_unknown", TwoPlans, Both, |d, s| {
            upd_pair(d, &s.alpha, "sase-999", "Partial rename")
        }),
        case("note_append", TaskOpen, Both, |d, s| {
            nt_append(d, &s.task, "golden note")
        }),
        case("note_append_blank", TaskOpen, Both, |d, s| {
            nt_append(d, &s.task, "   ")
        }),
        case("note_append_not_found", TaskOpen, Both, |d, _| {
            nt_append(d, "sase-999", "ghost note")
        }),
        case("note_edit", TaskWithNote, Both, |d, s| {
            nt_edit(d, &s.task, &s.note_id, "corrected note")
        }),
        case("note_edit_unknown", TaskWithNote, Both, |d, s| {
            nt_edit(d, &s.task, "note-999", "corrected note")
        }),
        case("note_edit_blank", TaskWithNote, Both, |d, s| {
            nt_edit(d, &s.task, &s.note_id, "   ")
        }),
        case("note_remove", TaskWithNote, Both, |d, s| {
            nt_remove(d, &s.task, &s.note_id)
        }),
        case("note_remove_unknown", TaskWithNote, Both, |d, s| {
            nt_remove(d, &s.task, "note-999")
        }),
        case("open_reopen", TreeAllClosed, Both, |d, s| {
            op_open(d, &s.phase)
        }),
        case("open_not_found", Tree, Both, |d, _| op_open(d, "sase-999")),
        case("open_already_open", Tree, Both, |d, s| op_open(d, &s.task)),
        case("close_task", Tree, Both, |d, s| {
            cl_close1(d, &s.task, "done", BeadResolutionWire::Done, false)
        }),
        case("close_descendant_guard", Tree, Both, |d, s| {
            cl_close1(d, &s.epic, "done", BeadResolutionWire::Done, false)
        }),
        case("close_force_epic", Tree, Both, |d, s| {
            cl_close1(
                d,
                &s.epic,
                "superseded by a narrower epic",
                BeadResolutionWire::Superseded,
                true,
            )
        }),
        case("close_not_found", Tree, Both, |d, _| {
            cl_close(
                d,
                &["sase-999".to_string()],
                "done",
                BeadResolutionWire::Done,
                false,
            )
        }),
        case("close_with_note", Tree, Both, |d, s| {
            cl_note(d, &s.task, "closing note")
        }),
        case("close_blank_note", Tree, Both, |d, s| {
            cl_note(d, &s.task, "   ")
        }),
        remove_case("remove_epic_cascade", Tree, |d, s| rm_one(d, &s.epic)),
        case("remove_empty", Tree, Both, |d, _| rm_many(d, &[])),
        case("remove_not_found", Tree, Both, |d, _| {
            rm_many(d, &["sase-999".to_string()])
        }),
        remove_case("remove_single", Tree, |d, s| rm_one(d, &s.task)),
        case("remove_single_not_found", Tree, Both, |d, _| {
            rm_one(d, "sase-999")
        }),
        case("claim_launch", Tree, Both, |d, s| {
            cl_launch(d, &s.phase, AGENT)
        }),
        case("claim_launch_closed", TreePhaseClosed, Both, |d, s| {
            cl_launch(d, &s.phase, AGENT)
        }),
        case("claim_launch_blank_agent", Tree, Both, |d, s| {
            cl_launch(d, &s.phase, "   ")
        }),
        case("claim_launch_not_found", Tree, Both, |d, _| {
            cl_launch(d, "sase-999", AGENT)
        }),
        case("claim_wait", Tree, Both, |d, s| cl_wait(d, &s.phase, AGENT)),
        case("claim_wait_not_found", Tree, Both, |d, _| {
            cl_wait(d, "sase-999", AGENT)
        }),
        case("release", TreePhaseClaimed, Both, |d, s| {
            cl_release(d, &s.phase, AGENT)
        }),
        case("release_unclaimed", Tree, Both, |d, s| {
            cl_release(d, &s.phase, AGENT)
        }),
        case("preclaim", TreeTwoPhases, Both, |d, s| {
            preclaim_ok(d, &s.epic, &s.phase, &s.phase2)
        }),
        case("preclaim_not_epic", Tree, Both, |d, s| {
            preclaim_bad(d, &s.phase, &s.task)
        }),
        case("mark_ready", Tree, Both, |d, s| mk_ready(d, &s.epic)),
        case("mark_ready_again", TreeEpicReady, Both, |d, s| {
            mk_ready(d, &s.epic)
        }),
        case("unmark_ready", TreeEpicReady, Both, |d, s| {
            unmk_ready(d, &s.epic)
        }),
        case("unmark_not_ready", Tree, Both, |d, s| {
            unmk_ready(d, &s.epic)
        }),
        control_case("export_seeded", Tree, EventOnly, |d, _| {
            outcome_string(export_jsonl(d))
        }),
        control_case("export_legacy", Tree, LegacyOnly, |d, _| {
            outcome_string(export_jsonl(d))
        }),
        case("dep_add", TwoPlans, Both, |d, s| {
            dep_add(d, &s.alpha, &s.beta)
        }),
        case("dep_add_duplicate", PlansWithDep, Both, |d, s| {
            dep_add(d, &s.alpha, &s.beta)
        }),
        case("dep_add_not_found", TwoPlans, Both, |d, s| {
            dep_add(d, &s.alpha, "sase-999")
        }),
        case("dep_remove", PlansWithDep, Both, |d, s| {
            dep_rm(d, &s.alpha, &s.beta)
        }),
        case("dep_remove_empty", TwoPlans, Both, |d, s| {
            outcome_string(remove_dependencies(d, &s.alpha, &[], some_now()))
        }),
        case("ref_add", TwoPlans, Both, |d, s| {
            ref_add(d, &s.alpha, "agent:test-agent.1")
        }),
        case("ref_add_duplicate", PlansWithRef, Both, |d, s| {
            ref_add(d, &s.alpha, "agent:test-agent.1")
        }),
        case("ref_add_invalid", TwoPlans, Both, |d, s| {
            ref_add(d, &s.alpha, "not-a-reference")
        }),
        case("ref_remove", PlansWithRef, Both, |d, s| {
            ref_rm(d, &s.alpha, "agent:test-agent.1")
        }),
        case("ref_remove_absent", TwoPlans, Both, |d, s| {
            ref_rm(d, &s.alpha, "agent:nobody.1")
        }),
        case("link_add", TwoPlans, Both, |d, s| {
            lnk_add(d, &s.alpha, &format!("bead:{}", s.beta), "related")
        }),
        case("link_add_bad_relation", TwoPlans, Both, |d, s| {
            lnk_add(d, &s.alpha, &format!("bead:{}", s.beta), "frobnicate")
        }),
        case("link_add_unknown_target", TwoPlans, Both, |d, s| {
            lnk_add(d, &s.alpha, "bead:sase-999", "related")
        }),
        case("projection_set", TwoPlans, Both, |d, s| {
            proj(
                d,
                &s.alpha,
                &format!("bead:{}", s.beta),
                "00000000000000000000000000000002",
            )
        }),
        case("projection_replay", PlansWithProjection, Both, |d, s| {
            proj(
                d,
                &s.alpha,
                &format!("bead:{}", s.beta),
                "00000000000000000000000000000001",
            )
        }),
        case("projection_missing_op", TwoPlans, Both, |d, s| {
            proj(d, &s.alpha, &format!("bead:{}", s.beta), "")
        }),
        case("projections_batch", TwoPlans, Both, |d, s| {
            outcome_string(set_bead_link_projections(
                d,
                &[
                    BeadLinkProjectionRequestWire {
                        issue_id: s.alpha.clone(),
                        target_ref: format!("bead:{}", s.beta),
                        relation: "related".to_string(),
                        direction: BeadLinkDirectionWire::Out,
                        present: true,
                        operation_id: "0000000000000000000000000000000a"
                            .to_string(),
                        description: Some("batch link".to_string()),
                        origin: Some(ArtifactLinkOriginWire::Manual),
                        uses: 1,
                        now: some_now(),
                    },
                    BeadLinkProjectionRequestWire {
                        issue_id: s.beta.clone(),
                        target_ref: format!("bead:{}", s.alpha),
                        relation: "related".to_string(),
                        direction: BeadLinkDirectionWire::Out,
                        present: true,
                        operation_id: "0000000000000000000000000000000b"
                            .to_string(),
                        description: Some("batch link".to_string()),
                        origin: Some(ArtifactLinkOriginWire::Manual),
                        uses: 1,
                        now: some_now(),
                    },
                ],
            ))
        }),
        case("link_remove", PlansWithLink, Both, |d, s| {
            lnk_rm(d, &s.alpha, &format!("bead:{}", s.beta), "related")
        }),
        case("link_remove_bad_relation", PlansWithLink, Both, |d, s| {
            lnk_rm(d, &s.alpha, &format!("bead:{}", s.beta), "frobnicate")
        }),
        case("plus_one", TaskOpen, Both, |d, s| {
            plus1(d, &s.task, "reporter-one")
        }),
        case("plus_one_blank_reporter", TaskOpen, Both, |d, s| {
            plus1(d, &s.task, "   ")
        }),
        case("plus_one_repeat", TaskPlusOned, Both, |d, s| {
            plus1(d, &s.task, "reporter-one")
        }),
        case("plus_one_closed", TaskClosed, Both, |d, s| {
            plus1(d, &s.task, "reporter-one")
        }),
        case("snooze", TaskOpen, Both, |d, s| snz(d, &s.task, None)),
        case("snooze_zero_plus_ones", TaskOpen, Both, |d, s| {
            snz(d, &s.task, Some(0))
        }),
        case("snooze_not_found", TaskOpen, Both, |d, _| {
            snz(d, "sase-999", None)
        }),
        case("snooze_cancel", TaskSnoozed, Both, |d, s| {
            snz_cancel(d, &s.task)
        }),
        case("snooze_cancel_not_snoozed", TaskOpen, Both, |d, s| {
            snz_cancel(d, &s.task)
        }),
    ]
}
