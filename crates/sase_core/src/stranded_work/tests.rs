//! Reducer tests: one per state row, one per cause, every owner kind,
//! delegation edges, propagation shapes, the `sase-1n6` fixture, and
//! determinism under shuffled input.

use super::classify::reduce_stranded_work;
use super::wire::{
    StrandedWorkBeadFactsWire, StrandedWorkInputWire,
    StrandedWorkMemberFactsWire, StrandedWorkReportWire,
    StrandedWorkVerdictWire, STRANDED_WORK_WIRE_SCHEMA_VERSION,
};
use crate::bead::wire::BeadCompletionFailureWire;

const NOW: f64 = 1_800_000_000.0;
const OLD: f64 = NOW - 3_600.0;

fn agent(name: &str) -> StrandedWorkMemberFactsWire {
    StrandedWorkMemberFactsWire {
        name: name.to_string(),
        artifact_dir: format!("/artifacts/{name}"),
        turn_kind: "agent".to_string(),
        lifecycle: "done".to_string(),
        ended_at: Some(OLD),
        ..StrandedWorkMemberFactsWire::default()
    }
}

fn live_agent(name: &str, lifecycle: &str) -> StrandedWorkMemberFactsWire {
    StrandedWorkMemberFactsWire {
        name: name.to_string(),
        artifact_dir: format!("/artifacts/{name}"),
        turn_kind: "agent".to_string(),
        lifecycle: lifecycle.to_string(),
        process_live: true,
        started_at: Some(OLD),
        ..StrandedWorkMemberFactsWire::default()
    }
}

fn monitor(name: &str) -> StrandedWorkMemberFactsWire {
    StrandedWorkMemberFactsWire {
        name: name.to_string(),
        artifact_dir: format!("/artifacts/{name}"),
        turn_kind: "monitor".to_string(),
        lifecycle: "done".to_string(),
        ended_at: Some(OLD),
        ..StrandedWorkMemberFactsWire::default()
    }
}

fn bead(id: &str, kind: &str) -> StrandedWorkBeadFactsWire {
    StrandedWorkBeadFactsWire {
        id: id.to_string(),
        kind: kind.to_string(),
        status: "in_progress".to_string(),
        candidate: true,
        assignee: Some(id.to_string()),
        ..StrandedWorkBeadFactsWire::default()
    }
}

fn context_bead(
    id: &str,
    kind: &str,
    status: &str,
    parent: &str,
) -> StrandedWorkBeadFactsWire {
    StrandedWorkBeadFactsWire {
        id: id.to_string(),
        kind: kind.to_string(),
        status: status.to_string(),
        candidate: false,
        parent_id: Some(parent.to_string()),
        ..StrandedWorkBeadFactsWire::default()
    }
}

fn input(beads: Vec<StrandedWorkBeadFactsWire>) -> StrandedWorkInputWire {
    StrandedWorkInputWire {
        now: NOW,
        beads,
        ..StrandedWorkInputWire::default()
    }
}

fn recovered_failure(failed_at: &str) -> BeadCompletionFailureWire {
    BeadCompletionFailureWire {
        close_event_id: "beads-1:close-event".to_string(),
        run_id: "origin-run".to_string(),
        agent_name: "origin-agent".to_string(),
        artifact_dir: "/artifacts/origin-run".to_string(),
        failed_at: failed_at.to_string(),
        evidence_locations: vec![
            "/artifacts/origin-run/failure.json".to_string()
        ],
        failure_code: "hook_failed".to_string(),
    }
}

fn unix_seconds(timestamp: &str) -> f64 {
    chrono::DateTime::parse_from_rfc3339(timestamp)
        .unwrap()
        .timestamp() as f64
}

fn reduce(
    bead: StrandedWorkBeadFactsWire,
    context: Vec<StrandedWorkBeadFactsWire>,
) -> StrandedWorkVerdictWire {
    let bead_id = bead.id.clone();
    let mut beads = vec![bead];
    beads.extend(context);
    let report = reduce_stranded_work(&input(beads));
    report
        .verdicts
        .into_iter()
        .find(|verdict| verdict.bead_id == bead_id)
        .expect("candidate bead must have a verdict")
}

fn kept_phase(id: &str) -> StrandedWorkBeadFactsWire {
    let mut member = agent(&format!("{id}--3"));
    member.declared_bead_action = Some("keep".to_string());
    StrandedWorkBeadFactsWire {
        members: vec![member],
        ..bead(id, "phase")
    }
}

#[test]
fn open_recovery_facts_survive_missing_records_and_use_failed_liveness_cause() {
    let failed_at = "2027-01-15T08:00:00Z";
    let mut recovered = StrandedWorkBeadFactsWire {
        id: "recovered-phase".to_string(),
        kind: "phase".to_string(),
        status: "open".to_string(),
        candidate: false,
        records_available: false,
        completion_failure: Some(recovered_failure(failed_at)),
        ..StrandedWorkBeadFactsWire::default()
    };
    let now = unix_seconds(failed_at) + 3_600.0;
    let report = reduce_stranded_work(&StrandedWorkInputWire {
        now,
        settle_grace_seconds: 120.0,
        beads: vec![recovered.clone()],
        ..StrandedWorkInputWire::default()
    });
    let verdict = &report.verdicts[0];
    assert_eq!(verdict.state, "stranded");
    assert_eq!(verdict.reason.as_deref(), Some("failed"));
    assert_eq!(verdict.last_turn.as_deref(), Some("origin-agent"));
    assert_eq!(
        verdict.last_turn_artifact_dir.as_deref(),
        Some("/artifacts/origin-run")
    );
    assert_eq!(
        verdict.completion_failure.as_ref().unwrap().run_id,
        "origin-run"
    );
    assert_eq!(report.stranded_ids, vec!["recovered-phase"]);

    recovered.completion_failure = Some(recovered_failure(failed_at));
    let live = StrandedWorkMemberFactsWire {
        name: "successor.agent".to_string(),
        artifact_dir: "/artifacts/successor".to_string(),
        lifecycle: "running".to_string(),
        process_live: true,
        ..StrandedWorkMemberFactsWire::default()
    };
    recovered.members = vec![live];
    let live_report = reduce_stranded_work(&StrandedWorkInputWire {
        now,
        settle_grace_seconds: 120.0,
        beads: vec![recovered],
        ..StrandedWorkInputWire::default()
    });
    assert_eq!(live_report.verdicts[0].state, "owned");
    assert_eq!(
        live_report.verdicts[0].owner.as_deref(),
        Some("successor.agent")
    );
}

#[test]
fn recovery_settle_grace_applies_and_ordinary_open_drafts_stay_excluded() {
    let failed_at = "2027-01-15T08:00:00Z";
    let recovered = StrandedWorkBeadFactsWire {
        id: "recovering".to_string(),
        kind: "phase".to_string(),
        status: "open".to_string(),
        completion_failure: Some(recovered_failure(failed_at)),
        ..StrandedWorkBeadFactsWire::default()
    };
    let draft = StrandedWorkBeadFactsWire {
        id: "ordinary-draft".to_string(),
        kind: "phase".to_string(),
        status: "open".to_string(),
        ..StrandedWorkBeadFactsWire::default()
    };
    let report = reduce_stranded_work(&StrandedWorkInputWire {
        now: unix_seconds(failed_at) + 30.0,
        settle_grace_seconds: 120.0,
        beads: vec![recovered, draft],
        ..StrandedWorkInputWire::default()
    });
    assert_eq!(report.verdicts.len(), 1);
    assert_eq!(report.verdicts[0].bead_id, "recovering");
    assert_eq!(report.verdicts[0].state, "settling");
}

#[test]
fn owned_lifecycle_kinds() {
    for (lifecycle, reason) in [
        ("running", "running"),
        ("asking", "asking"),
        ("waiting", "waiting"),
        ("queued", "queued"),
        ("starting", "running"),
        ("unknown", "running"),
    ] {
        let verdict = reduce(
            StrandedWorkBeadFactsWire {
                members: vec![live_agent("w--1", lifecycle)],
                ..bead("w", "phase")
            },
            Vec::new(),
        );
        assert_eq!(verdict.state, "owned", "lifecycle {lifecycle}");
        assert_eq!(verdict.reason.as_deref(), Some(reason));
        assert_eq!(verdict.owner.as_deref(), Some("w--1"));
    }
}

#[test]
fn owned_remote_non_terminal_is_never_lost() {
    let member = StrandedWorkMemberFactsWire {
        remote: true,
        process_live: false,
        ..live_agent("w--1", "running")
    };
    let verdict = reduce(
        StrandedWorkBeadFactsWire {
            members: vec![member],
            ..bead("w", "phase")
        },
        Vec::new(),
    );
    assert_eq!(verdict.state, "owned");
    assert_eq!(verdict.reason.as_deref(), Some("running"));
}

#[test]
fn owned_finalizing_beats_done_lifecycle() {
    let member = StrandedWorkMemberFactsWire {
        finalizing: true,
        ..agent("w--1")
    };
    let verdict = reduce(
        StrandedWorkBeadFactsWire {
            members: vec![member],
            ..bead("w", "phase")
        },
        Vec::new(),
    );
    assert_eq!(verdict.state, "owned");
    assert_eq!(verdict.reason.as_deref(), Some("finalizing"));
}

#[test]
fn owned_each_pending_handoff_kind() {
    for kind in ["plan", "questions", "monitor", "gate", "pipe"] {
        let member = StrandedWorkMemberFactsWire {
            pending_handoff: Some(kind.to_string()),
            ..agent("w--1")
        };
        let verdict = reduce(
            StrandedWorkBeadFactsWire {
                members: vec![member],
                ..bead("w", "phase")
            },
            Vec::new(),
        );
        assert_eq!(verdict.state, "owned", "handoff kind {kind}");
        assert_eq!(verdict.reason.as_deref(), Some("handoff"));
    }
    let inflight = StrandedWorkMemberFactsWire {
        handoff_inflight: true,
        ..agent("w--1")
    };
    let verdict = reduce(
        StrandedWorkBeadFactsWire {
            members: vec![inflight],
            ..bead("w", "phase")
        },
        Vec::new(),
    );
    assert_eq!(verdict.state, "owned");
    assert_eq!(verdict.reason.as_deref(), Some("handoff"));
}

#[test]
fn owned_recovery_in_flight_beats_failed_lifecycle() {
    let member = StrandedWorkMemberFactsWire {
        lifecycle: "failed".to_string(),
        recovery_in_flight: true,
        ..agent("w--1")
    };
    let verdict = reduce(
        StrandedWorkBeadFactsWire {
            members: vec![member],
            ..bead("w", "phase")
        },
        Vec::new(),
    );
    assert_eq!(verdict.state, "owned");
    assert_eq!(verdict.reason.as_deref(), Some("recovery"));
}

#[test]
fn owned_monitor_and_gate_turn_states() {
    for (turn_kind, turn_state, reason) in [
        ("monitor", "running", "monitor"),
        ("monitor", "pending", "monitor"),
        ("monitor", "settling", "monitor"),
        ("gate", "pending", "gate"),
        ("gate", "running", "gate"),
    ] {
        let member = StrandedWorkMemberFactsWire {
            turn_kind: turn_kind.to_string(),
            turn_state: Some(turn_state.to_string()),
            ..monitor("w--mon")
        };
        let verdict = reduce(
            StrandedWorkBeadFactsWire {
                members: vec![member],
                ..bead("w", "phase")
            },
            Vec::new(),
        );
        assert_eq!(
            verdict.state, "owned",
            "{turn_kind} turn_state {turn_state}"
        );
        assert_eq!(verdict.reason.as_deref(), Some(reason));
    }
}

#[test]
fn owned_monitor_followup_pending() {
    let member = StrandedWorkMemberFactsWire {
        followup_pending: true,
        ..monitor("w--mon")
    };
    let verdict = reduce(
        StrandedWorkBeadFactsWire {
            members: vec![member],
            ..bead("w", "phase")
        },
        Vec::new(),
    );
    assert_eq!(verdict.state, "owned");
    assert_eq!(verdict.reason.as_deref(), Some("monitor"));
    assert_eq!(verdict.owner.as_deref(), Some("w--mon"));
}

#[test]
fn owned_followup_launched_with_running_successor() {
    let launched = StrandedWorkMemberFactsWire {
        followup_outcome: Some("launched".to_string()),
        followup_agent: Some("w--2".to_string()),
        ..monitor("w--mon")
    };
    let successor = live_agent("w--2", "running");
    let verdict = reduce(
        StrandedWorkBeadFactsWire {
            members: vec![launched, successor],
            ..bead("w", "phase")
        },
        Vec::new(),
    );
    assert_eq!(verdict.state, "owned");
    assert_eq!(verdict.owner.as_deref(), Some("w--2"));
}

#[test]
fn owner_is_newest_qualifying_member() {
    let old = live_agent("w--1", "running");
    let mut new = live_agent("w--2", "waiting");
    new.started_at = Some(OLD + 100.0);
    let verdict = reduce(
        StrandedWorkBeadFactsWire {
            members: vec![old, new],
            ..bead("w", "phase")
        },
        Vec::new(),
    );
    assert_eq!(verdict.state, "owned");
    assert_eq!(verdict.owner.as_deref(), Some("w--2"));
    assert_eq!(verdict.reason.as_deref(), Some("waiting"));
}

#[test]
fn unknown_when_no_members() {
    let mut found = bead("w", "phase");
    found.records_available = true;
    let found = reduce(found, Vec::new());
    assert_eq!(found.state, "unknown");
    assert_eq!(found.reason.as_deref(), Some("no_records"));

    let mut missing = bead("w", "phase");
    missing.records_available = false;
    let missing = reduce(missing, Vec::new());
    assert_eq!(missing.state, "unknown");
    assert_eq!(missing.reason.as_deref(), Some("records_unavailable"));
}

#[test]
fn settling_within_grace_and_exact_boundary() {
    let mut recent = agent("w--1");
    recent.ended_at = Some(NOW - 10.0);
    let verdict = reduce(
        StrandedWorkBeadFactsWire {
            members: vec![recent],
            ..bead("w", "phase")
        },
        Vec::new(),
    );
    assert_eq!(verdict.state, "settling");
    assert_eq!(verdict.reason, None);
    assert_eq!(verdict.since, Some(NOW - 10.0));

    let mut boundary = agent("w--1");
    boundary.ended_at = Some(NOW - 120.0);
    let verdict = reduce(
        StrandedWorkBeadFactsWire {
            members: vec![boundary],
            ..bead("w", "phase")
        },
        Vec::new(),
    );
    assert_eq!(verdict.state, "stranded");
    assert_eq!(verdict.reason.as_deref(), Some("unclosed"));
}

#[test]
fn delegation_needs_launched_child_epic() {
    for status in ["in_progress", "claimed"] {
        let verdict = reduce(
            StrandedWorkBeadFactsWire {
                members: vec![agent("w--1")],
                ..bead("w", "phase")
            },
            vec![context_bead("w.e1", "epic", status, "w")],
        );
        assert_eq!(verdict.state, "delegated", "child status {status}");
        assert_eq!(verdict.delegates, vec!["w.e1".to_string()]);
        assert_eq!(verdict.reason, None);
    }

    for (kind, status) in [
        ("epic", "open"),
        ("epic", "closed"),
        ("task", "in_progress"),
        ("phase", "in_progress"),
    ] {
        let mut member = agent("w--1");
        member.declared_bead_action = Some("keep".to_string());
        let verdict = reduce(
            StrandedWorkBeadFactsWire {
                members: vec![member],
                ..bead("w", "phase")
            },
            vec![context_bead("w.c", kind, status, "w")],
        );
        assert_eq!(
            verdict.state, "stranded",
            "child {kind}/{status} must not delegate"
        );
        assert!(verdict.delegates.is_empty());
    }
}

#[test]
fn cause_followup_failed_variants() {
    for outcome in ["error", "launched-degraded", "lost", "stopped"] {
        let mut newest = monitor("w--mon");
        newest.ended_at = Some(OLD + 200.0);
        newest.followup_outcome = Some(outcome.to_string());
        let verdict = reduce(
            StrandedWorkBeadFactsWire {
                members: vec![agent("w--1"), newest],
                ..bead("w", "phase")
            },
            Vec::new(),
        );
        assert_eq!(verdict.state, "stranded", "outcome {outcome}");
        assert_eq!(verdict.reason.as_deref(), Some("followup_failed"));
        assert_eq!(verdict.last_turn.as_deref(), Some("w--1"));
    }
}

#[test]
fn cause_failed() {
    let mut member = agent("w--1");
    member.lifecycle = "failed".to_string();
    let verdict = reduce(
        StrandedWorkBeadFactsWire {
            members: vec![member],
            ..bead("w", "phase")
        },
        Vec::new(),
    );
    assert_eq!(verdict.state, "stranded");
    assert_eq!(verdict.reason.as_deref(), Some("failed"));
}

#[test]
fn cause_stopped() {
    let mut member = agent("w--1");
    member.lifecycle = "stopped".to_string();
    let verdict = reduce(
        StrandedWorkBeadFactsWire {
            members: vec![member],
            ..bead("w", "phase")
        },
        Vec::new(),
    );
    assert_eq!(verdict.state, "stranded");
    assert_eq!(verdict.reason.as_deref(), Some("stopped"));
}

#[test]
fn cause_lost_for_dead_local_non_terminal() {
    let mut member = agent("w--1");
    member.lifecycle = "running".to_string();
    member.ended_at = None;
    member.started_at = Some(OLD);
    let verdict = reduce(
        StrandedWorkBeadFactsWire {
            members: vec![member],
            ..bead("w", "phase")
        },
        Vec::new(),
    );
    assert_eq!(verdict.state, "stranded");
    assert_eq!(verdict.reason.as_deref(), Some("lost"));
}

#[test]
fn cause_child_landed_and_newer_agent_turn_wins() {
    let mut landed = context_bead("w.e1", "epic", "closed", "w");
    landed.closed_at = Some(OLD + 100.0);
    let verdict = reduce(kept_phase("w"), vec![landed]);
    assert_eq!(verdict.state, "stranded");
    assert_eq!(verdict.reason.as_deref(), Some("child_landed"));

    let mut landed = context_bead("w.e1", "epic", "closed", "w");
    landed.closed_at = Some(OLD - 100.0);
    let verdict = reduce(kept_phase("w"), vec![landed]);
    assert_eq!(verdict.state, "stranded");
    assert_eq!(verdict.reason.as_deref(), Some("kept"));
}

#[test]
fn cause_close_failed_kept_unclosed() {
    let mut close = agent("w--1");
    close.declared_bead_action = Some("close".to_string());
    let verdict = reduce(
        StrandedWorkBeadFactsWire {
            members: vec![close],
            ..bead("w", "phase")
        },
        Vec::new(),
    );
    assert_eq!(verdict.reason.as_deref(), Some("close_failed"));

    let verdict = reduce(kept_phase("w"), Vec::new());
    assert_eq!(verdict.state, "stranded");
    assert_eq!(verdict.reason.as_deref(), Some("kept"));
    assert_eq!(verdict.last_turn.as_deref(), Some("w--3"));
    assert_eq!(
        verdict.last_turn_artifact_dir.as_deref(),
        Some("/artifacts/w--3")
    );
    assert_eq!(verdict.declared_bead_action.as_deref(), Some("keep"));
    assert_eq!(verdict.session.as_deref(), Some("w"));

    let verdict = reduce(
        StrandedWorkBeadFactsWire {
            members: vec![agent("w--1")],
            ..bead("w", "phase")
        },
        Vec::new(),
    );
    assert_eq!(verdict.reason.as_deref(), Some("unclosed"));
}

#[test]
fn non_candidate_context_has_no_verdict() {
    let report = reduce_stranded_work(&input(vec![
        kept_phase("w"),
        context_bead("w.e1", "epic", "closed", "w"),
    ]));
    assert_eq!(report.verdicts.len(), 1);
    assert_eq!(report.verdicts[0].bead_id, "w");
    assert_eq!(report.schema_version, STRANDED_WORK_WIRE_SCHEMA_VERSION);
}

fn live_land(id: &str) -> StrandedWorkBeadFactsWire {
    StrandedWorkBeadFactsWire {
        members: vec![live_agent(&format!("{id}--land"), "waiting")],
        ..bead(id, "epic")
    }
}

fn verdict_by<'a>(
    report: &'a StrandedWorkReportWire,
    id: &str,
) -> &'a StrandedWorkVerdictWire {
    report
        .verdicts
        .iter()
        .find(|verdict| verdict.bead_id == id)
        .expect("verdict must exist")
}

#[test]
fn stranded_epic_blocks_parent_phase() {
    let mut epic = kept_phase("w.e1");
    epic.kind = "epic".to_string();
    let mut phase = live_land("w");
    phase.kind = "phase".to_string();
    phase.parent_id = None;
    epic.parent_id = Some("w".to_string());
    let report = reduce_stranded_work(&input(vec![epic, phase]));

    let leaf = verdict_by(&report, "w.e1");
    assert_eq!(leaf.state, "stranded");
    assert_eq!(leaf.blocks, vec!["w".to_string()]);

    let parent = verdict_by(&report, "w");
    assert_eq!(parent.state, "blocked");
    assert_eq!(parent.blocked_by, vec!["w.e1".to_string()]);
    assert!(parent.blocks.is_empty());
    assert_eq!(report.stranded_ids, vec!["w.e1".to_string()]);
    assert_eq!(report.blocked_ids, vec!["w".to_string()]);
}

#[test]
fn propagation_walks_through_stranded_ancestors() {
    let mut leaf = kept_phase("l");
    leaf.parent_id = Some("m".to_string());
    let mut middle = kept_phase("m");
    middle.kind = "epic".to_string();
    middle.parent_id = Some("r".to_string());
    let mut root = live_land("r");
    root.parent_id = None;
    let report = reduce_stranded_work(&input(vec![leaf, middle, root]));

    let middle_verdict = verdict_by(&report, "m");
    assert_eq!(middle_verdict.state, "stranded");
    assert_eq!(middle_verdict.blocks, vec!["r".to_string()]);
    assert_eq!(middle_verdict.blocked_by, vec!["l".to_string()]);

    let leaf_verdict = verdict_by(&report, "l");
    assert_eq!(leaf_verdict.state, "stranded");
    assert_eq!(leaf_verdict.blocks, vec!["m".to_string(), "r".to_string()]);

    let root_verdict = verdict_by(&report, "r");
    assert_eq!(root_verdict.state, "blocked");
    assert_eq!(
        root_verdict.blocked_by,
        vec!["l".to_string(), "m".to_string()]
    );
    assert_eq!(report.stranded_ids, vec!["l".to_string(), "m".to_string()]);
    assert_eq!(report.blocked_ids, vec!["r".to_string()]);
}

fn sase_1n6_beads() -> Vec<StrandedWorkBeadFactsWire> {
    let t1 = OLD;
    let t2 = OLD + 100.0;
    let t3 = OLD + 200.0;
    let t4 = OLD + 300.0;
    let t5 = OLD + 400.0;
    let mut first = agent("leaf--1");
    first.ended_at = Some(t1);
    let mut mon = monitor("leaf--mon");
    mon.ended_at = Some(t2);
    mon.followup_outcome = Some("launched".to_string());
    mon.followup_agent = Some("leaf--2".to_string());
    let mut second = agent("leaf--2");
    second.ended_at = Some(t3);
    let mut mon0 = monitor("leaf--mon-0");
    mon0.ended_at = Some(t4);
    mon0.followup_outcome = Some("launched".to_string());
    mon0.followup_agent = Some("leaf--3".to_string());
    let mut third = agent("leaf--3");
    third.ended_at = Some(t5);
    third.declared_bead_action = Some("keep".to_string());
    let leaf = StrandedWorkBeadFactsWire {
        id: "sase-1n6.5.1.5.2.1.2".to_string(),
        parent_id: Some("sase-1n6.5.1.5.2.1".to_string()),
        members: vec![first, mon, second, mon0, third],
        ..bead("sase-1n6.5.1.5.2.1.2", "phase")
    };
    let mut mid = live_land("sase-1n6.5.1.5.2.1");
    mid.parent_id = Some("sase-1n6.5.1.5".to_string());
    let mut delegated = StrandedWorkBeadFactsWire {
        members: vec![agent("sase-1n6.5.1.5--land")],
        ..bead("sase-1n6.5.1.5", "phase")
    };
    delegated.parent_id = Some("sase-1n6.5.1".to_string());
    let mut top = live_land("sase-1n6.5.1");
    top.parent_id = Some("sase-1n6".to_string());
    let root = live_land("sase-1n6");
    vec![leaf, mid, delegated, top, root]
}

#[test]
fn sase_1n6_shape() {
    let report = reduce_stranded_work(&input(sase_1n6_beads()));
    assert_eq!(
        report.stranded_ids,
        vec!["sase-1n6.5.1.5.2.1.2".to_string()]
    );

    let leaf = verdict_by(&report, "sase-1n6.5.1.5.2.1.2");
    assert_eq!(leaf.state, "stranded");
    assert_eq!(leaf.reason.as_deref(), Some("kept"));
    assert_eq!(leaf.last_turn.as_deref(), Some("leaf--3"));
    assert_eq!(
        leaf.blocks,
        vec![
            "sase-1n6.5.1.5.2.1".to_string(),
            "sase-1n6.5.1.5".to_string(),
            "sase-1n6.5.1".to_string(),
            "sase-1n6".to_string(),
        ]
    );

    let mid = verdict_by(&report, "sase-1n6.5.1.5.2.1");
    assert_eq!(mid.state, "blocked");
    assert_eq!(mid.blocked_by, vec!["sase-1n6.5.1.5.2.1.2".to_string()]);

    let delegated = verdict_by(&report, "sase-1n6.5.1.5");
    assert_eq!(delegated.state, "blocked");
    assert_eq!(delegated.delegates, vec!["sase-1n6.5.1.5.2.1".to_string()]);

    assert_eq!(
        report.blocked_ids,
        vec![
            "sase-1n6".to_string(),
            "sase-1n6.5.1".to_string(),
            "sase-1n6.5.1.5".to_string(),
            "sase-1n6.5.1.5.2.1".to_string(),
        ]
    );
    let ids: Vec<&str> =
        report.verdicts.iter().map(|v| v.bead_id.as_str()).collect();
    let mut sorted = ids.clone();
    sorted.sort();
    assert_eq!(ids, sorted);
}

#[test]
fn shuffled_input_reduces_identically() {
    let mut beads = sase_1n6_beads();
    beads.reverse();
    for bead in &mut beads {
        bead.members.reverse();
    }
    let baseline = reduce_stranded_work(&input(sase_1n6_beads()));
    let shuffled = reduce_stranded_work(&input(beads));
    assert_eq!(baseline, shuffled);
}

#[test]
fn wire_defaults_and_unknown_fields() {
    let value = serde_json::json!({
        "now": NOW,
        "beads": [],
    });
    let parsed: StrandedWorkInputWire = serde_json::from_value(value).unwrap();
    assert_eq!(parsed.schema_version, STRANDED_WORK_WIRE_SCHEMA_VERSION);
    assert!((parsed.settle_grace_seconds - 120.0).abs() < f64::EPSILON);

    let bad = serde_json::json!({
        "now": NOW,
        "beads": [],
        "bogus": true,
    });
    assert!(serde_json::from_value::<StrandedWorkInputWire>(bad).is_err());
}
