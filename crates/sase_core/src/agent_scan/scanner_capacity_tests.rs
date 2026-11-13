use std::collections::BTreeSet;
use std::fs;

use serde_json::{json, Value};
use tempfile::tempdir;

use super::tests::{capacity_artifact_dir, write_capacity_meta, write_json};
use super::*;

const DEAD_PID: i64 = 1_700_000_000;

fn capacity_options() -> AgentArtifactScanOptionsWire {
    AgentArtifactScanOptionsWire {
        capacity_only: true,
        ..AgentArtifactScanOptionsWire::default()
    }
}

fn assert_scan_data_equal(
    optimized: &AgentArtifactScanWire,
    legacy: &AgentArtifactScanWire,
) {
    assert_eq!(
        serde_json::to_value(&optimized.records).unwrap(),
        serde_json::to_value(&legacy.records).unwrap(),
        "retained record JSON differs"
    );
    assert_eq!(
        serde_json::to_value(&optimized.clan_context).unwrap(),
        serde_json::to_value(&legacy.clan_context).unwrap(),
        "pre-retention clan context JSON differs"
    );
    assert_eq!(
        serde_json::to_value(super::tests::rust_capacity_snapshot(
            &optimized.records,
            None,
        ))
        .unwrap(),
        serde_json::to_value(super::tests::rust_capacity_snapshot(
            &legacy.records,
            None,
        ))
        .unwrap(),
        "runner-capacity decisions differ"
    );
}

#[test]
fn compact_meta_projection_preserves_capacity_coercions_and_aliases() {
    let payload = json!({
        "agent_family_parallel": "yes",
        "agent_family": "legacy-clan",
        "agent_clan_generation": "g1",
        "clan_tribe": "night shift",
        "clan_summary": "summary",
        "parent_timestamp": "20260427190000",
        "runner_claim_owner_key": " owner ",
        "pid": "2147483647",
        "stopped_at": "2026-04-27T19:00:00Z",
        "unrelated_payload": {
            "large_list": (0..512).collect::<Vec<_>>(),
            "nested": {"ignored": [true, false, null]}
        }
    });
    let bytes = serde_json::to_vec(&payload).unwrap();
    let projected = parse_capacity_meta(&bytes).unwrap();
    let full_map = serde_json::from_slice::<Value>(&bytes)
        .unwrap()
        .as_object()
        .unwrap()
        .clone();
    let full = agent_meta_from_object(&full_map);

    for (field, projected_value, full_value) in [
        ("agent_clan", projected.agent_clan.clone(), full.agent_clan),
        (
            "agent_clan_generation",
            projected.agent_clan_generation,
            full.agent_clan_generation,
        ),
        ("clan_tribe", projected.clan_tribe, full.clan_tribe),
        ("clan_summary", projected.clan_summary, full.clan_summary),
        (
            "parent_timestamp",
            projected.parent_timestamp,
            full.parent_timestamp,
        ),
        (
            "runner_claim_owner_key",
            projected.runner_claim_owner_key,
            full.runner_claim_owner_key,
        ),
        ("stopped_at", projected.stopped_at, full.stopped_at),
    ] {
        assert_eq!(projected_value, full_value, "field {field}");
    }
    assert_eq!(projected.pid, full.pid);
    assert!(projected.agent_session_parallel);
    assert_eq!(projected.agent_session, None);
    assert_eq!(projected.agent_clan.as_deref(), Some("legacy-clan"));
    assert!(parse_capacity_meta(b"[]").is_err());
    assert!(parse_capacity_meta(b"{\"agent_clan\": \"x\"} trailing").is_err());
}

#[test]
fn both_capacity_scan_entry_points_match_the_full_parse_legacy_oracle() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let root_timestamp = "20260427190000";
    let live_timestamp = "20260427190001";

    // A legacy-family alias, a cycle, and shared owner identity exercise the
    // transitive closure while retaining pre-prune clan declarations.
    let root = write_capacity_meta(
        &projects,
        root_timestamp,
        json!({
            "name": "stopped-root",
            "stopped_at": "2026-04-27T19:00:00Z",
            "parent_timestamp": live_timestamp,
            "agent_family_parallel": true,
            "agent_family": "research",
            "agent_clan_generation": "g1",
            "clan_tribe": "night shift",
            "clan_summary": "legacy family summary",
            "runner_claim_owner_key": " owner-a "
        }),
    );
    let live = write_capacity_meta(
        &projects,
        live_timestamp,
        json!({
            "name": "live-child",
            "pid": std::process::id(),
            "parent_timestamp": root_timestamp,
            "agent_clan": "research",
            "agent_clan_generation": "g1",
            "runner_claim_owner_key": "owner-a"
        }),
    );
    let chain_root_timestamp = "20260427190015";
    let chain_middle_timestamp = "20260427190016";
    write_capacity_meta(
        &projects,
        chain_root_timestamp,
        json!({"name": "chain-root", "stopped_at": "stopped"}),
    );
    write_capacity_meta(
        &projects,
        chain_middle_timestamp,
        json!({
            "name": "chain-middle",
            "pid": DEAD_PID,
            "parent_timestamp": chain_root_timestamp
        }),
    );
    write_capacity_meta(
        &projects,
        "20260427190017",
        json!({
            "name": "chain-child",
            "pid": std::process::id(),
            "parent_timestamp": chain_middle_timestamp
        }),
    );
    let fallback_source = write_capacity_meta(
        &projects,
        "20260427190018",
        json!({
            "name": "fallback-tribe-source",
            "stopped_at": "stopped",
            "parent_timestamp": "fallback-generation",
            "agent_clan": "fallback-clan",
            "clan_tribe": "fallback tribe"
        }),
    );
    write_capacity_meta(
        &projects,
        "20260427190019",
        json!({
            "name": "fallback-clan-live",
            "pid": std::process::id(),
            "parent_timestamp": "fallback-generation",
            "agent_clan": "fallback-clan"
        }),
    );

    // Same timestamp across workflows must retain the old deterministic
    // clan declaration winner. A separate generation has summary only.
    let duplicate_workflow = projects
        .join("proj")
        .join("artifacts")
        .join("run")
        .join(root_timestamp);
    write_json(
        &duplicate_workflow.join("agent_meta.json"),
        json!({
            "name": "parallel-workflow",
            "agent_clan": "research",
            "agent_clan_generation": "g1",
            "clan_tribe": "workflow winner"
        }),
    );
    let second_generation = write_capacity_meta(
        &projects,
        "20260427190002",
        json!({
            "name": "summary-only",
            "agent_clan": "research",
            "agent_clan_generation": "g2",
            "clan_summary": "generation two"
        }),
    );

    // Day-sharded layout, whitespace-only owner key, and a markerless dead
    // PID cover the compact path's layout and coercion decisions.
    let sharded = projects
        .join("proj")
        .join("artifacts")
        .join("ace-run")
        .join("202604")
        .join("27")
        .join("20260427190003");
    write_json(
        &sharded.join("agent_meta.json"),
        json!({
            "name": "dead-markerless",
            "pid": DEAD_PID,
            "runner_claim_owner_key": "   "
        }),
    );
    let duplicate_layout_timestamp = "20260427190014";
    let duplicate_legacy =
        capacity_artifact_dir(&projects, duplicate_layout_timestamp);
    write_json(
        &duplicate_legacy.join("agent_meta.json"),
        json!({"name": "legacy-layout"}),
    );
    let duplicate_sharded = projects
        .join("proj")
        .join("artifacts")
        .join("ace-run")
        .join("202604")
        .join("27")
        .join(duplicate_layout_timestamp);
    write_json(
        &duplicate_sharded.join("agent_meta.json"),
        json!({"name": "day-sharded-layout"}),
    );

    // Marker uncertainty uses the full parser. Pending questions keep stopped
    // records, but a dead PID still lets a running-marker row be pruned.
    let pending = write_capacity_meta(
        &projects,
        "20260427190004",
        json!({"name": "pending", "stopped_at": "stopped", "pid": DEAD_PID}),
    );
    write_json(&pending.join("pending_question.json"), json!({"id": "q"}));
    let waiting = write_capacity_meta(
        &projects,
        "20260427190005",
        json!({"name": "stopped-waiting", "stopped_at": "stopped"}),
    );
    write_json(
        &waiting.join("waiting.json"),
        json!({"slot_requested_at": "t"}),
    );
    let dead_running = write_capacity_meta(
        &projects,
        "20260427190006",
        json!({"name": "dead-running", "pid": DEAD_PID}),
    );
    write_json(&dead_running.join("running.json"), json!({"pid": DEAD_PID}));
    let malformed_marker = write_capacity_meta(
        &projects,
        "20260427190007",
        json!({"name": "malformed-marker", "stopped_at": "stopped"}),
    );
    fs::write(malformed_marker.join("running.json"), b"{broken").unwrap();

    // Missing/malformed metadata stays conservative. Its timestamp can
    // still act as a closure target, and it cannot erase clan inputs.
    let malformed_meta = capacity_artifact_dir(&projects, "20260427190008");
    fs::create_dir_all(&malformed_meta).unwrap();
    fs::write(malformed_meta.join("agent_meta.json"), b"not json").unwrap();
    let stopped_orphan = write_capacity_meta(
        &projects,
        "20260427190009",
        json!({
            "name": "pruned-clan-member",
            "stopped_at": "stopped",
            "agent_family_parallel": true,
            "agent_family": "orphan-clan",
            "clan_summary": "entirely pruned clan"
        }),
    );
    write_json(
        &stopped_orphan.join("workflow_state.json"),
        json!({"appears_as_agent": false}),
    );
    let done = write_capacity_meta(
        &projects,
        "20260427190010",
        json!({"name": "done"}),
    );
    write_json(&done.join("done.json"), json!({}));

    // A matching timestamp and owner key in another project must not enter
    // the root project's retention closure.
    let other_project = projects
        .join("other")
        .join("artifacts")
        .join("ace-run")
        .join(root_timestamp);
    write_json(
        &other_project.join("agent_meta.json"),
        json!({
            "stopped_at": "stopped",
            "runner_claim_owner_key": "owner-a"
        }),
    );

    let clan_records = tmp.path().join("clan-records");
    crate::agent_clan_record::record_clan_attributes(
        &clan_records,
        crate::agent_clan_record::ClanRecordUpdateWire {
            clan: "research".to_string(),
            generation: "g1".to_string(),
            tribe: Some(crate::agent_clan_record::ClanAttributeUpdateWire {
                value: Some("durable tribe".to_string()),
                source:
                    crate::agent_clan_record::ClanAttributeSourceWire::Declared,
                source_identity: Some("durable-record".to_string()),
            }),
            summary: Some(crate::agent_clan_record::ClanAttributeUpdateWire {
                value: Some("durable summary".to_string()),
                source:
                    crate::agent_clan_record::ClanAttributeSourceWire::Script,
                source_identity: Some("durable-record".to_string()),
            }),
            ..Default::default()
        },
    )
    .unwrap();
    let mut options = capacity_options();
    options.clan_records_dir =
        Some(clan_records.to_string_lossy().into_owned());
    let process_missing = |pid| pid == DEAD_PID as i32;
    let optimized = scan_agent_artifacts_with_process_observation(
        &projects,
        options.clone(),
        process_missing,
    );
    let legacy = scan_agent_artifacts_legacy_capacity_for_test(
        &projects,
        options.clone(),
        process_missing,
    );
    assert_scan_data_equal(&optimized, &legacy);
    let mut option_variants = vec![options.clone()];
    let mut newest_first = options.clone();
    newest_first.newest_first = true;
    option_variants.push(newest_first);
    let mut no_waiting_payload = options.clone();
    no_waiting_payload.include_waiting = false;
    option_variants.push(no_waiting_payload);
    let mut no_workflow_state = options.clone();
    no_workflow_state.include_workflow_state = false;
    option_variants.push(no_workflow_state);
    let mut only_run = options.clone();
    only_run.only_workflow_dirs = vec!["run".to_string()];
    option_variants.push(only_run);
    let mut only_other_project = options.clone();
    only_other_project.only_projects = vec!["other".to_string()];
    option_variants.push(only_other_project);
    for option_variant in option_variants {
        let optimized = scan_agent_artifacts_with_process_observation(
            &projects,
            option_variant.clone(),
            process_missing,
        );
        let legacy = scan_agent_artifacts_legacy_capacity_for_test(
            &projects,
            option_variant,
            process_missing,
        );
        assert_scan_data_equal(&optimized, &legacy);
    }
    assert!(
        optimized
            .clan_context
            .iter()
            .any(|context| context.agent_clan == "orphan-clan"
                && context.clan_summary.as_deref()
                    == Some("entirely pruned clan")),
        "a fully pruned clan member still contributes context"
    );
    assert!(optimized.clan_context.iter().any(|context| {
        context.agent_clan == "research"
            && context.agent_clan_generation.as_deref() == Some("g1")
            && context.clan_tribe.as_deref() == Some("durable tribe")
            && context.clan_summary.as_deref() == Some("durable summary")
            && context.clan_tribe_source_launch_timestamp.is_none()
    }));
    assert!(optimized
        .records
        .iter()
        .any(|record| record.artifact_dir == root.to_string_lossy()));
    assert!(optimized
        .records
        .iter()
        .any(|record| record.artifact_dir == live.to_string_lossy()));
    for timestamp in [
        chain_root_timestamp,
        chain_middle_timestamp,
        "20260427190017",
    ] {
        assert!(optimized
            .records
            .iter()
            .any(|record| record.timestamp == timestamp));
    }
    assert!(optimized.records.iter().any(|record| {
        record.artifact_dir == fallback_source.to_string_lossy()
    }));
    assert!(optimized.clan_context.iter().any(|context| {
        context.agent_clan == "fallback-clan"
            && context.agent_clan_generation.is_none()
            && context.clan_tribe.as_deref() == Some("fallback tribe")
    }));
    assert!(optimized
        .records
        .iter()
        .any(|record| record.artifact_dir == pending.to_string_lossy()));
    assert!(optimized
        .records
        .iter()
        .any(|record| record.artifact_dir == waiting.to_string_lossy()));
    assert!(!optimized
        .records
        .iter()
        .any(|record| record.artifact_dir == dead_running.to_string_lossy()));
    assert!(!optimized
        .records
        .iter()
        .any(|record| record.artifact_dir == sharded.to_string_lossy()));
    assert!(!optimized
        .records
        .iter()
        .any(|record| record.artifact_dir == stopped_orphan.to_string_lossy()));
    assert!(optimized.records.iter().any(|record| {
        record.timestamp == duplicate_layout_timestamp
            && record.artifact_dir == duplicate_sharded.to_string_lossy()
    }));
    assert!(optimized.records.iter().any(
        |record| record.artifact_dir == second_generation.to_string_lossy()
    ));

    let exact_active = write_capacity_meta(
        &projects,
        "20260427190011",
        json!({
            "name": "exact-active",
            "pid": std::process::id(),
            "parent_timestamp": "20260427190012"
        }),
    );
    let outside_exact = projects
        .join("proj")
        .join("artifacts")
        .join("run")
        .join("20260427190012");
    write_json(
        &outside_exact.join("agent_meta.json"),
        json!({"name": "outside-exact", "stopped_at": "stopped"}),
    );
    let exact_dirs = vec![
        live.clone(),
        root.clone(),
        exact_active.clone(),
        live.clone(),
    ];
    let exact_optimized = scan_agent_artifact_dirs_with_process_observation(
        &projects,
        &exact_dirs,
        options.clone(),
        process_missing,
    );
    let exact_legacy = scan_agent_artifact_dirs_legacy_capacity_for_test(
        &projects,
        &exact_dirs,
        options.clone(),
        process_missing,
    );
    assert_scan_data_equal(&exact_optimized, &exact_legacy);
    assert!(exact_optimized
        .records
        .iter()
        .any(|record| record.artifact_dir == root.to_string_lossy()));
    assert!(!exact_optimized
        .records
        .iter()
        .any(|record| record.artifact_dir == outside_exact.to_string_lossy()));

    // The singular API deliberately keeps its historical no-closure behavior.
    let stopped = write_capacity_meta(
        &projects,
        "20260427190013",
        json!({"name": "singular", "stopped_at": "stopped"}),
    );
    assert!(scan_agent_artifact_dir(&projects, &stopped, &options).is_some());
}

#[cfg(unix)]
#[test]
fn dangling_marker_symlinks_force_the_full_conservative_path() {
    use std::os::unix::fs::symlink;

    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let dangling = write_capacity_meta(
        &projects,
        "20260427191000",
        json!({"name": "dangling", "stopped_at": "stopped"}),
    );
    symlink("absent-target", dangling.join("running.json")).unwrap();

    let options = capacity_options();
    let process_missing = |_pid| false;
    let optimized = scan_agent_artifacts_with_process_observation(
        &projects,
        options.clone(),
        process_missing,
    );
    let legacy = scan_agent_artifacts_legacy_capacity_for_test(
        &projects,
        options,
        process_missing,
    );
    assert_scan_data_equal(&optimized, &legacy);
    assert_eq!(optimized.records.len(), 1);
}

#[test]
fn grouped_clan_context_matches_the_legacy_resolver_for_many_generations() {
    let mut keys = BTreeSet::new();
    let mut members = Vec::new();
    for generation in 0..64 {
        let generation = format!("g{generation}");
        keys.insert(("research".to_string(), Some(generation.clone())));
        members.push(crate::agent_clan_tribe::ClanTribeMemberWire {
            agent_clan: "research".to_string(),
            agent_clan_generation: Some(generation.clone()),
            clan_tribe: Some("first".to_string()),
            clan_summary: None,
            launch_timestamp: "20260427190000".to_string(),
            identity: "a".to_string(),
        });
        members.push(crate::agent_clan_tribe::ClanTribeMemberWire {
            agent_clan: "research".to_string(),
            agent_clan_generation: Some(generation),
            clan_tribe: Some("winner".to_string()),
            clan_summary: Some("summary".to_string()),
            launch_timestamp: "20260427190000".to_string(),
            identity: "z".to_string(),
        });
    }
    let grouped = resolve_clan_context(keys.clone(), members.clone());
    let legacy =
        super::super::context::resolve_clan_context_legacy(keys, members);
    assert_eq!(
        serde_json::to_value(grouped).unwrap(),
        serde_json::to_value(legacy).unwrap()
    );
}
